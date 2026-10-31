//! A database an older weft filled, booted by this one: every document it
//! wrote is carried to today's shape (`weft_runtime::schema::documents`)
//! and reads, what was derived from the old JSON follows it, and what a
//! step moved out of the JSON is where it lives now. The
//! database is built as that older weft left it (its history up to
//! [`OLDER_WEFT_HAD`]), and the rows are the committed version 1 samples,
//! as the weft before versions wrote them: a Slack
//! connection and its recorded events recipes, a signing-secret
//! connection, a program carrying a service recipe, and a project's resync.
//! A version build waiting on its images has its own test below, from a
//! later weft: that older one had no `version_build.inputs` yet.
//! (What the same boot moves out of a column it then drops is
//! `infra_units_move`.)

use serde_json::Value;
use weft_task_store::db::{Client as _, Pool};

fn samples(document: &str, version: i32) -> Vec<Value> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("documents")
        .join(document)
        .join(format!("v{version}.json"));
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

/// The hash the weft before wrote for a recipe: sha256 over its canonical
/// JSON, as it stood.
fn old_recipe_hash(events: &Value) -> String {
    weft_core::project::hash::sha256_hex(weft_core::project::hash::canonical_json(events).as_bytes())
}

/// The last migration the weft these rows come from had: the release
/// before the units and the resync's drive moved out of where they were.
pub(crate) const OLDER_WEFT_HAD: &str = "20261020T134642_infra_lifecycle_command_health_commands";

#[weft_db_macros::test]
async fn a_database_an_older_weft_filled_boots_and_every_row_reads(pool: Pool) {
    weft_task_store::schema_guard::build_as_of(&pool, &weft_runtime::schema::groups(), |m| m.id <= OLDER_WEFT_HAD)
        .await
        .unwrap();
    let specs = samples("access_grant.spec", 1);
    let (slack, hmac) = (&specs[0], &specs[1]);
    let slack_events = &slack["events"];
    let old_hash = old_recipe_hash(slack_events);
    let slack_id = uuid::Uuid::new_v4();
    let hmac_id = uuid::Uuid::new_v4();
    for (id, service, spec, hash) in
        [(slack_id, "slack", slack, Some(old_hash.clone())), (hmac_id, "hmac_auth", hmac, None)]
    {
        pool.execute(
            "INSERT INTO access_grant (id, tenant_id, service, client_id, spec_json, values_sealed, \
             provider_account, events_recipe_hash) VALUES ($1, 'local', $2, 'cid', $3::text::jsonb, '', 'T123', $4)",
            &[&id, &service, &spec.to_string(), &hash],
        )
        .await
        .unwrap();
    }
    pool.execute(
        "INSERT INTO service_events_recipe (service, recipe_hash, events_json) VALUES ('slack', $1, $2::text::jsonb)",
        &[&old_hash, &slack_events.to_string()],
    )
        .await
        .unwrap();
    let project_id = uuid::Uuid::new_v4();
    let program = &samples("project_definition.project_json", 1)[0];
    pool.execute(
        "INSERT INTO project_definition (project_id, definition_hash, project_json, recorded_at_unix) \
         VALUES ($1, 'the-hash-it-was-made-under', $2, 0)",
        &[&project_id, &program.to_string()],
    )
    .await
    .unwrap();
    // A resync under way, its drive inside its JSON.
    let resync = &samples("project.resync", 1)[0];
    pool.execute(
        "INSERT INTO project (id, name, status, project_json, updated_at, tenant_id, transition, resync) \
         VALUES ($1, 'p', 'registered', $2, 0, 'local', 'resyncing', $3::text::jsonb)",
        &[&project_id, &program.to_string(), &resync.to_string()],
    )
    .await
    .unwrap();
    weft_runtime::schema::apply(&pool).await.unwrap();

    let read_spec = |id: uuid::Uuid| {
        let pool = pool.clone();
        async move {
            let (weft_task_store::db::JsonText(spec), version): (weft_task_store::db::JsonText<Value>, i32) =
                pool.one("SELECT spec, spec_version FROM access_grant WHERE id = $1", &[&id]).await.unwrap();
            assert_eq!(version, weft_core::AccessSpec::STORED.version(), "the row was carried");
            weft_core::AccessSpec::STORED.decode(spec, version).expect("the connection's recipe reads")
        }
    };
    let slack_spec = read_spec(slack_id).await;
    let hmac_spec = read_spec(hmac_id).await;
    assert!(matches!(hmac_spec.verify, Some(weft_core::access::events::VerifyKind::Hmac { .. })));

    // The recorded recipe is kept under the hash of its new JSON, and the
    // Slack connection names that hash, so a verified push finds it.
    let recipes = weft_access_store::events_recipes_of(&pool, "slack").await.expect("the recorded recipes read");
    assert_eq!(recipes.len(), 1);
    let new_hash = weft_access_store::events_recipe_hash(&slack_spec.events).unwrap();
    assert_ne!(new_hash, old_hash);
    assert_eq!(recipes[0].recipe_hash, new_hash);
    let routed = weft_access_store::connections_for_event(&pool, "slack", "T123", &["cid".to_string()], &new_hash)
        .await
        .unwrap();
    assert_eq!(routed.iter().map(|t| t.id).collect::<Vec<_>>(), vec![slack_id]);
    let hmac_hash: Option<String> =
        pool.scalar("SELECT events_recipe_hash FROM access_grant WHERE id = $1", &[&hmac_id]).await.unwrap();
    assert_eq!(hmac_hash, None);

    // The program reads, under the hash it was made under.
    let (json, version): (String, i32) = pool
        .one(
            "SELECT project_json, project_json_version FROM project_definition \
             WHERE project_id = $1 AND definition_hash = 'the-hash-it-was-made-under'",
            &[&project_id],
        )
        .await
        .unwrap();
    weft_core::project::ProjectDefinition::STORED.decode(json.as_str(), version).expect("the program reads");

    // The resync's drive moved to its own column.
    let (weft_task_store::db::JsonText(plan), plan_version, driver): (weft_task_store::db::JsonText<Value>, i32, Option<uuid::Uuid>) =
        pool.one("SELECT resync, resync_version, resync_driver FROM project WHERE id = $1", &[&project_id]).await.unwrap();
    assert_eq!(driver.map(|d| d.to_string()).as_deref(), resync["driver"].as_str());
    assert!(plan.get("driver").is_none(), "{plan}");
    let plan = weft_dispatcher::task_kinds::resync::ResyncPlan::stored(project_id, plan, plan_version, driver).expect("the plan reads");
    assert_eq!(Some(plan.driver), driver);
}

/// The last migration the weft before `version_build.inputs` carried a
/// version had.
const UNVERSIONED_BUILD_INPUTS_HAD: &str = "20261030T090159_task_exact_values_as_text";

/// A version build still waiting on its images across an update keeps the
/// inputs the weft before wrote (version 1, its assets an object keyed by
/// path and type); the boot labels them version 1 and carries them, so the
/// version registers with them once its images are built.
#[weft_db_macros::test]
async fn a_waiting_version_builds_inputs_are_carried(pool: Pool) {
    weft_task_store::schema_guard::build_as_of(&pool, &weft_runtime::schema::groups(), |m| m.id <= UNVERSIONED_BUILD_INPUTS_HAD)
        .await
        .unwrap();
    let older = samples("version_build.inputs", 1);
    let ids: Vec<uuid::Uuid> = older.iter().map(|_| uuid::Uuid::new_v4()).collect();
    for (id, inputs) in ids.iter().zip(&older) {
        pool.execute(
            "INSERT INTO version_build (id, ask, project_id, tenant_id, project_name, manifest, inputs, program, images, \
             waits_on, state, asked_at) VALUES ($1, 1, $1, 'local', 'p', '{}', $2, '{}', '{}', '[]', 'waiting', 0)",
            &[id, &inputs.to_string()],
        )
        .await
        .unwrap();
    }
    weft_runtime::schema::apply(&pool).await.unwrap();

    let today = samples("version_build.inputs", weft_core::builds::BuildInputs::STORED.version());
    for (id, want) in ids.iter().zip(&today) {
        let (inputs, version): (String, i32) =
            pool.one("SELECT inputs, inputs_version FROM version_build WHERE id = $1", &[id]).await.unwrap();
        assert_eq!(version, weft_core::builds::BuildInputs::STORED.version(), "the row was carried");
        let read = weft_core::builds::BuildInputs::STORED.decode(inputs.as_str(), version).expect("the inputs read");
        assert_eq!(serde_json::to_value(&read).unwrap(), *want);
    }
}
