//! A database the release before recorded triggers' cuts left, booted by
//! this one: every version a build registered there has its triggers'
//! cuts walked from the program it keeps, as a registration walks them
//! (`program_trigger_cut`, through the project group's data step), and a
//! cut a run kept is marked as having none. A second boot walks nothing.

use serde_json::json;
use weft_dispatcher::project_store::ProjectStoreOps;
use weft_task_store::db::{Client as _, Pool};

/// The newest migration the weft before recorded cuts shipped.
const BEFORE_RECORDED_CUTS: &str = "20261101T191834_worker_door_worker_memory_note";

/// A program with two triggers, one feeding a node.
fn program(id: uuid::Uuid) -> weft_core::ProjectDefinition {
    let node = |id: &str, trigger: bool| {
        json!({
            "id": id, "nodeType": if trigger { "Cron" } else { "Debug" }, "label": null, "config": {}, "position": { "x": 0.0, "y": 0.0 },
            "inputs": if trigger { json!([]) } else { json!([{ "name": "data", "portType": "String", "required": true }]) },
            "outputs": [{ "name": "out", "portType": "String", "required": false }],
            "features": if trigger { json!({ "isTrigger": true }) } else { json!({}) },
            "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
        })
    };
    serde_json::from_value(json!({
        "id": id, "name": "p", "description": null,
        "nodes": [node("tick", true), node("seen", false), node("tock", true)],
        "edges": [{ "id": "e", "source": "tick", "sourceHandle": "out", "target": "seen", "targetHandle": "data" }],
        "groups": [], "createdAt": "1970-01-01T00:00:00Z", "updatedAt": "1970-01-01T00:00:00Z"
    }))
    .unwrap()
}

#[weft_db_macros::test]
async fn a_version_an_older_weft_registered_has_its_triggers_cuts_walked(pool: Pool) {
    weft_task_store::schema_guard::build_as_of(&pool, &weft_runtime::schema::groups(), |m| m.id <= BEFORE_RECORDED_CUTS)
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4();
    let program = program(id);
    let json = serde_json::to_string(&program).unwrap();
    for (hash, cut_from) in [("version", None), ("cut", Some("version"))] {
        pool.execute(
            "INSERT INTO project_definition (project_id, definition_hash, project_json, project_json_version, recorded_at_unix, cut_from) \
             VALUES ($1, $2, $3, $4, 0, $5)",
            &[&id, &hash, &json, &weft_core::ProjectDefinition::STORED.version(), &cut_from],
        )
        .await
        .unwrap();
    }

    weft_runtime::schema::apply(&pool).await.unwrap();

    let projects = weft_dispatcher::PostgresProjectStore::new(pool.clone());
    let walked = weft_core::run_spec::trigger_cuts(&program);
    assert_eq!(walked.len(), 2);
    assert_eq!(projects.trigger_cuts(id, "version").await.unwrap(), Some(walked.clone()), "walked as a registration walks them");
    assert_eq!(projects.trigger_cuts(id, "cut").await.unwrap(), None, "a run's cut has none");
    let nullable: String = pool
        .scalar("SELECT is_nullable::text FROM information_schema.columns WHERE table_name = 'project_definition' AND column_name = 'trigger_cuts_walked'", &[])
        .await
        .unwrap();
    assert_eq!(nullable, "NO", "the contract part made every row say");

    weft_runtime::schema::apply(&pool).await.expect("a boot with nothing to do");
    assert_eq!(projects.trigger_cuts(id, "version").await.unwrap(), Some(walked), "a second boot walks nothing again");
}
