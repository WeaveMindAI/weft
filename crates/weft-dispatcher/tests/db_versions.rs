//! The version tree's Postgres store against a real database: the rows
//! the tree verbs read and write, and the one cross-store rule (a run's
//! row dies with its journal).
//!
//! Same rig as `db_lifecycle.rs`: `#[sqlx::test]` hands each test a fresh
//! database; `setup` applies the dispatcher's whole schema (every group,
//! the version tables included) exactly as a boot does.
#![cfg(feature = "db-tests")]

use std::collections::BTreeMap;

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_core::run_spec::RunSpec;
use weft_core::ProjectDefinition;
use weft_dispatcher::journal::postgres::PostgresJournal;
use weft_dispatcher::journal::Journal;
use weft_dispatcher::versions::{version_id, Head, PostgresVersionStore, RunRow, VersionRow, VersionStoreOps};
use weft_journal::ExecEvent;

const TENANT: &str = "tenant-1";

async fn setup(pool: &PgPool) -> (PostgresJournal, weft_dispatcher::ProjectStore, PostgresVersionStore) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
    let journal = PostgresJournal::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    let versions = PostgresVersionStore::new(pool.clone());
    (journal, projects, versions)
}

fn rig_project(id: Uuid) -> ProjectDefinition {
    serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] })).expect("rig ProjectDefinition")
}

async fn seed_project(projects: &weft_dispatcher::ProjectStore, id: Uuid) {
    projects
        .register_with_hashes(rig_project(id), "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None, None, None)
        .await
        .expect("register project");
}

fn manifest(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries.iter().map(|(p, h)| (p.to_string(), h.to_string())).collect()
}

fn version(project: Uuid, m: &BTreeMap<String, String>, parent: Option<&str>, label: Option<&str>, at: u64) -> VersionRow {
    VersionRow {
        id: version_id(m),
        project_id: project,
        parent_id: parent.map(str::to_string),
        manifest: m.clone(),
        label: label.map(str::to_string),
        created_at: at,
    }
}

fn run(project: Uuid, color: Uuid, version: &str, seed: Option<Uuid>, spec: Option<RunSpec>, at: u64) -> RunRow {
    RunRow {
        color,
        project_id: project,
        version_id: version.to_string(),
        seed_color: seed,
        stale: vec!["b".into(), "c".into()],
        spec,
        definition_hash: "def-1".into(),
        example: None,
        created_at: at,
    }
}

/// The same manifest is one row however often it is recorded, and a
/// second upsert keeps the first parent and label.
#[sqlx::test]
async fn identical_code_is_one_version(pool: PgPool) {
    let (_, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let m = manifest(&[("main.weft", "aaa"), ("weft.toml", "bbb")]);
    assert!(versions.upsert_version(&version(project, &m, None, Some("base"), 1)).await.unwrap());
    assert!(!versions.upsert_version(&version(project, &m, Some("other"), None, 2)).await.unwrap());
    let rows = versions.versions(project).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].parent_id, None);
    assert_eq!(rows[0].label.as_deref(), Some("base"));
    assert_eq!(rows[0].manifest, m);
    let by_id = versions.version(project, &rows[0].id).await.unwrap().expect("found");
    assert_eq!(by_id, rows[0]);
    versions.set_label(project, &rows[0].id, Some("renamed")).await.unwrap();
    assert_eq!(versions.version(project, &rows[0].id).await.unwrap().unwrap().label.as_deref(), Some("renamed"));
}

#[sqlx::test]
async fn retention_keeps_program_references_when_execution_selection_has_changed_format(pool: PgPool) {
    let (journal, projects, _) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let color = Uuid::new_v4();
    let birth = ExecEvent::ExecutionStarted {
        color, project_id: project.to_string(), entry_node: "mid".into(),
        phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()),
        program: None, source_version: None, node_test: false, subgraph: None, seed: None, at_unix: 0,
    };
    journal.record_event(&birth).await.unwrap();
    let mut row = serde_json::to_value(&birth).unwrap();
    row["subgraph"] = json!(["mid"]);
    sqlx::query("UPDATE exec_event SET payload_json = $2 WHERE color = $1 AND kind = 'execution_started'")
        .bind(color.to_string()).bind(row.to_string()).execute(&pool).await.unwrap();
    assert_eq!(journal.definition_hashes_in_use(&project.to_string()).await.unwrap(), vec!["def-1"]);
    row["definition_hash"] = json!(42);
    sqlx::query("UPDATE exec_event SET payload_json = $2 WHERE color = $1 AND kind = 'execution_started'")
        .bind(color.to_string()).bind(row.to_string()).execute(&pool).await.unwrap();
    assert!(journal.definition_hashes_in_use(&project.to_string()).await.is_err(), "an unreadable reference must block deletion");
}

/// A run's row round-trips whole (spec included) and lists under its
/// project in the order it was recorded, whatever its clock says
/// (`created_at` is whole seconds; two runs in one second must not
/// swap).
#[sqlx::test]
async fn runs_round_trip_and_list_in_recording_order(pool: PgPool) {
    let (_, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let m = manifest(&[("main.weft", "aaa")]);
    let v = version(project, &m, None, None, 1);
    versions.upsert_version(&v).await.unwrap();
    let (seed, child) = (Uuid::new_v4(), Uuid::new_v4());
    let spec = RunSpec { name: "angry".into(), from: [("classify".into(), Default::default())].into(), ..Default::default() };
    versions.insert_run(&run(project, seed, &v.id, None, None, 10)).await.unwrap();
    versions.insert_run(&run(project, child, &v.id, Some(seed), Some(spec.clone()), 5)).await.unwrap();
    let rows = versions.runs(project).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.color).collect::<Vec<_>>(), vec![seed, child]);
    assert_eq!(rows[1].seed_color, Some(seed));
    assert_eq!(rows[1].stale, vec!["b", "c"]);
    assert_eq!(rows[1].spec, Some(spec));
    versions.set_run_example(child, Some("angry")).await.unwrap();
    let again = versions.run(child).await.unwrap().expect("found");
    assert_eq!(again.example.as_deref(), Some("angry"));
    versions.set_run_example(child, None).await.unwrap();
    assert_eq!(versions.run(child).await.unwrap().expect("found").example, None);
    assert!(
        versions.set_run_example(Uuid::new_v4(), Some("x")).await.is_err(),
        "an unrecorded run is refused"
    );
}

/// Head lives on the project row: moved by checkpoint, run and branch,
/// cleared with the activation on deactivate.
#[sqlx::test]
async fn head_and_activation_live_on_the_project_row(pool: PgPool) {
    let (_, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    assert_eq!(versions.head(project).await.unwrap(), Default::default());
    let color = Uuid::new_v4();
    versions.move_head(project, &Head::default(), Some("v1"), Some(color)).await.unwrap();
    let activated = weft_core::project::hash::ProgramIdentity {
        binary_hash: "bin-A".into(), definition_hash: "def-1".into(), implementations: Default::default(),
    };
    sqlx::query("UPDATE project SET activation_version = 'v1', activation_program = $2 WHERE id = $1")
        .bind(project).bind(sqlx::types::Json(&activated)).execute(&pool).await.unwrap();
    projects.register_with_hashes(rig_project(project), "db-rig", "", TENANT, Some("bin-B"), Some("def-2"), None, None, None, None).await.unwrap();
    assert_eq!(projects.activation_program(project).await.unwrap(), Some(activated), "a build preserves the code activation used");
    let head = versions.head(project).await.unwrap();
    assert_eq!(head.head_version.as_deref(), Some("v1"));
    assert_eq!(head.head_run, Some(color));
    assert_eq!(head.activation_version.as_deref(), Some("v1"));
    versions
        .move_head(project, &Head { head_version: Some("v1".into()), head_run: Some(color), activation_version: None }, Some("v2"), None)
        .await
        .unwrap();
    projects.set_lifecycle_guarded(project, &weft_dispatcher::project_store::ProjectLifecycle::wiped()).await.unwrap();
    assert_eq!(projects.activation_program(project).await.unwrap(), None);
    let head = versions.head(project).await.unwrap();
    assert_eq!((head.head_version.as_deref(), head.head_run, head.activation_version), (Some("v2"), None, None));
    assert!(
        versions.move_head(Uuid::new_v4(), &Head::default(), Some("v"), None).await.is_err(),
        "no such project"
    );
    // A lost race is Ok(false), never an error: the handler turns it
    // into a 409, and a decode failure here used to make it a 500.
    let stale = Head { head_version: Some("nowhere".into()), head_run: None, activation_version: None };
    assert_eq!(
        versions.move_head(project, &stale, Some("v2"), None).await.unwrap(),
        false,
        "head is not where the caller thought, so nothing moves"
    );
}

/// `weft clean <color>` drops the run's row through the version store
/// (the table is the version store's; the journal owns the journal),
/// and a head that pointed at the run keeps its version and loses the
/// run pointer.
#[sqlx::test]
async fn deleting_a_run_clears_its_row_and_head_run(pool: PgPool) {
    let (journal, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let m = manifest(&[("main.weft", "aaa")]);
    let v = version(project, &m, None, None, 1);
    versions.upsert_version(&v).await.unwrap();
    let color = Uuid::new_v4();
    journal
        .record_event(&ExecEvent::ExecutionStarted {
            color,
            project_id: project.to_string(),
            entry_node: "a".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("def-1".into()),
            program: None, source_version: None, node_test: false,
            subgraph: None,
            seed: None,
            at_unix: 0,
        })
        .await
        .unwrap();
    versions.insert_run(&run(project, color, &v.id, None, None, 5)).await.unwrap();
    versions.move_head(project, &Head::default(), Some(&v.id), Some(color)).await.unwrap();
    // The order `clean_execution` uses: the tree row first (so a
    // failure leaves the journal reachable and the command retryable),
    // then the journal.
    versions.delete_run(color).await.unwrap();
    journal.delete_execution(color).await.unwrap();
    assert!(versions.run(color).await.unwrap().is_none(), "the run row is gone");
    // Idempotent, which is what makes the retry safe.
    versions.delete_run(color).await.unwrap();
    let head = versions.head(project).await.unwrap();
    assert_eq!(head.head_version.as_deref(), Some(v.id.as_str()));
    assert_eq!(head.head_run, None);
    assert_eq!(versions.versions(project).await.unwrap().len(), 1, "the version stays");
}

/// Deleting versions cascades to their runs; a project's removal
/// cascades to everything.
#[sqlx::test]
async fn deleting_versions_cascades_to_their_runs(pool: PgPool) {
    let (_, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let root = version(project, &manifest(&[("main.weft", "1")]), None, None, 1);
    let child = version(project, &manifest(&[("main.weft", "2")]), Some(&root.id), None, 2);
    versions.upsert_version(&root).await.unwrap();
    versions.upsert_version(&child).await.unwrap();
    let (r1, r2) = (Uuid::new_v4(), Uuid::new_v4());
    versions.insert_run(&run(project, r1, &root.id, None, None, 3)).await.unwrap();
    versions.insert_run(&run(project, r2, &child.id, None, None, 4)).await.unwrap();
    versions.delete_versions(project, &[child.id.clone()]).await.unwrap();
    assert!(versions.run(r2).await.unwrap().is_none());
    assert!(versions.run(r1).await.unwrap().is_some());
    assert_eq!(versions.versions(project).await.unwrap().len(), 1);
    assert!(projects.remove(project).await.unwrap());
    assert_eq!(versions.versions(project).await.unwrap().len(), 1, "removing the live project preserves its history");
    assert!(versions.run(r1).await.unwrap().is_some());
}
