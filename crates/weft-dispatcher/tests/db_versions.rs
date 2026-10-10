//! The version tree's Postgres store against a real database: the rows
//! the tree verbs read and write, and the runs it shows, read off the
//! runs' own rows.
//!
//! Same rig as `db_lifecycle.rs`: `#[sqlx::test]` hands each test a fresh
//! database; `setup` applies the dispatcher's whole schema (every group,
//! the version tables included) exactly as a boot does.
#![cfg(feature = "db-tests")]

use std::collections::BTreeMap;

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_core::activation::ActivationKey;
use weft_core::instance::Owner;
use weft_core::run_spec::RunSpec;
use weft_core::ProjectDefinition;
use weft_dispatcher::activation_store::{ActivationLifecycle, ActivationStoreOps, PostgresActivationStore, SignalsGoing};
use weft_dispatcher::journal::postgres::PostgresJournal;
use weft_dispatcher::journal::Journal;
use weft_core::versions::Head;
use weft_dispatcher::versions::{version_id, PostgresVersionStore, VersionRow, VersionStoreOps};
use weft_journal::record::{Queued, Then};
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
        .register_with_hashes(rig_project(id), "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None, None, None, None)
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

/// Start a run of `project` by hand, from `version`, the way `weft run`
/// queues one: what the tree shows of it rides its row.
async fn start_run(
    journal: &PostgresJournal,
    project: Uuid,
    version: &str,
    seed: Option<Uuid>,
    spec: Option<&RunSpec>,
    at: u64,
) -> Uuid {
    let execution_id = weft_core::new_execution_id();
    let birth = ExecEvent::ExecutionStarted {
        execution_id, project_id: project, entry_node: "a".into(),
        phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()), binary_hash: Some("bin-A".into()),
        source_version: Some(version.into()), run_kind: weft_core::exec::RunKind::Execution, selection: None,
        seed: seed.map(|parent| weft_core::run_spec::Seed { parent, origins: Default::default() }),
        instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: at,
        settings: Default::default(),
    };
    let spec = spec.map(|spec| serde_json::to_value(spec).unwrap());
    let stale = ["b".to_string(), "c".to_string()];
    let queued = Queued {
        events: std::slice::from_ref(&birth), tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false, stale: &stale, spec: spec.as_ref(), example: None,
    };
    assert!(journal.queue_run(queued, false).await.unwrap());
    execution_id
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

/// The program definitions a project's runs need are read off their rows,
/// whatever their record holds.
#[sqlx::test]
async fn the_definitions_in_use_are_read_off_the_runs(pool: PgPool) {
    let (journal, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    assert!(journal.definition_hashes_in_use(project).await.unwrap().is_empty());
    let v = version(project, &manifest(&[("main.weft", "aaa")]), None, None, 1);
    versions.upsert_version(&v).await.unwrap();
    start_run(&journal, project, &v.id, None, None, 1).await;
    assert_eq!(journal.definition_hashes_in_use(project).await.unwrap(), vec!["def-1"]);
}

/// A run started by hand lists under its version with what it was asked
/// (its spec, its seed, the places it ran again), in the order it was
/// started, and carries the saved example it is set to. A run nobody
/// started by hand (a trigger's) is counted on its version, never listed.
#[sqlx::test]
async fn runs_round_trip_and_list_in_recording_order(pool: PgPool) {
    let (journal, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let m = manifest(&[("main.weft", "aaa")]);
    let v = version(project, &m, None, None, 1);
    versions.upsert_version(&v).await.unwrap();
    let spec = RunSpec { name: "angry".into(), from: [("classify".into(), Default::default())].into(), ..Default::default() };
    let seed = start_run(&journal, project, &v.id, None, Some(&RunSpec::default()), 5).await;
    let child = start_run(&journal, project, &v.id, Some(seed), Some(&spec), 5).await;
    start_run(&journal, project, &v.id, None, None, 6).await;
    let rows = versions.runs(project).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.execution_id).collect::<Vec<_>>(), vec![seed, child], "in the order they were started");
    assert_eq!(rows[1].version_id, v.id);
    assert_eq!(rows[1].seed_execution_id, Some(seed));
    assert_eq!(rows[1].stale, vec!["b", "c"]);
    assert_eq!(rows[1].spec, Some(spec));
    versions.set_run_example(child, Some("angry")).await.unwrap();
    let again = versions.run(child).await.unwrap().expect("found");
    assert_eq!(again.example.as_deref(), Some("angry"));
    versions.set_run_example(child, None).await.unwrap();
    assert_eq!(versions.run(child).await.unwrap().expect("found").example, None);
    assert!(
        versions.set_run_example(Uuid::new_v4(), Some("x")).await.is_err(),
        "a run that is not on record is refused"
    );
}

/// Head lives on the project row, moved by checkpoint, run and branch.
/// The activated versions are read off the trigger activations: a build
/// leaves them alone, and a deactivation clears them.
#[sqlx::test]
async fn head_lives_on_the_project_row_and_activations_name_their_versions(pool: PgPool) {
    let (_, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    assert_eq!(versions.head(project).await.unwrap(), Default::default());
    let execution_id = Uuid::new_v4();
    versions.move_head(project, &Head::default(), Some("v1"), Some(execution_id)).await.unwrap();
    let activated = weft_core::project::hash::ProgramIdentity {
        binary_hash: "bin-A".into(), definition_hash: "def-1".into(), implementations: Default::default(),
    };
    let activations = PostgresActivationStore::new(pool.clone());
    let keys = [ActivationKey::new("door", Owner::Shared)];
    let setup_execution_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(project, &keys, setup_execution_id, None).await.unwrap().is_ok());
    assert!(activations.record_activation_source(project, setup_execution_id, &activated, "v1").await.unwrap());
    activations.end_activating(project, setup_execution_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    projects.register_with_hashes(rig_project(project), "db-rig", "", TENANT, Some("bin-B"), Some("def-2"), None, None, None, None, None).await.unwrap();
    let listed = activations.list(project).await.unwrap();
    assert_eq!(listed[0].program, Some(activated), "a build preserves the code activation used");
    let head = versions.head(project).await.unwrap();
    assert_eq!(head.head_version.as_deref(), Some("v1"));
    assert_eq!(head.head_run, Some(execution_id));
    assert_eq!(head.activated_versions, vec!["v1".to_string()]);
    versions
        .move_head(project, &Head { head_version: Some("v1".into()), head_run: Some(execution_id), activated_versions: vec![] }, Some("v2"), None)
        .await
        .unwrap();
    activations.set_lifecycle_guarded(project, &keys, &ActivationLifecycle::wiped(), SignalsGoing::Kept).await.unwrap();
    assert_eq!(activations.list(project).await.unwrap()[0].program, None);
    let head = versions.head(project).await.unwrap();
    assert_eq!((head.head_version.as_deref(), head.head_run, head.activated_versions), (Some("v2"), None, vec![]));
    assert!(
        versions.move_head(Uuid::new_v4(), &Head::default(), Some("v"), None).await.is_err(),
        "no such project"
    );
    // A lost race is Ok(false), never an error: the handler turns it
    // into a 409, and a decode failure here used to make it a 500.
    let stale = Head { head_version: Some("nowhere".into()), head_run: None, activated_versions: vec![] };
    assert!(
        !versions.move_head(project, &stale, Some("v2"), None).await.unwrap(),
        "head is not where the caller thought, so nothing moves"
    );
}

/// `weft clean <execution_id>` erases the run, and a head that pointed at
/// it keeps its version and loses the run pointer.
#[sqlx::test]
async fn cleaning_a_run_erases_it_and_clears_head_run(pool: PgPool) {
    let (journal, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let m = manifest(&[("main.weft", "aaa")]);
    let v = version(project, &m, None, None, 1);
    versions.upsert_version(&v).await.unwrap();
    let execution_id = start_run(&journal, project, &v.id, None, Some(&RunSpec::default()), 5).await;
    versions.move_head(project, &Head::default(), Some(&v.id), Some(execution_id)).await.unwrap();
    // The order `clean_execution` uses.
    versions.forget_head_run(execution_id).await.unwrap();
    journal.delete_execution(execution_id).await.unwrap();
    assert!(versions.run(execution_id).await.unwrap().is_none(), "the run is gone");
    // Idempotent, which is what makes the retry safe.
    versions.forget_head_run(execution_id).await.unwrap();
    journal.delete_execution(execution_id).await.unwrap();
    let head = versions.head(project).await.unwrap();
    assert_eq!(head.head_version.as_deref(), Some(v.id.as_str()));
    assert_eq!(head.head_run, None);
    assert_eq!(versions.versions(project).await.unwrap().len(), 1, "the version stays");
}

/// A pruned version takes its runs out of the tree (they stay on record
/// until retention erases them); a project's removal drops its whole tree
/// (the store deletes it explicitly: a version left behind kept naming
/// stored files of a project that no longer existed, and the same id
/// registered again inherited a tree it never made).
#[sqlx::test]
async fn deleting_versions_takes_their_runs_out_of_the_tree(pool: PgPool) {
    let (journal, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let root = version(project, &manifest(&[("main.weft", "1")]), None, None, 1);
    let child = version(project, &manifest(&[("main.weft", "2")]), Some(&root.id), None, 2);
    versions.upsert_version(&root).await.unwrap();
    versions.upsert_version(&child).await.unwrap();
    let r1 = start_run(&journal, project, &root.id, None, Some(&RunSpec::default()), 3).await;
    let r2 = start_run(&journal, project, &child.id, None, Some(&RunSpec::default()), 4).await;
    for run in [r1, r2] {
        let written = journal.append(run, &[ExecEvent::ExecutionCompleted { execution_id: run, at_unix: 5 }], Then::Stays).await.unwrap();
        assert!(matches!(written, weft_journal::record::Appended::At(_)));
    }
    versions.delete_versions(project, std::slice::from_ref(&child.id)).await.unwrap();
    assert!(versions.run(r2).await.unwrap().is_none());
    assert!(versions.run(r1).await.unwrap().is_some());
    assert_eq!(versions.versions(project).await.unwrap().len(), 1);
    assert!(projects.remove(project).await.unwrap());
    assert!(versions.versions(project).await.unwrap().is_empty(), "removing the project drops its tree");
    assert!(versions.run(r1).await.unwrap().is_none(), "and the tree's runs with it");
}

/// A version's trigger count sums every writer's lane and names the
/// newest run across them.
#[sqlx::test]
async fn trigger_runs_sum_the_lanes_and_name_the_newest_run(pool: PgPool) {
    let (_journal, _projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    let older = Uuid::now_v7();
    let newer = Uuid::now_v7();
    for (lane, runs, last) in [("a", 2_i64, newer), ("b", 3, older)] {
        sqlx::query("INSERT INTO version_runs (project_id, source_version, lane, runs, last_run) VALUES ($1, 'v1', $2, $3, $4)")
            .bind(project)
            .bind(lane)
            .bind(runs)
            .bind(last)
            .execute(&pool)
            .await
            .unwrap();
    }
    let counted = versions.trigger_runs(project).await.unwrap();
    assert_eq!(counted.len(), 1);
    assert_eq!(counted["v1"].runs, 5);
    assert_eq!(counted["v1"].last, newer);
}
