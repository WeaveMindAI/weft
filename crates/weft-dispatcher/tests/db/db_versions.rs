//! The version tree's Postgres store against a real database: the rows
//! the tree verbs read and write, and the runs it shows, read off the
//! runs' own rows.
//!
//! Same rig as `db_lifecycle.rs`: `#[weft_db_macros::test]` hands each test a fresh
//! database; `setup` applies the dispatcher's whole schema (every group,
//! the version tables included) exactly as a boot does.

use std::collections::BTreeMap;

use serde_json::json;
use uuid::Uuid;

use weft_core::activation::ActivationKey;
use weft_core::instance::Owner;
use weft_core::run_spec::RunSpec;
use weft_core::ProjectDefinition;
use weft_dispatcher::activation_store::{ActivationLifecycle, ActivationStoreOps, Claimant, PostgresActivationStore, SignalsGoing};
use weft_dispatcher::store::postgres::PostgresStore;
use weft_dispatcher::store::RunStore;
use weft_core::working_copy::{Expect, Head, WorkingChange};
use weft_dispatcher::versions::{version_id, PostgresVersionStore, VersionRow, VersionStoreOps};
use weft_dispatcher::working_copies::{PostgresWorkingCopyStore, WorkingCopyStoreOps, Write, Written};
use weft_journal::record::{Queued, Then};
use weft_journal::RunEvent;
use weft_task_store::db::{Client as _, Pool};

const TENANT: &str = "tenant-1";

async fn setup(pool: &Pool) -> (PostgresStore, weft_dispatcher::ProjectStore, PostgresVersionStore) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let store = PostgresStore::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    let versions = PostgresVersionStore::new(pool.clone());
    (store, projects, versions)
}

fn rig_project(id: Uuid) -> ProjectDefinition {
    serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] })).expect("rig ProjectDefinition")
}

async fn seed_project(projects: &weft_dispatcher::ProjectStore, id: Uuid) {
    projects
        .register_with_hashes(rig_project(id), "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None, None, None, None, None)
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
    store: &PostgresStore,
    project: Uuid,
    version: &str,
    seed: Option<Uuid>,
    spec: Option<&RunSpec>,
    at: u64,
) -> Uuid {
    let run_id = weft_core::new_run_id();
    let birth = RunEvent::RunStarted { given: Default::default(),
        run_id, project_id: project, entry_node: "a".into(),
        phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()), binary_hash: Some("bin-A".into()),
        source_version: Some(version.into()), run_kind: weft_core::run::RunKind::Project, cut_from: None,
        seed: seed.map(|parent| weft_core::run_spec::Seed { parent, origins: Default::default(), answers: Default::default() }),
        instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: at,
        settings: Default::default(),
    };
    let stale = ["b".to_string(), "c".to_string()];
    let queued = Queued {
        events: std::slice::from_ref(&birth), tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false, stale: &stale, spec, example: None, program: None,
    };
    assert!(store.queue_run(queued, false).await.unwrap());
    run_id
}

/// The same manifest is one row however often it is recorded, and a
/// second upsert keeps the first parent and label.
#[weft_db_macros::test]
async fn identical_code_is_one_version(pool: Pool) {
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

/// A run started by hand lists under its version with what it was asked
/// (its spec, its seed, the places it ran again), in the order it was
/// started, and carries the saved example it is set to. A run nobody
/// started by hand (a trigger's) is counted on its version, never listed.
#[weft_db_macros::test]
async fn runs_round_trip_and_list_in_recording_order(pool: Pool) {
    let (store, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let m = manifest(&[("main.weft", "aaa")]);
    let v = version(project, &m, None, None, 1);
    versions.upsert_version(&v).await.unwrap();
    let spec = RunSpec { name: "angry".into(), from: [("classify".into(), Default::default())].into(), ..Default::default() };
    let seed = start_run(&store, project, &v.id, None, Some(&RunSpec::default()), 5).await;
    let child = start_run(&store, project, &v.id, Some(seed), Some(&spec), 5).await;
    start_run(&store, project, &v.id, None, None, 6).await;
    let rows = versions.runs(project).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.run_id).collect::<Vec<_>>(), vec![seed, child], "in the order they were started");
    assert_eq!(rows[1].version_id, v.id);
    assert_eq!(rows[1].seed_run_id, Some(seed));
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

/// A run started with values holding NUL characters (a page pasted as a
/// starting value) starts, and its spec reads back exactly.
#[weft_db_macros::test]
async fn a_spec_holding_nul_reads_back_exactly(pool: Pool) {
    let (store, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let v = version(project, &manifest(&[("main.weft", "aaa")]), None, None, 1);
    versions.upsert_version(&v).await.unwrap();
    let spec = RunSpec { name: "pasted".into(), caller: vec![json!({ "page": "a\u{0}b" })], ..Default::default() };
    let run = start_run(&store, project, &v.id, None, Some(&spec), 5).await;
    assert_eq!(versions.run(run).await.unwrap().expect("found").spec, Some(spec));
}

/// A head lives on a working copy, moved by a stamp, a run and a switch,
/// compare-and-set. The activated versions are read off the trigger
/// activations: a build leaves them alone, and a deactivation clears them.
#[weft_db_macros::test]
async fn heads_live_on_working_copies_and_activations_name_their_versions(pool: Pool) {
    let (_, projects, versions) = setup(&pool).await;
    let copies = PostgresWorkingCopyStore::new(pool.clone());
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let copy = copies.program_copy(project, &manifest(&[]), &Head::default(), 1).await.unwrap();
    assert_eq!(copy.head, Head::default());
    let run_id = Uuid::new_v4();
    let v1 = Head { head_version: Some("v1".into()), head_run: Some(run_id) };
    assert!(copies.move_head(copy.id, &Head::default(), &v1, 2).await.unwrap());
    let activated = weft_core::project::hash::ProgramIdentity {
        binary_hash: "bin-A".into(), definition_hash: "def-1".into(), implementations: Default::default(),
    };
    let activations = PostgresActivationStore::new(pool.clone());
    let keys = [ActivationKey::new("door", Owner::Shared)];
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(project, &keys, setup_run_id, None, Claimant::Anyone, uuid::Uuid::new_v4()).await.unwrap().is_ok());
    assert!(activations.record_activation_source(project, setup_run_id, &activated, "v1", &Default::default()).await.unwrap());
    activations.end_activating(project, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    projects.register_with_hashes(rig_project(project), "db-rig", "", TENANT, Some("bin-B"), Some("def-2"), None, None, None, None, None, None).await.unwrap();
    let listed = activations.list(project).await.unwrap();
    assert_eq!(listed[0].program, Some(activated), "a build preserves the code activation used");
    assert_eq!(copies.get(copy.id).await.unwrap().unwrap().head, v1);
    assert_eq!(versions.activated_versions(project).await.unwrap(), vec!["v1".to_string()]);
    let v2 = Head { head_version: Some("v2".into()), head_run: None };
    assert!(copies.move_head(copy.id, &v1, &v2, 3).await.unwrap());
    activations.set_lifecycle_guarded(project, &keys, &ActivationLifecycle::refused(), SignalsGoing::Kept, weft_dispatcher::activation_store::Forget::Nothing, weft_dispatcher::activation_store::DownCause::Person, None).await.unwrap();
    assert_eq!(activations.list(project).await.unwrap()[0].program, None);
    assert!(versions.activated_versions(project).await.unwrap().is_empty());
    assert!(copies.move_head(Uuid::new_v4(), &Head::default(), &v2, 4).await.is_err(), "no such working copy");
    // A lost race is Ok(false), never an error: the action turns it into
    // a 409.
    let stale = Head { head_version: Some("nowhere".into()), head_run: None };
    assert!(!copies.move_head(copy.id, &stale, &v1, 5).await.unwrap(), "head is not where the caller thought, so nothing moves");
    assert_eq!(copies.get(copy.id).await.unwrap().unwrap().head, v2);
}

/// `weft clean <run_id>` erases the run, and every head that pointed at
/// it keeps its version and loses the run pointer.
#[weft_db_macros::test]
async fn cleaning_a_run_erases_it_and_clears_head_run(pool: Pool) {
    let (store, projects, versions) = setup(&pool).await;
    let copies = PostgresWorkingCopyStore::new(pool.clone());
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let m = manifest(&[("main.weft", "aaa")]);
    let v = version(project, &m, None, None, 1);
    versions.upsert_version(&v).await.unwrap();
    let run_id = start_run(&store, project, &v.id, None, Some(&RunSpec::default()), 5).await;
    let copy = copies.program_copy(project, &m, &Head::default(), 1).await.unwrap();
    let on_run = Head { head_version: Some(v.id.clone()), head_run: Some(run_id) };
    assert!(copies.move_head(copy.id, &Head::default(), &on_run, 2).await.unwrap());
    // The order `clean_run` uses.
    copies.forget_head_run(run_id).await.unwrap();
    store.delete_run(run_id).await.unwrap();
    assert!(versions.run(run_id).await.unwrap().is_none(), "the run is gone");
    // Idempotent, which is what makes the retry safe.
    copies.forget_head_run(run_id).await.unwrap();
    store.delete_run(run_id).await.unwrap();
    let head = copies.get(copy.id).await.unwrap().unwrap().head;
    assert_eq!(head.head_version.as_deref(), Some(v.id.as_str()));
    assert_eq!(head.head_run, None);
    assert_eq!(versions.versions(project).await.unwrap().len(), 1, "the version stays");
}

/// Two writers of one file of a working copy: the one that read the file
/// before the other wrote is refused whole, nothing of it lands, and the
/// revision moves only with a change of the files.
#[weft_db_macros::test]
async fn a_stale_writer_of_a_working_copy_is_refused(pool: Pool) {
    let (_, projects, _) = setup(&pool).await;
    let copies = PostgresWorkingCopyStore::new(pool.clone());
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let (one, two, three) = ("1".repeat(64), "2".repeat(64), "3".repeat(64));
    let folder = copies
        .register_folder(project, "box", "/home/me/shop", &manifest(&[("main.weft", &one)]), &Head::default(), 1)
        .await
        .unwrap();
    assert_eq!(folder.synced, folder.working, "a folder starts as what it holds");
    let again = copies.register_folder(project, "box", "/home/me/shop", &manifest(&[]), &Head::default(), 2).await.unwrap();
    assert_eq!((again.id, again.working.len()), (folder.id, 1), "registering again changes nothing");
    let change = |hash: &str, read: &str| WorkingChange { path: "main.weft".into(), hash: Some(hash.into()), expected: Expect::Hash { hash: read.into() } };
    let first = copies.write(folder.id, Write { changes: &[change(&two, &one)], ..Default::default() }, 3).await.unwrap();
    let Written::Landed(row) = first else { panic!("the first writer lands: {first:?}") };
    assert_eq!((row.revision, row.working["main.weft"].as_str()), (1, two.as_str()));
    let second = copies.write(folder.id, Write { changes: &[change(&three, &one)], ..Default::default() }, 4).await.unwrap();
    assert_eq!(second, Written::Stale(vec!["main.weft".into()]));
    let now = copies.get(folder.id).await.unwrap().unwrap();
    assert_eq!((now.revision, now.working["main.weft"].as_str()), (1, two.as_str()));
    // What the folder holds moves alone, without a revision.
    let synced = BTreeMap::from([("main.weft".to_string(), Some(two.clone()))]);
    let Written::Landed(row) = copies.write(folder.id, Write { changes: &[], synced: Some(&synced), conflicts: Some(&[]) }, 5).await.unwrap() else {
        panic!("a report of what the folder holds lands")
    };
    assert_eq!((row.revision, row.synced["main.weft"].as_str()), (1, two.as_str()));
    // A switch replaces the files only from the revision it read.
    let to = Head { head_version: Some("v9".into()), head_run: None };
    assert!(copies.replace(folder.id, 0, &manifest(&[]), &to, 6).await.unwrap().is_none(), "a stale revision moves nothing");
    let switched = copies.replace(folder.id, 1, &manifest(&[("main.weft", &three)]), &to, 7).await.unwrap().expect("its revision");
    assert_eq!((switched.revision, switched.head), (2, to));
    // Forgetting it, too, goes only from the revision it read: a write
    // that landed after is never lost with the copy.
    assert!(!copies.remove(folder.id, 1).await.unwrap(), "a stale revision removes nothing");
    assert!(copies.get(folder.id).await.unwrap().is_some());
    assert!(copies.remove(folder.id, 2).await.unwrap());
    assert!(copies.list(project).await.unwrap().is_empty(), "the copy is gone");
}

/// A pruned version takes its runs out of the tree (they stay on record
/// until retention erases them); a project's removal drops its whole tree
/// (the store deletes it explicitly: a version left behind kept naming
/// stored files of a project that no longer existed, and the same id
/// registered again inherited a tree it never made).
#[weft_db_macros::test]
async fn deleting_versions_takes_their_runs_out_of_the_tree(pool: Pool) {
    let (store, projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let root = version(project, &manifest(&[("main.weft", "1")]), None, None, 1);
    let child = version(project, &manifest(&[("main.weft", "2")]), Some(&root.id), None, 2);
    versions.upsert_version(&root).await.unwrap();
    versions.upsert_version(&child).await.unwrap();
    let r1 = start_run(&store, project, &root.id, None, Some(&RunSpec::default()), 3).await;
    let r2 = start_run(&store, project, &child.id, None, Some(&RunSpec::default()), 4).await;
    for run in [r1, r2] {
        let written = store.append(run, &[RunEvent::RunCompleted { run_id: run, at_unix: 5 }], Then::Stays).await.unwrap();
        assert!(matches!(written, weft_journal::record::Appended::At(_)));
    }
    versions.delete_versions(project, std::slice::from_ref(&child.id)).await.unwrap();
    assert!(versions.run(r2).await.unwrap().is_none());
    assert!(versions.run(r1).await.unwrap().is_some());
    assert_eq!(versions.versions(project).await.unwrap().len(), 1);
    assert!(projects.remove(project).await.unwrap().is_some());
    assert!(versions.versions(project).await.unwrap().is_empty(), "removing the project drops its tree");
    assert!(versions.run(r1).await.unwrap().is_none(), "and the tree's runs with it");
}

/// A version's trigger count sums every writer's lane and names the
/// newest run across them.
#[weft_db_macros::test]
async fn trigger_runs_sum_the_lanes_and_name_the_newest_run(pool: Pool) {
    let (_store, _projects, versions) = setup(&pool).await;
    let project = Uuid::new_v4();
    let older = Uuid::now_v7();
    let newer = Uuid::now_v7();
    for (lane, runs, last) in [("a", 2_i64, newer), ("b", 3, older)] {
        pool.execute(
            "INSERT INTO version_runs (project_id, source_version, lane, runs, last_run) VALUES ($1, 'v1', $2, $3, $4)",
            &[&project, &lane, &runs, &last],
        )
        .await
        .unwrap();
    }
    let counted = versions.trigger_runs(project).await.unwrap();
    assert_eq!(counted.len(), 1);
    assert_eq!(counted["v1"].runs, 5);
    assert_eq!(counted["v1"].last, newer);
}

/// The stores a checkpoint writes through, under the project's lock, keep
/// two checkouts recording different edits at once apart: each is a
/// working copy with its own head, so both versions land, each becomes the
/// head of the checkout that recorded it, and both parent on the version
/// the two started from. Each side reads its copy's head, records the
/// version under it and moves that copy's head compare-and-set, the steps
/// the checkpoint action takes (`stamp_locked`, which needs a whole
/// dispatcher and is not driven here).
#[weft_db_macros::test]
async fn two_checkouts_recording_at_once_each_move_their_own_head(pool: Pool) {
    let (_, projects, versions) = setup(&pool).await;
    let copies = PostgresWorkingCopyStore::new(pool.clone());
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let base = version(project, &manifest(&[("main.weft", "0")]), None, None, 1);
    versions.upsert_version(&base).await.unwrap();
    let on_base = Head { head_version: Some(base.id.clone()), head_run: None };
    let mut folders = Vec::new();
    for (path, file) in [("/home/me/one", "1"), ("/home/me/two", "2")] {
        let folder = copies.register_folder(project, "box", path, &manifest(&[("main.weft", file)]), &on_base, 2).await.unwrap();
        folders.push(folder.id);
    }

    let checkpoint = |copy: Uuid| {
        let (pool, copies, versions) = (pool.clone(), &copies, &versions);
        async move {
            weft_dispatcher::lease::with_project_transition_lock(&pool, project, || async {
                let row = copies.get(copy).await?.expect("the copy");
                let recorded = version(project, &row.working, row.head.head_version.as_deref(), None, 3);
                assert!(versions.upsert_version(&recorded).await?, "a new version");
                let to = Head { head_version: Some(recorded.id.clone()), head_run: None };
                assert!(copies.move_head(copy, &row.head, &to, 3).await?, "its own head moved");
                Ok(recorded.id)
            })
            .await
            .unwrap()
        }
    };
    let (first, second) = tokio::join!(checkpoint(folders[0]), checkpoint(folders[1]));

    assert_ne!(first, second, "two edits are two versions");
    let tree = versions.versions(project).await.unwrap();
    for landed in [&first, &second] {
        let row = tree.iter().find(|v| &v.id == landed).expect("recorded, not just answered");
        assert_eq!(row.parent_id.as_deref(), Some(base.id.as_str()), "under the version both started from");
    }
    for (copy, landed) in [(folders[0], &first), (folders[1], &second)] {
        assert_eq!(copies.get(copy).await.unwrap().unwrap().head.head_version.as_ref(), Some(landed), "each checkout's head is its own version");
    }
}
