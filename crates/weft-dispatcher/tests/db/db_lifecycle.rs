//! Layer-3 tests for the dispatcher's OWN SQL, against a REAL Postgres.
//! The dispatcher's correctness-critical decisions (task stamping, the
//! atomic run birth, the lifecycle writes) live in SQL statements
//! the fake stores never execute, so a faked layer cannot catch their
//! bugs. These tests exercise the actual statements.
//!
//! Each test gets a fresh isolated database via `#[weft_db_macros::test]` (it reads
//! `$DATABASE_URL`, creates a random DB, drops it after). The schema is
//! the REAL migration path in production order: one pass over the
//! dispatcher's groups
//! (`app::DISPATCHER_GROUPS`), then the run store and project store wrap the pool,
//! the way a booting dispatcher does.

use serde_json::json;
use uuid::Uuid;
use weft_task_store::db::{Client as _, Pool, Session};

use weft_core::activation::ActivationKey;
use weft_core::instance::{InstanceId, Owner};
use weft_core::ProjectDefinition;
use weft_dispatcher::activation_store::{
    ActivationLifecycle, ActivationStoreOps, Claimant, DownCause, Forget, LifecycleWrite, PostgresActivationStore, ProjectStatus, SignalsGoing,
};
use weft_dispatcher::api::signal::{instance_waiting_tokens, signals_visible_to, waiting_fires};
use weft_task_store::parked_fires::{WaitsFor, WaitsOn};
use weft_dispatcher::parked_drain::{due_tokens, next_due};
use weft_journal::record::{Queued, Then};
use weft_journal::RunEvent;
use weft_task_store::parked_fires::{park, ParkAppend, ParkRefusal, Waiting};
use weft_dispatcher::store::postgres::PostgresStore;
use weft_dispatcher::store::{BakeStore, RunStore, SignalRegistration, SignalStore, TokenStore};
use weft_dispatcher::versions::VersionStoreOps;

const TENANT: &str = "tenant-1";

/// The real boot-time migration path: the dispatcher's groups in
/// dependency order, in one pass. Returns the run store + the project store, the two handles the
/// SQL under test goes through.
async fn setup(pool: &Pool) -> (PostgresStore, weft_dispatcher::ProjectStore) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let store = PostgresStore::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    (store, projects)
}

/// A minimal, empty-but-valid project definition for `id`.
fn empty_project(id: Uuid) -> ProjectDefinition {
    serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] }))
        .expect("minimal ProjectDefinition")
}

/// Register a project with `binary_hash` as its running image, the state
/// every enqueue-stamp read depends on.
async fn seed_project(
    projects: &weft_dispatcher::ProjectStore,
    id: Uuid,
    binary_hash: &str,
) {
    projects
        .register_with_hashes(
            empty_project(id),
            "db-rig",
            "",
            TENANT,
            Some(binary_hash),
            Some("def-1"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("register project");
}

/// The birth of run `run_id` of `project_id`, in `phase`, on image
/// `binary_hash`.
fn birth(run_id: weft_core::RunId, project_id: Uuid, phase: weft_core::context::Phase, binary_hash: &str) -> RunEvent {
    RunEvent::RunStarted { given: Default::default(),
        run_id, project_id, entry_node: "entry".into(), phase,
        definition_hash: Some("def-1".into()), binary_hash: Some(binary_hash.into()),
        source_version: (phase == weft_core::context::Phase::TriggerSetup).then(|| "source".into()),
        run_kind: weft_core::run::RunKind::Project, cut_from: None, seed: None, instance: None, stand_in: None,
        fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        settings: Default::default(),
    }
}

/// Queue the run `events` start (`events[0]` its birth), the way the
/// dispatcher starts a run by hand or a setup run.
async fn queue(store: &PostgresStore, events: &[RunEvent], for_activation: bool) -> anyhow::Result<bool> {
    store.queue_run(Queued {
        events, tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT, watch_end: false,
        stale: &[], spec: None, example: None, program: None,
    }, for_activation).await
}

/// A fire run of `project_id`, queued.
async fn queued_run(store: &PostgresStore, project_id: Uuid) -> weft_core::RunId {
    let run_id = weft_core::new_run_id();
    assert!(queue(store, &[birth(run_id, project_id, weft_core::context::Phase::Fire, "bin-A")], false).await.unwrap());
    run_id
}

/// End run `run_id`, which nobody drives, completed.
async fn complete(store: &PostgresStore, run_id: weft_core::RunId) -> RunEvent {
    let completed = RunEvent::RunCompleted { run_id, at_unix: 2 };
    let written = store.append(run_id, std::slice::from_ref(&completed), Then::Stays).await.unwrap();
    assert!(matches!(written, weft_journal::record::Appended::At(_)), "{written:?}");
    completed
}

/// One column of run `run_id`'s row.
async fn run_column<T>(pool: &Pool, column: &str, run_id: weft_core::RunId) -> Option<T>
where
    T: for<'r> weft_task_store::db::FromSql<'r> + Send,
{
    pool.scalar_opt(&format!("SELECT {column} FROM run WHERE run_id = $1"), &[&run_id])
        .await
        .unwrap()
}

/// A minimal entry-signal registration.
fn entry_signal(token: &str, project_id: Uuid) -> SignalRegistration {
    SignalRegistration {
        instance: None,
        activation_trigger: None,
        source_version: None,
        setup_run_id: None,
        program: None,
        token: token.to_string(),
        tenant_id: TENANT.to_string(),
        project_id,
        run_id: None,
        node_id: "feed".to_string(),
        is_resume: false,
        spec_json: "{}".to_string(),
        consumer_kind: None,
        tags: Vec::new(),
        consumer_payload: None,
        surface_kind: "task_callback".to_string(),
        mount_path: None,
        mount_methods: Vec::new(),
        auth_kind: "none".to_string(),
        auth_config: None,
        kind_state: json!({}),
        kind_state_seq: 0,
        access_id: None,
        port_snapshot: None,
        holds: false,
        generation: 0,
    }
}

// ----- a run keeps its image -----------------------------------------------

/// A run keeps the image it was born on across project edits: delivery
/// reads its row, and so does every claim that carries it on after an
/// answer.
#[weft_db_macros::test]
async fn a_queued_run_keeps_the_image_it_was_born_on(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run_id = weft_core::new_run_id();
    let start = birth(run_id, id, weft_core::context::Phase::Fire, "bin-A");
    seed_project(&projects, id, "bin-B").await;
    assert!(queue(&store, &[start], false).await.unwrap());
    assert_eq!(run_column::<String>(&pool, "binary_hash", run_id).await.as_deref(), Some("bin-A"));
    assert_eq!(run_column::<String>(&pool, "state", run_id).await.as_deref(), Some("queued"));
}

// ----- atomic run birth ------------------------------------------

#[weft_db_macros::test]
async fn registered_sources_belong_to_the_exact_program(pool: Pool) {
    let (_, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    let implementations = weft_dispatcher::project_store::ProgramCode::default();
    let source = std::collections::BTreeMap::from([("main.weft".into(), "original-file".into())]);
    projects.register_with_hashes(empty_project(id), "sources", "", TENANT,
        Some("binary"), Some("graph"), None, None, Some(&implementations), Some(&weft_dispatcher::project_store::RunningSource { manifest: &source, inputs: None }), None, None).await.unwrap();
    let program = projects.running_program_identity(id).await.unwrap().unwrap();
    assert_eq!(projects.program_source(id, &program).await.unwrap(), source);
    // A later build of other code moves the running program, and the
    // old identity no longer answers for it.
    projects.register_with_hashes(empty_project(id), "sources", "", TENANT,
        Some("different-binary"), Some("graph"), None, None, Some(&implementations), None, None, None).await.unwrap();
    assert!(projects.program_source(id, &program).await.is_err());
    let changed = weft_core::project::hash::ProgramIdentity { binary_hash: "different-binary".into(), ..program };
    assert!(projects.program_source(id, &changed).await.is_err(), "changing code cannot relabel old sources");
}

/// The program identity every setup birth here names (`trigger_setup_birth`).
fn setup_program() -> weft_core::project::hash::ProgramIdentity {
    weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-1".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    }
}

/// The birth of trigger setup `run_id` of project `id`, from
/// version `source`.
fn trigger_setup_birth(id: Uuid, run_id: Uuid) -> RunEvent {
    birth(run_id, id, weft_core::context::Phase::TriggerSetup, "bin-A")
}

#[weft_db_macros::test]
async fn pruning_a_source_waits_for_setup_and_removes_its_unused_bake(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let versions = weft_dispatcher::versions::PostgresVersionStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    pool.execute("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)", &[&id]).await.unwrap();
    let run_id = Uuid::new_v4();
    let birth = trigger_setup_birth(id, run_id);
    queue(&store, std::slice::from_ref(&birth), false).await.unwrap();
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err());
    let complete = complete(&store, run_id).await;
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err(), "publication still owns this source");
    let bake = weft_dispatcher::store::TriggerBake::from_events(&[birth, complete], &setup_program()).unwrap().unwrap();
    store.finish_trigger_setup(run_id, Some(&bake)).await.unwrap();
    versions.delete_versions(id, &["source".into()]).await.unwrap();
    assert!(store.trigger_bakes(id, None).await.unwrap().read.is_empty());
    assert!(versions.version(id, "source").await.unwrap().is_none());
}

/// A prior bake that no longer reads never blocks a fresh capture: the
/// setup lands, its capture replacing the prior, and the answer says the
/// prior was replaced, naming its row, so what it held for other
/// triggers is not lost unseen.
#[weft_db_macros::test]
async fn a_fresh_capture_replaces_a_prior_bake_that_no_longer_reads(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    pool.execute("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)", &[&id]).await.unwrap();
    let run_id = Uuid::new_v4();
    let birth = trigger_setup_birth(id, run_id);
    queue(&store, std::slice::from_ref(&birth), false).await.unwrap();
    let complete = complete(&store, run_id).await;
    let bake = weft_dispatcher::store::TriggerBake::from_events(&[birth, complete], &setup_program()).unwrap().unwrap();
    pool.execute(
        "INSERT INTO trigger_bake (project_id, instance_id, program_hash, bake_json, bake_json_version, source_version) \
         VALUES ($1, NULL, $2, '{\"nonsense\": true}', $3, 'source')",
        &[&id, &bake.program.digest(), &weft_dispatcher::store::TriggerBake::STORED.version()],
    )
    .await
    .unwrap();
    // Another program's bake, which reads, beside it.
    let mut elsewhere = bake.clone();
    elsewhere.program.binary_hash = "bin-B".into();
    pool.execute(
        "INSERT INTO trigger_bake (project_id, instance_id, program_hash, bake_json, bake_json_version, source_version) \
         VALUES ($1, NULL, $2, $3, $4, 'source')",
        &[&id, &elsewhere.program.digest(), &serde_json::to_string(&elsewhere).unwrap(), &weft_dispatcher::store::TriggerBake::STORED.version()],
    )
    .await
    .unwrap();
    let before = store.trigger_bakes(id, None).await.unwrap();
    assert_eq!(before.read.len(), 1, "the row that reads is read past the one that does not");
    assert_eq!(before.read[0].program, elsewhere.program);
    let unreadable = before.unreadable_for(&bake.program).expect("read past, and named");
    assert!(unreadable.error.contains(&weft_dispatcher::store::TriggerBake::row(id, None, &bake.program.digest())), "{}", unreadable.error);
    assert!(store.is_trigger_setup_pending(run_id).await.unwrap());
    let lost = store.finish_trigger_setup(run_id, Some(&bake)).await.unwrap().expect("the prior did not read");
    assert!(lost.error.contains(&weft_dispatcher::store::TriggerBake::row(id, None, &bake.program.digest())), "{}", lost.error);
    assert!(!store.is_trigger_setup_pending(run_id).await.unwrap(), "the setup landed");
    let saved = store.trigger_bakes(id, None).await.unwrap();
    assert!(saved.unreadable.is_empty());
    let saved: Vec<_> = saved.read.into_iter().filter(|saved| saved.program == bake.program).collect();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].run_id, run_id, "the fresh capture replaced the prior");
}

/// The feed trigger's shared activation, and an entry signal it governs.
fn feed_key() -> ActivationKey {
    ActivationKey::new("feed", Owner::Shared)
}

fn governed_entry(token: &str, project_id: Uuid, setup: Uuid) -> SignalRegistration {
    let mut entry = entry_signal(token, project_id);
    entry.activation_trigger = Some("feed".into());
    entry.setup_run_id = Some(setup);
    entry
}

#[weft_db_macros::test]
async fn a_rearm_claim_refuses_a_trigger_that_left_active(pool: Pool) {
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active), Claimant::Anyone, Uuid::new_v4())
        .await.unwrap(), Err(weft_dispatcher::activation_store::ClaimRefused::NotExpected)), "a re-arm never creates a trigger that was never activated");
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::holding(), false, None).await.unwrap().expect("owned");
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active), Claimant::Anyone, Uuid::new_v4())
        .await.unwrap(), Err(weft_dispatcher::activation_store::ClaimRefused::NotExpected)), "a deactivate between the look and the claim wins: the re-arm refuses");
    let again = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], again, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    activations.end_activating(id, again, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    assert!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active), Claimant::Anyone, Uuid::new_v4())
        .await.unwrap().is_ok(), "still Active: the re-arm claims");
}

#[weft_db_macros::test]
async fn an_instances_wipe_takes_its_row_and_its_signals_in_one_write(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    let mut entry = governed_entry("ada-wiped-entry", id, setup_run_id);
    entry.instance = Some(ada.clone());
    store.signal_insert(&entry).await.unwrap();
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");

    let written = activations
        .set_lifecycle_guarded(id, std::slice::from_ref(&ada_key), &ActivationLifecycle::refused(), SignalsGoing::Activations, Forget::InstanceRows, DownCause::Person, None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { unlisten, .. } = written else { panic!("applied: {written:?}") };
    assert_eq!(unlisten.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["ada-wiped-entry"],
        "the signals are handed back for the listener cleanup");
    assert!(activations.list(id).await.unwrap().is_empty(), "the instance's wiped row is gone");
    assert!(store.signal_get("ada-wiped-entry").await.unwrap().is_none(), "and its signal went with it");
}

/// A park keeps the signals and leaves them listening: what they hear
/// waits for the trigger to be back on, so the write hands nothing back
/// for the listener to let go of.
#[weft_db_macros::test]
async fn a_park_keeps_its_signals_listening(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    store.signal_insert(&governed_entry("parked-entry", id, setup_run_id)).await.unwrap();
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");

    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { unlisten, .. } = written else { panic!("applied: {written:?}") };
    assert!(unlisten.is_empty(), "a parked trigger goes on listening: {unlisten:?}");
    assert!(store.signal_get("parked-entry").await.unwrap().is_some(), "and its signal stays stored");
}

/// A door that refuses takes the place an entry signal keeps in what it
/// reads in the write that sets the door, and only while the row is still
/// at the version that place was computed from: a wake that claimed in
/// between refuses the whole write, the door included, so it is asked
/// again.
#[weft_db_macros::test]
async fn a_refusing_door_takes_the_place_only_from_the_row_it_was_computed_from(pool: Pool) {
    use weft_dispatcher::store::FreshPlace;
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    activate_with_entry(&activations, &store, id, "polled-entry").await;
    let at = store.signal_get("polled-entry").await.unwrap().expect("armed").kind_state_seq;
    let refused = ActivationLifecycle::refused();

    let stale = [FreshPlace { token: "polled-entry".into(), kind_state_seq: at - 1, kind_state: json!({ "fresh": true }) }];
    let moved = activations
        .set_lifecycle_guarded(id, &[feed_key()], &refused, SignalsGoing::Places(&stale), Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();
    assert!(matches!(moved, LifecycleWrite::StateMoved), "{moved:?}");
    assert_eq!(activations.list(id).await.unwrap()[0].lifecycle.status, ProjectStatus::Active, "nothing was written, the door included");
    assert_eq!(store.signal_get("polled-entry").await.unwrap().unwrap().kind_state_seq, at);

    let fresh = [FreshPlace { token: "polled-entry".into(), kind_state_seq: at, kind_state: json!({ "fresh": true }) }];
    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &refused, SignalsGoing::Places(&fresh), Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { unlisten, .. } = written else { panic!("applied: {written:?}") };
    assert!(unlisten.is_empty(), "the rows stay for the next activation");
    let row = store.signal_get("polled-entry").await.unwrap().expect("kept");
    assert_eq!(row.kind_state, json!({ "fresh": true }), "the place is gone");
    assert_eq!(row.kind_state_seq, at + 1, "and a wake's claim at the old version fails");
    assert_eq!(activations.list(id).await.unwrap()[0].lifecycle.status, ProjectStatus::Inactive);
}

/// An infra take-down issues its commands in the write that takes the
/// triggers down, and marks them with the first: nothing claims a command
/// while a trigger reading its copies still listens, and no trigger is
/// down for a command that does not exist. An upgrade of the same copies
/// already in flight refuses the whole write.
#[weft_db_macros::test]
async fn an_infra_take_down_issues_its_commands_with_the_triggers(pool: Pool) {
    use weft_dispatcher::infra_lifecycle_command::{TakeDownCommands, TakeDownWork, Teardown, UpgradeWork};
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    let commands = |work| TakeDownCommands { tenant_id: TENANT.into(), issued_by_replica: "disp-1".into(), work };
    let shared = weft_core::instance::Copies::Shared;
    let down = ActivationLifecycle::down(&weft_core::TakeDown::DEFAULT, 0);

    let upgrade = UpgradeWork {
        nodes: Vec::new(),
        running: vec!["db".into()],
        take_down: weft_core::TakeDown::DEFAULT,
        binary_hash: None,
        definition_hash: None,
        infra_hash: None,
        waits_on_tokens: Vec::new(),
        stopped: false,
    };
    let in_flight = commands(TakeDownWork::Upgrade { instance: None, work: upgrade.clone() });
    let weft_dispatcher::infra_lifecycle_command::Issued::Commands(first_upgrade) = in_flight.issue(&pool, id).await.unwrap().expect("the project is there") else {
        panic!("an upgrade with nothing in flight is issued")
    };
    let refused = activations
        .set_lifecycle_guarded(id, &[feed_key()], &down, SignalsGoing::Kept, Forget::Nothing, DownCause::Infra(&in_flight), None)
        .await
        .unwrap();
    assert!(matches!(refused, LifecycleWrite::UpgradeInFlight(command) if command == first_upgrade[0]), "{refused:?}");
    let row = activations.list(id).await.unwrap().remove(0);
    assert_eq!((row.lifecycle.status, row.lifecycle.went_down), (ProjectStatus::Active, None), "nothing was written");
    let upgrades: i64 = pool.scalar("SELECT COUNT(*) FROM infra_lifecycle_command WHERE project_id = $1 AND verb = 'upgrade'", &[&id])
        .await
        .unwrap();
    assert_eq!(upgrades, 1, "and nothing issued");

    let stop = commands(TakeDownWork::Teardown {
        nodes: vec!["db".into(), "cache".into()],
        copies: shared,
        teardown: Teardown::Stop { force: false },
        waits: true,
    });
    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &down, SignalsGoing::Kept, Forget::Nothing, DownCause::Infra(&stop), None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { commands: ids, .. } = written else { panic!("applied: {written:?}") };
    assert_eq!(ids.len(), 2, "one command per node named");
    assert!(ids[0] < ids[1]);
    let row = activations.list(id).await.unwrap().remove(0);
    assert_eq!(row.lifecycle.status, ProjectStatus::Inactive);
    assert_eq!(row.lifecycle.went_down, Some(weft_broker_client::activation::WentDown::Infra { first_command: ids[0] }));
    let drains: Vec<(String, bool)> =
        pool.all("SELECT node_id, drains FROM infra_lifecycle_command WHERE id = ANY($1) ORDER BY id", &[&ids]).await.unwrap();
    assert_eq!(drains, vec![("db".to_string(), true), ("cache".to_string(), true)], "issued as asked, in the order named");

    // One a person took down after it was listed stays theirs: never
    // marked to come back with the infra.
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();
    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &down, SignalsGoing::Kept, Forget::Nothing, DownCause::Infra(&stop), None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { keys, commands: ids, .. } = written else { panic!("applied: {written:?}") };
    assert!(keys.is_empty(), "nothing on to take down: {keys:?}");
    assert_eq!(ids.len(), 2, "the commands are issued all the same");
    assert_eq!(activations.list(id).await.unwrap().remove(0).lifecycle.went_down, None, "the person's take-down keeps no mark");

    // A project being removed issues nothing on its own.
    pool.execute("UPDATE project SET removing = TRUE WHERE id = $1", &[&id]).await.unwrap();
    assert_eq!(stop.issue(&pool, id).await.unwrap(), None);
    let refused = activations
        .set_lifecycle_guarded(id, &[feed_key()], &down, SignalsGoing::Kept, Forget::Nothing, DownCause::Infra(&stop), None)
        .await
        .unwrap();
    assert!(matches!(refused, LifecycleWrite::NotFound), "nor with its triggers: {refused:?}");
}

/// The feed switched on with one entry signal, `token`.
async fn activate_with_entry(activations: &PostgresActivationStore, store: &PostgresStore, id: Uuid, token: &str) {
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    store.signal_insert(&governed_entry(token, id, setup_run_id)).await.unwrap();
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
}

/// A take-down hands back its triggers' entry tokens as they stood before
/// it, even when it deleted their signals: what a wait for their runs
/// counts at the workers' door. An infra command keeps them for its own
/// wait.
#[weft_db_macros::test]
async fn a_take_down_keeps_the_tokens_its_wait_counts(pool: Pool) {
    use weft_dispatcher::infra_lifecycle_command::{TakeDownCommands, TakeDownWork, Teardown};
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    activate_with_entry(&activations, &store, id, "door-token").await;
    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::refused(), SignalsGoing::Activations, Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { tokens, unlisten, .. } = written else { panic!("applied: {written:?}") };
    assert_eq!(tokens, vec!["door-token".to_string()]);
    assert_eq!(unlisten.len(), 1, "the signal itself went");
    assert!(store.signal_get("door-token").await.unwrap().is_none());

    activate_with_entry(&activations, &store, id, "next-token").await;
    let stop = TakeDownCommands {
        tenant_id: TENANT.into(),
        issued_by_replica: "disp-1".into(),
        work: TakeDownWork::Teardown { nodes: vec!["db".into()], copies: weft_core::instance::Copies::Shared, teardown: Teardown::Stop { force: false }, waits: true },
    };
    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::refused(), SignalsGoing::Activations, Forget::Nothing, DownCause::Infra(&stop), None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { commands, .. } = written else { panic!("applied: {written:?}") };
    let kept: Vec<String> =
        pool.scalar("SELECT waits_on_tokens FROM infra_lifecycle_command WHERE id = $1", &[&commands[0]]).await.unwrap();
    assert_eq!(kept, vec!["next-token".to_string()], "the stop's wait counts the trigger it took down");

    activate_with_entry(&activations, &store, id, "upgrade-token").await;
    let upgrade = TakeDownCommands {
        tenant_id: TENANT.into(),
        issued_by_replica: "disp-1".into(),
        work: TakeDownWork::Upgrade {
            instance: None,
            work: weft_dispatcher::infra_lifecycle_command::UpgradeWork {
                nodes: Vec::new(),
                running: vec!["db".into()],
                take_down: weft_core::TakeDown::DEFAULT,
                binary_hash: None,
                definition_hash: None,
                infra_hash: None,
                waits_on_tokens: Vec::new(),
                stopped: false,
            },
        },
    };
    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::refused(), SignalsGoing::Activations, Forget::Nothing, DownCause::Infra(&upgrade), None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { commands, .. } = written else { panic!("applied: {written:?}") };
    let weft_task_store::db::JsonText(work): weft_task_store::db::JsonText<serde_json::Value> =
        pool.scalar("SELECT spec_json FROM infra_lifecycle_command WHERE id = $1", &[&commands[0]]).await.unwrap();
    assert_eq!(work["waits_on_tokens"], json!(["upgrade-token"]), "an upgrade's stop leg counts the trigger it took down");
}

/// Down with a door that holds what arrives until `deadline_unix`, out of
/// sight (`--door hide --door-for <minutes>`, the deadline set by hand).
fn hidden_until(deadline_unix: i64) -> ActivationLifecycle {
    let take_down = weft_core::TakeDown {
        door: weft_core::Door::Hide,
        door_for_minutes: Some(1),
        held: weft_core::Held::Keep,
        running: weft_core::Running::Wait,
    };
    ActivationLifecycle { fires_deadline_unix: Some(deadline_unix), ..ActivationLifecycle::down(&take_down, 0) }
}

/// The listener's side of closing door windows, answered by a test: it
/// writes down what it was told to let go of, keeps every one of them
/// with `keeps` when set, and gives every entry signal it is asked about
/// the fresh place `place` when set (none otherwise), or cannot be
/// reached for places when `unreachable`.
#[derive(Default)]
struct Doors {
    asked: std::sync::Mutex<Vec<String>>,
    keeps: Option<&'static str>,
    place: Option<serde_json::Value>,
    unreachable: bool,
}

#[async_trait::async_trait]
impl weft_dispatcher::reaper::DoorWindowListener for Doors {
    async fn let_go(&self, signals: &[SignalRegistration]) -> anyhow::Result<Vec<(String, String)>> {
        self.asked.lock().unwrap().extend(signals.iter().map(|s| s.token.clone()));
        Ok(match self.keeps {
            Some(why) => signals.iter().map(|s| (s.token.clone(), why.to_string())).collect(),
            None => Vec::new(),
        })
    }

    async fn fresh_places(&self, signals: &[SignalRegistration]) -> anyhow::Result<Vec<weft_dispatcher::store::FreshPlace>> {
        anyhow::ensure!(!self.unreachable, "the listener cannot be reached");
        Ok(match &self.place {
            Some(place) => signals
                .iter()
                .map(|s| weft_dispatcher::store::FreshPlace { token: s.token.clone(), kind_state_seq: s.kind_state_seq, kind_state: place.clone() })
                .collect(),
            None => Vec::new(),
        })
    }
}

/// A door window that ends takes the place its entry signals keep in what
/// they read in the write that closes it, only while each row is still at
/// the version its place was computed from, and only for the windows that
/// close: a place computed from a row a wake moved since closes nothing,
/// and one for a trigger whose window is still open is not written.
#[weft_db_macros::test]
async fn a_window_that_ends_takes_the_places_with_the_close(pool: Pool) {
    use weft_dispatcher::activation_store::{close_door_windows, door_windows_over, WindowsClosed};
    use weft_dispatcher::store::FreshPlace;
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (over, open) = (feed_key(), ActivationKey::new("reads", Owner::Shared));
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[over.clone(), open.clone()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    store.signal_insert(&governed_entry("feed-entry", id, setup_run_id)).await.unwrap();
    let mut reads = entry_signal("reads-entry", id);
    reads.activation_trigger = Some("reads".into());
    reads.setup_run_id = Some(setup_run_id);
    reads.node_id = "reads".into();
    store.signal_insert(&reads).await.unwrap();
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    for (key, deadline) in [(&over, 100), (&open, i64::MAX / 2)] {
        activations
            .set_lifecycle_guarded(id, std::slice::from_ref(key), &hidden_until(deadline), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None)
            .await
            .unwrap();
    }
    let seq = |token: &'static str| {
        let store = &store;
        async move { store.signal_get(token).await.unwrap().expect("kept").kind_state_seq }
    };
    let (feed_at, reads_at) = (seq("feed-entry").await, seq("reads-entry").await);
    let place = |token: &str, at: i64| FreshPlace { token: token.into(), kind_state_seq: at, kind_state: json!({ "fresh": true }) };
    let keys = [over.clone(), open.clone()];

    let moved = close_door_windows(&pool, id, &keys, &[place("feed-entry", feed_at - 1)]).await.unwrap();
    assert_eq!(moved, WindowsClosed::StateMoved);
    assert_eq!(door_windows_over(&pool).await.unwrap(), vec![(id, over.clone())], "nothing closed");

    let closed = close_door_windows(&pool, id, &keys, &[place("feed-entry", feed_at), place("reads-entry", reads_at)]).await.unwrap();
    assert_eq!(closed, WindowsClosed::Closed(1));
    let feed = store.signal_get("feed-entry").await.unwrap().expect("kept");
    assert_eq!((feed.kind_state, feed.kind_state_seq), (json!({ "fresh": true }), feed_at + 1), "the closed window's place is gone");
    let reads = store.signal_get("reads-entry").await.unwrap().expect("kept");
    assert_eq!((reads.kind_state, reads.kind_state_seq), (json!({}), reads_at), "the open window keeps its place");
}

/// The sweep closes a window with the places the listener gives, and a
/// project whose places cannot be had keeps its window open and fails the
/// pass, naming it.
#[weft_db_macros::test]
async fn the_sweep_closes_a_window_only_with_its_places(pool: Pool) {
    use weft_dispatcher::activation_store::door_windows_over;
    use weft_dispatcher::reaper::close_door_windows_over;
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    activate_with_entry(&activations, &store, id, "feed-entry").await;
    activations
        .set_lifecycle_guarded(id, &[feed_key()], &hidden_until(100), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();

    let unreachable = Doors { unreachable: true, ..Doors::default() };
    let failed = close_door_windows_over(&pool, &unreachable).await.unwrap_err();
    assert!(format!("{failed:#}").contains(&id.to_string()), "names the project: {failed:#}");
    assert_eq!(door_windows_over(&pool).await.unwrap(), vec![(id, feed_key())], "its window stays to close");

    let listener = Doors { place: Some(json!({ "fresh": true })), ..Doors::default() };
    assert_eq!(close_door_windows_over(&pool, &listener).await.unwrap(), vec![id]);
    assert!(door_windows_over(&pool).await.unwrap().is_empty());
    assert_eq!(store.signal_get("feed-entry").await.unwrap().expect("kept").kind_state, json!({ "fresh": true }));
}

/// A door that holds for a while takes work until its window ends, then
/// refuses: the close flips only the windows that are over, keeps the row
/// (so `weft activate` brings it back), and says when the next window
/// still open ends.
#[weft_db_macros::test]
async fn a_door_stops_taking_work_once_its_window_ends(pool: Pool) {
    use weft_dispatcher::activation_store::{close_door_windows, door_windows_over, next_door_window_end, WindowsClosed};
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (over, open) = (feed_key(), ActivationKey::new("reads", Owner::Shared));
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[over.clone(), open.clone()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    for (key, deadline) in [(&over, 100), (&open, i64::MAX / 2)] {
        activations
            .set_lifecycle_guarded(id, std::slice::from_ref(key), &hidden_until(deadline), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None)
            .await
            .unwrap();
    }
    assert_eq!(next_door_window_end(&pool).await.unwrap(), Some(100));

    // The windows are over by the database's clock: one in the past, one
    // far ahead.
    assert_eq!(door_windows_over(&pool).await.unwrap(), vec![(id, over.clone())]);
    assert_eq!(
        close_door_windows(&pool, id, &[over.clone(), open.clone()], &[]).await.unwrap(),
        WindowsClosed::Closed(1),
        "only the window that is over closes"
    );
    assert!(door_windows_over(&pool).await.unwrap().is_empty(), "taking no work, it is over no longer");
    assert_eq!(close_door_windows(&pool, id, std::slice::from_ref(&over), &[]).await.unwrap(), WindowsClosed::Closed(0), "a window closes once");
    let listed = activations.list(id).await.unwrap();
    let lifecycle = |key: &ActivationKey| listed.iter().find(|a| &a.key == key).expect("kept").lifecycle.clone();
    assert!(!lifecycle(&over).accepting_fires, "the window that is over takes no more work");
    assert_eq!(lifecycle(&over).mode(), weft_core::activation::ActivationMode::Down(weft_core::Door::Refuse), "its door refuses now");
    assert!(lifecycle(&open).accepting_fires, "the window still open takes work");
    assert_eq!(next_door_window_end(&pool).await.unwrap(), Some(i64::MAX / 2));
}

/// A door window closes at its time even when the listener keeps one of
/// its signals: the window closes, nothing looks again for it, the signal is
/// marked kept and the refusal written down for `weft status`. The next
/// pass tells the listener again about the kept signal, and once it lets
/// go both are cleared.
#[weft_db_macros::test]
async fn a_refused_let_go_still_ends_the_window_and_is_written_down(pool: Pool) {
    use weft_dispatcher::activation_store::{door_windows_over, next_door_window_end};
    use weft_dispatcher::reaper::close_door_windows_over;
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (key, later) = (feed_key(), ActivationKey::new("reads", Owner::Shared));
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[key.clone(), later.clone()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    store.signal_insert(&governed_entry("tok-feed", id, setup_run_id)).await.unwrap();
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    let window_over = hidden_until(100);
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&key), &window_over, SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None).await.unwrap();

    let kept = Doors { keeps: Some("the listener is down"), ..Doors::default() };
    assert_eq!(close_door_windows_over(&pool, &kept).await.unwrap(), vec![id]);
    assert!(door_windows_over(&pool).await.unwrap().is_empty(), "the window closed whatever the listener answered");
    assert_eq!(next_door_window_end(&pool).await.unwrap(), None, "nothing to look at again");
    let marked = || {
        let pool = pool.clone();
        async move {
            pool.scalar::<Option<String>>("SELECT let_go_failed FROM signal WHERE token = 'tok-feed'", &[]).await.unwrap()
        }
    };
    assert_eq!(marked().await.as_deref(), Some("the listener is down"), "the kept signal is marked with why");
    let written: Vec<(String, String)> = pool.all("SELECT callee, error FROM unanswered_call", &[]).await.unwrap();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].0, format!("let-go:{id}"), "written down for the project whose signal was kept, not as the loop failing");
    assert!(written[0].1.contains("the listener is down") && !written[0].1.contains("tok-feed"), "says why, never the token: {}", written[0].1);

    // The next pass tells the listener again about the kept signal; once
    // it lets go, the mark and the failure go.
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&later), &window_over, SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None).await.unwrap();
    let let_go = Doors::default();
    assert_eq!(close_door_windows_over(&pool, &let_go).await.unwrap(), vec![id]);
    assert!(let_go.asked.lock().unwrap().contains(&"tok-feed".to_string()), "the kept signal is let go of again");
    assert_eq!(marked().await, None);
    let left: i64 = pool.scalar("SELECT count(*) FROM unanswered_call", &[]).await.unwrap();
    assert_eq!(left, 0);
}

#[weft_db_macros::test]
async fn activation_cleanup_cannot_finish_or_wipe_a_newer_activation(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], first, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), None, Claimant::Anyone, Uuid::new_v4()).await.unwrap(),
        Err(weft_dispatcher::activation_store::ClaimRefused::Claimed)), "one activation of a trigger at a time");
    store.signal_insert(&governed_entry("old-entry", id, first)).await.unwrap();
    let removed = activations.end_activating(id, first, &ActivationLifecycle::refused(), true, None).await.unwrap().unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].token, "old-entry");
    assert!(store.signal_get("old-entry").await.unwrap().is_none());
    assert!(queue(&store, &[trigger_setup_birth(id, first)], true).await.is_err(),
        "a cancelled activation cannot later start its setup");
    assert!(crate::events(store.events_log(first).await.unwrap()).is_empty());

    let second = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], second, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    store.signal_insert(&governed_entry("new-entry", id, second)).await.unwrap();
    let program = weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-2".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    };
    assert!(!activations.record_activation_source(id, first, &program, "first-source", &Default::default()).await.unwrap(),
        "the ended activation cannot record over the newer one");
    assert!(activations.record_activation_source(id, second, &program, "second-source", &Default::default()).await.unwrap());
    for to in [ActivationLifecycle::refused(), ActivationLifecycle::active()] {
        assert!(activations.end_activating(id, first, &to, true, None).await.unwrap().is_none());
        assert!(store.signal_get("new-entry").await.unwrap().is_some());
        let row = activations.list(id).await.unwrap().remove(0);
        assert_eq!(row.lifecycle.activating_run_id, Some(second));
        assert_eq!(row.source_version.as_deref(), Some("second-source"));
    }
    assert!(activations.end_activating(id, second, &ActivationLifecycle::active(), false, None).await.unwrap().is_some());
    let row = activations.list(id).await.unwrap().remove(0);
    assert_eq!((row.lifecycle.status, row.source_version.as_deref()), (ProjectStatus::Active, Some("second-source")));
}

/// A trigger's cut is recorded by the setup that turned it on, per
/// trigger and owner, only by the activation that still owns the row; it
/// stays while the trigger is down (what it depends on is what brings it
/// back), and a failed activation puts it back as it was.
#[weft_db_macros::test]
async fn a_triggers_cut_is_recorded_kept_while_down_and_restored(pool: Pool) {
    use weft_dispatcher::activation_store::RecordedCut;
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = ActivationKey::new("feed", Owner::Instance(InstanceId::new("ada").unwrap()));
    let program = setup_program();
    let cut = RecordedCut { fingerprint: "cut-1".into(), depends_on: vec!["db".into(), "one.bridge".into()] };
    let cuts = std::collections::BTreeMap::from([("feed".to_string(), cut.clone())]);
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key(), ada.clone()], first, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claimed");
    assert!(!activations.record_activation_source(id, Uuid::new_v4(), &program, "v1", &cuts).await.unwrap(), "only the owning setup records");
    assert!(activations.record_activation_source(id, first, &program, "v1", &cuts).await.unwrap());
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    let cut_of = |rows: &[weft_dispatcher::activation_store::Activation], key: &ActivationKey| {
        rows.iter().find(|a| &a.key == key).expect("a row").cut.clone()
    };
    let rows = activations.list(id).await.unwrap();
    assert_eq!(cut_of(&rows, &feed_key()), Some(cut.clone()));
    assert_eq!(cut_of(&rows, &ada), Some(cut.clone()), "every owner of the trigger gets its cut");

    activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();
    let rows = activations.list(id).await.unwrap();
    assert_eq!(cut_of(&rows, &feed_key()), Some(cut.clone()), "kept while down");
    assert!(rows.iter().find(|a| a.key == feed_key()).unwrap().program.is_none(), "what it fired goes with it");

    let again = Uuid::new_v4();
    let previous = activations.try_begin_activating(id, &[feed_key()], again, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claimed");
    let moved = std::collections::BTreeMap::from([("feed".to_string(), RecordedCut { fingerprint: "cut-2".into(), depends_on: Vec::new() })]);
    assert!(activations.record_activation_source(id, again, &program, "v2", &moved).await.unwrap());
    assert!(activations.restore(id, again, &previous).await.unwrap());
    assert_eq!(cut_of(&activations.list(id).await.unwrap(), &feed_key()), Some(cut), "a failed activation puts the cut back");
}

/// The work held on a trigger is the events queued on its signals and
/// the runs it fired that are paused: counted for it alone, its paused
/// runs moved onto a new build when a resync keeps them, and its queued
/// events dropped when a take-down drops what is held, its signals kept.
#[weft_db_macros::test]
async fn the_work_held_on_a_trigger_is_counted_moved_and_dropped(pool: Pool) {
    use weft_dispatcher::store::postgres::{drop_held_arrivals, held_by_activation, held_on, move_paused_runs};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    seed_parked_signal(&store, "feed-tok", id).await;
    let feed = ActivationKey::new("trigger-feed-tok", Owner::Shared);
    let other = ActivationKey::new("other", Owner::Shared);
    assert!(matches!(park(&pool, "feed-tok", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked));
    let paused = queued_run(&store, id).await;
    pool.execute("UPDATE run SET state = 'parked', fired_by = 'trigger-feed-tok' WHERE run_id = $1", &[&paused])
        .await
        .unwrap();
    let held = held_on(&pool, id, std::slice::from_ref(&feed)).await.unwrap();
    assert_eq!((held.arrivals, held.paused), (1, 1));
    assert!(held_on(&pool, id, std::slice::from_ref(&other)).await.unwrap().is_empty(), "another trigger holds nothing");
    assert_eq!(held_by_activation(&pool, id).await.unwrap().get(&feed), Some(&held));

    let program = weft_journal::record::RunsProgram::of("def-1", id, &empty_project(id)).unwrap();
    assert_eq!(move_paused_runs(&pool, &[paused], &program, "bin-B", "v2").await.unwrap(), 1);
    assert_eq!(run_column::<String>(&pool, "binary_hash", paused).await.as_deref(), Some("bin-B"));
    assert_eq!(run_column::<String>(&pool, "source_version", paused).await.as_deref(), Some("v2"));
    assert_eq!(move_paused_runs(&pool, &[paused], &program, "bin-B", "v2").await.unwrap(), 0, "a run on the build already is not moved again");
    let ended = queued_run(&store, id).await;
    assert_eq!(move_paused_runs(&pool, &[ended], &program, "bin-B", "v2").await.unwrap(), 0, "only a paused run moves");

    assert_eq!(drop_held_arrivals(&pool, id, std::slice::from_ref(&feed)).await.unwrap(), 1);
    assert_eq!(held_on(&pool, id, std::slice::from_ref(&feed)).await.unwrap().arrivals, 0);
    assert!(store.signal_get("feed-tok").await.unwrap().is_some(), "the signal stays");
}

/// A move that moves no run keeps no cut: the cut is kept only once a
/// run it moved names it.
#[weft_db_macros::test]
async fn a_move_that_moves_nothing_keeps_no_cut(pool: Pool) {
    use weft_dispatcher::store::postgres::move_paused_runs;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let program = weft_journal::record::RunsProgram::of("def-1", id, &empty_project(id)).unwrap();
    assert!(program.cut_from().is_some(), "the program moved onto is a cut");
    let kept = || async {
        pool.scalar::<i64>(
            "SELECT COUNT(*) FROM project_definition WHERE project_id = $1 AND definition_hash = $2",
            &[&id, &program.definition_hash()],
        )
        .await
        .unwrap()
    };
    let queued = queued_run(&store, id).await;
    assert_eq!(move_paused_runs(&pool, &[queued], &program, "bin-B", "v2").await.unwrap(), 0, "a queued run does not move");
    assert_eq!(kept().await, 0, "no cut kept for a move of nothing");
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&queued]).await.unwrap();
    assert_eq!(move_paused_runs(&pool, &[queued], &program, "bin-B", "v2").await.unwrap(), 1);
    assert_eq!(kept().await, 1, "the cut the moved run names is kept");
}

/// Each owner's copy of a trigger is its own activation: an instance's
/// activation neither blocks nor ends the shared one or another
/// instance's, and taking one down removes only the signals it governs.
#[weft_db_macros::test]
async fn an_instances_activation_is_its_own_row(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let bob_key = ActivationKey::new("feed", Owner::Instance(bob.clone()));

    let shared_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], shared_setup, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    let ada_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), ada_setup, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok(),
        "the shared activation in flight does not block an instance's");
    assert!(matches!(activations.try_begin_activating(id, &[bob_key.clone(), ada_key.clone()], Uuid::new_v4(), None, Claimant::Anyone, Uuid::new_v4()).await.unwrap(),
        Err(weft_dispatcher::activation_store::ClaimRefused::Claimed)), "a claim over a busy key claims nothing");
    let bob_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&bob_key), bob_setup, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());

    let mut ada_entry = governed_entry("ada-entry", id, ada_setup);
    ada_entry.instance = Some(ada.clone());
    let mut wrong = ada_entry.clone();
    wrong.token = "wrong".into();
    wrong.setup_run_id = Some(bob_setup);
    assert!(store.signal_insert(&wrong).await.is_err(), "bob's setup cannot arm ada's trigger");
    store.signal_insert(&ada_entry).await.unwrap();
    let mut bob_entry = governed_entry("bob-entry", id, bob_setup);
    bob_entry.instance = Some(bob.clone());
    store.signal_insert(&bob_entry).await.unwrap();

    for setup in [shared_setup, ada_setup, bob_setup] {
        activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }
    assert!(matches!(
        activations.set_lifecycle_guarded(id, std::slice::from_ref(&ada_key), &ActivationLifecycle::refused(), SignalsGoing::Kept, Forget::InstanceRows, DownCause::Person, None).await.unwrap(),
        LifecycleWrite::Applied { .. }
    ));
    let statuses: Vec<(Option<String>, ProjectStatus)> = activations.list(id).await.unwrap().into_iter()
        .map(|a| (a.key.instance().map(|m| m.as_str().to_string()), a.lifecycle.status)).collect();
    assert_eq!(statuses, vec![
        (None, ProjectStatus::Active),
        (Some("bob".into()), ProjectStatus::Active),
    ], "an instance's wiped trigger leaves no row, like one never activated");

    let begun = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&bob_key), begun, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    let refused = activations.set_lifecycle_guarded(id, &[feed_key(), bob_key], &ActivationLifecycle::refused(), SignalsGoing::Kept, Forget::InstanceRows, DownCause::Person, None).await.unwrap();
    assert!(matches!(&refused, LifecycleWrite::Rejected { blocker } if blocker.contains("instance 'bob'")), "{refused:?}");
    let removed = activations.end_activating(id, begun, &ActivationLifecycle::refused(), true, None).await.unwrap().unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["bob-entry"]);
    assert!(store.signal_get("ada-entry").await.unwrap().is_some(), "ada's signal is ada's activation's to remove");
}

/// An instance's activation cancelled (or reaped) mid-way lands wiped, so
/// its row goes with the signals it registered, as if never activated;
/// the shared one cancelled the same way keeps its row, inactive.
#[weft_db_macros::test]
async fn a_cancelled_instances_activation_leaves_no_row(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));

    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key(), ada_key.clone()], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    let mut ada_entry = governed_entry("ada-entry", id, setup_run_id);
    ada_entry.instance = Some(ada.clone());
    store.signal_insert(&ada_entry).await.unwrap();
    store.signal_insert(&governed_entry("shared-entry", id, setup_run_id)).await.unwrap();

    let mut removed: Vec<String> = activations
        .end_activating(id, setup_run_id, &ActivationLifecycle::refused(), true, None)
        .await
        .unwrap()
        .expect("owned")
        .into_iter()
        .map(|s| s.token)
        .collect();
    removed.sort();
    assert_eq!(removed, vec!["ada-entry".to_string(), "shared-entry".to_string()]);
    let rows: Vec<(Option<String>, ProjectStatus)> = activations.list(id).await.unwrap().into_iter()
        .map(|a| (a.key.instance().map(|m| m.as_str().to_string()), a.lifecycle.status)).collect();
    assert_eq!(rows, vec![(None, ProjectStatus::Inactive)], "ada's row goes; the shared one stays");
    let again = activations.try_begin_activating(id, std::slice::from_ref(&ada_key), Uuid::new_v4(), None, Claimant::Anyone, Uuid::new_v4()).await.unwrap();
    assert!(again.expect("claimed").is_empty(), "ada activates again as never activated");
}

/// A re-arm's values are stored in the transaction that lands its
/// activation Active, and never by an activation that no longer owns its
/// triggers.
#[weft_db_macros::test]
async fn a_rearm_stores_its_values_as_it_lands(pool: Pool) {
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let writes = [weft_access_store::InstanceValueWrite {
        node: "feed".into(),
        field: "channel".into(),
        value: json!("news"),
        connection: None,
    }];
    let store = weft_dispatcher::activation_store::ValuesStore { tenant: TENANT, instance: &ada, writes: &writes, cleared: &[] };
    let stored = || async { weft_access_store::instance_values(&pool, TENANT, id, &ada).await.unwrap() };

    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    assert!(activations.end_activating(id, Uuid::new_v4(), &ActivationLifecycle::active(), false, Some(&store))
        .await.unwrap().is_none());
    assert!(stored().await.is_empty(), "a landing that owns nothing stores nothing");
    assert!(activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, Some(&store))
        .await.unwrap().is_some());
    assert_eq!(stored().await.get("feed").and_then(|fields| fields.get("channel")), Some(&json!("news")));
}

/// An instance's trigger taken down refusing with nothing kept leaves no
/// row; the shared one keeps its row, and an instance whose door holds
/// keeps its own. The instance activates again from nothing.
#[weft_db_macros::test]
async fn a_refused_instances_row_is_forgotten(pool: Pool) {
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let instance = |name: &str| ActivationKey::new("feed", Owner::Instance(InstanceId::new(name).unwrap()));
    let (ada, bob, cyd) = (instance("ada"), instance("bob"), instance("cyd"));
    for key in [feed_key(), ada.clone(), bob.clone(), cyd.clone()] {
        let setup = Uuid::new_v4();
        assert!(activations.try_begin_activating(id, std::slice::from_ref(&key), setup, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
        activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }
    let rows = || async {
        activations.list(id).await.unwrap().into_iter()
            .map(|a| (a.key.instance().map(|m| m.as_str().to_string()), a.lifecycle.status)).collect::<Vec<_>>()
    };

    assert!(matches!(
        activations.set_lifecycle_guarded(id, &[feed_key(), ada.clone(), cyd.clone()], &ActivationLifecycle::refused(), SignalsGoing::Kept, Forget::InstanceRows, DownCause::Person, None).await.unwrap(),
        LifecycleWrite::Applied { .. }
    ));
    assert_eq!(rows().await, vec![(None, ProjectStatus::Inactive), (Some("bob".into()), ProjectStatus::Active)]);

    // Refusing with what is held kept, the row stays: it is what that work
    // waits on.
    activations
        .set_lifecycle_guarded(id, std::slice::from_ref(&bob), &ActivationLifecycle::refused(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();
    assert_eq!(rows().await[1], (Some("bob".into()), ProjectStatus::Inactive), "bob keeps the row");

    let setup = Uuid::new_v4();
    let previous = activations.try_begin_activating(id, std::slice::from_ref(&ada), setup, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claimed");
    assert!(previous.is_empty(), "ada activates again as never activated");
    activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&ada), &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None).await.unwrap();
    assert_eq!(rows().await[1], (Some("ada".into()), ProjectStatus::Inactive), "a parked instance keeps the row");
}

#[weft_db_macros::test]
async fn trigger_bake_ownership_publication_and_project_cleanup(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    pool.execute("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)", &[&id]).await.unwrap();
    let first = Uuid::new_v4();
    let birth = trigger_setup_birth(id, first);
    queue(&store, std::slice::from_ref(&birth), false).await.unwrap();
    let complete = complete(&store, first).await;
    let bake = weft_dispatcher::store::TriggerBake::from_events(&[birth, complete], &setup_program()).unwrap().unwrap();
    assert_eq!(store.finish_trigger_setup(first, Some(&bake)).await.unwrap(), None, "nothing before it to replace");

    let second = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], second, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    assert!(queue(&store, &[trigger_setup_birth(id, Uuid::new_v4())], true).await.is_err(),
        "a setup no activation is setting up is never born as one");
    queue(&store, &[trigger_setup_birth(id, second)], true).await.unwrap();
    let mut entry = governed_entry("baked-entry", id, first);
    entry.program = Some(bake.program.clone());
    entry.source_version = Some(bake.source_version.clone());
    assert!(store.signal_insert(&entry).await.is_err(), "a setup that owns no activation cannot arm");
    entry.setup_run_id = Some(second);
    store.signal_insert(&entry).await.unwrap();
    let armed = store.signal_get("baked-entry").await.unwrap().unwrap();
    assert_eq!(armed.program, entry.program);
    assert_eq!(armed.source_version, entry.source_version);
    assert_eq!(armed.setup_run_id, Some(second));
    activations.end_activating(id, second, &ActivationLifecycle::active(), false, None).await.unwrap();
    assert!(store.signal_insert(&entry).await.is_err(), "no late registration after activation ends");
    store.finish_trigger_setup(second, None).await.unwrap();
    assert_eq!(store.trigger_bakes(id, None).await.unwrap().read[0].run_id, first);
    store.delete_run(first).await.unwrap();
    assert_eq!(store.trigger_bakes(id, None).await.unwrap().read[0].run_id, first);
    assert!(store.trigger_bakes(Uuid::new_v4(), None).await.unwrap().read.is_empty());
    projects.remove(id).await.unwrap();
    assert!(store.trigger_bakes(id, None).await.unwrap().read.is_empty());
}

/// A run's files go with its record, never with its end: ending a run
/// queues nothing, and each way its record is deleted (a clean of the
/// run, its keep running out, its project removed) queues its files for
/// the broker to delete.
#[weft_db_macros::test]
async fn a_runs_files_are_queued_when_its_record_goes_not_when_it_ends(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let queued = |run: weft_core::RunId| {
        let pool = pool.clone();
        async move { pool.scalar::<i64>("SELECT COUNT(*) FROM storage_sweep WHERE run_id = $1", &[&run]).await.unwrap() }
    };
    let [cleaned, expired, removed] = [(); 3].map(|_| weft_core::new_run_id());
    for run in [cleaned, expired, removed] {
        assert!(queue(&store, &[birth(run, id, weft_core::context::Phase::Fire, "bin-A")], false).await.unwrap());
    }
    for run in [cleaned, expired, removed] {
        complete(&store, run).await;
    }
    for run in [cleaned, expired, removed] {
        assert_eq!(queued(run).await, 0, "ending a run leaves its files");
    }
    store.delete_run(cleaned).await.unwrap();
    assert_eq!(queued(cleaned).await, 1, "a clean of the run");
    pool.execute("UPDATE run SET keep_until = 0 WHERE run_id = $1", &[&expired]).await.unwrap();
    assert_eq!(store.erase_expired(1, 100).await.unwrap().0, 1);
    assert_eq!(queued(expired).await, 1, "its keep ran out");
    projects.remove(id).await.unwrap();
    assert_eq!(queued(removed).await, 1, "its project removed");
    let tenant: String = pool.scalar("SELECT tenant_id FROM storage_sweep WHERE run_id = $1", &[&cleaned]).await.unwrap();
    assert_eq!(tenant, TENANT, "queued under the tenant its files were written under");
}

/// How many rows of `table` belong to run `run_id`.
async fn rows_of(pool: &Pool, table: &str, run_id: weft_core::RunId) -> i64 {
    pool.scalar(&format!("SELECT COUNT(*) FROM {table} WHERE run_id = $1"), &[&run_id])
        .await
        .unwrap()
}

/// Queueing a run is ONE transaction: its row and its record's first row
/// commit together or not at all. A run of a project with no row is
/// refused with nothing written, and a run queued again by its id (a
/// retried start) writes nothing twice.
#[weft_db_macros::test]
async fn queueing_a_run_is_atomic_and_happens_once(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let missing = Uuid::new_v4();
    let lost = weft_core::new_run_id();
    let err = queue(&store, &[birth(lost, missing, weft_core::context::Phase::Fire, "bin-A")], false).await.unwrap_err();
    assert!(format!("{err:#}").contains("has no row"), "{err:?}");
    assert_eq!((rows_of(&pool, "run", lost).await, rows_of(&pool, "run_log", lost).await), (0, 0), "a refused start leaves nothing");

    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run_id = weft_core::new_run_id();
    let start = birth(run_id, id, weft_core::context::Phase::Fire, "bin-A");
    let kick = RunEvent::NodeKicked {
        run_id, node_id: "entry".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 1,
    };
    assert!(queue(&store, &[start.clone(), kick.clone()], false).await.unwrap());
    assert_eq!((rows_of(&pool, "run", run_id).await, rows_of(&pool, "run_log", run_id).await), (1, 1),
        "the birth and its kicks are the record's first row");
    assert_eq!(crate::events(store.events_log(run_id).await.unwrap()).len(), 2);
    assert!(!queue(&store, &[start, kick], false).await.unwrap(), "a retried start finds its run");
    assert_eq!(rows_of(&pool, "run_log", run_id).await, 1, "and writes nothing twice");

    // A run of a cut is queued with the cut, kept with the project's
    // programs under the hash its birth names, marked with what it was cut
    // from; a start refused keeps nothing, and a birth naming another
    // program than the one it is started with is refused.
    let cut_of = |project: Uuid| {
        let cut: weft_core::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": project, "nodes": [], "edges": [], "groups": [],
        })).unwrap();
        weft_journal::record::RunsProgram::of("def-1", project, &cut).unwrap()
    };
    let cut_birth = |run_id, project, program: &weft_journal::record::RunsProgram| {
        let mut born = birth(run_id, project, weft_core::context::Phase::Fire, "bin-A");
        if let RunEvent::RunStarted { definition_hash, cut_from, .. } = &mut born {
            *definition_hash = Some(program.definition_hash().to_string());
            *cut_from = program.cut_from().map(str::to_string);
        }
        born
    };
    let queue_cut = |events: Vec<RunEvent>, program: weft_journal::record::RunsProgram| {
        let store = &store;
        async move {
            store.queue_run(Queued {
                events: &events, tenant: TENANT,
                keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT, watch_end: false,
                stale: &[], spec: None, example: None, program: Some(&program),
            }, false).await
        }
    };
    let kept = |project: Uuid, hash: String| {
        let pool = pool.clone();
        async move {
            pool.all::<(Option<String>,)>(
                "SELECT cut_from FROM project_definition WHERE project_id = $1 AND definition_hash = $2",
                &[&project, &hash],
            ).await.unwrap()
        }
    };
    let program = cut_of(id);
    let cut_run = weft_core::new_run_id();
    assert!(queue_cut(vec![cut_birth(cut_run, id, &program)], program.clone()).await.unwrap());
    assert_eq!(kept(id, program.definition_hash().to_string()).await, [(Some("def-1".to_string()),)], "the cut is kept with the run, as a cut");

    let refused = cut_of(missing);
    let lost_cut = weft_core::new_run_id();
    assert!(queue_cut(vec![cut_birth(lost_cut, missing, &refused)], refused.clone()).await.is_err());
    assert!(kept(missing, refused.definition_hash().to_string()).await.is_empty(), "a refused start keeps no cut");

    let mismatched = weft_core::new_run_id();
    let err = queue_cut(vec![birth(mismatched, id, weft_core::context::Phase::Fire, "bin-A")], cut_of(id)).await.unwrap_err();
    assert!(format!("{err:#}").contains("is started with program"), "{err:?}");
    assert_eq!(rows_of(&pool, "run", mismatched).await, 0, "a birth that names another program is not started");
}

/// Seed a live worker of `project_id` on image `binary_hash`, driving
/// `in_flight` runs per trigger token.
async fn seed_worker(pool: &Pool, replica: &str, project_id: Uuid, binary_hash: &str, in_flight: serde_json::Value, alive: bool) {
    let until = weft_dispatcher::lease::now_unix() + if alive { 60 } else { -60 };
    pool.execute(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix, binary_hash, in_flight) \
         VALUES ($1, $2, $3, $4, $5, $6::text::jsonb) \
         ON CONFLICT (replica) DO UPDATE SET leased_until_unix = EXCLUDED.leased_until_unix, in_flight = EXCLUDED.in_flight",
        &[&replica, &project_id, &TENANT, &until, &binary_hash, &in_flight.to_string()],
    )
    .await
    .unwrap();
}

/// The install's news the writes left for `run`, by kind, oldest first.
async fn news_of(pool: &Pool, run: weft_core::RunId) -> Vec<(Uuid, String)> {
    pool.all("SELECT project_id, kind FROM weft_news WHERE run_id = $1 ORDER BY seq", &[&run]).await.unwrap()
}

/// Claim queued run `run_id` of `project_id` for `owner`, the way
/// a worker takes it.
async fn claim(pool: &Pool, run_id: weft_core::RunId, project_id: Uuid, owner: &str) {
    weft_journal::record::claim(&pool.get().await.unwrap(), run_id, project_id, owner).await.unwrap().expect("claimed");
}

/// A drain counts what the project's live workers say they drive for the
/// triggers it reaches, plus the runs it reaches on record that are queued
/// or driven by a live worker. A run parked on a wait is not going, and
/// neither is a run whose worker's lease lapsed past its claim's grace
/// (the lost-run sweep owns it).
#[weft_db_macros::test]
async fn the_drain_counts_what_workers_drive_and_the_runs_queued_or_driven(pool: Pool) {
    use weft_dispatcher::drain::{going, AtTheDoor};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (tok, other) = (AtTheDoor::Tokens(vec!["tok".to_string()]), AtTheDoor::Tokens(vec!["other".to_string()]));
    assert_eq!(going(&pool, id, &AtTheDoor::Every, &[]).await.unwrap(), 0);

    let run = queued_run(&store, id).await;
    let runs = [run];
    assert_eq!(going(&pool, id, &AtTheDoor::Every, &runs).await.unwrap(), 1, "queued for a worker");
    claim(&pool, run, id, "worker-a").await;
    assert_eq!(going(&pool, id, &AtTheDoor::Every, &runs).await.unwrap(), 1, "within its claim's grace, before its worker's lease is written");
    pool.execute("UPDATE run SET delivered_until = 0 WHERE run_id = $1", &[&run]).await.unwrap();
    assert_eq!(going(&pool, id, &AtTheDoor::Every, &runs).await.unwrap(), 0, "past the grace, its owner has no live lease: lost, not going");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({ "tok": 2 }), true).await;
    assert_eq!(going(&pool, id, &AtTheDoor::Every, &runs).await.unwrap(), 3, "two its worker states, plus the run on record");
    assert_eq!(going(&pool, id, &tok, &[]).await.unwrap(), 2, "only the runs it reaches on record count");
    assert_eq!(going(&pool, id, &other, &runs).await.unwrap(), 1, "a worker's count for a trigger it does not reach is not");
    seed_worker(&pool, "worker-b", id, "bin-B", json!({ "tok": 4 }), true).await;
    assert_eq!(going(&pool, id, &tok, &runs).await.unwrap(), 7, "every live worker of the project counts");
    let old_build = AtTheDoor::Builds(vec!["bin-A".to_string()]);
    assert_eq!(going(&pool, id, &old_build, &[]).await.unwrap(), 2, "a build's workers count, another's do not");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({ "tok": 2 }), false).await;
    assert_eq!(going(&pool, id, &AtTheDoor::Every, &runs).await.unwrap(), 4, "a worker whose lease lapsed says nothing, and its run is lost");

    pool.execute("UPDATE run SET state = 'parked', owner = NULL WHERE run_id = $1", &[&run]).await.unwrap();
    seed_worker(&pool, "worker-b", id, "bin-B", json!({}), true).await;
    assert_eq!(going(&pool, id, &AtTheDoor::Every, &runs).await.unwrap(), 0, "a parked run is not going");
}

/// A run whose owner's lease ran out is let go of: a durable one is
/// queued again for the next worker, its epoch raised so a late batch of
/// the old owner is refused; a fast one lived in its worker's memory and
/// ends, cancelled. A run whose owner is alive is left alone, and so is one
/// within the grace its claim gave it, before its worker's lease is
/// written.
#[weft_db_macros::test]
async fn a_lost_run_is_queued_again_when_durable_and_ended_when_fast_or_asked_to_stop(pool: Pool) {
    use weft_dispatcher::store::Lost;
    use weft_core::run_settings::{Keeping, RunSettings};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let start = |keeping: Keeping| {
        let run_id = weft_core::new_run_id();
        let mut started = birth(run_id, id, weft_core::context::Phase::Fire, "bin-A");
        if let RunEvent::RunStarted { settings, .. } = &mut started {
            *settings = RunSettings::new(keeping, true).unwrap();
        }
        (run_id, started)
    };
    let now = weft_dispatcher::lease::now_unix();
    let (durable, started) = start(Keeping::Durable);
    queue(&store, &[started], false).await.unwrap();
    claim(&pool, durable, id, "worker-a").await;
    let epoch: i32 = run_column(&pool, "epoch", durable).await.unwrap();

    seed_worker(&pool, "worker-a", id, "bin-A", json!({}), true).await;
    assert_eq!(store.let_go_of_lost(durable, now, None).await.unwrap(), Lost::NotLost, "its owner is alive");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({}), false).await;
    assert_eq!(store.let_go_of_lost(durable, now, None).await.unwrap(), Lost::NotLost, "within its claim's grace");
    let past_grace = weft_journal::record::delivered_until(now) + 5;
    assert_eq!(store.let_go_of_lost(durable, past_grace, None).await.unwrap(), Lost::Requeued);
    assert_eq!(run_column::<String>(&pool, "state", durable).await.as_deref(), Some("queued"));
    assert_eq!(run_column::<Option<String>>(&pool, "owner", durable).await, Some(None));
    assert!(run_column::<i32>(&pool, "epoch", durable).await.unwrap() > epoch, "a late batch of the old owner is refused");

    let (fast, started) = start(Keeping::Fast);
    queue(&store, &[started], false).await.unwrap();
    claim(&pool, fast, id, "worker-a").await;
    assert_eq!(store.let_go_of_lost(fast, past_grace, None).await.unwrap(), Lost::Ended);
    assert_eq!(run_column::<String>(&pool, "state", fast).await.as_deref(), Some("ended"));
    assert!(matches!(crate::events(store.events_log(fast).await.unwrap()).last(), Some(RunEvent::RunCancelled { .. })));
    assert_eq!(store.let_go_of_lost(fast, past_grace, None).await.unwrap(), Lost::NotLost, "an ended run is not lost");

    // A durable run asked to stop while its worker drove it ends when that
    // worker goes away, for what asked: queued again, it would only start a
    // worker to read the cancel off its claim.
    let (stopped, started) = start(Keeping::Durable);
    queue(&store, &[started], false).await.unwrap();
    claim(&pool, stopped, id, "worker-a").await;
    store.cancel_run(stopped, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!(store.let_go_of_lost(stopped, past_grace, None).await.unwrap(), Lost::Ended);
    assert_eq!(run_column::<String>(&pool, "state", stopped).await.as_deref(), Some("ended"));
    assert!(matches!(
        crate::events(store.events_log(stopped).await.unwrap()).last(),
        Some(RunEvent::RunCancelled { cause: Some(weft_core::run::CancelCause::User), .. })
    ));
}

/// A worker whose runs only wait vouched for itself until their waits
/// give up: its lease holds that long through the broker's tick, held to
/// the longest a wait can hold; its runs are still worked on and not lost,
/// it still counts among its program's copies, and the `lapses` loop looks
/// once the vouch could lapse, not before: the grace its claim gave it
/// counts for nothing while its worker's lease outlives it. Once the vouch
/// passed with the worker gone, its run is lost.
#[weft_db_macros::test]
async fn a_vouching_workers_runs_are_its_own_until_its_vouch_passes(pool: Pool) {
    use weft_dispatcher::store::Lost;
    use weft_task_store::runs::in_flight;
    use weft_task_store::worker_door::lease_until;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let now = weft_dispatcher::lease::now_unix();
    let run = queued_run(&store, id).await;
    claim(&pool, run, id, "worker-a").await;
    let tick = |replica: &'static str, vouch: Option<i64>| {
        let pool = pool.clone();
        async move {
            let in_flight = std::collections::BTreeMap::from([("t1".to_string(), 1u32)]);
            let weft_task_store::db::Json(answer): weft_task_store::db::Json<serde_json::Value> = pool.scalar(
                "SELECT weft_door_tick($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[
                    &replica,
                    &id,
                    &TENANT,
                    &"bin-A",
                    &lease_until(now, 15, vouch),
                    &weft_task_store::db::Json(weft_task_store::db::WeftMade(&in_flight)),
                    &0i64,
                    &Vec::<String>::new(),
                    &Vec::<i64>::new(),
                    &Vec::<Option<String>>::new(),
                    &false,
                ],
            )
            .await
            .unwrap();
            answer["copies"].as_i64().unwrap()
        }
    };
    assert_eq!(tick("worker-a", Some(now + 600)).await, 1);
    assert_eq!(tick("worker-b", None).await, 2, "the vouching copy still counts");
    assert!(in_flight(&pool, run).await.unwrap(), "vouched: still worked on");
    assert_eq!(store.let_go_of_lost(run, now + 500, None).await.unwrap(), Lost::NotLost, "its own until its vouch passes");
    pool.execute("DELETE FROM worker_lease WHERE replica = 'worker-b'", &[]).await.unwrap();
    let margin = weft_task_store::worker_door::WORKER_LEASE_SECS;
    let earliest = |at: i64| {
        let pool = pool.clone();
        async move { weft_dispatcher::lapses::earliest_lapse(&pool, at).await.unwrap() }
    };
    let grace: i64 = run_column(&pool, "delivered_until", run).await.expect("claimed with a grace");
    assert!(grace < now + 600, "the vouch outlives the claim's grace");
    assert_eq!(earliest(now).await, Some(now + 600 + margin), "once the vouch could lapse, not at the grace's end");
    assert_eq!(store.let_go_of_lost(run, now + 700, None).await.unwrap(), Lost::Ended, "its vouch passed and its worker gone: lost");
}

/// A cancel asked of a run a worker drives, and an answer handed to one,
/// wake this process's news teller once they commit, which tells them.
#[weft_db_macros::test]
async fn a_cancel_and_a_handed_answer_are_told_as_news(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let bus = std::sync::Arc::new(weft_platform_traits::FakeNews::default());
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    let told = |kind: weft_platform_traits::NewsKind, run: weft_core::RunId| {
        let bus = bus.clone();
        async move {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !bus.announced.lock().iter().flatten().any(|news| news.kind == kind && news.run == run) {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{kind:?} of {run} told"));
        }
    };
    let answered = queued_run(&store, id).await;
    claim(&pool, answered, id, "worker-a").await;
    seed_wait(&store, "tok-told", id, answered).await;
    assert!(matches!(store.answer("tok-told", &weft_core::primitive::WaitAnswer::Given { value: json!(1) }).await.unwrap(), weft_dispatcher::store::Answered::Reached { .. }));
    told(weft_platform_traits::NewsKind::Answer, answered).await;
    let cancelled = queued_run(&store, id).await;
    claim(&pool, cancelled, id, "worker-a").await;
    assert!(store.cancel_run(cancelled, None, &weft_core::run::CancelCause::User).await.unwrap().requested);
    told(weft_platform_traits::NewsKind::Cancel, cancelled).await;
}

/// A cancel ends a run nobody drives on the spot, after its last row; a
/// run a worker drives is asked to stop, and its worker writes the ending.
#[weft_db_macros::test]
async fn a_cancel_ends_a_waiting_run_and_asks_a_driven_one_to_stop(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let cause = weft_core::run::CancelCause::User;

    let waiting = queued_run(&store, id).await;
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&waiting]).await.unwrap();
    let written = store.cancel_run(waiting, None, &cause).await.unwrap();
    assert!(!written.requested);
    assert_eq!(written.node_cancellations, Some(0));
    assert_eq!(run_column::<String>(&pool, "state", waiting).await.as_deref(), Some("ended"));
    assert!(matches!(crate::events(store.events_log(waiting).await.unwrap()).last(), Some(RunEvent::RunCancelled { .. })));

    let driven = queued_run(&store, id).await;
    claim(&pool, driven, id, "worker-a").await;
    let written = store.cancel_run(driven, None, &cause).await.unwrap();
    assert!(written.requested, "its worker is asked");
    assert_eq!(news_of(&pool, driven).await, vec![(id, "cancel".to_string())], "and the install's news says so, with the write");
    assert!(news_of(&pool, waiting).await.is_empty(), "a run nobody drives makes no news");
    assert_eq!(written.node_cancellations, None);
    assert_eq!(run_column::<String>(&pool, "state", driven).await.as_deref(), Some("running"), "its worker ends it");
    let asked: Option<String> = run_column(&pool, "cancel_requested", driven).await;
    assert!(asked.is_some());
    assert_eq!(crate::events(store.events_log(driven).await.unwrap()).len(), 1, "nothing written into a record its worker writes");
}

/// A cancel queues the last call of each wait that carries one, in its own
/// commit: one `cancel_call` task per wait, carrying the call and the
/// connection it is sent as, deduplicated on the wait, and none for a wait
/// that carries no call.
#[weft_db_macros::test]
async fn a_cancel_queues_the_last_call_of_each_wait_that_carries_one(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&store, id).await;
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&run]).await.unwrap();
    seed_wait(&store, "tok-plain", id, run).await;
    let mut calling = entry_signal("tok-calling", id);
    calling.node_id = "render".into();
    calling.is_resume = true;
    calling.run_id = Some(run);
    let mut spec = weft_core::primitive::SignalSpec::of_kind("poll_endpoint", json!({ "url": "https://queue.example/requests/7/status" }));
    spec.on_cancel = Some(weft_core::signal::CancelCall::put("https://queue.example/requests/7/cancel"));
    calling.spec_json = serde_json::to_string(&spec).unwrap();
    store.signal_insert(&calling).await.expect("seed the wait with a last call");

    let written = store.cancel_run(run, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!((written.removed.len(), written.cancel_calls), (2, 1));
    type TaskRow = (String, Option<String>, Option<Uuid>, weft_task_store::db::JsonText<serde_json::Value>);
    let tasks: Vec<TaskRow> =
        pool.all("SELECT kind, dedup_key, run_id, payload FROM task WHERE kind = 'cancel_call'", &[]).await.unwrap();
    assert_eq!(tasks.len(), 1, "{tasks:?}");
    let (_, dedup, task_run, payload) = &tasks[0];
    assert_eq!(dedup.as_deref(), Some(weft_task_store::kinds::cancel_call_key("tok-calling").as_str()));
    assert_eq!(*task_run, Some(run));
    let call: weft_core::signal::listener_protocol::CancelCallRequest = serde_json::from_value(payload.0.clone()).unwrap();
    assert_eq!(call.token, "tok-calling");
    assert_eq!(call.spec.on_cancel, Some(weft_core::signal::CancelCall::put("https://queue.example/requests/7/cancel")));

    let again = store.cancel_run(run, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!((again.removed.len(), again.cancel_calls), (0, 0), "the waits went with the first cancel");

    // A run a worker drives, asked to cancel while one of its nodes was
    // still registering its wait: the wait lands after the cancel, and
    // the removal at the run's ending sends its call all the same.
    let driven = queued_run(&store, id).await;
    claim(&pool, driven, id, "worker-a").await;
    assert!(store.cancel_run(driven, None, &weft_core::run::CancelCause::User).await.unwrap().requested);
    let mut late = calling.clone();
    late.token = "tok-late".into();
    late.run_id = Some(driven);
    store.signal_insert(&late).await.expect("a wait registered after the cancel");
    // Its worker saw the cancel and ended the run so.
    pool.execute("UPDATE run SET state = 'ended', outcome = 'cancelled', owner = NULL WHERE run_id = $1", &[&driven]).await.unwrap();
    // As `run_ends` calls it: holding the run's row in a transaction of its
    // own, so the removal must not wait on that row.
    let mut holding_conn = pool.get().await.unwrap();
    let holding = holding_conn.transaction().await.unwrap();
    weft_journal::record::lock_in(&holding, driven).await.unwrap();
    let removed = tokio::time::timeout(std::time::Duration::from_secs(10), store.signal_remove_for_run_id(driven))
        .await
        .expect("the removal never waits on the run's row")
        .unwrap();
    holding.commit().await.unwrap();
    assert_eq!(removed.len(), 1);
    let queued: Vec<Option<String>> =
        pool.scalars("SELECT dedup_key FROM task WHERE kind = 'cancel_call' AND run_id = $1", &[&driven]).await.unwrap();
    assert_eq!(queued, vec![Some(weft_task_store::kinds::cancel_call_key("tok-late"))]);

    // A run that ended any other way takes its waits down without a call,
    // even one asked to cancel that finished before its worker saw it.
    let finished = queued_run(&store, id).await;
    let mut kept = calling.clone();
    kept.token = "tok-kept".into();
    kept.run_id = Some(finished);
    store.signal_insert(&kept).await.expect("a wait of a run that ends on its own");
    pool.execute(
        "UPDATE run SET state = 'ended', outcome = 'completed', cancel_requested = '\"user\"' WHERE run_id = $1",
        &[&finished],
    )
    .await
    .unwrap();
    assert_eq!(store.signal_remove_for_run_id(finished).await.unwrap().len(), 1);
    let none: i64 = pool.scalar("SELECT COUNT(*) FROM task WHERE kind = 'cancel_call' AND run_id = $1", &[&finished]).await.unwrap();
    assert_eq!(none, 0);

    // Removing the project cuts short a run whose worker never wrote the
    // cancel: its wait goes with the project, and its call is queued and
    // kept with it.
    let cut = queued_run(&store, id).await;
    claim(&pool, cut, id, "worker-b").await;
    assert!(store.cancel_run(cut, None, &weft_core::run::CancelCause::User).await.unwrap().requested);
    let mut stranded = calling.clone();
    stranded.token = "tok-stranded".into();
    stranded.run_id = Some(cut);
    store.signal_insert(&stranded).await.expect("a wait registered after the cancel");
    projects.remove(id).await.unwrap().expect("the project was there");
    let kept: Vec<Option<String>> = pool.scalars("SELECT dedup_key FROM task WHERE kind = 'cancel_call' ORDER BY dedup_key", &[])
        .await
        .unwrap();
    assert!(kept.contains(&Some(weft_task_store::kinds::cancel_call_key("tok-stranded"))), "{kept:?}");
    assert!(kept.contains(&Some(weft_task_store::kinds::cancel_call_key("tok-calling"))), "a queued call outlives the project: {kept:?}");
    assert!(!kept.contains(&Some(weft_task_store::kinds::cancel_call_key("tok-kept"))), "{kept:?}");
}

/// A charge an earlier drive opened and no response priced is booked
/// unknown when the dispatcher cancels the parked run: one `CostReported`
/// closing it, before the ending, in the same write.
#[weft_db_macros::test]
async fn a_cancel_books_a_parked_runs_open_charge_unknown(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&store, id).await;
    let opened = RunEvent::ChargeOpened {
        run_id: run,
        node_id: "dub".into(),
        frames: Vec::new(),
        service: "jobs".into(),
        charge: "job-1".into(),
        scratch: json!({ "model": "m" }),
        origin: weft_core::CredentialOwner::Platform,
        at_unix: 2,
    };
    let written = store.append(run, std::slice::from_ref(&opened), Then::Stays).await.unwrap();
    assert!(matches!(written, weft_journal::record::Appended::At(_)), "{written:?}");
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&run]).await.unwrap();

    let written = store.cancel_run(run, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!(written.node_cancellations, Some(0));
    let events = crate::events(store.events_log(run).await.unwrap());
    let costs: Vec<&RunEvent> = events.iter().filter(|event| matches!(event, RunEvent::CostReported { .. })).collect();
    assert_eq!(costs.len(), 1, "{events:?}");
    assert!(
        matches!(costs[0], RunEvent::CostReported { node_id, service, charge: Some(charge), amount_usd: None, .. }
            if node_id == "dub" && service == "jobs" && charge == "job-1"),
        "{:?}", costs[0]
    );
    assert!(matches!(&events[events.len() - 2..], [RunEvent::CostReported { .. }, RunEvent::RunCancelled { .. }]), "{events:?}");
    assert_eq!(weft_journal::ChargeBook::of_record(&events).into_open().count(), 0, "the record holds no open charge");
}

/// An ending somebody waits on is found off the run's row, whether or not
/// its announcement was heard: the look after a lost notification finds
/// it, two dispatchers never take it at once, and once handled it is not
/// found again.
#[weft_db_macros::test]
async fn an_ending_with_work_left_is_found_without_its_announcement(pool: Pool) {
    use weft_dispatcher::run_ends::{clear_flags, take_flagged, Jobs};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run_id = weft_core::new_run_id();
    let started = birth(run_id, id, weft_core::context::Phase::Fire, "bin-A");
    assert!(store.queue_run(Queued {
        events: std::slice::from_ref(&started), tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: true, stale: &[], spec: None, example: None, program: None,
    }, false).await.unwrap());
    let quiet = queued_run(&store, id).await;
    complete(&store, run_id).await;
    complete(&store, quiet).await;

    let mut one_conn = pool.get().await.unwrap();
    let one = one_conn.transaction().await.unwrap();
    let found = take_flagged(&one, run_id).await.unwrap().expect("the watched ending is flagged");
    assert!(found.watch_end);
    let mut other_conn = pool.get().await.unwrap();
    let other = other_conn.transaction().await.unwrap();
    assert!(take_flagged(&other, quiet).await.unwrap().is_none(), "nobody waits on the other ending");
    assert!(take_flagged(&other, run_id).await.unwrap().is_none(), "held by the first look");
    other.rollback().await.unwrap();
    clear_flags(&one, run_id, Jobs { watch_end: true, holds_signals: false }).await.unwrap();
    one.commit().await.unwrap();
    let again = one_conn.transaction().await.unwrap();
    assert!(take_flagged(&again, run_id).await.unwrap().is_none(), "handled once");
}

/// The retention loop erases a run once it ended longer ago than it is
/// kept for, with its record, and never a run that has not ended, however
/// old.
#[weft_db_macros::test]
async fn retention_erases_ended_runs_past_their_keep_and_never_a_waiting_one(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ended = queued_run(&store, id).await;
    complete(&store, ended).await;
    let parked = queued_run(&store, id).await;
    pool.execute("UPDATE run SET state = 'parked', started_at = 0 WHERE run_id = $1", &[&parked]).await.unwrap();
    let keep_until: i64 = run_column(&pool, "keep_until", ended).await.expect("an ended run has its keep");

    assert_eq!(store.erase_expired(keep_until, 100).await.unwrap().0, 0, "kept until its keep runs out");
    assert_eq!(store.erase_expired(i64::MAX, 100).await.unwrap().0, 1);
    assert_eq!((rows_of(&pool, "run", ended).await, rows_of(&pool, "run_log", ended).await), (0, 0));
    assert_eq!(rows_of(&pool, "run", parked).await, 1, "a parked run is never erased");
}

/// The referenced-image set (the keep-set image pruning deletes against)
/// must cover a project's current hash, the image of every run that has
/// not ended (a run waiting on a form resumes on the image it started
/// on), every live worker's image (it may drive a run its door just bore),
/// every infra image ref in any project's tag map, AND the refs recorded
/// on live infra units (a unit left UP across a sync stays frozen at its
/// recorded image, which can be older than the project's current map):
/// `weft clean --images` deletes everything outside this set, so any of
/// them escaping it would be deleted out from under a running workload.
/// A finished run's image and a lapsed worker's drop out; a project with
/// no infra tags contributes none; a blank ref is skipped (matches no
/// image); a unit stamped before refs were recorded contributes nothing.
#[weft_db_macros::test]
async fn referenced_images_cover_projects_runs_workers_maps_and_unit_refs(
    pool: Pool,
) {
    let (store, projects) = setup(&pool).await;
    let plain = Uuid::new_v4();
    seed_project(&projects, plain, "hash-current").await;
    // A second project whose committed infra sync recorded two nodes'
    // image tags; both refs are the supervisor's to apply, so both are
    // referenced no matter which node they belong to. One blank ref is
    // skipped (a buggy writer's blank matches no image and must not be
    // laundered into the set).
    let with_infra = Uuid::new_v4();
    seed_project(&projects, with_infra, "hash-infra-project").await;
    pool.execute(
        "UPDATE project SET infra_image_tags_json = $1 WHERE id = $2",
        &[
            &serde_json::json!({
                "bridge-node": { "bridge": "weft-infra-bridge:live1", "blank": "" },
                "engine-node": { "engine": "weft-infra-engine:live2" },
            })
            .to_string(),
            &with_infra,
        ],
    )
    .await
    .expect("seed infra image tags");
    // An infra_node row for that project: one unit UP and frozen at an
    // image OLDER than the map above (the case the map alone cannot
    // cover), one unit stamped before refs were recorded (contributes
    // nothing), one unit long gone terminal.
    pool.execute(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, health_rules_json_version) VALUES ($1, 'n1', 'inst1', 'running', 1)",
        &[&with_infra],
    )
    .await
    .expect("seed infra_node");
    pool.execute(
        "INSERT INTO infra_unit (project_id, node_id, unit, status, stop_behavior, flaky_after_seconds, \
             recovery_after_seconds, image_refs) VALUES \
         ($1, 'n1', 'frozen', 'running', 'keep_running', 30, 30, ARRAY['weft-infra-bridge:0ld-frozen']), \
         ($1, 'n1', 'legacy', 'running', 'keep_running', 30, 30, '{}'), \
         ($1, 'n1', 'terminal', 'stopped', 'stop', 30, 30, ARRAY['weft-infra-bridge:terminal-gone'])",
        &[&with_infra],
    )
    .await
    .expect("seed infra_node units");
    // A live worker's image stays (it may drive a run its door just bore,
    // before anything of it is on record); a worker whose lease lapsed is
    // gone.
    seed_worker(&pool, "worker-live", plain, "hash-live-worker", json!({}), true).await;
    seed_worker(&pool, "worker-gone", plain, "hash-gone-worker", json!({}), false).await;

    // A run still waiting (a form, a timer) resumes on the image it
    // started on, so its image stays; a finished run's does not.
    for (hash, finished) in [("hash-waiting-run", false), ("hash-finished-run", true)] {
        let run_id = weft_core::new_run_id();
        queue(&store, &[birth(run_id, plain, weft_core::context::Phase::Fire, hash)], false).await.unwrap();
        if finished {
            complete(&store, run_id).await;
        }
    }

    let referenced = weft_dispatcher::api::project::referenced_images_query(&pool.get().await.expect("a connection"), weft_dispatcher::build::prune::ImageScope::All)
        .await
        .expect("referenced set");
    let mut hashes = referenced.worker_hashes.clone();
    hashes.sort();
    assert_eq!(
        hashes,
        vec![
            "hash-current".to_string(),
            "hash-infra-project".to_string(),
            "hash-live-worker".to_string(),
            "hash-waiting-run".to_string(),
        ],
        "project hashes + live workers' images + the images of runs that have not ended; \
         a lapsed worker's image and finished runs' images out"
    );
    assert_eq!(
        referenced.infra_refs,
        vec![
            "weft-infra-bridge:0ld-frozen".to_string(),
            "weft-infra-bridge:live1".to_string(),
            "weft-infra-bridge:terminal-gone".to_string(),
            "weft-infra-engine:live2".to_string(),
        ],
        "every ref of every project's tag map AND every recorded unit \
         ref is referenced (all rows, all units: over-keeping is the \
         safe direction); the blank ref and the ref-less legacy unit \
         contribute nothing"
    );
}

// ----- parked fires: one row per event, FIFO per trigger ------------------

/// An event waiting for its trigger, its `attempts`-th failure behind it,
/// due at `not_before`.
fn parked(attempts: u32, not_before: i64) -> Waiting {
    Waiting { fire_id: Uuid::new_v4(), payload: json!({ "v": 1 }), caller: None, attempts, not_before, waits_for: None, failures: 0, skipped: false }
}

/// Seed one signal row for `token`. Entry rows are keyed by
/// `(project_id, node_id)`, so the node id is derived from the token:
/// many tokens per project, no collisions.
async fn seed_parked_signal(store: &PostgresStore, token: &str, project: Uuid) {
    let mut sig = entry_signal(token, project);
    sig.node_id = format!("trigger-{token}");
    sig.activation_trigger = Some(sig.node_id.clone());
    store
        .signal_insert(&sig)
        .await
        .expect("seed signal row");
}

/// A wait `token` of run `run_id`.
async fn seed_wait(store: &PostgresStore, token: &str, project: Uuid, run_id: weft_core::RunId) {
    let mut wait = entry_signal(token, project);
    wait.node_id = format!("wait-{token}");
    wait.is_resume = true;
    wait.run_id = Some(run_id);
    store.signal_insert(&wait).await.expect("seed the wait");
}

/// An answer reaches its run once: a run nobody drives gets it in its
/// record and is queued to carry on; a run a worker drives has it handed
/// to that worker (nobody else writes its record); a run that ended takes
/// nothing. The wait's signal goes in every case, so a second answer finds
/// nothing, and an entry's token is never a wait.
#[weft_db_macros::test]
async fn an_answer_reaches_its_run_once(pool: Pool) {
    use weft_dispatcher::store::Answered;
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&store, "tok-entry", project).await;
    assert!(matches!(store.answer("tok-entry", &weft_core::primitive::WaitAnswer::Given { value: json!(1) }).await.unwrap(), Answered::Gone), "an entry is not a wait");

    let parked_run = queued_run(&store, project).await;
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&parked_run]).await.unwrap();
    seed_wait(&store, "tok-parked", project, parked_run).await;
    let Answered::Reached { consumed } = store.answer("tok-parked", &weft_core::primitive::WaitAnswer::Given { value: json!("yes") }).await.unwrap() else { panic!("reached") };
    assert_eq!(consumed.token, "tok-parked");
    assert!(store.signal_get("tok-parked").await.unwrap().is_none(), "answered once");
    assert_eq!(run_column::<String>(&pool, "state", parked_run).await.as_deref(), Some("queued"), "queued to carry on");
    assert!(matches!(crate::events(store.events_log(parked_run).await.unwrap()).last(),
        Some(RunEvent::SuspensionResolved { token, value, .. }) if token == "tok-parked" && value == &json!("yes")));
    assert!(matches!(store.answer("tok-parked", &weft_core::primitive::WaitAnswer::Given { value: json!("again") }).await.unwrap(), Answered::Gone));

    let driven = queued_run(&store, project).await;
    claim(&pool, driven, project, "worker-a").await;
    seed_wait(&store, "tok-driven", project, driven).await;
    static CHANNELS: &[&str] = &[weft_task_store::parked_fires::PARKED_FIRE_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(pool.config(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    assert!(matches!(store.answer("tok-driven", &weft_core::primitive::WaitAnswer::Given { value: json!(2) }).await.unwrap(), Answered::Reached { .. }));
    assert_eq!(news_of(&pool, driven).await, vec![(project, "answer".to_string())], "the install's news says it was handed, with the write");
    assert!(news_of(&pool, parked_run).await.is_empty(), "an answer written into a record makes no news");
    let named = weft_task_store::runs::run_payload(project, driven);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    assert!(
        heard.woken_before(deadline, |c, p| c == weft_task_store::parked_fires::PARKED_FIRE_CHANNEL && p == named).await.unwrap(),
        "the database announces the answer handed, naming its run"
    );
    assert_eq!(crate::events(store.events_log(driven).await.unwrap()).len(), 1, "nothing written into a record its worker writes");
    assert_eq!(
        weft_task_store::parked_fires::answers_for(&pool, driven).await.unwrap(),
        vec![("tok-driven".to_string(), weft_core::primitive::WaitAnswer::Given { value: json!(2) })]
    );

    // A skip is an answer of its own kind, never a value: written as such
    // into a parked run's record, and handed as such to a driven run's
    // worker.
    let skipped_run = queued_run(&store, project).await;
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&skipped_run]).await.unwrap();
    seed_wait(&store, "tok-skipped", project, skipped_run).await;
    assert!(matches!(store.answer("tok-skipped", &weft_core::primitive::WaitAnswer::Skipped).await.unwrap(), Answered::Reached { .. }));
    assert!(matches!(crate::events(store.events_log(skipped_run).await.unwrap()).last(),
        Some(RunEvent::SuspensionSkipped { token, .. }) if token == "tok-skipped"));
    seed_wait(&store, "tok-driven-skipped", project, driven).await;
    assert!(matches!(store.answer("tok-driven-skipped", &weft_core::primitive::WaitAnswer::Skipped).await.unwrap(), Answered::Reached { .. }));
    assert_eq!(
        weft_task_store::parked_fires::answers_for(&pool, driven).await.unwrap().last(),
        Some(&("tok-driven-skipped".to_string(), weft_core::primitive::WaitAnswer::Skipped))
    );

    let ended = queued_run(&store, project).await;
    seed_wait(&store, "tok-ended", project, ended).await;
    complete(&store, ended).await;
    assert!(matches!(store.answer("tok-ended", &weft_core::primitive::WaitAnswer::Given { value: json!(3) }).await.unwrap(), Answered::RunEnded { .. }));
    assert!(store.signal_get("tok-ended").await.unwrap().is_none());

    // An answer queued while the run's trigger was not live is the wait's
    // one answer: a later one finds the wait answered, and the queued one
    // stays for the drain to hand over.
    let queued_for = queued_run(&store, project).await;
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&queued_for]).await.unwrap();
    seed_wait(&store, "tok-queued", project, queued_for).await;
    assert_eq!(park(&pool, "tok-queued", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);
    assert!(matches!(store.answer("tok-queued", &weft_core::primitive::WaitAnswer::Given { value: json!("second") }).await.unwrap(), Answered::Gone));
    assert!(store.signal_get("tok-queued").await.unwrap().is_some(), "the queued answer still has its wait");
}

/// The consumer listing decodes the same row shape as every signal store
/// read, so a column added to the row reaches this query too: one
/// visible entry signal lists, with its node (a trigger with no
/// activation row yet is visible).
#[weft_db_macros::test]
async fn the_consumer_listing_reads_the_whole_signal_row(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&store, "tok-entry", project).await;

    let listed = signals_visible_to(&pool, TENANT, &[], &[], None).await.expect("listing decodes");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].token, "tok-entry");
    assert_eq!(listed[0].node_id, "trigger-tok-entry");
    assert!(signals_visible_to(&pool, "someone-else", &[], &[], None).await.unwrap().is_empty());
}

/// The signal-token listing shows an entry form as a trigger
/// (`isResume: false`, its rendered kind kept) and goes on showing it
/// after a fire was queued on it, since an entry starts runs for as long
/// as it is registered; a run's question is listed as a resume until it
/// is answered, and gone from the listing once it is.
#[weft_db_macros::test]
async fn an_entry_form_lists_as_a_trigger_before_and_after_a_fire(pool: Pool) {
    use weft_dispatcher::api::signal::consumer_listing;
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let rendered = |node: &str| json!({ "kind": "form", "node": node });
    let mut entry = entry_signal("tok-start", project);
    entry.node_id = "start".into();
    entry.consumer_payload = Some(rendered("start"));
    store.signal_insert(&entry).await.unwrap();
    let run_id = queued_run(&store, project).await;
    for token in ["tok-ask", "tok-answered"] {
        let mut wait = entry_signal(token, project);
        wait.node_id = format!("wait-{token}");
        wait.is_resume = true;
        wait.run_id = Some(run_id);
        wait.consumer_payload = Some(rendered(&wait.node_id));
        store.signal_insert(&wait).await.unwrap();
    }
    assert_eq!(park(&pool, "tok-start", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked, "a fire queued on the entry");
    assert_eq!(park(&pool, "tok-answered", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked, "an answer queued on the question");

    let listed = consumer_listing(signals_visible_to(&pool, TENANT, &[], &[], None).await.unwrap());
    let shown: Vec<(&str, &str, bool)> = listed
        .iter()
        .map(|payload| (payload["node"].as_str().unwrap(), payload["kind"].as_str().unwrap(), payload["isResume"].as_bool().unwrap()))
        .collect();
    assert_eq!(shown, vec![("start", "form", false), ("wait-tok-ask", "form", true)]);
}

/// A run whose build's worker cannot start ends every run queued on that
/// build, and only those: a parked run of the build waits on in its
/// record, and a run queued on another build of the project is not
/// touched. A new build of the project moves no paused run: it resumes on
/// the build it was born on until a resync moves it.
#[weft_db_macros::test]
async fn a_worker_that_cannot_start_ends_the_runs_queued_on_its_build_alone(pool: Pool) {
    use weft_core::context::Phase;
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let on = |binary: &'static str| {
        let store = &store;
        async move {
            let run_id = weft_core::new_run_id();
            assert!(queue(store, &[birth(run_id, project, Phase::Fire, binary)], false).await.unwrap());
            run_id
        }
    };
    let (first, second, paused, other) = (on("bin-A").await, on("bin-A").await, on("bin-A").await, on("bin-B").await);
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&paused]).await.unwrap();

    let mut ending = weft_dispatcher::delivery::queued_on_build(&pool, project, Some("bin-A")).await.unwrap();
    ending.sort();
    let mut expected = vec![first, second];
    expected.sort();
    assert_eq!(ending, expected, "the queued runs of that build, never the parked one nor another build's");

    seed_project(&projects, project, "bin-C").await;
    assert_eq!(run_column::<String>(&pool, "binary_hash", paused).await.as_deref(), Some("bin-A"), "a build moves no paused run");
    assert_eq!(run_column::<String>(&pool, "binary_hash", other).await.as_deref(), Some("bin-B"));
}

/// The park names its refusal instead of returning "0 rows": a re-run
/// that finds its own event queued (nothing lost), a wait already
/// answered, an entry queue at its cap (a refused NEW event, a loss the
/// caller must say out loud), and a vanished signal (the project was wiped
/// under the fire) are four different facts. A signal going takes its
/// queue along.
#[weft_db_macros::test]
async fn a_park_names_its_refusal(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&store, "tok-entry", project).await;
    let waiting_run = queued_run(&store, project).await;
    seed_wait(&store, "tok-resume", project, waiting_run).await;

    let first = parked(0, 0);
    assert_eq!(park(&pool, "tok-entry", &first, None).await.unwrap(), ParkAppend::Parked);
    assert_eq!(
        park(&pool, "tok-entry", &Waiting { attempts: 9, not_before: 99, ..first.clone() }, None).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::AlreadyQueued),
        "a re-run of a task that already parked this event finds it"
    );

    park(&pool, "tok-resume", &parked(0, 0), None).await.unwrap();
    assert_eq!(
        park(&pool, "tok-resume", &parked(0, 0), None).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered),
        "one answer resolves one wait; a second is a duplicate"
    );

    // Fill the entry queue to its cap in one write, then a NEW event is
    // refused (a loss, named as such).
    pool.execute(
        "INSERT INTO parked_fire (token, fire_id, payload, attempts, not_before, is_resume) \
         SELECT 'tok-entry', gen_random_uuid(), '{}', 0, 0, FALSE FROM generate_series(2, $1::bigint)",
        &[&weft_task_store::parked_fires::MAX_PARKED_ENTRY_FIRES],
    )
    .await
    .expect("fill the queue to the cap");
    assert_eq!(park(&pool, "tok-entry", &parked(0, 0), None).await.unwrap(), ParkAppend::Refused(ParkRefusal::QueueFull));

    store.signal_remove_many(&["tok-entry".to_string()]).await.unwrap();
    let left: i64 = pool.scalar("SELECT COUNT(*) FROM parked_fire WHERE token = 'tok-entry'", &[]).await.unwrap();
    assert_eq!(left, 0, "a signal going takes its queue along");
    assert_eq!(
        park(&pool, "tok-entry", &parked(0, 0), None).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::RowGone),
        "a vanished signal means the trigger was wiped under the fire"
    );
}

/// The sweep's selection, against the real statement: only triggers that
/// are live, only tokens whose HEAD is due. A backing-off head blocks its
/// whole token (FIFO: a later event may not overtake it).
#[weft_db_macros::test]
async fn the_sweep_selects_only_due_heads_on_active_triggers(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let active = Uuid::new_v4();
    let inactive = Uuid::new_v4();
    seed_project(&projects, active, "bin-A").await;
    seed_project(&projects, inactive, "bin-B").await;
    // The due trigger's activation is Active, the other project's is
    // parked; the rest have no activation row, which reads as live.
    let activations = PostgresActivationStore::new(pool.clone());
    for (project, trigger, to) in [
        (active, "trigger-tok-due", ActivationLifecycle::active()),
        (inactive, "trigger-tok-inactive", ActivationLifecycle::holding()),
    ] {
        let setup_run_id = Uuid::new_v4();
        let key = ActivationKey::new(trigger, Owner::Shared);
        assert!(activations.try_begin_activating(project, &[key], setup_run_id, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
        activations.end_activating(project, setup_run_id, &to, false, None).await.unwrap().expect("owned");
    }

    let now = weft_dispatcher::lease::now_unix();
    seed_parked_signal(&store, "tok-due", active).await;
    park(&pool, "tok-due", &parked(0, now - 10), None).await.unwrap();
    seed_parked_signal(&store, "tok-later", active).await;
    park(&pool, "tok-later", &parked(3, now + 300), None).await.unwrap();
    park(&pool, "tok-later", &parked(0, now - 10), None).await.unwrap();
    seed_parked_signal(&store, "tok-inactive", inactive).await;
    park(&pool, "tok-inactive", &parked(0, now - 10), None).await.unwrap();

    assert_eq!(due_tokens(&pool, now).await.unwrap(), ["tok-due"],
        "due heads on live triggers only; a backing-off head holds its queue, and a parked trigger's queue is left alone");
    assert_eq!(next_due(&pool).await.unwrap(), Some(now - 10));
}

/// One trigger's events are handed over in the order they came, and two
/// drains never take one head: the second finds the head locked and takes
/// nothing (not the event behind it, which would overtake it).
#[weft_db_macros::test]
async fn two_drains_never_take_one_head_and_events_leave_in_order(pool: Pool) {
    use weft_task_store::parked_fires::{remove_in, restamp_in, take_head};
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&store, "tok", project).await;
    let (first, second) = (parked(0, 0), parked(0, 0));
    park(&pool, "tok", &first, None).await.unwrap();
    park(&pool, "tok", &second, None).await.unwrap();
    let now = weft_dispatcher::lease::now_unix();

    let mut one_conn = pool.get().await.unwrap();
    let one = one_conn.transaction().await.unwrap();
    let head = take_head(&one, "tok", now).await.unwrap().expect("the head");
    assert_eq!(head.waiting.fire_id, first.fire_id, "first in, first out");
    let mut other_conn = pool.get().await.unwrap();
    let other = other_conn.transaction().await.unwrap();
    assert!(take_head(&other, "tok", now).await.unwrap().is_none(), "a held head is skipped, and nothing overtakes it");
    other.rollback().await.unwrap();

    // A head that could not be handed over stays the head, backing off.
    let turn = weft_task_store::parked_fires::Turn { attempts: 1, not_before: now + 60, waits_for: None, failures: 0 };
    restamp_in(&one, &head, &turn).await.unwrap();
    one.commit().await.unwrap();
    let again = one_conn.transaction().await.unwrap();
    assert!(take_head(&again, "tok", now).await.unwrap().is_none(), "backing off, it holds its queue");
    let head = take_head(&again, "tok", now + 60).await.unwrap().expect("due again");
    assert_eq!((head.waiting.fire_id, head.waiting.attempts), (first.fire_id, 1));
    remove_in(&again, &head).await.unwrap();
    again.commit().await.unwrap();

    let last = one_conn.transaction().await.unwrap();
    assert_eq!(take_head(&last, "tok", now).await.unwrap().expect("the next").waiting.fire_id, second.fire_id);
}

/// An event parked because its instance has not filled a value never comes
/// due on a timer: the sweep and its next-due read pass its head by,
/// whatever its stamp says. The instance's rows are what a change of their
/// values routes again, and `weft status` counts them per trigger with
/// the latest reason.
#[weft_db_macros::test]
async fn fires_waiting_on_an_instance_value_wait_for_the_instance(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&store, "tok-ada", project).await;
    seed_parked_signal(&store, "tok-timer", project).await;
    pool.execute("UPDATE signal SET instance_id = 'ada' WHERE token = 'tok-ada'", &[])
        .await
        .expect("make tok-ada ada's");
    let gap = |reason: &str| Waiting { waits_for: WaitsFor::new(WaitsOn::Instance, reason), ..parked(1, 0) };
    park(&pool, "tok-ada", &gap("instance 'ada' at 'answer': 'key' is not filled"), None).await.unwrap();
    park(&pool, "tok-ada", &gap("instance 'ada' at 'answer': 'model' is not filled"), None).await.unwrap();
    park(&pool, "tok-timer", &parked(1, 0), None).await.unwrap();

    let now = weft_dispatcher::lease::now_unix();
    assert_eq!(due_tokens(&pool, now).await.unwrap(), ["tok-timer"], "only the timer-retried head is due");
    assert_eq!(next_due(&pool).await.unwrap(), Some(0), "the next-due read sees the timer head alone");

    let ada = InstanceId::new("ada").unwrap();
    assert_eq!(instance_waiting_tokens(&pool, project, &ada).await.unwrap(), ["tok-ada"]);
    assert!(instance_waiting_tokens(&pool, project, &InstanceId::new("bob").unwrap()).await.unwrap().is_empty());

    let waits = waiting_fires(&pool, project).await.unwrap();
    let key = ActivationKey::new("trigger-tok-ada", Owner::from_instance(Some(ada)));
    let waiting = waits.get(&key).expect("ada's trigger waits");
    assert_eq!((waiting.fires, waiting.on), (2, weft_task_store::parked_fires::UnclockedWait::Instance));
    assert_eq!(waiting.reason, "instance 'ada' at 'answer': 'model' is not filled", "the event parked last");
    assert_eq!(waits.len(), 1, "an event retried on its timer is not waiting on an instance");
}

/// A queued event whose hand-over keeps failing is tried twice more
/// shortly, then stops: no clock brings it again, its trigger's queue
/// waits behind it, `weft status` shows it, and what may have fixed it
/// (an activation, a new version, the worker settings) takes it whatever
/// its turn says. One waiting on infra is what infra coming back hands
/// over, and nothing else.
#[weft_db_macros::test]
async fn a_failing_event_stops_its_queue_until_something_may_have_fixed_it(pool: Pool) {
    use weft_dispatcher::parked_drain::tokens_waiting_on;
    use weft_task_store::parked_fires::{restamp_in, take_head, take_head_now, waiting, Turn};
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&store, "tok-fail", project).await;
    seed_parked_signal(&store, "tok-infra", project).await;
    let failed = waiting(Uuid::new_v4(), json!({}), None, 0, WaitsOn::Failure, "the worker could not be reached");
    park(&pool, "tok-fail", &failed, None).await.unwrap();
    park(&pool, "tok-fail", &parked(0, 0), None).await.unwrap();
    park(&pool, "tok-infra", &waiting(Uuid::new_v4(), json!({}), None, 0, WaitsOn::Infra, "infra 'db' is not running"), None).await.unwrap();

    let now = weft_dispatcher::lease::now_unix();
    assert!(due_tokens(&pool, now).await.unwrap().is_empty(), "the first failure waits a few seconds; infra waits on no clock");
    assert_eq!(next_due(&pool).await.unwrap(), Some(failed.not_before));
    for tried in 2..=3 {
        let mut conn = pool.get().await.unwrap();
        let tx = conn.transaction().await.unwrap();
        let head = take_head(&tx, "tok-fail", failed.not_before + 3600).await.unwrap().expect("due again");
        let turn = Turn::next(head.waiting.attempts, head.waiting.failures, WaitsOn::Failure, "the worker could not be reached", now);
        assert_eq!(turn.failures, tried);
        restamp_in(&tx, &head, &turn).await.unwrap();
        tx.commit().await.unwrap();
    }
    assert!(due_tokens(&pool, now + 3600).await.unwrap().is_empty(), "stopped: no clock brings it, and its queue waits behind it");
    assert_eq!(next_due(&pool).await.unwrap(), None, "nothing to book a look at");

    let waits = waiting_fires(&pool, project).await.unwrap();
    let stopped = waits.get(&ActivationKey::new("trigger-tok-fail", Owner::Shared)).expect("shown");
    use weft_task_store::parked_fires::UnclockedWait;
    assert_eq!((stopped.fires, stopped.on, stopped.reason.as_str()), (1, UnclockedWait::Failure, "the worker could not be reached"));
    assert_eq!(tokens_waiting_on(&pool, UnclockedWait::Failure, Some(project)).await.unwrap(), ["tok-fail"]);
    assert!(tokens_waiting_on(&pool, UnclockedWait::Failure, Some(Uuid::new_v4())).await.unwrap().is_empty());
    assert_eq!(tokens_waiting_on(&pool, UnclockedWait::Infra, None).await.unwrap(), ["tok-infra"]);

    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    let head = take_head_now(&tx, "tok-fail").await.unwrap().expect("what may have fixed it takes it");
    assert_eq!(head.waiting.fire_id, failed.fire_id);
}

/// Every status reader goes through `infra_node::observe`, which reads
/// the copies with the commands the supervisor has not finished applied:
/// an instance's start queued over its stopped copy reads provisioning at
/// once (not the old `stopped`), a queued stop of a running copy reads
/// stopping, and a start of a copy with no row yet lists it as starting.
#[weft_db_macros::test]
async fn copies_read_with_the_commands_under_way(pool: Pool) {
    use weft_broker::lifecycle_writes::{issue_command, IssuedCommand};
    use weft_dispatcher::infra_lifecycle_command::{issue_lifecycle, InfraLifecycleVerb, Teardown};
    use weft_dispatcher::infra_node::{observe, InfraNodeStatus};
    let (_store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    for (node, instance, status) in [("bridge", Some("ada"), "stopped"), ("db", None, "running"), ("cache", None, "running")] {
        pool.execute(
            "INSERT INTO infra_node (project_id, node_id, instance_id, copy_id, status, health_rules_json_version) \
             VALUES ($1, $2, $3, 'inst', $4, 1)",
            &[&project, &node, &instance, &status],
        )
        .await
        .expect("seed a copy");
    }
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    issue_lifecycle(&pool, TENANT, project, Some("db"), &weft_core::instance::Copies::Shared, Teardown::Stop { force: false }, false, &[], "disp-1")
        .await
        .unwrap();
    for instance in [&ada, &bob] {
        let spec = json!({});
        let apply = IssuedCommand {
            tenant_id: TENANT,
            project_id: project,
            node_id: Some("bridge"),
            copies: &weft_core::instance::Copies::Instance(instance.clone()),
            verb: InfraLifecycleVerb::Apply,
            spec_json: Some(&spec),
            source_hash: None,
            issued_by_replica: "worker-1",
        };
        issue_command(&pool, &apply).await.unwrap().expect("the project row is there");
    }

    let copies = observe(&pool, project, &empty_project(project)).await.unwrap();
    assert_eq!(copies.status_of("bridge", Some(&ada)), Some(InfraNodeStatus::Provisioning));
    assert_eq!(copies.status_of("db", None), Some(InfraNodeStatus::Stopping));
    assert_eq!(copies.status_of("cache", None), Some(InfraNodeStatus::Running), "nothing under way reads the row");
    assert_eq!(copies.status_of("bridge", Some(&bob)), Some(InfraNodeStatus::Provisioning));
    assert_eq!(copies.starting, vec![("bridge".to_string(), Some(bob))]);
    assert_eq!(copies.status_of("bridge", None), None, "no shared copy, nothing starting one");
}

/// A copy being started counts from when the start was asked for, before
/// the supervisor writes any row: an instance's copy with no row yet, and
/// one restarted over its stopped row (which holds no stamp of its own),
/// both read as provisioning since their apply command was issued, on the
/// database's clock.
#[weft_db_macros::test]
async fn a_starting_copy_counts_from_its_request(pool: Pool) {
    use weft_broker::lifecycle_writes::{issue_command, IssuedCommand};
    use weft_dispatcher::infra_lifecycle_command::InfraLifecycleVerb;
    use weft_dispatcher::infra_node::observe;
    let (_store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    pool.execute(
        "INSERT INTO infra_node (project_id, node_id, instance_id, copy_id, status, health_rules_json_version) VALUES ($1, 'bridge', 'ada', 'inst', 'stopped', 1)",
        &[&project],
    )
    .await
    .expect("seed ada's stopped copy");
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    let mut issued = Vec::new();
    for (instance, asked_unix) in [(&ada, 1_000_i64), (&bob, 2_000)] {
        let spec = json!({});
        let apply = IssuedCommand {
            tenant_id: TENANT,
            project_id: project,
            node_id: Some("bridge"),
            copies: &weft_core::instance::Copies::Instance(instance.clone()),
            verb: InfraLifecycleVerb::Apply,
            spec_json: Some(&spec),
            source_hash: None,
            issued_by_replica: "worker-1",
        };
        let id = issue_command(&pool, &apply).await.unwrap().expect("the project row is there");
        // Asked well in the past, so the count cannot pass by luck.
        pool.execute("UPDATE infra_lifecycle_command SET issued_at_unix = $2 WHERE id = $1", &[&id, &asked_unix])
            .await
            .unwrap();
        issued.push(asked_unix);
    }

    let copies = observe(&pool, project, &empty_project(project)).await.unwrap();
    let (db_now,): (i64,) = pool.one("SELECT EXTRACT(EPOCH FROM NOW())::BIGINT", &[]).await.unwrap();
    let restarted = copies.progress_of("bridge", Some(&ada)).expect("a restart over a stopped row shows progress");
    assert_eq!(restarted.since_unix, issued[0]);
    let fresh = copies.progress_of("bridge", Some(&bob)).expect("a start with no row yet shows progress");
    assert_eq!(fresh.since_unix, issued[1]);
    assert_eq!(fresh.waiting, None, "nothing has reported what it waits on yet");
    assert!(fresh.as_of_unix <= db_now && fresh.as_of_unix > issued[1], "read on the database's clock: {}", fresh.as_of_unix);

    // Once the supervisor stamps the row later, the request still counts.
    pool.execute("UPDATE infra_node SET status = 'provisioning', provisioning_since_unix = 5000 WHERE project_id = $1", &[&project])
        .await
        .unwrap();
    let copies = observe(&pool, project, &empty_project(project)).await.unwrap();
    assert_eq!(copies.progress_of("bridge", Some(&ada)).map(|p| p.since_unix), Some(issued[0]), "the request came first");
}

/// What holds the program's own verbs back is infra work on the copies
/// the bar starts and stops: an instance's copy coming up holds nothing
/// back, the shared copies' work does, and so does the project going.
#[weft_db_macros::test]
async fn only_work_on_the_shared_copies_holds_the_program_back(pool: Pool) {
    use weft_dispatcher::infra_lifecycle_command::{any_in_flight, issue_lifecycle, Teardown};
    use weft_core::instance::Copies;
    let (_store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let stop = |copies: Copies| {
        let pool = pool.clone();
        async move {
            issue_lifecycle(&pool, TENANT, project, Some("db"), &copies, Teardown::Stop { force: false }, false, &[], "disp-1")
                .await
                .unwrap()
        }
    };
    let finish = |id: i64| {
        let pool = pool.clone();
        async move {
            pool.execute("UPDATE infra_lifecycle_command SET completed_at_unix = 1 WHERE id = $1", &[&id]).await.unwrap();
        }
    };

    let ada = stop(Copies::Instance(InstanceId::new("ada").unwrap())).await;
    assert!(!any_in_flight(&pool, project).await.unwrap(), "an instance's copy holds nothing back");
    finish(ada).await;
    let shared = stop(Copies::Shared).await;
    assert!(any_in_flight(&pool, project).await.unwrap(), "the shared copies' work does");
    finish(shared).await;
    stop(Copies::Every).await;
    assert!(any_in_flight(&pool, project).await.unwrap(), "and so does the project going");
}

/// The claim answers every named row as it was before, and a failure
/// before setup touched anything puts each back as it was: a parked
/// trigger parked, and one never activated without a row, so it reads
/// as never activated rather than as wiped.
#[weft_db_macros::test]
async fn a_failed_activation_puts_the_triggers_back_as_the_claim_found_them(pool: Pool) {
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let fresh = ActivationKey::new("fresh", Owner::Shared);
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claim");
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    activations.set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None).await.unwrap();

    let second = Uuid::new_v4();
    let previous = activations
        .try_begin_activating(id, &[feed_key(), fresh.clone()], second, None, Claimant::Anyone, Uuid::new_v4())
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(previous.len(), 1, "only the trigger activated before had a row");
    assert_eq!(previous[0].key, feed_key());
    assert_eq!(previous[0].lifecycle.mode().as_str(), "take");
    let during: Vec<_> = activations.list(id).await.unwrap();
    assert!(during.iter().all(|a| a.lifecycle.status == ProjectStatus::Activating), "both read activating at once");

    assert!(activations.restore(id, second, &previous).await.unwrap());
    let after = activations.list(id).await.unwrap();
    assert_eq!(after.len(), 1, "the never-activated trigger has no row again");
    assert_eq!(after[0].key, feed_key());
    assert_eq!(after[0].lifecycle.mode().as_str(), "take");
    assert!(!activations.restore(id, second, &previous).await.unwrap(), "a second restore finds no claim");
}

/// Everywhere one run lives, so a table dropped from the erase list
/// cannot ship quietly.
async fn footprint(pool: &Pool, run_id: weft_core::RunId) -> i64 {
    let mut rows = 0;
    for table in ["run", "run_log", "run_tag", "trigger_setup", "run_search_queue", "run_search"] {
        rows += rows_of(pool, table, run_id).await;
    }
    rows + pool.scalar::<i64>(
        "SELECT (SELECT COUNT(*) FROM signal WHERE run_id = $1 AND is_resume) \
              + (SELECT COUNT(*) FROM parked_fire WHERE run_id = $1)",
        &[&run_id],
    )
    .await
    .unwrap()
}

/// Removing a project frees the space its history took: every table a
/// run touches loses its rows, for every run of that
/// project and no other's, in the removal's own transaction, so a run
/// started for it afterwards finds no project and nothing is orphaned.
///
/// This is the test the erase list needs, because a table left out of
/// it fails silently. The rows are simply still there, on a path
/// nobody runs twice, reachable by nothing.
#[weft_db_macros::test]
async fn removing_a_projects_runs_frees_every_table_they_touched(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let doomed = Uuid::new_v4();
    let neighbour = Uuid::new_v4();
    seed_project(&projects, doomed, "bin-A").await;
    seed_project(&projects, neighbour, "bin-A").await;

    let first = queued_run(&store, doomed).await;
    let second = queued_run(&store, doomed).await;
    let survivor = queued_run(&store, neighbour).await;

    // Everything else a run leaves behind, on the first run.
    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    weft_journal::tags::tag_run_in(&tx, first, &["user_7".to_string()], 10)
        .await
        .expect("tag");
    tx.commit().await.unwrap();
    pool.execute("INSERT INTO trigger_setup (project_id, run_id) VALUES ($1, $2)", &[&doomed, &first])
        .await
        .expect("trigger_setup");
    for (token, run_id, is_resume) in [
        ("resume-tok", Some(first), true),
        ("entry-tok", None, false),
    ] {
        pool.execute(
            "INSERT INTO signal \
             (token, tenant_id, project_id, run_id, node_id, is_resume, spec_json, created_at, spec_json_version, program_json_version) \
             VALUES ($1, $2, $3, $4, 'wait', $5, '{}', 1, $6, 1)",
            &[&token, &TENANT, &doomed, &run_id, &is_resume, &weft_core::primitive::SignalSpec::STORED.version()],
        )
        .await
        .expect("signal");
    }
    let tx = conn.transaction().await.unwrap();
    weft_task_store::parked_fires::hand_answer_in(&tx, "answered-tok", first, &weft_core::primitive::WaitAnswer::Given { value: json!(1) }).await.expect("a handed answer");
    tx.execute("INSERT INTO run_search_queue (run_id) VALUES ($1) ON CONFLICT DO NOTHING", &[&first])
        .await
        .expect("waiting for the index");
    tx.commit().await.unwrap();
    assert!(footprint(&pool, first).await > 0, "the run left something behind to erase");

    let removed = projects.remove(doomed).await.expect("remove the project").expect("it was there");
    let mut tokens: Vec<String> = removed.into_iter().map(|s| s.token).collect();
    tokens.sort();
    assert_eq!(tokens, ["entry-tok", "resume-tok"], "its run's wait and its entry, for the listener to let go of");

    assert_eq!(footprint(&pool, first).await, 0, "nothing of the first run is left");
    assert_eq!(footprint(&pool, second).await, 0, "nothing of the second run is left");
    assert!(footprint(&pool, survivor).await > 0, "the neighbour project is untouched");

    // A run started for it from now on finds no project.
    let late = weft_core::new_run_id();
    let refused = queue(&store, &[birth(late, doomed, weft_core::context::Phase::Fire, "bin-A")], false).await;
    let why = format!("{:#}", refused.unwrap_err());
    assert!(why.contains("has no row"), "a late start is refused: {why}");
    assert_eq!(footprint(&pool, late).await, 0);
}

/// Removing a project takes, in one transaction, everything kept under
/// it that its runs needed or its triggers ran on: its signals (handed back
/// for the listener to let go of), its queued and claimed work, its
/// recorded programs, its version tree and its infra lease. Another
/// project's are never touched, and a project already gone removes
/// nothing.
#[weft_db_macros::test]
async fn removing_a_project_takes_everything_kept_under_it(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let live = Uuid::from_u128(1);
    let removed = Uuid::from_u128(2);
    seed_project(&projects, live, "bin").await;
    seed_project(&projects, removed, "bin").await;
    let queue = |project: Uuid, key: &'static str| {
        let pool = pool.clone();
        async move {
            weft_task_store::tasks::enqueue_dedup(
                &pool,
                weft_task_store::tasks::NewTask {
                    kind: weft_task_store::TaskKind::RegisterSignal,
                    project_id: Some(project),
                    dedup_key: Some(key.into()),
                    run_id: None,
                    tenant_id: TENANT.into(),
                    payload: json!({}),
                },
            )
            .await
            .unwrap();
        }
    };
    queue(live, "a").await;
    queue(removed, "b").await;
    queue(removed, "claimed").await;
    pool.execute("UPDATE task SET status = 'claimed' WHERE dedup_key = 'claimed'", &[]).await.unwrap();
    store.signal_insert(&entry_signal("live-entry", live)).await.unwrap();
    store.signal_insert(&entry_signal("gone-entry", removed)).await.unwrap();
    for (project, replica) in [(live, "sup-live"), (removed, "sup-gone")] {
        pool.execute(
            "INSERT INTO infra_owner (project_id, supervisor_replica, tenant_id, leased_until_unix) VALUES ($1, $2, $3, $4)",
            &[&project, &replica, &TENANT, &(weft_dispatcher::lease::now_unix() + 60)],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO infra_health_rule (project_id, node_id, rule, consecutive_failures) VALUES ($1, 'db', 'take-down-while-flaky', 1)",
            &[&project],
        )
        .await
        .unwrap();
    }
    let programs = |project: Uuid| {
        let pool = pool.clone();
        async move {
            pool.scalar::<i64>("SELECT COUNT(*) FROM project_definition WHERE project_id = $1", &[&project])
                .await
                .unwrap()
        }
    };
    assert!(programs(removed).await > 0, "the project recorded its program");

    let signals = projects.remove(removed).await.unwrap().expect("the project was there");
    assert_eq!(signals.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["gone-entry"], "handed back to let go of");
    let tasks: Vec<(Uuid, String)> =
        pool.all("SELECT project_id, dedup_key FROM task ORDER BY dedup_key", &[]).await.unwrap();
    assert_eq!(tasks, vec![(live, "a".to_string())], "its work went, the claimed one too");
    assert!(store.signal_get("live-entry").await.unwrap().is_some());
    assert_eq!(programs(removed).await, 0);
    assert!(programs(live).await > 0);
    let leases: Vec<Uuid> = pool.scalars("SELECT project_id FROM infra_owner", &[]).await.unwrap();
    assert_eq!(leases, vec![live], "its supervisor lease went with its row");
    let health: Vec<Uuid> = pool.scalars("SELECT project_id FROM infra_health_rule", &[]).await.unwrap();
    assert_eq!(health, vec![live], "where its health rules stood went too");
    assert!(projects.remove(removed).await.unwrap().is_none(), "nothing left to remove");
}

/// Once `weft rm` marked a project, no run of it is born: neither one the
/// dispatcher queues nor one a worker bears in a batch, so the take-down
/// that follows reaches every run the project has. Another project's runs
/// are still born, and a project with no row cannot be marked.
#[weft_db_macros::test]
async fn a_project_being_removed_has_no_run_born(pool: Pool) {
    use weft_journal::frame::{Batch, BatchHead, RunHead};
    use weft_journal::record::{Born, Fate, Written};
    let (store, projects) = setup(&pool).await;
    let (doomed, kept) = (Uuid::from_u128(1), Uuid::from_u128(2));
    seed_project(&projects, doomed, "bin").await;
    seed_project(&projects, kept, "bin").await;
    assert!(projects.mark_removing(doomed).await.unwrap());
    assert!(!projects.mark_removing(Uuid::from_u128(3)).await.unwrap(), "no row to mark");
    assert!(projects.get(doomed).await.unwrap().expect("still there").removing, "it reads as being removed");
    let registered = projects
        .register_with_hashes(empty_project(doomed), "doomed", "", TENANT, None, None, None, None, None, None, None, None)
        .await
        .expect_err("a project being removed is not registered over");
    assert!(weft_dispatcher::project_store::ProjectRemoving::is(&registered), "{registered:#}");

    let refused = queue(&store, &[birth(weft_core::new_run_id(), doomed, weft_core::context::Phase::Fire, "bin")], false)
        .await
        .expect_err("a queued run of a project being removed is refused");
    assert!(refused.to_string().contains("is being removed"), "{refused}");
    queued_run(&store, kept).await;

    let bear = |project: Uuid| {
        let pool = pool.clone();
        async move {
            let run_id = weft_core::new_run_id();
            let events = vec![birth(run_id, project, weft_core::context::Phase::Fire, "bin")];
            let row = weft_journal::stored::encode(&events);
            let head = BatchHead {
                durable: true,
                lane: 0,
                runs: vec![RunHead {
                    run_id,
                    epoch: 1,
                    first_seq: 0,
                    row_sizes: vec![row.len() as u32],
                    born: Born::of(&events[0], weft_core::run_settings::KeepFor::WEFT_DEFAULT),
                    written: Written::of(&events),
                    secrets: Vec::new(),
                }],
                programs: Vec::new(),
                events_version: weft_journal::stored::STORED_EVENTS.version(),
            };
            let batch = Batch { head, rows: vec![row.as_slice()] };
            let answers = weft_journal::record::record_batch(&pool.get().await.unwrap(), "w1", TENANT, "w1:0", &batch, &weft_access_store::seal_json).await.unwrap();
            (answers[0].fate, run_column::<String>(&pool, "state", run_id).await)
        }
    };
    assert_eq!(bear(doomed).await, (Fate::ProjectRemoving, None), "a worker's birth for a project being removed goes nowhere, and says why");
    assert_eq!(bear(kept).await, (Fate::Accepted, Some("running".to_string())));
}

/// Removing a project takes its instances' connections, values and tokens:
/// each acts only in this project, and nobody could list them to delete
/// them once it is gone. The author's own connections stay theirs.
#[weft_db_macros::test]
async fn removing_a_project_takes_what_its_instances_had(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let grant = |instance: Option<&'static str>| {
        let pool = pool.clone();
        async move {
            let grant = Uuid::new_v4();
            pool.execute(
                "INSERT INTO access_grant (id, tenant_id, service, spec, values_sealed, project_id, instance_id, spec_version, exclusive) \
                 VALUES ($1, $2, 'svc', '{}', '', $3, $4, $5, FALSE)",
                &[&grant, &TENANT, &id, &instance, &weft_core::AccessSpec::STORED.version()],
            ).await.expect("grant");
            grant
        }
    };
    let adas = grant(Some("ada")).await;
    let authors = grant(None).await;
    pool.execute(
        "INSERT INTO instance_value (tenant_id, project_id, instance_id, node, field, value, grant_id) \
         VALUES ($1, $2, 'ada', 'post', 'account', '{}', $3), ($1, $2, 'ada', 'digest', 'cron', '\"0 0 3 * * *\"', NULL)",
        &[&TENANT, &id, &adas],
    ).await.expect("values");
    let token = |hash: &str, instance: Option<InstanceId>, expires_at: Option<u64>| weft_dispatcher::store::SignalToken {
        id: Uuid::new_v4(),
        token_hash: hash.into(),
        recognizer: "wft-test-...".into(),
        tenant_id: TENANT.into(),
        name: None,
        allowed_projects: vec![id],
        allowed_tags: Vec::new(),
        allowed_displays: Vec::new(),
        all_displays: false,
        created_at: 0,
        instance,
        expires_at,
        kind: weft_core::signal_token::TokenKind::Caller,
    };
    store.mint_signal_token(&token("ada-token", Some(InstanceId::new("ada").unwrap()), Some(10))).await.unwrap();
    store.mint_signal_token(&token("author-token", None, None)).await.unwrap();

    projects.remove(id).await.unwrap();
    let grants: Vec<Uuid> = pool.scalars("SELECT id FROM access_grant", &[]).await.unwrap();
    assert_eq!(grants, vec![authors], "the author's connection stays theirs");
    let values: i64 = pool.scalar("SELECT COUNT(*) FROM instance_value", &[]).await.unwrap();
    assert_eq!(values, 0, "an instance's values go with the project, connections or not");
    assert!(store.get_signal_token("ada-token").await.unwrap().is_none());
    assert!(store.get_signal_token("author-token").await.unwrap().is_some(), "the author revokes their own tokens");
}

/// Taking a whole project down removes every signal it has, entry
/// signals included (their run is NULL), and keeps only the waits of
/// the run it is told to spare.
#[weft_db_macros::test]
async fn a_whole_project_take_down_removes_its_entry_signals(pool: Pool) {
    use weft_dispatcher::store::postgres::remove_project_signals_except;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let spared = queued_run(&store, id).await;
    let mut wait = entry_signal("spared-wait", id);
    wait.is_resume = true;
    wait.run_id = Some(spared);
    wait.node_id = "ask".into();
    store.signal_insert(&entry_signal("entry", id)).await.unwrap();
    store.signal_insert(&wait).await.unwrap();

    let removed = remove_project_signals_except(&pool, id, Some(spared)).await.unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["entry"]);
    let removed = remove_project_signals_except(&pool, id, None).await.unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["spared-wait"]);
}

/// A per-instance trigger has one entry per instance at the same place, and
/// each copy reads its own: an instance's display never shows another
/// instance's registration, and the shared read sees none of them.
#[weft_db_macros::test]
async fn an_entry_is_read_for_its_own_instance(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    for (token, instance) in [("tok-a", "alice"), ("tok-b", "bob")] {
        let signal = SignalRegistration {
            instance: Some(weft_core::instance::InstanceId::new(instance).unwrap()),
            ..entry_signal(token, id)
        };
        store.signal_insert(&signal).await.expect("insert an instance's entry");
    }
    for (token, instance) in [("tok-a", "alice"), ("tok-b", "bob")] {
        let instance = weft_core::instance::InstanceId::new(instance).unwrap();
        let entry = store.signal_entry_at(id, "feed", Some(&instance)).await.unwrap().expect("its entry");
        assert_eq!(entry.token, token);
    }
    assert!(store.signal_entry_at(id, "feed", None).await.unwrap().is_none(), "no shared copy exists");
}

async fn seed_dispatcher_command(pool: &Pool, project_id: Uuid, verb: &str) -> i64 {
    pool.scalar(
        "INSERT INTO infra_lifecycle_command (tenant_id, project_id, verb, issued_by_replica, issued_at_unix) \
         VALUES ($1, $2, $3, 'test-instance', $4) RETURNING id",
        &[&TENANT, &project_id, &verb, &weft_dispatcher::lease::now_unix()],
    )
    .await
    .expect("seed a dispatcher command")
}

async fn complete_command(pool: &Pool, id: i64) {
    pool.execute(
        "UPDATE infra_lifecycle_command SET completed_at_unix = $1 WHERE id = $2",
        &[&weft_dispatcher::lease::now_unix(), &id],
    )
    .await
    .expect("complete command");
}

/// What brings back a trigger an infra take-down took down, read by
/// command ids: never while a take-down reaching its copies is under way;
/// then the last take-down since called off, or an apply after it that
/// succeeded. An apply that landed before the take-down, a take-down that
/// did what it was asked, and commands over other copies say nothing.
#[weft_db_macros::test]
async fn a_trigger_down_with_its_infra_reads_the_commands_since(pool: Pool) {
    use weft_broker::lifecycle_writes::{issue_command, IssuedCommand};
    use weft_dispatcher::infra_lifecycle_command::{issue_lifecycle, since_take_down, InfraLifecycleVerb, SinceTakeDown, Teardown};
    let (_store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let shared = weft_core::instance::Copies::Shared;
    let end = |id: i64, outcome: &'static str| {
        let pool = pool.clone();
        async move {
            pool.execute(
                "UPDATE infra_lifecycle_command SET completed_at_unix = $1, outcome = $2 WHERE id = $3",
                &[&weft_dispatcher::lease::now_unix(), &outcome, &id],
            )
            .await
            .expect("end a command");
        }
    };
    let stop = |node: &'static str| {
        let (pool, shared) = (pool.clone(), shared.clone());
        async move { issue_lifecycle(&pool, TENANT, project, Some(node), &shared, Teardown::Stop { force: false }, false, &[], "disp-1").await.unwrap() }
    };
    let apply = |node: &'static str| {
        let (pool, shared) = (pool.clone(), shared.clone());
        async move {
            let spec = json!({});
            let command = IssuedCommand {
                tenant_id: TENANT,
                project_id: project,
                node_id: Some(node),
                copies: &shared,
                verb: InfraLifecycleVerb::Apply,
                spec_json: Some(&spec),
                source_hash: None,
                issued_by_replica: "worker-1",
            };
            issue_command(&pool, &command).await.unwrap().expect("the project row is there")
        }
    };
    let db = vec![("db".to_string(), None)];
    let since = |first: i64| {
        let (pool, db) = (pool.clone(), db.clone());
        async move { since_take_down(&pool, project, &db, first).await.unwrap() }
    };

    let early = apply("db").await;
    end(early, "succeeded").await;
    let taken = stop("db").await;
    assert_eq!(since(taken).await, SinceTakeDown { under_way: true, returned: false }, "its own take-down is under way");
    end(taken, "cancelled").await;
    assert!(since(taken).await.brings_back(), "called off with the copies as they were");

    let taken = stop("db").await;
    let other = stop("cache").await;
    end(taken, "succeeded").await;
    assert_eq!(since(taken).await, SinceTakeDown { under_way: false, returned: false }, "a stop that did what it was asked, and one of other copies under way");
    end(other, "cancelled").await;
    assert!(!since(taken).await.brings_back(), "another copy's take-down says nothing of these");
    let started = apply("db").await;
    assert!(!since(taken).await.brings_back(), "a start under way has not brought them back yet");
    end(started, "succeeded").await;
    assert!(since(taken).await.brings_back(), "started again after the stop");

    let taken = stop("db").await;
    let again = stop("db").await;
    end(taken, "cancelled").await;
    assert!(!since(taken).await.brings_back(), "a later take-down of the same copies still under way holds it down");
    end(again, "succeeded").await;
    assert!(!since(taken).await.brings_back(), "the last take-down did what it was asked");

    // Its own take-down did what it was asked (a unit its `on_stop` keeps
    // up still reads running); a later one called off says nothing.
    let taken = stop("db").await;
    end(taken, "succeeded").await;
    let later = stop("db").await;
    end(later, "cancelled").await;
    assert!(!since(taken).await.brings_back(), "a take-down that did what it was asked is never undone by a later one called off");
}

/// A drain under a command's claim keeps it until its next look however
/// far that is, so nothing renews it on a clock, and nobody else claims it
/// meanwhile; a cancel of the command is announced once, as it is asked,
/// so the drain stops on it rather than reading the flag on a clock.
#[weft_db_macros::test]
async fn a_drains_claim_lasts_to_its_next_look_and_a_cancel_is_announced(pool: Pool) {
    use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL};
    use weft_dispatcher::lifecycle_claimer::{claim_one, CommandClaim};
    let (_store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let command = seed_dispatcher_command(&pool, id, "deactivate").await;
    assert_eq!(claim_one(&pool, "r1").await.unwrap().expect("claimed").id, command);

    let next_look = weft_dispatcher::lease::now_unix() + 3600;
    assert!(CommandClaim { id: command, replica: "r1" }.keep_until(&pool, next_look).await.unwrap());
    let lapse: i64 = pool.scalar("SELECT claimed_at_unix FROM infra_lifecycle_command WHERE id = $1", &[&command]).await.unwrap();
    let ttl = weft_broker_client::lifecycle_command::CLAIM_LEASE_TTL.as_secs() as i64;
    assert_eq!(lapse + ttl, next_look, "the claim lapses at the drain's next look");
    assert!(claim_one(&pool, "r2").await.unwrap().is_none(), "nobody else takes it meanwhile");
    assert!(!CommandClaim { id: command, replica: "r2" }.keep_until(&pool, next_look).await.unwrap(), "only its holder keeps it");

    static CHANNELS: &[&str] = &[INFRA_COMMAND_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(pool.config(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    let cancel = async || pool.execute("UPDATE infra_lifecycle_command SET cancel_requested = TRUE WHERE id = $1", &[&command]).await;
    let cancelled = |c: &str, p: &str| c == INFRA_COMMAND_CHANNEL && InfraCommandSignal::parse(p) == Some(InfraCommandSignal::Cancel { id: command });
    cancel().await.unwrap();
    let deadline = || tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    assert!(heard.woken_before(deadline(), cancelled).await.unwrap(), "a cancel names its command");
    cancel().await.unwrap();
    assert!(!heard.woken_before(deadline(), cancelled).await.unwrap(), "asked once, announced once");
}

/// A cancel completes a supervisor command nobody claimed, and only flags
/// one a supervisor claimed: it stays pending until the supervisor, which
/// hears the flag, completes it with what it really did.
#[weft_db_macros::test]
async fn cancelling_a_claimed_supervisor_command_leaves_it_pending_for_the_supervisor(pool: Pool) {
    let (_store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let running = seed_dispatcher_command(&pool, id, "stop").await;
    let waiting = seed_dispatcher_command(&pool, id, "apply").await;
    pool.execute("UPDATE infra_lifecycle_command SET claimed_by_replica = 'sup-1', claimed_at_unix = 0 WHERE id = $1", &[&running])
        .await
        .unwrap();
    assert_eq!(weft_dispatcher::infra_lifecycle_command::request_cancel_owner(&pool, id, None).await.unwrap(), 2);
    let row = async |command: i64| {
        pool.one::<(bool, Option<i64>, Option<String>)>(
            "SELECT cancel_requested, completed_at_unix, outcome FROM infra_lifecycle_command WHERE id = $1",
            &[&command],
        )
        .await
    };
    let (flagged, completed, _) = row(running).await.unwrap();
    assert!(flagged && completed.is_none(), "the claimed one is flagged and left to its supervisor");
    let (_, completed, outcome) = row(waiting).await.unwrap();
    assert!(completed.is_some(), "the unclaimed one is done at once");
    assert_eq!(outcome.as_deref(), Some("cancelled"));
}

/// A connection made again, or gone, marks the signals acting as it that
/// no holder holds, and only those, and tells the holders; made again, it
/// also brings back a held one stopped over it; a rename or a
/// token refresh marks and tells nothing. A registration again keeps the mark
/// until the listener brought it up, and one by anyone else since makes the
/// row no longer the caller's.
#[weft_db_macros::test]
async fn a_changed_connection_marks_its_signals_for_registering_again(pool: Pool) {
    use weft_dispatcher::store::postgres::{access_taken_afresh, reregister_for_access, signals_with_stale_access};
    setup(&pool).await;
    let (project, grant) = (Uuid::new_v4(), Uuid::new_v4());
    pool.execute(
        "INSERT INTO access_grant (id, tenant_id, service, spec, values_sealed, spec_version, exclusive) VALUES ($1, $2, 'svc', '{}', 'sealed', $3, FALSE)",
        &[&grant, &TENANT, &weft_core::AccessSpec::STORED.version()],
    )
    .await
    .expect("a connection");
    for (token, access, holds) in [("poll", Some(grant), false), ("held", Some(grant), true), ("plain", None, false)] {
        pool.execute(
            "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, access_id, holds, \
             spec_json_version, program_json_version) \
             VALUES ($1, $2, $3, $1, FALSE, '{}', 1, $4, $5, $6, 1)",
            &[&token, &TENANT, &project, &access.map(|a| a.to_string()), &holds, &weft_core::primitive::SignalSpec::STORED.version()],
        )
        .await
        .expect("signal");
    }
    let stale_tokens = || async { signals_with_stale_access(&pool).await.unwrap().into_iter().map(|s| s.token).collect::<Vec<_>>() };
    assert!(stale_tokens().await.is_empty(), "nothing changed yet");
    let mut told = Session::connect(pool.config()).await.unwrap();
    told.listen([weft_dispatcher::holders::ACCESS_REMADE_CHANNEL]).await.unwrap();
    let mut next_told = async || {
        tokio::time::timeout(std::time::Duration::from_secs(10), told.notified())
            .await
            .expect("the holders are told")
            .unwrap()
            .expect("the session stays open")
            .payload()
            .to_string()
    };

    pool.execute(
        "UPDATE access_grant SET label = 'renamed', values_sealed = 'refreshed', expires_at = now() WHERE id = $1",
        &[&grant],
    )
    .await
    .unwrap();
    assert!(stale_tokens().await.is_empty(), "a rename or a token refresh changes nothing a registration read");
    // Notices arrive in commit order: the mark comes first only if the
    // refresh told the holders nothing.
    pool.execute("SELECT pg_notify($1, 'mark')", &[&weft_dispatcher::holders::ACCESS_REMADE_CHANNEL]).await.unwrap();
    assert_eq!(next_told().await, "mark", "a token refresh does not tell the holders");

    // A reconnect or a rotate (`weft_access_store` flows), over a signal
    // given up for its connection.
    pool.execute("UPDATE signal SET held_failed = 'refused' WHERE token IN ('poll', 'held')", &[]).await.unwrap();
    pool.execute("UPDATE access_grant SET reconnects = reconnects + 1 WHERE id = $1", &[&grant]).await.unwrap();
    assert_eq!(next_told().await, grant.to_string(), "a reconnect tells the holders");
    let held_given_up: Option<String> = pool.scalar("SELECT held_failed FROM signal WHERE token = 'held'", &[]).await.unwrap();
    assert_eq!(held_given_up, None, "a held signal stopped over its connection is held again");
    let stale = signals_with_stale_access(&pool).await.unwrap();
    assert_eq!(stale.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), ["poll"], "a holder hears it itself");
    let fresh = reregister_for_access(&pool, &stale[0]).await.unwrap().expect("still the read row");
    assert_ne!(fresh.generation, stale[0].generation, "a new registration");
    let (seq, given_up): (i64, Option<String>) = pool.one("SELECT kind_state_seq, held_failed FROM signal WHERE token = 'poll'", &[])
        .await
        .unwrap();
    assert_eq!(seq, stale[0].kind_state_seq + 1, "a write from the registration it replaced is refused");
    assert_eq!(given_up, None, "its give-up is forgotten");
    assert_eq!(stale_tokens().await, ["poll"], "marked until the listener brought it up");
    assert!(reregister_for_access(&pool, &stale[0]).await.unwrap().is_none(), "an older read is not the caller's");
    access_taken_afresh(&pool, &fresh.token, fresh.generation).await.unwrap();
    assert!(stale_tokens().await.is_empty());

    pool.execute("UPDATE signal SET held_failed = 'refused' WHERE token = 'held'", &[]).await.unwrap();
    pool.execute("DELETE FROM access_grant WHERE id = $1", &[&grant]).await.unwrap();
    assert_eq!(stale_tokens().await, ["poll"], "a connection that went marks it too");
    let held_given_up: Option<String> = pool.scalar("SELECT held_failed FROM signal WHERE token = 'held'", &[]).await.unwrap();
    assert_eq!(held_given_up.as_deref(), Some("refused"), "a connection that went brings nothing back");
    assert_eq!(next_told().await, grant.to_string(), "and tells the holders");
}

/// A person asking for a look keeps when they asked on the project (what
/// arms its given-up health rules again), starts a fresh round of the
/// supervisor's tries over it, and sends the notice that wakes the
/// supervisor for that project.
#[weft_db_macros::test]
async fn asking_for_a_look_wakes_the_supervisor_for_the_project(pool: Pool) {
    use weft_broker_client::lifecycle_command::{INFRA_COMMAND_CHANNEL, LOOK_WAKE};
    setup(&pool).await;
    let id = Uuid::new_v4();
    static CHANNELS: &[&str] = &[INFRA_COMMAND_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(pool.config(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    weft_dispatcher::infra_lifecycle_command::person_asks_for_look(&pool, id).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let look = weft_broker_client::lifecycle_command::look_payload(id);
    assert!(
        heard.woken_before(deadline, |c, p| c == LOOK_WAKE.channel && (LOOK_WAKE.concerns)(p) && p == look).await.unwrap(),
        "the look names its project"
    );
    let asked: Option<i64> = pool.scalar("SELECT asked_at_unix FROM infra_health_look WHERE project_id = $1", &[&id])
        .await
        .unwrap();
    assert!(asked.is_some(), "the person's look is kept on the project");

    // Passes over the project that failed until the supervisor stopped
    // trying: a look starts a fresh round of tries.
    pool.execute("UPDATE infra_health_look SET failures = 4, retry_at_unix = 99 WHERE project_id = $1", &[&id])
        .await
        .unwrap();
    weft_dispatcher::infra_lifecycle_command::person_asks_for_look(&pool, id).await.unwrap();
    let (failures, retry_at): (i32, Option<i64>) =
        pool.one("SELECT failures, retry_at_unix FROM infra_health_look WHERE project_id = $1", &[&id])
            .await
            .unwrap();
    assert_eq!((failures, retry_at), (0, None), "a fresh round");
}

/// A project's health verbs are claimed one at a time, in issue order: a
/// recovery is not claimed while the park before it is still running (it
/// could land last and leave the triggers down), while an upgrade of the
/// same project is neither held behind them nor holds them.
#[weft_db_macros::test]
async fn health_verbs_of_a_project_are_claimed_in_order(pool: Pool) {
    use weft_dispatcher::infra_lifecycle_command::InfraLifecycleVerb;
    use weft_dispatcher::lifecycle_claimer::claim_one;
    let (_store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let park = seed_dispatcher_command(&pool, project, "deactivate").await;
    let recover = seed_dispatcher_command(&pool, project, "reactivate").await;
    let upgrade = seed_dispatcher_command(&pool, project, "upgrade").await;

    let first = claim_one(&pool, "disp-1").await.unwrap().expect("the park");
    assert_eq!((first.id, first.verb), (park, InfraLifecycleVerb::Deactivate));
    let second = claim_one(&pool, "disp-2").await.unwrap().expect("the upgrade");
    assert_eq!(second.id, upgrade, "the recovery waits on the park; the upgrade does not");
    assert!(claim_one(&pool, "disp-2").await.unwrap().is_none(), "nothing else is claimable yet");

    complete_command(&pool, park).await;
    let third = claim_one(&pool, "disp-2").await.unwrap().expect("the recovery");
    assert_eq!((third.id, third.verb), (recover, InfraLifecycleVerb::Reactivate));
}

/// A health take-down or bring-back that ends, whether it succeeded or
/// failed, asks the supervisor for exactly one look at its project, so the
/// rules that read what it did (or count its failure) run again without
/// waiting for a unit to change. Any other command's ending asks for none.
#[weft_db_macros::test]
async fn a_health_command_that_ends_asks_for_exactly_one_look(pool: Pool) {
    use weft_broker_client::lifecycle_command::{look_payload, INFRA_COMMAND_CHANNEL};
    let (_store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let take_down = seed_dispatcher_command(&pool, project, "deactivate").await;
    let bring_back = seed_dispatcher_command(&pool, project, "reactivate").await;
    let stop = seed_dispatcher_command(&pool, project, "stop").await;
    let mut heard = Session::connect(pool.config()).await.unwrap();
    heard.listen([INFRA_COMMAND_CHANNEL]).await.unwrap();
    for (id, outcome) in [(take_down, "succeeded"), (bring_back, "failed"), (stop, "succeeded")] {
        pool.execute(
            "UPDATE infra_lifecycle_command SET completed_at_unix = $1, outcome = $2 WHERE id = $3",
            &[&weft_dispatcher::lease::now_unix(), &outcome, &id],
        )
        .await
        .unwrap();
    }
    // Notices arrive in commit order: everything the endings sent comes
    // before this mark.
    pool.execute("SELECT pg_notify($1, 'mark')", &[&INFRA_COMMAND_CHANNEL]).await.unwrap();
    let mut payloads = Vec::new();
    loop {
        let notice = tokio::time::timeout(std::time::Duration::from_secs(10), heard.notified())
            .await
            .expect("the mark arrives")
            .unwrap()
            .expect("the session stays open");
        if notice.payload() == "mark" {
            break;
        }
        payloads.push(notice.payload().to_string());
    }
    let look = look_payload(project);
    assert_eq!(
        payloads,
        vec![format!("done:{take_down}"), look.clone(), format!("done:{bring_back}"), look.clone(), format!("done:{stop}")],
        "one look per health command, whatever its outcome; none for a stop"
    );
}

/// One upgrade of the same copies at a time: a second is refused naming the
/// one in flight, another owner's goes ahead, and once the first ends a new
/// one is issued.
#[weft_db_macros::test]
async fn a_second_upgrade_of_the_same_copies_is_refused(pool: Pool) {
    use weft_dispatcher::infra_lifecycle_command::{Issued, TakeDownCommands, TakeDownWork, UpgradeWork};
    let (_store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let work = UpgradeWork {
        nodes: Vec::new(),
        running: Vec::new(),
        take_down: weft_core::TakeDown::DEFAULT,
        binary_hash: None,
        definition_hash: None,
        infra_hash: None,
        waits_on_tokens: Vec::new(),
        stopped: false,
    };
    let ada = InstanceId::new("ada").unwrap();
    let issue = |instance: Option<&InstanceId>| {
        let (pool, work, instance) = (pool.clone(), work.clone(), instance.cloned());
        async move {
            let commands = TakeDownCommands {
                tenant_id: TENANT.into(),
                issued_by_replica: "disp-1".into(),
                work: TakeDownWork::Upgrade { instance, work },
            };
            commands.issue(&pool, project).await.unwrap().expect("the project is there")
        }
    };

    let Issued::Commands(first) = issue(None).await else { panic!("the first upgrade is issued") };
    let first = first[0];
    assert_eq!(issue(None).await, Issued::UpgradeInFlight(first));
    assert!(matches!(issue(Some(&ada)).await, Issued::Commands(_)), "an instance's copies are other copies");

    complete_command(&pool, first).await;
    assert!(matches!(issue(None).await, Issued::Commands(ids) if ids != [first]));
}

// ----- held signals: the holders' claims and their count --------------------

/// A held entry signal written the way a registration writes it.
fn held_signal(token: &str, project_id: Uuid, seq: i64) -> SignalRegistration {
    SignalRegistration { holds: true, kind_state_seq: seq, ..entry_at(token, project_id) }
}

/// An entry signal at a place of its own, so several sit in one project.
fn entry_at(token: &str, project_id: Uuid) -> SignalRegistration {
    SignalRegistration { node_id: token.to_string(), ..entry_signal(token, project_id) }
}

/// A registration that rewrites a held row takes its holder's claim away,
/// so the holder running the old row stops it and the new one comes up as
/// the row now reads.
#[weft_db_macros::test]
async fn a_rewritten_held_row_lets_its_holder_go(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    store.signal_insert(&held_signal("sse", id, 0)).await.unwrap();
    let first = store.signal_get("sse").await.unwrap().unwrap().generation;
    pool.execute(
        "UPDATE signal SET held_by = 'h1', serving = '{\"status\":\"up\"}', held_failed = 'gave up' WHERE token = 'sse'",
        &[],
    )
    .await
    .unwrap();
    store.signal_insert(&held_signal("sse", id, 1)).await.unwrap();
    let (held_by, serving, held_failed, holds): (Option<String>, Option<String>, Option<String>, bool) =
        pool.one("SELECT held_by, serving, held_failed, holds FROM signal WHERE token = 'sse'", &[]).await.unwrap();
    assert_eq!((held_by, serving, held_failed, holds), (None, None, None, true), "a rewrite is let go of and tried again");
    let second = store.signal_get("sse").await.unwrap().unwrap().generation;
    assert!(second > first, "a rewrite is a new registration: {first} then {second}");
}

/// A write names the registration it made, a wake's claim keeps it, and
/// the undo of a registration removes the row only while it is still that
/// registration: a later arm's row is never removed under it.
#[weft_db_macros::test]
async fn a_registration_is_removed_only_while_the_row_is_still_it(pool: Pool) {
    use weft_dispatcher::store::SignalWrite;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let SignalWrite::Written { generation: first } = store.signal_insert(&held_signal("sse", id, 0)).await.unwrap() else {
        panic!("a new row is written")
    };
    assert_eq!(store.signal_get("sse").await.unwrap().unwrap().generation, first, "the write names the registration it made");
    // A wake's claim, as the broker writes it: the version moves, the
    // registration does not.
    pool.execute("UPDATE signal SET kind_state = '{\"next_at\": 6}', kind_state_seq = kind_state_seq + 1 WHERE token = 'sse'", &[])
        .await
        .unwrap();
    // A later arm registers the row again.
    let SignalWrite::Written { generation: later } = store.signal_insert(&held_signal("sse", id, 2)).await.unwrap() else {
        panic!("the later arm lands")
    };
    assert_ne!(later, first);
    assert!(store.signal_remove_registration("sse", first).await.unwrap().is_none(), "the first arm's undo leaves the later row");
    assert_eq!(store.signal_get("sse").await.unwrap().unwrap().generation, later);
    let removed = store.signal_remove_registration("sse", later).await.unwrap().expect("the current registration is removed");
    assert_eq!(removed.token, "sse");
    assert!(store.signal_get("sse").await.unwrap().is_none());
}

/// The holders are counted from the held rows a listener holds: a parked
/// activation's and a signal that holds nothing do not count. Read off
/// the sizing at one signal to a holder, on a pool never set.
#[weft_db_macros::test]
async fn the_holders_count_the_held_rows_of_live_activations(pool: Pool) {
    async fn held_count(pool: &Pool) -> anyhow::Result<u32> {
        pool.execute("DELETE FROM holder_pool", &[]).await?;
        let holders = weft_platform_traits::FakeHolderPool::default();
        weft_dispatcher::holders::size(pool, &holders, &weft_platform_traits::FakeHolderNews::default(), &per_copy(1), &std::sync::atomic::AtomicBool::new(false))
            .await?;
        let sizes = holders.sizes.lock();
        Ok(sizes[0])
    }
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    assert_eq!(held_count(&pool).await.unwrap(), 0);
    store.signal_insert(&held_signal("one", id, 0)).await.unwrap();
    store.signal_insert(&held_signal("two", id, 0)).await.unwrap();
    store.signal_insert(&entry_at("form", id)).await.unwrap();
    assert_eq!(held_count(&pool).await.unwrap(), 2);
    pool.execute(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at, \
         activation_program_version) VALUES ($1, 'wiped', 'inactive', FALSE, FALSE, 0, 1)",
        &[&id],
    )
    .await
    .unwrap();
    pool.execute("UPDATE signal SET activation_trigger = 'wiped' WHERE token = 'two'", &[]).await.unwrap();
    assert_eq!(held_count(&pool).await.unwrap(), 1, "a wiped activation's row holds nothing");
    pool.execute(
        "UPDATE trigger_activation SET accepting_fires = TRUE, fires_visible_to_consumers = TRUE WHERE trigger = 'wiped'",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(held_count(&pool).await.unwrap(), 2, "a parked one's goes on holding: what it hears waits");
    pool.execute("UPDATE signal SET held_failed = 'misconfigured' WHERE token = 'one'", &[]).await.unwrap();
    assert_eq!(held_count(&pool).await.unwrap(), 1, "a row its holder gave up on needs no holder");
}

/// The holders run as many copies as the held signals need, none for none,
/// and look again only when the count is announced to have changed. A
/// process that has set the pool asks it to change only when the count it
/// needs differs from the one last set, by whichever replica set it; one
/// that has not set it yet (a replica just started) sets it whatever was
/// written, since the pool may have changed behind weft's back. The
/// holders are told of every change of the held set all the same.
#[weft_db_macros::test]
async fn the_holders_are_sized_to_the_held_signals(pool: Pool) {
    use std::sync::atomic::AtomicBool;
    use weft_dispatcher::holders::size;
    use weft_task_store::drain::DrainStep;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (holders, here) = (weft_platform_traits::FakeHolderPool::default(), AtomicBool::new(false));
    let news = weft_platform_traits::FakeHolderNews::default();
    assert_eq!(size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap(), DrainStep::Done);
    assert_eq!(*holders.sizes.lock(), vec![0], "never set: the pool is set even to none");
    size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![0], "set to none already: not asked again");
    for t in ["a", "b", "c"] {
        store.signal_insert(&held_signal(t, id, 0)).await.unwrap();
    }
    assert_eq!(size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap(), DrainStep::Done);
    store.signal_insert(&held_signal("d", id, 0)).await.unwrap();
    let (sibling, there) = (weft_platform_traits::FakeHolderPool::default(), AtomicBool::new(false));
    size(&pool, &sibling, &news, &per_copy(2), &there).await.unwrap();
    assert_eq!(*sibling.sizes.lock(), vec![2], "a replica just started sets the pool to the count written, as it may have drifted");
    size(&pool, &sibling, &news, &per_copy(2), &there).await.unwrap();
    assert_eq!(*sibling.sizes.lock(), vec![2], "and then trusts it");
    size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![0, 2], "four signals still need the two holders the other replica set");
    store.signal_remove_many(&["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]).await.unwrap();
    size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![0, 2, 0]);
    assert_eq!(
        *news.told.lock(),
        vec![weft_platform_traits::HolderNote::HeldSetChanged; 7],
        "each sizing tells the holders, after the pool is sized for it"
    );
}

/// The holder settings with `signals_per_copy` to a holder, the rest as
/// they default (a target of 50%).
fn per_copy(signals_per_copy: u32) -> weft_platform_traits::config::HolderSettings {
    weft_platform_traits::config::HolderSettings { signals_per_copy, ..Default::default() }
}

/// Claim `held` signals of project `id`, each `(token, holder)`.
async fn claimed(store: &PostgresStore, pool: &Pool, id: Uuid, held: &[(&str, &str)]) {
    for (t, holder) in held {
        store.signal_insert(&held_signal(t, id, 0)).await.unwrap();
        pool.execute("UPDATE signal SET held_by = $2 WHERE token = $1", &[t, holder]).await.unwrap();
    }
}

/// The holders run as many copies as the load they said, summed, needs at
/// the target each, when that is more than the held signals need; the
/// floor wins when it is not, and counts the room a holder gave up; and
/// never more holders than signals.
#[weft_db_macros::test]
async fn the_holders_are_sized_to_their_reported_load(pool: Pool) {
    use std::sync::atomic::AtomicBool;
    use weft_broker::held_signals::{report_load, HandTo};
    use weft_dispatcher::holders::size;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    claimed(&store, &pool, id, &[("a", "h1"), ("b", "h2"), ("c", "h3")]).await;
    let (holders, here) = (weft_platform_traits::FakeHolderPool::default(), AtomicBool::new(false));
    let news = weft_platform_traits::FakeHolderNews::default();
    size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2], "three signals, two to a holder");

    for (h, cpu) in [("h1", 60), ("h2", 55), ("h3", 0)] {
        report_load(&pool, h, cpu, 2, &[], HandTo::SpareOrAny).await.unwrap();
    }
    size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2, 3], "115% at 50% each: the load wins");

    for h in ["h1", "h2"] {
        report_load(&pool, h, 20, 2, &[], HandTo::SpareOrAny).await.unwrap();
    }
    size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2, 3, 2], "40% needs one, three signals need two: the floor wins");

    for h in ["h1", "h2"] {
        report_load(&pool, h, 20, 1, &[], HandTo::SpareOrAny).await.unwrap();
    }
    size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2, 3, 2, 3], "two holders keeping one each leave room for one more: the room given up raises the floor");
    report_load(&pool, "h3", 400, 2, &[], HandTo::SpareOrAny).await.unwrap();
    size(&pool, &holders, &news, &per_copy(2), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2, 3, 2, 3], "three signals run three holders at most");
}

/// Only a holder that claims signals counts: a load said by one that
/// claims nothing (its claims freed, or never taken) holds no work. A
/// copy that dies keeps its claims until its check frees them, which
/// forgets what it said; until then its load counts, as its signals still
/// need a holder. And no copy goes while what it held moves: its signals
/// free and nobody took them yet, or taken by a holder that has not said
/// its load since.
#[weft_db_macros::test]
async fn a_load_said_by_a_holder_that_claims_nothing_is_left_out(pool: Pool) {
    use std::sync::atomic::AtomicBool;
    use weft_broker::held_signals::{hold, release, report_load, HandTo};
    use weft_dispatcher::holders::size;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    claimed(&store, &pool, id, &[("a", "h1"), ("b", "h1"), ("c", "h1"), ("d", "h2"), ("e", "h2"), ("f", "h2")]).await;
    let (holders, here) = (weft_platform_traits::FakeHolderPool::default(), AtomicBool::new(false));
    let news = weft_platform_traits::FakeHolderNews::default();
    for h in ["h1", "h2"] {
        report_load(&pool, h, 30, 10, &[], HandTo::SpareOrAny).await.unwrap();
    }
    report_load(&pool, "h3", 200, 10, &[], HandTo::SpareOrAny).await.unwrap();
    size(&pool, &holders, &news, &per_copy(10), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2], "60% from the two that claim; h3 claims nothing and is left out");

    claimed(&store, &pool, id, &[("g", "h3")]).await;
    size(&pool, &holders, &news, &per_copy(10), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2, 6], "once it claims a signal, its 200% counts");

    release(&pool, "h3").await.unwrap();
    size(&pool, &holders, &news, &per_copy(10), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2, 6], "its check went off: g is free, and nobody took it yet");
    hold(&pool, "h1", &[], Some(10), &[]).await.unwrap();
    size(&pool, &holders, &news, &per_copy(10), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2, 6], "h1 took it and has not said its load since");
    report_load(&pool, "h1", 35, 10, &[], HandTo::SpareOrAny).await.unwrap();
    size(&pool, &holders, &news, &per_copy(10), &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![2, 6, 2], "measured on what it holds now: the sum decides");
}

/// A resize leaves every load said as it is, so no holder says its load
/// again because the pool moved: each look still answers what it said.
#[weft_db_macros::test]
async fn a_resize_leaves_every_load_said(pool: Pool) {
    use std::sync::atomic::AtomicBool;
    use weft_broker::held_signals::{hold, report_load, HandTo};
    use weft_dispatcher::holders::size;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    claimed(&store, &pool, id, &[("a", "h1"), ("c", "h1"), ("b", "h2")]).await;
    for (h, cpu) in [("h1", 60), ("h2", 55)] {
        report_load(&pool, h, cpu, 10, &[], HandTo::SpareOrAny).await.unwrap();
    }
    let rows = || async {
        let rows: Vec<(String, String, i32, bool)> =
            pool.all("SELECT replica, xmin::text, cpu_percent, settled FROM holder_load ORDER BY replica", &[]).await.unwrap();
        rows
    };
    let before = rows().await;
    let holders = weft_platform_traits::FakeHolderPool::default();
    size(&pool, &holders, &weft_platform_traits::FakeHolderNews::default(), &per_copy(10), &AtomicBool::new(false)).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![3]);
    assert_eq!(rows().await, before, "not one load row written");
    let holding: Vec<_> = ["a", "c"].map(|t| weft_broker_client::protocol::HeldNow { token: t.into(), serving: None }).into();
    let look = hold(&pool, "h1", &holding, Some(10), &[]).await.unwrap();
    assert_eq!(look.load, Some(60), "the holder still has what it said, so it says nothing");
}

/// A holder's load said wakes the holder sizing, a load left behind by a
/// take does not, a load said again does, and a holder going does.
#[weft_db_macros::test]
async fn a_holders_load_said_wakes_the_holder_sizing(pool: Pool) {
    use weft_broker::held_signals::{release, report_load, HandTo};
    use weft_dispatcher::holders::HELD_SIGNALS_CHANNEL;
    setup(&pool).await;
    static CHANNELS: &[&str] = &[HELD_SIGNALS_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(pool.config(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    async fn woken(heard: &mut weft_task_store::pg_signal::Subscription) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        heard.woken_before(deadline, |c, _| c == HELD_SIGNALS_CHANNEL).await.unwrap()
    }
    report_load(&pool, "h1", 30, 2, &[], HandTo::SpareOrAny).await.unwrap();
    assert!(woken(&mut heard).await, "a first report");
    report_load(&pool, "h1", 30, 2, &[], HandTo::SpareOrAny).await.unwrap();
    assert!(!woken(&mut heard).await, "the same report again writes nothing");
    pool.execute("UPDATE holder_load SET settled = false", &[]).await.unwrap();
    assert!(!woken(&mut heard).await, "a load left behind only holds the count where it is");
    report_load(&pool, "h1", 30, 2, &[], HandTo::SpareOrAny).await.unwrap();
    assert!(woken(&mut heard).await, "said again after it was left behind");
    report_load(&pool, "h1", 45, 2, &[], HandTo::SpareOrAny).await.unwrap();
    assert!(woken(&mut heard).await, "a new load");
    release(&pool, "h1").await.unwrap();
    assert!(woken(&mut heard).await, "a holder going");
}

/// A held row coming or going announces itself at its commit, which is
/// what wakes the holder sizing, and so does one losing its holder or
/// given up on; a row that holds nothing stays silent.
#[weft_db_macros::test]
async fn a_held_row_coming_or_going_wakes_the_holder_sizing(pool: Pool) {
    use weft_dispatcher::holders::HELD_SIGNALS_CHANNEL;
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    static CHANNELS: &[&str] = &[HELD_SIGNALS_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(pool.config(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    async fn woken(heard: &mut weft_task_store::pg_signal::Subscription) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        heard.woken_before(deadline, |c, _| c == HELD_SIGNALS_CHANNEL).await.unwrap()
    }
    store.signal_insert(&entry_at("form", id)).await.unwrap();
    assert!(!woken(&mut heard).await, "a signal that holds nothing is silent");
    store.signal_insert(&held_signal("sse", id, 0)).await.unwrap();
    assert!(woken(&mut heard).await, "a held one coming");
    pool.execute("UPDATE signal SET held_by = 'h1' WHERE token = 'sse'", &[]).await.unwrap();
    assert!(!woken(&mut heard).await, "a holder taking it changes no holder's work");
    pool.execute("UPDATE signal SET held_by = NULL WHERE token = 'sse'", &[]).await.unwrap();
    assert!(woken(&mut heard).await, "its holder letting go (or its check going off) frees it for another");
    pool.execute("UPDATE signal SET held_failed = 'misconfigured' WHERE token = 'sse'", &[]).await.unwrap();
    assert!(woken(&mut heard).await, "a give-up takes it out of the held set");
    store.signal_remove_many(&["sse".to_string()]).await.unwrap();
    assert!(woken(&mut heard).await, "and going");
    pool.execute(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at, \
         activation_program_version) VALUES ($1, 'parked', 'active', TRUE, TRUE, 0, 1)",
        &[&id],
    )
    .await
    .unwrap();
    assert!(woken(&mut heard).await, "an activation coming may change which rows are held");
    pool.execute("UPDATE trigger_activation SET status = 'inactive' WHERE project_id = $1 AND trigger = 'parked'", &[&id]).await.unwrap();
    assert!(woken(&mut heard).await, "an activation parked changes which rows are held");
    pool.execute("DELETE FROM trigger_activation WHERE project_id = $1 AND trigger = 'parked'", &[&id]).await.unwrap();
    assert!(woken(&mut heard).await, "and one forgotten too");
}

/// The rows a dispatcher holds in memory (`weft_dispatcher::held`) say
/// when they change, naming whose: a tenant's public entries and the
/// activations that arm them, a project's worker levers, a project's infra
/// copies. A signal that is no public entry is silent: a run waiting on a
/// reply writes one, and a tenant's routes are not read again for it.
#[weft_db_macros::test]
async fn the_held_rows_announce_their_changes(pool: Pool) {
    use weft_dispatcher::held::{INFRA_STATUS_CHANNEL, ROUTES_CHANNEL, WORKER_SETTINGS_CHANNEL};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    static CHANNELS: &[&str] = &[ROUTES_CHANNEL, WORKER_SETTINGS_CHANNEL, INFRA_STATUS_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(pool.config(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    async fn heard_on(heard: &mut weft_task_store::pg_signal::Subscription, channel: &'static str, key: &str) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        heard.woken_before(deadline, |c, p| c == channel && p == key).await.unwrap()
    }
    let project = id.to_string();

    store.signal_insert(&entry_at("callback", id)).await.unwrap();
    assert!(!heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a signal that is no public entry is silent");
    let route = SignalRegistration {
        surface_kind: "public_entry".into(),
        mount_path: Some(format!("/{TENANT}/{id}/notes")),
        ..entry_at("route", id)
    };
    store.signal_insert(&route).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a public entry coming names its tenant");
    pool.execute(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at, \
         activation_program_version) VALUES ($1, 'route', 'active', TRUE, TRUE, 0, 1)",
        &[&id],
    )
    .await
    .unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "an activation coming");
    pool.execute("UPDATE trigger_activation SET status = 'inactive' WHERE project_id = $1", &[&id]).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "an activation taken down");
    store.signal_remove_many(&["route".to_string()]).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a public entry going");

    pool.execute("UPDATE project SET worker_settings_json = '{\"minInstances\": 1}' WHERE id = $1", &[&id])
        .await
        .unwrap();
    assert!(heard_on(&mut heard, WORKER_SETTINGS_CHANNEL, &project).await, "a project's worker levers changing name it");

    pool.execute(
        "INSERT INTO infra_node (project_id, node_id, status, health_rules_json_version) VALUES ($1, 'db', 'provisioning', 1)",
        &[&id],
    )
    .await
    .unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "an infra copy coming names its project");
    pool.execute("UPDATE infra_node SET status = 'running' WHERE project_id = $1", &[&id]).await.unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and its status changing");
    // A worker keeps where each piece answers, so a move is news too.
    pool.execute("UPDATE infra_node SET endpoints_json = '{\"main\": \"db:5432\"}' WHERE project_id = $1", &[&id])
        .await
        .unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and where it answers changing");
    // A program registered again may declare other infra.
    pool.execute("UPDATE project SET project_json = project_json || ' ' WHERE id = $1", &[&id]).await.unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and the infra it declares changing");
}

/// What an infra says changed is written where its node put it, with no
/// node running: a password into the connection the node published, a
/// baked output over the saved one. A push naming anything the node did
/// not hand weft, or a copy that is gone, is refused and writes nothing.
#[weft_db_macros::test]
async fn what_an_infra_says_changed_is_written_where_its_node_put_it(pool: Pool) {
    let (_store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    pool.execute(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, baked_json, health_rules_json_version) VALUES ($1, 'db', 'c1', 'running', $2, 1)",
        &[&project, &json!({ "address": "db:5432" }).to_string()],
    )
    .await
    .unwrap();
    let spec: weft_core::AccessSpec = serde_json::from_value(json!({
        "service": "selfrun",
        "acquisition": { "kind": "static", "fields": [{ "name": "host", "secret": false }, { "name": "password" }] },
    }))
    .unwrap();
    weft_access_store::publish_grant(
        &pool,
        TENANT,
        weft_access_store::PublishAccess {
            spec,
            project_id: project,
            instance: None,
            node_id: "db".into(),
            values: [("host".to_string(), "db".to_string()), ("password".to_string(), "p1".to_string())].into(),
            label: None,
        },
    )
    .await
    .unwrap();
    let password = || async {
        let sealed: String = pool.scalar(
            "SELECT values_sealed FROM access_grant WHERE project_id = $1 AND published_by_node = 'db'",
            &[&project],
        )
        .await
        .unwrap();
        weft_access_store::open_json(&sealed).unwrap()["password"].as_str().unwrap().to_string()
    };
    let baked = || async {
        pool.scalar::<weft_task_store::db::JsonText<serde_json::Value>>("SELECT baked_json FROM infra_node WHERE project_id = $1 AND node_id = 'db'", &[&project])
            .await
            .unwrap()
            .0
    };

    let reset = weft_core::infra::bake::PushedValues {
        connection: [("password".to_string(), "p2".to_string())].into(),
        outputs: [("address".to_string(), json!("db:6543"))].into(),
    };
    weft_access_store::write_pushed_values(&pool, project, "c1", &reset).await.unwrap();
    assert_eq!(password().await, "p2", "the connection holds the new password");
    assert_eq!(baked().await, json!({ "address": "db:6543" }));
    // A value holding NUL characters is kept exactly, the others beside it.
    let odd = weft_core::infra::bake::PushedValues { outputs: [("address".to_string(), json!("db:\u{0}1"))].into(), ..Default::default() };
    weft_access_store::write_pushed_values(&pool, project, "c1", &odd).await.unwrap();
    assert_eq!(baked().await, json!({ "address": "db:\u{0}1" }));
    weft_access_store::write_pushed_values(&pool, project, "c1", &reset).await.unwrap();

    // Only what the node handed weft can change, and nothing is written
    // when any of it is refused.
    let refused = |e: anyhow::Error| match e.downcast::<weft_access_store::AccessError>() {
        Ok(weft_access_store::AccessError::Invalid(why)) => why,
        other => panic!("not a refusal: {other:?}"),
    };
    let stray = weft_core::infra::bake::PushedValues {
        connection: [("password".to_string(), "p3".to_string())].into(),
        outputs: [("status".to_string(), json!("up"))].into(),
    };
    let why = refused(weft_access_store::write_pushed_values(&pool, project, "c1", &stray).await.unwrap_err());
    assert!(why.contains("no baked output 'status'"), "{why}");
    assert_eq!(password().await, "p2", "a refused push changes nothing");
    let unknown = weft_core::infra::bake::PushedValues { connection: [("user".to_string(), "root".to_string())].into(), ..Default::default() };
    let why = refused(weft_access_store::write_pushed_values(&pool, project, "c1", &unknown).await.unwrap_err());
    assert!(why.contains("stores no 'user'"), "{why}");
    let why = refused(weft_access_store::write_pushed_values(&pool, project, "gone", &reset).await.unwrap_err());
    assert!(why.contains("no longer running"), "{why}");
}

/// An activation is orphaned only once the task driving it is over: while
/// that task waits to be claimed, is claimed, or rests on its setup run
/// (parked on a person), the activation is alive whatever became of the
/// process that claimed it, since a claim its claimant left is rescued and
/// the task carries the activation on. While its task's claim is renewed,
/// the `lapses` loop looks when that claim would lapse; resting, never.
/// One claimed under a task that is gone is orphaned at once.
#[weft_db_macros::test]
async fn an_activation_is_orphaned_only_once_its_task_is_over(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let task = weft_task_store::tasks::enqueue(
        &pool,
        weft_task_store::tasks::NewTask {
            kind: weft_task_store::TaskKind::Activate,
            project_id: Some(id),
            dedup_key: None,
            run_id: None,
            tenant_id: TENANT.into(),
            payload: json!({}),
        },
    )
    .await
    .unwrap();
    let setup_run = weft_core::new_run_id();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run, None, Claimant::Anyone, task).await.unwrap().is_ok());
    let row = activations.list(id).await.unwrap().into_iter().find(|a| a.key == feed_key()).unwrap();
    assert_eq!(row.driver_task, Some(task), "the claim names the task driving it");
    let activating = row.activating.expect("where it stands");
    assert_eq!(activating.step, weft_dispatcher::activation_store::ActivationStep::Preparing);
    assert!(!activating.listened, "never activated: it listened to nothing");
    assert!(activations.list_orphaned().await.unwrap().is_empty(), "its task waits to be claimed");
    let now = weft_dispatcher::lease::now_unix();
    assert_eq!(weft_dispatcher::lapses::earliest_lapse(&pool, now).await.unwrap(), None, "nothing is looked at for a task not claimed");

    let weft_task_store::tasks::Claim::Task(_) = weft_task_store::tasks::claim_next(&pool, "disp-1").await.unwrap() else { panic!("claimed") };
    assert!(activations.list_orphaned().await.unwrap().is_empty(), "its task is claimed");
    let claimed_until: i64 = pool.scalar("SELECT claimed_until_unix FROM task WHERE id = $1", &[&task]).await.unwrap();
    assert_eq!(weft_dispatcher::lapses::earliest_lapse(&pool, now).await.unwrap(), Some(claimed_until), "looked at when its claim would lapse");

    assert!(queue(&store, &[birth(setup_run, id, weft_core::context::Phase::TriggerSetup, "bin-A")], true).await.unwrap());
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&setup_run]).await.unwrap();
    assert!(weft_task_store::tasks::rest_claim(&pool, task, "disp-1", setup_run).await.unwrap());
    assert!(activations.list_orphaned().await.unwrap().is_empty(), "its task rests on its setup run");
    assert_eq!(weft_dispatcher::lapses::earliest_lapse(&pool, now).await.unwrap(), None, "and nothing is looked at for it");

    assert!(activations.set_step(id, setup_run, weft_dispatcher::activation_store::ActivationStep::SettingUp).await.unwrap());
    assert!(weft_task_store::tasks::resume_claim(&pool, task, "disp-1").await.unwrap());
    weft_task_store::tasks::fail(&pool, task, "disp-1", "the rollback failed".into()).await.unwrap();
    let orphaned = activations.list_orphaned().await.unwrap();
    assert_eq!(orphaned.iter().map(|s| s.run_id).collect::<Vec<_>>(), vec![setup_run], "its task ended without landing it");

    let other = Uuid::new_v4();
    seed_project(&projects, other, "bin-A").await;
    let gone = weft_core::new_run_id();
    assert!(activations.try_begin_activating(other, &[feed_key()], gone, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().is_ok());
    assert!(activations.list_orphaned().await.unwrap().iter().any(|s| s.run_id == gone), "a task that is gone drives nothing");
}

/// A claim records, for each trigger, whether it listened the whole time
/// it was down as the claim found it; a claim that fails puts back the
/// task that turned the trigger on before, so a task trying again never
/// reads a trigger it did not turn on as its own.
#[weft_db_macros::test]
async fn a_claim_keeps_what_it_found_and_a_restore_puts_back_the_driver(pool: Pool) {
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
    let setup_run = weft_core::new_run_id();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run, None, Claimant::Anyone, first).await.unwrap().is_ok());
    activations.end_activating(id, setup_run, &weft_dispatcher::activation_store::ActivationLifecycle::active(), false, None).await.unwrap().expect("landed");
    let on = activations.list(id).await.unwrap().into_iter().find(|a| a.key == feed_key()).unwrap();
    assert_eq!((on.driver_task, on.activating), (Some(first), None), "on, turned on by the first task");

    let again = weft_core::new_run_id();
    let previous = activations
        .try_begin_activating(id, &[feed_key()], again, Some(ProjectStatus::Active), Claimant::Anyone, second)
        .await
        .unwrap()
        .expect("claimed");
    let claimed = activations.list(id).await.unwrap().into_iter().find(|a| a.key == feed_key()).unwrap();
    assert_eq!(claimed.driver_task, Some(second));
    assert!(claimed.activating.expect("activating").listened, "on when claimed: a re-arm listened the whole time");
    assert!(activations.restore(id, again, &previous).await.unwrap());
    let restored = activations.list(id).await.unwrap().into_iter().find(|a| a.key == feed_key()).unwrap();
    assert_eq!((restored.lifecycle.status, restored.driver_task, restored.activating), (ProjectStatus::Active, Some(first), None));
}

/// A claimed task lapses when its claimant stops renewing it, which the
/// picker rescues; one resting on a run has no lapse while that run goes,
/// and once the run ended a claim's length ago the `lapses` loop looks.
#[weft_db_macros::test]
async fn a_resting_task_claim_lapses_only_after_its_run_ended(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let task = weft_task_store::tasks::enqueue(
        &pool,
        weft_task_store::tasks::NewTask {
            kind: weft_task_store::TaskKind::ProgramCall,
            project_id: Some(id),
            dedup_key: None,
            run_id: None,
            tenant_id: TENANT.into(),
            payload: json!({}),
        },
    )
    .await
    .unwrap();
    let weft_task_store::tasks::Claim::Task(claimed) = weft_task_store::tasks::claim_next(&pool, "disp-1").await.unwrap() else {
        panic!("claimed");
    };
    assert_eq!(claimed.id, task);
    // What a picker run dry reads: when the earliest renewed claim lapses.
    let claim_lapse = || {
        let pool = pool.clone();
        async move {
            match weft_task_store::tasks::claim_next(&pool, "disp-2").await.unwrap() {
                weft_task_store::tasks::Claim::Dry { next_lapse } => next_lapse,
                weft_task_store::tasks::Claim::Task(task) => panic!("nothing is claimable, yet {} was claimed", task.id),
            }
        }
    };
    let lapse = claim_lapse().await.expect("a renewed claim lapses");
    assert!(lapse > weft_dispatcher::lease::now_unix());

    let run = queued_run(&store, id).await;
    assert!(weft_task_store::tasks::rest_claim(&pool, task, "disp-1", run).await.unwrap());
    assert_eq!(claim_lapse().await, None, "a resting claim has no lapse");
    assert!(weft_task_store::tasks::heartbeat(&pool, task, "disp-1").await.unwrap(), "still ours");
    assert_eq!(claim_lapse().await, None, "and a beat leaves it resting");
    let now = weft_dispatcher::lease::now_unix();
    assert_eq!(weft_dispatcher::lapses::earliest_lapse(&pool, now).await.unwrap(), None, "its run goes");

    complete(&store, run).await;
    let ended = now;
    pool.execute("UPDATE run SET ended_at = $2 WHERE run_id = $1", &[&run, &ended]).await.unwrap();
    let at = weft_dispatcher::lapses::earliest_lapse(&pool, now).await.unwrap().expect("its run ended");
    assert_eq!(at, ended + weft_task_store::tasks::claim_duration_secs());
    assert!(weft_task_store::tasks::resume_claim(&pool, task, "disp-1").await.unwrap(), "its claimant takes it back");
    assert!(claim_lapse().await.is_some());
}

/// A run its workers refuse waits a little longer each time, and the third
/// refusal is the one that ends it, through the one cancel, with why; an
/// ended run is refused no more. A worker still starting gives the run a
/// longer wait and counts no refusal.
#[weft_db_macros::test]
async fn a_run_refused_three_times_ends_with_why(pool: Pool) {
    use weft_dispatcher::delivery::{refused, refused_cause, starting_wait_secs, wait_for_worker, RefusedBy, REFUSALS_TO_END, RETRY_WAITS};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&store, id).await;
    let now = weft_dispatcher::lease::now_unix();

    wait_for_worker(&pool, run).await.unwrap();
    let until: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
    assert!(until >= now + starting_wait_secs(), "joins the same bring-up later");
    assert_eq!(run_column::<i16>(&pool, "refusals", run).await, Some(0));

    for (n, wait) in RETRY_WAITS.iter().enumerate() {
        assert_eq!(refused(&pool, run).await.unwrap(), Some(n as i16 + 1));
        let until: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
        assert!(until >= now + wait && until <= now + wait + 2, "refusal {} waits {wait}s: {until}", n + 1);
    }
    assert_eq!(refused(&pool, run).await.unwrap(), Some(REFUSALS_TO_END), "the refusal that ends it");
    // What the delivery then does, through `cancel_run_id`.
    store.cancel_run(run, None, &refused_cause(RefusedBy::Worker, "the worker answered 500")).await.unwrap();
    assert_eq!(run_column::<String>(&pool, "state", run).await.as_deref(), Some("ended"));
    let cause: String = run_column(&pool, "cancel_cause", run).await.unwrap();
    assert!(cause.to_string().contains("its worker refused it 3 times: the worker answered 500"), "{cause}");
    assert_eq!(refused(&pool, run).await.unwrap(), None, "an ended run is not refused again");
}

/// Changing a project's worker levers is what a person does about workers
/// that refused its runs or could not be reached: its queued runs start
/// their tries again, a waiting one takeable at once, while one out with a
/// delivery right now stays with it; another project's are left as they
/// are.
#[weft_db_macros::test]
async fn new_worker_levers_hand_the_refused_runs_out_again(pool: Pool) {
    use weft_dispatcher::delivery::{refused, unreached};
    let (store, projects) = setup(&pool).await;
    let (id, other) = (Uuid::new_v4(), Uuid::new_v4());
    seed_project(&projects, id, "bin-A").await;
    seed_project(&projects, other, "bin-A").await;
    let run = queued_run(&store, id).await;
    let handed = queued_run(&store, id).await;
    let elsewhere = queued_run(&store, other).await;
    for refused_run in [run, run, elsewhere] {
        refused(&pool, refused_run).await.unwrap();
    }
    unreached(&pool, handed).await.unwrap();
    // Taken again: out with a delivery until its lease runs out.
    let lease = weft_dispatcher::lease::now_unix() + 30;
    pool.execute("UPDATE run SET delivered_until = $2, waits = FALSE WHERE run_id = $1", &[&handed, &lease])
        .await
        .unwrap();
    projects.set_worker_overrides(id, &weft_platform_traits::WorkerOverrides::default()).await.unwrap();
    assert_eq!(run_column::<i16>(&pool, "refusals", run).await, Some(0));
    assert_eq!(run_column::<Option<i64>>(&pool, "delivered_until", run).await, Some(None), "takeable at once");
    assert_eq!(run_column::<i16>(&pool, "unreached", handed).await, Some(0), "its tries start again");
    assert_eq!(run_column::<Option<i64>>(&pool, "delivered_until", handed).await, Some(Some(lease)), "its hand-off is left to it");
    assert_eq!(run_column::<i16>(&pool, "refusals", elsewhere).await, Some(1), "another project's run is left alone");
}

/// A run whose workers cannot be reached waits a little longer each time,
/// and the third such hand-off stops its tries without ending it: nothing
/// books it, until a worker of its project comes alive after that (which
/// wakes the delivery, as no other project's worker does), or a build of
/// the project registers, either of which starts its tries again. A run
/// its worker claimed before the call broke counts nothing.
#[weft_db_macros::test]
async fn a_run_whose_workers_cannot_be_reached_waits_for_what_can_fix_it(pool: Pool) {
    use weft_dispatcher::delivery::{next_hand_out, take, unreached, RETRY_WAITS, UNREACHED_TO_STOP};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&store, id).await;
    let now = weft_dispatcher::lease::now_unix();
    for (n, wait) in RETRY_WAITS.iter().enumerate() {
        assert_eq!(unreached(&pool, run).await.unwrap(), Some(n as i16 + 1));
        let until: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
        assert!(until >= now + wait && until <= now + wait + 2, "try {} waits {wait}s: {until}", n + 1);
    }
    assert_eq!(unreached(&pool, run).await.unwrap(), Some(UNREACHED_TO_STOP), "the try that stops them");
    assert_eq!(run_column::<String>(&pool, "state", run).await.as_deref(), Some("queued"), "the run goes on waiting");
    let stopped_at: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
    assert_eq!(next_hand_out(&pool).await.unwrap(), None, "nothing books it");

    // A worker's lease coming alive wakes the delivery only for a project
    // whose runs' tries stopped.
    let mut back = Session::connect(pool.config()).await.unwrap();
    back.listen([weft_dispatcher::delivery::WORKERS_BACK_CHANNEL]).await.unwrap();
    let mut heard = async || {
        tokio::time::timeout(std::time::Duration::from_millis(500), back.notified())
            .await
            .ok()
            .map(|n| n.unwrap().expect("the session stays open").payload().to_string())
    };
    let other = Uuid::new_v4();
    pool.execute(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix) VALUES ('w-other', $1, $2, 'bin-A', $3)",
        &[&other, &TENANT, &(now + 15)],
    )
    .await
    .unwrap();
    assert_eq!(heard().await, None, "a project with nothing stopped says nothing");

    // A worker of the project that came alive before the tries stopped
    // changes nothing; one that came alive in the second they stopped (it
    // may have come up after the stop) is what the delivery takes it again
    // for.
    pool.execute(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, alive_since) \
         VALUES ('w-1', $1, $2, 'bin-A', $3, $4)",
        &[&id, &TENANT, &(now + 15), &(stopped_at - 1)],
    )
    .await
    .unwrap();
    assert_eq!(heard().await, Some(id.to_string()), "its worker coming alive wakes the delivery");
    assert!(take(&pool, now + 3600, 32).await.unwrap().is_empty(), "a worker alive since before");
    pool.execute("UPDATE worker_lease SET alive_since = $1", &[&stopped_at]).await.unwrap();
    let taken: Vec<_> = take(&pool, now, 32).await.unwrap().into_iter().map(|d| d.run_id).collect();
    assert_eq!(taken, [run], "a worker alive since the second they stopped");
    assert_eq!(run_column::<i16>(&pool, "unreached", run).await, Some(0), "its tries start again");

    // Its tries stopped again, a build of the project registering starts
    // them again.
    for _ in 0..UNREACHED_TO_STOP {
        unreached(&pool, run).await.unwrap();
    }
    seed_project(&projects, id, "bin-B").await;
    assert_eq!(run_column::<i16>(&pool, "unreached", run).await, Some(0));
    assert_eq!(run_column::<Option<i64>>(&pool, "delivered_until", run).await, Some(None), "takeable at once");

    // Claimed by its worker before the call broke: nothing is counted.
    assert!(weft_journal::record::claim(&pool.get().await.unwrap(), run, id, "w-1").await.unwrap().is_some());
    assert_eq!(unreached(&pool, run).await.unwrap(), None, "a claimed run is its worker's");
}

/// A run its workers answer busy is tried again for as long as they do,
/// counting no try: after a pause as long as its busy stretch so far, at
/// least the first wait and at most the cap, so the pause grows and stays
/// booked. Its stretch is kept across answers, and a claim clears it. The
/// claim that leaves no run of the project answered busy also ends what
/// `weft status` shows of its workers answering busy, and so does a change
/// of its worker levers; a claim of one never answered busy, or while
/// another still waits, leaves that alone. A run not queued is not
/// counted. A run they refused or could not be reached with waits on them
/// too, so a busy run going leaves what is written while one of those is
/// queued. A run they failed whose tries started again (a worker of the
/// project came alive) still clears it on reaching a worker.
#[weft_db_macros::test]
async fn a_run_its_workers_keep_answering_busy_is_tried_again_with_a_growing_pause(pool: Pool) {
    use weft_dispatcher::delivery::{busy, busy_wait_cap_secs, busy_wait_secs, next_hand_out};
    use weft_task_store::unanswered::{failed, Callee};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&store, id).await;
    let calm = queued_run(&store, id).await;
    let now = weft_dispatcher::lease::now_unix();
    let paused = |pool: Pool, by: i64| async move {
        let until: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
        let at = weft_dispatcher::lease::now_unix();
        assert!(until >= at - 2 + by && until <= at + by, "paused until {until}, expected {by}s from {at}");
    };

    let lasted = busy(&pool, run).await.unwrap().expect("queued");
    assert!((0..=2).contains(&lasted), "its stretch starts: {lasted}");
    paused(pool.clone(), busy_wait_secs()).await;
    let since: i64 = run_column(&pool, "busy_since", run).await.unwrap();
    assert!(since >= now && since <= now + 2, "{since}");

    // A stretch going is kept, and the pause grows with it.
    let grown = busy_wait_secs() * 4;
    pool.execute("UPDATE run SET busy_since = busy_since - $2 WHERE run_id = $1", &[&run, &grown]).await.unwrap();
    let lasted = busy(&pool, run).await.unwrap().expect("queued");
    assert!(lasted >= grown && lasted <= grown + 2, "{lasted}");
    assert_eq!(run_column::<i64>(&pool, "busy_since", run).await, Some(since - grown), "a stretch going is kept");
    paused(pool.clone(), lasted).await;

    // However long it lasts, the pause stops at the cap, the run stays
    // queued counting no try, and its next try is booked.
    pool.execute("UPDATE run SET busy_since = busy_since - 3600 WHERE run_id = $1", &[&run]).await.unwrap();
    assert!(busy(&pool, run).await.unwrap().expect("queued") >= 3600);
    paused(pool.clone(), busy_wait_cap_secs()).await;
    assert_eq!(run_column::<i16>(&pool, "unreached", run).await, Some(0), "counting no try");
    assert_eq!(run_column::<String>(&pool, "state", run).await.as_deref(), Some("queued"));
    assert!(next_hand_out(&pool).await.unwrap().is_some(), "its next try is booked");

    // Written down for `weft status`: a claim of a run never answered
    // busy leaves it, so does the claim of a busy run while another still
    // waits on them answering busy, and the claim of the last clears it
    // and its stretch.
    let also = queued_run(&store, id).await;
    busy(&pool, also).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    let written = || {
        let pool = pool.clone();
        async move {
            pool.scalar::<i64>("SELECT count(*) FROM unanswered_call WHERE callee = $1", &[&format!("workers:{id}")])
                .await
                .unwrap()
        }
    };
    claim(&pool, calm, id, "worker-a").await;
    assert_eq!(written().await, 1, "a run never answered busy says nothing about it");
    claim(&pool, run, id, "worker-a").await;
    assert_eq!(written().await, 1, "another run still waits on them answering busy");
    assert_eq!(run_column::<Option<i64>>(&pool, "busy_since", run).await, Some(None), "a claim clears the stretch");
    assert_eq!(busy(&pool, run).await.unwrap(), None, "a run not queued is not counted");
    claim(&pool, also, id, "worker-a").await;
    assert_eq!(written().await, 0, "the last busy run reaching a worker clears it");

    // Changing the project's worker levers tries its queued runs afresh,
    // and what was written of its workers goes with those tries.
    let waiting = queued_run(&store, id).await;
    busy(&pool, waiting).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    projects.set_worker_overrides(id, &weft_platform_traits::WorkerOverrides::default()).await.unwrap();
    assert_eq!(written().await, 0, "the levers changing clears it");
    assert_eq!(run_column::<Option<i64>>(&pool, "busy_since", waiting).await, Some(None), "its stretch starts afresh");
    assert_eq!(run_column::<bool>(&pool, "waited_on_workers", waiting).await, Some(false), "and so does its mark");

    // A busy run that goes without reaching a worker, cancelled or
    // erased, takes what was written of its workers with it, as a claim's
    // does.
    let cancelled = queued_run(&store, id).await;
    busy(&pool, cancelled).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    store.cancel_run(cancelled, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!(run_column::<Option<i64>>(&pool, "busy_since", cancelled).await, Some(None), "an ended run has no stretch");
    assert_eq!(written().await, 0, "a cancelled busy run clears it");
    let erased = queued_run(&store, id).await;
    busy(&pool, erased).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    store.delete_run(erased).await.unwrap();
    assert_eq!(written().await, 0, "an erased busy run clears it");

    // One record holds every failure of the project's workers: a busy run
    // going leaves it while a run they could not be reached with, or one
    // they refused, still waits, and the last of those reaching a worker
    // clears it.
    let gone = queued_run(&store, id).await;
    let unreached_run = queued_run(&store, id).await;
    busy(&pool, gone).await.unwrap().expect("queued");
    weft_dispatcher::delivery::unreached(&pool, unreached_run).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "connection refused").await.unwrap().log();
    store.cancel_run(gone, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!(written().await, 1, "the run they could not be reached with still waits: its failure stays");
    let refused_run = queued_run(&store, id).await;
    weft_dispatcher::delivery::refused(&pool, refused_run).await.unwrap().expect("queued");
    claim(&pool, unreached_run, id, "worker-a").await;
    assert_eq!(written().await, 1, "the run they refused still waits");
    claim(&pool, refused_run, id, "worker-a").await;
    assert_eq!(written().await, 0, "the last run they failed reaching a worker clears it");

    // A run whose tries stopped and started again (a worker of the
    // project came alive) no longer counts its failures, yet it reaching a
    // worker still clears what was written of them.
    use weft_dispatcher::delivery::{take, UNREACHED_TO_STOP};
    let back = queued_run(&store, id).await;
    for _ in 0..UNREACHED_TO_STOP {
        weft_dispatcher::delivery::unreached(&pool, back).await.unwrap().expect("queued");
    }
    failed(&pool, Callee::Workers(id), None, "connection refused").await.unwrap().log();
    let stopped_at: i64 = run_column(&pool, "delivered_until", back).await.unwrap();
    pool.execute(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, alive_since) \
         VALUES ('w-back', $1, $2, 'bin-A', $3, $4)",
        &[&id, &TENANT, &(stopped_at + 15), &stopped_at],
    )
    .await
    .unwrap();
    let taken: Vec<_> = take(&pool, stopped_at, 32).await.unwrap().into_iter().map(|d| d.run_id).collect();
    assert!(taken.contains(&back), "its worker coming alive starts its tries again: {taken:?}");
    assert_eq!(run_column::<i16>(&pool, "unreached", back).await, Some(0), "its tries start again");
    assert_eq!(run_column::<bool>(&pool, "waited_on_workers", back).await, Some(true), "the failure it carried is kept");
    assert_eq!(written().await, 1, "nothing reached a worker yet");
    claim(&pool, back, id, "worker-a").await;
    assert_eq!(written().await, 0, "a run they failed reaching a worker clears it, its tries started again or not");
    assert_eq!(run_column::<bool>(&pool, "waited_on_workers", back).await, Some(false), "a claim clears the mark");
}

/// A drain marks the runs it waits on watched, and the ones waiting to
/// resume, so their ending announces itself; it looks again when the
/// grace of a run just handed to a worker ends, and, once that worker's
/// lease outlives the grace, when the lease could lapse, which for a
/// worker whose runs only wait is its vouch's end.
#[weft_db_macros::test]
async fn a_drain_hears_its_runs_end_and_looks_at_a_vouch_end(pool: Pool) {
    use weft_dispatcher::drain::{mark_watched, workers_look};
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let queued = queued_run(&store, id).await;
    let parked = queued_run(&store, id).await;
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&parked]).await.unwrap();
    let driven = queued_run(&store, id).await;
    claim(&pool, driven, id, "worker-a").await;

    let runs = [queued, parked, driven];
    mark_watched(&pool, id, &runs).await.unwrap();
    for run in [queued, parked, driven] {
        assert_eq!(run_column::<bool>(&pool, "watch_end", run).await, Some(true));
    }
    let grace: i64 = run_column(&pool, "delivered_until", driven).await.unwrap();
    assert_eq!(workers_look(&pool, id, &weft_dispatcher::drain::AtTheDoor::Every, &runs).await.unwrap(), Some(grace), "a run just handed is looked at when its grace ends");
    let vouch = weft_dispatcher::lease::now_unix() + 3 * 86_400;
    pool.execute(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix, binary_hash) VALUES ('worker-a', $1, $2, $3, 'bin-A')",
        &[&id, &TENANT, &vouch],
    )
    .await
    .unwrap();
    assert_eq!(workers_look(&pool, id, &weft_dispatcher::drain::AtTheDoor::Every, &runs).await.unwrap(), Some(vouch), "then one look, at the vouch's end, the grace outlived");

    let mut flagged_conn = pool.get().await.unwrap();
    let flagged = flagged_conn.transaction().await.unwrap();
    complete(&store, queued).await;
    assert!(weft_dispatcher::run_ends::take_flagged(&flagged, queued).await.unwrap().is_some(), "its ending has somebody to tell");
}

/// Retention looks next exactly when the earliest erasable run's keep runs
/// out; a run still flagged past its keep is not erasable, so it never
/// makes retention look in a loop.
#[weft_db_macros::test]
async fn retention_looks_next_at_the_earliest_erasable_keep(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    assert_eq!(store.next_expiry().await.unwrap(), None);
    let ended = queued_run(&store, id).await;
    complete(&store, ended).await;
    pool.execute("UPDATE run SET keep_until = 120 WHERE run_id = $1", &[&ended]).await.unwrap();
    assert_eq!(store.next_expiry().await.unwrap(), Some(120));
    let flagged = queued_run(&store, id).await;
    complete(&store, flagged).await;
    pool.execute("UPDATE run SET keep_until = 0, watch_end = TRUE WHERE run_id = $1", &[&flagged]).await.unwrap();
    assert_eq!(store.next_expiry().await.unwrap(), Some(120), "the flagged run waits for its ending to be handled");
}

/// A resync holds the project's transition from before its take-down to
/// after its triggers are back: it enters with its plan written down and
/// its task enqueued, in one go, and only over the build the project
/// registered; no build, no second resync and no other activation start
/// meanwhile, while its own take-down and its own drive's turn-on go
/// through; none starts while a trigger is mid-flip. One that stopped (its
/// task failed, why written into its plan) still holds it and reads as
/// standing still; taken up again it gets a new drive, and the stopped
/// drive's task writes nothing more. Its task's end lands the transition
/// and keeps the plan as that drive's record. A person giving it up lands
/// the transition with what it left down written into the plan, kept for
/// `weft status` until the next resync begins; its drive turns nothing on
/// after that, and a later cancel finds nothing under way to give up.
#[weft_db_macros::test]
async fn a_resync_keeps_builds_and_other_activations_out_until_it_lets_go(pool: Pool) {
    use weft_dispatcher::activation_store::ClaimRefused;
    use weft_dispatcher::task_kinds::resync::{task, ResyncOwner, ResyncPlan, Stopped};
    let (_store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claim");
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    let now = || weft_dispatcher::lease::now_unix();
    let stale = weft_dispatcher::transition::heartbeat_stale_secs();
    let transition = |projects: weft_dispatcher::ProjectStore| async move { projects.transition(id).await.unwrap().unwrap().as_str() };
    let plan = ResyncPlan {
        driver: Uuid::new_v4(),
        owners: vec![ResyncOwner { owner: Owner::Shared, on: vec!["feed".into()], down: Vec::new() }],
        take_down: weft_core::TakeDown::DEFAULT,
        build: weft_core::builds::BuildHashes { binary_hash: Some("bin-A".into()), ..Default::default() },
        resynced: Vec::new(),
        urls: Vec::new(),
        held: Default::default(),
        old_builds: Vec::new(),
        old_tokens: Vec::new(),
        switching: false,
        runs: 0,
        moved_runs: 0,
        off: Vec::new(),
        stopped: None,
        given_up: None,
    };
    let claim = |claimant: Claimant| {
        let activations = activations.clone();
        async move { activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), None, claimant, Uuid::new_v4()).await.unwrap().map(|_| ()) }
    };

    let mid_flip = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], mid_flip, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("a manual activate");
    assert_eq!(projects.try_begin_resyncing(id, &plan, task(TENANT, id, plan.driver)).await.unwrap(), None, "no resync while a trigger is mid-flip");
    activations.end_activating(id, mid_flip, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    let moved = ResyncPlan { build: weft_core::builds::BuildHashes { binary_hash: Some("bin-B".into()), ..Default::default() }, ..plan.clone() };
    assert_eq!(projects.try_begin_resyncing(id, &moved, task(TENANT, id, plan.driver)).await.unwrap(), None, "none over a build that moved");

    let driver = projects.try_begin_resyncing(id, &plan, task(TENANT, id, plan.driver)).await.unwrap().expect("entered");
    assert_eq!(transition(projects.clone()).await, "resyncing");
    let kind: String = pool.scalar("SELECT kind FROM task WHERE id = $1", &[&driver]).await.unwrap();
    assert_eq!(kind, weft_task_store::TaskKind::Resync.as_str(), "its task is enqueued with it");
    let held = projects.resync(id).await.unwrap().expect("its plan is written down");
    assert_eq!((held.plan.clone(), held.driven, held.standing_still()), (plan.clone(), true, None));
    let other = ResyncPlan { driver: Uuid::new_v4(), ..plan.clone() };
    assert_eq!(projects.try_begin_resyncing(id, &other, task(TENANT, id, other.driver)).await.unwrap(), None, "one resync at a time");
    assert!(!projects.try_begin_building(id, now() - stale).await.unwrap(), "no build starts under a resync");

    let other_drive = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, Some(Uuid::new_v4()))
        .await
        .unwrap();
    assert!(matches!(other_drive, LifecycleWrite::ResyncGone), "another drive takes nothing down: {other_drive:?}");
    let down = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, Some(plan.driver))
        .await
        .unwrap();
    assert!(matches!(down, LifecycleWrite::Applied { .. }), "its own take-down goes through: {down:?}");
    assert_eq!(claim(Claimant::Anyone).await, Err(ClaimRefused::Resyncing), "no other activation under it");
    assert_eq!(claim(Claimant::Resync(Uuid::new_v4())).await, Err(ClaimRefused::ResyncGone), "nor another drive's");
    let again = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], again, None, Claimant::Resync(plan.driver), Uuid::new_v4()).await.unwrap().expect("its own turn-on goes through");
    activations.end_activating(id, again, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");

    // Its task stopped: why is written into the plan, which still holds.
    let stopped = ResyncPlan { stopped: Some(Stopped { status: 409, error: "infra down".into(), at_unix: now(), waits_for: None, build_cannot_run: false }), ..plan.clone() };
    assert!(projects.write_resync(id, &stopped).await.unwrap());
    pool.execute("UPDATE task SET status = 'failed', completed_at_unix = $2 WHERE id = $1", &[&driver, &now()]).await.unwrap();
    let held = projects.resync(id).await.unwrap().expect("still held");
    assert_eq!(held.standing_still().as_deref(), Some("infra down"), "nothing drives it, and it says why");
    assert_eq!(projects.stopped_resyncs().await.unwrap().into_iter().map(|(p, _)| p).collect::<Vec<_>>(), [id]);
    assert!(projects.settle_transition(None, now() - stale).await.unwrap().is_empty(), "its triggers wait to come back");
    assert!(!projects.try_begin_building(id, now() - stale).await.unwrap(), "no build lands between its two halves");

    // Taken up again: its stop forgotten, a new drive and a new task.
    let resumed = projects.resume_resync(id, TENANT).await.unwrap().expect("held");
    assert_ne!(resumed, driver);
    let held = projects.resync(id).await.unwrap().expect("still held");
    assert!(held.driven && held.plan.stopped.is_none());
    let drive = held.plan.driver;
    assert_ne!(drive, plan.driver, "a new drive");
    assert!(!projects.write_resync(id, &plan).await.unwrap(), "the stopped drive writes nothing more");
    assert_eq!(projects.drive(id, plan.driver).await.unwrap(), None);
    assert_eq!(claim(Claimant::Resync(plan.driver)).await, Err(ClaimRefused::ResyncGone), "and turns nothing on");
    assert_eq!(projects.resume_resync(id, TENANT).await.unwrap(), Some(resumed), "one task drives it");

    // Its task ends it: the transition lands, the plan stays its record.
    assert!(!projects.end_resync(id, plan.driver).await.unwrap(), "only its own drive ends it");
    assert!(projects.end_resync(id, drive).await.unwrap());
    assert_eq!(transition(projects.clone()).await, "none");
    assert_eq!(projects.drive(id, drive).await.unwrap().map(|(p, under_way)| (p.driver, under_way)), Some((drive, false)));
    assert_eq!(projects.resync(id).await.unwrap(), None);
    assert_eq!(projects.resume_resync(id, TENANT).await.unwrap(), None, "nothing to take up");

    // A new one, given up by a person once it took the feed down: the
    // transition lands, and what it left down is written into its plan in
    // the same transaction, for `weft status`.
    let next = ResyncPlan {
        driver: Uuid::new_v4(),
        owners: vec![ResyncOwner { owner: Owner::Shared, on: vec!["feed".into()], down: vec!["feed".into()] }],
        ..plan.clone()
    };
    projects.try_begin_resyncing(id, &next, task(TENANT, id, next.driver)).await.unwrap().expect("entered over the record");
    assert_eq!(projects.given_up_resync(id).await.unwrap(), None, "nothing given up yet");
    let taken = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, Some(next.driver))
        .await
        .unwrap();
    assert!(matches!(taken, LifecycleWrite::Applied { .. }), "{taken:?}");
    let given = projects.give_up_resync(id).await.unwrap().expect("under way");
    let left_down = vec![weft_core::activation::OwnerTriggers { owner: Owner::Shared, triggers: vec!["feed".into()] }];
    assert_eq!(given.driver, next.driver);
    assert_eq!(given.given_up.as_ref().map(|g| &g.left_down), Some(&left_down), "the feed it took down stays down");
    assert_eq!(projects.given_up_resync(id).await.unwrap(), given.given_up, "kept for `weft status`");
    assert_eq!(transition(projects.clone()).await, "none");
    assert_eq!(projects.drive(id, next.driver).await.unwrap(), None, "its drive reads nothing to go on with");
    assert!(!projects.write_resync(id, &next).await.unwrap(), "nor writes");
    assert_eq!(claim(Claimant::Resync(next.driver)).await, Err(ClaimRefused::ResyncGone), "its drive turns nothing on");
    let late = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, Some(next.driver))
        .await
        .unwrap();
    assert!(matches!(late, LifecycleWrite::ResyncGone), "and takes nothing down: {late:?}");
    assert_eq!(projects.resync(id).await.unwrap(), None, "nothing is under way");
    assert!(projects.try_begin_building(id, now() - stale).await.unwrap(), "a build starts once it let go");
    assert_eq!(projects.give_up_resync(id).await.unwrap(), None, "a later cancel finds no resync under way");
    assert_eq!(transition(projects.clone()).await, "building", "and changes nothing: the build it let through keeps the project");
    assert_eq!(projects.given_up_resync(id).await.unwrap(), given.given_up, "what it left down is still shown");

    // The next resync begins over it: what the given-up one left down is
    // no longer shown.
    assert_eq!(projects.settle_transition(Some(id), now() + stale).await.unwrap(), [id], "the build let go");

    // A person takes the feed down on purpose: the record stops naming it,
    // in the take-down's own transaction.
    let decided = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None)
        .await
        .unwrap();
    assert!(matches!(decided, LifecycleWrite::Applied { .. }), "{decided:?}");
    let given_up = projects.given_up_resync(id).await.unwrap().expect("the record stays");
    assert!(given_up.left_down.is_empty(), "nothing it left down is undecided: {given_up:?}");
    assert_eq!(given_up.at_unix, given.given_up.as_ref().unwrap().at_unix);

    let third = ResyncPlan { driver: Uuid::new_v4(), ..plan.clone() };
    projects.try_begin_resyncing(id, &third, task(TENANT, id, third.driver)).await.unwrap().expect("entered over the record");
    assert_eq!(projects.given_up_resync(id).await.unwrap(), None);
}

/// What `weft status` reads of a resync under way: where its task last
/// said it is (the runs on the old version it waits for), and nothing of a
/// task that no longer drives it. One that stopped waiting on the database
/// keeps that through the store, is among the stops weft takes up again,
/// and taken up again drives on under a new drive whose task has said
/// nothing yet.
#[weft_db_macros::test]
async fn a_resync_shows_what_it_waits_for_and_a_stop_on_the_database_is_taken_up_again(pool: Pool) {
    use weft_core::deactivation::{ResyncProgress, RunGoing};
    use weft_dispatcher::task_kinds::resync::{task, ResyncOwner, ResyncPlan, ResyncWait, Stopped};
    let (_store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let plan = ResyncPlan {
        driver: Uuid::new_v4(),
        owners: vec![ResyncOwner { owner: Owner::Shared, on: vec!["feed".into()], down: vec!["feed".into()] }],
        take_down: weft_core::TakeDown::DEFAULT,
        build: weft_core::builds::BuildHashes { binary_hash: Some("bin-A".into()), ..Default::default() },
        resynced: Vec::new(),
        urls: Vec::new(),
        held: Default::default(),
        old_builds: Vec::new(),
        old_tokens: Vec::new(),
        switching: false,
        runs: 1,
        moved_runs: 0,
        off: Vec::new(),
        stopped: None,
        given_up: None,
    };
    let driving = projects.try_begin_resyncing(id, &plan, task(TENANT, id, plan.driver)).await.unwrap().expect("entered");
    let held = projects.resync(id).await.unwrap().expect("under way");
    assert_eq!(held.progress, ResyncProgress::default(), "its task has said nothing yet");

    let waits = ResyncProgress { waits_for: vec![RunGoing { run_id: Uuid::new_v4(), since_unix: 1_760_000_000 }] };
    weft_task_store::tasks::set_progress(&pool, driving, &serde_json::to_value(&waits).unwrap()).await.unwrap();
    let held = projects.resync(id).await.unwrap().expect("under way");
    assert_eq!(held.progress, waits, "what its task said it waits for");
    assert_eq!(held.standing().progress, waits);
    assert_eq!(held.standing().stopped, None, "a task drives it");

    // Its task stopped waiting on the database.
    let at_unix = weft_dispatcher::lease::now_unix();
    let stop = Stopped { status: 500, error: "busy".into(), at_unix, waits_for: Some(ResyncWait::Database), build_cannot_run: false };
    assert!(projects.write_resync(id, &ResyncPlan { stopped: Some(stop.clone()), ..plan.clone() }).await.unwrap());
    pool.execute("UPDATE task SET status = 'failed', completed_at_unix = $2 WHERE id = $1", &[&driving, &at_unix]).await.unwrap();
    let stored: String = pool.scalar("SELECT resync::jsonb->'stopped'->>'waits_for' FROM project WHERE id = $1", &[&id]).await.unwrap();
    assert_eq!(stored, "database");
    let held = projects.resync(id).await.unwrap().expect("still held");
    assert_eq!(held.progress, ResyncProgress::default(), "a task that ended says nothing of where it is");
    let standing = held.standing();
    assert_eq!(standing.stopped.as_ref().map(|stop| stop.error.as_str()), Some("busy"));
    assert!(standing.stopped.unwrap().then.starts_with("it goes on by itself shortly"));
    let stopped: Vec<_> = projects.stopped_resyncs().await.unwrap().into_iter().map(|(p, plan)| (p, plan.unwrap().stopped)).collect();
    assert_eq!(stopped, [(id, Some(stop))], "among the stops weft takes up again");

    let resumed = projects.resume_resync(id, TENANT).await.unwrap().expect("held");
    assert_ne!(resumed, driving);
    let held = projects.resync(id).await.unwrap().expect("under way again");
    assert!(held.driven && held.plan.stopped.is_none() && held.progress == ResyncProgress::default(), "{held:?}");
}

/// A resync whose plan no longer reads can be neither driven, resumed nor
/// given up by a person, so weft gives it up, for that project alone: the
/// transition lands, and the record that takes the plan's place keeps the
/// document and why it did not read, for `weft status`. A stopped resync
/// of another project that reads is still there to take up again.
#[weft_db_macros::test]
async fn a_resync_whose_plan_no_longer_reads_is_given_up_for_its_project_alone(pool: Pool) {
    use weft_dispatcher::task_kinds::resync::{task, ResyncOwner, ResyncPlan, Stopped};
    let (_store, projects) = setup(&pool).await;
    let (broken, stopped) = (Uuid::new_v4(), Uuid::new_v4());
    for id in [broken, stopped] {
        seed_project(&projects, id, "bin-A").await;
        let plan = ResyncPlan {
            driver: Uuid::new_v4(),
            owners: vec![ResyncOwner { owner: Owner::Shared, on: vec!["feed".into()], down: vec!["feed".into()] }],
            take_down: weft_core::TakeDown::DEFAULT,
            build: weft_core::builds::BuildHashes { binary_hash: Some("bin-A".into()), ..Default::default() },
            resynced: Vec::new(),
            urls: Vec::new(),
            held: Default::default(),
            old_builds: Vec::new(),
            old_tokens: Vec::new(),
            switching: false,
            runs: 0,
            moved_runs: 0,
            off: Vec::new(),
            stopped: Some(Stopped { status: 428, error: "infra down".into(), at_unix: 1, waits_for: None, build_cannot_run: false }),
            given_up: None,
        };
        projects.try_begin_resyncing(id, &plan, task(TENANT, id, plan.driver)).await.unwrap().expect("entered");
    }
    pool.execute("UPDATE project SET resync = '{\"owners\": 7}' WHERE id = $1", &[&broken]).await.unwrap();
    let listed = |projects: weft_dispatcher::ProjectStore| async move {
        let mut listed: Vec<(Uuid, bool)> =
            projects.stopped_resyncs().await.unwrap().into_iter().map(|(id, plan)| (id, plan.is_ok())).collect();
        listed.sort();
        listed
    };
    let mut want = vec![(broken, false), (stopped, true)];
    want.sort();
    assert_eq!(listed(projects.clone()).await, want);

    assert!(!projects.give_up_unreadable_resync(stopped).await.unwrap(), "a plan that reads is not given up");
    assert!(projects.give_up_unreadable_resync(broken).await.unwrap());
    assert_eq!(projects.transition(broken).await.unwrap().unwrap().as_str(), "none", "the project lets go");
    assert_eq!(projects.resync(broken).await.unwrap(), None, "nothing is under way");
    let given_up = projects.given_up_resync(broken).await.unwrap().expect("the record is shown");
    assert!(given_up.left_down.is_empty(), "what it left down is not known: {given_up:?}");
    let unreadable = given_up.unreadable.expect("it says why");
    assert_eq!(unreadable.document, json!({ "owners": 7 }), "the document is kept");
    assert!(unreadable.error.contains(&format!("project {broken}'s resync")), "{}", unreadable.error);

    assert_eq!(listed(projects.clone()).await, vec![(stopped, true)], "it leaves the selection, and the other stays");
    assert!(!projects.give_up_unreadable_resync(broken).await.unwrap(), "nothing is left to give up");
    assert!(projects.resume_resync(stopped, TENANT).await.unwrap().is_some(), "the other is taken up again");
}

/// A held resync plan that no longer reads comes back as its own error,
/// beside every other project's stopped resync: one unreadable plan holds
/// back no other's retry.
#[weft_db_macros::test]
async fn an_unreadable_resync_plan_holds_back_no_other_stopped_resync(pool: Pool) {
    use weft_dispatcher::task_kinds::resync::{ResyncOwner, ResyncPlan, Stopped};
    let (_store, projects) = setup(&pool).await;
    let (good, bad) = (Uuid::new_v4(), Uuid::new_v4());
    seed_project(&projects, good, "bin-A").await;
    seed_project(&projects, bad, "bin-A").await;
    let plan = ResyncPlan {
        driver: Uuid::new_v4(),
        owners: vec![ResyncOwner { owner: Owner::Shared, on: vec!["feed".into()], down: Vec::new() }],
        take_down: weft_core::TakeDown::DEFAULT,
        build: weft_core::builds::BuildHashes { binary_hash: Some("bin-A".into()), ..Default::default() },
        resynced: Vec::new(),
        urls: Vec::new(),
        held: Default::default(),
        old_builds: Vec::new(),
        old_tokens: Vec::new(),
        switching: false,
        runs: 0,
        moved_runs: 0,
        off: Vec::new(),
        stopped: Some(Stopped { status: 409, error: "infra down".into(), at_unix: 1, waits_for: None, build_cannot_run: false }),
        given_up: None,
    };
    plan.row_write(&pool, good, &format!("UPDATE project SET transition = 'resyncing', {} WHERE id = $1", ResyncPlan::ROW), &[])
        .await
        .unwrap();
    pool.execute(
        "UPDATE project SET transition = 'resyncing', resync = '{\"nonsense\": true}', resync_version = $2, resync_driver = $3 WHERE id = $1",
        &[&bad, &ResyncPlan::STORED.version(), &Uuid::new_v4()],
    )
    .await
    .unwrap();
    let mut stopped = projects.stopped_resyncs().await.unwrap();
    stopped.sort_by_key(|(id, _)| *id != good);
    assert_eq!(stopped.len(), 2, "{stopped:?}");
    assert_eq!(stopped[0].0, good);
    assert_eq!(stopped[0].1.as_ref().unwrap(), &plan);
    assert_eq!(stopped[1].0, bad);
    let unreadable = stopped[1].1.as_ref().expect_err("the bad plan does not read");
    assert!(format!("{unreadable:#}").contains(&bad.to_string()), "{unreadable:#}");
}

/// A reactivation over parked triggers that fails puts them back with
/// everything they kept: the entry row and the event parked on it, the
/// wait of a run the trigger fired and the answer a person sent it. An
/// activation that is cancelled or found dead drops only the trigger's
/// own entry row: the wait and its answer stay with the run, which
/// nothing there ends.
#[weft_db_macros::test]
async fn a_failed_reactivation_keeps_what_the_park_kept(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claim");
    store.signal_insert(&governed_entry("kept-entry", id, first)).await.unwrap();
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    let waiting_run = queued_run(&store, id).await;
    let mut wait = entry_signal("kept-wait", id);
    wait.node_id = "ask".into();
    wait.is_resume = true;
    wait.run_id = Some(waiting_run);
    wait.activation_trigger = Some("feed".into());
    store.signal_insert(&wait).await.unwrap();
    activations.set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None).await.unwrap();
    assert_eq!(park(&pool, "kept-entry", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);
    assert_eq!(park(&pool, "kept-wait", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);
    let parked_on = |token: &'static str| {
        let pool = pool.clone();
        async move {
            pool.scalar::<i64>("SELECT COUNT(*) FROM parked_fire WHERE token = $1", &[&token]).await.unwrap()
        }
    };

    let failing = Uuid::new_v4();
    let previous = activations.try_begin_activating(id, &[feed_key()], failing, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claim");
    assert!(activations.restore(id, failing, &previous).await.unwrap());
    let after = activations.list(id).await.unwrap();
    assert_eq!(after[0].lifecycle.mode().as_str(), "take", "the trigger is down again, its door taking");
    for token in ["kept-entry", "kept-wait"] {
        assert!(store.signal_get(token).await.unwrap().is_some(), "{token} kept");
        assert_eq!(parked_on(token).await, 1, "and what waits on {token} too");
    }

    let cancelled = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], cancelled, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claim");
    let removed = activations
        .end_activating(id, cancelled, &ActivationLifecycle::refused(), true, None)
        .await
        .unwrap()
        .expect("owned");
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), ["kept-entry"], "only the entry row goes");
    assert!(store.signal_get("kept-wait").await.unwrap().is_some(), "the run's wait stays");
    assert_eq!(parked_on("kept-wait").await, 1, "with the answer sent to it");
}

/// An activation nobody will finish (cancelled, or its driver dead) over
/// triggers that kept work lands them parked with every signal and what
/// waits on it; over triggers that kept none, it wipes them.
#[weft_db_macros::test]
async fn an_unfinished_activation_keeps_what_its_triggers_kept(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claim");
    store.signal_insert(&governed_entry("kept-entry", id, first)).await.unwrap();
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    activations.set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::holding(), SignalsGoing::Kept, Forget::Nothing, DownCause::Person, None).await.unwrap();
    assert_eq!(park(&pool, "kept-entry", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);

    let cancelled = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], cancelled, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claim");
    let removed = activations.end_unfinished(id, cancelled).await.unwrap().expect("owned");
    assert!(removed.is_empty(), "nothing goes");
    assert_eq!(activations.list(id).await.unwrap()[0].lifecycle.mode().as_str(), "take", "down with what it kept, its door taking");
    let waiting: i64 = pool.scalar("SELECT COUNT(*) FROM parked_fire WHERE token = 'kept-entry'", &[]).await.unwrap();
    assert_eq!(waiting, 1, "the event waiting on it stays");
    assert!(activations.end_unfinished(id, cancelled).await.unwrap().is_none(), "ended once");

    pool.execute("DELETE FROM parked_fire", &[]).await.unwrap();
    let empty = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], empty, None, Claimant::Anyone, Uuid::new_v4()).await.unwrap().expect("claim");
    let removed = activations.end_unfinished(id, empty).await.unwrap().expect("owned");
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), ["kept-entry"], "no work kept: wiped");
    assert_eq!(activations.list(id).await.unwrap()[0].lifecycle.mode().as_str(), "refuse");
}

/// A trigger whose setup captured a value holding a NUL character is
/// baked, and its bake reads back exactly: the bake is kept as its JSON
/// text, with the source version it baked written beside it.
#[weft_db_macros::test]
async fn a_bake_whose_captured_values_hold_a_nul_is_kept_exactly(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    pool.execute("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)", &[&id]).await.unwrap();
    let run_id = Uuid::new_v4();
    let birth = trigger_setup_birth(id, run_id);
    queue(&store, std::slice::from_ref(&birth), false).await.unwrap();
    let complete = complete(&store, run_id).await;
    let mut bake = weft_dispatcher::store::TriggerBake::from_events(&[birth, complete], &setup_program()).unwrap().unwrap();
    let ports = json!({ "page": "a\u{0}b" });
    bake.captured.insert(
        "entry".into(),
        weft_dispatcher::store::TriggerCapture {
            spec: weft_core::primitive::SignalSpec::of_kind("timer", json!({ "spec": { "kind": "after", "duration_ms": 100 } })),
            ports: ports.clone(),
        },
    );
    assert_eq!(store.finish_trigger_setup(run_id, Some(&bake)).await.unwrap(), None);
    let saved = store.trigger_bakes(id, None).await.unwrap().read;
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].captured["entry"].ports, ports);
    let source: String = pool.scalar("SELECT source_version FROM trigger_bake WHERE project_id = $1", &[&id]).await.unwrap();
    assert_eq!(source, bake.source_version);
}
