//! Layer-3 tests for the dispatcher's OWN SQL, against a REAL Postgres.
//! The dispatcher's correctness-critical decisions (task stamping, the
//! atomic execution birth, the lifecycle writes) live in SQL statements
//! the fake stores never execute, so a faked layer cannot catch their
//! bugs. These tests exercise the actual statements.
//!
//! Each test gets a fresh isolated database via `#[sqlx::test]` (it reads
//! `$DATABASE_URL`, creates a random DB, drops it after). When
//! `DATABASE_URL` is unset the macro skips the test, so a dev box without
//! Postgres still builds. The schema is the REAL migration path in
//! production order: `app::apply_core_schema` (one pass over every
//! group), then the journal and store wrap the pool, exactly what a
//! booting dispatcher runs.
//!
//! Gated behind the `db-tests` feature (off by default) so a plain
//! `cargo test --workspace` needs no Postgres; run with
//! `cargo test -p weft-dispatcher --features db-tests` and `$DATABASE_URL` set.
#![cfg(feature = "db-tests")]

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_core::activation::ActivationKey;
use weft_core::instance::{InstanceId, Owner};
use weft_core::ProjectDefinition;
use weft_dispatcher::activation_store::{
    ActivationLifecycle, ActivationStoreOps, LifecycleWrite, PostgresActivationStore, ProjectStatus, SignalsGoing,
};
use weft_dispatcher::api::project::{due_parked_tokens, owners_with_triggers_on, release_stale_drain_claims};
use weft_dispatcher::api::signal::{
    append_parked_fire, instance_gap_tokens, instance_waits, restamp_parked_fire, signals_visible_to, ParkAppend,
    ParkedFire, ParkRefusal,
};
use weft_dispatcher::journal::postgres::PostgresJournal;
use weft_dispatcher::journal::{Journal, SignalRegistration};
use weft_dispatcher::versions::VersionStoreOps;

const TENANT: &str = "tenant-1";

/// The real boot-time migration path: journal schema first (production
/// runs it at journal connect), then every core table in dependency
/// order. Returns the journal + the project store, the two handles the
/// SQL under test goes through.
async fn setup(pool: &PgPool) -> (PostgresJournal, weft_dispatcher::ProjectStore) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
    let journal = PostgresJournal::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    (journal, projects)
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
        )
        .await
        .expect("register project");
}

/// The execute task of `execution_id`'s birth, on image `binary_hash`.
fn execute_task(
    project_id: Uuid,
    execution_id: weft_core::ExecutionId,
    binary_hash: &str,
    unrecorded_birth: Option<&[weft_journal::ExecEvent]>,
) -> weft_task_store::tasks::NewTask {
    weft_dispatcher::task_kinds::execute::execution_task_spec(weft_dispatcher::task_kinds::execute::ExecutionTask {
        kind: weft_task_store::TaskKind::Execute,
        project_id,
        execution_id,
        definition_hash: "def-1",
        binary_hash,
        tenant_id: TENANT,
        run_class: weft_core::run_class::RunClass::Short,
        live_connection: None,
        unrecorded_birth,
    })
    .unwrap()
}

/// A minimal entry-signal registration.
fn entry_signal(token: &str, project_id: Uuid) -> SignalRegistration {
    SignalRegistration {
        instance: None,
        activation_trigger: None,
        source_version: None,
        setup_execution_id: None,
        program: None,
        token: token.to_string(),
        tenant_id: TENANT.to_string(),
        project_id,
        execution_id: None,
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
    }
}

// ----- task stamping (the enqueue reads the project row) -------------------

/// Birth and resume retain the original image across project edits.
#[sqlx::test]
async fn execution_birth_and_resume_pin_the_original_image(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;

    let execution_id = weft_core::ExecutionId::new_v4();
    let program = weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-1".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    };
    let start = weft_journal::ExecEvent::ExecutionStarted {
        execution_id, project_id: id, entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()),
        program: Some(program), source_version: None, run_kind: weft_core::exec::RunKind::Execution, subgraph: None, seed: None, instance: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        run_class: weft_core::run_class::RunClass::Short,
    };
    let task = execute_task(id, execution_id, "bin-A", None);
    seed_project(&projects, id, "bin-B").await;
    journal.start_execution(&start, &[], task, false).await.unwrap();

    let (kind, binary_hash): (String, Option<String>) = sqlx::query_as(
        "SELECT kind, binary_hash FROM task WHERE execution_id = $1",
    )
    .bind(execution_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("task row");
    assert_eq!(kind, "execute");
    assert_eq!(
        binary_hash.as_deref(),
        Some("bin-A"),
        "the execute task must carry the image it was enqueued for"
    );
    weft_dispatcher::task_kinds::execute::enqueue_resume(&pool, id, execution_id, "def-1", TENANT).await.unwrap();
    let resume_hash: String = sqlx::query_scalar("SELECT binary_hash FROM task WHERE execution_id = $1 AND kind = 'resume'")
        .bind(execution_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(resume_hash, "bin-A");
}

// ----- atomic execution birth ------------------------------------------

#[sqlx::test]
async fn registered_sources_belong_to_the_exact_program(pool: PgPool) {
    let (_, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    let implementations = std::collections::BTreeMap::new();
    let source = std::collections::BTreeMap::from([("main.weft".into(), "original-file".into())]);
    projects.register_with_hashes(empty_project(id), "sources", "", TENANT,
        Some("binary"), Some("graph"), None, None, Some(&implementations), Some(&source)).await.unwrap();
    let program = projects.running_program_identity(id).await.unwrap().unwrap();
    assert_eq!(projects.program_source(id, &program).await.unwrap(), source);
    // A later build of other code moves the running program, and the
    // old identity no longer answers for it.
    projects.register_with_hashes(empty_project(id), "sources", "", TENANT,
        Some("different-binary"), Some("graph"), None, None, Some(&implementations), None).await.unwrap();
    assert!(projects.program_source(id, &program).await.is_err());
    let changed = weft_core::project::hash::ProgramIdentity { binary_hash: "different-binary".into(), ..program };
    assert!(projects.program_source(id, &changed).await.is_err(), "changing code cannot relabel old sources");
}

fn trigger_setup_birth(id: Uuid, execution_id: Uuid) -> (weft_journal::ExecEvent, weft_task_store::tasks::NewTask) {
    let program = weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-1".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    };
    let start = weft_journal::ExecEvent::ExecutionStarted {
        execution_id, project_id: id, entry_node: "entry".into(),
        phase: weft_core::context::Phase::TriggerSetup, definition_hash: Some("def-1".into()),
        program: Some(program), source_version: Some("source".into()), run_kind: weft_core::exec::RunKind::Execution, subgraph: None, seed: None, instance: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        run_class: weft_core::run_class::RunClass::Short,
    };
    let task = execute_task(id, execution_id, "bin-A", None);
    (start, task)
}

#[sqlx::test]
async fn pruning_a_source_waits_for_setup_and_removes_its_unused_bake(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let versions = weft_dispatcher::versions::PostgresVersionStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    sqlx::query("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)")
        .bind(id).execute(&pool).await.unwrap();
    let execution_id = Uuid::new_v4();
    let (birth, task) = trigger_setup_birth(id, execution_id);
    journal.start_execution(&birth, &[], task, false).await.unwrap();
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err());
    let complete = weft_journal::ExecEvent::ExecutionCompleted { execution_id, at_unix: 2 };
    journal.record_event(&complete).await.unwrap();
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err(), "publication still owns this source");
    let bake = weft_dispatcher::journal::TriggerBake::from_events(&[birth, complete]).unwrap().unwrap();
    journal.finish_trigger_setup(execution_id, Some(&bake)).await.unwrap();
    versions.delete_versions(id, &["source".into()]).await.unwrap();
    assert!(journal.trigger_bakes(id, None).await.unwrap().is_empty());
    assert!(versions.version(id, "source").await.unwrap().is_none());
}

/// The feed trigger's shared activation, and an entry signal it governs.
fn feed_key() -> ActivationKey {
    ActivationKey::new("feed", Owner::Shared)
}

fn governed_entry(token: &str, project_id: Uuid, setup: Uuid) -> SignalRegistration {
    let mut entry = entry_signal(token, project_id);
    entry.activation_trigger = Some("feed".into());
    entry.setup_execution_id = Some(setup);
    entry
}

#[sqlx::test]
async fn a_rearm_claim_refuses_a_trigger_that_left_active(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active))
        .await.unwrap(), Err(weft_dispatcher::activation_store::ClaimRefused::NotExpected)), "a re-arm never creates a trigger that was never activated");
    let setup_execution_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_execution_id, None).await.unwrap().is_ok());
    activations.end_activating(id, setup_execution_id, &ActivationLifecycle::parked(), false, None).await.unwrap().expect("owned");
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active))
        .await.unwrap(), Err(weft_dispatcher::activation_store::ClaimRefused::NotExpected)), "a deactivate between the look and the claim wins: the re-arm refuses");
    let again = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], again, None).await.unwrap().is_ok());
    activations.end_activating(id, again, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    assert!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active))
        .await.unwrap().is_ok(), "still Active: the re-arm claims");
}

#[sqlx::test]
async fn an_instances_wipe_takes_its_row_and_its_signals_in_one_write(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let setup_execution_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), setup_execution_id, None).await.unwrap().is_ok());
    let mut entry = governed_entry("ada-wiped-entry", id, setup_execution_id);
    entry.instance = Some(ada.clone());
    journal.signal_insert(&entry).await.unwrap();
    activations.end_activating(id, setup_execution_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");

    let written = activations
        .set_lifecycle_guarded(id, std::slice::from_ref(&ada_key), &ActivationLifecycle::wiped(), SignalsGoing::Activations)
        .await
        .unwrap();
    let LifecycleWrite::Applied { unlisten } = written else { panic!("applied: {written:?}") };
    assert_eq!(unlisten.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["ada-wiped-entry"],
        "the signals are handed back for the listener cleanup");
    assert!(activations.list(id).await.unwrap().is_empty(), "the instance's wiped row is gone");
    assert!(journal.signal_get("ada-wiped-entry").await.unwrap().is_none(), "and its signal went with it");
}

/// A park keeps the signals and leaves them listening: what they hear
/// waits for the trigger to be back on, so the write hands nothing back
/// for the listener to let go of.
#[sqlx::test]
async fn a_park_keeps_its_signals_listening(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let setup_execution_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_execution_id, None).await.unwrap().is_ok());
    journal.signal_insert(&governed_entry("parked-entry", id, setup_execution_id)).await.unwrap();
    activations.end_activating(id, setup_execution_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");

    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept)
        .await
        .unwrap();
    let LifecycleWrite::Applied { unlisten } = written else { panic!("applied: {written:?}") };
    assert!(unlisten.is_empty(), "a parked trigger goes on listening: {unlisten:?}");
    assert!(journal.signal_get("parked-entry").await.unwrap().is_some(), "and its signal stays stored");
}

/// A hibernation takes work until its grace window ends, then stops: the
/// end flips only the windows that are over, keeps them hibernating (the
/// row stays, so `weft activate` brings it back), and says when the next
/// window still open ends.
#[sqlx::test]
async fn a_hibernation_stops_taking_work_once_its_window_ends(pool: PgPool) {
    use weft_dispatcher::activation_store::{end_grace_windows, grace_windows_over, next_grace_end};
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (over, open) = (feed_key(), ActivationKey::new("reads", Owner::Shared));
    let setup_execution_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[over.clone(), open.clone()], setup_execution_id, None).await.unwrap().is_ok());
    activations.end_activating(id, setup_execution_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    for (key, deadline) in [(&over, 100), (&open, i64::MAX / 2)] {
        activations
            .set_lifecycle_guarded(id, std::slice::from_ref(key), &ActivationLifecycle::hibernating(deadline), SignalsGoing::Kept)
            .await
            .unwrap();
    }
    assert_eq!(next_grace_end(&pool).await.unwrap(), Some(100));

    // The windows are over by the database's clock: one in the past, one
    // far ahead.
    assert_eq!(grace_windows_over(&pool).await.unwrap(), vec![(id, over.clone())]);
    assert_eq!(end_grace_windows(&pool, id, &[over.clone(), open.clone()]).await.unwrap(), 1, "only the window that is over ends");
    assert!(grace_windows_over(&pool).await.unwrap().is_empty(), "taking no work, it is over no longer");
    assert_eq!(end_grace_windows(&pool, id, std::slice::from_ref(&over)).await.unwrap(), 0, "a window ends once");
    let listed = activations.list(id).await.unwrap();
    let lifecycle = |key: &ActivationKey| listed.iter().find(|a| &a.key == key).expect("kept").lifecycle.clone();
    assert!(!lifecycle(&over).accepting_fires, "the window that is over takes no more work");
    assert_eq!(lifecycle(&over).mode(), weft_core::activation::ActivationMode::Down(weft_core::DeactivationMode::Hibernate), "and is still a hibernation");
    assert!(lifecycle(&open).accepting_fires, "the window still open takes work");
    assert_eq!(next_grace_end(&pool).await.unwrap(), Some(i64::MAX / 2));
}

#[sqlx::test]
async fn activation_cleanup_cannot_finish_or_wipe_a_newer_activation(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], first, None).await.unwrap().is_ok());
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), None).await.unwrap(),
        Err(weft_dispatcher::activation_store::ClaimRefused::Claimed)), "one activation of a trigger at a time");
    journal.signal_insert(&governed_entry("old-entry", id, first)).await.unwrap();
    let removed = activations.end_activating(id, first, &ActivationLifecycle::wiped(), true, None).await.unwrap().unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].token, "old-entry");
    assert!(journal.signal_get("old-entry").await.unwrap().is_none());
    let (late_birth, late_task) = trigger_setup_birth(id, first);
    assert!(journal.start_execution(&late_birth, &[], late_task, true).await.is_err(),
        "a cancelled activation cannot later start its setup");
    assert!(journal.events_log(first).await.unwrap().is_empty());

    let second = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], second, None).await.unwrap().is_ok());
    journal.signal_insert(&governed_entry("new-entry", id, second)).await.unwrap();
    let program = weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-2".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    };
    assert!(!activations.record_activation_source(id, first, &program, "first-source").await.unwrap(),
        "the ended activation cannot record over the newer one");
    assert!(activations.record_activation_source(id, second, &program, "second-source").await.unwrap());
    for to in [ActivationLifecycle::wiped(), ActivationLifecycle::active()] {
        assert!(activations.end_activating(id, first, &to, true, None).await.unwrap().is_none());
        assert!(journal.signal_get("new-entry").await.unwrap().is_some());
        let row = activations.list(id).await.unwrap().remove(0);
        assert_eq!(row.lifecycle.activating_execution_id, Some(second));
        assert_eq!(row.source_version.as_deref(), Some("second-source"));
    }
    assert!(activations.end_activating(id, second, &ActivationLifecycle::active(), false, None).await.unwrap().is_some());
    let row = activations.list(id).await.unwrap().remove(0);
    assert_eq!((row.lifecycle.status, row.source_version.as_deref()), (ProjectStatus::Active, Some("second-source")));
}

/// Each owner's copy of a trigger is its own activation: an instance's
/// activation neither blocks nor ends the shared one or another
/// instance's, and taking one down removes only the signals it governs.
#[sqlx::test]
async fn an_instances_activation_is_its_own_row(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let bob_key = ActivationKey::new("feed", Owner::Instance(bob.clone()));

    let shared_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], shared_setup, None).await.unwrap().is_ok());
    let ada_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), ada_setup, None).await.unwrap().is_ok(),
        "the shared activation in flight does not block an instance's");
    assert!(matches!(activations.try_begin_activating(id, &[bob_key.clone(), ada_key.clone()], Uuid::new_v4(), None).await.unwrap(),
        Err(weft_dispatcher::activation_store::ClaimRefused::Claimed)), "a claim over a busy key claims nothing");
    let bob_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&bob_key), bob_setup, None).await.unwrap().is_ok());

    let mut ada_entry = governed_entry("ada-entry", id, ada_setup);
    ada_entry.instance = Some(ada.clone());
    let mut wrong = ada_entry.clone();
    wrong.token = "wrong".into();
    wrong.setup_execution_id = Some(bob_setup);
    assert!(journal.signal_insert(&wrong).await.is_err(), "bob's setup cannot arm ada's trigger");
    journal.signal_insert(&ada_entry).await.unwrap();
    let mut bob_entry = governed_entry("bob-entry", id, bob_setup);
    bob_entry.instance = Some(bob.clone());
    journal.signal_insert(&bob_entry).await.unwrap();

    for setup in [shared_setup, ada_setup, bob_setup] {
        activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }
    assert!(matches!(
        activations.set_lifecycle_guarded(id, std::slice::from_ref(&ada_key), &ActivationLifecycle::wiped(), SignalsGoing::Kept).await.unwrap(),
        LifecycleWrite::Applied { .. }
    ));
    let statuses: Vec<(Option<String>, ProjectStatus)> = activations.list(id).await.unwrap().into_iter()
        .map(|a| (a.key.instance().map(|m| m.as_str().to_string()), a.lifecycle.status)).collect();
    assert_eq!(statuses, vec![
        (None, ProjectStatus::Active),
        (Some("bob".into()), ProjectStatus::Active),
    ], "an instance's wiped trigger leaves no row, like one never activated");

    let begun = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&bob_key), begun, None).await.unwrap().is_ok());
    let refused = activations.set_lifecycle_guarded(id, &[feed_key(), bob_key], &ActivationLifecycle::wiped(), SignalsGoing::Kept).await.unwrap();
    assert!(matches!(&refused, LifecycleWrite::Rejected { blocker } if blocker.contains("instance 'bob'")), "{refused:?}");
    let removed = activations.end_activating(id, begun, &ActivationLifecycle::wiped(), true, None).await.unwrap().unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["bob-entry"]);
    assert!(journal.signal_get("ada-entry").await.unwrap().is_some(), "ada's signal is ada's activation's to remove");
}

/// An instance's activation cancelled (or reaped) mid-way lands wiped, so
/// its row goes with the signals it registered, as if never activated;
/// the shared one cancelled the same way keeps its row, inactive.
#[sqlx::test]
async fn a_cancelled_instances_activation_leaves_no_row(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));

    let setup_execution_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key(), ada_key.clone()], setup_execution_id, None).await.unwrap().is_ok());
    let mut ada_entry = governed_entry("ada-entry", id, setup_execution_id);
    ada_entry.instance = Some(ada.clone());
    journal.signal_insert(&ada_entry).await.unwrap();
    journal.signal_insert(&governed_entry("shared-entry", id, setup_execution_id)).await.unwrap();

    let mut removed: Vec<String> = activations
        .end_activating(id, setup_execution_id, &ActivationLifecycle::wiped(), true, None)
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
    let again = activations.try_begin_activating(id, std::slice::from_ref(&ada_key), Uuid::new_v4(), None).await.unwrap();
    assert!(again.expect("claimed").is_empty(), "ada activates again as never activated");
}

/// A re-arm's values are stored in the transaction that lands its
/// activation Active, and never by an activation that no longer owns its
/// triggers.
#[sqlx::test]
async fn a_rearm_stores_its_values_as_it_lands(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let writes = [weft_access_store::InstanceValueWrite {
        step: "feed".into(),
        field: "channel".into(),
        value: json!("news"),
        connection: None,
    }];
    let store = weft_dispatcher::activation_store::ValuesStore { tenant: TENANT, instance: &ada, writes: &writes, cleared: &[] };
    let stored = || async { weft_access_store::instance_values(&pool, TENANT, id, &ada).await.unwrap() };

    let setup_execution_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), setup_execution_id, None).await.unwrap().is_ok());
    assert!(activations.end_activating(id, Uuid::new_v4(), &ActivationLifecycle::active(), false, Some(&store))
        .await.unwrap().is_none());
    assert!(stored().await.is_empty(), "a landing that owns nothing stores nothing");
    assert!(activations.end_activating(id, setup_execution_id, &ActivationLifecycle::active(), false, Some(&store))
        .await.unwrap().is_some());
    assert_eq!(stored().await.get("feed").and_then(|fields| fields.get("channel")), Some(&json!("news")));
}

/// An instance's trigger wiped leaves no row, whether the wipe lands at
/// once or after its drain; the shared trigger wiped keeps its row, and
/// an instance hibernated or parked keeps its own. The instance activates again
/// from nothing.
#[sqlx::test]
async fn a_wiped_instances_row_is_forgotten(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let instance = |name: &str| ActivationKey::new("feed", Owner::Instance(InstanceId::new(name).unwrap()));
    let (ada, bob, cyd) = (instance("ada"), instance("bob"), instance("cyd"));
    for key in [feed_key(), ada.clone(), bob.clone(), cyd.clone()] {
        let setup = Uuid::new_v4();
        assert!(activations.try_begin_activating(id, std::slice::from_ref(&key), setup, None).await.unwrap().is_ok());
        activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }
    let rows = || async {
        activations.list(id).await.unwrap().into_iter()
            .map(|a| (a.key.instance().map(|m| m.as_str().to_string()), a.lifecycle.status)).collect::<Vec<_>>()
    };

    assert!(matches!(
        activations.set_lifecycle_guarded(id, &[feed_key(), ada.clone(), cyd.clone()], &ActivationLifecycle::wiped(), SignalsGoing::Kept).await.unwrap(),
        LifecycleWrite::Applied { .. }
    ));
    assert_eq!(rows().await, vec![(None, ProjectStatus::Inactive), (Some("bob".into()), ProjectStatus::Active)]);

    let draining = ActivationLifecycle::deactivating_to(ActivationLifecycle::wiped(), i64::MAX);
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&bob), &draining, SignalsGoing::Kept).await.unwrap();
    assert_eq!(rows().await[1], (Some("bob".into()), ProjectStatus::Deactivating), "the row stays while it drains");
    assert!(activations.cas_status(id, &bob, ProjectStatus::Deactivating, ProjectStatus::Inactive).await.unwrap());
    assert_eq!(rows().await, vec![(None, ProjectStatus::Inactive)], "the drain landing forgets the instance's row");
    assert!(!activations.cas_status(id, &bob, ProjectStatus::Deactivating, ProjectStatus::Inactive).await.unwrap(),
        "a second landing finds nothing");

    let setup = Uuid::new_v4();
    let previous = activations.try_begin_activating(id, std::slice::from_ref(&ada), setup, None).await.unwrap().expect("claimed");
    assert!(previous.is_empty(), "ada activates again as never activated");
    activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&ada), &ActivationLifecycle::parked(), SignalsGoing::Kept).await.unwrap();
    assert_eq!(rows().await[1], (Some("ada".into()), ProjectStatus::Inactive), "a parked instance keeps the row");
}

/// Who a plain `weft resync` brings up to date, and who `weft deactivate
/// --all-instances` takes down: every owner with a trigger on, read off the
/// activation rows, the program first, instances in id order, and nobody
/// whose triggers are off or still coming up.
#[sqlx::test]
async fn the_owners_with_triggers_on_come_from_the_rows(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let instance = |name: &str| Owner::Instance(InstanceId::new(name).unwrap());
    let owners = || async { owners_with_triggers_on(&activations, id).await.unwrap() };
    assert!(owners().await.is_empty(), "no row, nobody's triggers are on");

    for owner in [Owner::Shared, instance("bob"), instance("ada")] {
        let setup = Uuid::new_v4();
        assert!(activations.try_begin_activating(id, &[ActivationKey::new("feed", owner)], setup, None).await.unwrap().is_ok());
        activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }
    assert!(activations.try_begin_activating(id, &[ActivationKey::new("feed", instance("cyd"))], Uuid::new_v4(), None).await.unwrap().is_ok());
    assert_eq!(owners().await, vec![Owner::Shared, instance("ada"), instance("bob")], "cyd's is still activating");

    let ada = ActivationKey::new("feed", instance("ada"));
    assert!(matches!(
        activations.set_lifecycle_guarded(id, &[ada, feed_key()], &ActivationLifecycle::wiped(), SignalsGoing::Kept).await.unwrap(),
        LifecycleWrite::Applied { .. }
    ));
    assert_eq!(owners().await, vec![instance("bob")], "an instance's triggers stay on when the program's go off");
}

#[sqlx::test]
async fn trigger_bake_ownership_publication_and_project_cleanup(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    sqlx::query("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)")
        .bind(id).execute(&pool).await.unwrap();
    let first = Uuid::new_v4();
    let (birth, task) = trigger_setup_birth(id, first);
    journal.start_execution(&birth, &[], task, false).await.unwrap();
    let complete = weft_journal::ExecEvent::ExecutionCompleted { execution_id: first, at_unix: 2 };
    journal.record_event(&complete).await.unwrap();
    let bake = weft_dispatcher::journal::TriggerBake::from_events(&[birth.clone(), complete]).unwrap().unwrap();
    journal.finish_trigger_setup(first, Some(&bake)).await.unwrap();

    let second = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], second, None).await.unwrap().is_ok());
    let (second_birth, second_task) = trigger_setup_birth(id, second);
    let (other_birth, other_task) = trigger_setup_birth(id, Uuid::new_v4());
    assert!(journal.start_execution(&other_birth, &[], other_task, true).await.is_err(),
        "a setup no activation is setting up is never born as one");
    journal.start_execution(&second_birth, &[], second_task, true).await.unwrap();
    let mut entry = governed_entry("baked-entry", id, first);
    entry.program = Some(bake.program.clone());
    entry.source_version = Some(bake.source_version.clone());
    assert!(journal.signal_insert(&entry).await.is_err(), "a setup that owns no activation cannot arm");
    entry.setup_execution_id = Some(second);
    journal.signal_insert(&entry).await.unwrap();
    let armed = journal.signal_get("baked-entry").await.unwrap().unwrap();
    assert_eq!(armed.program, entry.program);
    assert_eq!(armed.source_version, entry.source_version);
    assert_eq!(armed.setup_execution_id, Some(second));
    activations.end_activating(id, second, &ActivationLifecycle::active(), false, None).await.unwrap();
    assert!(journal.signal_insert(&entry).await.is_err(), "no late registration after activation ends");
    journal.finish_trigger_setup(second, None).await.unwrap();
    assert_eq!(journal.trigger_bakes(id, None).await.unwrap()[0].execution_id, first);
    journal.delete_execution(first).await.unwrap();
    assert_eq!(journal.trigger_bakes(id, None).await.unwrap()[0].execution_id, first);
    assert!(journal.trigger_bakes(Uuid::new_v4(), None).await.unwrap().is_empty());
    projects.remove(id).await.unwrap();
    assert!(journal.trigger_bakes(id, None).await.unwrap().is_empty());
}

/// The birth of an execution (`ExecutionStarted` + `execution` seed +
/// kicks + the execute task) is ONE transaction: a failure anywhere rolls
/// everything back. Witness: starting for a project with NO row fails the
/// seed's project check AFTER the ExecutionStarted insert already ran in the
/// same transaction; nothing may survive (no journal row, no execution, no task).
/// Before the atomic birth, this exact failure left a journaled "ghost"
/// execution with no task, which nothing would ever run or reclaim.
#[sqlx::test]
async fn start_execution_birth_is_atomic(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let missing_project = Uuid::new_v4(); // never registered
    let execution_id = weft_core::ExecutionId::new_v4();
    let now = 1_700_000_000u64;
    let start = weft_journal::ExecEvent::ExecutionStarted {
        execution_id,
        project_id: missing_project,
        entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        program: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
        subgraph: None,
        seed: None,
        instance: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: now,
        run_class: weft_core::run_class::RunClass::Short,
    };
    let kick = weft_journal::ExecEvent::NodeKicked {
        execution_id,
        node_id: "entry".into(),
        frames: Vec::new(),
        firing: true,
        payload: None,
        port_snapshot: None,
        at_unix: now,
    };
    let task = weft_task_store::tasks::NewTask {
        kind: weft_task_store::TaskKind::Execute.into(),
        target: weft_task_store::TaskTarget::Worker,
        project_id: Some(missing_project),
        dedup_key: Some(format!("{execution_id}:execute")),
        execution_id: Some(execution_id.to_string()),
        tenant_id: TENANT.into(),
        target_replica: None,
        binary_hash: None,
        payload: json!({}),
    };
    let err = journal
        .start_execution(&start, std::slice::from_ref(&kick), task.clone(), false)
        .await
        .expect_err("missing project must fail the birth");
    assert!(format!("{err:#}").contains("has no row"), "{err:?}");
    // NOTHING survives: the whole birth rolled back.
    let (events,): (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM exec_event WHERE execution_id = $1")
            .bind(execution_id.to_string())
            .fetch_one(&pool)
            .await
            .expect("count events");
    let (execution_ids,): (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM execution WHERE execution_id = $1")
            .bind(execution_id.to_string())
            .fetch_one(&pool)
            .await
            .expect("count executions");
    let (tasks,): (i64,) = sqlx::query_as("SELECT COUNT(*)::bigint FROM task WHERE execution_id = $1")
        .bind(execution_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("count tasks");
    assert_eq!((events, execution_ids, tasks), (0, 0, 0), "a failed birth must leave nothing");

    // And the positive path: with the project registered, the SAME birth
    // commits everything together.
    let registered = Uuid::new_v4();
    seed_project(&projects, registered, "bin-A").await;
    let execution_id2 = weft_core::ExecutionId::new_v4();
    let start2 = weft_journal::ExecEvent::ExecutionStarted {
        execution_id: execution_id2,
        project_id: registered,
        entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        program: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
        subgraph: None,
        seed: None,
        instance: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: now,
        run_class: weft_core::run_class::RunClass::Short,
    };
    let task2 = weft_task_store::tasks::NewTask {
        execution_id: Some(execution_id2.to_string()),
        dedup_key: Some(format!("{execution_id2}:execute")),
        project_id: Some(registered),
        ..task
    };
    journal
        .start_execution(&start2, &[], task2.clone(), false)
        .await
        .expect("birth for a registered project");
    let (events2,): (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM exec_event WHERE execution_id = $1")
            .bind(execution_id2.to_string())
            .fetch_one(&pool)
            .await
            .expect("count events");
    let (tasks2,): (i64,) = sqlx::query_as("SELECT COUNT(*)::bigint FROM task WHERE execution_id = $1")
        .bind(execution_id2.to_string())
        .fetch_one(&pool)
        .await
        .expect("count tasks");
    assert_eq!((events2, tasks2), (1, 1), "a successful birth commits the event AND the task");

    sqlx::query("DELETE FROM task WHERE execution_id = $1").bind(execution_id2.to_string()).execute(&pool).await.unwrap();
    journal.start_execution(&start2, &[], task2.clone(), false).await.unwrap();
    let births: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM exec_event WHERE execution_id = $1 AND kind = 'execution_started'")
        .bind(execution_id2.to_string()).fetch_one(&pool).await.unwrap();
    let tasks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE execution_id = $1")
        .bind(execution_id2.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!((births, tasks), (1, 0), "finished admission cannot create a second execution");
}

/// An unrecorded run is born with its execution row and task alone (no
/// journal row), is never listed, and is forgotten by a cancel that
/// finds no process driving it. A failed one written afterwards becomes an
/// ordinary run, listed with its rows; a second write is refused.
#[sqlx::test]
async fn an_unrecorded_run_is_born_unjournaled_and_forgotten_or_recorded(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let birth = |execution_id: weft_core::ExecutionId| {
        let start = weft_journal::ExecEvent::ExecutionStarted {
            execution_id, project_id: id, entry_node: "route".into(),
            phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()),
            program: None, source_version: None, run_kind: weft_core::exec::RunKind::Unrecorded,
            subgraph: None, seed: None, instance: None, fired_trigger: Some("route".into()),
            instance_values: Default::default(), picks: Default::default(), at_unix: 1,
            run_class: weft_core::run_class::RunClass::Short,
        };
        let kick = weft_journal::ExecEvent::NodeKicked {
            execution_id, node_id: "route".into(), frames: vec![], firing: true, payload: None, port_snapshot: None, at_unix: 1,
        };
        let task = execute_task(id, execution_id, "bin-A", Some(&[start.clone(), kick.clone()]));
        (start, kick, task)
    };
    let rows = |execution_id: weft_core::ExecutionId| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM exec_event WHERE execution_id = $1")
                .bind(execution_id.to_string()).fetch_one(&pool).await.unwrap()
        }
    };
    let kind = |execution_id: weft_core::ExecutionId| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT kind FROM execution WHERE execution_id = $1")
                .bind(execution_id.to_string()).fetch_optional(&pool).await.unwrap()
        }
    };
    let listed = || async {
        journal.list_executions(TENANT, &weft_dispatcher::journal::ExecutionQuery {
            limit: 50, ..Default::default()
        }).await.unwrap().executions.into_iter().map(|e| e.execution_id).collect::<Vec<_>>()
    };

    // Born: the execution row and the task, no journal row.
    let gone = weft_core::ExecutionId::new_v4();
    let (start, kick, task) = birth(gone);
    journal.start_execution(&start, std::slice::from_ref(&kick), task, false).await.unwrap();
    assert_eq!(rows(gone).await, 0, "an unrecorded birth writes no journal row");
    assert_eq!(kind(gone).await.as_deref(), Some("unrecorded"));
    let payload: serde_json::Value = sqlx::query_scalar("SELECT payload FROM task WHERE execution_id = $1")
        .bind(gone.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(payload["unrecorded_birth"].as_array().map(Vec::len), Some(2), "the birth rides the task");
    assert!(!listed().await.contains(&gone), "an unrecorded run is not listed");

    // A cancel with no process driving it: nothing journaled, the run forgotten
    // and its files queued for the sweep.
    journal.cancel_execution(gone, None, &weft_core::exec::CancelCause::User).await.unwrap();
    assert_eq!(rows(gone).await, 0, "no cancel terminal for an unrecorded run");
    assert_eq!(kind(gone).await, None, "forgotten");
    let swept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM storage_sweep WHERE execution_id = $1")
        .bind(gone.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(swept, 1);

    // A failed one, written afterwards, is an ordinary listed run.
    let failed = weft_core::ExecutionId::new_v4();
    let (start, kick, task) = birth(failed);
    journal.start_execution(&start, std::slice::from_ref(&kick), task, false).await.unwrap();
    let mut record = vec![start, kick, weft_journal::ExecEvent::ExecutionFailed { execution_id: failed, error: "boom".into(), at_unix: 2 }];
    if let weft_journal::ExecEvent::ExecutionStarted { run_kind, .. } = &mut record[0] {
        *run_kind = weft_core::exec::RunKind::Execution;
    }
    weft_journal::unrecorded::record_retroactively(&pool, &record, None).await.unwrap();
    assert_eq!(rows(failed).await, 3);
    assert_eq!(kind(failed).await.as_deref(), Some("execution"));
    assert!(listed().await.contains(&failed), "a failed unrecorded run lists like any other");
    let again = weft_journal::unrecorded::record_retroactively(&pool, &record, None).await.unwrap_err();
    assert!(again.to_string().contains("not an unrecorded run"), "{again:#}");

    // A run whose costs reached the journal keeps its row: they are
    // addressed by it.
    let paid = weft_core::ExecutionId::new_v4();
    let (start, kick, task) = birth(paid);
    journal.start_execution(&start, std::slice::from_ref(&kick), task, false).await.unwrap();
    weft_journal::record_events(&pool, &[weft_journal::ExecEvent::CostReported {
        execution_id: paid, node_id: "llm".into(), frames: vec![], cost_id: "c".into(), service: "llm".into(),
        model: None, amount_usd: Some(0.1), billed: true, origin: weft_core::CredentialOwner::Author,
        metadata: serde_json::json!({}), at_unix: 2,
    }], None, None).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    assert!(!weft_journal::unrecorded::forget_in(&mut tx, paid).await.unwrap());
    tx.commit().await.unwrap();
    assert_eq!(kind(paid).await.as_deref(), Some("unrecorded"));
    assert!(!listed().await.contains(&paid));
}

/// An in-flight unrecorded run is live to every project sweep exactly
/// while a worker can still drive it: its execute task waiting, or held
/// by a claim that is being renewed. The same run whose claim lapsed is
/// not live, and neither is one whose task finished. The stop-by-tag
/// read goes through the same rule.
#[sqlx::test]
async fn an_unrecorded_run_is_live_while_a_worker_holds_its_task(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let execution_id = weft_core::ExecutionId::new_v4();
    let start = weft_journal::ExecEvent::ExecutionStarted {
        execution_id, project_id: id, entry_node: "route".into(),
        phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()),
        program: None, source_version: None, run_kind: weft_core::exec::RunKind::Unrecorded,
        subgraph: None, seed: None, instance: None, fired_trigger: Some("route".into()),
        instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        run_class: weft_core::run_class::RunClass::Short,
    };
    let task = execute_task(id, execution_id, "bin-A", Some(std::slice::from_ref(&start)));
    journal.start_execution(&start, &[], task, false).await.unwrap();
    sqlx::query("INSERT INTO execution_tag (execution_id, tag, tagged_at_unix) VALUES ($1, 'poll', 1)")
        .bind(execution_id.to_string()).execute(&pool).await.unwrap();

    let live = || async {
        journal.list_non_terminal_execution_ids_for_project(id).await.unwrap().into_iter().map(|(c, _)| c).collect::<Vec<_>>()
    };
    let tagged = || async {
        weft_journal::tags::live_tagged_executions(&pool, id, "poll").await.unwrap().len()
    };
    assert_eq!(live().await, vec![execution_id], "waiting for a worker: live");
    assert_eq!(tagged().await, 1, "and reachable by tag");

    let claim = |until: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query("UPDATE task SET status = 'claimed', claimed_by = 'worker-a', claimed_until_unix = $2 WHERE execution_id = $1")
                .bind(execution_id.to_string()).bind(until).execute(&pool).await.unwrap();
        }
    };
    let now = weft_dispatcher::lease::now_unix();
    claim(now + 60).await;
    assert_eq!(live().await, vec![execution_id], "held by a renewed claim: live");

    claim(now - 60).await;
    assert!(live().await.is_empty(), "its claim lapsed, so its worker is gone and so is the run");
    assert_eq!(tagged().await, 0);

    claim(now + 60).await;
    sqlx::query("UPDATE task SET status = 'complete' WHERE execution_id = $1").bind(execution_id.to_string()).execute(&pool).await.unwrap();
    assert!(live().await.is_empty(), "its task finished, so did the run");
}

/// `weft rm --journal`'s quiesce waits for every live run of the
/// project, an unrecorded one started by hand included, and returns at
/// the ending's announcement rather than on a safety tick.
#[sqlx::test]
async fn quiesce_waits_until_no_run_of_the_project_is_live(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let execution_id = weft_core::ExecutionId::new_v4();
    let start = weft_journal::ExecEvent::ExecutionStarted {
        execution_id, project_id: id, entry_node: "a".into(),
        phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()),
        program: None, source_version: None, run_kind: weft_core::exec::RunKind::Unrecorded,
        subgraph: None, seed: None, instance: None, fired_trigger: None,
        instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        run_class: weft_core::run_class::RunClass::Short,
    };
    let task = execute_task(id, execution_id, "bin-A", Some(std::slice::from_ref(&start)));
    journal.start_execution(&start, &[], task, false).await.unwrap();

    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), weft_dispatcher::take_down::RUN_ENDING_CHANNELS)
        .await
        .unwrap();
    let journal = std::sync::Arc::new(journal);
    let waiter = {
        let journal = journal.clone();
        let signals = watch.subscribe();
        tokio::spawn(async move { weft_dispatcher::take_down::wait_until_no_live_runs(journal.as_ref(), signals, id).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!waiter.is_finished(), "a hand-started run is still live, so the quiesce waits");

    let mut tx = pool.begin().await.unwrap();
    weft_journal::unrecorded::forget_in(&mut tx, execution_id).await.unwrap();
    tx.commit().await.unwrap();
    // What every caller of `forget_in` does once its transaction commits.
    weft_task_store::announce::committed(&pool);
    tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
        .await
        .expect("the ending wakes the quiesce at once")
        .unwrap()
        .unwrap();
}

/// Forgetting an unrecorded run announces its ending on the channel the
/// dispatcher re-checks drains from, at the commit and not before, and
/// the run stops counting as live at once, while its execute task is
/// still claimed (the worker closes it only after the settle). A run
/// whose costs keep its row is stamped ended instead of dropped.
#[sqlx::test]
async fn forgetting_an_unrecorded_run_announces_its_ending_at_the_commit(pool: PgPool) {
    use weft_journal::unrecorded::{UnrecordedEnded, UNRECORDED_ENDED_CHANNEL};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let execution_id = weft_core::ExecutionId::new_v4();
    let start = weft_journal::ExecEvent::ExecutionStarted {
        execution_id, project_id: id, entry_node: "route".into(),
        phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()),
        program: None, source_version: None, run_kind: weft_core::exec::RunKind::Unrecorded,
        subgraph: None, seed: None, instance: None, fired_trigger: Some("route".into()),
        instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        run_class: weft_core::run_class::RunClass::Short,
    };
    let task = execute_task(id, execution_id, "bin-A", Some(std::slice::from_ref(&start)));
    journal.start_execution(&start, &[], task, false).await.unwrap();
    sqlx::query("UPDATE task SET status = 'claimed', claimed_by = 'worker-a', claimed_until_unix = $2 WHERE execution_id = $1")
        .bind(execution_id.to_string()).bind(weft_dispatcher::lease::now_unix() + 60).execute(&pool).await.unwrap();
    weft_journal::record_events(&pool, &[weft_journal::ExecEvent::CostReported {
        execution_id, node_id: "llm".into(), frames: vec![], cost_id: "c".into(), service: "llm".into(),
        model: None, amount_usd: Some(0.1), billed: true, origin: weft_core::CredentialOwner::Author,
        metadata: serde_json::json!({}), at_unix: 2,
    }], None, None).await.unwrap();
    let live = || async {
        journal.list_non_terminal_execution_ids_for_project(id).await.unwrap().into_iter().map(|(c, _)| c).collect::<Vec<_>>()
    };
    assert_eq!(live().await, vec![execution_id]);

    static CHANNELS: &[&str] = &[UNRECORDED_ENDED_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS).await.unwrap();
    let mut heard = watch.subscribe();
    async fn heard_next(heard: &mut weft_task_store::pg_signal::Subscription) -> Option<String> {
        match heard.next().await.unwrap() {
            weft_task_store::pg_signal::Heard::Signal { channel, payload } if channel == UNRECORDED_ENDED_CHANNEL => {
                Some(payload.to_string())
            }
            _ => None,
        }
    }

    let mut tx = pool.begin().await.unwrap();
    assert!(!weft_journal::unrecorded::forget_in(&mut tx, execution_id).await.unwrap(), "its cost keeps the row");
    let before = tokio::time::timeout(std::time::Duration::from_millis(300), heard_next(&mut heard)).await;
    assert!(before.is_err(), "nothing is announced before the commit");
    tx.commit().await.unwrap();
    // What every caller of `forget_in` does once its transaction commits.
    weft_task_store::announce::committed(&pool);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let announced = loop {
        let got = tokio::time::timeout_at(deadline, heard_next(&mut heard)).await.expect("the ending is announced");
        if let Some(payload) = got {
            break serde_json::from_str::<UnrecordedEnded>(&payload).unwrap();
        }
    };
    assert_eq!(announced, UnrecordedEnded { execution_id, project_id: id, fired_by: Some("route".into()), instance: None });
    assert!(live().await.is_empty(), "ended at once, although its task is still claimed");
    let ended: Option<i64> = sqlx::query_scalar("SELECT ended_at_unix FROM execution WHERE execution_id = $1")
        .bind(execution_id.to_string()).fetch_one(&pool).await.unwrap();
    assert!(ended.is_some(), "a row its costs keep is stamped ended");
}

// ----- supervisor lease hygiene -----------------------------------------

/// Project removal releases the project's `infra_owner` lease, and the
/// ghost-lease sweep drops any lease a removal left behind, so a
/// supervisor never renews ownership of a deleted project forever.
#[sqlx::test]
async fn removed_projects_do_not_keep_supervisor_leases(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;

    // A live project with a lease, and a ghost lease for a project that
    // was never (or is no longer) registered.
    let live = Uuid::new_v4();
    seed_project(&projects, live, "hash-live").await;
    for (project, replica) in [(live, "sup-live"), (Uuid::new_v4(), "sup-ghost")] {
        sqlx::query(
            "INSERT INTO infra_owner (project_id, supervisor_replica, tenant_id, leased_until_unix) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(project)
        .bind(replica)
        .bind(TENANT)
        .bind(weft_dispatcher::lease::now_unix() + 60)
        .execute(&pool)
        .await
        .expect("seed infra_owner");
    }

    weft_dispatcher::infra_owner::release_ghost_leases(&pool).await.expect("sweep");
    let leases: Vec<(Uuid,)> = sqlx::query_as("SELECT project_id FROM infra_owner")
        .fetch_all(&pool)
        .await
        .expect("list leases");
    assert_eq!(
        leases,
        vec![(live,)],
        "ghost lease dropped, live project's lease kept"
    );

    let released =
        weft_dispatcher::infra_owner::release_project(&pool, live)
            .await
            .expect("release");
    assert_eq!(released, 1, "project removal releases its lease");
}

/// The referenced-image set (the keep-set image pruning deletes against) must cover a
/// project's current hash, the hash stamped on a pending/claimed task (a
/// run settling on an older image keeps that image until it ends), every infra image ref in any project's tag map,
/// AND the refs recorded on live infra units (a unit left UP across a
/// sync stays frozen at its recorded image, which can be older than the
/// project's current map): `weft clean --images` deletes everything
/// outside this set, so any of them escaping it would be deleted out
/// from under a running workload. Completed tasks' hashes drop out; a project with no infra tags contributes none; a
/// blank ref is skipped (matches no image); a unit stamped before refs
/// were recorded contributes nothing.
#[sqlx::test]
async fn referenced_images_cover_projects_tasks_maps_and_unit_refs(
    pool: PgPool,
) {
    let (_journal, projects) = setup(&pool).await;
    let now = weft_dispatcher::lease::now_unix();
    let plain = Uuid::new_v4();
    seed_project(&projects, plain, "hash-current").await;
    // A second project whose committed infra sync recorded two nodes'
    // image tags; both refs are the supervisor's to apply, so both are
    // referenced no matter which node they belong to. One blank ref is
    // skipped (a buggy writer's blank matches no image and must not be
    // laundered into the set).
    let with_infra = Uuid::new_v4();
    seed_project(&projects, with_infra, "hash-infra-project").await;
    sqlx::query("UPDATE project SET infra_image_tags_json = $1 WHERE id = $2")
        .bind(serde_json::json!({
            "bridge-node": { "bridge": "weft-infra-bridge:live1", "blank": "" },
            "engine-node": { "engine": "weft-infra-engine:live2" },
        }))
        .bind(with_infra)
        .execute(&pool)
        .await
        .expect("seed infra image tags");
    // An infra_node row for that project: one unit UP and frozen at an
    // image OLDER than the map above (the case the map alone cannot
    // cover), one unit stamped before refs were recorded (contributes
    // nothing), one unit long gone terminal.
    sqlx::query(
        "INSERT INTO infra_node \
         (project_id, node_id, copy_id, status, units_json) \
         VALUES ($1, 'n1', 'inst1', 'running', $2)",
    )
    .bind(with_infra)
    .bind(serde_json::json!({
        "frozen": {
            "status": "running",
            "stop_behavior": { "kind": "keep_running" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30,
            "image_refs": ["weft-infra-bridge:0ld-frozen"]
        },
        "legacy": {
            "status": "running",
            "stop_behavior": { "kind": "keep_running" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30
        },
        "terminal": {
            "status": "stopped",
            "stop_behavior": { "kind": "stop" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30,
            "image_refs": ["weft-infra-bridge:terminal-gone"]
        }
    }))
    .execute(&pool)
    .await
    .expect("seed infra_node units");
    for (status, hash) in [
        ("pending", "hash-pending-task"),
        ("claimed", "hash-claimed-task"),
        ("complete", "hash-done-task"),
    ] {
        sqlx::query(
            "INSERT INTO task \
             (id, kind, target, project_id, tenant_id, status, binary_hash, payload, attempts, created_at_unix) \
             VALUES ($4, 'execute', 'worker', gen_random_uuid(), 'tenant', $1, $2, '{}'::jsonb, 0, $3)",
        )
        .bind(status)
        .bind(hash)
        .bind(now)
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .expect("seed task");
    }

    // A run still waiting (a form, a timer) resumes on the image it
    // started on, so its image stays; a finished run's does not.
    for (execution_id, hash, finished) in [("run-waiting", "hash-waiting-run", false), ("run-finished", "hash-finished-run", true)] {
        sqlx::query(
            "INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase) \
             VALUES ($1, gen_random_uuid(), 'tenant', $2, 'fire')",
        )
        .bind(execution_id)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed an execution");
        let started = serde_json::json!({ "program": { "binary_hash": hash, "definition_hash": "d", "implementations": {} } });
        sqlx::query("INSERT INTO exec_event (execution_id, kind, payload_json, created_at) VALUES ($1, 'execution_started', $2, 0)")
            .bind(execution_id)
            .bind(started.to_string())
            .execute(&pool)
            .await
            .expect("seed its start");
        if finished {
            sqlx::query("INSERT INTO exec_event (execution_id, kind, payload_json, created_at) VALUES ($1, 'execution_completed', '{}', 0)")
                .bind(execution_id)
                .execute(&pool)
                .await
                .expect("seed its end");
        }
    }

    let referenced = weft_dispatcher::api::project::referenced_images_query(&mut pool.acquire().await.expect("a connection"), weft_dispatcher::build::prune::ImageScope::All)
        .await
        .expect("referenced set");
    let mut hashes = referenced.worker_hashes.clone();
    hashes.sort();
    assert_eq!(
        hashes,
        vec![
            "hash-claimed-task".to_string(),
            "hash-current".to_string(),
            "hash-infra-project".to_string(),
            "hash-pending-task".to_string(),
            "hash-waiting-run".to_string(),
        ],
        "project + live task hashes in (pending AND claimed) + live runs' images; \
         completed task hashes and finished runs' images out"
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

// ----- parked-fire queue: append classification, retry backoff ---------

/// One parked element with an explicit retry state (the values every
/// backoff decision reads).
fn parked(id: &str, attempts: u32, not_before_unix: i64) -> ParkedFire {
    ParkedFire {
        id: id.to_string(),
        payload: json!({ "v": 1 }),
        received_at_unix: 1_700_000_000,
        attempts,
        not_before_unix,
        instance_gap: None,
    }
}

/// Seed one signal row for `token`. Entry rows are keyed by
/// `(project_id, node_id)`, so the node id is derived from the token:
/// many tokens per project, no collisions.
async fn seed_parked_signal(journal: &PostgresJournal, token: &str, project: Uuid) {
    let mut sig = entry_signal(token, project);
    sig.node_id = format!("trigger-{token}");
    sig.activation_trigger = Some(sig.node_id.clone());
    journal
        .signal_insert(&sig)
        .await
        .expect("seed signal row");
}

/// Consuming a resume token deletes its row and hands the row back, so
/// what follows the DELETE (unregistering its kind) reads the spec it
/// had. An entry row is not consumed.
#[sqlx::test]
async fn consume_suspension_returns_the_deleted_row(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-entry", project).await;
    let mut resume = entry_signal("tok-resume", project);
    resume.is_resume = true;
    journal
        .signal_insert(&resume)
        .await
        .expect("seed resume signal");

    let consumed = journal.consume_suspension("tok-resume").await.unwrap().expect("the resume row");
    assert_eq!(consumed.token, "tok-resume");
    assert!(consumed.is_resume);
    assert!(journal.signal_get("tok-resume").await.unwrap().is_none(), "single use");
    assert!(journal.consume_suspension("tok-resume").await.unwrap().is_none());

    assert!(journal.consume_suspension("tok-entry").await.unwrap().is_none(), "entry rows stay");
    assert!(journal.signal_get("tok-entry").await.unwrap().is_some(), "entry row kept");
}

/// The consumer listing decodes the same row shape as every journal
/// read, so a column added to the row reaches this query too: one
/// visible entry signal lists, with its node (a trigger with no
/// activation row yet is visible).
#[sqlx::test]
async fn the_consumer_listing_reads_the_whole_signal_row(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-entry", project).await;

    let listed = signals_visible_to(&pool, TENANT, &[], &[], None).await.expect("listing decodes");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].token, "tok-entry");
    assert_eq!(listed[0].node_id, "trigger-tok-entry");
    assert!(signals_visible_to(&pool, "someone-else", &[], &[], None).await.unwrap().is_empty());
}

/// Read one token's queue back as parsed JSON.
async fn parked_queue(pool: &PgPool, token: &str) -> serde_json::Value {
    sqlx::query_as::<_, (serde_json::Value,)>("SELECT parked_fires FROM signal WHERE token = $1")
        .bind(token)
        .fetch_one(pool)
        .await
        .expect("read parked_fires")
        .0
}

/// The append names its refusal instead of returning "0 rows": a re-run
/// that finds its own element queued (nothing lost), a resume signal
/// already answered, an entry queue at its cap (a refused NEW fire, a
/// loss the caller must say out loud), and a vanished row (the project
/// was wiped under the fire) are four different facts.
#[sqlx::test]
async fn parked_fire_append_names_its_refusal(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-entry", project).await;
    let mut resume = entry_signal("tok-resume", project);
    resume.is_resume = true;
    journal
        .signal_insert(&resume)
        .await
        .expect("seed resume signal");

    assert_eq!(
        append_parked_fire(&pool, "tok-entry", &parked("f1", 0, 0)).await.unwrap(),
        ParkAppend::Parked
    );
    assert_eq!(
        append_parked_fire(&pool, "tok-entry", &parked("f1", 9, 99)).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::AlreadyQueued),
        "a re-run of a task that already parked this fire finds its element"
    );

    append_parked_fire(&pool, "tok-resume", &parked("r1", 0, 0))
        .await
        .unwrap();
    assert_eq!(
        append_parked_fire(&pool, "tok-resume", &parked("r2", 0, 0)).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered),
        "one submission answers one suspension; a second is a duplicate"
    );

    // Fill the entry queue to its cap in one write, then a NEW fire is
    // refused (a loss, named as such).
    sqlx::query(
        "UPDATE signal SET parked_fires = ( \
             SELECT jsonb_agg(jsonb_build_object('id', 'filler-' || g, 'payload', '{}', \
                                        'received_at_unix', 0) ORDER BY g) \
             FROM generate_series(1, 1000) g) \
         WHERE token = 'tok-entry'",
    )
    .execute(&pool)
    .await
    .expect("fill the queue to the cap");
    assert_eq!(
        append_parked_fire(&pool, "tok-entry", &parked("f2", 0, 0)).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::QueueFull)
    );

    sqlx::query("DELETE FROM signal WHERE token = 'tok-resume'")
        .execute(&pool)
        .await
        .expect("wipe the resume row");
    assert_eq!(
        append_parked_fire(&pool, "tok-resume", &parked("r3", 0, 0)).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::RowGone),
        "a vanished row means the project was wiped under the fire"
    );
}

/// The sweep's selection, against the real statement: only ACTIVE
/// projects, only unclaimed rows, only tokens whose HEAD is due. A
/// backing-off head blocks its whole token (FIFO: a later fire may not
/// overtake it), and an element from before the backoff fields existed
/// reads as due now.
#[sqlx::test]
async fn the_sweep_selects_only_due_heads_on_active_triggers(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let active = Uuid::new_v4();
    let inactive = Uuid::new_v4();
    seed_project(&projects, active, "bin-A").await;
    seed_project(&projects, inactive, "bin-B").await;
    // The due trigger's activation is Active, the other project's is
    // parked; the rest have no activation row, which reads as live.
    let activations = PostgresActivationStore::new(pool.clone());
    for (project, trigger, to) in [
        (active, "trigger-tok-due", ActivationLifecycle::active()),
        (inactive, "trigger-tok-inactive", ActivationLifecycle::parked()),
    ] {
        let setup_execution_id = Uuid::new_v4();
        let key = ActivationKey::new(trigger, Owner::Shared);
        assert!(activations.try_begin_activating(project, &[key], setup_execution_id, None).await.unwrap().is_ok());
        activations.end_activating(project, setup_execution_id, &to, false, None).await.unwrap().expect("owned");
    }

    let now = weft_dispatcher::lease::now_unix();
    seed_parked_signal(&journal, "tok-due", active).await;
    append_parked_fire(&pool, "tok-due", &parked("f-due", 0, now - 10))
        .await
        .unwrap();
    seed_parked_signal(&journal, "tok-later", active).await;
    append_parked_fire(&pool, "tok-later", &parked("f-later", 3, now + 300))
        .await
        .unwrap();
    seed_parked_signal(&journal, "tok-legacy", active).await;
    append_parked_fire(&pool, "tok-legacy", &parked("f-legacy", 0, 0))
        .await
        .unwrap();
    // Strip the backoff fields: the element shape an older dispatcher
    // wrote, which must read as due now.
    sqlx::query(
        "UPDATE signal SET parked_fires = \
         '[{\"id\": \"f-legacy\", \"payload\": {}, \"received_at_unix\": 1}]'::jsonb \
         WHERE token = 'tok-legacy'",
    )
    .execute(&pool)
    .await
    .expect("write a legacy element");
    seed_parked_signal(&journal, "tok-claimed", active).await;
    append_parked_fire(&pool, "tok-claimed", &parked("f-claimed", 0, now - 10))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = $1, drain_claimed_by = 'replica-x' \
         WHERE token = 'tok-claimed'",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("claim the token");
    seed_parked_signal(&journal, "tok-inactive", inactive).await;
    append_parked_fire(&pool, "tok-inactive", &parked("f-inactive", 0, now - 10))
        .await
        .unwrap();

    let selected: std::collections::HashSet<String> =
        due_parked_tokens(&pool, now).await.unwrap().into_iter().map(|(t, _)| t).collect();
    let expected: std::collections::HashSet<String> =
        ["tok-due", "tok-legacy"].into_iter().map(String::from).collect();
    assert_eq!(
        selected,
        expected,
        "due heads on live triggers only; a backing-off head, a claimed row, \
         and a parked trigger's queue are all left alone"
    );
}

/// A fire parked because its instance has not filled a value never comes
/// due on a timer: the sweep and its next-due read pass its head by,
/// whatever its stamp says. The instance's rows are what a change of their
/// values routes again, and `weft status` counts them per trigger with
/// the latest reason.
#[sqlx::test]
async fn fires_waiting_on_an_instance_value_wait_for_the_instance(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-ada", project).await;
    seed_parked_signal(&journal, "tok-timer", project).await;
    sqlx::query("UPDATE signal SET instance_id = 'ada' WHERE token = 'tok-ada'")
        .execute(&pool)
        .await
        .expect("make tok-ada ada's");
    let gap = |id: &str, reason: &str| ParkedFire { instance_gap: Some(reason.to_string()), ..parked(id, 1, 0) };
    append_parked_fire(&pool, "tok-ada", &gap("f1", "instance 'ada' at 'answer': 'key' is not filled")).await.unwrap();
    append_parked_fire(&pool, "tok-ada", &gap("f2", "instance 'ada' at 'answer': 'model' is not filled")).await.unwrap();
    append_parked_fire(&pool, "tok-timer", &parked("f3", 1, 0)).await.unwrap();

    let now = weft_dispatcher::lease::now_unix();
    let due: Vec<String> = due_parked_tokens(&pool, now).await.unwrap().into_iter().map(|(t, _)| t).collect();
    assert_eq!(due, ["tok-timer"], "only the timer-retried head is due");
    let next = weft_dispatcher::api::project::next_parked_fire_due(&pool).await.unwrap();
    assert_eq!(next, Some(0), "the next-due read sees the timer head alone");

    let ada = InstanceId::new("ada").unwrap();
    assert_eq!(instance_gap_tokens(&pool, project, &ada).await.unwrap(), ["tok-ada"]);
    assert!(instance_gap_tokens(&pool, project, &InstanceId::new("bob").unwrap()).await.unwrap().is_empty());

    let waits = instance_waits(&pool, project).await.unwrap();
    let key = ActivationKey::new("trigger-tok-ada", Owner::from_instance(Some(ada)));
    let waiting = waits.get(&key).expect("ada's trigger waits");
    assert_eq!(waiting.fires, 2);
    assert_eq!(waiting.reason, "instance 'ada' at 'answer': 'model' is not filled", "the fire parked last");
    assert_eq!(waits.len(), 1, "a fire retried on its timer is not waiting on an instance");
}

/// Every status reader goes through `infra_node::observe`, which reads
/// the copies with the commands the supervisor has not finished applied:
/// an instance's start queued over its stopped copy reads provisioning at
/// once (not the old `stopped`), a queued stop of a running copy reads
/// stopping, and a start of a copy with no row yet lists it as starting.
#[sqlx::test]
async fn copies_read_with_the_commands_under_way(pool: PgPool) {
    use weft_broker::lifecycle_writes::{issue_command, IssuedCommand};
    use weft_dispatcher::infra_lifecycle_command::{issue_lifecycle, InfraLifecycleVerb, RunningPolicy, TakeDown};
    use weft_dispatcher::infra_node::{observe, InfraNodeStatus};
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    for (node, instance, status) in [("bridge", Some("ada"), "stopped"), ("db", None, "running"), ("cache", None, "running")] {
        sqlx::query(
            "INSERT INTO infra_node (project_id, node_id, instance_id, copy_id, status) \
             VALUES ($1, $2, $3, 'inst', $4)",
        )
        .bind(project)
        .bind(node)
        .bind(instance)
        .bind(status)
        .execute(&pool)
        .await
        .expect("seed a copy");
    }
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    issue_lifecycle(&pool, TENANT, project, Some("db"), &weft_core::instance::Copies::Shared, TakeDown::Stop { force: false }, RunningPolicy::Cancel, 60, "disp-1")
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
            running_policy: None,
            spec_json: Some(&spec),
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

/// What holds the program's own verbs back is infra work on the copies
/// the bar starts and stops: an instance's copy coming up holds nothing
/// back, the shared copies' work does, and so does the project going.
#[sqlx::test]
async fn only_work_on_the_shared_copies_holds_the_program_back(pool: PgPool) {
    use weft_dispatcher::infra_lifecycle_command::{any_in_flight, issue_lifecycle, RunningPolicy, TakeDown};
    use weft_core::instance::Copies;
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let stop = |copies: Copies| {
        let pool = pool.clone();
        async move {
            issue_lifecycle(&pool, TENANT, project, Some("db"), &copies, TakeDown::Stop { force: false }, RunningPolicy::Cancel, 60, "disp-1")
                .await
                .unwrap()
        }
    };
    let finish = |id: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query("UPDATE infra_lifecycle_command SET completed_at_unix = 1 WHERE id = $1").bind(id).execute(&pool).await.unwrap();
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
#[sqlx::test]
async fn a_failed_activation_puts_the_triggers_back_as_the_claim_found_them(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let fresh = ActivationKey::new("fresh", Owner::Shared);
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None).await.unwrap().expect("claim");
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    activations.set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept).await.unwrap();

    let second = Uuid::new_v4();
    let previous = activations
        .try_begin_activating(id, &[feed_key(), fresh.clone()], second, None)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(previous.len(), 1, "only the trigger activated before had a row");
    assert_eq!(previous[0].key, feed_key());
    assert_eq!(previous[0].lifecycle.mode().as_str(), "park");
    let during: Vec<_> = activations.list(id).await.unwrap();
    assert!(during.iter().all(|a| a.lifecycle.status == ProjectStatus::Activating), "both read activating at once");

    assert!(activations.restore(id, second, &previous).await.unwrap());
    let after = activations.list(id).await.unwrap();
    assert_eq!(after.len(), 1, "the never-activated trigger has no row again");
    assert_eq!(after[0].key, feed_key());
    assert_eq!(after[0].lifecycle.mode().as_str(), "park");
    assert!(!activations.restore(id, second, &previous).await.unwrap(), "a second restore finds no claim");
}

/// Claims older than the threshold release (both columns); a fresh
/// claim survives. The sweep depends on this: it is the only thing that
/// re-drives an Active project's queue, so a crashed process's stale claim
/// must not starve the token's retries until the next activate.
#[sqlx::test]
async fn stale_drain_claims_release_and_fresh_ones_survive(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-stale", project).await;
    seed_parked_signal(&journal, "tok-fresh", project).await;
    let now = weft_dispatcher::lease::now_unix();
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = $1, drain_claimed_by = 'replica-x' \
         WHERE token = 'tok-stale'",
    )
    .bind(now - 301)
    .execute(&pool)
    .await
    .expect("seed a stale claim");
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = $1, drain_claimed_by = 'replica-y' \
         WHERE token = 'tok-fresh'",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed a fresh claim");

    release_stale_drain_claims(&pool).await.expect("release pass");

    let stale: (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT drain_claimed_at_unix, drain_claimed_by FROM signal WHERE token = 'tok-stale'",
    )
    .fetch_one(&pool)
    .await
    .expect("stale row");
    assert_eq!(stale, (None, None), "the stale claim must be gone, both columns");
    let fresh: (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT drain_claimed_at_unix, drain_claimed_by FROM signal WHERE token = 'tok-fresh'",
    )
    .fetch_one(&pool)
    .await
    .expect("fresh row");
    assert_eq!(
        fresh,
        (Some(now), Some("replica-y".to_string())),
        "a live claim must survive the release"
    );
}

/// A dispatch failure re-stamps its head IN PLACE: attempt count up, due
/// time out, position kept (a pop-and-re-append would reorder one
/// trigger's events), the rest of the queue untouched, and the write
/// fenced on the drain's claim nonce.
#[sqlx::test]
async fn a_failed_dispatch_restamps_its_head_in_place(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok", project).await;
    append_parked_fire(&pool, "tok", &parked("f1", 2, 1)).await.unwrap();
    append_parked_fire(&pool, "tok", &parked("f2", 0, 0)).await.unwrap();
    sqlx::query("UPDATE signal SET drain_claimed_by = 'drain-owner' WHERE token = 'tok'")
        .execute(&pool)
        .await
        .expect("hold the drain claim");

    let now = weft_dispatcher::lease::now_unix();
    let rows = restamp_parked_fire(&pool, "tok", "f1", 3, now + 4, Some("drain-owner"))
        .await
        .unwrap();
    assert_eq!(rows, 1, "our own claim restamps the element");

    let queue = parked_queue(&pool, "tok").await;
    let elements = queue.as_array().expect("queue is an array");
    assert_eq!(elements.len(), 2, "no element may be added or lost");
    assert_eq!(elements[0]["id"], json!("f1"), "the failed fire stays the head");
    assert_eq!(elements[0]["attempts"], json!(3), "the attempt count moves up");
    assert_eq!(elements[0]["not_before_unix"], json!(now + 4), "the due time moves out");
    assert_eq!(
        (
            elements[1]["id"].clone(),
            elements[1]["attempts"].clone(),
            elements[1]["not_before_unix"].clone()
        ),
        (json!("f2"), json!(0), json!(0)),
        "the element behind the head is untouched"
    );

    // Someone else's claim, and an element that is not queued: both are
    // no-ops, and neither may disturb the array.
    assert_eq!(
        restamp_parked_fire(&pool, "tok", "f1", 4, now + 8, Some("someone-else"))
            .await
            .unwrap(),
        0,
        "a fenced restamp on another owner's claim writes nothing"
    );
    assert_eq!(
        restamp_parked_fire(&pool, "tok", "nope", 1, now + 1, Some("drain-owner"))
            .await
            .unwrap(),
        0,
        "restamping an element that is not queued writes nothing"
    );
    assert_eq!(
        parked_queue(&pool, "tok").await,
        queue,
        "both refused restamps left the queue exactly as it was"
    );
}

/// Journal a fresh execution for `project`, the way production does
/// (the `execution` index row rides the same transaction).
async fn start_execution(journal: &PostgresJournal, project: Uuid) -> weft_core::ExecutionId {
    let execution_id = weft_core::ExecutionId::new_v4();
    journal
        .record_event(&weft_journal::ExecEvent::ExecutionStarted {
            execution_id,
            project_id: project,
            entry_node: "start".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("def-1".into()),
            program: None,
            run_kind: weft_core::exec::RunKind::Execution,
            source_version: None,
            subgraph: None,
            seed: None,
            instance: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
            run_class: weft_core::run_class::RunClass::Short,
        })
        .await
        .expect("ExecutionStarted");
    execution_id
}

async fn count(pool: &PgPool, sql: &str, execution_id: weft_core::ExecutionId) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .bind(execution_id.to_string())
        .fetch_one(pool)
        .await
        .expect("count")
}

/// Everywhere one execution lives, so a table dropped from the erase
/// list cannot ship quietly.
async fn footprint(pool: &PgPool, execution_id: weft_core::ExecutionId) -> i64 {
    count(pool, "SELECT COUNT(*) FROM exec_event WHERE execution_id = $1", execution_id).await
        + count(pool, "SELECT COUNT(*) FROM execution WHERE execution_id = $1", execution_id).await
        + count(pool, "SELECT COUNT(*) FROM execution_tag WHERE execution_id = $1", execution_id).await
        + count(pool, "SELECT COUNT(*) FROM trigger_setup WHERE execution_id = $1", execution_id).await
        + count(pool, "SELECT COUNT(*) FROM signal WHERE execution_id = $1 AND is_resume = TRUE", execution_id)
            .await
}

/// Removing a project frees the space its history took: every table an
/// execution touches loses its rows, for every execution of that
/// project and no other's.
///
/// This is the test the erase list needs, because a table left out of
/// it fails silently. The rows are simply still there, on a path
/// nobody runs twice, reachable by nothing.
///
/// The entry signal is the control: an execution's erase takes its
/// RESUME tokens (they only mean anything inside that run) and leaves
/// the project's registered entry points alone, which removal deals
/// with separately.
#[sqlx::test]
async fn removing_a_projects_executions_frees_every_table_they_touched(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let doomed = Uuid::new_v4();
    let neighbour = Uuid::new_v4();
    seed_project(&projects, doomed, "bin-A").await;
    seed_project(&projects, neighbour, "bin-A").await;

    let first = start_execution(&journal, doomed).await;
    let second = start_execution(&journal, doomed).await;
    let survivor = start_execution(&journal, neighbour).await;

    // Everything else a run leaves behind, on the first execution.
    let mut tx = pool.begin().await.unwrap();
    weft_journal::tags::tag_execution_in(&mut tx, first, &["user_7".to_string()], 10, None)
        .await
        .expect("tag");
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO trigger_setup (project_id, execution_id) VALUES ($1, $2)")
        .bind(doomed)
        .bind(first.to_string())
        .execute(&pool)
        .await
        .expect("trigger_setup");
    for (token, execution_id, is_resume) in [
        ("resume-tok", Some(first), true),
        ("entry-tok", None, false),
    ] {
        sqlx::query(
            "INSERT INTO signal \
             (token, tenant_id, project_id, execution_id, node_id, is_resume, spec_json, created_at) \
             VALUES ($1, $2, $3, $4, 'wait', $5, '{}', 1)",
        )
        .bind(token)
        .bind(TENANT)
        .bind(doomed)
        .bind(execution_id.map(|c| c.to_string()))
        .bind(is_resume)
        .execute(&pool)
        .await
        .expect("signal");
    }
    assert!(footprint(&pool, first).await > 0, "the run left something behind to erase");

    let erased = journal
        .delete_project_executions(doomed)
        .await
        .expect("erase the project's executions");
    assert_eq!(erased, 2, "both of the project's executions");

    assert_eq!(footprint(&pool, first).await, 0, "nothing of the first run is left");
    assert_eq!(footprint(&pool, second).await, 0, "nothing of the second run is left");
    assert!(footprint(&pool, survivor).await > 0, "the neighbour project is untouched");
    let entry: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM signal WHERE token = 'entry-tok'")
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(entry, 1, "an entry signal is not an execution's to take");
}

/// The erase at removal is best-effort, because the project row is
/// already gone by then and failing the answer would say the removal
/// did not happen when it did. This is the retry path: executions
/// whose project no longer exists are findable, and stop being
/// findable once they are erased.
#[sqlx::test]
async fn executions_of_a_gone_project_can_be_found_again(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let gone = Uuid::new_v4();
    let living = Uuid::new_v4();
    seed_project(&projects, gone, "bin-A").await;
    seed_project(&projects, living, "bin-A").await;
    let orphan = start_execution(&journal, gone).await;
    start_execution(&journal, living).await;

    // While both projects exist there is nothing to sweep.
    assert!(
        journal.projects_with_orphan_executions().await.expect("sweep").is_empty(),
        "an execution whose project is still there is not an orphan"
    );

    projects.remove(gone).await.expect("remove the project row");
    let orphaned = journal.projects_with_orphan_executions().await.expect("sweep");
    assert_eq!(orphaned, vec![gone], "the gone project's executions are findable");

    journal.delete_project_executions(gone).await.expect("erase");
    assert_eq!(footprint(&pool, orphan).await, 0);
    assert!(
        journal.projects_with_orphan_executions().await.expect("sweep again").is_empty(),
        "once erased there is nothing left to come back for"
    );
}

/// A removed project's queued work is cleared; a live project's never is.
#[sqlx::test]
async fn a_removed_projects_work_is_cleared_and_nothing_else(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let live = Uuid::from_u128(1);
    let removed = Uuid::from_u128(2);
    seed_project(&projects, live, "bin").await;
    let queue = |project: Uuid, key: &'static str| {
        let pool = pool.clone();
        async move {
            weft_task_store::tasks::enqueue_dedup(
                &pool,
                weft_task_store::tasks::NewTask {
                    kind: "execute".into(),
                    target: weft_task_store::TaskTarget::Worker,
                    project_id: Some(project),
                    dedup_key: Some(key.into()),
                    execution_id: None,
                    tenant_id: TENANT.into(),
                    target_replica: None,
                    binary_hash: Some("bin".into()),
                    payload: json!({}),
                },
            )
            .await
            .unwrap();
        }
    };
    queue(live, "a").await;
    queue(removed, "b").await;
    // Work of the removed project the sweep must leave alone: a worker
    // task already claimed (its worker finishes or the orphan sweep
    // recovers it), and a pending task for the dispatcher.
    queue(removed, "claimed").await;
    sqlx::query("UPDATE task SET status = 'claimed' WHERE dedup_key = 'claimed'")
        .execute(&pool)
        .await
        .unwrap();
    weft_task_store::tasks::enqueue_dedup(
        &pool,
        weft_task_store::tasks::NewTask {
            kind: "execute".into(),
            target: weft_task_store::TaskTarget::Dispatcher,
            project_id: Some(removed),
            dedup_key: Some("dispatcher".into()),
            execution_id: None,
            tenant_id: TENANT.into(),
            target_replica: None,
            binary_hash: None,
            payload: json!({}),
        },
    )
    .await
    .unwrap();
    assert_eq!(weft_dispatcher::reaper::drop_work_of_removed_projects(&pool).await.unwrap(), 1);
    let left: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT project_id, dedup_key FROM task ORDER BY dedup_key").fetch_all(&pool).await.unwrap();
    assert_eq!(
        left,
        vec![(live, "a".to_string()), (removed, "claimed".to_string()), (removed, "dispatcher".to_string())]
    );
}

/// Removing a project takes its instances' connections, values and tokens:
/// each acts only in this project, and nobody could list them to delete
/// them once it is gone. The author's own connections stay theirs.
#[sqlx::test]
async fn removing_a_project_takes_what_its_instances_had(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let grant = |instance: Option<&'static str>| {
        let pool = pool.clone();
        async move {
            let grant = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO access_grant (id, tenant_id, service, spec_json, values_sealed, project_id, instance_id) \
                 VALUES ($1, $2, 'svc', '{}', '', $3, $4)",
            )
            .bind(grant).bind(TENANT).bind(id).bind(instance)
            .execute(&pool).await.expect("grant");
            grant
        }
    };
    let adas = grant(Some("ada")).await;
    let authors = grant(None).await;
    sqlx::query(
        "INSERT INTO instance_value (tenant_id, project_id, instance_id, step, field, value, grant_id) \
         VALUES ($1, $2, 'ada', 'post', 'account', '{}', $3), ($1, $2, 'ada', 'digest', 'cron', '\"0 0 3 * * *\"', NULL)",
    )
    .bind(TENANT).bind(id).bind(adas).execute(&pool).await.expect("values");
    let token = |hash: &str, instance: Option<InstanceId>, expires_at: Option<u64>| weft_dispatcher::journal::SignalToken {
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
    journal.mint_signal_token(&token("ada-token", Some(InstanceId::new("ada").unwrap()), Some(10))).await.unwrap();
    journal.mint_signal_token(&token("author-token", None, None)).await.unwrap();

    projects.remove(id).await.unwrap();
    let grants: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM access_grant").fetch_all(&pool).await.unwrap();
    assert_eq!(grants, vec![authors], "the author's connection stays theirs");
    let values: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM instance_value").fetch_one(&pool).await.unwrap();
    assert_eq!(values, 0, "an instance's values go with the project, connections or not");
    assert!(journal.get_signal_token("ada-token").await.unwrap().is_none());
    assert!(journal.get_signal_token("author-token").await.unwrap().is_some(), "the author revokes their own tokens");
}

/// Taking a whole project down removes every signal it has, entry
/// signals included (their execution is NULL), and keeps only the waits of
/// the run it is told to spare.
#[sqlx::test]
async fn a_whole_project_take_down_removes_its_entry_signals(pool: PgPool) {
    use weft_dispatcher::journal::postgres::remove_project_signals_except;
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let spared = weft_core::ExecutionId::new_v4();
    let mut wait = entry_signal("spared-wait", id);
    wait.is_resume = true;
    wait.execution_id = Some(spared);
    wait.node_id = "ask".into();
    journal.signal_insert(&entry_signal("entry", id)).await.unwrap();
    journal.signal_insert(&wait).await.unwrap();

    let removed = remove_project_signals_except(&pool, id, Some(spared)).await.unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["entry"]);
    let removed = remove_project_signals_except(&pool, id, None).await.unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["spared-wait"]);
}

/// A signal whose project row is gone is swept, and a live project's is
/// left alone.
#[sqlx::test]
async fn the_signals_of_a_removed_project_are_swept(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let live = Uuid::new_v4();
    let gone = Uuid::new_v4();
    seed_project(&projects, live, "bin-A").await;
    seed_project(&projects, gone, "bin-A").await;
    journal.signal_insert(&entry_signal("live-entry", live)).await.unwrap();
    journal.signal_insert(&entry_signal("gone-entry", gone)).await.unwrap();
    sqlx::query("DELETE FROM project WHERE id = $1").bind(gone).execute(&pool).await.unwrap();

    let swept = weft_dispatcher::journal::postgres::remove_signals_of_removed_projects(&pool).await.unwrap();
    assert_eq!(swept.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["gone-entry"]);
    assert!(journal.signal_get("live-entry").await.unwrap().is_some());
}

/// A per-instance trigger has one entry per instance at the same place, and
/// each copy reads its own: an instance's display never shows another
/// instance's registration, and the shared read sees none of them.
#[sqlx::test]
async fn an_entry_is_read_for_its_own_instance(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    for (token, instance) in [("tok-a", "alice"), ("tok-b", "bob")] {
        let signal = SignalRegistration {
            instance: Some(weft_core::instance::InstanceId::new(instance).unwrap()),
            ..entry_signal(token, id)
        };
        journal.signal_insert(&signal).await.expect("insert an instance's entry");
    }
    for (token, instance) in [("tok-a", "alice"), ("tok-b", "bob")] {
        let instance = weft_core::instance::InstanceId::new(instance).unwrap();
        let entry = journal.signal_entry_at(id, "feed", Some(&instance)).await.unwrap().expect("its entry");
        assert_eq!(entry.token, token);
    }
    assert!(journal.signal_entry_at(id, "feed", None).await.unwrap().is_none(), "no shared copy exists");
}

async fn seed_dispatcher_command(pool: &PgPool, project_id: Uuid, verb: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO infra_lifecycle_command (tenant_id, project_id, verb, issued_by_replica, issued_at_unix) \
         VALUES ($1, $2, $3, 'test-instance', $4) RETURNING id",
    )
    .bind(TENANT)
    .bind(project_id)
    .bind(verb)
    .bind(weft_dispatcher::lease::now_unix())
    .fetch_one(pool)
    .await
    .expect("seed a dispatcher command")
}

async fn complete_command(pool: &PgPool, id: i64) {
    sqlx::query("UPDATE infra_lifecycle_command SET completed_at_unix = $1 WHERE id = $2")
        .bind(weft_dispatcher::lease::now_unix())
        .bind(id)
        .execute(pool)
        .await
        .expect("complete command");
}

/// A project's health verbs are claimed one at a time, in issue order: a
/// recovery is not claimed while the park before it is still running (it
/// could land last and leave the triggers down), while an upgrade of the
/// same project is neither held behind them nor holds them.
#[sqlx::test]
async fn health_verbs_of_a_project_are_claimed_in_order(pool: PgPool) {
    use weft_dispatcher::infra_lifecycle_command::InfraLifecycleVerb;
    use weft_dispatcher::lifecycle_claimer::claim_one;
    let (_journal, projects) = setup(&pool).await;
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

/// One upgrade of the same copies at a time: a second is refused naming the
/// one in flight, another owner's goes ahead, and once the first ends a new
/// one is issued.
#[sqlx::test]
async fn a_second_upgrade_of_the_same_copies_is_refused(pool: PgPool) {
    use weft_dispatcher::infra_lifecycle_command::{issue_upgrade, RunningPolicy, UpgradeIssued, UpgradeWork};
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let work = UpgradeWork {
        nodes: Vec::new(),
        running_policy: RunningPolicy::Cancel,
        drain_timeout_secs: 60,
        trigger_deactivation: None,
        binary_hash: None,
        definition_hash: None,
        infra_hash: None,
        stopped: false,
    };
    let ada = InstanceId::new("ada").unwrap();
    let issue = |instance: Option<&InstanceId>| {
        let (pool, work, instance) = (pool.clone(), work.clone(), instance.cloned());
        async move { issue_upgrade(&pool, TENANT, project, instance.as_ref(), &work, "disp-1").await.unwrap() }
    };

    let UpgradeIssued::Issued(first) = issue(None).await else { panic!("the first upgrade is issued") };
    assert_eq!(issue(None).await, UpgradeIssued::AlreadyInFlight(first));
    assert!(matches!(issue(Some(&ada)).await, UpgradeIssued::Issued(_)), "an instance's copies are other copies");

    complete_command(&pool, first).await;
    assert!(matches!(issue(None).await, UpgradeIssued::Issued(id) if id != first));
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
#[sqlx::test]
async fn a_rewritten_held_row_lets_its_holder_go(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    journal.signal_insert(&held_signal("sse", id, 0)).await.unwrap();
    sqlx::query("UPDATE signal SET held_by = 'h1', held_until = 9999999999, serving = '{\"status\":\"up\"}' WHERE token = 'sse'")
        .execute(&pool)
        .await
        .unwrap();
    journal.signal_insert(&held_signal("sse", id, 1)).await.unwrap();
    let (held_by, serving, holds): (Option<String>, Option<serde_json::Value>, bool) =
        sqlx::query_as("SELECT held_by, serving, holds FROM signal WHERE token = 'sse'").fetch_one(&pool).await.unwrap();
    assert_eq!((held_by, serving, holds), (None, None, true));
}

/// The holders are counted from the held rows a listener holds: a parked
/// activation's and a signal that holds nothing do not count.
#[sqlx::test]
async fn the_holders_count_the_held_rows_of_live_activations(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 0);
    journal.signal_insert(&held_signal("one", id, 0)).await.unwrap();
    journal.signal_insert(&held_signal("two", id, 0)).await.unwrap();
    journal.signal_insert(&entry_at("form", id)).await.unwrap();
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 2);
    sqlx::query(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at) \
         VALUES ($1, 'wiped', 'inactive', FALSE, FALSE, 0)",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE signal SET activation_trigger = 'wiped' WHERE token = 'two'").execute(&pool).await.unwrap();
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 1, "a wiped activation's row holds nothing");
    sqlx::query("UPDATE trigger_activation SET accepting_fires = TRUE, fires_visible_to_consumers = TRUE WHERE trigger = 'wiped'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 2, "a parked one's goes on holding: what it hears waits");
}

/// The holders run as many copies as the held signals need, none for none,
/// and look again only when the count is announced to have changed.
#[sqlx::test]
async fn the_holders_are_sized_to_the_held_signals(pool: PgPool) {
    use weft_task_store::drain::DrainStep;
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let holders = weft_platform_traits::FakeHolderPool::default();
    assert_eq!(weft_dispatcher::holders::size(&pool, &holders, 2).await.unwrap(), DrainStep::Done);
    for t in ["a", "b", "c"] {
        journal.signal_insert(&held_signal(t, id, 0)).await.unwrap();
    }
    assert_eq!(weft_dispatcher::holders::size(&pool, &holders, 2).await.unwrap(), DrainStep::Done);
    journal.signal_remove_many(&["a".to_string(), "b".to_string(), "c".to_string()]).await.unwrap();
    weft_dispatcher::holders::size(&pool, &holders, 2).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![0, 2, 0]);
}

/// A held row coming or going announces itself at its commit, which is
/// what wakes the holder sizing; a row that holds nothing stays silent.
#[sqlx::test]
async fn a_held_row_coming_or_going_wakes_the_holder_sizing(pool: PgPool) {
    use weft_dispatcher::holders::HELD_SIGNALS_CHANNEL;
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    static CHANNELS: &[&str] = &[HELD_SIGNALS_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS).await.unwrap();
    let mut heard = watch.subscribe();
    async fn woken(heard: &mut weft_task_store::pg_signal::Subscription) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        heard.woken_before(deadline, |c, _| c == HELD_SIGNALS_CHANNEL).await.unwrap()
    }
    journal.signal_insert(&entry_at("form", id)).await.unwrap();
    assert!(!woken(&mut heard).await, "a signal that holds nothing is silent");
    journal.signal_insert(&held_signal("sse", id, 0)).await.unwrap();
    assert!(woken(&mut heard).await, "a held one coming");
    journal.signal_remove_many(&["sse".to_string()]).await.unwrap();
    assert!(woken(&mut heard).await, "and going");
    sqlx::query(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at) \
         VALUES ($1, 'parked', 'active', TRUE, TRUE, 0)",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(woken(&mut heard).await, "an activation coming may change which rows are held");
    sqlx::query("UPDATE trigger_activation SET status = 'inactive' WHERE project_id = $1 AND trigger = 'parked'").bind(id).execute(&pool).await.unwrap();
    assert!(woken(&mut heard).await, "an activation parked changes which rows are held");
    sqlx::query("DELETE FROM trigger_activation WHERE project_id = $1 AND trigger = 'parked'").bind(id).execute(&pool).await.unwrap();
    assert!(woken(&mut heard).await, "and one forgotten too");
}

/// The rows a dispatcher holds in memory (`weft_dispatcher::held`) say
/// when they change, naming whose: a tenant's public entries and the
/// activations that arm them, a project's worker levers, a project's infra
/// copies. A signal that is no public entry is silent: a run waiting on a
/// reply writes one, and a tenant's routes are not read again for it.
#[sqlx::test]
async fn the_held_rows_announce_their_changes(pool: PgPool) {
    use weft_dispatcher::held::{INFRA_STATUS_CHANNEL, ROUTES_CHANNEL, WORKER_SETTINGS_CHANNEL};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    static CHANNELS: &[&str] = &[ROUTES_CHANNEL, WORKER_SETTINGS_CHANNEL, INFRA_STATUS_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS).await.unwrap();
    let mut heard = watch.subscribe();
    async fn heard_on(heard: &mut weft_task_store::pg_signal::Subscription, channel: &'static str, key: &str) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        heard.woken_before(deadline, |c, p| c == channel && p == key).await.unwrap()
    }
    let project = id.to_string();

    journal.signal_insert(&entry_at("callback", id)).await.unwrap();
    assert!(!heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a signal that is no public entry is silent");
    let route = SignalRegistration {
        surface_kind: "public_entry".into(),
        mount_path: Some(format!("/{TENANT}/notes")),
        ..entry_at("route", id)
    };
    journal.signal_insert(&route).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a public entry coming names its tenant");
    sqlx::query(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at) \
         VALUES ($1, 'route', 'active', TRUE, TRUE, 0)",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "an activation coming");
    sqlx::query("UPDATE trigger_activation SET status = 'inactive' WHERE project_id = $1").bind(id).execute(&pool).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "an activation taken down");
    journal.signal_remove_many(&["route".to_string()]).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a public entry going");

    sqlx::query("UPDATE project SET worker_settings_json = '{\"minInstances\": 1}'::jsonb WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(heard_on(&mut heard, WORKER_SETTINGS_CHANNEL, &project).await, "a project's worker levers changing name it");

    sqlx::query("INSERT INTO infra_node (project_id, node_id, status) VALUES ($1, 'db', 'provisioning')")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "an infra copy coming names its project");
    sqlx::query("UPDATE infra_node SET status = 'running' WHERE project_id = $1").bind(id).execute(&pool).await.unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and its status changing");
    // A worker keeps where each piece answers, so a move is news too.
    sqlx::query("UPDATE infra_node SET endpoints_json = '{\"main\": \"db:5432\"}'::jsonb WHERE project_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and where it answers changing");
    // A program registered again may declare other infra.
    sqlx::query("UPDATE project SET project_json = project_json || ' ' WHERE id = $1").bind(id).execute(&pool).await.unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and the infra it declares changing");
}

/// A live run's birth, as a caller's handshake makes it: its
/// `ExecutionStarted` and its execute task, waiting for the caller until
/// 1 000.
fn live_birth(project: Uuid, execution_id: weft_core::ExecutionId) -> (weft_journal::ExecEvent, weft_task_store::tasks::NewTask) {
    let start = weft_journal::ExecEvent::ExecutionStarted {
        execution_id,
        project_id: project,
        entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        program: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
        subgraph: None,
        seed: None,
        instance: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
        run_class: weft_core::run_class::RunClass::Short,
    };
    let task = weft_dispatcher::task_kinds::execute::execution_task_spec(weft_dispatcher::task_kinds::execute::ExecutionTask {
        kind: weft_task_store::TaskKind::Execute,
        project_id: project,
        execution_id,
        definition_hash: "def-1",
        binary_hash: "bin-A",
        tenant_id: TENANT,
        run_class: weft_core::run_class::RunClass::Short,
        live_connection: Some(weft_task_store::kinds::LiveConnectionStart {
            spec: weft_core::primitive::SignalSpec::of_kind("route", json!({})),
            request: Default::default(),
            arrive_by: Some(1_000),
            fired: None,
        }),
        unrecorded_birth: None,
    })
    .unwrap();
    (start, task)
}

/// A live call's admission at the entry `tok`, which takes `at_once` runs
/// at once, taking a slot for `execution_id` that holds until 1 000.
fn live_admission(at_once: u32, execution_id: weft_core::ExecutionId) -> weft_dispatcher::entry_limits::Admission {
    let limits = weft_core::signal::EntryLimits { per_caller_per_minute: Some(0), per_minute: Some(0), at_once: Some(at_once) }.resolve();
    let edge = weft_dispatcher::entry_limits::EdgeConfig {
        trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 1, outside: 1, domains: 2 },
        invalid_tokens_per_minute: None,
    };
    weft_dispatcher::entry_limits::Admission::call(&edge, None, "tok", "ip:a", &limits, Some((&execution_id.to_string(), 1_000)), 0)
}

/// A live call its entry's limits refuse is never born: the refusal and
/// the birth are one call to the database, so nothing of the run (journal,
/// execution, task, slot) is written, and the refusal is counted. A run
/// already born is left as it is when its birth is asked for again.
#[sqlx::test]
async fn a_live_call_refused_at_its_limit_is_never_born(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let first = weft_core::ExecutionId::new_v4();
    let (start, task) = live_birth(project, first);
    journal.admit_and_start_execution(&live_admission(1, first), &start, &[], task.clone()).await.unwrap().expect("room for one");
    journal
        .admit_and_start_execution(&live_admission(1, first), &start, &[], task)
        .await
        .unwrap()
        .expect("a retry of a run already born collapses onto it, slot and all");

    let second = weft_core::ExecutionId::new_v4();
    let (start, task) = live_birth(project, second);
    let refused = journal.admit_and_start_execution(&live_admission(1, second), &start, &[], task).await.unwrap().unwrap_err();
    assert_eq!(refused.reason, weft_dispatcher::entry_limits::Limited::AtOnce);
    for table in ["exec_event", "execution", "task", "entry_slot"] {
        let (n,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*)::bigint FROM {table} WHERE execution_id = $1"))
            .bind(second.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 0, "{table} holds a run its entry refused");
    }
    assert_eq!(
        weft_dispatcher::entry_limits::recent_refusals(&pool, "tok", 0).await.unwrap(),
        vec![(weft_dispatcher::entry_limits::Limited::AtOnce, 1)]
    );
}

/// A live run born at its caller's handshake whose caller never came is
/// erased whole once their ticket expires: its journal, its execution, its
/// task and its entry slot, so nothing of it is left. One the caller did
/// reach is never touched.
#[sqlx::test]
async fn a_live_run_whose_caller_never_came_leaves_nothing(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let born = |execution_id: weft_core::ExecutionId| live_birth(project, execution_id);
    let count = |table: &'static str, execution_id: weft_core::ExecutionId| {
        let pool = pool.clone();
        async move {
            let (n,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*)::bigint FROM {table} WHERE execution_id = $1"))
                .bind(execution_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
            n
        }
    };

    // Born as a live call is: admitted at its entry (a slot taken for it)
    // in the same commit as its birth.
    let admitted = |execution_id: weft_core::ExecutionId| live_admission(5, execution_id);

    let absent = weft_core::ExecutionId::new_v4();
    let (start, task) = born(absent);
    journal.admit_and_start_execution(&admitted(absent), &start, &[], task).await.unwrap().expect("admitted");
    assert_eq!(count("entry_slot", absent).await, 1, "the slot is taken with the birth");

    let present = weft_core::ExecutionId::new_v4();
    let (start, task) = born(present);
    journal.start_execution(&start, &[], task, false).await.unwrap();
    let claimed = weft_task_store::tasks::claim_execution(&pool, "worker-a", project, &present.to_string())
        .await
        .unwrap()
        .expect("the caller arrived")
        .task;

    const PAST: weft_task_store::tasks::UnclaimedLiveRun = weft_task_store::tasks::UnclaimedLiveRun::PastDeadline { now: 1_001 };
    let gone = weft_task_store::tasks::callers_never_arrived(&pool, 1_001).await.unwrap();
    assert_eq!(gone.len(), 1, "only the run nobody claimed");
    assert_eq!(gone[0].execution_id, absent.to_string());
    assert!(journal.erase_unclaimed_live_run(absent, PAST).await.unwrap());
    for table in ["exec_event", "execution", "task", "entry_slot"] {
        assert_eq!(count(table, absent).await, 0, "{table} still holds the run nobody came for");
    }
    assert!(!journal.erase_unclaimed_live_run(present, PAST).await.unwrap(), "a claimed run is the caller's");
    assert!(weft_task_store::tasks::requeue(&pool, claimed.id, "worker-a").await.unwrap());
    assert!(
        !journal.erase_unclaimed_live_run(present, PAST).await.unwrap(),
        "nor one put back pending: it is pinned to the worker its caller reached"
    );
    assert_eq!(count("execution", present).await, 1);
    assert!(weft_task_store::tasks::callers_never_arrived(&pool, 1_001).await.unwrap().is_empty());

    // A run its handshake could not pass to any worker goes at once,
    // long before its deadline, slot and all, so the caller's retry finds
    // the route's slot free; the run a caller did reach is never taken.
    let unreached = weft_core::ExecutionId::new_v4();
    let (start, task) = born(unreached);
    journal.admit_and_start_execution(&admitted(unreached), &start, &[], task).await.unwrap().expect("admitted");
    const UNREACHED: weft_task_store::tasks::UnclaimedLiveRun = weft_task_store::tasks::UnclaimedLiveRun::NeverPassedOn;
    assert!(!journal.erase_unclaimed_live_run(unreached, weft_task_store::tasks::UnclaimedLiveRun::PastDeadline { now: 999 }).await.unwrap(), "its deadline has not passed");
    assert!(journal.erase_unclaimed_live_run(unreached, UNREACHED).await.unwrap());
    for table in ["exec_event", "execution", "task", "entry_slot"] {
        assert_eq!(count(table, unreached).await, 0, "{table} still holds the run no worker got");
    }
    assert!(!journal.erase_unclaimed_live_run(present, UNREACHED).await.unwrap(), "a claimed run is the caller's");
}
