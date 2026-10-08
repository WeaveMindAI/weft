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
use weft_dispatcher::api::project::owners_with_triggers_on;
use weft_dispatcher::api::signal::{instance_gap_tokens, instance_waits, signals_visible_to};
use weft_dispatcher::parked_drain::{due_tokens, next_due};
use weft_journal::record::{Queued, Then};
use weft_journal::ExecEvent;
use weft_task_store::parked_fires::{park, ParkAppend, ParkRefusal, Waiting};
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

/// The birth of run `execution_id` of `project_id`, in `phase`, on image
/// `binary_hash`.
fn birth(execution_id: weft_core::ExecutionId, project_id: Uuid, phase: weft_core::context::Phase, binary_hash: &str) -> ExecEvent {
    ExecEvent::ExecutionStarted {
        execution_id, project_id, entry_node: "entry".into(), phase,
        definition_hash: Some("def-1".into()), binary_hash: Some(binary_hash.into()),
        source_version: (phase == weft_core::context::Phase::TriggerSetup).then(|| "source".into()),
        run_kind: weft_core::exec::RunKind::Execution, selection: None, seed: None, instance: None, stand_in: None,
        fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        settings: Default::default(),
    }
}

/// Queue the run `events` start (`events[0]` its birth), the way the
/// dispatcher starts a run by hand or a setup run.
async fn queue(journal: &PostgresJournal, events: &[ExecEvent], for_activation: bool) -> anyhow::Result<bool> {
    journal.queue_run(Queued {
        events, tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT, watch_end: false,
        stale: &[], spec: None, example: None,
    }, for_activation).await
}

/// A fire run of `project_id`, queued.
async fn queued_run(journal: &PostgresJournal, project_id: Uuid) -> weft_core::ExecutionId {
    let execution_id = weft_core::new_execution_id();
    assert!(queue(journal, &[birth(execution_id, project_id, weft_core::context::Phase::Fire, "bin-A")], false).await.unwrap());
    execution_id
}

/// End run `execution_id`, which nobody drives, completed.
async fn complete(journal: &PostgresJournal, execution_id: weft_core::ExecutionId) -> ExecEvent {
    let completed = ExecEvent::ExecutionCompleted { execution_id, at_unix: 2 };
    let written = journal.append(execution_id, std::slice::from_ref(&completed), Then::Stays).await.unwrap();
    assert!(matches!(written, weft_journal::record::Appended::At(_)), "{written:?}");
    completed
}

/// One column of run `execution_id`'s row.
async fn run_column<T>(pool: &PgPool, column: &str, execution_id: weft_core::ExecutionId) -> Option<T>
where
    T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + Unpin,
{
    sqlx::query_scalar(&format!("SELECT {column} FROM run WHERE execution_id = $1"))
        .bind(execution_id)
        .fetch_optional(pool)
        .await
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

// ----- a run keeps its image -----------------------------------------------

/// A run keeps the image it was born on across project edits: delivery
/// reads its row, and so does every claim that carries it on after an
/// answer.
#[sqlx::test]
async fn a_queued_run_keeps_the_image_it_was_born_on(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let execution_id = weft_core::new_execution_id();
    let start = birth(execution_id, id, weft_core::context::Phase::Fire, "bin-A");
    seed_project(&projects, id, "bin-B").await;
    assert!(queue(&journal, &[start], false).await.unwrap());
    assert_eq!(run_column::<String>(&pool, "binary_hash", execution_id).await.as_deref(), Some("bin-A"));
    assert_eq!(run_column::<String>(&pool, "state", execution_id).await.as_deref(), Some("queued"));
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

/// The program identity every setup birth here names (`trigger_setup_birth`).
fn setup_program() -> weft_core::project::hash::ProgramIdentity {
    weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-1".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    }
}

/// The birth of trigger setup `execution_id` of project `id`, from
/// version `source`.
fn trigger_setup_birth(id: Uuid, execution_id: Uuid) -> ExecEvent {
    birth(execution_id, id, weft_core::context::Phase::TriggerSetup, "bin-A")
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
    let birth = trigger_setup_birth(id, execution_id);
    queue(&journal, std::slice::from_ref(&birth), false).await.unwrap();
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err());
    let complete = complete(&journal, execution_id).await;
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err(), "publication still owns this source");
    let bake = weft_dispatcher::journal::TriggerBake::from_events(&[birth, complete], &setup_program()).unwrap().unwrap();
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
    assert!(queue(&journal, &[trigger_setup_birth(id, first)], true).await.is_err(),
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
    let birth = trigger_setup_birth(id, first);
    queue(&journal, std::slice::from_ref(&birth), false).await.unwrap();
    let complete = complete(&journal, first).await;
    let bake = weft_dispatcher::journal::TriggerBake::from_events(&[birth, complete], &setup_program()).unwrap().unwrap();
    journal.finish_trigger_setup(first, Some(&bake)).await.unwrap();

    let second = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], second, None).await.unwrap().is_ok());
    assert!(queue(&journal, &[trigger_setup_birth(id, Uuid::new_v4())], true).await.is_err(),
        "a setup no activation is setting up is never born as one");
    queue(&journal, &[trigger_setup_birth(id, second)], true).await.unwrap();
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

/// How many rows of `table` belong to run `execution_id`.
async fn rows_of(pool: &PgPool, table: &str, execution_id: weft_core::ExecutionId) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE execution_id = $1"))
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Queueing a run is ONE transaction: its row and its record's first row
/// commit together or not at all. A run of a project with no row is
/// refused with nothing written, and a run queued again by its id (a
/// retried start) writes nothing twice.
#[sqlx::test]
async fn queueing_a_run_is_atomic_and_happens_once(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let missing = Uuid::new_v4();
    let lost = weft_core::new_execution_id();
    let err = queue(&journal, &[birth(lost, missing, weft_core::context::Phase::Fire, "bin-A")], false).await.unwrap_err();
    assert!(format!("{err:#}").contains("has no row"), "{err:?}");
    assert_eq!((rows_of(&pool, "run", lost).await, rows_of(&pool, "run_log", lost).await), (0, 0), "a refused start leaves nothing");

    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let execution_id = weft_core::new_execution_id();
    let start = birth(execution_id, id, weft_core::context::Phase::Fire, "bin-A");
    let kick = ExecEvent::NodeKicked {
        execution_id, node_id: "entry".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 1,
    };
    assert!(queue(&journal, &[start.clone(), kick.clone()], false).await.unwrap());
    assert_eq!((rows_of(&pool, "run", execution_id).await, rows_of(&pool, "run_log", execution_id).await), (1, 1),
        "the birth and its kicks are the record's first row");
    assert_eq!(journal.events_log(execution_id).await.unwrap().len(), 2);
    assert!(!queue(&journal, &[start, kick], false).await.unwrap(), "a retried start finds its run");
    assert_eq!(rows_of(&pool, "run_log", execution_id).await, 1, "and writes nothing twice");
}

/// Seed a live worker of `project_id` on image `binary_hash`, driving
/// `in_flight` runs per trigger token.
async fn seed_worker(pool: &PgPool, replica: &str, project_id: Uuid, binary_hash: &str, in_flight: serde_json::Value, alive: bool) {
    let until = weft_dispatcher::lease::now_unix() + if alive { 60 } else { -60 };
    sqlx::query(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix, binary_hash, in_flight) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (replica) DO UPDATE SET leased_until_unix = EXCLUDED.leased_until_unix, in_flight = EXCLUDED.in_flight",
    )
    .bind(replica)
    .bind(project_id)
    .bind(TENANT)
    .bind(until)
    .bind(binary_hash)
    .bind(in_flight)
    .execute(pool)
    .await
    .unwrap();
}

/// Claim queued run `execution_id` of `project_id` for `owner`, the way
/// a worker takes it.
async fn claim(pool: &PgPool, execution_id: weft_core::ExecutionId, project_id: Uuid, owner: &str) {
    let mut conn = pool.acquire().await.unwrap();
    weft_journal::record::claim(&mut conn, execution_id, project_id, owner).await.unwrap().expect("claimed");
}

/// A drain counts what the project's live workers say they drive, plus
/// the runs on record that are queued or driven by a live worker. A run
/// parked on a wait is not going, and neither is a run whose worker's
/// lease lapsed (the lost-run sweep owns it).
#[sqlx::test]
async fn the_drain_counts_what_workers_drive_and_the_runs_queued_or_driven(pool: PgPool) {
    use weft_dispatcher::drain::{going, DrainScope, Reaching};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let every = weft_core::instance::Copies::Shared;
    let scope = DrainScope { project_id: id, reaching: Reaching::Copies(&every), except: None };
    let older = DrainScope { project_id: id, reaching: Reaching::OtherImages("bin-B"), except: None };
    assert_eq!(going(&pool, &scope).await.unwrap(), 0);

    let run = queued_run(&journal, id).await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 1, "queued for a worker");
    claim(&pool, run, id, "worker-a").await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 0, "its owner has no live lease: lost, not going");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({ "tok": 2 }), true).await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 3, "two its worker states, plus the run on record");
    assert_eq!(going(&pool, &older).await.unwrap(), 3, "all of it runs on an image other than bin-B");
    seed_worker(&pool, "worker-b", id, "bin-B", json!({ "tok": 4 }), true).await;
    assert_eq!(going(&pool, &older).await.unwrap(), 3, "the worker on bin-B is not older than bin-B");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({ "tok": 2 }), false).await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 4, "a worker whose lease lapsed says nothing, and its run is lost");

    sqlx::query("UPDATE run SET state = 'parked', owner = NULL WHERE execution_id = $1").bind(run).execute(&pool).await.unwrap();
    seed_worker(&pool, "worker-b", id, "bin-B", json!({}), true).await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 0, "a parked run is not going");
}

/// A run whose owner's lease ran out is let go of: a durable one is
/// queued again for the next worker, its epoch raised so a late batch of
/// the old owner is refused; a fast one lived in its worker's memory and
/// ends, cancelled. A run whose owner is alive is left alone.
#[sqlx::test]
async fn a_lost_run_is_queued_again_when_durable_and_ended_when_fast(pool: PgPool) {
    use weft_dispatcher::journal::Lost;
    use weft_core::run_settings::{Keeping, RunSettings};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let start = |keeping: Keeping| {
        let execution_id = weft_core::new_execution_id();
        let mut started = birth(execution_id, id, weft_core::context::Phase::Fire, "bin-A");
        if let ExecEvent::ExecutionStarted { settings, .. } = &mut started {
            *settings = RunSettings::new(keeping, true).unwrap();
        }
        (execution_id, started)
    };
    let now = weft_dispatcher::lease::now_unix();
    let (durable, started) = start(Keeping::Durable);
    queue(&journal, &[started], false).await.unwrap();
    claim(&pool, durable, id, "worker-a").await;
    let epoch: i32 = run_column(&pool, "epoch", durable).await.unwrap();

    seed_worker(&pool, "worker-a", id, "bin-A", json!({}), true).await;
    assert_eq!(journal.let_go_of_lost(durable, now, None).await.unwrap(), Lost::NotLost, "its owner is alive");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({}), false).await;
    assert_eq!(journal.let_go_of_lost(durable, now, None).await.unwrap(), Lost::Requeued);
    assert_eq!(run_column::<String>(&pool, "state", durable).await.as_deref(), Some("queued"));
    assert_eq!(run_column::<Option<String>>(&pool, "owner", durable).await, Some(None));
    assert!(run_column::<i32>(&pool, "epoch", durable).await.unwrap() > epoch, "a late batch of the old owner is refused");

    let (fast, started) = start(Keeping::Fast);
    queue(&journal, &[started], false).await.unwrap();
    claim(&pool, fast, id, "worker-a").await;
    assert_eq!(journal.let_go_of_lost(fast, now, None).await.unwrap(), Lost::Ended);
    assert_eq!(run_column::<String>(&pool, "state", fast).await.as_deref(), Some("ended"));
    assert!(matches!(journal.events_log(fast).await.unwrap().last(), Some(ExecEvent::ExecutionCancelled { .. })));
    assert_eq!(journal.let_go_of_lost(fast, now, None).await.unwrap(), Lost::NotLost, "an ended run is not lost");
}

/// A cancel ends a run nobody drives on the spot, after its last row; a
/// run a worker drives is asked to stop, and its worker writes the ending.
#[sqlx::test]
async fn a_cancel_ends_a_waiting_run_and_asks_a_driven_one_to_stop(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let cause = weft_core::exec::CancelCause::User;

    let waiting = queued_run(&journal, id).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE execution_id = $1").bind(waiting).execute(&pool).await.unwrap();
    let written = journal.cancel_execution(waiting, None, &cause).await.unwrap();
    assert!(!written.requested);
    assert_eq!(written.node_cancellations, Some(0));
    assert_eq!(run_column::<String>(&pool, "state", waiting).await.as_deref(), Some("ended"));
    assert!(matches!(journal.events_log(waiting).await.unwrap().last(), Some(ExecEvent::ExecutionCancelled { .. })));

    let driven = queued_run(&journal, id).await;
    claim(&pool, driven, id, "worker-a").await;
    let written = journal.cancel_execution(driven, None, &cause).await.unwrap();
    assert!(written.requested, "its worker is asked");
    assert_eq!(written.node_cancellations, None);
    assert_eq!(run_column::<String>(&pool, "state", driven).await.as_deref(), Some("running"), "its worker ends it");
    let asked: Option<serde_json::Value> = run_column(&pool, "cancel_requested", driven).await;
    assert!(asked.is_some());
    assert_eq!(journal.events_log(driven).await.unwrap().len(), 1, "nothing written into a record its worker writes");
}

/// An ending somebody waits on is found off the run's row, whether or not
/// its announcement was heard: the look after a lost notification finds
/// it, two dispatchers never take it at once, and once handled it is not
/// found again.
#[sqlx::test]
async fn an_ending_with_work_left_is_found_without_its_announcement(pool: PgPool) {
    use weft_dispatcher::run_ends::{clear_flags, take_flagged};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let execution_id = weft_core::new_execution_id();
    let started = birth(execution_id, id, weft_core::context::Phase::Fire, "bin-A");
    assert!(journal.queue_run(Queued {
        events: std::slice::from_ref(&started), tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: true, stale: &[], spec: None, example: None,
    }, false).await.unwrap());
    let quiet = queued_run(&journal, id).await;
    complete(&journal, execution_id).await;
    complete(&journal, quiet).await;

    let mut one = pool.begin().await.unwrap();
    let found = take_flagged(&mut one, execution_id).await.unwrap().expect("the watched ending is flagged");
    assert!(found.watch_end);
    let mut other = pool.begin().await.unwrap();
    assert!(take_flagged(&mut other, quiet).await.unwrap().is_none(), "nobody waits on the other ending");
    assert!(take_flagged(&mut other, execution_id).await.unwrap().is_none(), "held by the first look");
    other.rollback().await.unwrap();
    clear_flags(&mut one, &found).await.unwrap();
    one.commit().await.unwrap();
    let mut again = pool.begin().await.unwrap();
    assert!(take_flagged(&mut again, execution_id).await.unwrap().is_none(), "handled once");
}

/// The retention loop erases a run once it ended longer ago than it is
/// kept for, with its record, and never a run that has not ended, however
/// old.
#[sqlx::test]
async fn retention_erases_ended_runs_past_their_keep_and_never_a_waiting_one(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ended = queued_run(&journal, id).await;
    complete(&journal, ended).await;
    let parked = queued_run(&journal, id).await;
    sqlx::query("UPDATE run SET state = 'parked', started_at = 0 WHERE execution_id = $1").bind(parked).execute(&pool).await.unwrap();
    let keep_until: i64 = run_column(&pool, "keep_until", ended).await.expect("an ended run has its keep");

    assert_eq!(journal.erase_expired(keep_until, 100).await.unwrap().0, 0, "kept until its keep runs out");
    assert_eq!(journal.erase_expired(i64::MAX, 100).await.unwrap().0, 1);
    assert_eq!((rows_of(&pool, "run", ended).await, rows_of(&pool, "run_log", ended).await), (0, 0));
    assert_eq!(rows_of(&pool, "run", parked).await, 1, "a parked run is never erased");
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
#[sqlx::test]
async fn referenced_images_cover_projects_runs_workers_maps_and_unit_refs(
    pool: PgPool,
) {
    let (journal, projects) = setup(&pool).await;
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
    // A live worker's image stays (it may drive a run its door just bore,
    // before anything of it is on record); a worker whose lease lapsed is
    // gone.
    seed_worker(&pool, "worker-live", plain, "hash-live-worker", json!({}), true).await;
    seed_worker(&pool, "worker-gone", plain, "hash-gone-worker", json!({}), false).await;

    // A run still waiting (a form, a timer) resumes on the image it
    // started on, so its image stays; a finished run's does not.
    for (hash, finished) in [("hash-waiting-run", false), ("hash-finished-run", true)] {
        let execution_id = weft_core::new_execution_id();
        queue(&journal, &[birth(execution_id, plain, weft_core::context::Phase::Fire, hash)], false).await.unwrap();
        if finished {
            complete(&journal, execution_id).await;
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
    Waiting { fire_id: Uuid::new_v4(), payload: json!({ "v": 1 }), caller: None, attempts, not_before, instance_gap: None }
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

/// A wait `token` of run `execution_id`.
async fn seed_wait(journal: &PostgresJournal, token: &str, project: Uuid, execution_id: weft_core::ExecutionId) {
    let mut wait = entry_signal(token, project);
    wait.node_id = format!("wait-{token}");
    wait.is_resume = true;
    wait.execution_id = Some(execution_id);
    journal.signal_insert(&wait).await.expect("seed the wait");
}

/// An answer reaches its run once: a run nobody drives gets it in its
/// record and is queued to carry on; a run a worker drives has it handed
/// to that worker (nobody else writes its record); a run that ended takes
/// nothing. The wait's signal goes in every case, so a second answer finds
/// nothing, and an entry's token is never a wait.
#[sqlx::test]
async fn an_answer_reaches_its_run_once(pool: PgPool) {
    use weft_dispatcher::journal::Answered;
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-entry", project).await;
    assert!(matches!(journal.answer("tok-entry", &json!(1)).await.unwrap(), Answered::Gone), "an entry is not a wait");

    let parked_run = queued_run(&journal, project).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE execution_id = $1").bind(parked_run).execute(&pool).await.unwrap();
    seed_wait(&journal, "tok-parked", project, parked_run).await;
    let Answered::Reached { consumed } = journal.answer("tok-parked", &json!("yes")).await.unwrap() else { panic!("reached") };
    assert_eq!(consumed.token, "tok-parked");
    assert!(journal.signal_get("tok-parked").await.unwrap().is_none(), "answered once");
    assert_eq!(run_column::<String>(&pool, "state", parked_run).await.as_deref(), Some("queued"), "queued to carry on");
    assert!(matches!(journal.events_log(parked_run).await.unwrap().last(),
        Some(ExecEvent::SuspensionResolved { token, value, .. }) if token == "tok-parked" && value == &json!("yes")));
    assert!(matches!(journal.answer("tok-parked", &json!("again")).await.unwrap(), Answered::Gone));

    let driven = queued_run(&journal, project).await;
    claim(&pool, driven, project, "worker-a").await;
    seed_wait(&journal, "tok-driven", project, driven).await;
    assert!(matches!(journal.answer("tok-driven", &json!(2)).await.unwrap(), Answered::Reached { .. }));
    assert_eq!(journal.events_log(driven).await.unwrap().len(), 1, "nothing written into a record its worker writes");
    assert_eq!(weft_task_store::parked_fires::answers_for(&pool, driven).await.unwrap(), vec![("tok-driven".to_string(), json!(2))]);

    let ended = queued_run(&journal, project).await;
    seed_wait(&journal, "tok-ended", project, ended).await;
    complete(&journal, ended).await;
    assert!(matches!(journal.answer("tok-ended", &json!(3)).await.unwrap(), Answered::RunEnded { .. }));
    assert!(journal.signal_get("tok-ended").await.unwrap().is_none());

    // An answer queued while the run's trigger was not live is the wait's
    // one answer: a later one finds the wait answered, and the queued one
    // stays for the drain to hand over.
    let queued_for = queued_run(&journal, project).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE execution_id = $1").bind(queued_for).execute(&pool).await.unwrap();
    seed_wait(&journal, "tok-queued", project, queued_for).await;
    assert_eq!(park(&pool, "tok-queued", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);
    assert!(matches!(journal.answer("tok-queued", &json!("second")).await.unwrap(), Answered::Gone));
    assert!(journal.signal_get("tok-queued").await.unwrap().is_some(), "the queued answer still has its wait");
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

/// The park names its refusal instead of returning "0 rows": a re-run
/// that finds its own event queued (nothing lost), a wait already
/// answered, an entry queue at its cap (a refused NEW event, a loss the
/// caller must say out loud), and a vanished signal (the project was wiped
/// under the fire) are four different facts. A signal going takes its
/// queue along.
#[sqlx::test]
async fn a_park_names_its_refusal(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-entry", project).await;
    let waiting_run = queued_run(&journal, project).await;
    seed_wait(&journal, "tok-resume", project, waiting_run).await;

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
    sqlx::query(
        "INSERT INTO parked_fire (token, fire_id, payload, attempts, not_before, is_resume) \
         SELECT 'tok-entry', gen_random_uuid(), '{}', 0, 0, FALSE FROM generate_series(2, $1)",
    )
    .bind(weft_task_store::parked_fires::MAX_PARKED_ENTRY_FIRES)
    .execute(&pool)
    .await
    .expect("fill the queue to the cap");
    assert_eq!(park(&pool, "tok-entry", &parked(0, 0), None).await.unwrap(), ParkAppend::Refused(ParkRefusal::QueueFull));

    journal.signal_remove_many(&["tok-entry".to_string()]).await.unwrap();
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM parked_fire WHERE token = 'tok-entry'").fetch_one(&pool).await.unwrap();
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
    park(&pool, "tok-due", &parked(0, now - 10), None).await.unwrap();
    seed_parked_signal(&journal, "tok-later", active).await;
    park(&pool, "tok-later", &parked(3, now + 300), None).await.unwrap();
    park(&pool, "tok-later", &parked(0, now - 10), None).await.unwrap();
    seed_parked_signal(&journal, "tok-inactive", inactive).await;
    park(&pool, "tok-inactive", &parked(0, now - 10), None).await.unwrap();

    assert_eq!(due_tokens(&pool, now).await.unwrap(), ["tok-due"],
        "due heads on live triggers only; a backing-off head holds its queue, and a parked trigger's queue is left alone");
    assert_eq!(next_due(&pool).await.unwrap(), Some(now - 10));
}

/// One trigger's events are handed over in the order they came, and two
/// drains never take one head: the second finds the head locked and takes
/// nothing (not the event behind it, which would overtake it).
#[sqlx::test]
async fn two_drains_never_take_one_head_and_events_leave_in_order(pool: PgPool) {
    use weft_task_store::parked_fires::{remove_in, restamp_in, take_head};
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok", project).await;
    let (first, second) = (parked(0, 0), parked(0, 0));
    park(&pool, "tok", &first, None).await.unwrap();
    park(&pool, "tok", &second, None).await.unwrap();
    let now = weft_dispatcher::lease::now_unix();

    let mut one = pool.begin().await.unwrap();
    let head = take_head(&mut one, "tok", now).await.unwrap().expect("the head");
    assert_eq!(head.waiting.fire_id, first.fire_id, "first in, first out");
    let mut other = pool.begin().await.unwrap();
    assert!(take_head(&mut other, "tok", now).await.unwrap().is_none(), "a held head is skipped, and nothing overtakes it");
    other.rollback().await.unwrap();

    // A head that could not be handed over stays the head, backing off.
    restamp_in(&mut one, &head, 1, now + 60, None).await.unwrap();
    one.commit().await.unwrap();
    let mut again = pool.begin().await.unwrap();
    assert!(take_head(&mut again, "tok", now).await.unwrap().is_none(), "backing off, it holds its queue");
    let head = take_head(&mut again, "tok", now + 60).await.unwrap().expect("due again");
    assert_eq!((head.waiting.fire_id, head.waiting.attempts), (first.fire_id, 1));
    remove_in(&mut again, &head).await.unwrap();
    again.commit().await.unwrap();

    let mut last = pool.begin().await.unwrap();
    assert_eq!(take_head(&mut last, "tok", now).await.unwrap().expect("the next").waiting.fire_id, second.fire_id);
}

/// An event parked because its instance has not filled a value never comes
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
    let gap = |reason: &str| Waiting { instance_gap: Some(reason.to_string()), ..parked(1, 0) };
    park(&pool, "tok-ada", &gap("instance 'ada' at 'answer': 'key' is not filled"), None).await.unwrap();
    park(&pool, "tok-ada", &gap("instance 'ada' at 'answer': 'model' is not filled"), None).await.unwrap();
    park(&pool, "tok-timer", &parked(1, 0), None).await.unwrap();

    let now = weft_dispatcher::lease::now_unix();
    assert_eq!(due_tokens(&pool, now).await.unwrap(), ["tok-timer"], "only the timer-retried head is due");
    assert_eq!(next_due(&pool).await.unwrap(), Some(0), "the next-due read sees the timer head alone");

    let ada = InstanceId::new("ada").unwrap();
    assert_eq!(instance_gap_tokens(&pool, project, &ada).await.unwrap(), ["tok-ada"]);
    assert!(instance_gap_tokens(&pool, project, &InstanceId::new("bob").unwrap()).await.unwrap().is_empty());

    let waits = instance_waits(&pool, project).await.unwrap();
    let key = ActivationKey::new("trigger-tok-ada", Owner::from_instance(Some(ada)));
    let waiting = waits.get(&key).expect("ada's trigger waits");
    assert_eq!(waiting.fires, 2);
    assert_eq!(waiting.reason, "instance 'ada' at 'answer': 'model' is not filled", "the event parked last");
    assert_eq!(waits.len(), 1, "an event retried on its timer is not waiting on an instance");
}

/// Every status reader goes through `infra_node::observe`, which reads
/// the copies with the commands the supervisor has not finished applied:
/// an instance's start queued over its stopped copy reads provisioning at
/// once (not the old `stopped`), a queued stop of a running copy reads
/// stopping, and a start of a copy with no row yet lists it as starting.
#[sqlx::test]
async fn copies_read_with_the_commands_under_way(pool: PgPool) {
    use weft_broker::lifecycle_writes::{issue_command, IssuedCommand};
    use weft_dispatcher::infra_lifecycle_command::{issue_lifecycle, InfraLifecycleVerb, TakeDown};
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
    issue_lifecycle(&pool, TENANT, project, Some("db"), &weft_core::instance::Copies::Shared, TakeDown::Stop { force: false }, None, "disp-1")
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
    use weft_dispatcher::infra_lifecycle_command::{any_in_flight, issue_lifecycle, TakeDown};
    use weft_core::instance::Copies;
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let stop = |copies: Copies| {
        let pool = pool.clone();
        async move {
            issue_lifecycle(&pool, TENANT, project, Some("db"), &copies, TakeDown::Stop { force: false }, None, "disp-1")
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

/// Everywhere one run lives, so a table dropped from the erase list
/// cannot ship quietly.
async fn footprint(pool: &PgPool, execution_id: weft_core::ExecutionId) -> i64 {
    let mut rows = 0;
    for table in ["run", "run_log", "execution_tag", "trigger_setup", "run_search_queue", "run_search"] {
        rows += rows_of(pool, table, execution_id).await;
    }
    rows + sqlx::query_scalar::<_, i64>(
        "SELECT (SELECT COUNT(*) FROM signal WHERE execution_id = $1 AND is_resume) \
              + (SELECT COUNT(*) FROM parked_fire WHERE execution_id = $1)",
    )
    .bind(execution_id)
    .fetch_one(pool)
    .await
    .unwrap()
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

    let first = queued_run(&journal, doomed).await;
    let second = queued_run(&journal, doomed).await;
    let survivor = queued_run(&journal, neighbour).await;

    // Everything else a run leaves behind, on the first execution.
    let mut tx = pool.begin().await.unwrap();
    weft_journal::tags::tag_execution_in(&mut tx, first, &["user_7".to_string()], 10)
        .await
        .expect("tag");
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO trigger_setup (project_id, execution_id) VALUES ($1, $2)")
        .bind(doomed)
        .bind(first)
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
        .bind(execution_id)
        .bind(is_resume)
        .execute(&pool)
        .await
        .expect("signal");
    }
    let mut tx = pool.begin().await.unwrap();
    weft_task_store::parked_fires::hand_answer_in(&mut tx, "answered-tok", first, &json!(1)).await.expect("a handed answer");
    sqlx::query("INSERT INTO run_search_queue (execution_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(first)
        .execute(&mut *tx)
        .await
        .expect("waiting for the index");
    tx.commit().await.unwrap();
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
    let orphan = queued_run(&journal, gone).await;
    queued_run(&journal, living).await;

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

/// A removed project's waiting work is cleared; work already taken is
/// left to finish, and a live project's is never touched.
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
                    kind: weft_task_store::TaskKind::RegisterSignal.into(),
                    project_id: Some(project),
                    dedup_key: Some(key.into()),
                    execution_id: None,
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
    sqlx::query("UPDATE task SET status = 'claimed' WHERE dedup_key = 'claimed'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(weft_dispatcher::reaper::drop_work_of_removed_projects(&pool).await.unwrap(), 1);
    let left: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT project_id, dedup_key FROM task ORDER BY dedup_key").fetch_all(&pool).await.unwrap();
    assert_eq!(left, vec![(live, "a".to_string()), (removed, "claimed".to_string())]);
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
    let spared = queued_run(&journal, id).await;
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

/// What an infra says changed is written where its node put it, with no
/// node running: a password into the connection the node published, a
/// baked output over the saved one. A push naming anything the node did
/// not hand weft, or a copy that is gone, is refused and writes nothing.
#[sqlx::test]
async fn what_an_infra_says_changed_is_written_where_its_node_put_it(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, baked_json) VALUES ($1, 'db', 'c1', 'running', $2)",
    )
    .bind(project)
    .bind(json!({ "address": "db:5432" }))
    .execute(&pool)
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
        let sealed: String = sqlx::query_scalar("SELECT values_sealed FROM access_grant WHERE project_id = $1 AND published_by_node = 'db'")
            .bind(project)
            .fetch_one(&pool)
            .await
            .unwrap();
        weft_access_store::open_json(&sealed).unwrap()["password"].as_str().unwrap().to_string()
    };
    let baked = || async {
        sqlx::query_scalar::<_, serde_json::Value>("SELECT baked_json FROM infra_node WHERE project_id = $1 AND node_id = 'db'")
            .bind(project)
            .fetch_one(&pool)
            .await
            .unwrap()
    };

    let reset = weft_core::infra::bake::PushedValues {
        connection: [("password".to_string(), "p2".to_string())].into(),
        outputs: [("address".to_string(), json!("db:6543"))].into(),
    };
    weft_access_store::write_pushed_values(&pool, project, "c1", &reset).await.unwrap();
    assert_eq!(password().await, "p2", "the connection holds the new password");
    assert_eq!(baked().await, json!({ "address": "db:6543" }));

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
