//! Layer-3 tests for the dispatcher's OWN SQL, against a REAL Postgres.
//! The dispatcher's correctness-critical decisions (task stamping, the
//! listener reap-vs-placement lock discipline) live in SQL statements the
//! fake stores never execute, so a faked layer cannot catch their bugs;
//! two escaped to the live cluster before this rig existed (a `uuid =
//! text` bind error in the enqueue stamp, and the reap/placement
//! write-skew). These tests exercise the actual statements.
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

use async_trait::async_trait;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_core::ProjectDefinition;
use weft_dispatcher::api::project::{due_parked_tokens, release_stale_drain_claims};
use weft_dispatcher::api::signal::{
    append_parked_fire, restamp_parked_fire, signals_visible_to, ParkAppend, ParkedFire,
    ParkRefusal,
};
use weft_dispatcher::journal::postgres::PostgresJournal;
use weft_dispatcher::journal::{Journal, SignalPlacement, SignalRegistration};
use weft_dispatcher::listener::{ListenerBackend, ListenerHandle, ListenerPool};
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

/// Insert a live `listener_pod` row whose spawn grace is already past
/// (so it is reap-eligible the moment it holds zero signals).
async fn seed_listener_pod(pool: &PgPool, pod_name: &str, owner: &str) {
    let now = weft_dispatcher::lease::now_unix();
    sqlx::query(
        "INSERT INTO listener_pod \
         (pod_name, admin_url, namespace, owner_pod_id, leased_until_unix, grace_until_unix) \
         VALUES ($1, $2, 'weft-system', $3, $4, $5)",
    )
    .bind(pod_name)
    .bind(format!("http://{pod_name}.weft-system.svc.cluster.local:8080"))
    .bind(owner)
    .bind(now + 3600)
    .bind(now - 1)
    .execute(pool)
    .await
    .expect("insert listener_pod");
}

async fn listener_pod_exists(pool: &PgPool, pod_name: &str) -> bool {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT 1::bigint FROM listener_pod WHERE pod_name = $1")
            .bind(pod_name)
            .fetch_optional(pool)
            .await
            .expect("listener_pod lookup");
    row.is_some()
}

/// A minimal entry-signal registration for the placement tests.
fn entry_signal(token: &str, project_id: Uuid) -> SignalRegistration {
    SignalRegistration {
        source_version: None,
        setup_color: None,
        program: None,
        token: token.to_string(),
        tenant_id: TENANT.to_string(),
        project_id,
        color: None,
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
        listener_pod: None,
    }
}

/// Dumb fake: records `stop` calls, never spawns (the reap tests never
/// spawn through the backend).
#[derive(Default)]
struct FakeBackend {
    stopped: std::sync::Mutex<Vec<String>>,
}

#[async_trait]
impl ListenerBackend for FakeBackend {
    async fn spawn(&self, _pod_name: &str, _namespace: &str) -> anyhow::Result<ListenerHandle> {
        anyhow::bail!("FakeBackend::spawn is not used by these tests")
    }
    async fn stop(&self, pod_name: &str, _namespace: &str) -> anyhow::Result<()> {
        self.stopped.lock().unwrap().push(pod_name.to_string());
        Ok(())
    }
}

// ----- task stamping (the enqueue reads the project row) -------------------

/// Birth and resume retain the original image across project edits.
#[sqlx::test]
async fn execution_birth_and_resume_pin_the_original_image(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;

    let color = weft_core::Color::new_v4();
    let program = weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-1".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    };
    let start = weft_journal::ExecEvent::ExecutionStarted {
        color, project_id: id, entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire, definition_hash: Some("def-1".into()),
        program: Some(program), source_version: None, node_test: false, subgraph: None, seed: None, at_unix: 1,
    };
    let task = weft_dispatcher::task_kinds::execute::execution_task_spec(
        weft_task_store::TaskKind::Execute, id, color, "def-1", "bin-A", Some(TENANT), None, None,
    ).unwrap();
    seed_project(&projects, id, "bin-B").await;
    journal.start_execution(&start, &[], task, None).await.unwrap();

    let (kind, binary_hash): (String, Option<String>) = sqlx::query_as(
        "SELECT kind, binary_hash FROM task WHERE color = $1",
    )
    .bind(color.to_string())
    .fetch_one(&pool)
    .await
    .expect("task row");
    assert_eq!(kind, "execute");
    assert_eq!(
        binary_hash.as_deref(),
        Some("bin-A"),
        "the execute task must carry the image it was enqueued for"
    );
    weft_dispatcher::task_kinds::execute::enqueue_resume(&pool, id, color, "def-1", Some(TENANT)).await.unwrap();
    let resume_hash: String = sqlx::query_scalar("SELECT binary_hash FROM task WHERE color = $1 AND kind = 'resume'")
        .bind(color.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(resume_hash, "bin-A");
}

// ----- listener reap vs placement (the advisory-lock handshake) ------------

/// The placement stamp refuses a pod whose registry row is gone (the
/// reaped-mid-placement case): `signal_insert` writes nothing and errors,
/// so a signal can never be committed pointing at a reaped pod.
#[sqlx::test]
async fn signal_insert_refuses_a_reaped_pod(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;

    let placement = SignalPlacement { listener_pod: "listener-ghost".to_string(), generation: 1 };
    let err = journal
        .signal_insert(&entry_signal("tok-1", id), &placement)
        .await
        .expect_err("stamp onto a nonexistent pod must fail");
    assert!(
        err.to_string().contains("reaped"),
        "error must name the reap race: {err}"
    );
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*)::bigint FROM signal")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count.0, 0, "no signal row may be committed on a reaped pod");

    // With the pod row present, the same insert lands.
    seed_listener_pod(&pool, "listener-ghost", "disp-1").await;
    journal
        .signal_insert(&entry_signal("tok-1", id), &placement)
        .await
        .expect("stamp onto a live pod");
}

/// `set_placement` (the re-place path's stamp) has the same guard.
#[sqlx::test]
async fn set_placement_refuses_a_reaped_pod(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    seed_listener_pod(&pool, "listener-a", "disp-1").await;
    journal
        .signal_insert(
            &entry_signal("tok-1", id),
            &SignalPlacement { listener_pod: "listener-a".to_string(), generation: 1 },
        )
        .await
        .expect("initial placement");

    let err = weft_dispatcher::listener::set_placement(&pool, "tok-1", "listener-gone", 2)
        .await
        .expect_err("re-place onto a nonexistent pod must fail");
    assert!(err.to_string().contains("reaped"), "error must name the reap race: {err}");

    // The row still points at the ORIGINAL pod (the failed stamp wrote
    // nothing).
    let (pod,): (Option<String>,) =
        sqlx::query_as("SELECT listener_pod FROM signal WHERE token = 'tok-1'")
            .fetch_one(&pool)
            .await
            .expect("signal row");
    assert_eq!(pod.as_deref(), Some("listener-a"));
}

/// The reaper deletes an idle (zero-signal, past-grace) pod, row first,
/// then the backend stop; a pod holding a placed signal is left alone.
#[sqlx::test]
async fn reap_deletes_idle_pods_and_spares_placed_ones(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    seed_listener_pod(&pool, "listener-idle", "disp-1").await;
    seed_listener_pod(&pool, "listener-held", "disp-1").await;
    journal
        .signal_insert(
            &entry_signal("tok-1", id),
            &SignalPlacement { listener_pod: "listener-held".to_string(), generation: 1 },
        )
        .await
        .expect("place a signal on listener-held");

    let backend = FakeBackend::default();
    let listeners = ListenerPool::new("weft-system".to_string());
    listeners
        .reap_idle(&backend, &pool, "disp-1")
        .await
        .expect("reap sweep");

    assert!(!listener_pod_exists(&pool, "listener-idle").await, "idle pod must be reaped");
    assert!(listener_pod_exists(&pool, "listener-held").await, "placed pod must survive");
    assert_eq!(
        *backend.stopped.lock().unwrap(),
        vec!["listener-idle".to_string()],
        "exactly the idle pod's k8s objects are stopped"
    );
}

/// The race itself, stress-looped: a placement stamping onto a pod and
/// the reaper sweeping it run CONCURRENTLY, many rounds. The advisory
/// lock guarantees exactly one winner per round: either the signal is
/// committed AND the pod row survives, or the stamp failed AND the pod
/// row is gone. The broken interleaving (signal committed on a deleted
/// pod: a placement nothing will ever fire) must never appear.
#[sqlx::test]
async fn reap_and_stamp_race_has_exactly_one_winner(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let journal = std::sync::Arc::new(journal);
    let listeners = std::sync::Arc::new(ListenerPool::new("weft-system".to_string()));

    for round in 0..25 {
        let pod = format!("listener-race-{round}");
        let token = format!("tok-race-{round}");
        seed_listener_pod(&pool, &pod, "disp-1").await;

        let stamp = {
            let journal = journal.clone();
            let sig = entry_signal(&token, id);
            let placement = SignalPlacement { listener_pod: pod.clone(), generation: 1 };
            tokio::spawn(async move { journal.signal_insert(&sig, &placement).await })
        };
        let reap = {
            let listeners = listeners.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                let backend = FakeBackend::default();
                listeners.reap_idle(&backend, &pool, "disp-1").await
            })
        };
        let stamp_result = stamp.await.expect("stamp task");
        reap.await.expect("reap task").expect("reap sweep");

        let signal_placed: Option<(String,)> = sqlx::query_as(
            "SELECT listener_pod FROM signal WHERE token = $1 AND listener_pod IS NOT NULL",
        )
        .bind(&token)
        .fetch_optional(&pool)
        .await
        .expect("signal lookup");
        let pod_alive = listener_pod_exists(&pool, &pod).await;

        match (&signal_placed, pod_alive, &stamp_result) {
            // Stamp won: signal committed, pod survived the sweep.
            (Some(_), true, Ok(())) => {}
            // Reap won: pod gone, stamp refused, nothing committed.
            (None, false, Err(_)) => {}
            other => panic!(
                "round {round}: broken interleaving (signal_placed={:?}, pod_alive={}, \
                 stamp={:?})",
                other.0,
                other.1,
                stamp_result.as_ref().map(|_| ()),
            ),
        }
        // Reset for the next round (delete whichever side survived).
        sqlx::query("DELETE FROM signal WHERE token = $1")
            .bind(&token)
            .execute(&pool)
            .await
            .expect("cleanup signal");
        sqlx::query("DELETE FROM listener_pod WHERE pod_name = $1")
            .bind(&pod)
            .execute(&pool)
            .await
            .expect("cleanup pod");
    }
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
    projects.set_running_hashes(id, Some("binary"), Some("graph"), None, None).await.unwrap();
    assert_eq!(projects.program_source(id, &program).await.unwrap(), source);
    projects.set_running_hashes(id, Some("different-binary"), None, None, None).await.unwrap();
    assert!(projects.program_source(id, &program).await.is_err());
    let changed = weft_core::project::hash::ProgramIdentity { binary_hash: "different-binary".into(), ..program };
    assert!(projects.program_source(id, &changed).await.is_err(), "changing code cannot relabel old sources");
}

fn trigger_setup_birth(id: Uuid, color: Uuid) -> (weft_journal::ExecEvent, weft_task_store::tasks::NewTask) {
    let program = weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-1".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    };
    let start = weft_journal::ExecEvent::ExecutionStarted {
        color, project_id: id, entry_node: "entry".into(),
        phase: weft_core::context::Phase::TriggerSetup, definition_hash: Some("def-1".into()),
        program: Some(program), source_version: Some("source".into()), node_test: false, subgraph: None, seed: None, at_unix: 1,
    };
    let task = weft_dispatcher::task_kinds::execute::execution_task_spec(
        weft_task_store::TaskKind::Execute, id, color, "def-1", "bin-A", Some(TENANT), None, None,
    ).unwrap();
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
    let color = Uuid::new_v4();
    let (birth, task) = trigger_setup_birth(id, color);
    journal.start_execution(&birth, &[], task, None).await.unwrap();
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err());
    let complete = weft_journal::ExecEvent::ExecutionCompleted { color, at_unix: 2 };
    journal.record_event(&complete).await.unwrap();
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err(), "publication still owns this source");
    let bake = weft_dispatcher::journal::TriggerBake::from_events(&[birth, complete]).unwrap().unwrap();
    journal.finish_trigger_setup(color, Some(&bake)).await.unwrap();
    versions.delete_versions(id, &["source".into()]).await.unwrap();
    assert!(journal.trigger_bakes(id).await.unwrap().is_empty());
    assert!(versions.version(id, "source").await.unwrap().is_none());
}

#[sqlx::test]
async fn activation_cleanup_cannot_finish_or_wipe_a_newer_activation(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    seed_listener_pod(&pool, "activation-listener", "dispatcher").await;
    let placement = SignalPlacement { listener_pod: "activation-listener".into(), generation: 1 };
    let first = Uuid::new_v4();
    assert!(projects.try_begin_activating(id, first).await.unwrap());
    let mut entry = entry_signal("old-entry", id);
    entry.setup_color = Some(first);
    journal.signal_insert(&entry, &placement).await.unwrap();
    let removed = projects.end_activating(id, first,
        &weft_dispatcher::project_store::ProjectLifecycle::wiped(), true).await.unwrap().unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].token, "old-entry");
    assert!(journal.signal_get("old-entry").await.unwrap().is_none());
    let (late_birth, late_task) = trigger_setup_birth(id, first);
    assert!(journal.start_execution(&late_birth, &[], late_task, Some(first)).await.is_err(),
        "a cancelled activation cannot later start as a standalone bake");
    assert!(journal.events_log(first).await.unwrap().is_empty());

    let second = Uuid::new_v4();
    assert!(projects.try_begin_activating(id, second).await.unwrap());
    entry.token = "new-entry".into();
    entry.setup_color = Some(second);
    journal.signal_insert(&entry, &placement).await.unwrap();
    sqlx::query("UPDATE project SET activation_version = 'second-source' WHERE id = $1")
        .bind(id).execute(&pool).await.unwrap();
    for to in [weft_dispatcher::project_store::ProjectLifecycle::wiped(),
        weft_dispatcher::project_store::ProjectLifecycle::active()] {
        assert!(projects.end_activating(id, first, &to, true).await.unwrap().is_none());
        assert!(journal.signal_get("new-entry").await.unwrap().is_some());
        assert_eq!(projects.lifecycle(id).await.unwrap().unwrap().activating_ts_color, Some(second));
        let version: Option<String> = sqlx::query_scalar("SELECT activation_version FROM project WHERE id = $1")
            .bind(id).fetch_one(&pool).await.unwrap();
        assert_eq!(version.as_deref(), Some("second-source"));
    }
    assert!(projects.end_activating(id, second,
        &weft_dispatcher::project_store::ProjectLifecycle::active(), false).await.unwrap().is_some());
}

#[sqlx::test]
async fn trigger_bake_ownership_publication_and_project_cleanup(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    sqlx::query("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)")
        .bind(id).execute(&pool).await.unwrap();
    let first = Uuid::new_v4();
    let (birth, task) = trigger_setup_birth(id, first);
    journal.start_execution(&birth, &[], task, None).await.unwrap();
    assert!(!projects.try_begin_activating(id, Uuid::new_v4()).await.unwrap());
    let second = Uuid::new_v4();
    let (second_birth, second_task) = trigger_setup_birth(id, second);
    assert!(journal.start_execution(&second_birth, &[], second_task.clone(), None).await.is_err());
    assert!(journal.events_log(second).await.unwrap().is_empty());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE color = $1")
        .bind(second.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(count, 0, "a refused setup cannot leave queued work");
    let complete = weft_journal::ExecEvent::ExecutionCompleted { color: first, at_unix: 2 };
    journal.record_event(&complete).await.unwrap();
    let bake = weft_dispatcher::journal::TriggerBake::from_events(&[birth.clone(), complete]).unwrap().unwrap();
    journal.finish_trigger_setup(first, Some(&bake)).await.unwrap();
    assert!(projects.try_begin_activating(id, second).await.unwrap());
    let (other_birth, other_task) = trigger_setup_birth(id, Uuid::new_v4());
    assert!(journal.start_execution(&other_birth, &[], other_task, None).await.is_err());
    journal.start_execution(&second_birth, &[], second_task, Some(second)).await.unwrap();
    seed_listener_pod(&pool, "bake-listener", "dispatcher").await;
    let placement = SignalPlacement { listener_pod: "bake-listener".into(), generation: 1 };
    let mut entry = entry_signal("baked-entry", id);
    entry.program = Some(bake.program.clone());
    entry.source_version = Some(bake.source_version.clone());
    entry.setup_color = Some(first);
    assert!(journal.signal_insert(&entry, &placement).await.is_err(), "a superseded setup cannot arm");
    entry.setup_color = Some(second);
    journal.signal_insert(&entry, &placement).await.unwrap();
    let armed = journal.signal_get("baked-entry").await.unwrap().unwrap();
    assert_eq!(armed.program, entry.program);
    assert_eq!(armed.source_version, entry.source_version);
    assert_eq!(armed.setup_color, Some(second));
    projects.end_activating(id, second, &weft_dispatcher::project_store::ProjectLifecycle::active(), false).await.unwrap();
    assert!(journal.signal_insert(&entry, &placement).await.is_err(), "no late registration after activation ends");
    journal.finish_trigger_setup(second, None).await.unwrap();
    assert_eq!(journal.trigger_bakes(id).await.unwrap()[0].color, first);
    journal.delete_execution(first).await.unwrap();
    assert_eq!(journal.trigger_bakes(id).await.unwrap()[0].color, first);
    assert!(journal.trigger_bakes(Uuid::new_v4()).await.unwrap().is_empty());
    projects.remove(id).await.unwrap();
    assert!(journal.trigger_bakes(id).await.unwrap().is_empty());
}

/// The birth of an execution (`ExecutionStarted` + `execution_color` seed +
/// kicks + the execute task) is ONE transaction: a failure anywhere rolls
/// everything back. Witness: starting for a project with NO row fails the
/// seed's project check AFTER the ExecutionStarted insert already ran in the
/// same transaction; nothing may survive (no journal row, no color, no task).
/// Before the atomic birth, this exact failure left a journaled "ghost"
/// execution with no task, which nothing would ever run or reclaim.
#[sqlx::test]
async fn start_execution_birth_is_atomic(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let missing_project = Uuid::new_v4(); // never registered
    let color = weft_core::Color::new_v4();
    let now = 1_700_000_000u64;
    let start = weft_journal::ExecEvent::ExecutionStarted {
        color,
        project_id: missing_project,
        entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        program: None, source_version: None, node_test: false,
        subgraph: None,
        seed: None,
        at_unix: now,
    };
    let kick = weft_journal::ExecEvent::NodeKicked {
        color,
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
        dedup_key: Some(format!("{color}:execute")),
        color: Some(color.to_string()),
        tenant_id: Some(TENANT.into()),
        target_pod_name: None,
        binary_hash: None,
        payload: json!({}),
    };
    let err = journal
        .start_execution(&start, std::slice::from_ref(&kick), task.clone(), None)
        .await
        .expect_err("missing project must fail the birth");
    assert!(format!("{err:#}").contains("has no row"), "{err:?}");
    // NOTHING survives: the whole birth rolled back.
    let (events,): (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM exec_event WHERE color = $1")
            .bind(color.to_string())
            .fetch_one(&pool)
            .await
            .expect("count events");
    let (colors,): (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM execution_color WHERE color = $1")
            .bind(color.to_string())
            .fetch_one(&pool)
            .await
            .expect("count colors");
    let (tasks,): (i64,) = sqlx::query_as("SELECT COUNT(*)::bigint FROM task WHERE color = $1")
        .bind(color.to_string())
        .fetch_one(&pool)
        .await
        .expect("count tasks");
    assert_eq!((events, colors, tasks), (0, 0, 0), "a failed birth must leave nothing");

    // And the positive path: with the project registered, the SAME birth
    // commits everything together.
    let registered = Uuid::new_v4();
    seed_project(&projects, registered, "bin-A").await;
    let color2 = weft_core::Color::new_v4();
    let start2 = weft_journal::ExecEvent::ExecutionStarted {
        color: color2,
        project_id: registered,
        entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        program: None, source_version: None, node_test: false,
        subgraph: None,
        seed: None,
        at_unix: now,
    };
    let task2 = weft_task_store::tasks::NewTask {
        color: Some(color2.to_string()),
        dedup_key: Some(format!("{color2}:execute")),
        project_id: Some(registered),
        ..task
    };
    journal
        .start_execution(&start2, &[], task2.clone(), None)
        .await
        .expect("birth for a registered project");
    let (events2,): (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM exec_event WHERE color = $1")
            .bind(color2.to_string())
            .fetch_one(&pool)
            .await
            .expect("count events");
    let (tasks2,): (i64,) = sqlx::query_as("SELECT COUNT(*)::bigint FROM task WHERE color = $1")
        .bind(color2.to_string())
        .fetch_one(&pool)
        .await
        .expect("count tasks");
    assert_eq!((events2, tasks2), (1, 1), "a successful birth commits the event AND the task");

    sqlx::query("DELETE FROM task WHERE color = $1").bind(color2.to_string()).execute(&pool).await.unwrap();
    journal.start_execution(&start2, &[], task2.clone(), None).await.unwrap();
    let live_error = journal.start_live_execution(&start2, &[], task2, 0.8).await.unwrap_err();
    assert!(live_error.to_string().contains("already started"), "{live_error:#}");
    let births: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM exec_event WHERE color = $1 AND kind = 'execution_started'")
        .bind(color2.to_string()).fetch_one(&pool).await.unwrap();
    let tasks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE color = $1")
        .bind(color2.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!((births, tasks), (1, 0), "finished admission cannot create a second execution");
}

// =====================================================================
// Supervisor pool: pending-command gate + ghost-lease hygiene.
// =====================================================================

/// Recording fake for the supervisor backend: no processes, no kubectl.
#[derive(Default)]
struct FakeSupervisorBackend {
    stopped: std::sync::Mutex<Vec<String>>,
    spawned: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl weft_dispatcher::supervisor_pool::SupervisorBackend for FakeSupervisorBackend {
    async fn spawn(
        &self,
        pod_name: &str,
        _namespace: &str,
    ) -> anyhow::Result<weft_dispatcher::supervisor_pool::SupervisorHandle> {
        self.spawned.lock().unwrap().push(pod_name.to_string());
        Ok(weft_dispatcher::supervisor_pool::SupervisorHandle {
            admin_url: format!("http://{pod_name}.test:8080"),
        })
    }
    async fn stop(&self, pod_name: &str, _namespace: &str) -> anyhow::Result<()> {
        self.stopped.lock().unwrap().push(pod_name.to_string());
        Ok(())
    }
}

async fn seed_supervisor_pod(pool: &PgPool, pod_name: &str, owner: &str, past_grace: bool) {
    let now = weft_dispatcher::lease::now_unix();
    let grace = if past_grace { now - 60 } else { now + 60 };
    sqlx::query(
        "INSERT INTO supervisor_pod \
         (pod_name, admin_url, namespace, owner_pod_id, leased_until_unix, grace_until_unix) \
         VALUES ($1, 'http://x:8080', 'weft-system', $2, $3, $4)",
    )
    .bind(pod_name)
    .bind(owner)
    .bind(now + 60)
    .bind(grace)
    .execute(pool)
    .await
    .expect("seed supervisor_pod");
}

async fn seed_pending_command(pool: &PgPool, project_id: Uuid) -> i64 {
    let (id,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, issued_by_pod, issued_at_unix) \
         VALUES ($1, $2, 'svc', 'apply', 'test-pod', $3) RETURNING id",
    )
    .bind(TENANT)
    .bind(project_id)
    .bind(weft_dispatcher::lease::now_unix())
    .fetch_one(pool)
    .await
    .expect("seed pending command");
    id
}

async fn supervisor_pod_exists(pool: &PgPool, pod_name: &str) -> bool {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT pod_name FROM supervisor_pod WHERE pod_name = $1")
            .bind(pod_name)
            .fetch_optional(pool)
            .await
            .expect("query supervisor_pod");
    row.is_some()
}

/// Register a project AND give it a namespace, so its lifecycle commands
/// are claimable per the broker's precondition (`project_namespace <> ''`,
/// the same condition `pending_commands_exist` checks).
async fn seed_claimable_project(pool: &PgPool, projects: &weft_dispatcher::ProjectStore) -> Uuid {
    let id = Uuid::new_v4();
    seed_project(projects, id, "hash-x").await;
    sqlx::query("UPDATE project SET project_namespace = 'wft-test-ns' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .expect("set project namespace");
    id
}

/// A zero-owner past-grace supervisor is NOT idle while a claimable
/// lifecycle command sits pending: the command may have been issued after
/// the spawn site ran (worker cold start), and reaping the pod strands it
/// (the exact production incident: `weft infra start` timing out after
/// 120s). Once the command completes, the same pod is reaped. A DRAINING
/// pod is exempt from the gate: drain releases its work then relies on
/// the reap, so gating it would zombie the pod forever.
#[sqlx::test]
async fn reap_spares_the_pool_while_a_command_is_pending(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let backend = FakeSupervisorBackend::default();
    let sup = weft_dispatcher::supervisor_pool::SupervisorPool::new("weft-system".into());

    let project = seed_claimable_project(&pool, &projects).await;
    seed_supervisor_pod(&pool, "sup-1", "disp-1", true).await;
    let cmd = seed_pending_command(&pool, project).await;

    sup.reap_idle(&backend, &pool, "disp-1").await.expect("reap");
    assert!(
        supervisor_pod_exists(&pool, "sup-1").await,
        "pending command must keep the zero-owner pod alive"
    );

    // A draining pod is reaped even while the command pends.
    seed_supervisor_pod(&pool, "sup-drain", "disp-1", true).await;
    sqlx::query("UPDATE supervisor_pod SET draining = TRUE WHERE pod_name = 'sup-drain'")
        .execute(&pool)
        .await
        .expect("mark draining");
    sup.reap_idle(&backend, &pool, "disp-1").await.expect("reap");
    assert!(
        !supervisor_pod_exists(&pool, "sup-drain").await,
        "draining pods must not be protected by the pending gate"
    );
    assert!(
        supervisor_pod_exists(&pool, "sup-1").await,
        "the non-draining pod stays protected"
    );

    sqlx::query("UPDATE infra_lifecycle_command SET completed_at_unix = $1 WHERE id = $2")
        .bind(weft_dispatcher::lease::now_unix())
        .bind(cmd)
        .execute(&pool)
        .await
        .expect("complete command");
    sup.reap_idle(&backend, &pool, "disp-1").await.expect("reap");
    assert!(
        !supervisor_pod_exists(&pool, "sup-1").await,
        "with the command completed the idle pod is reaped"
    );
    assert_eq!(
        backend.stopped.lock().unwrap().as_slice(),
        ["sup-drain", "sup-1"]
    );
}

/// An EMPTY pool with a claimable pending command is stranded work:
/// nothing outside the `infra start` sync top spawns supervisors, and
/// that site already ran. `reconcile` (the reaper's sweep) must re-seed
/// the pool so the command gets claimed within one sweep. An UNCLAIMABLE
/// command (dispatcher-owned verb, or a project with no namespace) must
/// NOT trigger a spawn: a supervisor could never serve it.
#[sqlx::test]
async fn reconcile_reseeds_an_empty_pool_when_a_command_pends(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let backend = FakeSupervisorBackend::default();
    let sup = weft_dispatcher::supervisor_pool::SupervisorPool::new("weft-system".into());

    // Unclaimable pendings first: a dispatcher-owned verb on a claimable
    // project, and a supervisor verb on a namespace-less project.
    let claimable = seed_claimable_project(&pool, &projects).await;
    sqlx::query(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, issued_by_pod, issued_at_unix) \
         VALUES ($1, $2, NULL, 'deactivate', 'test-pod', $3)",
    )
    .bind(TENANT)
    .bind(claimable)
    .bind(weft_dispatcher::lease::now_unix())
    .execute(&pool)
    .await
    .expect("seed dispatcher-verb command");
    seed_pending_command(&pool, Uuid::new_v4()).await;
    sup.reconcile(&backend, &pool, "disp-1").await.expect("reconcile");
    assert!(
        backend.spawned.lock().unwrap().is_empty(),
        "unclaimable commands must not spawn a supervisor"
    );

    // A claimable supervisor-verb command does.
    seed_pending_command(&pool, claimable).await;
    sup.reconcile(&backend, &pool, "disp-1").await.expect("reconcile");
    assert_eq!(
        backend.spawned.lock().unwrap().len(),
        1,
        "empty pool + claimable pending command must spawn a supervisor"
    );
}

/// Project removal releases the project's `infra_owner` lease, and
/// `reconcile` drops any ghost lease left by older removals, so a
/// supervisor never renews ownership of a deleted project forever.
#[sqlx::test]
async fn removed_projects_do_not_keep_supervisor_leases(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let backend = FakeSupervisorBackend::default();
    let sup = weft_dispatcher::supervisor_pool::SupervisorPool::new("weft-system".into());

    // A live project with a lease, and a ghost lease for a project that
    // was never (or is no longer) registered.
    let live = Uuid::new_v4();
    seed_project(&projects, live, "hash-live").await;
    for (project, pod) in [(live, "sup-live"), (Uuid::new_v4(), "sup-ghost")] {
        sqlx::query(
            "INSERT INTO infra_owner (project_id, supervisor_pod, namespace, tenant_id, leased_until_unix) \
             VALUES ($1, $2, 'ns', $3, $4)",
        )
        .bind(project)
        .bind(pod)
        .bind(TENANT)
        .bind(weft_dispatcher::lease::now_unix() + 60)
        .execute(&pool)
        .await
        .expect("seed infra_owner");
    }

    sup.reconcile(&backend, &pool, "disp-1").await.expect("reconcile");
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
        weft_dispatcher::supervisor_pool::release_project(&pool, live)
            .await
            .expect("release");
    assert_eq!(released, 1, "project removal releases its lease");
}

/// The referenced-image set (`GET /images/referenced`) must cover a
/// project's current hash, the hash a still-alive (e.g. draining) worker
/// pod runs, the hash stamped on a pending/claimed task (a task can
/// outlive both the project pointer and its pod between a resync and the
/// cold-start sweep), every infra image ref in any project's tag map,
/// AND the refs recorded on live infra units (a unit left UP across a
/// sync stays frozen at its recorded image, which can be older than the
/// project's current map): `weft clean --images` deletes everything
/// outside this set, so any of them escaping it would be deleted out
/// from under a running workload. Terminal pods' and completed tasks'
/// hashes drop out; a project with no infra tags contributes none; a
/// blank ref is skipped (matches no image); a unit stamped before refs
/// were recorded contributes nothing.
#[sqlx::test]
async fn referenced_images_cover_projects_pods_tasks_maps_and_unit_refs(
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
         (project_id, node_id, instance_id, namespace, status, units_json) \
         VALUES ($1, 'n1', 'inst1', 'ns', 'running', $2)",
    )
    .bind(with_infra)
    .bind(serde_json::json!({
        "frozen": {
            "status": "running",
            "stop_behavior": { "kind": "no_op" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30,
            "image_refs": ["weft-infra-bridge:0ld-frozen"]
        },
        "legacy": {
            "status": "running",
            "stop_behavior": { "kind": "no_op" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30
        },
        "terminal": {
            "status": "stopped",
            "stop_behavior": { "kind": "scale_to_zero" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30,
            "image_refs": ["weft-infra-bridge:terminal-gone"]
        }
    }))
    .execute(&pool)
    .await
    .expect("seed infra_node units");
    for (pod, hash, terminal) in [
        ("wp-drain", "hash-draining", false),
        ("wp-done", "hash-terminal", true),
    ] {
        sqlx::query(
            "INSERT INTO worker_pod \
             (pod_name, project_id, namespace, status, owner_dispatcher, \
              last_heartbeat_unix, created_at_unix, terminal_at_unix, binary_hash) \
             VALUES ($1, gen_random_uuid(), 'ns', 'alive', 'disp-1', $2, $2, $3, $4)",
        )
        .bind(pod)
        .bind(now)
        .bind(if terminal { Some(now) } else { None })
        .bind(hash)
        .execute(&pool)
        .await
        .expect("seed worker_pod");
    }
    for (status, hash) in [
        ("pending", "hash-pending-task"),
        ("claimed", "hash-claimed-task"),
        ("complete", "hash-done-task"),
    ] {
        sqlx::query(
            "INSERT INTO task \
             (id, kind, target, project_id, status, binary_hash, payload, attempts, created_at_unix) \
             VALUES ($4, 'execute', 'worker', gen_random_uuid(), $1, $2, '{}'::jsonb, 0, $3)",
        )
        .bind(status)
        .bind(hash)
        .bind(now)
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .expect("seed task");
    }

    let referenced = weft_dispatcher::api::project::referenced_images_query(&pool)
        .await
        .expect("referenced set");
    let mut hashes = referenced.worker_hashes.clone();
    hashes.sort();
    assert_eq!(
        hashes,
        vec![
            "hash-claimed-task".to_string(),
            "hash-current".to_string(),
            "hash-draining".to_string(),
            "hash-infra-project".to_string(),
            "hash-pending-task".to_string(),
        ],
        "project + live pod + live task hashes in (pending AND claimed); \
         terminal pod and completed task hashes out"
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

/// The pick-then-register window: placement RE-ARMS the chosen pod's
/// grace before the listener HTTP round-trip, so the idle reaper firing
/// mid-placement declines instead of tearing the pod down under the
/// in-flight `/register` (a live incident: DNS failure dialing a pod the
/// reaper had deleted three seconds earlier). The register closure here
/// IS the race: it runs a full reap sweep while the placement is
/// mid-flight and asserts the pod survives it.
#[sqlx::test]
async fn placement_rearms_grace_so_a_midflight_reap_declines(pool: PgPool) {
    setup(&pool).await;
    // A stand-in listener answering only `/load` (never saturated), so
    // `pick_live` can choose the seeded pod without a real listener.
    let load_router = axum::Router::new().route(
        "/load",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({
                "saturated": false, "mem_pressure": 0.0, "signals": 0, "held_connections": 0
            }))
        }),
    );
    let load_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind load stub");
    let admin_url = format!("http://{}", load_listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        axum::serve(load_listener, load_router).await.expect("serve load stub");
    });

    // One live, zero-signal, PAST-GRACE pod: reapable the instant before
    // a placement picks it.
    let now = weft_dispatcher::lease::now_unix();
    sqlx::query(
        "INSERT INTO listener_pod \
         (pod_name, admin_url, namespace, owner_pod_id, leased_until_unix, grace_until_unix) \
         VALUES ('listener-est', $1, 'weft-system', 'disp-1', $2, $3)",
    )
    .bind(&admin_url)
    .bind(now + 3600)
    .bind(now - 1)
    .execute(&pool)
    .await
    .expect("seed listener_pod");

    let backend = FakeBackend::default();
    let listeners = ListenerPool::new("weft-system".to_string());
    let (pod_name, ()) = listeners
        .place_signal(&backend, &pool, "disp-1", |_handle| {
            let pool = pool.clone();
            let backend = &backend;
            let listeners = &listeners;
            async move {
                // The reaper fires while the register round-trip is in
                // flight. The re-armed grace must make it decline.
                listeners.reap_idle(backend, &pool, "disp-1").await?;
                Ok(())
            }
        })
        .await
        .expect("placement survives a mid-flight reap");
    assert_eq!(pod_name, "listener-est");
    assert!(
        listener_pod_exists(&pool, "listener-est").await,
        "the re-armed pod must survive the mid-placement reap sweep"
    );
    assert!(
        backend.stopped.lock().unwrap().is_empty(),
        "nothing may be torn down while the placement is in flight"
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
    }
}

/// Seed one signal row for `token`, placed on the `listener-park` pod
/// the caller seeds once per test. Entry rows are keyed by
/// `(project_id, node_id)`, so the node id is derived from the token:
/// many tokens per project, no collisions.
async fn seed_parked_signal(journal: &PostgresJournal, token: &str, project: Uuid) {
    let mut sig = entry_signal(token, project);
    sig.node_id = format!("trigger-{token}");
    journal
        .signal_insert(
            &sig,
            &SignalPlacement { listener_pod: "listener-park".to_string(), generation: 1 },
        )
        .await
        .expect("seed signal row");
}

/// Consuming a resume token deletes its row and hands the row back
/// naming the pod that held it, so the in-RAM unregister that follows
/// the DELETE knows where to go. An entry row is not consumed.
#[sqlx::test]
async fn consume_suspension_returns_the_deleted_row_with_its_holder(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_listener_pod(&pool, "listener-park", "disp-1").await;
    seed_parked_signal(&journal, "tok-entry", project).await;
    let mut resume = entry_signal("tok-resume", project);
    resume.is_resume = true;
    journal
        .signal_insert(
            &resume,
            &SignalPlacement { listener_pod: "listener-park".to_string(), generation: 1 },
        )
        .await
        .expect("seed resume signal");

    let consumed = journal.consume_suspension("tok-resume").await.unwrap().expect("the resume row");
    assert_eq!(consumed.token, "tok-resume");
    assert_eq!(consumed.listener_pod.as_deref(), Some("listener-park"));
    assert!(journal.signal_get("tok-resume").await.unwrap().is_none(), "single use");
    assert!(journal.consume_suspension("tok-resume").await.unwrap().is_none());

    assert!(journal.consume_suspension("tok-entry").await.unwrap().is_none(), "entry rows stay");
    let entry = journal.signal_get("tok-entry").await.unwrap().expect("entry row kept");
    assert_eq!(entry.listener_pod.as_deref(), Some("listener-park"), "a read names the holder");
}

/// The consumer listing decodes the same row shape as every journal
/// read, so a column added to the row reaches this query too: one
/// visible entry signal lists, naming its holder.
#[sqlx::test]
async fn the_consumer_listing_reads_the_whole_signal_row(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    sqlx::query("UPDATE project SET fires_visible_to_consumers = TRUE WHERE id = $1")
        .bind(project)
        .execute(&pool)
        .await
        .expect("show fires to consumers");
    seed_listener_pod(&pool, "listener-park", "disp-1").await;
    seed_parked_signal(&journal, "tok-entry", project).await;

    let listed = signals_visible_to(&pool, TENANT, &[], &[]).await.expect("listing decodes");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].token, "tok-entry");
    assert_eq!(listed[0].listener_pod.as_deref(), Some("listener-park"));
    assert!(signals_visible_to(&pool, "someone-else", &[], &[]).await.unwrap().is_empty());
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
    seed_listener_pod(&pool, "listener-park", "disp-1").await;
    seed_parked_signal(&journal, "tok-entry", project).await;
    let mut resume = entry_signal("tok-resume", project);
    resume.is_resume = true;
    journal
        .signal_insert(
            &resume,
            &SignalPlacement { listener_pod: "listener-park".to_string(), generation: 1 },
        )
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
async fn the_sweep_selects_only_due_heads_on_active_projects(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let active = Uuid::new_v4();
    let inactive = Uuid::new_v4();
    seed_project(&projects, active, "bin-A").await;
    seed_project(&projects, inactive, "bin-B").await;
    // A fresh register is 'registered', one step below Active; only the
    // first project is promoted.
    sqlx::query("UPDATE project SET status = 'active' WHERE id = $1")
        .bind(active)
        .execute(&pool)
        .await
        .expect("promote one project");
    seed_listener_pod(&pool, "listener-park", "disp-1").await;

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
        "UPDATE signal SET drain_claimed_at_unix = $1, drain_claimed_by = 'pod-x' \
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
        "due heads on Active projects only; a backing-off head, a claimed row, \
         and a non-Active project's queue are all left alone"
    );
}

/// Claims older than the threshold release (both columns); a fresh
/// claim survives. The sweep depends on this: it is the only thing that
/// re-drives an Active project's queue, so a crashed pod's stale claim
/// must not starve the token's retries until the next activate.
#[sqlx::test]
async fn stale_drain_claims_release_and_fresh_ones_survive(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_listener_pod(&pool, "listener-park", "disp-1").await;
    seed_parked_signal(&journal, "tok-stale", project).await;
    seed_parked_signal(&journal, "tok-fresh", project).await;
    let now = weft_dispatcher::lease::now_unix();
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = $1, drain_claimed_by = 'pod-x' \
         WHERE token = 'tok-stale'",
    )
    .bind(now - 301)
    .execute(&pool)
    .await
    .expect("seed a stale claim");
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = $1, drain_claimed_by = 'pod-y' \
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
        (Some(now), Some("pod-y".to_string())),
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
    seed_listener_pod(&pool, "listener-park", "disp-1").await;
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

/// A supervisor pod that is gone from the cluster while still owning
/// projects used to be invisible to every sweep: `renew_owned` kept its
/// lease fresh for as long as the dispatcher ran, and `reap_idle` only
/// ever considers pods owning NOTHING. So its projects stayed leased to
/// a pod that could never claim them, and every `weft infra start` on
/// one waited for ever on an apply nothing would execute.
///
/// It is not a rare shape: `setup.sh` rebuilds the kind node whenever
/// the cluster's shape changes, which destroys every supervisor at once
/// and leaves the rows behind. That is how this was found.
#[sqlx::test]
async fn a_supervisor_the_cluster_no_longer_has_is_forgotten_and_its_projects_released(
    pool: PgPool,
) {
    let (_journal, projects) = setup(&pool).await;
    let sup = weft_dispatcher::supervisor_pool::SupervisorPool::new("weft-system".into());
    let project = seed_claimable_project(&pool, &projects).await;

    seed_supervisor_pod(&pool, "sup-gone", "disp-1", true).await;
    seed_supervisor_pod(&pool, "sup-live", "disp-1", true).await;
    sqlx::query(
        "INSERT INTO infra_owner \
         (project_id, supervisor_pod, namespace, tenant_id, leased_until_unix) \
         VALUES ($1, 'sup-gone', 'weft-system', $2, $3)",
    )
    .bind(project)
    .bind(TENANT)
    .bind(weft_dispatcher::lease::now_unix() + 60)
    .execute(&pool)
    .await
    .expect("seed infra_owner");

    // The cluster answers with one of the two Deployments.
    let kube = weft_platform_traits::FakeKube::new();
    kube.set_workloads(
        "weft-system",
        vec![weft_platform_traits::WorkloadReplicaState {
            kind: weft_platform_traits::WorkloadKind::Deployment,
            name: "sup-live".into(),
            namespace: "weft-system".into(),
            desired: 1,
            ready: 1,
            // The label the pool's selector reads; the fake honours it
            // the way kubectl's `-l` does.
            labels: [("weft.dev/role".to_string(), "infra-supervisor".to_string())]
                .into_iter()
                .collect(),
        }],
    );

    let forgotten = sup
        .forget_vanished(kube.as_ref(), &pool, "disp-1")
        .await
        .expect("forget");
    assert_eq!(forgotten, 1);
    assert!(!supervisor_pod_exists(&pool, "sup-gone").await, "the vanished pod is forgotten");
    assert!(supervisor_pod_exists(&pool, "sup-live").await, "the live one is untouched");

    // Ownership is a lease, not the infrastructure: releasing it moves
    // who is responsible so a live supervisor adopts the project, and
    // touches nothing that is running.
    let owners: Vec<(String,)> =
        sqlx::query_as("SELECT supervisor_pod FROM infra_owner WHERE project_id = $1")
            .bind(project)
            .fetch_all(&pool)
            .await
            .expect("query infra_owner");
    assert!(owners.is_empty(), "the project is free to be claimed again: {owners:?}");

    // Idempotent: a second sweep with nothing vanished changes nothing.
    assert_eq!(sup.forget_vanished(kube.as_ref(), &pool, "disp-1").await.expect("again"), 0);
}

/// Journal a fresh execution for `project`, the way production does
/// (the `execution_color` index row rides the same transaction).
async fn start_execution(journal: &PostgresJournal, project: Uuid) -> weft_core::Color {
    let color = weft_core::Color::new_v4();
    journal
        .record_event(&weft_journal::ExecEvent::ExecutionStarted {
            color,
            project_id: project,
            entry_node: "start".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("def-1".into()),
            program: None,
            node_test: false,
            source_version: None,
            subgraph: None,
            seed: None,
            at_unix: 1,
        })
        .await
        .expect("ExecutionStarted");
    color
}

async fn count(pool: &PgPool, sql: &str, color: weft_core::Color) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .bind(color.to_string())
        .fetch_one(pool)
        .await
        .expect("count")
}

/// Everywhere one execution lives, so a table dropped from the erase
/// list cannot ship quietly.
async fn footprint(pool: &PgPool, color: weft_core::Color) -> i64 {
    count(pool, "SELECT COUNT(*) FROM exec_event WHERE color = $1", color).await
        + count(pool, "SELECT COUNT(*) FROM execution_color WHERE color = $1", color).await
        + count(pool, "SELECT COUNT(*) FROM execution_tag WHERE color = $1", color).await
        + count(pool, "SELECT COUNT(*) FROM trigger_setup WHERE color = $1", color).await
        + count(pool, "SELECT COUNT(*) FROM signal WHERE color = $1 AND is_resume = TRUE", color)
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

    // Everything else a run leaves behind, on the first colour.
    let mut tx = pool.begin().await.unwrap();
    weft_journal::tags::tag_execution_in(&mut tx, first, &["user_7".to_string()], 10, None)
        .await
        .expect("tag");
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO trigger_setup (project_id, color) VALUES ($1, $2)")
        .bind(doomed)
        .bind(first.to_string())
        .execute(&pool)
        .await
        .expect("trigger_setup");
    for (token, color, is_resume) in [
        ("resume-tok", Some(first), true),
        ("entry-tok", None, false),
    ] {
        sqlx::query(
            "INSERT INTO signal \
             (token, tenant_id, project_id, color, node_id, is_resume, spec_json, created_at) \
             VALUES ($1, $2, $3, $4, 'wait', $5, '{}', 1)",
        )
        .bind(token)
        .bind(TENANT)
        .bind(doomed)
        .bind(color.map(|c| c.to_string()))
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

/// Register an alive worker pod for `project`, the way a spawned worker
/// does once it boots.
async fn seed_worker_pod(pool: &PgPool, pod_name: &str, project: Uuid, binary_hash: &str) {
    let now = weft_dispatcher::lease::now_unix();
    sqlx::query(
        "INSERT INTO worker_pod \
         (pod_name, project_id, namespace, status, owner_dispatcher, \
          last_heartbeat_unix, created_at_unix, binary_hash) \
         VALUES ($1, $2, 'weft-workers', 'alive', 'disp-1', $3, $3, $4)",
    )
    .bind(pod_name)
    .bind(project)
    .bind(now)
    .bind(binary_hash)
    .execute(pool)
    .await
    .expect("seed worker_pod");
}

async fn pod_status(pool: &PgPool, pod_name: &str) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM worker_pod WHERE pod_name = $1")
        .bind(pod_name)
        .fetch_one(pool)
        .await
        .expect("worker_pod status")
}

/// A worker promised to a caller who has not arrived yet does not shut
/// itself down, and goes as soon as the promise runs out.
///
/// This is the whole of what keeps a slow caller's ticket good. Nothing
/// is queued when a caller is handed a ticket, by design, so every
/// other query says the pod has no work and a pod with no work exits in
/// half a minute. Before the promise existed, a caller on a bad
/// connection followed a redirect to a machine that had already gone,
/// holding a ticket that was still perfectly valid for another minute
/// and a half.
#[sqlx::test]
async fn a_worker_promised_to_a_caller_does_not_shut_itself_down(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_worker_pod(&pool, "wp-live", project, "bin-A").await;
    let store = weft_task_store::PostgresWorkerPodClient::new(pool.clone());

    // With nothing promised and nothing queued, the pod is free to go.
    // That is the behaviour the promise has to override, so prove it
    // first: otherwise the test below passes on a pod that could never
    // have exited anyway.
    assert!(
        weft_task_store::worker_pod::mark_done_if_idle(&pool, "wp-live").await.expect("cas"),
        "an idle pod with nothing promised shuts itself down"
    );

    // A second pod, this time reserved for a caller.
    seed_worker_pod(&pool, "wp-held", project, "bin-A").await;
    let now = weft_dispatcher::lease::now_unix();
    let reserved = weft_task_store::worker_pod::reserve_pod_for_caller(
        &pool,
        project,
        weft_platform_traits::SATURATION_MEM_FRACTION,
        Some("bin-A"),
        now + 240,
    )
    .await
    .expect("reserve")
    .expect("a pod is available to reserve");
    assert_eq!(reserved.0, "wp-held", "the only pod still alive is the one reserved");

    assert!(
        !weft_task_store::worker_pod::mark_done_if_idle(&pool, "wp-held").await.expect("cas"),
        "a pod promised to a caller stays up"
    );
    assert_eq!(pod_status(&pool, "wp-held").await, "alive");

    // The promise expires by itself: nothing clears it, and the next
    // time the pod asks, it goes.
    sqlx::query("UPDATE worker_pod SET held_until_unix = $2 WHERE pod_name = $1")
        .bind("wp-held")
        .bind(now - 1)
        .execute(&pool)
        .await
        .expect("age the promise");
    assert!(
        weft_task_store::worker_pod::mark_done_if_idle(&pool, "wp-held").await.expect("cas"),
        "once the promise has run out the pod is free to go"
    );
    assert_eq!(pod_status(&pool, "wp-held").await, "done");

    // The trait the worker itself calls goes through the same CAS.
    assert!(
        !weft_task_store::WorkerPodClient::mark_done_if_idle(&store, "wp-held")
            .await
            .expect("cas"),
        "a pod already done never flips twice"
    );
}

/// Choosing the pod and promising it to the caller are one act.
///
/// A caller is about to be handed a ticket naming this exact pod, and
/// nothing else in the system knows they are coming, so a pod chosen
/// but not yet promised is free to leave in the gap. Two callers
/// pointed at one pod both extend the promise, and neither ever cuts
/// it short.
#[sqlx::test]
async fn reserving_a_pod_promises_it_and_never_shortens_the_promise(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_worker_pod(&pool, "wp-one", project, "bin-A").await;
    let now = weft_dispatcher::lease::now_unix();

    let held_until = |pod: &str| {
        let pool = pool.clone();
        let pod = pod.to_string();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT held_until_unix FROM worker_pod WHERE pod_name = $1",
            )
            .bind(pod)
            .fetch_one(&pool)
            .await
            .expect("held_until_unix")
        }
    };
    assert_eq!(held_until("wp-one").await, 0, "a fresh pod is promised to nobody");

    let project_id = project;
    let reserve = |until: i64| {
        let (pool, project_id) = (pool.clone(), project_id);
        async move {
            weft_task_store::worker_pod::reserve_pod_for_caller(
                &pool,
                project_id,
                weft_platform_traits::SATURATION_MEM_FRACTION,
                Some("bin-A"),
                until,
            )
            .await
        }
    };
    assert_eq!(reserve(now + 240).await.expect("reserve").unwrap().0, "wp-one");
    assert_eq!(held_until("wp-one").await, now + 240);

    // A caller arriving later pushes the promise out.
    assert!(reserve(now + 300).await.expect("reserve").is_some());
    assert_eq!(held_until("wp-one").await, now + 300);

    // One arriving on the heels of the first does not pull it back in.
    assert!(reserve(now + 250).await.expect("reserve").is_some());
    assert_eq!(held_until("wp-one").await, now + 300, "a promise is never shortened");

    // A pod built from another program is not this caller's to take,
    // and there is nothing else: the handshake spawns instead.
    assert!(
        reserve_other(&pool, project, now + 240).await.is_none(),
        "a pod running a different program cannot serve this caller"
    );
}

async fn reserve_other(pool: &PgPool, project: Uuid, until: i64) -> Option<(String, String)> {
    weft_task_store::worker_pod::reserve_pod_for_caller(
        pool,
        project,
        weft_platform_traits::SATURATION_MEM_FRACTION,
        Some("bin-OTHER"),
        until,
    )
    .await
    .expect("reserve")
}

/// A removed project's queued work and workers are found for clearing;
/// a live project's never are, and neither is a node-test pod (its
/// scratch project is never a row).
#[sqlx::test]
async fn a_removed_projects_work_and_workers_are_cleared_and_nothing_else(pool: PgPool) {
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
                    color: None,
                    tenant_id: Some(TENANT.into()),
                    target_pod_name: None,
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
            color: None,
            tenant_id: Some(TENANT.into()),
            target_pod_name: None,
            binary_hash: None,
            payload: json!({}),
        },
    )
    .await
    .unwrap();
    for (pod, project, role) in [("wp-live", live, "worker"), ("wp-gone", removed, "worker"), ("nt-gone", removed, "node-test")] {
        weft_task_store::worker_pod::insert_spawning(&pool, pod, project, "ns", "d", Some("bin"), role, None)
            .await
            .unwrap();
    }

    assert_eq!(weft_dispatcher::reaper::drop_work_of_removed_projects(&pool).await.unwrap(), 1);
    let left: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT project_id, dedup_key FROM task ORDER BY dedup_key").fetch_all(&pool).await.unwrap();
    assert_eq!(
        left,
        vec![(live, "a".to_string()), (removed, "claimed".to_string()), (removed, "dispatcher".to_string())]
    );
    let workers: Vec<String> = weft_dispatcher::reaper::workers_of_removed_projects(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.pod_name)
        .collect();
    assert_eq!(workers, vec!["wp-gone".to_string()]);
}

/// A spawn queued for a project removed since completes as a no-op: a
/// worker brought up for it would only be killed by the removed-projects
/// sweep. A live project with no pod still spawns.
#[sqlx::test]
async fn a_spawn_for_a_removed_project_does_nothing(pool: PgPool) {
    use weft_dispatcher::task_kinds::spawn_pod::{nothing_to_spawn, SKIP_PROJECT_REMOVED};
    let (_journal, projects) = setup(&pool).await;
    let live = Uuid::from_u128(1);
    seed_project(&projects, live, "bin").await;
    assert_eq!(nothing_to_spawn(&pool, live, "bin").await.unwrap(), None);
    assert_eq!(
        nothing_to_spawn(&pool, Uuid::from_u128(2), "bin").await.unwrap(),
        Some(SKIP_PROJECT_REMOVED)
    );
}
