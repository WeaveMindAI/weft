//! Layer-3 tests for the dispatcher's OWN SQL, against a REAL Postgres.
//! The dispatcher's correctness-critical decisions (task stamping, the
//! listener reap-vs-placement lock discipline) live in SQL statements the
//! mock stores never execute, so a faked layer cannot catch their bugs;
//! two escaped to the live cluster before this rig existed (a `uuid =
//! text` bind error in the enqueue stamp, and the reap/placement
//! write-skew). These tests exercise the actual statements.
//!
//! Each test gets a fresh isolated database via `#[sqlx::test]` (it reads
//! `$DATABASE_URL`, creates a random DB, drops it after). When
//! `DATABASE_URL` is unset the macro skips the test, so a dev box without
//! Postgres still builds. The schema is the REAL migration path in
//! production order: journal first (`PostgresJournal::from_pool`), then
//! `app::run_core_migrations` (exactly what a booting dispatcher runs).
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
use weft_dispatcher::journal::postgres::PostgresJournal;
use weft_dispatcher::journal::{Journal, SignalPlacement, SignalRegistration};
use weft_dispatcher::listener::{ListenerBackend, ListenerHandle, ListenerPool};

const TENANT: &str = "tenant-1";

/// The real boot-time migration path: journal schema first (production
/// runs it at journal connect), then every core table in dependency
/// order. Returns the journal + the project store, the two handles the
/// SQL under test goes through.
async fn setup(pool: &PgPool) -> (PostgresJournal, weft_dispatcher::ProjectStore) {
    let journal: PostgresJournal = PostgresJournal::from_pool(pool.clone())
        .await
        .expect("journal schema");
    let projects = weft_dispatcher::app::run_core_migrations(pool)
        .await
        .expect("core migrations");
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
        token: token.to_string(),
        tenant_id: TENANT.to_string(),
        project_id: project_id.to_string(),
        color: None,
        node_id: "feed".to_string(),
        is_resume: false,
        spec_json: "{}".to_string(),
        consumer_kind: None,
        tags: Vec::new(),
        consumer_payload: None,
        surface_kind: "task_callback".to_string(),
        mount_path: None,
        auth_kind: "none".to_string(),
        auth_config: None,
        kind_state: json!({}),
        access_id: None,
        port_snapshot: None,
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

/// `enqueue_execute` stamps the task row with the project's CURRENT
/// `running_binary_hash` (read from the real `project` table: this is
/// the statement that shipped with a `uuid = text` bind error and only
/// failed on the live cluster).
#[sqlx::test]
async fn enqueue_execute_stamps_the_current_image(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;

    let color = weft_core::Color::new_v4();
    weft_dispatcher::task_kinds::execute::enqueue_execute(
        &pool,
        &id.to_string(),
        color,
        "def-1",
        Some(TENANT),
    )
    .await
    .expect("enqueue_execute");

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
        project_id: missing_project.to_string(),
        entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: "def-1".into(),
        at_unix: now,
    };
    let kick = weft_journal::ExecEvent::NodeKicked {
        color,
        node_id: "entry".into(),
        firing: true,
        payload: None,
        port_snapshot: None,
        at_unix: now,
    };
    let task = weft_task_store::tasks::NewTask {
        kind: weft_task_store::TaskKind::Execute.into(),
        target: weft_task_store::TaskTarget::Worker,
        project_id: Some(missing_project.to_string()),
        dedup_key: Some(format!("{color}:execute")),
        color: Some(color.to_string()),
        tenant_id: Some(TENANT.into()),
        target_pod_name: None,
        binary_hash: None,
        payload: json!({}),
    };
    let err = journal
        .start_execution(&start, std::slice::from_ref(&kick), task.clone())
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
        project_id: registered.to_string(),
        entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: "def-1".into(),
        at_unix: now,
    };
    let task2 = weft_task_store::tasks::NewTask {
        color: Some(color2.to_string()),
        dedup_key: Some(format!("{color2}:execute")),
        project_id: Some(registered.to_string()),
        ..task
    };
    journal
        .start_execution(&start2, &[], task2)
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

async fn seed_pending_command(pool: &PgPool, project_id: &str) -> i64 {
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
    let cmd = seed_pending_command(&pool, &project.to_string()).await;

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
    .bind(claimable.to_string())
    .bind(weft_dispatcher::lease::now_unix())
    .execute(&pool)
    .await
    .expect("seed dispatcher-verb command");
    seed_pending_command(&pool, &Uuid::new_v4().to_string()).await;
    sup.reconcile(&backend, &pool, "disp-1").await.expect("reconcile");
    assert!(
        backend.spawned.lock().unwrap().is_empty(),
        "unclaimable commands must not spawn a supervisor"
    );

    // A claimable supervisor-verb command does.
    seed_pending_command(&pool, &claimable.to_string()).await;
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
    for (project, pod) in [(live.to_string(), "sup-live"), (Uuid::new_v4().to_string(), "sup-ghost")] {
        sqlx::query(
            "INSERT INTO infra_owner (project_id, supervisor_pod, namespace, tenant_id, leased_until_unix) \
             VALUES ($1, $2, 'ns', $3, $4)",
        )
        .bind(&project)
        .bind(pod)
        .bind(TENANT)
        .bind(weft_dispatcher::lease::now_unix() + 60)
        .execute(&pool)
        .await
        .expect("seed infra_owner");
    }

    sup.reconcile(&backend, &pool, "disp-1").await.expect("reconcile");
    let leases: Vec<(String,)> = sqlx::query_as("SELECT project_id FROM infra_owner")
        .fetch_all(&pool)
        .await
        .expect("list leases");
    assert_eq!(
        leases,
        vec![(live.to_string(),)],
        "ghost lease dropped, live project's lease kept"
    );

    let released =
        weft_dispatcher::supervisor_pool::release_project(&pool, &live.to_string())
            .await
            .expect("release");
    assert_eq!(released, 1, "project removal releases its lease");
}

/// The referenced-image set (`GET /images/referenced`) must cover a
/// project's current hash, the hash a still-alive (e.g. draining) worker
/// pod runs, AND the hash stamped on a pending/claimed task (a task can
/// outlive both the project pointer and its pod between a resync and the
/// cold-start sweep): `weft clean --images` deletes everything outside
/// this set, so any of them escaping it would be deleted out from under a
/// running workload. Terminal pods' and completed tasks' hashes drop out.
#[sqlx::test]
async fn referenced_images_cover_projects_live_pods_and_live_tasks(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let now = weft_dispatcher::lease::now_unix();
    seed_project(&projects, Uuid::new_v4(), "hash-current").await;
    for (pod, hash, terminal) in [
        ("wp-drain", "hash-draining", false),
        ("wp-done", "hash-terminal", true),
    ] {
        sqlx::query(
            "INSERT INTO worker_pod \
             (pod_name, project_id, namespace, status, owner_dispatcher, \
              last_heartbeat_unix, created_at_unix, terminal_at_unix, binary_hash) \
             VALUES ($1, 'p', 'ns', 'alive', 'disp-1', $2, $2, $3, $4)",
        )
        .bind(pod)
        .bind(now)
        .bind(if terminal { Some(now) } else { None })
        .bind(hash)
        .execute(&pool)
        .await
        .expect("seed worker_pod");
    }
    for (status, hash) in [("pending", "hash-pending-task"), ("complete", "hash-done-task")] {
        sqlx::query(
            "INSERT INTO task \
             (id, kind, target, project_id, status, binary_hash, payload, attempts, created_at_unix) \
             VALUES ($4, 'execute', 'worker', 'p', $1, $2, '{}'::jsonb, 0, $3)",
        )
        .bind(status)
        .bind(hash)
        .bind(now)
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .expect("seed task");
    }

    let mut hashes = weft_dispatcher::api::project::referenced_image_hashes(&pool)
        .await
        .expect("referenced set");
    hashes.sort();
    assert_eq!(
        hashes,
        vec![
            "hash-current".to_string(),
            "hash-draining".to_string(),
            "hash-pending-task".to_string(),
        ],
        "project + live pod + live task hashes in; terminal pod and \
         completed task hashes out"
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

