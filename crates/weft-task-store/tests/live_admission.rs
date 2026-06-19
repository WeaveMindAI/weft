//! Layer-3 tests for the live-connection capacity + cleanup logic, run
//! against a REAL Postgres. The bugs this logic had (a cap race, a wrong
//! integer decode, a JSONB predicate, an admit/insert window) all lived IN
//! the SQL, so a faked store would not catch them; these exercise the actual
//! queries.
//!
//! Each test gets a fresh isolated database via `#[sqlx::test]` (it reads
//! `$DATABASE_URL`, creates a random DB, drops it after). When `DATABASE_URL`
//! is unset the macro skips the test, so a dev box without Postgres still
//! builds and `cargo test` passes; CI sets `DATABASE_URL` to run them.
//!
//! The schema is the crate's own `migrate()` (no sqlx migration files), so
//! every test runs both `tasks::migrate` and `worker_pod::migrate` first.
//!
//! Gated behind the `db-tests` feature (off by default) so a plain
//! `cargo test --workspace` needs no Postgres; CI runs
//! `cargo test -p weft-task-store --features db-tests` with `$DATABASE_URL` set.
#![cfg(feature = "db-tests")]

use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use weft_task_store::tasks::{
    self, admit_live_execution, claim_one, delete_pending_live_execution, reclaim_orphaned_tasks,
    ClaimFilter, SetupFailureOutcome,
};
use weft_task_store::{TaskKind, TaskTarget};
use weft_task_store::worker_pod::{
    self, has_live_for_project, mark_dead, mark_done_if_idle, register_alive,
};

const PROJECT: &str = "proj-1";
const TENANT: Option<&str> = Some("tenant-1");
const CAP: i32 = 2;

async fn setup(pool: &PgPool) {
    tasks::migrate(pool).await.expect("tasks schema");
    // `worker_pod::migrate` creates a fencing trigger ON `exec_event`, a
    // table owned by the dispatcher's journal layer (created at journal
    // connect, which runs before task-store migrate in production). These
    // task-store tests never touch the journal, so we stand up a minimal
    // `exec_event` table (matching the dispatcher's columns) purely so the
    // trigger DDL has a table to attach to. SYNC: keep these columns in step
    // with crates/weft-dispatcher/src/journal/postgres.rs `exec_event`.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS exec_event (
            id BIGSERIAL PRIMARY KEY,
            color TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            pod_name TEXT,
            dedup_key TEXT
        )"#,
    )
    .execute(pool)
    .await
    .expect("exec_event stub");
    worker_pod::migrate(pool).await.expect("worker_pod schema");
}

/// Insert an alive worker pod for the project.
async fn alive_pod(pool: &PgPool, pod: &str) {
    worker_pod::insert_spawning(pool, pod, PROJECT, "ns-1", "disp-1", "bin-1")
        .await
        .expect("insert_spawning");
    register_alive(pool, pod, PROJECT)
        .await
        .expect("register_alive");
}

/// A live-execution payload carrying a non-null `live_connection` (the shape
/// the admission/orphan/load predicates all key on).
fn live_payload(color: &str) -> Value {
    json!({
        "project_id": PROJECT,
        "color": color,
        "definition_hash": "hash-1",
        "live_connection": { "kind": "live_socket", "config": {} }
    })
}

/// Count in-flight (pending/claimed) live-execute tasks pinned to a pod, the
/// same definition of "live load" the admission SQL uses. Read directly so
/// the assertions are independent of the production query.
async fn live_load(pool: &PgPool, pod: &str) -> i64 {
    let (n,): (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*)::bigint FROM task
           WHERE target_pod_name = $1 AND kind = 'execute'
             AND status IN ('pending','claimed')
             AND payload -> 'live_connection' IS NOT NULL
             AND payload -> 'live_connection' != 'null'::jsonb"#,
    )
    .bind(pod)
    .fetch_one(pool)
    .await
    .expect("count");
    n
}

// ----- admission ------------------------------------------------------------

/// A single admission inserts one pinned execute task on the (only) pod and
/// reports it.
#[sqlx::test]
async fn admit_inserts_pinned_task(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();

    let admitted = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit")
        .expect("a pod was chosen");
    assert_eq!(admitted.pod_name, "pod-a");
    assert_eq!(admitted.namespace, "ns-1");
    assert_eq!(live_load(&pool, "pod-a").await, 1, "one slot taken");
}

/// The cap is enforced: with cap=2 and one pod, a third admission returns
/// `None` (the dispatcher would then spawn another pod).
#[sqlx::test]
async fn admit_enforces_cap(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;

    for _ in 0..CAP {
        let color = Uuid::new_v4().to_string();
        let got = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
            .await
            .expect("admit");
        assert!(got.is_some(), "admit under cap succeeds");
    }
    assert_eq!(live_load(&pool, "pod-a").await, CAP as i64);

    let color = Uuid::new_v4().to_string();
    let over = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit");
    assert!(over.is_none(), "admission past the cap is refused");
    assert_eq!(live_load(&pool, "pod-a").await, CAP as i64, "no overshoot");
}

/// cap == 0 means unbounded: admissions keep succeeding past any number.
#[sqlx::test]
async fn admit_cap_zero_is_unbounded(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    for _ in 0..5 {
        let color = Uuid::new_v4().to_string();
        let got = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), 0)
            .await
            .expect("admit");
        assert!(got.is_some(), "cap=0 never refuses");
    }
    assert_eq!(live_load(&pool, "pod-a").await, 5);
}

/// Least-loaded-first: with two pods of unequal load, the next admission goes
/// to the emptier one.
#[sqlx::test]
async fn admit_picks_least_loaded(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    alive_pod(&pool, "pod-b").await;
    // Load pod-a once (cap high so both are candidates).
    let c0 = Uuid::new_v4().to_string();
    let first = admit_live_execution(&pool, PROJECT, &c0, TENANT, &live_payload(&c0), 10)
        .await
        .expect("admit")
        .expect("chosen");
    // The next admission must avoid the now-loaded pod.
    let c1 = Uuid::new_v4().to_string();
    let second = admit_live_execution(&pool, PROJECT, &c1, TENANT, &live_payload(&c1), 10)
        .await
        .expect("admit")
        .expect("chosen");
    assert_ne!(second.pod_name, first.pod_name, "routed to the emptier pod");
    assert_eq!(live_load(&pool, "pod-a").await, 1);
    assert_eq!(live_load(&pool, "pod-b").await, 1);
}

/// No routable pod -> None (the dispatcher spawns one and retries).
#[sqlx::test]
async fn admit_no_pod_returns_none(pool: PgPool) {
    setup(&pool).await;
    let color = Uuid::new_v4().to_string();
    let got = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit");
    assert!(got.is_none(), "no pod -> no admission");
}

/// Dedup: a retry of the SAME handshake (same color) returns the SAME pod and
/// does NOT insert a second task.
#[sqlx::test]
async fn admit_is_idempotent_per_color(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();

    let first = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit")
        .expect("chosen");
    let retry = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit")
        .expect("chosen");
    assert_eq!(first.pod_name, retry.pod_name, "same pod on retry");
    assert_eq!(live_load(&pool, "pod-a").await, 1, "no duplicate task");
}

// ----- setup-failure cleanup (SetupFailureOutcome) -------------------------

/// A pending (never-claimed) live execution: cleanup deletes it and reports
/// `NoWorkerWillRun` (caller journals the cancel terminal).
#[sqlx::test]
async fn cleanup_pending_is_no_worker_will_run(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit")
        .expect("chosen");

    let outcome = delete_pending_live_execution(&pool, &color)
        .await
        .expect("cleanup");
    assert_eq!(outcome, SetupFailureOutcome::NoWorkerWillRun);
    assert_eq!(live_load(&pool, "pod-a").await, 0, "pending task deleted");
}

/// A CLAIMED live execution (the commit-but-Err race where a worker already
/// grabbed it): cleanup leaves it and reports `WorkerOwnsIt`. THIS is the path
/// that exercises the `SELECT 1::bigint` decode (a bare `SELECT 1` would
/// decode-error here and the branch would be unreachable).
#[sqlx::test]
async fn cleanup_claimed_is_worker_owns_it(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit")
        .expect("chosen");
    // Simulate the worker claiming the task.
    let claimed = claim_one(
        &pool,
        "pod-a",
        ClaimFilter::Worker { project_id: PROJECT.to_string() },
    )
    .await
    .expect("claim")
    .expect("a task to claim");
    assert_eq!(claimed.color.as_deref(), Some(color.as_str()));

    let outcome = delete_pending_live_execution(&pool, &color)
        .await
        .expect("cleanup");
    assert_eq!(
        outcome,
        SetupFailureOutcome::WorkerOwnsIt,
        "a claimed task is left for the worker; the decode must not error"
    );
    assert_eq!(live_load(&pool, "pod-a").await, 1, "claimed task untouched");
}

/// No task at all (enqueue never committed): cleanup reports `NoWorkerWillRun`
/// (caller journals the cancel for the started-but-taskless execution).
#[sqlx::test]
async fn cleanup_no_task_is_no_worker_will_run(pool: PgPool) {
    setup(&pool).await;
    let color = Uuid::new_v4().to_string();
    let outcome = delete_pending_live_execution(&pool, &color)
        .await
        .expect("cleanup");
    assert_eq!(outcome, SetupFailureOutcome::NoWorkerWillRun);
}

// ----- orphan recovery ------------------------------------------------------

/// A live execution pinned to a pod that died is reported as an orphan (so the
/// dispatcher cancels it), and is NOT requeued (it cannot run elsewhere).
#[sqlx::test]
async fn orphan_sweep_reports_dead_pod_live_execution(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit")
        .expect("chosen");
    // The pod dies.
    mark_dead(&pool, "pod-a").await.expect("mark_dead");

    let orphans = reclaim_orphaned_tasks(&pool).await.expect("reclaim");
    assert_eq!(orphans.len(), 1, "the dead-pod live execution is an orphan");
    assert_eq!(orphans[0].color, color);
    // It must NOT have been requeued (still pinned, not reset to a free pod).
    let (pin,): (Option<String>,) =
        sqlx::query_as("SELECT target_pod_name FROM task WHERE color = $1")
            .bind(&color)
            .fetch_one(&pool)
            .await
            .expect("row");
    assert_eq!(pin.as_deref(), Some("pod-a"), "live orphan is not requeued");
}

/// An ORDINARY (non-live) task pinned to a dead pod is REQUEUED (pin cleared),
/// not cancelled: the complementary half of the orphan-sweep partition.
#[sqlx::test]
async fn orphan_sweep_requeues_non_live_task(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    // A non-live execute task (live_connection = null) pinned to pod-a.
    let color = Uuid::new_v4().to_string();
    tasks::enqueue(
        &pool,
        tasks::NewTask {
            kind: TaskKind::Execute,
            target: TaskTarget::Worker,
            project_id: Some(PROJECT.to_string()),
            dedup_key: None,
            color: Some(color.clone()),
            tenant_id: TENANT.map(str::to_string),
            target_pod_name: Some("pod-a".to_string()),
            payload: json!({ "live_connection": Value::Null }),
        },
    )
    .await
    .expect("enqueue");
    mark_dead(&pool, "pod-a").await.expect("mark_dead");

    let orphans = reclaim_orphaned_tasks(&pool).await.expect("reclaim");
    assert!(orphans.is_empty(), "non-live task is not a live orphan");
    // It was requeued: pin cleared, status pending.
    let (pin, status): (Option<String>, String) =
        sqlx::query_as("SELECT target_pod_name, status FROM task WHERE color = $1")
            .bind(&color)
            .fetch_one(&pool)
            .await
            .expect("row");
    assert_eq!(pin, None, "non-live task pin cleared (requeued)");
    assert_eq!(status, "pending");
}

// ----- idle-exit gate -------------------------------------------------------

/// A pod with an in-flight live execution must NOT idle-exit (the live-execute
/// task is a pending/claimed worker task, which the `NOT EXISTS` guard sees).
#[sqlx::test]
async fn idle_exit_blocked_by_live_execution(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), CAP)
        .await
        .expect("admit")
        .expect("chosen");

    let exited = mark_done_if_idle(&pool, "pod-a").await.expect("idle check");
    assert!(!exited, "pod with a live execution must not idle-exit");
    assert!(has_live_for_project(&pool, PROJECT).await.expect("has_live"));
}

/// An idle pod (no tasks) DOES idle-exit.
#[sqlx::test]
async fn idle_exit_allowed_when_no_work(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let exited = mark_done_if_idle(&pool, "pod-a").await.expect("idle check");
    assert!(exited, "an idle pod self-exits");
}
