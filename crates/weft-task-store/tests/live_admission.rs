//! Layer-3 tests for the worker capacity + cleanup logic, run against a REAL
//! Postgres. The bugs this logic had (and the new memory-bounded placement,
//! draining-aware claiming, and draining idle-exit) all live IN the SQL, so a
//! faked store would not catch them; these exercise the actual queries.
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
use weft_task_store::worker_pod::{
    self, has_live_for_project, mark_dead, mark_done_if_idle, pick_admittable_for_project,
    pod_loads_for_project, projects_with_multiple_workers, register_alive, set_draining,
};
use weft_task_store::{TaskKind, TaskTarget};

const PROJECT: &str = "proj-1";
const TENANT: Option<&str> = Some("tenant-1");
/// The saturation threshold the production code uses (mirrored here so the
/// tests drive pressure relative to the real cutoff).
const SAT: f64 = weft_platform_traits::SATURATION_MEM_FRACTION;

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
    // `worker_pod::migrate` also creates a trigger ON `task` that stamps
    // `execution_color.owner_pod_name` on claim (ownership-follows-claim).
    // Same situation as exec_event: the table is the dispatcher journal's,
    // created before task-store migrate in production. Stand up a minimal
    // `execution_color` so the trigger has a row to update; seed a row per
    // color the tests claim so the stamp is observable. SYNC: keep columns
    // in step with crates/weft-dispatcher/src/journal/postgres.rs
    // `execution_color`.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS execution_color (
            color TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            started_at_unix BIGINT NOT NULL,
            phase TEXT NOT NULL,
            owner_pod_name TEXT
        )"#,
    )
    .execute(pool)
    .await
    .expect("execution_color stub");
    worker_pod::migrate(pool).await.expect("worker_pod schema");
}

/// Insert an alive worker pod for the project (pressure 0 until set).
async fn alive_pod(pool: &PgPool, pod: &str) {
    worker_pod::insert_spawning(pool, pod, PROJECT, "ns-1", "disp-1", "bin-1")
        .await
        .expect("insert_spawning");
    register_alive(pool, pod, PROJECT)
        .await
        .expect("register_alive");
}

/// Set a pod's reported memory pressure (what placement + scale-down read).
async fn set_pressure(pool: &PgPool, pod: &str, pressure: f64) {
    sqlx::query("UPDATE worker_pod SET mem_pressure = $2 WHERE pod_name = $1")
        .bind(pod)
        .bind(pressure)
        .execute(pool)
        .await
        .expect("set pressure");
}

/// Seed an `execution_color` row (as the journal's ExecutionStarted does)
/// so the claim trigger has a row to stamp.
async fn seed_color(pool: &PgPool, color: &str) {
    sqlx::query(
        "INSERT INTO execution_color (color, project_id, tenant_id, started_at_unix, phase) \
         VALUES ($1, $2, 'tenant-1', 0, 'running') ON CONFLICT (color) DO NOTHING",
    )
    .bind(color)
    .bind(PROJECT)
    .execute(pool)
    .await
    .expect("seed execution_color");
}

/// Read the color's current owner pod (what the broker's journal-write
/// check reads), set by the claim trigger.
async fn color_owner(pool: &PgPool, color: &str) -> Option<String> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT owner_pod_name FROM execution_color WHERE color = $1")
            .bind(color)
            .fetch_optional(pool)
            .await
            .expect("read owner");
    row.and_then(|(p,)| p)
}

fn live_payload(color: &str) -> Value {
    json!({
        "project_id": PROJECT,
        "color": color,
        "definition_hash": "hash-1",
        "live_connection": { "kind": "live_socket", "config": {} }
    })
}

/// Count in-flight (pending/claimed) live-execute tasks pinned to a pod.
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

// ----- admission (memory-bounded) ------------------------------------------

/// A single admission inserts one pinned execute task on the (only,
/// non-saturated) pod and reports it.
#[sqlx::test]
async fn admit_inserts_pinned_task(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();

    let admitted = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("a pod was chosen");
    assert_eq!(admitted.pod_name, "pod-a");
    assert_eq!(admitted.namespace, "ns-1");
    assert_eq!(live_load(&pool, "pod-a").await, 1, "one task pinned");
}

/// Memory saturation is enforced: a pod at/above the threshold is NOT chosen,
/// so a single-saturated-pod project returns None (dispatcher spawns another).
#[sqlx::test]
async fn admit_refuses_saturated_pod(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    set_pressure(&pool, "pod-a", SAT + 0.01).await;

    let color = Uuid::new_v4().to_string();
    let got = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit");
    assert!(got.is_none(), "a memory-saturated pod is not admittable");
    assert_eq!(live_load(&pool, "pod-a").await, 0, "no task inserted");
}

/// Below the threshold, admission keeps succeeding regardless of how many
/// connections are already on the pod: capacity is memory, not a count.
#[sqlx::test]
async fn admit_unbounded_below_saturation(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    set_pressure(&pool, "pod-a", SAT - 0.1).await;
    for _ in 0..5 {
        let color = Uuid::new_v4().to_string();
        let got = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
            .await
            .expect("admit");
        assert!(got.is_some(), "under saturation, count never refuses");
    }
    assert_eq!(live_load(&pool, "pod-a").await, 5);
}

/// Least-PRESSURED first: the next admission goes to the lower-pressure pod,
/// independent of how many connections each already holds.
#[sqlx::test]
async fn admit_picks_least_pressured(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    alive_pod(&pool, "pod-b").await;
    set_pressure(&pool, "pod-a", 0.5).await;
    set_pressure(&pool, "pod-b", 0.1).await;

    let color = Uuid::new_v4().to_string();
    let chosen = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");
    assert_eq!(chosen.pod_name, "pod-b", "routed to the lower-pressure pod");
}

/// A draining pod is never an admission target even when it has headroom.
#[sqlx::test]
async fn admit_skips_draining_pod(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    set_pressure(&pool, "pod-a", 0.0).await;
    set_draining(&pool, "pod-a").await.expect("drain");

    let color = Uuid::new_v4().to_string();
    let got = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit");
    assert!(got.is_none(), "a draining pod is not admittable");
}

/// No routable pod -> None (the dispatcher spawns one and retries).
#[sqlx::test]
async fn admit_no_pod_returns_none(pool: PgPool) {
    setup(&pool).await;
    let color = Uuid::new_v4().to_string();
    let got = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
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

    let first = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");
    let retry = admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");
    assert_eq!(first.pod_name, retry.pod_name, "same pod on retry");
    assert_eq!(live_load(&pool, "pod-a").await, 1, "no duplicate task");
}

// ----- placement helper (regular executions) -------------------------------

/// `pick_admittable_for_project` returns the least-pressured non-saturated,
/// non-draining pod, or None when none qualifies.
#[sqlx::test]
async fn pick_admittable_prefers_low_pressure_skips_saturated_and_draining(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-low").await;
    alive_pod(&pool, "pod-high").await;
    alive_pod(&pool, "pod-drain").await;
    set_pressure(&pool, "pod-low", 0.2).await;
    set_pressure(&pool, "pod-high", 0.4).await;
    set_pressure(&pool, "pod-drain", 0.0).await;
    set_draining(&pool, "pod-drain").await.expect("drain");

    let picked = pick_admittable_for_project(&pool, PROJECT, SAT)
        .await
        .expect("pick")
        .expect("a pod");
    assert_eq!(picked.0, "pod-low", "lowest pressure non-draining wins");

    // Saturate every non-draining pod: now nothing is admittable.
    set_pressure(&pool, "pod-low", SAT + 0.01).await;
    set_pressure(&pool, "pod-high", SAT + 0.01).await;
    let none = pick_admittable_for_project(&pool, PROJECT, SAT)
        .await
        .expect("pick");
    assert!(none.is_none(), "all saturated/draining -> no admittable pod");
}

// ----- scale-down candidate selection --------------------------------------

/// Only projects with more than one alive worker are scale-down candidates,
/// and their per-pod loads feed the planner.
#[sqlx::test]
async fn scaledown_candidates_and_loads(pool: PgPool) {
    setup(&pool).await;
    // One worker: not a candidate.
    alive_pod(&pool, "solo").await;
    assert!(
        projects_with_multiple_workers(&pool)
            .await
            .expect("candidates")
            .is_empty(),
        "single-worker project is not a scale-down candidate"
    );

    // A second worker makes it a candidate.
    alive_pod(&pool, "pod-b").await;
    let cands = projects_with_multiple_workers(&pool).await.expect("candidates");
    assert_eq!(cands, vec![PROJECT.to_string()]);

    set_pressure(&pool, "solo", 0.1).await;
    set_pressure(&pool, "pod-b", 0.2).await;
    let mut loads = pod_loads_for_project(&pool, PROJECT).await.expect("loads");
    loads.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(loads.len(), 2);
    assert_eq!(loads[0].0, "pod-b");
    assert!((loads[1].1 - 0.1).abs() < 1e-9, "solo pressure reported");
}

/// The candidate gate and the load query must agree on "draining": a
/// project with one live + one draining worker is NOT a scale-down
/// candidate (only one stable worker remains), and its load list holds
/// only the live one. If the gate counted the draining pod it would pass
/// here while the loads showed one pod, wasting a planner tick.
#[sqlx::test]
async fn scaledown_excludes_draining_from_candidate_and_loads(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-live").await;
    alive_pod(&pool, "pod-drain").await;
    set_draining(&pool, "pod-drain").await.expect("drain");

    assert!(
        projects_with_multiple_workers(&pool)
            .await
            .expect("candidates")
            .is_empty(),
        "one live + one draining is not a multi-worker candidate"
    );
    let loads = pod_loads_for_project(&pool, PROJECT).await.expect("loads");
    assert_eq!(loads.len(), 1, "only the non-draining worker is a load");
    assert_eq!(loads[0].0, "pod-live");
}

// ----- failed-boot reaping (stuck spawning) --------------------------------

/// `list_stale_spawning` surfaces spawning rows older than the threshold
/// (the reaper passes `now - SPAWN_BOOT_DEADLINE_SECS`). A row younger
/// than the threshold (still within the generous boot deadline) is NOT
/// surfaced; a row older is. A dead row drops out (reaped once).
#[sqlx::test]
async fn stale_spawning_lists_only_past_threshold(pool: PgPool) {
    setup(&pool).await;
    // A pod that JUST started spawning (never registered alive): too young.
    worker_pod::insert_spawning(&pool, "pod-fresh", PROJECT, "ns-1", "disp-1", "bin-1")
        .await
        .expect("insert fresh");
    // A pod older than the grace: backdate its created_at to the epoch.
    worker_pod::insert_spawning(&pool, "pod-old", PROJECT, "ns-1", "disp-1", "bin-1")
        .await
        .expect("insert old");
    sqlx::query("UPDATE worker_pod SET created_at_unix = $2 WHERE pod_name = $1")
        .bind("pod-old")
        .bind(0_i64) // unix epoch: unambiguously past the threshold
        .execute(&pool)
        .await
        .expect("backdate");

    // Threshold below the fresh pod's real now() but above the epoch, so
    // only pod-old is past it.
    let threshold = 1_000_000_i64;
    let stale = worker_pod::list_stale_spawning(&pool, threshold).await.expect("list");
    let names: Vec<_> = stale.iter().map(|r| r.pod_name.as_str()).collect();
    assert_eq!(names, vec!["pod-old"], "only the past-threshold spawning pod is listed");
    // created_at_unix is carried through so the reaper can apply the backstop.
    assert_eq!(stale[0].created_at_unix, 0);

    // After marking it dead it is no longer listed (reaped once).
    mark_dead(&pool, "pod-old").await.expect("mark_dead");
    let after = worker_pod::list_stale_spawning(&pool, threshold).await.expect("list");
    assert!(after.is_empty(), "a dead row is not re-listed");
}

// ----- draining-aware claiming ---------------------------------------------

/// A draining pod may NOT claim an UNPINNED task (so the drain can empty it),
/// but a non-draining sibling can.
#[sqlx::test]
async fn draining_pod_does_not_claim_unpinned(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-drain").await;
    alive_pod(&pool, "pod-live").await;
    set_draining(&pool, "pod-drain").await.expect("drain");

    // An unpinned worker execute task for the project.
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
            target_pod_name: None,
            payload: json!({ "live_connection": Value::Null }),
        },
    )
    .await
    .expect("enqueue");

    // The draining pod must NOT get it.
    let drained = claim_one(
        &pool,
        "pod-drain",
        ClaimFilter::Worker { project_id: PROJECT.to_string() },
    )
    .await
    .expect("claim");
    assert!(drained.is_none(), "draining pod cannot claim unpinned work");

    // The live sibling can.
    let live = claim_one(
        &pool,
        "pod-live",
        ClaimFilter::Worker { project_id: PROJECT.to_string() },
    )
    .await
    .expect("claim")
    .expect("a task");
    assert_eq!(live.color.as_deref(), Some(color.as_str()));
}

/// A draining pod MAY still claim a task PINNED to it (a resume it still owns,
/// or a cancel): draining stops new unaddressed work, not its own obligations.
#[sqlx::test]
async fn draining_pod_still_claims_pinned_to_it(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-drain").await;
    set_draining(&pool, "pod-drain").await.expect("drain");

    let color = Uuid::new_v4().to_string();
    tasks::enqueue(
        &pool,
        tasks::NewTask {
            kind: TaskKind::Resume,
            target: TaskTarget::Worker,
            project_id: Some(PROJECT.to_string()),
            dedup_key: None,
            color: Some(color.clone()),
            tenant_id: TENANT.map(str::to_string),
            target_pod_name: Some("pod-drain".to_string()),
            payload: json!({ "live_connection": Value::Null }),
        },
    )
    .await
    .expect("enqueue");

    let claimed = claim_one(
        &pool,
        "pod-drain",
        ClaimFilter::Worker { project_id: PROJECT.to_string() },
    )
    .await
    .expect("claim")
    .expect("the pinned task");
    assert_eq!(claimed.color.as_deref(), Some(color.as_str()));
}

// ----- color-ownership binding (claim trigger) -----------------------------

/// Claiming a color-bearing task atomically stamps the color's owner to
/// the claimer (the `task_claim_binds_color_owner` trigger), in the
/// claim's own transaction, so "claimed by X" and "owned by X" can never
/// disagree. This is what the broker's journal-write fence reads.
#[sqlx::test]
async fn claim_binds_color_owner(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    seed_color(&pool, &color).await;
    assert_eq!(color_owner(&pool, &color).await, None, "unowned before any claim");

    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");
    // Admission only INSERTS the pinned task; ownership binds on CLAIM.
    assert_eq!(color_owner(&pool, &color).await, None, "not owned until claimed");

    claim_one(&pool, "pod-a", ClaimFilter::Worker { project_id: PROJECT.to_string() })
        .await
        .expect("claim")
        .expect("the task");
    assert_eq!(
        color_owner(&pool, &color).await.as_deref(),
        Some("pod-a"),
        "claim stamps ownership"
    );
}

/// Latest-claim-wins: when a resume for a dead owner's color is reclaimed
/// by a fresh pod, ownership moves to the fresh pod on its claim. (This is
/// the handoff the resume-pinning logic relies on.)
#[sqlx::test]
async fn reclaim_moves_color_owner_to_fresh_pod(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-old").await;
    let color = Uuid::new_v4().to_string();
    seed_color(&pool, &color).await;

    // Old pod claims an unpinned resume -> owns the color.
    tasks::enqueue(
        &pool,
        tasks::NewTask {
            kind: TaskKind::Resume,
            target: TaskTarget::Worker,
            project_id: Some(PROJECT.to_string()),
            dedup_key: None,
            color: Some(color.clone()),
            tenant_id: TENANT.map(str::to_string),
            target_pod_name: None,
            payload: json!({ "live_connection": Value::Null }),
        },
    )
    .await
    .expect("enqueue");
    claim_one(&pool, "pod-old", ClaimFilter::Worker { project_id: PROJECT.to_string() })
        .await
        .expect("claim")
        .expect("task");
    assert_eq!(color_owner(&pool, &color).await.as_deref(), Some("pod-old"));

    // Old pod dies; the orphan sweep requeues the (non-live) resume (pin
    // cleared, status pending). A fresh pod then reclaims it.
    mark_dead(&pool, "pod-old").await.expect("mark_dead");
    reclaim_orphaned_tasks(&pool).await.expect("reclaim");
    alive_pod(&pool, "pod-new").await;
    let reclaimed = claim_one(
        &pool,
        "pod-new",
        ClaimFilter::Worker { project_id: PROJECT.to_string() },
    )
    .await
    .expect("claim")
    .expect("the requeued resume");
    assert_eq!(reclaimed.color.as_deref(), Some(color.as_str()));
    assert_eq!(
        color_owner(&pool, &color).await.as_deref(),
        Some("pod-new"),
        "ownership handed off to the fresh pod on reclaim"
    );
}

/// A claim of a NON-driver color-bearing task (a cancel_execution) must NOT
/// move color ownership: ownership follows the execute/resume DRIVER only.
/// Without this, a cancel claimed by a pod other than the owner would steal
/// ownership and fence the real owner's journal writes mid-run.
#[sqlx::test]
async fn cancel_claim_does_not_move_color_owner(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-owner").await;
    alive_pod(&pool, "pod-other").await;
    let color = Uuid::new_v4().to_string();
    seed_color(&pool, &color).await;

    // pod-owner claims the driving execute task -> owns the color.
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");
    // Force the live execute task onto pod-owner by pinning was done at admit;
    // claim it as pod-owner.
    claim_one(&pool, "pod-owner", ClaimFilter::Worker { project_id: PROJECT.to_string() })
        .await
        .expect("claim")
        .expect("the execute task");
    assert_eq!(color_owner(&pool, &color).await.as_deref(), Some("pod-owner"));

    // A cancel_execution task for the SAME color, deliberately pinned to a
    // DIFFERENT pod (the bug scenario: cancel routed to a non-owner). Its
    // claim must leave ownership untouched.
    tasks::enqueue(
        &pool,
        tasks::NewTask {
            kind: TaskKind::CancelExecution,
            target: TaskTarget::Worker,
            project_id: Some(PROJECT.to_string()),
            dedup_key: None,
            color: Some(color.clone()),
            tenant_id: TENANT.map(str::to_string),
            target_pod_name: Some("pod-other".to_string()),
            payload: json!({ "live_connection": Value::Null }),
        },
    )
    .await
    .expect("enqueue cancel");
    claim_one(&pool, "pod-other", ClaimFilter::Worker { project_id: PROJECT.to_string() })
        .await
        .expect("claim")
        .expect("the cancel task");
    assert_eq!(
        color_owner(&pool, &color).await.as_deref(),
        Some("pod-owner"),
        "a cancel claim must not steal ownership from the driving pod"
    );
}

// ----- idle-exit gate -------------------------------------------------------

/// A NON-draining pod with an in-flight live execution must NOT idle-exit.
#[sqlx::test]
async fn idle_exit_blocked_by_live_execution(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");

    let exited = mark_done_if_idle(&pool, "pod-a").await.expect("idle check");
    assert!(!exited, "pod with a live execution must not idle-exit");
    assert!(has_live_for_project(&pool, PROJECT).await.expect("has_live"));
}

/// An idle non-draining pod DOES idle-exit.
#[sqlx::test]
async fn idle_exit_allowed_when_no_work(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let exited = mark_done_if_idle(&pool, "pod-a").await.expect("idle check");
    assert!(exited, "an idle pod self-exits");
}

/// THE drain-completion case: a DRAINING pod with no work of its OWN idle-exits
/// even while a SIBLING is busy with the project's work. (Gating a draining
/// pod on project-wide work would pin it alive forever and the drain would
/// never complete.)
#[sqlx::test]
async fn draining_pod_exits_despite_busy_sibling(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-drain").await;
    alive_pod(&pool, "pod-busy").await;
    set_draining(&pool, "pod-drain").await.expect("drain");

    // The sibling holds an in-flight live execution (project has work), but it
    // is the SIBLING's, claimed by pod-busy.
    let color = Uuid::new_v4().to_string();
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen on a non-draining pod");
    // Make sure it landed on the busy pod and is claimed by it.
    claim_one(
        &pool,
        "pod-busy",
        ClaimFilter::Worker { project_id: PROJECT.to_string() },
    )
    .await
    .expect("claim")
    .expect("the live task");

    // The draining pod owns nothing -> it exits, despite the busy sibling.
    let exited = mark_done_if_idle(&pool, "pod-drain").await.expect("idle");
    assert!(exited, "draining pod exits when its OWN work is done");
    // The busy sibling must NOT exit (it still owns the live execution).
    let busy_exit = mark_done_if_idle(&pool, "pod-busy").await.expect("idle");
    assert!(!busy_exit, "the busy sibling keeps running");
}

// ----- setup-failure cleanup (SetupFailureOutcome) -------------------------

/// A pending (never-claimed) live execution: cleanup deletes it and reports
/// `NoWorkerWillRun` (caller journals the cancel terminal).
#[sqlx::test]
async fn cleanup_pending_is_no_worker_will_run(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");

    let outcome = delete_pending_live_execution(&pool, &color)
        .await
        .expect("cleanup");
    assert_eq!(outcome, SetupFailureOutcome::NoWorkerWillRun);
    assert_eq!(live_load(&pool, "pod-a").await, 0, "pending task deleted");
}

/// A CLAIMED live execution: cleanup leaves it and reports `WorkerOwnsIt`.
#[sqlx::test]
async fn cleanup_claimed_is_worker_owns_it(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");
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

/// No task at all (enqueue never committed): cleanup reports `NoWorkerWillRun`.
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
    admit_live_execution(&pool, PROJECT, &color, TENANT, &live_payload(&color), SAT)
        .await
        .expect("admit")
        .expect("chosen");
    mark_dead(&pool, "pod-a").await.expect("mark_dead");

    let orphans = reclaim_orphaned_tasks(&pool).await.expect("reclaim");
    assert_eq!(orphans.len(), 1, "the dead-pod live execution is an orphan");
    assert_eq!(orphans[0].color, color);
    let (pin,): (Option<String>,) =
        sqlx::query_as("SELECT target_pod_name FROM task WHERE color = $1")
            .bind(&color)
            .fetch_one(&pool)
            .await
            .expect("row");
    assert_eq!(pin.as_deref(), Some("pod-a"), "live orphan is not requeued");
}

/// An ORDINARY (non-live) task pinned to a dead pod is REQUEUED (pin cleared).
/// This is also the resume-owner-died handoff: a resume pinned to a now-dead
/// owner gets unpinned so a fresh worker takes over.
#[sqlx::test]
async fn orphan_sweep_requeues_non_live_task(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let color = Uuid::new_v4().to_string();
    tasks::enqueue(
        &pool,
        tasks::NewTask {
            kind: TaskKind::Resume,
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
    let (pin, status): (Option<String>, String) =
        sqlx::query_as("SELECT target_pod_name, status FROM task WHERE color = $1")
            .bind(&color)
            .fetch_one(&pool)
            .await
            .expect("row");
    assert_eq!(pin, None, "resume pin cleared (requeued) when owner died");
    assert_eq!(status, "pending");
}
