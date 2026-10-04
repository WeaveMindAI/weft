//! `task` table: durable work queue. Producers `enqueue`; one process
//! claims a row via `claim_one` (FOR UPDATE SKIP LOCKED), runs the
//! work, then `complete` or `fail`. Heartbeat extends the claim's
//! lease so a slow op doesn't lose the row to the stale-recovery
//! filter.
//!
//! Idempotency: every executor MUST be safe to re-run on partial
//! success. A row can be run again after a process crash (the lease
//! expires and `claim_one` rescues it) or after a surrender (the
//! claimer could not renew its lease and `requeue`d the row). Cluster
//! ops should treat "already exists" as success; executors with a
//! non-re-runnable side effect persist its outcome via
//! `store_result_partial` and read it back on a later claim.
//!
//! Dedup: a partial unique index on `(tenant_id, kind, dedup_key)`
//! for live rows lets producers attach to in-flight work via
//! `enqueue_dedup`. Tenant-scoped so dedup never crosses tenants.


use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

/// How long a claim is valid before another process can steal it, at this
/// install's pace (`weft_core::time_scale`): 60 seconds in real time.
/// processes heartbeat the claim while they work.
pub fn claim_duration_secs() -> i64 {
    weft_core::time_scale::scaled_secs(60)
}

/// How often a working process renews its claim: a quarter of
/// [`claim_duration_secs`] (15 seconds in real time), so a slow op
/// doesn't lose its claim to a transient hiccup.
pub fn claim_heartbeat_interval() -> std::time::Duration {
    weft_core::time_scale::scaled(std::time::Duration::from_secs(15))
}

/// How long terminal-state rows linger before the sweeper deletes
/// them. Long enough that producers polling for results see them.
pub const TERMINAL_RETENTION_SECS: i64 = 3600;

/// Where a task stands: weft-core's, since a client waiting on a task
/// reads it off the wire.
pub use weft_core::task::TaskStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskTarget {
    Dispatcher,
    Worker,
}

impl TaskTarget {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dispatcher => "dispatcher",
            Self::Worker => "worker",
        }
    }
}

/// A row from the `task` table. Producers fill `NewTask`; consumers
/// receive `Task` from `claim_one`.
///
/// Derives serde directly: this is also the wire shape used by
/// `weft-broker-client::protocol`. A new field on this struct
/// shows up on the wire automatically; no mirror type to keep in
/// sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub kind: String,
    pub status: TaskStatus,
    pub project_id: Option<Uuid>,
    pub execution_id: Option<String>,
    /// The tenant the task belongs to: every task has one, and the dedup
    /// uniqueness is scoped by it.
    pub tenant_id: String,
    /// Requested executable, retained when claimed and handed to a spawn handler.
    pub binary_hash: Option<String>,
    /// How many times this row has been claimed, INCLUDING the claim
    /// that returned this value. 1 on the first claim; > 1 means a
    /// prior claim existed (lease expired, or the claimer surrendered
    /// and requeued), which an executor guarding a non-re-runnable
    /// side effect reads to tell a fresh run from a retry.
    #[serde(default)]
    pub attempts: i32,
    pub payload: Value,
}

/// Producer-side spec for enqueuing a task. Serializable for the
/// same reason as `Task`: it's the wire shape on
/// `/v1/task/enqueue_dedup`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTask {
    /// The task kind as its raw STRING (the value stored on `task.kind` and
    /// dispatched on). Built-in producers pass `TaskKind::X.into()`; a runtime
    /// with its own task kinds passes its own kind string (e.g. `"build_image"`)
    /// directly, so an added kind never has to widen the built-in `TaskKind` enum.
    pub kind: String,
    pub target: TaskTarget,
    pub project_id: Option<Uuid>,
    pub dedup_key: Option<String>,
    pub execution_id: Option<String>,
    /// The tenant the task belongs to: required, because the dedup
    /// uniqueness is scoped by it (a NULL tenant would never dedup) and
    /// every task is somebody's.
    pub tenant_id: String,
    /// If set, only the named process replica can claim this task. A
    /// live execution is pinned to the worker replica its caller's
    /// connection reached when that worker claims it (see
    /// [`AWAITS_CALLER`]), since the caller is on THAT replica's socket.
    /// NULL means whoever the task is delivered to may claim it.
    pub target_replica: Option<String>,
    /// The worker IMAGE this task runs on: the project's
    /// `running_binary_hash` at enqueue time. A worker task is delivered
    /// to workers running exactly this image (`take_deliveries`), so new
    /// work never lands on a worker whose binary lacks the current
    /// graph's node impls. Required on every execute and resume; NULL
    /// on dispatcher tasks.
    #[serde(default)]
    pub binary_hash: Option<String>,
    pub payload: Value,
}

/// Which task the caller wants to claim. Internally-tagged serde shape
/// so the wire form is `{"kind": "dispatcher"}` or
/// `{"kind": "execution_id", "project_id": "...", "execution_id": "..."}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClaimFilter {
    /// Any dispatcher task.
    Dispatcher,
    /// The execute or resume task of one execution: what a worker that
    /// was handed that execution claims. A worker is never handed "some
    /// work of the project"; it is called for one execution.
    ExecutionId { project_id: Uuid, execution_id: String },
}

impl ClaimFilter {
    /// The [`TASK_READY_CHANNEL`] payload a task this filter can claim
    /// is announced with.
    pub fn ready_payload(&self) -> String {
        match self {
            Self::Dispatcher => ready_payload(TaskTarget::Dispatcher, None),
            Self::ExecutionId { project_id, .. } => ready_payload(TaskTarget::Worker, Some(*project_id)),
        }
    }
}

/// The channel a task that has just become claimable (inserted pending,
/// or put back to pending by a requeue or a reclaim) notifies on, from
/// the `task_ready_notify` trigger in [`GROUP`]. A lease that merely
/// expires announces nothing: a waiter's own deadline is what rescues
/// it.
pub const TASK_READY_CHANNEL: &str = "weft_task_ready";

/// A [`TASK_READY_CHANNEL`] payload: `dispatcher` for a dispatcher task,
/// `worker:<project_id>` for a worker one, since only that project's
/// workers can claim it (and the delivery sweep wakes on it).
/// SYNC: ready_payload <-> the `task_ready_notify` function in `GROUP`.
pub fn ready_payload(target: TaskTarget, project_id: Option<Uuid>) -> String {
    match target {
        TaskTarget::Dispatcher => "dispatcher".to_string(),
        TaskTarget::Worker => format!("worker:{}", project_id.map(|p| p.to_string()).unwrap_or_default()),
    }
}

/// Result of an `enqueue_dedup` call. Both arms carry the live row's
/// id; the variant tells the caller whether THIS call inserted the
/// row (in which case the executor will run their payload) or
/// attached to a row already in flight from a sibling caller.
///
/// Most callers only want the id (`outcome.id()`); the variant
/// exists for callers that care about idempotency tracing.
#[derive(Debug, Clone)]
pub enum DedupOutcome {
    Inserted(Uuid),
    AlreadyLive(Uuid),
}

impl DedupOutcome {
    /// The task's id: the new one, or the live one it collapsed onto.
    pub fn id(&self) -> Uuid {
        match self {
            Self::Inserted(id) | Self::AlreadyLive(id) => *id,
        }
    }
}

/// The `task` table's schema, applied at boot via `schema_guard::apply_groups`.
pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "task",
    tables: &["task"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS task (
            id UUID PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            project_id UUID,
            dedup_key TEXT,
            execution_id TEXT,
            tenant_id TEXT NOT NULL,
            target_replica TEXT,
            binary_hash TEXT,
            payload JSONB NOT NULL,
            claimed_by TEXT,
            claimed_until_unix BIGINT,
            attempts INTEGER NOT NULL DEFAULT 0,
            result JSONB,
            error TEXT,
            created_at_unix BIGINT NOT NULL,
            completed_at_unix BIGINT,
            -- Asked for again while claimed (`enqueue_or_rearm`): the
            -- claimant may already be past the point where it would
            -- have seen why, so finishing puts the row back to pending
            -- instead of ending it.
            rerun_requested BOOLEAN NOT NULL DEFAULT FALSE,
            -- Until when a worker task counts as handed to a worker that
            -- has not claimed it yet (`take_deliveries`): no second
            -- delivery is made before then, so a worker still starting
            -- up is not handed the same execution again.
            delivered_until_unix BIGINT
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_pending_dispatcher
            ON task(created_at_unix)
            WHERE status = 'pending' AND target = 'dispatcher'"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_pending_worker
            ON task(project_id, created_at_unix)
            WHERE status = 'pending' AND target = 'worker' AND project_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_claimed_expired
            ON task(claimed_until_unix)
            WHERE status = 'claimed'"#,
        // Tenant-scoped: isolation is enforced by the index itself,
        // not by the convention that every dedup_key embeds a
        // scope-checked resource. Two tenants can never collide on /
        // suppress each other's dedup tasks even if a future task kind
        // uses a non-scope-checked dedup_key.
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_task_dedup_live
            ON task(tenant_id, kind, dedup_key)
            WHERE dedup_key IS NOT NULL AND status IN ('pending', 'claimed')"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_execution_id
            ON task(execution_id)
            WHERE execution_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_tenant ON task(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_project
            ON task(project_id)
            WHERE project_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_terminal_completed
            ON task(completed_at_unix)
            WHERE status IN ('complete', 'failed')"#,
        // Announce every task that has just become claimable, so the
        // pickers sleep until there is work instead of asking on a
        // timer. From a trigger rather than from each writer, so no
        // write path (an enqueue, a live admission, a requeue, the
        // orphan reclaim) can forget it, and the notification goes out
        // when the write commits.
        // SYNC: task_ready_notify's payload <-> ready_payload above.
        r#"CREATE OR REPLACE FUNCTION task_ready_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_task_ready',
                    CASE WHEN NEW.target = 'dispatcher' THEN 'dispatcher'
                         ELSE 'worker:' || COALESCE(NEW.project_id::text, '') END);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS task_ready_on_insert ON task"#,
        r#"CREATE TRIGGER task_ready_on_insert
            AFTER INSERT ON task
            FOR EACH ROW
            WHEN (NEW.status = 'pending')
            EXECUTE FUNCTION task_ready_notify()"#,
        // Only the transition INTO pending: claims, heartbeats and
        // completions are the hot path and must stay silent.
        r#"DROP TRIGGER IF EXISTS task_ready_on_pending ON task"#,
        r#"CREATE TRIGGER task_ready_on_pending
            AFTER UPDATE OF status ON task
            FOR EACH ROW
            WHEN (NEW.status = 'pending' AND OLD.status IS DISTINCT FROM 'pending')
            EXECUTE FUNCTION task_ready_notify()"#,
        // Ownership-follows-claim: whenever a worker claims a task that
        // carries an execution, that process becomes the execution's owner. Done as
        // an AFTER-UPDATE trigger so it commits in the SAME transaction
        // as the claim itself (`tasks::claim_one`'s UPDATE), making
        // "claimed by process X" and "owned by process X" atomically
        // inseparable. Without this (a separate UPDATE after the claim
        // commits) a crash between the two leaves a task claimed by a
        // process that does not own its execution, so every journal write from
        // that process is fenced until the lease expires. "Latest claim
        // wins" is exactly what a resume handoff needs: the fresh process
        // that reclaims a dead owner's resume takes ownership here. The
        // task table's claim semantics already enforce one active process
        // per task, so there is never an overlap where two processes own one
        // execution. `execution` is seeded at ExecutionStarted (well
        // before any task is claimed), so the row always exists; if it
        // somehow does not the UPDATE matches zero rows and the
        // broker's journal-write owner check then refuses the process
        // loudly (no silent mis-bind).
        // SYNC: execution.owner_replica has exactly two writers,
        // this trigger and `bind_execution_id_owner` below (the appointed-driver
        // path for processes that never claim a task); the readers are
        // crates/weft-broker/src/handlers.rs journal_record and
        // crates/weft-broker/src/auth.rs resolve_storage_caller.
        r#"CREATE OR REPLACE FUNCTION weft_bind_execution_id_owner() RETURNS trigger AS $$
            BEGIN
                IF NEW.execution_id IS NOT NULL AND NEW.claimed_by IS NOT NULL THEN
                    UPDATE execution
                    SET owner_replica = NEW.claimed_by
                    WHERE execution_id = NEW.execution_id;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS task_claim_binds_execution_id_owner ON task"#,
        // Fire only on:
        //   - the pending/claimed-lapsed -> claimed transition
        //     (claimed_by goes from NULL/other to a process), not on every
        //     task UPDATE (complete, heartbeat-renew, requeue), so the
        //     hot path stamps ownership exactly once per claim; AND
        //   - tasks that actually DRIVE the execution: 'execute' and
        //     'resume'. A side-channel task that merely carries an execution
        //     (a 'cancel_execution' addressed to the owner) must NOT
        //     restamp ownership: ownership follows the driver. (cancel is
        //     pinned to the owner anyway, so even if it did fire the
        //     stamp would be owner->owner, but scoping by kind makes a
        //     future execution-bearing task kind unable to steal ownership by
        //     accident, which is the property we want to hold by
        //     construction, not by every caller remembering to pin.)
        r#"CREATE TRIGGER task_claim_binds_execution_id_owner
            AFTER UPDATE OF claimed_by ON task
            FOR EACH ROW
            WHEN (NEW.status = 'claimed' AND NEW.claimed_by IS NOT NULL
                  AND NEW.claimed_by IS DISTINCT FROM OLD.claimed_by
                  AND NEW.kind IN ('execute', 'resume'))
            EXECUTE FUNCTION weft_bind_execution_id_owner()"#,
    ],
    seed: &[],
};

/// Insert a new task. Returns the minted id. Does NOT enforce dedup
/// even if `spec.dedup_key` is set; use `enqueue_dedup` for that.
pub async fn enqueue(pool: &PgPool, spec: NewTask) -> Result<Uuid> {
    let id = Uuid::new_v4();
    let now = unix_now();
    sqlx::query(
        r#"INSERT INTO task (
            id, kind, status, target, project_id, dedup_key, execution_id, tenant_id,
            target_replica, binary_hash, payload, attempts, created_at_unix
        ) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8, $9, $10, 0, $11)"#,
    )
    .bind(id)
    .bind(spec.kind.as_str())
    .bind(spec.target.as_str())
    .bind(spec.project_id)
    .bind(spec.dedup_key.as_deref())
    .bind(spec.execution_id.as_deref())
    .bind(spec.tenant_id.as_str())
    .bind(spec.target_replica.as_deref())
    .bind(spec.binary_hash.as_deref())
    .bind(&spec.payload)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Insert with dedup. If a pending or claimed task with the same
/// `(tenant_id, kind, dedup_key)` already exists, returns its id
/// without inserting.
///
/// Concurrency: a transaction-scoped advisory lock keyed on
/// `hashtextextended("{tenant}|{kind}|{dedup_key}", 0)` serializes
/// concurrent callers on the same (tenant, kind, dedup_key). Without
/// the lock, two producers could both pass the SELECT (their snapshots
/// don't see each other's uncommitted INSERT) and the second would hit
/// a unique-violation on the partial index instead of returning
/// AlreadyLive.
pub async fn enqueue_dedup(pool: &PgPool, spec: NewTask) -> Result<DedupOutcome> {
    let mut tx = pool.begin().await?;
    let outcome = enqueue_dedup_in(&mut tx, spec).await?;
    tx.commit().await?;
    Ok(outcome)
}

/// [`enqueue_dedup`] on a caller-owned connection. MUST run inside a
/// transaction: the advisory lock is xact-scoped (it releases when the
/// caller's transaction ends), and the insert's atomicity with whatever else
/// the caller writes is the whole point of taking a connection.
pub async fn enqueue_dedup_in(
    conn: &mut sqlx::PgConnection,
    spec: NewTask,
) -> Result<DedupOutcome> {
    let dedup = spec
        .dedup_key
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("enqueue_dedup requires dedup_key"))?;

    // Lock + SELECT are scoped by tenant_id to match the
    // `(tenant_id, kind, dedup_key)` unique index: dedup never crosses
    // a tenant boundary. (`tenant_id IS NOT DISTINCT FROM $3` so a
    // NULL-tenant task dedups against other NULL-tenant tasks, matching
    // how the unique index treats them.)
    let tenant = spec.tenant_id.as_str();
    let lock_input = format!("{}|{}|{}", tenant, spec.kind.as_str(), dedup);
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&lock_input)
        .execute(&mut *conn)
        .await?;

    let existing: Option<(Uuid,)> = sqlx::query_as(
        r#"SELECT id FROM task
           WHERE tenant_id IS NOT DISTINCT FROM $1
             AND kind = $2 AND dedup_key = $3 AND status IN ('pending', 'claimed')
           LIMIT 1"#,
    )
    .bind(spec.tenant_id.as_str())
    .bind(spec.kind.as_str())
    .bind(dedup)
    .fetch_optional(&mut *conn)
    .await?;
    if let Some((id,)) = existing {
        return Ok(DedupOutcome::AlreadyLive(id));
    }

    let id = Uuid::new_v4();
    let now = unix_now();
    sqlx::query(
        r#"INSERT INTO task (
            id, kind, status, target, project_id, dedup_key, execution_id, tenant_id,
            target_replica, binary_hash, payload, attempts, created_at_unix
        ) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8, $9, $10, 0, $11)"#,
    )
    .bind(id)
    .bind(spec.kind.as_str())
    .bind(spec.target.as_str())
    .bind(spec.project_id)
    .bind(dedup)
    .bind(spec.execution_id.as_deref())
    .bind(spec.tenant_id.as_str())
    .bind(spec.target_replica.as_deref())
    .bind(spec.binary_hash.as_deref())
    .bind(&spec.payload)
    .bind(now)
    .execute(&mut *conn)
    .await?;
    Ok(DedupOutcome::Inserted(id))
}

/// [`enqueue_dedup`] for a task that means "go and look again": a pending
/// one already will, so the ask collapses onto it, but a CLAIMED one may
/// already have looked for the last time, so it is asked to run once
/// more when it finishes (`rerun_requested`) rather than collapsed
/// onto and forgotten. The next claim clears the ask, however the
/// current one ends, since that claim is itself the run asked for. A resume is the case: its worker reads the
/// journal a last time and exits, and a wake landing between that read
/// and the task's completion used to be collapsed onto the finishing
/// task and never driven.
pub async fn enqueue_or_rearm(pool: &PgPool, spec: NewTask) -> Result<DedupOutcome> {
    let mut tx = pool.begin().await?;
    let dedup = spec
        .dedup_key
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("enqueue_or_rearm requires dedup_key"))?;
    let lock_input = format!("{}|{}|{}", spec.tenant_id.as_str(), spec.kind.as_str(), dedup);
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&lock_input)
        .execute(&mut *tx)
        .await?;
    // The row lock of this UPDATE and the one of `complete`/`fail` order
    // the two: before the completion, the completion sees the flag and
    // re-pends; after it, this matches nothing and the insert below
    // queues a fresh task.
    let rearmed: Option<(Uuid,)> = sqlx::query_as(
        r#"UPDATE task SET rerun_requested = TRUE
           WHERE tenant_id IS NOT DISTINCT FROM $1
             AND kind = $2 AND dedup_key = $3 AND status = 'claimed'
           RETURNING id"#,
    )
    .bind(spec.tenant_id.as_str())
    .bind(spec.kind.as_str())
    .bind(dedup)
    .fetch_optional(&mut *tx)
    .await?;
    let outcome = match rearmed {
        Some((id,)) => DedupOutcome::AlreadyLive(id),
        None => enqueue_dedup_in(&mut tx, spec).await?,
    };
    tx.commit().await?;
    Ok(outcome)
}

/// Atomically claim one task for `replica` (the claiming process
/// replica). Picks oldest pending first; also rescues claims whose lease
/// expired (the claimant died mid-work).
///
/// One statement: the pick locks its row (`FOR UPDATE SKIP LOCKED`, so
/// sibling claimants skip it rather than queue behind it) and the claim
/// updates it, with no round trip between.
pub async fn claim_one(
    pool: &PgPool,
    replica: &str,
    filter: &ClaimFilter,
) -> Result<Option<Task>> {
    let now = unix_now();
    let claim_until = now + claim_duration_secs();
    let row = match filter {
        ClaimFilter::Dispatcher => {
            sqlx::query(&claim_sql(DISPATCHER_PICK))
                .bind(replica)
                .bind(now)
                .bind(claim_until)
                .fetch_optional(pool)
                .await?
        }
        ClaimFilter::ExecutionId { project_id, execution_id } => {
            sqlx::query(&claim_sql(EXECUTION_ID_PICK))
                .bind(replica)
                .bind(now)
                .bind(claim_until)
                .bind(*project_id)
                .bind(execution_id.as_str())
                .fetch_optional(pool)
                .await?
        }
    };
    row.map(row_to_task).transpose()
}

/// A live run's execute task whose caller is on the way: born at the
/// handshake, it is claimed only by the worker the caller's connection
/// reaches, which the claim pins it to. Never delivered, and erased with
/// its run if the caller never comes (`callers_never_arrived`).
// SYNC: AWAITS_CALLER <-> crates/weft-task-store/src/kinds.rs (LiveConnectionStart::arrive_by)
// A task with no live connection answers false, not NULL: `NOT NULL` is
// NULL, and the delivery would skip every ordinary task.
pub const AWAITS_CALLER: &str = "COALESCE(payload -> 'live_connection' ? 'arrive_by', FALSE)";

/// The claim around a pick: `$1` the claimant, `$2` now, `$3` the lease's
/// end. A live run waiting for its caller is pinned to the claimant. `RETURNING` hands back the row as claimed, so `attempts` counts the
/// claim the caller now holds. A new claim is the run that sees everything
/// asked before it, so it clears `rerun_requested`, and it is the delivery
/// the task was waiting for, so it clears `delivered_until_unix`.
fn claim_sql(pick: &str) -> String {
    format!(
        "UPDATE task \
         SET status = 'claimed', claimed_by = $1, claimed_until_unix = $3, attempts = attempts + 1, \
             rerun_requested = FALSE, delivered_until_unix = NULL, \
             target_replica = CASE WHEN {AWAITS_CALLER} THEN $1 ELSE target_replica END \
         WHERE id = ({pick}) \
         RETURNING id, kind, status, project_id, execution_id, tenant_id, binary_hash, attempts, payload"
    )
}

/// The oldest dispatcher task that is pending or whose claim lapsed.
const DISPATCHER_PICK: &str = r#"SELECT id FROM task
    WHERE target = 'dispatcher'
      AND (status = 'pending' OR (status = 'claimed' AND claimed_until_unix < $2))
      AND (target_replica IS NULL OR target_replica = $1)
    ORDER BY created_at_unix ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1"#;

/// The execute or resume task of execution `$5` of project `$4`, pending
/// or with a lapsed claim, that replica `$1` may run: unpinned, or
/// pinned to it (a live run is pinned to the replica its caller's
/// connection reached), and only while no OTHER task of the execution is
/// being driven (see [`NOT_DRIVEN_ELSEWHERE`]). The order is a tie-break:
/// the oldest first.
const EXECUTION_ID_PICK: &str = concat!(
    r#"SELECT id FROM task
    WHERE target = 'worker'
      AND project_id = $4
      AND execution_id = $5
      AND kind IN ('execute', 'resume')
      AND (status = 'pending' OR (status = 'claimed' AND claimed_until_unix < $2))
      AND (target_replica IS NULL OR target_replica = $1)
      AND "#,
    not_driven_elsewhere!(),
    r#"
    ORDER BY created_at_unix ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1"#
);

/// One execution is driven by one claim at a time. A resume can be asked for
/// while its execution is still being driven (a person answers a form
/// between the node registering its wait and the drive noticing it
/// suspended); run then, it would fold the journal a second time and run
/// the parked node again beside the live drive, so every side effect
/// below it would happen twice. It waits instead: the live drive resumes
/// the answer in place, and the resume, claimed once that drive ended,
/// folds a journal that already holds it. A lapsed claim does not count
/// (its driver is gone). `task` is the candidate row, `$2` now.
// SYNC: not_driven_elsewhere <-> take_deliveries (the same rule, inlined)
macro_rules! not_driven_elsewhere {
    () => {
        "NOT EXISTS (SELECT 1 FROM task other \
             WHERE other.execution_id = task.execution_id AND other.id <> task.id \
               AND other.kind IN ('execute', 'resume') \
               AND other.status = 'claimed' AND other.claimed_until_unix >= $2)"
    };
}
use not_driven_elsewhere;

/// One execution to hand to the project's workers: what
/// [`take_deliveries`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub task_id: Uuid,
    pub project_id: Uuid,
    pub tenant_id: String,
    pub execution_id: String,
    /// The worker image the execution runs on.
    pub binary_hash: String,
    pub run_class: weft_core::run_class::RunClass,
}

/// How long a delivery holds its task before another may be made: time
/// for a worker to start and claim, at this install's pace. A worker
/// that took the delivery claims long before; one that never does
/// (it could not start) is handed the execution again after this.
pub fn delivery_lease_secs() -> i64 {
    weft_core::time_scale::scaled_secs(120)
}

/// What one [`take_deliveries`] pass took.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Taken {
    pub deliveries: Vec<Delivery>,
    /// Rows that can never be delivered (no image, no run class), for
    /// the caller to end: the task store cannot write the journal, so
    /// the execution's terminal is the dispatcher's to write, then
    /// [`fail_undeliverable`] fails the task. Each stays taken (its
    /// delivery lease) meanwhile, and one the caller never ended is
    /// taken again once the lease runs out.
    pub undeliverable: Vec<Undeliverable>,
}

impl Taken {
    /// How many rows the pass took, deliverable or not.
    pub fn len(&self) -> usize {
        self.deliveries.len() + self.undeliverable.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A worker task [`take_deliveries`] took but cannot deliver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Undeliverable {
    pub task_id: Uuid,
    /// The execution the task drives, when the row names one that
    /// parses; `None` leaves only the task to fail.
    pub execution_id: Option<weft_core::ExecutionId>,
    pub reason: String,
}

/// Take up to `limit` executions that need a worker, and mark each as
/// delivered until [`delivery_lease_secs`] from now so no sibling (and no
/// later sweep) delivers it again meanwhile.
///
/// An execution needs a worker when its execute or resume task is pending
/// or its claim lapsed (the worker that had it died), no delivery of it is
/// outstanding, and no other task of its execution is being driven (the one
/// execution, one claim rule of `EXECUTION_ID_PICK`; the sweep that follows the
/// driving task's end delivers it). A task pinned to a replica is never delivered: it
/// is a live run, driven inside its caller's own connection; nor is a live
/// run still waiting for its caller ([`AWAITS_CALLER`]). A worker task
/// that cannot be delivered (no image, no run class) comes back in
/// [`Taken::undeliverable`] with its reason rather than delivered to a
/// guess, and never holds back the good rows taken beside it.
pub async fn take_deliveries(pool: &PgPool, limit: i64) -> Result<Taken> {
    let now = unix_now();
    let rows = sqlx::query(&format!(
        r#"UPDATE task SET delivered_until_unix = $2
           WHERE id IN (
               SELECT id FROM task
               WHERE target = 'worker'
                 AND kind IN ('execute', 'resume')
                 AND target_replica IS NULL
                 AND NOT {AWAITS_CALLER}
                 AND (status = 'pending' OR (status = 'claimed' AND claimed_until_unix < $1))
                 AND (delivered_until_unix IS NULL OR delivered_until_unix < $1)
                 AND NOT EXISTS (SELECT 1 FROM task other
                     WHERE other.execution_id = task.execution_id AND other.id <> task.id
                       AND other.kind IN ('execute', 'resume')
                       AND other.status = 'claimed' AND other.claimed_until_unix >= $1)
               ORDER BY created_at_unix ASC
               FOR UPDATE SKIP LOCKED
               LIMIT $3
           )
           RETURNING id, project_id, tenant_id, execution_id, binary_hash, payload ->> 'run_class' AS run_class"#,
    ))
    .bind(now)
    .bind(now + delivery_lease_secs())
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let mut taken = Taken::default();
    for row in rows {
        let task_id: Uuid = row.try_get("id")?;
        match delivery_of(task_id, &row) {
            Ok(delivery) => taken.deliveries.push(delivery),
            Err(reason) => {
                let execution_id: Option<String> = row.try_get("execution_id")?;
                taken.undeliverable.push(Undeliverable {
                    task_id,
                    execution_id: execution_id.and_then(|c| c.parse().ok()),
                    reason,
                });
            }
        }
    }
    Ok(taken)
}

/// Read one taken row as a [`Delivery`], or the reason it cannot be one.
fn delivery_of(task_id: Uuid, row: &sqlx::postgres::PgRow) -> std::result::Result<Delivery, String> {
    let get = |e: sqlx::Error| format!("worker task {task_id}: {e}");
    let project_id: Option<Uuid> = row.try_get("project_id").map_err(get)?;
    let execution_id: Option<String> = row.try_get("execution_id").map_err(get)?;
    let binary_hash: Option<String> = row.try_get("binary_hash").map_err(get)?;
    // Every execute and resume is built by one spec that always writes
    // its class (`kinds::ExecutionPayload::run_class`), so a missing one
    // is a corrupt row, never "short".
    let run_class: Option<String> = row.try_get("run_class").map_err(get)?;
    let run_class = weft_core::run_class::RunClass::parse(
        &run_class.ok_or_else(|| format!("worker task {task_id} names no run class"))?,
    )
    .map_err(|e| format!("worker task {task_id}: {e}"))?;
    Ok(Delivery {
        task_id,
        project_id: project_id.ok_or_else(|| format!("worker task {task_id} names no project"))?,
        tenant_id: row.try_get("tenant_id").map_err(get)?,
        execution_id: execution_id.ok_or_else(|| format!("worker task {task_id} names no execution"))?,
        binary_hash: binary_hash.ok_or_else(|| format!("worker task {task_id} names no worker image to run on"))?,
        run_class,
    })
}

/// Fail a task [`take_deliveries`] took but cannot deliver: pending, or
/// claimed with a lapsed claim (no live driver), as the delivery found it.
/// A no-op on a task another dispatcher already failed, so two copies
/// ending the same row agree.
pub async fn fail_undeliverable(pool: &PgPool, task_id: Uuid, error: &str) -> Result<()> {
    let now = unix_now();
    sqlx::query(&notify_terminal(
        r#"UPDATE task
           SET status = 'failed', error = $1, completed_at_unix = $2,
               claimed_until_unix = NULL
           WHERE id = $3
             AND (status = 'pending' OR (status = 'claimed' AND claimed_until_unix < $2))"#,
    ))
    .bind(error)
    .bind(now)
    .bind(task_id)
    .fetch_all(pool)
    .await?;
    Ok(())
}

/// Give a delivery back: the worker could not be reached, so the next
/// sweep may deliver the task at once instead of waiting out the lease.
pub async fn release_delivery(pool: &PgPool, task_id: Uuid) -> Result<()> {
    sqlx::query("UPDATE task SET delivered_until_unix = NULL WHERE id = $1 AND status = 'pending'")
        .bind(task_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// A cancel waiting for the worker that drives its execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAsked {
    pub execution_id: String,
    pub cause: weft_core::exec::CancelCause,
}

/// Take the pending `cancel_execution` tasks of `project_id` for any of
/// `execution_ids` (the executions the asking worker drives), completing each as
/// it is taken: the worker fires the execution's flag the moment it hears.
pub async fn take_cancels(pool: &PgPool, project_id: Uuid, execution_ids: &[String]) -> Result<Vec<CancelAsked>> {
    if execution_ids.is_empty() {
        return Ok(Vec::new());
    }
    let now = unix_now();
    // Taken and announced finished in one statement: a canceller waiting
    // on the task hears it the moment the worker has it.
    let rows = sqlx::query(&format!(
        "WITH taken AS ( \
             UPDATE task SET status = 'complete', completed_at_unix = $1, result = 'null'::jsonb \
             WHERE target = 'worker' AND kind = 'cancel_execution' AND status = 'pending' \
               AND project_id = $2 AND execution_id = ANY($3) \
             RETURNING id, execution_id, payload) \
         SELECT execution_id, payload, pg_notify('{}', id::text) FROM taken",
        crate::terminal::TERMINAL_CHANNEL
    ))
    .bind(now)
    .bind(project_id)
    .bind(execution_ids)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let payload: Value = row.try_get("payload")?;
            let p: crate::kinds::CancelExecutionPayload = serde_json::from_value(payload)?;
            let execution_id: Option<String> = row.try_get("execution_id")?;
            Ok(CancelAsked { execution_id: execution_id.unwrap_or(p.execution_id), cause: p.cause })
        })
        .collect()
}

/// Renew the claim's lease. Returns false if the row no longer
/// belongs to us (lease lost, manually transitioned, deleted).
/// The caller should abandon work and let the next claim recover.
pub async fn heartbeat(pool: &PgPool, task_id: Uuid, replica: &str) -> Result<bool> {
    let now = unix_now();
    let claim_until = now + claim_duration_secs();
    let rows = sqlx::query(
        r#"UPDATE task
           SET claimed_until_unix = $1
           WHERE id = $2 AND claimed_by = $3 AND status = 'claimed'"#,
    )
    .bind(claim_until)
    .bind(task_id)
    .bind(replica)
    .execute(pool)
    .await?;
    Ok(rows.rows_affected() > 0)
}

/// Surrender a claim: put the row back to `pending` with no claimant
/// so any matching process can claim it. Used by the picker when it can
/// no longer renew its lease (DB unreachable past the lease window)
/// but the work itself did not fail: terminalizing there would turn a
/// transient outage into a permanent failure. Guarded on
/// `claimed_by = $process`, so a thief that already re-claimed the row is
/// never clobbered; returns false in that case (the thief owns the
/// task) and true when the requeue landed. Keeps `target_replica`
/// (a pinned task stays addressed; surrender is not process death).
pub async fn requeue(pool: &PgPool, task_id: Uuid, replica: &str) -> Result<bool> {
    let rows = sqlx::query(
        r#"UPDATE task
           SET status = 'pending', claimed_by = NULL, claimed_until_unix = NULL
           WHERE id = $1 AND claimed_by = $2 AND status = 'claimed'"#,
    )
    .bind(task_id)
    .bind(replica)
    .execute(pool)
    .await?;
    Ok(rows.rows_affected() > 0)
}

/// Record a partial result on a still-claimed row WITHOUT completing
/// it. For executors whose work has a non-re-runnable side effect: the
/// harvested outcome is persisted here first, so a later claim of the
/// same task returns it instead of redoing the side effect. Guarded on
/// `claimed_by = $process`; a zero-row UPDATE (lease lost, row moved on)
/// fails loudly so the caller never believes an unrecorded result is
/// durable.
pub async fn store_result_partial(
    pool: &PgPool,
    task_id: Uuid,
    replica: &str,
    result: &Value,
) -> Result<()> {
    let updated = sqlx::query(
        r#"UPDATE task
           SET result = $1
           WHERE id = $2 AND claimed_by = $3 AND status = 'claimed'"#,
    )
    .bind(result)
    .bind(task_id)
    .bind(replica)
    .execute(pool)
    .await?;
    if updated.rows_affected() == 0 {
        anyhow::bail!(
            "store_result_partial: task {task_id} no longer claimed by {replica}; \
             the result was NOT recorded"
        );
    }
    Ok(())
}

/// The row's recorded result, or `None` when the row is gone or holds
/// none. Reads whatever `store_result_partial` or `complete` wrote;
/// the row outlives claims (only the terminal-retention sweep deletes
/// it), so a re-claim reads a prior claim's recorded outcome here.
pub async fn stored_result(pool: &PgPool, task_id: Uuid) -> Result<Option<Value>> {
    let row: Option<(Option<Value>,)> =
        sqlx::query_as("SELECT result FROM task WHERE id = $1")
            .bind(task_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(r,)| r))
}

/// `update` (an `UPDATE task ... ` that makes rows terminal) wrapped so
/// each row it changes notifies [`crate::terminal::TERMINAL_CHANNEL`]
/// with its id, in the same statement: the notification goes out when
/// the write commits. Returns one row per task it changed. A row the
/// update put back to pending (`rerun_requested`) is changed but not
/// terminal, so it is returned without the notification.
fn notify_terminal(update: &str) -> String {
    format!(
        "WITH done AS ({update} RETURNING id, status) \
         SELECT CASE WHEN status = 'pending' THEN NULL \
                     ELSE pg_notify('{}', id::text) END FROM done",
        crate::terminal::TERMINAL_CHANNEL
    )
}

/// The `SET` clause that ends a claim as `status`, unless it was asked
/// to run again while claimed: then it goes back to pending, unclaimed,
/// for the next claim to run (see [`enqueue_or_rearm`]).
fn end_claim_as(status: &str) -> String {
    format!(
        "status = CASE WHEN rerun_requested THEN 'pending' ELSE '{status}' END, \
         completed_at_unix = CASE WHEN rerun_requested THEN NULL ELSE $2 END, \
         claimed_by = CASE WHEN rerun_requested THEN NULL ELSE claimed_by END, \
         claimed_until_unix = NULL, \
         rerun_requested = FALSE"
    )
}

/// Mark a claim complete with a result payload. Bails if the row
/// no longer belongs to us so callers can react to lost claims.
pub async fn complete(
    pool: &PgPool,
    task_id: Uuid,
    replica: &str,
    result: Value,
) -> Result<()> {
    let now = unix_now();
    let updated = sqlx::query(&notify_terminal(
        &format!(
            "UPDATE task SET {}, result = $1 \
             WHERE id = $3 AND claimed_by = $4 AND status = 'claimed'",
            end_claim_as("complete")
        ),
    ))
    .bind(&result)
    .bind(now)
    .bind(task_id)
    .bind(replica)
    .fetch_all(pool)
    .await?;
    if updated.is_empty() {
        anyhow::bail!("complete: task {task_id} no longer claimed by {replica}");
    }
    Ok(())
}

/// Fail a task that is still PENDING (never claimed): the sweep-side
/// terminal for work that can no longer run at all, e.g. a task stamped
/// with a superseded image once no process of that image remains (nothing
/// will ever claim it; leaving it pending is an invisible forever-wait).
/// Returns false if the task moved on (claimed / completed) in the
/// meantime: someone IS handling it, so the caller backs off.
pub async fn fail_pending(pool: &PgPool, task_id: Uuid, error: &str) -> Result<bool> {
    let now = unix_now();
    let updated = sqlx::query(&notify_terminal(
        r#"UPDATE task
           SET status = 'failed', error = $1, completed_at_unix = $2
           WHERE id = $3 AND status = 'pending'"#,
    ))
    .bind(error)
    .bind(now)
    .bind(task_id)
    .fetch_all(pool)
    .await?;
    Ok(!updated.is_empty())
}

/// Mark a claim failed with an error. Bails on lost claim like
/// `complete`. Does not auto-retry; producers re-enqueue with their
/// own backoff policy.
pub async fn fail(
    pool: &PgPool,
    task_id: Uuid,
    replica: &str,
    error: String,
) -> Result<()> {
    let now = unix_now();
    let updated = sqlx::query(&notify_terminal(
        &format!(
            "UPDATE task SET {}, error = $1 \
             WHERE id = $3 AND claimed_by = $4 AND status = 'claimed'",
            end_claim_as("failed")
        ),
    ))
    .bind(&error)
    .bind(now)
    .bind(task_id)
    .bind(replica)
    .fetch_all(pool)
    .await?;
    if updated.is_empty() {
        anyhow::bail!("fail: task {task_id} no longer claimed by {replica}");
    }
    Ok(())
}

/// Outcome of `wait_for_terminal`. The dispatcher's task executor
/// returns this when a task it enqueued reaches a terminal state.
pub struct TaskOutcome {
    pub status: TaskStatus,
    pub result: Option<Value>,
    pub error: Option<String>,
}

/// [`peek`] scoped to a project: answers only when the task row
/// belongs to `project_id`, so an API route can hand back a task
/// outcome without leaking another project's tasks (ownership
/// enforced in the query, not by the caller remembering to check).
pub async fn peek_for_project(
    pool: &PgPool,
    task_id: Uuid,
    project_id: Uuid,
) -> Result<Option<TaskOutcome>> {
    // `result` is answered only once the task is terminal: a partial
    // result stored mid-claim (see `store_result_partial`) is an
    // internal re-claim handle, never a public outcome, so a poller
    // can treat "result present" as "done" without checking status.
    let row = sqlx::query(
        "SELECT status,
                CASE WHEN status IN ('complete', 'failed') THEN result END AS result,
                error
         FROM task WHERE id = $1 AND project_id = $2",
    )
    .bind(task_id)
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let status_str: String = row.try_get("status")?;
    let status =
        TaskStatus::parse(&status_str).ok_or_else(|| anyhow::anyhow!("bad status {status_str}"))?;
    Ok(Some(TaskOutcome {
        status,
        result: row.try_get("result")?,
        error: row.try_get("error")?,
    }))
}

pub(crate) async fn peek(pool: &PgPool, task_id: Uuid) -> Result<Option<TaskOutcome>> {
    // Same terminal-only `result` rule as `peek_for_project`: a
    // mid-claim partial result never leaves the store as an outcome.
    let row = sqlx::query(
        "SELECT status,
                CASE WHEN status IN ('complete', 'failed') THEN result END AS result,
                error
         FROM task WHERE id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let status_str: String = row.try_get("status")?;
    let status =
        TaskStatus::parse(&status_str).ok_or_else(|| anyhow::anyhow!("bad status {status_str}"))?;
    let result: Option<Value> = row.try_get("result")?;
    let error: Option<String> = row.try_get("error")?;
    Ok(Some(TaskOutcome {
        status,
        result,
        error,
    }))
}

/// Sweep terminal-state rows older than the retention window.
pub async fn sweep_terminal(pool: &PgPool) -> Result<u64> {
    let cutoff = unix_now() - TERMINAL_RETENTION_SECS;
    let rows = sqlx::query(
        r#"DELETE FROM task
           WHERE status IN ('complete', 'failed')
             AND completed_at_unix IS NOT NULL
             AND completed_at_unix < $1"#,
    )
    .bind(cutoff)
    .execute(pool)
    .await?;
    Ok(rows.rows_affected())
}

/// Delete the cancels nobody will take: pending for longer than a claim's
/// duration while nothing drives their execution any more (the drive ended,
/// or its worker went away, before the worker's cancel wait took them).
/// The execution's own terminal row is the cancel's record; the task was
/// only its delivery. Returns how many.
pub async fn drop_stale_cancels(pool: &PgPool) -> Result<u64> {
    let now = unix_now();
    Ok(sqlx::query(
        r#"DELETE FROM task c
           WHERE c.kind = 'cancel_execution' AND c.status = 'pending'
             AND c.created_at_unix < $1 - $2
             AND NOT EXISTS (SELECT 1 FROM task d
                 WHERE d.execution_id = c.execution_id AND d.kind IN ('execute', 'resume')
                   AND d.status = 'claimed' AND d.claimed_until_unix >= $1)"#,
    )
    .bind(now)
    .bind(claim_duration_secs())
    .execute(pool)
    .await?
    .rows_affected())
}

/// A live execution whose worker replica went away. The caller was on
/// THAT replica's connection, so the run cannot be re-run anywhere else
/// (the caller is gone with it); the dispatcher records a terminal
/// `ExecutionCancelled` for the execution so the journal does not keep a
/// started-but-unrunnable execution.
///
/// The `task_id` is carried so the reaper deletes the orphan's task ONLY
/// AFTER it recorded the cancel: the task row is the durable marker that
/// this execution still needs cancelling, so a failed cancel-record leaves it
/// for the next sweep.
pub struct OrphanedLiveExecution {
    pub task_id: Uuid,
    pub execution_id: String,
    pub project_id: Option<Uuid>,
}

/// Every live execution whose replica is gone: its pinned execute task's
/// claim lapsed (the replica stopped renewing it), or it was put back
/// pending, still pinned, and not claimed again within a claim's duration.
///
/// A read: the reaper cancels then deletes each, and a sweep that runs
/// twice re-finds the same not-yet-deleted rows (the cancel dedups).
pub async fn orphaned_live_executions(pool: &PgPool) -> Result<Vec<OrphanedLiveExecution>> {
    let now = unix_now();
    let rows = sqlx::query(
        r#"SELECT id, execution_id, project_id FROM task
           WHERE kind = 'execute'
             AND target_replica IS NOT NULL
             AND payload -> 'live_connection' IS NOT NULL
             AND payload -> 'live_connection' != 'null'::jsonb
             AND ((status = 'claimed' AND claimed_until_unix < $1)
                  OR (status = 'pending' AND created_at_unix < $1 - $2))"#,
    )
    .bind(now)
    .bind(claim_duration_secs())
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|r| {
            let task_id: Uuid = r.try_get("id")?;
            let execution_id: Option<String> = r.try_get("execution_id")?;
            let project_id: Option<Uuid> = r.try_get("project_id")?;
            let execution_id = execution_id.ok_or_else(|| anyhow::anyhow!("live execute task {task_id} has NULL execution"))?;
            Ok(OrphanedLiveExecution { task_id, execution_id, project_id })
        })
        .collect()
}

/// A live run born at its caller's handshake whose caller never reached a
/// worker before their routing token expired: nobody will ever claim it.
pub struct CallerNeverArrived {
    pub task_id: Uuid,
    pub execution_id: String,
}

/// The task row (`task`, unqualified) of a live run whose caller never
/// came: born for a caller, never claimed, past its `arrive_by` (`now` the
/// SQL parameter holding the current unix second). THE definition: the
/// reaper's read and the erase that follows both hold a row to it, so a
/// run claimed in between (even one put back pending since) is never
/// erased.
pub fn never_arrived_sql(now: &str) -> String {
    format!("({} AND (payload -> 'live_connection' ->> 'arrive_by')::bigint < {now})", unclaimed_live_sql())
}

/// The task row (`task`, unqualified) of a live run born for a caller that
/// no worker has claimed, whatever its deadline: what a handshake whose
/// call never reached a worker erases at once ([`never_arrived_sql`] is
/// the same row once its deadline passed).
pub fn unclaimed_live_sql() -> String {
    format!("(kind = 'execute' AND status = 'pending' AND target_replica IS NULL AND {AWAITS_CALLER})")
}

/// Which unclaimed live runs an erase may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnclaimedLiveRun {
    /// One whose caller never came by its deadline, as of `now`
    /// ([`never_arrived_sql`]): the reaper's.
    PastDeadline { now: i64 },
    /// One the handshake that bore it could not pass to any worker
    /// ([`unclaimed_live_sql`]): nobody holds a ticket for it, so it is
    /// erased at once rather than holding its entry slot until the
    /// deadline.
    NeverPassedOn,
}

/// Every live run whose caller never came ([`CallerNeverArrived`],
/// [`never_arrived_sql`]). A read: the reaper erases each run with its
/// task in one transaction, so a sweep that stops halfway finds the rest
/// next time.
pub async fn callers_never_arrived(pool: &PgPool, now: i64) -> Result<Vec<CallerNeverArrived>> {
    let rows = sqlx::query(&format!("SELECT id, execution_id FROM task WHERE {}", never_arrived_sql("$1")))
    .bind(now)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|r| {
            let task_id: Uuid = r.try_get("id")?;
            let execution_id: Option<String> = r.try_get("execution_id")?;
            let execution_id =
                execution_id.ok_or_else(|| anyhow::anyhow!("live execute task {task_id} has NULL execution"))?;
            Ok(CallerNeverArrived { task_id, execution_id })
        })
        .collect()
}

/// Delete a single task row by id. Used by the reaper to retire an orphaned
/// live-execution task AFTER its `ExecutionCancelled` has been journaled, so
/// the row survives (and the next sweep retries) if the cancel-record fails.
pub async fn delete_task(pool: &PgPool, id: Uuid) -> Result<()> {
    sqlx::query("DELETE FROM task WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Decode a `task` row. Every column propagates its decode error
/// via `?` (no `.expect()`, no `.ok().flatten()`): a decode failure
/// is schema drift and must fail loud, NOT silently null out
/// `project_id`/`tenant_id` (which would misroute work). The
/// nullable columns are typed `Option<_>`, so a real NULL is `None`
/// while a type mismatch is an `Err`.
fn row_to_task(row: sqlx::postgres::PgRow) -> Result<Task> {
    let id: Uuid = row.try_get("id")?;
    let kind: String = row.try_get("kind")?;
    let status_str: String = row.try_get("status")?;
    let status = TaskStatus::parse(&status_str)
        .ok_or_else(|| anyhow::anyhow!("unknown task status '{status_str}'"))?;
    let project_id: Option<Uuid> = row.try_get("project_id")?;
    let execution_id: Option<String> = row.try_get("execution_id")?;
    let tenant_id: String = row.try_get("tenant_id")?;
    let binary_hash: Option<String> = row.try_get("binary_hash")?;
    let attempts: i32 = row.try_get("attempts")?;
    let payload: Value = row.try_get("payload")?;
    Ok(Task {
        id,
        kind,
        status,
        project_id,
        execution_id,
        tenant_id,
        binary_hash,
        attempts,
        payload,
    })
}

pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs() as i64
}

/// Appoint `replica` (one running worker process) as the driver of `execution_id`.
/// Ownership normally follows the task claim (the
/// `task_claim_binds_execution_id_owner` trigger), but a process that never
/// claims a task (a node test: the dispatcher holds the task and calls the
/// test process directly) gets its driver appointed here.
/// Idempotent: a lease-loss re-claim re-stamps the same process name. A
/// missing execution row matches zero rows and fails loudly (the execution is
/// always seeded at ExecutionStarted first).
/// SYNC: writer of execution.owner_replica, see the
/// `task_claim_binds_execution_id_owner` trigger in [`GROUP`]
/// for the full writer/reader chain.
pub async fn bind_execution_id_owner(pool: &PgPool, execution_id: &str, replica: &str) -> Result<()> {
    let updated = sqlx::query(
        "UPDATE execution SET owner_replica = $2 WHERE execution_id = $1",
    )
    .bind(execution_id)
    .bind(replica)
    .execute(pool)
    .await?
    .rows_affected();
    if updated == 0 {
        anyhow::bail!("execution {execution_id} has no execution row to bind an owner onto");
    }
    Ok(())
}

#[cfg(test)]
mod wire_tests {
    // Layer-2 wire-shape tests: NewTask and Task are the JSON contract on
    // `/v1/task/enqueue_dedup` (producer sends NewTask, the row round-trips as
    // Task). Round-trip them through serde_json so a renamed/retyped field breaks
    // the test, not a live enqueue. `kind` is a free String so the runtime can add
    // its own task kinds without widening the built-in TaskKind enum.
    use super::*;

    #[test]
    fn new_task_json_round_trips() {
        let original = NewTask {
            kind: "build_image".to_string(),
            target: TaskTarget::Worker,
            project_id: Some(Uuid::from_u128(1)),
            dedup_key: Some("d1".to_string()),
            execution_id: Some("c1".to_string()),
            tenant_id: "t1".to_string(),
            target_replica: Some("replica-0".to_string()),
            binary_hash: Some("abc123".to_string()),
            payload: serde_json::json!({ "a": 1, "nested": [true, null] }),
        };
        let json = serde_json::to_string(&original).unwrap();
        let back: NewTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, original.kind);
        assert_eq!(back.target, original.target);
        assert_eq!(back.project_id, original.project_id);
        assert_eq!(back.dedup_key, original.dedup_key);
        assert_eq!(back.execution_id, original.execution_id);
        assert_eq!(back.tenant_id, original.tenant_id);
        assert_eq!(back.target_replica, original.target_replica);
        assert_eq!(back.binary_hash, original.binary_hash);
        assert_eq!(back.payload, original.payload);
        // The kind travels as a raw string, not a tagged enum.
        assert!(json.contains("\"kind\":\"build_image\""));
        // snake_case target on the wire.
        assert!(json.contains("\"target\":\"worker\""));
    }

    #[test]
    fn new_task_tolerates_an_omitted_binary_hash() {
        // The one behavior `#[serde(default)]` on `binary_hash` guarantees: a
        // producer that omits the field entirely (not `null`, ABSENT) still
        // deserializes, to None. This is the wire contract the attribute exists
        // for; without this test its removal would pass the round-trip above.
        let json = r#"{
            "kind": "fire_signal",
            "target": "dispatcher",
            "project_id": null,
            "dedup_key": null,
            "execution": null,
            "tenant_id": "t",
            "target_replica": null,
            "payload": {}
        }"#;
        let back: NewTask = serde_json::from_str(json).unwrap();
        assert_eq!(back.binary_hash, None);
    }

    #[test]
    fn task_json_round_trips_with_null_optionals() {
        let original = Task {
            id: Uuid::nil(),
            kind: "register_signal".to_string(),
            status: TaskStatus::Pending,
            project_id: None,
            execution_id: None,
            tenant_id: "t".into(),
            binary_hash: Some("original-image".into()),
            attempts: 2,
            payload: serde_json::json!(null),
        };
        let json = serde_json::to_string(&original).unwrap();
        let back: Task = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, original.id);
        assert_eq!(back.kind, original.kind);
        assert_eq!(back.status, original.status);
        assert_eq!(back.project_id, original.project_id);
        assert_eq!(back.execution_id, original.execution_id);
        assert_eq!(back.tenant_id, original.tenant_id);
        assert_eq!(back.binary_hash, original.binary_hash);
        assert_eq!(back.attempts, original.attempts);
        assert_eq!(back.payload, original.payload);
        assert!(json.contains("\"status\":\"pending\""));
    }

    /// Every task is somebody's: a task naming no tenant is refused on
    /// the wire, never stored as one that would never dedup.
    #[test]
    fn a_task_without_a_tenant_is_refused() {
        let json = r#"{
            "kind": "fire_signal",
            "target": "dispatcher",
            "project_id": null,
            "dedup_key": null,
            "execution": null,
            "tenant_id": null,
            "target_replica": null,
            "payload": {}
        }"#;
        assert!(serde_json::from_str::<NewTask>(json).is_err());
    }

    #[test]
    fn task_tolerates_an_omitted_attempts() {
        // `#[serde(default)]` on `attempts`: a producer that omits the
        // field entirely still deserializes, to 0 (meaning "unknown /
        // not a claim's view of the row").
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000000",
            "kind": "execute",
            "status": "pending",
            "project_id": null,
            "execution": null,
            "tenant_id": "t",
            "payload": {}
        }"#;
        let back: Task = serde_json::from_str(json).unwrap();
        assert_eq!(back.attempts, 0);
    }
}
