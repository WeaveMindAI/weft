//! `task` table: the dispatcher's durable work queue. Producers (a worker
//! through the broker, a listener, the dispatcher itself) `enqueue`; one
//! dispatcher process claims a row via `claim_one` (FOR UPDATE SKIP
//! LOCKED), runs the work, then `complete` or `fail`. Heartbeat extends the
//! claim's lease so a slow op doesn't lose the row to the stale-recovery
//! filter. A run is never a task: its own row (`run`) is its claim.
//!
//! Idempotency: every executor MUST be safe to re-run on partial
//! success. A row can be run again after a process crash (the lease
//! expires and `claim_one` rescues it) or after a surrender (the
//! claimer could not renew its lease and `surrender`ed the row). Cluster
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
    /// The run that asked for the work, when one did.
    pub execution_id: Option<weft_core::ExecutionId>,
    /// The tenant the task belongs to: every task has one, and the dedup
    /// uniqueness is scoped by it.
    pub tenant_id: String,
    /// How many times this row has been claimed, INCLUDING the claim
    /// that returned this value. 1 on the first claim; > 1 means a
    /// prior claim existed (lease expired, or the claimer surrendered
    /// and requeued), which an executor guarding a non-re-runnable
    /// side effect reads to tell a fresh run from a retry.
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
    pub project_id: Option<Uuid>,
    pub dedup_key: Option<String>,
    /// The run that asks for the work, when one does.
    pub execution_id: Option<weft_core::ExecutionId>,
    /// The tenant the task belongs to: required, because the dedup
    /// uniqueness is scoped by it (a NULL tenant would never dedup) and
    /// every task is somebody's.
    pub tenant_id: String,
    pub payload: Value,
}

/// The channel a task that has just become claimable (inserted pending,
/// or put back to pending by a requeue or a reclaim) is announced on, from
/// the `task_ready_notify` trigger in [`GROUP`] (through
/// `crate::announce`). A lease that merely expires announces nothing: a
/// picker's own deadline is what rescues it.
// SYNC: TASK_READY_CHANNEL <-> 'weft_task_ready' in the `task_ready_notify` function in GROUP
pub const TASK_READY_CHANNEL: &str = "weft_task_ready";

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
            project_id UUID,
            dedup_key TEXT,
            execution_id UUID,
            tenant_id TEXT NOT NULL,
            payload JSONB NOT NULL,
            claimed_by TEXT,
            claimed_until_unix BIGINT,
            attempts INTEGER NOT NULL DEFAULT 0,
            result JSONB,
            error TEXT,
            created_at_unix BIGINT NOT NULL,
            completed_at_unix BIGINT
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_pending
            ON task(created_at_unix)
            WHERE status = 'pending'"#,
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
        r#"CREATE INDEX IF NOT EXISTS idx_task_tenant ON task(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_project
            ON task(project_id)
            WHERE project_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_terminal_completed
            ON task(completed_at_unix)
            WHERE status IN ('complete', 'failed')"#,
        // The lock that serializes producers of one live task, held until
        // the transaction ends: the ONE spelling of its key, taken by
        // `weft_enqueue_dedup`.
        r#"CREATE OR REPLACE FUNCTION weft_lock_dedup(p_tenant TEXT, p_kind TEXT, p_dedup TEXT) RETURNS VOID AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended(p_tenant || '|' || p_kind || '|' || p_dedup, 0));
            END;
            $$ LANGUAGE plpgsql"#,
        // THE dedup'd enqueue ([`enqueue_dedup_in`]): the task already
        // live under `(tenant, kind, dedup_key)`, or the new one. A
        // transaction-scoped lock on that triple serializes two producers
        // of the same task, so the second finds the first's row instead of
        // tripping the unique index. Run inside the caller's transaction,
        // which the lock lasts.
        r#"CREATE OR REPLACE FUNCTION weft_enqueue_dedup(
                p_id UUID, p_kind TEXT, p_project UUID, p_dedup TEXT, p_execution UUID,
                p_tenant TEXT, p_payload JSONB, p_now BIGINT,
                OUT task_id UUID, OUT inserted BOOLEAN
            ) AS $$
            BEGIN
                PERFORM weft_lock_dedup(p_tenant, p_kind, p_dedup);
                SELECT t.id INTO task_id FROM task t
                    WHERE t.tenant_id = p_tenant AND t.kind = p_kind AND t.dedup_key = p_dedup
                      AND t.status IN ('pending', 'claimed')
                    LIMIT 1;
                IF FOUND THEN
                    inserted := FALSE;
                    RETURN;
                END IF;
                INSERT INTO task (id, kind, status, project_id, dedup_key, execution_id, tenant_id, payload, attempts, created_at_unix)
                    VALUES (p_id, p_kind, 'pending', p_project, p_dedup, p_execution, p_tenant, p_payload, 0, p_now);
                task_id := p_id;
                inserted := TRUE;
            END;
            $$ LANGUAGE plpgsql"#,
        // Announce every task that has just become claimable, so the
        // pickers sleep until there is work instead of asking on a timer.
        // From a trigger rather than from each writer, so no write path
        // (an enqueue, a requeue, the orphan reclaim) can forget it.
        // SYNC: 'weft_task_ready' <-> TASK_READY_CHANNEL
        r#"CREATE OR REPLACE FUNCTION task_ready_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM weft_announce('weft_task_ready', '');
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
    ],
    seed: &[],
};

/// Insert a new task. Returns the minted id, time-ordered (a node test's
/// run is named after its task). Does NOT enforce dedup even if
/// `spec.dedup_key` is set; use `enqueue_dedup` for that.
pub async fn enqueue(pool: &PgPool, spec: NewTask) -> Result<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        r#"INSERT INTO task (id, kind, status, project_id, dedup_key, execution_id, tenant_id, payload, attempts, created_at_unix)
           VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, 0, $8)"#,
    )
    .bind(id)
    .bind(spec.kind.as_str())
    .bind(spec.project_id)
    .bind(spec.dedup_key.as_deref())
    .bind(spec.execution_id)
    .bind(spec.tenant_id.as_str())
    .bind(&spec.payload)
    .bind(unix_now())
    .execute(pool)
    .await?;
    crate::announce::committed(pool);
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
/// AlreadyLive. The whole of it is one statement (`weft_enqueue_dedup`),
/// so on its own it is its own transaction: one round trip.
pub async fn enqueue_dedup(pool: &PgPool, spec: NewTask) -> Result<DedupOutcome> {
    let outcome = enqueue_dedup_in(&mut *pool.acquire().await?, spec).await?;
    crate::announce::committed(pool);
    Ok(outcome)
}

/// [`enqueue_dedup`] on a caller-owned connection: inside the caller's
/// transaction, the insert commits with whatever else it writes, and the
/// advisory lock lasts until it ends. The caller pokes the announcement
/// flusher once it commits (`crate::announce::committed`).
pub async fn enqueue_dedup_in(conn: &mut sqlx::PgConnection, spec: NewTask) -> Result<DedupOutcome> {
    let dedup_key = spec.dedup_key.as_deref().ok_or_else(|| anyhow::anyhow!("enqueue_dedup requires dedup_key"))?;
    let (id, inserted): (Uuid, bool) =
        sqlx::query_as("SELECT task_id, inserted FROM weft_enqueue_dedup($1, $2, $3, $4, $5, $6, $7, $8)")
            .bind(Uuid::now_v7())
            .bind(spec.kind.as_str())
            .bind(spec.project_id)
            .bind(dedup_key)
            .bind(spec.execution_id)
            .bind(spec.tenant_id.as_str())
            .bind(&spec.payload)
            .bind(unix_now())
            .fetch_one(&mut *conn)
            .await?;
    Ok(if inserted { DedupOutcome::Inserted(id) } else { DedupOutcome::AlreadyLive(id) })
}

/// Atomically claim one task for `replica` (the claiming dispatcher
/// process). Picks oldest pending first; also rescues claims whose lease
/// expired (the claimant died mid-work).
///
/// One statement: the pick locks its row (`FOR UPDATE SKIP LOCKED`, so
/// sibling claimants skip it rather than queue behind it) and the claim
/// updates it, with no round trip between.
pub async fn claim_one(pool: &PgPool, replica: &str) -> Result<Option<Task>> {
    let now = unix_now();
    let row = sqlx::query(
        r#"UPDATE task
           SET status = 'claimed', claimed_by = $1, claimed_until_unix = $3, attempts = attempts + 1
           WHERE id = (SELECT id FROM task
                       WHERE status = 'pending' OR (status = 'claimed' AND claimed_until_unix < $2)
                       ORDER BY created_at_unix ASC
                       FOR UPDATE SKIP LOCKED
                       LIMIT 1)
           RETURNING id, kind, status, project_id, execution_id, tenant_id, attempts, payload"#,
    )
    .bind(replica)
    .bind(now)
    .bind(now + claim_duration_secs())
    .fetch_optional(pool)
    .await?;
    row.map(row_to_task).transpose()
}

/// Renew the claim's lease. Returns false if the row no longer
/// belongs to us (lease lost, manually transitioned, deleted).
/// The caller should abandon work and let the next claim recover.
pub async fn heartbeat(pool: &PgPool, task_id: Uuid, replica: &str) -> Result<bool> {
    let claim_until = unix_now() + claim_duration_secs();
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

/// Surrender a claim the process can no longer hold (it cannot renew
/// it). The work itself did not fail, so the row goes back to `pending`
/// with no claimant for the next claim, and a transient outage never
/// turns into a permanent failure. Guarded on `claimed_by = $process`, so
/// a thief that already re-claimed the row is never clobbered; answers
/// false then (the thief owns the task) and true when the surrender
/// landed.
pub async fn surrender(pool: &PgPool, task_id: Uuid, replica: &str) -> Result<bool> {
    let surrendered = sqlx::query(
        r#"UPDATE task SET status = 'pending', claimed_by = NULL, claimed_until_unix = NULL
           WHERE id = $1 AND claimed_by = $2 AND status = 'claimed'"#,
    )
    .bind(task_id)
    .bind(replica)
    .execute(pool)
    .await?;
    crate::announce::committed(pool);
    Ok(surrendered.rows_affected() > 0)
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
/// each row it changes is announced on [`crate::terminal::TERMINAL_CHANNEL`]
/// with its id, in the same statement (`crate::announce`: the
/// announcement commits with the write, and its caller pokes the flusher).
/// Returns one row per task it changed.
fn notify_terminal(update: &str) -> String {
    format!(
        "WITH done AS ({update} RETURNING id) SELECT weft_announce('{}', id::text) FROM done",
        crate::terminal::TERMINAL_CHANNEL
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
    let updated = sqlx::query(&notify_terminal(
        "UPDATE task SET status = 'complete', completed_at_unix = $2, claimed_until_unix = NULL, result = $1 \
         WHERE id = $3 AND claimed_by = $4 AND status = 'claimed'",
    ))
    .bind(&result)
    .bind(unix_now())
    .bind(task_id)
    .bind(replica)
    .fetch_all(pool)
    .await?;
    crate::announce::committed(pool);
    if updated.is_empty() {
        anyhow::bail!("complete: task {task_id} no longer claimed by {replica}");
    }
    Ok(())
}

/// Fail a task that is still PENDING (never claimed): the sweep-side
/// terminal for work that can no longer run at all. Returns false if the
/// task moved on (claimed / completed) in the meantime: someone IS
/// handling it, so the caller backs off.
pub async fn fail_pending(pool: &PgPool, task_id: Uuid, error: &str) -> Result<bool> {
    let updated = sqlx::query(&notify_terminal(
        r#"UPDATE task
           SET status = 'failed', error = $1, completed_at_unix = $2
           WHERE id = $3 AND status = 'pending'"#,
    ))
    .bind(error)
    .bind(unix_now())
    .bind(task_id)
    .fetch_all(pool)
    .await?;
    crate::announce::committed(pool);
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
    let updated = sqlx::query(&notify_terminal(
        "UPDATE task SET status = 'failed', completed_at_unix = $2, claimed_until_unix = NULL, error = $1 \
         WHERE id = $3 AND claimed_by = $4 AND status = 'claimed'",
    ))
    .bind(&error)
    .bind(unix_now())
    .bind(task_id)
    .bind(replica)
    .fetch_all(pool)
    .await?;
    crate::announce::committed(pool);
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
    row.map(row_to_outcome).transpose()
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
    row.map(row_to_outcome).transpose()
}

fn row_to_outcome(row: sqlx::postgres::PgRow) -> Result<TaskOutcome> {
    let status_str: String = row.try_get("status")?;
    let status = TaskStatus::parse(&status_str).ok_or_else(|| anyhow::anyhow!("bad status {status_str}"))?;
    Ok(TaskOutcome { status, result: row.try_get("result")?, error: row.try_get("error")? })
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

/// Decode a `task` row. Every column propagates its decode error
/// via `?` (no `.expect()`, no `.ok().flatten()`): a decode failure
/// is schema drift and must fail loud, NOT silently null out
/// `project_id`/`tenant_id` (which would misroute work). The
/// nullable columns are typed `Option<_>`, so a real NULL is `None`
/// while a type mismatch is an `Err`.
fn row_to_task(row: sqlx::postgres::PgRow) -> Result<Task> {
    let status_str: String = row.try_get("status")?;
    let status = TaskStatus::parse(&status_str)
        .ok_or_else(|| anyhow::anyhow!("unknown task status '{status_str}'"))?;
    Ok(Task {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        status,
        project_id: row.try_get("project_id")?,
        execution_id: row.try_get("execution_id")?,
        tenant_id: row.try_get("tenant_id")?,
        attempts: row.try_get("attempts")?,
        payload: row.try_get("payload")?,
    })
}

pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs() as i64
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
            project_id: Some(Uuid::from_u128(1)),
            dedup_key: Some("d1".to_string()),
            execution_id: Some(Uuid::from_u128(2)),
            tenant_id: "t1".to_string(),
            payload: serde_json::json!({ "a": 1, "nested": [true, null] }),
        };
        let json = serde_json::to_string(&original).unwrap();
        let back: NewTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, original.kind);
        assert_eq!(back.project_id, original.project_id);
        assert_eq!(back.dedup_key, original.dedup_key);
        assert_eq!(back.execution_id, original.execution_id);
        assert_eq!(back.tenant_id, original.tenant_id);
        assert_eq!(back.payload, original.payload);
        // The kind travels as a raw string, not a tagged enum.
        assert!(json.contains("\"kind\":\"build_image\""));
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
            "project_id": null,
            "dedup_key": null,
            "execution_id": null,
            "tenant_id": null,
            "payload": {}
        }"#;
        assert!(serde_json::from_str::<NewTask>(json).is_err());
    }
}
