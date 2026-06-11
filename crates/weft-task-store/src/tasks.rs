//! `task` table: durable work queue. Producers `enqueue`; one Pod
//! claims a row via `claim_one` (FOR UPDATE SKIP LOCKED), runs the
//! work, then `complete` or `fail`. Heartbeat extends the claim's
//! lease so a slow op doesn't lose the row to the stale-recovery
//! filter.
//!
//! Idempotency: every executor MUST be safe to re-run on partial
//! success (Pod crash mid-task). Cluster ops should treat
//! "already exists" as success.
//!
//! Dedup: a partial unique index on `(tenant_id, kind, dedup_key)`
//! for live rows lets producers attach to in-flight work via
//! `enqueue_dedup`. Tenant-scoped so dedup never crosses tenants.

use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::kinds::TaskKind;

/// How long a claim is valid before another Pod can steal it. Pods
/// heartbeat the claim while they work.
pub const CLAIM_DURATION_SECS: i64 = 60;

/// How often a working Pod renews its claim. Below `CLAIM_DURATION_SECS`
/// so a slow op doesn't lose its claim to a transient hiccup.
pub const CLAIM_HEARTBEAT_INTERVAL_SECS: u64 = 15;

/// How long terminal-state rows linger before the sweeper deletes
/// them. Long enough that producers polling for results see them.
pub const TERMINAL_RETENTION_SECS: i64 = 3600;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Claimed,
    Complete,
    Failed,
}

impl TaskStatus {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "claimed" => Some(Self::Claimed),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

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
    pub project_id: Option<String>,
    pub color: Option<String>,
    pub tenant_id: Option<String>,
    pub payload: Value,
}

/// Producer-side spec for enqueuing a task. Serializable for the
/// same reason as `Task`: it's the wire shape on
/// `/v1/task/enqueue_dedup`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTask {
    pub kind: TaskKind,
    pub target: TaskTarget,
    pub project_id: Option<String>,
    pub dedup_key: Option<String>,
    pub color: Option<String>,
    pub tenant_id: Option<String>,
    /// If set, only the named pod can claim this task. Used by
    /// `cancel_execution` so a multi-pod project pool routes the
    /// cancel to the pod that owns the running color, not whoever
    /// claims first. NULL means any pod in the (target, project)
    /// scope can claim.
    pub target_pod_name: Option<String>,
    pub payload: Value,
}

/// Filter on which kinds of tasks the caller wants to claim.
/// Internally-tagged serde shape so the wire form is
/// `{"kind": "dispatcher"}` or `{"kind": "worker", "project_id": "..."}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClaimFilter {
    Dispatcher,
    Worker { project_id: String },
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
    pub fn id(&self) -> Uuid {
        match self {
            Self::Inserted(id) | Self::AlreadyLive(id) => *id,
        }
    }
}

/// Apply migrations for `task`.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS task (
            id UUID PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            project_id TEXT,
            dedup_key TEXT,
            color TEXT,
            tenant_id TEXT,
            target_pod_name TEXT,
            payload JSONB NOT NULL,
            claimed_by TEXT,
            claimed_until_unix BIGINT,
            attempts INTEGER NOT NULL DEFAULT 0,
            result JSONB,
            error TEXT,
            created_at_unix BIGINT NOT NULL,
            completed_at_unix BIGINT
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
        r#"CREATE INDEX IF NOT EXISTS idx_task_color
            ON task(color)
            WHERE color IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_tenant
            ON task(tenant_id)
            WHERE tenant_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_project
            ON task(project_id)
            WHERE project_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_task_terminal_completed
            ON task(completed_at_unix)
            WHERE status IN ('complete', 'failed')"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

/// Insert a new task. Returns the minted id. Does NOT enforce dedup
/// even if `spec.dedup_key` is set; use `enqueue_dedup` for that.
pub async fn enqueue(pool: &PgPool, spec: NewTask) -> Result<Uuid> {
    let id = Uuid::new_v4();
    let now = unix_now();
    sqlx::query(
        r#"INSERT INTO task (
            id, kind, status, target, project_id, dedup_key, color, tenant_id,
            target_pod_name, payload, attempts, created_at_unix
        ) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8, $9, 0, $10)"#,
    )
    .bind(id)
    .bind(spec.kind.as_str())
    .bind(spec.target.as_str())
    .bind(spec.project_id.as_deref())
    .bind(spec.dedup_key.as_deref())
    .bind(spec.color.as_deref())
    .bind(spec.tenant_id.as_deref())
    .bind(spec.target_pod_name.as_deref())
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
    let dedup = spec
        .dedup_key
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("enqueue_dedup requires dedup_key"))?;

    let mut tx = pool.begin().await?;
    // Lock + SELECT are scoped by tenant_id to match the
    // `(tenant_id, kind, dedup_key)` unique index: dedup never crosses
    // a tenant boundary. (`tenant_id IS NOT DISTINCT FROM $3` so a
    // NULL-tenant task dedups against other NULL-tenant tasks, matching
    // how the unique index treats them.)
    let tenant = spec.tenant_id.as_deref().unwrap_or("");
    let lock_input = format!("{}|{}|{}", tenant, spec.kind.as_str(), dedup);
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&lock_input)
        .execute(&mut *tx)
        .await?;

    let existing: Option<(Uuid,)> = sqlx::query_as(
        r#"SELECT id FROM task
           WHERE tenant_id IS NOT DISTINCT FROM $1
             AND kind = $2 AND dedup_key = $3 AND status IN ('pending', 'claimed')
           LIMIT 1"#,
    )
    .bind(spec.tenant_id.as_deref())
    .bind(spec.kind.as_str())
    .bind(dedup)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((id,)) = existing {
        tx.commit().await?;
        return Ok(DedupOutcome::AlreadyLive(id));
    }

    let id = Uuid::new_v4();
    let now = unix_now();
    sqlx::query(
        r#"INSERT INTO task (
            id, kind, status, target, project_id, dedup_key, color, tenant_id,
            target_pod_name, payload, attempts, created_at_unix
        ) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8, $9, 0, $10)"#,
    )
    .bind(id)
    .bind(spec.kind.as_str())
    .bind(spec.target.as_str())
    .bind(spec.project_id.as_deref())
    .bind(dedup)
    .bind(spec.color.as_deref())
    .bind(spec.tenant_id.as_deref())
    .bind(spec.target_pod_name.as_deref())
    .bind(&spec.payload)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(DedupOutcome::Inserted(id))
}

/// Atomically claim one task for `pod_id`. Picks oldest pending
/// first; also rescues claims whose lease expired (Pod died mid-work).
pub async fn claim_one(
    pool: &PgPool,
    pod_id: &str,
    filter: ClaimFilter,
) -> Result<Option<Task>> {
    let now = unix_now();
    let claim_until = now + CLAIM_DURATION_SECS;

    // ClaimFilter projects to two SQL parameters:
    //   - target_str: 'dispatcher' or 'worker'
    //   - project_filter: NULL for dispatcher (matches any), Some(id)
    //     for worker (matches that exact project_id).
    // The single SELECT below works for both via `($2 IS NULL OR
    // project_id = $2)`. Avoids two near-identical SQL strings that
    // can drift independently.
    let (target_str, project_filter): (&str, Option<&str>) = match &filter {
        ClaimFilter::Dispatcher => (TaskTarget::Dispatcher.as_str(), None),
        ClaimFilter::Worker { project_id } => (TaskTarget::Worker.as_str(), Some(project_id)),
    };

    let mut tx = pool.begin().await?;
    // `target_pod_name IS NULL OR target_pod_name = $pod` lets cancel
    // tasks be addressed to one specific pod in a multi-pod pool.
    // Tasks without an address are claimable by any pod matching
    // the (target, project) scope.
    // Worker-target claims must verify the picking pod is still
    // alive in `worker_pod`. Without this, a pod that's been marked
    // dead (e.g. by replace_stale_worker_if_needed during a sync that
    // bumped the binary_hash) keeps claiming tasks for the up-to-10s
    // window until its own heartbeat detects the dead row. Those
    // claims then fail when the fencing trigger rejects the
    // resulting journal writes. The DB has the source of truth;
    // let it enforce.
    //
    // Dispatcher-target claims aren't affected: dispatcher pods have
    // no `worker_pod` row at all, so the EXISTS check is gated on
    // target.
    let row = sqlx::query(
        r#"SELECT id, kind, status, project_id, color, tenant_id, payload
           FROM task
           WHERE target = $1
             AND ($2::TEXT IS NULL OR project_id = $2)
             AND (target_pod_name IS NULL OR target_pod_name = $3)
             AND (status = 'pending'
                  OR (status = 'claimed' AND claimed_until_unix < $4))
             AND (
                 target = 'dispatcher'
                 OR EXISTS (
                     SELECT 1 FROM worker_pod wp
                     WHERE wp.pod_name = $3
                       AND wp.status IN ('spawning', 'alive')
                 )
             )
           ORDER BY created_at_unix ASC
           FOR UPDATE SKIP LOCKED
           LIMIT 1"#,
    )
    .bind(target_str)
    .bind(project_filter)
    .bind(pod_id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(row) = row else {
        tx.commit().await?;
        return Ok(None);
    };

    let id: Uuid = row.try_get("id")?;

    sqlx::query(
        r#"UPDATE task
           SET status = 'claimed',
               claimed_by = $1,
               claimed_until_unix = $2,
               attempts = attempts + 1
           WHERE id = $3"#,
    )
    .bind(pod_id)
    .bind(claim_until)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(Some(row_to_task(row)?))
}

/// Renew the claim's lease. Returns false if the row no longer
/// belongs to us (lease lost, manually transitioned, deleted).
/// The caller should abandon work and let the next claim recover.
pub async fn heartbeat(pool: &PgPool, task_id: Uuid, pod_id: &str) -> Result<bool> {
    let now = unix_now();
    let claim_until = now + CLAIM_DURATION_SECS;
    let rows = sqlx::query(
        r#"UPDATE task
           SET claimed_until_unix = $1
           WHERE id = $2 AND claimed_by = $3 AND status = 'claimed'"#,
    )
    .bind(claim_until)
    .bind(task_id)
    .bind(pod_id)
    .execute(pool)
    .await?;
    Ok(rows.rows_affected() > 0)
}

/// Mark a claim complete with a result payload. Bails if the row
/// no longer belongs to us so callers can react to lost claims.
pub async fn complete(
    pool: &PgPool,
    task_id: Uuid,
    pod_id: &str,
    result: Value,
) -> Result<()> {
    let now = unix_now();
    let updated = sqlx::query(
        r#"UPDATE task
           SET status = 'complete',
               result = $1,
               completed_at_unix = $2,
               claimed_until_unix = NULL
           WHERE id = $3 AND claimed_by = $4 AND status = 'claimed'"#,
    )
    .bind(&result)
    .bind(now)
    .bind(task_id)
    .bind(pod_id)
    .execute(pool)
    .await?;
    if updated.rows_affected() == 0 {
        anyhow::bail!("complete: task {task_id} no longer claimed by {pod_id}");
    }
    Ok(())
}

/// Mark a claim failed with an error. Bails on lost claim like
/// `complete`. Does not auto-retry; producers re-enqueue with their
/// own backoff policy.
pub async fn fail(
    pool: &PgPool,
    task_id: Uuid,
    pod_id: &str,
    error: String,
) -> Result<()> {
    let now = unix_now();
    let updated = sqlx::query(
        r#"UPDATE task
           SET status = 'failed',
               error = $1,
               completed_at_unix = $2,
               claimed_until_unix = NULL
           WHERE id = $3 AND claimed_by = $4 AND status = 'claimed'"#,
    )
    .bind(&error)
    .bind(now)
    .bind(task_id)
    .bind(pod_id)
    .execute(pool)
    .await?;
    if updated.rows_affected() == 0 {
        anyhow::bail!("fail: task {task_id} no longer claimed by {pod_id}");
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

/// Poll a task row until it reaches terminal state, or timeout.
pub async fn wait_for_terminal(
    pool: &PgPool,
    task_id: Uuid,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<TaskOutcome> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let outcome = peek(pool, task_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("task {task_id} disappeared"))?;
        if matches!(outcome.status, TaskStatus::Complete | TaskStatus::Failed) {
            return Ok(outcome);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(outcome);
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn peek(pool: &PgPool, task_id: Uuid) -> Result<Option<TaskOutcome>> {
    let row = sqlx::query("SELECT status, result, error FROM task WHERE id = $1")
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
    let project_id: Option<String> = row.try_get("project_id")?;
    let color: Option<String> = row.try_get("color")?;
    let tenant_id: Option<String> = row.try_get("tenant_id")?;
    let payload: Value = row.try_get("payload")?;
    Ok(Task {
        id,
        kind,
        status,
        project_id,
        color,
        tenant_id,
        payload,
    })
}

pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs() as i64
}
