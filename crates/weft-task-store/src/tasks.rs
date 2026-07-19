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
    /// The task kind as its raw STRING (the value stored on `task.kind` and
    /// dispatched on). Built-in producers pass `TaskKind::X.into()`; a runtime
    /// with its own task kinds passes its own kind string (e.g. `"build_image"`)
    /// directly, so an added kind never has to widen the built-in `TaskKind` enum.
    pub kind: String,
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
    /// The worker IMAGE this task must run on: the project's
    /// `running_binary_hash` at enqueue time. An UNPINNED worker task
    /// is only claimable by a pod whose baked `worker_pod.binary_hash`
    /// matches, so new work never lands on a stale-image worker (which
    /// would run a binary missing the current graph's node impls); the
    /// cold-start sweep sees a project whose only pods are stale as
    /// having NO admittable pod and spawns a fresh one, and the stale
    /// pods idle-exit on their own. Pinned tasks bypass the check (a
    /// cancel/resume addressed to the color's owner must reach THAT
    /// pod regardless of its image). NULL = any pod (dispatcher-target
    /// tasks, and worker tasks with no image requirement).
    #[serde(default)]
    pub binary_hash: Option<String>,
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
    /// The enqueue was fenced by the placement-generation check (a stale
    /// held-event fire from a pod drained during a scale-down move). No
    /// task was created; the caller treats it as a successful no-op.
    /// Only the broker-backed FireSignal path can produce this; the
    /// local Postgres `enqueue_dedup` never fences (it has no generation
    /// context), so its callers never see it.
    Fenced,
}

impl DedupOutcome {
    /// The created task id, or `None` when the enqueue was fenced.
    pub fn id(&self) -> Option<Uuid> {
        match self {
            Self::Inserted(id) | Self::AlreadyLive(id) => Some(*id),
            Self::Fenced => None,
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
            binary_hash TEXT,
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
            target_pod_name, binary_hash, payload, attempts, created_at_unix
        ) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8, $9, $10, 0, $11)"#,
    )
    .bind(id)
    .bind(spec.kind.as_str())
    .bind(spec.target.as_str())
    .bind(spec.project_id.as_deref())
    .bind(spec.dedup_key.as_deref())
    .bind(spec.color.as_deref())
    .bind(spec.tenant_id.as_deref())
    .bind(spec.target_pod_name.as_deref())
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
    let tenant = spec.tenant_id.as_deref().unwrap_or("");
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
    .bind(spec.tenant_id.as_deref())
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
            id, kind, status, target, project_id, dedup_key, color, tenant_id,
            target_pod_name, binary_hash, payload, attempts, created_at_unix
        ) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8, $9, $10, 0, $11)"#,
    )
    .bind(id)
    .bind(spec.kind.as_str())
    .bind(spec.target.as_str())
    .bind(spec.project_id.as_deref())
    .bind(dedup)
    .bind(spec.color.as_deref())
    .bind(spec.tenant_id.as_deref())
    .bind(spec.target_pod_name.as_deref())
    .bind(spec.binary_hash.as_deref())
    .bind(&spec.payload)
    .bind(now)
    .execute(&mut *conn)
    .await?;
    Ok(DedupOutcome::Inserted(id))
}

/// The pod a live execution was admitted to (its k8s name + namespace),
/// returned by [`admit_live_execution`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedPod {
    pub pod_name: String,
    pub namespace: String,
}

/// [`admit_live_execution_in`]'s outcome. `Admitted` = THIS call inserted the
/// pinned task (the caller's transaction now owns a fresh admission and should
/// write whatever must commit atomically with it). `AlreadyAdmitted` = an
/// earlier call for the same color already inserted it (idempotent retry;
/// nothing new to commit). `Saturated` = every worker is at capacity, nothing
/// was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveAdmitOutcome {
    Admitted(AdmittedPod),
    AlreadyAdmitted(AdmittedPod),
    Saturated,
}

/// ATOMICALLY admit a live execution: pick the least-PRESSURED admittable
/// worker for the project (alive, not draining, memory pressure below
/// `saturation`) and insert the pinned execute task for the spec's color on
/// it, under a per-project advisory lock. `Saturated` when every worker is
/// saturated / draining or none is alive (the caller then spawns a fresh pod
/// and retries).
///
/// MUST run inside a caller-owned transaction: the advisory lock is
/// xact-scoped, and the caller writes the execution's journal birth in the
/// same transaction so "admitted" and "journaled" can never disagree.
/// Admission IS the task insert, so "admitted" and "has a task row" can never
/// disagree either. Capacity is governed by MEMORY pressure (the same metric
/// placement and scale-down use), NOT a connection count: a worker takes live
/// executions until its memory crosses the saturation threshold, then the
/// next one spawns. The advisory lock serializes admissions for the project
/// so a burst of concurrent handshakes (across sibling dispatcher Pods)
/// doesn't all pile onto the same least-pressured pod before its next
/// heartbeat updates the pressure reading.
///
/// The spec's `dedup_key` (`{color}:execute`) collapses a crash-retry of the
/// same handshake; the spec's `binary_hash` is the project's current
/// `running_binary_hash` (only a pod baked from that image is admittable; a
/// stale-image pod would run a binary missing the current graph's node impls).
/// The spec's `target_pod_name` must be `None`: the pin IS what this picks.
pub async fn admit_live_execution_in(
    conn: &mut sqlx::PgConnection,
    spec: &NewTask,
    saturation: f64,
) -> Result<LiveAdmitOutcome> {
    let project_id = spec
        .project_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("live admission requires project_id"))?;
    let color = spec
        .color
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("live admission requires color"))?;
    let dedup = spec
        .dedup_key
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("live admission requires dedup_key"))?;
    // Serialize admissions for this project so a burst of concurrent
    // handshakes reads a consistent pressure ordering and doesn't stampede
    // one pod (same discipline as `enqueue_dedup`'s per-dedup lock, here
    // per-project).
    let lock_input = format!("live-admit|{project_id}");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&lock_input)
        .execute(&mut *conn)
        .await?;

    // Pick the least-pressured admittable worker. Same predicate as
    // `worker_pod::pick_admittable_for_project`: alive, not draining, below
    // saturation, on the CURRENT image, least-pressured first (a spawning
    // pod reads 0 until its first heartbeat, so it is correctly preferred).
    let chosen: Option<(String, String)> = sqlx::query_as(
        r#"SELECT wp.pod_name, wp.namespace
           FROM worker_pod wp
           WHERE wp.project_id = $1
             AND wp.status IN ('spawning', 'alive')
             AND NOT wp.draining
             AND wp.mem_pressure < $2
             AND ($3::TEXT IS NULL OR wp.binary_hash = $3)
           ORDER BY wp.mem_pressure ASC, wp.created_at_unix ASC, wp.pod_name ASC
           LIMIT 1"#,
    )
    .bind(project_id)
    .bind(saturation)
    .bind(spec.binary_hash.as_deref())
    .fetch_optional(&mut *conn)
    .await?;
    let Some((pod_name, namespace)) = chosen else {
        return Ok(LiveAdmitOutcome::Saturated);
    };

    // A retry of the same handshake finds its task already live; return the
    // SAME pod it was admitted to, never insert a second task for the color.
    // LEFT JOIN so a task whose pod row was already GC'd is STILL found (an
    // inner join would miss it and let us insert a duplicate). `t.target_pod_name`
    // is the durable pin; `wp.namespace` is NULL only if that pod row is gone.
    let existing: Option<(String, Option<String>)> = sqlx::query_as(
        r#"SELECT t.target_pod_name, wp.namespace
           FROM task t LEFT JOIN worker_pod wp ON wp.pod_name = t.target_pod_name
           WHERE t.tenant_id IS NOT DISTINCT FROM $1
             AND t.kind = 'execute' AND t.dedup_key = $2
             AND t.status IN ('pending', 'claimed')
           LIMIT 1"#,
    )
    .bind(spec.tenant_id.as_deref())
    .bind(dedup)
    .fetch_optional(&mut *conn)
    .await?;
    if let Some((pod_name, namespace)) = existing {
        return match namespace {
            // Already admitted on a live pod: idempotent success on that pod.
            Some(namespace) => Ok(LiveAdmitOutcome::AlreadyAdmitted(AdmittedPod { pod_name, namespace })),
            // The original pod's row is gone (it died and was GC'd): the task
            // is now an orphan the sweep will cancel. Fail loud rather than
            // insert a SECOND task for the color on a fresh pod (which would
            // leave two execute tasks for one color). The caller surfaces the
            // error; the orphan sweep cancels the dead-pod task.
            None => Err(anyhow::anyhow!(
                "live execution for color {color} was admitted to pod '{pod_name}' which is gone; \
                 the orphan sweep will cancel it"
            )),
        };
    }

    let id = Uuid::new_v4();
    let now = unix_now();
    sqlx::query(
        r#"INSERT INTO task (
            id, kind, status, target, project_id, dedup_key, color, tenant_id,
            target_pod_name, binary_hash, payload, attempts, created_at_unix
        ) VALUES ($1, 'execute', 'pending', 'worker', $2, $3, $4, $5, $6, $7, $8, 0, $9)"#,
    )
    .bind(id)
    .bind(project_id)
    .bind(dedup)
    .bind(color)
    .bind(spec.tenant_id.as_deref())
    .bind(&pod_name)
    .bind(spec.binary_hash.as_deref())
    .bind(&spec.payload)
    .bind(now)
    .execute(&mut *conn)
    .await?;
    Ok(LiveAdmitOutcome::Admitted(AdmittedPod { pod_name, namespace }))
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
    // `target_pod_name IS NULL OR target_pod_name = $pod` lets cancel /
    // resume tasks be addressed to one specific pod in a multi-pod pool.
    // Tasks without an address are claimable by any pod matching the
    // (target, project) scope.
    //
    // Worker-target claims must verify the picking pod is still alive in
    // `worker_pod`. Without this, a pod that's been marked dead (e.g. by
    // reconcile_worker during a sync that bumped the
    // binary_hash) keeps claiming tasks for the up-to-10s window until
    // its own heartbeat detects the dead row. Those claims then fail
    // when the fencing trigger rejects the resulting journal writes. The
    // DB has the source of truth; let it enforce.
    //
    // DRAINING pods: a worker being scaled down must stop taking NEW,
    // unaddressed work (else the drain never empties it), but must still
    // run work ADDRESSED to it (a resume pinned to it because it still
    // owns the color, or a cancel). So a draining pod may claim a task
    // pinned to itself but not an unpinned one. Expressed as: the task
    // is pinned to THIS pod, OR (it is unpinned AND this pod is not
    // draining). Aliveness is required either way.
    //
    // STALE-IMAGE pods: an unpinned task stamped with a `binary_hash`
    // is only claimable by a pod baked from THAT image. Without this, a
    // still-alive worker built for the previous source claims a run for
    // the new source and fails at runtime with "unknown node type" (its
    // binary lacks the new node impls). The cold-start sweep applies
    // the same predicate, so a project whose only pods are stale gets a
    // fresh spawn; the stale pods stop receiving work and idle-exit.
    // Pinned tasks bypass (they must reach their addressed pod).
    //
    // Dispatcher-target claims aren't affected: dispatcher pods have no
    // `worker_pod` row at all, so the EXISTS check is gated on target.
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
                       AND (task.target_pod_name = $3
                            OR (NOT wp.draining
                                AND (task.binary_hash IS NULL
                                     OR task.binary_hash = wp.binary_hash)))
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

/// Fail a task that is still PENDING (never claimed): the sweep-side
/// terminal for work that can no longer run at all, e.g. a task stamped
/// with a superseded image once no pod of that image remains (nothing
/// will ever claim it; leaving it pending is an invisible forever-wait).
/// Returns false if the task moved on (claimed / completed) in the
/// meantime: someone IS handling it, so the caller backs off.
pub async fn fail_pending(pool: &PgPool, task_id: Uuid, error: &str) -> Result<bool> {
    let now = unix_now();
    let updated = sqlx::query(
        r#"UPDATE task
           SET status = 'failed', error = $1, completed_at_unix = $2
           WHERE id = $3 AND status = 'pending'"#,
    )
    .bind(error)
    .bind(now)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected() > 0)
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

/// A live-caller execution that was orphaned when its pinned worker pod
/// died before (or while) running it. The caller was gateway-routed to that
/// exact dead pod, so the execution cannot be re-run elsewhere (the caller
/// is gone); the dispatcher records a terminal `ExecutionCancelled` for the
/// color so the journal does not keep a started-but-unrunnable execution.
///
/// The `task_id` is carried so the reaper can delete the orphan's task ONLY
/// AFTER it has recorded the cancel: the task row is the durable marker that
/// this color still needs cancelling. If the cancel-record fails, the task
/// survives and the next sweep re-finds it, so a color is never stranded
/// un-cancelled (the delete and the cancel are NOT one transaction; the task
/// outlives the cancel attempt as the retry handle).
pub struct OrphanedLiveExecution {
    pub task_id: Uuid,
    pub color: String,
    pub project_id: Option<String>,
}

/// Recover EVERY task stranded on a worker pod that is no longer routable
/// (its `worker_pod` row is absent, `dead`, or `done`), so no work and no
/// journal junk is left behind. TASK-DRIVEN (scans the task table, not a
/// specific pod), so it is self-healing: it runs on a timer and re-finds any
/// still-stranded task regardless of which code path marked the pod dead
/// (the stale-heartbeat reaper, the stale-image replacement, anything) and
/// regardless of how many times it has run before. This is the durable retry
/// surface: a task pinned to a non-routable pod IS the evidence that it still
/// needs recovery, and it stays until recovered.
///
/// In one transaction:
///   1. LIVE EXECUTE orphans (`kind='execute'` with a non-null
///      `live_connection`, pinned to a non-routable pod): the caller was
///      gateway-routed to that exact dead pod, so the run is unrecoverable.
///      SELECT (do NOT delete) them and return (task_id, color); the
///      dispatcher records `ExecutionCancelled` then deletes each via
///      [`delete_task`], so a cancel-record failure leaves the task in place
///      and the NEXT sweep retries (the cancel is recorded under a dedup key,
///      so a re-record is a no-op).
///   2. EVERY OTHER stranded task (ordinary execute/resume/cancel pinned to,
///      or claimed by, a non-routable pod): clear the pin and requeue to
///      `pending` so any live pod can claim it. The live-execute orphans from
///      (1) are EXCLUDED (they are terminal, not re-runnable).
///
/// "Non-routable pod" = no `worker_pod` row with `status IN
/// ('spawning','alive')` for that pod name. A task with `target_pod_name`
/// NULL and no `claimed_by` is normal pending work, untouched.
///
/// Idempotent across sibling reaper Pods: (1) is a read (a second pass
/// re-finds the same not-yet-deleted orphans; the cancel dedups), and (2) is
/// a conditional UPDATE.
pub async fn reclaim_orphaned_tasks(pool: &PgPool) -> Result<Vec<OrphanedLiveExecution>> {
    // A pinned/claimed pod name is "routable" iff a spawning/alive row exists.
    // `pinned_dead` / `claimed_dead` CTEs express "references a pod that is
    // not routable" (the EXISTS is false: row absent OR terminal).
    let mut tx = pool.begin().await?;
    // 1. Live-execute orphans pinned to a non-routable pod: SELECT + report.
    let orphans = sqlx::query(
        r#"SELECT t.id, t.color, t.project_id
           FROM task t
           WHERE t.kind = 'execute'
             AND t.status IN ('pending', 'claimed')
             AND t.target_pod_name IS NOT NULL
             AND t.payload -> 'live_connection' IS NOT NULL
             AND t.payload -> 'live_connection' != 'null'::jsonb
             AND NOT EXISTS (
                 SELECT 1 FROM worker_pod wp
                 WHERE wp.pod_name = t.target_pod_name
                   AND wp.status IN ('spawning', 'alive')
             )"#,
    )
    .fetch_all(&mut *tx)
    .await?;
    let mut orphaned = Vec::with_capacity(orphans.len());
    for r in orphans {
        // A decode failure is schema drift: fail loud, never silently skip a
        // row (that would leave a started execution un-cancelled). A live
        // execute always carries a color, so a NULL here is itself corruption.
        let task_id: Uuid = r.try_get("id")?;
        let color: Option<String> = r.try_get("color")?;
        let project_id: Option<String> = r.try_get("project_id")?;
        let Some(color) = color else {
            anyhow::bail!("live execute orphan task {task_id} has NULL color");
        };
        orphaned.push(OrphanedLiveExecution { task_id, color, project_id });
    }
    // 2. Requeue every OTHER stranded task: pinned to OR claimed by a
    //    non-routable pod, excluding the live-execute orphans from (1).
    sqlx::query(
        r#"UPDATE task t
           SET status = 'pending', claimed_by = NULL, claimed_until_unix = NULL,
               target_pod_name = NULL
           WHERE t.status IN ('pending', 'claimed')
             AND (
                 (t.target_pod_name IS NOT NULL AND NOT EXISTS (
                     SELECT 1 FROM worker_pod wp
                     WHERE wp.pod_name = t.target_pod_name
                       AND wp.status IN ('spawning', 'alive')
                 ))
                 OR
                 (t.claimed_by IS NOT NULL AND NOT EXISTS (
                     SELECT 1 FROM worker_pod wp
                     WHERE wp.pod_name = t.claimed_by
                       AND wp.status IN ('spawning', 'alive')
                 ))
             )
             AND NOT (
                 t.kind = 'execute'
                 AND t.payload -> 'live_connection' IS NOT NULL
                 AND t.payload -> 'live_connection' != 'null'::jsonb
             )"#,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(orphaned)
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

/// Outcome of [`delete_pending_live_execution_in`]: who, if anyone, will run
/// the live execution after a failed dispatcher-side setup. Tells the caller
/// whether IT must journal the cancel terminal, or whether a worker owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupFailureOutcome {
    /// The pending execute task was deleted (or never existed): NO worker will
    /// run this color, so the caller must journal the terminal cancel.
    NoWorkerWillRun,
    /// An execute task is already `claimed`: a worker owns the run and will
    /// write its own terminal. The caller must NOT cancel (that would stack a
    /// second, contradictory terminal on the same color).
    WorkerOwnsIt,
}

/// Clean up a live execution whose dispatcher-side setup FAILED (after
/// [`admit_live_execution_in`] inserted its task). Because admission IS the
/// task insert, the cleanup is: delete the task IF still `pending` (no worker
/// has run it), then check whether ANY execute task for the color still
/// exists:
///   - none remains (we deleted the pending one, or none ever existed) ->
///     `NoWorkerWillRun`: the caller journals the cancel terminal.
///   - one remains (it was already `claimed` in the commit-but-Err race) ->
///     `WorkerOwnsIt`: the worker runs it and writes its own terminal; the
///     caller must NOT also cancel.
/// This is what lets the caller avoid stacking a second terminal on a color a
/// worker is concurrently finishing. There is no slot counter to release: the
/// task row IS the slot, so deleting it frees the slot.
///
/// MUST run inside a caller-owned transaction: the caller writes the cancel
/// terminals in the same transaction, so "task deleted" and "terminal
/// journaled" can never disagree (a crash between the two would otherwise
/// leave a live-looking execution nothing will ever run or reclaim).
pub async fn delete_pending_live_execution_in(
    conn: &mut sqlx::PgConnection,
    color: &str,
) -> Result<SetupFailureOutcome> {
    sqlx::query(
        r#"DELETE FROM task
           WHERE color = $1 AND kind = 'execute' AND status = 'pending'
             AND payload -> 'live_connection' IS NOT NULL
             AND payload -> 'live_connection' != 'null'::jsonb"#,
    )
    .bind(color)
    .execute(&mut *conn)
    .await?;
    // `1::bigint` so sqlx's i64 expectation matches: a bare `1` is typed
    // int4 by Postgres and would fail the decode whenever a row IS returned
    // (the exact pitfall guarded the same way in `worker_pod`).
    let remaining: Option<(i64,)> = sqlx::query_as(
        r#"SELECT 1::bigint FROM task WHERE color = $1 AND kind = 'execute' LIMIT 1"#,
    )
    .bind(color)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(if remaining.is_some() {
        SetupFailureOutcome::WorkerOwnsIt
    } else {
        SetupFailureOutcome::NoWorkerWillRun
    })
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
            project_id: Some("p1".to_string()),
            dedup_key: Some("d1".to_string()),
            color: Some("c1".to_string()),
            tenant_id: Some("t1".to_string()),
            target_pod_name: Some("pod-0".to_string()),
            binary_hash: Some("abc123".to_string()),
            payload: serde_json::json!({ "a": 1, "nested": [true, null] }),
        };
        let json = serde_json::to_string(&original).unwrap();
        let back: NewTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, original.kind);
        assert_eq!(back.target, original.target);
        assert_eq!(back.project_id, original.project_id);
        assert_eq!(back.dedup_key, original.dedup_key);
        assert_eq!(back.color, original.color);
        assert_eq!(back.tenant_id, original.tenant_id);
        assert_eq!(back.target_pod_name, original.target_pod_name);
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
            "color": null,
            "tenant_id": null,
            "target_pod_name": null,
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
            color: None,
            tenant_id: None,
            payload: serde_json::json!(null),
        };
        let json = serde_json::to_string(&original).unwrap();
        let back: Task = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, original.id);
        assert_eq!(back.kind, original.kind);
        assert_eq!(back.status, original.status);
        assert_eq!(back.project_id, original.project_id);
        assert_eq!(back.color, original.color);
        assert_eq!(back.tenant_id, original.tenant_id);
        assert_eq!(back.payload, original.payload);
        assert!(json.contains("\"status\":\"pending\""));
    }
}
