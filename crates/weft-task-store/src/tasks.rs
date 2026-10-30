//! `task` table: the dispatcher's durable work queue. Producers (a worker
//! through the broker, a listener, the dispatcher itself) `enqueue`; one
//! dispatcher process claims a row via `claim_next` (FOR UPDATE SKIP
//! LOCKED), runs the work, then `complete` or `fail`. Heartbeat extends the
//! claim's lease so a slow op doesn't lose the row to the stale-recovery
//! filter. A run is never a task: its own row (`run`) is its claim.
//!
//! Idempotency: every executor MUST be safe to re-run on partial
//! success. A row can be run again after a process crash (the lease
//! expires and `claim_next` rescues it) or after a surrender (the
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
use uuid::Uuid;

use crate::db::{Client, JsonText, Pool, Row};

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

/// How long terminal-state rows linger before the picker, run dry,
/// deletes them (`weft_prune_finished_tasks`, handed the cutoff by
/// [`claim_next`]). Long enough that producers polling for results see
/// them.
pub const TERMINAL_RETENTION_SECS: i64 = 3600;

/// Where a task stands: weft-core's, since a client waiting on a task
/// reads it off the wire.
pub use weft_core::task::TaskStatus;

/// A row from the `task` table. Producers fill `NewTask`; consumers
/// receive `Task` from `claim_next`.
///
/// Derives serde directly: this is also the wire shape used by
/// `weft-broker-client::protocol`. A new field on this struct
/// shows up on the wire automatically; no mirror type to keep in
/// sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    /// The kind as stored, a [`crate::TaskKind`]'s tag. Kept as text: a row
    /// written by another weft version may name a kind this build does not
    /// know, and the executor fails that task by name rather than the claim
    /// failing to read it.
    pub kind: String,
    pub status: TaskStatus,
    pub project_id: Option<Uuid>,
    /// The run that asked for the work, when one did.
    pub run_id: Option<weft_core::RunId>,
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
    /// What the task is. Stored on `task.kind` as its serde tag; a producer
    /// can only ask for a kind this build knows.
    pub kind: crate::TaskKind,
    pub project_id: Option<Uuid>,
    pub dedup_key: Option<String>,
    /// The run that asks for the work, when one does.
    pub run_id: Option<weft_core::RunId>,
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
            run_id UUID,
            tenant_id TEXT NOT NULL,
            -- The task's JSON as text (`db::JsonText`), like `result` and
            -- `progress`: what a node or a service handed it must come
            -- back exactly.
            payload TEXT NOT NULL,
            claimed_by TEXT,
            -- Until when the claim holds without a heartbeat. NULL on a
            -- claim that rests on a run (`claim_rests_on`).
            claimed_until_unix BIGINT,
            -- The run a claimed task waits on while it waits (an
            -- activation's setup run, which may wait days on a person):
            -- its claimant stops renewing, and the claim holds for as
            -- long as that run goes (`weft_dispatcher::lapses`).
            claim_rests_on UUID,
            attempts INTEGER NOT NULL DEFAULT 0,
            result TEXT,
            -- Where a running task says it is, for whoever follows it
            -- (`set_progress`); its kind's own shape.
            progress TEXT,
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
        // Delete up to 500 tasks that finished before `p_before` (unix
        // seconds), skipping any row another transaction holds, and answer
        // how many went: the picker's claim calls it in the statement that
        // finds nothing left to claim ([`claim_next`], which hands it the
        // cutoff from [`TERMINAL_RETENTION_SECS`]), so the table is pruned
        // once per burst of work, never on a timer, never on an enqueue.
        r#"CREATE OR REPLACE FUNCTION weft_prune_finished_tasks(p_before BIGINT) RETURNS INTEGER AS $$
            WITH gone AS (
                DELETE FROM task WHERE id IN (
                    SELECT id FROM task
                    WHERE status IN ('complete', 'failed') AND completed_at_unix < p_before
                    LIMIT 500 FOR UPDATE SKIP LOCKED)
                RETURNING 1)
            SELECT count(*)::INTEGER FROM gone
            $$ LANGUAGE sql"#,
        // THE dedup'd enqueue ([`enqueue_dedup_in`]): the task already
        // live under `(tenant, kind, dedup_key)`, or the new one. A
        // transaction-scoped lock on that triple serializes two producers
        // of the same task, so the second finds the first's row instead of
        // tripping the unique index. Run inside the caller's transaction,
        // which the lock lasts.
        r#"CREATE OR REPLACE FUNCTION weft_enqueue_dedup(
                p_id UUID, p_kind TEXT, p_project UUID, p_dedup TEXT, p_run UUID,
                p_tenant TEXT, p_payload TEXT, p_now BIGINT,
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
                INSERT INTO task (id, kind, status, project_id, dedup_key, run_id, tenant_id, payload, attempts, created_at_unix)
                    VALUES (p_id, p_kind, 'pending', p_project, p_dedup, p_run, p_tenant, p_payload, 0, p_now);
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
    data_steps: &[],
};

/// Insert a new task. Returns the minted id, time-ordered (a node test's
/// run is named after its task). Does NOT enforce dedup even if
/// `spec.dedup_key` is set; use `enqueue_dedup` for that.
pub async fn enqueue(pool: &Pool, spec: NewTask) -> Result<Uuid> {
    let id = Uuid::now_v7();
    pool.execute(
        r#"INSERT INTO task (id, kind, status, project_id, dedup_key, run_id, tenant_id, payload, attempts, created_at_unix)
           VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, 0, $8)"#,
        &[
            &id,
            &spec.kind.as_str(),
            &spec.project_id,
            &spec.dedup_key.as_deref(),
            &spec.run_id,
            &spec.tenant_id.as_str(),
            &JsonText(&spec.payload),
            &unix_now(),
        ],
    )
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
pub async fn enqueue_dedup(pool: &Pool, spec: NewTask) -> Result<DedupOutcome> {
    let outcome = enqueue_dedup_in(pool, spec).await?;
    crate::announce::committed(pool);
    Ok(outcome)
}

/// [`enqueue_dedup`] through `db`: inside the caller's transaction, the
/// insert commits with whatever else it writes, and the advisory lock
/// lasts until it ends. The caller pokes the announcement flusher once it
/// commits (`crate::announce::committed`).
pub async fn enqueue_dedup_in(db: &impl Client, spec: NewTask) -> Result<DedupOutcome> {
    let dedup_key = spec.dedup_key.as_deref().ok_or_else(|| anyhow::anyhow!("enqueue_dedup requires dedup_key"))?;
    let (id, inserted): (Uuid, bool) = db
        .one(
            "SELECT task_id, inserted FROM weft_enqueue_dedup($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &Uuid::now_v7(),
                &spec.kind.as_str(),
                &spec.project_id,
                &dedup_key,
                &spec.run_id,
                &spec.tenant_id.as_str(),
                &JsonText(&spec.payload),
                &unix_now(),
            ],
        )
        .await?;
    Ok(if inserted { DedupOutcome::Inserted(id) } else { DedupOutcome::AlreadyLive(id) })
}

/// What [`claim_next`] found.
#[derive(Debug)]
pub enum Claim {
    /// A task, now claimed.
    Task(Task),
    /// Nothing claimable, and when the earliest claim a claimant renews
    /// lapses (unix seconds), which a later claim then rescues; `None`
    /// with no such claim (a resting claim has no lapse here).
    Dry { next_lapse: Option<i64> },
}

/// Atomically claim one task for `replica` (the claiming dispatcher
/// process). Picks oldest pending first; also rescues claims whose lease
/// expired (the claimant died mid-work).
///
/// One statement: the pick locks its row (`FOR UPDATE SKIP LOCKED`, so
/// sibling claimants skip it rather than queue behind it) and the claim
/// updates it, with no round trip between. When nothing is claimable, the
/// same statement, only in that case, reads the earliest lapse of the
/// claims out (a picker run dry knows when to look again) and prunes the
/// finished tasks past their retention (`weft_prune_finished_tasks`).
pub async fn claim_next(pool: &Pool, replica: &str) -> Result<Claim> {
    let now = unix_now();
    // The lapse and the prune are evaluated only when the CASE reaches
    // them: never when a task was claimed. The prune deletes finished rows
    // only, which the pick never touches.
    let row: Row = pool
        .one(
            r#"WITH picked AS (
                   UPDATE task
                   SET status = 'claimed', claimed_by = $1, claimed_until_unix = $3, attempts = attempts + 1
                   WHERE id = (SELECT id FROM task
                               WHERE status = 'pending' OR (status = 'claimed' AND claimed_until_unix < $2)
                               ORDER BY created_at_unix ASC
                               FOR UPDATE SKIP LOCKED
                               LIMIT 1)
                   RETURNING id, kind, status, project_id, run_id, tenant_id, attempts, payload)
               SELECT p.*,
                      CASE WHEN p.id IS NULL
                           THEN (SELECT MIN(claimed_until_unix) FROM task WHERE status = 'claimed') END AS next_lapse,
                      CASE WHEN p.id IS NULL THEN weft_prune_finished_tasks($4) END AS pruned
               FROM (SELECT 1) AS one LEFT JOIN picked p ON TRUE"#,
            &[&replica, &now, &(now + claim_duration_secs()), &(now - TERMINAL_RETENTION_SECS)],
        )
        .await?;
    if row.try_get::<_, Option<Uuid>>("id")?.is_none() {
        let pruned: Option<i32> = row.try_get("pruned")?;
        if let Some(pruned) = pruned.filter(|pruned| *pruned > 0) {
            tracing::info!(target: "weft_task_store::tasks", pruned, "pruned finished tasks past their retention");
        }
        return Ok(Claim::Dry { next_lapse: row.try_get("next_lapse")? });
    }
    Ok(Claim::Task(row_to_task(row)?))
}

/// Renew the claim's lease. Returns false if the row no longer
/// belongs to us (lease lost, manually transitioned, deleted).
/// The caller should abandon work and let the next claim recover. A
/// claim resting on a run ([`rest_claim`]) stays resting.
pub async fn heartbeat(pool: &Pool, task_id: Uuid, replica: &str) -> Result<bool> {
    let claim_until = unix_now() + claim_duration_secs();
    let rows = pool
        .execute(
            r#"UPDATE task
               SET claimed_until_unix = CASE WHEN claim_rests_on IS NULL THEN $1::bigint END
               WHERE id = $2 AND claimed_by = $3 AND status = 'claimed'"#,
            &[&claim_until, &task_id, &replica],
        )
        .await?;
    Ok(rows > 0)
}

/// Let the claim rest on `run`, a run the task waits on that may wait for
/// days: its claimant stops renewing it, and no claim rescues it
/// (`claimed_until_unix` is empty). Whoever watches that run hands the
/// task back once it ended and nobody resumed the claim
/// (`weft_dispatcher::lapses`). False when the row is no longer ours.
pub async fn rest_claim(pool: &Pool, task_id: Uuid, replica: &str, run: weft_core::RunId) -> Result<bool> {
    let rows = pool
        .execute(
            r#"UPDATE task SET claimed_until_unix = NULL, claim_rests_on = $3
               WHERE id = $1 AND claimed_by = $2 AND status = 'claimed'"#,
            &[&task_id, &replica, &run],
        )
        .await?;
    Ok(rows > 0)
}

/// Take a resting claim ([`rest_claim`]) back to a lease renewed by its
/// claimant. False when the row is no longer ours (it was handed back
/// meanwhile).
pub async fn resume_claim(pool: &Pool, task_id: Uuid, replica: &str) -> Result<bool> {
    let rows = pool
        .execute(
            r#"UPDATE task SET claimed_until_unix = $3, claim_rests_on = NULL
               WHERE id = $1 AND claimed_by = $2 AND status = 'claimed'"#,
            &[&task_id, &replica, &(unix_now() + claim_duration_secs())],
        )
        .await?;
    Ok(rows > 0)
}

/// Surrender a claim the process can no longer hold (it cannot renew
/// it). The work itself did not fail, so the row goes back to `pending`
/// with no claimant for the next claim, and a transient outage never
/// turns into a permanent failure. Guarded on `claimed_by = $process`, so
/// a thief that already re-claimed the row is never clobbered; answers
/// false then (the thief owns the task) and true when the surrender
/// landed.
pub async fn surrender(pool: &Pool, task_id: Uuid, replica: &str) -> Result<bool> {
    let surrendered = pool
        .execute(
            r#"UPDATE task SET status = 'pending', claimed_by = NULL, claimed_until_unix = NULL
               WHERE id = $1 AND claimed_by = $2 AND status = 'claimed'"#,
            &[&task_id, &replica],
        )
        .await?;
    crate::announce::committed(pool);
    Ok(surrendered > 0)
}

/// Record a partial result on a still-claimed row WITHOUT completing
/// it. For executors whose work has a non-re-runnable side effect: the
/// harvested outcome is persisted here first, so a later claim of the
/// same task returns it instead of redoing the side effect. Guarded on
/// `claimed_by = $process`; a zero-row UPDATE (lease lost, row moved on)
/// fails loudly so the caller never believes an unrecorded result is
/// durable.
pub async fn store_result_partial(
    pool: &Pool,
    task_id: Uuid,
    replica: &str,
    result: &Value,
) -> Result<()> {
    let updated = pool
        .execute(
            r#"UPDATE task
               SET result = $1
               WHERE id = $2 AND claimed_by = $3 AND status = 'claimed'"#,
            &[&JsonText(result), &task_id, &replica],
        )
        .await?;
    if updated == 0 {
        anyhow::bail!(
            "store_result_partial: task {task_id} no longer claimed by {replica}; \
             the result was NOT recorded"
        );
    }
    Ok(())
}

/// The row's recorded result, or `None` when the row is gone or holds
/// none. Reads whatever `store_result_partial` or `complete` wrote;
/// the row outlives claims (only pruning deletes
/// it), so a re-claim reads a prior claim's recorded outcome here.
pub async fn stored_result(pool: &Pool, task_id: Uuid) -> Result<Option<Value>> {
    let result: Option<Option<JsonText<Value>>> = pool.scalar_opt("SELECT result FROM task WHERE id = $1", &[&task_id]).await?;
    Ok(result.flatten().map(|result| result.0))
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
    pool: &Pool,
    task_id: Uuid,
    replica: &str,
    result: Value,
) -> Result<()> {
    let updated = pool
        .query(
            &notify_terminal(
                "UPDATE task SET status = 'complete', completed_at_unix = $2, claimed_until_unix = NULL, result = $1 \
                 WHERE id = $3 AND claimed_by = $4 AND status = 'claimed'",
            ),
            &[&JsonText(&result), &unix_now(), &task_id, &replica],
        )
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
pub async fn fail_pending(pool: &Pool, task_id: Uuid, error: &str) -> Result<bool> {
    let updated = pool
        .query(
            &notify_terminal(
                r#"UPDATE task
                   SET status = 'failed', error = $1, completed_at_unix = $2
                   WHERE id = $3 AND status = 'pending'"#,
            ),
            &[&error, &unix_now(), &task_id],
        )
        .await?;
    crate::announce::committed(pool);
    Ok(!updated.is_empty())
}

/// Mark a claim failed with an error. Bails on lost claim like
/// `complete`. Does not auto-retry; producers re-enqueue with their
/// own backoff policy.
pub async fn fail(
    pool: &Pool,
    task_id: Uuid,
    replica: &str,
    error: String,
) -> Result<()> {
    let updated = pool
        .query(
            &notify_terminal(
                "UPDATE task SET status = 'failed', completed_at_unix = $2, claimed_until_unix = NULL, error = $1 \
                 WHERE id = $3 AND claimed_by = $4 AND status = 'claimed'",
            ),
            &[&error, &unix_now(), &task_id, &replica],
        )
        .await?;
    crate::announce::committed(pool);
    if updated.is_empty() {
        anyhow::bail!("fail: task {task_id} no longer claimed by {replica}");
    }
    Ok(())
}

/// Say where task `task_id` is now, for whoever follows it ([`progress`]):
/// its kind's own shape, replacing what it said before.
pub async fn set_progress(pool: &Pool, task_id: Uuid, progress: &Value) -> Result<()> {
    pool.execute("UPDATE task SET progress = $1 WHERE id = $2", &[&JsonText(progress), &task_id]).await?;
    Ok(())
}

/// Where task `task_id` last said it is ([`set_progress`]), when it said.
pub async fn progress(pool: &Pool, task_id: Uuid) -> Result<Option<Value>> {
    Ok(pool.scalar_opt::<Option<JsonText<Value>>>("SELECT progress FROM task WHERE id = $1", &[&task_id]).await?.flatten().map(|progress| progress.0))
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
    pool: &Pool,
    task_id: Uuid,
    project_id: Uuid,
) -> Result<Option<TaskOutcome>> {
    // `result` is answered only once the task is terminal: a partial
    // result stored mid-claim (see `store_result_partial`) is an
    // internal re-claim handle, never a public outcome, so a poller
    // can treat "result present" as "done" without checking status.
    let row = pool
        .opt(
            "SELECT status,
                    CASE WHEN status IN ('complete', 'failed') THEN result END AS result,
                    error
             FROM task WHERE id = $1 AND project_id = $2",
            &[&task_id, &project_id],
        )
        .await?;
    row.map(row_to_outcome).transpose()
}

pub(crate) async fn peek(pool: &Pool, task_id: Uuid) -> Result<Option<TaskOutcome>> {
    // Same terminal-only `result` rule as `peek_for_project`: a
    // mid-claim partial result never leaves the store as an outcome.
    let row = pool
        .opt(
            "SELECT status,
                    CASE WHEN status IN ('complete', 'failed') THEN result END AS result,
                    error
             FROM task WHERE id = $1",
            &[&task_id],
        )
        .await?;
    row.map(row_to_outcome).transpose()
}

fn row_to_outcome(row: Row) -> Result<TaskOutcome> {
    let status_str: String = row.try_get("status")?;
    let status = TaskStatus::parse(&status_str).ok_or_else(|| anyhow::anyhow!("bad status {status_str}"))?;
    let result: Option<JsonText<Value>> = row.try_get("result")?;
    Ok(TaskOutcome { status, result: result.map(|result| result.0), error: row.try_get("error")? })
}

/// Decode a `task` row. Every column propagates its decode error
/// via `?` (no `.expect()`, no `.ok().flatten()`): a decode failure
/// is schema drift and must fail loud, NOT silently null out
/// `project_id`/`tenant_id` (which would misroute work). The
/// nullable columns are typed `Option<_>`, so a real NULL is `None`
/// while a type mismatch is an `Err`.
fn row_to_task(row: Row) -> Result<Task> {
    let status_str: String = row.try_get("status")?;
    let status = TaskStatus::parse(&status_str)
        .ok_or_else(|| anyhow::anyhow!("unknown task status '{status_str}'"))?;
    Ok(Task {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        status,
        project_id: row.try_get("project_id")?,
        run_id: row.try_get("run_id")?,
        tenant_id: row.try_get("tenant_id")?,
        attempts: row.try_get("attempts")?,
        payload: row.try_get::<_, JsonText<Value>>("payload")?.0,
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
    // the test, not a live enqueue.
    use super::*;

    #[test]
    fn new_task_json_round_trips() {
        let original = NewTask {
            kind: crate::TaskKind::Resync,
            project_id: Some(Uuid::from_u128(1)),
            dedup_key: Some("d1".to_string()),
            run_id: Some(Uuid::from_u128(2)),
            tenant_id: "t1".to_string(),
            payload: serde_json::json!({ "a": 1, "nested": [true, null] }),
        };
        let json = serde_json::to_string(&original).unwrap();
        let back: NewTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, original.kind);
        assert_eq!(back.project_id, original.project_id);
        assert_eq!(back.dedup_key, original.dedup_key);
        assert_eq!(back.run_id, original.run_id);
        assert_eq!(back.tenant_id, original.tenant_id);
        assert_eq!(back.payload, original.payload);
        // The kind travels as its plain tag.
        assert!(json.contains("\"kind\":\"resync\""));
    }

    #[test]
    fn task_json_round_trips_with_null_optionals() {
        let original = Task {
            id: Uuid::nil(),
            kind: crate::TaskKind::RegisterSignal.to_string(),
            status: TaskStatus::Pending,
            project_id: None,
            run_id: None,
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
        assert_eq!(back.run_id, original.run_id);
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
            "run_id": null,
            "tenant_id": null,
            "payload": {}
        }"#;
        assert!(serde_json::from_str::<NewTask>(json).is_err());
    }
}
