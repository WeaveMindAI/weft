//! `worker_pod` table + journal-fencing trigger. One row per Pod.
//! Worker boots → `register_alive`; engine heartbeats; reaper marks
//! stale rows dead. The trigger on `exec_event` rejects writes whose
//! pod_name is not in `{spawning, alive}`, fencing stale Pods out.

use anyhow::Result;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::tasks::unix_now;

pub const HEARTBEAT_INTERVAL_SECS: u64 = 10;
pub const HEARTBEAT_STALE_SECS: i64 = 30;

/// Lifecycle states for a `worker_pod` row.
///
/// `spawning` → `alive` → `done | dead`.
///   - `spawning`: dispatcher reserved the row before `kubectl apply`.
///   - `alive`: worker registered itself, heartbeat is fresh.
///   - `done`: worker exited cleanly (heartbeat loop saw the row go
///     away, mark_done called).
///   - `dead`: reaper concluded the heartbeat is stale and the worker
///     died without writing `done`.
///
/// The fencing trigger lets `spawning` and `alive` rows write to
/// `exec_event`. Anything else is rejected.
const STATUS_SPAWNING: &str = "spawning";
const STATUS_ALIVE: &str = "alive";
const STATUS_DONE: &str = "done";
const STATUS_DEAD: &str = "dead";

/// Minimal projection of the `worker_pod` table: only the columns the stale /
/// terminal reapers act on. The e2e rig reads a DIFFERENT projection of the same
/// table (it needs `status` + `terminal_at_unix`, which production reads via
/// WHERE clauses, not struct fields), so the two are honestly-distinct read
/// shapes, not a fragmented concept; keep them separate and grep-linked.
/// SYNC: worker_pod columns <-> crates/weft-e2e/src/platform.rs (WorkerPodRow)
#[derive(Debug, Clone)]
pub struct WorkerPodRow {
    pub pod_name: String,
    pub project_id: String,
    pub namespace: String,
    pub last_heartbeat_unix: i64,
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS worker_pod (
            pod_name TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            namespace TEXT NOT NULL,
            status TEXT NOT NULL,
            owner_dispatcher TEXT NOT NULL,
            last_heartbeat_unix BIGINT NOT NULL,
            created_at_unix BIGINT NOT NULL,
            -- Set when the row transitions to a terminal status
            -- (done | dead). NULL while spawning/alive. The pod-GC
            -- sweep ages terminal rows against this to delete the
            -- finished k8s Pod object after a grace window.
            terminal_at_unix BIGINT,
            -- Binary hash the pod's image was built from. Recorded at
            -- spawn time. The spawn_pod executor compares this against
            -- the project's current running_binary_hash on every spawn
            -- attempt: a mismatch means the alive pod has a stale
            -- binary (e.g. user changed a node implementation since
            -- it spawned). The dispatcher kills the stale pod and
            -- proceeds with a fresh spawn.
            -- NOTE: live-connection load is NOT stored here. A pod's live
            -- load IS the count of in-flight live-execute tasks pinned to it
            -- (the task row is the capacity slot); admission and cap
            -- enforcement happen atomically as the task insert
            -- (`tasks::admit_live_execution`). There is no denormalized
            -- counter to drift, and idle-exit (`mark_done_if_idle`) is gated
            -- by its pending/claimed-task `NOT EXISTS` check (a live-execute
            -- task is a worker task, so an in-flight live execution already
            -- blocks idle-exit).
            binary_hash TEXT NOT NULL DEFAULT ''
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_worker_pod_project_alive
            ON worker_pod(project_id)
            WHERE status IN ('spawning', 'alive')"#,
        r#"CREATE INDEX IF NOT EXISTS idx_worker_pod_heartbeat
            ON worker_pod(last_heartbeat_unix)
            WHERE status = 'alive'"#,
        // Generation fencing: a Pod whose row is not {spawning, alive}
        // cannot write to exec_event. NULL pod_name (listener /
        // dispatcher writes) bypasses the check.
        r#"CREATE OR REPLACE FUNCTION weft_check_pod_alive() RETURNS trigger AS $$
            DECLARE
                pod_status TEXT;
            BEGIN
                IF NEW.pod_name IS NULL THEN
                    RETURN NEW;
                END IF;
                SELECT status INTO pod_status
                FROM worker_pod
                WHERE pod_name = NEW.pod_name;
                IF pod_status IS NULL OR pod_status NOT IN ('spawning', 'alive') THEN
                    RAISE EXCEPTION
                        'pod % is not alive (status=%)',
                        NEW.pod_name, COALESCE(pod_status, 'missing')
                        USING ERRCODE = 'P0001';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS exec_event_pod_alive_check ON exec_event"#,
        r#"CREATE TRIGGER exec_event_pod_alive_check
            BEFORE INSERT ON exec_event
            FOR EACH ROW
            EXECUTE FUNCTION weft_check_pod_alive()"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

/// Worker flips its pre-inserted row to `alive` and stamps the
/// first heartbeat. The dispatcher's `insert_spawning` always runs
/// before the kubectl apply that creates the pod, so the row is
/// guaranteed to exist by the time the worker calls this. If it
/// doesn't (e.g. a forged pod_name from a compromised caller), the
/// UPDATE affects zero rows and we surface that as an error: there
/// is no INSERT fallback so tenant-supplied namespace/owner data
/// never reaches the row.
pub async fn register_alive(
    pool: &PgPool,
    pod_name: &str,
    project_id: &str,
) -> Result<()> {
    let now = unix_now();
    // `AND status = 'spawning'` is load-bearing: without it a dead-
    // marked row (the reaper called `mark_dead` between this worker's
    // boot and its first heartbeat) could be flipped back to `alive`
    // by the worker, defeating the generation fencing. With the
    // guard, a dead row stays dead; rows_affected = 0 surfaces the
    // condition and the worker exits via `bail!` instead of
    // resurrecting itself.
    let res = sqlx::query(
        r#"UPDATE worker_pod
           SET status = $3, last_heartbeat_unix = $4
           WHERE pod_name = $1 AND project_id = $2 AND status = $5"#,
    )
    .bind(pod_name)
    .bind(project_id)
    .bind(STATUS_ALIVE)
    .bind(now)
    .bind(STATUS_SPAWNING)
    .execute(pool)
    .await?;
    if res.rows_affected() == 0 {
        anyhow::bail!(
            "no spawning worker_pod row for pod_name='{pod_name}' \
             project_id='{project_id}'; the row was either never \
             inserted by the dispatcher or was marked dead before \
             this worker registered"
        );
    }
    Ok(())
}

/// Reserve a row before `kubectl apply`. Idempotent on retry via
/// `ON CONFLICT DO NOTHING`. The dispatcher's spawn_pod task calls
/// this so a partial-success retry collides on the same pod_name.
pub async fn insert_spawning(
    pool: &PgPool,
    pod_name: &str,
    project_id: &str,
    namespace: &str,
    owner_dispatcher: &str,
    binary_hash: &str,
) -> Result<()> {
    let now = unix_now();
    sqlx::query(
        r#"INSERT INTO worker_pod (
            pod_name, project_id, namespace, status, owner_dispatcher,
            last_heartbeat_unix, created_at_unix, binary_hash
        ) VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
        ON CONFLICT (pod_name) DO NOTHING"#,
    )
    .bind(pod_name)
    .bind(project_id)
    .bind(namespace)
    .bind(STATUS_SPAWNING)
    .bind(owner_dispatcher)
    .bind(now)
    .bind(binary_hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Engine heartbeat. Returns false if the row no longer matches
/// (status changed, row deleted) so the worker can exit.
pub async fn heartbeat(pool: &PgPool, pod_name: &str) -> Result<bool> {
    let res = sqlx::query(
        r#"UPDATE worker_pod
           SET last_heartbeat_unix = $1
           WHERE pod_name = $2 AND status = 'alive'"#,
    )
    .bind(unix_now())
    .bind(pod_name)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn mark_done(pool: &PgPool, pod_name: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE worker_pod
           SET status = $1, terminal_at_unix = $3
           WHERE pod_name = $2 AND status IN ('spawning', 'alive')"#,
    )
    .bind(STATUS_DONE)
    .bind(pod_name)
    .bind(crate::tasks::unix_now())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_dead(pool: &PgPool, pod_name: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE worker_pod
           SET status = $1, terminal_at_unix = $3
           WHERE pod_name = $2 AND status IN ('spawning', 'alive')"#,
    )
    .bind(STATUS_DEAD)
    .bind(pod_name)
    .bind(crate::tasks::unix_now())
    .execute(pool)
    .await?;
    Ok(())
}

/// Idle self-exit (worker-driven). Flip this pod's row `alive ->
/// done` IFF there is no outstanding worker work for its project
/// (no pending task to claim, no claimed task in-flight). The
/// no-work check and the status flip are ONE atomic UPDATE, so a
/// task arriving concurrently is caught:
///   - lands BEFORE the CAS commits: the `NOT EXISTS` fails, the
///     row stays `alive`, the worker keeps running and claims it;
///   - lands AFTER: the row is already `done`, cold_start sees no
///     live pod and spawns a fresh one for the new work.
/// Either way no exec is ever routed to a dying worker. Returns
/// true if this call won the flip (caller should then exit).
pub async fn mark_done_if_idle(pool: &PgPool, pod_name: &str) -> Result<bool> {
    // The no-work check is correlated to the pod's OWN project
    // (`task.project_id = worker_pod.project_id`), read from the
    // row, never from a caller-supplied value. A worker can only
    // ever flip its own row (`pod_name = $1`), and only against its
    // own project's work: there's no project_id parameter to lie
    // about.
    let res = sqlx::query(
        r#"UPDATE worker_pod wp
           SET status = $2, terminal_at_unix = $3
           WHERE wp.pod_name = $1
             AND wp.status = 'alive'
             AND NOT EXISTS (
                 SELECT 1 FROM task t
                 WHERE t.target = 'worker'
                   AND t.project_id = wp.project_id
                   AND t.status IN ('pending', 'claimed')
             )"#,
    )
    .bind(pod_name)
    .bind(STATUS_DONE)
    .bind(crate::tasks::unix_now())
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn has_live_for_project(pool: &PgPool, project_id: &str) -> Result<bool> {
    // Cast literal to bigint so sqlx's i64 type expectation matches.
    // PostgreSQL types untyped integer literals as INT4; the column
    // shape doesn't matter here because we throw the value away.
    let row: Option<(i64,)> = sqlx::query_as(
        r#"SELECT 1::bigint FROM worker_pod
           WHERE project_id = $1 AND status IN ('spawning', 'alive')
           LIMIT 1"#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// Look up the currently-alive worker pod for `project_id`. Returns
/// the `(pod_name, namespace, binary_hash)` of the first match, or
/// None when no pod is alive. Used by `spawn_pod` to decide whether
/// to reuse the existing pod or replace it (when its binary_hash no
/// longer matches the project's current `running_binary_hash`).
pub async fn alive_pod_for_project_full(
    pool: &PgPool,
    project_id: &str,
) -> Result<Option<(String, String, String)>> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        r#"SELECT pod_name, namespace, binary_hash FROM worker_pod
           WHERE project_id = $1 AND status IN ('spawning', 'alive')
           ORDER BY created_at_unix ASC
           LIMIT 1"#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Pod that currently owns the project's worker pool, if any. Used
/// by cancel routing to address the cancel task to the right pod.
pub async fn alive_pod_for_project(
    pool: &PgPool,
    project_id: &str,
) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        r#"SELECT pod_name FROM worker_pod
           WHERE project_id = $1 AND status IN ('spawning', 'alive')
           ORDER BY created_at_unix ASC
           LIMIT 1"#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(p,)| p))
}

pub async fn list_stale(pool: &PgPool, threshold_unix: i64) -> Result<Vec<WorkerPodRow>> {
    let rows = sqlx::query(
        r#"SELECT pod_name, project_id, namespace, last_heartbeat_unix
           FROM worker_pod
           WHERE status = 'alive' AND last_heartbeat_unix < $1"#,
    )
    .bind(threshold_unix)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(parse_row).collect()
}

/// Terminal rows (`done` | `dead`) whose `terminal_at_unix` is
/// older than the threshold. The pod-GC sweep `kubectl delete`s
/// the finished k8s Pod object (using the row's own namespace, so
/// no namespace-mapper guessing) and removes the row. Driven off
/// the table, not `kubectl get`, so it's the single source of
/// truth and is fakeable in tests.
pub async fn list_terminal(pool: &PgPool, threshold_unix: i64) -> Result<Vec<WorkerPodRow>> {
    let rows = sqlx::query(
        r#"SELECT pod_name, project_id, namespace, last_heartbeat_unix
           FROM worker_pod
           WHERE status IN ('done', 'dead')
             AND terminal_at_unix IS NOT NULL
             AND terminal_at_unix < $1"#,
    )
    .bind(threshold_unix)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(parse_row).collect()
}

/// Remove a worker_pod row. Called by the pod-GC sweep after the
/// k8s Pod object is deleted, so the row and the object retire
/// together.
pub async fn delete_row(pool: &PgPool, pod_name: &str) -> Result<()> {
    sqlx::query("DELETE FROM worker_pod WHERE pod_name = $1")
        .bind(pod_name)
        .execute(pool)
        .await?;
    Ok(())
}

/// Decode a worker_pod row, propagating decode errors. A `.ok()`
/// drop here would silently skip a stale/terminal pod from the
/// reaper's / GC's list -> the pod leaks with no error. Fail loud
/// on schema drift instead.
fn parse_row(row: sqlx::postgres::PgRow) -> Result<WorkerPodRow> {
    Ok(WorkerPodRow {
        pod_name: row.try_get("pod_name")?,
        project_id: row.try_get("project_id")?,
        namespace: row.try_get("namespace")?,
        last_heartbeat_unix: row.try_get("last_heartbeat_unix")?,
    })
}

// NOTE: live-connection routing (which pod a new caller pins to + cap
// enforcement) lives in `tasks::admit_live_execution`: in one transaction,
// under a per-project advisory lock, it picks the least-loaded under-cap pod
// (load = count of in-flight live-execute tasks pinned to it) and INSERTs the
// pinned execute task on it. Admission IS the task insert, so there is no
// separate counter to drift and no admit/insert window. The correctness
// (no overshoot under concurrent handshakes across sibling Pods) is inherently
// a DB concern (the advisory lock + count are the whole point), covered by the
// Layer-4 cluster e2e, not a pure unit test.
