//! `worker_pod` table + journal-fencing trigger. One row per Pod.
//! Worker boots → `register_alive`; engine heartbeats; reaper marks
//! stale rows dead. The trigger on `exec_event` rejects writes whose
//! pod_name is not in `{spawning, alive}`, fencing stale Pods out.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
            created_at_unix BIGINT NOT NULL
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
    let res = sqlx::query(
        r#"UPDATE worker_pod
           SET status = $3, last_heartbeat_unix = $4
           WHERE pod_name = $1 AND project_id = $2"#,
    )
    .bind(pod_name)
    .bind(project_id)
    .bind(STATUS_ALIVE)
    .bind(now)
    .execute(pool)
    .await?;
    if res.rows_affected() == 0 {
        anyhow::bail!(
            "no worker_pod row for pod_name='{pod_name}' project_id='{project_id}'; \
             dispatcher must call insert_spawning before the worker boots"
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
) -> Result<()> {
    let now = unix_now();
    sqlx::query(
        r#"INSERT INTO worker_pod (
            pod_name, project_id, namespace, status, owner_dispatcher,
            last_heartbeat_unix, created_at_unix
        ) VALUES ($1, $2, $3, $4, $5, $6, $6)
        ON CONFLICT (pod_name) DO NOTHING"#,
    )
    .bind(pod_name)
    .bind(project_id)
    .bind(namespace)
    .bind(STATUS_SPAWNING)
    .bind(owner_dispatcher)
    .bind(now)
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
           SET status = $1
           WHERE pod_name = $2 AND status IN ('spawning', 'alive')"#,
    )
    .bind(STATUS_DONE)
    .bind(pod_name)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_dead(pool: &PgPool, pod_name: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE worker_pod
           SET status = $1
           WHERE pod_name = $2 AND status IN ('spawning', 'alive')"#,
    )
    .bind(STATUS_DEAD)
    .bind(pod_name)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn has_live_for_project(pool: &PgPool, project_id: &str) -> Result<bool> {
    let row: Option<(i64,)> = sqlx::query_as(
        r#"SELECT 1 FROM worker_pod
           WHERE project_id = $1 AND status IN ('spawning', 'alive')
           LIMIT 1"#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
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
    Ok(rows.into_iter().filter_map(parse_row).collect())
}

fn parse_row(row: sqlx::postgres::PgRow) -> Option<WorkerPodRow> {
    let pod_name: String = row.try_get("pod_name").ok()?;
    let project_id: String = row.try_get("project_id").ok()?;
    let namespace: String = row.try_get("namespace").ok()?;
    let last_heartbeat_unix: i64 = row.try_get("last_heartbeat_unix").ok()?;
    Some(WorkerPodRow {
        pod_name,
        project_id,
        namespace,
        last_heartbeat_unix,
    })
}

/// Spawn a background heartbeat task. Sets `shutdown` to true if the
/// row stops being alive (mark_done / mark_dead, row deleted).
pub fn spawn_heartbeat(pool: PgPool, pod_name: String, shutdown: Arc<AtomicBool>) {
    let interval = Duration::from_secs(HEARTBEAT_INTERVAL_SECS);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            match heartbeat(&pool, &pod_name).await {
                Ok(true) => continue,
                Ok(false) => {
                    tracing::warn!(
                        target: "weft_task_store::worker_pod",
                        %pod_name,
                        "worker_pod row no longer alive; signalling shutdown"
                    );
                    shutdown.store(true, Ordering::Relaxed);
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        target: "weft_task_store::worker_pod",
                        error = %e,
                        "heartbeat error; will retry"
                    );
                }
            }
        }
        let _ = mark_done(&pool, &pod_name).await;
    });
}
