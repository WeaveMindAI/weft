//! Background reapers that sweep stale rows and respawn missing
//! workers. Every dispatcher Pod runs all of these; FOR UPDATE
//! SKIP LOCKED + idempotent operations keep them from stepping
//! on each other.

use std::time::Duration;

use crate::state::DispatcherState;

/// Spawn every reaper. Returns immediately; the reapers run for the
/// lifetime of the process.
pub fn spawn_all(state: DispatcherState) {
    spawn_worker_pod_reaper(state.clone());
    spawn_tasks_sweeper(state.clone());
    spawn_listener_reaper(state.clone());
}

/// Worker-pod reaper. Once every 30s, scan for `alive` rows with
/// stale heartbeats. Mark them `dead` (which makes the fencing
/// trigger reject any further journal writes from them) and
/// `kubectl delete` the Pod. Pending tasks for the project pool
/// remain claimable: the cold-start trigger spawns a fresh Pod
/// when there's pending work and no live Pod.
fn spawn_worker_pod_reaper(state: DispatcherState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if let Err(e) = sweep_worker_pods(&state).await {
                tracing::warn!(
                    target: "weft_dispatcher::reaper",
                    error = %e,
                    "worker_pod reaper sweep failed"
                );
            }
        }
    });
}

async fn sweep_worker_pods(state: &DispatcherState) -> anyhow::Result<()> {
    let threshold = unix_now() - weft_task_store::worker_pod::HEARTBEAT_STALE_SECS;
    let stale = weft_task_store::worker_pod::list_stale(&state.pg_pool, threshold).await?;
    for row in stale {
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            project = %row.project_id,
            pod = %row.pod_name,
            last_heartbeat = row.last_heartbeat_unix,
            "marking stale worker pod dead"
        );
        weft_task_store::worker_pod::mark_dead(&state.pg_pool, &row.pod_name).await?;
        // Best-effort kubectl delete; --ignore-not-found drops cleanly.
        let _ = state
            .workers
            .kill_pod(row.pod_name.clone(), row.namespace.clone())
            .await;
        // Re-claim any tasks the dead Pod was holding so another
        // Pod (or the cold-start trigger) can pick them up.
        let _ = sqlx::query(
            r#"UPDATE task SET status = 'pending', claimed_by = NULL,
                   claimed_until_unix = NULL
               WHERE claimed_by = $1 AND status = 'claimed'"#,
        )
        .bind(&row.pod_name)
        .execute(&state.pg_pool)
        .await;
    }
    Ok(())
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Tasks-table retention sweeper. Once an hour, delete terminal
/// rows older than the retention window so the table stays small.
fn spawn_tasks_sweeper(state: DispatcherState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            match weft_task_store::tasks::sweep_terminal(&state.pg_pool).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(
                    target: "weft_dispatcher::reaper",
                    swept = n,
                    "tasks sweeper retired terminal rows"
                ),
                Err(e) => tracing::warn!(
                    target: "weft_dispatcher::reaper",
                    error = %e,
                    "tasks sweep failed"
                ),
            }
        }
    });
}

/// Listener reaper. Every 10s, scan `tenant_listener` rows and
/// reap any whose listener is idle. "Idle" is decided by
/// `ListenerPool::try_reap_if_idle` under an EXCLUSIVE per-tenant
/// OP-lock: the lock fences out concurrent `with_listener` calls,
/// and the signal table doubles as the "is the listener semantically
/// needed" check (zero rows for the tenant => safe to kill).
fn spawn_listener_reaper(state: DispatcherState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if let Err(e) = sweep_listeners(&state).await {
                tracing::warn!(
                    target: "weft_dispatcher::reaper",
                    error = %e,
                    "listener reaper sweep failed"
                );
            }
        }
    });
}

async fn sweep_listeners(state: &DispatcherState) -> anyhow::Result<()> {
    let rows = crate::lease::list_tenant_listener_rows(&state.pg_pool).await?;
    for row in rows {
        // Skip rows already mid-teardown on another Pod; their owner
        // will finish the transition or its lease will lapse and we
        // pick them up next sweep.
        if row.state == "stopping" {
            continue;
        }
        let tenant = crate::tenant::TenantId(row.tenant_id.clone());
        match state
            .listeners
            .try_reap_if_idle(
                &tenant,
                &row.namespace,
                state.listener_backend.as_ref(),
                &state.pg_pool,
                state.pod_id.as_str(),
            )
            .await
        {
            Ok(true) => tracing::info!(
                target: "weft_dispatcher::reaper",
                tenant = %tenant,
                namespace = %row.namespace,
                "reaped idle listener"
            ),
            Ok(false) => {} // operation in flight or signals present
            Err(e) => tracing::warn!(
                target: "weft_dispatcher::reaper",
                tenant = %tenant,
                error = %e,
                "listener kill failed"
            ),
        }
    }
    Ok(())
}
