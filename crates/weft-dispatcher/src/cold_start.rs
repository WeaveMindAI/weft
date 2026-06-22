//! Cold-start + scale-up trigger: scan for projects with pending worker
//! tasks but no ADMITTABLE worker Pod, and enqueue a `spawn_pod` task.
//! Run as a background loop on every dispatcher.
//!
//! "Admittable" = a `spawning`/`alive` pod that is not draining and is
//! below the memory-saturation threshold. This one condition serves
//! BOTH cold start (zero pods) AND scale-up under load (pods exist but
//! all are memory-saturated): in either case a project with pending work
//! and no pod that can take it gets one more worker. Capacity is bounded
//! by MEMORY, never a task/connection count.
//!
//! Dedup: `spawn_pod` tasks key on `project_id`, so at most one spawn is
//! in flight per project; concurrent dispatchers converge on one task,
//! and a sustained-saturation project ramps one worker per tick (spawn,
//! wait for it to come alive, and if still saturated spawn the next)
//! rather than bursting N workers for one spike.

use std::time::Duration;

use sqlx::Row;
use tokio::time::sleep;

use crate::state::DispatcherState;
use weft_task_store::tasks::{enqueue_dedup, NewTask, TaskTarget};
use weft_task_store::{SpawnPodPayload, TaskKind};

const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub fn spawn(state: DispatcherState) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = sweep_once(&state).await {
                tracing::warn!(
                    target: "weft_dispatcher::cold_start",
                    error = %e,
                    "sweep error; backing off"
                );
            }
            sleep(POLL_INTERVAL).await;
        }
    });
}

async fn sweep_once(state: &DispatcherState) -> anyhow::Result<()> {
    // Find projects with pending worker tasks that have no ADMITTABLE
    // pod (none alive/spawning, OR every one draining / memory-
    // saturated). The sync handler's `replace_stale_worker_if_needed`
    // already kills + waits-for-fresh-spawn when the binary_hash
    // changes BEFORE enqueueing any work, so a pod present here is
    // already on the right image.
    //
    // A `spawning` pod counts as admittable (pressure 0 until its first
    // heartbeat), so while a fresh worker boots we do not spawn a second:
    // the burst waits for the booting one. Only when every existing pod
    // is actually saturated does another spawn.
    //
    // Pinned tasks (target_pod_name set: a resume pinned to its live
    // owner, a live execution pinned at admit) are EXCLUDED: their pod
    // already exists by construction, and they must run on THAT pod, so
    // they must never trigger a fresh spawn (which the unpinned task's
    // owner would then also not be).
    let saturation = weft_platform_traits::SATURATION_MEM_FRACTION;
    let rows = sqlx::query(
        r#"SELECT DISTINCT t.project_id, t.tenant_id
           FROM task t
           WHERE t.target = 'worker'
             AND t.status = 'pending'
             AND t.project_id IS NOT NULL
             AND t.target_pod_name IS NULL
             AND NOT EXISTS (
                 SELECT 1 FROM worker_pod wp
                 WHERE wp.project_id = t.project_id
                   AND wp.status IN ('spawning', 'alive')
                   AND NOT wp.draining
                   AND wp.mem_pressure < $1
             )
           LIMIT 100"#,
    )
    .bind(saturation)
    .fetch_all(&state.pg_pool)
    .await?;

    for row in rows {
        let project_id: String = row.try_get("project_id")?;
        // `task.tenant_id` is nullable: some task kinds enqueue
        // without resolving a tenant (NewTask.tenant_id is
        // Option). When NULL, derive the tenant from the project
        // via the router. A decode failure (not a NULL value)
        // propagates via `?` as schema drift.
        let tenant_id: Option<String> = row.try_get("tenant_id")?;
        let tenant = tenant_id
            .clone()
            .unwrap_or_else(|| state.tenant_router.tenant_for_project(&project_id).to_string());
        // Worker placement: an infra project's worker runs in the
        // project's own namespace (next to its infra), a no-infra
        // project's worker runs in the shared worker namespace. A None
        // here means the project was unregistered between the task
        // enqueue and now. Skip; the task will time out and the user
        // retries. DB errors propagate via `?`.
        let Some(has_infra) = state.projects.project_has_infra(&project_id).await? else {
            tracing::warn!(
                target: "weft_dispatcher::cold_start",
                project_id = %project_id,
                "project_has_infra lookup returned None; project unregistered. skipping spawn"
            );
            continue;
        };
        let namespace =
            crate::project_namespace::worker_namespace(has_infra, &tenant, &project_id);
        let payload = SpawnPodPayload {
            project_id: project_id.clone(),
            tenant: tenant.clone(),
            namespace,
            owner_dispatcher: state.pod_id.as_str().to_string(),
        };
        let dedup = format!("{project_id}:spawn");
        // Propagate enqueue failures. The outer loop catches and
        // logs+backs off; silently discarding means the project has
        // pending worker tasks but no spawn task, and the failure
        // mode (DB hiccup, serde error) stays invisible until
        // next-tick rediscovery.
        enqueue_dedup(
            &state.pg_pool,
            NewTask {
                kind: TaskKind::SpawnPod,
                target: TaskTarget::Dispatcher,
                project_id: Some(project_id),
                dedup_key: Some(dedup),
                color: None,
                tenant_id: Some(tenant),
                target_pod_name: None,
                payload: serde_json::to_value(&payload)?,
            },
        )
        .await?;
    }

    Ok(())
}
