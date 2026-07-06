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
    crate::app::spawn_supervised("cold_start", async move {
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
    // saturated). The sync handler's `reconcile_worker`
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
    // Group by project_id ONLY. A project has exactly one tenant, but `task.tenant_id`
    // is nullable (some task kinds enqueue without resolving one), so selecting it
    // here returns spurious duplicate rows for one project when its pending tasks
    // carry mixed NULL/concrete stamps, and each duplicate re-runs the resolver +
    // enqueue below for nothing (the spawn dedup then collapses them). The placement
    // resolver below is the single authoritative source of the project's tenant, so
    // we read it there and never from the task stamp.
    let rows = sqlx::query(
        r#"SELECT DISTINCT t.project_id
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
                   AND (t.binary_hash IS NULL OR wp.binary_hash = t.binary_hash)
             )
           LIMIT 100"#,
    )
    .bind(saturation)
    .fetch_all(&state.pg_pool)
    .await?;

    for row in rows {
        let project_id: String = row.try_get("project_id")?;
        // Worker placement via the single resolver (source-declares-
        // infra AND its own namespace exists -> project namespace, else
        // shared pool). A None here means the project was unregistered
        // between the task enqueue and now. Skip; the task will time
        // out and the user retries. DB errors propagate via `?`.
        let Some(placement) = crate::placement::resolve_worker_placement(state, &project_id).await?
        else {
            tracing::warn!(
                target: "weft_dispatcher::cold_start",
                project_id = %project_id,
                "placement lookup found no project row; project unregistered. skipping spawn"
            );
            continue;
        };
        // The project's tenant comes from the authoritative resolver, not a task
        // stamp (see the query comment above).
        let tenant = placement.tenant.as_str().to_string();
        let payload = SpawnPodPayload {
            project_id: project_id.clone(),
            tenant: tenant.clone(),
            namespace: placement.namespace,
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
                kind: TaskKind::SpawnPod.into(),
                target: TaskTarget::Dispatcher,
                project_id: Some(project_id),
                dedup_key: Some(dedup),
                color: None,
                tenant_id: Some(tenant),
                target_pod_name: None,
                binary_hash: None,
                payload: serde_json::to_value(&payload)?,
            },
        )
        .await?;
    }

    // Superseded tasks: a pending unpinned worker task stamped with an
    // image that is NO LONGER the project's current one, with no alive
    // pod of that image left to claim it. Nothing will ever run it (the
    // claim filter is exact, and every future spawn bakes the CURRENT
    // image), so leaving it pending is an invisible forever-wait for
    // whoever fired it. Fail the task AND cancel its color so the
    // execution lands terminal, loudly, in the journal the user watches.
    // The window that produces these is small (a re-register between
    // enqueue and first claim, with the old pods gone), but real.
    let superseded = sqlx::query(
        r#"SELECT t.id, t.color
           FROM task t
           JOIN project p ON p.id = t.project_id::uuid
           WHERE t.target = 'worker'
             AND t.status = 'pending'
             AND t.target_pod_name IS NULL
             AND t.binary_hash IS NOT NULL
             AND p.running_binary_hash IS NOT NULL
             AND t.binary_hash <> p.running_binary_hash
             AND NOT EXISTS (
                 SELECT 1 FROM worker_pod wp
                 WHERE wp.project_id = t.project_id
                   AND wp.status IN ('spawning', 'alive')
                   AND wp.binary_hash = t.binary_hash
             )
           LIMIT 100"#,
    )
    .fetch_all(&state.pg_pool)
    .await?;
    for row in superseded {
        let task_id: uuid::Uuid = row.try_get("id")?;
        let color: Option<String> = row.try_get("color")?;
        let failed = weft_task_store::tasks::fail_pending(
            &state.pg_pool,
            task_id,
            "superseded: the project was rebuilt before this work was claimed and no worker \
             of the image it targeted remains; re-run against the current build",
        )
        .await?;
        if !failed {
            // Claimed in the window since the scan (a matching pod
            // appeared): it is being handled, leave it.
            continue;
        }
        tracing::warn!(
            target: "weft_dispatcher::cold_start",
            task_id = %task_id,
            color = ?color,
            "failed a superseded pending worker task (image rebuilt before claim, no \
             old-image pod remains); its execution is cancelled"
        );
        if let Some(color) = color.and_then(|c| c.parse::<weft_core::Color>().ok()) {
            if let Err(e) = crate::api::execution::cancel_color(state, color).await {
                tracing::warn!(
                    target: "weft_dispatcher::cold_start",
                    color = %color,
                    error = %e,
                    "cancel_color for a superseded task failed; the reaper's stuck-execution \
                     sweep will land the terminal"
                );
            }
        }
    }

    Ok(())
}
