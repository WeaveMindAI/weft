//! Cold-start + scale-up trigger: scan for projects with pending worker
//! tasks but no ADMITTABLE worker Pod, and enqueue a `spawn_pod` task.
//! Run as a background loop on every dispatcher, woken by the writes
//! that can change the answer.
//!
//! "Admittable" = a `spawning`/`alive` pod that is not draining and is
//! below the memory-saturation threshold. This one condition serves
//! BOTH cold start (zero pods) AND scale-up under load (pods exist but
//! all are memory-saturated): in either case a project with pending work
//! and no pod that can take it gets one more worker. Capacity is bounded
//! by MEMORY, never a task/connection count.
//!
//! Dedup: `spawn_pod` tasks key on project and requested binary, so one spawn
//! is in flight per image; concurrent dispatchers converge on one task,
//! and a sustained-saturation project ramps one worker per wake (spawn,
//! wait for it to come alive, and if still saturated spawn the next)
//! rather than bursting N workers for one spike.

use sqlx::Row;

use crate::pg_wake::{self, DrainStep, WakeOn};
use crate::state::DispatcherState;
use weft_task_store::tasks::{enqueue_dedup, NewTask, TaskTarget, TASK_READY_CHANNEL};
use weft_task_store::worker_pod::WORKER_POD_CHANNEL;
use weft_task_store::{SpawnPodPayload, TaskKind};

/// What changes the answer: worker work becoming claimable, and a
/// project's pods changing (one appears, dies, drains, or crosses the
/// saturation line). A spawn that failed announces neither; the safety
/// tick retries it.
const WAKE_ON: &[WakeOn] = &[
    WakeOn { channel: TASK_READY_CHANNEL, concerns: |payload| payload.starts_with("worker:") },
    WakeOn::any(WORKER_POD_CHANNEL),
];

/// Every dispatcher pod runs the sweep; the enqueue is dedup-keyed, so
/// siblings woken by the same write converge on one spawn.
pub fn spawn(state: DispatcherState) {
    crate::app::spawn_supervised("cold_start", async move {
        pg_wake::run(
            state.signals.subscribe(),
            WAKE_ON,
            pg_wake::SAFETY_POLL_INTERVAL,
            "weft_dispatcher::cold_start",
            || async { sweep_once(&state).await.map(|()| DrainStep::Done) },
        )
        .await;
    });
}

/// One pass over every project that needs a worker. Not batched: a
/// project already given its spawn stays in the answer until the pod
/// comes alive, so a batch would keep returning the same projects and
/// starve the rest.
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
    // Each executable gets its own capacity. Tasks without an image restriction
    // accept the current image. Tenant ownership comes from placement below.
    let rows = sqlx::query(
        r#"SELECT DISTINCT t.project_id, COALESCE(t.binary_hash, p.running_binary_hash) AS binary_hash
           FROM task t
           JOIN project p ON p.id = t.project_id
           WHERE t.target = 'worker'
             AND t.status = 'pending'
             AND t.project_id IS NOT NULL
             AND t.target_pod_name IS NULL
             AND NOT EXISTS (
                 SELECT 1 FROM worker_pod wp
                 WHERE wp.project_id = t.project_id
                   AND wp.status IN ('spawning', 'alive')
                   AND wp.role = 'worker'
                   AND NOT wp.draining
                   AND wp.mem_pressure < $1
                   AND (t.binary_hash IS NULL OR wp.binary_hash = t.binary_hash)
             )"#,
    )
    .bind(saturation)
    .fetch_all(&state.pg_pool)
    .await?;

    for row in rows {
        let project_id: uuid::Uuid = row.try_get("project_id")?;
        let binary_hash: String = row.try_get("binary_hash")?;
        // Worker placement via the single resolver (source-declares-
        // infra AND its own namespace exists -> project namespace, else
        // shared pool). A None here means the project was unregistered
        // between the task enqueue and now. Skip; the task will time
        // out and the user retries. DB errors propagate via `?`.
        let Some(placement) = crate::placement::resolve_worker_placement(state, project_id).await?
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
            project_id,
            tenant: tenant.clone(),
            namespace: placement.namespace,
            owner_dispatcher: state.pod_id.as_str().to_string(),
        };
        let dedup = format!("{project_id}:{binary_hash}:spawn");
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
                binary_hash: Some(binary_hash),
                payload: serde_json::to_value(&payload)?,
            },
        )
        .await?;
    }

    Ok(())
}
