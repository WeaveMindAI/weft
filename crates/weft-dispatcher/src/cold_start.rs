//! Cold-start trigger: scan for projects with pending worker tasks
//! but no live worker Pod, and enqueue a `spawn_pod` task. Run as a
//! background loop on every dispatcher.
//!
//! Dedup: `spawn_pod` tasks key on `project_id`, so concurrent
//! dispatchers all hashing the same orphan project converge on one
//! task.

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
    // Find projects with pending worker tasks that have no live
    // pod. The sync handler's `replace_stale_worker_if_needed`
    // already kills + waits-for-fresh-spawn when the binary_hash
    // changes BEFORE enqueueing any work, so by the time cold_start
    // runs we only need the simple "no pod, work pending" case.
    let rows = sqlx::query(
        r#"SELECT DISTINCT t.project_id, t.tenant_id
           FROM task t
           WHERE t.target = 'worker'
             AND t.status = 'pending'
             AND t.project_id IS NOT NULL
             AND NOT EXISTS (
                 SELECT 1 FROM worker_pod wp
                 WHERE wp.project_id = t.project_id
                   AND wp.status IN ('spawning', 'alive')
             )
           LIMIT 100"#,
    )
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
        // Workers live in the PROJECT namespace. The namespace is
        // written on register (NOT NULL in the schema); a None
        // here means the project was unregistered between the task
        // enqueue and now. Skip; the task will time out and the
        // user retries. DB errors propagate via `?`.
        let Some(namespace) = state.projects.project_namespace(&project_id).await? else {
            tracing::warn!(
                target: "weft_dispatcher::cold_start",
                project_id = %project_id,
                "project_namespace lookup returned None; project unregistered. skipping spawn"
            );
            continue;
        };
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
