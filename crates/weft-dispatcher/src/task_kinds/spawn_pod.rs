//! `spawn_pod` task: dispatcher kubectl-applies a worker Pod for a
//! project pool. Triggered by the cold-start scanner when there's
//! pending `target=worker` work for project P with no live Pod.
//!
//! Idempotency: pod name is derived from the task id, the worker_pod
//! row is INSERTed (ON CONFLICT DO NOTHING) BEFORE kubectl apply,
//! and the apply is itself idempotent on the manifest name. A retry
//! after a partial success (kubectl applied, dispatcher crashed)
//! collapses on the same pod name instead of creating a second Pod.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;
use weft_task_store::SpawnPodPayload;

use crate::backend::{k8s_worker::short_project_id, SpawnPodSpec};
use crate::state::DispatcherState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnPodResult {
    pub pod_name: String,
}

pub struct SpawnPodExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for SpawnPodExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: SpawnPodPayload = serde_json::from_value(task.payload.clone())?;

        // Idempotency: if a live Pod already exists for this project,
        // skip spawn entirely.
        if weft_task_store::worker_pod::has_live_for_project(&state.pg_pool, &payload.project_id).await? {
            return Ok(serde_json::json!({"skipped": "pod_already_alive"}));
        }

        // Deterministic pod name from task id. Two attempts of the
        // same task collide on this name in both worker_pod (PK) and
        // the k8s API server (manifest name).
        let task_short = &task.id.simple().to_string()[..8];
        let pod_name = format!("wp-{}-{}", short_project_id(&payload.project_id), task_short);

        // Reserve the row first. ON CONFLICT DO NOTHING means a
        // retry that already wrote it is silently fine.
        weft_task_store::worker_pod::insert_spawning(
            &state.pg_pool,
            &pod_name,
            &payload.project_id,
            &payload.namespace,
            &payload.owner_dispatcher,
        )
        .await?;

        // Resolve source_hash at spawn time (not enqueue time) so the
        // most recent hash is used even when the build finished after
        // the task was queued.
        let project_uuid: uuid::Uuid = payload.project_id.parse().map_err(|e| {
            anyhow::anyhow!("spawn_pod: bad project_id {}: {e}", payload.project_id)
        })?;
        let source_hash = state.projects.running_source_hash(project_uuid).await;

        let spec = SpawnPodSpec {
            project_id: payload.project_id.clone(),
            tenant: payload.tenant,
            namespace: payload.namespace.clone(),
            owner_dispatcher: payload.owner_dispatcher.clone(),
            source_hash,
        };
        let handle = state.workers.spawn_pod(&pod_name, spec).await?;
        Ok(serde_json::to_value(SpawnPodResult {
            pod_name: handle.pod_name,
        })?)
    }
}
