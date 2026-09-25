//! `spawn_pod` task: dispatcher applies a worker Pod for a
//! project pool. Triggered by the cold-start scanner when there's
//! pending `target=worker` work for project P with no live Pod.
//!
//! Idempotency: pod name is derived from the task id, the worker_pod
//! row is INSERTed (ON CONFLICT DO NOTHING) BEFORE the apply,
//! and the apply is itself idempotent on the manifest name. A retry
//! after a partial success (pod applied, dispatcher crashed)
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
        let project_id = payload.project_id;

        // Pending work chooses its image, including older runs resuming after
        // a rebuild. A project edit cannot retarget this spawn task.
        let want_hash = task.binary_hash.as_deref()
            .ok_or_else(|| anyhow::anyhow!("spawn_pod task has no requested binary identity"))?;

        if let Some(skipped) = nothing_to_spawn(&state.pg_pool, project_id, want_hash).await? {
            if skipped == SKIP_PROJECT_REMOVED {
                tracing::info!(
                    project_id = %project_id,
                    "spawn_pod: the project was removed since this spawn was queued; nothing to spawn"
                );
            }
            return Ok(serde_json::json!({ "skipped": skipped }));
        }

        // Deterministic pod name from task id. Two attempts of the
        // same task collide on this name in both worker_pod (PK) and
        // the k8s API server (manifest name).
        let task_short = &task.id.simple().to_string()[..8];
        let pod_name = format!("wp-{}-{}", short_project_id(payload.project_id), task_short);

        // Reserve the row first. ON CONFLICT DO NOTHING means a
        // retry that already wrote it is silently fine.
        weft_task_store::worker_pod::insert_spawning(
            &state.pg_pool,
            &pod_name,
            project_id,
            &payload.namespace,
            &payload.owner_dispatcher,
            Some(want_hash),
            "worker",
            None,
        )
        .await?;

        // Lazy shared-namespace creation. A no-infra project's worker
        // targets the shared worker namespace (the resolver decided
        // this at enqueue time). Unlike a per-project namespace (created
        // at first infra apply, guaranteed present before any worker),
        // the shared namespace is created HERE the first time any worker
        // lands in it. Idempotent (server-side apply); never torn down. An
        // infra project's per-project namespace already exists, so this
        // gate skips it.
        if payload.namespace == state.instance.shared_worker_namespace() {
            crate::shared_worker_namespace::ensure(
                &*state.kube,
                &crate::shared_worker_namespace::SharedWorkerNamespaceArgs {
                    instance: &state.instance,
                    pod_cidr: &state.cluster_pod_cidr,
                    service_cidr: &state.cluster_service_cidr,
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("ensure shared worker namespace: {e}"))?;
        }

        let spec = SpawnPodSpec {
            project_id: payload.project_id,
            tenant: payload.tenant,
            namespace: payload.namespace.clone(),
            owner_dispatcher: payload.owner_dispatcher.clone(),
            binary_hash: Some(want_hash.to_string()),
            // Worker verifies live-connection routing tokens with this.
            caller_token_secret_hex: hex::encode(state.caller_token_secret.as_ref()),
        };
        let handle = state.workers.spawn_pod(&pod_name, spec).await?;
        Ok(serde_json::to_value(SpawnPodResult {
            pod_name: handle.pod_name,
        })?)
    }
}

/// A spawn skipped because its project was removed after it was queued.
pub const SKIP_PROJECT_REMOVED: &str = "project_removed";
/// A spawn skipped because a pod that can take the work already exists.
pub const SKIP_ADMITTABLE_POD_EXISTS: &str = "admittable_pod_exists";

/// Whether a spawn for `project_id` on `want_hash` has nothing to do,
/// and why: `None` means spawn.
pub async fn nothing_to_spawn(
    pool: &sqlx::PgPool,
    project_id: uuid::Uuid,
    want_hash: &str,
) -> Result<Option<&'static str>> {
    // A project removed after this spawn was queued: a worker brought up
    // now would only be killed by the removed-projects sweep.
    let (exists,): (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM project WHERE id = $1)")
        .bind(project_id)
        .fetch_one(pool)
        .await?;
    if !exists {
        return Ok(Some(SKIP_PROJECT_REMOVED));
    }

    // Idempotency: if an ADMITTABLE pod already exists, nothing to
    // do. "Admittable" is the ONE predicate every spawn-enqueuer
    // uses (the cold-start sweep, the live-connect all-saturated
    // loop): alive/spawning, not draining, below memory saturation,
    // on the CURRENT image. Anything weaker here starves a real
    // request: an any-alive check would let a memory-saturated pod
    // (the horizontal scale-up trigger) or a stale-image survivor
    // (which can no longer claim hash-stamped work) suppress the
    // very spawn that was enqueued because of it. A `spawning` pod
    // counts (pressure 0 until its first heartbeat), so a booting
    // worker absorbs the burst instead of a spawn stampede.
    if weft_task_store::worker_pod::pick_admittable_for_project(
        pool,
        project_id,
        weft_platform_traits::SATURATION_MEM_FRACTION,
        Some(want_hash),
    )
    .await?
    .is_some()
    {
        return Ok(Some(SKIP_ADMITTABLE_POD_EXISTS));
    }
    Ok(None)
}
