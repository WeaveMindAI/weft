//! `provision_sidecar` task: a dispatcher Pod runs `kubectl apply`
//! on the sidecar's manifests, writes the resulting handle into
//! the `infra_pod` table, and returns the endpoint URL.
//!
//! Producers: any node calling `ctx.provision_sidecar` (the engine
//! enqueues the task and awaits its completion via the task client)
//! and the `weft infra start` route.
//!
//! Idempotency: dedup keyed on (project_id, node_id) so concurrent
//! callers converge on a single task. Inside the executor, if a
//! running row already exists in `infra_pod`, return it without
//! reapplying. Otherwise call the backend's idempotent `provision`.

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::node::SidecarSpec;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::{self, NewTask, Task, TaskStatus};
use weft_task_store::TaskKind;

use crate::backend::InfraSpec;
use crate::state::DispatcherState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionSidecarPayload {
    pub project_id: String,
    pub node_id: String,
    pub spec: SidecarSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionSidecarResult {
    pub instance_id: String,
    pub endpoint_url: Option<String>,
}

pub struct ProvisionSidecarExecutor;

const PROVISION_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const PROVISION_POLL_INTERVAL: Duration = Duration::from_secs(2);

pub async fn enqueue_and_wait(
    state: &DispatcherState,
    payload: ProvisionSidecarPayload,
) -> Result<ProvisionSidecarResult> {
    let dedup_key = format!("{}/{}", payload.project_id, payload.node_id);
    let payload_json = serde_json::to_value(&payload)?;
    let task_id = tasks::enqueue_dedup(
        &state.pg_pool,
        NewTask {
            kind: TaskKind::ProvisionSidecar,
            target: tasks::TaskTarget::Dispatcher,
            project_id: Some(payload.project_id.clone()),
            dedup_key: Some(dedup_key),
            color: None,
            tenant_id: None,
            target_pod_name: None,
            payload: payload_json,
        },
    )
    .await?
    .id();
    let outcome = tasks::wait_for_terminal(
        &state.pg_pool,
        task_id,
        PROVISION_WAIT_TIMEOUT,
        PROVISION_POLL_INTERVAL,
    )
    .await?;
    match outcome.status {
        TaskStatus::Complete => {
            let result_value = outcome
                .result
                .ok_or_else(|| anyhow::anyhow!("provision_sidecar complete with no result"))?;
            Ok(serde_json::from_value(result_value)?)
        }
        TaskStatus::Failed => {
            anyhow::bail!(
                "{}",
                outcome.error.unwrap_or_else(|| "provision_sidecar failed".into())
            )
        }
        other => anyhow::bail!("provision_sidecar did not reach terminal state: {other:?}"),
    }
}

#[async_trait]
impl TaskExecutor<DispatcherState> for ProvisionSidecarExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: ProvisionSidecarPayload = serde_json::from_value(task.payload.clone())?;

        // Idempotency: if a running infra_pod row already exists,
        // hand its handle back without reapplying.
        if let Some(existing) =
            crate::infra::handle_if_running(&state.pg_pool, &payload.project_id, &payload.node_id)
                .await?
        {
            return Ok(serde_json::to_value(ProvisionSidecarResult {
                instance_id: existing.id,
                endpoint_url: existing.endpoint_url,
            })?);
        }

        let tenant = state.tenant_router.tenant_for_project(&payload.project_id);
        let namespace = state.namespace_mapper.namespace_for(&tenant);

        // Read the pending image hash that infra::start (or
        // infra::upgrade) pre-wrote for this (project, node). Used
        // as the docker tag suffix for the sidecar image.
        let sidecar_hash = crate::infra::pending_image_hash(
            &state.pg_pool,
            &payload.project_id,
            &payload.node_id,
        )
        .await?;

        let infra_spec = InfraSpec {
            project_id: payload.project_id.clone(),
            infra_node_id: payload.node_id.clone(),
            sidecar: payload.spec,
            config: serde_json::Value::Null,
            tenant: tenant.to_string(),
            namespace,
            sidecar_hash: sidecar_hash.clone(),
        };
        let handle = state.infra.provision(infra_spec).await?;
        crate::infra::insert_running(
            &state.pg_pool,
            &payload.project_id,
            &payload.node_id,
            &handle,
        )
        .await?;
        // Re-stamp the image hash because insert_running's UPSERT
        // doesn't touch running_image_hash and we want the row to
        // show the hash that was actually used (matters for drift
        // detection: a silent UPDATE failure here would leave drift
        // comparing against a stale/blank hash).
        if let Some(h) = &sidecar_hash {
            sqlx::query(
                "UPDATE infra_pod SET running_image_hash = $1 \
                 WHERE project_id = $2 AND node_id = $3",
            )
            .bind(h)
            .bind(&payload.project_id)
            .bind(&payload.node_id)
            .execute(&state.pg_pool)
            .await?;
        }
        Ok(serde_json::to_value(ProvisionSidecarResult {
            instance_id: handle.id,
            endpoint_url: handle.endpoint_url,
        })?)
    }
}
