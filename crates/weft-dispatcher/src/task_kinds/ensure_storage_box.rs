//! `ensure_storage_box` task: a worker is about to make its first
//! storage call (or just got box-unreachable, possibly because the
//! reaper tore the box down mid-execution) and asks the dispatcher
//! to make sure its tenant's box exists. The executor provisions
//! lazily (idempotent kubectl apply) and returns the box's
//! in-cluster URL; from then on the worker talks to the box
//! DIRECTLY. The dispatcher is only this handshake, never bytes.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;
use crate::tenant::TenantId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnsureStorageBoxResult {
    pub box_url: String,
}

pub struct EnsureStorageBoxExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for EnsureStorageBoxExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let tenant = task
            .tenant_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("ensure_storage_box task has no tenant_id"))?;
        let url = crate::storage_box::ensure_box(state, &TenantId(tenant)).await?;
        Ok(serde_json::to_value(EnsureStorageBoxResult { box_url: url })?)
    }
}
