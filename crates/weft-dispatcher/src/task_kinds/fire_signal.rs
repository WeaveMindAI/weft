//! `fire_signal` task: a held-event signal fired inside a tenant
//! listener (Timer expiry, SSE event delivery, future browser-session
//! resolution). The listener enqueues a row through the broker; a
//! dispatcher Pod claims it and runs the same `dispatch_listener_outcome`
//! path a stateless fire goes through.
//!
//! Tenant pods never speak HTTP to the dispatcher: listener →
//! dispatcher coordination goes through the task table, gated by the
//! broker's per-tenant scope check.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;
use weft_task_store::FireSignalPayload;

use crate::state::DispatcherState;

pub struct FireSignalExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for FireSignalExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: FireSignalPayload = serde_json::from_value(task.payload.clone())?;
        // Held-event fires bypass the public lifecycle gate. Even
        // though the listener pod may have been reaped between when
        // the event was held and when this task runs, `with_listener`
        // (inside `dispatch_listener_outcome`) respawns it to honor
        // the fire; the reaper retires it again on the next sweep.
        let signal = state
            .journal
            .signal_get(&payload.token)
            .await?
            .ok_or_else(|| anyhow::anyhow!("signal {} not found", payload.token))?;
        // Use the FireSignal task id as the dedup nonce: an executor
        // retry of this same task re-calls dispatch with the same
        // nonce, so any RouteEntry task inserted on the first
        // attempt collapses on retry instead of producing a
        // duplicate execution.
        let nonce = task.id.to_string();
        let status = crate::api::signal::dispatch_listener_outcome(
            state,
            &payload.token,
            &signal.project_id,
            payload.payload,
            Some(&nonce),
        )
        .await
        .map_err(|(code, msg)| anyhow::anyhow!("dispatch_listener_outcome {code}: {msg}"))?;
        Ok(serde_json::json!({ "status": status.as_u16() }))
    }
}
