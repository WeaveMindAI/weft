//! `fire_signal` task: a held-event signal fired inside a pooled
//! listener process (Timer expiry, SSE event delivery, future
//! browser-session resolution). The listener enqueues a row through the
//! broker; a dispatcher claims it and runs the same
//! `dispatch_listener_outcome` path a stateless fire goes through.
//!
//! The listener never speaks HTTP to the dispatcher (only the other way
//! round): listener → dispatcher coordination goes through the task
//! table. The broker lets a listener enqueue this one kind only, and
//! stamps the task with the signal's own tenant, resolved from the
//! signal row, since a pooled listener carries none.

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
        // though the listener process holding this signal may have been
        // reaped between when the event was held and when this task
        // runs, `ensure_placed_handle` (inside `dispatch_listener_
        // outcome`) re-places the signal from its durable row onto a
        // live process to honor the fire; the reaper retires the empty process
        // again on the next sweep.
        let signal = state
            .journal
            .signal_get(&payload.token)
            .await?
            .ok_or_else(|| anyhow::anyhow!("signal {} not found", payload.token))?;
        // An entry's fire counts against its per-minute limit here, the
        // one place every listener-held fire passes (a resume answers a
        // run already going and is no entry). Past it the fire is
        // dropped: nobody called, so there is nobody to answer 429 to.
        // The task id is the fire's identity, so a retried task reuses
        // its first decision instead of counting the fire again.
        if !signal.is_resume {
            let limits = signal.spec()?.limits.resolve();
            let now = crate::lease::now_unix();
            if let Err(refused) = crate::entry_limits::admit_fire(&state.pg_pool, &payload.token, Some(&task.id.to_string()), &limits, now).await? {
                tracing::info!(
                    target: "weft_dispatcher::fire_signal",
                    token = %payload.token, project_id = %signal.project_id,
                    "fire dropped: {} is reached", refused.reason.describe()
                );
                return Ok(serde_json::json!({ "status": axum::http::StatusCode::TOO_MANY_REQUESTS.as_u16() }));
            }
        }
        // Use the FireSignal task id as the dedup nonce: an executor
        // retry of this same task re-calls dispatch with the same
        // nonce, so any RouteEntry task inserted on the first
        // attempt collapses on retry instead of producing a
        // duplicate execution.
        let nonce = task.id.to_string();
        let status = crate::api::signal::dispatch_listener_outcome(
            state,
            &payload.token,
            signal.project_id,
            &signal.tenant_id,
            payload.payload,
            Some(crate::api::signal::ParkedRef { id: &nonce, attempts: 0 }),
        )
        .await
        .map_err(|(code, msg)| anyhow::anyhow!("dispatch_listener_outcome {code}: {msg}"))?;
        Ok(serde_json::json!({ "status": status.as_u16() }))
    }
}
