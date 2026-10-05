//! `fire_signal` task: a held-event signal fired inside a pooled
//! listener process (Timer expiry, SSE event delivery, future
//! browser-session resolution). The listener enqueues a row through the
//! broker; a dispatcher claims it and runs it through the same gate and
//! `dispatch_listener_outcome` path a fire at the door goes through.
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
        // A fire a listener picked up itself passes the same rule as one
        // that came in at the door (`crate::arrival`): it becomes a run
        // while its trigger is on, waits while it is parked, and is
        // dropped once it takes no work. Even though the listener process
        // holding this signal may have been reaped between when the event
        // was held and when this task runs, `ensure_placed_handle` (inside
        // `dispatch_listener_outcome`) re-places the signal from its
        // durable row onto a live process to honor the fire.
        use axum::http::StatusCode;
        let routing = match crate::api::signal::lookup_signal_routing(state, &payload.token).await {
            Ok(routing) => routing,
            // Its row went after the listener heard it (a wipe, which told
            // the listener as it removed it): nothing is left to fire.
            Err((StatusCode::NOT_FOUND, _)) => {
                tracing::info!(target: "weft_dispatcher::fire_signal", token = %payload.token, "fire dropped: its signal is gone");
                return Ok(serde_json::json!({ "status": StatusCode::GONE.as_u16() }));
            }
            Err((code, msg)) => anyhow::bail!("signal {} {code}: {msg}", payload.token),
        };
        // An entry's fire counts against its per-minute limit here, the
        // one place every listener-held fire passes (a resume answers a
        // run already going and is no entry). Past it the fire is
        // dropped: nobody called, so there is nobody to answer 429 to.
        // The task id is the fire's identity, so a retried task reuses
        // its first decision instead of counting the fire again.
        if !routing.is_resume {
            let now = crate::lease::now_unix();
            if let Err(refused) = crate::entry_limits::admit_fire(&state.pg_pool, &payload.token, Some(&task.id.to_string()), &routing.limits, now).await? {
                tracing::info!(
                    target: "weft_dispatcher::fire_signal",
                    token = %payload.token, project_id = %routing.project_id,
                    "fire dropped: {} is reached", refused.reason.describe()
                );
                return Ok(serde_json::json!({ "status": axum::http::StatusCode::TOO_MANY_REQUESTS.as_u16() }));
            }
        }
        // The task id is the fire's identity: a retry of this same task
        // collapses onto what the first attempt did, whether it started
        // a run (the route task's dedup nonce) or queued the fire.
        let nonce = task.id.to_string();
        let status = match crate::api::signal::apply_lifecycle_gate(state, &payload.token, &routing, payload.payload, Some(&nonce)).await {
            Ok(status) => status,
            // Nobody called, so there is nobody to answer: a fire the
            // trigger takes no work for (wiped, past its hibernation), one
            // its full parked queue cannot hold, or an answer a run already
            // has is dropped, and says why.
            Err((code @ (StatusCode::GONE | StatusCode::TOO_MANY_REQUESTS | StatusCode::CONFLICT), msg)) => {
                tracing::info!(
                    target: "weft_dispatcher::fire_signal",
                    token = %payload.token, project_id = %routing.project_id,
                    "fire dropped: {msg}"
                );
                // The trigger takes no work, so its listener should not be
                // holding the signal: it lets go (and holds it again if a
                // reactivation lands meanwhile). A row gone was let go of by
                // whatever removed it.
                if code == StatusCode::GONE {
                    if let Some(signal) = state.journal.signal_get(&payload.token).await? {
                        // What it could not let go of is said where it failed;
                        // the next fire tries again.
                        if let Err(e) = crate::listener::let_go_of_stopped(&state.pg_pool, &state.listener, std::slice::from_ref(&signal)).await {
                            tracing::warn!(
                                target: "weft_dispatcher::fire_signal",
                                token = %payload.token, error = %format!("{e:#}"),
                                "could not look whether a signal let go of is wanted again; its next fire tries again"
                            );
                        }
                    }
                }
                code
            }
            Err((code, msg)) => anyhow::bail!("fire of {} {code}: {msg}", payload.token),
        };
        Ok(serde_json::json!({ "status": status.as_u16() }))
    }
}

