//! `fire_signal` task: an answer to a waiting run a listener picked up
//! itself (a form submitted, a job done), which the install hands to its
//! run. An entry's event never comes this way: the listener hands it to its
//! worker, or puts it in its trigger's queue (`weft_listener::fire_sink`).
//! The listener enqueues a row through the broker, the answer already made
//! by its kind; a dispatcher claims it and hands it to its run through
//! `crate::api::signal::answer_run`, without asking the listener again.
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
        // An answer passes the same rule as one that came in at the door
        // (`weft_core::arrival`): it reaches its run while its trigger is
        // on, waits while it is parked, and is dropped once it takes no
        // work.
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
        // A trigger that takes no work should not have a listener holding
        // its signal: its worker drops the fire, and the listener lets go
        // below. Read before the fire, which may wait.
        let takes_no_work = routing.standing().arrival(crate::lease::now_unix()) == weft_core::arrival::Arrival::Refused;
        // A retry of this same task finds the wait answered already, and
        // drops it as a duplicate below.
        let status = match crate::api::signal::answer_run(state, &payload.token, &routing, payload.value).await {
            Ok(status) if takes_no_work => {
                let_go_of_it(state, &payload.token).await?;
                status
            }
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
                    let_go_of_it(state, &payload.token).await?;
                }
                code
            }
            Err((code, msg)) => anyhow::bail!("fire of {} {code}: {msg}", payload.token),
        };
        Ok(serde_json::json!({ "status": status.as_u16() }))
    }
}

/// Have the listener let go of the signal `token`, whose trigger takes no
/// work (it holds it again if a reactivation lands meanwhile). A row gone
/// was let go of by whatever removed it.
async fn let_go_of_it(state: &DispatcherState, token: &str) -> Result<()> {
    if let Some(signal) = state.journal.signal_get(token).await? {
        // What it could not let go of is said where it failed; the next
        // fire tries again.
        if let Err(e) = crate::listener::let_go_of_stopped(&state.pg_pool, &state.listener, std::slice::from_ref(&signal)).await {
            tracing::warn!(
                target: "weft_dispatcher::fire_signal",
                %token, error = %format!("{e:#}"),
                "could not look whether a signal let go of is wanted again; its next fire tries again"
            );
        }
    }
    Ok(())
}

