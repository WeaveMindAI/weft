//! `live_arrival` task: a live caller followed the handshake's redirect
//! and reached one of the project's workers through the live door; give
//! birth to the execution the routing token promised, pinned to that
//! worker instance, which drives it inside the caller's own request.
//!
//! Nothing is born at the handshake. The dispatcher matched the route,
//! gated the caller and signed all of that into the routing token; a
//! caller who never follows the redirect leaves nothing behind.
//! When one does arrive, the worker enqueues this task (through the
//! broker, the only door a worker has to the control plane) and waits
//! on its outcome before attaching the connection; the birth itself is
//! the same atomic admit-and-journal every live execution had.
//!
//! The dedup key is `live-arrival:{execution_id}`: a caller's client that resent
//! the request (a retried POST, a browser's second attempt) converges on
//! one task and one execution, and `start_live_execution` answers a
//! second birth of the same execution with the instance it already sits on.

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::kinds::{LiveArrivalPayload, LiveArrivalResult};
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

pub struct LiveArrivalExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for LiveArrivalExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: LiveArrivalPayload = serde_json::from_value(task.payload.clone())?;
        // The token is the proof: signed by this dispatcher at the
        // handshake, unexpired, and naming the execution. A worker cannot ask
        // for a birth the handshake did not promise. The instance is the
        // caller's own worker: the broker refuses an arrival naming any
        // instance but the one asking.
        let claims = weft_core::caller_token::validate(
            &state.caller_token_secret,
            &payload.token,
            crate::lease::now_unix(),
        )
        .map_err(|why| anyhow::anyhow!("live arrival refused: {why}"))?;
        let route = crate::api::signal::armed_route(state, &claims.signal)
            .await
            .map_err(|(status, msg)| anyhow::anyhow!("{msg} ({status})"))?;
        anyhow::ensure!(
            route.project_id == claims.project_id,
            "routing token names project {} but its route belongs to {}",
            claims.project_id,
            route.project_id
        );
        // The project may have gone down between the handshake and the
        // arrival: a birth then would run on a project that is not
        // listening.
        route.require_active().map_err(|(_, msg)| anyhow::anyhow!("{msg}"))?;
        // The route was re-armed with another program since the handshake:
        // the caller was forwarded to workers of the old one, which must
        // not run the new one.
        if route.program.binary_hash != claims.binary_hash {
            return Ok(serde_json::to_value(LiveArrivalResult::Refused {
                status: axum::http::StatusCode::CONFLICT.as_u16(),
                message: weft_core::caller_token::refusal("a new version of this program went live after it was issued"),
            })?);
        }
        let tenant = state
            .tenant_router
            .tenant_for_project(route.project_id)
            .await
            .context("tenant for the arriving caller's project")?;
        let request = weft_core::caller::LiveRequest {
            method: payload.method,
            path: claims.path,
            params: claims.params,
            query: payload.query,
            headers: payload.headers,
            caller: claims.caller,
        };
        let instance = match crate::api::signal::birth_on_arrival(
            state, &route, &request, tenant.as_str(), claims.execution_id, &payload.instance,
            claims.member.as_ref(),
        )
        .await
        {
            Ok(instance) => instance,
            // A run refused before it was born (a member gap, a bad
            // payload) is the caller's answer, not this task failing.
            Err((status, message)) if status.is_client_error() => {
                return Ok(serde_json::to_value(LiveArrivalResult::Refused { status: status.as_u16(), message })?);
            }
            Err((status, msg)) => anyhow::bail!("{msg} ({status})"),
        };
        tracing::info!(
            target: "weft_dispatcher::live_arrival",
            execution_id = %claims.execution_id, %instance,
            node = %route.node_id,
            "live caller arrived; execution born on its worker"
        );
        Ok(serde_json::to_value(LiveArrivalResult::Born { execution_id: claims.execution_id.to_string(), instance })?)
    }
}

