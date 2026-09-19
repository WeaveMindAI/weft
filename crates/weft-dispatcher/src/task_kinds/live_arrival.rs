//! `live_arrival` task: a live caller followed the handshake's redirect
//! and is standing at the worker pod the routing token names; give birth
//! to the execution that token promised, pinned to that pod.
//!
//! Nothing is born at the handshake. The dispatcher matched the route,
//! gated the caller, chose a pod and signed all of that into the routing
//! token; a caller who never follows the redirect leaves nothing behind.
//! When one does arrive, the worker enqueues this task (through the
//! broker, the only door a worker has to the control plane) and waits
//! on its outcome before attaching the connection; the birth itself is
//! the same atomic admit-and-journal every live execution had.
//!
//! The dedup key is `live-arrival:{color}`: a caller's client that resent
//! the request (a retried POST, a browser's second attempt) converges on
//! one task and one execution, and `start_live_execution` answers a
//! second birth of the same color with the pod it already sits on.

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
        // handshake, unexpired, and naming the color and pod. A worker
        // cannot ask for a birth the handshake did not promise.
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
        let tenant = state
            .tenant_router
            .tenant_for_project(&route.project_id)
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
        let pod = crate::api::signal::birth_on_arrival(
            state, &route, &request, tenant.as_str(), claims.color, &claims.pod_name,
        )
        .await
        .map_err(|(status, msg)| anyhow::anyhow!("{msg} ({status})"))?;
        tracing::info!(
            target: "weft_dispatcher::live_arrival",
            color = %claims.color, pod = %pod.pod_name,
            node = %weft_core::project::plain_id(&route.node_id),
            "live caller arrived; execution born on its pod"
        );
        Ok(serde_json::to_value(LiveArrivalResult {
            color: claims.color.to_string(),
            pod_name: pod.pod_name,
        })?)
    }
}

