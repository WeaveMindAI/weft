//! Passing a live caller on to the project's workers, from an address the
//! install shares between its projects: the loopback port, the tunnel's
//! hostname, an install domain (`/connect/<tenant>/...`), or a project's
//! API domain served at its root.
//!
//! A caller who reaches the project's own address talks to its workers
//! straight. One who reaches a shared address is passed on here, and this
//! is a relay only: the tenant's routes (held in memory) pick the project
//! and the program serving the route, and the request goes to that
//! project's workers as the caller sent it. Every check (whether the route
//! takes calls, its gate, the instance, the limits) is the worker's door's
//! (`weft_engine::door`); nothing of the call is read or written here.
//!
//! The worker is told where the caller stood (`weft_core::net::relay_hop`):
//! the address this door read them at, the `Host` they sent, and the
//! prefix the route sits under, which the socket URL a browser is handed is
//! built on. It reads those only on a hop carrying weft's own credential.

use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::state::DispatcherState;

/// Pass `request` to a worker of `project` running `binary_hash`, at
/// `path` (with its leading slash) and `query`, both still percent-encoded
/// as the caller sent them. `prefix` is what the route sits under at the
/// address the caller used (`/connect/<tenant>`, or empty at an API
/// domain's root).
pub(crate) async fn to_project(
    state: &DispatcherState,
    project: uuid::Uuid,
    binary_hash: &str,
    caller: std::net::IpAddr,
    prefix: &str,
    path: &str,
    query: &str,
    request: Request,
) -> Response {
    let path_and_query = if query.is_empty() { path.to_string() } else { format!("{path}?{query}") };
    let tenant = match state.tenant_router.tenant_for_project(project).await {
        Ok(tenant) => tenant,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant lookup: {e:#}")).into_response(),
    };
    let target = match crate::delivery::worker_target(state, tenant.as_str(), project, binary_hash).await {
        Ok(target) => target,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("the project's workers: {e:#}")).into_response(),
    };
    let endpoint = match state.runner.endpoint(&target, weft_platform_traits::Patience::Brief).await {
        Ok(endpoint) => endpoint,
        // A new build's first worker takes minutes to come up; the caller
        // is told so after a short hold instead of being held through it.
        Err(e) if weft_platform_traits::WorkerStarting::is(&e) => {
            tracing::debug!(target: "weft_dispatcher::live_relay", %project, reason = %format!("{e:#}"), "a live caller arrived while the worker starts");
            return (StatusCode::SERVICE_UNAVAILABLE, format!("{e:#}; retry in a moment")).into_response();
        }
        Err(e) => {
            tracing::warn!(target: "weft_dispatcher::live_relay", %project, error = %format!("{e:#}"), "no worker for a live caller");
            return (StatusCode::SERVICE_UNAVAILABLE, format!("no worker of this project could be reached ({e:#}); retry the connection"))
                .into_response();
        }
    };
    let upstream = match crate::proxy::Upstream::worker(endpoint) {
        Ok(upstream) => upstream,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("the worker credential: {e}")).into_response(),
    };
    let forwarding = crate::proxy::Forwarding::ToWorker { caller, route_prefix: prefix.to_string() };
    let answer = crate::proxy::forward(&state.http, upstream, path_and_query, request, forwarding).await;
    // A socket the worker opened answers 101 from this door, not the
    // worker; every other answer the worker gave carries its mark. A
    // worker that could not be reached comes back as this door's own 502,
    // which forgets nothing: on Cloud Run a gone service still answers
    // (its 404), so an unreachable address is not the sign of one.
    let call = match weft_platform_traits::WorkerCall::answered(answer.status(), answer.headers()) {
        weft_platform_traits::WorkerCall::Answered { status: 101, .. } => weft_platform_traits::WorkerCall::Answered { status: 101, from_worker: true },
        call => call,
    };
    state.runner.call_ended(&target, call);
    // The platform's own refusal never reached the program, so it is not
    // the program's answer: it is named, as the delivery and the node
    // test name it.
    if call.platform_refused() {
        tracing::error!(target: "weft_dispatcher::live_relay", %project, status = %answer.status(), "the platform refused weft's call to the worker: weft's account may not invoke it");
        return (
            StatusCode::BAD_GATEWAY,
            format!("the platform refused weft's call to this project's worker ({}): weft's account may not invoke it", answer.status()),
        )
            .into_response();
    }
    answer
}
