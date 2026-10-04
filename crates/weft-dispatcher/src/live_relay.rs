//! The live door: where a live caller goes after the handshake.
//!
//! The handshake (`api::signal::connect_live`) checks the caller and hands
//! them a ticket and an address on this door:
//! `<door>/live/<project>/<path>?<their query>&wct=<ticket>`. Workers are
//! never public, so this door is how the caller reaches one: it reads the
//! ticket, asks the platform for an address of the project's workers at
//! the image the ticket names ([`weft_platform_traits::Runner::endpoint`]),
//! and forwards the caller's request there, a WebSocket included. The
//! worker checks the ticket again and claims the run the handshake gave
//! birth to; everything after that is between the caller and that worker,
//! with this door passing the bytes along.
//!
//! The caller's request reaches the worker as they sent it: method, path
//! under the project, query, headers and body. Only the hop's own headers
//! are dropped, and weft's credential rides a header of its own
//! ([`weft_platform_traits::WORKER_AUTH_HEADER`]), so a caller's own
//! `Authorization` reaches the program untouched.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::state::DispatcherState;

/// The prefix of every live caller URL on the install's door.
pub const LIVE_PREFIX: &str = "/live";

/// The live URL for a caller of `project`, on `door`
/// (`<scheme>://<host>[:port]`): the project rides the path, so one
/// hostname (the one the caller already used) serves every project with no
/// wildcard DNS or certificate.
///
/// The caller's OWN query string rides along in front of the ticket,
/// verbatim, so the request the caller resends is the request it made: the
/// query is the caller's data, like the headers and the body, and the
/// worker is where all three are read (`?verbose=1` on a route reaches the
/// program's `query` port). Only a `wct` the caller itself sent is
/// dropped, so nobody can shadow the hop's own ticket.
pub(crate) fn live_url(door: &str, project: uuid::Uuid, mount_path: &str, raw_query: &str, token: &str) -> String {
    let door = door.trim_end_matches('/');
    let path = mount_path.trim_start_matches('/');
    let carried: Vec<&str> = raw_query
        .split('&')
        .filter(|kv| !kv.is_empty() && *kv != "wct" && !kv.starts_with("wct="))
        .collect();
    let carried = if carried.is_empty() { String::new() } else { format!("{}&", carried.join("&")) };
    format!("{door}{LIVE_PREFIX}/{project}/{path}?{carried}wct={token}")
}

/// Split a live door path into the project and the path under it (with
/// its leading slash, as the worker is called at). `None` when the path is
/// not a live door path.
fn split_live_path(path: &str) -> Option<(uuid::Uuid, String)> {
    let rest = path.strip_prefix(LIVE_PREFIX)?.strip_prefix('/')?;
    let (project, under) = match rest.split_once('/') {
        Some((project, under)) => (project, under),
        None => (rest, ""),
    };
    Some((project.parse().ok()?, format!("/{under}")))
}

/// The ticket in a raw query string (`a=b&wct=...&c=d`).
fn ticket_in(raw_query: &str) -> Option<&str> {
    raw_query.split('&').find_map(|kv| kv.strip_prefix("wct="))
}

/// `ANY /live/{project}/...`: forward a live caller to one of the
/// project's workers.
pub async fn forward(State(state): State<DispatcherState>, request: Request) -> Response {
    let Some((project, under)) = split_live_path(request.uri().path()) else {
        return (StatusCode::NOT_FOUND, "no live endpoint at this path").into_response();
    };
    let raw_query = request.uri().query().unwrap_or("").to_string();
    // The ticket is read here as well as at the worker, so a call with no
    // good ticket never starts a worker, and the caller is told why here.
    let Some(ticket) = ticket_in(&raw_query) else {
        return (StatusCode::UNAUTHORIZED, "missing routing token").into_response();
    };
    let claims = match weft_core::caller_token::validate(&state.caller_token_secret, ticket, crate::lease::now_unix()) {
        Ok(claims) => claims,
        Err(why) => return (StatusCode::UNAUTHORIZED, weft_core::caller_token::refusal(&why)).into_response(),
    };
    if claims.project_id != project {
        return (StatusCode::FORBIDDEN, "this ticket is for another project").into_response();
    }
    let tenant = match state.tenant_router.tenant_for_project(project).await {
        Ok(tenant) => tenant,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant lookup: {e:#}")).into_response(),
    };
    let target = match crate::delivery::worker_target(&state, tenant.as_str(), project, &claims.binary_hash).await {
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
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("no worker of this project could be reached ({e:#}); retry the connection"),
            )
                .into_response();
        }
    };
    let path_and_query = if raw_query.is_empty() { under } else { format!("{under}?{raw_query}") };
    let upstream = match crate::proxy::Upstream::worker(endpoint) {
        Ok(upstream) => upstream,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("the worker credential: {e}")).into_response(),
    };
    let answer = crate::proxy::forward(&state.http, upstream, path_and_query, request).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    const P: &str = "5f0c3d52-6c6b-4c1e-9d4e-0a2b3c4d5e6f";

    #[test]
    fn the_project_rides_the_path() {
        let project: uuid::Uuid = P.parse().unwrap();
        assert_eq!(live_url("http://127.0.0.1:14112", project, "chat", "", "v1.a.b"), format!("http://127.0.0.1:14112/live/{P}/chat?wct=v1.a.b"));
        assert_eq!(live_url("https://weft.example.com/", project, "", "", "tok"), format!("https://weft.example.com/live/{P}/?wct=tok"));
    }

    /// The caller's own query rides to the worker, which is where the
    /// program reads it; a `wct` the caller sent cannot shadow the hop's.
    #[test]
    fn the_callers_query_rides_along_and_cannot_shadow_the_ticket() {
        let project: uuid::Uuid = P.parse().unwrap();
        assert_eq!(live_url("https://gw", project, "users/42", "verbose=1&q=a%20b", "t"), format!("https://gw/live/{P}/users/42?verbose=1&q=a%20b&wct=t"));
        assert_eq!(live_url("https://gw", project, "x", "wct=fake&a=1", "t"), format!("https://gw/live/{P}/x?a=1&wct=t"));
    }

    #[test]
    fn a_live_path_splits_into_the_project_and_what_is_under_it() {
        let project: uuid::Uuid = P.parse().unwrap();
        assert_eq!(split_live_path(&format!("/live/{P}/users/42")), Some((project, "/users/42".into())));
        assert_eq!(split_live_path(&format!("/live/{P}/")), Some((project, "/".into())));
        assert_eq!(split_live_path(&format!("/live/{P}")), Some((project, "/".into())));
        assert_eq!(split_live_path("/live/not-a-project/x"), None);
        assert_eq!(split_live_path(&format!("/lived/{P}/x")), None);
    }

    #[test]
    fn the_ticket_is_read_off_the_query() {
        assert_eq!(ticket_in("a=1&wct=v1.x.y&b=2"), Some("v1.x.y"));
        assert_eq!(ticket_in("a=1"), None);
    }
}
