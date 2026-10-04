//! Reaching a project's workers with a live caller.
//!
//! Workers are never public, so every live call reaches one through the
//! install: the handshake (`api::signal::connect_live`) checks the caller,
//! gives birth to their run and mints a signed ticket naming it, then
//! [`to_worker`] asks the platform for an address of the project's workers
//! at the image the ticket names ([`weft_platform_traits::Runner::endpoint`])
//! and passes the caller's request there, a WebSocket included, the ticket
//! riding [`weft_core::caller_token::TICKET_HEADER`]. The worker checks the
//! ticket again and claims the run; everything after that is between the
//! caller and that worker, with the install passing the bytes along.
//!
//! A call is answered in the one request that made it. The exception is a
//! browser's socket: a browser cannot put a credential on a WebSocket's
//! opening request, so it asks with a plain request first and is handed a
//! URL on this module's door ([`forward`], under [`LIVE_PREFIX`]) carrying
//! the ticket, which it opens its socket at.
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

/// The URL a browser opens its socket at, for `project`, on `door`
/// (`<scheme>://<host>[:port]`): the project rides the path, so one
/// hostname (the one the caller already used) serves every project with no
/// wildcard DNS or certificate.
///
/// `raw_path` and `raw_query` are the caller's, byte for byte as they sent
/// them (still percent-encoded), with the ticket appended as the LAST pair:
/// the worker holds the socket to the request the gate approved, so the
/// query it sees once [`split_ticket`] takes the ticket back off must be
/// exactly the one the gate hashed (`?verbose=1` on a route reaches the
/// program's `query` port).
pub(crate) fn live_url(door: &str, project: uuid::Uuid, raw_path: &str, raw_query: &str, token: &str) -> String {
    let door = door.trim_end_matches('/');
    let path = raw_path.trim_start_matches('/');
    let carried = if raw_query.is_empty() { String::new() } else { format!("{raw_query}&") };
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

/// A socket URL's query split into the caller's own query and the ticket
/// [`live_url`] appended as its last pair. `None` when the last pair is no
/// ticket.
fn split_ticket(raw_query: &str) -> Option<(&str, &str)> {
    match raw_query.rsplit_once('&') {
        Some((callers, last)) => Some((callers, last.strip_prefix("wct=")?)),
        None => Some(("", raw_query.strip_prefix("wct=")?)),
    }
}

/// `ANY /live/{project}/...`: the browser's socket, opened at the URL its
/// handshake answered with. The ticket rides that URL's query; it is
/// checked here as well as at the worker, so a socket with no good ticket
/// never starts a worker, and the caller is told why here.
pub async fn forward(State(state): State<DispatcherState>, request: Request) -> Response {
    let Some((project, under)) = split_live_path(request.uri().path()) else {
        return (StatusCode::NOT_FOUND, "no live endpoint at this path").into_response();
    };
    if !crate::api::signal::is_socket_opening(request.headers()) {
        return (
            StatusCode::BAD_REQUEST,
            "this address only opens a WebSocket; call the route itself for anything else",
        )
            .into_response();
    }
    let raw_query = request.uri().query().unwrap_or("").to_string();
    let Some((callers_query, ticket)) = split_ticket(&raw_query) else {
        return (StatusCode::UNAUTHORIZED, "missing routing token").into_response();
    };
    let claims = match weft_core::caller_token::validate(&state.caller_token_secret, ticket, crate::lease::now_unix()) {
        Ok(claims) => claims,
        Err(why) => return (StatusCode::UNAUTHORIZED, weft_core::caller_token::refusal(&why)).into_response(),
    };
    if claims.project_id != project {
        return (StatusCode::FORBIDDEN, "this ticket is for another project").into_response();
    }
    // The worker sees the caller's own query: the ticket moves to its header.
    let (callers_query, ticket) = (callers_query.to_string(), ticket.to_string());
    to_worker(&state, &claims, &ticket, &under, &callers_query, request).await
}

/// Pass `request` to a worker of the ticket's project and program, at
/// `path` (with its leading slash) and `query`, both still percent-encoded
/// as the caller sent them, with `ticket` (whose `claims` these are) for the
/// run it is for.
///
/// When no worker got the call (none could be reached, the platform
/// refused), the run born for it is nobody's: nobody holds a ticket a
/// worker would accept for it any more, since the caller is told to call
/// again. It is erased at once, with the entry slot it held, so that call
/// finds the route's slot free; left alone, it would hold the slot until
/// its deadline.
pub(crate) async fn to_worker(
    state: &DispatcherState,
    claims: &weft_core::caller_token::CallerTokenClaims,
    ticket: &str,
    path: &str,
    query: &str,
    request: Request,
) -> Response {
    match relay(state, claims.project_id, &claims.binary_hash, ticket, path, query, request).await {
        Relayed::Reached(answer) => answer,
        Relayed::NotReached(answer) => {
            let unclaimed = weft_task_store::tasks::UnclaimedLiveRun::NeverPassedOn;
            if let Err(e) = state.journal.erase_unclaimed_live_run(claims.execution_id, unclaimed).await {
                tracing::warn!(
                    target: "weft_dispatcher::live_relay",
                    execution_id = %claims.execution_id, error = %format!("{e:#}"),
                    "could not erase a live run no worker got; the reaper erases it once its ticket expires"
                );
            }
            answer
        }
    }
}

/// How passing a call to a worker went: the worker's own answer, or one
/// given before any worker had the call.
enum Relayed {
    Reached(Response),
    NotReached(Response),
}

async fn relay(
    state: &DispatcherState,
    project: uuid::Uuid,
    binary_hash: &str,
    ticket: &str,
    path: &str,
    query: &str,
    mut request: Request,
) -> Relayed {
    let not_reached = |status: StatusCode, message: String| Relayed::NotReached((status, message).into_response());
    let path_and_query = if query.is_empty() { path.to_string() } else { format!("{path}?{query}") };
    let ticket = match axum::http::HeaderValue::from_str(ticket) {
        Ok(ticket) => ticket,
        Err(e) => return not_reached(StatusCode::INTERNAL_SERVER_ERROR, format!("the ticket is not a header value: {e}")),
    };
    // Set, never appended: a ticket the caller sent under this name is
    // replaced by the one weft minted.
    request.headers_mut().insert(weft_core::caller_token::TICKET_HEADER, ticket);
    let tenant = match state.tenant_router.tenant_for_project(project).await {
        Ok(tenant) => tenant,
        Err(e) => return not_reached(StatusCode::INTERNAL_SERVER_ERROR, format!("tenant lookup: {e:#}")),
    };
    let target = match crate::delivery::worker_target(state, tenant.as_str(), project, binary_hash).await {
        Ok(target) => target,
        Err(e) => return not_reached(StatusCode::INTERNAL_SERVER_ERROR, format!("the project's workers: {e:#}")),
    };
    let endpoint = match state.runner.endpoint(&target, weft_platform_traits::Patience::Brief).await {
        Ok(endpoint) => endpoint,
        // A new build's first worker takes minutes to come up; the caller
        // is told so after a short hold instead of being held through it.
        Err(e) if weft_platform_traits::WorkerStarting::is(&e) => {
            tracing::debug!(target: "weft_dispatcher::live_relay", %project, reason = %format!("{e:#}"), "a live caller arrived while the worker starts");
            return not_reached(StatusCode::SERVICE_UNAVAILABLE, format!("{e:#}; retry in a moment"));
        }
        Err(e) => {
            tracing::warn!(target: "weft_dispatcher::live_relay", %project, error = %format!("{e:#}"), "no worker for a live caller");
            return not_reached(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("no worker of this project could be reached ({e:#}); retry the connection"),
            );
        }
    };
    let upstream = match crate::proxy::Upstream::worker(endpoint) {
        Ok(upstream) => upstream,
        Err(e) => return not_reached(StatusCode::INTERNAL_SERVER_ERROR, format!("the worker credential: {e}")),
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
        return not_reached(
            StatusCode::BAD_GATEWAY,
            format!("the platform refused weft's call to this project's worker ({}): weft's account may not invoke it", answer.status()),
        );
    }
    // Only a worker's own answer (or a socket it opened) says it had the
    // call; anything else (this door's 502, the platform's busy answers)
    // came from before it.
    match call {
        weft_platform_traits::WorkerCall::Answered { from_worker: true, .. } => Relayed::Reached(answer),
        _ => Relayed::NotReached(answer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: &str = "5f0c3d52-6c6b-4c1e-9d4e-0a2b3c4d5e6f";

    #[test]
    fn the_project_rides_the_path() {
        let project: uuid::Uuid = P.parse().unwrap();
        assert_eq!(live_url("http://127.0.0.1:14112", project, "/chat", "", "v1.a.b"), format!("http://127.0.0.1:14112/live/{P}/chat?wct=v1.a.b"));
        assert_eq!(live_url("https://weft.example.com/", project, "/", "", "tok"), format!("https://weft.example.com/live/{P}/?wct=tok"));
    }

    /// The caller's own query rides to the worker byte for byte, which is
    /// what the gate hashed; the ticket is the last pair, so a `wct` the
    /// caller sent stays theirs and cannot shadow the hop's.
    #[test]
    fn the_callers_query_rides_along_untouched_and_cannot_shadow_the_ticket() {
        let project: uuid::Uuid = P.parse().unwrap();
        for callers in ["verbose=1&q=a%20b", "a=1&&b=2&", "wct=fake&a=1"] {
            let url = live_url("https://gw", project, "/users/42", callers, "t");
            assert_eq!(url, format!("https://gw/live/{P}/users/42?{callers}&wct=t"));
            let query = url.split_once('?').unwrap().1;
            assert_eq!(split_ticket(query), Some((callers, "t")));
        }
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
    fn the_ticket_is_the_last_pair() {
        assert_eq!(split_ticket("wct=v1.x.y"), Some(("", "v1.x.y")));
        assert_eq!(split_ticket("a=1&wct=v1.x.y"), Some(("a=1", "v1.x.y")));
        assert_eq!(split_ticket("wct=v1.x.y&b=2"), None, "the ticket is always appended last");
        assert_eq!(split_ticket("a=1"), None);
    }
}
