//! What a runtime process serves, and on which port.
//!
//! The machine's internal port carries every role the machine runs, each
//! under its own prefix (`CoreRole::internal_prefix`): `/broker/...`,
//! `/listener/...`, `/supervisor/...`. The dispatcher has no internal
//! routes there. On an install whose API calls each carry an operator key,
//! the internal port also carries the public API, for callers on the
//! private network. The public port (behind the machine's front door)
//! carries the dispatcher's public API when the dispatcher is on the
//! machine, or passes it on to the dispatcher's own service when it is
//! not, and the internal routes again under
//! [`weft_platform_traits::roles::INTERNAL_DOOR`], for a caller outside
//! the private network (a cloud's queue delivering a wake). Every internal
//! route checks its caller's identity wherever it is reached.
//!
//! A process of one role serves it on one port, at its root: its internal
//! routes and, for the dispatcher, its public API.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use weft_platform_traits::roles::TICK_PATH;
use weft_platform_traits::{Alarm, CoreRole, IdentityTokens, Wake, WORKER_AUTH_HEADER};

/// A role's own tick, for a role that scales to zero: drain its loops,
/// then set its next wake.
#[derive(Clone)]
pub struct Tick {
    pub role: CoreRole,
    pub run: Arc<dyn Fn() -> futures::future::BoxFuture<'static, Duration> + Send + Sync>,
    pub alarm: Arc<dyn Alarm>,
}

/// The next wake for a tick that wants another look after `next`: the end
/// of the `next`-long slot `now` falls in, so every tick inside one slot
/// sets the same wake and the platform keeps one.
pub fn next_slot(now_ms: i64, next: Duration) -> i64 {
    let slot = (next.as_millis() as i64).max(1000);
    (now_ms / slot + 1) * slot
}

async fn tick(State(t): State<Tick>) -> Response {
    let next = (t.run)().await;
    let now = now_ms();
    let wake = Wake {
        key: format!("tick:{}", t.role),
        at_unix_ms: next_slot(now, next),
        role: t.role,
        path: TICK_PATH.to_string(),
        body: serde_json::json!({}),
    };
    match t.alarm.set(wake).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::error!(target: "weft_runtime::server", role = %t.role, error = %format!("{e:#}"), "could not set the next tick");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("the next tick could not be set: {e:#}")).into_response()
        }
    }
}

/// The tick route, for a role placed serverless.
pub fn tick_route(t: Tick) -> Router {
    Router::new().route(TICK_PATH, post(tick)).with_state(t)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_millis() as i64
}

/// Passes a role's internal calls on to the machine of its own it runs
/// on, which has no public address: the caller's credential goes with the
/// request untouched, and the role checks it itself.
#[derive(Clone)]
pub struct ToRole {
    pub base_url: String,
    pub http: reqwest::Client,
}

pub async fn to_role(State(to): State<ToRole>, request: Request) -> Response {
    let path_and_query = request.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
    let upstream = weft_dispatcher::proxy::Upstream { what: "the role's machine", base_url: to.base_url.clone(), auth: None, hold: None };
    weft_dispatcher::proxy::forward(&to.http, upstream, path_and_query, request).await
}

/// Passes the public API on to the dispatcher's own service. The pass
/// carries the address the caller used and who the caller is
/// (`weft_dispatcher::proxy`), so the dispatcher builds its links and
/// counts its callers as if they had reached it directly.
#[derive(Clone)]
pub struct ToDispatcher {
    pub base_url: String,
    pub tokens: Arc<dyn IdentityTokens>,
    pub http: reqwest::Client,
}

pub async fn to_dispatcher(State(to): State<ToDispatcher>, request: Request) -> Response {
    let token = match to.tokens.token_for(&to.base_url).await {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("no identity for the dispatcher: {e:#}")).into_response(),
    };
    let auth = match HeaderValue::from_str(&format!("Bearer {token}")) {
        Ok(v) => v,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("the dispatcher credential: {e}")).into_response(),
    };
    let path_and_query = request.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
    let upstream = weft_dispatcher::proxy::Upstream {
        what: "the dispatcher",
        base_url: to.base_url.clone(),
        auth: Some((HeaderName::from_static(WORKER_AUTH_HEADER), auth)),
        hold: None,
    };
    weft_dispatcher::proxy::forward(&to.http, upstream, path_and_query, request).await
}

/// Serve `app` on `listener` until the process ends, with the peer's
/// address on every request (the public door counts callers by it).
pub async fn serve(listener: tokio::net::TcpListener, app: Router) -> anyhow::Result<()> {
    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => tracing::info!(target: "weft_runtime", "SIGTERM received; draining"),
                    _ = tokio::signal::ctrl_c() => tracing::info!(target: "weft_runtime", "Ctrl+C received; draining"),
                }
            }
            Err(e) => {
                tracing::warn!(target: "weft_runtime", error = %e, "no SIGTERM handler; stopping on Ctrl+C only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tick_inside_one_slot_sets_the_same_wake() {
        let slot = Duration::from_secs(30);
        assert_eq!(next_slot(1_000, slot), 30_000);
        assert_eq!(next_slot(29_999, slot), 30_000);
        assert_eq!(next_slot(30_000, slot), 60_000);
        assert_eq!(next_slot(5, Duration::ZERO), 1_000, "never a wake in the past or at once");
    }
}
