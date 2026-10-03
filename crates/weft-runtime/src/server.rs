//! What a runtime process serves, and on which port.
//!
//! A local install's one process serves its internal port, carrying every
//! role under its own prefix (`CoreRole::internal_prefix`): `/broker/...`,
//! `/listener/...`, `/supervisor/...` (the dispatcher has no internal
//! routes there). Its public port carries the dispatcher's public API,
//! behind the door that routes by name (`weft_dispatcher::door`), and the
//! internal routes again under
//! [`weft_platform_traits::roles::INTERNAL_DOOR`], for a caller outside
//! the machine. Every internal route checks its caller's identity wherever
//! it is reached.
//!
//! A process of one role serves it on one port, at its root: its internal
//! routes (its tick, when it scales to zero) and, for the dispatcher, its
//! public API behind the door.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use weft_platform_traits::roles::TICK_PATH;
use weft_platform_traits::{Alarm, CoreRole, Wake};

/// A role's own tick, for a role that scales to zero: drain its loops,
/// then set its next wake. `run` takes the loops the caller says a write
/// woke (`?loop=<name>`, repeated: a writer's waker names them, a
/// builder's announcement names the build loop); the loops whose own next
/// look is due run too.
#[derive(Clone)]
pub struct Tick {
    pub role: CoreRole,
    pub run: Arc<dyn Fn(Vec<String>) -> futures::future::BoxFuture<'static, Duration> + Send + Sync>,
    pub alarm: Arc<dyn Alarm>,
}

/// The name of the query parameter a tick reads its woken loops from.
// SYNC: TICK_LOOP_PARAM <-> deploy/terraform/gcp/builds.tf (the push endpoint)
pub const TICK_LOOP_PARAM: &str = "loop";

/// The next wake for a tick that wants another look after `next`: the end
/// of the `next`-long slot `now` falls in, so every tick inside one slot
/// sets the same wake and the platform keeps one.
pub fn next_slot(now_ms: i64, next: Duration) -> i64 {
    let slot = (next.as_millis() as i64).max(1000);
    (now_ms / slot + 1) * slot
}

async fn tick(State(t): State<Tick>, axum::extract::RawQuery(query): axum::extract::RawQuery) -> Response {
    let woken: Vec<String> = url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .filter(|(k, _)| k == TICK_LOOP_PARAM)
        .map(|(_, v)| v.into_owned())
        .collect();
    let next = (t.run)(woken).await;
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

/// Serve `app` on `listener` until the process ends, with the peer's
/// address on every request (the public door counts callers by it).
pub async fn serve(listener: tokio::net::TcpListener, app: Router) -> anyhow::Result<()> {
    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

/// Until the process is told to stop (SIGTERM, or Ctrl+C). The SIGTERM
/// handler is installed by the call itself, not when the answer is first
/// awaited, so a stop that arrives while the caller is still busy with
/// something else is kept for it instead of killing the process.
pub fn shutdown() -> impl std::future::Future<Output = ()> {
    #[cfg(unix)]
    let term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
    async move {
        #[cfg(unix)]
        match term {
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
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
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
