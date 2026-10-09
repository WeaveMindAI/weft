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
use weft_task_store::pg_signal::{Busy, PgSignalWatch};

/// A role's own tick, for a role that scales to zero: drain its loops,
/// then set its next wake, if any loop wants one. `run` takes the loops
/// the caller says a write woke (`?loop=<name>`, repeated: a writer's
/// waker names them); the loops whose own next look is due run too. It
/// answers how soon a loop next wants a look, or `None` when none does,
/// and then no wake is set: the role sleeps until a write rings it.
#[derive(Clone)]
pub struct Tick {
    pub role: CoreRole,
    pub run: Arc<dyn Fn(Vec<String>) -> futures::future::BoxFuture<'static, Option<Duration>> + Send + Sync>,
    pub alarm: Arc<dyn Alarm>,
}

impl Tick {
    /// One pass: run the woken loops and the due ones, then set the next
    /// wake the pass asks for. The tick route calls it, and so does a
    /// process of the role as it starts, for what came due while no
    /// process of it was up.
    pub async fn pass(&self, woken: Vec<String>) -> anyhow::Result<()> {
        let Some(next) = (self.run)(woken).await else { return Ok(()) };
        let wake = Wake {
            key: format!("tick:{}", self.role),
            at_unix_ms: wake_at(now_ms(), next),
            role: self.role,
            path: TICK_PATH.to_string(),
            body: serde_json::json!({}),
        };
        self.alarm.set(wake).await
    }
}

/// The name of the query parameter a tick reads its woken loops from.
pub const TICK_LOOP_PARAM: &str = "loop";

/// The moment of a wake wanted `next` from `now_ms`: rounded up to the
/// whole second, so every pass that wants the same look sets the same
/// wake and the platform keeps one; a second away at the soonest.
pub fn wake_at(now_ms: i64, next: Duration) -> i64 {
    let at = now_ms.saturating_add(i64::try_from(next.as_millis()).unwrap_or(i64::MAX).max(1000));
    at.saturating_add(999) / 1000 * 1000
}

async fn tick(State(t): State<Tick>, axum::extract::RawQuery(query): axum::extract::RawQuery) -> Response {
    let woken: Vec<String> = url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .filter(|(k, _)| k == TICK_LOOP_PARAM)
        .map(|(_, v)| v.into_owned())
        .collect();
    match t.pass(woken).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::error!(target: "weft_runtime::server", role = %t.role, error = %format!("{e:#}"), "could not set the next tick");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("the next tick could not be set: {e:#}")).into_response()
        }
    }
}

/// Keep `watch` listening while a request is answered, to the end of its
/// answer's body: a stream of events is listened for until it closes. A
/// process that scales to zero lets its watch go quiet only between
/// requests (`weft_task_store::pg_signal::Listening::WhileBusy`), and
/// every request it answers may write, so a quiet watch listens again
/// before the request goes on.
pub async fn keep_listening(
    State(watch): State<Arc<PgSignalWatch>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let busy = watch.busy().await;
    next.run(request).await.map(|body| axum::body::Body::new(Held { body, _busy: busy }))
}

/// An answer's body, holding its process busy until it ends.
struct Held {
    body: axum::body::Body,
    _busy: Busy,
}

impl http_body::Body for Held {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
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
///
/// Every accepted connection sends at once (`TCP_NODELAY`). An answer
/// whose headers and body leave in two writes otherwise holds the body
/// until the caller acknowledges the headers, and a caller on a kept-alive
/// connection delays that acknowledgement by up to 40 ms: every call after
/// the first on a connection paid it.
pub async fn serve(listener: tokio::net::TcpListener, app: Router) -> anyhow::Result<()> {
    use axum::serve::ListenerExt as _;
    let listener = listener.tap_io(|connection| {
        if let Err(e) = connection.set_nodelay(true) {
            tracing::warn!(target: "weft_runtime", error = %e, "could not make a connection send at once; its answers may wait on the caller's acknowledgements");
        }
    });
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

    /// A wake lands where it was asked for, on the next whole second, so
    /// every pass that wants the same moment sets the same wake. A grid as
    /// wide as the wait itself would put a 5-hour wait at a random point
    /// inside it, almost always early, and wake a quiet install again and
    /// again before its look was due.
    #[test]
    fn a_wake_lands_on_the_next_whole_second_after_the_wait() {
        let hours = Duration::from_secs(5 * 3600 + 12 * 60 + 33);
        assert_eq!(wake_at(1_000, hours), 1_000 + 18_753_000);
        assert_eq!(wake_at(1_001, hours), 1_000 + 18_754_000, "rounded up, never early");
        assert_eq!(wake_at(10_200, Duration::from_millis(1_500)), 12_000);
        assert_eq!(wake_at(10_900, Duration::from_millis(1_500)), 13_000);
        assert_eq!(wake_at(5, Duration::ZERO), 2_000, "never a wake at once");
    }
}
