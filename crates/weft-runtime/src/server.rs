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

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use weft_platform_traits::roles::TICK_PATH;
use weft_platform_traits::{Alarm, CoreRole, WakeCall};
use weft_task_store::drain::{Pass, PassFailed, TickBody};
use weft_task_store::unanswered::{self, Callee};

/// One pass of a role's loops ([`Tick::run`]): what the tick was called
/// with, and which pass of its process it is.
pub type PassRun = Arc<dyn Fn(TickBody, Pass) -> futures::future::BoxFuture<'static, Result<(), PassFailed>> + Send + Sync>;

/// A role's own tick, for a role that scales to zero: run the loops the
/// call names. `run` takes what the tick was called with: the loops a
/// writer's ring names (`?loop=<name>`, repeated), or the wake's body
/// ([`TickBody`]: the booking it was set for, or a ring the alarm carried);
/// the loops whose own next look is due run too. Each loop books its own
/// next look, so the tick sets no wake of its own. A loop that fails books
/// its retries or is written down by the pass, which then answers the tick
/// as done; only a pass that could not write that down (the database did
/// not answer it) answers the tick as a failure, marked
/// [`PASS_FAILED_HEADER`] with the loops it names as its body
/// ([`PassFailed`], JSON), so the platform's queue calls it again for as
/// long as its retry window allows.
#[derive(Clone)]
pub struct Tick {
    pub role: CoreRole,
    pub run: PassRun,
    pub alarm: Arc<dyn Alarm>,
    /// Where a ring carried by the alarm is answered, once its pass went
    /// through, and where a booked wake set again for its moment is
    /// recorded on its loop's row:
    /// `None` for a role that never touches the database, whose ring the
    /// process that wrote it answers on its next ring that goes through,
    /// and whose early wake set again is not recorded (a booking that
    /// moves then cancels the wake it was first set as).
    pub pool: Option<weft_task_store::db::Pool>,
}

impl Tick {
    /// One pass: run what `call` says and the loops already due. The tick
    /// route calls it ([`Pass::Tick`]), and so does a process of the role
    /// as it starts ([`Pass::Start`]), for what came due while no process
    /// of it was up and for one more try of each loop that gave up. A ring
    /// the alarm carried is answered only once its pass went through.
    pub async fn pass(&self, call: TickBody, pass: Pass) -> Result<(), PassFailed> {
        let answered_ring = call.after_failed_ring;
        (self.run)(call, pass).await?;
        if let (true, Some(pool)) = (answered_ring, &self.pool) {
            if let Err(e) = unanswered::answered(pool, Callee::Role(self.role.as_str())).await {
                tracing::warn!(target: "weft_runtime::server", role = %self.role, error = %format!("{e:#}"), "could not clear a ring that failed before");
            }
        }
        Ok(())
    }
}

/// The name of the query parameter a tick reads its woken loops from.
pub const TICK_LOOP_PARAM: &str = "loop";

/// The header a tick's failure carries: the role answered, its pass ran
/// and could not write down how it went for the loops its body names
/// ([`PassFailed`], JSON). A failure without it never reached the role
/// (the platform answered).
pub const PASS_FAILED_HEADER: &str = "weft-pass-failed";

/// What a tick was called with: the loops its query names, and the wake's
/// body when an alarm called it. Any other body (a ring's empty one, a
/// push from another service) is no wake's.
fn tick_call(query: Option<&str>, body: &[u8]) -> (Option<i64>, TickBody) {
    let (at, mut call) = match serde_json::from_slice::<WakeCall<TickBody>>(body) {
        Ok(wake) => (Some(wake.at_unix_ms), wake.body),
        Err(_) => (None, TickBody::default()),
    };
    call.loops.extend(
        url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
            .filter(|(k, _)| k == TICK_LOOP_PARAM)
            .map(|(_, v)| v.into_owned()),
    );
    (at, call)
}

async fn tick(State(t): State<Tick>, axum::extract::RawQuery(query): axum::extract::RawQuery, body: axum::body::Bytes) -> Response {
    let (at, call) = tick_call(query.as_deref(), &body);
    // Early: set again for its moment, and run nothing. The loop's row
    // learns which wake of its booking now waits, so a booking that moves
    // cancels that one. A failure answers as one, so the platform delivers
    // this wake again.
    if let (Some(at), Some(booked)) = (at, &call.booked) {
        let again = booked.set_again();
        if let Some(set) = weft_platform_traits::alarm::set_again_if_early(&*t.alarm, at, || Ok(again.wake(t.role, at))).await {
            let set = match (set, &t.pool) {
                (Ok(()), Some(pool)) => weft_task_store::drain::set_again_recorded(pool, t.role, &again).await,
                (set, _) => set,
            };
            return match set {
                Ok(()) => StatusCode::OK.into_response(),
                Err(e) => {
                    tracing::error!(target: "weft_runtime::server", role = %t.role, error = %format!("{e:#}"), "could not set an early wake again");
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("the wake could not be set again: {e:#}")).into_response()
                }
            };
        }
    }
    match t.pass(call, Pass::Tick).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(failed) => {
            tracing::error!(target: "weft_runtime::server", role = %t.role, error = %failed, "a tick's pass could not write down how it went; the platform calls it again");
            (StatusCode::INTERNAL_SERVER_ERROR, [(PASS_FAILED_HEADER, "1")], axum::Json(failed)).into_response()
        }
    }
}

/// The tick route, for a role placed serverless.
pub fn tick_route(t: Tick) -> Router {
    Router::new().route(TICK_PATH, post(tick)).with_state(t)
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
    use weft_task_store::drain::BookedLook;

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock past UNIX_EPOCH")
            .as_millis() as i64
    }

    /// A ring names its loops in the query; a booked wake carries its
    /// booking in the body; anything else (an empty ring, a push from
    /// another service) is a call with nothing booked.
    #[test]
    fn a_tick_reads_its_loops_from_the_query_and_its_booking_from_the_body() {
        let (at, call) = tick_call(Some("loop=a&loop=b&other=c"), b"");
        assert_eq!((at, call.loops, call.booked), (None, vec!["a".to_string(), "b".to_string()], None));
        let body = serde_json::to_vec(&WakeCall {
            at_unix_ms: 42,
            body: TickBody { booked: Some(BookedLook { loop_name: "x".into(), seq: 3, set_again: 0 }), ..TickBody::default() },
        })
        .unwrap();
        let (at, call) = tick_call(None, &body);
        assert_eq!(at, Some(42));
        assert_eq!(call.booked, Some(BookedLook { loop_name: "x".into(), seq: 3, set_again: 0 }));
        let (at, call) = tick_call(None, br#"{"message":{"data":"e30="},"subscription":"s"}"#);
        assert_eq!((at, call), (None, TickBody::default()));
    }

    /// A booked wake that arrives well before its moment (a hop) is set
    /// again for that moment, under a name of its own each time (the task
    /// being answered still holds the old one), and runs nothing; one at
    /// its moment runs.
    #[tokio::test]
    async fn an_early_booked_wake_is_set_again_and_runs_nothing() {
        let alarm = Arc::new(weft_platform_traits::FakeAlarm::new());
        let ran = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter = ran.clone();
        let t = Tick {
            role: CoreRole::Dispatcher,
            run: Arc::new(move |_, _| {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }),
            alarm: alarm.clone(),
            pool: None,
        };
        let far = now_ms() + 20 * 24 * 3600 * 1000;
        let wake = weft_task_store::drain::loop_wake(CoreRole::Dispatcher, "retention", far, 7);
        let call = |wake: &weft_platform_traits::Wake, at: i64| {
            let body = serde_json::to_vec(&WakeCall { at_unix_ms: at, body: wake.body.clone() }).unwrap();
            axum::http::Request::post(TICK_PATH).header("content-type", "application/json").body(axum::body::Body::from(body)).unwrap()
        };
        use tower::ServiceExt as _;
        let answer = tick_route(t.clone()).oneshot(call(&wake, far)).await.unwrap();
        assert_eq!(answer.status(), StatusCode::OK);
        assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 0, "a hop runs nothing");
        let again = alarm.wakes();
        assert_eq!(again.len(), 1);
        assert_eq!((again[0].at_unix_ms, again[0].generation), (far, 7), "the same moment, the same booking");
        assert_ne!(again[0].name(), wake.name(), "under a name of its own");
        let answer = tick_route(t.clone()).oneshot(call(&again[0], far)).await.unwrap();
        assert_eq!(answer.status(), StatusCode::OK);
        let names: Vec<String> = alarm.wakes().iter().map(|w| w.name()).collect();
        assert_eq!(names.len(), 2);
        assert!(names[1] != names[0] && names[1] != wake.name(), "a second hop is named apart again");
        let answer = tick_route(t).oneshot(call(&again[0], now_ms())).await.unwrap();
        assert_eq!(answer.status(), StatusCode::OK);
        assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 1, "at its moment it runs");
    }

    /// A pass that could not write down how it went answers its tick as a
    /// failure the role itself gave, naming the loops it could not write
    /// down, so the platform's queue calls it again and a writer's ring
    /// knows the role answered and for which loops.
    #[tokio::test]
    async fn a_failed_pass_answers_its_tick_as_a_failure_of_its_own() {
        let failed = PassFailed { loops: vec!["run_ends".into()], error: "the database is down".into() };
        let answered = failed.clone();
        let t = Tick {
            role: CoreRole::Dispatcher,
            run: Arc::new(move |_, _| {
                let failed = answered.clone();
                Box::pin(async move { Err(failed) })
            }),
            alarm: Arc::new(weft_platform_traits::FakeAlarm::new()),
            pool: None,
        };
        use tower::ServiceExt as _;
        let ring = axum::http::Request::post(format!("{TICK_PATH}?loop=delivery&loop=run_ends")).body(axum::body::Body::empty()).unwrap();
        let answer = tick_route(t).oneshot(ring).await.unwrap();
        assert_eq!(answer.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(answer.headers().contains_key(PASS_FAILED_HEADER));
        let body = axum::body::to_bytes(answer.into_body(), usize::MAX).await.unwrap();
        assert_eq!(serde_json::from_slice::<PassFailed>(&body).unwrap(), failed);
    }
}
