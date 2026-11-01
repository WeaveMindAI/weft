//! The broker's end of the line (`weft_broker_client::line`): one
//! WebSocket per calling process, every call on it run through the same
//! router a request would reach, and the notifications the caller may see
//! pushed down it.
//!
//! The caller is checked at the door, the way every call is checked again
//! after it (each call carries its own bearer, and its handler verifies
//! it): a line nobody may open is refused before it is upgraded, and what
//! the door proved is what decides which notifications the line carries.
//!
//! The broker's database listener hears every notification of the install
//! (every journal row, every task), and only a few are for workers. One
//! [`LineFanout`] follows it for every line and hands each of those few to
//! the lines of the project or the tenant it names, so the work per
//! notification does not grow with the number of lines.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tokio::sync::mpsc;

use weft_broker_client::line::server::{self, Follower};
use weft_broker_client::line::{Notice, ACCESS_CHANNEL, CANCEL_CHANNEL, INFRA_STATUS_CHANNEL, LINE_PATH, PARKED_FIRE_CHANNEL, TRIGGERS_CHANNEL};
use weft_platform_traits::identity::Principal;
use weft_task_store::pg_signal::{Heard, Subscription};

use crate::state::BrokerState;

/// `api` with the line beside it. Each call, as a request or on a line,
/// holds the database's listening while it is answered
/// (`weft_task_store::pg_signal::keep_listening`): a write made while the
/// watch is quiet would be announced to nobody. Opening a line, and a line
/// with no call running, hold nothing: a worker whose runs only wait keeps
/// its line open without keeping the database awake. Probes touch nothing.
pub fn routes(api: Router, state: Arc<BrokerState>) -> Router {
    let served = api.layer(axum::middleware::from_fn_with_state(state.signals.clone(), weft_task_store::pg_signal::keep_listening));
    let on_line = served.clone();
    Router::new()
        .route(
            LINE_PATH,
            get(move |State(state): State<Arc<BrokerState>>, headers: HeaderMap, ws: WebSocketUpgrade| {
                let api = on_line.clone();
                async move { open(state, headers, ws, api).await }
            }),
        )
        .route("/health", get(crate::handlers::health))
        .with_state(state)
        .merge(served)
}

async fn open(state: Arc<BrokerState>, headers: HeaderMap, ws: WebSocketUpgrade, api: Router) -> Response {
    let principal = match crate::auth::verified_principal(&state, &headers).await {
        Ok(principal) => principal,
        Err((status, message)) => return (status, message).into_response(),
    };
    let follower = follower(state.lines.clone(), principal);
    let stopping = state.stopping.clone();
    server::upgrade(ws).on_upgrade(move |socket| server::serve(socket, api, Some(follower), stopping)).into_response()
}

/// How many notices a line may fall behind by before the fan-out lets go of
/// it; it then follows again and tells its caller to look again.
const LINE_NOTICES: usize = 1024;

/// What pushes one caller's notices down its line: what the fan-out hands
/// it, and whether the notifications can be trusted (the listener's own
/// losses and rechecks, which the caller's copies need as much as the
/// notifications). A worker hears its own project and its tenant's shared
/// connections; weft's own roles keep no copy that needs notices, so their
/// line follows nothing and says so. Either way the line is told how long
/// this copy keeps one line ([`LineFanout::start`]), so the caller knows
/// when it is cut.
fn follower(fanout: Arc<LineFanout>, principal: Principal) -> Follower {
    Box::new(move |notices: mpsc::Sender<Notice>| {
        tokio::spawn(async move {
            let Principal::Worker { tenant, project } = principal else {
                let _ = notices.send(fanout.following(false)).await;
                return;
            };
            let project = project.to_string();
            let mut first = true;
            loop {
                let (handed, mut heard) = mpsc::channel(LINE_NOTICES);
                let _following = fanout.follow(&tenant, &project, handed);
                let listening = fanout.listening.load(Ordering::Acquire);
                let said = match first {
                    true => fanout.following(listening),
                    // Back after falling behind: what was missed is not known.
                    false => Notice::Recheck { listening },
                };
                first = false;
                if notices.send(said).await.is_err() {
                    return;
                }
                while let Some(notice) = heard.recv().await {
                    if notices.send(notice).await.is_err() {
                        return;
                    }
                }
            }
        })
    })
}

/// Who a notification on `channel` is for, by its `payload`: an infra
/// copy's change is its project's; a connection's is its project's, or its
/// tenant's when it is shared across the tenant's projects
/// (`tenant:<tenant>`); a cancel, and an answer handed to a run, are the
/// run's project's (`<project> <run>`; an event parked for a trigger
/// names its project alone, and is for nobody on a line); a trigger's
/// change is its project's. Every other channel is for nobody on a line.
// SYNC: the channels and payloads <-> weft_broker_client::line::LINE_CHANNELS, crates/weft-dispatcher/src/infra_node.rs (infra_node_status_notify), crates/weft-dispatcher/src/project_store.rs (project_declared_infra_notify), crates/weft-access-store/src/lib.rs (access_notify), crates/weft-task-store/src/tasks.rs (task_ready_notify), crates/weft-task-store/src/runs.rs (run_payload), crates/weft-broker/src/caller_auth.rs (names), crates/weft-dispatcher/src/held.rs (access_changed)
fn audience<'a>(channel: &str, payload: &'a str) -> Option<Audience<'a>> {
    match channel {
        INFRA_STATUS_CHANNEL => Some(Audience::Project(payload)),
        CANCEL_CHANNEL | PARKED_FIRE_CHANNEL => payload.split_once(' ').map(|(project, _)| Audience::Project(project)),
        ACCESS_CHANNEL => Some(match payload.strip_prefix("tenant:") {
            Some(tenant) => Audience::Tenant(tenant),
            None => Audience::Project(payload),
        }),
        TRIGGERS_CHANNEL => Some(Audience::Project(payload)),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Audience<'a> {
    Project(&'a str),
    Tenant(&'a str),
}

/// Every line that follows notifications, by the project and the tenant of
/// its worker (see the module doc).
pub struct LineFanout {
    lines: Mutex<Lines>,
    /// Whether the broker's database listener hears right now.
    listening: AtomicBool,
    next: AtomicU64,
    /// How long this copy's platform lets one request (a line) live
    /// before cutting it, `None` when it never does.
    max_age: Option<std::time::Duration>,
}

#[derive(Default)]
struct Lines {
    by_project: HashMap<String, HashMap<u64, mpsc::Sender<Notice>>>,
    by_tenant: HashMap<String, HashMap<u64, mpsc::Sender<Notice>>>,
}

impl Lines {
    fn drop_line(&mut self, id: u64) {
        for of in [&mut self.by_project, &mut self.by_tenant] {
            of.retain(|_, lines| {
                lines.remove(&id);
                !lines.is_empty()
            });
        }
    }
}

/// A line's place in the fan-out, given up when it goes.
pub(crate) struct Following {
    fanout: Weak<LineFanout>,
    id: u64,
}

impl Drop for Following {
    fn drop(&mut self) {
        if let Some(fanout) = self.fanout.upgrade() {
            fanout.lines.lock().expect("line fanout").drop_line(self.id);
        }
    }
}

impl LineFanout {
    /// Start following `heard` (the broker's signal watch) for every line,
    /// each told as it opens that it is cut after `max_age`.
    pub fn start(mut heard: Subscription, max_age: Option<std::time::Duration>) -> Arc<Self> {
        let fanout = Arc::new(Self { lines: Mutex::default(), listening: AtomicBool::new(heard.listening()), next: AtomicU64::new(1), max_age });
        let following = Arc::downgrade(&fanout);
        tokio::spawn(async move {
            loop {
                let next = heard.next().await;
                let Some(fanout) = following.upgrade() else { return };
                match next {
                    Ok(Heard::Signal { channel, payload }) => {
                        if let Some(audience) = audience(channel, &payload) {
                            fanout.hand(audience, Notice::Signal { channel: channel.to_string(), payload: payload.to_string() });
                        }
                    }
                    // Looking again, whether or not the database's listener
                    // hears right now: a recheck may come from the install's
                    // news while the database sleeps (`crate::news`), and a
                    // cancel announced in a gap is found by looking.
                    Ok(Heard::Recheck) => fanout.say_to_all(Notice::Recheck { listening: heard.listening() }),
                    // Back from a quiet spell, in which what runs wait on
                    // came as the install's news: nothing to look at again.
                    Ok(Heard::Resumed) => fanout.say_to_all(Notice::Resumed),
                    Ok(Heard::Lost) => fanout.say_to_all(Notice::Lost),
                    Err(e) => {
                        tracing::error!(target: "weft_broker::line", error = %format!("{e:#}"), "the broker's database listener stopped; every line says so");
                        fanout.say_to_all(Notice::Lost);
                        return;
                    }
                }
            }
        });
        fanout
    }

    /// What a line is told as it opens: whether the listener hears
    /// (`listening`), and how long it keeps one line.
    fn following(&self, listening: bool) -> Notice {
        Notice::Following { listening, max_age_secs: self.max_age.map(|age| age.as_secs()) }
    }

    pub(crate) fn follow(self: &Arc<Self>, tenant: &str, project: &str, line: mpsc::Sender<Notice>) -> Following {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let mut lines = self.lines.lock().expect("line fanout");
        lines.by_project.entry(project.to_string()).or_default().insert(id, line.clone());
        lines.by_tenant.entry(tenant.to_string()).or_default().insert(id, line);
        Following { fanout: Arc::downgrade(self), id }
    }

    /// Hand `notice` to every line `audience` names. A line that cannot
    /// take it now is let go of: it follows again and looks again.
    fn hand(&self, audience: Audience<'_>, notice: Notice) {
        let mut lines = self.lines.lock().expect("line fanout");
        let named = match audience {
            Audience::Project(project) => lines.by_project.get(project),
            Audience::Tenant(tenant) => lines.by_tenant.get(tenant),
        };
        let behind: Vec<u64> =
            named.into_iter().flatten().filter(|(_, line)| line.try_send(notice.clone()).is_err()).map(|(id, _)| *id).collect();
        for id in behind {
            lines.drop_line(id);
        }
    }

    /// Every line is told `notice`: look again (saying whether the
    /// listener hears now), the listener hears again, or it stopped.
    fn say_to_all(&self, notice: Notice) {
        let listening = matches!(notice, Notice::Recheck { listening: true } | Notice::Resumed);
        self.listening.store(listening, Ordering::Release);
        self.send_to_all(notice);
    }

    /// Every open line looks again, the database's listener being as it
    /// is: what the install's news says when it may have missed some
    /// (`crate::news`).
    pub fn recheck_lines(&self) {
        self.send_to_all(Notice::Recheck { listening: self.listening.load(Ordering::Acquire) });
    }

    fn send_to_all(&self, notice: Notice) {
        let mut lines = self.lines.lock().expect("line fanout");
        let behind: Vec<u64> = lines
            .by_project
            .values()
            .flatten()
            .filter(|(_, line)| line.try_send(notice.clone()).is_err())
            .map(|(id, _)| *id)
            .collect();
        for id in behind {
            lines.drop_line(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    use axum::routing::post;
    use weft_broker_client::line::{BrokerCall, CallWait, Twice};
    use weft_broker_client::{BrokerLink, TokenSource};
    use weft_task_store::pg_signal::Heard;

    /// A call to `path` of the test's broker, never sent twice.
    fn call(path: &'static str) -> BrokerCall {
        BrokerCall { path, twice: Twice::NotSafe }
    }

    fn link(address: std::net::SocketAddr) -> BrokerLink {
        BrokerLink::new(
            format!("http://{address}"),
            TokenSource::worker(StdArc::new(weft_platform_traits::FixedToken("t".into())), "w-1"),
        )
    }

    /// The fake broker's routes: `/v1/echo` answers its body back with the
    /// caller's bearer and replica, and `/v1/hold` answers once released.
    fn api(release: StdArc<tokio::sync::Notify>) -> Router {
        Router::new()
            .route(
                "/v1/echo",
                post(|headers: HeaderMap, body: String| async move {
                    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                    format!("{body}|{}|{}", header("authorization"), header(weft_platform_traits::identity::REPLICA_HEADER))
                }),
            )
            .route(
                "/v1/hold",
                post(move || {
                    let release = release.clone();
                    async move {
                        release.notified().await;
                        "released"
                    }
                }),
            )
    }

    async fn serve(listener: tokio::net::TcpListener, app: Router) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() })
    }

    // A stopping broker copy says so on the line, and its caller moves to
    // a line on a running copy at once: a new call is answered there,
    // never held behind the one still running on the old line, which is
    // answered where it runs, and the old line is closed then. A call
    // written on the old line before the caller heard is turned away as
    // not done, with nothing of it run.
    weft_core::stress_test! {
        name: a_stopping_copy_answers_what_runs_and_its_caller_moves_to_a_running_copy,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            let release = StdArc::new(tokio::sync::Notify::new());
            let holding = StdArc::new(tokio::sync::Notify::new());
            let stopping = weft_task_store::stopping::Stopping::new();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            // `/v1/held` says when it is running, so the copy stops with it
            // on the line.
            let held_api = Router::new().route(
                "/v1/held",
                post({
                    let (release, holding) = (release.clone(), holding.clone());
                    move || {
                        let (release, holding) = (release.clone(), holding.clone());
                        async move {
                            holding.notify_one();
                            release.notified().await;
                            "released"
                        }
                    }
                }),
            );
            let app = api(StdArc::new(tokio::sync::Notify::new())).merge(held_api);
            // The first line lands on the copy that stops, every later one
            // on a running copy.
            let lines = StdArc::new(AtomicU64::new(0));
            let first_closed = StdArc::new(AtomicBool::new(false));
            let (served, stops, closed) = (app.clone(), stopping.clone(), first_closed.clone());
            let copies = Router::new().route(
                LINE_PATH,
                get(move |ws: WebSocketUpgrade| {
                    let (api, closed) = (served.clone(), closed.clone());
                    let (first, stopping) = match lines.fetch_add(1, Ordering::SeqCst) {
                        0 => (true, stops.clone()),
                        _ => (false, weft_task_store::stopping::Stopping::new()),
                    };
                    async move {
                        server::upgrade(ws).on_upgrade(move |socket| async move {
                            server::serve(socket, api, None, stopping).await;
                            if first {
                                closed.store(true, Ordering::SeqCst);
                            }
                        })
                    }
                }),
            );
            let _server = serve(listener, copies).await;
            let link = link(address);
            let held = tokio::spawn({
                let link = link.clone();
                async move { link.call(call("/v1/held"), b"{}".to_vec(), CallWait::within(Duration::from_secs(10))).await }
            });
            tokio::time::timeout(Duration::from_secs(10), holding.notified()).await.expect("the held call runs");
            stopping.begin();
            // Sent again while it is turned away, as every caller does
            // (`weft_broker_client::client::while_busy`): it is answered on
            // the running copy long before the held call ends.
            let moved = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let answer = link.call(call("/v1/echo"), b"late".to_vec(), CallWait::within(Duration::from_secs(10))).await.unwrap();
                    if weft_broker_client::client::answered_busy(answer.status, &answer.body) {
                        assert_eq!(String::from_utf8(answer.body).unwrap(), weft_broker_client::protocol::STOPPING);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                    break answer;
                }
            })
            .await
            .expect("a new call is answered on a running copy while the held one still runs");
            assert_eq!(moved.status, 200);
            assert!(!held.is_finished(), "the held call is still running on the old line");
            release.notify_one();
            let held = held.await.unwrap().unwrap();
            assert_eq!(String::from_utf8(held.body).unwrap(), "released", "the call it was running is answered");
            tokio::time::timeout(Duration::from_secs(5), async {
                while !first_closed.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("the stopping copy's line closes once it owes nothing");
        }
    }

    // A call rides the line with the headers it would carry as a request,
    // and one the broker holds never keeps a later call waiting.
    weft_core::stress_test! {
        name: calls_ride_the_line_and_a_held_one_holds_nothing_up,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            let release = StdArc::new(tokio::sync::Notify::new());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let _server = serve(listener, server::with_line(api(release.clone()))).await;
            let link = link(address);
            let held = tokio::spawn({
                let link = link.clone();
                async move { link.call(call("/v1/hold"), b"{}".to_vec(), CallWait::within(Duration::from_secs(10))).await }
            });
            let answer = link.call(call("/v1/echo"), b"hi".to_vec(), CallWait::within(Duration::from_secs(10))).await.unwrap();
            assert_eq!(answer.status, 200);
            assert_eq!(String::from_utf8(answer.body).unwrap(), "hi|Bearer t|w-1");
            assert!(!held.is_finished(), "the held call is still held");
            release.notify_one();
            let held = held.await.unwrap().unwrap();
            assert_eq!(String::from_utf8(held.body).unwrap(), "released");
            let missing = link.call(call("/v1/nowhere"), Vec::new(), CallWait::within(Duration::from_secs(10))).await.unwrap();
            assert_eq!(missing.status, 404);
        }
    }

    /// A call made while the broker cannot be reached goes out once it can,
    /// one that never could says it was never sent, and one whose caller
    /// stopped waiting is never sent at all.
    #[tokio::test]
    async fn a_call_waits_out_a_broker_that_is_not_up_yet() {
        // The port is held, bound and not listening, so a call to it is
        // refused until the broker comes up on it, and no other test can
        // take it meanwhile (a port let go of is anyone's).
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = socket.local_addr().unwrap();
        let link = link(address);
        let never = link.call(call("/v1/echo"), b"x".to_vec(), CallWait::within(Duration::from_millis(300))).await.unwrap_err();
        assert!(
            never.chain().any(|c| matches!(c.downcast_ref::<weft_broker_client::line::LineError>(), Some(weft_broker_client::line::LineError::NotSent { .. }))),
            "{never:#}"
        );
        let abandoned = tokio::time::timeout(Duration::from_millis(100), link.call(call("/v1/count"), Vec::new(), CallWait::within(Duration::from_secs(20)))).await;
        assert!(abandoned.is_err(), "its caller stopped waiting");
        let waiting = tokio::spawn({
            let link = link.clone();
            async move { link.call(call("/v1/echo"), b"late".to_vec(), CallWait::within(Duration::from_secs(20))).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let listener = socket.listen(1024).unwrap();
        let counted = StdArc::new(std::sync::atomic::AtomicUsize::new(0));
        let api = api(StdArc::default()).route(
            "/v1/count",
            post({
                let counted = counted.clone();
                move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    async { "counted" }
                }
            }),
        );
        let _server = serve(listener, server::with_line(api)).await;
        let answer = waiting.await.unwrap().unwrap();
        assert!(String::from_utf8(answer.body).unwrap().starts_with("late|"));
        link.call(call("/v1/count"), Vec::new(), CallWait::within(Duration::from_secs(10))).await.unwrap();
        assert_eq!(counted.load(Ordering::SeqCst), 1, "only the call still waited on reached the broker");
    }

    /// A call whose caller stopped waiting stops at the broker too: a held
    /// wait goes on for nobody no longer.
    #[tokio::test]
    async fn a_call_its_caller_gave_up_on_stops_at_the_broker() {
        struct Stopped(StdArc<tokio::sync::Notify>);
        impl Drop for Stopped {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }
        let stopped = StdArc::new(tokio::sync::Notify::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let api = api(StdArc::default()).route(
            "/v1/forever",
            post({
                let stopped = stopped.clone();
                move || {
                    let guard = Stopped(stopped.clone());
                    async move {
                        let _guard = guard;
                        std::future::pending::<&'static str>().await
                    }
                }
            }),
        );
        let _server = serve(listener, server::with_line(api)).await;
        let link = link(address);
        let wait = CallWait { to_send: Duration::from_secs(10), to_answer: Duration::from_millis(300) };
        let gave_up = link.call(call("/v1/forever"), Vec::new(), wait).await.unwrap_err();
        assert!(format!("{gave_up:#}").contains("did not answer"), "{gave_up:#}");
        tokio::time::timeout(Duration::from_secs(10), stopped.notified()).await.expect("the broker stopped the call");
        let answer = link.call(call("/v1/echo"), b"still".to_vec(), CallWait::within(Duration::from_secs(10))).await.unwrap();
        assert!(String::from_utf8(answer.body).unwrap().starts_with("still|"), "the line stays usable");
    }

    /// A line with nothing to do closes, so the broker holds no request for
    /// it, and opens again for the next call; one held open stays open.
    #[tokio::test]
    async fn an_idle_line_closes_and_opens_again_for_the_next_call() {
        use axum::extract::ws::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (opened_tx, mut opened) = mpsc::unbounded_channel::<()>();
        let (closed_tx, mut closed) = mpsc::unbounded_channel::<()>();
        let app = Router::new().route(
            LINE_PATH,
            get(move |ws: WebSocketUpgrade| {
                let (opened, closed) = (opened_tx.clone(), closed_tx.clone());
                async move {
                    ws.on_upgrade(move |mut socket| async move {
                        let _ = opened.send(());
                        while let Some(Ok(message)) = socket.recv().await {
                            if matches!(message, Message::Close(_)) {
                                break;
                            }
                        }
                        let _ = closed.send(());
                    })
                }
            }),
        );
        let _server = serve(listener, app).await;
        let link = link(address).closing_when_idle_after(Duration::from_millis(300));
        let held = link.stay_open();
        tokio::time::timeout(Duration::from_secs(10), opened.recv()).await.expect("held open, the line opened").unwrap();
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(closed.try_recv().is_err(), "a line held open stays open");
        drop(held);
        tokio::time::timeout(Duration::from_secs(10), closed.recv()).await.expect("with nothing to do, it closed").unwrap();
        let _woken = link.stay_open();
        tokio::time::timeout(Duration::from_secs(10), opened.recv()).await.expect("asked for again, it opened").unwrap();
    }

    /// A process that lets go of its last link closes its line: nothing
    /// stays open at the broker for a link nobody holds.
    #[tokio::test]
    async fn a_line_closes_with_its_last_link() {
        use axum::extract::ws::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (opened_tx, opened) = tokio::sync::oneshot::channel::<()>();
        let (closed_tx, closed) = tokio::sync::oneshot::channel::<()>();
        let sides = StdArc::new(std::sync::Mutex::new(Some((opened_tx, closed_tx))));
        let app = Router::new().route(
            LINE_PATH,
            get(move |ws: WebSocketUpgrade| {
                let sides = sides.clone();
                async move {
                    ws.on_upgrade(move |mut socket| async move {
                        let Some((opened, closed)) = sides.lock().unwrap().take() else { return };
                        let _ = opened.send(());
                        while let Some(Ok(message)) = socket.recv().await {
                            if matches!(message, Message::Close(_)) {
                                break;
                            }
                        }
                        let _ = closed.send(());
                    })
                }
            }),
        );
        let _server = serve(listener, app).await;
        let link = link(address);
        let _heard = link.subscribe();
        tokio::time::timeout(Duration::from_secs(10), opened).await.expect("the line opened").unwrap();
        drop(link);
        tokio::time::timeout(Duration::from_secs(10), closed).await.expect("the line closed").unwrap();
    }

    // What the broker pushes reaches a subscription, which is told when the
    // line breaks (so a copy kept on it stops trusting itself) and again
    // when the line is back.
    weft_core::stress_test! {
        name: notices_reach_the_line_and_a_break_is_heard,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            use axum::extract::ws::Message;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            // A broker that answers the follow with one notice, then hangs up.
            let app = Router::new().route(
                LINE_PATH,
                get(|ws: WebSocketUpgrade| async move {
                    ws.on_upgrade(|mut socket| async move {
                        let _follow = socket.recv().await;
                        for notice in [Notice::Following { listening: true, max_age_secs: None }, Notice::Signal { channel: INFRA_STATUS_CHANNEL.into(), payload: "p".into() }] {
                            let text = serde_json::to_string(&notice).unwrap();
                            let _ = socket.send(Message::Text(text.into())).await;
                        }
                    })
                }),
            );
            let _server = serve(listener, app).await;
            // The link is held for the whole test: a line nobody holds closes.
            let link = link(address);
            let mut heard = link.subscribe();
            async fn next(heard: &mut weft_task_store::pg_signal::Subscription) -> Heard {
                tokio::time::timeout(Duration::from_secs(10), heard.next()).await.expect("heard in time").expect("the line is up")
            }
            assert_eq!(next(&mut heard).await, Heard::Recheck);
            assert_eq!(next(&mut heard).await, Heard::Signal { channel: INFRA_STATUS_CHANNEL, payload: "p".into() });
            assert_eq!(next(&mut heard).await, Heard::Lost, "the broker hung up");
            assert!(!heard.listening());
            assert_eq!(next(&mut heard).await, Heard::Recheck, "the line came back by itself");
        }
    }

    // A line that opens on a copy whose database listener does not hear
    // (it went quiet while the database sleeps) still looks again (a
    // cancel announced while the line was down is found so) but trusts
    // nothing kept on it; a recheck from the install's news does the same.
    weft_core::stress_test! {
        name: a_line_opening_on_a_quiet_copy_looks_again_and_trusts_nothing,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            use axum::extract::ws::Message;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let app = Router::new().route(
                LINE_PATH,
                get(|ws: WebSocketUpgrade| async move {
                    ws.on_upgrade(|mut socket| async move {
                        let _follow = socket.recv().await;
                        for notice in [Notice::Following { listening: false, max_age_secs: None }, Notice::Recheck { listening: false }] {
                            let text = serde_json::to_string(&notice).unwrap();
                            let _ = socket.send(Message::Text(text.into())).await;
                        }
                        std::future::pending::<()>().await;
                    })
                }),
            );
            let _server = serve(listener, app).await;
            let link = link(address);
            let mut heard = link.subscribe();
            async fn next(heard: &mut weft_task_store::pg_signal::Subscription) -> (Heard, bool) {
                let heard_now = tokio::time::timeout(Duration::from_secs(10), heard.next()).await.expect("heard in time").expect("the line is up");
                (heard_now, heard.listening())
            }
            assert_eq!(next(&mut heard).await, (Heard::Recheck, false), "opened: look again, trust nothing");
            assert_eq!(next(&mut heard).await, (Heard::Recheck, false), "news missed: look again, trust nothing");
        }
    }

    /// A recheck reaches every line whether or not the database's listener
    /// hears, saying which; the listener hearing again after a quiet spell
    /// is said as such (nothing to look at again); a lost listener is said
    /// as lost.
    #[tokio::test]
    async fn a_recheck_reaches_every_line_whatever_the_listener() {
        let (tx, rx) = tokio::sync::broadcast::channel(8);
        let listening = StdArc::new(AtomicBool::new(false));
        let fanout = LineFanout::start(weft_task_store::pg_signal::Subscription::with_listening(rx, listening.clone()), None);
        let (line, mut heard) = mpsc::channel(8);
        let _following = fanout.follow("t", "p", line);
        async fn next(heard: &mut mpsc::Receiver<Notice>) -> Notice {
            tokio::time::timeout(Duration::from_secs(10), heard.recv()).await.expect("told in time").unwrap()
        }
        tx.send(Heard::Recheck).unwrap();
        assert_eq!(next(&mut heard).await, Notice::Recheck { listening: false });
        listening.store(true, Ordering::SeqCst);
        tx.send(Heard::Resumed).unwrap();
        assert_eq!(next(&mut heard).await, Notice::Resumed);
        tx.send(Heard::Lost).unwrap();
        assert_eq!(next(&mut heard).await, Notice::Lost);
    }

    /// A line the broker keeps for a while tells its process when the cut
    /// comes, counted from before the line opened, and stops saying so once
    /// that line is gone.
    #[tokio::test]
    async fn a_line_says_when_the_broker_cuts_it() {
        use axum::extract::ws::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let hang_up = StdArc::new(tokio::sync::Notify::new());
        let told = StdArc::new(AtomicBool::new(false));
        let app = Router::new().route(
            LINE_PATH,
            get({
                let hang_up = hang_up.clone();
                move |ws: WebSocketUpgrade| {
                    let (hang_up, told) = (hang_up.clone(), told.clone());
                    async move {
                        ws.on_upgrade(move |mut socket| async move {
                            // The line opened again after the hang-up is
                            // never followed: what it said stays cleared.
                            let _follow = socket.recv().await;
                            if !told.swap(true, Ordering::SeqCst) {
                                let text = serde_json::to_string(&Notice::Following { listening: true, max_age_secs: Some(3600) }).unwrap();
                                let _ = socket.send(Message::Text(text.into())).await;
                            }
                            hang_up.notified().await;
                        })
                    }
                }
            }),
        );
        let _server = serve(listener, app).await;
        let link = link(address);
        let mut cut = link.cut_at();
        let before = tokio::time::Instant::now();
        let _held = link.stay_open();
        tokio::time::timeout(Duration::from_secs(10), cut.wait_for(Option::is_some)).await.expect("told in time").unwrap();
        let told = cut.borrow_and_update().expect("a cut");
        assert_eq!(told.max_age, Duration::from_secs(3600));
        assert!(told.opened >= before && told.opened <= tokio::time::Instant::now(), "counted from the opening");
        hang_up.notify_one();
        tokio::time::timeout(Duration::from_secs(10), cut.wait_for(Option::is_none)).await.expect("cleared in time").unwrap();
    }

    #[test]
    fn a_notice_is_for_the_project_or_the_tenant_it_names() {
        let project = uuid::Uuid::from_u128(1).to_string();
        assert_eq!(audience(INFRA_STATUS_CHANNEL, &project), Some(Audience::Project(&project)));
        assert_eq!(audience(ACCESS_CHANNEL, &project), Some(Audience::Project(&project)));
        assert_eq!(audience(ACCESS_CHANNEL, "tenant:t"), Some(Audience::Tenant("t")));
        let cancel = format!("{project} e-1");
        assert_eq!(audience(CANCEL_CHANNEL, &cancel), Some(Audience::Project(&project)));
        assert_eq!(audience(PARKED_FIRE_CHANNEL, &cancel), Some(Audience::Project(&project)), "an answer handed to a run");
        assert_eq!(audience(PARKED_FIRE_CHANNEL, &project), None, "an event parked for a trigger rides no line");
        assert_eq!(audience("weft_task_ready", "worker:x"), None);
    }

    /// A notice reaches the lines of the project or tenant it names and no
    /// other, and a line that cannot keep up is let go of.
    #[tokio::test]
    async fn the_fanout_hands_a_notice_to_the_lines_it_names() {
        let fanout = StdArc::new(LineFanout { lines: Mutex::default(), listening: AtomicBool::new(true), next: AtomicU64::new(1), max_age: None });
        let (mine, mut mine_heard) = mpsc::channel(4);
        let (other, mut other_heard) = mpsc::channel(4);
        let (slow, _slow_heard) = mpsc::channel(1);
        let _mine = fanout.follow("t", "p1", mine);
        let _other = fanout.follow("u", "p2", other);
        let _slow = fanout.follow("t", "p3", slow);
        let signal = |payload: &str| Notice::Signal { channel: ACCESS_CHANNEL.into(), payload: payload.into() };
        fanout.hand(Audience::Project("p1"), signal("p1"));
        fanout.hand(Audience::Tenant("t"), signal("tenant:t"));
        assert_eq!(mine_heard.try_recv().unwrap(), signal("p1"));
        assert_eq!(mine_heard.try_recv().unwrap(), signal("tenant:t"));
        assert!(other_heard.try_recv().is_err(), "another project and tenant hear nothing");
        fanout.hand(Audience::Tenant("t"), signal("tenant:t"));
        assert!(!fanout.lines.lock().unwrap().by_project.contains_key("p3"), "the full line was let go of");
        assert!(fanout.lines.lock().unwrap().by_project.contains_key("p1"));
        drop(_mine);
        assert!(!fanout.lines.lock().unwrap().by_tenant.contains_key("t"), "a line that goes gives up its place");
    }
}
