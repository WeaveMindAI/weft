//! The worker side of provider WebSocket sessions: the dialer behind
//! `conn.socket(url)`.
//!
//! The socket twin of `metering::connection_client`, one dial per session:
//!
//!   1. route the session (straight to the service, or to the runtime's
//!      relay when the resolved credential carries one),
//!   2. sign the HANDSHAKE with the connection's resolved auth steps (the
//!      credential rides the handshake only, never a frame),
//!   3. on a direct session route with a registered meter, run the meter's
//!      session observation over every frame BOTH directions, and when the
//!      session ends (cleanly or cut, including by drop) record the
//!      accrued figure durably on the execution's cost trail.
//!
//! A relayed session is not measured here: the relay is where the
//! runtime's own measuring happens (same rule as HTTP). A worker-side
//! figure is a MEASUREMENT, never a charge.

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use weft_core::access::client::AppliedStep;
use weft_core::access::socket::{ProviderSocket, SocketDial, SocketMessage, SocketTransport};
use weft_core::error::{WeftError, WeftResult};
use weft_providers::{ProviderMeter, RouteClass, SessionObservation};

use crate::metering::CostSink;

/// How long a clean close waits for the peer's trailing frames before
/// booking the figure anyway. Cleanup only: the session itself carries
/// no deadline, but a peer that never answers the close must not hang
/// the node that already finished.
const CLOSE_DRAIN_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);

/// The connection's dialer, assembled at `ctx.open` next to its HTTP
/// client; holds everything a dial needs so the node-facing call is
/// just the URL.
pub struct ConnectionSocketDial {
    steps: Vec<AppliedStep>,
    relay_url: Option<String>,
    /// The service's meter, resolved once at assembly (tests inject one
    /// directly, same pattern as the HTTP middleware's rig).
    meter: Option<&'static dyn ProviderMeter>,
    sink: Arc<CostSink>,
}

impl ConnectionSocketDial {
    pub fn new(steps: Vec<AppliedStep>, relay_url: Option<String>, sink: Arc<CostSink>) -> Self {
        let meter = weft_providers::meter_for(&sink.service);
        Self { steps, relay_url, meter, sink }
    }
}

fn dial_err(service: &str, msg: impl std::fmt::Display) -> WeftError {
    WeftError::NodeExecution(format!("open a '{service}' socket: {msg}"))
}

/// The session URL in its HTTPS spelling, for route classification
/// against the meter's base (meters declare their base as https).
fn https_form(url: &url::Url) -> String {
    let mut s = url.to_string();
    if let Some(rest) = s.strip_prefix("wss://") {
        s = format!("https://{rest}");
    } else if let Some(rest) = s.strip_prefix("ws://") {
        s = format!("http://{rest}");
    }
    s
}

/// Swap an http(s) scheme for its WebSocket twin; ws(s) passes through.
fn ws_scheme(scheme: &str) -> WeftResult<&'static str> {
    match scheme {
        "https" | "wss" => Ok("wss"),
        "http" | "ws" => Ok("ws"),
        other => Err(WeftError::NodeExecution(format!(
            "'{other}' is not a scheme a socket can dial"
        ))),
    }
}

/// Apply the connection's resolved auth steps to a session URL,
/// producing the final URL (ws spelling) and the headers to stamp on
/// the handshake request. The step application is the same one every
/// HTTP request rides (`access::client::apply_plain_step`), so
/// headers, query, basic, path-prefix, and stored-base rewrites all
/// behave identically here. Schemes that sign each request's bytes
/// (SigV4, OAuth1a) have no handshake form and refuse loudly.
fn handshake_parts(
    service: &str,
    steps: &[AppliedStep],
    mut url: url::Url,
) -> WeftResult<(url::Url, reqwest::header::HeaderMap)> {
    if steps
        .iter()
        .any(|s| matches!(s, AppliedStep::SigV4 { .. } | AppliedStep::OAuth1a { .. }))
    {
        return Err(dial_err(
            service,
            "this service's auth signs each request's bytes, which a socket handshake \
             cannot carry; use the HTTP client instead",
        ));
    }
    let mut headers = reqwest::header::HeaderMap::new();
    for step in steps {
        weft_core::access::client::apply_plain_step(&mut url, &mut headers, step)
            .map_err(|e| dial_err(service, e))?;
    }
    // A stored base rewrites in the https spelling; dial its socket
    // twin.
    let scheme = ws_scheme(url.scheme())?;
    url.set_scheme(scheme)
        .map_err(|_| dial_err(service, "URL scheme rewrite refused"))?;
    Ok((url, headers))
}

#[async_trait::async_trait]
impl SocketDial for ConnectionSocketDial {
    async fn dial(&self, url: &str) -> WeftResult<ProviderSocket> {
        let service = &self.sink.service;
        let parsed: url::Url =
            url.parse().map_err(|e| dial_err(service, format!("bad URL '{url}': {e}")))?;
        let meter = self.meter;

        // Route the session, mirroring the HTTP lanes exactly.
        let (target, observation) = match &self.relay_url {
            Some(relay) => {
                // Relayed lane: rebuild against the relay; the relay does
                // the measuring. Only the meter knows the provider's base
                // to strip, so a relayed session requires one (the resolve
                // that handed out a relay guaranteed it).
                let Some(meter) = meter else {
                    return Err(dial_err(
                        service,
                        "the runtime relays this connection's sessions, but no meter is \
                         registered to route them by; connect your own credential on the node",
                    ));
                };
                let https = https_form(&parsed);
                let Some(route) = weft_providers::route_under(meter.base_url(), &https) else {
                    return Err(dial_err(
                        service,
                        format!(
                            "this session ({url}) is not under the service's API ({}); \
                             sessions on a runtime-supplied credential must address the \
                             service's own API",
                            meter.base_url(),
                        ),
                    ));
                };
                let relay: url::Url = relay
                    .parse()
                    .map_err(|e| dial_err(service, format!("bad relay URL: {e}")))?;
                let scheme = ws_scheme(relay.scheme()).map_err(|e| dial_err(service, e))?;
                let query = parsed.query().map(|q| format!("?{q}")).unwrap_or_default();
                let target = format!(
                    "{scheme}://{}{}/{route}{query}",
                    relay.authority(),
                    relay.path().trim_end_matches('/'),
                );
                let target: url::Url =
                    target.parse().map_err(|e| dial_err(service, format!("relay URL: {e}")))?;
                (target, None)
            }
            None => {
                // Direct lane: dial the URL the node wrote (ws spelling),
                // measuring when the meter prices this route as a session.
                let mut target = parsed.clone();
                let scheme = ws_scheme(target.scheme()).map_err(|e| dial_err(service, e))?;
                target
                    .set_scheme(scheme)
                    .map_err(|_| dial_err(service, "URL scheme rewrite refused"))?;
                let observation = match meter {
                    Some(meter) => {
                        let https = https_form(&parsed);
                        match weft_providers::route_under(meter.base_url(), &https)
                            .map(|route| (route.to_string(), meter))
                        {
                            Some((route, meter))
                                if meter.classify("GET", &route)
                                    == RouteClass::BillableSession =>
                            {
                                let observer = meter
                                    .observe_session(&route, parsed.query().unwrap_or(""))
                                    .map_err(|e| dial_err(service, format!("{e:#}")))?;
                                Some(observer)
                            }
                            // Not a session route (or not under the API):
                            // pass through unmeasured, exactly like an
                            // unknown HTTP route on a key the user holds.
                            _ => None,
                        }
                    }
                    None => None,
                };
                (target, observation)
            }
        };

        let (target, headers) = handshake_parts(service, &self.steps, target)?;
        let mut request = target
            .as_str()
            .into_client_request()
            .map_err(|e| dial_err(service, format!("handshake build: {e}")))?;
        request.headers_mut().extend(headers);
        // TLS rides the runtime's ONE client config (system trust roots,
        // explicit ring provider; `weft_core::net::tls_config`), the same
        // setup the raw-pipe engine dials with: tungstenite's own default
        // would guess the process crypto provider, which is ambiguous in
        // this dependency graph and panics.
        let connector = match target.scheme() {
            "wss" => Some(tokio_tungstenite::Connector::Rustls(
                weft_core::net::tls_config().map_err(|e| dial_err(service, e))?,
            )),
            _ => None,
        };
        let (stream, _) =
            tokio_tungstenite::connect_async_tls_with_config(request, None, false, connector)
                .await
                .map_err(|e| dial_err(service, format!("the session was refused: {e}")))?;

        Ok(ProviderSocket::new(Box::new(MeteredSocket {
            stream,
            finalizer: observation
                .map(|observer| SessionFinalizer { observer, sink: self.sink.clone() }),
        })))
    }
}

/// The live session: tungstenite underneath, the meter's session
/// observation tapping every data frame both ways, the figure booked
/// exactly once when the session ends (close, error, or drop).
struct MeteredSocket {
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    finalizer: Option<SessionFinalizer>,
}

/// Books the session's accrued figure, exactly once.
struct SessionFinalizer {
    observer: Box<dyn SessionObservation>,
    sink: Arc<CostSink>,
}

impl SessionFinalizer {
    /// End the observation and book its figure through
    /// [`CostSink::book`] (same discipline as the HTTP finalizer; a
    /// session's figure needs no follow-up resolve, its own frames are
    /// the whole measurement, so the cost is handed over ready).
    fn finish(self, interrupted: bool) {
        let cost = self.observer.end(interrupted);
        self.sink.book(std::future::ready(cost));
    }
}

impl MeteredSocket {
    fn tap_out(&mut self, payload: &[u8]) {
        if let Some(f) = self.finalizer.as_mut() {
            f.observer.on_frame_to_provider(payload);
        }
    }

    fn tap_in(&mut self, payload: &[u8]) {
        if let Some(f) = self.finalizer.as_mut() {
            f.observer.on_frame_to_caller(payload);
        }
    }

    fn finish(&mut self, interrupted: bool) {
        if let Some(f) = self.finalizer.take() {
            f.finish(interrupted);
        }
    }
}

#[async_trait::async_trait]
impl SocketTransport for MeteredSocket {
    async fn send(&mut self, msg: SocketMessage) -> WeftResult<()> {
        let frame = match &msg {
            SocketMessage::Text(s) => Message::Text(s.clone()),
            SocketMessage::Binary(b) => Message::Binary(b.clone()),
        };
        // The tap runs only after the write lands: a frame that never
        // reached the provider must not accrue.
        match self.stream.send(frame).await {
            Ok(()) => {
                self.tap_out(msg.as_bytes());
                Ok(())
            }
            Err(e) => {
                self.finish(true);
                Err(WeftError::NodeExecution(format!("the session dropped mid-send: {e}")))
            }
        }
    }

    async fn recv(&mut self) -> WeftResult<Option<SocketMessage>> {
        loop {
            match self.stream.next().await {
                Some(Ok(Message::Text(s))) => {
                    self.tap_in(s.as_bytes());
                    return Ok(Some(SocketMessage::Text(s)));
                }
                Some(Ok(Message::Binary(b))) => {
                    self.tap_in(&b);
                    return Ok(Some(SocketMessage::Binary(b)));
                }
                // Control frames are transport plumbing (tungstenite
                // already answered the ping); keep reading.
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                Some(Ok(Message::Close(frame))) => {
                    self.finish(false);
                    // A runtime-cut session says why in the close frame;
                    // surface that as the error the node fails on rather
                    // than a silent end.
                    if let Some(frame) = frame {
                        if !frame.reason.is_empty()
                            && frame.code
                                != tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal
                        {
                            return Err(WeftError::NodeExecution(format!(
                                "the session was closed: {}",
                                frame.reason
                            )));
                        }
                    }
                    return Ok(None);
                }
                Some(Err(e)) => {
                    self.finish(true);
                    return Err(WeftError::NodeExecution(format!(
                        "the session dropped: {e}"
                    )));
                }
                None => {
                    self.finish(false);
                    return Ok(None);
                }
            }
        }
    }

    async fn close(&mut self) -> WeftResult<()> {
        let _ = self.stream.close(None).await;
        // Drain what the peer sends around the close, feeding the
        // observation: frames in flight are part of the session and the
        // tap covers both directions until the end. The drain is BOUNDED:
        // this is cleanup after the node already finished, and a peer
        // that never answers the close must not hang the node; trailing
        // frames past the bound are a rounding detail on the figure,
        // while an unbounded wait would also never book it.
        let drain = async {
            loop {
                match self.stream.next().await {
                    Some(Ok(Message::Text(s))) => self.tap_in(s.as_bytes()),
                    Some(Ok(Message::Binary(b))) => self.tap_in(&b),
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => continue,
                }
            }
        };
        let _ = tokio::time::timeout(CLOSE_DRAIN_WINDOW, drain).await;
        self.finish(false);
        Ok(())
    }
}

impl Drop for MeteredSocket {
    fn drop(&mut self) {
        self.finish(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use weft_providers::MeasuredCost;

    fn step_url(steps: &[AppliedStep], url: &str) -> (String, reqwest::header::HeaderMap) {
        let (u, h) = handshake_parts("svc", steps, url.parse().unwrap()).unwrap();
        (u.to_string(), h)
    }

    #[test]
    fn the_handshake_carries_every_applicable_auth_shape() {
        let (u, h) = step_url(
            &[
                AppliedStep::Header { name: "xi-api-key".into(), value: "k".into() },
                AppliedStep::Query { name: "token".into(), value: "t".into() },
                AppliedStep::Basic { username: "u".into(), password: "p".into() },
                AppliedStep::PathPrefix { value: "/bot123".into() },
            ],
            "wss://api.example/v1/realtime?model=m",
        );
        assert_eq!(u, "wss://api.example/bot123/v1/realtime?model=m&token=t");
        assert_eq!(h["xi-api-key"], "k");
        assert!(h["authorization"].to_str().unwrap().starts_with("Basic "));

        // A stored base re-aims the session and the dial stays a
        // socket: the base's https spelling maps back to wss.
        let (u, _) = step_url(
            &[AppliedStep::BaseUrl { value: "https://tenant.example:9443/rt".into() }],
            "wss://api.example/v1/realtime?model=m",
        );
        assert_eq!(u, "wss://tenant.example:9443/rt/v1/realtime?model=m");
    }

    #[test]
    fn request_signing_schemes_refuse_a_handshake() {
        let err = handshake_parts(
            "svc",
            &[AppliedStep::SigV4 {
                service: "s3".into(),
                region: "r".into(),
                access_key_id: "a".into(),
                secret_access_key: "s".into(),
            }],
            "wss://api.example/v1".parse().unwrap(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("handshake cannot carry"), "{err}");
    }

    // ---- Rig: a recording task store, a byte-priced session meter, an
    // echo WS server ----

    #[derive(Default)]
    struct RecordingTaskStore {
        pub enqueued: Mutex<Vec<weft_task_store::NewTask>>,
    }

    #[async_trait::async_trait]
    impl weft_task_store::TaskStoreClient for RecordingTaskStore {
        async fn enqueue_dedup(
            &self,
            spec: weft_task_store::tasks::NewTask,
        ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            self.enqueued.lock().unwrap().push(spec);
            Ok(weft_task_store::tasks::DedupOutcome::Inserted(uuid::Uuid::new_v4()))
        }
        async fn wait_for_terminal(
            &self,
            _task_id: uuid::Uuid,
            _timeout: std::time::Duration,
            _poll_interval: std::time::Duration,
        ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!("socket tests only enqueue")
        }
        async fn claim_one(
            &self,
            _pod_id: &str,
            _filter: weft_task_store::tasks::ClaimFilter,
        ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
            Ok(None)
        }
        async fn heartbeat(&self, _task_id: uuid::Uuid, _pod_id: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _result: serde_json::Value,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _error: String,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// A meter whose session prices a dollar per byte SENT to the
    /// provider (and, when `inbound` is set, per byte received too).
    /// The dumbest possible session math, so the tests pin the
    /// plumbing, not the arithmetic.
    struct ByteMeter {
        base: &'static str,
        inbound: bool,
    }

    struct ByteSession {
        inbound: bool,
        sent: u64,
    }

    impl weft_providers::SessionObservation for ByteSession {
        fn on_frame_to_provider(&mut self, payload: &[u8]) {
            self.sent += payload.len() as u64;
        }
        fn on_frame_to_caller(&mut self, payload: &[u8]) {
            if self.inbound {
                self.sent += payload.len() as u64;
            }
        }
        fn accrued_usd(&self) -> f64 {
            self.sent as f64
        }
        fn end(self: Box<Self>, interrupted: bool) -> MeasuredCost {
            MeasuredCost {
                amount_usd: Some(self.sent as f64),
                model: Some("byte-model".into()),
                metadata: serde_json::json!({ "interrupted": interrupted }),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProviderMeter for ByteMeter {
        fn service(&self) -> &'static str {
            "bytesvc"
        }
        fn base_url(&self) -> &'static str {
            self.base
        }
        fn classify(&self, _method: &str, path: &str) -> RouteClass {
            match path {
                "live" => RouteClass::BillableSession,
                _ => RouteClass::Unknown,
            }
        }
        fn prepare(&self, _path: &str, _body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn observe(&self) -> Box<dyn weft_providers::CallObservation> {
            unreachable!("no one-shot routes")
        }
        fn observe_session(
            &self,
            _path: &str,
            _query: &str,
        ) -> anyhow::Result<Box<dyn SessionObservation>> {
            Ok(Box::new(ByteSession { inbound: self.inbound, sent: 0 }))
        }
        async fn resolve(
            &self,
            _observed: weft_providers::ObservedCall,
            _follow_up: weft_providers::FollowUp<'_>,
        ) -> MeasuredCost {
            unreachable!("no one-shot routes")
        }
    }

    /// A WS server that records the handshake and echoes every text
    /// frame back prefixed with "echo:". Serves ONE connection.
    async fn spawn_echo_ws() -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_srv = seen.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                stream,
                |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp| {
                    let auth = req
                        .headers()
                        .get("x-test-key")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    seen_srv.lock().unwrap().push(format!("handshake-key={auth}"));
                    seen_srv.lock().unwrap().push(format!("path={}", req.uri()));
                    Ok(resp)
                },
            )
            .await
            .unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                if let Message::Text(t) = msg {
                    seen_srv.lock().unwrap().push(format!("frame={t}"));
                    ws.send(Message::Text(format!("echo:{t}").into())).await.unwrap();
                }
            }
        });
        (format!("http://{addr}"), seen)
    }

    fn sink(
        tasks: Arc<RecordingTaskStore>,
        pending: Arc<crate::metering::PendingCostRecords>,
    ) -> Arc<CostSink> {
        Arc::new(CostSink {
            tasks,
            pending,
            project_id: "p1".into(),
            tenant_id: "t1".into(),
            color: uuid::Uuid::nil(),
            node_id: "node-x".into(),
            frames: weft_core::frames::LoopFrames::default(),
            service: "bytesvc".into(),
            origin: weft_core::CredentialOwner::TheirOwn,
        })
    }

    /// L3, the whole worker lane: the dialer signs the handshake with the
    /// connection's auth step, the meter's session observation taps the
    /// frames, and closing the socket books the accrued figure (billed:
    /// false, their-own) on the cost trail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_session_is_signed_measured_and_booked_on_close() {
        let (base, seen) = spawn_echo_ws().await;
        let base: &'static str = Box::leak(base.into_boxed_str());
        let meter: &'static ByteMeter = Box::leak(Box::new(ByteMeter { base, inbound: false }));

        let steps = weft_core::access::client::resolve_steps(
            &[weft_core::access::spec::AuthStep::Header {
                name: "x-test-key".into(),
                value: weft_core::access::spec::Template::new("{key}"),
            }],
            &[("key".to_string(), "sekrit".to_string())].into_iter().collect(),
        )
        .unwrap();
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = crate::metering::PendingCostRecords::new();
        let dial = ConnectionSocketDial {
            steps,
            relay_url: None,
            meter: Some(meter),
            sink: sink(tasks.clone(), pending.clone()),
        };

        let mut socket = dial.dial(&format!("{base}/live?mode=fast")).await.expect("dials");
        socket.send(SocketMessage::Text("hello".into())).await.unwrap();
        let echo = socket.recv().await.unwrap().expect("echo comes back");
        assert_eq!(echo, SocketMessage::Text("echo:hello".into()));
        socket.send(SocketMessage::Text("more!".into())).await.unwrap();
        socket.recv().await.unwrap();
        socket.close().await.unwrap();
        pending.wait_zero().await;

        // The handshake carried the resolved credential and the query.
        let seen = seen.lock().unwrap().clone();
        assert!(seen.contains(&"handshake-key=sekrit".to_string()), "{seen:?}");
        assert!(seen.iter().any(|s| s.contains("/live?mode=fast")), "{seen:?}");

        // The figure: 10 bytes sent ("hello" + "more!"), measured,
        // never billed, their-own.
        let enqueued = tasks.enqueued.lock().unwrap();
        assert_eq!(enqueued.len(), 1, "one session, one record");
        let payload = &enqueued[0].payload;
        assert_eq!(payload["amount_usd"], serde_json::json!(10.0));
        assert_eq!(payload["billed"], serde_json::json!(false));
        assert_eq!(payload["origin"], serde_json::json!("their-own"));
        assert_eq!(payload["model"], serde_json::json!("byte-model"));
        assert_eq!(payload["metadata"]["interrupted"], serde_json::json!(false));
    }

    /// A session on a route the meter does not price passes through
    /// unmeasured (own key), exactly like an unknown HTTP route; a
    /// DROPPED measured session still books its figure (interrupted).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_routes_pass_unmeasured_and_drops_still_book() {
        // Unmeasured lane.
        {
            let (base, _seen) = spawn_echo_ws().await;
            let base: &'static str = Box::leak(base.into_boxed_str());
            let meter: &'static ByteMeter = Box::leak(Box::new(ByteMeter { base, inbound: false }));
            let tasks = Arc::new(RecordingTaskStore::default());
            let pending = crate::metering::PendingCostRecords::new();
            let dial = ConnectionSocketDial {
                steps: Vec::new(),
                relay_url: None,
                meter: Some(meter),
                sink: sink(tasks.clone(), pending.clone()),
            };
            let mut socket = dial.dial(&format!("{base}/other")).await.expect("dials");
            socket.send(SocketMessage::Text("x".into())).await.unwrap();
            socket.close().await.unwrap();
            pending.wait_zero().await;
            assert!(tasks.enqueued.lock().unwrap().is_empty(), "unknown route = no record");
        }
        // Dropped measured session.
        {
            let (base, _seen) = spawn_echo_ws().await;
            let base: &'static str = Box::leak(base.into_boxed_str());
            let meter: &'static ByteMeter = Box::leak(Box::new(ByteMeter { base, inbound: false }));
            let tasks = Arc::new(RecordingTaskStore::default());
            let pending = crate::metering::PendingCostRecords::new();
            let dial = ConnectionSocketDial {
                steps: Vec::new(),
                relay_url: None,
                meter: Some(meter),
                sink: sink(tasks.clone(), pending.clone()),
            };
            let mut socket = dial.dial(&format!("{base}/live")).await.expect("dials");
            socket.send(SocketMessage::Text("abc".into())).await.unwrap();
            drop(socket);
            pending.wait_zero().await;
            let enqueued = tasks.enqueued.lock().unwrap();
            assert_eq!(enqueued.len(), 1);
            assert_eq!(enqueued[0].payload["amount_usd"], serde_json::json!(3.0));
            assert_eq!(
                enqueued[0].payload["metadata"]["interrupted"],
                serde_json::json!(true)
            );
        }
    }

    /// L3, a failed send accrues nothing: the peer is gone before the
    /// write, so the frames that never landed are absent from the
    /// booked figure (only the writes that succeeded count).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_send_is_not_in_the_booked_figure() {
        // A server that completes the handshake and then drops the
        // connection without a word.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            drop(ws);
        });
        let base: &'static str = Box::leak(format!("http://{addr}").into_boxed_str());
        let meter: &'static ByteMeter = Box::leak(Box::new(ByteMeter { base, inbound: false }));
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = crate::metering::PendingCostRecords::new();
        let dial = ConnectionSocketDial {
            steps: Vec::new(),
            relay_url: None,
            meter: Some(meter),
            sink: sink(tasks.clone(), pending.clone()),
        };
        let mut socket = dial.dial(&format!("{base}/live")).await.expect("dials");
        // Sends land in the write buffer until the dead peer surfaces;
        // count only the ones the transport accepted.
        let mut accepted = 0u64;
        let mut failed = false;
        for _ in 0..1000 {
            match socket.send(SocketMessage::Text("abc".into())).await {
                Ok(()) => accepted += 3,
                Err(_) => {
                    failed = true;
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(failed, "the dead peer must surface as a send error");
        drop(socket);
        pending.wait_zero().await;
        let enqueued = tasks.enqueued.lock().unwrap();
        assert_eq!(enqueued.len(), 1);
        assert_eq!(
            enqueued[0].payload["amount_usd"],
            serde_json::json!(accepted as f64),
            "the failed send's bytes are absent from the figure"
        );
    }

    /// L3, close drains the tail: a frame the provider sends around the
    /// close is still fed to the observation, so an inbound-priced
    /// session's figure includes it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_drains_inbound_frames_into_the_figure() {
        // A server that answers one request frame with "payload" and
        // then closes its side.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            if let Some(Ok(Message::Text(_))) = ws.next().await {
                ws.send(Message::Text("payload".into())).await.unwrap();
            }
            let _ = ws.close(None).await;
            while ws.next().await.is_some() {}
        });
        let base: &'static str = Box::leak(format!("http://{addr}").into_boxed_str());
        let meter: &'static ByteMeter = Box::leak(Box::new(ByteMeter { base, inbound: true }));
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = crate::metering::PendingCostRecords::new();
        let dial = ConnectionSocketDial {
            steps: Vec::new(),
            relay_url: None,
            meter: Some(meter),
            sink: sink(tasks.clone(), pending.clone()),
        };
        let mut socket = dial.dial(&format!("{base}/live")).await.expect("dials");
        socket.send(SocketMessage::Text("go".into())).await.unwrap();
        // Close without ever reading: the drain inside close() must
        // still feed "payload" to the observation.
        socket.close().await.unwrap();
        pending.wait_zero().await;
        let enqueued = tasks.enqueued.lock().unwrap();
        assert_eq!(enqueued.len(), 1);
        // 2 bytes out ("go") + 7 in ("payload").
        assert_eq!(enqueued[0].payload["amount_usd"], serde_json::json!(9.0));
        assert_eq!(enqueued[0].payload["metadata"]["interrupted"], serde_json::json!(false));
    }

    /// A peer that never answers the close (holds the line, sends
    /// nothing) must not hang the node: the drain is bounded, close
    /// returns, and the figure books as a clean end.
    #[tokio::test(start_paused = true)]
    async fn close_returns_even_when_the_peer_goes_silent() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Read forever, answer nothing, never close.
            while ws.next().await.is_some() {}
        });
        let base: &'static str = Box::leak(format!("http://{addr}").into_boxed_str());
        let meter: &'static ByteMeter = Box::leak(Box::new(ByteMeter { base, inbound: false }));
        let tasks = Arc::new(RecordingTaskStore::default());
        let pending = crate::metering::PendingCostRecords::new();
        let dial = ConnectionSocketDial {
            steps: Vec::new(),
            relay_url: None,
            meter: Some(meter),
            sink: sink(tasks.clone(), pending.clone()),
        };
        let mut socket = dial.dial(&format!("{base}/live")).await.expect("dials");
        socket.send(SocketMessage::Text("go".into())).await.unwrap();
        // With the clock paused, the bounded drain's timeout fires as
        // soon as the silent peer yields no progress; an unbounded
        // drain would hang this await forever.
        socket.close().await.unwrap();
        pending.wait_zero().await;
        let enqueued = tasks.enqueued.lock().unwrap();
        assert_eq!(enqueued.len(), 1);
        assert_eq!(enqueued[0].payload["amount_usd"], serde_json::json!(2.0));
        assert_eq!(enqueued[0].payload["metadata"]["interrupted"], serde_json::json!(false));
    }

    #[test]
    fn url_schemes_map_to_their_socket_twins() {
        assert_eq!(ws_scheme("https").unwrap(), "wss");
        assert_eq!(ws_scheme("http").unwrap(), "ws");
        assert_eq!(ws_scheme("wss").unwrap(), "wss");
        assert!(ws_scheme("ftp").is_err());
        assert_eq!(
            https_form(&"wss://api.example/v1/x?q=1".parse().unwrap()),
            "https://api.example/v1/x?q=1"
        );
    }
}
