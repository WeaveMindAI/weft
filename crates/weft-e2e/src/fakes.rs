//! Throwaway servers for triggers the system dials OUT to.
//!
//! `PollEndpoint`, `SseSubscribe`, and `SocketListen` are processed by the
//! LISTENER pod inside the kind cluster: the pod reaches out to a URL and fires
//! per event. To test them, the rig stands up a tiny server the listener can
//! connect to, then drives events from the test.
//!
//! ## Cluster reachability (the load-bearing detail)
//!
//! A server bound on the test host's `127.0.0.1` is NOT reachable from a pod:
//! a pod's default route is the pod network, and `host.docker.internal` does
//! not resolve in kind. The reachable path is the kind node container's
//! host-gateway IP on the docker bridge (e.g. `172.19.0.1`), which a pod CAN
//! reach (verified). So a fake:
//!   - binds on `0.0.0.0:<port>` on the host, and
//!   - advertises its URL as `http://<gateway-ip>:<port>` (the cluster-reachable
//!     address), which the test injects into the fixture's trigger URL.
//!
//! The tenant pod egress policy is `0.0.0.0/0 except {pod_cidr, service_cidr}`,
//! so egress to the host gateway is allowed by design.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// A spawned server task that is ABORTED when dropped. A bare `JoinHandle`
/// detaches on drop (the task keeps running and its port stays bound), so every
/// fake held one would leak a live server for the rest of the test process.
/// Each fake owns one of these instead, so dropping the fake tears its server
/// down. The field is never read; it exists for its `Drop`.
struct AbortOnDrop(#[allow(dead_code)] JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Bind a fake's listener: discover the cluster-reachable host-gateway IP, bind
/// `0.0.0.0:<ephemeral>` on the host, and return the gateway IP, the bound
/// listener, and the chosen port. The single place the bind + gateway dance
/// lives, so the four fakes don't each restate it.
async fn bind_host(what: &str) -> Result<(String, TcpListener, u16)> {
    let gateway = cluster_host_gateway().await?;
    let listener = TcpListener::bind(("0.0.0.0", 0))
        .await
        .with_context(|| format!("bind {what} fake"))?;
    let port = listener.local_addr()?.port();
    Ok((gateway, listener, port))
}

/// Spawn an axum app on a bound listener, returning an abort-on-drop handle.
/// Shared by the three HTTP-shaped fakes (poll / bytes / sse); the WS fake runs
/// a custom accept loop and builds its own [`AbortOnDrop`].
fn serve_axum(listener: TcpListener, app: Router) -> AbortOnDrop {
    AbortOnDrop(tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    }))
}

/// The kind node container's host-gateway IP, discovered from docker. This is
/// the address a pod uses to reach a server bound on the host. Resolved once;
/// the cluster name follows the daemon's default (`weft-local`) unless
/// `WEFT_CLUSTER_NAME` overrides it.
pub async fn cluster_host_gateway() -> Result<String> {
    // The docker container the cluster node runs in. KIND names it
    // `<cluster>-control-plane` (the default below); a minikube docker-driver
    // node IS the profile name, so a minikube-based harness sets
    // `WEFT_CLUSTER_NODE_CONTAINER=<profile>` directly. Either way we inspect
    // the node container's docker-bridge gateway, which its pods can reach.
    let node = std::env::var("WEFT_CLUSTER_NODE_CONTAINER").unwrap_or_else(|_| {
        let cluster =
            std::env::var("WEFT_CLUSTER_NAME").unwrap_or_else(|_| "weft-local".to_string());
        format!("{cluster}-control-plane")
    });
    let out = tokio::process::Command::new("docker")
        .args([
            "inspect",
            &node,
            "--format",
            "{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}",
        ])
        .output()
        .await
        .context("docker inspect for kind node gateway")?;
    if !out.status.success() {
        bail!(
            "docker inspect {node} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let gw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if gw.is_empty() {
        bail!("could not determine host-gateway IP for kind node {node}");
    }
    Ok(gw)
}

/// A fake HTTP endpoint the listener POLLS (`PollEndpoint`). Each poll returns
/// the current body; the test sets the body to drive what the next poll fires.
pub struct PollFake {
    base_url: String,
    body: Arc<Mutex<String>>,
    _server: AbortOnDrop,
}

impl PollFake {
    /// Bind a poll fake on an ephemeral host port and return it. `base_url` is
    /// the cluster-reachable URL to put in the fixture's `PollEndpoint.url`.
    pub async fn start(initial_body: &str) -> Result<Self> {
        let body = Arc::new(Mutex::new(initial_body.to_string()));
        let (gateway, listener, port) = bind_host("poll").await?;
        let app = Router::new()
            .route("/poll", get(poll_handler))
            .with_state(body.clone());
        Ok(Self {
            base_url: format!("http://{gateway}:{port}"),
            body,
            _server: serve_axum(listener, app),
        })
    }

    /// The cluster-reachable URL of the poll endpoint (`<base>/poll`).
    pub fn url(&self) -> String {
        format!("{}/poll", self.base_url)
    }

    /// Set the body the next poll will return (drives the next fire).
    pub async fn set_body(&self, body: &str) {
        *self.body.lock().await = body.to_string();
    }
}

async fn poll_handler(State(body): State<Arc<Mutex<String>>>) -> impl IntoResponse {
    let b = body.lock().await.clone();
    ([(axum::http::header::CONTENT_TYPE, "application/json")], b)
}

/// A fake HTTP server that serves fixed bytes at `/bytes`. Used by the storage
/// fixture: a FetchToStorage node fetches FROM here, so the rig controls the
/// exact content it can then download back and assert. Cluster-reachable like
/// the other fakes (bound on the host, advertised at the kind host-gateway IP).
pub struct BytesFake {
    base_url: String,
    _server: AbortOnDrop,
}

impl BytesFake {
    /// Serve `content` (as `application/octet-stream`) at `<url>()`.
    pub async fn start(content: Vec<u8>) -> Result<Self> {
        let body = Arc::new(content);
        let (gateway, listener, port) = bind_host("bytes").await?;
        let app = Router::new()
            .route("/bytes", get(bytes_handler))
            .with_state(body);
        Ok(Self {
            base_url: format!("http://{gateway}:{port}"),
            _server: serve_axum(listener, app),
        })
    }

    /// The cluster-reachable URL of the served bytes (`<base>/bytes`).
    pub fn url(&self) -> String {
        format!("{}/bytes", self.base_url)
    }
}

async fn bytes_handler(State(body): State<Arc<Vec<u8>>>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        (*body).clone(),
    )
}

/// A fake that PROMISES more bytes than it delivers, then breaks the
/// connection mid-body: it advertises `Content-Length: <declared>` but streams
/// only `<sent>` bytes before erroring the response body. A client streaming
/// the body (the worker's fetch-into-storage) sees an incomplete-body transport
/// error partway through, so a large fetch that has already uploaded one or more
/// parts is interrupted with work in flight. Used to prove the upload path
/// cleans up (aborts the in-flight upload, frees the quota reservation) and
/// leaves NO leftover when a source dies mid-transfer.
pub struct HangingBytesFake {
    base_url: String,
    _server: AbortOnDrop,
}

/// State for the hanging handler: how many real bytes to emit before erroring,
/// and the full length to advertise (must exceed `sent`).
#[derive(Clone)]
struct HangingState {
    sent: usize,
    declared: usize,
}

impl HangingBytesFake {
    /// Advertise `declared` bytes but deliver only `sent` before breaking the
    /// body. `sent` must be < `declared` (otherwise the transfer completes).
    pub async fn start(sent: usize, declared: usize) -> Result<Self> {
        if sent >= declared {
            bail!("HangingBytesFake needs sent ({sent}) < declared ({declared}) to interrupt");
        }
        let (gateway, listener, port) = bind_host("hanging-bytes").await?;
        let app = Router::new()
            .route("/bytes", get(hanging_handler))
            .with_state(HangingState { sent, declared });
        Ok(Self {
            base_url: format!("http://{gateway}:{port}"),
            _server: serve_axum(listener, app),
        })
    }

    /// The cluster-reachable URL of the served (truncated) bytes.
    pub fn url(&self) -> String {
        format!("{}/bytes", self.base_url)
    }
}

async fn hanging_handler(State(state): State<HangingState>) -> impl IntoResponse {
    use futures::stream::StreamExt;

    // Emit `sent` bytes in modest chunks, then ONE error item. axum aborts the
    // body on the error; combined with the oversized Content-Length below, the
    // client sees an incomplete-body transport error, exactly what a source
    // that dies mid-transfer looks like.
    const CHUNK: usize = 64 * 1024;
    let chunks = (state.sent + CHUNK - 1) / CHUNK;
    let data = futures::stream::iter((0..chunks).map(move |i| {
        let start = i * CHUNK;
        let end = (start + CHUNK).min(state.sent);
        Ok::<_, std::io::Error>(axum::body::Bytes::from(vec![(i % 251) as u8; end - start]))
    }));
    let broken = futures::stream::once(async {
        Err::<axum::body::Bytes, std::io::Error>(std::io::Error::other("fake source dropped mid-body"))
    });
    let body = axum::body::Body::from_stream(data.chain(broken));
    axum::response::Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        // Advertise MORE than we will send, so the early close is an
        // incomplete-body error on the client, not a clean EOF.
        .header(axum::http::header::CONTENT_LENGTH, state.declared.to_string())
        .body(body)
        .expect("build hanging response")
}

/// Shared state for the SSE fake: the broadcast sender plus a count of
/// connections that are ACTIVELY READING their stream (have polled it at
/// least once). The reading-count, not `tx.receiver_count()`, is the
/// correct readiness signal: a broadcast delivers an event only to a
/// receiver whose stream task has already been polled and is awaiting the
/// next item. A connection can have subscribed (bumping receiver_count)
/// yet not have reached its first poll, so it would miss a one-shot send.
/// Waiting on the reading-count guarantees every counted connection will
/// catch the next `send` (a single emission then reaches all of them,
/// exactly once each), which is what a real SSE feed delivers to its
/// open-and-reading connections.
#[derive(Clone)]
struct SseState {
    tx: tokio::sync::broadcast::Sender<(String, String)>,
    reading: Arc<std::sync::atomic::AtomicUsize>,
}

/// A fake Server-Sent-Events endpoint the listener SUBSCRIBES to
/// (`SseSubscribe`). The test pushes events through a channel; the server
/// streams them as SSE blocks to the connected listener.
pub struct SseFake {
    base_url: String,
    state: SseState,
    _server: AbortOnDrop,
}

impl SseFake {
    /// Bind an SSE fake. `base_url` + `/events` goes in `SseSubscribe.url`.
    pub async fn start() -> Result<Self> {
        let (tx, _rx) = tokio::sync::broadcast::channel::<(String, String)>(64);
        let state = SseState {
            tx,
            reading: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let (gateway, listener, port) = bind_host("sse").await?;
        let app = Router::new()
            .route("/events", get(sse_handler))
            .with_state(state.clone());
        Ok(Self {
            base_url: format!("http://{gateway}:{port}"),
            state,
            _server: serve_axum(listener, app),
        })
    }

    /// The cluster-reachable SSE URL.
    pub fn url(&self) -> String {
        format!("{}/events", self.base_url)
    }

    /// How many listener SSE connections are ACTIVELY READING their stream
    /// (have polled it at least once and are awaiting the next event). This
    /// is the readiness signal a test waits on before `push_event`: only a
    /// reading connection is guaranteed to catch the next single emission.
    /// A fixed "let the subscription settle" sleep is a latent flake; a raw
    /// `receiver_count` overcounts (a subscribed-but-not-yet-polled
    /// connection would miss a one-shot send); the reading-count is exact.
    pub fn subscriber_count(&self) -> usize {
        self.state.reading.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Block until at least `n` listener SSE connections are actively
    /// reading (or the deadline elapses, which is a real failure: the
    /// expected connection(s) never armed). Call before `push_event` so the
    /// event is never pushed before the connection(s) the test depends on
    /// are reading. `n = 1` is the normal single-holder case; `n = 2` is
    /// the move-overlap case (the test needs BOTH the old and new pod
    /// reading so the generation fence is actually exercised by one event
    /// reaching both).
    pub async fn wait_for_subscribers(
        &self,
        n: usize,
        deadline: std::time::Duration,
    ) -> Result<()> {
        // The count must reach `n` AND HOLD there for a short window before we
        // call it ready. The reading-count is a scalar (it can't tell which
        // pod each connection belongs to), and a listener's SSE client briefly
        // holds TWO connections while it reconnects (old not yet dropped, new
        // already reading). For n >= 2 that transient could let ONE pod's
        // reconnect satisfy the count while the OTHER pod isn't reading yet, so
        // a single observation of `>= n` is not enough. A reconnect blip
        // collapses back within ~1-2s (the old connection drops), whereas a
        // genuine set of `n` distinct readers stays put, so requiring the count
        // to stay `>= n` continuously across `STABLE_FOR` rules out the blip
        // without needing per-pod identity (which the host-gateway NAT hides:
        // every pod's traffic arrives from the one kind-node bridge address).
        const STABLE_FOR: std::time::Duration = std::time::Duration::from_secs(3);
        const POLL: std::time::Duration = std::time::Duration::from_millis(100);
        let start = std::time::Instant::now();
        let mut at_or_above_since: Option<std::time::Instant> = None;
        loop {
            let count = self.subscriber_count();
            if count >= n {
                let since = at_or_above_since.get_or_insert_with(std::time::Instant::now);
                if since.elapsed() >= STABLE_FOR {
                    return Ok(());
                }
            } else {
                // Dropped below the threshold: the previous run wasn't a stable
                // set of `n` readers (e.g. a reconnect blip), restart the timer.
                at_or_above_since = None;
            }
            if start.elapsed() >= deadline {
                anyhow::bail!(
                    "fewer than {n} listener(s) read the SSE feed STABLY (for {STABLE_FOR:?}) \
                     within {deadline:?} (last saw {count}); the expected connection(s) never \
                     armed, or only a transient reconnect briefly reached {n}"
                );
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Convenience for the common single-subscriber case.
    pub async fn wait_for_subscriber(&self, deadline: std::time::Duration) -> Result<()> {
        self.wait_for_subscribers(1, deadline).await
    }

    /// Push one SSE event with the given event name and JSON data line. Fires
    /// the listener's matching `SseSubscribe { event_name }`. The (event, data)
    /// pair is sent to the handler, which builds a single well-formed SSE frame
    /// from it (NOT a pre-formatted block: axum adds the `event:`/`data:` lines,
    /// so pre-formatting would double-wrap and corrupt the stream).
    ///
    /// Poll `subscriber_count() >= n` (n = the connections the test depends
    /// on) before calling this so the event reaches every reading connection.
    pub fn push_event(&self, event: &str, data: &str) {
        // Ignore the "no subscribers yet" error: the test sequences push after
        // the subscription is live, but a stray early push is harmless to drop.
        let _ = self.state.tx.send((event.to_string(), data.to_string()));
    }
}

async fn sse_handler(State(state): State<SseState>) -> impl IntoResponse {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures::stream::StreamExt;
    use std::sync::atomic::Ordering;

    // Subscribe NOW so events sent after this point are buffered for us,
    // then count this connection as "reading" only once its stream is
    // first polled (below), i.e. once the recv future is actually armed.
    let rx = state.tx.subscribe();
    // RAII: increment the reading-count when this connection's stream
    // starts being consumed, decrement when it is dropped (connection
    // closed / pod reaped). `wait_for_subscribers` waits on this count.
    struct ReadingGuard(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for ReadingGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let reading = state.reading.clone();
    // `stream::once` runs on the FIRST poll of the response stream: that
    // is the moment axum begins consuming, so the connection is now
    // reading. Arm the guard there, then chain the live event stream.
    let armed = futures::stream::once(async move {
        reading.fetch_add(1, Ordering::SeqCst);
        // Move the guard into the stream so it lives as long as the
        // connection and drops (decrement) when the connection ends.
        let _guard = ReadingGuard(state.reading.clone());
        futures::stream::unfold((rx, _guard), |(mut rx, guard)| async move {
            loop {
                match rx.recv().await {
                    Ok(item) => return Some((item, (rx, guard))),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
    })
    .flatten();
    let stream = armed.map(|(event, data)| {
        // Build ONE well-formed SSE frame: axum emits `event: <name>` and
        // `data: <data>` lines itself, so we pass the name and data separately
        // rather than a pre-formatted block.
        Ok::<_, std::convert::Infallible>(Event::default().event(event).data(data))
    });
    // A keep-alive comment line keeps the connection from being closed as an
    // empty body before the first real event (which is what produced the
    // listener's "error decoding response body" against a bare 200).
    Sse::new(stream).keep_alive(KeepAlive::default())
}


/// A fake WebSocket gateway the listener DIALS (`SocketListen`). The listener
/// connects, sends its configured handshake frame, and the fake can push
/// frames back (each fires the listener's signal). Inbound frames (the
/// handshake + heartbeats) are recorded so a test can assert the listener spoke
/// the configured protocol.
pub struct SocketFake {
    base_url: String,
    /// Frames the test wants pushed to the next connected client, drained in
    /// order as the connection accepts them.
    outbound: Arc<Mutex<Vec<String>>>,
    /// Frames received from the listener (handshake, heartbeats), in order.
    inbound: Arc<Mutex<Vec<String>>>,
    _server: AbortOnDrop,
}

impl SocketFake {
    /// Bind a WS fake. `ws_url()` goes in `SocketListen.url`.
    pub async fn start() -> Result<Self> {
        let outbound: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let inbound: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (gateway, listener, port) = bind_host("socket").await?;
        let out_c = outbound.clone();
        let in_c = inbound.clone();
        let server = AbortOnDrop(tokio::spawn(async move {
            // Spawn each connection into a JoinSet OWNED by this task, so when
            // the fake drops (AbortOnDrop aborts this task) the JoinSet is
            // dropped too, which aborts every per-connection task. Spawning them
            // detached (bare tokio::spawn) would leak any connection still open
            // at drop, breaking the "AbortOnDrop tears down all it spawned"
            // invariant. We `select!` between accepting and reaping completions
            // so finished connection tasks are drained from the set as they end,
            // rather than piling up for the life of the fake (a JoinSet only
            // frees a task's slot when it is joined, not when it completes).
            let mut conns = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _peer)) = accepted else { break };
                        let out_c = out_c.clone();
                        let in_c = in_c.clone();
                        conns.spawn(async move {
                            let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                                return;
                            };
                            serve_socket(ws, out_c, in_c).await;
                        });
                    }
                    // Reap a finished connection task. `join_next` is `None` only
                    // when the set is empty; that branch then parks (the accept
                    // arm drives progress), so this never busy-spins.
                    Some(_) = conns.join_next() => {}
                }
            }
        }));
        Ok(Self {
            base_url: format!("ws://{gateway}:{port}"),
            outbound,
            inbound,
            _server: server,
        })
    }

    /// The cluster-reachable `ws://` URL.
    pub fn ws_url(&self) -> String {
        format!("{}/socket", self.base_url)
    }

    /// Queue a text frame to push to the connected listener (fires its signal).
    pub async fn push_frame(&self, text: &str) {
        self.outbound.lock().await.push(text.to_string());
    }

    /// The frames the listener has sent us so far (handshake, heartbeats).
    pub async fn received(&self) -> Vec<String> {
        self.inbound.lock().await.clone()
    }
}

/// Drive one accepted WS connection: record inbound text frames, and push any
/// queued outbound frames. Runs until the socket closes.
async fn serve_socket(
    mut ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    outbound: Arc<Mutex<Vec<String>>>,
    inbound: Arc<Mutex<Vec<String>>>,
) {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    loop {
        // Push any queued frames first so a test that queued before connect
        // still delivers.
        let pending: Vec<String> = {
            let mut q = outbound.lock().await;
            std::mem::take(&mut *q)
        };
        for frame in pending {
            if ws.send(Message::Text(frame.into())).await.is_err() {
                return;
            }
        }
        // Then wait briefly for an inbound frame; loop to keep draining the
        // outbound queue as the test pushes more.
        match tokio::time::timeout(std::time::Duration::from_millis(100), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => inbound.lock().await.push(t.to_string()),
            Ok(Some(Ok(Message::Binary(b)))) => {
                inbound.lock().await.push(String::from_utf8_lossy(&b).into_owned())
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => return,
            Ok(Some(Ok(_))) => {} // ping/pong/other: ignore
            Ok(Some(Err(_))) => return,
            Err(_) => {} // timeout: loop to drain outbound again
        }
    }
}
