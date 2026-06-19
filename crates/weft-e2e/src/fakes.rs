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
    let cluster = std::env::var("WEFT_CLUSTER_NAME").unwrap_or_else(|_| "weft-local".to_string());
    let node = format!("{cluster}-control-plane");
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

/// A fake Server-Sent-Events endpoint the listener SUBSCRIBES to
/// (`SseSubscribe`). The test pushes events through a channel; the server
/// streams them as SSE blocks to the connected listener.
pub struct SseFake {
    base_url: String,
    tx: tokio::sync::broadcast::Sender<(String, String)>,
    _server: AbortOnDrop,
}

impl SseFake {
    /// Bind an SSE fake. `base_url` + `/events` goes in `SseSubscribe.url`.
    pub async fn start() -> Result<Self> {
        let (tx, _rx) = tokio::sync::broadcast::channel::<(String, String)>(64);
        let (gateway, listener, port) = bind_host("sse").await?;
        let app = Router::new()
            .route("/events", get(sse_handler))
            .with_state(tx.clone());
        Ok(Self {
            base_url: format!("http://{gateway}:{port}"),
            tx,
            _server: serve_axum(listener, app),
        })
    }

    /// The cluster-reachable SSE URL.
    pub fn url(&self) -> String {
        format!("{}/events", self.base_url)
    }

    /// Push one SSE event with the given event name and JSON data line. Fires
    /// the listener's matching `SseSubscribe { event_name }`. The (event, data)
    /// pair is sent to the handler, which builds a single well-formed SSE frame
    /// from it (NOT a pre-formatted block: axum adds the `event:`/`data:` lines,
    /// so pre-formatting would double-wrap and corrupt the stream).
    pub fn push_event(&self, event: &str, data: &str) {
        // Ignore the "no subscribers yet" error: the test sequences push after
        // the subscription is live, but a stray early push is harmless to drop.
        let _ = self.tx.send((event.to_string(), data.to_string()));
    }
}

async fn sse_handler(
    State(tx): State<tokio::sync::broadcast::Sender<(String, String)>>,
) -> impl IntoResponse {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures::stream::StreamExt;
    let rx = tx.subscribe();
    let stream = tokio_stream_from_broadcast(rx).map(|(event, data)| {
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

/// Adapt a tokio broadcast receiver into a Stream of its items, dropping lag
/// errors (a slow consumer just misses old events, which is fine for a fake).
fn tokio_stream_from_broadcast<T: Clone + Send + 'static>(
    rx: tokio::sync::broadcast::Receiver<T>,
) -> impl futures::Stream<Item = T> {
    futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(item) => return Some((item, rx)),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
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
