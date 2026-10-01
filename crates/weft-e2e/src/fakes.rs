//! Throwaway servers for triggers the system dials OUT to.
//!
//! `PollFake` and `SseFake` serve the `PollEndpoint` and `SseSubscribe`
//! kinds, which the LISTENER dials, and it runs in the install's runtime
//! process on the host; the other fakes are dialled by node code in a
//! WORKER container. To test them, the
//! rig stands up a tiny server both can connect to, then drives events from
//! the test.
//!
//! ## Reachability
//!
//! A server bound on the test host's `127.0.0.1` is NOT reachable from a
//! container. The address both sides reach is the gateway of the install's
//! Docker network (`weft`): the host owns that address, and every worker
//! sits on that network. So a fake:
//!   - binds on `0.0.0.0:<port>` on the host, and
//!   - advertises its URL as `http://<gateway-ip>:<port>`, which the test
//!     injects into the fixture's trigger URL.

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

/// Bind a fake's listener: discover the host's address on the network, bind
/// `0.0.0.0:<ephemeral>` on the host, and return the gateway IP, the bound
/// listener, and the chosen port. The single place the bind + gateway dance
/// lives, so the four fakes don't each restate it.
async fn bind_host(what: &str) -> Result<(String, TcpListener, u16)> {
    let gateway = host_address().await?;
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

/// The gateway of the install's Docker network: the host's own address on
/// that network, which the runtime and every worker reach.
pub async fn host_address() -> Result<String> {
    let args = ["network", "inspect", weft_platform_local::docker::NETWORK, "--format", "{{range .IPAM.Config}}{{.Gateway}}{{end}}"];
    let out = tokio::process::Command::new("docker").args(args).output().await.context("spawn docker")?;
    if !out.status.success() {
        bail!("docker {} failed: {}", args.join(" "), String::from_utf8_lossy(&out.stderr));
    }
    let ip = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if ip.is_empty() {
        bail!("the install's Docker network has no gateway; is the install up?");
    }
    Ok(ip)
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
    /// the reachable URL to put in the fixture's `PollEndpoint.url`.
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

    /// The reachable URL of the poll endpoint (`<base>/poll`).
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
/// exact content it can then download back and assert. Reachable like
/// the other fakes (bound on the host, advertised at the host's address on the network).
pub struct BytesFake {
    base_url: String,
    served: Arc<std::sync::atomic::AtomicUsize>,
    _server: AbortOnDrop,
}

/// What the bytes handler holds: the body, and a count of the times it
/// was fetched (a test's proof of WHO fetched, and when).
#[derive(Clone)]
struct BytesState {
    body: Arc<Vec<u8>>,
    served: Arc<std::sync::atomic::AtomicUsize>,
}

impl BytesFake {
    /// Serve `content` (as `application/octet-stream`) at `<url>()`.
    pub async fn start(content: Vec<u8>) -> Result<Self> {
        let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (gateway, listener, port) = bind_host("bytes").await?;
        let app = Router::new()
            .route("/bytes", get(bytes_handler))
            .with_state(BytesState { body: Arc::new(content), served: served.clone() });
        Ok(Self {
            base_url: format!("http://{gateway}:{port}"),
            served,
            _server: serve_axum(listener, app),
        })
    }

    /// The reachable URL of the served bytes (`<base>/bytes`).
    pub fn url(&self) -> String {
        format!("{}/bytes", self.base_url)
    }

    /// How many times the bytes were fetched so far.
    pub fn served(&self) -> usize {
        self.served.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// A fake provider with a QUEUE shape: money is committed when a job is
/// submitted, and the amount is only stated on a later read.
///
/// The shape every "submit now, pay later" provider has, and the one the
/// worker's open-charge machinery exists for. The rig owns both ends, so
/// a test can submit a job and then decide whether its answer is ever
/// read back: that is the difference between a spend with a figure on it
/// and a spend the trail has to record as unknown.
pub struct QueueFake {
    base_url: String,
    state: QueueState,
    _server: AbortOnDrop,
}

#[derive(Clone)]
struct QueueState {
    next_id: Arc<std::sync::atomic::AtomicUsize>,
    submits: Arc<std::sync::atomic::AtomicUsize>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
    /// What every read of this fake answers: `Some(units)` states a
    /// billed count, `None` says "still running", which is how a test
    /// produces a spend nobody can put a figure on. Set once at
    /// `start` and never changed, so it needs no lock: it used to be
    /// behind one for a `finish_with` that no test called.
    units: Option<u32>,
}

impl QueueFake {
    /// Bind the fake. `units` is what a read reports once the job has
    /// finished; `None` leaves every read saying "still running", which
    /// is how a test produces a spend nobody can put a figure on.
    pub async fn start(units: Option<u32>) -> Result<Self> {
        let state = QueueState {
            next_id: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            submits: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            units,
        };
        let (gateway, listener, port) = bind_host("queue").await?;
        let app = Router::new()
            .route("/submit", axum::routing::post(queue_submit))
            .route("/result/{id}", get(queue_result))
            .with_state(state.clone());
        Ok(Self {
            base_url: format!("http://{gateway}:{port}"),
            state,
            _server: serve_axum(listener, app),
        })
    }

    /// The reachable base a connection's stored base points at.
    pub fn base(&self) -> String {
        self.base_url.clone()
    }

    /// How many jobs were submitted (money committed).
    pub fn submits(&self) -> usize {
        self.state.submits.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// How many times an answer was read back.
    pub fn reads(&self) -> usize {
        self.state.reads.load(std::sync::atomic::Ordering::SeqCst)
    }

}

/// The header a read states the billed count in, the way a real queue
/// provider does.
// SYNC: QUEUE_UNITS_HEADER <-> crates/weft-e2e/fixtures/metering_queue/nodes/queue_job/mod.rs UNITS_HEADER
// (the fixture's meter declares this header; renaming one side alone
// makes the meter observe nothing and the test report an unpriced spend,
// naming neither file)
pub const QUEUE_UNITS_HEADER: &str = "x-queue-units";

async fn queue_submit(State(state): State<QueueState>) -> impl IntoResponse {
    state.submits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let id = state.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    axum::Json(serde_json::json!({ "request_id": format!("job-{id}") }))
}

async fn queue_result(State(state): State<QueueState>) -> impl IntoResponse {
    state.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    match state.units {
        Some(units) => (
            [(QUEUE_UNITS_HEADER, units.to_string())],
            axum::Json(serde_json::json!({ "status": "COMPLETED" })),
        )
            .into_response(),
        None => axum::Json(serde_json::json!({ "status": "IN_PROGRESS" })).into_response(),
    }
}

async fn bytes_handler(State(state): State<BytesState>) -> impl IntoResponse {
    state.served.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    (
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        (*state.body).clone(),
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

    /// The reachable URL of the served (truncated) bytes.
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
    let chunks = state.sent.div_ceil(CHUNK);
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

    /// The reachable SSE URL.
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
    /// are reading. `n = 1` is the normal case; a test with several
    /// subscriptions on one feed waits for all of them.
    pub async fn wait_for_subscribers(
        &self,
        n: usize,
        deadline: std::time::Duration,
    ) -> Result<()> {
        // The count must reach `n` AND HOLD there for a short window before we
        // call it ready. The reading-count is a scalar (it can't tell which
        // subscription each connection belongs to), and a listener's SSE client briefly
        // holds TWO connections while it reconnects (old not yet dropped, new
        // already reading). For n >= 2 that transient could let ONE
        // subscription's reconnect satisfy the count while another isn't reading yet, so
        // a single observation of `>= n` is not enough. A reconnect blip
        // collapses back within ~1-2s (the old connection drops), whereas a
        // genuine set of `n` distinct readers stays put, so requiring the count
        // to stay `>= n` continuously across `STABLE_FOR` rules out the blip
        // without needing per-connection identity.
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
    // closed / listener gone). `wait_for_subscribers` waits on this count.
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

