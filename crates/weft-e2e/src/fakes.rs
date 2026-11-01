//! Throwaway servers for triggers the system dials OUT to.
//!
//! `PollFake` and `SseFake` serve the `PollEndpoint` and `SseSubscribe`
//! kinds, which the listener dials from the install's runtime process; the
//! other fakes are dialled by node code in a worker, a process on the same
//! machine. So each fake binds an ephemeral port on `127.0.0.1` and
//! advertises `http://127.0.0.1:<port>`, which the test puts in the
//! fixture, then drives events from the test.

use std::sync::Arc;

use anyhow::{Context, Result};
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

/// Bind a fake's listener on an ephemeral loopback port, and return the
/// URL it answers at with the bound listener.
async fn bind_host(what: &str) -> Result<(String, TcpListener)> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .with_context(|| format!("bind {what} fake"))?;
    let at = listener.local_addr()?;
    Ok((format!("http://{at}"), listener))
}

/// Spawn an axum app on a bound listener, returning an abort-on-drop handle.
/// Every fake here serves through it.
fn serve_axum(listener: TcpListener, app: Router) -> AbortOnDrop {
    AbortOnDrop(tokio::spawn(async move {
        // A spawned task has no caller to return to: say it in the test's
        // log, where a fake that went quiet would otherwise look like the
        // system under test never calling.
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("weft-e2e: a fake server stopped serving: {e}");
        }
    }))
}

/// A fake HTTP endpoint the listener POLLS (`PollEndpoint`). Each poll returns
/// the current body; the test sets the body to drive what the next poll fires.
pub struct PollFake {
    base_url: String,
    body: Arc<Mutex<String>>,
    _server: AbortOnDrop,
}

impl PollFake {
    /// Bind a poll fake on an ephemeral loopback port and return it. `base_url` is
    /// the reachable URL to put in the fixture's `PollEndpoint.url`.
    pub async fn start(initial_body: &str) -> Result<Self> {
        let body = Arc::new(Mutex::new(initial_body.to_string()));
        let (base_url, listener) = bind_host("poll").await?;
        let app = Router::new()
            .route("/poll", get(poll_handler))
            .with_state(body.clone());
        Ok(Self {
            base_url,
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

/// A scripted model behind an OpenAI-compatible chat endpoint
/// (`POST /chat/completions`), for a `CustomProvider` whose base URL is
/// [`ChatFake::base_url`]: the Nth call answers the Nth scripted
/// assistant message (as one streamed chunk when the call asks for a
/// stream, the way the LLM nodes call), so a test drives an agent through exact turns
/// without paying for a model. A call past the script answers 500, so
/// an agent that asked once too often fails loudly. Keeps every request
/// body, so a test can read what the agent sent back.
pub struct ChatFake {
    base_url: String,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
    _server: AbortOnDrop,
}

#[derive(Clone)]
struct ChatState {
    script: Arc<Vec<serde_json::Value>>,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl ChatFake {
    /// Bind the fake: each of `replies` is one assistant message (`content`,
    /// and `tool_calls` for a turn that calls tools), answered in order.
    pub async fn start(replies: Vec<serde_json::Value>) -> Result<Self> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (base_url, listener) = bind_host("chat").await?;
        let state = ChatState { script: Arc::new(replies), requests: requests.clone() };
        let app = Router::new().route("/chat/completions", axum::routing::post(chat_completion)).with_state(state);
        Ok(Self { base_url, requests, _server: serve_axum(listener, app) })
    }

    /// The base URL a `CustomProvider` takes.
    pub fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// Every request body received so far, in order.
    pub async fn requests(&self) -> Vec<serde_json::Value> {
        self.requests.lock().await.clone()
    }
}

async fn chat_completion(
    State(state): State<ChatState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> axum::response::Response {
    let streams = body["stream"] == serde_json::json!(true);
    let turn = {
        let mut requests = state.requests.lock().await;
        requests.push(body);
        requests.len() - 1
    };
    let Some(message) = state.script.get(turn) else {
        let said = format!("the chat fake was scripted for {} calls; this is call {}", state.script.len(), turn + 1);
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, said).into_response();
    };
    let finish = if message.get("tool_calls").is_some() { "tool_calls" } else { "stop" };
    let usage = serde_json::json!({ "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 });
    if streams {
        // The whole message as one delta (each tool call with its index,
        // the way a streamed call names which call a piece belongs to),
        // then the finish, then the end marker.
        let mut delta = message.clone();
        if let Some(calls) = delta.get_mut("tool_calls").and_then(serde_json::Value::as_array_mut) {
            for (index, call) in calls.iter_mut().enumerate() {
                call["index"] = serde_json::json!(index);
            }
        }
        let chunk = |choice: serde_json::Value, usage: Option<serde_json::Value>| {
            let mut frame = serde_json::json!({ "id": format!("chatcmpl-{turn}"), "model": "scripted", "choices": [choice] });
            if let Some(usage) = usage {
                frame["usage"] = usage;
            }
            format!("data: {frame}\n\n")
        };
        let mut sse = chunk(serde_json::json!({ "index": 0, "delta": delta }), None);
        sse.push_str(&chunk(serde_json::json!({ "index": 0, "delta": {}, "finish_reason": finish }), Some(usage)));
        sse.push_str("data: [DONE]\n\n");
        return ([(axum::http::header::CONTENT_TYPE, "text/event-stream")], sse).into_response();
    }
    axum::Json(serde_json::json!({
        "id": format!("chatcmpl-{turn}"),
        "object": "chat.completion",
        "model": "scripted",
        "choices": [{ "index": 0, "message": message, "finish_reason": finish }],
        "usage": usage,
    }))
    .into_response()
}

/// A fake HTTP server that serves fixed bytes at `/bytes`. Used by the storage
/// fixture: a FetchToStorage node fetches FROM here, so the rig controls the
/// exact content it can then download back and assert. Bound on
/// `127.0.0.1` like the other fakes (see the module docs).
pub struct BytesFake {
    base_url: String,
    _server: AbortOnDrop,
}

impl BytesFake {
    /// Serve `content` (as `application/octet-stream`) at `<url>()`.
    pub async fn start(content: Vec<u8>) -> Result<Self> {
        let (base_url, listener) = bind_host("bytes").await?;
        let app = Router::new()
            .route("/bytes", get(bytes_handler))
            .with_state(Arc::new(content));
        Ok(Self {
            base_url,
            _server: serve_axum(listener, app),
        })
    }

    /// The reachable URL of the served bytes (`<base>/bytes`).
    pub fn url(&self) -> String {
        format!("{}/bytes", self.base_url)
    }
}

/// A fake provider with a QUEUE shape: money is committed when a job is
/// submitted, and the amount is only stated on a later read.
///
/// The shape every "submit now, pay later" provider has, and the one the
/// worker's open-charge machinery exists for: every read answers the job
/// finished, with its billed count in [`QUEUE_UNITS_HEADER`].
pub struct QueueFake {
    base_url: String,
    state: QueueState,
    _server: AbortOnDrop,
}

#[derive(Clone)]
struct QueueState {
    next_id: Arc<std::sync::atomic::AtomicUsize>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
    /// The billed count every read of this fake states.
    units: u32,
}

impl QueueFake {
    /// Bind the fake. `units` is the billed count every read reports.
    pub async fn start(units: u32) -> Result<Self> {
        let state = QueueState {
            next_id: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            units,
        };
        let (base_url, listener) = bind_host("queue").await?;
        let app = Router::new()
            .route("/submit", axum::routing::post(queue_submit))
            .route("/result/{id}", get(queue_result))
            .with_state(state.clone());
        Ok(Self {
            base_url,
            state,
            _server: serve_axum(listener, app),
        })
    }

    /// The reachable base a connection's stored base points at.
    pub fn base(&self) -> String {
        self.base_url.clone()
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
    let id = state.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    axum::Json(serde_json::json!({ "request_id": format!("job-{id}") }))
}

async fn queue_result(State(state): State<QueueState>) -> impl IntoResponse {
    state.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    (
        [(QUEUE_UNITS_HEADER, state.units.to_string())],
        axum::Json(serde_json::json!({ "status": "COMPLETED" })),
    )
}

async fn bytes_handler(State(body): State<Arc<Vec<u8>>>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        (*body).clone(),
    )
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
        let (base_url, listener) = bind_host("sse").await?;
        let app = Router::new()
            .route("/events", get(sse_handler))
            .with_state(state.clone());
        Ok(Self {
            base_url,
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
    /// Errors when no connection is reading at all: the event would be lost.
    pub fn push_event(&self, event: &str, data: &str) -> Result<()> {
        self.state
            .tx
            .send((event.to_string(), data.to_string()))
            .map_err(|_| anyhow::anyhow!("pushed SSE event '{event}' with no connection reading the feed; it was lost"))?;
        Ok(())
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

