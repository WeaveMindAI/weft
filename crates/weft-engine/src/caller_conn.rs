//! Production `CallerConnection` (worker side of a live caller
//! connection) plus the per-worker registry that attaches an accepted
//! socket to the right execution.
//!
//! Shape (one connection per execution color):
//!   - OUTBOUND: nodes call `send_chunk` / `terminate`; the connection
//!     pushes onto a bounded single-consumer `OutboundQueue` the socket
//!     task drains to the wire. The queue is a `VecDeque` the PRODUCER can
//!     evict the front of, so all three backpressure policies are real:
//!     `block` awaits a slot, `drop_newest` sheds the incoming chunk,
//!     `drop_oldest` pops the front and enqueues (so one slow caller
//!     cannot grow a multiplexing pod's RAM). Terminal items always land.
//!   - INBOUND (WebSocket): the socket task publishes each decoded
//!     message onto a bounded `InboundLog`; every node's `receive` holds
//!     its own absolute-offset cursor over the same window, so inbound
//!     BROADCASTS to all listeners (the model we settled on).
//!   - HTTP request parts are captured once at attach and read via
//!     `http_request`.
//!   - The terminate-once latch + connected flag live behind one mutex.
//!   - Every observable event (connect / inbound / outbound / error /
//!     disconnect) is projected to a `Caller*` journal row through the
//!     same kind of pump the bus uses, so the inspector replays it.
//!
//! TLS terminates at the gateway; this server speaks plain HTTP/WS over
//! the private cluster network and trusts the dispatcher-signed token
//! (verified in [`crate::run_pod`]'s accept path) for authentication.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequestParts, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use tokio::sync::mpsc;

use weft_core::caller::{
    CallerConnection, CallerError, CallerRuntimeConfig, DisconnectAction, HttpRequestParts,
    InboundMessage, OutboundChunk, resolve_disconnect, try_terminate,
};
use weft_core::caller_token;
use weft_core::signal::{Backpressure, DataType, Protocol};
use weft_core::Color;

/// Capacity of the outbound buffer (chunks queued toward the wire before
/// backpressure kicks in). Bounded so a slow caller slows the producer
/// (block mode) or sheds (drop modes) rather than growing RAM.
const OUTBOUND_BUFFER: usize = 256;

/// What the socket task pulls off the outbound queue.
#[derive(Debug, Clone)]
pub(crate) enum Outbound {
    /// A non-terminal chunk to write to the wire.
    Chunk(OutboundChunk),
    /// The terminal: final optional body, then close the wire.
    Terminate(Option<OutboundChunk>),
    /// An error to surface to the caller per the error mode (in-band
    /// chunk / close frame), then close.
    Error(String),
}

/// Single-consumer bounded outbound queue: many nodes push, one socket
/// task drains. Unlike an `mpsc`, the PRODUCER owns the buffer, so it can
/// evict the FRONT (which `mpsc` cannot), making `DropOldest` real instead
/// of degrading to `DropNewest`. Shared by `Arc`; `closed` is set when the
/// socket task ends so a blocked producer wakes and a drainer past the end
/// returns `None`.
///
/// Only non-terminal `Chunk`s are subject to the capacity policy; terminal
/// items (`Terminate`/`Error`) always land (the exchange must be able to
/// end). The `capacity` therefore bounds queued chunks, not the whole
/// buffer, which can briefly hold `capacity + 1` when a terminal arrives
/// behind a full queue.
pub(crate) struct OutboundQueue {
    inner: Mutex<OutboundInner>,
    /// Woken on every push (drainer) AND on every drain/close (a blocked
    /// `Block`-mode producer waiting for a slot).
    notify: tokio::sync::Notify,
    capacity: usize,
}

struct OutboundInner {
    items: std::collections::VecDeque<Outbound>,
    /// Set when the socket task ends: producers stop queueing (return a
    /// transport error) and the drainer drains the tail then returns `None`.
    closed: bool,
}

impl OutboundQueue {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(OutboundInner {
                items: std::collections::VecDeque::new(),
                closed: false,
            }),
            notify: tokio::sync::Notify::new(),
            capacity: capacity.max(1),
        })
    }

    /// Count of queued non-terminal chunks (the capacity bound applies to
    /// these; terminals always land). Caller holds the lock.
    fn chunk_count(inner: &OutboundInner) -> usize {
        inner
            .items
            .iter()
            .filter(|o| matches!(o, Outbound::Chunk(_)))
            .count()
    }

    /// `Block` mode: wait until a chunk slot frees, then enqueue. Errors
    /// only if the queue closes (socket gone).
    async fn push_block(&self, chunk: Outbound) -> Result<(), CallerError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().expect("outbound queue poisoned");
                if g.closed {
                    return Err(CallerError::Transport("outbound queue closed".into()));
                }
                if Self::chunk_count(&g) < self.capacity {
                    g.items.push_back(chunk);
                    drop(g);
                    self.notify.notify_waiters();
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// `DropNewest`: enqueue if there is room, else shed the incoming chunk.
    fn push_drop_newest(&self, chunk: Outbound) -> Result<(), CallerError> {
        let mut g = self.inner.lock().expect("outbound queue poisoned");
        if g.closed {
            return Err(CallerError::Transport("outbound queue closed".into()));
        }
        if Self::chunk_count(&g) < self.capacity {
            g.items.push_back(chunk);
            drop(g);
            self.notify.notify_waiters();
        }
        Ok(())
    }

    /// `DropOldest`: if full, evict the OLDEST queued chunk (not a terminal)
    /// to make room, then enqueue the incoming one. This is the behavior an
    /// `mpsc` cannot provide.
    fn push_drop_oldest(&self, chunk: Outbound) -> Result<(), CallerError> {
        let mut g = self.inner.lock().expect("outbound queue poisoned");
        if g.closed {
            return Err(CallerError::Transport("outbound queue closed".into()));
        }
        if Self::chunk_count(&g) >= self.capacity {
            // Drop the oldest CHUNK (skip terminals, which must survive).
            if let Some(pos) = g.items.iter().position(|o| matches!(o, Outbound::Chunk(_))) {
                g.items.remove(pos);
            }
        }
        g.items.push_back(chunk);
        drop(g);
        self.notify.notify_waiters();
        Ok(())
    }

    /// Enqueue a terminal (`Terminate`/`Error`). Always lands (the exchange
    /// must be able to end regardless of the chunk backlog). Best-effort:
    /// silent if the queue already closed (the socket is gone anyway).
    fn push_terminal(&self, item: Outbound) {
        {
            let mut g = self.inner.lock().expect("outbound queue poisoned");
            if g.closed {
                return;
            }
            g.items.push_back(item);
        }
        self.notify.notify_waiters();
    }

    /// Drainer: pop the front, waiting when empty. Returns `None` once the
    /// queue is closed AND drained (socket task should then end).
    async fn recv(&self) -> Option<Outbound> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().expect("outbound queue poisoned");
                if let Some(item) = g.items.pop_front() {
                    drop(g);
                    // Wake a Block-mode producer that may be waiting for the
                    // slot we just freed.
                    self.notify.notify_waiters();
                    return Some(item);
                }
                if g.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    /// Mark closed and wake everyone (drainer ends, blocked producers error).
    fn close(&self) {
        self.inner.lock().expect("outbound queue poisoned").closed = true;
        self.notify.notify_waiters();
    }
}

/// Production live caller connection. Shared (`Arc`) across the
/// concurrently-running nodes of one execution.
pub struct LiveCallerConnection {
    config: CallerRuntimeConfig,
    /// Outbound to the socket task. The producer side of the single-consumer
    /// `OutboundQueue`; it closes when the socket task ends.
    outbound: Arc<OutboundQueue>,
    /// Inbound stored log (WebSocket only): every decoded inbound message
    /// in arrival order. `receive()` reads from a per-reader cursor over
    /// this log, so a node that subscribes slightly after the first frame
    /// still sees it (no lost-message race that a raw broadcast has). This
    /// is the same offset-cursor model the bus uses. `None` for HTTP.
    inbound: Option<InboundLog>,
    /// HTTP request parts captured at attach (HTTP only). `None` for WS.
    http_request: Option<Arc<HttpRequestParts>>,
    /// Journal sink for `Caller*` observability events.
    journal: Arc<dyn CallerJournalSink>,
    color: Color,
    inner: Mutex<ConnInner>,
    /// Monotonic offset for journaled caller events (per connection).
    next_offset: AtomicU64,
    connected: AtomicBool,
}

/// Stored inbound log with a wakeup and a BOUNDED in-RAM window. A
/// `receive` reads the message at a per-reader absolute OFFSET (cursor),
/// waiting on `notify` when the cursor has caught up. Broadcast semantics
/// fall out: each reader has its own cursor over the same window, so every
/// reader sees every message it didn't start past. The window bounds RAM
/// for a long-lived high-volume socket; messages trimmed out of the window
/// are gone for cursors (cursors never read the DB, matching the bus). The
/// caller's journal sink persists each inbound for durability/replay
/// independently of this window.
#[derive(Clone)]
pub(crate) struct InboundLog {
    inner: Arc<Mutex<InboundInner>>,
    notify: Arc<tokio::sync::Notify>,
    /// Set true when the socket closes; a reader caught up past the end of
    /// a closed log gets a disconnect rather than blocking forever.
    closed: Arc<AtomicBool>,
}

/// The three outcomes of reading at an absolute offset (see `read_at`).
enum InboundRead {
    Got(InboundMessage),
    OutOfWindow { oldest_resident: u64 },
    NotYet,
}

struct InboundInner {
    /// Retained messages, front = `base_offset`. `offset N` lives at index
    /// `N - base_offset` when `base_offset <= N < base_offset + len`.
    msgs: std::collections::VecDeque<InboundMessage>,
    /// Absolute offset of `msgs.front()` (0 when empty/never-trimmed).
    base_offset: u64,
    /// One past the highest offset ever pushed (the "now" offset). Grows
    /// monotonically; unaffected by trimming.
    next_offset: u64,
    /// Max retained messages. The oldest are evicted past this.
    window: usize,
}

impl InboundLog {
    fn new(window: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(InboundInner {
                msgs: std::collections::VecDeque::new(),
                base_offset: 0,
                next_offset: 0,
                window: window.max(1),
            })),
            notify: Arc::new(tokio::sync::Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
    /// Append an inbound message, evict past the window, wake parked readers.
    fn push(&self, msg: InboundMessage) {
        {
            let mut g = self.inner.lock().expect("inbound log poisoned");
            g.msgs.push_back(msg);
            g.next_offset += 1;
            while g.msgs.len() > g.window {
                g.msgs.pop_front();
                g.base_offset += 1;
            }
        }
        self.notify.notify_waiters();
    }
    /// Mark the inbound side closed and wake readers so they unblock.
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
    /// Read at absolute `offset`. Offsets are absolute over the whole
    /// connection lifetime (never relative to the moving window), so a
    /// stored cursor offset means the same message forever. Three outcomes,
    /// no silent clamping:
    ///   - `Got(msg)`: the message at `offset` is resident.
    ///   - `OutOfWindow { oldest_resident }`: `offset` fell behind the
    ///     window (evicted; cursors never read the DB). The node decides
    ///     what to do; `oldest_resident` is absolute, valid to re-seed with.
    ///   - `NotYet`: `offset` is at/past the current end; wait for arrival.
    fn read_at(&self, offset: u64) -> InboundRead {
        let g = self.inner.lock().expect("inbound log poisoned");
        if offset < g.base_offset {
            return InboundRead::OutOfWindow { oldest_resident: g.base_offset };
        }
        if offset >= g.next_offset {
            return InboundRead::NotYet;
        }
        let idx = (offset - g.base_offset) as usize;
        InboundRead::Got(g.msgs.get(idx).cloned().expect("resident offset present"))
    }
    fn now_offset(&self) -> u64 {
        self.inner.lock().expect("inbound log poisoned").next_offset
    }
    fn retained_floor(&self) -> u64 {
        self.inner.lock().expect("inbound log poisoned").base_offset
    }
    fn last_offset(&self) -> Option<u64> {
        let g = self.inner.lock().expect("inbound log poisoned");
        g.next_offset.checked_sub(1).filter(|o| *o >= g.base_offset)
    }
}

struct ConnInner {
    terminated: bool,
}

impl LiveCallerConnection {
    fn next_offset(&self) -> u64 {
        self.next_offset.fetch_add(1, Ordering::SeqCst)
    }

    /// Surface a node/run error to the caller per the error mode. Best
    /// effort: records the `CallerErrored` event and pushes an `Error`
    /// outbound (the socket task turns it into an in-band chunk for HTTP
    /// after streaming started, or a WS close frame with the reason). Used
    /// by the execute path when a live-connection run fails with the
    /// caller still attached, so the caller learns why instead of seeing a
    /// silently dropped socket.
    pub async fn surface_error(&self, message: &str) {
        if self.config.error_mode == weft_core::signal::ErrorMode::DropChunk {
            // Tolerant streams: the chosen mode says swallow it. Still
            // journal it (observability), just don't push to the wire.
            let offset = self.next_offset();
            self.journal.errored(self.color, offset, message);
            return;
        }
        let offset = self.next_offset();
        self.journal.errored(self.color, offset, message);
        self.outbound.push_terminal(Outbound::Error(message.to_string()));
    }

    /// Resolve a gone-caller talk into the policy-correct outcome (cancel
    /// vs void), identical to the fake's contract.
    fn disconnected_outcome(&self) -> Result<(), CallerError> {
        match resolve_disconnect(self.config.suspend) {
            DisconnectAction::ContinueIntoVoid => Ok(()),
            DisconnectAction::CancelExecution => Err(CallerError::Disconnected),
        }
    }
}

#[async_trait]
impl CallerConnection for LiveCallerConnection {
    fn config(&self) -> &CallerRuntimeConfig {
        &self.config
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    async fn ensure_connected(&self) -> Result<(), CallerError> {
        // The socket is attached at construction (the server builds the
        // connection only once the caller's socket is in hand), so the
        // common case is already-connected. If the caller has since
        // dropped, surface the resolved policy outcome (cancel -> error,
        // keep-running -> Ok into the void).
        if self.is_connected() {
            Ok(())
        } else {
            self.disconnected_outcome()
        }
    }

    async fn send_chunk(&self, chunk: OutboundChunk) -> Result<(), CallerError> {
        if !self.is_connected() {
            return self.disconnected_outcome();
        }
        let offset = self.next_offset();
        self.journal.outbound(self.color, offset, &chunk, false);
        let chunk = Outbound::Chunk(chunk);
        match self.config.backpressure {
            // Await a free slot; only errors if the socket is gone.
            Backpressure::Block => self.outbound.push_block(chunk).await,
            // Shed the incoming chunk if the queue is full; keep what's queued.
            Backpressure::DropNewest => self.outbound.push_drop_newest(chunk),
            // Evict the oldest queued chunk to make room, then enqueue. Real
            // drop-oldest: the producer owns the buffer (see `OutboundQueue`).
            Backpressure::DropOldest => self.outbound.push_drop_oldest(chunk),
        }
    }

    async fn terminate(&self, final_chunk: Option<OutboundChunk>) -> Result<(), CallerError> {
        {
            let mut g = self.inner.lock().expect("caller conn poisoned");
            try_terminate(g.terminated)?;
            g.terminated = true;
        }
        let offset = self.next_offset();
        if let Some(c) = &final_chunk {
            self.journal.outbound(self.color, offset, c, true);
        } else {
            self.journal.disconnected(self.color, offset, "response complete");
        }
        // The terminal always lands (subject to no capacity policy); if the
        // socket task already ended (caller gone), it is silently dropped
        // (the exchange is over anyway).
        self.outbound.push_terminal(Outbound::Terminate(final_chunk));
        Ok(())
    }

    async fn receive(
        &self,
        cursor: &std::sync::atomic::AtomicU64,
    ) -> Result<InboundMessage, CallerError> {
        let log = self.inbound.as_ref().ok_or(CallerError::WrongProtocol {
            protocol: self.config.protocol.as_wire_str(),
        })?;
        recv_from_log(log, cursor).await
    }

    async fn request(
        &self,
        msg: OutboundChunk,
        cursor: &std::sync::atomic::AtomicU64,
    ) -> Result<InboundMessage, CallerError> {
        // No subscribe race: the cursor reads from a stored log, so a reply
        // landing between send and read is still at/after the cursor.
        self.send_chunk(msg).await?;
        let log = self.inbound.as_ref().ok_or(CallerError::WrongProtocol {
            protocol: self.config.protocol.as_wire_str(),
        })?;
        recv_from_log(log, cursor).await
    }

    fn http_request(&self) -> Result<Arc<HttpRequestParts>, CallerError> {
        self.http_request.clone().ok_or(CallerError::WrongProtocol {
            protocol: self.config.protocol.as_wire_str(),
        })
    }

    fn inbound_now_offset(&self) -> u64 {
        self.inbound.as_ref().map(|l| l.now_offset()).unwrap_or(0)
    }

    fn inbound_attach_offset(&self) -> u64 {
        // The inbound log is created empty when the socket attaches (this
        // connection is built only once the caller's socket is in hand), so
        // the attach point is offset 0. A built-in forward cursor therefore
        // sees every message sent on this connection, including one that
        // lands before the node's first read (no subscribe race). If a slow
        // node lets the window trim past offset 0 before reading, its first
        // read surfaces `OutOfWindow` (not a silent clamp) so it can decide.
        0
    }

    fn inbound_retained_floor(&self) -> u64 {
        self.inbound.as_ref().map(|l| l.retained_floor()).unwrap_or(0)
    }

    fn last_inbound_offset(&self) -> Option<u64> {
        self.inbound.as_ref().and_then(|l| l.last_offset())
    }
}

/// Read the message at `cursor` from the stored inbound log, waiting on
/// the log's notify until one arrives. Advances `cursor` by one on success.
/// A closed log with the cursor caught up is a disconnect (caller gone).
/// Arm the notify BEFORE the catch-up check so a message landing in the gap
/// still wakes us (no lost wakeup).
///
/// The wait is UNBOUNDED on purpose: a node parking for the next caller
/// message can legitimately wait minutes or hours (a chat user thinking),
/// and Weft never times out a user-controlled wait. The only things that
/// end this wait are a message arriving, the caller disconnecting, or the
/// session-cap firing (which closes the log -> a disconnect here). The
/// connect timeout bounds only `wait_for_attach`, never an inbound read.
async fn recv_from_log(
    log: &InboundLog,
    cursor: &std::sync::atomic::AtomicU64,
) -> Result<InboundMessage, CallerError> {
    loop {
        let notified = log.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let want = cursor.load(Ordering::SeqCst);
        match log.read_at(want) {
            // Got it: advance the cursor past the absolute offset read.
            InboundRead::Got(m) => {
                cursor.store(want + 1, Ordering::SeqCst);
                return Ok(m);
            }
            // Fell behind the window: surface the typed outcome (no silent
            // substitution) and MOVE the cursor to the next retained message
            // so the next `receive()` resumes there, identical to the bus's
            // `FellBehind`. The caller's inbound log is dense (no membership
            // entries), so the next retained message AT/AFTER a below-floor
            // cursor IS the floor: `resumed_at == oldest_resident`. Both
            // absolute, stable as the window slides.
            InboundRead::OutOfWindow { oldest_resident } => {
                cursor.store(oldest_resident, Ordering::SeqCst);
                return Err(CallerError::FellBehind { oldest_resident });
            }
            // Not yet arrived: fall through to wait on the notify.
            InboundRead::NotYet => {}
        }
        if log.closed.load(Ordering::SeqCst) {
            return Err(CallerError::Disconnected);
        }
        notified.await;
    }
}

/// A future that resolves once the session cap elapses, or NEVER when the
/// cap is `0` (no cap). The single legitimate deadline on a live exchange:
/// per-message waits are unbounded, but the author can bound the TOTAL
/// session via `max_session_secs` to cap a multiplexing pod's RAM/abuse.
/// Uses the injected clock so the rig can advance it deterministically.
async fn session_deadline(clock: &Arc<dyn weft_platform_traits::Clock>, cap_secs: u64) {
    if cap_secs == 0 {
        std::future::pending::<()>().await;
    } else {
        clock.sleep(std::time::Duration::from_secs(cap_secs)).await;
    }
}

/// Sink for the `Caller*` observability events. The engine wires the
/// real journal-backed impl; tests pass a recording fake. Mirrors the
/// bus journal pump's projection (connect / inbound / outbound / error /
/// disconnect, each with an offset).
pub trait CallerJournalSink: Send + Sync {
    fn connected(&self, color: Color, offset: u64, protocol: Protocol);
    fn inbound(&self, color: Color, offset: u64, msg: &InboundMessage);
    fn outbound(&self, color: Color, offset: u64, chunk: &OutboundChunk, terminal: bool);
    fn errored(&self, color: Color, offset: u64, message: &str);
    fn disconnected(&self, color: Color, offset: u64, reason: &str);
}

/// Per-worker registry mapping an execution color to its attached live
/// connection. The connection server inserts on attach; the loop driver
/// (`run_one_execution`) reads the connection for a color to wire into
/// `ctx.caller()`; removal happens when the socket task ends.
///
/// Cross-pod note: this is pod-local RAM, which is correct because a live
/// connection is pinned to ONE pod for its life (the routing token names
/// the pod), so the connection for a color only ever exists on the one
/// worker that accepted it.
#[derive(Clone, Default)]
pub struct CallerRegistry {
    inner: Arc<Mutex<HashMap<Color, Arc<LiveCallerConnection>>>>,
    /// Woken on every `attach`. The execute path awaits this when its
    /// caller has not arrived yet (the dispatcher starts the execution
    /// before, or racing with, the caller's socket attaching).
    attached: Arc<tokio::sync::Notify>,
}

impl CallerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn attach(&self, color: Color, conn: Arc<LiveCallerConnection>) {
        self.inner.lock().expect("registry poisoned").insert(color, conn);
        // `notify_waiters` (not `notify_one`): several execute paths may be
        // waiting for distinct colors; wake them all to re-check.
        self.attached.notify_waiters();
    }

    /// Await the connection for `color` to attach, bounded by `timeout`.
    /// Returns the connection once attached, or `None` on timeout (the
    /// caller never arrived; the execute path treats that as "no caller"
    /// and proceeds, and the caller handle's `ensure_connected()` then fails
    /// loud if a node actually needs the caller). Arming the `Notify` future
    /// BEFORE the map check closes the attach-between-check-and-wait race.
    pub async fn wait_for_attach(
        &self,
        color: Color,
        timeout: std::time::Duration,
    ) -> Option<Arc<LiveCallerConnection>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.attached.notified();
            tokio::pin!(notified);
            // Arm, THEN check: an attach landing now wakes the armed future.
            notified.as_mut().enable();
            if let Some(conn) = self.get(color) {
                return Some(conn);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.get(color); // last check at deadline
            }
        }
    }

    pub fn get(&self, color: Color) -> Option<Arc<LiveCallerConnection>> {
        self.inner.lock().expect("registry poisoned").get(&color).cloned()
    }

    pub fn detach(&self, color: Color) {
        self.inner.lock().expect("registry poisoned").remove(&color);
    }

    /// Mark a connection's caller as gone (socket task ended) and emit the
    /// disconnect journal event. Idempotent.
    pub fn mark_disconnected(&self, color: Color, reason: &str) {
        if let Some(conn) = self.get(color) {
            if conn.connected.swap(false, Ordering::SeqCst) {
                let offset = conn.next_offset();
                conn.journal.disconnected(color, offset, reason);
            }
        }
    }
}

/// Build a connection + the socket-facing channels. Returns the shared
/// `Arc<LiveCallerConnection>` (registered + handed to the driver) and the
/// halves the socket task drives: the outbound receiver (drain to wire)
/// and the inbound log (push decoded messages, close on socket end).
#[allow(clippy::type_complexity)]
pub(crate) fn new_connection(
    config: CallerRuntimeConfig,
    color: Color,
    http_request: Option<Arc<HttpRequestParts>>,
    journal: Arc<dyn CallerJournalSink>,
) -> (
    Arc<LiveCallerConnection>,
    Arc<OutboundQueue>,
    Option<InboundLog>,
) {
    let outbound = OutboundQueue::new(OUTBOUND_BUFFER);
    let inbound = match config.protocol {
        Protocol::Websocket => Some(InboundLog::new(config.inbound_window)),
        Protocol::Http => None,
    };
    let conn = Arc::new(LiveCallerConnection {
        config,
        outbound: outbound.clone(),
        inbound: inbound.clone(),
        http_request,
        journal: journal.clone(),
        color,
        inner: Mutex::new(ConnInner { terminated: false }),
        next_offset: AtomicU64::new(0),
        connected: AtomicBool::new(true),
    });
    // Connect event at offset 0 is stamped by the caller of this fn (the
    // server) once it has registered, so the journal ordering matches the
    // attach ordering; expose the protocol for that.
    let proto = conn.config.protocol;
    let off = conn.next_offset();
    journal.connected(color, off, proto);
    (conn, outbound, inbound)
}

// ----- Wire codec (data-type adapt) ----------------------------------

/// Encode an outbound chunk to a websocket frame per the declared data
/// type. JSON/text ride as Text frames; bytes as Binary.
fn chunk_to_ws(chunk: &OutboundChunk) -> Message {
    match chunk {
        OutboundChunk::Json(v) => Message::Text(v.to_string().into()),
        OutboundChunk::Text(s) => Message::Text(s.clone().into()),
        OutboundChunk::Bytes(b) => Message::Binary(b.clone().into()),
    }
}

/// Encode an outbound chunk to HTTP body bytes per the declared data type.
fn chunk_to_bytes(chunk: &OutboundChunk) -> Vec<u8> {
    match chunk {
        OutboundChunk::Json(v) => v.to_string().into_bytes(),
        OutboundChunk::Text(s) => s.clone().into_bytes(),
        OutboundChunk::Bytes(b) => b.clone(),
    }
}

/// Decode raw inbound bytes into an `InboundMessage` per the declared
/// data type. JSON parses (fails loud on bad JSON); text is UTF-8
/// (lossless required); bytes pass through.
fn decode_inbound(data_type: DataType, raw: &[u8]) -> Result<InboundMessage, String> {
    match data_type {
        DataType::Json => serde_json::from_slice(raw)
            .map(InboundMessage::Json)
            .map_err(|e| format!("inbound is not valid JSON: {e}")),
        DataType::Text => String::from_utf8(raw.to_vec())
            .map(InboundMessage::Text)
            .map_err(|_| "inbound is not valid UTF-8 text".to_string()),
        DataType::Bytes => Ok(InboundMessage::Bytes(raw.to_vec())),
    }
}

// ----- The worker connection server ----------------------------------

/// Shared state for the connection server: the registry it attaches into,
/// the per-color runtime config + journal factory, and the token secret.
#[derive(Clone)]
pub struct ConnServerState {
    pub registry: CallerRegistry,
    /// Verifies the dispatcher-signed routing token.
    pub token_secret: Arc<Vec<u8>>,
    /// This pod's name; a token addressed to another pod is rejected
    /// (per-pod pinning, option A).
    pub pod_name: String,
    /// Resolves the per-color runtime config + journal sink. Set by
    /// `run_pod` from the execution's signal config; the server needs the
    /// config (protocol, caps, data type) to build the connection, and
    /// the journal sink to record the exchange. Keyed by color.
    pub resolver: Arc<dyn ConnConfigResolver>,
    /// Worker clock (for the now()-based session deadline / heartbeat).
    pub clock: Arc<dyn weft_platform_traits::Clock>,
    /// Fires the per-execution cancel flag (cancel-on-disconnect for a
    /// caller-tied run). Looked up by color.
    pub canceller: Arc<dyn ExecutionCanceller>,
}

/// How the server learns a color's connection config + journal sink.
/// `run_pod` implements this over the worker's per-execution state.
pub trait ConnConfigResolver: Send + Sync {
    /// `Some((config, heartbeat_secs, journal))` when `color` is a live
    /// execution expecting a caller; `None` for an unknown/expired color
    /// (the server rejects the connection loud).
    fn resolve(
        &self,
        color: Color,
    ) -> Option<(CallerRuntimeConfig, u64, Arc<dyn CallerJournalSink>)>;
}

/// Fires the per-execution cancel flag (cancel-on-disconnect). `run_pod`
/// implements this over its pod-local cancel registry.
pub trait ExecutionCanceller: Send + Sync {
    fn cancel(&self, color: Color);
}

/// Build the connection server router. The connection is identified by
/// the signed `?wct=<token>` query param, NOT the path: the gateway
/// forwards the caller's ORIGINAL path (e.g. `/chat`, the author's mount
/// path) after stripping the namespace segment, so the worker accepts ANY
/// path via a fallback handler (any method, so HTTP verbs and the WS
/// upgrade GET all land here). `/healthz` is the one reserved path, for
/// the dispatcher's "is the worker routable yet" check.
pub fn connection_router(state: ConnServerState) -> Router {
    Router::new()
        .route("/healthz", any(|| async { StatusCode::OK }))
        .fallback(any(handle_connect))
        .with_state(state)
}

/// Run the connection server until the process exits. Binds `0.0.0.0:port`
/// (plain HTTP/WS; TLS terminates at the gateway). Spawned by `run_pod`.
pub async fn serve(state: ConnServerState, port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(target: "weft_engine::caller_conn", %addr, "connection server listening");
    axum::serve(listener, connection_router(state)).await?;
    Ok(())
}

/// Pull the `wct` token out of a raw query string (`a=b&wct=...&c=d`).
fn token_from_query(raw: &str) -> Option<String> {
    raw.split('&')
        .find_map(|kv| kv.strip_prefix("wct=").map(|v| v.to_string()))
}

/// Single-extractor handler: takes the whole request and pulls method,
/// headers, query, the optional WS upgrade, and the body manually. axum
/// caps handler arity and forbids combining several query/body extractors,
/// so one `Request` is the clean shape here.
async fn handle_connect(
    State(state): State<ConnServerState>,
    request: axum::extract::Request,
) -> Response {
    let method = request.method().clone();
    let headers = request.headers().clone();
    let raw_query = request.uri().query().unwrap_or("").to_string();

    // 1. Verify the dispatcher-signed token + pod pin.
    let Some(token) = token_from_query(&raw_query) else {
        return (StatusCode::UNAUTHORIZED, "missing routing token").into_response();
    };
    let now = state.clock.now_unix();
    let claims = match caller_token::validate(&state.token_secret, &token, now) {
        Ok(c) => c,
        Err(e) => return (StatusCode::UNAUTHORIZED, format!("bad routing token: {e}")).into_response(),
    };
    if claims.pod_name != state.pod_name {
        // Per-pod pinning: this connection was signed for another pod.
        return (StatusCode::FORBIDDEN, "routing token addressed to a different pod").into_response();
    }
    let color = claims.color;

    // 2. Resolve the execution's connection config (protocol, caps, data
    //    type) + journal sink.
    //
    // A valid, pod-pinned token is PROOF the dispatcher admitted this color
    // to THIS worker (it minted the token only after inserting the pinned
    // execute task). But the worker only populates its resolver when it
    // CLAIMS and starts that task, a beat after admission. So a caller that
    // the dispatcher redirected here can briefly arrive before the resolver
    // is populated. That is "not ready yet," not "unknown": poll the
    // resolver for a bounded window before giving up. Without this, a fast
    // caller racing the worker's task-claim gets a spurious 404 even though
    // the execution is genuinely starting. (A token for a color this worker
    // never gets assigned simply times out to the same 404, which is
    // correct: nothing will ever attach.)
    let resolved = {
        const READY_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
        const POLL: std::time::Duration = std::time::Duration::from_millis(50);
        let deadline = state.clock.now() + READY_WAIT;
        loop {
            if let Some(r) = state.resolver.resolve(color) {
                break Some(r);
            }
            if state.clock.now() >= deadline {
                break None;
            }
            state.clock.sleep(POLL).await;
        }
    };
    let Some((config, heartbeat_secs, journal)) = resolved else {
        return (StatusCode::NOT_FOUND, "no live execution for this token").into_response();
    };

    // 3. Branch on protocol. The connection layer is shared; only the
    //    socket wiring differs. Split the request so we can both attempt
    //    the WS upgrade (from the parts) and read the body (for HTTP).
    let (mut parts, body) = request.into_parts();
    match config.protocol {
        Protocol::Websocket => {
            // Try to extract the upgrade from the request parts. A
            // websocket trigger hit without an upgrade header is a misuse.
            match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
                Ok(upgrade) => {
                    tracing::info!(
                        target: "weft_engine::caller_conn",
                        color = %color, "ws upgrade accepted; attaching"
                    );
                    let st = state.clone();
                    // Enforce the inbound size cap at the TRANSPORT so an
                    // oversized frame is rejected before axum buffers it whole
                    // (the per-message check in `drive_ws` is the loud surface,
                    // not the RAM bound). `usize` cast is safe: the cap is a
                    // byte count that fits the platform word on any real pod.
                    let cap = config.max_inbound_bytes as usize;
                    let upgrade = upgrade.max_message_size(cap).max_frame_size(cap);
                    upgrade.on_upgrade(move |socket| {
                        drive_ws(socket, st, color, config, heartbeat_secs, journal)
                    })
                }
                Err(e) => {
                    tracing::warn!(
                        target: "weft_engine::caller_conn",
                        color = %color, error = ?e, "ws upgrade extraction failed"
                    );
                    (
                        StatusCode::BAD_REQUEST,
                        "websocket trigger requires a WebSocket upgrade",
                    )
                        .into_response()
                }
            }
        }
        Protocol::Http => {
            drive_http(state, color, config, heartbeat_secs, journal, method, headers, raw_query, body)
                .await
        }
    }
}

/// HTTP path: read the (capped) request body, build the connection with
/// the request parts, attach it, then stream the worker's outbound chunks
/// back as a chunked response body. The node's `respond`/`write`/`close`
/// drive what the caller receives.
#[allow(clippy::too_many_arguments)]
async fn drive_http(
    state: ConnServerState,
    color: Color,
    config: CallerRuntimeConfig,
    heartbeat_secs: u64,
    journal: Arc<dyn CallerJournalSink>,
    method: Method,
    headers: HeaderMap,
    raw_query: String,
    body: axum::body::Body,
) -> Response {
    // Enforce the inbound size cap while reading the body (untrusted
    // caller); fail loud past the cap.
    let limit = config.max_inbound_bytes;
    let bytes = match axum::body::to_bytes(body, limit as usize).await {
        Ok(b) => b,
        Err(_) => {
            return (StatusCode::PAYLOAD_TOO_LARGE, format!("request body exceeds {limit} bytes"))
                .into_response()
        }
    };
    if let Err(e) = weft_core::caller::check_inbound_size(bytes.len() as u64, limit) {
        return (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response();
    }
    let decoded = match decode_inbound(config.data_type, &bytes) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let parts = Arc::new(HttpRequestParts {
        method: method.to_string(),
        path: String::new(), // the author route is the gateway's concern; the worker sees the body
        query: raw_query,
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
            .collect(),
        body: decoded,
    });

    let (conn, outbound, _inb) = new_connection(config.clone(), color, Some(parts), journal);
    state.registry.attach(color, conn.clone());

    // Stream the worker's outbound chunks as a chunked HTTP body. The
    // first Terminate ends the stream. A keep-alive trickle is not needed
    // here: an HTTP response that hasn't started streaming holds the
    // socket open at the gateway via the pending response; once chunks
    // flow, they ARE the activity. (The heartbeat arg is consulted for WS;
    // for HTTP a long pre-first-byte think relies on the gateway's
    // request timeout being set generously, configured on the gateway.)
    let _ = heartbeat_secs;
    let registry = state.registry.clone();
    let canceller = state.canceller.clone();
    let policy = config.suspend;
    let clock = state.clock.clone();
    let max_session_secs = config.max_session_secs;
    let (tx, rx) = mpsc::channel::<Result<Vec<u8>, std::io::Error>>(16);
    tokio::spawn(async move {
        let session = session_deadline(&clock, max_session_secs);
        tokio::pin!(session);
        let reason = loop {
            tokio::select! {
                out = outbound.recv() => match out {
                    Some(Outbound::Chunk(c)) => {
                        if tx.send(Ok(chunk_to_bytes(&c))).await.is_err() {
                            break "caller hung up";
                        }
                    }
                    Some(Outbound::Terminate(final_chunk)) => {
                        if let Some(c) = final_chunk {
                            let _ = tx.send(Ok(chunk_to_bytes(&c))).await;
                        }
                        break "response complete";
                    }
                    Some(Outbound::Error(msg)) => {
                        // Error mode: after streaming started we can only send
                        // in-band then close (status already committed).
                        let _ = tx.send(Ok(format!("\n[error] {msg}").into_bytes())).await;
                        break "response errored";
                    }
                    None => break "outbound queue closed",
                },
                // Session cap: a configured `max_session_secs` ceiling on the
                // total exchange (0 = no cap, the future never resolves). The
                // ONLY deadline on a live exchange; per-message waits are
                // unbounded (a node may legitimately wait hours).
                _ = &mut session => break "session cap exceeded",
            }
        };
        // The exchange ended: stop producers (a blocked send now errors) and
        // mark the caller gone for this run.
        outbound.close();
        registry.mark_disconnected(color, reason);
        registry.detach(color);
        if matches!(resolve_disconnect(policy), DisconnectAction::CancelExecution) {
            canceller.cancel(color);
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = axum::body::Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .header("X-Accel-Buffering", "no") // ask proxies not to buffer the stream
        .body(body)
        .expect("response builds")
}

/// WebSocket path: bridge the socket to the connection. Spawns the read
/// pump (decode caller frames -> broadcast inbound), the write pump (drain
/// outbound -> frames), and the heartbeat (ping on a timer).
async fn drive_ws(
    mut socket: WebSocket,
    state: ConnServerState,
    color: Color,
    config: CallerRuntimeConfig,
    heartbeat_secs: u64,
    journal: Arc<dyn CallerJournalSink>,
) {
    let (conn, outbound, inbound) = new_connection(config.clone(), color, None, journal.clone());
    let inbound = inbound.expect("websocket connection has an inbound channel");
    state.registry.attach(color, conn.clone());

    let data_type = config.data_type;
    let max_inbound = config.max_inbound_bytes;
    let policy = config.suspend;
    let session = session_deadline(&state.clock, config.max_session_secs);
    tokio::pin!(session);

    // Single task owns the socket (recv + send are on one WebSocket).
    // Outbound chunks and heartbeat pings funnel through a select. Build the
    // heartbeat ticker ONLY when a heartbeat is configured; with none, the
    // arm is disabled and no phantom ticker is constructed.
    let mut heartbeat = (heartbeat_secs != 0).then(|| {
        let mut iv = tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        iv
    });

    let reason = loop {
        tokio::select! {
            // Caller -> program.
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(t))) => {
                    if t.len() as u64 > max_inbound {
                        break "inbound message exceeded size cap";
                    }
                    match decode_inbound(data_type, t.as_bytes()) {
                        Ok(msg) => {
                            let off = conn.next_offset();
                            journal.inbound(color, off, &msg);
                            inbound.push(msg);
                        }
                        Err(_) => break "inbound decode failed",
                    }
                }
                Some(Ok(Message::Binary(b))) => {
                    if b.len() as u64 > max_inbound {
                        break "inbound message exceeded size cap";
                    }
                    match decode_inbound(data_type, &b) {
                        Ok(msg) => {
                            let off = conn.next_offset();
                            journal.inbound(color, off, &msg);
                            inbound.push(msg);
                        }
                        Err(_) => break "inbound decode failed",
                    }
                }
                Some(Ok(Message::Close(_))) => break "caller closed the socket",
                Some(Ok(_)) => { /* ping/pong handled by axum */ }
                Some(Err(_)) => break "socket transport error",
                None => break "socket ended",
            },
            // Program -> caller.
            out = outbound.recv() => match out {
                Some(Outbound::Chunk(c)) => {
                    if socket.send(chunk_to_ws(&c)).await.is_err() {
                        break "caller hung up on send";
                    }
                }
                Some(Outbound::Terminate(final_chunk)) => {
                    if let Some(c) = final_chunk {
                        let _ = socket.send(chunk_to_ws(&c)).await;
                    }
                    let _ = socket.send(Message::Close(None)).await;
                    break "session closed by program";
                }
                Some(Outbound::Error(msg)) => {
                    let _ = socket.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1011, // internal error
                        reason: msg.into(),
                    }))).await;
                    break "session errored by program";
                }
                None => break "outbound queue closed",
            },
            // Keep-alive ping (worker-side; browsers can't ping us). The arm
            // only exists when a heartbeat is configured (`heartbeat` is
            // `Some`); otherwise it is permanently disabled.
            _ = async { heartbeat.as_mut().unwrap().tick().await }, if heartbeat.is_some() => {
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break "caller missed heartbeat";
                }
            }
            // Session cap: the one deadline on a live exchange (per-message
            // waits are unbounded). `0` = no cap (the future never resolves).
            _ = &mut session => break "session cap exceeded",
        }
    };

    // Wake any node parked in receive() so it unblocks (the log is now
    // closed; a caught-up reader gets a disconnect) and any producer blocked
    // on a full outbound queue (its send now errors).
    inbound.close();
    outbound.close();
    state.registry.mark_disconnected(color, reason);
    state.registry.detach(color);
    if matches!(resolve_disconnect(policy), DisconnectAction::CancelExecution) {
        state.canceller.cancel(color);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::caller::CallerHandle;
    use weft_core::signal::DataType;
    use weft_core::wait::SuspendPolicy;

    /// Recording journal sink: appends every event so tests assert the
    /// observable exchange was journaled in order.
    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<String>>,
    }
    impl RecordingSink {
        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }
    impl CallerJournalSink for RecordingSink {
        fn connected(&self, _c: Color, off: u64, _p: Protocol) {
            self.events.lock().unwrap().push(format!("connected@{off}"));
        }
        fn inbound(&self, _c: Color, off: u64, _m: &InboundMessage) {
            self.events.lock().unwrap().push(format!("inbound@{off}"));
        }
        fn outbound(&self, _c: Color, off: u64, _ch: &OutboundChunk, terminal: bool) {
            self.events.lock().unwrap().push(format!("outbound@{off}:term={terminal}"));
        }
        fn errored(&self, _c: Color, off: u64, _m: &str) {
            self.events.lock().unwrap().push(format!("errored@{off}"));
        }
        fn disconnected(&self, _c: Color, off: u64, _r: &str) {
            self.events.lock().unwrap().push(format!("disconnected@{off}"));
        }
    }

    fn ws_cfg() -> CallerRuntimeConfig {
        CallerRuntimeConfig {
            protocol: Protocol::Websocket,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: weft_core::signal::ErrorMode::Surface,
            connect_timeout_secs: 1,
            max_inbound_bytes: 1024,
            max_session_secs: 0,
            suspend: SuspendPolicy { can_suspend: false, default_hold_secs: 300 },
            inbound_window: 4,
        }
    }

    #[tokio::test]
    async fn outbound_chunks_reach_the_socket_channel_and_journal() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, out_rx, _inb) =
            new_connection(ws_cfg(), Color::nil(), None, sink.clone());
        let handle = CallerHandle::from_connection(conn.clone());
        let CallerHandle::Websocket(ws) = handle else { unreachable!() };
        ws.send(OutboundChunk::Json(serde_json::json!("hi"))).await.unwrap();
        ws.close().await.unwrap();
        // Socket task would drain these:
        let first = out_rx.recv().await.expect("chunk");
        assert!(matches!(first, Outbound::Chunk(_)));
        let term = out_rx.recv().await.expect("terminate");
        assert!(matches!(term, Outbound::Terminate(None)));
        // Journal saw connect, the outbound chunk, and the disconnect on close.
        let ev = sink.events();
        assert!(ev[0].starts_with("connected@0"), "got {ev:?}");
        assert!(ev.iter().any(|e| e.starts_with("outbound@")), "got {ev:?}");
        assert!(ev.iter().any(|e| e.starts_with("disconnected@")), "got {ev:?}");
    }

    #[tokio::test]
    async fn inbound_broadcasts_to_every_listener() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out_rx, inbound) =
            new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        let h1 = CallerHandle::from_connection(conn.clone());
        let h2 = CallerHandle::from_connection(conn.clone());
        let (CallerHandle::Websocket(a), CallerHandle::Websocket(b)) = (h1, h2) else {
            unreachable!()
        };
        // Both handles were built BEFORE this push, so their forward cursors
        // (pinned at attach == offset 0) see the message that arrives next.
        // Each reader has its own cursor: both get a copy, neither steals.
        inbound.push(InboundMessage::Json(serde_json::json!("ping")));
        let ra = tokio::spawn(async move { a.receive().await });
        let rb = tokio::spawn(async move { b.receive().await });
        let (va, vb) = (ra.await.unwrap().unwrap(), rb.await.unwrap().unwrap());
        assert_eq!(va, InboundMessage::Json(serde_json::json!("ping")));
        assert_eq!(vb, InboundMessage::Json(serde_json::json!("ping")));
    }

    #[tokio::test]
    async fn builtin_cursor_pins_at_attach_no_subscribe_race() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        // A message arrives AFTER attach (offset 0) but BEFORE the node
        // builds its handle / first reads. The built-in cursor pins at the
        // ATTACH offset (0), not at handle-build time, so this message is
        // still seen: the subscribe race is closed.
        inbound.push(InboundMessage::Json(serde_json::json!("opener")));
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        let got = ws.receive().await.unwrap();
        assert_eq!(got, InboundMessage::Json(serde_json::json!("opener")),
            "the built-in cursor pins at attach, so a message between connect and \
             first read is not missed");
    }

    #[tokio::test]
    async fn cursor_from_start_reads_retained_history() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        inbound.push(InboundMessage::Json(serde_json::json!("a")));
        inbound.push(InboundMessage::Json(serde_json::json!("b")));
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        // cursor_from_start reaches back over the retained window.
        let cursor = ws.cursor_from_start();
        assert_eq!(cursor.receive().await.unwrap(), InboundMessage::Json(serde_json::json!("a")));
        assert_eq!(cursor.receive().await.unwrap(), InboundMessage::Json(serde_json::json!("b")));
    }

    #[tokio::test]
    async fn inbound_window_trims_and_below_floor_falls_behind() {
        // ws_cfg() sets inbound_window = 4. Push 6: the oldest 2 are evicted.
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        for i in 0..6 {
            inbound.push(InboundMessage::Json(serde_json::json!(i)));
        }
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        // Absolute offsets: now == 6, retained floor == 2 (0,1 evicted).
        assert_eq!(ws.now_offset(), 6);
        assert_eq!(ws.retained_floor(), 2);
        // A cursor at absolute offset 0 does NOT silently substitute a
        // message: it surfaces `FellBehind` (the SAME contract as the bus),
        // carrying the absolute oldest_resident, AND advances the cursor to
        // the floor so the NEXT receive resumes there.
        let cursor = ws.cursor_at(0);
        match cursor.receive().await.expect_err("offset 0 was evicted") {
            // One field: dense log, resume point == window floor == 2.
            CallerError::FellBehind { oldest_resident } => assert_eq!(oldest_resident, 2),
            other => panic!("expected FellBehind, got {other:?}"),
        }
        // Same cursor, called again: resumes at the floor (offset 2 => value 2),
        // identical to the bus's advance-on-fell-behind behavior.
        assert_eq!(cursor.receive().await.unwrap(), InboundMessage::Json(serde_json::json!(2)));
    }

    #[tokio::test]
    async fn cursor_including_last_seeds_most_recent() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        inbound.push(InboundMessage::Json(serde_json::json!("first")));
        inbound.push(InboundMessage::Json(serde_json::json!("latest")));
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        // Includes only the most recent prior message, then is forward.
        let cursor = ws.cursor_including_last();
        assert_eq!(cursor.receive().await.unwrap(), InboundMessage::Json(serde_json::json!("latest")));
    }

    #[tokio::test]
    async fn cursor_at_positions_at_absolute_offset() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        for i in 0..4 {
            inbound.push(InboundMessage::Json(serde_json::json!(i)));
        }
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        // Absolute offsets: now == 4. A cursor at offset 2 reads message 2.
        assert_eq!(ws.now_offset(), 4);
        let c = ws.cursor_at(2);
        assert_eq!(c.receive().await.unwrap(), InboundMessage::Json(serde_json::json!(2)));
        assert_eq!(c.receive().await.unwrap(), InboundMessage::Json(serde_json::json!(3)));
        // `cursor_at(now)` is forward-only: sees only what arrives next.
        let fwd = ws.cursor_at(ws.now_offset());
        inbound.push(InboundMessage::Json(serde_json::json!(99)));
        assert_eq!(fwd.receive().await.unwrap(), InboundMessage::Json(serde_json::json!(99)));
    }

    #[tokio::test]
    async fn now_offset_and_retained_floor_track_window() {
        // window=4: after 7 pushes, now=7, floor=3 (offsets 0,1,2 evicted).
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        for i in 0..7 {
            inbound.push(InboundMessage::Json(serde_json::json!(i)));
        }
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        assert_eq!(ws.now_offset(), 7);
        assert_eq!(ws.retained_floor(), 3, "oldest still in the window=4");
    }

    #[tokio::test]
    async fn request_via_cursor_reads_next_reply() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, out_rx, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        let cursor = ws.cursor();
        // request() sends then reads the next inbound at the cursor. Reply
        // arrives after the handle/cursor exist (real request/reply order).
        inbound.push(InboundMessage::Json(serde_json::json!("pong")));
        let reply = cursor
            .request(OutboundChunk::Json(serde_json::json!("ping")))
            .await
            .unwrap();
        assert_eq!(reply, InboundMessage::Json(serde_json::json!("pong")));
        // The "ping" was actually queued to the socket.
        let sent = out_rx.recv().await.expect("ping queued");
        assert!(matches!(sent, Outbound::Chunk(_)));
    }

    #[tokio::test]
    async fn terminate_once_locks_out_second() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, _inb) = new_connection(ws_cfg(), Color::nil(), None, sink);
        conn.terminate(None).await.expect("first terminal");
        let err = conn.terminate(None).await.expect_err("second rejected");
        assert!(matches!(err, CallerError::AlreadyTerminated));
    }

    /// `receive()` is UNBOUNDED: it never returns `Timeout`. A node parked on
    /// the next message waits indefinitely; the only ways out are a message
    /// arriving or the connection closing (which yields `Disconnected`, NOT
    /// `Timeout`). Regression for the bug where `receive()` was bounded by
    /// the connect timeout and a quiet caller killed the node.
    /// A late message (arriving after the connect timeout would have fired)
    /// is still delivered: proves the wait is genuinely unbounded, not just
    /// "returns Disconnected eventually".
    #[tokio::test]
    async fn receive_delivers_a_late_message() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
        let inbound = inbound.expect("ws has inbound");
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        let cursor = ws.cursor();
        let recv = tokio::spawn(async move { cursor.receive().await });
        // Arrive "late" (well past the 1s connect timeout in real terms; we
        // use a short sleep to keep the test fast while still ordering the
        // push after the receive has parked).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        inbound.push(InboundMessage::Json(serde_json::json!("late")));
        let got = recv.await.expect("joins").expect("a message, not an error");
        assert_eq!(got, InboundMessage::Json(serde_json::json!("late")));
    }

    /// `session_deadline` resolves after the cap with the fake clock, and
    /// NEVER resolves when the cap is 0 (no cap). This is the one legitimate
    /// bound on a live exchange.
    #[tokio::test]
    async fn session_deadline_fires_only_when_capped() {
        use weft_platform_traits::{Clock, FakeClock};
        let clock: Arc<dyn Clock> = FakeClock::new();
        // cap=0 means no cap: the future must still be pending after a poll.
        let never = session_deadline(&clock, 0);
        tokio::pin!(never);
        assert!(
            futures_poll_pending(&mut never),
            "cap=0 must never resolve (no session cap)"
        );
        // cap>0 resolves (FakeClock::sleep advances itself and returns).
        session_deadline(&clock, 30).await;
    }

    /// Poll a pinned future once; return true if it is still Pending. A tiny
    /// helper so the no-cap test can assert "does not resolve" without a real
    /// timeout race.
    fn futures_poll_pending<F: std::future::Future>(
        fut: &mut std::pin::Pin<&mut F>,
    ) -> bool {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        fn noop(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        matches!(fut.as_mut().poll(&mut cx), Poll::Pending)
    }

    // ----- OutboundQueue backpressure policies (the real DropOldest) --------

    fn chunk(n: i64) -> Outbound {
        Outbound::Chunk(OutboundChunk::Json(serde_json::json!(n)))
    }
    fn chunk_n(o: &Outbound) -> i64 {
        match o {
            Outbound::Chunk(OutboundChunk::Json(v)) => v.as_i64().unwrap(),
            other => panic!("expected json chunk, got {other:?}"),
        }
    }

    /// `DropNewest`: once full, the INCOMING chunk is shed; the queued ones
    /// (the oldest) survive in order.
    #[tokio::test]
    async fn outbound_drop_newest_sheds_incoming() {
        let q = OutboundQueue::new(2);
        q.push_drop_newest(chunk(1)).unwrap();
        q.push_drop_newest(chunk(2)).unwrap();
        q.push_drop_newest(chunk(3)).unwrap(); // full -> 3 is shed
        assert_eq!(chunk_n(&q.recv().await.unwrap()), 1);
        assert_eq!(chunk_n(&q.recv().await.unwrap()), 2);
        // Nothing else queued.
        q.close();
        assert!(q.recv().await.is_none());
    }

    /// `DropOldest`: once full, the OLDEST queued chunk is evicted to make
    /// room for the incoming one. This is the behavior an `mpsc` cannot do
    /// and that the old code only PRETENDED to do.
    #[tokio::test]
    async fn outbound_drop_oldest_evicts_front() {
        let q = OutboundQueue::new(2);
        q.push_drop_oldest(chunk(1)).unwrap();
        q.push_drop_oldest(chunk(2)).unwrap();
        q.push_drop_oldest(chunk(3)).unwrap(); // full -> evict 1, keep 2,3
        assert_eq!(chunk_n(&q.recv().await.unwrap()), 2);
        assert_eq!(chunk_n(&q.recv().await.unwrap()), 3);
        q.close();
        assert!(q.recv().await.is_none());
    }

    /// A terminal always lands even when the chunk queue is full, and it is
    /// NOT counted against the chunk capacity (the exchange must be able to
    /// end). Drop-oldest never evicts a terminal.
    #[tokio::test]
    async fn outbound_terminal_always_lands_and_is_never_evicted() {
        let q = OutboundQueue::new(1);
        q.push_drop_oldest(chunk(1)).unwrap();
        q.push_terminal(Outbound::Terminate(None)); // lands despite full chunks
        q.push_drop_oldest(chunk(2)).unwrap(); // evicts chunk 1, NOT the terminal
        // Assert the EXACT surviving sequence in FIFO order: the terminal
        // (queued 2nd) then chunk 2. This pins all three properties: chunk 1
        // was the one evicted, chunk 2 was enqueued, and the terminal kept its
        // FIFO position and was never evicted.
        let a = q.recv().await.unwrap();
        assert!(matches!(a, Outbound::Terminate(None)), "terminal drains first (FIFO), got {a:?}");
        let b = q.recv().await.unwrap();
        assert_eq!(chunk_n(&b), 2, "chunk 1 was evicted, chunk 2 survived");
        // Nothing else (chunk 1 is gone).
        q.close();
        assert!(q.recv().await.is_none(), "only the terminal and chunk 2 survived");
    }

    // `Block`: a producer waiting on a full queue wakes and enqueues as soon
    // as the drainer frees a slot, and a producer blocked when the queue
    // CLOSES gets a transport error (no hang). Stress-looped on a multi-
    // thread runtime: the no-lost-wakeup property of the shared `Notify`
    // (one drainer + blocked producers) only surfaces under real contention,
    // and a lost wakeup would HANG `blocked.await` (the harness then fails
    // the iteration), not pass quietly.
    weft_core::stress_test! {
        name: outbound_block_waits_for_slot_then_errors_on_close,
        runs: 80,
        worker_threads: 4,
        async fn body() {
            let q = OutboundQueue::new(1);
            q.push_block(chunk(1)).await.unwrap();
            // Second push blocks (queue full); it completes only once the
            // drainer pops. If the wakeup is lost this await hangs -> failure.
            let q2 = q.clone();
            let blocked = tokio::spawn(async move { q2.push_block(chunk(2)).await });
            // Let the producer reach its park point, then free a slot.
            for _ in 0..8 { tokio::task::yield_now().await; }
            assert_eq!(chunk_n(&q.recv().await.unwrap()), 1); // frees a slot
            blocked.await.expect("joins").expect("enqueued after slot freed");
            assert_eq!(chunk_n(&q.recv().await.unwrap()), 2);
            // A producer blocked at close gets a transport error (no hang).
            let q3 = q.clone();
            q.push_block(chunk(9)).await.unwrap(); // fill again
            let blocked2 = tokio::spawn(async move { q3.push_block(chunk(10)).await });
            for _ in 0..8 { tokio::task::yield_now().await; }
            q.close();
            let res = blocked2.await.expect("joins");
            assert!(matches!(res, Err(CallerError::Transport(_))), "close wakes blocked producer");
        }
    }

    // `receive()` waits indefinitely (never Timeout) and `inbound.close()`
    // (the disconnect / session-cap path) wakes the parked reader as a
    // `Disconnected`. Stress-looped multi-thread: the close-wakes-reader path
    // is the lost-wakeup risk; a missed wakeup HANGS `recv.await`.
    weft_core::stress_test! {
        name: receive_waits_indefinitely_then_disconnects_on_close,
        runs: 80,
        worker_threads: 4,
        async fn body() {
            let sink = std::sync::Arc::new(RecordingSink::default());
            let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), None, sink);
            let inbound = inbound.expect("ws has inbound");
            let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
                unreachable!()
            };
            let cursor = ws.cursor();
            // Park a receive with no message; only close should release it.
            let recv = tokio::spawn(async move { cursor.receive().await });
            for _ in 0..8 { tokio::task::yield_now().await; }
            inbound.close();
            match recv.await.expect("task joins") {
                Err(CallerError::Disconnected) => {}
                other => panic!("expected Disconnected on close, got {other:?}"),
            }
        }
    }
}
