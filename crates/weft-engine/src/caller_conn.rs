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
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use tokio::sync::mpsc;

use weft_core::caller::{
    resolve_disconnect, try_send_head, try_terminate, CallerConnection, CallerError,
    CallerRuntimeConfig, CloseReason, DisconnectAction, HttpRequestParts, InboundMessage,
    LiveRequest, OutboundChunk, ResponseHead,
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
    /// The HTTP status line and headers, queued right before the first
    /// item on the wire when the program set them explicitly. The
    /// connection refuses a second one (`HeadAlreadySent`), so the
    /// drainer sees it at most once, and only ahead of the first item.
    Head(ResponseHead),
    /// A non-terminal chunk to write to the wire.
    Chunk(OutboundChunk),
    /// The terminal: final optional body, then close the wire (with
    /// this close frame on a WebSocket; `None` = normal closure).
    Terminate(Option<OutboundChunk>, Option<CloseReason>),
    /// An error to surface to the caller per the error mode (a real
    /// error status before anything went out, an in-band chunk after,
    /// a close frame on a socket), then close.
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

    /// Enqueue an item the capacity policy never touches: a terminal
    /// (`Terminate`/`Error`, the exchange must be able to end regardless
    /// of the chunk backlog) or a `Head` (it must reach the wire ahead
    /// of the chunk it was queued with; drop-oldest evicts chunks only).
    /// Best-effort: silent if the queue already closed (the socket is
    /// gone anyway; the item that follows reports it).
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
    /// What the caller sent to open the exchange, as the dispatcher
    /// matched and gated it (both protocols).
    handshake: Arc<LiveRequest>,
    /// HTTP request parts captured at attach (HTTP only). `None` for WS.
    http_request: Option<Arc<HttpRequestParts>>,
    /// Journal sink for `Caller*` observability events.
    journal: Arc<dyn CallerJournalSink>,
    color: Color,
    inner: Mutex<ConnInner>,
    /// Monotonic offset for journaled caller events (per connection).
    next_offset: AtomicU64,
    /// Whether the caller is attached, as a watch so a hold waiting on
    /// the caller (`disconnected()`) wakes the moment it hangs up.
    connected: tokio::sync::watch::Sender<bool>,
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
    /// Something was queued toward the wire: the head is committed, a
    /// later explicit one is refused (`try_send_head`).
    wire_started: bool,
}

impl LiveCallerConnection {
    /// The socket owns this connection even after execution cleanup removes
    /// its registry entry. Record the disconnect exactly once on that owner.
    fn mark_disconnected(&self, reason: &str) {
        if self.connected.send_replace(false) {
            let offset = self.next_offset();
            self.journal.disconnected(self.color, offset, reason);
        }
    }

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

    /// The run is over and the caller is still attached: end the
    /// exchange the way the program left it. A response that was
    /// streaming ends cleanly (the body is complete; `write` without a
    /// `close` is not an error), a socket gets its normal close frame,
    /// and an HTTP caller that never heard a word gets a loud `500`
    /// saying so, because a route that ends without answering is a bug
    /// in the graph and a caller left hanging until the worker exits
    /// would see a gateway `503` with no hint of why. A program that
    /// already terminated the exchange is left alone.
    pub async fn run_ended(&self) {
        let silent_http = {
            let mut g = self.inner.lock().expect("caller conn poisoned");
            if g.terminated {
                return;
            }
            g.terminated = true;
            let silent_http = self.config.protocol == Protocol::Http && !g.wire_started;
            g.wire_started = true;
            silent_http
        };
        if silent_http {
            let message = "the run ended without answering";
            let offset = self.next_offset();
            self.journal.errored(self.color, offset, message);
            self.outbound.push_terminal(Outbound::Error(message.to_string()));
        } else {
            self.outbound.push_terminal(Outbound::Terminate(None, None));
        }
    }

    /// Resolve a gone-caller talk into the policy-correct outcome (cancel
    /// vs void), identical to the fake's contract.
    fn disconnected_outcome(&self) -> Result<(), CallerError> {
        match resolve_disconnect(self.config.suspend) {
            DisconnectAction::ContinueIntoVoid => Ok(()),
            DisconnectAction::CancelExecution => Err(CallerError::Disconnected),
        }
    }

    /// Apply the first-item rule for an explicit head and mark the wire
    /// started. On HTTP the head is queued ahead of the item that
    /// follows; a WebSocket has no head beyond its upgrade, so the head
    /// is dropped there (documented on the trait), never an error.
    fn commit_head(&self, head: Option<ResponseHead>) -> Result<(), CallerError> {
        let mut g = self.inner.lock().expect("caller conn poisoned");
        if head.is_some() {
            try_send_head(g.wire_started)?;
        }
        g.wire_started = true;
        // Queued under the latch (see `terminate`): the head is in the
        // queue before any other node can pass its own latch, so no
        // concurrent chunk lands ahead of it. The queue's own lock is
        // never held while this one is taken, so no lock order cycle.
        if let Some(h) = head.filter(|_| self.config.protocol == Protocol::Http) {
            self.outbound.push_terminal(Outbound::Head(h));
        }
        Ok(())
    }
}

#[async_trait]
impl CallerConnection for LiveCallerConnection {
    fn config(&self) -> &CallerRuntimeConfig {
        &self.config
    }

    fn is_connected(&self) -> bool {
        *self.connected.borrow()
    }

    async fn disconnected(&self) {
        let mut rx = self.connected.subscribe();
        // `wait_for` reads the current value first, so a hang-up that
        // already happened resolves at once.
        let _ = rx.wait_for(|connected| !connected).await;
    }

    fn wire_started(&self) -> bool {
        self.inner.lock().expect("caller conn poisoned").wire_started
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

    fn handshake(&self) -> Arc<LiveRequest> {
        self.handshake.clone()
    }

    async fn send_chunk(
        &self,
        head: Option<ResponseHead>,
        chunk: OutboundChunk,
    ) -> Result<(), CallerError> {
        if !self.is_connected() {
            return self.disconnected_outcome();
        }
        // The floor under every stream: a head that gives the worker
        // nothing to write while the feed is quiet leaves a caller who
        // hung up undetectable, and a run that was supposed to die with
        // that caller open for ever. Checked HERE and not on
        // `terminate`, which ends the exchange in the same breath and
        // so has no quiet to go into.
        self.commit_head(head)?;
        // Refuse rather than talk to a caller whose exchange is no
        // longer being written down: the record is what anyone later
        // has to go on, and a run that keeps going while it is missing
        // is a run that says it went well and cannot show it.
        if let Some(why) = self.journal.degraded() {
            return Err(CallerError::JournalLost(why));
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

    async fn terminate(
        &self,
        head: Option<ResponseHead>,
        final_chunk: Option<OutboundChunk>,
        close: Option<CloseReason>,
    ) -> Result<(), CallerError> {
        {
            let mut g = self.inner.lock().expect("caller conn poisoned");
            try_terminate(g.terminated)?;
            // Both rules are checked before either latch flips, so a
            // refused head leaves the exchange open for a plain terminal.
            if head.is_some() {
                try_send_head(g.wire_started)?;
            }
            g.terminated = true;
            g.wire_started = true;
            // Queued under the latch: a concurrent node's chunk, which
            // passes its own latch only after this one is released,
            // cannot land ahead of the head.
            if let Some(h) = head.filter(|_| self.config.protocol == Protocol::Http) {
                self.outbound.push_terminal(Outbound::Head(h));
            }
        }
        if let Some(c) = &final_chunk {
            let offset = self.next_offset();
            self.journal.outbound(self.color, offset, c, true);
        }
        // The terminal always lands (subject to no capacity policy); if the
        // socket task already ended (caller gone), it is silently dropped
        // (the exchange is over anyway).
        self.outbound.push_terminal(Outbound::Terminate(final_chunk, close));
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
        self.send_chunk(None, msg).await?;
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

    /// Why this conversation has stopped being recorded, once it has.
    ///
    /// The next thing the program tries to send its caller fails with
    /// this, the same way a bus whose journal write failed refuses the
    /// next send. Losing the record quietly and finishing clean is the
    /// worst of the three outcomes: the run looks fine and its exchange
    /// is simply missing, which nobody finds until they go looking for
    /// it. `None` while the journal is keeping up, which is the default
    /// because most sinks (the test fakes) cannot fail.
    fn degraded(&self) -> Option<String> {
        None
    }
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

    /// Attach the one connection for `color`. `false` when this color
    /// already has one, and the caller must refuse rather than proceed.
    ///
    /// A routing token is good for one exchange, and the door it opens
    /// takes ONE connection. Letting a second in silently replaced the
    /// entry, which is worse than it sounds: the run keeps talking to
    /// the first connection (it captured it once, at birth), the second
    /// caller waits forever on a run that never answers them, and when
    /// they give up, a route that cannot suspend reads their departure
    /// as its own caller leaving and kills the run that was still
    /// serving the first one. Both would also journal from offset zero,
    /// so one exchange's rows would interleave with the other's.
    #[must_use]
    pub fn attach(&self, color: Color, conn: Arc<LiveCallerConnection>) -> bool {
        let mut inner = self.inner.lock().expect("registry poisoned");
        if inner.contains_key(&color) {
            return false;
        }
        inner.insert(color, conn);
        drop(inner);
        // `notify_waiters` (not `notify_one`): several execute paths may be
        // waiting for distinct colors; wake them all to re-check.
        self.attached.notify_waiters();
        true
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

    /// Drop a color's entry.
    ///
    /// Never panics: this runs from `ExecutionResidue`'s destructor,
    /// which can itself run during an unwind, and a panic there aborts
    /// the process. A poisoned registry is reported and the entry stays
    /// (the pod is already in trouble; taking it down is worse).
    pub fn detach(&self, color: Color) {
        match self.inner.lock() {
            Ok(mut inner) => {
                inner.remove(&color);
            }
            Err(_) => tracing::error!(
                target: "weft_engine::caller_conn",
                %color,
                "the caller registry is poisoned, so this execution's connection entry was not \
                 dropped; it goes with the pod"
            ),
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
    handshake: Arc<LiveRequest>,
    http_body: Option<InboundMessage>,
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
    // The HTTP request a node reads: the handshake beside the body. A
    // WebSocket carries no body, so it exposes the handshake alone.
    let http_request = http_body.map(|body| {
        Arc::new(HttpRequestParts { request: (*handshake).clone(), body })
    });
    let conn = Arc::new(LiveCallerConnection {
        config,
        outbound: outbound.clone(),
        inbound: inbound.clone(),
        handshake,
        http_request,
        journal: journal.clone(),
        color,
        inner: Mutex::new(ConnInner { terminated: false, wire_started: false }),
        next_offset: AtomicU64::new(0),
        connected: tokio::sync::watch::Sender::new(true),
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
/// data type. JSON parses (fails loud on bad JSON; an EMPTY body is
/// `null`, what a bodiless GET honestly carries); text is UTF-8
/// (lossless required); bytes pass through.
fn decode_inbound(data_type: DataType, raw: &[u8]) -> Result<InboundMessage, String> {
    match data_type {
        DataType::Json if raw.iter().all(u8::is_ascii_whitespace) => {
            Ok(InboundMessage::Json(serde_json::Value::Null))
        }
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
    /// The worker's door to the control plane: a caller's arrival is a
    /// `LiveArrival` task the dispatcher answers by giving birth to the
    /// execution the routing token promised, on this pod.
    pub tasks: Arc<dyn weft_task_store::TaskStoreClient>,
    /// The tenant this worker serves, stamped on the arrival task.
    pub tenant_id: String,
}

/// How long the connection server waits for the dispatcher to give birth
/// to an arriving caller's execution before answering the caller. An
/// internal service-to-service wait the caller cannot influence (the
/// dispatcher claims a task within its poll interval), so a bound is
/// right: past it the caller hears that the run did not start, instead
/// of a hang.
const ARRIVAL_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// The request as the birth reads it: the method, the query without the
/// gateway hop's own routing token, and the headers. What the handshake
/// established (the route, the gate's verdict, the path and its
/// captures) rides the token instead.
fn arrival_payload(
    token: &str,
    raw_query: &str,
    request: &axum::extract::Request,
) -> weft_task_store::kinds::LiveArrivalPayload {
    let mut query = weft_core::route::parse_query(raw_query);
    query.remove("wct");
    weft_task_store::kinds::LiveArrivalPayload {
        token: token.to_string(),
        method: request.method().as_str().to_string(),
        query,
        headers: request
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
            .collect(),
    }
}

/// The caller is here: ask the dispatcher for the execution the routing
/// token promises, born on this pod. The request as it arrived rides
/// the task with the token (the birth reads the route, the gate's
/// verdict and the path captures off the token, and the method, query
/// and headers off this). A birth the dispatcher refuses (the project
/// went down, the pod filled up) is the caller's answer, a `503` with
/// the reason; a dispatcher that does not answer in time is a `504`.
async fn ask_for_birth(
    state: &ConnServerState,
    claims: &caller_token::CallerTokenClaims,
    payload: weft_task_store::kinds::LiveArrivalPayload,
) -> Result<(), Response> {
    use weft_task_store::tasks::{NewTask, TaskStatus, TaskTarget};
    let project_id = claims.project_id;
    let enqueued = state
        .tasks
        .enqueue_dedup(NewTask {
            kind: weft_task_store::TaskKind::LiveArrival.into(),
            target: TaskTarget::Dispatcher,
            project_id: Some(project_id),
            dedup_key: Some(weft_task_store::kinds::live_arrival_dedup_key(claims.color)),
            // No color on the row: the broker scopes a task by every
            // resource it names, and this color names nothing yet. The
            // task is what BRINGS it into being, so a color here would
            // be refused as unknown. The project is the anchor the
            // broker checks, the dedup key carries the color so one
            // arrival is one birth, and the executor reads the color
            // off the signed token, which is the only trustworthy
            // source for it anyway.
            color: None,
            tenant_id: Some(state.tenant_id.clone()),
            target_pod_name: None,
            binary_hash: None,
            payload: serde_json::to_value(&payload).expect("the arrival payload serializes"),
        })
        .await;
    let task_id = match enqueued.map(|outcome| outcome.id()) {
        Ok(Some(id)) => id,
        Ok(None) => {
            return Err((StatusCode::BAD_GATEWAY, "the run could not be asked for: the arrival was fenced").into_response());
        }
        Err(e) => {
            tracing::error!(target: "weft_engine::caller_conn", color = %claims.color, error = %e, "arrival enqueue failed");
            return Err((StatusCode::BAD_GATEWAY, format!("the run could not be asked for: {e}")).into_response());
        }
    };
    let outcome = match state.tasks.wait_for_terminal(task_id, ARRIVAL_WAIT).await {
        Ok(outcome) => outcome,
        Err(e) => {
            tracing::error!(target: "weft_engine::caller_conn", color = %claims.color, error = %e, "arrival wait failed");
            return Err((StatusCode::BAD_GATEWAY, format!("the run could not be started: {e}")).into_response());
        }
    };
    match outcome.status {
        TaskStatus::Complete => Ok(()),
        TaskStatus::Failed => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("the run could not start: {}", outcome.error.unwrap_or_else(|| "no reason given".into())),
        )
            .into_response()),
        TaskStatus::Pending | TaskStatus::Claimed => Err((
            StatusCode::GATEWAY_TIMEOUT,
            "the dispatcher did not start the run in time; retry shortly",
        )
            .into_response()),
    }
}

/// What the server needs to build a color's connection when its caller
/// attaches: the runtime config, the heartbeat interval, the caller's
/// opening request (from the execute task's start record), and the
/// journal sink.
pub struct ResolvedLiveStart {
    pub config: CallerRuntimeConfig,
    pub heartbeat_secs: u64,
    pub request: Arc<LiveRequest>,
    pub journal: Arc<dyn CallerJournalSink>,
}

/// How the server learns a color's connection config + journal sink.
/// `run_pod` implements this over the worker's per-execution state.
pub trait ConnConfigResolver: Send + Sync {
    /// `Some` when `color` is a live execution expecting a caller; `None`
    /// for an unknown/expired color (the server rejects the connection
    /// loud).
    fn resolve(&self, color: Color) -> Option<ResolvedLiveStart>;
}


/// Why an exchange ended. The one fact the drainers decide a cancel
/// on: a caller-tied run (see `resolve_disconnect`) is cancelled when
/// the CALLER ended the exchange, and left to finish when the PROGRAM
/// did (it answered, or closed the socket, and may still be running
/// its last nodes: a `done` pulse, a Debug). Matching the words would
/// have been the bug this replaces: for a while every "response
/// complete" cancelled the run that had just answered, and the
/// executions panel filled with cancelled rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExchangeEnd {
    /// The caller stopped reading (HTTP).
    CallerHungUp,
    /// The program answered and ended the response (HTTP).
    ResponseComplete,
    /// The program surfaced an error as the answer (HTTP).
    ResponseErrored,
    /// The program's outbound queue closed without a word.
    OutboundQueueClosed,
    /// The configured `max_session_secs` ceiling.
    SessionCapExceeded,
    /// A caller's message was over `max_inbound_bytes` (socket).
    InboundTooLarge,
    /// A caller's message did not decode as the data type (socket).
    InboundDecodeFailed,
    /// The caller sent a close frame (socket).
    CallerClosedSocket,
    /// The socket transport failed under the caller (socket).
    SocketTransportError,
    /// The socket stream ended without a close frame (socket).
    SocketEnded,
    /// A send to the caller failed (socket).
    CallerHungUpOnSend,
    /// The program closed the socket (socket).
    SessionClosedByProgram,
    /// The program closed the socket with an error (socket).
    SessionErroredByProgram,
    /// The caller stopped answering pings (socket).
    CallerMissedHeartbeat,
}

impl ExchangeEnd {
    /// The words the journal's `caller_disconnected` row carries.
    fn as_str(self) -> &'static str {
        match self {
            Self::CallerHungUp => "caller hung up",
            Self::ResponseComplete => "response complete",
            Self::ResponseErrored => "response errored",
            Self::OutboundQueueClosed => "outbound queue closed",
            Self::SessionCapExceeded => "session cap exceeded",
            Self::InboundTooLarge => "inbound message exceeded size cap",
            Self::InboundDecodeFailed => "inbound decode failed",
            Self::CallerClosedSocket => "caller closed the socket",
            Self::SocketTransportError => "socket transport error",
            Self::SocketEnded => "socket ended",
            Self::CallerHungUpOnSend => "caller hung up on send",
            Self::SessionClosedByProgram => "session closed by program",
            Self::SessionErroredByProgram => "session errored by program",
            Self::CallerMissedHeartbeat => "caller missed heartbeat",
        }
    }

    /// Whether the CALLER ended the exchange (a hang-up, a close frame,
    /// a dead socket, a missed heartbeat, a message the connection
    /// refused, the session cap). Everything else is the program's own
    /// end, after which the run finishes on its own.
    fn caller_initiated(self) -> bool {
        match self {
            Self::CallerHungUp
            | Self::CallerHungUpOnSend
            | Self::CallerClosedSocket
            | Self::SocketTransportError
            | Self::SocketEnded
            | Self::CallerMissedHeartbeat
            | Self::InboundTooLarge
            | Self::InboundDecodeFailed
            | Self::SessionCapExceeded => true,
            Self::ResponseComplete
            | Self::ResponseErrored
            | Self::OutboundQueueClosed
            | Self::SessionClosedByProgram
            | Self::SessionErroredByProgram => false,
        }
    }
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

/// How long a connection may be quiet before the machine starts asking
/// the caller whether it is still there, and how those questions are
/// paced. Deliberately well under the four minutes at which a NAT or a
/// load balancer on the way starts dropping connections it thinks are
/// idle, so this doubles as what keeps those from reaping a healthy
/// feed.
///
/// The whole schedule has to FIT INSIDE the route's silence bound, which
/// is the other half of this and takes over the moment a probe goes
/// unanswered: a bound shorter than the schedule would end the
/// connection part way through asking. So probes start early and the
/// answers are given little time, leaving room under the default.
const KEEPALIVE_IDLE_SECS: u64 = 10;
const KEEPALIVE_INTERVAL_SECS: u64 = 5;
const KEEPALIVE_RETRIES: u32 = 3;

/// Refuse a silence bound the keepalive schedule could not finish inside
/// (`10 + 5 * 3`), so the two can never be set to fight each other.
const KEEPALIVE_SCHEDULE_SECS: u64 =
    KEEPALIVE_IDLE_SECS + KEEPALIVE_INTERVAL_SECS * KEEPALIVE_RETRIES as u64;

/// A handle on the accepted socket, kept so the route's own silence
/// bound can be applied once we know which route the caller asked for.
///
/// The socket is accepted before the request line is read, so accept
/// time knows nothing about the trigger. The floor goes on at accept
/// (a caller that never names a valid route is still bounded), and the
/// trigger's own value replaces it here.
///
/// Cloning the file descriptor rather than holding the `TcpStream`: the
/// stream itself belongs to hyper, and this only ever sets an option on
/// it. Dropping this closes the duplicate, never the connection.
#[derive(Clone)]
struct CallerSocket(Option<std::sync::Arc<socket2::Socket>>);

/// How each connection's own handle reaches its handler, which is
/// axum's supported route for per-connection data.
///
/// Written for the concrete listener rather than every `Listener`: axum
/// already carries a blanket impl over a listener's address type, and a
/// generic one here overlaps it.
impl axum::extract::connect_info::Connected<
    axum::serve::IncomingStream<'_, tokio::net::TcpListener>,
> for CallerSocket
{
    fn connect_info(
        stream: axum::serve::IncomingStream<'_, tokio::net::TcpListener>,
    ) -> Self {
        watch_for_a_vanished_caller(stream.io())
    }
}

impl CallerSocket {
    /// Give this connection its own bound on how long the caller's
    /// machine may leave what we send unacknowledged. `0` means leave
    /// the machine's own default, which is about fifteen minutes.
    fn bound_silence(&self, secs: u64) {
        let Some(socket) = &self.0 else { return };
        // A bound shorter than the schedule would end the connection
        // part way through asking the caller whether it is there, so a
        // quiet feed would be cut while it was still healthy. The
        // schedule wins, and the route's wish is honoured from there up.
        let secs = if secs > 0 { secs.max(KEEPALIVE_SCHEDULE_SECS) } else { 0 };
        let timeout = (secs > 0).then(|| std::time::Duration::from_secs(secs));
        if let Err(error) = socket.set_tcp_user_timeout(timeout) {
            tracing::warn!(
                target: "weft_engine::caller_conn",
                %error, secs,
                "could not set this route's caller-silence bound; the connection keeps \
                 the server's default"
            );
        }
    }
}

/// Run the connection server until the process exits. Binds `0.0.0.0:port`
/// (plain HTTP/WS; TLS terminates at the gateway). Spawned by `run_pod`.
pub async fn serve(state: ConnServerState, port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(target: "weft_engine::caller_conn", %addr, "connection server listening");
    // Each connection's own socket handle reaches its handler as
    // `ConnectInfo<CallerSocket>` (the floor goes on as it is accepted),
    // which is how the route it turns out to want can set its own
    // silence bound on that one connection.
    axum::serve(
        listener,
        connection_router(state).into_make_service_with_connect_info::<CallerSocket>(),
    )
    .await?;
    Ok(())
}

/// Take a handle on a freshly accepted connection and put the floor on
/// it, so a caller who VANISHES cannot hold a run open for ever.
///
/// A caller leaves two ways and only one of them says so. A tab closed
/// or a page navigated away sends a goodbye, which arrives as a readable
/// event and ends the exchange in milliseconds; that has always worked.
/// A caller that VANISHES (a lid closed, a network gone, a phone that
/// left the building) sends nothing at all, and a conversation with
/// nothing arriving is indistinguishable from a quiet one, which is the
/// normal state of a live feed.
///
/// So the machine on the other end is asked instead. Every packet it
/// receives its operating system acknowledges by itself, with no help
/// from the page or the program, so "nothing acknowledged for this long"
/// means nobody is there. The connection then fails, which is what the
/// drainer is already waiting for.
///
/// This is the one option that does it. TCP keepalive is the obvious
/// reach and is inert here: the kernel skips its probes entirely while
/// any data is in flight (`tp->packets_out` in `tcp_timer.c`), and a
/// live feed's own keepalive filler guarantees that forever. It would
/// have looked right in a test that stopped writing and done nothing in
/// production.
///
/// The filler the framing writes is what makes this work: it is not
/// proof the caller is alive (a write into the queue in front of the
/// socket succeeds whether or not anybody is listening), it is the
/// traffic that gives the far side something to acknowledge. Neither
/// half works without the other.
fn watch_for_a_vanished_caller(stream: &tokio::net::TcpStream) -> CallerSocket {
    // A filler line is twenty-odd bytes, and Nagle would hold it back
    // waiting for company that never comes. The write has to reach the
    // wire for the bound below to mean anything.
    let _ = stream.set_nodelay(true);
    let socket = match socket2::SockRef::from(stream).try_clone() {
        Ok(socket) => CallerSocket(Some(std::sync::Arc::new(socket))),
        Err(error) => {
            // Not fatal: the connection still serves, and a caller that
            // says goodbye is still noticed at once. What is lost is the
            // bound on one that vanishes, so it is worth a line.
            tracing::warn!(
                target: "weft_engine::caller_conn",
                %error,
                "could not watch this connection for a caller that vanishes; one that \
                 disappears without closing will hold its run until the machine gives up"
            );
            return CallerSocket(None);
        }
    };
    // The other half, and the one that covers a stream with NOTHING to
    // write. The kernel skips its keepalive probes whenever data is
    // still in flight, which is why they are useless on a feed sending
    // filler: that filler keeps data in flight for ever. A feed that
    // writes nothing has none, so it IS eligible, and the probe is the
    // one poke that costs the payload nothing: zero bytes, a sequence
    // number one behind what the caller expects, so the caller discards
    // it and answers with an acknowledgement. The byte stream the
    // program is sending is untouched.
    //
    // So the two together cover both ways of vanishing: the bound above
    // catches a caller that disappears with data in flight, these catch
    // one that disappears while everything is quiet.
    //
    // Under four minutes on purpose: this doubles as what keeps a NAT
    // or a load balancer on the way from dropping the connection as
    // idle, and those give up long before the network standard's two
    // hours.
    let probes = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(KEEPALIVE_IDLE_SECS))
        .with_interval(std::time::Duration::from_secs(KEEPALIVE_INTERVAL_SECS))
        .with_retries(KEEPALIVE_RETRIES);
    if let Err(error) = socket.0.as_ref().expect("just built").set_tcp_keepalive(&probes) {
        tracing::warn!(
            target: "weft_engine::caller_conn",
            %error,
            "could not probe a quiet caller on this connection; one that disappears \
             from a feed with nothing to send will hold its run until the machine gives up"
        );
    }
    // The floor, before any route is known: the request line has not
    // been read yet, so a caller that never names a valid route is
    // bounded too. A route with its own value replaces this once it
    // resolves.
    socket.bound_silence(weft_core::signal::DEFAULT_CALLER_SILENCE_SECS);
    socket
}

/// The caller's own query string, with the routing token taken back
/// out. The token is the hop's, appended to the caller's query when the
/// dispatcher built the redirect, so what the gate signed is what is
/// left once it goes.
fn query_without_routing_token(raw: &str) -> String {
    raw.split('&')
        .filter(|kv| !kv.starts_with("wct=") && !kv.is_empty())
        .collect::<Vec<_>>()
        .join("&")
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
    let raw_query = request.uri().query().unwrap_or("").to_string();

    // 1. Verify the dispatcher-signed token + pod pin.
    let Some(token) = token_from_query(&raw_query) else {
        return (StatusCode::UNAUTHORIZED, "missing routing token").into_response();
    };
    let now = state.clock.now_unix();
    let claims = match caller_token::validate(&state.token_secret, &token, now) {
        Ok(c) => c,
        // Every reason a ticket is no good has the same remedy, so the
        // answer leads with it and names the reason after. The one that
        // actually happens to people is expiry: a ticket is good for a
        // couple of minutes, and the pod is kept a little past that
        // precisely so this sentence can be said instead of the socket
        // simply being dead.
        Err(e) => {
            return (
                StatusCode::UNAUTHORIZED,
                format!(
                    "this connection ticket is no good ({e}). Ask for a new one at the \
                     address you called first and follow where it points: a ticket lasts \
                     a couple of minutes and opens one connection.",
                ),
            )
                .into_response()
        }
    };
    if claims.pod_name != state.pod_name {
        // Per-pod pinning: this connection was signed for another pod.
        return (StatusCode::FORBIDDEN, "routing token addressed to a different pod").into_response();
    }
    let color = claims.color;

    // 1b. Hold the caller to the request the gate approved, when the
    //     gate checked anything at all. The door carries the verdict,
    //     not the request, so without this a caller could pass the gate
    //     with one request and send a different one down the redirect
    //     they were handed. For a password-shaped check that changes
    //     nothing (the answer really was only about who they are); for
    //     a signature it is the whole point, because a signature's
    //     claim is about one exact request.
    //
    //     The body is read here rather than at the gate for an open
    //     route, so it is only re-read when there is a fingerprint to
    //     compare it against.
    let request = if let Some(approved) = claims.approved.clone() {
        let (parts, body) = request.into_parts();
        // Read against the language's own ceiling rather than the
        // route's: the route's cap is applied where the body is used,
        // and this read only exists to fingerprint what arrived.
        let bytes = match axum::body::to_bytes(
            body,
            weft_core::signal::live_connection::DEFAULT_MAX_INBOUND_BYTES as usize,
        )
        .await
        {
            Ok(b) => b,
            Err(_) => {
                return (StatusCode::PAYLOAD_TOO_LARGE, "request body exceeds the cap")
                    .into_response()
            }
        };
        // The path is not compared: it is signed into the claims, so it
        // is the dispatcher's word either way, and the gateway rewrote
        // the one on the wire.
        let arrived = caller_token::RequestFingerprint::of(
            parts.method.as_str(),
            &approved.path,
            &query_without_routing_token(&raw_query),
            &bytes,
        );
        if arrived != approved {
            return (
                StatusCode::FORBIDDEN,
                "this is not the request that was approved: the door you were given opens for \
                 the call you made, not another one",
            )
                .into_response();
        }
        axum::http::Request::from_parts(parts, axum::body::Body::from(bytes))
    } else {
        request
    };

    // 2. The caller is here: have the execution born on this pod. Nothing
    //    was born at the handshake (a caller who never follows the
    //    redirect leaves nothing behind); the routing token is the
    //    dispatcher's promise, and this is where it is kept.
    let arrival = arrival_payload(&token, &raw_query, &request);
    if let Err(response) = ask_for_birth(&state, &claims, arrival).await {
        return response;
    }

    // 3. Resolve the execution's connection config (protocol, caps, data
    //    type) + journal sink.
    //
    // The birth inserted the pinned execute task; the worker populates its
    // resolver when it CLAIMS and starts that task, a beat later. So the
    // resolver can briefly lag the birth. That is "not ready yet," not
    // "unknown": poll the resolver for a bounded window before giving up.
    // Without this, a fast caller racing the worker's task-claim gets a
    // spurious 404 even though the execution is genuinely starting.
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
    let Some(ResolvedLiveStart { config, heartbeat_secs, request: handshake, journal }) = resolved
    else {
        return (StatusCode::NOT_FOUND, "no live execution for this token").into_response();
    };

    // The route is known now, so its own answer to "how long may this
    // caller's machine go silent" replaces the floor put on at accept.
    if let Some(socket) = request.extensions().get::<axum::extract::ConnectInfo<CallerSocket>>() {
        socket.0.bound_silence(config.caller_silence_secs);
    }

    // 4. Branch on protocol. The connection layer is shared; only the
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
                        drive_ws(socket, st, color, config, heartbeat_secs, handshake, journal)
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
            // The method, headers and query were captured by the dispatcher
            // at the handshake and ride in `request`; the worker only
            // reads the body (the 307 made the caller resend it here).
            drop(parts);
            drive_http(state, color, config, heartbeat_secs, handshake, journal, body).await
        }
    }
}

/// HTTP path: read the (capped) request body, build the connection with
/// the handshake beside it, attach it, then answer with the head the
/// program's FIRST outbound item decides and a chunked body fed by the
/// rest. The node's `respond`/`write`/`close` drive what the caller
/// receives; nothing goes out before the program speaks, so a route can
/// answer 404, set a header, or stream. A program that never replies
/// holds the caller only as long as it runs: when the run ends the
/// worker closes the exchange with a `500` saying so (`run_ended`).
#[allow(clippy::too_many_arguments)]
async fn drive_http(
    state: ConnServerState,
    color: Color,
    config: CallerRuntimeConfig,
    heartbeat_secs: u64,
    request: Arc<LiveRequest>,
    journal: Arc<dyn CallerJournalSink>,
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

    let sink = journal.clone();
    let seed = decoded.clone();
    let (conn, outbound, _inb) =
        new_connection(config.clone(), color, request, Some(decoded), journal);
    // What the caller sent is written down the same way a socket's
    // first frame is. It used to be the one thing a caller said that
    // the journal never held, for no reason anybody chose: an HTTP
    // request is one inbound message that happens to arrive with the
    // connection rather than after it. Journaled AFTER the connection
    // exists so it takes the connection's own next offset and cannot
    // collide with the connect row at offset zero.
    sink.inbound(color, conn.next_offset(), &seed);
    if !state.registry.attach(color, conn.clone()) {
        return (
            StatusCode::CONFLICT,
            "this exchange already has a caller: a routing token opens one connection, and \
             following the same redirect twice is not a second one. Ask for a new one.",
        )
            .into_response();
    }

    // Stream the worker's outbound chunks as a chunked HTTP body. The
    // first Terminate ends the stream.
    //
    // A caller who leaves is found two ways, neither of which needs the
    // program to write. The body's receiver is dropped when the
    // handler's response goes away (the connection reset under it, or
    // the handler future dropped before the head), and `tx.closed()`
    // says so at once. And on every heartbeat the drainer writes the
    // head's `keepalive` filler (the bytes a reader of that framing
    // ignores), because a proxy on the way may keep our side open
    // after the far side hung up, and only a write finds that out.
    // Before the head is committed there is nothing to write; a head
    // with no filler (a raw stream) relies on the receiver alone.
    let mut heartbeat = (heartbeat_secs != 0).then(|| {
        let mut iv = tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        iv
    });
    let registry = state.registry.clone();
    let canceller = state.canceller.clone();
    let policy = config.suspend;
    let clock = state.clock.clone();
    let max_session_secs = config.max_session_secs;
    let (head_tx, head_rx) = tokio::sync::oneshot::channel::<ResponseHead>();
    let (tx, rx) = mpsc::channel::<Result<Vec<u8>, std::io::Error>>(16);
    tokio::spawn(async move {
        let session = session_deadline(&clock, max_session_secs);
        tokio::pin!(session);
        let mut head = HeldHead::new(head_tx);
        let reason = loop {
            tokio::select! {
                out = outbound.recv() => match out {
                    Some(Outbound::Head(h)) => head.explicit(h),
                    Some(Outbound::Chunk(c)) => {
                        if !head.commit_for(Some(&c)) {
                            break ExchangeEnd::CallerHungUp;
                        }
                        if tx.send(Ok(chunk_to_bytes(&c))).await.is_err() {
                            break ExchangeEnd::CallerHungUp;
                        }
                    }
                    Some(Outbound::Terminate(final_chunk, _close)) => {
                        if !head.commit_for(final_chunk.as_ref()) {
                            break ExchangeEnd::CallerHungUp;
                        }
                        if let Some(c) = final_chunk {
                            let _ = tx.send(Ok(chunk_to_bytes(&c))).await;
                        }
                        break ExchangeEnd::ResponseComplete;
                    }
                    Some(Outbound::Error(msg)) => {
                        // Before the first byte the status line is still
                        // ours to set: a real error status with the
                        // message as the body. After streaming started
                        // the status is committed, so the error goes
                        // in-band, then the stream closes.
                        if head.is_pending() {
                            head.commit_error();
                            let _ = tx.send(Ok(msg.into_bytes())).await;
                        } else {
                            let _ = tx.send(Ok(format!("\n[error] {msg}").into_bytes())).await;
                        }
                        break ExchangeEnd::ResponseErrored;
                    }
                    None => break ExchangeEnd::OutboundQueueClosed,
                },
                // The caller's side of the body is gone: the connection
                // dropped, or the handler was dropped before the head.
                _ = tx.closed() => break ExchangeEnd::CallerHungUp,
                // Quiet body: write the framing's filler, and a failed
                // write is the caller gone.
                _ = async { heartbeat.as_mut().unwrap().tick().await }, if heartbeat.is_some() => {
                    if let Some(filler) = head.keepalive() {
                        if tx.send(Ok(filler.as_bytes().to_vec())).await.is_err() {
                            break ExchangeEnd::CallerHungUp;
                        }
                    }
                }
                // Session cap: a configured `max_session_secs` ceiling on the
                // total exchange (0 = no cap, the future never resolves). The
                // ONLY deadline on a live exchange; per-message waits are
                // unbounded (a node may legitimately wait hours).
                _ = &mut session => break ExchangeEnd::SessionCapExceeded,
            }
        };
        // Ended with the head still held (the run finished, was cut, or
        // the queue closed without a word to the caller): say so with a
        // real status instead of hanging up mid-handshake.
        if head.is_pending() {
            head.commit_error();
            let _ = tx.send(Ok(format!("no response from the program: {}", reason.as_str()).into_bytes())).await;
        }
        // The exchange ended: stop producers (a blocked send now errors) and
        // mark the caller gone for this run. A tied run is cancelled only
        // when the CALLER ended it; a run that answered finishes on its own.
        outbound.close();
        conn.mark_disconnected(reason.as_str());
        registry.detach(color);
        if reason.caller_initiated()
            && matches!(resolve_disconnect(policy), DisconnectAction::CancelExecution)
        {
            canceller.cancel(color);
        }
    });

    // HOLD the response until the program's first item decides the head.
    // A dropped sender means the drainer ended without committing one,
    // which the drainer itself prevents above; answering 500 keeps that
    // path loud rather than a hang.
    let head = match head_rx.await {
        Ok(h) => h,
        Err(_) => ResponseHead::new(500)
            .with_content_type_for(&OutboundChunk::Text(String::new())),
    };
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = axum::body::Body::from_stream(stream);
    build_response(&head, body)
}

/// The response head while it is still the program's to decide: an
/// explicit `Head` item parks here until the item it precedes arrives,
/// and the first item on the wire commits it (with the defaults filled
/// in) through the oneshot the handler is waiting on. Pure state machine
/// over the drainer's items; `commit_for` reports whether the handler
/// was still there to receive the head.
struct HeldHead {
    tx: Option<tokio::sync::oneshot::Sender<ResponseHead>>,
    explicit: Option<ResponseHead>,
    /// The committed head's filler (see `ResponseHead::keepalive`),
    /// what the drainer writes on a quiet heartbeat; `None` until the
    /// head went out, and for a body whose framing has none.
    keepalive: Option<String>,
}

impl HeldHead {
    fn new(tx: tokio::sync::oneshot::Sender<ResponseHead>) -> Self {
        Self { tx: Some(tx), explicit: None, keepalive: None }
    }

    fn is_pending(&self) -> bool {
        self.tx.is_some()
    }

    fn keepalive(&self) -> Option<&str> {
        self.keepalive.as_deref()
    }

    /// The program set the head explicitly; it goes out with the item
    /// that follows. A head arriving after the commit is a connection
    /// bug (the connection refuses it), reported loud and ignored.
    fn explicit(&mut self, head: ResponseHead) {
        if self.tx.is_none() {
            tracing::error!(
                target: "weft_engine::caller_conn",
                "a response head reached the drainer after the head was committed; \
                 the connection should have refused it"
            );
            return;
        }
        self.explicit = Some(head);
    }

    /// Commit the head for the first item on the wire. `chunk` is the
    /// item's body when it has one: it supplies the content type the
    /// head does not name. No chunk and no explicit head is `204`. A
    /// later call is a no-op. Returns `false` when the handler already
    /// went away (the caller hung up before the first byte).
    fn commit_for(&mut self, chunk: Option<&OutboundChunk>) -> bool {
        let Some(tx) = self.tx.take() else { return true };
        let head = match (self.explicit.take(), chunk) {
            (Some(h), Some(c)) => h.with_content_type_for(c),
            (Some(h), None) => h,
            (None, Some(c)) => ResponseHead::default().with_content_type_for(c),
            (None, None) => ResponseHead::new(204),
        };
        self.keepalive = head.keepalive.clone();
        tx.send(head).is_ok()
    }

    /// Commit a `500` with a text body: the program failed (or ended
    /// silent) before the first byte, so the status line can still say so.
    fn commit_error(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(
                ResponseHead::new(500).with_content_type_for(&OutboundChunk::Text(String::new())),
            );
        }
    }
}

/// The axum response for a committed head: its status, its headers, and
/// `X-Accel-Buffering: no` so proxies pass a stream through. A header the
/// program spelled in a way HTTP cannot carry is a loud `500` naming it,
/// never a silent drop.
fn build_response(head: &ResponseHead, body: axum::body::Body) -> Response {
    let mut builder = Response::builder().status(head.status).header("X-Accel-Buffering", "no");
    for (name, value) in &head.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    match builder.body(body) {
        Ok(response) => response,
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the program's response head cannot be sent: {e}"),
        )
            .into_response(),
    }
}

/// WebSocket path: bridge the socket to the connection. Spawns the read
/// pump (decode caller frames -> broadcast inbound), the write pump (drain
/// outbound -> frames), and the heartbeat (ping on a timer).
#[allow(clippy::too_many_arguments)]
async fn drive_ws(
    mut socket: WebSocket,
    state: ConnServerState,
    color: Color,
    config: CallerRuntimeConfig,
    heartbeat_secs: u64,
    request: Arc<LiveRequest>,
    journal: Arc<dyn CallerJournalSink>,
) {
    let (conn, outbound, inbound) =
        new_connection(config.clone(), color, request, None, journal.clone());
    let inbound = inbound.expect("websocket connection has an inbound channel");
    if !state.registry.attach(color, conn.clone()) {
        // The socket is already upgraded here, so the only way to say
        // no is to close it. One exchange, one connection: the run is
        // already talking to the first socket and would never answer
        // this one.
        conn.mark_disconnected("this exchange already has a caller");
        return;
    }

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
                        break ExchangeEnd::InboundTooLarge;
                    }
                    match decode_inbound(data_type, t.as_bytes()) {
                        Ok(msg) => {
                            let off = conn.next_offset();
                            journal.inbound(color, off, &msg);
                            inbound.push(msg);
                        }
                        Err(_) => break ExchangeEnd::InboundDecodeFailed,
                    }
                }
                Some(Ok(Message::Binary(b))) => {
                    if b.len() as u64 > max_inbound {
                        break ExchangeEnd::InboundTooLarge;
                    }
                    match decode_inbound(data_type, &b) {
                        Ok(msg) => {
                            let off = conn.next_offset();
                            journal.inbound(color, off, &msg);
                            inbound.push(msg);
                        }
                        Err(_) => break ExchangeEnd::InboundDecodeFailed,
                    }
                }
                Some(Ok(Message::Close(_))) => break ExchangeEnd::CallerClosedSocket,
                Some(Ok(_)) => { /* ping/pong handled by axum */ }
                Some(Err(_)) => break ExchangeEnd::SocketTransportError,
                None => break ExchangeEnd::SocketEnded,
            },
            // Program -> caller.
            out = outbound.recv() => match out {
                // The connection drops a head on a WebSocket before it
                // reaches the queue (the upgrade was the only head);
                // one arriving here is a connection bug, loud and skipped.
                Some(Outbound::Head(_)) => tracing::error!(
                    target: "weft_engine::caller_conn",
                    color = %color, "a response head reached a websocket drainer"
                ),
                Some(Outbound::Chunk(c)) => {
                    if socket.send(chunk_to_ws(&c)).await.is_err() {
                        break ExchangeEnd::CallerHungUpOnSend;
                    }
                }
                Some(Outbound::Terminate(final_chunk, close)) => {
                    if let Some(c) = final_chunk {
                        let _ = socket.send(chunk_to_ws(&c)).await;
                    }
                    let close = close.unwrap_or_default();
                    let _ = socket.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: close.code,
                        reason: close.reason.into(),
                    }))).await;
                    break ExchangeEnd::SessionClosedByProgram;
                }
                Some(Outbound::Error(msg)) => {
                    let _ = socket.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1011, // internal error
                        reason: msg.into(),
                    }))).await;
                    break ExchangeEnd::SessionErroredByProgram;
                }
                None => break ExchangeEnd::OutboundQueueClosed,
            },
            // Keep-alive ping (worker-side; browsers can't ping us). The arm
            // only exists when a heartbeat is configured (`heartbeat` is
            // `Some`); otherwise it is permanently disabled.
            _ = async { heartbeat.as_mut().unwrap().tick().await }, if heartbeat.is_some() => {
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break ExchangeEnd::CallerMissedHeartbeat;
                }
            }
            // Session cap: the one deadline on a live exchange (per-message
            // waits are unbounded). `0` = no cap (the future never resolves).
            _ = &mut session => break ExchangeEnd::SessionCapExceeded,
        }
    };

    // Wake any node parked in receive() so it unblocks (the log is now
    // closed; a caught-up reader gets a disconnect) and any producer blocked
    // on a full outbound queue (its send now errors).
    inbound.close();
    outbound.close();
    conn.mark_disconnected(reason.as_str());
    state.registry.detach(color);
    // A tied run is cancelled only when the CALLER ended the exchange;
    // a socket the program closed leaves the run to finish.
    if reason.caller_initiated()
        && matches!(resolve_disconnect(policy), DisconnectAction::CancelExecution)
    {
        state.canceller.cancel(color);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::caller::CallerHandle;
    use weft_core::signal::DataType;
    use weft_core::wait::SuspendPolicy;

    const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);

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
            caller_silence_secs: weft_core::signal::DEFAULT_CALLER_SILENCE_SECS,
            max_session_secs: 0,
            suspend: SuspendPolicy { can_suspend: false, default_hold_secs: 300 },
            inbound_window: 4,
            journal: weft_core::stream_journal::JournalPolicy::default(),
        }
    }

    #[tokio::test]
    async fn socket_disconnect_is_recorded_after_registry_cleanup() {
        let journal = Arc::new(RecordingSink::default());
        let (conn, _, _) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, journal.clone());
        let registry = CallerRegistry::new();
        assert!(registry.attach(Color::nil(), conn.clone()), "the first caller attaches");
        conn.terminate(None, None, None).await.unwrap();
        registry.detach(Color::nil());
        conn.mark_disconnected("socket closed");
        conn.mark_disconnected("socket closed again");
        assert_eq!(journal.events.lock().unwrap().iter().filter(|event| event.starts_with("disconnected@")).count(), 1);
    }

    #[tokio::test]
    async fn outbound_chunks_reach_the_socket_channel_and_journal() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, out_rx, _inb) =
            new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink.clone());
        let handle = CallerHandle::from_connection(conn.clone());
        let CallerHandle::Websocket(ws) = handle else { unreachable!() };
        ws.send(OutboundChunk::Json(serde_json::json!("hi"))).await.unwrap();
        ws.close().await.unwrap();
        // Socket task would drain these:
        let first = out_rx.recv().await.expect("chunk");
        assert!(matches!(first, Outbound::Chunk(_)));
        let term = out_rx.recv().await.expect("terminate");
        assert!(matches!(term, Outbound::Terminate(None, None)));
        // Journal saw connect and the outbound chunk. The disconnect is the
        // socket task's to record when the exchange ends (`mark_disconnected`
        // on the connection it owns), never the close itself: a close with
        // no socket task behind it is not a caller going away.
        let ev = sink.events();
        assert!(ev[0].starts_with("connected@0"), "got {ev:?}");
        assert!(ev.iter().any(|e| e.starts_with("outbound@")), "got {ev:?}");
        assert!(!ev.iter().any(|e| e.starts_with("disconnected@")), "got {ev:?}");
    }

    #[tokio::test]
    async fn inbound_broadcasts_to_every_listener() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out_rx, inbound) =
            new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, out_rx, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, _inb) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
        conn.terminate(None, None, None).await.expect("first terminal");
        let err = conn.terminate(None, None, None).await.expect_err("second rejected");
        assert!(matches!(err, CallerError::AlreadyTerminated));
    }

    /// `receive()` is UNBOUNDED: no deadline ends it. A node parked on
    /// the next message waits indefinitely; the only ways out are a message
    /// arriving or the connection closing, which yields `Disconnected`.
    /// Regression for the bug where `receive()` was bounded by
    /// the connect timeout and a quiet caller killed the node.
    /// A late message (arriving after the connect timeout would have fired)
    /// is still delivered: proves the wait is genuinely unbounded, not just
    /// "returns Disconnected eventually".
    #[tokio::test]
    async fn receive_delivers_a_late_message() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        q.push_terminal(Outbound::Terminate(None, None)); // lands despite full chunks
        q.push_drop_oldest(chunk(2)).unwrap(); // evicts chunk 1, NOT the terminal
        // Assert the EXACT surviving sequence in FIFO order: the terminal
        // (queued 2nd) then chunk 2. This pins all three properties: chunk 1
        // was the one evicted, chunk 2 was enqueued, and the terminal kept its
        // FIFO position and was never evicted.
        let a = q.recv().await.unwrap();
        assert!(matches!(a, Outbound::Terminate(None, None)), "terminal drains first (FIFO), got {a:?}");
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

    // ----- The held head: what the first outbound item decides ----------

    struct NoResolver;
    impl ConnConfigResolver for NoResolver {
        fn resolve(&self, _color: Color) -> Option<ResolvedLiveStart> {
            None
        }
    }

    #[derive(Default)]
    struct RecordingCanceller {
        cancelled: Mutex<Vec<Color>>,
    }
    impl ExecutionCanceller for RecordingCanceller {
        fn cancel(&self, color: Color) {
            self.cancelled.lock().unwrap().push(color);
        }
    }

    fn http_cfg() -> CallerRuntimeConfig {
        CallerRuntimeConfig { protocol: Protocol::Http, ..ws_cfg() }
    }

    fn server_state() -> (ConnServerState, Arc<RecordingCanceller>) {
        let canceller = Arc::new(RecordingCanceller::default());
        let state = ConnServerState {
            registry: CallerRegistry::new(),
            token_secret: Arc::new(Vec::new()),
            pod_name: "pod-a".into(),
            resolver: Arc::new(NoResolver),
            clock: weft_platform_traits::FakeClock::new(),
            canceller: canceller.clone(),
            tasks: Arc::new(ArrivalTasks::default()),
            tenant_id: "tenant-a".into(),
        };
        (state, canceller)
    }

    /// The control plane as the connection server sees it: every
    /// arrival task it enqueued, and one answer for all of them.
    #[derive(Default)]
    struct ArrivalTasks {
        asked: Mutex<Vec<weft_task_store::tasks::NewTask>>,
        /// The status the dispatcher answers every arrival with, and
        /// the reason when it refused.
        answer: Mutex<Option<(weft_task_store::tasks::TaskStatus, Option<String>)>>,
    }
    #[async_trait]
    impl weft_task_store::TaskStoreClient for ArrivalTasks {
        async fn enqueue_dedup(
            &self,
            spec: weft_task_store::tasks::NewTask,
        ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            self.asked.lock().unwrap().push(spec);
            Ok(weft_task_store::tasks::DedupOutcome::Inserted(uuid::Uuid::new_v4()))
        }
        async fn wait_for_terminal(
            &self,
            _task_id: uuid::Uuid,
            _timeout: std::time::Duration,
        ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            let (status, error) = self.answer.lock().unwrap().clone().expect("the test set an answer");
            Ok(weft_task_store::tasks::TaskOutcome { status, result: None, error })
        }
        async fn claim_one(
            &self,
            _pod_id: &str,
            _filter: weft_task_store::tasks::ClaimFilter,
            _wait: std::time::Duration,
        ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
            Ok(None)
        }
        async fn heartbeat(&self, _task_id: uuid::Uuid, _pod_id: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn requeue(&self, _task_id: uuid::Uuid, _pod_id: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(&self, _task_id: uuid::Uuid, _pod_id: &str, _result: serde_json::Value) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(&self, _task_id: uuid::Uuid, _pod_id: &str, _error: String) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn routing_token(secret: &[u8], pod_name: &str, exp: i64) -> (String, Color) {
        // An open route: the gate approved nobody, so there is no
        // request to hold this caller to.
        approving_token(secret, pod_name, exp, None)
    }

    fn approving_token(
        secret: &[u8],
        pod_name: &str,
        exp: i64,
        approved: Option<caller_token::RequestFingerprint>,
    ) -> (String, Color) {
        let color = Color::new_v4();
        let token = caller_token::mint(
            secret,
            &caller_token::CallerTokenClaims {
                color,
                project_id: PROJECT,
                pod_name: pod_name.into(),
                signal: "sig-1".into(),
                path: "chat/room7".into(),
                params: [("room".to_string(), "room7".to_string())].into_iter().collect(),
                caller: None,
                approved,
                exp,
            },
        );
        (token, color)
    }

    /// The door opens for the call that was made, and for no other.
    ///
    /// The gate runs at the dispatcher and the program runs on the
    /// worker, with a redirect in between, so without this the caller
    /// could get a signature checked against one request and then send
    /// a different one down the redirect they were handed. The token
    /// carries a fingerprint of what was approved; the worker takes the
    /// fingerprint of what actually arrived and compares.
    ///
    /// The refusal comes BEFORE the birth is asked for, which is the
    /// half that matters: a rejected caller must leave no execution
    /// behind.
    #[tokio::test]
    async fn a_door_opens_only_for_the_request_it_was_given_for() {
        use tower::ServiceExt as _;
        let approved = caller_token::RequestFingerprint::of(
            "POST",
            "chat/room7",
            "verbose=1",
            b"{\"say\":\"hi\"}",
        );
        // Each case is one thing changed against what was approved.
        let cases: Vec<(&str, &str, &str, &str)> = vec![
            ("the body", "POST", "verbose=1", "{\"say\":\"everything\"}"),
            ("the method", "PUT", "verbose=1", "{\"say\":\"hi\"}"),
            ("the query", "POST", "verbose=2", "{\"say\":\"hi\"}"),
            ("a dropped query", "POST", "", "{\"say\":\"hi\"}"),
        ];
        for (changed, method, query, body) in cases {
            let (mut state, _) = server_state();
            let tasks = Arc::new(ArrivalTasks::default());
            state.tasks = tasks.clone();
            let (token, _) = approving_token(
                &[],
                "pod-a",
                state.clock.now_unix() + 60,
                Some(approved.clone()),
            );
            let uri = if query.is_empty() {
                format!("/chat/room7?wct={token}")
            } else {
                format!("/chat/room7?{query}&wct={token}")
            };
            let request = axum::http::Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap();
            let response = connection_router(state).oneshot(request).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::FORBIDDEN,
                "changing {changed} must not open the door"
            );
            assert!(
                tasks.asked.lock().unwrap().is_empty(),
                "changing {changed} left an execution behind"
            );
        }
    }

    /// The same door, used for the request it was actually given for:
    /// it opens, and the birth is asked for. Without this the test
    /// above would pass just as well on a worker that refused
    /// everything.
    ///
    /// The routing token rides in the query and is the hop's business,
    /// not the program's, so it is stripped before the fingerprint is
    /// taken. That strip has to be byte-exact against what the
    /// dispatcher hashed, which is what this pins.
    #[tokio::test]
    async fn the_approved_request_is_let_through() {
        use tower::ServiceExt as _;
        let (mut state, _) = server_state();
        let tasks = Arc::new(ArrivalTasks::default());
        *tasks.answer.lock().unwrap() = Some((
            weft_task_store::tasks::TaskStatus::Failed,
            Some("worker pod 'pod-a' is memory-saturated; retry shortly".into()),
        ));
        state.tasks = tasks.clone();
        let approved = caller_token::RequestFingerprint::of(
            "POST",
            "chat/room7",
            "verbose=1&page=2",
            b"{\"say\":\"hi\"}",
        );
        let (token, _) =
            approving_token(&[], "pod-a", state.clock.now_unix() + 60, Some(approved));
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/chat/room7?verbose=1&page=2&wct={token}"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{\"say\":\"hi\"}"))
            .unwrap();
        let response = connection_router(state).oneshot(request).await.unwrap();
        assert_ne!(response.status(), StatusCode::FORBIDDEN, "the door was given for this call");
        assert_eq!(tasks.asked.lock().unwrap().len(), 1, "the birth was asked for");
    }

    /// One door, one connection. A routing token is spent when it is
    /// used: a second caller holding a copy of it finds the exchange
    /// taken, and the run keeps talking to the caller it already has
    /// rather than being handed to whoever arrived last.
    #[tokio::test]
    async fn a_second_caller_cannot_take_over_an_exchange() {
        let color = Color::new_v4();
        let sink = Arc::new(RecordingSink::default());
        let (first, _, _) =
            new_connection(ws_cfg(), color, Arc::new(LiveRequest::default()), None, sink.clone());
        let (second, _, _) =
            new_connection(ws_cfg(), color, Arc::new(LiveRequest::default()), None, sink);
        let registry = CallerRegistry::new();
        assert!(registry.attach(color, first.clone()), "the first caller is admitted");
        assert!(!registry.attach(color, second), "the second is refused");
        assert!(
            Arc::ptr_eq(&registry.get(color).expect("the exchange still has its caller"), &first),
            "the run keeps the caller it already had"
        );
    }

    /// A caller with a valid token has the execution asked for before
    /// anything else: the arrival task carries the token and the request
    /// as it arrived (its method, query without the routing token,
    /// headers), keyed to the color. A birth the dispatcher refuses is
    /// the caller's answer, with the reason.
    #[tokio::test]
    async fn an_arriving_caller_asks_for_the_birth_and_hears_a_refusal() {
        use tower::ServiceExt as _;
        let (mut state, _) = server_state();
        let tasks = Arc::new(ArrivalTasks::default());
        *tasks.answer.lock().unwrap() = Some((
            weft_task_store::tasks::TaskStatus::Failed,
            Some("worker pod 'pod-a' is memory-saturated; retry shortly".into()),
        ));
        state.tasks = tasks.clone();
        let (token, color) = routing_token(&[], "pod-a", state.clock.now_unix() + 60);
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/chat/room7?verbose=1&wct={token}"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{}"))
            .unwrap();
        let response = connection_router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("memory-saturated"), "{body:?}");
        let asked = tasks.asked.lock().unwrap();
        assert_eq!(asked.len(), 1);
        let task = &asked[0];
        assert_eq!(task.kind, "live_arrival");
        assert_eq!(task.dedup_key.as_deref(), Some(format!("live-arrival:{color}").as_str()));
        assert_eq!(task.project_id, Some(PROJECT), "the project is the anchor the broker checks");
        assert_eq!(task.color, None, "the color does not exist yet; this task is what creates it");
        assert_eq!(task.tenant_id.as_deref(), Some("tenant-a"));
        let payload: weft_task_store::kinds::LiveArrivalPayload = serde_json::from_value(task.payload.clone()).unwrap();
        assert_eq!(payload.token, token);
        assert_eq!(payload.method, "POST");
        assert_eq!(payload.query.get("verbose").map(String::as_str), Some("1"));
        assert!(!payload.query.contains_key("wct"), "the routing token is the hop's, never the program's");
        assert!(payload.headers.iter().any(|(k, v)| k == "content-type" && v == "application/json"));
    }

    /// A caller who took too long gets told so, in words that say what
    /// to do, and leaves no execution behind.
    ///
    /// This is the answer the pod is deliberately kept alive a while
    /// longer to be able to give. The dispatcher holds the worker past
    /// the ticket's own expiry precisely so a late caller reaches
    /// something that can read their ticket and explain, instead of a
    /// socket that simply does not answer.
    #[tokio::test]
    async fn a_caller_whose_ticket_ran_out_is_told_to_ask_again() {
        use tower::ServiceExt as _;
        let (mut state, _) = server_state();
        let tasks = Arc::new(ArrivalTasks::default());
        state.tasks = tasks.clone();
        let (token, _) = routing_token(&[], "pod-a", state.clock.now_unix() - 1);
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/chat/room7?wct={token}"))
            .body(axum::body::Body::from("{}"))
            .unwrap();
        let response = connection_router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("expired"), "the reason is named: {body}");
        assert!(body.contains("Ask for a new one"), "and what to do about it: {body}");
        assert!(tasks.asked.lock().unwrap().is_empty(), "a refused caller starts nothing");
    }

    /// A token for another pod, or a forged one, never asks for a birth.
    #[tokio::test]
    async fn a_token_for_another_pod_asks_for_nothing() {
        use tower::ServiceExt as _;
        let (mut state, _) = server_state();
        let tasks = Arc::new(ArrivalTasks::default());
        state.tasks = tasks.clone();
        let (token, _) = routing_token(&[], "pod-b", state.clock.now_unix() + 60);
        let request = axum::http::Request::builder()
            .method("GET")
            .uri(format!("/feed?wct={token}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let response = connection_router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(tasks.asked.lock().unwrap().is_empty());
    }

    /// Drive one HTTP exchange: attach with `body`, run `program` against
    /// the attached connection from another task, and hand back the
    /// response's status, headers and whole body.
    async fn exchange<F, Fut>(
        body: &str,
        program: F,
    ) -> (StatusCode, Vec<(String, String)>, String)
    where
        F: FnOnce(Arc<LiveCallerConnection>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let (answer, _) = exchange_recording(body, program).await;
        answer
    }

    /// [`exchange`], plus the canceller, so a test can assert whether the
    /// exchange's end cancelled the run.
    async fn exchange_recording<F, Fut>(
        body: &str,
        program: F,
    ) -> ((StatusCode, Vec<(String, String)>, String), Arc<RecordingCanceller>)
    where
        F: FnOnce(Arc<LiveCallerConnection>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let (state, canceller) = server_state();
        let color = Color::new_v4();
        let registry = state.registry.clone();
        // The program side: wait for the attach, then talk.
        tokio::spawn(async move {
            let conn = registry
                .wait_for_attach(color, std::time::Duration::from_secs(5))
                .await
                .expect("the connection attaches");
            program(conn).await;
        });
        let request = Arc::new(LiveRequest {
            method: "POST".into(),
            path: "chat/room7".into(),
            ..Default::default()
        });
        let response = drive_http(
            state,
            color,
            http_cfg(),
            0,
            request,
            Arc::new(RecordingSink::default()),
            axum::body::Body::from(body.to_string()),
        )
        .await;
        let status = response.status();
        let headers = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        ((status, headers, String::from_utf8(bytes.to_vec()).unwrap()), canceller)
    }

    fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    #[tokio::test]
    async fn first_chunk_commits_a_200_with_its_content_type() {
        let (status, headers, body) = exchange("{}", |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn) else { unreachable!() };
            http.write(OutboundChunk::Json(serde_json::json!({"a": 1}))).await.unwrap();
            http.respond(OutboundChunk::Json(serde_json::json!({"b": 2}))).await.unwrap();
        })
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(header(&headers, "content-type"), Some("application/json"));
        assert_eq!(header(&headers, "x-accel-buffering"), Some("no"));
        assert_eq!(body, r#"{"a":1}{"b":2}"#);
    }

    /// Start one HTTP exchange the way a streaming caller does: the
    /// response comes back as soon as the program commits the head, and
    /// the test reads (or drops) its body itself. `heartbeat_secs` as
    /// the worker would pass it.
    async fn open_exchange<F, Fut>(
        heartbeat_secs: u64,
        program: F,
    ) -> (Response, Color, Arc<RecordingCanceller>)
    where
        F: FnOnce(Arc<LiveCallerConnection>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let (state, canceller) = server_state();
        let color = Color::new_v4();
        let registry = state.registry.clone();
        tokio::spawn(async move {
            let conn = registry
                .wait_for_attach(color, std::time::Duration::from_secs(5))
                .await
                .expect("the connection attaches");
            program(conn).await;
        });
        let request = Arc::new(LiveRequest { method: "GET".into(), path: "feed".into(), ..Default::default() });
        let response = drive_http(
            state,
            color,
            http_cfg(),
            heartbeat_secs,
            request,
            Arc::new(RecordingSink::default()),
            axum::body::Body::empty(),
        )
        .await;
        (response, color, canceller)
    }

    /// A program that streams a first event under an SSE head and then
    /// says nothing (a watched table that never changes).
    async fn quiet_feed(conn: Arc<LiveCallerConnection>) {
        let CallerHandle::Http(http) = CallerHandle::from_connection(conn) else { unreachable!() };
        let head = ResponseHead::new(200)
            .with_header("content-type", "text/event-stream")
            .with_keepalive(": keepalive\n\n");
        http.write_with(head, OutboundChunk::Text("data: first\n\n".into())).await.unwrap();
        std::future::pending::<()>().await;
    }

    /// The feed is quiet, the caller is still there: every heartbeat
    /// writes the head's filler, which an SSE reader ignores, so a proxy
    /// on the way sees traffic and a dead socket is found by the write.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_stream_writes_its_filler_on_every_heartbeat() {
        use tokio_stream::StreamExt as _;
        let (response, _, _) = open_exchange(2, quiet_feed).await;
        let mut body = response.into_body().into_data_stream();
        let first = body.next().await.unwrap().unwrap();
        assert_eq!(&first[..], b"data: first\n\n");
        for _ in 0..2 {
            let filler = body.next().await.unwrap().unwrap();
            assert_eq!(&filler[..], b": keepalive\n\n");
        }
    }

    /// The caller hangs up while the feed is quiet: the body's receiver
    /// goes away with the response, the drainer sees it without the
    /// program writing a byte, and the run is cancelled (the route
    /// cannot suspend), with the disconnect journaled.
    #[tokio::test]
    async fn a_caller_leaving_a_quiet_stream_cancels_the_run_without_a_write() {
        use tokio_stream::StreamExt as _;
        let (response, color, canceller) = open_exchange(0, quiet_feed).await;
        let mut body = response.into_body().into_data_stream();
        let first = body.next().await.unwrap().unwrap();
        assert_eq!(&first[..], b"data: first\n\n");
        drop(body);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while canceller.cancelled.lock().unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline, "the run was never cancelled after the caller left");
            tokio::task::yield_now().await;
        }
        assert_eq!(canceller.cancelled.lock().unwrap().as_slice(), &[color]);
    }

    /// A feed that has not said ANYTHING yet: the bus it watches was
    /// quiet from the moment the route opened, so no head has gone out
    /// and the drainer has no filler to write. The caller is still owed
    /// a response, so the handler is parked on the head. When that
    /// caller goes, hyper drops the handler; nothing of ours is left
    /// holding the body, and the run has to end there, because the
    /// heartbeat has no way to find out on its own.
    #[tokio::test]
    async fn a_caller_leaving_before_the_first_byte_still_ends_the_run() {
        let (state, canceller) = server_state();
        let color = Color::new_v4();
        let registry = state.registry.clone();
        tokio::spawn(async move {
            let conn = registry
                .wait_for_attach(color, std::time::Duration::from_secs(5))
                .await
                .expect("the connection attaches");
            let CallerHandle::Http(_http) = CallerHandle::from_connection(conn) else { unreachable!() };
            // Never writes: the watched table never changed.
            std::future::pending::<()>().await;
        });
        let request = Arc::new(LiveRequest { method: "GET".into(), path: "feed".into(), ..Default::default() });
        // The handler parks on the head, which is where a real one sits
        // too; dropping the task is what hyper does to it when the
        // connection goes.
        let handler = tokio::spawn(drive_http(
            state,
            color,
            http_cfg(),
            1,
            request,
            Arc::new(RecordingSink::default()),
            axum::body::Body::empty(),
        ));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!handler.is_finished(), "with nothing written the handler is still holding the head");
        handler.abort();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while canceller.cancelled.lock().unwrap().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "a caller who left before the first byte never ended the run"
            );
            tokio::task::yield_now().await;
        }
        assert_eq!(canceller.cancelled.lock().unwrap().as_slice(), &[color]);
    }

    /// A run that answered ended the exchange itself: it is left to
    /// finish (its last nodes still run), never cancelled as if the
    /// caller had left.
    #[tokio::test]
    async fn a_run_that_answers_is_not_cancelled_when_its_response_completes() {
        let ((status, _, _), canceller) = exchange_recording("{}", |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn) else { unreachable!() };
            http.respond(OutboundChunk::Json(serde_json::json!({"ok": true}))).await.unwrap();
        })
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(canceller.cancelled.lock().unwrap().is_empty(), "the program ended the exchange; the run finishes on its own");
    }

    /// The words are the fact: which ends are the caller's, and which
    /// the program's.
    #[test]
    fn only_a_callers_end_cancels_a_tied_run() {
        for end in [
            ExchangeEnd::CallerHungUp,
            ExchangeEnd::CallerHungUpOnSend,
            ExchangeEnd::CallerClosedSocket,
            ExchangeEnd::SocketTransportError,
            ExchangeEnd::SocketEnded,
            ExchangeEnd::CallerMissedHeartbeat,
            ExchangeEnd::InboundTooLarge,
            ExchangeEnd::InboundDecodeFailed,
            ExchangeEnd::SessionCapExceeded,
        ] {
            assert!(end.caller_initiated(), "{}", end.as_str());
        }
        for end in [
            ExchangeEnd::ResponseComplete,
            ExchangeEnd::ResponseErrored,
            ExchangeEnd::OutboundQueueClosed,
            ExchangeEnd::SessionClosedByProgram,
            ExchangeEnd::SessionErroredByProgram,
        ] {
            assert!(!end.caller_initiated(), "{}", end.as_str());
        }
    }

    #[tokio::test]
    async fn a_bare_close_is_204_with_no_body() {
        let (status, _headers, body) = exchange("", |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn) else { unreachable!() };
            http.close().await.unwrap();
        })
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn an_error_before_the_first_byte_is_a_real_500() {
        let (status, headers, body) = exchange("{}", |conn| async move {
            conn.surface_error("the node blew up").await;
        })
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(header(&headers, "content-type"), Some("text/plain; charset=utf-8"));
        assert_eq!(body, "the node blew up");
    }

    #[tokio::test]
    async fn a_run_that_ends_without_answering_is_a_loud_500() {
        let (status, headers, body) = exchange("{}", |conn| async move {
            conn.run_ended().await;
        })
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(header(&headers, "content-type"), Some("text/plain; charset=utf-8"));
        assert_eq!(body, "the run ended without answering");
    }

    #[tokio::test]
    async fn a_run_that_ends_mid_stream_completes_the_body() {
        let (status, _headers, body) = exchange("{}", |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn.clone()) else { unreachable!() };
            http.write(OutboundChunk::Text("partial".into())).await.unwrap();
            conn.run_ended().await;
        })
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "partial");
    }

    #[tokio::test]
    async fn a_run_that_already_answered_is_left_alone_at_its_end() {
        let (status, _headers, body) = exchange("{}", |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn.clone()) else { unreachable!() };
            http.respond(OutboundChunk::Text("done".into())).await.unwrap();
            conn.run_ended().await;
        })
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "done");
    }

    #[tokio::test]
    async fn an_explicit_head_sets_status_and_headers_for_the_first_chunk() {
        let (status, headers, body) = exchange("{}", |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn) else { unreachable!() };
            let head = ResponseHead::new(202).with_header("x-run", "r-1");
            http.write_with(head, OutboundChunk::Text("one ".into())).await.unwrap();
            http.write(OutboundChunk::Text("two".into())).await.unwrap();
            http.close().await.unwrap();
        })
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(header(&headers, "x-run"), Some("r-1"));
        assert_eq!(header(&headers, "content-type"), Some("text/plain; charset=utf-8"));
        assert_eq!(body, "one two");
    }

    #[tokio::test]
    async fn respond_with_sets_the_status_of_a_one_shot_answer() {
        let (status, headers, body) = exchange("{}", |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn) else { unreachable!() };
            let head = ResponseHead::new(404).with_header("content-type", "application/problem+json");
            http.respond_with(head, OutboundChunk::Json(serde_json::json!({"missing": true})))
                .await
                .unwrap();
        })
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(header(&headers, "content-type"), Some("application/problem+json"), "an explicit content type is kept");
        assert_eq!(body, r#"{"missing":true}"#);
    }

    #[tokio::test]
    async fn close_with_answers_a_bodiless_status() {
        let (status, _headers, body) = exchange("{}", |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn) else { unreachable!() };
            http.close_with(ResponseHead::new(403)).await.unwrap();
        })
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn the_handshake_and_the_body_reach_the_request_parts() {
        let (status, _headers, body) = exchange(r#"{"text":"hi"}"#, |conn| async move {
            let CallerHandle::Http(http) = CallerHandle::from_connection(conn.clone()) else { unreachable!() };
            let parts = http.request_parts().unwrap();
            assert_eq!(parts.request.method, "POST");
            assert_eq!(parts.request.path, "chat/room7");
            assert_eq!(conn.handshake().path, "chat/room7");
            assert_eq!(parts.body, InboundMessage::Json(serde_json::json!({"text": "hi"})));
            http.respond(OutboundChunk::Text("seen".into())).await.unwrap();
        })
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "seen");
    }

    /// A program that ends its exchange without ever answering (the queue
    /// closes with the head still held) gets a loud `500` rather than a
    /// caller hanging forever on a head that never comes.
    #[tokio::test]
    async fn a_silent_end_answers_500_with_the_reason() {
        let (state, _canceller) = server_state();
        let color = Color::new_v4();
        let registry = state.registry.clone();
        let request = Arc::new(LiveRequest::default());
        let cfg = CallerRuntimeConfig { max_session_secs: 1, ..http_cfg() };
        let (status, body) = {
            // Nobody talks; the session cap (1s on the fake clock, which
            // returns at once) ends the exchange with the head held.
            let response = drive_http(
                state,
                color,
                cfg,
                0,
                request,
                Arc::new(RecordingSink::default()),
                axum::body::Body::from("{}"),
            )
            .await;
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, String::from_utf8(bytes.to_vec()).unwrap())
        };
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("session cap exceeded"), "got: {body}");
        assert!(registry.get(color).is_none(), "the connection was detached");
    }

    #[tokio::test]
    async fn a_head_after_the_first_item_is_refused_at_the_connection() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, _inb) = new_connection(
            http_cfg(),
            Color::nil(),
            Arc::new(LiveRequest::default()),
            Some(InboundMessage::Json(serde_json::Value::Null)),
            sink,
        );
        conn.send_chunk(None, OutboundChunk::Text("a".into())).await.unwrap();
        let err = conn
            .send_chunk(Some(ResponseHead::new(201)), OutboundChunk::Text("b".into()))
            .await
            .expect_err("the head is committed by the first item");
        assert!(matches!(err, CallerError::HeadAlreadySent));
        // A refused head did not consume the terminal: a plain close lands.
        conn.terminate(None, None, None).await.expect("still open");
    }

    /// A WebSocket has no head beyond its upgrade: a head handed to a
    /// websocket connection is dropped before the queue, and a close
    /// carries its code and reason to the drainer.
    #[tokio::test]
    async fn a_websocket_drops_heads_and_queues_its_close_reason() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, out, _inb) =
            new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
        conn.send_chunk(Some(ResponseHead::new(201)), OutboundChunk::Text("a".into()))
            .await
            .unwrap();
        let first = out.recv().await.unwrap();
        assert!(matches!(first, Outbound::Chunk(_)), "no Head item on a websocket: {first:?}");
        conn.terminate(None, None, Some(CloseReason { code: 4000, reason: "done".into() }))
            .await
            .unwrap();
        match out.recv().await.unwrap() {
            Outbound::Terminate(None, Some(close)) => {
                assert_eq!(close.code, 4000);
                assert_eq!(close.reason, "done");
            }
            other => panic!("expected the close reason, got {other:?}"),
        }
    }

    /// A socket run that ends with the caller still on the line closes
    /// the socket normally: silence on a socket is not an error, and a
    /// run that already closed it is left alone.
    #[tokio::test]
    async fn a_websocket_run_that_ends_closes_the_socket_normally() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, out, _inb) =
            new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
        conn.run_ended().await;
        assert!(
            matches!(out.recv().await.unwrap(), Outbound::Terminate(None, None)),
            "a normal close, no error"
        );
        conn.run_ended().await;
        out.close();
        assert!(out.recv().await.is_none(), "the second end of run queues nothing");
    }

    #[test]
    fn an_empty_json_body_decodes_to_null() {
        assert_eq!(decode_inbound(DataType::Json, b"").unwrap(), InboundMessage::Json(serde_json::Value::Null));
        assert_eq!(decode_inbound(DataType::Json, b" \n").unwrap(), InboundMessage::Json(serde_json::Value::Null));
        assert!(decode_inbound(DataType::Json, b"nope").is_err());
        assert_eq!(decode_inbound(DataType::Text, b"").unwrap(), InboundMessage::Text(String::new()));
    }

    // `receive()` waits indefinitely, with no deadline, and `inbound.close()`
    // (the disconnect / session-cap path) wakes the parked reader as a
    // `Disconnected`. Stress-looped multi-thread: the close-wakes-reader path
    // is the lost-wakeup risk; a missed wakeup HANGS `recv.await`.
    weft_core::stress_test! {
        name: receive_waits_indefinitely_then_disconnects_on_close,
        runs: 80,
        worker_threads: 4,
        async fn body() {
            let sink = std::sync::Arc::new(RecordingSink::default());
            let (conn, _out, inbound) = new_connection(ws_cfg(), Color::nil(), Arc::new(LiveRequest::default()), None, sink);
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
