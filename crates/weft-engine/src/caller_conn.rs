//! Production `CallerConnection` (worker side of a live caller
//! connection), and the server callers arrive at.
//!
//! A caller's connection object is made where its run is made: the door
//! lets the call in, the run is born, and the run starts with its caller's
//! connection in hand. A plain HTTP call answered in one piece runs on the
//! very task that took the call ([`answer_http`]): the handler polls the
//! run until the program decides the answer, returns the response, and
//! spawns the run only when it has work left after answering. A streamed
//! answer and a websocket keep one task that feeds the wire.
//!
//! Shape (one connection per execution):
//!   - OUTBOUND: nodes call `send_chunk` / `terminate`; the connection
//!     pushes onto a bounded single-consumer `OutboundQueue` the socket
//!     task drains to the wire. The queue is a `VecDeque` the PRODUCER can
//!     evict the front of, so all three backpressure policies are real:
//!     `block` awaits a slot, `drop_newest` sheds the incoming chunk,
//!     `drop_oldest` pops the front and enqueues (so one slow caller
//!     cannot grow a multiplexing process's RAM). Terminal items always land.
//!   - INBOUND (WebSocket): the socket task publishes each decoded
//!     message onto a bounded `InboundLog`; every node's `receive` holds
//!     its own absolute-offset cursor over the same window, so inbound
//!     BROADCASTS to all listeners (the model we settled on).
//!   - HTTP request parts are captured once at attach and read via
//!     `http_request`.
//!   - The terminate-once latch + connected flag live behind one mutex.
//!   - The exchange is projected to `Caller*` journal rows (connect /
//!     inbound / outbound / error / disconnect) through the same kind of
//!     pump the bus uses, so the inspector replays it, for a websocket and
//!     a streamed answer. A call answered in one piece records none but an
//!     error: its request is the trigger's kick payload and its answer is
//!     what the program answered with, both on record already.
//!
//! This server speaks plain HTTP/WS. Every call is checked by the door
//! (`crate::door`); a call weft's relay passed on is recognised by weft's
//! hop credential, and a browser's socket shows the ticket a worker's door
//! signed.

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
use weft_core::signal::{Backpressure, DataType, Protocol};
use weft_core::ExecutionId;

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
    pub(crate) async fn recv(&self) -> Option<Outbound> {
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
    /// Where the `Caller*` observability rows go, each at its offset.
    record: CallerRecord,
    inner: Mutex<ConnInner>,
    /// Whether the caller is attached, as a watch so a hold waiting on
    /// the caller (`disconnected()`) wakes the moment it hangs up.
    connected: tokio::sync::watch::Sender<bool>,
    /// Whether the disconnect was handed out: the exactly-once guard,
    /// apart from `connected` so the row is written BEFORE anyone waiting
    /// on `connected` wakes (see `mark_disconnected`).
    disconnect_recorded: AtomicBool,
    /// Whether the caller's arrival is on the exchange's record: at once
    /// for a socket; for an HTTP call only once it streams, since a call
    /// answered in one piece records none of it ([`Self::caller_arrived`]).
    arrival_recorded: AtomicBool,
    /// A durable run's last word to its caller, waiting for the run's
    /// record to hold everything before it ([`Self::release_answer`]). It
    /// stays here until it is pushed, so the run's end always finds it.
    held: Mutex<Option<Outbound>>,
    /// Held across a chunk's record, commit and push, and across the last
    /// word's latch and queueing: what the caller receives is in the order
    /// its record says, and a chunk on its way out is never overtaken by
    /// the last word.
    sending: tokio::sync::Mutex<()>,
    /// Told when a last word is held, so a drive waiting on something else
    /// lets it go ([`Self::answer_held`]).
    held_note: tokio::sync::Notify,
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

/// Why an exchange whose run parked ends here: the run outlives its
/// caller (`outlivesCaller`), so its worker leaves while it waits and
/// whatever it says once resumed goes nowhere. Said to the caller (an
/// error before the first byte, in-band after it, a `1011` close on a
/// socket) and recorded on the exchange.
pub const PARKED: &str = "the run is waiting on a signal and carries on without this caller \
     (its route outlives its caller): no answer will come on this request";

/// Why an exchange whose run was handed back ends here: the worker it
/// reached is stopping, so the run carries on in another one, which this
/// caller's connection does not reach. Said and recorded like [`PARKED`].
pub const HANDED_BACK: &str = "the copy of the program serving this request is stopping, so the run \
     carries on in another one without this caller: no answer will come on this request";

impl ConnInner {
    /// An HTTP caller that has not heard a word yet: the run owes it an
    /// answer, and ending without one is a failure. The one rule,
    /// read by `owes_answer` and `run_ended`.
    fn unanswered_http(&self, protocol: Protocol) -> bool {
        protocol == Protocol::Http && !self.wire_started
    }
}

impl LiveCallerConnection {
    /// The sink the exchange is recorded through, for the run to close
    /// once it is over.
    pub fn journal(&self) -> Arc<dyn CallerJournalSink> {
        self.record.sink()
    }

    /// Resolves once a last word is held (a permit: one held before the
    /// wait began counts).
    pub(crate) async fn answer_held(&self) {
        self.held_note.notified().await
    }

    /// Whether a last word waits for the run's record (see `held`).
    pub(crate) fn holds_answer(&self) -> bool {
        self.held.lock().expect("caller conn poisoned").is_some()
    }

    /// Let the held last word go once the run's record holds everything
    /// before it. A record that cannot be written keeps it from leaving:
    /// the caller hears why instead. The answer stays held while the
    /// record is written, so a run ending meanwhile still sees it held and
    /// releases it after its ending; whichever release takes it pushes it,
    /// under the same lock it took it under, so whoever finds nothing held
    /// knows the answer is queued (the hang-up that closes the queue comes
    /// after).
    pub(crate) async fn release_answer(&self) {
        if !self.holds_answer() {
            return;
        }
        let committed = self.record.sink.committed().await;
        let mut held = self.held.lock().expect("caller conn poisoned");
        let Some(answer) = held.take() else { return };
        match committed {
            Ok(()) => self.outbound.push_terminal(answer),
            Err(why) => self.outbound.push_terminal(Outbound::Error(format!("the answer is not on the run's record, so it was not sent: {why}"))),
        }
    }

    /// The run is over and has said its last word: let the socket task
    /// send what is still queued, end the exchange, and record its
    /// `CallerDisconnected`, then return. The run's journal is closed
    /// only after this, so the disconnect row is part of the run's
    /// record. No deadline of its own: the tail waits on the caller
    /// reading it, the same as every other write to the caller, until the
    /// caller leaves or the session cap (when one is set) ends the
    /// exchange; every write is raced against both (`write_or_end`).
    pub async fn hang_up(&self) {
        self.outbound.close();
        CallerConnection::disconnected(self).await;
    }

    /// Record the caller's arrival, once: the connect row at offset zero
    /// and, for HTTP, the request body as the first inbound message (it
    /// arrives with the connection rather than after it). A socket's is
    /// recorded as it opens; an HTTP call's only once its answer streams
    /// (its first `send_chunk`), since a call answered in one piece records
    /// none of its exchange.
    pub(crate) fn caller_arrived(&self) {
        if self.arrival_recorded.swap(true, Ordering::SeqCst) {
            return;
        }
        self.record.connected(self.config.protocol);
        if let Some(http) = &self.http_request {
            self.record.inbound(&http.body);
        }
    }

    /// The exchange is over, for `reason`: said exactly once. The row goes
    /// to the journal first (when the exchange is recorded) and `connected`
    /// flips after: `hang_up` wakes on the flip and the run then closes the
    /// sink, so a flip first could close it before the row was handed over.
    /// Only the first call writes and flips: a second one returning at once
    /// leaves the flip to the first, so it never lands ahead of the row.
    pub(crate) fn mark_disconnected(&self, reason: &str) {
        if !self.disconnect_recorded.swap(true, Ordering::SeqCst) {
            if self.arrival_recorded.load(Ordering::SeqCst) {
                self.record.disconnected(reason);
            }
            self.connected.send_replace(false);
        }
    }

    /// Surface a node/run error to the caller per the error mode. Best
    /// effort: records the `CallerErrored` event and pushes an `Error`
    /// outbound (the socket task turns it into an in-band chunk for HTTP
    /// after streaming started, or a WS close frame with the reason). Used
    /// by the execute path when a live-connection run fails with the
    /// caller still attached, so the caller learns why instead of seeing a
    /// silently dropped socket. A program that already answered keeps its
    /// answer: the error is recorded and never pushed after it, whether
    /// that answer already left or still waits for the run's record.
    pub async fn surface_error(&self, message: &str) {
        self.record.errored(message);
        // Tolerant streams: the chosen mode says swallow it.
        if self.config.error_mode == weft_core::signal::ErrorMode::DropChunk {
            return;
        }
        if self.take_end(|_| ()).is_some() {
            self.outbound.push_terminal(Outbound::Error(message.to_string()));
        }
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
        let Some(silent_http) = self.take_end(|g| g.unanswered_http(self.config.protocol)) else {
            return;
        };
        if silent_http {
            let message = weft_core::caller::NO_ANSWER;
            self.record.errored(message);
            self.outbound.push_terminal(Outbound::Error(message.to_string()));
        } else {
            self.outbound.push_terminal(Outbound::Terminate(None, None));
        }
    }

    /// The run carries on without this caller (it parked on a wait,
    /// [`PARKED`], or was handed back, [`HANDED_BACK`]): it has NOT ended,
    /// so this never says it did. Whatever the exchange was doing, it
    /// stops here with `why`. A program that already terminated the
    /// exchange is left alone.
    pub async fn run_carries_on(&self, why: &str) {
        if self.take_end(|_| ()).is_none() {
            return;
        }
        self.record.errored(why);
        self.outbound.push_terminal(Outbound::Error(why.to_string()));
    }

    /// Claim the exchange's end once: `None` when it was already
    /// terminated, otherwise `read` of the state before the end marks
    /// the wire started.
    fn take_end<T>(&self, read: impl FnOnce(&ConnInner) -> T) -> Option<T> {
        let mut g = self.inner.lock().expect("caller conn poisoned");
        if g.terminated {
            return None;
        }
        g.terminated = true;
        let seen = read(&g);
        g.wire_started = true;
        Some(seen)
    }

    /// Resolve a gone-caller talk into the policy-correct outcome (cancel
    /// vs void), identical to the fake's contract.
    fn disconnected_outcome(&self) -> Result<(), CallerError> {
        match resolve_disconnect(self.config.outlives_caller) {
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
        // Nothing follows the exchange's last word: a write after it
        // would reach the caller ahead of an answer still held for the
        // run's record, or be recorded as said when nobody heard it.
        try_terminate(g.terminated)?;
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

    fn owes_answer(&self) -> bool {
        self.is_connected()
            && self.inner.lock().expect("caller conn poisoned").unanswered_http(self.config.protocol)
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
        let _sending = self.sending.lock().await;
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
        if let Some(why) = self.record.sink.degraded() {
            return Err(CallerError::JournalLost(why));
        }
        // An answer that streams is recorded whole, its caller's arrival
        // first.
        self.caller_arrived();
        self.record.outbound(&chunk, false);
        // A durable run's answer leaves only once it is on record.
        self.record.sink.committed().await.map_err(CallerError::JournalLost)?;
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
        let _sending = self.sending.lock().await;
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
        // The last words of a recorded exchange; an HTTP answer in one
        // piece records none of its exchange.
        if let (Some(c), true) = (&final_chunk, self.arrival_recorded.load(Ordering::SeqCst)) {
            self.record.outbound(c, true);
        }
        let terminal = Outbound::Terminate(final_chunk, close);
        if self.record.sink.waits_for_commit() {
            // A durable run's answer leaves only once it is on record. It
            // is held rather than waited for here: the drive lets it go
            // once the record holds it, and when the answer is the run's
            // last act, the run's ending is in that same write
            // (`execution_driver`, `Self::release_answer`).
            *self.held.lock().expect("caller conn poisoned") = Some(terminal);
            self.held_note.notify_one();
            return Ok(());
        }
        // The terminal always lands (subject to no capacity policy); if the
        // socket task already ended (caller gone), it is silently dropped
        // (the exchange is over anyway).
        self.outbound.push_terminal(terminal);
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
/// session-cap firing (which closes the log -> a disconnect here).
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
/// session via `max_session_secs` to cap a multiplexing process's RAM/abuse.
/// Uses the injected clock so the rig can advance it deterministically.
/// One write to the caller, raced against the exchange ending under it:
/// `gone` (the caller's side going away, where the transport says so
/// without a write) and the `session` cap. A caller that stops reading
/// would otherwise hold the write, and the drainer and `hang_up` with it,
/// past both. `Ok` carries whether the write went through.
async fn write_or_end(
    write: impl std::future::Future<Output = bool>,
    gone: impl std::future::Future<Output = ()>,
    session: &mut futures::future::BoxFuture<'static, ()>,
) -> Result<bool, ExchangeEnd> {
    tokio::select! {
        ok = write => Ok(ok),
        () = gone => Err(ExchangeEnd::CallerHungUp),
        () = session => Err(ExchangeEnd::SessionCapExceeded),
    }
}

fn session_deadline(clock: &Arc<dyn weft_platform_traits::Clock>, cap_secs: u64) -> futures::future::BoxFuture<'static, ()> {
    let clock = clock.clone();
    Box::pin(async move {
        if cap_secs == 0 {
            std::future::pending::<()>().await;
        } else {
            clock.sleep(std::time::Duration::from_secs(cap_secs)).await;
        }
    })
}

/// One exchange's record: the sink its `Caller*` rows go to and the
/// counter that hands each row the next offset, starting at zero. Every
/// caller (a live connection, a fired run's stand-in) writes through
/// one, so their rows number the same way.
pub(crate) struct CallerRecord {
    execution_id: ExecutionId,
    sink: Arc<dyn CallerJournalSink>,
    next_offset: AtomicU64,
}

impl CallerRecord {
    pub(crate) fn new(execution_id: ExecutionId, sink: Arc<dyn CallerJournalSink>) -> Self {
        Self { execution_id, sink, next_offset: AtomicU64::new(0) }
    }

    pub(crate) fn sink(&self) -> Arc<dyn CallerJournalSink> {
        self.sink.clone()
    }

    fn take_offset(&self) -> u64 {
        self.next_offset.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn connected(&self, protocol: Protocol) {
        self.sink.connected(self.execution_id, self.take_offset(), protocol);
    }

    pub(crate) fn inbound(&self, msg: &InboundMessage) {
        self.sink.inbound(self.execution_id, self.take_offset(), msg);
    }

    pub(crate) fn outbound(&self, chunk: &OutboundChunk, terminal: bool) {
        self.sink.outbound(self.execution_id, self.take_offset(), chunk, terminal);
    }

    pub(crate) fn errored(&self, message: &str) {
        self.sink.errored(self.execution_id, self.take_offset(), message);
    }

    pub(crate) fn disconnected(&self, reason: &str) {
        self.sink.disconnected(self.execution_id, self.take_offset(), reason);
    }
}

/// Sink for the `Caller*` observability events. The engine wires the
/// real journal-backed impl; tests pass a recording fake. Mirrors the
/// bus journal pump's projection (connect / inbound / outbound / error /
/// disconnect, each with an offset).
pub trait CallerJournalSink: Send + Sync {
    fn connected(&self, execution_id: ExecutionId, offset: u64, protocol: Protocol);
    fn inbound(&self, execution_id: ExecutionId, offset: u64, msg: &InboundMessage);
    fn outbound(&self, execution_id: ExecutionId, offset: u64, chunk: &OutboundChunk, terminal: bool);
    fn errored(&self, execution_id: ExecutionId, offset: u64, message: &str);
    fn disconnected(&self, execution_id: ExecutionId, offset: u64, reason: &str);

    /// Stop taking rows, and hand over what is held. A run closes its
    /// caller's sink once it is over, before it lets go of its record
    /// (waiting for it when it carries on elsewhere, or is durable) and
    /// before an unrecorded run's record is taken.
    fn close(&self);

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

    /// Resolve once what was handed so far is on record, for a run whose
    /// answer may not leave before it is (a durable run): the exchange's
    /// open window is written and waited for. Resolves at once for a run
    /// that does not wait (the default). `Err` is why it is not.
    fn committed(&self) -> futures::future::BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    /// Whether an answer waits for [`Self::committed`] before it leaves:
    /// a durable run's (the default does not).
    fn waits_for_commit(&self) -> bool {
        false
    }
}

/// Build a connection + the socket-facing channels. Returns the shared
/// `Arc<LiveCallerConnection>` (handed to the run as its caller) and the
/// halves the exchange's side drives: the outbound queue (drain to wire)
/// and the inbound log (push decoded messages, close on socket end).
#[allow(clippy::type_complexity)]
pub(crate) fn new_connection(
    config: CallerRuntimeConfig,
    execution_id: ExecutionId,
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
        record: CallerRecord::new(execution_id, journal.clone()),
        inner: Mutex::new(ConnInner { terminated: false, wire_started: false }),
        connected: tokio::sync::watch::Sender::new(true),
        disconnect_recorded: AtomicBool::new(false),
        arrival_recorded: AtomicBool::new(false),
        held: Mutex::new(None),
        held_note: tokio::sync::Notify::new(),
        sending: tokio::sync::Mutex::new(()),
    });
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

/// Shared state for the connection server: the door every call arrives at,
/// and what bears its run.
#[derive(Clone)]
pub struct ConnServerState {
    /// Where every call is checked before its run is born (`crate::door`).
    pub door: Arc<crate::door::Door>,
    /// Weft's own credential on a hop: a call carrying it was passed on by
    /// weft's relay, whose word on the caller's address is taken.
    pub weft_hop: crate::worker::WeftCredential,
    /// Gives birth to the run of a call the door let in.
    pub starter: Arc<dyn LiveStarter>,
    /// Worker clock (for the now()-based session deadline / heartbeat).
    pub clock: Arc<dyn weft_platform_traits::Clock>,
    /// Fires the per-execution cancel flag (cancel-on-disconnect for a
    /// caller-tied run). Looked up by execution.
    pub canceller: Arc<dyn ExecutionCanceller>,
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
    /// The caller's socket upgrade never completed (socket).
    SocketNeverOpened,
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
            Self::SocketNeverOpened => "the socket never opened",
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
            | Self::SocketNeverOpened
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
/// Fires the per-execution cancel flag (cancel-on-disconnect). The worker
/// implements this over its cancel registry.
pub trait ExecutionCanceller: Send + Sync {
    fn cancel(&self, execution_id: ExecutionId, cause: weft_core::exec::CancelCause);
}

/// A run born for a caller: where its exchange is recorded, and its drive,
/// which starts with the caller's connection in hand.
pub struct Born {
    pub execution_id: ExecutionId,
    /// The sink the exchange is recorded through: the run's record.
    pub sink: Arc<dyn CallerJournalSink>,
    /// The run's drive, given its caller's exchange. Whoever holds the
    /// future drives the run: the call's own task for an HTTP call, a task
    /// of its own for a socket.
    pub drive: Box<dyn FnOnce(crate::execution_driver::Exchange) -> futures::future::BoxFuture<'static, ()> + Send>,
}

/// Gives birth to the run of a caller the door let in.
#[async_trait]
pub trait LiveStarter: Send + Sync {
    /// Bear the run (its plan, its birth handed to its record). The error
    /// is the answer the caller gets instead; nothing started then.
    async fn bear(&self, admitted: Box<crate::door::Admitted>) -> Result<Born, Response>;
}

/// The caller of `conn` is gone (their side ended the exchange, for
/// `end`): the exchange stops, a run tied to its caller is cancelled, and
/// the connection says the caller left. The cancel lands before the caller
/// reads as gone, so whatever wakes on the caller leaving
/// (`weft_engine::worker`'s move off a worker no call holds open) finds the
/// run already cancelled.
fn exchange_ended(conn: &LiveCallerConnection, outbound: &OutboundQueue, canceller: &dyn ExecutionCanceller, execution_id: ExecutionId, end: ExchangeEnd) {
    outbound.close();
    if end.caller_initiated() && matches!(resolve_disconnect(conn.config.outlives_caller), DisconnectAction::CancelExecution) {
        canceller.cancel(execution_id, weft_core::exec::CancelCause::CallerGone);
    }
    conn.mark_disconnected(end.as_str());
}

/// A run its HTTP call drives on its own task ([`answer_http`]). Handed
/// on to a task of its own once the answer leaves with work left
/// ([`Self::carry_on`]); dropped with the call before that (the caller
/// went away), it ends the exchange as the caller's and still goes on to
/// its end on a task of its own, so a run never stops mid-step because
/// its caller left.
struct RunHere {
    run: Option<futures::future::BoxFuture<'static, ()>>,
    conn: Arc<LiveCallerConnection>,
    outbound: Arc<OutboundQueue>,
    canceller: Arc<dyn ExecutionCanceller>,
    execution_id: ExecutionId,
}

impl RunHere {
    /// Poll the run once more, and hand it to a task of its own when it
    /// still has work left.
    fn carry_on(mut self) {
        if let Some(mut run) = self.run.take() {
            if futures::FutureExt::now_or_never(&mut run).is_none() {
                tokio::spawn(run);
            }
        }
    }
}

impl Drop for RunHere {
    fn drop(&mut self) {
        if let Some(run) = self.run.take() {
            exchange_ended(&self.conn, &self.outbound, self.canceller.as_ref(), self.execution_id, ExchangeEnd::CallerHungUp);
            tokio::spawn(run);
        }
    }
}

/// A socket's run, started before its caller's socket opened. Dropped
/// before [`Self::opened`] (the upgrade never completed), the caller never
/// came: the exchange ends, and a run tied to its caller is cancelled
/// instead of running with nobody on the line.
struct Unopened {
    conn: Arc<LiveCallerConnection>,
    outbound: Arc<OutboundQueue>,
    canceller: Arc<dyn ExecutionCanceller>,
    execution_id: ExecutionId,
    armed: bool,
}

impl Unopened {
    /// The socket opened; from here the socket's task owns the exchange.
    fn opened(mut self) {
        self.armed = false;
    }
}

impl Drop for Unopened {
    fn drop(&mut self) {
        if self.armed {
            exchange_ended(&self.conn, &self.outbound, self.canceller.as_ref(), self.execution_id, ExchangeEnd::SocketNeverOpened);
        }
    }
}

/// Build the connection server router: every path but the worker's own
/// (`/_weft/`, `weft_core::route::RESERVED_PREFIX`) is a caller arriving at
/// the door, any method (HTTP verbs and the WS upgrade GET all land here).
pub fn connection_router(state: ConnServerState) -> Router {
    Router::new().fallback(any(handle_connect)).with_state(state)
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
struct CallerSocket {
    socket: Option<std::sync::Arc<socket2::Socket>>,
    /// The address the connection came from.
    peer: std::net::IpAddr,
}

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
        CallerSocket { socket: watch_for_a_vanished_caller(stream.io()), peer: stream.remote_addr().ip() }
    }
}

impl CallerSocket {
    /// Give this connection its own bound on how long the caller's
    /// machine may leave what we send unacknowledged. `0` means leave
    /// the machine's own default, which is about fifteen minutes.
    fn bound_silence(&self, secs: u64) {
        if let Some(socket) = &self.socket {
            bound_silence(socket, secs);
        }
    }
}

/// See [`CallerSocket::bound_silence`].
fn bound_silence(socket: &socket2::Socket, secs: u64) {
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

/// Serve the worker's router (its own endpoints and the connection
/// server) on `0.0.0.0:port` (plain HTTP/WS; TLS terminates in front of
/// the worker) until `shutdown` resolves.
pub async fn serve(
    app: Router,
    port: u16,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(target: "weft_engine::caller_conn", %addr, "worker listening");
    // Each connection's own socket handle reaches its handler as
    // `ConnectInfo<CallerSocket>` (the floor goes on as it is accepted),
    // which is how the route it turns out to want can set its own
    // silence bound on that one connection.
    axum::serve(listener, app.into_make_service_with_connect_info::<CallerSocket>())
        .with_graceful_shutdown(shutdown)
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
fn watch_for_a_vanished_caller(stream: &tokio::net::TcpStream) -> Option<std::sync::Arc<socket2::Socket>> {
    // A filler line is twenty-odd bytes, and Nagle would hold it back
    // waiting for company that never comes. The write has to reach the
    // wire for the bound below to mean anything.
    let _ = stream.set_nodelay(true);
    let socket = match socket2::SockRef::from(stream).try_clone() {
        Ok(socket) => std::sync::Arc::new(socket),
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
            return None;
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
    if let Err(error) = socket.set_tcp_keepalive(&probes) {
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
    bound_silence(&socket, weft_core::signal::DEFAULT_CALLER_SILENCE_SECS);
    Some(socket)
}

/// What weft's relay said about a call it passed on
/// (`weft_core::net::relay_hop`).
#[derive(Debug, PartialEq, Eq)]
struct Relayed {
    caller: std::net::IpAddr,
    route_prefix: String,
}

/// Read what weft's relay said about this call, when the hop carries
/// weft's own credential, and take every trace of the hop out of
/// `headers`: weft's credential, the relay's headers (anybody else's word
/// under those names is not taken), and the `Host` the hop replaced, put
/// back as the caller sent it. What is left is the request its caller
/// sent, which is what the door checks and the run reads.
fn relayed_by_weft(weft_hop: &crate::worker::WeftCredential, headers: &mut axum::http::HeaderMap) -> Result<Option<Relayed>, Response> {
    use weft_core::net::relay_hop;
    let by_weft = weft_hop.admits_headers(headers);
    let caller = headers.remove(relay_hop::CALLER_ADDRESS);
    let host = headers.remove(relay_hop::CALLER_HOST);
    let route_prefix = headers.remove(relay_hop::ROUTE_PREFIX);
    headers.remove(weft_platform_traits::WORKER_AUTH_HEADER);
    if !by_weft {
        return Ok(None);
    }
    let Some(caller) = caller.as_ref().and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok()) else {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, "weft's relay passed this call on without naming its caller").into_response());
    };
    if let Some(host) = host {
        headers.insert(axum::http::header::HOST, host);
    }
    let route_prefix = route_prefix.as_ref().and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    Ok(Some(Relayed { caller, route_prefix }))
}

/// A caller at the door: the door checks the call
/// ([`crate::door::Door::arrive`]), an HTTP call's body is read, the run it
/// starts is born ([`LiveStarter::bear`]) and starts with the caller's
/// connection in hand: on this very task for an HTTP call
/// ([`answer_http`]), on a task of its own for a socket ([`pump_ws`]).
/// Single-extractor: axum caps handler arity and forbids combining several
/// query/body extractors, so one `Request` is the clean shape here.
async fn handle_connect(
    State(state): State<ConnServerState>,
    request: axum::extract::Request,
) -> Response {
    let socket = request.extensions().get::<axum::extract::ConnectInfo<CallerSocket>>().map(|info| info.0.clone());
    let (mut parts, body) = request.into_parts();
    let relayed = match relayed_by_weft(&state.weft_hop, &mut parts.headers) {
        Ok(relayed) => relayed,
        Err(refused) => return refused,
    };
    let Some(peer) = relayed.as_ref().map(|r| r.caller).or(socket.as_ref().map(|s| s.peer)) else {
        // Every connection is served with its peer; one without is a
        // wiring bug, refused rather than counted as nobody.
        return (StatusCode::INTERNAL_SERVER_ERROR, "no peer address on the request").into_response();
    };
    let method = parts.method.as_str().to_string();
    let raw_path = parts.uri.path().to_string();
    let raw_query = parts.uri.query().unwrap_or("").to_string();
    let call = crate::door::Call {
        method: &method,
        raw_path: &raw_path,
        raw_query: &raw_query,
        headers: &parts.headers,
        peer,
        relayed: relayed.is_some(),
        route_prefix: relayed.as_ref().map_or("", |r| r.route_prefix.as_str()),
    };
    let (admitted, body) = match state.door.arrive(call, body).await {
        crate::door::Arrived::Answer(answer) => return answer,
        crate::door::Arrived::Run(admitted, body) => (admitted, body),
    };
    let config = CallerRuntimeConfig::from_config(&admitted.live_config, admitted.protocol);
    let heartbeat_secs = admitted.live_config.heartbeat_interval_secs;
    let handshake = Arc::new(admitted.opening.clone());
    // What a call brings is read before its run is born: a socket's
    // upgrade (a call without one starts nothing), an HTTP call's body
    // (capped; a body the run could not read starts nothing).
    let opening = match admitted.protocol {
        Protocol::Websocket => match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
            Ok(upgrade) => Opening::Socket(upgrade),
            Err(e) => {
                tracing::warn!(target: "weft_engine::caller_conn", error = ?e, "ws upgrade extraction failed");
                return (StatusCode::BAD_REQUEST, "websocket trigger requires a WebSocket upgrade").into_response();
            }
        },
        Protocol::Http => match read_body(&config, body).await {
            Ok(body) => Opening::Http(body),
            Err(refused) => return refused,
        },
    };
    drop(parts);
    let born = match state.starter.bear(admitted).await {
        Ok(born) => born,
        Err(answer) => return answer,
    };
    // The route is known now, so its own answer to "how long may this
    // caller's machine go silent" replaces the floor put on at accept.
    if let Some(socket) = &socket {
        socket.bound_silence(config.caller_silence_secs);
    }
    let execution_id = born.execution_id;
    let http_body = match &opening {
        Opening::Http(body) => Some(body.clone()),
        Opening::Socket(_) => None,
    };
    let (conn, outbound, inbound) = new_connection(config.clone(), execution_id, handshake, http_body, born.sink);
    let exchange = crate::execution_driver::Exchange { conn: conn.clone(), live: Some(conn.clone()), sink: conn.journal() };
    let run = (born.drive)(exchange);
    match opening {
        Opening::Http(_) => {
            let here = RunHere { run: Some(run), conn, outbound, canceller: state.canceller.clone(), execution_id };
            answer_http(here, heartbeat_secs, state.clock.clone()).await
        }
        Opening::Socket(upgrade) => {
            let unopened = Unopened { conn: conn.clone(), outbound: outbound.clone(), canceller: state.canceller.clone(), execution_id, armed: true };
            // A socket's run has a task of its own; the socket's task feeds
            // the wire.
            tokio::spawn(run);
            let inbound = inbound.expect("a websocket connection has an inbound log");
            // Enforce the inbound size cap at the TRANSPORT so an oversized
            // frame is rejected before axum buffers it whole (the
            // per-message check in `pump_ws` is the loud surface, not the
            // RAM bound). `usize` cast is safe: the cap is a byte count
            // that fits the platform word on any real process.
            let cap = config.max_inbound_bytes as usize;
            let upgrade = upgrade.max_message_size(cap).max_frame_size(cap);
            // A socket that never completes its upgrade drops the closure,
            // and with it `unopened`, which ends the exchange.
            let pump = SocketPump { conn, outbound, inbound, config, heartbeat_secs, clock: state.clock.clone(), canceller: state.canceller.clone(), execution_id };
            upgrade.on_upgrade(move |socket| pump_ws(socket, pump, unopened))
        }
    }
}

/// What a call brings besides its opening request.
enum Opening {
    Http(InboundMessage),
    Socket(WebSocketUpgrade),
}

/// Read an HTTP call's body under the route's cap and decode it as the
/// route's data type. A refusal is the caller's answer.
async fn read_body(config: &CallerRuntimeConfig, body: axum::body::Body) -> Result<InboundMessage, Response> {
    let limit = config.max_inbound_bytes;
    let bytes = axum::body::to_bytes(body, limit as usize)
        .await
        .map_err(|_| (StatusCode::PAYLOAD_TOO_LARGE, format!("request body exceeds {limit} bytes")).into_response())?;
    weft_core::caller::check_inbound_size(bytes.len() as u64, limit).map_err(|e| (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response())?;
    decode_inbound(config.data_type, &bytes).map_err(|e| (StatusCode::BAD_REQUEST, e).into_response())
}

/// An HTTP call's answer, on the call's own task: the run is polled here
/// until the program decides the answer (its first item toward the
/// caller). An answer in one piece (a `respond`, a bare close, an error, a
/// run that ended without a word) is the response, built here, and the run
/// goes on only if it has work left after answering; for a ping it ends in
/// the poll that answered and nothing is spawned. An answer that streams
/// (a first `write`) gets one task that feeds the response's body
/// ([`stream_body`]), and the run goes on on a task of its own. Nothing goes
/// out before the program speaks, so a route can answer 404, set a header,
/// or stream; a program that never replies holds the caller only as long as
/// it runs, and its end answers a `500` saying so (`run_ended`).
async fn answer_http(mut here: RunHere, heartbeat_secs: u64, clock: Arc<dyn weft_platform_traits::Clock>) -> Response {
    let outbound = here.outbound.clone();
    let mut explicit: Option<ResponseHead> = None;
    // The session cap (a configured `max_session_secs`; none never
    // resolves) runs from the call's arrival: it bounds the wait for the
    // answer, then the stream that follows it.
    let mut session = session_deadline(&clock, here.conn.config.max_session_secs);
    let first = loop {
        let item = match here.run.as_mut() {
            Some(run) => tokio::select! {
                biased;
                item = outbound.recv() => Waited::Item(item),
                () = run => Waited::RunOver,
                () = &mut session => Waited::SessionOver,
            },
            None => tokio::select! {
                item = outbound.recv() => Waited::Item(item),
                () = &mut session => Waited::SessionOver,
            },
        };
        match item {
            // The run ended (or paused) before it answered: its last words
            // are in the queue, read on the next turn, and nothing is left
            // to drive.
            Waited::RunOver => here.run = None,
            Waited::SessionOver => {
                let end = ExchangeEnd::SessionCapExceeded;
                exchange_ended(&here.conn, &outbound, here.canceller.as_ref(), here.execution_id, end);
                here.carry_on();
                return build_response(&error_head(), axum::body::Body::from(format!("no response from the program: {}", end.as_str())));
            }
            Waited::Item(Some(Outbound::Head(head))) => explicit = Some(head),
            Waited::Item(decisive) => break decisive,
        }
    };
    let (conn, execution_id) = (here.conn.clone(), here.execution_id);
    let response = match first {
        Some(Outbound::Chunk(chunk)) => {
            let head = committed_head(explicit, Some(&chunk));
            let body = stream_body(StreamedAnswer {
                conn: conn.clone(),
                outbound: outbound.clone(),
                canceller: here.canceller.clone(),
                execution_id,
                head: head.clone(),
                first: chunk,
                heartbeat_secs,
                session,
            });
            build_response(&head, body)
        }
        Some(Outbound::Terminate(last, _close)) => {
            let head = committed_head(explicit, last.as_ref());
            conn.mark_disconnected(ExchangeEnd::ResponseComplete.as_str());
            build_response(&head, axum::body::Body::from(last.as_ref().map(chunk_to_bytes).unwrap_or_default()))
        }
        // Before the first byte the status line is still ours to set: a
        // real error status, the message as the body.
        Some(Outbound::Error(message)) => {
            conn.mark_disconnected(ExchangeEnd::ResponseErrored.as_str());
            build_response(&error_head(), axum::body::Body::from(message))
        }
        Some(Outbound::Head(_)) => unreachable!("a head is held above, never decisive"),
        None => {
            conn.mark_disconnected(ExchangeEnd::OutboundQueueClosed.as_str());
            build_response(&error_head(), axum::body::Body::from(format!("no response from the program: {}", ExchangeEnd::OutboundQueueClosed.as_str())))
        }
    };
    here.carry_on();
    response
}

/// What the wait for an HTTP answer heard.
enum Waited {
    /// The run's next item toward its caller (`None`: the queue closed).
    Item(Option<Outbound>),
    /// The run ended or paused.
    RunOver,
    /// The session cap came first.
    SessionOver,
}

/// The head an answer goes out under: the program's own, else the default
/// for its first item (its content type), and `204` when it carries no
/// body and set none.
fn committed_head(explicit: Option<ResponseHead>, first: Option<&OutboundChunk>) -> ResponseHead {
    match (explicit, first) {
        (Some(head), Some(chunk)) => head.with_content_type_for(chunk),
        (Some(head), None) => head,
        (None, Some(chunk)) => ResponseHead::default().with_content_type_for(chunk),
        (None, None) => ResponseHead::new(204),
    }
}

/// A `500` with a text body: the program failed (or ended silent) before
/// the first byte, so the status line can still say so.
fn error_head() -> ResponseHead {
    ResponseHead::new(500).with_content_type_for(&OutboundChunk::Text(String::new()))
}

/// What feeds a streamed answer's body.
struct StreamedAnswer {
    conn: Arc<LiveCallerConnection>,
    outbound: Arc<OutboundQueue>,
    canceller: Arc<dyn ExecutionCanceller>,
    execution_id: ExecutionId,
    /// The head that went out: the body's framing (its keepalive filler,
    /// how an error is written in-band).
    head: ResponseHead,
    first: OutboundChunk,
    heartbeat_secs: u64,
    /// The session cap, started when the call arrived.
    session: futures::future::BoxFuture<'static, ()>,
}

/// A streamed answer's body, fed by one task from the run's outbound
/// queue.
///
/// A caller who leaves is found two ways, neither of which needs the
/// program to write. The body's receiver is dropped when the response goes
/// away (the connection reset under it), and `tx.closed()` says so at once.
/// And on every heartbeat the feeder writes the head's `keepalive` filler
/// (the bytes a reader of that framing ignores), because a proxy on the way
/// may keep our side open after the far side hung up, and only a write
/// finds that out. A head with no filler (a raw stream) relies on the
/// receiver alone.
fn stream_body(answer: StreamedAnswer) -> axum::body::Body {
    let (tx, rx) = mpsc::channel::<Result<Vec<u8>, std::io::Error>>(16);
    tokio::spawn(async move {
        let StreamedAnswer { conn, outbound, canceller, execution_id, head, first, heartbeat_secs, mut session } = answer;
        let mut heartbeat = (heartbeat_secs != 0).then(|| {
            let mut iv = tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            iv
        });
        let end = 'feed: {
            let sent = async { tx.send(Ok(chunk_to_bytes(&first))).await.is_ok() };
            match write_or_end(sent, tx.closed(), &mut session).await {
                Ok(true) => {}
                Ok(false) => break 'feed ExchangeEnd::CallerHungUp,
                Err(end) => break 'feed end,
            }
            loop {
                tokio::select! {
                    out = outbound.recv() => match out {
                        // A head after the first item is refused at the
                        // connection; one reaching the wire is a bug, loud
                        // and skipped.
                        Some(Outbound::Head(_)) => tracing::error!(
                            target: "weft_engine::caller_conn",
                            %execution_id, "a response head reached a stream already under way"
                        ),
                        Some(Outbound::Chunk(c)) => {
                            let sent = async { tx.send(Ok(chunk_to_bytes(&c))).await.is_ok() };
                            match write_or_end(sent, tx.closed(), &mut session).await {
                                Ok(true) => {}
                                Ok(false) => break 'feed ExchangeEnd::CallerHungUp,
                                Err(end) => break 'feed end,
                            }
                        }
                        Some(Outbound::Terminate(last, _close)) => {
                            if let Some(c) = last {
                                let sent = async { tx.send(Ok(chunk_to_bytes(&c))).await.is_ok() };
                                // The program answered: a last write the
                                // caller no longer reads still ends the
                                // exchange on the program's side, never as
                                // a hang-up (which would cancel a run that
                                // already answered).
                                if let Err(end) = write_or_end(sent, tx.closed(), &mut session).await {
                                    break 'feed end;
                                }
                            }
                            break 'feed ExchangeEnd::ResponseComplete;
                        }
                        // The status is committed, so the error goes
                        // in-band, then the stream closes.
                        Some(Outbound::Error(message)) => {
                            let sent = async { tx.send(Ok(head.in_band_error(&message))).await.is_ok() };
                            if let Err(end) = write_or_end(sent, tx.closed(), &mut session).await {
                                break 'feed end;
                            }
                            break 'feed ExchangeEnd::ResponseErrored;
                        }
                        None => break 'feed ExchangeEnd::OutboundQueueClosed,
                    },
                    // The caller's side of the body is gone.
                    _ = tx.closed() => break 'feed ExchangeEnd::CallerHungUp,
                    // Quiet body: write the framing's filler, and a failed
                    // write is the caller gone.
                    _ = async { heartbeat.as_mut().expect("armed").tick().await }, if heartbeat.is_some() => {
                        if let Some(filler) = head.keepalive.as_deref() {
                            let sent = async { tx.send(Ok(filler.as_bytes().to_vec())).await.is_ok() };
                            match write_or_end(sent, tx.closed(), &mut session).await {
                                Ok(true) => {}
                                Ok(false) => break 'feed ExchangeEnd::CallerHungUp,
                                Err(end) => break 'feed end,
                            }
                        }
                    }
                    // Session cap: a configured `max_session_secs` ceiling
                    // on the total exchange (0 = no cap, the future never
                    // resolves). The ONLY deadline on a live exchange;
                    // per-message waits are unbounded (a node may
                    // legitimately wait hours).
                    _ = &mut session => break 'feed ExchangeEnd::SessionCapExceeded,
                }
            }
        };
        // The exchange ended: stop producers (a blocked send now errors);
        // a tied run is cancelled only when the CALLER ended it, and a run
        // that answered finishes on its own.
        exchange_ended(&conn, &outbound, canceller.as_ref(), execution_id, end);
    });
    axum::body::Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
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

/// What a socket's task needs to bridge the socket to its connection.
struct SocketPump {
    conn: Arc<LiveCallerConnection>,
    outbound: Arc<OutboundQueue>,
    inbound: InboundLog,
    config: CallerRuntimeConfig,
    heartbeat_secs: u64,
    clock: Arc<dyn weft_platform_traits::Clock>,
    canceller: Arc<dyn ExecutionCanceller>,
    execution_id: ExecutionId,
}

/// WebSocket path: bridge the socket to the connection, on the socket's
/// one task: the read pump (decode caller frames -> broadcast inbound), the
/// write pump (drain outbound -> frames), and the heartbeat (ping on a
/// timer).
async fn pump_ws(mut socket: WebSocket, pump: SocketPump, unopened: Unopened) {
    let SocketPump { conn, outbound, inbound, config, heartbeat_secs, clock, canceller, execution_id } = pump;
    unopened.opened();
    conn.caller_arrived();

    let data_type = config.data_type;
    let max_inbound = config.max_inbound_bytes;
    let mut session = session_deadline(&clock, config.max_session_secs);

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
                            conn.record.inbound(&msg);
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
                            conn.record.inbound(&msg);
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
                    execution_id = %execution_id, "a response head reached a websocket drainer"
                ),
                // A socket reports a caller gone only through a failed
                // write, so each write races the session cap alone. A
                // caller that stops taking bytes still fails it: the
                // socket carries the route's caller-silence bound
                // (`bound_silence`, set before the protocol branch), so
                // the machine aborts a connection whose data sits
                // unacknowledged, or behind a closed window, that long.
                Some(Outbound::Chunk(c)) => {
                    let sent = async { socket.send(chunk_to_ws(&c)).await.is_ok() };
                    match write_or_end(sent, std::future::pending(), &mut session).await {
                        Ok(true) => {}
                        Ok(false) => break ExchangeEnd::CallerHungUpOnSend,
                        Err(end) => break end,
                    }
                }
                Some(Outbound::Terminate(final_chunk, close)) => {
                    let close = close.unwrap_or_default();
                    let sent = async {
                        if let Some(c) = final_chunk {
                            let _ = socket.send(chunk_to_ws(&c)).await;
                        }
                        socket.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                            code: close.code,
                            reason: close.reason.into(),
                        }))).await.is_ok()
                    };
                    // The program ended it: a close the caller no longer
                    // reads is still the program's end, never a hang-up.
                    if let Err(end) = write_or_end(sent, std::future::pending(), &mut session).await {
                        break end;
                    }
                    break ExchangeEnd::SessionClosedByProgram;
                }
                Some(Outbound::Error(msg)) => {
                    let sent = async {
                        socket.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                            code: 1011, // internal error
                            reason: msg.into(),
                        }))).await.is_ok()
                    };
                    // The program ended it: a close the caller no longer
                    // reads is still the program's end, never a hang-up.
                    if let Err(end) = write_or_end(sent, std::future::pending(), &mut session).await {
                        break end;
                    }
                    break ExchangeEnd::SessionErroredByProgram;
                }
                None => break ExchangeEnd::OutboundQueueClosed,
            },
            // Keep-alive ping (worker-side; browsers can't ping us). The arm
            // only exists when a heartbeat is configured (`heartbeat` is
            // `Some`); otherwise it is permanently disabled.
            _ = async { heartbeat.as_mut().unwrap().tick().await }, if heartbeat.is_some() => {
                let sent = async { socket.send(Message::Ping(Vec::new().into())).await.is_ok() };
                match write_or_end(sent, std::future::pending(), &mut session).await {
                    Ok(true) => {}
                    Ok(false) => break ExchangeEnd::CallerMissedHeartbeat,
                    Err(end) => break end,
                }
            }
            // Session cap: the one deadline on a live exchange (per-message
            // waits are unbounded). `0` = no cap (the future never resolves).
            _ = &mut session => break ExchangeEnd::SessionCapExceeded,
        }
    };

    // Wake any node parked in receive() so it unblocks (the log is now
    // closed; a caught-up reader gets a disconnect); a tied run is cancelled
    // only when the CALLER ended the exchange, and a socket the program
    // closed leaves the run to finish.
    inbound.close();
    exchange_ended(&conn, &outbound, canceller.as_ref(), execution_id, reason);
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::caller::CallerHandle;
    use weft_core::signal::DataType;

    /// The head an answer goes out under is the program's own, else the
    /// default for its first item, and an error after it went out is
    /// written in the framing it committed: a JSON line for a JSON-lines
    /// stream, a text line for a text one.
    #[test]
    fn the_committed_head_decides_the_framing() {
        let ndjson = committed_head(
            Some(ResponseHead::new(200).with_header("content-type", "application/x-ndjson").with_keepalive("\n")),
            Some(&OutboundChunk::Text("{\"a\":1}\n".into())),
        );
        assert_eq!(ndjson.in_band_error("boom"), b"\n{\"error\":\"boom\"}\n");
        assert_eq!(ndjson.keepalive.as_deref(), Some("\n"), "the filler comes off the committed head");
        let text = committed_head(None, Some(&OutboundChunk::Text("partial".into())));
        assert_eq!(text.in_band_error("boom"), b"\n[error] boom");
        assert_eq!(committed_head(None, None).status, 204, "no body and no head is a 204");
    }

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
        fn connected(&self, _c: ExecutionId, off: u64, _p: Protocol) {
            self.events.lock().unwrap().push(format!("connected@{off}"));
        }
        fn inbound(&self, _c: ExecutionId, off: u64, _m: &InboundMessage) {
            self.events.lock().unwrap().push(format!("inbound@{off}"));
        }
        fn outbound(&self, _c: ExecutionId, off: u64, _ch: &OutboundChunk, terminal: bool) {
            self.events.lock().unwrap().push(format!("outbound@{off}:term={terminal}"));
        }
        fn errored(&self, _c: ExecutionId, off: u64, _m: &str) {
            self.events.lock().unwrap().push(format!("errored@{off}"));
        }
        fn disconnected(&self, _c: ExecutionId, off: u64, _r: &str) {
            self.events.lock().unwrap().push(format!("disconnected@{off}"));
        }

        fn close(&self) {
            // Every row above is written as it is handed over.
        }
    }

    fn ws_cfg() -> CallerRuntimeConfig {
        CallerRuntimeConfig {
            protocol: Protocol::Websocket,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: weft_core::signal::ErrorMode::Surface,
            max_inbound_bytes: 1024,
            caller_silence_secs: weft_core::signal::DEFAULT_CALLER_SILENCE_SECS,
            max_session_secs: 0,
            outlives_caller: false,
            inbound_window: 4,
            journal: weft_core::stream_journal::JournalPolicy::default(),
        }
    }

    /// A socket's leaving is recorded once, however many sides say so.
    #[tokio::test]
    async fn a_sockets_disconnect_is_recorded_once() {
        let journal = Arc::new(RecordingSink::default());
        let (conn, _, _) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, journal.clone());
        conn.caller_arrived();
        conn.terminate(None, None, None).await.unwrap();
        conn.mark_disconnected("socket closed");
        conn.mark_disconnected("socket closed again");
        assert_eq!(journal.events.lock().unwrap().iter().filter(|event| event.starts_with("disconnected@")).count(), 1);
    }

    #[tokio::test]
    async fn outbound_chunks_reach_the_socket_channel_and_journal() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, out_rx, _inb) =
            new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink.clone());
        conn.caller_arrived();
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
            new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, out_rx, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
        let (conn, _out, _inb) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
        conn.terminate(None, None, None).await.expect("first terminal");
        let err = conn.terminate(None, None, None).await.expect_err("second rejected");
        assert!(matches!(err, CallerError::AlreadyTerminated));
    }

    /// `receive()` is UNBOUNDED: no deadline ends it. A node parked on
    /// the next message waits indefinitely; the only ways out are a message
    /// arriving or the connection closing, which yields `Disconnected`.
    /// A late message is still delivered: the wait is unbounded, not just
    /// "returns Disconnected eventually".
    #[tokio::test]
    async fn receive_delivers_a_late_message() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
        let inbound = inbound.expect("ws has inbound");
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(conn.clone()) else {
            unreachable!()
        };
        let cursor = ws.cursor();
        let recv = tokio::spawn(async move { cursor.receive().await });
        // Arrive "late": a short sleep orders the push after the receive
        // has parked.
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
        let mut never = session_deadline(&clock, 0);
        assert!(futures::FutureExt::now_or_never(&mut never).is_none(), "cap=0 must never resolve (no session cap)");
        // cap>0 resolves (FakeClock::sleep advances itself and returns).
        session_deadline(&clock, 30).await;
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

    #[derive(Default)]
    struct RecordingCanceller {
        cancelled: Mutex<Vec<ExecutionId>>,
    }
    impl ExecutionCanceller for RecordingCanceller {
        fn cancel(&self, execution_id: ExecutionId, _cause: weft_core::exec::CancelCause) {
            self.cancelled.lock().unwrap().push(execution_id);
        }
    }

    fn http_cfg() -> CallerRuntimeConfig {
        CallerRuntimeConfig { protocol: Protocol::Http, ..ws_cfg() }
    }

    /// Every run the server was asked to bear: none is born here (it
    /// records and refuses).
    #[derive(Default)]
    struct RecordingStarter {
        started: Mutex<Vec<String>>,
    }
    #[async_trait]
    impl LiveStarter for RecordingStarter {
        async fn bear(&self, admitted: Box<crate::door::Admitted>) -> Result<Born, Response> {
            self.started.lock().unwrap().push(admitted.trigger.token.clone());
            Err((StatusCode::SERVICE_UNAVAILABLE, "recorded").into_response())
        }
    }

    fn server_state() -> (ConnServerState, Arc<RecordingCanceller>) {
        let canceller = Arc::new(RecordingCanceller::default());
        let broker = crate::door::fake::FakeDoorBroker::new(vec![crate::door::fake::route("t1", "route", "chat/{room}")]);
        let state = ConnServerState {
            door: crate::door::fake::door(broker),
            weft_hop: crate::worker::WeftCredential::of(&weft_core::caller_token::ProjectSecret::of(b"install", uuid::Uuid::from_u128(7))),
            starter: Arc::new(RecordingStarter::default()),
            clock: weft_platform_traits::FakeClock::new(),
            canceller: canceller.clone(),
        };
        (state, canceller)
    }

    /// Weft's relay is believed about the caller only on a hop carrying
    /// weft's credential, and every trace of the hop is gone from what the
    /// door and the run read: the caller's own `Host` is back.
    #[test]
    fn only_weft_relay_names_the_caller_and_the_hop_leaves_no_trace() {
        use weft_core::net::relay_hop;
        let secret = weft_core::caller_token::ProjectSecret::of(b"install", uuid::Uuid::from_u128(7));
        let weft = crate::worker::WeftCredential::of(&secret);
        let key = secret.worker_door_key();
        let hop = |credential: &str| {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(weft_platform_traits::WORKER_AUTH_HEADER, format!("Bearer {credential}").parse().unwrap());
            headers.insert(relay_hop::CALLER_ADDRESS, "203.0.113.9".parse().unwrap());
            headers.insert(relay_hop::CALLER_HOST, "api.example.com".parse().unwrap());
            headers.insert(relay_hop::ROUTE_PREFIX, "/connect/acme".parse().unwrap());
            headers.insert("host", "127.0.0.1:49153".parse().unwrap());
            headers.insert("authorization", "Bearer the-callers-own".parse().unwrap());
            headers
        };
        let mut relayed = hop(&key);
        assert_eq!(
            relayed_by_weft(&weft, &mut relayed).unwrap(),
            Some(Relayed { caller: "203.0.113.9".parse().unwrap(), route_prefix: "/connect/acme".into() })
        );
        let left: Vec<&str> = relayed.keys().map(|k| k.as_str()).collect();
        assert_eq!(left, vec!["host", "authorization"], "{left:?}");
        assert_eq!(relayed["host"], "api.example.com");
        let mut forged = hop("00");
        assert_eq!(relayed_by_weft(&weft, &mut forged).unwrap(), None);
        assert_eq!(forged.keys().map(|k| k.as_str()).collect::<Vec<_>>(), vec!["host", "authorization"]);
        assert_eq!(forged["host"], "127.0.0.1:49153", "a caller's word on its host is not taken either");
        let mut unnamed = hop(&key);
        unnamed.remove(relay_hop::CALLER_ADDRESS);
        assert!(relayed_by_weft(&weft, &mut unnamed).is_err(), "weft's relay always names the caller");
    }

    /// A caller is checked at the door before anything starts, and a call
    /// the door lets in is handed to the starter.
    #[tokio::test]
    async fn a_caller_goes_through_the_door_before_a_run_starts() {
        use tower::ServiceExt as _;
        let (mut state, _) = server_state();
        let starter = Arc::new(RecordingStarter::default());
        state.starter = starter.clone();
        let call = |uri: &str| {
            let mut request = axum::http::Request::builder().method("POST").uri(uri).body(axum::body::Body::from("{}")).unwrap();
            request.extensions_mut().insert(axum::extract::ConnectInfo(CallerSocket { socket: None, peer: "10.0.0.1".parse().unwrap() }));
            request
        };
        let refused = connection_router(state.clone()).oneshot(call("/nowhere")).await.unwrap();
        assert_eq!(refused.status(), StatusCode::NOT_FOUND);
        assert!(starter.started.lock().unwrap().is_empty(), "a refused caller starts nothing");
        let _ = connection_router(state).oneshot(call("/chat/room7")).await.unwrap();
        assert_eq!(*starter.started.lock().unwrap(), vec!["t1".to_string()]);
    }

    /// What an HTTP call brings is read before its run is born: a body
    /// the run could not read is the caller's answer, and nothing starts.
    #[tokio::test]
    async fn a_body_the_run_cannot_read_starts_nothing() {
        use tower::ServiceExt as _;
        let (mut state, canceller) = server_state();
        let starter = Arc::new(RecordingStarter::default());
        state.starter = starter.clone();
        let mut request = axum::http::Request::builder().method("POST").uri("/chat/room7").body(axum::body::Body::from("not json")).unwrap();
        request.extensions_mut().insert(axum::extract::ConnectInfo(CallerSocket { socket: None, peer: "10.0.0.1".parse().unwrap() }));
        let refused = connection_router(state).oneshot(request).await.unwrap();
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        assert!(starter.started.lock().unwrap().is_empty(), "no run is born for a body it could not read");
        assert!(canceller.cancelled.lock().unwrap().is_empty(), "and none is left to cancel");
        let cfg = CallerRuntimeConfig { max_inbound_bytes: 4, ..http_cfg() };
        let over = read_body(&cfg, axum::body::Body::from("far more than four bytes")).await.expect_err("over the cap");
        assert_eq!(over.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    /// One HTTP call's run as the call's task holds it: `program` against
    /// the connection, with the server state's clock and the canceller.
    fn run_here<F, Fut>(
        cfg: CallerRuntimeConfig,
        request: LiveRequest,
        body: &str,
        program: F,
    ) -> (RunHere, Arc<dyn weft_platform_traits::Clock>, Arc<RecordingCanceller>)
    where
        F: FnOnce(Arc<LiveCallerConnection>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (state, canceller) = server_state();
        let execution_id = ExecutionId::new_v4();
        let body = decode_inbound(cfg.data_type, body.as_bytes()).expect("the test's body decodes");
        let (conn, outbound, _) = new_connection(cfg, execution_id, Arc::new(request), Some(body), Arc::new(RecordingSink::default()));
        let run: futures::future::BoxFuture<'static, ()> = Box::pin(program(conn.clone()));
        let here = RunHere { run: Some(run), conn, outbound, canceller: state.canceller.clone(), execution_id };
        (here, state.clock, canceller)
    }

    /// Drive one HTTP exchange the way the call's task does: [`run_here`]
    /// polled by [`answer_http`] until it answers. Hands back the
    /// response, the run's id and the canceller, so a test can read (or
    /// drop) the body itself and assert whether the exchange's end
    /// cancelled the run.
    async fn open_exchange<F, Fut>(
        cfg: CallerRuntimeConfig,
        request: LiveRequest,
        body: &str,
        heartbeat_secs: u64,
        program: F,
    ) -> (Response, ExecutionId, Arc<RecordingCanceller>)
    where
        F: FnOnce(Arc<LiveCallerConnection>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (here, clock, canceller) = run_here(cfg, request, body, program);
        let execution_id = here.execution_id;
        (answer_http(here, heartbeat_secs, clock).await, execution_id, canceller)
    }

    /// [`open_exchange`] for a `POST chat/room7` carrying `body`, read
    /// whole: the response's status, headers and body, and the canceller.
    async fn exchange_recording<F, Fut>(
        body: &str,
        program: F,
    ) -> ((StatusCode, Vec<(String, String)>, String), Arc<RecordingCanceller>)
    where
        F: FnOnce(Arc<LiveCallerConnection>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let request = LiveRequest { method: "POST".into(), path: "chat/room7".into(), ..Default::default() };
        let (response, _, canceller) = open_exchange(http_cfg(), request, body, 0, program).await;
        let status = response.status();
        let headers = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        ((status, headers, String::from_utf8(bytes.to_vec()).unwrap()), canceller)
    }

    /// [`exchange_recording`] without the canceller.
    async fn exchange<F, Fut>(body: &str, program: F) -> (StatusCode, Vec<(String, String)>, String)
    where
        F: FnOnce(Arc<LiveCallerConnection>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        exchange_recording(body, program).await.0
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

    /// A streaming exchange: `GET feed`, its response handed back as soon
    /// as the program commits the head. `heartbeat_secs` as the worker
    /// would pass it.
    async fn open_feed<F, Fut>(heartbeat_secs: u64, program: F) -> (Response, ExecutionId, Arc<RecordingCanceller>)
    where
        F: FnOnce(Arc<LiveCallerConnection>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let request = LiveRequest { method: "GET".into(), path: "feed".into(), ..Default::default() };
        open_exchange(http_cfg(), request, "", heartbeat_secs, program).await
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
        let (response, _, _) = open_feed(2, quiet_feed).await;
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
        let (response, execution_id, canceller) = open_feed(0, quiet_feed).await;
        let mut body = response.into_body().into_data_stream();
        let first = body.next().await.unwrap().unwrap();
        assert_eq!(&first[..], b"data: first\n\n");
        drop(body);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while canceller.cancelled.lock().unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline, "the run was never cancelled after the caller left");
            tokio::task::yield_now().await;
        }
        assert_eq!(canceller.cancelled.lock().unwrap().as_slice(), &[execution_id]);
    }

    /// A feed that has not said ANYTHING yet: the bus it watches was
    /// quiet from the moment the route opened, so no head has gone out
    /// and there is no body to write filler into. The caller is still
    /// owed a response, so the call's task is parked on the head. When
    /// that caller goes, hyper drops the task with the run it was
    /// polling, and the run has to end there, because the heartbeat has
    /// no way to find out on its own.
    #[tokio::test]
    async fn a_caller_leaving_before_the_first_byte_still_ends_the_run() {
        let request = LiveRequest { method: "GET".into(), path: "feed".into(), ..Default::default() };
        let (here, clock, canceller) = run_here(http_cfg(), request, "", |conn| async move {
            let CallerHandle::Http(_http) = CallerHandle::from_connection(conn) else { unreachable!() };
            // Never writes: the watched table never changed.
            std::future::pending::<()>().await;
        });
        let execution_id = here.execution_id;
        // Dropping the task is what hyper does to it when the connection
        // goes.
        let handler = tokio::spawn(answer_http(here, 1, clock));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!handler.is_finished(), "with nothing written the call's task is still holding the head");
        handler.abort();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while canceller.cancelled.lock().unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline, "a caller who left before the first byte never ended the run");
            tokio::task::yield_now().await;
        }
        assert_eq!(canceller.cancelled.lock().unwrap().as_slice(), &[execution_id]);
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
        assert_eq!(body, weft_core::caller::NO_ANSWER);
    }

    /// A run that parked has not ended: its caller hears that it
    /// carries on without them, never that it ended unanswered.
    #[tokio::test]
    async fn a_run_that_parks_says_so_and_never_claims_it_ended() {
        let (status, _headers, body) = exchange("{}", |conn| async move {
            conn.run_carries_on(PARKED).await;
        })
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body, PARKED);
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

    /// A program that never answers holds its caller only up to the
    /// session cap: the cap answers a loud `500` saying so, and the run,
    /// tied to its caller, is cancelled.
    #[tokio::test]
    async fn a_silent_end_answers_500_with_the_reason() {
        let cfg = CallerRuntimeConfig { max_session_secs: 1, ..http_cfg() };
        // Nobody talks; the session cap (1s on the fake clock, which
        // returns at once) ends the exchange with the head held.
        let (response, execution_id, canceller) =
            open_exchange(cfg, LiveRequest::default(), "{}", 0, |_conn| std::future::pending::<()>()).await;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("session cap exceeded"), "got: {body}");
        assert_eq!(canceller.cancelled.lock().unwrap().as_slice(), &[execution_id]);
    }

    #[tokio::test]
    async fn a_head_after_the_first_item_is_refused_at_the_connection() {
        let sink = Arc::new(RecordingSink::default());
        let (conn, _out, _inb) = new_connection(
            http_cfg(),
            ExecutionId::nil(),
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
            new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
            new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
            let (conn, _out, inbound) = new_connection(ws_cfg(), ExecutionId::nil(), Arc::new(LiveRequest::default()), None, sink);
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
