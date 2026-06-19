//! Live caller connection: the node-facing surface for a held connection
//! to an outside caller (see `crate::signal::live_connection`).
//!
//! Two execution worlds exist. The DURABLE world (`ctx.await_signal`) is
//! a disconnected wait: the worker parks, dies, journals, and a fresh
//! worker resumes later. The LIVE world (this module) is a caller
//! attached over a held connection for the life of the request/session;
//! the worker stays alive because it is awaiting on the open socket. It
//! is NOT durable: the connection is pinned to the one worker that
//! received it and dies with that process.
//!
//! ## Layering
//!
//! - [`CallerConnection`] is the I/O boundary (a trait). The production
//!   impl (engine crate) talks over the worker<->gateway socket; the
//!   fake (test-helpers) records calls and scripts inbound messages. The
//!   engine wires one onto the `ContextHandle` for runs that have a live
//!   connection; runs without one expose `None`.
//! - [`CallerHandle`] is the ergonomic author-facing wrapper
//!   `ExecutionContext::caller()` returns. Talk methods are
//!   protocol-specific (an HTTP handle has no `receive`), so the wrapper
//!   is an enum over the two protocol shapes: the interface is honest
//!   about what each protocol can do.
//! - The pure decision functions ([`resolve_disconnect`], the
//!   terminate-once state, the cap checks) carry no I/O and are unit
//!   tested directly.
//!
//! Everything is parameterized by the [`crate::signal::LiveConnectionConfig`]
//! (protocol, disconnect policy, caps, data type, ...); this
//! module re-expresses the relevant subset as a runtime
//! [`CallerRuntimeConfig`] the connection layer reads.

use std::sync::Arc;

// `Ordering` is only used by the hand-rolled fake (test-helpers); gate the
// import so a default build (fake compiled out) has no unused import.
#[cfg(any(test, feature = "test-helpers"))]
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::WeftResult;
use crate::signal::{Backpressure, DataType, ErrorMode, LiveConnectionConfig, Protocol};
use crate::wait::SuspendPolicy;

/// Runtime subset of the live-connection config the connection layer
/// reads. Derived once from the trigger's live-caller config when the
/// caller attaches, so every guardrail (caps, backpressure, data shape,
/// suspension defaults) is read from one place rather than re-parsed per
/// call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerRuntimeConfig {
    pub protocol: Protocol,
    pub data_type: DataType,
    pub backpressure: Backpressure,
    pub error_mode: ErrorMode,
    /// Worker-clock bound for the connect barrier and any bounded
    /// caller-reply wait. Always > 0 (validated on the signal).
    pub connect_timeout_secs: u64,
    /// Reject an inbound body / message larger than this.
    pub max_inbound_bytes: u64,
    /// Max total session duration. `0` = no cap.
    pub max_session_secs: u64,
    /// The run's suspension defaults (the single `can_suspend` axis +
    /// the default hold time). Seeds the wait-policy resolution chain AND
    /// decides what a disconnect means (see [`resolve_disconnect`]): a
    /// non-suspendable run that loses its caller is killed; a suspendable
    /// run continues with sends going into the void.
    pub suspend: SuspendPolicy,
    /// In-RAM inbound window size (WebSocket): how many recent messages the
    /// connection retains for cursors. Bounds RAM on a long-lived socket;
    /// cursors only read this window (never the DB). Always >= 1.
    pub inbound_window: usize,
}

impl CallerRuntimeConfig {
    /// Project a live-caller trigger's config onto the runtime subset the
    /// connection layer needs. `protocol` comes from which kind fired
    /// (`ApiEndpoint` -> `Http`, `LiveSocket` -> `Websocket`), not a config
    /// field, so it is passed alongside the shared body.
    pub fn from_config(cfg: &LiveConnectionConfig, protocol: Protocol) -> Self {
        Self {
            protocol,
            data_type: cfg.data_type,
            backpressure: cfg.backpressure,
            error_mode: cfg.error_mode,
            connect_timeout_secs: cfg.connect_timeout_secs,
            max_inbound_bytes: cfg.max_inbound_bytes,
            max_session_secs: cfg.max_session_secs,
            suspend: cfg.suspend,
            inbound_window: cfg.window.unwrap_or(DEFAULT_INBOUND_WINDOW),
        }
    }
}

/// Default in-RAM inbound window for a WebSocket caller. Matches the bus
/// default; bounds RAM without truncating short conversations.
pub const DEFAULT_INBOUND_WINDOW: usize = 64;

/// What happens when the caller is gone (disconnected, or the response
/// completed: the same event from the run's view). Derived purely from
/// the run's `can_suspend` axis, NOT a separate setting (collapsing the
/// two removed the contradictory combinations): a run that cannot be
/// suspended is tied to its caller, so losing the caller kills it; a run
/// that may be suspended outlives the caller, so it keeps running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectAction {
    /// Cancel THIS execution (via the per-execution cancel-by-color
    /// path). Never the pod, which multiplexes many runs.
    CancelExecution,
    /// Keep running to completion; further sends to the caller go into
    /// the void.
    ContinueIntoVoid,
}

/// Resolve the disconnect action from the run's suspendability. Pure; the
/// one place the mapping lives. `can_suspend == false` (caller-tied) ->
/// cancel; `true` (survives) -> continue into the void.
pub fn resolve_disconnect(suspend: SuspendPolicy) -> DisconnectAction {
    if suspend.can_suspend {
        DisconnectAction::ContinueIntoVoid
    } else {
        DisconnectAction::CancelExecution
    }
}

/// Why a talk/await operation on the caller connection did not complete.
/// Returned (never swallowed) so a node handles a dropped exchange as a
/// value, mirroring `bus::SendError`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallerError {
    /// The caller is no longer attached and this run is being cancelled as a
    /// result (the trigger's disconnect policy resolved to cancel). The
    /// keep-running policy never surfaces as an error: under it a gone-caller
    /// talk is a silent no-op (`Ok(())`) into the void, so `Disconnected` is
    /// ALWAYS the cancel case (no `action` field to disambiguate).
    #[error("caller disconnected; run cancelled")]
    Disconnected,
    /// The terminal act (HTTP respond/close, WS close) already happened.
    /// First terminal wins; later terminal attempts fail loud rather than
    /// silently double-finishing.
    #[error("response/session already completed")]
    AlreadyTerminated,
    /// The CONNECT barrier (`ensure_connected` / waiting for the caller to
    /// attach) elapsed without the caller arriving. Worker-clock driven so a
    /// never-arriving caller cannot pin a worker forever. This is the ONLY
    /// place a `Timeout` arises: an inbound read (`receive` / `request`) is
    /// unbounded and never yields `Timeout` (a node may wait hours for the
    /// next message); a read ends via a message, `Disconnected`, or the
    /// session cap.
    #[error("caller did not connect within {waited_secs}s")]
    Timeout { waited_secs: u64 },
    /// An inbound body / message exceeded `max_inbound_bytes`. Rejected
    /// loud (untrusted-caller abuse vector).
    #[error("inbound payload {got_bytes} bytes exceeds cap {cap_bytes}")]
    InboundTooLarge { got_bytes: u64, cap_bytes: u64 },
    /// The total session exceeded `max_session_secs`.
    #[error("session exceeded max duration {cap_secs}s")]
    SessionExpired { cap_secs: u64 },
    /// The cursor's offset was trimmed out of the in-RAM window (cursors
    /// only read RAM, never the DB). The cursor is MOVED to `oldest_resident`
    /// (the earliest message still retained), so the next `receive()` resumes
    /// there. ONE offset, because the caller's inbound log is dense (no
    /// membership entries to bridge), so "where I resume" and "the window
    /// floor" are always the same point. (The bus's `FellBehind` carries two
    /// offsets because its log is sparse and can resume past the floor; the
    /// caller never can.) Same fell-behind concept, minimal shape.
    #[error("cursor fell behind; resuming at oldest resident offset {oldest_resident}")]
    FellBehind { oldest_resident: u64 },
    /// The wrong-protocol talk method was called (e.g. `receive` on an
    /// HTTP connection). An honest-interface guard; the typed
    /// `CallerHandle` makes this unreachable from author code, but the
    /// low-level trait surfaces it for completeness.
    #[error("operation not valid for {protocol} connections")]
    WrongProtocol { protocol: &'static str },
    /// The connection layer hit a transport error (socket write failed,
    /// frame encode failed). Fails loud; never silently dropped.
    #[error("caller transport error: {0}")]
    Transport(String),
}

impl CallerError {
    /// Does this outcome mean "the inbound stream has ended, stop reading"
    /// (as opposed to a failure to propagate)? True for the genuinely terminal
    /// outcomes a read loop should break on cleanly: the caller disconnected,
    /// the connect barrier timed out, or the session cap fired. The stream is
    /// over and nothing more will arrive.
    ///
    /// `FellBehind` is deliberately NOT here: it is RESUMABLE, not terminal.
    /// The stream continues; this reader merely lost the trimmed-out messages
    /// and its cursor was moved to the retained floor. Collapsing it to "end
    /// of stream" would silently drop data, which is forbidden. So
    /// [`WsCaller::recv_next`] surfaces `FellBehind` as an `Err` the node must
    /// handle (resume via `receive()` at the floor, or stop on purpose), and
    /// the built-in forward cursor never hits it anyway (it stays ahead of the
    /// window). `WrongProtocol` / `Transport` / size-cap / already-terminated
    /// are likewise real errors, never end-of-stream.
    ///
    /// This is the language deciding which `CallerError`s are "end of stream"
    /// so a node author never hand-rolls the classification; pair with
    /// [`WsCaller::recv_next`] / [`CallerCursor::recv_next`] which return
    /// `Ok(None)` exactly on these.
    pub fn ends_stream(&self) -> bool {
        matches!(
            self,
            CallerError::Disconnected
                | CallerError::Timeout { .. }
                | CallerError::SessionExpired { .. }
        )
    }
}

/// Pure cap check for an inbound payload. `Ok(())` if within the cap,
/// `Err(InboundTooLarge)` otherwise. Extracted so the byte-size gate is
/// unit tested without a socket.
pub fn check_inbound_size(got_bytes: u64, cap_bytes: u64) -> Result<(), CallerError> {
    if got_bytes > cap_bytes {
        Err(CallerError::InboundTooLarge { got_bytes, cap_bytes })
    } else {
        Ok(())
    }
}

/// Pure session-duration cap check. `cap_secs == 0` disables the cap.
/// `Ok(())` if within budget, `Err(SessionExpired)` otherwise.
pub fn check_session_duration(elapsed_secs: u64, cap_secs: u64) -> Result<(), CallerError> {
    if cap_secs != 0 && elapsed_secs > cap_secs {
        Err(CallerError::SessionExpired { cap_secs })
    } else {
        Ok(())
    }
}

/// Whether a node's terminal act may proceed, given whether the
/// connection has already been terminated. Pure transition: `false ->
/// true` is the only legal terminal, every later one is rejected. The
/// connection layer holds the bool behind a lock and calls this to
/// decide; modeling it as a pure transition keeps the once-only rule
/// testable and identical for both protocols.
pub fn try_terminate(already_terminated: bool) -> Result<(), CallerError> {
    if already_terminated {
        Err(CallerError::AlreadyTerminated)
    } else {
        Ok(())
    }
}

/// One outbound message the node hands the connection layer. The layer
/// encodes it to the wire per the declared `DataType`. A general
/// send-value the connection adapts, rather than a per-protocol shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundChunk {
    /// A JSON value (declared `data_type = json`).
    Json(Value),
    /// UTF-8 text (declared `data_type = text`).
    Text(String),
    /// Raw bytes (declared `data_type = bytes`).
    Bytes(Vec<u8>),
}

/// One inbound message the connection layer decodes from the wire and
/// hands the node. Symmetric with [`OutboundChunk`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboundMessage {
    Json(Value),
    Text(String),
    Bytes(Vec<u8>),
}

/// The I/O boundary for a held caller connection. ONE trait for both
/// protocols (the protocol-specific surface is enforced by the typed
/// [`CallerHandle`] above this); methods invalid for the active protocol
/// return `CallerError::WrongProtocol`. Implementations:
///   - production (engine crate): drives the worker<->gateway socket.
///   - fake (test-helpers): records calls, scripts inbound messages.
///
/// All methods take `&self`: a connection is shared (`Arc`) across the
/// concurrently-running nodes of one execution, which talk to the caller
/// in parallel. Interior mutability lives in the impl.
#[async_trait]
pub trait CallerConnection: Send + Sync {
    /// The resolved runtime config (protocol, policies, caps).
    fn config(&self) -> &CallerRuntimeConfig;

    /// Is the caller attached right now? Never fails; pure status read.
    fn is_connected(&self) -> bool;

    /// Wait until the caller is actually attached. The worker is woken
    /// before the caller is pointed at it (true for BOTH protocols), so
    /// this may wait; if already connected it returns immediately. Bounded
    /// by `connect_timeout_secs`; on expiry returns `Timeout`. If the
    /// caller was attached then dropped, returns `Disconnected` carrying
    /// the resolved policy action.
    async fn ensure_connected(&self) -> Result<(), CallerError>;

    /// Append a non-terminal outbound chunk (HTTP `write`, WS `send`).
    /// Free-for-all: concurrent chunks from multiple nodes interleave on
    /// the wire. Honors the backpressure policy. Errors loud on
    /// transport failure or if the caller is gone under a `cancel` policy
    /// (`Disconnected`); under `keep-running` a gone caller is a silent
    /// no-op (`Ok(())`) into the void.
    async fn send_chunk(&self, chunk: OutboundChunk) -> Result<(), CallerError>;

    /// Terminate the exchange: HTTP one-shot `respond(body)` (a final
    /// body with no prior streaming) OR `close()` after streaming; WS
    /// `close()`. First terminal wins; a later one returns
    /// `AlreadyTerminated`. `final_chunk` carries the one-shot HTTP body
    /// (`Some`) or is `None` for a bare close.
    async fn terminate(&self, final_chunk: Option<OutboundChunk>) -> Result<(), CallerError>;

    /// WebSocket only: await the next inbound message at `cursor` (the
    /// reader's next absolute offset to read over the windowed inbound log),
    /// advancing it on success. Inbound is BROADCAST: every reader has its
    /// OWN cursor over the SAME log, so two nodes each see every message
    /// (neither steals it). A cursor that fell behind the in-RAM window
    /// surfaces `FellBehind` and moves to the retained floor (cursors only
    /// read RAM, never the DB; the window is the readable world). The wait is
    /// UNBOUNDED: a node may legitimately park here for minutes or hours
    /// waiting for the caller's next message, and Weft never times out a
    /// user-controlled wait. It ends only on a message, a disconnect
    /// (`Disconnected`), or the session cap firing (which disconnects).
    /// `Err(WrongProtocol)` on HTTP.
    async fn receive(
        &self,
        cursor: &std::sync::atomic::AtomicU64,
    ) -> Result<InboundMessage, CallerError>;

    /// WebSocket only: send a message, then read the next inbound at
    /// `cursor`. Because inbound is broadcast (not point-to-point), there
    /// is no strict send/reply correlation: "the reply" is simply the next
    /// message this reader observes after the send. A node needing strict
    /// correlation sends, then filters `receive` for the matching message
    /// itself. The read is UNBOUNDED, like `receive`. `Err(WrongProtocol)`
    /// on HTTP.
    async fn request(
        &self,
        msg: OutboundChunk,
        cursor: &std::sync::atomic::AtomicU64,
    ) -> Result<InboundMessage, CallerError>;

    /// HTTP only: the inbound request parts (method, path, query,
    /// headers, body) decoded per the declared data type. WebSocket
    /// inbound flows through `receive`/`request` instead.
    /// `Err(WrongProtocol)` on WebSocket.
    fn http_request(&self) -> Result<Arc<HttpRequestParts>, CallerError>;

    /// WebSocket only: the current "now" offset of the inbound stream (one
    /// past the highest message received so far). A cursor minted via
    /// `WsCaller::cursor()` starts here. The same `now_offset` concept the
    /// bus exposes; cursors only ever read RAM, never the DB. `0` for HTTP.
    fn inbound_now_offset(&self) -> u64;

    /// WebSocket only: the inbound offset captured when the caller ATTACHED
    /// to this worker (the connection-open point). The handle's BUILT-IN
    /// forward cursor starts here, NOT at the later `ctx.caller()`-call
    /// time, so a node never misses a message that arrived between the
    /// connection opening and the node reading (the subscribe race). Since
    /// the inbound log is empty at attach this is normally 0; it only
    /// differs once the window has trimmed past it (then a built-in cursor
    /// clamps up to the retained floor). `0` for HTTP.
    fn inbound_attach_offset(&self) -> u64;

    /// WebSocket only: the earliest inbound offset still resident in the
    /// in-RAM window. A cursor cannot read below this (older messages were
    /// trimmed; cursors never read the DB). `0` for HTTP.
    fn inbound_retained_floor(&self) -> u64;

    /// WebSocket only: the offset of the most recent inbound message still
    /// in RAM, if any. Used to seed a "forward + last message" cursor.
    /// `None` for HTTP or an empty stream.
    fn last_inbound_offset(&self) -> Option<u64>;
}

/// The inbound HTTP request a node reads on an `http` live connection.
/// Body is decoded to an [`InboundMessage`] per the declared data type;
/// the size cap was already enforced at decode time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRequestParts {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: InboundMessage,
}

/// Author-facing ergonomic wrapper. An enum over the two protocol
/// shapes so the talk surface is honest: the `Http` variant exposes
/// respond/write/close and the request parts; the `Websocket` variant
/// exposes send/receive/request/close. Both share the queries
/// (`is_connected`) and the one barrier (`ensure_connected`).
#[derive(Clone)]
pub enum CallerHandle {
    Http(HttpCaller),
    Websocket(WsCaller),
}

impl CallerHandle {
    /// Build the protocol-correct wrapper from a connection. Internal:
    /// `ExecutionContext::caller()` calls this.
    pub fn from_connection(conn: Arc<dyn CallerConnection>) -> Self {
        match conn.config().protocol {
            Protocol::Http => CallerHandle::Http(HttpCaller { conn }),
            // FORWARD-ONLY default, pinned at attach: the handle's built-in
            // cursor starts at the inbound stream's current "now", so a node
            // sees messages that arrive AFTER it got the handle, not the
            // whole prior history. Pinning at handle-construction (not at
            // first `receive`) closes the subscribe race: anything that
            // lands between attach and the first read is at/after this
            // offset and still seen. Same model as the bus's `cursor()`.
            // To read history, mint a positioned cursor (`cursor_from_start`
            // / `cursor_at` / `cursor_including_last`).
            Protocol::Websocket => {
                // Pin at ATTACH (connection-open), not at this call: a
                // message arriving between the connection opening and the
                // node's first read is at/after this offset and still seen.
                let start = conn.inbound_attach_offset();
                CallerHandle::Websocket(WsCaller {
                    conn,
                    cursor: Arc::new(std::sync::atomic::AtomicU64::new(start)),
                })
            }
        }
    }

    /// Is the caller attached right now? Shared by both protocols.
    pub fn is_connected(&self) -> bool {
        self.conn().is_connected()
    }

    /// Wait until the caller is attached (or fail loud on timeout /
    /// disconnect). The single barrier; identical meaning for both
    /// protocols.
    pub async fn ensure_connected(&self) -> WeftResult<()> {
        self.conn().ensure_connected().await.map_err(Into::into)
    }

    fn conn(&self) -> &Arc<dyn CallerConnection> {
        match self {
            CallerHandle::Http(h) => &h.conn,
            CallerHandle::Websocket(h) => &h.conn,
        }
    }
}

/// HTTP talk surface. No `receive`: an HTTP connection has no
/// independent inbound stream (the request body is read once via
/// [`Self::request_parts`]).
#[derive(Clone)]
pub struct HttpCaller {
    conn: Arc<dyn CallerConnection>,
}

impl HttpCaller {
    /// Wait until the caller is attached (no-op if already connected;
    /// fails loud on the connect timeout / disconnect policy). The single
    /// barrier, also reachable on the protocol-agnostic `CallerHandle`.
    pub async fn ensure_connected(&self) -> WeftResult<()> {
        self.conn.ensure_connected().await.map_err(Into::into)
    }

    /// Is the caller attached right now? Pure status read.
    pub fn is_connected(&self) -> bool {
        self.conn.is_connected()
    }

    /// The inbound request (method, path, query, headers, body).
    pub fn request_parts(&self) -> WeftResult<Arc<HttpRequestParts>> {
        self.conn.http_request().map_err(Into::into)
    }

    /// Stream a non-terminal chunk to the caller. Multiple nodes may
    /// stream concurrently; chunks interleave.
    pub async fn write(&self, chunk: OutboundChunk) -> WeftResult<()> {
        self.conn.send_chunk(chunk).await.map_err(Into::into)
    }

    /// One-shot response: a final body with no prior streaming.
    /// Terminal; first terminal wins.
    pub async fn respond(&self, body: OutboundChunk) -> WeftResult<()> {
        self.conn.terminate(Some(body)).await.map_err(Into::into)
    }

    /// Close the response after streaming. Terminal; first terminal wins.
    pub async fn close(&self) -> WeftResult<()> {
        self.conn.terminate(None).await.map_err(Into::into)
    }
}

/// WebSocket talk surface: full duplex. Holds a built-in FORWARD cursor
/// pinned at attach (its `receive`/`request` see messages arriving after
/// the handle was obtained, never prior history), matching the bus's
/// default. To read history, mint a positioned [`CallerCursor`] via
/// `cursor_from_start` / `cursor_at` / `cursor_including_last`. Each cursor
/// is an independent broadcast reader (no inter-node stealing).
#[derive(Clone)]
pub struct WsCaller {
    conn: Arc<dyn CallerConnection>,
    cursor: Arc<std::sync::atomic::AtomicU64>,
}

impl WsCaller {
    /// Wait until the caller is attached (no-op if already connected;
    /// fails loud on the connect timeout / disconnect policy). The single
    /// barrier, also reachable on the protocol-agnostic `CallerHandle`.
    pub async fn ensure_connected(&self) -> WeftResult<()> {
        self.conn.ensure_connected().await.map_err(Into::into)
    }

    /// Is the caller attached right now? Pure status read.
    pub fn is_connected(&self) -> bool {
        self.conn.is_connected()
    }

    /// Send a message (non-terminal). Concurrent sends interleave.
    pub async fn send(&self, msg: OutboundChunk) -> WeftResult<()> {
        self.conn.send_chunk(msg).await.map_err(Into::into)
    }

    /// Await the next inbound message on this handle's built-in cursor
    /// (UNBOUNDED: may wait minutes or hours; ends only on a message, a
    /// disconnect, or the session cap). Returns the TYPED [`CallerError`]
    /// (not flattened into `WeftError`) so a node can `match` every outcome,
    /// the same shape the bus cursor uses, notably [`CallerError::FellBehind`]
    /// (the cursor fell behind the retained window; resumable). Inbound is
    /// broadcast: every reader sees every message.
    pub async fn receive(&self) -> Result<InboundMessage, CallerError> {
        self.conn.receive(&self.cursor).await
    }

    /// Send a message, then read the next inbound from this handle's cursor
    /// (UNBOUNDED, see [`Self::receive`]). Typed error. Not strictly
    /// correlated (inbound is broadcast); see the
    /// `CallerConnection::request` contract.
    pub async fn request(&self, msg: OutboundChunk) -> Result<InboundMessage, CallerError> {
        self.conn.request(msg, &self.cursor).await
    }

    /// Read the next inbound message, collapsing the genuinely TERMINAL
    /// outcomes (disconnect, timeout, session cap) to `Ok(None)` so a node
    /// loops simply: `while let Some(msg) = ws.recv_next().await? { .. }`.
    /// A `FellBehind` (resumable) and real failures (`WrongProtocol`,
    /// transport, ...) propagate as `Err` (the built-in forward cursor never
    /// falls behind, so in practice only disconnect/cap end the loop). This
    /// is the language doing the end-of-stream classification so the node
    /// author writes domain logic, not a match over `CallerError`.
    ///
    /// NOTE: `recv_next` flattens its `Err` into the opaque `WeftError`, so a
    /// node that wants to RESUME after a `FellBehind` (read on at the retained
    /// floor instead of failing) must use the typed [`Self::receive`] and
    /// match `CallerError::FellBehind` itself; `recv_next` is the
    /// fail-fast-on-anything-unexpected convenience for the common case.
    pub async fn recv_next(&self) -> WeftResult<Option<InboundMessage>> {
        recv_next_from(self.conn.receive(&self.cursor).await)
    }

    /// Close the session. Terminal; first terminal wins.
    pub async fn close(&self) -> WeftResult<()> {
        self.conn.terminate(None).await.map_err(Into::into)
    }

    // ----- cursor positioning (mirrors the bus) -------------------------

    /// The current "now" offset of the inbound stream (one past the highest
    /// message received). Pair with [`Self::cursor_at`] to position
    /// relative to now (e.g. `cursor_at(now.saturating_sub(n))`).
    pub fn now_offset(&self) -> u64 {
        self.conn.inbound_now_offset()
    }

    /// The earliest inbound offset still resident in RAM. A cursor cannot
    /// read below this (older messages trimmed; cursors never read the DB).
    pub fn retained_floor(&self) -> u64 {
        self.conn.inbound_retained_floor()
    }

    /// A fresh independent forward cursor pinned at the current now. Each
    /// cursor reads the broadcast stream independently (its own position),
    /// so two cursors both see every message from where each started.
    pub fn cursor(&self) -> CallerCursor {
        CallerCursor::new(self.conn.clone(), self.conn.inbound_now_offset())
    }

    /// A cursor positioned at an explicit `offset`. The general primitive:
    /// forward-from-now is `cursor_at(now)`, history-from-the-window-start
    /// is `cursor_at(retained_floor)`, last-`n` is `cursor_at(now - n)`. An
    /// offset below the retained floor reads the earliest still-retained
    /// message (RAM-only: the window is the whole readable world).
    pub fn cursor_at(&self, offset: u64) -> CallerCursor {
        CallerCursor::new(self.conn.clone(), offset)
    }

    /// A cursor from the earliest inbound message STILL RETAINED in RAM
    /// (not necessarily offset 0: a windowed stream trims old messages).
    /// This reads everything a cursor can still reach, oldest first. On a
    /// journaled connection the trimmed-out prefix lives in the DB but a
    /// cursor never reads it.
    pub fn cursor_from_start(&self) -> CallerCursor {
        CallerCursor::new(self.conn.clone(), self.conn.inbound_retained_floor())
    }

    /// A forward cursor that ALSO replays the single most recent message
    /// already received (if any), so a late reader can grab the latest
    /// state without replaying all history. If no message has arrived yet,
    /// it is a plain forward cursor at now.
    pub fn cursor_including_last(&self) -> CallerCursor {
        let start = self.conn.last_inbound_offset().unwrap_or_else(|| self.conn.inbound_now_offset());
        CallerCursor::new(self.conn.clone(), start)
    }
}

/// An independent reader position over a WebSocket caller's broadcast
/// inbound stream. The same concept as `bus::BusCursor`: a forward iterator
/// you advance with `receive`/`request`. Mint via `WsCaller::cursor*`.
/// Cursors only ever read the in-RAM window, never the DB.
#[derive(Clone)]
pub struct CallerCursor {
    conn: Arc<dyn CallerConnection>,
    pos: Arc<std::sync::atomic::AtomicU64>,
}

impl CallerCursor {
    fn new(conn: Arc<dyn CallerConnection>, start: u64) -> Self {
        Self { conn, pos: Arc::new(std::sync::atomic::AtomicU64::new(start)) }
    }

    /// Await the next inbound message at this cursor (UNBOUNDED, see
    /// [`WsCaller::receive`]), advancing the cursor on success. Returns the
    /// TYPED [`CallerError`] so a node can `match` every outcome (notably
    /// [`CallerError::FellBehind`] when this cursor's absolute offset fell
    /// behind the retained window). On `FellBehind` the cursor advances to the
    /// floor; call again to resume there, or stop.
    pub async fn receive(&self) -> Result<InboundMessage, CallerError> {
        self.conn.receive(&self.pos).await
    }

    /// Send a message, then read the next inbound at this cursor (UNBOUNDED).
    /// Typed error (see [`Self::receive`]).
    pub async fn request(&self, msg: OutboundChunk) -> Result<InboundMessage, CallerError> {
        self.conn.request(msg, &self.pos).await
    }

    /// Read the next inbound at this cursor, collapsing end-of-stream to
    /// `Ok(None)` so a node loops `while let Some(m) = cur.recv_next().await? {}`.
    /// See [`WsCaller::recv_next`].
    pub async fn recv_next(&self) -> WeftResult<Option<InboundMessage>> {
        recv_next_from(self.conn.receive(&self.pos).await)
    }

    /// This cursor's current position (next offset it will read).
    pub fn position(&self) -> u64 {
        self.pos.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Map a raw `receive` result to the `recv_next` shape: a message is
/// `Some`, an end-of-stream outcome is `None`, a real error propagates.
/// Shared by `WsCaller` and `CallerCursor` so the classification lives once.
fn recv_next_from(
    res: Result<InboundMessage, CallerError>,
) -> WeftResult<Option<InboundMessage>> {
    match res {
        Ok(msg) => Ok(Some(msg)),
        Err(e) if e.ends_stream() => Ok(None),
        Err(e) => Err(e.into()),
    }
}

// ----- Hand-rolled fake (layer-3 test rig) ---------------------------

/// One recorded interaction with the fake caller connection. Append-only
/// log, in call order; tests assert against it. Dumb by construction: the
/// fake records and replays scripted state, no business logic.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerCall {
    EnsureConnected,
    SendChunk(OutboundChunk),
    Terminate(Option<OutboundChunk>),
    Receive,
    Request(OutboundChunk),
    HttpRequest,
}

/// Hand-rolled fake `CallerConnection` for contract tests. Records every
/// call in an append-only log and serves scripted state (connected flag,
/// queued inbound messages, the HTTP request parts). Enforces the one
/// piece of real state a fake legitimately owns: the terminate-once latch
/// (a pure transition via [`try_terminate`]), because "first terminal
/// wins" is the contract under test and must behave like production.
#[cfg(any(test, feature = "test-helpers"))]
pub struct FakeCallerConnection {
    config: CallerRuntimeConfig,
    inner: std::sync::Mutex<FakeCallerInner>,
}

#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
struct FakeCallerInner {
    connected: bool,
    terminated: bool,
    calls: Vec<CallerCall>,
    /// Inbound messages handed out by `receive` / `request` in order.
    inbound: std::collections::VecDeque<InboundMessage>,
    /// Scripted HTTP request parts returned by `http_request`.
    http_request: Option<Arc<HttpRequestParts>>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeCallerConnection {
    /// A fake that starts already connected (the common case: the caller
    /// arrived before the node ran).
    pub fn connected(config: CallerRuntimeConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            inner: std::sync::Mutex::new(FakeCallerInner {
                connected: true,
                ..Default::default()
            }),
        })
    }

    /// A fake that starts disconnected (script `set_connected(true)` to
    /// simulate the caller arriving).
    pub fn disconnected(config: CallerRuntimeConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            inner: std::sync::Mutex::new(FakeCallerInner::default()),
        })
    }

    pub fn set_connected(&self, connected: bool) {
        self.inner.lock().expect("fake caller poisoned").connected = connected;
    }

    /// Queue an inbound message for the next `receive` / `request`.
    pub fn push_inbound(&self, msg: InboundMessage) {
        self.inner
            .lock()
            .expect("fake caller poisoned")
            .inbound
            .push_back(msg);
    }

    pub fn set_http_request(&self, parts: HttpRequestParts) {
        self.inner.lock().expect("fake caller poisoned").http_request = Some(Arc::new(parts));
    }

    /// The append-only call log, in order.
    pub fn calls(&self) -> Vec<CallerCall> {
        self.inner.lock().expect("fake caller poisoned").calls.clone()
    }

    fn record(&self, call: CallerCall) {
        self.inner
            .lock()
            .expect("fake caller poisoned")
            .calls
            .push(call);
    }

    /// Resolve a gone-caller talk into the policy-correct outcome: under
    /// `cancel` it errors (mapped to a cancel by `WeftError`), under
    /// `keep-running` it is a silent no-op into the void. Mirrors what the
    /// production connection does so the fake exercises the same contract.
    fn disconnected_outcome(&self) -> Result<(), CallerError> {
        match resolve_disconnect(self.config.suspend) {
            DisconnectAction::ContinueIntoVoid => Ok(()),
            DisconnectAction::CancelExecution => Err(CallerError::Disconnected),
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl CallerConnection for FakeCallerConnection {
    fn config(&self) -> &CallerRuntimeConfig {
        &self.config
    }

    fn is_connected(&self) -> bool {
        self.inner.lock().expect("fake caller poisoned").connected
    }

    async fn ensure_connected(&self) -> Result<(), CallerError> {
        self.record(CallerCall::EnsureConnected);
        if self.is_connected() {
            Ok(())
        } else {
            // The fake never blocks: a test scripts arrival via
            // `set_connected(true)` before the call. A no-show is a
            // bounded-wait timeout (the caller never arrived); a
            // was-connected-then-dropped drop is modeled by the talk
            // methods via the disconnect policy, not here.
            Err(CallerError::Timeout {
                waited_secs: self.config.connect_timeout_secs,
            })
        }
    }

    async fn send_chunk(&self, chunk: OutboundChunk) -> Result<(), CallerError> {
        self.record(CallerCall::SendChunk(chunk));
        if self.is_connected() {
            Ok(())
        } else {
            self.disconnected_outcome()
        }
    }

    async fn terminate(&self, final_chunk: Option<OutboundChunk>) -> Result<(), CallerError> {
        self.record(CallerCall::Terminate(final_chunk));
        let mut g = self.inner.lock().expect("fake caller poisoned");
        try_terminate(g.terminated)?;
        g.terminated = true;
        Ok(())
    }

    async fn receive(
        &self,
        cursor: &std::sync::atomic::AtomicU64,
    ) -> Result<InboundMessage, CallerError> {
        self.record(CallerCall::Receive);
        if self.config.protocol != Protocol::Websocket {
            return Err(CallerError::WrongProtocol {
                protocol: self.config.protocol.as_wire_str(),
            });
        }
        // Cursor-indexed read over the scripted inbound (every reader sees
        // every message, mirroring production). The fake never blocks, so an
        // exhausted script models "no more is coming" = a disconnect, the
        // SAME terminal production surfaces when the inbound log closes.
        // Production `receive` NEVER returns `Timeout` (the read is unbounded;
        // a node may wait hours), so the fake must not either.
        let idx = cursor.load(Ordering::SeqCst) as usize;
        let g = self.inner.lock().expect("fake caller poisoned");
        match g.inbound.get(idx).cloned() {
            Some(m) => {
                cursor.fetch_add(1, Ordering::SeqCst);
                Ok(m)
            }
            None => Err(CallerError::Disconnected),
        }
    }

    async fn request(
        &self,
        msg: OutboundChunk,
        cursor: &std::sync::atomic::AtomicU64,
    ) -> Result<InboundMessage, CallerError> {
        self.record(CallerCall::Request(msg));
        if self.config.protocol != Protocol::Websocket {
            return Err(CallerError::WrongProtocol {
                protocol: self.config.protocol.as_wire_str(),
            });
        }
        // Exhausted script = disconnect (see `receive`), never `Timeout`.
        let idx = cursor.load(Ordering::SeqCst) as usize;
        let g = self.inner.lock().expect("fake caller poisoned");
        match g.inbound.get(idx).cloned() {
            Some(m) => {
                cursor.fetch_add(1, Ordering::SeqCst);
                Ok(m)
            }
            None => Err(CallerError::Disconnected),
        }
    }

    fn http_request(&self) -> Result<Arc<HttpRequestParts>, CallerError> {
        // Not recorded via `record` to keep `&self` non-async-lock simple;
        // record then read under the same lock.
        let mut g = self.inner.lock().expect("fake caller poisoned");
        g.calls.push(CallerCall::HttpRequest);
        if self.config.protocol != Protocol::Http {
            return Err(CallerError::WrongProtocol {
                protocol: self.config.protocol.as_wire_str(),
            });
        }
        g.http_request
            .clone()
            .ok_or_else(|| CallerError::Transport("no http request scripted".into()))
    }

    fn inbound_now_offset(&self) -> u64 {
        // The fake indexes inbound by cursor == position, never trims, so
        // "now" is the count of scripted messages.
        self.inner.lock().expect("fake caller poisoned").inbound.len() as u64
    }

    fn inbound_attach_offset(&self) -> u64 {
        // The fake models attach at construction with an empty log, so the
        // attach offset is 0 (a built-in forward cursor sees every scripted
        // message, matching the production race-free attach pin).
        0
    }

    fn inbound_retained_floor(&self) -> u64 {
        // The fake never trims: everything from offset 0 is retained.
        0
    }

    fn last_inbound_offset(&self) -> Option<u64> {
        let n = self.inner.lock().expect("fake caller poisoned").inbound.len();
        (n as u64).checked_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tied() -> SuspendPolicy {
        SuspendPolicy { can_suspend: false, default_hold_secs: 300 }
    }
    fn survives() -> SuspendPolicy {
        SuspendPolicy { can_suspend: true, default_hold_secs: 300 }
    }

    #[test]
    fn disconnect_derives_from_suspendability() {
        // Caller-tied (can't suspend): a disconnect cancels the run.
        assert_eq!(resolve_disconnect(tied()), DisconnectAction::CancelExecution);
        // Survives: a disconnect just sends into the void, run continues.
        assert_eq!(resolve_disconnect(survives()), DisconnectAction::ContinueIntoVoid);
    }

    #[test]
    fn inbound_size_cap() {
        assert!(check_inbound_size(100, 100).is_ok(), "at cap is allowed");
        assert!(check_inbound_size(99, 100).is_ok());
        let err = check_inbound_size(101, 100).expect_err("over cap");
        assert!(matches!(
            err,
            CallerError::InboundTooLarge { got_bytes: 101, cap_bytes: 100 }
        ));
    }

    #[test]
    fn session_duration_cap_disabled_at_zero() {
        assert!(check_session_duration(1_000_000, 0).is_ok(), "0 disables the cap");
        assert!(check_session_duration(10, 30).is_ok());
        assert!(check_session_duration(30, 30).is_ok(), "at cap is allowed");
        let err = check_session_duration(31, 30).expect_err("over cap");
        assert!(matches!(err, CallerError::SessionExpired { cap_secs: 30 }));
    }

    #[test]
    fn terminate_once_then_locks_out() {
        assert!(try_terminate(false).is_ok(), "first terminal wins");
        let err = try_terminate(true).expect_err("second terminal rejected");
        assert!(matches!(err, CallerError::AlreadyTerminated));
    }

    #[test]
    fn runtime_config_projects_from_config() {
        let cfg = LiveConnectionConfig {
            path: "chat".into(),
            auth: crate::signal::PublicEntryAuth::None,
            suspend: SuspendPolicy { can_suspend: true, default_hold_secs: 120 },
            connect_timeout_secs: 12,
            heartbeat_interval_secs: 25,
            max_inbound_bytes: 4096,
            max_session_secs: 600,
            data_type: DataType::Text,
            backpressure: Backpressure::DropNewest,
            error_mode: ErrorMode::DropChunk,
            journal_mode: crate::signal::JournalMode::Journaled,
            window: None,
        };
        let rc = CallerRuntimeConfig::from_config(&cfg, Protocol::Websocket);
        assert_eq!(rc.protocol, Protocol::Websocket);
        assert!(rc.suspend.can_suspend);
        assert_eq!(rc.suspend.default_hold_secs, 120);
        assert_eq!(rc.data_type, DataType::Text);
        assert_eq!(rc.backpressure, Backpressure::DropNewest);
        assert_eq!(rc.error_mode, ErrorMode::DropChunk);
        assert_eq!(rc.connect_timeout_secs, 12);
        assert_eq!(rc.max_inbound_bytes, 4096);
        assert_eq!(rc.max_session_secs, 600);
    }

    fn http_cfg() -> CallerRuntimeConfig {
        CallerRuntimeConfig {
            protocol: Protocol::Http,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: ErrorMode::Surface,
            connect_timeout_secs: 5,
            max_inbound_bytes: 1024,
            max_session_secs: 0,
            // Caller-tied: a gone caller cancels (exercised below).
            suspend: tied(),
            inbound_window: DEFAULT_INBOUND_WINDOW,
        }
    }

    fn ws_cfg() -> CallerRuntimeConfig {
        CallerRuntimeConfig { protocol: Protocol::Websocket, ..http_cfg() }
    }

    #[tokio::test]
    async fn fake_records_calls_and_enforces_terminate_once() {
        let fake = FakeCallerConnection::connected(http_cfg());
        let handle = CallerHandle::from_connection(fake.clone());
        let CallerHandle::Http(http) = handle else {
            panic!("http config must yield an Http handle");
        };
        http.write(OutboundChunk::Json(serde_json::json!("a"))).await.unwrap();
        http.respond(OutboundChunk::Json(serde_json::json!("done"))).await.unwrap();
        // Second terminal fails loud.
        let err = http.close().await.expect_err("second terminal rejected");
        assert!(err.to_string().contains("already completed"), "got: {err}");
        assert_eq!(
            fake.calls(),
            vec![
                CallerCall::SendChunk(OutboundChunk::Json(serde_json::json!("a"))),
                CallerCall::Terminate(Some(OutboundChunk::Json(serde_json::json!("done")))),
                CallerCall::Terminate(None),
            ]
        );
    }

    #[tokio::test]
    async fn ensure_connected_waits_then_oks_and_times_out() {
        let fake = FakeCallerConnection::disconnected(http_cfg());
        let handle = CallerHandle::from_connection(fake.clone());
        // No-show: bounded timeout, fails loud.
        let err = handle.ensure_connected().await.expect_err("no-show times out");
        assert!(err.to_string().contains("did not connect"), "got: {err}");
        // Caller arrives: now a no-op success.
        fake.set_connected(true);
        handle.ensure_connected().await.expect("connected now");
        assert!(handle.is_connected());
    }

    #[tokio::test]
    async fn talk_into_void_when_survives_but_cancels_when_tied() {
        // survives (can_suspend = true): a gone caller is a silent no-op.
        let keep = FakeCallerConnection::connected(CallerRuntimeConfig {
            suspend: survives(),
            ..http_cfg()
        });
        keep.set_connected(false);
        let CallerHandle::Http(h) = CallerHandle::from_connection(keep) else { unreachable!() };
        h.write(OutboundChunk::Json(serde_json::json!("x"))).await
            .expect("survives drops into the void, no error");

        // tied (can_suspend = false): a gone caller errors (maps to cancel).
        let cancel = FakeCallerConnection::connected(http_cfg());
        cancel.set_connected(false);
        let CallerHandle::Http(h2) = CallerHandle::from_connection(cancel) else { unreachable!() };
        let err = h2.write(OutboundChunk::Json(serde_json::json!("x"))).await
            .expect_err("tied run errors on gone caller");
        // Maps through WeftError to Cancelled.
        assert!(matches!(err, crate::error::WeftError::Cancelled), "got: {err:?}");
    }

    #[tokio::test]
    async fn ws_round_trip_and_wrong_protocol_guard() {
        let fake = FakeCallerConnection::connected(ws_cfg());
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(fake.clone()) else {
            panic!("ws config must yield a Websocket handle");
        };
        // Forward-default: the handle's cursor is pinned at attach, so the
        // reply must arrive AFTER the handle exists to be seen (the real
        // request/reply order; a message sent before subscribing is not
        // replayed by a forward cursor).
        fake.push_inbound(InboundMessage::Json(serde_json::json!("pong")));
        let reply = ws.request(OutboundChunk::Json(serde_json::json!("ping"))).await.unwrap();
        assert_eq!(reply, InboundMessage::Json(serde_json::json!("pong")));

        // http_request on a websocket connection is the wrong-protocol error.
        let err = fake.http_request().expect_err("http_request invalid on ws");
        assert!(matches!(err, CallerError::WrongProtocol { .. }));
    }
}
