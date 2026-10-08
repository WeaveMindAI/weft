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
//!   fake (shipped, the node-test rig attaches it) records calls and
//!   scripts inbound messages. The
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

use std::collections::BTreeMap;
use std::sync::Arc;

use std::sync::atomic::Ordering;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::WeftResult;
use crate::signal::{Backpressure, DataType, ErrorMode, LiveConnectionConfig, Protocol};

/// What the caller sent to OPEN the exchange, as the worker's door
/// (`weft_engine::door`) saw it: the method, path, query and headers of
/// their call, the route's captures and the gate's verdict. One shape for
/// both protocols, built when the run is born at the door. From there it
/// is carried everywhere it is read: the trigger's wake payload (so a
/// trigger node fans it onto ports), the task of a durable or fired run
/// (`LiveConnectionStart`), and [`HttpRequestParts`] (beside the body).
///
/// `path` is the path AS CALLED under the project, without the tenant
/// prefix and without a leading slash (`chat/room7`); `params` are the
/// route pattern's captures for it (`{"room": "room7"}`); `caller` is
/// the identity the auth gate established (`None` on an open route).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveRequest {
    /// Absent reads as empty rather than refusing, so a trigger whose
    /// method is fixed by the protocol can leave it out of what it
    /// wakes with. A socket upgrade is always a GET, so requiring it
    /// there would make a hand-typed `weft run --fire` fail deep in the
    /// node instead of at the envelope check that names the fix.
    #[serde(default)]
    pub method: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub query: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller: Option<Value>,
    /// The address the caller reached this install at (its request's own
    /// `Host` / `X-Forwarded-*`, [`crate::net::request_base_url`]),
    /// stated by the birth that read the caller's request. What a link
    /// this caller will fetch is built on (a route's answer carrying a
    /// file): a browser on the loopback port gets a loopback link, one
    /// on the tunnel a tunnel link. `None` for a run no real request
    /// started (`weft run --fire`), whose links fall back to the
    /// install's configured address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

/// A header's value out of a name/value list, case-insensitively, as
/// HTTP headers are read.
fn find_header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
}

impl LiveRequest {
    /// Read a header case-insensitively, as HTTP headers are.
    pub fn header(&self, name: &str) -> Option<&str> {
        find_header(&self.headers, name)
    }

    /// The credentials this request carries, which its run keeps in memory
    /// and never writes down ([`Redaction`]): the values of the headers that
    /// carry credentials on any request ([`CREDENTIAL_HEADERS`]) and of
    /// `also`, the headers the route's own gate read.
    pub fn credentials(&self, also: &[String]) -> Redaction {
        let mut secrets = Vec::new();
        for (name, value) in &self.headers {
            let carries = CREDENTIAL_HEADERS.iter().any(|h| name.eq_ignore_ascii_case(h))
                || also.iter().any(|h| name.eq_ignore_ascii_case(h));
            if !carries || value.trim().is_empty() {
                continue;
            }
            secrets.push(value.trim().to_string());
            // `Bearer <token>`: the token alone is the credential too, and
            // a node passing it on writes it without its scheme.
            if let Some((_, token)) = value.trim().split_once(' ') {
                if !token.trim().is_empty() {
                    secrets.push(token.trim().to_string());
                }
            }
        }
        // The longest first, so a value that contains another is replaced
        // whole.
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        secrets.dedup();
        Redaction { secrets }
    }
}

/// Headers that carry a credential on any request, whatever the route's
/// gate reads: an `Authorization`, a proxy's, a session cookie, and an
/// instance token.
pub const CREDENTIAL_HEADERS: &[&str] =
    &["authorization", "proxy-authorization", "cookie", crate::instance::INSTANCE_TOKEN_HEADER];

/// How long a credential is before it is also replaced inside other text
/// (see [`Redaction::scrub`]).
pub const SCRUBBED_INSIDE: usize = 8;

/// What stands in a run's record for a credential its caller sent.
pub const REDACTED: &str = "(a credential the caller sent: not written down)";

/// The credentials a run holds in memory and never writes down: wherever
/// one appears in what the run records (its opening request, a value a node
/// passes on), the record holds [`REDACTED`] in its place. The live run
/// keeps the real values: only its record is cleaned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Redaction {
    secrets: Vec<String>,
}

impl Redaction {
    /// Nothing to clean: the record is written as it is.
    pub fn is_empty(&self) -> bool {
        self.secrets.is_empty()
    }

    /// `value` with every credential replaced, in every string it holds. A
    /// credential is replaced wherever it appears inside a string; one
    /// shorter than [`SCRUBBED_INSIDE`] only where it is the whole string,
    /// since a few characters turn up inside unrelated text (an id, a
    /// count) and replacing them there would corrupt the record.
    pub fn scrub(&self, value: &mut serde_json::Value) {
        if self.is_empty() {
            return;
        }
        match value {
            serde_json::Value::String(text) => {
                let inside = |secret: &&String| secret.len() >= SCRUBBED_INSIDE && text.contains(secret.as_str());
                if self.secrets.iter().any(|secret| secret == text) {
                    *text = REDACTED.to_string();
                } else if self.secrets.iter().any(|secret| inside(&secret)) {
                    let mut cleaned = text.clone();
                    for secret in self.secrets.iter().filter(|secret| secret.len() >= SCRUBBED_INSIDE) {
                        cleaned = cleaned.replace(secret.as_str(), REDACTED);
                    }
                    *text = cleaned;
                }
            }
            serde_json::Value::Array(items) => items.iter_mut().for_each(|item| self.scrub(item)),
            serde_json::Value::Object(fields) => fields.values_mut().for_each(|field| self.scrub(field)),
            _ => {}
        }
    }
}

/// The status line and headers of an HTTP response. Held by the
/// worker until the program's first outbound item, so the program
/// decides them; a `Default` head is `200` with no headers. Ignored
/// on a WebSocket connection, whose only head is the upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseHead {
    pub status: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    /// Bytes a reader of this body ignores (an SSE comment line, a bare
    /// newline between ndjson lines), which the worker writes when the
    /// body has been quiet for a heartbeat. A write is the only thing
    /// that finds a caller who left without a word, so a body with no
    /// filler (a raw stream, where any byte is payload) is only found
    /// gone when the program next writes, or when the connection itself
    /// reports the drop. Whoever frames the body names its filler.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive: Option<String>,
}

impl Default for ResponseHead {
    fn default() -> Self {
        Self { status: 200, headers: Vec::new(), keepalive: None }
    }
}

impl ResponseHead {
    pub fn new(status: u16) -> Self {
        Self { status, headers: Vec::new(), keepalive: None }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// The head with the filler this body's framing ignores (see
    /// [`Self::keepalive`]).
    pub fn with_keepalive(mut self, filler: impl Into<String>) -> Self {
        self.keepalive = Some(filler.into());
        self
    }

    /// Read a header the head carries, case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        find_header(&self.headers, name)
    }

    /// Does the head already name a content type?
    pub fn has_content_type(&self) -> bool {
        self.header("content-type").is_some()
    }

    /// The content type the head names, without its parameters, lower
    /// case (`application/x-ndjson` for `application/x-ndjson; charset=utf-8`).
    fn content_type_essence(&self) -> Option<String> {
        self.header("content-type").map(|v| v.split(';').next().unwrap_or("").trim().to_ascii_lowercase())
    }

    /// The bytes that tell a reader of this body the program failed,
    /// once the status line is already out: in the body's own framing,
    /// so a reader parsing it gets the error as an item it can read.
    /// JSON lines get one line `{"error": "..."}`, server-sent events an
    /// `error` event, anything else a text line `[error] ...`. Each
    /// starts on a fresh line, since the last item may not have ended
    /// one (a blank line is skipped by a JSON-lines or SSE reader).
    pub fn in_band_error(&self, message: &str) -> Vec<u8> {
        const JSON_LINES: &[&str] = &[
            "application/x-ndjson",
            "application/ndjson",
            "application/jsonl",
            "application/x-jsonl",
            "application/jsonlines",
            "application/x-jsonlines",
        ];
        let essence = self.content_type_essence();
        match essence.as_deref() {
            Some(ct) if JSON_LINES.contains(&ct) => {
                format!("\n{}\n", serde_json::json!({ "error": message })).into_bytes()
            }
            Some("text/event-stream") => {
                let mut out = String::from("\nevent: error\n");
                for line in message.split('\n') {
                    out.push_str("data: ");
                    out.push_str(line);
                    out.push('\n');
                }
                out.push('\n');
                out.into_bytes()
            }
            _ => format!("\n[error] {message}").into_bytes(),
        }
    }

    /// The head with a content type matching the chunk's shape
    /// (`application/json`, `text/plain; charset=utf-8`,
    /// `application/octet-stream`), unless it already names one.
    pub fn with_content_type_for(mut self, chunk: &OutboundChunk) -> Self {
        if !self.has_content_type() {
            let ct = match chunk {
                OutboundChunk::Json(_) => "application/json",
                OutboundChunk::Text(_) => "text/plain; charset=utf-8",
                OutboundChunk::Bytes(_) => "application/octet-stream",
            };
            self.headers.push(("content-type".into(), ct.into()));
        }
        self
    }
}

/// Why a WebSocket session ends, as the close frame carries it. The
/// default is the normal closure (`1000`) with no reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseReason {
    pub code: u16,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

impl Default for CloseReason {
    fn default() -> Self {
        Self { code: 1000, reason: String::new() }
    }
}

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
    /// Reject an inbound body / message larger than this.
    pub max_inbound_bytes: u64,
    /// How long the caller's machine may leave what we sent it
    /// unacknowledged before the connection is called dead. `0` leaves
    /// the machine's own default. See
    /// [`crate::signal::DEFAULT_CALLER_SILENCE_SECS`].
    pub caller_silence_secs: u64,
    /// Max total session duration. `0` = no cap.
    pub max_session_secs: u64,
    /// Whether the run goes on once its caller has left (the trigger's
    /// `outlivesCaller`). Decides what a disconnect means (see
    /// [`resolve_disconnect`]): a run that does not outlive its caller is
    /// cancelled when it leaves, and cannot pause while it is on the line;
    /// one that does carries on, its sends going into the void.
    pub outlives_caller: bool,
    /// In-RAM inbound window size (WebSocket): how many recent messages the
    /// connection retains for cursors. Bounds RAM on a long-lived socket;
    /// cursors only read this window (never the DB). Always >= 1.
    pub inbound_window: usize,
    /// What the journal keeps of this conversation. The same type a bus
    /// carries, so the two cannot answer differently: how long a window
    /// accumulates before one row goes out, whether content is kept at
    /// all, and where it gets trimmed.
    pub journal: crate::stream_journal::JournalPolicy,
}

impl CallerRuntimeConfig {
    /// Project a live-caller trigger's config onto the runtime subset the
    /// connection layer needs. `protocol` comes from which kind fired
    /// (`Route` -> `Http`, `Socket` -> `Websocket`), not a config field,
    /// so it is passed alongside the shared body.
    pub fn from_config(cfg: &LiveConnectionConfig, protocol: Protocol) -> Self {
        Self {
            protocol,
            data_type: cfg.data_type,
            backpressure: cfg.backpressure,
            error_mode: cfg.error_mode,
            max_inbound_bytes: cfg.max_inbound_bytes,
            caller_silence_secs: cfg.caller_silence_secs,
            max_session_secs: cfg.max_session_secs,
            outlives_caller: cfg.outlives_caller,
            inbound_window: cfg.window.unwrap_or(DEFAULT_INBOUND_WINDOW),
            journal: cfg.journal_policy(),
        }
    }
}

/// Default in-RAM inbound window for a WebSocket caller. Matches the bus
/// default; bounds RAM without truncating short conversations.
pub const DEFAULT_INBOUND_WINDOW: usize = 64;

/// What happens when the caller is gone (disconnected, or the response
/// completed: the same event from the run's view), read off the run's
/// `outlivesCaller`: a run tied to its caller ends with it; a run that
/// outlives its caller keeps running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectAction {
    /// Cancel THIS execution (via the per-execution cancel-by-execution
    /// path). Never the process, which multiplexes many runs.
    CancelExecution,
    /// Keep running to completion; further sends to the caller go into
    /// the void.
    ContinueIntoVoid,
}

/// Resolve the disconnect action from the run's `outlivesCaller`. Pure;
/// the one place the mapping lives.
pub fn resolve_disconnect(outlives_caller: bool) -> DisconnectAction {
    if outlives_caller {
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
    /// A response head was given after the first item already went to
    /// the wire. The status line and headers are committed by the first
    /// outbound item (an explicit head, or a chunk with the defaults),
    /// so a later head cannot be honored; fails loud rather than
    /// silently dropping the status the program asked for.
    #[error("response head already sent; status and headers are set by the first outbound item")]
    HeadAlreadySent,
    /// An inbound body / message exceeded `max_inbound_bytes`. Rejected
    /// loud (untrusted-caller abuse vector).
    #[error("inbound payload {got_bytes} bytes exceeds cap {cap_bytes}")]
    InboundTooLarge { got_bytes: u64, cap_bytes: u64 },
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
    /// This exchange stopped being written to the journal, so the run
    /// refuses to keep answering: what it said from here on would exist
    /// nowhere afterwards. The twin of the bus's
    /// `SendError::JournalDegraded`, and for the same reason: of the
    /// three ways to handle a lost record, carrying on quietly is the
    /// one that leaves a run looking clean with its conversation
    /// missing, which nobody finds until they go looking.
    #[error("this caller's exchange is no longer being recorded: {0}")]
    JournalLost(String),
}

impl CallerError {
    /// Does this outcome mean "the inbound stream has ended, stop reading"
    /// (as opposed to a failure to propagate)? True for the one genuinely
    /// terminal outcome a read loop should break on cleanly: the caller
    /// disconnected, so nothing more will arrive. The session cap lands here
    /// too, because the engine enforces it at the transport
    /// (`ExchangeEnd::SessionCapExceeded`) by closing the connection.
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
        matches!(self, CallerError::Disconnected)
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

/// Whether an explicit response head may still be sent, given whether
/// anything already went to the wire. Same pure transition shape as
/// [`try_terminate`]: the first outbound item commits the head, every
/// later head is refused.
pub fn try_send_head(wire_started: bool) -> Result<(), CallerError> {
    if wire_started {
        Err(CallerError::HeadAlreadySent)
    } else {
        Ok(())
    }
}

/// Why a run whose HTTP caller never heard a word ended badly: every path
/// to its answer closed. Said to the caller and recorded on the run.
pub const NO_ANSWER: &str = "the run ended without answering: every path to its Reply (or Stream, or Close) \
     was skipped, usually because a value it waited on never arrived";

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
///   - fake (below, shipped: the node-test rig attaches it): records
///     calls, scripts inbound messages.
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

    /// Resolves once the caller is gone, at once when it already is.
    /// Lets a wait that holds for a caller notice a hang-up the moment
    /// it happens instead of polling `is_connected`.
    async fn disconnected(&self);

    /// Has anything been queued toward the wire yet (a chunk, a
    /// terminal, a head)? Once true the response head is committed and
    /// an explicit one is refused (`HeadAlreadySent`). Pure status
    /// read, so a node that may run before or after a stream (a Close)
    /// can pick the right terminal without a refusal round-trip.
    fn wire_started(&self) -> bool;

    /// Is an HTTP caller still on the line with nothing sent to it yet?
    /// A run that completes in that state answered nobody, which is a
    /// bug in its graph, so the run is recorded as failed (and its
    /// caller gets the error) rather than as a clean completion. Only a
    /// real caller is owed an answer: a fired run's stand-in and the
    /// test fake are not.
    fn owes_answer(&self) -> bool {
        false
    }

    /// Is the caller still attached? Returns immediately, never waits:
    /// `Ok(())` when the socket is there, otherwise the resolved
    /// disconnect outcome (under `cancel` a `Disconnected`, under
    /// `keep-running` an `Ok(())` into the void).
    ///
    /// A caller's run is born with its caller's connection in hand, so
    /// there is no wait for one to show up.
    async fn ensure_connected(&self) -> Result<(), CallerError>;

    /// What the caller sent to open the exchange (method, path, route
    /// parameters, query, headers, the gate's identity). Valid for both
    /// protocols: an HTTP call's request line, a WebSocket's upgrade.
    fn handshake(&self) -> Arc<LiveRequest>;

    /// Append a non-terminal outbound chunk (HTTP `write`, WS `send`).
    /// Free-for-all: concurrent chunks from multiple nodes interleave on
    /// the wire. Honors the backpressure policy. Errors loud on
    /// transport failure or if the caller is gone under a `cancel` policy
    /// (`Disconnected`); under `keep-running` a gone caller is a silent
    /// no-op (`Ok(())`) into the void.
    ///
    /// `head` sets the HTTP status line and headers; it is honored only
    /// on the FIRST outbound item (`HeadAlreadySent` after that). A
    /// WebSocket connection ignores it: its only head is the upgrade.
    async fn send_chunk(
        &self,
        head: Option<ResponseHead>,
        chunk: OutboundChunk,
    ) -> Result<(), CallerError>;

    /// Terminate the exchange: HTTP one-shot `respond(body)` (a final
    /// body with no prior streaming) OR `close()` after streaming; WS
    /// `close()`. First terminal wins; a later one returns
    /// `AlreadyTerminated`. `final_chunk` carries the one-shot HTTP body
    /// (`Some`) or is `None` for a bare close. `head` follows the same
    /// first-item rule as [`Self::send_chunk`]; `close` is the WebSocket
    /// close frame's code and reason (`None` = normal closure), ignored
    /// on HTTP.
    async fn terminate(
        &self,
        head: Option<ResponseHead>,
        final_chunk: Option<OutboundChunk>,
        close: Option<CloseReason>,
    ) -> Result<(), CallerError>;

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

    /// HTTP only: the inbound request (the handshake plus the body
    /// decoded per the declared data type). WebSocket inbound flows
    /// through `receive`/`request` instead. `Err(WrongProtocol)` on
    /// WebSocket.
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

/// The inbound HTTP request a node reads on an `http` live connection:
/// the handshake (method, path, params, query, headers, caller) and the
/// body decoded to an [`InboundMessage`] per the declared data type;
/// the size cap was already enforced at decode time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRequestParts {
    pub request: LiveRequest,
    pub body: InboundMessage,
}

/// Author-facing ergonomic wrapper. An enum over the two protocol
/// shapes so the talk surface is honest: the `Http` variant exposes
/// respond/write/close and the request parts; the `Websocket` variant
/// exposes send/receive/request/close. Both share the queries
/// (`is_connected`, `ensure_connected`).
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

    /// Is the caller still there? Never waits; a gone caller fails loud
    /// under the disconnect policy. Identical meaning for both protocols.
    pub async fn ensure_connected(&self) -> WeftResult<()> {
        self.conn().ensure_connected().await.map_err(Into::into)
    }

    /// What the caller sent to open the exchange, for either protocol.
    pub fn request(&self) -> Arc<LiveRequest> {
        self.conn().handshake()
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
    /// Is the caller still there? Never waits; a gone caller fails loud
    /// under the disconnect policy. Also reachable on the
    /// protocol-agnostic `CallerHandle`.
    pub async fn ensure_connected(&self) -> WeftResult<()> {
        self.conn.ensure_connected().await.map_err(Into::into)
    }

    /// Is the caller attached right now? Pure status read.
    pub fn is_connected(&self) -> bool {
        self.conn.is_connected()
    }

    /// Has anything gone toward the wire yet? While `false` the status
    /// line is still yours to set (`write_with`, `respond_with`,
    /// `close_with`); once `true` only the plain forms are accepted.
    pub fn wire_started(&self) -> bool {
        self.conn.wire_started()
    }

    /// The inbound request: the handshake (method, path, params, query,
    /// headers, caller) and the decoded body.
    pub fn request_parts(&self) -> WeftResult<Arc<HttpRequestParts>> {
        self.conn.http_request().map_err(Into::into)
    }

    /// Stream a non-terminal chunk to the caller. Multiple nodes may
    /// stream concurrently; chunks interleave. The first chunk on the
    /// wire commits a `200` head with a content type matching its shape;
    /// to choose the status or headers, use [`Self::write_with`] for
    /// that first chunk.
    pub async fn write(&self, chunk: OutboundChunk) -> WeftResult<()> {
        self.conn.send_chunk(None, chunk).await.map_err(Into::into)
    }

    /// [`Self::write`] with an explicit response head. Only valid as the
    /// FIRST outbound item; after that it fails with `HeadAlreadySent`.
    pub async fn write_with(&self, head: ResponseHead, chunk: OutboundChunk) -> WeftResult<()> {
        self.conn.send_chunk(Some(head), chunk).await.map_err(Into::into)
    }

    /// One-shot response: a final body with no prior streaming, under
    /// a `200` head with a content type matching the body's shape.
    /// Terminal; first terminal wins.
    pub async fn respond(&self, body: OutboundChunk) -> WeftResult<()> {
        self.conn.terminate(None, Some(body), None).await.map_err(Into::into)
    }

    /// [`Self::respond`] with an explicit status and headers. Terminal.
    pub async fn respond_with(&self, head: ResponseHead, body: OutboundChunk) -> WeftResult<()> {
        self.conn.terminate(Some(head), Some(body), None).await.map_err(Into::into)
    }

    /// Close the response after streaming (or, with nothing streamed,
    /// answer `204` with no body). Terminal; first terminal wins.
    pub async fn close(&self) -> WeftResult<()> {
        self.conn.terminate(None, None, None).await.map_err(Into::into)
    }

    /// [`Self::close`] with an explicit head and no body (a `404` with
    /// nothing to say). Terminal; only valid as the first outbound item.
    pub async fn close_with(&self, head: ResponseHead) -> WeftResult<()> {
        self.conn.terminate(Some(head), None, None).await.map_err(Into::into)
    }
}

/// WebSocket talk surface: full duplex. Holds a built-in FORWARD cursor
/// pinned at ATTACH, not at the `ctx.caller()` call, so `receive` /
/// `request` see everything since the connection opened, including what
/// landed while the node was starting (see
/// [`CallerConnection::inbound_attach_offset`]). To start somewhere
/// else, mint a positioned [`CallerCursor`] via
/// `cursor_from_start` / `cursor_at` / `cursor_including_last`. Each cursor
/// is an independent broadcast reader (no inter-node stealing).
#[derive(Clone)]
pub struct WsCaller {
    conn: Arc<dyn CallerConnection>,
    cursor: Arc<std::sync::atomic::AtomicU64>,
}

impl WsCaller {
    /// Is the caller still there? Never waits; a gone caller fails loud
    /// under the disconnect policy. Also reachable on the
    /// protocol-agnostic `CallerHandle`.
    pub async fn ensure_connected(&self) -> WeftResult<()> {
        self.conn.ensure_connected().await.map_err(Into::into)
    }

    /// Is the caller attached right now? Pure status read.
    pub fn is_connected(&self) -> bool {
        self.conn.is_connected()
    }

    /// Send a message (non-terminal). Concurrent sends interleave.
    pub async fn send(&self, msg: OutboundChunk) -> WeftResult<()> {
        self.conn.send_chunk(None, msg).await.map_err(Into::into)
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

    /// Close the session with a normal-closure frame (`1000`, no
    /// reason). Terminal; first terminal wins.
    pub async fn close(&self) -> WeftResult<()> {
        self.conn.terminate(None, None, None).await.map_err(Into::into)
    }

    /// Close the session with an explicit close code and reason.
    /// Terminal; first terminal wins.
    pub async fn close_with(&self, close: CloseReason) -> WeftResult<()> {
        self.conn.terminate(None, None, Some(close)).await.map_err(Into::into)
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
//
// Shipped, not `#[cfg(test)]`: the node-test rig (`crate::node_test`,
// itself a shipped artifact the per-package test binary links) attaches
// it so a catalog node talking to a caller is testable at the fake tier.

/// One recorded interaction with the fake caller connection. Append-only
/// log, in call order; tests assert against it. Dumb by construction: the
/// fake records and replays scripted state, no business logic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerCall {
    EnsureConnected,
    SendChunk { head: Option<ResponseHead>, chunk: OutboundChunk },
    Terminate {
        head: Option<ResponseHead>,
        final_chunk: Option<OutboundChunk>,
        close: Option<CloseReason>,
    },
    Receive,
    Request(OutboundChunk),
    HttpRequest,
}

/// Hand-rolled fake `CallerConnection` for contract tests. Records every
/// call in an append-only log and serves scripted state (connected flag,
/// queued inbound messages, the handshake, the HTTP body). Enforces the
/// two pieces of real state a fake legitimately owns, both pure
/// transitions shared with production: the terminate-once latch
/// ([`try_terminate`]) and the head-before-first-item rule
/// ([`try_send_head`]), because "first terminal wins" and "the first
/// item commits the head" are the contracts under test.
pub struct FakeCallerConnection {
    config: CallerRuntimeConfig,
    /// The connected flag, as a watch so `disconnected()` wakes on a
    /// scripted hang-up.
    connected: tokio::sync::watch::Sender<bool>,
    inner: std::sync::Mutex<FakeCallerInner>,
}

#[derive(Default)]
struct FakeCallerInner {
    terminated: bool,
    /// Something already went to the wire (a head can no longer be set).
    wire_started: bool,
    calls: Vec<CallerCall>,
    /// Inbound messages handed out by `receive` / `request` in order.
    inbound: std::collections::VecDeque<InboundMessage>,
    /// The scripted handshake (`with_handshake`); empty by default.
    handshake: Arc<LiveRequest>,
    /// The scripted HTTP body returned inside `http_request`.
    http_body: Option<InboundMessage>,
}

impl FakeCallerConnection {
    /// A fake that starts already connected (the common case: the caller
    /// arrived before the node ran).
    pub fn connected(config: CallerRuntimeConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            connected: tokio::sync::watch::Sender::new(true),
            inner: std::sync::Mutex::new(FakeCallerInner::default()),
        })
    }

    /// A fake that starts disconnected (script `set_connected(true)` to
    /// simulate the caller arriving).
    pub fn disconnected(config: CallerRuntimeConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            connected: tokio::sync::watch::Sender::new(false),
            inner: std::sync::Mutex::new(FakeCallerInner::default()),
        })
    }

    pub fn set_connected(&self, connected: bool) {
        self.connected.send_replace(connected);
    }

    /// Queue an inbound message for the next `receive` / `request`.
    pub fn push_inbound(&self, msg: InboundMessage) {
        self.inner
            .lock()
            .expect("fake caller poisoned")
            .inbound
            .push_back(msg);
    }

    /// Script what the caller sent to open the exchange (both protocols).
    pub fn set_handshake(&self, request: LiveRequest) {
        self.inner.lock().expect("fake caller poisoned").handshake = Arc::new(request);
    }

    /// Script the HTTP body `http_request` hands out beside the handshake.
    pub fn set_http_body(&self, body: InboundMessage) {
        self.inner.lock().expect("fake caller poisoned").http_body = Some(body);
    }

    /// Every response head the fake was handed, in order. On a
    /// well-behaved exchange that is at most one, on the first item.
    pub fn heads(&self) -> Vec<ResponseHead> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                CallerCall::SendChunk { head, .. } | CallerCall::Terminate { head, .. } => head,
                _ => None,
            })
            .collect()
    }

    /// Every chunk the fake was handed (streamed and final), in order.
    pub fn chunks(&self) -> Vec<OutboundChunk> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                CallerCall::SendChunk { chunk, .. } => Some(chunk),
                CallerCall::Terminate { final_chunk, .. } => final_chunk,
                _ => None,
            })
            .collect()
    }

    /// The terminal call's close reason, if the exchange was terminated.
    pub fn close_reason(&self) -> Option<Option<CloseReason>> {
        self.calls().into_iter().find_map(|c| match c {
            CallerCall::Terminate { close, .. } => Some(close),
            _ => None,
        })
    }

    /// Refuse a write after the terminal, enforce the head-before-first-item
    /// rule and mark the wire started.
    fn commit_head(g: &mut FakeCallerInner, head: &Option<ResponseHead>) -> Result<(), CallerError> {
        try_terminate(g.terminated)?;
        if head.is_some() {
            try_send_head(g.wire_started)?;
        }
        g.wire_started = true;
        Ok(())
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
        match resolve_disconnect(self.config.outlives_caller) {
            DisconnectAction::ContinueIntoVoid => Ok(()),
            DisconnectAction::CancelExecution => Err(CallerError::Disconnected),
        }
    }
}

#[async_trait]
impl CallerConnection for FakeCallerConnection {
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
        self.inner.lock().expect("fake caller poisoned").wire_started
    }

    async fn ensure_connected(&self) -> Result<(), CallerError> {
        self.record(CallerCall::EnsureConnected);
        if self.is_connected() {
            Ok(())
        } else {
            // Same as production: this never waits, so a test scripts
            // arrival via `set_connected(true)` before the call, and a
            // caller that is not there resolves through the disconnect
            // policy.
            self.disconnected_outcome()
        }
    }

    fn handshake(&self) -> Arc<LiveRequest> {
        self.inner.lock().expect("fake caller poisoned").handshake.clone()
    }

    async fn send_chunk(
        &self,
        head: Option<ResponseHead>,
        chunk: OutboundChunk,
    ) -> Result<(), CallerError> {
        let mut g = self.inner.lock().expect("fake caller poisoned");
        g.calls.push(CallerCall::SendChunk { head: head.clone(), chunk });
        if !self.is_connected() {
            return self.disconnected_outcome();
        }
        Self::commit_head(&mut g, &head)
    }

    async fn terminate(
        &self,
        head: Option<ResponseHead>,
        final_chunk: Option<OutboundChunk>,
        close: Option<CloseReason>,
    ) -> Result<(), CallerError> {
        let mut g = self.inner.lock().expect("fake caller poisoned");
        g.calls.push(CallerCall::Terminate { head: head.clone(), final_chunk, close });
        try_terminate(g.terminated)?;
        Self::commit_head(&mut g, &head)?;
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
        // Production `receive` is unbounded (a node may wait hours) and has
        // no deadline outcome, so the fake must not invent one.
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
        // Exhausted script = disconnect (see `receive`), never a deadline.
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
        let body = g
            .http_body
            .clone()
            .ok_or_else(|| CallerError::Transport("no http body scripted".into()))?;
        Ok(Arc::new(HttpRequestParts { request: (*g.handshake).clone(), body }))
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

    /// The credentials a request carries are cleaned from everything its
    /// record holds, the token alone included, and nothing else is.
    #[test]
    fn a_requests_credentials_are_cleaned_from_its_record() {
        let request = LiveRequest {
            headers: vec![
                ("Authorization".into(), "Bearer tok-12345678".into()),
                ("X-Api-Key".into(), "key-9".into()),
                ("Accept".into(), "application/json".into()),
            ],
            ..Default::default()
        };
        let redaction = request.credentials(&["x-api-key".to_string()]);
        let mut record = serde_json::json!({
            "headers": [["Authorization", "Bearer tok-12345678"], ["X-Api-Key", "key-9"], ["Accept", "application/json"]],
            "passed_on": "token=tok-12345678",
            "short_inside": "key-9 in a sentence",
            "short_alone": "key-9",
            "count": 3,
        });
        redaction.scrub(&mut record);
        assert_eq!(record["headers"][0][1], REDACTED);
        assert_eq!(record["headers"][1][1], REDACTED);
        assert_eq!(record["headers"][2][1], "application/json");
        assert_eq!(record["passed_on"], format!("token={REDACTED}"));
        // A value shorter than SCRUBBED_INSIDE would match ordinary text,
        // so only a string that is exactly it is replaced.
        assert_eq!(record["short_inside"], "key-9 in a sentence");
        assert_eq!(record["short_alone"], REDACTED);
        assert_eq!(record["count"], 3);
        assert!(LiveRequest::default().credentials(&[]).is_empty());
    }

    #[test]
    fn in_band_error_speaks_the_body_framing() {
        let head = |ct: &str| ResponseHead::new(200).with_header("Content-Type", ct);
        let line = head("application/x-ndjson; charset=utf-8").in_band_error("the model \"failed\"");
        let text = String::from_utf8(line).unwrap();
        assert!(text.starts_with('\n') && text.ends_with('\n'), "{text:?}");
        let parsed: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(parsed, serde_json::json!({ "error": "the model \"failed\"" }));
        for ct in ["application/ndjson", "application/jsonl", "APPLICATION/X-JSONLINES"] {
            let text = String::from_utf8(head(ct).in_band_error("boom")).unwrap();
            assert_eq!(text, "\n{\"error\":\"boom\"}\n", "{ct}");
        }
        assert_eq!(
            String::from_utf8(head("text/event-stream").in_band_error("a\nb")).unwrap(),
            "\nevent: error\ndata: a\ndata: b\n\n",
        );
        assert_eq!(head("text/plain; charset=utf-8").in_band_error("boom"), b"\n[error] boom");
        assert_eq!(ResponseHead::new(200).in_band_error("boom"), b"\n[error] boom", "no content type is text");
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
    fn terminate_once_then_locks_out() {
        assert!(try_terminate(false).is_ok(), "first terminal wins");
        let err = try_terminate(true).expect_err("second terminal rejected");
        assert!(matches!(err, CallerError::AlreadyTerminated));
    }

    #[test]
    fn runtime_config_projects_from_config() {
        let cfg = LiveConnectionConfig {
            path: "chat".into(),
            methods: Vec::new(),
            auth: crate::signal::PublicEntryAuth::None,
            outlives_caller: true,
            heartbeat_interval_secs: 25,
            caller_silence_secs: 45,
            max_inbound_bytes: 4096,
            max_session_secs: 600,
            data_type: DataType::Text,
            backpressure: Backpressure::DropNewest,
            error_mode: ErrorMode::DropChunk,
            journal_mode: crate::signal::JournalMode::Journaled,
            journal_window_secs: None,
            window: None,
        };
        let rc = CallerRuntimeConfig::from_config(&cfg, Protocol::Websocket);
        assert_eq!(rc.protocol, Protocol::Websocket);
        assert!(rc.outlives_caller);
        assert_eq!(rc.data_type, DataType::Text);
        assert_eq!(rc.backpressure, Backpressure::DropNewest);
        assert_eq!(rc.error_mode, ErrorMode::DropChunk);
        assert_eq!(rc.max_inbound_bytes, 4096);
        assert_eq!(rc.max_session_secs, 600);
        assert_eq!(
            rc.caller_silence_secs, 45,
            "the trigger's own silence bound reaches the connection layer"
        );
    }

    fn http_cfg() -> CallerRuntimeConfig {
        CallerRuntimeConfig {
            protocol: Protocol::Http,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: ErrorMode::Surface,
            max_inbound_bytes: 1024,
            caller_silence_secs: crate::signal::DEFAULT_CALLER_SILENCE_SECS,
            max_session_secs: 0,
            // Caller-tied: a gone caller cancels (exercised below).
            outlives_caller: false,
            inbound_window: DEFAULT_INBOUND_WINDOW,
            journal: crate::stream_journal::JournalPolicy::default(),
        }
    }

    fn ws_cfg() -> CallerRuntimeConfig {
        CallerRuntimeConfig { protocol: Protocol::Websocket, ..http_cfg() }
    }

    /// What `outlivesCaller` decides, which is the whole lifetime axis.
    ///
    /// Off (the default): the run is the caller's, so the caller going
    /// away cancels it. On: the run is its own, and a caller going away
    /// is a no-op, so whatever it was doing carries on. Nothing else is
    /// refused for it: what the run does after answering is the author's
    /// to bound, and the language does not guess which loops are
    /// legitimate.
    #[test]
    fn who_owns_the_run_decides_what_a_disconnect_does() {
        assert_eq!(
            resolve_disconnect(false),
            DisconnectAction::CancelExecution,
            "a tied run ends with its caller"
        );
        assert_eq!(
            resolve_disconnect(true),
            DisconnectAction::ContinueIntoVoid,
            "a run that outlives its caller carries on, writing into the void"
        );
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
                CallerCall::SendChunk {
                    head: None,
                    chunk: OutboundChunk::Json(serde_json::json!("a")),
                },
                CallerCall::Terminate {
                    head: None,
                    final_chunk: Some(OutboundChunk::Json(serde_json::json!("done"))),
                    close: None,
                },
                CallerCall::Terminate { head: None, final_chunk: None, close: None },
            ]
        );
        assert_eq!(
            fake.chunks(),
            vec![
                OutboundChunk::Json(serde_json::json!("a")),
                OutboundChunk::Json(serde_json::json!("done"))
            ]
        );
    }

    /// The head rides the FIRST item only: an explicit head on the first
    /// write is recorded, a head after the wire started is refused.
    #[tokio::test]
    async fn a_head_is_honored_first_and_refused_after() {
        let fake = FakeCallerConnection::connected(http_cfg());
        let CallerHandle::Http(http) = CallerHandle::from_connection(fake.clone()) else {
            unreachable!()
        };
        let head = ResponseHead::new(201).with_header("x-run", "abc").with_keepalive("\n");
        http.write_with(head.clone(), OutboundChunk::Text("first".into())).await.unwrap();
        let err = http
            .respond_with(ResponseHead::new(500), OutboundChunk::Text("late".into()))
            .await
            .expect_err("a head after the first item is refused");
        assert!(err.to_string().contains("head already sent"), "got: {err}");
        // The log keeps every call, refused ones included: the first
        // head is the one that went out.
        assert_eq!(fake.heads()[0], head);
        // A terminal WITHOUT a head still lands after the refusal (the
        // refused call was not the terminal: nothing was committed).
        http.close().await.expect("close still works");
    }

    /// The handshake reaches both protocols through the shared accessor,
    /// and `request_parts` pairs it with the scripted body.
    #[tokio::test]
    async fn the_handshake_is_shared_and_the_http_body_rides_beside_it() {
        let fake = FakeCallerConnection::connected(http_cfg());
        let mut req = LiveRequest { method: "POST".into(), path: "chat/room7".into(), ..Default::default() };
        req.params.insert("room".into(), "room7".into());
        req.headers.push(("Content-Type".into(), "application/json".into()));
        fake.set_handshake(req.clone());
        fake.set_http_body(InboundMessage::Json(serde_json::json!({"text": "hi"})));
        let handle = CallerHandle::from_connection(fake.clone());
        assert_eq!(*handle.request(), req);
        assert_eq!(handle.request().header("content-type"), Some("application/json"));
        let CallerHandle::Http(http) = handle else { unreachable!() };
        let parts = http.request_parts().unwrap();
        assert_eq!(parts.request, req);
        assert_eq!(parts.body, InboundMessage::Json(serde_json::json!({"text": "hi"})));
    }

    #[test]
    fn a_head_gets_a_content_type_for_the_chunk_unless_it_names_one() {
        let json = ResponseHead::default().with_content_type_for(&OutboundChunk::Json(serde_json::json!(1)));
        assert_eq!(json.status, 200);
        assert_eq!(json.headers, vec![("content-type".to_string(), "application/json".to_string())]);
        let text = ResponseHead::new(201).with_content_type_for(&OutboundChunk::Text("x".into()));
        assert_eq!(text.headers[0].1, "text/plain; charset=utf-8");
        let bytes = ResponseHead::default().with_content_type_for(&OutboundChunk::Bytes(vec![1]));
        assert_eq!(bytes.headers[0].1, "application/octet-stream");
        let kept = ResponseHead::default()
            .with_header("Content-Type", "text/event-stream")
            .with_content_type_for(&OutboundChunk::Text("x".into()));
        assert_eq!(kept.headers.len(), 1, "an explicit content type is kept, case-insensitively");
    }

    #[test]
    fn wire_shapes_round_trip() {
        let mut req = LiveRequest { method: "GET".into(), path: "users/me".into(), ..Default::default() };
        req.query.insert("verbose".into(), "1".into());
        req.caller = Some(serde_json::json!({"key": 0}));
        req.base_url = Some("http://127.0.0.1:14111".into());
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["method"], "GET");
        assert_eq!(v["base_url"], "http://127.0.0.1:14111");
        assert!(v.get("params").is_none(), "empty maps are omitted on the wire");
        assert_eq!(serde_json::from_value::<LiveRequest>(v).unwrap(), req);

        let parts = HttpRequestParts { request: req, body: InboundMessage::Text("t".into()) };
        let back: HttpRequestParts = serde_json::from_value(serde_json::to_value(&parts).unwrap()).unwrap();
        assert_eq!(back, parts);

        let head = ResponseHead::new(404).with_header("x-a", "b").with_keepalive(": \n\n");
        let back: ResponseHead = serde_json::from_value(serde_json::to_value(&head).unwrap()).unwrap();
        assert_eq!(back, head);
        let bare: ResponseHead = serde_json::from_value(serde_json::json!({"status": 204})).unwrap();
        assert!(bare.headers.is_empty());
        assert!(bare.keepalive.is_none());
        let plain = serde_json::to_value(ResponseHead::new(200)).unwrap();
        assert!(plain.get("keepalive").is_none(), "no filler is omitted on the wire");

        let close = CloseReason { code: 1008, reason: "policy".into() };
        let back: CloseReason = serde_json::from_value(serde_json::to_value(&close).unwrap()).unwrap();
        assert_eq!(back, close);
        let bare: CloseReason = serde_json::from_value(serde_json::json!({"code": 1000})).unwrap();
        assert_eq!(bare, CloseReason::default());
    }

    #[tokio::test]
    async fn a_ws_close_carries_its_reason() {
        let fake = FakeCallerConnection::connected(ws_cfg());
        let CallerHandle::Websocket(ws) = CallerHandle::from_connection(fake.clone()) else {
            unreachable!()
        };
        ws.close_with(CloseReason { code: 4000, reason: "done".into() }).await.unwrap();
        assert_eq!(
            fake.close_reason(),
            Some(Some(CloseReason { code: 4000, reason: "done".into() }))
        );
    }

    #[tokio::test]
    async fn ensure_connected_is_a_status_read_not_a_wait() {
        let fake = FakeCallerConnection::disconnected(http_cfg());
        let handle = CallerHandle::from_connection(fake.clone());
        // No caller attached: resolves through the disconnect policy right
        // away (this config is tied, so cancelled) instead of waiting.
        let err = handle.ensure_connected().await.expect_err("no caller attached");
        assert!(matches!(err, crate::error::WeftError::Cancelled), "got: {err:?}");
        // Caller arrives: now a no-op success.
        fake.set_connected(true);
        handle.ensure_connected().await.expect("connected now");
        assert!(handle.is_connected());
    }

    #[tokio::test]
    async fn talk_into_void_when_survives_but_cancels_when_tied() {
        // Outlives its caller: a gone caller is a silent no-op.
        let keep = FakeCallerConnection::connected(CallerRuntimeConfig {
            outlives_caller: true,
            ..http_cfg()
        });
        keep.set_connected(false);
        let CallerHandle::Http(h) = CallerHandle::from_connection(keep) else { unreachable!() };
        h.write(OutboundChunk::Json(serde_json::json!("x"))).await
            .expect("survives drops into the void, no error");

        // Tied to its caller: a gone caller errors (maps to cancel).
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
        // The cursor is pinned at attach, so everything on the inbound log
        // is readable whenever it was pushed; queueing the reply here just
        // keeps the real request/reply order.
        fake.push_inbound(InboundMessage::Json(serde_json::json!("pong")));
        let reply = ws.request(OutboundChunk::Json(serde_json::json!("ping"))).await.unwrap();
        assert_eq!(reply, InboundMessage::Json(serde_json::json!("pong")));

        // http_request on a websocket connection is the wrong-protocol error.
        let err = fake.http_request().expect_err("http_request invalid on ws");
        assert!(matches!(err, CallerError::WrongProtocol { .. }));
    }
}
