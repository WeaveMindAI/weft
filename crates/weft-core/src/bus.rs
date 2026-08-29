//! In-process message bus: a channel between co-alive nodes in the same
//! execution (the same worker process).
//!
//! One node creates a bus (`ctx.create_bus(opts)`) and emits the marker on
//! an output port; downstream nodes resolve the marker via `ctx.bus`
//! and exchange messages in RAM. The pulse path never carries the bus
//! traffic itself (only the marker rides the pulse).
//!
//! ## The bus is an in-RAM log
//!
//! A bus is an append-only sequence of `BusEntry` records (`Joined`,
//! `Left`, `Message`, `Closed`), each carrying a monotonic offset. Two
//! pieces:
//!
//! - **`BusInner.log`**: the in-RAM entry deque behind a `Mutex`,
//!   plus a `Notify` that wakes waiting cursors when an entry lands.
//! - **`BusCursor`**: a `(Weak<BusInner>, next_offset, filter)`. Its
//!   `next()` future returns the next matching entry, or `None` when
//!   the bus is closed and the cursor is at the tail.
//!
//! Cursors are independent. Each consumer holds its own. Reading does
//! not consume; the same offset can be read by N cursors. Cursors only
//! ever read RAM: entries trimmed out of the in-RAM window are not
//! reachable by a cursor, and a cursor positioned inside a trimmed span
//! silently resumes forward at the next retained entry (only `Message`
//! entries are ever trimmed; membership entries are always retained, so
//! nothing a consumer needs can hide in a gap).
//!
//! A bus lives exactly as long as its worker process. It is NOT
//! rebuilt on worker restart: nothing rehydrates a bus from the
//! journal, and a marker resolved after a restart fails with
//! `BusLookupError::UnknownBus`. A live bus and a durable suspension
//! (`ctx.await_signal`) DO coexist: a bus-held worker resolves the
//! suspension in process (the driver's idle loop polls the journal for
//! the `SuspensionResolved` row while a bus keeps the worker alive). The
//! one real restriction is unrelated to the bus: a node may not
//! `await_signal` AFTER it has emitted on an output port (replay would
//! re-emit), enforced in the engine, not here.
//!
//! ## Windowed journaling (the inspector trail)
//!
//! Journaling exists for the INSPECTOR, not for resume. Every append
//! signals a per-execution `journal_pump_notify`; the engine's
//! bus-journal-pump task drains every live bus's unjournaled tail. The
//! pump buffers each bus's `Message` entries and flushes ONE
//! `BusWindow` journal row per bus per `journal_window` (a journaled
//! bus's row carries every message in the window; an ephemeral bus's
//! row carries only the per-sender/kind totals). Membership entries
//! (`Joined` / `Left` / `Closed`) are journaled as individual rows, and
//! any open window is flushed FIRST so the row stream never reorders
//! against the bus log. The pump acknowledges `journaled_through` to
//! the bus only AFTER a write succeeds, and owns the per-bus
//! `journaled_through` cursor.
//!
//! ## `BusOptions` (all five knobs)
//!
//! - **`ephemeral`**: picks the payload-retention mode (see below).
//! - **`window`**: the in-RAM bound, in `Message` entries, for BOTH
//!   modes (default 64).
//! - **`payload`**: what messages carry, `Json` or `Bytes`, frozen at
//!   creation.
//! - **`journal_window`**: the journal aggregation window (default 1s):
//!   one `BusWindow` row per bus per window.
//! - **`meta`**: creator-declared stream metadata readable off the
//!   handle by every consumer.
//!
//! The two payload-retention modes:
//!
//! - **Journaled** (default): every send writes the full payload into
//!   the log entry and it reaches the journal via the window rows.
//!   The in-RAM log only trims `Message` entries that are BOTH past
//!   the window AND already shipped to the journal, so durability is
//!   never traded for the RAM bound. Oversized payloads ARE allowed
//!   but a module-level warn threshold
//!   (`JOURNALED_PAYLOAD_WARN_BYTES`) logs loud at send time so the
//!   author sees the cost.
//! - **Ephemeral**: send stores the payload in an `EphemeralStore`
//!   sliding window keyed by offset; the log entry carries
//!   `payload: None` plus `payload_byte_size`, and the in-RAM log is
//!   TRUNCATED in lockstep with the window (entries below the oldest
//!   resident payload are dropped), so a long-running stream holds
//!   bounded RAM. A slow consumer's cursor silently resumes at the
//!   oldest retained entry; there is no backpressure (the camera never
//!   stalls because a downstream lags). Membership history survives
//!   truncation in a dedicated set (`ever_joined`), so `wait_for`
//!   semantics are unaffected.
//!
//! The marker JSON matches the stored-file markers' shape:
//! `{"__weft_bus__": {"id": "<uuid>", "mode": "journaled" | "ephemeral"}}`.
//! `id` and `mode` are the only fields the wire surfaces; everything
//! else (payload kind, windows, metadata) is read off the live handle
//! a consumer resolves from the marker.
//!
//! ## Identity (registration)
//!
//! A participant claims an identity by registering under a name it picks
//! (`bus.register("llm")`). Names are unique per bus (a second live
//! `register("llm")` errors so the caller can recover under a different
//! name). The registered name is the sender stamp on every `send`, and
//! is what peers wait on via `wait_for("llm")`. Registration is the
//! explicit "I am here and ready" act: a node that needs a long warmup
//! (download a model, open a socket) holds the bus the whole time and
//! only registers once it is truly ready, so peers waiting on it do not
//! release early.
//!
//! ## Waiting
//!
//! `wait_for(name)` parks until either `name` is live (Ok) or the bus
//! is closed (Err `Closed`). Membership is read from the truncation-
//! immune `ever_joined` set (maintained under the log lock), NOT by
//! scanning the log: a guest registering AFTER the host still sees
//! "host has joined" even on an ephemeral bus whose early entries were
//! evicted. See `wait_for_joins` for the precise mechanism.
//!
//! The bus does NOT try to decide for itself whether a name will ever
//! appear. That question depends on dispatch state, terminations, and
//! topology, all of which live in the engine. The engine watches the
//! execution and, when it concludes a wait can never be satisfied,
//! closes the bus. Every waiting cursor wakes; `next()` returns `None`.
//!
//! ## "wait" vs "park" vocabulary
//!
//! The word "park" in this codebase already names `ctx.await_signal`:
//! the worker pod is swapped, a fresh worker resumes from the journal,
//! the body re-flows through. That's workflow-level suspension. The bus
//! has nothing to do with that. A cursor's `next().await` is plain
//! in-process tokio await; the worker stays alive; no swap; no journal
//! write.
//!
//! The only reason the engine needs to know about the bus's await at
//! all is the stuck-detector: it tracks, per NODE EXECUTION, whether
//! that node is parked inside a bus wait with nothing to read, so when
//! every participant node is parked it can declare deadlock and close
//! every bus. Hence `WaitLiveness` and the `wait_on` protocol (which
//! pairs `enter_wait`/`exit_wait` internally); those live in
//! `crate::liveness`, which the bus plugs into as one `WaitSource`
//! among several. Two concepts, two names, no overlap.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::liveness::{no_liveness, wait_on, FiringLocation, WaitLiveness, WaitSource};

// ----- Public types ----------------------------------------------------

/// Bus payload-retention mode. Encoded as a string in the marker
/// (`"journaled"` or `"ephemeral"`) so consumers can read it without
/// touching the bus inner; centralised here so the two wire spellings
/// have a single source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusMode {
    Journaled,
    Ephemeral,
}

impl BusMode {
    pub fn as_wire_str(self) -> &'static str {
        match self {
            BusMode::Journaled => "journaled",
            BusMode::Ephemeral => "ephemeral",
        }
    }

    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "journaled" => Some(BusMode::Journaled),
            "ephemeral" => Some(BusMode::Ephemeral),
            _ => None,
        }
    }
}

/// What a bus's messages carry, declared at creation and frozen: JSON
/// values (chat-shaped traffic) or raw bytes (media frames; no base64
/// wrapping between nodes). A consumer reads the kind off the handle
/// (or the marker) and knows how to speak before the first message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BusPayloadKind {
    #[default]
    Json,
    Bytes,
}

impl BusPayloadKind {
    pub fn as_wire_str(self) -> &'static str {
        match self {
            BusPayloadKind::Json => "json",
            BusPayloadKind::Bytes => "bytes",
        }
    }

    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "json" => Some(BusPayloadKind::Json),
            "bytes" => Some(BusPayloadKind::Bytes),
            _ => None,
        }
    }
}

/// One message's payload, in its declared kind: a JSON value, or raw
/// bytes (media frames; no base64 wrapping inside the process). The
/// ONE wire payload vocabulary for every journaled exchange (bus
/// window rows, the live caller's `Caller*` events, SSE). On the wire
/// a byte payload serializes as `{"kind": "bytes", "data": "<base64>"}`
/// via the [`WirePayloadRepr`] mirror below (`bytes::Bytes` itself has
/// no serde).
// SYNC: WirePayload <-> packages/weft-graph/src/protocol.ts WirePayload
#[derive(Debug, Clone, PartialEq)]
pub enum WirePayload {
    Json(Value),
    Bytes(bytes::Bytes),
}

/// The serde mirror of [`WirePayload`]: identical shape with the bytes
/// carried as base64 text.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
enum WirePayloadRepr {
    Json(Value),
    Bytes(String),
}

impl Serialize for WirePayload {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use base64::Engine as _;
        let wire = match self {
            WirePayload::Json(v) => WirePayloadRepr::Json(v.clone()),
            WirePayload::Bytes(b) => {
                WirePayloadRepr::Bytes(base64::engine::general_purpose::STANDARD.encode(b))
            }
        };
        wire.serialize(s)
    }
}

impl<'de> Deserialize<'de> for WirePayload {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use base64::Engine as _;
        Ok(match WirePayloadRepr::deserialize(d)? {
            WirePayloadRepr::Json(v) => WirePayload::Json(v),
            WirePayloadRepr::Bytes(b64) => WirePayload::Bytes(
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map(bytes::Bytes::from)
                    .map_err(serde::de::Error::custom)?,
            ),
        })
    }
}

impl WirePayload {
    /// The payload's JSON value; loud `None` for a byte payload (the
    /// consumer asked with the wrong shape).
    pub fn as_json(&self) -> Option<&Value> {
        match self {
            WirePayload::Json(v) => Some(v),
            WirePayload::Bytes(_) => None,
        }
    }

    /// The payload's JSON value by move; `None` for a byte payload.
    pub fn into_json(self) -> Option<Value> {
        match self {
            WirePayload::Json(v) => Some(v),
            WirePayload::Bytes(_) => None,
        }
    }

    /// The payload's raw bytes by move; `None` for a JSON payload.
    pub fn into_bytes(self) -> Option<bytes::Bytes> {
        match self {
            WirePayload::Bytes(b) => Some(b),
            WirePayload::Json(_) => None,
        }
    }

    /// The payload's raw bytes; `None` for a JSON payload.
    pub fn as_bytes(&self) -> Option<&bytes::Bytes> {
        match self {
            WirePayload::Bytes(b) => Some(b),
            WirePayload::Json(_) => None,
        }
    }

    pub fn kind(&self) -> BusPayloadKind {
        match self {
            WirePayload::Json(_) => BusPayloadKind::Json,
            WirePayload::Bytes(_) => BusPayloadKind::Bytes,
        }
    }

    /// The payload's serialized byte size: the inspector metadata every
    /// message entry carries whatever its kind. A JSON value always
    /// serializes, so this cannot fail.
    pub fn byte_size(&self) -> u64 {
        match self {
            WirePayload::Json(v) => {
                serde_json::to_vec(v).expect("a serde_json::Value always serializes").len() as u64
            }
            WirePayload::Bytes(b) => b.len() as u64,
        }
    }
}

/// One observable record on a bus, in append-only order. The cursor
/// returns these directly to consumers; the engine's journal pump
/// projects them to `ExecEvent::Bus*` rows for replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusEntry {
    /// Monotonic per-bus position. The Nth append has offset N (0-based).
    /// Cursors use this as their resume cursor; the journal pump uses
    /// it to track "journaled through offset X" per bus.
    pub offset: u64,
    /// Unix seconds at append time. Diagnostic only; the inspector
    /// reads entries in dispatcher stream order, which preserves the
    /// engine's per-bus append order (and thus per-bus offset order),
    /// so `at_unix` is not part of the ordering key.
    pub at_unix: u64,
    pub kind: BusEntryKind,
}

/// One of the four shapes a bus entry takes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BusEntryKind {
    /// A handle registered `name`.
    Joined { name: String },
    /// A registered handle dropped, or the bus closed for it.
    Left { name: String },
    /// A successful `send` landed. `from` is the sender's registered
    /// name (the bus stamps it). For journaled buses, `payload` is
    /// `Some(value)`; for ephemeral buses, `payload` is `None` and the
    /// actual bytes live in the bus's `EphemeralStore` keyed by offset.
    /// `payload_byte_size` is ALWAYS populated: it feeds the window
    /// rows' totals (the only thing an ephemeral bus journals about a
    /// message), and is useful debugging info either way.
    Message {
        from: String,
        msg_kind: String,
        payload: Option<WirePayload>,
        payload_byte_size: u64,
    },
    /// Appended once at `close()`. Cursors never surface it as a
    /// regular `Some(entry)`; `next()` returns `None` when it reaches
    /// this marker. Journaled so the inspector renders "* the bus
    /// closed here" instead of the panel just ending.
    Closed,
}

/// One message inside a journal window row: the full detail a
/// journaled bus persists per message (boundaries, senders, payloads
/// all kept; only the ROW granularity is windowed).
// SYNC: WindowedBusMessage <-> packages/weft-graph/src/protocol.ts BusInspectorEvent 'message', extension-vscode/src/execFollower.ts DispatcherEvent 'bus_window' messages
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowedBusMessage {
    pub offset: u64,
    pub from: String,
    pub msg_kind: String,
    pub payload: WirePayload,
    pub payload_byte_size: u64,
    pub at_unix: u64,
}

/// One `(sender, kind)` rollup line of a journal window row: how many
/// messages and how many bytes passed. The whole story an ephemeral
/// bus journals; the summary line a journaled one carries besides its
/// messages.
// SYNC: BusWindowTotal <-> packages/weft-graph/src/protocol.ts BusInspectorEvent 'window' totals, extension-vscode/src/execFollower.ts DispatcherEvent 'bus_window' totals
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BusWindowTotal {
    pub from: String,
    pub msg_kind: String,
    pub count: u64,
    pub bytes: u64,
}

/// A window's aggregate: what one journal row carries for one bus.
#[derive(Debug, Clone, PartialEq)]
pub struct BusWindowAggregate {
    pub first_offset: u64,
    pub last_offset: u64,
    /// Per-message detail; empty when `include_payloads` was false
    /// (an ephemeral bus journals metadata only).
    pub messages: Vec<WindowedBusMessage>,
    pub totals: Vec<BusWindowTotal>,
    /// Append time (unix seconds) of the LAST entry in the window: the
    /// truthful stamp for the journal row (the row describes the bus's
    /// own appends, not the moment the pump got around to flushing).
    pub last_at_unix: u64,
}

/// Fold one window's MESSAGE entries into the aggregate a journal row
/// carries. Pure; the pump owns the windowing clock, this owns the
/// shape. Non-message entries are the caller's bug (membership rows
/// are journaled individually, never windowed). Returns `None` on an
/// empty window (nothing to write).
pub fn aggregate_window(entries: &[BusEntry], include_payloads: bool) -> Option<BusWindowAggregate> {
    let mut messages = Vec::new();
    let mut totals: Vec<BusWindowTotal> = Vec::new();
    let mut first_offset: Option<u64> = None;
    let mut last_offset = 0;
    let mut last_at_unix = 0;
    for entry in entries {
        let BusEntryKind::Message { from, msg_kind, payload, payload_byte_size } = &entry.kind
        else {
            unreachable!("only Message entries are windowed");
        };
        first_offset.get_or_insert(entry.offset);
        last_offset = entry.offset;
        last_at_unix = entry.at_unix;
        match totals.iter_mut().find(|t| &t.from == from && &t.msg_kind == msg_kind) {
            Some(t) => {
                t.count += 1;
                t.bytes += payload_byte_size;
            }
            None => totals.push(BusWindowTotal {
                from: from.clone(),
                msg_kind: msg_kind.clone(),
                count: 1,
                bytes: *payload_byte_size,
            }),
        }
        if include_payloads {
            messages.push(WindowedBusMessage {
                offset: entry.offset,
                from: from.clone(),
                msg_kind: msg_kind.clone(),
                payload: payload
                    .clone()
                    .expect("a journaled bus's retained Message always carries its payload"),
                payload_byte_size: *payload_byte_size,
                at_unix: entry.at_unix,
            });
        }
    }
    Some(BusWindowAggregate {
        first_offset: first_offset?,
        last_offset,
        messages,
        totals,
        last_at_unix,
    })
}

/// Configuration the producer passes to `ctx.create_bus(opts)`.
#[derive(Debug, Clone, Default)]
pub struct BusOptions {
    /// `true` switches the bus to ephemeral mode (metadata-only in the
    /// journal, slow consumers silently skip to the oldest retained
    /// entry). `false` (default)
    /// is the journaled mode (full payload persisted to the journal/DB for
    /// durability). NOTE: in EITHER mode the in-RAM log is a bounded
    /// window (cursors only ever read RAM, never the DB); the mode only
    /// decides whether the payload is also persisted for replay-after-
    /// worker-death, not whether RAM is bounded.
    pub ephemeral: bool,
    /// Per-bus in-RAM window size. `None` falls back to
    /// `DEFAULT_BUS_WINDOW`. Bounds RAM in BOTH modes: a journaled bus
    /// trims log entries that are already persisted to the DB once the
    /// window is exceeded (durability unaffected, the DB keeps them);
    /// an ephemeral bus trims by this window with no DB backstop. Larger
    /// windows let slower consumers reach further back in RAM at the cost
    /// of memory.
    pub window: Option<usize>,
    /// What the messages carry: JSON values (default) or raw bytes
    /// (media frames). Frozen at creation; a `send` of the other shape
    /// is refused loudly. Consumers read it off the handle/marker.
    pub payload: BusPayloadKind,
    /// The journal aggregation window: the pump ships ONE journal row
    /// per bus per window (the window's messages for a journaled bus,
    /// the rollup for an ephemeral one) instead of a row per message.
    /// `None` falls back to [`DEFAULT_JOURNAL_WINDOW`] (1s). A quiet
    /// bus degenerates to per-message rows (a window holds one
    /// message); a fast stream collapses to one row per window. Only
    /// the JOURNAL granularity: what travels the bus is untouched.
    pub journal_window: Option<std::time::Duration>,
    /// Creator-declared stream metadata, frozen at creation and
    /// readable by every consumer (off the handle or the marker):
    /// what a per-message header would otherwise repeat (an audio
    /// stream's sample rate and encoding, a video stream's dimensions).
    /// Keep it small; it rides the marker. `Null` = none.
    pub meta: Value,
}

/// Default journal aggregation window. One second keeps a chat bus
/// visually per-message while collapsing frame-rate streams ~50x.
pub const DEFAULT_JOURNAL_WINDOW: std::time::Duration = std::time::Duration::from_secs(1);

/// Default in-RAM window for a bus (both modes). 64 entries fits the
/// common consumer-lags-a-few-messages case without growing RAM
/// unboundedly on a long stream. Override via `BusOptions::window`.
pub const DEFAULT_BUS_WINDOW: usize = 64;

/// Default warn threshold for journaled payload size. Sends above this
/// log a `warn!` so the author sees the cost; the send still proceeds.
/// 1MB is generous for chat-shaped payloads, loud for image / video.
pub const JOURNALED_PAYLOAD_WARN_BYTES: u64 = 1_048_576;

/// Why a `send` did not land. Returned (not swallowed) so a dropped
/// message is a value the caller must handle, never a silent no-op.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SendError {
    /// The bus was explicitly closed (`close`). No further sends land.
    #[error("bus is closed")]
    Closed,
    /// This handle has not registered a name yet, so it has no identity
    /// to stamp the message with. Call `register` first.
    #[error("cannot send before registering a name on the bus")]
    NotRegistered,
    /// The payload's shape does not match the bus's declared payload
    /// kind (a JSON send on a bytes bus, or the reverse). The kind is
    /// frozen at creation; read it off the handle before sending.
    #[error("this bus carries {expected} payloads, not {sent}")]
    WrongPayload { expected: &'static str, sent: &'static str },
    /// The journal pump previously failed to write this JOURNALED
    /// bus's tail. The send is REJECTED BEFORE appending anything: in
    /// journaled mode the journal trail is the durability story, and
    /// silently accepting sends that may never reach it would corrupt
    /// the inspector's replay. The flag clears on the pump's next
    /// successful batch (the pump keeps the failed tail buffered and
    /// retries on its next pass), after which sends proceed again.
    /// EPHEMERAL buses are never rejected on degradation: their
    /// journal trail is diagnostic metadata, not the data plane, and
    /// stalling live frames because Postgres burped would invert the
    /// mode's no-backpressure design.
    #[error("bus journal write degraded: {0}")]
    JournalDegraded(String),
}

/// Why a `register` call failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegisterError {
    /// Another live participant already holds this name.
    #[error("name '{0}' is already registered on the bus")]
    NameTaken(String),
    /// This handle already registered under a name; a handle registers
    /// once.
    #[error("this handle already registered as '{0}'")]
    AlreadyRegistered(String),
    /// The bus is already closed. The caller cannot join a dead channel.
    #[error("bus is closed; cannot register")]
    Closed,
}

/// Why a `wait_for` resolved without the awaited peer appearing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WaitError {
    /// The bus closed before the wait was satisfied.
    #[error("bus closed while waiting for peers")]
    Closed,
}

/// Why a typed cursor read (`next_json` / `next_bytes`) failed
/// (distinct from "closed", which is `None`). The raw `next()` cannot
/// fail: gaps are bridged silently and close is `None`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CursorError {
    /// The consumer asked for one payload shape but the bus carries the
    /// other (`next_json` on a bytes bus, or the reverse). Mirrors the
    /// send side's `SendError::WrongPayload`: the kind is frozen at
    /// creation; read it off the handle before reading.
    #[error("this bus carries {expected} payloads, not {requested}")]
    WrongPayload { expected: &'static str, requested: &'static str },
}

// ----- Wait liveness (shared with every in-process wait) --------------
//
// The wait-tracking vocabulary the bus reports through (FiringLocation,
// WaitId, WaitLiveness, WaitSource, wait_on) lives in
// `crate::liveness`: the generator's pulls and the engine's
// delivery-waits park through the exact same tracker, so the engine's
// stuck-check sees every in-process wait in one place. The bus is one
// WaitSource among several; its generation is the log's append
// generation.

/// The bus as a [`WaitSource`]: its state-change generation is the
/// log's append generation (every send / register / close / drop
/// funnels through `push_entry`, which bumps it under the log lock).
impl WaitSource for BusInner {
    fn gen_now(&self) -> u64 {
        self.append_gen_now()
    }
    fn settled_gen(&self) -> u64 {
        self.append_gen_settled()
    }
    fn wake_waiters(&self) {
        BusInner::wake_waiters(self)
    }
    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.log_notify.notified()
    }
}

/// In-RAM ring of recent ephemeral payloads, bounded by capacity.
/// Insertion at the back; eviction from the front when the bound is
/// hit. The current floor (smallest still-resident offset) is read
/// from `entries.front()` directly under the lock; there is no
/// separate atomic.
///
/// This is the load-bearing piece of the "ephemeral" mode: the
/// producer never blocks, RAM stays bounded (the bus log truncates in
/// lockstep with this window, see `BusHandle::send`), and slow
/// consumers silently resume at the oldest retained entry. There is no
/// backpressure: the camera does not stall because a downstream lags.
struct EphemeralStore {
    capacity: usize,
    entries: Mutex<VecDeque<(u64, WirePayload)>>,
}

impl EphemeralStore {
    fn new(capacity: usize) -> Result<Self, &'static str> {
        if capacity == 0 {
            return Err("window must be >= 1; 0 would evict every payload \
                        before any cursor could read it");
        }
        Ok(Self {
            capacity,
            entries: Mutex::new(VecDeque::new()),
        })
    }

    /// Insert a fresh (offset, payload). Caller holds the bus log lock,
    /// so offsets are appended in order. Evicts the front when capacity
    /// is reached.
    fn insert(&self, offset: u64, payload: WirePayload) {
        let mut q = self.entries.lock().expect("ephemeral store poisoned");
        q.push_back((offset, payload));
        while q.len() > self.capacity {
            q.pop_front();
        }
    }

    /// The smallest still-resident offset, if any payload is resident.
    /// Caller holds the bus log lock (same discipline as `insert`).
    fn floor(&self) -> Option<u64> {
        self.entries
            .lock()
            .expect("ephemeral store poisoned")
            .front()
            .map(|(o, _)| *o)
    }

    /// Lookup payload for an offset the caller already knows is inside
    /// the retained range (the log truncates in lockstep with this
    /// window under the same lock, so every retained Message entry has
    /// a resident payload). PANICS on a miss: `send` inserts the log
    /// entry AND the payload under one log lock, and truncation never
    /// retains an entry whose payload it evicted, so a miss is a bus
    /// invariant violation, not a recoverable state.
    fn get_resident(&self, offset: u64) -> WirePayload {
        let q = self.entries.lock().expect("ephemeral store poisoned");
        q.iter()
            .find(|(o, _)| *o == offset)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| {
                panic!(
                    "bus invariant: ephemeral entry at offset {offset} retained in the \
                     log but absent from the payload store; send/truncation must move \
                     in lockstep under the log lock"
                )
            })
    }
}

/// Shared bus state behind every handle. The log is the source of
/// truth for ordering and content; `present_names` is a denormalized
/// view of currently-registered identities (kept in sync with the log
/// under the same lock as appends).
///
/// `liveness` and `journal_pump_notify` are `Weak` on purpose: the
/// engine's `BusCoordinator` pins each bus via `BusRegistry::inner:
/// HashMap<Uuid, Arc<BusInner>>`, and an `Arc` back-reference from
/// the bus to the coordinator would form a cycle. `Weak` lets the
/// coordinator drop naturally; a wait/pump-notify observed after
/// coordinator drop is a no-op.
///
/// Public so the engine's per-execution bus-journal-pump task can
/// hold `Weak<BusInner>` and call the engine-facing `pub fn`s in this
/// file's impl block. Handle-level state, options, and the log
/// itself stay private to this module.
pub struct BusInner {
    /// Stable channel identity. The JSON marker embeds this id;
    /// `BusRegistry::lookup` resolves a marker back to `Arc<BusInner>`
    /// via this id.
    id: Uuid,
    /// Whether this bus runs in ephemeral mode. Frozen at creation.
    ephemeral: bool,
    /// The entry log, ordered by offset. ALL state-changes (Joined,
    /// Left, Message, Closed) go through here. Cursors read by
    /// offset; the journal pump drains the unjournaled tail.
    ///
    /// In BOTH modes the log can be NON-contiguous: only `Message`
    /// entries are ever trimmed (membership entries Joined / Left /
    /// Closed are always retained, load-bearing for cursors and
    /// bounded by participant churn, not by traffic). Ephemeral drops
    /// a Message once the sliding window evicts its payload; journaled
    /// drops a Message once it is BOTH past the window AND already
    /// shipped to the DB (`offset < journaled_through`), so durability
    /// is unaffected (the journal keeps it) but a live cursor cannot
    /// reach it (cursors only read RAM, never the DB). Cursors detect
    /// the offset gaps and bridge them silently, resuming at the next
    /// retained entry.
    log: Mutex<Vec<BusEntry>>,
    /// Every name that EVER registered, regardless of later `Left`
    /// entries. The single source of truth for membership waits
    /// (`wait_for*`). Maintained under the log lock.
    ever_joined: Mutex<HashSet<String>>,
    /// Highest offset the pump has successfully shipped to the
    /// journal, +1. Reads as `journaled_through`; the pump pulls
    /// `log[journaled_through..]` per drain. Atomic so the bus can
    /// read it without holding the log lock.
    journaled_through: AtomicU64,
    /// Set once when the bus is closed. Cursors return `None` after
    /// reaching the `Closed` entry; further sends error.
    closed: AtomicBool,
    /// Set once when the journal pump has failed to write this bus's
    /// tail. Cleared by the pump's next successful batch
    /// (`acknowledge_journaled_through`). The next `send` while this
    /// is set returns `SendError::JournalDegraded`.
    journal_degraded: AtomicBool,
    /// The last journal-pump error reason. Read by the next `send` to
    /// produce a non-empty `JournalDegraded(reason)`.
    degraded_reason: Mutex<String>,
    /// Wake on every state change (append, close, registration). One
    /// `Notify` drives every cursor wait; the cursor re-reads state on
    /// every wake. Cheap because `notify_waiters` stores no permits.
    log_notify: Notify,
    /// Denormalized: who's registered right now. Kept in sync with the
    /// log under the same `log` lock so a `register`/`drop` race
    /// against `close()` cannot produce a Joined without a matching
    /// log entry.
    present_names: Mutex<HashSet<String>>,
    /// Engine node-liveness hook. `Weak::new()` outside an engine-driven
    /// execution; set once at bus creation via
    /// `BusHandle::create_with_engine`. Fired at every cursor wait via
    /// the shared `wait_on` protocol and at every append via
    /// `push_entry`.
    liveness: Weak<dyn WaitLiveness>,
    /// Per-bus append generation: bumped on every `push_entry`, under
    /// the same log lock, AFTER the entry lands. The engine's liveness
    /// map holds a `Weak<BusInner>` per parked node and compares each
    /// parked node's observed generation against `append_gen_settled`
    /// (locked read; see `WaitLiveness::observed`). Never read on the
    /// send path beyond the one `fetch_add`; all comparison cost lives
    /// on the engine's stuck-check.
    append_gen: AtomicU64,
    /// Per-execution journal pump wake. The bus ping it after every
    /// append; the pump wakes, walks every live bus, drains tails.
    /// `Weak::new()` outside an engine-driven execution.
    journal_pump_notify: Weak<Notify>,
    /// Optional ephemeral payload store. `Some` iff `ephemeral`.
    ephemeral_store: Option<EphemeralStore>,
    /// What the messages carry (JSON vs raw bytes). Frozen at creation.
    payload_kind: BusPayloadKind,
    /// The journal aggregation window (see [`BusOptions::journal_window`]).
    journal_window: std::time::Duration,
    /// Creator-declared stream metadata (see [`BusOptions::meta`]).
    meta: Value,
    /// In-RAM retained-window size, in Message entries, for BOTH modes.
    /// Cursors only ever read RAM, so this bounds how far back any cursor
    /// can reach. Ephemeral trims by this with no DB backstop; journaled
    /// trims only Message entries already shipped to the DB
    /// (`offset < journaled_through`), so durability is unaffected and the
    /// trail past the window is recoverable from the journal, just not by a
    /// live cursor.
    window: usize,
}

impl BusInner {
    /// Lock the log, recovering from poisoning. The only user code
    /// that ever runs under this lock is a cursor's filter closure,
    /// and that section is read-only: a panicking filter leaves the
    /// log structurally intact, so recovering the data is sound.
    /// Without recovery, one panicking filter would poison the mutex,
    /// every other participant (including `BusHandle::drop`, which
    /// runs DURING the panicking task's unwind) would panic on the
    /// lock, and a panic-during-unwind aborts the whole worker
    /// process, killing every execution on the pod.
    fn lock_log(&self) -> std::sync::MutexGuard<'_, Vec<BusEntry>> {
        self.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The bus's current append generation, read WITHOUT the log lock.
    /// Suitable for the observe path only (`WaitLiveness::observed`): a
    /// value read here may lag an entry that already landed in the log
    /// (`push_entry` pushes first, bumps after), which is conservative
    /// for the stuck-check (the waiter reads as behind). Never use it
    /// for the close decision itself; that needs `append_gen_settled`.
    pub fn append_gen_now(&self) -> u64 {
        self.append_gen.load(Ordering::Acquire)
    }

    /// The bus's current append generation, read UNDER the log lock.
    /// `push_entry` holds the log lock through the bump, so a locked
    /// read of generation G proves the log contains nothing past G at
    /// that point. The engine's stuck-check scan uses this so a torn
    /// read (entry in the log, bump not yet visible) can never enable
    /// a close. One lock acquisition per bus per stuck-check; zero
    /// cost on the message path.
    pub fn append_gen_settled(&self) -> u64 {
        let _log = self.lock_log();
        self.append_gen.load(Ordering::Acquire)
    }

    /// Wake every cursor parked on this bus WITHOUT appending anything.
    /// Used by the engine when a node task resolves one of its concurrent
    /// waits (a `select!`/`join!` branch): its sibling waits' parked flags
    /// went stale (the task is provably running), so the engine clears
    /// them and pings each sibling's bus here to make its wait loop re-run
    /// `observed` -> re-check -> `parked` and restore a truthful flag.
    /// Spurious wakes are always safe (the wait loop re-checks state on
    /// every wake); this just forces a prompt re-evaluation.
    pub fn wake_waiters(&self) {
        self.log_notify.notify_waiters();
    }

    /// Append one entry to the log under the log lock, and wake the
    /// log_notify + the journal-pump notify. The single point of
    /// append: `close`, `register`, `send`, and `Drop` all funnel
    /// through this so the wake discipline (always log_notify, always
    /// pump-notify) can never drift between paths. Returns the offset
    /// the entry landed at. Offsets derive from the LAST entry's
    /// offset (not the length): ephemeral eviction removes interior
    /// entries, so length is not a position.
    fn push_entry(
        &self,
        log: &mut Vec<BusEntry>,
        kind: BusEntryKind,
    ) -> u64 {
        let offset = log.last().map(|e| e.offset + 1).unwrap_or(0);
        log.push(BusEntry {
            offset,
            at_unix: now_unix(),
            kind,
        });
        // Bump AFTER the entry lands, under the caller's log lock: a
        // waiter whose condition evaluation starts after observing
        // generation G is then guaranteed (lock serialization) to see
        // every entry appended at or before G.
        self.append_gen.fetch_add(1, Ordering::AcqRel);
        self.log_notify.notify_waiters();
        if let Some(p) = self.journal_pump_notify.upgrade() {
            p.notify_waiters();
        }
        // Ground-truth "a bus appended" signal for the engine's stuck-
        // check: an append while a peer is parked means the peer has new
        // input, so the loop must not declare deadlock. Same wake the
        // log_notify gives cursors, surfaced to the engine as a
        // generation bump it can compare across its idle window.
        if let Some(w) = self.liveness.upgrade() {
            w.on_source_event();
        }
        offset
    }

    /// Trim a JOURNALED bus's in-RAM log to the retained window. Caller
    /// holds the log lock. Drops only Message entries that are BOTH already
    /// shipped to the DB (`offset < journaled_through`) AND older than the
    /// most recent `window` Message entries. Membership entries are always
    /// kept. No-op until both the window AND the journaled prefix are
    /// exceeded, so the common case (short conversation, or pump momentarily
    /// behind) keeps everything.
    fn trim_journaled_window(&self, log: &mut Vec<BusEntry>) {
        let msg_count = log
            .iter()
            .filter(|e| matches!(e.kind, BusEntryKind::Message { .. }))
            .count();
        if msg_count <= self.window {
            return;
        }
        // The offset floor below which Message entries may be dropped: keep
        // the newest `window` messages. Walk messages newest->oldest,
        // counting `window`, and take the offset of the window's oldest kept
        // message as the floor.
        let mut kept = 0usize;
        let mut window_floor = 0u64;
        for e in log.iter().rev() {
            if matches!(e.kind, BusEntryKind::Message { .. }) {
                kept += 1;
                if kept == self.window {
                    window_floor = e.offset;
                    break;
                }
            }
        }
        let journaled_through = self.journaled_through.load(Ordering::Acquire);
        log.retain(|e| {
            // Always keep membership entries and anything in the window.
            if !matches!(e.kind, BusEntryKind::Message { .. }) || e.offset >= window_floor {
                return true;
            }
            // A Message below the window: drop ONLY if already journaled.
            // An un-shipped message stays so the pump can still drain it and
            // a fresh worker can replay it (durability before RAM bound).
            e.offset >= journaled_through
        });
    }

    /// Close this bus. Appends a `Closed` entry (idempotent), sets the
    /// closed flag, wakes every cursor waiting on the log and the
    /// engine's liveness hook. Public so the engine's `BusCoordinator` can
    /// fire stuck-detection close via a raw `Arc<BusInner>` it pins
    /// for the pump's lifetime.
    pub fn close(&self) {
        let mut log = self.lock_log();
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.push_entry(&mut log, BusEntryKind::Closed);
    }

    /// Read whether the journal pump previously failed for this bus.
    pub fn is_journal_degraded(&self) -> bool {
        self.journal_degraded.load(Ordering::Acquire)
    }

    /// Drain unjournaled tail. Returns every RETAINED log entry at or
    /// past the last journaled offset. The pump calls
    /// `acknowledge_journaled_through(through_offset)` after each
    /// write succeeds. Atomic snapshot under the log lock.
    ///
    /// On an ephemeral bus whose eviction outran the pump (the pump
    /// lagged more than a full window), the evicted-but-unjournaled
    /// messages are permanently lost to the inspector trail; `send`
    /// logged that loudly at eviction time. The drain simply resumes
    /// from the next retained entry (per-entry acks then advance
    /// `journaled_through` past the gap). On a JOURNALED bus this
    /// cannot lose anything: `trim_journaled_window` only ever drops
    /// entries strictly below `journaled_through` (already drained),
    /// so an un-shipped entry is always still resident here.
    pub fn drain_journal_tail(&self) -> Vec<BusEntry> {
        let log = self.lock_log();
        let from_offset = self.journaled_through.load(Ordering::Acquire);
        let from_idx = log.partition_point(|e| e.offset < from_offset);
        log[from_idx..].to_vec()
    }

    /// Mark `through_offset` as successfully journaled (the new
    /// `journaled_through` value, which is one past the highest
    /// journaled offset). Idempotent and monotone (never moves
    /// backwards). Also clears any `journal_degraded` flag: a fresh
    /// successful write supersedes the previous failure.
    pub fn acknowledge_journaled_through(&self, through_offset: u64) {
        // `fetch_max` makes the documented monotonicity true BY
        // CONSTRUCTION rather than relying on a single-pump-per-execution
        // topology assumption stated nowhere: a load-then-store max would
        // regress the cursor if two acks ever interleaved (5 then 3),
        // after which the pump re-drains and double-ships journal rows.
        self.journaled_through
            .fetch_max(through_offset, Ordering::AcqRel);
        if self.journal_degraded.swap(false, Ordering::AcqRel) {
            self.degraded_reason
                .lock()
                .expect("degraded_reason poisoned")
                .clear();
        }
    }

    /// Set the degraded flag. Called by the engine pump after a write
    /// failure. The next `send` returns `SendError::JournalDegraded`.
    pub fn mark_journal_degraded(&self, reason: impl Into<String>) {
        *self
            .degraded_reason
            .lock()
            .expect("degraded_reason poisoned") = reason.into();
        self.journal_degraded.store(true, Ordering::Release);
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn ephemeral(&self) -> bool {
        self.ephemeral
    }

    /// What the messages carry (JSON vs raw bytes).
    pub fn payload_kind(&self) -> BusPayloadKind {
        self.payload_kind
    }

    /// The journal aggregation window: how often the pump ships one
    /// aggregate row for this bus.
    pub fn journal_window(&self) -> std::time::Duration {
        self.journal_window
    }

    /// Creator-declared stream metadata (`Null` when none was set).
    pub fn meta(&self) -> &Value {
        &self.meta
    }

    /// The in-RAM retained window size (entries), for both modes.
    pub fn window(&self) -> usize {
        self.window
    }

    /// One past the highest offset ever appended. The pump compares
    /// this with `journaled_through` to know whether anything is left
    /// to drain. Snapshot under the log lock. Derived from the last
    /// entry's offset, not the length (ephemeral eviction removes
    /// interior entries).
    pub fn log_len(&self) -> u64 {
        let log = self.lock_log();
        log.last().map(|e| e.offset + 1).unwrap_or(0)
    }

    /// The earliest offset still resident in the in-RAM log (the floor a
    /// cursor can reach; anything below it was trimmed out of RAM and is not
    /// readable by a cursor, by design cursors never read the DB). `0` on an
    /// empty log.
    pub fn retained_floor(&self) -> u64 {
        let log = self.lock_log();
        log.first().map(|e| e.offset).unwrap_or(0)
    }

    /// The pump's `journaled_through` cursor (one past the highest
    /// journaled offset). Used by the engine's shutdown to wait
    /// until every Closed entry has been shipped.
    pub fn journaled_through(&self) -> u64 {
        self.journaled_through.load(Ordering::Acquire)
    }

    /// Whether `close()` has been called on this bus.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

// ----- BusHandle (the participant-facing handle) ----------------------

/// A participant's handle on a bus. Cheap to clone (shares the inner
/// state); each handle has its own registered identity (once it
/// registers). All consumer reads go through `cursor()`; there is
/// only ONE primitive for reading.
pub struct BusHandle {
    inner: Arc<BusInner>,
    /// `(name, joined_offset)` once `register` succeeds. The name is
    /// stamped on every `send`; the offset of this handle's own
    /// `Joined` entry anchors `cursor()` so the registration
    /// handshake is race-free (see `cursor`). `None` until
    /// registration.
    registration: Option<(String, u64)>,
    /// The node EXECUTION that minted this handle (`(node_id, frames)`),
    /// or `None` for a bus created outside an engine-driven node (unit
    /// tests, `BusHandle::create`). Used to key the node's bus liveness:
    /// every wait this handle enters, and the participant membership it
    /// registers, are attributed to this node execution. Forked handles
    /// (`new_handle`) inherit it, because a forked handle is still the
    /// SAME node grabbing a second grip on the bus.
    node: Option<FiringLocation>,
}

impl BusHandle {
    /// Fork a fresh handle on the same bus, with NO registered
    /// identity. A name is claimed by exactly one handle via
    /// `register`; this fork lets a second participant join the same
    /// bus under a different name (or stay anonymous for read-only
    /// observation). Each handle's cursors are independent; ask for
    /// one via [`Self::cursor`].
    ///
    /// Not `Clone` deliberately: `Clone`'s standard contract is "the
    /// result is interchangeable with the original", which would
    /// suggest the new handle could send under the original's name.
    /// It can't (the registered name does not carry across). Naming
    /// the method `new_handle` makes the intent explicit at the call
    /// site.
    pub fn new_handle(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            registration: None,
            node: self.node.clone(),
        }
    }
}

impl BusHandle {
    /// Create a fresh bus with default options and no engine hooks.
    /// Used in unit tests and code outside an engine-driven execution.
    pub fn create() -> Self {
        Self::create_with_options(BusOptions::default())
            .expect("default BusOptions cannot fail")
    }

    /// Create with explicit options, no engine hooks. Errors on an
    /// invalid `window`.
    pub fn create_with_options(opts: BusOptions) -> Result<Self, &'static str> {
        Self::create_with_engine(opts, no_liveness(), Weak::<Notify>::new(), None)
    }

    /// Create with options + engine hooks. The liveness hook fires
    /// whenever this node's cursor enters/leaves a wait and on every
    /// append; the pump notify fires after every append to signal the
    /// engine's bus-journal-pump task. `node` is the minting node
    /// execution (`None` outside an engine-driven node). Errors on an
    /// invalid `window` (must be >= 1 when `ephemeral=true`).
    pub fn create_with_engine(
        opts: BusOptions,
        liveness: Weak<dyn WaitLiveness>,
        journal_pump_notify: Weak<Notify>,
        node: Option<FiringLocation>,
    ) -> Result<Self, &'static str> {
        let window = opts.window.unwrap_or(DEFAULT_BUS_WINDOW);
        if window == 0 {
            return Err("bus window must be >= 1; 0 would evict every entry before any \
                        cursor could read it");
        }
        let ephemeral_store = if opts.ephemeral {
            Some(EphemeralStore::new(window)?)
        } else {
            None
        };
        let inner = Arc::new(BusInner {
            id: Uuid::new_v4(),
            ephemeral: opts.ephemeral,
            log: Mutex::new(Vec::new()),
            ever_joined: Mutex::new(HashSet::new()),
            journaled_through: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            journal_degraded: AtomicBool::new(false),
            degraded_reason: Mutex::new(String::new()),
            log_notify: Notify::new(),
            append_gen: AtomicU64::new(0),
            present_names: Mutex::new(HashSet::new()),
            liveness,
            journal_pump_notify,
            ephemeral_store,
            payload_kind: opts.payload,
            journal_window: opts.journal_window.unwrap_or(DEFAULT_JOURNAL_WINDOW),
            meta: opts.meta,
            window,
        });
        Ok(Self {
            inner,
            registration: None,
            node,
        })
    }

    /// Build a handle from an existing inner, attributed to the node
    /// execution `node` that is resolving the marker. Used by
    /// `BusRegistry::lookup` to hand a consumer a fresh handle on a bus
    /// the producer already created; the consumer's own node identity
    /// (not the producer's) keys its liveness.
    pub(crate) fn from_inner(inner: Arc<BusInner>, node: Option<FiringLocation>) -> Self {
        Self {
            inner,
            registration: None,
            node,
        }
    }

    /// Stable channel identity.
    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    /// Whether this bus runs in ephemeral mode.
    pub fn is_ephemeral(&self) -> bool {
        self.inner.ephemeral
    }

    /// What this bus's messages carry (JSON vs raw bytes).
    pub fn payload_kind(&self) -> BusPayloadKind {
        self.inner.payload_kind()
    }

    /// Creator-declared stream metadata (`Null` when none was set): the
    /// stream-level facts a consumer needs before the first message (an
    /// audio stream's sample rate, a video stream's dimensions).
    pub fn meta(&self) -> &Value {
        self.inner.meta()
    }

    /// Borrow the shared `Arc<BusInner>` so the engine can hold it
    /// alive across the pump's lifetime. Used by `BusCoordinator` so
    /// the bus is not freed before the pump has journaled the final
    /// `Closed` entry.
    pub fn inner_arc(&self) -> Arc<BusInner> {
        self.inner.clone()
    }

    /// Claim `name` as this handle's identity. Errors if the bus is
    /// closed (`Closed`), the name is already held (`NameTaken`), or
    /// this handle already registered (`AlreadyRegistered`).
    ///
    /// The append to the log AND the `present_names` insert AND the
    /// closed-flag re-check all happen under the SAME `log` lock. A
    /// `close()` in flight either lands before (this register sees
    /// `closed=true` and errors), or after (this register has finished
    /// inserting, the close appends the `Closed` entry next). No
    /// register-then-close race producing an orphan Joined.
    pub fn register(&mut self, name: impl Into<String>) -> Result<(), RegisterError> {
        if let Some((existing, _)) = &self.registration {
            return Err(RegisterError::AlreadyRegistered(existing.clone()));
        }
        let name = name.into();
        let mut log = self.inner.lock_log();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RegisterError::Closed);
        }
        {
            let mut present = self
                .inner
                .present_names
                .lock()
                .expect("present_names poisoned");
            if present.contains(&name) {
                return Err(RegisterError::NameTaken(name));
            }
            present.insert(name.clone());
        }
        self.inner
            .ever_joined
            .lock()
            .expect("ever_joined poisoned")
            .insert(name.clone());
        let joined_offset = self.inner.push_entry(
            &mut log,
            BusEntryKind::Joined { name: name.clone() },
        );
        drop(log);
        self.registration = Some((name, joined_offset));
        Ok(())
    }

    /// This handle's registered name, if it has registered.
    pub fn name(&self) -> Option<&str> {
        self.registration.as_ref().map(|(n, _)| n.as_str())
    }

    /// Send a message stamped with this handle's registered name.
    ///
    /// Failure modes (all loud, no silent drop):
    /// - `NotRegistered`: handle has no identity.
    /// - `Closed`: bus has been closed.
    /// - `JournalDegraded` (journaled buses only): the pump previously
    ///   failed to write this bus's tail. The send is rejected BEFORE
    ///   appending anything; see `SendError::JournalDegraded` for the
    ///   recovery contract. Ephemeral buses are never gated on the
    ///   journal (their trail is diagnostics, not the data plane).
    pub fn send(&self, kind: impl Into<String>, payload: Value) -> Result<(), SendError> {
        self.send_payload(kind, WirePayload::Json(payload))
    }

    /// Send raw bytes (a media frame) on a bytes-kind bus. The bytes
    /// travel as bytes; base64 exists only at the journal boundary.
    pub fn send_bytes(
        &self,
        kind: impl Into<String>,
        payload: impl Into<bytes::Bytes>,
    ) -> Result<(), SendError> {
        self.send_payload(kind, WirePayload::Bytes(payload.into()))
    }

    fn send_payload(&self, kind: impl Into<String>, payload: WirePayload) -> Result<(), SendError> {
        if payload.kind() != self.inner.payload_kind {
            return Err(SendError::WrongPayload {
                expected: self.inner.payload_kind.as_wire_str(),
                sent: payload.kind().as_wire_str(),
            });
        }
        let from = self
            .registration
            .as_ref()
            .map(|(n, _)| n.clone())
            .ok_or(SendError::NotRegistered)?;
        // Shared metadata derivation (same shape the live-caller events
        // use); infallible for both payload shapes.
        let payload_byte_size = payload.byte_size();
        let kind = kind.into();
        // Journaled mode keeps the payload in the log entry. Ephemeral
        // mode hides it from the log (the inspector renders the size +
        // hash prefix instead) and parks the bytes in the per-bus
        // sliding-window store for live cursors to pick up.
        let entry_payload = if self.inner.ephemeral { None } else { Some(payload.clone()) };
        let mut log = self.inner.lock_log();
        // Check Closed BEFORE JournalDegraded: a closed bus is the more
        // fundamental failure (no further sends will ever succeed),
        // and a caller retrying after the pump cleared the degraded
        // flag should not get a fresh error type for the same
        // impossible-to-satisfy send.
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed);
        }
        if !self.inner.ephemeral && self.inner.is_journal_degraded() {
            let reason = self
                .inner
                .degraded_reason
                .lock()
                .expect("degraded_reason poisoned")
                .clone();
            return Err(SendError::JournalDegraded(reason));
        }
        let offset = self.inner.push_entry(
            &mut log,
            BusEntryKind::Message {
                from,
                msg_kind: kind,
                payload: entry_payload,
                payload_byte_size,
            },
        );
        if let Some(store) = &self.inner.ephemeral_store {
            store.insert(offset, payload);
            // Drop the log entries of EVICTED messages eagerly (every
            // send, O(window + membership) scan) so a long-running
            // ephemeral stream holds bounded RAM. Membership entries
            // (Joined / Left / Closed) are always retained: they are
            // load-bearing for cursors and bounded by participant
            // churn, not traffic. Cursors bridge the resulting offset
            // gaps silently, resuming at the next retained entry.
            // Eager (not amortized) so the
            // invariant "a retained Message always has a resident
            // payload" holds, which is what `get_resident` panics on.
            //
            // The journal pump is woken on every append and normally
            // stays well ahead; if it lagged a FULL window behind,
            // the evicted unjournaled messages are permanently lost
            // to the inspector trail: log that loudly.
            if let Some(floor) = store.floor() {
                let journaled_through =
                    self.inner.journaled_through.load(Ordering::Acquire);
                let mut lost_unjournaled = 0u64;
                log.retain(|e| {
                    let keep = e.offset >= floor
                        || !matches!(e.kind, BusEntryKind::Message { .. });
                    if !keep && e.offset >= journaled_through {
                        lost_unjournaled += 1;
                    }
                    keep
                });
                if lost_unjournaled > 0 {
                    tracing::error!(
                        target: "weft_core::bus",
                        bus_id = %self.inner.id,
                        lost = lost_unjournaled,
                        "ephemeral bus evicted messages the journal pump had not \
                         shipped yet (pump lagged a full window); the inspector \
                         trail has a permanent gap here"
                    );
                }
            }
        } else {
            // Journaled mode: bound the in-RAM log to `window` Message
            // entries WITHOUT losing durability. We only ever drop Message
            // entries that are BOTH (a) already shipped to the DB
            // (`offset < journaled_through`, so a fresh worker can replay
            // them) AND (b) older than the most recent `window` messages.
            // Membership entries (Joined/Left/Closed) are always retained
            // (load-bearing for cursors, bounded by participant churn). A
            // cursor that had pointed into the trimmed span silently
            // resumes at the next retained entry, the same RAM-only
            // semantics ephemeral has: a cursor never reaches the DB,
            // the window is the whole world.
            self.inner.trim_journaled_window(&mut log);
        }
        drop(log);
        if !self.inner.ephemeral && payload_byte_size > JOURNALED_PAYLOAD_WARN_BYTES {
            tracing::warn!(
                target: "weft_core::bus",
                bus_id = %self.inner.id,
                bytes = payload_byte_size,
                threshold = JOURNALED_PAYLOAD_WARN_BYTES,
                "journaled bus received an oversized payload; consider ephemeral mode"
            );
        }
        Ok(())
    }

    // ----- cursor API ---------------------------------------------

    /// A fresh cursor. For a REGISTERED handle it starts at this
    /// handle's own `Joined` entry; for an unregistered (observer)
    /// handle it starts at the current tail.
    ///
    /// The registered anchor is what makes the registration handshake
    /// race-free: `register` is the "I am here and ready" act peers
    /// wait on, so once a peer's `wait_for(me)` resolves, anything it
    /// sends lands at an offset AFTER my `Joined` entry, and a cursor
    /// anchored there can never miss it. A tail-snapshot cursor here
    /// would race: the peer's first message could land between my
    /// `register()` and my `cursor()` and be lost forever (both sides
    /// then park on each other).
    pub fn cursor(&self) -> BusCursor {
        let next_offset = match &self.registration {
            Some((_, joined_offset)) => *joined_offset,
            None => self.inner.log_len(),
        };
        BusCursor::new(Arc::downgrade(&self.inner), next_offset, None, self.node.clone())
    }

    /// The earliest offset still resident in RAM. A cursor cannot read
    /// below this (older entries were trimmed out of the window; cursors
    /// never read the DB). [`Self::cursor_from_start`] starts here.
    pub fn retained_floor(&self) -> u64 {
        self.inner.retained_floor()
    }

    /// A fresh cursor positioned at an explicit `offset`. The general
    /// primitive behind the others: history-from-the-window-start is
    /// `cursor_at(retained_floor)` (what [`Self::cursor_from_start`]
    /// does); a plain forward cursor is [`Self::cursor`]. An `offset`
    /// below the retained floor silently resumes at the oldest retained
    /// entry (RAM-only: the window is the whole readable world); an
    /// `offset` past the tail simply waits for entries to arrive.
    pub fn cursor_at(&self, offset: u64) -> BusCursor {
        BusCursor::new(Arc::downgrade(&self.inner), offset, None, self.node.clone())
    }

    /// A fresh cursor from the earliest entry STILL RETAINED in RAM. Not
    /// "offset 0": on a windowed bus the early entries may have been
    /// trimmed (journaled ones live on in the DB, but a cursor never reads
    /// the DB). This reads everything a cursor can still reach, oldest
    /// first.
    pub fn cursor_from_start(&self) -> BusCursor {
        self.cursor_at(self.inner.retained_floor())
    }

    // ----- membership wait -----------------------------------------

    /// Wait until `name` has joined this bus at any point in its
    /// history. Resolves immediately if `name` already appeared
    /// (past-aware: the cursor walks the log from offset 0). Errors
    /// with `Closed` if the bus closes before the wait is satisfied.
    pub async fn wait_for(&self, name: &str) -> Result<(), WaitError> {
        let name_owned = name.to_string();
        self.wait_for_joins(move |seen| seen.contains(&name_owned)).await
    }

    /// Past-aware membership wait. Re-evaluates `pred` against the
    /// truncation-immune `ever_joined` set (every name that ever
    /// registered, maintained under the log lock at register time);
    /// returns as soon as `pred` is true. Errors with `Closed` if the
    /// bus closes before `pred` is satisfied. Reading `ever_joined`
    /// instead of walking the log keeps a join that happened before
    /// the wait (or that an ephemeral log already truncated) just as
    /// visible as one landing during the wait. This is the single
    /// source of truth for membership waits; `wait_for` is a one-liner
    /// that builds a predicate and calls in. An all-of / any-of variant
    /// is a two-line predicate to add here the moment a real consumer
    /// needs one.
    ///
    /// Runs the shared wait protocol (`liveness::wait_on`), which
    /// holds exactly ONE wait registration (one `WaitId`) across the
    /// entire call: the wait's observed/parked state in the engine's
    /// liveness map stays continuous across wakes instead of being
    /// re-minted at the conservative not-parked baseline on each wake,
    /// which would keep deferring a provable deadlock.
    async fn wait_for_joins<F>(&self, mut pred: F) -> Result<(), WaitError>
    where
        F: FnMut(&HashSet<String>) -> bool + Send + Sync,
    {
        // Predicate-first, closed-second: a join that landed before
        // the close still satisfies the wait (matching the old
        // log-scan order, where a Joined entry preceding the Closed
        // entry was found first). Both reads happen under the log
        // lock so they see one consistent snapshot (register and
        // close both mutate under it).
        let check = || -> Option<Result<(), WaitError>> {
            let _log = self.inner.lock_log();
            let seen = self
                .inner
                .ever_joined
                .lock()
                .expect("ever_joined poisoned");
            if pred(&seen) {
                return Some(Ok(()));
            }
            if self.inner.closed.load(Ordering::Acquire) {
                return Some(Err(WaitError::Closed));
            }
            None
        };
        // The shared wait protocol (`liveness::wait_on`) holds ONE
        // guard for the whole wait, arms before every check (no lost
        // wakeup) and records observed under the same generation
        // discipline the stuck-check relies on.
        let source = self.inner.clone() as Arc<dyn WaitSource>;
        wait_on(&self.inner.liveness, &self.node, &source, check).await
    }

    // ----- lifecycle ---------------------------------------------

    pub fn close(&self) {
        self.inner.close();
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// JSON marker for THIS bus. The marker is what a producer puts
    /// on an output port; consumers resolve it back via `ctx.bus`.
    pub fn marker(&self) -> Value {
        let mode = if self.inner.ephemeral { BusMode::Ephemeral } else { BusMode::Journaled };
        crate::weft_type::WeftType::bus_marker(&self.inner.id.to_string(), mode)
    }
}

/// A bus handle that CLOSES the bus when dropped: the shape every
/// producer (and every consumer whose exit must not leave peers parked)
/// wants, made unforgettable. Every exit path of the owning node body
/// (success, error, panic-unwind) runs the close; peers' cursors read
/// the `Closed` entry and end instead of parking forever. Derefs to
/// [`BusHandle`], so the whole surface is available. Produced by
/// `ctx.open_bus` / `ctx.join_bus`; a deliberate non-closing observer
/// uses `ctx.bus_from_input` instead.
pub struct ClosingBus {
    handle: BusHandle,
}

impl ClosingBus {
    pub fn new(handle: BusHandle) -> Self {
        Self { handle }
    }
}

impl std::ops::Deref for ClosingBus {
    type Target = BusHandle;
    fn deref(&self) -> &BusHandle {
        &self.handle
    }
}

impl std::ops::DerefMut for ClosingBus {
    fn deref_mut(&mut self) -> &mut BusHandle {
        &mut self.handle
    }
}

impl Drop for ClosingBus {
    fn drop(&mut self) {
        self.handle.close();
    }
}

impl Drop for BusHandle {
    /// When a registered handle goes away, append a `Left` entry and
    /// remove the name from the live set. Unregistered handles drop
    /// with no log effect.
    ///
    /// The append and the `present_names` removal happen under the
    /// same `log` lock as everything else. If the bus has already
    /// been closed (the `Closed` entry has landed), we bail without
    /// touching either: a closed bus does not accept further log
    /// entries, and neither `wait_for*` nor a fresh `register` will
    /// ever read `present_names` again on a closed bus, so the
    /// cleanup would be dead work.
    fn drop(&mut self) {
        let Some((name, _)) = self.registration.take() else { return };
        let mut log = self.inner.lock_log();
        if self.inner.closed.load(Ordering::Acquire) {
            // The bus is dead; no cursor or wait_for can read state
            // from here on, so neither the Left entry nor the
            // present_names removal is observable. Bail.
            return;
        }
        self.inner
            .present_names
            .lock()
            .expect("present_names poisoned")
            .remove(&name);
        self.inner.push_entry(&mut log, BusEntryKind::Left { name });
    }
}

// ----- BusCursor ------------------------------------------------------

type CursorFilter = Arc<dyn Fn(&BusEntry) -> bool + Send + Sync + 'static>;

/// Independent read-cursor over a bus log. `next()` returns the next
/// matching entry, `None` when the cursor reaches a `Closed` entry or
/// the bus's inner has been reclaimed.
///
/// Stores `Weak<BusInner>`, upgrades to `Arc` per `next()` call. The
/// cursor never pins a bus alive on its own: a producer-side drop
/// releases the bus naturally even if a consumer's cursor still
/// exists. The Arc is held only for the duration of one `next()`.
pub struct BusCursor {
    inner: Weak<BusInner>,
    next_offset: u64,
    filter: Option<CursorFilter>,
    /// The node execution that owns this cursor (inherited from the
    /// handle that minted it), or `None` outside an engine-driven node.
    /// Keys this cursor's waits in the engine's liveness map, so a node
    /// parked in `next()` is correctly attributed.
    node: Option<FiringLocation>,
}

impl BusCursor {
    fn new(
        inner: Weak<BusInner>,
        next_offset: u64,
        filter: Option<CursorFilter>,
        node: Option<FiringLocation>,
    ) -> Self {
        Self {
            inner,
            next_offset,
            filter,
            node,
        }
    }

    /// Attach a filter closure. Non-matching entries are skipped (the
    /// cursor advances past them). The filter sees the FULL resolved
    /// entry, including payload, on both journaled AND ephemeral
    /// buses (see `next` for the resolve-before-filter rationale and
    /// the locking contract).
    pub fn with_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&BusEntry) -> bool + Send + Sync + 'static,
    {
        self.filter = Some(Arc::new(filter));
        self
    }

    /// The cursor's current offset (the offset it will read NEXT).
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// The next MESSAGE of kind `kind`, skipping membership entries and
    /// other kinds: `(sender, payload)`, or `None` when the bus closes.
    /// The typed read every plain consumer wants instead of matching
    /// the entry enum by hand. Composes with a constructed filter (both
    /// must accept).
    pub async fn next_message(&mut self, kind: &str) -> Option<(String, WirePayload)> {
        loop {
            match self.next().await {
                None => return None,
                Some(entry) => match entry.kind {
                    BusEntryKind::Message { from, msg_kind, payload, .. }
                        if msg_kind == kind =>
                    {
                        let payload = payload.expect(
                            "a cursor-delivered Message always carries its payload \
                             (ephemeral payloads are resolved before delivery)",
                        );
                        return Some((from, payload));
                    }
                    _ => continue,
                },
            }
        }
    }

    /// [`Self::next_message`] for a JSON bus: `(sender, value)`. The
    /// payload shape is frozen at bus creation, so a mismatch here is a
    /// wiring bug (a bytes bus read as JSON), surfaced as a loud
    /// `CursorError::WrongPayload` the caller must handle.
    pub async fn next_json(
        &mut self,
        kind: &str,
    ) -> Result<Option<(String, Value)>, CursorError> {
        match self.next_message(kind).await {
            None => Ok(None),
            Some((from, payload)) => match payload.into_json() {
                Some(value) => Ok(Some((from, value))),
                None => Err(CursorError::WrongPayload { expected: "bytes", requested: "json" }),
            },
        }
    }

    /// [`Self::next_message`] for a bytes bus: `(sender, bytes)`. A
    /// JSON payload here is the same wiring bug in the other direction,
    /// surfaced as `CursorError::WrongPayload`.
    pub async fn next_bytes(
        &mut self,
        kind: &str,
    ) -> Result<Option<(String, bytes::Bytes)>, CursorError> {
        match self.next_message(kind).await {
            None => Ok(None),
            Some((from, payload)) => match payload.into_bytes() {
                Some(bytes) => Ok(Some((from, bytes))),
                None => Err(CursorError::WrongPayload { expected: "json", requested: "bytes" }),
            },
        }
    }

    /// Next matching entry, `None` on close (no more entries). A cursor
    /// pointing into a trimmed span (its offsets evicted from the
    /// in-RAM window) silently resumes at the next retained entry: only
    /// `Message` entries are ever trimmed, membership entries are
    /// always retained, so nothing the consumer needs hides in a gap.
    ///
    /// On EPHEMERAL buses the payload lives in the store, not in the
    /// log entry. The payload is resolved from the store BEFORE the
    /// filter runs, so a filter that pattern-matches `payload:
    /// Some(...)` works the same way on both bus modes (filter-first
    /// on the raw entry would silently produce zero matches, a
    /// footgun). Journaled entries (and non-Message kinds) are
    /// filtered by reference and cloned only on accept, so rejecting
    /// a journaled entry never pays a payload clone.
    ///
    /// The filter closure runs under the bus log lock (the scan is
    /// read-only and zero-copy on rejection); the lock is
    /// poison-recovering (see `BusInner::lock_log`) so a panicking
    /// filter fails only its own node, not every bus participant.
    pub async fn next(&mut self) -> Option<BusEntry> {
        let inner = self.inner.upgrade()?;
        // One destructure splits the borrows: the closure takes the
        // cursor position and filter, the wait keeps the node identity.
        let Self { next_offset, filter, node, .. } = self;
        // The full search under the log lock is the wait condition:
        // `Some(Some(entry))` on a hit, `Some(None)` at end-of-stream,
        // `None` parks. The shared protocol (`liveness::wait_on`)
        // holds one guard across the whole call and arms before every
        // search, so a send landing mid-search can never be a lost
        // wakeup, and "observed == current" provably means "this
        // waiter saw everything and still parked".
        let search_inner = inner.clone();
        let evaluate = move || -> Option<Option<BusEntry>> {
            let log = search_inner.lock_log();
            // The scan starts at the first RETAINED entry at or past the
            // cursor's offset: an offset inside a trimmed span (only
            // `Message` entries are ever evicted; membership entries are
            // always retained, so nothing the consumer needs hides in a
            // gap) is silently bridged, and the cursor only ever moves
            // FORWARD, never skipping a retained entry.
            let mut idx = log.partition_point(|e| e.offset < *next_offset);
            let len = log.len();
            while idx < len {
                let entry = &log[idx];
                if matches!(entry.kind, BusEntryKind::Closed) {
                    *next_offset = entry.offset;
                    return Some(None);
                }
                let needs_ephemeral_resolve = matches!(
                    &entry.kind,
                    BusEntryKind::Message { payload, .. } if payload.is_none()
                );
                let resolved_entry: Option<BusEntry> = if needs_ephemeral_resolve {
                    let store = search_inner.ephemeral_store.as_ref().expect(
                        "Message with payload=None on a non-ephemeral bus is impossible \
                         by construction (send routes ephemeral payloads into the store)",
                    );
                    // A RETAINED Message always has a resident
                    // payload (send evicts the log entry in the
                    // same locked section that evicts the
                    // payload); a miss panics inside
                    // `get_resident`.
                    let value = store.get_resident(entry.offset);
                    let BusEntryKind::Message {
                        from,
                        msg_kind,
                        payload_byte_size,
                        ..
                    } = &entry.kind
                    else {
                        unreachable!("needs_ephemeral_resolve guards Message variant");
                    };
                    Some(BusEntry {
                        offset: entry.offset,
                        at_unix: entry.at_unix,
                        kind: BusEntryKind::Message {
                            from: from.clone(),
                            msg_kind: msg_kind.clone(),
                            payload: Some(value),
                            payload_byte_size: *payload_byte_size,
                        },
                    })
                } else {
                    None
                };
                let entry_for_filter: &BusEntry = resolved_entry.as_ref().unwrap_or(entry);
                let allow = filter.as_ref().is_none_or(|f| f(entry_for_filter));
                if !allow {
                    idx += 1;
                    continue;
                }
                *next_offset = entry.offset + 1;
                return Some(Some(resolved_entry.unwrap_or_else(|| entry.clone())));
            }
            // Scan reached the tail without a hit; next read resumes at
            // the tail offset (one past the last retained entry, so
            // filter-rejected entries are not rescanned). Never moved
            // backward so a cursor positioned past the tail keeps its
            // place.
            *next_offset = (*next_offset).max(log.last().map(|e| e.offset + 1).unwrap_or(0));
            if search_inner.closed.load(Ordering::Acquire) {
                // The log is closed and this cursor is at (or started
                // past) its tail. Under the log lock, `closed` implies
                // the `Closed` entry is already in the log, so a cursor
                // past the tail has consumed or skipped it: this is
                // end-of-stream.
                return Some(None);
            }
            None
        };
        let source = inner.clone() as Arc<dyn WaitSource>;
        wait_on(&inner.liveness, node, &source, evaluate).await
    }
}

// ----- Registry --------------------------------------------------------

/// Why a `BusRegistry::lookup` failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BusLookupError {
    #[error("value is not a bus marker")]
    NotABusMarker,
    #[error("bus marker uuid is malformed: {0}")]
    InvalidUuid(String),
    #[error("no bus with id {0} is live in this execution")]
    UnknownBus(Uuid),
}

/// Per-execution registry mapping a bus's id to its live state. The
/// engine's `BusCoordinator` owns one. A node creating a bus inserts;
/// a node receiving a marker looks up. Single source of truth for
/// "every bus this execution minted": holds the strong `Arc<BusInner>`
/// so the bus stays pinned for the lifetime of the execution and the
/// pump's `Weak` refs always upgrade. Released by `shutdown()`'s
/// `clear()` after the final drain.
pub struct BusRegistry {
    inner: Mutex<HashMap<Uuid, Arc<BusInner>>>,
}

impl Default for BusRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BusRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Pin a fresh bus's `Arc<BusInner>` so consumers can resolve
    /// markers carrying its id AND the pump can drain it. Called once
    /// per bus, at creation time.
    pub fn insert(&self, handle: &BusHandle) {
        self.inner
            .lock()
            .expect("BusRegistry mutex poisoned")
            .insert(handle.inner.id, Arc::clone(&handle.inner));
    }

    /// Resolve a marker JSON value to a fresh consumer handle attributed
    /// to the resolving node execution `node` (whose identity keys its
    /// own bus liveness). Errors loud on every failure mode.
    pub fn lookup(
        &self,
        marker: &Value,
        node: Option<FiringLocation>,
    ) -> Result<BusHandle, BusLookupError> {
        let id_str = crate::weft_type::WeftType::bus_marker_id(marker)
            .ok_or(BusLookupError::NotABusMarker)?;
        let id = Uuid::parse_str(id_str)
            .map_err(|_| BusLookupError::InvalidUuid(id_str.to_string()))?;
        let inner = self
            .inner
            .lock()
            .expect("BusRegistry mutex poisoned")
            .get(&id)
            .map(Arc::clone)
            .ok_or(BusLookupError::UnknownBus(id))?;
        Ok(BusHandle::from_inner(inner, node))
    }

    /// Snapshot every currently-live bus as `Weak<BusInner>` for the
    /// engine's journal pump. The pump upgrades each Weak per drain
    /// tick; if the registry was just cleared (shutdown), the upgrade
    /// fails and the pump's next iteration is a no-op.
    pub fn live_bus_weaks(&self) -> Vec<Weak<BusInner>> {
        let map = self.inner.lock().expect("BusRegistry mutex poisoned");
        map.values().map(Arc::downgrade).collect()
    }

    /// Snapshot every currently-live bus's `Arc<BusInner>`. Used by
    /// the engine to drive `close_all`, `has_unclosed_bus`, and
    /// `fully_drained` against a stable list of strong refs.
    pub fn live_bus_arcs(&self) -> Vec<Arc<BusInner>> {
        let map = self.inner.lock().expect("BusRegistry mutex poisoned");
        map.values().map(Arc::clone).collect()
    }

    /// Release the registry's strong refs. After this returns, any
    /// bus whose only other ref was the registry is freed. Called by
    /// `BusCoordinator::shutdown` after the final drain has been acked.
    pub fn clear(&self) {
        self.inner
            .lock()
            .expect("BusRegistry mutex poisoned")
            .clear();
    }
}

impl std::fmt::Debug for BusHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusHandle")
            .field("id", &self.inner.id)
            .field("ephemeral", &self.inner.ephemeral)
            .field("name", &self.name())
            .field("closed", &self.is_closed())
            .finish()
    }
}

// ----- internal helpers ------------------------------------------------

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX_EPOCH")
        .as_secs()
}

// =====================================================================
//                                tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn registered(bus: &BusHandle, name: &str) -> BusHandle {
        let mut h = bus.new_handle();
        h.register(name).unwrap();
        h
    }

    /// A bytes bus carries raw frames end to end (no base64 inside the
    /// process), refuses the wrong payload shape both ways, and its
    /// marker + handle expose the declared parameters and metadata.
    #[tokio::test]
    async fn a_bytes_bus_carries_frames_and_declares_its_parameters() {
        let bus = BusHandle::create_with_options(BusOptions {
            ephemeral: true,
            payload: BusPayloadKind::Bytes,
            journal_window: Some(std::time::Duration::from_millis(250)),
            meta: serde_json::json!({ "sample_rate": 16000 }),
            ..Default::default()
        })
        .unwrap();
        let producer = registered(&bus, "mic");
        let mut cursor = bus.cursor();

        // The wrong shape is refused loudly; the right one flows as bytes.
        let err = producer.send("frame", json!({"x": 1})).unwrap_err();
        assert!(matches!(err, SendError::WrongPayload { expected: "bytes", sent: "json" }));
        producer.send_bytes("frame", vec![1u8, 2, 3]).unwrap();
        let (from, frame) = cursor.next_bytes("frame").await.unwrap().unwrap();
        assert_eq!((from.as_str(), frame.as_ref()), ("mic", &[1u8, 2, 3][..]));

        // The declared parameters read back off the handle; the marker
        // surfaces only id + mode (everything else lives on the handle
        // a consumer resolves from it).
        assert_eq!(bus.payload_kind(), BusPayloadKind::Bytes);
        assert_eq!(bus.meta()["sample_rate"], 16000);
        let marker = &bus.marker()["__weft_bus__"];
        assert_eq!(marker["mode"], "ephemeral");
        assert!(marker.get("payload").is_none(), "the marker carries only id + mode");

        // And a JSON bus refuses bytes symmetrically.
        let jbus = BusHandle::create();
        let p = registered(&jbus, "chat");
        let err = p.send_bytes("frame", vec![0u8]).unwrap_err();
        assert!(matches!(err, SendError::WrongPayload { expected: "json", sent: "bytes" }));
    }

    /// The window aggregate keeps a journaled bus's messages whole and
    /// reduces an ephemeral bus's window to its per-sender/kind rollup.
    #[test]
    fn a_window_aggregates_messages_or_metadata() {
        let entry = |offset: u64, from: &str, kind: &str, bytes: u64| BusEntry {
            offset,
            at_unix: 7,
            kind: BusEntryKind::Message {
                from: from.into(),
                msg_kind: kind.into(),
                payload: Some(WirePayload::Json(json!(offset))),
                payload_byte_size: bytes,
            },
        };
        let entries =
            vec![entry(3, "mic", "audio", 10), entry(4, "mic", "audio", 20), entry(6, "b", "x", 5)];

        let journaled = aggregate_window(&entries, true).unwrap();
        assert_eq!((journaled.first_offset, journaled.last_offset), (3, 6));
        assert_eq!(journaled.last_at_unix, 7, "stamped from the last entry's append time");
        assert_eq!(journaled.messages.len(), 3);
        assert_eq!(journaled.messages[1].payload, WirePayload::Json(json!(4)));
        assert_eq!(journaled.totals.len(), 2);
        assert_eq!((journaled.totals[0].count, journaled.totals[0].bytes), (2, 30));

        let ephemeral = aggregate_window(&entries, false).unwrap();
        assert!(ephemeral.messages.is_empty(), "no payloads for an ephemeral window");
        assert_eq!(ephemeral.totals, journaled.totals);
        assert_eq!(ephemeral.last_at_unix, 7, "the stamp rides even without payloads");
        assert!(aggregate_window(&[], true).is_none(), "an empty window writes nothing");
    }

    /// A byte payload's wire form is tagged base64 and round-trips.
    #[test]
    fn byte_payloads_serialize_as_tagged_base64() {
        let p = WirePayload::Bytes(bytes::Bytes::from(vec![0u8, 255, 7]));
        let wire = serde_json::to_value(&p).unwrap();
        assert_eq!(wire["kind"], "bytes");
        assert_eq!(wire["data"], "AP8H");
        assert_eq!(serde_json::from_value::<WirePayload>(wire).unwrap(), p);
        assert_eq!(p.byte_size(), 3);
    }

    /// A typed cursor read of the wrong shape is a loud error in BOTH
    /// directions: `next_json` on a bytes bus and `next_bytes` on a
    /// JSON bus, mirroring the send side's `WrongPayload`.
    #[tokio::test]
    async fn typed_reads_refuse_the_wrong_payload_shape_both_ways() {
        let bytes_bus = BusHandle::create_with_options(BusOptions {
            payload: BusPayloadKind::Bytes,
            ..Default::default()
        })
        .unwrap();
        let mic = registered(&bytes_bus, "mic");
        let mut c = bytes_bus.cursor();
        mic.send_bytes("frame", vec![1u8, 2]).unwrap();
        assert_eq!(
            c.next_json("frame").await.unwrap_err(),
            CursorError::WrongPayload { expected: "bytes", requested: "json" }
        );

        let json_bus = BusHandle::create();
        let chat = registered(&json_bus, "chat");
        let mut c = json_bus.cursor();
        chat.send("msg", json!("hi")).unwrap();
        assert_eq!(
            c.next_bytes("msg").await.unwrap_err(),
            CursorError::WrongPayload { expected: "json", requested: "bytes" }
        );
    }

    /// The close-on-drop guard: dropping a `ClosingBus` (any exit path
    /// of the owning body) closes the bus, so a parked reader ends
    /// instead of waiting forever.
    #[tokio::test]
    async fn the_closing_guard_ends_readers_on_drop() {
        let bus = BusHandle::create();
        let mut cursor = bus.cursor();
        let guard = ClosingBus::new(registered(&bus, "host"));
        guard.send("hi", json!(1)).unwrap();
        drop(guard);
        assert!(matches!(cursor.next_json("hi").await, Ok(Some((_, v))) if v == json!(1)));
        assert!(cursor.next_json("hi").await.unwrap().is_none(), "closed on drop");
        assert!(bus.is_closed());
    }

    /// Drive `cursor` until its next Message entry; skips Joined/Left.
    /// Returns `(from, kind, payload)` or None on close.
    async fn next_message(cursor: &mut BusCursor) -> Option<(String, String, Value)> {
        loop {
            match cursor.next().await {
                None => return None,
                Some(entry) => match entry.kind {
                    BusEntryKind::Message { from, msg_kind, payload, .. } => {
                        return Some((from, msg_kind, payload.expect("journaled payload").into_json().expect("json bus")));
                    }
                    _ => continue,
                },
            }
        }
    }

    #[tokio::test]
    async fn send_recv_round_trip() {
        let bus = BusHandle::create();
        let stt = registered(&bus, "stt");
        let consumer = registered(&bus, "llm");
        let mut cursor = consumer.cursor();
        stt.send("transcript", json!({ "text": "hi" })).unwrap();
        let (from, kind, payload) = next_message(&mut cursor).await.unwrap();
        assert_eq!(from, "stt");
        assert_eq!(kind, "transcript");
        assert_eq!(payload, json!({ "text": "hi" }));
    }

    #[tokio::test]
    async fn sends_stamped_with_registered_name() {
        let bus = BusHandle::create();
        let llm = registered(&bus, "llm");
        let rx = registered(&bus, "rx");
        let mut cursor = rx.cursor();
        llm.send("response", json!("hi")).unwrap();
        assert_eq!(next_message(&mut cursor).await.unwrap().0, "llm");
    }

    #[tokio::test]
    async fn send_before_register_errors() {
        let bus = BusHandle::create();
        assert_eq!(bus.send("x", json!(1)), Err(SendError::NotRegistered));
    }

    #[tokio::test]
    async fn duplicate_name_register_errors() {
        let bus = BusHandle::create();
        let _llm = registered(&bus, "llm");
        let mut clash = bus.new_handle();
        assert_eq!(
            clash.register("llm"),
            Err(RegisterError::NameTaken("llm".into()))
        );
        assert!(clash.register("llm-2").is_ok());
    }

    #[tokio::test]
    async fn double_register_same_handle_errors() {
        let bus = BusHandle::create();
        let mut h = bus.new_handle();
        h.register("a").unwrap();
        assert_eq!(
            h.register("b"),
            Err(RegisterError::AlreadyRegistered("a".into()))
        );
    }

    #[tokio::test]
    async fn wait_for_releases_on_register() {
        let bus = BusHandle::create();
        let waiter = bus.new_handle();
        let bus2 = bus.new_handle();
        let wait = tokio::spawn(async move { waiter.wait_for("llm").await });
        tokio::task::yield_now().await;
        let _llm = registered(&bus2, "llm");
        assert!(wait.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn wait_for_resolves_on_past_register() {
        // wait_for reads the truncation-immune `ever_joined` set, so a
        // wait_for issued AFTER the peer joined still resolves immediately.
        let bus = BusHandle::create();
        let _host = registered(&bus, "host");
        let guest = bus.new_handle();
        // No yield needed; the Joined entry is in the log.
        assert!(guest.wait_for("host").await.is_ok());
    }

    #[tokio::test]
    async fn wait_for_errors_on_close() {
        let bus = BusHandle::create();
        let waiter = bus.new_handle();
        let wait = tokio::spawn(async move { waiter.wait_for("llm").await });
        tokio::task::yield_now().await;
        bus.close();
        assert_eq!(wait.await.unwrap().unwrap_err(), WaitError::Closed);
    }

    #[tokio::test]
    async fn close_before_recv_returns_none() {
        let bus = BusHandle::create();
        let stt = registered(&bus, "stt");
        let consumer = registered(&bus, "llm");
        let mut cursor = consumer.cursor();
        bus.close();
        assert_eq!(stt.send("late", json!(null)), Err(SendError::Closed));
        assert!(next_message(&mut cursor).await.is_none());
    }

    #[tokio::test]
    async fn close_unblocks_a_waiting_cursor() {
        let bus = BusHandle::create();
        let consumer = registered(&bus, "llm");
        let mut cursor = consumer.cursor();
        let recv_task = tokio::spawn(async move { next_message(&mut cursor).await });
        tokio::task::yield_now().await;
        bus.close();
        assert!(recv_task.await.unwrap().is_none());
    }

    #[tokio::test]
    async fn close_drains_buffered_then_ends() {
        let bus = BusHandle::create();
        let stt = registered(&bus, "stt");
        let consumer = registered(&bus, "llm");
        let mut cursor = consumer.cursor();
        stt.send("a", json!(1)).unwrap();
        stt.send("b", json!(2)).unwrap();
        bus.close();
        assert_eq!(next_message(&mut cursor).await.unwrap().1, "a");
        assert_eq!(next_message(&mut cursor).await.unwrap().1, "b");
        assert!(next_message(&mut cursor).await.is_none(), "drained, then None");
    }

    #[tokio::test]
    async fn two_consumers_each_see_messages() {
        let bus = BusHandle::create();
        let stt = registered(&bus, "stt");
        let a = registered(&bus, "llm");
        let b = registered(&bus, "tts");
        let mut ca = a.cursor();
        let mut cb = b.cursor();
        stt.send("m", json!(1)).unwrap();
        assert_eq!(next_message(&mut ca).await.unwrap().2, json!(1));
        assert_eq!(next_message(&mut cb).await.unwrap().2, json!(1));
    }

    // Regression for the `Notify::notified()` arm-then-check race
    // under a MULTI-THREADED tokio runtime. The new cursor still
    // pin+enables the future before re-checking the log, so the same
    // arm-then-check discipline applies. Stress-looped so any future
    // regression of the arm-then-check window reproduces on the first
    // failing CI run rather than waiting for the flake to find us.
    crate::stress_test!(
        name: wait_for_does_not_miss_notify_under_multi_thread,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            for _ in 0..200 {
                let bus = BusHandle::create();
                let waiter = bus.new_handle();
                let registrar = bus.new_handle();
                drop(bus);
                let wait = tokio::spawn(async move { waiter.wait_for("peer").await });
                let reg = tokio::spawn(async move {
                    let mut h = registrar;
                    h.register("peer").unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                });
                let result = tokio::time::timeout(std::time::Duration::from_millis(500), wait)
                    .await
                    .expect("wait_for must not hang under multi-threaded runtime")
                    .expect("join ok");
                assert!(result.is_ok(), "wait_for resolved Ok");
                let _ = reg.await;
            }
        }
    );

    #[tokio::test]
    async fn dropping_a_registered_handle_unregisters_it() {
        let bus = BusHandle::create();
        let llm = registered(&bus, "llm");
        assert!(bus.wait_for("llm").await.is_ok());
        drop(llm);
        // The new wait_for reads the `ever_joined` set, so it would still
        // find the past Joined. The honest "is the name live RIGHT NOW"
        // signal is `present_names`; we exercise it indirectly via
        // wait_for_all_releases_on_complete_set.
        // For this regression we assert membership-set hygiene:
        let _new_llm = registered(&bus, "llm"); // not NameTaken
    }

    // ----- New shape: cursors + offsets + log entries -------------

    #[tokio::test]
    async fn cursor_returns_entries_in_order_with_monotonic_offsets() {
        let bus = BusHandle::create();
        let p = registered(&bus, "p");
        p.send("a", json!(1)).unwrap();
        p.send("b", json!(2)).unwrap();
        let mut c = bus.cursor_from_start();
        let mut offsets = Vec::new();
        for _ in 0..3 {
            let e = c.next().await.unwrap();
            offsets.push(e.offset);
        }
        assert_eq!(offsets, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn cursor_with_filter_skips_non_matching() {
        let bus = BusHandle::create();
        let p = registered(&bus, "p");
        p.send("a", json!(1)).unwrap();
        p.send("b", json!(2)).unwrap();
        let mut c = bus.cursor_from_start().with_filter(|e| {
            matches!(&e.kind, BusEntryKind::Message { msg_kind, .. } if msg_kind == "b")
        });
        let e = c.next().await.unwrap();
        match e.kind {
            BusEntryKind::Message { msg_kind, .. } => assert_eq!(msg_kind, "b"),
            _ => panic!("expected Message"),
        }
    }

    #[tokio::test]
    async fn closed_entry_terminates_cursor() {
        let bus = BusHandle::create();
        let p = registered(&bus, "p");
        p.send("a", json!(1)).unwrap();
        bus.close();
        // After close, the producer handle's drop runs in scope but
        // the bus is already closed, so no Left entry is appended.
        // The log is Joined, Message, Closed. The cursor returns the
        // first two, then `None` on the Closed.
        drop(p);
        let mut c = bus.cursor_from_start();
        let _joined = c.next().await.unwrap();
        let _msg = c.next().await.unwrap();
        assert!(c.next().await.is_none());
    }

    #[tokio::test]
    async fn ephemeral_send_does_not_journal_payload_in_log_entry() {
        let bus =
            BusHandle::create_with_options(BusOptions { ephemeral: true, window: Some(8), ..Default::default() }).expect("valid options");
        let p = registered(&bus, "p");
        p.send("frame", json!({ "bytes": "AAAA" })).unwrap();
        // Read entries directly out of the log to confirm payload=None.
        let log = bus.inner.log.lock().unwrap();
        let msg = log
            .iter()
            .find_map(|e| match &e.kind {
                BusEntryKind::Message { payload, payload_byte_size, .. } => {
                    Some((payload.clone(), *payload_byte_size))
                }
                _ => None,
            })
            .unwrap();
        assert!(msg.0.is_none(), "ephemeral log entry must hide payload");
        assert!(msg.1 > 0, "byte size must be populated");
    }

    #[tokio::test]
    async fn ephemeral_cursor_returns_payload_when_window_still_has_it() {
        let bus =
            BusHandle::create_with_options(BusOptions { ephemeral: true, window: Some(8), ..Default::default() }).expect("valid options");
        let p = registered(&bus, "p");
        p.send("frame", json!({ "n": 1 })).unwrap();
        let mut c = bus.cursor_from_start();
        // skip the Joined
        let _j = c.next().await.unwrap();
        let m = c.next().await.unwrap();
        match m.kind {
            BusEntryKind::Message { payload, .. } => {
                assert_eq!(payload.unwrap(), WirePayload::Json(json!({ "n": 1 })));
            }
            _ => panic!("expected Message"),
        }
    }

    /// A slow ephemeral consumer whose offsets were evicted silently
    /// resumes at the oldest retained message: no error, no backward
    /// motion, no message delivered twice.
    #[tokio::test]
    async fn ephemeral_slow_consumer_silently_resumes_at_retained_floor() {
        let bus =
            BusHandle::create_with_options(BusOptions { ephemeral: true, window: Some(2), ..Default::default() }).expect("valid options");
        let p = registered(&bus, "p");
        // Create the cursor BEFORE sends so it starts at the joined-only
        // tail; the test exercises a consumer reading slowly enough that
        // the store evicts.
        let mut c = bus.cursor_from_start();
        for i in 0..6 {
            p.send("frame", json!({ "i": i })).unwrap();
        }
        // First next() is the Joined entry (always retained, never in the
        // ephemeral store).
        let _j = c.next().await.unwrap();
        // Offsets 1..=4 were evicted (window=2 keeps 5,6). The cursor
        // bridges the gap silently: the next read delivers the oldest
        // RETAINED message (i=4 at offset 5), then the tail one.
        let m = c.next().await.expect("a retained message");
        let BusEntryKind::Message { payload, .. } = m.kind else { panic!("expected Message") };
        assert_eq!(m.offset, 5, "resumes at the oldest retained message, never backward");
        assert_eq!(payload.unwrap(), WirePayload::Json(json!({ "i": 4 })));
    }

    /// A Joined entry sitting between evicted Messages must ALWAYS be
    /// surfaced (never skipped). Build the sparse log:
    /// Joined "a" (0), msg (1, evicted), Joined "b" (2), msg (3, live).
    fn sparse_membership_bus() -> BusHandle {
        let bus = BusHandle::create_with_options(BusOptions {
            ephemeral: true,
            window: Some(1),
            ..Default::default()
        })
        .expect("valid options");
        let a = registered(&bus, "a");
        a.send("first", json!({ "i": 0 })).unwrap(); // offset 1
        let _b = registered(&bus, "b"); // Joined offset 2
        a.send("late", json!({ "i": 99 })).unwrap(); // offset 3, evicts offset 1
        bus
    }

    /// The cursor bridges the evicted-message gap silently, surfacing
    /// BOTH Joined entries and the live message. The load-bearing
    /// property: Joined "b" is delivered even though it sits past a
    /// trimmed message.
    #[tokio::test]
    async fn gap_bridging_never_skips_a_joined_entry() {
        let bus = sparse_membership_bus();
        let mut c = bus.cursor_from_start();
        let (mut joins, mut live) = (0u32, 0u32);
        for _ in 0..10 {
            match c.next().await {
                Some(entry) => match entry.kind {
                    BusEntryKind::Joined { .. } => joins += 1,
                    BusEntryKind::Message { .. } => { live += 1; break; }
                    _ => {}
                },
                None => break,
            }
        }
        assert_eq!(joins, 2, "both Joined entries surfaced (b not skipped across the gap)");
        assert_eq!(live, 1, "reaches the live message");
    }

    /// The exact scenario worked through with the user: messages A,B,D,E,G,H
    /// and Join entries C,F; all non-Join messages trimmed. A cursor sitting
    /// at E (an evicted message, mid-log) must RESUME FORWARD at the next
    /// retained entry (F, the Join) WITHOUT skipping it, and must NOT go
    /// backward to the floor (C).
    #[tokio::test]
    async fn resume_is_forward_to_next_retained_never_backward() {
        // window=1 so each new message evicts the previous one's payload;
        // Join entries are never evicted.
        let bus = BusHandle::create_with_options(BusOptions { ephemeral: true, window: Some(1), ..Default::default() })
            .expect("opts");
        let a = registered(&bus, "a");        // Joined @0
        a.send("A", json!(0)).unwrap();        // @1 msg
        a.send("B", json!(1)).unwrap();        // @2 msg
        let _c = registered(&bus, "c");        // Joined @3  (this is "C"/F-like)
        a.send("D", json!(3)).unwrap();        // @4 msg
        a.send("E", json!(4)).unwrap();        // @5 msg
        let _f = registered(&bus, "f");        // Joined @6  (this is "F")
        a.send("G", json!(6)).unwrap();        // @7 msg
        a.send("H", json!(7)).unwrap();        // @8 msg (live; window=1 keeps only this)
        // Cursor positioned AT an evicted mid-log message offset (E == @5).
        // The gap is bridged silently: the first read delivers the next
        // retained entry FORWARD of the cursor, the Join @6 (NOT backward
        // to @0/@3, and NOT skipped).
        let e = cur_next(&bus, 5).await;
        assert_eq!(e.offset, 6, "resumes forward at the next retained entry (the Join @6)");
        assert!(matches!(e.kind, BusEntryKind::Joined { .. }), "the Join @6 is delivered");
    }

    /// Helper for the resume test: one read off a fresh cursor at `offset`.
    async fn cur_next(bus: &BusHandle, offset: u64) -> BusEntry {
        bus.cursor_at(offset).next().await.expect("an entry")
    }

    #[tokio::test]
    async fn journaled_send_returns_journal_degraded_after_flag_set() {
        let bus = BusHandle::create();
        let p = registered(&bus, "p");
        bus.inner.mark_journal_degraded("simulated failure");
        let err = p.send("x", json!(1));
        assert!(matches!(err, Err(SendError::JournalDegraded(_))));
        // A successful pump batch (the ack) clears the flag; sends
        // proceed again.
        bus.inner.acknowledge_journaled_through(bus.inner.log_len());
        assert!(p.send("y", json!(2)).is_ok());
    }

    #[tokio::test]
    async fn drain_journal_tail_returns_unjournaled_entries_and_advances_on_ack() {
        let bus = BusHandle::create();
        let p = registered(&bus, "p");
        p.send("a", json!(1)).unwrap();
        p.send("b", json!(2)).unwrap();
        let batch = bus.inner.drain_journal_tail();
        assert_eq!(batch.len(), 3, "Joined + 2 Messages");
        let through = batch.last().unwrap().offset + 1;
        bus.inner.acknowledge_journaled_through(through);
        let empty = bus.inner.drain_journal_tail();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn marker_round_trips_id_and_mode() {
        let journaled = BusHandle::create();
        let m = journaled.marker();
        assert_eq!(
            crate::weft_type::WeftType::bus_marker_id(&m),
            Some(journaled.id().to_string().as_str())
        );
        assert_eq!(
            crate::weft_type::WeftType::bus_marker_mode(&m),
            Some(BusMode::Journaled)
        );
        let eph = BusHandle::create_with_options(BusOptions { ephemeral: true, window: None, ..Default::default() })
            .expect("valid options");
        let me = eph.marker();
        assert_eq!(
            crate::weft_type::WeftType::bus_marker_mode(&me),
            Some(BusMode::Ephemeral)
        );
    }

    #[tokio::test]
    async fn window_zero_is_rejected_loud() {
        // window=0 would evict every entry before any cursor could read
        // it; the only honest answer is to fail loud at construction. The
        // window applies to both modes, so a zero is rejected regardless of
        // `ephemeral`.
        let err = BusHandle::create_with_options(BusOptions {
            ephemeral: true,
            window: Some(0),
            ..Default::default()
        })
        .expect_err("zero window must fail loud");
        assert!(
            err.contains("window"),
            "error message names the offending field: {err}"
        );
    }

    #[tokio::test]
    async fn register_during_close_is_rejected_loud() {
        // Tests the register-vs-close ordering: once the bus has been
        // closed (the `Closed` entry sits in the log), no further
        // register lands.
        let bus = BusHandle::create();
        bus.close();
        let mut h = bus.new_handle();
        assert_eq!(h.register("late"), Err(RegisterError::Closed));
    }

    /// A cursor minted at-or-past the tail of a CLOSED log must
    /// return `Ok(None)` instead of busy-looping: its scan never
    /// reaches the `Closed` entry (it starts past it), and the
    /// closed-flag re-check used to `continue` BEFORE the park,
    /// producing a zero-await-point spin that pinned a worker thread
    /// at 100% CPU forever (not even a surrounding timeout could
    /// fire, because the future never yielded).
    #[tokio::test]
    async fn cursor_minted_after_close_returns_none_without_spinning() {
        let bus = BusHandle::create();
        let p = registered(&bus, "p");
        p.send("msg", json!(1)).unwrap();
        bus.close();
        // Tail-anchored cursor on an unregistered handle: starts past
        // the Closed entry.
        let observer = bus.new_handle();
        let mut c = observer.cursor();
        let r = tokio::time::timeout(std::time::Duration::from_millis(500), c.next())
            .await
            .expect("must resolve, not spin");
        assert!(r.is_none(), "end-of-stream");
    }

    /// A REGISTERED handle's `cursor()` anchors at its own `Joined`
    /// entry, so a peer message that lands between `register()` and
    /// `cursor()` is still delivered. A tail-snapshot cursor here
    /// would silently lose it (the deterministic core of the
    /// handshake race that deadlocked the chat demo).
    #[tokio::test]
    async fn registered_cursor_sees_messages_sent_before_cursor_creation() {
        let bus = BusHandle::create();
        let mut guest = bus.new_handle();
        guest.register("guest").unwrap();
        // Host observed the guest's Joined entry and sends BEFORE the
        // guest mints its cursor.
        let host = registered(&bus, "host");
        host.send("hello", json!("first")).unwrap();
        let mut c = guest.cursor().with_filter(|e| {
            matches!(&e.kind, BusEntryKind::Message { from, .. } if from == "host")
        });
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), c.next())
            .await
            .expect("must resolve")
            .expect("an entry");
        match got.kind {
            BusEntryKind::Message { payload, .. } => {
                assert_eq!(payload, Some(WirePayload::Json(json!("first"))), "the pre-cursor message arrives");
            }
            other => panic!("expected the host message, got {other:?}"),
        }
    }

    // Full handshake under racing tasks: register -> wait_for(peer)
    // -> cursor() -> converse. With tail-snapshot cursors this
    // deadlocked ~1/1000 runs (the peer's first message slipped
    // between register and cursor); the registration-anchored cursor
    // makes the recipe sound by construction.
    crate::stress_test!(
        name: registration_handshake_never_loses_first_message,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            for _ in 0..50 {
                let bus = BusHandle::create();
                let mut host = bus.new_handle();
                let mut guest = bus.new_handle();
                drop(bus);
                let host_task = tokio::spawn(async move {
                    host.register("host").unwrap();
                    host.wait_for("guest").await.unwrap();
                    let mut replies = host.cursor().with_filter(|e| {
                        matches!(&e.kind, BusEntryKind::Message { from, .. } if from == "guest")
                    });
                    host.send("turn", json!("hello guest")).unwrap();
                    let reply = replies.next().await.expect("guest reply");
                    assert!(matches!(reply.kind, BusEntryKind::Message { .. }));
                });
                let guest_task = tokio::spawn(async move {
                    guest.register("guest").unwrap();
                    guest.wait_for("host").await.unwrap();
                    let mut inbox = guest.cursor().with_filter(|e| {
                        matches!(&e.kind, BusEntryKind::Message { from, .. } if from == "host")
                    });
                    let msg = inbox.next().await.expect("host message");
                    assert!(matches!(msg.kind, BusEntryKind::Message { .. }));
                    guest.send("turn", json!("hello host")).unwrap();
                });
                let both = async {
                    host_task.await.expect("host task");
                    guest_task.await.expect("guest task");
                };
                tokio::time::timeout(std::time::Duration::from_millis(2000), both)
                    .await
                    .expect("handshake must not deadlock");
            }
        }
    );

    // Publish-vs-park race: the consumer arms its park while the
    // producer appends. Every message must be delivered exactly once
    // in order; a lost wake-up here would park the consumer forever.
    crate::stress_test!(
        name: publish_vs_park_never_drops_or_hangs,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            for _ in 0..50 {
                let bus = BusHandle::create();
                let producer = registered(&bus, "p");
                let consumer = bus.new_handle();
                drop(bus);
                let mut c = consumer.cursor().with_filter(|e| {
                    matches!(&e.kind, BusEntryKind::Message { .. })
                });
                let recv = tokio::spawn(async move {
                    let mut got = Vec::new();
                    for _ in 0..20 {
                        let entry = c.next().await.expect("a message");
                        let BusEntryKind::Message { payload, .. } = entry.kind else {
                            panic!("filter admits only messages");
                        };
                        got.push(payload.unwrap().into_json().unwrap());
                    }
                    got
                });
                for i in 0..20 {
                    producer.send("n", json!(i)).unwrap();
                    if i % 5 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                let got = tokio::time::timeout(std::time::Duration::from_millis(2000), recv)
                    .await
                    .expect("consumer must not hang")
                    .expect("join ok");
                let want: Vec<Value> = (0..20).map(|i| json!(i)).collect();
                assert_eq!(got, want, "in-order, no drops, no dups");
            }
        }
    );

    // Close-vs-parked-cursor race: a cursor parked at the tail must
    // wake and return Ok(None) when the bus closes, whatever the
    // interleaving between its arm and the close.
    crate::stress_test!(
        name: close_always_unblocks_parked_cursors,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            for _ in 0..100 {
                let bus = BusHandle::create();
                let consumer = bus.new_handle();
                let closer = bus.new_handle();
                drop(bus);
                let mut c = consumer.cursor();
                let recv = tokio::spawn(async move { c.next().await });
                let close = tokio::spawn(async move { closer.close() });
                let r = tokio::time::timeout(std::time::Duration::from_millis(1000), recv)
                    .await
                    .expect("parked cursor must wake on close")
                    .expect("join ok");
                assert!(r.is_none());
                close.await.expect("closer ok");
            }
        }
    );

    // ----- windowing + cursor-positioning (the new model) ---------------

    /// A journaled bus trims its in-RAM log to the window, but ONLY entries
    /// already shipped to the DB. Here the pump never runs (journaled_through
    /// stays 0), so nothing is trimmed even past the window: durability wins
    /// over the RAM bound. The full history is still readable from RAM.
    #[tokio::test]
    async fn journaled_keeps_unshipped_entries_past_window() {
        let bus = BusHandle::create_with_options(BusOptions { ephemeral: false, window: Some(2), ..Default::default() })
            .expect("opts");
        let tx = registered(&bus, "tx");
        let rx = registered(&bus, "rx");
        for i in 0..5 {
            tx.send("m", json!({ "i": i })).unwrap();
        }
        // Pump never acked, so journaled_through==0: every message is still
        // in RAM, readable from the retained floor, despite window=2.
        let mut cursor = rx.cursor_from_start();
        let mut seen = Vec::new();
        while let Some((_, _, p)) = next_message(&mut cursor).await {
            seen.push(p["i"].as_i64().unwrap());
            if seen.len() == 5 { break; }
        }
        assert_eq!(seen, vec![0, 1, 2, 3, 4], "un-shipped journaled entries are never trimmed");
    }

    /// Once entries are journaled (pump acked), a journaled bus DOES trim
    /// them out of RAM past the window. A cursor that reaches into the
    /// trimmed span silently bridges it and delivers the next retained
    /// message (RAM-only: the window is the whole readable world; the DB
    /// has the data but a cursor never reads it).
    #[tokio::test]
    async fn journaled_trims_shipped_entries_past_window() {
        let bus = BusHandle::create_with_options(BusOptions { ephemeral: false, window: Some(2), ..Default::default() })
            .expect("opts");
        let tx = registered(&bus, "tx");
        for i in 0..5 {
            tx.send("m", json!({ "i": i })).unwrap();
        }
        // Simulate the pump shipping everything to the DB, then one more send
        // triggers the trim of the now-shipped older messages.
        bus.inner_arc().acknowledge_journaled_through(bus.inner.log_len());
        tx.send("m", json!({ "i": 5 })).unwrap();

        // Early MESSAGES were trimmed from RAM (membership entries always
        // kept; cursors never read the DB). The trim actually happened:
        // the oldest retained Message sits past the trimmed span.
        let oldest_msg = {
            let log = bus.inner.log.lock().unwrap();
            log.iter()
                .find(|e| matches!(e.kind, BusEntryKind::Message { .. }))
                .map(|e| e.offset)
                .expect("a retained message")
        };
        assert!(oldest_msg > 1, "shipped messages past the window were trimmed");
        // A cursor into the trimmed span bridges it silently and delivers
        // the oldest RETAINED message.
        let mut c = bus.new_handle().cursor_at(1);
        let mut first_msg: Option<i64> = None;
        for _ in 0..10 {
            match c.next().await {
                Some(e) => {
                    if let BusEntryKind::Message { payload, .. } = e.kind {
                        first_msg = Some(payload.unwrap().into_json().unwrap()["i"].as_i64().unwrap());
                        break;
                    }
                }
                None => break,
            }
        }
        assert!(
            first_msg.is_some_and(|i| i > 0),
            "cursor bridged the trimmed span to a retained message, got {first_msg:?}"
        );
    }

    /// A cursor at the current tail is forward-only (sees only what
    /// arrives after).
    #[tokio::test]
    async fn cursor_at_tail_is_forward_only() {
        let bus = BusHandle::create();
        let tx = registered(&bus, "tx");
        let obs = bus.new_handle();
        tx.send("m", json!({ "i": 0 })).unwrap(); // before the cursor
        let mut cursor = obs.cursor(); // unregistered handle: tail cursor
        tx.send("m", json!({ "i": 1 })).unwrap(); // after the cursor
        let (_, _, p) = next_message(&mut cursor).await.unwrap();
        assert_eq!(p["i"].as_i64().unwrap(), 1, "forward cursor skips the pre-existing message");
    }
}
