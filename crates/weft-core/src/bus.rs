//! In-process message bus: a channel between co-alive nodes in the same
//! run (the same worker process).
//!
//! One node creates a bus (`ctx.open_bus(port, opts, name)`) and emits the
//! marker on an output port; downstream nodes resolve the marker via `ctx.bus`
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
//!   `next(&mut state)` future returns the next matching entry, or `None`
//!   when the bus is closed and the cursor is at the tail.
//!
//! Cursors are independent. Each consumer holds its own. Reading does
//! not consume; the same offset can be read by N cursors. Cursors only
//! ever read RAM: entries trimmed out of the in-RAM window are not
//! reachable by a cursor, and a cursor positioned inside a trimmed span
//! silently resumes forward at the next retained entry (on a live bus
//! only `Message` entries are ever trimmed; membership entries are always
//! retained, so nothing a consumer needs can hide in a gap). A resumed
//! reader carrying on behind a message the bus no longer holds is
//! refused instead (below).
//!
//! ## A bus outlives a pause
//!
//! A run pauses with a bus open: a member that pauses on a wait stays
//! one (its names stay registered, nobody reads it leave), and when
//! every node of the run waits, the whole run pauses around its buses. Every read and membership wait is a place the run
//! can pause, so it takes the firing's state
//! (`crate::context::PausePoint`); a firing opens one reader per bus a
//! run of its body, and its [`FiringBuses`] keeps how far that reader
//! read and which name it holds, saved with the state. The worker that picks the run up opens every bus that was
//! open again, with its id, at the offset it stopped at
//! ([`BusHandle::reopen`]), before any body runs: a journaled bus with
//! the last window of messages its record kept whole, an ephemeral one
//! empty. A resumed body creating its bus on the same port gets that bus
//! back, its reader starts where it stopped, and its name comes back to
//! it. A cursor of a resumed firing carrying on behind a message the bus
//! no longer holds (one it came back from the pause without, or one its
//! window dropped while the firing waited) fails its first read. A node
//! whose reads must not stop at all holds the run instead
//! (`crate::context::Holding`).
//!
//! ## Windowed journaling (the inspector trail)
//!
//! Every append
//! signals a per-run `journal_pump_notify`; the engine's
//! bus-journal-pump task drains every live bus's unjournaled tail. The
//! pump buffers each bus's `Message` entries and flushes ONE
//! `BusWindow` journal row per bus per journal window (a journaled
//! bus's row carries every message in the window; an ephemeral bus's
//! row carries only the per-sender/kind totals). Membership entries
//! (`Joined` / `Left` / `Closed`) are journaled as individual rows, and
//! any open window is flushed FIRST so the row stream never reorders
//! against the bus log. The pump acknowledges `journaled_through` to
//! the bus only AFTER a write succeeds, and owns the per-bus
//! `journaled_through` cursor.
//!
//! ## `BusOptions` (all four knobs)
//!
//! - **`journal`**: what the journal keeps, the same
//!   [`crate::stream_journal::JournalPolicy`] every channel takes: the
//!   payload-retention mode (see below) and the journal aggregation
//!   window (default 1s, one `BusWindow` row per bus per window).
//! - **`window`**: the in-RAM bound, in `Message` entries, for BOTH
//!   modes (default 64).
//! - **`payload`**: what messages carry, `Json` or `Bytes`, frozen at
//!   creation.
//! - **`meta`**: creator-declared stream metadata readable off the
//!   handle by every consumer.
//!
//! The two payload-retention modes:
//!
//! - **Journaled** (default): every send writes the full payload into
//!   the log entry and it reaches the journal via the window rows.
//!   The in-RAM log only trims `Message` entries that are BOTH past
//!   the window AND already shipped to the journal, so durability is
//!   never traded for the RAM bound. A payload of any size travels
//!   the bus untouched; what the journal KEEPS of it is decided in
//!   one place for every channel in the language
//!   ([`crate::stream_journal`]), which trims rather than warns.
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
//! (download a model, open a socket) has the bus the whole time and
//! only registers once it is truly ready, so peers waiting on it do not
//! release early. Registering is also what makes a node a member: the
//! bus lives until nobody is on it and no node can still join it (below).
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
//! run and, when it concludes a wait can never be satisfied,
//! closes the bus, failed, naming why. Every waiting cursor wakes, and
//! its `next()` returns that failure.
//!
//! A member waiting on its own bus (`wait_for`, a read, `until_end`)
//! after every other member left keeps waiting: it is a member, so the
//! bus is open, and a node given the marker later may still join and
//! talk. Nothing hangs for good: once nothing else in the run can move,
//! that same stuck check closes the bus failed and the wait ends with
//! that failure.
//!
//! ## How a bus ends
//!
//! The close carries an end ([`StreamEnd`]): `Finished`, or `Failed`
//! with an error. Nobody owns a bus. A firing that registers a name on
//! it (creating it with `ctx.open_bus`, joining it with `ctx.join_bus`,
//! or registering on any handle it got) is a member until its body
//! returns, however it returns, and then its names leave
//! ([`FiringBuses::leave`]). A member leaving does nothing else to the
//! bus, failed or not: the others carry on, and one of them may be the
//! way the conversation recovers. Once nobody is on it, the bus ends as
//! soon as no node can still join it, as its last member's body returned:
//! finished, or failed with its error, so a reader of a stream whose only
//! producer failed halfway learns it is incomplete instead of reading a
//! short one as whole. A bus nobody ever joined ends finished. A node can
//! still join while the bus's marker is on its way to it (on a wire, in a
//! loop that will hand it to an iteration still to come) or in the input
//! of a firing that has not ended and has not used the bus yet: the
//! engine looks ([`crate::run::bus_reach`]) once it hears the bus is
//! unattended ([`crate::liveness::WaitLiveness::unattended`]), and ends it
//! ([`BusInner::end_unattended`]) when nothing can. A firing uses a bus by
//! registering on it, opening its reader, or waiting on it; one that only
//! hands the marker on holds the bus until it returns.
//! Any node holding the bus may end it sooner with [`BusHandle::close`]:
//! finished while somebody is on it, the way its last member left once
//! nobody is. A reader that never registers (`ctx.bus_from_input`, a
//! debug tap) is no member, and once it reads it keeps nothing open. A
//! member that pauses stays one across the pause. The engine's
//! own closes (a stuck run, the run's end) say failed, naming why.
//!
//! Membership is the log's `Joined` / `Left` entries, which the run's
//! record folds into who is on each bus (`RecordedBus::members`); a bus
//! opened again after a pause starts from that, so every worker reads
//! the same members. A send touches none of it.
//!
//! ## Waiting and the engine
//!
//! A cursor's `next` is a plain in-process tokio await; the worker stays
//! alive and nothing is written. The engine needs to know about it for
//! two decisions: whether every node is waiting with nothing to read (a
//! deadlock, which it ends by closing the buses), and whether the run
//! can pause around them. Hence `WaitLiveness` and the
//! `wait_or_pause` protocol, in `crate::liveness`, which the bus plugs
//! into as one `WaitSource` among several.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::cancellation::CancellationFlag;
use crate::context::{PausePoint, Point};
use crate::error::{WeftError, WeftResult};
use crate::liveness::{no_liveness, wait_or_pause, FiringLocation, WaitLiveness, WaitSource};
use crate::run::saved::{BusPosition, Registration};

// ----- Public types ----------------------------------------------------

/// What a bus's messages carry, declared at creation and frozen: JSON
/// values (chat-shaped traffic) or raw bytes (media frames; no base64
/// wrapping between nodes). A consumer reads the kind off the handle
/// (or the marker) and knows how to speak before the first message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

impl BusEntry {
    /// A JSON message's kind and value: `None` for a membership entry, the
    /// close, or a bytes message.
    pub fn as_json(&self) -> Option<(&str, &Value)> {
        match &self.kind {
            BusEntryKind::Message { msg_kind, payload: Some(payload), .. } => payload.as_json().map(|value| (msg_kind.as_str(), value)),
            _ => None,
        }
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

    /// The payload's JSON value by move with its type; `None` for a byte
    /// payload. A bus declares no type for its messages, so a message's
    /// type is read off its own markers ([`WeftType::infer`]: a stored
    /// file is its file type, a secret a `Secret`, an access its
    /// service's `Access`). With it, a reader handles the message the
    /// way a typed input is handled: files found by
    /// [`crate::storage::media::map_stored_files`], secrets revealed by
    /// [`crate::context::RunContext::reveal_in`]. Every reader that does
    /// either reads its message's type here.
    ///
    /// [`WeftType::infer`]: crate::weft_type::WeftType::infer
    pub fn into_typed_json(self) -> Option<(Value, crate::weft_type::WeftType)> {
        let value = self.into_json()?;
        let ty = crate::weft_type::WeftType::infer(&value);
        Some((value, ty))
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
/// projects them to `RunEvent::Bus*` rows for replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all_fields = "camelCase")]
pub enum BusEntryKind {
    /// A handle registered `name`.
    Joined { name: String },
    /// A name left the open bus: its firing's body returned
    /// ([`BusInner::leave`]), or its handle outside every firing dropped.
    /// `end` is how it left, which is how the bus ends if nobody else
    /// joins it after ([`BusInner::end_unattended`]).
    Left { name: String, end: StreamEnd },
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
    /// Appended once at the close, saying how the bus ended. Cursors
    /// never surface it as a regular `Some(entry)`; `next()` returns
    /// `None` when it reaches this marker of a bus that finished, and the
    /// failure as an error for one that ended failed. Journaled so the inspector
    /// renders where the bus closed, or failed, instead of the panel
    /// just ending.
    Closed { end: StreamEnd },
}

/// How a bus ended: the same vocabulary a generator's stream ends with.
/// `Finished` when the body of its last member to leave returned
/// normally (or the bus was closed on purpose), `Failed` with that body's
/// error otherwise. Weft writes it ([`FiringBuses::leave`]); a node can
/// only end a bus as finished ([`BusHandle::close`]).
pub use crate::primitive::StreamEnd;

/// One message inside a journal window row: who sent it, when, and
/// what it said as far as the journal keeps it. Only the ROW
/// granularity is windowed; every message of a journaled bus gets its
/// own line here.
// SYNC: WindowedBusMessage <-> packages/weft-graph/src/protocol.ts BusInspectorEvent 'message', extension-vscode/src/runFollower.ts DispatcherEvent 'bus_window' messages
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowedBusMessage {
    pub offset: u64,
    pub from: String,
    pub msg_kind: String,
    /// What was said, when the journal keeps it at all: absent for a
    /// payload of raw bytes. A message bigger than the policy allows is
    /// still here, cut down, and `trimmed` says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<WirePayload>,
    /// The message's TRUE size, whatever was kept of it.
    pub payload_byte_size: u64,
    /// The payload here is a cut-down copy of a message too big to
    /// write down whole. Always alongside a payload: something is
    /// always kept, down to the first few thousand characters of the
    /// value written out as text.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub trimmed: bool,
    pub at_unix: u64,
}

/// One `(sender, kind)` rollup line of a journal window row: how many
/// messages and how many bytes passed. The whole story an ephemeral
/// bus journals; the summary line a journaled one carries besides its
/// messages.
// SYNC: BusWindowTotal <-> packages/weft-graph/src/protocol.ts BusInspectorEvent 'window' totals, extension-vscode/src/runFollower.ts DispatcherEvent 'bus_window' totals
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    /// Per-message detail; empty for an ephemeral bus, which journals
    /// the totals and nothing else.
    pub messages: Vec<WindowedBusMessage>,
    pub totals: Vec<BusWindowTotal>,
    /// Append time (unix seconds) of the LAST entry in the window: the
    /// truthful stamp for the journal row (the row describes the bus's
    /// own appends, not the moment the pump got around to flushing).
    pub last_at_unix: u64,
}

/// Fold one window's MESSAGE entries into the aggregate a journal row
/// carries, keeping of each payload what `policy` allows. Pure; the
/// pump owns the windowing clock, this owns the shape. Non-message
/// entries are the caller's bug (membership rows are journaled
/// individually, never windowed). Returns `None` on an empty window
/// (nothing to write).
pub fn aggregate_window(
    entries: &[BusEntry],
    policy: &crate::stream_journal::JournalPolicy,
) -> Option<BusWindowAggregate> {
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
        if !policy.is_ephemeral() {
            // What the journal keeps of a message is decided in one
            // place for every channel in the language, so a bus message
            // too big to write down is cut exactly as a caller's is,
            // and raw bytes are counted rather than copied.
            let entry_payload = payload
                .as_ref()
                .expect("a journaled bus's retained Message always carries its payload");
            let kept = crate::stream_journal::record(entry_payload, policy);
            messages.push(WindowedBusMessage {
                offset: entry.offset,
                from: from.clone(),
                msg_kind: msg_kind.clone(),
                payload: kept.payload,
                payload_byte_size: *payload_byte_size,
                trimmed: kept.trimmed,
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
    /// What the journal keeps of this bus: whether payloads are written
    /// down (journaled, the default) or only counted (ephemeral, where
    /// slow consumers also silently skip to the oldest retained entry),
    /// and the window one journal row gathers. NOTE: in EITHER mode the
    /// in-RAM log is a bounded window (cursors only ever read RAM, never
    /// the DB); the mode only decides whether the payload is also
    /// persisted, not whether RAM is bounded. A node whose users tune it
    /// passes `ctx.journal_policy()?` here.
    pub journal: crate::stream_journal::JournalPolicy,
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
    /// Creator-declared stream metadata, frozen at creation and
    /// readable by every consumer (off the handle or the marker):
    /// what a per-message header would otherwise repeat (an audio
    /// stream's sample rate and encoding, a video stream's dimensions).
    /// Keep it small; it rides the marker. `Null` = none.
    pub meta: Value,
}

/// Default in-RAM window for a bus (both modes). 64 entries fits the
/// common consumer-lags-a-few-messages case without growing RAM
/// unboundedly on a long stream. Override via `BusOptions::window`.
pub const DEFAULT_BUS_WINDOW: usize = 64;

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
    /// The firing already holds another name on this bus (through another
    /// handle, or held over its pause): a firing holds one name per bus,
    /// the one its saved state carries over a pause.
    #[error("this node already holds the name '{0}' on the bus; a node holds one name per bus")]
    FiringHoldsAnother(String),
    /// The body of the handle's firing already returned, taking its names
    /// off every bus ([`BusInner::leave`]): a task the body left running
    /// cannot join for it after.
    #[error("cannot register '{0}': this node's body already returned, and a node is on a bus only while its body runs")]
    FiringLeft(String),
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

    /// Whether the next insert evicts a payload. Caller holds the bus log
    /// lock.
    fn is_full(&self) -> bool {
        self.entries.lock().expect("ephemeral store poisoned").len() >= self.capacity
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
/// truth for ordering and content; `members` is a denormalized view of
/// who is on the bus now (kept in sync with the log's `Joined` / `Left`
/// entries under the same lock as appends).
///
/// `liveness` and `journal_pump_notify` are `Weak` on purpose: the
/// engine's `BusCoordinator` pins each bus via `BusRegistry::inner:
/// HashMap<Uuid, Arc<BusInner>>`, and an `Arc` back-reference from
/// the bus to the coordinator would form a cycle. `Weak` lets the
/// coordinator drop naturally; a wait/pump-notify observed after
/// coordinator drop is a no-op.
///
/// Public so the engine's per-run bus-journal-pump task can
/// hold `Weak<BusInner>` and call the engine-facing `pub fn`s in this
/// file's impl block. Handle-level state, options, and the log
/// itself stay private to this module.
pub struct BusInner {
    /// Stable channel identity. The JSON marker embeds this id;
    /// `BusRegistry::lookup` resolves a marker back to `Arc<BusInner>`
    /// via this id.
    id: Uuid,
    /// What the journal keeps of this bus (its mode and window).
    /// Frozen at creation.
    journal: crate::stream_journal::JournalPolicy,
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
    /// The offset the next entry lands at. Written under the log lock
    /// (`push_entry`); a bus opened again after a pause starts at the
    /// offset it stopped at, with no entry of its own yet.
    next_offset: AtomicU64,
    /// Every name that EVER registered, regardless of later `Left`
    /// entries. The single source of truth for membership waits
    /// (`wait_for*`). Maintained under the log lock.
    ever_joined: Mutex<HashSet<String>>,
    /// One past the newest message the bus no longer holds in RAM: one a
    /// bus opened again after a pause could not bring back (an ephemeral
    /// bus keeps none, a journaled one only its last window of messages
    /// kept whole), or one its window dropped as it went (raised by every
    /// trim, under the log lock). A resumed firing's cursor carrying on
    /// below it would read on over messages it never got
    /// ([`BusHandle::cursor_at`]). 0 for a bus that never dropped one.
    lost_through: AtomicU64,
    /// The filtered cursors of firings on this bus, carried past the
    /// entries their filters turn down just before the bus drops any
    /// ([`BusInner::carry_filtered_cursors`], which locks it inside the
    /// log lock; nothing holding it takes the log lock).
    filtered_cursors: Mutex<Vec<Weak<CursorSlot>>>,
    /// Highest offset the pump has successfully shipped to the
    /// journal, +1. Reads as `journaled_through`; the pump pulls
    /// `log[journaled_through..]` per drain. Atomic so the bus can
    /// read it without holding the log lock.
    journaled_through: AtomicU64,
    /// Set once when the bus is closed. Cursors return `None` after
    /// reaching the `Closed` entry; further sends error.
    closed: AtomicBool,
    /// How the bus ended, set under the log lock just before `closed`.
    end: std::sync::OnceLock<StreamEnd>,
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
    /// Who is on the bus now, by name: every name whose `Joined` has no
    /// `Left` yet, the same set the run's record folds from those entries
    /// (`RecordedBus::members`), which is what a bus opened again after a
    /// pause starts from. When the last one leaves, the bus ends
    /// ([`BusInner::leave`]). Kept in sync with the log under the same
    /// `log` lock, so a `register` / leave racing `close()` cannot produce
    /// a `Joined` without its entry.
    members: Mutex<HashMap<String, Member>>,
    /// How the last name to leave left (its `Left` entry's end), `None`
    /// while none has: how the bus ends once nobody can still join it
    /// ([`Self::end_unattended`]). Written under the `log` lock.
    last_left: Mutex<Option<StreamEnd>>,
    /// The firings whose bodies used this bus (registered on it, opened
    /// its reader, or waited on it) and have not returned: a firing
    /// holding the marker in its input that used the bus already is no
    /// node that may still join it ([`crate::run::bus_reach`]).
    used_by: Mutex<HashSet<FiringLocation>>,
    /// Engine node-liveness hook. `Weak::new()` outside an engine-driven
    /// run; set once at bus creation via
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
    /// Per-run journal pump wake. The bus ping it after every
    /// append; the pump wakes, walks every live bus, drains tails.
    /// `Weak::new()` outside an engine-driven run.
    journal_pump_notify: Weak<Notify>,
    /// Optional ephemeral payload store. `Some` iff the journal mode is
    /// ephemeral.
    ephemeral_store: Option<EphemeralStore>,
    /// What the messages carry (JSON vs raw bytes). Frozen at creation.
    payload_kind: BusPayloadKind,
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

/// One name on a bus ([`BusInner::members`]).
struct Member {
    /// The firing whose body registered it, `None` for a handle outside
    /// every firing (a test's, a bus seeded from outside a body). A
    /// firing's name stays its own until its body returns, through every
    /// handle it drops and every pause; a name outside a firing leaves
    /// with its handle.
    firing: Option<FiringLocation>,
    /// The offset of its `Joined` entry: where a handle taking the name
    /// back anchors its reader.
    joined_offset: u64,
    /// A live handle carries it now. A firing's name whose handle went
    /// (dropped mid-body, or by a pause) is taken back by the next handle
    /// of the same firing registering it, with nothing appended.
    in_hand: bool,
}

/// A bus to open again where a paused run left it
/// ([`BusHandle::reopen`]), as the run's record tells it.
pub struct ReopenedBus {
    pub id: Uuid,
    pub opts: BusOptions,
    /// The offset its next entry lands at.
    pub next_offset: u64,
    /// Every name that ever joined it.
    pub joined: Vec<String>,
    /// Who was on it when the run paused: each name, the paused firing
    /// it belongs to, and the offset of its `Joined` entry. Every one of
    /// them is a firing that paused, since a run pauses only once every
    /// node of it waits, and each body that returned before took its
    /// names off with it.
    pub members: Vec<(String, FiringLocation, u64)>,
    /// The messages a journaled bus's record kept whole (each with its
    /// payload, untrimmed), oldest first, at most its window, every one
    /// after `lost_through`.
    pub retained: Vec<WindowedBusMessage>,
    /// One past the newest message it cannot bring back
    /// ([`BusInner::lost_through`]).
    pub lost_through: u64,
    /// How it ended before the pause, `None` for a bus still open: an
    /// ended one comes back ended, its `Closed` entry at `next_offset`
    /// and already on the record.
    pub end: Option<StreamEnd>,
    /// How the last name to leave it left, `None` while none has: how an
    /// open bus nobody is on ends once no node can still join it.
    pub last_left: Option<StreamEnd>,
}

impl BusInner {
    /// An empty bus `id`, opened with `opts` and the engine's hooks.
    /// Errors on an invalid `window` (must be >= 1) or a journal window
    /// out of range ([`crate::stream_journal::JournalPolicy::check`]).
    fn new(id: Uuid, opts: &BusOptions, liveness: Weak<dyn WaitLiveness>, journal_pump_notify: Weak<Notify>) -> Result<Self, String> {
        let window = opts.window.unwrap_or(DEFAULT_BUS_WINDOW);
        if window == 0 {
            return Err("bus window must be >= 1; 0 would evict every entry before any \
                        cursor could read it"
                .into());
        }
        opts.journal.check().map_err(|why| format!("bus: {why}"))?;
        let ephemeral_store = if opts.journal.is_ephemeral() {
            Some(EphemeralStore::new(window)?)
        } else {
            None
        };
        Ok(BusInner {
            id,
            journal: opts.journal,
            log: Mutex::new(Vec::new()),
            next_offset: AtomicU64::new(0),
            ever_joined: Mutex::new(HashSet::new()),
            lost_through: AtomicU64::new(0),
            filtered_cursors: Mutex::new(Vec::new()),
            journaled_through: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            end: std::sync::OnceLock::new(),
            journal_degraded: AtomicBool::new(false),
            degraded_reason: Mutex::new(String::new()),
            log_notify: Notify::new(),
            append_gen: AtomicU64::new(0),
            members: Mutex::new(HashMap::new()),
            last_left: Mutex::new(None),
            used_by: Mutex::new(HashSet::new()),
            liveness,
            journal_pump_notify,
            ephemeral_store,
            payload_kind: opts.payload,
            meta: opts.meta.clone(),
            window,
        })
    }

    /// Lock the log, recovering from poisoning. The only user code
    /// that ever runs under this lock is a cursor's filter closure,
    /// and that section is read-only: a panicking filter leaves the
    /// log structurally intact, so recovering the data is sound.
    /// Without recovery, one panicking filter would poison the mutex,
    /// every other participant (including `BusHandle::drop`, which
    /// runs DURING the panicking task's unwind) would panic on the
    /// lock, and a panic-during-unwind aborts the whole worker
    /// process, killing every run on the process.
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
    /// the entry landed at, the bus's `next_offset` (never the log's
    /// length: eviction removes interior entries, and a bus opened again
    /// after a pause starts past entries it no longer holds).
    fn push_entry(
        &self,
        log: &mut Vec<BusEntry>,
        kind: BusEntryKind,
    ) -> u64 {
        let offset = self.next_offset.fetch_add(1, Ordering::AcqRel);
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
        self.carry_filtered_cursors(log);
        let mut dropped_through = 0u64;
        log.retain(|e| {
            // Always keep membership entries and anything in the window.
            // A Message below the window: drop ONLY if already journaled.
            // An un-shipped message stays so the pump can still drain it
            // onto the record (durability before RAM bound).
            let keep = !matches!(e.kind, BusEntryKind::Message { .. })
                || e.offset >= window_floor
                || e.offset >= journaled_through;
            if !keep {
                dropped_through = dropped_through.max(e.offset + 1);
            }
            keep
        });
        self.lost_through.fetch_max(dropped_through, Ordering::AcqRel);
    }

    /// `entry` with its payload, when it is a retained message of an
    /// ephemeral bus (whose log entry carries none); `None` for any other
    /// entry, which carries what a reader sees. Caller holds the log lock.
    fn resolve_ephemeral(&self, entry: &BusEntry) -> Option<BusEntry> {
        let BusEntryKind::Message { from, msg_kind, payload: None, payload_byte_size } = &entry.kind else {
            return None;
        };
        let store = self.ephemeral_store.as_ref().expect(
            "Message with payload=None on a non-ephemeral bus is impossible \
             by construction (send routes ephemeral payloads into the store)",
        );
        // A RETAINED Message always has a resident payload (send evicts
        // the log entry in the same locked section that evicts the
        // payload); a miss panics inside `get_resident`.
        Some(BusEntry {
            offset: entry.offset,
            at_unix: entry.at_unix,
            kind: BusEntryKind::Message {
                from: from.clone(),
                msg_kind: msg_kind.clone(),
                payload: Some(store.get_resident(entry.offset)),
                payload_byte_size: *payload_byte_size,
            },
        })
    }

    /// Where a cursor at `from` with `filter` would look next: carried
    /// over the entries right after it that the filter turns down, as its
    /// read would, stopping at a `Closed` entry, at the tail, and at a
    /// message the bus no longer holds (a gap is never bridged here: a
    /// dropped message may be one the reader wanted).
    fn past_turned_down(&self, from: u64, filter: &CursorFilter) -> u64 {
        let log = self.lock_log();
        self.past_turned_down_in(&log, from, filter)
    }

    /// [`Self::past_turned_down`] over `log`, which the caller holds
    /// locked.
    fn past_turned_down_in(&self, log: &[BusEntry], from: u64, filter: &CursorFilter) -> u64 {
        let mut at = from;
        let mut idx = log.partition_point(|e| e.offset < from);
        while let Some(entry) = log.get(idx) {
            if entry.offset != at || matches!(entry.kind, BusEntryKind::Closed { .. }) {
                break;
            }
            let resolved = self.resolve_ephemeral(entry);
            // A reader's filter runs here on the sender's thread: one that
            // panics stops the carry (the reader meets the panic on its own
            // read) instead of failing the sender.
            let turned_down = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| !filter(resolved.as_ref().unwrap_or(entry))));
            if !turned_down.unwrap_or(false) {
                break;
            }
            at += 1;
            idx += 1;
        }
        at
    }

    /// Carry every filtered cursor of a firing past the entries right
    /// after it that its filter turns down, just before the bus drops
    /// messages from `log` (held locked, every message in it still
    /// readable): a reader filtering out its own messages, whose firing
    /// pauses after sending more than the window holds, is saved past
    /// them rather than behind a gap ([`CursorSlot::saved_offset`]).
    fn carry_filtered_cursors(&self, log: &[BusEntry]) {
        self.filtered_cursors.lock().unwrap_or_else(std::sync::PoisonError::into_inner).retain(|slot| {
            let Some(slot) = slot.upgrade() else { return false };
            let filter = slot.filter.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
            if let Some(filter) = filter {
                let carried = self.past_turned_down_in(log, slot.position(), &filter);
                slot.carried.fetch_max(carried, Ordering::AcqRel);
            }
            true
        });
    }

    /// Close this bus as `end`. Appends a `Closed` entry carrying it
    /// (idempotent: the first close's end stands), sets the closed
    /// flag, wakes every cursor waiting on the log and the engine's
    /// liveness hook. Public so the engine can fire the stuck-detection
    /// close via a raw `Arc<BusInner>` it pins for the pump's lifetime.
    pub fn close(&self, end: StreamEnd) {
        let mut log = self.lock_log();
        self.close_in(&mut log, end);
    }

    /// [`Self::close`] with the log already locked.
    fn close_in(&self, log: &mut Vec<BusEntry>, end: StreamEnd) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        // Under the log lock, before the flag: whoever reads `closed`
        // also reads the end.
        let _ = self.end.set(end.clone());
        self.closed.store(true, Ordering::Release);
        self.push_entry(log, BusEntryKind::Closed { end });
    }

    /// The firing at `at` left: its body returned (`end` says how), so
    /// every name it has on the bus leaves, each with its `Left` entry,
    /// whether a handle still carries it or not, and it no longer counts
    /// as having used the bus. Nothing else happens to the bus, unless
    /// that left nobody on it: then it ends once no node can still join
    /// it ([`Self::unattended`]), as the last name to leave left. A firing
    /// with no name on the bus leaves none, and tells of a bus it made
    /// with nobody on it.
    pub fn leave(&self, at: &FiringLocation, end: StreamEnd) {
        self.used_by.lock().expect("bus users poisoned").remove(at);
        let mut log = self.lock_log();
        self.leave_in(&mut log, |_, member| member.firing.as_ref() == Some(at), end);
    }

    /// Take every member `which` picks off the bus, each with its `Left`
    /// entry carrying `end`. The one way a name leaves: a firing's body
    /// returning ([`Self::leave`]) and a handle outside every firing
    /// dropping ([`BusHandle`]'s drop) both come through here. A bus left
    /// with nobody on it is unattended ([`Self::unattended`]), whether a
    /// name just left or none was there. A closed bus takes no entry any
    /// more, so nothing is written on one.
    fn leave_in(&self, log: &mut Vec<BusEntry>, which: impl Fn(&str, &Member) -> bool, end: StreamEnd) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let (gone, nobody_left) = {
            let mut members = self.members.lock().expect("bus members poisoned");
            let gone: Vec<String> = members.iter().filter(|(name, member)| which(name, member)).map(|(name, _)| name.clone()).collect();
            for name in &gone {
                members.remove(name);
            }
            (gone, members.is_empty())
        };
        if !gone.is_empty() {
            *self.last_left.lock().expect("bus last leave poisoned") = Some(end.clone());
        }
        let left_now = !gone.is_empty();
        for name in gone {
            self.push_entry(log, BusEntryKind::Left { name, end: end.clone() });
        }
        if !nobody_left {
            return;
        }
        match self.liveness.upgrade() {
            Some(engine) => engine.unattended(self.id),
            // Outside a run (a test's bus, a run whose engine is gone) no
            // node can still join it, so the last name leaving ends it.
            None if left_now => self.close_in(log, end),
            None => {}
        }
    }

    /// End the bus if nobody is on it, as the last name to leave left
    /// (finished when none ever did): the engine's call, once no node of
    /// the run can still join it ([`crate::run::bus_reach`]). A bus a node
    /// joined meanwhile stays open. Answers whether it ended now.
    pub fn end_unattended(&self) -> bool {
        let mut log = self.lock_log();
        if self.closed.load(Ordering::Acquire) || !self.members.lock().expect("bus members poisoned").is_empty() {
            return false;
        }
        let end = self.last_left.lock().expect("bus last leave poisoned").clone().unwrap_or(StreamEnd::Finished);
        self.close_in(&mut log, end);
        true
    }

    /// Whether anybody is on the bus.
    pub fn attended(&self) -> bool {
        !self.members.lock().expect("bus members poisoned").is_empty()
    }

    /// The firing at `at` used the bus (registered on it, opened its
    /// reader, or waited on it): from here on, holding its marker is no
    /// reason for the bus to stay open. With nobody on the bus the engine
    /// is told again, since this may be what kept it open.
    fn used(&self, at: &FiringLocation) {
        if !self.used_by.lock().expect("bus users poisoned").insert(at.clone()) {
            return;
        }
        if !self.is_closed() && !self.attended() {
            if let Some(engine) = self.liveness.upgrade() {
                engine.unattended(self.id);
            }
        }
    }

    /// Whether the firing at `at` used the bus ([`Self::used`]) in a body
    /// that has not returned.
    pub fn used_by(&self, at: &FiringLocation) -> bool {
        self.used_by.lock().expect("bus users poisoned").contains(at)
    }

    /// How the bus ended, `None` while it is open.
    pub fn end(&self) -> Option<StreamEnd> {
        self.end.get().cloned()
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
        // CONSTRUCTION rather than relying on a single-pump-per-run
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

    /// What the journal keeps of this bus. The pump's decision to close
    /// a window and the fold's decision on what to keep both read it, so
    /// they cannot come from two different policies.
    pub fn journal(&self) -> crate::stream_journal::JournalPolicy {
        self.journal
    }

    /// What the messages carry (JSON vs raw bytes).
    pub fn payload_kind(&self) -> BusPayloadKind {
        self.payload_kind
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
    /// to drain. Snapshot under the log lock.
    pub fn log_len(&self) -> u64 {
        let _log = self.lock_log();
        self.next_offset.load(Ordering::Acquire)
    }

    /// The earliest offset still resident in the in-RAM log (the floor a
    /// cursor can reach; anything below it was trimmed out of RAM and is not
    /// readable by a cursor, by design cursors never read the DB). The next
    /// offset on an empty log.
    pub fn retained_floor(&self) -> u64 {
        let log = self.lock_log();
        log.first().map(|e| e.offset).unwrap_or_else(|| self.next_offset.load(Ordering::Acquire))
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
    /// The FIRING that minted this handle (`(node_id, frames)`),
    /// or `None` for a bus created outside an engine-driven node (unit
    /// tests, `BusHandle::create`). Used to key the node's bus liveness:
    /// every wait this handle enters, and the participant membership it
    /// registers, are attributed to this firing. Forked handles
    /// (`new_handle`) inherit it, because a forked handle is still the
    /// SAME node grabbing a second grip on the bus.
    node: Option<FiringLocation>,
    /// That firing's side of every bus it touches ([`FiringBuses`]): where
    /// its one reader on the bus starts and how far it read, the name it
    /// holds, whether its body returned. `None` outside a firing.
    firing: Option<Arc<FiringBuses>>,
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
            firing: self.firing.clone(),
            registration: None,
            node: self.node.clone(),
        }
    }
}

impl BusHandle {
    /// Create a fresh bus with default options and no engine hooks.
    /// Used in unit tests and code outside an engine-driven run.
    pub fn create() -> Self {
        Self::create_with_options(BusOptions::default())
            .expect("default BusOptions cannot fail")
    }

    /// Create with explicit options, no engine hooks. Errors on an
    /// invalid `window` or journal window.
    pub fn create_with_options(opts: BusOptions) -> Result<Self, String> {
        Self::create_with_engine(opts, no_liveness(), Weak::<Notify>::new(), None, None)
    }

    /// Create with options + engine hooks. The liveness hook fires
    /// whenever this node's cursor enters/leaves a wait and on every
    /// append; the pump notify fires after every append to signal the
    /// engine's bus-journal-pump task. `node` is the minting firing and
    /// `firing` its [`FiringBuses`] (`None` outside an engine-driven
    /// node). Errors on an invalid `window` (must be >= 1) or a journal
    /// window out of range
    /// ([`crate::stream_journal::JournalPolicy::check`]).
    pub fn create_with_engine(
        opts: BusOptions,
        liveness: Weak<dyn WaitLiveness>,
        journal_pump_notify: Weak<Notify>,
        node: Option<FiringLocation>,
        firing: Option<Arc<FiringBuses>>,
    ) -> Result<Self, String> {
        let inner = BusInner::new(Uuid::new_v4(), &opts, liveness, journal_pump_notify)?;
        Ok(Self { inner: Arc::new(inner), registration: None, node, firing })
    }

    /// Open again a bus a run paused with, where it stood
    /// ([`ReopenedBus`]): the same id, so every marker handed out still
    /// names it, the next entry at the offset the bus stopped at,
    /// everything before it already on the run's record, its members
    /// on it again, each its paused firing's to take back, and
    /// for a journaled bus the messages its record kept whole, so a reader
    /// reaching back finds them. A bus that ended before the pause comes
    /// back ended the same way: its readers read what it kept and come to
    /// its end, and a close or a register meets an ended bus, as they
    /// would have before the pause. The handle is the run's, attributed to
    /// no firing; the bodies reach the bus by marker or port as before.
    pub fn reopen(
        bus: ReopenedBus,
        liveness: Weak<dyn WaitLiveness>,
        journal_pump_notify: Weak<Notify>,
    ) -> Result<Self, String> {
        let inner = BusInner::new(bus.id, &bus.opts, liveness, journal_pump_notify)?;
        {
            let mut log = inner.lock_log();
            for message in bus.retained {
                let Some(payload) = message.payload else {
                    return Err(format!("its record kept message {} without its payload, which cannot be read again", message.offset));
                };
                log.push(BusEntry {
                    offset: message.offset,
                    at_unix: message.at_unix,
                    kind: BusEntryKind::Message {
                        from: message.from,
                        msg_kind: message.msg_kind,
                        payload: Some(payload),
                        payload_byte_size: message.payload_byte_size,
                    },
                });
            }
            let mut next_offset = bus.next_offset;
            if let Some(end) = bus.end {
                // Its `Closed` entry, already on the record: past it, so
                // the pump writes it no second time.
                log.push(BusEntry { offset: next_offset, at_unix: now_unix(), kind: BusEntryKind::Closed { end: end.clone() } });
                let _ = inner.end.set(end);
                inner.closed.store(true, Ordering::Release);
                next_offset += 1;
            }
            inner.next_offset.store(next_offset, Ordering::Release);
            inner.journaled_through.store(next_offset, Ordering::Release);
            inner.lost_through.store(bus.lost_through, Ordering::Release);
            *inner.ever_joined.lock().expect("ever_joined poisoned") = bus.joined.into_iter().collect();
            *inner.last_left.lock().expect("bus last leave poisoned") = bus.last_left;
            if !inner.closed.load(Ordering::Acquire) {
                let mut members = inner.members.lock().expect("bus members poisoned");
                for (name, firing, joined_offset) in bus.members {
                    members.insert(name, Member { firing: Some(firing), joined_offset, in_hand: false });
                }
            }
        }
        // Open with nobody on it (its last member left, or it never had
        // one, before the pause): the engine looks again at whether a node
        // can still join it.
        if !inner.is_closed() && !inner.attended() {
            if let Some(engine) = inner.liveness.upgrade() {
                engine.unattended(inner.id);
            }
        }
        Ok(Self { inner: Arc::new(inner), registration: None, node: None, firing: None })
    }

    /// Build a handle from an existing inner, attributed to the firing
    /// `node` resolving the marker (and its [`FiringBuses`]). Used by
    /// `BusRegistry::lookup` to hand a consumer a fresh handle on a bus
    /// the producer already created; the consumer's own firing (not the
    /// producer's) keys its liveness and its positions.
    pub(crate) fn from_inner(inner: Arc<BusInner>, node: Option<FiringLocation>, firing: Option<Arc<FiringBuses>>) -> Self {
        Self {
            inner,
            registration: None,
            node,
            firing,
        }
    }

    /// Stable channel identity.
    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    /// Whether this bus runs in ephemeral mode.
    pub fn is_ephemeral(&self) -> bool {
        self.inner.journal.is_ephemeral()
    }

    /// What the journal keeps of this bus: its mode and window, as it
    /// was opened with them.
    pub fn journal(&self) -> crate::stream_journal::JournalPolicy {
        self.inner.journal
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

    /// Claim `name` as this handle's identity, which makes its firing a
    /// member of the bus until its body returns ([`BusInner::leave`]).
    /// Errors if the bus is closed (`Closed`), the name is already held
    /// (`NameTaken`), this handle already registered
    /// (`AlreadyRegistered`), or its firing's body already returned
    /// (`FiringLeft`).
    ///
    /// The append to the log AND the `members` insert AND the
    /// closed-flag re-check all happen under the SAME `log` lock. A
    /// `close()` in flight either lands before (this register sees
    /// `closed=true` and errors), or after (this register has finished
    /// inserting, the close appends the `Closed` entry next). No
    /// register-then-close race producing an orphan Joined.
    ///
    /// A name this handle's firing already has on the bus with no handle
    /// carrying it (one it dropped, or held over its pause) is taken back
    /// as it was, at its first `Joined` offset, with nothing appended: to
    /// its peers the firing never left. That holds on a bus that ended
    /// while the firing was paused too: the resumed body gets its name
    /// back on the ended bus, and only a name it never held is refused
    /// (`Closed`).
    pub fn register(&mut self, name: impl Into<String>) -> Result<(), RegisterError> {
        if let Some((existing, _)) = &self.registration {
            return Err(RegisterError::AlreadyRegistered(existing.clone()));
        }
        let name = name.into();
        let mut log = self.inner.lock_log();
        if let Some(firing) = &self.firing {
            if firing.has_left() {
                return Err(RegisterError::FiringLeft(name));
            }
            if let Some(held) = firing.held_name(self.inner.id) {
                if held != name {
                    return Err(RegisterError::FiringHoldsAnother(held));
                }
            }
        }
        if self.inner.closed.load(Ordering::Acquire) {
            // The bus ended while the firing was paused holding this name
            // (here, or before the run was picked up): the resumed body
            // gets the name back as it held it, the bus ended, so its
            // reader comes to that end. Only a name it never held is
            // refused.
            let Some(held) = self.firing.as_ref().and_then(|firing| firing.held_over_its_pause(self.inner.id)).filter(|held| held.name == name)
            else {
                return Err(RegisterError::Closed);
            };
            drop(log);
            self.note_registered(&name, held.joined_offset);
            self.registration = Some((name, held.joined_offset));
            self.note_use();
            return Ok(());
        }
        let firing = self.member_firing();
        let joined_offset = {
            let mut members = self.inner.members.lock().expect("bus members poisoned");
            match members.get_mut(&name) {
                Some(member) if !member.in_hand && firing.is_some() && member.firing == firing => {
                    member.in_hand = true;
                    Some(member.joined_offset)
                }
                Some(_) => return Err(RegisterError::NameTaken(name)),
                None => None,
            }
        };
        let joined_offset = match joined_offset {
            Some(taken_back) => taken_back,
            None => {
                self.inner.ever_joined.lock().expect("ever_joined poisoned").insert(name.clone());
                let joined_offset = self.inner.push_entry(&mut log, BusEntryKind::Joined { name: name.clone() });
                self.inner.members.lock().expect("bus members poisoned").insert(name.clone(), Member { firing, joined_offset, in_hand: true });
                joined_offset
            }
        };
        drop(log);
        self.note_registered(&name, joined_offset);
        self.registration = Some((name, joined_offset));
        self.note_use();
        Ok(())
    }

    /// The firing a name this handle registers belongs to: the handle's
    /// own, `None` for a handle outside every firing.
    fn member_firing(&self) -> Option<FiringLocation> {
        self.firing.as_ref().and(self.node.clone())
    }

    /// This handle's firing used the bus (registered on it, opened its
    /// reader, or waited on it): holding its marker no longer keeps the
    /// bus open for it ([`BusInner::used`]), and its body returning takes
    /// that back ([`FiringBuses::leave`]).
    fn note_use(&self) {
        if let (Some(firing), Some(at)) = (&self.firing, &self.node) {
            firing.touched(self.inner.id);
            self.inner.used(at);
        }
    }

    /// Tell this handle's firing it holds `name` on this bus, for its
    /// saved positions.
    fn note_registered(&self, name: &str, joined_offset: u64) {
        if let Some(firing) = &self.firing {
            firing.registered(self.inner.id, Registration { name: name.to_string(), joined_offset });
        }
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
        let entry_payload = if self.inner.journal.is_ephemeral() { None } else { Some(payload.clone()) };
        let mut log = self.inner.lock_log();
        // Check Closed BEFORE JournalDegraded: a closed bus is the more
        // fundamental failure (no further sends will ever succeed),
        // and a caller retrying after the pump cleared the degraded
        // flag should not get a fresh error type for the same
        // impossible-to-satisfy send.
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed);
        }
        if !self.inner.journal.is_ephemeral() && self.inner.is_journal_degraded() {
            let reason = self
                .inner
                .degraded_reason
                .lock()
                .expect("degraded_reason poisoned")
                .clone();
            return Err(SendError::JournalDegraded(reason));
        }
        // This send evicts a payload: carry the filtered cursors while
        // every message in the log still has its own.
        if self.inner.ephemeral_store.as_ref().is_some_and(EphemeralStore::is_full) {
            self.inner.carry_filtered_cursors(&log);
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
                let mut dropped_through = 0u64;
                log.retain(|e| {
                    let keep = e.offset >= floor
                        || !matches!(e.kind, BusEntryKind::Message { .. });
                    if !keep {
                        dropped_through = dropped_through.max(e.offset + 1);
                        if e.offset >= journaled_through {
                            lost_unjournaled += 1;
                        }
                    }
                    keep
                });
                self.inner.lost_through.fetch_max(dropped_through, Ordering::AcqRel);
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
            // (`offset < journaled_through`, so the record holds them) AND
            // (b) older than the most recent `window` messages.
            // Membership entries (Joined/Left/Closed) are always retained
            // (load-bearing for cursors, bounded by participant churn). A
            // cursor that had pointed into the trimmed span silently
            // resumes at the next retained entry, the same RAM-only
            // semantics ephemeral has: a cursor never reaches the DB,
            // the window is the whole world.
            self.inner.trim_journaled_window(&mut log);
        }
        drop(log);
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
    ///
    /// A firing opens one reader per bus a run of its body (this one,
    /// [`Self::cursor_at`] or [`Self::cursor_from_start`]): a second is
    /// refused. In a resumed firing it starts where its reader stopped
    /// before the pause.
    #[track_caller]
    pub fn cursor(&self) -> WeftResult<BusCursor> {
        let anchor = match &self.registration {
            Some((_, joined_offset)) => *joined_offset,
            None => self.inner.log_len(),
        };
        self.cursor_at(anchor)
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
    ///
    /// A resumed firing's cursor carrying on from where it stopped before
    /// the pause, below a message the bus no longer holds
    /// ([`BusInner::lost_through`]: one it came back from the pause
    /// without, or one its window dropped while the firing was paused)
    /// would read on over messages it never got: its first read fails
    /// instead. A reader the body opens fresh starts like any other.
    #[track_caller]
    pub fn cursor_at(&self, offset: u64) -> WeftResult<BusCursor> {
        let at = std::panic::Location::caller();
        let (start, slot) = match &self.firing {
            Some(firing) => {
                let (start, slot) = firing.open_cursor(&self.inner, offset, at).map_err(|first| {
                    WeftError::NodeFailed(format!(
                        "node '{}' opened a second reader on a bus at {}:{}:{}, which it already opened one on at \
                         {}:{}:{}: a node keeps one reader per bus, so that it can come back where it stopped after a \
                         pause. Read every message through that one reader, filtering by sender or kind in the node's \
                         code",
                        self.node.as_ref().map_or("?", |node| node.node_id.as_str()),
                        at.file(),
                        at.line(),
                        at.column(),
                        first.file(),
                        first.line(),
                        first.column(),
                    ))
                })?;
                (start, Some(slot))
            }
            None => (CursorStart::Fresh(offset), None),
        };
        self.note_use();
        let behind = matches!(start, CursorStart::Saved(saved) if saved < self.inner.lost_through.load(Ordering::Acquire));
        let cancellation = self.firing.as_ref().and_then(|firing| firing.cancellation.clone());
        Ok(BusCursor::new(Arc::downgrade(&self.inner), self.inner.id, start.offset(), None, self.node.clone(), slot, behind, cancellation))
    }

    /// A fresh cursor from the earliest entry STILL RETAINED in RAM. Not
    /// "offset 0": on a windowed bus the early entries may have been
    /// trimmed (journaled ones live on in the DB, but a cursor never reads
    /// the DB). This reads everything a cursor can still reach, oldest
    /// first.
    #[track_caller]
    pub fn cursor_from_start(&self) -> WeftResult<BusCursor> {
        self.cursor_at(self.inner.retained_floor())
    }

    // ----- membership wait -----------------------------------------

    /// Wait until `name` has joined this bus at any point in its
    /// history. Resolves immediately if `name` already appeared
    /// (past-aware: membership survives truncation and a pause). Fails
    /// if the bus closes before the wait is satisfied. The run can pause
    /// while it waits, so it takes the firing's state; a resumed body gets
    /// past it at once, since the list of who joined comes back with the
    /// bus. Not a read, so the rule of `crate::context::Places` does not
    /// count it.
    #[track_caller]
    pub fn wait_for<'a, P: PausePoint>(&'a self, name: &'a str, at: &'a mut P) -> impl std::future::Future<Output = WeftResult<()>> + 'a {
        self.wait_for_at(name, at, Point::unchecked("waited for a member of a bus", std::panic::Location::caller()))
    }

    async fn wait_for_at<P: PausePoint>(&self, name: &str, at: &mut P, point: Point) -> WeftResult<()> {
        at.before(&point, self.node.is_some())?;
        self.note_use();
        let name_owned = name.to_string();
        match self.wait_for_joins(at.pausable(), move |seen| seen.contains(&name_owned)).await {
            Some(Ok(())) => Ok(()),
            Some(Err(())) => Err(WeftError::NodeFailed(format!("the bus closed before '{name}' joined it"))),
            None => Err(at.paused().await),
        }
    }

    /// Wait until the bus ends, for a node that keeps a bus open while
    /// others use it and has nothing to read on it: a member waiting for
    /// the node that closes it. `Ok` once it ended finished; an error once
    /// it ended failed (`the bus ended failed: <why>`), as a reader's read
    /// fails there, so the `?` fails the node rather than letting a broken
    /// stream pass as a whole one (a body handling the failure itself
    /// matches the error, and [`Self::end`] says why).
    ///
    /// The run can pause while it waits, so it takes the firing's state
    /// (or the `Holding`). It is no reader: it keeps no place on the bus,
    /// so a resumed body waits on, however many messages the bus dropped
    /// meanwhile, and the firing may still open its one reader on the bus.
    /// Ends with the cancelled error when the run is cancelled.
    #[track_caller]
    pub fn until_end<'a, P: PausePoint>(&'a self, at: &'a mut P) -> impl std::future::Future<Output = WeftResult<()>> + 'a {
        self.until_end_at(at, Point::unchecked("waited for a bus to end", std::panic::Location::caller()))
    }

    async fn until_end_at<P: PausePoint>(&self, at: &mut P, point: Point) -> WeftResult<()> {
        at.before(&point, self.node.is_some())?;
        self.note_use();
        let inner = self.inner.clone();
        let check = move || -> Option<()> {
            let _log = inner.lock_log();
            inner.closed.load(Ordering::Acquire).then_some(())
        };
        let source = self.inner.clone() as Arc<dyn WaitSource>;
        let wait = wait_or_pause(&self.inner.liveness, &self.node, &source, at.pausable(), check);
        let cancellation = self.firing.as_ref().and_then(|firing| firing.cancellation.clone());
        let ended = match cancellation.as_deref() {
            Some(cancellation) => tokio::select! {
                biased;
                () = cancellation.cancelled() => None,
                ended = wait => Some(ended),
            },
            None => Some(wait.await),
        };
        match ended {
            None => Err(WeftError::Cancelled),
            Some(Ok(())) => match self.inner.end() {
                Some(StreamEnd::Failed { error }) => Err(WeftError::NodeFailed(format!("the bus ended failed: {error}"))),
                Some(StreamEnd::Finished) | None => Ok(()),
            },
            Some(Err(crate::liveness::Paused)) => Err(at.paused().await),
        }
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
    /// Runs the shared wait protocol (`liveness::wait_or_pause`), which
    /// holds exactly ONE wait registration (one `WaitId`) across the
    /// entire call: the wait's observed/parked state in the engine's
    /// liveness map stays continuous across wakes instead of being
    /// re-minted at the conservative not-parked baseline on each wake,
    /// which would keep deferring a provable deadlock.
    /// `Some(Err(()))` when the bus closed first, `None` when the run
    /// paused.
    async fn wait_for_joins<F>(&self, pausable: bool, mut pred: F) -> Option<Result<(), ()>>
    where
        F: FnMut(&HashSet<String>) -> bool + Send + Sync,
    {
        // Predicate-first, closed-second: a join that landed before
        // the close still satisfies the wait (matching the old
        // log-scan order, where a Joined entry preceding the Closed
        // entry was found first). Both reads happen under the log
        // lock so they see one consistent snapshot (register and
        // close both mutate under it).
        let check = || -> Option<Result<(), ()>> {
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
                return Some(Err(()));
            }
            None
        };
        // The shared wait protocol (`liveness::wait_or_pause`) holds ONE
        // guard for the whole wait, arms before every check (no lost
        // wakeup) and records observed under the same generation
        // discipline the stuck-check relies on.
        let source = self.inner.clone() as Arc<dyn WaitSource>;
        wait_or_pause(&self.inner.liveness, &self.node, &source, pausable, check).await.ok()
    }

    // ----- lifecycle ---------------------------------------------

    /// End the bus now: "no more messages", whoever calls it and whoever
    /// is still on it. With somebody on it, it ends finished. With nobody
    /// on it, it ends as it would have ended itself: as its last member
    /// left, so a stream whose producer failed halfway still reads as
    /// failed however a node later closes it (finished when nobody ever
    /// joined). A failed end is never a node's to write; weft writes it
    /// from how the body of the last member to leave returned
    /// ([`FiringBuses::leave`]).
    pub fn close(&self) {
        if !self.inner.end_unattended() {
            self.inner.close(StreamEnd::Finished);
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// How the bus ended, `None` while it is open. What a reader asks
    /// once its cursor has run out: a `Failed` end is a stream that did
    /// not complete, and the reader says so in its own protocol.
    pub fn end(&self) -> Option<StreamEnd> {
        self.inner.end()
    }

    /// JSON marker for THIS bus. The marker is what a producer puts
    /// on an output port; consumers resolve it back via `ctx.bus`.
    pub fn marker(&self) -> Value {
        crate::weft_type::WeftType::bus_marker(&self.inner.id.to_string(), self.inner.journal.mode)
    }
}

/// One firing's side of every bus it touches. Two jobs:
///
/// - Taking its names off every bus when its body returns
///   ([`Self::leave`]), however it returns: a firing that registered a
///   name on a bus (by creating it, `ctx.open_bus`, or joining it,
///   `ctx.join_bus`) is a member of it until then, and nobody owns a bus.
///   A bus ends once nobody is on it and no node can still join it,
///   the way its last member's body returned: finished, or failed with
///   its error. A body that paused
///   leaves nothing: it carries on from its state, its names with it
///   ([`body_end`]).
/// - Where it stands on each bus, saved with its state
///   ([`crate::run::saved::BusPosition`]): how far its reader read (a
///   firing opens one reader per bus a run of its body), and the name it
///   holds. A resumed firing starts from the positions it saved
///   ([`Self::resumed`]): its reader starts there, and its names come
///   back to it.
///
/// One per firing, held by its ctx handle and by every bus handle the
/// firing gets.
#[derive(Default)]
pub struct FiringBuses {
    /// Where the firing stood on each bus when it last saved, by bus id.
    restored: std::collections::BTreeMap<String, BusPosition>,
    /// Where it stands now, by bus id: its reader (its next offset,
    /// shared with the cursor) and the name it holds.
    positions: Mutex<HashMap<Uuid, LivePosition>>,
    /// Its body returned and its names left ([`Self::leave`]): a handle
    /// of it registering after that is refused.
    left: AtomicBool,
    /// The buses it used this run of its body ([`BusHandle::note_use`])
    /// and the ones it made: each is told when its body returns, so one
    /// it used stops counting it, and one it made that nobody joined
    /// learns it is unattended.
    touched: Mutex<HashSet<Uuid>>,
    /// The run's cancel: a reader of the firing ends with the cancelled
    /// error the moment it trips ([`BusCursor::next`]). `None` outside a
    /// run (a test's firing).
    cancellation: Option<Arc<CancellationFlag>>,
}

/// Where a firing's reader starts ([`FiringBuses::open_cursor`]): where
/// it stopped when the firing paused, or where the body asked.
#[derive(Clone, Copy)]
enum CursorStart {
    Saved(u64),
    Fresh(u64),
}

impl CursorStart {
    fn offset(self) -> u64 {
        match self {
            Self::Saved(offset) | Self::Fresh(offset) => offset,
        }
    }
}

/// Where a firing's cursor stands, shared by the cursor (which moves it
/// as it reads) and the firing's [`FiringBuses`] (which saves it).
struct CursorSlot {
    bus: Weak<BusInner>,
    next_offset: AtomicU64,
    /// How far the bus carried it past entries its filter turns down
    /// before dropping them ([`BusInner::carry_filtered_cursors`]).
    carried: AtomicU64,
    filter: Mutex<Option<CursorFilter>>,
}

impl CursorSlot {
    /// The offset a save keeps: where the cursor's next read would look,
    /// past the entries right after it that its filter turns down
    /// ([`BusInner::past_turned_down`]). A reader filtering out its own
    /// messages that pauses after sending some is then not behind them,
    /// whatever the bus drops of them before it comes back.
    fn saved_offset(&self) -> u64 {
        let offset = self.position();
        let filter = self.filter.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        match (filter, self.bus.upgrade()) {
            (Some(filter), Some(inner)) => inner.past_turned_down(offset, &filter),
            _ => offset,
        }
    }

    /// Where the cursor stands: as far as it read, or as far as the bus
    /// carried it, whichever is further.
    fn position(&self) -> u64 {
        self.next_offset.load(Ordering::Acquire).max(self.carried.load(Ordering::Acquire))
    }
}

/// One bus as the firing touches it now ([`FiringBuses::positions`]).
#[derive(Default, Clone)]
struct LivePosition {
    /// The reader it opened on this bus this run of its body, kept once
    /// the body dropped it (how far it read is where a save leaves it),
    /// and the line that opened it.
    cursor: Option<(Arc<CursorSlot>, &'static std::panic::Location<'static>)>,
    /// The name it took since it resumed: `None` while it took none (its
    /// saved one stands).
    registered: Option<Registration>,
}

/// How a body's return takes it off the buses it is a member of
/// ([`FiringBuses::leave`]), which is how a bus it leaves last ends:
/// `Finished` when it returned, `Failed` with its error otherwise, and
/// nothing when it paused: the body carries on from its saved state, a
/// member of its buses still.
pub fn body_end(result: &WeftResult<()>) -> Option<StreamEnd> {
    match result {
        Ok(()) => Some(StreamEnd::Finished),
        Err(WeftError::Suspended { .. }) => None,
        Err(error) => Some(StreamEnd::Failed { error: error.to_string() }),
    }
}

impl FiringBuses {
    /// The side of a firing resumed from the positions it saved
    /// ([`crate::run::saved::Positions::buses`]).
    pub fn resumed(restored: std::collections::BTreeMap<String, BusPosition>) -> Self {
        Self { restored, ..Self::default() }
    }

    /// The same side, of a run cancelled by `cancellation`: the firing's
    /// readers end with the cancelled error when it trips.
    pub fn cancelled_by(mut self, cancellation: Arc<CancellationFlag>) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    /// The body of the firing at `at` returned as `end`: its names leave
    /// every bus it has one on ([`BusInner::leave`]), the ones it took
    /// this run of its body and the ones it held over a pause and never
    /// took back, and every bus it used or made hears it is done. `bus`
    /// finds a bus of the run by id. A bus this left with nobody on it
    /// ends as `end` once no node can still join it; any other stays open.
    /// Once only: a later call (the engine's failure path after the body's
    /// own end) changes nothing.
    pub fn leave(&self, at: &FiringLocation, end: StreamEnd, bus: impl Fn(Uuid) -> Option<Arc<BusInner>>) {
        if self.left.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut buses = self.named_on();
        buses.extend(self.touched.lock().expect("touched buses poisoned").iter().copied());
        buses.sort_unstable();
        buses.dedup();
        for id in buses {
            if let Some(inner) = bus(id) {
                inner.leave(at, end.clone());
            }
        }
    }

    /// Its body paused (`body_end` answered `None`): it keeps its names,
    /// and the buses it used forget that it did. A parked firing holding a
    /// bus's marker counts as one that may still join it
    /// ([`crate::run::bus_reach`]), as it would on a worker picking the run
    /// up, whose buses know nothing of who used them.
    pub fn paused(&self, at: &FiringLocation, bus: impl Fn(Uuid) -> Option<Arc<BusInner>>) {
        for id in self.touched.lock().expect("touched buses poisoned").iter() {
            if let Some(inner) = bus(*id) {
                inner.used_by.lock().expect("bus users poisoned").remove(at);
            }
        }
    }

    /// The firing at `at` resumed: each bus its saved positions say it read
    /// or held a name on before its pause counts it as having used it, as
    /// its body goes on past that use without making it again. Read off
    /// the state its record keeps, so any worker counts the same.
    pub fn used_before_its_pause(&self, at: &FiringLocation, bus: impl Fn(Uuid) -> Option<Arc<BusInner>>) {
        let used: Vec<Uuid> = self
            .restored
            .iter()
            .filter(|(_, position)| position.cursor.is_some() || position.registered.is_some())
            .filter_map(|(id, _)| Uuid::parse_str(id).ok())
            .collect();
        for id in used {
            if let Some(inner) = bus(id) {
                self.touched(id);
                inner.used(at);
            }
        }
    }

    /// It used bus `id`, or made it: its body returning tells that bus
    /// ([`Self::leave`]).
    pub fn touched(&self, id: Uuid) {
        self.touched.lock().expect("touched buses poisoned").insert(id);
    }

    /// Whether its body returned and took its names off its buses.
    fn has_left(&self) -> bool {
        self.left.load(Ordering::Acquire)
    }

    /// Where the firing stands on every bus it touched, this run of its
    /// body or before a pause: what its saved state carries. A bus whose
    /// reader it has not opened again since resuming keeps the position it
    /// was saved with.
    pub fn positions(&self) -> std::collections::BTreeMap<String, BusPosition> {
        let mut positions = self.restored.clone();
        // Taken out first: a reader's saved offset reads its bus's log,
        // which is never locked inside this lock.
        let live: Vec<(Uuid, LivePosition)> =
            self.positions.lock().expect("bus positions poisoned").iter().map(|(bus, live)| (*bus, live.clone())).collect();
        for (bus, LivePosition { cursor, registered }) in live {
            let position = positions.entry(bus.to_string()).or_default();
            if let Some((cursor, _)) = cursor {
                position.cursor = Some(cursor.saved_offset());
            }
            if let Some(registered) = registered {
                position.registered = Some(registered);
            }
        }
        positions
    }

    /// The buses the firing has a name on, taken this run of its body or
    /// before a pause: the ones it leaves when its body returns.
    fn named_on(&self) -> Vec<Uuid> {
        let mut buses: Vec<Uuid> = self
            .restored
            .iter()
            .filter(|(_, position)| position.registered.is_some())
            .filter_map(|(bus, _)| Uuid::parse_str(bus).ok())
            .collect();
        let live = self.positions.lock().expect("bus positions poisoned");
        buses.extend(live.iter().filter(|(_, position)| position.registered.is_some()).map(|(bus, _)| *bus));
        buses.sort_unstable();
        buses.dedup();
        buses
    }

    /// Open the firing's reader on `bus`, asked to start at `requested`:
    /// it starts where it stopped before the pause, if it read there.
    /// Answers its start and the slot it stands in. A second reader on
    /// the bus in one run of the body, opened `at`, is refused with the
    /// line that opened the first: the one position a firing saves per
    /// bus could only bring one of them back.
    fn open_cursor(
        &self,
        bus: &Arc<BusInner>,
        requested: u64,
        at: &'static std::panic::Location<'static>,
    ) -> Result<(CursorStart, Arc<CursorSlot>), &'static std::panic::Location<'static>> {
        let mut positions = self.positions.lock().expect("bus positions poisoned");
        let live = positions.entry(bus.id).or_default();
        if let Some((_, first)) = &live.cursor {
            return Err(first);
        }
        let start = match self.restored.get(&bus.id.to_string()).and_then(|saved| saved.cursor) {
            Some(offset) => CursorStart::Saved(offset),
            None => CursorStart::Fresh(requested),
        };
        let slot = Arc::new(CursorSlot {
            bus: Arc::downgrade(bus),
            next_offset: AtomicU64::new(start.offset()),
            carried: AtomicU64::new(0),
            filter: Mutex::new(None),
        });
        live.cursor = Some((slot.clone(), at));
        Ok((start, slot))
    }

    /// The firing holds `registration` on `bus`.
    fn registered(&self, bus: Uuid, registration: Registration) {
        self.positions.lock().expect("bus positions poisoned").entry(bus).or_default().registered = Some(registration);
    }

    /// The name the firing holds on `bus` now: taken this run of its body,
    /// else held over its pause and not let go.
    fn held_name(&self, bus: Uuid) -> Option<String> {
        let live = self.positions.lock().expect("bus positions poisoned").get(&bus).and_then(|live| live.registered.clone());
        live.or_else(|| self.restored.get(&bus.to_string()).and_then(|saved| saved.registered.clone())).map(|registration| registration.name)
    }

    /// The name the firing held on `bus` when it paused, if this run of its
    /// body has neither taken it back nor let it go yet.
    fn held_over_its_pause(&self, bus: Uuid) -> Option<Registration> {
        let touched = self.positions.lock().expect("bus positions poisoned").get(&bus).is_some_and(|live| live.registered.is_some());
        if touched {
            return None;
        }
        self.restored.get(&bus.to_string()).and_then(|saved| saved.registered.clone())
    }

}

impl Drop for BusHandle {
    /// A registered handle going away leaves its name where it belongs.
    /// A firing's name stays on the bus, the firing's, until its body
    /// returns ([`FiringBuses::leave`]): dropping a handle mid-body, or
    /// with a pause, is not leaving, and the firing's next handle
    /// registering the name takes it back. A handle outside every firing
    /// leaves with its name now, with its `Left` entry, ending the bus if
    /// it was the last one on it ([`BusInner::leave_in`]).
    fn drop(&mut self) {
        let Some((name, _)) = self.registration.take() else { return };
        let mut log = self.inner.lock_log();
        if self.member_firing().is_some() {
            if let Some(member) = self.inner.members.lock().expect("bus members poisoned").get_mut(&name) {
                member.in_hand = false;
            }
            return;
        }
        self.inner.leave_in(&mut log, |member_name, _| member_name == name, StreamEnd::Finished);
    }
}

// ----- BusCursor ------------------------------------------------------

type CursorFilter = Arc<dyn Fn(&BusEntry) -> bool + Send + Sync + 'static>;

/// Independent read-cursor over a bus log. `next(&mut state)` returns
/// the next matching entry, `None` when the cursor reaches the `Closed`
/// entry of a bus that finished or the bus's inner has been reclaimed,
/// and the failure as an error when the bus ended failed. A read of a
/// firing ends with the cancelled error when its run is cancelled.
/// Every read is a place the
/// run can pause, so it takes the firing's state
/// ([`crate::context::PausePoint`]).
///
/// Stores `Weak<BusInner>`, upgrades to `Arc` per `next()` call. The
/// cursor never pins a bus alive on its own: a producer-side drop
/// releases the bus naturally even if a consumer's cursor still
/// exists. The Arc is held only for the duration of one `next()`.
pub struct BusCursor {
    inner: Weak<BusInner>,
    /// The bus's id: what its reads are keyed by
    /// (`crate::context::Places`).
    bus: Uuid,
    next_offset: u64,
    filter: Option<CursorFilter>,
    /// The firing that owns this cursor (inherited from the
    /// handle that minted it), or `None` outside an engine-driven node.
    /// Keys this cursor's waits in the engine's liveness map, so a node
    /// parked in `next()` is correctly attributed.
    node: Option<FiringLocation>,
    /// Where the firing's [`FiringBuses`] reads this cursor's next offset
    /// (and its filter) when it saves; `None` outside a firing.
    slot: Option<Arc<CursorSlot>>,
    /// The cursor of a resumed firing starts below what the bus came back
    /// from the pause without ([`BusHandle::cursor_at`]).
    behind: bool,
    /// The run's cancel, inherited from the firing
    /// ([`FiringBuses::cancellation`]).
    cancellation: Option<Arc<CancellationFlag>>,
}

impl BusCursor {
    fn new(
        inner: Weak<BusInner>,
        bus: Uuid,
        next_offset: u64,
        filter: Option<CursorFilter>,
        node: Option<FiringLocation>,
        slot: Option<Arc<CursorSlot>>,
        behind: bool,
        cancellation: Option<Arc<CancellationFlag>>,
    ) -> Self {
        Self {
            inner,
            bus,
            next_offset,
            filter,
            node,
            slot,
            behind,
            cancellation,
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
        let filter: CursorFilter = Arc::new(filter);
        if let Some(slot) = &self.slot {
            let first = slot.filter.lock().unwrap_or_else(std::sync::PoisonError::into_inner).replace(filter.clone()).is_none();
            if let (true, Some(inner)) = (first, slot.bus.upgrade()) {
                inner.filtered_cursors.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(Arc::downgrade(slot));
            }
        }
        self.filter = Some(filter);
        self
    }

    /// The cursor's current offset (the offset it will read NEXT).
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// The next MESSAGE of kind `kind`, skipping membership entries and
    /// other kinds: `(sender, payload)`, `None` when the bus finishes, the
    /// failure as an error when it ended failed.
    /// The typed read every plain consumer wants instead of matching
    /// the entry enum by hand. Composes with a constructed filter (both
    /// must accept).
    #[track_caller]
    pub fn next_message<'a, P: PausePoint>(
        &'a mut self,
        kind: &'a str,
        at: &'a mut P,
    ) -> impl std::future::Future<Output = WeftResult<Option<(String, WirePayload)>>> + 'a {
        self.next_message_at(kind, at, Point::direct("read a bus", std::panic::Location::caller()))
    }

    async fn next_message_at<P: PausePoint>(&mut self, kind: &str, at: &mut P, point: Point) -> WeftResult<Option<(String, WirePayload)>> {
        loop {
            match self.next_at(at, point).await? {
                None => return Ok(None),
                Some(entry) => match entry.kind {
                    BusEntryKind::Message { from, msg_kind, payload, .. }
                        if msg_kind == kind =>
                    {
                        let payload = payload.expect(
                            "a cursor-delivered Message always carries its payload \
                             (ephemeral payloads are resolved before delivery)",
                        );
                        return Ok(Some((from, payload)));
                    }
                    _ => continue,
                },
            }
        }
    }

    /// [`Self::next_message`] for a JSON bus: `(sender, value)`. The
    /// payload shape is frozen at bus creation, so a mismatch here is a
    /// wiring bug (a bytes bus read as JSON), and fails loudly. A reader
    /// that hands the value outside weft, where a file or a secret in it
    /// has to become what it stands for, reads it with
    /// [`Self::next_message`] and [`WirePayload::into_typed_json`].
    #[track_caller]
    pub fn next_json<'a, P: PausePoint>(
        &'a mut self,
        kind: &'a str,
        at: &'a mut P,
    ) -> impl std::future::Future<Output = WeftResult<Option<(String, Value)>>> + 'a {
        let point = Point::direct("read a bus", std::panic::Location::caller());
        async move {
            match self.next_message_at(kind, at, point).await? {
                None => Ok(None),
                Some((from, payload)) => match payload.into_json() {
                    Some(value) => Ok(Some((from, value))),
                    None => Err(wrong_payload("bytes", "json")),
                },
            }
        }
    }

    /// [`Self::next_message`] for a bytes bus: `(sender, bytes)`. A
    /// JSON payload here is the same wiring bug in the other direction.
    #[track_caller]
    pub fn next_bytes<'a, P: PausePoint>(
        &'a mut self,
        kind: &'a str,
        at: &'a mut P,
    ) -> impl std::future::Future<Output = WeftResult<Option<(String, bytes::Bytes)>>> + 'a {
        let point = Point::direct("read a bus", std::panic::Location::caller());
        async move {
            match self.next_message_at(kind, at, point).await? {
                None => Ok(None),
                Some((from, payload)) => match payload.into_bytes() {
                    Some(bytes) => Ok(Some((from, bytes))),
                    None => Err(wrong_payload("json", "bytes")),
                },
            }
        }
    }

    /// Next matching entry, `None` once the bus finished (no more
    /// entries), the failure as an error once it ended failed: a reader
    /// never takes a stream its producer broke off for a whole one. A
    /// cursor
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
    ///
    /// When the run pauses while this waits, the firing's state is saved
    /// with the cursor where it stands (nothing read) and the read
    /// returns the suspension the body hands back with `?`. When the run
    /// is cancelled, the read ends with the cancelled error, nothing read.
    #[track_caller]
    pub fn next<'a, P: PausePoint>(&'a mut self, at: &'a mut P) -> impl std::future::Future<Output = WeftResult<Option<BusEntry>>> + 'a {
        self.next_at(at, Point::direct("read a bus", std::panic::Location::caller()))
    }

    async fn next_at<P: PausePoint>(&mut self, at: &mut P, point: Point) -> WeftResult<Option<BusEntry>> {
        at.before(&point.on(self.bus.as_u128()), self.node.is_some())?;
        if self.behind {
            return Err(WeftError::NodeFailed(
                "this node paused with messages of a bus it had not read yet, and the bus no longer holds them \
                 (a bus keeps only its last window of messages, and over a pause an ephemeral bus keeps none and a \
                 journaled one only those of that window after the newest one over 100 KB): it cannot read on as if \
                 it had them. \
                 Make the bus journaled with a window that holds what a reader may fall behind by, or hold the run \
                 while reading it (`ctx.hold()`)"
                    .into(),
            ));
        }
        let pausable = at.pausable();
        let Some(inner) = self.inner.upgrade() else { return Ok(None) };
        // One destructure splits the borrows: the closure takes the
        // cursor position and filter, the wait keeps the node identity.
        let Self { next_offset, filter, node, slot, cancellation, .. } = self;
        // The full search under the log lock is the wait condition:
        // `Some(Some(entry))` on a hit, `Some(None)` at end-of-stream,
        // `None` parks. The shared protocol (`liveness::wait_on`)
        // holds one guard across the whole call and arms before every
        // search, so a send landing mid-search can never be a lost
        // wakeup, and "observed == current" provably means "this
        // waiter saw everything and still parked".
        let search_inner = inner.clone();
        // A reborrow the search moves in, so the position is the
        // cursor's own again once the wait is over (the slot reads it).
        let next_offset_now = &mut *next_offset;
        let evaluate = move || -> Option<Option<BusEntry>> {
            let next_offset = &mut *next_offset_now;
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
                if matches!(entry.kind, BusEntryKind::Closed { .. }) {
                    *next_offset = entry.offset;
                    return Some(None);
                }
                let resolved_entry = search_inner.resolve_ephemeral(entry);
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
            *next_offset = (*next_offset).max(search_inner.next_offset.load(Ordering::Acquire));
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
        let wait = wait_or_pause(&inner.liveness, node, &source, pausable, evaluate);
        // The cancel is checked first, so a read of a cancelled run never
        // hands out one more entry.
        let read = match cancellation.as_deref() {
            Some(cancellation) => tokio::select! {
                biased;
                () = cancellation.cancelled() => None,
                read = wait => Some(read),
            },
            None => Some(wait.await),
        };
        if let Some(slot) = slot {
            slot.next_offset.store(*next_offset, Ordering::Release);
        }
        match read {
            None => Err(WeftError::Cancelled),
            Some(Ok(Some(entry))) => Ok(Some(entry)),
            Some(Ok(None)) => match inner.end() {
                Some(StreamEnd::Failed { error }) => Err(WeftError::NodeFailed(format!("the bus ended failed: {error}"))),
                Some(StreamEnd::Finished) | None => Ok(None),
            },
            Some(Err(crate::liveness::Paused)) => Err(at.paused().await),
        }
    }
}

impl crate::context::kept::sealed::Sealed for BusCursor {}

/// A bus reader read through the state (`Kept::read`): the next entry.
impl crate::context::StateRead for BusCursor {
    type Item = BusEntry;

    fn read_through<'a, P: PausePoint>(
        &'a mut self,
        at: &'a mut P,
        point: Point,
    ) -> impl std::future::Future<Output = WeftResult<Option<BusEntry>>> + Send + 'a {
        self.next_at(at, point)
    }
}

/// A typed read that asked for one payload shape on a bus carrying the
/// other: the shape is frozen at creation, so this is a wiring bug.
fn wrong_payload(expected: &str, requested: &str) -> WeftError {
    WeftError::NodeFailed(format!("this bus carries {expected} payloads, not {requested}"))
}

// ----- Registry --------------------------------------------------------

/// Why a `BusRegistry::lookup` failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BusLookupError {
    #[error("value is not a bus marker")]
    NotABusMarker,
    #[error("bus marker uuid is malformed: {0}")]
    InvalidUuid(String),
    #[error("no bus with id {0} is live in this run")]
    UnknownBus(Uuid),
}

/// Per-run registry mapping a bus's id to its live state. The
/// engine's `BusCoordinator` owns one. A node creating a bus inserts;
/// a node receiving a marker looks up. Single source of truth for
/// "every bus this run minted": holds the strong `Arc<BusInner>`
/// so the bus stays pinned for the lifetime of the run and the
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
    /// to the resolving firing `node` (whose identity keys its
    /// own bus liveness) and its [`FiringBuses`]. Errors loud on every
    /// failure mode.
    pub fn lookup(
        &self,
        marker: &Value,
        node: Option<FiringLocation>,
        firing: Option<Arc<FiringBuses>>,
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
        Ok(BusHandle::from_inner(inner, node, firing))
    }

    /// A handle on the bus `id` for the firing `node`, if the run holds
    /// it.
    pub fn handle(&self, id: Uuid, node: Option<FiringLocation>, firing: Option<Arc<FiringBuses>>) -> Option<BusHandle> {
        Some(BusHandle::from_inner(self.bus(id)?, node, firing))
    }

    /// The bus `id`, if the run holds it.
    pub fn bus(&self, id: Uuid) -> Option<Arc<BusInner>> {
        self.inner.lock().expect("BusRegistry mutex poisoned").get(&id).map(Arc::clone)
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
            .field("journal", &self.inner.journal)
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
    use crate::context::HarnessRead;
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
            journal: crate::stream_journal::JournalPolicy {
                window: std::time::Duration::from_millis(250),
                ..crate::stream_journal::JournalPolicy::ephemeral()
            },
            payload: BusPayloadKind::Bytes,
            meta: serde_json::json!({ "sample_rate": 16000 }),
            ..Default::default()
        })
        .unwrap();
        let producer = registered(&bus, "mic");
        let mut cursor = bus.cursor().unwrap();

        // The wrong shape is refused loudly; the right one flows as bytes.
        let err = producer.send("frame", json!({"x": 1})).unwrap_err();
        assert!(matches!(err, SendError::WrongPayload { expected: "bytes", sent: "json" }));
        producer.send_bytes("frame", vec![1u8, 2, 3]).unwrap();
        let (from, frame) = cursor.next_bytes("frame", &mut HarnessRead).await.unwrap().unwrap();
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

        let kept = crate::stream_journal::JournalPolicy::default();
        let metadata_only =
            crate::stream_journal::JournalPolicy::ephemeral();

        let journaled = aggregate_window(&entries, &kept).unwrap();
        assert_eq!((journaled.first_offset, journaled.last_offset), (3, 6));
        assert_eq!(journaled.last_at_unix, 7, "stamped from the last entry's append time");
        assert_eq!(journaled.messages.len(), 3);
        assert_eq!(journaled.messages[1].payload, Some(WirePayload::Json(json!(4))));
        assert!(!journaled.messages[1].trimmed, "a small message is kept whole");
        assert_eq!(journaled.totals.len(), 2);
        assert_eq!((journaled.totals[0].count, journaled.totals[0].bytes), (2, 30));

        let ephemeral = aggregate_window(&entries, &metadata_only).unwrap();
        assert!(ephemeral.messages.is_empty(), "no payloads for an ephemeral window");
        assert_eq!(ephemeral.totals, journaled.totals);
        assert_eq!(ephemeral.last_at_unix, 7, "the stamp rides even without payloads");
        assert!(aggregate_window(&[], &kept).is_none(), "an empty window writes nothing");
    }

    /// The journal never writes down a message bigger than its limit,
    /// on a bus exactly as on a caller conversation: the row keeps a
    /// cut-down copy, says it was cut, and still reports the true size.
    /// Raw bytes are counted and never copied.
    #[test]
    fn a_bus_message_too_big_to_keep_is_written_down_cut_short() {
        let entry = |offset: u64, payload: WirePayload, bytes: u64| BusEntry {
            offset,
            at_unix: 7,
            kind: BusEntryKind::Message {
                from: "mic".into(),
                msg_kind: "audio".into(),
                payload: Some(payload),
                payload_byte_size: bytes,
            },
        };
        let huge = "x".repeat(crate::stream_journal::JOURNAL_TRIM_BYTES * 2);
        let entries = vec![
            entry(1, WirePayload::Json(json!({ "text": huge })), 400_000),
            entry(2, WirePayload::Bytes(bytes::Bytes::from(vec![7u8; 16])), 16),
        ];

        let window =
            aggregate_window(&entries, &crate::stream_journal::JournalPolicy::default()).unwrap();
        let big = &window.messages[0];
        assert!(big.trimmed, "the row says the copy is cut short");
        assert_eq!(big.payload_byte_size, 400_000, "the true size is still reported");
        let kept = big.payload.as_ref().expect("a cut copy is still a copy").byte_size();
        assert!(
            kept as usize <= crate::stream_journal::JOURNAL_TRIM_BYTES,
            "nothing bigger than the limit reaches the journal, got {kept} bytes"
        );

        let binary = &window.messages[1];
        assert_eq!(binary.payload, None, "raw bytes are counted, never written down");
        assert_eq!(binary.payload_byte_size, 16);
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
        let mut c = bytes_bus.cursor().unwrap();
        mic.send_bytes("frame", vec![1u8, 2]).unwrap();
        assert_eq!(c.next_json("frame", &mut HarnessRead).await.unwrap_err().to_string(), wrong_payload("bytes", "json").to_string());

        let json_bus = BusHandle::create();
        let chat = registered(&json_bus, "chat");
        let mut c = json_bus.cursor().unwrap();
        chat.send("msg", json!("hi")).unwrap();
        assert_eq!(c.next_bytes("msg", &mut HarnessRead).await.unwrap_err().to_string(), wrong_payload("json", "bytes").to_string());
    }

    /// `firing`'s body, at `node`, returned as `end`: its names leave
    /// `bus`, the one bus the test knows.
    fn left(firing: &FiringBuses, node: &str, end: StreamEnd, bus: &BusHandle) {
        let inner = bus.inner_arc();
        firing.leave(&at(node), end, |id| (id == inner.id()).then(|| inner.clone()));
    }

    /// A member of `bus` at `node`, registered as `node`, with its firing.
    fn member(bus: &BusHandle, node: &str) -> (BusHandle, Arc<FiringBuses>) {
        let firing = Arc::new(FiringBuses::default());
        let mut handle = firing_handle(bus, node, &firing);
        handle.register(node).unwrap();
        (handle, firing)
    }

    /// The creator returning leaves the bus to a member still on it: its
    /// name leaves, and nothing else happens. The last one leaving ends it
    /// finished.
    #[tokio::test]
    async fn a_bus_lives_until_its_last_member_leaves() {
        let bus = BusHandle::create();
        let mut reader = bus.cursor().unwrap();
        let (host, host_firing) = member(&bus, "host");
        let (guest, guest_firing) = member(&bus, "guest");
        host.send("hi", json!(1)).unwrap();
        drop(host);
        left(&host_firing, "host", StreamEnd::Finished, &bus);
        assert!(!bus.is_closed(), "the guest is still on it");
        assert!(matches!(&bus.inner.lock_log().last().unwrap().kind, BusEntryKind::Left { name, .. } if name == "host"));
        guest.send("hi", json!(2)).unwrap();
        drop(guest);
        left(&guest_firing, "guest", StreamEnd::Finished, &bus);
        assert_eq!(bus.end(), Some(StreamEnd::Finished), "the last member left");
        let mut read = Vec::new();
        while let Some((_, value)) = reader.next_json("hi", &mut HarnessRead).await.unwrap() {
            read.push(value);
        }
        assert_eq!(read, vec![json!(1), json!(2)], "both messages, then the end");
    }

    /// A bus whose last member left failing ends failed with its error: a
    /// reader drains what was sent, then reads the failure, so a stream
    /// whose only producer crashed never reads as whole.
    #[tokio::test]
    async fn the_last_member_leaving_failed_ends_the_bus_failed() {
        let bus = BusHandle::create();
        let mut reader = bus.cursor().unwrap();
        let (host, firing) = member(&bus, "host");
        host.send("hi", json!(1)).unwrap();
        drop(host);
        assert!(!bus.is_closed(), "a firing dropping its handle is not leaving");
        left(&firing, "host", StreamEnd::Failed { error: "the model went away".into() }, &bus);
        assert!(matches!(reader.next_json("hi", &mut HarnessRead).await, Ok(Some((_, v))) if v == json!(1)));
        let failed = reader.next_json("hi", &mut HarnessRead).await.unwrap_err();
        assert!(matches!(&failed, WeftError::NodeFailed(why) if why.contains("the model went away")), "{failed}");
        assert_eq!(bus.end(), Some(StreamEnd::Failed { error: "the model went away".into() }));
    }

    /// A member that fails just leaves: the others stay on an open bus (one
    /// of them may be how the conversation recovers), and the bus ends as
    /// its last member returns, finished here, not as the earlier failure.
    #[test]
    fn a_failing_member_leaves_the_others_an_open_bus() {
        let bus = BusHandle::create();
        let (_host, host_firing) = member(&bus, "host");
        let (_guest, guest_firing) = member(&bus, "guest");
        left(&guest_firing, "guest", StreamEnd::Failed { error: "the guest broke".into() }, &bus);
        assert!(!bus.is_closed(), "a member failing does not end the bus");
        left(&host_firing, "host", StreamEnd::Finished, &bus);
        assert_eq!(bus.end(), Some(StreamEnd::Finished), "it ends as the last one to leave returned");
    }

    /// Any node holding the bus may end it, a member or not, whoever is
    /// still on it; the first end stands, so members leaving after (failed
    /// or not) change nothing, and a firing leaves only once.
    #[test]
    fn a_close_ends_the_bus_for_everyone_and_the_first_end_stands() {
        let bus = BusHandle::create();
        let (_host, host_firing) = member(&bus, "host");
        let (_guest, guest_firing) = member(&bus, "guest");
        let closer = firing_handle(&bus, "closer", &Arc::new(FiringBuses::default()));
        closer.close();
        assert_eq!(bus.end(), Some(StreamEnd::Finished));
        let after = bus.inner.log_len();
        left(&guest_firing, "guest", StreamEnd::Failed { error: "boom".into() }, &bus);
        left(&host_firing, "host", StreamEnd::Failed { error: "boom".into() }, &bus);
        assert_eq!(bus.end(), Some(StreamEnd::Finished), "the close stands");
        assert_eq!(bus.inner.log_len(), after, "an ended bus takes no entry");

        let open = BusHandle::create();
        let (_a, firing) = member(&open, "a");
        let (_b, _) = member(&open, "b");
        left(&firing, "a", StreamEnd::Finished, &open);
        let after = open.inner.log_len();
        left(&firing, "a", StreamEnd::Failed { error: "again".into() }, &open);
        assert_eq!(open.inner.log_len(), after, "a firing leaves once");
        let mut late = firing_handle(&open, "a", &firing);
        assert_eq!(late.register("a"), Err(RegisterError::FiringLeft("a".into())), "a body that returned cannot join for it after");
        assert_eq!(BusHandle::create().end(), None, "an open bus has no end yet");
    }

    /// A close on an open bus nobody is on ends it the way its last member
    /// left: failed with that member's error here, a bus that paused while
    /// a node could still join it after its only member left failing. A
    /// bus nobody ever joined ends finished.
    #[tokio::test]
    async fn a_close_with_nobody_on_the_bus_ends_it_as_its_last_member_left() {
        let failure = StreamEnd::Failed { error: "the producer broke off".into() };
        let bus = BusHandle::reopen(
            ReopenedBus {
                id: Uuid::new_v4(),
                opts: BusOptions::default(),
                next_offset: 3,
                joined: vec!["host".into()],
                members: Vec::new(),
                retained: Vec::new(),
                lost_through: 3,
                end: None,
                last_left: Some(failure.clone()),
            },
            no_liveness(),
            Weak::<Notify>::new(),
        )
        .unwrap();
        assert!(!bus.is_closed(), "open, with nobody on it");
        firing_handle(&bus, "closer", &Arc::new(FiringBuses::default())).close();
        assert_eq!(bus.end(), Some(failure), "it ends as its last member left");
        let error = bus.cursor_from_start().unwrap().next(&mut HarnessRead).await.unwrap_err().to_string();
        assert!(error.contains("the bus ended failed: the producer broke off"), "{error}");

        let never_joined = BusHandle::create();
        never_joined.close();
        assert_eq!(never_joined.end(), Some(StreamEnd::Finished), "nobody ever joined");
    }

    /// A reader that never registers is no member: it keeps nothing open,
    /// and its firing leaving changes nothing on the bus.
    #[test]
    fn an_observer_keeps_nothing_open() {
        let bus = BusHandle::create();
        let watcher_firing = Arc::new(FiringBuses::default());
        let watcher = firing_handle(&bus, "watcher", &watcher_firing);
        let _cursor = watcher.cursor().unwrap();
        let (_host, host_firing) = member(&bus, "host");
        left(&watcher_firing, "watcher", StreamEnd::Failed { error: "the tap broke".into() }, &bus);
        assert!(!bus.is_closed(), "the watcher was never on it");
        left(&host_firing, "host", StreamEnd::Finished, &bus);
        assert_eq!(bus.end(), Some(StreamEnd::Finished), "the host was its only member, whoever still reads");
    }

    /// A name outside every firing (a test's handle) leaves with its
    /// handle, and the last one leaving that way ends the bus finished.
    #[test]
    fn a_handle_outside_a_firing_leaves_with_its_name() {
        let bus = BusHandle::create();
        let a = registered(&bus, "a");
        let b = registered(&bus, "b");
        drop(a);
        let _again = registered(&bus, "a");
        drop(b);
        assert!(!bus.is_closed(), "the second 'a' is still on it");
        drop(_again);
        assert_eq!(bus.end(), Some(StreamEnd::Finished));
    }

    /// A bus that finished reads to `None` after its last message; one
    /// that ended failed hands its failure to every reader, also one that
    /// opens after the end.
    #[tokio::test]
    async fn a_failed_bus_reads_as_its_failure() {
        let finished = BusHandle::create();
        let mut reader = finished.cursor().unwrap();
        registered(&finished, "host").send("hi", json!(1)).unwrap();
        finished.close();
        assert!(reader.next_json("hi", &mut HarnessRead).await.unwrap().is_some());
        assert!(reader.next_json("hi", &mut HarnessRead).await.unwrap().is_none());

        let failed = BusHandle::create();
        failed.inner_arc().close(StreamEnd::Failed { error: "the producer broke off".into() });
        let mut late = failed.cursor_from_start().unwrap();
        let error = late.next(&mut HarnessRead).await.unwrap_err();
        assert!(matches!(&error, WeftError::NodeFailed(why) if why.contains("the producer broke off")), "{error}");
    }

    /// A reader of a firing waiting on a quiet bus ends with the cancelled
    /// error the moment its run is cancelled, and a read asked after the
    /// cancel ends at once, even with a message waiting.
    #[tokio::test]
    async fn a_read_ends_cancelled_when_the_run_is_cancelled() {
        let cancellation = CancellationFlag::new_arc();
        let firing = Arc::new(FiringBuses::default().cancelled_by(cancellation.clone()));
        let bus = BusHandle::create();
        let mut reader = BusHandle::from_inner(bus.inner_arc(), None, Some(firing)).cursor().unwrap();
        let waiting = tokio::spawn(async move {
            let read = reader.next(&mut HarnessRead).await;
            (read, reader)
        });
        tokio::task::yield_now().await;
        cancellation.cancel_because(crate::run::CancelCause::User);
        let (read, mut reader) = tokio::time::timeout(std::time::Duration::from_secs(5), waiting).await.unwrap().unwrap();
        assert!(matches!(read, Err(WeftError::Cancelled)), "{read:?}");
        registered(&bus, "host").send("hi", json!(1)).unwrap();
        assert!(matches!(reader.next(&mut HarnessRead).await, Err(WeftError::Cancelled)));
    }

    /// Only a body that returned leaves its buses finished; a skip or any
    /// failure leaves them failed, and a pause leaves nothing (the body
    /// carries on from its state, a member still).
    #[test]
    fn a_body_end_follows_how_it_returned() {
        assert_eq!(body_end(&Ok(())), Some(StreamEnd::Finished));
        assert_eq!(body_end(&Err(WeftError::Suspended { token: Some("t".into()) })), None);
        assert_eq!(body_end(&Err(WeftError::Suspended { token: None })), None);
        assert!(matches!(body_end(&Err(WeftError::WaitSkipped("w".into()))), Some(StreamEnd::Failed { .. })));
        assert_eq!(
            body_end(&Err(WeftError::NodeFailed("the model went away".into()))),
            Some(StreamEnd::Failed { error: WeftError::NodeFailed("the model went away".into()).to_string() })
        );
        assert!(matches!(body_end(&Err(WeftError::Cancelled)), Some(StreamEnd::Failed { .. })));
    }

    /// Drive `cursor` until its next Message entry; skips Joined/Left.
    /// Returns `(from, kind, payload)` or None on close.
    async fn next_message(cursor: &mut BusCursor) -> Option<(String, String, Value)> {
        loop {
            match cursor.next(&mut crate::context::kept::FiringRead).await.unwrap() {
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
        let mut cursor = consumer.cursor().unwrap();
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
        let mut cursor = rx.cursor().unwrap();
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
        let wait = tokio::spawn(async move { waiter.wait_for("llm", &mut HarnessRead).await });
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
        assert!(guest.wait_for("host", &mut HarnessRead).await.is_ok());
    }

    #[tokio::test]
    async fn wait_for_errors_on_close() {
        let bus = BusHandle::create();
        let waiter = bus.new_handle();
        let wait = tokio::spawn(async move { waiter.wait_for("llm", &mut HarnessRead).await });
        tokio::task::yield_now().await;
        bus.close();
        assert!(wait.await.unwrap().unwrap_err().to_string().contains("the bus closed before 'llm' joined it"));
    }

    #[tokio::test]
    async fn close_before_recv_returns_none() {
        let bus = BusHandle::create();
        let stt = registered(&bus, "stt");
        let consumer = registered(&bus, "llm");
        let mut cursor = consumer.cursor().unwrap();
        bus.close();
        assert_eq!(stt.send("late", json!(null)), Err(SendError::Closed));
        assert!(next_message(&mut cursor).await.is_none());
    }

    #[tokio::test]
    async fn close_unblocks_a_waiting_cursor() {
        let bus = BusHandle::create();
        let consumer = registered(&bus, "llm");
        let mut cursor = consumer.cursor().unwrap();
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
        let mut cursor = consumer.cursor().unwrap();
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
        let mut ca = a.cursor().unwrap();
        let mut cb = b.cursor().unwrap();
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
                let wait = tokio::spawn(async move { waiter.wait_for("peer", &mut HarnessRead).await });
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

    // ----- New shape: cursors + offsets + log entries -------------

    #[tokio::test]
    async fn cursor_returns_entries_in_order_with_monotonic_offsets() {
        let bus = BusHandle::create();
        let p = registered(&bus, "p");
        p.send("a", json!(1)).unwrap();
        p.send("b", json!(2)).unwrap();
        let mut c = bus.cursor_from_start().unwrap();
        let mut offsets = Vec::new();
        for _ in 0..3 {
            let e = c.next(&mut HarnessRead).await.unwrap().unwrap();
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
        let mut c = bus.cursor_from_start().unwrap().with_filter(|e| {
            matches!(&e.kind, BusEntryKind::Message { msg_kind, .. } if msg_kind == "b")
        });
        let e = c.next(&mut HarnessRead).await.unwrap().unwrap();
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
        let mut c = bus.cursor_from_start().unwrap();
        let _joined = c.next(&mut HarnessRead).await.unwrap().unwrap();
        let _msg = c.next(&mut HarnessRead).await.unwrap().unwrap();
        assert!(c.next(&mut HarnessRead).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ephemeral_send_does_not_journal_payload_in_log_entry() {
        let bus =
            BusHandle::create_with_options(BusOptions { journal: crate::stream_journal::JournalPolicy::ephemeral(), window: Some(8), ..Default::default() }).expect("valid options");
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
            BusHandle::create_with_options(BusOptions { journal: crate::stream_journal::JournalPolicy::ephemeral(), window: Some(8), ..Default::default() }).expect("valid options");
        let p = registered(&bus, "p");
        p.send("frame", json!({ "n": 1 })).unwrap();
        let mut c = bus.cursor_from_start().unwrap();
        // skip the Joined
        let _j = c.next(&mut HarnessRead).await.unwrap().unwrap();
        let m = c.next(&mut HarnessRead).await.unwrap().unwrap();
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
            BusHandle::create_with_options(BusOptions { journal: crate::stream_journal::JournalPolicy::ephemeral(), window: Some(2), ..Default::default() }).expect("valid options");
        let p = registered(&bus, "p");
        // Create the cursor BEFORE sends so it starts at the joined-only
        // tail; the test exercises a consumer reading slowly enough that
        // the store evicts.
        let mut c = bus.cursor_from_start().unwrap();
        for i in 0..6 {
            p.send("frame", json!({ "i": i })).unwrap();
        }
        // First next() is the Joined entry (always retained, never in the
        // ephemeral store).
        let _j = c.next(&mut HarnessRead).await.unwrap().unwrap();
        // Offsets 1..=4 were evicted (window=2 keeps 5,6). The cursor
        // bridges the gap silently: the next read delivers the oldest
        // RETAINED message (i=4 at offset 5), then the tail one.
        let m = c.next(&mut HarnessRead).await.unwrap().expect("a retained message");
        let BusEntryKind::Message { payload, .. } = m.kind else { panic!("expected Message") };
        assert_eq!(m.offset, 5, "resumes at the oldest retained message, never backward");
        assert_eq!(payload.unwrap(), WirePayload::Json(json!({ "i": 4 })));
    }

    /// A Joined entry sitting between evicted Messages must ALWAYS be
    /// surfaced (never skipped). Build the sparse log:
    /// Joined "a" (0), msg (1, evicted), Joined "b" (2), msg (3, live).
    fn sparse_membership_bus() -> BusHandle {
        let bus = BusHandle::create_with_options(BusOptions {
            journal: crate::stream_journal::JournalPolicy::ephemeral(),
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
        let mut c = bus.cursor_from_start().unwrap();
        let (mut joins, mut live) = (0u32, 0u32);
        for _ in 0..10 {
            match c.next(&mut HarnessRead).await.unwrap() {
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
        let bus = BusHandle::create_with_options(BusOptions { journal: crate::stream_journal::JournalPolicy::ephemeral(), window: Some(1), ..Default::default() })
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
        bus.cursor_at(offset).unwrap().next(&mut HarnessRead).await.unwrap().expect("an entry")
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
            Some(crate::stream_journal::JournalMode::Journaled)
        );
        let eph = BusHandle::create_with_options(BusOptions { journal: crate::stream_journal::JournalPolicy::ephemeral(), window: None, ..Default::default() })
            .expect("valid options");
        let me = eph.marker();
        assert_eq!(
            crate::weft_type::WeftType::bus_marker_mode(&me),
            Some(crate::stream_journal::JournalMode::Ephemeral)
        );
    }

    #[tokio::test]
    async fn window_zero_is_rejected_loud() {
        // window=0 would evict every entry before any cursor could read
        // it; the only honest answer is to fail loud at construction. The
        // window applies to both modes, so a zero is rejected regardless of
        // `ephemeral`.
        let err = BusHandle::create_with_options(BusOptions {
            journal: crate::stream_journal::JournalPolicy::ephemeral(),
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
        let mut c = observer.cursor().unwrap();
        let r = tokio::time::timeout(std::time::Duration::from_millis(500), c.next(&mut HarnessRead))
            .await
            .expect("must resolve, not spin")
            .unwrap();
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
        let mut c = guest.cursor().unwrap().with_filter(|e| {
            matches!(&e.kind, BusEntryKind::Message { from, .. } if from == "host")
        });
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), c.next(&mut HarnessRead))
            .await
            .expect("must resolve")
            .unwrap()
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
                    host.wait_for("guest", &mut HarnessRead).await.unwrap();
                    let mut replies = host.cursor().unwrap().with_filter(|e| {
                        matches!(&e.kind, BusEntryKind::Message { from, .. } if from == "guest")
                    });
                    host.send("turn", json!("hello guest")).unwrap();
                    let reply = replies.next(&mut HarnessRead).await.unwrap().expect("guest reply");
                    assert!(matches!(reply.kind, BusEntryKind::Message { .. }));
                });
                let guest_task = tokio::spawn(async move {
                    guest.register("guest").unwrap();
                    guest.wait_for("host", &mut HarnessRead).await.unwrap();
                    let mut inbox = guest.cursor().unwrap().with_filter(|e| {
                        matches!(&e.kind, BusEntryKind::Message { from, .. } if from == "host")
                    });
                    let msg = inbox.next(&mut HarnessRead).await.unwrap().expect("host message");
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
                let mut c = consumer.cursor().unwrap().with_filter(|e| {
                    matches!(&e.kind, BusEntryKind::Message { .. })
                });
                let recv = tokio::spawn(async move {
                    let mut got = Vec::new();
                    for _ in 0..20 {
                        let entry = c.next(&mut HarnessRead).await.unwrap().expect("a message");
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
                let mut c = consumer.cursor().unwrap();
                let recv = tokio::spawn(async move { c.next(&mut HarnessRead).await.unwrap() });
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
        let bus = BusHandle::create_with_options(BusOptions { window: Some(2), ..Default::default() })
            .expect("opts");
        let tx = registered(&bus, "tx");
        let rx = registered(&bus, "rx");
        for i in 0..5 {
            tx.send("m", json!({ "i": i })).unwrap();
        }
        // Pump never acked, so journaled_through==0: every message is still
        // in RAM, readable from the retained floor, despite window=2.
        let mut cursor = rx.cursor_from_start().unwrap();
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
        let bus = BusHandle::create_with_options(BusOptions { window: Some(2), ..Default::default() })
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
        let mut c = bus.new_handle().cursor_at(1).unwrap();
        let mut first_msg: Option<i64> = None;
        for _ in 0..10 {
            match c.next(&mut HarnessRead).await.unwrap() {
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
        let mut cursor = obs.cursor().unwrap(); // unregistered handle: tail cursor
        tx.send("m", json!({ "i": 1 })).unwrap(); // after the cursor
        let (_, _, p) = next_message(&mut cursor).await.unwrap();
        assert_eq!(p["i"].as_i64().unwrap(), 1, "forward cursor skips the pre-existing message");
    }

    fn at(node: &str) -> FiringLocation {
        FiringLocation::new(node, vec![])
    }

    /// A handle of the firing at `node`, with that firing's buses.
    fn firing_handle(bus: &BusHandle, node: &str, firing: &Arc<FiringBuses>) -> BusHandle {
        BusHandle::from_inner(bus.inner_arc(), Some(at(node)), Some(firing.clone()))
    }

    /// A firing that pauses keeps its name: nobody reads it leave, nobody
    /// else can take it, and its resumed body takes it back as it was,
    /// at its first `Joined` offset, with nothing appended.
    #[test]
    fn a_paused_firing_keeps_its_name_and_takes_it_back() {
        let bus = BusHandle::create();
        let firing = Arc::new(FiringBuses::default());
        let mut host = firing_handle(&bus, "host", &firing);
        host.register("host").unwrap();
        let joined = host.registration.as_ref().unwrap().1;
        let before = bus.inner.log_len();
        drop(host);
        assert_eq!(bus.inner.log_len(), before, "a firing's handle going writes no Left");

        let mut other = firing_handle(&bus, "intruder", &Arc::new(FiringBuses::default()));
        assert_eq!(other.register("host"), Err(RegisterError::NameTaken("host".into())));

        let resumed = Arc::new(FiringBuses::default());
        let mut again = firing_handle(&bus, "host", &resumed);
        again.register("host").unwrap();
        assert_eq!(again.registration.as_ref().unwrap().1, joined, "taken back at its first Joined offset");
        assert_eq!(bus.inner.log_len(), before, "taking it back appends nothing");
        assert_eq!(resumed.positions()[&bus.id().to_string()].registered, Some(Registration { name: "host".into(), joined_offset: joined }));
    }

    /// A name held over a pause leaves, with its `Left`, once its firing's
    /// body returns without taking it back; another firing returning lets
    /// go of nothing.
    #[test]
    fn a_name_held_over_leaves_once_its_firing_returns() {
        let bus = BusHandle::create();
        let (_guest, _) = member(&bus, "guest");
        let firing = Arc::new(FiringBuses::default());
        let mut host = firing_handle(&bus, "host", &firing);
        host.register("host").unwrap();
        drop(host);
        let before = bus.inner.log_len();
        bus.inner.leave(&at("someone-else"), StreamEnd::Finished);
        assert_eq!(bus.inner.log_len(), before, "another firing's end lets go of nothing");
        let saved = firing.positions();
        left(&FiringBuses::resumed(saved), "host", StreamEnd::Finished, &bus);
        assert_eq!(bus.inner.log_len(), before + 1);
        assert!(matches!(&bus.inner.lock_log().last().unwrap().kind, BusEntryKind::Left { name, .. } if name == "host"));
        assert!(!bus.is_closed(), "the guest is still on it");
        let mut newcomer = firing_handle(&bus, "other", &Arc::new(FiringBuses::default()));
        newcomer.register("host").expect("the name is free again");
    }

    /// A bus opened again after a pause keeps its id, carries on at the
    /// offset it stopped at, still knows who joined, gives the names its
    /// paused firings hold back to them, and hands a reader reaching back
    /// the messages its record kept whole.
    #[tokio::test]
    async fn a_reopened_bus_carries_on_where_it_stopped() {
        let id = Uuid::new_v4();
        let kept = |offset: u64, text: &str| WindowedBusMessage {
            offset,
            from: "host".into(),
            msg_kind: "msg".into(),
            payload: Some(WirePayload::Json(json!(text))),
            payload_byte_size: 4,
            trimmed: false,
            at_unix: 1,
        };
        let bus = BusHandle::reopen(
            ReopenedBus {
                id,
                opts: BusOptions::default(),
                next_offset: 10,
                joined: vec!["host".into(), "guest".into()],
                members: vec![("guest".into(), at("guest"), 3)],
                retained: vec![kept(7, "a"), kept(8, "b")],
                lost_through: 7,
                end: None,
                last_left: None,
            },
            no_liveness(),
            Weak::<Notify>::new(),
        )
        .unwrap();
        assert_eq!(bus.id(), id);
        assert_eq!(bus.inner.log_len(), 10);
        assert_eq!(bus.inner.journaled_through(), 10, "everything before it is on record already");

        let mut back = bus.cursor_at(0).unwrap();
        assert_eq!(next_message(&mut back).await.unwrap().2, json!("a"));
        assert_eq!(next_message(&mut back).await.unwrap().2, json!("b"));

        let mut guest = firing_handle(&bus, "guest", &Arc::new(FiringBuses::default()));
        guest.register("guest").unwrap();
        assert_eq!(guest.registration.as_ref().unwrap().1, 3);
        guest.wait_for("host", &mut crate::context::kept::FiringRead).await.expect("host joined before the pause");

        let host = registered(&bus, "host");
        assert_eq!(host.registration.as_ref().unwrap().1, 10, "the next entry lands where the bus stopped");
        host.send("msg", json!("c")).unwrap();
        assert_eq!(next_message(&mut back).await.unwrap().2, json!("c"));
    }

    /// A bus that ended before the pause comes back ended, as it was: a
    /// reader reads what it kept and comes to its end (`None` when it
    /// finished, the failure when it failed), a close changes nothing, a
    /// register is refused as on any ended bus, the name a paused firing
    /// held on it is not held any more, and nothing is left for the pump
    /// to write again.
    #[tokio::test]
    async fn a_bus_that_ended_before_the_pause_comes_back_ended() {
        let kept = WindowedBusMessage {
            offset: 4,
            from: "host".into(),
            msg_kind: "msg".into(),
            payload: Some(WirePayload::Json(json!("last"))),
            payload_byte_size: 6,
            trimmed: false,
            at_unix: 1,
        };
        let ended = |end: StreamEnd| {
            BusHandle::reopen(
                ReopenedBus {
                    id: Uuid::new_v4(),
                    opts: BusOptions::default(),
                    next_offset: 5,
                    joined: vec!["host".into()],
                    members: vec![("guest".into(), at("guest"), 3)],
                    retained: vec![kept.clone()],
                    lost_through: 4,
                    end: Some(end),
                    last_left: None,
                },
                no_liveness(),
                Weak::<Notify>::new(),
            )
            .unwrap()
        };
        let finished = ended(StreamEnd::Finished);
        assert!(finished.is_closed());
        assert_eq!(finished.end(), Some(StreamEnd::Finished));
        assert_eq!(finished.inner.journaled_through(), finished.inner.log_len(), "its end is on record already");
        assert!(finished.inner.drain_journal_tail().is_empty(), "the pump writes nothing again");
        let mut reader = finished.cursor_from_start().unwrap();
        assert_eq!(next_message(&mut reader).await.unwrap().2, json!("last"));
        assert!(reader.next(&mut crate::context::kept::FiringRead).await.unwrap().is_none(), "a finished end");
        finished.close();
        assert_eq!(finished.end(), Some(StreamEnd::Finished), "a close changes nothing");
        let mut guest = firing_handle(&finished, "guest", &Arc::new(FiringBuses::default()));
        assert_eq!(guest.register("guest"), Err(RegisterError::Closed));

        let failed = ended(StreamEnd::Failed { error: "the session went away".into() });
        failed.close();
        let mut reader = failed.cursor_from_start().unwrap();
        assert_eq!(next_message(&mut reader).await.unwrap().2, json!("last"));
        let error = reader.next(&mut crate::context::kept::FiringRead).await.unwrap_err().to_string();
        assert!(error.contains("the bus ended failed: the session went away"), "{error}");
    }

    /// A resumed firing's reader starts where it stopped, and its position
    /// carries on from there, also once the body dropped it; a second
    /// reader on the bus in the same run of the body is refused.
    #[tokio::test]
    async fn a_resumed_firings_reader_starts_where_it_stopped_and_is_the_only_one() {
        let bus = BusHandle::create();
        let host = registered(&bus, "host");
        for i in 0..4 {
            host.send("msg", json!(i)).unwrap();
        }
        let saved = std::collections::BTreeMap::from([(bus.id().to_string(), BusPosition { cursor: Some(3), registered: None })]);
        let firing = Arc::new(FiringBuses::resumed(saved));
        let reader = firing_handle(&bus, "reader", &firing);
        let mut cursor = reader.cursor_from_start().unwrap();
        assert_eq!(cursor.next_offset(), 3, "the reader starts where it stopped");
        assert_eq!(next_message(&mut cursor).await.unwrap().2, json!(2));
        drop(cursor);
        assert_eq!(firing.positions()[&bus.id().to_string()].cursor, Some(4), "a dropped reader is saved where it stopped");
        let refused = reader.cursor().err().expect("a second reader is refused").to_string();
        assert!(refused.contains("opened a second reader on a bus at") && refused.contains("which it already opened one on at"), "{refused}");
        assert!(bus.cursor().is_ok(), "a cursor outside every firing is not held to it");
    }

    /// A resumed firing's cursor behind messages the reopened bus came back
    /// without (an ephemeral bus reopens empty) fails its first read
    /// instead of reading on over the hole; one that read them all reads
    /// on.
    #[tokio::test]
    async fn a_resumed_cursor_behind_what_the_bus_kept_fails_its_read() {
        let bus = BusHandle::reopen(
            ReopenedBus {
                id: Uuid::new_v4(),
                opts: BusOptions { journal: crate::stream_journal::JournalPolicy::ephemeral(), ..Default::default() },
                next_offset: 10,
                joined: vec!["host".into()],
                members: Vec::new(),
                retained: Vec::new(),
                lost_through: 10,
                end: None,
                last_left: None,
            },
            no_liveness(),
            Weak::<Notify>::new(),
        )
        .unwrap();
        let saved = std::collections::BTreeMap::from([(bus.id().to_string(), BusPosition { cursor: Some(7), registered: None })]);
        let reader = firing_handle(&bus, "reader", &Arc::new(FiringBuses::resumed(saved)));
        let refused = reader.cursor_from_start().unwrap().next(&mut crate::context::kept::FiringRead).await.unwrap_err().to_string();
        assert!(refused.contains("paused with messages of a bus it had not read yet"), "{refused}");
        let caught_up = std::collections::BTreeMap::from([(bus.id().to_string(), BusPosition { cursor: Some(10), registered: None })]);
        let reader = firing_handle(&bus, "caught-up", &Arc::new(FiringBuses::resumed(caught_up)));
        let mut caught_up = reader.cursor_from_start().unwrap();
        let mut fresh = firing_handle(&bus, "late", &Arc::new(FiringBuses::default())).cursor_from_start().unwrap();
        registered(&bus, "host").send("msg", json!("after")).unwrap();
        assert!(fresh.next(&mut crate::context::kept::FiringRead).await.unwrap().is_some(), "a cursor of a firing never paused reads as asked");
        assert!(caught_up.next(&mut crate::context::kept::FiringRead).await.unwrap().is_some(), "a reader that missed nothing reads on");
    }

    /// A firing that paused in place, its bus live, while its window
    /// dropped messages it had not read, fails its first read when it
    /// resumes; one that read up to what the bus still holds reads on.
    #[tokio::test]
    async fn a_resumed_cursor_behind_what_the_window_dropped_fails_its_read() {
        let bus = BusHandle::create_with_options(BusOptions {
            journal: crate::stream_journal::JournalPolicy::ephemeral(),
            window: Some(2),
            ..Default::default()
        })
        .unwrap();
        let host = registered(&bus, "host");
        for i in 0..5 {
            host.send("msg", json!(i)).unwrap();
        }
        let lost_through = bus.inner.lost_through.load(Ordering::Acquire);
        assert!(lost_through > 1, "the window dropped the oldest messages");
        let resumed_at = |offset: u64| {
            let saved = std::collections::BTreeMap::from([(bus.id().to_string(), BusPosition { cursor: Some(offset), registered: None })]);
            firing_handle(&bus, "reader", &Arc::new(FiringBuses::resumed(saved))).cursor_from_start().unwrap()
        };
        let refused = resumed_at(1).next(&mut crate::context::kept::FiringRead).await.unwrap_err().to_string();
        assert!(refused.contains("paused with messages of a bus it had not read yet"), "{refused}");
        let read = resumed_at(lost_through).next(&mut crate::context::kept::FiringRead).await.unwrap();
        assert!(read.is_some(), "a reader that missed nothing the bus dropped reads on");
        let mut live = bus.cursor_at(1).unwrap();
        assert!(live.next(&mut crate::context::kept::FiringRead).await.unwrap().is_some(), "a live slow reader skips ahead");
    }

    /// A reader filtering out its own messages that pauses after sending
    /// some saves its place past them, so the bus dropping them before it
    /// comes back leaves it behind nothing; a message it would read stops
    /// the carry.
    #[tokio::test]
    async fn a_filtered_reader_is_saved_past_what_its_filter_turns_down() {
        let bus = BusHandle::create_with_options(BusOptions {
            journal: crate::stream_journal::JournalPolicy::ephemeral(),
            window: Some(2),
            ..Default::default()
        })
        .unwrap();
        let firing = Arc::new(FiringBuses::default());
        let host = {
            let mut host = firing_handle(&bus, "host", &firing);
            host.register("host").unwrap();
            host
        };
        let guest = registered(&bus, "guest");
        let mut replies = host.cursor().unwrap().with_filter(|entry| matches!(&entry.kind, BusEntryKind::Message { from, .. } if from == "guest"));
        guest.send("msg", json!("hello")).unwrap();
        assert!(replies.next(&mut crate::context::kept::FiringRead).await.unwrap().is_some());
        for i in 0..3 {
            host.send("msg", json!(i)).unwrap();
        }
        let saved = firing.positions()[&bus.id().to_string()].cursor.expect("the reader is saved");
        assert_eq!(saved, bus.inner.next_offset.load(Ordering::Acquire), "carried past its own messages to the tail");
        assert!(bus.inner.lost_through.load(Ordering::Acquire) <= saved, "nothing it would read was dropped");

        guest.send("msg", json!("again")).unwrap();
        let wanted = bus.inner.next_offset.load(Ordering::Acquire) - 1;
        host.send("msg", json!("after")).unwrap();
        assert_eq!(firing.positions()[&bus.id().to_string()].cursor, Some(wanted), "a message it would read stops the carry");
    }

    /// A firing's name stays its own until its body returns, through a
    /// handle it dropped: no `Left`, the save still holds it, another name
    /// on the bus is refused, and the firing's next handle takes it back.
    #[tokio::test]
    async fn a_dropped_handle_leaves_the_name_with_its_firing() {
        let bus = BusHandle::create();
        let firing = Arc::new(FiringBuses::default());
        let mut handle = firing_handle(&bus, "a", &firing);
        handle.register("a").unwrap();
        let joined = handle.registration.as_ref().unwrap().1;
        let before = bus.inner.log_len();
        drop(handle);
        assert_eq!(bus.inner.log_len(), before, "no Left");
        assert_eq!(firing.positions()[&bus.id().to_string()].registered, Some(Registration { name: "a".into(), joined_offset: joined }));
        let mut other = firing_handle(&bus, "a", &firing);
        assert_eq!(other.register("b"), Err(RegisterError::FiringHoldsAnother("a".into())));
        let mut back = firing_handle(&bus, "a", &firing);
        back.register("a").unwrap();
        assert_eq!(back.registration.as_ref().unwrap().1, joined, "taken back at its first Joined offset");
        assert_eq!(bus.inner.log_len(), before, "taking it back appends nothing");
    }

    /// A firing holds one name per bus: a second handle of it taking
    /// another name is refused, and so is one taking another name while
    /// the one it held over its pause is still its own.
    #[tokio::test]
    async fn a_firing_holds_one_name_per_bus() {
        let bus = BusHandle::create();
        let firing = Arc::new(FiringBuses::default());
        let mut first = firing_handle(&bus, "a", &firing);
        first.register("a").unwrap();
        let mut second = firing_handle(&bus, "a", &firing);
        assert_eq!(second.register("b"), Err(RegisterError::FiringHoldsAnother("a".into())));

        let saved = std::collections::BTreeMap::from([(
            bus.id().to_string(),
            BusPosition { cursor: None, registered: Some(Registration { name: "held".into(), joined_offset: 0 }) },
        )]);
        let mut resumed = firing_handle(&bus, "held", &Arc::new(FiringBuses::resumed(saved)));
        assert_eq!(resumed.register("other"), Err(RegisterError::FiringHoldsAnother("held".into())));
    }
}
