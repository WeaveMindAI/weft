use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::Color;

// ----- Wake signals (unified trigger + suspension mechanism) ----------
//
// A wake signal is "something the listener listens for on behalf of
// a node." When it fires, the dispatcher either spawns a fresh run
// (entry path: `register_signal`) or resumes a paused firing (resume
// path: `await_signal`). The kind doesn't know which; the
// dispatcher's `RegisterRequest` and the journal's signal row carry
// that lifecycle metadata.
//
// `SignalSpec` is the wire shape: `kind` is a string tag and
// `config` is an opaque JSON blob owned by that kind. Per-kind data
// types and the `Signal` trait live in `crate::signal`; node code
// constructs kinds there and passes them straight to
// `ctx.register_signal(kind)` / `ctx.await_signal(kind)`. The
// framework projects the typed kind onto this wire shape via
// `signal::to_spec`. Authors never see `SignalSpec` directly.
//
// Adding a kind = one file in `weft-core/src/signal/<name>.rs` plus
// one file in `weft-listener/src/kinds/<name>.rs`. No central enum
// or match dispatch.

/// A wake-signal instance ready to be registered. Per-kind config
/// only; lifecycle metadata (entry vs resume, owning execution color)
/// rides the dispatcher's `RegisterRequest`, not the spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalSpec {
    /// Kind tag (e.g. `"api_endpoint"`, `"timer"`). Matched against the
    /// inventory of `SignalKind` impls; unknown tags fail validation.
    pub kind: String,
    /// Kind-specific configuration. Each kind owns its shape.
    #[serde(default)]
    pub config: Value,
    /// Optional consumer label. Set by nodes whose suspensions are
    /// processed by an external consumer (browser extension, etc).
    /// Token-scoped enumeration filters by this field. Charset is
    /// `[A-Za-z0-9_-]{1,64}` (validated in `signal::to_spec`).
    #[serde(default, rename = "consumerKind", alias = "consumer_kind", skip_serializing_if = "Option::is_none")]
    pub consumer_kind: Option<String>,
}

/// Where the signal lives on the public HTTP surface.
///
/// Two orthogonal axes drive how a signal is exposed externally:
/// SignalSurface (this enum) and `SignalAuth` (below). The kind
/// impl in the catalog returns one of each at register time; the
/// dispatcher copies them onto the `signal` row and the public
/// router uses them to dispatch incoming HTTP.
///
/// New surface kinds extend this enum without touching dispatcher
/// or listener routing logic: routing is generic in the surface
/// kind, the kind impl picks one.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignalSurface {
    /// Author-controlled HTTP entrypoint. Mounted at the dispatcher
    /// root: external callers POST to `<dispatcher_base>/<path>`.
    /// `path = ""` means root `/`. Path uniqueness enforced
    /// project-wide at register time (UNIQUE on signal.mount_path).
    /// Used by the live-caller kinds (ApiEndpoint, LiveSocket) and any
    /// future public-form-like kind.
    PublicEntry { path: String },
    /// Per-task callback. Mounted at `/signal/<token>` where the
    /// dispatcher mints the UUID at register time. Used by
    /// task-style signals (HumanQuery, future task-callback kinds)
    /// where each fire is a one-shot reply to a specific suspended
    /// firing and the URL is internal to the consumer flow.
    TaskCallback,
    /// Internal: no external HTTP surface at all. The signal fires
    /// from inside the listener (timer expires, SSE event arrives)
    /// and routes via a FireSignal broker task that a dispatcher
    /// Pod picks up. Used by Timer and SSE.
    Internal,
}

/// Authentication policy for the public HTTP surface. Independent
/// of `SignalSurface`: any surface kind can pick any auth kind.
///
/// Marker enum: it identifies the gate to run. The kind-specific
/// configuration (header name, value hash, future per-scheme bits)
/// lives in the `auth_config` JSON blob alongside, parsed by the
/// dispatcher's gate at fire time. New auth kinds: add a variant
/// here, add a match arm in `apply_auth_gate`, decide what shape
/// goes in `auth_config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignalAuth {
    /// Open. Anyone with the URL can fire. Suitable for raw
    /// webhooks where the URL itself is the secret.
    None,
    /// HTTP header carries an opaque key the caller must match.
    /// `auth_config` shape: `{ header_name, value_hash }` (sha256
    /// hex of the plaintext). Plaintext is never persisted on the
    /// signal row; it lives in the listener's in-RAM
    /// `secret_cache` until the listener pod restarts.
    ApiKey,
}

impl SignalAuth {
    /// Discriminant string for the `signal.auth_kind` column.
    pub fn kind_tag(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ApiKey => "api_key",
        }
    }
}

impl SignalSurface {
    /// Discriminant string for the `signal.surface_kind` column.
    pub fn kind_tag(&self) -> &'static str {
        match self {
            Self::PublicEntry { .. } => "public_entry",
            Self::TaskCallback => "task_callback",
            Self::Internal => "internal",
        }
    }
}

/// Listener-computed routing + auth metadata returned from
/// `/register`. The dispatcher copies these fields onto the
/// signal row; any plaintext secret the kind mints stays in the
/// listener's per-pod secret cache and is served via `/display`,
/// never crossing the wire as a structured field.
///
/// `auth_config` is a kind-specific JSON blob the dispatcher's
/// auth gate parses according to `auth.kind_tag()`. For
/// `ApiKey` the blob is `{header_name, value_hash}` (sha256 hex).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRouting {
    pub surface: SignalSurface,
    pub auth: SignalAuth,
    /// Hash + any other dispatcher-readable bits the gate needs.
    /// Plaintext secrets NEVER appear here.
    #[serde(default)]
    pub auth_config: Value,
}

// ----- Cost report (fire-and-forget primitive) ------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostReport {
    pub service: String,
    pub model: Option<String>,
    pub amount_usd: f64,
    pub metadata: Value,
}

// ----- Execution snapshot ---------------------------------------------
//
// Written by the worker when it stalls (all firings either terminal or
// waiting). The dispatcher stores this in the journal and hands it to
// the next worker invocation so the run continues exactly where it
// left off. See docs/v2-design.md §3.5.

/// Durable snapshot of an execution's in-progress state. Contains
/// everything a new worker needs to resume: the pulse table, the
/// per-node execution records, and the active suspensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub color: Color,
    pub pulses: crate::pulse::PulseTable,
    pub executions: crate::exec::NodeExecutionTable,
    pub suspensions: HashMap<String, SuspensionInfo>,
    /// Live `LoopInstance`s rebuilt from `LoopInstantiated` /
    /// `LoopIterationLaunched` / `LoopOutFired` / `LoopTerminated`
    /// events. A fresh worker reads these to rehydrate its engine's
    /// per-loop state on resume.
    #[serde(default)]
    pub loop_instances: HashMap<LoopInstanceKey, LoopInstanceSnapshot>,
    /// Roots kicked into this execution. The scheduler dispatches a
    /// kicked node once even when it has no wired pending inputs (it
    /// IS the entry point). Folded from `ExecEvent::NodeKicked`.
    /// `dispatched=true` once the engine has consumed the kick (the
    /// node was dispatched at frames=[]; further kicks on the same
    /// node id are a no-op).
    #[serde(default)]
    pub kicked: HashMap<String, KickedNode>,
    /// Fires that arrived for live suspensions but haven't been
    /// consumed by a worker's node completion yet. The worker
    /// seeds these into its link on startup so every waiting node
    /// finds its value when re-dispatched. Survives worker restarts
    /// because it's derived from journal events, not slot queues.
    #[serde(default)]
    pub pending_deliveries: HashMap<String, Value>,
    /// Per-(node, frames) ordered sequence of past `await_signal`
    /// calls. Each entry has the call_index (0-based ordinal of
    /// the call within the body), the token, and either the
    /// resolved value (if the corresponding fire arrived) or None
    /// (still pending; this is the live suspension).
    ///
    /// On replay, the runtime pre-loads this sequence per
    /// (node, frames); each `await_signal` call within the body
    /// pops the next entry and either returns its resolved value
    /// instantly OR re-suspends if pending. This is what makes
    /// multiple sequential awaits within one node body work.
    #[serde(default)]
    pub awaited_sequences: HashMap<(String, crate::frames::LoopFrames), Vec<AwaitedEntry>>,
    /// Journal rows the fold could not apply because they were
    /// corrupted (unparseable UUID, broken invariants, etc.). Empty
    /// in the normal case. Surfaced to the inspector so the user sees
    /// "row N corrupted" instead of a silently missing pulse. The
    /// fold ALSO logs each corruption at `error!` level for ops
    /// observability; this list is the user-visible counterpart.
    #[serde(default)]
    pub corruptions: Vec<JournalCorruption>,
}

/// One journal row the fold could not apply. The `site` names the
/// fold step that rejected the row (a closed enum over every fold
/// branch that can fail); `reason` says which field was malformed
/// and how. Carries no in-band data because corruption means the
/// data is unusable; the user's recovery is to investigate the
/// journal directly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalCorruption {
    pub site: CorruptionSite,
    /// Which field of the row was malformed (e.g. "pulse_id",
    /// "absorbed_pulse_ids[0]") and the parse error or invariant
    /// violation.
    pub reason: String,
}

/// Closed enum over the fold sites that can reject a row. Adding a
/// new fold branch that can fail forces adding a variant here; that
/// is the point. Serialised as the variant name on the wire so the
/// inspector renders a stable label.
// SYNC: CorruptionSite <-> packages/weft-graph/src/protocol.ts CorruptionSite
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CorruptionSite {
    /// `ExecEvent::PulseEmitted` fold path (`push_pulse` UUID parse
    /// or `Pulse::from_journal_emit` invariant check).
    PulseEmitted,
    /// `ExecEvent::NodeStarted` fold path (`parse_absorbed_ids` on
    /// `pulses_absorbed`).
    NodeStarted,
    /// `ExecEvent::NodeResumed` fold path (`parse_absorbed_ids` on
    /// `pulses_absorbed`).
    NodeResumed,
    /// `LoopIterationLaunched` arrived for a `LoopInstanceKey` with no
    /// preceding `LoopInstantiated`. Writer-order bug or row loss.
    LoopIterationLaunched,
    /// `LoopOutFired` arrived for a `LoopInstanceKey` with no preceding
    /// `LoopInstantiated`. Writer-order bug or row loss.
    LoopOutFired,
    /// `LoopTerminated` arrived for a `LoopInstanceKey` with no preceding
    /// `LoopInstantiated`. Writer-order bug or row loss.
    LoopTerminated,
    /// `ExecEvent::NodeCompleted` fold path (`push_pulse` on a carried
    /// closure emission).
    NodeCompleted,
    /// `ExecEvent::NodeFailed` fold path (`push_pulse` on a carried
    /// closure emission).
    NodeFailed,
    /// `ExecEvent::NodeSkipped` fold path (`push_pulse` on a carried
    /// closure emission).
    NodeSkipped,
    /// `ExecEvent::NodeCancelled` fold path (`push_pulse` on a carried
    /// closure emission).
    NodeCancelled,
}

/// One entry in the per-(node, frames) replay sequence rebuilt by
/// `fold_to_snapshot`. The runtime consumes these in call_index
/// order on every dispatch. Two kinds of observable points within
/// a node body produce entries:
///
/// - `Await { token, resolved }`: a past `ctx.await_signal` call.
///   `resolved=Some(value)` if the matching `SuspensionResolved`
///   already arrived; `None` for the still-pending tail.
///
/// - `Run { name, value }`: a past `ctx.run("name", fn)` call.
///   The closure's output was journaled and replays here without
///   re-running the closure (handles non-determinism between
///   awaits without forcing replay-from-top to recompute).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwaitedEntry {
    pub call_index: u32,
    pub kind: AwaitedEntryKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AwaitedEntryKind {
    Await {
        token: String,
        /// `Some(value)` iff `SuspensionResolved` arrived;
        /// `None` for the still-pending tail.
        resolved: Option<Value>,
    },
    Run {
        /// Author-supplied identifier for the call site. Used for
        /// debugging + journal traceability; the runtime keys on
        /// `call_index` only.
        name: String,
        value: Value,
    },
}

/// Per-paused-(node, frames) info stored in the snapshot. `token` is
/// the key in the outer HashMap. Enough to: identify the waiting
/// node/frames, re-register the signal on every fresh worker boot,
/// and route the delivered value back to the right oneshot when the
/// fire arrives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuspensionInfo {
    pub node_id: String,
    pub frames: crate::frames::LoopFrames,
    pub spec: SignalSpec,
    pub created_at_unix: u64,
    /// 0-based ordinal of the `await_signal` call within this
    /// (node_id, frames). The runtime uses this on replay to put
    /// resolved values back in the right order.
    #[serde(default)]
    pub call_index: u32,
}

/// One kicked root node in a folded snapshot. A kicked node is an
/// entry point of a fresh execution (a firing trigger, a manual-run
/// root, an InfraSetup root) that has no wired pending inputs and so
/// would never become ready on its own. The scheduler dispatches it
/// once at frames=[]. The optional `payload` carries the wake event's
/// data for the firing trigger (the HTTP body, the SSE event JSON,
/// the form submission, the timer info); node bodies read it via
/// `ctx.wake_payload()` in Fire phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KickedNode {
    /// Wake event payload for the firing trigger. `None` for every
    /// other kicked root.
    pub payload: Option<Value>,
    /// Flips to `true` once the engine has dispatched this kick (the
    /// node started at root frames). A second tick that sees
    /// `dispatched` must NOT re-dispatch.
    #[serde(default)]
    pub dispatched: bool,
}

// ----- Loop instance snapshot ----------------------------------------

/// Key under which a `LoopInstance` is tracked across the engine and
/// the journal fold. Nested loops, parallel sibling iterations, and
/// re-entrant runs each get a distinct key because the
/// `parent_frames` part differs.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopInstanceKey {
    pub group_id: String,
    pub parent_frames: crate::frames::LoopFrames,
    pub color: Color,
}

/// One body-side write to a `LoopOut` inward-in port at a single
/// iteration. The tag distinguishes "wrote a value (which MAY be JSON
/// null)" from "closed the port". Default serde on `Option<Value>`
/// collapses `Some(Value::Null)` and `None` to the same JSON form, so
/// we cannot use `Option<Value>` here without losing the closure
/// signal on journal round-trip. Carry semantics treat `Closed` as
/// "keep previous"; gather semantics treat `Closed` as "null slot at
/// index".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum LoopWrite {
    Value(Value),
    Closed,
}

impl LoopWrite {
    pub fn into_option(self) -> Option<Value> {
        match self {
            LoopWrite::Value(v) => Some(v),
            LoopWrite::Closed => None,
        }
    }

    pub fn as_value(&self) -> Option<&Value> {
        match self {
            LoopWrite::Value(v) => Some(v),
            LoopWrite::Closed => None,
        }
    }
}

/// How a value rides (or doesn't ride) in a journaled event: the full
/// value, or metadata-only with the bytes kept elsewhere. A GENERAL
/// payload mode reused by every feature with the journaled-vs-ephemeral
/// tradeoff (the bus's `BusMessage`, the live caller's `Caller*` events,
/// future high-volume streams), NOT bus-specific (hence the neutral name).
///
/// The tag distinguishes "journaled, payload IS Value::Null" from
/// "ephemeral, payload not in journal". Default serde on `Option<Value>`
/// collapses `Some(Value::Null)` and `None` to the same JSON form, so a
/// `Some(Null)` payload (a node legitimately sending JSON null) would
/// round-trip indistinguishable from an ephemeral message. The surrounding
/// event carries `payload_byte_size` and `payload_sha256_prefix` so the
/// inspector can render a stable identifier even on `Ephemeral`.
// SYNC: JournaledPayload <-> packages/weft-graph/src/protocol.ts JournaledPayload
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JournaledPayload {
    /// Journaled mode: full payload (which may be `Value::Null`) rides in
    /// the event so consumers can read history.
    Journaled { value: Value },
    /// Ephemeral mode: payload was stored in an in-RAM sliding window only;
    /// the journal carries the metadata so the inspector can render it
    /// without the value.
    Ephemeral,
}

impl JournaledPayload {
    pub fn value(&self) -> Option<&Value> {
        match self {
            JournaledPayload::Journaled { value } => Some(value),
            JournaledPayload::Ephemeral => None,
        }
    }

    pub fn into_value(self) -> Option<Value> {
        match self {
            JournaledPayload::Journaled { value } => Some(value),
            JournaledPayload::Ephemeral => None,
        }
    }
}

/// The journaled metadata for a payload `Value`: its serialized byte size
/// and the first 8 bytes of its SHA-256. ONE derivation shared by every
/// journaled-event producer (the bus and the live-caller connection both
/// stamp this on their events), so the size/hash shape can never drift
/// between them. A `Value` always serializes, so the serialize cannot fail.
pub fn payload_metadata(value: &Value) -> (u64, [u8; 8]) {
    let bytes = serde_json::to_vec(value).expect("a serde_json::Value always serializes");
    let size = bytes.len() as u64;
    let digest = Sha256::digest(&bytes);
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    (size, prefix)
}

/// Folded view of one live `LoopInstance`. The engine reads this on
/// resume to rehydrate its per-loop state. The full `LoopConfig`
/// `over` / `carry` / `trim_on_mismatch` ride along because the
/// snapshot may outlive the project version the loop was instantiated
/// under, and the engine has no other source of truth for the
/// instance's resolved config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopInstanceSnapshot {
    pub iter_count: u32,
    pub parallel: bool,
    pub max_iters: Option<u32>,
    /// Iter-input port names, in declared order. NOT optional: a
    /// snapshot whose `LoopInstantiated` writer omitted this field
    /// is corrupt, not legitimately legacy. Pre-prod fresh DB on
    /// every rebuild means a missing field is never a valid state.
    pub over: Vec<String>,
    /// Carry-port names, in declared order. Same rationale as
    /// `over`: required, never defaulted.
    pub carry: Vec<String>,
    /// Zip mode for mismatched iter-input lengths. Required.
    pub trim_on_mismatch: bool,
    /// Iterations the engine has launched body work for. Required,
    /// like every field below: the one writer (the journal fold)
    /// always emits all of them, so a missing field is a truncated /
    /// corrupt row that must FAIL deserialization (landing in the
    /// fold's corruption surface) instead of silently rehydrating an
    /// empty loop instance.
    pub launched: Vec<u32>,
    /// Iterations whose `LoopOut` has fired.
    pub out_fired: Vec<u32>,
    /// Per gather-port, the per-index slot. `Closed` means the body
    /// closed that port at that iteration (assembled outward list
    /// gets `null` there).
    pub gather_lists: HashMap<String, HashMap<u32, LoopWrite>>,
    /// Current carry-port values. A LoopOut firing whose carry-write
    /// port was closed does NOT update this map (previous value kept).
    pub carry_values: HashMap<String, Value>,
    /// Outer input bag captured at LoopIn first-fire. Used by the
    /// sequential drive mode to launch iteration N+1 after the
    /// LoopIn's outer-in pulses have already been absorbed by the
    /// first dispatch.
    pub outer_input: HashMap<String, Value>,
    /// `Some(reason)` once `LoopTerminated` has been recorded.
    pub terminated: Option<LoopTerminationReason>,
}

// SYNC: LoopTerminationReason <-> packages/weft-graph/src/protocol.ts LoopTerminationReason
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopTerminationReason {
    OverExhausted,
    DoneVoted,
    MaxItersReached,
    Cancelled,
    /// A boundary firing failed (config error, missing carry seed,
    /// outward emit failure): the engine closed the loop's outward
    /// ports and terminated the instance.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `LoopWrite::Value(Value::Null)` and `LoopWrite::Closed` MUST
    /// stay distinguishable across a JSON round-trip. The whole point
    /// of the tagged enum is preserving "body wrote JSON null" vs
    /// "body closed the port" through the journal. A regression
    /// here (e.g. someone adds `#[serde(untagged)]`) silently
    /// collapses the two, restoring the exact bug the enum exists
    /// to prevent.
    #[test]
    fn loop_write_value_null_vs_closed_distinguishable_in_json() {
        let v_null = LoopWrite::Value(Value::Null);
        let closed = LoopWrite::Closed;
        let j_null = serde_json::to_string(&v_null).expect("serialize value(null)");
        let j_closed = serde_json::to_string(&closed).expect("serialize closed");
        assert_ne!(
            j_null, j_closed,
            "LoopWrite::Value(Null) and LoopWrite::Closed must serialize differently",
        );
        let back_null: LoopWrite =
            serde_json::from_str(&j_null).expect("deserialize value(null)");
        let back_closed: LoopWrite =
            serde_json::from_str(&j_closed).expect("deserialize closed");
        assert_eq!(back_null, v_null);
        assert_eq!(back_closed, closed);
    }

    /// Within a HashMap-valued field (matching the journal event
    /// shape), the tag still distinguishes the two cases on round
    /// trip. The default serde behavior for `HashMap<String,
    /// Option<Value>>` collapsed both `Some(Null)` and `None` to
    /// JSON null; this test pins that `HashMap<String, LoopWrite>`
    /// does NOT collapse.
    #[test]
    fn loop_write_in_hashmap_round_trip_preserves_distinction() {
        use std::collections::HashMap;
        let mut m: HashMap<String, LoopWrite> = HashMap::new();
        m.insert("written_null".into(), LoopWrite::Value(Value::Null));
        m.insert("closed".into(), LoopWrite::Closed);
        m.insert("real".into(), LoopWrite::Value(serde_json::json!(42)));
        let s = serde_json::to_string(&m).expect("serialize");
        let back: HashMap<String, LoopWrite> =
            serde_json::from_str(&s).expect("deserialize");
        assert_eq!(back["written_null"], LoopWrite::Value(Value::Null));
        assert_eq!(back["closed"], LoopWrite::Closed);
        assert_eq!(back["real"], LoopWrite::Value(serde_json::json!(42)));
        assert_ne!(back["written_null"], back["closed"]);
    }
}

