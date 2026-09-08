use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    /// The connection this signal acts AS, when it needs one: an
    /// authed poll, a subscribed stream, a socket whose URL is minted
    /// by an authenticated call. Kind-independent on purpose: the
    /// listener resolves it through the broker at every use (each poll
    /// cycle, each reconnect) and applies the service's auth steps to
    /// whatever outbound call the kind makes, so no kind reimplements
    /// signing in and no credential is ever frozen onto a row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<AccessRef>,
    /// Pre-fire predicates over the payload the kind produced.
    /// Evaluated by shared plumbing between "the kind has a payload"
    /// and "a fire is enqueued", for every kind: filtering is one
    /// concept, not a per-kind feature. Empty/absent = fire on
    /// everything.
    #[serde(default, rename = "match", skip_serializing_if = "Vec::is_empty")]
    pub match_predicates: Vec<crate::signal::Predicate>,
}

impl SignalSpec {
    /// A spec carrying only a kind tag and its config: no connection,
    /// no filter, no consumer label. The shape most callers building a
    /// wire spec by hand want, and the one place the "everything else
    /// is optional" default is written down.
    pub fn of_kind(kind: impl Into<String>, config: Value) -> Self {
        Self {
            kind: kind.into(),
            config,
            consumer_kind: None,
            access: None,
            match_predicates: Vec::new(),
        }
    }
}

/// The stored connection a signal acts as; see [`SignalSpec::access`].
/// Exactly the reference an [`crate::access::Access`] value carries
/// (id + service), never a credential: the listener re-resolves it per
/// use, behind the tenant wall.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessRef {
    pub id: String,
    pub service: String,
    /// The stored values the trigger's input declared it needs
    /// (`requiresValues`), carried so the listener's resolve refuses a
    /// connection missing one instead of dialing with a blank address.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_values: Vec<String>,
}

impl From<&crate::access::Access> for AccessRef {
    fn from(access: &crate::access::Access) -> Self {
        Self {
            id: access.access_id().to_string(),
            service: access.service().to_string(),
            required_values: access.required_values().to_vec(),
        }
    }
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

// ----- Execution snapshot ---------------------------------------------

/// An execution's state as the journal fold rebuilds it: the pulse
/// table, the per-node execution records, the loop instances, the
/// kicks, and the active suspensions. Everything a fresh worker needs
/// to resume; the fold is the only writer, and a resume never reads
/// anything the fold did not derive from the journal's rows plus the
/// program. Never persisted: replay is the source of truth.
#[cfg(feature = "runtime")]
#[derive(Debug, Clone)]
pub struct ExecutionSnapshot {
    pub color: Color,
    pub pulses: crate::pulse::PulseTable,
    pub executions: crate::exec::NodeExecutionTable,
    pub suspensions: HashMap<String, SuspensionInfo>,
    /// Every loop instance, as the fold drove it from the loop rows.
    /// A resumed worker takes it over as its own runtime.
    pub loop_runtime: crate::exec::loop_runtime::LoopRuntime,
    /// Nodes kicked into this execution, keyed by where they fire. The
    /// scheduler dispatches a kicked node once even when it has no
    /// wired pending inputs (it IS an entry point). Folded from
    /// `ExecEvent::NodeKicked` (the run's roots, at frames `[]`), from
    /// a group's In boundary firing (the body's roots, at the group's
    /// frames) and from `ExecEvent::LoopIterationLaunched` (a loop
    /// body's roots, at the iteration's frames). `dispatched=true` once
    /// the engine has consumed the kick (the node started at that
    /// location; further kicks at the same location are a no-op).
    pub kicked: HashMap<crate::frames::FiringLocation, KickedNode>,
    /// Fires that arrived for live suspensions but haven't been
    /// consumed by a worker's node completion yet. The worker
    /// seeds these into its link on startup so every waiting node
    /// finds its value when re-dispatched. Survives worker restarts
    /// because it's derived from journal events, not slot queues.
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
    pub awaited_sequences: HashMap<crate::frames::FiringLocation, Vec<AwaitedEntry>>,
    /// Journal rows the fold could not apply because they were
    /// corrupted (unparseable UUID, broken invariants, a row the
    /// program cannot make sense of). Empty in the normal case.
    /// Surfaced to the inspector so the user sees "row N corrupted"
    /// instead of a silently missing pulse; the engine refuses to
    /// resume over any of them. The fold ALSO logs each corruption at
    /// `error!` level for ops observability.
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

/// Closed enum over the places a journal row can be rejected: every
/// fold branch that can fail, plus the row that does not decode at
/// all. Adding a new rejecting site forces adding a variant here; that
/// is the point. Serialised as the variant name on the wire so the
/// inspector renders a stable label.
// SYNC: CorruptionSite <-> packages/weft-graph/src/protocol.ts CorruptionSite
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CorruptionSite {
    /// `ExecEvent::PortEmitted` could not be put on the wires (the
    /// program has no such node or port, or a wire cannot read the
    /// value).
    PortEmitted,
    /// `ExecEvent::PortClosed` / `ExecEvent::PortTypeMismatch` names
    /// a port the program does not declare.
    PortClosed,
    /// `ExecEvent::PulsesConsumed` fold path (`parse_absorbed_ids` on
    /// `pulse_ids`).
    PulsesConsumed,
    /// A node lifecycle row (`NodeStarted`, a terminal) names a node
    /// the program does not have, or a firing the fold has no record
    /// for.
    NodeLifecycle,
    /// `ExecEvent::LoopInstantiated` arrived for a LoopIn the program
    /// does not have, or its config and inputs do not make a loop.
    LoopInstantiated,
    /// `ExecEvent::LoopStreamEnded` arrived for a `LoopInstanceKey`
    /// with no preceding `LoopInstantiated`. Writer-order bug or row
    /// loss.
    LoopStreamEnded,
    /// `LoopIterationLaunched` arrived for a `LoopInstanceKey` with no
    /// preceding `LoopInstantiated`, or the launch cannot be rebuilt.
    LoopIterationLaunched,
    /// `LoopOutFired` arrived for a `LoopInstanceKey` with no preceding
    /// `LoopInstantiated`, or its writes cannot be read off the LoopOut
    /// firing.
    LoopOutFired,
    /// `LoopTerminated` arrived for a `LoopInstanceKey` with no preceding
    /// `LoopInstantiated`, or its outward emit cannot be rebuilt.
    LoopTerminated,
    /// A journal row whose JSON no longer decodes to any `ExecEvent`
    /// at all (the display read surfaces it; state-rebuilding reads
    /// refuse the whole log instead).
    UndecodableRow,
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
/// once, at the frames its key names. The optional `payload` carries
/// the wake event's data for the firing trigger (the HTTP body, the SSE
/// event JSON, the form submission, the timer info); node bodies read
/// it via the `ctx.wake` bag in Fire phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KickedNode {
    /// This kick IS the firing trigger of the execution. Explicit,
    /// never inferred from payload presence: a fire with an empty
    /// body has a `null` payload, which `Option<Value>` cannot keep
    /// apart from "no payload" across a JSON round trip.
    #[serde(default)]
    pub firing: bool,
    /// Wake event payload for the firing trigger. `None` for every
    /// other kicked root.
    pub payload: Option<Value>,
    /// The firing trigger's setup-time port snapshot, seeded onto its
    /// ports at dispatch. `None` for every other kicked root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_snapshot: Option<Value>,
    /// Flips to `true` once the engine has dispatched this kick (the
    /// node started at the kick's frames). A second tick that sees
    /// `dispatched` must NOT re-dispatch.
    #[serde(default)]
    pub dispatched: bool,
    /// The scope this kick belongs to was gated off: the node is
    /// dispatched straight into a `ScopeSkipped` skip, so every node
    /// inside a scope that did not run says so in the journal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_skipped: Option<String>,
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

/// Why a stream ended: the wire vocabulary shared by the generator
/// runtime (`crate::generator` re-exports it), the journal's loop
/// snapshots, and the fold. `Finished` is the clean end; `Failed`
/// carries the producer's error so a consumer's pull surfaces it
/// through `?` instead of reading a truncated stream as complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamEnd {
    Finished,
    Failed { error: String },
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
