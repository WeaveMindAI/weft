use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Color;

// ----- Wake signals (unified trigger + suspension mechanism) ----------
//
// A wake signal is "something the listener listens for on behalf of
// a node." When it fires, the dispatcher either spawns a fresh run
// (entry path: `register_signal`) or resumes a paused lane (resume
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
    /// Kind tag (e.g. `"webhook"`, `"timer"`). Matched against the
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
    /// Used by Webhook, ApiPost, future public-form-like kinds.
    PublicEntry { path: String },
    /// Per-task callback. Mounted at `/signal/<token>` where the
    /// dispatcher mints the UUID at register time. Used by
    /// task-style signals (HumanQuery, future task-callback kinds)
    /// where each fire is a one-shot reply to a specific suspended
    /// lane and the URL is internal to the consumer flow.
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
// Written by the worker when it stalls (all lanes either terminal or
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
    /// Fires that arrived for live suspensions but haven't been
    /// consumed by a worker's node completion yet. The worker
    /// seeds these into its link on startup so every waiting node
    /// finds its value when re-dispatched. Survives worker restarts
    /// because it's derived from journal events, not slot queues.
    #[serde(default)]
    pub pending_deliveries: HashMap<String, Value>,
    /// Per-(node, lane) ordered sequence of past `await_signal`
    /// calls. Each entry has the call_index (0-based ordinal of
    /// the call within the body), the token, and either the
    /// resolved value (if the corresponding fire arrived) or None
    /// (still pending; this is the live suspension).
    ///
    /// On replay, the runtime pre-loads this sequence per
    /// (node, lane); each `await_signal` call within the body
    /// pops the next entry and either returns its resolved value
    /// instantly OR re-suspends if pending. This is what makes
    /// multiple sequential awaits within one node body work.
    #[serde(default)]
    pub awaited_sequences: HashMap<(String, crate::lane::Lane), Vec<AwaitedEntry>>,
}

/// One entry in the per-(node, lane) replay sequence rebuilt by
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

/// Per-paused-lane info stored in the snapshot. `token` is the key
/// in the outer HashMap. Enough to: identify the waiting node/lane,
/// re-register the signal on every fresh worker boot, and route the
/// delivered value back to the right oneshot when the fire arrives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuspensionInfo {
    pub node_id: String,
    pub lane: crate::lane::Lane,
    pub spec: SignalSpec,
    pub created_at_unix: u64,
    /// 0-based ordinal of the `await_signal` call within this
    /// (node_id, lane). The runtime uses this on replay to put
    /// resolved values back in the right order.
    #[serde(default)]
    pub call_index: u32,
}

/// Root seed for manual runs. Pulse is synthesized on the `__seed__`
/// port; nodes with no inputs become ready immediately. The
/// dispatcher mints `pulse_id` and journals the same UUID in the
/// `PulseSeeded` event, so a fresh worker's fold reconstructs the
/// seed pulse with the same identity the live worker used and
/// `NodeStarted.pulses_absorbed` matches by exact UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootSeed {
    pub node_id: String,
    pub pulse_id: String,
    #[serde(default)]
    pub value: Value,
}

