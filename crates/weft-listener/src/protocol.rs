//! Wire types shared between the listener and the dispatcher.
//!
//! Routing model:
//! - dispatcher hosts every external-facing URL (webhooks, forms,
//!   public form URLs). Incoming fires arrive at the dispatcher.
//! - dispatcher relays each stateless fire to the listener's
//!   `/process` endpoint, which returns a `ProcessOutcome` (value +
//!   target) the dispatcher acts on.
//! - dispatcher calls the listener's `/render` once at register time
//!   to compute the consumer-facing payload for a token; the result
//!   is cached on the signal row.
//! - listener owns kind-specific state (timer schedules, SSE
//!   connections, future browser sessions). When a held event fires,
//!   the listener enqueues a `FireSignal` task via the broker; the
//!   dispatcher's task picker drives it through the same routing as
//!   a stateless fire.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use weft_core::primitive::{SignalRouting, SignalSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// Opaque token the dispatcher minted. Used as the routing key.
    pub token: String,
    /// The resolved signal spec. Carries everything kind-specific.
    pub spec: SignalSpec,
    /// Node id this signal belongs to. Relayed back to the
    /// dispatcher on fire so it can attribute the event.
    pub node_id: String,
    /// True iff this signal is a mid-execution resume (HumanQuery
    /// awaiting form submission, etc) rather than an entry trigger.
    /// The listener uses this to decide which `ProcessTarget`
    /// discriminant to return at fire time. Without it, dual-use
    /// kinds like Form can't tell resume from entry.
    #[serde(default)]
    pub is_resume: bool,
    /// Color of the suspended execution to resume, present iff
    /// `is_resume`. Echoed back into `ProcessTarget::Resume`.
    #[serde(default)]
    pub color: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    /// Listener-computed routing + auth metadata for this signal.
    /// The dispatcher copies surface_kind, mount_path, auth_kind,
    /// auth_config onto the signal row. Plaintext secrets the kind
    /// minted live in the listener's secret_cache and are served via
    /// `/display`; they don't travel back through this response.
    pub routing: SignalRouting,
    /// Opaque per-kind state computed at register time. The
    /// dispatcher persists it on the signal row and ships it back
    /// on rehydrate so stateful kinds (Timer) survive a listener
    /// restart without resetting their schedule. `{}` for kinds
    /// that don't need it.
    #[serde(default)]
    pub kind_state: serde_json::Value,
}

/// Body for `POST /display` on the listener (admin-only). The
/// dispatcher proxies inspector reads here; the listener returns
/// whatever the kind impl wants to show (mount_path, just-minted
/// plaintext, etc). Looked up by token, not by node_id, because
/// the listener's in-RAM registry is keyed by token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayRequest {
    pub token: String,
}

/// Free-form display payload returned from the listener. Inspector
/// renders kind-specific. Standard fields the inspector knows
/// about: `mount_path`, `auth: { kind, header_name }`, `secret`
/// (plaintext, only present when minted recently and listener
/// still holds it in RAM).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayResponse {
    pub display: Value,
}

/// Body for `POST /action` on the listener (admin-only, project-
/// token gated at the dispatcher). The kind impl picks `kind`
/// names: e.g. `regenerate_api_key`. Generic dispatch shape so
/// new actions land without touching the listener's routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionRequest {
    pub token: String,
    /// Action name; the kind impl decides what it means.
    pub kind: String,
    /// Optional kind-specific payload.
    #[serde(default)]
    pub payload: Value,
}

/// Generic action response. The kind impl owns the JSON shape;
/// the dispatcher passes it through to the inspector verbatim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResponse {
    pub result: Value,
    /// `routing` is updated routing metadata when the action
    /// changed it (e.g. regenerate_api_key updates the value_hash
    /// on auth_config). The dispatcher writes the new fields onto
    /// the signal row when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<SignalRouting>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterRequest {
    pub token: String,
}

/// Body sent by the dispatcher to listener `/process` on every
/// stateless signal fire (webhook, form submission, etc).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessRequest {
    pub token: String,
    pub payload: Value,
}

/// Outcome of one stateless fire's listener-side processing. Two
/// orthogonal facts in one shape:
///
///   - `value`: what the listener computed from the payload. Today
///     most kinds echo the raw payload; future kinds may validate
///     against a schema, decorate with metadata, or shape across a
///     multi-step protocol. The dispatcher writes this verbatim
///     into the journal.
///
///   - `target`: where the dispatcher should route this fire. This
///     is kind-unaware: every kind picks one of the same three
///     targets. New routing targets land without touching any kind
///     module; new kinds land without touching dispatcher routing.
///
/// Splitting these matches the architecture: dispatcher is pure
/// transport (it reads `target` and acts), listener is the
/// kind-aware processor (it computes `value` from payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessOutcome {
    pub value: Value,
    pub target: ProcessTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessTarget {
    /// Resume a suspended execution. Dispatcher journals
    /// SuspensionResolved + enqueues a resume task. node_id is
    /// looked up from the signal row by token; it isn't echoed
    /// here.
    Resume { color: String },
    /// Start a fresh execution as an entry trigger. Dispatcher
    /// enqueues route_entry; node_id is looked up from the signal
    /// row by token.
    Entry,
    /// Listener consumed the fire; dispatcher does nothing. Covers
    /// Hold (multi-step protocol still in progress) AND NoOp
    /// (duplicate fire, unknown token, stateful kind misused). The
    /// optional `reason` is for ops logging only; the dispatcher
    /// treats every Drop the same.
    Drop { reason: Option<String> },
}
