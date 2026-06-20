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
    /// Tenant this signal belongs to. A pooled listener pod holds
    /// signals from many tenants, so tenancy is a property of each
    /// signal, not of the pod. The listener stamps this tenant onto the
    /// `FireSignal` task it enqueues when the signal fires, so the
    /// broker authorizes the cross-tenant write (the listener is a
    /// trusted control-plane caller) and the dispatcher routes the fire
    /// to the right tenant. The dispatcher already knows the tenant at
    /// register time (it ran `TenantRouter`); it puts it on the wire.
    pub tenant_id: String,
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
    /// The placement generation under which this pod holds the signal.
    /// The dispatcher bumps it on every (re)placement and tells the
    /// holding pod its value here. The pod stamps it on every held-event
    /// `FireSignal` it enqueues; the broker drops a fire whose generation
    /// is below the signal row's current one, so a stale old-pod fire
    /// during a scale-down move overlap is fenced out (no double-fire).
    pub placement_generation: i64,
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

/// Load report for `GET /load`. The dispatcher's placement reads this
/// to decide whether a listener can accept another signal. `saturated`
/// is the listener's OWN call from real measurements (the dispatcher
/// never second-guesses it with a count): when true, placement skips
/// this pod and tries another / spawns one. The raw counts are for
/// observability and tie-breaking among non-saturated pods (prefer the
/// least-loaded).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadReport {
    /// True when the pod has hit its memory saturation threshold
    /// (`mem_pressure >= SATURATION_MEM_FRACTION`) and must not accept
    /// new signals. `/register` also returns 503 when this is true, so a
    /// placement race that registers anyway fails loudly rather than
    /// overloading the pod.
    pub saturated: bool,
    /// Real memory pressure (usage/limit) in `[0.0, 1.0]`. The metric
    /// `saturated` is derived from, and the headroom the scale-down
    /// planner uses to decide whether a drained pod's load fits on the
    /// survivors. 0.0 when uncapped (local dev) or on a read glitch.
    pub mem_pressure: f64,
    /// Total signals held (placement count). Observability + tie-break
    /// among non-saturated pods (prefer the least-loaded).
    pub signals: u32,
    /// Signals running a live held-connection loop (Timer/SSE/poll/
    /// socket): the resource-heavy subset. Observability only now that
    /// saturation is memory-based.
    pub held_connections: u32,
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
    /// (duplicate fire, stateful kind misused). The optional `reason`
    /// is for ops logging only; the dispatcher treats every Drop the
    /// same.
    Drop { reason: Option<String> },
    /// This pod does NOT hold the signal for `token` in its registry.
    /// Distinct from `Drop` (a deliberate consume): it means the
    /// dispatcher routed the fire to the wrong pod, which happens during
    /// a scale-down move (the signal was re-placed onto another pod and
    /// the routing column flipped between the dispatcher's resolve and
    /// its POST). The dispatcher re-resolves the holder from the durable
    /// row and retries ONCE; because a move flips the routing column to
    /// the new pod BEFORE unregistering the old one, the re-resolve is
    /// guaranteed to find the live holder. If the signal row is gone, the
    /// re-resolve fails loud (a real inconsistency, not a silent drop).
    NotHeld,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> SignalSpec {
        SignalSpec {
            kind: "timer".into(),
            config: serde_json::json!({ "interval_secs": 60 }),
            consumer_kind: None,
        }
    }

    /// `RegisterRequest` crosses the dispatcher -> listener HTTP boundary
    /// and gained two REQUIRED fields in the pooled rework (`tenant_id`,
    /// `placement_generation`, neither `#[serde(default)]`). Round-trip
    /// pins them on the wire so a rename / drop is a test failure, not a
    /// runtime deserialize error on the listener.
    #[test]
    fn register_request_round_trips_with_tenant_and_generation() {
        let req = RegisterRequest {
            token: "tok-1".into(),
            tenant_id: "acme".into(),
            spec: spec(),
            node_id: "node-1".into(),
            is_resume: false,
            color: Some("c-1".into()),
            placement_generation: 7,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["tenant_id"], "acme");
        assert_eq!(json["placement_generation"], 7);
        let back: RegisterRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back.tenant_id, "acme");
        assert_eq!(back.placement_generation, 7);
        assert_eq!(back.token, "tok-1");
    }

    /// A required new field must be a hard deserialize failure when
    /// absent (the dispatcher and listener must agree on the contract).
    #[test]
    fn register_request_missing_generation_fails() {
        let json = serde_json::json!({
            "token": "tok-1",
            "tenant_id": "acme",
            "spec": { "kind": "timer", "config": {} },
            "node_id": "node-1",
            "is_resume": false,
            "color": null
            // placement_generation omitted
        });
        assert!(serde_json::from_value::<RegisterRequest>(json).is_err());
    }

    /// `LoadReport` is deserialized from the listener's `GET /load` by
    /// the dispatcher's placement; round-trip pins every field.
    #[test]
    fn load_report_round_trips() {
        let lr = LoadReport {
            saturated: true,
            mem_pressure: 0.83,
            signals: 12,
            held_connections: 3,
        };
        let json = serde_json::to_string(&lr).unwrap();
        let back: LoadReport = serde_json::from_str(&json).unwrap();
        assert!(back.saturated);
        assert_eq!(back.mem_pressure, 0.83);
        assert_eq!(back.signals, 12);
        assert_eq!(back.held_connections, 3);
    }

    /// `ProcessTarget` is serialized over the fire path; the new
    /// `NotHeld` variant and the `Drop { reason }` shape must keep their
    /// tag spelling (the dispatcher matches on them).
    #[test]
    fn process_target_round_trips_including_not_held() {
        for target in [
            ProcessTarget::Entry,
            ProcessTarget::Resume { color: "c-1".into() },
            ProcessTarget::Drop { reason: Some("dup".into()) },
            ProcessTarget::NotHeld,
        ] {
            let json = serde_json::to_string(&target).unwrap();
            let back: ProcessTarget = serde_json::from_str(&json).unwrap();
            assert_eq!(format!("{back:?}"), format!("{target:?}"));
        }
    }
}
