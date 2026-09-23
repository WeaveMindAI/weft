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
//!   connections, held sockets). When a held event fires,
//!   the listener enqueues a `FireSignal` task via the broker; the
//!   dispatcher's task picker drives it through the same routing as
//!   a stateless fire.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::primitive::{SignalRouting, SignalSpec};

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
    /// The PLACE this signal is registered at, spelled the way a person
    /// writes the node (`door`, or `one.door` inside the file the site
    /// `one` includes): the same string the dispatcher keys the signal
    /// row by. The kind's `render` shows it to a consumer as the
    /// task's node; a fire never echoes it (the dispatcher reads the
    /// row by token).
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
    /// Where routing and kind_state come from (see [`RegisterSource`]).
    pub source: RegisterSource,
}

/// Where a registration's routing and kind_state come from.
///
/// `Fresh` is the register/reactivate path: the kind computes routing
/// and its initial state; `prior_kind_state`
/// carries the token's previously-persisted state when the row already
/// exists (entry tokens are reused across reactivates), so a kind
/// whose state is a feed cursor can carry it forward instead of
/// re-priming and silently discarding everything that arrived while
/// the project was inactive.
///
/// `Restore` is the pod-move path (scale-down drain, fire
/// re-placement): both values come from the durable row VERBATIM. The
/// row is what the dispatcher routes by, so a recomputed routing could
/// drift from what fires actually arrive at; recomputing state would
/// reset a timer's clock mid-schedule. A move is not a user action, so
/// nothing may change.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RegisterSource {
    Fresh {
        #[serde(default)]
        prior_kind_state: Option<serde_json::Value>,
        /// The kind_state write-fence version the prior state was
        /// read at (0 for a brand-new token). The spawned task's
        /// durable cursor writes continue at `seq + 1` so a
        /// reactivate can never regress the fence.
        #[serde(default)]
        prior_seq: i64,
        /// When the registration was asked for, in ms since the epoch:
        /// the moment a node called `await_signal`, or the activation.
        /// A relative wait counts from here, so the trip through the
        /// dispatcher to this pod is not added to it.
        asked_at_unix_ms: i64,
    },
    Restore {
        routing: SignalRouting,
        kind_state: serde_json::Value,
        /// The row's `kind_state_seq` at restore time.
        #[serde(default)]
        seq: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    /// Listener-computed routing + auth metadata for this signal.
    /// The dispatcher copies surface_kind, mount_path, mount_methods,
    /// auth_kind, auth_config onto the signal row. No secret ever
    /// rides here: a gated route names the connection that holds it.
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

/// Body for `POST /live` on the listener. The dispatcher proxies a
/// read of what a trigger is showing here, whether the reader is the
/// editor or an outside client holding a signal token; the listener
/// has no public surface of its own and authorizes nothing, so the
/// dispatcher has already decided the reader may see this. Looked up
/// by token, not by node_id, because the in-RAM registry is keyed by
/// token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveRequest {
    pub token: String,
    /// The address an outside caller reaches this signal at, when it
    /// has one. The listener cannot work this out: it holds the bare
    /// path the kind computed, while the real address carries the
    /// dispatcher's own host, the tenant segment that walls one
    /// account's paths off from another's, and the `/connect/` prefix
    /// a held-connection kind is served under. All three live on the
    /// dispatcher, which reads them off the signal row
    /// (`SignalRegistration::public_url`) and sends the finished
    /// address here. Absent for a signal nothing calls in to.
    #[serde(default)]
    pub address: Option<String>,
}

/// What the trigger's kind shows, in the shape every node's display
/// uses (`weft_core::live::LiveFeed`), so a reader draws a trigger's
/// panel and an infra container's panel with one renderer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveResponse {
    pub live: crate::live::LiveFeed,
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

/// Body for `/wake_by_hand`: which signal a person is waking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeByHandRequest {
    pub token: String,
}

/// What that signal wakes with, or `None` when its kind cannot be woken
/// by hand at all (almost all of them: there is nothing truthful to
/// invent in place of the answer a form is waiting for).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeByHandResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

/// One verified push a provider delivered to the public events
/// receiver, as the listener is asked about it.
///
/// The dispatcher owns the public door and the "is this genuine"
/// verdict, and it stops there: which registered signals a push feeds
/// is a question about a KIND's own vocabulary (a topic name, a
/// subscription scope), and the kinds live here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushEvent {
    /// The service the push arrived for, as its access recipe names it.
    pub service: String,
    /// The topic within that service.
    pub topic: String,
    /// The named event the broker built from the raw delivery: the
    /// topic's declared fields and nothing else.
    pub event: Value,
}

/// Body sent by the dispatcher to listener `/match_push`: one push,
/// and the signals held by THIS pod that might be fed by it.
///
/// Batched per pod rather than per signal, because one account-routed
/// push can feed many subscriptions and a call each would make the
/// provider wait on a round trip per candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchPushRequest {
    pub push: PushEvent,
    pub tokens: Vec<String>,
}

/// One signal the push feeds, with the payload it wakes with.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchedPush {
    pub token: String,
    pub payload: Value,
}

/// Which of the offered signals the push feeds. A token this pod does
/// not hold, whose kind is not fed by pushes, whose kind says the push
/// does not address it, or whose filter refuses the payload, is simply
/// absent: a push that feeds nothing here is an empty list, never an
/// error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchPushResponse {
    pub matched: Vec<MatchedPush>,
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
        SignalSpec::of_kind("timer", serde_json::json!({ "interval_secs": 60 }))
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
            source: RegisterSource::Fresh {
                prior_kind_state: Some(serde_json::json!({"cursor": 42})),
                prior_seq: 9,
                asked_at_unix_ms: 1_700_000_000_123,
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["tenant_id"], "acme");
        assert_eq!(json["placement_generation"], 7);
        assert_eq!(json["source"]["prior_kind_state"]["cursor"], 42);
        let back: RegisterRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back.tenant_id, "acme");
        assert_eq!(back.placement_generation, 7);
        assert_eq!(back.token, "tok-1");
        match back.source {
            RegisterSource::Fresh { prior_kind_state, prior_seq, asked_at_unix_ms } => {
                assert_eq!(prior_kind_state.unwrap()["cursor"], 42);
                assert_eq!(prior_seq, 9);
                assert_eq!(asked_at_unix_ms, 1_700_000_000_123);
            }
            RegisterSource::Restore { .. } => panic!("round trip flipped the source"),
        }
    }

    /// A request must say where its state comes from, and a fresh one
    /// when it was asked for: without it a relative timer would count
    /// from a guess. A Restore round-trips its routing and state
    /// verbatim.
    #[test]
    fn register_source_is_required_and_restore_round_trips() {
        let mut json = serde_json::json!({
            "token": "tok-1",
            "tenant_id": "acme",
            "spec": { "kind": "timer", "config": {} },
            "node_id": "node-1",
            "placement_generation": 7
        });
        assert!(serde_json::from_value::<RegisterRequest>(json.clone()).is_err(), "no source");
        json["source"] = serde_json::json!({ "kind": "fresh" });
        assert!(serde_json::from_value::<RegisterRequest>(json).is_err(), "a fresh source with no asked_at");

        let restore = RegisterSource::Restore {
            routing: SignalRouting {
                surface: crate::primitive::SignalSurface::Internal,
                auth: crate::primitive::SignalAuth::None,
                auth_config: serde_json::Value::Null,
            },
            kind_state: serde_json::json!({"cursor": 7}),
            seq: 12,
        };
        let json = serde_json::to_value(&restore).unwrap();
        let back: RegisterSource = serde_json::from_value(json).unwrap();
        match back {
            RegisterSource::Restore { kind_state, seq, .. } => {
                assert_eq!(kind_state["cursor"], 7);
                assert_eq!(seq, 12);
            }
            RegisterSource::Fresh { .. } => panic!("round trip flipped the source"),
        }
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
