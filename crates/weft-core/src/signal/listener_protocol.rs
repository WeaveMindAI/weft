//! Wire types shared between the listener and the dispatcher.
//!
//! Routing model:
//! - dispatcher hosts every external-facing URL (webhooks, forms,
//!   public form URLs). Incoming fires arrive at the dispatcher.
//! - dispatcher relays each stateless fire to the listener's
//!   `/process` endpoint, which returns a `ProcessOutcome` (value +
//!   target) the dispatcher acts on.
//! - a registration is two calls: `/prepare` computes what the signal
//!   row holds (routing, kind state, the consumer-facing payload) and
//!   starts nothing; once the dispatcher has committed the row,
//!   `/start` brings the signal up (its first wake, its held
//!   connection), so whatever starts always finds its row.
//! - listener owns kind-specific state (timer schedules, SSE
//!   connections, held sockets). A signal that keeps a connection open
//!   between fires is held by a holder (the listener's code where
//!   something stays up), which claims its row. When a held event fires,
//!   the listener enqueues a `FireSignal` task via the broker; the
//!   dispatcher's task picker drives it through the same routing as
//!   a stateless fire.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::primitive::{SignalRouting, SignalSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareRequest {
    /// Opaque token the dispatcher minted. Used as the routing key.
    pub token: String,
    /// Tenant this signal belongs to. A pooled listener process holds
    /// signals from many tenants, so tenancy is a property of each
    /// signal, not of the process. The listener stamps this tenant onto the
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
    /// Execution of the suspended execution to resume, present iff
    /// `is_resume`. Echoed back into `ProcessTarget::Resume`.
    #[serde(default)]
    pub execution_id: Option<String>,
    /// Where the registration's kind_state starts from (see
    /// [`PrepareSource`]).
    pub source: PrepareSource,
    /// Whose signal it is (`None` for a shared one): an instance's signal
    /// reads its connection through that instance's alone, here as it
    /// will once up.
    #[serde(default)]
    pub for_instance: Option<crate::instance::InstanceScope>,
}

/// Where a registration's kind_state starts from: the kind computes
/// routing and its initial state; `prior_kind_state` carries the token's
/// previously-persisted state when the row already exists (entry tokens
/// are reused across reactivates), so a kind whose state is a feed cursor
/// can carry it forward instead of re-priming and silently discarding
/// everything that arrived while the project was inactive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareSource {
    #[serde(default)]
    pub prior_kind_state: Option<serde_json::Value>,
    /// When the registration was asked for, in ms since the epoch: the
    /// moment a node called `await_signal`, or the activation. A relative
    /// wait counts from here, so the trip through the dispatcher to the
    /// listener is not added to it.
    pub asked_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareResponse {
    /// Listener-computed routing + auth metadata for this signal.
    /// The dispatcher copies surface_kind, mount_path, mount_methods,
    /// auth_kind, auth_config onto the signal row. No secret ever
    /// rides here: a gated route names the connection that holds it.
    pub routing: SignalRouting,
    /// Opaque per-kind state the kind starts from (or carries forward
    /// from `PrepareSource::prior_kind_state`). The dispatcher writes it
    /// on the signal row; a listener that restarts reads it back off the
    /// row (`list_held`), so stateful kinds (Timer) keep their schedule.
    /// `{}` for kinds that don't need it.
    #[serde(default)]
    pub kind_state: serde_json::Value,
    /// What a consumer needs to answer this signal (a form's fields),
    /// `Null` for a kind nobody answers by hand. The dispatcher caches
    /// it on the signal row.
    pub rendered: serde_json::Value,
    /// Whether this signal keeps a connection to the outside open between
    /// fires, decided for this signal by its kind. The dispatcher writes it
    /// on the row, where a holder claims it and the number of holders is
    /// counted from.
    pub holds: bool,
}

/// Body for `POST /rehydrate` on the listener: bring up every held
/// signal of `project` it is not running, except those in `skip`. Only
/// the activating project's rows: another project's broken row is not
/// this activation's to fail on, and the walk stays the size of one
/// project. The dispatcher's activate names the rows it is about to
/// delete (triggers the source no longer has), so a row on its way out
/// is never brought up, and one that could not come up does not fail the
/// activation deleting it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RehydrateRequest {
    pub project: uuid::Uuid,
    pub skip: Vec<String>,
}

/// Body for `POST /start` on the listener: bring up the signal whose row
/// the dispatcher just committed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartRequest {
    pub token: String,
    pub mode: StartMode,
}

/// What the row being started is, which decides how hard the kind may
/// fail to come up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartMode {
    /// A registration the dispatcher just wrote: the kind may make broker
    /// and provider calls and refuse loudly, which fails the registration.
    New,
    /// A row the listener finds held and not running here (boot,
    /// rehydrate, a first use, the retry of a down row): it comes up and
    /// its task retries a transient failure. A connection already running
    /// under the token is left as it is: it came up from this same row.
    Restore,
    /// A row the dispatcher put back (the undo of a registration that
    /// replaced it). Whatever runs under the token was started from the
    /// replacement, so it is replaced like `New`; the row ran before, so
    /// it comes up and retries like `Restore`, because refusing here would
    /// turn off a trigger that was live.
    PutBack,
}

impl StartMode {
    /// Whether the kind may make broker and provider calls and refuse
    /// loudly, failing the registration.
    pub fn fresh(self) -> bool {
        self == StartMode::New
    }

    /// Whether a failure to come up is the listener's to retry (the row
    /// was live before), rather than the registering caller's to handle.
    pub fn retried(self) -> bool {
        matches!(self, StartMode::Restore | StartMode::PutBack)
    }
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
    /// The removed row's tenant, spec and kind state: the row is gone
    /// by the time this arrives, and what the signal arranged outside
    /// (a provider subscription it renews on its wakes) is torn down from
    /// them.
    pub tenant_id: String,
    pub spec: SignalSpec,
    pub kind_state: Value,
    /// The token comes back up right after (a registration replaced this
    /// row with one served the other way round): the answer waits for the
    /// teardown, which is keyed by the token, so it is over before anything
    /// serves the token again. Otherwise the answer does not wait on a
    /// provider round trip.
    #[serde(default)]
    pub reused: bool,
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
/// and the signals held by THIS process that might be fed by it.
///
/// Batched per process rather than per signal, because one account-routed
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

/// Which of the offered signals the push feeds. A token this process does
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
    Resume { execution_id: String },
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> SignalSpec {
        SignalSpec::of_kind("timer", serde_json::json!({ "interval_secs": 60 }))
    }

    /// `PrepareRequest` crosses the dispatcher -> listener HTTP
    /// boundary; round-trip pins its required fields on the wire so a
    /// rename or drop is a test failure, not a runtime deserialize error
    /// on the listener.
    #[test]
    fn register_request_round_trips() {
        let req = PrepareRequest {
            token: "tok-1".into(),
            tenant_id: "acme".into(),
            spec: spec(),
            node_id: "node-1".into(),
            is_resume: false,
            execution_id: Some("c-1".into()),
            source: PrepareSource {
                prior_kind_state: Some(serde_json::json!({"cursor": 42})),
                asked_at_unix_ms: 1_700_000_000_123,
            },
            for_instance: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["tenant_id"], "acme");
        assert_eq!(json["source"]["prior_kind_state"]["cursor"], 42);
        let back: PrepareRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back.tenant_id, "acme");
        assert_eq!(back.token, "tok-1");
        assert_eq!(back.source.prior_kind_state.unwrap()["cursor"], 42);
        assert_eq!(back.source.asked_at_unix_ms, 1_700_000_000_123);
    }

    /// A request must say when it was asked for: without it a relative
    /// timer would count from a guess.
    #[test]
    fn register_source_needs_its_asked_at() {
        let mut json = serde_json::json!({
            "token": "tok-1",
            "tenant_id": "acme",
            "spec": { "kind": "timer", "config": {} },
            "node_id": "node-1"
        });
        assert!(serde_json::from_value::<PrepareRequest>(json.clone()).is_err(), "no source");
        json["source"] = serde_json::json!({});
        assert!(serde_json::from_value::<PrepareRequest>(json.clone()).is_err(), "no asked_at");
        json["source"] = serde_json::json!({ "asked_at_unix_ms": 1 });
        assert!(serde_json::from_value::<PrepareRequest>(json).is_ok());
    }

    /// A start must say what it starts: a restore that read as a new
    /// registration would refuse on a transient error and turn off a live
    /// trigger.
    #[test]
    fn start_request_needs_its_mode() {
        assert!(serde_json::from_value::<StartRequest>(serde_json::json!({ "token": "t" })).is_err());
        let back: StartRequest =
            serde_json::from_value(serde_json::json!({ "token": "t", "mode": "restore" })).unwrap();
        assert_eq!(back.mode, StartMode::Restore);
    }

    /// `ProcessTarget` is serialized over the fire path; every variant
    /// must keep its tag spelling (the dispatcher matches on them).
    #[test]
    fn process_target_round_trips() {
        for target in [
            ProcessTarget::Entry,
            ProcessTarget::Resume { execution_id: "c-1".into() },
            ProcessTarget::Drop { reason: Some("dup".into()) },
        ] {
            let json = serde_json::to_string(&target).unwrap();
            let back: ProcessTarget = serde_json::from_str(&json).unwrap();
            assert_eq!(format!("{back:?}"), format!("{target:?}"));
        }
    }
}
