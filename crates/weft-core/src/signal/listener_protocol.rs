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
//!   something stays up), which claims its row. When an event the
//!   listener raises itself fires (a timer's tick, an event on a held
//!   connection), the listener routes it the way the dispatcher routes
//!   a `/process` outcome: an entry's event straight to its project's
//!   worker door, an answer to a waiting run as a `FireSignal` task
//!   through the broker, which the dispatcher's task picker runs.

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
    /// True iff this signal is a mid-run resume (HumanQuery
    /// awaiting form submission, etc) rather than an entry trigger.
    /// The listener uses this to decide which `ProcessTarget`
    /// discriminant to return at fire time. Without it, dual-use
    /// kinds like Form can't tell resume from entry.
    #[serde(default)]
    pub is_resume: bool,
    /// Run of the suspended run to resume, present iff
    /// `is_resume`. Echoed back into `ProcessTarget::Resume`.
    #[serde(default)]
    pub run_id: Option<crate::RunId>,
    /// Where the registration's kind_state starts from (see
    /// [`PrepareSource`]).
    pub source: PrepareSource,
    /// Whose signal it is (`None` for a shared one): an instance's signal
    /// reads its connection through that instance's alone, here as it
    /// will once up.
    #[serde(default)]
    pub for_instance: Option<crate::instance::InstanceScope>,
}

/// `POST /check`: whether each of these entry signals could be registered
/// and come up on this install, asked before what they would replace is
/// taken down (a resync's bring-back), registering and starting nothing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckRequest {
    pub signals: Vec<CheckSignal>,
}

/// One entry signal a [`CheckRequest`] asks about, as it would be
/// registered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckSignal {
    pub token: String,
    pub tenant_id: String,
    /// Its place, spelled the way the program writes it.
    pub node_id: String,
    pub spec: SignalSpec,
    #[serde(default)]
    pub for_instance: Option<crate::instance::InstanceScope>,
}

/// What `POST /check` answers: the signals that could not come up, each
/// with why. Empty: every one could, as far as anything short of asking
/// an outside service can tell.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckResponse {
    #[serde(default)]
    pub refused: Vec<CheckRefused>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRefused {
    pub token: String,
    pub node_id: String,
    pub why: String,
}

/// Where a registration's kind_state starts from: fresh, or from the row
/// it replaces. The listener decides which, the same way for every kind
/// (`weft_listener::kinds::prepare_signal`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareSource {
    /// The row already under the token, when there is one (an entry's
    /// token is reused each time its trigger is armed again; a resume's
    /// row is there when its registration is retried).
    #[serde(default)]
    pub prior: Option<PriorRow>,
    /// When the registration was asked for, in ms since the epoch: the
    /// moment a node called `await_signal`, or the activation. A relative
    /// wait counts from here, so the trip through the dispatcher to the
    /// listener is not added to it.
    pub asked_at_unix_ms: i64,
}

/// The row a registration replaces, as much of it as deciding what to
/// carry over takes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorRow {
    pub spec: SignalSpec,
    pub kind_state: Value,
    /// Whether its trigger listened the whole time since this row was last
    /// served: it was on, or down behind a door that takes or hides what
    /// arrives, or down for weft's own maintenance. A door that refused,
    /// or whose window ended, did not listen, and what arrived behind it
    /// is never caught up.
    pub listened: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareResponse {
    /// Listener-computed routing + auth metadata for this signal.
    /// The dispatcher copies surface_kind, mount_path, mount_methods,
    /// auth_kind, auth_config onto the signal row. No secret ever
    /// rides here: a gated route names the connection that holds it.
    pub routing: SignalRouting,
    /// Opaque per-kind state the kind starts from (fresh, or carried from
    /// `PrepareSource::prior`). The dispatcher writes it
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

/// Body for `POST /fresh_places` on the listener: the entry signals whose
/// trigger stops listening behind a door that refuses, each by its token
/// and spec. Nothing starts and nothing is held.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshPlacesRequest {
    pub signals: Vec<FreshPlaceAsk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshPlaceAsk {
    pub token: String,
    pub spec: SignalSpec,
}

/// Answer to `POST /fresh_places`: the state each asked signal whose kind
/// keeps its place in a source (a poll's cursor, what a folder held)
/// starts from with that place gone. A signal of any other kind is not
/// listed: its state stays as it is.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshPlacesResponse {
    pub places: Vec<FreshPlace>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshPlace {
    pub token: String,
    pub kind_state: Value,
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
    /// The row this one replaced under the same token, when the write
    /// that came before this start replaced one (a registration again, or
    /// the put-back of the row a failed one replaced): its wakes are
    /// cancelled before the new row's are set, so a wake set for the old
    /// spec never goes off. `None` for a token's first row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaced: Option<ReplacedSignal>,
}

/// What a start needs of the row it replaced to cancel the wakes that row
/// set: the moments are computed from its spec and state, and named with
/// its registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplacedSignal {
    pub spec: SignalSpec,
    pub kind_state: Value,
    pub generation: i64,
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
    /// For a run's wait: the last answer it saw from outside (a poll of a
    /// job's status), for a reader that parses it (`GetNodeState`'s
    /// `live`). `None` before the first one, and for a kind with no answer
    /// to keep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<LastAnswer>,
}

/// What a run's wait keeps of the last answer it saw from outside: the
/// answer as it came, unparsed, or only its size when it is past what a
/// wait keeps, so a reader is told it is missing rather than handed part
/// of it or nothing. A program reads it through `crate::program::NodeState`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LastAnswer {
    Kept(String),
    /// Its size in bytes.
    TooLarge(u64),
}

/// Body for `POST /cancel_call` on the listener: send the last call of a
/// wait whose run was cancelled (`crate::signal::cancel_call`). The wait's
/// row is gone by now (the cancel took it down), so everything the call
/// needs rides here: the spec carries the call and the connection it is
/// sent as.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelCallRequest {
    /// The token the wait was registered under. The broker answers where
    /// an infra address in the call is reached for it while the call is
    /// queued.
    pub token: String,
    pub tenant_id: String,
    /// Whose run waited, when it was an instance's: its connection is
    /// read through that instance's alone, as the wait's polls were.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_instance: Option<crate::instance::InstanceScope>,
    pub spec: SignalSpec,
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
    /// The removed row's registration (`signal.generation`), which names
    /// the wakes it had set, so they are cancelled with it. The listener
    /// stops only this registration (or an older one) of the token: a
    /// token can be registered again, and an unregister that arrives after
    /// a newer registration came up leaves that one running.
    pub generation: i64,
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

/// What a signal's kind made of one event, from the listener's
/// `/process` (an event that reached the install's doors) or from the
/// listener's own fires (a timer's tick, an event on a held connection).
/// Two orthogonal facts in one shape:
///
///   - `value`: what the kind computed from the payload (most kinds
///     echo it). Whoever acts on the outcome hands it on unchanged: as
///     the fire's payload at an entry's worker door, or as the answer a
///     waiting run receives.
///
///   - `target`: where the event goes. This is kind-unaware: every kind
///     picks one of the same three targets, so new kinds land without
///     touching the dispatcher's routing.
///
/// The dispatcher is pure transport (it reads `target` and acts); the
/// listener is the kind-aware processor (it computes `value`). Both read
/// `target` through [`ProcessTarget::for_signal`], so both refuse the same
/// shapes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessOutcome {
    pub value: Value,
    pub target: ProcessTarget,
}

/// A kind's target that does not fit its signal: an answer to a waiting
/// run made from an entry's event, or an entry's event made from a wait's
/// answer. The signal row says which the signal is (`is_resume`), and the
/// kind contradicted it, so the event goes nowhere.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MisroutedEvent {
    #[error("signal {token} is an entry, and its kind made an answer to a waiting run of its event")]
    AnswerFromEntry { token: String },
    #[error("signal {token} is a run's wait, and its kind made an entry's event of its answer")]
    EntryFromWait { token: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessTarget {
    /// Answer the wait of a run: the install hands the value to its run
    /// (`weft_dispatcher::api::signal::answer_run`). The wait is found by
    /// its token; only the run is echoed here.
    Resume { run_id: crate::RunId },
    /// Start a fresh run as an entry trigger: the event goes to
    /// the worker's door of the trigger's program (`weft_core::door_fire`),
    /// which reads the trigger by token.
    Entry,
    /// Listener consumed the fire; dispatcher does nothing. Covers
    /// Hold (multi-step protocol still in progress) AND NoOp
    /// (duplicate fire, stateful kind misused). The optional `reason`
    /// is for ops logging only; the dispatcher treats every Drop the
    /// same.
    Drop { reason: Option<String> },
}

impl ProcessTarget {
    /// This target, checked against the signal it was made for: `token`'s
    /// row says whether it is a run's wait (`is_resume`). A wait's event can
    /// only answer its run and an entry's can only start one; either may be
    /// dropped. The one place that decides it, for the dispatcher acting on
    /// `/process` and the listener acting on its own fires alike.
    pub fn for_signal(self, token: &str, is_resume: bool) -> Result<Self, MisroutedEvent> {
        match (&self, is_resume) {
            (Self::Resume { .. }, false) => Err(MisroutedEvent::AnswerFromEntry { token: token.to_string() }),
            (Self::Entry, true) => Err(MisroutedEvent::EntryFromWait { token: token.to_string() }),
            (Self::Resume { .. }, true) | (Self::Entry, false) | (Self::Drop { .. }, _) => Ok(self),
        }
    }
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
            run_id: Some(uuid::Uuid::nil()),
            source: PrepareSource {
                prior: Some(PriorRow { spec: spec(), kind_state: serde_json::json!({"cursor": 42}), listened: true }),
                asked_at_unix_ms: 1_700_000_000_123,
            },
            for_instance: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["tenant_id"], "acme");
        assert_eq!(json["source"]["prior"]["kind_state"]["cursor"], 42);
        assert_eq!(json["source"]["prior"]["listened"], true);
        assert_eq!(json["source"]["prior"]["spec"]["kind"], "timer");
        let back: PrepareRequest = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(back.tenant_id, "acme");
        assert_eq!(back.token, "tok-1");
        let prior = back.source.prior.unwrap();
        assert_eq!((prior.kind_state["cursor"].as_i64(), prior.listened, prior.spec.kind.as_str()), (Some(42), true, "timer"));
        assert_eq!(back.source.asked_at_unix_ms, 1_700_000_000_123);
        // A prior row must say whether its trigger listened: read as
        // either, it would carry or drop a place on a guess.
        let mut unsaid = json;
        unsaid["source"]["prior"].as_object_mut().unwrap().remove("listened");
        assert!(serde_json::from_value::<PrepareRequest>(unsaid).is_err());
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

    /// A cancel call carries the wait's whole spec across: the call
    /// itself and the connection it is sent as, since the row is gone.
    #[test]
    fn a_cancel_call_request_carries_the_call_and_its_connection() {
        let mut spec = SignalSpec::of_kind("poll_endpoint", serde_json::json!({ "url": "https://x/status" }));
        spec.access = Some(crate::primitive::AccessRef { id: "a1".into(), service: "fal".into(), required_values: Vec::new() });
        spec.on_cancel = Some(crate::signal::CancelCall::put("https://x/cancel"));
        let req = CancelCallRequest { token: "t".into(), tenant_id: "acme".into(), for_instance: None, spec };
        let wire = serde_json::to_value(&req).unwrap();
        assert_eq!(wire["spec"]["onCancel"], serde_json::json!({ "url": "https://x/cancel", "method": "put" }));
        let back: CancelCallRequest = serde_json::from_value(wire).unwrap();
        assert_eq!(back.spec.on_cancel, Some(crate::signal::CancelCall::put("https://x/cancel")));
        assert_eq!(back.spec.access.map(|a| a.id), Some("a1".to_string()));
    }

    /// `ProcessTarget` is serialized over the fire path; every variant
    /// must keep its tag spelling (the dispatcher matches on them).
    #[test]
    fn process_target_round_trips() {
        for target in [
            ProcessTarget::Entry,
            ProcessTarget::Resume { run_id: uuid::Uuid::nil() },
            ProcessTarget::Drop { reason: Some("dup".into()) },
        ] {
            let json = serde_json::to_string(&target).unwrap();
            let back: ProcessTarget = serde_json::from_str(&json).unwrap();
            assert_eq!(format!("{back:?}"), format!("{target:?}"));
        }
    }

    /// A wait's event only answers its run, an entry's only starts one,
    /// and either may be dropped; the other two shapes are refused.
    #[test]
    fn a_target_must_fit_its_signal() {
        let resume = || ProcessTarget::Resume { run_id: uuid::Uuid::nil() };
        let drop = || ProcessTarget::Drop { reason: None };
        assert!(matches!(resume().for_signal("t", true), Ok(ProcessTarget::Resume { .. })));
        assert!(matches!(ProcessTarget::Entry.for_signal("t", false), Ok(ProcessTarget::Entry)));
        assert!(matches!(drop().for_signal("t", true), Ok(ProcessTarget::Drop { .. })));
        assert!(matches!(drop().for_signal("t", false), Ok(ProcessTarget::Drop { .. })));
        assert_eq!(resume().for_signal("t", false).unwrap_err(), MisroutedEvent::AnswerFromEntry { token: "t".into() });
        assert_eq!(ProcessTarget::Entry.for_signal("t", true).unwrap_err(), MisroutedEvent::EntryFromWait { token: "t".into() });
    }
}
