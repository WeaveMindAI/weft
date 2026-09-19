//! Per-kind logic. One module per `Signal` impl in
//! `weft_core::signal`. Each module:
//!
//!   - declares a unit struct (`TimerHandler`, `LiveCallerHandler`, ...)
//!     implementing `KindHandler`.
//!   - parses the spec's opaque `config` blob into the kind's typed
//!     struct from `weft_core::signal`.
//!   - owns its own background task (timer schedule, SSE connect),
//!     `process`, `render` and `compute_routing`.
//!   - registers itself with the inventory at the bottom of the file.
//!
//! Not every file here is a kind: `event_source.rs` is shared machinery
//! (backoff, payload coercion) that several kinds lean on and registers
//! nothing.
//!
//! Adding a new kind = create `kinds/<name>.rs` (handler) + matching
//! file in `weft_core::signal`. The framework discovers it via the
//! inventory at startup; no central match, no enum.
//!
//! Top-level helpers in this file (`register_spec`, `process`,
//! `render`, `compute_routing`, etc.) look up the kind by tag and
//! delegate. They never know about specific kinds.
//!
//! Conventions enforced by the dispatch helpers below:
//!   - Stateful kinds (Timer, SSE) raise their own fires internally
//!     (a tick / an SSE event) and enqueue a `FireSignal` task via the
//!     broker; the dispatcher picker runs it back through `/process`,
//!     where the kind's `process_entry` routes it to
//!     `ProcessTarget::Entry`. (An unknown token still returns Drop;
//!     that's the genuine "no signal here" case.)
//!   - Resume signals (`is_resume = true`) always route to
//!     `ProcessTarget::Resume`; the kind's `process` impl is only
//!     consulted for entry-mode (`is_resume = false`).

pub mod event_source;
pub mod sse_subscribe;
pub mod poll_endpoint;
pub mod provider_events;
pub mod socket_listen;
pub mod stream_listen;
pub mod timer;
pub mod form;
pub mod live_connection;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tokio::task::JoinHandle;
use weft_core::primitive::{SignalRouting, SignalSpec};

use parking_lot::Mutex;

use crate::config::ListenerConfig;
use crate::event_context::FireContext;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{MatchedPush, ProcessOutcome, ProcessTarget, PushEvent};
use crate::registry::{RegisteredSignal, Registry, ServingState, TaskGuard, Transport};

/// Everything a kind's background task is spawned with, bundled: the
/// per-signal identity + fire plumbing (as a ready [`FireContext`],
/// which carries the spec-level pre-fire filter), the listener
/// config, the broker's event-serving client, and whether this is a
/// FRESH registration (a fresh one may make broker calls and refuse
/// activation loudly; a rehydrate must come up and let its task
/// retry, or one unreachable broker at boot would fail the whole
/// rebuild).
#[derive(Clone)]
pub struct SpawnCtx {
    pub fire: FireContext,
    pub config: Arc<ListenerConfig>,
    pub events_broker: Arc<weft_broker_client::BrokerEventsClient>,
    pub fresh: bool,
    /// The kind_state write-fence version the spawned task starts
    /// from: its durable cursor writes continue at `state_seq + 1`.
    /// Carried OUTSIDE the state blob so no handling of the kind's
    /// own state can ever lose the fence version.
    pub state_seq: i64,
    /// The signal's live serving state, shared with the registry
    /// entry so status a serving task writes lands where /display
    /// reads it.
    pub serving: Arc<Mutex<ServingState>>,
}

/// Per-kind handler. One unit struct per kind, registered with the
/// inventory below. Methods take typed config blobs from the spec;
/// each handler parses what it needs and ignores the rest.
#[async_trait]
pub trait KindHandler: Send + Sync {
    /// Kind tag, matched against `SignalSpec.kind`. Must match the
    /// `Signal::TAG` constant on the corresponding data struct in
    /// `weft_core::signal`.
    fn tag(&self) -> &'static str;

    /// Compute the public routing (URL surface + auth gate config)
    /// for this signal. Called once at register time; the dispatcher
    /// stores the result on the signal row and the public router
    /// dispatches by `surface_kind` + `mount_path`. Returns Err if the
    /// spec's config blob fails to deserialize into the kind's typed
    /// shape; the caller surfaces that as a 400 to whoever submitted
    /// the register.
    fn compute_routing(&self, spec: &SignalSpec) -> Result<SignalRouting>;

    /// True for kinds whose fires can arrive as BROAD account-routed
    /// pushes (matched by connection + topic rather than by an exact
    /// signal token). A RESUME registration of such a kind must carry
    /// at least one predicate: with none, the first push on the
    /// connection's topic would resume the wait, whoever caused it.
    /// Registration enforces this; the default is false (every other
    /// kind's fires address an exact token).
    fn broad_push_routed(&self) -> bool {
        false
    }

    /// Compute the initial opaque state to persist on the signal row
    /// at register time. Default: empty object. Kinds that need to
    /// survive a listener restart return values keyed by their own
    /// schema (Timer: `{"next_fire_at_unix_ms": <abs unix>}` for After,
    /// `{}` for Cron/At since those are wall-clock-absolute).
    ///
    /// **Persistence policy**: what this returns replaces the stored
    /// state, as long as it is not OLDER than what is there. The write
    /// carries a sequence number and the row keeps the higher one
    /// (`signal_insert`), so a registration that started before a live
    /// task's own update cannot land on top of it and undo it. In the
    /// ordinary case (a re-register on reactivate, with no task
    /// running) there is nothing newer and this simply replaces.
    ///
    /// `prior` is the previously-persisted state when the reused entry
    /// token already has a row (None on first registration). Timer
    /// IGNORES it
    /// (reactivate is a fresh schedule by design); a kind whose state
    /// is a feed cursor (poll_endpoint) returns it forward so a
    /// deactivate/activate cycle never re-primes and silently
    /// discards what arrived in between.
    fn compute_initial_state(&self, _spec: &SignalSpec, _prior: Option<&Value>) -> Result<Value> {
        Ok(Value::Object(serde_json::Map::new()))
    }

    /// Spawn any long-running task this kind needs (timer schedule,
    /// SSE subscriber). Returns `Ok(None)` for passive kinds that
    /// wait for an external HTTP fire (Form, live-caller). `Err` on
    /// malformed spec (or, on a FRESH registration, an unserviceable
    /// one) so register surfaces a 400 and activation fails loudly
    /// instead of minting a dead trigger.
    ///
    /// `kind_state` is the opaque blob persisted on the row at
    /// register time (or read back from the row on rehydrate).
    /// Kinds interpret it however they need. Default is empty `{}`.
    async fn spawn_task(
        &self,
        spec: &SignalSpec,
        kind_state: &Value,
        ctx: SpawnCtx,
    ) -> Result<Option<JoinHandle<()>>>;

    /// Decide how a fire's payload routes for an entry-mode signal.
    /// `is_resume` signals never reach this method: top-level
    /// `process` short-circuits to `ProcessTarget::Resume` first.
    fn process_entry(
        &self,
        sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome;

    /// What this signal wakes with when a PERSON wakes it by hand
    /// (`weft wake`), instead of waiting for whatever it waits for.
    ///
    /// `None`, the default, means this kind cannot be woken that way,
    /// and it is the honest answer for almost every kind: a form is
    /// waiting for an answer, a provider subscription for an event, and
    /// there is nothing truthful to invent in their place. A timer is
    /// the exception, because what it is waiting for IS the passage of
    /// time and "now" is a real value for it.
    ///
    /// The payload has to be this kind's own, which is the whole reason
    /// this is a method here rather than a branch in whoever handles the
    /// request: a tier that does not own a kind's wake shape cannot mint
    /// one without owning it by accident.
    fn wake_by_hand(&self) -> Option<Value> {
        None
    }

    /// Does this verified provider push address this signal, and what
    /// payload does it wake with?
    ///
    /// Only for kinds fed by pushes the provider aims at an ACCOUNT
    /// rather than at one subscription: the push names a connection and
    /// a topic, and which of that connection's signals it feeds is the
    /// kind's own question (its topic name, its subscription scope). A
    /// kind whose fires address an exact token never answers here, which
    /// is why the default is `None`.
    ///
    /// The signal's own filter is NOT this method's business: the
    /// listener applies the one shared predicate gate to whatever comes
    /// back, the same gate every other fire passes.
    fn match_push(&self, _sig: &RegisteredSignal, _push: &PushEvent) -> Option<Value> {
        None
    }

    /// Render the consumer-facing payload for this signal. Returns
    /// `Ok(None)` for kinds with no consumer surface (Timer,
    /// SseSubscribe) and `Err` for malformed specs (so the caller
    /// surfaces a 400 instead of silently rendering empty).
    fn render(&self, token: &str, sig: &RegisteredSignal) -> Result<Option<Value>>;

    /// Tear down anything the signal holds OUTSIDE this process (a
    /// provider-side subscription). Called after the registry entry
    /// was removed, detached from the unregister answer; a failure
    /// must log loudly, never propagate. Default: nothing held.
    async fn on_unregister(
        &self,
        _token: &str,
        _sig: &RegisteredSignal,
        _events_broker: &Arc<weft_broker_client::BrokerEventsClient>,
    ) {
    }

}

inventory::collect!(&'static dyn KindHandler);

/// Look up a registered handler by tag. Iterates the inventory once;
/// callers should not hold the result across kind additions (none in
/// production today; future hot-reload would require a different
/// design anyway).
pub fn lookup(tag: &str) -> Option<&'static dyn KindHandler> {
    inventory::iter::<&'static dyn KindHandler>
        .into_iter()
        .find(|h| h.tag() == tag)
        .copied()
}

fn handler_or_err(tag: &str) -> Result<&'static dyn KindHandler> {
    lookup(tag).ok_or_else(|| anyhow::anyhow!("unknown signal kind: '{tag}'"))
}

// ----- Public listener entrypoints (HTTP-driven) ---------------------

/// What the routing and kind_state come from. `Fresh` is the register
/// path: compute the routing and the initial kind_state from the spec,
/// handing the kind the token's previously-persisted state (entry
/// tokens are reused across reactivates) so cursor-bearing kinds carry
/// it forward. `Restore` is the rehydrate / pod-move path: both values
/// came back from the durable row, never recompute (the row is what
/// the dispatcher routes by, and a fresh `compute_initial_state` would
/// reset a Timer's clock).
pub enum RoutingSource {
    Fresh {
        prior_kind_state: Option<Value>,
        /// The write-fence version the prior state was read at.
        prior_seq: i64,
    },
    Restore {
        routing: SignalRouting,
        kind_state: Value,
        /// The row's `kind_state_seq` at restore time.
        seq: i64,
    },
}

/// The identity portion of one registered signal: everything the
/// registry stores about WHO the signal is, as one named-field bundle
/// so the two build sites (register, rehydrate) cannot transpose a
/// positional argument.
pub struct SignalIdentity {
    pub token: String,
    pub tenant_id: String,
    pub node_id: String,
    /// True iff this is a mid-execution resume (HumanQuery, etc).
    pub is_resume: bool,
    /// Color of the suspended execution to resume. Set iff `is_resume`.
    pub color: Option<String>,
    /// The placement generation under which this pod holds the signal.
    pub placement_generation: i64,
    pub spec: SignalSpec,
}

/// Register a signal in the in-RAM registry. Single path for both
/// register (fresh registration from the worker) and rehydrate (boot
/// or post-deactivate reconciliation). Returns the routing and
/// kind_state for the dispatcher to persist on the signal row; on
/// the Restore path the returned values are the same ones that came
/// in.
pub async fn register_in_registry(
    identity: SignalIdentity,
    source: RoutingSource,
    registry: Arc<Registry>,
    sink: FireSignalSink,
    config: Arc<ListenerConfig>,
    events_broker: Arc<weft_broker_client::BrokerEventsClient>,
) -> Result<(SignalRouting, Value)> {
    let SignalIdentity {
        token,
        tenant_id,
        node_id,
        is_resume,
        color,
        placement_generation,
        spec,
    } = identity;
    let handler = handler_or_err(&spec.kind)?;
    // A resume wait fed by broad account-routed pushes must pin
    // itself with a predicate (its minted correlation id): with none,
    // ANY push on the connection's topic would resume it.
    if is_resume && handler.broad_push_routed() && spec.match_predicates.is_empty() {
        anyhow::bail!(
            "a resume '{}' signal needs at least one predicate pinning it to its own \
             correlation id; without one, any event on the connection's topic would \
             resume this wait",
            spec.kind,
        );
    }
    let fresh = matches!(source, RoutingSource::Fresh { .. });
    let (routing, kind_state_owned, state_seq) = match source {
        RoutingSource::Fresh { prior_kind_state, prior_seq } => {
            let r = handler.compute_routing(&spec)?;
            let s = handler.compute_initial_state(&spec, prior_kind_state.as_ref())?;
            (r, s, prior_seq)
        }
        RoutingSource::Restore { routing, kind_state, seq } => (routing, kind_state, seq),
    };
    // The held-event loops (Timer, SSE, poll, socket) capture the
    // signal's tenant AND its placement generation so the fire they
    // enqueue is stamped with both. The pod has no single tenant, so
    // tenant travels with the signal; the generation lets the broker
    // fence a stale old-pod fire during a scale-down move overlap.
    // The FireContext also carries the spec-level pre-fire filter, so
    // every kind's events pass the same gate.
    let serving = Arc::new(Mutex::new(ServingState::default()));
    let ctx = SpawnCtx {
        fire: FireContext::new(
            sink.clone(),
            token.clone(),
            tenant_id.clone(),
            placement_generation,
            spec.match_predicates.clone(),
        ),
        config: config.clone(),
        events_broker,
        fresh,
        state_seq,
        serving: serving.clone(),
    };
    let task = handler
        .spawn_task(&spec, &kind_state_owned, ctx)
        .await?
        .map(|h| Arc::new(TaskGuard::new(h)));
    registry.insert(
        token,
        RegisteredSignal {
            spec,
            node_id,
            tenant_id,
            is_resume,
            color,
            placement_generation,
            task,
            routing: routing.clone(),
            serving,
        },
    );
    Ok((routing, kind_state_owned))
}

/// Process one stateless fire. Resume signals route to the
/// suspended color regardless of kind; entry signals delegate to
/// the kind's `process_entry`. Unknown tokens return Drop.
pub async fn process(
    token: &str,
    payload: Value,
    registry: Arc<Registry>,
) -> Result<ProcessOutcome> {
    let Some(signal) = registry.get(token) else {
        // This pod does not hold the signal. Almost always means the
        // dispatcher routed here during a scale-down move (the signal was
        // re-placed onto another pod). Return NotHeld so the dispatcher
        // re-resolves the holder and retries, instead of silently
        // dropping the fire.
        return Ok(ProcessOutcome {
            value: payload,
            target: ProcessTarget::NotHeld,
        });
    };

    if signal.is_resume {
        let Some(color) = signal.color.clone() else {
            tracing::warn!(
                target: "weft_listener::kinds",
                %token,
                "is_resume signal has no color; dropping"
            );
            return Ok(ProcessOutcome {
                value: payload,
                target: ProcessTarget::Drop {
                    reason: Some("is_resume signal missing color".into()),
                },
            });
        };
        return Ok(ProcessOutcome {
            value: payload,
            target: ProcessTarget::Resume { color },
        });
    }

    let handler = handler_or_err(&signal.spec.kind)?;
    Ok(handler.process_entry(&signal, payload))
}

/// What a signal held here wakes with when a person wakes it by hand.
///
/// `Ok(None)` is a real answer: the kind cannot be woken that way, and
/// whoever asked should say so rather than invent a payload. An unknown
/// token is an error, because the caller resolved this pod as the holder
/// and a token that is not here means the two disagree.
pub fn wake_by_hand(token: &str, registry: Arc<Registry>) -> Result<Option<Value>> {
    let signal = registry
        .get(token)
        .ok_or_else(|| anyhow::anyhow!("unknown token: {token}"))?;
    Ok(handler_or_err(&signal.spec.kind)?.wake_by_hand())
}

/// Which of `tokens` this verified provider push feeds, and with what.
///
/// Two gates, in this order. The KIND says whether the push addresses
/// the signal at all and what payload it becomes, because that reads the
/// kind's own settings. Then the signal's own filter runs, through the
/// one shared gate every fire passes, so a filter means the same thing
/// however the event reached us.
///
/// A token this pod no longer holds is skipped rather than reported: the
/// signal moved during a scale-down and its new holder is asked in the
/// same round, so answering "not held" here would only duplicate work
/// the dispatcher already did when it resolved the holders.
pub fn match_push(push: &PushEvent, tokens: &[String], registry: Arc<Registry>) -> Vec<MatchedPush> {
    let mut matched = Vec::new();
    for token in tokens {
        let Some(signal) = registry.get(token) else { continue };
        let Some(handler) = lookup(&signal.spec.kind) else {
            tracing::warn!(
                target: "weft_listener::kinds",
                %token, kind = %signal.spec.kind,
                "a registered signal has no handler; it cannot be fed by a push"
            );
            continue;
        };
        let Some(payload) = handler.match_push(&signal, push) else { continue };
        if !weft_core::signal::predicate::matches(&signal.spec.match_predicates, &payload) {
            tracing::debug!(
                target: "weft_listener::kinds",
                %token, kind = %signal.spec.kind,
                "push did not match the signal's filter; not firing"
            );
            continue;
        }
        matched.push(MatchedPush { token: token.clone(), payload });
    }
    matched
}

/// Render the consumer-facing payload for a registered signal.
pub fn render(token: &str, registry: Arc<Registry>) -> Result<Option<Value>> {
    let signal = registry
        .get(token)
        .ok_or_else(|| anyhow::anyhow!("unknown token: {token}"))?;
    let handler = handler_or_err(&signal.spec.kind)?;
    handler.render(token, &signal)
}

/// Display payload returned to the inspector: the routing (surface +
/// auth), the kind and its config, and the LIVE serving state for the
/// kinds whose task reports one (which transport serves the signal,
/// what it is doing right now).
pub fn compute_display(sig: &RegisteredSignal) -> Value {
    let serving = {
        let s = sig.serving.lock();
        if s.status.is_empty() && s.transport.is_none() {
            Value::Null
        } else {
            serde_json::json!({
                "state": s.status,
                "transport": s.transport.as_ref().map(|t| match t {
                    Transport::Socket => "socket",
                    Transport::Webhook => "webhook",
                    Transport::Unservable(_) => "unservable",
                }),
            })
        }
    };
    serde_json::json!({
        "surface": sig.routing.surface,
        "auth": sig.routing.auth,
        "kind": sig.spec.kind,
        "config": sig.spec.config,
        "serving": serving,
    })
}

/// Run a removed signal's kind-specific EXTERNAL teardown (anything
/// held outside this process). Called by the unregister route after
/// the registry entry is gone; detached from the answer, loud in
/// logs on failure, kind-agnostic here (the kind decides what, if
/// anything, to tear down).
pub async fn on_unregister(
    token: &str,
    sig: &RegisteredSignal,
    events_broker: &Arc<weft_broker_client::BrokerEventsClient>,
) {
    match lookup(&sig.spec.kind) {
        Some(handler) => handler.on_unregister(token, sig, events_broker).await,
        None => tracing::warn!(
            target: "weft_listener::kinds",
            %token, kind = %sig.spec.kind,
            "unknown signal kind at unregister; skipping external teardown"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kind shipped in weft-core must have a matching listener
    /// handler, or activating a program that uses it fails at runtime
    /// with "unknown signal kind".
    ///
    /// Read off weft-core's OWN inventory rather than a list written
    /// here. A hand-kept list is a list of what somebody remembered:
    /// this test used to hold one, so shipping a kind in core with no
    /// handler here passed green and broke at activation, which is the
    /// exact thing it exists to stop.
    #[test]
    fn every_core_kind_has_a_handler() {
        let mut handled: Vec<&'static str> = inventory::iter::<&'static dyn KindHandler>
            .into_iter()
            .map(|h| h.tag())
            .collect();
        handled.sort_unstable();
        let mut shipped: Vec<&'static str> =
            inventory::iter::<weft_core::signal::SignalKindEntry>
                .into_iter()
                .map(|entry| entry.tag)
                .collect();
        shipped.sort_unstable();
        assert_eq!(
            handled, shipped,
            "every signal kind weft-core ships needs a handler here, and a handler here \
             needs a kind in core; whichever side is longer is the side that moved"
        );
    }

    // ----- Matching a provider push against what this pod holds ------
    //
    // These pin the tier boundary. The dispatcher hands over a verified
    // push and a list of candidate tokens; every decision about WHICH of
    // them it feeds is made here. If someone moves one of these
    // decisions back into the dispatcher, the matching answer stops
    // coming from `match_push` and these tests stop passing.

    fn registered(kind: &str, config: Value, predicates: Vec<weft_core::signal::Predicate>) -> RegisteredSignal {
        RegisteredSignal {
            spec: SignalSpec {
                kind: kind.to_string(),
                config,
                consumer_kind: None,
                access: None,
                match_predicates: predicates,
            },
            node_id: "n1".into(),
            tenant_id: "t1".into(),
            is_resume: false,
            color: None,
            placement_generation: 1,
            task: None,
            routing: SignalRouting {
                surface: weft_core::primitive::SignalSurface::Internal,
                auth: weft_core::primitive::SignalAuth::None,
                auth_config: Value::Null,
            },
            serving: Arc::new(Mutex::new(ServingState::default())),
        }
    }

    fn subscription(topic: &str, scope: &str) -> Value {
        serde_json::json!({ "topic": topic, "scope": scope })
    }

    fn push(topic: &str, event: Value) -> PushEvent {
        PushEvent { service: "slack".into(), topic: topic.into(), event }
    }

    fn holding(entries: &[(&str, RegisteredSignal)]) -> Arc<Registry> {
        let registry = Arc::new(Registry::default());
        for (token, sig) in entries {
            registry.insert((*token).to_string(), sig.clone());
        }
        registry
    }

    #[test]
    fn a_push_feeds_the_subscription_on_its_own_topic() {
        let event = serde_json::json!({ "channel": "C1", "text": "hi" });
        let registry = holding(&[(
            "tok",
            registered("provider_events", subscription("messages", "account"), vec![]),
        )]);
        let got = match_push(&push("messages", event.clone()), &["tok".into()], registry);
        assert_eq!(got.len(), 1, "the topics agree, so it fires");
        assert_eq!(got[0].token, "tok");
        assert_eq!(got[0].payload, event, "and it wakes with the event as it arrived");
    }

    /// One connection can hold several topics whose field names
    /// overlap, so a mailbox push must not wake a file watch.
    #[test]
    fn a_push_on_another_topic_feeds_nothing() {
        let registry = holding(&[(
            "tok",
            registered("provider_events", subscription("files", "account"), vec![]),
        )]);
        let got = match_push(&push("messages", serde_json::json!({})), &["tok".into()], registry);
        assert!(got.is_empty());
    }

    /// An app-wide subscription means every install of the app, which a
    /// push aimed at ONE account can never be. Serving it here would
    /// half-serve it, so it is left to the dial-out socket.
    #[test]
    fn an_app_wide_subscription_is_not_served_by_an_account_push() {
        let registry = holding(&[(
            "tok",
            registered("provider_events", subscription("messages", "app"), vec![]),
        )]);
        let got = match_push(&push("messages", serde_json::json!({})), &["tok".into()], registry);
        assert!(got.is_empty());
    }

    /// The signal's own filter runs on a pushed payload exactly as it
    /// runs on one that arrived down a socket: a filter means the same
    /// thing however the event reached us.
    #[test]
    fn the_signals_own_filter_still_decides() {
        let only_c1 = vec![weft_core::signal::Predicate::eq("channel", "C1")];
        let registry = holding(&[(
            "tok",
            registered("provider_events", subscription("messages", "account"), only_c1),
        )]);
        let wrong = serde_json::json!({ "channel": "C2" });
        assert!(match_push(&push("messages", wrong), &["tok".into()], registry.clone()).is_empty());
        let right = serde_json::json!({ "channel": "C1" });
        assert_eq!(match_push(&push("messages", right), &["tok".into()], registry).len(), 1);
    }

    /// Every other kind answers "not mine" by default, so a push can
    /// never wake a timer or a form that happens to hang off the same
    /// connection.
    #[test]
    fn a_kind_that_is_not_fed_by_pushes_is_never_matched() {
        let registry = holding(&[(
            "tok",
            registered("timer", serde_json::json!({ "topic": "messages" }), vec![]),
        )]);
        let got = match_push(&push("messages", serde_json::json!({})), &["tok".into()], registry);
        assert!(got.is_empty(), "a timer is not fed by a provider push, whatever its config says");
    }

    /// A timer is the one kind a person can wake by hand, because what
    /// it waits for is the clock and "now" is true for it. The payload
    /// is the kind's own, and it is the same two fields a real tick
    /// carries, so the node cannot tell the two apart and does not have
    /// to.
    #[test]
    fn a_timer_is_the_kind_that_can_be_woken_by_hand() {
        let registry = holding(&[("tok", registered("timer", serde_json::json!({}), vec![]))]);
        let payload = wake_by_hand("tok", registry)
            .expect("the token is held here")
            .expect("a timer can be woken");
        assert!(payload["scheduledTime"].is_string(), "{payload}");
        assert!(payload["actualTime"].is_string(), "{payload}");
        assert_eq!(
            payload["scheduledTime"], payload["actualTime"],
            "a hand wake was aimed at this moment, so there is no gap to report"
        );
    }

    /// Every other kind answers with nothing, and whoever asked refuses
    /// rather than inventing what a form was waiting to hear.
    #[test]
    fn a_kind_with_no_truthful_stand_in_cannot_be_woken() {
        for kind in ["form", "provider_events", "poll_endpoint", "route", "socket"] {
            let registry = holding(&[("tok", registered(kind, serde_json::json!({}), vec![]))]);
            let answer = wake_by_hand("tok", registry).expect("the token is held here");
            assert!(answer.is_none(), "{kind} has nothing to wake with");
        }
    }

    /// A token this pod does not hold is an error, not a quiet nothing:
    /// whoever asked resolved this pod as the holder, so the two
    /// disagreeing is worth saying out loud.
    #[test]
    fn waking_a_token_this_pod_does_not_hold_is_an_error() {
        let why = wake_by_hand("gone", holding(&[])).expect_err("not held here");
        assert!(format!("{why:#}").contains("gone"), "{why:#}");
    }

    /// A token that moved to another pod mid-push is simply not this
    /// pod's answer to give; the dispatcher asked its new holder too.
    #[test]
    fn a_token_this_pod_no_longer_holds_is_skipped() {
        let registry = holding(&[]);
        let got = match_push(&push("messages", serde_json::json!({})), &["gone".into()], registry);
        assert!(got.is_empty());
    }
}
