//! Per-kind logic. One module per `Signal` impl in
//! `weft_core::signal`. Each module:
//!
//!   - declares a unit struct (`TimerHandler`, `LiveCallerHandler`, ...)
//!     implementing `KindHandler`.
//!   - parses the spec's opaque `config` blob into the kind's typed
//!     struct from `weft_core::signal`.
//!   - says what a signal of it needs between fires (`between_fires`),
//!     and owns what that takes: its wakes (a timer, a poll), or the
//!     connection it holds (an SSE feed, a socket); plus `process`,
//!     `render` and `compute_routing`.
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
//! Top-level helpers in this file (`prepare_signal`, `bring_up`,
//! `process`, `wake`, etc.) look up the kind by tag and
//! delegate. They never know about specific kinds.
//!
//! Conventions enforced by the dispatch helpers below:
//!   - Kinds that raise their own fires (a timer's tick, an SSE event)
//!     enqueue a `FireSignal` task via the broker; the dispatcher picker
//!     runs it back through `/process`, where the kind's
//!     `process_entry` routes it to `ProcessTarget::Entry`.
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
use weft_core::live::{LiveFeed, LiveItem};
use weft_core::primitive::{SignalRouting, SignalSpec};

use parking_lot::Mutex;

use crate::config::ListenerConfig;
use crate::event_context::FireContext;
use weft_core::signal::listener_protocol::{MatchedPush, ProcessOutcome, ProcessTarget, PushEvent, StartMode};
use crate::registry::{RegisteredSignal, ServingState, TaskGuard, Transport};

/// Everything a kind's work on one signal runs with, bundled: the
/// per-signal identity + fire plumbing (as a ready [`FireContext`],
/// which carries the spec-level pre-fire filter), the listener config,
/// the broker's event-serving client, and whether this is a FRESH
/// registration (a fresh one may make broker calls and refuse
/// activation loudly; a restore must come up and let its task retry, or
/// one unreachable broker at boot would fail the whole rebuild). Handed
/// to a held connection's task at spawn and to a wake.
#[derive(Clone)]
pub struct SpawnCtx {
    pub fire: FireContext,
    pub config: Arc<ListenerConfig>,
    pub events_broker: Arc<weft_broker_client::BrokerEventsClient>,
    pub fresh: bool,
    /// The signal's live serving state, shared with the registry
    /// entry so status a serving task writes lands where the node's
    /// display reads it.
    pub serving: Arc<Mutex<ServingState>>,
}

/// Everything a kind needs to render what it is showing. Bundled
/// rather than passed as two arguments, because one of them (the
/// address) comes from the dispatcher rather than from the listener's
/// own state, and a named field says so where a positional argument
/// would not.
pub struct LiveCtx<'a> {
    pub sig: &'a RegisteredSignal,
    /// The address an outside caller reaches this signal at, as the
    /// dispatcher computed it (host, tenant segment and `/connect/`
    /// prefix included). `None` for a signal nothing calls in to, and
    /// for a caller that did not supply one.
    pub address: Option<&'a str>,
}

/// What a signal needs between two fires. It decides where the signal
/// runs: the listener keeps nothing in memory between calls, so it serves
/// `Called` and `Wakes` signals itself, and a `Holds` signal is held by a
/// holder (`crate::hold`), the listener's code where something stays up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BetweenFires {
    /// Nothing runs between fires; the outside calls in (a route, a
    /// form).
    Called,
    /// Wants waking at times ([`KindHandler::next_wake`]); each wake is
    /// handed to the platform's `Alarm`, which calls the listener back
    /// ([`KindHandler::on_wake`]).
    Wakes,
    /// Keeps a connection to the outside open
    /// ([`KindHandler::spawn_task`]), in a holder.
    Holds,
}

/// Why a `Wakes` kind is asked for its next wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeFrom {
    /// A fresh registration or a restore: arming the first wake.
    Armed,
    /// A wake aimed at `aimed_at_ms` was just handled.
    Woken { aimed_at_ms: i64 },
}

/// One wake as a kind handles it.
#[derive(Debug, Clone)]
pub struct Woken {
    /// The moment this wake was set for.
    pub aimed_at_ms: i64,
    /// When it actually arrived.
    pub now_ms: i64,
    /// The signal's durable state as read just now, and the write-fence
    /// version it is at.
    pub state: Value,
    pub seq: i64,
    /// True when the signal is a parked run's wait (`await_signal`)
    /// rather than an entry trigger, read off the row like the state.
    pub is_resume: bool,
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

    /// What one signal of this kind needs between fires, from its spec and
    /// the state its registration settled ([`Self::settle`]): most kinds
    /// answer the same for every signal, one whose transport depends on
    /// the connection (a provider subscription pushed to the install, or
    /// a socket it dials) answers per signal. Required, so a new kind does
    /// not compile until its author has answered. A state the kind cannot
    /// read is an error, never a guess.
    fn between_fires(&self, spec: &SignalSpec, kind_state: &Value) -> Result<BetweenFires>;

    /// Settle, as a signal is registered and before its row is written,
    /// what its state records about how it will be served, looking outside
    /// when the answer depends on it (which transport a provider
    /// subscription rides). Nothing starts and nothing is held; a signal
    /// that cannot be served is refused here, loudly. Default: the state
    /// as computed.
    async fn settle(&self, _spec: &SignalSpec, kind_state: Value, _ctx: &SpawnCtx) -> Result<Value> {
        Ok(kind_state)
    }

    /// For a `Wakes` kind: whether a fresh registration handles its first
    /// wake on the spot, so what that wake does (a provider subscription's
    /// first subscribe) fails the registration loudly instead of failing
    /// later, in the background.
    fn wakes_at_once_when_fresh(&self) -> bool {
        false
    }

    /// For a `Wakes` kind: the next moment it wants waking (unix ms), or
    /// `None` when it has nothing left to wait for (a one-shot that
    /// fired). Computed from the spec and the durable state alone, and
    /// the SAME moment for every caller asking within one slot, so
    /// arming a signal twice (a registration and a rehydrate, two copies
    /// of the listener) sets one wake, never two.
    fn next_wake(&self, _spec: &SignalSpec, _state: &Value, _from: WakeFrom, _now_ms: i64) -> Result<Option<i64>> {
        Ok(None)
    }

    /// For a `Wakes` kind: handle one wake. A wake may arrive late,
    /// twice, or after the thing it was set for changed, so the kind
    /// recomputes from `woken.state` whether anything is due, CLAIMS the
    /// moment through [`FireContext::claim_kind_state`] before acting (so
    /// of two copies woken for it, one acts), fires through `ctx.fire`,
    /// and answers the state it now stands at (for its next wake), or
    /// `None` when another copy claimed this moment first: that copy set
    /// the next wake from the state it wrote, so this one sets none.
    async fn on_wake(&self, _spec: &SignalSpec, _woken: Woken, _ctx: SpawnCtx) -> Result<Option<Value>> {
        anyhow::bail!("the '{}' kind does not wake", self.tag())
    }

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
    /// state, and moves the row to a new version, only while the row is
    /// still at the version `prior` was read at (`signal_insert` is a
    /// compare-and-set). A wake that claimed a moment in between moved
    /// the version, so the dispatcher reads the row again and asks
    /// again; a wake claiming after the registration finds the version
    /// moved and loses. Either way the registration's state is computed
    /// from the state it replaces.
    ///
    /// `prior` is the previously-persisted state when the reused entry
    /// token already has a row (None on first registration). Timer
    /// IGNORES it
    /// (reactivate is a fresh schedule by design); a kind whose state
    /// is a feed cursor (poll_endpoint) returns it forward so a
    /// deactivate/activate cycle never re-primes and silently
    /// discards what arrived in between.
    ///
    /// `asked_at_unix_ms` is when the registration was asked for (the
    /// node's `await_signal`, or the activation), for a kind whose state
    /// counts from that moment. `is_resume` says which of the two it is,
    /// for a kind that behaves differently while a run waits on it.
    fn compute_initial_state(
        &self,
        _spec: &SignalSpec,
        _prior: Option<&Value>,
        _asked_at_unix_ms: i64,
        _is_resume: bool,
    ) -> Result<Value> {
        Ok(Value::Object(serde_json::Map::new()))
    }

    /// Refuse a spec this kind cannot serve as a parked run's wait
    /// (`await_signal`), naming why. Called at registration, only for a
    /// resume. Default: every spec the kind validates can be awaited.
    fn check_resume(&self, _spec: &SignalSpec) -> Result<()> {
        Ok(())
    }

    /// For a `Holds` kind: spawn the task that holds its connection
    /// (an SSE subscriber, a socket). Every other kind keeps the default
    /// `Ok(None)`. `Err` on malformed spec (or, on a FRESH registration,
    /// an unserviceable one) so the start surfaces a 400 and activation
    /// fails loudly instead of minting a dead trigger.
    ///
    /// `kind_state` is the opaque blob persisted on the row at
    /// register time (or read back from the row on restore).
    async fn spawn_task(
        &self,
        _spec: &SignalSpec,
        _kind_state: &Value,
        _ctx: SpawnCtx,
    ) -> Result<Option<JoinHandle<()>>> {
        Ok(None)
    }

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

    /// Render what a consumer needs to ANSWER this signal: a form's
    /// fields and their prefills, for the listing at
    /// `GET /signal-token/signals`. Not to be confused with `live`
    /// below, which is what the node SHOWS: a form is answered through
    /// this one, and what it shows through that one is what it is
    /// asking.
    ///
    /// Returns `Ok(None)` for kinds nobody answers by hand (Timer,
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

    /// What this kind SHOWS on the node's body, and to any client that
    /// reads the node's display: what the listener answers on its
    /// `POST /live`, in the same shape an infra node's container serves
    /// on its `GET /live`.
    ///
    /// The kind owns this, not the node that declared the trigger. The
    /// surface and the auth are computed here at register, so this is
    /// the only place that knows them.
    ///
    /// The default is the address: where a caller sends a request and
    /// how they get past the door. Every kind with a public entry gets
    /// that for free and adds its own lines by calling `address_items`
    /// and extending. A kind that fires from inside the runtime gets
    /// nothing from the default and says what it is doing instead.
    ///
    /// Read-only, unlike an infra node's display: a trigger's panel has
    /// no buttons, because nothing about a registration is the reader's
    /// to change from here.
    fn live(&self, ctx: &LiveCtx<'_>) -> LiveFeed {
        LiveFeed::new(address_items(ctx))
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

/// Who a new signal is, as the dispatcher names it: everything its row
/// will say about it besides what the kind computes.
pub struct SignalIdentity {
    pub token: String,
    pub tenant_id: String,
    pub node_id: String,
    /// True iff this is a mid-execution resume (HumanQuery, etc).
    pub is_resume: bool,
    /// Execution of the suspended execution to resume. Set iff `is_resume`.
    pub execution_id: Option<String>,
    pub spec: SignalSpec,
}

/// What a kind computes for a new signal's row.
#[derive(Debug)]
pub struct Prepared {
    pub routing: SignalRouting,
    pub kind_state: Value,
    /// What a consumer needs to answer it (see [`KindHandler::render`]),
    /// `None` for a kind nobody answers by hand.
    pub rendered: Option<Value>,
    /// Whether the signal keeps a connection open between fires, so a
    /// holder holds it.
    pub holds: bool,
}

/// Compute a new signal's row: its routing, its starting kind state, its
/// rendered payload and whether a holder holds it. Nothing starts and
/// nothing is held. The signal comes up with [`bring_up`] once the
/// dispatcher has committed the row, so whatever it starts (a wake, a
/// held connection) always finds its row.
///
/// `prior_kind_state` is the state the row already holds when the token
/// is reused (an entry across reactivates), so a kind whose state is a
/// feed cursor carries it forward instead of re-priming.
/// `asked_at_unix_ms` is when the registration was asked for.
pub async fn prepare_signal(
    state: &crate::ListenerState,
    identity: SignalIdentity,
    for_instance: Option<weft_core::instance::InstanceScope>,
    prior_kind_state: Option<&Value>,
    asked_at_unix_ms: i64,
) -> Result<Prepared> {
    let SignalIdentity { token, tenant_id, node_id, is_resume, execution_id, spec } = identity;
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
    if is_resume {
        handler.check_resume(&spec)?;
    }
    let routing = handler.compute_routing(&spec)?;
    let computed = handler.compute_initial_state(&spec, prior_kind_state, asked_at_unix_ms, is_resume)?;
    // Settling may look at the signal's connection, through the broker,
    // as the signal itself would once up; its slot shows nothing.
    let ctx = spawn_ctx(state, &token, &tenant_id, for_instance, &spec, true, Arc::default());
    let kind_state = handler.settle(&spec, computed, &ctx).await?;
    let holds = handler.between_fires(&spec, &kind_state)? == BetweenFires::Holds;
    let signal = RegisteredSignal {
        spec,
        node_id,
        tenant_id,
        is_resume,
        execution_id,
        task: None,
        kind_state: Some(kind_state.clone()),
        routing: routing.clone(),
        serving: Arc::default(),
    };
    let rendered = handler.render(&token, &signal)?;
    Ok(Prepared { routing, kind_state, rendered, holds })
}

/// Bring a row up: a signal that holds a connection gets its task started
/// and its registry entry, after one already running under the token is
/// stopped ([`stop_held`]); a signal that wakes gets its next wake set
/// (setting it twice is one wake, see [`KindHandler::next_wake`]), or, on
/// a fresh registration of a kind that asks for it, is woken on the spot;
/// a signal the outside calls in to needs nothing.
///
/// Only a held connection is kept in the in-RAM registry: its task lives
/// in this process, which claims it (`crate::hold`), so a rewrite of its
/// row reaches it through the claim it loses. A `Called` or `Wakes` signal
/// is never cached: any of several serverless copies may answer for it,
/// and `/unregister` reaches one of them, so a cached copy elsewhere would
/// keep routing a signal that is gone. Those are read from their held row
/// on every call ([`crate::registry::held`]).
///
/// `StartMode::New` is a signal the dispatcher just registered: the kind
/// may then make broker calls and refuse loudly (see [`SpawnCtx::fresh`]).
/// A restore (boot, rehydrate, a holder taking it) or a put-back (a row
/// the dispatcher put back) comes up and lets its task retry.
///
/// Every caller goes through [`crate::registry::hold`], which makes the
/// bring-up single-flight per token and lets go of a row deleted while it
/// came up.
pub async fn bring_up(state: &crate::ListenerState, row: weft_broker_client::protocol::SignalRowWire, mode: StartMode) -> Result<()> {
    let spec: SignalSpec = serde_json::from_str(&row.spec_json)
        .map_err(|e| anyhow::anyhow!("malformed spec_json for signal {}: {e}", row.token))?;
    let handler = handler_or_err(&spec.kind)?;
    let fresh = mode.fresh();
    match handler.between_fires(&spec, &row.kind_state)? {
        BetweenFires::Called => Ok(()),
        BetweenFires::Wakes if fresh && handler.wakes_at_once_when_fresh() => {
            wake(state, WakeBody { token: row.token.clone(), due_at_ms: now_unix_ms() }).await
        }
        BetweenFires::Wakes => arm_next_wake(state, handler, &row.token, &spec, &row.kind_state, WakeFrom::Armed).await,
        BetweenFires::Holds => {
            let routing = row.to_routing().map_err(|e| anyhow::anyhow!("to_routing for signal {}: {e}", row.token))?;
            // What already runs under the token stops, teardown included,
            // BEFORE the new task starts: an outside teardown is keyed by
            // the token (a provider subscription is dropped by it), so run
            // after the new task subscribed it would drop the new one.
            stop_held(state, &row.token).await;
            let serving = Arc::new(Mutex::new(ServingState::default()));
            let mut ctx = spawn_ctx(state, &row.token, &row.tenant_id, row.for_instance.clone(), &spec, fresh, serving.clone());
            // Its fires go out under this holder's claim, so a copy that
            // lost the row (to another holder, or to a registration that no
            // longer holds) delivers nothing from then on.
            ctx.fire = ctx.fire.held_by(state.config.replica.clone());
            let Some(task) = handler.spawn_task(&spec, &row.kind_state, ctx).await? else {
                anyhow::bail!(
                    "the '{}' kind holds a connection but started no task; its `between_fires` \
                     and its `spawn_task` disagree",
                    spec.kind,
                );
            };
            state.registry.insert(
                row.token,
                RegisteredSignal {
                    spec,
                    node_id: row.node_id,
                    tenant_id: row.tenant_id,
                    is_resume: row.is_resume,
                    execution_id: row.execution_id,
                    task: Some(Arc::new(TaskGuard::new(task))),
                    kind_state: None,
                    routing,
                    serving,
                },
            );
            Ok(())
        }
    }
}

/// A signal's row was removed: stop it, and tear down what it arranged
/// outside this process. A connection this process holds stops through
/// [`forget`]; any other signal tears down from the removed row the
/// request carries (a subscription it renews on its wakes), loud in logs
/// on failure, and detached unless the token is `reused` right after. A
/// connection a holder elsewhere holds stops there, when its claim goes
/// with the row.
///
/// A row this listener cannot tear down is an error, which says whether
/// trying again can help ([`UnregisterError`]).
pub async fn unregister(
    state: &crate::ListenerState,
    req: weft_core::signal::listener_protocol::UnregisterRequest,
) -> std::result::Result<(), UnregisterError> {
    if state.registry.get(&req.token).is_some() || state.registry.down_reason(&req.token).is_some() {
        let teardown = forget(state, &req.token);
        if req.reused {
            teardown
                .await
                .map_err(|e| UnregisterError::TeardownFailed(anyhow::anyhow!("the teardown of signal {} ended abnormally: {e}", req.token)))?;
        }
        return Ok(());
    }
    let handler = lookup(&req.spec.kind).ok_or_else(|| UnregisterError::UnknownKind(req.spec.kind.clone()))?;
    let holds = handler.between_fires(&req.spec, &req.kind_state).map_err(|e| {
        UnregisterError::Unreadable(e.context(format!("signal {}'s state cannot be read, so what it arranged outside is not torn down", req.token)))
    })? == BetweenFires::Holds;
    if holds {
        return Ok(());
    }
    let sig = RegisteredSignal {
        spec: req.spec,
        node_id: String::new(),
        tenant_id: req.tenant_id,
        is_resume: false,
        execution_id: None,
        task: None,
        kind_state: Some(req.kind_state),
        routing: SignalRouting {
            surface: weft_core::primitive::SignalSurface::Internal,
            auth: weft_core::primitive::SignalAuth::None,
            auth_config: Value::Null,
        },
        serving: Arc::default(),
    };
    let broker = state.events_broker.clone();
    let token = req.token.clone();
    let teardown = tokio::spawn(async move { handler.on_unregister(&req.token, &sig, &broker).await });
    if req.reused {
        teardown
            .await
            .map_err(|e| UnregisterError::TeardownFailed(anyhow::anyhow!("the teardown of signal {token} ended abnormally: {e}")))?;
    }
    Ok(())
}

/// Why [`unregister`] could not tear a signal down.
#[derive(Debug)]
pub enum UnregisterError {
    /// Its state cannot be read by its kind: no listener ever will, so
    /// trying again cannot help.
    Unreadable(anyhow::Error),
    /// Its kind is not one this listener knows (a listener older than the
    /// program that registered it): a newer one does.
    UnknownKind(String),
    /// The teardown itself ended abnormally.
    TeardownFailed(anyhow::Error),
}

impl std::fmt::Display for UnregisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnregisterError::Unreadable(e) | UnregisterError::TeardownFailed(e) => write!(f, "{e:#}"),
            UnregisterError::UnknownKind(kind) => write!(f, "the signal is of the kind '{kind}', which this listener does not know"),
        }
    }
}

/// Stop holding `token`: drop its registry entry (which aborts its task)
/// and have its kind tear down what it holds outside this process (a
/// provider-side subscription), detached and loud in logs on failure. A
/// wake still set for it finds no row and does nothing. Answers the
/// teardown's task, for a caller that reuses the token right after.
///
/// The teardown runs under the token's bring-up guard (the one
/// [`crate::registry::hold`] takes), so a bring-up of the same token that
/// comes after this call waits for it: the teardown is keyed by the
/// token, and run after the new task subscribed it would drop the new
/// subscription. The guard is taken synchronously when free, which is the
/// case for an unregister; when a bring-up of this token holds it (a row
/// found gone while it came up), the teardown queues behind that one.
pub fn forget(state: &crate::ListenerState, token: &str) -> tokio::task::JoinHandle<()> {
    state.registry.clear_down(token);
    let teardown = stop_held(state, token);
    let registry = state.registry.clone();
    let token = token.to_string();
    let guard = registry.bring_up_guard(&token);
    let taken = guard.clone().try_lock_owned();
    tokio::spawn(async move {
        let held = match taken {
            Ok(held) => held,
            Err(_) => guard.clone().lock_owned().await,
        };
        teardown.await;
        drop(held);
        drop(guard);
        registry.release_bring_up_guard(&token);
    })
}

/// Stop what runs under `token`: its entry leaves the registry at once,
/// and the returned future aborts its task and has its kind tear down what
/// it holds outside this process ([`on_unregister`]). Every path that ends
/// or replaces a held entry goes through here, so no displaced entry skips
/// its teardown and leaves, say, a provider subscription posting to it.
pub fn stop_held(state: &crate::ListenerState, token: &str) -> impl std::future::Future<Output = ()> + Send + 'static {
    // Our handle to the task goes first, so the loop is not left serving
    // while its teardown talks to the provider.
    let displaced = remove_here(state, token);
    let broker = state.events_broker.clone();
    let token = token.to_string();
    async move {
        let Some(sig) = displaced else { return };
        on_unregister(&token, &sig, &broker).await;
    }
}

/// Stop what runs under `token` in this process and nothing more, for a
/// signal that goes on elsewhere: another holder took it, or this process
/// is handing it on as it stops. Its entry leaves the registry, its task
/// stops, and it is no longer counted down; what it arranged outside stays
/// (a provider subscription is keyed by the token, so tearing it down
/// would undo what the next holder arranged).
pub fn stop_here(state: &crate::ListenerState, token: &str) {
    state.registry.clear_down(token);
    remove_here(state, token);
}

/// Take `token`'s entry out of the registry and drop its handle to the
/// task, which stops the task once no reader still holds a copy.
fn remove_here(state: &crate::ListenerState, token: &str) -> Option<RegisteredSignal> {
    let mut sig = state.registry.remove(token)?;
    drop(sig.task.take());
    Some(sig)
}

/// The context a kind's work on one signal runs with.
#[allow(clippy::too_many_arguments)]
fn spawn_ctx(
    state: &crate::ListenerState,
    token: &str,
    tenant_id: &str,
    for_instance: Option<weft_core::instance::InstanceScope>,
    spec: &SignalSpec,
    fresh: bool,
    serving: Arc<Mutex<ServingState>>,
) -> SpawnCtx {
    SpawnCtx {
        fire: FireContext::new(
            state.fire_sink.clone(),
            token.to_string(),
            tenant_id.to_string(),
            for_instance,
            spec.match_predicates.clone(),
        ),
        config: state.config.clone(),
        events_broker: state.events_broker.clone(),
        fresh,
        serving,
    }
}

/// The path the alarm calls the listener back at.
// SYNC: WAKE_PATH <-> crates/weft-listener/src/router.rs (the route)
pub const WAKE_PATH: &str = "/wake";

/// What a wake carries back.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WakeBody {
    pub token: String,
    /// The moment the kind asked to be woken at. Carried in the body
    /// rather than read off the alarm's delivery time, because a platform
    /// may deliver a far wake EARLY (Cloud Tasks cannot schedule past 30
    /// days, so the GCP alarm sets the longest it can): a wake arriving
    /// before this moment is set again for it and wakes nothing.
    pub due_at_ms: i64,
}

/// How early a wake may arrive and still count as its moment: clocks on
/// the alarm's side and on this one never agree to the millisecond, and a
/// wake set again for a moment a hair away would be the same wake (same
/// key, same time), which the platform drops as already set.
const EARLY_WAKE_TOLERANCE: std::time::Duration = std::time::Duration::from_secs(5);

/// Set a `Wakes` signal's next wake from its state, when it has one.
pub async fn arm_next_wake(
    state: &crate::ListenerState,
    handler: &'static dyn KindHandler,
    token: &str,
    spec: &SignalSpec,
    kind_state: &Value,
    from: WakeFrom,
) -> Result<()> {
    let now_ms = now_unix_ms();
    let Some(at) = handler.next_wake(spec, kind_state, from, now_ms)? else {
        return Ok(());
    };
    set_wake(state, token, at).await
}

async fn set_wake(state: &crate::ListenerState, token: &str, at_unix_ms: i64) -> Result<()> {
    state
        .alarm
        .set(weft_platform_traits::Wake {
            key: format!("signal:{token}"),
            at_unix_ms,
            role: weft_platform_traits::CoreRole::Listener,
            path: WAKE_PATH.to_string(),
            body: serde_json::to_value(WakeBody { token: token.to_string(), due_at_ms: at_unix_ms })?,
        })
        .await
}

/// Handle one wake: read the signal's durable row, let its kind act,
/// set the next wake. A wake whose signal is gone does nothing (a wake is
/// only ever set once its row is committed, see [`bring_up`], so no row
/// means gone), and so does one whose row no longer wakes (registered
/// again to be served another way: the wake was set for the row it
/// replaced). One that arrived before its moment is set again for that
/// moment. An error answers the alarm with a failure, and every alarm
/// retries one.
pub async fn wake(state: &crate::ListenerState, body: WakeBody) -> Result<()> {
    let now_ms = now_unix_ms();
    let aimed_at_ms = body.due_at_ms;
    if aimed_at_ms - now_ms > EARLY_WAKE_TOLERANCE.as_millis() as i64 {
        return set_wake(state, &body.token, aimed_at_ms).await;
    }
    let Some(row) = state.signals.get_held(&body.token).await? else {
        tracing::debug!(target: "weft_listener::kinds", token = %body.token, "a wake for a signal that is gone; nothing to do");
        return Ok(());
    };
    let spec: SignalSpec = serde_json::from_str(&row.spec_json)
        .map_err(|e| anyhow::anyhow!("malformed spec_json for signal {}: {e}", row.token))?;
    let handler = handler_or_err(&spec.kind)?;
    if handler.between_fires(&spec, &row.kind_state)? != BetweenFires::Wakes {
        tracing::info!(
            target: "weft_listener::kinds", token = %row.token, kind = %spec.kind,
            "a wake for a signal that no longer wakes (its row was registered again); nothing to do"
        );
        return Ok(());
    }
    // A `Wakes` kind keeps nothing in this process between calls (see
    // `bring_up`), so its serving slot lives for this one wake.
    let serving = Arc::new(Mutex::new(ServingState::default()));
    let ctx = spawn_ctx(state, &row.token, &row.tenant_id, row.for_instance.clone(), &spec, false, serving);
    let woken = Woken { aimed_at_ms, now_ms, state: row.kind_state.clone(), seq: row.kind_state_seq, is_resume: row.is_resume };
    let Some(after) = handler.on_wake(&spec, woken, ctx).await? else {
        return Ok(());
    };
    arm_next_wake(state, handler, &row.token, &spec, &after, WakeFrom::Woken { aimed_at_ms }).await
}

fn now_unix_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Process one stateless fire. Resume signals route to the
/// suspended execution regardless of kind; entry signals delegate to
/// the kind's `process_entry`. A signal with no held row drops the fire,
/// saying why.
pub async fn process(state: &crate::ListenerState, token: &str, payload: Value) -> Result<ProcessOutcome> {
    let Some(signal) = crate::registry::held(state, token).await? else {
        return Ok(ProcessOutcome {
            value: payload,
            target: ProcessTarget::Drop { reason: Some("no signal is held under this token".into()) },
        });
    };

    if signal.is_resume {
        let Some(execution_id) = signal.execution_id.clone() else {
            tracing::warn!(
                target: "weft_listener::kinds",
                %token,
                "is_resume signal has no execution; dropping"
            );
            return Ok(ProcessOutcome {
                value: payload,
                target: ProcessTarget::Drop {
                    reason: Some("is_resume signal missing execution".into()),
                },
            });
        };
        return Ok(ProcessOutcome {
            value: payload,
            target: ProcessTarget::Resume { execution_id },
        });
    }

    let handler = handler_or_err(&signal.spec.kind)?;
    Ok(handler.process_entry(&signal, payload))
}

/// What a held signal wakes with when a person wakes it by hand.
///
/// `Ok(None)` is a real answer: the kind cannot be woken that way, and
/// whoever asked should say so rather than invent a payload. An unknown
/// token is an error: the caller read the signal's row a moment ago.
pub async fn wake_by_hand(state: &crate::ListenerState, token: &str) -> Result<Option<Value>> {
    let signal = crate::registry::held(state, token)
        .await?
        .ok_or_else(|| anyhow::anyhow!("unknown token: {token}"))?;
    hand_wake_payload(&signal)
}

/// [`wake_by_hand`] for a signal already in hand.
fn hand_wake_payload(signal: &RegisteredSignal) -> Result<Option<Value>> {
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
/// A token with no held row is skipped: the signal went between the
/// dispatcher narrowing the candidates and this answer.
pub async fn match_push(state: &crate::ListenerState, push: &PushEvent, tokens: &[String]) -> Result<Vec<MatchedPush>> {
    let mut held = Vec::with_capacity(tokens.len());
    for token in tokens {
        if let Some(signal) = crate::registry::held(state, token).await? {
            held.push((token.clone(), signal));
        }
    }
    Ok(match_held(push, &held))
}

/// [`match_push`] over signals already in hand.
fn match_held(push: &PushEvent, held: &[(String, RegisteredSignal)]) -> Vec<MatchedPush> {
    let mut matched = Vec::new();
    for (token, signal) in held {
        let Some(handler) = lookup(&signal.spec.kind) else {
            tracing::warn!(
                target: "weft_listener::kinds",
                %token, kind = %signal.spec.kind,
                "a registered signal has no handler; it cannot be fed by a push"
            );
            continue;
        };
        let Some(payload) = handler.match_push(signal, push) else { continue };
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

/// What a registered signal shows: its kind's `live` answer.
///
/// The listener serves this on `POST /live`, and what comes back is the
/// same shape an infra node's container serves on its own `/live`, so
/// whoever reads a node's display draws one thing whichever produced it.
pub fn compute_live(ctx: &LiveCtx<'_>) -> LiveFeed {
    match lookup(&ctx.sig.spec.kind) {
        Some(handler) => handler.live(ctx),
        // An unknown kind is a listener older than the program that
        // registered the signal. Say that, rather than showing an
        // empty panel that reads as "nothing to report".
        None => LiveFeed::new(vec![LiveItem::text(
            "Display",
            format!("this listener does not know the kind '{}'", ctx.sig.spec.kind),
        )]),
    }
}

/// A trigger's display is read-only: no door carries a press to a
/// listener, so a button on one would be a button the reader cannot
/// use. A kind that puts one there has a bug, and it is said at the
/// door (`/live` answers 500 with this), rather than drawn as a button
/// that fails on the click.
pub fn read_only_display(kind: &str, live: &LiveFeed) -> Result<(), String> {
    match live.items.iter().find(|item| item.action.is_some()) {
        Some(item) => Err(format!(
            "the kind '{kind}' put a button on '{}', and a trigger's display is read-only: no door \
             carries a press to a listener. If a trigger kind needs a button, open an issue on \
             weft's repository to discuss it before wiring one",
            item.label
        )),
        None => Ok(()),
    }
}

/// The address lines every public-entry kind shows: where a caller
/// sends a request, and how they get past the door.
///
/// The address is the dispatcher's to compute and is shown verbatim,
/// because only the dispatcher knows the host it answers on, the
/// tenant segment the path is stored under, and whether the kind is
/// served under `/connect/`. Without one, the line falls back to the
/// route pattern the kind itself holds, which is a fragment of the
/// real address and says so, rather than reading as somewhere to send.
///
/// Nothing secret is ever here. A gated route names the CONNECTION
/// that holds the material, and the broker answers the check; the
/// listener never sees a key.
///
/// A kind whose surface is internal (a timer, a poll) has no address,
/// and gets no lines from here.
pub fn address_items(ctx: &LiveCtx<'_>) -> Vec<LiveItem> {
    use weft_core::primitive::{SignalAuth, SignalSurface};
    let sig = ctx.sig;
    let SignalSurface::PublicEntry { path, methods } = &sig.routing.surface else {
        return Vec::new();
    };
    // The methods ride WITH the address rather than on a line of their
    // own: "POST https://..." is one thing a person copies, and a
    // route that serves any method says nothing rather than "ANY".
    let verbs = if methods.is_empty() { String::new() } else { format!("{} ", methods.join("/")) };
    let mut items = vec![match ctx.address {
        Some(address) => LiveItem::text("Address", format!("{verbs}{address}")),
        None => LiveItem::text(
            "Route (partial)",
            format!("{verbs}/{}", path.trim_start_matches('/')),
        ),
    }];
    items.push(match sig.routing.auth {
        // "open" is about the door, not about who knows the address.
        SignalAuth::None => LiveItem::text("Auth", "open (anyone with the URL)"),
        SignalAuth::Connection => {
            // `compute_routing` writes `service` alongside `access_id` on
            // every gated route, so a missing one is a broken
            // registration. Say that instead of inventing a name for it.
            match sig.routing.auth_config.get("service").and_then(Value::as_str) {
                Some(service) => LiveItem::text(
                    "Auth",
                    format!("checked against the wired {service} connection"),
                ),
                None => LiveItem::text(
                    "Auth",
                    "gated, but this registration names no connection to check against",
                ),
            }
        }
    });
    items
}

/// A URL the user configured, as a line their display can carry.
///
/// A configured URL is the one place a trigger's display can hold a
/// credential: a Telegram poll loop carries its bot token in the PATH
/// (`/bot<token>/getUpdates`), an SSE feed carries `?access_token=`,
/// and any of them can carry `user:pass@`. So the whole URL is shown
/// as a SECRET: the reader sees it masked, and reveals or copies it
/// when they actually need it. The label is the kind's own word for
/// what the URL is ("Polling", "Listening to").
///
/// An empty URL is not a line at all. A socket that dials an address
/// minted per connection has none to show, and a blank box reads as a
/// bug in the display rather than as the truth about the signal.
pub fn configured_url_item(label: &str, url: &str) -> Option<LiveItem> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }
    Some(LiveItem::secret(label, url))
}

/// Read a kind's own config for its display, or the line that says why
/// it could not be read.
///
/// Register refuses a spec that does not parse, so a signal whose
/// config fails here is one registered by a program older or newer than
/// this listener, or a row that rotted. Either way the reader gets the
/// serde error itself, not a shrug: it names the field and the shape,
/// which is the whole difference between "something is wrong" and
/// something anybody can act on. One line, so the rest of the display
/// still renders.
pub fn config_for_display<T: serde::de::DeserializeOwned>(
    sig: &RegisteredSignal,
    label: &str,
) -> Result<T, LiveFeed> {
    serde_json::from_value::<T>(sig.spec.config.clone()).map_err(|e| {
        LiveFeed::new(vec![LiveItem::text(
            label,
            format!("this listener cannot read the '{}' config: {e}", sig.spec.kind),
        )])
    })
}

/// One line for what a kind's background task is doing right now
/// (connected, retrying, unservable), or nothing when the kind keeps
/// no such task. Kinds that hold a connection add it to their display.
pub fn serving_item(sig: &RegisteredSignal) -> Option<LiveItem> {
    // Copy out and drop the guard: the kind's serving task writes this
    // slot, and nothing it writes should wait on a `format!` here.
    let (status, transport) = {
        let s = sig.serving.lock();
        (s.status.clone(), s.transport.clone())
    };
    if status.is_empty() && transport.is_none() {
        return None;
    }
    let transport = match transport {
        Some(Transport::Socket) => " (socket)".to_string(),
        Some(Transport::Webhook) => " (webhook)".to_string(),
        Some(Transport::Unservable(why)) => format!(" (unservable: {why})"),
        None => String::new(),
    };
    let status = if status.is_empty() { "serving" } else { status.as_str() };
    Some(LiveItem::text("State", format!("{status}{transport}")))
}

/// Write what a kind's serving task is doing right now where its
/// display reads it (`serving_item`). The one home for the write, so
/// every kind that holds a connection or a loop reports the same way.
pub fn set_serving_status(ctx: &SpawnCtx, status: impl Into<String>) {
    ctx.serving.lock().status = status.into();
}

/// Where an engine that holds a connection reports what it is doing.
/// The usual sink is the one registration's slot (`serving_sink`); an
/// engine that serves several registrations at once (a provider socket
/// shared by every subscription of one topic) fans one report out to
/// every slot it serves, so no panel says "holding" while the socket
/// is down.
pub type ServingReport = Arc<dyn Fn(String) + Send + Sync>;

/// The report that writes to this registration's own slot.
pub fn serving_sink(ctx: &SpawnCtx) -> ServingReport {
    let serving = ctx.serving.clone();
    Arc::new(move |status| serving.lock().status = status)
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

    // ----- What a trigger shows -------------------------------------
    //
    // The kind owns its display, so these prove what a public entry
    // says about itself: the address a caller uses (the dispatcher's,
    // verbatim) and the door, which names a connection and never a
    // secret.

    fn signal(surface: weft_core::primitive::SignalSurface, auth_config: Value) -> RegisteredSignal {
        use weft_core::primitive::SignalAuth;
        let auth = if auth_config.is_null() { SignalAuth::None } else { SignalAuth::Connection };
        RegisteredSignal {
            spec: SignalSpec {
                kind: "timer".into(),
                config: Value::Object(Default::default()),
                consumer_kind: None,
                access: None,
                match_predicates: Vec::new(),
                limits: Default::default(),
                run_class: Default::default(),
            },
            node_id: "ask".into(),
            tenant_id: "t".into(),
            is_resume: false,
            execution_id: None,
            task: None,
            kind_state: None,
            routing: SignalRouting { surface, auth, auth_config },
            serving: Arc::new(Mutex::new(ServingState::default())),
        }
    }

    fn public(path: &str, methods: &[&str]) -> weft_core::primitive::SignalSurface {
        weft_core::primitive::SignalSurface::PublicEntry {
            path: path.into(),
            methods: methods.iter().map(|m| m.to_string()).collect(),
        }
    }

    fn ctx<'a>(sig: &'a RegisteredSignal, address: Option<&'a str>) -> LiveCtx<'a> {
        LiveCtx { sig, address }
    }

    #[test]
    fn a_public_entry_shows_the_address_a_caller_actually_uses() {
        // The dispatcher's address, verbatim: it carries the host, the
        // tenant segment and the `/connect/` prefix, none of which the
        // listener's own route pattern has.
        let sig = signal(public("hooks/x", &[]), Value::Null);
        let items = address_items(&ctx(&sig, Some("https://w.example/local/hooks/x")));
        assert_eq!(
            items,
            vec![
                LiveItem::text("Address", "https://w.example/local/hooks/x"),
                LiveItem::text("Auth", "open (anyone with the URL)"),
            ]
        );
    }

    #[test]
    fn the_methods_ride_with_the_address() {
        // One thing to copy. A route that serves any method says
        // nothing rather than "ANY".
        let sig = signal(public("chat/{room}", &["POST", "PUT"]), Value::Null);
        let items = address_items(&ctx(&sig, Some("https://w.example/local/chat/{room}")));
        assert_eq!(
            items[0],
            LiveItem::text("Address", "POST/PUT https://w.example/local/chat/{room}")
        );
    }

    #[test]
    fn a_gated_route_names_the_connection_and_nothing_secret() {
        let sig = signal(
            public("chat", &[]),
            serde_json::json!({ "access_id": "acc-1", "service": "api_key_auth" }),
        );
        let items = address_items(&ctx(&sig, Some("https://w.example/local/chat")));
        assert_eq!(
            items[1],
            LiveItem::text("Auth", "checked against the wired api_key_auth connection")
        );
        // The material lives on the connection; the listener has none
        // of it and the display must not imply otherwise.
        let shown = serde_json::to_string(&items).expect("items serialize");
        assert!(!shown.contains("acc-1"), "{shown}");
    }

    #[test]
    fn without_an_address_the_line_says_it_is_only_a_fragment() {
        // Nobody can send to this, so it must not read like somewhere
        // to send. Only a caller that skipped the dispatcher sees it.
        let sig = signal(public("hooks/x", &[]), Value::Null);
        let items = address_items(&ctx(&sig, None));
        assert_eq!(items[0], LiveItem::text("Route (partial)", "/hooks/x"));
    }

    #[test]
    fn a_signal_that_nothing_calls_into_has_no_address_to_show() {
        let sig = signal(weft_core::primitive::SignalSurface::Internal, Value::Null);
        assert!(address_items(&ctx(&sig, Some("https://w.example/x"))).is_empty());
    }

    #[test]
    fn a_button_on_a_triggers_display_is_refused_at_the_door() {
        // Nothing carries a press to a listener, so a kind that puts a
        // button on its display has shipped one the reader cannot use;
        // the door says so instead of drawing it.
        let with_button = LiveFeed::new(vec![LiveItem::text("Phone", "paired")
            .with_action(weft_core::live::LiveAction::new("Disconnect", "unpair"))]);
        let why = read_only_display("poll_endpoint", &with_button).expect_err("a button is refused");
        assert!(why.contains("poll_endpoint") && why.contains("Phone") && why.contains("read-only"), "{why}");
        let plain = LiveFeed::new(vec![LiveItem::text("Phone", "paired")]);
        assert!(read_only_display("poll_endpoint", &plain).is_ok());
    }

    #[test]
    fn a_kind_this_listener_does_not_know_says_that_rather_than_showing_nothing() {
        let mut sig = signal(weft_core::primitive::SignalSurface::Internal, Value::Null);
        sig.spec.kind = "from_a_newer_program".into();
        let feed = compute_live(&ctx(&sig, None));
        assert_eq!(feed.items.len(), 1);
        assert!(
            feed.items[0].data.as_str().unwrap().contains("from_a_newer_program"),
            "{:?}",
            feed.items[0]
        );
    }

    /// A registered signal of one kind, carrying the config a test wants
    /// its display computed from.
    fn with_config(kind: &str, config: Value) -> RegisteredSignal {
        let mut sig = signal(weft_core::primitive::SignalSurface::Internal, Value::Null);
        sig.spec.kind = kind.into();
        sig.spec.config = config;
        sig
    }

    #[test]
    fn a_poll_url_carrying_a_token_is_masked_not_printed() {
        // The kind's own motivating case: a bot token lives in the PATH,
        // so there is no query string to strip and no part of the URL
        // that is safe to show.
        let sig = with_config(
            "poll_endpoint",
            serde_json::json!({
                "url": "https://api.telegram.org/bot12345:SECRET/getUpdates",
                "interval_secs": 30
            }),
        );
        let feed = compute_live(&ctx(&sig, None));
        assert_eq!(feed.items[0].kind, weft_core::live::LiveItemKind::Secret);
        assert_eq!(feed.items[0].label, "Polling");
        assert_eq!(feed.items[1], LiveItem::text("Every", "30s"));
        // Masked, and still there for the reader who needs it: the
        // panel reveals a secret on a click and copies the real value.
        assert_eq!(feed.items[0].data.as_str().unwrap(), "https://api.telegram.org/bot12345:SECRET/getUpdates");
    }

    /// A configured URL is the one place a trigger's display can hold a
    /// credential, so every kind that shows one masks it.
    #[test]
    fn every_kind_that_shows_a_configured_url_masks_it() {
        let cases = [
            ("sse_subscribe", serde_json::json!({
                "url": "https://feed.example/stream?access_token=SECRET",
                "event_name": "tick"
            })),
            ("socket_listen", serde_json::json!({
                "url": "wss://gw.example/socket?token=SECRET"
            })),
            ("stream_listen", serde_json::json!({
                "address": "user:pass@stream.example:9000",
                "tls": true,
                "framing": { "kind": "delimiter", "bytes": "\r\n" },
                "fire": ".*"
            })),
        ];
        for (kind, config) in cases {
            let sig = with_config(kind, config);
            let feed = compute_live(&ctx(&sig, None));
            assert_eq!(
                feed.items[0].kind,
                weft_core::live::LiveItemKind::Secret,
                "{kind} shows its url in the clear"
            );
        }
    }

    #[test]
    fn a_socket_with_no_configured_url_says_where_it_dials_instead() {
        // A dynamic gateway mints the address per connection, so there
        // is none to show; an empty box would read as a broken display.
        let sig = with_config("socket_listen", serde_json::json!({ "url": "" }));
        let feed = compute_live(&ctx(&sig, None));
        assert_eq!(
            feed.items[0],
            LiveItem::text("Listening to", "an address minted for each connection")
        );
    }

    #[test]
    fn a_config_this_listener_cannot_read_says_what_is_wrong_with_it() {
        // Register refuses a spec that does not parse, so this is a row
        // from another version. The serde error names the field, which
        // is the difference between a shrug and something actionable.
        let sig = with_config("timer", serde_json::json!({ "spec": { "kind": "hourly" } }));
        let feed = compute_live(&ctx(&sig, None));
        assert_eq!(feed.items.len(), 1);
        assert_eq!(feed.items[0].label, "Fires");
        let said = feed.items[0].data.as_str().unwrap();
        assert!(said.contains("timer"), "{said}");
        assert!(said.contains("hourly") || said.contains("unknown variant"), "{said}");
    }

    #[test]
    fn a_gated_route_with_no_connection_named_says_so() {
        let mut sig = signal(public("chat", &[]), serde_json::json!({ "access_id": "acc-1" }));
        sig.routing.auth = weft_core::primitive::SignalAuth::Connection;
        let items = address_items(&ctx(&sig, Some("https://w.example/local/chat")));
        assert_eq!(
            items[1],
            LiveItem::text("Auth", "gated, but this registration names no connection to check against")
        );
    }

    // ----- Matching a provider push against what this process holds ------
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
                limits: Default::default(),
                run_class: Default::default(),
            },
            node_id: "n1".into(),
            tenant_id: "t1".into(),
            is_resume: false,
            execution_id: None,
            task: None,
            kind_state: None,
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

    fn holding(entries: &[(&str, RegisteredSignal)]) -> Vec<(String, RegisteredSignal)> {
        entries.iter().map(|(token, sig)| ((*token).to_string(), sig.clone())).collect()
    }

    #[test]
    fn a_push_feeds_the_subscription_on_its_own_topic() {
        let event = serde_json::json!({ "channel": "C1", "text": "hi" });
        let registry = holding(&[(
            "tok",
            registered("provider_events", subscription("messages", "account"), vec![]),
        )]);
        let got = match_held(&push("messages", event.clone()), &registry);
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
        let got = match_held(&push("messages", serde_json::json!({})), &registry);
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
        let got = match_held(&push("messages", serde_json::json!({})), &registry);
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
        assert!(match_held(&push("messages", wrong), &registry).is_empty());
        let right = serde_json::json!({ "channel": "C1" });
        assert_eq!(match_held(&push("messages", right), &registry).len(), 1);
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
        let got = match_held(&push("messages", serde_json::json!({})), &registry);
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
        let payload = hand_wake_payload(&registry[0].1)
            .expect("the kind is known")
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
            let answer = hand_wake_payload(&registry[0].1).expect("the kind is known");
            assert!(answer.is_none(), "{kind} has nothing to wake with");
        }
    }
}
