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
//!   - Fires a kind raises itself (a timer's tick, an SSE event) are
//!     processed here, in the listener: `fire_sink` calls
//!     [`process_in`], which runs an entry's event through the kind's
//!     `process_entry`; the sink then hands it straight to its project's
//!     worker door, or parks it in its trigger's queue when that worker
//!     cannot be reached. Only an answer to a waiting run goes out as a
//!     `FireSignal` task. The dispatcher calls `/process` for an event
//!     that reaches it instead: a call at a public entry's own path or at
//!     `/signal/{token}`, a provider's push at `/events/...`, a wait
//!     woken by hand.
//!   - Resume signals (`is_resume = true`) always route to
//!     `ProcessTarget::Resume`; the kind's `process` impl is only
//!     consulted for entry-mode (`is_resume = false`).

pub mod event_source;
pub mod folder_watch;
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
use weft_core::signal::listener_protocol::{MatchedPush, PriorRow, ProcessOutcome, ProcessTarget, PushEvent, StartMode};
use crate::registry::{RegisteredSignal, Registrations, ServingState, TaskGuard, Transport};

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
    /// Told each time the serving state changes, so a holder says it
    /// (`crate::hold`): the row is where another process reads it.
    pub said: Arc<tokio::sync::Notify>,
    /// The credentials this process connects with, as the broker last said.
    pub reach: Arc<crate::reach::Reach>,
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
    /// When it arrived, or its moment when it arrived a little before it
    /// (within what counts as on time).
    pub now_ms: i64,
    /// The signal's durable state as read just now.
    pub stored: RowState,
    /// True when the signal is a parked run's wait (`await_signal`)
    /// rather than an entry trigger, read off the row like the state.
    pub is_resume: bool,
}

/// A signal's durable state as its row holds it, and the write-fence
/// version it is at: whoever acts on it writes the next version from
/// `seq` ([`FireContext::claim_kind_state`]).
#[derive(Debug, Clone, PartialEq)]
pub struct RowState {
    pub state: Value,
    pub seq: i64,
}

/// What a wake leaves: the state the signal now stands at, and the row
/// version it stands at (one past the one woken at when the wake wrote
/// it, the same when it wrote nothing). `None` when a write of it could
/// not be made, so the row may not say what the state says: the next wake
/// then reads the row instead of carrying the state.
#[derive(Debug, Clone)]
pub struct Woke {
    pub state: Value,
    pub seq: Option<i64>,
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

    /// Whether what this signal does as it first comes up (its first wake,
    /// its held connection) could work on this install, from its spec and
    /// the state [`Self::settle`] settled, asked before anything it would
    /// replace is taken down and doing nothing outside (a provider
    /// subscription that needs the provider to call in needs an address
    /// the internet reaches). What only an outside service can answer is
    /// not known until it is asked. Default: nothing to check beyond
    /// settling.
    async fn check_coming_up(&self, _spec: &SignalSpec, _kind_state: &Value, _ctx: &SpawnCtx) -> Result<()> {
        Ok(())
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
    /// of the listener) sets one wake, never two. After a wake
    /// (`WakeFrom::Woken`), a moment after the one handled: a wake's name
    /// is its moment's, and the task being answered still holds it.
    fn next_wake(&self, _spec: &SignalSpec, _state: &Value, _from: WakeFrom, _now_ms: i64) -> Result<Option<i64>> {
        Ok(None)
    }

    /// For a `Wakes` kind: handle one wake. A wake may arrive late,
    /// twice, or after the thing it was set for changed, so the kind
    /// recomputes from `woken.stored` whether anything is due, CLAIMS the
    /// moment through [`FireContext::claim_kind_state`] before acting (so
    /// of two copies woken for it, one acts), fires through `ctx.fire`,
    /// and answers the state it now stands at (for its next wake), or
    /// `None` when another copy claimed this moment first: that copy set
    /// the next wake from the state it wrote, so this one sets none.
    async fn on_wake(&self, _spec: &SignalSpec, _woken: Woken, _ctx: SpawnCtx) -> Result<Option<Woke>> {
        anyhow::bail!("the '{}' kind does not wake", self.tag())
    }

    /// For a `Wakes` kind: whether its wakes act on the row they carry
    /// ([`WakeBody::row`]) rather than read it, because most of them change
    /// nothing worth writing (a poll that found nothing new): such a wake
    /// touches no database.
    fn wakes_from_body(&self) -> bool {
        false
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

    /// The state a new signal's row starts from when nothing is carried
    /// over ([`prepare_signal`] decides that). Default: an empty object. A
    /// kind that keeps something across a listener restart keys it its
    /// own way (Timer: the next moment it fires, `{"next_fire_at_unix_ms":
    /// <unix ms>}`, or `{}` once nothing is left to fire).
    ///
    /// What a registration computes replaces the stored state, and moves
    /// the row to a new version, only while the row is still at the
    /// version it was read at (`signal_insert` is a compare-and-set). A
    /// wake that claimed a moment in between moved the version, so the
    /// dispatcher reads the row again and asks again; a wake claiming
    /// after the registration finds the version moved and loses.
    ///
    /// `asked_at_unix_ms` is when the registration was asked for (the
    /// node's `await_signal`, or the activation), for a kind whose state
    /// counts from that moment. `is_resume` says which of the two it is,
    /// for a kind that behaves differently while a run waits on it.
    fn compute_initial_state(&self, _spec: &SignalSpec, _asked_at_unix_ms: i64, _is_resume: bool) -> Result<Value> {
        Ok(Value::Object(serde_json::Map::new()))
    }

    /// For a kind that keeps its place in what it reads (a feed's cursor,
    /// what a folder held): what identifies that source in `spec`, so a
    /// registration again can tell whether it reads the same one. `None`,
    /// the default, for a kind with no place to keep. A kind with a source
    /// keeps nothing [`Self::settle`] decides in its state: a door that
    /// refuses gives it [`Self::compute_initial_state`]'s state, without
    /// settling again ([`fresh_place`]).
    fn source(&self, _spec: &SignalSpec) -> Result<Option<Value>> {
        Ok(None)
    }

    /// For a kind with a [`Self::source`]: the state a registration again
    /// starts from when [`prepare_signal`] carries over `prior`, the state
    /// of the row it replaces. It keeps the place and nothing else (no
    /// failure count, nothing about how the last registration was served).
    fn carry(&self, _spec: &SignalSpec, _prior: &Value) -> Result<Value> {
        anyhow::bail!("the '{}' kind names a source it reads from but says nothing of what it carries over", self.tag())
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
    /// `stored` is the kind's state as the row holds it (written at
    /// register time, or by the task itself since), and the write-fence
    /// version it is at.
    async fn spawn_task(
        &self,
        _spec: &SignalSpec,
        _stored: RowState,
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

    /// For a run's wait: what it kept of the last answer it saw from
    /// outside (a poll of a job's status), for a reader that parses it
    /// itself (`GetNodeState`'s `live`). Read off the signal as it stands
    /// now. Default: a kind with no answer to keep.
    fn last_answer(&self, _sig: &RegisteredSignal) -> Option<weft_core::signal::listener_protocol::LastAnswer> {
        None
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
    /// True iff this is a mid-run resume (HumanQuery, etc).
    pub is_resume: bool,
    /// Run of the suspended run to resume. Set iff `is_resume`.
    pub run_id: Option<weft_core::RunId>,
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
/// `prior` is the row already under the token, when there is one; what
/// it carries over is decided by [`carried`], the same way for every
/// kind. `asked_at_unix_ms` is when the registration was asked for.
pub async fn prepare_signal(
    state: &crate::ListenerState,
    identity: SignalIdentity,
    for_instance: Option<weft_core::instance::InstanceScope>,
    prior: Option<&PriorRow>,
    asked_at_unix_ms: i64,
) -> Result<Prepared> {
    let SignalIdentity { token, tenant_id, node_id, is_resume, run_id, spec } = identity;
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
    // A last call is sent when a run is cancelled while it waits; an entry
    // trigger has no run to cancel, so it would never go.
    if !is_resume && spec.on_cancel.is_some() {
        anyhow::bail!(
            "a '{}' entry trigger carries a last call on cancel, which only a run's wait can send; \
             wait with `ctx.await_signal_with_cancel` instead",
            spec.kind,
        );
    }
    let routing = handler.compute_routing(&spec)?;
    let computed = match carried(handler, &spec, is_resume, prior)? {
        Some(prior) => handler.carry(&spec, prior)?,
        None => handler.compute_initial_state(&spec, asked_at_unix_ms, is_resume)?,
    };
    // Settling may look at the signal's connection, through the broker,
    // as the signal itself would once up; its slot shows nothing, and it
    // has no registration yet to give up on, nor to keep what it fetched for.
    let ctx = spawn_ctx(state, &token, crate::reach::UNREGISTERED, &tenant_id, for_instance, &spec, true, Arc::default());
    let kind_state = handler.settle(&spec, computed, &ctx).await?;
    let holds = handler.between_fires(&spec, &kind_state)? == BetweenFires::Holds;
    let signal = RegisteredSignal {
        generation: crate::reach::UNREGISTERED,
        spec,
        node_id,
        tenant_id,
        is_resume,
        run_id,
        task: None,
        kind_state: Some(kind_state.clone()),
        routing: routing.clone(),
        serving: Arc::default(),
        gave_up: None,
    };
    let rendered = handler.render(&token, &signal)?;
    Ok(Prepared { routing, kind_state, rendered, holds })
}

/// Whether the signal `identity` could be registered and come up on this
/// install, decided without registering or starting it: what registering
/// it settles ([`prepare_signal`], which writes nothing and starts
/// nothing), then what its kind checks of its coming up
/// ([`KindHandler::check_coming_up`]). An error says why not. It settles
/// from a fresh state, carrying nothing over from a row: what decides
/// whether a signal can come up on the install (its kind, its connection,
/// the install's address) is never what a row carries.
pub async fn check_signal(
    state: &crate::ListenerState,
    identity: SignalIdentity,
    for_instance: Option<weft_core::instance::InstanceScope>,
    asked_at_unix_ms: i64,
) -> Result<()> {
    let handler = handler_or_err(&identity.spec.kind)?;
    let (token, tenant_id, spec) = (identity.token.clone(), identity.tenant_id.clone(), identity.spec.clone());
    let prepared = prepare_signal(state, identity, for_instance.clone(), None, asked_at_unix_ms).await?;
    let ctx = spawn_ctx(state, &token, crate::reach::UNREGISTERED, &tenant_id, for_instance, &spec, true, Arc::default());
    handler.check_coming_up(&spec, &prepared.kind_state, &ctx).await
}

/// The state of the replaced row `prior` a registration of `spec` carries
/// over, decided once here for every kind: only an entry's (a run's wait
/// starts afresh), only when its trigger listened the whole time the row
/// was not served ([`PriorRow::listened`]), and only for a kind that keeps
/// its place in a source ([`KindHandler::source`]) when the source is the
/// one the row read. So what arrived while the trigger listened is caught
/// up from where it left off, what arrived behind a door that refused
/// never is, and a trigger pointed at another feed or folder starts there
/// fresh. `None`: the kind starts fresh ([`KindHandler::compute_initial_state`]).
fn carried<'a>(handler: &dyn KindHandler, spec: &SignalSpec, is_resume: bool, prior: Option<&'a PriorRow>) -> Result<Option<&'a Value>> {
    let Some(prior) = prior.filter(|prior| !is_resume && prior.listened && prior.spec.kind == spec.kind) else {
        return Ok(None);
    };
    let Some(source) = handler.source(spec)? else {
        return Ok(None);
    };
    let before = handler.source(&prior.spec).map_err(|e| e.context("reading the source of the row this registration replaces"))?;
    Ok((before.as_ref() == Some(&source)).then_some(&prior.kind_state))
}

/// The state an entry signal of `spec` starts from once its place in what
/// it reads is gone, at `now_unix_ms`: a trigger that stops listening
/// behind a door that refuses loses it there, so whatever brings the
/// signal up later starts from its source as it finds it. Only for a kind
/// that keeps such a place ([`KindHandler::source`]); `None` for any other
/// kind, whose state stays as it is (a timer keeps its schedule). It is
/// the state a fresh registration computes
/// ([`KindHandler::compute_initial_state`]), unsettled (see
/// [`KindHandler::source`]).
pub fn fresh_place(spec: &SignalSpec, now_unix_ms: i64) -> Result<Option<Value>> {
    let handler = handler_or_err(&spec.kind)?;
    if handler.source(spec)?.is_none() {
        return Ok(None);
    }
    handler.compute_initial_state(spec, now_unix_ms, false).map(Some)
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
            let woken = WakeBody {
                token: row.token.clone(),
                due_at_ms: now_unix_ms(),
                generation: Some(row.generation),
                set_again: 0,
                row: None,
                ended: false,
            };
            match wake(state, woken).await? {
                None => Ok(()),
                Some(why) => anyhow::bail!("{why}"),
            }
        }
        BetweenFires::Wakes => arm_next_wake(state, handler, &spec, &row, WakeFrom::Armed, now_unix_ms(), true).await,
        BetweenFires::Holds => {
            let routing = row.to_routing().map_err(|e| anyhow::anyhow!("to_routing for signal {}: {e}", row.token))?;
            // What already runs under the token stops, teardown included,
            // BEFORE the new task starts: an outside teardown is keyed by
            // the token (a provider subscription is dropped by it), so run
            // after the new task subscribed it would drop the new one.
            // Only an older registration (or this one) is displaced: a
            // bring-up of a stale row never gets this far
            // ([`crate::registry::hold`] leaves a newer one be).
            stop_held(state, &row.token, Registrations::UpTo(row.generation)).await;
            let serving = Arc::new(Mutex::new(ServingState::default()));
            let mut ctx = spawn_ctx(state, &row.token, row.generation, &row.tenant_id, row.for_instance.clone(), &spec, fresh, serving.clone());
            // Its fires go out under this holder's claim, so a copy that
            // lost the row (to another holder, or to a registration that no
            // longer holds) delivers nothing from then on.
            ctx.fire = ctx.fire.held_by(state.config.replica.clone());
            let stored = RowState { state: row.kind_state.clone(), seq: row.kind_state_seq };
            let Some(task) = handler.spawn_task(&spec, stored, ctx).await? else {
                anyhow::bail!(
                    "the '{}' kind holds a connection but started no task; its `between_fires` \
                     and its `spawn_task` disagree",
                    spec.kind,
                );
            };
            state.registry.insert(
                row.token,
                RegisteredSignal {
                    generation: row.generation,
                    spec,
                    node_id: row.node_id,
                    tenant_id: row.tenant_id,
                    is_resume: row.is_resume,
                    run_id: row.run_id,
                    task: Some(Arc::new(TaskGuard::new(task))),
                    kind_state: None,
                    routing,
                    serving,
                    gave_up: None,
                },
            );
            Ok(())
        }
    }
}

/// A signal's row was removed: stop it, and tear down what it arranged
/// outside this process. A signal that wakes has its wakes cancelled,
/// whether or not it is counted down here. A connection this process
/// holds stops through [`forget`]; any other signal tears down from the
/// removed row the request carries (a subscription it renews on its
/// wakes), loud in logs on failure, and detached unless the token is
/// `reused` right after. Which of the two is decided by the removed
/// registration's own presence here, never by an older one of the token
/// lingering here (which `forget` stops beside it). A connection a holder
/// elsewhere holds stops there, when its claim goes with the row.
///
/// The removed row is one registration of its token (`req.generation`),
/// and a token can be registered again (a resume's token is fixed), so
/// an unregister can arrive after a newer registration of the token came
/// up here: it then stops nothing and tears nothing down (the teardown is
/// keyed by the token, so it would undo what the newer one arranged), and
/// only the removed row's own wakes, named by its generation, are
/// cancelled. Only a newer registration that RUNS here counts as come
/// up: one that is counted down or given up on runs nothing, so the
/// removed row's own entry, which may still be what runs (the newer one
/// failed before displacing it), is stopped; what is newer is left be,
/// since [`forget`] covers only the removed registration and older, and
/// no outside teardown keyed by the token is run beside it. A newer
/// registration this listener knows nothing of, running on a sibling
/// listener, is found in the token's row ([`superseded_elsewhere`]) and
/// keeps every token-keyed teardown off the same way.
///
/// A row this listener cannot tear down is an error, which says whether
/// trying again can help ([`UnregisterError`]).
pub async fn unregister(
    state: &crate::ListenerState,
    req: weft_core::signal::listener_protocol::UnregisterRequest,
) -> std::result::Result<(), UnregisterError> {
    let mine = Registrations::UpTo(req.generation);
    let read = lookup(&req.spec.kind).ok_or_else(|| UnregisterError::UnknownKind(req.spec.kind.clone())).and_then(|handler| {
        let between = handler.between_fires(&req.spec, &req.kind_state).map_err(|e| {
            UnregisterError::Unreadable(e.context(format!("signal {}'s state cannot be read, so what it arranged outside is not torn down", req.token)))
        })?;
        Ok((handler, between))
    });
    if let Some(newer) = state.registry.get(&req.token).map(|s| s.generation).filter(|g| *g > req.generation) {
        tracing::debug!(
            target: "weft_listener::kinds",
            token = %req.token, generation = req.generation, newer,
            "an unregister of an older registration arrived after a newer one came up; the newer one stays"
        );
        if let Ok((_, BetweenFires::Wakes)) = &read {
            cancel_wakes_of(state, &req).await?;
        }
        return Ok(());
    }
    // Registration `g` itself, when it is here, stops and tears down
    // through `forget`, and so does anything older still lingering here
    // (a leftover counted down, a held one whose successor's kind no
    // longer holds). When `g` never came up here (it wakes or is called
    // and came up on a sibling listener), `forget` has no entry of it to
    // tear down, so its outside teardown runs from the removed row the
    // request carries, beside the leftovers' and not instead of it. A
    // newer registration counted down or given up on here keeps that
    // token-keyed teardown off, since it would undo what the newer one
    // arranges.
    let present = state.registry.present(&req.token, req.generation);
    let newer = state.registry.newer_than(&req.token, req.generation).is_some();
    let superseded = !present && !newer && superseded_elsewhere(state, &req, &read).await?;
    // Counted down here first, so a retry stops before its wakes are
    // cancelled. A newer registration running on a sibling listener keeps
    // every teardown keyed by the token off, the leftovers' included: they
    // only stop here.
    let leftovers = match superseded {
        true => {
            stop_here_only(state, &req.token, mine);
            None
        }
        false => Some(forget(state, &req.token, mine)),
    };
    let from_row = if present || newer || superseded {
        // One whose kind or state cannot be read never came up, so it set
        // no wake.
        if let Ok((_, BetweenFires::Wakes)) = &read {
            cancel_wakes_of(state, &req).await?;
        }
        None
    } else {
        let (handler, between) = read?;
        match between {
            // A connection held elsewhere stops there, when its claim goes
            // with the row.
            BetweenFires::Holds => None,
            BetweenFires::Wakes => {
                cancel_wakes_of(state, &req).await?;
                Some(handler)
            }
            BetweenFires::Called => Some(handler),
        }
    };
    let teardown = from_row.map(|handler| {
        let sig = RegisteredSignal {
            generation: req.generation,
            spec: req.spec,
            node_id: String::new(),
            tenant_id: req.tenant_id,
            is_resume: false,
            run_id: None,
            task: None,
            kind_state: Some(req.kind_state),
            routing: SignalRouting {
                surface: weft_core::primitive::SignalSurface::Internal,
                auth: weft_core::primitive::SignalAuth::None,
                auth_config: Value::Null,
            },
            serving: Arc::default(),
            gave_up: None,
        };
        let broker = state.events_broker.clone();
        let token = req.token.clone();
        tokio::spawn(async move { handler.on_unregister(&token, &sig, &broker).await })
    });
    if req.reused {
        let ended = |e: tokio::task::JoinError| UnregisterError::TeardownFailed(anyhow::anyhow!("the teardown of signal {} ended abnormally: {e}", req.token));
        if let Some(leftovers) = leftovers {
            leftovers.await.map_err(ended)?;
        }
        if let Some(teardown) = teardown {
            teardown.await.map_err(ended)?;
        }
    }
    Ok(())
}

/// Whether removed registration `req`, which neither it nor anything newer
/// of its token runs here, has been superseded by a newer registration of
/// the token that a sibling listener runs: its row now listens at a newer
/// generation. Then the token-keyed teardown would undo what that newer
/// one arranged outside (cancel the provider subscription it relies on),
/// so it is not run. Read through the broker only when that teardown is
/// about to run (an older entry of the token lingers here, or the removed
/// row's kind tears down from its row), and never for a `reused` token:
/// its caller brings the newer one up only after this answers, so what
/// the row now reads is not running anywhere yet and the removed one's
/// teardown must run. A row read that fails fails this unregister
/// ([`UnregisterError::RowUnread`]): tearing down blind could cancel a
/// live subscription, skipping blind could leave a dead one renewed.
async fn superseded_elsewhere(
    state: &crate::ListenerState,
    req: &weft_core::signal::listener_protocol::UnregisterRequest,
    read: &std::result::Result<(&'static dyn KindHandler, BetweenFires), UnregisterError>,
) -> std::result::Result<bool, UnregisterError> {
    let lingering = state.registry.get(&req.token).is_some();
    let from_row = matches!(read, Ok((_, BetweenFires::Wakes | BetweenFires::Called)));
    if req.reused || !(lingering || from_row) {
        return Ok(false);
    }
    let row = state.signals.get_held(&req.token).await.map_err(|e| {
        UnregisterError::RowUnread(e.context(format!(
            "signal {}'s row cannot be read, so whether a newer registration of it runs elsewhere is unknown and nothing is torn down",
            req.token
        )))
    })?;
    let Some(newer) = row.map(|r| r.generation).filter(|g| *g > req.generation) else {
        return Ok(false);
    };
    tracing::debug!(
        target: "weft_listener::kinds",
        token = %req.token, generation = req.generation, newer,
        "an unregister of an older registration arrived after a newer one came up on another listener; what it arranged outside stays"
    );
    Ok(true)
}

/// Cancel the wakes of the removed row `req` names ([`cancel_wakes`]),
/// once a bring-up of its token running here has ended (under the guard
/// [`crate::registry::hold`] takes), so a wake that bring-up set is
/// among those cancelled.
async fn cancel_wakes_of(
    state: &crate::ListenerState,
    req: &weft_core::signal::listener_protocol::UnregisterRequest,
) -> std::result::Result<(), UnregisterError> {
    let guard = state.registry.bring_up_guard(&req.token);
    let cancelled = {
        let _held = guard.lock().await;
        cancel_wakes(state, &req.token, req.generation, &req.spec, &req.kind_state).await
    };
    drop(guard);
    state.registry.release_bring_up_guard(&req.token);
    cancelled.map_err(|e| UnregisterError::Unreadable(e.context(format!("signal {}'s wakes cannot be named, so they are not cancelled", req.token))))
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
    /// Its row could not be read to tell whether a newer registration of
    /// its token runs on another listener; nothing was touched, and
    /// trying again can help.
    RowUnread(anyhow::Error),
}

impl std::fmt::Display for UnregisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnregisterError::Unreadable(e) | UnregisterError::TeardownFailed(e) | UnregisterError::RowUnread(e) => write!(f, "{e:#}"),
            UnregisterError::UnknownKind(kind) => write!(f, "the signal is of the kind '{kind}', which this listener does not know"),
        }
    }
}

/// Stop holding `token`: drop its registry entry (which aborts its task),
/// stop counting it down or given up here, drop the credentials the process
/// keeps for it (`crate::reach`), and have its kind tear down
/// what it holds outside this process (a provider-side subscription),
/// detached and loud in logs on failure. Its wakes are its caller's to
/// cancel ([`unregister`]). Answers the teardown's task, for a caller
/// that reuses the token right after.
///
/// The teardown runs under the token's bring-up guard (the one
/// [`crate::registry::hold`] takes), so a bring-up of the same token that
/// comes after this call waits for it: the teardown is keyed by the
/// token, and run after the new task subscribed it would drop the new
/// subscription. The guard is taken synchronously when free, which is the
/// case for an unregister; when a bring-up of this token holds it (a row
/// found gone while it came up), the teardown queues behind that one.
///
/// Only the registrations `which` names are stopped: a newer one that
/// came up under the token since stays, with what it arranged outside.
pub fn forget(state: &crate::ListenerState, token: &str, which: Registrations) -> tokio::task::JoinHandle<()> {
    state.registry.clear_down(token, which);
    state.registry.drop_unrecorded(token, which);
    state.reach.forget(token, which);
    let teardown = stop_held(state, token, which);
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

/// Stop what runs under `token` when it is one of `which`: its entry
/// leaves the registry at once, and the returned future aborts its task
/// and has its kind tear down what it holds outside this process
/// ([`on_unregister`]). Every path that ends or replaces a held entry goes
/// through here, so no displaced entry skips its teardown and leaves, say,
/// a provider subscription posting to it.
pub fn stop_held(state: &crate::ListenerState, token: &str, which: Registrations) -> impl std::future::Future<Output = ()> + Send + 'static {
    // Our handle to the task goes first, so the loop is not left serving
    // while its teardown talks to the provider.
    let displaced = remove_here(state, token, which);
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
/// stops, it is no longer counted down, and nothing is kept for it in
/// `crate::reach`; what it arranged outside stays
/// (a provider subscription is keyed by the token, so tearing it down
/// would undo what the next holder arranged).
pub fn stop_here(state: &crate::ListenerState, token: &str) {
    // Whatever runs under the token is this holder's claim, whichever
    // registration it is.
    stop_here_only(state, token, Registrations::All);
}

/// [`stop_here`] for the registrations `which` names: a newer one that
/// came up here since stays.
fn stop_here_only(state: &crate::ListenerState, token: &str, which: Registrations) {
    state.registry.clear_down(token, which);
    state.registry.drop_unrecorded(token, which);
    state.reach.forget(token, which);
    remove_here(state, token, which);
}

/// Take `token`'s entry out of the registry when it is one of `which`, and
/// drop its handle to the task, which stops the task once no reader still
/// holds a copy.
fn remove_here(state: &crate::ListenerState, token: &str, which: Registrations) -> Option<RegisteredSignal> {
    let mut sig = state.registry.remove(token, which)?;
    drop(sig.task.take());
    Some(sig)
}

/// The context a kind's work on one signal (its registration
/// `generation`) runs with.
#[allow(clippy::too_many_arguments)]
fn spawn_ctx(
    state: &crate::ListenerState,
    token: &str,
    generation: i64,
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
            generation,
            tenant_id.to_string(),
            for_instance,
            spec.match_predicates.clone(),
        ),
        config: state.config.clone(),
        events_broker: state.events_broker.clone(),
        fresh,
        serving,
        said: state.holding.changed.clone(),
        reach: state.reach.clone(),
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
    /// The registration it was set for (`signal.generation`): a wake for
    /// one the row no longer is does nothing. Absent on a wake set before
    /// registrations were numbered, which acts on the row as it stands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// How many times a receiver this moment's wake arrived early at set
    /// it again: each is a booking of its own, named apart, since the task
    /// being answered still holds the name of the one before.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub set_again: u32,
    /// The row itself, as the last wake left it, for a kind that acts on
    /// it alone ([`KindHandler::wakes_from_body`]) when it fits the
    /// alarm ([`weft_platform_traits::Alarm::largest_body`]): such a wake
    /// reads nothing, and writes only what changed for good.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<weft_broker_client::protocol::SignalRowWire>,
    /// Set by [`cancel_wakes`] under the name of a wake it cancelled, so a
    /// wake of the same registration being handled right now finds the name
    /// taken when it sets its next one there, and its chain ends. It does
    /// nothing when it goes off.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ended: bool,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

/// One of a signal's wakes, as the alarm knows it: named after the
/// signal, the moment, the registration and how many times it was set
/// again before its moment (`WakeBody::set_again`). A registration's
/// moments only move forward, so no two of its bookings share a name, and
/// a wake of another registration of the token is another wake.
fn signal_wake(token: &str, generation: i64, set_again: u32, at_unix_ms: i64, body: Value) -> weft_platform_traits::Wake {
    weft_platform_traits::Wake {
        key: weft_platform_traits::alarm::again_key(&format!("signal:{token}"), set_again),
        at_unix_ms,
        generation,
        queue: weft_platform_traits::WakeQueue::Triggers,
        role: weft_platform_traits::CoreRole::Listener,
        path: WAKE_PATH.to_string(),
        body,
    }
}

/// Set a `Wakes` signal's next wake from `row` (its state as it stands
/// now, as of `now_ms`), when it has one. The wake carries the row for a
/// kind that acts on it alone, when `row_is_written` (the row says what
/// `row` says) and it fits the alarm; otherwise it reads the row.
pub async fn arm_next_wake(
    state: &crate::ListenerState,
    handler: &'static dyn KindHandler,
    spec: &SignalSpec,
    row: &weft_broker_client::protocol::SignalRowWire,
    from: WakeFrom,
    now_ms: i64,
    row_is_written: bool,
) -> Result<()> {
    let Some(at) = handler.next_wake(spec, &row.kind_state, from, now_ms)? else {
        return Ok(());
    };
    let mut body = WakeBody {
        token: row.token.clone(),
        due_at_ms: at,
        generation: Some(row.generation),
        set_again: 0,
        row: None,
        ended: false,
    };
    if handler.wakes_from_body() && row_is_written {
        let carried = WakeBody { row: Some(row.clone()), ..body.clone() };
        if serde_json::to_vec(&carried)?.len() <= state.alarm.largest_body() {
            body = carried;
        }
    }
    state.alarm.set(signal_wake(&row.token, row.generation, 0, at, serde_json::to_value(body)?)).await
}

/// End the wakes of registration `generation` of a `Wakes` signal, from
/// its spec and its state: the moment its state names next and the one
/// after it, which are the moments a wake of it being handled right now
/// sets next ([`wake`] reads its row before setting any later one). Each
/// is cancelled, then set again as an ended wake ([`WakeBody::ended`]), so
/// such a wake, which may set its next one after this, finds the name
/// taken and its chain stops there. A hop of a far wake is cancelled with
/// it. A wake a receiver set again before its moment
/// (`WakeBody::set_again`) is not named here; it carries no row, so at its
/// moment it reads the row, finds the signal gone or registered again,
/// and does nothing. Ending a wake that fails is tried once more, then
/// logged with the wake it left.
pub async fn cancel_wakes(state: &crate::ListenerState, token: &str, generation: i64, spec: &SignalSpec, kind_state: &Value) -> Result<()> {
    let handler = handler_or_err(&spec.kind)?;
    if handler.between_fires(spec, kind_state)? != BetweenFires::Wakes {
        return Ok(());
    }
    let mut moments = Vec::new();
    if let Some(next) = handler.next_wake(spec, kind_state, WakeFrom::Armed, now_unix_ms())? {
        moments.push(next);
        moments.extend(handler.next_wake(spec, kind_state, WakeFrom::Woken { aimed_at_ms: next }, next)?);
    }
    moments.dedup();
    for at in moments {
        let ended = WakeBody {
            token: token.to_string(),
            due_at_ms: at,
            generation: Some(generation),
            set_again: 0,
            row: None,
            ended: true,
        };
        let wake = signal_wake(token, generation, 0, at, serde_json::to_value(ended)?);
        if end_wake(state, &wake).await.is_ok() {
            continue;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if let Err(e) = end_wake(state, &wake).await {
            tracing::error!(
                target: "weft_listener::kinds", %token, wake = %wake.name(), error = %format!("{e:#}"),
                "a wake of a signal that is gone or replaced could not be ended; one left set at this moment goes off once more, \
                 and keeps going only if it carries its row and no moment after it was ended"
            );
        }
    }
    Ok(())
}

/// Cancel `wake`, then set it again as the ended wake it carries.
async fn end_wake(state: &crate::ListenerState, wake: &weft_platform_traits::Wake) -> Result<()> {
    state.alarm.cancel(wake).await?;
    state.alarm.set(wake.clone()).await
}

/// A wake's failure that delivering the wake again repeats: a spec, a
/// state or a schedule no listener reads, or the broker's answer about the
/// signal's connection that asking again repeats (gone, refused, needing a
/// reconnect: [`crate::listener_access::answered_for_good`]). A kind wraps such a
/// failure with [`for_good`]; [`wake`] then gives the signal up on it at
/// once. Every other failure of a wake is tried again, within the
/// deliveries the alarm gives it ([`wake_delivered`]).
#[derive(Debug)]
pub struct ForGood(anyhow::Error);

impl std::fmt::Display for ForGood {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl std::error::Error for ForGood {}

/// `e`, marked as a wake's failure that delivering it again repeats
/// ([`ForGood`]).
pub fn for_good(e: anyhow::Error) -> anyhow::Error {
    ForGood(e).into()
}

fn is_for_good(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| cause.is::<ForGood>())
}

/// Whether asking again repeats a failure: one marked [`ForGood`], or the
/// broker's answer about the signal's connection that asking again
/// repeats ([`crate::listener_access::answered_for_good`]). What a held
/// signal's loop ends on, as a wake gives its signal up on it.
pub fn repeats(e: &anyhow::Error) -> bool {
    is_for_good(e) || crate::listener_access::answered_for_good(e)
}

/// What a held signal's own loop does with a failed attempt (`what`
/// names it): one asking again repeats ([`repeats`]) stops the signal
/// with why, said on its node and written on its row until it is
/// registered again, and ends the loop (`Break`); any other is said on
/// its node and waited out on the ladder (`Continue`).
pub async fn held_attempt_failed(
    what: &str,
    e: anyhow::Error,
    said: &mut event_source::Said,
    backoff: &mut event_source::Backoff,
    fire: &FireContext,
) -> std::ops::ControlFlow<()> {
    if repeats(&e) {
        let why = format!("{what}: {e:#}");
        said.stopped(&why);
        fire.give_up(&why).await;
        return std::ops::ControlFlow::Break(());
    }
    said.down(format!("{what}: {e:#}"));
    backoff.wait_then_climb().await;
    std::ops::ControlFlow::Continue(())
}

/// Handle one delivery of a wake ([`wake`]) within the deliveries the
/// alarm gives it. A failure on the last of them gives the signal up,
/// once, with why, shown on its node, and answers the wake as handled
/// (`Ok(Some(why))`): it stays stopped until it is registered again,
/// which turning its trigger off and on does, as does a resync moving it
/// onto a new build, and, for one that signs in through a connection,
/// reconnecting that connection. The why
/// tells a wake that failed to act from one that acted and could not set
/// the wake after it ([`NextWakeNotSet`]). A failure on an earlier one is
/// `Err`, which the alarm delivers again.
pub async fn wake_delivered(
    state: &crate::ListenerState,
    body: WakeBody,
    delivery: weft_platform_traits::Delivery,
) -> Result<Option<String>> {
    let (token, generation) = (body.token.clone(), body.generation);
    // Any delivery after another may follow a wake that acted, wrote the
    // row and then failed (setting the next wake, or answering at all: a
    // listener that died mid-wake answered nothing), so a row it carries
    // may be behind the one written: acting on it would lose its claim and
    // set no next wake. It reads the row instead.
    let body = if delivery.redelivered { WakeBody { row: None, ..body } } else { body };
    let e = match wake(state, body).await {
        Err(e) if delivery.is_last() => e,
        handled => return handled,
    };
    // The bound the platform gives, never this delivery's count: a give-up
    // that failed to be written (the broker down) answers a failure, and
    // the wake comes back past it.
    let why = match e.downcast_ref::<NextWakeNotSet>() {
        Some(_) => format!(
            "its wake was handled, but the wake after it could not be set on the last of the {} times the listener handled it, \
             so nothing wakes it again: {e:#}",
            delivery.attempts
        ),
        None => format!("its wake failed each of the {} times the listener handled it, the last with: {e:#}", delivery.attempts),
    };
    let generation = match generation {
        Some(generation) => generation,
        // A wake set before registrations were numbered gives up the row
        // as it stands.
        None => match state.signals.get_held(&token).await? {
            Some(row) => row.generation,
            None => return Ok(None),
        },
    };
    give_up(state, &token, generation, &why).await?;
    Ok(Some(why))
}

/// The context a wake's failure carries when its kind acted and what
/// failed after was setting the wake after it. The alarm delivers it
/// again like any failure; given up, it says so ([`wake_delivered`]).
#[derive(Debug)]
pub struct NextWakeNotSet;

impl std::fmt::Display for NextWakeNotSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("setting the next wake")
    }
}

/// Handle one wake: let its kind act on the row (carried in the wake, or
/// read now), set the next wake. A wake whose signal is gone, was
/// registered again since, was given up, or no longer wakes (registered
/// again to be served another way) does nothing. One that arrived before
/// its moment is set again for that moment.
///
/// `Ok(Some(why))` is a wake dropped for good: the row can never be acted
/// on (a spec, a kind or a state no listener can read, or a failure its
/// kind marks [`ForGood`]), so the signal is given up on, once, shown on
/// its node, and the wake answered as handled. `Err` is a failure that
/// may pass (the broker or the provider not answering now, the fire's
/// delivery), which the alarm delivers again, as many times as it gives
/// a wake ([`wake_delivered`]).
pub async fn wake(state: &crate::ListenerState, body: WakeBody) -> Result<Option<String>> {
    if body.ended {
        tracing::debug!(target: "weft_listener::kinds", token = %body.token, "an ended wake went off; nothing to do");
        return Ok(None);
    }
    let aimed_at_ms = body.due_at_ms;
    // Set again, it is one that reads its row at its moment: a cancel
    // names only the wakes the row's state names.
    let again = || {
        let again = WakeBody { set_again: body.set_again + 1, row: None, ..body.clone() };
        Ok(signal_wake(&again.token, again.generation.unwrap_or(0), again.set_again, aimed_at_ms, serde_json::to_value(&again)?))
    };
    if let Some(set) = weft_platform_traits::alarm::set_again_if_early(&*state.alarm, aimed_at_ms, again).await {
        set?;
        return Ok(None);
    }
    // A wake inside the tolerance counts as arriving at its moment, so the
    // next moment its kind picks is after this one.
    let now_ms = now_unix_ms().max(aimed_at_ms);
    let row = match body.row {
        Some(row) => row,
        None => match state.signals.get_held(&body.token).await? {
            Some(row) if body.generation.is_none_or(|g| g == row.generation) && row.held_failed.is_none() => row,
            _ => {
                tracing::debug!(target: "weft_listener::kinds", token = %body.token, "a wake for a signal gone, registered again or given up; nothing to do");
                state.reach.forget(&body.token, body.generation.map_or(Registrations::All, Registrations::UpTo));
                return Ok(None);
            }
        },
    };
    let readable = || -> Result<(SignalSpec, &'static dyn KindHandler, BetweenFires)> {
        let spec: SignalSpec = serde_json::from_str(&row.spec_json).map_err(|e| anyhow::anyhow!("malformed spec_json: {e}"))?;
        let handler = handler_or_err(&spec.kind)?;
        let between = handler.between_fires(&spec, &row.kind_state)?;
        if between == BetweenFires::Wakes {
            handler.next_wake(&spec, &row.kind_state, WakeFrom::Woken { aimed_at_ms }, now_ms)?;
        }
        Ok((spec, handler, between))
    };
    let (spec, handler) = match readable() {
        Ok((spec, handler, BetweenFires::Wakes)) => (spec, handler),
        Ok(_) => {
            tracing::info!(target: "weft_listener::kinds", token = %row.token, "a wake for a signal that no longer wakes (its row was registered again); nothing to do");
            return Ok(None);
        }
        Err(e) => return never_handled(state, &row, e).await,
    };
    // A `Wakes` kind keeps nothing in this process between calls (see
    // `bring_up`), so its serving slot lives for this one wake.
    let serving = Arc::new(Mutex::new(ServingState::default()));
    let ctx = spawn_ctx(state, &row.token, row.generation, &row.tenant_id, row.for_instance.clone(), &spec, false, serving);
    let woken = Woken { aimed_at_ms, now_ms, stored: RowState { state: row.kind_state.clone(), seq: row.kind_state_seq }, is_resume: row.is_resume };
    let after = match handler.on_wake(&spec, woken, ctx).await {
        Ok(Some(after)) => after,
        Ok(None) => return Ok(None),
        Err(e) if is_for_good(&e) => return never_handled(state, &row, e).await,
        Err(e) => return Err(e),
    };
    let written = after.seq.is_some();
    let row = weft_broker_client::protocol::SignalRowWire { kind_state: after.state, kind_state_seq: after.seq.unwrap_or(row.kind_state_seq), ..row };
    set_next_after_acting(state, handler, &spec, &row, aimed_at_ms, written).await.map_err(|e| e.context(NextWakeNotSet))?;
    Ok(None)
}

/// Set the wake after the one aimed at `aimed_at_ms`, whose kind acted and
/// left `row` (`written`: the row says so too).
async fn set_next_after_acting(
    state: &crate::ListenerState,
    handler: &'static dyn KindHandler,
    spec: &SignalSpec,
    row: &weft_broker_client::protocol::SignalRowWire,
    aimed_at_ms: i64,
    written: bool,
) -> Result<()> {
    let done_at_ms = now_unix_ms().max(aimed_at_ms);
    if handler.wakes_from_body() && !on_time_or_still_registered(state, handler, spec, row, aimed_at_ms, done_at_ms).await? {
        tracing::info!(target: "weft_listener::kinds", token = %row.token, "a wake whose signal went or was registered again while it was handled; it sets no next one");
        state.reach.forget(&row.token, Registrations::UpTo(row.generation));
        return Ok(());
    }
    arm_next_wake(state, handler, spec, row, WakeFrom::Woken { aimed_at_ms }, done_at_ms, written).await
}

/// Whether a wake of a kind that acts on its carried row
/// ([`KindHandler::wakes_from_body`]) may set its next one from `row`
/// without reading the row. Such a wake touches no row, so it learns
/// nothing of its registration being removed or registered again while it
/// was handled; that removal ends the moment named next and the one after
/// it ([`cancel_wakes`]), so setting either finds it taken. A wake handled
/// past the first moment after its own (`aimed_at_ms`), or delivered that
/// late, sets a later one, which nothing ended: it reads the row first,
/// and goes on only while the row is still its registration.
async fn on_time_or_still_registered(
    state: &crate::ListenerState,
    handler: &'static dyn KindHandler,
    spec: &SignalSpec,
    row: &weft_broker_client::protocol::SignalRowWire,
    aimed_at_ms: i64,
    done_at_ms: i64,
) -> Result<bool> {
    let from = WakeFrom::Woken { aimed_at_ms };
    if handler.next_wake(spec, &row.kind_state, from, done_at_ms)? == handler.next_wake(spec, &row.kind_state, from, aimed_at_ms)? {
        return Ok(true);
    }
    Ok(state.signals.get_held(&row.token).await?.is_some_and(|now| now.generation == row.generation && now.held_failed.is_none()))
}

/// The wake of `row` can never be handled, for `e`: its signal is given up
/// on, and the wake answered as handled with why.
async fn never_handled(state: &crate::ListenerState, row: &weft_broker_client::protocol::SignalRowWire, e: anyhow::Error) -> Result<Option<String>> {
    let why = format!("its wake can never be handled: {e:#}");
    give_up(state, &row.token, row.generation, &why).await?;
    Ok(Some(why))
}

/// Give up on registration `generation` of signal `token` for good, with
/// why: written once on its row, which its node then shows, and stopped
/// here. A registration again clears it.
pub(crate) async fn give_up(state: &crate::ListenerState, token: &str, generation: i64, reason: &str) -> Result<()> {
    tracing::error!(target: "weft_listener::kinds", %token, %reason, "giving up on a signal until it is registered again");
    state.fire_sink.give_up(token, generation, reason).await
}

fn now_unix_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Process one stateless fire. Resume signals route to the
/// suspended run regardless of kind; entry signals delegate to
/// the kind's `process_entry`. A signal with no held row drops the fire,
/// saying why.
pub async fn process(state: &crate::ListenerState, token: &str, payload: Value) -> Result<ProcessOutcome> {
    process_in(&state.registry, &state.signals, token, payload).await
}

/// [`process`], from the registry and the signal rows themselves.
pub async fn process_in(
    registry: &crate::registry::Registry,
    signals: &weft_broker_client::BrokerSignalClient,
    token: &str,
    payload: Value,
) -> Result<ProcessOutcome> {
    let Some(signal) = crate::registry::held_in(registry, signals, token).await? else {
        return Ok(ProcessOutcome {
            value: payload,
            target: ProcessTarget::Drop { reason: Some("no signal is held under this token".into()) },
        });
    };

    if signal.is_resume {
        // The signal table holds no wait without its run (a check on the
        // row), so a held one without it is a broken invariant.
        let run_id = signal.run_id.ok_or_else(|| anyhow::anyhow!("the wait {token} names no run"))?;
        return Ok(ProcessOutcome {
            value: payload,
            target: ProcessTarget::Resume { run_id },
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
/// The raw last answer `sig` saw, by its kind's rule
/// ([`KindHandler::last_answer`]); `None` for a kind this listener does
/// not know, whose display says so.
pub fn last_answer(sig: &RegisteredSignal) -> Option<weft_core::signal::listener_protocol::LastAnswer> {
    lookup(&sig.spec.kind).and_then(|handler| handler.last_answer(sig))
}

/// Send the last call of a wait whose run was cancelled
/// (`weft_core::signal::cancel_call`), once, as the wait's connection, to
/// the address it names.
/// The wait's row is gone: everything comes with the request, and nothing
/// fetched for it is kept past the call. An error says why the call did
/// not go through, a refusal naming the status and what the service said.
pub async fn cancel_call(state: &crate::ListenerState, req: weft_core::signal::listener_protocol::CancelCallRequest) -> Result<()> {
    let call = req.spec.on_cancel.clone().ok_or_else(|| anyhow::anyhow!("the wait {} carries no last call", req.token))?;
    call.validate().map_err(|e| anyhow::anyhow!(e))?;
    let ctx = spawn_ctx(state, &req.token, crate::reach::UNREGISTERED, &req.tenant_id, req.for_instance.clone(), &req.spec, false, Arc::default());
    // Nothing it fetched is kept (`UNREGISTERED`), so there is nothing to
    // forget after it: dropping what is kept under the token would only
    // drop a newer registration's.
    send_cancel_call(&call, &req.spec.access, &ctx).await
}

/// How long a last call may take before it counts as not having gone
/// through.
const CANCEL_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

async fn send_cancel_call(
    call: &weft_core::signal::CancelCall,
    access: &Option<weft_core::primitive::AccessRef>,
    ctx: &SpawnCtx,
) -> Result<()> {
    use anyhow::Context;
    use weft_core::signal::CancelMethod;
    let (client, _) = crate::listener_access::client_for(access, ctx).await?;
    let url = call.url.as_str();
    let request = match call.method {
        CancelMethod::Post => client.post(url),
        CancelMethod::Put => client.put(url),
        CancelMethod::Patch => client.patch(url),
        CancelMethod::Delete => client.delete(url),
    };
    let request = match &call.body {
        Some(body) => request.json(body),
        None => request,
    };
    // Bounded, unlike a poll: a cancel is one short call of weft's own, and
    // one that never answers would hold its task, and the dispatcher
    // sending it, for good.
    let request = request.timeout(CANCEL_CALL_TIMEOUT);
    let what = format!("{} {}", call.method.as_http(), call.url);
    let resp = request.send().await.with_context(|| format!("the last call {what} did not reach the service"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_else(|e| format!("<body read failed: {e}>"));
        anyhow::bail!("the service refused the last call {what}: {status}: {body}");
    }
    Ok(())
}

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
    serving_sink(ctx)(status.into());
}

/// Where an engine that holds a connection reports what it is doing.
/// The usual sink is the one registration's slot (`serving_sink`); an
/// engine that serves several registrations at once (a provider socket
/// shared by every subscription of one topic) fans one report out to
/// every slot it serves, so no panel says "holding" while the socket
/// is down.
pub type ServingReport = Arc<dyn Fn(String) + Send + Sync>;

/// The report that writes to this registration's own slot, and says it
/// changed.
pub fn serving_sink(ctx: &SpawnCtx) -> ServingReport {
    let (serving, said) = (ctx.serving.clone(), ctx.said.clone());
    Arc::new(move |status| {
        serving.lock().status = status;
        said.notify_one();
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

    /// A door that refuses takes a poll's cursor and a folder watch's
    /// snapshot (each then primes silently on its first look), and leaves a
    /// timer's schedule alone.
    #[test]
    fn a_place_in_a_source_goes_and_a_schedule_stays() {
        use weft_core::signal::{to_spec, PollEndpoint, Timer, TimerSpec};
        let poll: PollEndpoint = serde_json::from_value(serde_json::json!({
            "url": "https://feed.test/items", "interval_secs": 60, "delta": { "items": "items", "cursor_field": "id" }
        }))
        .unwrap();
        assert_eq!(fresh_place(&to_spec(poll), 1_000).unwrap(), Some(serde_json::json!({})), "a poll starts unprimed");
        let watch: weft_core::signal::FolderWatch = serde_json::from_value(serde_json::json!({ "folder": "/srv/inbox" })).unwrap();
        assert_eq!(fresh_place(&to_spec(watch), 1_000).unwrap(), Some(serde_json::json!({})), "a folder watch has seen nothing");
        let timer = to_spec(Timer { spec: TimerSpec::After { duration_ms: 60_000 } });
        assert_eq!(fresh_place(&timer, 1_000).unwrap(), None, "a timer has no place in a source");
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
            generation: 1,
            spec: SignalSpec {
                kind: "timer".into(),
                config: Value::Object(Default::default()),
                consumer_kind: None,
                access: None,
                match_predicates: Vec::new(),
                limits: Default::default(),
                settings: Default::default(),
                on_cancel: None,
            },
            node_id: "ask".into(),
            tenant_id: "t".into(),
            is_resume: false,
            run_id: None,
            task: None,
            kind_state: None,
            routing: SignalRouting { surface, auth, auth_config },
            serving: Arc::new(Mutex::new(ServingState::default())),
            gave_up: None,
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
            generation: 1,
            spec: SignalSpec {
                kind: kind.to_string(),
                config,
                consumer_kind: None,
                access: None,
                match_predicates: predicates,
                limits: Default::default(),
                settings: Default::default(),
                on_cancel: None,
            },
            node_id: "n1".into(),
            tenant_id: "t1".into(),
            is_resume: false,
            run_id: None,
            task: None,
            kind_state: None,
            routing: SignalRouting {
                surface: weft_core::primitive::SignalSurface::Internal,
                auth: weft_core::primitive::SignalAuth::None,
                auth_config: Value::Null,
            },
            serving: Arc::new(Mutex::new(ServingState::default())),
            gave_up: None,
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
