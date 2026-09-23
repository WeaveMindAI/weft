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
use weft_core::live::{LiveFeed, LiveItem};
use weft_core::primitive::{SignalRouting, SignalSpec};

use parking_lot::Mutex;

use crate::config::ListenerConfig;
use crate::event_context::FireContext;
use crate::fire_sink::FireSignalSink;
use weft_core::signal::listener_protocol::{MatchedPush, ProcessOutcome, ProcessTarget, PushEvent};
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
    ///
    /// `asked_at_unix_ms` is when the registration was asked for (the
    /// node's `await_signal`, or the activation), for a kind whose state
    /// counts from that moment.
    fn compute_initial_state(&self, _spec: &SignalSpec, _prior: Option<&Value>, _asked_at_unix_ms: i64) -> Result<Value> {
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
        /// When the registration was asked for (ms since the epoch).
        asked_at_unix_ms: i64,
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
        RoutingSource::Fresh { prior_kind_state, prior_seq, asked_at_unix_ms } => {
            let r = handler.compute_routing(&spec)?;
            let s = handler.compute_initial_state(&spec, prior_kind_state.as_ref(), asked_at_unix_ms)?;
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

/// Render what a consumer needs to answer a registered signal (see
/// [`KindHandler::render`]); `compute_live` is the other question,
/// what the node shows.
pub fn render(token: &str, registry: Arc<Registry>) -> Result<Option<Value>> {
    let signal = registry
        .get(token)
        .ok_or_else(|| anyhow::anyhow!("unknown token: {token}"))?;
    let handler = handler_or_err(&signal.spec.kind)?;
    handler.render(token, &signal)
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
            },
            node_id: "ask".into(),
            tenant_id: "t".into(),
            is_resume: false,
            color: None,
            placement_generation: 0,
            task: None,
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
