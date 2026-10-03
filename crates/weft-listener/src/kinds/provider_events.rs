//! The transport-neutral event subscription, served. A registered
//! `provider_events` signal names a connection, a topic and a filter;
//! this handler decides which transport serves it and runs it:
//!
//!   - **dial-in**: the service pushes to the install's public events
//!     surface. This side's job is only the provider subscription
//!     (subscribe, renew before expiry, unsubscribe at unregister),
//!     driven through the broker on the signal's wakes; the fires arrive
//!     through the receiver. Nothing stays up for it, so a cloud install,
//!     where a held connection costs money, chooses it whenever the service
//!     offers it for a subscription of one account.
//!   - **dial-out**: the service declares a socket recipe and the
//!     connection holds every value its mint call needs. A holder keeps
//!     ONE shared socket per (connection, topic), fanning every inbound
//!     event to all of that pair's subscriptions; the engine re-resolves
//!     the connection and re-mints the address on every reconnect.
//!
//! The registration settles the transport ([`KindHandler::settle`]) and
//! records it on the row, so activating a trigger that cannot be served
//! fails loudly right there, naming what is missing, and the row says
//! where the signal runs: dial-in wakes, dial-out is held. A fresh
//! dial-in subscribes at once, so a provider refusal is that
//! activation's error. A row registered before its transport was
//! settled is held, and its holder decides on the way up, retrying
//! resolution until it can.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use futures_util::FutureExt;
use serde_json::Value;
use tokio::task::JoinHandle;
use weft_broker_client::protocol::{
    SubscriptionDropRequest, SubscriptionEnsureRequest, SubscriptionEnsureResponse,
};
use weft_core::access::events::EventsSpec;
use weft_core::access::spec::lookup_path;
use weft_core::primitive::{AccessRef, SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{EventScope, ProviderEvents, Signal};

use crate::event_context::FireContext;
use weft_core::signal::listener_protocol::{ProcessOutcome, ProcessTarget, PushEvent};
use crate::registry::{RegisteredSignal, ServingState, TaskGuard, Transport};
use crate::socket_engine::{self, CyclePlan, PrepareError};

use super::{BetweenFires, KindHandler, LiveCtx, SpawnCtx, WakeFrom, Woken};
use weft_core::live::{LiveFeed, LiveItem};

pub struct ProviderEventsHandler;

#[async_trait]
impl KindHandler for ProviderEventsHandler {
    fn tag(&self) -> &'static str {
        ProviderEvents::TAG
    }

    fn between_fires(&self, _spec: &SignalSpec, kind_state: &Value) -> Result<BetweenFires> {
        Ok(match Settled::read(kind_state)?.transport {
            Some(SettledTransport::Webhook) => BetweenFires::Wakes,
            Some(SettledTransport::Socket) | None => BetweenFires::Holds,
        })
    }

    /// Resolve the connection and decide the transport, refusing a
    /// subscription no transport can serve.
    async fn settle(&self, spec: &SignalSpec, kind_state: Value, ctx: &SpawnCtx) -> Result<Value> {
        let (cfg, access) = parse_spec(spec)?;
        let decided = resolve_and_decide(&cfg, &access, None, ctx).await?;
        let settled = match decided.transport {
            Transport::Socket => Settled { transport: Some(SettledTransport::Socket), ..Settled::default() },
            Transport::Webhook => Settled {
                transport: Some(SettledTransport::Webhook),
                renew_margin_secs: renew_margin_secs(&decided.topic),
                ..Settled::default()
            },
            Transport::Unservable(reason) => {
                anyhow::bail!("this trigger cannot be served on the '{}' connection: {reason}", access.service)
            }
        };
        settled.into_state(kind_state)
    }

    fn wakes_at_once_when_fresh(&self) -> bool {
        // The first subscribe: a provider refusal fails the activation.
        true
    }

    /// A dial-in subscription wakes to subscribe as it comes up (at the
    /// next whole second, so every arming within one second sets one
    /// wake), then before each expiry to renew.
    fn next_wake(&self, _spec: &SignalSpec, state: &Value, from: WakeFrom, now_ms: i64) -> Result<Option<i64>> {
        let settled = Settled::read(state)?;
        if settled.transport != Some(SettledTransport::Webhook) {
            return Ok(None);
        }
        Ok(match from {
            WakeFrom::Armed => Some(next_whole_second(now_ms)),
            WakeFrom::Woken { .. } => settled.renew_at_ms,
        })
    }

    /// Subscribe (or renew) through the broker, then claim the moment,
    /// recording when it was subscribed and when to renew.
    ///
    /// The ensure comes before the claim, and it is idempotent with the
    /// provider: it makes sure one live subscription serves this token,
    /// subscribing only when none does and renewing only inside the renewal
    /// margin. So two copies woken for one moment both ensure and still end
    /// on one subscription, and a copy that dies between the ensure and the
    /// claim leaves the row unclaimed for the alarm's retry, which ensures
    /// again. The claim has one winner, which records the subscription and
    /// sets the next wake; a copy whose claim lost ends quietly.
    async fn on_wake(&self, spec: &SignalSpec, woken: Woken, ctx: SpawnCtx) -> Result<Option<Value>> {
        let (cfg, access) = parse_spec(spec)?;
        let ensured = ensure_via_broker(&ctx, &access, &cfg).await?;
        let mut settled = Settled::read(&woken.state)?;
        let margin_secs = settled.renew_margin_secs.unwrap_or(0);
        settled.renew_at_ms = ensured.expires_at_unix.map(|at| renew_at_ms(&ctx, &cfg, at, margin_secs, woken.now_ms));
        settled.subscribed = true;
        let after = settled.into_state(woken.state)?;
        if !ctx.fire.claim_kind_state(after.clone(), woken.seq).await? {
            tracing::debug!(
                target: "weft_listener::provider_events",
                token = %ctx.fire.token(), topic = %cfg.topic,
                "another copy claimed this subscription's moment first; its write and its next wake stand"
            );
            return Ok(None);
        }
        Ok(Some(after))
    }

    fn broad_push_routed(&self) -> bool {
        // Account-routed pushes match provider_events signals by
        // connection + topic, so a resume must carry a pinning
        // predicate (enforced at registration).
        true
    }

    /// A push arrives for one ACCOUNT and one topic, so this is where a
    /// subscription decides whether it was meant.
    ///
    /// The connection was already matched by the dispatcher (a push
    /// reaches this signal's process only because the signal hangs off one
    /// of the connections the broker named). What is left is this kind's
    /// own vocabulary, and it is the reason this decision cannot live
    /// anywhere else:
    ///
    ///   - the TOPIC. One connection can hold several, and their field
    ///     names overlap, so a mailbox push must not wake a file watch.
    ///   - the SCOPE. An `app` subscription means "every install of my
    ///     app", which a push aimed at one account can never be: serving
    ///     it here would half-serve it, so those are left to the
    ///     dial-out socket that really does see every install.
    ///
    /// The payload is the named event as it arrived. The event's shape
    /// is the topic's declared fields, which is exactly what the trigger
    /// declared it wakes with.
    fn match_push(&self, sig: &RegisteredSignal, push: &PushEvent) -> Option<Value> {
        let cfg = match parse_config(&sig.spec) {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::warn!(target: "weft_listener::provider_events", error = %format!("{e:#}"), "a held subscription's spec cannot be read; no push feeds it");
                return None;
            }
        };
        if cfg.topic != push.topic {
            return None;
        }
        if cfg.scope == EventScope::App {
            return None;
        }
        Some(push.event.clone())
    }

    fn compute_routing(&self, _spec: &SignalSpec) -> Result<SignalRouting> {
        // Fires arrive internally: from this process's shared socket, or
        // from the public events receiver (which routes by content,
        // not by a mount path).
        Ok(SignalRouting {
            surface: SignalSurface::Internal,
            auth: SignalAuth::None,
            auth_config: Value::Null,
        })
    }

    async fn spawn_task(
        &self,
        spec: &SignalSpec,
        kind_state: &Value,
        ctx: SpawnCtx,
    ) -> Result<Option<JoinHandle<()>>> {
        let (cfg, access) = parse_spec(spec)?;
        set_status(&ctx, "starting");
        let settled = Settled::read(kind_state)?.transport.map(|t| match t {
            SettledTransport::Socket => Transport::Socket,
            SettledTransport::Webhook => Transport::Webhook,
        });
        Ok(Some(serve_subscription(cfg, access, settled, ctx)))
    }

    fn process_entry(&self, _sig: &RegisteredSignal, payload: Value) -> ProcessOutcome {
        ProcessOutcome { value: payload, target: ProcessTarget::Entry }
    }

    /// No register-time snapshot: the trigger's state is LIVE (which
    /// transport serves it, what the task is doing right now). It is
    /// read off the registry entry's serving slot when the node's
    /// display is asked for, never frozen onto the signal row.
    /// Nothing calls in, so there is no address to show: the display
    /// is what this signal is listening to and whether the loop
    /// holding it is healthy right now.
    fn live(&self, ctx: &LiveCtx<'_>) -> LiveFeed {
        let sig = ctx.sig;
        let cfg = match super::config_for_display::<ProviderEvents>(sig, "Topic") {
            Ok(cfg) => cfg,
            Err(feed) => return feed,
        };
        // A topic is a provider's own word ("messages", "files"), never
        // a credential, so it is shown plainly.
        let mut items = vec![LiveItem::text("Topic", cfg.topic)];
        // A dial-in subscription is read off its row, which says whether
        // it is subscribed and when it renews; a held one says what its
        // holder's task is doing.
        match sig.kind_state.as_ref().map(Settled::read) {
            Some(Ok(settled @ Settled { transport: Some(SettledTransport::Webhook), .. })) => {
                items.push(LiveItem::text("State", webhook_state(&settled)))
            }
            Some(Err(e)) => items.push(LiveItem::text("State", format!("{e:#}"))),
            _ => items.extend(super::serving_item(sig)),
        }
        LiveFeed::new(items)
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }

    /// A dial-in subscription holds provider-side state (the provider
    /// keeps posting to a channel weft asked for); drop it at the
    /// provider too. A socket-served signal never subscribed, so there is
    /// nothing to drop. A signal that carries its state here is one served
    /// by wakes, which for this kind is the dial-in one (a held one's task
    /// owns its state and carries none); a held one that fell back to a
    /// webhook says so on its serving slot.
    async fn on_unregister(
        &self,
        token: &str,
        sig: &RegisteredSignal,
        events_broker: &Arc<weft_broker_client::BrokerEventsClient>,
    ) {
        let pushed = sig.serving.lock().transport == Some(Transport::Webhook) || sig.kind_state.is_some();
        if !pushed {
            return;
        }
        if let Err(e) = events_broker
            .subscription_drop(&SubscriptionDropRequest {
                tenant: sig.tenant_id.clone(),
                signal_token: token.to_string(),
            })
            .await
        {
            tracing::warn!(
                target: "weft_listener::provider_events",
                %token, error = %format!("{e:#}"),
                "provider subscription teardown failed at unregister; the provider \
                 channel lapses on its own expiry"
            );
        }
    }
}

inventory::submit!(&ProviderEventsHandler as &dyn KindHandler);

/// Write the signal's live serving status where the kind's `live` reads it.
fn set_status(ctx: &SpawnCtx, status: impl Into<String>) {
    super::set_serving_status(ctx, status);
}

/// Which transport serves a subscription, decided from the recipe,
/// the subscription's SCOPE, what the connection actually holds, and
/// whether this install takes a push over a held connection
/// (`ListenerConfig::prefer_push`). Pure. Dial-in wins there when the
/// service pushes this topic for one account: it keeps nothing up between
/// events. Otherwise dial-out
/// when the connection can run the mint call (it needs no public address
/// and its socket is single-owner by construction); otherwise dial-in;
/// otherwise a reason naming exactly what is missing. An APP-wide
/// subscription is socket-only: the push receiver routes each event to
/// ONE account's connections, so it can never deliver "every install of
/// your app".
pub fn decide_transport(
    topic: &EventsSpec,
    scope: EventScope,
    available: &BTreeMap<String, String>,
    prefer_push: bool,
) -> Transport {
    if prefer_push && scope == EventScope::Account && topic.webhook.is_some() {
        return Transport::Webhook;
    }
    if let Some(socket) = &topic.socket {
        let Some(connect) = &socket.minted.connect else {
            return Transport::Unservable("its socket recipe declares no connect call".into());
        };
        match connect.value_names() {
            Ok(names) => {
                let missing: Vec<&String> =
                    names.iter().filter(|n| !available.contains_key(*n)).collect();
                if missing.is_empty() {
                    return Transport::Socket;
                }
                if scope == EventScope::App {
                    return Transport::Unservable(format!(
                        "an app-wide subscription only works over the service's dial-out \
                         transport, which needs the stored value '{}'; reconnect the \
                         account and provide it",
                        missing[0]
                    ));
                }
                if topic.webhook.is_none() {
                    return Transport::Unservable(format!(
                        "its dial-out transport needs the stored value '{}', which the \
                         connection does not hold; reconnect and provide it",
                        missing[0]
                    ));
                }
                // Fall through to the webhook transport.
            }
            Err(e) => return Transport::Unservable(format!("its socket recipe is invalid: {e}")),
        }
    }
    if scope == EventScope::App {
        return Transport::Unservable(
            "an app-wide subscription only works over a dial-out transport, and this \
             topic declares none"
                .into(),
        );
    }
    if topic.webhook.is_some() {
        return Transport::Webhook;
    }
    Transport::Unservable("the service declares no transport for this topic".into())
}

/// What a `provider_events` spec subscribes to, and through which
/// connection.
fn parse_spec(spec: &SignalSpec) -> Result<(ProviderEvents, AccessRef)> {
    let cfg = parse_config(spec)?;
    let access = spec.access.clone().ok_or_else(|| anyhow::anyhow!("provider_events signal has no connection"))?;
    Ok((cfg, access))
}

/// What the signal subscribes to, without the connection it reads it
/// through (a push already arrived on one).
fn parse_config(spec: &SignalSpec) -> Result<ProviderEvents> {
    serde_json::from_value(spec.config.clone()).map_err(|e| anyhow::anyhow!("malformed provider_events spec: {e}"))
}

/// One topic of a resolved connection: its recipe, every value the
/// connection holds (its stored ones and its recipe's), and the
/// connection's own provider account.
struct ResolvedTopic {
    topic: EventsSpec,
    values: BTreeMap<String, String>,
    provider_account: Option<String>,
}

/// Resolve `access` through the broker and read its `topic`.
async fn resolve_topic(access: &AccessRef, topic: &str, ctx: &SpawnCtx) -> Result<ResolvedTopic> {
    let source = crate::listener_access::resolve(access, ctx).await?;
    let spec = source.events.get(topic).cloned().ok_or_else(|| {
        anyhow::anyhow!(
            "'{}' declares no event topic named '{topic}'; reconnect the account (an older connection may predate the topic)",
            access.service
        )
    })?;
    let mut values = source.values;
    values.extend(source.recipe_values);
    Ok(ResolvedTopic { topic: spec, values, provider_account: source.provider_account })
}

/// The transport decision plus everything it was decided FROM that
/// the serving task needs: the topic recipe and the connection's own
/// provider account.
struct DecidedServing {
    transport: Transport,
    topic: EventsSpec,
    provider_account: Option<String>,
}

/// Resolve the subscription's connection and the transport that serves
/// it: `settled` when the registration recorded one, otherwise decided
/// from what the connection holds ([`decide_transport`]).
async fn resolve_and_decide(
    cfg: &ProviderEvents,
    access: &AccessRef,
    settled: Option<Transport>,
    ctx: &SpawnCtx,
) -> Result<DecidedServing> {
    let resolved = resolve_topic(access, &cfg.topic, ctx).await?;
    let transport =
        settled.unwrap_or_else(|| decide_transport(&resolved.topic, cfg.scope, &resolved.values, ctx.config.prefer_push));
    Ok(DecidedServing { transport, topic: resolved.topic, provider_account: resolved.provider_account })
}

/// The long-running per-signal task: serve the transport until the
/// signal stops (its abort drops the membership guard). It resolves the
/// connection (the topic and its own provider account), retrying on the
/// ladder until it can (one unreachable broker must not keep it down
/// for good), and serves the transport the registration `settled`; a row
/// registered before its transport was settled decides it here. An
/// unservable decision is TERMINAL: retrying would re-derive the same
/// facts, and a reconnect of the account re-registers the signal and
/// re-runs the decision.
fn serve_subscription(
    cfg: ProviderEvents,
    access: AccessRef,
    settled: Option<Transport>,
    ctx: SpawnCtx,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let decided = {
            let mut backoff = crate::kinds::event_source::Backoff::new();
            loop {
                match resolve_and_decide(&cfg, &access, settled.clone(), &ctx).await {
                    Ok(decided) => break decided,
                    Err(e) => {
                        set_status(&ctx, format!("{e:#}"));
                        backoff.wait_then_climb().await;
                    }
                }
            }
        };
        ctx.serving.lock().transport = Some(decided.transport.clone());
        match decided.transport {
            Transport::Socket => {
                set_status(&ctx, "connecting the event socket");
                // The tail of the task: joins the shared socket and
                // parks until aborted at unregister.
                serve_socket(&cfg, &access, decided.provider_account, &ctx).await;
            }
            Transport::Webhook => serve_webhook(&cfg, &access, &decided.topic, &ctx).await,
            Transport::Unservable(reason) => {
                tracing::warn!(
                    target: "weft_listener::provider_events",
                    token = %ctx.fire.token(), %reason,
                    "the subscription cannot be served; ending the serving task (a \
                     reconnect re-registers the signal and re-runs the decision)"
                );
                set_status(&ctx, reason);
            }
        }
    })
}

// ---------- Dial-out: the shared per-(connection, topic) socket ----------

/// One socket per (connection, topic) in this process, fanning events
/// to all of that pair's subscriptions. Keyed by the listener replica
/// too, so several in-process listeners (tests) never cross wires.
static SOCKETS: LazyLock<DashMap<String, SocketShare>> = LazyLock::new(DashMap::new);

/// One subscription on a shared socket: its fire plumbing plus what
/// it may hear. On a multi-install socket (the recipe declares
/// `multi_account`) an account-scoped subscriber only hears frames
/// proven to be its own account's; an app-wide one hears everything.
#[derive(Clone)]
struct Subscriber {
    fire: FireContext,
    scope: EventScope,
    provider_account: Option<String>,
    /// This subscription's own display slot. The shared engine's state
    /// is fanned out to every subscriber's slot, so each panel says
    /// what the socket they all ride on is doing.
    serving: Arc<parking_lot::Mutex<ServingState>>,
}

struct SocketShare {
    subscribers: Arc<DashMap<String, Subscriber>>,
    /// The engine's latest report, so a subscription that joins after
    /// the socket settled (or failed for good) starts from the truth
    /// rather than from "connecting".
    last: Arc<parking_lot::Mutex<String>>,
    /// Holds the shared engine alive: the [`TaskGuard`]'s drop aborts
    /// it when the share dies with its last subscriber. Load-bearing
    /// through its drop alone, which is why the lint sees no read.
    #[allow(dead_code)]
    engine: TaskGuard,
}

/// Membership in a socket share; dropping it (the per-signal task
/// being aborted at unregister) removes the subscription and tears
/// the shared socket down when it was the last one.
struct SubscriberGuard {
    key: String,
    token: String,
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        if let Some(share) = SOCKETS.get(&self.key) {
            share.subscribers.remove(&self.token);
            if share.subscribers.is_empty() {
                drop(share);
                // Remove-if-still-empty: a racing join re-creates the
                // share; losing that race only costs one reconnect.
                SOCKETS.remove_if(&self.key, |_, s| s.subscribers.is_empty());
            }
        }
    }
}

/// Join (creating if absent) the shared socket and park until
/// unregistered.
async fn serve_socket(
    cfg: &ProviderEvents,
    access: &AccessRef,
    provider_account: Option<String>,
    ctx: &SpawnCtx,
) {
    let key = format!("{}|{}|{}", ctx.config.replica, access.id, cfg.topic);
    let _guard = SubscriberGuard { key: key.clone(), token: ctx.fire.token().to_string() };
    {
        let share = SOCKETS.entry(key.clone()).or_insert_with(|| {
            let subscribers: Arc<DashMap<String, Subscriber>> = Arc::new(DashMap::new());
            let last: Arc<parking_lot::Mutex<String>> = Arc::new(parking_lot::Mutex::new(String::new()));
            // One socket serves every subscription of this topic, so
            // what it is doing is every subscription's state: each
            // report lands in every subscriber's slot, and is kept for
            // whoever joins later.
            let report: super::ServingReport = {
                let subscribers = subscribers.clone();
                let last = last.clone();
                Arc::new(move |status: String| {
                    *last.lock() = status.clone();
                    for subscriber in subscribers.iter() {
                        subscriber.serving.lock().status = status.clone();
                    }
                })
            };
            let engine = spawn_shared_engine(
                access.clone(),
                cfg.topic.clone(),
                ctx.clone(),
                subscribers.clone(),
                report,
            );
            SocketShare { subscribers, last, engine: TaskGuard::new(engine) }
        });
        share.subscribers.insert(
            ctx.fire.token().to_string(),
            Subscriber {
                fire: ctx.fire.clone(),
                scope: cfg.scope,
                provider_account,
                serving: ctx.serving.clone(),
            },
        );
        // Start from what the socket is doing NOW: a share that already
        // settled (or gave up for good) has said so, and a join must not
        // paint "connecting" over it.
        let seen = share.last.lock().clone();
        set_status(ctx, if seen.is_empty() { "joining the shared event socket".to_string() } else { seen });
    }
    // Park until aborted; the guard's drop does the cleanup.
    std::future::pending::<()>().await;
}

/// The shared engine: resolve + mint per cycle, unwrap the recipe's
/// envelope, map the named fields, fan to every subscriber (each
/// subscriber's own FireContext evaluates its own filter).
fn spawn_shared_engine(
    access: AccessRef,
    topic_name: String,
    ctx: SpawnCtx,
    subscribers: Arc<DashMap<String, Subscriber>>,
    report: super::ServingReport,
) -> JoinHandle<()> {
    let prepare_ctx = ctx.clone();
    let prepare_access = access.clone();
    let prepare_topic = topic_name.clone();
    // The recipe of the CURRENT cycle, shared with on_event (the
    // engine calls prepare before any event of the cycle arrives).
    let recipe: Arc<parking_lot::Mutex<Option<EventsSpec>>> =
        Arc::new(parking_lot::Mutex::new(None));
    let recipe_for_events = recipe.clone();

    let prepare = Box::new(move || {
        let ctx = prepare_ctx.clone();
        let access = prepare_access.clone();
        let topic_name = prepare_topic.clone();
        let recipe = recipe.clone();
        async move {
            let resolved = resolve_topic(&access, &topic_name, &ctx).await.map_err(PrepareError::Transient)?;
            let socket = resolved.topic.socket.clone().ok_or_else(|| {
                PrepareError::Transient(anyhow::anyhow!(
                    "the '{topic_name}' topic declares no socket recipe"
                ))
            })?;
            let url = socket_engine::mint_socket_url(&socket.minted, &resolved.values).await?;
            *recipe.lock() = Some(resolved.topic);
            Ok(CyclePlan {
                url,
                handshake: None,
                heartbeat: None,
                heartbeat_secs: 30,
                replies: socket.minted.replies.clone(),
            })
        }
        .boxed()
    });

    let on_event = Box::new(move |frame: Value| {
        let subscribers = subscribers.clone();
        let recipe = recipe_for_events.clone();
        let topic_name = topic_name.clone();
        async move {
            let Some(topic) = recipe.lock().clone() else { return };
            let socket = topic.socket.clone();
            let event = match socket.as_ref().map(|s| s.event_path.as_str()) {
                Some("") | None => Some(frame.clone()),
                Some(path) => lookup_path(&frame, path).cloned(),
            };
            // A frame with nothing at the event path is protocol
            // chatter (hello frames, acks), not an event.
            let Some(event) = event else { return };
            let multi_account = socket.as_ref().is_some_and(|s| s.multi_account);
            // Whose account the FRAME concerns, on a socket that
            // carries several installs' events (the app-scoped
            // funnel), read through the topic's account concept.
            let frame_account =
                if multi_account { topic.socket_frame_account(&frame) } else { None };
            let named = topic.named_event(&event, &BTreeMap::new());
            for entry in subscribers.iter() {
                let sub = entry.value();
                if multi_account && sub.scope == EventScope::Account {
                    // An account-scoped subscription on a
                    // multi-install socket fires ONLY on a frame
                    // PROVEN to be its own account's. Another
                    // install's frame is another party's (skipped
                    // quietly: normal routing); an unknown on either
                    // side is a shape mismatch that must surface,
                    // never a delivery.
                    match (frame_account.as_deref(), sub.provider_account.as_deref()) {
                        (Some(of_frame), Some(own)) => {
                            if of_frame != own {
                                continue;
                            }
                        }
                        (of_frame, own) => {
                            tracing::warn!(
                                target: "weft_listener::provider_events",
                                token = %entry.key(), topic = %topic_name,
                                frame_account = of_frame.unwrap_or("<not in the frame>"),
                                subscriber_account = own.unwrap_or("<not on the connection>"),
                                "not delivering a shared-socket frame to an account-scoped \
                                 subscription: the account is unknown on one side"
                            );
                            continue;
                        }
                    }
                }
                // A pushed event has no replay cursor; the delivery
                // outcome is already logged by the fire path.
                let _ = sub.fire.fire(named.clone(), "provider_events").await;
            }
        }
        .boxed()
    });

    // The engine is shared by every subscription of this (connection,
    // topic); `report` fans its state out to all of them (`serve_socket`).
    socket_engine::spawn(prepare, on_event, "provider_events", report)
}

// ---------- Dial-in: the provider subscription lifecycle ----------

/// The least time between two renewals, whatever expiry the provider
/// answers, so the wakes never spin.
const RENEW_FLOOR_MS: i64 = 60_000;

/// How long before an expiry the topic's subscription renews, when its
/// recipe says.
fn renew_margin_secs(topic: &EventsSpec) -> Option<u64> {
    topic.webhook.as_ref().and_then(|w| w.subscribe.as_ref()).and_then(|s| s.renew_margin_secs)
}

/// When a subscription that expires at `expires_at_unix` (unix seconds)
/// renews, in unix ms: half its `margin_secs` before the expiry, and never
/// sooner than [`RENEW_FLOOR_MS`] after `now_ms`. The broker renews
/// anything within the whole margin of its expiry, so aiming at the
/// middle of it leaves room for a wake that arrives early and for the
/// broker's clock running behind this one; aimed at the margin's edge, a
/// wake a moment early renews nothing. `floored` when the floor decided
/// it: the provider answered an expiry too close to renew before.
#[derive(Debug, PartialEq, Eq)]
struct Renewal {
    at_ms: i64,
    floored: bool,
}

fn renewal(expires_at_unix: i64, margin_secs: u64, now_ms: i64) -> Renewal {
    let wanted = expires_at_unix * 1000 - i64::try_from(margin_secs).unwrap_or(i64::MAX / 2_000) * 500;
    let floor = now_ms + RENEW_FLOOR_MS;
    Renewal { at_ms: wanted.max(floor), floored: wanted < floor }
}

/// [`renewal`]'s moment for one subscription, saying so when the floor
/// decided it.
fn renew_at_ms(ctx: &SpawnCtx, cfg: &ProviderEvents, expires_at_unix: i64, margin_secs: u64, now_ms: i64) -> i64 {
    let renewal = renewal(expires_at_unix, margin_secs, now_ms);
    if renewal.floored {
        tracing::warn!(
            target: "weft_listener::provider_events",
            token = %ctx.fire.token(), topic = %cfg.topic, expires_at_unix,
            "the provider answered an expiry too close to renew before it; renewing on the floor"
        );
    }
    renewal.at_ms
}

/// `ms` rounded up to a whole second.
fn next_whole_second(ms: i64) -> i64 {
    (ms + 999).div_euclid(1000) * 1000
}

/// What the registration settled about how a subscription is served,
/// kept in its kind state.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Settled {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transport: Option<SettledTransport>,
    /// For dial-in: how long before an expiry the topic renews.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    renew_margin_secs: Option<u64>,
    /// For dial-in: when the subscription next renews (unix ms), `None`
    /// before the first subscribe and for one that never expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    renew_at_ms: Option<i64>,
    /// For dial-in: whether a subscription was made, which is what tells
    /// one that never expires from one not made yet.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    subscribed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum SettledTransport {
    Socket,
    Webhook,
}

impl Settled {
    /// From a signal's state; a state that says nothing (a row
    /// registered before transports were settled) is an empty one, and
    /// one this kind did not write is an error.
    fn read(state: &Value) -> Result<Self> {
        if state.is_null() {
            return Ok(Self::default());
        }
        serde_json::from_value(state.clone()).map_err(|e| anyhow::anyhow!("this subscription's recorded state cannot be read: {e}"))
    }

    /// Written into `state`, alongside whatever else it holds.
    fn into_state(self, state: Value) -> Result<Value> {
        let mut state = match state {
            Value::Object(map) => map,
            Value::Null => serde_json::Map::new(),
            other => anyhow::bail!("a provider_events state is an object, not {other}"),
        };
        for key in ["transport", "renew_margin_secs", "renew_at_ms", "subscribed"] {
            state.remove(key);
        }
        if let Value::Object(settled) = serde_json::to_value(self)? {
            state.extend(settled);
        }
        Ok(Value::Object(state))
    }
}

/// What a dial-in subscription shows on its node, from its row.
fn webhook_state(settled: &Settled) -> String {
    match (settled.renew_at_ms, settled.subscribed) {
        (Some(at), _) => format!("subscribed, events arrive by push (webhook); renews at {}", unix_ms_text(at)),
        (None, true) => "subscribed, events arrive by push (webhook); the subscription does not expire".to_string(),
        (None, false) => "subscribing (webhook)".to_string(),
    }
}

/// A unix-ms moment as a person reads it.
fn unix_ms_text(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms).map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string()).unwrap_or_else(|| ms.to_string())
}

async fn ensure_via_broker(
    ctx: &SpawnCtx,
    access: &AccessRef,
    cfg: &ProviderEvents,
) -> anyhow::Result<SubscriptionEnsureResponse> {
    ctx.events_broker
        .subscription_ensure(&SubscriptionEnsureRequest {
            tenant: ctx.fire.tenant_id().to_string(),
            service: access.service.clone(),
            topic: cfg.topic.clone(),
            access_id: access.id.clone(),
            signal_token: ctx.fire.token().to_string(),
            for_instance: ctx.fire.for_instance().cloned(),
            params: cfg.params.clone(),
        })
        .await
}

/// Keep the provider subscription alive: ensure, sleep until the
/// renewal margin, re-ensure. A topic that never expires (or needs no
/// subscribing at all) parks forever after the first ensure. Never
/// returns except through the task's abort.
async fn serve_webhook(
    cfg: &ProviderEvents,
    access: &AccessRef,
    topic: &EventsSpec,
    ctx: &SpawnCtx,
) {
    let margin_secs = renew_margin_secs(topic).unwrap_or(0);
    let mut backoff = crate::kinds::event_source::Backoff::new();
    loop {
        match ensure_via_broker(ctx, access, cfg).await {
            Ok(ensured) => {
                set_status(ctx, "subscribed; events arrive via push");
                match ensured.expires_at_unix {
                    None => std::future::pending::<()>().await,
                    Some(at) => {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        let wake_ms = renew_at_ms(ctx, cfg, at, margin_secs, now_ms);
                        // At least the floor ahead of now, so positive.
                        tokio::time::sleep(std::time::Duration::from_millis((wake_ms - now_ms) as u64)).await;
                    }
                }
            }
            Err(e) => {
                // The broker names the fix (no public address, a
                // provider refusal); surface it on the trigger and
                // keep trying: the operator may reinstall with the
                // tunnel, or the provider may recover.
                set_status(ctx, format!("subscription failed: {e:#}"));
                backoff.wait_then_climb().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(json: serde_json::Value) -> EventsSpec {
        serde_json::from_value(json).unwrap()
    }

    fn slack_topic() -> EventsSpec {
        topic(serde_json::json!({
            "fields": { "type": "type" },
            "account": { "value": "team", "path": "team_id" },
            "socket": {
                "connect": {
                    "url": "https://slack.com/api/apps.connections.open",
                    "method": "POST",
                    "auth": [{ "kind": "header", "name": "Authorization",
                               "value": "Bearer {app_token}" }],
                    "captures": [{ "name": "url", "path": "url" }]
                }
            },
            "webhook": { "verify": { "kind": "hmac",
                                     "signature_header": "X-Slack-Signature",
                                     "timestamp_header": "X-Slack-Request-Timestamp",
                                     "concat": "v0:{timestamp}:{body}",
                                     "prefix": "v0=" } }
        }))
    }

    /// Dial-out wins when the connection holds what the mint call
    /// needs; a missing value falls to dial-in when one exists, and
    /// names the exact missing value when none does.
    #[test]
    fn the_transport_follows_the_connection() {
        let both = slack_topic();
        let with_token =
            BTreeMap::from([("app_token".to_string(), "xapp-1".to_string())]);
        assert_eq!(
            decide_transport(&both, EventScope::Account, &with_token, false),
            Transport::Socket
        );
        assert_eq!(
            decide_transport(&both, EventScope::Account, &BTreeMap::new(), false),
            Transport::Webhook
        );
        // An APP-wide subscription never falls to the push transport
        // (it routes per account, which can never mean "every
        // install"); without the dial-out value it refuses, naming it.
        assert_eq!(
            decide_transport(&both, EventScope::App, &with_token, false),
            Transport::Socket
        );
        match decide_transport(&both, EventScope::App, &BTreeMap::new(), false) {
            Transport::Unservable(reason) => {
                assert!(reason.contains("app-wide"), "{reason}");
                assert!(reason.contains("app_token"), "{reason}");
            }
            other => panic!("expected unservable, got {other:?}"),
        }

        let socket_only = topic(serde_json::json!({
            "fields": { "type": "type" },
            "account": { "value": "team", "path": "team_id" },
            "socket": {
                "connect": {
                    "url": "https://x/open", "method": "POST",
                    "auth": [{ "kind": "header", "name": "Authorization",
                               "value": "Bearer {app_token}" }],
                    "captures": [{ "name": "url", "path": "url" }]
                }
            }
        }));
        match decide_transport(&socket_only, EventScope::Account, &BTreeMap::new(), false) {
            Transport::Unservable(reason) => {
                assert!(reason.contains("app_token"), "{reason}")
            }
            other => panic!("expected unservable, got {other:?}"),
        }
    }

    /// On an install that prefers the push, a subscription of one account
    /// that the service pushes rides it, even when the connection could
    /// dial out: nothing has to stay up for it. An app-wide one still dials
    /// out.
    #[test]
    fn an_install_that_prefers_the_push_takes_it() {
        let both = slack_topic();
        let with_token = BTreeMap::from([("app_token".to_string(), "xapp-1".to_string())]);
        assert_eq!(decide_transport(&both, EventScope::Account, &with_token, true), Transport::Webhook);
        assert_eq!(decide_transport(&both, EventScope::App, &with_token, true), Transport::Socket);
    }

    /// A pushed subscription wakes (to subscribe, then to renew) and is
    /// never held; a dialed one, or one whose transport was never settled,
    /// is held.
    #[test]
    fn the_settled_transport_decides_where_a_subscription_runs() {
        let spec: SignalSpec = serde_json::from_value(serde_json::json!({
            "kind": "provider_events", "config": { "topic": "messages" }
        }))
        .unwrap();
        let handler = ProviderEventsHandler;
        let webhook = Settled {
            transport: Some(SettledTransport::Webhook),
            renew_margin_secs: Some(60),
            renew_at_ms: Some(5_000),
            subscribed: true,
        }
        .into_state(serde_json::json!({ "other": 1 }))
        .unwrap();
        assert_eq!(webhook["other"], 1, "the rest of the state stays");
        assert_eq!(handler.between_fires(&spec, &webhook).unwrap(), BetweenFires::Wakes);
        assert_eq!(handler.next_wake(&spec, &webhook, WakeFrom::Armed, 1_000).unwrap(), Some(1_000), "subscribe as it comes up");
        assert_eq!(handler.next_wake(&spec, &webhook, WakeFrom::Woken { aimed_at_ms: 1_000 }, 1_200).unwrap(), Some(5_000));
        let socket = Settled { transport: Some(SettledTransport::Socket), ..Settled::default() }.into_state(Value::Null).unwrap();
        assert_eq!(handler.between_fires(&spec, &socket).unwrap(), BetweenFires::Holds);
        assert_eq!(handler.next_wake(&spec, &socket, WakeFrom::Armed, 1_000).unwrap(), None);
        assert_eq!(handler.between_fires(&spec, &serde_json::json!({})).unwrap(), BetweenFires::Holds, "unsettled: decided on the way up");
        assert!(handler.between_fires(&spec, &serde_json::json!({ "transport": "carrier_pigeon" })).is_err(), "a state it did not write is an error");
    }

    /// Two armings within one second set one wake: the first subscribe is
    /// aimed at the next whole second.
    #[test]
    fn arming_a_subscription_aims_at_the_next_whole_second() {
        let spec: SignalSpec = serde_json::from_value(serde_json::json!({
            "kind": "provider_events", "config": { "topic": "messages" }
        }))
        .unwrap();
        let webhook = Settled { transport: Some(SettledTransport::Webhook), ..Settled::default() }.into_state(Value::Null).unwrap();
        let armed = |now_ms| ProviderEventsHandler.next_wake(&spec, &webhook, WakeFrom::Armed, now_ms).unwrap();
        assert_eq!((armed(1_001), armed(1_999), armed(2_000)), (Some(2_000), Some(2_000), Some(2_000)));
        assert_eq!(next_whole_second(-1), 0);
    }

    /// A subscription renews its margin before the expiry, and never
    /// sooner than the floor after now, saying when the floor decided it.
    #[test]
    fn a_subscription_renews_its_margin_before_expiry_on_a_floor() {
        let now_ms = 1_000_000;
        assert_eq!(renewal(10_000, 600, now_ms), Renewal { at_ms: 9_700_000, floored: false }, "in the middle of the margin");
        assert_eq!(renewal(10_000, 0, now_ms), Renewal { at_ms: 10_000_000, floored: false });
        assert_eq!(renewal(1_030, 0, now_ms), Renewal { at_ms: now_ms + RENEW_FLOOR_MS, floored: true }, "too close");
        assert_eq!(renewal(500, 0, now_ms), Renewal { at_ms: now_ms + RENEW_FLOOR_MS, floored: true }, "already past");
    }

    /// The display tells a subscription being made from one made that
    /// never expires, and from one that renews.
    #[test]
    fn a_dial_in_subscription_says_whether_it_is_subscribed() {
        let mut settled = Settled { transport: Some(SettledTransport::Webhook), ..Settled::default() };
        assert_eq!(webhook_state(&settled), "subscribing (webhook)");
        settled.subscribed = true;
        assert!(webhook_state(&settled).ends_with("does not expire"), "{}", webhook_state(&settled));
        settled.renew_at_ms = Some(0);
        assert!(webhook_state(&settled).ends_with("renews at 1970-01-01 00:00 UTC"), "{}", webhook_state(&settled));
    }

    /// A spec names its topic and its connection; one missing either is
    /// refused naming what is wrong.
    #[test]
    fn a_spec_names_its_topic_and_its_connection() {
        let whole: SignalSpec = serde_json::from_value(serde_json::json!({
            "kind": "provider_events", "config": { "topic": "messages" },
            "access": { "id": "a-1", "service": "slack" }
        }))
        .unwrap();
        let (cfg, access) = parse_spec(&whole).unwrap();
        assert_eq!((cfg.topic.as_str(), cfg.scope, access.id.as_str()), ("messages", EventScope::Account, "a-1"));
        let without_access: SignalSpec = serde_json::from_value(serde_json::json!({
            "kind": "provider_events", "config": { "topic": "messages" }
        }))
        .unwrap();
        let e = parse_spec(&without_access).unwrap_err().to_string();
        assert!(e.contains("no connection"), "{e}");
        let malformed: SignalSpec = serde_json::from_value(serde_json::json!({
            "kind": "provider_events", "config": { "topic": 5 }
        }))
        .unwrap();
        let e = parse_spec(&malformed).unwrap_err().to_string();
        assert!(e.contains("malformed provider_events spec"), "{e}");
    }
}
