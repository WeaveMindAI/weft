//! The transport-neutral event subscription, served. A registered
//! `provider_events` signal names a connection, a topic and a filter;
//! this handler decides which transport the environment can serve and
//! runs it:
//!
//!   - **dial-out**: the service declares a socket recipe and the
//!     connection holds every value its mint call needs. ONE shared
//!     socket per (connection, topic) on this pod, fanning every
//!     inbound event to all of that pair's subscriptions; the engine
//!     re-resolves the connection and re-mints the address on every
//!     reconnect.
//!   - **dial-in**: the service pushes to the public events surface.
//!     This side's job is only the provider subscription (subscribe,
//!     renew before expiry, unsubscribe at unregister), driven
//!     through the broker; the fires arrive through the receiver.
//!
//! A FRESH registration makes the transport decision (and, dial-in,
//! the first subscribe) BEFORE answering, so activating a trigger
//! that cannot be served fails loudly right there, naming what is
//! missing; it never mints a silently-dead trigger. The serving task
//! then serves exactly what activation decided, so the two can never
//! diverge. A rehydrate decides for itself in the background,
//! retrying resolution until it can (one unreachable broker at boot
//! must not fail the whole rebuild).

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
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::{RegisteredSignal, TaskGuard, Transport};
use crate::socket_engine::{self, CyclePlan, PrepareError};

use super::{KindHandler, SpawnCtx};

pub struct ProviderEventsHandler;

#[async_trait]
impl KindHandler for ProviderEventsHandler {
    fn tag(&self) -> &'static str {
        ProviderEvents::TAG
    }

    fn compute_routing(
        &self,
        _token: &str,
        _spec: &SignalSpec,
        _secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<SignalRouting> {
        // Fires arrive internally: from this pod's shared socket, or
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
        _kind_state: &Value,
        ctx: SpawnCtx,
    ) -> Result<Option<JoinHandle<()>>> {
        let cfg: ProviderEvents = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed provider_events spec: {e}"))?;
        let access = spec
            .access
            .clone()
            .ok_or_else(|| anyhow::anyhow!("provider_events signal has no connection"))?;
        set_status(&ctx, "starting");

        let decided = if ctx.fresh {
            // Decide (and for dial-in, subscribe) NOW, so activation
            // fails loudly on an unservable trigger instead of
            // minting a dead one. The decision travels into the
            // serving task, which serves exactly what was reported.
            let source = crate::listener_access::resolve(&access, &ctx).await?;
            let topic = source
                .events
                .get(&cfg.topic)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "'{}' declares no event topic named '{}'; reconnect the account (an \
                         older connection may predate the topic)",
                        access.service,
                        cfg.topic
                    )
                })?
                .clone();
            let mut available = source.values.clone();
            available.extend(source.recipe_values.clone());
            let transport = decide_transport(&topic, cfg.scope, &available);
            match &transport {
                Transport::Socket => {}
                Transport::Webhook => {
                    // The first subscribe runs here so a missing
                    // public address (or a provider refusal) is THIS
                    // activation's error.
                    ensure_via_broker(&ctx, &access, &cfg)
                        .await
                        .map_err(|e| anyhow::anyhow!("{e:#}"))?;
                }
                Transport::Unservable(reason) => {
                    return Err(anyhow::anyhow!(
                        "this trigger cannot be served on the '{}' connection: {reason}",
                        access.service
                    ));
                }
            }
            Some(DecidedServing {
                transport,
                topic,
                provider_account: source.provider_account.clone(),
            })
        } else {
            None
        };

        Ok(Some(serve_subscription(cfg, access, decided, ctx)))
    }

    fn process_entry(&self, _sig: &RegisteredSignal, payload: Value) -> ProcessOutcome {
        ProcessOutcome { value: payload, target: ProcessTarget::Entry }
    }

    /// No register-time snapshot: the trigger's state is LIVE (which
    /// transport serves it, what the task is doing right now) and is
    /// served from the registry entry's serving slot via /display,
    /// never frozen onto the signal row.
    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }

    /// A dial-in subscription holds provider-side state (the provider
    /// keeps posting to a channel weft asked for); drop it at the
    /// provider too. A socket-served signal never subscribed, and one
    /// whose transport was never recorded holds nothing this pod
    /// arranged; either way there is nothing to drop, and a provider
    /// channel that outlives its signal lapses on its own expiry.
    async fn on_unregister(
        &self,
        token: &str,
        sig: &RegisteredSignal,
        events_broker: &Arc<weft_broker_client::BrokerEventsClient>,
    ) {
        if sig.serving.lock().transport != Some(Transport::Webhook) {
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

/// Write the signal's live serving status where /display reads it.
fn set_status(ctx: &SpawnCtx, status: impl Into<String>) {
    ctx.serving.lock().status = status.into();
}

/// Which transport serves a subscription, decided from the recipe,
/// the subscription's SCOPE, and what the connection actually holds.
/// Pure. Dial-out wins when the connection can run the mint call (it
/// needs no public address and its socket is single-owner by
/// construction); otherwise dial-in; otherwise a reason naming
/// exactly what is missing. An APP-wide subscription is socket-only:
/// the push receiver routes each event to ONE account's connections,
/// so it can never deliver "every install of your app".
pub fn decide_transport(
    topic: &EventsSpec,
    scope: EventScope,
    available: &BTreeMap<String, String>,
) -> Transport {
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

/// The transport decision plus everything it was decided FROM that
/// the serving task needs: the topic recipe and the connection's own
/// provider account.
struct DecidedServing {
    transport: Transport,
    topic: EventsSpec,
    provider_account: Option<String>,
}

/// The long-running per-signal task: serve the decided transport
/// until the signal unregisters (its abort drops the membership
/// guard). A FRESH registration hands its decision in, so what runs
/// here is exactly what activation reported; a rehydrate decides for
/// itself, retrying resolution on the ladder until it can (one
/// unreachable broker at boot must not fail the whole rebuild). An
/// unservable decision is TERMINAL: retrying would re-derive the same
/// facts, and a reconnect of the account re-registers the signal and
/// re-runs the decision.
fn serve_subscription(
    cfg: ProviderEvents,
    access: AccessRef,
    decided: Option<DecidedServing>,
    ctx: SpawnCtx,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let decided = match decided {
            Some(d) => d,
            None => {
                let mut backoff = crate::kinds::event_source::Backoff::new();
                loop {
                    let source = match crate::listener_access::resolve(&access, &ctx).await {
                        Ok(s) => s,
                        Err(e) => {
                            set_status(&ctx, format!("cannot resolve the connection: {e:#}"));
                            backoff.wait_then_climb().await;
                            continue;
                        }
                    };
                    let Some(topic) = source.events.get(&cfg.topic).cloned() else {
                        set_status(
                            &ctx,
                            format!(
                                "the connection declares no '{}' topic; reconnect the account",
                                cfg.topic
                            ),
                        );
                        backoff.wait_then_climb().await;
                        continue;
                    };
                    let mut available = source.values.clone();
                    available.extend(source.recipe_values.clone());
                    break DecidedServing {
                        transport: decide_transport(&topic, cfg.scope, &available),
                        topic,
                        provider_account: source.provider_account,
                    };
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

/// One pod-wide socket per (connection, topic), fanning events to all
/// of that pair's subscriptions. Keyed by pod too, so several
/// in-process listeners (tests) never cross wires.
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
}

struct SocketShare {
    subscribers: Arc<DashMap<String, Subscriber>>,
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
    let key = format!("{}|{}|{}", ctx.config.pod_name, access.id, cfg.topic);
    let _guard = SubscriberGuard { key: key.clone(), token: ctx.fire.token().to_string() };
    {
        let share = SOCKETS.entry(key.clone()).or_insert_with(|| {
            let subscribers: Arc<DashMap<String, Subscriber>> = Arc::new(DashMap::new());
            let engine = spawn_shared_engine(
                access.clone(),
                cfg.topic.clone(),
                ctx.clone(),
                subscribers.clone(),
            );
            SocketShare { subscribers, engine: TaskGuard::new(engine) }
        });
        share.subscribers.insert(
            ctx.fire.token().to_string(),
            Subscriber { fire: ctx.fire.clone(), scope: cfg.scope, provider_account },
        );
    }
    set_status(ctx, "holding the event socket");
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
            let source = crate::listener_access::resolve(&access, &ctx)
                .await
                .map_err(PrepareError::Transient)?;
            let topic = source
                .events
                .get(&topic_name)
                .cloned()
                .ok_or_else(|| {
                    PrepareError::Transient(anyhow::anyhow!(
                        "the connection declares no '{topic_name}' topic"
                    ))
                })?;
            let socket = topic.socket.clone().ok_or_else(|| {
                PrepareError::Transient(anyhow::anyhow!(
                    "the '{topic_name}' topic declares no socket recipe"
                ))
            })?;
            let mut values = source.values;
            values.extend(source.recipe_values);
            let url = socket_engine::mint_socket_url(&socket.minted, &values).await?;
            *recipe.lock() = Some(topic.clone());
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
                sub.fire.fire(named.clone(), "provider_events").await;
            }
        }
        .boxed()
    });

    socket_engine::spawn(prepare, on_event, "provider_events")
}

// ---------- Dial-in: the provider subscription lifecycle ----------

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
    let margin_secs = topic
        .webhook
        .as_ref()
        .and_then(|w| w.subscribe.as_ref())
        .and_then(|s| s.renew_margin_secs)
        .unwrap_or(0);
    let mut backoff = crate::kinds::event_source::Backoff::new();
    loop {
        match ensure_via_broker(ctx, access, cfg).await {
            Ok(ensured) => {
                set_status(ctx, "subscribed; events arrive via push");
                match ensured.expires_at_unix {
                    None => std::future::pending::<()>().await,
                    Some(at) => {
                        let wake = at - margin_secs as i64;
                        let now = chrono::Utc::now().timestamp();
                        if wake <= now {
                            // A renewal point already behind now
                            // means the provider answered a stale (or
                            // absurdly short) expiry; keep the 60s
                            // floor so the loop never spins, and say
                            // so.
                            tracing::warn!(
                                target: "weft_listener::provider_events",
                                token = %ctx.fire.token(), topic = %cfg.topic,
                                expires_at_unix = at,
                                "the provider answered an expiry already behind the \
                                 renewal point; renewing on the 60s floor"
                            );
                        }
                        let sleep_secs = (wake - now).max(60) as u64;
                        tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
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
            decide_transport(&both, EventScope::Account, &with_token),
            Transport::Socket
        );
        assert_eq!(
            decide_transport(&both, EventScope::Account, &BTreeMap::new()),
            Transport::Webhook
        );
        // An APP-wide subscription never falls to the push transport
        // (it routes per account, which can never mean "every
        // install"); without the dial-out value it refuses, naming it.
        assert_eq!(
            decide_transport(&both, EventScope::App, &with_token),
            Transport::Socket
        );
        match decide_transport(&both, EventScope::App, &BTreeMap::new()) {
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
        match decide_transport(&socket_only, EventScope::Account, &BTreeMap::new()) {
            Transport::Unservable(reason) => {
                assert!(reason.contains("app_token"), "{reason}")
            }
            other => panic!("expected unservable, got {other:?}"),
        }
    }
}
