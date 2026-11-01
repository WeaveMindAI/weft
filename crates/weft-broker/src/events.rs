//! Provider-event surfaces on the broker: what the listener needs to
//! SERVE subscriptions (resolve a connection's event source, keep a
//! provider-side subscription alive), and what the public receiver
//! needs to accept a push (verification against the operator's
//! registered apps, account extraction, connection matching).
//!
//! The broker is the right side for all of it because it holds what
//! nothing else may: the apps file's secrets. The dispatcher stays
//! secret-free and forwards, exactly like the access admin verbs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use base64::Engine as _;
use weft_core::jwt::jwk::JwkSet;
use serde_json::Value;

use weft_core::access::events::{check_jwks_url, read_path, EventsSpec, Handshake, VerifyKind};
use weft_core::access::spec::lookup_path;
use weft_core::access::verify::{
    bearer_token, check_identity_claims, verify_push, IdentityClaims, SignedPush, VerifyError, VerifySecrets,
};

use weft_broker_client::protocol::{
    EventTargetWire, EventVerdict, EventVerifyRequest, ListenerResolveRequest,
    ListenerResolvedSource, SubscriptionDropRequest, SubscriptionEnsureRequest,
    SubscriptionEnsureResponse,
};

use crate::handlers::{internal, unavailable_or_internal};
use crate::auth::{AuthedCaller, Role};
use crate::state::BrokerState;
use weft_core::wire_shape::Json;

type ApiError = (StatusCode, String);

pub fn routes() -> Router<Arc<BrokerState>> {
    Router::new()
        // The listener's serving surface.
        .route("/v1/access/listener-resolve", post(listener_resolve))
        .route("/v1/access/subscription/ensure", post(subscription_ensure))
        .route("/v1/access/subscription/check", post(subscription_check))
        .route("/v1/access/subscription/drop", post(subscription_drop))
        // The receive surface the dispatcher forwards raw pushes to.
        .route("/v1/events/verify", post(events_verify))
}


fn listener_only(caller: &crate::auth::CallerIdentity) -> Result<(), ApiError> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    Ok(())
}

/// The public address the provider posts a topic's events to. `None`
/// = this weft is not reachable from the internet; the caller turns
/// that into the teaching error.
/// A local-dev install publishes a loopback base URL (a port the kind
/// node maps to the operator's machine); a provider on the internet
/// cannot reach it, so for
/// subscriptions it counts as "no public address" and meets the
/// teaching error instead of a subscribe that can never deliver. A quick
/// tunnel that cannot say its address is an error.
pub async fn receiver_url(state: &BrokerState, service: &str, topic: &str) -> anyhow::Result<Option<String>> {
    Ok(state.internet_base().await?.map(|base| receiver_on(&base, service, topic)))
}

/// The receiver address on `base`.
fn receiver_on(base: &str, service: &str, topic: &str) -> String {
    format!("{}/events/{service}/{topic}", base.trim_end_matches('/'))
}

/// Whether verifying a push under `verify` reads the address the
/// provider was told to post to: a signature over it (Twilio's), or an
/// identity token whose audience defaults to it for a receiving app
/// that names none.
fn uses_receiver_url(verify: &VerifyKind, receiving_apps: &[crate::app_provider::RegisteredApp]) -> bool {
    match verify {
        VerifyKind::Hmac { concat, .. } | VerifyKind::Signature { concat, .. } => {
            concat.covers(&weft_core::access::events::SignedPart::Url)
        }
        VerifyKind::Oidc { .. } => {
            receiving_apps.iter().filter_map(|a| a.events.as_ref()).any(|e| e.audience.is_none())
        }
        VerifyKind::TokenEcho | VerifyKind::ApiKeys { .. } => false,
    }
}

// ---------- Listener serving surface ----------

/// POST /v1/access/listener-resolve: the connection's auth values,
/// its event topics, and the recipe-named extra values, fresh (the
/// same lazy refresh a worker resolve runs). Listener role only; the
/// tenant and the instance are the registered signal's (they traveled
/// with its registration), the same trust the listener's fires already
/// ride on.
async fn listener_resolve(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ListenerResolveRequest>,
) -> Result<Json<ListenerResolvedSource>, ApiError> {
    listener_only(&caller)?;
    let access_id = parse_access_id(&req.access_id)?;
    let source = weft_access_store::resolve_event_source(
        &state.pool,
        &req.tenant,
        weft_access_store::GrantUser::of(req.for_instance.as_ref()),
        access_id,
        &req.service,
        &req.required_values,
    )
    .await
    .map_err(crate::handlers::store_err)?;
    // The store's answer IS the wire shape: one definition, no copy.
    Ok(Json(source))
}

fn parse_access_id(raw: &str) -> Result<uuid::Uuid, ApiError> {
    raw.parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("malformed connection id '{raw}'")))
}

/// POST /v1/access/subscription/ensure: make sure a live provider
/// subscription serves this signal; answers the current expiry so
/// the listener knows when to come back. Refuses loudly, naming the
/// fix, when the topic needs inbound delivery and this weft has no
/// public address. The broker fills the receiver address: the address
/// configuration is its to know.
async fn subscription_ensure(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SubscriptionEnsureRequest>,
) -> Result<Json<SubscriptionEnsureResponse>, ApiError> {
    listener_only(&caller)?;
    let access_id = parse_access_id(&req.access_id)?;
    // Read only right before a subscribe call, so a recipe with no
    // subscribe call, or a subscription needing no renewal, never asks a
    // quick tunnel its address. `None` when this weft has no
    // internet-reachable address; the store then refuses with the
    // teaching error.
    let (service, topic) = (req.service.clone(), req.topic.clone());
    let receiver = || async { receiver_url(&state, &service, &topic).await };
    let ensured = weft_access_store::ensure_subscription(
        &state.pool,
        &state.lock_pool,
        &weft_access_store::EnsureSubscription {
            for_instance: req.for_instance,
            tenant: req.tenant,
            service: req.service,
            topic: req.topic,
            access_id,
            signal_token: req.signal_token,
            params: req.params,
        },
        receiver,
    )
    .await
    // Its subscribe call, its row and its retirement of the old rows
    // commit one by one (a row written may already be the newest).
    .map_err(crate::handlers::store_err_after_writes)?;
    Ok(Json(SubscriptionEnsureResponse {
        expires_at_unix: ensured.expires_at.map(|t| t.timestamp()),
    }))
}

/// POST /v1/access/subscription/check: whether
/// `/v1/access/subscription/ensure` could subscribe this signal on this
/// install (`weft_access_store::check_subscription`), subscribing nothing:
/// a refusal names the fix, as the subscribe would. Asked before anything
/// is taken down for the signal to come back on.
async fn subscription_check(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SubscriptionEnsureRequest>,
) -> Result<Json<Value>, ApiError> {
    listener_only(&caller)?;
    let access_id = parse_access_id(&req.access_id)?;
    let (service, topic) = (req.service.clone(), req.topic.clone());
    let receiver = || async { receiver_url(&state, &service, &topic).await };
    weft_access_store::check_subscription(
        &state.pool,
        &weft_access_store::EnsureSubscription {
            for_instance: req.for_instance,
            tenant: req.tenant,
            service: req.service,
            topic: req.topic,
            access_id,
            signal_token: req.signal_token,
            params: req.params,
        },
        receiver,
    )
    .await
    .map_err(crate::handlers::store_err)?;
    Ok(Json(Value::Null))
}

/// POST /v1/access/subscription/drop: stop (at the provider) and
/// forget every subscription serving this signal.
async fn subscription_drop(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SubscriptionDropRequest>,
) -> Result<Json<Value>, ApiError> {
    listener_only(&caller)?;
    let dropped =
        weft_access_store::drop_subscriptions_for_signal(&state.pool, &state.lock_pool, &req.tenant, &req.signal_token)
            .await
            .map_err(crate::handlers::store_err)?;
    Ok(Json(serde_json::json!({ "dropped": dropped })))
}

// ---------- The receive surface ----------

/// POST /v1/events/verify: prove a push genuine and say who it is
/// for. Dispatcher-forwarded (control-plane gate); the caller never
/// supplies anything trusted, only the raw request it received.
async fn events_verify(
    State(state): State<Arc<BrokerState>>,
    headers_in: HeaderMap,
    Json(req): Json<EventVerifyRequest>,
) -> Result<Json<EventVerdict>, ApiError> {
    crate::auth::control_plane(&state, &headers_in).await?;

    let body_bytes = base64::engine::general_purpose::STANDARD
        .decode(&req.body_b64)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("body_b64: {e}")))?;

    // The service's recorded recipes (hash-keyed, one per distinct
    // events block a store flow has seen). Never seen = cannot verify
    // = refuse; a 404 here means "open a project using this service's
    // access node first", which the log states. Each candidate recipe
    // is tried in turn: the one whose material verifies the push is
    // the one that answers, and routing then reaches only the
    // connections that declared that exact recipe, so a recipe one
    // spec recorded never answers for another spec's connections.
    let recipes = weft_access_store::events_recipes_of(&state.pool, &req.service)
        .await
        .map_err(unavailable_or_internal)?;
    let candidates: Vec<(String, EventsSpec)> = recipes
        .into_iter()
        .filter_map(|r| r.events.get(&req.topic).cloned().map(|t| (r.recipe_hash, t)))
        .collect();
    if candidates.is_empty() {
        tracing::warn!(
            target: "weft_broker::events",
            service = %req.service, topic = %req.topic,
            "push for an unknown service/topic refused (no recipe on file; connecting a \
             connection for the service records it)"
        );
        return Err((StatusCode::NOT_FOUND, "unknown event source".into()));
    }
    if candidates.iter().all(|(_, t)| t.webhook.is_none()) {
        return Err((StatusCode::NOT_FOUND, "this topic does not receive pushes".into()));
    }

    let mut refusal = None;
    for (recipe_hash, topic) in &candidates {
        let Some(webhook) = topic.webhook.clone() else { continue };
        match verify_and_route(&state, &req, recipe_hash, topic, &webhook, &body_bytes).await {
            Ok(verdict) => return Ok(verdict),
            // A verification refusal may just mean "not this recipe";
            // try the next one and answer the last refusal if none
            // verifies. Any other failure is final.
            Err(e) if e.0 == StatusCode::UNAUTHORIZED => refusal = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(refusal.expect("at least one webhook candidate was tried"))
}

/// The verification + routing pipeline for ONE recorded recipe:
/// verify, answer a handshake, decode, extract, match (routing only
/// to the connections whose snapshot declares this exact recipe).
/// Split from the route so the route stays the auth + shape edge and
/// the per-recipe attempt loop.
async fn verify_and_route(
    state: &Arc<BrokerState>,
    req: &EventVerifyRequest,
    recipe_hash: &str,
    topic: &EventsSpec,
    webhook: &weft_core::access::events::WebhookRecipe,
    body_bytes: &[u8],
) -> Result<Json<EventVerdict>, ApiError> {
    let now = chrono::Utc::now().timestamp();
    // Every scheme consults ONLY the operator's registered apps: a
    // user-registered app (rows on their connections) never supplies
    // a secret and never enables receiving.
    let receiving_apps: Vec<crate::app_provider::RegisteredApp> = state
        .app_provider
        .apps(&req.service)
        .await
        .map_err(internal)?
        .into_iter()
        .filter(|a| a.events.is_some())
        .collect();

    // The subscription the push names, for a recipe that routes by one
    // (a token echo always does): read once, for the token echo's proof
    // and for the routing below.
    let presented_id = match (&webhook.route_by, &webhook.verify) {
        (weft_core::access::events::RouteBy::Subscription, _) | (_, VerifyKind::TokenEcho) => {
            let id_path = webhook.id_path.as_deref().unwrap_or_default();
            read_path(&parse_body(body_bytes), &req.headers, id_path)
                .map_err(|e| refused(&req.service, VerifyError::from(e)))?
                .map(display_of)
        }
        _ => None,
    };
    let named = match &presented_id {
        Some(id) => weft_access_store::subscription_by_id(&state.pool, id, &req.service, &req.topic)
            .await
            .map_err(unavailable_or_internal)?,
        None => None,
    };

    // The push as the schemes see it. The url is where the provider
    // was TOLD to post, which is the address the push came in at: a
    // quick tunnel's old hostname is dead once it changes, so a push that
    // arrived there was sent to the current one, while the platform's own
    // address keeps answering after the install's domain became the
    // address, and a provider told it earlier posts there until its
    // subscription is renewed. It is read only for a scheme that uses it,
    // so no other push pays for asking a quick tunnel its address.
    let receiver = match uses_receiver_url(&webhook.verify, &receiving_apps) {
        true => {
            let host = req.headers.iter().find(|(name, _)| name.eq_ignore_ascii_case("host")).map(|(_, host)| host.as_str());
            state.address.arrived_at(host).await.map_err(unavailable_or_internal)?.map(|base| receiver_on(&base, &req.service, &req.topic))
        }
        false => None,
    };
    let push = weft_core::access::verify::PushParts {
        body: body_bytes,
        headers: &req.headers,
        url: receiver.as_deref(),
        method: &req.method,
    };

    // Verification first, always: even the address-proving handshake
    // is signed on the schemes that sign, and answering an unsigned
    // challenge would let anyone confirm this endpoint exists.
    let (verified_client_ids, subscription) = match &webhook.verify {
        // Refused at recipe validation (a provider never presents one of
        // our keys), so a recorded recipe cannot carry it.
        VerifyKind::ApiKeys { .. } => {
            return Err(refused(
                &req.service,
                VerifyError::NotConfigured("push scheme (api_keys gates callers, never pushes)"),
            ));
        }
        VerifyKind::Hmac { .. } | VerifyKind::Signature { .. } => {
            if receiving_apps.is_empty() {
                return Err(refused(&req.service, VerifyError::NotConfigured("receiving app")));
            }
            // The push is read once, and a fault of the REQUEST's (a part
            // missing, sent twice with two values, or too old) is final:
            // no app's material changes it.
            let signed = SignedPush::read(&webhook.verify, &push, now).map_err(|e| refused(&req.service, e))?;
            let client_id = verifying_app(&signed, &receiving_apps).map_err(|why| refused(&req.service, why))?;
            (vec![client_id], None)
        }
        VerifyKind::TokenEcho => {
            // The proof is the token weft minted at subscribe time; the
            // push named its subscription (read above) and echoes the
            // token.
            if presented_id.is_none() {
                return Err(refused(&req.service, VerifyError::Missing("subscription id".into())));
            }
            let Some(sub) = named.clone() else {
                // An unknown minted id: a lapsed channel still
                // delivering inside its overlap window, or noise.
                // Either way there is nothing to feed; 200-drop.
                return Ok(Json(EventVerdict::Drop {
                    reason: "no subscription behind this push".into(),
                }));
            };
            let token_path = webhook.token_path.as_deref().unwrap_or_default();
            let presented_token = read_path(&parse_body(body_bytes), &req.headers, token_path)
                .map_err(|e| refused(&req.service, VerifyError::from(e)))?
                .map(display_of);
            let secrets = VerifySecrets {
                minted_token: Some(sub.token.clone()),
                presented_token,
                ..Default::default()
            };
            verify_push(&webhook.verify, &push, &secrets, now)
                .map_err(|e| refused(&req.service, e))?;
            (Vec::new(), Some(sub))
        }
        VerifyKind::Oidc { issuers, jwks_url } => {
            // A push recipe names its addresses literally (validated at
            // load), so the templates resolve against nothing.
            let issuers: Vec<String> = issuers.iter().map(|t| t.0.clone()).collect();
            let jwks_url = jwks_url.0.as_str();
            let token = bearer_token(&req.headers)
                .ok_or_else(|| refused(&req.service, VerifyError::Missing("Authorization header".into())))?;
            if receiving_apps.is_empty() {
                return Err(refused(&req.service, VerifyError::NotConfigured("receiving app")));
            }
            // What each receiving app expects of the token: its audience
            // (the receiver's own address when it names none, which is
            // what the queue uses then) and the identity it pushes as.
            let expected: Vec<VerifySecrets> = receiving_apps
                .iter()
                .filter_map(|a| a.events.as_ref())
                .map(|e| VerifySecrets {
                    audience: e.audience.clone().or_else(|| receiver.clone()),
                    expected_email: e.push_email.clone(),
                    ..Default::default()
                })
                .collect();
            let audiences: Vec<String> = expected.iter().filter_map(|e| e.audience.clone()).collect();
            if audiences.is_empty() {
                return Err(refused(&req.service, VerifyError::NotConfigured("push audience")));
            }
            let claims = verify_identity_token(token, &audiences, &issuers, jwks_url)
                .await
                .map_err(|e| refused(&req.service, e))?;
            claims_match_an_app(&claims, &expected).map_err(|e| refused(&req.service, e))?;
            // The token proves the push came through the operator's
            // configured queue; any operator app for the service may
            // have made the watch, so all of them bound the match.
            (
                receiving_apps.iter().map(|a| a.app.client_id.clone()).collect(),
                None,
            )
        }
    };

    // The address-proving handshake, answered only AFTER the
    // verification above.
    if let Some(handshake) = &webhook.handshake {
        let body_json = parse_body(body_bytes);
        match handshake {
            Handshake::EchoBodyField { path } => {
                if let Some(challenge) = lookup_path(&body_json, path).and_then(Value::as_str) {
                    return Ok(Json(EventVerdict::Challenge {
                        body: challenge.to_string(),
                        content_type: "text/plain".into(),
                    }));
                }
            }
            Handshake::EchoQueryParam { param } => {
                if let Some(token) = req.query.get(param) {
                    return Ok(Json(EventVerdict::Challenge {
                        body: token.clone(),
                        content_type: "text/plain".into(),
                    }));
                }
            }
            Handshake::CallbackUrl { path } => {
                if let Some(url) = lookup_path(&body_json, path).and_then(Value::as_str) {
                    confirm_callback_url(url).await.map_err(internal)?;
                    return Ok(Json(EventVerdict::Challenge {
                        body: String::new(),
                        content_type: "text/plain".into(),
                    }));
                }
            }
        }
    }

    // Decode the envelope, unwrap the event, name the fields.
    let outer = decoded_body(webhook, body_bytes).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let event = if webhook.event_path.is_empty() {
        outer.clone()
    } else {
        lookup_path(&outer, &webhook.event_path).cloned().unwrap_or(Value::Null)
    };
    let named_event = topic.named_event(&event, &req.headers).map_err(|e| refused(&req.service, VerifyError::from(e)))?;

    // Routing follows the recipe's OWN declaration, never the
    // verification scheme: `routeBy` is the one switch (validation
    // already ties `subscription` routing to a declared `idPath`).
    // The token-echo proof happens to look its subscription up during
    // verification; any other scheme routing by subscription looks it
    // up here, by the same (id, service, topic) key.
    match webhook.route_by {
        weft_core::access::events::RouteBy::Subscription => {
            let sub = match (subscription.or(named), presented_id) {
                (Some(sub), _) => sub,
                (None, None) => {
                    return Ok(Json(EventVerdict::Drop { reason: "the push names no subscription".into() }));
                }
                (None, Some(_)) => {
                    return Ok(Json(EventVerdict::Drop { reason: "no subscription behind this push".into() }));
                }
            };
            Ok(Json(EventVerdict::Deliver {
                named_event,
                targets: Vec::new(),
                signal_token: Some(sub.signal_token),
            }))
        }
        weft_core::access::events::RouteBy::Account => {
            let Some(account) = read_path(&outer, &req.headers, &topic.account.path)
                .map_err(|e| refused(&req.service, VerifyError::from(e)))?
                .map(display_of)
            else {
                return Ok(Json(EventVerdict::Drop {
                    reason: "the push names no account".into(),
                }));
            };
            let targets = weft_access_store::connections_for_event(
                &state.pool,
                &req.service,
                &account,
                &verified_client_ids,
                recipe_hash,
            )
            .await
            .map_err(unavailable_or_internal)?;
            if targets.is_empty() {
                return Ok(Json(EventVerdict::Drop {
                    reason: "no connection here belongs to that account".into(),
                }));
            }
            let targets = targets
                .into_iter()
                .map(|t| EventTargetWire { access_id: t.id.to_string(), tenant_id: t.tenant_id })
                .collect();
            Ok(Json(EventVerdict::Deliver { named_event, targets, signal_token: None }))
        }
    }
}

/// The client id of the receiving app whose material verifies `signed`:
/// that app IS the one the push came for. Each configured one is tried
/// (usually there is exactly one), and an app that cannot verify it (no
/// secret, one not in the scheme's form, or another secret) only means
/// "not this app". When none verifies, the reason names why each failed.
fn verifying_app(signed: &SignedPush<'_>, receiving_apps: &[crate::app_provider::RegisteredApp]) -> Result<String, String> {
    let mut failures = Vec::with_capacity(receiving_apps.len());
    for app in receiving_apps {
        let events = app.events.as_ref();
        let secrets = VerifySecrets {
            signing_secret: events.and_then(|e| e.signing_secret.clone()),
            public_key: events.and_then(|e| e.public_key.clone()),
            ..Default::default()
        };
        match signed.check(&secrets) {
            Ok(()) => return Ok(app.app.client_id.clone()),
            Err(e) => failures.push(format!("app '{}': {e}", app.app.client_id)),
        }
    }
    Err(format!("no receiving app verifies it ({})", failures.join("; ")))
}

/// A refused push: log the real reason for the operator, answer a
/// flat 401 (detail would be an oracle for whoever is probing).
fn refused(service: &str, reason: impl std::fmt::Display) -> ApiError {
    tracing::warn!(
        target: "weft_broker::events",
        service = %service, reason = %reason,
        "push refused"
    );
    (StatusCode::UNAUTHORIZED, "refused".into())
}

fn parse_body(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}

fn display_of(v: Value) -> String {
    match v {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

/// The (decoded) outer body: the declared envelope decode applied
/// when the recipe has one, else the body parsed as JSON (Null for
/// the header-only pushes).
fn decoded_body(
    webhook: &weft_core::access::events::WebhookRecipe,
    bytes: &[u8],
) -> Result<Value, String> {
    let Some(decode) = &webhook.decode else { return Ok(parse_body(bytes)) };
    match decode.encoding {
        weft_core::access::events::Encoding::Base64Json => {
            let raw = parse_body(bytes);
            let encoded = lookup_path(&raw, &decode.path)
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    format!("the push carries nothing at '{}' to decode", decode.path)
                })?;
            // The queue relays use URL-safe base64; accept standard
            // too, since both alphabets appear in the wild.
            let bytes = base64::engine::general_purpose::URL_SAFE
                .decode(encoded)
                .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded))
                .or_else(|_| base64::engine::general_purpose::STANDARD.decode(encoded))
                .map_err(|e| format!("the push's '{}' is not base64: {e}", decode.path))?;
            serde_json::from_slice(&bytes)
                .map_err(|e| format!("the decoded '{}' is not JSON: {e}", decode.path))
        }
        weft_core::access::events::Encoding::FormJson => {
            // The raw body is form data; `path` names the field whose
            // value is the JSON event (Slack interactivity: `payload=`).
            let text = url::form_urlencoded::parse(bytes)
                .find(|(k, _)| k == decode.path.as_str())
                .map(|(_, v)| v.into_owned())
                .ok_or_else(|| {
                    format!("the form-encoded push carries no '{}' field", decode.path)
                })?;
            serde_json::from_str(&text)
                .map_err(|e| format!("the form field '{}' is not JSON: {e}", decode.path))
        }
    }
}

/// Visit a subscription-confirmation URL (the queue-relay handshake
/// that hands a URL to GET). Refuses anything that is not https; the
/// request itself carries nothing.
async fn confirm_callback_url(url: &str) -> anyhow::Result<()> {
    if !url.starts_with("https://") {
        anyhow::bail!("a confirmation URL must be https, got '{url}'");
    }
    let resp = weft_core::access::client::base_client().get(url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("the confirmation URL answered {}", resp.status());
    }
    Ok(())
}

// ---------- OIDC identity-token verification ----------

/// Whether a verified token's claims satisfy one receiving app: the token
/// names one audience, and the app expecting that audience holds it to
/// that app's identity ([`check_identity_claims`]).
fn claims_match_an_app(claims: &IdentityClaims, expected: &[VerifySecrets]) -> Result<(), VerifyError> {
    let mut verdict = Err(VerifyError::BadIdentityToken("it names no receiving app's audience".into()));
    for app in expected.iter().filter(|e| e.audience.as_deref() == Some(claims.audience.as_str())) {
        verdict = check_identity_claims(claims, app);
        if verdict.is_ok() {
            break;
        }
    }
    verdict
}

/// What the last fetch of an address brought back, as far as a token
/// naming one key cares.
#[derive(Debug, Clone, Copy, PartialEq)]
enum LastOutcome {
    /// Keys, among them the token's.
    HoldsKey,
    /// Keys, but not the token's (the issuer rotated, or the token names a
    /// made-up key).
    LacksKey,
    /// A failure.
    Failed,
}

/// Whether the last fetch of an address, `age` after it ended, still
/// answers a token instead of a new fetch: keys holding the token's key
/// for their hour, anything else for the refetch floor. So a token naming
/// a key the kept set lacks fetches again (the issuer rotated), and a
/// failing address is not fetched again either, but neither sooner than
/// the floor: tokens naming made-up keys, or sent at an address that
/// fails, cannot make weft fetch once per request.
fn last_fetch_answers(age: Duration, outcome: LastOutcome) -> bool {
    age < match outcome {
        LastOutcome::HoldsKey => JWKS_TTL,
        LastOutcome::LacksKey | LastOutcome::Failed => JWKS_REFETCH_FLOOR,
    }
}

const JWKS_TTL: Duration = Duration::from_secs(3600);
const JWKS_REFETCH_FLOOR: Duration = Duration::from_secs(60);
/// How long one key fetch may take, start to finish. An internal wait no
/// user controls, so it has a deadline: an issuer that never answers
/// fails its own tokens after this, and holds nobody else's.
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// The keys every identity-token check of this process reads.
static JWKS: std::sync::LazyLock<JwksCache> = std::sync::LazyLock::new(|| JwksCache::new(JWKS_FETCH_TIMEOUT));

/// The issuers' published keys, kept per JWKS address, and fetched again
/// as [`last_fetch_answers`] says.
///
/// Each address is its own: kept keys are read without waiting on any
/// fetch, one fetch of an address runs at a time (callers that need it
/// meanwhile wait for its outcome instead of fetching again), and a
/// fetch is bounded by the cache's timeout, so an issuer that never
/// answers stalls only the tokens that name it.
struct JwksCache {
    by_url: std::sync::Mutex<HashMap<String, Arc<UrlKeys>>>,
    fetch_timeout: Duration,
}

/// One address's keys: what its last fetch brought back, and the lock one
/// fetch of it holds.
#[derive(Default)]
struct UrlKeys {
    /// Read and written in passing, never held across a fetch.
    last: std::sync::Mutex<Option<LastFetch>>,
    fetching: tokio::sync::Mutex<()>,
}

struct LastFetch {
    ended: Instant,
    /// The keys, or why the fetch brought back none.
    outcome: Result<JwkSet, String>,
}

impl UrlKeys {
    /// What the last fetch answers for a token naming `kid`, while
    /// [`last_fetch_answers`] says it still does: its keys, or its failure.
    fn answer(&self, kid: &str) -> Option<Result<JwkSet, String>> {
        let last = self.last.lock().expect("the last fetch is only swapped, never left half-written");
        let last = last.as_ref()?;
        let outcome = match &last.outcome {
            Ok(keys) if keys.find(kid).is_some() => LastOutcome::HoldsKey,
            Ok(_) => LastOutcome::LacksKey,
            Err(_) => LastOutcome::Failed,
        };
        last_fetch_answers(last.ended.elapsed(), outcome).then(|| last.outcome.clone())
    }

    fn record(&self, outcome: &Result<JwkSet, String>) {
        let mut last = self.last.lock().expect("the last fetch is only swapped, never left half-written");
        *last = Some(LastFetch { ended: Instant::now(), outcome: outcome.clone() });
    }
}

impl JwksCache {
    /// A cache whose every fetch, start to finish, ends within
    /// `fetch_timeout`.
    fn new(fetch_timeout: Duration) -> Self {
        Self { by_url: Default::default(), fetch_timeout }
    }

    /// The keys published at `url`, for a token naming the key `kid`.
    async fn keys(&self, url: &str, kid: &str) -> Result<JwkSet, String> {
        let entry = self.by_url.lock().expect("the address map is changed only in whole steps").entry(url.to_string()).or_default().clone();
        if let Some(answer) = entry.answer(kid) {
            return answer;
        }
        let _one_fetch = entry.fetching.lock().await;
        // A fetch that ran while this caller waited answers it too, keys
        // or failure.
        if let Some(answer) = entry.answer(kid) {
            return answer;
        }
        let outcome = self.fetch(url).await;
        entry.record(&outcome);
        outcome
    }

    async fn fetch(&self, url: &str) -> Result<JwkSet, String> {
        let failed = |e: reqwest::Error| format!("{:#}", anyhow::Error::from(e));
        let resp = weft_core::access::client::base_client().get(url).timeout(self.fetch_timeout).send().await.map_err(failed)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("{url} answered {status}"));
        }
        resp.json::<JwkSet>().await.map_err(|e| format!("{url} answered no key set: {}", failed(e)))
    }
}

/// Verify a signed OIDC identity token: signature against the keys
/// published at the recipe's `jwksUrl`, audience, expiry (the
/// library refuses expired tokens), the recipe's issuers. Claim
/// policy beyond that is [`check_identity_claims`]'s.
async fn verify_identity_token(
    token: &str,
    audiences: &[String],
    issuers: &[String],
    jwks_url: &str,
) -> Result<IdentityClaims, VerifyError> {
    let mut validation = weft_core::jwt::Validation::new(weft_core::jwt::Algorithm::RS256);
    validation.set_audience(audiences);
    validation.set_issuer(issuers);
    decode_jwt(token, &validation, jwks_url).await
}

/// Decode and verify one JWT against the keys published at `jwks_url`
/// (the key named by the token's `kid`), under `validation`. The one
/// signature-plus-keys step both the push path (`verify_identity_token`)
/// and the caller path (`caller_auth`) share; what each demands of the
/// claims is theirs.
pub(crate) async fn decode_jwt<T: serde::de::DeserializeOwned>(
    token: &str,
    validation: &weft_core::jwt::Validation,
    jwks_url: &str,
) -> Result<T, VerifyError> {
    // Checked here, on the address about to be fetched: a gated route's
    // comes off its connection's stored values, unknown until now.
    check_jwks_url(jwks_url).map_err(VerifyError::UnusableKeyAddress)?;
    let header = weft_core::jwt::decode_header(token)
        .map_err(|e| VerifyError::BadIdentityToken(format!("unreadable header: {e}")))?;
    let kid = header
        .kid
        .ok_or_else(|| VerifyError::BadIdentityToken("no key id".into()))?;
    let jwks = JWKS
        .keys(jwks_url, &kid)
        .await
        .map_err(|why| VerifyError::BadIdentityToken(format!("key fetch failed: {why}")))?;
    let jwk = jwks
        .find(&kid)
        .ok_or_else(|| VerifyError::BadIdentityToken("unknown signing key".into()))?;
    let key = weft_core::jwt::DecodingKey::from_jwk(jwk)
        .map_err(|e| VerifyError::BadIdentityToken(format!("unusable key: {e}")))?;
    let data = weft_core::jwt::decode::<T>(token, &key, validation)
        .map_err(|e| VerifyError::BadIdentityToken(format!("{e}")))?;
    Ok(data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With several receiving apps, a token is held to the app whose
    /// audience it names, never only to the first app on file.
    #[test]
    fn an_identity_token_is_held_to_the_app_whose_audience_it_names() {
        let app = |aud: &str, email: &str| VerifySecrets {
            audience: Some(aud.into()),
            expected_email: Some(email.into()),
            ..Default::default()
        };
        let apps = [app("https://a.example/events/google/mail", "a@x.iam"), app("https://b.example/aud", "b@x.iam")];
        let claims = |aud: &str, email: &str| IdentityClaims {
            audience: aud.into(),
            email: Some(email.into()),
            email_verified: true,
            expires_at: 0,
        };
        claims_match_an_app(&claims("https://b.example/aud", "b@x.iam"), &apps).expect("the second app's token");
        let err = claims_match_an_app(&claims("https://b.example/aud", "a@x.iam"), &apps).unwrap_err();
        assert!(err.to_string().contains("different identity"), "{err}");
        let err = claims_match_an_app(&claims("https://c.example", "a@x.iam"), &apps).unwrap_err();
        assert!(err.to_string().contains("no receiving app"), "{err}");
    }

    /// A kept key set answers for its hour while it holds the token's
    /// key; a token naming a key it lacks (a rotation) fetches again, and
    /// so does a failed fetch, but neither sooner than the floor.
    #[test]
    fn a_rotated_issuer_key_is_fetched_again_within_the_floor() {
        let minute = Duration::from_secs(60);
        assert!(last_fetch_answers(minute * 59, LastOutcome::HoldsKey));
        assert!(!last_fetch_answers(minute * 60, LastOutcome::HoldsKey));
        assert!(last_fetch_answers(minute / 2, LastOutcome::LacksKey), "a made-up key cannot force a fetch per token");
        assert!(!last_fetch_answers(minute, LastOutcome::LacksKey), "a rotated key is fetched after the floor");
        assert!(last_fetch_answers(minute / 2, LastOutcome::Failed), "a failing address cannot force a fetch per token");
        assert!(!last_fetch_answers(minute, LastOutcome::Failed), "a failed address is tried again after the floor");
    }

    /// A server that accepts the connection and never answers.
    async fn silent_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((conn, _)) = listener.accept().await {
                held.push(conn);
            }
        });
        format!("http://{addr}/jwks.json")
    }

    /// A server that answers every request with an empty key set.
    async fn answering_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().fallback(|| async { r#"{"keys":[]}"# });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/jwks.json")
    }

    // An issuer that never answers holds only its own address: another
    // address's keys arrive meanwhile, and the silent one fails at the
    // fetch deadline, as do the callers that waited on that same fetch.
    weft_core::stress_test!(
        name: a_silent_issuer_holds_only_its_own_address,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let cache = Arc::new(JwksCache::new(Duration::from_secs(2)));
            let silent = silent_server().await;
            let answering = answering_server().await;
            let stalled: Vec<_> = (0..3)
                .map(|_| {
                    let (cache, silent) = (cache.clone(), silent.clone());
                    tokio::spawn(async move { cache.keys(&silent, "k").await })
                })
                .collect();
            tokio::task::yield_now().await;
            let keys = tokio::time::timeout(Duration::from_secs(1), cache.keys(&answering, "k"))
                .await
                .expect("another address is not held by the silent one")
                .expect("its keys arrive");
            assert!(keys.keys.is_empty());
            for waiting in stalled {
                let outcome = tokio::time::timeout(Duration::from_secs(10), waiting)
                    .await
                    .expect("the silent fetch ends at its deadline")
                    .unwrap();
                assert!(!outcome.unwrap_err().is_empty());
            }
        }
    );

    /// A server answering every request with `500`, counting the requests.
    async fn failing_server() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let counted = hits.clone();
        let app = Router::new().fallback(move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { StatusCode::INTERNAL_SERVER_ERROR }
        });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/jwks.json"), hits)
    }

    /// A failing address is fetched once within the floor: every token
    /// after it gets the recorded failure instead of a fetch of its own.
    #[tokio::test]
    async fn a_failing_key_address_is_fetched_once_within_the_floor() {
        let (url, hits) = failing_server().await;
        let cache = JwksCache::new(Duration::from_secs(5));
        for kid in ["k", "k", "other"] {
            let err = cache.keys(&url, kid).await.unwrap_err();
            assert!(err.contains("500"), "{err}");
        }
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// Signing keys come over https only, checked on the address about
    /// to be fetched, before anything of the token is read.
    #[tokio::test]
    async fn an_http_key_address_is_refused_before_any_fetch() {
        let validation = weft_core::jwt::Validation::new(weft_core::jwt::Algorithm::RS256);
        let err = decode_jwt::<Value>("not.a.token", &validation, "http://issuer.example/jwks.json").await.unwrap_err();
        assert!(matches!(err, VerifyError::UnusableKeyAddress(_)), "{err}");
    }

    /// With several receiving apps, one whose secret is missing or not
    /// in the scheme's form is only "not this app": the next app's
    /// secret verifies, and when none does, the reason names each app.
    #[test]
    fn the_next_app_verifies_when_one_is_misconfigured() {
        let kind: VerifyKind = serde_json::from_value(serde_json::json!({
            "kind": "hmac",
            "signature": { "in": "header:webhook-signature", "list": " ", "pair": ",", "key": "v1" },
            "timestamp": { "in": "header:webhook-timestamp" },
            "concat": "{header:webhook-id}.{timestamp}.{body}",
            "secret": { "stripPrefix": "whsec_", "decode": "base64" },
            "encoding": "base64"
        }))
        .unwrap();
        let app = |id: &str, events: Value| -> crate::app_provider::RegisteredApp {
            serde_json::from_value(serde_json::json!({
                "covers": [], "tokenUrl": "https://p.example/token", "label": id, "client_id": id, "events": events
            }))
            .unwrap()
        };
        let headers: Vec<(String, String)> = [
            ("webhook-id", "msg_p5jXN8AQM9LWM0D4loKWxJek"),
            ("webhook-timestamp", "1614265330"),
            ("webhook-signature", "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let push = weft_core::access::verify::PushParts { body: br#"{"test": 2432232314}"#, headers: &headers, url: None, method: "POST" };
        let signed = SignedPush::read(&kind, &push, 1614265330).expect("the request is whole");
        let apps = [
            app("no-secret", serde_json::json!({})),
            app("no-prefix", serde_json::json!({ "signingSecret": "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw" })),
            app("right", serde_json::json!({ "signingSecret": "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw" })),
        ];
        assert_eq!(verifying_app(&signed, &apps).as_deref(), Ok("right"));
        let why = verifying_app(&signed, &apps[..2]).unwrap_err();
        assert!(why.contains("app 'no-secret': no signing secret") && why.contains("app 'no-prefix': no usable signing secret"), "{why}");
    }

    /// The envelope decode: URL-safe and standard base64 both
    /// unwrap; junk fails naming the path.
    #[test]
    fn the_envelope_decode_unwraps_both_alphabets() {
        let recipe: weft_core::access::events::WebhookRecipe = serde_json::from_value(
            serde_json::json!({
                "verify": { "kind": "oidc",
                            "issuers": ["https://accounts.google.com"],
                            "jwksUrl": "https://www.googleapis.com/oauth2/v3/certs" },
                "decode": { "path": "message.data", "encoding": "base64_json" }
            }),
        )
        .unwrap();
        let inner = serde_json::json!({ "emailAddress": "q@x.com", "historyId": 42 });
        let inner_text = serde_json::to_string(&inner).unwrap();
        for engine in [
            base64::engine::general_purpose::URL_SAFE.encode(&inner_text),
            base64::engine::general_purpose::STANDARD.encode(&inner_text),
        ] {
            let push = serde_json::json!({ "message": { "data": engine } });
            let out = decoded_body(&recipe, serde_json::to_string(&push).unwrap().as_bytes())
                .expect("decodes");
            assert_eq!(out, inner);
        }
        let bad = serde_json::json!({ "message": { "data": "!!!" } });
        let err =
            decoded_body(&recipe, serde_json::to_string(&bad).unwrap().as_bytes()).unwrap_err();
        assert!(err.contains("message.data"), "{err}");
    }

    /// The form decode: a urlencoded `payload=` field unwraps to its
    /// JSON (Slack interactivity); a missing field and non-JSON both
    /// fail naming the field.
    #[test]
    fn the_form_decode_unwraps_the_payload_field() {
        let recipe: weft_core::access::events::WebhookRecipe = serde_json::from_value(
            serde_json::json!({
                "verify": { "kind": "hmac",
                            "signature": { "in": "header:X-Slack-Signature", "pair": "=", "key": "v0" },
                            "timestamp": { "in": "header:X-Slack-Request-Timestamp" },
                            "concat": "v0:{timestamp}:{body}" },
                "decode": { "path": "payload", "encoding": "form_json" }
            }),
        )
        .unwrap();
        let inner = serde_json::json!({ "type": "block_actions", "team": { "id": "T1" } });
        let body: String = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("payload", &serde_json::to_string(&inner).unwrap())
            .finish();
        let out = decoded_body(&recipe, body.as_bytes()).expect("decodes");
        assert_eq!(out, inner);

        let err = decoded_body(&recipe, b"other=1").unwrap_err();
        assert!(err.contains("payload"), "{err}");
        let err = decoded_body(&recipe, b"payload=not-json").unwrap_err();
        assert!(err.contains("payload"), "{err}");
    }
}
