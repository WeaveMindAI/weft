//! Provider-event surfaces on the broker: what the listener needs to
//! SERVE subscriptions (resolve a connection's event source, keep a
//! provider-side subscription alive), and what the public receiver
//! needs to accept a push (verification against the operator's
//! registered apps, account extraction, connection matching).
//!
//! The broker is the right side for all of it because it holds the
//! two things nothing else may: the apps file's secrets, and the
//! network egress lock that contains the declared outbound calls.
//! The dispatcher stays secret-free and forwards, exactly like the
//! access admin verbs.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine as _;
use serde_json::Value;

use weft_core::access::events::{read_path, EventsSpec, Handshake, VerifyKind};
use weft_core::access::spec::lookup_path;
use weft_core::access::verify::{
    bearer_token, check_identity_claims, verify_push, IdentityClaims, VerifyError, VerifySecrets,
};

use weft_broker_client::protocol::{
    EventTargetWire, EventVerdict, EventVerifyRequest, ListenerResolveRequest,
    ListenerResolvedSource, SubscriptionDropRequest, SubscriptionEnsureRequest,
    SubscriptionEnsureResponse,
};

use crate::auth::{AuthedCaller, Role};
use crate::state::BrokerState;

type ApiError = (StatusCode, String);

pub fn routes() -> Router<Arc<BrokerState>> {
    Router::new()
        // The listener's serving surface.
        .route("/v1/access/listener-resolve", post(listener_resolve))
        .route("/v1/access/subscription/ensure", post(subscription_ensure))
        .route("/v1/access/subscription/drop", post(subscription_drop))
        // The receive surface the dispatcher forwards raw pushes to.
        .route("/v1/events/verify", post(events_verify))
}

fn internal<E: std::fmt::Display>(e: E) -> ApiError {
    tracing::error!(target: "weft_broker::events", "internal: {e:#}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
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
/// A local-dev install publishes a loopback base URL (the operator's
/// port-forward); a provider on the internet cannot reach it, so for
/// subscriptions it counts as "no public address" and meets the
/// teaching error instead of a subscribe that can never deliver.
pub fn receiver_url(state: &BrokerState, service: &str, topic: &str) -> Option<String> {
    let base = state.internet_base()?;
    Some(format!("{}/events/{service}/{topic}", base.trim_end_matches('/')))
}

// ---------- Listener serving surface ----------

/// POST /v1/access/listener-resolve: the connection's auth values,
/// its event topics, and the recipe-named extra values, fresh (the
/// same lazy refresh a worker resolve runs). Listener role only; the
/// tenant is the registered signal's, the same trust the listener's
/// fires already ride on.
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
        access_id,
        &req.service,
        &req.required_values,
    )
    .await
    .map_err(store_err)?;
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
    // `None` when this weft has no internet-reachable address; the
    // store refuses (with the teaching error) exactly when the topic
    // must TELL the provider where to send. A topic whose delivery
    // address is operator-configured at the provider needs nothing.
    let url = receiver_url(&state, &req.service, &req.topic);
    let ensured = weft_access_store::ensure_subscription(
        &state.pool,
        &weft_access_store::EnsureSubscription {
            tenant: req.tenant,
            service: req.service,
            topic: req.topic,
            access_id,
            signal_token: req.signal_token,
            params: req.params,
            receiver_url: url,
        },
    )
    .await
    .map_err(store_err)?;
    Ok(Json(SubscriptionEnsureResponse {
        expires_at_unix: ensured.expires_at.map(|t| t.timestamp()),
    }))
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
        weft_access_store::drop_subscriptions_for_signal(&state.pool, &req.tenant, &req.signal_token)
            .await
            .map_err(store_err)?;
    Ok(Json(serde_json::json!({ "dropped": dropped })))
}

fn store_err(e: anyhow::Error) -> ApiError {
    match weft_access_store::client_status(&e) {
        Some((status, msg)) => {
            (StatusCode::from_u16(status).expect("store status codes are valid"), msg)
        }
        None => internal(e),
    }
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
        .map_err(internal)?;
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

    // The push as the schemes see it. The url is where the provider
    // was TOLD to post (the receiver's own address for this topic):
    // the address-signing schemes (Twilio's) cover exactly that.
    let receiver = receiver_url(state, &req.service, &req.topic);
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
        VerifyKind::Hmac { .. } | VerifyKind::Signature { .. } => {
            // The app whose material verifies IS the receiving app;
            // try each configured one (usually exactly one).
            let mut matched = None;
            for app in &receiving_apps {
                let events = app.events.as_ref();
                let secrets = VerifySecrets {
                    signing_secret: events.and_then(|e| e.signing_secret.clone()),
                    public_key: events.and_then(|e| e.public_key.clone()),
                    ..Default::default()
                };
                match verify_push(&webhook.verify, &push, &secrets, now) {
                    Ok(()) => {
                        matched = Some(app.app.client_id.clone());
                        break;
                    }
                    Err(VerifyError::BadSignature) => continue,
                    Err(e) => return Err(refused(&req.service, e)),
                }
            }
            let Some(client_id) = matched else {
                if receiving_apps.is_empty() {
                    return Err(refused(
                        &req.service,
                        VerifyError::NotConfigured("receiving app"),
                    ));
                }
                return Err(refused(&req.service, VerifyError::BadSignature));
            };
            (vec![client_id], None)
        }
        VerifyKind::TokenEcho => {
            // The proof is the token weft minted at subscribe time;
            // the push names its subscription and echoes the token.
            let body_json = parse_body(body_bytes);
            let id_path = webhook.id_path.as_deref().unwrap_or_default();
            let presented_id = read_path(&body_json, &req.headers, id_path)
                .map(display_of)
                .ok_or_else(|| refused(&req.service, VerifyError::MissingHeader("subscription id".into())))?;
            let token_path = webhook.token_path.as_deref().unwrap_or_default();
            let presented_token = read_path(&body_json, &req.headers, token_path).map(display_of);
            let sub = weft_access_store::subscription_by_id(
                &state.pool,
                &presented_id,
                &req.service,
                &req.topic,
            )
            .await
            .map_err(internal)?;
            let Some(sub) = sub else {
                // An unknown minted id: a lapsed channel still
                // delivering inside its overlap window, or noise.
                // Either way there is nothing to feed; 200-drop.
                return Ok(Json(EventVerdict::Drop {
                    reason: "no subscription behind this push".into(),
                }));
            };
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
            let token = bearer_token(&req.headers)
                .ok_or_else(|| refused(&req.service, VerifyError::MissingHeader("Authorization".into())))?;
            // The audience defaults to the receiver's own address,
            // which is what the queue uses when none was configured.
            let configured = receiving_apps
                .iter()
                .find_map(|a| a.events.as_ref())
                .cloned();
            let audience = configured
                .as_ref()
                .and_then(|e| e.audience.clone())
                .or_else(|| receiver_url(state, &req.service, &req.topic))
                .ok_or_else(|| refused(&req.service, VerifyError::NotConfigured("push audience")))?;
            let claims = verify_identity_token(token, &audience, issuers, jwks_url)
                .await
                .map_err(|e| refused(&req.service, e))?;
            check_identity_claims(
                &claims,
                &VerifySecrets {
                    audience: Some(audience),
                    expected_email: configured.and_then(|e| e.push_email),
                    ..Default::default()
                },
            )
            .map_err(|e| refused(&req.service, e))?;
            if receiving_apps.is_empty() {
                return Err(refused(&req.service, VerifyError::NotConfigured("receiving app")));
            }
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
    let named_event = topic.named_event(&event, &req.headers);

    // Routing follows the recipe's OWN declaration, never the
    // verification scheme: `route_by` is the one switch (validation
    // already ties `subscription` routing to a declared `id_path`).
    // The token-echo proof happens to look its subscription up during
    // verification; any other scheme routing by subscription looks it
    // up here, by the same (id, service, topic) key.
    match webhook.route_by {
        weft_core::access::events::RouteBy::Subscription => {
            let sub = match subscription {
                Some(sub) => sub,
                None => {
                    let body_json = parse_body(body_bytes);
                    let id_path = webhook.id_path.as_deref().unwrap_or_default();
                    let Some(presented_id) =
                        read_path(&body_json, &req.headers, id_path).map(display_of)
                    else {
                        return Ok(Json(EventVerdict::Drop {
                            reason: "the push names no subscription".into(),
                        }));
                    };
                    let sub = weft_access_store::subscription_by_id(
                        &state.pool,
                        &presented_id,
                        &req.service,
                        &req.topic,
                    )
                    .await
                    .map_err(internal)?;
                    match sub {
                        Some(sub) => sub,
                        None => {
                            return Ok(Json(EventVerdict::Drop {
                                reason: "no subscription behind this push".into(),
                            }))
                        }
                    }
                }
            };
            Ok(Json(EventVerdict::Deliver {
                named_event,
                targets: Vec::new(),
                signal_token: Some(sub.signal_token),
            }))
        }
        weft_core::access::events::RouteBy::Account => {
            let Some(account) = read_path(&outer, &req.headers, &topic.account.path).map(display_of)
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
            .map_err(internal)?;
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

/// A refused push: log the real reason for the operator, answer a
/// flat 401 (detail would be an oracle for whoever is probing).
fn refused(service: &str, e: VerifyError) -> ApiError {
    tracing::warn!(
        target: "weft_broker::events",
        service = %service, reason = %e,
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
    let raw = parse_body(bytes);
    let Some(decode) = &webhook.decode else { return Ok(raw) };
    let encoded = lookup_path(&raw, &decode.path)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("the push carries nothing at '{}' to decode", decode.path))?;
    match decode.encoding {
        weft_core::access::events::Encoding::Base64Json => {
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
    }
}

/// Visit a subscription-confirmation URL (the queue-relay handshake
/// that hands a URL to GET). Refuses anything that is not https to a
/// public host by relying on the broker's egress lock; the request
/// itself carries nothing.
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

/// The published-keys cache, per JWKS address: fetched at most once
/// per hour. Keys rotate slowly and every push carries a fresh token,
/// so a stale cache surfaces as one refused push and a refetch.
static JWKS_CACHE: tokio::sync::Mutex<
    std::collections::BTreeMap<String, (std::time::Instant, jsonwebtoken::jwk::JwkSet)>,
> = tokio::sync::Mutex::const_new(std::collections::BTreeMap::new());

const JWKS_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

/// Verify a signed OIDC identity token: signature against the keys
/// published at the recipe's `jwks_url`, audience, expiry (the
/// library refuses expired tokens), the recipe's issuers. Claim
/// policy beyond that is [`check_identity_claims`]'s.
async fn verify_identity_token(
    token: &str,
    audience: &str,
    issuers: &[String],
    jwks_url: &str,
) -> Result<IdentityClaims, VerifyError> {
    let header = jsonwebtoken::decode_header(token)
        .map_err(|e| VerifyError::BadIdentityToken(format!("unreadable header: {e}")))?;
    let kid = header
        .kid
        .ok_or_else(|| VerifyError::BadIdentityToken("no key id".into()))?;

    let jwks = fetch_jwks(jwks_url)
        .await
        .map_err(|e| VerifyError::BadIdentityToken(format!("key fetch failed: {e}")))?;
    let jwk = jwks
        .find(&kid)
        .ok_or_else(|| VerifyError::BadIdentityToken("unknown signing key".into()))?;
    let key = jsonwebtoken::DecodingKey::from_jwk(jwk)
        .map_err(|e| VerifyError::BadIdentityToken(format!("unusable key: {e}")))?;

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
    validation.set_audience(&[audience]);
    validation.set_issuer(issuers);
    let data = jsonwebtoken::decode::<IdentityClaims>(token, &key, &validation)
        .map_err(|e| VerifyError::BadIdentityToken(format!("{e}")))?;
    Ok(data.claims)
}

async fn fetch_jwks(jwks_url: &str) -> anyhow::Result<jsonwebtoken::jwk::JwkSet> {
    let mut cache = JWKS_CACHE.lock().await;
    if let Some((at, set)) = cache.get(jwks_url) {
        if at.elapsed() < JWKS_TTL {
            return Ok(set.clone());
        }
    }
    let set: jsonwebtoken::jwk::JwkSet = weft_core::access::client::base_client()
        .get(jwks_url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    cache.insert(jwks_url.to_string(), (std::time::Instant::now(), set.clone()));
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The envelope decode: URL-safe and standard base64 both
    /// unwrap; junk fails naming the path.
    #[test]
    fn the_envelope_decode_unwraps_both_alphabets() {
        let recipe: weft_core::access::events::WebhookRecipe = serde_json::from_value(
            serde_json::json!({
                "verify": { "kind": "oidc",
                            "issuers": ["https://accounts.google.com"],
                            "jwks_url": "https://www.googleapis.com/oauth2/v3/certs" },
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

}
