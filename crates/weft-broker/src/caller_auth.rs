//! The caller-verify door: is the party opening a live route who the
//! route's auth connection says it must be?
//!
//! A `Route { auth: <connection> }` names a stored connection whose
//! service recipe declares a `verify` scheme (a set of API keys, a JWT
//! issuer, an HMAC secret) and whose stored values hold the material.
//! The dispatcher forwards the caller's opening request here and admits
//! the run only on a yes; it never sees the material. The broker is the
//! right side because it holds the connections, exactly as it holds the
//! apps file's secrets for provider pushes (`events::verify`).
//!
//! The arithmetic is `weft_core::access::verify` (pure, shared with the
//! push path); the identity-token scheme's key fetch is the one piece of
//! I/O, shared with the push path too (`events::decode_jwt`). Refusals
//! are flat: the reason goes to the log, the caller gets a `401`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine as _;
use serde_json::Value;
use sqlx::PgPool;

use weft_core::access::events::VerifyKind;
use weft_core::access::verify::{
    bearer_token, needs_issuer_keys, secrets_from_values, verify_push, PushParts, VerifyError,
};
use weft_broker_client::protocol::{CallerVerified, CallerVerifyRequest};

use crate::handlers::internal;
use crate::state::BrokerState;

type ApiError = (StatusCode, String);

pub fn routes() -> Router<Arc<BrokerState>> {
    Router::new().route("/v1/caller/verify", post(caller_verify))
}

/// POST /v1/caller/verify: check one caller of a live route against the
/// connection the route is gated by. Dispatcher-forwarded (control-plane
/// gate); the caller of the ROUTE never reaches this door.
async fn caller_verify(
    State(state): State<Arc<BrokerState>>,
    headers_in: HeaderMap,
    Json(req): Json<CallerVerifyRequest>,
) -> Result<Json<CallerVerified>, ApiError> {
    crate::auth::control_plane(&state, &headers_in).await?;
    let now = chrono::Utc::now().timestamp();
    match verify_caller(&state.verifiers, &state.pool, &req, now).await {
        Ok(identity) => Ok(Json(CallerVerified { identity })),
        Err(CallerRefusal::Refused(e)) => {
            tracing::warn!(
                target: "weft_broker::caller_auth",
                tenant = %req.tenant, access_id = %req.access_id, path = %req.path,
                reason = %e,
                "caller refused"
            );
            Err((StatusCode::UNAUTHORIZED, "refused".into()))
        }
        Err(CallerRefusal::Failed(e)) => Err(internal(e)),
    }
}

/// Why a caller was not verified: a refusal (the request did not prove
/// what the scheme demands, or the connection cannot verify anyone),
/// or a failure of ours (the store, a malformed id).
#[derive(Debug)]
pub enum CallerRefusal {
    Refused(String),
    Failed(anyhow::Error),
}

impl From<VerifyError> for CallerRefusal {
    fn from(e: VerifyError) -> Self {
        CallerRefusal::Refused(e.to_string())
    }
}

/// Which connection a gate verifies against, and for whom: everything the
/// read of its verifier depends on.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct VerifierKey {
    tenant: String,
    for_instance: Option<(uuid::Uuid, weft_core::instance::InstanceId)>,
    access_id: uuid::Uuid,
    service: String,
}

/// The verifiers this broker keeps (`BrokerState::verifiers`): a gate
/// checks every caller of its route against the same connection, so the
/// connection is read once and kept until it changes
/// (`weft_broker_client::line::ACCESS_CHANNEL`). The process already holds
/// the key that opens every stored connection, so keeping one open adds
/// nothing a look at its memory would not already give.
pub type HeldVerifiers = weft_task_store::held_copy::HeldCopy<VerifierKey, weft_access_store::CallerVerifier>;

/// Which verifiers a notice on `weft_access` drops (its payload is
/// `weft_access_store`'s `access_notify`).
pub fn verifiers_changed(payload: &str) -> weft_task_store::held_copy::Changed<VerifierKey, weft_access_store::CallerVerifier> {
    let payload = payload.to_string();
    weft_task_store::held_copy::Changed::Matching(Box::new(move |key: &VerifierKey, _| names(&payload, key)))
}

/// Whether a change announced with `payload` reaches the verifier under
/// `key`: a tenant's shared connections reach every verifier of the tenant,
/// a project's own (its instances') every verifier for that project's
/// instances. A payload that names neither reaches every verifier.
// SYNC: the payloads <-> crates/weft-access-store/src/lib.rs (access_notify), crates/weft-dispatcher/src/held.rs (access_changed), crates/weft-broker/src/line.rs (audience)
fn names(payload: &str, key: &VerifierKey) -> bool {
    match payload.strip_prefix("tenant:") {
        Some(tenant) => key.tenant == tenant,
        None => match payload.parse::<uuid::Uuid>() {
            Ok(project) => key.for_instance.as_ref().is_some_and(|(of, _)| *of == project),
            Err(_) => true,
        },
    }
}

/// The check itself: load the connection's scheme and values (kept in
/// `verifiers` until they change), resolve the scheme's templates against
/// the values, run it. Answers the identity the scheme established.
/// Separate from the route so the db-tests exercise it directly.
pub async fn verify_caller(
    verifiers: &HeldVerifiers,
    pool: &PgPool,
    req: &CallerVerifyRequest,
    now_unix: i64,
) -> Result<Value, CallerRefusal> {
    let access_id: uuid::Uuid = req
        .access_id
        .parse()
        .map_err(|_| CallerRefusal::Failed(anyhow::anyhow!("malformed connection id '{}'", req.access_id)))?;
    let key = VerifierKey {
        tenant: req.tenant.clone(),
        for_instance: req.for_instance.as_ref().map(|scope| (scope.project_id, scope.instance.clone())),
        access_id,
        service: req.service.clone(),
    };
    let verifier = verifiers
        .get_or_load(key, || async {
            weft_access_store::caller_verifier(pool, &req.tenant, weft_access_store::GrantUser::of(req.for_instance.as_ref()), access_id, &req.service)
                .await
                .map_err(|e| match e.downcast_ref::<weft_access_store::AccessError>() {
                    // A connection that is not there (or another tenant's,
                    // which reads the same) cannot admit anyone.
                    Some(weft_access_store::AccessError::NotFound) => {
                        CallerRefusal::Refused("no such connection".into())
                    }
                    _ => CallerRefusal::Failed(e),
                })
        })
        .await?;
    let Some(kind) = verifier.verify.clone() else {
        return Err(CallerRefusal::Refused(format!(
            "the '{}' service declares no `verify` block, so its connections cannot gate a route",
            req.service
        )));
    };
    let kind = kind.resolved(&verifier.values).map_err(CallerRefusal::Refused)?;
    let secrets = secrets_from_values(&verifier.values);
    let body = base64::engine::general_purpose::STANDARD
        .decode(&req.body_b64)
        .map_err(|e| CallerRefusal::Failed(anyhow::anyhow!("body_b64: {e}")))?;
    let push = PushParts { body: &body, headers: &req.headers, url: None, method: &req.method };

    if !needs_issuer_keys(&kind) {
        return Ok(verify_push(&kind, &push, &secrets, now_unix)?);
    }
    let VerifyKind::Oidc { issuers, jwks_url } = &kind else {
        unreachable!("needs_issuer_keys is true for the oidc scheme only");
    };
    let token = bearer_token(&req.headers)
        .ok_or_else(|| VerifyError::MissingHeader("Authorization".into()))?;
    // Signature, issuer and expiry always; the audience only when the
    // connection names one (a token minted for a specific app). The
    // claims ARE the identity: the program reads `sub`, `email`, roles,
    // whatever the issuer put there, off the request's `caller`.
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
    validation.set_issuer(&issuers.iter().map(|t| t.0.as_str()).collect::<Vec<_>>());
    match &secrets.audience {
        Some(aud) => validation.set_audience(&[aud.as_str()]),
        None => validation.validate_aud = false,
    }
    let claims: Value = crate::events::decode_jwt(token, &validation, &jwks_url.0).await?;
    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_change_drops_the_verifiers_it_can_reach() {
        let (project, other) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let key = |tenant: &str, for_instance: Option<uuid::Uuid>| VerifierKey {
            tenant: tenant.into(),
            for_instance: for_instance.map(|p| (p, weft_core::instance::InstanceId::new("ann").unwrap())),
            access_id: uuid::Uuid::from_u128(9),
            service: "api_key_auth".into(),
        };
        assert!(names("tenant:t", &key("t", None)), "a shared connection of the tenant");
        assert!(names("tenant:t", &key("t", Some(project))), "an instance may use the tenant's shared ones too");
        assert!(!names("tenant:t", &key("u", None)), "another tenant's");
        assert!(names(&project.to_string(), &key("t", Some(project))), "the project's instances");
        assert!(!names(&project.to_string(), &key("t", None)), "an instance's connection is never the author's");
        assert!(!names(&project.to_string(), &key("t", Some(other))), "another project's instances");
        assert!(names("garbled", &key("t", None)), "a payload naming nothing reaches everything");
    }

    /// jsonwebtoken carries no crypto of its own; a build without a backend
    /// feature compiles and then panics at the first signature it checks,
    /// which is how every genuine token once took the broker down.
    #[test]
    fn the_jwt_library_has_a_crypto_backend() {
        let key = jsonwebtoken::EncodingKey::from_secret(b"k");
        let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), &serde_json::json!({ "sub": "x", "exp": 4_000_000_000u64 }), &key)
            .expect("signs");
        let data = jsonwebtoken::decode::<serde_json::Value>(
            &token,
            &jsonwebtoken::DecodingKey::from_secret(b"k"),
            &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256),
        )
        .expect("verifies");
        assert_eq!(data.claims["sub"], "x");
    }
}
