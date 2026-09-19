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
    match verify_caller(&state.pool, &req, now).await {
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

/// The check itself, over the pool: load the connection's scheme and
/// values, resolve the scheme's templates against the values, run it.
/// Answers the identity the scheme established. Separate from the route
/// so the db-tests exercise it directly.
pub async fn verify_caller(
    pool: &PgPool,
    req: &CallerVerifyRequest,
    now_unix: i64,
) -> Result<Value, CallerRefusal> {
    let access_id: uuid::Uuid = req
        .access_id
        .parse()
        .map_err(|_| CallerRefusal::Failed(anyhow::anyhow!("malformed connection id '{}'", req.access_id)))?;
    let verifier =
        weft_access_store::caller_verifier(pool, &req.tenant, access_id, &req.service)
            .await
            .map_err(|e| match e.downcast_ref::<weft_access_store::AccessError>() {
                // A connection that is not there (or another tenant's,
                // which reads the same) cannot admit anyone.
                Some(weft_access_store::AccessError::NotFound) => {
                    CallerRefusal::Refused("no such connection".into())
                }
                _ => CallerRefusal::Failed(e),
            })?;
    let Some(kind) = verifier.verify else {
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
