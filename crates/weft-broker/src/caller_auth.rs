//! The caller-verify door: is the party opening a live route who the
//! route's check says it must be?
//!
//! A Route (or Socket) names its check in its `auth` (a set of API keys,
//! a JWT issuer, an HMAC secret) and the stored connection whose values
//! hold the material; the door forwards both with the request.
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
use axum::Router;
use base64::Engine as _;
use serde_json::Value;
use weft_task_store::db::Pool;

use weft_core::access::events::{VerifyKind, VerifySide};
use weft_core::access::verify::{
    bearer_token, needs_issuer_keys, secrets_from_values, verify_push, PushParts, VerifyError,
};
use weft_broker_client::protocol::{CallerVerified, CallerVerifyRequest};

use crate::state::BrokerState;
use weft_core::wire_shape::Json;

type ApiError = (StatusCode, String);

pub fn routes() -> Router<Arc<BrokerState>> {
    Router::new().route("/v1/caller/verify", post(caller_verify))
}

/// POST /v1/caller/verify: check one caller of a live route against the
/// connection the route is gated by. Asked by the worker whose door the
/// caller reached, for its own tenant's connections and its own project's
/// instances; the caller of the ROUTE never reaches this door.
async fn caller_verify(
    State(state): State<Arc<BrokerState>>,
    headers_in: HeaderMap,
    Json(req): Json<CallerVerifyRequest>,
) -> Result<Json<CallerVerified>, ApiError> {
    match crate::auth::verified_principal(&state, &headers_in).await? {
        weft_platform_traits::identity::Principal::Worker { tenant, project } => {
            let own = tenant == req.tenant && req.for_instance.as_ref().is_none_or(|scope| scope.project_id == project);
            if !own {
                return Err((StatusCode::FORBIDDEN, "a worker checks callers of its own project's routes only".into()));
            }
        }
        weft_platform_traits::identity::Principal::Core => crate::auth::control_plane(&state, &headers_in).await?,
        weft_platform_traits::identity::Principal::InfraCopy { .. } => return Err(crate::auth::infra_copy_refused()),
    }
    let now = chrono::Utc::now().timestamp();
    match verify_caller(&state.verifiers, &state.pool, state.install, &req, now).await {
        Ok(verified) => Ok(Json(verified)),
        Err(CallerRefusal::Refused(e)) => {
            tracing::warn!(
                target: "weft_broker::caller_auth",
                tenant = %req.tenant, access_id = %req.access_id, path = %req.path,
                reason = %e,
                "caller refused"
            );
            Err((StatusCode::UNAUTHORIZED, "refused".into()))
        }
        Err(CallerRefusal::Malformed(why)) => Err((StatusCode::BAD_REQUEST, why)),
        Err(CallerRefusal::Failed(e)) => Err(crate::handlers::unavailable_or_internal(e.context("verify a caller"))),
    }
}

/// Why a caller was not verified: a refusal (the request did not prove
/// what the scheme demands, or the connection cannot verify anyone), an
/// ask that is not one (a malformed id or body, the asking worker's
/// fault), or a failure of ours (the store).
#[derive(Debug)]
pub enum CallerRefusal {
    Refused(String),
    Malformed(String),
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
pub type HeldVerifiers = weft_task_store::held_copy::HeldCopy<VerifierKey, std::collections::BTreeMap<String, String>>;

/// Which verifiers a notice on `weft_access` drops (its payload is
/// `weft_access_store`'s `access_notify`).
pub fn verifiers_changed(payload: &str) -> weft_task_store::held_copy::Changed<VerifierKey, std::collections::BTreeMap<String, String>> {
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

/// The check itself: load the connection's values (kept in `verifiers`
/// until they change; the connection must be of the service the route's
/// scheme reads), resolve the route's scheme's templates against them,
/// run it. Answers the identity the scheme established, and
/// the headers it read the credential from.
/// Separate from the route so the database tests exercise it directly.
pub async fn verify_caller(
    verifiers: &HeldVerifiers,
    pool: &Pool,
    install: weft_core::install::InstallKind,
    req: &CallerVerifyRequest,
    now_unix: i64,
) -> Result<CallerVerified, CallerRefusal> {
    let access_id: uuid::Uuid = req
        .access_id
        .parse()
        .map_err(|_| CallerRefusal::Malformed(format!("malformed connection id '{}'", req.access_id)))?;
    let key = VerifierKey {
        tenant: req.tenant.clone(),
        for_instance: req.for_instance.as_ref().map(|scope| (scope.project_id, scope.instance.clone())),
        access_id,
        service: req.service.clone(),
    };
    // The route's check is held to the rules a check of callers keeps,
    // and must read a connection of the service it names.
    req.verify.validate(&req.service, VerifySide::Caller).map_err(CallerRefusal::Malformed)?;
    match weft_core::access::callers::CallerScheme::of(&req.verify) {
        Some(scheme) if scheme.service() == req.service => {}
        _ => {
            return Err(CallerRefusal::Malformed(format!(
                "a {} check does not read a '{}' connection",
                req.verify.tag(),
                req.service
            )))
        }
    }
    let values = verifiers
        .get_or_load(key, || async {
            weft_access_store::caller_values(pool, install, &req.tenant, weft_access_store::GrantUser::of(req.for_instance.as_ref()), access_id, &req.service)
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
    let kind = req.verify.resolved(&values).map_err(CallerRefusal::Refused)?;
    let verified = |identity: Value| CallerVerified { identity, credential_headers: kind.credential_headers() };
    let secrets = secrets_from_values(&values);
    let body = base64::engine::general_purpose::STANDARD
        .decode(&req.body_b64)
        .map_err(|e| CallerRefusal::Malformed(format!("body_b64: {e}")))?;
    let push = PushParts { body: &body, headers: &req.headers, url: req.url.as_deref(), method: &req.method };

    if !needs_issuer_keys(&kind) {
        return Ok(verified(verify_push(&kind, &push, &secrets, now_unix)?));
    }
    let VerifyKind::Oidc { issuers, jwks_url } = &kind else {
        unreachable!("needs_issuer_keys is true for the oidc scheme only");
    };
    // The audience is what tells this gate's tokens apart from the ones
    // the same issuer mints for every other app it serves (one Auth0
    // tenant, one Firebase project); the recipe declares it required.
    let audience = secrets.audience.as_deref().ok_or(VerifyError::NotConfigured("audience"))?;
    let token = bearer_token(&req.headers).ok_or_else(|| VerifyError::Missing("Authorization header".into()))?;
    // Signature, issuer, audience and expiry. The claims ARE the
    // identity: the program reads `sub`, `email`, roles, whatever the
    // issuer put there, off the request's `caller`.
    let mut validation = weft_core::jwt::Validation::new(weft_core::jwt::Algorithm::RS256);
    validation.set_issuer(&issuers.iter().map(|t| t.0.as_str()).collect::<Vec<_>>());
    validation.set_audience(&[audience]);
    let claims: Value = crate::events::decode_jwt(token, &validation, &jwks_url.0).await?;
    Ok(verified(claims))
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

    /// jsonwebtoken carries no crypto of its own; a build that hands it none
    /// compiles and then panics at the first signature it checks, which is
    /// how every genuine token once took the broker down. `weft_core::jwt`
    /// hands it ring.
    #[test]
    fn the_jwt_library_has_a_crypto_backend() {
        let key = weft_core::jwt::EncodingKey::from_secret(b"k");
        let token = weft_core::jwt::encode(&weft_core::jwt::Header::default(), &serde_json::json!({ "sub": "x", "exp": 4_000_000_000u64 }), &key)
            .expect("signs");
        let data = weft_core::jwt::decode::<serde_json::Value>(
            &token,
            &weft_core::jwt::DecodingKey::from_secret(b"k"),
            &weft_core::jwt::Validation::new(weft_core::jwt::Algorithm::HS256),
        )
        .expect("verifies");
        assert_eq!(data.claims["sub"], "x");
    }
}
