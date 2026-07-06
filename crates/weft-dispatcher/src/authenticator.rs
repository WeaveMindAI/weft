//! Caller authentication: turning an HTTP request into the tenant making it.
//!
//! Every USER-facing dispatcher operation acts on behalf of one tenant, and the
//! tenant must come from the REQUEST (who is calling), never from the resource
//! (which tenant owns the project). Those are two different questions:
//!   - `TenantRouter::tenant_for_project` answers "which tenant owns project X",
//!     used by background loops (cold-start, reaper) that have no request.
//!   - `Authenticator` answers "which tenant is making THIS request", used at
//!     the HTTP edge to scope and gate every user operation.
//!
//! The default authenticator returns `local` for every request: no token needed.
//! An impl that verifies a signed token instead derives the real tenant WITHOUT
//! calling out to an auth service per request: the token is signed by the auth
//! service's private key and the dispatcher verifies it with the matching public
//! key it holds, so a valid signature alone proves identity. That is the only
//! shape that works across many clusters (each cluster's dispatcher trusts the
//! same public key; no shared session store).

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};

use crate::state::DispatcherState;
use crate::tenant::TenantId;

/// Why a request could not be attributed to a tenant. The HTTP edge maps this
/// to a status: `Missing` and `Invalid` are 401 (the caller must present a
/// valid credential), distinct so logs can tell "sent nothing" from "sent
/// something bad" without leaking which to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// No credential on the request (no `Authorization` header).
    Missing,
    /// A credential was present but did not verify (bad signature, expired,
    /// wrong issuer/audience, malformed). The string is for server logs only.
    Invalid(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::Missing => f.write_str("missing credential"),
            AuthError::Invalid(why) => write!(f, "invalid credential: {why}"),
        }
    }
}

/// Authenticates a request to the tenant making it. The default trusts the
/// single `local` tenant; an impl that verifies a signed token derives the real
/// tenant.
pub trait Authenticator: Send + Sync {
    fn authenticate(&self, headers: &HeaderMap) -> Result<TenantId, AuthError>;

    /// Whether the request is a CONTROL-PLANE / operator caller, allowed to hit
    /// cross-tenant ops endpoints (cluster diagnostics like `listener_inspect`)
    /// that no single tenant may see. Mirrors the broker's
    /// `CallerScope::ControlPlane`. The default: the local operator IS the
    /// control plane. A token-verifying impl grants this only to its own
    /// ops/admin tokens, never to a tenant token, so a tenant can never reach
    /// cross-tenant diagnostics.
    fn is_control_plane(&self, headers: &HeaderMap) -> bool;
}

/// The built-in authenticator: every request is tenant `local`. There is no auth
/// service, so there is nothing to verify.
pub struct LocalAuthenticator;

impl Authenticator for LocalAuthenticator {
    fn authenticate(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
        Ok(TenantId::local())
    }

    fn is_control_plane(&self, _headers: &HeaderMap) -> bool {
        // The local operator IS the control plane, so ops endpoints are
        // reachable. A token-verifying impl gates this on an ops token.
        true
    }
}

pub fn local_authenticator() -> Arc<dyn Authenticator> {
    Arc::new(LocalAuthenticator)
}

/// The tenant a user-facing request is acting as, extracted at the HTTP edge.
///
/// Adding `caller: CallerTenant` to a handler is the single, type-enforced way
/// to say "this operation is scoped to the authenticated caller's tenant": axum
/// runs the `Authenticator` before the handler body, so a request that does not
/// authenticate never reaches handler logic. The inner `TenantId` then threads
/// into every tenant-scoped store call. Handlers that authenticate by another
/// mechanism (signal token, broker box identity) do NOT use this
/// extractor; they keep their own gate.
pub struct CallerTenant(pub TenantId);

impl FromRequestParts<DispatcherState> for CallerTenant {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &DispatcherState,
    ) -> Result<Self, Self::Rejection> {
        match state.authenticator.authenticate(&parts.headers) {
            Ok(tenant) => Ok(CallerTenant(tenant)),
            // Both arms are 401: the caller must present a valid credential.
            // The reason string is logged server-side; the body stays generic
            // so it does not reveal whether a token was absent vs rejected.
            Err(e) => {
                tracing::debug!(target: "weft_dispatcher::auth", error = %e, "request rejected");
                Err((StatusCode::UNAUTHORIZED, "unauthorized".to_string()))
            }
        }
    }
}

/// Marks a handler as control-plane / operator only: it rejects any request the
/// `Authenticator` does not recognize as a control-plane caller with `403`.
/// Used by cross-tenant ops endpoints (cluster diagnostics) that no single
/// tenant may reach.
pub struct ControlPlaneCaller;

impl FromRequestParts<DispatcherState> for ControlPlaneCaller {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &DispatcherState,
    ) -> Result<Self, Self::Rejection> {
        if state.authenticator.is_control_plane(&parts.headers) {
            Ok(ControlPlaneCaller)
        } else {
            Err((StatusCode::FORBIDDEN, "control-plane only".to_string()))
        }
    }
}

/// Authorize a caller against a project: the project must exist AND belong to
/// the caller's tenant. Returns the same `NOT_FOUND` for "no such project" and
/// "exists but belongs to another tenant" so a caller cannot probe which
/// project ids exist in other tenants (no existence leak); `INTERNAL_SERVER_ERROR`
/// only on a real store failure.
///
/// This is the single gate every user-facing, project-scoped handler calls
/// before acting. It builds on `ProjectStore::tenant_for` (the project to tenant
/// mapping), which is also what background loops use, so there is one source of
/// truth for project ownership. List endpoints do NOT use this (they filter in
/// SQL by tenant); this is for per-resource ops keyed by a project id.
pub async fn authorize_project(
    state: &DispatcherState,
    caller: &TenantId,
    id: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    match state.projects.tenant_for(id).await {
        Ok(Some(owner)) if owner == caller.as_str() => Ok(()),
        // Missing OR cross-tenant: indistinguishable to the caller.
        Ok(_) => Err((StatusCode::NOT_FOUND, "not found".to_string())),
        Err(e) => {
            tracing::warn!(
                target: "weft_dispatcher::auth",
                project_id = %id,
                error = %e,
                "tenant_for failed during authorization"
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "authorization failed".to_string(),
            ))
        }
    }
}

/// Authorize a caller against an execution (identified by its color): the
/// execution's project must exist AND belong to the caller's tenant. Resolves
/// color to its project via the journal, then reuses `authorize_project`, so
/// execution access inherits the exact same project-ownership rule. Returns the
/// resolved project id on success (handlers often need it next).
///
/// A color with no project mapping is `NOT_FOUND` (same as cross-tenant: no
/// existence leak). A corrupt mapping (a color row pointing at no/garbage
/// project) is a real server fault, surfaced as `INTERNAL_SERVER_ERROR`.
pub async fn authorize_execution(
    state: &DispatcherState,
    caller: &TenantId,
    color: weft_core::Color,
) -> Result<String, (StatusCode, String)> {
    let project_id = match state.journal.execution_project(color).await {
        Ok(crate::journal::ColorLookup::Found(p)) => p,
        Ok(crate::journal::ColorLookup::NotFound) => {
            return Err((StatusCode::NOT_FOUND, "not found".to_string()))
        }
        Ok(crate::journal::ColorLookup::Corrupt) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "execution mapping corrupt".to_string(),
            ))
        }
        Err(e) => {
            tracing::warn!(
                target: "weft_dispatcher::auth",
                color = %color,
                error = %e,
                "execution_project failed during authorization"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "authorization failed".to_string(),
            ));
        }
    };
    let id = project_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "execution mapping corrupt".to_string()))?;
    authorize_project(state, caller, id).await?;
    Ok(project_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_authenticator_returns_local_for_any_request() {
        let auth = LocalAuthenticator;
        // No header at all.
        assert_eq!(auth.authenticate(&HeaderMap::new()).unwrap(), TenantId::local());
        // A stray Authorization header is ignored: local has one tenant.
        let mut h = HeaderMap::new();
        h.insert("authorization", "Bearer whatever".parse().unwrap());
        assert_eq!(auth.authenticate(&h).unwrap(), TenantId::local());
    }
}
