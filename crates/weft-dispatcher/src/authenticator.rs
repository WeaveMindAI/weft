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
//! Two authenticators exist. `LocalAuthenticator` answers `local` for every
//! request with no credential: the local install is bound to loopback, so
//! whoever reaches it is the person at the machine. `OperatorKeyAuthenticator`
//! is what a shared install runs: every admin request carries an operator key
//! (`Authorization: Bearer wft-...`), a row of the signal-token table with
//! kind `operator`, looked up by its hash on every request, so a revoked key
//! stops working on the next request on every replica.
//!
//! The check runs as a LAYER over the whole admin surface
//! ([`require_caller`]), never per handler: a route added to that surface
//! without thinking about auth is still behind it. The handler-side
//! extractors ([`CallerTenant`], [`ControlPlaneCaller`]) only read what the
//! layer established.

use std::sync::Arc;

use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::journal::{Journal, TokenKind};
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
    /// A credential was present but did not verify (unknown, revoked, the
    /// wrong kind, malformed). The string is for server logs only.
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

/// Who an authenticated admin request comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub tenant: TenantId,
    /// Whether the caller may reach cross-tenant ops endpoints that no
    /// single tenant may see.
    /// Mirrors the broker's `CallerScope::ControlPlane`.
    pub control_plane: bool,
}

/// Authenticates an admin request. `tokens` is where the install keeps its
/// hashed credentials; an authenticator that needs none ignores it.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, headers: &HeaderMap, tokens: &dyn Journal) -> Result<Caller, AuthError>;
}

/// The local install's authenticator: every request is tenant `local`, the
/// control plane. There is no credential to check.
pub struct LocalAuthenticator;

#[async_trait::async_trait]
impl Authenticator for LocalAuthenticator {
    async fn authenticate(&self, _headers: &HeaderMap, _tokens: &dyn Journal) -> Result<Caller, AuthError> {
        Ok(Caller { tenant: TenantId::local(), control_plane: true })
    }
}

pub fn local_authenticator() -> Arc<dyn Authenticator> {
    Arc::new(LocalAuthenticator)
}

/// A shared install's authenticator: the request's bearer must be an operator
/// key of the install's one tenant (one team per install). The operator IS the
/// control plane of its own install.
pub struct OperatorKeyAuthenticator {
    pub tenant: TenantId,
}

#[async_trait::async_trait]
impl Authenticator for OperatorKeyAuthenticator {
    async fn authenticate(&self, headers: &HeaderMap, tokens: &dyn Journal) -> Result<Caller, AuthError> {
        let presented = bearer(headers)?;
        let hash = weft_core::signal_token::token_hash(presented);
        let row = tokens
            .get_signal_token(&hash)
            .await
            .map_err(|e| AuthError::Invalid(format!("token lookup failed: {e}")))?
            .ok_or_else(|| AuthError::Invalid("unknown token".into()))?;
        check_operator(&row, &self.tenant, crate::lease::now_unix() as u64)?;
        Ok(Caller { tenant: self.tenant.clone(), control_plane: true })
    }
}

/// The rule [`OperatorKeyAuthenticator`] applies to a found row, apart so it
/// is testable without a store: an operator key of this install's tenant.
/// A caller token (a frontend's) is refused here whatever its scope.
fn check_operator(row: &crate::journal::SignalToken, tenant: &TenantId, now: u64) -> Result<(), AuthError> {
    if row.kind != TokenKind::Operator {
        return Err(AuthError::Invalid("a caller token cannot administer the install".into()));
    }
    if row.expired(now) {
        return Err(AuthError::Invalid("an expired operator key".into()));
    }
    if row.tenant_id != tenant.as_str() {
        return Err(AuthError::Invalid("operator key of another tenant".into()));
    }
    Ok(())
}

/// The value of `Authorization: Bearer <value>`.
fn bearer(headers: &HeaderMap) -> Result<&str, AuthError> {
    let value = headers.get(axum::http::header::AUTHORIZATION).ok_or(AuthError::Missing)?;
    value
        .to_str()
        .ok()
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| AuthError::Invalid("not a bearer credential".into()))
}

/// The layer over the admin surface: authenticate, then hand the [`Caller`]
/// to the handler through the request's extensions. A refused request never
/// reaches a handler. Both refusals are the same generic 401, so the answer
/// does not reveal whether a key was absent, unknown or of the wrong kind.
pub async fn require_caller(
    State(state): State<DispatcherState>,
    mut request: Request,
    next: Next,
) -> Response {
    match state.authenticator.authenticate(request.headers(), &*state.journal).await {
        Ok(caller) => {
            request.extensions_mut().insert(caller);
            next.run(request).await
        }
        Err(e) => {
            tracing::debug!(target: "weft_dispatcher::auth", error = %e, "request rejected");
            (StatusCode::UNAUTHORIZED, "unauthorized: pass an operator key (`weft login <target>`)")
                .into_response()
        }
    }
}

fn caller_from(parts: &Parts) -> Result<&Caller, (StatusCode, String)> {
    parts.extensions.get::<Caller>().ok_or_else(|| {
        // A handler asking for its caller on a route the layer does not
        // cover: a wiring bug, refused rather than served unauthenticated.
        tracing::error!(target: "weft_dispatcher::auth", path = %parts.uri.path(), "route has no auth layer");
        (StatusCode::INTERNAL_SERVER_ERROR, "route mounted without authentication".to_string())
    })
}

/// The tenant a user-facing request is acting as, established by
/// [`require_caller`].
///
/// Adding `caller: CallerTenant` to a handler is the single, type-enforced way
/// to say "this operation is scoped to the authenticated caller's tenant". The
/// inner `TenantId` then threads into every tenant-scoped store call. Handlers
/// that authenticate by another mechanism (signal token, broker box identity)
/// do NOT use this extractor; they keep their own gate.
pub struct CallerTenant(pub TenantId);

impl FromRequestParts<DispatcherState> for CallerTenant {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &DispatcherState,
    ) -> Result<Self, Self::Rejection> {
        Ok(CallerTenant(caller_from(parts)?.tenant.clone()))
    }
}

/// Marks a handler as control-plane / operator only: it rejects any caller
/// [`require_caller`] did not establish as control plane with `403`. Used by
/// cross-tenant ops endpoints (install diagnostics) that no single tenant may
/// reach.
pub struct ControlPlaneCaller;

impl FromRequestParts<DispatcherState> for ControlPlaneCaller {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &DispatcherState,
    ) -> Result<Self, Self::Rejection> {
        if caller_from(parts)?.control_plane {
            Ok(ControlPlaneCaller)
        } else {
            Err((StatusCode::FORBIDDEN, "control-plane only".to_string()))
        }
    }
}

/// What a caller is told when no project they may see holds an id.
/// "Never registered" and "belongs to somebody else" get the SAME
/// words, so nobody can probe which ids exist elsewhere. The words name
/// the usual cause, because a bare "not found" leaves a person staring
/// at a project that is plainly right there on their disk: the
/// dispatcher has simply never been told about it.
pub const NO_SUCH_PROJECT: &str = "this dispatcher holds no project under that id; \
     if the project is new, `weft run` or `weft activate` registers it";

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
        Ok(_) => Err((StatusCode::NOT_FOUND, NO_SUCH_PROJECT.to_string())),
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

/// Authorize a caller against an execution (identified by its execution):
/// the tenant STAMPED on the execution when it was born must be the
/// caller's. Returns the execution's owner (tenant + project id), which
/// handlers need next.
///
/// Deliberately NOT `authorize_project` on the execution's project: an
/// execution outlives its project (the journal is the record of what
/// ran, and `weft rm` takes only the project), so asking the project
/// store made every execution of a removed project un-replayable and
/// UNDELETABLE, with `weft clean` (the documented way to remove them)
/// refused on the rows that needed it most. The stamped tenant is
/// equally strict, because a project cannot change tenant, and it
/// cannot be deleted out from under the row.
///
/// An execution with no `execution` row is `NOT_FOUND`, the same
/// answer as a cross-tenant execution, so neither leaks the other's
/// existence.
/// Takes the JOURNAL, not the whole state: the execution's own row is
/// the only thing this consults, and saying so in the signature is
/// what keeps a future edit from reaching for the project store again.
pub async fn authorize_execution(
    journal: &dyn crate::journal::Journal,
    caller: &TenantId,
    execution_id: weft_core::ExecutionId,
) -> Result<crate::journal::ExecutionOwner, (StatusCode, String)> {
    let owner = match journal.execution_owner(execution_id).await {
        Ok(Some(o)) => o,
        Ok(None) => return Err((StatusCode::NOT_FOUND, "not found".to_string())),
        Err(e) => {
            tracing::warn!(
                target: "weft_dispatcher::auth",
                execution_id = %execution_id,
                error = %e,
                "execution_owner failed during authorization"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "authorization failed".to_string(),
            ));
        }
    };
    if owner.tenant != caller.as_str() {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }
    Ok(owner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::SignalToken;

    fn row(kind: TokenKind, tenant: &str) -> SignalToken {
        SignalToken {
            id: uuid::Uuid::nil(),
            kind,
            token_hash: "h".into(),
            recognizer: "wft-x".into(),
            tenant_id: tenant.into(),
            name: None,
            allowed_projects: vec![],
            allowed_tags: vec![],
            allowed_displays: vec![],
            all_displays: false,
            created_at: 0,
            member: None,
            expires_at: None,
        }
    }

    #[test]
    fn only_an_operator_key_of_the_install_s_tenant_administers() {
        let tenant = TenantId::local();
        assert!(check_operator(&row(TokenKind::Operator, "local"), &tenant, 100).is_ok());
        assert!(check_operator(&row(TokenKind::Caller, "local"), &tenant, 100).is_err(), "a frontend's token");
        assert!(check_operator(&row(TokenKind::Operator, "other"), &tenant, 100).is_err());
        let lapsed = SignalToken { expires_at: Some(50), ..row(TokenKind::Operator, "local") };
        assert!(check_operator(&lapsed, &tenant, 100).is_err(), "an expired key");
    }

    #[test]
    fn the_bearer_is_read_strictly() {
        let mut h = HeaderMap::new();
        assert_eq!(bearer(&h), Err(AuthError::Missing));
        h.insert("authorization", "Basic abc".parse().unwrap());
        assert!(matches!(bearer(&h), Err(AuthError::Invalid(_))));
        h.insert("authorization", "Bearer ".parse().unwrap());
        assert!(matches!(bearer(&h), Err(AuthError::Invalid(_))));
        h.insert("authorization", "Bearer wft-a-b".parse().unwrap());
        assert_eq!(bearer(&h), Ok("wft-a-b"));
    }

    #[tokio::test]
    async fn local_answers_local_for_any_request() {
        let journal = crate::journal::fake::FakeJournal::default();
        let mut h = HeaderMap::new();
        h.insert("authorization", "Bearer whatever".parse().unwrap());
        let caller = LocalAuthenticator.authenticate(&h, &journal).await.unwrap();
        assert_eq!(caller, Caller { tenant: TenantId::local(), control_plane: true });
    }

    #[tokio::test]
    async fn an_operator_key_authenticates_until_revoked() {
        let journal = crate::journal::fake::FakeJournal::default();
        let key = weft_core::signal_token::generate_token();
        let mut minted = row(TokenKind::Operator, "local");
        minted.token_hash = weft_core::signal_token::token_hash(&key);
        journal.mint_signal_token(&minted).await.unwrap();
        let auth = OperatorKeyAuthenticator { tenant: TenantId::local() };
        let mut h = HeaderMap::new();
        h.insert("authorization", format!("Bearer {key}").parse().unwrap());
        assert!(auth.authenticate(&h, &journal).await.is_ok());
        assert!(journal.revoke_signal_token(minted.id, "local").await.unwrap());
        assert!(auth.authenticate(&h, &journal).await.is_err(), "revocation is immediate");
    }
}
