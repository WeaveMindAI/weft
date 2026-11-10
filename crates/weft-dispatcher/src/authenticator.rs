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
//! Every install, local or cloud, runs `OperatorKeyAuthenticator`: every
//! admin request carries an operator key (`Authorization: Bearer wft-...`).
//! One is the install's own, kept among its secrets
//! (`weft_core::signal_token::INSTALL_KEY_SECRET`) and checked against its
//! hash, never stored: retired by rotating the secret. Every other is a row
//! of the signal-token table with kind `operator` (`weft token mint
//! --operator`, `weft target export`), looked up by its hash on every
//! request, so a revoked key stops working on the next request on every
//! replica. A local install is no exception: being on loopback keeps the
//! internet out, never a web page the person has open (its browser reaches
//! loopback too).
//!
//! The check runs as a LAYER over the whole admin surface
//! ([`require_caller`]), never per handler: a route added to that surface
//! without thinking about auth is still behind it. The handler-side
//! extractor ([`CallerTenant`]) only reads what the layer established.

use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::store::{RunStore, TokenStore};
use weft_core::signal_token::TokenKind;
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
}

/// Authenticates an admin request. `tokens` is where the install keeps its
/// hashed credentials.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, headers: &HeaderMap, tokens: &dyn TokenStore) -> Result<Caller, AuthError>;
}

/// The install's authenticator: the request's bearer must be the install's
/// own key or an operator key of the install's one tenant (one team per
/// install).
pub struct OperatorKeyAuthenticator {
    pub tenant: TenantId,
    /// The hash of the install's own key ([`weft_core::signal_token::token_hash`]).
    install_key_hash: String,
}

impl OperatorKeyAuthenticator {
    /// For the install whose own key is `install_key`. An empty one is
    /// refused: it would let no request in, or every one.
    pub fn new(tenant: TenantId, install_key: &str) -> anyhow::Result<Self> {
        let install_key = install_key.trim();
        anyhow::ensure!(
            !install_key.is_empty(),
            "{} is empty: the install has no key of its own to accept",
            weft_core::signal_token::INSTALL_KEY_SECRET
        );
        Ok(Self { tenant, install_key_hash: weft_core::signal_token::token_hash(install_key) })
    }
}

#[async_trait::async_trait]
impl Authenticator for OperatorKeyAuthenticator {
    async fn authenticate(&self, headers: &HeaderMap, tokens: &dyn TokenStore) -> Result<Caller, AuthError> {
        let presented = bearer(headers)?;
        let hash = weft_core::signal_token::token_hash(presented);
        if hash == self.install_key_hash {
            return Ok(Caller { tenant: self.tenant.clone() });
        }
        let row = tokens
            .get_signal_token(&hash)
            .await
            .map_err(|e| AuthError::Invalid(format!("token lookup failed: {e}")))?
            .ok_or_else(|| AuthError::Invalid("unknown token".into()))?;
        check_operator(&row, &self.tenant, crate::lease::now_unix() as u64)?;
        Ok(Caller { tenant: self.tenant.clone() })
    }
}

/// The rule [`OperatorKeyAuthenticator`] applies to a found row, apart so it
/// is testable without a store: an operator key of this install's tenant.
/// A caller token (a frontend's) is refused here whatever its scope.
fn check_operator(row: &crate::store::SignalToken, tenant: &TenantId, now: u64) -> Result<(), AuthError> {
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
    match state.authenticator.authenticate(request.headers(), &*state.tokens).await {
        Ok(caller) => {
            request.extensions_mut().insert(caller);
            next.run(request).await
        }
        Err(e) => {
            tracing::debug!(target: "weft_dispatcher::auth", error = %e, "request rejected");
            (
                StatusCode::UNAUTHORIZED,
                "unauthorized: this install takes an operator key; for a target, `weft login <target>`; for this machine's own \
                 install, the CLI sends the key in its secrets.env, which `./setup.sh` writes",
            )
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

/// What a caller is told when no project they may see holds an id.
/// "Never registered" and "belongs to somebody else" get the SAME
/// words, so nobody can probe which ids exist elsewhere. The words name
/// the usual cause, because a bare "not found" leaves a person staring
/// at a project that is plainly right there on their disk: the
/// dispatcher has simply never been told about it.
pub const NO_SUCH_PROJECT: &str = "this dispatcher holds no project under that id; \
     if the project is new, `weft run`, `weft infra start` or `weft activate` registers it";

/// Authorize a caller against a project: the project must exist AND belong to
/// the caller's tenant. Returns the same `NOT_FOUND` for "no such project" and
/// "exists but belongs to another tenant" so a caller cannot probe which
/// project ids exist in other tenants (no existence leak); `SERVICE_UNAVAILABLE`
/// only when the database did not take the read right now, which the
/// caller asks again on, and `INTERNAL_SERVER_ERROR` for any other failure
/// (`crate::api::unavailable_or_internal`).
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
        Err(e) => Err(crate::api::unavailable_or_internal("read who owns this project", e.context(format!("project {id}")))),
    }
}

/// Authorize a caller against a run (identified by its run):
/// the tenant STAMPED on the run when it was born must be the
/// caller's. Returns the run's owner (tenant + project id), which
/// handlers need next.
///
/// Deliberately NOT `authorize_project` on the run's project: a
/// run outlives its project (its record is the record of what
/// ran, and `weft rm` takes only the project), so asking the project
/// store made every run of a removed project un-replayable and
/// UNDELETABLE, with `weft clean` (the documented way to remove them)
/// refused on the rows that needed it most. The stamped tenant is
/// equally strict, because a project cannot change tenant, and it
/// cannot be deleted out from under the row.
///
/// A run with no `run` row is `NOT_FOUND`, the same
/// answer as a cross-tenant run, so neither leaks the other's
/// existence.
/// Takes the RUN STORE, not the whole state: the run's own row is
/// the only thing this consults, and saying so in the signature is
/// what keeps a future edit from reaching for the project store again.
pub async fn authorize_run(
    runs: &dyn RunStore,
    caller: &TenantId,
    run_id: weft_core::RunId,
) -> Result<crate::store::RunOwner, (StatusCode, String)> {
    let owner = match runs.run_owner(run_id).await {
        Ok(Some(o)) => o,
        Ok(None) => return Err((StatusCode::NOT_FOUND, "not found".to_string())),
        Err(e) => {
            return Err(crate::api::unavailable_or_internal("read who owns this run", e.context(format!("run {run_id}"))))
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
    use crate::store::SignalToken;

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
            instance: None,
            connections: false,
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

    fn bearing(key: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("authorization", format!("Bearer {key}").parse().unwrap());
        h
    }

    #[tokio::test]
    async fn an_operator_key_authenticates_until_revoked() {
        let tokens = crate::store::FakeStore::default();
        let key = weft_core::signal_token::generate_token();
        let mut minted = row(TokenKind::Operator, "local");
        minted.token_hash = weft_core::signal_token::token_hash(&key);
        tokens.mint_signal_token(&minted).await.unwrap();
        let auth = OperatorKeyAuthenticator::new(TenantId::local(), &weft_core::signal_token::generate_token()).unwrap();
        assert!(auth.authenticate(&bearing(&key), &tokens).await.is_ok());
        assert!(tokens.revoke_signal_token(minted.id, "local").await.unwrap());
        assert!(auth.authenticate(&bearing(&key), &tokens).await.is_err(), "revocation is immediate");
    }

    /// The install's own key is accepted with nothing in the store, and
    /// the store never learns it; anything else is refused, a missing
    /// key first of all.
    #[tokio::test]
    async fn the_installs_own_key_is_accepted_without_a_row() {
        let tokens = crate::store::FakeStore::default();
        let own = weft_core::signal_token::generate_token();
        let auth = OperatorKeyAuthenticator::new(TenantId::local(), &own).unwrap();
        assert_eq!(auth.authenticate(&bearing(&own), &tokens).await, Ok(Caller { tenant: TenantId::local() }));
        assert!(tokens.list_signal_tokens("local").await.unwrap().is_empty());
        assert!(matches!(auth.authenticate(&bearing("wft-not-it"), &tokens).await, Err(AuthError::Invalid(_))));
        assert_eq!(auth.authenticate(&HeaderMap::new(), &tokens).await, Err(AuthError::Missing));
        assert!(OperatorKeyAuthenticator::new(TenantId::local(), "  ").is_err(), "no empty key");
    }
}
