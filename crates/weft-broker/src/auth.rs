//! Caller identity: verify the bearer through the platform's
//! `CallerIdentity`, cache what it proved, and interpret it per endpoint.
//!
//! The platform answers WHO a token is (`Principal`: one of weft's own
//! roles, or one project's worker). What that caller may do here is this
//! module's: a worker acts for its own project only; a weft role acts for
//! any tenant, and says which of its surfaces a call is for in the
//! `ROLE_HEADER`. The process replica a call comes from travels in the
//! `REPLICA_HEADER`, and that is what ties a worker's claims, the execution it
//! drives and the journal rows it writes to one running copy.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{FromRequestParts, State},
    http::{request::Parts, HeaderMap, StatusCode},
};
use hmac::{Hmac, Mac};
use lru::LruCache;
use parking_lot::Mutex;
use sha2::Sha256;
use weft_platform_traits::identity::{Principal, REPLICA_HEADER, ROLE_HEADER};
use weft_platform_traits::CoreRole;

use crate::state::BrokerState;

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// The base URLs callers address the broker by (a worker in a
    /// container and a role on the machine may use different ones). A
    /// platform whose tokens name their audience checks it against these.
    pub audiences: Vec<String>,
}

/// What a caller is to the broker's data surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The listener: holds signals belonging to every tenant and fires
    /// them. Runs weft's code only, so it acts for any tenant; per fire
    /// it still proves the signal exists and the task's tenant is the
    /// signal's.
    Listener,
    /// One project's worker: runs the user's compiled program (including
    /// untrusted ExecPython), so it acts for that project only.
    Worker,
    /// The supervisor: applies every tenant's infra. Runs weft's code
    /// only; per op it proves the project it acts on is real and uses
    /// that project's tenant.
    InfraSupervisor,
}

// NOTE: there is deliberately no `Infra` role. The agent beside an infra
// copy (`Principal::InfraCopy`) asks the broker for two things, each
// checked by its own handler: a look at its project's health
// (`/v1/infra/look`) and a push of its copy's values (`/v1/infra/pushed`).
// Every other surface refuses it (`infra_copy_refused`). A unit's
// endpoints are resolved by the WORKER via `ctx.endpoint()`, and its
// lifecycle is the supervisor's.

/// The tenant authority of a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerScope {
    /// A worker, pinned to ONE project of ONE tenant, both from its
    /// verified identity, never from anything it sends.
    Tenant { tenant: String, project: uuid::Uuid },
    /// One of weft's own roles: acts for any tenant, validated per op.
    ControlPlane,
}

impl CallerScope {
    /// The tenant this caller is pinned to, or `None` for a control-plane
    /// caller.
    pub fn pinned_tenant(&self) -> Option<&str> {
        match self {
            Self::Tenant { tenant, .. } => Some(tenant),
            Self::ControlPlane => None,
        }
    }

    /// The project this caller is pinned to, or `None` for a
    /// control-plane caller.
    pub fn pinned_project(&self) -> Option<uuid::Uuid> {
        match self {
            Self::Tenant { project, .. } => Some(*project),
            Self::ControlPlane => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CallerIdentity {
    pub scope: CallerScope,
    pub role: Role,
    /// The calling process replica (`REPLICA_HEADER`). Required from a
    /// worker: its records, its claims and the runs it drives are all
    /// bound to it.
    pub replica: Option<String>,
}

/// Cache key: HMAC-SHA-256 of the bearer token under a per-process random
/// key, so a memory dump of the cache yields nothing reversible to a
/// token.
type TokenHash = [u8; 32];

type HmacSha256 = Hmac<Sha256>;

/// Bounded so a flood of forged tokens cannot grow the broker's memory.
const CACHE_CAPACITY: usize = 4096;

pub struct IdentityCache {
    inner: Mutex<LruCache<TokenHash, (Principal, Instant)>>,
    hmac_key: [u8; 32],
    /// How stale a revoked-but-cached token can be.
    ttl: Duration,
}

impl IdentityCache {
    /// Hard-fails if the OS RNG is unavailable: a guessable cache key
    /// defeats the point of hashing tokens.
    pub fn new() -> anyhow::Result<Self> {
        let mut hmac_key = [0u8; 32];
        getrandom::getrandom(&mut hmac_key)
            .map_err(|e| anyhow::anyhow!("OS RNG unavailable for identity cache key: {e}"))?;
        let cap = NonZeroUsize::new(CACHE_CAPACITY).expect("non-zero cache capacity");
        Ok(Self { inner: Mutex::new(LruCache::new(cap)), hmac_key, ttl: Duration::from_secs(30) })
    }

    pub fn get(&self, token: &str) -> Option<Principal> {
        let key = self.hash(token);
        let mut cache = self.inner.lock();
        let (principal, at) = cache.get(&key)?;
        if at.elapsed() < self.ttl {
            Some(principal.clone())
        } else {
            cache.pop(&key);
            None
        }
    }

    pub fn put(&self, token: &str, principal: Principal) {
        let key = self.hash(token);
        self.inner.lock().put(key, (principal, Instant::now()));
    }

    fn hash(&self, token: &str) -> TokenHash {
        let mut mac = HmacSha256::new_from_slice(&self.hmac_key).expect("HMAC-SHA-256 accepts any key length");
        mac.update(token.as_bytes());
        mac.finalize().into_bytes().into()
    }
}

/// Verify the bearer (cached) and return WHO it is:
///   - missing / empty bearer  -> 401
///   - refused by the platform -> 401 (the reason is logged, not sent)
///   - the check could not run -> 503
pub async fn verified_principal(state: &Arc<BrokerState>, headers: &HeaderMap) -> Result<Principal, (StatusCode, String)> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .filter(|t| !t.is_empty())
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".into()))?;
    if let Some(cached) = state.identity_cache.get(token) {
        return Ok(cached);
    }
    let principal = state.identity.verify(token, &state.auth.audiences).await.map_err(|refused| {
        tracing::warn!(target: "weft_broker::auth", reason = %refused, "caller refused");
        match refused {
            weft_platform_traits::IdentityRefused::Unavailable(why) => {
                (StatusCode::SERVICE_UNAVAILABLE, format!("the identity check could not run: {why}"))
            }
            _ => (StatusCode::UNAUTHORIZED, "the bearer token was not accepted".to_string()),
        }
    })?;
    state.identity_cache.put(token, principal.clone());
    Ok(principal)
}

/// The replica a call comes from, when it named one.
fn replica_of(headers: &HeaderMap) -> Option<String> {
    headers.get(REPLICA_HEADER).and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty()).map(str::to_string)
}

/// The role a weft role calls as.
fn role_of(headers: &HeaderMap) -> Result<CoreRole, (StatusCode, String)> {
    let raw = headers
        .get(ROLE_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or((StatusCode::BAD_REQUEST, format!("a weft role names itself in the {ROLE_HEADER} header")))?;
    CoreRole::parse(raw).map_err(|e| (StatusCode::BAD_REQUEST, e))
}

/// What a verified caller is to the broker's data surface.
pub(crate) fn interpret(principal: Principal, role: Option<CoreRole>, replica: Option<String>) -> Result<CallerIdentity, (StatusCode, String)> {
    match principal {
        Principal::Worker { tenant, project } => {
            let replica = replica.ok_or((
                StatusCode::BAD_REQUEST,
                format!("a worker names its process replica in the {REPLICA_HEADER} header"),
            ))?;
            Ok(CallerIdentity { scope: CallerScope::Tenant { tenant, project }, role: Role::Worker, replica: Some(replica) })
        }
        Principal::Core => {
            let role = match role {
                Some(CoreRole::Listener) => Role::Listener,
                Some(CoreRole::Supervisor) => Role::InfraSupervisor,
                Some(other) => {
                    return Err((StatusCode::FORBIDDEN, format!("the {other} has no broker data role")))
                }
                None => return Err((StatusCode::BAD_REQUEST, format!("a weft role names itself in the {ROLE_HEADER} header"))),
            };
            Ok(CallerIdentity { scope: CallerScope::ControlPlane, role, replica })
        }
        Principal::InfraCopy { .. } => Err(infra_copy_refused()),
    }
}

/// An infra copy's agent may only look at its project's health
/// (`/v1/infra/look`) and push its own copy's values (`/v1/infra/pushed`).
pub(crate) fn infra_copy_refused() -> (StatusCode, String) {
    (
        StatusCode::FORBIDDEN,
        "an infra copy's agent may only look at its project's health (/v1/infra/look) and push its own copy's values (/v1/infra/pushed)".into(),
    )
}

/// The broker surface's identity: a worker, the listener or the
/// supervisor.
pub async fn extract_identity(state: &Arc<BrokerState>, headers: &HeaderMap) -> Result<CallerIdentity, (StatusCode, String)> {
    let principal = verified_principal(state, headers).await?;
    let role = match principal {
        Principal::Core => Some(role_of(headers)?),
        Principal::Worker { .. } | Principal::InfraCopy { .. } => None,
    };
    interpret(principal, role, replica_of(headers))
}

/// Resolve a runtime-storage caller into the pure key-wall identity
/// (`CallerAuth`), the identity behind the runtime-file plane's prefix
/// wall:
///   - the dispatcher -> ControlPlane (the CLI admin verbs).
///   - a worker -> Worker { tenant, project, execution }, verifying any
///     claimed `execution_id` the way its records are: the run must be the
///     caller's project's, running, and driven by the calling replica.
/// Any other weft role has no runtime-storage identity (403).
pub async fn resolve_storage_caller(
    state: &Arc<BrokerState>,
    headers: &HeaderMap,
    execution_id: Option<&str>,
) -> Result<weft_core::storage::key::CallerAuth, (StatusCode, String)> {
    use weft_core::storage::key::CallerAuth;
    match verified_principal(state, headers).await? {
        Principal::Core => match role_of(headers)? {
            CoreRole::Dispatcher => Ok(CallerAuth::ControlPlane),
            other => Err((StatusCode::FORBIDDEN, format!("the {other} has no runtime-storage identity"))),
        },
        Principal::Worker { tenant, project } => {
            let replica = replica_of(headers);
            let (execution_id, instance) = match execution_id {
                None => (None, None),
                Some(execution_id) => {
                    let parsed: weft_core::ExecutionId =
                        execution_id.parse().map_err(|e| (StatusCode::BAD_REQUEST, format!("not an execution id: {e}")))?;
                    // The worker writes a run's record before any call that
                    // names it, so the row is there.
                    let row = sqlx::query_as::<_, (String, uuid::Uuid, Option<String>, Option<String>)>(
                        "SELECT tenant_id, project_id, owner, instance_id FROM run WHERE execution_id = $1 AND state = 'running'",
                    )
                    .bind(parsed)
                    .fetch_optional(&state.pool)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
                    let Some((execution_id_tenant, execution_id_project, owner, instance)) = row else {
                        return Err((StatusCode::FORBIDDEN, "no such run is running".into()));
                    };
                    if execution_id_tenant != tenant || execution_id_project != project {
                        tracing::warn!(
                            target: "weft_broker::scope",
                            caller_project = %project,
                            execution_id = %execution_id,
                            "runtime storage rejected cross-project execution claim"
                        );
                        return Err((StatusCode::FORBIDDEN, "execution belongs to a different project".into()));
                    }
                    // Same gate as its records: only the replica driving the
                    // run acts for it.
                    if replica.is_none() || owner.as_deref() != replica.as_deref() {
                        return Err((StatusCode::FORBIDDEN, "execution is not driven by the calling replica".into()));
                    }
                    (Some(execution_id.to_string()), instance)
                }
            };
            Ok(CallerAuth::Worker { tenant, project_id: project.to_string(), execution_id, instance })
        }
        Principal::InfraCopy { .. } => Err(infra_copy_refused()),
    }
}

/// Require the dispatcher, for an admin request (runtime-storage admin,
/// access admin).
pub(crate) async fn control_plane(state: &Arc<BrokerState>, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    match resolve_storage_caller(state, headers, None).await? {
        weft_core::storage::key::CallerAuth::ControlPlane => Ok(()),
        weft_core::storage::key::CallerAuth::Worker { .. } | weft_core::storage::key::CallerAuth::Tenant { .. } => {
            Err((StatusCode::FORBIDDEN, "the admin surface is dispatcher-only".into()))
        }
    }
}

/// Axum extractor: the handler signs `(State, AuthedCaller, Json<...>)`.
pub struct AuthedCaller(pub CallerIdentity);

impl<S> FromRequestParts<S> for AuthedCaller
where
    S: Send + Sync,
    Arc<BrokerState>: axum::extract::FromRef<S>,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let State(broker): State<Arc<BrokerState>> = State::from_request_parts(parts, state)
            .await
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "broker state missing".into()))?;
        let identity = extract_identity(&broker, &parts.headers).await?;
        Ok(AuthedCaller(identity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker() -> Principal {
        Principal::Worker { tenant: "t".into(), project: uuid::Uuid::from_u128(1) }
    }

    #[test]
    fn a_worker_is_pinned_to_its_project_and_needs_its_replica() {
        let id = interpret(worker(), None, Some("w-1".into())).unwrap();
        assert_eq!(id.role, Role::Worker);
        assert_eq!(id.scope.pinned_project(), Some(uuid::Uuid::from_u128(1)));
        assert_eq!(id.replica.as_deref(), Some("w-1"));
        assert_eq!(interpret(worker(), None, None).unwrap_err().0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn a_weft_role_carries_its_broker_role_or_is_refused() {
        assert_eq!(interpret(Principal::Core, Some(CoreRole::Listener), None).unwrap().role, Role::Listener);
        assert_eq!(interpret(Principal::Core, Some(CoreRole::Supervisor), None).unwrap().role, Role::InfraSupervisor);
        assert_eq!(interpret(Principal::Core, Some(CoreRole::Dispatcher), None).unwrap_err().0, StatusCode::FORBIDDEN);
        assert_eq!(interpret(Principal::Core, None, None).unwrap_err().0, StatusCode::BAD_REQUEST);
        assert_eq!(interpret(Principal::Core, Some(CoreRole::Listener), None).unwrap().scope, CallerScope::ControlPlane);
    }

    #[test]
    fn the_cache_remembers_a_principal_by_its_token() {
        let cache = IdentityCache::new().unwrap();
        assert!(cache.get("tok").is_none());
        cache.put("tok", worker());
        assert_eq!(cache.get("tok"), Some(worker()));
        assert!(cache.get("other").is_none());
    }
}
