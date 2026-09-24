//! Caller identity: extract + validate the projected SA token, and
//! cache the REVIEWED cryptographic identity (`ReviewedToken`: who the
//! token is, no role/tenant attached) so the hot path doesn't hit the
//! k8s API on every request. Role + tenant interpretation runs per
//! endpoint ON TOP of a cache hit, so endpoints with different caller
//! universes (the storage-authorize path admits the dispatcher + box
//! SAs, not just the role table) share one TokenReview + one cache.

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

use crate::state::BrokerState;

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Audience claim every projected SA token must carry.
    pub audience: String,
    /// The namespace this broker's own install runs its control plane
    /// in (`weft_core::infra::Instance::system_namespace`). The only
    /// namespace whose dispatcher, listener and supervisor accounts
    /// count as control plane: an account of the same name in another
    /// install on the same cluster verifies just as well at TokenReview,
    /// and must still be a stranger here.
    pub system_namespace: String,
}

// Service-account names, defined ONCE.
// `from_sa_name` and `classify_caller` both branch on these; without
// shared consts a rename would update one site and silently break the
// other (e.g. workers losing their storage identity).
pub(crate) const WORKER_SA: &str = "weft-worker-sa";
pub(crate) const LISTENER_SA: &str = "weft-listener-sa";
pub(crate) const INFRA_SUPERVISOR_SA: &str = "weft-infra-supervisor-sa";
pub(crate) const DISPATCHER_SA: &str = "weft-dispatcher";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Pooled listener: a trusted control-plane service that holds
    /// signals belonging to MANY tenants and fires held events for any
    /// of them. Runs our code only (every kind handler is data-only,
    /// never executes user code), so it is trusted to act cross-tenant.
    /// Its scope is `ControlPlane`; per-fire it still proves the signal
    /// exists and the task's tenant is the signal's real tenant.
    Listener,
    /// Per-execution worker: runs the user's compiled project (including
    /// untrusted ExecPython). Scoped to exactly its own tenant; the
    /// broker never lets it act cross-tenant.
    Worker,
    /// Pooled infra-supervisor: a trusted control-plane service that
    /// reconciles infrastructure for MANY tenants' namespaces. Runs our
    /// code only (declarative manifests it compiled from the typed infra
    /// surface, confined to the caller's own namespace by construction),
    /// so it is trusted to act cross-tenant.
    /// Its scope is `ControlPlane`; per-op it proves the project/
    /// namespace it acts on is real and uses that resource's tenant.
    InfraSupervisor,
}

impl Role {
    /// Whether this role is a trusted control-plane service (acts for
    /// any tenant, scope = ControlPlane) vs a tenant-scoped pod (acts
    /// only for its own tenant). Listener + supervisor are pooled
    /// trusted services; the worker is tenant-scoped because it runs
    /// untrusted user code.
    fn is_control_plane(self) -> bool {
        match self {
            Self::Listener | Self::InfraSupervisor => true,
            // The worker runs untrusted user code, so it is tenant-scoped: the
            // broker never lets it act cross-tenant.
            Self::Worker => false,
        }
    }
}

// NOTE: there is deliberately no `Infra` role. Pods the supervisor
// brings up from an `InfraSpec` (`weft-infra-sa`) never talk to the
// broker: their endpoint URLs are resolved by the WORKER via
// `ctx.endpoint()` (the broker's `/infra/endpoint_url`, Worker|Listener
// only), and their lifecycle is the supervisor's job. So `weft-infra-sa`
// has no SA-name mapping here; an infra pod that somehow presented a
// token would fail role resolution (403), which is correct.

impl Role {
    fn from_sa_name(sa: &str) -> Option<Self> {
        match sa {
            LISTENER_SA => Some(Self::Listener),
            WORKER_SA => Some(Self::Worker),
            INFRA_SUPERVISOR_SA => Some(Self::InfraSupervisor),
            _ => None,
        }
    }
}

/// The tenant authority of a caller. A `Tenant` caller may act ONLY
/// for that tenant (the worker, running untrusted user code). A
/// `ControlPlane` caller (pooled listener / supervisor, trusted, runs
/// our code only) may act for ANY tenant; the broker still validates
/// per-op that the specific resource exists and uses the resource's
/// own tenant for writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerScope {
    /// A worker, pinned to ONE project of ONE tenant. Both are read
    /// from the pod's own kubelet-stamped identity and the row the
    /// dispatcher wrote for it, never from anything the pod supplies,
    /// so a handler can hold a request to them.
    Tenant { tenant: String, project: uuid::Uuid },
    ControlPlane,
}

impl CallerScope {
    /// The tenant this caller is pinned to, or `None` for a
    /// control-plane caller that is not pinned to any single tenant.
    pub fn pinned_tenant(&self) -> Option<&str> {
        match self {
            Self::Tenant { tenant, .. } => Some(tenant),
            Self::ControlPlane => None,
        }
    }

    /// The project this caller is pinned to, or `None` for a
    /// control-plane caller that acts for any of them.
    pub fn pinned_project(&self) -> Option<uuid::Uuid> {
        match self {
            Self::Tenant { project, .. } => Some(*project),
            Self::ControlPlane => None,
        }
    }
}

// NOTE: the dispatcher (`weft-dispatcher` in `weft-system`) is deliberately
// NOT in the role table: it never calls the broker's tenant data endpoints. It
// appears only on the runtime-file plane's caller resolution (see
// `resolve_storage_caller`), which maps it to the control plane from the raw
// reviewed token.

/// The cached output of a TokenReview: who the token cryptographically
/// is, with no role/tenant interpretation attached. Interpretation
/// (role table, tenant lookup) happens per endpoint ON TOP of this,
/// so endpoints with different caller universes (the runtime-storage
/// caller resolution admits the dispatcher as the control plane) share one
/// review + one cache.
#[derive(Debug, Clone)]
pub struct ReviewedToken {
    pub sa_name: String,
    pub namespace: String,
    pub pod_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CallerIdentity {
    /// The caller's tenant authority. `Tenant(t)` for a worker (acts
    /// only for t); `ControlPlane` for a pooled listener / supervisor
    /// (acts for any tenant, validated per-op).
    pub scope: CallerScope,
    pub role: Role,
    pub namespace: String,
    /// `pod_name` claimed inside the SA token (extra projection); the
    /// kubelet stamps the bound pod's name into the token's
    /// `kubernetes.io/pod` claim. Used to bind journal/worker_pod
    /// writes to the actual sender pod.
    pub pod_name: Option<String>,
}

/// Cache key: HMAC-SHA-256 of the bearer token under a per-process
/// random key. The HMAC key never leaves the broker's address space,
/// so a memory dump that captures the cache map alone yields
/// ciphertext that's not feasible to reverse to plaintext tokens
/// (defeats offline rainbow-table attacks against captured cache
/// state). Plain SHA-256 wouldn't, since SA tokens are compact JWTs
/// over a known character set.
type TokenHash = [u8; 32];

type HmacSha256 = Hmac<Sha256>;

/// Bounded entries: the cache is keyed by token, so a tenant rotating
/// its SA tokens or a flood of forged-then-rejected tokens cannot
/// blow the broker's memory. 4096 caps RSS at a few MB of identity
/// rows even under churn, which dwarfs realistic working sets (one
/// entry per live pod per role per tenant).
const CACHE_CAPACITY: usize = 4096;

pub struct IdentityCache {
    inner: Mutex<LruCache<TokenHash, (ReviewedToken, Instant)>>,
    /// Per-process HMAC key, generated fresh at construction. New
    /// process means existing cache entries become unreachable, which
    /// is fine: the next request re-validates.
    hmac_key: [u8; 32],
    /// How long a reviewed token stays cached (30s, see `new`).
    /// Independent of the ~1h kubelet token projection: this TTL bounds
    /// how stale a revoked-but-still-cached token can be, not the token
    /// lifetime. Short so a revocation takes effect within 30s.
    ttl: Duration,
}

impl IdentityCache {
    /// Build a cache with a fresh per-process HMAC key. Hard-fails if
    /// the OS RNG is unavailable: a clock-derived seed would be
    /// guessable by anyone with rough knowledge of process start time,
    /// which defeats the cache-key threat model. A pod with no
    /// `/dev/urandom` is broken; the broker should refuse to come up
    /// rather than serve identities under a predictable cache key.
    pub fn new() -> anyhow::Result<Self> {
        let mut hmac_key = [0u8; 32];
        getrandom::getrandom(&mut hmac_key)
            .map_err(|e| anyhow::anyhow!("OS RNG unavailable for identity cache key: {e}"))?;
        let cap = NonZeroUsize::new(CACHE_CAPACITY).expect("non-zero cache capacity");
        Ok(Self {
            inner: Mutex::new(LruCache::new(cap)),
            hmac_key,
            ttl: Duration::from_secs(30),
        })
    }

    pub fn get(&self, token: &str) -> Option<ReviewedToken> {
        let key = self.hash(token);
        let mut cache = self.inner.lock();
        let (id, at) = cache.get(&key)?;
        if at.elapsed() < self.ttl {
            Some(id.clone())
        } else {
            cache.pop(&key);
            None
        }
    }

    pub fn put(&self, token: &str, reviewed: ReviewedToken) {
        let key = self.hash(token);
        self.inner.lock().put(key, (reviewed, Instant::now()));
    }

    fn hash(&self, token: &str) -> TokenHash {
        let mut mac = HmacSha256::new_from_slice(&self.hmac_key)
            .expect("HMAC-SHA-256 accepts any key length");
        mac.update(token.as_bytes());
        mac.finalize().into_bytes().into()
    }
}

/// Validate the bearer cryptographically (TokenReview, cached) and
/// return WHO it is, with no role/tenant interpretation:
///   - missing / empty bearer  → 401
///   - TokenReview rejects     → 401
pub async fn reviewed_token(
    state: &Arc<BrokerState>,
    headers: &HeaderMap,
) -> Result<ReviewedToken, (StatusCode, String)> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".into()))?;
    if token.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "empty bearer".into()));
    }

    if let Some(cached) = state.identity_cache.get(token) {
        return Ok(cached);
    }

    let outcome = state
        .kube_client
        .token_review(token, &state.auth.audience)
        .await
        .map_err(|e| {
            tracing::warn!(target: "weft_broker::auth", error = %e, "tokenreview failed");
            (StatusCode::UNAUTHORIZED, format!("tokenreview: {e}"))
        })?;
    let reviewed = ReviewedToken {
        sa_name: outcome.sa_name,
        namespace: outcome.namespace,
        pod_name: outcome.pod_name,
    };
    state.identity_cache.put(token, reviewed.clone());
    Ok(reviewed)
}

/// Additional CONTROL-PLANE service accounts, from the deploy config:
/// `WEFT_BROKER_EXTRA_CONTROL_PLANE_SAS` is a comma list of
/// `namespace/serviceaccount` pairs the runtime trusts with the admin
/// surface alongside the dispatcher. TokenReview still verifies every
/// token; this only extends WHICH verified identities count as control
/// plane, and each stays distinct in audit logs. Parsed once per process.
fn extra_control_plane_sas() -> &'static [(String, String)] {
    static EXTRA: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    EXTRA.get_or_init(|| {
        std::env::var("WEFT_BROKER_EXTRA_CONTROL_PLANE_SAS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|pair| {
                let (ns, sa) = pair.trim().split_once('/')?;
                (!ns.is_empty() && !sa.is_empty()).then(|| (ns.to_string(), sa.to_string()))
            })
            .collect()
    })
}

/// What a verified service account is to this install, before any
/// tenant lookup. Both identity planes (the broker surface's
/// `extract_identity`, the runtime-storage plane's
/// `resolve_storage_caller`) start from this one answer, so they cannot
/// disagree about who is control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallerClass {
    /// A trusted account of THIS install's control plane. `role` is its
    /// broker role; `None` for the dispatcher and the deploy's extra
    /// admin accounts, which reach only the admin surface.
    ControlPlane { role: Option<Role> },
    /// A worker account: its tenant and project come from a lookup.
    Worker,
    /// No identity here; the message says why.
    Refused(String),
}

/// Classify a reviewed token's account. A control-plane account counts
/// only in `system_namespace`: another install sharing the cluster runs
/// the same account names in its own system namespace, and its
/// dispatcher, listener or supervisor is no authority over this one.
/// `extra` is the deploy's extra control-plane `(namespace, account)`
/// pairs, each already namespace-qualified.
pub(crate) fn classify_caller(
    sa_name: &str,
    namespace: &str,
    system_namespace: &str,
    extra: &[(String, String)],
) -> CallerClass {
    if extra.iter().any(|(ns, sa)| ns == namespace && sa == sa_name) {
        return CallerClass::ControlPlane { role: None };
    }
    let role = if sa_name == DISPATCHER_SA {
        None
    } else {
        match Role::from_sa_name(sa_name) {
            Some(role) if !role.is_control_plane() => return CallerClass::Worker,
            Some(role) => Some(role),
            None => return CallerClass::Refused(format!("unknown service account '{sa_name}'")),
        }
    };
    if namespace != system_namespace {
        return CallerClass::Refused(format!(
            "service account '{sa_name}' in '{namespace}' is not this install's control plane"
        ));
    }
    CallerClass::ControlPlane { role }
}

/// Resolve a runtime-storage caller from its presented token, into the
/// pure key-wall identity (`CallerAuth`). This is the identity authority
/// behind the runtime-file plane's prefix wall, run IN-PROCESS by the
/// broker's own runtime-storage handlers (the broker is both the authority
/// and the data path, so there is no relay):
///   - the dispatcher (`weft-dispatcher` in this install's system
///     namespace) or a deploy-configured extra admin account ->
///     ControlPlane (the CLI admin verbs: list/usage/delete/presign/wipe
///     for a tenant).
///   - a worker (`weft-worker-sa`) -> Worker { tenant, project, color },
///     resolving tenant + project from the token's namespace (or, in the
///     shared worker namespace, from the worker's pod identity), and
///     verifying any claimed `color` the same way journal writes do (the
///     color's owning pod must be the caller, and the color must belong to
///     the caller's project).
/// A `color` claim that is absent yields `color: None` (execution-scoped
/// keys then unreachable, which the wall enforces). Any other account,
/// the listener and supervisor included, has no runtime-storage
/// identity (403).
pub async fn resolve_storage_caller(
    state: &Arc<BrokerState>,
    headers: &HeaderMap,
    color: Option<&str>,
) -> Result<weft_core::storage::key::CallerAuth, (StatusCode, String)> {
    use weft_core::storage::key::CallerAuth;
    let reviewed = reviewed_token(state, headers).await?;
    match classify_caller(
        &reviewed.sa_name,
        &reviewed.namespace,
        &state.auth.system_namespace,
        extra_control_plane_sas(),
    ) {
        CallerClass::ControlPlane { role: None } => Ok(CallerAuth::ControlPlane),
        CallerClass::ControlPlane { role: Some(_) } => Err((
            StatusCode::FORBIDDEN,
            format!("service account '{}' has no runtime-storage identity", reviewed.sa_name),
        )),
        CallerClass::Refused(why) => Err((StatusCode::FORBIDDEN, why)),
        CallerClass::Worker => {
            // Who this worker is, from its own unforgeable identity.
            // One resolver, shared with `extract_identity`, so the two
            // planes cannot end up with different ideas of a caller.
            let resolved = crate::scope::lookup_worker_scope(
                &state.scope_cache,
                &state.pool,
                &reviewed.namespace,
                reviewed.pod_name.as_deref(),
            )
            .await?;
            let (project_id, tenant_id) = (resolved.project, resolved.tenant);
            let color = match color {
                None => None,
                Some(color) => {
                    let row: Option<(String, uuid::Uuid, Option<String>)> = sqlx::query_as(
                        "SELECT tenant_id, project_id, owner_pod_name \
                         FROM execution_color WHERE color = $1",
                    )
                    .bind(color)
                    .fetch_optional(&state.pool)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
                    let Some((color_tenant, color_project, owner_pod)) = row else {
                        return Err((StatusCode::FORBIDDEN, "unknown execution color".into()));
                    };
                    if color_tenant != tenant_id || color_project != project_id {
                        tracing::warn!(
                            target: "weft_broker::scope",
                            caller_ns = %reviewed.namespace,
                            color = %color,
                            "runtime storage rejected cross-project color claim"
                        );
                        return Err((
                            StatusCode::FORBIDDEN,
                            "color belongs to a different project".into(),
                        ));
                    }
                    // Same gate as journal writes: only the pod that claimed
                    // the execution drives its color.
                    if reviewed.pod_name.is_none()
                        || owner_pod.as_deref() != reviewed.pod_name.as_deref()
                    {
                        return Err((
                            StatusCode::FORBIDDEN,
                            "color is not owned by the calling pod".into(),
                        ));
                    }
                    Some(color.to_string())
                }
            };
            Ok(CallerAuth::Worker { tenant: tenant_id, project_id: project_id.to_string(), color })
        }
    }
}

/// Resolve + require a control-plane caller for an admin request
/// (runtime-storage admin, access admin). The dispatcher signs with its
/// own SA token; anything else is refused.
pub(crate) async fn control_plane(
    state: &Arc<BrokerState>,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, String)> {
    match resolve_storage_caller(state, headers, None).await? {
        weft_core::storage::key::CallerAuth::ControlPlane => Ok(()),
        weft_core::storage::key::CallerAuth::Worker { .. } => {
            Err((StatusCode::FORBIDDEN, "the admin surface is dispatcher-only".into()))
        }
    }
}

/// Axum-extractor backend: reviewed token + role table + tenant
/// resolution. Reject patterns on top of `reviewed_token`:
///   - SA name not in our role table → 403 (`weft-{role}-sa` only)
///   - namespace not registered to a tenant → 403
pub async fn extract_identity(
    state: &Arc<BrokerState>,
    headers: &HeaderMap,
) -> Result<CallerIdentity, (StatusCode, String)> {
    let reviewed = reviewed_token(state, headers).await?;
    // Control-plane services (pooled listener / supervisor) run in the
    // control-plane namespace and are not pinned to a tenant: their
    // scope is ControlPlane and per-op validation derives the tenant
    // from the resource being acted on.
    //
    // Tenant-scoped pods (worker) resolve their single tenant. A worker
    // in a PER-PROJECT namespace resolves it from the namespace (the
    // dispatcher registers namespace -> tenant; an unregistered
    // namespace is a 403). A worker in the SHARED namespace can't:
    // that namespace holds many tenants and has no registry row, so it
    // resolves the tenant from its own pod identity instead (the
    // kubelet-stamped, unforgeable `pod_name` -> the dispatcher-written
    // `worker_pod` row -> project -> tenant). Both paths derive the
    // tenant from trusted, dispatcher-written state, never from
    // anything the pod supplies.
    let (role, scope) = match classify_caller(
        &reviewed.sa_name,
        &reviewed.namespace,
        &state.auth.system_namespace,
        extra_control_plane_sas(),
    ) {
        CallerClass::ControlPlane { role: Some(role) } => (role, CallerScope::ControlPlane),
        CallerClass::ControlPlane { role: None } => {
            return Err((
                StatusCode::FORBIDDEN,
                format!("service account '{}' has no broker role", reviewed.sa_name),
            ))
        }
        CallerClass::Refused(why) => return Err((StatusCode::FORBIDDEN, why)),
        CallerClass::Worker => {
            let resolved = crate::scope::lookup_worker_scope(
                &state.scope_cache,
                &state.pool,
                &reviewed.namespace,
                reviewed.pod_name.as_deref(),
            )
            .await?;
            (Role::Worker, CallerScope::Tenant { tenant: resolved.tenant, project: resolved.project })
        }
    };
    Ok(CallerIdentity {
        scope,
        role,
        namespace: reviewed.namespace,
        pod_name: reviewed.pod_name,
    })
}

/// Convenience extractor for handlers: pulls headers out, runs
/// `extract_identity`, returns the resolved `CallerIdentity` to the
/// handler body. The handler signs `(State, AuthedCaller, Json<...>)`.
pub struct AuthedCaller(pub CallerIdentity);

impl<S> FromRequestParts<S> for AuthedCaller
where
    S: Send + Sync,
    Arc<BrokerState>: axum::extract::FromRef<S>,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let State(broker): State<Arc<BrokerState>> =
            State::from_request_parts(parts, state).await.map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "broker state missing".into(),
                )
            })?;
        let identity = extract_identity(&broker, &parts.headers).await?;
        Ok(AuthedCaller(identity))
    }
}

#[cfg(test)]
mod classify_tests {
    use super::{classify_caller, CallerClass, Role, DISPATCHER_SA, INFRA_SUPERVISOR_SA, LISTENER_SA, WORKER_SA};

    const OURS: &str = "weft-system";
    const THEIRS: &str = "weft-system-other";

    fn classify(sa: &str, ns: &str) -> CallerClass {
        classify_caller(sa, ns, OURS, &[])
    }

    #[test]
    fn our_dispatcher_is_control_plane() {
        assert_eq!(classify(DISPATCHER_SA, OURS), CallerClass::ControlPlane { role: None });
    }

    #[test]
    fn another_installs_dispatcher_is_refused() {
        assert!(matches!(classify(DISPATCHER_SA, THEIRS), CallerClass::Refused(_)));
    }

    #[test]
    fn another_installs_listener_and_supervisor_are_refused() {
        assert!(matches!(classify(LISTENER_SA, THEIRS), CallerClass::Refused(_)));
        assert!(matches!(classify(INFRA_SUPERVISOR_SA, THEIRS), CallerClass::Refused(_)));
    }

    #[test]
    fn our_listener_and_supervisor_carry_their_role() {
        assert_eq!(classify(LISTENER_SA, OURS), CallerClass::ControlPlane { role: Some(Role::Listener) });
        assert_eq!(
            classify(INFRA_SUPERVISOR_SA, OURS),
            CallerClass::ControlPlane { role: Some(Role::InfraSupervisor) }
        );
    }

    #[test]
    fn a_worker_anywhere_needs_a_lookup() {
        assert_eq!(classify(WORKER_SA, "wft-project-a"), CallerClass::Worker);
    }

    #[test]
    fn an_unknown_account_is_refused() {
        assert!(matches!(classify("default", OURS), CallerClass::Refused(_)));
    }

    #[test]
    fn an_extra_account_counts_only_in_its_own_namespace() {
        let extra = [("ops".to_string(), "admin".to_string())];
        assert_eq!(classify_caller("admin", "ops", OURS, &extra), CallerClass::ControlPlane { role: None });
        assert!(matches!(classify_caller("admin", "elsewhere", OURS, &extra), CallerClass::Refused(_)));
    }
}
