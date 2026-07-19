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
}

// Service-account names + the dispatcher namespace, defined ONCE.
// `from_sa_name` and `resolve_storage_caller` both branch on these; without
// shared consts a rename would update one site and silently break the
// other (e.g. workers losing their storage identity).
pub(crate) const WORKER_SA: &str = "weft-worker-sa";
pub(crate) const LISTENER_SA: &str = "weft-listener-sa";
pub(crate) const INFRA_SUPERVISOR_SA: &str = "weft-infra-supervisor-sa";
pub(crate) const DISPATCHER_SA: &str = "weft-dispatcher";
pub(crate) const DISPATCHER_NS: &str = "weft-system";

/// The shared worker namespace: holds no-infra workers from MANY
/// tenants, so it maps to no single tenant and has NO
/// `weft_namespace_tenant` row. A worker here resolves its tenant from
/// its own pod identity (`worker_pod` row -> project -> tenant), not
/// from the namespace. See `extract_identity`.
// SYNC: SHARED_WORKER_NAMESPACE <-> crates/weft-dispatcher/src/project_namespace.rs SHARED_WORKER_NAMESPACE, crates/weft-e2e/tests/worker_placement.rs SHARED_WORKER_NAMESPACE
pub(crate) const SHARED_WORKER_NAMESPACE: &str = "wft-shared-workers";

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
    Tenant(String),
    ControlPlane,
}

impl CallerScope {
    /// The tenant this caller is pinned to, or `None` for a
    /// control-plane caller that is not pinned to any single tenant.
    pub fn pinned_tenant(&self) -> Option<&str> {
        match self {
            Self::Tenant(t) => Some(t),
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

/// Resolve a runtime-storage caller from its presented token, into the
/// pure key-wall identity (`CallerAuth`). This is the identity authority
/// behind the runtime-file plane's prefix wall, run IN-PROCESS by the
/// broker's own runtime-storage handlers (the broker is both the authority
/// Additional CONTROL-PLANE service accounts, from the deploy config:
/// `WEFT_BROKER_EXTRA_CONTROL_PLANE_SAS` is a comma list of
/// `namespace/serviceaccount` pairs the runtime trusts with the admin
/// surface alongside the dispatcher. TokenReview still verifies every
/// token; this only extends WHICH verified identities count as control
/// plane, and each stays distinct in audit logs. Parsed once per process.
fn is_extra_control_plane(namespace: &str, sa_name: &str) -> bool {
    static EXTRA: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    EXTRA
        .get_or_init(|| {
            std::env::var("WEFT_BROKER_EXTRA_CONTROL_PLANE_SAS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|pair| {
                    let (ns, sa) = pair.trim().split_once('/')?;
                    (!ns.is_empty() && !sa.is_empty())
                        .then(|| (ns.to_string(), sa.to_string()))
                })
                .collect()
        })
        .iter()
        .any(|(ns, sa)| ns == namespace && sa == sa_name)
}

/// and the data path, so there is no relay):
///   - the dispatcher (`weft-dispatcher` in `weft-system`) -> ControlPlane
///     (the CLI admin verbs: list/usage/delete/presign/wipe for a tenant).
///   - a worker (`weft-worker-sa`) -> Worker { tenant, project, color },
///     resolving tenant + project from the token's namespace (or, in the
///     shared worker namespace, from the worker's pod identity), and
///     verifying any claimed `color` the same way journal writes do (the
///     color's owning pod must be the caller, and the color must belong to
///     the caller's project).
/// A `color` claim that is absent yields `color: None` (execution-scoped
/// keys then unreachable, which the wall enforces). Any other SA has no
/// runtime-storage identity (403).
pub async fn resolve_storage_caller(
    state: &Arc<BrokerState>,
    headers: &HeaderMap,
    color: Option<&str>,
) -> Result<weft_core::storage::key::CallerAuth, (StatusCode, String)> {
    use weft_core::storage::key::CallerAuth;
    let reviewed = reviewed_token(state, headers).await?;
    if is_extra_control_plane(&reviewed.namespace, &reviewed.sa_name) {
        return Ok(CallerAuth::ControlPlane);
    }
    match reviewed.sa_name.as_str() {
        DISPATCHER_SA if reviewed.namespace == DISPATCHER_NS => Ok(CallerAuth::ControlPlane),
        WORKER_SA => {
            // Resolve the worker's project AND tenant. Two worker-hosting
            // shapes, told apart by namespace (mirrors `storage_authorize`'s
            // old logic, which this replaces):
            //  - PER-PROJECT namespace: the namespace IS the project, so the
            //    one `project` row whose `project_namespace` matches gives
            //    both ids from a single source of truth. The registration
            //    gate is preserved by write ordering (the dispatcher writes
            //    the namespace->tenant registry row before stamping
            //    `project_namespace`).
            //  - SHARED namespace: maps to no project, so resolve from the
            //    worker's OWN unforgeable pod identity (kubelet-stamped
            //    pod_name -> the dispatcher-written `worker_pod` row ->
            //    project -> tenant).
            let row: Option<(String, String)> =
                if reviewed.namespace == SHARED_WORKER_NAMESPACE {
                    let pod_name = reviewed.pod_name.as_deref().ok_or((
                        StatusCode::FORBIDDEN,
                        "shared-namespace worker token carries no pod_name".to_string(),
                    ))?;
                    sqlx::query_as(
                        "SELECT p.id::text, p.tenant_id \
                         FROM worker_pod wp JOIN project p ON p.id::text = wp.project_id \
                         WHERE wp.pod_name = $1",
                    )
                    .bind(pod_name)
                    .fetch_optional(&state.pool)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
                } else {
                    sqlx::query_as(
                        "SELECT id::text, tenant_id FROM project WHERE project_namespace = $1",
                    )
                    .bind(&reviewed.namespace)
                    .fetch_optional(&state.pool)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
                };
            let (project_id, tenant_id) = row.ok_or((
                StatusCode::FORBIDDEN,
                format!(
                    "could not resolve a project for worker in namespace '{}' (pod {:?}); \
                     not a registered project namespace and no matching worker_pod row",
                    reviewed.namespace, reviewed.pod_name
                ),
            ))?;
            let color = match color {
                None => None,
                Some(color) => {
                    let row: Option<(String, String, Option<String>)> = sqlx::query_as(
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
            Ok(CallerAuth::Worker { tenant: tenant_id, project_id, color })
        }
        other => Err((
            StatusCode::FORBIDDEN,
            format!("service account '{other}' has no runtime-storage identity"),
        )),
    }
}

/// Resolve the namespace's owning tenant. Authoritative lookup: the
/// dispatcher writes a row to `weft_namespace_tenant` whenever it
/// creates a namespace, so the broker doesn't have to parse the
/// namespace string (which would be unsafe if a tenant could create
/// their own namespaces). A missing row = unrecognized namespace =
/// 403. Cached in the scope cache.
pub async fn namespace_tenant(
    state: &Arc<BrokerState>,
    namespace: &str,
) -> Result<String, (StatusCode, String)> {
    crate::scope::lookup_namespace_tenant(&state.scope_cache, &state.pool, namespace).await
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
    let role = Role::from_sa_name(&reviewed.sa_name).ok_or((
        StatusCode::FORBIDDEN,
        format!("unknown service account '{}'", reviewed.sa_name),
    ))?;
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
    let scope = if role.is_control_plane() {
        CallerScope::ControlPlane
    } else if reviewed.namespace == SHARED_WORKER_NAMESPACE {
        let pod_name = reviewed.pod_name.as_deref().ok_or((
            StatusCode::FORBIDDEN,
            "shared-namespace worker token carries no pod_name; cannot resolve tenant".to_string(),
        ))?;
        CallerScope::Tenant(
            crate::scope::lookup_pod_tenant(&state.scope_cache, &state.pool, pod_name).await?,
        )
    } else {
        CallerScope::Tenant(namespace_tenant(state, &reviewed.namespace).await?)
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

