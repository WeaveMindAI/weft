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
// `from_sa_name` and `storage_authorize` both branch on these; without
// shared consts a rename would update one site and silently break the
// other (e.g. workers losing their storage identity).
pub(crate) const WORKER_SA: &str = "weft-worker-sa";
pub(crate) const LISTENER_SA: &str = "weft-listener-sa";
pub(crate) const INFRA_SUPERVISOR_SA: &str = "weft-infra-supervisor-sa";
pub(crate) const STORAGE_SA: &str = "weft-storage-sa";
pub(crate) const DISPATCHER_SA: &str = "weft-dispatcher";
pub(crate) const DISPATCHER_NS: &str = "weft-system";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Listener,
    Worker,
    /// Per-tenant infra-supervisor pod. Owns health probing,
    /// HealthProtocol evaluation, and lifecycle execution
    /// (stop/terminate kubectl ops) for its tenant's projects.
    InfraSupervisor,
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

// NOTE: the per-tenant storage box (`weft-storage-sa`) and the
// dispatcher (`weft-dispatcher` in `weft-system`) are deliberately
// NOT in the role table: they never call the broker's tenant data
// endpoints. They only appear on `/storage/authorize` (see
// `handlers::storage_authorize`), which resolves them from the raw
// reviewed token.

/// The cached output of a TokenReview: who the token cryptographically
/// is, with no role/tenant interpretation attached. Interpretation
/// (role table, tenant lookup) happens per endpoint ON TOP of this,
/// so endpoints with different caller universes (the storage
/// authorize path admits the dispatcher and storage boxes) share one
/// review + one cache.
#[derive(Debug, Clone)]
pub struct ReviewedToken {
    pub sa_name: String,
    pub namespace: String,
    pub pod_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CallerIdentity {
    pub tenant_id: String,
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
    let tenant_id = namespace_tenant(state, &reviewed.namespace).await?;
    Ok(CallerIdentity {
        tenant_id,
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

