//! Caller identity: extract + validate the projected SA token,
//! cache the resolved `(tenant, role)` so the hot path doesn't hit
//! the k8s API on every request.

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
    /// Tenant namespace prefix (e.g. `wm-`). Reverse-mapped from the
    /// caller's namespace to derive the tenant id.
    pub namespace_prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Listener,
    Worker,
    Sidecar,
}

impl Role {
    fn from_sa_name(sa: &str) -> Option<Self> {
        match sa {
            "weft-listener-sa" => Some(Self::Listener),
            "weft-worker-sa" => Some(Self::Worker),
            "weft-sidecar-sa" => Some(Self::Sidecar),
            _ => None,
        }
    }
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
    inner: Mutex<LruCache<TokenHash, (CallerIdentity, Instant)>>,
    /// Per-process HMAC key, generated fresh at construction. New
    /// process means existing cache entries become unreachable, which
    /// is fine: the next request re-validates.
    hmac_key: [u8; 32],
    /// Identities are cached for this long. SA tokens rotate every
    /// hour (kubelet projection); shorter cache keeps the validation
    /// chain fresh against revocation.
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

    pub fn get(&self, token: &str) -> Option<CallerIdentity> {
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

    pub fn put(&self, token: &str, identity: CallerIdentity) {
        let key = self.hash(token);
        self.inner.lock().put(key, (identity, Instant::now()));
    }

    fn hash(&self, token: &str) -> TokenHash {
        let mut mac = HmacSha256::new_from_slice(&self.hmac_key)
            .expect("HMAC-SHA-256 accepts any key length");
        mac.update(token.as_bytes());
        mac.finalize().into_bytes().into()
    }
}

/// Axum extractor: validates the bearer + returns the resolved
/// identity. Reject patterns:
///   - missing / empty bearer  → 401
///   - TokenReview rejects     → 401
///   - SA name not in our role table → 403 (`weft-{role}-sa` only)
///   - namespace doesn't carry our prefix → 403
pub async fn extract_identity(
    state: &Arc<BrokerState>,
    headers: &HeaderMap,
) -> Result<CallerIdentity, (StatusCode, String)> {
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

    let role = Role::from_sa_name(&outcome.sa_name).ok_or((
        StatusCode::FORBIDDEN,
        format!("unknown service account '{}'", outcome.sa_name),
    ))?;
    let tenant_id = outcome
        .namespace
        .strip_prefix(&state.auth.namespace_prefix)
        .ok_or((
            StatusCode::FORBIDDEN,
            format!(
                "namespace '{}' has no '{}' prefix",
                outcome.namespace, state.auth.namespace_prefix
            ),
        ))?
        .to_string();

    let identity = CallerIdentity {
        tenant_id,
        role,
        namespace: outcome.namespace,
        pod_name: outcome.pod_name,
    };
    state.identity_cache.put(token, identity.clone());
    Ok(identity)
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
