//! Scope checks. The security-critical surface of the broker.
//!
//! Every endpoint that touches per-tenant data runs one of these
//! checks before delegating to the underlying client. Each check
//! resolves the resource's owning tenant from Postgres, then
//! compares against `caller.tenant_id`. Resolution is cached because
//! the mappings are immutable in steady state, but cached entries
//! still expire on a TTL so a deleted-then-reissued resource id
//! eventually re-validates against the live row.
//!
//! 403 responses log the caller identity + the requested scope so
//! attempted cross-tenant access shows up in the audit trail.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use lru::LruCache;
use sqlx::postgres::PgPool;
use tokio::sync::Mutex;

use crate::auth::CallerIdentity;

/// Cache size per resource kind. 100k is well above any realistic
/// active-color count and avoids the perf cliff DashMap's "drop
/// half the iter" eviction was producing.
const CACHE_CAPACITY: usize = 100_000;

/// Cache entries expire after this so a deleted resource doesn't
/// stay cached as "owned by tenant X" forever. Five minutes is long
/// enough to amortize the lookup across hot paths and short enough
/// that revoke / delete propagates without manual flush.
const CACHE_TTL: Duration = Duration::from_secs(300);

/// Cache for `(resource_id) -> tenant_id`, true LRU eviction with a
/// per-entry expiry.
#[derive(Clone)]
pub struct ScopeCache {
    project_to_tenant: Arc<Mutex<LruCache<String, (String, Instant)>>>,
    color_to_tenant: Arc<Mutex<LruCache<String, (String, Instant)>>>,
    signal_to_tenant: Arc<Mutex<LruCache<String, (String, Instant)>>>,
}

impl ScopeCache {
    pub fn new() -> Self {
        let cap = NonZeroUsize::new(CACHE_CAPACITY).expect("non-zero capacity");
        Self {
            project_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
            color_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
            signal_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
        }
    }
}

impl Default for ScopeCache {
    fn default() -> Self {
        Self::new()
    }
}

async fn cache_get(
    map: &Mutex<LruCache<String, (String, Instant)>>,
    key: &str,
) -> Option<String> {
    let mut g = map.lock().await;
    let entry = g.get(key)?;
    if entry.1.elapsed() < CACHE_TTL {
        Some(entry.0.clone())
    } else {
        // Expired; pop so the next lookup re-fetches.
        g.pop(key);
        None
    }
}

async fn cache_put(
    map: &Mutex<LruCache<String, (String, Instant)>>,
    key: String,
    value: String,
) {
    let mut g = map.lock().await;
    g.put(key, (value, Instant::now()));
}

/// Reject with 403 if `project_id` does not belong to `caller.tenant_id`.
pub async fn require_project_owned_by(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    project_id: &str,
) -> Result<(), (StatusCode, String)> {
    let tenant = lookup_project_tenant(cache, pool, project_id).await?;
    if tenant != caller.tenant_id {
        log_denied(caller, "project", project_id, &tenant);
        return Err((StatusCode::FORBIDDEN, "project not owned by caller".into()));
    }
    Ok(())
}

/// Reject with 403 if `color` does not belong to `caller.tenant_id`.
pub async fn require_color_owned_by(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    color: &str,
) -> Result<(), (StatusCode, String)> {
    let tenant = lookup_color_tenant(cache, pool, color).await?;
    if tenant != caller.tenant_id {
        log_denied(caller, "color", color, &tenant);
        return Err((StatusCode::FORBIDDEN, "color not owned by caller".into()));
    }
    Ok(())
}

/// Reject with 403 if `token` (signal token) does not belong to
/// `caller.tenant_id`.
pub async fn require_signal_owned_by(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    token: &str,
) -> Result<(), (StatusCode, String)> {
    let tenant = lookup_signal_tenant(cache, pool, token).await?;
    if tenant != caller.tenant_id {
        log_denied(caller, "signal", token, &tenant);
        return Err((StatusCode::FORBIDDEN, "signal not owned by caller".into()));
    }
    Ok(())
}

/// Reject with 403 if `claimed_tenant` differs from `caller.tenant_id`.
/// Used when the request body itself names a tenant (e.g. signal list
/// for tenant); the broker refuses to serve cross-tenant lookups even
/// if the caller asks for them.
pub fn require_tenant_eq(
    caller: &CallerIdentity,
    claimed_tenant: &str,
) -> Result<(), (StatusCode, String)> {
    if claimed_tenant != caller.tenant_id {
        log_denied(caller, "tenant", claimed_tenant, claimed_tenant);
        return Err((StatusCode::FORBIDDEN, "tenant mismatch".into()));
    }
    Ok(())
}

async fn lookup_project_tenant(
    cache: &ScopeCache,
    pool: &PgPool,
    project_id: &str,
) -> Result<String, (StatusCode, String)> {
    if let Some(t) = cache_get(&cache.project_to_tenant, project_id).await {
        return Ok(t);
    }
    let row: Option<(String,)> = sqlx::query_as("SELECT tenant_id FROM project WHERE id = $1::uuid")
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project lookup: {e}")))?;
    let tenant = row
        .ok_or((StatusCode::NOT_FOUND, "unknown project".into()))?
        .0;
    cache_put(
        &cache.project_to_tenant,
        project_id.to_string(),
        tenant.clone(),
    )
    .await;
    Ok(tenant)
}

async fn lookup_color_tenant(
    cache: &ScopeCache,
    pool: &PgPool,
    color: &str,
) -> Result<String, (StatusCode, String)> {
    if let Some(t) = cache_get(&cache.color_to_tenant, color).await {
        return Ok(t);
    }
    let row: Option<(String,)> =
        sqlx::query_as("SELECT tenant_id FROM execution_color WHERE color = $1")
            .bind(color)
            .fetch_optional(pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("color lookup: {e}")))?;
    let tenant = row
        .ok_or((StatusCode::NOT_FOUND, "unknown color".into()))?
        .0;
    cache_put(&cache.color_to_tenant, color.to_string(), tenant.clone()).await;
    Ok(tenant)
}

async fn lookup_signal_tenant(
    cache: &ScopeCache,
    pool: &PgPool,
    token: &str,
) -> Result<String, (StatusCode, String)> {
    if let Some(t) = cache_get(&cache.signal_to_tenant, token).await {
        return Ok(t);
    }
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT tenant_id FROM signal WHERE token = $1",
    )
    .bind(token)
    .fetch_optional(pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal lookup: {e}")))?;
    let tenant = row
        .ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?
        .0;
    cache_put(&cache.signal_to_tenant, token.to_string(), tenant.clone()).await;
    Ok(tenant)
}

fn log_denied(caller: &CallerIdentity, kind: &str, requested: &str, owner: &str) {
    tracing::warn!(
        target: "weft_broker::scope",
        caller_tenant = %caller.tenant_id,
        caller_role = ?caller.role,
        caller_ns = %caller.namespace,
        scope = kind,
        requested,
        owner = owner,
        "broker rejected cross-tenant access"
    );
}
