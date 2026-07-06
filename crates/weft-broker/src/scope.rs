//! Scope checks. The security-critical surface of the broker.
//!
//! Every endpoint that touches per-tenant data runs one of these
//! checks before delegating to the underlying client. Each check
//! resolves the resource's owning tenant from Postgres, then enforces
//! the caller's scope: a tenant-scoped caller (worker, runs untrusted
//! user code) must match the resource's tenant; a control-plane caller
//! (pooled listener / supervisor, trusted, runs our code only) passes
//! and the resolved tenant is used for any write. The helpers RETURN
//! the resource's tenant so write paths stamp the resource's true
//! tenant, never the caller identity (a control-plane caller has none).
//! Resolution is cached because the mappings are immutable in steady
//! state, but cached entries still expire on a TTL so a
//! deleted-then-reissued resource id eventually re-validates against
//! the live row.
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
    namespace_to_tenant: Arc<Mutex<LruCache<String, (String, Instant)>>>,
}

impl ScopeCache {
    pub fn new() -> Self {
        let cap = NonZeroUsize::new(CACHE_CAPACITY).expect("non-zero capacity");
        Self {
            project_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
            color_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
            signal_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
            namespace_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
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

/// Resolve `project_id`'s owning tenant, enforcing ownership. For a
/// tenant-scoped caller (worker), 403 unless the project belongs to the
/// caller's tenant. For a control-plane caller (pooled listener /
/// supervisor), any real project is allowed. Returns the project's
/// tenant either way, so write paths stamp the resource's true tenant
/// (never the caller's, which a control-plane caller does not have).
pub async fn require_project_owned_by(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    project_id: &str,
) -> Result<String, (StatusCode, String)> {
    let tenant = lookup_project_tenant(cache, pool, project_id).await?;
    enforce_scope(caller, "project", project_id, &tenant)?;
    Ok(tenant)
}

/// Resolve `color`'s owning tenant, enforcing ownership. See
/// `require_project_owned_by` for the tenant-vs-control-plane rule.
pub async fn require_color_owned_by(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    color: &str,
) -> Result<String, (StatusCode, String)> {
    let tenant = lookup_color_tenant(cache, pool, color).await?;
    enforce_scope(caller, "color", color, &tenant)?;
    Ok(tenant)
}

/// Resolve a signal `token`'s owning tenant, enforcing ownership. See
/// `require_project_owned_by` for the tenant-vs-control-plane rule.
/// The pooled listener fires held events for many tenants' signals;
/// as a control-plane caller it passes the enforcement and the
/// returned tenant is the signal's own, which the broker stamps on the
/// FireSignal task.
pub async fn require_signal_owned_by(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    token: &str,
) -> Result<String, (StatusCode, String)> {
    let tenant = lookup_signal_tenant(cache, pool, token).await?;
    enforce_scope(caller, "signal", token, &tenant)?;
    Ok(tenant)
}

/// Resolve the tenant for a namespace named in a request body,
/// enforcing ownership. For a tenant-scoped caller, 403 unless it is
/// the caller's tenant. For a control-plane caller (pooled supervisor),
/// any namespace registered to a real tenant is allowed; returns that
/// tenant. A namespace with no registered tenant is a 403 regardless.
pub async fn require_namespace_owned_by(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    namespace: &str,
) -> Result<String, (StatusCode, String)> {
    let tenant = lookup_namespace_tenant(cache, pool, namespace).await?;
    enforce_scope(caller, "namespace", namespace, &tenant)?;
    Ok(tenant)
}

/// Enforce a caller's scope against a tenant named directly in a
/// request body (not derived from a resource lookup). A tenant-scoped
/// caller must name its own tenant; a control-plane caller (trusted)
/// may name any tenant. Used by supervisor list endpoints that ask
/// "give me work for tenant T": the pooled supervisor legitimately asks
/// about many tenants, a worker only ever its own.
pub fn require_tenant_in_scope(
    caller: &CallerIdentity,
    requested_tenant: &str,
) -> Result<(), (StatusCode, String)> {
    match caller.scope.pinned_tenant() {
        Some(t) if t != requested_tenant => {
            log_denied(caller, "tenant", requested_tenant, requested_tenant);
            Err((StatusCode::FORBIDDEN, "tenant mismatch".into()))
        }
        _ => Ok(()),
    }
}

/// Enforce a caller's scope against a resource's resolved tenant.
/// Tenant-scoped callers must match; control-plane callers pass (they
/// are trusted to act for any tenant). Centralized so every resource
/// kind enforces the rule identically.
fn enforce_scope(
    caller: &CallerIdentity,
    kind: &str,
    resource: &str,
    resource_tenant: &str,
) -> Result<(), (StatusCode, String)> {
    match caller.scope.pinned_tenant() {
        Some(caller_tenant) if caller_tenant != resource_tenant => {
            log_denied(caller, kind, resource, resource_tenant);
            Err((StatusCode::FORBIDDEN, format!("{kind} not owned by caller")))
        }
        // Pinned and matching, or control-plane (not pinned): allowed.
        _ => Ok(()),
    }
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

/// Namespace -> tenant, from the registry the dispatcher writes at
/// namespace-creation time. 403 on a namespace it never created.
pub async fn lookup_namespace_tenant(
    cache: &ScopeCache,
    pool: &PgPool,
    namespace: &str,
) -> Result<String, (StatusCode, String)> {
    if let Some(t) = cache_get(&cache.namespace_to_tenant, namespace).await {
        return Ok(t);
    }
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT tenant_id FROM weft_namespace_tenant WHERE namespace = $1",
    )
    .bind(namespace)
    .fetch_optional(pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("namespace lookup: {e}")))?;
    let tenant = row
        .ok_or((
            StatusCode::FORBIDDEN,
            format!(
                "namespace '{namespace}' is not registered to any tenant; only namespaces \
                 the dispatcher created can authenticate"
            ),
        ))?
        .0;
    cache_put(&cache.namespace_to_tenant, namespace.to_string(), tenant.clone()).await;
    Ok(tenant)
}

/// Pod -> tenant, for a worker in the shared namespace (where the
/// namespace maps to no single tenant). The pod's `worker_pod` row,
/// written by the dispatcher at spawn time, names the project; the
/// project names the tenant. The token's `pod_name` is kubelet-stamped
/// and unforgeable, and the row is dispatcher-written, so this resolves
/// the tenant from trusted state only (never from anything the pod
/// itself supplies). 403 on a pod with no row (forged / already GC'd).
/// The project leg reuses the `project_to_tenant` cache.
pub async fn lookup_pod_tenant(
    cache: &ScopeCache,
    pool: &PgPool,
    pod_name: &str,
) -> Result<String, (StatusCode, String)> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT project_id FROM worker_pod WHERE pod_name = $1")
            .bind(pod_name)
            .fetch_optional(pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("worker_pod lookup: {e}")))?;
    let project_id = row
        .ok_or((
            StatusCode::FORBIDDEN,
            format!(
                "pod '{pod_name}' has no worker_pod row; a shared-namespace worker must have \
                 been spawned by the dispatcher to authenticate"
            ),
        ))?
        .0;
    lookup_project_tenant(cache, pool, &project_id).await
}

fn log_denied(caller: &CallerIdentity, kind: &str, requested: &str, owner: &str) {
    tracing::warn!(
        target: "weft_broker::scope",
        caller_tenant = ?caller.scope.pinned_tenant(),
        caller_role = ?caller.role,
        caller_ns = %caller.namespace,
        scope = kind,
        requested,
        owner = owner,
        "broker rejected cross-tenant access"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{CallerScope, Role};

    fn tenant_caller(tenant: &str) -> CallerIdentity {
        CallerIdentity {
            scope: CallerScope::Tenant(tenant.to_string()),
            role: Role::Worker,
            namespace: format!("wft-{tenant}"),
            pod_name: Some("pod-x".into()),
        }
    }

    fn control_plane_caller(role: Role) -> CallerIdentity {
        CallerIdentity {
            scope: CallerScope::ControlPlane,
            role,
            namespace: "weft-system".into(),
            pod_name: Some("pod-cp".into()),
        }
    }

    #[test]
    fn tenant_caller_matching_resource_passes() {
        let caller = tenant_caller("acme");
        assert!(enforce_scope(&caller, "project", "p1", "acme").is_ok());
    }

    #[test]
    fn tenant_caller_foreign_resource_rejected() {
        let caller = tenant_caller("acme");
        let err = enforce_scope(&caller, "project", "p1", "globex").unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn control_plane_caller_any_resource_passes() {
        // The whole point of the trusted pooled pod: it acts for any
        // tenant. Both a listener and a supervisor are control-plane.
        for role in [Role::Listener, Role::InfraSupervisor] {
            let caller = control_plane_caller(role);
            assert!(
                enforce_scope(&caller, "signal", "tok", "any-tenant").is_ok(),
                "control-plane {role:?} must pass for any tenant"
            );
        }
    }

    #[test]
    fn require_tenant_in_scope_tenant_match() {
        assert!(require_tenant_in_scope(&tenant_caller("acme"), "acme").is_ok());
    }

    #[test]
    fn require_tenant_in_scope_tenant_mismatch_rejected() {
        let err = require_tenant_in_scope(&tenant_caller("acme"), "globex").unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn require_tenant_in_scope_control_plane_any_tenant() {
        let caller = control_plane_caller(Role::InfraSupervisor);
        assert!(require_tenant_in_scope(&caller, "any-tenant").is_ok());
    }

    #[test]
    fn worker_is_never_control_plane_scope() {
        // The worker runs untrusted user code; it must always be
        // tenant-pinned, never control-plane. Guard the invariant at
        // the scope level: a Worker caller's scope pins a tenant.
        let caller = tenant_caller("acme");
        assert_eq!(caller.scope.pinned_tenant(), Some("acme"));
    }
}
