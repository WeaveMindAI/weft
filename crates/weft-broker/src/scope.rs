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
use crate::handlers::internal;

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
    color_to_scope: Arc<Mutex<LruCache<String, (ProjectScope, Instant)>>>,
    signal_to_scope: Arc<Mutex<LruCache<String, (ProjectScope, Instant)>>>,
    /// A worker's own (tenant, project), keyed by the pod it is.
    worker_to_scope: Arc<Mutex<LruCache<String, (ProjectScope, Instant)>>>,
}

impl ScopeCache {
    pub fn new() -> Self {
        let cap = NonZeroUsize::new(CACHE_CAPACITY).expect("non-zero capacity");
        Self {
            project_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
            color_to_scope: Arc::new(Mutex::new(LruCache::new(cap))),
            signal_to_scope: Arc::new(Mutex::new(LruCache::new(cap))),
            worker_to_scope: Arc::new(Mutex::new(LruCache::new(cap))),
        }
    }
}

impl Default for ScopeCache {
    fn default() -> Self {
        Self::new()
    }
}

async fn cache_get<V: Clone>(
    map: &Mutex<LruCache<String, (V, Instant)>>,
    key: &str,
) -> Option<V> {
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

async fn cache_put<V>(map: &Mutex<LruCache<String, (V, Instant)>>, key: String, value: V) {
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
    let owner = ProjectScope { tenant, project: project_id.to_string() };
    enforce_scope(caller, "project", project_id, &owner)?;
    Ok(owner.tenant)
}

/// WHOSE an execution is: the tenant that owns it and the project it
/// belongs to. One row answers both, so they are read together and
/// can never be a stale tenant against a live project.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectScope {
    pub tenant: String,
    pub project: String,
}

/// Resolve who `color` belongs to, enforcing ownership. See
/// `require_project_owned_by` for the tenant-vs-control-plane rule.
pub async fn require_color_scope(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    color: &str,
) -> Result<ProjectScope, (StatusCode, String)> {
    let scope = lookup_color_scope(cache, pool, color).await?;
    enforce_scope(caller, "color", color, &scope)?;
    Ok(scope)
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
    let owner = lookup_signal_scope(cache, pool, token).await?;
    enforce_scope(caller, "signal", token, &owner)?;
    Ok(owner.tenant)
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
    owner: &ProjectScope,
) -> Result<(), (StatusCode, String)> {
    let Some(caller_tenant) = caller.scope.pinned_tenant() else {
        // Control plane: not pinned to anything, trusted to act for
        // any tenant, validated per-op against the resource itself.
        return Ok(());
    };
    if caller_tenant != owner.tenant {
        log_denied(caller, kind, resource, &owner.tenant);
        return Err((StatusCode::FORBIDDEN, format!("{kind} not owned by caller")));
    }
    // A pinned caller is ONE project's, not one tenant's. A sibling
    // project of the same tenant is no more its business than another
    // tenant's would be: it is where that project's definition, its
    // infrastructure and its credentials live, and the caller can
    // name one in a request body. Checked HERE so no verb can be the
    // one that forgot to ask.
    if caller.scope.pinned_project().is_some_and(|p| p != owner.project) {
        log_denied(caller, &format!("{kind} (project)"), resource, &owner.project);
        return Err((StatusCode::FORBIDDEN, format!("{kind} belongs to a different project")));
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
        .map_err(|e| internal(anyhow::anyhow!("project lookup: {e}")))?;
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

async fn lookup_color_scope(
    cache: &ScopeCache,
    pool: &PgPool,
    color: &str,
) -> Result<ProjectScope, (StatusCode, String)> {
    if let Some(scope) = cache_get(&cache.color_to_scope, color).await {
        return Ok(scope);
    }
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT tenant_id, project_id FROM execution_color WHERE color = $1")
            .bind(color)
            .fetch_optional(pool)
            .await
            .map_err(|e| internal(anyhow::anyhow!("color lookup: {e}")))?;
    let (tenant, project) = row.ok_or((StatusCode::NOT_FOUND, "unknown color".into()))?;
    let scope = ProjectScope { tenant, project };
    cache_put(&cache.color_to_scope, color.to_string(), scope.clone()).await;
    Ok(scope)
}

async fn lookup_signal_scope(
    cache: &ScopeCache,
    pool: &PgPool,
    token: &str,
) -> Result<ProjectScope, (StatusCode, String)> {
    if let Some(scope) = cache_get(&cache.signal_to_scope, token).await {
        return Ok(scope);
    }
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT tenant_id, project_id FROM signal WHERE token = $1",
    )
    .bind(token)
    .fetch_optional(pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("signal lookup: {e}")))?;
    let (tenant, project) = row.ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?;
    let scope = ProjectScope { tenant, project };
    cache_put(&cache.signal_to_scope, token.to_string(), scope.clone()).await;
    Ok(scope)
}


/// Pod -> project. The pod's `worker_pod` row is written by the
/// dispatcher before the pod is created, and the token's `pod_name` is
/// kubelet-stamped and unforgeable, so this resolves from trusted
/// state only, never from anything the pod itself supplies. 403 on a
/// pod with no row (forged, or already retired).
async fn lookup_pod_scope(
    pool: &PgPool,
    namespace: &str,
    pod_name: &str,
) -> Result<ProjectScope, (StatusCode, String)> {
    // Matched on the namespace AS WELL AS the name. A pod name is
    // unique within its namespace, not across the cluster, so a name
    // on its own is not an identity: two namespaces may each hold a
    // pod called the same thing. Both halves come from the verified
    // token, and the row records both, so there is no reason to ask
    // with only one of them.
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT p.id::text, p.tenant_id \
         FROM worker_pod wp JOIN project p ON p.id::text = wp.project_id \
         WHERE wp.pod_name = $1 AND wp.namespace = $2",
    )
    .bind(pod_name)
    .bind(namespace)
    .fetch_optional(pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("worker_pod lookup: {e}")))?;
    let (project, tenant) = row.ok_or((
        StatusCode::FORBIDDEN,
        format!(
            "pod '{namespace}/{pod_name}' has no worker_pod row; a worker must have \
             been spawned by the dispatcher to authenticate"
        ),
    ))?;
    Ok(ProjectScope { tenant, project })
}

/// WHICH project a worker is, from its own unforgeable identity.
///
/// The pod, always. Its name is stamped into the token by the kubelet
/// and handed back by the apiserver when the token is reviewed, so it
/// is the caller's identity rather than the caller's claim, and the
/// `worker_pod` row naming its project was written by the dispatcher
/// before the pod existed.
///
/// The pod and NOT the namespace it sits in, even though a namespace
/// could answer the same question for the workers that have one of
/// their own. Three reasons, in order:
///
///   - Only some workers have a namespace to themselves; the rest
///     share one. Answering from the pod answers for all of them the
///     same way, so there is one rule rather than two that have to
///     agree.
///   - A namespace's record has to be torn down when the namespace
///     is, and the delete is asynchronous, so a pod still draining
///     inside a terminating namespace outlives the record that
///     identifies it. The pod's own record is retired against the pod
///     itself and so cannot be early.
///   - A namespace admits a token that is not bound to any pod at
///     all (one minted by hand). This refuses it, because a token
///     with no pod behind it has no identity here.
///
/// Nothing read here comes from the pod, which is what lets a handler
/// hold a request to the project this returns.
///
/// ONE definition, because every plane that authenticates a worker
/// asks this same question, and two answers to it would be two
/// different ideas of who a caller is.
pub async fn lookup_worker_scope(
    cache: &ScopeCache,
    pool: &PgPool,
    namespace: &str,
    pod_name: Option<&str>,
) -> Result<ProjectScope, (StatusCode, String)> {
    let pod_name = pod_name.ok_or((
        StatusCode::FORBIDDEN,
        "worker token is not bound to a pod, so it names no project".to_string(),
    ))?;
    // Cached because this runs on EVERY authenticated request. The key
    // is the pod's full identity, and a pod belongs to one project for
    // its whole life, so a cached answer cannot become wrong; the TTL
    // is what eventually forgets a retired one.
    let key = format!("{namespace}/{pod_name}");
    if let Some(scope) = cache_get(&cache.worker_to_scope, &key).await {
        return Ok(scope);
    }
    let scope = lookup_pod_scope(pool, namespace, pod_name).await?;
    cache_put(&cache.worker_to_scope, key, scope.clone()).await;
    Ok(scope)
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
            scope: CallerScope::Tenant {
                tenant: tenant.to_string(),
                project: format!("{tenant}-project"),
            },
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

    fn owned_by(tenant: &str, project: &str) -> ProjectScope {
        ProjectScope { tenant: tenant.into(), project: project.into() }
    }

    #[test]
    fn tenant_caller_matching_resource_passes() {
        let caller = tenant_caller("acme");
        assert!(enforce_scope(&caller, "project", "p1", &owned_by("acme", "acme-project")).is_ok());
    }

    #[test]
    fn tenant_caller_foreign_resource_rejected() {
        let caller = tenant_caller("acme");
        let err = enforce_scope(&caller, "project", "p1", &owned_by("globex", "globex-project"))
            .unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    /// A worker is ONE project's. A sibling project of its own tenant
    /// holds that project's definition, its infrastructure and its
    /// credentials, and the caller can name one in a request body, so
    /// the tenant matching is not enough on its own.
    #[test]
    fn tenant_caller_sibling_project_rejected() {
        let caller = tenant_caller("acme");
        let err = enforce_scope(&caller, "project", "p2", &owned_by("acme", "another-project"))
            .unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(err.1.contains("different project"), "{}", err.1);
    }

    #[test]
    fn control_plane_caller_any_resource_passes() {
        // The whole point of the trusted pooled pod: it acts for any
        // tenant, and any project inside it. Both a listener and a
        // supervisor are control-plane.
        for role in [Role::Listener, Role::InfraSupervisor] {
            let caller = control_plane_caller(role);
            assert!(
                enforce_scope(&caller, "signal", "tok", &owned_by("any-tenant", "any-project"))
                    .is_ok(),
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
