//! Scope checks. The security-critical surface of the broker.
//!
//! Every endpoint that touches per-tenant data runs one of these
//! checks before delegating to the underlying client. Each check
//! resolves the resource's owning tenant from Postgres, then enforces
//! the caller's scope: a tenant-scoped caller (worker, runs untrusted
//! user code) must match the resource's tenant; a control-plane caller
//! (the listener or the supervisor, trusted, runs our code only) passes
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
/// active-execution count and avoids the perf cliff DashMap's "drop
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
    project_to_tenant: Arc<Mutex<LruCache<uuid::Uuid, (String, Instant)>>>,
    execution_id_to_scope: Arc<Mutex<LruCache<String, (ExecutionScope, Instant)>>>,
    signal_to_scope: Arc<Mutex<LruCache<String, (ProjectScope, Instant)>>>,
}

impl ScopeCache {
    pub fn new() -> Self {
        let cap = NonZeroUsize::new(CACHE_CAPACITY).expect("non-zero capacity");
        Self {
            project_to_tenant: Arc::new(Mutex::new(LruCache::new(cap))),
            execution_id_to_scope: Arc::new(Mutex::new(LruCache::new(cap))),
            signal_to_scope: Arc::new(Mutex::new(LruCache::new(cap))),
        }
    }
}

impl Default for ScopeCache {
    fn default() -> Self {
        Self::new()
    }
}

async fn cache_get<K: std::hash::Hash + Eq + std::borrow::Borrow<Q>, Q: std::hash::Hash + Eq + ?Sized, V: Clone>(
    map: &Mutex<LruCache<K, (V, Instant)>>,
    key: &Q,
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

async fn cache_put<K: std::hash::Hash + Eq, V>(map: &Mutex<LruCache<K, (V, Instant)>>, key: K, value: V) {
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
    project_id: uuid::Uuid,
) -> Result<String, (StatusCode, String)> {
    let tenant = lookup_project_tenant(cache, pool, project_id).await?;
    let owner = ProjectScope { tenant, project: project_id };
    enforce_scope(caller, "project", &project_id.to_string(), &owner)?;
    Ok(owner.tenant)
}

/// WHOSE an execution is: the tenant that owns it and the project it
/// belongs to. One row answers both, so they are read together and
/// can never be a stale tenant against a live project.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectScope {
    pub tenant: String,
    pub project: uuid::Uuid,
}

/// WHOSE an execution is, and who it is for: its project scope, plus the
/// instance its run was started for (`execution.instance_id`, born with
/// the execution and never changed). What every worker call about a run
/// resolves to, so an instance's pick, copy or storage is found from the
/// run itself and never from anything the worker says.
#[derive(Debug, Clone)]
pub struct ExecutionScope {
    pub tenant: String,
    pub project: uuid::Uuid,
    pub instance: Option<weft_core::instance::InstanceId>,
}

impl ExecutionScope {
    pub fn project_scope(&self) -> ProjectScope {
        ProjectScope { tenant: self.tenant.clone(), project: self.project }
    }
}

/// Resolve who `execution_id` belongs to, enforcing ownership. See
/// `require_project_owned_by` for the tenant-vs-control-plane rule.
pub async fn require_execution_id_scope(
    cache: &ScopeCache,
    pool: &PgPool,
    caller: &CallerIdentity,
    execution_id: &str,
) -> Result<ExecutionScope, (StatusCode, String)> {
    let scope = lookup_execution_id_scope(cache, pool, execution_id).await?;
    enforce_scope(caller, "execution_id", execution_id, &scope.project_scope())?;
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
        log_denied(caller, &format!("{kind} (project)"), resource, &owner.project.to_string());
        return Err((StatusCode::FORBIDDEN, format!("{kind} belongs to a different project")));
    }
    Ok(())
}

async fn lookup_project_tenant(
    cache: &ScopeCache,
    pool: &PgPool,
    project_id: uuid::Uuid,
) -> Result<String, (StatusCode, String)> {
    if let Some(t) = cache_get(&cache.project_to_tenant, &project_id).await {
        return Ok(t);
    }
    let row: Option<(String,)> = sqlx::query_as("SELECT tenant_id FROM project WHERE id = $1")
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| crate::handlers::unavailable_or_internal(anyhow::Error::from(e).context("project lookup")))?;
    let tenant = row
        .ok_or((StatusCode::NOT_FOUND, "unknown project".into()))?
        .0;
    cache_put(
        &cache.project_to_tenant,
        project_id,
        tenant.clone(),
    )
    .await;
    Ok(tenant)
}

/// Read `execution_id`'s scope into the cache ahead of the asks that
/// need it. Only a head start: an execution that cannot be read here is
/// read again, and refused with the reason, by the first ask that needs
/// it, so nothing is lost by not answering here.
pub async fn warm_execution_id_scope(cache: &ScopeCache, pool: &PgPool, execution_id: &str) {
    if let Err((_, why)) = lookup_execution_id_scope(cache, pool, execution_id).await {
        tracing::debug!(target: "weft_broker::scope", execution_id, why, "could not read an execution's scope ahead of its asks");
    }
}

async fn lookup_execution_id_scope(
    cache: &ScopeCache,
    pool: &PgPool,
    execution_id: &str,
) -> Result<ExecutionScope, (StatusCode, String)> {
    if let Some(scope) = cache_get(&cache.execution_id_to_scope, execution_id).await {
        return Ok(scope);
    }
    let row: Option<(String, uuid::Uuid, Option<String>)> =
        sqlx::query_as("SELECT tenant_id, project_id, instance_id FROM execution WHERE execution_id = $1")
            .bind(execution_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| crate::handlers::unavailable_or_internal(anyhow::Error::from(e).context("execution lookup")))?;
    let (tenant, project, instance) = row.ok_or((StatusCode::NOT_FOUND, "unknown execution".into()))?;
    let instance = instance
        .map(weft_core::instance::InstanceId::new)
        .transpose()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("corrupt execution.instance_id: {e}")))?;
    let scope = ExecutionScope { tenant, project, instance };
    cache_put(&cache.execution_id_to_scope, execution_id.to_string(), scope.clone()).await;
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
    let row: Option<(String, uuid::Uuid)> = sqlx::query_as(
        "SELECT tenant_id, project_id FROM signal WHERE token = $1",
    )
    .bind(token)
    .fetch_optional(pool)
    .await
    .map_err(|e| crate::handlers::unavailable_or_internal(anyhow::Error::from(e).context("signal lookup")))?;
    let (tenant, project) = row.ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?;
    let scope = ProjectScope { tenant, project };
    cache_put(&cache.signal_to_scope, token.to_string(), scope.clone()).await;
    Ok(scope)
}


fn log_denied(caller: &CallerIdentity, kind: &str, requested: &str, owner: &str) {
    tracing::warn!(
        target: "weft_broker::scope",
        caller_tenant = ?caller.scope.pinned_tenant(),
        caller_role = ?caller.role,
        caller_replica = ?caller.replica,
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
                project: project(&format!("{tenant}-project")),
            },
            role: Role::Worker,
            replica: Some("replica-x".into()),
        }
    }

    fn control_plane_caller(role: Role) -> CallerIdentity {
        CallerIdentity {
            scope: CallerScope::ControlPlane,
            role,
            replica: Some("replica-cp".into()),
        }
    }

    fn project(name: &str) -> uuid::Uuid {
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, name.as_bytes())
    }

    fn owned_by(tenant: &str, name: &str) -> ProjectScope {
        ProjectScope { tenant: tenant.into(), project: project(name) }
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
        // The whole point of the trusted pooled process: it acts for any
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
