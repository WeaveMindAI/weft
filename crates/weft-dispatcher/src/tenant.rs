//! Tenant identity.
//!
//! A tenant is a single billing / isolation unit. The built-in default uses one
//! tenant called `local`. Every dispatcher operation that touches a listener, a
//! worker, or a signal threads a `TenantId` through.
//!
//! The `TenantRouter` trait resolves a project's owning tenant; the default
//! answers `local`.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

/// Stable identifier for a tenant. Carries no auth state on its
/// own; resolution is `TenantRouter`'s job.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantId(pub String);

impl TenantId {
    pub fn local() -> Self {
        Self("local".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Picks the owning tenant for a given project. The default returns `local` for
/// every project (no I/O); resolving a project's real owner is a lookup, so the
/// method is async. Every project row already carries its owning `tenant_id`.
///
/// The result is fallible: the tenant a project is keyed by drives worker-process
/// namespacing, storage keying, and signal mounting, so an unresolvable tenant
/// (the project row is gone, or the store errored) must fail LOUD at the caller,
/// not resolve to a sentinel that would silently mis-key or strand state. Every
/// caller already runs in a `Result` context and propagates with `?`.
///
/// A caller that must tolerate a deleted project reads the tenant off the
/// run's own row instead (`ExecutionOwner::tenant`, frozen at its birth), so
/// it never reaches this lookup, which stays uniformly loud.
#[async_trait]
pub trait TenantRouter: Send + Sync {
    async fn tenant_for_project(&self, project_id: uuid::Uuid) -> Result<TenantId>;
}

/// The built-in router: every project belongs to tenant `local`. No I/O; the
/// async signature is satisfied trivially and never errors.
pub struct LocalTenantRouter;

#[async_trait]
impl TenantRouter for LocalTenantRouter {
    async fn tenant_for_project(&self, _project_id: uuid::Uuid) -> Result<TenantId> {
        Ok(TenantId::local())
    }
}

pub fn local_router() -> Arc<dyn TenantRouter> {
    Arc::new(LocalTenantRouter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_router_returns_local_tenant() {
        let r = LocalTenantRouter;
        assert_eq!(
            r.tenant_for_project(uuid::Uuid::from_u128(0x107)).await.unwrap(),
            TenantId::local()
        );
    }
}
