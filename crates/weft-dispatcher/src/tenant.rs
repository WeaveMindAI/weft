//! Tenant identity and namespace resolution.
//!
//! A tenant is a single billing / isolation unit. Cloud sets one
//! per user; OSS hardcodes a single tenant called `local`. Every
//! dispatcher operation that touches a listener, a worker, or a
//! signal threads a `TenantId` through. K8s namespacing falls out
//! of `NamespaceMapper` at the edge.
//!
//! The traits exist so the closed-source cloud crate can plug in
//! per-request resolution (auth → tenant) without forking the
//! dispatcher.

use std::sync::Arc;

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

/// Picks the tenant for a given project. OSS returns `local` for
/// every project; cloud derives from request auth.
pub trait TenantRouter: Send + Sync {
    fn tenant_for_project(&self, project_id: &str) -> TenantId;
}

/// Resolves a tenant to its kubernetes namespace.
pub trait NamespaceMapper: Send + Sync {
    fn namespace_for(&self, tenant: &TenantId) -> String;
}

/// OSS router: every project belongs to tenant `local`.
pub struct LocalTenantRouter;

impl TenantRouter for LocalTenantRouter {
    fn tenant_for_project(&self, _project_id: &str) -> TenantId {
        TenantId::local()
    }
}

/// OSS namespace mapper: tenant `local` lives in `wm-local`.
/// Cloud uses a prefix-based mapper (`wm-user-<id>`) configured at
/// startup.
pub struct PrefixNamespaceMapper {
    prefix: String,
}

impl PrefixNamespaceMapper {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self { prefix: prefix.into() }
    }
}

impl NamespaceMapper for PrefixNamespaceMapper {
    fn namespace_for(&self, tenant: &TenantId) -> String {
        format!("{}{}", self.prefix, tenant.as_str())
    }
}

pub fn local_router() -> Arc<dyn TenantRouter> {
    Arc::new(LocalTenantRouter)
}

pub fn local_namespace_mapper() -> Arc<dyn NamespaceMapper> {
    Arc::new(PrefixNamespaceMapper::new("wm-"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_router_returns_local_tenant() {
        let r = LocalTenantRouter;
        assert_eq!(r.tenant_for_project("any-project"), TenantId::local());
    }

    #[test]
    fn prefix_mapper_concatenates() {
        let m = PrefixNamespaceMapper::new("wm-");
        assert_eq!(m.namespace_for(&TenantId::local()), "wm-local");
        assert_eq!(m.namespace_for(&TenantId("user-x".into())), "wm-user-x");
    }
}
