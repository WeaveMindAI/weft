use std::sync::Arc;

use crate::backend::{InfraBackend, WorkerBackend};
use crate::config::DispatcherConfig;
use crate::events::EventBus;
use crate::infra::InfraRegistry;
use crate::journal::Journal;
use crate::listener::{ListenerBackend, ListenerPool};
use crate::project_store::ProjectStore;
use crate::slots::Slots;
use crate::tenant::{NamespaceMapper, TenantRouter};

/// Stable identifier for this dispatcher Pod. Derived from
/// `WEFT_POD_ID` (set explicitly), or from `HOSTNAME` (StatefulSet
/// auto-sets it), or a random uuid for local-process dev.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodId(pub String);

impl PodId {
    pub fn from_env() -> Self {
        if let Ok(v) = std::env::var("WEFT_POD_ID") {
            return Self(v);
        }
        if let Ok(v) = std::env::var("HOSTNAME") {
            return Self(v);
        }
        Self(format!("local-{}", uuid::Uuid::new_v4().simple()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PodId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Top-level dispatcher state. Shared across HTTP handlers via
/// `axum::extract::State`. All fields are `Arc`-friendly.
#[derive(Clone)]
pub struct DispatcherState {
    pub config: Arc<DispatcherConfig>,
    pub pod_id: PodId,
    pub journal: Arc<dyn Journal>,
    /// Direct Postgres pool handle. Owned here (not threaded
    /// through Journal) so lease management, EventBus pub/sub, and
    /// other DB-backed primitives can share connections without
    /// extending the Journal trait into a kitchen sink.
    pub pg_pool: sqlx::PgPool,
    pub workers: Arc<dyn WorkerBackend>,
    pub infra: Arc<dyn InfraBackend>,
    pub projects: ProjectStore,
    pub events: EventBus,
    pub slots: Slots,
    /// Spawns per-tenant listener instances.
    pub listener_backend: Arc<dyn ListenerBackend>,
    /// Per-tenant listener pool. One entry per active tenant; the
    /// listener multiplexes every project the tenant owns.
    pub listeners: ListenerPool,
    /// Provisioned sidecars per (project, node). Populated by
    /// `weft infra up`, cleared by `weft infra down`. Looked up by
    /// `ctx.sidecar_endpoint()` to resolve a node's endpoint URL.
    pub infra_registry: InfraRegistry,
    /// Resolves a tenant for a given project. OSS returns `local`;
    /// cloud derives from request auth.
    pub tenant_router: Arc<dyn TenantRouter>,
    /// Resolves a tenant to its kubernetes namespace.
    pub namespace_mapper: Arc<dyn NamespaceMapper>,
}
