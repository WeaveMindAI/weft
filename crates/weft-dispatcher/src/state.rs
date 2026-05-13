use std::sync::Arc;

use crate::backend::{InfraBackend, WorkerBackend};
use crate::events::EventBus;
use crate::journal::Journal;
use crate::listener::{ListenerBackend, ListenerPool};
use crate::project_store::ProjectStore;
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
    /// Spawns per-tenant listener instances.
    pub listener_backend: Arc<dyn ListenerBackend>,
    /// Per-tenant listener pool. One entry per active tenant; the
    /// listener multiplexes every project the tenant owns.
    pub listeners: ListenerPool,
    /// Resolves a tenant for a given project. OSS returns `local`;
    /// cloud derives from request auth.
    pub tenant_router: Arc<dyn TenantRouter>,
    /// Resolves a tenant to its kubernetes namespace.
    pub namespace_mapper: Arc<dyn NamespaceMapper>,
    /// Externally-reachable base URL of this dispatcher. Used to
    /// mint user-facing signal URLs (`<base>/signal/<token>`) at
    /// register time. Architecture-4: the dispatcher hosts every
    /// external URL; the listener has no public surface.
    pub public_base_url: String,
    /// Cluster Pod / Service CIDRs. Threaded into rendered tenant
    /// namespace NetworkPolicies so `ipBlock except <cluster-cidrs>`
    /// expresses "internet but not other Pods." Must be the cluster
    /// operator's actual CIDRs; defaults are Kind's.
    pub cluster_pod_cidr: String,
    pub cluster_service_cidr: String,
    /// Kubernetes namespace name of the cluster's ingress controller
    /// (ingress-nginx by default; Traefik / Contour / etc. use
    /// different namespaces). Threaded into rendered sidecar policies
    /// so public-facing sidecars accept ingress from the right
    /// controller.
    pub cluster_ingress_namespace: String,
}
