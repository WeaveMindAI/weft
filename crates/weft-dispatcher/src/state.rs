use std::sync::Arc;

use crate::backend::WorkerBackend;
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
    /// different namespaces). Threaded into rendered infra-pod policies
    /// so public-facing infra pods accept ingress from the right
    /// controller.
    pub cluster_ingress_namespace: String,
    /// Docker tag of the per-tenant infra-supervisor pod image. The
    /// dispatcher renders + applies a Deployment with this image to
    /// each tenant namespace at first project register.
    pub supervisor_image: String,
    /// Docker tag of the per-tenant storage-box image (lazy-applied
    /// on first storage use; see `storage_box`).
    pub storage_image: String,
    /// In-cluster base URL of THIS dispatcher service (what tenant
    /// pods, e.g. the storage box's grow/shrink requests, call).
    pub internal_base_url: String,
    /// Control-plane client for tenant storage boxes (mint, sweeps,
    /// usage, wipes). Never carries file bytes.
    pub storage_admin: Arc<dyn weft_storage::client::StorageAdminOps>,
    /// Relay to the broker's `/storage/authorize`: how the
    /// dispatcher verifies a storage box's bearer on the internal
    /// grow/shrink endpoints.
    pub broker_authorize: Arc<dyn weft_storage::auth::BrokerAuthorizeOps>,
    /// kube client used by the reaper (supervisor scale-down). The
    /// listener and worker backends hold their own clones of the
    /// same `Arc<dyn KubeClient>` (constructed once in main). The
    /// trait lives in `weft-platform-traits`, shared with the
    /// supervisor crate.
    pub kube: Arc<dyn weft_platform_traits::KubeClient>,
    /// HMAC secret the dispatcher signs live-connection routing tokens
    /// with (the worker verifies with the same secret). Empty when live
    /// connections aren't provisioned (`WEFT_CALLER_TOKEN_SECRET` unset);
    /// the handshake then fails loud rather than minting unverifiable
    /// tokens.
    pub caller_token_secret: Arc<Vec<u8>>,
    /// Public origin of the live-connection gateway (e.g.
    /// `https://live.example.com` in cloud, or the nip.io-based local
    /// origin). The handshake builds the per-pod caller URL by prefixing
    /// the pod subdomain onto this host. Empty disables live connections.
    pub gateway_base_url: String,
}
