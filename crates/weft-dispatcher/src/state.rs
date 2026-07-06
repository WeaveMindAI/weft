use std::sync::Arc;

use crate::authenticator::Authenticator;
use crate::backend::{ProjectBuilder, WorkerBackend};
use crate::events::EventBus;
use crate::journal::Journal;
use crate::listener::{ListenerBackend, ListenerPool};
use crate::project_store::ProjectStore;
use crate::supervisor_pool::{SupervisorBackend, SupervisorPool};
use crate::tenant::TenantRouter;

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
    /// Builds a project's latest saved source on demand so a verb (`run` /
    /// `activate` / infra start) can just be clicked on a not-yet-built (or
    /// edited-since-built) project and it builds first. `None` when a runnable
    /// definition is already registered before the verb (nothing to build): the
    /// generic seam a source-tree build path fills. Whatever that impl needs to do
    /// its work (a builder, a bucket, a registry) it carries itself; the dispatcher
    /// does not hold those.
    pub ensure_built: Option<Arc<dyn ProjectBuilder>>,
    pub projects: ProjectStore,
    pub events: EventBus,
    /// Spawns pooled listener pods.
    pub listener_backend: Arc<dyn ListenerBackend>,
    /// Pooled listener placement. Each listener pod holds signals across
    /// many tenants; placement is per-signal, load-based, with scale-up
    /// and scale-down.
    pub listeners: ListenerPool,
    /// Spawns pooled infra-supervisor pods.
    pub supervisor_backend: Arc<dyn SupervisorBackend>,
    /// Pooled infra-supervisor placement. Each supervisor pod owns the
    /// infra of many projects (exclusive `infra_owner` lease); the pool
    /// scales the pod count up and down by load.
    pub supervisors: SupervisorPool,
    /// Authenticates a user-facing request to the tenant making it. The default
    /// returns `local` for every request (no token); a token-verifying impl reads
    /// the caller's signed token.
    pub authenticator: Arc<dyn Authenticator>,
    /// Resolves the owning tenant for a given project. The default returns
    /// `local`.
    pub tenant_router: Arc<dyn TenantRouter>,
    /// Decides the worker namespace for a project. The default is the structural
    /// has-infra rule. The 3 worker-placement sites route through this so there is
    /// one answer to "where does this worker live."
    pub placement: Arc<dyn crate::placement::PlacementPolicy>,
    /// Decides the sandbox runtime (`runtimeClassName`) for a pod, or none. The
    /// default runs on the host runtime. Held here so any pod spawner shares the
    /// same decision the worker backend uses.
    pub sandbox: Arc<dyn crate::placement::SandboxPolicy>,
    /// Frees a deleted project's stored data, run as the project is removed
    /// (before the project row is dropped). The default (`WipeProjectFiles`)
    /// frees the project's `project/`-scoped runtime files from the object
    /// store. Canonical doc on the `ProjectReclaimer` trait in `placement.rs`.
    pub project_reclaimer: Arc<dyn crate::placement::ProjectReclaimer>,
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
    /// The control-plane namespace: where pooled, trusted, tenant-
    /// agnostic services run (infra-supervisor pods; listener pods).
    /// Defaults to the dispatcher's own namespace.
    pub control_plane_namespace: String,
    /// The in-cluster broker URL the dispatcher proxies the CLI `weft files`
    /// verbs to (the broker owns the runtime-file bucket + metadata; the
    /// dispatcher never touches bytes, it just fronts the CLI as the control
    /// plane). Same URL the worker pods use for the journal.
    pub broker_url: String,
    /// The dispatcher's own projected SA token path, signed onto the broker
    /// storage-admin requests so the broker resolves it to the control plane.
    pub broker_token_path: std::path::PathBuf,
    /// Shared HTTP client for the broker storage-admin proxy.
    pub http: reqwest::Client,
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
    /// `https://live.example.com`, or a nip.io-based origin). The
    /// handshake builds the per-pod caller URL by prefixing
    /// the pod subdomain onto this host. Empty disables live connections.
    pub gateway_base_url: String,
}
