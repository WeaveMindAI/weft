//! K8s API surface used by subsystems.
//!
//! Split into three traits so each consumer can ask for the
//! narrowest contract it needs:
//!
//!   - `KubeReader`: read-only operations. Listener uses this to
//!     resolve cross-namespace service URLs; supervisor uses this in
//!     its health loop.
//!   - `KubeWriter`: mutating operations. Supervisor uses this in
//!     its lifecycle loop.
//!   - `KubeClient`: union of both. Convenience when a subsystem
//!     wants the full surface.
//!
//! The production impl (`KubectlClient`) shells out to `kubectl`.
//! `kube-rs` would let us talk the API directly but doubles compile
//! time; for v1 the supervisor's RBAC scope is already enforced
//! cluster-side, so `kubectl` with the projected SA token is
//! equivalent.
//!
//! `FakeKube` is an in-memory drop-in for tests. It records every
//! call so tests can assert "this scale was issued with these args."

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

/// Replica-managing workload kinds k8s exposes. Names carried on
/// `WorkloadReplicaState.kind` so callers can target the right
/// API (`kubectl scale deployment/x` vs `statefulset/x`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
}

impl WorkloadKind {
    /// kubectl resource prefix: `deployment/<name>` or
    /// `statefulset/<name>`.
    pub fn kubectl_prefix(self) -> &'static str {
        match self {
            Self::Deployment => "deployment",
            Self::StatefulSet => "statefulset",
        }
    }
}

/// One workload's replica state. `kind` is what API to call when
/// scaling; `name` is the k8s metadata.name; `labels` carries the
/// weft.dev/* labels the supervisor uses to resolve instance/node.
#[derive(Debug, Clone)]
pub struct WorkloadReplicaState {
    pub kind: WorkloadKind,
    pub name: String,
    pub namespace: String,
    pub desired: i64,
    pub ready: i64,
    pub labels: HashMap<String, String>,
}

/// Three-valued lookup for `KubeReader::deployment_exists`.
/// Distinguishes "not there" (legitimate no-op) from "kubectl
/// could not answer" (apiserver flap, auth blip, etc.). Callers
/// log Errored loudly and back off; NotFound is silent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentLookup {
    Exists,
    NotFound,
    Errored(String),
}

/// Options for `KubeWriter::delete_named`. The two orthogonal
/// axes a `kubectl delete` cares about:
///   - `wait`: block until the resource is gone (`--wait=true`)
///     vs fire-and-forget (`--wait=false`). Listener teardown
///     waits (so a fresh spawn doesn't collide); the worker-pod
///     reaper does not (it shouldn't block the sweep loop).
///   - `foreground_cascade`: `--cascade=foreground` so the
///     resource's dependents (ReplicaSet, Pods) finish deleting
///     before the call returns. Only meaningful for workloads;
///     Services / Pods don't need it.
/// Fields are private: construction goes through the named
/// constructors so the nonsensical combo (`no_wait + cascade`)
/// is unrepresentable. Impls read via `wait()` / `foreground_cascade()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteOpts {
    wait: bool,
    foreground_cascade: bool,
}

impl DeleteOpts {
    /// Block until gone, no cascade. For Services and other
    /// instant deletes the caller wants confirmed-gone.
    pub fn wait() -> Self {
        Self { wait: true, foreground_cascade: false }
    }
    /// Block until gone, foreground cascade. For workloads whose
    /// Pods should drain before the call returns.
    pub fn wait_cascade() -> Self {
        Self { wait: true, foreground_cascade: true }
    }
    /// Fire-and-forget, no cascade. For the worker-pod reaper,
    /// which must not block its sweep loop on a slow delete.
    pub fn no_wait() -> Self {
        Self { wait: false, foreground_cascade: false }
    }

    pub fn waits(&self) -> bool {
        self.wait
    }
    pub fn cascades(&self) -> bool {
        self.foreground_cascade
    }
}

#[async_trait]
pub trait KubeReader: Send + Sync {
    /// List Deployment + StatefulSet replica state in a namespace,
    /// filtered by `selector` (label selector passed to kubectl's
    /// `-l`). Pass `weft.dev/role=infra` for supervisor reads.
    async fn list_replica_state(
        &self,
        namespace: &str,
        selector: &str,
    ) -> Result<Vec<WorkloadReplicaState>>;

    /// Three-valued: Exists / NotFound / Errored. `kubectl scale`
    /// doesn't accept `--ignore-not-found`, so callers that need
    /// to scale a Deployment that might not exist check existence
    /// first and route around NotFound.
    async fn deployment_exists(&self, namespace: &str, name: &str) -> DeploymentLookup;

    /// The first container's `state.waiting.reason` for a pod, or
    /// `None` if the container isn't waiting (running / not yet
    /// scheduled / pod gone). Used by the worker spawn to detect
    /// `ImagePullBackOff` / `ErrImagePull` early instead of
    /// waiting out the full readiness timeout.
    async fn pod_waiting_reason(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<Option<String>>;
}

#[async_trait]
pub trait KubeWriter: Send + Sync {
    /// Scale a workload (Deployment or StatefulSet) to `replicas`.
    /// `kind` picks the kubectl API. Idempotent.
    async fn scale_workload(
        &self,
        namespace: &str,
        kind: WorkloadKind,
        name: &str,
        replicas: u32,
    ) -> Result<()>;

    /// Delete a single named resource (Service / Deployment / Pod
    /// / etc.) from `namespace`. Always `--ignore-not-found`. The
    /// wait + cascade behavior comes from `DeleteOpts`.
    async fn delete_named(
        &self,
        namespace: &str,
        kind: &str,
        name: &str,
        opts: DeleteOpts,
    ) -> Result<()>;

    /// Delete every weft-managed resource matching the label
    /// selector. PVCs whose `metadata.name` appears in
    /// `preserve_pvcs` are kept; every other PVC is deleted. Use
    /// for Terminate; do NOT use for "bounce pods" (that's
    /// `delete_pods`).
    ///
    /// The list comes from `InfraSpec.lifecycle.on_terminate.preserve_pvcs`
    /// (preserved via the `infra_node` row at apply time so the
    /// supervisor can honor it on terminate).
    async fn delete_by_label(
        &self,
        namespace: &str,
        selector: &str,
        preserve_pvcs: &[String],
    ) -> Result<()>;

    /// Delete only the Pod resources matching the selector. The
    /// Deployment / StatefulSet / Service / ConfigMap / Secret /
    /// PVC all survive; the controller respawns Pods with the same
    /// spec. Use for HealthProtocol `BouncePods` actions: the
    /// process gets a fresh start, the surrounding infrastructure
    /// stays put.
    async fn delete_pods(&self, namespace: &str, selector: &str) -> Result<()>;

    /// Apply a raw (multi-document) YAML manifest. The single-JSON
    /// `apply` above is for one server-side-apply call; this one is
    /// for the dispatcher's listener spawn which renders a
    /// Deployment + Service together. Both routes converge on
    /// `kubectl apply -f -` in the production impl.
    async fn apply_yaml(&self, manifest: &str) -> Result<()>;

    /// Delete a (cluster-scoped) namespace and everything in it.
    /// Always `--ignore-not-found` and non-blocking (the namespace
    /// finalizer reaps contents asynchronously). Distinct from
    /// `delete_named`, which deletes a resource WITHIN a namespace.
    async fn delete_namespace(&self, name: &str) -> Result<()>;

    /// Block until a Deployment reaches Ready, or fail after the
    /// timeout. Used by the listener spawn to gate the admin-URL
    /// health probe on k8s actually rolling out the new pods.
    async fn wait_rollout_status(
        &self,
        namespace: &str,
        deployment: &str,
        timeout_seconds: u32,
    ) -> Result<()>;

    /// Pipe a single manifest into `kubectl apply -f -`. Idempotent
    /// from k8s' perspective (server-side apply).
    async fn apply(&self, manifest: &serde_json::Value) -> Result<()>;
}

/// Full surface = reader + writer. The blanket impl is auto for
/// anything that implements both, but we declare it as a marker so
/// consumers that need both can take `Arc<dyn KubeClient>` instead
/// of two separate trait objects.
pub trait KubeClient: KubeReader + KubeWriter {}
impl<T: KubeReader + KubeWriter + ?Sized> KubeClient for T {}

// ---------- production impl ----------

mod kubectl;
pub use kubectl::KubectlClient;

/// Construct a production kube client and sanity-check `kubectl` is
/// on PATH. Returns `Arc<dyn KubeClient>` so call sites bind to the
/// trait, not the struct.
pub async fn in_cluster() -> Result<Arc<dyn KubeClient>> {
    Ok(KubectlClient::in_cluster().await?)
}

// ---------- fake ----------

#[cfg(any(test, feature = "test-helpers"))]
mod fake;

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::{FakeKube, KubeCall};
