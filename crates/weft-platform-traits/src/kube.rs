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
//! The production impl (`KubeApiClient`) talks to the Kubernetes API in
//! process, one client per process: no `kubectl` is ever started, and
//! a watch replaces a list repeated on a timer.
//!
//! `FakeKube` is an in-memory drop-in for tests. It records every
//! call so tests can assert "this scale was issued with these args."

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

/// Replica-managing workload kinds k8s exposes. Carried on
/// `WorkloadReplicaState.kind` so a caller scaling a workload reaches
/// the right API (the Deployment one or the StatefulSet one).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
}

/// The kinds of resource `KubeWriter::delete_named` deletes by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NamedKind {
    Pod,
    Service,
    Deployment,
}

impl std::fmt::Display for NamedKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pod => "pod",
            Self::Service => "service",
            Self::Deployment => "deployment",
        })
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

/// Options for `KubeWriter::delete_named`. Two orthogonal axes:
///   - `wait`: return only once the resource is gone, or as soon as
///     the apiserver accepted the delete. Listener teardown waits (so
///     a fresh spawn doesn't collide); the worker-pod reaper does not
///     (it shouldn't block the sweep loop).
///   - `foreground_cascade`: a foreground propagation policy, so the
///     resource's dependents (ReplicaSet, Pods) finish deleting
///     before it does. Only meaningful for workloads; Services / Pods
///     don't need it.
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

/// A running watch of workloads (see `KubeReader::watch_replica_state`).
pub type ReplicaWatch = futures::stream::BoxStream<'static, Result<Vec<WorkloadReplicaState>>>;

/// One node port and the Service that holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePortHolder {
    pub namespace: String,
    pub service: String,
    pub port: u16,
}

#[async_trait]
pub trait KubeReader: Send + Sync {
    /// List Deployment + StatefulSet replica state in a namespace,
    /// filtered by `selector` (a Kubernetes label selector). The selector value must match the labels the target
    /// workloads were minted with: `weft.dev/role=infra` for the
    /// user's infra NODES (project namespaces), `infra-supervisor`
    /// for the supervisor's OWN Deployment (tenant namespace). These
    /// are distinct concepts; an exact-match selector for one will
    /// not match the other.
    async fn list_replica_state(
        &self,
        namespace: &str,
        selector: &str,
    ) -> Result<Vec<WorkloadReplicaState>>;

    /// Every change to the Deployments and StatefulSets matching
    /// `selector` in `namespace` (the same set `list_replica_state`
    /// answers), each handed out as the whole matching set once it
    /// changed; the first item is the set as it is now. A dropped
    /// connection is picked back up inside the stream (relisted, then
    /// resumed), so the stream lasts as long as its holder keeps it; an
    /// error item says one look failed, and the stream carries on.
    async fn watch_replica_state(&self, namespace: &str, selector: &str) -> Result<ReplicaWatch>;

    /// The first container's `state.waiting.reason` for a pod, or
    /// `None` if the container isn't waiting (running / not yet
    /// scheduled / pod gone). Any other failure to read the pod
    /// propagates, never dressed up as "not waiting". Used by the worker spawn to detect
    /// `ImagePullBackOff` / `ErrImagePull` early instead of
    /// waiting out the full readiness timeout.
    async fn pod_waiting_reason(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<Option<String>>;

    /// The pod's `status.phase` (`Pending` / `Running` / `Succeeded` /
    /// `Failed`), or `None` when the pod is not visible (deleted, not
    /// yet applied, transient apiserver miss). Callers poll; the
    /// node-test executor watches a run-to-completion pod through
    /// this.
    async fn pod_phase(&self, namespace: &str, pod_name: &str) -> Result<Option<String>>;

    /// Every node port the cluster has handed out, with the Service
    /// holding it. Cluster wide on purpose: a node port is unique
    /// across the whole cluster, so a door picking one has to see
    /// every Service, including the ones weft did not create.
    ///
    /// Read rather than remembered, because the apiserver is what
    /// actually owns these numbers: it hands dynamic allocations out
    /// of the same range a door picks from, and it refuses a Service
    /// asking for a port already taken.
    async fn node_ports(&self) -> Result<Vec<NodePortHolder>>;

    /// One named container's logs (full stdout+stderr as the
    /// apiserver serves them). Named explicitly so a pod that grows a second
    /// container keeps this call unambiguous. Errors when the pod has
    /// no readable logs; the node-test executor reads a completed
    /// pod's report through this.
    async fn pod_logs(&self, namespace: &str, pod_name: &str, container: &str) -> Result<String>;
}

#[async_trait]
pub trait KubeWriter: Send + Sync {
    /// Scale a workload (Deployment or StatefulSet) to `replicas`.
    /// `kind` picks the API it goes through. Idempotent.
    async fn scale_workload(
        &self,
        namespace: &str,
        kind: WorkloadKind,
        name: &str,
        replicas: u32,
    ) -> Result<()>;

    /// Delete a single named resource (Service / Deployment / Pod
    /// / Pod) from `namespace`. One already gone is fine. The wait +
    /// cascade behavior comes from `DeleteOpts`.
    async fn delete_named(
        &self,
        namespace: &str,
        kind: NamedKind,
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

    /// Apply a raw (multi-document) YAML manifest. Use this one when
    /// several resources are rendered together, as the dispatcher's
    /// listener spawn renders a Deployment + Service; for a single
    /// resource, `apply` takes the JSON directly. Both routes end in a
    /// server-side apply through the in-process API client.
    async fn apply_yaml(&self, manifest: &str) -> Result<()>;

    /// Delete a (cluster-scoped) namespace and everything in it.
    /// One already gone is fine, and the call does not block (the namespace
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

    /// Server-side apply a single manifest through the in-process API
    /// client. Idempotent.
    async fn apply(&self, manifest: &serde_json::Value) -> Result<()>;
}

/// Full surface = reader + writer. The blanket impl is auto for
/// anything that implements both, but we declare it as a marker so
/// consumers that need both can take `Arc<dyn KubeClient>` instead
/// of two separate trait objects.
pub trait KubeClient: KubeReader + KubeWriter {}
impl<T: KubeReader + KubeWriter + ?Sized> KubeClient for T {}

// ---------- production impl ----------

mod api;
pub use api::KubeApiClient;

/// The production kube client for wherever this process runs (the
/// pod's service account in the cluster, the kubeconfig outside it).
/// Returns `Arc<dyn KubeClient>` so call sites bind to the trait, not
/// the struct.
pub async fn in_cluster() -> Result<Arc<dyn KubeClient>> {
    KubeApiClient::connect().await
}

// ---------- fake ----------

#[cfg(any(test, feature = "test-helpers"))]
mod fake;

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::{FakeKube, KubeCall};
