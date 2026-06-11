//! Pluggable backends.
//!
//! OSS weft ships `K8sWorkerBackend` for worker pod spawn. The
//! closed-source weavemind repo adds cloud implementations plugging
//! into the same `WorkerBackend` trait.
//!
//! Infra provisioning is not a backend here: the dispatcher routes
//! intent through the `infra_lifecycle_command` table, and the
//! per-tenant infra supervisor pod claims those rows and runs
//! kubectl. The dispatcher itself never shells kubectl for user
//! infra.

pub mod k8s_worker;

pub use k8s_worker::K8sWorkerBackend;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait WorkerBackend: Send + Sync {
    /// Spawn a worker Pod for `spec.project_id`'s pool. The Pod claims
    /// any pending `target=worker` task scoped to its project; one Pod
    /// multiplexes many concurrent executions. Cold-start path called
    /// by the dispatcher when no live Pod exists for the project.
    ///
    /// Pod name is chosen by the caller (deterministic from the
    /// spawn task id) so a partial-success retry collides on the
    /// same name instead of creating a second Pod.
    async fn spawn_pod(
        &self,
        pod_name: &str,
        spec: SpawnPodSpec,
    ) -> anyhow::Result<WorkerHandle>;

    async fn kill_pod(&self, pod_name: String, namespace: String) -> anyhow::Result<()>;
}

/// Spec for spawning a worker Pod. The Pod runs the project's
/// hash-tagged image (`weft-worker-<project_id>:<hash>`), claims
/// tasks for that project, and scale-to-zeros when idle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnPodSpec {
    pub project_id: String,
    pub tenant: String,
    /// The PROJECT namespace (`wm-project-<tenant>-<project>`), not
    /// the tenant namespace. Workers live in the project namespace
    /// so cross-project lateral access is contained at the k8s
    /// boundary.
    pub namespace: String,
    /// Dispatcher Pod id stamped on the worker_pod row for traceability.
    pub owner_dispatcher: String,
    /// Binary hash that identifies which worker image to pull.
    /// None when the project has never been built/registered with
    /// a hash by the CLI; backends should fail loudly in that case
    /// rather than fall back to `:latest` so a misconfigured CLI
    /// doesn't silently spawn the wrong image.
    pub binary_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkerHandle {
    /// k8s pod name minted by the spawn (worker_pod PRIMARY KEY).
    pub pod_name: String,
}
