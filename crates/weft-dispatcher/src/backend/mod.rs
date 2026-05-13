//! Pluggable backends. OSS weft ships `K8sWorkerBackend` +
//! `KindInfraBackend` (local dev on a kind cluster). The
//! closed-source weavemind repo adds cloud implementations
//! plugging into the same traits.

pub mod k8s_worker;
pub mod kind_infra;

pub use kind_infra::KindInfraBackend;
pub use k8s_worker::K8sWorkerBackend;


use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub namespace: String,
    /// Dispatcher Pod id stamped on the worker_pod row for traceability.
    pub owner_dispatcher: String,
    /// Source hash that identifies which worker image to pull.
    /// None when the project has never been built/registered with
    /// a hash by the CLI; backends should fail loudly in that case
    /// rather than fall back to `:latest` so a misconfigured CLI
    /// doesn't silently spawn the wrong image.
    pub source_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkerHandle {
    /// k8s pod name minted by the spawn (worker_pod PRIMARY KEY).
    pub pod_name: String,
}

#[async_trait]
pub trait InfraBackend: Send + Sync {
    /// Apply the sidecar's Deployment / Service / PVC manifests.
    /// Called the first time a project's infra is brought up, and
    /// again after `delete`. Idempotent: applying a spec that
    /// already matches the cluster state is a no-op.
    async fn provision(&self, spec: InfraSpec) -> anyhow::Result<InfraHandle>;

    /// Scan the cluster for already-provisioned sidecars belonging
    /// to weft projects and return their handles. Called on
    /// dispatcher startup so a restart doesn't orphan resources.
    /// Default is empty (backends that don't persist anything
    /// external). The dispatcher passes the list of tenant
    /// namespaces it knows about so the backend doesn't have to
    /// guess where to look.
    async fn rehydrate(&self, _namespaces: &[String]) -> anyhow::Result<Vec<AdoptedHandle>> {
        Ok(Vec::new())
    }

    /// Scale the sidecar's Deployment to 0 replicas. Keeps the
    /// Deployment / Service / PVC in place so a subsequent
    /// `scale_up` can bring the same instance (and its persisted
    /// state) back without re-apply.
    async fn scale_to_zero(&self, handle: &InfraHandle) -> anyhow::Result<()>;

    /// Scale a previously-zeroed Deployment back to 1. Paired with
    /// `scale_to_zero`; no-op if the Deployment is already running.
    async fn scale_up(&self, handle: &InfraHandle) -> anyhow::Result<()>;

    /// Block until the sidecar's `/health` endpoint answers 200 OK,
    /// or the deadline passes. The dispatcher calls this after
    /// `provision` / `scale_up` so the user-facing `start` response
    /// only returns once a subsequent `activate` can safely query
    /// `/outputs` without racing the Pod's readiness. Default impl
    /// polls `{endpoint_url}/health` every 500ms.
    async fn wait_ready(&self, handle: &InfraHandle) -> anyhow::Result<()> {
        let Some(endpoint) = handle.endpoint_url.as_deref() else {
            return Ok(());
        };
        let health = format!(
            "{}/health",
            endpoint.trim_end_matches('/').trim_end_matches("/action")
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let client = reqwest::Client::new();
        loop {
            if let Ok(resp) = client
                .get(&health)
                .timeout(std::time::Duration::from_secs(2))
                .send()
                .await
            {
                if resp.status().is_success() {
                    return Ok(());
                }
            }
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("sidecar at {health} did not become ready within 60s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    /// Delete every k8s resource owned by this handle: Deployment,
    /// Service, Ingress, PVC. Idempotent. After `delete` the
    /// instance's state is gone; `provision` has to apply fresh.
    async fn delete(&self, handle: InfraHandle) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraSpec {
    pub project_id: String,
    pub infra_node_id: String,
    /// Sidecar declaration mirrored from the node's metadata.
    pub sidecar: weft_core::node::SidecarSpec,
    /// Project's per-instance config overrides, read from the node
    /// definition's config block. Backend handlers can use this for
    /// env vars, resource limits, etc.
    #[serde(default)]
    pub config: Value,
    /// Tenant + namespace resolved by the dispatcher. The backend
    /// never resolves these on its own.
    pub tenant: String,
    pub namespace: String,
    /// Source-hash that identifies which sidecar image to pull.
    /// Becomes the docker tag suffix (`weft-sidecar-<name>:<hash>`).
    /// None when the dispatcher couldn't resolve a hash for this
    /// node (e.g. a CLI-less invocation); backends should fail
    /// loudly rather than fall back to `:latest` in that case.
    #[serde(default, rename = "sidecarHash", alias = "sidecar_hash")]
    pub sidecar_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InfraHandle {
    pub id: String,
    /// Cluster-local URL the sidecar is reachable at. Computed by
    /// the backend at provision time (e.g. `http://<svc>.<ns>.svc.cluster.local:PORT`).
    /// None only for backends that can't resolve until the pod is
    /// scheduled; the dispatcher treats None as "not ready yet."
    pub endpoint_url: Option<String>,
    /// Namespace the sidecar lives in. Set at provision time so
    /// every later op (scale, delete, port-forward) can target the
    /// right namespace without consulting an env var.
    pub namespace: String,
}

/// One (project, node) pair adopted from the cluster at startup.
/// The dispatcher upserts these into `infra_pod` with the status
/// implied by the Deployment's current replica count.
#[derive(Debug, Clone)]
pub struct AdoptedHandle {
    pub project_id: String,
    pub node_id: String,
    pub handle: InfraHandle,
    /// true when the Deployment is at replicas=1+, false when at 0.
    pub running: bool,
}
