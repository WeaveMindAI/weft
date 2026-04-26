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

use weft_core::Color;

#[async_trait]
pub trait WorkerBackend: Send + Sync {
    /// Spawn a worker for this project's execution color.
    /// The worker's image / binary identity is implicit in the
    /// backend (k8s looks up `weft-worker-<project_id>:latest`;
    /// a future cloud backend might pull from a registry). The
    /// dispatcher never handles a host filesystem path anymore.
    async fn spawn_worker(&self, wake: WakeContext) -> anyhow::Result<WorkerHandle>;

    async fn kill_worker(&self, handle: WorkerHandle) -> anyhow::Result<()>;
}

/// Re-export `RootSeed` from core so backends can reference it by
/// the canonical type. Kept here for compatibility with existing
/// callers; new code should use `weft_core::RootSeed` directly.
pub use weft_core::primitive::RootSeed;

/// Minimal handoff passed to `spawn_worker`. The dispatcher-to-worker
/// channel is the WebSocket (`/ws/executions/{color}`), so all the
/// actual wake data lives on the slot and is delivered in the
/// `Start` message after the worker's `Ready` handshake. The spawn
/// call only needs enough to boot the worker and point it at the
/// right socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeContext {
    pub project_id: String,
    pub color: Color,
}

#[derive(Debug, Clone)]
pub struct WorkerHandle {
    pub id: String,
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
    /// external).
    async fn rehydrate(&self) -> anyhow::Result<Vec<AdoptedHandle>> {
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

    /// Stream events from an infra node back to the dispatcher.
    /// Events arrive as serialized JSON payloads.
    async fn stream_events(&self, handle: InfraHandle) -> anyhow::Result<EventStream>;
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
}

#[derive(Debug, Clone)]
pub struct InfraHandle {
    pub id: String,
    /// Cluster-local URL the sidecar is reachable at. Computed by
    /// the backend at provision time (e.g. `http://<svc>.<ns>.svc.cluster.local:PORT`).
    /// None only for backends that can't resolve until the pod is
    /// scheduled; the dispatcher treats None as "not ready yet."
    pub endpoint_url: Option<String>,
}

/// One (project, node) pair adopted from the cluster at startup.
/// The dispatcher seeds these into `InfraRegistry` with the status
/// implied by the Deployment's current replica count.
#[derive(Debug, Clone)]
pub struct AdoptedHandle {
    pub project_id: String,
    pub node_id: String,
    pub handle: InfraHandle,
    /// true when the Deployment is at replicas=1+, false when at 0.
    pub running: bool,
}

pub type EventStream = tokio::sync::mpsc::Receiver<Value>;
