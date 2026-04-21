//! Docker-based infra backend for local dev. Spawns a sidecar
//! container per infra node via `docker run`. The sidecar exposes
//! the standard HTTP contract (/health, /outputs, /live, /events,
//! /action). The dispatcher polls /events (SSE) for incoming
//! messages and POSTs /action for outbound commands.
//!
//! Docker is the "kind" in spirit: simplest thing that puts a
//! container-isolated workload on the user's machine. Full k8s
//! (via kind or k3s) is overkill for local dev; enterprise BYOC
//! and cloud use real k8s via a separate backend.
//!
//! Phase A2 covers spawn + deprovision. Event streaming lands when
//! the first infra-bound node ships (Slack, WhatsApp, etc).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};

use crate::backend::{EventStream, InfraBackend, InfraHandle, InfraSpec};

pub struct DockerInfraBackend {
    /// Handle id -> container name, for teardown lookups.
    containers: Arc<Mutex<HashMap<String, String>>>,
}

impl DockerInfraBackend {
    pub fn new() -> Self {
        Self { containers: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl Default for DockerInfraBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl InfraBackend for DockerInfraBackend {
    async fn provision(&self, spec: InfraSpec) -> anyhow::Result<InfraHandle> {
        // Phase A2: read the image from the spec's config. Full
        // sidecar.toml parsing lands when an actual sidecar-bearing
        // node ships.
        let image = spec
            .config
            .get("image")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("infra spec missing 'image'"))?;
        let name = format!("weft-infra-{}-{}", spec.infra_node_id, uuid::Uuid::new_v4().simple());

        let status = Command::new("docker")
            .args(["run", "-d", "--rm", "--name", &name, image])
            .status()
            .await
            .map_err(|e| anyhow::anyhow!("docker run: {e}"))?;
        if !status.success() {
            anyhow::bail!("docker exited with {}", status);
        }

        let handle_id = name.clone();
        self.containers.lock().await.insert(handle_id.clone(), name);
        Ok(InfraHandle { id: handle_id })
    }

    async fn deprovision(&self, handle: InfraHandle) -> anyhow::Result<()> {
        let name = self.containers.lock().await.remove(&handle.id);
        let Some(name) = name else {
            // Nothing known to deprovision: idempotent success.
            return Ok(());
        };
        let _ = Command::new("docker")
            .args(["kill", &name])
            .status()
            .await;
        Ok(())
    }

    async fn stream_events(&self, _handle: InfraHandle) -> anyhow::Result<EventStream> {
        // Phase A2 placeholder: return an empty receiver. When the
        // first infra-backed node lands, wire a background task that
        // subscribes to the container's /events SSE and forwards
        // payloads into the mpsc channel.
        let (_tx, rx) = mpsc::channel(64);
        Ok(rx)
    }
}
