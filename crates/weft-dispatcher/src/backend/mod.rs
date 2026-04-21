//! Pluggable backends. OSS weft ships `SubprocessWorkerBackend` +
//! `KindInfraBackend` (local dev). The closed-source weavemind repo
//! adds cloud implementations plugging into the same traits.

pub mod subprocess;

pub use subprocess::SubprocessWorkerBackend;

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::Color;

#[async_trait]
pub trait WorkerBackend: Send + Sync {
    /// Spawn a worker to run the given binary with the given wake
    /// context. Returns a handle that can be used to kill the worker
    /// later.
    async fn spawn_worker(
        &self,
        binary_path: &std::path::Path,
        wake: WakeContext,
    ) -> anyhow::Result<WorkerHandle>;

    async fn kill_worker(&self, handle: WorkerHandle) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeContext {
    pub project_id: String,
    pub color: Color,
    pub resume_node: String,
    pub resume_value: Value,
    #[serde(default)]
    pub kind: WakeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WakeKind {
    #[default]
    Fresh,
    Resume,
}

#[derive(Debug, Clone)]
pub struct WorkerHandle {
    pub id: String,
}

#[async_trait]
pub trait InfraBackend: Send + Sync {
    /// Provision an infra node per the given spec. Returns a handle.
    async fn provision(&self, spec: InfraSpec) -> anyhow::Result<InfraHandle>;

    async fn deprovision(&self, handle: InfraHandle) -> anyhow::Result<()>;

    /// Stream events from an infra node back to the dispatcher.
    /// Events arrive as serialized JSON payloads.
    async fn stream_events(&self, handle: InfraHandle) -> anyhow::Result<EventStream>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraSpec {
    pub project_id: String,
    pub infra_node_id: String,
    /// Reference to a `sidecar.toml` for this infra node.
    pub sidecar_manifest: PathBuf,
    pub config: Value,
}

#[derive(Debug, Clone)]
pub struct InfraHandle {
    pub id: String,
}

pub type EventStream = tokio::sync::mpsc::Receiver<Value>;
