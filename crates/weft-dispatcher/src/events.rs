//! In-memory pub/sub for project and execution events. The dispatcher
//! emits events here as state transitions happen; SSE handlers
//! subscribe and forward to clients (the CLI's `weft follow`, the
//! VS Code extension's right sidebar).
//!
//! Phase A: in-memory only. Phase B: multi-process dispatchers
//! coordinate via restate pub/sub so an event emitted on instance 1
//! reaches a subscriber on instance 2.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};

use weft_core::Color;

/// An event the dispatcher publishes about some piece of runtime
/// state changing. Tagged enum so SSE serialization matches the
/// spec in the design doc.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DispatcherEvent {
    ExecutionStarted { color: Color, entry_node: String, project_id: String },
    ExecutionSuspended { color: Color, node: String, token: String, metadata: serde_json::Value, project_id: String },
    ExecutionResumed { color: Color, node: String, project_id: String },
    ExecutionCompleted { color: Color, project_id: String, outputs: serde_json::Value },
    ExecutionFailed { color: Color, project_id: String, error: String },
    NodeStatusChanged { color: Color, node: String, status: String, project_id: String },
    NodeStarted { color: Color, node: String, lane: String, input: serde_json::Value, project_id: String },
    NodeCompleted { color: Color, node: String, lane: String, output: serde_json::Value, project_id: String },
    NodeFailed { color: Color, node: String, lane: String, error: String, project_id: String },
    NodeSkipped { color: Color, node: String, lane: String, project_id: String },
    CostReported { color: Color, project_id: String, service: String, amount_usd: f64 },
    TriggerUrlChanged { project_id: String, node_id: String, url: String },
    ProjectRegistered { project_id: String, name: String },
    ProjectActivated { project_id: String },
    ProjectDeactivated { project_id: String },
}

impl DispatcherEvent {
    pub fn project_id(&self) -> &str {
        match self {
            Self::ExecutionStarted { project_id, .. }
            | Self::ExecutionSuspended { project_id, .. }
            | Self::ExecutionResumed { project_id, .. }
            | Self::ExecutionCompleted { project_id, .. }
            | Self::ExecutionFailed { project_id, .. }
            | Self::NodeStatusChanged { project_id, .. }
            | Self::NodeStarted { project_id, .. }
            | Self::NodeCompleted { project_id, .. }
            | Self::NodeFailed { project_id, .. }
            | Self::NodeSkipped { project_id, .. }
            | Self::CostReported { project_id, .. }
            | Self::TriggerUrlChanged { project_id, .. }
            | Self::ProjectRegistered { project_id, .. }
            | Self::ProjectActivated { project_id }
            | Self::ProjectDeactivated { project_id } => project_id,
        }
    }

    pub fn color(&self) -> Option<Color> {
        match self {
            Self::ExecutionStarted { color, .. }
            | Self::ExecutionSuspended { color, .. }
            | Self::ExecutionResumed { color, .. }
            | Self::ExecutionCompleted { color, .. }
            | Self::ExecutionFailed { color, .. }
            | Self::NodeStatusChanged { color, .. }
            | Self::NodeStarted { color, .. }
            | Self::NodeCompleted { color, .. }
            | Self::NodeFailed { color, .. }
            | Self::NodeSkipped { color, .. }
            | Self::CostReported { color, .. } => Some(*color),
            Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::ProjectDeactivated { .. } => None,
        }
    }
}

/// A broadcast bus keyed by project_id. New subscribers get a
/// receiver wired to a bounded channel; drops when the subscriber
/// disconnects.
#[derive(Clone, Default)]
pub struct EventBus {
    inner: Arc<RwLock<HashMap<String, broadcast::Sender<DispatcherEvent>>>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn subscribe_project(&self, project_id: &str) -> broadcast::Receiver<DispatcherEvent> {
        let mut inner = self.inner.write().await;
        inner
            .entry(project_id.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    pub async fn publish(&self, event: DispatcherEvent) {
        let inner = self.inner.read().await;
        if let Some(tx) = inner.get(event.project_id()) {
            // Intentional: drop the event if nobody is listening or
            // the buffer is full. Subscribers that care about every
            // event should use the journal, not the bus.
            let _ = tx.send(event);
        }
    }
}
