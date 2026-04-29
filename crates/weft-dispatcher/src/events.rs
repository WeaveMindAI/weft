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
    NodeSuspended { color: Color, node: String, lane: String, token: String, project_id: String },
    NodeResumed { color: Color, node: String, lane: String, token: String, value: serde_json::Value, project_id: String },
    NodeRetried { color: Color, node: String, lane: String, reason: String, project_id: String },
    NodeCancelled { color: Color, node: String, lane: String, reason: String, project_id: String },
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
            | Self::NodeSuspended { project_id, .. }
            | Self::NodeResumed { project_id, .. }
            | Self::NodeRetried { project_id, .. }
            | Self::NodeCancelled { project_id, .. }
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
            | Self::NodeSuspended { color, .. }
            | Self::NodeResumed { color, .. }
            | Self::NodeRetried { color, .. }
            | Self::NodeCancelled { color, .. }
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
///
/// In multi-Pod mode, `publish` also fires a Postgres NOTIFY so
/// subscribers on other Pods see the event. A background listener
/// task on each Pod LISTENs and re-broadcasts to the local bus,
/// ignoring events that originated on the same Pod (deduped via
/// the `from_pod` field in the payload).
#[derive(Clone, Default)]
pub struct EventBus {
    inner: Arc<RwLock<HashMap<String, broadcast::Sender<DispatcherEvent>>>>,
    /// Postgres pool. Set on construction; events get NOTIFY'd via
    /// this. `None` means single-Pod mode (tests + degraded fallback);
    /// publishes still hit local subscribers.
    pg_pool: Option<sqlx::PgPool>,
    /// Identifier for the publishing Pod. Echoed in the NOTIFY
    /// payload so a Pod that hears its own NOTIFY drops it instead
    /// of double-broadcasting.
    pod_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireEvent {
    from_pod: String,
    event: DispatcherEvent,
}

/// The Postgres channel name we use for cross-Pod fanout. Single
/// channel for all events; subscribers filter by project.
pub const NOTIFY_CHANNEL: &str = "weft_dispatcher_events";

impl EventBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an EventBus wired to Postgres for multi-Pod fanout.
    pub fn with_postgres(pg_pool: sqlx::PgPool, pod_id: String) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            pg_pool: Some(pg_pool),
            pod_id: Some(pod_id),
        }
    }

    pub async fn subscribe_project(&self, project_id: &str) -> broadcast::Receiver<DispatcherEvent> {
        let mut inner = self.inner.write().await;
        inner
            .entry(project_id.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    pub async fn publish(&self, event: DispatcherEvent) {
        // Local fanout.
        {
            let inner = self.inner.read().await;
            if let Some(tx) = inner.get(event.project_id()) {
                let _ = tx.send(event.clone());
            }
        }

        // Cross-Pod NOTIFY. Best-effort: if Postgres is wedged we
        // log and move on. SSE clients on other Pods will miss the
        // event but the journal still has it.
        if let (Some(pool), Some(pod_id)) = (&self.pg_pool, &self.pod_id) {
            let wire = WireEvent {
                from_pod: pod_id.clone(),
                event,
            };
            let payload = match serde_json::to_string(&wire) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(target: "weft_dispatcher::events", "serialize: {e}");
                    return;
                }
            };
            // Postgres NOTIFY payload max is 8KB. We don't expect
            // events to approach that, but cap defensively.
            if payload.len() > 7900 {
                tracing::warn!(
                    target: "weft_dispatcher::events",
                    size = payload.len(),
                    "event payload too large for NOTIFY; skipping cross-Pod fanout"
                );
                return;
            }
            // Use the literal SQL form because pg_notify() takes
            // text args directly. Quote escape via sqlx bind.
            let res = sqlx::query("SELECT pg_notify($1, $2)")
                .bind(NOTIFY_CHANNEL)
                .bind(payload)
                .execute(pool)
                .await;
            if let Err(e) = res {
                tracing::warn!(target: "weft_dispatcher::events", "pg_notify: {e}");
            }
        }
    }

    /// Re-broadcast an event received via Postgres NOTIFY. Drops
    /// the event if it originated from this Pod (we already
    /// fanned it out locally on publish).
    pub async fn ingest_remote(&self, raw: &str) {
        let Ok(wire) = serde_json::from_str::<WireEvent>(raw) else {
            tracing::warn!(target: "weft_dispatcher::events", "drop malformed remote event");
            return;
        };
        if let Some(my_pod) = &self.pod_id {
            if &wire.from_pod == my_pod {
                return;
            }
        }
        let inner = self.inner.read().await;
        if let Some(tx) = inner.get(wire.event.project_id()) {
            let _ = tx.send(wire.event);
        }
    }
}
