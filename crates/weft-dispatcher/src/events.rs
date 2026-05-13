//! Pub/sub for project and execution events. Two layers:
//!
//!   - **Per-pod broadcast** (`EventBus`): SSE handlers subscribe;
//!     local publishers push directly. Tokio `broadcast::Sender` keyed
//!     by `project_id`.
//!   - **Cross-pod fanout via Postgres LISTEN/NOTIFY**: a publisher
//!     calls `EventBus::publish_local_and_remote`, which (a) pushes
//!     locally so this pod's SSE consumers see it instantly and (b)
//!     issues `NOTIFY weft_dispatcher_events, '<json>'`. A long-lived
//!     LISTEN task on every other pod receives, decodes, and pushes
//!     to its own local broadcast.
//!
//! The split is deliberate: ExecEvent flows through `journal_bridge`
//! which polls `exec_event` independently on every pod (so each pod
//! ends up publishing the same events to its local broadcast). The
//! NOTIFY channel only carries the smaller cross-cutting events that
//! don't sit on the journal path: ProjectRegistered, ProjectActivated,
//! ProjectDeactivated, TriggerUrlChanged. These fit inside Postgres
//! NOTIFY's 8000-byte payload cap with room to spare.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::{broadcast, RwLock};

use weft_core::Color;

/// LISTEN channel name. Single channel for all cross-pod events;
/// receivers route by `project_id` themselves.
const NOTIFY_CHANNEL: &str = "weft_dispatcher_events";

/// An event the dispatcher publishes about some piece of runtime
/// state changing. Tagged enum so SSE serialization matches the
/// spec in the design doc.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DispatcherEvent {
    ExecutionStarted { color: Color, entry_node: String, project_id: String },
    ExecutionCompleted { color: Color, project_id: String, outputs: serde_json::Value },
    ExecutionFailed { color: Color, project_id: String, error: String },
    ExecutionCancelled { color: Color, project_id: String, reason: String },
    NodeStarted { color: Color, node: String, lane: String, input: serde_json::Value, project_id: String },
    NodeSuspended { color: Color, node: String, lane: String, token: String, project_id: String },
    NodeResumed { color: Color, node: String, lane: String, token: String, value: serde_json::Value, project_id: String },
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
            | Self::ExecutionCompleted { project_id, .. }
            | Self::ExecutionFailed { project_id, .. }
            | Self::ExecutionCancelled { project_id, .. }
            | Self::NodeStarted { project_id, .. }
            | Self::NodeSuspended { project_id, .. }
            | Self::NodeResumed { project_id, .. }
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
            | Self::ExecutionCompleted { color, .. }
            | Self::ExecutionFailed { color, .. }
            | Self::ExecutionCancelled { color, .. }
            | Self::NodeStarted { color, .. }
            | Self::NodeSuspended { color, .. }
            | Self::NodeResumed { color, .. }
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

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<RwLock<HashMap<String, broadcast::Sender<DispatcherEvent>>>>,
    /// Postgres pool used by `publish_local_and_remote` for NOTIFY.
    /// `None` for tests or single-pod contexts where the cross-pod
    /// channel isn't wired; in that case `publish_local_and_remote`
    /// falls back to `publish_local_only` and logs once.
    pool: Option<PgPool>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            pool: None,
        }
    }
}

impl EventBus {
    /// In-process-only bus (tests, pre-pool init).
    pub fn local_only() -> Self {
        Self::default()
    }

    /// Bus with cross-pod fanout via Postgres LISTEN/NOTIFY.
    /// Spawns a long-lived LISTEN task that pushes received events
    /// into the local broadcast.
    pub async fn with_notify(pool: PgPool) -> anyhow::Result<Self> {
        let bus = Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            pool: Some(pool.clone()),
        };
        let bus_for_listener = bus.clone();
        tokio::spawn(async move {
            if let Err(e) = run_listener(pool, bus_for_listener).await {
                tracing::error!(
                    target: "weft_dispatcher::events",
                    error = %e,
                    "LISTEN task exited; cross-pod fanout disabled until restart"
                );
            }
        });
        Ok(bus)
    }

    pub async fn subscribe_project(&self, project_id: &str) -> broadcast::Receiver<DispatcherEvent> {
        let mut inner = self.inner.write().await;
        inner
            .entry(project_id.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    /// Push to local subscribers only. Used by `journal_bridge`,
    /// where every pod's bridge polls the journal independently
    /// (the cross-pod fanout for ExecEvent is the journal itself).
    pub async fn publish_local(&self, event: DispatcherEvent) {
        self.publish_local_inner(&event).await;
    }

    /// Push locally AND issue NOTIFY so sibling pods receive it.
    /// Used for the events that don't ride the journal:
    /// ProjectRegistered/Activated/Deactivated, TriggerUrlChanged,
    /// and the ExecutionStarted "fast-path" emitted by run/activate
    /// before the journal_bridge poll picks up the row.
    pub async fn publish(&self, event: DispatcherEvent) {
        self.publish_local_inner(&event).await;
        let Some(pool) = &self.pool else {
            return;
        };
        let payload = match serde_json::to_string(&event) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    target: "weft_dispatcher::events",
                    error = %e,
                    "serialize DispatcherEvent for NOTIFY"
                );
                return;
            }
        };
        // Postgres NOTIFY caps payloads at 8000 bytes (NAMEDATALEN -
        // some). Skip if our event blew that and rely on the local
        // broadcast plus the next poll-based fanout.
        if payload.len() > 7800 {
            tracing::warn!(
                target: "weft_dispatcher::events",
                size = payload.len(),
                kind = ?std::mem::discriminant(&event),
                "DispatcherEvent too large for NOTIFY; cross-pod skipped"
            );
            return;
        }
        if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
            .bind(NOTIFY_CHANNEL)
            .bind(&payload)
            .execute(pool)
            .await
        {
            tracing::error!(
                target: "weft_dispatcher::events",
                error = %e,
                "pg_notify failed"
            );
        }
    }

    async fn publish_local_inner(&self, event: &DispatcherEvent) {
        let inner = self.inner.read().await;
        if let Some(tx) = inner.get(event.project_id()) {
            let _ = tx.send(event.clone());
        }
    }
}

/// Long-lived LISTEN handler. Reconnects on error: PgListener handles
/// connection drops internally, but a fatal error here means we lose
/// cross-pod fanout, which is a real outage signal.
async fn run_listener(pool: PgPool, bus: EventBus) -> anyhow::Result<()> {
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen(NOTIFY_CHANNEL).await?;
    loop {
        match listener.recv().await {
            Ok(notif) => {
                let payload = notif.payload();
                match serde_json::from_str::<DispatcherEvent>(payload) {
                    Ok(event) => bus.publish_local_inner(&event).await,
                    Err(e) => {
                        tracing::warn!(
                            target: "weft_dispatcher::events",
                            error = %e,
                            payload_len = payload.len(),
                            "could not decode NOTIFY payload"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::events",
                    error = %e,
                    "PgListener recv error; PgListener will reconnect"
                );
            }
        }
    }
}
