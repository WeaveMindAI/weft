//! Wire contract for the task queue: the kind enum and the typed
//! payload struct for every kind. Both producers (dispatcher,
//! engine `context`) and consumers (the dispatcher and engine
//! pickers) refer to these definitions, so a typo can't drift the
//! two sides apart silently.
//!
//! Each `TaskKind` variant maps to one `*Payload` struct with the
//! exact JSON shape the executor expects. The string returned by
//! `TaskKind::as_str()` is the canonical wire tag persisted to the
//! `task.kind` column.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// Dispatcher: route an incoming entry-fire payload onto a
    /// fresh execution. Producer = the public-fire HTTP handler.
    RouteEntry,
    /// Dispatcher: register a wake signal with the listener and
    /// return its mint info to the worker that asked.
    RegisterSignal,
    /// Dispatcher: provision a sidecar Pod for an infra node.
    ProvisionSidecar,
    /// Dispatcher: spawn a worker Pod for the project's pool.
    SpawnPod,
    /// Dispatcher: fire a held-event signal that the listener
    /// observed (Timer fired, SSE event arrived, future browser
    /// session resolved). Producer = listener (via broker).
    /// Replaces the `/signal/internal-resume` HTTP push: keeping
    /// listener → dispatcher coordination on the task table means
    /// the listener never opens an HTTP connection to the
    /// dispatcher and the trust seam stays at the broker.
    FireSignal,
    /// Worker: run a fresh execution.
    Execute,
    /// Worker: resume a suspended execution after a fire.
    Resume,
    /// Worker: cancel a running execution by color. Addressed to
    /// one pod via `target_pod_name`.
    CancelExecution,
    /// Dispatcher: journal a `CostReported` event on behalf of a
    /// worker. Producers = engine `ctx.report_cost`. Routed
    /// through the task table (not direct journal write) so a
    /// worker pod dying mid-call still has the cost record
    /// committed: the broker's atomic INSERT into `task` is the
    /// durable handoff, and the dispatcher's executor catches up
    /// later regardless of pod state.
    RecordCost,
    /// Dispatcher: journal a `LogLine` event on behalf of a worker.
    /// Same durability rationale as `RecordCost`.
    RecordLog,
}

impl TaskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RouteEntry => "route_entry",
            Self::RegisterSignal => "register_signal",
            Self::ProvisionSidecar => "provision_sidecar",
            Self::SpawnPod => "spawn_pod",
            Self::FireSignal => "fire_signal",
            Self::Execute => "execute",
            Self::Resume => "resume",
            Self::CancelExecution => "cancel_execution",
            Self::RecordCost => "record_cost",
            Self::RecordLog => "record_log",
        }
    }
}

impl From<TaskKind> for String {
    fn from(k: TaskKind) -> String {
        k.as_str().to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPayload {
    pub project_id: String,
    pub color: String,
}

/// Payload for `TaskKind::FireSignal`. Producer = listener; consumer =
/// dispatcher's executor, which calls `dispatch_listener_outcome`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FireSignalPayload {
    pub token: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelExecutionPayload {
    pub project_id: String,
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnPodPayload {
    pub project_id: String,
    pub tenant: String,
    pub namespace: String,
    pub owner_dispatcher: String,
}

/// Payload for `TaskKind::RecordCost`. The dispatcher's executor
/// validates `amount_usd >= 0` and writes the journal event. The
/// broker also rejects negative amounts on enqueue so a malicious
/// worker can't submit and immediately die before validation runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordCostPayload {
    pub color: String,
    pub service: String,
    pub model: Option<String>,
    pub amount_usd: f64,
    pub metadata: serde_json::Value,
}

/// Payload for `TaskKind::RecordLog`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordLogPayload {
    pub color: String,
    pub level: String,
    pub message: String,
}
