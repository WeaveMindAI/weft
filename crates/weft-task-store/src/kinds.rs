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
    /// worker. Producer = the broker's cost-settle handler (a
    /// worker's `ctx.settle_cost`). Routed
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

// This enum holds only the kinds the dispatcher itself ships. A deployment that
// adds its own task kinds (e.g. an in-cluster image build) registers + enqueues
// them by string via the string-keyed task dispatch, without widening this enum.

impl TaskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RouteEntry => "route_entry",
            Self::RegisterSignal => "register_signal",
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
    /// `running_definition_hash` snapshotted at enqueue time (same
    /// value the journal's `ExecutionStarted` carries). The worker
    /// passes it as `expected_hash` to the broker's
    /// `project_fetch_definition`, which looks the shape up in the
    /// APPEND-ONLY `project_definition` history keyed by
    /// `(project_id, hash)`: the execution always runs on the shape
    /// the user clicked Run against, even when a later edit advances
    /// the project row's hash before the worker claims the task. A
    /// missing history row is a hard 404 (the worker fails the
    /// execution loudly); there is no race semantics on this path,
    /// the hash IS the lookup key.
    pub definition_hash: String,
    /// Present only for executions STARTED by a live-caller handshake (the
    /// dispatcher's `/connect` endpoint). Opaque to this generic store:
    /// carries the trigger's full signal spec JSON (kind tag + config body),
    /// from which the worker recovers BOTH the wire protocol (the tag:
    /// `api_endpoint` -> HTTP, `live_socket` -> WS) and the connection knobs
    /// (the body) to build the `CallerConnection` runtime config and expect
    /// a caller to attach for this color. `None` for every ordinary
    /// pull-queue / resume execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_connection: Option<serde_json::Value>,
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
