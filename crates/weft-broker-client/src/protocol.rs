//! Wire types for every broker endpoint. Both the server (`weft-broker`)
//! and the client side import from here, so a typo can't drift the
//! two ends apart.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use weft_journal::ExecEvent;
use weft_task_store::tasks::{ClaimFilter, NewTask, Task, TaskOutcome, TaskStatus, TaskTarget};
use weft_task_store::TaskKind;

// ---------- Journal ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecordRequest {
    pub event: ExecEvent,
    /// Worker pod name for fencing trigger; None for listener writes
    /// (which arch-5 doesn't currently emit, but the field is here
    /// for symmetry with the trait).
    pub pod_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecordResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalFetchRequest {
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalFetchResponse {
    pub events: Vec<ExecEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalHasTerminalRequest {
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalHasTerminalResponse {
    pub terminal: bool,
}

// ---------- Tasks ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnqueueDedupRequest {
    pub spec: NewTaskWire,
}

/// Mirror of `NewTask` for the wire. `TaskKind` and `TaskTarget`
/// derive `Serialize` / `Deserialize` directly (snake_case) so a new
/// variant on the producer side becomes a deserialization error on
/// the broker side instead of a silent string-table miss.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTaskWire {
    pub kind: TaskKind,
    pub target: TaskTarget,
    pub project_id: Option<String>,
    pub dedup_key: Option<String>,
    pub color: Option<String>,
    pub tenant_id: Option<String>,
    pub target_pod_name: Option<String>,
    pub payload: Value,
}

impl NewTaskWire {
    pub fn from_new_task(spec: &NewTask) -> Self {
        Self {
            kind: spec.kind,
            target: spec.target,
            project_id: spec.project_id.clone(),
            dedup_key: spec.dedup_key.clone(),
            color: spec.color.clone(),
            tenant_id: spec.tenant_id.clone(),
            target_pod_name: spec.target_pod_name.clone(),
            payload: spec.payload.clone(),
        }
    }

    pub fn into_new_task(self) -> NewTask {
        NewTask {
            kind: self.kind,
            target: self.target,
            project_id: self.project_id,
            dedup_key: self.dedup_key,
            color: self.color,
            tenant_id: self.tenant_id,
            target_pod_name: self.target_pod_name,
            payload: self.payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnqueueDedupResponse {
    pub id: Uuid,
    pub inserted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWaitTerminalRequest {
    pub task_id: Uuid,
    pub timeout_ms: u64,
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWaitTerminalResponse {
    pub status: TaskStatus,
    pub result: Option<Value>,
    pub error: Option<String>,
}

impl TaskWaitTerminalResponse {
    pub fn from_outcome(o: TaskOutcome) -> Self {
        Self {
            status: o.status,
            result: o.result,
            error: o.error,
        }
    }
    pub fn into_outcome(self) -> TaskOutcome {
        TaskOutcome {
            status: self.status,
            result: self.result,
            error: self.error,
        }
    }
}

impl TaskWaitTerminalRequest {
    pub fn new(task_id: Uuid, timeout: Duration, poll_interval: Duration) -> Self {
        Self {
            task_id,
            timeout_ms: timeout.as_millis() as u64,
            poll_interval_ms: poll_interval.as_millis() as u64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClaimOneRequest {
    pub pod_id: String,
    pub filter: ClaimFilterWire,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClaimFilterWire {
    Dispatcher,
    Worker { project_id: String },
}

impl ClaimFilterWire {
    pub fn from_filter(f: &ClaimFilter) -> Self {
        match f {
            ClaimFilter::Dispatcher => Self::Dispatcher,
            ClaimFilter::Worker { project_id } => Self::Worker {
                project_id: project_id.clone(),
            },
        }
    }
    pub fn into_filter(self) -> ClaimFilter {
        match self {
            Self::Dispatcher => ClaimFilter::Dispatcher,
            Self::Worker { project_id } => ClaimFilter::Worker { project_id },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClaimOneResponse {
    pub task: Option<TaskWire>,
}

/// Wire shape of `Task` (status as the canonical string).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWire {
    pub id: Uuid,
    pub kind: String,
    pub status: TaskStatus,
    pub project_id: Option<String>,
    pub color: Option<String>,
    pub tenant_id: Option<String>,
    pub payload: Value,
}

impl TaskWire {
    pub fn from_task(t: Task) -> Self {
        Self {
            id: t.id,
            kind: t.kind,
            status: t.status,
            project_id: t.project_id,
            color: t.color,
            tenant_id: t.tenant_id,
            payload: t.payload,
        }
    }
    pub fn into_task(self) -> Task {
        Task {
            id: self.id,
            kind: self.kind,
            status: self.status,
            project_id: self.project_id,
            color: self.color,
            tenant_id: self.tenant_id,
            payload: self.payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHeartbeatRequest {
    pub task_id: Uuid,
    pub pod_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHeartbeatResponse {
    pub renewed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleteRequest {
    pub task_id: Uuid,
    pub pod_id: String,
    pub result: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleteResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFailRequest {
    pub task_id: Uuid,
    pub pod_id: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFailResponse {}

// ---------- worker_pod ----------

/// Worker flips its row to `alive` and starts heartbeating. The
/// dispatcher's earlier `insert_spawning` already wrote `namespace`
/// and `owner_dispatcher` from trusted server-side values, so this
/// request only needs the worker's own identity. The broker re-
/// derives the namespace from the SA token if it ever needs it
/// (caller.namespace) rather than trusting wire data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodRegisterAliveRequest {
    pub pod_name: String,
    pub project_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodRegisterAliveResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodHeartbeatRequest {
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodHeartbeatResponse {
    pub renewed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodMarkDoneRequest {
    pub pod_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPodMarkDoneResponse {}

// ---------- Infra ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraSidecarEndpointRequest {
    pub project_id: String,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraSidecarEndpointResponse {
    pub endpoint_url: Option<String>,
}

// ---------- Signals ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalListForTenantRequest {
    pub tenant_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalListForTenantResponse {
    pub rows: Vec<SignalRowWire>,
}

/// Wire shape for a row of the signal table that the listener
/// rehydrates from. Mirrors the columns the listener reads today.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRowWire {
    pub token: String,
    pub node_id: String,
    pub spec_json: String,
    pub is_resume: bool,
    pub color: Option<String>,
    pub surface_kind: String,
    pub mount_path: Option<String>,
    pub auth_kind: String,
    pub auth_config: Option<Value>,
    /// Opaque per-kind state. `{}` for kinds that don't persist
    /// anything. Timer uses it to recover the absolute
    /// `next_fire_at_unix` across listener restarts; other stateful
    /// kinds use the same channel for whatever state they need to
    /// survive a restart.
    #[serde(default)]
    pub kind_state: Value,
}
