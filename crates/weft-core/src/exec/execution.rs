//! Per-dispatch execution record. A `NodeExecution` is created every
//! time a node fires; multiple fires (parallel lanes, loop iterations)
//! produce multiple entries keyed by node id.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lane::Lane;
use crate::Color;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionStatus {
    Running,
    Completed,
    Failed,
    WaitingForInput,
    Skipped,
    Cancelled,
}

impl NodeExecutionStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::WaitingForInput => "waiting_for_input",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Record of one dispatch of a node. Pulses stay pure data carriers;
/// every bit of execution metadata (status, input, output, cost,
/// logs, timing) lives here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecution {
    pub id: uuid::Uuid,
    pub node_id: String,
    pub status: NodeExecutionStatus,
    /// Input pulses consumed by this dispatch.
    pub pulses_absorbed: Vec<uuid::Uuid>,
    /// Pulse this execution is producing on its output ports (used for
    /// callback routing when the node is suspended).
    pub dispatch_pulse: uuid::Uuid,
    pub error: Option<String>,
    /// Suspension token (set while `status == WaitingForInput`).
    pub callback_id: Option<String>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub cost_usd: f64,
    pub logs: Vec<Value>,
    pub color: Color,
    pub lane: Lane,
}

/// One entry per node, growing as each dispatch records its lifecycle.
pub type NodeExecutionTable = BTreeMap<String, Vec<NodeExecution>>;

/// Aggregate status for a node derived from all its executions.
/// Used by the dashboard and by SSE events.
pub fn summarize_status(executions: &[NodeExecution]) -> String {
    if executions.is_empty() {
        return "pending".to_string();
    }
    let total = executions.len();
    let running = executions.iter().filter(|e| matches!(e.status, NodeExecutionStatus::Running | NodeExecutionStatus::WaitingForInput)).count();
    let failed = executions.iter().filter(|e| e.status == NodeExecutionStatus::Failed).count();
    let completed = executions.iter().filter(|e| e.status == NodeExecutionStatus::Completed).count();
    let skipped = executions.iter().filter(|e| e.status == NodeExecutionStatus::Skipped).count();
    let cancelled = executions.iter().filter(|e| e.status == NodeExecutionStatus::Cancelled).count();

    let base = if running > 0 {
        "running"
    } else if cancelled == total {
        "cancelled"
    } else if skipped == total {
        "skipped"
    } else if failed > 0 && completed == 0 {
        "failed"
    } else {
        "completed"
    };

    if total <= 1 {
        return base.to_string();
    }

    let mut parts = Vec::new();
    if completed > 0 { parts.push(format!("{completed} completed")); }
    if failed > 0 { parts.push(format!("{failed} failed")); }
    if running > 0 { parts.push(format!("{running} running")); }
    if skipped > 0 { parts.push(format!("{skipped} skipped")); }
    if cancelled > 0 { parts.push(format!("{cancelled} cancelled")); }
    format!("{base} ({total} executions: {})", parts.join(", "))
}
