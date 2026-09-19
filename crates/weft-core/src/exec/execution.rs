//! Per-dispatch execution record. A `NodeExecution` is created every
//! time a node fires; multiple fires (parallel loop iterations) produce
//! multiple entries keyed by node id.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::LoopFrames;
use crate::Color;

// SYNC: NodeExecutionStatus <-> packages/weft-graph/src/protocol.ts NodeExecutionStatus
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

/// Record of one node-firing execution. Pulses stay pure data
/// carriers; every bit of execution metadata (status, cost, logs,
/// timing) lives here. The received input records what the firing used,
/// even after stream pulses are consumed. Outputs are reconstructed from
/// emission rows; the record retains which ports were mentioned and closed.
///
/// Suspend-then-resume keeps the same record. The `status`
/// transitions through Running ↔ WaitingForInput on the same
/// record without churning new entries. A location gets a SECOND
/// record only when the first went terminal and the node fired again
/// there (a streaming or bus consumer); `ordinal` tells them apart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecution {
    pub id: uuid::Uuid,
    /// Effective inputs at this firing, derived once under its own birth
    /// context. Inherited display never reinterprets them through a child cut.
    pub received: super::ready::FiringInput,
    pub node_id: String,
    pub status: NodeExecutionStatus,
    pub skip_reason: Option<super::skip::SkipReason>,
    /// Input pulses consumed by this dispatch.
    pub pulses_absorbed: Vec<uuid::Uuid>,
    /// This firing's rank among the records at its `(node, color,
    /// frames)`: 0 for the first, 1 for a second firing after the
    /// first went terminal. Stamped at creation (`next_firing_ordinal`)
    /// on the live side and in the fold alike; the firing's derived
    /// emissions (its termination sweep, a boundary's forwarding) key
    /// on it, so two firings at one location never share a pulse id.
    pub ordinal: usize,
    pub error: Option<String>,
    /// Suspension token (set while `status == WaitingForInput`).
    pub callback_id: Option<String>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    pub cost_usd: f64,
    pub logs: Vec<Value>,
    /// The output ports this firing has put on a wire or closed (an
    /// emission, a close): what its termination sweep leaves alone,
    /// closing every other output port. Kept on the record so the
    /// worker and the fold read one thing.
    #[serde(default)]
    pub mentioned_ports: HashSet<String>,
    /// Explicit output closures, including generator ends already delivered.
    #[serde(default)]
    pub closed_output_ports: HashSet<String>,
    pub color: Color,
    pub frames: LoopFrames,
    /// The run this record was inherited from, when a seeded run reused
    /// it instead of firing the node again (`ExecutionStarted.seed`).
    /// `None` for a firing of this run's own. The record still carries
    /// THIS run's `color` (the table is this run's); the origin is here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<Color>,
}

/// One entry per node, growing as each dispatch records its lifecycle.
pub type NodeExecutionTable = BTreeMap<String, Vec<NodeExecution>>;

/// Aggregate status for a node derived from all its executions.
/// Used by SSE events.
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

/// The ordinal the NEXT record opened at `(node, color, frames)` gets:
/// the count of records already there. Every site that opens a record
/// (the scheduler, the boundary pass, the journal fold) stamps it on
/// `NodeExecution::ordinal` at creation; nothing re-derives it later.
pub fn next_firing_ordinal(
    executions: &NodeExecutionTable,
    node_id: &str,
    color: Color,
    frames: &LoopFrames,
) -> usize {
    executions
        .get(node_id)
        .map(|v| v.iter().filter(|e| e.color == color && &e.frames == frames).count())
        .unwrap_or(0)
}

/// The latest record at `(node, color, frames)`: the firing every
/// row and every sweep about that location is about.
pub fn latest_firing<'a>(
    executions: &'a NodeExecutionTable,
    node_id: &str,
    color: Color,
    frames: &LoopFrames,
) -> Option<&'a NodeExecution> {
    executions.get(node_id)?.iter().rev().find(|e| e.color == color && &e.frames == frames)
}

/// `latest_firing`, mutably: the record a lifecycle row about the
/// location updates.
pub fn latest_firing_mut<'a>(
    executions: &'a mut NodeExecutionTable,
    node_id: &str,
    color: Color,
    frames: &LoopFrames,
) -> Option<&'a mut NodeExecution> {
    executions.get_mut(node_id)?.iter_mut().rev().find(|e| e.color == color && &e.frames == frames)
}
