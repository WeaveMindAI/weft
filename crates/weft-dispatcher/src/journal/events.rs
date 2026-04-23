//! Event-sourced execution state.
//!
//! The journal records one event per state change reported by the
//! worker (plus a few dispatcher-side events like PulseSeeded at
//! fresh-run time). Folding the event log reconstructs a complete
//! `ExecutionSnapshot`: pulses, executions, active suspensions. This
//! replaces periodic snapshots. Replay is the source of truth; an
//! explicit snapshot blob is just a materialized view of the fold.
//!
//! Why event sourcing: non-deterministic nodes (LLMs, HTTP) are
//! fine because we don't re-call them on replay, we play back the
//! recorded outputs. Dispatcher-side code (`postprocess_output`) is
//! deterministic today, but we record `PulseEmitted` events so
//! replay doesn't depend on that determinism at all: downstream
//! pulses come straight from the events, not from re-running the
//! pure function.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::lane::Lane;
use weft_core::primitive::{ExecutionSnapshot, SuspensionInfo, WakeSignalSpec};
use weft_core::Color;

/// One event in the execution log. Append-only; events are never
/// edited or deleted by the dispatcher. User-initiated cleanup
/// (`weft clean`) is the only path that removes them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecEvent {
    /// A new execution was minted (color + project + first-seed node).
    /// Written once at fresh-run time.
    ExecutionStarted {
        color: Color,
        project_id: String,
        entry_node: String,
        at_unix: u64,
    },

    /// An initial pulse was seeded into the graph (manual-run root,
    /// trigger-fire root with payload, resume post-delivery).
    PulseSeeded {
        color: Color,
        node_id: String,
        port: String,
        lane: Lane,
        value: Value,
        at_unix: u64,
    },

    /// A node was absorbed into a dispatch (ready group picked up,
    /// pulses marked Absorbed, NodeExecution::Running created).
    NodeStarted {
        color: Color,
        node_id: String,
        lane: Lane,
        input: Value,
        /// Ids of the Pulse records the group absorbed. Replay uses
        /// this to flip them Absorbed in the reconstructed pulse
        /// table.
        pulses_absorbed: Vec<String>,
        at_unix: u64,
    },

    NodeCompleted {
        color: Color,
        node_id: String,
        lane: Lane,
        output: Value,
        at_unix: u64,
    },

    NodeFailed {
        color: Color,
        node_id: String,
        lane: Lane,
        error: String,
        at_unix: u64,
    },

    NodeSkipped {
        color: Color,
        node_id: String,
        lane: Lane,
        at_unix: u64,
    },

    /// A downstream pulse that results from a node completing or
    /// failing. The dispatcher derives these from the project's
    /// edges + the output value at record time and writes one per
    /// emitted pulse. Having them explicit means replay doesn't
    /// have to re-run `postprocess_output`.
    PulseEmitted {
        color: Color,
        source_node: String,
        source_port: String,
        target_node: String,
        target_port: String,
        lane: Lane,
        value: Value,
        at_unix: u64,
    },

    /// A node called `await_signal`; the dispatcher recorded its
    /// suspension. The token is what the user-facing URL wraps.
    SuspensionRegistered {
        color: Color,
        node_id: String,
        lane: Lane,
        token: String,
        spec: WakeSignalSpec,
        at_unix: u64,
    },

    /// A wake signal fired. The matching suspension's delivery
    /// value is recorded here. On replay the suspended node re-runs
    /// and its `await_signal` returns this value immediately via
    /// the seeded delivery slot. Journaling the value (not just
    /// the fact of firing) makes the fire durable: any worker that
    /// spawns for this color can read the value from the event log.
    SuspensionResolved {
        color: Color,
        token: String,
        value: Value,
        at_unix: u64,
    },

    /// Worker reported a cost attribution.
    CostReported {
        color: Color,
        service: String,
        model: Option<String>,
        amount_usd: f64,
        metadata: Value,
        at_unix: u64,
    },

    /// Structured log line emitted by a node.
    LogLine {
        color: Color,
        level: String,
        message: String,
        at_unix: u64,
    },

    /// Execution reached a terminal state.
    ExecutionCompleted {
        color: Color,
        outputs: Value,
        at_unix: u64,
    },

    ExecutionFailed {
        color: Color,
        error: String,
        at_unix: u64,
    },

    /// Worker stalled: every lane is either terminal or waiting. No
    /// new progress events until the worker is respawned. This
    /// event exists for UI timelines and crash-loop detection; it
    /// does not itself mutate execution state.
    Stalled { color: Color, at_unix: u64 },

    /// Dispatcher spawned a worker for this color. Used by the
    /// crash-loop detector.
    WorkerSpawned { color: Color, at_unix: u64 },

    /// A worker invocation ended for a reason other than clean
    /// completion. Paired with `WorkerSpawned` to detect loops.
    WorkerCrashed {
        color: Color,
        reason: String,
        at_unix: u64,
    },
}

impl ExecEvent {
    pub fn color(&self) -> Color {
        match self {
            Self::ExecutionStarted { color, .. }
            | Self::PulseSeeded { color, .. }
            | Self::NodeStarted { color, .. }
            | Self::NodeCompleted { color, .. }
            | Self::NodeFailed { color, .. }
            | Self::NodeSkipped { color, .. }
            | Self::PulseEmitted { color, .. }
            | Self::SuspensionRegistered { color, .. }
            | Self::SuspensionResolved { color, .. }
            | Self::CostReported { color, .. }
            | Self::LogLine { color, .. }
            | Self::ExecutionCompleted { color, .. }
            | Self::ExecutionFailed { color, .. }
            | Self::Stalled { color, .. }
            | Self::WorkerSpawned { color, .. }
            | Self::WorkerCrashed { color, .. } => *color,
        }
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::ExecutionStarted { .. } => "execution_started",
            Self::PulseSeeded { .. } => "pulse_seeded",
            Self::NodeStarted { .. } => "node_started",
            Self::NodeCompleted { .. } => "node_completed",
            Self::NodeFailed { .. } => "node_failed",
            Self::NodeSkipped { .. } => "node_skipped",
            Self::PulseEmitted { .. } => "pulse_emitted",
            Self::SuspensionRegistered { .. } => "suspension_registered",
            Self::SuspensionResolved { .. } => "suspension_resolved",
            Self::CostReported { .. } => "cost_reported",
            Self::LogLine { .. } => "log_line",
            Self::ExecutionCompleted { .. } => "execution_completed",
            Self::ExecutionFailed { .. } => "execution_failed",
            Self::Stalled { .. } => "stalled",
            Self::WorkerSpawned { .. } => "worker_spawned",
            Self::WorkerCrashed { .. } => "worker_crashed",
        }
    }
}

// ----- Fold: events -> ExecutionSnapshot -----------------------------

/// Fold a color's event log into the `ExecutionSnapshot` a new
/// worker can resume from. The returned snapshot is the current
/// "live" state; it omits suspensions whose `SuspensionResolved`
/// event has already been recorded, so the resumed worker only sees
/// still-pending suspensions.
pub fn fold_to_snapshot(color: Color, events: &[ExecEvent]) -> ExecutionSnapshot {
    use weft_core::exec::{NodeExecution, NodeExecutionStatus, NodeExecutionTable};
    use weft_core::pulse::{Pulse, PulseStatus, PulseTable};

    let mut pulses: PulseTable = Default::default();
    let mut executions: NodeExecutionTable = Default::default();
    let mut suspensions: HashMap<String, SuspensionInfo> = HashMap::new();
    let mut pending_deliveries: HashMap<String, Value> = HashMap::new();
    // Track which node each PulseSeeded / PulseEmitted pulse went to
    // so NodeStarted can flip them Absorbed. Keyed by
    // (node_id, port, lane).
    let mut pulse_ids_by_location: HashMap<(String, String, Lane), Vec<String>> = HashMap::new();

    for ev in events {
        match ev {
            ExecEvent::ExecutionStarted { .. } => {}
            ExecEvent::PulseSeeded { color: c, node_id, port, lane, value, .. }
            | ExecEvent::PulseEmitted {
                color: c,
                target_node: node_id,
                target_port: port,
                lane,
                value,
                ..
            } => {
                let pulse = Pulse::new(
                    *c,
                    lane.clone(),
                    node_id.clone(),
                    port.clone(),
                    value.clone(),
                );
                pulse_ids_by_location
                    .entry((node_id.clone(), port.clone(), lane.clone()))
                    .or_default()
                    .push(pulse.id.to_string());
                pulses.entry(node_id.clone()).or_default().push(pulse);
            }
            ExecEvent::NodeStarted { node_id, lane, input, pulses_absorbed, at_unix, color: c } => {
                let absorbed_uuids: Vec<uuid::Uuid> = pulses_absorbed
                    .iter()
                    .filter_map(|s| s.parse().ok())
                    .collect();
                // Absorb pulses by (node, lane) location. The ids
                // on the journal's pulses_absorbed list come from
                // the original worker's Pulse::new() call, which
                // the fold's Pulse::new() can't reproduce (new UUID
                // each time). So instead of uuid-matching we drain
                // up to N pending pulses on this bucket, where N is
                // the length of `pulses_absorbed`. Works because
                // pulses in a bucket are FIFO and the original
                // dispatch absorbed the first N.
                if let Some(bucket) = pulses.get_mut(node_id) {
                    let mut remaining = absorbed_uuids.len();
                    for p in bucket.iter_mut() {
                        if remaining == 0 {
                            break;
                        }
                        if p.status == PulseStatus::Pending && &p.lane == lane {
                            p.status = PulseStatus::Absorbed;
                            remaining -= 1;
                        }
                    }
                }
                let record = NodeExecution {
                    id: uuid::Uuid::new_v4(),
                    node_id: node_id.clone(),
                    status: NodeExecutionStatus::Running,
                    pulses_absorbed: absorbed_uuids,
                    dispatch_pulse: uuid::Uuid::new_v4(),
                    error: None,
                    callback_id: None,
                    started_at: *at_unix,
                    completed_at: None,
                    input: Some(input.clone()),
                    output: None,
                    cost_usd: 0.0,
                    logs: Vec::new(),
                    color: *c,
                    lane: lane.clone(),
                };
                executions.entry(node_id.clone()).or_default().push(record);
            }
            ExecEvent::NodeCompleted { node_id, lane, output, at_unix, color: c } => {
                // Find the matching execution record. If it carries
                // a `callback_id` (i.e. it was suspended on a wake
                // signal and just resumed), that fire is now
                // consumed: drop the suspension and its pending
                // delivery so a later respawn doesn't re-deliver.
                let mut consumed_token: Option<String> = None;
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.lane == lane)
                    {
                        consumed_token = e.callback_id.clone();
                        e.status = NodeExecutionStatus::Completed;
                        e.completed_at = Some(*at_unix);
                        e.output = Some(output.clone());
                        e.callback_id = None;
                    }
                }
                if let Some(token) = consumed_token {
                    suspensions.remove(&token);
                    pending_deliveries.remove(&token);
                }
            }
            ExecEvent::NodeFailed { node_id, lane, error, at_unix, color: c } => {
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.lane == lane)
                    {
                        e.status = NodeExecutionStatus::Failed;
                        e.completed_at = Some(*at_unix);
                        e.error = Some(error.clone());
                    }
                }
            }
            ExecEvent::NodeSkipped { node_id, lane, at_unix, color: c } => {
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.lane == lane)
                    {
                        e.status = NodeExecutionStatus::Skipped;
                        e.completed_at = Some(*at_unix);
                    }
                }
            }
            ExecEvent::SuspensionRegistered {
                node_id, lane, token, spec, at_unix, ..
            } => {
                suspensions.insert(
                    token.clone(),
                    SuspensionInfo {
                        node_id: node_id.clone(),
                        lane: lane.clone(),
                        spec: spec.clone(),
                        created_at_unix: *at_unix,
                    },
                );
                // Also mark the corresponding NodeExecution as
                // WaitingForInput so resume logic can tell which
                // nodes are parked vs truly running.
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| &e.lane == lane && e.status == NodeExecutionStatus::Running)
                    {
                        e.status = NodeExecutionStatus::WaitingForInput;
                        e.callback_id = Some(token.clone());
                    }
                }
            }
            ExecEvent::SuspensionResolved { token, value, .. } => {
                // A fire happened for this token; stash the value
                // so any worker that spawns next seeds it into its
                // link. The `NodeCompleted` handler above clears
                // both the suspension and the pending delivery
                // once a worker has consumed the fire.
                pending_deliveries.insert(token.clone(), value.clone());
            }
            ExecEvent::CostReported { .. }
            | ExecEvent::LogLine { .. }
            | ExecEvent::ExecutionCompleted { .. }
            | ExecEvent::ExecutionFailed { .. }
            | ExecEvent::Stalled { .. }
            | ExecEvent::WorkerSpawned { .. }
            | ExecEvent::WorkerCrashed { .. } => {
                // Pure logging events; no state mutation in the
                // fold's output (they're still readable via
                // events_for for timeline UIs).
            }
        }
    }

    ExecutionSnapshot {
        color,
        pulses,
        executions,
        suspensions,
        pending_deliveries,
    }
}
