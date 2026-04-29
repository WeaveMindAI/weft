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
    /// `pulse_id` is the UUID the dispatcher minted; the worker's
    /// `RootSeed.pulse_id` carries the same UUID so the fold and
    /// the live engine push pulses with the same identity, and
    /// `NodeStarted.pulses_absorbed` matches by exact UUID.
    PulseSeeded {
        color: Color,
        pulse_id: String,
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

    /// The current attempt parked on a wake signal. State
    /// transition Running → Suspended on the same execution record.
    NodeSuspended {
        color: Color,
        node_id: String,
        lane: Lane,
        token: String,
        at_unix: u64,
    },

    /// The current attempt's wake signal fired; the same
    /// attempt continues. State transition Suspended → Running on
    /// the same execution record. NOT a new attempt.
    NodeResumed {
        color: Color,
        node_id: String,
        lane: Lane,
        token: String,
        value: Value,
        at_unix: u64,
    },

    /// A retry policy opened a new attempt on the same execution
    /// record after a previous attempt failed. Future-proofing.
    NodeRetried {
        color: Color,
        node_id: String,
        lane: Lane,
        reason: String,
        at_unix: u64,
    },

    /// The user cancelled the run while this node was still
    /// non-terminal (Running or WaitingForInput). Closes the
    /// record to status=Cancelled with `reason` as the error so
    /// the modal shows what happened.
    NodeCancelled {
        color: Color,
        node_id: String,
        lane: Lane,
        reason: String,
        at_unix: u64,
    },

    /// A downstream pulse the engine produced during postprocess.
    /// `pulse_id` is the UUID the engine minted in its pulse table;
    /// `NodeStarted.pulses_absorbed` later lists the same UUID so
    /// replay can flip the pulse to Absorbed by exact match. No
    /// inference, no counting, no prefix-matching.
    PulseEmitted {
        color: Color,
        pulse_id: String,
        source_node: String,
        source_port: String,
        target_node: String,
        target_port: String,
        lane: Lane,
        value: Value,
        at_unix: u64,
    },

    /// An Expand work item ran in the engine's `preprocess`. The
    /// `absorbed_pulse_id` flips to Absorbed; N child-lane pulses
    /// (each carrying the engine-minted `pulse_id`) appear on the
    /// same node bucket. `lane_suffix` per child is the frames the
    /// Expand appended (1 frame for the common case; >1 when
    /// `lane_depth` peels multiple list layers in one operation).
    PulsesExpanded {
        color: Color,
        node_id: String,
        port: String,
        absorbed_pulse_id: String,
        base_lane: Lane,
        children: Vec<ExpandedChildRecord>,
        at_unix: u64,
    },

    /// A Gather work item ran in the engine's `preprocess`. The
    /// `absorbed_pulse_ids` flip to Absorbed; one parent-lane pulse
    /// with `gathered: true` appears on the bucket.
    PulsesGathered {
        color: Color,
        node_id: String,
        port: String,
        absorbed_pulse_ids: Vec<String>,
        parent_lane: Lane,
        pulse_id: String,
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

    /// A suspension's fire could not be delivered (5x retries
    /// exhausted, dispatcher fail-dispatch). The resumed node
    /// gets a synthetic failure value so only that lane fails;
    /// siblings keep running.
    SuspensionFailed {
        color: Color,
        node_id: String,
        token: String,
        reason: String,
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

/// One leaf produced by an Expand work item, journaled as part of
/// a `PulsesExpanded` event. `pulse_id` is the engine-minted UUID
/// of the resulting child pulse so replay can flip it to Absorbed
/// when the consumer's `NodeStarted` lists the same UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandedChildRecord {
    pub pulse_id: String,
    pub lane_suffix: Lane,
    pub value: Value,
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
            | Self::NodeSuspended { color, .. }
            | Self::NodeResumed { color, .. }
            | Self::NodeRetried { color, .. }
            | Self::NodeCancelled { color, .. }
            | Self::PulseEmitted { color, .. }
            | Self::PulsesExpanded { color, .. }
            | Self::PulsesGathered { color, .. }
            | Self::SuspensionRegistered { color, .. }
            | Self::SuspensionResolved { color, .. }
            | Self::SuspensionFailed { color, .. }
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
            Self::NodeSuspended { .. } => "node_suspended",
            Self::NodeResumed { .. } => "node_resumed",
            Self::NodeRetried { .. } => "node_retried",
            Self::NodeCancelled { .. } => "node_cancelled",
            Self::PulseEmitted { .. } => "pulse_emitted",
            Self::PulsesExpanded { .. } => "pulses_expanded",
            Self::PulsesGathered { .. } => "pulses_gathered",
            Self::SuspensionRegistered { .. } => "suspension_registered",
            Self::SuspensionResolved { .. } => "suspension_resolved",
            Self::SuspensionFailed { .. } => "suspension_failed",
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

    /// Push a pulse with the engine-minted UUID. Replay's pulse
    /// table mirrors the worker's because both use the same UUIDs;
    /// `NodeStarted.pulses_absorbed` later flips by exact match.
    fn push_pulse(
        pulses: &mut PulseTable,
        pulse_id: &str,
        color: Color,
        lane: Lane,
        target_node: &str,
        target_port: &str,
        value: Value,
        gathered: bool,
    ) {
        let id = pulse_id.parse().unwrap_or_else(|_| uuid::Uuid::new_v4());
        let mut pulse = Pulse {
            id,
            color,
            lane,
            target_node: target_node.to_string(),
            target_port: target_port.to_string(),
            value,
            status: PulseStatus::Pending,
            gathered,
        };
        pulse.gathered = gathered;
        pulses.entry(target_node.to_string()).or_default().push(pulse);
    }

    for ev in events {
        match ev {
            ExecEvent::ExecutionStarted { .. } => {}
            ExecEvent::PulseSeeded { color: c, pulse_id, node_id, port, lane, value, .. } => {
                push_pulse(
                    &mut pulses,
                    pulse_id,
                    *c,
                    lane.clone(),
                    node_id,
                    port,
                    value.clone(),
                    false,
                );
            }
            ExecEvent::PulseEmitted {
                color: c,
                pulse_id,
                target_node,
                target_port,
                lane,
                value,
                ..
            } => {
                push_pulse(
                    &mut pulses,
                    pulse_id,
                    *c,
                    lane.clone(),
                    target_node,
                    target_port,
                    value.clone(),
                    false,
                );
            }
            ExecEvent::PulsesExpanded {
                color: c,
                node_id,
                port,
                absorbed_pulse_id,
                base_lane,
                children,
                ..
            } => {
                // Mirror engine's `apply_expand`: flip the absorbed
                // pulse to Absorbed (UUID-exact), then push N child
                // pulses on the same node bucket using the engine's
                // UUIDs. Lane = base_lane + lane_suffix (the suffix
                // can be >1 frame when `lane_depth` peels multiple
                // list layers in a single Expand operation).
                if let Some(absorbed_id) = absorbed_pulse_id.parse::<uuid::Uuid>().ok() {
                    if let Some(bucket) = pulses.get_mut(node_id) {
                        if let Some(p) = bucket.iter_mut().find(|p| p.id == absorbed_id) {
                            p.status = PulseStatus::Absorbed;
                        }
                    }
                }
                for child in children {
                    let mut lane = base_lane.clone();
                    lane.extend_from_slice(&child.lane_suffix);
                    push_pulse(
                        &mut pulses,
                        &child.pulse_id,
                        *c,
                        lane,
                        node_id,
                        port,
                        child.value.clone(),
                        false,
                    );
                }
            }
            ExecEvent::PulsesGathered {
                color: c,
                node_id,
                port,
                absorbed_pulse_ids,
                parent_lane,
                pulse_id,
                value,
                ..
            } => {
                let absorbed: Vec<uuid::Uuid> = absorbed_pulse_ids
                    .iter()
                    .filter_map(|s| s.parse().ok())
                    .collect();
                if let Some(bucket) = pulses.get_mut(node_id) {
                    for p in bucket.iter_mut() {
                        if absorbed.contains(&p.id) {
                            p.status = PulseStatus::Absorbed;
                        }
                    }
                }
                push_pulse(
                    &mut pulses,
                    pulse_id,
                    *c,
                    parent_lane.clone(),
                    node_id,
                    port,
                    value.clone(),
                    true,
                );
            }
            ExecEvent::NodeStarted { node_id, lane, input, pulses_absorbed, at_unix, color: c } => {
                let absorbed_uuids: Vec<uuid::Uuid> = pulses_absorbed
                    .iter()
                    .filter_map(|s| s.parse().ok())
                    .collect();
                // UUID-exact absorb. The worker minted these UUIDs
                // when emitting / expanding / gathering; the fold
                // pushed pulses with the same UUIDs above; flipping
                // by exact match reconstructs the engine's state.
                if !absorbed_uuids.is_empty() {
                    if let Some(bucket) = pulses.get_mut(node_id) {
                        for p in bucket.iter_mut() {
                            if absorbed_uuids.contains(&p.id) && p.status == PulseStatus::Pending {
                                p.status = PulseStatus::Absorbed;
                            }
                        }
                    }
                }
                // One execution record per (node, lane). NodeStarted
                // creates it; subsequent Suspended/Resumed/Completed
                // events mutate the same record. If a record already
                // exists at this lane (rare: replayed-after-crash
                // path), keep the original — Started is supposed to
                // be idempotent for the same logical execution.
                let already_exists = executions
                    .get(node_id)
                    .map(|v| v.iter().any(|e| e.color == *c && &e.lane == lane))
                    .unwrap_or(false);
                if !already_exists {
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
                        prior_attempts: Vec::new(),
                    };
                    executions.entry(node_id.clone()).or_default().push(record);
                }
            }
            ExecEvent::NodeSuspended { node_id, lane, token, color: c, .. } => {
                // Same record, state Running → WaitingForInput.
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.lane == lane)
                    {
                        e.status = NodeExecutionStatus::WaitingForInput;
                        e.callback_id = Some(token.clone());
                    }
                }
            }
            ExecEvent::NodeResumed { node_id, lane, token, color: c, .. } => {
                // Same record, state WaitingForInput → Running. The
                // delivery has been consumed; drop the suspension
                // and its pending delivery so a fresh worker
                // doesn't re-deliver. Subsequent NodeCompleted/
                // Failed/Skipped will close the record.
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.lane == lane)
                    {
                        e.status = NodeExecutionStatus::Running;
                        e.callback_id = None;
                    }
                }
                if !token.is_empty() {
                    suspensions.remove(token);
                    pending_deliveries.remove(token);
                }
            }
            ExecEvent::NodeRetried { node_id, lane, reason: _, at_unix, color: c } => {
                // Snapshot the closed-out attempt into
                // `prior_attempts`, reset the live fields, open a
                // new attempt. Future-proofing: today no path emits
                // this; the fold handles it so retries land cleanly.
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.lane == lane)
                    {
                        let attempt = weft_core::exec::NodeAttempt {
                            status: e.status.clone(),
                            started_at: e.started_at,
                            completed_at: e.completed_at,
                            error: e.error.clone(),
                            output: e.output.clone(),
                        };
                        e.prior_attempts.push(attempt);
                        e.status = NodeExecutionStatus::Running;
                        e.error = None;
                        e.output = None;
                        e.started_at = *at_unix;
                        e.completed_at = None;
                    }
                }
            }
            ExecEvent::NodeCancelled { node_id, lane, reason, at_unix, color: c } => {
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.lane == lane)
                    {
                        e.status = NodeExecutionStatus::Cancelled;
                        e.completed_at = Some(*at_unix);
                        e.error = Some(reason.clone());
                        e.callback_id = None;
                    }
                }
            }
            ExecEvent::NodeCompleted { node_id, lane, output, at_unix, color: c } => {
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.lane == lane)
                    {
                        e.status = NodeExecutionStatus::Completed;
                        e.completed_at = Some(*at_unix);
                        e.output = Some(output.clone());
                        e.callback_id = None;
                    }
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
                // Records signal routing info so a fresh worker can
                // identify which lane a delivery belongs to. The
                // execution record's state transition lives in
                // `NodeSuspended` (separate event from the worker).
                suspensions.insert(
                    token.clone(),
                    SuspensionInfo {
                        node_id: node_id.clone(),
                        lane: lane.clone(),
                        spec: spec.clone(),
                        created_at_unix: *at_unix,
                    },
                );
            }
            ExecEvent::SuspensionResolved { token, value, .. } => {
                // A fire happened for this token; stash the value
                // so any worker that spawns next seeds it into its
                // link. The `NodeCompleted` handler above clears
                // both the suspension and the pending delivery
                // once a worker has consumed the fire.
                pending_deliveries.insert(token.clone(), value.clone());
            }
            ExecEvent::SuspensionFailed { token, node_id, reason, .. } => {
                // Dispatcher gave up delivering this fire. Drop the
                // suspension and surface a synthetic failure to the
                // node execution so only that lane fails.
                suspensions.remove(token);
                pending_deliveries.remove(token);
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.callback_id.as_deref() == Some(token.as_str()))
                    {
                        e.status = NodeExecutionStatus::Failed;
                        e.error = Some(format!("suspension fire failed: {reason}"));
                    }
                }
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

#[cfg(test)]
mod fold_pulse_tests {
    //! Round-trip tests for the pulse-table fold. Each test
    //! constructs a journal that mirrors what the engine would
    //! ship for a specific fan-out / fan-in scenario and asserts
    //! the resulting `ExecutionSnapshot.pulses` matches the
    //! engine's pulse table at the same point in time.
    //!
    //! Why these tests matter: fresh-spawn replay rebuilds the
    //! pulse table from these events alone. If a pulse stays
    //! Pending when it shouldn't, `find_ready_nodes` re-dispatches
    //! upstream nodes after every resume.

    use super::*;
    use serde_json::json;
    use uuid::Uuid;
    use weft_core::lane::{Lane, LaneFrame};
    use weft_core::pulse::PulseStatus;

    fn color() -> Color {
        Uuid::nil()
    }

    fn pulse_id() -> String {
        Uuid::new_v4().to_string()
    }

    fn frame(count: u32, index: u32) -> LaneFrame {
        LaneFrame { count, index }
    }

    fn lane(frames: &[LaneFrame]) -> Lane {
        frames.to_vec()
    }

    /// Single-mode pulse: source emits one pulse, target absorbs it.
    /// Snapshot should show that pulse Absorbed, no Pending pulses.
    #[test]
    fn single_emit_then_absorb() {
        let pid = pulse_id();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "a".into(),
                source_port: "out".into(),
                target_node: "b".into(),
                target_port: "in".into(),
                lane: lane(&[]),
                value: json!(1),
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "b".into(),
                lane: lane(&[]),
                input: json!({"in": 1}),
                pulses_absorbed: vec![pid.clone()],
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let bucket = snap.pulses.get("b").expect("bucket b");
        assert_eq!(bucket.len(), 1);
        assert_eq!(bucket[0].id.to_string(), pid);
        assert_eq!(bucket[0].status, PulseStatus::Absorbed);
    }

    /// Expand depth 1: source emits 5 child-lane pulses on edge to
    /// target; target's 5 NodeStarted events absorb each by UUID.
    /// All 5 pulses should be Absorbed, none Pending.
    #[test]
    fn expand_depth_one_emit_side() {
        let mut events = Vec::new();
        let mut child_ids = Vec::new();
        for i in 0..5u32 {
            let pid = pulse_id();
            child_ids.push(pid.clone());
            events.push(ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid,
                source_node: "src".into(),
                source_port: "items".into(),
                target_node: "tgt".into(),
                target_port: "x".into(),
                lane: lane(&[frame(5, i)]),
                value: json!(i),
                at_unix: 0,
            });
        }
        for (i, pid) in child_ids.iter().enumerate() {
            events.push(ExecEvent::NodeStarted {
                color: color(),
                node_id: "tgt".into(),
                lane: lane(&[frame(5, i as u32)]),
                input: json!({"x": i}),
                pulses_absorbed: vec![pid.clone()],
                at_unix: 0,
            });
        }
        let snap = fold_to_snapshot(color(), &events);
        let bucket = snap.pulses.get("tgt").expect("tgt bucket");
        assert_eq!(bucket.len(), 5);
        for p in bucket {
            assert_eq!(p.status, PulseStatus::Absorbed, "pulse {} should be Absorbed", p.id);
        }
    }

    /// Receive-side Expand: source emits one parent pulse to a
    /// `lane_mode: Expand` input port. The engine's preprocess
    /// records `PulsesExpanded` with N children. NodeStarted on
    /// each child absorbs by UUID. Result: 1 absorbed parent + N
    /// absorbed children, zero Pending.
    #[test]
    fn expand_recv_side_with_preprocess_event() {
        let parent_id = pulse_id();
        let child_ids: Vec<String> = (0..3).map(|_| pulse_id()).collect();

        let mut events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: parent_id.clone(),
                source_node: "src".into(),
                source_port: "list".into(),
                target_node: "fanout".into(),
                target_port: "items".into(),
                lane: lane(&[]),
                value: json!([10, 20, 30]),
                at_unix: 0,
            },
            ExecEvent::PulsesExpanded {
                color: color(),
                node_id: "fanout".into(),
                port: "items".into(),
                absorbed_pulse_id: parent_id.clone(),
                base_lane: lane(&[]),
                children: child_ids
                    .iter()
                    .enumerate()
                    .map(|(i, pid)| ExpandedChildRecord {
                        pulse_id: pid.clone(),
                        lane_suffix: lane(&[frame(3, i as u32)]),
                        value: json!(10 * (i as i64 + 1)),
                    })
                    .collect(),
                at_unix: 0,
            },
        ];
        for (i, pid) in child_ids.iter().enumerate() {
            events.push(ExecEvent::NodeStarted {
                color: color(),
                node_id: "fanout".into(),
                lane: lane(&[frame(3, i as u32)]),
                input: json!({"items": 10 * (i + 1)}),
                pulses_absorbed: vec![pid.clone()],
                at_unix: 0,
            });
        }
        let snap = fold_to_snapshot(color(), &events);
        let bucket = snap.pulses.get("fanout").expect("fanout bucket");
        assert_eq!(bucket.len(), 4, "1 parent + 3 children");
        for p in bucket {
            assert_eq!(p.status, PulseStatus::Absorbed, "{:?}", p);
        }
    }

    /// Expand with depth 2: a single Expand operation peels two
    /// list layers; one absorbed parent → 6 children with 2-frame
    /// lane suffixes. All siblings absorbed.
    #[test]
    fn expand_depth_two_in_one_event() {
        let parent_id = pulse_id();
        // 2x3 grid of children
        let mut children = Vec::new();
        let mut ids = Vec::new();
        for i in 0..2u32 {
            for j in 0..3u32 {
                let pid = pulse_id();
                ids.push((i, j, pid.clone()));
                children.push(ExpandedChildRecord {
                    pulse_id: pid,
                    lane_suffix: lane(&[frame(2, i), frame(3, j)]),
                    value: json!([i, j]),
                });
            }
        }
        let mut events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: parent_id.clone(),
                source_node: "src".into(),
                source_port: "grid".into(),
                target_node: "fan".into(),
                target_port: "in".into(),
                lane: lane(&[]),
                value: json!([[1, 2, 3], [4, 5, 6]]),
                at_unix: 0,
            },
            ExecEvent::PulsesExpanded {
                color: color(),
                node_id: "fan".into(),
                port: "in".into(),
                absorbed_pulse_id: parent_id,
                base_lane: lane(&[]),
                children,
                at_unix: 0,
            },
        ];
        for (i, j, pid) in &ids {
            events.push(ExecEvent::NodeStarted {
                color: color(),
                node_id: "fan".into(),
                lane: lane(&[frame(2, *i), frame(3, *j)]),
                input: json!({"in": [i, j]}),
                pulses_absorbed: vec![pid.clone()],
                at_unix: 0,
            });
        }
        let snap = fold_to_snapshot(color(), &events);
        let bucket = snap.pulses.get("fan").expect("fan bucket");
        assert_eq!(bucket.len(), 7);
        for p in bucket {
            assert_eq!(p.status, PulseStatus::Absorbed, "{:?}", p);
        }
    }

    /// Receive-side Gather: 3 child-lane pulses converge into a
    /// single parent-lane pulse. The 3 children flip Absorbed via
    /// `PulsesGathered`; the gathered pulse is then absorbed by
    /// the gather node's NodeStarted.
    #[test]
    fn gather_receive_side() {
        let child_ids: Vec<String> = (0..3).map(|_| pulse_id()).collect();
        let gathered_id = pulse_id();

        let mut events = Vec::new();
        for (i, pid) in child_ids.iter().enumerate() {
            events.push(ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "leaf".into(),
                source_port: "out".into(),
                target_node: "gather".into(),
                target_port: "items".into(),
                lane: lane(&[frame(3, i as u32)]),
                value: json!(i),
                at_unix: 0,
            });
        }
        events.push(ExecEvent::PulsesGathered {
            color: color(),
            node_id: "gather".into(),
            port: "items".into(),
            absorbed_pulse_ids: child_ids.clone(),
            parent_lane: lane(&[]),
            pulse_id: gathered_id.clone(),
            value: json!([0, 1, 2]),
            at_unix: 0,
        });
        events.push(ExecEvent::NodeStarted {
            color: color(),
            node_id: "gather".into(),
            lane: lane(&[]),
            input: json!({"items": [0, 1, 2]}),
            pulses_absorbed: vec![gathered_id.clone()],
            at_unix: 0,
        });

        let snap = fold_to_snapshot(color(), &events);
        let bucket = snap.pulses.get("gather").expect("gather bucket");
        assert_eq!(bucket.len(), 4, "3 children + 1 gathered parent");
        for p in bucket {
            assert_eq!(p.status, PulseStatus::Absorbed, "{:?}", p);
        }
        // Specifically the gathered pulse must have `gathered: true`
        // so the engine's preprocess won't re-collapse it.
        let gathered = bucket
            .iter()
            .find(|p| p.id.to_string() == gathered_id)
            .expect("gathered pulse present");
        assert!(gathered.gathered, "gathered flag preserved on replay");
    }

    /// Nested Expand → Gather: source emits a list-of-lists; first
    /// Expand peels the outer dimension (2 children), Gather then
    /// collapses each pair of inner siblings back to the parent.
    /// At the end, all intermediate pulses are Absorbed and only
    /// the final emit point (downstream of the gather) is Pending
    /// or absorbed depending on whether the consumer started.
    #[test]
    fn nested_expand_then_gather_resume_state() {
        // Topology: src → fanout(Expand) → leaf(Single) →
        //                gather(Gather) → consumer
        // Lane shape after first Expand: [frame(2, i)].
        // Second Expand peels another dim: [frame(2, i), frame(2, j)].
        // Gather depth-2 collapses inner: [frame(2, i)].
        // Outer gather collapses: [].
        let parent_id = pulse_id();
        let outer_children: Vec<String> = (0..2).map(|_| pulse_id()).collect();
        let inner_children: Vec<Vec<String>> =
            (0..2).map(|_| (0..2).map(|_| pulse_id()).collect()).collect();
        let inner_gathered: Vec<String> = (0..2).map(|_| pulse_id()).collect();
        let outer_gathered = pulse_id();

        let mut events = Vec::new();
        // 1. Parent pulse arrives at fanout.
        events.push(ExecEvent::PulseEmitted {
            color: color(),
            pulse_id: parent_id.clone(),
            source_node: "src".into(),
            source_port: "list".into(),
            target_node: "fanout".into(),
            target_port: "in".into(),
            lane: lane(&[]),
            value: json!([[1, 2], [3, 4]]),
            at_unix: 0,
        });
        // 2. fanout's preprocess expands depth 1 (outer).
        events.push(ExecEvent::PulsesExpanded {
            color: color(),
            node_id: "fanout".into(),
            port: "in".into(),
            absorbed_pulse_id: parent_id,
            base_lane: lane(&[]),
            children: outer_children
                .iter()
                .enumerate()
                .map(|(i, pid)| ExpandedChildRecord {
                    pulse_id: pid.clone(),
                    lane_suffix: lane(&[frame(2, i as u32)]),
                    value: json!([(i + 1), (i + 2)]),
                })
                .collect(),
            at_unix: 0,
        });
        // 3. Each fanout lane fires; emits to leaf (still expand-2)
        for (i, outer_pid) in outer_children.iter().enumerate() {
            events.push(ExecEvent::NodeStarted {
                color: color(),
                node_id: "fanout".into(),
                lane: lane(&[frame(2, i as u32)]),
                input: json!({}),
                pulses_absorbed: vec![outer_pid.clone()],
                at_unix: 0,
            });
            events.push(ExecEvent::NodeCompleted {
                color: color(),
                node_id: "fanout".into(),
                lane: lane(&[frame(2, i as u32)]),
                output: json!({}),
                at_unix: 0,
            });
            // each fanout lane emits 2 child-lane pulses to leaf
            for (j, leaf_pid) in inner_children[i].iter().enumerate() {
                events.push(ExecEvent::PulseEmitted {
                    color: color(),
                    pulse_id: leaf_pid.clone(),
                    source_node: "fanout".into(),
                    source_port: "out".into(),
                    target_node: "leaf".into(),
                    target_port: "x".into(),
                    lane: lane(&[frame(2, i as u32), frame(2, j as u32)]),
                    value: json!(0),
                    at_unix: 0,
                });
            }
        }
        // 4. Each leaf lane runs and emits a result to gather (single).
        let mut leaf_results: Vec<String> = Vec::new();
        for i in 0..2 {
            for j in 0..2 {
                let leaf_pid = &inner_children[i][j];
                events.push(ExecEvent::NodeStarted {
                    color: color(),
                    node_id: "leaf".into(),
                    lane: lane(&[frame(2, i as u32), frame(2, j as u32)]),
                    input: json!({"x": 0}),
                    pulses_absorbed: vec![leaf_pid.clone()],
                    at_unix: 0,
                });
                events.push(ExecEvent::NodeCompleted {
                    color: color(),
                    node_id: "leaf".into(),
                    lane: lane(&[frame(2, i as u32), frame(2, j as u32)]),
                    output: json!({"out": j as i64}),
                    at_unix: 0,
                });
                let pid = pulse_id();
                leaf_results.push(pid.clone());
                events.push(ExecEvent::PulseEmitted {
                    color: color(),
                    pulse_id: pid,
                    source_node: "leaf".into(),
                    source_port: "out".into(),
                    target_node: "gather".into(),
                    target_port: "items".into(),
                    lane: lane(&[frame(2, i as u32), frame(2, j as u32)]),
                    value: json!(j),
                    at_unix: 0,
                });
            }
        }
        // 5. Inner gather (2 per outer lane) → emits at outer lane.
        for i in 0..2 {
            let absorb_ids = vec![leaf_results[i * 2].clone(), leaf_results[i * 2 + 1].clone()];
            events.push(ExecEvent::PulsesGathered {
                color: color(),
                node_id: "gather".into(),
                port: "items".into(),
                absorbed_pulse_ids: absorb_ids,
                parent_lane: lane(&[frame(2, i as u32)]),
                pulse_id: inner_gathered[i].clone(),
                value: json!([0, 1]),
                at_unix: 0,
            });
        }
        // 6. Outer gather collapses to root lane.
        events.push(ExecEvent::PulsesGathered {
            color: color(),
            node_id: "gather".into(),
            port: "items".into(),
            absorbed_pulse_ids: inner_gathered.clone(),
            parent_lane: lane(&[]),
            pulse_id: outer_gathered.clone(),
            value: json!([[0, 1], [0, 1]]),
            at_unix: 0,
        });

        // Resume point: gather hasn't run yet. The outer gathered
        // pulse should be Pending; everything upstream Absorbed.
        let snap = fold_to_snapshot(color(), &events);
        let bucket = snap.pulses.get("gather").expect("gather bucket");
        let pending: Vec<_> = bucket.iter().filter(|p| p.status == PulseStatus::Pending).collect();
        assert_eq!(
            pending.len(),
            1,
            "exactly one Pending pulse at gather (outer gathered, awaiting dispatch)",
        );
        assert_eq!(pending[0].id.to_string(), outer_gathered);
        assert_eq!(pending[0].lane, lane(&[]));
        assert!(pending[0].gathered);

        // No upstream node should re-fire on resume: leaf and
        // fanout have no Pending pulses left.
        assert!(
            snap.pulses
                .get("leaf")
                .map(|b| b.iter().all(|p| p.status == PulseStatus::Absorbed))
                .unwrap_or(true),
            "leaf bucket all absorbed"
        );
        assert!(
            snap.pulses
                .get("fanout")
                .map(|b| b.iter().all(|p| p.status == PulseStatus::Absorbed))
                .unwrap_or(true),
            "fanout bucket all absorbed"
        );
    }

    /// End-to-end shape of the `hello` workflow: seed source →
    /// source emits list → double expand → 5 doubles → sum gather
    /// → review (suspends). At the resume point only `review` has
    /// a Pending pulse; nothing upstream re-fires.
    ///
    /// This is the regression test for the bug where `PulseSeeded`
    /// didn't carry a `pulse_id`, so the fold minted a fresh UUID
    /// for the seed pulse and the `NodeStarted source` event
    /// (carrying the engine's actual UUID) failed to absorb it.
    /// Fresh-spawn replay then re-fired source after every resume.
    #[test]
    fn hello_shape_seed_through_suspend() {
        let seed_pid = pulse_id();
        let source_emit_pid = pulse_id();
        let expand_children: Vec<String> = (0..5).map(|_| pulse_id()).collect();
        let double_emit_pids: Vec<String> = (0..5).map(|_| pulse_id()).collect();
        let gathered_pid = pulse_id();
        let sum_emit_pid = pulse_id();

        let mut events = Vec::new();
        events.push(ExecEvent::PulseSeeded {
            color: color(),
            pulse_id: seed_pid.clone(),
            node_id: "source".into(),
            port: "__seed__".into(),
            lane: lane(&[]),
            value: json!(null),
            at_unix: 0,
        });
        events.push(ExecEvent::NodeStarted {
            color: color(),
            node_id: "source".into(),
            lane: lane(&[]),
            input: json!({}),
            pulses_absorbed: vec![seed_pid.clone()],
            at_unix: 0,
        });
        events.push(ExecEvent::NodeCompleted {
            color: color(),
            node_id: "source".into(),
            lane: lane(&[]),
            output: json!({"numbers": [1, 2, 3, 4, 5]}),
            at_unix: 0,
        });
        events.push(ExecEvent::PulseEmitted {
            color: color(),
            pulse_id: source_emit_pid.clone(),
            source_node: "source".into(),
            source_port: "numbers".into(),
            target_node: "double".into(),
            target_port: "n".into(),
            lane: lane(&[]),
            value: json!([1, 2, 3, 4, 5]),
            at_unix: 0,
        });
        events.push(ExecEvent::PulsesExpanded {
            color: color(),
            node_id: "double".into(),
            port: "n".into(),
            absorbed_pulse_id: source_emit_pid,
            base_lane: lane(&[]),
            children: expand_children
                .iter()
                .enumerate()
                .map(|(i, pid)| ExpandedChildRecord {
                    pulse_id: pid.clone(),
                    lane_suffix: lane(&[frame(5, i as u32)]),
                    value: json!(i + 1),
                })
                .collect(),
            at_unix: 0,
        });
        for (i, pid) in expand_children.iter().enumerate() {
            events.push(ExecEvent::NodeStarted {
                color: color(),
                node_id: "double".into(),
                lane: lane(&[frame(5, i as u32)]),
                input: json!({"n": i + 1}),
                pulses_absorbed: vec![pid.clone()],
                at_unix: 0,
            });
            events.push(ExecEvent::NodeCompleted {
                color: color(),
                node_id: "double".into(),
                lane: lane(&[frame(5, i as u32)]),
                output: json!({"doubled": (i + 1) * 8}),
                at_unix: 0,
            });
            events.push(ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: double_emit_pids[i].clone(),
                source_node: "double".into(),
                source_port: "doubled".into(),
                target_node: "sum".into(),
                target_port: "values".into(),
                lane: lane(&[frame(5, i as u32)]),
                value: json!((i + 1) * 8),
                at_unix: 0,
            });
        }
        events.push(ExecEvent::PulsesGathered {
            color: color(),
            node_id: "sum".into(),
            port: "values".into(),
            absorbed_pulse_ids: double_emit_pids.clone(),
            parent_lane: lane(&[]),
            pulse_id: gathered_pid.clone(),
            value: json!([8, 16, 24, 32, 40]),
            at_unix: 0,
        });
        events.push(ExecEvent::NodeStarted {
            color: color(),
            node_id: "sum".into(),
            lane: lane(&[]),
            input: json!({"values": [8, 16, 24, 32, 40]}),
            pulses_absorbed: vec![gathered_pid.clone()],
            at_unix: 0,
        });
        events.push(ExecEvent::NodeCompleted {
            color: color(),
            node_id: "sum".into(),
            lane: lane(&[]),
            output: json!({"total": 120}),
            at_unix: 0,
        });
        events.push(ExecEvent::PulseEmitted {
            color: color(),
            pulse_id: sum_emit_pid.clone(),
            source_node: "sum".into(),
            source_port: "total".into(),
            target_node: "review".into(),
            target_port: "total".into(),
            lane: lane(&[]),
            value: json!(120),
            at_unix: 0,
        });
        events.push(ExecEvent::NodeStarted {
            color: color(),
            node_id: "review".into(),
            lane: lane(&[]),
            input: json!({"total": 120}),
            pulses_absorbed: vec![sum_emit_pid],
            at_unix: 0,
        });
        // Suspension registered (no NodeCompleted yet); review's
        // exec stays Running. Fold treats it as non-terminal in
        // apply_snapshot.

        let snap = fold_to_snapshot(color(), &events);
        // Source: seed pulse Absorbed.
        let src = snap.pulses.get("source").expect("source bucket");
        assert!(
            src.iter().all(|p| p.status == PulseStatus::Absorbed),
            "source bucket all absorbed (no re-fire on resume)"
        );
        // Double: 1 absorbed parent + 5 absorbed children.
        let dbl = snap.pulses.get("double").expect("double bucket");
        assert_eq!(dbl.len(), 6);
        assert!(
            dbl.iter().all(|p| p.status == PulseStatus::Absorbed),
            "double bucket all absorbed"
        );
        // Sum: 5 absorbed children + 1 absorbed gathered.
        let sum_b = snap.pulses.get("sum").expect("sum bucket");
        assert_eq!(sum_b.len(), 6);
        assert!(
            sum_b.iter().all(|p| p.status == PulseStatus::Absorbed),
            "sum bucket all absorbed"
        );
        // Review: 1 absorbed pulse (NodeStarted absorbed it).
        let rev = snap.pulses.get("review").expect("review bucket");
        assert_eq!(rev.len(), 1);
        assert_eq!(rev[0].status, PulseStatus::Absorbed);
    }

    /// Lifecycle model: NodeStarted opens an exec record;
    /// NodeSuspended flips its state (no new record); NodeResumed
    /// flips back; NodeCompleted closes it. Result: exactly ONE
    /// record per (node, lane) regardless of how many times the
    /// engine had to dispatch.
    #[test]
    fn lifecycle_one_record_per_lane() {
        use weft_core::exec::NodeExecutionStatus;

        let pid = pulse_id();
        let token = "tok-1".to_string();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "review".into(),
                target_port: "in".into(),
                lane: lane(&[]),
                value: json!(42),
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[]),
                input: json!({"in": 42}),
                pulses_absorbed: vec![pid],
                at_unix: 0,
            },
            ExecEvent::NodeSuspended {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[]),
                token: token.clone(),
                at_unix: 0,
            },
            ExecEvent::NodeResumed {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[]),
                token: token.clone(),
                value: json!("approved"),
                at_unix: 0,
            },
            ExecEvent::NodeCompleted {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[]),
                output: json!({"decision_approved": true}),
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let execs = snap.executions.get("review").expect("review execs");
        assert_eq!(execs.len(), 1, "one record per (node, lane)");
        assert_eq!(execs[0].status, NodeExecutionStatus::Completed);
        assert!(execs[0].output.is_some());
        // Suspension is consumed: no leftover entry in suspensions
        // or pending_deliveries.
        assert!(snap.suspensions.is_empty(), "suspensions cleared after resume+complete");
        assert!(snap.pending_deliveries.is_empty());
    }

    /// Mid-suspension snapshot: a (node, lane) is parked. The
    /// fold should leave the record in WaitingForInput, with the
    /// suspension info preserved in `suspensions`. No completed_at.
    #[test]
    fn lifecycle_mid_suspension_state() {
        use weft_core::exec::NodeExecutionStatus;
        use weft_core::primitive::WakeSignalSpec;

        let pid = pulse_id();
        let token = "tok-park".to_string();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "review".into(),
                target_port: "in".into(),
                lane: lane(&[]),
                value: json!(7),
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[]),
                input: json!({"in": 7}),
                pulses_absorbed: vec![pid],
                at_unix: 0,
            },
            ExecEvent::NodeSuspended {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[]),
                token: token.clone(),
                at_unix: 0,
            },
            ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[]),
                token: token.clone(),
                spec: WakeSignalSpec {
                    is_resume: true,
                    kind: weft_core::primitive::WakeSignalKind::Form {
                        form_type: "human_query".into(),
                        schema: weft_core::primitive::FormSchema {
                            title: String::new(),
                            description: None,
                            fields: Vec::new(),
                        },
                        title: None,
                        description: None,
                    },
                },
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let execs = snap.executions.get("review").expect("review execs");
        assert_eq!(execs.len(), 1);
        assert_eq!(execs[0].status, NodeExecutionStatus::WaitingForInput);
        assert_eq!(execs[0].callback_id.as_deref(), Some(token.as_str()));
        assert!(execs[0].completed_at.is_none());
        assert_eq!(snap.suspensions.len(), 1);
    }

    /// Fan-out suspend: 5 lanes all park; only one fire arrives.
    /// The journal records 5 NodeStarted + 5 NodeSuspended; then
    /// the resolved lane fires NodeResumed + NodeCompleted. The
    /// other 4 lanes stay WaitingForInput. Critical: only ONE
    /// record per lane, regardless of any worker churn upstream.
    #[test]
    fn lifecycle_partial_resume_no_spurious_starts() {
        use weft_core::exec::NodeExecutionStatus;
        use weft_core::primitive::WakeSignalSpec;

        let mut events = Vec::new();
        let mut tokens = Vec::new();
        // Open all 5 lanes.
        for i in 0..5 {
            let pid = pulse_id();
            events.push(ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "double".into(),
                source_port: "doubled".into(),
                target_node: "review".into(),
                target_port: "total".into(),
                lane: lane(&[frame(5, i as u32)]),
                value: json!(i * 8),
                at_unix: 0,
            });
            events.push(ExecEvent::NodeStarted {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[frame(5, i as u32)]),
                input: json!({"total": i * 8}),
                pulses_absorbed: vec![pid],
                at_unix: 0,
            });
            let tok = format!("tok-{i}");
            tokens.push(tok.clone());
            events.push(ExecEvent::NodeSuspended {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[frame(5, i as u32)]),
                token: tok.clone(),
                at_unix: 0,
            });
            events.push(ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "review".into(),
                lane: lane(&[frame(5, i as u32)]),
                token: tok,
                spec: WakeSignalSpec {
                    is_resume: true,
                    kind: weft_core::primitive::WakeSignalKind::Form {
                        form_type: "human_query".into(),
                        schema: weft_core::primitive::FormSchema {
                            title: String::new(),
                            description: None,
                            fields: Vec::new(),
                        },
                        title: None,
                        description: None,
                    },
                },
                at_unix: 0,
            });
        }
        // Resolve only lane 2.
        events.push(ExecEvent::SuspensionResolved {
            color: color(),
            token: tokens[2].clone(),
            value: json!("approved"),
            at_unix: 0,
        });
        // Worker resumes that one lane (no others fire).
        events.push(ExecEvent::NodeResumed {
            color: color(),
            node_id: "review".into(),
            lane: lane(&[frame(5, 2)]),
            token: tokens[2].clone(),
            value: json!("approved"),
            at_unix: 0,
        });
        events.push(ExecEvent::NodeCompleted {
            color: color(),
            node_id: "review".into(),
            lane: lane(&[frame(5, 2)]),
            output: json!({"decision": "approved"}),
            at_unix: 0,
        });

        let snap = fold_to_snapshot(color(), &events);
        let execs = snap.executions.get("review").expect("review execs");
        assert_eq!(execs.len(), 5, "exactly 5 records, one per lane");

        let by_lane: std::collections::HashMap<u32, &weft_core::exec::NodeExecution> = execs
            .iter()
            .map(|e| (e.lane[0].index, e))
            .collect();
        for i in 0u32..5 {
            let e = by_lane.get(&i).expect("lane present");
            if i == 2 {
                assert_eq!(e.status, NodeExecutionStatus::Completed);
                assert!(e.callback_id.is_none(), "resolved lane has no callback");
            } else {
                assert_eq!(
                    e.status,
                    NodeExecutionStatus::WaitingForInput,
                    "lane {i} still parked"
                );
                assert!(e.callback_id.is_some());
            }
        }
        // 4 suspensions still pending; 1 resolved → consumed.
        assert_eq!(snap.suspensions.len(), 4);
        assert!(snap.pending_deliveries.is_empty());
    }
}
