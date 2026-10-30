//! Per-dispatch run record. A `NodeFiring` is created every
//! time a node fires; multiple fires (parallel loop iterations) produce
//! multiple entries keyed by node id.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Deref;

use indexmap::IndexMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::{FiringLocation, LoopFrames};
use crate::RunId;

// SYNC: NodeFiringStatus <-> packages/weft-graph/src/protocol.ts NodeFiringStatus
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeFiringStatus {
    Running,
    Completed,
    Failed,
    WaitingForInput,
    Skipped,
    Cancelled,
}

impl NodeFiringStatus {
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

/// Record of one node-firing run. Pulses stay pure data
/// carriers; every bit of run metadata (status, cost, logs,
/// timing) lives here. The received input records what the firing used,
/// even after stream pulses are consumed. Outputs are reconstructed from
/// emission rows; the record retains which ports were mentioned and closed.
///
/// Suspend-then-resume keeps the same record. The `status`
/// transitions through Running and WaitingForInput on the same
/// record without churning new entries. One run fires a node
/// once per frames, so a location holds at most one record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeFiring {
    pub id: uuid::Uuid,
    /// Effective inputs at this firing, derived once under its own birth
    /// context. Inherited display never reinterprets them through a child cut.
    pub received: super::ready::FiringInput,
    pub node_id: String,
    /// Changed through the table it sits in ([`NodeFiringTable::set_status`]),
    /// which keeps where its open, waiting and failed firings are.
    status: NodeFiringStatus,
    pub skip_reason: Option<super::skip::SkipReason>,
    /// Input pulses consumed by this dispatch.
    pub pulses_absorbed: Vec<uuid::Uuid>,
    pub error: Option<String>,
    /// The firing failed on a wait it gave up at a time limit
    /// (`WeftError::WaitGaveUp`): a run this failure ends reached a time
    /// limit, and its caller hears it as one. Carried by the `NodeFailed`
    /// row, so whichever worker ends the run reads it here.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub gave_up_wait: bool,
    /// Suspension token (set while `status == WaitingForInput`).
    pub callback_id: Option<String>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    pub cost_usd: f64,
    pub logs: Vec<Value>,
    /// The output ports this firing has put on a wire or closed (an
    /// emission, a close): what its termination sweep leaves alone,
    /// closing every other output port, and what a body resumed after
    /// a pause starts from, so its replayed emissions on these ports
    /// send nothing. Kept on the record so the worker and the fold
    /// read one thing.
    #[serde(default)]
    pub mentioned_ports: HashSet<String>,
    /// Explicit output closures, including generator ends already delivered.
    #[serde(default)]
    pub closed_output_ports: HashSet<String>,
    pub run_id: RunId,
    pub frames: LoopFrames,
    /// The run this record was inherited from, when a seeded run reused
    /// it instead of firing the node again (`RunStarted.seed`).
    /// `None` for a firing of this run's own. The record still carries
    /// THIS run's `run_id` (the table is this run's); the origin is here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<RunId>,
}

impl NodeFiring {
    /// The record of a firing that starts now, `Running`, having taken
    /// `pulses_absorbed`.
    pub fn started(
        node_id: &str,
        frames: LoopFrames,
        run_id: RunId,
        received: super::ready::FiringInput,
        pulses_absorbed: Vec<uuid::Uuid>,
        started_at: u64,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            received,
            node_id: node_id.to_string(),
            status: NodeFiringStatus::Running,
            skip_reason: None,
            pulses_absorbed,
            error: None,
            gave_up_wait: false,
            callback_id: None,
            started_at,
            completed_at: None,
            cost_usd: 0.0,
            logs: Vec::new(),
            mentioned_ports: HashSet::new(),
            closed_output_ports: HashSet::new(),
            run_id,
            frames,
            inherited_from: None,
        }
    }

    pub fn status(&self) -> NodeFiringStatus {
        self.status
    }

    /// Whether this firing has a stream open: something went out (a bus's
    /// marker, a generator's first item) on one of `stream_ports`, its
    /// node's outputs that open one (`WeftType::opens_a_stream`), and it
    /// was not closed since. Read off the record, so the worker and the
    /// fold agree, and it decides whether a failure may still be caught
    /// (`context::caught_failure`).
    pub fn opened_a_stream<'a>(&self, mut stream_ports: impl Iterator<Item = &'a str>) -> bool {
        stream_ports.any(|port| self.mentioned_ports.contains(port) && !self.closed_output_ports.contains(port))
    }
}

/// One node's firings, keyed by the frames each fired at, in the order
/// they started.
pub type NodeFirings = IndexMap<LoopFrames, NodeFiring>;

/// Every firing of a run, per node: one record per frames, in the order
/// the firings started, found by their frames without walking the others
/// (a loop body's node has one record per iteration, and every row about
/// one of them looks it up). Beside them, where the firings that have not
/// ended are, which of those wait for input and which failed: what every
/// turn of a drive asks, answered without walking every firing the run
/// ever had. A record's status changes only here ([`Self::set_status`]),
/// which keeps those three in step.
#[derive(Debug, Clone, Default)]
pub struct NodeFiringTable {
    nodes: BTreeMap<String, NodeFirings>,
    open: BTreeSet<FiringLocation>,
    waiting: BTreeSet<FiringLocation>,
    failed: BTreeSet<FiringLocation>,
}

impl NodeFiringTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `record` after its node's others: a firing that just started,
    /// or one a seeded run imported. A location holds at most one record,
    /// so the caller has made sure none is there; a second one is an
    /// engine invariant broken, and panics (a drive catches it and fails
    /// the run) rather than leave the open, waiting and failed firings
    /// naming a record that was replaced.
    pub fn insert(&mut self, record: NodeFiring) {
        let at = FiringLocation::new(record.node_id.clone(), record.frames.clone());
        assert!(self.at(&at.node_id, &at.frames).is_none(), "a second record of '{}' at frames {:?}: a node fires once per frames", at.node_id, at.frames);
        self.index(&at, record.status);
        self.nodes.entry(record.node_id.clone()).or_default().insert(record.frames.clone(), record);
    }

    /// The record at `(node, frames)`.
    pub fn at(&self, node_id: &str, frames: &LoopFrames) -> Option<&NodeFiring> {
        self.nodes.get(node_id)?.get(frames)
    }

    /// The record at `(node, frames)`, to change anything but its status.
    pub fn at_mut(&mut self, node_id: &str, frames: &LoopFrames) -> Option<&mut NodeFiring> {
        self.nodes.get_mut(node_id)?.get_mut(frames)
    }

    /// Move the record at `(node, frames)` to `status`. Answers the
    /// record, `None` when there is none.
    pub fn set_status(&mut self, node_id: &str, frames: &LoopFrames, status: NodeFiringStatus) -> Option<&mut NodeFiring> {
        let at = FiringLocation::new(node_id, frames.clone());
        self.nodes.get_mut(node_id)?.get_mut(frames)?.status = status;
        self.open.remove(&at);
        self.waiting.remove(&at);
        self.failed.remove(&at);
        self.index(&at, status);
        self.at_mut(node_id, frames)
    }

    /// Whether a firing has not ended yet (running, or waiting for input).
    pub fn any_open(&self) -> bool {
        !self.open.is_empty()
    }

    /// The firings that have not ended yet, in place order.
    pub fn open(&self) -> impl Iterator<Item = &NodeFiring> {
        self.records(&self.open)
    }

    /// The firings parked on a wait (`WaitingForInput`), in place order.
    pub fn waiting(&self) -> impl Iterator<Item = &NodeFiring> {
        self.records(&self.waiting)
    }

    /// The firings that failed, in place order.
    pub fn failed(&self) -> impl Iterator<Item = &NodeFiring> {
        self.records(&self.failed)
    }

    fn records<'a>(&'a self, places: &'a BTreeSet<FiringLocation>) -> impl Iterator<Item = &'a NodeFiring> {
        places.iter().map(|at| self.at(&at.node_id, &at.frames).expect("an indexed firing has its record"))
    }

    fn index(&mut self, at: &FiringLocation, status: NodeFiringStatus) {
        match status {
            NodeFiringStatus::Running => {
                self.open.insert(at.clone());
            }
            NodeFiringStatus::WaitingForInput => {
                self.open.insert(at.clone());
                self.waiting.insert(at.clone());
            }
            NodeFiringStatus::Failed => {
                self.failed.insert(at.clone());
            }
            NodeFiringStatus::Completed | NodeFiringStatus::Skipped | NodeFiringStatus::Cancelled => {}
        }
    }
}

/// Read like the map it holds: per node, its firings. Changes go through
/// the table's own methods.
impl Deref for NodeFiringTable {
    type Target = BTreeMap<String, NodeFirings>;
    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

/// The record at `(node, run_id, frames)`: the one firing every
/// row and every sweep about that location is about.
pub fn firing_at<'a>(
    firings: &'a NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &LoopFrames,
) -> Option<&'a NodeFiring> {
    firings.at(node_id, frames).filter(|e| e.run_id == run_id)
}

/// `firing_at`, mutably: the record a lifecycle row about the location
/// updates (its status through [`NodeFiringTable::set_status`]).
pub fn firing_at_mut<'a>(
    firings: &'a mut NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &LoopFrames,
) -> Option<&'a mut NodeFiring> {
    firings.at_mut(node_id, frames).filter(|e| e.run_id == run_id)
}

/// `firing_at_mut`, moving the record to `status` first
/// ([`NodeFiringTable::set_status`]).
pub fn set_firing_status<'a>(
    firings: &'a mut NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &LoopFrames,
    status: NodeFiringStatus,
) -> Option<&'a mut NodeFiring> {
    firing_at(firings, node_id, run_id, frames)?;
    firings.set_status(node_id, frames, status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::Frame;

    fn frames(index: u32) -> LoopFrames {
        vec![Frame::Loop { index }]
    }

    fn started(node: &str, at: LoopFrames) -> NodeFiring {
        NodeFiring::started(node, at, RunId::nil(), Default::default(), Vec::new(), 0)
    }

    fn places<'a>(records: impl Iterator<Item = &'a NodeFiring>) -> Vec<(String, LoopFrames)> {
        records.map(|record| (record.node_id.clone(), record.frames.clone())).collect()
    }

    /// A record is found by its frames among a thousand of its node's, and
    /// the node's records read in the order they started.
    #[test]
    fn a_record_is_found_by_its_frames_and_records_keep_their_start_order() {
        let mut table = NodeFiringTable::new();
        for index in (0..1000).rev() {
            table.insert(started("body", frames(index)));
        }
        table.insert(started("other", frames(7)));
        let found = firing_at(&table, "body", RunId::nil(), &frames(7)).expect("iteration 7 fired");
        assert_eq!((found.node_id.as_str(), &found.frames), ("body", &frames(7)));
        assert!(firing_at(&table, "body", RunId::nil(), &frames(1000)).is_none(), "iteration 1000 never fired");
        assert!(firing_at(&table, "body", RunId::from_u128(1), &frames(7)).is_none(), "another run's location");
        let order: Vec<u32> = table["body"].values().map(|record| record.frames[0].loop_index().unwrap()).collect();
        assert_eq!(order, (0..1000).rev().collect::<Vec<_>>());
    }

    /// The open, waiting and failed firings follow every status change,
    /// and read what a walk over every record would.
    #[test]
    fn the_open_waiting_and_failed_firings_follow_the_statuses() {
        let mut table = NodeFiringTable::new();
        for index in 0..4 {
            table.insert(started("a", frames(index)));
        }
        table.insert(started("b", vec![]));
        let walked = |table: &NodeFiringTable, keep: fn(NodeFiringStatus) -> bool| -> Vec<(String, LoopFrames)> {
            let mut all: Vec<_> = places(table.values().flat_map(|records| records.values()).filter(|record| keep(record.status())));
            all.sort();
            all
        };
        let check = |table: &NodeFiringTable| {
            assert_eq!(places(table.open()), walked(table, |status| !status.is_terminal()));
            assert_eq!(places(table.waiting()), walked(table, |status| status == NodeFiringStatus::WaitingForInput));
            assert_eq!(places(table.failed()), walked(table, |status| status == NodeFiringStatus::Failed));
            assert_eq!(table.any_open(), table.open().next().is_some());
        };
        check(&table);
        table.set_status("a", &frames(1), NodeFiringStatus::WaitingForInput).expect("a record");
        table.set_status("a", &frames(2), NodeFiringStatus::Failed);
        table.set_status("b", &vec![], NodeFiringStatus::Completed);
        check(&table);
        assert_eq!(places(table.waiting()), vec![("a".to_string(), frames(1))]);
        table.set_status("a", &frames(1), NodeFiringStatus::Running);
        table.set_status("a", &frames(0), NodeFiringStatus::Skipped);
        table.set_status("a", &frames(3), NodeFiringStatus::Cancelled);
        check(&table);
        assert_eq!(places(table.open()), vec![("a".to_string(), frames(1))]);
        assert!(table.set_status("a", &frames(9), NodeFiringStatus::Completed).is_none(), "no record there");
        table.set_status("a", &frames(1), NodeFiringStatus::Completed);
        check(&table);
        assert!(!table.any_open());
    }
}
