use std::collections::{BTreeMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::LoopFrames;
use crate::project::ProjectDefinition;
use crate::ExecutionId;

/// A unit of data flowing between nodes in an execution. Pulses carry
/// their own execution identity (execution) and a frame stack (`frames`)
/// identifying which iteration of which (nested) loop the pulse belongs
/// to. Nodes fire when all required inputs have a pulse with matching
/// `(execution_id, frames)` at the exact same frame stack.
///
/// Pulses do NOT carry execution metadata; that lives in
/// `NodeExecution` records. This split is load-bearing: the scheduler
/// can replay pulses without the metadata machinery, the metadata can
/// grow without disturbing the hot path.
///
/// The value is SHARED, never copied: one emission that fans out to
/// fifty wires is fifty pulses pointing at one `Arc<Value>`. The only
/// owned copy a value ever gets is the input bag handed to the node
/// body that consumes it.
///
/// A pulse's `id` is DERIVED from the emission that placed it and the
/// wire it landed on (`exec::emission::pulse_id`), never minted at
/// random: the journal records the emission once and the fold puts
/// the same pulse, with the same id, on the same wire, so every row
/// that names a pulse by id (a stream take, a loop's item launch)
/// resolves identically live and on replay.
///
/// A pulse with `closed: true` is a CLOSURE marker, not data. It tells
/// the consumer "nothing will ever arrive on this port at this frame
/// stack". An explicit `Value::Null` with `closed: false` is a
/// user-sent null and runs the consumer normally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pulse {
    pub id: uuid::Uuid,
    pub execution_id: ExecutionId,
    pub frames: LoopFrames,
    /// The destination node id.
    pub target_node: String,
    /// The port on the destination node.
    pub target_port: String,
    pub value: Arc<Value>,
    pub status: PulseStatus,
    /// Closure marker. `true` means this pulse is the engine telling
    /// the consumer "nothing will arrive here": the upstream terminated
    /// without firing this port. Required port + closure -> consumer
    /// skips (and cascades closure on its outputs). Optional port +
    /// closure -> consumer fires with the port treated as missing.
    /// `value` is always `Null` when `closed`; the field is for
    /// serialisation symmetry only.
    pub closed: bool,
    /// The producer failed or its output was refused: which node broke
    /// and why, carried unchanged however many skips sit between that
    /// node and this wire. Stream consumers receive it at the end;
    /// scalar consumers retain ordinary closure behavior. Neither may
    /// replace it with a backup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<Failure>,
    /// A supplied output or a used input backup, including its stream end.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub provided: bool,
    /// Derived from this execution's input backup after the real supplier
    /// ended without data. Kept after absorption so replay cannot select
    /// the same backup twice.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub backup: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<ExecutionId>,
}

/// A node that broke, as the closures it leaves behind carry it: the
/// node, spelled the way the program reads it (through its call sites),
/// and its error. Set once, where the node fails, and passed on
/// unchanged by every skip it causes, so a message three nodes further
/// down still names the node that actually failed.
// SYNC: Failure <-> packages/weft-graph/src/protocol.ts Failure
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Failure {
    pub node: String,
    pub error: String,
}

impl Failure {
    /// The failure of the node `node_id` firing at `frames`, spelled
    /// through the call sites on those frames.
    pub fn at(project: &ProjectDefinition, node_id: &str, frames: &LoopFrames, error: impl Into<String>) -> Self {
        let call_path: Vec<String> = crate::frames::call_path(frames).into_iter().map(str::to_string).collect();
        Self { node: crate::project::address_of(project, node_id, &call_path), error: error.into() }
    }
}

// SYNC: Display for Failure <-> packages/weft-graph/src/webview/lib/utils/status.ts failureText
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}' failed: {}", self.node, self.error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PulseStatus {
    Pending,
    /// A supplied emission waiting for its source's enclosing group gate.
    Gated,
    /// Handed to a LIVE in-process sink (a running consumer's generator
    /// feed, a loop's stream queue) but not yet taken. Invisible to
    /// readiness (the consumer is already running; a re-dispatch would
    /// double-run it) yet still in flight for completion accounting.
    /// In-RAM only: the journal fold never produces it (a crash before
    /// the take refolds the pulse as Pending; the consumer that was
    /// taking it went down with the worker and is failed, never run
    /// again).
    Routed,
    /// Consumed by a dispatch, a take, or cancellation and never read
    /// again.
    Absorbed,
}

impl PulseStatus {
    pub fn is_pending(&self) -> bool {
        matches!(self, PulseStatus::Pending)
    }

    /// Still in flight: not yet absorbed. Pending AND routed pulses are
    /// work the execution has not finished (a routed item sits in a
    /// live sink awaiting its take).
    pub fn in_flight(&self) -> bool {
        !matches!(self, PulseStatus::Absorbed)
    }
}

impl Pulse {
    /// A data pulse. `id` comes from `exec::emission::pulse_id` (the
    /// emission plus the wire); the value is shared with every other
    /// wire the same emission reached.
    pub fn new(
        id: uuid::Uuid,
        execution_id: ExecutionId,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        value: Arc<Value>,
    ) -> Self {
        Self {
            id,
            execution_id,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value,
            status: PulseStatus::Pending,
            closed: false,
            failure: None,
            provided: false,
            backup: false,
            inherited_from: None,
        }
    }

    /// Closure marker: the upstream terminated without firing this port.
    /// Carries no data (value is always Null). The consumer treats this
    /// as "nothing will arrive here ever again at this frame stack".
    /// Required port + closure -> consumer skips. Optional port +
    /// closure -> consumer fires with the port missing.
    pub fn closure(
        id: uuid::Uuid,
        execution_id: ExecutionId,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
    ) -> Self {
        Self::closure_with_failure(id, execution_id, frames, target_node, target_port, None)
    }

    /// Closure carrying WHY the upstream ended: `Some(failure)` marks a
    /// producer that broke (a generator consumer's pull gets the error),
    /// `None` a clean finish or a decline.
    pub fn closure_with_failure(
        id: uuid::Uuid,
        execution_id: ExecutionId,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        failure: Option<Failure>,
    ) -> Self {
        Self {
            id,
            execution_id,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value: Arc::new(Value::Null),
            status: PulseStatus::Pending,
            closed: true,
            failure,
            provided: false,
            backup: false,
            inherited_from: None,
        }
    }

    /// Mark this pulse as absorbed. Absorbed pulses are never reused.
    pub fn absorb(&mut self) {
        self.status = PulseStatus::Absorbed;
    }
}

/// Pending values plus bounded per-stream consumption history. Removing an
/// item frees its payload without forgetting that this stream supplied data.
#[derive(Debug, Clone, Default)]
pub struct PulseTable {
    buckets: BTreeMap<String, Vec<Pulse>>,
    consumed_streams: HashSet<(ExecutionId, crate::frames::FiringLocation, String)>,
}

impl PulseTable {
    pub fn new() -> Self { Self::default() }

    pub fn stream_was_consumed(&self, execution_id: ExecutionId, node: &str, port: &str, frames: &LoopFrames) -> bool {
        self.consumed_streams.contains(&(execution_id, crate::frames::FiringLocation::new(node, frames.clone()), port.into()))
    }

    /// Call only after validating that every id belongs to this bucket.
    /// The live router and journal fold share this consumption operation.
    pub fn remove_consumed(&mut self, node: &str, ids: &[uuid::Uuid]) {
        let bucket = self.buckets.get_mut(node).expect("consumed pulse bucket exists");
        for pulse in bucket.iter().filter(|p| ids.contains(&p.id)) {
            self.consumed_streams.insert((pulse.execution_id,
                crate::frames::FiringLocation::new(node, pulse.frames.clone()), pulse.target_port.clone()));
        }
        bucket.retain(|p| !ids.contains(&p.id));
    }
}

impl Deref for PulseTable {
    type Target = BTreeMap<String, Vec<Pulse>>;
    fn deref(&self) -> &Self::Target { &self.buckets }
}

impl DerefMut for PulseTable {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.buckets }
}

impl<const N: usize> From<[(String, Vec<Pulse>); N]> for PulseTable {
    fn from(values: [(String, Vec<Pulse>); N]) -> Self {
        Self { buckets: BTreeMap::from(values), consumed_streams: HashSet::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A closure crosses processes (the engine's in-RAM table, a seeded
    /// run's history, the inspector) as JSON: the failure travels as the
    /// node that broke plus its error, and comes back the same.
    #[test]
    fn a_failed_closure_round_trips_with_the_node_that_broke() {
        let failure = Failure { node: "auth.query".into(), error: "the database is down".into() };
        let pulse = Pulse::closure_with_failure(uuid::Uuid::nil(), uuid::Uuid::nil(), vec![], "sink", "in", Some(failure.clone()));
        let wire = serde_json::to_value(&pulse).unwrap();
        assert_eq!(wire["failure"], json!({ "node": "auth.query", "error": "the database is down" }));
        let back: Pulse = serde_json::from_value(wire).unwrap();
        assert_eq!(back.failure, Some(failure.clone()));
        assert_eq!(failure.to_string(), "'auth.query' failed: the database is down");

        let plain = serde_json::to_value(Pulse::closure(uuid::Uuid::nil(), uuid::Uuid::nil(), vec![], "sink", "in")).unwrap();
        assert!(plain.get("failure").is_none(), "a plain closure carries no failure field");
    }
}
