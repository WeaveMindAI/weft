use std::collections::{BTreeMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::LoopFrames;
use crate::Color;

/// A unit of data flowing between nodes in an execution. Pulses carry
/// their own execution identity (color) and a frame stack (`frames`)
/// identifying which iteration of which (nested) loop the pulse belongs
/// to. Nodes fire when all required inputs have a pulse with matching
/// `(color, frames)` at the exact same frame stack.
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
    pub color: Color,
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
    /// The producer failed or its output was refused. Stream consumers
    /// receive this error at the end; scalar consumers retain ordinary
    /// closure behavior. Neither may replace this error with a backup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_error: Option<String>,
    /// A supplied output or a used input backup, including its stream end.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub provided: bool,
    /// Derived from this execution's input backup after the real supplier
    /// ended without data. Kept after absorption so replay cannot select
    /// the same backup twice.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub backup: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<Color>,
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
    /// the take refolds the pulse as Pending and re-delivers, the
    /// documented at-least-once crash semantics).
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
        color: Color,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        value: Arc<Value>,
    ) -> Self {
        Self {
            id,
            color,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value,
            status: PulseStatus::Pending,
            closed: false,
            close_error: None,
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
        color: Color,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
    ) -> Self {
        Self::closure_with_error(id, color, frames, target_node, target_port, None)
    }

    /// Closure carrying WHY the upstream ended, for generator ports:
    /// `Some(error)` marks a failed stream end (the consumer's pull
    /// gets the error), `None` a clean finish.
    pub fn closure_with_error(
        id: uuid::Uuid,
        color: Color,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        close_error: Option<String>,
    ) -> Self {
        Self {
            id,
            color,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value: Arc::new(Value::Null),
            status: PulseStatus::Pending,
            closed: true,
            close_error,
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
    consumed_streams: HashSet<(Color, crate::frames::FiringLocation, String)>,
}

impl PulseTable {
    pub fn new() -> Self { Self::default() }

    pub fn stream_was_consumed(&self, color: Color, node: &str, port: &str, frames: &LoopFrames) -> bool {
        self.consumed_streams.contains(&(color, crate::frames::FiringLocation::new(node, frames.clone()), port.into()))
    }

    /// Call only after validating that every id belongs to this bucket.
    /// The live router and journal fold share this consumption operation.
    pub fn remove_consumed(&mut self, node: &str, ids: &[uuid::Uuid]) {
        let bucket = self.buckets.get_mut(node).expect("consumed pulse bucket exists");
        for pulse in bucket.iter().filter(|p| ids.contains(&p.id)) {
            self.consumed_streams.insert((pulse.color,
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
