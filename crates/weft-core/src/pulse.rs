use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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
    pub value: Value,
    pub status: PulseStatus,
    /// Closure marker. `true` means this pulse is the engine telling
    /// the consumer "nothing will arrive here": the upstream terminated
    /// without firing this port. Required port + closure -> consumer
    /// skips (and cascades closure on its outputs). Optional port +
    /// closure -> consumer fires with the port treated as missing.
    /// `value` is always `Null` when `closed`; the field is for
    /// serialisation symmetry only.
    pub closed: bool,
    /// Only ever `Some` on a closure (`closed: true`) targeting a
    /// generator port: the closure is the stream's end, and this says
    /// the producer FAILED with this error rather than finishing
    /// cleanly. A consumer pull that takes a failed end gets the error;
    /// a clean end (`None`) reads as "stream finished". Ordinary
    /// closures never carry it: their consumers act on the closure
    /// itself (skip / missing input), never on why upstream ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PulseStatus {
    Pending,
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
    pub fn new(
        color: Color,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            color,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value,
            status: PulseStatus::Pending,
            closed: false,
            close_error: None,
        }
    }

    /// Closure marker: the upstream terminated without firing this port.
    /// Carries no data (value is always Null). The consumer treats this
    /// as "nothing will arrive here ever again at this frame stack".
    /// Required port + closure -> consumer skips. Optional port +
    /// closure -> consumer fires with the port missing.
    pub fn closure(
        color: Color,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
    ) -> Self {
        Self::closure_with_error(color, frames, target_node, target_port, None)
    }

    /// Closure carrying WHY the upstream ended, for generator ports:
    /// `Some(error)` marks a failed stream end (the consumer's pull
    /// gets the error), `None` a clean finish.
    pub fn closure_with_error(
        color: Color,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        close_error: Option<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            color,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value: Value::Null,
            status: PulseStatus::Pending,
            closed: true,
            close_error,
        }
    }

    /// Mark this pulse as absorbed. Absorbed pulses are never reused.
    pub fn absorb(&mut self) {
        self.status = PulseStatus::Absorbed;
    }

    /// Reconstruct a pulse from a journaled `PulseEmitted` event. The
    /// caller passes the event's full shape; this constructor enforces
    /// the closure invariants (closed implies value: Null; an error
    /// only rides a closure) so a broken journal row is caught at the
    /// reconstruction boundary rather than silently propagated into
    /// the live pulse table. Returns `Err` instead of panicking so a
    /// corrupt row in one execution's journal doesn't poison the
    /// dispatcher's cancel path or any other fold-driven HTTP handler.
    #[allow(clippy::too_many_arguments)]
    pub fn from_journal_emit(
        id: uuid::Uuid,
        color: Color,
        frames: LoopFrames,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        value: Value,
        closed: bool,
        close_error: Option<String>,
    ) -> Result<Self, &'static str> {
        if closed && !value.is_null() {
            return Err(
                "closure pulse must have value: Null (journal row violates the closed-implies-null invariant)"
            );
        }
        if !closed && close_error.is_some() {
            return Err(
                "a close_error can only ride a closure pulse (journal row violates the invariant)"
            );
        }
        Ok(Self {
            id,
            color,
            frames,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value,
            status: PulseStatus::Pending,
            closed,
            close_error,
        })
    }
}

/// All in-flight pulses, keyed by destination node. Every scheduler
/// iteration iterates this structure.
pub type PulseTable = BTreeMap<String, Vec<Pulse>>;
