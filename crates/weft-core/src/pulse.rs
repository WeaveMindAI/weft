use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::lane::Lane;
use crate::Color;

/// A unit of data flowing between nodes in an execution. Pulses carry
/// their own execution identity (color) and parallel/loop
/// sub-dimension (lane). Nodes fire when all required inputs have a
/// pulse with matching (color, lane).
///
/// Pulses do NOT carry execution metadata; that lives in
/// `NodeExecution` records. This split is load-bearing: the scheduler
/// can replay pulses without the metadata machinery, the metadata can
/// grow without disturbing the hot path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pulse {
    pub id: uuid::Uuid,
    pub color: Color,
    pub lane: Lane,
    /// The destination node id.
    pub target_node: String,
    /// The port on the destination node. Always set: v1 had an
    /// Option<String> with implicit defaults; v2 requires explicit
    /// routing.
    pub target_port: String,
    pub value: Value,
    pub status: PulseStatus,
    /// Set when this pulse was synthesized by a Gather transformation;
    /// prevents re-gathering on subsequent scheduler passes.
    #[serde(default)]
    pub gathered: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PulseStatus {
    Pending,
    Absorbed,
}

impl PulseStatus {
    pub fn is_pending(&self) -> bool {
        matches!(self, PulseStatus::Pending)
    }
}

impl Pulse {
    pub fn new(
        color: Color,
        lane: Lane,
        target_node: impl Into<String>,
        target_port: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            color,
            lane,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value,
            status: PulseStatus::Pending,
            gathered: false,
        }
    }

    /// Mark this pulse as absorbed (consumed by a dispatch, expand,
    /// gather, or cancellation). Absorbed pulses are never reused.
    pub fn absorb(&mut self) {
        self.status = PulseStatus::Absorbed;
    }
}

/// All in-flight pulses, keyed by destination node. Every scheduler
/// iteration iterates this structure.
pub type PulseTable = BTreeMap<String, Vec<Pulse>>;
