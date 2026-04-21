use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lane::Lane;
use crate::Color;

/// A data carrier between nodes. Carries its own execution identity
/// (color) and parallel/loop sub-dimension (lane). Pulses flow along
/// graph edges; nodes fire when all required inputs have a pulse with
/// matching color and lane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pulse {
    pub id: uuid::Uuid,
    pub color: Color,
    pub lane: Lane,
    pub target_node: String,
    pub target_port: String,
    pub value: Value,
    pub status: PulseStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PulseStatus {
    Pending,
    Absorbed,
}

impl Pulse {
    pub fn new(color: Color, lane: Lane, target_node: impl Into<String>, target_port: impl Into<String>, value: Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            color,
            lane,
            target_node: target_node.into(),
            target_port: target_port.into(),
            value,
            status: PulseStatus::Pending,
        }
    }
}
