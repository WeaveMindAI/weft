//! Pulse-table mutation events the engine emits as it runs
//! preprocess and postprocess. The engine pushes one entry per
//! mutation; the runtime crate translates these into worker→
//! dispatcher messages so the journal records the same set of
//! pulse-table changes the engine actually applied.
//!
//! This is the load-bearing piece that makes replay exact:
//! `NodeStarted.pulses_absorbed` UUIDs match the `pulse_id`s here,
//! so the dispatcher's fold reconstructs the engine's pulse table
//! one-for-one without inferring expand/gather behavior.

use serde_json::Value;

use crate::lane::Lane;
use crate::Color;

#[derive(Debug, Clone)]
pub enum PulseMutation {
    /// `postprocess_output` placed a pulse on a downstream edge.
    Emitted {
        pulse_id: uuid::Uuid,
        source_node: String,
        source_port: String,
        target_node: String,
        target_port: String,
        color: Color,
        lane: Lane,
        value: Value,
    },
    /// `apply_expand` absorbed `absorbed_pulse_id` and produced N
    /// child-lane pulses on the same node bucket. Each child carries
    /// the lane suffix the engine appended (1 frame for the common
    /// case; >1 when `lane_depth` peels multiple list layers in a
    /// single Expand).
    Expanded {
        node_id: String,
        port: String,
        absorbed_pulse_id: uuid::Uuid,
        color: Color,
        base_lane: Lane,
        children: Vec<ExpandedChild>,
    },
    /// `apply_gather` absorbed N sibling pulses and produced one
    /// parent-lane pulse with `gathered: true`.
    Gathered {
        node_id: String,
        port: String,
        absorbed_pulse_ids: Vec<uuid::Uuid>,
        color: Color,
        parent_lane: Lane,
        pulse_id: uuid::Uuid,
        value: Value,
    },
}

#[derive(Debug, Clone)]
pub struct ExpandedChild {
    pub pulse_id: uuid::Uuid,
    pub lane_suffix: Lane,
    pub value: Value,
}
