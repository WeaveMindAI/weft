//! Output postprocessing. After a node returns from `execute`, the
//! runtime calls this to emit pulses on each outgoing edge, applying
//! the Expand lane split. No type checking here: type enforcement is
//! the consumer's input boundary's job (`ready::check_input`). (Gather
//! is an input-side transform; see `preprocess` + `ready::build_input`.)

use serde_json::{Map, Value};

use crate::exec::mutations::PulseMutation;
use crate::lane::{Lane, LaneFrame};
use crate::project::{Edge, EdgeIndex, LaneMode, ProjectDefinition};
use crate::pulse::{Pulse, PulseTable};
use crate::Color;


/// Process a node's output: emit pulses on each outgoing edge,
/// applying the Expand lane split, and append one
/// `PulseMutation::Emitted` per produced pulse so the runtime can ship
/// a faithful journal record.
///
/// No type checking here. Type enforcement lives at exactly one
/// boundary, the CONSUMER's input (`ready::check_input`): a node emits
/// its output as produced, and a bad-typed value is caught when it
/// reaches whatever node consumes it.
pub fn postprocess_output(
    node_id: &str,
    output: &Value,
    color: Color,
    lane: &Lane,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    mutations: &mut Vec<PulseMutation>,
) {
    let Some(node) = project.nodes.iter().find(|n| n.id == node_id) else {
        tracing::error!(target: "weft::exec::postprocess", node = node_id, "node not found in project");
        return;
    };

    let output_obj = output.as_object();
    let outgoing = edge_idx.get_outgoing(project, node_id);

    for port in &node.outputs {
        let value = output_obj
            .and_then(|o| o.get(&port.name))
            .cloned()
            .unwrap_or(Value::Null);
        match port.lane_mode {
            LaneMode::Single => emit_single(
                node_id,
                &outgoing,
                &port.name,
                &value,
                color,
                lane,
                pulses,
                mutations,
            ),
            LaneMode::Expand => emit_expand(
                node_id,
                &outgoing,
                port,
                &value,
                color,
                lane,
                pulses,
                mutations,
            ),
            // Gather is an INPUT-port lane mode only (the language never
            // assigns it to an output port). The collection happens on
            // the receiving node's input side in `preprocess::apply_gather`,
            // not here, so there is nothing to do at emit time.
            LaneMode::Gather => {}
        }
    }
}

/// Emit a Single output port value on every outgoing edge from that
/// port.
fn emit_single(
    source_node: &str,
    outgoing: &[&Edge],
    port: &str,
    value: &Value,
    color: Color,
    lane: &Lane,
    pulses: &mut PulseTable,
    mutations: &mut Vec<PulseMutation>,
) {
    for edge in outgoing {
        if edge.source_handle.as_deref() != Some(port) {
            continue;
        }
        let target_handle = edge.target_handle.as_deref().unwrap_or("default");

        let already_pending = pulses.get(&edge.target).map(|ps| {
            ps.iter().any(|p| {
                p.status.is_pending()
                    && p.color == color
                    && &p.lane == lane
                    && p.target_port == target_handle
            })
        }).unwrap_or(false);
        if already_pending {
            continue;
        }

        let pulse = Pulse::new(
            color,
            lane.clone(),
            edge.target.clone(),
            target_handle.to_string(),
            value.clone(),
        );
        let pulse_id = pulse.id;
        pulses.entry(edge.target.clone()).or_default().push(pulse);
        mutations.push(PulseMutation::Emitted {
            pulse_id,
            source_node: source_node.to_string(),
            source_port: port.to_string(),
            target_node: edge.target.clone(),
            target_port: target_handle.to_string(),
            color,
            lane: lane.clone(),
            value: value.clone(),
        });
    }
}

/// Emit an Expand port: split the array value into N child-lane
/// pulses on each downstream edge. Each downstream pulse becomes
/// its own `PulseMutation::Emitted` so replay's NodeStarted
/// matching is exact by UUID.
fn emit_expand(
    node_id: &str,
    outgoing: &[&Edge],
    port: &crate::project::PortDefinition,
    value: &Value,
    color: Color,
    lane: &Lane,
    pulses: &mut PulseTable,
    mutations: &mut Vec<PulseMutation>,
) {
    let items: Vec<Value> = if value.is_null() {
        vec![Value::Null]
    } else {
        match value.as_array() {
            Some(arr) => arr.clone(),
            None => {
                tracing::error!(
                    target: "weft::exec::postprocess",
                    node = node_id, port = %port.name,
                    "expand output not an array"
                );
                Vec::new()
            }
        }
    };

    let n = items.len() as u32;

    for edge in outgoing.iter().filter(|e| e.source_handle.as_deref() == Some(&port.name)) {
        let target_handle = edge.target_handle.as_deref().unwrap_or("default");

        for (i, item) in items.iter().enumerate() {
            let mut child_lane = lane.clone();
            child_lane.push(LaneFrame { count: n, index: i as u32 });

            // Pure split: emit each element untouched. Type enforcement
            // is the consumer's input boundary's job (`ready::check_input`),
            // not the producer's.
            let pulse = Pulse::new(
                color,
                child_lane.clone(),
                edge.target.clone(),
                target_handle.to_string(),
                item.clone(),
            );
            let pulse_id = pulse.id;
            pulses.entry(edge.target.clone()).or_default().push(pulse);
            mutations.push(PulseMutation::Emitted {
                pulse_id,
                source_node: node_id.to_string(),
                source_port: port.name.clone(),
                target_node: edge.target.clone(),
                target_port: target_handle.to_string(),
                color,
                lane: child_lane,
                value: item.clone(),
            });
        }
    }
}

/// Emit null on every output port at the given lane. Used when a
/// node is skipped or fails at dispatch time. Mutations are
/// appended to the caller's vec so the worker can ship them.
pub fn emit_null_downstream(
    node_id: &str,
    color: Color,
    lane: &Lane,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    mutations: &mut Vec<PulseMutation>,
) {
    let Some(node) = project.nodes.iter().find(|n| n.id == node_id) else {
        return;
    };
    let mut null_output = Map::new();
    for port in &node.outputs {
        null_output.insert(port.name.clone(), Value::Null);
    }
    postprocess_output(
        node_id,
        &Value::Object(null_output),
        color,
        lane,
        project,
        pulses,
        edge_idx,
        mutations,
    );
}

