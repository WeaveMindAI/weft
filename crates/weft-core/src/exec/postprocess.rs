//! Output postprocessing. After a node returns from `execute`, the
//! runtime calls this to emit pulses on each outgoing edge, applying
//! Expand/Gather lane transformations and runtime type checks.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::exec::execution::{NodeExecutionStatus, NodeExecutionTable};
use crate::exec::mutations::PulseMutation;
use crate::exec::typecheck::runtime_type_check;
use crate::lane::{Lane, LaneFrame};
use crate::project::{Edge, EdgeIndex, LaneMode, NodeDefinition, ProjectDefinition};
use crate::pulse::{Pulse, PulseTable};
use crate::weft_type::WeftType;
use crate::Color;

/// Outcome of `postprocess_output`. `gather_fired` lets the caller
/// re-run readiness; mutations land in the caller-provided vec so
/// the runtime can ship them to the dispatcher for journaling.
#[derive(Debug, Default)]
pub struct PostprocessResult {
    pub gather_fired: bool,
}

/// Process a node's output. Emits pulses downstream and appends one
/// `PulseMutation::Emitted` per produced pulse to `mutations`, so
/// the runtime can ship a faithful journal record.
pub fn postprocess_output(
    node_id: &str,
    output: &Value,
    color: Color,
    lane: &Lane,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    node_executions: &mut NodeExecutionTable,
    mutations: &mut Vec<PulseMutation>,
) -> PostprocessResult {
    let mut result = PostprocessResult::default();
    let Some(node) = project.nodes.iter().find(|n| n.id == node_id) else {
        tracing::error!(target: "weft::exec::postprocess", node = node_id, "node not found in project");
        return result;
    };

    let output_obj = output.as_object();
    let outgoing = edge_idx.get_outgoing(project, node_id);

    // Per-port runtime type check on Single ports. Mismatches coerce
    // to null and mark the execution failed.
    let mut failed_ports: HashSet<String> = HashSet::new();
    for port in &node.outputs {
        if port.lane_mode != LaneMode::Single {
            continue;
        }
        let connected = outgoing.iter().any(|e| e.source_handle.as_deref() == Some(&port.name));
        if !connected {
            continue;
        }
        let value = output_obj
            .and_then(|o| o.get(&port.name))
            .cloned()
            .unwrap_or(Value::Null);
        if value.is_null() || port.port_type.is_unresolved() {
            continue;
        }
        if !runtime_type_check(&port.port_type, &value) {
            let err = format!(
                "output '{}' expected {}, got {}",
                port.name,
                port.port_type,
                WeftType::infer(&value)
            );
            tracing::error!(target: "weft::exec::postprocess", node = node_id, "{err}");
            if let Some(execs) = node_executions.get_mut(node_id) {
                if let Some(exec) = execs.iter_mut().rev().find(|e| e.color == color && &e.lane == lane) {
                    exec.status = NodeExecutionStatus::Failed;
                    exec.error = Some(err);
                }
            }
            failed_ports.insert(port.name.clone());
        }
    }

    // Collect gather-port values to process together at the end.
    let mut gather_values: HashMap<String, Value> = HashMap::new();

    for port in &node.outputs {
        let value = if failed_ports.contains(&port.name) {
            Value::Null
        } else {
            output_obj
                .and_then(|o| o.get(&port.name))
                .cloned()
                .unwrap_or(Value::Null)
        };
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
            LaneMode::Gather => {
                gather_values.insert(port.name.clone(), value);
            }
        }
    }

    if !gather_values.is_empty() {
        if lane.is_empty() {
            for (port_name, value) in &gather_values {
                emit_single(
                    node_id,
                    &outgoing,
                    port_name,
                    value,
                    color,
                    lane,
                    pulses,
                    mutations,
                );
            }
        } else {
            result.gather_fired = try_gather(
                node_id,
                node,
                color,
                lane,
                &gather_values,
                &outgoing,
                pulses,
                node_executions,
                mutations,
            );
        }
    }

    result
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

            let checked = if item.is_null() || port.port_type.is_unresolved() {
                item.clone()
            } else if !runtime_type_check(&port.port_type, item) {
                tracing::error!(
                    target: "weft::exec::postprocess",
                    node = node_id, port = %port.name, lane = ?child_lane,
                    "expand item type mismatch; coercing to null"
                );
                Value::Null
            } else {
                item.clone()
            };

            let pulse = Pulse::new(
                color,
                child_lane.clone(),
                edge.target.clone(),
                target_handle.to_string(),
                checked.clone(),
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
                value: checked,
            });
        }
    }
}

/// Gather port: when all sibling lanes at the top level have
/// completed, collect their output port values into a list and emit
/// at the parent lane.
fn try_gather(
    node_id: &str,
    node: &NodeDefinition,
    color: Color,
    lane: &Lane,
    gather_values: &HashMap<String, Value>,
    outgoing: &[&Edge],
    pulses: &mut PulseTable,
    node_executions: &NodeExecutionTable,
    mutations: &mut Vec<PulseMutation>,
) -> bool {
    let top = lane.last().unwrap();
    let expected = top.count;
    let parent_lane: Lane = lane[..lane.len() - 1].to_vec();

    // Check siblings: all sibling executions for this node at
    // matching parent-lane + count have reached a terminal state.
    let siblings: Vec<_> = node_executions
        .get(node_id)
        .map(|execs| {
            execs
                .iter()
                .filter(|e| {
                    e.color == color
                        && e.lane.len() == lane.len()
                        && e.lane[..e.lane.len() - 1] == parent_lane[..]
                        && e.lane.last().map(|f| f.count) == Some(expected)
                        && e.status.is_terminal()
                })
                .collect()
        })
        .unwrap_or_default();

    if (siblings.len() as u32) < expected {
        return false;
    }

    for (port_name, _) in gather_values {
        let mut ordered: Vec<(u32, Value)> = siblings
            .iter()
            .map(|e| {
                let idx = e.lane.last().unwrap().index;
                let value = if matches!(e.status, NodeExecutionStatus::Failed | NodeExecutionStatus::Skipped) {
                    Value::Null
                } else {
                    e.output
                        .as_ref()
                        .and_then(|o| o.get(port_name))
                        .cloned()
                        .unwrap_or(Value::Null)
                };
                (idx, value)
            })
            .collect();
        ordered.sort_by_key(|(i, _)| *i);
        let gathered = Value::Array(ordered.into_iter().map(|(_, v)| v).collect());

        if let Some(port_def) = node.outputs.iter().find(|p| &p.name == port_name) {
            if !port_def.port_type.is_unresolved() && !runtime_type_check(&port_def.port_type, &gathered) {
                tracing::error!(
                    target: "weft::exec::postprocess",
                    node = node_id, port = port_name.as_str(),
                    "gather type mismatch"
                );
            }
        }

        emit_single(node_id, outgoing, port_name, &gathered, color, &parent_lane, pulses, mutations);
    }

    true
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
    node_executions: &mut NodeExecutionTable,
    mutations: &mut Vec<PulseMutation>,
) -> PostprocessResult {
    let Some(node) = project.nodes.iter().find(|n| n.id == node_id) else {
        return PostprocessResult::default();
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
        node_executions,
        mutations,
    )
}
