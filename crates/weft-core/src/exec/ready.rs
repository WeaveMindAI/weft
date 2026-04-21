//! Readiness. Find which nodes have enough pending pulses to fire at
//! a matching `(color, lane)`, aggregate their inputs, return as
//! `ReadyGroup`s.
//!
//! Expects `preprocess_input` has already run (Expand/Gather
//! transformations applied), so matching is a simple `color+lane`
//! join.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::exec::skip::check_should_skip;
use crate::exec::typecheck::runtime_type_check;
use crate::lane::{Lane, LaneFrame};
use crate::project::{EdgeIndex, GroupBoundaryRole, LaneMode, NodeDefinition, ProjectDefinition};
use crate::pulse::{Pulse, PulseTable};
use crate::weft_type::WeftType;
use crate::Color;

/// One dispatch ready to fire. `input` is the aggregated inputs
/// object; `pulse_ids` are the pulses that will be absorbed when the
/// caller commits the dispatch.
pub struct ReadyGroup {
    pub lane: Lane,
    pub color: Color,
    pub input: Value,
    pub should_skip: bool,
    pub pulse_ids: Vec<uuid::Uuid>,
    pub error: Option<String>,
}

pub fn find_ready_nodes<'a>(
    project: &'a ProjectDefinition,
    pulses: &PulseTable,
    edge_idx: &EdgeIndex,
) -> Vec<(String, ReadyGroup)> {
    let mut result = Vec::new();

    for node in &project.nodes {
        let Some(node_pulses) = pulses.get(&node.id) else { continue };
        if !node_pulses.iter().any(|p| p.status.is_pending()) {
            continue;
        }

        let incoming = edge_idx.get_incoming(project, &node.id);
        let wired: HashSet<&str> = incoming
            .iter()
            .map(|e| e.target_handle.as_deref().unwrap_or("default"))
            .collect();
        let has_incoming = !wired.is_empty();

        let required: HashSet<&str> = node
            .inputs
            .iter()
            .filter(|p| p.required)
            .map(|p| p.name.as_str())
            .collect();

        let mut config_filled: HashSet<&str> = HashSet::new();
        for port in &node.inputs {
            if !port.configurable || wired.contains(port.name.as_str()) {
                continue;
            }
            if node.config.get(&port.name).map(|v| !v.is_null()).unwrap_or(false) {
                config_filled.insert(port.name.as_str());
            }
        }

        for group in find_groups_for_node(node, node_pulses, &required, &wired, &config_filled, has_incoming) {
            result.push((node.id.clone(), group));
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Per-node matching
// ---------------------------------------------------------------------------

fn find_groups_for_node(
    node: &NodeDefinition,
    node_pulses: &[Pulse],
    required: &HashSet<&str>,
    wired: &HashSet<&str>,
    config_filled: &HashSet<&str>,
    has_incoming: bool,
) -> Vec<ReadyGroup> {
    let gather_ports: HashSet<&str> = node
        .inputs
        .iter()
        .filter(|p| p.lane_mode == LaneMode::Gather)
        .map(|p| p.name.as_str())
        .collect();

    let pending: Vec<&Pulse> = node_pulses
        .iter()
        .filter(|p| p.status.is_pending())
        .filter(|p| !gather_ports.contains(p.target_port.as_str()) || p.gathered)
        .collect();

    // Group pulses by (color, lane).
    let mut groups: HashMap<(Color, Lane), Vec<&Pulse>> = HashMap::new();
    for p in &pending {
        groups.entry((p.color, p.lane.clone())).or_default().push(p);
    }

    // Suppress broadcast lanes when deeper lanes exist for the same
    // color: deeper lanes mean we're in an expanded context, and the
    // shallower one would be double-counted.
    let keys: Vec<(Color, Lane)> = groups.keys().cloned().collect();
    let suppressed: HashSet<(Color, Lane)> = keys
        .iter()
        .filter(|(color_a, lane_a)| {
            keys.iter().any(|(color_b, lane_b)| {
                color_a == color_b && lane_a.len() < lane_b.len() && lane_b.starts_with(lane_a)
            })
        })
        .cloned()
        .collect();

    let mut ready = Vec::new();

    for ((color, lane), group_pulses) in &groups {
        if suppressed.contains(&(*color, lane.clone())) {
            continue;
        }

        let all_satisfied = wired.iter().all(|port_name| {
            if group_pulses.iter().any(|p| p.target_port == *port_name) {
                return true;
            }
            // Broadcast: a shallower-lane pulse counts.
            if !lane.is_empty() {
                return node_pulses.iter().any(|p| {
                    p.status.is_pending()
                        && p.color == *color
                        && p.lane.len() < lane.len()
                        && lane.starts_with(&p.lane)
                        && p.target_port == *port_name
                });
            }
            false
        });

        if has_incoming && !all_satisfied {
            continue;
        }

        let mut type_errors = Vec::new();
        let input = build_input(node, node_pulses, lane, color, &mut type_errors);

        // Group boundary skip rules: only In-boundary skips; Out
        // forwards whatever came through.
        let is_out_boundary = node
            .group_boundary
            .as_ref()
            .map(|gb| gb.role == GroupBoundaryRole::Out)
            .unwrap_or(false);
        let should_skip = if is_out_boundary || !has_incoming {
            false
        } else {
            check_should_skip(node, node_pulses, lane, *color, required, wired, config_filled)
        };

        let pulse_ids: Vec<uuid::Uuid> = group_pulses
            .iter()
            .filter(|p| &p.lane == lane)
            .map(|p| p.id)
            .collect();

        ready.push(ReadyGroup {
            lane: lane.clone(),
            color: *color,
            input,
            should_skip,
            pulse_ids,
            error: if type_errors.is_empty() { None } else { Some(type_errors.join("; ")) },
        });
    }

    // Shape mismatch detection: if we produced no ready groups and
    // there are multiple wired ports with incompatible lane shapes,
    // emit an error-ready group so the dispatcher can surface it to
    // the user.
    if ready.is_empty() && wired.len() > 1 {
        if let Some(mismatch) = detect_shape_mismatch(&pending, wired, &node.id) {
            ready.push(mismatch);
        }
    }

    ready
}

// ---------------------------------------------------------------------------
// Input aggregation
// ---------------------------------------------------------------------------

fn build_input(
    node: &NodeDefinition,
    node_pulses: &[Pulse],
    lane: &Lane,
    color: &Color,
    type_errors: &mut Vec<String>,
) -> Value {
    let mut obj = Map::new();

    // Collect pulses for this (color, lane) plus broadcast parents.
    for p in node_pulses.iter().filter(|p| p.status.is_pending() && &p.color == color) {
        if &p.lane == lane {
            obj.insert(p.target_port.clone(), p.value.clone());
        } else if p.lane.len() < lane.len() && lane.starts_with(&p.lane) && !obj.contains_key(&p.target_port) {
            obj.insert(p.target_port.clone(), p.value.clone());
        }
    }

    // Config fills any unsatisfied configurable port.
    for port in &node.inputs {
        if !port.configurable || obj.contains_key(&port.name) {
            continue;
        }
        if let Some(cfg) = node.config.get(&port.name) {
            if !cfg.is_null() {
                obj.insert(port.name.clone(), cfg.clone());
            }
        }
    }

    // Runtime type enforcement on Single-mode ports.
    for port in &node.inputs {
        if port.lane_mode != LaneMode::Single || port.port_type.is_unresolved() {
            continue;
        }
        if let Some(value) = obj.get(&port.name) {
            if !value.is_null() && !runtime_type_check(&port.port_type, value) {
                let err = format!(
                    "type mismatch on '{}': expected {}, got {}",
                    port.name,
                    port.port_type,
                    WeftType::infer(value)
                );
                tracing::error!(target: "weft::exec::ready", node = %node.id, "{err}");
                type_errors.push(err);
                obj.insert(port.name.clone(), Value::Null);
            }
        }
    }

    Value::Object(obj)
}

// ---------------------------------------------------------------------------
// Shape mismatch helper
// ---------------------------------------------------------------------------

fn detect_shape_mismatch(
    pending: &[&Pulse],
    wired: &HashSet<&str>,
    node_id: &str,
) -> Option<ReadyGroup> {
    let mut port_lanes: HashMap<&str, Vec<&Lane>> = HashMap::new();
    for p in pending {
        let port = p.target_port.as_str();
        if wired.contains(port) {
            port_lanes.entry(port).or_default().push(&p.lane);
        }
    }
    if port_lanes.len() < 2 {
        return None;
    }

    let entries: Vec<(&str, &Vec<&Lane>)> = port_lanes.iter().map(|(k, v)| (*k, v)).collect();
    let mut mismatch = false;
    'outer: for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            let compatible = entries[i].1.iter().any(|la| entries[j].1.iter().any(|lb| lanes_compatible(la, lb)));
            if !compatible {
                mismatch = true;
                break 'outer;
            }
        }
    }
    if !mismatch {
        return None;
    }

    let detail: Vec<String> = port_lanes
        .iter()
        .map(|(port, lanes)| {
            let shapes: HashSet<String> = lanes.iter().map(|l| format_lane(l)).collect();
            format!("{port}: {}", shapes.into_iter().collect::<Vec<_>>().join(" | "))
        })
        .collect();

    let msg = format!("shape mismatch: {}", detail.join("; "));
    tracing::error!(target: "weft::exec::ready", node = %node_id, "{msg}");

    Some(ReadyGroup {
        lane: Vec::new(),
        color: pending.first().map(|p| p.color).unwrap_or_else(uuid::Uuid::nil),
        input: Value::Null,
        should_skip: false,
        pulse_ids: pending.iter().map(|p| p.id).collect(),
        error: Some(msg),
    })
}

fn lanes_compatible(a: &Lane, b: &Lane) -> bool {
    let min = a.len().min(b.len());
    a.iter().take(min).zip(b.iter()).all(|(x, y)| x.count == y.count)
}

fn format_lane(lane: &Lane) -> String {
    if lane.is_empty() {
        return "scalar".into();
    }
    let parts: Vec<String> = lane.iter().map(|f: &LaneFrame| format!("{}:{}", f.count, f.index)).collect();
    format!("[{}]", parts.join(", "))
}
