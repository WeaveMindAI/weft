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
        let Some(node_pulses) = pulses.get(&node.id) else {
            tracing::trace!(target: "weft::exec::ready", node = %node.id, "skip: no pulse bucket");
            continue;
        };
        let pending_count = node_pulses.iter().filter(|p| p.status.is_pending()).count();
        if pending_count == 0 {
            tracing::trace!(target: "weft::exec::ready", node = %node.id, "skip: no pending pulses");
            continue;
        }
        tracing::info!(
            target: "weft::exec::ready",
            node = %node.id,
            pending_count,
            "considering node"
        );

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

        let groups = find_groups_for_node(node, node_pulses, &required, &wired, &config_filled, has_incoming);
        tracing::info!(
            target: "weft::exec::ready",
            node = %node.id,
            groups_returned = groups.len(),
            "find_groups_for_node done"
        );
        for group in groups {
            result.push((node.id.clone(), group));
        }
    }

    tracing::info!(
        target: "weft::exec::ready",
        ready_total = result.len(),
        "find_ready_nodes summary"
    );
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

    // Runtime type enforcement on input ports: the single check point
    // (see `check_input`). A mismatch on a required port aggregates
    // into `type_errors` (the node fails loudly); a mismatch on an
    // optional port nulls the port and the node proceeds.
    for port in &node.inputs {
        let Some(value) = obj.get(&port.name) else {
            continue;
        };
        match check_input(port, value) {
            InputCheck::Ok => {}
            InputCheck::NullIt => {
                obj.insert(port.name.clone(), Value::Null);
            }
            InputCheck::Fail(err) => {
                tracing::error!(target: "weft::exec::ready", node = %node.id, "{err}");
                type_errors.push(err);
                obj.insert(port.name.clone(), Value::Null);
            }
        }
    }

    Value::Object(obj)
}

/// Outcome of checking one incoming value against an input port type.
#[derive(Debug, PartialEq, Eq)]
enum InputCheck {
    /// Value matches the port type (or there's nothing to check).
    Ok,
    /// Type mismatch on an OPTIONAL port: the node didn't get a valid
    /// value here, but it declared it can do without one, so null the
    /// port and let the node proceed.
    NullIt,
    /// Type mismatch on a REQUIRED port: the node cannot proceed.
    Fail(String),
}

/// Check one incoming value against its input port type. THE single
/// place input type enforcement lives.
///
/// Uniform across lane modes because port types are POST-TRANSFORM:
/// by the time a value reaches here the Expand split / Gather collect
/// already happened, and the port type describes exactly what this
/// lane carries (Single: `T`; one Expand element: `T`; gathered list:
/// `List[T]`). So there is no expand/gather branching: every value is
/// checked against the port type as a whole.
///
/// The consequence of a mismatch is decided ONLY by required-vs-
/// optional, never by where the value came from:
///   - optional port (not required, or type admits null) -> `NullIt`
///   - required port -> `Fail`
///
/// Null = "no pulse" is never itself a mismatch; an unresolved port
/// type is never a mismatch (the compiler resolves the types that
/// matter before dispatch).
fn check_input(port: &crate::project::PortDefinition, value: &Value) -> InputCheck {
    if value.is_null() || port.port_type.is_unresolved() || runtime_type_check(&port.port_type, value)
    {
        return InputCheck::Ok;
    }
    // Optional = not required, or the declared type explicitly admits
    // null (`T?` / `T | Null`).
    if !port.required || port.port_type.contains_null() {
        return InputCheck::NullIt;
    }
    InputCheck::Fail(format!(
        "type mismatch on '{}': expected {}, got {}",
        port.name,
        port.port_type,
        WeftType::infer(value)
    ))
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

#[cfg(test)]
mod tests {
    use super::{check_input, InputCheck};
    use crate::project::PortDefinition;
    use serde_json::json;

    fn port(ty: &str, lane_mode: &str, required: bool) -> PortDefinition {
        serde_json::from_value(json!({
            "name": "p", "portType": ty, "required": required, "laneMode": lane_mode
        }))
        .expect("port")
    }

    #[test]
    fn matching_value_is_ok_regardless_of_required() {
        assert_eq!(check_input(&port("String", "Single", true), &json!("ok")), InputCheck::Ok);
        assert_eq!(check_input(&port("String", "Single", false), &json!("ok")), InputCheck::Ok);
    }

    #[test]
    fn null_is_ok_no_pulse() {
        // Null is "no pulse", never itself a mismatch (the skip layer
        // decides whether a required port being null elides the node).
        assert_eq!(check_input(&port("String", "Single", true), &json!(null)), InputCheck::Ok);
    }

    #[test]
    fn mismatch_on_required_fails() {
        let p = port("String", "Single", true);
        match check_input(&p, &json!(42)) {
            InputCheck::Fail(msg) => assert!(msg.contains("expected")),
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    #[test]
    fn mismatch_on_optional_nulls_it() {
        // Optional = not required. The node declared it can do without
        // a valid value here, so a mismatch nulls the port (no fail).
        assert_eq!(check_input(&port("String", "Single", false), &json!(42)), InputCheck::NullIt);
    }

    #[test]
    fn mismatch_on_nullable_required_nulls_it() {
        // A required port whose TYPE admits null (`String | Null`) is
        // optional in the "can-do-without" sense: a mismatch nulls it.
        assert_eq!(
            check_input(&port("String | Null", "Single", true), &json!(42)),
            InputCheck::NullIt
        );
    }

    #[test]
    fn gather_checks_assembled_list_as_a_whole() {
        // A gather input is declared `List[T]` (POST-transform), checked
        // against `List[T]` like any value. No expand/gather special-
        // casing: same code path as Single.
        let req = port("List[Number]", "Gather", true);
        assert_eq!(check_input(&req, &json!([1, 2, 3])), InputCheck::Ok);
        // A skipped sibling's null makes the list `List[Number|Null]`,
        // which fails `List[Number]` on a required port.
        match check_input(&req, &json!([1, null, 3])) {
            InputCheck::Fail(_) => {}
            other => panic!("expected Fail, got {other:?}"),
        }
        // Same gather, optional port: the bad list nulls instead.
        let opt = port("List[Number]", "Gather", false);
        assert_eq!(check_input(&opt, &json!([1, null, 3])), InputCheck::NullIt);
    }

    #[test]
    fn expand_lane_element_checked_against_element_type() {
        // After the split, an Expand lane carries one element checked
        // against the element type T -- the SAME whole-value check.
        let req = port("Number", "Expand", true);
        assert_eq!(check_input(&req, &json!(7)), InputCheck::Ok);
        match check_input(&req, &json!("nan")) {
            InputCheck::Fail(_) => {}
            other => panic!("expected Fail, got {other:?}"),
        }
    }
}
