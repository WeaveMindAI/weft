//! Pulse preprocessing. Runs before readiness checking to normalize
//! pending pulses so that `find_ready_nodes` can match by
//! `(color, lane)` without special-casing Expand/Gather.
//!
//! Two transformations, applied per node:
//!
//! 1. **Expand**: a pending pulse on an `Expand` port carrying a list
//!    is replaced by N child-lane pending pulses (one per element).
//!    The original is absorbed.
//!
//! 2. **Gather**: when all sibling pending pulses on a `Gather` port
//!    at the deepest lane level arrive (count == expected), they are
//!    replaced by a single parent-lane pending pulse carrying a list.
//!    The originals are absorbed.
//!
//! Returns `true` if any transformation was applied (caller should
//! re-run readiness).

use std::collections::HashMap;

use serde_json::Value;

use crate::exec::typecheck::runtime_type_check;
use crate::lane::{Lane, LaneFrame};
use crate::project::{LaneMode, NodeDefinition, ProjectDefinition};
use crate::pulse::{Pulse, PulseStatus, PulseTable};
use crate::Color;

pub fn preprocess_input(project: &ProjectDefinition, pulses: &mut PulseTable) -> bool {
    let mut work = Vec::new();

    for node in &project.nodes {
        collect_expand_work(node, pulses, &mut work);
        collect_gather_work(node, pulses, &mut work);
    }

    if work.is_empty() {
        return false;
    }

    for w in work {
        apply(w, project, pulses);
    }
    true
}

// ---------------------------------------------------------------------------
// Work collection (read-only phase)
// ---------------------------------------------------------------------------

enum Work {
    Expand(ExpandWork),
    Gather(GatherWork),
}

struct ExpandWork {
    node_id: String,
    absorb_pulse: uuid::Uuid,
    port: String,
    color: Color,
    base_lane: Lane,
    /// (lane-suffix, value) per leaf item produced by peeling
    /// `lane_depth` List layers.
    leaves: Vec<(Lane, Value)>,
}

struct GatherWork {
    node_id: String,
    absorb_pulses: Vec<uuid::Uuid>,
    port: String,
    color: Color,
    parent_lane: Lane,
    gathered: Vec<Value>,
}

fn collect_expand_work(node: &NodeDefinition, pulses: &PulseTable, out: &mut Vec<Work>) {
    let Some(node_pulses) = pulses.get(&node.id) else { return };

    for port in node.inputs.iter().filter(|p| p.lane_mode == LaneMode::Expand) {
        let depth = port.lane_depth.max(1);
        for p in node_pulses.iter().filter(|p| {
            p.status.is_pending() && p.target_port == port.name && p.value.is_array()
        }) {
            let Some(arr) = p.value.as_array() else { continue };
            if arr.is_empty() {
                continue;
            }
            out.push(Work::Expand(ExpandWork {
                node_id: node.id.clone(),
                absorb_pulse: p.id,
                port: port.name.clone(),
                color: p.color,
                base_lane: p.lane.clone(),
                leaves: expand_recursive(&p.value, depth),
            }));
        }
    }
}

fn collect_gather_work(node: &NodeDefinition, pulses: &PulseTable, out: &mut Vec<Work>) {
    let Some(node_pulses) = pulses.get(&node.id) else { return };

    for port in node.inputs.iter().filter(|p| p.lane_mode == LaneMode::Gather) {
        let mut groups: HashMap<(Color, Lane, u32), Vec<(u32, uuid::Uuid, Value)>> = HashMap::new();
        for p in node_pulses.iter().filter(|p| {
            p.status.is_pending()
                && p.target_port == port.name
                && !p.lane.is_empty()
                && !p.gathered
        }) {
            let parent_lane = p.lane[..p.lane.len() - 1].to_vec();
            let frame = *p.lane.last().unwrap();
            groups
                .entry((p.color, parent_lane, frame.count))
                .or_default()
                .push((frame.index, p.id, p.value.clone()));
        }

        for ((color, parent_lane, expected), mut siblings) in groups {
            siblings.sort_by_key(|(idx, _, _)| *idx);
            siblings.dedup_by_key(|(idx, _, _)| *idx);
            if (siblings.len() as u32) < expected {
                continue;
            }

            let already_gathered = node_pulses.iter().any(|p| {
                p.color == color
                    && p.lane == parent_lane
                    && p.target_port == port.name
                    && p.status.is_pending()
            });
            if already_gathered {
                continue;
            }

            let gathered: Vec<Value> = siblings.iter().map(|(_, _, v)| v.clone()).collect();
            let absorb: Vec<uuid::Uuid> = siblings.iter().map(|(_, id, _)| *id).collect();

            out.push(Work::Gather(GatherWork {
                node_id: node.id.clone(),
                absorb_pulses: absorb,
                port: port.name.clone(),
                color,
                parent_lane,
                gathered,
            }));
        }
    }
}

// ---------------------------------------------------------------------------
// Work application (mutating phase)
// ---------------------------------------------------------------------------

fn apply(work: Work, project: &ProjectDefinition, pulses: &mut PulseTable) {
    match work {
        Work::Expand(w) => apply_expand(w, project, pulses),
        Work::Gather(w) => apply_gather(w, project, pulses),
    }
}

fn apply_expand(w: ExpandWork, project: &ProjectDefinition, pulses: &mut PulseTable) {
    if let Some(ps) = pulses.get_mut(&w.node_id) {
        if let Some(p) = ps.iter_mut().find(|p| p.id == w.absorb_pulse) {
            p.absorb();
        }
    }

    let port_type = project
        .nodes
        .iter()
        .find(|n| n.id == w.node_id)
        .and_then(|n| n.inputs.iter().find(|p| p.name == w.port))
        .map(|p| &p.port_type);

    let bucket = pulses.entry(w.node_id.clone()).or_default();
    for (lane_suffix, item) in &w.leaves {
        let mut child_lane = w.base_lane.clone();
        child_lane.extend_from_slice(lane_suffix);

        let checked = match port_type {
            Some(pt) if !item.is_null() && !pt.is_unresolved() && !runtime_type_check(pt, item) => {
                tracing::error!(
                    target: "weft::exec::preprocess",
                    node = %w.node_id, port = %w.port, lane = ?child_lane,
                    "expand type mismatch; coercing to null"
                );
                Value::Null
            }
            _ => item.clone(),
        };

        bucket.push(Pulse::new(w.color, child_lane, w.node_id.clone(), w.port.clone(), checked));
    }
}

fn apply_gather(w: GatherWork, project: &ProjectDefinition, pulses: &mut PulseTable) {
    if let Some(ps) = pulses.get_mut(&w.node_id) {
        for p in ps.iter_mut() {
            if w.absorb_pulses.contains(&p.id) {
                p.absorb();
            }
        }
    }
    let gathered = Value::Array(w.gathered);

    // Lane warning path kept but we always emit the gathered value: a
    // downstream validation error is more actionable than a silent null.
    let port_def = project
        .nodes
        .iter()
        .find(|n| n.id == w.node_id)
        .and_then(|n| n.inputs.iter().find(|p| p.name == w.port));
    if let Some(port) = port_def {
        if !port.port_type.is_unresolved() && !runtime_type_check(&port.port_type, &gathered) {
            tracing::error!(
                target: "weft::exec::preprocess",
                node = %w.node_id, port = %w.port,
                "gather type mismatch; downstream check will reject"
            );
        }
    }

    let bucket = pulses.entry(w.node_id.clone()).or_default();
    let mut pulse = Pulse::new(w.color, w.parent_lane, w.node_id, w.port, gathered);
    pulse.gathered = true;
    bucket.push(pulse);
}

// ---------------------------------------------------------------------------
// List expansion helper
// ---------------------------------------------------------------------------

/// Recursively peel `depth` List layers from `data`, emitting
/// `(lane-suffix, leaf-value)` tuples. For a uniform list, equivalent
/// to a breadth-first walk.
fn expand_recursive(data: &Value, depth: u32) -> Vec<(Lane, Value)> {
    if depth == 0 {
        return vec![(Vec::new(), data.clone())];
    }
    let Some(arr) = data.as_array() else {
        return vec![(vec![LaneFrame { count: 1, index: 0 }], data.clone())];
    };
    if arr.is_empty() {
        return vec![(vec![LaneFrame { count: 1, index: 0 }], Value::Null)];
    }

    let count = arr.len() as u32;
    let mut out = Vec::new();
    for (i, item) in arr.iter().enumerate() {
        let head = LaneFrame { count, index: i as u32 };
        for (tail, leaf) in expand_recursive(item, depth - 1) {
            let mut lane = vec![head];
            lane.extend(tail);
            out.push((lane, leaf));
        }
    }
    out
}
