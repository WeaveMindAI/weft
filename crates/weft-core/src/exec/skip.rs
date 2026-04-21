//! Skip decision: given a ReadyGroup, decide if the node's work
//! should be elided because a required input is null or an
//! `oneOfRequired` group is entirely null.

use std::collections::HashSet;

use crate::lane::Lane;
use crate::project::NodeDefinition;
use crate::pulse::Pulse;
use crate::Color;

pub fn check_should_skip(
    node: &NodeDefinition,
    node_pulses: &[Pulse],
    lane: &Lane,
    color: Color,
    required: &HashSet<&str>,
    wired: &HashSet<&str>,
    config_filled: &HashSet<&str>,
) -> bool {
    // Required ports: each must be either config-filled, unwired, or
    // have a non-null pulse (or null only if its type includes Null).
    for port_name in required {
        if config_filled.contains(port_name) || !wired.contains(port_name) {
            continue;
        }
        match find_pulse(node_pulses, lane, color, port_name) {
            None => return true,
            Some(p) => {
                if p.value.is_null() && !port_accepts_null(node, port_name) {
                    return true;
                }
            }
        }
    }

    // oneOfRequired groups: if EVERY port in a group is null/missing,
    // skip.
    for group in &node.features.one_of_required {
        if group.is_empty() {
            continue;
        }
        let all_null = group.iter().all(|port_name| is_effectively_null(node, node_pulses, lane, color, port_name, wired, config_filled));
        if all_null {
            return true;
        }
    }

    false
}

fn is_effectively_null(
    node: &NodeDefinition,
    node_pulses: &[Pulse],
    lane: &Lane,
    color: Color,
    port_name: &str,
    wired: &HashSet<&str>,
    config_filled: &HashSet<&str>,
) -> bool {
    if config_filled.contains(port_name) {
        return false;
    }
    if !wired.contains(port_name) {
        return true;
    }
    match find_pulse(node_pulses, lane, color, port_name) {
        None => true,
        Some(p) => p.value.is_null() && !port_accepts_null(node, port_name),
    }
}

fn find_pulse<'a>(pulses: &'a [Pulse], lane: &Lane, color: Color, port: &str) -> Option<&'a Pulse> {
    pulses.iter().find(|p| {
        p.status.is_pending()
            && p.color == color
            && p.target_port == port
            && (&p.lane == lane || (p.lane.len() < lane.len() && lane.starts_with(&p.lane)))
    })
}

fn port_accepts_null(node: &NodeDefinition, port_name: &str) -> bool {
    node.inputs
        .iter()
        .find(|p| p.name == port_name)
        .map(|p| p.port_type.contains_null())
        .unwrap_or(false)
}
