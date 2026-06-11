//! Skip decision: given a ReadyGroup (every wired port has an
//! arrival), decide whether to run the body or short-circuit to a
//! Skipped lifecycle event.
//!
//! Two skip reasons, both about CLOSURES (the structural "nothing's
//! coming" marker), never about user-emitted null values:
//!
//!   - Any REQUIRED port arrived as a closure -> skip. Required means
//!     the body declared "I cannot run without this", and the engine
//!     has proof nothing will ever arrive.
//!   - Every port (when ALL inputs are optional) arrived as a closure
//!     -> skip. Nothing the body could meaningfully act on. Running a
//!     no-input firing would be busy-work.
//!
//! A user-emitted `null` on a required port is NOT a skip. Null is
//! data the body has to interpret; the body runs.

use std::collections::HashSet;

use crate::frames::LoopFrames;
use crate::project::NodeDefinition;
use crate::pulse::Pulse;
use crate::Color;

#[allow(clippy::too_many_arguments)]
pub fn check_should_skip(
    node: &NodeDefinition,
    node_pulses: &[Pulse],
    frames: &LoopFrames,
    color: Color,
    required: &HashSet<&str>,
    wired: &HashSet<&str>,
    config_filled: &HashSet<&str>,
) -> bool {
    // Rule 1: any wired required port that arrived as a closure -> skip.
    // (`config_filled` never overlaps `wired`: wires are authoritative
    // and config only fills unwired ports, so no config check here.)
    for port_name in required {
        if !wired.contains(port_name) {
            continue;
        }
        if port_arrived_closed(node_pulses, frames, color, port_name) {
            return true;
        }
    }

    // Rule 2: every port dead -> skip. Covers the all-optional case
    // (a node with only optional inputs whose every input was closed
    // upstream has no value to act on). For nodes with at least one
    // required input, rule 1 already covered the "required closed"
    // case; this catches "every optional closed too" only when there
    // are no required inputs at all. A port is dead when it arrived
    // as a closure OR can never produce a value at all (unwired and
    // not config-filled); counting unwired ports as alive would fire
    // the body with a completely empty input bag, exactly the
    // busy-work firing this rule exists to prevent. Same shape as the
    // oneOfRequired loop below.
    if !node.inputs.is_empty() {
        let all_dead = node.inputs.iter().all(|port| {
            if config_filled.contains(port.name.as_str()) {
                return false;
            }
            if !wired.contains(port.name.as_str()) {
                return true;
            }
            port_arrived_closed(node_pulses, frames, color, &port.name)
        });
        if all_dead {
            return true;
        }
    }

    // oneOfRequired groups: skip when EVERY port in a group is closed.
    for group in &node.features.one_of_required {
        if group.is_empty() {
            continue;
        }
        let all_closed = group.iter().all(|port_name| {
            if config_filled.contains(port_name.as_str()) {
                return false;
            }
            if !wired.contains(port_name.as_str()) {
                return true;
            }
            port_arrived_closed(node_pulses, frames, color, port_name)
        });
        if all_closed {
            return true;
        }
    }

    false
}

fn port_arrived_closed(
    node_pulses: &[Pulse],
    frames: &LoopFrames,
    color: Color,
    port_name: &str,
) -> bool {
    super::ready::resolve_port_value(node_pulses, color, frames, port_name)
        .map(|p| p.closed)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pulse::Pulse;
    use serde_json::json;

    fn node_one_required() -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n",
            "nodeType": "X",
            "label": null,
            "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [{ "name": "a", "portType": "Number", "required": true }],
            "outputs": [],
            "features": {},
            "scope": [],
            "groupBoundary": null,
            "requiresInfra": false,
            "images": []
        }))
        .expect("node json")
    }

    fn node_all_optional() -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n",
            "nodeType": "X",
            "label": null,
            "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [
                { "name": "a", "portType": "Number", "required": false },
                { "name": "b", "portType": "Number", "required": false }
            ],
            "outputs": [],
            "features": {},
            "scope": [],
            "groupBoundary": null,
            "requiresInfra": false,
            "images": []
        }))
        .expect("node json")
    }

    fn data_pulse(port: &str, value: serde_json::Value) -> Pulse {
        Pulse::new(uuid::Uuid::nil(), Vec::new(), "n", port, value)
    }

    fn closure_pulse(port: &str) -> Pulse {
        Pulse::closure(uuid::Uuid::nil(), Vec::new(), "n", port)
    }

    #[test]
    fn user_null_on_required_does_not_skip() {
        let node = node_one_required();
        let pulses = vec![data_pulse("a", json!(null))];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let config_filled = HashSet::new();
        assert!(
            !check_should_skip(&node, &pulses, &Vec::new(), uuid::Uuid::nil(), &required, &wired, &config_filled),
            "user-emitted null is data; required port + null must NOT skip"
        );
    }

    #[test]
    fn closure_on_required_skips() {
        let node = node_one_required();
        let pulses = vec![closure_pulse("a")];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let config_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &pulses, &Vec::new(), uuid::Uuid::nil(), &required, &wired, &config_filled),
            "closure on a required port must skip"
        );
    }

    #[test]
    fn all_optional_all_closed_skips() {
        let node = node_all_optional();
        let pulses = vec![closure_pulse("a"), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let config_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &pulses, &Vec::new(), uuid::Uuid::nil(), &required, &wired, &config_filled),
            "all-optional + all-closed must skip"
        );
    }

    #[test]
    fn all_optional_one_value_fires() {
        let node = node_all_optional();
        let pulses = vec![data_pulse("a", json!(7)), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let config_filled = HashSet::new();
        assert!(
            !check_should_skip(&node, &pulses, &Vec::new(), uuid::Uuid::nil(), &required, &wired, &config_filled),
            "all-optional + one value + one closed must NOT skip"
        );
    }

    #[test]
    fn all_optional_closed_wire_plus_unwired_unconfigured_skips() {
        // `a` wired and closed, `b` unwired with no config: nothing
        // the body could act on. Counting the unwired port as alive
        // would fire the body with an empty input bag (busy-work).
        let node = node_all_optional();
        let pulses = vec![closure_pulse("a")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let config_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &pulses, &Vec::new(), uuid::Uuid::nil(), &required, &wired, &config_filled),
            "closed wire + unwired unconfigured port must skip"
        );
    }

    #[test]
    fn all_optional_closed_wire_plus_config_filled_fires() {
        // Same shape but `b` is config-filled: the body has a value
        // to act on, so it fires.
        let node = node_all_optional();
        let pulses = vec![closure_pulse("a")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let config_filled: HashSet<&str> = ["b"].into_iter().collect();
        assert!(
            !check_should_skip(&node, &pulses, &Vec::new(), uuid::Uuid::nil(), &required, &wired, &config_filled),
            "a config-filled port keeps the node alive"
        );
    }

    #[test]
    fn all_optional_one_user_null_fires() {
        let node = node_all_optional();
        let pulses = vec![data_pulse("a", json!(null)), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let config_filled = HashSet::new();
        assert!(
            !check_should_skip(&node, &pulses, &Vec::new(), uuid::Uuid::nil(), &required, &wired, &config_filled),
            "user-emitted null is data; one null + one closed must NOT skip"
        );
    }
}
