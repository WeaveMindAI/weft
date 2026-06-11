//! Readiness. Find which nodes have enough pending pulses to fire at
//! a matching `(color, frames)`, aggregate their inputs, return as
//! `ReadyGroup`s.
//!
//! Matching is exact-frame: a firing at `(color, frames)` only sees
//! pulses whose `frames` are exactly the firing's frame stack. Loops
//! emit broadcast inputs and the implicit `self.index` at the body's
//! own frame stack directly, one pulse per iteration.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::exec::skip::check_should_skip;
use crate::exec::typecheck::runtime_type_check;
use crate::frames::LoopFrames;
use crate::project::{EdgeIndex, GroupBoundaryRole, NodeDefinition, ProjectDefinition};
use crate::pulse::{Pulse, PulseTable};
use crate::weft_type::WeftType;
use crate::Color;

/// One dispatch ready to fire. `input` is the aggregated inputs
/// object; `pulse_ids` are the pulses that will be absorbed when the
/// caller commits the dispatch.
pub struct ReadyGroup {
    pub frames: LoopFrames,
    pub color: Color,
    pub input: Value,
    /// Wired ports whose resolved pulse for this firing was a CLOSURE
    /// (the upstream terminated without firing the port). Disjoint
    /// from the keys of `input`: closures carry no data, so they
    /// never appear there. Surfaces through `NodeStarted` so the
    /// inspector can render closed ports distinctly from user-emitted
    /// nulls.
    pub closed_ports: Vec<String>,
    pub should_skip: bool,
    pub pulse_ids: Vec<uuid::Uuid>,
    pub error: Option<String>,
}

/// THE single rule for "which pulse does a firing at (color, frames)
/// see on `port`?". Exact-frame match only: a firing at one frame
/// stack sees only pulses at the SAME frame stack.
///
/// Two-pulse case at the exact key: at most one non-closed pending
/// data pulse can coexist with one pending closure (the closure was a
/// pre-emission from a sibling producer whose port terminated, AND a
/// later sibling emitted real data; per "data outranks
/// structural-nothing", both stay in the table and this resolver
/// prefers the non-closed one). `find_groups_for_node` absorbs every
/// pulse at the firing's exact `(color, frames)` together, so the
/// closure does not leak across ticks. This shape makes live and
/// replay agree by construction.
///
/// Returns `None` when nothing pending reaches this firing on this port.
pub(crate) fn resolve_port_value<'a>(
    pulses: &'a [Pulse],
    color: Color,
    frames: &LoopFrames,
    port: &str,
) -> Option<&'a Pulse> {
    let mut winner: Option<&Pulse> = None;
    for p in pulses
        .iter()
        .filter(|p| p.status.is_pending() && p.color == color && p.target_port == port && &p.frames == frames)
    {
        winner = Some(match winner {
            None => p,
            Some(current) => {
                if pulse_rank(p) > pulse_rank(current) { p } else { current }
            }
        });
    }
    winner
}

/// Higher rank wins. Data (1) > closure (0).
fn pulse_rank(p: &Pulse) -> u8 {
    if p.closed { 0 } else { 1 }
}


pub fn find_ready_nodes(
    project: &ProjectDefinition,
    pulses: &PulseTable,
    edge_idx: &EdgeIndex,
) -> Vec<(String, ReadyGroup)> {
    let mut result = Vec::new();

    for node in &project.nodes {
        let Some(node_pulses) = pulses.get(&node.id) else {
            continue;
        };
        let pending_count = node_pulses.iter().filter(|p| p.status.is_pending()).count();
        if pending_count == 0 {
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

        let groups = find_groups_for_node(
            node, node_pulses, &required, &wired, &config_filled, has_incoming,
        );
        for group in groups {
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
    let pending: Vec<&Pulse> = node_pulses
        .iter()
        .filter(|p| p.status.is_pending())
        .collect();

    // Group pulses by (color, frames). A firing is one exact point in
    // frame space; matching is exact.
    let mut groups: HashMap<(Color, LoopFrames), Vec<&Pulse>> = HashMap::new();
    for p in &pending {
        groups
            .entry((p.color, p.frames.clone()))
            .or_default()
            .push(p);
    }

    let mut ready = Vec::new();

    for ((color, frames), group_pulses) in &groups {
        let all_satisfied = wired.iter().all(|port_name| {
            group_pulses.iter().any(|p| p.target_port == *port_name)
        });

        if has_incoming && !all_satisfied {
            continue;
        }

        let mut type_errors = Vec::new();
        let input = build_input(node, node_pulses, frames, color, wired, &mut type_errors);

        // Group/Loop boundary skip rules: only In-boundary skips; Out
        // forwards whatever came through.
        let is_out_boundary = node
            .group_boundary
            .as_ref()
            .map(|gb| gb.role == GroupBoundaryRole::Out)
            .unwrap_or(false);
        let should_skip = if is_out_boundary || !has_incoming {
            false
        } else {
            check_should_skip(node, node_pulses, frames, *color, required, wired, config_filled)
        };

        let pulse_ids: Vec<uuid::Uuid> = group_pulses.iter().map(|p| p.id).collect();

        let mut closed_ports: Vec<String> = wired
            .iter()
            .filter(|port_name| {
                resolve_port_value(node_pulses, *color, frames, port_name)
                    .map(|p| p.closed)
                    .unwrap_or(false)
            })
            .map(|p| p.to_string())
            .collect();
        closed_ports.sort();

        ready.push(ReadyGroup {
            frames: frames.clone(),
            color: *color,
            input,
            closed_ports,
            should_skip,
            pulse_ids,
            error: if type_errors.is_empty() { None } else { Some(type_errors.join("; ")) },
        });
    }

    ready
}

// ---------------------------------------------------------------------------
// Input aggregation
// ---------------------------------------------------------------------------

fn build_input(
    node: &NodeDefinition,
    node_pulses: &[Pulse],
    frames: &LoopFrames,
    color: &Color,
    wired: &HashSet<&str>,
    type_errors: &mut Vec<String>,
) -> Value {
    let mut obj = Map::new();

    // Per-port value resolution via the shared `resolve_port_value`
    // (also used by `skip::port_arrived_closed`). Enumerate distinct
    // ports that have any pending pulse at this exact frame, then
    // resolve each once. build_input and skip share ONE definition of
    // "which pulse does this port see" so the two layers can never
    // disagree on the firing's view.
    let distinct_ports: HashSet<&str> = node_pulses
        .iter()
        .filter(|p| p.status.is_pending() && &p.color == color && &p.frames == frames)
        .map(|p| p.target_port.as_str())
        .collect();
    for port in distinct_ports {
        if let Some(winner) =
            resolve_port_value(node_pulses, *color, frames, port)
        {
            if winner.closed {
                continue;
            }
            obj.insert(port.to_string(), winner.value.clone());
        }
    }

    fill_input_from_config(node, wired, &mut obj);

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
    Ok,
    NullIt,
    Fail(String),
}

/// Insert each UNWIRED configurable input port's config value into
/// `obj`. Wires are authoritative: a wired port whose pulse resolved
/// to a closure stays absent (the closure means upstream produced
/// nothing; silently substituting the config value would mask the
/// upstream failure AND contradict the skip layer, whose
/// `config_filled` set deliberately excludes wired ports). Shared
/// between the pulse-driven dispatch path (`build_input`) and the
/// kick-driven dispatch path (`build_kicked_input` below) so the two
/// paths can't disagree on what counts as "configured".
pub fn fill_input_from_config(
    node: &NodeDefinition,
    wired: &HashSet<&str>,
    obj: &mut Map<String, Value>,
) {
    for port in &node.inputs {
        if !port.configurable
            || wired.contains(port.name.as_str())
            || obj.contains_key(&port.name)
        {
            continue;
        }
        if let Some(cfg) = node.config.get(&port.name) {
            if !cfg.is_null() {
                obj.insert(port.name.clone(), cfg.clone());
            }
        }
    }
}

/// Build an InputBag for a node firing from a KICK (entry node /
/// trigger payload), not from upstream pulses. Starts empty and
/// fills only from the node's config (configurable input ports).
/// This is what makes a `Range { from: 0, to: 10, step: 2 }` orphan
/// see its config values at runtime.
///
/// Wake payloads from trigger kicks ride a separate channel
/// (`ctx.wake_payload()`) that the engine wires up at dispatch time,
/// so they don't need to be merged here.
pub fn build_kicked_input(node: &NodeDefinition) -> Value {
    let mut obj = Map::new();
    // Kicked nodes are entry points: no wired pending inputs by
    // definition, so the wired set is empty.
    fill_input_from_config(node, &HashSet::new(), &mut obj);
    Value::Object(obj)
}

/// Check one incoming value against its input port type. THE single
/// place input type enforcement lives.
fn check_input(port: &crate::project::PortDefinition, value: &Value) -> InputCheck {
    if value.is_null()
        || port.port_type.is_unresolved()
        || runtime_type_check(&port.port_type, value)
    {
        return InputCheck::Ok;
    }
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

#[cfg(test)]
mod tests {
    use super::{build_kicked_input, check_input, resolve_port_value, InputCheck};
    use crate::frames::LoopIteration;
    use crate::project::{NodeDefinition, PortDefinition, Position};
    use crate::pulse::Pulse;
    use crate::NodeFeatures;
    use serde_json::json;

    fn frame(i: u32) -> LoopIteration {
        LoopIteration { index: i }
    }

    #[test]
    fn resolve_port_value_exact_frame_only() {
        let color = uuid::Uuid::nil();
        let firing_frames = vec![frame(0), frame(1)];
        let pulses = vec![
            // Shallower-frame pulses are NOT visible: no prefix
            // broadcast in the new world.
            Pulse::new(color, vec![], "n", "p", json!("shallow")),
            Pulse::new(color, vec![frame(0)], "n", "p", json!("mid")),
            Pulse::new(color, firing_frames.clone(), "n", "p", json!("exact")),
        ];
        let winner = resolve_port_value(&pulses, color, &firing_frames, "p")
            .expect("exact-frame pulse reaches firing");
        assert_eq!(winner.value, json!("exact"));
    }

    #[test]
    fn resolve_port_value_returns_none_if_no_exact_match() {
        let color = uuid::Uuid::nil();
        let firing_frames = vec![frame(0), frame(1)];
        let pulses = vec![
            Pulse::new(color, vec![], "n", "p", json!("shallow")),
        ];
        assert!(resolve_port_value(&pulses, color, &firing_frames, "p").is_none());
    }

    #[test]
    fn resolve_port_value_prefers_data_over_closure_at_same_key() {
        let color = uuid::Uuid::nil();
        let frames = vec![];
        let pulses = vec![
            Pulse::closure(color, frames.clone(), "n", "p"),
            Pulse::new(color, frames.clone(), "n", "p", json!(42)),
        ];
        let winner = resolve_port_value(&pulses, color, &frames, "p")
            .expect("a winner");
        assert_eq!(winner.value, json!(42));
        assert!(!winner.closed);
    }

    fn port(ty: &str, required: bool) -> PortDefinition {
        serde_json::from_value(json!({
            "name": "p", "portType": ty, "required": required
        }))
        .expect("port")
    }

    #[test]
    fn matching_value_is_ok_regardless_of_required() {
        assert_eq!(check_input(&port("String", true), &json!("ok")), InputCheck::Ok);
        assert_eq!(check_input(&port("String", false), &json!("ok")), InputCheck::Ok);
    }

    #[test]
    fn null_is_ok_no_pulse() {
        assert_eq!(check_input(&port("String", true), &json!(null)), InputCheck::Ok);
    }

    #[test]
    fn mismatch_on_required_fails() {
        let p = port("String", true);
        match check_input(&p, &json!(42)) {
            InputCheck::Fail(msg) => assert!(msg.contains("expected")),
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    #[test]
    fn mismatch_on_optional_nulls_it() {
        assert_eq!(check_input(&port("String", false), &json!(42)), InputCheck::NullIt);
    }

    #[test]
    fn mismatch_on_nullable_required_nulls_it() {
        assert_eq!(
            check_input(&port("String | Null", true), &json!(42)),
            InputCheck::NullIt
        );
    }

    fn kicked_node(node_type: &str, inputs: Vec<PortDefinition>, config: serde_json::Value) -> NodeDefinition {
        NodeDefinition {
            id: "k".into(),
            node_type: node_type.into(),
            label: None,
            config,
            position: Position { x: 0.0, y: 0.0 },
            inputs,
            outputs: Vec::new(),
            features: NodeFeatures::default(),
            scope: Vec::new(),
            group_boundary: None,
            requires_infra: false,
            images: Vec::new(),
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        }
    }

    /// Regression: a kicked orphan node (entry node with no incoming
    /// edges) used to receive an empty input bag, so a configurable
    /// port like `Range.to` set via source config (`Range { to: 10 }`)
    /// arrived at runtime as `missing input on port: to`. The kick
    /// path now flows through `build_kicked_input` which fills
    /// configurable ports from `node.config`.
    #[test]
    fn build_kicked_input_fills_configurable_ports_from_config() {
        let inputs = vec![
            serde_json::from_value(json!({
                "name": "from", "portType": "Number",
                "required": false, "configurable": true,
            })).unwrap(),
            serde_json::from_value(json!({
                "name": "to", "portType": "Number",
                "required": true, "configurable": true,
            })).unwrap(),
            serde_json::from_value(json!({
                "name": "step", "portType": "Number",
                "required": false, "configurable": true,
            })).unwrap(),
        ];
        let config = json!({ "from": 0, "to": 10, "step": 2 });
        let node = kicked_node("Range", inputs, config);
        let input = build_kicked_input(&node);
        assert_eq!(input, json!({ "from": 0, "to": 10, "step": 2 }));
    }

    /// A wired-only port (`configurable: false`) must NOT be filled
    /// from config even if the user accidentally put a value in the
    /// node's config map: that's the wired-only contract.
    #[test]
    fn build_kicked_input_ignores_non_configurable_ports() {
        let inputs = vec![
            serde_json::from_value(json!({
                "name": "in", "portType": "String",
                "required": true, "configurable": false,
            })).unwrap(),
        ];
        let config = json!({ "in": "should be ignored" });
        let node = kicked_node("X", inputs, config);
        let input = build_kicked_input(&node);
        assert_eq!(input, json!({}));
    }

    /// Wires are authoritative: a WIRED configurable port whose pulse
    /// resolved to a closure (so it is absent from the bag) must NOT
    /// be silently backfilled from config. The closure means upstream
    /// produced nothing; substituting the config value would mask the
    /// upstream failure and contradict the skip layer (whose
    /// `config_filled` set excludes wired ports).
    #[test]
    fn fill_input_from_config_skips_wired_ports() {
        let inputs = vec![
            serde_json::from_value(json!({
                "name": "wired_p", "portType": "Number",
                "required": false, "configurable": true,
            })).unwrap(),
            serde_json::from_value(json!({
                "name": "free_p", "portType": "Number",
                "required": false, "configurable": true,
            })).unwrap(),
        ];
        let config = json!({ "wired_p": 1, "free_p": 2 });
        let node = kicked_node("X", inputs, config);
        let wired: std::collections::HashSet<&str> = ["wired_p"].into_iter().collect();
        let mut obj = serde_json::Map::new();
        super::fill_input_from_config(&node, &wired, &mut obj);
        assert_eq!(
            serde_json::Value::Object(obj),
            json!({ "free_p": 2 }),
            "wired port stays absent; unwired port fills from config"
        );
    }
}
