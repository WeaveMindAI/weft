//! Skip decision: given a ReadyGroup (every wired port has an
//! arrival), decide whether to run the body or short-circuit to a
//! Skipped lifecycle event, and say WHY.
//!
//! Most skips are about CLOSURES (the structural "nothing's coming"
//! marker), never about user-emitted null values:
//!
//!   - Any REQUIRED port arrived as a closure -> skip. Required means
//!     the body declared "I cannot run without this", and the engine
//!     has proof nothing will ever arrive.
//!   - Every port (when ALL inputs are optional) arrived as a closure
//!     -> skip. Nothing the body could meaningfully act on. Running a
//!     no-input firing would be busy-work.
//!   - Every port of a `@require_one_of` group arrived as a closure.
//!
//! A user-emitted `null` on a required port is NOT a skip. Null is
//! data the body has to interpret; the body runs.
//!
//! The ONE exception, and the only place a VALUE decides: the
//! `_should_flow` port. It is the author saying whether this node runs
//! at all, so `false` on it skips the node the same way a closure
//! does. Nothing else reads a value here.
//!
//! `Generator[T]` ports are exempt from every closure rule: a closure
//! on a generator port is the stream's END, and an end with no items
//! before it is the EMPTY STREAM, a value the body acts on (the
//! documented pull contract answers `Ok(None)` immediately). Skipping
//! the consumer there would make "zero items" behave differently from
//! "one item", and the body's post-loop code would never run.

use std::collections::HashSet;

use crate::project::NodeDefinition;
use crate::pulse::Pulse;

/// Why a firing did not run. Carried on the Skipped lifecycle event so
/// the inspector can tell a DECISION (the author's `_should_flow` said
/// no) from a CONSEQUENCE (an input it needed never arrived).
// SYNC: SkipReason <-> packages/weft-graph/src/protocol.ts SkipReason
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SkipReason {
    /// `_should_flow` arrived `false`: this node was told not to run.
    DidNotFlow,
    /// `_should_flow` closed: whatever decides whether this node runs
    /// never said yes.
    FlowClosed,
    /// A required input arrived closed, so nothing can drive the body.
    RequiredInputClosed { port: String },
    /// Every input this node could act on arrived closed (or could
    /// never arrive at all).
    EveryInputClosed,
    /// Every port of a `@require_one_of` group arrived closed.
    OneOfGroupClosed { ports: Vec<String> },
    /// The scope this node lives in (a group or a loop) did not run:
    /// its `_should_flow` said no, or a loop's list never came. Every
    /// node inside a gated scope carries this, however deep.
    ScopeSkipped { scope: String },
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DidNotFlow => write!(f, "its `_should_flow` said no"),
            Self::FlowClosed => write!(f, "nothing ever answered its `_should_flow`"),
            Self::RequiredInputClosed { port } => {
                write!(f, "the required input '{port}' closed")
            }
            Self::EveryInputClosed => write!(f, "every input closed"),
            Self::OneOfGroupClosed { ports } => {
                write!(f, "every input of the group ({}) closed", ports.join(", "))
            }
            Self::ScopeSkipped { scope } => {
                write!(f, "the scope '{scope}' it lives in did not run")
            }
        }
    }
}

/// The port every node carries, deciding whether it runs at all.
/// Unwired it is the literal `true`; wired, a `false` value or a
/// closure skips the node.
// SYNC: SHOULD_FLOW_PORT <-> packages/weft-graph/src/protocol.ts SHOULD_FLOW_PORT,
// docs/src/language/syntax.md (reserved keys),
// packages/weft-syntax/weft.tmLanguage.json (reserved-key rule; see its README)
pub const SHOULD_FLOW_PORT: &str = "_should_flow";

/// Rule 0 of the skip decision: did `_should_flow` say no? The only
/// rule that reads a VALUE, because this port IS the author's decision
/// rather than data: a `false` means "do not run this", written on a
/// wire or straight into the braces. A closure means whatever computes
/// the permission never spoke, which is a different fact and gets its
/// own reason.
///
/// Split out of `check_should_skip` because it applies to EVERY node,
/// including entry nodes with no incoming edges (which the closure
/// rules never see: with nothing wired there is nothing to close, but
/// a `_should_flow: false` literal still turns the node off).
pub fn check_flow_permission(node: &NodeDefinition, group_pulses: &[&Pulse]) -> Option<SkipReason> {
    match super::ready::resolve_port_value(group_pulses, SHOULD_FLOW_PORT) {
        Some(pulse) if pulse.closed => Some(SkipReason::FlowClosed),
        Some(pulse) if *pulse.value == serde_json::Value::Bool(false) => {
            Some(SkipReason::DidNotFlow)
        }
        // A value arrived and it is not `false`: this node flows.
        Some(_) => None,
        // Nothing on the wire: a literal in the braces still decides.
        None => {
            if node.port_literals.get(SHOULD_FLOW_PORT) == Some(&serde_json::Value::Bool(false)) {
                Some(SkipReason::DidNotFlow)
            } else {
                None
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn check_should_skip(
    node: &NodeDefinition,
    group_pulses: &[&Pulse],
    required: &HashSet<&str>,
    wired: &HashSet<&str>,
    literal_filled: &HashSet<&str>,
) -> Option<SkipReason> {
    // Rule 0: `_should_flow` said no. The port is optional, so neither
    // of its skip reasons reaches rule 1.
    if let Some(reason) = check_flow_permission(node, group_pulses) {
        return Some(reason);
    }

    // Rule 1: any wired required port that arrived as a closure -> skip.
    // (`literal_filled` never overlaps `wired`: wires are authoritative
    // and config only fills unwired ports, so no config check here.)
    // Generator ports are exempt: their closure is the empty stream,
    // not "nothing is coming" (module doc).
    for port_name in required {
        if !wired.contains(port_name) || super::ready::is_generator_input(node, port_name) {
            continue;
        }
        if port_arrived_closed(group_pulses, port_name) {
            return Some(SkipReason::RequiredInputClosed { port: (*port_name).to_string() });
        }
    }

    // A scope's In boundary stops here: `_should_flow` is the only gate
    // a group has, and a loop's only other gates are the lists it
    // iterates (its required ports, rule 1). Every other input of a
    // scope is optional at the boundary: a closed one closes that edge
    // into the children, and each child applies its own rule as if the
    // scope were not there. So "every input closed" never skips a
    // scope; it starts, and what runs inside decides for itself.
    if node
        .group_boundary
        .as_ref()
        .is_some_and(|b| b.role == crate::project::GroupBoundaryRole::In)
    {
        return None;
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
    // Only inputs a wire can drive participate: a literal-only port (a
    // compiler-read list, an author-narrowed setting) is not data flow,
    // so it neither keeps the node alive (a node whose data wires all
    // closed has nothing to act on) nor counts as a dead port (a node
    // with ONLY such inputs is an emitter and must run).
    // `_should_flow` is permission, not data: it can never be the value
    // a body acts on, so it neither keeps a node alive here nor counts
    // as one of the dead ports. Rule 0 above is the only rule that reads
    // it.
    let wireable: Vec<&crate::project::InputDefinition> = node
        .inputs
        .iter()
        .filter(|p| p.accepts.wire && p.name != SHOULD_FLOW_PORT)
        .collect();
    if !wireable.is_empty() {
        let all_dead = wireable.iter().all(|port| {
            if literal_filled.contains(port.name.as_str()) {
                return false;
            }
            if !wired.contains(port.name.as_str()) {
                return true;
            }
            // A wired generator port in a formed group has an arrival
            // by construction, and even a closure-arrival is a live
            // value (the empty stream): never dead.
            if port.is_generator() {
                return false;
            }
            port_arrived_closed(group_pulses, &port.name)
        });
        if all_dead {
            return Some(SkipReason::EveryInputClosed);
        }
    }

    // oneOfRequired groups: skip when EVERY port in a group is closed.
    for group in &node.features.one_of_required {
        if group.is_empty() {
            continue;
        }
        let all_closed = group.iter().all(|port_name| {
            if literal_filled.contains(port_name.as_str()) {
                return false;
            }
            if !wired.contains(port_name.as_str()) {
                return true;
            }
            // A generator port's closure is the empty stream, a live
            // value; it keeps its oneOf group satisfied.
            if super::ready::is_generator_input(node, port_name) {
                return false;
            }
            port_arrived_closed(group_pulses, port_name)
        });
        if all_closed {
            return Some(SkipReason::OneOfGroupClosed { ports: group.clone() });
        }
    }

    None
}

fn port_arrived_closed(group_pulses: &[&Pulse], port_name: &str) -> bool {
    super::ready::resolve_port_value(group_pulses, port_name)
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

    /// A node with a data input plus the language's own guard port, the
    /// shape enrich gives every catalog node.
    fn node_with_guard() -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n",
            "nodeType": "X",
            "label": null,
            "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [
                { "name": "a", "portType": "Number", "required": true },
                { "name": SHOULD_FLOW_PORT, "portType": "T", "required": false }
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

    /// The guard is the one port whose VALUE decides: `false` skips the
    /// node even though every input it needs arrived.
    #[test]
    fn a_false_guard_skips_a_node_whose_inputs_all_arrived() {
        let node = node_with_guard();
        let pulses = vec![data_pulse("a", json!(7)), data_pulse(SHOULD_FLOW_PORT, json!(false))];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a", SHOULD_FLOW_PORT].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new()),
            Some(SkipReason::DidNotFlow),
        );
    }

    /// A guard that never spoke is a different fact from one that said
    /// no, and the journal has to be able to tell them apart.
    #[test]
    fn a_closed_guard_skips_with_its_own_reason() {
        let node = node_with_guard();
        let pulses = vec![data_pulse("a", json!(7)), closure_pulse(SHOULD_FLOW_PORT)];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a", SHOULD_FLOW_PORT].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new()),
            Some(SkipReason::FlowClosed),
        );
    }

    /// Any other value is permission: only `false` says no, so a node
    /// guarded by something that emits a string still runs.
    #[test]
    fn a_non_false_guard_lets_the_node_run() {
        let node = node_with_guard();
        for value in [json!(true), json!("ready"), json!(0), json!(null)] {
            let pulses =
                vec![data_pulse("a", json!(7)), data_pulse(SHOULD_FLOW_PORT, value.clone())];
            let required: HashSet<&str> = ["a"].into_iter().collect();
            let wired: HashSet<&str> = ["a", SHOULD_FLOW_PORT].into_iter().collect();
            assert_eq!(
                check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new()),
                None,
                "a guard carrying {value} is not a refusal",
            );
        }
    }

    /// `_should_flow: false` written straight into the braces turns a
    /// node off with no wire at all.
    #[test]
    fn a_false_guard_literal_skips_the_node() {
        let mut node = node_with_guard();
        node.port_literals.insert(SHOULD_FLOW_PORT.to_string(), json!(false));
        let pulses = vec![data_pulse("a", json!(7))];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new()),
            Some(SkipReason::DidNotFlow),
        );
    }

    /// The guard is permission, never data: a live guard must not keep
    /// a node alive whose every real input closed, or the body fires
    /// with nothing to act on.
    #[test]
    fn a_live_guard_does_not_revive_a_node_whose_inputs_all_closed() {
        let mut node = node_all_optional();
        node.inputs.push(
            serde_json::from_value(json!({
                "name": SHOULD_FLOW_PORT, "portType": "T", "required": false
            }))
            .expect("guard port"),
        );
        let pulses = vec![
            closure_pulse("a"),
            closure_pulse("b"),
            data_pulse(SHOULD_FLOW_PORT, json!(true)),
        ];
        let wired: HashSet<&str> = ["a", "b", SHOULD_FLOW_PORT].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &HashSet::new(), &wired, &HashSet::new()),
            Some(SkipReason::EveryInputClosed),
        );
    }

    fn data_pulse(port: &str, value: serde_json::Value) -> Pulse {
        Pulse::new(uuid::Uuid::new_v4(), uuid::Uuid::nil(), Vec::new(), "n", port, std::sync::Arc::new(value))
    }

    fn closure_pulse(port: &str) -> Pulse {
        Pulse::closure(uuid::Uuid::new_v4(), uuid::Uuid::nil(), Vec::new(), "n", port)
    }

    /// The pulses as a firing sees them.
    fn view(pulses: &[Pulse]) -> Vec<&Pulse> {
        pulses.iter().collect()
    }

    fn node_generator_required() -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n",
            "nodeType": "X",
            "label": null,
            "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [{ "name": "rows", "portType": "Generator[Number]", "required": true }],
            "outputs": [],
            "features": {},
            "scope": [],
            "groupBoundary": null,
            "requiresInfra": false,
            "images": []
        }))
        .expect("node json")
    }

    #[test]
    fn empty_stream_closure_on_a_generator_port_does_not_skip() {
        // A closure on a Generator port is the EMPTY STREAM, a value
        // the body consumes (`next()` answers Ok(None) right away),
        // never a "nothing is coming" skip signal.
        let node = node_generator_required();
        let pulses = vec![closure_pulse("rows")];
        let required: HashSet<&str> = ["rows"].into_iter().collect();
        let wired: HashSet<&str> = ["rows"].into_iter().collect();
        let literal_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &literal_filled).is_none(),
            "an empty stream must RUN its consumer, not skip it"
        );
    }

    #[test]
    fn user_null_on_required_does_not_skip() {
        let node = node_one_required();
        let pulses = vec![data_pulse("a", json!(null))];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let literal_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &literal_filled).is_none(),
            "user-emitted null is data; required port + null must NOT skip"
        );
    }

    #[test]
    fn closure_on_required_skips() {
        let node = node_one_required();
        let pulses = vec![closure_pulse("a")];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let literal_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &literal_filled).is_some(),
            "closure on a required port must skip"
        );
    }

    #[test]
    fn all_optional_all_closed_skips() {
        let node = node_all_optional();
        let pulses = vec![closure_pulse("a"), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let literal_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &literal_filled).is_some(),
            "all-optional + all-closed must skip"
        );
    }

    /// A scope's In boundary has one gate, `_should_flow`: every input
    /// closed does not skip it (the closures reach the children, who
    /// decide for themselves), where the same shape on a plain node
    /// skips.
    #[test]
    fn a_scope_in_boundary_never_skips_on_closed_inputs() {
        let mut boundary = node_all_optional();
        boundary.group_boundary = Some(crate::project::GroupBoundary {
            group_id: "g".into(),
            role: crate::project::GroupBoundaryRole::In,
        });
        let pulses = vec![closure_pulse("a"), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let literal_filled = HashSet::new();
        assert!(
            check_should_skip(&boundary, &view(&pulses), &required, &wired, &literal_filled).is_none(),
            "an In boundary starts its scope whatever its inputs did"
        );
        let plain = node_all_optional();
        assert!(
            check_should_skip(&plain, &view(&pulses), &required, &wired, &literal_filled).is_some(),
            "the same closures skip a plain node"
        );
    }

    #[test]
    fn all_optional_one_value_fires() {
        let node = node_all_optional();
        let pulses = vec![data_pulse("a", json!(7)), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let literal_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &literal_filled).is_none(),
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
        let literal_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &literal_filled).is_some(),
            "closed wire + unwired unconfigured port must skip"
        );
    }

    #[test]
    fn all_optional_closed_wire_plus_literal_filled_fires() {
        // Same shape but `b` is config-filled: the body has a value
        // to act on, so it fires.
        let node = node_all_optional();
        let pulses = vec![closure_pulse("a")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let literal_filled: HashSet<&str> = ["b"].into_iter().collect();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &literal_filled).is_none(),
            "a config-filled port keeps the node alive"
        );
    }

    #[test]
    fn all_optional_one_user_null_fires() {
        let node = node_all_optional();
        let pulses = vec![data_pulse("a", json!(null)), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let literal_filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &literal_filled).is_none(),
            "user-emitted null is data; one null + one closed must NOT skip"
        );
    }
}
