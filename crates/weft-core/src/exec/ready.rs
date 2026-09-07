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

use crate::exec::skip::{check_should_skip, SkipReason};
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
    /// Set when this firing must NOT run its body, and why. `None`
    /// means run it.
    pub skip: Option<SkipReason>,
    pub pulse_ids: Vec<uuid::Uuid>,
    pub error: Option<String>,
    /// This node is outside the part of the graph the execution runs
    /// (a setup phase's closure, a trigger fire's program): the pulses
    /// that reached it are absorbed silently, nothing is journaled, and
    /// the body never runs. Never set on a kicked node.
    pub out_of_scope: bool,
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


/// `dispatchable`, when set, is the only part of the graph this
/// execution may dispatch (a setup phase's closure, or a manual run's
/// subgraph). A node outside it is READY THE MOMENT ANY PULSE LANDS on
/// it, as a skip: waiting for its full input set would park the pulse
/// forever when one of its other parents is itself outside the set and
/// never fires (an unkicked entry node feeding a side branch). (Named
/// `dispatchable`, not `scope`: `NodeDefinition.scope` in this file is
/// a node's group-nesting path, a different thing entirely.)
pub fn find_ready_nodes(
    project: &ProjectDefinition,
    pulses: &PulseTable,
    edge_idx: &EdgeIndex,
    dispatchable: Option<&HashSet<String>>,
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
        let out_of_scope = dispatchable.is_some_and(|s| !s.contains(&node.id));

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

        let mut literal_filled: HashSet<&str> = HashSet::new();
        for (name, value) in &node.port_literals {
            if !wired.contains(name.as_str()) && literal_is_data(node, name, value) {
                literal_filled.insert(name.as_str());
            }
        }

        let groups = find_groups_for_node(
            node, node_pulses, &required, &wired, &literal_filled, has_incoming, out_of_scope,
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
    literal_filled: &HashSet<&str>,
    has_incoming: bool,
    out_of_scope: bool,
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

        // An out-of-scope node never waits for its full input set: it
        // will not run, so any pulse that lands is absorbed by a skip
        // dispatch right away (see `find_ready_nodes`'s doc).
        if has_incoming && !all_satisfied && !out_of_scope {
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
        // An entry node (no incoming edges) has nothing wired, so the
        // closure rules cannot apply; but `_should_flow: false` in its
        // braces still turns it off, so rule 0 runs on its own there.
        // An out-of-scope node never runs at all: no skip reason, the
        // group is absorbed silently by the driver.
        let skip = if out_of_scope || is_out_boundary || !has_incoming {
            // Pulses only ever ride edges, so a node with no incoming
            // edges cannot form a group here; an entry node's
            // `_should_flow` is decided where its kick is synthesized
            // (the engine's kick path runs `check_flow_permission`).
            None
        } else {
            check_should_skip(node, node_pulses, frames, *color, required, wired, literal_filled)
        };

        let pulse_ids: Vec<uuid::Uuid> = group_pulses.iter().map(|p| p.id).collect();

        // A generator port never lists as closed: its closure is the
        // stream's END (an empty stream when no items preceded it), a
        // value the firing consumes through its live feed, not a
        // structural "nothing arrived" (see the skip module doc).
        let mut closed_ports: Vec<String> = wired
            .iter()
            .filter(|port_name| {
                let is_generator = node
                    .inputs
                    .iter()
                    .any(|p| p.name == **port_name && p.port_type.as_generator().is_some());
                !is_generator
                    && resolve_port_value(node_pulses, *color, frames, port_name)
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
            skip,
            pulse_ids,
            error: if type_errors.is_empty() { None } else { Some(type_errors.join("; ")) },
            out_of_scope,
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

    fill_input_from_literals(node, wired, &mut obj);

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

/// Why the port's widget refuses a value its type accepts (a number
/// outside its range or off its step). One rule, [`Widget::check_value`],
/// shared with the compiler: the compiler refuses the written constant
/// at build time and this refuses the wired or replayed one at run
/// time, both loudly, so the same number is never legal on one path and
/// quietly replaced on the other.
fn widget_refusal(port: &crate::project::InputDefinition, value: &Value) -> Option<String> {
    port.widget
        .as_ref()?
        .check_value(value)
        .err()
        .map(|why| format!("'{}': {why}", port.name))
}

/// Outcome of checking one incoming value against an input port type.
#[derive(Debug, PartialEq, Eq)]
enum InputCheck {
    Ok,
    NullIt,
    Fail(String),
}

/// Insert each UNWIRED input port's body-supplied literal
/// (`node.port_literals`, populated by the enrich normalization from
/// braces values and assignment statements, per the port's literal
/// placement) into `obj`. Wires are authoritative: a wired port whose pulse
/// resolved to a closure stays absent (the closure means upstream
/// produced nothing; silently substituting the literal would mask the
/// upstream failure AND contradict the skip layer, whose
/// `literal_filled` set deliberately excludes wired ports). Shared
/// between the pulse-driven dispatch path (`build_input`) and the
/// kick-driven dispatch path (`build_kicked_input` below) so the two
/// paths can't disagree on what counts as "body-supplied".
pub fn fill_input_from_literals(
    node: &NodeDefinition,
    wired: &HashSet<&str>,
    obj: &mut Map<String, Value>,
) {
    for (name, value) in &node.port_literals {
        if wired.contains(name.as_str()) || obj.contains_key(name) || !literal_is_data(node, name, value) {
            continue;
        }
        obj.insert(name.clone(), value.clone());
    }
}

/// Does a written constant carry a value for the port? A `null` is data
/// only on a port whose type admits Null (`String | Null`): there it
/// fills the port like any value. Anywhere else a written `null` is the
/// absence of a value, which leaves the port to its default or unmet.
/// The compiler's `required-port-unmet` / `@require_one_of` rules read
/// the same line, so a source that compiles is a source that fills.
pub fn literal_is_data(node: &NodeDefinition, port: &str, value: &Value) -> bool {
    !value.is_null() || port_admits_null(node, port)
}

/// Whether the port's declared type admits Null as a value.
pub fn port_admits_null(node: &NodeDefinition, port: &str) -> bool {
    node.inputs
        .iter()
        .find(|p| p.name == port)
        .is_some_and(|p| p.port_type.port_value_type().contains_null())
}

/// Build the input-port values for a node firing from a KICK (entry node /
/// trigger payload), not from upstream pulses, with whatever the ports
/// refuse. Starts empty and fills only from the node's body-supplied
/// port literals.
/// This is what makes a `Range { from: 0, to: 10, step: 2 }` orphan
/// see its body values at runtime.
///
/// Wake payloads from trigger kicks ride a separate channel (the
/// `ctx.wake` bag) that the engine wires up at dispatch time, so they
/// don't need to be merged here.
pub fn build_kicked_input(
    node: &NodeDefinition,
    port_snapshot: Option<&Value>,
) -> (Value, Vec<String>) {
    // A firing trigger's ports replay the setup-time snapshot: seed the
    // bag from it first (only keys naming declared input ports; the
    // snapshot is runtime-written so extras would be a writer bug, and
    // dropping them keeps the port contract the single source of shape).
    let mut obj = Map::new();
    if let Some(snapshot) = port_snapshot.and_then(Value::as_object) {
        for (k, v) in snapshot {
            if node.inputs.iter().any(|p| p.name == *k) {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    // Kicked nodes are entry points: no wired pending inputs by
    // definition, so the wired set is empty. Literals skip keys the
    // snapshot already filled.
    fill_input_from_literals(node, &HashSet::new(), &mut obj);
    // The same gate the pulse path runs (`check_input`). A trigger's
    // ports are exactly where a widget's domain rule earns its keep (a
    // poll interval of zero, a fractional one that would truncate), and
    // skipping the check here meant those rules bound nothing on the
    // one path that uses them.
    let mut errors = Vec::new();
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
                errors.push(err);
                obj.insert(port.name.clone(), Value::Null);
            }
        }
    }
    (Value::Object(obj), errors)
}

/// Check one incoming value against its input port type. THE single
/// place input type enforcement lives.
fn check_input(port: &crate::project::InputDefinition, value: &Value) -> InputCheck {
    // A generator port's pulses carry ITEMS: each value is checked
    // against the ELEMENT type, never against `Generator[T]` itself
    // (the whole-port handle only exists in the consumer's bag, built
    // by the engine after this gate).
    // A connection picker and a resource picker both hold what the
    // EDITOR stored (`{id, identity}` for a connection, `{id, label}`
    // for a pick) until the bag builder rewrites it into the value the
    // port's type describes: an access marker carrying the widget's
    // service, or the bare id string. So the stored handle is what such
    // a port legally carries at this point, and its shape is held to
    // `Widget::check_handle_shape` where the rewrite happens, by the
    // same rule the compiler applies to the written value. Judging it
    // against the port's type here refused every connection a program
    // picks in its own source.
    if port.widget.as_ref().is_some_and(|w| {
        matches!(w, crate::node::Widget::Access { .. } | crate::node::Widget::RemoteSelect { .. })
    }) && value.is_object()
    {
        return InputCheck::Ok;
    }
    let declared = port.port_type.port_value_type();
    if declared.is_unresolved() || declared.accepts_runtime_value(value) {
        // The type fits. What is left is the widget's declared domain (a
        // number's range and step), and a value outside it FAILS whether
        // the port is required or not. Nulling it instead would hand the
        // node the port's default in place of the number the author
        // wrote, which is the silent substitution this check exists to
        // stop: a poll interval of 0 would quietly become 30.
        return match widget_refusal(port, value) {
            Some(why) => InputCheck::Fail(why),
            None => InputCheck::Ok,
        };
    }
    // The type does not fit. A null is data only where the type admits
    // it (accepted above); on an optional port a null means "nothing
    // arrived" and the bag fills the default.
    if value.is_null() && !port.required {
        return InputCheck::Ok;
    }
    // A wrong-typed value on an optional port is upstream sending
    // something this port cannot hold: the port degrades to nothing
    // arrived and the node runs. On a required port there is nothing
    // to run with.
    if !port.required {
        return InputCheck::NullIt;
    }
    InputCheck::Fail(format!(
        "type mismatch on '{}': expected {}, got {}",
        port.name,
        declared,
        WeftType::infer(value)
    ))
}

#[cfg(test)]
mod tests {
    use super::{build_kicked_input, check_input, resolve_port_value, InputCheck};
    use crate::frames::LoopIteration;
    use crate::project::{InputDefinition, NodeDefinition, Position};
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

    fn port(ty: &str, required: bool) -> InputDefinition {
        serde_json::from_value(json!({
            "name": "p", "portType": ty, "required": required
        }))
        .expect("port")
    }

    #[test]
    fn named_type_accepts_fitting_object_and_refuses_misfit() {
        // A declared shape validates structurally (inference can never
        // produce a nominal name, so an infer-and-compare gate would
        // refuse every legitimate value). Regression: the readiness
        // gate once had its own infer-based check beside
        // `accepts_runtime_value` and failed every Named input.
        let p = port("Profile={ name: String, age: Number, nickname?: String }", true);
        assert_eq!(check_input(&p, &json!({"name": "Ada", "age": 36})), InputCheck::Ok);
        match check_input(&p, &json!({"name": "Ada"})) {
            InputCheck::Fail(msg) => assert!(msg.contains("Profile"), "names the type: {msg}"),
            other => panic!("missing required field must fail, got {other:?}"),
        }
    }

    #[test]
    fn matching_value_is_ok_regardless_of_required() {
        assert_eq!(check_input(&port("String", true), &json!("ok")), InputCheck::Ok);
        assert_eq!(check_input(&port("String", false), &json!("ok")), InputCheck::Ok);
    }

    #[test]
    fn null_is_data_only_where_the_type_admits_it() {
        assert_eq!(check_input(&port("String | Null", true), &json!(null)), InputCheck::Ok);
        assert_eq!(check_input(&port("String", false), &json!(null)), InputCheck::Ok, "optional: absent");
        match check_input(&port("String", true), &json!(null)) {
            InputCheck::Fail(msg) => assert!(msg.contains("expected String, got Null"), "{msg}"),
            other => panic!("a required String has nothing to run with on null, got {other:?}"),
        }
    }

    #[test]
    fn a_written_null_fills_a_nullable_port_and_no_other() {
        let inputs = vec![
            serde_json::from_value(json!({ "name": "maybe", "portType": "String | Null", "required": true })).unwrap(),
            serde_json::from_value(json!({ "name": "plain", "portType": "String", "required": false })).unwrap(),
        ];
        let node = kicked_node("X", inputs, json!({ "maybe": null, "plain": null }));
        let (input, _) = build_kicked_input(&node, None);
        assert_eq!(input.get("maybe"), Some(&json!(null)), "null is data on a nullable port");
        assert!(input.get("plain").is_none(), "null on a plain port is no value");
    }

    #[test]
    fn a_widget_binds_a_wired_value_like_a_written_one() {
        let count: InputDefinition = serde_json::from_value(json!({
            "name": "count", "portType": "Number", "required": true,
            "widget": { "kind": "number", "min": 1, "max": 8, "step": 1 }
        })).unwrap();
        assert_eq!(check_input(&count, &json!(3)), InputCheck::Ok);
        for bad in [json!(0), json!(9), json!(2.5)] {
            match check_input(&count, &bad) {
                InputCheck::Fail(msg) => assert!(msg.contains("'count'"), "{msg}"),
                other => panic!("{bad} must be refused by the widget, got {other:?}"),
            }
        }
        // A widget's OPTION LIST is what the editor offers, not a
        // domain rule: a node's code routinely takes more than the list
        // names (any HTTP method, a model id shipped after the list was
        // written), so a wired value outside it runs and the node's own
        // code decides.
        let mode: InputDefinition = serde_json::from_value(json!({
            "name": "mode", "portType": "String", "required": false,
            "widget": { "kind": "select", "options": ["added", "removed", "both"] }
        })).unwrap();
        assert_eq!(check_input(&mode, &json!("both")), InputCheck::Ok);
        assert_eq!(check_input(&mode, &json!("sideways")), InputCheck::Ok);
    }

    /// A whole-number step means the input takes whole numbers. A
    /// fractional one is the arrow key's increment and holds a typed
    /// value to nothing: a temperature box stepping by 0.1 takes 0.85.
    #[test]
    fn a_step_asks_for_a_whole_number_only_when_it_is_one() {
        let every_two: InputDefinition = serde_json::from_value(json!({
            "name": "n", "portType": "Number", "required": true,
            "widget": { "kind": "number", "min": 1, "step": 2 }
        })).unwrap();
        assert_eq!(check_input(&every_two, &json!(3)), InputCheck::Ok);
        assert_eq!(check_input(&every_two, &json!(4)), InputCheck::Ok);
        assert!(matches!(check_input(&every_two, &json!(2.5)), InputCheck::Fail(_)));
        let tenths: InputDefinition = serde_json::from_value(json!({
            "name": "temperature", "portType": "Number", "required": false,
            "widget": { "kind": "number", "min": 0, "max": 2, "step": 0.1 }
        })).unwrap();
        for fine in [json!(0.85), json!(1.0), json!(0.07)] {
            assert_eq!(check_input(&tenths, &fine), InputCheck::Ok, "{fine}");
        }
        assert!(matches!(check_input(&tenths, &json!(2.5)), InputCheck::Fail(_)), "the range still binds");
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

    /// A required nullable port used to swallow a wrong-typed value as
    /// a null, so the node ran and could not tell "never arrived" from
    /// "arrived wrong". Only an OPTIONAL port drops a value.
    #[test]
    fn a_wrong_type_on_a_required_nullable_port_fails() {
        match check_input(&port("String | Null", true), &json!(42)) {
            InputCheck::Fail(msg) => assert!(msg.contains("type mismatch"), "{msg}"),
            other => panic!("expected Fail, got {other:?}"),
        }
        // A null itself is still data there.
        assert_eq!(check_input(&port("String | Null", true), &json!(null)), InputCheck::Ok);
    }

    /// A connection picked in the source is a stored handle until the
    /// bag builder stamps it into an access value. The gate here must
    /// let it through: judging it against `Access` refused every node
    /// with a connection written in its braces, which is all of them.
    #[test]
    fn a_picked_connection_passes_the_gate_as_the_handle_it_is() {
        let account: InputDefinition = serde_json::from_value(json!({
            "name": "account", "portType": "Access", "required": false,
            "widget": { "kind": "access", "service": "slack" }
        })).unwrap();
        let handle = json!({ "id": "11111111-1111-1111-1111-111111111111", "identity": "someone" });
        assert_eq!(check_input(&account, &handle), InputCheck::Ok);
        // A resource pick is stored the same way, on a port typed for
        // the id the node ends up reading.
        let sheet: InputDefinition = serde_json::from_value(json!({
            "name": "sheet", "portType": "String", "required": true,
            "widget": { "kind": "remote_select", "access": "account", "sources": [] }
        })).unwrap();
        assert_eq!(
            check_input(&sheet, &json!({ "id": "1AbC", "label": "Budget" })),
            InputCheck::Ok
        );
        // A pasted raw id is the port's own type and still checked.
        assert_eq!(check_input(&sheet, &json!("1AbC")), InputCheck::Ok);
        assert!(matches!(check_input(&sheet, &json!(7)), InputCheck::Fail(_)));
        let node = kicked_node("SlackAccess", vec![account], json!({ "account": handle.clone() }));
        let (input, refusals) = build_kicked_input(&node, None);
        assert!(refusals.is_empty(), "{refusals:?}");
        assert_eq!(input.get("account"), Some(&handle), "the handle reaches the bag intact");
    }

    /// A trigger's own ports are held to the same line as a pulsed
    /// node's. This is the path a poll interval actually arrives on, so
    /// leaving it unchecked meant the interval's rules bound nothing.
    #[test]
    fn a_kicked_node_is_held_to_its_ports_like_a_pulsed_one() {
        // Optional, as every interval port in the catalog is: a widget
        // refusal must fail there too, or the default silently replaces
        // the number the author wrote.
        let interval: InputDefinition = serde_json::from_value(json!({
            "name": "intervalSecs", "portType": "Number", "required": false,
            "widget": { "kind": "number", "min": 1, "step": 1 }
        })).unwrap();
        let node = kicked_node("Poll", vec![interval], json!({ "intervalSecs": 1.5 }));
        let (input, refusals) = build_kicked_input(&node, None);
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(refusals[0].contains("intervalSecs"), "{refusals:?}");
        assert_eq!(input.get("intervalSecs"), Some(&json!(null)));
    }

    fn kicked_node(node_type: &str, inputs: Vec<InputDefinition>, literals: serde_json::Value) -> NodeDefinition {
        NodeDefinition {
            id: "k".into(),
            node_type: node_type.into(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            inputs,
            outputs: Vec::new(),
            features: NodeFeatures::default(),
            scope: Vec::new(),
            group_boundary: None,
            requires_infra: false,
            images: Vec::new(),
            published_service: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: literals
                .as_object()
                .expect("literal fixture is an object")
                .clone()
                .into_iter()
                .collect(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
            source_file: None,
        }
    }

    /// Regression: a kicked orphan node (entry node with no incoming
    /// edges) used to receive an empty input bag, so a body-settable
    /// port like `Range.to` set via source config (`Range { to: 10 }`)
    /// arrived at runtime as `missing input on port: to`. The kick
    /// path now flows through `build_kicked_input` which fills
    /// ports from the enrich-normalized `node.port_literals`.
    #[test]
    fn build_kicked_input_fills_ports_from_literals() {
        let inputs = vec![
            serde_json::from_value(json!({
                "name": "from", "portType": "Number",
                "required": false,
            })).unwrap(),
            serde_json::from_value(json!({
                "name": "to", "portType": "Number",
                "required": true,
            })).unwrap(),
            serde_json::from_value(json!({
                "name": "step", "portType": "Number",
                "required": false,
            })).unwrap(),
        ];
        let node = kicked_node("Range", inputs, json!({ "from": 0, "to": 10, "step": 2 }));
        let (input, _) = build_kicked_input(&node, None);
        assert_eq!(input, json!({ "from": 0, "to": 10, "step": 2 }));
    }

    /// A firing trigger's setup-time snapshot seeds its ports: declared
    /// ports come through, a stray snapshot key (writer bug) is dropped,
    /// and a body literal fills only what the snapshot left unset.
    #[test]
    fn build_kicked_input_seeds_the_port_snapshot() {
        let inputs = vec![
            serde_json::from_value(json!({
                "name": "endpointUrl", "portType": "String",
                "required": true,
            })).unwrap(),
            serde_json::from_value(json!({
                "name": "mode", "portType": "String",
                "required": false,
            })).unwrap(),
        ];
        let node = kicked_node("Recv", inputs, json!({ "mode": "media" }));
        let snapshot = json!({ "endpointUrl": "http://bridge", "ghost": 1 });
        let (input, _) = build_kicked_input(&node, Some(&snapshot));
        assert_eq!(input, json!({ "endpointUrl": "http://bridge", "mode": "media" }));
    }

    /// Wires are authoritative: a WIRED port whose pulse resolved to a
    /// closure (so it is absent from the bag) must NOT be silently
    /// backfilled from its body literal. The closure means upstream
    /// produced nothing; substituting the literal would mask the
    /// upstream failure and contradict the skip layer (whose
    /// `literal_filled` set excludes wired ports).
    #[test]
    fn fill_input_from_literals_skips_wired_ports() {
        let inputs = vec![
            serde_json::from_value(json!({
                "name": "wired_p", "portType": "Number",
                "required": false,
            })).unwrap(),
            serde_json::from_value(json!({
                "name": "free_p", "portType": "Number",
                "required": false,
            })).unwrap(),
        ];
        let node = kicked_node("X", inputs, json!({ "wired_p": 1, "free_p": 2 }));
        let wired: std::collections::HashSet<&str> = ["wired_p"].into_iter().collect();
        let mut obj = serde_json::Map::new();
        super::fill_input_from_literals(&node, &wired, &mut obj);
        assert_eq!(
            serde_json::Value::Object(obj),
            json!({ "free_p": 2 }),
            "wired port stays absent; unwired port fills from its literal"
        );
    }
}
