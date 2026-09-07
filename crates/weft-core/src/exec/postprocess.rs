//! Output postprocessing. After a node returns from `execute`, the
//! runtime calls this to emit pulses on each outgoing edge. No type
//! checking here: type enforcement is the consumer's input boundary's
//! job (`ready::check_input`).

use std::collections::HashSet;

use serde_json::Value;

use crate::error::{WeftError, WeftResult};
use crate::exec::emission::PulseEmission;
use crate::frames::LoopFrames;
use crate::project::{Edge, EdgeIndex, ProjectDefinition};
use crate::pulse::{Pulse, PulseTable};
use crate::Color;

/// Whether `edge` lands on a `Generator[T]` input port. Generator ports
/// deliberately step outside the one-pulse-per-key reasoning: their
/// consumer fires once and then PULLS, taking items in arrival order,
/// so successive pulses at the same key are never ambiguous to it. The
/// fan-in collision checks and the same-value dedup below therefore do
/// not apply to them (two equal items are two items), and a closure
/// (the stream's end) coexists with still-buffered items instead of
/// being dropped against them.
fn edge_targets_generator(project: &ProjectDefinition, edge: &Edge) -> bool {
    let handle = edge.target_handle.as_deref().unwrap_or("default");
    project
        .nodes
        .iter()
        .find(|n| n.id == edge.target)
        .and_then(|n| n.inputs.iter().find(|p| p.name == handle))
        .is_some_and(|p| p.port_type.as_generator().is_some())
}


/// Process a node's output: emit pulses ONLY for the output ports the
/// node mentioned in `output`. Ports the node didn't mention this call
/// don't get touched; if the node never mentions a port across all its
/// emissions, the engine emits a CLOSURE on that port at termination
/// (see `close_unmentioned_downstream`).
///
/// Returns the set of port names actually emitted on this call.
pub fn postprocess_output(
    node_id: &str,
    output: &Value,
    color: Color,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    emissions: &mut Vec<PulseEmission>,
) -> WeftResult<HashSet<String>> {
    let mut mentioned = HashSet::new();
    let Some(node) = project.nodes.iter().find(|n| n.id == node_id) else {
        return Err(WeftError::NodeExecution(format!(
            "node '{node_id}' is not in the project at postprocess time; the project was \
             mutated between dispatch and postprocess",
        )));
    };

    let Some(output_obj) = output.as_object() else {
        // The engine always hands postprocess an object (node bodies
        // build maps; the loop emitters construct maps). Anything else
        // is an engine bug; silently treating it as "emitted nothing"
        // would auto-close every declared port at termination.
        return Err(WeftError::NodeExecution(format!(
            "node '{node_id}' postprocess received a non-object output bag; engine bug",
        )));
    };

    let outgoing = edge_idx.get_outgoing(project, node_id);

    // What each wire DELIVERS, worked out BEFORE any pulse mutation. A
    // wire with a path reads its keys off the value right here, once
    // per wire, so five wires off one port are five independent
    // projections; a `?` key found absent makes that wire deliver a
    // closure. A value that breaks its declared contract (a required
    // key missing) fails the firing, and doing all of it up front means
    // a failing firing leaves no partial state behind: the caller drops
    // `emissions` on Err, so a pulse committed before a later failure
    // would have no journal row and replay would diverge.
    let mut deliveries: Vec<Delivery<'_>> = Vec::new();
    for (port_name, value) in output_obj {
        // An undeclared port means the value would silently vanish
        // (downstream never fires AND never receives a closure): fail
        // the firing instead. Node-author emissions are caught earlier
        // by the engine's declared-output check, so reaching this is
        // an engine-internal wiring bug.
        let Some(port) = node.outputs.iter().find(|p| &p.name == port_name) else {
            return Err(WeftError::NodeExecution(format!(
                "node '{node_id}' emitted on undeclared output port '{port_name}' \
                 (declared: {declared:?})",
                declared = node.outputs.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            )));
        };
        for edge in outgoing.iter().filter(|e| e.source_handle.as_deref() == Some(port_name.as_str())) {
            let delivered = if edge.path.is_empty() {
                Some(value.clone())
            } else {
                match crate::deref::project_value(value, &port.port_type, &edge.path) {
                    Ok(crate::deref::Projection::Value(v)) => Some(v),
                    Ok(crate::deref::Projection::Closed { .. }) => None,
                    Err(e) => {
                        return Err(WeftError::NodeExecution(format!(
                            "node '{node_id}' emitted on '{port_name}' a value the wire to \
                             '{}.{}' cannot read: {e}",
                            edge.target,
                            edge.target_handle.as_deref().unwrap_or("default"),
                        )));
                    }
                }
            };
            deliveries.push(Delivery { edge, port: port_name.as_str(), value: delivered });
        }
    }

    // Fan-in collisions, on what the wires deliver. A closure is never
    // a collision: a data pulse outranks it.
    for d in &deliveries {
        let Some(value) = &d.value else { continue };
        // Generator targets take repeated pulses by design; the
        // fan-in collision reasoning below is about single-shot
        // ports and does not apply (see `edge_targets_generator`).
        if edge_targets_generator(project, d.edge) {
            continue;
        }
        let target_handle = d.edge.target_handle.as_deref().unwrap_or("default");
        // Cross-firing collision: another in-flight data pulse at
        // the same fan-in key with a DIFFERENT value.
        let cross_firing_conflict = pulses.get(&d.edge.target).and_then(|ps| {
            ps.iter().find(|p| {
                p.status.is_pending()
                    && p.color == color
                    && &p.frames == frames
                    && p.target_port == target_handle
                    && !p.closed
                    && p.value != *value
            })
        });
        if cross_firing_conflict.is_some() {
            return Err(WeftError::NodeExecution(format!(
                "fan-in collision on '{target}.{target_port}' at frames {frames:?}: \
                 another upstream producer wired to the same input already emitted a \
                 DIFFERENT value. Rewire so only one source feeds this port.",
                target = d.edge.target,
                target_port = target_handle,
            )));
        }
        // Within-firing collision: a sibling output of the SAME
        // firing also wires to the same fan-in target with a
        // different value.
        let within_firing_conflict = deliveries.iter().any(|other| {
            other.port != d.port
                && other.edge.target == d.edge.target
                && other.edge.target_handle.as_deref().unwrap_or("default") == target_handle
                && other.value.as_ref().is_some_and(|v| v != value)
        });
        if within_firing_conflict {
            return Err(WeftError::NodeExecution(format!(
                "fan-in collision on '{target}.{target_port}' at frames {frames:?}: \
                 two output ports of node '{node_id}' are wired to the same input and \
                 emitted DIFFERENT values in one firing. Rewire so only one source \
                 feeds this port.",
                target = d.edge.target,
                target_port = target_handle,
            )));
        }
    }

    // Every port is declared (validated above); emit.
    for d in deliveries {
        mentioned.insert(d.port.to_string());
        match d.value {
            Some(value) => emit_value_on_edge(project, node_id, d.edge, d.port, value, color, frames, pulses, emissions),
            None => emit_closure_on_edge(project, node_id, d.edge, d.port, color, frames, None, pulses, emissions),
        }
    }
    Ok(mentioned)
}

/// One wire's share of an emission: the edge, the port it leaves from,
/// and what lands on the other end (`None` = a closure, from a `?` key
/// the path found absent).
struct Delivery<'a> {
    edge: &'a Edge,
    port: &'a str,
    value: Option<Value>,
}

/// Put a value on ONE edge. Fan-in policy:
/// - Same data already pending at this key: dedup silently
///   (same-source double-edge).
/// - Closure already pending: append the data pulse anyway; both stay
///   in the table. `resolve_port_value` prefers the non-closed pulse
///   at read time.
/// - Generator target: append unconditionally. Repeated pulses are the
///   port's contract, and two EQUAL items are two items (the dedup
///   would silently drop one).
#[allow(clippy::too_many_arguments)]
fn emit_value_on_edge(
    project: &ProjectDefinition,
    source_node: &str,
    edge: &Edge,
    port: &str,
    value: Value,
    color: Color,
    frames: &LoopFrames,
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    let target_handle = edge.target_handle.as_deref().unwrap_or("default");

    let already_present_same_value = !edge_targets_generator(project, edge)
        && pulses.get(&edge.target).map(|ps| {
            ps.iter().any(|p| {
                p.status.is_pending()
                    && p.color == color
                    && &p.frames == frames
                    && p.target_port == target_handle
                    && !p.closed
                    && p.value == value
            })
        }).unwrap_or(false);
    if already_present_same_value {
        return;
    }

    let pulse = Pulse::new(
        color,
        frames.clone(),
        edge.target.clone(),
        target_handle.to_string(),
        value,
    );
    emissions.push(PulseEmission {
        pulse: pulse.clone(),
        source_node: source_node.to_string(),
        source_port: port.to_string(),
    });
    pulses.entry(edge.target.clone()).or_default().push(pulse);
}

/// Emit a CLOSURE marker on every output port the node never
/// mentioned. Called when the node's firing ends (Completed, Failed,
/// Skipped, Cancelled) so downstream consumers learn no value is
/// coming for those ports. Generic over node type: this module never
/// inspects `node.node_type`. Callers that need to skip auto-close
/// (loop boundary nodes on per-iteration firings) do so at the call
/// site by NOT invoking this function. Keeping the language layer
/// free of node-name strings is the invariant.
///
/// GENERATOR output ports are swept whether or not they were
/// mentioned: on a generator, "mentioned" means "yielded at least
/// once", not "finished", and the closure IS the stream's end (the
/// consumer's pull reads it as "finished"). `failure` is the firing's
/// error when this sweep runs on a failure path: it rides the
/// generator ports' closures as a FAILED end, so the consumer's pull
/// surfaces the producer's error through `?` instead of reading a
/// clean finish over a truncated stream. Ordinary ports never carry
/// it: their consumers act on the closure itself (skip / missing
/// input).
pub fn close_unmentioned_downstream(
    node_id: &str,
    mentioned: &HashSet<String>,
    color: Color,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    emissions: &mut Vec<PulseEmission>,
    failure: Option<&str>,
) -> WeftResult<()> {
    let Some(node) = project.nodes.iter().find(|n| n.id == node_id) else {
        // Same impossible state as `postprocess_output`'s node lookup;
        // silently skipping would leave every downstream consumer
        // waiting on ports that will never close.
        return Err(WeftError::NodeExecution(format!(
            "node '{node_id}' is not in the project at closure-sweep time; the project \
             was mutated between dispatch and termination",
        )));
    };
    let outgoing = edge_idx.get_outgoing(project, node_id);
    for port in &node.outputs {
        let is_generator = port.port_type.as_generator().is_some();
        if mentioned.contains(&port.name) && !is_generator {
            continue;
        }
        let close_error = if is_generator { failure } else { None };
        emit_closure_on_outgoing(
            project, node_id, &port.name, color, frames, close_error, &outgoing, pulses,
            emissions,
        );
    }
    Ok(())
}

/// Emit a CLOSURE on ONE specific output port at (color, frames).
/// Shared primitive for the termination-time sweep
/// (`close_unmentioned_downstream`, port-by-port) and the mid-firing
/// `ctx.close_port` call.
pub fn emit_port_closure(
    node_id: &str,
    port_name: &str,
    color: Color,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    emissions: &mut Vec<PulseEmission>,
) -> WeftResult<()> {
    let declared = project
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .map(|n| n.outputs.iter().any(|p| p.name == port_name));
    match declared {
        Some(true) => {}
        // A closure on an undeclared port is a runtime wiring bug;
        // swallowing it leaves the (real) downstream consumers of the
        // intended port waiting forever with only a log line as trace.
        Some(false) => {
            return Err(WeftError::NodeExecution(format!(
                "close_port on undeclared output port '{port_name}' of node '{node_id}'",
            )));
        }
        None => {
            return Err(WeftError::NodeExecution(format!(
                "close_port: node '{node_id}' is not in the project",
            )));
        }
    }
    let outgoing = edge_idx.get_outgoing(project, node_id);
    emit_closure_on_outgoing(
        project, node_id, port_name, color, frames, None, &outgoing, pulses, emissions,
    );
    Ok(())
}

/// Emit a closure on every outgoing edge of `port_name`.
#[allow(clippy::too_many_arguments)]
fn emit_closure_on_outgoing(
    project: &ProjectDefinition,
    node_id: &str,
    port_name: &str,
    color: Color,
    frames: &LoopFrames,
    close_error: Option<&str>,
    outgoing: &[&Edge],
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    for edge in outgoing
        .iter()
        .filter(|e| e.source_handle.as_deref() == Some(port_name))
    {
        emit_closure_on_edge(project, node_id, edge, port_name, color, frames, close_error, pulses, emissions);
    }
}

/// Put a closure on ONE edge. On a NORMAL target port, a closure is
/// suppressed by anything already pending at the key (a data pulse
/// outranks it forever; a closure is single-shot). On a GENERATOR
/// target port, buffered items and the closure coexist (the items are
/// taken first, then the end); only a closure already pending dedups a
/// second one.
#[allow(clippy::too_many_arguments)]
fn emit_closure_on_edge(
    project: &ProjectDefinition,
    node_id: &str,
    edge: &Edge,
    port_name: &str,
    color: Color,
    frames: &LoopFrames,
    close_error: Option<&str>,
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    let target_handle = edge.target_handle.as_deref().unwrap_or("default");
    let generator_target = edge_targets_generator(project, edge);

    let already_present = pulses
        .get(&edge.target)
        .map(|ps| {
            ps.iter().any(|p| {
                p.status.is_pending()
                    && p.color == color
                    && &p.frames == frames
                    && p.target_port == target_handle
                    && (!generator_target || p.closed)
            })
        })
        .unwrap_or(false);
    if already_present {
        return;
    }

    let pulse = Pulse::closure_with_error(
        color,
        frames.clone(),
        edge.target.clone(),
        target_handle.to_string(),
        close_error.filter(|_| generator_target).map(str::to_string),
    );
    emissions.push(PulseEmission {
        pulse: pulse.clone(),
        source_node: node_id.to_string(),
        source_port: port_name.to_string(),
    });
    pulses.entry(edge.target.clone()).or_default().push(pulse);
}

#[cfg(test)]
mod fan_in_tests {
    use super::*;
    use crate::project::Edge;
    use serde_json::json;

    /// Minimal project for the emit-level tests: a `consumer` with a
    /// plain `in` input (the emit helpers only inspect the TARGET
    /// port's type; the provenance strings the tests pass as source
    /// node ids never resolve).
    fn direct_project() -> ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("src", vec!["out"], vec![]), node("consumer", vec![], vec!["in"])],
            "edges": [],
            "groups": [],
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }))
        .expect("test project")
    }

    fn edge(source_handle: &str, target: &str, target_handle: &str) -> Edge {
        Edge {
            id: format!("{source_handle}-{target}-{target_handle}"),
            source: "src".into(),
            source_handle: Some(source_handle.into()),
            target: target.into(),
            target_handle: Some(target_handle.into()),
            path: Vec::new(),
            span: None,
            source_file: None,
        }
    }

    #[test]
    fn same_value_fan_in_dedups_silently() {
        let outgoing = [edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let color = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        let project = direct_project();
        emit_value_on_edge(&project, "src1", outgoing_refs[0], "out", json!(42), color, &frames, &mut pulses, &mut emissions);
        emit_value_on_edge(&project, "src2", outgoing_refs[0], "out", json!(42), color, &frames, &mut pulses, &mut emissions);

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<_> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 1, "exactly one pending pulse after dedup");
    }

    #[test]
    fn data_and_existing_closure_coexist_and_data_wins_at_resolve_time() {
        let outgoing = [edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let color = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        let project = direct_project();
        emit_closure_on_outgoing(
            &project, "src1", "out", color, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
        );
        emit_value_on_edge(
            &project, "src2", outgoing_refs[0], "out", json!(42), color, &frames, &mut pulses, &mut emissions,
        );

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<_> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 2, "both pulses pending in the table");
        let winner = crate::exec::ready::resolve_port_value(consumer_bucket, color, &frames, "in")
            .expect("a winner");
        assert_eq!(winner.value, json!(42), "resolve picks data over closure");
        assert!(!winner.closed);
    }

    #[test]
    fn closure_skips_when_data_already_pending() {
        let outgoing = [edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let color = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        let project = direct_project();
        emit_value_on_edge(
            &project, "src1", outgoing_refs[0], "out", json!(42), color, &frames, &mut pulses, &mut emissions,
        );
        let emissions_before = emissions.len();
        emit_closure_on_outgoing(
            &project, "src2", "out", color, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
        );
        assert_eq!(
            emissions.len(),
            emissions_before,
            "closure emission is a no-op when data is already pending; nothing journaled"
        );

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<_> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 1, "only the data pulse remains pending");
        assert_eq!(pending[0].value, json!(42));
        assert!(!pending[0].closed);
    }

    fn port(name: &str) -> crate::project::PortDefinition {
        crate::project::PortDefinition {
            name: name.into(),
            port_type: crate::weft_type::WeftType::primitive(crate::weft_type::WeftPrimitive::String),
            required: false,
            description: None,
            synthesized_from_carry: false,
            declared_type: None,
        }
    }

    fn node(id: &str, outputs: Vec<&str>, inputs: Vec<&str>) -> serde_json::Value {
        let n = crate::project::NodeDefinition {
            id: id.into(),
            node_type: "Test".into(),
            label: None,
            config: Value::Null,
            position: crate::project::Position { x: 0.0, y: 0.0 },
            inputs: inputs
                .into_iter()
                .map(|n| crate::project::InputDefinition::from_wire_port(port(n)))
                .collect(),
            outputs: outputs.into_iter().map(port).collect(),
            features: Default::default(),
            scope: Vec::new(),
            group_boundary: None,
            requires_infra: false,
            images: Vec::new(),
            published_service: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
            source_file: None,
        };
        serde_json::to_value(n).unwrap()
    }

    /// `src` with outputs out1/out2; `consumer.in` fed by the given
    /// (source_handle) edges.
    fn project_with_edges(handles: &[&str]) -> ProjectDefinition {
        let edges: Vec<serde_json::Value> = handles
            .iter()
            .map(|h| {
                serde_json::json!({
                    "id": format!("e-{h}"),
                    "source": "src",
                    "sourceHandle": h,
                    "target": "consumer",
                    "targetHandle": "in",
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("src", vec!["out1", "out2"], vec![]), node("consumer", vec![], vec!["in"])],
            "edges": edges,
            "groups": [],
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }))
        .expect("test project")
    }

    fn run_postprocess(
        project: &ProjectDefinition,
        output: serde_json::Value,
        pulses: &mut PulseTable,
        emissions: &mut Vec<PulseEmission>,
    ) -> WeftResult<HashSet<String>> {
        let edge_idx = EdgeIndex::build(project);
        postprocess_output(
            "src", &output, uuid::Uuid::nil(), &Vec::new(), project, pulses, &edge_idx, emissions,
        )
    }

    /// Cross-firing collision: a pending data pulse with a DIFFERENT
    /// value already sits at the fan-in key. The second firing must
    /// fail atomically: no pulse committed, no emission journaled.
    #[test]
    fn cross_firing_different_value_errors_with_no_partial_state() {
        let project = project_with_edges(&["out1"]);
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        run_postprocess(&project, json!({"out1": "A"}), &mut pulses, &mut emissions)
            .expect("first emit");
        let pulses_before = pulses.get("consumer").map(|b| b.len()).unwrap_or(0);
        let emissions_before = emissions.len();

        let err = run_postprocess(&project, json!({"out1": "B"}), &mut pulses, &mut emissions)
            .expect_err("conflicting value must error");
        assert!(
            err.to_string().contains("another upstream producer"),
            "names the cross-firing case: {err}"
        );
        assert_eq!(pulses.get("consumer").map(|b| b.len()).unwrap_or(0), pulses_before);
        assert_eq!(emissions.len(), emissions_before, "no partial journal mutations");
    }

    /// Within-firing collision: two sibling output ports of ONE firing
    /// wire to the same input with different values. Atomic failure.
    #[test]
    fn within_firing_sibling_collision_errors_with_no_partial_state() {
        let project = project_with_edges(&["out1", "out2"]);
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let err = run_postprocess(
            &project,
            json!({"out1": "A", "out2": "B"}),
            &mut pulses,
            &mut emissions,
        )
        .expect_err("sibling collision must error");
        assert!(
            err.to_string().contains("two output ports"),
            "names the within-firing case: {err}"
        );
        assert!(pulses.get("consumer").map(|b| b.is_empty()).unwrap_or(true));
        assert!(emissions.is_empty(), "no partial journal mutations");
    }

    /// Same value through both siblings is the sanctioned fan-in shape
    /// (dedup, one pulse).
    #[test]
    fn within_firing_same_value_dedups() {
        let project = project_with_edges(&["out1", "out2"]);
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        run_postprocess(
            &project,
            json!({"out1": "A", "out2": "A"}),
            &mut pulses,
            &mut emissions,
        )
        .expect("same value is fine");
        let pending = pulses.get("consumer").map(|b| b.len()).unwrap_or(0);
        assert_eq!(pending, 1, "deduped to one pulse");
    }

    /// An undeclared output port fails the firing loudly and commits
    /// nothing: silently dropping the value would make downstream
    /// neither fire nor skip, stalling the execution invisibly.
    #[test]
    fn undeclared_output_port_errors_with_no_partial_state() {
        let project = project_with_edges(&["out1"]);
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let err = run_postprocess(
            &project,
            json!({"out1": "A", "nope": "B"}),
            &mut pulses,
            &mut emissions,
        )
        .expect_err("undeclared port must error");
        assert!(
            err.to_string().contains("undeclared output port 'nope'"),
            "names the port: {err}"
        );
        assert!(pulses.get("consumer").map(|b| b.is_empty()).unwrap_or(true));
        assert!(emissions.is_empty(), "no partial journal mutations");
    }

    #[test]
    fn closure_dedups_against_closure() {
        let outgoing = [edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let color = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        let project = direct_project();
        emit_closure_on_outgoing(
            &project, "src1", "out", color, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
        );
        emit_closure_on_outgoing(
            &project, "src2", "out", color, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
        );

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<_> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 1, "closure dedups silently against existing closure");
        assert!(pending[0].closed);
    }

    /// A record-typed source with two dereferencing wires and one
    /// plain one. Ports are typed for the projection to read.
    fn deref_project() -> ProjectDefinition {
        let ty = "{ profile: { wpm: Number, name?: String }, id: String }";
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                { "id": "src", "nodeType": "Test", "label": null, "config": null,
                  "position": { "x": 0.0, "y": 0.0 }, "inputs": [],
                  "outputs": [{ "name": "out", "portType": ty, "required": true }],
                  "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                { "id": "speed", "nodeType": "Test", "label": null, "config": null,
                  "position": { "x": 0.0, "y": 0.0 },
                  "inputs": [{ "name": "in", "portType": "Number", "required": true }],
                  "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                { "id": "who", "nodeType": "Test", "label": null, "config": null,
                  "position": { "x": 0.0, "y": 0.0 },
                  "inputs": [{ "name": "in", "portType": "String", "required": false }],
                  "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                { "id": "whole", "nodeType": "Test", "label": null, "config": null,
                  "position": { "x": 0.0, "y": 0.0 },
                  "inputs": [{ "name": "in", "portType": ty, "required": true }],
                  "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }
            ],
            "edges": [
                { "id": "e1", "source": "src", "sourceHandle": "out", "target": "speed", "targetHandle": "in",
                  "path": ["profile", "wpm"] },
                { "id": "e2", "source": "src", "sourceHandle": "out", "target": "who", "targetHandle": "in",
                  "path": ["profile", "name"] },
                { "id": "e3", "source": "src", "sourceHandle": "out", "target": "whole", "targetHandle": "in" }
            ],
            "groups": [],
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }))
        .expect("deref project")
    }

    fn pending_on<'a>(pulses: &'a PulseTable, node: &str) -> Vec<&'a Pulse> {
        pulses.get(node).map(|b| b.iter().filter(|p| p.status.is_pending()).collect()).unwrap_or_default()
    }

    /// Each wire projects on its own: the dereferencing wires deliver
    /// the key's value, the plain wire the whole record, and an absent
    /// `?` key closes only its own wire.
    #[test]
    fn a_wire_with_a_path_delivers_the_key_and_an_absent_optional_key_closes_it() {
        let project = deref_project();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let value = json!({ "profile": { "wpm": 42 }, "id": "u1" });
        run_postprocess(&project, json!({ "out": value.clone() }), &mut pulses, &mut emissions)
            .expect("emit");

        let speed = pending_on(&pulses, "speed");
        assert_eq!(speed.len(), 1);
        assert_eq!(speed[0].value, json!(42));
        assert!(!speed[0].closed);

        let who = pending_on(&pulses, "who");
        assert_eq!(who.len(), 1);
        assert!(who[0].closed, "the absent `name?` closes that wire alone");

        let whole = pending_on(&pulses, "whole");
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0].value, value, "the plain wire is untouched");
        // Every delivery is journaled with what it delivered.
        assert_eq!(emissions.len(), 3);
    }

    /// A required key missing is a value that broke its declared type:
    /// the firing fails, and nothing is committed.
    #[test]
    fn a_missing_required_key_fails_the_firing_atomically() {
        let project = deref_project();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let err = run_postprocess(
            &project,
            json!({ "out": { "profile": { "name": "x" }, "id": "u1" } }),
            &mut pulses,
            &mut emissions,
        )
        .expect_err("a required key missing must fail");
        assert!(err.to_string().contains("no 'wpm'"), "{err}");
        assert!(pulses.values().all(|b| b.is_empty()), "no pulse committed");
        assert!(emissions.is_empty(), "nothing journaled");
    }
}
