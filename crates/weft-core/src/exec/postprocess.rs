//! Output postprocessing. After a node returns from `execute`, the
//! runtime calls this to emit pulses on each outgoing edge. No type
//! checking here: type enforcement is the consumer's input boundary's
//! job (`ready::check_input`).
//!
//! Every function here takes the EMISSION id of the act it serves
//! (see `exec::emission`): pulse ids derive from it, so the journal
//! fold, replaying the same emission over the same program, puts the
//! same pulses in the table. Values are shared (`Arc`), never copied
//! per wire; a wire with a key path gets its own projected value.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use crate::error::{WeftError, WeftResult};
use crate::exec::emission::{pulse_id, PulseEmission};
use crate::frames::LoopFrames;
use crate::project::{Edge, EdgeIndex, ProjectDefinition};
use crate::pulse::{Pulse, PulseTable};
use crate::Color;


/// What a firing hands out on its ports: one shared value per port. A
/// node body's emission wraps its owned values once; a boundary
/// forwards the shared values it received; a loop shares its
/// broadcast inputs across every iteration.
pub type OutputBag = BTreeMap<String, Arc<Value>>;

/// Process a node's output: emit pulses ONLY for the output ports the
/// node mentioned in `output`. Ports the node didn't mention this call
/// don't get touched; if the node never mentions a port across all its
/// emissions, the engine emits a CLOSURE on that port at termination
/// (see `close_unmentioned_downstream`).
///
/// Returns the set of port names actually emitted on this call.
#[allow(clippy::too_many_arguments)]
pub fn postprocess_output(
    node_id: &str,
    output: &OutputBag,
    emission_id: Uuid,
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

    let outgoing = edge_idx.get_outgoing(project, node_id);

    // What each wire DELIVERS, worked out BEFORE any pulse mutation. A
    // plain wire shares the emitted value (one `Arc` per port, however
    // many wires leave it); a wire with a path reads its keys off the
    // value right here, once per wire, so five wires off one port are
    // five independent projections; a `?` key found absent makes that
    // wire deliver a closure. A value that breaks its declared contract
    // (a required key missing) fails the firing, and doing all of it up
    // front means a failing firing leaves no partial state behind: the
    // caller drops `emissions` on Err, so a pulse committed before a
    // later failure would have no journal row and replay would diverge.
    let mut deliveries: Vec<Delivery<'_>> = Vec::new();
    for (port_name, value) in output {
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
                    Ok(crate::deref::Projection::Value(v)) => Some(Arc::new(v)),
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

    // A single-shot input port holds one value per firing of its
    // consumer. The compiler's `duplicate-input-port` rule gives every
    // input exactly one wire (it keys on the edge's target port, and
    // the compiler names one on every edge it builds), so the only
    // way a second, different
    // value can reach a port with one already pending is the SAME
    // producer firing again at the same location before the consumer
    // took the first: refused before anything is committed. An equal
    // value dedups in `emit_value_on_edge`; a closure is never a
    // collision (a data pulse outranks it); a generator target takes
    // repeated pulses by design (see `edge_targets_generator`).
    for d in &deliveries {
        let Some(value) = &d.value else { continue };
        if super::ready::edge_targets_generator(project, d.edge) {
            continue;
        }
        let target_handle = d.edge.target_handle.as_deref().unwrap_or("default");
        let earlier_value_pending = pulses.get(&d.edge.target).is_some_and(|ps| {
            ps.iter().any(|p| {
                p.status.is_pending()
                    && p.color == color
                    && &p.frames == frames
                    && p.target_port == target_handle
                    && !p.closed
                    && !same_value(&p.value, value)
            })
        });
        if earlier_value_pending {
            return Err(WeftError::NodeExecution(format!(
                "'{node_id}.{port}' emitted a value onto '{target}.{target_port}' at frames \
                 {frames:?} while a DIFFERENT value from an earlier firing of '{node_id}' is \
                 still pending there; a consumer takes one value per firing, so the earlier \
                 one has to be consumed before the next lands",
                port = d.port,
                target = d.edge.target,
                target_port = target_handle,
            )));
        }
    }

    // Every port is declared (validated above); emit.
    for d in deliveries {
        mentioned.insert(d.port.to_string());
        match d.value {
            Some(value) => emit_value_on_edge(
                project, node_id, d.edge, d.port, value, emission_id, color, frames, pulses, emissions,
            ),
            None => emit_closure_on_edge(
                project, node_id, d.edge, d.port, emission_id, color, frames, None, pulses, emissions,
            ),
        }
    }
    Ok(mentioned)
}

/// Two shared values are the same when they are one allocation or
/// compare equal; the pointer check makes the common fan-in case (one
/// emission reaching one port over two wires) free.
fn same_value(a: &Arc<Value>, b: &Arc<Value>) -> bool {
    Arc::ptr_eq(a, b) || **a == **b
}

/// One wire's share of an emission: the edge, the port it leaves from,
/// and what lands on the other end (`None` = a closure, from a `?` key
/// the path found absent).
struct Delivery<'a> {
    edge: &'a Edge,
    port: &'a str,
    value: Option<Arc<Value>>,
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
/// - The pulse's id already in the bucket: the same emission on the
///   same wire replayed (a journal row applied twice); one pulse,
///   never two. The id names the wire end to end, so two output ports
///   of one emission into one input port are two ids.
#[allow(clippy::too_many_arguments)]
fn emit_value_on_edge(
    project: &ProjectDefinition,
    source_node: &str,
    edge: &Edge,
    port: &str,
    value: Arc<Value>,
    emission_id: Uuid,
    color: Color,
    frames: &LoopFrames,
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    let target_handle = edge.target_handle.as_deref().unwrap_or("default");
    let id = pulse_id(emission_id, port, &edge.target, target_handle, false);

    let bucket = pulses.entry(edge.target.clone()).or_default();
    if bucket.iter().any(|p| p.id == id) {
        return;
    }
    let already_present_same_value = !super::ready::edge_targets_generator(project, edge)
        && bucket.iter().any(|p| {
            p.status.is_pending()
                && p.color == color
                && &p.frames == frames
                && p.target_port == target_handle
                && !p.closed
                && same_value(&p.value, &value)
        });
    if already_present_same_value {
        return;
    }

    let pulse = Pulse::new(
        id,
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
    bucket.push(pulse);
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
#[allow(clippy::too_many_arguments)]
pub fn close_unmentioned_downstream(
    node_id: &str,
    mentioned: &HashSet<String>,
    emission_id: Uuid,
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
        let is_generator = port.is_generator();
        if mentioned.contains(&port.name) && !is_generator {
            continue;
        }
        let close_error = if is_generator { failure } else { None };
        emit_closure_on_outgoing(
            project, node_id, &port.name, emission_id, color, frames, close_error, &outgoing,
            pulses, emissions,
        );
    }
    Ok(())
}

/// Emit a CLOSURE on ONE specific output port at (color, frames).
/// Shared primitive for the termination-time sweep
/// (`close_unmentioned_downstream`, port-by-port) and the mid-firing
/// `ctx.close_port` call.
#[allow(clippy::too_many_arguments)]
pub fn emit_port_closure(
    node_id: &str,
    port_name: &str,
    emission_id: Uuid,
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
        project, node_id, port_name, emission_id, color, frames, None, &outgoing, pulses, emissions,
    );
    Ok(())
}

/// Emit a closure on every outgoing edge of `port_name`.
#[allow(clippy::too_many_arguments)]
fn emit_closure_on_outgoing(
    project: &ProjectDefinition,
    node_id: &str,
    port_name: &str,
    emission_id: Uuid,
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
        emit_closure_on_edge(
            project, node_id, edge, port_name, emission_id, color, frames, close_error, pulses,
            emissions,
        );
    }
}

/// Put a closure on ONE edge. On a NORMAL target port, a closure is
/// suppressed by anything already pending at the key (a data pulse
/// outranks it forever; a closure is single-shot). On a GENERATOR
/// target port, buffered items and the closure coexist (the items are
/// taken first, then the end); only a closure already pending dedups a
/// second one. A closure whose id is already in the bucket is the same
/// emission replayed, and dedups like the value case.
#[allow(clippy::too_many_arguments)]
fn emit_closure_on_edge(
    project: &ProjectDefinition,
    node_id: &str,
    edge: &Edge,
    port_name: &str,
    emission_id: Uuid,
    color: Color,
    frames: &LoopFrames,
    close_error: Option<&str>,
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    let target_handle = edge.target_handle.as_deref().unwrap_or("default");
    let generator_target = super::ready::edge_targets_generator(project, edge);
    let id = pulse_id(emission_id, port_name, &edge.target, target_handle, true);

    let bucket = pulses.entry(edge.target.clone()).or_default();
    if bucket.iter().any(|p| p.id == id) {
        return;
    }
    let already_present = bucket.iter().any(|p| {
        p.status.is_pending()
            && p.color == color
            && &p.frames == frames
            && p.target_port == target_handle
            && (!generator_target || p.closed)
    });
    if already_present {
        return;
    }

    let pulse = Pulse::closure_with_error(
        id,
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
    bucket.push(pulse);
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

    fn emission() -> Uuid {
        Uuid::new_v4()
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
        emit_value_on_edge(&project, "src1", outgoing_refs[0], "out", Arc::new(json!(42)), emission(), color, &frames, &mut pulses, &mut emissions);
        emit_value_on_edge(&project, "src2", outgoing_refs[0], "out", Arc::new(json!(42)), emission(), color, &frames, &mut pulses, &mut emissions);

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<_> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 1, "exactly one pending pulse after dedup");
    }

    /// The same emission applied twice (a journal row replayed) is
    /// one pulse, even on a generator target where equal items are
    /// otherwise two items.
    #[test]
    fn a_replayed_emission_is_one_pulse_even_on_a_generator_target() {
        let project: ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", vec!["out"], vec![]),
                { "id": "consumer", "nodeType": "Test", "label": null, "config": null,
                  "position": { "x": 0.0, "y": 0.0 },
                  "inputs": [{ "name": "in", "portType": "Generator[Number]", "required": true }],
                  "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }
            ],
            "edges": [{ "id": "e", "source": "src", "sourceHandle": "out", "target": "consumer", "targetHandle": "in" }],
            "groups": [],
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }))
        .expect("stream project");
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let same = emission();
        for _ in 0..2 {
            postprocess_output("src", &bag(json!({"out": 1})), same, Uuid::nil(), &Vec::new(), &project, &mut pulses, &edge_idx, &mut emissions).expect("emit");
        }
        assert_eq!(pulses["consumer"].len(), 1, "one pulse for one emission, however often applied");
        let other = emission();
        postprocess_output("src", &bag(json!({"out": 1})), other, Uuid::nil(), &Vec::new(), &project, &mut pulses, &edge_idx, &mut emissions).expect("emit");
        assert_eq!(pulses["consumer"].len(), 2, "a second emission of an equal item is a second item");
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
            &project, "src1", "out", emission(), color, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
        );
        emit_value_on_edge(
            &project, "src2", outgoing_refs[0], "out", Arc::new(json!(42)), emission(), color, &frames, &mut pulses, &mut emissions,
        );

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<&Pulse> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 2, "both pulses pending in the table");
        let winner = crate::exec::ready::resolve_port_value(&pending, "in")
            .expect("a winner");
        assert_eq!(*winner.value, json!(42), "resolve picks data over closure");
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
            &project, "src1", outgoing_refs[0], "out", Arc::new(json!(42)), emission(), color, &frames, &mut pulses, &mut emissions,
        );
        let emissions_before = emissions.len();
        emit_closure_on_outgoing(
            &project, "src2", "out", emission(), color, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
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
        assert_eq!(*pending[0].value, json!(42));
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

    fn bag(output: serde_json::Value) -> OutputBag {
        output
            .as_object()
            .expect("test bag is an object")
            .iter()
            .map(|(k, v)| (k.clone(), Arc::new(v.clone())))
            .collect()
    }

    fn run_postprocess(
        project: &ProjectDefinition,
        output: serde_json::Value,
        pulses: &mut PulseTable,
        emissions: &mut Vec<PulseEmission>,
    ) -> WeftResult<HashSet<String>> {
        let edge_idx = EdgeIndex::build(project);
        postprocess_output(
            "src", &bag(output), emission(), uuid::Uuid::nil(), &Vec::new(), project, pulses, &edge_idx, emissions,
        )
    }

    /// The producer fires again at the same location with a DIFFERENT
    /// value while its first value is still pending at the consumer:
    /// the second firing fails atomically, no pulse committed, no
    /// emission reported. The same value again dedups to one pulse.
    #[test]
    fn a_second_value_over_a_pending_one_errors_with_no_partial_state() {
        let project = project_with_edges(&["out1"]);
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        run_postprocess(&project, json!({"out1": "A"}), &mut pulses, &mut emissions)
            .expect("first emit");
        let pulses_before = pulses.get("consumer").map(|b| b.len()).unwrap_or(0);
        let emissions_before = emissions.len();

        let err = run_postprocess(&project, json!({"out1": "B"}), &mut pulses, &mut emissions)
            .expect_err("conflicting value must error");
        assert!(err.to_string().contains("still pending"), "names the case: {err}");
        assert_eq!(pulses.get("consumer").map(|b| b.len()).unwrap_or(0), pulses_before);
        assert_eq!(emissions.len(), emissions_before, "no partial journal mutations");

        run_postprocess(&project, json!({"out1": "A"}), &mut pulses, &mut emissions)
            .expect("the same value again is fine");
        assert_eq!(pulses["consumer"].len(), 1, "deduped to one pulse");
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
            &project, "src1", "out", emission(), color, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
        );
        emit_closure_on_outgoing(
            &project, "src2", "out", emission(), color, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
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
                  "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                { "id": "whole2", "nodeType": "Test", "label": null, "config": null,
                  "position": { "x": 0.0, "y": 0.0 },
                  "inputs": [{ "name": "in", "portType": ty, "required": true }],
                  "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }
            ],
            "edges": [
                { "id": "e1", "source": "src", "sourceHandle": "out", "target": "speed", "targetHandle": "in",
                  "path": ["profile", "wpm"] },
                { "id": "e2", "source": "src", "sourceHandle": "out", "target": "who", "targetHandle": "in",
                  "path": ["profile", "name"] },
                { "id": "e3", "source": "src", "sourceHandle": "out", "target": "whole", "targetHandle": "in" },
                { "id": "e4", "source": "src", "sourceHandle": "out", "target": "whole2", "targetHandle": "in" }
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
    /// the key's value, the plain wires share ONE allocation of the
    /// whole record, and an absent `?` key closes only its own wire.
    #[test]
    fn a_wire_with_a_path_delivers_the_key_and_plain_wires_share_the_value() {
        let project = deref_project();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let value = json!({ "profile": { "wpm": 42 }, "id": "u1" });
        run_postprocess(&project, json!({ "out": value.clone() }), &mut pulses, &mut emissions)
            .expect("emit");

        let speed = pending_on(&pulses, "speed");
        assert_eq!(speed.len(), 1);
        assert_eq!(*speed[0].value, json!(42));
        assert!(!speed[0].closed);

        let who = pending_on(&pulses, "who");
        assert_eq!(who.len(), 1);
        assert!(who[0].closed, "the absent `name?` closes that wire alone");

        let whole = pending_on(&pulses, "whole");
        let whole2 = pending_on(&pulses, "whole2");
        assert_eq!(*whole[0].value, value, "the plain wire is untouched");
        assert!(
            Arc::ptr_eq(&whole[0].value, &whole2[0].value),
            "two plain wires off one port point at one value"
        );
        // Every delivery is reported with what it delivered.
        assert_eq!(emissions.len(), 4);
    }

    /// The same emission over the same program yields the same pulse
    /// ids: the property the journal fold rests on.
    #[test]
    fn the_same_emission_yields_the_same_pulse_ids() {
        let project = deref_project();
        let edge_idx = EdgeIndex::build(&project);
        let same = emission();
        let value = bag(json!({ "out": { "profile": { "wpm": 42 }, "id": "u1" } }));
        let ids = |pulses: &PulseTable| -> Vec<(String, Uuid)> {
            let mut v: Vec<(String, Uuid)> = pulses.iter().flat_map(|(n, b)| b.iter().map(move |p| (n.clone(), p.id))).collect();
            v.sort();
            v
        };
        let mut a = PulseTable::default();
        postprocess_output("src", &value, same, Uuid::nil(), &Vec::new(), &project, &mut a, &edge_idx, &mut Vec::new()).expect("emit");
        let mut b = PulseTable::default();
        postprocess_output("src", &value, same, Uuid::nil(), &Vec::new(), &project, &mut b, &edge_idx, &mut Vec::new()).expect("emit");
        assert_eq!(ids(&a), ids(&b));
        assert_eq!(ids(&a).len(), 4);
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
