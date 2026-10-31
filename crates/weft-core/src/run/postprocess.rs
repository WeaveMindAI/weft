//! Output postprocessing. After a node returns from `execute`, the
//! runtime calls this to emit pulses on each outgoing edge. No type
//! checking here: type enforcement is the consumer's input boundary's
//! job (`ready::check_input`).
//!
//! Every function here takes the EMISSION id of the act it serves
//! (see `run::emission`): pulse ids derive from it, so the journal
//! fold, replaying the same emission over the same program, puts the
//! same pulses in the table. Values are shared (`Arc`), never copied
//! per wire; a wire with a key path gets its own projected value.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use crate::error::{WeftError, WeftResult};
use crate::run::emission::{pulse_id, PulseEmission};
use crate::frames::LoopFrames;
use crate::project::{Edge, ProgramIndex, ProjectDefinition};
use crate::pulse::{Failure, Pulse, PulseTable};
use crate::record_ref::{Step, ValueRef};
use crate::RunId;


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
    run_id: RunId,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    program_idx: &ProgramIndex,
    emissions: &mut Vec<PulseEmission>,
) -> WeftResult<HashSet<String>> {
    postprocess_output_kept(node_id, output, &BTreeMap::new(), emission_id, run_id, frames, project, pulses, program_idx, emissions)
}

/// [`postprocess_output`], knowing where the run's record keeps each
/// port's value (`kept`, by port; a port missing from it is not known):
/// every pulse it puts on a wire carries where its value is kept
/// (`Pulse::origin`), a wire with a key path the part it reads.
#[allow(clippy::too_many_arguments)]
pub fn postprocess_output_kept(
    node_id: &str,
    output: &OutputBag,
    kept: &BTreeMap<String, Arc<ValueRef>>,
    emission_id: Uuid,
    run_id: RunId,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    program_idx: &ProgramIndex,
    emissions: &mut Vec<PulseEmission>,
) -> WeftResult<HashSet<String>> {
    let mut mentioned = HashSet::new();
    let Some(node) = program_idx.node(project, node_id) else {
        return Err(WeftError::NodeFailed(format!(
            "node '{node_id}' is not in the project at postprocess time; the project was \
             mutated between dispatch and postprocess",
        )));
    };

    let outgoing = program_idx.get_outgoing(project, node_id, frames);

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
            return Err(WeftError::NodeFailed(format!(
                "node '{node_id}' emitted on undeclared output port '{port_name}' \
                 (declared: {declared:?})",
                declared = node.outputs.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            )));
        };
        mentioned.insert(port_name.clone());
        let origin = kept.get(port_name);
        for edge in outgoing.iter().filter(|e| e.source_handle.as_deref() == Some(port_name.as_str())) {
            let delivered = if edge.path.is_empty() {
                Some(value.clone())
            } else {
                // Read off the type the port has where this firing ran.
                let at = node.types_at(&crate::frames::call_path(frames));
                match crate::deref::project_value(value, at.output(&port.name).unwrap_or(&port.port_type), &edge.path) {
                    Ok(crate::deref::Projection::Value(v)) => Some(Arc::new(v)),
                    Ok(crate::deref::Projection::Closed { .. }) => None,
                    Err(e) => {
                        return Err(WeftError::NodeFailed(format!(
                            "node '{node_id}' emitted on '{port_name}' a value the wire to \
                             '{}.{}' cannot read: {e}",
                            edge.target,
                            edge.target_handle.as_deref().unwrap_or("default"),
                        )));
                    }
                }
            };
            let origin = origin.map(|origin| part_on_wire(origin, edge));
            deliveries.push(Delivery { edge, port: port_name.as_str(), value: delivered, origin });
        }
    }

    // A single-shot input port holds one value per firing of its
    // consumer. The compiler's `duplicate-input-port` rule gives every
    // input exactly one wire (it keys on the edge's target port, and
    // the compiler names one on every edge it builds), a location fires
    // once per run (`run::firing::firing_at`), and the runner
    // sends an ordinary port at most once per firing (a body replayed
    // after a pause sends it again as nothing). So the engine never
    // brings a second, different value onto a port with one pending;
    // only a journal can, with rows that say so (two `PortEmitted` for
    // one port at one location, or a supplied value landing where its
    // source's own emission waits). That is refused before anything is
    // committed, and the fold reports it as corruption rather than pick
    // one. An equal value dedups in `emit_value_on_edge`; a closure is
    // never a collision (a data pulse outranks it); a generator target
    // takes repeated pulses by design (see `edge_targets_generator`).
    for d in &deliveries {
        let Some(value) = &d.value else { continue };
        if super::ready::edge_targets_generator(program_idx, d.edge) {
            continue;
        }
        let target_handle = d.edge.target_handle.as_deref().unwrap_or("default");
        let Some(landing_frames) = program_idx.frames_across(d.edge, frames) else { continue };
        let earlier_value_pending = pulses.get(&d.edge.target).is_some_and(|ps| {
            ps.iter().any(|p| {
                p.status.is_pending()
                    && p.run_id == run_id
                    && p.frames == landing_frames
                    && p.target_port == target_handle
                    && !p.closed
                    && !same_value(&p.value, value)
            })
        });
        if earlier_value_pending {
            return Err(WeftError::NodeFailed(format!(
                "'{node_id}.{port}' emitted a value onto '{target}.{target_port}' at frames \
                 {frames:?} while a DIFFERENT value from '{node_id}' is still pending there; \
                 a consumer takes one value per firing, so the two cannot both stand",
                port = d.port,
                target = d.edge.target,
                target_port = target_handle,
            )));
        }
    }

    // Every port is declared (validated above); emit.
    for d in deliveries {
        match d.value {
            Some(value) => emit_value_on_edge(
                program_idx, node_id, d.edge, d.port, value, d.origin, emission_id, run_id, frames, pulses, emissions,
            ),
            None => emit_closure_on_edge(
                program_idx, node_id, d.edge, d.port, emission_id, run_id, frames, None, pulses, emissions,
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
    origin: Option<Arc<ValueRef>>,
}

/// Where the record keeps what `edge` delivers of a value kept at
/// `origin`: the value itself, or the part its key path reads.
pub(crate) fn part_on_wire(origin: &Arc<ValueRef>, edge: &Edge) -> Arc<ValueRef> {
    if edge.path.is_empty() {
        return origin.clone();
    }
    Arc::new(origin.as_ref().clone().part(edge.path.iter().map(|key| Step::Key(key.clone())).collect()))
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
pub(crate) fn emit_value_on_edge(
    program_idx: &ProgramIndex,
    source_node: &str,
    edge: &Edge,
    port: &str,
    value: Arc<Value>,
    origin: Option<Arc<ValueRef>>,
    emission_id: Uuid,
    run_id: RunId,
    frames: &LoopFrames,
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    let Some(frames) = program_idx.frames_across(edge, frames) else { return };
    let frames = &frames;
    let target_handle = edge.target_port();
    let id = pulse_id(emission_id, &edge.id, false);

    if pulses.landed(&edge.target, id) {
        return;
    }
    let bucket = pulses.entry(edge.target.clone()).or_default();
    let already_present_same_value = !super::ready::edge_targets_generator(program_idx, edge)
        && bucket.iter().any(|p| {
            p.status.is_pending()
                && p.run_id == run_id
                && &p.frames == frames
                && p.target_port == target_handle
                && !p.closed
                && same_value(&p.value, &value)
        });
    if already_present_same_value {
        return;
    }

    let mut pulse = Pulse::new(
        id,
        run_id,
        frames.clone(),
        edge.target.clone(),
        target_handle.to_string(),
        value,
    );
    pulse.origin = origin;
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
/// clean finish over a truncated stream. Scalar closures retain it too,
/// so a skip further down still names the node that broke.
#[allow(clippy::too_many_arguments)]
pub fn close_unmentioned_downstream(
    node_id: &str,
    mentioned: &HashSet<String>,
    emission_id: Uuid,
    run_id: RunId,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    program_idx: &ProgramIndex,
    emissions: &mut Vec<PulseEmission>,
    failure: Option<&Failure>,
    closed: &HashSet<String>,
) -> WeftResult<()> {
    let Some(node) = program_idx.node(project, node_id) else {
        // Same impossible state as `postprocess_output`'s node lookup;
        // silently skipping would leave every downstream consumer
        // waiting on ports that will never close.
        return Err(WeftError::NodeFailed(format!(
            "node '{node_id}' is not in the project at closure-sweep time; the project \
             was mutated between dispatch and termination",
        )));
    };
    let outgoing = program_idx.get_outgoing(project, node_id, frames);
    for port in &node.outputs {
        let is_generator = port.is_generator();
        if closed.contains(&port.name) || (mentioned.contains(&port.name) && !is_generator) {
            continue;
        }
        emit_closure_on_outgoing(
            program_idx, node_id, &port.name, emission_id, run_id, frames, failure, &outgoing,
            pulses, emissions,
        );
    }
    Ok(())
}

/// Emit a CLOSURE on ONE specific output port at (run, frames).
/// Shared primitive for the termination-time sweep
/// (`close_unmentioned_downstream`, port-by-port) and the mid-firing
/// `ctx.close_port` call.
#[allow(clippy::too_many_arguments)]
pub fn emit_port_closure(
    node_id: &str,
    port_name: &str,
    emission_id: Uuid,
    run_id: RunId,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    program_idx: &ProgramIndex,
    emissions: &mut Vec<PulseEmission>,
    failure: Option<&Failure>,
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
            return Err(WeftError::NodeFailed(format!(
                "close_port on undeclared output port '{port_name}' of node '{node_id}'",
            )));
        }
        None => {
            return Err(WeftError::NodeFailed(format!(
                "close_port: node '{node_id}' is not in the project",
            )));
        }
    }
    let outgoing = program_idx.get_outgoing(project, node_id, frames);
    emit_closure_on_outgoing(
        program_idx, node_id, port_name, emission_id, run_id, frames, failure, &outgoing, pulses, emissions,
    );
    Ok(())
}

/// A loop's `LoopIn` closure sweep, run by each iteration's launch
/// (`loop_runtime`): first close, WITH its error, every inside output
/// whose same-named outer input arrived as a failed closure
/// (`closed_failures`, from the firing input), then close everything
/// else the firing did not mention plainly. The one place a boundary
/// forwards a failure as a failure: a port the node does not declare as
/// an output, or one the firing mentioned, is left to the plain sweep's
/// own rules.
#[allow(clippy::too_many_arguments)]
pub fn close_failed_then_unmentioned_downstream(
    node_id: &str,
    closed_failures: &BTreeMap<String, Failure>,
    mentioned: &HashSet<String>,
    emission_id: Uuid,
    run_id: RunId,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    program_idx: &ProgramIndex,
    emissions: &mut Vec<PulseEmission>,
) -> WeftResult<()> {
    let declared: HashSet<&str> = project
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .map(|n| n.outputs.iter().map(|o| o.name.as_str()).collect())
        .unwrap_or_default();
    let mut closed = HashSet::new();
    for (port, failure) in closed_failures {
        if mentioned.contains(port) || !declared.contains(port.as_str()) {
            continue;
        }
        emit_port_closure(
            node_id, port, emission_id, run_id, frames, project, pulses, program_idx, emissions,
            Some(failure),
        )?;
        closed.insert(port.clone());
    }
    close_unmentioned_downstream(
        node_id, mentioned, emission_id, run_id, frames, project, pulses, program_idx, emissions,
        None, &closed,
    )
}

/// Emit a closure on every outgoing edge of `port_name`.
#[allow(clippy::too_many_arguments)]
fn emit_closure_on_outgoing(
    program_idx: &ProgramIndex,
    node_id: &str,
    port_name: &str,
    emission_id: Uuid,
    run_id: RunId,
    frames: &LoopFrames,
    failure: Option<&Failure>,
    outgoing: &[&Edge],
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    for edge in outgoing
        .iter()
        .filter(|e| e.source_handle.as_deref() == Some(port_name))
    {
        emit_closure_on_edge(
            program_idx, node_id, edge, port_name, emission_id, run_id, frames, failure, pulses,
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
pub(crate) fn emit_closure_on_edge(
    program_idx: &ProgramIndex,
    node_id: &str,
    edge: &Edge,
    port_name: &str,
    emission_id: Uuid,
    run_id: RunId,
    frames: &LoopFrames,
    failure: Option<&Failure>,
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    let Some(frames) = program_idx.frames_across(edge, frames) else { return };
    let frames = &frames;
    let target_handle = edge.target_port();
    let generator_target = super::ready::edge_targets_generator(program_idx, edge);
    let id = pulse_id(emission_id, &edge.id, true);

    if pulses.landed(&edge.target, id) {
        return;
    }
    let bucket = pulses.entry(edge.target.clone()).or_default();
    let already_present = bucket.iter().any(|p| {
        p.status.is_pending()
            && p.run_id == run_id
            && &p.frames == frames
            && p.target_port == target_handle
            && (!generator_target || p.closed)
    });
    if already_present {
        return;
    }

    let pulse = Pulse::closure_with_failure(
        id,
        run_id,
        frames.clone(),
        edge.target.clone(),
        target_handle.to_string(),
        failure.cloned(),
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
            via: Vec::new(),
            span: None,
            source_file: None,
        }
    }

    fn emission() -> Uuid {
        Uuid::new_v4()
    }

    #[test]
    fn unused_output_is_still_mentioned_for_future_seed_consumers() {
        let project = direct_project();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let mentioned = postprocess_output("src", &OutputBag::from([("out".into(), Arc::new(json!(42)))]),
            emission(), Uuid::nil(), &vec![], &project, &mut pulses, &ProgramIndex::build(&project), &mut emissions).unwrap();
        assert_eq!(mentioned, HashSet::from(["out".into()]));
        assert!(pulses.is_empty());
        assert!(emissions.is_empty());
    }

    #[test]
    fn failed_scalar_closure_keeps_its_reason() {
        let mut project = direct_project();
        project.edges.push(edge("out", "consumer", "in"));
        let index = ProgramIndex::build(&project);
        let refused = Failure { node: "src".into(), error: "refused output".into() };
        for explicit in [true, false] {
            let mut pulses = PulseTable::default();
            let mut emissions = Vec::new();
            if explicit {
                emit_port_closure("src", "out", emission(), Uuid::nil(), &vec![], &project,
                    &mut pulses, &index, &mut emissions, Some(&refused)).unwrap();
            } else {
                close_unmentioned_downstream("src", &HashSet::new(), emission(), Uuid::nil(), &vec![], &project,
                    &mut pulses, &index, &mut emissions, Some(&refused), &HashSet::new()).unwrap();
            }
            assert_eq!(pulses["consumer"][0].failure.as_ref(), Some(&refused));
        }
    }

    #[test]
    fn completion_does_not_close_a_consumed_generator_end_again() {
        let mut project = direct_project();
        project.nodes[0].outputs[0].port_type = serde_json::from_value(json!("Generator[Number]")).unwrap();
        project.nodes[1].inputs[0].port_type = serde_json::from_value(json!("Generator[Number]")).unwrap();
        project.edges.push(edge("out", "consumer", "in"));
        let index = ProgramIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        emit_port_closure("src", "out", emission(), Uuid::nil(), &vec![], &project, &mut pulses, &index, &mut emissions, None).unwrap();
        assert_eq!(emissions.len(), 1);
        pulses.clear();
        emissions.clear();
        close_unmentioned_downstream("src", &HashSet::from(["out".into()]), emission(), Uuid::nil(), &vec![], &project,
            &mut pulses, &index, &mut emissions, None, &HashSet::from(["out".into()])).unwrap();
        assert!(emissions.is_empty());
        assert!(pulses.is_empty());
    }

    #[test]
    fn same_value_fan_in_dedups_silently() {
        let outgoing = [edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let run_id = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        let project = direct_project();
        emit_value_on_edge(&ProgramIndex::build(&project), "src1", outgoing_refs[0], "out", Arc::new(json!(42)), None, emission(), run_id, &frames, &mut pulses, &mut emissions);
        emit_value_on_edge(&ProgramIndex::build(&project), "src2", outgoing_refs[0], "out", Arc::new(json!(42)), None, emission(), run_id, &frames, &mut pulses, &mut emissions);

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
        let program_idx = ProgramIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let same = emission();
        for _ in 0..2 {
            postprocess_output("src", &bag(json!({"out": 1})), same, Uuid::nil(), &Vec::new(), &project, &mut pulses, &program_idx, &mut emissions).expect("emit");
        }
        assert_eq!(pulses["consumer"].len(), 1, "one pulse for one emission, however often applied");
        let other = emission();
        postprocess_output("src", &bag(json!({"out": 1})), other, Uuid::nil(), &Vec::new(), &project, &mut pulses, &program_idx, &mut emissions).expect("emit");
        assert_eq!(pulses["consumer"].len(), 2, "a second emission of an equal item is a second item");
    }

    #[test]
    fn data_and_existing_closure_coexist_and_data_wins_at_resolve_time() {
        let outgoing = [edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let run_id = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        let project = direct_project();
        emit_closure_on_outgoing(
            &ProgramIndex::build(&project), "src1", "out", emission(), run_id, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
        );
        emit_value_on_edge(
            &ProgramIndex::build(&project), "src2", outgoing_refs[0], "out", Arc::new(json!(42)), None, emission(), run_id, &frames, &mut pulses, &mut emissions,
        );

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<&Pulse> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 2, "both pulses pending in the table");
        let winner = crate::run::ready::resolve_port_value(&pending, "in")
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
        let run_id = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        let project = direct_project();
        emit_value_on_edge(
            &ProgramIndex::build(&project), "src1", outgoing_refs[0], "out", Arc::new(json!(42)), None, emission(), run_id, &frames, &mut pulses, &mut emissions,
        );
        let emissions_before = emissions.len();
        emit_closure_on_outgoing(
            &ProgramIndex::build(&project), "src2", "out", emission(), run_id, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
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
            type_params: Vec::new(),
            place_types: Vec::new(),
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
            requires_infra: false, per_instance: None,
            fires_with: Default::default(), baked_outputs: Default::default(),
            images: Vec::new(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
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
        let program_idx = ProgramIndex::build(project);
        postprocess_output(
            "src", &bag(output), emission(), uuid::Uuid::nil(), &Vec::new(), project, pulses, &program_idx, emissions,
        )
    }

    /// A second, DIFFERENT value on a port whose first is still pending
    /// at the consumer (what only a journal's rows can bring, never the
    /// runner): refused atomically, no pulse committed, no emission
    /// reported. The same value again dedups to one pulse.
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
    /// neither fire nor skip, stalling the run invisibly.
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
        let run_id = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        let project = direct_project();
        emit_closure_on_outgoing(
            &ProgramIndex::build(&project), "src1", "out", emission(), run_id, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
        );
        emit_closure_on_outgoing(
            &ProgramIndex::build(&project), "src2", "out", emission(), run_id, &frames, None, &outgoing_refs, &mut pulses, &mut emissions,
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
        let program_idx = ProgramIndex::build(&project);
        let same = emission();
        let value = bag(json!({ "out": { "profile": { "wpm": 42 }, "id": "u1" } }));
        let ids = |pulses: &PulseTable| -> Vec<(String, Uuid)> {
            let mut v: Vec<(String, Uuid)> = pulses.iter().flat_map(|(n, b)| b.iter().map(move |p| (n.clone(), p.id))).collect();
            v.sort();
            v
        };
        let mut a = PulseTable::default();
        postprocess_output("src", &value, same, Uuid::nil(), &Vec::new(), &project, &mut a, &program_idx, &mut Vec::new()).expect("emit");
        let mut b = PulseTable::default();
        postprocess_output("src", &value, same, Uuid::nil(), &Vec::new(), &project, &mut b, &program_idx, &mut Vec::new()).expect("emit");
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
