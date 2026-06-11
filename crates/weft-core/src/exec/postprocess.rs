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

    // Pre-validate the whole output_obj BEFORE any pulse mutation. A
    // single-port emission whose value collides with an existing-pending
    // data pulse at the fan-in target is the producer's fault; failing
    // only at the point of the bad port would leave already-committed
    // pulses live in the pulse table without matching journal
    // `PulseEmitted` events (the caller drops `emissions` on Err),
    // causing replay divergence. Validate every port up front so a
    // failing firing leaves no partial state behind.
    for (port_name, value) in output_obj {
        // An undeclared port means the value would silently vanish
        // (downstream never fires AND never receives a closure): fail
        // the firing instead. Node-author emissions are caught earlier
        // by the engine's declared-output check, so reaching this is
        // an engine-internal wiring bug.
        if !node.outputs.iter().any(|p| &p.name == port_name) {
            return Err(WeftError::NodeExecution(format!(
                "node '{node_id}' emitted on undeclared output port '{port_name}' \
                 (declared: {declared:?})",
                declared = node.outputs.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            )));
        }
        for edge in outgoing.iter().filter(|e| e.source_handle.as_deref() == Some(port_name.as_str())) {
            let target_handle = edge.target_handle.as_deref().unwrap_or("default");
            // Cross-firing collision: another in-flight data pulse at
            // the same fan-in key with a DIFFERENT value.
            let cross_firing_conflict = pulses.get(&edge.target).and_then(|ps| {
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
                    target = edge.target,
                    target_port = target_handle,
                )));
            }
            // Within-firing collision: a sibling output of the SAME
            // firing also wires to the same fan-in target with a
            // different value.
            let within_firing_conflict = output_obj.iter().any(|(other_port_name, other_value)| {
                if other_port_name == port_name { return false; }
                if other_value == value { return false; }
                outgoing.iter().any(|other_edge| {
                    other_edge.source_handle.as_deref() == Some(other_port_name.as_str())
                        && other_edge.target == edge.target
                        && other_edge.target_handle.as_deref().unwrap_or("default") == target_handle
                })
            });
            if within_firing_conflict {
                return Err(WeftError::NodeExecution(format!(
                    "fan-in collision on '{target}.{target_port}' at frames {frames:?}: \
                     two output ports of node '{node_id}' are wired to the same input and \
                     emitted DIFFERENT values in one firing. Rewire so only one source \
                     feeds this port.",
                    target = edge.target,
                    target_port = target_handle,
                )));
            }
        }
    }

    // Every port is declared (validated above); emit.
    for (port_name, value) in output_obj {
        mentioned.insert(port_name.clone());
        emit_single(
            node_id,
            &outgoing,
            port_name,
            value,
            color,
            frames,
            pulses,
            emissions,
        );
    }
    Ok(mentioned)
}

/// Emit a value on every outgoing edge from `port`. Fan-in policy:
/// - Same data already pending at this key: dedup silently
///   (same-source double-edge).
/// - Closure already pending: append the data pulse anyway; both stay
///   in the table. `resolve_port_value` prefers the non-closed pulse
///   at read time.
fn emit_single(
    source_node: &str,
    outgoing: &[&Edge],
    port: &str,
    value: &Value,
    color: Color,
    frames: &LoopFrames,
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    for edge in outgoing {
        if edge.source_handle.as_deref() != Some(port) {
            continue;
        }
        let target_handle = edge.target_handle.as_deref().unwrap_or("default");

        let already_present_same_value = pulses.get(&edge.target).map(|ps| {
            ps.iter().any(|p| {
                p.status.is_pending()
                    && p.color == color
                    && &p.frames == frames
                    && p.target_port == target_handle
                    && !p.closed
                    && p.value == *value
            })
        }).unwrap_or(false);
        if already_present_same_value {
            continue;
        }

        let pulse = Pulse::new(
            color,
            frames.clone(),
            edge.target.clone(),
            target_handle.to_string(),
            value.clone(),
        );
        emissions.push(PulseEmission {
            pulse: pulse.clone(),
            source_node: source_node.to_string(),
            source_port: port.to_string(),
        });
        pulses.entry(edge.target.clone()).or_default().push(pulse);
    }
}

/// Emit a CLOSURE marker on every output port the node never
/// mentioned. Called when the node's firing ends (Completed, Failed,
/// Skipped, Cancelled) so downstream consumers learn no value is
/// coming for those ports. Generic over node type: this module never
/// inspects `node.node_type`. Callers that need to skip auto-close
/// (loop boundary nodes on per-iteration firings) do so at the call
/// site by NOT invoking this function. Keeping the language layer
/// free of node-name strings is the invariant.
pub fn close_unmentioned_downstream(
    node_id: &str,
    mentioned: &HashSet<String>,
    color: Color,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    emissions: &mut Vec<PulseEmission>,
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
        if mentioned.contains(&port.name) {
            continue;
        }
        emit_closure_on_outgoing(node_id, &port.name, color, frames, &outgoing, pulses, emissions);
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
    emit_closure_on_outgoing(node_id, port_name, color, frames, &outgoing, pulses, emissions);
    Ok(())
}

/// Shared closure-emit helper.
fn emit_closure_on_outgoing(
    node_id: &str,
    port_name: &str,
    color: Color,
    frames: &LoopFrames,
    outgoing: &[&Edge],
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
) {
    for edge in outgoing
        .iter()
        .filter(|e| e.source_handle.as_deref() == Some(port_name))
    {
        let target_handle = edge.target_handle.as_deref().unwrap_or("default");

        let already_present = pulses
            .get(&edge.target)
            .map(|ps| {
                ps.iter().any(|p| {
                    p.status.is_pending()
                        && p.color == color
                        && &p.frames == frames
                        && p.target_port == target_handle
                })
            })
            .unwrap_or(false);
        if already_present {
            continue;
        }

        let pulse = Pulse::closure(
            color,
            frames.clone(),
            edge.target.clone(),
            target_handle.to_string(),
        );
        emissions.push(PulseEmission {
            pulse: pulse.clone(),
            source_node: node_id.to_string(),
            source_port: port_name.to_string(),
        });
        pulses.entry(edge.target.clone()).or_default().push(pulse);
    }
}

#[cfg(test)]
mod fan_in_tests {
    use super::*;
    use crate::project::Edge;
    use serde_json::json;

    fn edge(source_handle: &str, target: &str, target_handle: &str) -> Edge {
        Edge {
            id: format!("{source_handle}-{target}-{target_handle}"),
            source: "src".into(),
            source_handle: Some(source_handle.into()),
            target: target.into(),
            target_handle: Some(target_handle.into()),
            span: None,
        }
    }

    #[test]
    fn same_value_fan_in_dedups_silently() {
        let outgoing = vec![edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let color = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        emit_single("src1", &outgoing_refs, "out", &json!(42), color, &frames, &mut pulses, &mut emissions);
        emit_single("src2", &outgoing_refs, "out", &json!(42), color, &frames, &mut pulses, &mut emissions);

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<_> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 1, "exactly one pending pulse after dedup");
    }

    #[test]
    fn data_and_existing_closure_coexist_and_data_wins_at_resolve_time() {
        let outgoing = vec![edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let color = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        emit_closure_on_outgoing(
            "src1", "out", color, &frames, &outgoing_refs, &mut pulses, &mut emissions,
        );
        emit_single(
            "src2", &outgoing_refs, "out", &json!(42), color, &frames, &mut pulses, &mut emissions,
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
        let outgoing = vec![edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let color = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        emit_single(
            "src1", &outgoing_refs, "out", &json!(42), color, &frames, &mut pulses, &mut emissions,
        );
        let emissions_before = emissions.len();
        emit_closure_on_outgoing(
            "src2", "out", color, &frames, &outgoing_refs, &mut pulses, &mut emissions,
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
            configurable: false,
            synthesized_from_carry: false,
        }
    }

    fn node(id: &str, outputs: Vec<&str>, inputs: Vec<&str>) -> serde_json::Value {
        let n = crate::project::NodeDefinition {
            id: id.into(),
            node_type: "Test".into(),
            label: None,
            config: Value::Null,
            position: crate::project::Position { x: 0.0, y: 0.0 },
            inputs: inputs.into_iter().map(port).collect(),
            outputs: outputs.into_iter().map(port).collect(),
            features: Default::default(),
            scope: Vec::new(),
            group_boundary: None,
            requires_infra: false,
            images: Vec::new(),
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
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
        let outgoing = vec![edge("out", "consumer", "in")];
        let outgoing_refs: Vec<&Edge> = outgoing.iter().collect();
        let mut pulses = PulseTable::default();
        let mut emissions = Vec::new();
        let color = uuid::Uuid::nil();
        let frames: LoopFrames = Vec::new();

        emit_closure_on_outgoing(
            "src1", "out", color, &frames, &outgoing_refs, &mut pulses, &mut emissions,
        );
        emit_closure_on_outgoing(
            "src2", "out", color, &frames, &outgoing_refs, &mut pulses, &mut emissions,
        );

        let consumer_bucket = pulses.get("consumer").expect("consumer bucket");
        let pending: Vec<_> = consumer_bucket
            .iter()
            .filter(|p| p.status.is_pending())
            .collect();
        assert_eq!(pending.len(), 1, "closure dedups silently against existing closure");
        assert!(pending[0].closed);
    }
}
