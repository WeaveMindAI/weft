//! Group boundaries. The compiler flattens a group into two
//! `Passthrough` nodes, `<group>__in` and `<group>__out`, wired like
//! any other node. A boundary is a pure function of what reaches it:
//! it forwards every input port to its same-named output (minus the
//! scope's gate, which it consumes), closes the outputs it received
//! nothing for, and, for an In boundary, kicks the scope's roots; a
//! gated-off In boundary closes the scope's outward surface and kicks
//! every member into a skip. Nothing about it is a fact the engine
//! learned from outside, so the journal never records a boundary
//! firing: the live engine and the journal fold both fire it from
//! this module, whenever its inputs are ready, and both hold the same
//! in-memory record afterwards (which is what readiness reads so the
//! boundary is not fired twice, and what the screen reads to paint
//! the group ran or skipped). Same record, with two documented
//! exceptions: the record's own `id` is local to each side (nothing
//! compares it across them), and its timestamps are the engine's
//! clock on one side and the row's `at_unix` on the other.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::exec::emission::{boundary_emission, PulseEmission};
use crate::exec::execution::{
    next_firing_ordinal, NodeExecution, NodeExecutionStatus, NodeExecutionTable,
};
use crate::exec::postprocess::{close_unmentioned_downstream, emit_port_closure, postprocess_output, OutputBag};
use crate::context::Phase;
use crate::exec::ready::{find_ready_among, kicked_group, settle_out_of_run, InputBag, OutOfRun, ReadyGroup};
use crate::exec::skip::{SkipReason, SHOULD_FLOW_PORT};
use crate::frames::{FiringLocation, LoopFrames};
use crate::primitive::KickedNode;
use crate::project::{
    boundary_out_id, scope_body_roots, scope_members, EdgeIndex, GroupBoundaryRole,
    NodeDefinition, ProjectDefinition,
};
use crate::pulse::{PulseStatus, PulseTable};
use crate::Color;

/// The compiler's node type for a group boundary.
pub const PASSTHROUGH: &str = "Passthrough";

pub fn is_passthrough(node: &NodeDefinition) -> bool {
    node.node_type == PASSTHROUGH
}

/// One boundary dispatch: what it absorbed, what it put on the wires,
/// and how its record ended.
pub struct BoundaryDispatch {
    pub node_id: String,
    pub frames: LoopFrames,
    pub color: Color,
    /// Every pulse this dispatch consumed (the stream-gate settling
    /// the live engine does per absorbed pulse reads these).
    pub absorbed: Vec<Uuid>,
    pub emissions: Vec<PulseEmission>,
    pub outcome: BoundaryOutcome,
}

pub enum BoundaryOutcome {
    /// The boundary is outside the part of the graph this execution
    /// runs: its pulses were absorbed silently and no record exists.
    OutOfScope,
    /// The boundary fired: its record is terminal with `status`, and
    /// the screen paints it from `input` (what reached it),
    /// `closed_ports`, and for a forwarding firing `output` (what it
    /// handed out, the same shared values).
    Fired {
        record_id: Uuid,
        status: NodeExecutionStatus,
        input: InputBag,
        closed_ports: Vec<String>,
        output: Option<OutputBag>,
        skip_reason: Option<SkipReason>,
        error: Option<String>,
    },
}

/// Fire every `Passthrough` whose inputs are ready (a pulse-driven
/// group at its exact frames, or a kick not yet dispatched), and keep
/// going while a firing makes another one ready (a group nested in a
/// group), until none is. THE one boundary pass: the live engine runs
/// it every scheduler turn before it dispatches ordinary nodes, and
/// the journal fold runs it after every row it applies, so both hold
/// the same boundary records without a journal row for any of them.
/// `dispatchable` is the run's node set (`None` = the whole graph),
/// `now` stamps the records.
#[allow(clippy::too_many_arguments)]
/// What one pass over the table did with no row behind it: see
/// [`settle_table`].
#[derive(Default)]
pub struct TablePass {
    /// The group boundaries that fired, in order.
    pub boundaries: Vec<BoundaryDispatch>,
    /// The pulses the run will never dispatch, settled.
    pub out_of_run: OutOfRun,
}

/// Everything that happens to the table without a row of its own, in
/// the one order both sides use: the group boundaries made ready fire
/// (`fire_ready_passthroughs`), then what the run never dispatches
/// settles (`settle_out_of_run`). The worker runs it at the top of
/// every turn and at the end of its cancel walk; the fold runs it
/// after every row, so the two hold the same table.
#[allow(clippy::too_many_arguments)]
pub fn settle_table(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    phase: Phase,
    dispatchable: Option<&HashSet<String>>,
    color: Color,
    now: u64,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    kicked: &mut HashMap<FiringLocation, KickedNode>,
) -> TablePass {
    let boundaries = fire_ready_passthroughs(project, edge_idx, dispatchable, color, now, pulses, executions, kicked);
    let out_of_run = settle_out_of_run(project, phase, dispatchable, pulses);
    TablePass { boundaries, out_of_run }
}

pub fn fire_ready_passthroughs(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    dispatchable: Option<&HashSet<String>>,
    color: Color,
    now: u64,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    kicked: &mut HashMap<FiringLocation, KickedNode>,
) -> Vec<BoundaryDispatch> {
    // Only a boundary can fire here, so readiness looks at the
    // boundaries alone: this pass runs after every journal row on the
    // fold side, and a whole-program scan there would cost a full
    // readiness walk per row.
    let boundaries: HashMap<&str, &NodeDefinition> =
        project.nodes.iter().filter(|n| is_passthrough(n)).map(|n| (n.id.as_str(), n)).collect();
    let mut fired = Vec::new();
    if boundaries.is_empty() {
        return fired;
    }
    loop {
        let mut ready: Vec<(&NodeDefinition, ReadyGroup)> = Vec::new();
        // Kicks first: a kicked boundary with a pulse-driven group at
        // the same location lets the pulses dispatch it (the kick is
        // consumed either way), matching the scheduler's kick rule.
        let pulse_driven =
            find_ready_among(project, boundaries.values().copied(), pulses, edge_idx, dispatchable);
        let mut covered: HashSet<FiringLocation> = HashSet::new();
        // Every group readiness forms is dispatched here, whatever its
        // color: a pulse table holds one execution, and a group this
        // pass declined would reach the ordinary dispatch loop, which
        // must never see a boundary.
        for (def, group) in pulse_driven {
            covered.insert(FiringLocation::new(def.id.clone(), group.frames.clone()));
            ready.push((def, group));
        }
        let mut kick_locations: Vec<FiringLocation> = kicked
            .iter()
            .filter(|(loc, info)| !info.dispatched && boundaries.contains_key(loc.node_id.as_str()))
            .map(|(loc, _)| loc.clone())
            .collect();
        kick_locations.sort_by(|a, b| a.node_id.cmp(&b.node_id).then(frames_key(&a.frames).cmp(&frames_key(&b.frames))));
        for loc in kick_locations {
            let def = boundaries[loc.node_id.as_str()];
            let info = kicked.get_mut(&loc).expect("listed from this map");
            info.dispatched = true;
            if covered.contains(&loc) {
                continue;
            }
            ready.push((def, kicked_group(def, info, &loc.frames, color)));
        }
        if ready.is_empty() {
            return fired;
        }
        // Deterministic order, so the live engine and the fold create
        // records (and their ordinals) in the same sequence.
        ready.sort_by(|(a, ga), (b, gb)| a.id.cmp(&b.id).then(frames_key(&ga.frames).cmp(&frames_key(&gb.frames))));
        for (def, group) in ready {
            fired.push(dispatch_passthrough(
                def, group, project, edge_idx, now, pulses, executions, kicked,
            ));
        }
    }
}

fn frames_key(frames: &LoopFrames) -> Vec<u32> {
    frames.iter().map(|f| f.index).collect()
}

/// Dispatch one ready `Passthrough` group. Absorbs its pulses, opens
/// its record, and runs the boundary: forward (an In boundary then
/// kicks the scope's roots), skip (a gated In boundary closes the
/// scope's outward ports and kicks every member into a skip), or fail
/// (a value a port refused closes every output). The record is
/// terminal on return.
#[allow(clippy::too_many_arguments)]
fn dispatch_passthrough(
    node_def: &NodeDefinition,
    group: ReadyGroup,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    now: u64,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    kicked: &mut HashMap<FiringLocation, KickedNode>,
) -> BoundaryDispatch {
    let node_id = node_def.id.clone();
    let color = group.color;
    let frames = group.frames.clone();
    // A boundary absorbs its whole group. It has no live feed, so
    // there is no generator port to leave pending (the compiler
    // refuses a stream-typed port on a GROUP boundary, its
    // `generator-through-group` rule; a loop boundary may carry one,
    // and dispatches through the ordinary node path, never here);
    // every pulse the group formed over is consumed here, or the
    // group would form again on the next iteration of the pass and
    // never let it end.
    let absorbed: Vec<Uuid> = group.pulse_ids.clone();
    if let Some(bucket) = pulses.get_mut(&node_id) {
        for p in bucket.iter_mut() {
            if absorbed.contains(&p.id) && p.status == PulseStatus::Pending {
                p.absorb();
            }
        }
    }
    if group.out_of_scope {
        return BoundaryDispatch {
            node_id,
            frames,
            color,
            absorbed,
            emissions: Vec::new(),
            outcome: BoundaryOutcome::OutOfScope,
        };
    }

    let ordinal = next_firing_ordinal(executions, &node_id, color, &frames);
    let emission_id = boundary_emission(color, &node_id, &frames, ordinal);
    let record_id = Uuid::new_v4();
    executions.entry(node_id.clone()).or_default().push(NodeExecution {
        id: record_id,
        node_id: node_id.clone(),
        status: NodeExecutionStatus::Running,
        pulses_absorbed: absorbed.clone(),
        ordinal,
        error: group.error.clone(),
        callback_id: None,
        started_at: now,
        completed_at: None,
        cost_usd: 0.0,
        logs: Vec::new(),
        port_warnings: Vec::new(),
        mentioned_ports: Default::default(),
        color,
        frames: frames.clone(),
    });
    let in_scope = node_def
        .group_boundary
        .as_ref()
        .filter(|gb| gb.role == GroupBoundaryRole::In)
        .map(|gb| gb.group_id.clone());

    let mut emissions = Vec::new();
    let mut output = None;
    let mut error = group.error.clone();
    let status = if let Some(reason) = &group.skip {
        if let Some(group_id) = &in_scope {
            emissions.extend(tear_down_gated_scope(
                project, edge_idx, pulses, kicked, emission_id, color, group_id, &frames, reason,
            ));
        }
        NodeExecutionStatus::Skipped
    } else if let Some(err) = &group.error {
        // A pre-dispatch failure (a port refused its value): nothing
        // was forwarded, so every output closes.
        sweep_all_outputs(&node_id, emission_id, color, &frames, project, edge_idx, pulses, &mut emissions, err);
        NodeExecutionStatus::Failed
    } else {
        // The scope's gate is consumed here, never forwarded: the In
        // boundary has no `_should_flow` output, and the children take
        // the scope's decision as a whole.
        let mut forwarded: OutputBag = group.input.clone().into_iter().collect();
        forwarded.remove(SHOULD_FLOW_PORT);
        match postprocess_output(
            &node_id, &forwarded, emission_id, color, &frames, project, pulses, edge_idx,
            &mut emissions,
        ) {
            Ok(mentioned) => {
                // Closed inputs are absent from the bag, so the sweep
                // closes their same-named outputs and skips cascade
                // through (and out of) the group.
                if let Err(e) = close_unmentioned_downstream(
                    &node_id, &mentioned, emission_id, color, &frames, project, pulses, edge_idx,
                    &mut emissions, None,
                ) {
                    tracing::error!(
                        target: "weft_core::exec::boundary",
                        node = %node_id,
                        error = %e,
                        "closure sweep failed; consumers of this boundary's unclosed ports \
                         will neither fire nor skip"
                    );
                }
                output = Some(forwarded);
                // The scope has started: everything inside it runs now,
                // wired to its edges or not. Its own roots are kicked at
                // the scope's frames.
                if let Some(group_id) = &in_scope {
                    let roots = scope_body_roots(project, edge_idx, group_id);
                    kick_scope(kicked, &roots, &frames, None);
                }
                NodeExecutionStatus::Completed
            }
            Err(e) => {
                let err = e.to_string();
                sweep_all_outputs(&node_id, emission_id, color, &frames, project, edge_idx, pulses, &mut emissions, &err);
                error = Some(err);
                NodeExecutionStatus::Failed
            }
        }
    };
    let record = executions
        .get_mut(&node_id)
        .and_then(|v| v.iter_mut().find(|e| e.id == record_id))
        .expect("pushed above");
    record.status = status.clone();
    record.completed_at = Some(now);
    record.error = error.clone();
    BoundaryDispatch {
        node_id,
        frames,
        color,
        absorbed,
        emissions,
        outcome: BoundaryOutcome::Fired {
            record_id,
            status,
            input: group.input,
            closed_ports: group.closed_ports,
            output,
            skip_reason: group.skip,
            error,
        },
    }
}

/// Close every output of a boundary that forwarded nothing.
#[allow(clippy::too_many_arguments)]
fn sweep_all_outputs(
    node_id: &str,
    emission_id: Uuid,
    color: Color,
    frames: &LoopFrames,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    emissions: &mut Vec<PulseEmission>,
    err: &str,
) {
    if let Err(e) = close_unmentioned_downstream(
        node_id, &HashSet::new(), emission_id, color, frames, project, pulses, edge_idx, emissions,
        Some(err),
    ) {
        tracing::error!(
            target: "weft_core::exec::boundary",
            node = %node_id,
            error = %e,
            "closure sweep failed; consumers of this boundary's unclosed ports will \
             neither fire nor skip"
        );
    }
}

/// A scope whose In boundary skipped with `reason` (a group's
/// Passthrough or a loop's LoopIn alike): its outward surface closes
/// at the scope's own frames, then every member (nested scopes
/// included) is kicked straight into a skip that says its scope did
/// not run. THE one teardown of a gated scope, shared by the boundary
/// pass, the engine's LoopIn skip and the journal fold's `NodeSkipped`
/// arm. A boundary that is itself inside a gated scope
/// (`ScopeSkipped`) owes nothing: every member of that scope got its
/// own skip from the scope's sweep, and the scope's outward closures
/// came with its own In boundary's skip.
#[allow(clippy::too_many_arguments)]
pub fn tear_down_gated_scope(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    kicked: &mut HashMap<FiringLocation, KickedNode>,
    emission_id: Uuid,
    color: Color,
    group_id: &str,
    frames: &LoopFrames,
    reason: &SkipReason,
) -> Vec<PulseEmission> {
    if matches!(reason, SkipReason::ScopeSkipped { .. }) {
        return Vec::new();
    }
    let emissions = close_scope_outward(project, edge_idx, pulses, emission_id, color, group_id, frames);
    let members: Vec<String> =
        scope_members(project, group_id).into_iter().map(|n| n.id.clone()).collect();
    kick_scope(kicked, &members, frames, Some(group_id));
    emissions
}

/// The closures a scope that never runs (or a loop that ends
/// abnormally) owes the outside: one per outward port of its Out
/// boundary, at the scope's own frames, so downstream skips cascade
/// instead of deadlocking. A group's and a loop's alike; the outward
/// surface is the Out node's outputs either way. A missing `__out`
/// node (impossible unless the compiled project shape is corrupt) is
/// logged at error level rather than returned, since teardown paths
/// cannot propagate.
#[allow(clippy::too_many_arguments)]
pub fn close_scope_outward(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    emission_id: Uuid,
    color: Color,
    group_id: &str,
    frames: &LoopFrames,
) -> Vec<PulseEmission> {
    let out_id = boundary_out_id(group_id);
    let Some(out_node) = project.nodes.iter().find(|n| n.id == out_id) else {
        tracing::error!(
            target: "weft_core::exec::boundary",
            group_id = %group_id,
            "scope teardown: project has no '{out_id}' node; outward consumers \
             will not receive closures (corrupt compiled project shape)"
        );
        return Vec::new();
    };
    let mut emissions = Vec::new();
    for port in &out_node.outputs {
        // Unreachable by construction (we iterate the node's own
        // declared outputs), but teardown cannot propagate, so log
        // loud rather than unwrap.
        if let Err(e) = emit_port_closure(
            &out_id, &port.name, emission_id, color, frames, project, pulses, edge_idx,
            &mut emissions,
        ) {
            tracing::error!(
                target: "weft_core::exec::boundary",
                group_id = %group_id,
                port = %port.name,
                error = %e,
                "scope teardown closure failed"
            );
        }
    }
    emissions
}

/// Kick `roots` at `frames`: one entry per root, first launch wins,
/// like every other kick. With `skipped_by`, the scope was gated off
/// and every kick dispatches straight into a `ScopeSkipped` skip. The
/// group launcher and the loop launcher both come through here.
pub fn kick_scope(
    kicked: &mut HashMap<FiringLocation, KickedNode>,
    roots: &[String],
    frames: &LoopFrames,
    skipped_by: Option<&str>,
) {
    for root in roots {
        kicked
            .entry(FiringLocation::new(root.clone(), frames.clone()))
            .or_insert_with(|| KickedNode {
                firing: false,
                payload: None,
                port_snapshot: None,
                dispatched: false,
                scope_skipped: skipped_by.map(str::to_string),
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::execution::NodeExecutionStatus;
    use crate::exec::postprocess::postprocess_output;
    use serde_json::json;
    use std::sync::Arc;

    /// A group `g` around one node `inner`: `src.out` feeds `g__in.x`
    /// (and its gate), `g__in.x` feeds `inner.in`, `inner.out` feeds
    /// `g__out.y`, `g__out.y` feeds `sink.in`. `lonely` sits in the
    /// group with no wire in (a scope root).
    fn grouped_project() -> ProjectDefinition {
        let node = |id: &str, ty: &str, inputs: Vec<serde_json::Value>, outputs: Vec<&str>, scope: Vec<&str>, boundary: serde_json::Value| {
            json!({
                "id": id, "nodeType": ty, "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 },
                "inputs": inputs,
                "outputs": outputs.iter().map(|o| json!({ "name": o, "portType": "Number", "required": true })).collect::<Vec<_>>(),
                "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false, "images": []
            })
        };
        let inp = |name: &str, required: bool| json!({ "name": name, "portType": "Number", "required": required });
        let gate = json!({ "name": SHOULD_FLOW_PORT, "portType": "Boolean", "required": false });
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", "Test", vec![], vec!["out", "flow"], vec![], json!(null)),
                node("g__in", "Passthrough", vec![inp("x", true), gate], vec!["x"], vec![], json!({ "groupId": "g", "role": "In" })),
                node("inner", "Test", vec![inp("in", true)], vec!["out"], vec!["g"], json!(null)),
                node("lonely", "Test", vec![], vec!["out"], vec!["g"], json!(null)),
                node("g__out", "Passthrough", vec![inp("y", true)], vec!["y"], vec![], json!({ "groupId": "g", "role": "Out" })),
                node("sink", "Test", vec![inp("in", true)], vec![], vec![], json!(null)),
            ],
            "edges": [
                { "id": "e0", "source": "src", "sourceHandle": "out", "target": "g__in", "targetHandle": "x" },
                { "id": "e1", "source": "src", "sourceHandle": "flow", "target": "g__in", "targetHandle": SHOULD_FLOW_PORT },
                { "id": "e2", "source": "g__in", "sourceHandle": "x", "target": "inner", "targetHandle": "in" },
                { "id": "e3", "source": "inner", "sourceHandle": "out", "target": "g__out", "targetHandle": "y" },
                { "id": "e4", "source": "g__out", "sourceHandle": "y", "target": "sink", "targetHandle": "in" }
            ],
            "groups": [],
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }))
        .expect("grouped project")
    }

    fn emit_src(project: &ProjectDefinition, edge_idx: &EdgeIndex, pulses: &mut PulseTable, flow: bool) {
        let mut bag = OutputBag::new();
        bag.insert("out".into(), Arc::new(json!(7)));
        bag.insert("flow".into(), Arc::new(json!(flow)));
        postprocess_output("src", &bag, Uuid::new_v4(), Uuid::nil(), &Vec::new(), project, pulses, edge_idx, &mut Vec::new())
            .expect("src emits");
    }

    fn pending<'a>(pulses: &'a PulseTable, node: &str) -> Vec<&'a crate::pulse::Pulse> {
        pulses.get(node).map(|b| b.iter().filter(|p| p.status.is_pending()).collect()).unwrap_or_default()
    }

    #[test]
    fn an_in_boundary_forwards_shares_the_value_and_kicks_the_scope_roots() {
        let project = grouped_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        emit_src(&project, &edge_idx, &mut pulses, true);
        let src_value = pending(&pulses, "g__in").iter().find(|p| p.target_port == "x").unwrap().value.clone();

        let fired = fire_ready_passthroughs(
            &project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked,
        );
        assert_eq!(fired.len(), 1);
        let BoundaryOutcome::Fired { status, output, closed_ports, .. } = &fired[0].outcome else {
            panic!("in scope")
        };
        assert_eq!(*status, NodeExecutionStatus::Completed);
        assert!(closed_ports.is_empty());
        let out = output.as_ref().expect("forwarded");
        assert!(out.contains_key("x") && !out.contains_key(SHOULD_FLOW_PORT), "the gate is consumed: {out:?}");
        let inner = pending(&pulses, "inner");
        assert_eq!(inner.len(), 1);
        assert!(Arc::ptr_eq(&inner[0].value, &src_value), "the forwarded value is the same allocation");
        assert!(pending(&pulses, "g__in").is_empty(), "the boundary's pulses are absorbed");
        let rec = &executions["g__in"][0];
        assert_eq!(rec.status, NodeExecutionStatus::Completed);
        assert_eq!(rec.started_at, 5);
        let lonely = kicked.get(&FiringLocation::new("lonely", Vec::new())).expect("scope root kicked");
        assert!(!lonely.dispatched && lonely.scope_skipped.is_none());
        assert!(!kicked.contains_key(&FiringLocation::new("inner", Vec::new())), "a wired member is not a root");
    }

    #[test]
    fn a_gated_off_in_boundary_closes_the_scope_and_kicks_every_member_into_a_skip() {
        let project = grouped_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        emit_src(&project, &edge_idx, &mut pulses, false);

        let fired = fire_ready_passthroughs(
            &project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked,
        );
        assert_eq!(fired.len(), 1);
        let BoundaryOutcome::Fired { status, skip_reason, output, .. } = &fired[0].outcome else {
            panic!("in scope")
        };
        assert_eq!(*status, NodeExecutionStatus::Skipped);
        assert_eq!(*skip_reason, Some(SkipReason::DidNotFlow));
        assert!(output.is_none());
        assert!(pending(&pulses, "inner").is_empty(), "nothing is forwarded into a gated scope");
        let sink = pending(&pulses, "sink");
        assert_eq!(sink.len(), 1);
        assert!(sink[0].closed, "the scope's outward port closes");
        for member in ["inner", "lonely"] {
            let kick = kicked.get(&FiringLocation::new(member, Vec::new())).unwrap_or_else(|| panic!("{member} kicked"));
            assert_eq!(kick.scope_skipped.as_deref(), Some("g"));
        }
        // The Out boundary lives in the enclosing scope, so it is not
        // a member: the closures above are what it owes the outside.
        assert!(!kicked.contains_key(&FiringLocation::new("g__out", Vec::new())));
    }

    /// The Out boundary forwards whatever came through; a second
    /// boundary made ready by the first fires in the same pass.
    #[test]
    fn the_pass_runs_to_a_fixpoint_and_the_out_boundary_forwards() {
        let project = grouped_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        emit_src(&project, &edge_idx, &mut pulses, true);
        fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked);
        // `inner` runs and emits into the Out boundary.
        let mut bag = OutputBag::new();
        bag.insert("out".into(), Arc::new(json!(8)));
        postprocess_output("inner", &bag, Uuid::new_v4(), Uuid::nil(), &Vec::new(), &project, &mut pulses, &edge_idx, &mut Vec::new()).unwrap();
        let fired = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 6, &mut pulses, &mut executions, &mut kicked);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].node_id, "g__out");
        let sink = pending(&pulses, "sink");
        assert_eq!(sink.len(), 1);
        assert_eq!(*sink[0].value, json!(8));
    }

    /// The pass over a table it already settled fires nothing and adds
    /// nothing: the property the fold rests on when a row is applied
    /// twice, and the reason the pass can run after every row.
    #[test]
    fn a_second_pass_over_a_settled_table_is_a_no_op() {
        let project = grouped_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        emit_src(&project, &edge_idx, &mut pulses, true);
        fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked);
        let ids: Vec<Uuid> = pulses["inner"].iter().map(|p| p.id).collect();
        let records = executions["g__in"].len();
        let again = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 6, &mut pulses, &mut executions, &mut kicked);
        assert!(again.is_empty(), "nothing is ready twice");
        assert_eq!(pulses["inner"].iter().map(|p| p.id).collect::<Vec<_>>(), ids);
        assert_eq!(executions["g__in"].len(), records);
    }

    #[test]
    fn an_out_of_scope_boundary_absorbs_silently() {
        let project = grouped_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        emit_src(&project, &edge_idx, &mut pulses, true);
        let scope: HashSet<String> = ["src".to_string()].into_iter().collect();
        let fired = fire_ready_passthroughs(&project, &edge_idx, Some(&scope), Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked);
        assert_eq!(fired.len(), 1);
        assert!(matches!(fired[0].outcome, BoundaryOutcome::OutOfScope));
        assert!(executions.is_empty(), "no record for an out-of-scope boundary");
        assert!(pending(&pulses, "g__in").is_empty());
        assert!(pending(&pulses, "inner").is_empty());
    }


    /// A group fed twice at one location fires twice: the second
    /// firing's record has the next ordinal and its pulses new ids.
    #[test]
    fn a_second_firing_at_one_location_gets_the_next_ordinal_and_new_ids() {
        let project = grouped_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        emit_src(&project, &edge_idx, &mut pulses, true);
        fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked);
        let first: Vec<Uuid> = pulses["inner"].iter().map(|p| p.id).collect();
        // `inner` takes the first delivery, then `src` fires again.
        for p in pulses.get_mut("inner").unwrap().iter_mut() {
            p.absorb();
        }
        emit_src(&project, &edge_idx, &mut pulses, true);
        let fired = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 6, &mut pulses, &mut executions, &mut kicked);
        assert_eq!(fired.len(), 1);
        let records = &executions["g__in"];
        assert_eq!(records.iter().map(|e| e.ordinal).collect::<Vec<_>>(), vec![0, 1]);
        let second: Vec<Uuid> = pending(&pulses, "inner").iter().map(|p| p.id).collect();
        assert_eq!(second.len(), 1);
        assert!(!first.contains(&second[0]), "a second firing's pulse is a new pulse");
    }

    /// A value a boundary's port refuses fails the boundary before it
    /// forwards: every output closes, the record is Failed with the
    /// reason, the scope's roots are not kicked.
    #[test]
    fn a_refused_value_fails_the_boundary_and_closes_every_output() {
        let project = grouped_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        let mut bag = OutputBag::new();
        bag.insert("out".into(), Arc::new(json!("not a number")));
        bag.insert("flow".into(), Arc::new(json!(true)));
        postprocess_output("src", &bag, Uuid::new_v4(), Uuid::nil(), &Vec::new(), &project, &mut pulses, &edge_idx, &mut Vec::new()).unwrap();
        let fired = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked);
        assert_eq!(fired.len(), 1);
        let BoundaryOutcome::Fired { status, error, output, .. } = &fired[0].outcome else { panic!("in scope") };
        assert_eq!(*status, NodeExecutionStatus::Failed);
        assert!(error.as_deref().is_some_and(|e| e.contains("'x'")), "{error:?}");
        assert!(output.is_none());
        let inner = pending(&pulses, "inner");
        assert_eq!(inner.len(), 1);
        assert!(inner[0].closed, "the refused port closes downstream");
        assert!(kicked.is_empty(), "a failed scope kicks nothing");
        assert_eq!(executions["g__in"][0].error, *error);
    }

    /// A group with no wire into its In boundary starts from a kick,
    /// and the kick is consumed by the pass.
    #[test]
    fn a_kicked_in_boundary_fires_from_its_kick() {
        let mut project = grouped_project();
        project.edges.retain(|e| e.target != "g__in");
        let g_in = project.nodes.iter_mut().find(|n| n.id == "g__in").unwrap();
        g_in.inputs.clear();
        g_in.outputs.clear();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        kick_scope(&mut kicked, &["g__in".to_string()], &Vec::new(), None);
        let fired = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked);
        assert_eq!(fired.len(), 1);
        assert_eq!(executions["g__in"][0].status, NodeExecutionStatus::Completed);
        assert!(kicked[&FiringLocation::new("g__in", Vec::new())].dispatched);
        assert!(kicked.contains_key(&FiringLocation::new("lonely", Vec::new())), "the scope's roots start");
        let again = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 6, &mut pulses, &mut executions, &mut kicked);
        assert!(again.is_empty(), "a consumed kick fires nothing more");
    }

    /// A gated-off scope's teardown reaches a nested group: the nested
    /// In boundary is kicked into its skip and closes nothing more (its
    /// scope's outward surface was closed by the outer boundary).
    #[test]
    fn a_gated_scope_tears_down_its_nested_group_once() {
        let node = |id: &str, ty: &str, inputs: Vec<serde_json::Value>, outputs: Vec<&str>, scope: Vec<&str>, boundary: serde_json::Value| {
            json!({
                "id": id, "nodeType": ty, "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 },
                "inputs": inputs,
                "outputs": outputs.iter().map(|o| json!({ "name": o, "portType": "Number", "required": true })).collect::<Vec<_>>(),
                "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false, "images": []
            })
        };
        let inp = |name: &str| json!({ "name": name, "portType": "Number", "required": true });
        let gate = json!({ "name": SHOULD_FLOW_PORT, "portType": "Boolean", "required": false });
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", "Test", vec![], vec!["out", "flow"], vec![], json!(null)),
                node("g__in", "Passthrough", vec![inp("x"), gate], vec!["x"], vec![], json!({ "groupId": "g", "role": "In" })),
                node("h__in", "Passthrough", vec![inp("x")], vec!["x"], vec!["g"], json!({ "groupId": "h", "role": "In" })),
                node("inner", "Test", vec![inp("in")], vec!["out"], vec!["g", "h"], json!(null)),
                node("h__out", "Passthrough", vec![inp("y")], vec!["y"], vec!["g"], json!({ "groupId": "h", "role": "Out" })),
                node("g__out", "Passthrough", vec![inp("y")], vec!["y"], vec![], json!({ "groupId": "g", "role": "Out" })),
                node("sink", "Test", vec![inp("in")], vec![], vec![], json!(null)),
            ],
            "edges": [
                { "id": "e0", "source": "src", "sourceHandle": "out", "target": "g__in", "targetHandle": "x" },
                { "id": "e1", "source": "src", "sourceHandle": "flow", "target": "g__in", "targetHandle": SHOULD_FLOW_PORT },
                { "id": "e2", "source": "g__in", "sourceHandle": "x", "target": "h__in", "targetHandle": "x" },
                { "id": "e3", "source": "h__in", "sourceHandle": "x", "target": "inner", "targetHandle": "in" },
                { "id": "e4", "source": "inner", "sourceHandle": "out", "target": "h__out", "targetHandle": "y" },
                { "id": "e5", "source": "h__out", "sourceHandle": "y", "target": "g__out", "targetHandle": "y" },
                { "id": "e6", "source": "g__out", "sourceHandle": "y", "target": "sink", "targetHandle": "in" }
            ],
            "groups": [],
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }))
        .expect("nested project");
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        emit_src(&project, &edge_idx, &mut pulses, false);
        let fired = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked);
        let ids: Vec<&str> = fired.iter().map(|f| f.node_id.as_str()).collect();
        assert_eq!(ids, vec!["g__in", "h__in", "h__out"], "the outer skip kicks the nested boundaries, which fire in the same pass");
        for nested in &fired[1..] {
            let BoundaryOutcome::Fired { skip_reason, .. } = &nested.outcome else { panic!("in scope") };
            assert_eq!(*skip_reason, Some(SkipReason::ScopeSkipped { scope: "g".into() }), "{}", nested.node_id);
            assert!(nested.emissions.is_empty(), "{} closes nothing more", nested.node_id);
        }
        assert_eq!(pending(&pulses, "sink").len(), 1, "the outer surface closed once");
        assert!(pending(&pulses, "g__out").is_empty(), "the nested Out boundary's wire is not closed by the outer teardown");
        assert_eq!(kicked[&FiringLocation::new("inner", Vec::new())].scope_skipped.as_deref(), Some("g"));
    }
}
