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
use std::sync::Arc;

use uuid::Uuid;

use crate::exec::emission::{boundary_emission, PulseEmission};
use crate::exec::execution::{
    next_firing_ordinal, NodeExecution, NodeExecutionStatus, NodeExecutionTable,
};
use crate::exec::postprocess::{close_unmentioned_downstream, emit_port_closure, postprocess_output, OutputBag};
use crate::primitive::Phase;
use crate::exec::ready::{find_ready_among, kicked_group, settle_out_of_run, InputBag, OutOfRun, ReadyGroup};
use crate::exec::skip::{SkipReason, SHOULD_FLOW_PORT};
use crate::frames::{FiringLocation, Located, LoopFrames};
use crate::primitive::KickedNode;
use crate::project::{
    boundary_in_id, boundary_out_id, scope_body_roots, scope_members, EdgeIndex, GroupBoundaryRole,
    NodeDefinition, ProjectDefinition,
};
use crate::pulse::{Pulse, PulseStatus, PulseTable};
use crate::Color;

/// The compiler's node type for a group boundary.
pub const PASSTHROUGH: &str = crate::project::boundary_types::PASSTHROUGH;

/// Whether the call site `site` calls the shared body `body`.
fn site_calls_body(project: &ProjectDefinition, site: &str, body: &str) -> bool {
    project.groups.iter().any(|g| g.id == site
        && matches!(&g.kind, crate::project::GroupKind::Call { body: called } if called == body))
}

/// A boundary the pass fires: a group's, a call site's or a shared
/// body's (see `boundary_types::is_forwarding`). A loop's halves are
/// not among them; the engine drives those.
pub fn is_passthrough(node: &NodeDefinition) -> bool {
    crate::project::boundary_types::is_forwarding(&node.node_type)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopePermission {
    Pending,
    Allowed,
    Skipped(String),
}

/// A kick and an arriving value obey the same enclosing group gates.
pub fn scope_permission(
    project: &ProjectDefinition,
    node: &NodeDefinition,
    frames: &LoopFrames,
    executions: &NodeExecutionTable,
) -> ScopePermission {
    // `depth` is how many frames of the stack the scopes walked so far
    // account for. A loop opens one per iteration and is gated by its
    // own machinery, so it is skipped here. A shared body is a scope of
    // its own at the top level, called from anywhere: its members'
    // frames carry the caller's frames first (loops and calls the scope
    // chain knows nothing about), then the call frame its site pushed.
    // The body's In fired under exactly that prefix, so the gate is
    // looked up at the innermost call frame whose site calls this body,
    // and the walk continues from there.
    let mut depth = 0;
    for scope in &node.scope {
        let boundary = project.nodes.iter().find(|n| n.id == boundary_in_id(scope))
            .expect("compiled node scope has an In boundary");
        if boundary.node_type == crate::project::boundary_types::LOOP_IN {
            depth += 1;
            continue;
        }
        if boundary.node_type == crate::project::boundary_types::INCLUDE_IN {
            let Some(call) = (depth..frames.len()).rev().find(|&i| {
                frames[i].call_site().is_some_and(|site| site_calls_body(project, site, scope))
            }) else { return ScopePermission::Pending };
            depth = call + 1;
        }
        let Some(gate_frames) = frames.get(..depth) else { return ScopePermission::Pending };
        let Some(record) = executions.get(&boundary_in_id(scope))
            .and_then(|records| records.iter().rev().find(|r| r.frames == gate_frames))
        else { return ScopePermission::Pending };
        match record.status {
            NodeExecutionStatus::Completed => {}
            NodeExecutionStatus::Skipped | NodeExecutionStatus::Failed | NodeExecutionStatus::Cancelled => return ScopePermission::Skipped(scope.clone()),
            _ => return ScopePermission::Pending,
        }
    }
    ScopePermission::Allowed
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
    dispatchable: Option<&HashSet<Located>>,
    color: Color,
    now: u64,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    kicked: &mut HashMap<FiringLocation, KickedNode>,
) -> TablePass {
    let mut boundaries = Vec::new();
    loop {
        let mut changed = settle_supplied_gates(project, edge_idx, pulses, executions);
        changed |= settle_input_streams(project, edge_idx, color, pulses, executions, kicked);
        let fired = fire_ready_passthroughs(project, edge_idx, dispatchable, color, now, pulses, executions, kicked);
        let settled = fired.is_empty();
        boundaries.extend(fired);
        if settled && !changed { break; }
    }
    let out_of_run = settle_out_of_run(project, phase, dispatchable, pulses);
    TablePass { boundaries, out_of_run }
}

/// Materialize an absent or cleanly empty input stream through the ordinary
/// pulse table. Absorbed real items remain evidence that a backup cannot run.
/// Both the driver and journal derive these same identities before routing.
fn settle_input_streams(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    color: Color,
    pulses: &mut PulseTable,
    executions: &NodeExecutionTable,
    kicked: &HashMap<FiringLocation, KickedNode>,
) -> bool {
    let Some(selection) = edge_idx.selection() else { return false };
    let mut changed = false;
    for node in project.nodes.iter().filter(|n| selection.nodes.iter().any(|place| place.id == n.id)) {
        let ports = crate::exec::ready::generator_inputs(node);
        if ports.is_empty() { continue; }
        let mut frames: HashSet<LoopFrames> = pulses.get(&node.id).into_iter().flatten()
            .filter(|p| p.color == color).map(|p| p.frames.clone()).collect();
        frames.extend(kicked.keys().filter(|loc| loc.node_id == node.id).map(|loc| loc.frames.clone()));
        // A node under no loop fires once per place it is at, whether or
        // not anything has reached it there yet.
        if node.scope.iter().all(|scope| project.nodes.iter()
            .find(|n| n.id == boundary_in_id(scope)).is_some_and(|n| !crate::project::boundary_types::opens_frame(&n.node_type)))
        { frames.extend(selection.nodes.iter().filter(|place| place.id == node.id).map(Located::frames)); }
        for frames in frames {
            let at = Located::at(&node.id, &frames);
            if !selection.nodes.contains(&at) { continue; }
            if scope_permission(project, node, &frames, executions) != ScopePermission::Allowed { continue; }
            for port in &ports {
                if pulses.stream_was_consumed(color, &node.id, port, &frames) { continue; }
                let bucket = pulses.entry(node.id.clone()).or_default();
                let history: Vec<_> = bucket.iter().filter(|p|
                    p.color == color && p.frames == frames && p.target_port == *port).collect();
                if history.iter().any(|p| p.backup || !p.closed || p.close_error.is_some()) { continue; }
                let ended = history.iter().any(|p| p.closed);
                if !ended && selection.has_supplier(project, &at, port) { continue; }
                let backup = selection.input.get(&at).and_then(|values| values.get(*port));
                let origin = selection.input_origins.get(&at).and_then(|ports| ports.get(*port)).copied();
                if ended && backup.is_none() { continue; }
                let identity = serde_json::to_vec(&(&node.id, port, &frames)).expect("stream location serializes");
                let base = Uuid::new_v5(&color, &identity);
                let end_id = Uuid::new_v5(&base, b"backup-end");
                if history.iter().any(|p| p.id == end_id) { continue; }
                for pulse in bucket.iter_mut().filter(|p|
                    p.color == color && p.frames == frames && p.target_port == *port && p.closed)
                { pulse.absorb(); }
                if let Some(value) = backup {
                    let items = value.as_array().expect("resolved generator backup is an item list");
                    for (index, value) in items.iter().enumerate() {
                        let id = Uuid::new_v5(&base, &(index as u64).to_be_bytes());
                        let mut pulse = Pulse::new(id, color, frames.clone(), &node.id, *port, Arc::new(value.clone()));
                        pulse.provided = true;
                        pulse.backup = true;
                        pulse.inherited_from = origin;
                        bucket.push(pulse);
                    }
                }
                let mut end = Pulse::closure(end_id, color, frames.clone(), &node.id, *port);
                end.provided = backup.is_some();
                end.backup = backup.is_some();
                end.inherited_from = origin;
                bucket.push(end);
                changed = true;
            }
        }
    }
    changed
}

fn settle_supplied_gates(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &NodeExecutionTable,
) -> bool {
    let mut changed = false;
    for pulse in pulses.values_mut().flatten().filter(|p| p.provided && !p.backup && matches!(p.status, PulseStatus::Pending | PulseStatus::Gated)) {
        let edge = edge_idx.get_incoming(project, &pulse.target_node, &pulse.frames).into_iter()
            .find(|edge| edge.target_handle.as_deref().unwrap_or("default") == pulse.target_port)
            .expect("a supplied pulse belongs to an original selected wire");
        let source = project.nodes.iter().find(|node| node.id == edge.source).expect("wire source exists");
        let next = match scope_permission(project, source, &pulse.frames, executions) {
            ScopePermission::Pending => PulseStatus::Gated,
            ScopePermission::Allowed => PulseStatus::Pending,
            ScopePermission::Skipped(_) if pulse.closed => PulseStatus::Pending,
            ScopePermission::Skipped(_) => {
                if source.outputs.iter().any(|port| port.name == edge.source_handle.as_deref().unwrap_or("default")
                    && matches!(port.port_type, crate::weft_type::WeftType::Generator(_)))
                {
                    PulseStatus::Absorbed
                } else {
                    pulse.closed = true;
                    pulse.value = std::sync::Arc::new(serde_json::Value::Null);
                    pulse.id = Uuid::new_v5(&pulse.id, b"scope-skipped");
                    changed = true;
                    PulseStatus::Pending
                }
            }
        };
        changed |= pulse.status != next;
        pulse.status = next;
    }
    changed
}

pub fn fire_ready_passthroughs(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    dispatchable: Option<&HashSet<Located>>,
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
        for (def, mut group) in pulse_driven {
            match scope_permission(project, def, &group.frames, executions) {
                ScopePermission::Pending => continue,
                ScopePermission::Allowed => {}
                ScopePermission::Skipped(scope) => group.skip = Some(SkipReason::ScopeSkipped { scope }),
            }
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
            if dispatchable.is_some_and(|s| !s.contains(&Located::at(&def.id, &loc.frames))) {
                kicked.get_mut(&loc).expect("listed from this map").dispatched = true;
                continue;
            }
            let permission = scope_permission(project, def, &loc.frames, executions);
            if permission == ScopePermission::Pending { continue; }
            let info = kicked.get_mut(&loc).expect("listed from this map");
            if let ScopePermission::Skipped(scope) = permission { info.scope_skipped = Some(scope); }
            info.dispatched = true;
            if covered.contains(&loc) {
                continue;
            }
            ready.push((def, kicked_group(def, info, &loc.frames, color, project, edge_idx)));
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

fn frames_key(frames: &LoopFrames) -> String {
    crate::frames::frames_text(frames)
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
    let emission_id = boundary_emission(&node_id, &frames, ordinal);
    let record_id = Uuid::new_v4();
    executions.entry(node_id.clone()).or_default().push(NodeExecution {
        id: record_id,
        received: group.received.clone(),
        skip_reason: group.skip.clone(),
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
        mentioned_ports: Default::default(),
        closed_output_ports: Default::default(),
        color,
        frames: frames.clone(),
        inherited_from: None,
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
            emissions.extend(tear_down_scope(
                project, edge_idx, pulses, kicked, emission_id, color, group_id, &frames, Some(reason),
            ));
        }
        NodeExecutionStatus::Skipped
    } else if let Some(err) = &group.error {
        // A pre-dispatch failure (a port refused its value): nothing
        // was forwarded, so every output closes.
        sweep_all_outputs(&node_id, emission_id, color, &frames, project, edge_idx, pulses, &mut emissions, err);
        // And the scope never starts, which the INSIDE has to be told
        // as well. Closing only the In boundary's own outputs left
        // every scope root (a member no wire feeds) never started,
        // never skipped and never closed, so anything the Out boundary
        // fed from that branch waited for a value that was never
        // coming and the run ended Stuck. This is the same teardown the
        // gated-off path does; only the reason differs.
        if let Some(group_id) = &in_scope {
            emissions.extend(tear_down_scope(
                project, edge_idx, pulses, kicked, emission_id, color, group_id, &frames, None,
            ));
        }
        NodeExecutionStatus::Failed
    } else {
        // The scope's gate is consumed here, never forwarded: the In
        // boundary has no `_should_flow` output, and the children take
        // the scope's decision as a whole.
        let mut forwarded: OutputBag = group.received.input.clone().into_iter().collect();
        forwarded.remove(SHOULD_FLOW_PORT);
        forwarded.remove(crate::exec::skip::SHOULD_NOT_FLOW_PORT);
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
                    &mut emissions, None, &HashSet::new(),
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
                    let roots = scope_body_roots(project, edge_idx, group_id, &frames);
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
    for emission in &mut emissions {
        emission.pulse.provided = group.received.provided_ports.contains(&emission.source_port);
        emission.pulse.inherited_from = group.received.inherited_ports.get(&emission.source_port).copied();
        if let Some(pulse) = pulses.get_mut(&emission.pulse.target_node)
            .and_then(|bucket| bucket.iter_mut().find(|pulse| pulse.id == emission.pulse.id))
        {
            pulse.provided = emission.pulse.provided;
            pulse.inherited_from = emission.pulse.inherited_from;
        }
    }
    BoundaryDispatch {
        node_id,
        frames,
        color,
        absorbed,
        emissions,
        outcome: BoundaryOutcome::Fired {
            record_id,
            status,
            input: group.received.input,
            closed_ports: group.received.closed_ports,
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
        Some(err), &HashSet::new(),
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
pub fn tear_down_scope(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    kicked: &mut HashMap<FiringLocation, KickedNode>,
    emission_id: Uuid,
    color: Color,
    group_id: &str,
    frames: &LoopFrames,
    reason: Option<&SkipReason>,
) -> Vec<PulseEmission> {
    // A call site holds no member of its own: the scope it gates is the
    // body it calls, one call frame deeper, boundaries included.
    let body = project.groups.iter().find(|g| g.id == group_id).and_then(|g| match &g.kind {
        crate::project::GroupKind::Call { body } => Some(body.clone()),
        _ => None,
    });
    // An enclosing scope that was itself skipped has already torn this
    // one down on its way past: its members are the enclosing scope's
    // members too, and its Out's outward closures went out with them.
    // Doing it again would emit a second closure on the same ports. A
    // FAILED boundary (`reason: None`) has no such enclosing pass, so it
    // always tears down. A call site is the exception: its body is
    // compiled at the top level, so no enclosing sweep reaches the
    // body's nodes, and the site takes them down itself under the call
    // (its own outward closures are still the enclosing pass's).
    let taken_down_above = matches!(reason, Some(SkipReason::ScopeSkipped { .. }));
    if taken_down_above && body.is_none() {
        return Vec::new();
    }
    let mut emissions = if taken_down_above { Vec::new() } else {
        close_scope_outward(project, edge_idx, pulses, emission_id, color, group_id, frames)
    };
    let (scope, frames): (String, LoopFrames) = match body {
        Some(body) => {
            let mut inside = frames.clone();
            inside.push(crate::frames::Frame::Call { site: group_id.to_string() });
            (body, inside)
        }
        None => (group_id.to_string(), frames.clone()),
    };
    let frames = &frames;
    let mut members: Vec<&NodeDefinition> = scope_members(project, &scope);
    if scope != group_id {
        members.extend(project.nodes.iter().filter(|n| n.id == boundary_in_id(&scope) || n.id == boundary_out_id(&scope)));
    }
    let mut exits = edge_idx.selection().cloned()
        .unwrap_or_else(|| crate::project::selection::RunSelection::whole(project));
    // The exits: wires leaving the scope, minus the ones into its own
    // Out and, for a call, into the site's Out (both were closed
    // outward already; a closure there would fire the Out a second
    // time and record a refused call's exit as completed). Leaving is
    // decided by PLACE: the nodes of a file included by a site inside
    // this scope are compiled at the top level, so by id alone a wire
    // into that body looked like an exit, and the body's In woke up on
    // the closure and completed before the site's own skip took the
    // body down, leaving one node with two records.
    let inside: HashSet<Located> = crate::project::selection::members_with_paths(project, &scope,
        &crate::frames::call_path(frames).into_iter().map(str::to_string).collect::<Vec<_>>()).into_iter().collect();
    exits.edges.retain(|wire| crate::project::selection::wire_ends(project, wire).is_some_and(|(_, target)|
        target.id != boundary_out_id(&scope) && target.id != boundary_out_id(group_id) && !inside.contains(&target)));
    let exit_index = EdgeIndex::selected(project, exits);
    let members: Vec<&NodeDefinition> = members.into_iter()
        .filter(|node| edge_idx.admits(&node.id, frames)).collect();
    for member in &members {
        if let Err(error) = close_unmentioned_downstream(&member.id, &HashSet::new(), emission_id,
            color, frames, project, pulses, &exit_index, &mut emissions, None, &HashSet::new())
        {
            tracing::error!(node = %member.id, %error, "scope member closure failed");
        }
    }
    // EVERY member, for a failure exactly as for a gating, in every
    // phase: a refused scope skips everything under it, a trigger setup,
    // an infra setup, a fired run and a manual run alike.
    //
    // A skip carrying `ScopeSkipped` emits no closures (there is
    // nothing to close: the scope never ran), so the members must be
    // told directly. Kicking only the roots nothing feeds, on the
    // theory that the boundary's own closure sweep reaches the rest,
    // does not hold: a member fed by one of those roots receives
    // nothing at all and waits for ever, and an In boundary of a NESTED
    // group treats closed inputs as "still start the scope", so the
    // nested body would run inside a scope whose entry had failed.
    //
    // Triggers are members like any other here. A scope START never
    // kicks a trigger (`scope_body_roots`: a trigger is kicked by its
    // fire, or payload-less by a manual run), but a scope REFUSAL is the
    // only thing that can still give a trigger inside it a record: its
    // own kick, when there is one, waits on this gate, and without this
    // it would wait for ever. A trigger the run fires keeps its fire:
    // `kick_scope` only stamps the scope's verdict onto a kick that is
    // already there, and a fire that lands later is refused the same
    // way when it dispatches (`scope_permission` reads the gate's
    // record).
    let members: Vec<String> = members.into_iter().map(|n| n.id.clone()).collect();
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
            &mut emissions, None,
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
/// and every kick dispatches straight into a `ScopeSkipped` skip; a
/// kick already there (a trigger's fire) keeps its payload and takes
/// the verdict. The group launcher, the loop launcher and the teardown
/// of a refused scope all come through here.
pub fn kick_scope(
    kicked: &mut HashMap<FiringLocation, KickedNode>,
    roots: &[String],
    frames: &LoopFrames,
    skipped_by: Option<&str>,
) {
    for root in roots {
        // First writer wins, and nothing ever removes a kick.
        //
        // That means a location kicked once cannot be kicked again, so a
        // scope that fires TWICE at one location (a group fed twice at
        // the same frames) does not re-kick the body roots it kicked the
        // first time. That is a real limitation of this table, it
        // predates the teardown below, and it is NOT worked around here:
        // an earlier attempt keyed "this kick is spent" off the
        // `dispatched` flag, which the engine sets when it schedules and
        // the fold sets when it applies `NodeStarted`. Those are
        // different moments, so live and replay disagreed about whether
        // a slot was spent, and a replacement also wiped a firing
        // trigger's wake payload and port snapshot. Both are worse than
        // the limitation. See the note in `tear_down_scope`.
        kicked
            .entry(FiringLocation::new(root.clone(), frames.clone()))
            .and_modify(|kick| {
                if let Some(scope) = skipped_by { kick.scope_skipped = Some(scope.into()); }
            })
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
    fn input_stream_backups_wait_and_replay_once_after_a_clean_empty_end() {
        let mut project = grouped_project();
        let sink = project.nodes.iter_mut().find(|n| n.id == "sink").unwrap();
        sink.inputs[0].port_type = crate::weft_type::WeftType::Generator(Box::new(sink.inputs[0].port_type.clone()));
        let mut selection = crate::project::selection::RunSelection::whole(&project);
        selection.input.entry(Located::top("sink")).or_default().insert("in".into(), json!([1, 2]));
        let index = EdgeIndex::selected(&project, selection);
        let mut pulses = PulseTable::new();
        let executions = NodeExecutionTable::new();
        let kicked = HashMap::new();
        assert!(!settle_input_streams(&project, &index, Uuid::nil(), &mut pulses, &executions, &kicked));
        let end = Pulse::closure(Uuid::new_v4(), Uuid::nil(), vec![], "sink", "in");
        pulses.insert("sink".into(), vec![end.clone()]);
        assert!(settle_input_streams(&project, &index, Uuid::nil(), &mut pulses, &executions, &kicked));
        let supplied = pending(&pulses, "sink");
        assert_eq!(supplied.iter().map(|p| p.value.as_ref().clone()).collect::<Vec<_>>(), vec![json!(1), json!(2), json!(null)]);
        assert!(supplied[2].closed);
        assert!(supplied.iter().all(|p| p.backup));
        let ids: Vec<_> = supplied.iter().map(|p| p.id).collect();
        let mut replay = PulseTable::from([("sink".into(), vec![end])]);
        assert!(settle_input_streams(&project, &index, Uuid::nil(), &mut replay, &executions, &kicked));
        assert_eq!(pending(&replay, "sink").iter().map(|p| p.id).collect::<Vec<_>>(), ids);
        pulses.remove_consumed("sink", &ids);
        assert!(!settle_input_streams(&project, &index, Uuid::nil(), &mut pulses, &executions, &kicked));
        assert!(pending(&pulses, "sink").is_empty());

        for failed in [false, true] {
            let mut real = Pulse::new(Uuid::new_v4(), Uuid::nil(), vec![], "sink", "in", Arc::new(json!(9)));
            real.absorb();
            let end = Pulse::closure_with_error(Uuid::new_v4(), Uuid::nil(), vec![], "sink", "in", failed.then(|| "producer failed".into()));
            let mut history = if failed { vec![end] } else { vec![real, end] };
            pulses.insert("sink".into(), std::mem::take(&mut history));
            assert!(!settle_input_streams(&project, &index, Uuid::nil(), &mut pulses, &executions, &kicked));
            assert!(!pulses["sink"].iter().any(|p| p.backup));
        }
    }

    #[test]
    fn simulated_output_waits_for_its_source_group_and_closes_when_false() {
        for flow in [false, true] {
            let mut project = grouped_project();
            project.edges.push(serde_json::from_value(json!({"id":"direct", "source":"lonely", "sourceHandle":"out", "target":"sink", "targetHandle":"in"})).unwrap());
            project.edges.retain(|edge| edge.id != "e4");
            let mut selection = crate::project::selection::RunSelection::restricted(&project,
                ["sink", "g__in", "src"].into_iter().map(Located::top).collect()).unwrap();
            selection.suppliers.insert(Located::top("lonely"));
            selection.edges.insert(Located::top("direct"));
            let dispatchable = selection.dispatchable_nodes();
            let index = EdgeIndex::selected(&project, selection);
            let mut supplied = Pulse::new(Uuid::new_v4(), Uuid::nil(), vec![], "sink", "in", Arc::new(json!(42)));
            supplied.provided = true;
            let mut pulses = PulseTable::from([("sink".into(), vec![supplied])]);
            let mut executions = NodeExecutionTable::new();
            let mut kicked = HashMap::new();
            settle_table(&project, &index, Phase::Fire, Some(&dispatchable), Uuid::nil(), 0,
                &mut pulses, &mut executions, &mut kicked);
            assert!(pending(&pulses, "sink").is_empty());
            assert_eq!(pulses["sink"][0].status, PulseStatus::Gated);
            emit_src(&project, &index, &mut pulses, flow);
            settle_table(&project, &index, Phase::Fire, Some(&dispatchable), Uuid::nil(), 0,
                &mut pulses, &mut executions, &mut kicked);
            let delivered = pending(&pulses, "sink");
            assert!(!delivered.is_empty());
            assert!(delivered.iter().all(|p| p.closed != flow));
            assert!(!executions.contains_key("lonely"));
            assert!(!executions.contains_key("inner"));
        }
    }

    #[test]
    fn selected_trigger_waits_for_group_and_false_gate_keeps_its_wake_fact() {
        let mut project = grouped_project();
        project.nodes.iter_mut().find(|n| n.id == "lonely").unwrap().features.is_trigger = true;
        let selection = crate::project::selection::RunSelection::restricted(&project,
            [Located::top("lonely")].into_iter().collect()).unwrap();
        let dispatchable = selection.dispatchable_nodes();
        let index = EdgeIndex::selected(&project, selection);
        let trigger = project.nodes.iter().find(|n| n.id == "lonely").unwrap();
        let mut executions = NodeExecutionTable::new();
        assert_eq!(scope_permission(&project, trigger, &vec![], &executions), ScopePermission::Pending);
        let loc = FiringLocation::new("lonely", vec![]);
        let wake = json!({"event": "wake"});
        let mut kicked = HashMap::from([(loc.clone(), KickedNode {
            firing: true, payload: Some(wake.clone()), port_snapshot: Some(json!({})),
            dispatched: false, scope_skipped: None,
        })]);
        let mut pulses = PulseTable::new();
        emit_src(&project, &index, &mut pulses, false);
        settle_table(&project, &index, Phase::Fire, Some(&dispatchable), Uuid::nil(), 0,
            &mut pulses, &mut executions, &mut kicked);
        assert_eq!(scope_permission(&project, trigger, &vec![], &executions), ScopePermission::Skipped("g".into()));
        assert_eq!(kicked[&loc].payload, Some(wake));
        assert!(kicked[&loc].firing);
        assert_eq!(kicked[&loc].scope_skipped.as_deref(), Some("g"));
        assert!(!kicked.keys().any(|loc| loc.node_id == "inner"));
        assert!(!executions.contains_key("inner"));
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
    /// Two call sites `a` and `b` of one shared body `B` (one member
    /// `B.n`), each fed by `src` and each feeding its own sink. What the
    /// two calls share is the body's nodes; what keeps them apart is the
    /// call frame each site pushes.
    fn called_project() -> ProjectDefinition {
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
        let edge = |s: &str, sp: &str, t: &str, tp: &str| json!({ "id": format!("{s}.{sp}->{t}.{tp}"), "source": s, "sourceHandle": sp, "target": t, "targetHandle": tp });
        use crate::project::boundary_types as bt;
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", "Test", vec![], vec!["a", "b"], vec![], json!(null)),
                node("a__in", bt::CALL_IN, vec![inp("x")], vec!["x"], vec![], json!({ "groupId": "a", "role": "In" })),
                node("a__out", bt::CALL_OUT, vec![inp("y")], vec!["y"], vec![], json!({ "groupId": "a", "role": "Out" })),
                node("b__in", bt::CALL_IN, vec![inp("x")], vec!["x"], vec![], json!({ "groupId": "b", "role": "In" })),
                node("b__out", bt::CALL_OUT, vec![inp("y")], vec!["y"], vec![], json!({ "groupId": "b", "role": "Out" })),
                node("B__in", bt::INCLUDE_IN, vec![inp("x")], vec!["x"], vec![], json!({ "groupId": "B", "role": "In" })),
                node("B.n", "Test", vec![inp("in")], vec!["out"], vec!["B"], json!(null)),
                node("B__out", bt::INCLUDE_OUT, vec![inp("y")], vec!["y"], vec![], json!({ "groupId": "B", "role": "Out" })),
                node("sa", "Test", vec![inp("in")], vec![], vec![], json!(null)),
                node("sb", "Test", vec![inp("in")], vec![], vec![], json!(null)),
            ],
            "edges": [
                edge("src", "a", "a__in", "x"), edge("src", "b", "b__in", "x"),
                edge("a__in", "x", "B__in", "x"), edge("b__in", "x", "B__in", "x"),
                edge("B__in", "x", "B.n", "in"), edge("B.n", "out", "B__out", "y"),
                edge("B__out", "y", "a__out", "y"), edge("B__out", "y", "b__out", "y"),
                edge("a__out", "y", "sa", "in"), edge("b__out", "y", "sb", "in"),
            ],
            "groups": [
                { "id": "a", "kind": "call", "body": "B", "nodeIds": [] },
                { "id": "b", "kind": "call", "body": "B", "nodeIds": [] },
                { "id": "B", "kind": "body", "nodeIds": ["B.n"] }
            ],
            "createdAt": "1970-01-01T00:00:00Z",
            "updatedAt": "1970-01-01T00:00:00Z",
        }))
        .expect("called project")
    }

    /// A call site's In pushes the site's frame onto what it forwards, so
    /// the shared body fires once per site, each at its own frame; the
    /// body's Out pops the frame and answers only the site it names.
    /// A site whose In skips takes the body down under its call: the
    /// body's nodes and boundaries are kicked into a skip one frame
    /// deeper, and the other site's call is untouched.
    #[test]
    fn a_skipped_call_site_takes_its_body_down_under_the_call() {
        use crate::frames::Frame;
        let project = called_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut kicked = HashMap::new();
        let emissions = tear_down_scope(&project, &edge_idx, &mut pulses, &mut kicked, Uuid::new_v4(), Uuid::nil(), "a", &vec![], Some(&SkipReason::DidNotFlow));
        let at = vec![Frame::Call { site: "a".into() }];
        for member in ["B__in", "B.n", "B__out"] {
            let kick = kicked.get(&FiringLocation::new(member, at.clone())).unwrap_or_else(|| panic!("{member} kicked under the call: {kicked:?}"));
            assert_eq!(kick.scope_skipped.as_deref(), Some("a"));
        }
        assert!(kicked.keys().all(|loc| loc.frames == at), "nothing kicked at another call: {kicked:?}");
        assert!(emissions.iter().any(|e| e.pulse.target_node == "sa" && e.pulse.closed), "the site's Out closes outward: {emissions:?}");
        assert!(!emissions.iter().any(|e| e.pulse.target_node == "a__out"), "the site's Out is closed outward, never fed a closure of its own: {emissions:?}");
        // A site skipped by an enclosing scope still takes its body down,
        // one call deeper, and closes nothing outward a second time.
        let mut kicked = HashMap::new();
        let emissions = tear_down_scope(&project, &edge_idx, &mut pulses, &mut kicked, Uuid::new_v4(), Uuid::nil(), "b",
            &vec![], Some(&SkipReason::ScopeSkipped { scope: "outer".into() }));
        let at_b = vec![Frame::Call { site: "b".into() }];
        assert!(kicked.contains_key(&FiringLocation::new("B.n", at_b.clone())), "{kicked:?}");
        assert!(!emissions.iter().any(|e| e.pulse.target_node == "sb" || e.pulse.target_node == "b__out"), "the enclosing pass owns the outward closures: {emissions:?}");
    }

    /// A refused group that holds a call site: the wire from the group's
    /// In into the site, and from there into the body, does not leave the
    /// group, so the body's In receives no closure and gets its one
    /// record from the site's teardown.
    #[test]
    fn a_refused_group_does_not_wake_the_body_of_a_site_inside_it() {
        use crate::frames::Frame;
        let mut project = called_project();
        // Put site `a` inside a group `g`: g__in -> a__in.x replaces src -> a__in.x.
        use crate::project::boundary_types as bt;
        let boundary = |id: &str, ty: &str, role: &str| serde_json::from_value::<NodeDefinition>(json!({
            "id": id, "nodeType": ty, "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
            "inputs": [{ "name": "x", "portType": "Number", "required": true }],
            "outputs": [{ "name": "x", "portType": "Number", "required": true }],
            "features": {}, "scope": [], "groupBoundary": { "groupId": "g", "role": role }, "requiresInfra": false, "images": []
        })).unwrap();
        project.nodes.push(boundary("g__in", bt::PASSTHROUGH, "In"));
        project.nodes.push(boundary("g__out", bt::PASSTHROUGH, "Out"));
        for id in ["a__in", "a__out"] {
            project.nodes.iter_mut().find(|n| n.id == id).unwrap().scope.push("g".into());
        }
        project.edges.retain(|e| !(e.source == "src" && e.target == "a__in"));
        project.edges.push(serde_json::from_value(json!({ "id": "g__in.x->a__in.x", "source": "g__in", "sourceHandle": "x", "target": "a__in", "targetHandle": "x" })).unwrap());
        project.groups.push(serde_json::from_value(json!({ "id": "g", "kind": "group", "nodeIds": ["a__in", "a__out"] })).unwrap());
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut kicked = HashMap::new();
        let emissions = tear_down_scope(&project, &edge_idx, &mut pulses, &mut kicked, Uuid::new_v4(), Uuid::nil(), "g", &vec![], Some(&SkipReason::DidNotFlow));
        assert!(!emissions.iter().any(|e| e.pulse.target_node == "B__in"), "the body's In is under the group, not past it: {emissions:?}");
        assert!(kicked.contains_key(&FiringLocation::new("a__in", vec![])), "{kicked:?}");
        // The site's own skip then takes the body down under the call.
        let emissions = tear_down_scope(&project, &edge_idx, &mut pulses, &mut kicked, Uuid::new_v4(), Uuid::nil(), "a", &vec![], Some(&SkipReason::ScopeSkipped { scope: "g".into() }));
        assert!(kicked.contains_key(&FiringLocation::new("B__in", vec![Frame::Call { site: "a".into() }])), "{kicked:?}");
        assert!(!emissions.iter().any(|e| e.pulse.target_node == "B__in"), "{emissions:?}");
    }

    #[test]
    fn a_shared_body_fires_once_per_call_site_and_answers_the_right_one() {
        use crate::frames::Frame;
        let project = called_project();
        let edge_idx = EdgeIndex::build(&project);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        let mut bag = OutputBag::new();
        bag.insert("a".into(), Arc::new(json!(1)));
        bag.insert("b".into(), Arc::new(json!(2)));
        postprocess_output("src", &bag, Uuid::new_v4(), Uuid::nil(), &Vec::new(), &project, &mut pulses, &edge_idx, &mut Vec::new()).unwrap();

        let fired = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 5, &mut pulses, &mut executions, &mut kicked);
        let mut names: Vec<(String, LoopFrames)> = fired.iter().map(|f| (f.node_id.clone(), f.frames.clone())).collect();
        names.sort();
        let at = |site: &str| vec![Frame::Call { site: site.into() }];
        assert_eq!(names, vec![
            ("B__in".to_string(), at("a")), ("B__in".to_string(), at("b")),
            ("a__in".to_string(), vec![]), ("b__in".to_string(), vec![]),
        ], "both sites fire at the root, the body once per site one frame deeper");
        // The body's member holds one value per call, each under its site's frame.
        let mut inside: Vec<(LoopFrames, serde_json::Value)> = pending(&pulses, "B.n").iter().map(|p| (p.frames.clone(), (*p.value).clone())).collect();
        inside.sort_by_key(|(f, _)| crate::frames::frames_text(f));
        assert_eq!(inside, vec![(at("a"), json!(1)), (at("b"), json!(2))]);
        // The body's gate is its In record under the call frame, so a
        // member is allowed under a site that fired and pending under one that did not.
        let member = project.nodes.iter().find(|n| n.id == "B.n").unwrap();
        assert_eq!(scope_permission(&project, member, &at("a"), &executions), ScopePermission::Allowed);
        assert_eq!(scope_permission(&project, member, &at("zzz"), &executions), ScopePermission::Pending);

        // The member runs for call `a` and answers: the body's Out fires at
        // `a`'s frame, pops it, and only `a`'s site takes the result.
        let mut out = OutputBag::new();
        out.insert("out".into(), Arc::new(json!(10)));
        postprocess_output("B.n", &out, Uuid::new_v4(), Uuid::nil(), &at("a"), &project, &mut pulses, &edge_idx, &mut Vec::new()).unwrap();
        let fired = fire_ready_passthroughs(&project, &edge_idx, None, Uuid::nil(), 6, &mut pulses, &mut executions, &mut kicked);
        let mut names: Vec<(String, LoopFrames)> = fired.iter().map(|f| (f.node_id.clone(), f.frames.clone())).collect();
        names.sort();
        assert_eq!(names, vec![("B__out".to_string(), at("a")), ("a__out".to_string(), vec![])]);
        let sa = pending(&pulses, "sa");
        assert_eq!(sa.len(), 1);
        assert_eq!(*sa[0].value, json!(10));
        assert!(sa[0].frames.is_empty(), "back at the caller's frames");
        assert!(pending(&pulses, "sb").is_empty(), "the other site is still waiting on its own call");
        assert!(pending(&pulses, "b__out").is_empty());
    }

    /// A loop inside an included file inside a loop: the gate walk
    /// consumes one frame per loop and one per body, so the body's In
    /// record is looked up under the outer iteration plus the call frame,
    /// and the member inside the inner loop is allowed one frame deeper.
    #[test]
    fn a_gate_walk_counts_loop_and_call_frames_alike() {
        use crate::frames::Frame;
        use crate::project::boundary_types as bt;
        let node = |id: &str, ty: &str, scope: Vec<&str>, boundary: serde_json::Value| json!({
            "id": id, "nodeType": ty, "label": null, "config": null,
            "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false, "images": []
        });
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("L1__in", bt::LOOP_IN, vec![], json!({ "groupId": "L1", "role": "In" })),
                node("B__in", bt::INCLUDE_IN, vec![], json!({ "groupId": "B", "role": "In" })),
                node("B.L2__in", bt::LOOP_IN, vec!["B"], json!({ "groupId": "B.L2", "role": "In" })),
                node("B.L2.n", "Test", vec!["B", "B.L2"], json!(null)),
            ],
            "edges": [],
            "groups": [
                { "id": "L1.site", "kind": "call", "body": "B", "nodeIds": [], "parentGroupId": "L1" },
                { "id": "L1.other", "kind": "call", "body": "B", "nodeIds": [], "parentGroupId": "L1" },
                { "id": "B", "kind": "body", "nodeIds": ["B.L2.n"] }
            ],
            "createdAt": "1970-01-01T00:00:00Z", "updatedAt": "1970-01-01T00:00:00Z",
        })).unwrap();
        let member = project.nodes.iter().find(|n| n.id == "B.L2.n").unwrap();
        let outer = Frame::Loop { index: 0 };
        let call = Frame::Call { site: "L1.site".into() };
        let inner = Frame::Loop { index: 1 };
        let frames = vec![outer.clone(), call.clone(), inner];
        let mut executions = NodeExecutionTable::default();
        assert_eq!(scope_permission(&project, member, &frames, &executions), ScopePermission::Pending, "the body has not started");
        executions.entry("B__in".into()).or_default().push(NodeExecution {
            id: Uuid::new_v4(), received: Default::default(), skip_reason: None, node_id: "B__in".into(),
            status: NodeExecutionStatus::Completed, pulses_absorbed: vec![], ordinal: 0, error: None,
            callback_id: None, started_at: 0, completed_at: Some(1), cost_usd: 0.0, logs: vec![],
            mentioned_ports: Default::default(), closed_output_ports: Default::default(),
            color: Uuid::nil(), frames: vec![outer, call], inherited_from: None,
        });
        assert_eq!(scope_permission(&project, member, &frames, &executions), ScopePermission::Allowed);
        let other_call = vec![Frame::Loop { index: 0 }, Frame::Call { site: "L1.other".into() }, Frame::Loop { index: 1 }];
        assert_eq!(scope_permission(&project, member, &other_call, &executions), ScopePermission::Pending, "another site's call has its own gate");
    }

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
        let scope: HashSet<Located> = [Located::top("src")].into_iter().collect();
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
    /// reason, and the scope is torn down.
    ///
    /// Torn down, not merely left alone: the scope never starts, so its
    /// outward ports close and every member is told. Leaving the inside
    /// untouched stranded every member no closure reaches, and anything
    /// the Out boundary fed from that branch waited forever.
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
        // Every member, the same as a scope that was gated off: a
        // scope-skip emits no closures, so a member nobody kicks is a
        // member nothing will ever settle. `lonely` (no wire into it)
        // and `inner` (wire-fed) both have to be told.
        for member in ["lonely", "inner"] {
            assert!(
                kicked.keys().any(|loc| loc.node_id == member),
                "a failed scope tells {member} instead of stranding it: {kicked:?}"
            );
        }
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
