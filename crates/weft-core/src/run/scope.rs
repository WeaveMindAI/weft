//! Scopes at run time. A group, a call site and an included body are no
//! nodes: each runs as a SCOPE, once per copy ([`ScopeCopy`]: per loop
//! iteration around it, per call of a body), and the engine decides each
//! copy here. A copy waits until the copy around it is open, then:
//!
//! - with a gate (a wire landing on its `_should_flow` or
//!   `_should_not_flow`), it waits for the gate's value and opens or does
//!   not run on what the gate says ([`flow_verdict`]);
//! - with a gate written in its braces, or handed a value at a group
//!   start, that decides at once, either way;
//! - with no gate, it opens on the first value that reaches any of its
//!   members from outside, or at once when nothing comes from outside;
//!   when every wire coming in closed with nothing on it, it does not run.
//!   A wire coming in that only carries what something inside sent out
//!   first is not waited on: it cannot arrive before the scope opens.
//!
//! Opening sends what starts at the scope's own ports (the literal on an
//! input, a closure on an input nobody feeds and on an output no member
//! writes), and kicks the members that have no input; every other member
//! waits on its own inputs, and every output leaves the moment it is
//! made. A copy that does not run closes every wire that leaves it (with
//! the failure that stopped it, when one did) and tells everything inside
//! it, however deep, that it did not run.
//!
//! A loop is a scope plus iterations: each iteration it launches is a
//! copy, opened by the launch through the same opening ([`open_iteration`])
//! and taken down through the same teardown when its `LoopIn` does not
//! run ([`tear_down_loop`]).
//!
//! Nothing here is a fact the engine learned from outside: a decision is
//! a function of the program and of the pulses, so the journal never
//! records one. The live engine and the journal fold both decide every
//! copy in [`settle_table`], in the same order, and hold the same table
//! afterwards. Same records, with one documented exception: the time a
//! record carries is the engine's clock on one side and the row's
//! `at_unix` on the other.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use crate::frames::{FiringLocation, Located, LoopFrames};
use crate::primitive::{KickTable, KickedNode, Phase};
use crate::project::scopes::ScopeCopy;
use crate::project::graph::GraphView;
use crate::project::{CrossingSide, Given, GivenOrigin, GivenPort, GroupDefinition, NodeDefinition, ProgramIndex, ProjectDefinition};
use crate::pulse::{Failure, Pulse, PulseStatus, PulseTable};
use crate::run::emission::{scope_emission, stand_in_emission, PulseEmission};
use crate::run::postprocess::{close_unmentioned_downstream, emit_closure_on_edge, emit_value_on_edge, part_on_wire};
use crate::record_ref::{GivenSide, Step, ValueRef};
use crate::run::skip::{flow_verdict, SkipReason};
use crate::RunId;

/// How a copy of a scope was decided.
// SYNC: ScopeState <-> packages/weft-graph/src/protocol.ts ScopeState
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ScopeState {
    Open,
    /// It did not run, and why.
    Skipped { reason: SkipReason },
}

/// A copy's decision, and when it was made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRecord {
    pub state: ScopeState,
    pub at: u64,
}

/// What a run decided about its scopes: every copy decided so far, the
/// copies waiting to be, and the places that stood in for their node
/// already. A copy with no record is waiting (or has not been reached).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeTable {
    copies: BTreeMap<ScopeCopy, ScopeRecord>,
    /// The copies that can be decided now (every copy around them is
    /// open) and are not yet: kept as copies open and are decided, so a
    /// turn never walks what the run decided before.
    waiting: BTreeSet<ScopeCopy>,
    /// Whether the top copies were put in `waiting` (the first pass does).
    started: bool,
    /// The scopes some copy of which did not run: whose members' pulses
    /// a pass looks at for what the run will never take.
    skipped: BTreeSet<String>,
    /// The stand-ins whose outputs are on the wires, and the handed
    /// streams whose items are, each sent once.
    sent: BTreeSet<Sent>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Sent {
    StandIn(FiringLocation),
    Stream(FiringLocation, String),
}

impl ScopeTable {
    /// The decision about `copy`, if it has one.
    pub fn get(&self, copy: &ScopeCopy) -> Option<&ScopeRecord> {
        self.copies.get(copy)
    }

    /// Every copy decided, in order.
    pub fn iter(&self) -> impl Iterator<Item = (&ScopeCopy, &ScopeRecord)> {
        self.copies.iter()
    }

    /// Forget the decision about every copy `gone` picks, and what was
    /// sent at every location `sent_at` picks: a reader that only shows
    /// the run is done with a copy once nothing can happen inside it. A
    /// copy still waiting is never picked by such a reader.
    pub fn forget(&mut self, mut gone: impl FnMut(&ScopeCopy) -> bool, mut sent_at: impl FnMut(&FiringLocation) -> bool) {
        self.copies.retain(|copy, _| !gone(copy));
        self.sent.retain(|sent| match sent {
            Sent::StandIn(at) | Sent::Stream(at, _) => !sent_at(at),
        });
    }

    /// The copies waiting to be decided.
    pub fn waiting(&self) -> impl Iterator<Item = &ScopeCopy> {
        self.waiting.iter()
    }

    /// Whether some copy of `scope` is decided: one look in the ordered
    /// table (its copies sort together, the copy at no frames first),
    /// however many copies the run decided.
    fn decided_any(&self, scope: &str) -> bool {
        self.copies.range(ScopeCopy::new(scope, Vec::new())..).next().is_some_and(|(copy, _)| copy.scope == scope)
    }

    fn decide(&mut self, copy: ScopeCopy, state: ScopeState, at: u64) {
        self.waiting.remove(&copy);
        if matches!(state, ScopeState::Skipped { .. }) {
            self.skipped.insert(copy.scope.clone());
        }
        self.copies.entry(copy).or_insert(ScopeRecord { state, at });
    }

    /// `copy` opened: what sits directly in it can be decided now.
    fn open(&mut self, program_idx: &ProgramIndex, copy: &ScopeCopy, at: u64) {
        self.decide(copy.clone(), ScopeState::Open, at);
        for child in program_idx.scopes().children_of(copy) {
            if !self.copies.contains_key(&child) {
                self.waiting.insert(child);
            }
        }
    }

    /// Put the run's top copies in `waiting`, once.
    fn start(&mut self, program_idx: &ProgramIndex) {
        if self.started { return; }
        self.started = true;
        for copy in program_idx.scopes().top_copies() {
            if !self.copies.contains_key(&copy) {
                self.waiting.insert(copy);
            }
        }
    }
}

/// Whether a node at some frames may fire: every copy around it is open
/// (`Allowed`), one did not run (`Skipped`, naming the outermost that did
/// not), or one is still waiting (`Pending`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopePermission {
    Pending,
    Allowed,
    Skipped(String),
}

/// Whether the node at `frames` may fire, by the copies around it. A kick
/// and an arriving value obey the same answer. The node's own copy is the
/// innermost (a loop's `LoopOut` fires inside the iteration it gathers),
/// and every copy up from it must be open.
pub fn scope_permission(program_idx: &ProgramIndex, node: &NodeDefinition, frames: &LoopFrames, scopes: &ScopeTable) -> ScopePermission {
    chain_permission(program_idx, program_idx.scopes().node_copy(node, frames), scopes)
}

fn chain_permission(program_idx: &ProgramIndex, mut at: Option<ScopeCopy>, scopes: &ScopeTable) -> ScopePermission {
    let mut skipped = None;
    let mut pending = false;
    while let Some(copy) = at {
        match scopes.copies.get(&copy).map(|record| &record.state) {
            Some(ScopeState::Open) => {}
            Some(ScopeState::Skipped { .. }) => skipped = Some(copy.scope.clone()),
            None => pending = true,
        }
        at = match program_idx.scopes().parent_of(&copy) {
            Ok(parent) => parent,
            Err(why) => {
                tracing::error!(target: "weft_core::run::scope", "{why}; the node waits (corrupt frames)");
                return ScopePermission::Pending;
            }
        };
    }
    match (skipped, pending) {
        (Some(scope), _) => ScopePermission::Skipped(scope),
        (None, true) => ScopePermission::Pending,
        (None, false) => ScopePermission::Allowed,
    }
}

/// What one pass over the table did with no row behind it: see
/// [`settle_table`].
#[derive(Debug, Default)]
pub struct TablePass {
    /// The copies decided, in the order they were: each copy that was
    /// decided on its own, followed (when it did not run) by every copy
    /// inside it, which did not run because it did not.
    pub decided: Vec<(ScopeCopy, ScopeState)>,
    /// What the copies sent and closed, what stand-ins sent, and the
    /// handed streams' items.
    pub emissions: Vec<PulseEmission>,
    /// Pending pulses the run will never take, now absorbed: inside a
    /// copy that did not run, or on a gate already decided.
    pub absorbed: Vec<Uuid>,
    /// Pending pulses into a trigger at Fire, removed from the table.
    pub dropped: Vec<Uuid>,
}

/// Everything that happens to the table without a row of its own, in the
/// one order both sides use: every copy that can be decided is (and what
/// opening one makes decidable, until nothing more is), the stand-ins and
/// handed streams whose scope opened send, and what the run will never
/// take settles. The worker runs it at the top of every turn and at the
/// end of its cancel walk; the fold runs it after every row, so the two
/// hold the same table. `given` is what the run is given at the places its
/// program says ([`crate::project::Given`]).
#[allow(clippy::too_many_arguments)]
pub fn settle_table(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    given: &Given,
    phase: Phase,
    run_id: RunId,
    now: u64,
    pulses: &mut PulseTable,
    scopes: &mut ScopeTable,
    kicked: &mut KickTable,
) -> TablePass {
    let mut pass = TablePass::default();
    scopes.start(program_idx);
    loop {
        let decided = decide_ready_copies(project, program_idx, given, run_id, now, pulses, scopes, kicked, &mut pass.emissions);
        let sent = send_given(project, program_idx, given, run_id, pulses, scopes, kicked, &mut pass.emissions);
        let settled = decided.is_empty() && !sent;
        pass.decided.extend(decided);
        if settled { break; }
    }
    pass.absorbed = absorb_spent(program_idx, project, pulses, scopes);
    if matches!(phase, Phase::Fire) {
        for trigger in project.nodes.iter().filter(|n| n.features.is_trigger) {
            if let Some(bucket) = pulses.remove(&trigger.id) {
                pass.dropped.extend(bucket.iter().filter(|p| p.status == PulseStatus::Pending).map(|p| p.id));
            }
        }
    }
    pass
}

/// Decide every copy that is waiting with the copy around it open, in a
/// fixed order, once. Answers what was decided.
#[allow(clippy::too_many_arguments)]
fn decide_ready_copies(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    given: &Given,
    run_id: RunId,
    now: u64,
    pulses: &mut PulseTable,
    scopes: &mut ScopeTable,
    kicked: &mut KickTable,
    emissions: &mut Vec<PulseEmission>,
) -> Vec<(ScopeCopy, ScopeState)> {
    let mut decided = Vec::new();
    let waiting: Vec<ScopeCopy> = scopes.waiting.iter().cloned().collect();
    for copy in waiting {
        let Some(state) = verdict(project, program_idx, given, &copy, run_id, pulses) else { continue };
        match &state {
            ScopeState::Open => open(project, program_idx, &copy, run_id, now, pulses, scopes, kicked, emissions),
            ScopeState::Skipped { reason } => {
                let reason = reason.clone();
                let inside = take_down(project, program_idx, &copy, &reason, run_id, now, pulses, scopes, kicked, emissions);
                decided.push((copy, state));
                decided.extend(inside);
                continue;
            }
        }
        decided.push((copy, state));
    }
    decided
}

/// The copies waiting to be decided with every copy around them open, in
/// order: the top scopes' copies and the children of every open copy, less
/// those decided already. Before the run's first pass, the top copies.
pub fn waiting_copies(program_idx: &ProgramIndex, scopes: &ScopeTable) -> Vec<ScopeCopy> {
    if !scopes.started {
        return program_idx.scopes().top_copies().into_iter().filter(|copy| !scopes.copies.contains_key(copy)).collect();
    }
    scopes.waiting.iter().cloned().collect()
}

/// What `copy` is decided as now, or `None` while it waits.
fn verdict(project: &ProjectDefinition, program_idx: &ProgramIndex, given: &Given, copy: &ScopeCopy, run_id: RunId, pulses: &PulseTable) -> Option<ScopeState> {
    let scope = program_idx.scopes().get(&copy.scope)?;
    let group = project.group(&copy.scope)?;
    // A gate handed a value at a group start is the gate's, as a literal
    // written on it would be (the cut keeps no wire onto it).
    let handed_gate: BTreeMap<String, Value> = given.inputs.get(&Located::at(&copy.scope, &copy.frames)).into_iter().flatten()
        .filter(|(port, _)| crate::run::skip::is_gate_port(port))
        .map(|(port, value)| (port.clone(), value.clone()))
        .collect();
    let gated = handed_gate.is_empty()
        && scope.gates.iter().any(|&at| program_idx.source_frames(&project.edges[at], &copy.frames).is_some());
    if gated {
        let gate: Vec<&Pulse> = pending_at(pulses, &copy.scope, run_id, &copy.frames).collect();
        if gate.is_empty() {
            return None;
        }
        return Some(match flow_verdict(&gate, &group.port_literals) {
            Some(reason) => ScopeState::Skipped { reason },
            None => ScopeState::Open,
        });
    }
    // A gate written in the braces or handed decides at once, either way,
    // as a wired one does when its value arrives.
    let literals = if handed_gate.is_empty() { group.port_literals.clone() } else { handed_gate };
    let gate_speaks = literals.iter().any(|(port, value)| crate::run::skip::is_gate_port(port) && !value.is_null());
    if gate_speaks {
        return Some(match flow_verdict(&[], &literals) {
            Some(reason) => ScopeState::Skipped { reason },
            None => ScopeState::Open,
        });
    }
    let mut waiting = false;
    let mut failure: Option<Failure> = None;
    for leg in scope.entering.iter().filter(|leg| !leg.fed_back) {
        let edge = &project.edges[leg.edge];
        let lands_at = leg.inner_frames(&copy.frames);
        if program_idx.source_frames(edge, &lands_at).is_none() {
            continue;
        }
        let here: Vec<&Pulse> = pending_at(pulses, &edge.target, run_id, &lands_at)
            .filter(|pulse| pulse.target_port == edge.target_port()).collect();
        match crate::run::ready::resolve_port_value(&here, edge.target_port()) {
            None => waiting = true,
            // A stream's clean end is the empty stream, a value.
            Some(pulse) if !pulse.closed || (pulse.failure.is_none() && super::ready::edge_targets_generator(program_idx, edge)) => {
                return Some(ScopeState::Open);
            }
            Some(pulse) => {
                if failure.is_none() { failure = pulse.failure.clone(); }
            }
        }
    }
    if waiting {
        return None;
    }
    // Nothing comes in from outside (it opens with the copy around it),
    // or everything that came in closed with nothing on it.
    let anything_came = scope.entering.iter().filter(|leg| !leg.fed_back).any(|leg| {
        program_idx.source_frames(&project.edges[leg.edge], &leg.inner_frames(&copy.frames)).is_some()
    });
    Some(if anything_came { ScopeState::Skipped { reason: SkipReason::EveryInputClosed { failure } } } else { ScopeState::Open })
}

/// The pending pulses of this run waiting at `target` (a node or a
/// group's gate) at exactly `frames`.
fn pending_at<'a>(pulses: &'a PulseTable, target: &str, run_id: RunId, frames: &'a LoopFrames) -> impl Iterator<Item = &'a Pulse> + 'a {
    pulses.get(target).into_iter().flatten()
        .filter(move |pulse| pulse.status.is_pending() && pulse.run_id == run_id && pulse.frames == *frames)
}

/// Open `copy`: record it, take its gate's pulses, send what starts at
/// its own ports, and kick its members that have no input.
#[allow(clippy::too_many_arguments)]
fn open(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    copy: &ScopeCopy,
    run_id: RunId,
    now: u64,
    pulses: &mut PulseTable,
    scopes: &mut ScopeTable,
    kicked: &mut KickTable,
    emissions: &mut Vec<PulseEmission>,
) {
    scopes.open(program_idx, copy, now);
    absorb_gate(pulses, copy, run_id);
    let Some(scope) = program_idx.scopes().get(&copy.scope) else { return };
    let group = project.group(&copy.scope);
    let emission_id = scope_emission(&copy.scope, &copy.frames);
    for &at in &scope.sends {
        let edge = &project.edges[at];
        match group.and_then(|group| sent_value(project, group, edge, &copy.frames)) {
            // A literal the program holds: kept in no row.
            Some(Ok(value)) => emit_value_on_edge(
                program_idx, &copy.scope, edge, edge.source_port(), value, None, emission_id, run_id, &copy.frames, pulses, emissions,
            ),
            Some(Err(failure)) => emit_closure_on_edge(
                program_idx, &copy.scope, edge, edge.source_port(), emission_id, run_id, &copy.frames, Some(&failure), pulses, emissions,
            ),
            None => emit_closure_on_edge(
                program_idx, &copy.scope, edge, edge.source_port(), emission_id, run_id, &copy.frames, None, pulses, emissions,
            ),
        }
    }
    kick_unfed_members(project, program_idx, &copy.scope, &copy.frames, kicked);
}

/// What a wire that starts at one of `group`'s own ports carries when the
/// group opens: the literal written on the input it starts at (through
/// the wire's key path), or nothing (a closure) for an input with no
/// literal and for an output no member writes. `Err` when the path cannot
/// read the literal, which the compiler refuses, so only a hand-made
/// program gets there: the closure carries why.
fn sent_value(project: &ProjectDefinition, group: &GroupDefinition, edge: &crate::project::Edge, frames: &LoopFrames) -> Option<Result<Arc<Value>, Failure>> {
    let own = edge.via.first()?;
    if own.side != CrossingSide::In {
        return None;
    }
    let value = group.port_literals.get(&own.port)?;
    // The port's type where this copy runs (a group inside an included
    // file has the types its site gave it).
    let port_type = group.types_at(&crate::frames::call_path(frames)).input(&own.port)?;
    if value.is_null() && !port_type.port_value_type().contains_null() {
        return None;
    }
    if edge.path.is_empty() {
        return Some(Ok(Arc::new(value.clone())));
    }
    match crate::deref::project_value(&Arc::new(value.clone()), port_type, &edge.path) {
        Ok(crate::deref::Projection::Value(v)) => Some(Ok(Arc::new(v))),
        Ok(crate::deref::Projection::Closed { .. }) => None,
        Err(error) => {
            let call_path: Vec<String> = crate::frames::call_path(frames).into_iter().map(str::to_string).collect();
            let node = crate::project::group_address(project, &group.id, &call_path);
            Some(Err(Failure { node, error: format!("its '{}' cannot be read by a wire: {error}", own.port) }))
        }
    }
}

/// Kick the members of `scope` directly at `frames` that start with its
/// copy ([`starting_members`]).
fn kick_unfed_members(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    scope: &str,
    frames: &LoopFrames,
    kicked: &mut KickTable,
) {
    let roots = starting_members(project, program_idx, Some(scope), frames);
    kick_scope(kicked, &roots, frames, None);
}

/// The members directly in `scope` (the top of the program for `None`) at
/// `frames` that start with it: those no wire feeds there. A trigger is
/// never among them (its fire kicks it, or a run kicks it payload-less at
/// its start), nor a place the program does not run there, nor one that
/// stands in for its node (it sends on its own, [`send_given`]).
pub fn starting_members(project: &ProjectDefinition, program_idx: &ProgramIndex, scope: Option<&str>, frames: &LoopFrames) -> Vec<String> {
    let members: &[usize] = match scope {
        Some(scope) => match program_idx.scopes().get(scope) {
            Some(scope) => &scope.members,
            None => return Vec::new(),
        },
        None => program_idx.scopes().top_members(),
    };
    members.iter()
        .map(|&at| &project.nodes[at])
        .filter(|node| !node.features.is_trigger)
        .filter(|node| program_idx.admits(&node.id, frames) && !program_idx.stands_in(&node.id, frames))
        .filter(|node| program_idx.get_incoming(project, &node.id, frames).is_empty())
        .map(|node| node.id.clone())
        .collect()
}

/// Take `copy` down: record it with `reason`, close every wire that
/// leaves it (with the failure that stopped it, when one did), and tell
/// everything inside that it did not run. Answers the copies inside it,
/// decided with it.
#[allow(clippy::too_many_arguments)]
fn take_down(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    copy: &ScopeCopy,
    reason: &SkipReason,
    run_id: RunId,
    now: u64,
    pulses: &mut PulseTable,
    scopes: &mut ScopeTable,
    kicked: &mut KickTable,
    emissions: &mut Vec<PulseEmission>,
) -> Vec<(ScopeCopy, ScopeState)> {
    scopes.decide(copy.clone(), ScopeState::Skipped { reason: reason.clone() }, now);
    absorb_gate(pulses, copy, run_id);
    let Some(scope) = program_idx.scopes().get(&copy.scope) else { return Vec::new() };
    let failure = reason.inherited_failure().cloned();
    let emission_id = scope_emission(&copy.scope, &copy.frames);
    for leg in &scope.leaving {
        let edge = &project.edges[leg.edge];
        let from = leg.inner_frames(&copy.frames);
        emit_closure_on_edge(
            program_idx, &edge.source, edge, edge.source_port(), emission_id, run_id, &from, failure.as_ref(), pulses, emissions,
        );
    }
    tell_inside(project, program_idx, copy, now, scopes, kicked)
}

/// Every copy inside `copy` is recorded as not run because `copy` did
/// not, and every node inside is kicked straight into the skip that says
/// so (a kick already there, a trigger's fire, keeps its payload and takes
/// the verdict). Answers the copies it decided.
fn tell_inside(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    copy: &ScopeCopy,
    now: u64,
    scopes: &mut ScopeTable,
    kicked: &mut KickTable,
) -> Vec<(ScopeCopy, ScopeState)> {
    let inside = program_idx.scopes().inside(copy);
    let mut decided = Vec::new();
    for inner in inside.copies {
        if scopes.copies.contains_key(&inner) { continue; }
        let state = ScopeState::Skipped { reason: SkipReason::ScopeSkipped { scope: copy.scope.clone() } };
        scopes.decide(inner.clone(), state.clone(), now);
        decided.push((inner, state));
    }
    for (at, frames) in inside.nodes {
        let node = &project.nodes[at];
        if !program_idx.admits(&node.id, &frames) || program_idx.stands_in(&node.id, &frames) { continue; }
        kick_scope(kicked, std::slice::from_ref(&node.id), &frames, Some(&copy.scope));
    }
    decided
}

/// Open iteration `body_frames` of the loop `loop_id`: the one opening
/// every scope gets, for the copy a launch starts. Its members no wire
/// feeds start with it, and the groups inside it become decidable.
pub fn open_iteration(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    loop_id: &str,
    body_frames: &LoopFrames,
    now: u64,
    scopes: &mut ScopeTable,
    kicked: &mut KickTable,
) {
    scopes.open(program_idx, &ScopeCopy::new(loop_id, body_frames.clone()), now);
    kick_unfed_members(project, program_idx, loop_id, body_frames, kicked);
}

/// The teardown of a loop whose `LoopIn` at `frames` did not run with
/// `reason`: its outward ports (its `LoopOut`'s) close at the loop's
/// frames, with the failure that stopped it when one did, and everything
/// inside it is told it did not run. A `LoopIn` that did not run because
/// the scope around it did not (`ScopeSkipped`) owes nothing: that
/// scope's teardown closed the wires out of it and told what is inside.
/// Answers what it sent and the copies inside the loop it decided.
#[allow(clippy::too_many_arguments)]
pub fn tear_down_loop(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    loop_id: &str,
    frames: &LoopFrames,
    reason: &SkipReason,
    run_id: RunId,
    now: u64,
    pulses: &mut PulseTable,
    scopes: &mut ScopeTable,
    kicked: &mut KickTable,
) -> TablePass {
    let mut pass = TablePass::default();
    if matches!(reason, SkipReason::ScopeSkipped { .. }) {
        return pass;
    }
    pass.emissions = close_loop_outward_ports(
        project, program_idx, pulses, scope_emission(loop_id, frames), run_id, loop_id, frames, reason.inherited_failure(),
    );
    pass.decided = tell_inside(project, program_idx, &ScopeCopy::new(loop_id, frames.clone()), now, scopes, kicked);
    pass
}

/// The closures a loop that never ran or ended abnormally (it failed,
/// it was cancelled) owes the outside: one per outward port of its
/// `LoopOut`, at the loop's frames, carrying `failure` when that is why,
/// so what is outside reads a failure as a failure and skips cascade
/// instead of deadlocking.
#[allow(clippy::too_many_arguments)]
pub fn close_loop_outward_ports(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &mut PulseTable,
    emission_id: Uuid,
    run_id: RunId,
    loop_id: &str,
    frames: &LoopFrames,
    failure: Option<&Failure>,
) -> Vec<PulseEmission> {
    let mut emissions = Vec::new();
    let loop_out = crate::project::loop_out_id(loop_id);
    if let Err(error) = close_unmentioned_downstream(
        &loop_out, &Default::default(), emission_id, run_id, frames, project, pulses, program_idx, &mut emissions,
        failure, &Default::default(),
    ) {
        // Only a program with no `LoopOut` for its loop gets here: the
        // compiler mints both halves together.
        tracing::error!(target: "weft_core::run::scope", loop_id, %error, "a loop's outward ports did not close (corrupt compiled program)");
    }
    emissions
}

/// Send what the program gives: every stand-in whose scope opened sends
/// its outputs, and every handed stream at a place that fires puts its
/// items on its port. Each once. Answers whether anything was sent.
fn send_given(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    given: &Given,
    run_id: RunId,
    pulses: &mut PulseTable,
    scopes: &mut ScopeTable,
    kicked: &KickTable,
    emissions: &mut Vec<PulseEmission>,
) -> bool {
    let mut sent = false;
    for (place, stand_in) in &given.outputs {
        let frames = place.frames();
        let location = FiringLocation::new(place.id.clone(), frames.clone());
        if scopes.sent.contains(&Sent::StandIn(location.clone())) { continue; }
        let Some(node) = program_idx.node(project, &place.id) else { continue };
        // A stand-in sits in the scope its node sits in, even a loop's
        // `LoopOut` standing in for its whole loop.
        let around = node.scope.last().map(|scope| ScopeCopy::new(scope.clone(), frames.clone()));
        match chain_permission(program_idx, around, scopes) {
            ScopePermission::Pending => continue,
            // The scope's teardown closed what leaves it; nothing to send.
            ScopePermission::Skipped(_) => {}
            ScopePermission::Allowed => {
                let emission_id = stand_in_emission(&place.id, &frames);
                let start = emissions.len();
                for (port, given) in &stand_in.ports {
                    // Where the run's record keeps what it was given: its birth.
                    let kept = Arc::new(ValueRef::Given { run: run_id, place: place.clone(), port: port.clone(), side: GivenSide::Output });
                    for edge in program_idx.get_outgoing(project, &place.id, &frames).into_iter().filter(|e| e.source_port() == port) {
                        // What the stand-in was given and cannot be sent as
                        // given (a stream that is no list, a port its node
                        // does not have, a key path the value lacks) closes
                        // the wire with why: a broken seed or emit reads as
                        // a failure downstream, never as nothing sent.
                        let broken = |error: String| Failure::at(project, &place.id, &frames, error);
                        let output = node.outputs.iter().find(|o| o.name == *port);
                        match (given, output) {
                            (GivenPort::Value { .. }, None) => emit_closure_on_edge(
                                program_idx, &place.id, edge, port, emission_id, run_id, &frames,
                                Some(&broken(format!("it stands in with a value for '{port}', which it does not have"))), pulses, emissions,
                            ),
                            (GivenPort::Value { value }, Some(output)) if output.is_generator() => match value.as_array() {
                                Some(items) => {
                                    for (index, item) in items.iter().enumerate() {
                                        let item_emission = crate::run::emission::stream_emission(&place.id, &frames, index as u64);
                                        let origin = Arc::new(kept.as_ref().clone().part(vec![Step::Item(index as u64)]));
                                        emit_value_on_edge(program_idx, &place.id, edge, port, Arc::new(item.clone()), Some(origin), item_emission, run_id, &frames, pulses, emissions);
                                    }
                                    emit_closure_on_edge(program_idx, &place.id, edge, port, emission_id, run_id, &frames, None, pulses, emissions);
                                }
                                None => emit_closure_on_edge(
                                    program_idx, &place.id, edge, port, emission_id, run_id, &frames,
                                    Some(&broken(format!("its stream '{port}' was given a value that is no list of items"))), pulses, emissions,
                                ),
                            },
                            (GivenPort::Value { value }, Some(output)) => {
                                let delivered = if edge.path.is_empty() {
                                    Ok(Some(Arc::new(value.clone())))
                                } else {
                                    match crate::deref::project_value(&Arc::new(value.clone()), &output.port_type, &edge.path) {
                                        Ok(crate::deref::Projection::Value(v)) => Ok(Some(Arc::new(v))),
                                        Ok(crate::deref::Projection::Closed { .. }) => Ok(None),
                                        Err(error) => Err(broken(format!("its '{port}' cannot be read by a wire: {error}"))),
                                    }
                                };
                                match delivered {
                                    Ok(Some(value)) => emit_value_on_edge(
                                        program_idx, &place.id, edge, port, value, Some(part_on_wire(&kept, edge)), emission_id, run_id, &frames, pulses, emissions,
                                    ),
                                    Ok(None) => emit_closure_on_edge(program_idx, &place.id, edge, port, emission_id, run_id, &frames, None, pulses, emissions),
                                    Err(failure) => emit_closure_on_edge(program_idx, &place.id, edge, port, emission_id, run_id, &frames, Some(&failure), pulses, emissions),
                                }
                            }
                            (GivenPort::Closed { failure }, _) => emit_closure_on_edge(
                                program_idx, &place.id, edge, port, emission_id, run_id, &frames, failure.as_ref(), pulses, emissions,
                            ),
                        }
                    }
                }
                // A port the stand-in gives nothing for closes, as a node
                // that ran and never sent it would.
                let given: std::collections::HashSet<String> = stand_in.ports.keys().cloned().collect();
                if let Err(error) = close_unmentioned_downstream(
                    &place.id, &given, emission_id, run_id, &frames, project, pulses, program_idx, emissions, None, &Default::default(),
                ) {
                    tracing::error!(target: "weft_core::run::scope", place = %place, %error, "a stand-in's unsent ports did not close");
                }
                for emission in &mut emissions[start..] {
                    mark_given(&mut emission.pulse, stand_in.origin);
                    if let Some(pulse) = pulses.get_mut(&emission.pulse.target_node)
                        .and_then(|bucket| bucket.iter_mut().find(|pulse| pulse.id == emission.pulse.id))
                    {
                        mark_given(pulse, stand_in.origin);
                    }
                }
            }
        }
        scopes.sent.insert(Sent::StandIn(location));
        sent = true;
    }
    for (place, ports) in &given.inputs {
        let Some(node) = program_idx.node(project, &place.id) else { continue };
        for (port, value) in ports {
            if !crate::run::ready::is_generator_input(node, port) { continue; }
            // At every frames the node fires at: where something reached it,
            // and where it was kicked.
            let mut at: BTreeSet<LoopFrames> = pulses.get(&place.id).into_iter().flatten()
                .filter(|p| p.run_id == run_id && Located::at(&place.id, &p.frames) == *place).map(|p| p.frames.clone()).collect();
            at.extend(kicked.keys().filter(|loc| loc.node_id == place.id && Located::at(&loc.node_id, &loc.frames) == *place).map(|loc| loc.frames.clone()));
            for frames in at {
                let location = FiringLocation::new(place.id.clone(), frames.clone());
                let key = Sent::Stream(location, port.clone());
                if scopes.sent.contains(&key) { continue; }
                if scope_permission(program_idx, node, &frames, scopes) != ScopePermission::Allowed { continue; }
                let identity = serde_json::to_vec(&(&place.id, port, &frames)).expect("a stream location serializes");
                let base = Uuid::new_v5(&run_id, &identity);
                let bucket = pulses.entry(place.id.clone()).or_default();
                let kept = ValueRef::Given { run: run_id, place: place.clone(), port: port.clone(), side: GivenSide::Input };
                for (index, item) in value.as_array().into_iter().flatten().enumerate() {
                    let mut pulse = Pulse::new(Uuid::new_v5(&base, &(index as u64).to_be_bytes()), run_id, frames.clone(), &place.id, port, Arc::new(item.clone()));
                    pulse.provided = true;
                    pulse.origin = Some(Arc::new(kept.clone().part(vec![Step::Item(index as u64)])));
                    bucket.push(pulse);
                }
                let mut end = Pulse::closure(Uuid::new_v5(&base, b"end"), run_id, frames.clone(), &place.id, port);
                end.provided = true;
                bucket.push(end);
                scopes.sent.insert(key);
                sent = true;
            }
        }
    }
    sent
}

/// Mark a pulse a stand-in sent with where its value came from.
fn mark_given(pulse: &mut Pulse, origin: GivenOrigin) {
    match origin {
        GivenOrigin::Emitted | GivenOrigin::Baked => pulse.provided = true,
        GivenOrigin::Inherited { run } => pulse.inherited_from = Some(run),
    }
}

/// Absorb the pending pulses on `copy`'s gate: a decision takes them.
fn absorb_gate(pulses: &mut PulseTable, copy: &ScopeCopy, run_id: RunId) {
    pulses.absorb(&copy.scope, |p| p.run_id == run_id && p.frames == copy.frames);
}

/// Absorb every pending pulse the run will never take: one at a node
/// inside a copy that did not run (it reached the node before the copy
/// was decided, or from a source outside it after), and one on a gate
/// already decided. Only the buckets that can hold one are looked at: the
/// nodes inside a scope some copy of which did not run, and the gates of
/// the scopes decided. Answers their ids.
fn absorb_spent(program_idx: &ProgramIndex, project: &ProjectDefinition, pulses: &mut PulseTable, scopes: &ScopeTable) -> Vec<Uuid> {
    let mut absorbed = Vec::new();
    if scopes.copies.is_empty() {
        return absorbed;
    }
    let can_hold: Vec<String> = pulses.iter()
        .filter(|(target, bucket)| !bucket.is_empty() && match program_idx.node(project, target) {
            Some(node) => node.scope.iter().any(|scope| scopes.skipped.contains(scope)),
            None => scopes.decided_any(target),
        })
        .map(|(target, _)| target.clone())
        .collect();
    for target in can_hold {
        let node = program_idx.node(project, &target);
        absorbed.extend(pulses.absorb(&target, |pulse| match node {
            Some(node) => matches!(scope_permission(program_idx, node, &pulse.frames, scopes), ScopePermission::Skipped(_)),
            None => scopes.copies.contains_key(&ScopeCopy::new(target.clone(), pulse.frames.clone())),
        }));
    }
    absorbed
}

/// Kick `roots` at `frames`: one entry per root, first launch wins, like
/// every other kick. With `skipped_by`, the scope did not run and every
/// kick dispatches straight into a `ScopeSkipped` skip; a kick already
/// there (a trigger's fire) keeps its payload and takes the verdict. A
/// copy's opening, a loop's launch and every teardown come through here.
pub fn kick_scope(
    kicked: &mut KickTable,
    roots: &[String],
    frames: &LoopFrames,
    skipped_by: Option<&str>,
) {
    for root in roots {
        // First writer wins, and nothing ever removes a kick: a scope
        // opens once per copy, so its roots there are kicked once. A
        // second write at a kicked location only marks it skipped
        // (`skipped_by`) and keeps the rest (a firing trigger's wake
        // payload and port snapshot).
        let (kick, made) = kicked.kick(FiringLocation::new(root.clone(), frames.clone()), || {
            KickedNode::new(false, None, None, skipped_by.map(str::to_string))
        });
        if let (false, Some(scope)) = (made, skipped_by) {
            kick.scope_skipped = Some(scope.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::Frame;
    use serde_json::json;

    /// A node: its id, scope chain, inputs, outputs, and whether it is a
    /// trigger.
    type Node<'a> = (&'a str, &'a [&'a str], &'a [&'a str], &'a [&'a str], bool);
    /// A wire: source, source port, the group ports it passes as `(group,
    /// port, side)`, target, target port.
    type Wire<'a> = (&'a str, &'a str, &'a [(&'a str, &'a str, &'a str)], &'a str, &'a str);

    /// A program from its nodes, its groups (`(id, kind, parent)`, kind
    /// `group`, `body`, `call:<body>` or `loop`), and its wires.
    fn program(
        nodes: &[Node],
        groups: &[(&str, &str, Option<&str>)],
        wires: &[Wire],
    ) -> ProjectDefinition {
        let port = |name: &&str| json!({ "name": name, "portType": "String", "required": false });
        let nodes: Vec<Value> = nodes.iter().map(|(id, scope, inputs, outputs, trigger)| json!({
            "id": id, "nodeType": "T", "label": null, "config": {}, "position": { "x": 0, "y": 0 },
            "scope": scope, "groupBoundary": null, "requiresInfra": false,
            "inputs": inputs.iter().map(port).collect::<Vec<_>>(),
            "outputs": outputs.iter().map(port).collect::<Vec<_>>(),
            "features": { "isTrigger": trigger },
        })).collect();
        let groups: Vec<Value> = groups.iter().map(|(id, kind, parent)| {
            let mut group = match kind.split_once(':') {
                Some(("call", body)) => json!({ "kind": "call", "body": body }),
                _ if *kind == "loop" => json!({ "kind": "loop", "loopConfig": {} }),
                _ => json!({ "kind": kind }),
            };
            group["id"] = json!(id);
            group["parentGroupId"] = json!(parent);
            group
        }).collect();
        let edges: Vec<Value> = wires.iter().map(|(source, sp, via, target, tp)| json!({
            "id": format!("{source}.{sp}->{target}.{tp}"), "source": source, "sourceHandle": sp,
            "target": target, "targetHandle": tp,
            "via": via.iter().map(|(group, port, side)| json!({ "group": group, "port": port, "side": side })).collect::<Vec<_>>(),
        })).collect();
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000", "nodes": nodes, "edges": edges, "groups": groups,
        })).expect("test program")
    }

    struct Run {
        project: ProjectDefinition,
        index: ProgramIndex,
        given: Given,
        pulses: PulseTable,
        scopes: ScopeTable,
        kicked: KickTable,
    }

    impl Run {
        fn of(project: ProjectDefinition) -> Self {
            Self::given(project, Given::default())
        }

        /// A run of `project` given `given`, the program saying where.
        fn given(mut project: ProjectDefinition, given: Given) -> Self {
            project.given = given.places();
            let index = ProgramIndex::build(&project);
            Self { project, index, given, pulses: PulseTable::default(), scopes: ScopeTable::default(), kicked: KickTable::new() }
        }

        fn settle(&mut self, phase: Phase) -> TablePass {
            settle_table(&self.project, &self.index, &self.given, phase, RunId::nil(), 1, &mut self.pulses, &mut self.scopes, &mut self.kicked)
        }

        fn arrive(&mut self, target: &str, port: &str, frames: LoopFrames, value: Option<Value>) {
            let id = Uuid::new_v4();
            let pulse = match value {
                Some(value) => Pulse::new(id, RunId::nil(), frames, target, port, Arc::new(value)),
                None => Pulse::closure(id, RunId::nil(), frames, target, port),
            };
            self.pulses.entry(target.to_string()).or_default().push(pulse);
        }

        fn state(&self, scope: &str, frames: LoopFrames) -> Option<&ScopeState> {
            self.scopes.get(&ScopeCopy::new(scope, frames)).map(|record| &record.state)
        }

        fn kick(&self, node: &str, frames: LoopFrames) -> Option<&KickedNode> {
            self.kicked.get(&FiringLocation::new(node, frames))
        }
    }

    /// Whether a scope has a copy decided answers what a walk over every
    /// copy would, whatever the copies' frames, beside scopes whose names
    /// sort right next to it.
    #[test]
    fn a_scope_with_a_copy_decided_is_found_without_walking_the_copies() {
        let mut table = ScopeTable::default();
        let walked = |table: &ScopeTable, scope: &str| table.iter().any(|(copy, _)| copy.scope == scope);
        let names = ["g", "g.inner", "f", "h", "gg"];
        for name in names {
            assert_eq!(table.decided_any(name), walked(&table, name));
        }
        for index in 0..50 {
            table.decide(ScopeCopy::new("g.inner", vec![Frame::Loop { index }]), ScopeState::Open, 0);
        }
        table.decide(ScopeCopy::new("gg", vec![Frame::Loop { index: 3 }, Frame::Loop { index: 1 }]), ScopeState::Open, 0);
        for name in names {
            assert_eq!(table.decided_any(name), walked(&table, name), "{name}");
        }
        assert!(!table.decided_any("g") && table.decided_any("g.inner") && table.decided_any("gg"));
        table.decide(ScopeCopy::new("g", vec![]), ScopeState::Open, 0);
        assert!(table.decided_any("g"));
    }

    /// `flag -> g._should_flow`; `g` holds `m`, which has no input.
    fn gated() -> ProjectDefinition {
        program(
            &[("flag", &[], &[], &["out"], false), ("g.m", &["g"], &[], &["out"], false)],
            &[("g", "group", None)],
            &[("flag", "out", &[], "g", "_should_flow")],
        )
    }

    /// A gated copy waits for its gate, and its member no wire feeds is
    /// not started before the copy opens; a true gate opens it and starts
    /// the member.
    #[test]
    fn a_gated_scope_opens_on_its_gate_and_only_then_starts_its_unfed_members() {
        let mut run = Run::of(gated());
        run.settle(Phase::Fire);
        assert_eq!(run.state("g", vec![]), None, "it waits for its gate");
        assert!(run.kick("g.m", vec![]).is_none(), "nothing inside starts before the copy opens");
        run.arrive("g", "_should_flow", vec![], Some(json!(true)));
        let pass = run.settle(Phase::Fire);
        assert_eq!(pass.decided, vec![(ScopeCopy::new("g", vec![]), ScopeState::Open)]);
        let kick = run.kick("g.m", vec![]).expect("the member starts with the copy");
        assert_eq!(kick.scope_skipped, None);
        assert!(run.pulses["g"].is_empty(), "the decision took the gate's value off its wire");
        assert_eq!(run.pulses.all_absorbed().filter(|pulse| pulse.target_node == "g").count(), 1, "the gate's value is taken");
    }

    /// A false or closed gate takes the copy down: everything inside is
    /// told, the member kicked straight into its skip.
    #[test]
    fn a_false_or_closed_gate_does_not_run_the_scope() {
        for (gate, reason) in [(Some(json!(false)), SkipReason::DidNotFlow), (None, SkipReason::FlowClosed { failure: None })] {
            let mut run = Run::of(gated());
            run.arrive("g", "_should_flow", vec![], gate);
            run.settle(Phase::Fire);
            assert_eq!(run.state("g", vec![]), Some(&ScopeState::Skipped { reason }));
            assert_eq!(run.kick("g.m", vec![]).and_then(|kick| kick.scope_skipped.as_deref()), Some("g"));
        }
    }

    /// `src -> g.x -> g.r`, `g.r -> g.y -> sink`, and `g.u` with no input:
    /// with no gate the copy opens on the first value from outside.
    fn fed() -> ProjectDefinition {
        program(
            &[
                ("src", &[], &[], &["out"], false),
                ("g.r", &["g"], &["in"], &["out"], false),
                ("g.u", &["g"], &[], &["out"], false),
                ("sink", &[], &["in"], &[], false),
            ],
            &[("g", "group", None)],
            &[
                ("src", "out", &[("g", "x", "in")], "g.r", "in"),
                ("g.r", "out", &[("g", "y", "out")], "sink", "in"),
            ],
        )
    }

    #[test]
    fn an_ungated_scope_opens_on_the_first_value_that_reaches_it() {
        let mut run = Run::of(fed());
        run.settle(Phase::Fire);
        assert_eq!(run.state("g", vec![]), None, "nothing came yet");
        assert!(run.kick("g.u", vec![]).is_none());
        run.arrive("g.r", "in", vec![], Some(json!("v")));
        run.settle(Phase::Fire);
        assert_eq!(run.state("g", vec![]), Some(&ScopeState::Open));
        assert!(run.kick("g.u", vec![]).is_some(), "the member with no input starts as the copy opens");
        assert!(run.pulses["g.r"][0].status.is_pending(), "a member's own input is its own to take");
    }

    /// Everything that came in closed with nothing on it: the copy does
    /// not run, and what leaves it closes, so the consumer outside reads
    /// "nothing" instead of waiting.
    #[test]
    fn an_ungated_scope_whose_every_input_closed_does_not_run_and_closes_what_leaves_it() {
        let mut run = Run::of(fed());
        run.arrive("g.r", "in", vec![], None);
        run.settle(Phase::Fire);
        assert_eq!(run.state("g", vec![]), Some(&ScopeState::Skipped { reason: SkipReason::EveryInputClosed { failure: None } }));
        let at_sink: Vec<&Pulse> = run.pulses["sink"].iter().collect();
        assert!(at_sink.len() == 1 && at_sink[0].closed && at_sink[0].status.is_pending(), "{at_sink:?}");
        assert_eq!(run.kick("g.r", vec![]).and_then(|kick| kick.scope_skipped.as_deref()), Some("g"));
        assert!(run.pulses["g.r"].is_empty(), "a pulse inside a copy that did not run leaves its wire");
        assert_eq!(run.pulses.all_absorbed().filter(|pulse| pulse.target_node == "g.r").count(), 1, "a pulse inside a copy that did not run is absorbed");
    }

    /// A scope nothing reaches from outside opens with the run.
    #[test]
    fn a_scope_nothing_reaches_opens_at_once() {
        let mut run = Run::of(program(&[("g.u", &["g"], &[], &[], false)], &[("g", "group", None)], &[]));
        run.settle(Phase::Fire);
        assert_eq!(run.state("g", vec![]), Some(&ScopeState::Open));
        assert!(run.kick("g.u", vec![]).is_some());
    }

    /// A gate written in the group's braces decides at once: `false`
    /// keeps everything inside off.
    #[test]
    fn a_written_gate_decides_at_once() {
        let mut project = program(&[("g.u", &["g"], &[], &[], false)], &[("g", "group", None)], &[]);
        project.groups[0].port_literals.insert("_should_flow".into(), json!(false));
        let mut run = Run::of(project);
        run.settle(Phase::Fire);
        assert_eq!(run.state("g", vec![]), Some(&ScopeState::Skipped { reason: SkipReason::DidNotFlow }));
        assert_eq!(run.kick("g.u", vec![]).and_then(|kick| kick.scope_skipped.as_deref()), Some("g"));
    }

    /// A written gate that lets the group flow decides at once, as a wired
    /// one does when its value arrives: the group opens without waiting on
    /// what comes in from outside.
    #[test]
    fn a_written_gate_that_flows_opens_at_once() {
        let mut project = program(
            &[("src", &[], &[], &["out"], false), ("g.m", &["g"], &["in"], &[], false)],
            &[("g", "group", None)],
            &[("src", "out", &[("g", "x", "in")], "g.m", "in")],
        );
        project.groups[0].port_literals.insert("_should_flow".into(), json!(true));
        let mut run = Run::of(project);
        run.settle(Phase::Fire);
        assert_eq!(run.state("g", vec![]), Some(&ScopeState::Open));
    }

    /// A wire into a scope that only brings back what something inside it
    /// sent out is never what opens it: with nothing else coming in, the
    /// scope opens at once and starts its input-less member, instead of
    /// waiting on a value that needs it open first.
    #[test]
    fn a_wire_that_only_brings_back_what_the_scope_sent_is_not_waited_on() {
        let mut run = Run::of(program(
            &[
                ("G.u", &["G"], &[], &["out"], false),
                ("G.m", &["G"], &["in"], &[], false),
                ("S", &[], &["in"], &["out"], false),
            ],
            &[("G", "group", None)],
            &[
                ("G.u", "out", &[("G", "o", "out")], "S", "in"),
                ("S", "out", &[("G", "x", "in")], "G.m", "in"),
            ],
        ));
        run.settle(Phase::Fire);
        assert_eq!(run.state("G", vec![]), Some(&ScopeState::Open));
        assert!(run.kick("G.u", vec![]).is_some());
    }

    /// Two calls of one included file chained (`two` reads what `one`
    /// sends): the wire from `one` into `two` runs from the file's node to
    /// the file's node, and is still a value from outside `two`, so `two`
    /// waits on it like on any other.
    #[test]
    fn a_wire_from_one_call_into_another_is_waited_on() {
        let mut run = Run::of(program(
            &[
                ("src", &[], &[], &["out"], false),
                ("B.k", &["B"], &[], &["out"], false),
                ("B.n", &["B"], &["x"], &["y"], false),
                ("sink", &[], &["in"], &[], false),
            ],
            &[("one", "call:B", None), ("two", "call:B", None), ("B", "body", None)],
            &[
                ("src", "out", &[("one", "x", "in"), ("B", "x", "in")], "B.n", "x"),
                ("B.n", "y", &[("B", "y", "out"), ("one", "y", "out"), ("two", "x", "in"), ("B", "x", "in")], "B.n", "x"),
                ("B.n", "y", &[("B", "y", "out"), ("two", "y", "out")], "sink", "in"),
            ],
        ));
        run.settle(Phase::Fire);
        assert_eq!(run.state("two", vec![]), None, "two waits for what one sends");
        assert!(run.kick("B.k", vec![Frame::Call { site: "two".into() }]).is_none());
    }

    /// The same feedback inside an included file: a group of the file whose
    /// output comes back into it through a node of the file is not held up
    /// by it, under each call.
    #[test]
    fn a_wire_that_brings_back_what_a_scope_sent_inside_an_included_file_is_not_waited_on() {
        let mut run = Run::of(program(
            &[
                ("B.G.x", &["B", "B.G"], &[], &["out"], false),
                ("B.G.m", &["B", "B.G"], &["in"], &[], false),
                ("B.y", &["B"], &["in"], &["out"], false),
            ],
            &[("s", "call:B", None), ("B", "body", None), ("B.G", "group", Some("B"))],
            &[
                ("B.G.x", "out", &[("B.G", "o", "out")], "B.y", "in"),
                ("B.y", "out", &[("B.G", "i", "in")], "B.G.m", "in"),
            ],
        ));
        run.settle(Phase::Fire);
        let call = vec![Frame::Call { site: "s".into() }];
        assert_eq!(run.state("B.G", call.clone()), Some(&ScopeState::Open));
        assert!(run.kick("B.G.x", call).is_some());
    }

    /// A gate handed a value at a group start (`--group g='{"_should_flow":
    /// false}'`) is the gate's: the scope decides at once, whatever its
    /// gate wire would bring.
    #[test]
    fn a_handed_gate_decides_at_once() {
        let project = program(
            &[("src", &[], &[], &["flow"], false), ("g.u", &["g"], &[], &[], false)],
            &[("g", "group", None)],
            &[("src", "flow", &[], "g", "_should_flow")],
        );
        let mut given = Given::default();
        given.inputs.insert(Located::top("g"), [("_should_flow".to_string(), json!(false))].into());
        let mut run = Run::given(project, given);
        run.settle(Phase::Fire);
        assert_eq!(run.state("g", vec![]), Some(&ScopeState::Skipped { reason: SkipReason::DidNotFlow }));
        assert_eq!(run.kick("g.u", vec![]).and_then(|kick| kick.scope_skipped.as_deref()), Some("g"));
    }

    /// Two calls of one included file are two copies of its scopes: a
    /// value reaching one call opens that call's copy alone.
    #[test]
    fn each_call_of_an_included_file_is_its_own_copy() {
        let mut run = Run::of(program(
            &[
                ("one_src", &[], &[], &["out"], false),
                ("two_src", &[], &[], &["out"], false),
                ("B.n", &["B"], &["in"], &[], false),
                ("B.u", &["B"], &[], &[], false),
            ],
            &[("one", "call:B", None), ("two", "call:B", None), ("B", "body", None)],
            &[
                ("one_src", "out", &[("one", "x", "in"), ("B", "x", "in")], "B.n", "in"),
                ("two_src", "out", &[("two", "x", "in"), ("B", "x", "in")], "B.n", "in"),
            ],
        ));
        let one = vec![Frame::Call { site: "one".into() }];
        let two = vec![Frame::Call { site: "two".into() }];
        run.arrive("B.n", "in", one.clone(), Some(json!("v")));
        run.settle(Phase::Fire);
        assert_eq!(run.state("one", vec![]), Some(&ScopeState::Open));
        assert_eq!(run.state("B", one.clone()), Some(&ScopeState::Open));
        assert!(run.kick("B.u", one).is_some());
        assert_eq!(run.state("two", vec![]), None, "the other call still waits");
        assert!(run.kick("B.u", two).is_none());
    }

    /// Each iteration of a loop is its own copy, and a group inside the
    /// loop is decided once per iteration.
    #[test]
    fn each_iteration_decides_the_groups_inside_it_on_its_own() {
        let mut run = Run::of(program(
            &[("L.flag", &["L"], &[], &["out"], false), ("L.g.m", &["L", "L.g"], &[], &[], false)],
            &[("L", "loop", None), ("L.g", "group", Some("L"))],
            &[("L.flag", "out", &[], "L.g", "_should_flow")],
        ));
        let first = vec![Frame::Loop { index: 0 }];
        let second = vec![Frame::Loop { index: 1 }];
        for frames in [&first, &second] {
            open_iteration(&run.project, &run.index, "L", frames, 1, &mut run.scopes, &mut run.kicked);
        }
        assert!(run.kick("L.flag", first.clone()).is_some() && run.kick("L.flag", second.clone()).is_some());
        run.arrive("L.g", "_should_flow", first.clone(), Some(json!(true)));
        run.arrive("L.g", "_should_flow", second.clone(), Some(json!(false)));
        run.settle(Phase::Fire);
        assert_eq!(run.state("L.g", first.clone()), Some(&ScopeState::Open));
        assert_eq!(run.state("L.g", second.clone()), Some(&ScopeState::Skipped { reason: SkipReason::DidNotFlow }));
        assert_eq!(run.kick("L.g.m", first).map(|kick| kick.scope_skipped.is_none()), Some(true));
        assert_eq!(run.kick("L.g.m", second).and_then(|kick| kick.scope_skipped.as_deref()), Some("L.g"));
    }

    /// A group inside a loop fed by a node of the loop's body waits for
    /// that node in each iteration (it runs before the group, not after):
    /// with nothing coming, the group's copy does not run, and its
    /// input-less member never starts.
    #[test]
    fn a_group_fed_from_its_loops_body_waits_for_it_in_each_iteration() {
        let mut run = Run::of(program(
            &[
                ("L.p", &["L"], &[], &["out"], false),
                ("L.g.m", &["L", "L.g"], &["in"], &[], false),
                ("L.g.u", &["L", "L.g"], &[], &[], false),
            ],
            &[("L", "loop", None), ("L.g", "group", Some("L"))],
            &[("L.p", "out", &[("L.g", "x", "in")], "L.g.m", "in")],
        ));
        let first = vec![Frame::Loop { index: 0 }];
        open_iteration(&run.project, &run.index, "L", &first, 1, &mut run.scopes, &mut run.kicked);
        run.settle(Phase::Fire);
        assert_eq!(run.state("L.g", first.clone()), None, "the copy waits for L.p");
        run.arrive("L.g.m", "in", first.clone(), None);
        run.settle(Phase::Fire);
        assert_eq!(run.state("L.g", first.clone()), Some(&ScopeState::Skipped { reason: SkipReason::EveryInputClosed { failure: None } }));
        assert_eq!(run.kick("L.g.u", first).and_then(|kick| kick.scope_skipped.as_deref()), Some("L.g"));
    }

    /// A group a copy that did not run holds is decided with it, and the
    /// decision says why.
    #[test]
    fn a_scope_inside_one_that_did_not_run_is_decided_with_it() {
        let mut run = Run::of(program(
            &[("flag", &[], &[], &["out"], false), ("g.h.m", &["g", "g.h"], &[], &[], false)],
            &[("g", "group", None), ("g.h", "group", Some("g"))],
            &[("flag", "out", &[], "g", "_should_flow")],
        ));
        run.arrive("g", "_should_flow", vec![], Some(json!(false)));
        let pass = run.settle(Phase::Fire);
        assert_eq!(pass.decided, vec![
            (ScopeCopy::new("g", vec![]), ScopeState::Skipped { reason: SkipReason::DidNotFlow }),
            (ScopeCopy::new("g.h", vec![]), ScopeState::Skipped { reason: SkipReason::ScopeSkipped { scope: "g".into() } }),
        ]);
        assert_eq!(run.kick("g.h.m", vec![]).and_then(|kick| kick.scope_skipped.as_deref()), Some("g"));
    }

    /// What reaches a trigger at Fire is dropped (a fired trigger replays
    /// its setup's capture); a setup feeds it.
    #[test]
    fn a_fire_drops_what_reaches_a_trigger_and_a_setup_keeps_it() {
        let project = program(&[("trig", &[], &["in"], &[], true)], &[], &[]);
        let mut fire = Run::of(project.clone());
        fire.arrive("trig", "in", vec![], Some(json!("v")));
        assert_eq!(fire.settle(Phase::Fire).dropped.len(), 1);
        assert!(!fire.pulses.contains_key("trig"));
        let mut setup = Run::of(project);
        setup.arrive("trig", "in", vec![], Some(json!("v")));
        assert!(setup.settle(Phase::TriggerSetup).dropped.is_empty());
        assert!(setup.pulses["trig"][0].status.is_pending());
    }
}
