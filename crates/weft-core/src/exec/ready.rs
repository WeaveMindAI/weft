//! Readiness. Find which nodes have enough pending pulses to fire at
//! a matching `(color, frames)`, aggregate their inputs, return as
//! `ReadyGroup`s.
//!
//! Matching is exact-frame: a firing at `(color, frames)` only sees
//! pulses whose `frames` are exactly the firing's frame stack. Loops
//! emit broadcast inputs and the implicit `self.index` at the body's
//! own frame stack directly, one pulse per iteration.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::exec::skip::{check_flow_permission, check_should_skip, SkipReason};
use crate::frames::{Located, LoopFrames};
use crate::project::{Edge, EdgeIndex, GroupBoundaryRole, NodeDefinition, ProjectDefinition};
use crate::primitive::Phase;
use crate::pulse::{Pulse, PulseStatus, PulseTable};
use crate::weft_type::WeftType;
use crate::Color;

/// A firing's input ports and the values on them, shared with the
/// pulses that carried them. The node body gets its own copy
/// ([`owned_bag`]); a boundary forwards the shared values as they are.
pub type InputBag = BTreeMap<String, Arc<Value>>;

/// The one owned copy of a bag: what a node body receives.
pub fn owned_bag(bag: &InputBag) -> Map<String, Value> {
    bag.iter().map(|(k, v)| (k.clone(), (**v).clone())).collect()
}

/// One dispatch ready to fire. `input` is the aggregated inputs;
/// `pulse_ids` are the pulses that will be absorbed when the caller
/// commits the dispatch.
pub struct ReadyGroup {
    pub frames: LoopFrames,
    pub color: Color,
    pub received: FiringInput,
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

/// THE single rule for "which pulse does a firing see on `port`?".
/// `group_pulses` are the pulses this firing sees: the pending ones at
/// its exact `(color, frames)` when readiness forms the group, or the
/// ones a record absorbed when the journal fold rebuilds its input.
/// Exact-frame matching happens where that slice is built.
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
/// Returns `None` when nothing reaches this firing on this port.
pub fn resolve_port_value<'a>(group_pulses: &[&'a Pulse], port: &str) -> Option<&'a Pulse> {
    let mut winner: Option<&Pulse> = None;
    for p in group_pulses.iter().copied().filter(|p| p.target_port == port) {
        winner = Some(match winner {
            None => p,
            Some(current) => {
                if pulse_rank(p) > pulse_rank(current) { p } else { current }
            }
        });
    }
    winner
}

/// Resolve authored backups only after the ordinary supplier has ended or
/// is absent. These derived input facts are never journaled as wire emissions.
pub fn effective_input_pulses(
    node: &NodeDefinition,
    actual: &[&Pulse],
    wired: &HashSet<&str>,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    color: Color,
    frames: &LoopFrames,
) -> Vec<Pulse> {
    let mut effective: Vec<_> = actual.iter().map(|pulse| (*pulse).clone()).collect();
    let Some(selection) = edge_idx.selection() else { return effective };
    let at = Located::at(&node.id, frames);
    for port in &node.inputs {
        if !selection.includes_port(&at, node, &port.name) { continue; }
        let winner = resolve_port_value(actual, &port.name);
        if winner.is_some_and(|pulse| !pulse.closed || pulse.close_error.is_some()) { continue; }
        if winner.is_none() && selection.has_supplier(project, &at, &port.name) { continue; }
        if winner.is_none() && !wired.contains(port.name.as_str())
            && node.port_literals.get(&port.name).is_some_and(|v| literal_is_data(node, &port.name, v))
        { continue; }
        if matches!(port.port_type, WeftType::Generator(_)) { continue; }
        let backup = selection.input.get(&at).and_then(|ports| ports.get(&port.name));
        if let Some(value) = backup {
            let mut pulse = Pulse::new(uuid::Uuid::nil(), color, frames.clone(), &node.id, &port.name, Arc::new(value.clone()));
            pulse.provided = true;
            pulse.backup = true;
            pulse.inherited_from = selection.input_origins.get(&at).and_then(|ports| ports.get(&port.name)).copied();
            effective.push(pulse);
        } else if winner.is_none() && wired.contains(port.name.as_str()) {
            effective.push(Pulse::closure(uuid::Uuid::nil(), color, frames.clone(), &node.id, &port.name));
        }
    }
    effective
}

/// Higher rank wins. Data (1) > closure (0).
fn pulse_rank(p: &Pulse) -> u8 {
    if p.closed { 0 } else { 1 }
}

/// The input ports of `node_id` a wire feeds at `frames`. What
/// readiness waits on, and what the input bag treats as authoritative
/// over body literals.
pub fn wired_inputs<'a>(
    project: &'a ProjectDefinition,
    edge_idx: &EdgeIndex,
    node_id: &str,
    frames: &LoopFrames,
) -> HashSet<&'a str> {
    edge_idx
        .get_incoming(project, node_id, frames)
        .iter()
        .map(|e| e.target_handle.as_deref().unwrap_or("default"))
        .collect()
}

/// Whether `port` is an input of `node` that takes a stream
/// (`Generator[T]`). THE one derivation of "is this a stream input":
/// every reader (the live dispatch, the stream routing pass, the
/// journal fold, the skip and closure sweeps, the loop source) asks
/// here, so none can disagree on which pulses a start leaves pending.
pub fn is_generator_input(node: &NodeDefinition, port: &str) -> bool {
    node.inputs.iter().any(|p| p.name == port && p.is_generator())
}

/// The input ports of `node` that take a stream. A firing that RUNS
/// never absorbs the pulses on these at dispatch: they are the
/// stream's items and its end, taken one by one through the firing's
/// live feed.
pub fn generator_inputs(node: &NodeDefinition) -> HashSet<&str> {
    node.inputs
        .iter()
        .filter(|p| is_generator_input(node, &p.name))
        .map(|p| p.name.as_str())
        .collect()
}

/// The pulses a settle pass took out of the run: see
/// [`settle_out_of_run`].
#[derive(Debug, Default)]
pub struct OutOfRun {
    /// Pending pulses on nodes outside the run's node set, now
    /// Absorbed.
    pub absorbed: Vec<uuid::Uuid>,
    /// Pending pulses into a trigger at Fire, removed from the table.
    pub dropped: Vec<uuid::Uuid>,
}

/// Settle what this run will never dispatch, with no row of its own:
/// a pulse on a node outside `dispatchable` (a setup phase's closure,
/// a fire's subgraph: engine plumbing the user never chose, or another
/// program's node, so neither is this run's business to paint) is
/// absorbed as it lands; at Fire, a pulse into a trigger is dropped
/// with its bucket (a trigger's ports replay its setup-time snapshot,
/// so an upstream node that ran for the output path must not
/// re-dispatch it with a live value). Runs after every change to the
/// table on both sides (the worker's turn, the fold's row), so the two
/// hold the same pulses.
pub fn settle_out_of_run(
    project: &ProjectDefinition,
    phase: Phase,
    dispatchable: Option<&HashSet<Located>>,
    pulses: &mut PulseTable,
) -> OutOfRun {
    let mut out = OutOfRun::default();
    if let Some(dispatchable) = dispatchable {
        for node in &project.nodes {
            if let Some(bucket) = pulses.get_mut(&node.id) {
                for p in bucket.iter_mut().filter(|p| p.status == PulseStatus::Pending
                    && !dispatchable.contains(&Located::at(&node.id, &p.frames)))
                {
                    p.absorb();
                    out.absorbed.push(p.id);
                }
            }
        }
    }
    if matches!(phase, Phase::Fire) {
        for trigger in project.nodes.iter().filter(|n| n.features.is_trigger) {
            if let Some(bucket) = pulses.remove(&trigger.id) {
                out.dropped.extend(bucket.iter().filter(|p| p.status == PulseStatus::Pending).map(|p| p.id));
            }
        }
    }
    out
}

/// Whether `edge` lands on a stream input of its target. Two equal
/// items on such an edge are two items, and a closure (the stream's
/// end) coexists with still-buffered ones, so every dedup and every
/// pending-value rule exempts these edges.
pub fn edge_targets_generator(project: &ProjectDefinition, edge: &Edge) -> bool {
    let handle = edge.target_handle.as_deref().unwrap_or("default");
    project.nodes.iter().find(|n| n.id == edge.target).is_some_and(|n| is_generator_input(n, handle))
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
    dispatchable: Option<&HashSet<Located>>,
) -> Vec<(String, ReadyGroup)> {
    find_ready_among(project, project.nodes.iter(), pulses, edge_idx, dispatchable)
        .into_iter()
        .map(|(node, group)| (node.id.clone(), group))
        .collect()
}

/// `find_ready_nodes` over a chosen set of nodes: the boundary pass
/// asks only about the group boundaries, after every journal row, so
/// it must not walk the whole program each time.
pub fn find_ready_among<'a>(
    project: &ProjectDefinition,
    candidates: impl IntoIterator<Item = &'a NodeDefinition>,
    pulses: &PulseTable,
    edge_idx: &EdgeIndex,
    dispatchable: Option<&HashSet<Located>>,
) -> Vec<(&'a NodeDefinition, ReadyGroup)> {
    let mut result = Vec::new();

    for node in candidates {
        let Some(node_pulses) = pulses.get(&node.id) else {
            continue;
        };
        let pending: Vec<&Pulse> = node_pulses.iter().filter(|p| p.status.is_pending()).collect();
        if pending.is_empty() {
            continue;
        }
        // Group pulses by (color, frames). A firing is one exact point in
        // frame space; matching is exact, and which wires feed the node
        // is a fact of that point too (a node of an included file is
        // wired by the call it fires under).
        let mut groups: Vec<((Color, LoopFrames), Vec<&Pulse>)> = Vec::new();
        for p in pending {
            let key = (p.color, p.frames.clone());
            match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, group)) => group.push(p),
                None => groups.push((key, vec![p])),
            }
        }
        for ((color, frames), group_pulses) in groups {
            let out_of_scope = dispatchable.is_some_and(|s| !s.contains(&Located::at(&node.id, &frames)));
            let wired = wired_inputs(project, edge_idx, &node.id, &frames);
            let required: HashSet<&str> = node
                .inputs
                .iter()
                .filter(|p| p.required && edge_idx.includes_port(node, &frames, &p.name))
                .map(|p| p.name.as_str())
                .collect();
            let mut literal_filled: HashSet<&str> = HashSet::new();
            for (name, value) in &node.port_literals {
                if edge_idx.includes_port(node, &frames, name) && !wired.contains(name.as_str()) && literal_is_data(node, name, value) {
                    literal_filled.insert(name.as_str());
                }
            }
            if let Some(group) = ready_group_at(
                node, &group_pulses, color, &frames, &required, &wired, &literal_filled, out_of_scope, project, edge_idx,
            ) {
                result.push((node, group));
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Per-firing matching
// ---------------------------------------------------------------------------

/// The ready group the pending pulses at one exact `(color, frames)`
/// form for `node`, if they make it ready. `wired` are the ports a wire
/// feeds at that point, `required` the required ones the run reads.
#[allow(clippy::too_many_arguments)]
fn ready_group_at(
    node: &NodeDefinition,
    group_pulses: &[&Pulse],
    color: Color,
    frames: &LoopFrames,
    required: &HashSet<&str>,
    wired: &HashSet<&str>,
    literal_filled: &HashSet<&str>,
    out_of_scope: bool,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
) -> Option<ReadyGroup> {
    let has_incoming = !wired.is_empty();
    let effective = effective_input_pulses(node, group_pulses, wired, project, edge_idx, color, frames);
    let effective_refs: Vec<_> = effective.iter().collect();
    let all_satisfied = wired.iter().all(|port_name| {
        effective_refs.iter().any(|p| p.target_port == *port_name)
    });

    // An out-of-scope node never waits for its full input set: it
    // will not run, so any pulse that lands is absorbed by a skip
    // dispatch right away (see `find_ready_nodes`'s doc).
    if has_incoming && !all_satisfied && !out_of_scope {
        return None;
    }

    let received = firing_input(node, &effective_refs, wired, frames, edge_idx);

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
        // (`kicked_group` runs `check_flow_permission`).
        None
    } else {
        let mut filled = literal_filled.clone();
        filled.extend(effective.iter().filter(|p| p.backup && !p.closed).map(|p| p.target_port.as_str()));
        check_should_skip(node, &effective_refs, required, wired, &filled)
    };

    let pulse_ids: Vec<uuid::Uuid> = group_pulses.iter().map(|p| p.id).collect();

    Some(ReadyGroup {
        frames: frames.clone(),
        color,
        error: received.error(),
        received,
        skip,
        pulse_ids,
        out_of_scope,
    })
}

/// The ready group a KICK synthesizes for `node` at `frames`: an entry
/// node (a firing trigger, a manual-run root, a scope body's root) has
/// no wired pending inputs, so the scheduler builds its bag from the
/// node's written constants (and a firing trigger's setup-time port
/// snapshot). A kicked node still answers to `_should_flow`: a `false`
/// literal in its braces turns it off; a kick from a scope that was
/// gated off is dispatched straight into its skip. The ONE derivation
/// of a kick's dispatch, shared by the live scheduler and the journal
/// fold (for the group boundaries it dispatches itself).
pub fn kicked_group(
    node: &NodeDefinition,
    kick: &crate::primitive::KickedNode,
    frames: &LoopFrames,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
) -> ReadyGroup {
    let wired = wired_inputs(project, edge_idx, &node.id, frames);
    let effective = effective_input_pulses(node, &[], &wired, project, edge_idx, color, frames);
    let effective_refs: Vec<_> = effective.iter().collect();
    let received = if kick.firing {
        let (input, errors) = build_kicked_input(node, kick.port_snapshot.as_ref());
        FiringInput { input, type_errors: errors, ..Default::default() }
    } else {
        firing_input(node, &effective_refs, &wired, frames, edge_idx)
    };
    let skip = match &kick.scope_skipped {
        Some(scope) => Some(SkipReason::ScopeSkipped { scope: scope.clone() }),
        // Trigger preparation already evaluated its wired gate. A fire
        // requires that capture and reads its baked settings. Live pulses
        // into triggers are dropped by settle_out_of_run; enclosing group
        // gates still decide scope_permission before this kick dispatches.
        None if kick.firing => check_flow_permission(node, &[]),
        None => check_should_skip(node, &effective_refs,
            &node.inputs.iter().filter(|p| p.required && edge_idx.includes_port(node, frames, &p.name)).map(|p| p.name.as_str()).collect(),
            &wired, &received.input.keys().map(String::as_str).collect()),
    };
    ReadyGroup {
        frames: frames.clone(),
        color,
        error: received.error(),
        received,
        skip,
        // A kick absorbs nothing, so it must fold nothing in: a pulse
        // read into the bag here would stay pending for ever (absorption
        // is driven by this list) and the execution could never reach
        // Completed. Whatever is waiting on a wire is read by the
        // ordinary pulse-driven dispatch instead, or, for a trigger in
        // the firing phase, dropped by `settle_out_of_run`.
        pulse_ids: Vec::new(),
        // A kicked node whose own ports refuse their values fails the
        // same way a pulsed one does.
        out_of_scope: false,
    }
}

// ---------------------------------------------------------------------------
// Input aggregation
// ---------------------------------------------------------------------------

/// What a firing sees on its ports: the bag its body reads, the wired
/// ports whose pulse was a closure, and every value a port refused.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FiringInput {
    pub input: InputBag,
    /// Wired ports whose resolved pulse for this firing was a CLOSURE
    /// (the upstream terminated without firing the port). Disjoint
    /// from the keys of `input`: closures carry no data, so they never
    /// appear there. A generator port never lists as closed: its
    /// closure is the stream's END (an empty stream when no items
    /// preceded it), a value the firing consumes through its live
    /// feed, not a structural "nothing arrived" (see the skip module
    /// doc).
    pub closed_ports: Vec<String>,
    /// The subset of `closed_ports` whose closure carries WHY: the
    /// producer failed (or was cancelled) rather than declining to emit,
    /// keyed by port, with the error text. A boundary forwarding a
    /// closure keeps this on the same-named output, so "broke" never
    /// reads as "nothing there" one scope level up (the inverted gate
    /// tells the two apart).
    pub closed_with_error: BTreeMap<String, String>,
    pub type_errors: Vec<String>,
    pub provided_ports: Vec<String>,
    pub backup_ports: Vec<String>,
    pub inherited_ports: BTreeMap<String, Color>,
}

impl FiringInput {
    pub fn error(&self) -> Option<String> {
        (!self.type_errors.is_empty()).then(|| self.type_errors.join("; "))
    }
}

/// Build a firing's view of its ports from the pulses it sees. THE
/// one derivation, shared by readiness (over the pending pulses at the
/// firing's exact key) and the journal fold (over the pulses a record
/// absorbed, when it rebuilds what the firing received). The one copy
/// of each value the bag takes is the copy the node body owns.
pub fn firing_input(
    node: &NodeDefinition,
    group_pulses: &[&Pulse],
    wired: &HashSet<&str>,
    frames: &LoopFrames,
    edge_idx: &EdgeIndex,
) -> FiringInput {
    let mut obj = InputBag::new();

    // Per-port value resolution via the shared `resolve_port_value`
    // (also used by `skip::port_arrived_closed`). Enumerate distinct
    // ports that have any pulse in this firing's view, then resolve
    // each once. This and the skip layer share ONE definition of
    // "which pulse does this port see" so the two can never disagree.
    let distinct_ports: HashSet<&str> = group_pulses.iter().map(|p| p.target_port.as_str()).collect();
    for port in distinct_ports {
        if let Some(winner) = resolve_port_value(group_pulses, port) {
            if winner.closed {
                continue;
            }
            obj.insert(port.to_string(), winner.value.clone());
        }
    }

    fill_input_from_literals(node, wired, &mut obj);
    obj.retain(|port, _| edge_idx.includes_port(node, frames, port));

    // Runtime type enforcement on input ports: the single check point
    // (see `check_input`). A mismatch on a required port aggregates
    // into `type_errors` (the node fails loudly); a mismatch on an
    // optional port nulls the port and the node proceeds.
    let type_errors = check_bag(node, &mut obj);

    let mut closed_ports: Vec<String> = wired
        .iter()
        .filter(|port_name| {
            !is_generator_input(node, port_name)
                && resolve_port_value(group_pulses, port_name)
                    .map(|p| p.closed)
                    .unwrap_or(false)
        })
        .map(|p| p.to_string())
        .collect();
    closed_ports.sort();
    let closed_with_error: BTreeMap<String, String> = closed_ports
        .iter()
        .filter_map(|port| {
            resolve_port_value(group_pulses, port)
                .and_then(|p| p.close_error.clone())
                .map(|error| (port.clone(), error))
        })
        .collect();

    let mut provided_ports = Vec::new();
    let mut backup_ports = Vec::new();
    let mut inherited_ports = BTreeMap::new();
    for port in group_pulses.iter().map(|p| p.target_port.as_str()).collect::<std::collections::BTreeSet<_>>() {
        if let Some(winner) = resolve_port_value(group_pulses, port) {
            if winner.provided { provided_ports.push(port.to_string()); }
            if winner.backup { backup_ports.push(port.to_string()); }
            if let Some(origin) = winner.inherited_from { inherited_ports.insert(port.to_string(), origin); }
        }
    }
    FiringInput { input: obj, closed_ports, closed_with_error, type_errors, provided_ports, backup_ports, inherited_ports }
}

/// Runtime type enforcement on input ports: the single check point
/// (see `check_input`). A mismatch is returned and the node fails
/// loudly, on a required port and an optional one alike. The gate is
/// not data: `_should_flow` takes any value and only a `false` says
/// no (`check_flow_permission` is its one rule), so it is never held
/// to a type here.
fn check_bag(node: &NodeDefinition, obj: &mut InputBag) -> Vec<String> {
    let mut errors = Vec::new();
    for port in node.inputs.iter().filter(|p| !crate::exec::skip::is_gate_port(&p.name)) {
        let Some(value) = obj.get(&port.name) else {
            continue;
        };
        match check_input(port, value) {
            InputCheck::Ok => {}
            InputCheck::Fail(err) => {
                tracing::error!(target: "weft::exec::ready", node = %node.id, "{err}");
                errors.push(err);
                obj.insert(port.name.clone(), Arc::new(Value::Null));
            }
        }
    }
    errors
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
/// between the pulse-driven dispatch path (`firing_input`) and the
/// kick-driven dispatch path (`build_kicked_input` below) so the two
/// paths can't disagree on what counts as "body-supplied".
pub fn fill_input_from_literals(
    node: &NodeDefinition,
    wired: &HashSet<&str>,
    obj: &mut InputBag,
) {
    for (name, value) in &node.port_literals {
        if wired.contains(name.as_str()) || obj.contains_key(name) || !literal_is_data(node, name, value) {
            continue;
        }
        obj.insert(name.clone(), Arc::new(value.clone()));
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
) -> (InputBag, Vec<String>) {
    let mut obj = InputBag::new();
    // A firing trigger's ports replay the setup-time snapshot: seed the
    // bag from it (only keys naming declared input ports; the
    // snapshot is runtime-written so extras would be a writer bug, and
    // dropping them keeps the port contract the single source of shape).
    if let Some(snapshot) = port_snapshot.and_then(Value::as_object) {
        for (k, v) in snapshot {
            if node.inputs.iter().any(|p| p.name == *k) {
                obj.insert(k.clone(), Arc::new(v.clone()));
            }
        }
    }
    // Literals fill whatever the snapshot left unset. The wired set
    // stays empty on purpose: a wired port falls back to its literal
    // here, which is what a trigger fired with nothing upstream running
    // has always done.
    fill_input_from_literals(node, &HashSet::new(), &mut obj);
    // The same gate the pulse path runs (`check_input`). A trigger's
    // ports are exactly where a widget's domain rule earns its keep (a
    // poll interval of zero, a fractional one that would truncate), and
    // skipping the check here meant those rules bound nothing on the
    // one path that uses them.
    let errors = check_bag(node, &mut obj);
    (obj, errors)
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
    // A wrong-typed value is upstream sending something this port
    // cannot hold, and the firing fails whether the port is required
    // or not. Dropping it on an optional port and running on the
    // default used to hide a wiring bug as a node that ran on nothing.
    InputCheck::Fail(format!(
        "type mismatch on '{}': expected {}, got {}",
        port.name,
        declared,
        WeftType::infer(value)
    ))
}

#[cfg(test)]
mod tests {
    use crate::frames::{Frame, Located};
    use super::{build_kicked_input, check_input, resolve_port_value, InputCheck};
    use serde_json::Value;
    use crate::project::{EdgeIndex, InputDefinition, NodeDefinition, Position, ProjectDefinition};
    use super::{effective_input_pulses, firing_input};
    use std::collections::HashSet;
    use crate::pulse::Pulse;
    use crate::NodeFeatures;
    use serde_json::json;

    fn frame(i: u32) -> Frame {
        Frame::Loop { index: i }
    }

    fn data(color: uuid::Uuid, frames: Vec<Frame>, node: &str, port: &str, value: serde_json::Value) -> Pulse {
        Pulse::new(uuid::Uuid::new_v4(), color, frames, node, port, std::sync::Arc::new(value))
    }

    /// The settle pass absorbs what lands outside the run's node set
    /// and, at Fire, drops what lands on a trigger; a setup phase
    /// leaves the trigger's bucket alone, and an untargeted run
    /// absorbs nothing.
    #[test]
    fn the_settle_pass_absorbs_outside_the_run_and_drops_trigger_pulses_at_fire() {
        use crate::primitive::Phase;
        use crate::pulse::PulseStatus;
        let project: crate::project::ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "name": "settle", "description": null,
            "nodes": [
                { "id": "trig", "nodeType": "T", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                  "inputs": [{ "name": "in", "portType": "String", "required": true }], "outputs": [],
                  "features": { "isTrigger": true }, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                { "id": "inside", "nodeType": "T", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                  "inputs": [{ "name": "in", "portType": "String", "required": true }], "outputs": [],
                  "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                { "id": "outside", "nodeType": "T", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                  "inputs": [{ "name": "in", "portType": "String", "required": true }], "outputs": [],
                  "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }
            ],
            "edges": [], "groups": [],
            "createdAt": "1970-01-01T00:00:00Z", "updatedAt": "1970-01-01T00:00:00Z"
        }))
        .unwrap();
        let color = uuid::Uuid::nil();
        let table = || {
            let mut t = crate::pulse::PulseTable::default();
            for node in ["trig", "inside", "outside"] {
                t.entry(node.into()).or_default().push(data(color, vec![], node, "in", json!("v")));
            }
            t
        };
        let run: std::collections::HashSet<Located> = ["trig", "inside"].iter().map(|s| Located::top(*s)).collect();

        let mut fire = table();
        let out = super::settle_out_of_run(&project, Phase::Fire, Some(&run), &mut fire);
        assert_eq!(out.absorbed, vec![fire["outside"][0].id]);
        assert_eq!(fire["outside"][0].status, PulseStatus::Absorbed);
        assert_eq!(out.dropped.len(), 1, "the trigger's pending pulse is reported dropped");
        assert!(!fire.contains_key("trig"), "the trigger's bucket is gone");
        assert_eq!(fire["inside"][0].status, PulseStatus::Pending, "a pulse inside the run is untouched");

        let mut setup = table();
        let out = super::settle_out_of_run(&project, Phase::TriggerSetup, Some(&run), &mut setup);
        assert_eq!(out.absorbed.len(), 1);
        assert!(out.dropped.is_empty());
        assert_eq!(setup["trig"][0].status, PulseStatus::Pending, "a setup phase feeds the trigger");

        let mut untargeted = table();
        let out = super::settle_out_of_run(&project, Phase::Fire, None, &mut untargeted);
        assert!(out.absorbed.is_empty(), "no node set: nothing is outside the run");
        assert_eq!(out.dropped.len(), 1);
    }

    /// A firing sees only the pulses at its own frame stack: the group
    /// is formed by exact `(color, frames)`, so a shallower pulse is
    /// never in its view.
    #[test]
    fn groups_form_at_the_exact_frame_only() {
        let color = uuid::Uuid::nil();
        let firing_frames = vec![frame(0), frame(1)];
        let node = kicked_node("X", vec![port("String", true)], json!({}));
        let pulses = [
            data(color, vec![], "k", "p", json!("shallow")),
            data(color, vec![frame(0)], "k", "p", json!("mid")),
            data(color, firing_frames.clone(), "k", "p", json!("exact")),
        ];
        let wired: std::collections::HashSet<&str> = ["p"].into_iter().collect();
        let project = ProjectDefinition {
            id: color, nodes: vec![node.clone()], edges: vec![], groups: vec![],
            created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
        };
        let index = EdgeIndex::build(&project);
        let groups: Vec<_> = pulses.iter().map(|p| p.frames.clone()).collect::<std::collections::BTreeSet<_>>().into_iter()
            .filter_map(|frames| {
                let at: Vec<&Pulse> = pulses.iter().filter(|p| p.frames == frames).collect();
                super::ready_group_at(&node, &at, color, &frames, &std::collections::HashSet::new(), &wired,
                    &std::collections::HashSet::new(), false, &project, &index)
            }).collect();
        assert_eq!(groups.len(), 3, "one group per frame stack");
        let exact = groups.iter().find(|g| g.frames == firing_frames).expect("the exact group");
        assert_eq!(Value::Object(super::owned_bag(&exact.received.input)), json!({"p": "exact"}));
        assert_eq!(exact.pulse_ids, vec![pulses[2].id]);
    }

    #[test]
    fn resolve_port_value_prefers_data_over_closure_at_same_key() {
        let color = uuid::Uuid::nil();
        let frames = vec![];
        let pulses = [
            Pulse::closure(uuid::Uuid::new_v4(), color, frames.clone(), "n", "p"),
            data(color, frames.clone(), "n", "p", json!(42)),
        ];
        let view: Vec<&Pulse> = pulses.iter().collect();
        let winner = resolve_port_value(&view, "p").expect("a winner");
        assert_eq!(*winner.value, json!(42));
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
        assert_eq!(input.get("maybe").map(|v| &**v), Some(&json!(null)), "null is data on a nullable port");
        assert!(!input.contains_key("plain"), "null on a plain port is no value");
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

    /// An optional port used to swallow a wrong-typed value as a null
    /// and let the node run on its default; a wrong type is a wiring
    /// bug on any port, and the firing fails.
    #[test]
    fn mismatch_on_optional_fails_too() {
        match check_input(&port("String", false), &json!(42)) {
            InputCheck::Fail(msg) => assert!(msg.contains("type mismatch"), "{msg}"),
            other => panic!("expected Fail, got {other:?}"),
        }
        assert_eq!(check_input(&port("String", false), &json!(null)), InputCheck::Ok, "a null on an optional port is nothing arrived");
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
        assert_eq!(input.get("account").map(|v| &**v), Some(&handle), "the handle reaches the bag intact");
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
        assert_eq!(input.get("intervalSecs").map(|v| &**v), Some(&json!(null)));
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
            fires_with: Default::default(),
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
            include_contents: None,
            source_file: None,
        }
    }

    #[test]
    fn a_selected_boundary_cannot_receive_or_validate_excluded_literals_and_backups() {
        let mut node = kicked_node("Passthrough", vec![port("String", true)], json!({"p": 99}));
        let mut included = port("String", true);
        included.name = "selected".into();
        node.inputs.push(included);
        node.port_literals.insert("selected".into(), json!("kept"));
        let project = ProjectDefinition {
            id: uuid::Uuid::nil(), nodes: vec![node.clone()], edges: vec![], groups: vec![],
            created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
        };
        let mut selection = crate::project::selection::RunSelection::default();
        selection.nodes.insert(Located::top(&node.id));
        selection.boundary_ports.insert(Located::top(&node.id), ["selected".into()].into());
        selection.input.insert(Located::top(&node.id), [("p".into(), json!("unused"))].into());
        let index = EdgeIndex::selected(&project, selection);
        let effective = effective_input_pulses(&node, &[], &HashSet::new(), &project, &index, project.id, &vec![]);
        assert!(effective.is_empty());
        let received = firing_input(&node, &[], &HashSet::new(), &vec![], &index);
        assert_eq!(super::owned_bag(&received.input), json!({"selected":"kept"}).as_object().unwrap().clone());
        assert!(received.type_errors.is_empty(), "the excluded invalid literal cannot fail this boundary");
    }

    #[test]
    fn backups_wait_for_real_input_and_only_replace_absence() {
        let node = kicked_node("X", vec![port("String", true)], json!({}));
        let mut source = node.clone();
        source.id = "source".into();
        let project = ProjectDefinition {
            id: uuid::Uuid::nil(), nodes: vec![source, node.clone()],
            edges: vec![serde_json::from_value(json!({"id":"wire", "source":"source", "target":"k", "sourceHandle":"out", "targetHandle":"p"})).unwrap()],
            groups: vec![], created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
        };
        let mut selection = crate::project::selection::RunSelection::whole(&project);
        selection.input.entry(Located::top("k")).or_default().insert("p".into(), json!("backup"));
        let index = EdgeIndex::selected(&project, selection.clone());
        let wired = HashSet::from(["p"]);
        let color = project.id;
        let effective = effective_input_pulses(&node, &[], &wired, &project, &index, color, &vec![]);
        assert!(effective.is_empty(), "a selected supplier is still pending");
        let real = data(color, vec![], "k", "p", json!("real"));
        let closure = Pulse::closure(uuid::Uuid::new_v4(), color, vec![], "k", "p");
        for actual in [vec![&real], vec![&closure, &real]] {
            let effective = effective_input_pulses(&node, &actual, &wired, &project, &index, color, &vec![]);
            let view = firing_input(&node, &effective.iter().collect::<Vec<_>>(), &wired, &vec![], &index);
            assert_eq!(*view.input["p"], json!("real"));
            assert!(!effective.iter().any(|p| p.provided));
        }
        let effective = effective_input_pulses(&node, &[&closure], &wired, &project, &index, color, &vec![]);
        assert_eq!(*firing_input(&node, &effective.iter().collect::<Vec<_>>(), &wired, &vec![], &index).input["p"], json!("backup"));
        selection.nodes.remove(&Located::top("source"));
        let index = EdgeIndex::selected(&project, selection);
        let effective = effective_input_pulses(&node, &[], &wired, &project, &index, color, &vec![]);
        assert_eq!(*firing_input(&node, &effective.iter().collect::<Vec<_>>(), &wired, &vec![], &index).input["p"], json!("backup"));
        let null = data(color, vec![], "k", "p", Value::Null);
        let effective = effective_input_pulses(&node, &[&null], &wired, &project, &index, color, &vec![]);
        assert!(!firing_input(&node, &effective.iter().collect::<Vec<_>>(), &wired, &vec![], &index).type_errors.is_empty());
        assert!(!effective.iter().any(|p| p.provided), "invalid real data does not choose a backup");
        let failed = Pulse::closure_with_error(uuid::Uuid::new_v4(), color, vec![], "k", "p", Some("producer failed".into()));
        let effective = effective_input_pulses(&node, &[&failed], &wired, &project, &index, color, &vec![]);
        assert!(effective.iter().all(|p| !p.backup));
        assert_eq!(effective[0].close_error.as_deref(), Some("producer failed"));
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
        assert_eq!(Value::Object(super::owned_bag(&input)), json!({ "from": 0, "to": 10, "step": 2 }));
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
        assert_eq!(Value::Object(super::owned_bag(&input)), json!({ "endpointUrl": "http://bridge", "mode": "media" }));
    }

    /// An ACTIVATED trigger replays the ports it registered with, and
    /// they beat the node's own literal: the snapshot is the shape the
    /// trigger is already running on.
    #[test]
    fn the_setup_snapshot_beats_the_literal_on_the_same_port() {
        let inputs = vec![
            serde_json::from_value(json!({
                "name": "endpointUrl", "portType": "String", "required": true,
            }))
            .unwrap(),
        ];
        let node = kicked_node("Recv", inputs, json!({ "endpointUrl": "http://from-the-body" }));
        let snapshot = json!({ "endpointUrl": "http://registered" });
        let (input, errors) = build_kicked_input(&node, Some(&snapshot));
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(
            Value::Object(super::owned_bag(&input)),
            json!({ "endpointUrl": "http://registered" })
        );
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
        let mut obj = super::InputBag::new();
        super::fill_input_from_literals(&node, &wired, &mut obj);
        assert_eq!(
            serde_json::Value::Object(super::owned_bag(&obj)),
            json!({ "free_p": 2 }),
            "wired port stays absent; unwired port fills from its literal"
        );
    }
}
