//! Readiness. Find which nodes have enough pending pulses to fire at
//! a matching `(run_id, frames)`, aggregate their inputs, return as
//! `ReadyGroup`s.
//!
//! Matching is exact-frame: a firing at `(run_id, frames)` only sees
//! pulses whose `frames` are exactly the firing's frame stack. Loops
//! emit broadcast inputs and the implicit `self.index` at the body's
//! own frame stack directly, one pulse per iteration.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::run::skip::{check_flow_permission, check_should_skip, SkipReason};
use crate::frames::LoopFrames;
use crate::project::{Edge, ProgramIndex, GroupBoundaryRole, NodeDefinition, ProjectDefinition};
use crate::pulse::{Failure, Pulse, PulseTable};
use crate::weft_type::WeftType;
use crate::RunId;

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
    pub run_id: RunId,
    pub received: FiringInput,
    /// Set when this firing must NOT run its body, and why. `None`
    /// means run it.
    pub skip: Option<SkipReason>,
    pub pulse_ids: Vec<uuid::Uuid>,
    pub error: Option<String>,
}

/// THE single rule for "which pulse does a firing see on `port`?".
/// `group_pulses` are the pulses this firing sees: the pending ones at
/// its exact `(run_id, frames)` when readiness forms the group, or the
/// ones a record absorbed when the journal fold rebuilds its input.
/// Exact-frame matching happens where that slice is built.
///
/// Two-pulse case at the exact key: at most one non-closed pending
/// data pulse can coexist with one pending closure (the closure was a
/// pre-emission from a sibling producer whose port terminated, AND a
/// later sibling emitted real data; per "data outranks
/// structural-nothing", both stay in the table and this resolver
/// prefers the non-closed one). `find_groups_for_node` absorbs every
/// pulse at the firing's exact `(run_id, frames)` together, so the
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

/// Higher rank wins. Data (1) > closure (0).
fn pulse_rank(p: &Pulse) -> u8 {
    if p.closed { 0 } else { 1 }
}

/// The input ports of `node_id` a wire feeds at `frames`, and its stream
/// inputs handed a value there (their items arrive as pulses, as a
/// wire's would, `crate::run::scope`). What readiness waits on, what
/// gets a live feed, and what the input bag treats as authoritative over
/// body literals.
pub fn wired_inputs<'a>(
    project: &'a ProjectDefinition,
    program_idx: &ProgramIndex,
    node_id: &str,
    frames: &LoopFrames,
) -> HashSet<&'a str> {
    let mut wired: HashSet<&'a str> = program_idx
        .get_incoming(project, node_id, frames)
        .iter()
        .map(|e| e.target_handle.as_deref().unwrap_or("default"))
        .collect();
    if let (Some(handed), Some(node)) = (program_idx.handed_ports(node_id, frames), program_idx.node(project, node_id)) {
        wired.extend(node.inputs.iter()
            .filter(|input| input.is_generator() && handed.contains(&input.name))
            .map(|input| input.name.as_str()));
    }
    wired
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

/// Whether `edge` lands on a stream input of its target. Two equal
/// items on such an edge are two items, and a closure (the stream's
/// end) coexists with still-buffered ones, so every dedup and every
/// pending-value rule exempts these edges.
pub fn edge_targets_generator(program_idx: &ProgramIndex, edge: &Edge) -> bool {
    let handle = edge.target_handle.as_deref().unwrap_or("default");
    program_idx.stream_inputs(&edge.target).iter().any(|input| input == handle)
}

/// Every node whose pending pulses make it ready, with the group each
/// forms. `given` is what the run is given at the places its program says
/// ([`crate::project::Given`]).
pub fn find_ready_nodes(
    project: &ProjectDefinition,
    pulses: &PulseTable,
    program_idx: &ProgramIndex,
    given: &crate::project::Given,
) -> Vec<(String, ReadyGroup)> {
    find_ready_among(project, project.nodes.iter(), pulses, program_idx, given)
        .into_iter()
        .map(|(node, group)| (node.id.clone(), group))
        .collect()
}

/// `find_ready_nodes` over a chosen set of nodes: the engine asks only
/// the nodes some pending pulse waits at, so it never walks the whole
/// program each turn.
pub fn find_ready_among<'a>(
    project: &ProjectDefinition,
    candidates: impl IntoIterator<Item = &'a NodeDefinition>,
    pulses: &PulseTable,
    program_idx: &ProgramIndex,
    given: &crate::project::Given,
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
        // Group pulses by (run, frames). A firing is one exact point in
        // frame space; matching is exact, and which wires feed the node
        // is a fact of that point too (a node of an included file is
        // wired by the call it fires under).
        let mut groups: Vec<((RunId, LoopFrames), Vec<&Pulse>)> = Vec::new();
        for p in pending {
            let key = (p.run_id, p.frames.clone());
            match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, group)) => group.push(p),
                None => groups.push((key, vec![p])),
            }
        }
        for ((run_id, frames), group_pulses) in groups {
            let wired = wired_inputs(project, program_idx, &node.id, &frames);
            let required = required_inputs(node, &wired);
            let filled = filled_ports(node, &wired, &frames, program_idx);
            if let Some(group) = ready_group_at(
                node, &group_pulses, run_id, &frames, &required, &wired, &filled, given,
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

/// The ready group the pending pulses at one exact `(run_id, frames)`
/// form for `node`, if they make it ready. `wired` are the ports a wire
/// feeds at that point, `required` the required ones among them,
/// `filled` the unwired ports a constant or a handed value fills.
#[allow(clippy::too_many_arguments)]
fn ready_group_at(
    node: &NodeDefinition,
    group_pulses: &[&Pulse],
    run_id: RunId,
    frames: &LoopFrames,
    required: &HashSet<&str>,
    wired: &HashSet<&str>,
    filled: &HashSet<&str>,
    given: &crate::project::Given,
) -> Option<ReadyGroup> {
    let all_satisfied = wired.iter().all(|port_name| {
        group_pulses.iter().any(|p| p.target_port == *port_name)
    });
    if !all_satisfied {
        return None;
    }

    let received = firing_input(node, group_pulses, wired, frames, given);

    // A loop's `LoopOut` takes whatever came through: only its `LoopIn`
    // can skip.
    let is_out_boundary = node
        .group_boundary
        .as_ref()
        .map(|gb| gb.role == GroupBoundaryRole::Out)
        .unwrap_or(false);
    // Pulses only ever ride edges, so a node with no incoming edges
    // cannot form a group here; an entry node's `_should_flow` is
    // decided where its kick is synthesized (`kicked_group` runs
    // `check_flow_permission`).
    let skip = if is_out_boundary {
        None
    } else {
        check_should_skip(node, group_pulses, required, wired, filled)
    };

    let pulse_ids: Vec<uuid::Uuid> = group_pulses.iter().map(|p| p.id).collect();

    Some(ReadyGroup {
        frames: frames.clone(),
        run_id,
        error: received.error(),
        received,
        skip,
        pulse_ids,
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
/// fold (to read back what a kicked firing received).
pub fn kicked_group(
    node: &NodeDefinition,
    kick: &crate::primitive::KickedNode,
    frames: &LoopFrames,
    run_id: RunId,
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    given: &crate::project::Given,
) -> ReadyGroup {
    let wired = wired_inputs(project, program_idx, &node.id, frames);
    let received = if kick.firing {
        let (input, errors) = build_kicked_input(node, frames, kick.port_snapshot.as_ref());
        FiringInput { input, type_errors: errors, ..Default::default() }
    } else {
        firing_input(node, &[], &wired, frames, given)
    };
    let skip = match &kick.scope_skipped {
        Some(scope) => Some(SkipReason::ScopeSkipped { scope: scope.clone() }),
        // Trigger preparation already evaluated its wired gate. A fire
        // requires that capture and reads its baked settings. Live pulses
        // into triggers are dropped at Fire by the settle pass; the
        // scopes around it still decide before this kick dispatches.
        None if kick.firing => check_flow_permission(node, &[]),
        None => check_should_skip(node, &[], &required_inputs(node, &wired), &wired,
            &filled_ports(node, &wired, frames, program_idx)),
    };
    ReadyGroup {
        frames: frames.clone(),
        run_id,
        error: received.error(),
        received,
        skip,
        // A kick absorbs nothing, so it must fold nothing in: a pulse
        // read into the bag here would stay pending for ever (absorption
        // is driven by this list) and the run could never reach
        // Completed. Whatever is waiting on a wire is read by the
        // ordinary pulse-driven dispatch instead, or, for a trigger in
        // the firing phase, dropped by the settle pass.
        pulse_ids: Vec::new(),
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
    /// keyed by port, with the failure the closure carried. A loop's
    /// boundary forwarding a closure keeps this on the same-named output,
    /// so "broke" never reads as "nothing there" one level up (the
    /// inverted gate tells the two apart).
    pub closed_failures: BTreeMap<String, Failure>,
    pub type_errors: Vec<String>,
    /// The ports whose value a person handed in or a stand-in sent
    /// (`ProjectDefinition::given`), not a node that ran in this run.
    pub provided_ports: Vec<String>,
    /// The ports whose value a seed's saved result sent, with the run
    /// that made it.
    pub inherited_ports: BTreeMap<String, RunId>,
    /// Where the run's record keeps each port's value, for the ports whose
    /// value is a pulse's that knows it (`Pulse::origin`): what a reader
    /// showing the run names the value by. Empty on a live engine. Never
    /// on the wire, as the pulses' own.
    #[serde(skip)]
    pub origins: BTreeMap<String, Arc<crate::record_ref::ValueRef>>,
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
    given: &crate::project::Given,
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

    // A value handed to a port is the port's (no wire lands there); a
    // stream's items reach the feed as pulses instead.
    let handed = given.inputs.get(&crate::frames::Located::at(&node.id, frames));
    for (port, value) in handed.into_iter().flatten() {
        if !is_generator_input(node, port) {
            obj.insert(port.clone(), std::sync::Arc::new(value.clone()));
        }
    }
    fill_input_from_literals(node, wired, &mut obj);

    // Runtime type enforcement on input ports: the single check point
    // (see `check_input`). A mismatch on a required port aggregates
    // into `type_errors` (the node fails loudly); a mismatch on an
    // optional port nulls the port and the node proceeds.
    let type_errors = check_bag(node, frames, wired, &mut obj);

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
    let closed_failures: BTreeMap<String, Failure> = closed_ports
        .iter()
        .filter_map(|port| {
            resolve_port_value(group_pulses, port)
                .and_then(|p| p.failure.clone())
                .map(|failure| (port.clone(), failure))
        })
        .collect();

    let mut provided: std::collections::BTreeSet<String> = handed.into_iter().flatten().map(|(port, _)| port.clone()).collect();
    let mut inherited_ports = BTreeMap::new();
    for port in group_pulses.iter().map(|p| p.target_port.as_str()).collect::<std::collections::BTreeSet<_>>() {
        if let Some(winner) = resolve_port_value(group_pulses, port) {
            if winner.provided { provided.insert(port.to_string()); }
            if let Some(origin) = winner.inherited_from { inherited_ports.insert(port.to_string(), origin); }
        }
    }
    let provided_ports = provided.into_iter().collect();
    // A port's origin is its winning pulse's while the bag still holds
    // that pulse's value: a handed value or a refused one replaced it.
    // Looked at only when a pulse knows one, which a live engine's never
    // does.
    let mut origins = BTreeMap::new();
    if group_pulses.iter().any(|p| p.origin.is_some()) {
        for (port, value) in &obj {
            let Some(winner) = resolve_port_value(group_pulses, port).filter(|p| !p.closed) else { continue };
            if let Some(origin) = winner.origin.as_ref().filter(|_| Arc::ptr_eq(&winner.value, value)) {
                origins.insert(port.clone(), origin.clone());
            }
        }
    }
    FiringInput { input: obj, closed_ports, closed_failures, type_errors, provided_ports, inherited_ports, origins }
}

/// Runtime type enforcement on input ports: the single check point
/// (see `check_input`). A mismatch is returned and the node fails
/// loudly, on a required port and an optional one alike. The gate is
/// not data: `_should_flow` takes any value and only a `false` says
/// no (`check_flow_permission` is its one rule), so it is never held
/// to a type here.
fn check_bag(node: &NodeDefinition, frames: &LoopFrames, wired: &HashSet<&str>, obj: &mut InputBag) -> Vec<String> {
    let mut errors = Vec::new();
    // Each port held to its type at the place this firing runs (a node
    // inside an included file has the types its site gave it).
    let place = crate::frames::call_path(frames);
    let types = node.types_at(&place);
    for port in node.inputs.iter().filter(|p| !crate::run::skip::is_gate_port(&p.name)) {
        let Some(value) = obj.get(&port.name) else {
            continue;
        };
        let placed;
        let port = match types.input(&port.name) {
            Some(here) if *here != port.port_type => {
                placed = crate::project::InputDefinition { port: crate::project::PortDefinition { port_type: here.clone(), ..port.port.clone() }, ..port.clone() };
                &placed
            }
            _ => port,
        };
        match check_input(port, value, wired.contains(port.name.as_str())) {
            InputCheck::Ok => {}
            InputCheck::Fail(err) => {
                tracing::error!(target: "weft::run::ready", node = %node.id, "{err}");
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
///
/// An `@instance_filled` literal (and a connection picked on the install) is
/// left out: it names no value until the engine swaps in what the run's
/// instance provides or the install picked (checked when the run was born),
/// so judging the marker itself against the port's type here would refuse
/// every such field. It still counts
/// as filled for readiness, since a run is only born once every such
/// field it needs has a value or a fallback.
pub fn fill_input_from_literals(
    node: &NodeDefinition,
    wired: &HashSet<&str>,
    obj: &mut InputBag,
) {
    for (name, value) in &node.port_literals {
        if wired.contains(name.as_str())
            || obj.contains_key(name)
            || !literal_is_data(node, name, value)
            || crate::picks::fills_later(value)
        {
            continue;
        }
        obj.insert(name.clone(), Arc::new(value.clone()));
    }
}

/// The inputs whose closure skips `node` at this point (rule 1 of
/// `check_should_skip`): the required ones, and the ones a wire feeds
/// here that are not `optional` (an input the node's metadata calls
/// optional is required once wired, unless the program wrote `?`).
pub fn required_inputs<'a>(node: &'a NodeDefinition, wired: &HashSet<&str>) -> HashSet<&'a str> {
    node.inputs
        .iter()
        // The gates are rule 0's alone: a closure on one is never a
        // required input closing.
        .filter(|p| !crate::run::skip::is_gate_port(&p.name))
        .filter(|p| p.is_required(wired.contains(p.name.as_str())))
        .map(|p| p.name.as_str())
        .collect()
}

/// The unwired ports a written constant or a handed value fills at
/// `frames`: what keeps a node with no live wire alive in the skip rules.
/// An `@instance_filled` literal counts, since a run is only born once
/// each such field it needs has the instance's value or a fallback; it is
/// left out of the bag itself ([`fill_input_from_literals`]) only until
/// the engine puts that value in. The ONE reading both the pulse path and
/// a kicked root use: reading it off the bag instead made a node whose
/// every setting is `@instance_filled` look like it had nothing, and skip.
pub fn filled_ports<'a>(
    node: &'a NodeDefinition,
    wired: &HashSet<&str>,
    frames: &LoopFrames,
    program_idx: &ProgramIndex,
) -> HashSet<&'a str> {
    let handed = program_idx.handed_ports(&node.id, frames);
    node.inputs
        .iter()
        .map(|input| input.name.as_str())
        .filter(|name| !wired.contains(name))
        .filter(|name| {
            handed.is_some_and(|ports| ports.contains(*name))
                || node.port_literals.get(*name).is_some_and(|value| literal_is_data(node, name, value))
        })
        .collect()
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
    frames: &LoopFrames,
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
    let errors = check_bag(node, frames, &HashSet::new(), &mut obj);
    (obj, errors)
}

/// Check one incoming value against its input port type. THE single
/// place input type enforcement lives.
fn check_input(port: &crate::project::InputDefinition, value: &Value, wired: bool) -> InputCheck {
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
    if declared.accepts_runtime_value(value) {
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
    // arrived" and the bag fills the default. A port required once wired
    // is not optional when a wire delivered the null.
    if value.is_null() && !port.is_required(wired) {
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
    use crate::project::{ProgramIndex, InputDefinition, NodeDefinition, Position, ProjectDefinition};
    use super::firing_input;
    use std::collections::HashSet;
    use crate::pulse::Pulse;
    use crate::NodeFeatures;
    use serde_json::json;

    fn frame(i: u32) -> Frame {
        Frame::Loop { index: i }
    }

    fn data(run_id: uuid::Uuid, frames: Vec<Frame>, node: &str, port: &str, value: serde_json::Value) -> Pulse {
        Pulse::new(uuid::Uuid::new_v4(), run_id, frames, node, port, std::sync::Arc::new(value))
    }

    /// An input the metadata calls optional is required once wired: wired
    /// and closed, the node skips as for a required input; unwired, or
    /// wired with a value, it runs. A wired null fails it, as on a
    /// required input, instead of reading as "nothing arrived". One the
    /// program wrote `?` runs on a closed wire.
    #[test]
    fn an_optional_input_skips_only_when_its_wire_closes_unless_written_optional() {
        let project = |wire_instance: bool| -> ProjectDefinition {
            let mut edges = vec![json!({ "id": "e1", "source": "src", "target": "n",
                "sourceHandle": "data", "targetHandle": "data" })];
            if wire_instance {
                edges.push(json!({ "id": "e2", "source": "src", "target": "n",
                    "sourceHandle": "id", "targetHandle": "instance" }));
            }
            serde_json::from_value(json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "name": "p", "description": null,
                "nodes": [
                    { "id": "src", "nodeType": "S", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                      "inputs": [], "outputs": [
                          { "name": "id", "portType": "String", "required": true },
                          { "name": "data", "portType": "String", "required": true }],
                      "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                    { "id": "n", "nodeType": "N", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                      "inputs": [
                          { "name": "instance", "portType": "String", "required": false },
                          { "name": "data", "portType": "String", "required": false }],
                      "outputs": [],
                      "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }
                ],
                "edges": edges, "groups": [],
                "createdAt": "1970-01-01T00:00:00Z", "updatedAt": "1970-01-01T00:00:00Z"
            }))
            .unwrap()
        };
        let run_id = uuid::Uuid::nil();
        let ready = |project: &ProjectDefinition, instance: Option<Pulse>| {
            let mut table = crate::pulse::PulseTable::default();
            let bucket = table.entry("n".into()).or_default();
            bucket.push(data(run_id, vec![], "n", "data", json!("d")));
            bucket.extend(instance);
            let index = ProgramIndex::build(project);
            let mut found = super::find_ready_nodes(project, &table, &index, &Default::default());
            assert_eq!(found.len(), 1, "the node's group is formed");
            found.remove(0).1
        };
        let wired = project(true);

        let closed = ready(&wired, Some(Pulse::closure(uuid::Uuid::new_v4(), run_id, vec![], "n", "instance")));
        assert_eq!(closed.skip, Some(crate::run::skip::SkipReason::RequiredInputClosed { port: "instance".into(), failure: None }));

        let given = ready(&wired, Some(data(run_id, vec![], "n", "instance", json!("ada"))));
        assert_eq!(given.skip, None, "a value on the wire runs the node");
        assert!(given.received.type_errors.is_empty());

        let null = ready(&wired, Some(data(run_id, vec![], "n", "instance", json!(null))));
        assert_eq!(null.skip, None);
        assert_eq!(null.received.type_errors.len(), 1, "a wired null is not 'nothing arrived': {:?}", null.received.type_errors);

        let mut written_optional = project(true);
        written_optional.nodes[1].inputs[0].optional = true;
        let closed = ready(&written_optional, Some(Pulse::closure(uuid::Uuid::new_v4(), run_id, vec![], "n", "instance")));
        assert_eq!(closed.skip, None, "`?` runs the node on a closed wire");

        let unwired = ready(&project(false), None);
        assert_eq!(unwired.skip, None, "unwired, the node runs without it");
    }

    /// A firing sees only the pulses at its own frame stack: the group
    /// is formed by exact `(run_id, frames)`, so a shallower pulse is
    /// never in its view.
    #[test]
    fn groups_form_at_the_exact_frame_only() {
        let run_id = uuid::Uuid::nil();
        let firing_frames = vec![frame(0), frame(1)];
        let node = kicked_node("X", vec![port("String", true)], json!({}));
        let pulses = [
            data(run_id, vec![], "k", "p", json!("shallow")),
            data(run_id, vec![frame(0)], "k", "p", json!("mid")),
            data(run_id, firing_frames.clone(), "k", "p", json!("exact")),
        ];
        let wired: std::collections::HashSet<&str> = ["p"].into_iter().collect();
        let groups: Vec<_> = pulses.iter().map(|p| p.frames.clone()).collect::<std::collections::BTreeSet<_>>().into_iter()
            .filter_map(|frames| {
                let at: Vec<&Pulse> = pulses.iter().filter(|p| p.frames == frames).collect();
                super::ready_group_at(&node, &at, run_id, &frames, &std::collections::HashSet::new(), &wired,
                    &std::collections::HashSet::new(), &Default::default())
            }).collect();
        assert_eq!(groups.len(), 3, "one group per frame stack");
        let exact = groups.iter().find(|g| g.frames == firing_frames).expect("the exact group");
        assert_eq!(Value::Object(super::owned_bag(&exact.received.input)), json!({"p": "exact"}));
        assert_eq!(exact.pulse_ids, vec![pulses[2].id]);
    }

    #[test]
    fn resolve_port_value_prefers_data_over_closure_at_same_key() {
        let run_id = uuid::Uuid::nil();
        let frames = vec![];
        let pulses = [
            Pulse::closure(uuid::Uuid::new_v4(), run_id, frames.clone(), "n", "p"),
            data(run_id, frames.clone(), "n", "p", json!(42)),
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
        assert_eq!(check_input(&p, &json!({"name": "Ada", "age": 36}), false), InputCheck::Ok);
        match check_input(&p, &json!({"name": "Ada"}), false) {
            InputCheck::Fail(msg) => assert!(msg.contains("Profile"), "names the type: {msg}"),
            other => panic!("missing required field must fail, got {other:?}"),
        }
    }

    #[test]
    fn matching_value_is_ok_regardless_of_required() {
        assert_eq!(check_input(&port("String", true), &json!("ok"), false), InputCheck::Ok);
        assert_eq!(check_input(&port("String", false), &json!("ok"), false), InputCheck::Ok);
    }

    #[test]
    fn null_is_data_only_where_the_type_admits_it() {
        assert_eq!(check_input(&port("String | Null", true), &json!(null), false), InputCheck::Ok);
        assert_eq!(check_input(&port("String", false), &json!(null), false), InputCheck::Ok, "optional: absent");
        match check_input(&port("String", true), &json!(null), false) {
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
        let (input, _) = build_kicked_input(&node, &vec![], None);
        assert_eq!(input.get("maybe").map(|v| &**v), Some(&json!(null)), "null is data on a nullable port");
        assert!(!input.contains_key("plain"), "null on a plain port is no value");
    }

    #[test]
    fn a_widget_binds_a_wired_value_like_a_written_one() {
        let count: InputDefinition = serde_json::from_value(json!({
            "name": "count", "portType": "Number", "required": true,
            "widget": { "kind": "number", "min": 1, "max": 8, "step": 1 }
        })).unwrap();
        assert_eq!(check_input(&count, &json!(3), false), InputCheck::Ok);
        for bad in [json!(0), json!(9), json!(2.5)] {
            match check_input(&count, &bad, false) {
                InputCheck::Fail(msg) => assert!(msg.contains("'count'"), "{msg}"),
                other => panic!("{bad} must be refused by the widget, got {other:?}"),
            }
        }
        // A select's options are a domain rule too: a wired value
        // outside them is refused the way a written one is.
        let mode: InputDefinition = serde_json::from_value(json!({
            "name": "mode", "portType": "String", "required": false,
            "widget": { "kind": "select", "options": ["added", "removed", "both"] }
        })).unwrap();
        assert_eq!(check_input(&mode, &json!("both"), false), InputCheck::Ok);
        match check_input(&mode, &json!("sideways"), false) {
            InputCheck::Fail(msg) => assert!(msg.contains("'mode'") && msg.contains("'added'"), "{msg}"),
            other => panic!("a value outside the options must be refused, got {other:?}"),
        }
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
        assert_eq!(check_input(&every_two, &json!(3), false), InputCheck::Ok);
        assert_eq!(check_input(&every_two, &json!(4), false), InputCheck::Ok);
        assert!(matches!(check_input(&every_two, &json!(2.5), false), InputCheck::Fail(_)));
        let tenths: InputDefinition = serde_json::from_value(json!({
            "name": "temperature", "portType": "Number", "required": false,
            "widget": { "kind": "number", "min": 0, "max": 2, "step": 0.1 }
        })).unwrap();
        for fine in [json!(0.85), json!(1.0), json!(0.07)] {
            assert_eq!(check_input(&tenths, &fine, false), InputCheck::Ok, "{fine}");
        }
        assert!(matches!(check_input(&tenths, &json!(2.5), false), InputCheck::Fail(_)), "the range still binds");
    }

    #[test]
    fn mismatch_on_required_fails() {
        let p = port("String", true);
        match check_input(&p, &json!(42), false) {
            InputCheck::Fail(msg) => assert!(msg.contains("expected")),
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// An optional port used to swallow a wrong-typed value as a null
    /// and let the node run on its default; a wrong type is a wiring
    /// bug on any port, and the firing fails.
    #[test]
    fn mismatch_on_optional_fails_too() {
        match check_input(&port("String", false), &json!(42), false) {
            InputCheck::Fail(msg) => assert!(msg.contains("type mismatch"), "{msg}"),
            other => panic!("expected Fail, got {other:?}"),
        }
        assert_eq!(check_input(&port("String", false), &json!(null), false), InputCheck::Ok, "a null on an optional port is nothing arrived");
    }

    /// A required nullable port used to swallow a wrong-typed value as
    /// a null, so the node ran and could not tell "never arrived" from
    /// "arrived wrong". Only an OPTIONAL port drops a value.
    #[test]
    fn a_wrong_type_on_a_required_nullable_port_fails() {
        match check_input(&port("String | Null", true), &json!(42), false) {
            InputCheck::Fail(msg) => assert!(msg.contains("type mismatch"), "{msg}"),
            other => panic!("expected Fail, got {other:?}"),
        }
        // A null itself is still data there.
        assert_eq!(check_input(&port("String | Null", true), &json!(null), false), InputCheck::Ok);
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
        assert_eq!(check_input(&account, &handle, false), InputCheck::Ok);
        // A resource pick is stored the same way, on a port typed for
        // the id the node ends up reading.
        let sheet: InputDefinition = serde_json::from_value(json!({
            "name": "sheet", "portType": "String", "required": true,
            "widget": { "kind": "remote_select", "access": "account", "sources": [] }
        })).unwrap();
        assert_eq!(
            check_input(&sheet, &json!({ "id": "1AbC", "label": "Budget" }), false),
            InputCheck::Ok
        );
        // A pasted raw id is the port's own type and still checked.
        assert_eq!(check_input(&sheet, &json!("1AbC"), false), InputCheck::Ok);
        assert!(matches!(check_input(&sheet, &json!(7), false), InputCheck::Fail(_)));
        let node = kicked_node("SlackAccess", vec![account], json!({ "account": handle.clone() }));
        let (input, refusals) = build_kicked_input(&node, &vec![], None);
        assert!(refusals.is_empty(), "{refusals:?}");
        assert_eq!(input.get("account").map(|v| &**v), Some(&handle), "the handle reaches the bag intact");
    }

    /// An `@instance_filled` field reaches the bag only once the engine swaps
    /// in the instance's value: the marker itself is never judged against
    /// the port (a String port would refuse the marker's object).
    #[test]
    fn an_instance_filled_literal_waits_for_the_instances_value() {
        let marker = crate::instance::instance_filled_literal(Some(json!("0 0 3 * * *")));
        let node = kicked_node("Cron", vec![port("String", true)], json!({ "p": marker }));
        let (input, refusals) = build_kicked_input(&node, &vec![], None);
        assert!(refusals.is_empty(), "{refusals:?}");
        assert!(!input.contains_key("p"));
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
        let (input, refusals) = build_kicked_input(&node, &vec![], None);
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(refusals[0].contains("intervalSecs"), "{refusals:?}");
        assert_eq!(input.get("intervalSecs").map(|v| &**v), Some(&json!(null)));
    }

    fn kicked_node(node_type: &str, inputs: Vec<InputDefinition>, literals: serde_json::Value) -> NodeDefinition {
        NodeDefinition {
            id: "k".into(),
            node_type: node_type.into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            inputs,
            outputs: Vec::new(),
            features: NodeFeatures::default(),
            scope: Vec::new(),
            group_boundary: None,
            requires_infra: false, per_instance: None, per_instance_paths: Default::default(),
            images: Vec::new(),
            fires_with: Default::default(), baked_outputs: Default::default(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
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
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        }
    }

    /// A value handed to a port (the program says the port is handed, the
    /// run carries the value) is the port's: the firing reads it with no
    /// pulse behind it, says it was handed, and a wire into that port lands
    /// nothing there.
    #[test]
    fn a_handed_value_fills_its_port_and_says_so() {
        let node = kicked_node("X", vec![port("String", true)], json!({}));
        let mut source = node.clone();
        source.id = "source".into();
        let mut project = ProjectDefinition {
            id: uuid::Uuid::nil(), nodes: vec![source, node.clone()],
            edges: vec![serde_json::from_value(json!({"id":"wire", "source":"source", "target":"k", "sourceHandle":"out", "targetHandle":"p"})).unwrap()],
            groups: vec![], created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
            defaults: Default::default(), given: Default::default(), broken: Vec::new(), metadata_wires: Vec::new(),
        };
        let mut given = crate::project::Given::default();
        given.inputs.insert(Located::top("k"), [("p".into(), json!("handed"))].into());
        project.given = given.places();
        let index = ProgramIndex::build(&project);
        let received = firing_input(&node, &[], &HashSet::new(), &vec![], &given);
        assert_eq!(*received.input["p"], json!("handed"));
        assert_eq!(received.provided_ports, vec!["p".to_string()]);
        assert!(index.get_incoming(&project, "k", &vec![]).is_empty(), "nothing lands on a handed port");
    }

    /// A scope root whose only setting is `@instance_filled` runs when its
    /// scope starts, like one with a written value: the marker stays out
    /// of the bag until the engine puts the instance's value in, and must
    /// not read as "nothing will ever come" (it used to skip with every
    /// input closed, silently, in a completed run).
    #[test]
    fn a_kicked_root_whose_setting_each_instance_fills_runs() {
        let marker = crate::instance::instance_filled_literal(None);
        let node = kicked_node("X", vec![port("String", true)], json!({ "p": marker }));
        let project = ProjectDefinition {
            id: uuid::Uuid::nil(), nodes: vec![node.clone()], edges: vec![], groups: vec![],
            created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
            defaults: Default::default(), given: Default::default(), broken: Vec::new(), metadata_wires: Vec::new(),
        };
        let index = ProgramIndex::build(&project);
        let kick = crate::primitive::KickedNode::new(false, None, None, None);
        let group = super::kicked_group(&node, &kick, &vec![], project.id, &project, &index, &Default::default());
        assert!(group.skip.is_none(), "{:?}", group.skip);
        assert!(!group.received.input.contains_key("p"), "the marker waits for the instance's value");
    }

    /// A join a run starts at, one branch handed a value (the wire
    /// from the branch's source is outside the run), runs on that value:
    /// a handed branch delivers on a kicked root.
    #[test]
    fn a_kicked_join_handed_its_branches_runs() {
        let branch = |name: &str| -> InputDefinition {
            // A join's branches run without a value, as the compiler marks them.
            serde_json::from_value(json!({ "name": name, "portType": "String", "required": false, "optional": true })).expect("port")
        };
        let mut node = kicked_node("Join", vec![branch("a"), branch("b")], json!({}));
        node.features.join = true;
        let mut project = ProjectDefinition {
            id: uuid::Uuid::nil(), nodes: vec![node.clone()], edges: vec![], groups: vec![],
            created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
            defaults: Default::default(), given: Default::default(), broken: Vec::new(), metadata_wires: Vec::new(),
        };
        let mut given = crate::project::Given::default();
        given.inputs.insert(Located::top("k"), [("a".into(), json!("handed in"))].into());
        project.given = given.places();
        let index = ProgramIndex::build(&project);
        let kick = crate::primitive::KickedNode::new(false, None, None, None);
        let group = super::kicked_group(&node, &kick, &vec![], project.id, &project, &index, &given);
        assert!(group.skip.is_none(), "{:?}", group.skip);
        assert_eq!(*group.received.input["a"], json!("handed in"));
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
        let (input, _) = build_kicked_input(&node, &vec![], None);
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
        let (input, _) = build_kicked_input(&node, &vec![], Some(&snapshot));
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
        let (input, errors) = build_kicked_input(&node, &vec![], Some(&snapshot));
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
