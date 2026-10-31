//! What a seeded run reuses of the run it is seeded from (`weft run
//! --seed`), separately from what it runs: the places whose saved result
//! still holds (their code and everything feeding them unchanged), the
//! ones among them that send that result into what runs again (they stand
//! in for themselves in the run's cut, `crate::project::Given`), and the
//! values the old run handed its starts that this one keeps.
//!
//! Everything here is per PLACE (`Located`): a node of an included file
//! has one result per call that reached it, and a child reuses the result
//! of the call it runs, never the other call's.
//!
//! This is the runtime planner: it reads the old run's snapshot. What it
//! hands the resolver, `crate::run_spec::SeedPlan`, is pure and lives with
//! the resolver, so a build without the runtime resolves the same way.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::Value;

use crate::run::NodeFiringStatus;
use crate::frames::{loop_indices, Located, LoopFrames};
use crate::primitive::RunSnapshot;
use crate::project::{loop_in_id, GivenOrigin, GivenPort, GroupKind, ProgramIndex, ProjectDefinition, StandIn};
use crate::project::hash::ProgramIdentity;
use crate::project::graph::{GraphView, ProjectGraph};
use crate::project::cut::{self, Walk};
use crate::project::places::{enclosing_loops, every_place, incoming, members_in, outgoing, scopes_around};
use crate::run_spec::{reused_group_start, OutputWire, RunSpec, SeedPlan};
use crate::RunId;

/// One complete result the parent retained. Its origin is already chosen;
/// a failed newer attempt never falls through to an older successful one.
#[derive(Debug, Clone)]
pub struct SeedOutcome {
    pub origin: RunId,
    pub slice_hash: String,
    /// The values the run that made it handed this place's inputs.
    pub handed: BTreeMap<String, Value>,
    /// The gates that run handed the groups around this place
    /// ([`gates_handed_around`]): they decided whether it ran.
    pub gates: BTreeMap<String, Value>,
    pub emitted_live_handle: bool,
    /// Original wake payload, only when this result actually fired a trigger.
    pub fire: Option<Value>,
    /// What it sent, port by port: what it sends again when it stands in.
    pub outputs: BTreeMap<String, GivenPort>,
}

/// The gates `given` hands the groups around `place` in `project` (a
/// `--group g='{"_should_flow": ...}'`), keyed `<group>.<gate>`, outermost
/// first: a result made under another gate value is no result of a run
/// handing this one.
pub fn gates_handed_around(given: &crate::project::Given, project: &ProjectDefinition, place: &Located) -> BTreeMap<String, Value> {
    if given.inputs.is_empty() {
        return BTreeMap::new();
    }
    let g = ProjectGraph::new(project);
    scopes_around(&g, place).iter()
        .filter_map(|scope| given.inputs.get(scope).map(|ports| (scope, ports)))
        .flat_map(|(scope, ports)| ports.iter()
            .filter(|(port, _)| crate::run::skip::is_gate_port(port))
            .map(move |(port, value)| (format!("{}.{port}", crate::project::group_address(project, &scope.id, &scope.path)), value.clone())))
        .collect()
}

/// A stored file as the store holds it now, for a reused output that
/// names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileNow {
    At(u64),
    Gone,
}

/// Why a reused node's output cannot stand in for it any more, or
/// `None` when it still can: the output names a stored `file` the node
/// left at one version, and the file is at another now (another run, or
/// a later node, changed it), or is gone. `node` and `port` are spelled
/// the way the program reads them. The reason names both ways out.
pub fn moved_file_refusal(node: &str, port: &str, file: &crate::storage::StoredFile, now: FileNow) -> Option<String> {
    let what = match now {
        FileNow::At(version) if version == file.version => return None,
        FileNow::At(version) => format!(
            "has changed since (that run left it at version {}, it is at version {version} now)",
            file.version
        ),
        FileNow::Gone => "is gone (deleted, or expired with the run that made it)".to_string(),
    };
    Some(format!(
        "--seed would reuse what '{node}' produced on '{port}', but the file it names, '{}', {what}, so \
         the saved result no longer says what that node made. Hand the file in yourself \
         (`--emit {node}='{{\"{port}\": ...}}'`), or run '{node}' again by keeping it out of the seed \
         (`--seed-before {node}`).",
        file.filename
    ))
}

/// What a node at `place` sent, port by port, read off a run's output
/// wires: a stream's items as one list, a port that closed as a closure
/// (with the failure it carried), and nothing for a port it never sent
/// (a stand-in closes those).
pub fn sent_ports(node: &crate::project::NodeDefinition, place: &Located, outputs: &[OutputWire]) -> BTreeMap<String, GivenPort> {
    let mut sent = BTreeMap::new();
    for port in &node.outputs {
        let mut wires: Vec<&OutputWire> = outputs.iter()
            .filter(|wire| wire.place() == *place && wire.port == port.name && loop_indices(&wire.frames).is_empty())
            .collect();
        wires.sort_by_key(|wire| wire.ordinal);
        if wires.is_empty() { continue; }
        let given = if port.is_generator() {
            match wires.iter().find(|wire| wire.closed && wire.failure.is_some()) {
                Some(failed) => GivenPort::Closed { failure: failed.failure.clone() },
                None => GivenPort::Value { value: Value::Array(wires.iter().filter(|wire| !wire.closed).map(|wire| wire.value.clone()).collect()) },
            }
        } else {
            match wires.iter().find(|wire| !wire.closed) {
                Some(wire) => GivenPort::Value { value: wire.value.clone() },
                None => GivenPort::Closed { failure: wires.iter().find_map(|wire| wire.failure.clone()) },
            }
        };
        sent.insert(port.name.clone(), given);
    }
    sent
}

/// Choose what a run walked as `authored` reuses of `outcomes` (the old
/// run's complete results, by place). A place is reused when its result
/// still holds: its slice (its code and everything feeding it) is
/// unchanged, it handed no live connection on, what this run hands it is
/// what the old run did, it is not downstream of something this run
/// fires or emits afresh, the `--seed-until` / `--seed-before` cap lets
/// it, and nothing it depends on runs again. A loop is reused whole or
/// not at all. History relevant to this run reaches back past its starts
/// (what fed them), so a start's inputs come from the old run too.
///
/// What is reused and feeds something that runs again stands in for
/// itself with its saved outputs; what is reused and feeds nothing that
/// runs is only shown.
///
/// A place this run runs again that waited in the old run replays the
/// answers its waits were given there (`answered`: each such place with
/// the fingerprint of its own code there, `ProgramIdentity::node_hash`),
/// when its code is what it was: it carries on where it stopped instead of
/// asking again.
///
/// `authored` is the spec resolved with no seed: the walk it runs and the
/// values its cut hands its starts.
#[allow(clippy::too_many_arguments)]
pub fn seed_plan(
    project: &ProjectDefinition,
    program: &ProgramIdentity,
    authored_run: &crate::run_spec::Resolved,
    spec: &RunSpec,
    outcomes: &BTreeMap<Located, SeedOutcome>,
    answered: &BTreeMap<Located, crate::project::hash::SourceHash>,
    until: &[String],
    before: &[String],
) -> anyhow::Result<SeedPlan> {
    let g = ProjectGraph::new(project);
    let authored = &authored_run.walk;
    let cap: Option<BTreeSet<Located>> = (!until.is_empty() || !before.is_empty()).then(|| {
        cut::walk(&g, &cut::SelectionBounds { target: until.to_vec(), before: before.to_vec(), ..Default::default() })
            .map(|walked| walked.kept())
    }).transpose().map_err(anyhow::Error::msg)?;
    let hashes = program.slice_hashes(project)?;
    let handed_now = &authored_run.given.inputs;
    let roots = roots_of(&g, authored);
    let places = every_place(project);
    let kept = authored.kept();
    let mut invalid = BTreeSet::new();
    let mut warnings = Vec::new();
    for place in &places {
        let spelled = || crate::project::address_of(project, &place.id, &place.path);
        let Some(old) = outcomes.get(place) else { invalid.insert(place.clone()); continue };
        if hashes.get(place) != Some(&old.slice_hash) {
            invalid.insert(place.clone());
            warnings.push(format!("cannot seed '{}': its code or dependencies changed; reuse stops before it", spelled()));
        }
        if old.emitted_live_handle {
            invalid.insert(place.clone());
        }
        if authored.runs.contains(place) && !old.handed.is_empty() && !handed_now.contains_key(place) && !roots.contains(place) {
            // An old start is now an interior computation: what it was
            // handed is not a starting value of this run.
            invalid.insert(place.clone());
        }
        // A handed value is the port's whatever a wire brings, so a port
        // handed now what the old run did not hand it read something else.
        if let Some(now) = handed_now.get(place) {
            if now.iter().any(|(port, value)| old.handed.get(port) != Some(value)) {
                invalid.insert(place.clone());
            }
        }
        // A gate handed now to a group around it decides whether it runs,
        // and the old result was made under whatever gate it had then.
        if gates_handed_around(&authored_run.given, project, place) != old.gates {
            invalid.insert(place.clone());
        }
        if cap.as_ref().is_some_and(|cap| !cap.contains(place)) {
            invalid.insert(place.clone());
        }
    }
    // A fire and an emit are fresh events: what they reach runs again.
    let fresh: Vec<Located> = spec.emit.keys().chain(spec.fire.iter().map(|(node, _)| node))
        .map(|spelled| crate::project::places::named(&g, spelled).map(crate::project::places::Named::into_place))
        .collect::<Result<_, String>>().map_err(anyhow::Error::msg)?;
    invalid.extend(cut::downstream(&g, &fresh));
    // What a place depends on: one walk per place; the fixpoint below only
    // re-reads them.
    let dependencies: BTreeMap<&Located, BTreeSet<Located>> = places.iter()
        .map(|place| (place, cut::walk_bounds(&g, &cut::Bounds::setup(std::slice::from_ref(place))).runs)).collect();
    loop {
        let previous = invalid.len();
        for (place, dependencies) in &dependencies {
            if dependencies.iter().any(|dep| dep != *place && authored.runs.contains(dep) && invalid.contains(dep)) {
                invalid.insert((*place).clone());
            }
        }
        for group in project.groups.iter().filter(|g| matches!(g.kind, GroupKind::Loop { .. })) {
            for entry in places.iter().filter(|place| place.id == loop_in_id(&group.id)) {
                let members = members_in(&g, &group.id, &entry.path);
                let origins: BTreeSet<_> = members.iter().filter_map(|member| outcomes.get(member).map(|o| o.origin)).collect();
                if origins.len() != 1 || members.iter().any(|member| invalid.contains(member)) {
                    invalid.extend(members);
                }
            }
        }
        if invalid.len() == previous { break; }
    }
    let relevant = cut::walk_bounds(&g, &cut::Bounds::setup(&kept.iter().cloned().collect::<Vec<_>>())).runs;
    let origins: BTreeMap<Located, RunId> = outcomes.iter()
        .filter(|(place, _)| relevant.contains(*place) && hashes.contains_key(*place) && !invalid.contains(*place))
        .map(|(place, result)| (place.clone(), result.origin))
        .collect();
    // A start the seed reuses whole is said so: a node start, or a group
    // start whose every member this run would run is reused.
    for place in spec.handed(project).map_err(anyhow::Error::msg)?.keys() {
        let reused: BTreeSet<RunId> = if g.group(&place.id).is_some() {
            let Some(members) = reused_group_start(&g, place, &authored.runs, &origins) else { continue };
            members.iter().map(|member| origins[member]).collect()
        } else {
            match origins.get(place) { Some(origin) => [*origin].into(), None => continue }
        };
        let from = reused.iter().map(RunId::to_string).collect::<Vec<_>>().join(", ");
        let address = if g.group(&place.id).is_some() {
            crate::project::group_address(project, &place.id, &place.path)
        } else {
            crate::project::address_of(project, &place.id, &place.path)
        };
        warnings.push(format!(
            "'{address}' keeps the result {} {from} gave it: its code and what it is handed are as they were. Run without \
             --seed to run it again.",
            if reused.len() == 1 { "run" } else { "runs" },
        ));
    }
    // The starts that run again keep what the old run handed them, where
    // this run hands nothing new: they were its starting values, whatever
    // changed in their code.
    let mut handed = BTreeMap::new();
    for place in authored.runs.iter().filter(|place| roots.contains(*place) && !origins.contains_key(*place)) {
        let Some(old) = outcomes.get(place).filter(|old| !old.handed.is_empty()) else { continue };
        let now = handed_now.get(place);
        let still: BTreeMap<String, Value> = old.handed.iter().filter(|(port, _)| now.is_none_or(|now| !now.contains_key(*port)))
            .map(|(port, value)| (port.clone(), value.clone())).collect();
        if !still.is_empty() { handed.insert(place.clone(), still); }
    }
    // What is reused and sends into what runs again stands in.
    let running: BTreeSet<&Located> = authored.runs.iter().filter(|place| !origins.contains_key(*place)).collect();
    let scopes_running: BTreeSet<Located> = running.iter().flat_map(|place| scopes_around(&g, place)).collect();
    let mut stand_ins = BTreeMap::new();
    for (place, origin) in &origins {
        let feeds = outgoing(&g, place).any(|(_, target)| running.contains(&target) || scopes_running.contains(&target));
        if !feeds { continue; }
        let old = &outcomes[place];
        stand_ins.insert(place.clone(), StandIn { ports: old.outputs.clone(), origin: GivenOrigin::Inherited { run: *origin } });
    }
    // What runs again and waited there replays its answers, its code
    // unchanged.
    let mut answers = BTreeSet::new();
    for (place, was) in answered {
        if !running.contains(place) { continue; }
        if program.node_hash(project, &place.id)? == *was {
            answers.insert(place.clone());
        } else {
            warnings.push(format!(
                "'{}' runs again and asks again: its code changed since the seed run answered it",
                crate::project::address_of(project, &place.id, &place.path),
            ));
        }
    }
    Ok(SeedPlan { origins, stand_ins, handed, answers, warnings })
}

/// The places a walk starts its work at: those it runs that no wire
/// from what it keeps feeds.
fn roots_of(g: &ProjectGraph, walked: &Walk) -> BTreeSet<Located> {
    let kept = walked.kept();
    walked.runs.iter()
        .filter(|place| !incoming(g, place).any(|(_, source)| kept.contains(&source)))
        .cloned().collect()
}

/// Save the authored spec with the starting values its seed supplied, for
/// a run that is frozen into an example: the values the old run handed
/// the starts that run again, and what a reused result sent into a start.
/// Results computed inside the run stay computations when the example
/// runs again.
pub fn starting_parameters(
    project: &ProjectDefinition,
    spec: &RunSpec,
    authored: &Walk,
    planned: &SeedPlan,
    outcomes: &BTreeMap<Located, SeedOutcome>,
) -> anyhow::Result<RunSpec> {
    let g = ProjectGraph::new(project);
    let mut saved = spec.clone();
    if saved.fire.is_none() {
        for (place, outcome) in outcomes.iter().filter(|(place, _)| authored.runs.contains(*place) && planned.origins.contains_key(*place)) {
            if let Some(payload) = &outcome.fire {
                anyhow::ensure!(saved.fire.is_none(), "seed history contains more than one triggering use case");
                saved.fire = Some((crate::project::address_of(project, &place.id, &place.path), payload.clone()));
            }
        }
    }
    let roots = roots_of(&g, authored);
    if saved.from.is_empty() && saved.group.is_none()
        && (saved.fire != spec.fire || roots.iter().any(|root| planned.origins.contains_key(root)
            && outcomes.get(root).is_some_and(|old| !old.handed.is_empty())))
    {
        // Name every root so keeping one root's inputs does not silently
        // drop the independent branches of the same use case. A root
        // inside a loop starts as the outermost loop around it; one inside
        // an included file is spelled through the call it ran under.
        for root in &roots {
            if g.is_trigger(&root.id) { continue; }
            let start = enclosing_loops(&g, root).into_iter().next().unwrap_or_else(|| root.clone());
            saved.from.entry(crate::project::group_address(project, &start.id, &start.path)).or_default();
        }
    }
    let handed_by_place: Vec<(String, Located)> = saved.from.keys().chain(saved.group.iter().map(|(id, _)| id))
        .map(|spelled| Ok((spelled.clone(), crate::project::places::named(&g, spelled)?.into_place())))
        .collect::<Result<_, String>>().map_err(anyhow::Error::msg)?;
    // What the old run handed a place: kept for one that runs again, or
    // as it was for one this run reuses.
    let handed_to = |place: &Located| -> Option<&BTreeMap<String, Value>> {
        planned.handed.get(place).or_else(|| planned.origins.contains_key(place).then(|| outcomes.get(place).map(|old| &old.handed)).flatten())
    };
    for (spelled, start) in handed_by_place {
        let mut ports: BTreeMap<String, Value> = BTreeMap::new();
        if let Some(old) = handed_to(&start) {
            ports.extend(old.iter().map(|(port, value)| (port.clone(), value.clone())));
        }
        if g.group(&start.id).is_some() {
            // A group start's inputs were handed to the members that read
            // them: each comes back under the group's own port, where the
            // wire read it whole.
            for member in members_in(&g, &start.id, &start.path) {
                let Some(old) = handed_to(&member) else { continue };
                for (edge, _) in incoming(&g, &member) {
                    let Some(k) = edge.via.iter().position(|c| c.side == crate::project::CrossingSide::In && c.group == start.id) else { continue };
                    if !edge.path_after(k).is_empty() { continue; }
                    if let Some(value) = old.get(edge.target_port()) {
                        ports.entry(edge.via[k].port.clone()).or_insert_with(|| value.clone());
                    }
                }
            }
        }
        // What a reused result outside the run sent into the start (into
        // one of its inputs, or a group start's), projected the way the
        // wire reads it: those were the start's real inputs.
        let readers: Vec<(Located, String)> = if g.group(&start.id).is_some() {
            members_in(&g, &start.id, &start.path).into_iter().map(|member| (member, String::new())).collect()
        } else {
            vec![(start.clone(), String::new())]
        };
        for (reader, _) in readers {
            for (edge, source) in incoming(&g, &reader) {
                if authored.runs.contains(&source) || !planned.origins.contains_key(&source) { continue; }
                let Some(old) = outcomes.get(&source) else { continue };
                let Some(GivenPort::Value { value }) = old.outputs.get(edge.source_port()) else { continue };
                // A loop start reads on its `LoopIn`, whose ports are the
                // loop's own.
                let loop_entry = g.node(&reader.id).and_then(|node| node.group_boundary.as_ref())
                    .is_some_and(|boundary| boundary.group_id == start.id);
                let (port, path) = if start.id == reader.id || loop_entry {
                    (edge.target_port().to_string(), edge.path.clone())
                } else {
                    let Some(k) = edge.via.iter().position(|c| c.side == crate::project::CrossingSide::In && c.group == start.id) else { continue };
                    let before: usize = edge.via.iter().take(k + 1).map(|c| c.path.len()).sum();
                    (edge.via[k].port.clone(), edge.path[..before].to_vec())
                };
                // The source's type where its value was made (a node inside
                // an included file has the types its site gave it).
                let ty = g.node(&source.id).and_then(|node| node.types_at(&source.path).output(edge.source_port()).cloned())
                    .ok_or_else(|| anyhow::anyhow!("seed wire names undeclared output '{}.{}'", source.id, edge.source_port()))?;
                let delivered = if path.is_empty() { Some(value.clone()) } else {
                    match crate::deref::project_value(&std::sync::Arc::new(value.clone()), &ty, &path).map_err(anyhow::Error::msg)? {
                        crate::deref::Projection::Value(value) => Some(value),
                        crate::deref::Projection::Closed { .. } => None,
                    }
                };
                if let Some(value) = delivered {
                    if let Some(previous) = ports.insert(port.clone(), value.clone()) {
                        anyhow::ensure!(previous == value || handed_to(&start).is_some_and(|old| old.get(&port) == Some(&previous)),
                            "seed supplies conflicting starting values to '{spelled}.{port}'");
                    }
                }
            }
        }
        let entry = if saved.group.as_ref().is_some_and(|(id, _)| *id == spelled) {
            &mut saved.group.as_mut().expect("checked").1
        } else {
            saved.from.entry(spelled).or_default()
        };
        for (port, value) in ports { entry.entry(port).or_insert(value); }
    }
    Ok(saved)
}

/// The places of a folded run whose outcome a child may inherit: every
/// record the place has is Completed or Skipped (a node fired in three
/// loop iterations must have settled in all three), and none is parked
/// on a suspension. A place with no record never ran and is not here,
/// unless a scope around it did not run (it was told so, and that is its
/// outcome too).
pub fn inheritable_nodes(project: &ProjectDefinition, snapshot: &RunSnapshot) -> HashSet<Located> {
    let g = ProjectGraph::new(project);
    let index = ProgramIndex::build(project);
    let mut eligible: HashSet<Located> = HashSet::new();
    let mut unfinished: HashSet<Located> = HashSet::new();
    for (id, records) in snapshot.firings.iter() {
        for record in records.values() {
            let place = Located::at(id, &record.frames);
            if matches!(record.status(), NodeFiringStatus::Completed | NodeFiringStatus::Skipped) {
                eligible.insert(place);
            } else {
                unfinished.insert(place);
            }
        }
    }
    eligible.retain(|place| !unfinished.contains(place));
    let places = every_place(project);
    // Loops are reused whole: an outermost loop (none around it, through
    // its scope or through a site on its path) is complete or not, and
    // every place inside it, nested loops and called bodies included,
    // follows that verdict.
    for group in project.groups.iter().filter(|group| matches!(group.kind, GroupKind::Loop { .. })) {
        let entry_id = loop_in_id(&group.id);
        for at in places.iter().filter(|place| place.id == entry_id) {
            if enclosing_loops(&g, at).iter().any(|around| around.id != group.id) { continue; }
            let members = members_in(&g, &group.id, &at.path);
            let complete = complete_loop(project, &index, snapshot, &group.id, &at.frames())
                && members.iter().all(|member| snapshot.firings.get(&member.id).is_none_or(|records|
                    records.values().filter(|record| Located::at(&member.id, &record.frames) == *member)
                        .all(|record| matches!(record.status(), NodeFiringStatus::Completed | NodeFiringStatus::Skipped))));
            for member in members {
                if complete { eligible.insert(member); } else { eligible.remove(&member); }
            }
        }
    }
    eligible
}

/// Whether a scope around the node `id` was decided not to run at
/// `frames`.
fn scope_skipped_at(project: &ProjectDefinition, index: &ProgramIndex, snapshot: &RunSnapshot, id: &str, frames: &LoopFrames) -> bool {
    project.nodes.iter().find(|node| node.id == id).is_some_and(|node| matches!(
        crate::run::scope::scope_permission(index, node, frames, &snapshot.scopes),
        crate::run::scope::ScopePermission::Skipped(_)
    ))
}

/// Whether the loop `group`, opened at `frames`, ran to a clean end
/// with every iteration settled.
fn complete_loop(project: &ProjectDefinition, index: &ProgramIndex, snapshot: &RunSnapshot, group: &str, frames: &LoopFrames) -> bool {
    let entry = loop_in_id(group);
    if scope_skipped_at(project, index, snapshot, &entry, frames) { return true; }
    let Some(records) = snapshot.firings.get(&entry) else { return false };
    let Some(record) = records.get(frames) else { return false };
    if record.status() == NodeFiringStatus::Skipped { return true; }
    if record.status() != NodeFiringStatus::Completed { return false; }
    let key = crate::primitive::LoopInstanceKey { group_id: group.into(), parent_frames: frames.clone(), run_id: snapshot.run_id };
    let Some(instance) = snapshot.loop_runtime.get(&key) else { return false };
    if instance.terminated.is_none_or(|reason| matches!(reason,
        crate::primitive::LoopTerminationReason::Failed | crate::primitive::LoopTerminationReason::Cancelled)) { return false; }
    instance.launched.iter().all(|index_launched| {
        let mut iteration = frames.clone();
        iteration.push(crate::frames::Frame::Loop { index: index_launched });
        scope_settled(project, index, snapshot, group, &iteration)
    })
}

/// The innermost scope around a node or a group (its `scope` chain, or
/// its parents) that runs at frames of its own: a loop (one per
/// iteration) or a body (one per call). `None` at the top.
fn frame_scope<'a>(project: &'a ProjectDefinition, around: impl DoubleEndedIterator<Item = &'a String>) -> Option<&'a str> {
    around.rev().find(|scope| project.groups.iter().any(|g| g.id == **scope && matches!(g.kind, GroupKind::Loop { .. } | GroupKind::Body)))
        .map(String::as_str)
}

/// The groups around `group`, outermost first.
fn groups_around(project: &ProjectDefinition, group: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut at = project.groups.iter().find(|g| g.id == group).and_then(|g| g.parent_group_id.clone());
    while let Some(id) = at {
        at = project.groups.iter().find(|g| g.id == id).and_then(|g| g.parent_group_id.clone());
        out.push(id);
    }
    out.reverse();
    out
}

/// Whether everything that fires at `frames` inside the scope `scope`
/// that runs at frames of its own (a loop's iteration, or a body under
/// one call) has settled: its nodes, a nested loop through
/// `complete_loop`, and each call site's body one call frame deeper. A
/// loop's `LoopOut` fires per iteration and is checked here; a loop's
/// `LoopIn` fires at its parent's frames and is the parent's to check.
fn scope_settled(project: &ProjectDefinition, index: &ProgramIndex, snapshot: &RunSnapshot, scope: &str, frames: &LoopFrames) -> bool {
    let settled_at = |id: &str| snapshot.firings.get(id).and_then(|records| records.get(frames))
        .is_some_and(|record| matches!(record.status(), NodeFiringStatus::Completed | NodeFiringStatus::Skipped));
    for node in &project.nodes {
        if !index.admits(&node.id, frames) || index.stands_in(&node.id, frames) { continue; }
        let own_out = node.node_type == crate::project::boundary_types::LOOP_OUT
            && node.group_boundary.as_ref().is_some_and(|b| b.group_id == scope);
        let inside = frame_scope(project, node.scope.iter()) == Some(scope);
        if !inside && !own_out { continue; }
        if scope_skipped_at(project, index, snapshot, &node.id, frames) { continue; }
        if inside && !own_out {
            if let Some(boundary) = &node.group_boundary {
                if node.node_type == crate::project::boundary_types::LOOP_IN {
                    if !complete_loop(project, index, snapshot, &boundary.group_id, frames) { return false; }
                    continue;
                }
                // A nested loop's `LoopOut` fires per iteration: `complete_loop` checks it.
                continue;
            }
        }
        if !settled_at(&node.id) { return false; }
    }
    for site in project.groups.iter() {
        let GroupKind::Call { body, .. } = &site.kind else { continue };
        let around = groups_around(project, &site.id);
        if frame_scope(project, around.iter()) != Some(scope) { continue; }
        let mut inside_call = frames.clone();
        inside_call.push(crate::frames::Frame::Call { site: site.id.clone() });
        if !scope_settled(project, index, snapshot, body, &inside_call) { return false; }
    }
    true
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chat_file(version: u64) -> crate::storage::StoredFile {
        crate::storage::StoredFile {
            key: "t/project/p/f".into(),
            mime_type: "application/x-ndjson".into(),
            size_bytes: 10,
            filename: "chat.jsonl".into(),
            version,
            link_secs: None,
        }
    }

    /// A reused output's file still stands in only at the version the
    /// node left it at; a newer version or a missing file refuses,
    /// naming the node, the port, the file and both ways out.
    #[test]
    fn a_reused_file_must_be_where_the_node_left_it() {
        assert_eq!(moved_file_refusal("ask", "historyFile", &chat_file(4), FileNow::At(4)), None);
        let moved = moved_file_refusal("ask", "historyFile", &chat_file(4), FileNow::At(6)).unwrap();
        for part in ["'ask'", "'historyFile'", "'chat.jsonl'", "version 4", "version 6", "--emit ask=", "--seed-before ask"] {
            assert!(moved.contains(part), "{part} in {moved}");
        }
        let gone = moved_file_refusal("ask", "historyFile", &chat_file(4), FileNow::Gone).unwrap();
        assert!(gone.contains("is gone"), "{gone}");
    }

    /// The files an output names are found wherever the value holds
    /// them; a file at a URL is not in storage.
    #[test]
    fn stored_files_are_found_anywhere_in_a_value() {
        let value = json!({
            "reply": "hi",
            "files": [chat_file(2).to_value(), { "nested": chat_file(3).to_value() }],
            "link": { "__weft_image__": { "url": "https://x/y.png", "mimeType": "image/png", "sizeBytes": 1, "filename": "y.png" } },
        });
        let mut versions: Vec<u64> = crate::storage::stored_files_within(&value).iter().map(|f| f.version).collect();
        versions.sort_unstable();
        assert_eq!(versions, vec![2, 3]);
        assert_eq!(crate::storage::stored_files_within(&chat_file(1).to_value()).len(), 1);
    }

    fn top(id: &str) -> Located {
        Located::top(id)
    }

    fn tops(ids: &[&str]) -> BTreeSet<Located> {
        ids.iter().map(|id| top(id)).collect()
    }

    /// `a -> b -> c`, plus a loop `l` after `c`: `c -> l__in -> body -> l__out -> d`.
    fn program(b_cfg: &str, wires: &[(&str, &str)]) -> ProjectDefinition {
        let node = |id: &str, ty: &str, cfg: &str, scope: &[&str], boundary: Value| {
            json!({
                "id": id, "nodeType": ty, "label": null, "config": {"v": cfg},
                "position": {"x": 0, "y": 0},
                "inputs": [{"name": "in", "portType": "String", "required": true}],
                "outputs": [{"name": "out", "portType": "String", "required": true}],
                "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false,
            })
        };
        let edges: Vec<Value> = wires
            .iter()
            .map(|(s, t)| json!({"id": format!("{s}->{t}"), "source": s, "target": t, "sourceHandle": "out", "targetHandle": "in"}))
            .collect();
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("a", "T", "a", &[], Value::Null),
                node("b", "T", b_cfg, &[], Value::Null),
                node("c", "T", "c", &[], Value::Null),
                node("l__in", "LoopIn", "", &[], json!({"groupId": "l", "role": "In"})),
                node("body", "T", "", &["l"], Value::Null),
                node("l__out", "LoopOut", "", &[], json!({"groupId": "l", "role": "Out"})),
                node("d", "T", "d", &[], Value::Null),
                node("x", "T", "x", &[], Value::Null),
            ],
            "edges": edges,
            "groups": [{"id": "l", "kind": "loop", "loopConfig": {}, "nodeIds": ["body"], "inPorts": [], "outPorts": []}],
        }))
        .expect("program")
    }

    const WIRES: &[(&str, &str)] = &[("a", "b"), ("b", "c"), ("c", "l__in"), ("l__in", "body"), ("body", "l__out"), ("l__out", "d")];

    fn identity(project: &ProjectDefinition) -> ProgramIdentity {
        ProgramIdentity {
            definition_hash: crate::project::hash::compute_definition_hash(project).unwrap(),
            binary_hash: "worker".into(),
            implementations: project.nodes.iter().map(|node| (node.node_type.clone(), "implementation".into())).collect(),
        }
    }

    /// Every place's result, from one run, each having sent `"<id> out"`.
    fn outcomes(project: &ProjectDefinition) -> BTreeMap<Located, SeedOutcome> {
        identity(project).slice_hashes(project).unwrap().into_iter().map(|(place, slice_hash)| {
            let outputs = [("out".to_string(), GivenPort::Value { value: json!(format!("{} out", place.id)) })].into();
            (place, SeedOutcome { origin: RunId::nil(), slice_hash, handed: BTreeMap::new(), gates: BTreeMap::new(), emitted_live_handle: false, fire: None, outputs })
        }).collect()
    }

    fn authored(project: &ProjectDefinition, spec: &RunSpec) -> crate::run_spec::Resolved {
        crate::run_spec::resolve_spec(spec, project, &Default::default()).unwrap()
    }

    fn plan(project: &ProjectDefinition, spec: &RunSpec, old: &BTreeMap<Located, SeedOutcome>, until: &[&str], before: &[&str]) -> anyhow::Result<SeedPlan> {
        let owned = |names: &[&str]| names.iter().map(|name| name.to_string()).collect::<Vec<_>>();
        seed_plan(project, &identity(project), &authored(project, spec), spec, old, &BTreeMap::new(), &owned(until), &owned(before))
    }

    /// What a run of `spec` seeded by `planned` runs.
    fn runs(project: &ProjectDefinition, spec: &RunSpec, planned: &SeedPlan) -> BTreeSet<Located> {
        crate::run_spec::resolve_seeded(spec, project, &Default::default(), Some(planned)).unwrap().runs
    }

    fn planned(parent: &ProjectDefinition, child: &ProjectDefinition, until: &[&str], missing: &[&str]) -> BTreeSet<Located> {
        let mut old = outcomes(parent);
        for node in missing { old.remove(&top(node)); }
        let spec = RunSpec::whole("test");
        let planned = plan(child, &spec, &old, until, &[]).unwrap();
        runs(child, &spec, &planned)
    }

    /// A start that runs again keeps what a reused result outside the run
    /// sent it, and a value the person handed it wins over that; the
    /// saved spec walks the same cut. A whole run that reuses an interior
    /// result saves nothing more.
    #[test]
    fn saved_seeded_starts_preserve_the_cut_and_external_values_only() {
        let project = program("b", &[("a", "b"), ("b", "c"), ("c", "l__in"), ("l__out", "d")]);
        for (spec, source, entry, port, value) in [
            (RunSpec { from: [("b".into(), BTreeMap::new())].into(), target: vec!["c".into()], ..RunSpec::whole("case") }, "a", "b", "in", "a out"),
            (RunSpec { from: [("b".into(), [("in".into(), json!("handed"))].into())].into(), target: vec!["c".into()], ..RunSpec::whole("case") }, "a", "b", "in", "handed"),
            (RunSpec { group: Some(("l".into(), BTreeMap::new())), ..RunSpec::whole("case") }, "c", "l", "in", "c out"),
        ] {
            let authored = authored(&project, &spec);
            let planned = SeedPlan { origins: [(top(source), RunId::nil())].into(), ..Default::default() };
            let saved = starting_parameters(&project, &spec, &authored.walk, &planned, &outcomes(&project)).unwrap();
            let held = saved.from.get(entry).or(saved.group.as_ref().map(|(_, ports)| ports)).unwrap();
            assert_eq!(held[port], json!(value), "{entry}");
            assert_eq!(saved.target, spec.target);
            assert_eq!(crate::run_spec::resolve_spec(&saved, &project, &Default::default()).unwrap().runs, authored.runs);
        }
        let whole = RunSpec::whole("whole");
        let planned = SeedPlan { origins: [(top("a"), RunId::nil())].into(), ..Default::default() };
        assert_eq!(starting_parameters(&project, &whole, &authored(&project, &whole).walk, &planned, &outcomes(&project)).unwrap(), whole);
    }

    #[test]
    fn saved_seed_input_contains_the_projected_field() {
        let mut project = program("b", &[("a", "b")]);
        project.nodes.iter_mut().find(|node| node.id == "a").unwrap().outputs[0].port_type =
            crate::weft_type::WeftType::parse("{ field: String }").unwrap();
        project.edges[0].path = vec!["field".into()];
        let spec = RunSpec { from: [("b".into(), BTreeMap::new())].into(), ..RunSpec::whole("projected") };
        let planned = SeedPlan { origins: [(top("a"), RunId::nil())].into(), ..Default::default() };
        let mut old = outcomes(&project);
        old.get_mut(&top("a")).unwrap().outputs.insert("out".into(), GivenPort::Value { value: json!({"field": "delivered"}) });
        let saved = starting_parameters(&project, &spec, &authored(&project, &spec).walk, &planned, &old).unwrap();
        assert_eq!(saved.from["b"]["in"], json!("delivered"));
    }

    /// `src -> s (a call of the body @f) -> out`: the body holds `@f.n`,
    /// which reads the site's input, an unwired `@f.free`, and a plain
    /// group `@f.g` whose member `@f.g.x` writes an output of `@f.g` that
    /// nobody reads.
    fn called_program() -> ProjectDefinition {
        let node = |id: &str, scope: &[&str]| json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": {"x": 0, "y": 0},
            "inputs": [{"name": "v", "portType": "String", "required": false}],
            "outputs": [{"name": "v", "portType": "String", "required": true}],
            "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false,
        });
        let wire = |s: &str, via: &[(&str, &str)], t: &str| json!({
            "id": format!("{s}->{t}"), "source": s, "target": t, "sourceHandle": "v", "targetHandle": "v",
            "via": via.iter().map(|(group, side)| json!({"group": group, "port": "v", "side": side})).collect::<Vec<_>>(),
        });
        let port = json!([{"name": "v", "portType": "String", "required": false}]);
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", &[]),
                node("@f.n", &["@f"]),
                node("@f.free", &["@f"]),
                node("@f.g.x", &["@f", "@f.g"]),
                node("out", &[]),
            ],
            "edges": [
                wire("src", &[("s", "in"), ("@f", "in")], "@f.n"),
                wire("@f.n", &[("@f", "out"), ("s", "out")], "out"),
                wire("@f.g.x", &[("@f.g", "out")], "@f.g"),
            ],
            "groups": [
                {"id": "s", "kind": "call", "body": "@f", "nodeIds": [], "inPorts": port, "outPorts": port},
                {"id": "@f", "kind": "body", "nodeIds": ["@f.n", "@f.free"], "inPorts": port, "outPorts": port},
                {"id": "@f.g", "kind": "group", "parentGroupId": "@f", "nodeIds": ["@f.g.x"], "outPorts": port}
            ],
        })).expect("called program")
    }

    /// A saved seeded run spells every root the way the program reads:
    /// a root inside an included file through its site; each spelling
    /// resolves back to the place it came from, and a reused root keeps
    /// what the old run handed it.
    #[test]
    fn saved_roots_inside_an_included_file_are_spelled_through_the_site() {
        let mut project = called_program();
        // No wire into the site: what it calls starts on its own.
        project.edges.retain(|e| e.source != "src");
        let spec = RunSpec::whole("case");
        let authored = authored(&project, &spec);
        let at = |id: &str| Located::new(id, vec!["s".into()]);
        let roots = roots_of(&ProjectGraph::new(&project), &authored.walk);
        for root in [top("src"), at("@f.n"), at("@f.free"), at("@f.g.x")] {
            assert!(roots.contains(&root), "{root} in {roots:?}");
        }
        let mut old = outcomes(&project);
        old.get_mut(&at("@f.free")).unwrap().handed.insert("v".into(), json!("kept"));
        let planned = SeedPlan { origins: roots.iter().map(|r| (r.clone(), RunId::nil())).collect(), ..Default::default() };
        let saved = starting_parameters(&project, &spec, &authored.walk, &planned, &old).unwrap();
        let spelled: BTreeSet<&str> = saved.from.keys().map(String::as_str).collect();
        assert_eq!(spelled, ["src", "s.n", "s.free", "s.g.x"].into_iter().collect(), "{spelled:?}");
        assert_eq!(saved.from["s.free"]["v"], json!("kept"));
        for (key, place) in [("s.free", at("@f.free")), ("s.g.x", at("@f.g.x")), ("s.n", at("@f.n"))] {
            let (id, path) = crate::project::resolve_address(&project, key);
            assert_eq!(Located::new(id, path), place, "{key}");
        }
    }

    #[test]
    fn saved_whole_run_preserves_original_roots_and_recomputes_their_descendants() {
        for trigger in [false, true] {
            let mut project = program("b", WIRES);
            project.nodes.iter_mut().find(|node| node.id == "a").unwrap().features.is_trigger = trigger;
            let spec = RunSpec::whole("case");
            let authored = authored(&project, &spec);
            let mut previous = outcomes(&project);
            if trigger {
                previous.get_mut(&top("a")).unwrap().fire = Some(json!({"event": "original wake"}));
            } else {
                previous.get_mut(&top("a")).unwrap().handed.insert("in".into(), json!("original input"));
            }
            let planned = SeedPlan { origins: [(top("a"), RunId::nil())].into(), ..Default::default() };
            let saved = starting_parameters(&project, &spec, &authored.walk, &planned, &previous).unwrap();
            assert!(saved.from.contains_key("x"), "independent original root remains part of the run");
            assert!(!saved.from.contains_key("b"), "computed interior inputs must not be frozen");
            if trigger {
                assert_eq!(saved.fire, Some(("a".into(), json!({"event": "original wake"}))));
                assert!(!saved.from.contains_key("a"));
            } else {
                assert_eq!(saved.from["a"]["in"], json!("original input"));
            }
            let fresh = crate::run_spec::resolve_spec(&saved, &project, &Default::default()).unwrap();
            assert_eq!(fresh.runs, authored.runs);
            assert!(fresh.runs.contains(&top("a")) && fresh.runs.contains(&top("b")) && fresh.runs.contains(&top("d")));
        }
    }

    /// A group's members that start with it keep what the old run handed
    /// them (a group start hands its inputs to the members that read
    /// them), and the members no wire feeds are named too.
    #[test]
    fn saved_whole_group_start_keeps_unwired_members() {
        let mut project = program("b", &[]);
        project.groups[0].kind = GroupKind::Group;
        project.groups[0].in_ports = vec![serde_json::from_value(json!({"name": "in", "portType": "String", "required": false})).unwrap()];
        project.groups[0].node_ids.push("x".into());
        project.nodes.retain(|node| node.group_boundary.is_none());
        project.nodes.iter_mut().find(|node| node.id == "x").unwrap().scope = vec!["l".into()];
        project.edges = vec![serde_json::from_value(json!({
            "id": "l.in->body.in", "source": "l", "target": "body", "sourceHandle": "in", "targetHandle": "in",
            "via": [{"group": "l", "port": "in", "side": "in"}],
        })).unwrap()];
        let spec = RunSpec::whole("whole");
        let authored = authored(&project, &spec);
        let mut previous = outcomes(&project);
        previous.get_mut(&top("body")).unwrap().handed.insert("in".into(), json!("saved group input"));
        let planned = SeedPlan { origins: [(top("body"), RunId::nil())].into(), ..Default::default() };
        let saved = starting_parameters(&project, &spec, &authored.walk, &planned, &previous).unwrap();
        assert_eq!(saved.from["body"]["in"], json!("saved group input"));
        assert!(saved.from.contains_key("x"), "the member no wire feeds is named");
        let fresh = crate::run_spec::resolve_spec(&saved, &project, &Default::default()).unwrap();
        assert_eq!(fresh.runs, authored.runs);
        assert!(fresh.runs.contains(&top("x")));
    }

    #[test]
    fn a_new_trigger_run_does_not_inherit_an_unrelated_trigger_use_case() {
        let mut project = program("b", WIRES);
        for node in project.nodes.iter_mut().filter(|node| node.id == "a" || node.id == "x") {
            node.features.is_trigger = true;
        }
        let mut previous = outcomes(&project);
        previous.get_mut(&top("a")).unwrap().fire = Some(json!({"old": "event"}));
        let spec = RunSpec { fire: Some(("x".into(), json!({"new": "event"}))), ..RunSpec::whole("case") };
        let planned = plan(&project, &spec, &previous, &[], &[]).unwrap();
        assert!(planned.origins.is_empty());
        assert_eq!(starting_parameters(&project, &spec, &authored(&project, &spec).walk, &planned, &previous).unwrap(), spec);
    }

    #[test]
    fn nested_live_handles_keep_the_producer_and_its_consumers_out_of_reuse() {
        let project = program("b", WIRES);
        let mut previous = outcomes(&project);
        let handle = json!({"record": [{"__weft_bus__":{"id":"old-run-handle","mode":"broadcast"}}]});
        assert!(crate::weft_type::WeftType::contains_bus_handle(&handle));
        assert!(!crate::weft_type::WeftType::contains_bus_handle(&json!({"record":["ordinary", 1, null]})));
        previous.get_mut(&top("a")).unwrap().emitted_live_handle = crate::weft_type::WeftType::contains_bus_handle(&handle);
        let planned = plan(&project, &RunSpec::whole("test"), &previous, &[], &[]).unwrap();
        assert!(!planned.origins.contains_key(&top("a")));
        assert!(!planned.origins.contains_key(&top("b")));
        assert!(!planned.origins.contains_key(&top("body")));
        assert!(planned.origins.contains_key(&top("x")));
    }

    /// History before a start is reused (it fed the start), nothing
    /// unrelated is, and an unchanged run reuses its own work too: the
    /// cut it runs is empty, and the reused start stands in for itself.
    #[test]
    fn reuse_keeps_history_before_an_explicit_start_without_widening_the_run() {
        let p = program("b", WIRES);
        let spec = RunSpec { from: BTreeMap::from([("c".into(), BTreeMap::new())]), target: vec!["d".into()], ..RunSpec::whole("test") };
        let result = plan(&p, &spec, &outcomes(&p), &[], &[]).unwrap();
        assert!(result.origins.contains_key(&top("a")));
        assert!(result.origins.contains_key(&top("b")));
        assert!(result.origins.contains_key(&top("c")));
        assert!(!result.origins.contains_key(&top("x")), "unrelated history cannot become part of this use case");
        assert!(runs(&p, &spec, &result).is_empty(), "unchanged selected work is reused too");
    }

    #[test]
    fn a_seed_boundary_before_a_loop_runs_the_whole_loop_and_its_consumers() {
        let p = program("b", WIRES);
        let spec = RunSpec::whole("test");
        let result = plan(&p, &spec, &outcomes(&p), &["c", "x"], &[]).unwrap();
        assert_eq!(runs(&p, &spec, &result), tops(&["body", "l__in", "l__out", "d"]));
        let stand_ins: BTreeSet<Located> = result.stand_ins.keys().cloned().collect();
        assert_eq!(stand_ins, tops(&["c"]), "the reused result the loop reads stands in for itself");
        assert!(plan(&p, &spec, &outcomes(&p), &["body"], &[]).is_err());
    }

    /// A value handed to a start is that port's whatever its wire brings:
    /// a result made with something else on that port is no result of
    /// this run, and one made with the same value is.
    #[test]
    fn an_explicit_start_reruns_when_it_is_handed_what_the_old_run_did_not() {
        let p = program("b", WIRES);
        let mut old = outcomes(&p);
        let mut spec = RunSpec::whole("test");
        spec.from.insert("b".into(), BTreeMap::from([("in".into(), Value::String("new".into()))]));
        let result = plan(&p, &spec, &old, &[], &[]).unwrap();
        assert!(!result.origins.contains_key(&top("b")), "the old run read its wire on that port");
        assert!(!result.origins.contains_key(&top("d")));
        assert!(result.origins.contains_key(&top("a")), "upstream history remains reusable");
        let resolved = crate::run_spec::resolve_seeded(&spec, &p, &Default::default(), Some(&result)).unwrap();
        assert_eq!(resolved.given.inputs[&top("b")]["in"], json!("new"));
        old.get_mut(&top("b")).unwrap().handed.insert("in".into(), json!("new"));
        let result = plan(&p, &spec, &old, &[], &[]).unwrap();
        assert!(result.origins.contains_key(&top("b")), "the same handed value is reusable");
        assert!(runs(&p, &spec, &result).is_empty());
        assert!(result.warnings.iter().any(|w| w.starts_with("'b' keeps the result run") && w.contains("without --seed")),
            "the reused start is announced: {:?}", result.warnings);
    }

    /// A gate handed to a group start decides whether its members run: a
    /// result made under another gate value is not reused, one made under
    /// the same is.
    #[test]
    fn a_member_reruns_when_its_groups_handed_gate_changed() {
        let mut p = program("b", WIRES);
        p.nodes.iter_mut().find(|node| node.id == "x").unwrap().scope = vec!["gx".into()];
        p.groups.push(serde_json::from_value(json!({"id": "gx", "kind": "group", "nodeIds": ["x"], "inPorts": [], "outPorts": []})).unwrap());
        let spec = RunSpec { group: Some(("gx".into(), BTreeMap::from([("_should_flow".into(), json!(true))]))), ..RunSpec::whole("t") };
        let result = plan(&p, &spec, &outcomes(&p), &[], &[]).unwrap();
        assert!(!result.origins.contains_key(&top("x")), "made with no gate handed, it runs again");
        let mut old = outcomes(&p);
        old.get_mut(&top("x")).unwrap().gates.insert("gx._should_flow".into(), json!(true));
        let result = plan(&p, &spec, &old, &[], &[]).unwrap();
        assert!(result.origins.contains_key(&top("x")), "made under the same gate, it is reused");
    }

    /// A value handed to a loop start (`--group l='{"in": ...}'`) is its
    /// `LoopIn`'s, and feeds the loop.
    #[test]
    fn a_value_handed_to_a_loop_start_is_its_entrys() {
        let p = program("b", WIRES);
        let spec = RunSpec { group: Some(("l".into(), BTreeMap::from([("in".into(), json!("v"))]))), ..RunSpec::whole("t") };
        let resolved = crate::run_spec::resolve_spec(&spec, &p, &Default::default()).unwrap();
        assert_eq!(resolved.given.inputs[&top("l__in")]["in"], json!("v"));
        crate::run_spec::refuse_unfed(&p, &resolved, &spec).expect("the handed value feeds the loop");
        assert!(resolved.warnings.is_empty(), "{:?}", resolved.warnings);
    }

    /// A group start whose members the bounds run are all reused is
    /// reused whole, even when other members of the group are not in the
    /// run: the plan says so, and the resolver agrees and skips it.
    #[test]
    fn a_group_start_reused_for_what_the_run_runs_is_skipped() {
        let mut p = program("b", WIRES);
        for id in ["x", "a"] {
            p.nodes.iter_mut().find(|node| node.id == id).unwrap().scope = vec!["gx".into()];
        }
        p.groups.push(serde_json::from_value(json!({"id": "gx", "kind": "group", "nodeIds": ["x", "a"], "inPorts": [], "outPorts": []})).unwrap());
        let spec = RunSpec { from: BTreeMap::from([("gx".into(), BTreeMap::new())]), target: vec!["x".into()], ..RunSpec::whole("t") };
        let result = plan(&p, &spec, &outcomes(&p), &[], &[]).unwrap();
        assert!(result.origins.contains_key(&top("x")));
        assert!(result.warnings.iter().any(|w| w.starts_with("'gx' keeps the result run")), "{:?}", result.warnings);
        crate::run_spec::resolve_seeded(&spec, &p, &Default::default(), Some(&result)).expect("the reused start is skipped, not refused");
    }

    /// `--feed` on a loop start runs what feeds the loop's ports, one level
    /// up, as it does for a group's.
    #[test]
    fn a_fed_loop_start_runs_its_feeder() {
        let p = program("b", WIRES);
        let spec = RunSpec { from: BTreeMap::from([("l".into(), BTreeMap::new())]), feed: vec!["l".into()], ..RunSpec::whole("t") };
        let resolved = crate::run_spec::resolve_spec(&spec, &p, &Default::default()).unwrap();
        assert!(resolved.runs.contains(&top("c")), "the loop's feeder runs: {:?}", resolved.runs);
        assert!(!resolved.runs.contains(&top("b")), "and nothing above it");
        let refused = crate::run_spec::refuse_unfed(&p, &resolved, &spec).unwrap_err().to_string();
        assert!(refused.contains("c.in gets nothing"), "the feeder's own input is the one left unfed: {refused}");
    }

    /// A trigger stood in for (`--emit`) sends what it was given to what
    /// it feeds: nothing it feeds is short of a value.
    #[test]
    fn an_emitted_trigger_feeds_what_it_reaches() {
        let mut p = program("b", WIRES);
        p.nodes[0].features.is_trigger = true;
        let spec = RunSpec { emit: BTreeMap::from([("a".into(), BTreeMap::from([("out".into(), json!("x"))]))]), ..RunSpec::whole("t") };
        let resolved = crate::run_spec::resolve_spec(&spec, &p, &Default::default()).unwrap();
        crate::run_spec::refuse_unfed(&p, &resolved, &spec).expect("the stand-in feeds b");
        assert!(resolved.program.edges.iter().any(|edge| edge.source == "a" && edge.target == "b"));
    }

    /// A group start the seed reuses whole is announced as one, named by
    /// the group.
    #[test]
    fn a_group_start_reused_whole_is_announced() {
        let p = program("b", WIRES);
        let spec = RunSpec { group: Some(("l".into(), BTreeMap::new())), ..RunSpec::whole("test") };
        let result = plan(&p, &spec, &outcomes(&p), &[], &[]).unwrap();
        assert!(runs(&p, &spec, &result).is_empty(), "every member is reused");
        assert!(result.warnings.iter().any(|w| w.starts_with("'l' keeps the result run") && w.contains("without --seed")),
            "the reused group start is announced: {:?}", result.warnings);
    }

    #[test]
    fn unchanged_program_reuses_every_result() {
        let p = program("b", WIRES);
        assert!(planned(&p, &p, &[], &[]).is_empty());
    }

    #[test]
    fn config_change_recomputes_only_its_dependencies_and_whole_loop() {
        let parent = program("b", WIRES);
        let child = program("changed", WIRES);
        assert_eq!(planned(&parent, &child, &[], &[]), tops(&["b", "c", "l__in", "body", "l__out", "d"]));
    }

    #[test]
    fn new_wire_invalidates_its_consumer_and_dependents() {
        let parent = program("b", WIRES);
        let mut wires = WIRES.to_vec();
        wires.push(("x", "c"));
        let child = program("b", &wires);
        assert_eq!(planned(&parent, &child, &[], &[]), tops(&["c", "l__in", "body", "l__out", "d"]));
    }

    #[test]
    fn new_or_unfinished_nodes_cannot_reuse_older_history() {
        let child = program("b", WIRES);
        let mut parent = child.clone();
        parent.nodes.retain(|node| node.id != "x");
        assert_eq!(planned(&parent, &child, &[], &[]), tops(&["x"]));
        assert_eq!(planned(&child, &child, &[], &["c"]), tops(&["c", "l__in", "body", "l__out", "d"]));
    }

    #[test]
    fn seed_endpoints_reject_unknown_nodes_and_loop_interiors() {
        let p = program("b", WIRES);
        let spec = RunSpec { target: vec!["b".into()], ..RunSpec::whole("test") };
        for node in ["missing", "body"] {
            let error = plan(&p, &spec, &outcomes(&p), &[node], &[]).unwrap_err();
            assert!(error.to_string().contains(node));
        }
    }

    #[test]
    fn exclusive_and_inclusive_seed_caps_do_not_widen_the_run_cut() {
        let p = program("b", WIRES);
        let spec = RunSpec { target: vec!["c".into()], ..RunSpec::whole("test") };
        let old = outcomes(&p);
        let until = plan(&p, &spec, &old, &["b"], &[]).unwrap();
        assert!(until.origins.contains_key(&top("b")));
        assert_eq!(runs(&p, &spec, &until), tops(&["c"]));
        let before = plan(&p, &spec, &old, &[], &["b"]).unwrap();
        assert!(!before.origins.contains_key(&top("b")));
        assert_eq!(runs(&p, &spec, &before), tops(&["b", "c"]));
    }

    #[test]
    fn a_changed_implementation_invalidates_the_production_slice() {
        let p = program("b", WIRES);
        let mut edited = identity(&p);
        edited.implementations.insert("T".into(), "new code".into());
        let spec = RunSpec::whole("test");
        let result = seed_plan(&p, &edited, &authored(&p, &spec), &spec, &outcomes(&p), &BTreeMap::new(), &[], &[]).unwrap();
        assert_eq!(runs(&p, &spec, &result).len(), p.nodes.len());
        assert!(!result.warnings.is_empty());
    }

    /// A start that runs again keeps what the old run handed it, and an
    /// interior node's old value is never turned into a start of its own.
    #[test]
    fn an_old_handed_value_is_kept_only_by_a_start() {
        let parent = program("b", WIRES);
        let child = program("changed", WIRES);
        let mut old = outcomes(&parent);
        old.get_mut(&top("b")).unwrap().handed.insert("in".into(), json!("old"));
        old.get_mut(&top("c")).unwrap().handed.insert("in".into(), json!("interior"));
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), ..RunSpec::whole("test") };
        let result = plan(&child, &spec, &old, &[], &[]).unwrap();
        assert_eq!(result.handed[&top("b")]["in"], json!("old"));
        assert!(!result.handed.contains_key(&top("c")), "seed does not invent an interior starting point");
        let resolved = crate::run_spec::resolve_seeded(&spec, &child, &Default::default(), Some(&result)).unwrap();
        assert_eq!(resolved.given.inputs[&top("b")]["in"], json!("old"));
        assert!(!resolved.given.inputs.contains_key(&top("c")));
    }

    #[test]
    fn expanding_before_an_old_start_recomputes_its_handed_result() {
        let project = program("b", WIRES);
        let mut old = outcomes(&project);
        old.get_mut(&top("b")).unwrap().handed.insert("in".into(), json!("old manual input"));
        let spec = RunSpec::whole("expanded");
        let planned = plan(&project, &spec, &old, &[], &[]).unwrap();
        assert!(planned.origins.contains_key(&top("a")));
        assert!(planned.origins.contains_key(&top("x")));
        assert!(!planned.origins.contains_key(&top("b")));
        assert!(!planned.origins.contains_key(&top("d")));
        assert!(!planned.handed.contains_key(&top("b")));
        assert_eq!(starting_parameters(&project, &spec, &authored(&project, &spec).walk, &planned, &old).unwrap(), spec);
    }

    #[test]
    fn an_unwired_loop_member_is_reused_or_rerun_with_its_whole_loop() {
        let mut p = program("b", WIRES);
        let mut lonely = p.nodes.iter().find(|node| node.id == "body").unwrap().clone();
        lonely.id = "lonely".into();
        p.nodes.push(lonely);
        p.groups[0].node_ids.push("lonely".into());
        assert!(planned(&p, &p, &[], &[]).is_empty());
        let rerun = planned(&p, &p, &["c", "x"], &[]);
        for node in ["lonely", "body", "l__in", "l__out", "d"] { assert!(rerun.contains(&top(node)), "{node} in {rerun:?}"); }
    }

    #[test]
    fn a_loop_cannot_mix_results_from_different_origins() {
        let p = program("b", WIRES);
        let mut old = outcomes(&p);
        old.get_mut(&top("body")).unwrap().origin = RunId::new_v4();
        let spec = RunSpec::whole("test");
        let result = plan(&p, &spec, &old, &[], &[]).unwrap();
        let ran = runs(&p, &spec, &result);
        for node in ["body", "l__in", "l__out", "d"] { assert!(ran.contains(&top(node))); }
    }

    #[test]
    fn firing_and_simulating_a_source_always_produce_new_downstream_work() {
        let mut p = program("b", WIRES);
        p.nodes.iter_mut().find(|node| node.id == "a").unwrap().features.is_trigger = true;
        let old = outcomes(&p);
        for spec in [
            RunSpec { fire: Some(("a".into(), json!({}))), ..RunSpec::whole("fire") },
            RunSpec { emit: BTreeMap::from([("a".into(), BTreeMap::from([("out".into(), json!("new"))]))]), ..RunSpec::whole("emit") },
        ] {
            let result = plan(&p, &spec, &old, &[], &[]).unwrap();
            assert!(!result.origins.contains_key(&top("a")));
            assert!(!result.origins.contains_key(&top("b")));
            assert!(!result.origins.contains_key(&top("d")));
        }
    }

    /// A node that runs again and waited in the old run replays the
    /// answers it was given there when its own code is what it was, even
    /// though what feeds it changed; changed itself, it asks again.
    #[test]
    fn a_recorded_answer_is_reused_when_the_code_did_not_change() {
        let parent = program("b", WIRES);
        let mut child = parent.clone();
        child.nodes.iter_mut().find(|node| node.id == "a").unwrap().config = json!({"v": "a changed"});
        let spec = RunSpec::whole("test");
        let answered = BTreeMap::from([(top("b"), identity(&parent).node_hash(&parent, "b").unwrap())]);
        let result = seed_plan(&child, &identity(&child), &authored(&child, &spec), &spec, &outcomes(&parent), &answered, &[], &[]).unwrap();
        assert!(runs(&child, &spec, &result).contains(&top("b")), "b runs again: what feeds it changed");
        assert_eq!(result.answers, tops(&["b"]));

        let mut rewritten = parent.clone();
        rewritten.nodes.iter_mut().find(|node| node.id == "b").unwrap().config = json!({"v": "b changed"});
        let result = seed_plan(&rewritten, &identity(&rewritten), &authored(&rewritten, &spec), &spec, &outcomes(&parent), &answered, &[], &[]).unwrap();
        assert!(result.answers.is_empty(), "b's own code changed");
        assert!(result.warnings.iter().any(|w| w.starts_with("'b' runs again and asks again")), "{:?}", result.warnings);
        // A place this run reuses whole needs no replay.
        let result = seed_plan(&parent, &identity(&parent), &authored(&parent, &spec), &spec, &outcomes(&parent), &answered, &[], &[]).unwrap();
        assert!(result.answers.is_empty() && result.origins.contains_key(&top("b")));
    }
}
