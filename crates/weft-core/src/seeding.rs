//! Choose reusable history separately from the child's executable selection.
//! Real suppliers win over backups, and loops are reused as a whole.
//!
//! Everything here is per PLACE (`Located`): a node of an included file
//! has one result per call that reached it, and a child reuses the
//! result of the call it runs, never the other call's.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::Value;

use crate::exec::NodeExecutionStatus;
use crate::frames::{loop_indices, Located, LoopFrames};
use crate::primitive::ExecutionSnapshot;
use crate::project::{boundary_in_id, GroupBoundaryRole, GroupKind, ProjectDefinition};
use crate::project::hash::ProgramIdentity;
use crate::project::selection::{enclosing_loops, every_place, is_body, members_with_paths, source_place, RunSelection, SelectionBounds};
use crate::run_spec::{ExpectedWire, RunSpec};
use crate::Color;

/// One complete result the parent retained. Its origin is already chosen;
/// a failed newer attempt never falls through to an older successful one.
#[derive(Debug, Clone)]
pub struct SeedOutcome {
    pub origin: Color,
    pub slice_hash: String,
    pub used_backups: BTreeMap<String, Value>,
    pub backup_origins: BTreeMap<String, Color>,
    pub absent_ports: BTreeSet<String>,
    /// Ordinary group ports actually evaluated under the origin's cut.
    pub boundary_ports: BTreeSet<String>,
    pub emitted_live_handle: bool,
    /// Original wake payload, only when this result actually fired a trigger.
    pub fire: Option<Value>,
}

#[derive(Debug)]
pub struct SeedPlan {
    pub origins: BTreeMap<Located, Color>,
    pub selection: RunSelection,
    pub warnings: Vec<String>,
}

/// Save the authored cut with the external starting values supplied by its seed.
/// Results computed inside that cut remain computations when the example runs again.
pub fn starting_parameters(
    project: &ProjectDefinition,
    spec: &RunSpec,
    authored: &RunSelection,
    planned: &SeedPlan,
    outcomes: &BTreeMap<Located, SeedOutcome>,
    history: &[ExpectedWire],
) -> anyhow::Result<RunSpec> {
    let mut saved = spec.clone();
    if saved.fire.is_none() {
        for (place, outcome) in outcomes.iter().filter(|(place, _)| authored.nodes.contains(*place)
            && planned.origins.contains_key(*place)) {
            if let Some(payload) = &outcome.fire {
                anyhow::ensure!(saved.fire.is_none(), "seed history contains more than one triggering use case");
                saved.fire = Some((crate::project::address_of(project, &place.id, &place.path), payload.clone()));
            }
        }
    }
    let roots = authored.roots(project);
    if saved.from.is_empty() && saved.group.is_none()
        && (saved.fire != spec.fire || roots.iter().any(|root| planned.origins.contains_key(root)
            && outcomes.get(root).is_some_and(|old| !old.used_backups.is_empty())))
    {
        // Name all original roots so preserving one root's inputs does not
        // silently discard independent branches of the same use case.
        for root in &roots {
            let node = project.nodes.iter().find(|node| node.id == root.id).expect("selected root");
            if node.features.is_trigger { continue; }
            // A root inside a loop is started as the loop, the outermost
            // one around it (a loop around a site on its path included);
            // a group's In as the group; anything else as itself. Written
            // the way the program reads: a start inside an included file
            // is spelled through the call it ran under.
            // A body's own In is no start of its own (a body runs through
            // its site): it is spelled as the half under the site.
            let start = enclosing_loops(project, root).into_iter().next()
                .or_else(|| node.group_boundary.as_ref()
                    .filter(|boundary| boundary.role == GroupBoundaryRole::In && !is_body(project, &boundary.group_id))
                    .map(|boundary| Located::new(boundary.group_id.clone(), root.path.clone())))
                .unwrap_or_else(|| root.clone());
            saved.from.entry(crate::project::address_of(project, &start.id, &start.path)).or_default();
        }
    }
    for (id, ports) in saved.from.iter_mut().chain(saved.group.iter_mut().map(|(id, ports)| (&*id, ports))) {
        let entry = crate::project::selection::start_node_at(project, id).map_err(anyhow::Error::msg)?;
        let node = project.nodes.iter().find(|node| node.id == entry.id).expect("validated start");
        if planned.origins.contains_key(&entry) {
            if let Some(old) = outcomes.get(&entry) {
                for (port, value) in &old.used_backups { ports.entry(port.clone()).or_insert_with(|| value.clone()); }
            }
        }
        if let Some(inputs) = planned.selection.input.get(&entry) {
            for (port, value) in inputs { ports.entry(port.clone()).or_insert_with(|| value.clone()); }
        }
        let mut supplied = BTreeMap::new();
        for (edge, source) in project.edges.iter().filter(|edge| edge.target == entry.id)
            .filter_map(|edge| source_place(project, &entry, edge).map(|source| (edge, source)))
            .filter(|(edge, source)| authored.has_edge(project, &entry, edge, false)
                && !authored.nodes.contains(source) && planned.origins.contains_key(source))
        {
            let port = edge.target_handle.as_deref().unwrap_or("default");
            let source_port = edge.source_handle.as_deref().unwrap_or("default");
            let source_type = project.nodes.iter().find(|node| node.id == source.id)
                .and_then(|node| node.outputs.iter().find(|port| port.name == source_port))
                .ok_or_else(|| anyhow::anyhow!("seed wire names undeclared output '{}.{source_port}'", source.id))?;
            let mut values = Vec::new();
            for output in history.iter().filter(|output| Located::at(&output.node, &output.frames) == source
                && output.port == source_port && loop_indices(&output.frames).is_empty() && !output.closed) {
                // Freeze the value delivered by this wire, including its field
                // projection, rather than the source's enclosing object.
                match crate::deref::project_value(&output.value, &source_type.port_type, &edge.path)
                    .map_err(anyhow::Error::msg)? {
                    crate::deref::Projection::Value(value) => values.push(value),
                    crate::deref::Projection::Closed { .. } => {},
                }
            }
            let Some(declared) = node.inputs.iter().find(|input| input.name == port) else {
                anyhow::bail!("seed wire targets undeclared input '{}.{port}'", node.id);
            };
            if values.is_empty() && ports.contains_key(port) { continue; }
            let value = if declared.is_generator() {
                Value::Array(values)
            } else {
                let Some(first) = values.first() else { continue };
                anyhow::ensure!(values.iter().all(|value| value == first), "seed supplies multiple different starting values to '{}.{port}'", node.id);
                first.clone()
            };
            if let Some(previous) = supplied.insert(port.to_string(), value.clone()) {
                anyhow::ensure!(previous == value, "seed supplies conflicting starting values to '{}.{port}'", node.id);
            }
        }
        // Seed outputs were real inputs to this start, so they won over an authored backup.
        ports.extend(supplied);
    }
    Ok(saved)
}

/// Choose history independently of the authored executable cut. Only the
/// frontier becomes wire supply; history before it remains displayable.
#[allow(clippy::too_many_arguments)]
pub fn seed_plan(
    project: &ProjectDefinition,
    program: &ProgramIdentity,
    selection: &RunSelection,
    spec: &RunSpec,
    outcomes: &BTreeMap<Located, SeedOutcome>,
    until: &[String],
    before: &[String],
) -> anyhow::Result<SeedPlan> {
    let cap = RunSelection::carve(project, &SelectionBounds {
        target: until.to_vec(), before: before.to_vec(), ..Default::default()
    }).map_err(anyhow::Error::msg)?;
    let hashes = program.slice_hashes(project)?;
    let starting_inputs = spec.starting_inputs(project);
    let roots: BTreeSet<_> = selection.roots(project).into_iter().collect();
    let places = every_place(project);
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
        if selection.nodes.contains(place) && !old.used_backups.is_empty()
            && !starting_inputs.contains_key(place) && !roots.contains(place)
        {
            // An old manual start is now an interior computation. Its former
            // backup is not a starting parameter of this newly selected use case.
            invalid.insert(place.clone());
        }
        if crate::project::selection::is_ordinary_boundary(project, &place.id)
            && selection.boundary_ports.get(place).is_some_and(|ports| !ports.is_subset(&old.boundary_ports))
        { invalid.insert(place.clone()); }
        if let Some(authored) = starting_inputs.get(place) {
            if authored.iter().any(|(port, value)| old.used_backups.get(port).is_some_and(|used| used != value)
                || old.absent_ports.contains(port))
            { invalid.insert(place.clone()); }
        }
    }
    // A start is a selection boundary, not a request to spend again. Reuse
    // depends on code and effective inputs. Explicit fires and output
    // simulations still create fresh events and invalidate their consumers.
    let starts: Vec<Located> = spec.emit.keys().chain(spec.fire.iter().map(|(node, _)| node))
        .map(|spelled| { let (id, path) = crate::project::resolve_address(project, spelled); Located::new(id, path) }).collect();
    invalid.extend(RunSelection::downstream(project, &starts));
    invalid.extend(places.iter().filter(|place| !cap.nodes.contains(place)).cloned());
    // Dependency selections include control paths and whole loops, without
    // expanding unrelated ports of ordinary groups. One walk per place;
    // the fixpoint below only re-reads them.
    let dependencies: BTreeMap<&Located, BTreeSet<Located>> = places.iter()
        .map(|place| (place, RunSelection::dependencies(project, std::slice::from_ref(place)).nodes)).collect();
    loop {
        let previous = invalid.len();
        for (place, dependencies) in &dependencies {
            if dependencies.iter().any(|dep| selection.nodes.contains(dep) && invalid.contains(dep)
                && !crate::project::selection::is_ordinary_boundary(project, &dep.id)) {
                invalid.insert((*place).clone());
            }
        }
        for group in project.groups.iter().filter(|g| matches!(g.kind, GroupKind::Loop { .. })) {
            for entry in places.iter().filter(|place| place.id == boundary_in_id(&group.id)) {
                let members = members_with_paths(project, &group.id, &entry.path);
                let origins: BTreeSet<_> = members.iter().filter_map(|member| outcomes.get(member).map(|o| o.origin)).collect();
                if origins.len() != 1 || members.iter().any(|member| invalid.contains(member)) {
                    invalid.extend(members);
                }
            }
        }
        if invalid.len() == previous { break; }
    }
    let relevant = selection.history_nodes(project);
    let origins: BTreeMap<_, _> = outcomes.iter().filter(|(place, _)| relevant.contains(*place) && hashes.contains_key(*place) && !invalid.contains(*place))
        .map(|(place, result)| (place.clone(), result.origin)).collect();
    // A value supplied at a start is a backup: it stands in when nothing
    // upstream supplies the port. Under a seed the upstream result is
    // usually right there in history, so the backup goes unused and the
    // node is reused as it was. Correct, and invisible from the outside
    // (the run reports success and nothing ran), so it is said out loud.
    for (place, ports) in &starting_inputs {
        let (Some(origin), Some(old)) = (origins.get(place), outcomes.get(place)) else { continue };
        for port in ports.keys().filter(|port| !old.used_backups.contains_key(*port)) {
            warnings.push(format!(
                "'{}.{port}' keeps the value run {origin} gave it; the value supplied at this start is a backup \
                 and only stands in when nothing upstream supplies the port. Run without --seed to hand it in.",
                crate::project::address_of(project, &place.id, &place.path)
            ));
        }
    }
    let mut execution = selection.clone();
    for place in starting_inputs.keys().filter(|place| selection.nodes.contains(*place)) {
        if let Some(old) = outcomes.get(place).filter(|old| !old.used_backups.is_empty() && hashes.get(place) == Some(&old.slice_hash)) {
            let inputs = execution.input.entry(place.clone()).or_default();
            for (port, value) in &old.used_backups {
                if !inputs.contains_key(port) {
                    inputs.insert(port.clone(), value.clone());
                    execution.input_origins.entry(place.clone()).or_default().insert(port.clone(), old.backup_origins.get(port).copied().unwrap_or(old.origin));
                }
            }
        }
    }
    execution = execution.with_reused(project, &origins.keys().cloned().collect());
    Ok(SeedPlan { origins, selection: execution, warnings })
}


/// The places of a folded run whose outcome a child may inherit: every
/// record the place has is Completed or Skipped (a node fired in three
/// loop iterations must have settled in all three), and none is parked
/// on a suspension. A place with no record never ran and is not here.
pub fn inheritable_nodes(project: &ProjectDefinition, snapshot: &ExecutionSnapshot) -> HashSet<Located> {
    let mut eligible: HashSet<Located> = HashSet::new();
    let mut unfinished: HashSet<Located> = HashSet::new();
    for (id, records) in &snapshot.executions {
        for record in records {
            let place = Located::at(id, &record.frames);
            if matches!(record.status, NodeExecutionStatus::Completed | NodeExecutionStatus::Skipped) {
                eligible.insert(place);
            } else {
                unfinished.insert(place);
            }
        }
    }
    eligible.retain(|place| !unfinished.contains(place));
    let places = snapshot.selection.as_ref().map(|selection| selection.nodes.clone()).unwrap_or_else(|| every_place(project));
    for place in places.iter().filter(|place| crate::project::selection::is_ordinary_boundary(project, &place.id)) {
        if skipped_scope(project, snapshot, place, &place.frames()) { eligible.insert(place.clone()); }
    }
    // Loops are reused whole: an outermost loop (none around it, through
    // its scope or through a site on its path) is complete or not, and
    // every place inside it, nested loops and called bodies included,
    // follows that verdict.
    for group in project.groups.iter().filter(|group| matches!(group.kind, GroupKind::Loop { .. })) {
        let Some(entry) = project.nodes.iter().find(|node| node.id == boundary_in_id(&group.id)) else { continue };
        for at in places.iter().filter(|place| place.id == entry.id) {
            if enclosing_loops(project, at).iter().any(|around| around.id != group.id) { continue; }
            let members = members_with_paths(project, &group.id, &at.path);
            let complete = complete_loop(project, snapshot, &group.id, &at.frames())
                && members.iter().all(|member| snapshot.executions.get(&member.id).is_none_or(|records|
                    records.iter().filter(|record| Located::at(&member.id, &record.frames) == *member)
                        .all(|record| matches!(record.status, NodeExecutionStatus::Completed | NodeExecutionStatus::Skipped))));
            for member in members {
                if complete { eligible.insert(member); } else { eligible.remove(&member); }
            }
        }
    }
    eligible
}

/// Whether a gate around `place` (a site on its path, a group in its
/// scope, its own container for an Out boundary) was skipped at
/// `frames`: the gate's In fired under a prefix of these frames and
/// recorded a skip.
fn skipped_scope(project: &ProjectDefinition, snapshot: &ExecutionSnapshot, place: &Located, frames: &LoopFrames) -> bool {
    let node = project.nodes.iter().find(|node| node.id == place.id);
    let gates = place.path.iter().cloned()
        .chain(node.iter().flat_map(|node| node.scope.iter().cloned()))
        .chain(node.iter().flat_map(|node| node.group_boundary.iter()
            .filter(|boundary| boundary.role == GroupBoundaryRole::Out).map(|boundary| boundary.group_id.clone())));
    gates.into_iter().any(|group| {
        snapshot.executions.get(&boundary_in_id(&group)).is_some_and(|records| records.iter().any(|record|
            frames.starts_with(&record.frames) && record.status == NodeExecutionStatus::Skipped))
    })
}

/// Whether the loop `group`, opened at `frames`, ran to a clean end
/// with every iteration settled.
fn complete_loop(project: &ProjectDefinition, snapshot: &ExecutionSnapshot, group: &str, frames: &LoopFrames) -> bool {
    let Some(entry) = project.nodes.iter().find(|node| node.id == boundary_in_id(group)) else { return false };
    if skipped_scope(project, snapshot, &Located::at(&entry.id, frames), frames) { return true; }
    let Some(records) = snapshot.executions.get(&entry.id) else { return false };
    let Some(record) = records.iter().find(|record| &record.frames == frames) else { return false };
    if record.status == NodeExecutionStatus::Skipped { return true; }
    if record.status != NodeExecutionStatus::Completed { return false; }
    let key = crate::primitive::LoopInstanceKey { group_id: group.into(), parent_frames: frames.clone(), color: snapshot.color };
    let Some(instance) = snapshot.loop_runtime.get(&key) else { return false };
    if instance.terminated.is_none_or(|reason| matches!(reason,
        crate::primitive::LoopTerminationReason::Failed | crate::primitive::LoopTerminationReason::Cancelled)) { return false; }
    instance.launched.iter().all(|index| {
        let mut iteration = frames.clone();
        iteration.push(crate::frames::Frame::Loop { index: *index });
        scope_settled(project, snapshot, group, &iteration)
    })
}

/// The innermost scope around `node` that opens a frame (a loop or a
/// body): the scope whose frames the node fires at. `None` at the top.
fn frame_scope_of(project: &ProjectDefinition, node: &crate::project::NodeDefinition) -> Option<String> {
    node.scope.iter().rev().find(|scope| project.nodes.iter()
        .any(|n| n.id == boundary_in_id(scope) && crate::project::boundary_types::opens_frame(&n.node_type))).cloned()
}

/// Whether everything that fires at `frames` inside the frame-opening
/// scope `scope` (a loop's iteration, or a body under one call) has
/// settled: its nodes, the halves of the plain groups and call sites
/// inside it, a nested loop through `complete_loop`, and a called body
/// one call frame deeper. A loop's Out fires per iteration and is
/// checked here; a body's own halves fire at the call's frames and are
/// checked here too; a loop's In fires at its parent's frames and is
/// the parent's to check.
fn scope_settled(project: &ProjectDefinition, snapshot: &ExecutionSnapshot, scope: &str, frames: &LoopFrames) -> bool {
    let kind_of = |group: &str| project.groups.iter().find(|g| g.id == group).map(|g| &g.kind);
    let is_body = is_body(project, scope);
    let settled_at = |id: &str| snapshot.executions.get(id).is_some_and(|records| records.iter().any(|record|
        &record.frames == frames && matches!(record.status, NodeExecutionStatus::Completed | NodeExecutionStatus::Skipped)));
    for node in &project.nodes {
        let own_half = node.group_boundary.as_ref().filter(|b| b.group_id == scope)
            .is_some_and(|b| is_body || b.role == GroupBoundaryRole::Out);
        let inside = frame_scope_of(project, node).as_deref() == Some(scope);
        if !inside && !own_half { continue; }
        if skipped_scope(project, snapshot, &Located::at(&node.id, frames), frames) { continue; }
        if inside {
            if let Some(boundary) = &node.group_boundary {
                match (kind_of(&boundary.group_id), &boundary.role) {
                    (Some(GroupKind::Loop { .. }), GroupBoundaryRole::In) => {
                        if !complete_loop(project, snapshot, &boundary.group_id, frames) { return false; }
                        continue;
                    }
                    // A nested loop's Out fires per iteration: `complete_loop` checks it.
                    (Some(GroupKind::Loop { .. }), GroupBoundaryRole::Out) => continue,
                    (Some(GroupKind::Call { body }), GroupBoundaryRole::In) => {
                        if !settled_at(&node.id) { return false; }
                        let mut inside_call = frames.clone();
                        inside_call.push(crate::frames::Frame::Call { site: boundary.group_id.clone() });
                        if !scope_settled(project, snapshot, body, &inside_call) { return false; }
                        continue;
                    }
                    _ => {}
                }
            }
        }
        if !settled_at(&node.id) { return false; }
    }
    true
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn top(id: &str) -> Located {
        Located::top(id)
    }

    fn tops(ids: &[&str]) -> BTreeSet<Located> {
        ids.iter().map(|id| top(id)).collect()
    }

    /// `a -> b -> c`, plus a loop `l` after `c`: `c -> l__in -> body -> l__out -> d`.
    fn program(b_cfg: &str, wires: &[(&str, &str)]) -> ProjectDefinition {
        let node = |id: &str, cfg: &str, scope: &[&str], boundary: Value| {
            json!({
                "id": id, "nodeType": "T", "label": null, "config": {"v": cfg},
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
                node("a", "a", &[], Value::Null),
                node("b", b_cfg, &[], Value::Null),
                node("c", "c", &[], Value::Null),
                node("l__in", "", &[], json!({"groupId": "l", "role": "In"})),
                node("body", "", &["l"], Value::Null),
                node("l__out", "", &[], json!({"groupId": "l", "role": "Out"})),
                node("d", "d", &[], Value::Null),
                node("x", "x", &[], Value::Null),
            ],
            "edges": edges,
            "groups": [{"id": "l", "kind": "loop", "loopConfig": {}, "nodes": ["body"], "inPorts": [], "outPorts": []}],
        }))
        .expect("program")
    }

    #[test]
    fn saved_seeded_starts_preserve_the_cut_and_external_values_only() {
        let project = program("b", &[("a", "b"), ("b", "c"), ("c", "l__in"), ("l__out", "d")]);
        for (spec, source, entry) in [
            (RunSpec { from: [("b".into(), [("in".into(), json!("backup"))].into())].into(), target: vec!["c".into()], ..RunSpec::whole("case") }, "a", "b"),
            (RunSpec { group: Some(("l".into(), BTreeMap::new())), ..RunSpec::whole("case") }, "c", "l__in"),
        ] {
            let authored = crate::run_spec::resolve_spec(&spec, &project).unwrap().selection;
            let planned = SeedPlan { origins: [(top(source), Color::nil())].into(), selection: authored.clone(), warnings: vec![] };
            let history = vec![ExpectedWire { node: source.into(), port: "out".into(), value: json!("original input"), ..Default::default() }];
            let saved = starting_parameters(&project, &spec, &authored, &planned, &outcomes(&project), &history).unwrap();
            assert_eq!(saved.starting_inputs(&project)[&top(entry)]["in"], json!("original input"));
            assert_eq!(saved.target, spec.target);
            assert_eq!(crate::run_spec::resolve_spec(&saved, &project).unwrap().selection.nodes, authored.nodes);
        }
        let whole = RunSpec::whole("whole");
        let authored = RunSelection::whole(&project);
        let planned = SeedPlan { origins: [(top("a"), Color::nil())].into(), selection: authored.clone(), warnings: vec![] };
        let history = vec![ExpectedWire { node: "a".into(), port: "out".into(), value: json!("intermediate"), ..Default::default() }];
        assert_eq!(starting_parameters(&project, &whole, &authored, &planned, &outcomes(&project), &history).unwrap(), whole);
    }

    #[test]
    fn saved_seed_input_contains_the_projected_field() {
        let mut project = program("b", &[("a", "b")]);
        project.nodes.iter_mut().find(|node| node.id == "a").unwrap().outputs[0].port_type =
            crate::weft_type::WeftType::parse("{ field: String }").unwrap();
        project.edges[0].path = vec!["field".into()];
        let spec = RunSpec { from: [("b".into(), BTreeMap::new())].into(), ..RunSpec::whole("projected") };
        let authored = crate::run_spec::resolve_spec(&spec, &project).unwrap().selection;
        let planned = SeedPlan { origins: [(top("a"), Color::nil())].into(), selection: authored.clone(), warnings: vec![] };
        let history = vec![ExpectedWire { node: "a".into(), port: "out".into(), value: json!({"field":"delivered"}), ..Default::default() }];
        let saved = starting_parameters(&project, &spec, &authored, &planned, &outcomes(&project), &history).unwrap();
        assert_eq!(saved.from["b"]["in"], json!("delivered"));
    }

    /// `src -> s (a call of body @f) -> out`; the body holds `@f.n` fed by
    /// its In, an unwired root `@f.free`, and a plain group `@f.g` with
    /// an unwired member `@f.g.x`.
    fn called_program() -> ProjectDefinition {
        use crate::project::boundary_types as bt;
        let node = |id: &str, ty: &str, scope: &[&str], boundary: Value| json!({
            "id": id, "nodeType": ty, "label": null, "config": {},
            "position": {"x": 0, "y": 0},
            "inputs": [{"name": "v", "portType": "String", "required": false}],
            "outputs": [{"name": "v", "portType": "String", "required": true}],
            "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false,
        });
        // One port name on both sides, the way a boundary forwards it.
        let edge = |s: &str, t: &str| json!({"id": format!("{s}->{t}"), "source": s, "target": t, "sourceHandle": "v", "targetHandle": "v"});
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", "T", &[], Value::Null),
                node("s__in", bt::CALL_IN, &[], json!({"groupId": "s", "role": "In"})),
                node("s__out", bt::CALL_OUT, &[], json!({"groupId": "s", "role": "Out"})),
                node("@f__in", bt::INCLUDE_IN, &[], json!({"groupId": "@f", "role": "In"})),
                node("@f.n", "T", &["@f"], Value::Null),
                node("@f.free", "T", &["@f"], Value::Null),
                node("@f.g__in", bt::PASSTHROUGH, &["@f"], json!({"groupId": "@f.g", "role": "In"})),
                node("@f.g.x", "T", &["@f", "@f.g"], Value::Null),
                node("@f.g__out", bt::PASSTHROUGH, &["@f"], json!({"groupId": "@f.g", "role": "Out"})),
                node("@f__out", bt::INCLUDE_OUT, &[], json!({"groupId": "@f", "role": "Out"})),
                node("out", "T", &[], Value::Null),
            ],
            "edges": [
                edge("src", "s__in"), edge("s__in", "@f__in"), edge("@f__in", "@f.n"), edge("@f.n", "@f__out"),
                edge("@f__out", "s__out"), edge("s__out", "out"), edge("@f.g.x", "@f.g__out"),
            ],
            "groups": [
                {"id": "s", "kind": "call", "body": "@f", "nodeIds": []},
                {"id": "@f", "kind": "body", "nodeIds": ["@f.n", "@f.free"]},
                {"id": "@f.g", "kind": "group", "nodeIds": ["@f.g.x"]}
            ],
        })).expect("called program")
    }

    /// A saved seeded run spells every root the way the program reads:
    /// a root inside an included file through its site, a plain group
    /// inside the file as `site.group`, and the file's own In (a file
    /// that takes nothing) as the half under the site; each spelling
    /// resolves back to the place it came from.
    #[test]
    fn saved_roots_inside_an_included_file_are_spelled_through_the_site() {
        let mut project = called_program();
        // No wire into the site: the body's In is a root of the whole run.
        project.edges.retain(|e| e.source != "src" && e.source != "s__in");
        let authored = RunSelection::whole(&project);
        let roots: BTreeSet<Located> = authored.roots(&project).into_iter().collect();
        let at = |id: &str| Located::new(id, vec!["s".into()]);
        for root in [at("@f__in"), at("@f.free"), at("@f.g__in")] {
            assert!(roots.contains(&root), "{root} in {roots:?}");
        }
        let mut old = outcomes(&project);
        old.get_mut(&at("@f.free")).unwrap().used_backups.insert("v".into(), json!("kept"));
        let planned = SeedPlan { origins: roots.iter().map(|r| (r.clone(), Color::nil())).collect(), selection: authored.clone(), warnings: vec![] };
        let saved = starting_parameters(&project, &RunSpec::whole("case"), &authored, &planned, &old, &[]).unwrap();
        let spelled: BTreeSet<&str> = saved.from.keys().map(String::as_str).collect();
        assert_eq!(spelled, ["src", "s", "s.__in", "s.free", "s.g", "s.g.x"].into_iter().collect(), "{spelled:?}");
        assert_eq!(saved.from["s.free"]["v"], json!("kept"));
        for (key, place) in [("s.__in", at("@f__in")), ("s.free", at("@f.free")), ("s.g", at("@f.g__in"))] {
            assert_eq!(crate::project::selection::start_node_at(&project, key).unwrap(), place, "{key}");
        }
    }

    const WIRES: &[(&str, &str)] = &[("a", "b"), ("b", "c"), ("c", "l__in"), ("l__in", "body"), ("body", "l__out"), ("l__out", "d")];

    #[test]
    fn saved_whole_run_preserves_original_roots_and_recomputes_their_descendants() {
        for trigger in [false, true] {
            let mut project = program("b", WIRES);
            project.nodes.iter_mut().find(|node| node.id == "a").unwrap().features.is_trigger = trigger;
            let spec = RunSpec::whole("case");
            let authored = RunSelection::whole(&project);
            let mut previous = outcomes(&project);
            if trigger {
                previous.get_mut(&top("a")).unwrap().fire = Some(json!({"event": "original wake"}));
            } else {
                previous.get_mut(&top("a")).unwrap().used_backups.insert("in".into(), json!("original input"));
            }
            let planned = SeedPlan { origins: [(top("a"), Color::nil())].into(), selection: authored.clone(), warnings: Vec::new() };
            let history = vec![ExpectedWire { node: "a".into(), port: "out".into(), value: json!("computed output"), ..Default::default() }];
            let saved = starting_parameters(&project, &spec, &authored, &planned, &previous, &history).unwrap();
            assert!(saved.from.contains_key("x"), "independent original root remains part of the run");
            assert!(!saved.from.contains_key("b"), "computed interior inputs must not be frozen");
            if trigger {
                assert_eq!(saved.fire, Some(("a".into(), json!({"event": "original wake"}))));
                assert!(!saved.from.contains_key("a"));
            } else {
                assert_eq!(saved.from["a"]["in"], json!("original input"));
            }
            let fresh = crate::run_spec::resolve_spec(&saved, &project).unwrap();
            assert_eq!(fresh.selection.nodes, authored.nodes);
            assert!(fresh.selection.nodes.contains(&top("a")) && fresh.selection.nodes.contains(&top("b")) && fresh.selection.nodes.contains(&top("d")));
        }
    }

    #[test]
    fn saved_whole_group_start_keeps_unwired_members() {
        let mut project = program("b", &[("l__in", "body"), ("body", "l__out"), ("l__out", "d")]);
        project.groups[0].kind = GroupKind::Group;
        project.groups[0].node_ids.push("x".into());
        project.nodes.iter_mut().find(|node| node.id == "x").unwrap().scope = vec!["l".into()];
        for node in project.nodes.iter_mut().filter(|node| node.group_boundary.is_some()) {
            node.node_type = "Passthrough".into();
            node.outputs[0].name = "in".into();
        }
        for edge in project.edges.iter_mut().filter(|edge| edge.source == "l__in" || edge.source == "l__out") {
            edge.source_handle = Some("in".into());
        }
        let spec = RunSpec::whole("whole");
        let authored = RunSelection::whole(&project);
        let mut previous = outcomes(&project);
        previous.get_mut(&top("l__in")).unwrap().used_backups.insert("in".into(), json!("saved group input"));
        let planned = SeedPlan {
            origins: [(top("l__in"), Color::nil())].into(),
            selection: authored.clone(),
            warnings: vec![],
        };
        let saved = starting_parameters(&project, &spec, &authored, &planned, &previous, &[]).unwrap();
        assert_eq!(saved.from["l"]["in"], json!("saved group input"));
        assert!(!saved.from.contains_key("l__in"));
        let fresh = crate::run_spec::resolve_spec(&saved, &project).unwrap().selection;
        assert_eq!(fresh.nodes, authored.nodes);
        assert!(fresh.nodes.contains(&top("x")));
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
        let selected = crate::run_spec::resolve_spec(&spec, &project).unwrap().selection;
        let planned = seed_plan(&project, &identity(&project), &selected, &spec, &previous, &[], &[]).unwrap();
        assert!(planned.origins.is_empty());
        assert_eq!(starting_parameters(&project, &spec, &selected, &planned, &previous, &[]).unwrap(), spec);
    }

    fn identity(project: &ProjectDefinition) -> ProgramIdentity {
        ProgramIdentity {
            definition_hash: crate::project::hash::compute_definition_hash(project).unwrap(),
            binary_hash: "worker".into(),
            implementations: project.nodes.iter().map(|node| (node.node_type.clone(), "implementation".into())).collect(),
        }
    }

    fn outcomes(project: &ProjectDefinition) -> BTreeMap<Located, SeedOutcome> {
        identity(project).slice_hashes(project).unwrap().into_iter().map(|(place, slice_hash)| {
            (place, SeedOutcome { origin: Color::nil(), slice_hash, used_backups: BTreeMap::new(), backup_origins: BTreeMap::new(), absent_ports: BTreeSet::new(), boundary_ports: BTreeSet::new(), emitted_live_handle: false, fire: None })
        }).collect()
    }

    #[test]
    fn nested_live_handles_keep_the_producer_and_its_consumers_out_of_reuse() {
        let project = program("b", WIRES);
        let mut previous = outcomes(&project);
        let handle = json!({"record": [{"__weft_bus__":{"id":"old-run-handle","mode":"broadcast"}}]});
        assert!(crate::weft_type::WeftType::contains_bus_handle(&handle));
        assert!(!crate::weft_type::WeftType::contains_bus_handle(&json!({"record":["ordinary", 1, null]})));
        previous.get_mut(&top("a")).unwrap().emitted_live_handle = crate::weft_type::WeftType::contains_bus_handle(&handle);
        let selected = RunSelection::whole(&project);
        let planned = seed_plan(&project, &identity(&project), &selected, &RunSpec::whole("test"), &previous, &[], &[]).unwrap();
        assert!(!planned.origins.contains_key(&top("a")));
        assert!(!planned.origins.contains_key(&top("b")));
        assert!(!planned.origins.contains_key(&top("body")));
        assert!(planned.origins.contains_key(&top("x")));
    }

    #[test]
    fn reuse_keeps_history_before_an_explicit_start_without_widening_the_run() {
        let p = program("b", WIRES);
        let spec = RunSpec { from: BTreeMap::from([("c".into(), BTreeMap::new())]), target: vec!["d".into()], ..RunSpec::whole("test") };
        let selected = RunSelection::carve(&p, &SelectionBounds { from: spec.from.keys().cloned().collect(), target: spec.target.clone(), ..Default::default() }).unwrap();
        let result = seed_plan(&p, &identity(&p), &selected, &spec, &outcomes(&p), &[], &[]).unwrap();
        assert!(result.origins.contains_key(&top("a")));
        assert!(result.origins.contains_key(&top("b")));
        assert!(result.origins.contains_key(&top("c")));
        assert!(!result.origins.contains_key(&top("x")), "unrelated history cannot become part of this use case");
        assert!(result.selection.nodes.is_empty(), "unchanged selected work is reused too");
        assert!(!result.selection.nodes.contains(&top("a")));
    }

    #[test]
    fn a_seed_boundary_before_a_loop_runs_the_whole_loop_and_its_consumers() {
        let p = program("b", WIRES);
        let selected = RunSelection::whole(&p);
        let result = seed_plan(&p, &identity(&p), &selected, &RunSpec::whole("test"), &outcomes(&p), &["c".into(), "x".into()], &[]).unwrap();
        assert_eq!(result.selection.nodes, tops(&["body", "l__in", "l__out", "d"]));
        assert!(seed_plan(&p, &identity(&p), &selected, &RunSpec::whole("test"), &outcomes(&p), &["body".into()], &[]).is_err());
    }

    #[test]
    fn an_explicit_start_reuses_unchanged_inputs_and_reruns_a_changed_used_backup() {
        let p = program("b", WIRES);
        let mut old = outcomes(&p);
        let mut spec = RunSpec::whole("test");
        spec.from.insert("b".into(), BTreeMap::from([("in".into(), Value::String("new".into()))]));
        let selected = crate::run_spec::resolve_spec(&spec, &p).unwrap().selection;
        let result = seed_plan(&p, &identity(&p), &selected, &spec, &old, &[], &[]).unwrap();
        assert!(result.origins.contains_key(&top("b")), "an unused backup does not change the effective input");
        assert!(result.origins.contains_key(&top("a")), "upstream history remains reusable");
        assert!(result.warnings.iter().any(|w| w.starts_with("'b.in' keeps the value run") && w.contains("without --seed")),
            "the unused backup is announced: {:?}", result.warnings);
        old.get_mut(&top("b")).unwrap().used_backups.insert("in".into(), json!("old"));
        let result = seed_plan(&p, &identity(&p), &selected, &spec, &old, &[], &[]).unwrap();
        assert!(!result.origins.contains_key(&top("b")));
        assert!(!result.origins.contains_key(&top("d")));
        assert_eq!(result.selection.input[&top("b")]["in"], json!("new"));
        old.get_mut(&top("b")).unwrap().used_backups.insert("in".into(), json!("new"));
        let result = seed_plan(&p, &identity(&p), &selected, &spec, &old, &[], &[]).unwrap();
        assert!(result.origins.contains_key(&top("b")), "the same used backup is reusable");
        assert!(result.selection.nodes.is_empty());
        assert!(result.warnings.is_empty(), "a backup the seed run itself used is not unused: {:?}", result.warnings);
    }

    fn planned(parent: &ProjectDefinition, child: &ProjectDefinition, until: &[&str], missing: &[&str]) -> SeedPlan {
        let mut old = outcomes(parent);
        for node in missing { old.remove(&top(node)); }
        seed_plan(child, &identity(child), &RunSelection::whole(child), &RunSpec::whole("test"), &old,
            &until.iter().map(|node| (*node).into()).collect::<Vec<_>>(), &[]).unwrap()
    }

    #[test]
    fn unchanged_program_reuses_every_result() {
        let p = program("b", WIRES);
        assert!(planned(&p, &p, &[], &[]).selection.nodes.is_empty());
    }

    #[test]
    fn config_change_recomputes_only_its_dependencies_and_whole_loop() {
        let parent = program("b", WIRES);
        let child = program("changed", WIRES);
        assert_eq!(planned(&parent, &child, &[], &[]).selection.nodes,
            tops(&["b", "c", "l__in", "body", "l__out", "d"]));
    }

    #[test]
    fn new_wire_invalidates_its_consumer_and_dependents() {
        let parent = program("b", WIRES);
        let mut wires = WIRES.to_vec();
        wires.push(("x", "c"));
        let child = program("b", &wires);
        assert_eq!(planned(&parent, &child, &[], &[]).selection.nodes,
            tops(&["c", "l__in", "body", "l__out", "d"]));
    }

    #[test]
    fn new_or_unfinished_nodes_cannot_reuse_older_history() {
        let child = program("b", WIRES);
        let mut parent = child.clone();
        parent.nodes.retain(|node| node.id != "x");
        assert_eq!(planned(&parent, &child, &[], &[]).selection.nodes, tops(&["x"]));
        assert_eq!(planned(&child, &child, &[], &["c"]).selection.nodes,
            tops(&["c", "l__in", "body", "l__out", "d"]));
    }

    #[test]
    fn seed_endpoints_reject_unknown_nodes_and_loop_interiors() {
        let p = program("b", WIRES);
        let selected = RunSelection::carve(&p, &SelectionBounds { target: vec!["b".into()], ..Default::default() }).unwrap();
        for node in ["missing", "body"] {
            let error = seed_plan(&p, &identity(&p), &selected, &RunSpec::whole("test"), &outcomes(&p),
                &[node.into()], &[]).unwrap_err();
            assert!(error.to_string().contains(node));
        }
    }

    #[test]
    fn exclusive_and_inclusive_seed_caps_do_not_widen_the_execution_cut() {
        let p = program("b", WIRES);
        let selection = RunSelection::carve(&p, &SelectionBounds { target: vec!["c".into()], ..Default::default() }).unwrap();
        let old = outcomes(&p);
        let until = seed_plan(&p, &identity(&p), &selection, &RunSpec::whole("test"), &old, &["b".into()], &[]).unwrap();
        assert!(until.origins.contains_key(&top("b")));
        assert_eq!(until.selection.nodes, tops(&["c"]));
        let before = seed_plan(&p, &identity(&p), &selection, &RunSpec::whole("test"), &old, &[], &["b".into()]).unwrap();
        assert!(!before.origins.contains_key(&top("b")));
        assert_eq!(before.selection.nodes, tops(&["b", "c"]));
    }

    #[test]
    fn a_changed_implementation_invalidates_the_production_slice() {
        let p = program("b", WIRES);
        let mut edited = identity(&p);
        edited.implementations.insert("T".into(), "new code".into());
        let result = seed_plan(&p, &edited, &RunSelection::whole(&p), &RunSpec::whole("test"), &outcomes(&p), &[], &[]).unwrap();
        assert_eq!(result.selection.nodes.len(), p.nodes.len());
        assert!(!result.warnings.is_empty());
    }

    #[test]
    fn used_ancestor_backup_is_copied_only_to_an_explicit_start() {
        let p = program("b", WIRES);
        let mut old = outcomes(&p);
        old.get_mut(&top("b")).unwrap().used_backups.insert("in".into(), json!("old"));
        old.get_mut(&top("c")).unwrap().used_backups.insert("in".into(), json!("interior"));
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), ..RunSpec::whole("test") };
        let selection = crate::run_spec::resolve_spec(&spec, &p).unwrap().selection;
        let result = seed_plan(&p, &identity(&p), &selection, &spec, &old, &[], &[]).unwrap();
        assert_eq!(result.selection.input[&top("b")]["in"], json!("old"));
        assert_eq!(result.selection.input_origins[&top("b")]["in"], old[&top("b")].origin);
        assert!(!result.selection.input.contains_key(&top("c")), "seed does not invent an interior starting point");
        assert!(!result.selection.input.contains_key(&top("a")) || result.selection.input[&top("a")].is_empty());
    }

    #[test]
    fn expanding_before_an_old_start_recomputes_its_backup_dependent_result() {
        let project = program("b", WIRES);
        let mut old = outcomes(&project);
        old.get_mut(&top("b")).unwrap().used_backups.insert("in".into(), json!("old manual input"));
        let spec = RunSpec::whole("expanded");
        let selection = RunSelection::whole(&project);
        let planned = seed_plan(&project, &identity(&project), &selection, &spec, &old, &[], &[]).unwrap();
        assert!(planned.origins.contains_key(&top("a")));
        assert!(planned.origins.contains_key(&top("x")));
        assert!(!planned.origins.contains_key(&top("b")));
        assert!(!planned.origins.contains_key(&top("d")));
        assert!(!planned.selection.input.contains_key(&top("b")));
        assert_eq!(starting_parameters(&project, &spec, &selection, &planned, &old, &[]).unwrap(), spec);
    }

    #[test]
    fn an_unwired_loop_member_is_reused_or_rerun_with_its_whole_loop() {
        let mut p = program("b", WIRES);
        let mut lonely = p.nodes.iter().find(|node| node.id == "body").unwrap().clone();
        lonely.id = "lonely".into();
        p.nodes.push(lonely);
        assert!(planned(&p, &p, &[], &[]).selection.nodes.is_empty());
        let rerun = planned(&p, &p, &["c", "x"], &[]);
        for node in ["lonely", "body", "l__in", "l__out", "d"] { assert!(rerun.selection.nodes.contains(&top(node))); }
    }

    #[test]
    fn a_loop_cannot_mix_results_from_different_origins() {
        let p = program("b", WIRES);
        let mut old = outcomes(&p);
        old.get_mut(&top("body")).unwrap().origin = Color::new_v4();
        let result = seed_plan(&p, &identity(&p), &RunSelection::whole(&p), &RunSpec::whole("test"), &old, &[], &[]).unwrap();
        for node in ["body", "l__in", "l__out", "d"] { assert!(result.selection.nodes.contains(&top(node))); }
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
            let selection = crate::run_spec::resolve_spec(&spec, &p).unwrap().selection;
            let result = seed_plan(&p, &identity(&p), &selection, &spec, &old, &[], &[]).unwrap();
            assert!(!result.origins.contains_key(&top("a")));
            assert!(!result.origins.contains_key(&top("b")));
            assert!(!result.origins.contains_key(&top("d")));
        }
    }
}
