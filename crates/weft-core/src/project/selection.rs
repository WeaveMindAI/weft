//! The immutable portion of a program admitted to one execution.
//!
//! Ordinary boundaries forward a port, not every branch of their group.
//! Loops remain indivisible. Control dependencies are admitted separately
//! from data traversal, so evaluating a group gate cannot admit siblings.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{boundary_in_id, boundary_out_id, Edge, GroupKind, ProjectDefinition};

// SYNC: RunSelection <-> packages/weft-graph/src/run-spec.ts RunSelection
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSelection {
    pub nodes: BTreeSet<String>,
    pub edges: BTreeSet<String>,
    /// Boundary input ports whose data participates in this execution.
    pub boundary_ports: BTreeMap<String, BTreeSet<String>>,
    /// Enclosing group ids, including those gating simulated sources.
    pub gates: BTreeSet<String>,
    /// Sources replaced by authored emissions or retained seed history.
    pub suppliers: BTreeSet<String>,
    /// Authored backups for selected receiving ports, recorded at birth.
    pub input: crate::run_spec::PortValues,
    /// Origins only for inherited backups. Authored backups belong to this run.
    pub input_origins: BTreeMap<String, BTreeMap<String, crate::Color>>,
}

#[derive(Debug, Clone, Default)]
pub struct SelectionBounds {
    pub from: Vec<String>,
    pub emit: Vec<String>,
    pub target: Vec<String>,
    pub before: Vec<String>,
    pub group: Option<String>,
    pub fire: Option<String>,
}

#[derive(Clone, Copy)]
enum Direction {
    Upstream,
    Downstream,
}

impl RunSelection {
    /// Ordinary boundaries expose only the ports on selected paths.
    pub fn includes_port(&self, node: &super::NodeDefinition, port: &str) -> bool {
        node.node_type != "Passthrough"
            || self.boundary_ports.get(&node.id).is_some_and(|ports| ports.contains(port))
    }

    pub fn dispatchable_nodes(&self) -> std::collections::HashSet<String> {
        self.nodes.iter().cloned().collect()
    }

    pub fn downstream_nodes(project: &ProjectDefinition, starts: &[String]) -> BTreeSet<String> {
        let starts: Vec<_> = starts.iter().flat_map(|id| {
            if project.groups.iter().any(|group| &group.id == id) {
                group_nodes(project, id).into_iter().collect()
            } else { vec![id.clone()] }
        }).collect();
        walk(project, &starts, Direction::Downstream, &BTreeSet::new())
    }

    /// Rebuild a selected node set's data paths and controls, preserving loops.
    pub fn restricted(project: &ProjectDefinition, nodes: BTreeSet<String>) -> Result<Self, String> {
        for id in &nodes {
            if !project.nodes.iter().any(|node| &node.id == id) {
                return Err(format!("unknown node '{id}'"));
            }
        }
        let selection = Self::from_nodes(project, nodes, BTreeSet::new());
        selection.validate_loops(project)?;
        Ok(selection)
    }

    pub fn whole(project: &ProjectDefinition) -> Self {
        Self::from_nodes(project, project.nodes.iter().map(|n| n.id.clone()).collect(), BTreeSet::new())
    }

    /// History relevant to this cut, including ancestors before its starts.
    /// An unrelated branch is not inherited merely because the seed retained it.
    pub fn history_nodes(&self, project: &ProjectDefinition) -> BTreeSet<String> {
        let starts = self.nodes.iter().chain(&self.suppliers).flat_map(|id| {
            if is_ordinary_boundary(project, id) {
                self.boundary_ports.get(id).into_iter().flatten()
                    .map(|port| (id.clone(), Some(port.clone()))).collect::<Vec<_>>()
            } else { vec![(id.clone(), None)] }
        }).collect();
        walk_ports(project, starts, Direction::Upstream, &BTreeSet::new())
    }

    /// Keep the authored cut while replacing reusable results with their
    /// frontier supply. History that feeds no new work adds no control work.
    pub fn with_reused(&self, project: &ProjectDefinition, reused: &BTreeSet<String>) -> Self {
        let nodes: BTreeSet<_> = self.nodes.difference(reused).cloned().collect();
        let mut suppliers = self.suppliers.clone();
        suppliers.extend(project.edges.iter().filter(|edge| self.edges.contains(&edge.id)
            && nodes.contains(&edge.target) && reused.contains(&edge.source)).map(|edge| edge.source.clone()));
        for node in project.nodes.iter().filter(|node| nodes.contains(&node.id) || self.suppliers.contains(&node.id)) {
            suppliers.extend(node.scope.iter().map(|group| boundary_in_id(group)).filter(|gate| reused.contains(gate)));
        }
        let mut selected = Self::from_nodes(project, nodes, suppliers);
        selected.nodes.retain(|id| self.nodes.contains(id) && !reused.contains(id));
        selected.edges.retain(|id| self.edges.contains(id)
            && project.edges.iter().any(|edge| &edge.id == id && selected.nodes.contains(&edge.target)));
        selected.input = self.input.clone();
        selected.input_origins = self.input_origins.clone();
        selected
    }

    /// Manual carving and trigger fire use identical walks and bounds.
    pub fn carve(project: &ProjectDefinition, bounds: &SelectionBounds) -> Result<Self, String> {
        let mut entries = BTreeSet::new();
        for id in &bounds.from {
            if !entries.insert(start_node(project, id)?) {
                return Err(format!("starting entry '{id}' is supplied more than once"));
            }
        }
        let mut target = Vec::new();
        let mut before = Vec::new();
        let mut excluded: BTreeSet<_> = bounds.emit.iter().cloned().collect();
        for (inclusive, ends) in [(true, &bounds.target), (false, &bounds.before)] {
            for id in ends {
                let endpoints = if project.groups.iter().any(|group| group.id == *id) {
                    if let Some(parent) = enclosing_loop_of_group(project, id)? {
                        return Err(format!("cannot cut at '{id}' inside loop '{parent}'; select the whole loop"));
                    }
                    group_nodes(project, id)
                } else {
                    validate_endpoint(project, id)?;
                    BTreeSet::from([id.clone()])
                };
                if inclusive { target.extend(endpoints); } else {
                    excluded.extend(endpoints.iter().cloned());
                    before.extend(endpoints);
                }
            }
        }
        let ids = bounds.emit.iter().chain(bounds.fire.iter());
        for id in ids {
            validate_endpoint(project, id)?;
        }
        for id in &bounds.emit {
            if let Some(group) = project.groups.iter().find(|group| boundary_in_id(&group.id) == *id) {
                return Err(format!("cannot supply outputs of group entry '{id}'; use --from {} with its input payload", group.id));
            }
        }
        if let Some(id) = bounds.emit.iter().find(|id| bounds.from.contains(id) || bounds.fire.as_ref() == Some(id)) {
            return Err(format!("'{id}' cannot both run and have its outputs supplied"));
        }
        if let Some(fire) = &bounds.fire {
            if !project.nodes.iter().any(|n| n.id == *fire && n.features.is_trigger) {
                return Err(format!("'{fire}' is not a trigger"));
            }
        }
        let mut selection = if let Some(group) = &bounds.group {
            if !bounds.from.is_empty() || !bounds.emit.is_empty() || !bounds.target.is_empty()
                || !bounds.before.is_empty()
            {
                return Err("group cannot be combined with from, emit, target, or before".into());
            }
            if !project.groups.iter().any(|g| &g.id == group) {
                return Err(format!("unknown group '{group}'"));
            }
            let nodes = group_nodes(project, group);
            if bounds.fire.as_ref().is_some_and(|id| !nodes.contains(id)) {
                return Err("the fired trigger is outside the selected group".into());
            }
            Self::from_nodes(project, nodes, BTreeSet::new())
        } else {
            let mut stops: BTreeSet<_> = project.nodes.iter().filter(|n| n.features.is_trigger)
                .map(|n| n.id.clone()).collect();
            stops.extend(entries);
            stops.extend(bounds.emit.iter().cloned());
            let starts: Vec<_> = bounds.from.iter().chain(&bounds.emit).chain(bounds.fire.iter())
                .cloned().collect();
            let mut nodes = if starts.is_empty() {
                project.nodes.iter().map(|n| n.id.clone()).collect()
            } else {
                let downstream = Self::downstream_nodes(project, &starts);
                let consumers: Vec<_> = downstream.iter().filter(|id| !is_ordinary_boundary(project, id)).cloned().collect();
                let mut dependencies = walk(project, &consumers, Direction::Upstream, &stops);
                dependencies.extend(downstream);
                dependencies
            };
            let mut suppliers: BTreeSet<_> = bounds.emit.iter().cloned().collect();
            for ends in [&target, &before] {
                if !ends.is_empty() {
                    let allowed = walk(project, ends, Direction::Upstream, &stops);
                    nodes.retain(|id| allowed.contains(id));
                    suppliers.retain(|id| allowed.contains(id));
                }
            }
            suppliers.retain(|id| !before.contains(id));
            nodes.retain(|id| !excluded.contains(id));
            let selection = Self::from_nodes(project, nodes, suppliers);
            // Structural completion cannot restore an explicitly excluded endpoint.
            if selection.nodes.iter().any(|id| excluded.contains(id)) {
                return Err("the requested cut removes group control machinery needed by the run".into());
            }
            selection
        };
        if bounds.fire.is_some() {
            // A fired trigger reads its baked inputs. A shared setup producer
            // may run for another consumer without feeding the trigger again.
            // Unfired triggers close their outputs without reading setup inputs.
            for edge in project.edges.iter().filter(|edge| project.nodes.iter()
                .any(|node| node.id == edge.target && node.features.is_trigger)) {
                selection.edges.remove(&edge.id);
            }
        }
        selection.validate_loops(project)?;
        Ok(selection)
    }

    /// Data dependencies and enclosing controls, with whole-loop expansion.
    pub fn dependencies(project: &ProjectDefinition, targets: &[String]) -> Self {
        let nodes = walk(project, targets, Direction::Upstream, &BTreeSet::new());
        Self::from_nodes(project, nodes, BTreeSet::new())
    }

    /// Both setup phases stop inclusively at their targets.
    pub fn setup(project: &ProjectDefinition, targets: &[String]) -> Result<Self, String> {
        for target in targets {
            validate_endpoint(project, target)?;
        }
        let selection = Self::dependencies(project, targets);
        selection.validate_loops(project)?;
        Ok(selection)
    }

    pub fn contains_edge(&self, edge: &Edge) -> bool {
        self.edges.contains(&edge.id)
    }

    pub fn has_supplier(&self, project: &ProjectDefinition, node: &str, port: &str) -> bool {
        project.edges.iter().any(|edge| self.edges.contains(&edge.id)
            && edge.target == node && edge.target_handle.as_deref().unwrap_or("default") == port
            && (self.nodes.contains(&edge.source) || self.suppliers.contains(&edge.source)))
    }

    /// Roots are relative to selected wires. Enclosing gates still control
    /// when these intents can dispatch, including an explicitly fired trigger.
    pub fn roots(&self, project: &ProjectDefinition) -> Vec<String> {
        project.nodes.iter().filter(|node| self.nodes.contains(&node.id))
            .filter(|node| !project.edges.iter().any(|edge| edge.target == node.id
                && self.edges.contains(&edge.id)
                && (self.nodes.contains(&edge.source) || self.suppliers.contains(&edge.source))))
            .map(|node| node.id.clone()).collect()
    }

    pub fn validate_loops(&self, project: &ProjectDefinition) -> Result<(), String> {
        for group in project.groups.iter().filter(|g| matches!(g.kind, GroupKind::Loop { .. })) {
            let members = group_nodes(project, &group.id);
            if members.iter().any(|id| self.nodes.contains(id))
                && !members.is_subset(&self.nodes)
            {
                return Err(format!("cannot cut inside loop '{}'; select the whole loop", group.id));
            }
        }
        Ok(())
    }

    fn from_nodes(project: &ProjectDefinition, mut nodes: BTreeSet<String>, suppliers: BTreeSet<String>) -> Self {
        let mut gates = BTreeSet::new();
        loop {
            let before = nodes.len();
            for node in &project.nodes {
                if nodes.contains(&node.id) || suppliers.contains(&node.id) {
                    gates.extend(node.scope.iter().cloned());
                    if is_ordinary_boundary(project, &node.id) {
                        gates.extend(node.group_boundary.iter().map(|boundary| boundary.group_id.clone()));
                    }
                }
            }
            for gate in &gates {
                let boundary = boundary_in_id(gate);
                if suppliers.contains(&boundary) { continue; }
                nodes.insert(boundary.clone());
                let sources: Vec<_> = project.edges.iter()
                    .filter(|edge| edge.target == boundary && edge.target_handle.as_deref() == Some("_should_flow"))
                    .map(|edge| (edge.source.clone(), Some(edge.source_handle.as_deref().unwrap_or("default").into()))).collect();
                nodes.extend(walk_ports(project, sources, Direction::Upstream, &project.nodes.iter()
                    .filter(|n| n.features.is_trigger).map(|n| n.id.clone()).collect()));
            }
            if nodes.len() == before { break; }
        }
        let mut edges = BTreeSet::new();
        let mut boundary_ports: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        // A boundary port is used when a selected consumer reads it. Walk
        // backward through boundary chains without admitting other ports.
        let mut pending: Vec<_> = project.edges.iter().filter(|edge| nodes.contains(&edge.target)
            && !is_ordinary_boundary(project, &edge.target)).collect();
        for node in project.nodes.iter().filter(|node| nodes.contains(&node.id) && is_ordinary_boundary(project, &node.id)) {
            let terminal = !project.edges.iter().any(|edge| edge.source == node.id && nodes.contains(&edge.target));
            if terminal {
                for edge in project.edges.iter().filter(|edge| edge.target == node.id
                    && (nodes.contains(&edge.source) || suppliers.contains(&edge.source)))
                {
                    boundary_ports.entry(node.id.clone()).or_default().insert(edge.target_handle.as_deref().unwrap_or("default").into());
                    pending.push(edge);
                }
                boundary_ports.entry(node.id.clone()).or_default().extend(node.port_literals.keys().cloned());
            }
        }
        for gate in &gates {
            let boundary = boundary_in_id(gate);
            boundary_ports.entry(boundary.clone()).or_default().insert("_should_flow".into());
            pending.extend(project.edges.iter().filter(|edge| edge.target == boundary
                && edge.target_handle.as_deref() == Some("_should_flow")));
        }
        while let Some(edge) = pending.pop() {
            if !edges.insert(edge.id.clone()) { continue; }
            if is_ordinary_boundary(project, &edge.source) && nodes.contains(&edge.source) {
                let port = edge.source_handle.as_deref().unwrap_or("default");
                boundary_ports.entry(edge.source.clone()).or_default().insert(port.into());
                pending.extend(project.edges.iter().filter(|incoming| incoming.target == edge.source
                    && incoming.target_handle.as_deref().unwrap_or("default") == port));
            }
        }
        Self { nodes, edges, boundary_ports, gates, suppliers, input: BTreeMap::new(), input_origins: BTreeMap::new() }
    }
}

/// A logical group start retains its entire body and normal entry machinery.
pub fn start_node(project: &ProjectDefinition, id: &str) -> Result<String, String> {
    if project.groups.iter().any(|group| group.id == id) {
        if let Some(parent) = enclosing_loop_of_group(project, id)? {
            return Err(format!("cannot start '{id}' inside loop '{parent}'; select the whole loop"));
        }
        return Ok(boundary_in_id(id));
    }
    validate_endpoint(project, id)?;
    Ok(id.into())
}

/// The loop a group sits inside, if any: a group is cut or started whole,
/// and a loop's body cannot be cut at all, so such a group is off limits
/// as an endpoint. Errors when the group has no entry node.
fn enclosing_loop_of_group(project: &ProjectDefinition, group: &str) -> Result<Option<String>, String> {
    let entry = boundary_in_id(group);
    let node = project.nodes.iter().find(|node| node.id == entry)
        .ok_or_else(|| format!("group '{group}' has no entry"))?;
    Ok(project.groups.iter()
        .find(|candidate| matches!(candidate.kind, GroupKind::Loop { .. }) && node.scope.contains(&candidate.id))
        .map(|candidate| candidate.id.clone()))
}

pub fn validate_endpoint(project: &ProjectDefinition, id: &str) -> Result<(), String> {
    let node = project.nodes.iter().find(|node| node.id == id)
        .ok_or_else(|| format!("unknown node '{id}'"))?;
    for group in project.groups.iter().filter(|g| matches!(g.kind, GroupKind::Loop { .. })) {
        if node.scope.contains(&group.id)
            || node.group_boundary.as_ref().is_some_and(|b| b.group_id == group.id)
        {
            return Err(format!("cannot cut at '{id}' inside loop '{}'; select the whole loop", group.id));
        }
    }
    Ok(())
}

fn group_nodes(project: &ProjectDefinition, group: &str) -> BTreeSet<String> {
    project.nodes.iter().filter(|n| n.scope.iter().any(|g| g == group)
        || n.id == boundary_in_id(group) || n.id == boundary_out_id(group))
        .map(|n| n.id.clone()).collect()
}

pub(crate) fn is_ordinary_boundary(project: &ProjectDefinition, id: &str) -> bool {
    project.nodes.iter().find(|n| n.id == id).and_then(|n| n.group_boundary.as_ref())
        .is_some_and(|b| project.groups.iter().any(|g| g.id == b.group_id && matches!(g.kind, GroupKind::Group)))
}

fn walk(project: &ProjectDefinition, starts: &[String], direction: Direction, stops: &BTreeSet<String>) -> BTreeSet<String> {
    walk_ports(project, starts.iter().map(|id| (id.clone(), None)).collect(), direction, stops)
}

fn walk_ports(project: &ProjectDefinition, mut pending: Vec<(String, Option<String>)>, direction: Direction, stops: &BTreeSet<String>) -> BTreeSet<String> {
    let mut nodes = BTreeSet::new();
    let mut visited = BTreeSet::new();
    while let Some((id, port)) = pending.pop() {
        if !visited.insert((id.clone(), port.clone())) { continue; }
        nodes.insert(id.clone());
        if stops.contains(&id) { continue; }
        if let Some(group) = project.groups.iter().find(|g| matches!(g.kind, GroupKind::Loop { .. })
            && group_nodes(project, &g.id).contains(&id))
        {
            for member in group_nodes(project, &group.id) {
                if !nodes.contains(&member) { pending.push((member, None)); }
            }
        }
        for edge in &project.edges {
            let (here, here_port, next, next_port) = match direction {
                Direction::Upstream => (&edge.target, &edge.target_handle, &edge.source, &edge.source_handle),
                Direction::Downstream => (&edge.source, &edge.source_handle, &edge.target, &edge.target_handle),
            };
            if here != &id { continue; }
            if is_ordinary_boundary(project, &id) && port.as_ref().is_some_and(|p|
                here_port.as_deref().unwrap_or("default") != p)
            { continue; }
            pending.push((next.clone(), if is_ordinary_boundary(project, next) {
                Some(next_port.as_deref().unwrap_or("default").into())
            } else { None }));
        }
    }
    nodes
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn program() -> ProjectDefinition {
        let node = |id: &str, scope: &[&str], boundary: Value| json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {"isTrigger": id == "trigger"}, "scope": scope,
            "groupBoundary": boundary, "requiresInfra": false
        });
        let edge = |source: &str, sp: &str, target: &str, tp: &str| json!({
            "id": format!("{source}.{sp}->{target}.{tp}"), "source": source, "target": target,
            "sourceHandle": sp, "targetHandle": tp
        });
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("a", &[], Value::Null), node("unrelated", &[], Value::Null),
                node("gate", &[], Value::Null),
                node("g__in", &[], json!({"groupId": "g", "role": "In"})),
                node("b", &["g"], Value::Null), node("c", &["g"], Value::Null),
                node("sibling", &["g"], Value::Null), node("trigger", &["g"], Value::Null),
                node("g__out", &[], json!({"groupId": "g", "role": "Out"})),
                node("after", &[], Value::Null)
            ],
            "edges": [
                edge("a", "out", "g__in", "x"), edge("unrelated", "out", "g__in", "y"),
                edge("gate", "out", "g__in", "_should_flow"),
                edge("g__in", "x", "b", "in"), edge("g__in", "y", "sibling", "in"),
                edge("b", "out", "c", "in"), edge("trigger", "out", "c", "event"),
                edge("c", "out", "g__out", "result"), edge("g__out", "result", "after", "in")
            ],
            "groups": [{"id": "g", "kind": "group", "nodeIds": ["b", "c", "sibling", "trigger"]}]
        })).unwrap()
    }

    fn names(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|v| (*v).into()).collect()
    }

    #[test]
    fn manual_starts_pull_side_dependencies_but_stop_at_every_named_start() {
        let mut project = program();
        project.edges.push(serde_json::from_value(json!({"id":"side", "source":"unrelated", "sourceHandle":"out", "target":"c", "targetHandle":"extra"})).unwrap());
        for emitting in [false, true] {
            let bounds = SelectionBounds {
                from: if emitting { vec![] } else { vec!["b".into()] },
                emit: if emitting { vec!["b".into()] } else { vec![] },
                target: vec!["after".into()], ..Default::default()
            };
            let selected = RunSelection::carve(&project, &bounds).unwrap();
            for id in ["c", "after", "unrelated", "trigger", "gate"] { assert!(selected.nodes.contains(id), "{id}"); }
            for id in ["a", "sibling"] { assert!(!selected.nodes.contains(id), "{id}"); }
            assert_eq!(selected.nodes.contains("b"), !emitting);
        }
        let selected = RunSelection::carve(&project, &SelectionBounds { from: vec!["c".into()], ..Default::default() }).unwrap();
        assert!(!selected.nodes.contains("unrelated"), "c itself is the boundary; do not recover its input producers");
        assert!(!selected.nodes.contains("b"));
    }

    #[test]
    fn public_group_and_loop_endpoints_include_or_exclude_the_whole_container() {
        for looping in [false, true] {
            let mut project = program();
            if looping { project.groups[0].kind = GroupKind::Loop { loop_config: json!({}) }; }
            let through = RunSelection::carve(&project, &SelectionBounds { target: vec!["g".into()], ..Default::default() }).unwrap();
            assert!(group_nodes(&project, "g").is_subset(&through.nodes));
            assert!(!through.nodes.contains("after"));
            let before = RunSelection::carve(&project, &SelectionBounds { before: vec!["g".into()], ..Default::default() }).unwrap();
            assert!(group_nodes(&project, "g").is_disjoint(&before.nodes));
            assert_eq!(before.nodes, names(&["a", "unrelated", "gate"]));
        }
    }

    #[test]
    fn setup_stops_inside_group_and_follows_only_used_boundary_port() {
        let project = program();
        let selection = RunSelection::setup(&project, &["b".into()]).unwrap();
        assert_eq!(selection.nodes, names(&["a", "b", "g__in", "gate"]));
        assert_eq!(selection.boundary_ports["g__in"], names(&["x", "_should_flow"]));
        assert_eq!(selection.edges, names(&["a.out->g__in.x", "g__in.x->b.in", "gate.out->g__in._should_flow"]));
    }

    #[test]
    fn mixed_bounds_do_not_restore_exclusive_endpoint() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            target: vec!["c".into()], before: vec!["b".into()], ..Default::default()
        }).unwrap();
        assert!(!selection.nodes.contains("b"));
        assert!(!selection.nodes.contains("c"));
        assert!(!selection.nodes.contains("sibling"));
        assert!(selection.nodes.contains("a"));
    }

    #[test]
    fn a_terminal_boundary_keeps_its_selected_input_paths() {
        let project = program();
        let selection = RunSelection::carve(&project, &SelectionBounds { target: vec!["g__out".into()], ..Default::default() }).unwrap();
        assert_eq!(selection.boundary_ports["g__out"], names(&["result"]));
        assert!(selection.edges.contains("c.out->g__out.result"));
        assert!(!selection.nodes.contains("after"));
        let before = RunSelection::carve(&project, &SelectionBounds { before: vec!["b".into()], ..Default::default() }).unwrap();
        assert_eq!(before.boundary_ports["g__in"], names(&["x", "_should_flow"]));
        assert!(!before.edges.contains("g__in.x->b.in"));
        assert!(!before.edges.contains("unrelated.out->g__in.y"));
    }

    #[test]
    fn fire_does_not_admit_unrelated_group_branch() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            fire: Some("trigger".into()), ..Default::default()
        }).unwrap();
        assert_eq!(selection.nodes, names(&["a", "b", "c", "trigger", "g__in", "g__out", "gate", "after"]));
        assert!(!selection.edges.contains("unrelated.out->g__in.y"));
    }

    #[test]
    fn grouped_fire_removes_setup_wires_and_keeps_the_group_gate() {
        let mut project = program();
        let mut setup_edge = project.edges[0].clone();
        setup_edge.id = "setup-trigger".into();
        setup_edge.source = "b".into();
        setup_edge.target = "trigger".into();
        project.edges.push(setup_edge);
        let selection = RunSelection::carve(&project, &SelectionBounds {
            group: Some("g".into()), fire: Some("trigger".into()), ..Default::default()
        }).unwrap();
        assert!(!selection.edges.contains("setup-trigger"));
        assert!(selection.nodes.contains("g__in"));
        assert!(selection.nodes.contains("trigger"));
    }

    #[test]
    fn a_group_entry_cannot_be_replaced_by_emissions() {
        let error = RunSelection::carve(&program(), &SelectionBounds {
            emit: vec!["g__in".into()], ..Default::default()
        }).unwrap_err();
        assert!(error.contains("use --from g"));
    }

    #[test]
    fn fire_runs_shared_producer_without_reopening_trigger_setup_wire() {
        let mut project = program();
        project.edges.push(serde_json::from_value(json!({
            "id": "setup", "source": "b", "sourceHandle": "out",
            "target": "trigger", "targetHandle": "setting"
        })).unwrap());
        let setup = RunSelection::setup(&project, &["trigger".into()]).unwrap();
        assert!(setup.nodes.contains("b"));
        assert!(setup.edges.contains("setup"));
        assert!(!setup.nodes.contains("c"));
        for from in [vec![], vec!["b".into()]] {
            let fire = RunSelection::carve(&project, &SelectionBounds {
                fire: Some("trigger".into()), from, ..Default::default()
            }).unwrap();
            assert!(fire.nodes.contains("b"));
            assert!(fire.nodes.contains("c"));
            assert!(fire.edges.contains("b.out->c.in"));
            assert!(fire.edges.contains("trigger.out->c.event"));
            assert!(!fire.edges.contains("setup"));
        }
    }

    #[test]
    fn simulated_source_keeps_enclosing_gate_without_running_its_body() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            emit: vec!["c".into()], ..Default::default()
        }).unwrap();
        assert_eq!(selection.nodes, names(&["g__in", "g__out", "gate", "after"]));
        assert_eq!(selection.suppliers, names(&["c"]));
        assert_eq!(selection.gates, names(&["g"]));
        assert!(selection.has_supplier(&program(), "g__out", "result"));
    }

    #[test]
    fn empty_cut_stays_empty() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            before: vec!["a".into()], ..Default::default()
        }).unwrap();
        assert!(selection.nodes.is_empty());
        assert!(selection.roots(&program()).is_empty());
    }

    #[test]
    fn reusing_everything_leaves_no_control_work_or_internal_wires() {
        let project = program();
        let selection = RunSelection::whole(&project);
        let reused = selection.with_reused(&project, &selection.nodes);
        assert!(reused.nodes.is_empty());
        assert!(reused.edges.is_empty());
        assert!(reused.gates.is_empty());
        assert!(reused.suppliers.is_empty());
    }

    #[test]
    fn reused_group_gate_is_history_and_only_the_running_consumers_get_wires() {
        let project = program();
        let selection = RunSelection::setup(&project, &["c".into()]).unwrap();
        let reused = selection.with_reused(&project, &names(&["a", "b", "g__in", "gate", "trigger"]));
        assert_eq!(reused.nodes, names(&["c"]));
        assert_eq!(reused.edges, names(&["b.out->c.in", "trigger.out->c.event"]));
        assert!(reused.suppliers.contains("g__in"), "the inherited gate controls c");
        assert!(!reused.nodes.contains("gate"));
    }

    #[test]
    fn loop_endpoints_rejected_but_group_selection_is_whole() {
        let mut project = program();
        project.groups[0].kind = GroupKind::Loop { loop_config: json!({}) };
        for id in ["b", "c", "g__in", "g__out"] {
            assert!(validate_endpoint(&project, id).unwrap_err().contains("inside loop"));
            assert!(RunSelection::setup(&project, &[id.into()]).is_err());
        }
        let selection = RunSelection::carve(&project, &SelectionBounds {
            group: Some("g".into()), ..Default::default()
        }).unwrap();
        assert!(group_nodes(&project, "g").is_subset(&selection.nodes));
        let selection = RunSelection::carve(&project, &SelectionBounds {
            target: vec!["after".into()], ..Default::default()
        }).unwrap();
        assert!(group_nodes(&project, "g").is_subset(&selection.nodes));
    }

    #[test]
    fn group_and_loop_starts_accept_entry_values_and_have_distinct_ends() {
        for kind in [GroupKind::Group, GroupKind::Loop { loop_config: json!({}) }] {
            let mut project = program();
            project.groups[0].kind = kind;
            project.nodes.iter_mut().find(|node| node.id == "g__in").unwrap().inputs = serde_json::from_value(json!([
                {"name":"x", "portType":"String", "required":true}
            ])).unwrap();
            let from: crate::run_spec::RunSpec = serde_json::from_value(json!({"name":"case", "from":{"g":{"x":"backup"}}})).unwrap();
            let group: crate::run_spec::RunSpec = serde_json::from_value(json!({"name":"case", "group":["g",{"x":"backup"}]})).unwrap();
            let start = crate::run_spec::resolve_spec(&from, &project).unwrap().selection;
            let confined = crate::run_spec::resolve_spec(&group, &project).unwrap().selection;
            assert_eq!(start.input["g__in"]["x"], json!("backup"));
            assert_eq!(confined.input, start.input);
            assert!(start.nodes.contains("after"));
            assert!(!confined.nodes.contains("after"));
            assert!(group_nodes(&project, "g").is_subset(&start.nodes));
            assert!(group_nodes(&project, "g").is_subset(&confined.nodes));
            assert!(!start.nodes.contains("a"));
            let both = crate::run_spec::RunSpec { group: group.group, ..from };
            assert!(crate::run_spec::resolve_spec(&both, &project).unwrap_err().to_string().contains("cannot be combined"));
        }
    }

    #[test]
    fn selection_round_trip_retains_empty_set_and_supplier_facts() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            emit: vec!["c".into()], ..Default::default()
        }).unwrap();
        assert_eq!(serde_json::from_value::<RunSelection>(serde_json::to_value(&selection).unwrap()).unwrap(), selection);
        assert_eq!(serde_json::from_value::<RunSelection>(serde_json::to_value(RunSelection::default()).unwrap()).unwrap(), RunSelection::default());
    }
}
