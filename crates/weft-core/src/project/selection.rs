//! The immutable portion of a program admitted to one execution.
//!
//! Ordinary boundaries forward a port, not every branch of their group.
//! Loops remain indivisible. Control dependencies are admitted separately
//! from data traversal, so evaluating a group gate cannot admit siblings.
//!
//! Every node here is a PLACE ([`Located`]): its id and the call path
//! it runs under. An included file is compiled once and reached through
//! every site that includes it, so one id is as many places as there
//! are calls of it in the run, and a cut spelled through a site
//! (`--from one.strip --target two.strip`) starts the body at one place
//! and ends it at another. Wires are places too: the wire from `strip`
//! to `loud` is in the run under `one` and not under `two`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::frames::Located;
use super::{boundary_in_id, boundary_out_id, Edge, GroupBoundaryRole, GroupKind, NodeDefinition, ProjectDefinition};

// SYNC: RunSelection <-> packages/weft-graph/src/run-spec.ts RunSelection
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSelection {
    pub nodes: BTreeSet<Located>,
    /// Wires, each at the place of its deeper end: a wire into a body
    /// from a site's In sits inside the call, so does the wire out of a
    /// body into its site's Out.
    pub edges: BTreeSet<Located>,
    /// Boundary input ports whose data participates in this execution.
    pub boundary_ports: BTreeMap<Located, BTreeSet<String>>,
    /// Enclosing groups, each at the place its In runs: a site at its
    /// caller's place, a body at the place inside the call.
    pub gates: BTreeSet<Located>,
    /// Sources replaced by authored emissions or retained seed history.
    pub suppliers: BTreeSet<Located>,
    /// Authored backups for selected receiving ports, recorded at birth.
    pub input: BTreeMap<Located, BTreeMap<String, serde_json::Value>>,
    /// Origins only for inherited backups. Authored backups belong to this run.
    pub input_origins: BTreeMap<Located, BTreeMap<String, crate::Color>>,
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

#[derive(Clone, Copy, PartialEq)]
enum Direction {
    Upstream,
    Downstream,
}

/// How a wire relates to the call sites: it crosses into a body (from a
/// site's In to the body's In), out of one (from the body's Out to a
/// site's Out), or stays at one place.
enum Crossing<'a> {
    Into(&'a str),
    OutOf(&'a str),
    None,
}

fn crossing<'a>(project: &'a ProjectDefinition, edge: &Edge) -> Crossing<'a> {
    let boundary = |id: &str| project.nodes.iter().find(|n| n.id == id).and_then(|n| n.group_boundary.as_ref());
    let (Some(source), Some(target)) = (boundary(&edge.source), boundary(&edge.target)) else { return Crossing::None };
    match (&source.role, &target.role) {
        (GroupBoundaryRole::In, GroupBoundaryRole::In) if body_of(project, &source.group_id) == Some(target.group_id.as_str()) =>
            Crossing::Into(&source.group_id),
        (GroupBoundaryRole::Out, GroupBoundaryRole::Out) if body_of(project, &target.group_id) == Some(source.group_id.as_str()) =>
            Crossing::OutOf(&target.group_id),
        _ => Crossing::None,
    }
}

/// The place at the other end of `edge` when this end is at `from`:
/// entering a body pushes the site, leaving one pops it. A wire out of
/// a body belongs to the site the walk is under; another site's wire
/// on the same boundary is not on this path, and is `None`.
fn step(project: &ProjectDefinition, from: &Located, edge: &Edge, direction: Direction) -> Option<Located> {
    let other = match direction {
        Direction::Upstream => &edge.source,
        Direction::Downstream => &edge.target,
    };
    let at = Located::new(other.clone(), from.path.clone());
    Some(match (crossing(project, edge), direction) {
        (Crossing::Into(site), Direction::Downstream) | (Crossing::OutOf(site), Direction::Upstream) => at.into_call(site),
        (Crossing::OutOf(site), Direction::Downstream) | (Crossing::Into(site), Direction::Upstream) => {
            if from.site() != Some(site) { return None; }
            at.out_of_call()?
        }
        (Crossing::None, _) => at,
    })
}

/// The place of `edge`'s source when its target is at `at`; `None`
/// when the wire is another call's (see `step`).
pub fn source_place(project: &ProjectDefinition, at: &Located, edge: &Edge) -> Option<Located> {
    step(project, at, edge, Direction::Upstream)
}

/// The places of a wire's two ends, `(source, target)`, from the wire's
/// own place (the deeper end's): the shallower end of a wire into or
/// out of a body is one site up. `None` for an unknown wire.
pub fn wire_ends(project: &ProjectDefinition, wire: &Located) -> Option<(Located, Located)> {
    let edge = project.edges.iter().find(|edge| edge.id == wire.id)?;
    let deep = |id: &str| Located::new(id, wire.path.clone());
    Some(match crossing(project, edge) {
        Crossing::Into(_) => (deep(&edge.source).out_of_call()?, deep(&edge.target)),
        Crossing::OutOf(_) => (deep(&edge.source), deep(&edge.target).out_of_call()?),
        Crossing::None => (deep(&edge.source), deep(&edge.target)),
    })
}

/// A wire as a place: its id, at the deeper of its two ends.
fn edge_place(edge: &Edge, a: &Located, b: &Located) -> Located {
    let deeper = if a.path.len() >= b.path.len() { a } else { b };
    Located::new(edge.id.clone(), deeper.path.clone())
}

/// The wires into `at` that are on its path, each with the place of its
/// source and the wire's own place.
fn incoming<'a>(project: &'a ProjectDefinition, at: &Located) -> impl Iterator<Item = (&'a Edge, Located, Located)> + 'a {
    let at = at.clone();
    project.edges.iter().filter_map(move |edge| {
        if edge.target != at.id { return None; }
        let source = step(project, &at, edge, Direction::Upstream)?;
        let place = edge_place(edge, &source, &at);
        Some((edge, source, place))
    })
}

/// The wires out of `at` that are on its path, each with the place of
/// its target and the wire's own place.
fn outgoing<'a>(project: &'a ProjectDefinition, at: &Located) -> impl Iterator<Item = (&'a Edge, Located, Located)> + 'a {
    let at = at.clone();
    project.edges.iter().filter_map(move |edge| {
        if edge.source != at.id { return None; }
        let target = step(project, &at, edge, Direction::Downstream)?;
        let place = edge_place(edge, &at, &target);
        Some((edge, target, place))
    })
}

impl RunSelection {
    /// Ordinary boundaries expose only the ports on selected paths.
    pub fn includes_port(&self, at: &Located, node: &NodeDefinition, port: &str) -> bool {
        !super::boundary_types::is_port_selected(&node.node_type)
            || self.boundary_ports.get(at).is_some_and(|ports| ports.contains(port))
    }

    /// Whether the wire `edge`, read from the end at `from`, is in the run.
    pub fn has_edge(&self, project: &ProjectDefinition, from: &Located, edge: &Edge, from_source: bool) -> bool {
        let direction = if from_source { Direction::Downstream } else { Direction::Upstream };
        step(project, from, edge, direction).is_some_and(|other| self.edges.contains(&edge_place(edge, from, &other)))
    }

    /// Whether the wire `edge` into the node at `at` carries something
    /// in this run: the wire is in it and so is what is at its source
    /// (a node of the run, or a supplier standing in for one).
    pub fn fed_by(&self, project: &ProjectDefinition, at: &Located, edge: &Edge) -> bool {
        step(project, at, edge, Direction::Upstream).is_some_and(|source| self.edges.contains(&edge_place(edge, &source, at))
            && (self.nodes.contains(&source) || self.suppliers.contains(&source)))
    }

    pub fn dispatchable_nodes(&self) -> std::collections::HashSet<Located> {
        self.nodes.iter().cloned().collect()
    }

    /// Every place downstream of `starts`, the starts included.
    pub fn downstream(project: &ProjectDefinition, starts: &[Located]) -> BTreeSet<Located> {
        walk(project, starts, Direction::Downstream, &|_| false)
    }

    /// Rebuild a selected node set's data paths and controls, preserving loops.
    pub fn restricted(project: &ProjectDefinition, nodes: BTreeSet<Located>) -> Result<Self, String> {
        for place in &nodes {
            if !project.nodes.iter().any(|node| node.id == place.id) {
                return Err(format!("unknown node '{}'", place.id));
            }
        }
        let selection = Self::from_nodes(project, nodes, BTreeSet::new());
        selection.validate_loops(project)?;
        Ok(selection)
    }

    /// The whole program: every node at every place it can run.
    pub fn whole(project: &ProjectDefinition) -> Self {
        Self::from_nodes(project, every_place(project), BTreeSet::new())
    }

    /// History relevant to this cut, including ancestors before its starts.
    /// An unrelated branch is not inherited merely because the seed retained it.
    pub fn history_nodes(&self, project: &ProjectDefinition) -> BTreeSet<Located> {
        let starts = self.nodes.iter().chain(&self.suppliers).flat_map(|place| {
            if is_ordinary_boundary(project, &place.id) {
                self.boundary_ports.get(place).into_iter().flatten()
                    .map(|port| (place.clone(), Some(port.clone()))).collect::<Vec<_>>()
            } else { vec![(place.clone(), None)] }
        }).collect();
        walk_ports(project, starts, Direction::Upstream, &|_| false)
    }

    /// Keep the authored cut while replacing reusable results with their
    /// frontier supply. History that feeds no new work adds no control work.
    pub fn with_reused(&self, project: &ProjectDefinition, reused: &BTreeSet<Located>) -> Self {
        let nodes: BTreeSet<_> = self.nodes.difference(reused).cloned().collect();
        let mut suppliers = self.suppliers.clone();
        for place in &nodes {
            suppliers.extend(incoming(project, place)
                .filter(|(_, source, wire)| self.edges.contains(wire) && reused.contains(source))
                .map(|(_, source, _)| source));
        }
        for place in nodes.iter().chain(&self.suppliers) {
            suppliers.extend(gates_of(project, place).into_iter()
                .map(|gate| Located::new(boundary_in_id(&gate.id), gate.path))
                .filter(|gate| reused.contains(gate)));
        }
        let mut selected = Self::from_nodes(project, nodes, suppliers);
        selected.nodes.retain(|place| self.nodes.contains(place) && !reused.contains(place));
        let targets: BTreeSet<Located> = selected.nodes.iter()
            .flat_map(|place| incoming(project, place).map(|(_, _, wire)| wire)).collect();
        selected.edges.retain(|wire| self.edges.contains(wire) && targets.contains(wire));
        selected.input = self.input.clone();
        selected.input_origins = self.input_origins.clone();
        selected
    }

    /// Manual carving and trigger fire use identical walks and bounds.
    pub fn carve(project: &ProjectDefinition, bounds: &SelectionBounds) -> Result<Self, String> {
        // Every bound is spelled from the top of the program, through
        // the call sites (`triage.up`), and resolves to one place.
        let mut entries = BTreeSet::new();
        for id in &bounds.from {
            if !entries.insert(start_node_at(project, id)?) {
                return Err(format!("starting entry '{id}' is supplied more than once"));
            }
        }
        let mut target = Vec::new();
        let mut before = Vec::new();
        let emits: Vec<Located> = bounds.emit.iter().map(|id| locate(project, id)).collect::<Result<_, _>>()?;
        let fire: Option<Located> = bounds.fire.as_ref().map(|id| locate(project, id)).transpose()?;
        let mut excluded: BTreeSet<Located> = emits.iter().cloned().collect();
        for (inclusive, ends) in [(true, &bounds.target), (false, &bounds.before)] {
            for spelled in ends {
                let place = locate(project, spelled)?;
                let endpoints: Vec<Located> = if project.groups.iter().any(|group| group.id == place.id) {
                    validate_group_place(project, &place, spelled)?;
                    members_with_paths(project, &place.id, &place.path)
                } else {
                    validate_endpoint(project, spelled)?;
                    vec![place]
                };
                if inclusive { target.extend(endpoints); } else {
                    excluded.extend(endpoints.iter().cloned());
                    before.extend(endpoints);
                }
            }
        }
        for spelled in bounds.emit.iter().chain(bounds.fire.iter()) {
            validate_endpoint(project, spelled)?;
        }
        for place in &emits {
            if let Some(group) = project.groups.iter().find(|group| boundary_in_id(&group.id) == place.id) {
                return Err(format!("cannot supply outputs of group entry '{}'; use --from {} with its input payload", place.id, group.id));
            }
        }
        if let Some(place) = emits.iter().find(|place| entries.contains(place) || fire.as_ref() == Some(place)) {
            return Err(format!("'{}' cannot both run and have its outputs supplied", place.id));
        }
        if let Some(fire) = &fire {
            if !project.nodes.iter().any(|n| n.id == fire.id && n.features.is_trigger) {
                return Err(format!("'{}' is not a trigger", fire.id));
            }
        }
        let mut selection = if let Some(group) = &bounds.group {
            if !bounds.from.is_empty() || !bounds.emit.is_empty() || !bounds.target.is_empty()
                || !bounds.before.is_empty()
            {
                return Err("group cannot be combined with from, emit, target, or before".into());
            }
            let place = locate(project, group)?;
            if !project.groups.iter().any(|g| g.id == place.id) {
                return Err(format!("unknown group '{}'", place.id));
            }
            validate_group_place(project, &place, group)?;
            let nodes: BTreeSet<Located> = members_with_paths(project, &place.id, &place.path).into_iter().collect();
            if fire.as_ref().is_some_and(|fire| !nodes.contains(fire)) {
                return Err("the fired trigger is outside the selected group".into());
            }
            Self::from_nodes(project, nodes, BTreeSet::new())
        } else {
            let is_trigger = |place: &Located| project.nodes.iter().any(|n| n.id == place.id && n.features.is_trigger);
            let stops = |place: &Located| is_trigger(place) || entries.contains(place) || emits.contains(place);
            let starts: Vec<Located> = entries.iter().cloned().chain(emits.iter().cloned()).chain(fire.iter().cloned()).collect();
            let mut nodes = if starts.is_empty() {
                every_place(project)
            } else {
                let downstream = Self::downstream(project, &starts);
                let consumers: Vec<Located> = downstream.iter().filter(|place| !is_ordinary_boundary(project, &place.id)).cloned().collect();
                let mut nodes = walk(project, &consumers, Direction::Upstream, &stops);
                nodes.extend(downstream);
                nodes
            };
            let mut suppliers: BTreeSet<Located> = emits.iter().cloned().collect();
            for ends in [&target, &before] {
                if !ends.is_empty() {
                    let allowed = walk(project, ends, Direction::Upstream, &stops);
                    nodes.retain(|place| allowed.contains(place));
                    suppliers.retain(|place| allowed.contains(place));
                }
            }
            suppliers.retain(|place| !before.contains(place));
            nodes.retain(|place| !excluded.contains(place));
            let selection = Self::from_nodes(project, nodes, suppliers);
            // Structural completion cannot restore an explicitly excluded endpoint.
            if selection.nodes.iter().any(|place| excluded.contains(place)) {
                return Err("the requested cut removes group control machinery needed by the run".into());
            }
            selection
        };
        if fire.is_some() {
            // A fired trigger reads its baked inputs. A shared setup producer
            // may run for another consumer without feeding the trigger again.
            // Unfired triggers close their outputs without reading setup inputs.
            let into_trigger: BTreeSet<&str> = project.edges.iter().filter(|edge| project.nodes.iter()
                .any(|node| node.id == edge.target && node.features.is_trigger)).map(|edge| edge.id.as_str()).collect();
            selection.edges.retain(|wire| !into_trigger.contains(wire.id.as_str()));
        }
        selection.validate_loops(project)?;
        Ok(selection)
    }

    /// Data dependencies and enclosing controls, with whole-loop expansion.
    pub fn dependencies(project: &ProjectDefinition, targets: &[Located]) -> Self {
        let walked = walk(project, targets, Direction::Upstream, &|_| false);
        Self::from_nodes(project, walked, BTreeSet::new())
    }

    /// Both setup phases stop inclusively at their targets.
    pub fn setup(project: &ProjectDefinition, targets: &[Located]) -> Result<Self, String> {
        for target in targets {
            validate_place(project, target, &super::address_of(project, &target.id, &target.path))?;
        }
        let selection = Self::dependencies(project, targets);
        selection.validate_loops(project)?;
        Ok(selection)
    }

    /// Whether something in the run feeds `port` of the node at `at`.
    pub fn has_supplier(&self, project: &ProjectDefinition, at: &Located, port: &str) -> bool {
        project.edges.iter().any(|edge| edge.target == at.id
            && edge.target_handle.as_deref().unwrap_or("default") == port && self.fed_by(project, at, edge))
    }

    /// Roots are relative to selected wires. Enclosing gates still control
    /// when these intents can dispatch, including an explicitly fired trigger.
    ///
    /// A member of a loop body is never a root here, wired or not: it
    /// runs once per iteration, at the iteration's frames, and the loop
    /// launcher kicks it there (`scope_body_roots`). A kick from this
    /// list would land at the loop's own frames, where no iteration's
    /// value can ever reach it, and hold the run open for ever. A
    /// member of a plain group is kicked by both and the two kicks
    /// merge (same node, same frames): the group launcher stamps its
    /// verdict onto the kick that is already there.
    pub fn roots(&self, project: &ProjectDefinition) -> Vec<Located> {
        self.nodes.iter()
            .filter(|place| !in_a_loop_body(project, place))
            .filter(|place| !project.edges.iter().any(|edge| edge.target == place.id && self.fed_by(project, place, edge)))
            .cloned().collect()
    }

    pub fn validate_loops(&self, project: &ProjectDefinition) -> Result<(), String> {
        for place in &self.nodes {
            for group in enclosing_loops(project, place) {
                let members = members_with_paths(project, &group.id, &group.path);
                if members.iter().any(|member| !self.nodes.contains(member)) {
                    return Err(format!("cannot cut inside loop '{}'; select the whole loop", group.id));
                }
            }
        }
        Ok(())
    }

    fn from_nodes(project: &ProjectDefinition, mut nodes: BTreeSet<Located>, suppliers: BTreeSet<Located>) -> Self {
        let mut gates = BTreeSet::new();
        let is_trigger = |place: &Located| project.nodes.iter().any(|n| n.id == place.id && n.features.is_trigger);
        loop {
            let before = nodes.len();
            for place in nodes.iter().chain(&suppliers) {
                gates.extend(gates_of(project, place));
            }
            for gate in &gates {
                let boundary = Located::new(boundary_in_id(&gate.id), gate.path.clone());
                if suppliers.contains(&boundary) { continue; }
                nodes.insert(boundary.clone());
                let sources: Vec<_> = incoming(project, &boundary)
                    .filter(|(edge, _, _)| edge.target_handle.as_deref() == Some("_should_flow"))
                    .map(|(edge, source, _)| (source, Some(edge.source_handle.as_deref().unwrap_or("default").to_string())))
                    .collect();
                nodes.extend(walk_ports(project, sources, Direction::Upstream, &is_trigger));
            }
            if nodes.len() == before { break; }
        }
        let mut edges = BTreeSet::new();
        let mut boundary_ports: BTreeMap<Located, BTreeSet<String>> = BTreeMap::new();
        // A boundary port is used when a node of the run writes it or a
        // selected consumer reads it. The consumer side walks backward
        // through boundary chains without admitting other ports. The
        // producer side matters for a port nobody downstream reads: the
        // node that fills it runs (a node fires when its inputs arrive,
        // whatever happens to its result), so the value reaches the
        // boundary and stops there. Carrying only the read ports left
        // such a boundary with no live input at all, recorded as skipped
        // at the start of a run its members completed, and re-run by
        // every seeded run after (a skip is not reused).
        let mut pending: Vec<(&Edge, Located, Located)> = nodes.iter()
            .filter(|place| !is_ordinary_boundary(project, &place.id))
            .flat_map(|place| incoming(project, place)).collect();
        for place in nodes.iter().filter(|place| is_ordinary_boundary(project, &place.id)) {
            let node = project.nodes.iter().find(|n| n.id == place.id).expect("selected node exists");
            for (edge, source, wire) in incoming(project, place)
                .filter(|(_, source, _)| nodes.contains(source) || suppliers.contains(source))
            {
                boundary_ports.entry(place.clone()).or_default().insert(edge.target_handle.as_deref().unwrap_or("default").into());
                pending.push((edge, source, wire));
            }
            let terminal = !outgoing(project, place).any(|(_, target, _)| nodes.contains(&target));
            if terminal {
                boundary_ports.entry(place.clone()).or_default().extend(node.port_literals.keys().cloned());
            }
        }
        for gate in &gates {
            let boundary = Located::new(boundary_in_id(&gate.id), gate.path.clone());
            boundary_ports.entry(boundary.clone()).or_default().insert("_should_flow".into());
            pending.extend(incoming(project, &boundary)
                .filter(|(edge, _, _)| edge.target_handle.as_deref() == Some("_should_flow")));
        }
        while let Some((edge, source, wire)) = pending.pop() {
            if !edges.insert(wire) { continue; }
            if is_ordinary_boundary(project, &source.id) && nodes.contains(&source) {
                let port = edge.source_handle.as_deref().unwrap_or("default");
                boundary_ports.entry(source.clone()).or_default().insert(port.into());
                pending.extend(incoming(project, &source)
                    .filter(|(edge, _, _)| edge.target_handle.as_deref().unwrap_or("default") == port));
            }
        }
        Self { nodes, edges, boundary_ports, gates, suppliers, input: BTreeMap::new(), input_origins: BTreeMap::new() }
    }
}

/// Every place in the program: the top-level nodes, and each included
/// file's nodes once per site that reaches it, however deep.
pub fn every_place(project: &ProjectDefinition) -> BTreeSet<Located> {
    let in_a_body = |node: &NodeDefinition| enclosing_body(project, node).is_some();
    let mut places: BTreeSet<Located> = project.nodes.iter().filter(|node| !in_a_body(node))
        .map(|node| Located::top(node.id.clone())).collect();
    for group in project.groups.iter().filter(|group| matches!(group.kind, GroupKind::Call { .. })) {
        let entry = project.nodes.iter().find(|node| node.id == boundary_in_id(&group.id));
        if entry.is_some_and(|entry| !in_a_body(entry)) {
            places.extend(members_with_paths(project, &group.id, &[]));
        }
    }
    places
}

/// The groups that gate `place`, each at the place its In runs: the
/// node's own scopes (a boundary's own container among them) at the
/// node's path, and every site on the path at the path above it.
fn gates_of(project: &ProjectDefinition, place: &Located) -> Vec<Located> {
    let mut gates: Vec<Located> = place.path.iter().enumerate()
        .map(|(depth, site)| Located::new(site.clone(), place.path[..depth].to_vec())).collect();
    if let Some(node) = project.nodes.iter().find(|n| n.id == place.id) {
        gates.extend(node.scope.iter().map(|group| Located::new(group.clone(), place.path.clone())));
        if is_ordinary_boundary(project, &node.id) {
            gates.extend(node.group_boundary.iter().map(|boundary| Located::new(boundary.group_id.clone(), place.path.clone())));
        }
    }
    gates
}

/// Whether `place` runs inside a loop's body: a loop in the node's own
/// scope, or around a site on its path. A loop's own boundaries are
/// not inside it (they run at the loop's frames), so `enclosing_loops`,
/// which counts them, is the wrong question for what the loop launcher
/// owns.
pub fn in_a_loop_body(project: &ProjectDefinition, place: &Located) -> bool {
    let is_loop = |group: &str| project.groups.iter().any(|g| g.id == group && matches!(g.kind, GroupKind::Loop { .. }));
    let scope_has_loop = |id: &str| project.nodes.iter().find(|n| n.id == id).is_some_and(|n| n.scope.iter().any(|g| is_loop(g)));
    scope_has_loop(&place.id) || place.path.iter().any(|site| scope_has_loop(&boundary_in_id(site)))
}

/// The loops around `place`, outermost first, each at the place the
/// loop runs: a loop in the node's own scope, and a loop around any
/// site on its path (a call inside a loop runs whole with the loop).
pub fn enclosing_loops(project: &ProjectDefinition, place: &Located) -> Vec<Located> {
    let is_loop = |group: &str| project.groups.iter().any(|g| g.id == group && matches!(g.kind, GroupKind::Loop { .. }));
    let node = |id: &str| project.nodes.iter().find(|n| n.id == id);
    let mut loops = Vec::new();
    for (depth, site) in place.path.iter().enumerate() {
        if let Some(entry) = node(&boundary_in_id(site)) {
            loops.extend(entry.scope.iter().filter(|group| is_loop(group))
                .map(|group| Located::new(group.clone(), place.path[..depth].to_vec())));
        }
    }
    if let Some(node) = node(&place.id) {
        loops.extend(node.scope.iter().chain(node.group_boundary.iter().map(|b| &b.group_id))
            .filter(|group| is_loop(group)).map(|group| Located::new(group.clone(), place.path.clone())));
    }
    loops.dedup();
    loops
}

/// A group's members, each at the place it runs when the group is at
/// `path`: its nodes and boundaries at `path`, and behind every call
/// site among them (the group itself, when it is one) the whole body
/// one site deeper.
pub fn members_with_paths(project: &ProjectDefinition, group: &str, path: &[String]) -> Vec<Located> {
    let mut out: Vec<Located> = project.nodes.iter().filter(|n| n.scope.iter().any(|g| g == group)
        || n.id == boundary_in_id(group) || n.id == boundary_out_id(group))
        .map(|n| Located::new(n.id.clone(), path.to_vec())).collect();
    let sites = project.groups.iter().filter(|site| matches!(site.kind, GroupKind::Call { .. }))
        .filter(|site| site.id == group || project.nodes.iter().any(|n| n.id == boundary_in_id(&site.id) && n.scope.iter().any(|g| g == group)));
    for site in sites {
        let body = body_of(project, &site.id).expect("a call site names its body");
        let mut deeper = path.to_vec();
        deeper.push(site.id.clone());
        out.extend(members_with_paths(project, body, &deeper));
    }
    out
}

/// A bound as a person wrote it, resolved: the node or group id and the
/// call path it names. An unknown spelling is reported as written.
fn locate(project: &ProjectDefinition, spelled: &str) -> Result<Located, String> {
    let (id, path) = super::resolve_address(project, spelled);
    if project.nodes.iter().any(|n| n.id == id) || project.groups.iter().any(|g| g.id == id) {
        Ok(Located::new(id, path))
    } else {
        Err(format!("unknown node '{spelled}'"))
    }
}

/// The start a spelled bound names, at its place: a group (a call site
/// included) starts at its In boundary; a node starts at itself. Never
/// inside a loop, and never a bare body: a body is started through a
/// site that calls it.
pub fn start_node_at(project: &ProjectDefinition, spelled: &str) -> Result<Located, String> {
    let place = locate(project, spelled)?;
    if project.groups.iter().any(|group| group.id == place.id) {
        validate_group_place(project, &place, spelled)?;
        return Ok(Located::new(boundary_in_id(&place.id), place.path));
    }
    validate_place(project, &place, spelled)?;
    Ok(place)
}

/// A group may be an endpoint (cut at, started, or run as `--group`)
/// under the same rule as a node (`validate_place`): nothing between
/// the top of the program and it runs whole, so no loop around it and
/// none around a site on its path; and a group inside an included file
/// is named through a site, never through the file's own id. A body is
/// never an endpoint: it runs through the site that calls it.
fn validate_group_place(project: &ProjectDefinition, place: &Located, spelled: &str) -> Result<(), String> {
    let group = project.groups.iter().find(|group| group.id == place.id)
        .ok_or_else(|| format!("unknown group '{spelled}'"))?;
    if matches!(group.kind, GroupKind::Body) {
        return Err(format!("cannot cut at '{spelled}': it is an included file, which runs through the site that includes it; name the site"));
    }
    let entry = Located::new(boundary_in_id(&group.id), place.path.clone());
    let node = project.nodes.iter().find(|node| node.id == entry.id)
        .ok_or_else(|| format!("group '{}' has no entry", group.id))?;
    // The group's own container is not "around" it: a loop is cut whole.
    if let Some(container) = enclosing_loops(project, &entry).into_iter().find(|l| l.id != group.id) {
        return Err(format!("cannot cut at '{spelled}' inside loop '{}'; select the whole loop", container.id));
    }
    if place.path.is_empty() {
        if let Some(body) = enclosing_body(project, node) {
            return Err(inside_a_file(project, spelled, &group.id, &body));
        }
    }
    Ok(())
}

/// The refusal for a body's node or group named without a site: the
/// spelling that names one call of it.
fn inside_a_file(project: &ProjectDefinition, spelled: &str, id: &str, body: &str) -> String {
    let example = project.groups.iter()
        .find(|g| matches!(&g.kind, GroupKind::Call { body: called } if called == body))
        .map(|g| super::address_of(project, id, std::slice::from_ref(&g.id)))
        .unwrap_or_else(|| format!("<site>.{}", id.rsplit('.').next().unwrap_or(id)));
    format!("cannot cut at '{spelled}': it is inside an included file, which is reached through a site; name it through the site, like `{example}`")
}

/// A node may be an endpoint when nothing between the top of the
/// program and it runs whole: not the node's own loops, and not a loop
/// around any site on its call path (a site inside a loop is cut with
/// the loop). A node inside an included file is reached through its
/// site, spelled `site.node`; naming the body's own id (`Triage.up`)
/// says nothing about which call, so it is refused with the spelling
/// that does.
pub fn validate_endpoint(project: &ProjectDefinition, spelled: &str) -> Result<(), String> {
    validate_place(project, &locate(project, spelled)?, spelled)
}

fn validate_place(project: &ProjectDefinition, place: &Located, spelled: &str) -> Result<(), String> {
    let node = project.nodes.iter().find(|node| node.id == place.id)
        .ok_or_else(|| format!("unknown node '{spelled}'"))?;
    if let Some(container) = enclosing_loop(project, node) {
        return Err(format!("cannot cut at '{spelled}' inside loop '{container}'; select the whole loop"));
    }
    if place.path.is_empty() {
        if let Some(body) = enclosing_body(project, node) {
            return Err(inside_a_file(project, spelled, &place.id, &body));
        }
    }
    for site in &place.path {
        let site_in = project.nodes.iter().find(|n| n.id == boundary_in_id(site))
            .ok_or_else(|| format!("call site '{site}' has no entry"))?;
        if let Some(container) = enclosing_loop(project, site_in) {
            return Err(format!("cannot cut at '{spelled}': the site '{site}' sits inside loop '{container}'; select the whole loop"));
        }
    }
    Ok(())
}

pub(crate) fn is_ordinary_boundary(project: &ProjectDefinition, id: &str) -> bool {
    project.nodes.iter().find(|n| n.id == id).and_then(|n| n.group_boundary.as_ref())
        .is_some_and(|b| project.groups.iter().any(|g| g.id == b.group_id && matches!(g.kind, GroupKind::Group | GroupKind::Call { .. } | GroupKind::Body)))
}

/// Whether `group` is an included file's body.
pub fn is_body(project: &ProjectDefinition, group: &str) -> bool {
    project.groups.iter().any(|g| g.id == group && matches!(g.kind, GroupKind::Body))
}

/// The shared body a call site's group stands for, when `group` is one.
fn body_of<'a>(project: &'a ProjectDefinition, group: &str) -> Option<&'a str> {
    project.groups.iter().find(|g| g.id == group).and_then(|g| match &g.kind {
        GroupKind::Call { body } => Some(body.as_str()),
        _ => None,
    })
}

/// The loop around `node`, if any: a loop's body runs whole, once per
/// iteration, so a run is never cut inside one.
fn enclosing_loop(project: &ProjectDefinition, node: &NodeDefinition) -> Option<String> {
    project.groups.iter().find(|g| matches!(g.kind, GroupKind::Loop { .. }) && (node.scope.contains(&g.id)
        || node.group_boundary.as_ref().is_some_and(|b| b.group_id == g.id))).map(|g| g.id.clone())
}

/// The included file's body `node` sits in (as a member or as one of
/// its boundaries), if any.
pub fn enclosing_body(project: &ProjectDefinition, node: &NodeDefinition) -> Option<String> {
    project.groups.iter().find(|g| matches!(g.kind, GroupKind::Body) && (node.scope.contains(&g.id)
        || node.group_boundary.as_ref().is_some_and(|b| b.group_id == g.id))).map(|g| g.id.clone())
}

fn walk(project: &ProjectDefinition, starts: &[Located], direction: Direction, stops: &dyn Fn(&Located) -> bool) -> BTreeSet<Located> {
    walk_ports(project, starts.iter().map(|place| (place.clone(), None)).collect(), direction, stops)
}

/// The walk is over places: entering a body through a site pushes the
/// site, leaving it through the site's other half pops it, and another
/// caller's wire on the same body boundary is never taken (see `step`).
/// A body reached through two sites is walked once per site. A stop is
/// reached and not walked through.
fn walk_ports(project: &ProjectDefinition, mut pending: Vec<(Located, Option<String>)>, direction: Direction, stops: &dyn Fn(&Located) -> bool) -> BTreeSet<Located> {
    let mut nodes = BTreeSet::new();
    let mut visited = BTreeSet::new();
    while let Some((place, port)) = pending.pop() {
        if !visited.insert((place.clone(), port.clone())) { continue; }
        nodes.insert(place.clone());
        if stops(&place) { continue; }
        // A loop goes in whole: reaching any part of one pulls in every
        // member and both boundaries, at the loop's place.
        if let Some(group) = enclosing_loops(project, &place).into_iter().next() {
            for member in members_with_paths(project, &group.id, &group.path) {
                if !visited.contains(&(member.clone(), None)) { pending.push((member, None)); }
            }
        }
        let ordinary = is_ordinary_boundary(project, &place.id);
        let next: Vec<(Located, Option<String>)> = match direction {
            Direction::Upstream => incoming(project, &place)
                .filter(|(edge, _, _)| !ordinary || port.as_ref().is_none_or(|p| edge.target_handle.as_deref().unwrap_or("default") == p))
                .map(|(edge, source, _)| (source, port_of(project, &edge.source, edge.source_handle.as_deref()))).collect(),
            Direction::Downstream => outgoing(project, &place)
                .filter(|(edge, _, _)| !ordinary || port.as_ref().is_none_or(|p| edge.source_handle.as_deref().unwrap_or("default") == p))
                .map(|(edge, target, _)| (target, port_of(project, &edge.target, edge.target_handle.as_deref()))).collect(),
        };
        pending.extend(next);
        // A `_should_flow` wire into a group's door says "run what is in
        // here", the way the same wire into a node says "run this node":
        // walking forward through it puts the whole group in the run,
        // its body behind a call site included. A data port on the door
        // keeps the port-by-port walk, so a cut stays precise.
        if direction == Direction::Downstream && port.as_deref().is_some_and(crate::exec::skip::is_gate_port) {
            if let Some(group) = gated_group(project, &place) {
                for member in members_with_paths(project, &group, &place.path) {
                    if !visited.contains(&(member.clone(), None)) { pending.push((member, None)); }
                }
            }
        }
    }
    nodes
}

/// The group whose door `place` is: an ordinary In boundary's group,
/// `None` for anything else.
fn gated_group(project: &ProjectDefinition, place: &Located) -> Option<String> {
    let node = project.nodes.iter().find(|n| n.id == place.id)?;
    let boundary = node.group_boundary.as_ref()?;
    (is_ordinary_boundary(project, &place.id) && boundary.role == GroupBoundaryRole::In).then(|| boundary.group_id.clone())
}

/// The port a walk arrives on at an ordinary boundary (whose ports are
/// walked one at a time); `None` for any other node.
fn port_of(project: &ProjectDefinition, node: &str, handle: Option<&str>) -> Option<String> {
    is_ordinary_boundary(project, node).then(|| handle.unwrap_or("default").to_string())
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

    /// Top-level places, by id.
    fn tops(values: &[&str]) -> BTreeSet<Located> {
        values.iter().map(|v| Located::top(*v)).collect()
    }

    fn names(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|v| (*v).into()).collect()
    }

    fn top(id: &str) -> Located {
        Located::top(id)
    }

    fn at(id: &str, path: &[&str]) -> Located {
        Located::new(id, path.iter().map(|s| s.to_string()).collect())
    }

    fn ids(places: &BTreeSet<Located>) -> BTreeSet<String> {
        places.iter().map(|p| p.id.clone()).collect()
    }

    fn top_members(project: &ProjectDefinition, group: &str) -> BTreeSet<Located> {
        members_with_paths(project, group, &[]).into_iter().collect()
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
            for id in ["c", "after", "unrelated", "trigger", "gate"] { assert!(selected.nodes.contains(&top(id)), "{id}"); }
            for id in ["a", "sibling"] { assert!(!selected.nodes.contains(&top(id)), "{id}"); }
            assert_eq!(selected.nodes.contains(&top("b")), !emitting);
        }
        let selected = RunSelection::carve(&project, &SelectionBounds { from: vec!["c".into()], ..Default::default() }).unwrap();
        assert!(!selected.nodes.contains(&top("unrelated")), "c itself is the boundary; do not recover its input producers");
        assert!(!selected.nodes.contains(&top("b")));
    }

    #[test]
    fn public_group_and_loop_endpoints_include_or_exclude_the_whole_container() {
        for looping in [false, true] {
            let mut project = program();
            if looping { project.groups[0].kind = GroupKind::Loop { loop_config: json!({}) }; }
            let through = RunSelection::carve(&project, &SelectionBounds { target: vec!["g".into()], ..Default::default() }).unwrap();
            assert!(top_members(&project, "g").is_subset(&through.nodes));
            assert!(!through.nodes.contains(&top("after")));
            let before = RunSelection::carve(&project, &SelectionBounds { before: vec!["g".into()], ..Default::default() }).unwrap();
            assert!(top_members(&project, "g").is_disjoint(&before.nodes));
            assert_eq!(before.nodes, tops(&["a", "unrelated", "gate"]));
        }
    }

    /// An unwired node inside a plain group is a root the run kicks
    /// (its kick merges with the group launcher's); the same node
    /// inside a loop is the loop launcher's alone, once per iteration,
    /// so the pre-run list leaves it out. The loop's own door stays a
    /// root the way any node does.
    #[test]
    fn a_loop_body_member_is_never_a_pre_run_root() {
        for looping in [false, true] {
            let mut project = program();
            project.nodes.push(serde_json::from_value(json!({
                "id": "lonely", "nodeType": "T", "label": null, "config": {},
                "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
                "features": {}, "scope": ["g"], "groupBoundary": null, "requiresInfra": false
            })).unwrap());
            project.groups[0].node_ids.push("lonely".into());
            if looping { project.groups[0].kind = GroupKind::Loop { loop_config: json!({}) }; }
            let roots: BTreeSet<Located> = RunSelection::whole(&project).roots(&project).into_iter().collect();
            let expected = if looping { tops(&["a", "unrelated", "gate"]) } else { tops(&["a", "unrelated", "gate", "trigger", "lonely"]) };
            assert_eq!(roots, expected, "looping={looping}");
            assert_eq!(in_a_loop_body(&project, &top("lonely")), looping);
            assert!(!in_a_loop_body(&project, &top("g__in")), "a door is not inside its own loop");
        }
    }

    #[test]
    fn setup_stops_inside_group_and_follows_only_used_boundary_port() {
        let project = program();
        let selection = RunSelection::setup(&project, &[top("b")]).unwrap();
        assert_eq!(selection.nodes, tops(&["a", "b", "g__in", "gate"]));
        assert_eq!(selection.boundary_ports[&top("g__in")], names(&["x", "_should_flow"]));
        assert_eq!(selection.edges, tops(&["a.out->g__in.x", "g__in.x->b.in", "gate.out->g__in._should_flow"]));
    }

    #[test]
    fn mixed_bounds_do_not_restore_exclusive_endpoint() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            target: vec!["c".into()], before: vec!["b".into()], ..Default::default()
        }).unwrap();
        assert!(!selection.nodes.contains(&top("b")));
        assert!(!selection.nodes.contains(&top("c")));
        assert!(!selection.nodes.contains(&top("sibling")));
        assert!(selection.nodes.contains(&top("a")));
    }

    #[test]
    fn a_terminal_boundary_keeps_its_selected_input_paths() {
        let project = program();
        let selection = RunSelection::carve(&project, &SelectionBounds { target: vec!["g__out".into()], ..Default::default() }).unwrap();
        assert_eq!(selection.boundary_ports[&top("g__out")], names(&["result"]));
        assert!(selection.edges.contains(&top("c.out->g__out.result")));
        assert!(!selection.nodes.contains(&top("after")));
        let before = RunSelection::carve(&project, &SelectionBounds { before: vec!["b".into()], ..Default::default() }).unwrap();
        assert_eq!(before.boundary_ports[&top("g__in")], names(&["x", "_should_flow"]));
        assert!(!before.edges.contains(&top("g__in.x->b.in")));
        assert!(!before.edges.contains(&top("unrelated.out->g__in.y")));
    }

    /// A group output nobody downstream reads is still carried up to
    /// the group's Out when the node that fills it is in the run: the
    /// Out is not a root with every input closed, and its record is
    /// the group completing, not a skip.
    #[test]
    fn a_boundary_carries_every_port_a_node_of_the_run_writes() {
        let project = program();
        let whole = RunSelection::whole(&project);
        assert!(whole.edges.contains(&top("c.out->g__out.result")), "{:?}", whole.edges);
        assert!(whole.boundary_ports[&top("g__out")].contains("result"));
        assert!(!whole.roots(&project).contains(&top("g__out")), "a fed Out is not a root: {:?}", whole.roots(&project));
    }

    #[test]
    fn fire_does_not_admit_unrelated_group_branch() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            fire: Some("trigger".into()), ..Default::default()
        }).unwrap();
        assert_eq!(selection.nodes, tops(&["a", "b", "c", "trigger", "g__in", "g__out", "gate", "after"]));
        assert!(!selection.edges.contains(&top("unrelated.out->g__in.y")));
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
        assert!(!selection.edges.contains(&top("setup-trigger")));
        assert!(selection.nodes.contains(&top("g__in")));
        assert!(selection.nodes.contains(&top("trigger")));
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
        let setup = RunSelection::setup(&project, &[top("trigger")]).unwrap();
        assert!(setup.nodes.contains(&top("b")));
        assert!(setup.edges.contains(&top("setup")));
        assert!(!setup.nodes.contains(&top("c")));
        for from in [vec![], vec!["b".into()]] {
            let fire = RunSelection::carve(&project, &SelectionBounds {
                fire: Some("trigger".into()), from, ..Default::default()
            }).unwrap();
            assert!(fire.nodes.contains(&top("b")));
            assert!(fire.nodes.contains(&top("c")));
            assert!(fire.edges.contains(&top("b.out->c.in")));
            assert!(fire.edges.contains(&top("trigger.out->c.event")));
            assert!(!fire.edges.contains(&top("setup")));
        }
    }

    #[test]
    fn simulated_source_keeps_enclosing_gate_without_running_its_body() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            emit: vec!["c".into()], ..Default::default()
        }).unwrap();
        assert_eq!(selection.nodes, tops(&["g__in", "g__out", "gate", "after"]));
        assert_eq!(selection.suppliers, tops(&["c"]));
        assert_eq!(selection.gates, tops(&["g"]));
        assert!(selection.has_supplier(&program(), &top("g__out"), "result"));
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
        let selection = RunSelection::setup(&project, &[top("c")]).unwrap();
        let reused = selection.with_reused(&project, &tops(&["a", "b", "g__in", "gate", "trigger"]));
        assert_eq!(reused.nodes, tops(&["c"]));
        assert_eq!(reused.edges, tops(&["b.out->c.in", "trigger.out->c.event"]));
        assert!(reused.suppliers.contains(&top("g__in")), "the inherited gate controls c");
        assert!(!reused.nodes.contains(&top("gate")));
    }

    #[test]
    fn loop_endpoints_rejected_but_group_selection_is_whole() {
        let mut project = program();
        project.groups[0].kind = GroupKind::Loop { loop_config: json!({}) };
        for id in ["b", "c", "g__in", "g__out"] {
            assert!(validate_endpoint(&project, id).unwrap_err().contains("inside loop"));
            assert!(RunSelection::setup(&project, &[top(id)]).is_err());
        }
        let selection = RunSelection::carve(&project, &SelectionBounds {
            group: Some("g".into()), ..Default::default()
        }).unwrap();
        assert!(top_members(&project, "g").is_subset(&selection.nodes));
        let selection = RunSelection::carve(&project, &SelectionBounds {
            target: vec!["after".into()], ..Default::default()
        }).unwrap();
        assert!(top_members(&project, "g").is_subset(&selection.nodes));
    }

    /// A loop is bad at running half of itself, so no selection may
    /// hold some of a loop's body and not the rest. Every door that
    /// builds a selection asks `validate_loops` before handing it
    /// back, and this is the check itself: a partial body is refused
    /// naming the loop, the whole body passes.
    ///
    /// The refusal here is NOT the one a named endpoint gets. Asking
    /// to cut AT a node inside a loop is turned down earlier, by
    /// `validate_place`, whose message reads "cannot cut AT '<node>'
    /// inside loop". This one has no "at": it is about the SHAPE of
    /// the resulting set, which is why it can catch a cut that named
    /// nothing illegal and still came out half a loop.
    #[test]
    fn a_selection_holding_half_a_loop_is_refused() {
        let mut project = program();
        project.groups[0].kind = GroupKind::Loop { loop_config: json!({}) };

        let refusal = RunSelection::restricted(&project, tops(&["b"]))
            .expect_err("one member of a loop body is half a loop");
        assert!(refusal.contains("cannot cut inside loop 'g'"), "{refusal}");
        assert!(refusal.contains("select the whole loop"), "{refusal}");
        assert!(!refusal.contains("cannot cut at"), "this is the shape check, not the endpoint one: {refusal}");

        // Two of the four is still half.
        assert!(RunSelection::restricted(&project, tops(&["b", "c"])).is_err());

        // The whole body, and the check is satisfied: it refuses a
        // partial loop, not every loop.
        let whole: BTreeSet<Located> = top_members(&project, "g")
            .into_iter()
            .chain(tops(&["g__in", "g__out"]))
            .collect();
        RunSelection::restricted(&project, whole).expect("the whole body is a legal cut");

        // A plain group is not a loop: half of one is fine.
        let mut plain = program();
        plain.groups[0].kind = GroupKind::Group;
        RunSelection::restricted(&plain, tops(&["b"])).expect("a group may be cut into");
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
            assert_eq!(start.input[&top("g__in")]["x"], json!("backup"));
            assert_eq!(confined.input, start.input);
            assert!(start.nodes.contains(&top("after")));
            assert!(!confined.nodes.contains(&top("after")));
            assert!(top_members(&project, "g").is_subset(&start.nodes));
            assert!(top_members(&project, "g").is_subset(&confined.nodes));
            assert!(!start.nodes.contains(&top("a")));
            let both = crate::run_spec::RunSpec { group: group.group, ..from };
            assert!(crate::run_spec::resolve_spec(&both, &project).unwrap_err().to_string().contains("cannot be combined"));
        }
    }

    /// `src` feeds two call sites `a` and `b` of one body `B` (member
    /// `B.n`), each answering its own sink.
    fn called_program() -> ProjectDefinition {
        use super::super::boundary_types as bt;
        let node = |id: &str, ty: &str, scope: &[&str], boundary: Value| json!({
            "id": id, "nodeType": ty, "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false
        });
        let edge = |source: &str, sp: &str, target: &str, tp: &str| json!({
            "id": format!("{source}.{sp}->{target}.{tp}"), "source": source, "target": target,
            "sourceHandle": sp, "targetHandle": tp
        });
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", "T", &[], Value::Null),
                node("a__in", bt::CALL_IN, &[], json!({"groupId": "a", "role": "In"})),
                node("a__out", bt::CALL_OUT, &[], json!({"groupId": "a", "role": "Out"})),
                node("b__in", bt::CALL_IN, &[], json!({"groupId": "b", "role": "In"})),
                node("b__out", bt::CALL_OUT, &[], json!({"groupId": "b", "role": "Out"})),
                node("B__in", bt::INCLUDE_IN, &[], json!({"groupId": "B", "role": "In"})),
                node("B.n", "T", &["B"], Value::Null),
                node("B.free", "T", &["B"], Value::Null),
                node("B__out", bt::INCLUDE_OUT, &[], json!({"groupId": "B", "role": "Out"})),
                node("sa", "T", &[], Value::Null), node("sb", "T", &[], Value::Null),
            ],
            "edges": [
                edge("src", "a", "a__in", "x"), edge("src", "b", "b__in", "x"),
                edge("a__in", "x", "B__in", "x"), edge("b__in", "x", "B__in", "x"),
                edge("B__in", "x", "B.n", "in"), edge("B.n", "out", "B__out", "y"),
                edge("B__out", "y", "a__out", "y"), edge("B__out", "y", "b__out", "y"),
                edge("a__out", "y", "sa", "in"), edge("b__out", "y", "sb", "in"),
            ],
            "groups": [
                {"id": "a", "kind": "call", "body": "B", "nodeIds": []},
                {"id": "b", "kind": "call", "body": "B", "nodeIds": []},
                {"id": "B", "kind": "body", "nodeIds": ["B.n", "B.free"]}
            ]
        })).unwrap()
    }

    /// Aiming at what one call site feeds pulls that site and the whole
    /// body under it, never the other site's chain: the body's outer
    /// wires belong to the sites, and the walk enters a body only
    /// through the site it came from.
    #[test]
    fn a_call_site_is_cut_whole_with_its_body_and_without_the_other_callers() {
        let project = called_program();
        let selection = RunSelection::carve(&project, &SelectionBounds { target: vec!["sa".into()], ..Default::default() }).unwrap();
        let expected: BTreeSet<Located> = [top("src"), top("a__in"), top("a__out"), at("B__in", &["a"]), at("B.n", &["a"]), at("B__out", &["a"]), top("sa")].into();
        assert_eq!(selection.nodes, expected, "{:?}", selection.nodes);
        assert!(selection.edges.contains(&at("a__in.x->B__in.x", &["a"])) && !selection.edges.iter().any(|e| e.id == "b__in.x->B__in.x"));
        // Naming the site itself is the same cut.
        let by_site = RunSelection::carve(&project, &SelectionBounds { target: vec!["a".into()], ..Default::default() }).unwrap();
        assert!(by_site.nodes.contains(&at("B.n", &["a"])) && by_site.nodes.contains(&top("a__in")) && !by_site.nodes.contains(&top("sa")));
        // A node inside the body is named through a site. The body's own
        // id says nothing about which call, so it is refused with the
        // spelling that does.
        let err = validate_endpoint(&project, "B.n").unwrap_err();
        assert!(err.contains("inside an included file") && err.contains("like `a.n`"), "{err}");
        validate_endpoint(&project, "a.n").unwrap();
        validate_endpoint(&project, "b.n").unwrap();
        // The body's own id is no endpoint, however it is asked for.
        for bounds in [SelectionBounds { group: Some("B".into()), ..Default::default() }, SelectionBounds { target: vec!["B".into()], ..Default::default() }, SelectionBounds { from: vec!["B".into()], ..Default::default() }] {
            let err = RunSelection::carve(&project, &bounds).unwrap_err();
            assert!(err.contains("included file") && err.contains("name the site"), "{err}");
        }
        // `--group a` is the same cut as `--target a` without what feeds
        // the site: the call, whole, at its place.
        let grouped = RunSelection::carve(&project, &SelectionBounds { group: Some("a".into()), ..Default::default() }).unwrap();
        let expected: BTreeSet<Located> = [top("a__in"), top("a__out"), at("B__in", &["a"]), at("B.n", &["a"]), at("B.free", &["a"]), at("B__out", &["a"])].into();
        assert_eq!(grouped.nodes, expected, "{:?}", grouped.nodes);
        assert!(!grouped.nodes.contains(&top("src")) && !grouped.nodes.contains(&top("sa")));
        // The whole program holds the body once per site.
        let whole = RunSelection::whole(&project);
        assert!(whole.nodes.contains(&at("B.n", &["a"])) && whole.nodes.contains(&at("B.n", &["b"])) && !whole.nodes.contains(&top("B.n")));
        assert_eq!(whole.nodes.len(), 7 + 2 * 4);
    }

    /// A cut spelled through a site is a cut inside that one call: the
    /// node, the site above it and the body's gate are in the run, each
    /// at its place, so the birth kicks land at the call's frames and
    /// the other site stays out.
    #[test]
    fn a_cut_inside_an_included_file_carries_its_call() {
        let project = called_program();
        // To a node inside the call: the call site, the body's In and the node.
        let to = RunSelection::carve(&project, &SelectionBounds { target: vec!["a.n".into()], ..Default::default() }).unwrap();
        let expected: BTreeSet<Located> = [top("src"), top("a__in"), at("B__in", &["a"]), at("B.n", &["a"])].into();
        assert_eq!(to.nodes, expected, "{:?}", to.nodes);
        // From a node inside the call: it is a root, kicked under the call,
        // and the site's In is its gate, kicked at the top.
        let from = RunSelection::carve(&project, &SelectionBounds { from: vec!["a.n".into()], ..Default::default() }).unwrap();
        for place in [at("B.n", &["a"]), top("a__in"), at("B__in", &["a"]), top("sa")] {
            assert!(from.nodes.contains(&place), "{place} in {:?}", from.nodes);
        }
        for id in ["src", "b__in", "sb"] {
            assert!(!from.nodes.iter().any(|p| p.id == id), "{id} out of {:?}", from.nodes);
        }
        assert!(from.roots(&project).contains(&top("a__in")), "the gate has nothing feeding it: {:?}", from.roots(&project));
        assert_eq!(at("B.n", &["a"]).frames(), vec![crate::frames::Frame::Call { site: "a".into() }]);
        // The birth kicks: the site's In at the top (its closed inputs
        // start the body under the call, and the node reads its backup
        // there); the node itself is fed by the body's In, so it is no
        // root and gets no kick of its own.
        let kicks = crate::run_spec::KickPlan::for_selection(&project, &from, None, None);
        let kick_of = |node: &str| kicks.iter().filter(|k| k.node == node).map(|k| k.frames.clone()).collect::<Vec<_>>();
        assert_eq!(kick_of("a__in"), vec![vec![]]);
        assert!(kick_of("B.n").is_empty());
        // A root inside a body IS kicked under the call.
        let mut inside = from.clone();
        inside.nodes.insert(at("B.free", &["a"]));
        let kicks = crate::run_spec::KickPlan::for_selection(&project, &inside, None, None);
        assert_eq!(kicks.iter().filter(|k| k.node == "B.free").map(|k| k.frames.clone()).collect::<Vec<_>>(),
            vec![vec![crate::frames::Frame::Call { site: "a".into() }]]);
        // Before it: the site and the body's gate, not the node.
        let before = RunSelection::carve(&project, &SelectionBounds { from: vec!["a".into()], before: vec!["a.n".into()], ..Default::default() }).unwrap();
        assert!(before.nodes.contains(&top("a__in")) && before.nodes.contains(&at("B__in", &["a"])) && !before.nodes.iter().any(|p| p.id == "B.n"), "{:?}", before.nodes);
    }

    /// Two chained calls of one file: `one` feeds `two`. A cut from a
    /// node in the first call to the same node in the second is two
    /// places of one id, and it holds exactly the path between them:
    /// the rest of the first call, the second site, and the second
    /// call's gate. The node's backup applies at the first place only,
    /// and the second place is fed by the wire.
    #[test]
    fn a_cut_from_one_call_to_the_next_is_two_places_of_one_node() {
        let mut project = called_program();
        // Rewire: src -> a, a -> b (instead of src -> b), b -> sb.
        project.edges.retain(|e| e.id != "src.b->b__in.x" && e.id != "a__out.y->sa.in");
        project.edges.push(serde_json::from_value(json!({"id": "a__out.y->b__in.x", "source": "a__out", "sourceHandle": "y", "target": "b__in", "targetHandle": "x"})).unwrap());
        let cut = RunSelection::carve(&project, &SelectionBounds { from: vec!["a.n".into()], target: vec!["b.n".into()], ..Default::default() }).unwrap();
        let expected: BTreeSet<Located> = [
            at("B.n", &["a"]), at("B__out", &["a"]), top("a__out"), top("b__in"), at("B__in", &["b"]), at("B.n", &["b"]),
            top("a__in"), at("B__in", &["a"]),
        ].into();
        assert_eq!(cut.nodes, expected, "{:?}", cut.nodes);
        assert!(!cut.nodes.iter().any(|p| p.id == "src" || p.id == "sb" || p.id == "b__out"));
        assert!(cut.edges.contains(&at("B.n.out->B__out.y", &["a"])) && !cut.edges.contains(&at("B.n.out->B__out.y", &["b"])));
        assert!(cut.edges.contains(&at("B__out.y->a__out.y", &["a"])));
        assert!(cut.edges.contains(&top("a__out.y->b__in.x")));
        assert!(cut.edges.contains(&at("b__in.x->B__in.x", &["b"])) && !cut.edges.iter().any(|e| e.id == "a__in.x->B__in.x" && e.path == vec!["b".to_string()]));
        // Only the first site's In is a root; the second is fed by the first call.
        assert_eq!(cut.roots(&project), vec![top("a__in")]);
        assert!(cut.has_supplier(&project, &at("B.n", &["b"]), "in"));
        // The wire out of the first call's node is in the run at that
        // place, and at the second place the same wire is not.
        let wire = project.edges.iter().find(|e| e.id == "B.n.out->B__out.y").unwrap();
        assert!(cut.has_edge(&project, &at("B.n", &["a"]), wire, true));
        assert!(!cut.has_edge(&project, &at("B.n", &["b"]), wire, true));
        // Before the second place: the second call's gate opens, the node does not run.
        let before = RunSelection::carve(&project, &SelectionBounds { from: vec!["a.n".into()], before: vec!["b.n".into()], ..Default::default() }).unwrap();
        assert!(before.nodes.contains(&at("B__in", &["b"])) && !before.nodes.contains(&at("B.n", &["b"])) && before.nodes.contains(&at("B.n", &["a"])), "{:?}", before.nodes);
        // The spec's backup lands at the first place only.
        let spec: crate::run_spec::RunSpec = serde_json::from_value(json!({"name": "case", "from": {"a.n": {"in": "cut"}}, "target": ["b.n"]})).unwrap();
        project.nodes.iter_mut().find(|n| n.id == "B.n").unwrap().inputs = serde_json::from_value(json!([{"name": "in", "portType": "String", "required": true}])).unwrap();
        let resolved = crate::run_spec::resolve_spec(&spec, &project).unwrap();
        assert_eq!(resolved.selection.input.keys().cloned().collect::<Vec<_>>(), vec![at("B.n", &["a"])]);
        assert_eq!(resolved.kicks.iter().map(|k| (k.node.clone(), k.frames.clone())).collect::<Vec<_>>(), vec![("a__in".to_string(), vec![])]);
    }

    /// A group inside an included file is an endpoint through its site,
    /// like a node, and never through the file's own id.
    #[test]
    fn a_group_inside_an_included_file_is_an_endpoint_through_its_site() {
        let mut project = called_program();
        let node = |id: &str, scope: &[&str], boundary: Value| serde_json::from_value::<NodeDefinition>(json!({
            "id": id, "nodeType": "T", "label": null, "config": {}, "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false })).unwrap();
        project.nodes.push(node("B.g__in", &["B"], json!({"groupId": "B.g", "role": "In"})));
        project.nodes.push(node("B.g.x", &["B", "B.g"], Value::Null));
        project.nodes.push(node("B.g__out", &["B"], json!({"groupId": "B.g", "role": "Out"})));
        project.groups.push(serde_json::from_value(json!({"id": "B.g", "kind": "group", "nodeIds": ["B.g.x"]})).unwrap());
        for bounds in [SelectionBounds { group: Some("a.g".into()), ..Default::default() }, SelectionBounds { target: vec!["a.g".into()], ..Default::default() }] {
            let cut = RunSelection::carve(&project, &bounds).unwrap();
            assert!(cut.nodes.contains(&at("B.g.x", &["a"])) && cut.nodes.contains(&at("B.g__in", &["a"])), "{:?}", cut.nodes);
            assert!(!cut.nodes.iter().any(|p| p.path == vec!["b".to_string()]));
        }
        assert_eq!(start_node_at(&project, "a.g").unwrap(), at("B.g__in", &["a"]));
        let err = RunSelection::carve(&project, &SelectionBounds { target: vec!["B.g".into()], ..Default::default() }).unwrap_err();
        assert!(err.contains("like `a.g`"), "{err}");
    }

    /// A site inside a loop is cut with the loop, the whole body under
    /// it included, however the loop is reached.
    #[test]
    fn a_call_inside_a_loop_goes_in_whole() {
        use super::super::boundary_types as bt;
        let mut project = called_program();
        let lp = |id: &str, ty: &str, scope: &[&str], boundary: Value| serde_json::from_value::<NodeDefinition>(json!({
            "id": id, "nodeType": ty, "label": null, "config": {}, "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false })).unwrap();
        project.nodes.push(lp("l__in", bt::LOOP_IN, &[], json!({"groupId": "l", "role": "In"})));
        project.nodes.push(lp("l__out", bt::LOOP_OUT, &[], json!({"groupId": "l", "role": "Out"})));
        project.nodes.push(lp("l.c__in", bt::CALL_IN, &["l"], json!({"groupId": "l.c", "role": "In"})));
        project.nodes.push(lp("l.c__out", bt::CALL_OUT, &["l"], json!({"groupId": "l.c", "role": "Out"})));
        project.nodes.push(lp("after", "T", &[], Value::Null));
        project.groups.push(serde_json::from_value(json!({"id": "l", "kind": "loop", "loopConfig": {}, "nodeIds": []})).unwrap());
        project.groups.push(serde_json::from_value(json!({"id": "l.c", "kind": "call", "body": "B", "nodeIds": []})).unwrap());
        let edge = |id: &str, s: &str, sp: &str, t: &str, tp: &str| serde_json::from_value::<Edge>(json!({"id": id, "source": s, "sourceHandle": sp, "target": t, "targetHandle": tp})).unwrap();
        project.edges.push(edge("e1", "src", "l", "l__in", "items"));
        project.edges.push(edge("e2", "l__in", "items", "l.c__in", "x"));
        project.edges.push(edge("e3", "l.c__in", "x", "B__in", "x"));
        project.edges.push(edge("e4", "B__out", "y", "l.c__out", "y"));
        project.edges.push(edge("e5", "l.c__out", "y", "l__out", "ys"));
        project.edges.push(edge("e6", "l__out", "ys", "after", "in"));
        let selection = RunSelection::carve(&project, &SelectionBounds { target: vec!["after".into()], ..Default::default() }).unwrap();
        for place in [top("l__in"), top("l__out"), top("l.c__in"), at("B__in", &["l.c"]), at("B.n", &["l.c"]), at("B.free", &["l.c"]), at("B__out", &["l.c"]), top("after")] {
            assert!(selection.nodes.contains(&place), "{place} in {:?}", selection.nodes);
        }
        assert!(!selection.nodes.iter().any(|p| p.id == "a__in" || p.id == "b__in"));
        for spelled in ["l.c.n", "l.c", "l.c.free"] {
            let err = RunSelection::carve(&project, &SelectionBounds { target: vec![spelled.into()], ..Default::default() }).unwrap_err();
            assert!(err.contains("inside loop 'l'"), "{spelled}: {err}");
        }
        let members: BTreeSet<Located> = members_with_paths(&project, "l", &[]).into_iter().collect();
        assert!(members.contains(&at("B.n", &["l.c"])) && members.contains(&top("l.c__in")) && members.contains(&top("l__out")));
        assert_eq!(ids(&members), names(&["l__in", "l__out", "l.c__in", "l.c__out", "B__in", "B.n", "B.free", "B__out"]));
    }

    #[test]
    fn a_place_reads_and_writes_as_one_key() {
        let place = at("B.n", &["x", "X.a"]);
        assert_eq!(place.to_string(), "x/X.a/B.n");
        assert_eq!(serde_json::to_value(&place).unwrap(), json!("x/X.a/B.n"));
        assert_eq!(serde_json::from_value::<Located>(json!("x/X.a/B.n")).unwrap(), place);
        assert_eq!(serde_json::from_value::<Located>(json!("src")).unwrap(), top("src"));
        assert!(serde_json::from_value::<Located>(json!("x/")).is_err());
        assert!(serde_json::from_value::<Located>(json!("/n")).is_err());
        assert_eq!(Located::at("n", &vec![crate::frames::Frame::Loop { index: 2 }, crate::frames::Frame::Call { site: "x".into() }, crate::frames::Frame::Loop { index: 0 }]), at("n", &["x"]));
    }

    #[test]
    fn selection_round_trip_retains_empty_set_and_supplier_facts() {
        let selection = RunSelection::carve(&program(), &SelectionBounds {
            emit: vec!["c".into()], ..Default::default()
        }).unwrap();
        assert_eq!(serde_json::from_value::<RunSelection>(serde_json::to_value(&selection).unwrap()).unwrap(), selection);
        assert_eq!(serde_json::from_value::<RunSelection>(serde_json::to_value(RunSelection::default()).unwrap()).unwrap(), RunSelection::default());
    }

    /// The program an API builder writes: a route at the top, and the
    /// work hanging off it through `_should_flow` alone, once as a
    /// plain group and once as an included file whose body includes
    /// another. The route feeds no data into either.
    fn gated_program() -> ProjectDefinition {
        use super::super::boundary_types as bt;
        let node = |id: &str, ty: &str, scope: &[&str], boundary: Value, trigger: bool| json!({
            "id": id, "nodeType": ty, "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {"isTrigger": trigger}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false
        });
        let edge = |source: &str, sp: &str, target: &str, tp: &str| json!({
            "id": format!("{source}.{sp}->{target}.{tp}"), "source": source, "target": target,
            "sourceHandle": sp, "targetHandle": tp
        });
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("db", "T", &[], Value::Null, false),
                node("live", "T", &[], Value::Null, true),
                node("g__in", bt::PASSTHROUGH, &[], json!({"groupId": "g", "role": "In"}), false),
                node("g.make", "T", &["g"], Value::Null, false),
                node("g__out", bt::PASSTHROUGH, &[], json!({"groupId": "g", "role": "Out"}), false),
                node("s__in", bt::CALL_IN, &[], json!({"groupId": "s", "role": "In"}), false),
                node("s__out", bt::CALL_OUT, &[], json!({"groupId": "s", "role": "Out"}), false),
                node("S__in", bt::INCLUDE_IN, &[], json!({"groupId": "S", "role": "In"}), false),
                node("S.query", "T", &["S"], Value::Null, false),
                node("S.inner__in", bt::CALL_IN, &["S"], json!({"groupId": "S.inner", "role": "In"}), false),
                node("S.inner__out", bt::CALL_OUT, &["S"], json!({"groupId": "S.inner", "role": "Out"}), false),
                node("S__out", bt::INCLUDE_OUT, &[], json!({"groupId": "S", "role": "Out"}), false),
                node("I__in", bt::INCLUDE_IN, &[], json!({"groupId": "I", "role": "In"}), false),
                node("I.deep", "T", &["I"], Value::Null, false),
                node("I__out", bt::INCLUDE_OUT, &[], json!({"groupId": "I", "role": "Out"}), false),
                node("stray", "T", &[], Value::Null, false),
            ],
            "edges": [
                edge("db", "access", "g__in", "db"), edge("g__in", "db", "g.make", "account"),
                edge("live", "method", "g__in", "_should_flow"),
                edge("db", "access", "s__in", "db"), edge("s__in", "db", "S__in", "db"),
                edge("S__in", "db", "S.query", "account"), edge("S__in", "db", "S.inner__in", "db"),
                edge("S.inner__in", "db", "I__in", "db"), edge("I__in", "db", "I.deep", "account"),
                edge("live", "method", "s__in", "_should_flow"),
                edge("S.query", "count", "S.inner__in", "_should_flow"),
                edge("stray", "out", "live", "setting"),
            ],
            "groups": [
                {"id": "g", "kind": "group", "nodeIds": ["g.make"]},
                {"id": "s", "kind": "call", "body": "S", "nodeIds": []},
                {"id": "S", "kind": "body", "nodeIds": ["S.query"]},
                {"id": "S.inner", "kind": "call", "body": "I", "nodeIds": [], "parentGroupId": "S"},
                {"id": "I", "kind": "body", "nodeIds": ["I.deep"]}
            ]
        })).unwrap()
    }

    /// `g._should_flow = live.method` means "run the group once the
    /// route fired", the way it means "run the node" on a node: the
    /// fire's program holds the group's members and what they need,
    /// the database included. Through a call site the same wire runs
    /// the included file, and a `_should_flow` inside that file runs
    /// the file it includes in turn.
    #[test]
    fn a_should_flow_into_a_door_runs_everything_behind_it() {
        let project = gated_program();
        let fire = RunSelection::carve(&project, &SelectionBounds { fire: Some("live".into()), ..Default::default() }).unwrap();
        for place in [top("db"), top("live"), top("g__in"), top("g.make"), top("s__in"),
            at("S__in", &["s"]), at("S.query", &["s"]), at("S.inner__in", &["s"]),
            at("I__in", &["s", "S.inner"]), at("I.deep", &["s", "S.inner"])] {
            assert!(fire.nodes.contains(&place), "{place:?} missing from {:?}", fire.nodes);
        }
        assert!(fire.edges.contains(&top("db.access->g__in.db")) && fire.edges.contains(&top("g__in.db->g.make.account")));
        assert!(fire.edges.contains(&at("S__in.db->S.query.account", &["s"])));
        assert!(fire.edges.contains(&at("I__in.db->I.deep.account", &["s", "S.inner"])));
        assert_eq!(fire.boundary_ports[&top("g__in")], names(&["db", "_should_flow"]));
        assert_eq!(fire.boundary_ports[&at("I__in", &["s", "S.inner"])], names(&["db", "_should_flow"]));
        // Setup-only producers stay out of a fire, as before.
        assert!(!fire.nodes.contains(&top("stray")));
        let roots: BTreeSet<Located> = fire.roots(&project).into_iter().collect();
        assert!(roots.contains(&top("db")) && roots.contains(&top("live")), "{roots:?}");
    }

    /// What a trigger's `_should_flow` runs, runs ONCE at activation: the
    /// setup phase walks up from every trigger, so the node is in it, and
    /// a fire walks down from the trigger and stops at triggers on the way
    /// up, so it is not. That is the documented way to create tables
    /// before a program serves, and it holds only if both walks agree.
    #[test]
    fn a_node_feeding_a_triggers_gate_runs_at_setup_and_never_on_a_fire() {
        let mut project = gated_program();
        project.edges.push(serde_json::from_value(json!({
            "id": "db.access->live._should_flow", "source": "db", "sourceHandle": "access",
            "target": "live", "targetHandle": "_should_flow"
        })).unwrap());
        let setup = RunSelection::setup(&project, &[top("live")]).unwrap();
        assert!(setup.nodes.contains(&top("db")), "the trigger's gate source is in the setup program: {:?}", setup.nodes);
        let fire = RunSelection::carve(&project, &SelectionBounds { fire: Some("live".into()), ..Default::default() }).unwrap();
        assert!(!fire.edges.iter().any(|wire| wire.id == "db.access->live._should_flow"),
            "a wire into the trigger is not re-read on a fire: {:?}", fire.edges);
    }

    /// The same doors reached on a data port keep the port-by-port
    /// walk: a fire that feeds `db` into the group runs only what reads
    /// it, so a precise cut stays precise.
    #[test]
    fn a_data_port_on_a_door_still_walks_port_by_port() {
        let mut project = gated_program();
        project.edges.retain(|edge| edge.target_handle.as_deref() != Some("_should_flow"));
        project.edges.push(serde_json::from_value(json!({
            "id": "live.method->g__in.when", "source": "live", "sourceHandle": "method", "target": "g__in", "targetHandle": "when"
        })).unwrap());
        let fire = RunSelection::carve(&project, &SelectionBounds { fire: Some("live".into()), ..Default::default() }).unwrap();
        assert!(fire.nodes.contains(&top("g__in")));
        assert!(!fire.nodes.contains(&top("g.make")), "nothing reads `when`, so the member is not pulled in: {:?}", fire.nodes);
        assert!(!fire.nodes.contains(&at("S.query", &["s"])));
    }
}
