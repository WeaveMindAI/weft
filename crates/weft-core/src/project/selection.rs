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
use super::graph::{GraphView, ProjectGraph};
use super::{boundary_in_id, Edge, GroupBoundaryRole, GroupKind, NodeDefinition, ProjectDefinition};

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
    pub input_origins: BTreeMap<Located, BTreeMap<String, crate::ExecutionId>>,
}

#[derive(Debug, Clone, Default)]
pub struct SelectionBounds {
    pub from: Vec<String>,
    pub emit: Vec<String>,
    pub target: Vec<String>,
    pub before: Vec<String>,
    pub group: Option<String>,
    pub fire: Option<String>,
    /// Starts (a `from` entry or the `group`) whose feeders run too: for
    /// each start port not handed a value (the set beside it), the node
    /// that feeds it, found through any doors on the way, and nothing
    /// above that node.
    pub feed: Vec<(String, BTreeSet<String>)>,
    /// Infra places the run reads only baked outputs of, every one saved
    /// (`crate::infra::bake::covered`): each one this run reaches does not
    /// run, its saved values go out in its place, and nothing above it
    /// runs for its sake.
    pub baked: BTreeSet<Located>,
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

fn crossing<'a>(g: &'a impl GraphView, edge: &Edge) -> Crossing<'a> {
    let boundary = |id: &str| g.node(id).and_then(|n| n.group_boundary.as_ref());
    let (Some(source), Some(target)) = (boundary(&edge.source), boundary(&edge.target)) else { return Crossing::None };
    match (&source.role, &target.role) {
        (GroupBoundaryRole::In, GroupBoundaryRole::In) if g.body_of(&source.group_id) == Some(target.group_id.as_str()) =>
            Crossing::Into(&source.group_id),
        (GroupBoundaryRole::Out, GroupBoundaryRole::Out) if g.body_of(&target.group_id) == Some(source.group_id.as_str()) =>
            Crossing::OutOf(&target.group_id),
        _ => Crossing::None,
    }
}

/// The place at the other end of `edge` when this end is at `from`:
/// entering a body pushes the site, leaving one pops it. A wire out of
/// a body belongs to the site the walk is under; another site's wire
/// on the same boundary is not on this path, and is `None`.
fn step(g: &impl GraphView, from: &Located, edge: &Edge, direction: Direction) -> Option<Located> {
    let other = match direction {
        Direction::Upstream => &edge.source,
        Direction::Downstream => &edge.target,
    };
    let at = Located::new(other.clone(), from.path.clone());
    Some(match (crossing(g, edge), direction) {
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
    let edge = project.edge(&wire.id)?;
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
fn incoming<'a>(g: &'a impl GraphView, at: &Located) -> impl Iterator<Item = (&'a Edge, Located, Located)> + 'a {
    let edges: Vec<&Edge> = g.edges_into(&at.id).collect();
    let at = at.clone();
    edges.into_iter().filter_map(move |edge| {
        let source = step(g, &at, edge, Direction::Upstream)?;
        let place = edge_place(edge, &source, &at);
        Some((edge, source, place))
    })
}

/// Every place reached from `seeds` by following wires backward, seeds
/// included: the plain data walk over PLACES. A body entered through a
/// site is walked under that site alone (see `step`), so an infra node
/// inside a file included twice is reached once per call, and a wire
/// another caller put on the same body boundary is never taken.
///
/// Wires only. Nothing structural is pulled in (no enclosing group's
/// door, no gate feeding it, no loop taken whole), which is what tells
/// this walk apart from [`RunSelection::dependencies`]: that one is the
/// run's shape, this one is what a value's path runs through.
pub fn upstream_by_wires(g: &ProjectGraph, seeds: &[Located]) -> BTreeSet<Located> {
    let mut reached = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut pending: Vec<(Located, Option<String>)> = seeds.iter().map(|place| (place.clone(), None)).collect();
    while let Some((place, port)) = pending.pop() {
        if !visited.insert((place.clone(), port.clone())) { continue; }
        reached.insert(place.clone());
        pending.extend(upstream_of(g, &place, port.as_deref()));
    }
    reached
}

/// The wires a walk arriving at `place` on `port` follows backward, each
/// with its source's place and the port the walk arrives on there. A
/// node takes every wire into it. An ordinary boundary takes only the
/// wires into `port`: its ports are walked one at a time, so what feeds
/// a group's `y` never reaches a member reading its `x`, and what feeds
/// its gate reaches no member at all.
fn upstream_of(g: &ProjectGraph, place: &Located, port: Option<&str>) -> Vec<(Located, Option<String>)> {
    let ordinary = g.is_ordinary_boundary(&place.id);
    incoming(g, place)
        .filter(|(edge, _, _)| !ordinary || port.is_none_or(|p| edge.target_handle.as_deref().unwrap_or("default") == p))
        .map(|(edge, source, _)| (source, port_of(g, &edge.source, edge.source_handle.as_deref())))
        .collect()
}

/// The nodes `feed` asks to run: for each fed start, the node feeding
/// each of its input ports that was not handed a value, found by
/// following the wire outward through any doors on the way, and those
/// doors (a feeder inside a group hands its value out through the
/// group's output door). Only that node; what feeds IT stays outside
/// unless the cut already reaches it.
fn feeders_of(
    g: &ProjectGraph,
    feed: &[(String, BTreeSet<String>)],
    entries: &BTreeSet<Located>,
    group: Option<&str>,
) -> Result<BTreeSet<Located>, String> {
    let mut feeders = BTreeSet::new();
    for (spelled, handed) in feed {
        let start = start_node_in(g, spelled)?;
        let group_start = group.is_some_and(|group| start_node_in(g, group).is_ok_and(|place| place == start));
        if !entries.contains(&start) && !group_start {
            return Err(format!("--feed {spelled}: only a start can be fed; name it with --from or --group too"));
        }
        let node = g.node(&start.id).ok_or_else(|| format!("unknown node '{spelled}'"))?;
        let mut pending: Vec<(Located, String)> = node.inputs.iter()
            .filter(|input| !crate::exec::skip::is_gate_port(&input.name) && !handed.contains(&input.name))
            .map(|input| (start.clone(), input.name.clone())).collect();
        let mut visited = BTreeSet::new();
        while let Some((place, port)) = pending.pop() {
            if !visited.insert((place.clone(), port.clone())) { continue; }
            for (edge, source, _) in incoming(g, &place) {
                if edge.target_handle.as_deref().unwrap_or("default") != port { continue; }
                if g.is_ordinary_boundary(&source.id) {
                    // A door on the way runs too: the value crosses it. A
                    // group's output door is how a feeder inside that group
                    // hands its value out, and nothing else would bring it.
                    feeders.insert(source.clone());
                    pending.push((source, edge.source_handle.as_deref().unwrap_or("default").to_string()));
                } else if let Some(each) = loop_of(g, &source) {
                    // A loop's result comes out of the loop as a whole: it
                    // runs whole, and never cut inside.
                    feeders.extend(members_in(g, &each, &source.path));
                } else {
                    feeders.insert(source);
                }
            }
        }
    }
    Ok(feeders)
}

/// The loop whose boundary `place` is, if it is one.
fn loop_of(g: &ProjectGraph, place: &Located) -> Option<String> {
    let boundary = g.node(&place.id)?.group_boundary.as_ref()?;
    g.is_loop(&boundary.group_id).then(|| boundary.group_id.clone())
}

/// Where a value that arrives at a port ends up needed: the input that
/// would leave its node skipped without it, and the first start of the
/// run the value passes on the way (the door a person hands it at).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Need {
    pub needer: (Located, String),
    pub start: Option<(Located, String)>,
    /// The `@require_one_of` set the needer belongs to, when that set is
    /// why it is needed (every other member gets nothing); `None` when
    /// the input is required on its own.
    pub one_of: Option<Vec<String>>,
}

/// A gate a run's start sits behind that cannot open in the run
/// ([`RunSelection::shut_gates`]): the door whose `_should_flow` it is,
/// and the unfired triggers every source of its value ends at (none
/// when the value comes from outside the run).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutGate {
    pub door: Located,
    pub triggers: Vec<Located>,
}

/// The input a value arriving at `port` of the node at `at` ends up
/// needed by, or `None` when nothing that needs it lies on its path.
///
/// A node answers for itself: its input is needed when it is required
/// with no default, or when it belongs to a `@require_one_of` set whose
/// every other member gets nothing (`fed(place, port)` says whether an
/// input of the node at `place` gets a value some other way). A group, a
/// call site or a body boundary requires nothing of its own (its only
/// say over whether it runs is its gate), so the walk follows the port
/// through it, and through every boundary nested behind it, until it
/// reaches nodes. A loop's In requires the ports it iterates or carries
/// (with no list there is no iteration), which the compiler records as
/// that port's `required`, so a loop answers like a node for those and
/// is walked through for the rest.
///
/// `counts(place, port)` says where a value arriving there still matters:
/// everywhere for the compiler; for a run, only places the run executes,
/// never a trigger (a fired one reads its bake, the others close), and not
/// past a start that was handed a value for that port. `is_start` marks
/// the run's starts, so the answer can name the door to hand a value at.
pub fn required_consumer(
    project: &ProjectDefinition,
    at: &Located,
    port: &str,
    counts: &dyn Fn(&Located, &str) -> bool,
    is_start: &dyn Fn(&Located) -> bool,
    fed: &dyn Fn(&Located, &str) -> bool,
) -> Option<Need> {
    let mut pending = vec![(at.clone(), port.to_string(), None::<(Located, String)>)];
    let mut visited = BTreeSet::new();
    while let Some((place, port, start)) = pending.pop() {
        if !visited.insert((place.clone(), port.clone())) { continue; }
        if crate::exec::skip::is_gate_port(&port) || !counts(&place, &port) { continue; }
        let Some(node) = project.node(&place.id) else { continue };
        let start = start.or_else(|| is_start(&place).then(|| (place.clone(), port.clone())));
        let input = node.inputs.iter().find(|p| p.name == port);
        if input.is_some_and(|input| input.required && input.default.is_none()) {
            return Some(Need { needer: (place, port), start, one_of: None });
        }
        let last_of = node.features.one_of_required.iter()
            .find(|set| set.contains(&port) && set.iter().all(|other| *other == port || !fed(&place, other)));
        if let Some(set) = last_of {
            return Some(Need { needer: (place, port), start, one_of: Some(set.clone()) });
        }
        if !super::boundary_types::is_boundary(&node.node_type) { continue; }
        for (edge, target, _) in outgoing(project, &place) {
            if edge.source_handle.as_deref().unwrap_or("default") != port { continue; }
            pending.push((target, edge.target_handle.as_deref().unwrap_or("default").to_string(), start.clone()));
        }
    }
    None
}

/// The wires out of `at` that are on its path, each with the place of
/// its target and the wire's own place.
fn outgoing<'a>(g: &'a impl GraphView, at: &Located) -> impl Iterator<Item = (&'a Edge, Located, Located)> + 'a {
    let edges: Vec<&Edge> = g.edges_out_of(&at.id).collect();
    let at = at.clone();
    edges.into_iter().filter_map(move |edge| {
        let target = step(g, &at, edge, Direction::Downstream)?;
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
        self.fed(project, at, edge)
    }

    fn fed(&self, g: &impl GraphView, at: &Located, edge: &Edge) -> bool {
        step(g, at, edge, Direction::Upstream).is_some_and(|source| self.edges.contains(&edge_place(edge, &source, at))
            && (self.nodes.contains(&source) || self.suppliers.contains(&source)))
    }

    pub fn dispatchable_nodes(&self) -> std::collections::HashSet<Located> {
        self.nodes.iter().cloned().collect()
    }

    /// Every place downstream of `starts`, the starts included.
    pub fn downstream(project: &ProjectDefinition, starts: &[Located]) -> BTreeSet<Located> {
        walk(&ProjectGraph::new(project), starts, Direction::Downstream, &|_| false)
    }

    /// Rebuild a selected node set's data paths and controls, preserving loops.
    pub fn restricted(project: &ProjectDefinition, nodes: BTreeSet<Located>) -> Result<Self, String> {
        let g = ProjectGraph::new(project);
        for place in &nodes {
            if g.node(&place.id).is_none() {
                return Err(format!("unknown node '{}'", place.id));
            }
        }
        let selection = Self::from_nodes(&g, nodes, BTreeSet::new());
        selection.validate_loops_in(&g)?;
        Ok(selection)
    }

    /// The whole program: every node at every place it can run.
    pub fn whole(project: &ProjectDefinition) -> Self {
        let g = ProjectGraph::new(project);
        Self::from_nodes(&g, every_place_in(&g), BTreeSet::new())
    }

    /// History relevant to this cut, including ancestors before its starts.
    /// An unrelated branch is not inherited merely because the seed retained it.
    pub fn history_nodes(&self, project: &ProjectDefinition) -> BTreeSet<Located> {
        let g = ProjectGraph::new(project);
        let starts = self.nodes.iter().chain(&self.suppliers).flat_map(|place| {
            if g.is_ordinary_boundary(&place.id) {
                self.boundary_ports.get(place).into_iter().flatten()
                    .map(|port| (place.clone(), Some(port.clone()))).collect::<Vec<_>>()
            } else { vec![(place.clone(), None)] }
        }).collect();
        walk_ports(&g, starts, Direction::Upstream, &|_| false)
    }

    /// Keep the authored cut while replacing reusable results with their
    /// frontier supply. History that feeds no new work adds no control work.
    pub fn with_reused(&self, project: &ProjectDefinition, reused: &BTreeSet<Located>) -> Self {
        let g = ProjectGraph::new(project);
        let nodes: BTreeSet<_> = self.nodes.difference(reused).cloned().collect();
        let mut suppliers = self.suppliers.clone();
        for place in &nodes {
            suppliers.extend(incoming(&g, place)
                .filter(|(_, source, wire)| self.edges.contains(wire) && reused.contains(source))
                .map(|(_, source, _)| source));
        }
        for place in nodes.iter().chain(&self.suppliers) {
            suppliers.extend(gates_of(&g, place).into_iter()
                .map(|gate| Located::new(boundary_in_id(&gate.id), gate.path))
                .filter(|gate| reused.contains(gate)));
        }
        let mut selected = Self::from_nodes(&g, nodes, suppliers);
        selected.nodes.retain(|place| self.nodes.contains(place) && !reused.contains(place));
        let targets: BTreeSet<Located> = selected.nodes.iter()
            .flat_map(|place| incoming(&g, place).map(|(_, _, wire)| wire)).collect();
        selected.edges.retain(|wire| self.edges.contains(wire) && targets.contains(wire));
        selected.input = self.input.clone();
        selected.input_origins = self.input_origins.clone();
        selected
    }

    /// Manual carving and trigger fire use identical walks and bounds.
    pub fn carve(project: &ProjectDefinition, bounds: &SelectionBounds) -> Result<Self, String> {
        Self::carve_in(&ProjectGraph::new(project), bounds)
    }

    /// [`Self::carve`] over a program already indexed, for a caller that
    /// carves many runs from one program.
    pub fn carve_in(g: &ProjectGraph, bounds: &SelectionBounds) -> Result<Self, String> {
        let project = g.project();
        // Every bound is spelled from the top of the program, through
        // the call sites (`triage.up`), and resolves to one place.
        let mut entries = BTreeSet::new();
        for id in &bounds.from {
            if !entries.insert(start_node_in(g, id)?) {
                return Err(format!("starting entry '{id}' is supplied more than once"));
            }
        }
        let mut target = Vec::new();
        let mut before = Vec::new();
        let emits: Vec<Located> = bounds.emit.iter().map(|id| locate(g, id)).collect::<Result<_, _>>()?;
        let fire: Option<Located> = bounds.fire.as_ref().map(|id| locate(g, id)).transpose()?;
        let mut excluded: BTreeSet<Located> = emits.iter().cloned().collect();
        let feeders = feeders_of(g, &bounds.feed, &entries, bounds.group.as_deref())?;
        for (inclusive, ends) in [(true, &bounds.target), (false, &bounds.before)] {
            for spelled in ends {
                let place = locate(g, spelled)?;
                let endpoints: Vec<Located> = if g.group(&place.id).is_some() {
                    validate_group_place(g, &place, spelled)?;
                    members_in(g, &place.id, &place.path)
                } else {
                    validate_endpoint_in(g, spelled)?;
                    vec![place]
                };
                if inclusive { target.extend(endpoints); } else {
                    excluded.extend(endpoints.iter().cloned());
                    before.extend(endpoints);
                }
            }
        }
        for spelled in bounds.emit.iter().chain(bounds.fire.iter()) {
            validate_endpoint_in(g, spelled)?;
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
            if !g.is_trigger(&fire.id) {
                return Err(format!("'{}' is not a trigger", fire.id));
            }
        }
        let mut selection = if let Some(group) = &bounds.group {
            if !bounds.from.is_empty() || !bounds.emit.is_empty() || !bounds.target.is_empty()
                || !bounds.before.is_empty()
            {
                return Err("group cannot be combined with from, emit, target, or before".into());
            }
            let place = locate(g, group)?;
            if g.group(&place.id).is_none() {
                return Err(format!("unknown group '{}'", place.id));
            }
            validate_group_place(g, &place, group)?;
            let mut nodes: BTreeSet<Located> = members_in(g, &place.id, &place.path).into_iter().collect();
            if fire.as_ref().is_some_and(|fire| !nodes.contains(fire)) {
                return Err("the fired trigger is outside the selected group".into());
            }
            nodes.extend(feeders);
            let supplied: BTreeSet<Located> = nodes.intersection(&bounds.baked).cloned().collect();
            nodes.retain(|place| !supplied.contains(place));
            drop_only_feeding(g, &mut nodes, &supplied, &supplied, &fire.iter().cloned().collect(), &BTreeSet::new());
            Self::from_nodes(g, nodes, supplied)
        } else {
            let is_trigger = |place: &Located| g.is_trigger(&place.id);
            let stops = |place: &Located| {
                is_trigger(place) || entries.contains(place) || emits.contains(place) || bounds.baked.contains(place)
            };
            let starts: Vec<Located> = entries.iter().cloned().chain(emits.iter().cloned()).chain(fire.iter().cloned()).collect();
            let mut nodes = if starts.is_empty() {
                every_place_in(g)
            } else {
                let downstream = walk(g, &starts, Direction::Downstream, &|_| false);
                let consumers: Vec<Located> = downstream.iter().filter(|place| !g.is_ordinary_boundary(&place.id)).cloned().collect();
                let mut nodes = walk(g, &consumers, Direction::Upstream, &stops);
                nodes.extend(downstream);
                nodes
            };
            nodes.extend(feeders.iter().cloned());
            let mut suppliers: BTreeSet<Located> = emits.iter().cloned().collect();
            // An endpoint's walk stops at the starts, so nothing above them
            // runs. A start that lies downstream of another start is no
            // boundary though: the cut runs from the one furthest up, so
            // the walk passes it and every start upstream stays. A fed
            // start's feeders are kept as well, one level and no further.
            let others_reach = |start: &Located| entries.iter().chain(&emits)
                .filter(|other| *other != start)
                .any(|other| walk(g, std::slice::from_ref(other), Direction::Downstream, &|_| false).contains(start));
            let inner: BTreeSet<Located> = entries.iter().filter(|start| others_reach(start)).cloned().collect();
            let bounded = |place: &Located| is_trigger(place)
                || emits.contains(place)
                || (entries.contains(place) && !inner.contains(place));
            for ends in [&target, &before] {
                if !ends.is_empty() {
                    let mut allowed = walk(g, ends, Direction::Upstream, &bounded);
                    allowed.extend(feeders.iter().cloned());
                    nodes.retain(|place| allowed.contains(place));
                    suppliers.retain(|place| allowed.contains(place));
                }
            }
            suppliers.retain(|place| !before.contains(place));
            nodes.retain(|place| !excluded.contains(place));
            // A baked place the run reaches supplies its saved values.
            let baked: BTreeSet<Located> = nodes.iter().filter(|place| bounds.baked.contains(place)).cloned().collect();
            for place in &baked {
                nodes.remove(place);
                suppliers.insert(place.clone());
            }
            let named: BTreeSet<Located> = entries.iter().chain(fire.iter()).chain(target.iter()).cloned().collect();
            drop_only_feeding(g, &mut nodes, &baked, &suppliers, &named, &before.iter().cloned().collect());
            let selection = Self::from_nodes(g, nodes, suppliers);
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
            let into_trigger: BTreeSet<&str> = project.edges.iter().filter(|edge| g.is_trigger(&edge.target))
                .map(|edge| edge.id.as_str()).collect();
            selection.edges.retain(|wire| !into_trigger.contains(wire.id.as_str()));
        }
        selection.validate_loops_in(g)?;
        Ok(selection)
    }

    /// Data dependencies and enclosing controls, with whole-loop expansion.
    pub fn dependencies(project: &ProjectDefinition, targets: &[Located]) -> Self {
        Self::dependencies_in(&ProjectGraph::new(project), targets)
    }

    /// [`Self::dependencies`] over a program already indexed.
    pub fn dependencies_in(g: &ProjectGraph, targets: &[Located]) -> Self {
        let walked = walk(g, targets, Direction::Upstream, &|_| false);
        Self::from_nodes(g, walked, BTreeSet::new())
    }

    /// Both setup phases stop inclusively at their targets.
    pub fn setup(project: &ProjectDefinition, targets: &[Located]) -> Result<Self, String> {
        let g = ProjectGraph::new(project);
        for target in targets {
            validate_place(&g, target, &super::address_of(project, &target.id, &target.path))?;
        }
        let selection = Self::dependencies_in(&g, targets);
        selection.validate_loops_in(&g)?;
        Ok(selection)
    }

    /// The gates that leave `start` shut: its own `_should_flow`, and
    /// that of every group it runs inside, when the gate is wired, handed
    /// no value, and every branch its value could come from in this run
    /// ends at a trigger that does not fire (`fired` is the one that
    /// does), or outside the run. Such a gate closes, so the start skips
    /// whatever it is fed. Empty when every gate could open: a branch
    /// counts as open when a node on it has nothing to wait on, a
    /// supplied or reused source feeds it, or a value is handed on the
    /// way. Deciding that from the graph alone, a branch counts as open
    /// whenever any of its inputs might be, so a refusal built on this is
    /// never wrong.
    ///
    /// Each gate is named by its door, because that is where a value
    /// opens it: the start's own, or the enclosing group's In, which only
    /// a start AT that group can be handed.
    pub fn shut_gates(&self, project: &ProjectDefinition, start: &Located, fired: Option<&Located>) -> Vec<ShutGate> {
        let g = ProjectGraph::new(project);
        let mut doors: BTreeSet<Located> = gates_of(&g, start).into_iter()
            .map(|gate| Located::new(boundary_in_id(&gate.id), gate.path)).collect();
        doors.insert(start.clone());
        let gate = crate::exec::skip::SHOULD_FLOW_PORT;
        doors.into_iter().filter_map(|door| {
            if self.input.get(&door).is_some_and(|ports| ports.contains_key(gate)) { return None; }
            if !g.edges_into(&door.id).any(|edge| edge.target_handle.as_deref() == Some(gate)) { return None; }
            let mut triggers = BTreeSet::new();
            (!self.could_carry(&g, &door, gate, fired, &mut BTreeSet::new(), &mut triggers))
                .then(|| ShutGate { door, triggers: triggers.into_iter().collect() })
        }).collect()
    }

    /// Whether a value could reach `port` of `at` in this run (see
    /// [`Self::shut_gates`]), collecting the unfired triggers that every
    /// dead branch ends at. A wire counts only when the runtime would
    /// read it ([`Self::fed_by`]): the run's wire set also holds wires
    /// into a selected node whose source does not run, and a node whose
    /// every wire is like that is a root that fires on its own.
    fn could_carry(
        &self,
        g: &ProjectGraph,
        at: &Located,
        port: &str,
        fired: Option<&Located>,
        seen: &mut BTreeSet<(Located, String)>,
        triggers: &mut BTreeSet<Located>,
    ) -> bool {
        if !seen.insert((at.clone(), port.to_string())) { return false; }
        if self.input.get(at).is_some_and(|ports| ports.contains_key(port)) { return true; }
        let wires: Vec<(Edge, Located)> = incoming(g, at)
            .filter(|(edge, _, _)| edge.target_handle.as_deref().unwrap_or("default") == port && self.fed(g, at, edge))
            .map(|(edge, source, _)| (edge.clone(), source)).collect();
        wires.into_iter().any(|(edge, source)| {
            if self.suppliers.contains(&source) { return true; }
            let Some(node) = g.node(&source.id) else { return false };
            if node.features.is_trigger && fired != Some(&source) {
                triggers.insert(source);
                return false;
            }
            if g.is_ordinary_boundary(&source.id) {
                let through = edge.source_handle.as_deref().unwrap_or("default");
                return self.could_carry(g, &source, through, fired, seen, triggers);
            }
            // A node: open when nothing it reads comes over a wire of the
            // run, or when any wired input could carry.
            let ports: BTreeSet<String> = incoming(g, &source)
                .filter(|(edge, _, _)| self.fed(g, &source, edge))
                .map(|(edge, _, _)| edge.target_handle.as_deref().unwrap_or("default").to_string()).collect();
            ports.is_empty() || ports.iter().any(|port| self.could_carry(g, &source, port, fired, seen, triggers))
        })
    }

    /// Whether something in the run feeds `port` of the node at `at`.
    pub fn has_supplier(&self, project: &ProjectDefinition, at: &Located, port: &str) -> bool {
        project.edges_into(&at.id).any(|edge| edge.target_handle.as_deref().unwrap_or("default") == port
            && self.fed_by(project, at, edge))
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
        let g = ProjectGraph::new(project);
        self.nodes.iter()
            .filter(|place| !in_loop_body(&g, place))
            .filter(|place| !g.edges_into(&place.id).any(|edge| self.fed(&g, place, edge)))
            .cloned().collect()
    }

    pub fn validate_loops(&self, project: &ProjectDefinition) -> Result<(), String> {
        self.validate_loops_in(&ProjectGraph::new(project))
    }

    fn validate_loops_in(&self, g: &ProjectGraph) -> Result<(), String> {
        // Each loop at each place is checked once, however many of its
        // members the run holds.
        let mut checked = BTreeSet::new();
        for place in &self.nodes {
            for group in loops_around(g, place) {
                if !checked.insert(group.clone()) { continue; }
                let members = members_in(g, &group.id, &group.path);
                if members.iter().any(|member| !self.nodes.contains(member)) {
                    return Err(format!("cannot cut inside loop '{}'; select the whole loop", group.id));
                }
            }
        }
        Ok(())
    }

    fn from_nodes(g: &ProjectGraph, mut nodes: BTreeSet<Located>, suppliers: BTreeSet<Located>) -> Self {
        let mut gates = BTreeSet::new();
        let is_trigger = |place: &Located| g.is_trigger(&place.id);
        loop {
            let before = nodes.len();
            for place in nodes.iter().chain(&suppliers) {
                gates.extend(gates_of(g, place));
            }
            for gate in &gates {
                let boundary = Located::new(boundary_in_id(&gate.id), gate.path.clone());
                if suppliers.contains(&boundary) { continue; }
                nodes.insert(boundary.clone());
                let sources: Vec<_> = incoming(g, &boundary)
                    .filter(|(edge, _, _)| edge.target_handle.as_deref() == Some("_should_flow"))
                    .map(|(edge, source, _)| (source, Some(edge.source_handle.as_deref().unwrap_or("default").to_string())))
                    .collect();
                // A supplier's values are handed in, so nothing above it
                // runs for the gate either (a baked place, an `--emit`).
                let stops = |place: &Located| is_trigger(place) || suppliers.contains(place);
                nodes.extend(walk_ports(g, sources, Direction::Upstream, &stops).into_iter().filter(|place| !suppliers.contains(place)));
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
            .filter(|place| !g.is_ordinary_boundary(&place.id))
            .flat_map(|place| incoming(g, place)).collect();
        for place in nodes.iter().filter(|place| g.is_ordinary_boundary(&place.id)) {
            let node = g.node(&place.id).expect("selected node exists");
            for (edge, source, wire) in incoming(g, place)
                .filter(|(_, source, _)| nodes.contains(source) || suppliers.contains(source))
            {
                boundary_ports.entry(place.clone()).or_default().insert(edge.target_handle.as_deref().unwrap_or("default").into());
                pending.push((edge, source, wire));
            }
            let terminal = !outgoing(g, place).any(|(_, target, _)| nodes.contains(&target));
            if terminal {
                boundary_ports.entry(place.clone()).or_default().extend(node.port_literals.keys().cloned());
            }
        }
        for gate in &gates {
            let boundary = Located::new(boundary_in_id(&gate.id), gate.path.clone());
            boundary_ports.entry(boundary.clone()).or_default().insert("_should_flow".into());
            pending.extend(incoming(g, &boundary)
                .filter(|(edge, _, _)| edge.target_handle.as_deref() == Some("_should_flow")));
        }
        while let Some((edge, source, wire)) = pending.pop() {
            if !edges.insert(wire) { continue; }
            if g.is_ordinary_boundary(&source.id) && nodes.contains(&source) {
                let port = edge.source_handle.as_deref().unwrap_or("default");
                boundary_ports.entry(source.clone()).or_default().insert(port.into());
                pending.extend(incoming(g, &source)
                    .filter(|(edge, _, _)| edge.target_handle.as_deref().unwrap_or("default") == port));
            }
        }
        Self { nodes, edges, boundary_ports, gates, suppliers, input: BTreeMap::new(), input_origins: BTreeMap::new() }
    }
}

/// Take out of `nodes` what only feeds the `baked` places: a node above
/// one of them that reaches nothing kept without passing through a
/// supplier (a baked place's saved values go out in its place, so its
/// feeders have nothing to feed). Kept are the other nodes, `named` (what
/// the run was asked to start at or run), the `goals` the run stops before
/// (what it exists to feed), and anything inside a loop, which runs whole.
fn drop_only_feeding(
    g: &ProjectGraph,
    nodes: &mut BTreeSet<Located>,
    baked: &BTreeSet<Located>,
    suppliers: &BTreeSet<Located>,
    named: &BTreeSet<Located>,
    goals: &BTreeSet<Located>,
) {
    if baked.is_empty() {
        return;
    }
    let starts: Vec<Located> = baked.iter().cloned().collect();
    let candidates: BTreeSet<Located> = walk(g, &starts, Direction::Upstream, &|place| g.is_trigger(&place.id))
        .into_iter()
        .filter(|place| nodes.contains(place) && !named.contains(place) && loops_around(g, place).is_empty())
        .collect();
    if candidates.is_empty() {
        return;
    }
    // One walk up from everything kept, never through a supplier nor into
    // a trigger's inputs (a run never delivers those: a trigger read them
    // when it was set up): a candidate it reaches feeds something kept.
    let kept: Vec<Located> = nodes.difference(&candidates).chain(goals.iter()).filter(|place| !g.is_trigger(&place.id)).cloned().collect();
    let feeds_kept = walk(g, &kept, Direction::Upstream, &|place| suppliers.contains(place) || g.is_trigger(&place.id));
    nodes.retain(|place| !candidates.contains(place) || feeds_kept.contains(place));
}

/// The output ports of `place` that a wire into one of `runs` reads, each
/// wire followed on its path (so a file included twice reads its own
/// site's node, never the other site's). A wire into a trigger is not a
/// read: a run never delivers it (a trigger read its inputs when it was
/// set up).
pub fn ports_read_by(project: &ProjectDefinition, place: &Located, runs: &BTreeSet<Located>) -> BTreeSet<String> {
    let g = ProjectGraph::new(project);
    outgoing(&g, place)
        .filter(|(_, target, _)| runs.contains(target) && !g.is_trigger(&target.id))
        .map(|(edge, _, _)| edge.source_handle.clone().unwrap_or_else(|| "default".into()))
        .collect()
}

/// Every place in the program: the top-level nodes, and each included
/// file's nodes once per site that reaches it, however deep.
pub fn every_place(project: &ProjectDefinition) -> BTreeSet<Located> {
    every_place_in(&ProjectGraph::new(project))
}

/// [`every_place`] over a program already indexed.
pub fn every_place_in(g: &ProjectGraph) -> BTreeSet<Located> {
    let project = g.project();
    let in_a_body = |node: &NodeDefinition| body_around(g, node).is_some();
    let mut places: BTreeSet<Located> = project.nodes.iter().filter(|node| !in_a_body(node))
        .map(|node| Located::top(node.id.clone())).collect();
    for group in project.groups.iter().filter(|group| matches!(group.kind, GroupKind::Call { .. })) {
        let entry = g.node(&boundary_in_id(&group.id));
        if entry.is_some_and(|entry| !in_a_body(entry)) {
            places.extend(members_in(g, &group.id, &[]));
        }
    }
    places
}

/// The groups that gate `place`, each at the place its In runs: the
/// node's own scopes (a boundary's own container among them) at the
/// node's path, and every site on the path at the path above it.
fn gates_of(g: &ProjectGraph, place: &Located) -> Vec<Located> {
    let mut gates: Vec<Located> = place.path.iter().enumerate()
        .map(|(depth, site)| Located::new(site.clone(), place.path[..depth].to_vec())).collect();
    if let Some(node) = g.node(&place.id) {
        gates.extend(node.scope.iter().map(|group| Located::new(group.clone(), place.path.clone())));
        if g.is_ordinary_boundary(&node.id) {
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
fn in_loop_body(project: &impl GraphView, place: &Located) -> bool {
    let scope_has_loop = |id: &str| project.node(id).is_some_and(|n| n.scope.iter().any(|g| project.is_loop(g)));
    scope_has_loop(&place.id) || place.path.iter().any(|site| scope_has_loop(&boundary_in_id(site)))
}

/// The loops around `place`, outermost first, each at the place the
/// loop runs: a loop in the node's own scope, and a loop around any
/// site on its path (a call inside a loop runs whole with the loop).
pub fn enclosing_loops(project: &ProjectDefinition, place: &Located) -> Vec<Located> {
    loops_around(project, place)
}

fn loops_around(project: &impl GraphView, place: &Located) -> Vec<Located> {
    let mut loops = Vec::new();
    for (depth, site) in place.path.iter().enumerate() {
        if let Some(entry) = project.node(&boundary_in_id(site)) {
            loops.extend(entry.scope.iter().filter(|group| project.is_loop(group))
                .map(|group| Located::new(group.clone(), place.path[..depth].to_vec())));
        }
    }
    if let Some(node) = project.node(&place.id) {
        loops.extend(node.scope.iter().chain(node.group_boundary.iter().map(|b| &b.group_id))
            .filter(|group| project.is_loop(group)).map(|group| Located::new(group.clone(), place.path.clone())));
    }
    loops.dedup();
    loops
}

/// A group's members, each at the place it runs when the group is at
/// `path`: its nodes and boundaries at `path`, and behind every call
/// site among them (the group itself, when it is one) the whole body
/// one site deeper. Takes the graph so a caller walking many groups
/// indexes the program once.
pub fn members_in(g: &ProjectGraph, group: &str, path: &[String]) -> Vec<Located> {
    let mut out: Vec<Located> = g.members(group).map(|n| Located::new(n.id.clone(), path.to_vec())).collect();
    for site in g.sites(group) {
        let body = g.body_of(&site.id).expect("a call site names its body");
        let mut deeper = path.to_vec();
        deeper.push(site.id.clone());
        out.extend(members_in(g, body, &deeper));
    }
    out
}

/// A bound as a person wrote it, resolved: the node or group id and the
/// call path it names. An unknown spelling is reported as written.
fn locate(g: &ProjectGraph, spelled: &str) -> Result<Located, String> {
    let (id, path) = super::resolve_address(g.project(), spelled);
    if g.node(&id).is_some() || g.group(&id).is_some() {
        Ok(Located::new(id, path))
    } else {
        Err(format!("unknown node '{spelled}'"))
    }
}

/// The start a spelled bound names, at its place: a group (a call site
/// included) starts at its In boundary; a node starts at itself. Never
/// inside a loop, and never a bare body: a body is started through a
/// site that calls it.
pub fn start_node_in(g: &ProjectGraph, spelled: &str) -> Result<Located, String> {
    let place = locate(g, spelled)?;
    if g.group(&place.id).is_some() {
        validate_group_place(g, &place, spelled)?;
        return Ok(Located::new(boundary_in_id(&place.id), place.path));
    }
    validate_place(g, &place, spelled)?;
    Ok(place)
}

/// A group may be an endpoint (cut at, started, or run as `--group`)
/// under the same rule as a node (`validate_place`): nothing between
/// the top of the program and it runs whole, so no loop around it and
/// none around a site on its path; and a group inside an included file
/// is named through a site, never through the file's own id. A body is
/// never an endpoint: it runs through the site that calls it.
fn validate_group_place(g: &ProjectGraph, place: &Located, spelled: &str) -> Result<(), String> {
    let group = g.group(&place.id).ok_or_else(|| format!("unknown group '{spelled}'"))?;
    if matches!(group.kind, GroupKind::Body) {
        return Err(format!("cannot cut at '{spelled}': it is an included file, which runs through the site that includes it; name the site"));
    }
    let entry = Located::new(boundary_in_id(&group.id), place.path.clone());
    let node = g.node(&entry.id).ok_or_else(|| format!("group '{}' has no entry", group.id))?;
    // The group's own container is not "around" it: a loop is cut whole.
    if let Some(container) = loops_around(g, &entry).into_iter().find(|l| l.id != group.id) {
        return Err(format!("cannot cut at '{spelled}' inside loop '{}'; select the whole loop", container.id));
    }
    if place.path.is_empty() {
        if let Some(body) = body_around(g, node) {
            return Err(inside_a_file(g.project(), spelled, &group.id, &body));
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
fn validate_endpoint_in(g: &ProjectGraph, spelled: &str) -> Result<(), String> {
    validate_place(g, &locate(g, spelled)?, spelled)
}

fn validate_place(g: &ProjectGraph, place: &Located, spelled: &str) -> Result<(), String> {
    let node = g.node(&place.id).ok_or_else(|| format!("unknown node '{spelled}'"))?;
    if let Some(container) = enclosing_loop(g, node) {
        return Err(format!("cannot cut at '{spelled}' inside loop '{container}'; select the whole loop"));
    }
    if place.path.is_empty() {
        if let Some(body) = body_around(g, node) {
            return Err(inside_a_file(g.project(), spelled, &place.id, &body));
        }
    }
    for site in &place.path {
        let site_in = g.node(&boundary_in_id(site)).ok_or_else(|| format!("call site '{site}' has no entry"))?;
        if let Some(container) = enclosing_loop(g, site_in) {
            return Err(format!("cannot cut at '{spelled}': the site '{site}' sits inside loop '{container}'; select the whole loop"));
        }
    }
    Ok(())
}

/// Whether `group` is an included file's body.
pub fn is_body(project: &ProjectDefinition, group: &str) -> bool {
    project.group(group).is_some_and(|g| matches!(g.kind, GroupKind::Body))
}

/// The loop around `node`, if any: a loop's body runs whole, once per
/// iteration, so a run is never cut inside one. The first in the
/// program's group order, when the node is in several.
fn enclosing_loop(g: &ProjectGraph, node: &NodeDefinition) -> Option<String> {
    first_group_around(g, node, |kind| matches!(kind, GroupKind::Loop { .. }))
}

/// The included file's body `node` sits in (as a member or as one of
/// its boundaries), if any.
pub fn enclosing_body(project: &ProjectDefinition, node: &NodeDefinition) -> Option<String> {
    body_around(project, node)
}

fn body_around(project: &impl GraphView, node: &NodeDefinition) -> Option<String> {
    first_group_around(project, node, |kind| matches!(kind, GroupKind::Body))
}

/// The first group, in the program's group order, of a kind `wanted`
/// takes, that `node` is a member or a boundary of.
fn first_group_around(g: &impl GraphView, node: &NodeDefinition, wanted: impl Fn(&GroupKind) -> bool) -> Option<String> {
    let project = g.project();
    let order = |id: &str| project.groups.iter().position(|group| group.id == id);
    node.scope.iter().chain(node.group_boundary.iter().map(|b| &b.group_id))
        .filter(|id| g.group(id).is_some_and(|group| wanted(&group.kind)))
        .filter_map(|id| order(id).map(|at| (at, id)))
        .min()
        .map(|(_, id)| id.clone())
}

fn walk(g: &ProjectGraph, starts: &[Located], direction: Direction, stops: &dyn Fn(&Located) -> bool) -> BTreeSet<Located> {
    walk_ports(g, starts.iter().map(|place| (place.clone(), None)).collect(), direction, stops)
}

/// The walk is over places: entering a body through a site pushes the
/// site, leaving it through the site's other half pops it, and another
/// caller's wire on the same body boundary is never taken (see `step`).
/// A body reached through two sites is walked once per site. A stop is
/// reached and not walked through.
fn walk_ports(g: &ProjectGraph, mut pending: Vec<(Located, Option<String>)>, direction: Direction, stops: &dyn Fn(&Located) -> bool) -> BTreeSet<Located> {
    let mut nodes = BTreeSet::new();
    let mut visited = BTreeSet::new();
    // A loop's members are pushed once per loop place, not once per
    // member reached.
    let mut loops_taken = BTreeSet::new();
    while let Some((place, port)) = pending.pop() {
        if !visited.insert((place.clone(), port.clone())) { continue; }
        nodes.insert(place.clone());
        if stops(&place) { continue; }
        // A loop goes in whole: reaching any part of one pulls in every
        // member and both boundaries, at the loop's place.
        if let Some(group) = loops_around(g, &place).into_iter().next() {
            if loops_taken.insert(group.clone()) {
                for member in members_in(g, &group.id, &group.path) {
                    if !visited.contains(&(member.clone(), None)) { pending.push((member, None)); }
                }
            }
        }
        let ordinary = g.is_ordinary_boundary(&place.id);
        let next: Vec<(Located, Option<String>)> = match direction {
            Direction::Upstream => upstream_of(g, &place, port.as_deref()),
            Direction::Downstream => outgoing(g, &place)
                .filter(|(edge, _, _)| !ordinary || port.as_ref().is_none_or(|p| edge.source_handle.as_deref().unwrap_or("default") == p))
                .map(|(edge, target, _)| (target, port_of(g, &edge.target, edge.target_handle.as_deref()))).collect(),
        };
        pending.extend(next);
        // A `_should_flow` wire into a group's door says "run what is in
        // here", the way the same wire into a node says "run this node":
        // walking forward through it puts the whole group in the run,
        // its body behind a call site included. A data port on the door
        // keeps the port-by-port walk, so a cut stays precise.
        if direction == Direction::Downstream && port.as_deref().is_some_and(crate::exec::skip::is_gate_port) {
            if let Some(group) = gated_group(g, &place) {
                for member in members_in(g, &group, &place.path) {
                    if !visited.contains(&(member.clone(), None)) { pending.push((member, None)); }
                }
            }
        }
    }
    nodes
}

/// The group whose door `place` is: an ordinary In boundary's group,
/// `None` for anything else.
fn gated_group(g: &ProjectGraph, place: &Located) -> Option<String> {
    let node = g.node(&place.id)?;
    let boundary = node.group_boundary.as_ref()?;
    (g.is_ordinary_boundary(&place.id) && boundary.role == GroupBoundaryRole::In).then(|| boundary.group_id.clone())
}

/// The port a walk arrives on at an ordinary boundary (whose ports are
/// walked one at a time); `None` for any other node.
fn port_of(g: &ProjectGraph, node: &str, handle: Option<&str>) -> Option<String> {
    g.is_ordinary_boundary(node).then(|| handle.unwrap_or("default").to_string())
}

/// A run's selection as its record keeps it: the selection itself, and the
/// digest it is stored under. Every run of one trigger of one program has
/// the same selection, so the record keeps each one once, by digest
/// (`run_selection`), and a run's birth names only the digest: that is its
/// written form. Reading a birth back resolves the digest to the selection
/// the reader read with the run ([`RecordedSelection::resolving`]); a
/// birth read with no selection to resolve it fails to decode, naming the
/// digest, rather than reading as a run of the whole program.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedSelection {
    digest: std::sync::Arc<str>,
    selection: std::sync::Arc<RunSelection>,
}

thread_local! {
    /// The selections a decode in progress on this thread may resolve
    /// ([`RecordedSelection::resolving`]).
    static RESOLVABLE: std::cell::RefCell<Vec<RecordedSelection>> = const { std::cell::RefCell::new(Vec::new()) };
}

impl RecordedSelection {
    /// `selection`, with the digest it is kept under.
    pub fn new(selection: RunSelection) -> Self {
        let value = serde_json::to_value(&selection).expect("a selection serializes");
        let digest = super::hash::sha256_hex(super::hash::canonical_json(&value).as_bytes());
        Self { digest: digest.into(), selection: std::sync::Arc::new(selection) }
    }

    /// A selection read back from the record under `digest`.
    pub fn read(digest: String, selection: RunSelection) -> Self {
        Self { digest: digest.into(), selection: std::sync::Arc::new(selection) }
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn selection(&self) -> &std::sync::Arc<RunSelection> {
        &self.selection
    }

    /// Run `decode` with `known` resolvable by digest: a birth decoded
    /// inside it reads its selection from them. Decoding is synchronous,
    /// so the scope is exactly the decode.
    pub fn resolving<R>(known: &[RecordedSelection], decode: impl FnOnce() -> R) -> R {
        struct Restore(Vec<RecordedSelection>);
        impl Drop for Restore {
            fn drop(&mut self) {
                RESOLVABLE.with(|cell| *cell.borrow_mut() = std::mem::take(&mut self.0));
            }
        }
        let _restore = Restore(RESOLVABLE.with(|cell| std::mem::replace(&mut *cell.borrow_mut(), known.to_vec())));
        decode()
    }
}

impl std::ops::Deref for RecordedSelection {
    type Target = RunSelection;

    fn deref(&self) -> &RunSelection {
        &self.selection
    }
}

impl Serialize for RecordedSelection {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.digest)
    }
}

impl<'de> Deserialize<'de> for RecordedSelection {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let digest = String::deserialize(deserializer)?;
        RESOLVABLE
            .with(|cell| cell.borrow().iter().find(|known| *known.digest == *digest).cloned())
            .ok_or_else(|| serde::de::Error::custom(format!("the run's selection {digest} is not on record with it")))
    }
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
        members_in(&ProjectGraph::new(project), group, &[]).into_iter().collect()
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
            assert_eq!(in_loop_body(&project, &top("lonely")), looping);
            assert!(!in_loop_body(&project, &top("g__in")), "a door is not inside its own loop");
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
            assert!(validate_endpoint_in(&ProjectGraph::new(&project), id).unwrap_err().contains("inside loop"));
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
            let start = crate::run_spec::resolve_spec(&from, &project, &Default::default()).unwrap().selection;
            let confined = crate::run_spec::resolve_spec(&group, &project, &Default::default()).unwrap().selection;
            assert_eq!(start.input[&top("g__in")]["x"], json!("backup"));
            assert_eq!(confined.input, start.input);
            assert!(start.nodes.contains(&top("after")));
            assert!(!confined.nodes.contains(&top("after")));
            assert!(top_members(&project, "g").is_subset(&start.nodes));
            assert!(top_members(&project, "g").is_subset(&confined.nodes));
            assert!(!start.nodes.contains(&top("a")));
            let both = crate::run_spec::RunSpec { group: group.group, ..from };
            assert!(crate::run_spec::resolve_spec(&both, &project, &Default::default()).unwrap_err().to_string().contains("cannot be combined"));
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
        let err = validate_endpoint_in(&ProjectGraph::new(&project), "B.n").unwrap_err();
        assert!(err.contains("inside an included file") && err.contains("like `a.n`"), "{err}");
        validate_endpoint_in(&ProjectGraph::new(&project), "a.n").unwrap();
        validate_endpoint_in(&ProjectGraph::new(&project), "b.n").unwrap();
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
        let resolved = crate::run_spec::resolve_spec(&spec, &project, &Default::default()).unwrap();
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
        assert_eq!(start_node_in(&ProjectGraph::new(&project), "a.g").unwrap(), at("B.g__in", &["a"]));
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
        let members: BTreeSet<Located> = members_in(&ProjectGraph::new(&project), "l", &[]).into_iter().collect();
        assert!(members.contains(&at("B.n", &["l.c"])) && members.contains(&top("l.c__in")) && members.contains(&top("l__out")));
        assert_eq!(ids(&members), names(&["l__in", "l__out", "l.c__in", "l.c__out", "B__in", "B.n", "B.free", "B__out"]));
    }

    /// The index answers every lookup a walk makes exactly as scanning
    /// the program does, on programs with groups, gates, and included
    /// files called from several sites.
    #[test]
    fn the_index_answers_like_a_scan() {
        use crate::project::graph::{GraphView, ProjectGraph};
        for project in [program(), called_program(), gated_program()] {
            let g = ProjectGraph::new(&project);
            let ids = |nodes: Vec<&NodeDefinition>| nodes.into_iter().map(|n| n.id.clone()).collect::<Vec<_>>();
            let edge_ids = |edges: Vec<&Edge>| edges.into_iter().map(|e| e.id.clone()).collect::<Vec<_>>();
            let group_ids = project.groups.iter().map(|g| g.id.clone()).chain(["missing".to_string()]);
            for id in project.nodes.iter().map(|n| n.id.clone()).chain(group_ids.clone()) {
                assert_eq!(g.node(&id).map(|n| &n.id), project.node(&id).map(|n| &n.id), "{id}");
                assert_eq!(edge_ids(g.edges_into(&id).collect()), edge_ids(project.edges_into(&id).collect()), "{id}");
                assert_eq!(edge_ids(g.edges_out_of(&id).collect()), edge_ids(project.edges_out_of(&id).collect()), "{id}");
                assert_eq!(g.is_ordinary_boundary(&id), project.is_ordinary_boundary(&id), "{id}");
                assert_eq!(g.is_trigger(&id), project.is_trigger(&id), "{id}");
            }
            for id in group_ids {
                assert_eq!(g.group(&id).map(|g| &g.id), project.group(&id).map(|g| &g.id), "{id}");
                assert_eq!(ids(g.members(&id).collect()), ids(project.members(&id).collect()), "{id}");
                let sites = |it: Vec<&super::super::GroupDefinition>| it.into_iter().map(|g| g.id.clone()).collect::<Vec<_>>();
                assert_eq!(sites(g.sites(&id).collect()), sites(project.sites(&id).collect()), "{id}");
                assert_eq!(g.body_of(&id), project.body_of(&id), "{id}");
                assert_eq!(g.is_loop(&id), project.is_loop(&id), "{id}");
            }
            for edge in &project.edges {
                assert_eq!(g.edge(&edge.id).map(|e| &e.id), Some(&edge.id));
            }
            for node in &project.nodes {
                assert_eq!(body_around(&g, node), body_around(&project, node), "{}", node.id);
            }
        }
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

    /// A recorded selection is written as its digest, and reads back only
    /// where the selection it names is known: two equal selections share a
    /// digest, and a digest nothing resolves is refused, naming it.
    #[test]
    fn a_recorded_selection_is_written_as_its_digest_and_read_from_what_is_known() {
        let selection = RunSelection { nodes: [top("a"), top("b")].into_iter().collect(), ..Default::default() };
        let recorded = RecordedSelection::new(selection.clone());
        assert_eq!(recorded.digest(), RecordedSelection::new(selection.clone()).digest());
        let written = serde_json::to_value(&recorded).unwrap();
        assert_eq!(written, json!(recorded.digest()));
        let back: RecordedSelection = RecordedSelection::resolving(std::slice::from_ref(&recorded), || serde_json::from_value(written.clone()).unwrap());
        assert_eq!(*back, selection);
        let unknown = serde_json::from_value::<RecordedSelection>(written).unwrap_err().to_string();
        assert!(unknown.contains(recorded.digest()), "{unknown}");
    }
}
