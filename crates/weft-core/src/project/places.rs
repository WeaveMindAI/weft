//! A program's PLACES, and the walks over them.
//!
//! Every node here is a place ([`Located`]): its id and the call path it
//! runs under. An included file is compiled once and reached through
//! every site that includes it, so one id is as many places as there are
//! calls of it, and a wire is taken at a place: the wire from `strip` to
//! `loud` exists under `one` and under `two`, and a wire into a body from
//! one site is never taken under another (it enters through its own
//! site, [`super::Edge::via`]).

use std::collections::BTreeSet;

use crate::frames::Located;
use super::graph::{GraphView, ProjectGraph};
use super::{Edge, GroupKind, NodeDefinition, ProjectDefinition};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Direction {
    Upstream,
    Downstream,
}

/// The place at the other end of `edge` from `from`: the call path
/// carried across the sites the wire crosses ([`Edge::carry_calls`]).
/// `None` when the wire is another call's.
pub fn step(g: &impl GraphView, from: &Located, edge: &Edge, direction: Direction) -> Option<Located> {
    let mut path = from.path.clone();
    let other = match direction {
        Direction::Downstream => &edge.target,
        Direction::Upstream => &edge.source,
    };
    Edge::carry_calls(edge.site_crossings(g), direction, &mut path, |site| Some(site.as_str()), str::to_string)
        .then(|| Located::new(other, path))
}

/// The wires into `at` that are taken there, each with its source's
/// place. A wire that stops at a group port nobody reads carries nothing
/// and is not among them.
pub fn incoming<'a>(g: &'a impl GraphView, at: &Located) -> impl Iterator<Item = (&'a Edge, Located)> + 'a {
    let edges: Vec<&Edge> = g.edges_into(&at.id).filter(|edge| !edge.to_group_port()).collect();
    let at = at.clone();
    edges.into_iter().filter_map(move |edge| step(g, &at, edge, Direction::Upstream).map(|source| (edge, source)))
}

/// The wires out of `at` that are taken there, each with its target's
/// place.
pub fn outgoing<'a>(g: &'a impl GraphView, at: &Located) -> impl Iterator<Item = (&'a Edge, Located)> + 'a {
    let edges: Vec<&Edge> = g.edges_out_of(&at.id).filter(|edge| !edge.to_group_port()).collect();
    let at = at.clone();
    edges.into_iter().filter_map(move |edge| step(g, &at, edge, Direction::Downstream).map(|target| (edge, target)))
}

/// Every place reached from `seeds` by following wires backward, seeds
/// included: the plain data walk. A body entered through a site is walked
/// under that site alone (see [`step`]), so an infra node inside a file
/// included twice is reached once per call. Wires only: nothing
/// structural is pulled in (no gate around a node, no loop taken whole),
/// which is what tells it apart from the walk a run is cut with
/// (`super::cut`): that one is the run's shape, this one is what a value's
/// path runs through.
pub fn upstream_by_wires(g: &ProjectGraph, seeds: &[Located]) -> BTreeSet<Located> {
    let mut reached = BTreeSet::new();
    let mut pending: Vec<Located> = seeds.to_vec();
    while let Some(place) = pending.pop() {
        if !reached.insert(place.clone()) { continue; }
        pending.extend(incoming(g, &place).map(|(_, source)| source));
    }
    reached
}

/// Every place that reads a per-instance value when `seeds` (the places
/// of `@per_instance` and `@instance_filled` nodes) are per instance: what
/// runs per instance with them (`super::NodeDefinition::per_instance`).
/// The walk follows wires, with one more step: a wire landing on a
/// group's gate decides whether the group runs, so reaching the group's
/// place reaches everything inside it there (the body behind each call
/// site among it, one site deeper). A loop's gate lands on its `LoopIn`,
/// which launches every iteration, so reaching it reaches the loop's
/// inside. A group's own port (a wire from it) is reached only through
/// its gate, never on its own: a literal is no instance's value. A body
/// entered through one site is walked under that site alone ([`step`]),
/// so one site's instance value never reaches another site's places.
pub fn per_instance_reach(g: &ProjectGraph, seeds: Vec<Located>) -> BTreeSet<Located> {
    let mut reached = BTreeSet::new();
    let mut frontier = seeds;
    while let Some(at) = frontier.pop() {
        let opens = if g.group(&at.id).is_some() {
            Some(at.id.clone())
        } else {
            g.node(&at.id)
                .and_then(|n| n.group_boundary.as_ref())
                .filter(|b| b.role == super::GroupBoundaryRole::In)
                .map(|b| b.group_id.clone())
        };
        let next: Vec<Located> = outgoing(g, &at)
            .map(|(_, target)| target)
            .chain(opens.into_iter().flat_map(|group| members_in(g, &group, &at.path)))
            .collect();
        for next in next {
            if reached.insert(next.clone()) {
                frontier.push(next);
            }
        }
    }
    reached
}

/// Where a value that arrives at a port ends up needed: the input that
/// would leave its node skipped without it, and the first start of the
/// run the value passes on the way (where a person hands it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Need {
    pub needer: (Located, String),
    pub start: Option<(Located, String)>,
    /// The `@require_one_of` set the needer belongs to, when that set is
    /// why it is needed (every other member gets nothing); `None` when
    /// the input is required on its own.
    pub one_of: Option<Vec<String>>,
}

/// The input a value arriving at `port` of the node at `at` ends up
/// needed by, or `None` when nothing that needs it lies on its path.
///
/// A node answers for itself: its input is needed when it is required
/// (or required once wired, which a port reached along a wire is) with
/// no default, or when it belongs to a `@require_one_of` set whose every
/// other member gets nothing (`fed(place, port)` says whether an input of
/// the node at `place` gets a value some other way). A loop's `LoopIn`
/// requires the ports it iterates or carries (with no list there is no
/// iteration), which the compiler records as that port's `required`, so a
/// loop answers like a node for those and is walked through for the rest,
/// to the members that read the port inside.
///
/// `counts(place, port)` says where a value arriving there still matters:
/// everywhere for the compiler; for a run, only places the run executes,
/// never a trigger (a fired one reads its bake, the others close), and not
/// past a start that was handed a value for that port. `is_start` marks
/// the run's starts, so the answer can name where to hand a value.
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
        if crate::run::skip::is_gate_port(&port) || !counts(&place, &port) { continue; }
        let Some(node) = project.node(&place.id) else { continue };
        let start = start.or_else(|| is_start(&place).then(|| (place.clone(), port.clone())));
        let input = node.inputs.iter().find(|p| p.name == port);
        // The walk only ever reaches a port along a wire, so an input
        // required once wired counts as required here. A default does
        // not help: it fills only an unwired port, and a wired one whose
        // wire closes skips its node (`ready::required_inputs`).
        if input.is_some_and(|input| input.is_required(true)) {
            return Some(Need { needer: (place, port), start, one_of: None });
        }
        let last_of = node.features.one_of_required.iter()
            .find(|set| set.contains(&port) && set.iter().all(|other| *other == port || !fed(&place, other)));
        if let Some(set) = last_of {
            return Some(Need { needer: (place, port), start, one_of: Some(set.clone()) });
        }
        if node.node_type != super::boundary_types::LOOP_IN { continue; }
        for (edge, target) in outgoing(project, &place) {
            if edge.source_port() != port { continue; }
            pending.push((target, edge.target_port().to_string(), start.clone()));
        }
    }
    None
}

/// The output ports of `place` that a wire into one of `runs` reads, each
/// wire followed on its path (so a file included twice reads its own
/// site's node, never the other site's). A wire into a trigger is not a
/// read: a run never delivers it (a trigger read its inputs when it was
/// set up).
pub fn ports_read_by(g: &impl GraphView, place: &Located, runs: &BTreeSet<Located>) -> BTreeSet<String> {
    outgoing(g, place)
        .filter(|(_, target)| runs.contains(target) && !g.is_trigger(&target.id))
        .map(|(edge, _)| edge.source_port().to_string())
        .collect()
}

/// Every wire at every place it runs: `(wire, source place, target
/// place)`, a wire inside an included file once per site that reaches
/// it, and a wire into or out of a file's body only for the site it
/// belongs to. What resolves and checks types per place reads.
pub fn located_wires(project: &ProjectDefinition) -> Vec<(&Edge, Located, Located)> {
    let mut wires = Vec::new();
    for at in every_place(project) {
        wires.extend(incoming(project, &at).map(|(edge, source)| (edge, source, at.clone())));
    }
    wires
}

/// Every place a group runs at, each at the call path its members run
/// under: a group of the program once, at the top; a group inside an
/// included file (the file's own body among them) once per site that
/// reaches it, however deep. A site sits in the file around it, at that
/// file's path.
pub fn every_group_place(project: &ProjectDefinition) -> BTreeSet<Located> {
    let g = ProjectGraph::new(project);
    let mut out = BTreeSet::new();
    // Each file reached (its body, `None` for the program) and the path
    // reaching it.
    let mut pending: Vec<(Option<String>, Vec<String>)> = vec![(None, Vec::new())];
    while let Some((file, path)) = pending.pop() {
        for group in &project.groups {
            if body_of_group(&g, &group.id) != file { continue; }
            out.insert(Located::new(group.id.clone(), path.clone()));
            if let GroupKind::Call { body, .. } = &group.kind {
                let mut deeper = path.clone();
                deeper.push(group.id.clone());
                pending.push((Some(body.clone()), deeper));
            }
        }
    }
    out
}

/// The group place each of `edge`'s crossings passes, in order, for the
/// wire taken from `source` (its source's place): the call path carried
/// across the sites the way [`Edge::carry_calls`] carries it, a site's
/// own port read in the file around it and a body's inside the call.
pub fn crossing_places(g: &impl GraphView, edge: &Edge, source: &Located) -> Vec<Located> {
    let mut path = source.path.clone();
    edge.via.iter().map(|crossing| {
        if !g.is_site(&crossing.group) {
            return Located::new(crossing.group.clone(), path.clone());
        }
        match crossing.side {
            super::CrossingSide::In => {
                let at = Located::new(crossing.group.clone(), path.clone());
                path.push(crossing.group.clone());
                at
            }
            super::CrossingSide::Out => {
                path.pop();
                Located::new(crossing.group.clone(), path.clone())
            }
        }
    }).collect()
}

/// Every wire at every place it runs, the way type resolution and type
/// checks read them: [`located_wires`] with their target's place, and a
/// wire that stops at a group port nobody reads (`None` for its target)
/// at every place its source runs from which the wire is taken (one
/// leaving an included file leaves through the site of that place only,
/// [`Edge::carry_calls`]).
pub fn wires_at_places(project: &ProjectDefinition) -> Vec<(&Edge, Located, Option<Located>)> {
    let g = ProjectGraph::new(project);
    let mut wires: Vec<(&Edge, Located, Option<Located>)> =
        located_wires(project).into_iter().map(|(edge, source, target)| (edge, source, Some(target))).collect();
    let sources: BTreeSet<Located> = every_place_in(&g).into_iter().chain(every_group_place(project)).collect();
    for edge in project.edges.iter().filter(|edge| edge.to_group_port()) {
        for at in sources.iter().filter(|at| at.id == edge.source) {
            let mut path = at.path.clone();
            if Edge::carry_calls(edge.site_crossings(&g), Direction::Downstream, &mut path, |site| Some(site.as_str()), str::to_string) {
                wires.push((edge, at.clone(), None));
            }
        }
    }
    wires
}

/// A place whose state in a run is one thing (`GetNodeState`): a node
/// outside every loop, or a loop, which is read as a whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatePlace {
    Node(Located),
    Loop(Located),
}

/// The place `spelled` names (written the way `--from` takes it, read by
/// [`named`]) whose state a run can tell, or why it has none. A node
/// inside a loop fires once per iteration, so it is refused like a cut
/// inside a loop, and the outermost loop around it is named instead; a
/// plain group or an include's site has no state of its own (it is a
/// scope: whether it ran is the state of the nodes inside it).
pub fn state_place(project: &ProjectDefinition, spelled: &str) -> Result<StatePlace, String> {
    let g = ProjectGraph::new(project);
    let found = named(&g, spelled)?;
    let subject = match &found {
        Named::Node(_) => "node",
        Named::Group(place) if g.is_loop(&place.id) => "loop",
        Named::Group(_) => {
            return Err(format!("'{spelled}' is a group, and a group has no state of its own; name a node inside it, or a loop"));
        }
    };
    if let Some(around) = loop_around(&g, &found) {
        let around = super::group_address(project, &around.id, &around.path);
        return Err(format!("the {subject} '{spelled}' is inside loop '{around}': it runs once per iteration, so it has no one state; read the loop '{around}' instead"));
    }
    Ok(match found {
        Named::Node(place) => StatePlace::Node(place),
        Named::Group(place) => StatePlace::Loop(place),
    })
}

/// Every place in the program: the top-level nodes, and each included
/// file's nodes once per site that reaches it, however deep.
pub fn every_place(project: &ProjectDefinition) -> BTreeSet<Located> {
    every_place_in(&ProjectGraph::new(project))
}

/// The places a program runs: every place ([`every_place`]) its call
/// sites run (`ProgramIndex::admits`), less those that stand in for
/// themselves (`ProjectDefinition::given`).
pub fn running_places(project: &ProjectDefinition) -> BTreeSet<Located> {
    let index = super::ProgramIndex::build(project);
    every_place(project)
        .into_iter()
        .filter(|place| index.admits(&place.id, &place.frames()) && !index.stands_in(&place.id, &place.frames()))
        .collect()
}

/// [`every_place`] over a program already indexed.
pub fn every_place_in(g: &ProjectGraph) -> BTreeSet<Located> {
    let project = g.project();
    let mut places: BTreeSet<Located> = project.nodes.iter().filter(|node| body_around(g, node).is_none())
        .map(|node| Located::top(node.id.clone())).collect();
    for site in project.groups.iter().filter(|group| matches!(group.kind, GroupKind::Call { .. })) {
        if body_of_group(g, &site.id).is_none() {
            places.extend(members_in(g, &site.id, &[]));
        }
    }
    places
}

/// The groups `group` sits in, outermost first (in its own file).
fn groups_around(g: &impl GraphView, group: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut at = g.group(group).and_then(|here| here.parent_group_id.clone());
    while let Some(id) = at {
        at = g.group(&id).and_then(|here| here.parent_group_id.clone());
        out.push(id);
    }
    out.reverse();
    out
}

/// The scopes `place` runs inside, outermost first, each at the place it
/// runs at (`Located` naming the scope): every site on its path (at the
/// path above it) with the groups around that site, then the groups and
/// loops around the node itself (at its own path). A loop's `LoopIn` and
/// `LoopOut` sit in the scope around their loop.
pub fn scopes_around(g: &impl GraphView, place: &Located) -> Vec<Located> {
    let mut out = path_scopes(g, &place.path);
    if let Some(node) = g.node(&place.id) {
        out.extend(node.scope.iter().map(|group| Located::new(group.clone(), place.path.clone())));
    }
    out
}

/// The scopes the group at `group` runs inside, outermost first, the way
/// [`scopes_around`] lists a node's.
pub fn scopes_around_group(g: &impl GraphView, group: &Located) -> Vec<Located> {
    let mut out = path_scopes(g, &group.path);
    out.extend(groups_around(g, &group.id).into_iter().map(|id| Located::new(id, group.path.clone())));
    out
}

/// The scopes a call path runs inside: each site on it (at the path above
/// it), after the groups around that site.
fn path_scopes(g: &impl GraphView, path: &[String]) -> Vec<Located> {
    let mut out = Vec::new();
    for (depth, site) in path.iter().enumerate() {
        let above = path[..depth].to_vec();
        out.extend(groups_around(g, site).into_iter().map(|group| Located::new(group, above.clone())));
        out.push(Located::new(site.clone(), above));
    }
    out
}

/// The loops around `place`, outermost first, each at the place the loop
/// runs: a loop in the node's own scope, a loop around any site on its
/// path (a call inside a loop runs whole with the loop), and a loop's own
/// boundaries' loop.
pub fn enclosing_loops(g: &impl GraphView, place: &Located) -> Vec<Located> {
    let mut loops: Vec<Located> = scopes_around(g, place).into_iter().filter(|scope| g.is_loop(&scope.id)).collect();
    if let Some(boundary) = g.node(&place.id).and_then(|node| node.group_boundary.as_ref()) {
        loops.push(Located::new(boundary.group_id.clone(), place.path.clone()));
    }
    loops.dedup();
    loops
}

/// A group's members, each at the place it runs when the group is at
/// `path`: its nodes at `path` (a loop's own two boundaries among a
/// loop's), and behind every call site among them (the group itself,
/// when it is one) the whole body one site deeper. Takes the graph so a
/// caller walking many groups indexes the program once.
pub fn members_in(g: &impl GraphView, group: &str, path: &[String]) -> Vec<Located> {
    let mut out: Vec<Located> = g.members(group).map(|n| Located::new(n.id.clone(), path.to_vec())).collect();
    let sites: Vec<(String, String)> = g.sites(group)
        .filter_map(|site| g.body_of(&site.id).map(|body| (site.id.clone(), body.to_string()))).collect();
    for (site, body) in sites {
        let mut deeper = path.to_vec();
        deeper.push(site);
        out.extend(members_in(g, &body, &deeper));
    }
    out
}

/// What a name a person wrote names in a program: a node, or a group (a
/// plain group, a loop, or an include's site), each at the place it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Named {
    Node(Located),
    Group(Located),
}

impl Named {
    pub fn place(&self) -> &Located {
        match self {
            Self::Node(place) | Self::Group(place) => place,
        }
    }

    pub fn into_place(self) -> Located {
        match self {
            Self::Node(place) | Self::Group(place) => place,
        }
    }
}

/// THE reading of a name a person writes for a place of a program
/// (`check`, `auth.check` through the site `auth`, `each` for a loop),
/// shared by everything that takes one: a run's bounds, a node state read,
/// the CLI's node arguments, and every read of a past run, which reads the
/// name against the program that run ran. [`super::resolve_address`] walks
/// the call sites; this is that walk, refusing every name that does not
/// name one place, each time with the spelling that would:
///
/// - a name nothing in the program answers to;
/// - a loop's boundary the compiler made (`each__in`), which nobody wrote:
///   its loop is named (a node a person named that way reads as written);
/// - something inside an included file named by its compiled id
///   (`@src:lib.check`), which says nothing about which call of the file:
///   it is named through a site;
/// - an included file's body itself, which runs through its site.
pub fn named(g: &impl GraphView, spelled: &str) -> Result<Named, String> {
    let project = g.project();
    if let Some(group) = super::boundary_loop(spelled) {
        if g.is_loop(&super::resolve_address(project, group).0) {
            return Err(format!("'{spelled}' is a loop's boundary the compiler made; name the loop, `{group}`"));
        }
    }
    let (id, path) = super::resolve_address(project, spelled);
    let inside_body = match (g.node(&id), g.group(&id)) {
        (Some(node), _) => body_around(g, node),
        (None, Some(_)) => body_of_group(g, &id),
        (None, None) => {
            return Err(format!(
                "unknown node '{spelled}': nothing in the program is named that (a node inside an included file is \
                 named through its site, like `site.{}`)",
                spelled.rsplit('.').next().unwrap_or(spelled)
            ));
        }
    };
    // Through a site, the walk has already left the file's compiled ids
    // behind; only a name with no site on its path can be one.
    if let Some(body) = inside_body.filter(|_| path.is_empty()) {
        if body == id {
            return Err(format!("'{spelled}' is an included file, which runs through the site that includes it; name the site"));
        }
        let example = project.groups.iter()
            .find(|g| matches!(&g.kind, GroupKind::Call { body: called, .. } if *called == body))
            .map(|g| super::address_of(project, &id, std::slice::from_ref(&g.id)))
            .unwrap_or_else(|| format!("<site>.{}", id.rsplit('.').next().unwrap_or(&id)));
        return Err(format!(
            "'{spelled}' is inside an included file, which is reached through a site; name it through the site, like `{example}`"
        ));
    }
    let place = Located::new(id, path);
    Ok(if g.node(&place.id).is_some() { Named::Node(place) } else { Named::Group(place) })
}

/// The outermost loop `named` runs inside, at the place the loop runs, if
/// any: a loop around the node or group itself, or around any site on its
/// call path (a call inside a loop runs whole with the loop). A loop's own
/// boundaries are inside it; a loop is not inside itself. A loop runs once
/// per iteration as a whole, so a place inside one is no end of a run and
/// has no one state: the loop is what is named instead.
pub fn loop_around(g: &impl GraphView, named: &Named) -> Option<Located> {
    match named {
        Named::Node(place) => enclosing_loops(g, place).into_iter().next(),
        Named::Group(place) => scopes_around_group(g, place).into_iter().find(|scope| g.is_loop(&scope.id)),
    }
}

/// The included file's body `group` sits in (itself, when it is one).
pub fn body_of_group(g: &impl GraphView, group: &str) -> Option<String> {
    let mut at = g.group(group);
    while let Some(here) = at {
        if matches!(here.kind, GroupKind::Body) { return Some(here.id.clone()); }
        at = here.parent_group_id.as_deref().and_then(|parent| g.group(parent));
    }
    None
}

/// Whether `group` is an included file's body.
pub fn is_body(g: &impl GraphView, group: &str) -> bool {
    g.group(group).is_some_and(|group| matches!(group.kind, GroupKind::Body))
}

/// The included file's body `node` sits in, if any: the first, in the
/// program's group order, of the groups it is a member or a boundary of.
fn body_around(g: &impl GraphView, node: &NodeDefinition) -> Option<String> {
    node.scope.iter().chain(node.group_boundary.iter().map(|b| &b.group_id))
        .filter(|id| g.group(id).is_some_and(|group| matches!(group.kind, GroupKind::Body)))
        .filter_map(|id| g.group_position(id).map(|at| (at, id)))
        .min()
        .map(|(_, id)| id.clone())
}

#[cfg(test)]
mod named_tests {
    use super::{loop_around, named, state_place, Named};
    use crate::frames::Located;
    use crate::project::ProjectDefinition;

    /// `plain` and `fan__out` at the top; the file `Auth` (with `check`)
    /// included at `auth`; the loop `lp` holding `inner`, the loop `deep`
    /// with `x`, and a second include of `Auth` at the site `site`.
    fn program() -> ProjectDefinition {
        let node = |id: &str, scope: &[&str]| serde_json::json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false
        });
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("plain", &[]), node("fan__out", &[]), node("Auth.check", &["Auth"]),
                node("lp.inner", &["lp"]), node("lp.deep.x", &["lp", "lp.deep"]),
            ],
            "edges": [],
            "groups": [
                {"id": "Auth", "kind": "body", "nodeIds": ["Auth.check"]},
                {"id": "auth", "kind": "call", "body": "Auth", "nodeIds": []},
                {"id": "lp", "kind": "loop", "loopConfig": {}, "nodeIds": ["lp.inner"]},
                {"id": "lp.deep", "kind": "loop", "loopConfig": {}, "nodeIds": ["lp.deep.x"], "parentGroupId": "lp"},
                {"id": "lp.site", "kind": "call", "body": "Auth", "nodeIds": [], "parentGroupId": "lp"}
            ]
        })).unwrap()
    }

    fn at(id: &str, path: &[&str]) -> Located {
        Located::new(id.to_string(), path.iter().map(|s| s.to_string()).collect())
    }

    /// A name reads through the call sites to the one place it names: a
    /// node, a group, a site, and a node a person called `__out`.
    #[test]
    fn a_name_reads_to_its_place() {
        let p = program();
        assert_eq!(named(&p, "plain"), Ok(Named::Node(at("plain", &[]))));
        assert_eq!(named(&p, "auth.check"), Ok(Named::Node(at("Auth.check", &["auth"]))));
        assert_eq!(named(&p, "lp.site.check"), Ok(Named::Node(at("Auth.check", &["lp.site"]))));
        assert_eq!(named(&p, "auth"), Ok(Named::Group(at("auth", &[]))));
        assert_eq!(named(&p, "lp.deep"), Ok(Named::Group(at("lp.deep", &[]))));
        assert_eq!(named(&p, "fan__out"), Ok(Named::Node(at("fan__out", &[]))));
    }

    /// Every name that does not name one place is refused with the name
    /// that would: nothing by that name, a loop's boundary, a file's node
    /// by its compiled id, and the file itself.
    #[test]
    fn a_name_that_names_no_one_place_is_refused_with_the_one_that_would() {
        let p = program();
        let unknown = named(&p, "nope").unwrap_err();
        assert!(unknown.contains("unknown node 'nope'"), "{unknown}");
        let boundary = named(&p, "lp__in").unwrap_err();
        assert!(boundary.contains("loop's boundary") && boundary.contains("`lp`"), "{boundary}");
        let compiled = named(&p, "Auth.check").unwrap_err();
        assert!(compiled.contains("included file") && compiled.contains("`auth.check`"), "{compiled}");
        let body = named(&p, "Auth").unwrap_err();
        assert!(body.contains("name the site"), "{body}");
        for malformed in ["auth.", ".check", ""] {
            assert!(named(&p, malformed).is_err(), "{malformed}");
        }
    }

    /// The loop around a place is the outermost one: around the node
    /// itself, or around a site on its call path. A loop is not inside
    /// itself.
    #[test]
    fn the_loop_around_a_place_is_the_outermost() {
        let p = program();
        let around = |spelled: &str| loop_around(&p, &named(&p, spelled).unwrap());
        assert_eq!(around("lp.deep.x"), Some(at("lp", &[])));
        assert_eq!(around("lp.site.check"), Some(at("lp", &[])));
        assert_eq!(around("lp.deep"), Some(at("lp", &[])));
        assert_eq!(around("lp"), None);
        assert_eq!(around("auth.check"), None);
    }

    /// A node inside nested loops has no one state, and the loop to read
    /// instead is the outermost, the one whose state is one thing.
    #[test]
    fn a_state_inside_nested_loops_names_the_outermost() {
        let p = program();
        let refused = state_place(&p, "lp.deep.x").unwrap_err();
        assert!(refused.contains("read the loop 'lp' instead"), "{refused}");
        assert!(state_place(&p, "auth").unwrap_err().contains("is a group"));
    }
}
