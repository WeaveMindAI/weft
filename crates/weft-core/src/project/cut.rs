//! The walk and the cut: what a run started with bounds (`--from`,
//! `--target`, a trigger's fire, a setup phase's targets) runs, and the
//! smaller program it runs.
//!
//! The walk:
//!
//! ```text
//! starts: --from N, --from G (a group: what is inside runs, what feeds it
//!         from outside does not), --group G, --emit N (N does not run; you
//!         give its outputs), --fire T (trigger T with your event), and the
//!         producers --feed adds one level up
//! ends:   --target N, --before N (N does not run), or with neither: as far
//!         as the wires go
//!
//! 1. down along wires from the starts
//! 2. back UP from the ends, stopping at a start, at a trigger nobody fired,
//!    and at a node with no inputs (a loop is taken whole); the gate of a
//!    scope around something reached is walked too, unless a start sits
//!    in that scope (the start opens it: its gate is not walked, and a
//!    gate nothing in the run feeds lets the scope run)
//! 3. everything step 2 touches runs
//! ```
//!
//! The cut ([`cut`]) is then a real program: the nodes the walk keeps,
//! with their original ids, the wires between them, the scopes something
//! in it still waits on, and where the run is given what it does not
//! compute ([`super::GivenPlaces`]): the ports handed a value at the starts,
//! and the nodes that stand in for themselves (an `--emit`, an infra node's
//! baked values, a seed's saved result). The values themselves are the
//! run's ([`super::Given`]), never the cut's, so runs that differ only by
//! their values run one cut. It runs on the program's worker like any
//! version, and is stored under its own fingerprint.

use std::collections::{BTreeMap, BTreeSet};

use crate::frames::Located;
use super::graph::{GraphView, ProjectGraph};
use super::places::{
    enclosing_loops, every_place_in, incoming, loop_around, members_in, named, outgoing, scopes_around, Named,
};
use super::{CrossingSide, Edge, Given, GroupDefinition, GroupKind, ProjectDefinition};

/// The bounds a person (or a trigger's fire, or a setup phase) puts on a
/// run. Every bound is spelled from the top of the program, through the
/// call sites (`triage.up`), and resolves to one place.
#[derive(Debug, Clone, Default)]
pub struct SelectionBounds {
    pub from: Vec<String>,
    pub emit: Vec<String>,
    pub target: Vec<String>,
    pub before: Vec<String>,
    pub group: Option<String>,
    pub fire: Option<String>,
    /// Starts (a `from` entry or the `group`) whose feeders run too: for
    /// each start port not handed a value, the node that feeds it, and
    /// nothing above that node.
    pub feed: Vec<String>,
    /// Per start, the ports handed a value (`from` and `group`): a handed
    /// port is the value's, so nothing feeds it and nothing is walked for
    /// it (its feeder, a handed gate's source).
    pub handed: BTreeMap<String, BTreeSet<String>>,
    /// Infra places the run reads only baked outputs of, every one saved
    /// (`crate::infra::bake::covered`): each one this run reaches does not
    /// run, its saved values go out in its place, and nothing above it
    /// runs for its sake.
    pub baked: BTreeSet<Located>,
}

/// What the walk keeps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Walk {
    /// The places that run: the fired trigger and every other trigger the
    /// walk reached among them (one nobody fired closes its outputs).
    pub runs: BTreeSet<Located>,
    /// The places that send without running: the `--emit`ted nodes and
    /// the baked infra the walk reached.
    pub stand_ins: BTreeSet<Located>,
    /// The places the run starts at: `--from` nodes, the fired trigger,
    /// the feeders, and the members a group start begins with.
    pub starts: BTreeSet<Located>,
    /// The groups started (`--from G`, `--group G`), at their places: no
    /// wire into one from outside runs.
    pub group_starts: BTreeSet<Located>,
    pub fired: Option<Located>,
    /// The walk set its targets up ([`Bounds::sets_up`]): what feeds a
    /// trigger is what the run is for.
    pub sets_up: bool,
}

impl Walk {
    /// Every place the cut keeps.
    pub fn kept(&self) -> BTreeSet<Located> {
        self.runs.union(&self.stand_ins).cloned().collect()
    }
}

/// The bounds of a run resolved to places: what [`walk_bounds`] walks.
/// A setup phase builds one directly (its targets are places already); a
/// person's bounds come through [`resolve`].
#[derive(Debug, Clone, Default)]
pub struct Bounds {
    pub node_starts: BTreeSet<Located>,
    pub group_starts: BTreeSet<Located>,
    pub emits: BTreeSet<Located>,
    pub fired: Option<Located>,
    pub targets: Vec<Located>,
    pub befores: Vec<Located>,
    pub feeders: BTreeSet<Located>,
    /// The scopes whose gate is handed a value at their place: their gate's
    /// wire carries nothing, so its source is not walked for it.
    pub handed_gates: BTreeSet<Located>,
    /// The places that stand in for themselves without being named
    /// (baked infra, a seed's saved results): the walk stops at each it
    /// reaches, and never starts from one.
    pub given: BTreeSet<Located>,
    /// The targets are set up rather than run to (a setup phase): no
    /// trigger stops the walk, since what feeds a trigger is what its setup
    /// is for.
    pub sets_up: bool,
}

impl Bounds {
    /// The setup of `targets`: everything they need, with nothing to start
    /// from. With no target it walks nothing.
    pub fn setup(targets: &[Located]) -> Self {
        Self { targets: targets.to_vec(), sets_up: true, ..Self::default() }
    }
}

/// A node or a group named as an end of a cut: one that does not compile
/// is refused with the compiler's own reason, before any rule about where
/// an end may sit (an infra node inside a loop is refused for being there,
/// never told to select the loop); then one inside a loop, which runs
/// whole, is refused naming the loop.
fn endpoint(g: &ProjectGraph, found: &Named, spelled: &str) -> Result<(), String> {
    refuse_broken(&g.project().broken, &BTreeSet::from([found.place().id.as_str()]))?;
    match loop_around(g, found) {
        Some(around) => Err(format!(
            "cannot cut at '{spelled}' inside loop '{}'; select the whole loop",
            super::group_address(g.project(), &around.id, &around.path)
        )),
        None => Ok(()),
    }
}

/// Resolve `bounds` against the program. Refuses bounds that name
/// nothing, name something inside a loop, or contradict each other.
pub fn resolve(g: &ProjectGraph, bounds: &SelectionBounds) -> Result<Bounds, String> {
    let project = g.project();
    let mut resolved = Bounds { given: bounds.baked.clone(), ..Bounds::default() };
    for spelled in &bounds.from {
        let found = named(g, spelled)?;
        endpoint(g, &found, spelled)?;
        let fresh = match found {
            Named::Group(place) => resolved.group_starts.insert(place),
            Named::Node(place) => resolved.node_starts.insert(place),
        };
        if !fresh {
            return Err(format!("starting entry '{spelled}' is supplied more than once"));
        }
    }
    if let Some(group) = &bounds.group {
        if !bounds.from.is_empty() || !bounds.emit.is_empty() || !bounds.target.is_empty() || !bounds.before.is_empty() {
            return Err("group cannot be combined with from, emit, target, or before".into());
        }
        let found = named(g, group)?;
        let Named::Group(place) = &found else {
            return Err(format!("'{group}' is a node, and --group takes a group"));
        };
        endpoint(g, &found, group)?;
        let place = place.clone();
        resolved.targets.extend(members_in(g, &place.id, &place.path));
        resolved.group_starts.insert(place);
    }
    for spelled in &bounds.emit {
        let found = named(g, spelled)?;
        if let Named::Group(_) = found {
            return Err(format!("cannot supply outputs of group '{spelled}'; use --from {spelled} with its input payload"));
        }
        endpoint(g, &found, spelled)?;
        resolved.emits.insert(found.into_place());
    }
    if let Some(spelled) = &bounds.fire {
        let found = named(g, spelled)?;
        endpoint(g, &found, spelled)?;
        if !g.is_trigger(&found.place().id) {
            return Err(format!("'{spelled}' is not a trigger"));
        }
        resolved.fired = Some(found.into_place());
    }
    if let Some(place) = resolved.emits.iter().find(|place| resolved.node_starts.contains(*place) || resolved.fired.as_ref() == Some(*place)) {
        return Err(format!("'{}' cannot both run and have its outputs supplied", super::address_of(project, &place.id, &place.path)));
    }
    for (inclusive, ends) in [(true, &bounds.target), (false, &bounds.before)] {
        for spelled in ends {
            let found = named(g, spelled)?;
            endpoint(g, &found, spelled)?;
            let endpoints: Vec<Located> = match found {
                Named::Group(place) => members_in(g, &place.id, &place.path),
                Named::Node(place) => vec![place],
            };
            if inclusive { resolved.targets.extend(endpoints) } else { resolved.befores.extend(endpoints) }
        }
    }
    resolved.feeders = feeders_of(g, &bounds.feed, &bounds.handed, &resolved.node_starts, &resolved.group_starts)?;
    for (spelled, ports) in &bounds.handed {
        if !ports.iter().any(|port| crate::run::skip::is_gate_port(port)) { continue; }
        if let Named::Group(place) = named(g, spelled)? {
            resolved.handed_gates.insert(place);
        }
    }
    Ok(resolved)
}

/// Walk `bounds` over the program: [`resolve`] then [`walk_bounds`],
/// refusing a walk that would hold part of a loop.
pub fn walk(g: &ProjectGraph, bounds: &SelectionBounds) -> Result<Walk, String> {
    let walked = walk_bounds(g, &resolve(g, bounds)?);
    validate_loops(g, &walked)?;
    Ok(walked)
}

/// The walk a setup phase of `targets` runs ([`Bounds::setup`]), refusing
/// a target inside a loop and a walk that would hold part of one.
pub fn walk_setup(g: &ProjectGraph, targets: &[Located]) -> Result<Walk, String> {
    for target in targets {
        endpoint(g, &Named::Node(target.clone()), &super::address_of(g.project(), &target.id, &target.path))?;
    }
    let walked = walk_bounds(g, &Bounds::setup(targets));
    validate_loops(g, &walked)?;
    Ok(walked)
}

/// Walk resolved bounds over the program (see the module doc).
pub fn walk_bounds(g: &ProjectGraph, bounds: &Bounds) -> Walk {
    let Bounds { node_starts, group_starts, emits, fired, targets, befores, feeders, handed_gates, given, sets_up } = bounds;
    // A setup of nothing sets nothing up (with no end the walk would keep
    // the whole program).
    if *sets_up && targets.is_empty() {
        return Walk { sets_up: true, ..Walk::default() };
    }
    let mut seeds: Vec<Located> = node_starts.iter().chain(emits).chain(fired.iter()).cloned().collect();
    for start in group_starts {
        seeds.extend(members_in(g, &start.id, &start.path));
    }
    let reached_down = downstream(g, &seeds);

    // A start downstream of another start is no stop: the run starts at
    // the one furthest up, so the walk passes it. A group start counts by
    // its members.
    let members_of = |start: &Located| members_in(g, &start.id, &start.path);
    let inner: BTreeSet<Located> = node_starts.iter()
        .filter(|start| {
            let others: Vec<Located> = node_starts.iter().chain(emits).chain(fired.iter()).cloned()
                .chain(group_starts.iter().flat_map(members_of))
                .filter(|other| other != *start)
                .collect();
            downstream(g, &others).contains(*start)
        })
        .cloned().collect();
    // The same for a group start: one another start reaches is no border.
    // A start inside it (a member also started or stood in for) is the
    // group's own, never upstream of it.
    let outer_groups: BTreeSet<Located> = group_starts.iter()
        .filter(|start| {
            let own = members_of(start);
            let others: Vec<Located> = node_starts.iter().chain(emits).chain(fired.iter()).cloned()
                .chain(group_starts.iter().filter(|other| other != start).flat_map(members_of))
                .filter(|other| !own.contains(other))
                .collect();
            let reached = downstream(g, &others);
            !own.iter().any(|member| reached.contains(member))
        })
        .cloned().collect();
    let stops = Stops {
        nodes: node_starts.iter().filter(|start| !inner.contains(*start)).chain(fired.iter()).chain(feeders).cloned().collect(),
        stand_ins: emits.iter().chain(given).cloned().collect(),
        groups: outer_groups,
        handed_gates: handed_gates.clone(),
        fired: fired.clone(),
        sets_up: *sets_up,
    };
    let scope_starts: Vec<Located> = node_starts.iter().chain(emits).chain(fired.iter()).chain(feeders).cloned().collect();
    let ends: Vec<Located> = if !targets.is_empty() || !befores.is_empty() {
        let mut ends = targets.clone();
        for before in befores {
            ends.extend(incoming(g, before).map(|(_, source)| source));
        }
        // With starts, the walk goes down from them to the ends: an end
        // they do not reach is no end of this run.
        if !seeds.is_empty() {
            ends.retain(|end| reached_down.contains(end));
        }
        ends
    } else if !seeds.is_empty() {
        reached_down.iter().cloned().collect()
    } else {
        every_place_in(g).into_iter().collect()
    };
    let excluded: BTreeSet<&Located> = befores.iter().collect();
    let mut reached = walk_up(g, &ends, &stops, &scope_starts);
    reached.extend(feeders.iter().cloned());
    reached.retain(|place| !excluded.contains(place));

    let mut walked = Walk { fired: fired.clone(), group_starts: group_starts.clone(), sets_up: *sets_up, ..Walk::default() };
    for place in reached {
        if stops.stand_ins.contains(&place) {
            walked.stand_ins.insert(place);
        } else {
            walked.runs.insert(place);
        }
    }
    walked.starts = node_starts.iter().chain(fired.iter()).chain(feeders)
        .cloned()
        .chain(group_starts.iter().flat_map(|start| members_in(g, &start.id, &start.path)))
        .filter(|place| walked.runs.contains(place)).collect();
    walked
}

/// Where the walk up stops: a start (it runs, nothing above it does), a
/// stand-in (it sends, nothing above it runs), and a group start's
/// border (no wire into it from outside runs).
struct Stops {
    nodes: BTreeSet<Located>,
    stand_ins: BTreeSet<Located>,
    groups: BTreeSet<Located>,
    /// The scopes whose gate was handed a value: not walked through.
    handed_gates: BTreeSet<Located>,
    fired: Option<Located>,
    /// A setup phase: no trigger stops the walk (what feeds a trigger is
    /// what its setup is for).
    sets_up: bool,
}

/// Every place downstream of `seeds`, the seeds included: along wires, a
/// gate's wire into a group taking the whole group (the gate decides
/// whether all of it runs), and a loop taken whole.
pub fn downstream(g: &ProjectGraph, seeds: &[Located]) -> BTreeSet<Located> {
    let mut reached = BTreeSet::new();
    let mut pending: Vec<Located> = seeds.to_vec();
    while let Some(place) = pending.pop() {
        if !reached.insert(place.clone()) { continue; }
        pending.extend(whole_loop(g, &place));
        for (edge, target) in outgoing(g, &place) {
            if g.group(&edge.target).is_some() {
                pending.extend(members_in(g, &target.id, &target.path));
            } else {
                pending.push(target);
            }
        }
    }
    reached.retain(|place| g.node(&place.id).is_some());
    reached
}

/// Every place the walk up from `ends` touches (see the module doc).
fn walk_up(g: &ProjectGraph, ends: &[Located], stops: &Stops, starts: &[Located]) -> BTreeSet<Located> {
    let mut reached = BTreeSet::new();
    let mut gates_walked = BTreeSet::new();
    let mut pending: Vec<Located> = ends.to_vec();
    while let Some(place) = pending.pop() {
        if g.node(&place.id).is_none() || !reached.insert(place.clone()) { continue; }
        let unfired_trigger = g.is_trigger(&place.id) && stops.fired.as_ref() != Some(&place) && !stops.sets_up;
        if stops.nodes.contains(&place) || stops.stand_ins.contains(&place) || unfired_trigger {
            continue;
        }
        pending.extend(whole_loop(g, &place));
        // The scopes whose gates decide whether `place` gets what it
        // reads: the ones around it, and a group sending to it from its own
        // port (the group comes along to send) with the ones around that.
        let mut scopes = scopes_around(g, &place);
        for (edge, source) in incoming(g, &place) {
            if enters_a_started_group(g, edge, &place, &stops.groups) { continue; }
            if g.group(&source.id).is_some() {
                scopes.extend(super::places::scopes_around_group(g, &source));
                scopes.push(source);
                continue;
            }
            pending.push(source);
        }
        for scope in scopes {
            if g.is_loop(&scope.id) || stops.handed_gates.contains(&scope) || !gates_walked.insert(scope.clone()) { continue; }
            if starts.iter().any(|start| inside(g, start, &scope)) || stops.groups.iter().any(|start| group_inside(g, start, &scope)) {
                continue;
            }
            for (edge, source) in incoming(g, &scope) {
                // A gate fed by a trigger this run does not fire keeps its
                // default (the cut drops the wire): nothing to walk.
                let quiet = g.is_trigger(&source.id) && stops.fired.as_ref() != Some(&source)
                    && !stops.sets_up && !stops.stand_ins.contains(&source);
                if crate::run::skip::is_gate_port(edge.target_port()) && g.group(&source.id).is_none() && !quiet {
                    pending.push(source);
                }
            }
        }
    }
    reached
}

/// Whether `edge`, landing at `at`, enters one of the started groups at
/// its place ([`started_group_entered`]).
fn enters_a_started_group(g: &ProjectGraph, edge: &Edge, at: &Located, starts: &BTreeSet<Located>) -> bool {
    started_group_entered(g, edge, at, starts).is_some()
}

/// The started group (one of `starts`, at its place) `edge` enters on its
/// way to `at`, and the port it enters on: through one of the group's
/// ports, or into a started loop's `LoopIn` (every wire into it comes from
/// outside the loop, on the port of the same name). Where a person hands
/// the value the wire would carry.
pub fn started_group_entered(g: &impl GraphView, edge: &Edge, at: &Located, starts: &BTreeSet<Located>) -> Option<(Located, String)> {
    if starts.is_empty() {
        return None;
    }
    let loop_entered = g.node(&at.id)
        .filter(|node| node.node_type == super::boundary_types::LOOP_IN)
        .and_then(|node| node.group_boundary.as_ref())
        .map(|boundary| Located::new(boundary.group_id.clone(), at.path.clone()))
        .filter(|place| starts.contains(place));
    if let Some(place) = loop_entered {
        return Some((place, edge.target_port().to_string()));
    }
    edge.via.iter().enumerate().find_map(|(k, crossing)| {
        if crossing.side != CrossingSide::In { return None; }
        let deeper = edge.via[k..].iter().filter(|c| c.side == CrossingSide::In && g.is_site(&c.group)).count();
        let path = at.path.len().checked_sub(deeper).map(|n| at.path[..n].to_vec())?;
        let place = Located::new(crossing.group.clone(), path);
        starts.contains(&place).then(|| (place, crossing.port.clone()))
    })
}

/// Whether the node at `place` runs inside the scope at `scope`.
fn inside(g: &ProjectGraph, place: &Located, scope: &Located) -> bool {
    scopes_around(g, place).contains(scope)
}

/// Whether the group at `group` is the scope at `scope` or sits in it.
fn group_inside(g: &ProjectGraph, group: &Located, scope: &Located) -> bool {
    group == scope || super::places::scopes_around_group(g, group).contains(scope)
}

/// The whole loop around `place` (the outermost), at its place: reaching
/// any part of one runs all of it.
fn whole_loop(g: &ProjectGraph, place: &Located) -> Vec<Located> {
    enclosing_loops(g, place).into_iter().next()
        .map(|around| members_in(g, &around.id, &around.path))
        .unwrap_or_default()
}

/// The nodes `feed` asks to run: for each fed start, the node feeding
/// each of its inputs that was not handed a value (for a group start, each
/// input of the group), found along the wire across any group border.
/// Only that node; what feeds IT stays outside unless the cut already
/// reaches it. A loop's result comes out of the loop as a whole: a loop
/// feeding a start runs whole.
fn feeders_of(
    g: &ProjectGraph,
    feed: &[String],
    handed: &BTreeMap<String, BTreeSet<String>>,
    node_starts: &BTreeSet<Located>,
    group_starts: &BTreeSet<Located>,
) -> Result<BTreeSet<Located>, String> {
    let mut feeders = BTreeSet::new();
    let nothing = BTreeSet::new();
    for spelled in feed {
        let handed = handed.get(spelled).unwrap_or(&nothing);
        let start = named(g, spelled)?.into_place();
        let mut sources = Vec::new();
        if group_starts.contains(&start) {
            let this_start = BTreeSet::from([start.clone()]);
            for member in members_in(g, &start.id, &start.path) {
                for (edge, source) in incoming(g, &member) {
                    let Some((_, port)) = started_group_entered(g, edge, &member, &this_start) else { continue };
                    if !handed.contains(&port) && !crate::run::skip::is_gate_port(&port) {
                        sources.push(source);
                    }
                }
            }
        } else if node_starts.contains(&start) {
            for (edge, source) in incoming(g, &start) {
                let port = edge.target_port();
                if !crate::run::skip::is_gate_port(port) && !handed.contains(port) {
                    sources.push(source);
                }
            }
        } else {
            return Err(format!("--feed {spelled}: only a start can be fed; name it with --from or --group too"));
        }
        for source in sources {
            if g.group(&source.id).is_some() { continue; }
            match enclosing_loops(g, &source).into_iter().next() {
                Some(around) => feeders.extend(members_in(g, &around.id, &around.path)),
                None => { feeders.insert(source); }
            }
        }
    }
    Ok(feeders)
}

/// Refuse a walk that holds part of a loop: a loop runs whole.
fn validate_loops(g: &ProjectGraph, walked: &Walk) -> Result<(), String> {
    let kept = walked.kept();
    let mut checked = BTreeSet::new();
    for place in &kept {
        for around in enclosing_loops(g, place) {
            if !checked.insert(around.clone()) { continue; }
            if members_in(g, &around.id, &around.path).iter().any(|member| !kept.contains(member)) {
                return Err(format!("cannot cut inside loop '{}'; select the whole loop", super::group_address(g.project(), &around.id, &around.path)));
            }
        }
    }
    Ok(())
}

/// The program a walk keeps, with what is `given` (see the module doc):
/// the nodes it keeps (each place a stand-in sends from among them), the
/// wires that carry something between them, and the scopes something in
/// it still waits on. Every other group is dissolved: its members become
/// members of the scope around it, and the wires stop naming it. Ids stay
/// the program's own, so the journal, the inspector, `weft diff` and
/// `--seed` line up.
///
/// A group is kept when something in the cut waits on it to open: a wire
/// lands on its gate from inside the cut, a member with no input runs (it
/// starts when the group opens), a member reads one of the group's own
/// ports (the group sends it when it opens), or its gate is written or
/// handed (a `false` keeps everything inside off). A call site and the
/// body it calls are kept whenever anything under the call is (the call
/// frame is what tells the calls apart), and a loop is whole or absent.
///
/// `given` is what the run is given; it keeps only what goes to a place
/// the cut keeps, and the cut says where that is.
///
/// Refused when the cut keeps a part of the program that does not compile
/// ([`ProjectDefinition::broken`]), naming every one: the program around a
/// broken part runs, and nothing that needs it does.
pub fn cut(project: &ProjectDefinition, walked: &Walk, given: &mut Given) -> Result<ProjectDefinition, String> {
    let g = ProjectGraph::new(project);
    let kept = walked.kept();
    // One reading with the walk's (`walk_up`): a trigger the run does not
    // fire, unless the run sets up (where every trigger takes what reaches
    // it), or it stands in.
    let unfired = |place: &Located| g.is_trigger(&place.id) && walked.fired.as_ref() != Some(place)
        && !walked.sets_up && !walked.stand_ins.contains(place);

    // The wires, each kept when it carries something at some place. A
    // wire into a trigger carries nothing but in a setup: a fired trigger
    // replays what its setup captured, and one nobody fired closes its
    // outputs.
    let mut edge_ids: BTreeSet<&str> = BTreeSet::new();
    let mut sending_groups: BTreeSet<&str> = BTreeSet::new();
    // The group a kept wire starts at, at its place: kept to send it.
    let mut senders: BTreeSet<Located> = BTreeSet::new();
    for place in &walked.runs {
        for (edge, source) in incoming(&g, place) {
            if g.is_trigger(&place.id) && !walked.sets_up { continue; }
            let from_scope = g.group(&source.id).is_some();
            if from_scope || kept.contains(&source) {
                edge_ids.insert(&edge.id);
                if from_scope {
                    sending_groups.insert(&edge.source);
                    senders.insert(source);
                }
            }
        }
    }
    // A group's gate, at every place the group runs, unless the gate was
    // handed a value there (the value is the gate's).
    let mut gated: BTreeSet<&str> = BTreeSet::new();
    let scope_places: BTreeSet<Located> = kept.iter().flat_map(|place| scopes_around(&g, place))
        .chain(senders.iter().flat_map(|group| {
            let mut around = super::places::scopes_around_group(&g, group);
            around.push(group.clone());
            around
        }))
        .collect();
    let gate_handed_at = |scope: &Located| given.inputs.get(scope)
        .is_some_and(|ports| ports.keys().any(|port| crate::run::skip::is_gate_port(port)));
    for scope in scope_places.iter().filter(|scope| !gate_handed_at(scope)) {
        for (edge, source) in incoming(&g, scope) {
            if !crate::run::skip::is_gate_port(edge.target_port()) || !kept.contains(&source) || unfired(&source) { continue; }
            edge_ids.insert(&edge.id);
            gated.insert(&edge.target);
        }
    }

    // The scopes something waits on.
    let incoming_kept = |place: &Located| incoming(&g, place).any(|(edge, _)| edge_ids.contains(edge.id.as_str()));
    let unfed_members: BTreeSet<String> = walked.runs.iter().chain(&walked.stand_ins)
        .filter(|place| walked.stand_ins.contains(*place) || !incoming_kept(place))
        .filter_map(|place| g.node(&place.id).and_then(|node| node.scope.last().cloned()))
        .collect();
    let used_scopes: BTreeSet<&str> = scope_places.iter().map(|scope| scope.id.as_str()).collect();
    let handed_gate = |group: &str| scope_places.iter().any(|scope| scope.id == group && gate_handed_at(scope));
    let keeps = |group: &GroupDefinition| -> bool {
        if !used_scopes.contains(group.id.as_str()) {
            return false;
        }
        match group.kind {
            GroupKind::Loop { .. } | GroupKind::Call { .. } | GroupKind::Body => true,
            GroupKind::Group => gated.contains(group.id.as_str())
                || unfed_members.contains(&group.id)
                || sending_groups.contains(group.id.as_str())
                || handed_gate(&group.id)
                || group.port_literals.keys().any(|port| crate::run::skip::is_gate_port(port)),
        }
    };
    let dissolved: BTreeSet<String> = project.groups.iter().filter(|group| !keeps(group)).map(|group| group.id.clone()).collect();

    let kept_ids: BTreeSet<&str> = kept.iter().map(|place| place.id.as_str()).collect();
    let nodes: Vec<super::NodeDefinition> = project.nodes.iter().filter(|node| kept_ids.contains(node.id.as_str())).map(|node| {
        let mut node = node.clone();
        node.scope.retain(|scope| !dissolved.contains(scope));
        node
    }).collect();
    let edges: Vec<Edge> = project.edges.iter().filter(|edge| edge_ids.contains(edge.id.as_str())).map(|edge| {
        let mut edge = edge.clone();
        edge.via.retain(|crossing| !dissolved.contains(&crossing.group));
        edge
    }).collect();

    // The places each site at the top of the call tree runs, when it runs
    // only part of its body.
    let parent_of = |group: &GroupDefinition| -> Option<String> {
        let mut at = group.parent_group_id.clone();
        while let Some(id) = at {
            if !dissolved.contains(&id) { return Some(id); }
            at = project.group(&id).and_then(|parent| parent.parent_group_id.clone());
        }
        None
    };
    let mut groups: Vec<GroupDefinition> = Vec::new();
    for group in project.groups.iter().filter(|group| !dissolved.contains(&group.id)) {
        let mut group = group.clone();
        group.parent_group_id = parent_of(&group);
        if let GroupKind::Call { runs, .. } = &mut group.kind {
            if super::places::body_of_group(&g, &group.id).is_none() {
                let under: BTreeSet<Located> = members_in(&g, &group.id, &[])
                    .into_iter().filter_map(|place| relative(&place, &group.id)).collect();
                let running: BTreeSet<Located> = kept.iter().filter_map(|place| relative(place, &group.id)).collect();
                *runs = (running != under).then_some(running);
            }
        }
        groups.push(group);
    }
    // Members and children, as the cut has them.
    let group_ids: BTreeSet<String> = groups.iter().map(|group| group.id.clone()).collect();
    let mut node_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for node in &nodes {
        if let Some(scope) = node.scope.last() {
            node_ids.entry(scope.clone()).or_default().push(node.id.clone());
        }
    }
    let mut children: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for group in &groups {
        if let Some(parent) = &group.parent_group_id {
            children.entry(parent.clone()).or_default().push(group.id.clone());
        }
    }
    for group in &mut groups {
        group.node_ids = node_ids.remove(&group.id).unwrap_or_default();
        group.child_group_ids = children.remove(&group.id).unwrap_or_default();
        group.child_group_ids.retain(|child| group_ids.contains(child));
    }

    // What the cut needs, for its broken parts: the nodes it keeps, the
    // groups it keeps as scopes (their gates, their own port values), and
    // every group a kept wire passes through, whether or not the cut keeps
    // that group as a scope (a plain group nothing waits on is dissolved,
    // and the wires through its ports still run). A group a kept node
    // merely sits in is not among them: an error about the group's own
    // ports or values is no business of a member that reads none of them.
    let in_cut: BTreeSet<&str> = nodes
        .iter()
        .map(|node| node.id.as_str())
        .chain(group_ids.iter().map(String::as_str))
        .chain(project.edges.iter().filter(|edge| edge_ids.contains(edge.id.as_str())).flat_map(|edge| edge.via.iter().map(|crossing| crossing.group.as_str())))
        .collect();
    refuse_broken(&project.broken, &in_cut)?;

    given.inputs.retain(|place, _| kept.contains(place) || group_ids.contains(&place.id));
    given.outputs.retain(|place, _| walked.stand_ins.contains(place));
    Ok(ProjectDefinition {
        id: project.id,
        nodes,
        edges,
        groups,
        created_at: project.created_at,
        updated_at: project.updated_at,
        defaults: project.defaults,
        given: given.places(),
        broken: Vec::new(),
        // Only drawn, and a cut is run, never drawn: every value one
        // carried is already on the input it points at.
        metadata_wires: Vec::new(),
    })
}

/// The refusal of a cut whose nodes and groups are `in_cut` and which
/// needs some of `broken`: every one it needs, in the program's order.
fn refuse_broken(broken: &[super::Broken], in_cut: &BTreeSet<&str>) -> Result<(), String> {
    // A reason over several lines (an included file's parse errors) keeps
    // its own lines under its first.
    let needed: Vec<String> = broken.iter().filter(|part| part.needed_by(in_cut)).map(|part| part.why.replace('\n', "\n    ")).collect();
    if needed.is_empty() {
        return Ok(());
    }
    Err(format!("it needs parts of the program that do not compile:\n  {}", needed.join("\n  ")))
}

/// `place` as seen from inside the site `site` at the top of the call
/// tree: its node, and the sites below `site` on its path; `None` for a
/// place not under it.
fn relative(place: &Located, site: &str) -> Option<Located> {
    let (first, below) = place.path.split_first()?;
    (first == site).then(|| Located::new(place.id.clone(), below.to_vec()))
}

