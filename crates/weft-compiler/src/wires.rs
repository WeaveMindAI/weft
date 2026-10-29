//! From the lines a person wrote to the wires that run.
//!
//! A person wires a group through its ports: `g.x = a.out` outside, then
//! `m.in = self.x` inside, two LEGS. The program that runs has no node at
//! a group's port, so the legs are joined end to end into one wire from the
//! node that produces the value to the node that reads it, naming every
//! group port it passes ([`weft_core::project::Edge::via`]). A call site
//! is a group whose inside is the body it calls: entering the site enters
//! the body, and leaving the body leaves through every site that calls it.
//!
//! Three kinds of wire come out besides the plain one:
//! - a wire landing on a group's gate (`g._should_flow = a.ok`) ends at the
//!   group;
//! - a wire starting at a group's own port carries what is written there
//!   when the group opens: the literal on an input (`g.x = 5`), or nothing
//!   (an input nobody feeds, an output no member writes);
//! - a leg that reaches a group's port nobody on the far side reads ends at
//!   the group, carrying nothing, so the editor still draws it.
//!
//! A loop is not joined through: its `LoopIn` and `LoopOut` are nodes, and
//! a leg to or from a loop's port lands on them.

use std::collections::{BTreeMap, BTreeSet};

use weft_core::project::{Crossing, CrossingSide, Edge, Span};

use crate::weft_compiler::SourceError;

/// One end of a leg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum End {
    /// A node (a loop's boundary among them).
    Node(String),
    /// A group's port, from outside it (`g.x`).
    Outer(String),
    /// A group's port, from inside it (`self.x`).
    Inner(String),
}

/// One line a person wrote, its ends resolved.
#[derive(Debug, Clone)]
pub(crate) struct Leg {
    pub(crate) source: End,
    pub(crate) source_port: String,
    pub(crate) target: End,
    pub(crate) target_port: String,
    pub(crate) path: Vec<String>,
    pub(crate) span: Option<Span>,
    pub(crate) source_file: Option<String>,
}

/// What the joining needs to know of a group that is not a loop.
#[derive(Debug, Clone)]
pub(crate) struct Scope {
    pub(crate) id: String,
    /// The body it calls, for a call site.
    pub(crate) calls: Option<String>,
    pub(crate) is_body: bool,
    pub(crate) in_ports: Vec<String>,
    pub(crate) out_ports: Vec<String>,
}

/// Where a wire is on its way: a face of a group's port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Face {
    /// An input, seen from outside (where `g.x = ...` lands).
    OuterIn,
    /// An input, seen from inside (what `... = self.x` reads).
    InnerIn,
    /// An output, seen from inside (where `self.y = ...` lands).
    InnerOut,
    /// An output, seen from outside (what `... = g.y` reads).
    OuterOut,
}

type Port = (String, String, Face);

/// The leg a wire arrived on, whose line a crossing keeps.
#[derive(Debug, Clone, Default)]
struct Arrived {
    path: Vec<String>,
    span: Option<Span>,
    source_file: Option<String>,
}

/// Join `legs` into the program's wires. `scopes` are every group that is
/// not a loop. Every leg ends up on a wire or refused: a leg starting at a
/// group port the group does not declare (`m.in = self.typo`) still sends
/// from that port, so its wire exists and validate names the typo on that
/// leg's line, and a loop of legs through group ports with no node on it
/// is refused once, naming the group and port it loops at.
pub(crate) fn join(legs: &[Leg], scopes: &[Scope], errors: &mut Vec<SourceError>) -> Vec<Edge> {
    let by_id: BTreeMap<&str, &Scope> = scopes.iter().map(|s| (s.id.as_str(), s)).collect();
    let sites_of: BTreeMap<&str, Vec<&str>> = {
        let mut sites: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for scope in scopes {
            if let Some(body) = &scope.calls { sites.entry(body.as_str()).or_default().push(scope.id.as_str()); }
        }
        sites
    };
    // The legs leaving each port face, and the faces something lands on.
    let mut from_face: BTreeMap<Port, Vec<usize>> = BTreeMap::new();
    let mut landed: BTreeSet<Port> = BTreeSet::new();
    for (index, leg) in legs.iter().enumerate() {
        if let Some(face) = source_face(leg) {
            from_face.entry(face).or_default().push(index);
        }
        if let Some(face) = target_face(leg) {
            landed.insert(face);
        }
    }
    let mut joiner = Joiner {
        legs,
        by_id: &by_id,
        sites_of: &sites_of,
        from_face: &from_face,
        edges: BTreeMap::new(),
        followed: BTreeSet::new(),
        refused: BTreeSet::new(),
        probing: false,
        errors,
    };
    for (index, leg) in legs.iter().enumerate() {
        if let End::Node(source) = &leg.source {
            joiner.follow_leg(index, source, &leg.source_port, Vec::new(), Vec::new(), &mut Vec::new());
        }
    }
    // What starts at a group's own ports.
    for scope in scopes {
        if !scope.is_body {
            for port in &scope.in_ports {
                if landed.contains(&(scope.id.clone(), port.clone(), Face::OuterIn)) { continue; }
                joiner.send_own(&scope.id, port, CrossingSide::In);
            }
        }
        if scope.calls.is_none() {
            for port in &scope.out_ports {
                if landed.contains(&(scope.id.clone(), port.clone(), Face::InnerOut)) { continue; }
                joiner.send_own(&scope.id, port, CrossingSide::Out);
            }
        }
    }
    // A leg nothing reached starts at a group port. When the group does
    // not declare that port, it sends from it all the same: the wire is
    // made, and validate reports the unknown port on the leg's own line.
    for (index, leg) in legs.iter().enumerate() {
        if joiner.followed.contains(&index) { continue; }
        let Some((group, port, face)) = source_face(leg) else { continue };
        let Some(scope) = by_id.get(group.as_str()) else { continue };
        let side = match face {
            Face::InnerIn if !scope.in_ports.contains(&port) => CrossingSide::In,
            Face::OuterOut if !scope.out_ports.contains(&port) => CrossingSide::Out,
            _ => continue,
        };
        joiner.send_own(&group, &port, side);
    }
    // What is still unreached starts at a declared port only another
    // unreached leg lands on: a loop of group ports with no node on it
    // (refused as the walk comes back round), or a leg past one. Walk
    // each to find the loop, making no wire.
    joiner.probing = true;
    for index in 0..legs.len() {
        if joiner.followed.contains(&index) { continue; }
        joiner.follow_leg(index, "", "", Vec::new(), Vec::new(), &mut Vec::new());
    }
    joiner.edges.into_values().collect()
}

/// A port face a wire passed, and the calls (sites, outermost first) it
/// passed it under.
type Passed = (Port, Vec<String>);

/// The port face a leg starts at, when it starts at a group's port.
fn source_face(leg: &Leg) -> Option<Port> {
    match &leg.source {
        End::Node(_) => None,
        End::Outer(group) => Some((group.clone(), leg.source_port.clone(), Face::OuterOut)),
        End::Inner(group) => Some((group.clone(), leg.source_port.clone(), Face::InnerIn)),
    }
}

/// The port face a leg lands on, when it lands on a group's port (a gate
/// is not one: it lands on the group).
fn target_face(leg: &Leg) -> Option<Port> {
    match &leg.target {
        End::Node(_) => None,
        End::Outer(_) if weft_core::run::skip::is_gate_port(&leg.target_port) => None,
        End::Outer(group) => Some((group.clone(), leg.target_port.clone(), Face::OuterIn)),
        End::Inner(group) => Some((group.clone(), leg.target_port.clone(), Face::InnerOut)),
    }
}

struct Joiner<'a, 'e> {
    legs: &'a [Leg],
    by_id: &'a BTreeMap<&'a str, &'a Scope>,
    sites_of: &'a BTreeMap<&'a str, Vec<&'a str>>,
    from_face: &'a BTreeMap<Port, Vec<usize>>,
    edges: BTreeMap<String, Edge>,
    /// Every leg some walk followed.
    followed: BTreeSet<usize>,
    /// Every port loop refused, as the faces it passes (sorted, so a loop
    /// walked into at any face is the same one), so each is refused once.
    refused: BTreeSet<Vec<Port>>,
    /// Walking only to find port loops: no wire is made.
    probing: bool,
    errors: &'e mut Vec<SourceError>,
}

impl Joiner<'_, '_> {
    /// Send from `group`'s own `port`: an input as the group opens
    /// (`side` In), an output nothing inside writes (`side` Out).
    fn send_own(&mut self, group: &str, port: &str, side: CrossingSide) {
        let own = Crossing { group: group.to_string(), port: port.to_string(), side, path: Vec::new(), span: None, source_file: None };
        match side {
            CrossingSide::In => self.enter(group, port, group, port, vec![own], Vec::new(), &mut Vec::new(), true),
            CrossingSide::Out => self.leave(group, port, group, port, vec![own], Vec::new(), &mut Vec::new(), true),
        }
    }

    /// Follow leg `index`, which the wire from `source.source_port`
    /// arrived on with `via` and `path` so far.
    #[allow(clippy::too_many_arguments)]
    fn follow_leg(&mut self, index: usize, source: &str, source_port: &str, via: Vec<Crossing>, path: Vec<String>, seen: &mut Vec<Passed>) {
        self.followed.insert(index);
        let legs = self.legs;
        let leg = &legs[index];
        let mut path = path;
        path.extend(leg.path.iter().cloned());
        let arrived = Arrived { path: leg.path.clone(), span: leg.span, source_file: leg.source_file.clone() };
        match &leg.target {
            End::Node(target) => {
                self.emit(source, source_port, target, &leg.target_port, via, path, leg.span, leg.source_file.clone());
            }
            End::Outer(group) if weft_core::run::skip::is_gate_port(&leg.target_port) => {
                self.emit(source, source_port, group, &leg.target_port, via, path, leg.span, leg.source_file.clone());
            }
            End::Outer(group) => {
                let mut via = via;
                via.push(Crossing { group: group.clone(), port: leg.target_port.clone(), side: CrossingSide::In, path: arrived.path, span: arrived.span, source_file: arrived.source_file });
                self.enter(source, source_port, group, &leg.target_port, via, path, seen, false);
            }
            End::Inner(group) => {
                let mut via = via;
                via.push(Crossing { group: group.clone(), port: leg.target_port.clone(), side: CrossingSide::Out, path: arrived.path, span: arrived.span, source_file: arrived.source_file });
                self.leave(source, source_port, group, &leg.target_port, via, path, seen, false);
            }
        }
    }

    /// The wire has crossed into `group` through `port` (its crossing is
    /// the last of `via`): follow every leg that reads `self.port` inside,
    /// and for a call site, into its body. With nothing reading it, the
    /// wire ends at the port (a send that reaches nothing is no wire).
    #[allow(clippy::too_many_arguments)]
    fn enter(&mut self, source: &str, source_port: &str, group: &str, port: &str, via: Vec<Crossing>, path: Vec<String>, seen: &mut Vec<Passed>, sending: bool) {
        let face = (group.to_string(), port.to_string(), Face::InnerIn);
        if !self.mark(&face, seen, &via) { return; }
        if let Some(body) = self.by_id.get(group).and_then(|scope| scope.calls.clone()) {
            let mut via = via;
            via.push(Crossing { group: body.clone(), port: port.to_string(), side: CrossingSide::In, path: Vec::new(), span: None, source_file: None });
            self.enter(source, source_port, &body, port, via, path, seen, sending);
        } else {
            let legs = self.from_face.get(&face).cloned().unwrap_or_default();
            if legs.is_empty() && !sending {
                self.emit(source, source_port, group, port, via.clone(), path.clone(), None, None);
            }
            for leg in legs {
                self.follow_leg(leg, source, source_port, via.clone(), path.clone(), seen);
            }
        }
        seen.pop();
    }

    /// The wire has crossed out of `group` through `port` (its crossing is
    /// the last of `via`): follow every leg that reads `group.port`
    /// outside, and for a body, out through the site it came in by, or,
    /// for a wire that started inside the body, through every site that
    /// calls it.
    #[allow(clippy::too_many_arguments)]
    fn leave(&mut self, source: &str, source_port: &str, group: &str, port: &str, via: Vec<Crossing>, path: Vec<String>, seen: &mut Vec<Passed>, sending: bool) {
        let face = (group.to_string(), port.to_string(), Face::OuterOut);
        if !self.mark(&face, seen, &via) { return; }
        if self.by_id.get(group).is_some_and(|scope| scope.is_body) {
            // The call it is in: the site whose crossing comes right before
            // the latest entry into this body.
            let entered_by = via.iter().rposition(|c| c.group == group && c.side == CrossingSide::In)
                .and_then(|at| at.checked_sub(1))
                .map(|at| via[at].group.clone());
            let sites: Vec<String> = match entered_by {
                Some(site) => vec![site],
                None => self.sites_of.get(group).cloned().unwrap_or_default().into_iter().map(str::to_string).collect(),
            };
            for site in &sites {
                let site = site.as_str();
                let mut via = via.clone();
                via.push(Crossing { group: site.to_string(), port: port.to_string(), side: CrossingSide::Out, path: Vec::new(), span: None, source_file: None });
                self.leave(source, source_port, site, port, via, path.clone(), seen, sending);
            }
        } else {
            let legs = self.from_face.get(&face).cloned().unwrap_or_default();
            if legs.is_empty() && !sending {
                self.emit(source, source_port, group, port, via.clone(), path.clone(), None, None);
            }
            for leg in legs {
                self.follow_leg(leg, source, source_port, via.clone(), path.clone(), seen);
            }
        }
        seen.pop();
    }

    /// Note the wire passing `face` under the calls it is in (a file's port
    /// passed under two calls of it is two places); `false` when it passed
    /// it already: a loop through group ports with no node on it, refused
    /// (once, on the line the wire came back in by) unless that loop was
    /// refused before.
    fn mark(&mut self, face: &Port, seen: &mut Vec<Passed>, via: &[Crossing]) -> bool {
        let mut calls: Vec<String> = Vec::new();
        for crossing in via {
            if self.by_id.get(crossing.group.as_str()).is_some_and(|scope| scope.calls.is_some()) {
                match crossing.side {
                    CrossingSide::In => calls.push(crossing.group.clone()),
                    CrossingSide::Out => { calls.pop(); }
                }
            }
        }
        let here = (face.clone(), calls);
        if let Some(at) = seen.iter().position(|passed| *passed == here) {
            // The same loop, found from wherever a walk entered it.
            let mut round: Vec<Port> = seen[at..].iter().map(|(port, _)| port.clone()).collect();
            round.sort();
            if !self.refused.insert(round) { return false; }
            let (group, port, _) = face;
            let line = via.iter().rev().find(|crossing| crossing.span.is_some());
            self.errors.push(SourceError::at(
                line.and_then(|crossing| crossing.span).unwrap_or_default(),
                format!(
                    "the wires through '{}.{port}' come back to it without passing any node: a value \
                     there would go round for ever. Remove one of the lines that join the group's ports",
                    weft_core::project::plain_id(group)
                ),
            ).in_file(line.and_then(|crossing| crossing.source_file.as_deref()))
            // The loop is the group's ports' business: it breaks the group
            // (and a cut that runs nothing of it never notices), never the
            // program around it.
            .about(group));
            return false;
        }
        seen.push(here);
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn emit(&mut self, source: &str, source_port: &str, target: &str, target_port: &str, via: Vec<Crossing>, path: Vec<String>, span: Option<Span>, source_file: Option<String>) {
        if self.probing { return; }
        let id = wire_id(source, source_port, &via, target, target_port, &path);
        self.edges.entry(id.clone()).or_insert(Edge {
            id,
            source: source.to_string(),
            target: target.to_string(),
            source_handle: Some(source_port.to_string()),
            target_handle: Some(target_port.to_string()),
            path,
            via,
            span,
            source_file,
        });
    }
}

/// The id of a wire: its two ends, every group port it passes, and the keys
/// each of its legs reads (`path`, the whole wire's), so a body's member
/// wired once per site that calls it has one wire per site, and two lines
/// reading different keys of one port into one input are two wires (which
/// validate then refuses as two drivers), never one silently kept.
pub(crate) fn wire_id(source: &str, source_port: &str, via: &[Crossing], target: &str, target_port: &str, path: &[String]) -> String {
    let keys = |keys: &[String]| if keys.is_empty() { String::new() } else { format!(".{}", keys.join(".")) };
    let mut id = format!("e-{source}-{source_port}");
    let mut read = 0;
    for crossing in via {
        let side = match crossing.side { CrossingSide::In => "in", CrossingSide::Out => "out" };
        id.push_str(&format!("-{side}:{}:{}{}", crossing.group, crossing.port, keys(&crossing.path)));
        read += crossing.path.len();
    }
    id.push_str(&format!("-{target}-{target_port}{}", keys(path.get(read..).unwrap_or_default())));
    id
}
