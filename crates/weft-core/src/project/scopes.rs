//! A program's scopes, indexed once: what the engine reads to drive its
//! groups, loops, call sites and included bodies.
//!
//! A group is not a node. It is a scope: its members sit in the program
//! with the group in their `scope`, and every wire that passes one of its
//! ports names it ([`super::Edge::via`]). What the engine needs to know
//! about a group is therefore what this index answers, per scope: who is
//! directly inside, which scopes sit inside it, which wires decide whether
//! it runs (its gate), which bring it values from outside (they can open
//! it), which take values out (it closes them when it does not run), and
//! which start at one of its own ports (it sends them when it opens).
//!
//! A scope runs once per COPY ([`ScopeCopy`]): the frames its direct
//! members fire at. A group inside a loop has one copy per iteration, a
//! body one per call (the frames end with the call frame of the site that
//! called it), a loop one per iteration (the iteration's frames).
//!
//! A loop the program holds without its boundaries (a cut of part of its
//! body, `super::cut`) is never launched: the run sits inside its
//! iteration 0, which opens with the copy around it ([`Scope::entered`]).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{CrossingSide, GroupKind, ProjectDefinition};
use crate::frames::{Frame, LoopFrames};

/// One copy of a scope: the scope, and the frames its direct members fire
/// at. A loop's copies are its iterations (the frames end with the
/// iteration's); a body's are its calls (they end with the call frame of
/// the site that called it).
// SYNC: ScopeCopy <-> packages/weft-graph/src/protocol.ts ScopeCopy
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScopeCopy {
    pub scope: String,
    pub frames: LoopFrames,
}

impl ScopeCopy {
    pub fn new(scope: impl Into<String>, frames: LoopFrames) -> Self {
        Self { scope: scope.into(), frames }
    }
}

impl std::fmt::Display for ScopeCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.frames.is_empty() {
            write!(f, "{}", self.scope)
        } else {
            write!(f, "{}#{}", self.scope, crate::frames::frames_text(&self.frames))
        }
    }
}

/// What kind of scope this is, as the engine drives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeKind {
    Group,
    /// Driven by its `LoopIn` / `LoopOut`: each iteration it launches is a
    /// copy, opened by the launch.
    Loop,
    /// A call site: its only content is the body it calls, one call frame
    /// deeper.
    Call { body: String },
    /// An included file's body: its copies are its calls.
    Body,
}

/// A wire, as one scope sees it: the wire, and the call sites between the
/// scope's copy and the wire's end inside it (the target of a wire coming
/// in, the source of one going out), outermost first. A copy at frames
/// `F` finds the wire's inner end at `F` plus one call frame per site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leg {
    /// The wire's position in `project.edges`.
    pub edge: usize,
    pub deeper: Vec<String>,
    /// A wire coming in whose source only runs after something inside the
    /// scope did (its value goes out and comes back): it can never be what
    /// opens the scope, so the scope never waits on it.
    pub fed_back: bool,
}

impl Leg {
    /// The frames of the wire's inner end, for the scope's copy at `frames`.
    pub fn inner_frames(&self, frames: &LoopFrames) -> LoopFrames {
        let mut inner = frames.clone();
        inner.extend(self.deeper.iter().map(|site| Frame::Call { site: site.clone() }));
        inner
    }
}

/// One scope of the program; see the module doc.
#[derive(Debug, Clone)]
pub struct Scope {
    pub id: String,
    pub kind: ScopeKind,
    /// The scope this one sits in, `None` at the top of its file (a body's
    /// parent is whichever site called it, read off the frames).
    pub parent: Option<String>,
    /// The nodes directly inside (a nested loop's `LoopIn` and `LoopOut`
    /// among them), by position in `project.nodes`.
    pub members: Vec<usize>,
    /// The nodes that fire directly in a copy of it, by position: its
    /// members less a nested loop's `LoopOut` (which fires in that loop's
    /// iterations), and, for a loop, its own `LoopOut`. The other side of
    /// [`ScopeIndex::node_copy`].
    pub firing: Vec<usize>,
    /// The scopes directly inside, loops included.
    pub children: Vec<String>,
    /// The wires that land on its gate.
    pub gates: Vec<usize>,
    /// The wires that start at one of its own ports: a literal on an
    /// input, an input nobody feeds, an output no member writes.
    pub sends: Vec<usize>,
    /// The wires that bring it a value from outside.
    pub entering: Vec<Leg>,
    /// The wires that take a value from inside it out.
    pub leaving: Vec<Leg>,
    /// A loop whose `LoopIn` the program does not hold: a cut of part of
    /// its body, which runs inside the loop's iteration 0. Nothing launches
    /// that iteration: it is a copy like a group's, opening with the copy
    /// around it (or at the run's start), and nothing of the loop's own
    /// runs (its gate, its other iterations, its outputs).
    pub entered: bool,
}

impl Scope {
    /// Whether a copy of it opens as a candidate when the copy around it
    /// opens (at the run's start for one at the top): a group, a site and
    /// an entered loop. A body opens under the site calling it, and a loop
    /// the program launches opens each iteration as it launches it.
    fn opens_with_its_parent(&self) -> bool {
        match self.kind {
            ScopeKind::Group | ScopeKind::Call { .. } => true,
            ScopeKind::Loop => self.entered,
            ScopeKind::Body => false,
        }
    }

    /// The frames of its copy inside the copy at `frames` around it: one
    /// iteration deeper for an entered loop (its iteration 0), the same
    /// frames for any other scope (a launched loop's are where its body is
    /// listed when it never iterated, [`ScopeIndex::inside`]).
    fn copy_frames(&self, frames: &LoopFrames) -> LoopFrames {
        let mut inner = frames.clone();
        if self.entered {
            inner.push(Frame::Loop { index: 0 });
        }
        inner
    }
}

/// Every scope of a program, by id, and the scopes at the top. Built once
/// per program ([`super::ProgramIndex`] holds one).
#[derive(Debug, Clone, Default)]
pub struct ScopeIndex {
    scopes: HashMap<String, Scope>,
    /// The scopes outside every other, in the program's group order: the
    /// top's groups, sites and entered loops (a body is never at the top:
    /// it runs under a site; nor a loop the program launches: its
    /// iterations open as it launches them).
    top: Vec<String>,
    /// The nodes that fire outside every scope (a top loop's `LoopOut`
    /// is not one: it fires in that loop's iterations).
    top_firing: Vec<usize>,
}

impl ScopeIndex {
    pub fn build(project: &ProjectDefinition) -> Self {
        let mut scopes: HashMap<String, Scope> = project.groups.iter().map(|group| {
            let kind = match &group.kind {
                GroupKind::Group => ScopeKind::Group,
                GroupKind::Loop { .. } => ScopeKind::Loop,
                GroupKind::Call { body, .. } => ScopeKind::Call { body: body.clone() },
                GroupKind::Body => ScopeKind::Body,
            };
            let entered = kind == ScopeKind::Loop
                && !project.nodes.iter().any(|node| node.id == super::loop_in_id(&group.id));
            let scope = Scope {
                id: group.id.clone(),
                kind,
                entered,
                parent: group.parent_group_id.clone(),
                members: Vec::new(),
                firing: Vec::new(),
                children: Vec::new(),
                gates: Vec::new(),
                sends: Vec::new(),
                entering: Vec::new(),
                leaving: Vec::new(),
            };
            (group.id.clone(), scope)
        }).collect();
        let mut top = Vec::new();
        for group in &project.groups {
            match &group.parent_group_id {
                Some(parent) => {
                    if let Some(parent) = scopes.get_mut(parent) {
                        parent.children.push(group.id.clone());
                    }
                }
                None if scopes[&group.id].opens_with_its_parent() => top.push(group.id.clone()),
                None => {}
            }
        }
        let mut top_firing = Vec::new();
        for (at, node) in project.nodes.iter().enumerate() {
            if let Some(scope) = node.scope.last().and_then(|scope| scopes.get_mut(scope)) {
                scope.members.push(at);
            }
            match firing_scope(node) {
                Some(scope) => {
                    if let Some(scope) = scopes.get_mut(scope) {
                        scope.firing.push(at);
                    }
                }
                None => top_firing.push(at),
            }
        }
        let is_site = |id: &str| project.groups.iter().any(|g| g.id == id && matches!(g.kind, GroupKind::Call { .. }));
        for (at, edge) in project.edges.iter().enumerate() {
            if edge.to_group_port() {
                // A port nobody on the far side reads: nothing travels it.
                continue;
            }
            if edge.from_group_port() {
                if let Some(scope) = scopes.get_mut(&edge.source) {
                    scope.sends.push(at);
                }
            } else if crate::run::skip::is_gate_port(edge.target_port()) {
                if let Some(scope) = scopes.get_mut(&edge.target) {
                    scope.gates.push(at);
                }
            }
            let from_own_port = edge.from_group_port();
            for (k, crossing) in edge.via.iter().enumerate() {
                let Some(scope) = scopes.get_mut(&crossing.group) else { continue };
                match crossing.side {
                    CrossingSide::In => {
                        // A group's own literal comes from inside it.
                        if k == 0 && from_own_port { continue; }
                        let deeper = edge.via[k..].iter()
                            .filter(|c| c.side == CrossingSide::In && is_site(&c.group))
                            .map(|c| c.group.clone()).collect();
                        scope.entering.push(Leg { edge: at, deeper, fed_back: false });
                    }
                    CrossingSide::Out => {
                        let mut deeper: Vec<String> = edge.via[..=k].iter()
                            .filter(|c| c.side == CrossingSide::Out && is_site(&c.group))
                            .map(|c| c.group.clone()).collect();
                        deeper.reverse();
                        scope.leaving.push(Leg { edge: at, deeper, fed_back: false });
                    }
                }
            }
        }
        let mut index = Self { scopes, top, top_firing };
        index.mark_fed_back(project);
        index
    }

    /// Mark every wire coming into a scope whose source is downstream of
    /// something inside it ([`Leg::fed_back`]), place by place (a node of an
    /// included file under one call is not the same node under another).
    /// Places are read from where the scope sits: in its own file, so a
    /// scope inside an included file is walked within one call of it (a
    /// wire leaving the file has no call to return to, and the walk stops
    /// there). A body itself is no such scope: what reaches it comes through
    /// its calls, and a wire from one call into another is a value from
    /// outside, never one it sent.
    fn mark_fed_back(&mut self, project: &ProjectDefinition) {
        let g = super::graph::ProjectGraph::new(project);
        let ids: Vec<String> = self.scopes.keys().cloned().collect();
        for id in ids {
            if self.scopes[&id].kind == ScopeKind::Body { continue; }
            let inside = super::places::members_in(&g, &id, &[]);
            if inside.is_empty() { continue; }
            let reached = runs_after(&g, &inside);
            let scope = self.scopes.get_mut(&id).expect("an indexed scope");
            for leg in &mut scope.entering {
                let edge = &project.edges[leg.edge];
                let target = crate::frames::Located::new(edge.target.clone(), leg.deeper.clone());
                leg.fed_back = super::places::step(&g, &target, edge, super::places::Direction::Upstream)
                    .is_some_and(|source| reached.contains(&source));
            }
        }
    }

    /// The sites that call the included body `body`.
    pub fn sites_calling<'a>(&'a self, body: &'a str) -> impl Iterator<Item = &'a Scope> + 'a {
        self.scopes.values().filter(move |scope| matches!(&scope.kind, ScopeKind::Call { body: called } if called == body))
    }

    /// The scope with this id.
    pub fn get(&self, id: &str) -> Option<&Scope> {
        self.scopes.get(id)
    }

    /// The nodes that fire directly in a copy of `scope` (outside every
    /// scope for `None`), by position: [`Scope::firing`]. Empty for a
    /// scope the program does not have.
    pub fn firing_in(&self, scope: Option<&str>) -> &[usize] {
        match scope {
            Some(scope) => self.scopes.get(scope).map_or(&[], |scope| &scope.firing),
            None => &self.top_firing,
        }
    }

    /// The copy of the scope a node at `frames` fires DIRECTLY in, or
    /// `None` for a node outside every scope ([`firing_scope`]).
    pub fn node_copy(&self, node: &super::NodeDefinition, frames: &LoopFrames) -> Option<ScopeCopy> {
        firing_scope(node).map(|scope| ScopeCopy::new(scope, frames.clone()))
    }

    /// The copy `copy` runs inside: `Ok(None)` at the top. A loop's
    /// iteration sits in the loop's own scope one frame up; a body's call
    /// in the site its call frame names, one frame up; any other scope in
    /// its parent at the same frames. `Err` when the frames cannot be the
    /// copy's (a body's that do not end on a call of one of its sites, a
    /// loop's that do not end on an iteration), which only a corrupt record
    /// produces.
    pub fn parent_of(&self, copy: &ScopeCopy) -> Result<Option<ScopeCopy>, String> {
        let scope = self.scopes.get(&copy.scope).ok_or_else(|| format!("no scope '{}' in the program", copy.scope))?;
        match &scope.kind {
            ScopeKind::Loop => {
                let Some((Frame::Loop { .. }, above)) = copy.frames.split_last() else {
                    return Err(format!("'{copy}' is no iteration of the loop '{}'", copy.scope));
                };
                Ok(scope.parent.as_ref().map(|parent| ScopeCopy::new(parent.clone(), above.to_vec())))
            }
            ScopeKind::Body => {
                let Some((Frame::Call { site }, above)) = copy.frames.split_last() else {
                    return Err(format!("'{copy}' is no call of the included body '{}'", copy.scope));
                };
                let calls_it = self.scopes.get(site).is_some_and(|s| s.kind == ScopeKind::Call { body: copy.scope.clone() });
                if !calls_it {
                    return Err(format!("'{copy}' names the site '{site}', which does not call '{}'", copy.scope));
                }
                Ok(Some(ScopeCopy::new(site.clone(), above.to_vec())))
            }
            ScopeKind::Group | ScopeKind::Call { .. } => {
                Ok(scope.parent.as_ref().map(|parent| ScopeCopy::new(parent.clone(), copy.frames.clone())))
            }
        }
    }

    /// The copies `copy` opens as candidates when it opens: its groups and
    /// sites at its frames, an entered loop's iteration 0 one iteration
    /// deeper, and a site's body one call frame deeper. A loop the program
    /// launches is not among them: its iterations open as it launches them.
    pub fn children_of(&self, copy: &ScopeCopy) -> Vec<ScopeCopy> {
        let Some(scope) = self.scopes.get(&copy.scope) else { return Vec::new() };
        let mut out: Vec<ScopeCopy> = scope.children.iter()
            .filter_map(|child| self.scopes.get(child))
            .filter(|child| child.opens_with_its_parent())
            .map(|child| ScopeCopy::new(child.id.clone(), child.copy_frames(&copy.frames)))
            .collect();
        if let ScopeKind::Call { body } = &scope.kind {
            let mut frames = copy.frames.clone();
            frames.push(Frame::Call { site: copy.scope.clone() });
            out.push(ScopeCopy::new(body.clone(), frames));
        }
        out
    }

    /// The frames of the copy of `scope` (the top of a file for `None`)
    /// reached through the call path `path` that a run opens without
    /// launching anything: one call frame per site on the path, and
    /// iteration 0 of each entered loop around it (itself included),
    /// outermost first. What a node sitting directly in `scope` fires at
    /// when the run kicks it, hands it a value or has it stand in. A node
    /// inside a loop the program launches fires at the iterations it
    /// launches instead.
    pub fn frames_at(&self, scope: Option<&str>, path: &[String]) -> LoopFrames {
        let mut frames = LoopFrames::new();
        for site in path {
            let above = self.scopes.get(site).and_then(|site| site.parent.as_deref());
            self.push_entered(above, &mut frames);
            frames.push(Frame::Call { site: site.clone() });
        }
        self.push_entered(scope, &mut frames);
        frames
    }

    /// Push iteration 0 of every entered loop from the top of the file
    /// down to `scope`, itself included.
    fn push_entered(&self, scope: Option<&str>, frames: &mut LoopFrames) {
        let mut chain = Vec::new();
        let mut at = scope.and_then(|id| self.scopes.get(id));
        while let Some(here) = at {
            chain.push(here);
            at = here.parent.as_deref().and_then(|parent| self.scopes.get(parent));
        }
        frames.extend(chain.into_iter().rev().filter(|scope| scope.entered).map(|_| Frame::Loop { index: 0 }));
    }

    /// The copies at the top of a run, candidates the moment it starts.
    pub fn top_copies(&self) -> Vec<ScopeCopy> {
        self.top.iter().filter_map(|id| self.scopes.get(id))
            .map(|scope| ScopeCopy::new(scope.id.clone(), scope.copy_frames(&Vec::new())))
            .collect()
    }

    /// Everything inside `copy`, however deep, at the frames each would
    /// run at: what a copy that does not run takes down with it. Its
    /// groups and sites at its frames, each site's body one call frame
    /// deeper, and so on down. A loop inside never iterated, so what is
    /// in its body is listed at the loop's own frames: that is how it
    /// reads as taken down by the scope around the loop, never by an
    /// iteration. The loop itself is no copy (its iterations are). An
    /// entered loop's iteration 0 is a copy like a group's, one iteration
    /// deeper.
    pub fn inside(&self, copy: &ScopeCopy) -> Inside {
        let mut inside = Inside::default();
        let mut pending = vec![copy.clone()];
        let mut first = true;
        while let Some(next) = pending.pop() {
            let Some(scope) = self.scopes.get(&next.scope) else { continue };
            if !first && (scope.kind != ScopeKind::Loop || scope.entered) {
                inside.copies.push(next.clone());
            }
            first = false;
            inside.nodes.extend(scope.members.iter().map(|&at| (at, next.frames.clone())));
            pending.extend(scope.children.iter().filter_map(|child| self.scopes.get(child))
                .map(|child| ScopeCopy::new(child.id.clone(), child.copy_frames(&next.frames))));
            if let ScopeKind::Call { body } = &scope.kind {
                let mut frames = next.frames.clone();
                frames.push(Frame::Call { site: next.scope.clone() });
                pending.push(ScopeCopy::new(body.clone(), frames));
            }
        }
        inside.copies.sort();
        inside.nodes.sort();
        inside
    }
}

/// The scope a node fires directly in, `None` outside every scope. A
/// loop's `LoopOut` fires once per iteration, inside the iteration's copy
/// (though it sits beside the loop, in the scope around it); every other
/// node (a loop's `LoopIn` among them) in the innermost scope of its own.
fn firing_scope(node: &super::NodeDefinition) -> Option<&str> {
    fires_per_iteration_of(node).or_else(|| node.scope.last().map(String::as_str))
}

/// The loop whose iterations a node fires in although it sits beside that
/// loop: a loop's `LoopOut`. `None` for every other node. The one reading
/// of that rule, for [`firing_scope`] and for whoever asks what an
/// iteration holds (`crate::seeding`, `crate::run::forget`).
pub fn fires_per_iteration_of(node: &super::NodeDefinition) -> Option<&str> {
    (node.node_type == super::boundary_types::LOOP_OUT)
        .then(|| node.group_boundary.as_ref().map(|boundary| boundary.group_id.as_str()))
        .flatten()
}

/// The places that run only after `seeds` did (the seeds included):
/// along wires, and at a scope's gate everything inside that scope. Not a
/// run's walk (`super::cut::downstream`), which takes a loop whole once it
/// reaches the loop's start: a node of the loop a seed sits in can run
/// before the seed in each iteration, so it does not wait on it.
fn runs_after(g: &super::graph::ProjectGraph, seeds: &[crate::frames::Located]) -> std::collections::BTreeSet<crate::frames::Located> {
    use super::graph::GraphView;
    let mut reached = std::collections::BTreeSet::new();
    let mut pending: Vec<crate::frames::Located> = seeds.to_vec();
    while let Some(place) = pending.pop() {
        if !reached.insert(place.clone()) { continue; }
        for (edge, target) in super::places::outgoing(g, &place) {
            if g.group(&edge.target).is_some() {
                pending.extend(super::places::members_in(g, &target.id, &target.path));
            } else {
                pending.push(target);
            }
        }
    }
    reached
}

/// What [`ScopeIndex::inside`] lists.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Inside {
    /// The nodes, by position in `project.nodes`, each at its frames.
    pub nodes: Vec<(usize, LoopFrames)>,
    /// The copies of the scopes (never a loop's).
    pub copies: Vec<ScopeCopy>,
}
