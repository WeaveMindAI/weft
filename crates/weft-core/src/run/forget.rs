//! What a run's tables forget once nothing can need it any more: a firing
//! once it ended (with the pulses it took and the answers it got), and a
//! loop's iteration once nothing inside it is alive (what it decided,
//! kicked, sent and ran inside). The same forgetting runs wherever a run's
//! tables are held for long: the worker driving the run, its fold, and a
//! reader showing the run (`weft_journal::Fold`). Their memory then holds
//! what is open, never what the run already did.
//!
//! A forgotten firing is still known to have ended: where it ended is
//! marked, and an iteration forgotten is kept as a range of indices of its
//! loop, so a second start where a firing ended is still refused.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::frames::{Frame, FiringLocation, LoopFrames};
use crate::primitive::{AwaitedEntry, KickTable, LoopInstanceKey};
use crate::project::scopes::{ScopeCopy, ScopeKind};
use crate::project::{loop_in_id, ProgramIndex, ProjectDefinition};
use crate::pulse::PulseTable;
use crate::run::loop_runtime::{iteration_frames, Indices, LoopNews, LoopRuntime};
use crate::run::scope::ScopeTable;
use crate::run::{NodeFiring, NodeFiringStatus, NodeFiringTable};

/// A run's tables, as whoever holds them lends them to forget in.
pub struct Tables<'a> {
    pub pulses: &'a mut PulseTable,
    pub firings: &'a mut NodeFiringTable,
    pub kicked: &'a mut KickTable,
    pub scopes: &'a mut ScopeTable,
    pub awaited_sequences: &'a mut HashMap<FiringLocation, Vec<AwaitedEntry>>,
    pub loop_runtime: &'a mut LoopRuntime,
}

/// A firing forgotten ([`Forgetting::forget_firing`]): its record and the
/// answers it was given.
pub struct Forgotten {
    pub record: NodeFiring,
    pub answers: Vec<AwaitedEntry>,
}

/// What a run forgot, and what it still has to look at to forget more.
#[derive(Debug, Clone, Default)]
pub struct Forgetting {
    /// Where a forgotten firing ended, outside every iteration forgotten
    /// since (those are [`Self::finished`]).
    ended: HashSet<FiringLocation>,
    /// The iterations launched and not forgotten yet, by their copy, with
    /// their loop and index.
    iterations: BTreeMap<ScopeCopy, (LoopInstanceKey, u32)>,
    /// Of those, the ones whose `LoopOut` fired or whose loop ended: the
    /// ones to forget once nothing inside is alive.
    due: BTreeSet<ScopeCopy>,
    /// Per loop, the iterations forgotten.
    finished: HashMap<LoopInstanceKey, Indices>,
    /// The loops whose iterations are never forgotten (a seed's ancestor
    /// keeps the loops it hands down whole).
    whole_loops: BTreeSet<String>,
    /// Whether the run's first failure is kept (see [`Self::first_failure`]).
    keeps_first_failure: bool,
    /// The failed firing kept, by what makes it the first
    /// (`NodeFiring::failure_order`).
    first_failure: Option<((u64, String, u64), FiringLocation)>,
}

impl Forgetting {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep the run's first failure even once it ended: the one a run
    /// that fails names (the earliest to end, then by node and start), so
    /// a worker's ending says the cause and not a later failure.
    pub fn keeping_first_failure(mut self) -> Self {
        self.keeps_first_failure = true;
        self
    }

    /// Never forget an iteration of the loops `loops`.
    pub fn keeping_loops(mut self, loops: BTreeSet<String>) -> Self {
        self.whole_loops = loops;
        self
    }

    /// How many entries this holds: what a test of a run's memory
    /// compares across run lengths.
    pub fn holds(&self) -> usize {
        self.ended.len() + self.iterations.len() + self.due.len()
            + self.finished.values().map(Indices::runs).sum::<usize>()
    }

    /// Learn what happened to the loops' iterations since last time
    /// (`LoopRuntime::take_news`, which this asks to be told).
    pub fn hear(&mut self, loop_runtime: &mut LoopRuntime) {
        loop_runtime.report();
        for news in loop_runtime.take_news() {
            match news {
                LoopNews::Launched(key, index) => {
                    if !self.whole_loops.contains(&key.group_id) {
                        let copy = ScopeCopy::new(key.group_id.clone(), iteration_frames(&key.parent_frames, index));
                        self.iterations.insert(copy, (key, index));
                    }
                }
                LoopNews::OutFired(key, index) => {
                    let copy = ScopeCopy::new(key.group_id.clone(), iteration_frames(&key.parent_frames, index));
                    if self.iterations.contains_key(&copy) {
                        self.due.insert(copy);
                    }
                }
                // Every iteration of an ended loop has run all it will.
                LoopNews::Ended(key) => {
                    self.due.extend(self.iterations.iter().filter(|(_, (of, _))| *of == key).map(|(copy, _)| copy.clone()));
                }
            }
        }
    }

    /// Whether a firing ended at `(node, frames)` and was forgotten: its
    /// place is marked, or the iteration it sat in is.
    pub fn ended(&self, places: &Places, node: &str, frames: &LoopFrames) -> bool {
        if self.ended.contains(&FiringLocation::new(node, frames.clone())) {
            return true;
        }
        let Some(def) = places.index.node(places.project, node) else { return false };
        let mut at = places.index.scopes().node_copy(def, frames);
        while let Some(copy) = at {
            if let Some(Frame::Loop { index }) = copy.frames.last() {
                let is_loop = places.index.scopes().get(&copy.scope).is_some_and(|scope| scope.kind == ScopeKind::Loop);
                if is_loop {
                    let parent_frames = copy.frames[..copy.frames.len() - 1].to_vec();
                    let key = LoopInstanceKey { group_id: copy.scope.clone(), parent_frames, run_id: places.run_id };
                    if self.finished.get(&key).is_some_and(|done| done.contains(index)) {
                        return true;
                    }
                }
            }
            at = places.index.scopes().parent_of(&copy).ok().flatten();
        }
        false
    }

    /// Take the ended record at `at` out of `tables`, with the pulses it
    /// took, the answers it was given and its kick, and mark where it
    /// ended. Answers what was forgotten, to let go of what else belongs to
    /// it: nothing when there is no record, it is not to be forgotten on
    /// its own ([`forgettable`]) or it is the run's first failure, kept;
    /// the failure kept before when this one takes its place.
    pub fn forget_firing(&mut self, places: &Places, tables: &mut Tables, at: &FiringLocation) -> Vec<Forgotten> {
        let Some(record) = tables.firings.at(&at.node_id, &at.frames) else { return Vec::new() };
        if !record.status().is_terminal() || !forgettable(places, record) {
            return Vec::new();
        }
        match self.compete(tables, at) {
            Competed::Kept(before) => before.into_iter().map(|before| *before).collect(),
            Competed::Lost => self.take_out(tables, at).into_iter().collect(),
        }
    }

    /// Whether the ended record at `at` is the run's first failure: kept
    /// when it is (with the failure it took the place of, taken out), lost
    /// when it is not one, or a later one. Every record leaving the table
    /// goes through here first, alone or with its iteration, so the run's
    /// failure is always the earliest.
    fn compete(&mut self, tables: &mut Tables, at: &FiringLocation) -> Competed {
        if self.keeps(at) {
            return Competed::Kept(None);
        }
        let Some(record) = tables.firings.at(&at.node_id, &at.frames) else { return Competed::Lost };
        if !self.keeps_first_failure || record.status() != NodeFiringStatus::Failed {
            return Competed::Lost;
        }
        let key = record.failure_order();
        if self.first_failure.as_ref().is_some_and(|(kept, _)| *kept <= key) {
            return Competed::Lost;
        }
        let before = self.first_failure.replace((key, at.clone())).map(|(_, before)| before);
        Competed::Kept(before.and_then(|before| self.take_out(tables, &before)).map(Box::new))
    }

    fn take_out(&mut self, tables: &mut Tables, at: &FiringLocation) -> Option<Forgotten> {
        let record = tables.firings.remove(&at.node_id, &at.frames)?;
        tables.pulses.forget_absorbed(&record.pulses_absorbed);
        let answers = tables.awaited_sequences.remove(at).unwrap_or_default();
        tables.kicked.forget(|kicked| kicked == at);
        self.ended.insert(at.clone());
        Some(Forgotten { record, answers })
    }

    /// Forget every ended firing `busy` does not hold (a body still
    /// running after the engine ended its record), as
    /// [`Self::forget_firing`] does.
    pub fn forget_ended(&mut self, places: &Places, tables: &mut Tables, busy: impl Fn(&FiringLocation) -> bool) -> Vec<Forgotten> {
        let ended: Vec<FiringLocation> = tables.firings.iter()
            .flat_map(|(node, records)| records.iter().filter(|(_, record)| record.status().is_terminal()).map(|(frames, _)| FiringLocation::new(node.clone(), frames.clone())))
            .filter(|at| !busy(at))
            .collect();
        ended.iter().flat_map(|at| self.forget_firing(places, tables, at)).collect()
    }

    /// Whether the record at `at` is the run's first failure, kept.
    fn keeps(&self, at: &FiringLocation) -> bool {
        self.first_failure.as_ref().is_some_and(|(_, kept)| kept == at)
    }

    /// Forget every iteration whose `LoopOut` fired or whose loop ended,
    /// once nothing inside it is alive: no firing open, `busy` (a body
    /// still running), inherited or the run's first failure, no pulse
    /// waiting, no kick or copy waiting, no loop running. What it decided,
    /// kicked, sent, took and ran inside goes with it, and its index joins
    /// its loop's finished ones. The failures inside compete for the run's
    /// first failure before anything goes. Answers the iterations and the
    /// firings forgotten, for what else the caller holds about them.
    pub fn forget_finished_iterations(&mut self, places: &Places, tables: &mut Tables, busy: impl Fn(&FiringLocation) -> bool) -> ForgotIterations {
        self.hear(tables.loop_runtime);
        let mut forgot = ForgotIterations::default();
        let due: Vec<ScopeCopy> = self.due.iter().cloned().collect();
        for iteration in &due {
            let inside = |at: &FiringLocation| places.location_in(&at.node_id, &at.frames, iteration);
            let failed: Vec<FiringLocation> = records_where(tables, |at, record| {
                inside(at) && record.status() == NodeFiringStatus::Failed && !busy(at)
            });
            for at in &failed {
                if let Competed::Kept(Some(before)) = self.compete(tables, at) {
                    forgot.firings.push(*before);
                }
            }
            let held = |at: &FiringLocation, record: &NodeFiring| busy(at) || self.keeps(at) || record.inherited_from.is_some();
            if !nothing_alive_in(places, tables, iteration, &held) {
                continue;
            }
            self.due.remove(iteration);
            let Some((key, index)) = self.iterations.remove(iteration) else { continue };
            // The records still inside ended (a loop's own boundaries go
            // with the iteration they sit in).
            for at in records_where(tables, |at, _| inside(at)) {
                forgot.firings.extend(self.take_out(tables, &at));
            }
            tables.scopes.forget(|copy| places.copy_in(copy, iteration), inside);
            tables.kicked.forget(inside);
            tables.pulses.forget_streams(inside);
            tables.pulses.forget_absorbed_where(|pulse| match places.index.node(places.project, &pulse.target_node) {
                Some(_) => inside(&FiringLocation::new(pulse.target_node.clone(), pulse.frames.clone())),
                None => places.copy_in(&ScopeCopy::new(pulse.target_node.clone(), pulse.frames.clone()), iteration),
            });
            let loop_inside = |key: &LoopInstanceKey| places.location_in(&loop_in_id(&key.group_id), &key.parent_frames, iteration);
            tables.loop_runtime.forget(loop_inside);
            tables.awaited_sequences.retain(|at, _| !inside(at));
            // What ended inside is covered by the iteration's index now,
            // and so is any loop that ran inside it.
            self.ended.retain(|at| !inside(at));
            self.finished.retain(|key, _| !loop_inside(key));
            self.iterations.retain(|_, (of, _)| !loop_inside(of));
            self.due.retain(|copy| self.iterations.contains_key(copy));
            self.finished.entry(key).or_default().insert(index);
            forgot.iterations.push(iteration.clone());
        }
        forgot
    }
}

/// What forgetting finished iterations let go of
/// ([`Forgetting::forget_finished_iterations`]).
#[derive(Default)]
pub struct ForgotIterations {
    pub iterations: Vec<ScopeCopy>,
    pub firings: Vec<Forgotten>,
}

/// How an ended record fared against the run's first failure
/// ([`Forgetting::compete`]).
enum Competed {
    /// It is the first failure, kept; with the one it took the place of.
    Kept(Option<Box<Forgotten>>),
    /// It is no failure, or a later one.
    Lost,
}

/// Where the records `pick` picks are.
fn records_where(tables: &Tables, pick: impl Fn(&FiringLocation, &NodeFiring) -> bool) -> Vec<FiringLocation> {
    tables.firings.iter()
        .flat_map(|(node, records)| records.iter().map(move |(frames, record)| (FiringLocation::new(node.clone(), frames.clone()), record)))
        .filter(|(at, record)| pick(at, record))
        .map(|(at, _)| at)
        .collect()
}

/// Whether a firing can be forgotten once it ended. Not a loop's `LoopIn`,
/// which its loop may end again (a failed stream fails the loop after its
/// `LoopIn` completed): it goes with the iteration it sits in, or stays,
/// one per loop at the top. Not one the run inherited, which no row of
/// its own ends.
fn forgettable(places: &Places, record: &NodeFiring) -> bool {
    record.inherited_from.is_none()
        && !places.index.node(places.project, &record.node_id)
            .is_some_and(|def| def.node_type == crate::project::boundary_types::LOOP_IN)
}

/// Whether nothing inside `iteration` can still happen: no firing open
/// or `held`, no pulse waiting, no kick or copy waiting, no loop running.
fn nothing_alive_in(places: &Places, tables: &Tables, iteration: &ScopeCopy, held: &dyn Fn(&FiringLocation, &NodeFiring) -> bool) -> bool {
    let inside = |node: &str, frames: &LoopFrames| places.location_in(node, frames, iteration);
    let any_alive = tables.firings.iter().any(|(node, records)| records.iter().any(|(frames, record)| {
            inside(node, frames) && (!record.status().is_terminal() || held(&FiringLocation::new(node.clone(), frames.clone()), record))
        }))
        || tables.kicked.undispatched().any(|at| inside(&at.node_id, &at.frames))
        || tables.scopes.waiting().any(|copy| places.copy_in(copy, iteration))
        || tables.loop_runtime.iter().any(|(key, inst)| inst.terminated.is_none() && inside(&loop_in_id(&key.group_id), &key.parent_frames))
        // Only the buckets that can hold a pulse inside the loop are read:
        // a stream's backlog waiting at its `LoopIn` is not.
        || tables.pulses.iter().any(|(target, bucket)| {
            !bucket.is_empty() && places.can_sit_in(target, &iteration.scope) && bucket.iter().any(|pulse| match places.index.node(places.project, target) {
                Some(_) => inside(target, &pulse.frames),
                None => places.copy_in(&ScopeCopy::new(target.clone(), pulse.frames.clone()), iteration),
            })
        });
    !any_alive
}

/// Where things sit in a program's scopes, to tell what is inside a loop's
/// iteration.
pub struct Places<'a> {
    pub project: &'a ProjectDefinition,
    pub index: &'a ProgramIndex,
    /// The run whose loops these are.
    pub run_id: crate::RunId,
}

impl Places<'_> {
    /// Whether `copy` is `iteration` or sits inside it, however deep.
    /// Going out from a copy, its frames only get shorter, so the walk
    /// stops as soon as they no longer start with the iteration's.
    pub fn copy_in(&self, copy: &ScopeCopy, iteration: &ScopeCopy) -> bool {
        let mut at = Some(copy.clone());
        while let Some(copy) = at {
            if !copy.frames.starts_with(&iteration.frames) {
                return false;
            }
            if copy == *iteration {
                return true;
            }
            at = self.index.scopes().parent_of(&copy).ok().flatten();
        }
        false
    }

    /// Whether node `node` firing at `frames` sits inside `iteration`.
    pub fn location_in(&self, node: &str, frames: &LoopFrames, iteration: &ScopeCopy) -> bool {
        frames.starts_with(&iteration.frames)
            && self.index.node(self.project, node)
                .and_then(|def| self.index.scopes().node_copy(def, frames))
                .is_some_and(|copy| self.copy_in(&copy, iteration))
    }

    /// Whether node `node` sits inside the loop `group` (a member, however
    /// deep, or one in a body a site inside it may call) or is one of its
    /// boundaries.
    pub fn in_loop(&self, node: &str, group: &str) -> bool {
        self.can_sit_in(node, group)
            || self.index.node(self.project, node).is_some_and(|def| def.group_boundary.as_ref().is_some_and(|b| b.group_id == group))
    }

    /// Whether `target` (a node, or a scope's gate) can sit inside an
    /// iteration of the loop `group` at all: inside it in the program, or
    /// in an included body a site inside the loop calls.
    pub fn can_sit_in(&self, target: &str, group: &str) -> bool {
        let mut at = match self.index.node(self.project, target) {
            Some(def) if def.group_boundary.as_ref().is_some_and(|b| b.group_id == group)
                && def.node_type == crate::project::boundary_types::LOOP_OUT => return true,
            Some(def) => def.scope.last().cloned(),
            None => Some(target.to_string()),
        };
        while let Some(scope) = at {
            if scope == group {
                return true;
            }
            let Some(found) = self.index.scopes().get(&scope) else { return false };
            if found.kind == ScopeKind::Body {
                // An included body sits wherever a site calling it does.
                return self.index.scopes().sites_calling(&scope).any(|site| self.can_sit_in(&site.id, group));
            }
            at = found.parent.clone();
        }
        false
    }
}
