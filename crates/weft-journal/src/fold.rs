//! The fold: journal rows plus the program, in, a run's state
//! out. One row at a time, so a live reader (the dispatcher's journal
//! bridge) keeps a fold per open run and applies each row as it
//! lands, and a resume (the engine, the cancel writers) folds the
//! whole log in one go through the same code.
//!
//! The journal records facts the engine learned from outside
//! (`crate::events`); the fold recomputes everything else with the
//! same functions the live engine ran: an emission fans out through
//! `postprocess_output`, a terminal sweeps its unmentioned ports
//! through `close_unmentioned_downstream`, the run's scopes are decided
//! through `scope::settle_table` after every row, a loop launches and
//! terminates through `loop_runtime`. Because the pulse ids those
//! functions mint derive from the emission and the wire, the table the
//! fold builds is the table the live engine held.
//!
//! A row the fold cannot apply (the program has no such node, a loop
//! row with no instance, a pulse a row names that is not there) is a
//! `JournalCorruption`: logged at error level, listed on the snapshot,
//! and the row skipped. The engine refuses to resume over any of them;
//! the display reads render what applied.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use weft_core::run::forget::{Forgetting, Forgotten, Places, Tables};
use weft_core::run::scope::{settle_table, tear_down_loop, ScopeState};
use weft_core::run::emission::{loop_termination_emission, stand_in_emission, stream_emission, terminal_sweep_emission, PulseEmission};
use weft_core::run::loop_runtime::{
    self, classify_loop_out, close_loop_outward, emit_loop_outward, instantiate, launch_iteration,
    LoopAdvance, LoopStreamItem,
};
use weft_core::run::postprocess::{close_unmentioned_downstream, emit_port_closure, postprocess_output_kept, OutputBag};
use weft_core::run::ready::{firing_input, kicked_group, wired_inputs};
use weft_core::run::skip::SkipReason;
use weft_core::run::{
    firing_at, firing_at_mut, NodeFiring, NodeFiringStatus,
};
use weft_core::frames::{FiringLocation, Located, LoopFrames};
use weft_core::primitive::{
    AwaitEnd, AwaitedEntry, AwaitedEntryKind, CorruptionSite, RunSnapshot, JournalCorruption, KickedNode,
    LoopInstanceKey, LoopTerminationReason, SuspensionInfo,
};
use weft_core::project::scopes::ScopeCopy;
use weft_core::project::{loop_in_id, loop_out_id, GivenOrigin, GivenPort, ProgramIndex, NodeDefinition, ProjectDefinition};
use weft_core::pulse::Failure;
use weft_core::live_event::PlaceOutputs;
use weft_core::record_ref::{GivenSide, Position, Shown, ValueRef};
use weft_core::RunId;

use crate::events::RunEvent;

/// A wait that ended: the firing that waited, the wait's token, and how
/// it ended.
#[derive(Debug, Clone, PartialEq)]
pub struct EndedWait {
    pub at: FiringLocation,
    pub token: String,
    pub end: AwaitEnd,
    /// Where the record keeps the answer, for an answered wait whose
    /// answer's row came with its position ([`Fold::apply_at`]).
    pub answer_kept: Option<Arc<ValueRef>>,
}

/// What one applied row put in the world, beyond the snapshot: the
/// live bridge turns these into the events the editor paints.
#[derive(Default)]
pub struct FoldEffects {
    /// Every pulse this row put on a wire, with its provenance
    /// (emissions, closures, a loop's body pulses, a loop's outward
    /// values, what the scopes it decided sent and closed, what its
    /// stand-ins sent).
    pub emissions: Vec<PulseEmission>,
    /// The copies of scopes this row decided, in order.
    pub scopes: Vec<(ScopeCopy, ScopeState)>,
    /// The wait this row ended, the first ending on record: an answer, a
    /// skip or a give-up row for a registered wait, or the registration
    /// of a wait whose ending was already on record.
    pub wait_ended: Option<EndedWait>,
    /// On a `LoopOutFired`: the body's `done` vote as the firing read
    /// it.
    pub loop_out_done: Option<Option<bool>>,
    /// The corruptions this row was reported as. Non-empty means the
    /// row could not be applied, in whole or in part: a reader
    /// painting the row has nothing true to paint it from but these.
    pub rejections: Vec<JournalCorruption>,
}

impl FoldEffects {
    /// Whether the row was reported as a corruption.
    pub fn rejected(&self) -> bool {
        !self.rejections.is_empty()
    }
}

/// One run's state, folded row by row over its program.
#[derive(Clone)]
pub struct Fold {
    project: Arc<ProjectDefinition>,
    program_idx: ProgramIndex,
    /// The run's phase, from `RunStarted`; `None` before that
    /// row.
    phase: Option<weft_core::context::Phase>,
    snap: RunSnapshot,
    /// Per record, what it handed out: for the screen only.
    outputs: HashMap<Uuid, OutputBag>,
    /// Source values before wire projection, including outputs without a
    /// consumer in this run. Seed delivery projects these through child wires.
    output_history: Option<Vec<OutputEmission>>,
    output_ids: HashSet<(Uuid, String, String)>,
    /// Where the record keeps what each record handed out, by port, for
    /// the emissions whose rows came with their positions
    /// ([`Self::apply_at`]).
    output_kept: HashMap<Uuid, BTreeMap<String, Arc<ValueRef>>>,
    /// Where the record keeps an answer that came before its wait was
    /// registered (`RunSnapshot::pending_deliveries`), by token.
    answers_kept: HashMap<String, Arc<ValueRef>>,
    /// Where each registered wait is, by token: how an ending finds its
    /// wait without walking every wait of the run.
    awaits: HashMap<String, FiringLocation>,
    /// Whether rows come with their positions ([`Self::apply_at`]), so
    /// the fold names where the values it holds are kept.
    positioned: bool,
    /// The firings and loops the last row ended, which a showing fold
    /// forgets as the next row comes (a reader paints a row from the fold
    /// right after applying it, so they stay one row longer).
    ended: Vec<Ended>,
    /// Per place a firing completed at, the record whose output a fold
    /// that forgets keeps, with when it started (`NodeFiring::start`): the
    /// run's outputs
    /// are each place's last completed firing's.
    kept_output: HashMap<Located, (u64, Uuid)>,
    /// What a fold reconstructing a seed's ancestor keeps ([`Self::keeping`]).
    keeps: Option<Keeps>,
}

/// What a fold reconstructing a seed's ancestor keeps of what it would
/// otherwise forget.
#[derive(Clone)]
struct Keeps {
    /// The places the runs after it take from it.
    places: BTreeSet<Located>,
    /// The loops around one of them, or with one as a boundary: kept whole.
    loops: BTreeSet<String>,
}

/// The places of `fold`'s program, for its forgetting.
fn places_of<'a>(project: &'a ProjectDefinition, index: &'a ProgramIndex, run_id: RunId) -> Places<'a> {
    Places { project, index, run_id }
}

/// What a row ended, for a showing fold to forget.
#[derive(Clone)]
enum Ended {
    Firing(FiringLocation),
    Loop(LoopInstanceKey),
}

#[derive(Clone)]
struct OutputEmission {
    id: Uuid,
    node: String,
    frames: LoopFrames,
    port: String,
    value: Option<Arc<Value>>,
    error: Option<Failure>,
}

impl Fold {
    pub fn new(run_id: RunId, project: Arc<ProjectDefinition>) -> Self {
        let program_idx = ProgramIndex::build(&project);
        Self {
            project,
            program_idx,
            phase: None,
            snap: RunSnapshot {
                run_id,
                scopes: Default::default(),
                binary_hash: None,
                definition_hash: None,
                cut_from: None,
                given: Default::default(),
                inherited_origins: Default::default(),
                pulses: Default::default(),
                firings: Default::default(),
                forgetting: None,
                suspensions: HashMap::new(),
                loop_runtime: loop_runtime::LoopRuntime::new(),
                kicked: Default::default(),
                pending_deliveries: HashMap::new(),
                awaited_sequences: HashMap::new(),
                corruptions: Vec::new(),
            },
            outputs: HashMap::new(),
            output_history: None,
            output_ids: HashSet::new(),
            output_kept: HashMap::new(),
            answers_kept: HashMap::new(),
            awaits: HashMap::new(),
            positioned: false,
            ended: Vec::new(),
            kept_output: HashMap::new(),
            keeps: None,
        }
    }

    /// A fold that SHOWS the run (the replay, the live view, a run's
    /// events, a cancel finding the open firings): a firing's values are
    /// what its events name when it starts and ends. Once it ended the
    /// firing is forgotten, and once nothing inside a loop's iteration is
    /// alive, so is the iteration (its scope copies, kicks, inner loops);
    /// a pulse taken off its wire is forgotten at once. What the run's
    /// outputs read (each place's last completed output) stays. Its memory
    /// holds what is open, never what the run already did, so it does not
    /// grow with the run. No reader of a firing after it ended may fold
    /// this way: a resume, a seed, the outputs review.
    pub fn showing(mut self) -> Self {
        self.snap.pulses.forget_taken();
        self.forgets(Forgetting::new())
    }

    /// The fold a worker drives a run on (`weft_engine`'s): it forgets
    /// what ended as a showing fold does ([`Self::showing`]), and keeps
    /// what driving the run still needs: a pulse a firing still open took
    /// (a resumed firing takes it again), and the run's first failure,
    /// which the run's ending names.
    pub fn driving(self) -> Self {
        self.forgets(Forgetting::new().keeping_first_failure())
    }

    fn forgets(mut self, forgetting: Forgetting) -> Self {
        self.snap.loop_runtime.report();
        self.snap.forgetting = Some(forgetting);
        self
    }

    fn forgetting(&self) -> bool {
        self.snap.forgetting.is_some()
    }

    /// A fold that reconstructs a seed's ancestor
    /// (`crate::seed::SeedChain::materialize`): it forgets what it folds as
    /// a showing fold does ([`Self::showing`]), except what the runs after
    /// it take from it, `places`: their firings (with what they received,
    /// handed out and were answered), their output history
    /// ([`Self::with_output_history`]), and every iteration of a loop
    /// around one of them, which is reused whole and is reusable only when
    /// every iteration settled.
    pub fn keeping(self, places: BTreeSet<Located>) -> Self {
        let mut fold = self.showing();
        let found = places_of(&fold.project, &fold.program_idx, fold.snap.run_id);
        let every_loop: Vec<&str> = fold.project.groups.iter()
            .filter(|group| matches!(group.kind, weft_core::project::GroupKind::Loop { .. }))
            .map(|group| group.id.as_str())
            .collect();
        let mut loops: BTreeSet<String> = every_loop.iter()
            .filter(|group| places.iter().any(|place| found.in_loop(&place.id, group)))
            .map(|group| group.to_string())
            .collect();
        // A loop inside a kept loop is kept whole too, however deep:
        // whether the outer one is reusable reads every iteration of the
        // inner one (`weft_core::seeding::complete_loop`).
        loop {
            let inside: Vec<String> = every_loop.iter()
                .filter(|inner| !loops.contains(**inner) && loops.iter().any(|kept| found.in_loop(&loop_in_id(inner), kept)))
                .map(|inner| inner.to_string())
                .collect();
            if inside.is_empty() {
                break;
            }
            loops.extend(inside);
        }
        fold.output_history = Some(Vec::new());
        if let Some(forgetting) = fold.snap.forgetting.take() {
            fold.snap.forgetting = Some(forgetting.keeping_loops(loops.clone()));
        }
        fold.keeps = Some(Keeps { places, loops });
        fold
    }

    /// Whether this fold keeps the firing at `at` once it ended
    /// ([`Self::keeping`]).
    fn keeps_firing(&self, at: &FiringLocation) -> bool {
        let Some(keeps) = &self.keeps else { return false };
        let found = places_of(&self.project, &self.program_idx, self.snap.run_id);
        keeps.places.contains(&Located::at(&at.node_id, &at.frames))
            || keeps.loops.iter().any(|group| found.in_loop(&at.node_id, group))
    }

    pub fn run_id(&self) -> RunId {
        self.snap.run_id
    }

    pub fn project(&self) -> &Arc<ProjectDefinition> {
        &self.project
    }

    pub fn snapshot(&self) -> &RunSnapshot {
        &self.snap
    }

    /// Seed reconstruction needs every stream item. Live projection and worker
    /// replay leave this disabled so their memory does not grow with the stream.
    pub fn with_output_history(mut self) -> Self {
        self.output_history = Some(Vec::new());
        self
    }

    /// Complete review evidence, including finite stream termination and unused ports.
    pub fn output_wires(&self) -> anyhow::Result<Vec<weft_core::run_spec::OutputWire>> {
        let history = self.output_history.as_ref()
            .ok_or_else(|| anyhow::anyhow!("run {} was reconstructed without output history", self.run_id()))?;
        let mut ordinals = HashMap::new();
        Ok(history.iter().map(|output| {
            let ordinal = ordinals.entry((output.node.clone(), output.frames.clone(), output.port.clone())).or_insert(0u64);
            let wire = weft_core::run_spec::OutputWire {
                node: output.node.clone(), frames: output.frames.clone(), port: output.port.clone(),
                ordinal: *ordinal, closed: output.value.is_none(), failure: output.error.clone(),
                value: output.value.as_ref().map(|value| value.as_ref().clone()).unwrap_or(Value::Null),
            };
            *ordinal += 1;
            wire
        }).collect())
    }

    /// Import already reconstructed results: the records `places` have in
    /// `source` (their firings, what they handed out, their recorded
    /// waits and their output history), marked as inherited. Nothing goes
    /// on a wire: a reused place this run's cut needs stands in for itself
    /// there (`ProjectDefinition::given`) and sends on its own; the rest
    /// is only shown. A place the cut does not hold is imported all the
    /// same: the run's record shows everything it reused.
    pub fn inherit(&mut self, source: &Fold, places: &BTreeSet<Located>) -> anyhow::Result<()> {
        anyhow::ensure!(source.snap.corruptions.is_empty(), "seed {} contains corrupt journal rows", source.run_id());
        let history = source.output_history.as_ref()
            .ok_or_else(|| anyhow::anyhow!("seed {} was reconstructed without its output history", source.run_id()))?;
        let reusable = weft_core::seeding::inheritable_nodes(&source.project, &source.snap);
        // The records at one place: the node's firings under that
        // place's calls, at every loop iteration.
        let records_at = |snap: &RunSnapshot, place: &Located| -> Vec<weft_core::run::NodeFiring> {
            snap.firings.get(&place.id).into_iter().flat_map(|records| records.values())
                .filter(|record| Located::at(&place.id, &record.frames) == *place).cloned().collect()
        };
        for place in places {
            anyhow::ensure!(records_at(&self.snap, place).is_empty(), "history for '{place}' was already imported");
            anyhow::ensure!(source.project.nodes.iter().any(|definition| definition.id == place.id),
                "seed node '{place}' is not in the program of run {}", source.run_id());
            let records = records_at(&source.snap, place);
            anyhow::ensure!(reusable.contains(place), "seed {} has no complete reusable result for '{place}'", source.run_id());
            anyhow::ensure!(!source.snap.inherited_origins.contains_key(place),
                "seed result '{place}' must name its original run, not intermediate run {}", source.run_id());
            anyhow::ensure!(records.iter().all(|record| matches!(record.status(), NodeFiringStatus::Completed | NodeFiringStatus::Skipped)),
                "cannot inherit unfinished or unsuccessful result '{place}' from {}", source.run_id());
            anyhow::ensure!(records.iter().all(|record| record.completed_at.is_some()), "seed result '{place}' has no completion time");
            anyhow::ensure!(records.iter().all(|record| record.inherited_from.is_none()),
                "seed result '{place}' must name its original run, not intermediate run {}", source.run_id());
        }
        for place in places {
            let records: Vec<_> = records_at(&source.snap, place).into_iter().map(|record| {
                let mut inherited = record;
                inherited.run_id = self.run_id();
                inherited.inherited_from = Some(source.run_id());
                inherited
            }).collect();
            if records.is_empty() { continue; }
            for record in &records {
                if let Some(output) = source.outputs.get(&record.id) { self.outputs.insert(record.id, output.clone()); }
                if let Some(kept) = source.output_kept.get(&record.id) { self.output_kept.insert(record.id, kept.clone()); }
            }
            for record in records {
                self.snap.firings.insert(record);
            }
        }
        for (location, entries) in &source.snap.awaited_sequences {
            if places.contains(&Located::at(&location.node_id, &location.frames)) {
                self.snap.awaited_sequences.insert(location.clone(), entries.clone());
                self.index_awaits(location);
            }
        }
        for output in history.iter().filter(|output| places.contains(&Located::at(&output.node, &output.frames))) {
            self.remember_output(output.clone());
        }
        Ok(())
    }

    /// Take the answers `places` were given in `source`, for the firings
    /// this run runs again there: at each of their frames, the steps its
    /// body went through in `source` up to the first wait nobody answered
    /// (`weft_core::primitive::answered_steps`), so the body replays them
    /// and carries on from there instead of asking again.
    pub fn take_answers(&mut self, source: &Fold, places: &BTreeSet<Located>) {
        for (location, entries) in &source.snap.awaited_sequences {
            if !places.contains(&Located::at(&location.node_id, &location.frames)) { continue; }
            let answered = weft_core::primitive::answered_steps(entries);
            if answered.is_empty() || self.snap.awaited_sequences.contains_key(location) { continue; }
            self.snap.awaited_sequences.insert(location.clone(), answered);
            self.index_awaits(location);
        }
    }

    /// Know where each wait of the entries at `at` is ([`Self::awaits`]).
    fn index_awaits(&mut self, at: &FiringLocation) {
        for entry in self.snap.awaited_sequences.get(at).into_iter().flatten() {
            if let AwaitedEntryKind::Await { token, .. } = &entry.kind {
                self.awaits.entry(token.clone()).or_insert_with(|| at.clone());
            }
        }
    }

    /// Decide what the run's scopes can decide now (`scope::settle_table`):
    /// what follows every row, run once more after the seed's history is in.
    pub fn settle(&mut self, at_unix: u64) -> FoldEffects {
        let mut effects = FoldEffects::default();
        self.scope_pass(at_unix, &mut effects);
        effects
    }

    /// The snapshot, with every awaited sequence in call order.
    pub fn into_snapshot(mut self) -> RunSnapshot {
        for entries in self.snap.awaited_sequences.values_mut() {
            entries.sort_by_key(|e| e.call_index);
        }
        self.snap
    }

    /// The snapshot as it stands, with every awaited sequence in call
    /// order, leaving the fold to take more rows.
    pub fn current_snapshot(&self) -> RunSnapshot {
        let mut snap = self.snap.clone();
        for entries in snap.awaited_sequences.values_mut() {
            entries.sort_by_key(|e| e.call_index);
        }
        snap
    }

    /// Apply one row, in journal order, then decide what the run's
    /// scopes can decide now. A row reported as corruption decides
    /// nothing: a reader painting it has only the corruption to paint.
    pub fn apply(&mut self, ev: &RunEvent) -> FoldEffects {
        self.apply_at(ev, None)
    }

    /// [`Self::apply`] for an event read at `at` in the run's record:
    /// the values it brings (a port's emission, a wait's answer) are known
    /// to be kept there, and every pulse and firing that carries one names
    /// where (`Pulse::origin`), which is what a reader showing the run
    /// names a value by.
    pub fn apply_at(&mut self, ev: &RunEvent, at: Option<Position>) -> FoldEffects {
        self.positioned |= at.is_some();
        self.forget_ended();
        let corruptions_before = self.snap.corruptions.len();
        let mut effects = self.apply_row(ev, at);
        effects.rejections = self.snap.corruptions[corruptions_before..].to_vec();
        if effects.rejections.is_empty() {
            self.scope_pass(ev.at_unix(), &mut effects);
            if self.forgetting() {
                self.note_ended(ev);
            }
        }
        effects
    }

    fn positioned(&self) -> bool {
        self.positioned
    }

    /// Where the record keeps the value the event at `at` carries.
    fn kept_at(&self, at: Option<Position>) -> Option<Arc<ValueRef>> {
        at.map(|at| Arc::new(ValueRef::Row { run: self.snap.run_id, seq: at.seq, index: at.index }))
    }

    /// What a showing fold forgets once `ev` is painted: the firing it
    /// ended, the values of the loop it ended.
    fn note_ended(&mut self, ev: &RunEvent) {
        let run_id = self.snap.run_id;
        match ev {
            RunEvent::NodeCompleted { node_id, frames, .. }
            | RunEvent::NodeFailed { node_id, frames, .. }
            | RunEvent::NodeSkipped { node_id, frames, .. }
            | RunEvent::NodeCancelled { node_id, frames, .. } => {
                self.ended.push(Ended::Firing(FiringLocation::new(node_id.clone(), frames.clone())));
            }
            RunEvent::LoopTerminated { group_id, parent_frames, .. } => {
                self.ended.push(Ended::Loop(LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), run_id }));
            }
            _ => {}
        }
    }

    /// Forget what the last row ended now, instead of as the next row
    /// comes: for a reader done reading the record, which paints nothing
    /// more from it.
    pub fn read_to_the_end(&mut self) {
        self.forget_ended();
    }

    /// Forget what the last row ended ([`Self::showing`]), then every
    /// iteration nothing inside is alive in any more.
    fn forget_ended(&mut self) {
        for ended in std::mem::take(&mut self.ended) {
            match ended {
                Ended::Firing(at) => self.forget_firing(&at),
                // What a loop held is the values of what it ran: a fold
                // that only shows the run lets them go.
                Ended::Loop(key) if self.snap.pulses.forgets_taken() => {
                    if let Some(inst) = self.snap.loop_runtime.get_mut(&key) {
                        inst.shed_values();
                    }
                }
                Ended::Loop(_) => {}
            }
        }
        self.forget_finished_iterations();
    }

    /// Forget the ended firing at `at`: its record, what it took, was
    /// answered and where it was kicked (`Forgetting::forget_firing`).
    fn forget_firing(&mut self, at: &FiringLocation) {
        if self.keeps_firing(at) {
            return;
        }
        let snap = &mut self.snap;
        let places = places_of(&self.project, &self.program_idx, snap.run_id);
        let Some(forgetting) = snap.forgetting.as_mut() else { return };
        let mut tables = Tables {
            pulses: &mut snap.pulses,
            firings: &mut snap.firings,
            kicked: &mut snap.kicked,
            scopes: &mut snap.scopes,
            awaited_sequences: &mut snap.awaited_sequences,
            loop_runtime: &mut snap.loop_runtime,
        };
        for forgotten in forgetting.forget_firing(&places, &mut tables, at) {
            self.let_go(forgotten);
        }
    }

    /// Forget every iteration nothing inside is alive in any more
    /// (`Forgetting::forget_finished_iterations`), with the waits inside
    /// and what the fold holds of the firings it took out.
    fn forget_finished_iterations(&mut self) {
        let snap = &mut self.snap;
        let places = places_of(&self.project, &self.program_idx, snap.run_id);
        let Some(forgetting) = snap.forgetting.as_mut() else { return };
        let mut tables = Tables {
            pulses: &mut snap.pulses,
            firings: &mut snap.firings,
            kicked: &mut snap.kicked,
            scopes: &mut snap.scopes,
            awaited_sequences: &mut snap.awaited_sequences,
            loop_runtime: &mut snap.loop_runtime,
        };
        let forgot = forgetting.forget_finished_iterations(&places, &mut tables, |_| false);
        for iteration in &forgot.iterations {
            snap.suspensions.retain(|_, wait| !places.location_in(&wait.node_id, &wait.frames, iteration));
            self.awaits.retain(|_, at| !places.location_in(&at.node_id, &at.frames, iteration));
        }
        for forgotten in forgot.firings {
            self.let_go(forgotten);
        }
    }

    /// Let go of what the fold holds of a firing its tables forgot,
    /// whichever way it went: its outputs, unless it is its place's last
    /// completed one (the run's outputs read it), and its waits.
    fn let_go(&mut self, Forgotten { record, answers }: Forgotten) {
        let boundary = self.program_idx.node(&self.project, &record.node_id)
            .is_some_and(|def| weft_core::project::boundary_types::is_boundary(&def.node_type));
        if record.status() == NodeFiringStatus::Completed && !boundary {
            let place = Located::at(&record.node_id, &record.frames);
            let dropped = match self.kept_output.get(&place) {
                Some(&(kept_start, _)) if kept_start > record.start() => Some(record.id),
                _ => self.kept_output.insert(place, (record.start(), record.id)).map(|(_, previous)| previous),
            };
            if let Some(dropped) = dropped {
                self.outputs.remove(&dropped);
                self.output_kept.remove(&dropped);
            }
        } else {
            self.outputs.remove(&record.id);
            self.output_kept.remove(&record.id);
        }
        for entry in answers {
            if let AwaitedEntryKind::Await { token, .. } = entry.kind {
                self.awaits.remove(&token);
                self.snap.suspensions.remove(&token);
            }
        }
    }

    fn apply_row(&mut self, ev: &RunEvent, at: Option<Position>) -> FoldEffects {
        let mut effects = FoldEffects::default();
        let run_id = self.snap.run_id;
        match ev {
            RunEvent::RunStarted { phase, binary_hash, definition_hash, cut_from, given, seed, .. } => {
                self.snap.binary_hash = binary_hash.clone();
                self.snap.definition_hash = definition_hash.clone();
                self.snap.cut_from = cut_from.clone();
                // The run's values go where its program says it is given
                // something, and nowhere else; anything else and the
                // program and the birth were never resolved together.
                match given.check_against(&self.project) {
                    Ok(()) => self.snap.given = Arc::new(given.as_ref().clone()),
                    Err(why) => self.report(CorruptionSite::RunGiven, why),
                }
                self.snap.inherited_origins = seed.as_ref().map(|seed| seed.origins.clone()).unwrap_or_default();
                self.phase = Some(*phase);
                self.remember_given();
            }
            RunEvent::NodeKicked { node_id, frames, firing, payload, port_snapshot, .. } => {
                // First kick wins; further kicks on the same location are
                // a true no-op (the documented contract). The first kick's
                // payload and `dispatched` flag are authoritative; a later
                // kick carrying a different payload is a writer-level bug
                // the fold must not paper over by silently merging.
                let (kick, made) = self.snap.kicked.kick(FiringLocation::new(node_id.clone(), frames.clone()), || {
                    KickedNode::new(*firing, payload.clone(), port_snapshot.clone(), None)
                });
                if !made && *firing && !kick.firing {
                    kick.firing = true;
                    kick.payload = payload.clone();
                    kick.port_snapshot = port_snapshot.clone();
                }
            }
            RunEvent::PortEmitted { emission_id, node_id, frames, port, value, .. } => {
                // An emission belongs to a firing: a row about one the
                // journal never opened is corruption.
                let Some(record_id) = self.firing_record_id(node_id, frames, CorruptionSite::PortEmitted, ev) else {
                    return effects;
                };
                let shared = value.clone();
                let mut bag = OutputBag::new();
                bag.insert(port.clone(), shared.clone());
                let kept: BTreeMap<String, Arc<ValueRef>> = self.kept_at(at).map(|kept| (port.clone(), kept)).into_iter().collect();
                match postprocess_output_kept(
                    node_id, &bag, &kept, *emission_id, run_id, frames, &self.project,
                    &mut self.snap.pulses, &self.program_idx, &mut effects.emissions,
                ) {
                    Ok(mentioned) => {
                        self.remember_output(OutputEmission {
                            id: *emission_id, node: node_id.clone(), frames: frames.clone(),
                            port: port.clone(), value: Some(shared.clone()), error: None,
                        });
                        // An output is mentioned even without a wire;
                        // a later seeded run can connect a consumer to it.
                        self.record_at_mut(node_id, frames)
                            .expect("`firing_record_id` found this record and nothing removes records")
                            .mentioned_ports
                            .extend(mentioned);
                        // A firing's emissions merge into one output
                        // bag, later wins per port: an ordinary port is
                        // mentioned once per firing (the runner refuses
                        // a second mention), a generator port once per
                        // item, so its recorded output is the last item.
                        self.outputs.entry(record_id).or_default().insert(port.clone(), shared);
                        let ports = self.output_kept.entry(record_id).or_default();
                        match kept.into_values().next() {
                            Some(kept) => ports.insert(port.clone(), kept),
                            None => ports.remove(port),
                        };
                    }
                    Err(e) => self.report(CorruptionSite::PortEmitted, format!("{}: {e}", describe(ev))),
                }
            }
            RunEvent::PortClosed { emission_id, node_id, frames, port, .. } => {
                if self.firing_record_id(node_id, frames, CorruptionSite::PortClosed, ev).is_none() {
                    return effects;
                }
                self.close_port(node_id, frames, port, *emission_id, ev, &mut effects);
            }
            RunEvent::PulsesConsumed { node_id, pulse_ids, .. } => {
                // The take path's durability: REMOVE the consumed
                // pulses from the table, as the live driver does
                // (`consume_stream_pulses` removes, never tombstones: a
                // long stream would otherwise grow
                // the bucket by one spent entry per item, turning
                // every per-pass scan of a REFOLDED worker quadratic
                // where the live worker's is not). Safe because
                // consumed stream pulses are never un-absorb targets
                // (a stream consumer is never re-run). Deliberately
                // NOT recorded on the consuming record's
                // `pulses_absorbed`: that list exists for the
                // resume-time un-absorb, which never applies here, and
                // a long stream would grow the record by one uuid per
                // item forever.
                // A named pulse the table does not hold is a row
                // about a take that never landed here (its emission
                // row lost): corruption, as loud as the live side's
                // `drop_consumed_pulses` and as the launch row's
                // `stream_pulse` lookup.
                // The row is applied whole or not at all: an id that
                // does not parse, or a pulse the table does not hold,
                // rejects the row before anything is removed. No bucket
                // is created for a node that has none: the live table
                // never holds an empty bucket the drive did not open,
                // and the resumed worker takes this table as its own.
                let corruptions_before = self.snap.corruptions.len();
                let ids = self.parse_ids(pulse_ids, CorruptionSite::PulsesConsumed);
                if self.snap.corruptions.len() > corruptions_before {
                    return effects;
                }
                let Some(bucket) = self.snap.pulses.get_mut(node_id) else {
                    self.report(
                        CorruptionSite::PulsesConsumed,
                        format!("{}: the table holds no pulses for the node", describe(ev)),
                    );
                    return effects;
                };
                let missing: Vec<Uuid> =
                    ids.iter().copied().filter(|id| !bucket.iter().any(|p| p.id == *id)).collect();
                if !missing.is_empty() {
                    self.report(
                        CorruptionSite::PulsesConsumed,
                        format!("{}: {} of the named pulses are not in the table: {missing:?}", describe(ev), missing.len()),
                    );
                    return effects;
                }
                self.snap.pulses.remove_consumed(node_id, &ids);
            }
            RunEvent::NodeStarted { node_id, frames, at_unix, .. } => {
                if self.node_def(node_id, ev).is_none() {
                    return effects;
                }
                // One run fires a node once per frames: the live
                // dispatch opens a record with NodeStarted and continues
                // it with NodeResumed, never a second NodeStarted. So one
                // over any record already here (open, or ended) is a row
                // the writer could not have produced: corruption, skipped
                // whole (absorbing its pulses into no record would strand
                // them for the resume-time un-absorb).
                let held = self.record_at(node_id, frames).map(|record| record.status().is_terminal());
                let forgotten = || self.snap.forgetting.as_ref()
                    .is_some_and(|forgetting| forgetting.ended(&places_of(&self.project, &self.program_idx, run_id), node_id, frames));
                let already = match held {
                    Some(true) => Some("already ended"),
                    Some(false) => Some("already open"),
                    // One a fold that forgets let go of is still known
                    // to have ended.
                    None if forgotten() => Some("already ended"),
                    None => None,
                };
                if let Some(what) = already {
                    self.report(
                        CorruptionSite::NodeLifecycle,
                        format!("{}: a firing is {what} at these frames, and a node fires once per frames", describe(ev)),
                    );
                    return effects;
                }
                let received = self.receiving(node_id, frames);
                let absorbed = self.absorb_pending(node_id, frames, false);
                self.snap.kicked.set_dispatched(&FiringLocation::new(node_id.clone(), frames.clone()), true);
                self.snap.firings.insert(NodeFiring::started(node_id, frames.clone(), run_id, received, absorbed, *at_unix));
            }
            RunEvent::NodeSuspended { node_id, frames, token, .. } => {
                // A suspension of a firing the journal never opened is
                // corruption like every other lifecycle row: dropped
                // quietly, the record would read Running and a resume
                // would re-run a body that is parked on a signal.
                if self.firing_record_id(node_id, frames, CorruptionSite::NodeLifecycle, ev).is_none() {
                    return effects;
                }
                if let Some(e) = self.snap.firings.set_status(node_id, frames, NodeFiringStatus::WaitingForInput) {
                    e.callback_id = Some(token.clone());
                }
            }
            RunEvent::NodeResumed { node_id, frames, token, .. } => {
                // Absorb the pending pulses at the location, exactly as
                // the NodeStarted arm does. The live resume
                // dispatch absorbs them in RAM; if the fold left them
                // Pending, a refold on a fresh worker would hold the
                // node's record terminal (Completed) yet a Pending pulse
                // at its (node, frames) location, and `find_ready_nodes`
                // (which keys purely on Pending pulses, ignoring the
                // record) would re-fire the node: double run on
                // every respawn. The un-absorb path (`redispatch_locations`)
                // also assumes resume-time pulses were folded absorbed
                // before it puts them back. A resume of a firing the
                // journal never opened is corruption, skipped whole:
                // pulses absorbed into no record could never be given
                // back.
                if self.firing_record_id(node_id, frames, CorruptionSite::NodeLifecycle, ev).is_none() {
                    return effects;
                }
                let absorbed = self.absorb_pending(node_id, frames, false);
                if let Some(e) = self.snap.firings.set_status(node_id, frames, NodeFiringStatus::Running) {
                    e.callback_id = None;
                    for id in absorbed {
                        if !e.pulses_absorbed.contains(&id) {
                            e.pulses_absorbed.push(id);
                        }
                    }
                }
                // Only clear suspension state when this resume was
                // suspension-driven (token present). A loop boundary
                // re-fired after a refold has no token to remove.
                if let Some(t) = token {
                    self.snap.suspensions.remove(t);
                }
            }
            RunEvent::NodeCompleted { node_id, frames, at_unix, .. } => {
                self.terminate(node_id, frames, NodeFiringStatus::Completed, None, None, false, *at_unix, ev, &mut effects);
            }
            RunEvent::NodeFailed { node_id, frames, error, gave_up_wait, at_unix, .. } => {
                self.terminate(node_id, frames, NodeFiringStatus::Failed, Some(error), None, *gave_up_wait, *at_unix, ev, &mut effects);
            }
            RunEvent::NodeSkipped { node_id, frames, reason, at_unix, .. } => {
                self.terminate(node_id, frames, NodeFiringStatus::Skipped, None, Some(reason), false, *at_unix, ev, &mut effects);
            }
            RunEvent::NodeCancelled { node_id, frames, reason, at_unix, .. } => {
                self.terminate(node_id, frames, NodeFiringStatus::Cancelled, Some(reason), None, false, *at_unix, ev, &mut effects);
            }
            RunEvent::LoopInstantiated { group_id, parent_frames, .. } => {
                let loop_in_id = loop_in_id(group_id);
                let Some(def) = self.project.nodes.iter().find(|n| n.id == loop_in_id) else {
                    self.report(CorruptionSite::LoopInstantiated, format!("{}: the program has no LoopIn '{loop_in_id}'", describe(ev)));
                    return effects;
                };
                let Some(view) = self.firing_view_of(&loop_in_id, parent_frames) else {
                    self.report(CorruptionSite::LoopInstantiated, format!("{}: no LoopIn firing at these frames", describe(ev)));
                    return effects;
                };
                if let Err(e) = instantiate(&mut self.snap.loop_runtime, def, &self.project, &view, parent_frames, run_id) {
                    self.report(CorruptionSite::LoopInstantiated, format!("{}: {e}", describe(ev)));
                }
            }
            RunEvent::LoopIterationLaunched { group_id, parent_frames, index, stream_pulse, at_unix, .. } => {
                let key = LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), run_id };
                if self.snap.loop_runtime.get(&key).is_none() {
                    self.report(CorruptionSite::LoopIterationLaunched, format!("{}: no prior LoopInstantiated", describe(ev)));
                    return effects;
                }
                // A stream-driven launch consumed one item pulse; it is
                // consumed in the SAME atomic row as the launch marker,
                // so a refold never re-delivers a launched item. The
                // pulse targets exactly the loop's LoopIn bucket, and it
                // is REMOVED to match the live table byte for byte (see
                // the `PulsesConsumed` arm), once the launch went
                // through: a launch the program refuses leaves the item
                // where it was, so the table still holds it. Its value
                // is what the iteration receives.
                let item = match stream_pulse {
                    None => None,
                    Some(sp) => {
                        let id = match sp.parse::<Uuid>() {
                            Ok(id) => id,
                            Err(e) => {
                                self.report(CorruptionSite::LoopIterationLaunched, format!("{}: stream_pulse={sp:?} unparseable: {e}", describe(ev)));
                                return effects;
                            }
                        };
                        let loop_in_id = loop_in_id(group_id);
                        let held = self
                            .snap
                            .pulses
                            .get(&loop_in_id)
                            .and_then(|bucket| bucket.iter().find(|p| p.id == id))
                            .map(|p| LoopStreamItem { pulse: p.id, value: p.value.clone(), origin: p.origin.clone() });
                        match held {
                            Some(item) => Some(item),
                            None => {
                                self.report(CorruptionSite::LoopIterationLaunched, format!("{}: stream_pulse={sp} names a pulse LoopIn '{loop_in_id}' does not hold", describe(ev)));
                                return effects;
                            }
                        }
                    }
                };
                let taken = item.as_ref().map(|i| i.pulse);
                match launch_iteration(
                    &mut self.snap.loop_runtime, &key, *index, item, &self.project, &self.program_idx, &mut self.snap.pulses,
                    &mut self.snap.scopes, &mut self.snap.kicked, *at_unix,
                ) {
                    Ok(launch) => {
                        if let Some(id) = taken {
                            self.snap.pulses.remove_consumed(&loop_in_id(group_id), &[id]);
                        }
                        effects.emissions.extend(launch.emissions);
                        effects.scopes.push((ScopeCopy::new(group_id.clone(), launch.body_frames), ScopeState::Open));
                    }
                    Err(e) => self.report(CorruptionSite::LoopIterationLaunched, format!("{}: {e}", describe(ev))),
                }
            }
            RunEvent::LoopOutFired { group_id, parent_frames, index, .. } => {
                let key = LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), run_id };
                let Some(config) = self.snap.loop_runtime.get(&key).map(|i| i.config.clone()) else {
                    self.report(CorruptionSite::LoopOutFired, format!("{}: no prior LoopInstantiated", describe(ev)));
                    return effects;
                };
                let loop_out_id = loop_out_id(group_id);
                let Some(def) = self.project.nodes.iter().find(|n| n.id == loop_out_id) else {
                    self.report(CorruptionSite::LoopOutFired, format!("{}: the program has no LoopOut '{loop_out_id}'", describe(ev)));
                    return effects;
                };
                let body_frames = loop_runtime::iteration_frames(parent_frames, *index);
                let Some(view) = self.firing_view_of(&loop_out_id, &body_frames) else {
                    self.report(CorruptionSite::LoopOutFired, format!("{}: no LoopOut firing at the iteration's frames", describe(ev)));
                    return effects;
                };
                match classify_loop_out(def, &config, &view) {
                    Ok(writes) => {
                        // The vote is an effect only once the firing's
                        // writes landed: a row the runtime refuses is
                        // corruption, and a firing it already holds (or
                        // one after the loop ended) recorded nothing;
                        // neither paints.
                        match self.snap.loop_runtime.apply_loop_out_writes(&key, *index, writes.gather_writes, writes.carry_writes, writes.origins) {
                            Ok(true) => effects.loop_out_done = Some(writes.done_vote),
                            Ok(false) => {}
                            Err(e) => self.report(CorruptionSite::LoopOutFired, format!("{}: {e}", describe(ev))),
                        }
                    }
                    Err(e) => self.report(CorruptionSite::LoopOutFired, format!("{}: {e}", describe(ev))),
                }
            }
            RunEvent::LoopStreamEnded { group_id, parent_frames, end, .. } => {
                let key = LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), run_id };
                if let Err(e) = self.snap.loop_runtime.record_stream_end(&key, end.clone()) {
                    self.report(CorruptionSite::LoopStreamEnded, format!("{}: {e}", describe(ev)));
                }
            }
            RunEvent::LoopTerminated { group_id, parent_frames, reason, .. } => {
                let key = LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), run_id };
                if self.snap.loop_runtime.get(&key).is_none() {
                    self.report(CorruptionSite::LoopTerminated, format!("{}: no prior LoopInstantiated", describe(ev)));
                    return effects;
                }
                if matches!(reason, LoopTerminationReason::Failed | LoopTerminationReason::Cancelled) {
                    // An abnormal end has no payload: the instance is
                    // marked dead and the consumers are told "nothing
                    // will arrive" so they cascade-skip instead of
                    // deadlocking. Already terminated (a replayed row):
                    // nothing to close twice.
                    match self.snap.loop_runtime.terminate(&key, *reason) {
                        Ok(true) => {
                            effects.emissions.extend(close_loop_outward(&key, &self.project, &self.program_idx, &mut self.snap.pulses, *reason));
                        }
                        Ok(false) => {}
                        Err(e) => self.report(CorruptionSite::LoopTerminated, format!("{}: {e}", describe(ev))),
                    }
                } else {
                    match self.snap.loop_runtime.emit_outward(&key, *reason) {
                        Err(e) => self.report(CorruptionSite::LoopTerminated, format!("{}: {e}", describe(ev))),
                        // Already terminated: a replayed row, nothing to
                        // put on the wires twice.
                        Ok(LoopAdvance::Idle) => {}
                        Ok(LoopAdvance::EmitOutward { gather, carry, origins, .. }) => {
                            match emit_loop_outward(&key, gather, carry, &origins, &self.project, &self.program_idx, &mut self.snap.pulses) {
                                Ok((output, emissions)) => {
                                    for (port, value) in output {
                                        self.remember_output(OutputEmission {
                                            id: loop_termination_emission(group_id, parent_frames), node: loop_out_id(group_id),
                                            frames: parent_frames.clone(), port, value: Some(value), error: None,
                                        });
                                    }
                                    effects.emissions.extend(emissions);
                                }
                                Err(e) => self.report(CorruptionSite::LoopTerminated, format!("{}: {e}", describe(ev))),
                            }
                        }
                        Ok(LoopAdvance::LaunchNext { .. }) => {
                            self.report(CorruptionSite::LoopTerminated, format!("{}: emit_outward answered LaunchNext; LoopRuntime invariant violated", describe(ev)));
                        }
                    }
                }
            }
            RunEvent::SuspensionRegistered { node_id, frames, token, spec, call_index, at_unix, .. } => {
                self.snap.suspensions.insert(
                    token.clone(),
                    SuspensionInfo {
                        node_id: node_id.clone(),
                        frames: frames.clone(),
                        spec: spec.clone(),
                        created_at_unix: *at_unix,
                        call_index: *call_index,
                    },
                );
                // Close the out-of-order window: the wait's registration is
                // the worker's row and its answer may be the dispatcher's
                // (written while nobody drives the run), so nothing orders
                // the two, and an answer can be on record first. If the
                // resolution already landed, `pending_deliveries` holds its
                // value; stamp it now so the entry is born resolved, and
                // this row is the one that ended the wait.
                // Without this, the SuspensionResolved arm found no entry to
                // mark (not registered yet), the entry lands `ended:
                // None`, and the await never resumes (permanent hang, fire
                // consumed). Making the fold order-insensitive for the
                // Registered/Resolved pair is the right invariant.
                let ended = self.snap.pending_deliveries.remove(token);
                let answer_kept = self.answers_kept.remove(token);
                let at = FiringLocation::new(node_id.clone(), frames.clone());
                self.awaits.insert(token.clone(), at.clone());
                if let Some(end) = &ended {
                    effects.wait_ended = Some(EndedWait { at: at.clone(), token: token.clone(), end: end.clone(), answer_kept });
                }
                self.snap
                    .awaited_sequences
                    .entry(at)
                    .or_default()
                    .push(AwaitedEntry {
                        call_index: *call_index,
                        kind: AwaitedEntryKind::Await { token: token.clone(), ended },
                    });
            }
            RunEvent::RunOutput { node_id, frames, call_index, name, value, .. } => {
                self.snap
                    .awaited_sequences
                    .entry(FiringLocation::new(node_id.clone(), frames.clone()))
                    .or_default()
                    .push(AwaitedEntry {
                        call_index: *call_index,
                        kind: AwaitedEntryKind::Run { name: name.clone(), value: value.clone() },
                    });
            }
            RunEvent::SuspensionResolved { token, value, .. } => {
                let kept = self.kept_at(at);
                effects.wait_ended = self.end_await(token, AwaitEnd::Answered { value: value.clone() }, kept);
            }
            RunEvent::SuspensionSkipped { token, .. } => effects.wait_ended = self.end_await(token, AwaitEnd::Skipped, None),
            // Written by the worker that held the wait, after its
            // `SuspensionRegistered`, so the entry is always there.
            RunEvent::SuspensionGaveUp { token, error, .. } => {
                effects.wait_ended = self.end_await(token, AwaitEnd::GaveUp { error: error.clone() }, None);
            }
            // A metered call's cost record: the cost of a firing belongs on
            // its run record. A record may already be terminal when
            // the cost lands (a meter settles once the call it measured is
            // over, which can be after its node ended); the fold still books
            // it onto the matching (run, frames) record. An unknown amount (`None`) adds
            // nothing here (the sum is a number); the honest unknown lives
            // in the event's own row.
            RunEvent::CostReported { node_id, frames, amount_usd, .. } => {
                if let Some(amount) = amount_usd {
                    if let Some(e) = self.record_at_mut(node_id, frames) {
                        e.cost_usd += amount;
                    }
                }
            }
            // Observability-only events: they carry no state the resume
            // fold needs. Bus replay and caller-exchange replay are read
            // straight from the row stream by the inspector, not from the
            // snapshot. Caller events are additionally non-durable by
            // design (a live connection dies with its worker), so they
            // never contribute to a resumed run's state. A charge's
            // opening is read by [`ChargeBook`], which the drive reads.
            RunEvent::ChargeOpened { .. }
            | RunEvent::LogLine { .. }
            | RunEvent::FileEdited { .. }
            | RunEvent::TriggerCaptured { .. }
            | RunEvent::RunTagged { .. }
            | RunEvent::RunCompleted { .. }
            | RunEvent::RunFailed { .. }
            | RunEvent::RunCancelled { .. }
            | RunEvent::BusJoined { .. }
            | RunEvent::BusLeft { .. }
            | RunEvent::BusWindow { .. }
            | RunEvent::BusClosed { .. }
            | RunEvent::CallerConnected { .. }
            | RunEvent::CallerWindow { .. }
            | RunEvent::CallerErrored { .. }
            | RunEvent::CallerDisconnected { .. } => {}
        }
        effects
    }

    // ----- Display accessors ------------------------------------------

    /// What the firing at `(node, frames)` received: the bag
    /// its body read and the wired ports that arrived closed, rebuilt
    /// from the pulses the record absorbed (or, for a kicked entry
    /// node, from its kick). `None` when no firing sits there.
    ///
    /// Each value comes with where the record keeps it, when the fold
    /// knows ([`Self::apply_at`]). A showing fold ([`Self::showing`]) has
    /// dropped the values of a firing that ended.
    pub fn firing_view(&self, node_id: &str, frames: &LoopFrames) -> Option<FiringView> {
        let record = self.record_at(node_id, frames)?;
        let view = &record.received;
        Some(FiringView {
            input: view.input.iter().map(|(port, value)| (port.clone(), Shown::of(view.origins.get(port), value))).collect(),
            closed_ports: view.closed_ports.clone(),
            provided_ports: view.provided_ports.clone(),
            inherited_ports: view.inherited_ports.clone(),
        })
    }

    /// What the firing at `(node, frames)` handed out, merged
    /// across its emissions (later wins per port), each value with where
    /// the record keeps it when the fold knows. `None` for a firing that
    /// emitted nothing, or none there.
    pub fn output_of(&self, node_id: &str, frames: &LoopFrames) -> Option<BTreeMap<String, Shown>> {
        let record = self.record_at(node_id, frames)?;
        self.shown_output(record.id)
    }

    /// What record `id` handed out, shown ([`Self::output_of`]).
    fn shown_output(&self, id: Uuid) -> Option<BTreeMap<String, Shown>> {
        let bag = self.outputs.get(&id).filter(|bag| !bag.is_empty())?;
        let kept = self.output_kept.get(&id);
        Some(bag.iter().map(|(port, value)| (port.clone(), Shown::of(kept.and_then(|kept| kept.get(port)), value))).collect())
    }

    /// The values the firing at `(node, frames)` handed out, one per
    /// port (a generator port's last item); `None` for a firing that
    /// emitted nothing, or none there. What a body resumed after a
    /// pause reads its earlier emissions from. By place, never by a
    /// record id: the worker's own record of a firing carries an id of
    /// its own.
    pub fn firing_output(&self, node_id: &str, frames: &LoopFrames) -> Option<&OutputBag> {
        self.outputs.get(&self.record_at(node_id, frames)?.id)
    }

    /// The run's outputs: per node, what its last completed firing
    /// handed out.
    /// The last completed output bag of every node at every place it
    /// ran, keyed by the place (`one/@src:lib:clean.strip`): one file
    /// included twice is two entries, never one overwriting the other.
    /// A loop's boundaries are the compiler's and hold nothing a person
    /// wrote, so they are left out.
    pub fn final_outputs(&self) -> PlaceOutputs<Shown> {
        let boundary = |id: &str| self.project.nodes.iter().any(|n| n.id == id && n.group_boundary.is_some());
        // Per place, its last completed firing by start: the one a fold
        // that forgets kept as it forgot the firing, or one still held.
        let mut last: BTreeMap<Located, (u64, Uuid)> = self.kept_output.clone().into_iter().collect();
        for (node_id, firings) in self.snap.firings.iter() {
            if boundary(node_id) { continue; }
            for firing in firings.values().filter(|e| e.status() == NodeFiringStatus::Completed) {
                let place = Located::at(node_id, &firing.frames);
                if last.get(&place).is_none_or(|&(kept, _)| kept < firing.start()) {
                    last.insert(place, (firing.start(), firing.id));
                }
            }
        }
        PlaceOutputs(last.into_iter().filter_map(|(place, (_, id))| Some((place.to_string(), self.shown_output(id)?))).collect())
    }

    // ----- Internals --------------------------------------------------

    fn remember_output(&mut self, emission: OutputEmission) {
        if self.keeps.as_ref().is_some_and(|keeps| !keeps.places.contains(&Located::at(&emission.node, &emission.frames))) {
            return;
        }
        if let Some(history) = &mut self.output_history {
            if self.output_ids.insert((emission.id, emission.node.clone(), emission.port.clone())) {
                history.push(emission);
            }
        }
    }

    /// The given outputs of this run's stand-ins (`RunSnapshot::given`)
    /// that were handed in by hand or baked, as the history of their place:
    /// what the run's review and a frozen example read for them. One a
    /// seed carried in belongs to the run it came from, whose history
    /// holds it already.
    fn remember_given(&mut self) {
        if self.output_history.is_none() { return; }
        let project = self.project.clone();
        let given = self.snap.given.clone();
        for (place, stand_in) in &given.outputs {
            if matches!(stand_in.origin, GivenOrigin::Inherited { .. }) { continue; }
            let frames = place.frames();
            let generator = |port: &str| project.nodes.iter().find(|node| node.id == place.id)
                .and_then(|node| node.outputs.iter().find(|output| output.name == port)).is_some_and(|output| output.is_generator());
            for (port, given) in &stand_in.ports {
                let at = |id: Uuid, value: Option<Arc<Value>>, error: Option<Failure>| OutputEmission {
                    id, node: place.id.clone(), frames: frames.clone(), port: port.clone(), value, error,
                };
                let id = stand_in_emission(&place.id, &frames);
                match given {
                    GivenPort::Value { value } if generator(port) => {
                        for (index, item) in value.as_array().into_iter().flatten().enumerate() {
                            self.remember_output(at(stream_emission(&place.id, &frames, index as u64), Some(Arc::new(item.clone())), None));
                        }
                        self.remember_output(at(id, None, None));
                    }
                    GivenPort::Value { value } => self.remember_output(at(id, Some(Arc::new(value.clone())), None)),
                    GivenPort::Closed { failure } => self.remember_output(at(id, None, failure.clone())),
                }
            }
        }
    }

    /// What a copy that did not run closed on the wires leaving it, as
    /// the history of the member each wire leaves from, with the failure
    /// that stopped the copy (`scope::settle_table` closes them so): the
    /// history is what a seeded run stands in from and what a frozen
    /// example expects, so a plain closure there would launder the
    /// failure for both. A copy that did not run because the one around
    /// it did not closed nothing of its own.
    fn remember_scope_closures(&mut self, copy: &ScopeCopy, reason: &SkipReason) {
        if self.output_history.is_none() || matches!(reason, SkipReason::ScopeSkipped { .. }) { return; }
        let Some(scope) = self.program_idx.scopes().get(&copy.scope) else { return };
        let id = weft_core::run::emission::scope_emission(&copy.scope, &copy.frames);
        let closures: Vec<OutputEmission> = scope.leaving.iter()
            .map(|leg| (&self.project.edges[leg.edge], leg.inner_frames(&copy.frames)))
            .filter(|(edge, _)| self.program_idx.node(&self.project, &edge.source).is_some())
            .map(|(edge, frames)| OutputEmission {
                id, node: edge.source.clone(), frames, port: edge.source_port().to_string(),
                value: None, error: reason.inherited_failure().cloned(),
            })
            .collect();
        for closure in closures {
            self.remember_output(closure);
        }
    }

    /// What a loop that never ran closed on its outward ports, as its
    /// `LoopOut`'s history at the loop's frames (`scope::tear_down_loop`).
    fn remember_loop_closures(&mut self, loop_id: &str, frames: &LoopFrames, reason: &SkipReason) {
        if self.output_history.is_none() || matches!(reason, SkipReason::ScopeSkipped { .. }) { return; }
        let node_id = loop_out_id(loop_id);
        let id = weft_core::run::emission::scope_emission(loop_id, frames);
        let ports: Vec<String> = self.project.nodes.iter().find(|node| node.id == node_id).into_iter()
            .flat_map(|node| node.outputs.iter().map(|port| port.name.clone())).collect();
        for port in ports {
            self.remember_output(OutputEmission { id, node: node_id.clone(), frames: frames.clone(), port,
                value: None, error: reason.inherited_failure().cloned() });
        }
    }

    fn firing_view_of(&self, node_id: &str, frames: &LoopFrames) -> Option<weft_core::run::ready::FiringInput> {
        self.record_at(node_id, frames).map(|record| record.received.clone())
    }

    /// What the firing about to start at `(node, frames)` receives. A
    /// value handed to one of its ports is kept in the run's birth, which
    /// is where it is said to be when the fold names where values are
    /// kept, whether the firing was reached by a pulse or by a kick (a
    /// node whose inputs are all handed has nothing feeding it, so a kick
    /// starts it). A trigger's kicked ports and a literal are kept in no
    /// row.
    fn receiving(&self, node_id: &str, frames: &LoopFrames) -> weft_core::run::ready::FiringInput {
        let def = self.project.nodes.iter().find(|n| n.id == node_id).expect("validated node");
        let pending: Vec<_> = self.snap.pulses.get(node_id).into_iter().flatten()
            .filter(|p| p.run_id == self.snap.run_id && p.frames == *frames && p.status.is_pending()).collect();
        let kick = pending.is_empty().then(|| self.snap.kicked.get(&FiringLocation::new(node_id, frames.clone()))).flatten();
        let mut received = match kick {
            Some(kick) => kicked_group(def, kick, frames, self.snap.run_id, &self.project, &self.program_idx, &self.snap.given).received,
            None => firing_input(def, &pending, &wired_inputs(&self.project, &self.program_idx, node_id, frames), frames, &self.snap.given),
        };
        // A trigger's fire hands it its setup's snapshot, never what the
        // run is given.
        let handed = !kick.is_some_and(|kick| kick.firing);
        if handed && self.positioned() {
            let place = Located::at(node_id, frames);
            for (port, handed) in self.snap.given.inputs.get(&place).into_iter().flatten() {
                if received.input.get(port).is_some_and(|value| **value == *handed) {
                    let kept = ValueRef::Given { run: self.snap.run_id, place: place.clone(), port: port.clone(), side: GivenSide::Input };
                    received.origins.insert(port.clone(), Arc::new(kept));
                }
            }
        }
        received
    }

    /// Decide what the run's scopes can decide now (`scope::settle_table`,
    /// the pass the live engine runs at the top of every turn).
    fn scope_pass(&mut self, now: u64, effects: &mut FoldEffects) {
        // Rows before the birth row fold as a run fired by a trigger.
        let given = self.snap.given.clone();
        let pass = settle_table(
            &self.project,
            &self.program_idx,
            &given,
            self.phase.unwrap_or(weft_core::context::Phase::Fire),
            self.snap.run_id,
            now,
            &mut self.snap.pulses,
            &mut self.snap.scopes,
            &mut self.snap.kicked,
        );
        for (copy, state) in &pass.decided {
            if let ScopeState::Skipped { reason } = state {
                self.remember_scope_closures(copy, reason);
            }
        }
        effects.emissions.extend(pass.emissions);
        effects.scopes.extend(pass.decided);
    }

    /// Absorb every pending pulse at `(node, frames)` and return
    /// their ids, minus the items on the node's generator ports when
    /// `include_generator` is false (a run dispatch leaves those for
    /// its live feed; a skip absorbs everything).
    fn absorb_pending(&mut self, node_id: &str, frames: &LoopFrames, include_generator: bool) -> Vec<Uuid> {
        let generator_ports = self.program_idx.stream_inputs(node_id);
        self.snap.pulses.absorb(node_id, |p| {
            &p.frames == frames && (include_generator || !generator_ports.contains(&p.target_port))
        })
    }

    /// A port of a running firing closed (the body's own close, or a
    /// refused value). `false` when the program cannot place the
    /// closure (reported as corruption, nothing applied).
    #[allow(clippy::too_many_arguments)]
    fn close_port(
        &mut self,
        node_id: &str,
        frames: &LoopFrames,
        port: &str,
        emission_id: Uuid,
        ev: &RunEvent,
        effects: &mut FoldEffects,
    ) -> bool {
        match emit_port_closure(
            node_id, port, emission_id, self.snap.run_id, frames, &self.project,
            &mut self.snap.pulses, &self.program_idx, &mut effects.emissions, None,
        ) {
            Ok(()) => {
                self.remember_output(OutputEmission {
                    id: emission_id, node: node_id.into(), frames: frames.clone(),
                    port: port.into(), value: None, error: None,
                });
                let record = self.record_at_mut(node_id, frames)
                    .expect("`firing_record_id` found this record and nothing removes records");
                record.mentioned_ports.insert(port.to_string());
                record.closed_output_ports.insert(port.to_string());
                true
            }
            Err(e) => {
                self.report(CorruptionSite::PortClosed, format!("{}: {e}", describe(ev)));
                false
            }
        }
    }

    /// A firing ended: close its record and put the closures it owes
    /// on the wires. An ordinary node closes every output it never
    /// mentioned (a skip absorbed everything and mentioned nothing; a
    /// scope-skipped one owes nothing, its scope's teardown closed what
    /// leaves the scope). A `LoopIn` that skipped (its loop gated off)
    /// takes its loop down (`scope::tear_down_loop`): the loop's outward
    /// ports close and everything inside is told. A loop boundary
    /// otherwise closes nothing on its own: a failing one whose instance
    /// never existed closes the loop's outward surface here; every other
    /// loop teardown rode a `LoopTerminated` row. `gave_up_wait` is a
    /// failure's (`NodeFiring::gave_up_wait`).
    #[allow(clippy::too_many_arguments)]
    fn terminate(
        &mut self,
        node_id: &str,
        frames: &LoopFrames,
        status: NodeFiringStatus,
        error: Option<&str>,
        skip_reason: Option<&SkipReason>,
        gave_up_wait: bool,
        at_unix: u64,
        ev: &RunEvent,
        effects: &mut FoldEffects,
    ) {
        let run_id = self.snap.run_id;
        let Some(def) = self.node_def(node_id, ev) else { return };
        if self.firing_record_id(node_id, frames, CorruptionSite::NodeLifecycle, ev).is_none() {
            return;
        }
        // A skip decided before the body ran is the whole group's
        // consumption: whatever was left pending for its feeds (a
        // stream's items) goes with it. A body that ran and ended
        // skipped (a person skipped what it waited for) ends like one that
        // completed: it took its inputs when it started, and a stream
        // item reaching it after its end is dropped by the routing, as
        // for any ended consumer.
        if status == NodeFiringStatus::Skipped && skip_reason != Some(&SkipReason::WaitSkipped) {
            let more = self.absorb_pending(node_id, frames, true);
            if let Some(e) = self.record_at_mut(node_id, frames) {
                for id in more {
                    if !e.pulses_absorbed.contains(&id) {
                        e.pulses_absorbed.push(id);
                    }
                }
            }
        }
        let e = self.snap.firings.set_status(node_id, frames, status)
            .expect("`firing_record_id` found this record and nothing removes records");
        e.skip_reason = skip_reason.cloned();
        e.completed_at = Some(at_unix);
        if let Some(err) = error {
            e.error = Some(err.to_string());
        }
        e.gave_up_wait = gave_up_wait;
        e.callback_id = None;
        let is_loop_boundary = weft_core::project::boundary_types::is_boundary(&def.node_type);
        let loop_entered = def.group_boundary.as_ref()
            .filter(|_| def.node_type == weft_core::project::boundary_types::LOOP_IN)
            .map(|boundary| boundary.group_id.clone());
        let emission_id = terminal_sweep_emission(node_id, frames);
        match (&status, skip_reason) {
            (NodeFiringStatus::Skipped, Some(reason)) if loop_entered.is_some() => {
                let loop_id = loop_entered.expect("checked");
                let pass = tear_down_loop(
                    &self.project, &self.program_idx, &loop_id, frames, reason, run_id, at_unix,
                    &mut self.snap.pulses, &mut self.snap.scopes, &mut self.snap.kicked,
                );
                self.remember_loop_closures(&loop_id, frames, reason);
                effects.emissions.extend(pass.emissions);
                effects.scopes.extend(pass.decided);
            }
            (NodeFiringStatus::Skipped, Some(SkipReason::ScopeSkipped { .. })) => {}
            _ if is_loop_boundary => {
                // A boundary firing that failed with no instance behind
                // it: the loop never got a `LoopTerminated`, so its
                // outward surface closes from here.
                if status == NodeFiringStatus::Failed {
                    if let Ok(key) = loop_runtime::instance_key(&def, frames, run_id) {
                        if self.snap.loop_runtime.get(&key).is_none() {
                            effects.emissions.extend(close_loop_outward(&key, &self.project, &self.program_idx, &mut self.snap.pulses, LoopTerminationReason::Failed));
                        }
                    }
                }
            }
            _ => {
                let mentioned = e.mentioned_ports.clone();
                let closed = e.closed_output_ports.clone();
                // A skip row carries no error of its own; the failure it
                // inherited (an input that closed because its producer
                // broke) rides its closures exactly as the live engine
                // sends them.
                let own = error.map(|error| Failure::at(&self.project, node_id, frames, error));
                let failure = own.as_ref().or_else(|| skip_reason.and_then(SkipReason::inherited_failure));
                if let Err(e) = close_unmentioned_downstream(
                    node_id, &mentioned, emission_id, run_id, frames, &self.project,
                    &mut self.snap.pulses, &self.program_idx, &mut effects.emissions, failure, &closed,
                ) {
                    // A rejected row decides no scope: the reader that
                    // paints the row paints nothing for it, and decisions
                    // it made possible without their cause would show alone.
                    self.report(CorruptionSite::NodeLifecycle, format!("{}: {e}", describe(ev)));
                    return;
                }
                for port in def.outputs.iter().filter(|port| !closed.contains(&port.name) && (!mentioned.contains(&port.name) || port.is_generator())) {
                    self.remember_output(OutputEmission {
                        id: emission_id, node: node_id.into(), frames: frames.clone(), port: port.name.clone(),
                        value: None, error: failure.cloned(),
                    });
                }
            }
        }
    }

    /// The program's node a row is about, or a corruption when the
    /// program has no such node.
    fn node_def(&mut self, node_id: &str, ev: &RunEvent) -> Option<NodeDefinition> {
        match self.project.nodes.iter().find(|n| n.id == node_id) {
            Some(def) => Some(def.clone()),
            None => {
                self.report(CorruptionSite::NodeLifecycle, format!("{}: the program has no node '{node_id}'", describe(ev)));
                None
            }
        }
    }

    /// The id of the record at `(node, frames)`, or a
    /// corruption at `site` when there is none (a row about a firing
    /// the journal never opened).
    fn firing_record_id(&mut self, node_id: &str, frames: &LoopFrames, site: CorruptionSite, ev: &RunEvent) -> Option<Uuid> {
        match self.record_at(node_id, frames).map(|e| e.id) {
            Some(id) => Some(id),
            None => {
                self.report(site, format!("{}: no firing of '{node_id}' at these frames", describe(ev)));
                None
            }
        }
    }

    /// End the wait `token` with `end`, unless it already ended: the
    /// first ending on record stands, so an answer to a wait already given
    /// up is ignored (the call it would have answered failed, and stays
    /// failed). Answers the wait it ended, `None` when it had already
    /// ended or is not registered yet. An ending that comes before its
    /// registration (an answer the dispatcher wrote while nobody drove the
    /// run) waits in `pending_deliveries`, and there too the first one
    /// stands.
    /// `answer_kept` is where the record keeps an answer.
    fn end_await(&mut self, token: &str, end: AwaitEnd, answer_kept: Option<Arc<ValueRef>>) -> Option<EndedWait> {
        if let Some(at) = self.awaits.get(token) {
            let entry = self.snap.awaited_sequences.get_mut(at).into_iter().flatten().find_map(|entry| match &mut entry.kind {
                AwaitedEntryKind::Await { token: t, ended } if t == token => Some(ended),
                _ => None,
            });
            let Some(ended) = entry else {
                // Every registration lands its entry beside its token; a
                // token whose entry is gone is a fold that lost track.
                unreachable!("wait {token} is registered at {at:?} and its entry is not there");
            };
            if ended.is_some() {
                return None;
            }
            *ended = Some(end.clone());
            return Some(EndedWait { at: at.clone(), token: token.to_string(), end, answer_kept });
        }
        if !self.snap.pending_deliveries.contains_key(token) {
            self.snap.pending_deliveries.insert(token.to_string(), end);
            if let Some(kept) = answer_kept {
                self.answers_kept.insert(token.to_string(), kept);
            }
        }
        None
    }

    /// The record a row about `(node, frames)` is about: the same
    /// `firing_at` the live engine ends and sweeps by.
    fn record_at(&self, node_id: &str, frames: &LoopFrames) -> Option<&NodeFiring> {
        firing_at(&self.snap.firings, node_id, self.snap.run_id, frames)
    }

    fn record_at_mut(&mut self, node_id: &str, frames: &LoopFrames) -> Option<&mut NodeFiring> {
        firing_at_mut(&mut self.snap.firings, node_id, self.snap.run_id, frames)
    }

    fn parse_ids(&mut self, ids: &[String], site: CorruptionSite) -> Vec<Uuid> {
        let mut out = Vec::with_capacity(ids.len());
        for s in ids {
            match s.parse::<Uuid>() {
                Ok(id) => out.push(id),
                Err(e) => self.report(site, format!("pulse_id={s:?} unparseable: {e}")),
            }
        }
        out
    }

    fn report(&mut self, site: CorruptionSite, reason: String) {
        tracing::error!(
            target: "weft_journal::fold",
            run_id = %self.snap.run_id, ?site, reason = %reason,
            "skip row during fold (journal corruption)"
        );
        self.snap.corruptions.push(JournalCorruption { site, reason });
    }
}

/// A row named in a corruption report: its kind and the firing it is
/// about, never its payload.
fn describe(ev: &RunEvent) -> String {
    match ev {
        RunEvent::PortEmitted { node_id, frames, port, .. }
        | RunEvent::PortClosed { node_id, frames, port, .. } => {
            format!("{} node={node_id} frames={frames:?} port={port}", ev.kind_str())
        }
        RunEvent::NodeStarted { node_id, frames, .. }
        | RunEvent::NodeSuspended { node_id, frames, .. }
        | RunEvent::NodeResumed { node_id, frames, .. }
        | RunEvent::NodeCompleted { node_id, frames, .. }
        | RunEvent::NodeFailed { node_id, frames, .. }
        | RunEvent::NodeSkipped { node_id, frames, .. }
        | RunEvent::NodeCancelled { node_id, frames, .. }
        | RunEvent::PulsesConsumed { node_id, frames, .. } => {
            format!("{} node={node_id} frames={frames:?}", ev.kind_str())
        }
        RunEvent::LoopInstantiated { group_id, parent_frames, .. }
        | RunEvent::LoopStreamEnded { group_id, parent_frames, .. }
        | RunEvent::LoopTerminated { group_id, parent_frames, .. } => {
            format!("{} group={group_id} parent_frames={parent_frames:?}", ev.kind_str())
        }
        RunEvent::LoopIterationLaunched { group_id, parent_frames, index, .. }
        | RunEvent::LoopOutFired { group_id, parent_frames, index, .. } => {
            format!("{} group={group_id} parent_frames={parent_frames:?} index={index}", ev.kind_str())
        }
        other => other.kind_str().to_string(),
    }
}

/// What a firing received, for the screen.
pub struct FiringView {
    pub input: BTreeMap<String, Shown>,
    pub closed_ports: Vec<String>,
    /// The ports whose value a person handed in or a stand-in sent.
    pub provided_ports: Vec<String>,
    /// The ports a seed's saved result sent, with the run that made it.
    pub inherited_ports: std::collections::BTreeMap<String, RunId>,
}

/// The charges a run opened (`RunEvent::ChargeOpened`) that no cost on
/// its record closed yet: what a drive picking the run up again holds
/// open, so the read that states their amount prices them, and what a run
/// ending for good books as unknown. Keyed as the worker keeps them
/// (`weft_engine::metering`): by service and the provider's id.
///
/// Kept apart from [`Fold`]: it needs no program. A drive reads it before
/// anything else of the run; whoever ends a run with no drive (a cancel,
/// a worker's give-up) reads it to close what is left
/// ([`unpriced_charges_closed`]).
#[derive(Debug, Default)]
pub struct ChargeBook {
    open: BTreeMap<(String, String), OpenedCharge>,
}

/// One charge still open, as its `ChargeOpened` wrote it.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenedCharge {
    pub node_id: String,
    pub frames: LoopFrames,
    pub service: String,
    /// The id the provider gave the charge.
    pub charge: String,
    pub scratch: Value,
    pub origin: weft_core::CredentialOwner,
}

impl ChargeBook {
    /// The charges `events`, a run's record in order, leave open.
    pub fn of_record<'a>(events: impl IntoIterator<Item = &'a RunEvent>) -> Self {
        let mut book = Self::default();
        for event in events {
            book.apply(event);
        }
        book
    }

    /// Apply one row: an opening adds its charge, a cost naming a charge
    /// closes it, every other row leaves the book as it is.
    pub fn apply(&mut self, ev: &RunEvent) {
        match ev {
            RunEvent::ChargeOpened { node_id, frames, service, charge, scratch, origin, .. } => {
                self.open.insert((service.clone(), charge.clone()), OpenedCharge {
                    node_id: node_id.clone(),
                    frames: frames.clone(),
                    service: service.clone(),
                    charge: charge.clone(),
                    scratch: scratch.clone(),
                    origin: origin.clone(),
                });
            }
            RunEvent::CostReported { service, charge: Some(charge), .. } => {
                self.open.remove(&(service.clone(), charge.clone()));
            }
            _ => {}
        }
    }

    /// The charges still open, in a stable order.
    pub fn into_open(self) -> impl Iterator<Item = OpenedCharge> {
        self.open.into_values()
    }
}

/// What a run ending with no drive to price them writes before its ending:
/// one unknown `CostReported` per charge `book` (the run's record, read in
/// order) leaves open, closing it, booked where its `ChargeOpened` was
/// (node, iteration, service, whose key). `why` says how the run ended.
/// Every writer of such an ending (the dispatcher's cancel of a run nobody
/// drives, the broker's give-up of a run whose worker could not write its
/// record) goes through here, so no spend ends neither priced nor unknown.
pub fn unpriced_charges_closed(run_id: RunId, book: ChargeBook, why: &str, at_unix: u64) -> Vec<RunEvent> {
    book.into_open()
        .map(|charge| {
            let (model, metadata) = unpriced_cost(charge.scratch, why);
            RunEvent::CostReported {
                run_id,
                node_id: charge.node_id,
                frames: charge.frames,
                cost_id: Uuid::now_v7().to_string(),
                service: charge.service,
                model,
                amount_usd: None,
                billed: false,
                origin: charge.origin,
                metadata,
                charge: Some(charge.charge),
                at_unix,
            }
        })
        .collect()
}

/// The model and metadata an open charge is booked unknown with: the
/// meter's state for it (`scratch`), its resolution naming `why` nothing
/// priced it. The one wording, whether the worker that held the charge
/// books it or the run ended with no drive ([`unpriced_charges_closed`]).
pub fn unpriced_cost(scratch: Value, why: &str) -> (Option<String>, Value) {
    // `scratch` is meter-owned and a meter may replace it wholesale, so
    // it is not guaranteed to be an object. Indexing a non-object `Value`
    // panics, and the worker reaches this from a destructor that can run
    // during an unwind, where a panic aborts the process. Keep whatever
    // the meter left, under a key, rather than trusting its shape.
    let mut metadata = match scratch {
        object @ Value::Object(_) => object,
        other => serde_json::json!({ "meterState": other }),
    };
    metadata["resolution"] = serde_json::json!(format!("the call spent, and no response ever stated what it cost ({why})"));
    let model = metadata["model"].as_str().map(str::to_string);
    (model, metadata)
}

/// Fold a whole log at once.
pub fn fold_to_snapshot<'a>(
    run_id: RunId,
    project: Arc<ProjectDefinition>,
    events: impl IntoIterator<Item = &'a RunEvent>,
) -> RunSnapshot {
    let mut fold = Fold::new(run_id, project);
    for ev in events {
        fold.apply(ev);
    }
    fold.into_snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::run::NodeFiringTable;

    /// What a fold shows of some ports, written out.
    fn shown(ports: &BTreeMap<String, Shown>) -> Value {
        Value::Object(ports.iter().map(|(port, value)| (port.clone(), value.value.as_deref().cloned().expect("a fold holds what it shows"))).collect())
    }

    /// What a fold shows of a run's outputs, written out.
    fn shown_outputs(outputs: &PlaceOutputs<Shown>) -> Value {
        Value::Object(outputs.0.iter().map(|(place, ports)| (place.clone(), shown(ports))).collect())
    }
    use serde_json::json;
    use weft_core::frames::Frame;
    use weft_core::pulse::PulseStatus;

    fn run_id() -> RunId {
        Uuid::nil()
    }

    fn frame(index: u32) -> Frame {
        Frame::Loop { index }
    }

    /// A node in a test program: `(id, type, inputs, outputs, scope,
    /// boundary)`. Inputs are `(name, type, required)`.
    fn node(
        id: &str,
        ty: &str,
        inputs: &[(&str, &str, bool)],
        outputs: &[(&str, &str)],
        scope: &[&str],
        boundary: Value,
        config: Value,
    ) -> Value {
        json!({
            "id": id, "nodeType": ty, "label": null, "config": config,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": inputs.iter().map(|(n, t, r)| json!({ "name": n, "portType": t, "required": r })).collect::<Vec<_>>(),
            "outputs": outputs.iter().map(|(n, t)| json!({ "name": n, "portType": t, "required": true })).collect::<Vec<_>>(),
            "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false, "images": []
        })
    }

    fn edge(source: &str, sh: &str, target: &str, th: &str) -> Value {
        through(source, sh, &[], target, th)
    }

    /// A wire through the group ports `via`, each `(group, port, side)`.
    fn through(source: &str, sh: &str, via: &[(&str, &str, &str)], target: &str, th: &str) -> Value {
        json!({
            "id": format!("{source}.{sh}->{target}.{th}"), "source": source, "sourceHandle": sh, "target": target, "targetHandle": th,
            "via": via.iter().map(|(group, port, side)| json!({ "group": group, "port": port, "side": side })).collect::<Vec<_>>(),
        })
    }

    /// A program of `nodes` and `edges`, with a loop for every `LoopIn`
    /// among the nodes.
    fn project(nodes: Vec<Value>, edges: Vec<Value>) -> Arc<ProjectDefinition> {
        let loops: Vec<Value> = nodes.iter()
            .filter(|node| node["nodeType"] == "LoopIn")
            .map(|node| {
                let id = node["groupBoundary"]["groupId"].clone();
                let members: Vec<Value> = nodes.iter().filter(|n| n["scope"].as_array().and_then(|s| s.last()) == Some(&id)).map(|n| n["id"].clone()).collect();
                json!({ "id": id, "kind": "loop", "loopConfig": {}, "nodeIds": members })
            })
            .collect();
        project_with(nodes, edges, loops)
    }

    fn project_with(nodes: Vec<Value>, edges: Vec<Value>, groups: Vec<Value>) -> Arc<ProjectDefinition> {
        Arc::new(
            serde_json::from_value(json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "nodes": nodes,
                "edges": edges,
                "groups": groups,
                "createdAt": "1970-01-01T00:00:00Z",
                "updatedAt": "1970-01-01T00:00:00Z",
            }))
            .expect("test project"),
        )
    }

    fn group(id: &str, kind: Value, parent: Option<&str>, ports: &[(&str, &str)]) -> Value {
        let mut group = kind;
        group["id"] = json!(id);
        group["parentGroupId"] = json!(parent);
        group["inPorts"] = json!(ports.iter().filter(|(_, side)| *side == "in").map(|(name, _)| json!({ "name": name, "portType": "Number", "required": false })).collect::<Vec<_>>());
        group["outPorts"] = json!(ports.iter().filter(|(_, side)| *side == "out").map(|(name, _)| json!({ "name": name, "portType": "Number", "required": false })).collect::<Vec<_>>());
        group
    }

    /// `src.out` fans out to `a.in`, `b.in` and `c.in`.
    fn fan_out_project() -> Arc<ProjectDefinition> {
        project(
            vec![
                node("src", "T", &[], &[("out", "Number")], &[], Value::Null, Value::Null),
                node("a", "T", &[("in", "Number", true)], &[("out", "Number")], &[], Value::Null, Value::Null),
                node("b", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
                node("c", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![edge("src", "out", "a", "in"), edge("src", "out", "b", "in"), edge("src", "out", "c", "in")],
        )
    }

    fn started(node: &str, frames: LoopFrames, at: u64) -> RunEvent {
        RunEvent::NodeStarted { run_id: run_id(), node_id: node.into(), frames, at_unix: at }
    }

    fn completed(node: &str, frames: LoopFrames, at: u64) -> RunEvent {
        RunEvent::NodeCompleted { run_id: run_id(), node_id: node.into(), frames, at_unix: at }
    }

    fn emitted(emission: Uuid, node: &str, frames: LoopFrames, port: &str, value: Value) -> RunEvent {
        RunEvent::PortEmitted {
            run_id: run_id(),
            emission_id: emission,
            node_id: node.into(),
            frames,
            port: port.into(),
            value: Arc::new(value),
            at_unix: 0,
        }
    }

    fn started_run() -> RunEvent {
        RunEvent::RunStarted {
            run_id: run_id(),
            project_id: uuid::Uuid::nil(),
            entry_node: "src".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("h".into()),
            binary_hash: None, cut_from: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
            seed: None,
            instance: None, stand_in: None, fired_trigger: None, settings: Default::default(), instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            given: Default::default(),
        }
    }

    /// The birth of a run given `given`, and `definition` saying where.
    fn started_given(definition: &mut ProjectDefinition, given: weft_core::project::Given) -> RunEvent {
        definition.given = given.places();
        let RunEvent::RunStarted { run_id, project_id, entry_node, phase, definition_hash, binary_hash, cut_from, source_version, run_kind, seed, instance, stand_in, fired_trigger, settings, instance_values, picks, at_unix, .. } = started_run() else { unreachable!() };
        RunEvent::RunStarted { run_id, project_id, entry_node, phase, definition_hash, binary_hash, cut_from, source_version, run_kind, seed, instance, stand_in, fired_trigger, settings, instance_values, picks, at_unix, given: Box::new(given) }
    }

    fn kicked(node: &str) -> RunEvent {
        RunEvent::NodeKicked {
            run_id: run_id(),
            node_id: node.into(), frames: vec![],
            firing: true,
            payload: None,
            port_snapshot: None,
            at_unix: 0,
        }
    }

    /// The one pulse absorbed at `node`.
    fn only_absorbed<'a>(snap: &'a RunSnapshot, node: &str) -> &'a weft_core::pulse::Pulse {
        let absorbed: Vec<_> = snap.pulses.all_absorbed().filter(|p| p.target_node == node).collect();
        assert_eq!(absorbed.len(), 1, "one pulse absorbed at {node}: {absorbed:?}");
        absorbed[0]
    }

    fn pending<'a>(snap: &'a RunSnapshot, node: &str) -> Vec<&'a weft_core::pulse::Pulse> {
        snap.pulses.get(node).map(|b| b.iter().filter(|p| p.status.is_pending()).collect()).unwrap_or_default()
    }

    /// A replay crosses a call the way the live engine did: the wire into
    /// a site pushes its frame, the shared body's rows carry it, and the
    /// wire out hands the result back to that site alone.
    #[test]
    fn a_call_replays_under_its_site_frame_and_answers_its_own_site() {
        let project = project_with(
            vec![
                node("src", "T", &[], &[("a", "Number"), ("b", "Number")], &[], Value::Null, Value::Null),
                node("B.n", "T", &[("in", "Number", true)], &[("out", "Number")], &["B"], Value::Null, Value::Null),
                node("sa", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
                node("sb", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![
                through("src", "a", &[("a", "x", "in"), ("B", "x", "in")], "B.n", "in"),
                through("src", "b", &[("b", "x", "in"), ("B", "x", "in")], "B.n", "in"),
                through("B.n", "out", &[("B", "y", "out"), ("a", "y", "out")], "sa", "in"),
                through("B.n", "out", &[("B", "y", "out"), ("b", "y", "out")], "sb", "in"),
            ],
            vec![
                group("a", json!({ "kind": "call", "body": "B" }), None, &[("x", "in"), ("y", "out")]),
                group("b", json!({ "kind": "call", "body": "B" }), None, &[("x", "in"), ("y", "out")]),
                group("B", json!({ "kind": "body", "nodeIds": ["B.n"] }), None, &[("x", "in"), ("y", "out")]),
            ],
        );
        let call = |site: &str| vec![Frame::Call { site: site.into() }];
        let emission = Uuid::new_v4();
        let events = vec![
            started_run(),
            kicked("src"),
            started("src", vec![], 1),
            emitted(emission, "src", vec![], "a", json!(1)),
            emitted(emission, "src", vec![], "b", json!(2)),
            completed("src", vec![], 2),
            // The body's member ran for call `a` and answered.
            started("B.n", call("a"), 3),
            emitted(Uuid::new_v4(), "B.n", call("a"), "out", json!(10)),
            completed("B.n", call("a"), 4),
        ];
        let mut fold = Fold::new(run_id(), project);
        for ev in &events {
            fold.apply(ev);
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        // Each call's copy of the body opened, under its site's frame.
        for site in ["a", "b"] {
            let copy = ScopeCopy::new("B", call(site));
            assert_eq!(snap.scopes.get(&copy).map(|record| &record.state), Some(&ScopeState::Open), "{copy}");
        }
        // `B.n` for call `b` is still waiting with its own value.
        let waiting = pending(snap, "B.n");
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].frames, call("b"));
        assert_eq!(*waiting[0].value, json!(2));
        // The answer came back to `a`'s sink at the root, and only there.
        let sa = pending(snap, "sa");
        assert_eq!(sa.len(), 1);
        assert_eq!(*sa[0].value, json!(10));
        assert!(sa[0].frames.is_empty());
        assert!(pending(snap, "sb").is_empty());
    }

    /// A fired trigger reads its baked port snapshot without rerunning
    /// the source that prepared it.
    #[test]
    fn a_fired_triggers_wired_port_shows_the_value_that_reached_it() {
        let project = project(
            vec![
                node("sched", "T", &[], &[("value", "String")], &[], Value::Null, Value::Null),
                node("wired", "Cron", &[("cron", "String", true)], &[("scheduledTime", "String")], &[], Value::Null, Value::Null),
            ],
            vec![edge("sched", "value", "wired", "cron")],
        );
        let cron = json!("0 0 * * * *");
        let events = vec![
            started_run(),
            RunEvent::NodeKicked {
                run_id: run_id(),
                node_id: "wired".into(), frames: vec![],
                firing: true,
                payload: Some(json!({"scheduledTime":"event"})),
                port_snapshot: Some(json!({"cron":cron})),
                at_unix: 0,
            },
            started("wired", vec![], 1),
        ];
        let mut fold = Fold::new(run_id(), project);
        for ev in &events {
            fold.apply(ev);
        }
        assert!(fold.snapshot().corruptions.is_empty(), "{:?}", fold.snapshot().corruptions);
        let view = fold.firing_view("wired", &vec![]).expect("the trigger fired");
        assert_eq!(shown(&view.input), json!({ "cron": cron }), "the wired port carries what reached it");
        assert!(view.provided_ports.is_empty(), "baked settings are not authored emissions");
    }

    /// A stand-in (`--emit`, here) reaches an ordinary consumer while its
    /// node has no firing of its own: it sends as the run starts, and its
    /// consumer says the value was handed in.
    #[test]
    fn a_stand_in_reaches_its_consumers_view() {
        let mut definition = project(
            vec![
                node("sched", "T", &[], &[("value", "String")], &[], Value::Null, Value::Null),
                node("wired", "T", &[("cron", "String", true)], &[("scheduledTime", "String")], &[], Value::Null, Value::Null),
            ],
            vec![edge("sched", "value", "wired", "cron")],
        ).as_ref().clone();
        let cron = json!("0 0 * * * *");
        let mut given = weft_core::project::Given::default();
        given.outputs.insert(Located::top("sched"), weft_core::project::StandIn {
            ports: [("value".to_string(), GivenPort::Value { value: cron.clone() })].into(),
            origin: GivenOrigin::Emitted,
        });
        let birth = started_given(&mut definition, given);
        let mut fold = Fold::new(run_id(), Arc::new(definition)).with_output_history();
        for ev in [birth, started("wired", vec![], 1)] {
            assert!(!fold.apply(&ev).rejected());
        }
        assert!(!fold.snapshot().firings.contains_key("sched"), "a stand-in never runs");
        let view = fold.firing_view("wired", &vec![]).expect("the consumer ran");
        assert_eq!(shown(&view.input), json!({ "cron": cron }));
        assert_eq!(view.provided_ports, vec!["cron"]);
        let wires = fold.output_wires().unwrap();
        assert!(wires.iter().any(|wire| wire.node == "sched" && wire.port == "value" && wire.value == cron), "the history holds what it sent");
    }

    /// One emission row fans out over the program: three consumers,
    /// one shared value, and the consumer that started absorbed its
    /// pulse while the others still wait.
    #[test]
    fn one_emission_row_fans_out_and_a_start_absorbs_its_pulse() {
        let project = fan_out_project();
        let emission = Uuid::new_v4();
        let big = json!(4242);
        let events = vec![
            started_run(),
            kicked("src"),
            started("src", vec![], 0),
            emitted(emission, "src", vec![], "out", big.clone()),
            completed("src", vec![], 1),
            started("a", vec![], 2),
        ];
        let mut fold = Fold::new(run_id(), project);
        for ev in &events {
            fold.apply(ev);
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert!(snap.pulses["a"].is_empty(), "the start took a's pulse off its wire");
        let a = only_absorbed(snap, "a");
        let b = &snap.pulses["b"][0];
        let c = &snap.pulses["c"][0];
        assert_eq!(b.status, PulseStatus::Pending);
        assert_eq!(c.status, PulseStatus::Pending);
        assert!(Arc::ptr_eq(&a.value, &b.value) && Arc::ptr_eq(&b.value, &c.value), "one allocation");
        assert_eq!(*a.value, big);
        assert_eq!(snap.firings["a"][0].pulses_absorbed, vec![a.id]);
        assert_eq!(a.id, weft_core::run::emission::pulse_id(emission, "src.out->a.in", false));
        let view = fold.firing_view("a", &vec![]).expect("a fired");
        assert_eq!(shown(&view.input), json!({ "in": big }));
        assert_eq!(fold.output_of("src", &vec![]).map(|output| shown(&output)), Some(json!({ "out": big })));
        assert_eq!(shown_outputs(&fold.final_outputs()), json!({ "src": { "out": big } }));
    }

    /// A completion closes every port the firing never mentioned; a
    /// consumer whose required input closed folds skipped when its
    /// skip row lands, and nothing stays pending at its location.
    #[test]
    fn a_terminal_sweeps_the_unmentioned_ports_and_a_skip_absorbs_the_closure() {
        let project = fan_out_project();
        let events = vec![
            started("src", vec![], 0),
            completed("src", vec![], 1),
            started("b", vec![], 2),
            RunEvent::NodeSkipped {
                run_id: run_id(),
                node_id: "b".into(),
                frames: vec![],
                reason: SkipReason::RequiredInputClosed { port: "in".into(), failure: None },
                at_unix: 2,
            },
        ];
        let snap = fold_to_snapshot(run_id(), project, &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        for n in ["a", "c"] {
            assert_eq!(snap.pulses[n].len(), 1, "{n} got a closure");
            assert!(snap.pulses[n][0].closed);
        }
        assert!(snap.pulses["b"].is_empty(), "the skip took b's closure off its wire");
        let b = only_absorbed(&snap, "b");
        assert!(b.closed);
        assert_eq!(snap.firings["b"][0].status(), NodeFiringStatus::Skipped);
        assert!(pending(&snap, "b").is_empty());
        // `a` and `c` never started: their closures wait for the
        // scheduler.
        assert_eq!(pending(&snap, "a").len(), 1);
    }

    /// A replayed emission row is one pulse: the fold is idempotent
    /// over a crash-replayed writer.
    #[test]
    fn a_replayed_emission_row_is_idempotent() {
        let project = fan_out_project();
        let emission = Uuid::new_v4();
        let events = vec![
            started("src", vec![], 0),
            emitted(emission, "src", vec![], "out", json!(1)),
            emitted(emission, "src", vec![], "out", json!(1)),
        ];
        let mut fold = Fold::new(run_id(), project);
        for e in &events {
            fold.apply(e);
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(snap.pulses["a"].len(), 1);
        assert_eq!(snap.pulses["b"].len(), 1);
        assert_eq!(snap.pulses["a"][0].id, weft_core::run::emission::pulse_id(emission, "src.out->a.in", false));
        assert_eq!(*snap.pulses["a"][0].value, json!(1));
        assert_eq!(snap.pulses["a"][0].status, PulseStatus::Pending);
        assert_eq!(fold.output_of("src", &vec![]).map(|output| shown(&output)), Some(json!({ "out": 1 })));
    }

    /// The awaited sequence of a firing reads in call order whatever
    /// order its rows landed in (a register and a run output are
    /// written by independent dispatcher paths).
    #[test]
    fn an_awaited_sequence_reads_in_call_order() {
        let register = |call_index: u32| RunEvent::SuspensionRegistered {
            run_id: run_id(),
            node_id: "a".into(),
            frames: vec![],
            token: format!("t{call_index}"),
            spec: make_spec(),
            call_index,
            at_unix: 0,
        };
        let events = vec![
            register(2),
            RunEvent::RunOutput { run_id: run_id(), node_id: "a".into(), frames: vec![], call_index: 0, name: "decide".into(), value: json!("left"), at_unix: 0 },
            register(1),
        ];
        let snap = fold_to_snapshot(run_id(), fan_out_project(), &events);
        let seq = &snap.awaited_sequences[&FiringLocation::new("a", vec![])];
        assert_eq!(seq.iter().map(|e| e.call_index).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert!(matches!(&seq[0].kind, AwaitedEntryKind::Run { name, .. } if name == "decide"));
        assert!(matches!(&seq[1].kind, AwaitedEntryKind::Await { token, .. } if token == "t1"));
    }

    /// `gen.rows` streams into loop `lp` over its `rows` port; the body
    /// `add` reads `item` and writes `res`.
    fn stream_loop_project() -> Arc<ProjectDefinition> {
        project(
            vec![
                node("gen", "T", &[], &[("rows", "Generator[Number]")], &[], Value::Null, Value::Null),
                node(
                    "lp__in",
                    "LoopIn",
                    &[("rows", "Generator[Number]", true), ("_should_flow", "Boolean", false)],
                    &[("rows", "Number"), ("index", "Number")],
                    &[],
                    json!({ "groupId": "lp", "role": "In" }),
                    json!({ "parallel": false, "over": ["rows"], "carry": [] }),
                ),
                node("add", "T", &[("item", "Number", true)], &[("res", "Number")], &["lp"], Value::Null, Value::Null),
                node(
                    "lp__out",
                    "LoopOut",
                    &[("res", "Number", false), ("done", "Boolean", false)],
                    &[("res", "List[Number]")],
                    &[],
                    json!({ "groupId": "lp", "role": "Out" }),
                    json!({ "parentId": null }),
                ),
                node("sink", "T", &[("res", "List[Number]", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![
                edge("gen", "rows", "lp__in", "rows"),
                edge("lp__in", "rows", "add", "item"),
                edge("add", "res", "lp__out", "res"),
                edge("lp__out", "res", "sink", "res"),
            ],
        )
    }

    /// A stream-driven launch takes its item off the LoopIn's bucket
    /// and hands it to the iteration; the stream's end is durable on
    /// the instance.
    #[test]
    fn a_stream_launch_takes_its_item_and_the_stream_end_is_durable() {
        let e1 = Uuid::new_v4();
        let e2 = Uuid::new_v4();
        let item1 = weft_core::run::emission::pulse_id(e1, "gen.rows->lp__in.rows", false);
        let events = vec![
            started("gen", vec![], 0),
            emitted(e1, "gen", vec![], "rows", json!(7)),
            started("lp__in", vec![], 1),
            loop_row_instantiated(),
            emitted(e2, "gen", vec![], "rows", json!(8)),
            RunEvent::LoopIterationLaunched {
                run_id: run_id(),
                group_id: "lp".into(),
                parent_frames: vec![],
                index: 0,
                stream_pulse: Some(item1.to_string()),
                at_unix: 1,
            },
            RunEvent::LoopStreamEnded {
                run_id: run_id(),
                group_id: "lp".into(),
                parent_frames: vec![],
                end: weft_core::generator::StreamEnd::Failed { error: "upstream broke".into() },
                at_unix: 2,
            },
        ];
        let snap = fold_to_snapshot(run_id(), stream_loop_project(), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let add = pending(&snap, "add");
        assert_eq!(add.len(), 1, "the item (the implicit index has no wire here): {add:?}");
        let item = add.iter().find(|p| p.target_port == "item").expect("the item reached the body");
        assert_eq!(*item.value, json!(7));
        assert_eq!(item.frames, vec![frame(0)]);
        let loop_in: Vec<Uuid> = snap.pulses["lp__in"].iter().map(|p| p.id).collect();
        assert!(!loop_in.contains(&item1), "the launched item left the LoopIn's bucket");
        assert_eq!(loop_in.len(), 1, "the second item still waits there");
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.launched, weft_core::run::loop_runtime::Indices::from([0]));
        let ended = snap.loop_runtime.stream_instances_with_recorded_end();
        assert_eq!(ended.len(), 1);
        assert!(matches!(&ended[0].2, weft_core::generator::StreamEnd::Failed { error } if error == "upstream broke"));
    }

    /// The crash window between an emission and the consumer's start:
    /// the pulse refolds Pending, so the consumer is ready (it never
    /// started, so dispatching it is its first run, not a second).
    #[test]
    fn a_crash_between_emission_and_start_refolds_the_consumer_ready() {
        let project = fan_out_project();
        let events = vec![
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(1)),
            completed("src", vec![], 1),
        ];
        let snap = fold_to_snapshot(run_id(), project.clone(), &events);
        let program_idx = ProgramIndex::build(&project);
        let ready = weft_core::run::find_ready_nodes(&project, &snap.pulses, &program_idx, &snap.given);
        let mut ids: Vec<&str> = ready.iter().map(|(n, _)| n.as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    /// A row about a firing the journal never opened, or a node the
    /// program does not have, is corruption, never a silent record.
    #[test]
    fn rows_about_unknown_firings_are_corruption() {
        let project = fan_out_project();
        let events = vec![
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(1)),
            completed("ghost", vec![], 1),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "nope", json!(1)),
        ];
        let snap = fold_to_snapshot(run_id(), project, &events);
        let sites: Vec<CorruptionSite> = snap.corruptions.iter().map(|c| c.site).collect();
        assert_eq!(
            sites,
            vec![CorruptionSite::PortEmitted, CorruptionSite::NodeLifecycle, CorruptionSite::PortEmitted],
            "{:?}",
            snap.corruptions
        );
        assert!(!snap.pulses.contains_key("a"), "an emission before its start put nothing on the wires");
    }

    /// `src.out` reaches `inner.in` through group `g`'s port `x` and its
    /// nested group `h`'s, and `inner.out` leaves both on `y` for
    /// `sink.in`; `g`'s gate is `src.flow`.
    fn nested_group_project() -> Arc<ProjectDefinition> {
        project_with(
            vec![
                node("src", "T", &[], &[("out", "Number"), ("flow", "Boolean")], &[], Value::Null, Value::Null),
                node("inner", "T", &[("in", "Number", true)], &[("out", "Number")], &["g", "h"], Value::Null, Value::Null),
                node("sink", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![
                through("src", "out", &[("g", "x", "in"), ("h", "x", "in")], "inner", "in"),
                edge("src", "flow", "g", "_should_flow"),
                through("inner", "out", &[("h", "y", "out"), ("g", "y", "out")], "sink", "in"),
            ],
            vec![
                group("g", json!({ "kind": "group" }), None, &[("x", "in"), ("y", "out")]),
                group("h", json!({ "kind": "group", "nodeIds": ["inner"] }), Some("g"), &[("x", "in"), ("y", "out")]),
            ],
        )
    }

    fn decided(snap: &RunSnapshot, scope: &str) -> Option<ScopeState> {
        snap.scopes.get(&ScopeCopy::new(scope, vec![])).map(|record| record.state.clone())
    }

    /// Two nested scopes open from the fold with no row of their own: the
    /// gate's value opens `g`, which makes `h` decidable, and the value
    /// that reached `inner` opens it. The rows say which they decided.
    #[test]
    fn nested_scopes_open_from_the_rows_alone() {
        let project = nested_group_project();
        let events = vec![
            started_run(),
            kicked("src"),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(9)),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(true)),
            completed("src", vec![], 1),
        ];
        let mut fold = Fold::new(run_id(), project);
        let mut opened = Vec::new();
        for ev in &events {
            let effects = fold.apply(ev);
            opened.extend(effects.scopes.into_iter().map(|(copy, state)| (copy.scope, state)));
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(opened, vec![("g".to_string(), ScopeState::Open), ("h".to_string(), ScopeState::Open)]);
        let inner = pending(snap, "inner");
        assert_eq!(inner.len(), 1);
        assert_eq!(*inner[0].value, json!(9));
    }

    /// A gated-off scope folds as not run: the copy inside it with it,
    /// every member kicked into a scope skip, and the scope's consumer
    /// sees a closure. What reached a member before is absorbed.
    #[test]
    fn a_gated_off_scope_folds_skipped() {
        let project = nested_group_project();
        let events = vec![
            started_run(),
            kicked("src"),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(9)),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(false)),
            completed("src", vec![], 1),
        ];
        let snap = fold_to_snapshot(run_id(), project, &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(decided(&snap, "g"), Some(ScopeState::Skipped { reason: SkipReason::DidNotFlow }));
        assert_eq!(decided(&snap, "h"), Some(ScopeState::Skipped { reason: SkipReason::ScopeSkipped { scope: "g".into() } }));
        assert!(pending(&snap, "sink")[0].closed);
        let kick = snap.kicked.get(&FiringLocation::new("inner", vec![])).expect("inner told");
        assert_eq!(kick.scope_skipped.as_deref(), Some("g"));
        assert!(pending(&snap, "inner").is_empty());
    }

    /// The history says what the wire said: a node skipped because its
    /// input closed on a failure closes its own outputs WITH that
    /// failure, and the frozen example (`output_wires`) records it so.
    #[test]
    fn a_skip_inherited_from_a_failure_is_remembered_with_it() {
        let down = Failure { node: "src".into(), error: "the database is down".into() };
        let mut fold = Fold::new(run_id(), fan_out_project()).with_output_history();
        for event in [
            started_run(), kicked("src"), started("src", vec![], 0),
            RunEvent::NodeFailed { run_id: run_id(), node_id: "src".into(), frames: vec![], error: "the database is down".into(), gave_up_wait: false, at_unix: 1 },
            started("a", vec![], 2),
            RunEvent::NodeSkipped {
                run_id: run_id(), node_id: "a".into(), frames: vec![],
                reason: SkipReason::RequiredInputClosed { port: "in".into(), failure: Some(down.clone()) },
                at_unix: 2,
            },
        ] { assert!(!fold.apply(&event).rejected()); }
        let wires = fold.output_wires().unwrap();
        let a_out = wires.iter().find(|wire| wire.node == "a" && wire.port == "out").expect("a.out remembered");
        assert!(a_out.closed);
        assert_eq!(a_out.failure.as_ref(), Some(&down), "the skip passes the failure on, in the history too");
    }

    /// Whether a failure gave up a wait rides its `NodeFailed` row, so the
    /// record a refold builds on any worker says it; a row written before
    /// the fact existed reads as a failure that gave up nothing.
    #[test]
    fn a_failure_that_gave_up_a_wait_says_so_after_a_refold() {
        let failed = |gave_up_wait| RunEvent::NodeFailed {
            run_id: run_id(), node_id: "src".into(), frames: vec![], error: "gave up its wait".into(), gave_up_wait, at_unix: 1,
        };
        for gave_up_wait in [true, false] {
            let row = serde_json::to_value(failed(gave_up_wait)).unwrap();
            assert_eq!(row.get("gave_up_wait").is_some(), gave_up_wait, "only a wait given up is written: {row}");
            let back: RunEvent = serde_json::from_value(row).unwrap();
            let events = [started_run(), kicked("src"), started("src", vec![], 0), back];
            let snap = fold_to_snapshot(run_id(), fan_out_project(), &events);
            assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
            let record = &snap.firings["src"][0];
            assert_eq!(record.status(), NodeFiringStatus::Failed);
            assert_eq!(record.gave_up_wait, gave_up_wait);
        }
    }

    /// A scope gated off by a gate that closed on a failure closes
    /// what leaves it with that failure, and the history remembers it
    /// under the member each wire leaves from, once: the scope inside
    /// remembers nothing of its own (a plain closure there would launder
    /// the enclosing failure on replay).
    #[test]
    fn a_scope_gated_off_by_a_failure_is_remembered_with_it() {
        let mut fold = Fold::new(run_id(), nested_group_project()).with_output_history();
        for event in [
            started_run(), kicked("src"), started("src", vec![], 0),
            RunEvent::NodeFailed { run_id: run_id(), node_id: "src".into(), frames: vec![], error: "the database is down".into(), gave_up_wait: false, at_unix: 1 },
        ] { assert!(!fold.apply(&event).rejected()); }
        let snap = fold.snapshot();
        let down = Failure { node: "src".into(), error: "the database is down".into() };
        assert_eq!(decided(snap, "g"), Some(ScopeState::Skipped { reason: SkipReason::FlowClosed { failure: Some(down.clone()) } }));
        // Two hops past the node that broke (the gate closed, the scope
        // closed what leaves it), the closure still names `src`.
        assert_eq!(pending(snap, "sink")[0].failure.as_ref(), Some(&down), "the wire carries it");
        let wires = fold.output_wires().unwrap();
        let out: Vec<_> = wires.iter().filter(|wire| wire.node == "inner" && wire.port == "out").collect();
        assert_eq!(out.len(), 1, "{wires:?}");
        assert!(out[0].closed);
        assert_eq!(out[0].failure.as_ref(), Some(&down), "and so does the history");
    }

    /// A value a member's port will refuse still opens the scope it
    /// reaches: a group's port takes what its wire brings, and the member
    /// refuses it at its own firing (`weft_core::run::ready::firing_input`).
    #[test]
    fn a_value_a_member_will_refuse_opens_its_scope() {
        let mut fold = Fold::new(run_id(), nested_group_project()).with_output_history();
        for event in [
            started_run(), kicked("src"), started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!("nine")),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(true)),
            completed("src", vec![], 1),
        ] { assert!(!fold.apply(&event).rejected()); }
        let snap = fold.snapshot();
        assert_eq!(decided(snap, "h"), Some(ScopeState::Open));
        assert_eq!(*pending(snap, "inner")[0].value, json!("nine"));
        // The member takes it at its firing, and refuses it there: its
        // `in` is a Number.
        assert!(!fold.apply(&started("inner", vec![], 2)).rejected());
        let received = fold.firing_view_of("inner", &vec![]).expect("inner fired");
        let error = received.error().expect("a String on a Number port is a type error");
        assert!(error.contains("'in'"), "{error}");
    }

    /// A seeded run whose reused gate source stands in for itself decides
    /// the scope again from what it sends: off, the members it runs again
    /// are told, as in the run they came from.
    #[test]
    fn a_reused_gate_decides_the_scope_again() {
        let mut definition = nested_group_project().as_ref().clone();
        let parent = Uuid::new_v4();
        let mut given = weft_core::project::Given::default();
        given.outputs.insert(Located::top("src"), weft_core::project::StandIn {
            ports: [
                ("out".to_string(), GivenPort::Value { value: json!(9) }),
                ("flow".to_string(), GivenPort::Value { value: json!(false) }),
            ].into(),
            origin: GivenOrigin::Inherited { run: parent },
        });
        let birth = started_given(&mut definition, given);
        let mut child = Fold::new(Uuid::new_v4(), Arc::new(definition));
        assert!(!child.apply(&birth).rejected());
        let snap = child.snapshot();
        assert_eq!(decided(snap, "g"), Some(ScopeState::Skipped { reason: SkipReason::DidNotFlow }));
        let kick = snap.kicked.get(&FiringLocation::new("inner", vec![])).expect("inner told again");
        assert_eq!(kick.scope_skipped.as_deref(), Some("g"));
        assert_eq!(pending(snap, "sink")[0].inherited_from, None, "the closure is the child's own scope's");
    }

    /// Two members leaving a scope that did not run on ports of one name
    /// are two closures in the history, one per member.
    #[test]
    fn closures_of_two_members_on_one_port_name_keep_both() {
        let project = project_with(
            vec![
                node("src", "T", &[], &[("flow", "Boolean")], &[], Value::Null, Value::Null),
                node("one", "T", &[], &[("out", "Number")], &["g"], Value::Null, Value::Null),
                node("two", "T", &[], &[("out", "Number")], &["g"], Value::Null, Value::Null),
                node("sink", "T", &[("a", "Number", false), ("b", "Number", false)], &[], &[], Value::Null, Value::Null),
            ],
            vec![
                edge("src", "flow", "g", "_should_flow"),
                through("one", "out", &[("g", "a", "out")], "sink", "a"),
                through("two", "out", &[("g", "b", "out")], "sink", "b"),
            ],
            vec![group("g", json!({ "kind": "group", "nodeIds": ["one", "two"] }), None, &[("a", "out"), ("b", "out")])],
        );
        let mut fold = Fold::new(run_id(), project).with_output_history();
        for event in [
            started_run(), kicked("src"), started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(false)),
            completed("src", vec![], 1),
        ] { assert!(!fold.apply(&event).rejected()); }
        let outputs = fold.output_wires().unwrap();
        for member in ["one", "two"] {
            assert_eq!(outputs.iter().filter(|wire| wire.node == member && wire.port == "out" && wire.closed).count(), 1, "{member} closure survives");
        }
    }

    /// `items` feeds loop `lp` over `items` with carry `acc`; the body
    /// `add` reads `item` and `acc` and writes `acc` and `res`.
    fn loop_project(parallel: bool) -> Arc<ProjectDefinition> {
        project(
            vec![
                node("feed", "T", &[], &[("items", "List[Number]"), ("seed", "Number")], &[], Value::Null, Value::Null),
                node(
                    "lp__in",
                    "LoopIn",
                    &[("items", "List[Number]", true), ("acc", "Number", false), ("_should_flow", "Boolean", false)],
                    &[("items", "Number"), ("acc", "Number"), ("index", "Number")],
                    &[],
                    json!({ "groupId": "lp", "role": "In" }),
                    json!({ "parallel": parallel, "over": ["items"], "carry": ["acc"] }),
                ),
                node("add", "T", &[("item", "Number", true), ("acc", "Number", true)], &[("acc", "Number"), ("res", "Number")], &["lp"], Value::Null, Value::Null),
                node(
                    "lp__out",
                    "LoopOut",
                    &[("acc", "Number", false), ("res", "Number", false), ("done", "Boolean", false)],
                    &[("acc", "Number"), ("res", "List[Number]")],
                    &[],
                    json!({ "groupId": "lp", "role": "Out" }),
                    json!({ "parentId": null }),
                ),
                node("sink", "T", &[("res", "List[Number]", true), ("acc", "Number", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![
                edge("feed", "items", "lp__in", "items"),
                edge("feed", "seed", "lp__in", "acc"),
                edge("lp__in", "items", "add", "item"),
                edge("lp__in", "acc", "add", "acc"),
                edge("add", "acc", "lp__out", "acc"),
                edge("add", "res", "lp__out", "res"),
                edge("lp__out", "res", "sink", "res"),
                edge("lp__out", "acc", "sink", "acc"),
            ],
        )
    }

    fn loop_row_instantiated() -> RunEvent {
        RunEvent::LoopInstantiated { run_id: run_id(), group_id: "lp".into(), parent_frames: vec![], at_unix: 0 }
    }

    fn loop_row_launched(index: u32) -> RunEvent {
        RunEvent::LoopIterationLaunched {
            run_id: run_id(),
            group_id: "lp".into(),
            parent_frames: vec![],
            index,
            stream_pulse: None,
            at_unix: 0,
        }
    }

    fn loop_row_out_fired(index: u32) -> RunEvent {
        RunEvent::LoopOutFired { run_id: run_id(), group_id: "lp".into(), parent_frames: vec![], index, at_unix: 0 }
    }

    fn lp_key() -> LoopInstanceKey {
        LoopInstanceKey { group_id: "lp".into(), parent_frames: vec![], run_id: run_id() }
    }

    /// The rows a sequential loop writes up to the first LoopOut.
    fn loop_prefix() -> Vec<RunEvent> {
        vec![
            started("feed", vec![], 0),
            emitted(Uuid::new_v4(), "feed", vec![], "items", json!([10, 20, 30])),
            emitted(Uuid::new_v4(), "feed", vec![], "seed", json!(1)),
            completed("feed", vec![], 0),
            started("lp__in", vec![], 1),
            loop_row_instantiated(),
            loop_row_launched(0),
            completed("lp__in", vec![], 1),
        ]
    }

    /// A launch the runtime refuses (here a stream launch on a loop
    /// whose `over` is a list) is corruption that applies nothing: the
    /// pulse the row named stays where it was and no iteration is
    /// recorded, so the peek-then-remove order is what the row rests on.
    #[test]
    fn a_refused_launch_leaves_its_pulse_and_records_no_iteration() {
        // A second value on the LoopIn's list port, landing after its
        // start took the first: a pulse still on its wire for the launch
        // to name.
        let e1 = Uuid::new_v4();
        let items = weft_core::run::emission::pulse_id(e1, "feed.items->lp__in.items", false);
        let events = vec![
            started("feed", vec![], 0),
            emitted(Uuid::new_v4(), "feed", vec![], "items", json!([10, 20, 30])),
            emitted(Uuid::new_v4(), "feed", vec![], "seed", json!(1)),
            started("lp__in", vec![], 1),
            loop_row_instantiated(),
            emitted(e1, "feed", vec![], "items", json!([10, 20, 30])),
            RunEvent::LoopIterationLaunched {
                run_id: run_id(),
                group_id: "lp".into(),
                parent_frames: vec![],
                index: 0,
                stream_pulse: Some(items.to_string()),
                at_unix: 1,
            },
        ];
        let snap = fold_to_snapshot(run_id(), loop_project(false), &events);
        assert_eq!(snap.corruptions.len(), 1, "{:?}", snap.corruptions);
        assert!(snap.corruptions[0].reason.contains("not a stream"), "{}", snap.corruptions[0].reason);
        assert!(snap.pulses["lp__in"].iter().any(|p| p.id == items), "the named pulse stays in the LoopIn's bucket");
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert!(inst.launched.is_empty(), "no iteration was recorded: {:?}", inst.launched);
        assert!(pending(&snap, "add").is_empty(), "no body pulse was put on a wire");
    }

    /// A launch naming a pulse a start already took launches nothing: an
    /// item is taken once, by the launch that names it while it is still
    /// on its wire.
    #[test]
    fn a_launch_naming_a_taken_pulse_is_corruption() {
        let e1 = Uuid::new_v4();
        let items = weft_core::run::emission::pulse_id(e1, "feed.items->lp__in.items", false);
        let events = vec![
            started("feed", vec![], 0),
            emitted(e1, "feed", vec![], "items", json!([10, 20, 30])),
            emitted(Uuid::new_v4(), "feed", vec![], "seed", json!(1)),
            completed("feed", vec![], 0),
            started("lp__in", vec![], 1),
            loop_row_instantiated(),
            RunEvent::LoopIterationLaunched {
                run_id: run_id(), group_id: "lp".into(), parent_frames: vec![], index: 0,
                stream_pulse: Some(items.to_string()), at_unix: 1,
            },
        ];
        let snap = fold_to_snapshot(run_id(), loop_project(false), &events);
        assert_eq!(snap.corruptions.len(), 1, "{:?}", snap.corruptions);
        assert!(snap.corruptions[0].reason.contains("does not hold"), "{}", snap.corruptions[0].reason);
        assert!(snap.pulses.absorbed(items).is_some(), "the start's take stands");
        assert!(snap.loop_runtime.get(&lp_key()).expect("instance").launched.is_empty());
    }

    /// A loop resumed between instantiation and its first LoopOut
    /// rebuilds the instance from the LoopIn's pulses: the outer input,
    /// the carry seed, and iteration 0's body pulses (the slice, the
    /// carry, the index).
    #[test]
    fn a_loop_resumed_before_its_first_loop_out_rebuilds_its_seeds() {
        let snap = fold_to_snapshot(run_id(), loop_project(false), &loop_prefix());
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.iter_cap, Some(3));
        assert_eq!(inst.launched, weft_core::run::loop_runtime::Indices::from([0]));
        assert_eq!(*inst.carry_values["acc"], json!(1));
        assert_eq!(*inst.outer_input["items"], json!([10, 20, 30]));
        assert!(!inst.outer_input.contains_key("_should_flow"));
        let add = pending(&snap, "add");
        let by_port = |port: &str| add.iter().find(|p| p.target_port == port).map(|p| (*p.value).clone());
        assert_eq!(by_port("item"), Some(json!(10)));
        assert_eq!(by_port("acc"), Some(json!(1)));
        assert!(add.iter().all(|p| p.frames == vec![frame(0)]));
    }

    /// LoopOut's writes come off its own firing's pulses; the next
    /// launch carries the updated carry; termination puts the
    /// assembled list and the final carry on the outward wires.
    #[test]
    fn loop_out_writes_and_termination_fold_from_the_body_pulses() {
        let mut events = loop_prefix();
        let body = |i: u32, acc: i64, res: i64| {
            vec![
                started("add", vec![frame(i)], 2),
                emitted(Uuid::new_v4(), "add", vec![frame(i)], "acc", json!(acc)),
                emitted(Uuid::new_v4(), "add", vec![frame(i)], "res", json!(res)),
                completed("add", vec![frame(i)], 2),
                started("lp__out", vec![frame(i)], 3),
                loop_row_out_fired(i),
                completed("lp__out", vec![frame(i)], 3),
            ]
        };
        events.extend(body(0, 11, 100));
        events.push(loop_row_launched(1));
        let snap = fold_to_snapshot(run_id(), loop_project(false), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.out_fired, weft_core::run::loop_runtime::Indices::from([0]));
        assert_eq!(*inst.carry_values["acc"], json!(11));
        let add1: Vec<_> = pending(&snap, "add").into_iter().filter(|p| p.frames == vec![frame(1)]).collect();
        assert_eq!(*add1.iter().find(|p| p.target_port == "acc").unwrap().value, json!(11), "iteration 1 carries the write");
        assert_eq!(*add1.iter().find(|p| p.target_port == "item").unwrap().value, json!(20));

        events.extend(body(1, 22, 200));
        events.push(loop_row_launched(2));
        events.extend(body(2, 33, 300));
        events.push(RunEvent::LoopTerminated {
            run_id: run_id(),
            group_id: "lp".into(),
            parent_frames: vec![],
            reason: LoopTerminationReason::OverExhausted,
            at_unix: 9,
        });
        let snap = fold_to_snapshot(run_id(), loop_project(false), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.terminated, Some(LoopTerminationReason::OverExhausted));
        let sink = pending(&snap, "sink");
        let by_port = |port: &str| sink.iter().find(|p| p.target_port == port).map(|p| (*p.value).clone());
        assert_eq!(by_port("res"), Some(json!([100, 200, 300])));
        assert_eq!(by_port("acc"), Some(json!(33)));
        let mut definition = loop_project(false).as_ref().clone();
        definition.groups.push(serde_json::from_value(json!({"id":"lp","kind":"loop","loopConfig":{},"nodeIds":["add"]})).unwrap());
        let eligible = weft_core::seeding::inheritable_nodes(&definition, &snap);
        assert!(["lp__in", "add", "lp__out"].iter().all(|node| eligible.contains(&Located::top(*node))));
        let mut missing_iteration = snap.clone();
        missing_iteration.firings = NodeFiringTable::new();
        for record in snap.firings.values().flat_map(|records| records.values()) {
            if !(record.node_id == "add" && record.frames == vec![frame(1)]) {
                missing_iteration.firings.insert(record.clone());
            }
        }
        let eligible = weft_core::seeding::inheritable_nodes(&definition, &missing_iteration);
        assert!(["lp__in", "add", "lp__out"].iter().all(|node| !eligible.contains(&Located::top(*node))), "one missing iteration invalidates the whole loop");
        // Replaying the terminal row puts nothing on the wires twice.
        events.push(events.last().unwrap().clone());
        let again = fold_to_snapshot(run_id(), loop_project(false), &events);
        assert_eq!(pending(&again, "sink").len(), 2);
    }

    /// A loop that ran no iteration is reused whole: the child imports its
    /// records, its `LoopOut` stands in for it with the empty result, and
    /// no body firing is invented.
    #[test]
    fn a_zero_iteration_loop_reuses_its_empty_result_without_inventing_body_firings() {
        let project = loop_project(false);
        let mut source = Fold::new(run_id(), project.clone()).with_output_history();
        for row in [
            started_run(), started("feed", vec![], 0),
            emitted(Uuid::new_v4(), "feed", vec![], "items", json!([])),
            emitted(Uuid::new_v4(), "feed", vec![], "seed", json!(1)), completed("feed", vec![], 0),
            started("lp__in", vec![], 1), loop_row_instantiated(), completed("lp__in", vec![], 1),
            RunEvent::LoopTerminated { run_id: run_id(), group_id: "lp".into(), parent_frames: vec![], reason: LoopTerminationReason::OverExhausted, at_unix: 2 },
        ] { assert!(!source.apply(&row).rejected()); }
        let members = BTreeSet::from([Located::top("lp__in"), Located::top("add"), Located::top("lp__out")]);
        let eligible = weft_core::seeding::inheritable_nodes(&project, source.snapshot());
        assert!(members.iter().all(|node| eligible.contains(node)));
        let outputs = source.output_wires().unwrap();
        let sent = weft_core::seeding::sent_ports(project.nodes.iter().find(|node| node.id == "lp__out").unwrap(), &Located::top("lp__out"), &outputs);
        assert_eq!(sent["res"], GivenPort::Value { value: json!([]) });
        let mut definition = project.as_ref().clone();
        let mut given = weft_core::project::Given::default();
        given.outputs.insert(Located::top("lp__out"), weft_core::project::StandIn { ports: sent, origin: GivenOrigin::Inherited { run: source.run_id() } });
        let birth = started_given(&mut definition, given);
        let mut child = Fold::new(Uuid::new_v4(), Arc::new(definition));
        child.apply(&birth);
        child.inherit(&source, &members).unwrap();
        child.settle(3);
        assert!(!child.snapshot().firings.contains_key("add"));
        assert!(!child.snapshot().firings.contains_key("lp__out"));
        assert_eq!(child.snapshot().firings["lp__in"][0].inherited_from, Some(source.run_id()));
        let result = pending(child.snapshot(), "sink").into_iter().find(|pulse| pulse.target_port == "res").unwrap();
        assert_eq!(result.value.as_ref(), &json!([]));
        assert_eq!(result.inherited_from, Some(source.run_id()));
        assert!(child.snapshot().loop_runtime.iter().next().is_none(), "the loop's runtime is the run it came from's");
    }

    /// A failed loop closes its outward surface from the terminal row;
    /// the boundary's own failure row then adds nothing.
    #[test]
    fn a_failed_loop_closes_its_outward_ports_once() {
        let mut events = loop_prefix();
        events.push(RunEvent::LoopTerminated {
            run_id: run_id(),
            group_id: "lp".into(),
            parent_frames: vec![],
            reason: LoopTerminationReason::Failed,
            at_unix: 9,
        });
        events.push(RunEvent::NodeFailed {
            run_id: run_id(),
            node_id: "lp__in".into(),
            frames: vec![],
            error: "boom".into(),
            gave_up_wait: false,
            at_unix: 9,
        });
        let snap = fold_to_snapshot(run_id(), loop_project(false), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let sink = pending(&snap, "sink");
        assert_eq!(sink.len(), 2);
        assert!(sink.iter().all(|p| p.closed));
    }

    /// A loop boundary that failed before any instance existed closes
    /// the loop's outward surface from its own failure row.
    #[test]
    fn a_boundary_failure_with_no_instance_closes_the_loop_outward() {
        let events = vec![
            started("lp__in", vec![], 1),
            RunEvent::NodeFailed {
                run_id: run_id(),
                node_id: "lp__in".into(),
                frames: vec![],
                error: "config".into(),
                gave_up_wait: false,
                at_unix: 1,
            },
        ];
        let snap = fold_to_snapshot(run_id(), loop_project(false), &events);
        let sink = pending(&snap, "sink");
        assert_eq!(sink.len(), 2);
        assert!(sink.iter().all(|p| p.closed));
    }

    /// Parallel lanes: every launch row lands its own body pulses at
    /// its own frames, and the lanes resume independently.
    #[test]
    fn parallel_loop_lanes_fold_independently() {
        let mut events = loop_prefix();
        events.insert(events.len() - 1, loop_row_launched(1));
        events.insert(events.len() - 1, loop_row_launched(2));
        events.extend(vec![
            started("add", vec![frame(2)], 2),
            emitted(Uuid::new_v4(), "add", vec![frame(2)], "res", json!(300)),
            completed("add", vec![frame(2)], 2),
            started("lp__out", vec![frame(2)], 3),
            loop_row_out_fired(2),
            completed("lp__out", vec![frame(2)], 3),
        ]);
        let snap = fold_to_snapshot(run_id(), loop_project(true), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.launched, weft_core::run::loop_runtime::Indices::from([0, 1, 2]));
        assert_eq!(inst.out_fired, weft_core::run::loop_runtime::Indices::from([2]));
        let lanes: HashSet<u32> = pending(&snap, "add").iter().map(|p| p.frames[0].loop_index().expect("a loop frame")).collect();
        assert_eq!(lanes, [0, 1].into_iter().collect(), "lanes 0 and 1 still wait; lane 2 absorbed");
        // A closed write records as Closed, a null slot in the list.
        assert_eq!(inst.gather_lists["res"][&2], weft_core::run::loop_runtime::LoopWrite::Value(Arc::new(json!(300))));
    }

    #[test]
    fn kicked_payload_survives_dispatch_for_resume() {
        let payload = json!({"body": "hello"});
        let events = vec![
            started_run(),
            RunEvent::NodeKicked {
                run_id: run_id(),
                node_id: "src".into(), frames: vec![],
                firing: true,
                payload: Some(payload.clone()),
                port_snapshot: None,
                at_unix: 0,
            },
            started("src", vec![], 0),
        ];
        let snap = fold_to_snapshot(run_id(), fan_out_project(), &events);
        let kick = snap.kicked.get(&FiringLocation::new("src", Vec::new())).expect("kick survives fold");
        assert!(kick.dispatched(), "NodeStarted at root frames consumed the kick");
        assert_eq!(kick.payload.as_ref(), Some(&payload));
    }

    fn make_spec() -> weft_core::primitive::SignalSpec {
        use weft_core::signal::{to_spec, Form, FormSchema};
        to_spec(Form {
            form_type: "human_query".into(),
            schema: FormSchema { fields: Vec::new() },
            title: None,
            description: None,
            consumer_kind: None,
        })
    }

    /// Suspend, resolve, resume, complete: one record the whole way,
    /// the answer ends the wait for the bridge, and the resume clears the
    /// suspension.
    #[test]
    fn lifecycle_one_record_per_frames() {
        let token = "tok-1".to_string();
        let events = vec![
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(42)),
            completed("src", vec![], 0),
            started("a", vec![], 0),
            RunEvent::NodeSuspended { run_id: run_id(), node_id: "a".into(), frames: vec![], token: token.clone(), at_unix: 0 },
            RunEvent::SuspensionRegistered {
                run_id: run_id(),
                node_id: "a".into(),
                frames: vec![],
                token: token.clone(),
                spec: make_spec(),
                call_index: 0,
                at_unix: 0,
            },
            RunEvent::SuspensionResolved { run_id: run_id(), token: token.clone(), value: json!("approved"), at_unix: 0 },
            RunEvent::NodeResumed { run_id: run_id(), node_id: "a".into(), frames: vec![], token: Some(token.clone()), at_unix: 0 },
            completed("a", vec![], 0),
        ];
        let mut fold = Fold::new(run_id(), fan_out_project());
        let mut ended = Vec::new();
        for ev in &events {
            ended.extend(fold.apply(ev).wait_ended);
        }
        let snap = fold.into_snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let firings = &snap.firings["a"];
        assert_eq!(firings.len(), 1, "one record per (node, frames)");
        assert_eq!(firings[0].status(), NodeFiringStatus::Completed);
        assert!(snap.suspensions.is_empty(), "suspensions cleared after resume+complete");
        assert!(snap.pending_deliveries.is_empty());
        assert_eq!(ended, vec![EndedWait { at: FiringLocation::new("a", vec![]), token, end: AwaitEnd::Answered { value: json!("approved") }, answer_kept: None }], "the answer ends the wait, once");
        let seq = &snap.awaited_sequences[&FiringLocation::new("a", vec![])];
        assert!(matches!(&seq[0].kind, AwaitedEntryKind::Await { ended: Some(AwaitEnd::Answered { value }), .. } if value == &json!("approved")));
    }

    /// A resume absorbs the pulses that arrived while the node waited,
    /// so a later refold never re-fires a completed node over them.
    #[test]
    fn node_resumed_absorbs_pulses() {
        let events = vec![
            started("a", vec![], 0),
            RunEvent::NodeSuspended { run_id: run_id(), node_id: "a".into(), frames: vec![], token: "t".into(), at_unix: 0 },
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(1)),
            RunEvent::NodeResumed { run_id: run_id(), node_id: "a".into(), frames: vec![], token: None, at_unix: 0 },
        ];
        let snap = fold_to_snapshot(run_id(), fan_out_project(), &events);
        assert!(snap.pulses["a"].is_empty());
        let a = only_absorbed(&snap, "a");
        assert_eq!(snap.firings["a"][0].pulses_absorbed, vec![a.id]);
        assert_eq!(snap.firings["a"][0].status(), NodeFiringStatus::Running);
    }

    /// A node fires once per frames: a second `NodeStarted` where the
    /// firing already ended is corruption, opens nothing, and sweeps no
    /// second closure downstream.
    #[test]
    fn node_started_after_terminal_is_corruption() {
        let events = vec![
            started("src", vec![], 0),
            completed("src", vec![], 1),
            started("a", vec![], 2),
            completed("a", vec![], 3),
            started("src", vec![], 4),
            completed("src", vec![], 5),
        ];
        let snap = fold_to_snapshot(run_id(), fan_out_project(), &events);
        let sites: Vec<CorruptionSite> = snap.corruptions.iter().map(|c| c.site).collect();
        assert_eq!(sites, vec![CorruptionSite::NodeLifecycle], "{:?}", snap.corruptions);
        assert!(snap.corruptions[0].reason.contains("already ended"), "{:?}", snap.corruptions[0]);
        assert_eq!(snap.firings["src"].len(), 1, "the second start opened nothing");
        assert_eq!(snap.firings["src"][0].status(), NodeFiringStatus::Completed);
        assert!(snap.pulses["a"].is_empty(), "a's start took the closure");
        assert!(only_absorbed(&snap, "a").closed, "one closure, from the one firing");
    }

    /// A stream take removes the pulse from the table (no tombstone),
    /// and the stream's end is durable on the loop instance.
    #[test]
    fn output_review_keeps_every_stream_item_and_closure_without_a_consumer() {
        let project = project(vec![node("gen", "T", &[], &[("rows", "Generator[Number]")], &[], Value::Null, Value::Null)], vec![]);
        let mut fold = Fold::new(run_id(), project).with_output_history();
        for event in [
            started_run(), started("gen", vec![], 1),
            emitted(Uuid::new_v4(), "gen", vec![], "rows", json!(10)),
            emitted(Uuid::new_v4(), "gen", vec![], "rows", json!(20)),
            RunEvent::PortClosed { run_id: run_id(), emission_id: Uuid::new_v4(), node_id: "gen".into(), frames: vec![], port: "rows".into(), at_unix: 2 },
            completed("gen", vec![], 3),
        ] { assert!(!fold.apply(&event).rejected()); }
        let wires = fold.output_wires().unwrap();
        assert_eq!(wires.len(), 3);
        assert_eq!(wires.iter().map(|wire| wire.ordinal).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(wires.iter().map(|wire| wire.value.clone()).collect::<Vec<_>>(), vec![json!(10), json!(20), Value::Null]);
        assert_eq!(wires.iter().map(|wire| wire.closed).collect::<Vec<_>>(), vec![false, false, true]);
    }

    #[test]
    fn pulses_consumed_removes_and_loop_stream_end_is_durable() {
        let project = project(
            vec![
                node("gen", "T", &[], &[("rows", "Generator[Number]")], &[], Value::Null, Value::Null),
                node("take", "T", &[("rows", "Generator[Number]", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![edge("gen", "rows", "take", "rows")],
        );
        let e1 = Uuid::new_v4();
        let e2 = Uuid::new_v4();
        let item1 = weft_core::run::emission::pulse_id(e1, "gen.rows->take.rows", false);
        let events = vec![
            started("gen", vec![], 0),
            emitted(e1, "gen", vec![], "rows", json!(1)),
            started("take", vec![], 0),
            emitted(e2, "gen", vec![], "rows", json!(2)),
            RunEvent::PulsesConsumed {
                run_id: run_id(),
                node_id: "take".into(),
                frames: vec![],
                pulse_ids: vec![item1.to_string()],
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(run_id(), project, &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let bucket = &snap.pulses["take"];
        assert_eq!(bucket.len(), 1, "the taken item is gone, the second item stays");
        assert_eq!(*bucket[0].value, json!(2));
        assert_eq!(bucket[0].status, PulseStatus::Pending, "a running consumer's items are not absorbed by its start");
        assert!(snap.firings["take"][0].pulses_absorbed.is_empty());
    }

    #[test]
    fn suspension_resolved_before_registered_still_resolves() {
        let events = [
            RunEvent::SuspensionResolved { run_id: run_id(), token: "t".into(), value: json!(5), at_unix: 0 },
            RunEvent::SuspensionRegistered {
                run_id: run_id(),
                node_id: "a".into(),
                frames: vec![],
                token: "t".into(),
                spec: make_spec(),
                call_index: 0,
                at_unix: 0,
            },
        ];
        let mut fold = Fold::new(run_id(), fan_out_project());
        let ended: Vec<Option<EndedWait>> = events.iter().map(|ev| fold.apply(ev).wait_ended).collect();
        assert_eq!(ended, vec![None, Some(EndedWait { at: FiringLocation::new("a", vec![]), token: "t".into(), end: AwaitEnd::Answered { value: json!(5) }, answer_kept: None })],
            "an answer on record before its wait ends the wait at the registration");
        let snap = fold.into_snapshot();
        let seq = &snap.awaited_sequences[&FiringLocation::new("a", vec![])];
        assert!(matches!(&seq[0].kind, AwaitedEntryKind::Await { ended: Some(AwaitEnd::Answered { value }), .. } if value == &json!(5)));
        assert!(snap.pending_deliveries.is_empty(), "the registration took the answer");
    }

    /// A wait given up stays given up: the answer that comes after it is
    /// ignored, so a replay fails the call the way it failed live.
    #[test]
    fn an_answer_after_a_wait_was_given_up_is_ignored() {
        let events = [
            RunEvent::SuspensionRegistered {
                run_id: run_id(),
                node_id: "a".into(),
                frames: vec![],
                token: "t".into(),
                spec: make_spec(),
                call_index: 0,
                at_unix: 0,
            },
            RunEvent::SuspensionGaveUp { run_id: run_id(), token: "t".into(), error: "gave up".into(), at_unix: 0 },
            RunEvent::SuspensionResolved { run_id: run_id(), token: "t".into(), value: json!(5), at_unix: 0 },
        ];
        let mut fold = Fold::new(run_id(), fan_out_project());
        let ended: Vec<Option<EndedWait>> = events.iter().map(|ev| fold.apply(ev).wait_ended).collect();
        assert_eq!(ended, vec![None, Some(EndedWait { at: FiringLocation::new("a", vec![]), token: "t".into(), end: AwaitEnd::GaveUp { error: "gave up".into() }, answer_kept: None }), None],
            "the give-up ends the wait; the late answer ends nothing");
        let snap = fold.into_snapshot();
        let seq = &snap.awaited_sequences[&FiringLocation::new("a", vec![])];
        assert!(matches!(&seq[0].kind, AwaitedEntryKind::Await { ended: Some(AwaitEnd::GaveUp { error }), .. } if error == "gave up"));
        assert!(snap.pending_deliveries.is_empty(), "nothing is left to deliver");
    }


    /// A run of a cut folds over the cut: a wire the cut left out carries
    /// nothing, so what is outside the run receives no pulse.
    #[test]
    fn a_cut_keeps_the_fold_off_the_outside() {
        let project = fan_out_project();
        let spec = weft_core::run_spec::RunSpec { target: vec!["a".into()], ..weft_core::run_spec::RunSpec::whole("x") };
        let cut = weft_core::run_spec::resolve_spec(&spec, &project, &Default::default()).unwrap().program;
        assert!(cut.nodes.iter().all(|node| node.id == "src" || node.id == "a"));
        let events = vec![
            started_run(),
            kicked("src"),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(9)),
            completed("src", vec![], 1),
        ];
        let snap = fold_to_snapshot(run_id(), Arc::new(cut), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(pending(&snap, "a").len(), 1);
        assert!(!snap.pulses.contains_key("b") && !snap.pulses.contains_key("c"), "no pulse crosses the cut");
    }

    /// Every rejection site names its row kind, and a rejected row
    /// applies nothing.
    #[test]
    fn every_corruption_site_is_reported_by_its_row() {
        let project = loop_project(false);
        let lp = |kind: &str| -> RunEvent {
            match kind {
                "launched" => loop_row_launched(0),
                "out" => loop_row_out_fired(0),
                "ended" => RunEvent::LoopStreamEnded {
                    run_id: run_id(),
                    group_id: "lp".into(),
                    parent_frames: vec![],
                    end: weft_core::generator::StreamEnd::Finished,
                    at_unix: 0,
                },
                _ => RunEvent::LoopTerminated {
                    run_id: run_id(),
                    group_id: "lp".into(),
                    parent_frames: vec![],
                    reason: LoopTerminationReason::OverExhausted,
                    at_unix: 0,
                },
            }
        };
        let events = vec![
            // A resume, and a suspension, of a firing that never opened.
            RunEvent::NodeResumed { run_id: run_id(), node_id: "add".into(), frames: vec![], token: None, at_unix: 0 },
            RunEvent::NodeSuspended { run_id: run_id(), node_id: "add".into(), frames: vec![], token: "t".into(), at_unix: 0 },
            started("feed", vec![], 0),
            // A second start over a firing still open.
            started("feed", vec![], 0),
            // A close on an undeclared port.
            RunEvent::PortClosed { run_id: run_id(), emission_id: Uuid::new_v4(), node_id: "feed".into(), frames: vec![], port: "nope".into(), at_unix: 0 },
            // A take naming a pulse the table does not hold.
            RunEvent::PulsesConsumed { run_id: run_id(), node_id: "feed".into(), frames: vec![], pulse_ids: vec![Uuid::nil().to_string()], at_unix: 0 },
            // Loop rows with no LoopIn firing / no instance.
            loop_row_instantiated(),
            lp("launched"),
            lp("out"),
            lp("ended"),
            lp("terminated"),
        ];
        let snap = fold_to_snapshot(run_id(), project, &events);
        let sites: Vec<CorruptionSite> = snap.corruptions.iter().map(|c| c.site).collect();
        assert_eq!(
            sites,
            vec![
                CorruptionSite::NodeLifecycle,
                CorruptionSite::NodeLifecycle,
                CorruptionSite::NodeLifecycle,
                CorruptionSite::PortClosed,
                CorruptionSite::PulsesConsumed,
                CorruptionSite::LoopInstantiated,
                CorruptionSite::LoopIterationLaunched,
                CorruptionSite::LoopOutFired,
                CorruptionSite::LoopStreamEnded,
                CorruptionSite::LoopTerminated,
            ],
            "{:?}",
            snap.corruptions
        );
        assert_eq!(snap.firings["feed"].len(), 1, "the duplicate start opened nothing");
        assert!(snap.corruptions[0].reason.contains("node_resumed"), "{:?}", snap.corruptions[0]);
        assert!(snap.corruptions[1].reason.contains("node_suspended"), "{:?}", snap.corruptions[1]);
        assert!(snap.corruptions[2].reason.contains("already open"), "{:?}", snap.corruptions[2]);
        assert!(snap.loop_runtime.get(&lp_key()).is_none());
    }

    /// A group is no node: a row naming one is corruption, on every row
    /// kind, and opens nothing.
    #[test]
    fn a_row_naming_a_group_is_corruption() {
        let events = vec![
            started_run(),
            started("g", vec![], 0),
            emitted(Uuid::new_v4(), "g", vec![], "x", json!(1)),
            completed("g", vec![], 1),
        ];
        let snap = fold_to_snapshot(run_id(), nested_group_project(), &events);
        let sites: Vec<CorruptionSite> = snap.corruptions.iter().map(|c| c.site).collect();
        assert_eq!(sites, vec![CorruptionSite::NodeLifecycle, CorruptionSite::PortEmitted, CorruptionSite::NodeLifecycle], "{:?}", snap.corruptions);
        assert!(!snap.firings.contains_key("g"), "no record opened for the group");
        assert!(!snap.pulses.contains_key("inner"), "nothing forwarded");
    }

    /// A value handed to a node that nothing feeds (a kick starts it) is
    /// named where the run keeps it, its birth, never written out.
    #[test]
    fn a_handed_input_reached_by_a_kick_is_named_at_the_birth() {
        let project = project(vec![node("a", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null)], vec![]);
        let mut definition = project.as_ref().clone();
        let mut given = weft_core::project::Given::default();
        given.inputs.insert(Located::top("a"), [("in".to_string(), json!(7))].into());
        let birth = started_given(&mut definition, given);
        let kick = RunEvent::NodeKicked { run_id: run_id(), node_id: "a".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 };
        let mut fold = Fold::new(run_id(), Arc::new(definition));
        for (seq, row) in [birth, kick, started("a", vec![], 1)].iter().enumerate() {
            assert!(!fold.apply_at(row, Some(Position { seq: seq as i32, index: 0 })).rejected());
        }
        let view = fold.firing_view("a", &vec![]).unwrap();
        assert_eq!(view.input["in"].at, ValueRef::Given { run: run_id(), place: Located::top("a"), port: "in".into(), side: GivenSide::Input });
    }

    /// A reused result stands in for itself in the child's cut: the child
    /// imports its record as the run it came from's, and what it sends
    /// reaches what runs again marked with that run.
    #[test]
    fn a_reused_result_stands_in_and_its_record_is_imported() {
        let project = project(
            vec![
                node("a", "T", &[("in", "Number", true)], &[("out", "Number")], &[], Value::Null, Value::Null),
                node("b", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
            ], vec![edge("a", "out", "b", "in")],
        );
        let mut original_definition = project.as_ref().clone();
        let mut original_given = weft_core::project::Given::default();
        original_given.inputs.insert(Located::top("a"), [("in".to_string(), json!(7))].into());
        let original_birth = started_given(&mut original_definition, original_given);
        let mut original = Fold::new(run_id(), Arc::new(original_definition)).with_output_history();
        let kick = RunEvent::NodeKicked { run_id: run_id(), node_id: "a".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 };
        for row in [original_birth, kick, started("a", vec![], 1), emitted(Uuid::new_v4(), "a", vec![], "out", json!(9)), completed("a", vec![], 2)] {
            assert!(!original.apply(&row).rejected());
        }
        let mut child_definition = project.as_ref().clone();
        let mut child_given = weft_core::project::Given::default();
        child_given.outputs.insert(Located::top("a"), weft_core::project::StandIn {
            ports: [("out".to_string(), GivenPort::Value { value: json!(9) })].into(),
            origin: GivenOrigin::Inherited { run: original.run_id() },
        });
        let child_birth = started_given(&mut child_definition, child_given);
        let mut child = Fold::new(Uuid::new_v4(), Arc::new(child_definition));
        child.apply(&child_birth);
        child.inherit(&original, &BTreeSet::from([Located::top("a")])).unwrap();
        child.settle(3);
        let history = child.firing_view("a", &vec![]).unwrap();
        assert_eq!(shown(&history.input)["in"], json!(7));
        assert_eq!(child.snapshot().firings["a"][0].inherited_from, Some(original.run_id()));
        assert!(!child.snapshot().pulses.contains_key("a"));
        assert!(child.snapshot().kicked.is_empty(), "a stand-in is never kicked");
        assert_eq!(*pending(child.snapshot(), "b")[0].value, json!(9));
        child.apply(&started("b", vec![], 4));
        let view = child.firing_view("b", &vec![]).unwrap();
        assert_eq!(shown(&view.input)["in"], json!(9));
        assert_eq!(view.inherited_ports.get("in"), Some(&original.run_id()));
    }

    /// A node run again replays the answers the seed run's firing was
    /// given, up to its first wait nobody answered.
    #[test]
    fn a_node_run_again_takes_the_answers_its_seed_gave() {
        let project = fan_out_project();
        let mut original = Fold::new(run_id(), project.clone());
        for row in [
            started_run(), kicked("src"), started("src", vec![], 0),
            RunEvent::SuspensionRegistered { run_id: run_id(), node_id: "src".into(), frames: vec![], token: "t0".into(), spec: make_spec(), call_index: 0, at_unix: 1 },
            RunEvent::SuspensionResolved { run_id: run_id(), token: "t0".into(), value: json!("yes"), at_unix: 2 },
            RunEvent::SuspensionRegistered { run_id: run_id(), node_id: "src".into(), frames: vec![], token: "t1".into(), spec: make_spec(), call_index: 1, at_unix: 3 },
        ] { assert!(!original.apply(&row).rejected()); }
        let mut child = Fold::new(Uuid::new_v4(), project);
        child.apply(&started_run());
        child.take_answers(&original, &BTreeSet::from([Located::top("src")]));
        let replayed = &child.snapshot().awaited_sequences[&FiringLocation::new("src", vec![])];
        assert_eq!(replayed.len(), 1, "the unanswered wait is asked again");
        assert!(matches!(&replayed[0].kind, AwaitedEntryKind::Await { ended: Some(AwaitEnd::Answered { value }), .. } if *value == json!("yes")));
    }

    /// A LoopOut row is a vote only once its writes landed: the same
    /// row again (a firing the runtime already holds) records nothing
    /// and carries no vote for the screen.
    #[test]
    fn a_loop_out_row_votes_once_its_writes_landed() {
        let mut events = loop_prefix();
        events.extend(vec![
            started("add", vec![frame(0)], 2),
            emitted(Uuid::new_v4(), "add", vec![frame(0)], "acc", json!(11)),
            emitted(Uuid::new_v4(), "add", vec![frame(0)], "res", json!(100)),
            completed("add", vec![frame(0)], 2),
            started("lp__out", vec![frame(0)], 3),
        ]);
        let mut fold = Fold::new(run_id(), loop_project(false));
        for e in &events {
            fold.apply(e);
        }
        let first = fold.apply(&loop_row_out_fired(0));
        assert_eq!(first.loop_out_done, Some(None), "the body cast no vote, and that is the row's effect");
        let again = fold.apply(&loop_row_out_fired(0));
        assert_eq!(again.loop_out_done, None, "a firing already held records nothing");
        assert!(fold.snapshot().corruptions.is_empty(), "{:?}", fold.snapshot().corruptions);
        assert_eq!(fold.snapshot().loop_runtime.get(&lp_key()).unwrap().out_fired, weft_core::run::loop_runtime::Indices::from([0]));
    }

    /// The dispatcher's cancel row ends the firing and sweeps what it
    /// never mentioned, like any other terminal.
    #[test]
    fn a_cancel_row_sweeps_the_unmentioned_ports() {
        let events = vec![
            started("src", vec![], 0),
            RunEvent::NodeCancelled { run_id: run_id(), node_id: "src".into(), frames: vec![], reason: "stopped".into(), at_unix: 1 },
        ];
        let snap = fold_to_snapshot(run_id(), fan_out_project(), &events);
        assert_eq!(snap.firings["src"][0].status(), NodeFiringStatus::Cancelled);
        for node in ["a", "b", "c"] {
            let got = pending(&snap, node);
            assert_eq!(got.len(), 1, "{node}");
            assert!(got[0].closed, "{node}");
        }
    }

    /// What the screen reads: a firing's input bag, its merged output,
    /// and the run's outputs per node.
    #[test]
    fn display_accessors_read_the_bag_the_output_and_the_run_outputs() {
        let project = project(
            vec![
                node("src", "T", &[], &[("out", "Number"), ("other", "Number")], &[], Value::Null, Value::Null),
                node("a", "T", &[("in", "Number", true), ("also", "Number", false)], &[("res", "Number")], &[], Value::Null, Value::Null),
            ],
            vec![edge("src", "out", "a", "in"), edge("src", "other", "a", "also")],
        );
        let e1 = Uuid::new_v4();
        let e2 = Uuid::new_v4();
        let events = vec![
            started("src", vec![], 0),
            emitted(e1, "src", vec![], "out", json!(1)),
            emitted(e2, "src", vec![], "other", json!(2)),
            completed("src", vec![], 1),
            started("a", vec![], 2),
            completed("a", vec![], 3),
        ];
        let mut fold = Fold::new(run_id(), project);
        for e in &events {
            fold.apply(e);
        }
        assert!(fold.snapshot().corruptions.is_empty(), "{:?}", fold.snapshot().corruptions);
        assert_eq!(fold.output_of("src", &vec![]).map(|output| shown(&output)), Some(json!({ "out": 1, "other": 2 })), "two emissions merge into one output");
        assert_eq!(fold.output_of("a", &vec![]).map(|output| shown(&output)), None, "a firing that emitted nothing has no output");
        let view = fold.firing_view("a", &vec![]).expect("a fired");
        assert_eq!(shown(&view.input), json!({ "in": 1, "also": 2 }));
        assert!(view.closed_ports.is_empty());
        assert_eq!(shown_outputs(&fold.final_outputs()), json!({ "src": { "out": 1, "other": 2 } }));
    }

    fn charge_opened(service: &str, charge: &str, node: &str) -> RunEvent {
        RunEvent::ChargeOpened {
            run_id: run_id(),
            node_id: node.into(),
            frames: vec![frame(1)],
            service: service.into(),
            charge: charge.into(),
            scratch: json!({ "route": "jobs" }),
            origin: weft_core::CredentialOwner::Author,
            at_unix: 1,
        }
    }

    fn cost(service: &str, charge: Option<&str>) -> RunEvent {
        RunEvent::CostReported {
            run_id: run_id(),
            node_id: "n".into(),
            frames: vec![],
            cost_id: Uuid::now_v7().to_string(),
            service: service.into(),
            model: None,
            amount_usd: Some(1.0),
            billed: false,
            origin: weft_core::CredentialOwner::Author,
            metadata: json!({}),
            charge: charge.map(str::to_string),
            at_unix: 2,
        }
    }

    /// A charge is open from its opening until a cost names it: a cost
    /// priced on its own response closes nothing, the same id under
    /// another service is another charge, and what stays open keeps the
    /// node, iteration and meter state it was opened with.
    #[test]
    fn the_charge_book_holds_what_was_opened_and_not_yet_closed() {
        let record = vec![
            started_run(),
            charge_opened("jobs", "job-1", "dub"),
            charge_opened("jobs", "job-2", "dub"),
            charge_opened("other", "job-1", "voice"),
            cost("jobs", None),
            cost("jobs", Some("job-1")),
        ];
        let open: Vec<_> = ChargeBook::of_record(&record).into_open().collect();
        assert_eq!(open.len(), 2, "{open:?}");
        assert_eq!((open[0].service.as_str(), open[0].charge.as_str()), ("jobs", "job-2"));
        assert_eq!(open[1], OpenedCharge {
            node_id: "voice".into(),
            frames: vec![frame(1)],
            service: "other".into(),
            charge: "job-1".into(),
            scratch: json!({ "route": "jobs" }),
            origin: weft_core::CredentialOwner::Author,
        });

        let closed = [record, vec![cost("jobs", Some("job-2")), cost("other", Some("job-1"))]].concat();
        assert_eq!(ChargeBook::of_record(&closed).into_open().count(), 0, "every charge was closed");
    }

    /// A run ending with no drive closes each charge its record leaves
    /// open with one unknown cost, booked where the charge was opened and
    /// naming why; a charge already priced gets nothing, and the record
    /// then holds no open charge.
    #[test]
    fn a_run_ending_with_no_drive_books_its_open_charges_unknown() {
        let mut opened_with_model = charge_opened("jobs", "job-2", "dub");
        if let RunEvent::ChargeOpened { scratch, .. } = &mut opened_with_model {
            *scratch = json!({ "model": "eleven" });
        }
        let mut opened_bare = charge_opened("other", "job-3", "voice");
        if let RunEvent::ChargeOpened { scratch, .. } = &mut opened_bare {
            *scratch = json!("pending");
        }
        let record = vec![started_run(), charge_opened("jobs", "job-1", "dub"), opened_with_model, opened_bare, cost("jobs", Some("job-1"))];
        let closing = unpriced_charges_closed(run_id(), ChargeBook::of_record(&record), "the run was cancelled", 7);
        assert_eq!(closing.len(), 2, "{closing:?}");
        let RunEvent::CostReported { node_id, frames, service, model, amount_usd, origin, metadata, charge, at_unix, .. } = &closing[0] else {
            panic!("a cost: {:?}", closing[0]);
        };
        assert_eq!((node_id.as_str(), service.as_str(), charge.as_deref()), ("dub", "jobs", Some("job-2")));
        assert_eq!((frames, model.as_deref(), *amount_usd, origin, *at_unix), (&vec![frame(1)], Some("eleven"), None, &weft_core::CredentialOwner::Author, 7));
        assert_eq!(
            metadata["resolution"],
            json!("the call spent, and no response ever stated what it cost (the run was cancelled)")
        );
        let RunEvent::CostReported { metadata, model, charge, .. } = &closing[1] else { panic!("a cost: {:?}", closing[1]) };
        assert_eq!((charge.as_deref(), model), (Some("job-3"), &None));
        assert_eq!(metadata["meterState"], json!("pending"), "a meter state that is not an object is kept under a key");

        let ended = [record, closing].concat();
        assert_eq!(ChargeBook::of_record(&ended).into_open().count(), 0, "every charge is closed");
        assert!(unpriced_charges_closed(run_id(), ChargeBook::of_record(&ended), "again", 8).is_empty());
    }

    /// A sequential loop over a stream whose body is two nodes, `fmt` then
    /// `tag`: the shape of a long research run.
    fn long_loop_project() -> Arc<ProjectDefinition> {
        project(
            vec![
                node("gen", "T", &[], &[("rows", "Generator[Number]")], &[], Value::Null, Value::Null),
                node(
                    "lp__in",
                    "LoopIn",
                    &[("rows", "Generator[Number]", true), ("_should_flow", "Boolean", false)],
                    &[("rows", "Number"), ("index", "Number")],
                    &[],
                    json!({ "groupId": "lp", "role": "In" }),
                    json!({ "parallel": false, "over": ["rows"], "carry": [] }),
                ),
                node("fmt", "T", &[("item", "Number", true)], &[("text", "String")], &["lp"], Value::Null, Value::Null),
                node("tag", "T", &[("text", "String", true)], &[("res", "String")], &["lp"], Value::Null, Value::Null),
                node(
                    "lp__out",
                    "LoopOut",
                    &[("res", "String", false), ("done", "Boolean", false)],
                    &[("res", "List[String]")],
                    &[],
                    json!({ "groupId": "lp", "role": "Out" }),
                    json!({ "parentId": null }),
                ),
                node("sink", "T", &[("res", "List[String]", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![
                edge("gen", "rows", "lp__in", "rows"),
                edge("lp__in", "rows", "fmt", "item"),
                edge("fmt", "text", "tag", "text"),
                edge("tag", "res", "lp__out", "res"),
                edge("lp__out", "res", "sink", "res"),
            ],
        )
    }

    /// The rows of a run of `long_loop_project` over `n` items: the
    /// stream hands out one item per pull, so each comes out just before
    /// the iteration that takes it, and the iterations run one after the
    /// other.
    fn long_loop_rows(n: u32) -> Vec<RunEvent> {
        let text = json!("x".repeat(64));
        let launched = |index: u32, item: Uuid| RunEvent::LoopIterationLaunched {
            run_id: run_id(), group_id: "lp".into(), parent_frames: vec![], index,
            stream_pulse: Some(item.to_string()), at_unix: 1,
        };
        let mut rows = vec![started_run(), started("gen", vec![], 0), started("lp__in", vec![], 1), loop_row_instantiated()];
        for i in 0..n {
            let emission = Uuid::new_v4();
            let item = weft_core::run::emission::pulse_id(emission, "gen.rows->lp__in.rows", false);
            rows.extend([
                emitted(emission, "gen", vec![], "rows", json!(i)),
                launched(i, item),
                started("fmt", vec![frame(i)], 2),
                emitted(Uuid::new_v4(), "fmt", vec![frame(i)], "text", text.clone()),
                completed("fmt", vec![frame(i)], 2),
                started("tag", vec![frame(i)], 3),
                emitted(Uuid::new_v4(), "tag", vec![frame(i)], "res", text.clone()),
                completed("tag", vec![frame(i)], 3),
                started("lp__out", vec![frame(i)], 4),
                loop_row_out_fired(i),
                completed("lp__out", vec![frame(i)], 4),
            ]);
        }
        rows.extend([
            completed("gen", vec![], 5),
            RunEvent::LoopStreamEnded { run_id: run_id(), group_id: "lp".into(), parent_frames: vec![], end: weft_core::generator::StreamEnd::Finished, at_unix: 5 },
            RunEvent::LoopTerminated { run_id: run_id(), group_id: "lp".into(), parent_frames: vec![], reason: LoopTerminationReason::OverExhausted, at_unix: 5 },
            completed("lp__in", vec![], 5),
        ]);
        rows
    }

    /// The fastest of three folds of `rows`, and the snapshot it left.
    fn time_fold(rows: &[RunEvent]) -> (std::time::Duration, RunSnapshot) {
        let mut best = None;
        let mut snap = None;
        for _ in 0..3 {
            let start = std::time::Instant::now();
            let folded = fold_to_snapshot(run_id(), long_loop_project(), rows);
            let took = start.elapsed();
            best = Some(best.map_or(took, |best: std::time::Duration| best.min(took)));
            snap = Some(folded);
        }
        (best.expect("three folds ran"), snap.expect("three folds ran"))
    }

    /// What one row costs the fold does not grow with how long the run
    /// already is: a run three times longer folds in about three times the
    /// time. Every per-row lookup (the firing a row is about, the pulses a
    /// start absorbs, the scopes a pass looks at) used to walk everything
    /// the run had done so far, so three times the rows took nine times
    /// the time. A ratio, so it holds on a slow machine as on a fast one.
    #[test]
    fn folding_a_run_costs_the_same_per_row_however_long_it_is() {
        let n = 1500;
        let short = long_loop_rows(n);
        let long = long_loop_rows(3 * n);
        let (short_took, snap) = time_fold(&short);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let (long_took, snap) = time_fold(&long);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let sink = pending(&snap, "sink");
        assert_eq!(sink.len(), 1, "the loop ended and handed its list on");
        assert_eq!(sink[0].value.as_array().map(Vec::len), Some(3 * n as usize));
        let ratio = long_took.as_secs_f64() / short_took.as_secs_f64();
        assert!(
            ratio < 5.0,
            "{} rows folded in {short_took:?}, {} rows in {long_took:?}: {ratio:.1} times the time for 3 times the rows",
            short.len(), long.len(),
        );
    }

    /// What a reader paints of one applied row: its effects, and the
    /// display read of what the row is about.
    fn painted_by(fold: &Fold, ev: &RunEvent, effects: &FoldEffects) -> Value {
        let emissions: Vec<Value> = effects
            .emissions
            .iter()
            .map(|e| json!([e.pulse.id, e.pulse.target_node, e.pulse.target_port, *e.pulse.value, e.pulse.closed, e.pulse.origin.as_deref().map(|o| serde_json::to_value(o).unwrap())]))
            .collect();
        let ended = effects.wait_ended.as_ref().map(|w| json!([w.at.node_id, w.token, serde_json::to_value(&w.end).unwrap(), w.answer_kept.as_deref().map(|o| serde_json::to_value(o).unwrap())]));
        let shown = |ports: &BTreeMap<String, Shown>| -> Value {
            ports.iter().map(|(port, v)| (port.clone(), json!([serde_json::to_value(&v.at).unwrap(), v.value.as_deref().cloned()]))).collect::<serde_json::Map<_, _>>().into()
        };
        let read = match ev {
            RunEvent::NodeStarted { node_id, frames, .. } | RunEvent::NodeSkipped { node_id, frames, .. } => fold.firing_view(node_id, frames).map(|view| {
                json!([shown(&view.input), view.closed_ports, view.provided_ports, view.inherited_ports])
            }),
            RunEvent::NodeCompleted { node_id, frames, .. } => Some(fold.output_of(node_id, frames).map_or(Value::Null, |o| shown(&o))),
            RunEvent::RunCompleted { .. } => {
                Some(fold.final_outputs().0.iter().map(|(place, ports)| (place.clone(), shown(ports))).collect::<serde_json::Map<_, _>>().into())
            }
            _ => None,
        };
        json!({
            "emissions": emissions,
            "scopes": effects.scopes.iter().map(|(copy, state)| json!([copy.scope, copy.frames, state])).collect::<Vec<_>>(),
            "ended": ended,
            "vote": effects.loop_out_done,
            "rejected": effects.rejections.len(),
            "read": read,
        })
    }

    /// Apply `rows`, each at its place in the record, to a fold that keeps
    /// every value and to one that SHOWS the run alike: every row paints
    /// the same from both. Answers the showing fold.
    fn shows_like_full(project: Arc<ProjectDefinition>, rows: &[RunEvent]) -> Fold {
        let mut full = Fold::new(run_id(), project.clone());
        let mut showing = Fold::new(run_id(), project).showing();
        for (seq, ev) in rows.iter().enumerate() {
            let at = Some(Position { seq: seq as i32, index: 0 });
            let kept = full.apply_at(ev, at);
            let shown = showing.apply_at(ev, at);
            assert_eq!(painted_by(&showing, ev, &shown), painted_by(&full, ev, &kept), "row {seq}: {}", describe(ev));
        }
        assert!(showing.snapshot().corruptions.is_empty(), "{:?}", showing.snapshot().corruptions);
        showing
    }

    /// Where the record keeps value `seq` of the rows a test applies.
    fn kept_at(seq: usize) -> ValueRef {
        ValueRef::Row { run: run_id(), seq: seq as i32, index: 0 }
    }

    /// A fold that shows a run paints every row exactly as one that keeps
    /// every value, over a loop fed by a stream, a fan-out, a wait
    /// answered after it was registered and one before, and a failure;
    /// and once each firing ended it holds none of its values, but each
    /// place's last output, which the run's outputs read.
    #[test]
    fn a_showing_fold_paints_what_a_full_fold_paints_and_lets_ended_values_go() {
        let mut rows = long_loop_rows(5);
        rows.push(started("sink", vec![], 6));
        rows.push(completed("sink", vec![], 6));
        // What a row ended goes as the next row comes.
        rows.push(RunEvent::RunTagged { run_id: run_id(), tags: vec!["done".into()], at_unix: 7 });
        let showing = shows_like_full(long_loop_project(), &rows);
        let snap = showing.snapshot();
        // A loop's `LoopIn` at the top stays, one per loop: its loop may
        // end it again.
        assert_eq!(snap.firings.keys().collect::<Vec<_>>(), vec!["lp__in"], "every other firing ended, and is forgotten");
        assert_eq!(snap.pulses.all_absorbed().count(), 0, "a taken pulse is forgotten");
        assert!(snap.scopes.iter().all(|(copy, _)| copy.frames.is_empty()), "no iteration's copy is left: {:?}", snap.scopes.iter().collect::<Vec<_>>());
        assert!(snap.kicked.keys().all(|at| at.frames.is_empty()), "no iteration's kick is left");
        assert!(snap.loop_runtime.iter().all(|(_, inst)| inst.gather_lists.is_empty() && inst.carry_values.is_empty()), "an ended loop keeps no value");
        assert!(showing.outputs.len() <= 3, "one output kept per place: {}", showing.outputs.len());
        assert_eq!(snap.forgetting.as_ref().map(|forgetting| forgetting.holds()), Some(3), "where the two firings outside the loop ended, and the loop's one run of iterations");

        let token = "t".to_string();
        let registered = |token: &str| RunEvent::SuspensionRegistered {
            run_id: run_id(), node_id: "a".into(), frames: vec![], token: token.into(), spec: make_spec(), call_index: 0, at_unix: 0,
        };
        let waits = vec![
            started_run(),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(42)),
            completed("src", vec![], 0),
            started("a", vec![], 0),
            RunEvent::NodeSuspended { run_id: run_id(), node_id: "a".into(), frames: vec![], token: token.clone(), at_unix: 0 },
            registered(&token),
            RunEvent::SuspensionResolved { run_id: run_id(), token: token.clone(), value: json!("approved"), at_unix: 0 },
            RunEvent::NodeResumed { run_id: run_id(), node_id: "a".into(), frames: vec![], token: Some(token), at_unix: 0 },
            RunEvent::SuspensionResolved { run_id: run_id(), token: "early".into(), value: json!(5), at_unix: 0 },
            registered("early"),
            RunEvent::NodeFailed { run_id: run_id(), node_id: "a".into(), frames: vec![], error: "boom".into(), gave_up_wait: false, at_unix: 1 },
            started("b", vec![], 2),
            completed("b", vec![], 2),
            RunEvent::RunCompleted { run_id: run_id(), at_unix: 3 },
        ];
        shows_like_full(fan_out_project(), &waits);
    }

    /// What a fold that shows a run holds halfway through a long loop does
    /// not depend on how far the loop got: ten iterations in or four
    /// hundred, it holds the same firings, copies, kicks and pulses, and
    /// its loop's iterations are one run of indices.
    #[test]
    fn a_showing_fold_holds_the_same_however_far_a_loop_got() {
        let held = |n: u32| {
            let mut rows = long_loop_rows(n);
            // Cut before the loop ends, and let one more row come.
            let end = rows.iter().rposition(|ev| matches!(ev, RunEvent::NodeCompleted { node_id, .. } if node_id == "lp__out")).unwrap();
            rows.truncate(end + 1);
            rows.push(RunEvent::RunTagged { run_id: run_id(), tags: vec!["halfway".into()], at_unix: 7 });
            let showing = shows_like_full(long_loop_project(), &rows);
            let snap = showing.snapshot();
            let inst = snap.loop_runtime.iter().next().map(|(_, inst)| inst).expect("the loop runs");
            assert_eq!((inst.launched.len(), inst.out_fired.len()), (n as usize, n as usize));
            (
                snap.firings.values().map(|records| records.len()).sum::<usize>(),
                snap.pulses.values().map(Vec::len).sum::<usize>() + snap.pulses.all_absorbed().count(),
                snap.scopes.iter().count(),
                snap.kicked.len(),
                snap.loop_runtime.iter().count(),
                showing.outputs.len() + showing.output_kept.len() + snap.forgetting.as_ref().map_or(0, |forgetting| forgetting.holds()),
                format!("{:?}", inst.launched),
            )
        };
        let short = held(10);
        let long = held(400);
        assert_eq!((short.0, short.1, short.2, short.3, short.4, short.5), (long.0, long.1, long.2, long.3, long.4, long.5));
        assert_eq!((short.6.as_str(), long.6.as_str()), ("{0..=9}", "{0..=399}"));
    }

    /// A fold that forgets ended firings still refuses a second start
    /// where one ended: outside the loop by the place's mark, inside it by
    /// the iteration's index, once the iteration is forgotten; it holds
    /// the same either way however long the run.
    #[test]
    fn a_forgotten_firing_still_refuses_a_second_start() {
        let restarted = |again: RunEvent| {
            let mut rows = long_loop_rows(3);
            rows.push(RunEvent::RunTagged { run_id: run_id(), tags: vec!["done".into()], at_unix: 7 });
            let mut fold = Fold::new(run_id(), long_loop_project()).showing();
            for (seq, ev) in rows.iter().enumerate() {
                fold.apply_at(ev, Some(Position { seq: seq as i32, index: 0 }));
            }
            assert!(fold.snapshot().corruptions.is_empty(), "{:?}", fold.snapshot().corruptions);
            assert_eq!(fold.snapshot().firings.keys().collect::<Vec<_>>(), vec!["lp__in"], "every firing but the loop's `LoopIn` is forgotten");
            let refused = fold.apply_at(&again, Some(Position { seq: rows.len() as i32, index: 0 })).rejections;
            refused.into_iter().map(|corruption| corruption.reason).collect::<Vec<_>>()
        };
        for again in [started("gen", vec![], 9), started("fmt", vec![frame(1)], 9), started("lp__out", vec![frame(2)], 9)] {
            let refused = restarted(again.clone());
            assert_eq!(refused.len(), 1, "{again:?}: {refused:?}");
            assert!(refused[0].contains("a firing is already ended at these frames"), "{}", refused[0]);
        }
        assert!(restarted(started("fmt", vec![frame(3)], 9)).is_empty(), "an iteration that never ran is no place a firing ended");
    }

    /// A fold that drives a run keeps its earliest failure: one that ends
    /// earlier but is forgotten later takes the place of the one kept, and
    /// what the fold held of the one it replaces goes with it, so a late
    /// answer to that one's wait reads as an answer to any forgotten wait.
    #[test]
    fn a_driving_fold_keeps_the_earliest_failure_and_lets_the_other_go() {
        let failed = |node: &str, at_unix: u64| RunEvent::NodeFailed {
            run_id: run_id(), node_id: node.into(), frames: vec![], error: format!("{node} broke"), gave_up_wait: false, at_unix,
        };
        let rows = vec![
            started_run(),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(1)),
            completed("src", vec![], 0),
            started("a", vec![], 1),
            RunEvent::SuspensionRegistered { run_id: run_id(), node_id: "a".into(), frames: vec![], token: "t".into(), spec: make_spec(), call_index: 0, at_unix: 1 },
            failed("a", 9),
            started("b", vec![], 2),
            failed("b", 5),
            RunEvent::RunTagged { run_id: run_id(), tags: vec!["done".into()], at_unix: 10 },
            RunEvent::SuspensionResolved { run_id: run_id(), token: "t".into(), value: json!("late"), at_unix: 11 },
        ];
        let mut fold = Fold::new(run_id(), fan_out_project()).driving();
        for event in &rows {
            fold.apply(event);
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(snap.firings.keys().collect::<Vec<_>>(), vec!["b"], "the earlier failure is kept, the other forgotten");
        assert!(!fold.awaits.contains_key("t") && !snap.suspensions.contains_key("t"), "the replaced failure's wait went with it");
        // What is left of them is the output of `src`'s completed firing,
        // the place's last, which the run's outputs read.
        let src = fold.kept_output.get(&Located::top("src")).map(|&(_, id)| id).expect("src's output is kept");
        assert_eq!(fold.outputs.keys().collect::<Vec<_>>(), vec![&src], "nothing of the failed firings is held");
        assert_eq!(fold.output_kept.keys().collect::<Vec<_>>(), vec![&src]);
    }

    /// A seed's ancestor keeps whole every loop around a place handed down,
    /// and every loop inside one of those, however deep: whether the outer
    /// loop is reusable reads every iteration of the inner one.
    #[test]
    fn keeping_a_place_keeps_its_loops_and_the_loops_inside_them_whole() {
        let loop_in = |id: &str, group: &str, scope: &[&str]| node(
            id, "LoopIn", &[("rows", "List[Number]", true)], &[("rows", "Number")], scope,
            json!({ "groupId": group, "role": "In" }), json!({ "parallel": false, "over": ["rows"], "carry": [] }),
        );
        let loop_out = |id: &str, group: &str, scope: &[&str]| node(
            id, "LoopOut", &[("done", "Boolean", false)], &[], scope, json!({ "groupId": group, "role": "Out" }), json!({ "parentId": null }),
        );
        let program = project_with(
            vec![
                loop_in("o__in", "o", &[]),
                node("x", "T", &[("n", "Number", true)], &[], &["o"], Value::Null, Value::Null),
                loop_in("i__in", "i", &["o"]),
                node("y", "T", &[("n", "Number", true)], &[], &["o", "i"], Value::Null, Value::Null),
                loop_out("i__out", "i", &["o"]),
                loop_out("o__out", "o", &[]),
            ],
            vec![],
            vec![
                group("o", json!({ "kind": "loop", "loopConfig": {}, "nodeIds": ["x", "i__in", "i__out"] }), None, &[]),
                group("i", json!({ "kind": "loop", "loopConfig": {}, "nodeIds": ["y"] }), Some("o"), &[]),
            ],
        );
        let fold = Fold::new(run_id(), program).keeping(BTreeSet::from([Located::top("x")]));
        assert_eq!(fold.keeps.as_ref().map(|keeps| keeps.loops.clone()), Some(BTreeSet::from(["i".to_string(), "o".to_string()])));
    }

    /// A value is named where the record keeps it: a port's emission at
    /// its row, what a wire's key path reads as a part of it, a loop's
    /// item as an item of what fed the loop, a gathered list as a list of
    /// what each iteration wrote, a wait's answer at the row that brought
    /// it, even when it came before the wait was registered.
    #[test]
    fn a_value_is_named_where_the_record_keeps_it() {
        // A plain wire, and the answer to a wait that came first.
        let mut fold = Fold::new(run_id(), fan_out_project());
        let rows = [
            started_run(),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(7)),
            completed("src", vec![], 0),
            started("a", vec![], 1),
            RunEvent::SuspensionResolved { run_id: run_id(), token: "t".into(), value: json!("yes"), at_unix: 1 },
            RunEvent::SuspensionRegistered { run_id: run_id(), node_id: "a".into(), frames: vec![], token: "t".into(), spec: make_spec(), call_index: 0, at_unix: 1 },
        ];
        let mut ended = None;
        for (seq, ev) in rows.iter().enumerate() {
            ended = fold.apply_at(ev, Some(Position { seq: seq as i32, index: 0 })).wait_ended.or(ended);
        }
        let view = fold.firing_view("a", &vec![]).unwrap();
        assert_eq!(view.input["in"].at, kept_at(2));
        assert_eq!(fold.output_of("src", &vec![]).unwrap()["out"].at, kept_at(2));
        assert_eq!(ended.unwrap().answer_kept.as_deref(), Some(&kept_at(5)), "the answer is kept at its own row");

        // A key path, read off the emission.
        let mut paths = (*fan_out_project()).clone();
        paths.edges[0].path = vec!["n".into()];
        paths.nodes[0].outputs[0].port_type = serde_json::from_value(json!("{n: Number}")).unwrap();
        let mut fold = Fold::new(run_id(), Arc::new(paths));
        for (seq, ev) in [started_run(), started("src", vec![], 0), emitted(Uuid::new_v4(), "src", vec![], "out", json!({ "n": 3 })), completed("src", vec![], 0), started("a", vec![], 1)].iter().enumerate() {
            fold.apply_at(ev, Some(Position { seq: seq as i32, index: 0 }));
        }
        let view = fold.firing_view("a", &vec![]).unwrap();
        assert_eq!(view.input["in"].at, kept_at(2).part(vec![weft_core::record_ref::Step::Key("n".into())]));
        assert_eq!(view.input["in"].value.as_deref(), Some(&json!(3)));

        // A loop over a stream: each item at the row that put it on the
        // stream, the gathered list one item per iteration.
        let mut rows = long_loop_rows(3);
        rows.push(started("sink", vec![], 6));
        let mut fold = Fold::new(run_id(), long_loop_project());
        for (seq, ev) in rows.iter().enumerate() {
            fold.apply_at(ev, Some(Position { seq: seq as i32, index: 0 }));
        }
        let emitted_at = |node: &str, frames: &LoopFrames| rows.iter().position(|ev| matches!(ev, RunEvent::PortEmitted { node_id, frames: f, .. } if node_id == node && f == frames)).unwrap();
        let fmt = fold.firing_view("fmt", &vec![frame(1)]).unwrap();
        let second_item = rows.iter().enumerate().filter(|(_, ev)| matches!(ev, RunEvent::PortEmitted { node_id, .. } if node_id == "gen")).nth(1).unwrap().0;
        assert_eq!(fmt.input["item"].at, kept_at(second_item));
        let sink = fold.firing_view("sink", &vec![]).unwrap();
        let items: Vec<ValueRef> = (0..3).map(|i| kept_at(emitted_at("tag", &vec![frame(i)]))).collect();
        assert_eq!(sink.input["res"].at, ValueRef::List { items });
    }
}
