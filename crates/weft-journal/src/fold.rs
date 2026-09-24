//! The fold: journal rows plus the program, in, an execution's state
//! out. One row at a time, so a live reader (the dispatcher's journal
//! bridge) keeps a fold per open execution and applies each row as it
//! lands, and a resume (the engine, the cancel writers) folds the
//! whole log in one go through the same code.
//!
//! The journal records facts the engine learned from outside
//! (`crate::events`); the fold recomputes everything else with the
//! same functions the live engine ran: an emission fans out through
//! `postprocess_output`, a terminal sweeps its unmentioned ports
//! through `close_unmentioned_downstream`, a group boundary fires
//! through `boundary::fire_ready_passthroughs`, a loop launches and
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

use weft_core::exec::boundary::{
    is_passthrough, kick_scope, refused_scope, settle_table, tear_down_scope, BoundaryDispatch,
    BoundaryOutcome, RefusedScope,
};
use weft_core::exec::emission::{boundary_emission, loop_termination_emission, terminal_sweep_emission, PulseEmission};
use weft_core::exec::loop_runtime::{
    self, classify_loop_out, close_loop_outward, emit_loop_outward, instantiate, launch_iteration,
    LoopAdvance, LoopStreamItem,
};
use weft_core::exec::postprocess::{close_unmentioned_downstream, emit_port_closure, postprocess_output, OutputBag};
use weft_core::exec::ready::{
    effective_input_pulses, firing_input, generator_inputs, kicked_group, owned_bag, wired_inputs,
};
use weft_core::exec::skip::SkipReason;
use weft_core::exec::{
    latest_firing, latest_firing_mut, next_firing_ordinal, NodeExecution, NodeExecutionStatus,
};
use weft_core::frames::{FiringLocation, Located, LoopFrames};
use weft_core::primitive::{
    AwaitedEntry, AwaitedEntryKind, CorruptionSite, ExecutionSnapshot, JournalCorruption, KickedNode,
    LoopInstanceKey, LoopTerminationReason, SuspensionInfo,
};
use weft_core::project::{boundary_in_id, boundary_out_id, EdgeIndex, GroupBoundaryRole, NodeDefinition, ProjectDefinition};
use weft_core::pulse::PulseStatus;
use weft_core::Color;

use crate::events::ExecEvent;

/// What one applied row put in the world, beyond the snapshot: the
/// live bridge turns these into the events the editor paints.
#[derive(Default)]
pub struct FoldEffects {
    /// Every pulse this row put on a wire, with its provenance
    /// (emissions, closures, a loop's body pulses, a loop's outward
    /// values). Boundary firings' pulses are inside `boundaries`.
    pub emissions: Vec<PulseEmission>,
    /// The group boundaries this row made ready, fired in order.
    pub boundaries: Vec<BoundaryDispatch>,
    /// On a `NodeResumed` that resumed a suspension: the value the
    /// fire delivered (read before the delivery is cleared).
    pub resumed_value: Option<Value>,
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

/// One execution's state, folded row by row over its program.
pub struct Fold {
    project: Arc<ProjectDefinition>,
    edge_idx: EdgeIndex,
    /// The run's phase, from `ExecutionStarted`; `None` before that
    /// row.
    phase: Option<weft_core::context::Phase>,
    /// The run's node set, from `ExecutionStarted`; `None` before that
    /// row and for an untargeted run.
    dispatchable: Option<HashSet<Located>>,
    snap: ExecutionSnapshot,
    /// Per record, what it handed out: for the screen only.
    outputs: HashMap<Uuid, OutputBag>,
    /// Source values before wire projection, including outputs without a
    /// consumer in this run. Seed delivery projects these through child wires.
    output_history: Option<Vec<OutputEmission>>,
    output_ids: HashSet<(Uuid, String, String)>,
}

#[derive(Clone)]
struct OutputEmission {
    id: Uuid,
    node: String,
    frames: LoopFrames,
    port: String,
    value: Option<Arc<Value>>,
    error: Option<String>,
    provided: bool,
}

impl Fold {
    pub fn new(color: Color, project: Arc<ProjectDefinition>) -> Self {
        let edge_idx = EdgeIndex::build(&project);
        Self {
            project,
            edge_idx,
            phase: None,
            dispatchable: None,
            snap: ExecutionSnapshot {
                color,
                selection: None,
                program: None,
                inherited_origins: Default::default(),
                pulses: Default::default(),
                executions: Default::default(),
                suspensions: HashMap::new(),
                loop_runtime: loop_runtime::LoopRuntime::new(),
                kicked: HashMap::new(),
                pending_deliveries: HashMap::new(),
                awaited_sequences: HashMap::new(),
                corruptions: Vec::new(),
            },
            outputs: HashMap::new(),
            output_history: None,
            output_ids: HashSet::new(),
        }
    }

    pub fn color(&self) -> Color {
        self.snap.color
    }

    pub fn project(&self) -> &Arc<ProjectDefinition> {
        &self.project
    }

    pub fn snapshot(&self) -> &ExecutionSnapshot {
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
            .ok_or_else(|| anyhow::anyhow!("run {} was reconstructed without output history", self.color()))?;
        let mut ordinals = HashMap::new();
        Ok(history.iter().map(|output| {
            let ordinal = ordinals.entry((output.node.clone(), output.frames.clone(), output.port.clone())).or_insert(0u64);
            let wire = weft_core::run_spec::OutputWire {
                node: output.node.clone(), frames: output.frames.clone(), port: output.port.clone(),
                ordinal: *ordinal, closed: output.value.is_none(), error: output.error.clone(),
                value: output.value.as_ref().map(|value| value.as_ref().clone()).unwrap_or(Value::Null),
            };
            *ordinal += 1;
            wire
        }).collect())
    }

    /// Import already reconstructed results. Original inputs and outputs are
    /// history; only emissions into executable child nodes become new pulses.
    pub fn inherit(&mut self, source: &Fold, places: &BTreeSet<Located>) -> anyhow::Result<FoldEffects> {
        anyhow::ensure!(source.snap.corruptions.is_empty(), "seed {} contains corrupt journal rows", source.color());
        let history = source.output_history.as_ref()
            .ok_or_else(|| anyhow::anyhow!("seed {} was reconstructed without its output history", source.color()))?;
        let reusable = weft_core::seeding::inheritable_nodes(&source.project, &source.snap);
        // The records at one place: the node's firings under that
        // place's calls, at every loop iteration.
        let records_at = |snap: &ExecutionSnapshot, place: &Located| -> Vec<weft_core::exec::NodeExecution> {
            snap.executions.get(&place.id).into_iter().flatten()
                .filter(|record| Located::at(&place.id, &record.frames) == *place).cloned().collect()
        };
        for place in places {
            anyhow::ensure!(records_at(&self.snap, place).is_empty(), "history for '{place}' was already imported");
            anyhow::ensure!(source.project.nodes.iter().any(|definition| definition.id == place.id)
                && self.project.nodes.iter().any(|definition| definition.id == place.id), "seed node '{place}' is not in both programs");
            let records = records_at(&source.snap, place);
            anyhow::ensure!(reusable.contains(place)
                || (records.is_empty() && history.iter().any(|output| Located::at(&output.node, &output.frames) == *place && output.provided)),
                "seed {} has no complete reusable result for '{place}'", source.color());
            anyhow::ensure!(!source.snap.inherited_origins.contains_key(place),
                "seed result '{place}' must name its original run, not intermediate run {}", source.color());
            anyhow::ensure!(records.iter().all(|record| matches!(record.status, NodeExecutionStatus::Completed | NodeExecutionStatus::Skipped)),
                "cannot inherit unfinished or unsuccessful result '{place}' from {}", source.color());
            anyhow::ensure!(records.iter().all(|record| record.completed_at.is_some()), "seed result '{place}' has no completion time");
            anyhow::ensure!(records.iter().all(|record| record.inherited_from.is_none()),
                "seed result '{place}' must name its original run, not intermediate run {}", source.color());
        }
        for place in places {
            let records: Vec<_> = records_at(&source.snap, place).into_iter().map(|record| {
                let mut inherited = record;
                inherited.color = self.color();
                inherited.inherited_from = Some(inherited.inherited_from.unwrap_or(source.color()));
                inherited
            }).collect();
            if records.is_empty() { continue; }
            for record in &records {
                if let Some(output) = source.outputs.get(&record.id) { self.outputs.insert(record.id, output.clone()); }
            }
            self.snap.executions.entry(place.id.clone()).or_default().extend(records);
        }
        // A reused In boundary that refused its scope settled the
        // members by the kick its teardown gave them, never by a pulse
        // (a gated-off scope emits nothing inward, and the history holds
        // nothing for it). The record carries no kick, so the members
        // this run re-runs are told again here; the ones reused with it
        // keep their own records.
        for place in places {
            for record in records_at(&source.snap, place) {
                if record.status != NodeExecutionStatus::Skipped { continue; }
                let Some(group_id) = self.project.nodes.iter().find(|node| node.id == place.id)
                    .and_then(|node| node.group_boundary.as_ref())
                    .filter(|boundary| boundary.role == GroupBoundaryRole::In)
                    .map(|boundary| boundary.group_id.clone()) else { continue };
                // The teardown's own rule: a scope inside a scope that
                // was taken down owes its members nothing, the enclosing
                // scope's kick reached them (and reaches them again here
                // through ITS reused In).
                let refused = refused_scope(&self.project, &self.edge_idx, &group_id, &record.frames);
                if refused.owed_by_the_enclosing_pass(record.skip_reason.as_ref()) { continue; }
                let RefusedScope { frames, members, .. } = refused;
                let rerun: Vec<String> = members.into_iter()
                    .filter(|member| !places.contains(&Located::at(member, &frames))).collect();
                kick_scope(&mut self.snap.kicked, &rerun, &frames, Some(&group_id));
            }
        }
        for (location, entries) in &source.snap.awaited_sequences {
            if places.contains(&Located::at(&location.node_id, &location.frames)) { self.snap.awaited_sequences.insert(location.clone(), entries.clone()); }
        }
        for (_, instance) in source.snap.loop_runtime.iter() {
            if places.contains(&Located::at(boundary_in_id(&instance.key.group_id), &instance.key.parent_frames)) {
                self.snap.loop_runtime.inherit(instance, self.color()).map_err(anyhow::Error::msg)?;
            }
        }
        let mut frontier = self.snap.selection.clone()
            .unwrap_or_else(|| weft_core::project::selection::RunSelection::whole(&self.project));
        let nodes = frontier.nodes.clone();
        frontier.edges.retain(|wire| weft_core::project::selection::wire_ends(&self.project, wire)
            .is_some_and(|(source, target)| places.contains(&source) && nodes.contains(&target)));
        let edge_idx = EdgeIndex::selected(&self.project, frontier);
        let mut effects = FoldEffects::default();
        for output in history.iter().filter(|output| places.contains(&Located::at(&output.node, &output.frames))) {
            let start = effects.emissions.len();
            match &output.value {
                Some(value) => {
                    postprocess_output(&output.node, &OutputBag::from([(output.port.clone(), value.clone())]),
                        output.id, self.color(), &output.frames, &self.project, &mut self.snap.pulses,
                        &edge_idx, &mut effects.emissions)?;
                }
                None => {
                    emit_port_closure(&output.node, &output.port, output.id, self.color(), &output.frames,
                        &self.project, &mut self.snap.pulses, &edge_idx, &mut effects.emissions, output.error.as_deref())?;
                }
            }
            for emission in &mut effects.emissions[start..] {
                    emission.pulse.provided = output.provided;
                    emission.pulse.inherited_from = Some(source.color());
                    if let Some(pulse) = self.snap.pulses.get_mut(&emission.pulse.target_node)
                        .and_then(|pulses| pulses.iter_mut().find(|pulse| pulse.id == emission.pulse.id))
                    { pulse.provided = output.provided; pulse.inherited_from = Some(source.color()); }
            }
            self.remember_output(output.clone());
        }
        Ok(effects)
    }

    /// Settle only after all chosen ancestors have supplied their frontier.
    pub fn settle(&mut self, at_unix: u64) -> FoldEffects {
        let mut effects = FoldEffects::default();
        self.boundary_pass(at_unix, &mut effects);
        effects
    }

    /// The snapshot, with every awaited sequence in call order.
    pub fn into_snapshot(mut self) -> ExecutionSnapshot {
        for entries in self.snap.awaited_sequences.values_mut() {
            entries.sort_by_key(|e| e.call_index);
        }
        self.snap
    }

    /// The snapshot as it stands, with every awaited sequence in call
    /// order, leaving the fold to take more rows.
    pub fn current_snapshot(&self) -> ExecutionSnapshot {
        let mut snap = self.snap.clone();
        for entries in snap.awaited_sequences.values_mut() {
            entries.sort_by_key(|e| e.call_index);
        }
        snap
    }

    /// Apply one row, in journal order.
    pub fn apply(&mut self, ev: &ExecEvent) -> FoldEffects {
        let corruptions_before = self.snap.corruptions.len();
        let mut effects = self.apply_row(ev);
        effects.rejections = self.snap.corruptions[corruptions_before..].to_vec();
        effects
    }

    fn apply_row(&mut self, ev: &ExecEvent) -> FoldEffects {
        let mut effects = FoldEffects::default();
        let color = self.snap.color;
        match ev {
            ExecEvent::ExecutionStarted { phase, subgraph, program, seed, .. } => {
                self.snap.program = program.clone();
                self.snap.inherited_origins = seed.as_ref().map(|seed| seed.origins.clone()).unwrap_or_default();
                self.snap.selection = subgraph.clone();
                self.phase = Some(*phase);
                self.edge_idx = match subgraph {
                    Some(selection) => EdgeIndex::selected(&self.project, selection.clone()),
                    None => EdgeIndex::build(&self.project),
                };
                self.dispatchable = subgraph.as_ref().map(|s| s.dispatchable_nodes());
            }
            ExecEvent::NodeKicked { node_id, frames, firing, payload, port_snapshot, at_unix, .. } => {
                // First kick wins; further kicks on the same location are
                // a true no-op (the documented contract). The first kick's
                // payload and `dispatched` flag are authoritative; a later
                // kick carrying a different payload is a writer-level bug
                // the fold must not paper over by silently merging.
                self.snap
                    .kicked
                    .entry(FiringLocation::new(node_id.clone(), frames.clone()))
                    .and_modify(|kick| {
                        if *firing && !kick.firing {
                            kick.firing = true;
                            kick.payload = payload.clone();
                            kick.port_snapshot = port_snapshot.clone();
                        }
                    })
                    .or_insert_with(|| KickedNode {
                        firing: *firing,
                        payload: payload.clone(),
                        port_snapshot: port_snapshot.clone(),
                        dispatched: false,
                        scope_skipped: None,
                    });
                self.boundary_pass(*at_unix, &mut effects);
            }
            ExecEvent::PortEmitted { emission_id, node_id, frames, port, value, provided, at_unix, .. } => {
                // A provided value is the wire's value with no firing
                // behind it: its source never ran (it sits outside the
                // scope), so there is no record to hold it to; it fans
                // out and marks its pulses, nothing else. The run's own
                // emissions belong to a firing, and a row about one the
                // journal never opened is corruption.
                let record_id = if *provided {
                    None
                } else {
                    match self.firing_record_id(node_id, frames, CorruptionSite::PortEmitted, ev) {
                        Some(id) => Some(id),
                        None => return effects,
                    }
                };
                let shared = value.clone();
                let mut bag = OutputBag::new();
                bag.insert(port.clone(), shared.clone());
                let emitted_before = effects.emissions.len();
                match postprocess_output(
                    node_id, &bag, *emission_id, color, frames, &self.project,
                    &mut self.snap.pulses, &self.edge_idx, &mut effects.emissions,
                ) {
                    Ok(mentioned) => {
                        self.remember_output(OutputEmission {
                            id: *emission_id, node: node_id.clone(), frames: frames.clone(),
                            port: port.clone(), value: Some(shared.clone()), error: None, provided: *provided,
                        });
                        // A provided value rode the wire like the source's
                        // own emission; the pulses it became say so, and
                        // the consumer's firing view reads it off them.
                        if *provided {
                            for emitted in &effects.emissions[emitted_before..] {
                                if let Some(p) = self
                                    .snap
                                    .pulses
                                    .get_mut(&emitted.pulse.target_node)
                                    .and_then(|b| b.iter_mut().find(|p| p.id == emitted.pulse.id))
                                {
                                    p.provided = true;
                                }
                            }
                        }
                        if let Some(record_id) = record_id {
                            // An output is mentioned even without a wire;
                            // a later seeded run can connect a consumer to it.
                            self.record_mut(node_id, record_id)
                                .expect("`firing_record_id` found this record and nothing removes records")
                                .mentioned_ports
                                .extend(mentioned);
                            // A firing's emissions merge into one output
                            // bag, later wins per port: an ordinary port is
                            // mentioned once per firing (the runner refuses
                            // a second mention), a generator port once per
                            // item, so its recorded output is the last item.
                            self.outputs.entry(record_id).or_default().insert(port.clone(), shared);
                        }
                        self.boundary_pass(*at_unix, &mut effects);
                    }
                    Err(e) => self.report(CorruptionSite::PortEmitted, format!("{}: {e}", describe(ev))),
                }
            }
            ExecEvent::PortClosed { emission_id, node_id, frames, port, provided, at_unix, .. } => {
                if *provided {
                    let start = effects.emissions.len();
                    match emit_port_closure(node_id, port, *emission_id, color, frames,
                        &self.project, &mut self.snap.pulses, &self.edge_idx, &mut effects.emissions, None)
                    {
                        Ok(()) => {
                            self.remember_output(OutputEmission {
                                id: *emission_id, node: node_id.clone(), frames: frames.clone(),
                                port: port.clone(), value: None, error: None, provided: true,
                            });
                            for emission in &effects.emissions[start..] {
                                if let Some(pulse) = self.snap.pulses.get_mut(&emission.pulse.target_node)
                                    .and_then(|pulses| pulses.iter_mut().find(|p| p.id == emission.pulse.id))
                                { pulse.provided = true; }
                            }
                            self.boundary_pass(*at_unix, &mut effects);
                        }
                        Err(error) => self.report(CorruptionSite::PortClosed, format!("{}: {error}", describe(ev))),
                    }
                    return effects;
                }
                let Some(record_id) = self.firing_record_id(node_id, frames, CorruptionSite::PortClosed, ev) else {
                    return effects;
                };
                let _applied = self.close_port(record_id, node_id, frames, port, *emission_id, *at_unix, ev, &mut effects);
            }
            ExecEvent::PulsesConsumed { node_id, pulse_ids, .. } => {
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
            ExecEvent::NodeStarted { node_id, frames, at_unix, .. } => {
                if self.node_def(node_id, ev).is_none() {
                    return effects;
                }
                // Live dispatch opens a new record per firing whenever
                // no non-terminal one exists (it ships NodeResumed,
                // never a second NodeStarted, to continue a
                // non-terminal record). So a NodeStarted whose latest
                // same-key record is TERMINAL is a legitimate second
                // firing (a streaming/bus node fired its consumer
                // twice), and one whose latest record is still open is
                // a row the writer could not have produced: corruption,
                // skipped whole (absorbing its pulses into no record
                // would strand them for the resume-time un-absorb).
                let has_non_terminal = self
                    .latest_record(node_id, frames)
                    .map(|e| !e.status.is_terminal())
                    .unwrap_or(false);
                if has_non_terminal {
                    self.report(
                        CorruptionSite::NodeLifecycle,
                        format!("{}: a firing is already open at these frames", describe(ev)),
                    );
                    return effects;
                }
                let received = self.receiving(node_id, frames);
                let absorbed = self.absorb_pending(node_id, frames, false);
                if let Some(k) = self.snap.kicked.get_mut(&FiringLocation::new(node_id.clone(), frames.clone())) {
                    k.dispatched = true;
                }
                let ordinal = next_firing_ordinal(&self.snap.executions, node_id, color, frames);
                {
                    self.snap.executions.entry(node_id.clone()).or_default().push(NodeExecution {
                        id: Uuid::new_v4(),
                        received,
                        skip_reason: None,
                        node_id: node_id.clone(),
                        status: NodeExecutionStatus::Running,
                        pulses_absorbed: absorbed,
                        ordinal,
                        error: None,
                        callback_id: None,
                        started_at: *at_unix,
                        completed_at: None,
                        cost_usd: 0.0,
                        logs: Vec::new(),
                        mentioned_ports: Default::default(),
                        closed_output_ports: Default::default(),
                        color,
                        frames: frames.clone(),
                        inherited_from: None,
                    });
                }
            }
            ExecEvent::NodeSuspended { node_id, frames, token, .. } => {
                // A suspension of a firing the journal never opened is
                // corruption like every other lifecycle row: dropped
                // quietly, the record would read Running and a resume
                // would re-run a body that is parked on a signal.
                let Some(record_id) = self.firing_record_id(node_id, frames, CorruptionSite::NodeLifecycle, ev) else {
                    return effects;
                };
                if let Some(e) = self.record_mut(node_id, record_id) {
                    e.status = NodeExecutionStatus::WaitingForInput;
                    e.callback_id = Some(token.clone());
                }
            }
            ExecEvent::NodeResumed { node_id, frames, token, .. } => {
                // Flip the pending pulses at the location to Absorbed,
                // exactly as the NodeStarted arm does. The live resume
                // dispatch absorbs them in RAM; if the fold left them
                // Pending, a refold on a fresh worker would hold the
                // node's record terminal (Completed) yet a Pending pulse
                // at its (node, frames) location, and `find_ready_nodes`
                // (which keys purely on Pending pulses, ignoring the
                // record) would re-fire the node: double execution on
                // every respawn. The un-absorb path (`redispatch_locations`)
                // also assumes resume-time pulses were folded Absorbed
                // before it flips them back. A resume of a firing the
                // journal never opened is corruption, skipped whole:
                // pulses absorbed into no record could never be given
                // back.
                let Some(record_id) = self.firing_record_id(node_id, frames, CorruptionSite::NodeLifecycle, ev) else {
                    return effects;
                };
                let absorbed = self.absorb_pending(node_id, frames, false);
                if let Some(e) = self.record_mut(node_id, record_id) {
                    e.status = NodeExecutionStatus::Running;
                    e.callback_id = None;
                    for id in absorbed {
                        if !e.pulses_absorbed.contains(&id) {
                            e.pulses_absorbed.push(id);
                        }
                    }
                }
                // Only clear suspension state when this resume was
                // suspension-driven (token present). Crashed-Running
                // recovery has no token to remove.
                if let Some(t) = token {
                    self.snap.suspensions.remove(t);
                    effects.resumed_value = self.snap.pending_deliveries.remove(t);
                }
            }
            ExecEvent::NodeCompleted { node_id, frames, at_unix, .. } => {
                self.terminate(node_id, frames, NodeExecutionStatus::Completed, None, None, *at_unix, ev, &mut effects);
            }
            ExecEvent::NodeFailed { node_id, frames, error, at_unix, .. } => {
                self.terminate(node_id, frames, NodeExecutionStatus::Failed, Some(error), None, *at_unix, ev, &mut effects);
            }
            ExecEvent::NodeSkipped { node_id, frames, reason, at_unix, .. } => {
                self.terminate(node_id, frames, NodeExecutionStatus::Skipped, None, Some(reason), *at_unix, ev, &mut effects);
            }
            ExecEvent::NodeCancelled { node_id, frames, reason, at_unix, .. } => {
                self.terminate(node_id, frames, NodeExecutionStatus::Cancelled, Some(reason), None, *at_unix, ev, &mut effects);
            }
            ExecEvent::LoopInstantiated { group_id, parent_frames, .. } => {
                let loop_in_id = boundary_in_id(group_id);
                let Some(def) = self.project.nodes.iter().find(|n| n.id == loop_in_id) else {
                    self.report(CorruptionSite::LoopInstantiated, format!("{}: the program has no LoopIn '{loop_in_id}'", describe(ev)));
                    return effects;
                };
                let Some(view) = self.firing_view_of(&loop_in_id, parent_frames) else {
                    self.report(CorruptionSite::LoopInstantiated, format!("{}: no LoopIn firing at these frames", describe(ev)));
                    return effects;
                };
                if let Err(e) = instantiate(&mut self.snap.loop_runtime, def, &self.project, &view, parent_frames, color) {
                    self.report(CorruptionSite::LoopInstantiated, format!("{}: {e}", describe(ev)));
                }
            }
            ExecEvent::LoopIterationLaunched { group_id, parent_frames, index, stream_pulse, at_unix, .. } => {
                let key = LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), color };
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
                        let loop_in_id = boundary_in_id(group_id);
                        let held = self
                            .snap
                            .pulses
                            .get(&loop_in_id)
                            .and_then(|bucket| bucket.iter().find(|p| p.id == id))
                            .map(|p| LoopStreamItem { pulse: p.id, value: p.value.clone() });
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
                match launch_iteration(&mut self.snap.loop_runtime, &key, *index, item, &self.project, &self.edge_idx, &mut self.snap.pulses) {
                    Ok(launch) => {
                        if let Some(id) = taken {
                            let loop_in_id = boundary_in_id(group_id);
                            self.snap.pulses.remove_consumed(&loop_in_id, &[id]);
                        }
                        effects.emissions.extend(launch.emissions);
                        kick_scope(&mut self.snap.kicked, &launch.roots, &launch.body_frames, None);
                        self.boundary_pass(*at_unix, &mut effects);
                    }
                    Err(e) => self.report(CorruptionSite::LoopIterationLaunched, format!("{}: {e}", describe(ev))),
                }
            }
            ExecEvent::LoopOutFired { group_id, parent_frames, index, .. } => {
                let key = LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), color };
                let Some(config) = self.snap.loop_runtime.get(&key).map(|i| i.config.clone()) else {
                    self.report(CorruptionSite::LoopOutFired, format!("{}: no prior LoopInstantiated", describe(ev)));
                    return effects;
                };
                let loop_out_id = boundary_out_id(group_id);
                let Some(def) = self.project.nodes.iter().find(|n| n.id == loop_out_id) else {
                    self.report(CorruptionSite::LoopOutFired, format!("{}: the program has no LoopOut '{loop_out_id}'", describe(ev)));
                    return effects;
                };
                let body_frames = loop_runtime::iteration_frames(parent_frames, *index);
                let Some(view) = self.firing_view_of(&loop_out_id, &body_frames) else {
                    self.report(CorruptionSite::LoopOutFired, format!("{}: no LoopOut firing at the iteration's frames", describe(ev)));
                    return effects;
                };
                match classify_loop_out(def, &config, &view.input, &view.closed_ports) {
                    Ok(writes) => {
                        // The vote is an effect only once the firing's
                        // writes landed: a row the runtime refuses is
                        // corruption, and a firing it already holds (or
                        // one after the loop ended) recorded nothing;
                        // neither paints.
                        match self.snap.loop_runtime.apply_loop_out_writes(&key, *index, writes.gather_writes, writes.carry_writes) {
                            Ok(true) => effects.loop_out_done = Some(writes.done_vote),
                            Ok(false) => {}
                            Err(e) => self.report(CorruptionSite::LoopOutFired, format!("{}: {e}", describe(ev))),
                        }
                    }
                    Err(e) => self.report(CorruptionSite::LoopOutFired, format!("{}: {e}", describe(ev))),
                }
            }
            ExecEvent::LoopStreamEnded { group_id, parent_frames, end, .. } => {
                let key = LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), color };
                if let Err(e) = self.snap.loop_runtime.record_stream_end(&key, end.clone()) {
                    self.report(CorruptionSite::LoopStreamEnded, format!("{}: {e}", describe(ev)));
                }
            }
            ExecEvent::LoopTerminated { group_id, parent_frames, reason, at_unix, .. } => {
                let key = LoopInstanceKey { group_id: group_id.clone(), parent_frames: parent_frames.clone(), color };
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
                            effects.emissions.extend(close_loop_outward(&key, &self.project, &self.edge_idx, &mut self.snap.pulses, *reason));
                            self.boundary_pass(*at_unix, &mut effects);
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
                        Ok(LoopAdvance::EmitOutward { gather, carry, .. }) => {
                            match emit_loop_outward(&key, gather, carry, &self.project, &self.edge_idx, &mut self.snap.pulses) {
                                Ok((output, emissions)) => {
                                    for (port, value) in output {
                                        self.remember_output(OutputEmission {
                                            id: loop_termination_emission(group_id, parent_frames), node: boundary_out_id(group_id),
                                            frames: parent_frames.clone(), port, value: Some(value), error: None, provided: false,
                                        });
                                    }
                                    effects.emissions.extend(emissions);
                                    self.boundary_pass(*at_unix, &mut effects);
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
            ExecEvent::SuspensionRegistered { node_id, frames, token, spec, call_index, at_unix, .. } => {
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
                // Close the out-of-order window: a fire can journal
                // SuspensionResolved BEFORE the register executor journals
                // SuspensionRegistered (the two are written by independent
                // dispatcher paths with no ordering between them). If the
                // resolution already landed, `pending_deliveries` holds its
                // value; stamp it now so the entry is born resolved.
                // Without this, the SuspensionResolved arm found no entry to
                // mark (not registered yet), the entry lands `resolved:
                // None`, and the await never resumes (permanent hang, fire
                // consumed). Making the fold order-insensitive for the
                // Registered/Resolved pair is the right invariant.
                let resolved = self.snap.pending_deliveries.get(token).cloned();
                self.snap
                    .awaited_sequences
                    .entry(FiringLocation::new(node_id.clone(), frames.clone()))
                    .or_default()
                    .push(AwaitedEntry {
                        call_index: *call_index,
                        kind: AwaitedEntryKind::Await { token: token.clone(), resolved },
                    });
            }
            ExecEvent::RunOutput { node_id, frames, call_index, name, value, .. } => {
                self.snap
                    .awaited_sequences
                    .entry(FiringLocation::new(node_id.clone(), frames.clone()))
                    .or_default()
                    .push(AwaitedEntry {
                        call_index: *call_index,
                        kind: AwaitedEntryKind::Run { name: name.clone(), value: value.clone() },
                    });
            }
            ExecEvent::SuspensionResolved { token, value, .. } => {
                self.snap.pending_deliveries.insert(token.clone(), value.clone());
                for entries in self.snap.awaited_sequences.values_mut() {
                    for entry in entries.iter_mut() {
                        if let AwaitedEntryKind::Await { token: t, resolved } = &mut entry.kind {
                            if t == token {
                                *resolved = Some(value.clone());
                            }
                        }
                    }
                }
            }
            // A metered call's cost record: the cost of a firing belongs on
            // its execution record. A record may already be terminal when
            // the cost lands (a durable RecordCost task journals on its own
            // timeline); the fold still books it onto the matching
            // (color, frames) record. An unknown amount (`None`) adds
            // nothing here (the sum is a number); the honest unknown lives
            // in the event's own row.
            ExecEvent::CostReported { node_id, frames, amount_usd, .. } => {
                if let Some(amount) = amount_usd {
                    if let Some(e) = self.latest_record_mut(node_id, frames) {
                        e.cost_usd += amount;
                    }
                }
            }
            // Observability-only events: they carry no state the resume
            // fold needs. Bus replay and caller-exchange replay are read
            // straight from the row stream by the inspector, not from the
            // snapshot. Caller events are additionally non-durable by
            // design (a live connection dies with its worker), so they
            // never contribute to a resumed run's state.
            ExecEvent::LogLine { .. }
            | ExecEvent::TriggerCaptured { .. }
            | ExecEvent::ExecutionTagged { .. }
            | ExecEvent::ExecutionCompleted { .. }
            | ExecEvent::ExecutionFailed { .. }
            | ExecEvent::ExecutionCancelled { .. }
            | ExecEvent::BusJoined { .. }
            | ExecEvent::BusLeft { .. }
            | ExecEvent::BusWindow { .. }
            | ExecEvent::BusClosed { .. }
            | ExecEvent::CallerConnected { .. }
            | ExecEvent::CallerWindow { .. }
            | ExecEvent::CallerErrored { .. }
            | ExecEvent::CallerDisconnected { .. } => {}
        }
        effects
    }

    // ----- Display accessors ------------------------------------------

    /// What the latest firing at `(node, frames)` received: the bag
    /// its body read and the wired ports that arrived closed, rebuilt
    /// from the pulses the record absorbed (or, for a kicked entry
    /// node, from its kick). `None` when no firing sits there.
    pub fn firing_view(&self, node_id: &str, frames: &LoopFrames) -> Option<FiringView> {
        let view = self.firing_view_of(node_id, frames)?;
        Some(FiringView {
            input: Value::Object(owned_bag(&view.input)),
            closed_ports: view.closed_ports,
            provided_ports: view.provided_ports,
            backup_ports: view.backup_ports,
            inherited_ports: view.inherited_ports,
        })
    }

    /// What the latest firing at `(node, frames)` handed out, merged
    /// across its emissions (later wins per port). `None` for a firing
    /// that emitted nothing, or none there.
    pub fn output_of(&self, node_id: &str, frames: &LoopFrames) -> Option<Value> {
        let record = self.latest_record(node_id, frames)?;
        self.record_output(record)
    }

    pub fn record_output(&self, record: &NodeExecution) -> Option<Value> {
        let bag = self.outputs.get(&record.id)?;
        if bag.is_empty() {
            return None;
        }
        Some(Value::Object(owned_bag(bag)))
    }

    /// The run's outputs: per node, what its last completed firing
    /// handed out.
    /// The last completed output bag of every node at every place it
    /// ran, keyed by the place (`one/@src:lib:clean.strip`): one file
    /// included twice is two entries, never one overwriting the other.
    /// Boundaries are the compiler's and hold nothing a person wrote,
    /// so they are left out.
    pub fn final_outputs(&self) -> Value {
        let mut obj = serde_json::Map::new();
        let boundary = |id: &str| self.project.nodes.iter().any(|n| n.id == id && n.group_boundary.is_some());
        for (node_id, execs) in &self.snap.executions {
            if boundary(node_id) { continue; }
            let mut last_by_place: BTreeMap<Located, &weft_core::exec::NodeExecution> = BTreeMap::new();
            for exec in execs.iter().filter(|e| e.status == NodeExecutionStatus::Completed) {
                last_by_place.insert(Located::at(node_id, &exec.frames), exec);
            }
            for (place, last) in last_by_place {
                if let Some(bag) = self.outputs.get(&last.id).filter(|b| !b.is_empty()) {
                    obj.insert(place.to_string(), Value::Object(owned_bag(bag)));
                }
            }
        }
        Value::Object(obj)
    }

    // ----- Internals --------------------------------------------------

    fn remember_output(&mut self, emission: OutputEmission) {
        if let Some(history) = &mut self.output_history {
            if self.output_ids.insert((emission.id, emission.node.clone(), emission.port.clone())) {
                history.push(emission);
            }
        }
    }

    /// The outward closures a torn-down scope owes, remembered the way
    /// `close_scope_outward` sends them: with the failure that took the
    /// scope down, or plain when it was gated off. The history is what a
    /// seeded run replays and what a frozen example expects, so a plain
    /// record here would launder the failure for both.
    fn remember_scope_closures(&mut self, group: &str, frames: &LoopFrames, id: Uuid, failure: Option<&str>) {
        if self.output_history.is_none() { return; }
        let node_id = boundary_out_id(group);
        if !self.edge_idx.admits(&node_id, frames) { return; }
        let ports: Vec<_> = self.project.nodes.iter().find(|node| node.id == node_id).into_iter()
            .flat_map(|node| node.outputs.iter().filter(|port| self.edge_idx.includes_port(node, frames, &port.name)))
            .map(|port| port.name.clone()).collect();
        for port in ports {
            self.remember_output(OutputEmission { id, node: node_id.clone(), frames: frames.clone(), port,
                value: None, error: failure.map(str::to_string), provided: false });
        }
    }

    fn firing_view_of(&self, node_id: &str, frames: &LoopFrames) -> Option<weft_core::exec::ready::FiringInput> {
        self.latest_record(node_id, frames).map(|record| record.received.clone())
    }

    fn receiving(&self, node_id: &str, frames: &LoopFrames) -> weft_core::exec::ready::FiringInput {
        let def = self.project.nodes.iter().find(|n| n.id == node_id).expect("validated node");
        let pending: Vec<_> = self.snap.pulses.get(node_id).into_iter().flatten()
            .filter(|p| p.color == self.snap.color && p.frames == *frames && p.status.is_pending()).collect();
        if pending.is_empty() {
            if let Some(kick) = self.snap.kicked.get(&FiringLocation::new(node_id, frames.clone())) {
                return kicked_group(def, kick, frames, self.snap.color, &self.project, &self.edge_idx).received;
            }
        }
        let wired = wired_inputs(&self.project, &self.edge_idx, node_id, frames);
        let effective = effective_input_pulses(def, &pending, &wired, &self.project, &self.edge_idx, self.snap.color, frames);
        firing_input(def, &effective.iter().collect::<Vec<_>>(), &wired, frames, &self.edge_idx)
    }

    /// Run the boundary pass: fire every group boundary this row made
    /// ready, to a fixpoint.
    fn boundary_pass(&mut self, now: u64, effects: &mut FoldEffects) {
        // Rows before the birth row fold as an untargeted Fire run (the
        // whole graph dispatchable, nothing dropped), which is what
        // `dispatchable == None` already means for them.
        let pass = settle_table(
            &self.project,
            &self.edge_idx,
            self.phase.unwrap_or(weft_core::context::Phase::Fire),
            self.dispatchable.as_ref(),
            self.snap.color,
            now,
            &mut self.snap.pulses,
            &mut self.snap.executions,
            &mut self.snap.kicked,
        );
        for dispatch in &pass.boundaries {
            if let BoundaryOutcome::Fired { record_id, output, .. } = &dispatch.outcome {
                if let Some(output) = output {
                    self.outputs.insert(*record_id, output.clone());
                }
                if let Some(record) = self.snap.executions.get(&dispatch.node_id)
                    .and_then(|records| records.iter().find(|record| record.id == *record_id))
                {
                    let id = boundary_emission(&dispatch.node_id, &dispatch.frames, record.ordinal);
                    let provided_ports = record.received.provided_ports.clone();
                    let closed_with_error = record.received.closed_with_error.clone();
                    let skipped = record.status == NodeExecutionStatus::Skipped;
                    let taken_down_above =
                        matches!(record.skip_reason, Some(SkipReason::ScopeSkipped { .. }));
                    // The history says what the wire said, per status
                    // (`dispatch_passthrough`): a FAILED boundary (a port
                    // refused its value) swept every output with its own
                    // error, so that error wins on every port and takes
                    // the scope down; a SKIPPED one took the scope down
                    // with the failure its skip inherited (its gate closed
                    // on one), or plainly; a completed one forwarded each
                    // closed input with that input's own error. A type
                    // error recorded beside a skip never reached a wire.
                    let own_failure: Option<String> =
                        (record.status == NodeExecutionStatus::Failed).then(|| record.error.clone()).flatten();
                    let inherited_failure: Option<String> = skipped
                        .then(|| record.skip_reason.as_ref().and_then(|reason| reason.inherited_failure().map(str::to_string)))
                        .flatten();
                    let values = output.clone().unwrap_or_default();
                    // A boundary inside a scope that was taken down from
                    // above emitted nothing of its own on the wire (the
                    // enclosing teardown closed every member's exits), so
                    // the history holds nothing of its own for it either.
                    let ports: Vec<_> = if taken_down_above { Vec::new() } else {
                        self.project.nodes.iter().find(|node| node.id == dispatch.node_id)
                            .into_iter().flat_map(|node| node.outputs.iter().filter(|port| self.edge_idx.includes_port(node, &dispatch.frames, &port.name)))
                            .map(|port| port.name.clone()).collect()
                    };
                    for port in ports {
                        // A boundary forwards a failed closure WITH its
                        // error (`dispatch_passthrough`), and the history
                        // is what a seeded run replays: remembered plain,
                        // the seed would launder the failure into "nothing
                        // came" for a watcher outside the scope. A skipped
                        // In's own ports are remembered closed although
                        // the wire got no emission (its members were kicked
                        // into the scope's skip instead): a seed replaying
                        // the In alone has no kick to give them, so the
                        // closures are what tell them, with the failure
                        // the members' own exits carried.
                        let error = own_failure
                            .clone()
                            .or_else(|| closed_with_error.get(&port).cloned())
                            .or_else(|| inherited_failure.clone());
                        self.remember_output(OutputEmission {
                            id, node: dispatch.node_id.clone(), frames: dispatch.frames.clone(),
                            value: values.get(&port).cloned(), provided: provided_ports.contains(&port), port, error,
                        });
                    }
                    // A scope that never started (gated off, or its In
                    // failed) closed outward: the same closures, with the
                    // same failure, the wire got from `tear_down_scope`.
                    // Not for a scope inside a scope that was itself taken
                    // down: the enclosing pass closed its exits already,
                    // and the live teardown emits nothing for it.
                    if (skipped && !taken_down_above) || own_failure.is_some() {
                        let group = self.project.nodes.iter().find(|node| node.id == dispatch.node_id)
                            .and_then(|node| node.group_boundary.as_ref()).filter(|boundary| boundary.role == GroupBoundaryRole::In)
                            .map(|boundary| boundary.group_id.clone());
                        let failure = own_failure.as_deref().or(inherited_failure.as_deref());
                        if let Some(group) = group { self.remember_scope_closures(&group, &dispatch.frames, id, failure); }
                    }
                }
            }
        }
        effects.boundaries.extend(pass.boundaries);
    }

    /// Read the specific boundary firing, including its used backup origins.
    pub fn boundary_view(&self, dispatch: &BoundaryDispatch) -> Option<FiringView> {
        let BoundaryOutcome::Fired { record_id, .. } = &dispatch.outcome else { return None };
        let record = self.snap.executions.get(&dispatch.node_id).into_iter().flatten()
            .find(|record| record.id == *record_id).expect("a fired boundary has its execution record");
        Some(FiringView {
            input: Value::Object(owned_bag(&record.received.input)),
            closed_ports: record.received.closed_ports.clone(),
            provided_ports: record.received.provided_ports.clone(),
            backup_ports: record.received.backup_ports.clone(),
            inherited_ports: record.received.inherited_ports.clone(),
        })
    }

    /// Mark every pending pulse at `(node, frames)` Absorbed and return
    /// their ids, minus the items on the node's generator ports when
    /// `include_generator` is false (a run dispatch leaves those for
    /// its live feed; a skip absorbs everything).
    fn absorb_pending(&mut self, node_id: &str, frames: &LoopFrames, include_generator: bool) -> Vec<Uuid> {
        let generator_ports: HashSet<String> = self
            .project
            .nodes
            .iter()
            .find(|n| n.id == node_id)
            .map(|n| generator_inputs(n).into_iter().map(str::to_string).collect())
            .unwrap_or_default();
        let mut absorbed = Vec::new();
        if let Some(bucket) = self.snap.pulses.get_mut(node_id) {
            for p in bucket.iter_mut() {
                if p.status == PulseStatus::Pending
                    && &p.frames == frames
                    && (include_generator || !generator_ports.contains(&p.target_port))
                {
                    p.absorb();
                    absorbed.push(p.id);
                }
            }
        }
        absorbed
    }

    /// A port of a running firing closed (the body's own close, or a
    /// refused value). `false` when the program cannot place the
    /// closure (reported as corruption, nothing applied).
    #[allow(clippy::too_many_arguments)]
    fn close_port(
        &mut self,
        record_id: Uuid,
        node_id: &str,
        frames: &LoopFrames,
        port: &str,
        emission_id: Uuid,
        at_unix: u64,
        ev: &ExecEvent,
        effects: &mut FoldEffects,
    ) -> bool {
        match emit_port_closure(
            node_id, port, emission_id, self.snap.color, frames, &self.project,
            &mut self.snap.pulses, &self.edge_idx, &mut effects.emissions, None,
        ) {
            Ok(()) => {
                self.remember_output(OutputEmission {
                    id: emission_id, node: node_id.into(), frames: frames.clone(),
                    port: port.into(), value: None, error: None, provided: false,
                });
                self.record_mut(node_id, record_id)
                    .expect("`firing_record_id` found this record and nothing removes records")
                    .mentioned_ports
                    .insert(port.to_string());
                self.record_mut(node_id, record_id).expect("record exists")
                    .closed_output_ports.insert(port.to_string());
                self.boundary_pass(at_unix, effects);
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
    /// scope-skipped one owes nothing, its scope's boundary closed the
    /// outward surface). A scope's In boundary that skipped (a LoopIn
    /// gated off) closes the scope's outward surface and kicks every
    /// member into a skip. A loop boundary otherwise closes nothing on
    /// its own: a failing one whose instance never existed closes the
    /// loop's outward surface here; every other loop teardown rode a
    /// `LoopTerminated` row.
    #[allow(clippy::too_many_arguments)]
    fn terminate(
        &mut self,
        node_id: &str,
        frames: &LoopFrames,
        status: NodeExecutionStatus,
        error: Option<&str>,
        skip_reason: Option<&SkipReason>,
        at_unix: u64,
        ev: &ExecEvent,
        effects: &mut FoldEffects,
    ) {
        let color = self.snap.color;
        let Some(def) = self.node_def(node_id, ev) else { return };
        let Some(record_id) = self.firing_record_id(node_id, frames, CorruptionSite::NodeLifecycle, ev) else {
            return;
        };
        // A skip is the whole group's consumption: whatever was left
        // pending for its feeds (a stream's items) goes with it.
        if status == NodeExecutionStatus::Skipped {
            let more = self.absorb_pending(node_id, frames, true);
            if let Some(e) = self.record_mut(node_id, record_id) {
                for id in more {
                    if !e.pulses_absorbed.contains(&id) {
                        e.pulses_absorbed.push(id);
                    }
                }
            }
        }
        let e = self.record_mut(node_id, record_id).expect("`firing_record_id` found this record and nothing removes records");
        let ordinal = e.ordinal;
        e.status = status.clone();
        e.skip_reason = skip_reason.cloned();
        e.completed_at = Some(at_unix);
        if let Some(err) = error {
            e.error = Some(err.to_string());
        }
        e.callback_id = None;
        let is_loop_boundary = matches!(def.node_type.as_str(), "LoopIn" | "LoopOut");
        let in_scope = def
            .group_boundary
            .as_ref()
            .filter(|gb| gb.role == GroupBoundaryRole::In)
            .map(|gb| gb.group_id.clone());
        let emission_id = terminal_sweep_emission(node_id, frames, ordinal);
        match (&status, skip_reason) {
            (NodeExecutionStatus::Skipped, Some(reason)) if in_scope.is_some() => {
                let group_id = in_scope.expect("checked");
                effects.emissions.extend(tear_down_scope(
                    &self.project, &self.edge_idx, &mut self.snap.pulses, &mut self.snap.kicked,
                    emission_id, color, &group_id, frames, Some(reason), reason.inherited_failure(),
                ));
                self.remember_scope_closures(&group_id, frames, emission_id, reason.inherited_failure());
            }
            (NodeExecutionStatus::Skipped, Some(SkipReason::ScopeSkipped { .. })) => {}
            _ if is_loop_boundary => {
                // A boundary firing that failed with no instance behind
                // it: the loop never got a `LoopTerminated`, so its
                // outward surface closes from here.
                if status == NodeExecutionStatus::Failed {
                    if let Ok(key) = loop_runtime::instance_key(&def, frames, color) {
                        if self.snap.loop_runtime.get(&key).is_none() {
                            effects.emissions.extend(close_loop_outward(&key, &self.project, &self.edge_idx, &mut self.snap.pulses, LoopTerminationReason::Failed));
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
                let failure = error.or_else(|| skip_reason.and_then(SkipReason::inherited_failure));
                if let Err(e) = close_unmentioned_downstream(
                    node_id, &mentioned, emission_id, color, frames, &self.project,
                    &mut self.snap.pulses, &self.edge_idx, &mut effects.emissions, failure, &closed,
                ) {
                    // A rejected row fires no boundary: the reader that
                    // paints the row paints nothing for it, and boundaries
                    // it made ready without their cause would show alone.
                    self.report(CorruptionSite::NodeLifecycle, format!("{}: {e}", describe(ev)));
                    return;
                }
                for port in def.outputs.iter().filter(|port| !closed.contains(&port.name) && (!mentioned.contains(&port.name) || port.is_generator())) {
                    self.remember_output(OutputEmission {
                        id: emission_id, node: node_id.into(), frames: frames.clone(), port: port.name.clone(),
                        value: None, error: failure.map(str::to_string), provided: false,
                    });
                }
            }
        }
        self.boundary_pass(at_unix, effects);
    }

    /// The program's node a row is about, or a corruption when the
    /// program has no such node, or when it is a group boundary: a
    /// boundary never journals a row (the pass fires it on both sides),
    /// so a row naming one was written by nothing this engine runs.
    fn node_def(&mut self, node_id: &str, ev: &ExecEvent) -> Option<NodeDefinition> {
        match self.project.nodes.iter().find(|n| n.id == node_id) {
            Some(def) if is_passthrough(def) => {
                self.report(CorruptionSite::NodeLifecycle, format!("{}: '{node_id}' is a group boundary, which never journals a row", describe(ev)));
                None
            }
            Some(def) => Some(def.clone()),
            None => {
                self.report(CorruptionSite::NodeLifecycle, format!("{}: the program has no node '{node_id}'", describe(ev)));
                None
            }
        }
    }

    /// The id of the latest record at `(node, frames)`, or a
    /// corruption at `site` when there is none (a row about a firing
    /// the journal never opened), or when the node is a group boundary
    /// (see `node_def`).
    fn firing_record_id(&mut self, node_id: &str, frames: &LoopFrames, site: CorruptionSite, ev: &ExecEvent) -> Option<Uuid> {
        if self.project.nodes.iter().any(|n| n.id == node_id && is_passthrough(n)) {
            self.report(site, format!("{}: '{node_id}' is a group boundary, which never journals a row", describe(ev)));
            return None;
        }
        match self.latest_record(node_id, frames).map(|e| e.id) {
            Some(id) => Some(id),
            None => {
                self.report(site, format!("{}: no firing of '{node_id}' at these frames", describe(ev)));
                None
            }
        }
    }

    /// The record a row about `(node, frames)` is about: the same
    /// `latest_firing` the live engine ends and sweeps by.
    fn latest_record(&self, node_id: &str, frames: &LoopFrames) -> Option<&NodeExecution> {
        latest_firing(&self.snap.executions, node_id, self.snap.color, frames)
    }

    fn latest_record_mut(&mut self, node_id: &str, frames: &LoopFrames) -> Option<&mut NodeExecution> {
        latest_firing_mut(&mut self.snap.executions, node_id, self.snap.color, frames)
    }

    /// The record `firing_record_id` named, by id within its node's
    /// own records (a record never moves between nodes).
    fn record_mut(&mut self, node_id: &str, record_id: Uuid) -> Option<&mut NodeExecution> {
        self.snap.executions.get_mut(node_id)?.iter_mut().find(|e| e.id == record_id)
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
            color = %self.snap.color, ?site, reason = %reason,
            "skip row during fold (journal corruption)"
        );
        self.snap.corruptions.push(JournalCorruption { site, reason });
    }
}

/// A row named in a corruption report: its kind and the firing it is
/// about, never its payload.
fn describe(ev: &ExecEvent) -> String {
    match ev {
        ExecEvent::PortEmitted { node_id, frames, port, .. }
        | ExecEvent::PortClosed { node_id, frames, port, .. } => {
            format!("{} node={node_id} frames={frames:?} port={port}", ev.kind_str())
        }
        ExecEvent::NodeStarted { node_id, frames, .. }
        | ExecEvent::NodeSuspended { node_id, frames, .. }
        | ExecEvent::NodeResumed { node_id, frames, .. }
        | ExecEvent::NodeCompleted { node_id, frames, .. }
        | ExecEvent::NodeFailed { node_id, frames, .. }
        | ExecEvent::NodeSkipped { node_id, frames, .. }
        | ExecEvent::NodeCancelled { node_id, frames, .. }
        | ExecEvent::PulsesConsumed { node_id, frames, .. } => {
            format!("{} node={node_id} frames={frames:?}", ev.kind_str())
        }
        ExecEvent::LoopInstantiated { group_id, parent_frames, .. }
        | ExecEvent::LoopStreamEnded { group_id, parent_frames, .. }
        | ExecEvent::LoopTerminated { group_id, parent_frames, .. } => {
            format!("{} group={group_id} parent_frames={parent_frames:?}", ev.kind_str())
        }
        ExecEvent::LoopIterationLaunched { group_id, parent_frames, index, .. }
        | ExecEvent::LoopOutFired { group_id, parent_frames, index, .. } => {
            format!("{} group={group_id} parent_frames={parent_frames:?} index={index}", ev.kind_str())
        }
        other => other.kind_str().to_string(),
    }
}

/// What a firing received, for the screen.
pub struct FiringView {
    pub input: Value,
    pub closed_ports: Vec<String>,
    /// Input ports receiving supplied outputs or starting backups.
    pub provided_ports: Vec<String>,
    pub backup_ports: Vec<String>,
    pub inherited_ports: std::collections::BTreeMap<String, Color>,
}

/// Fold a whole log at once.
pub fn fold_to_snapshot<'a>(
    color: Color,
    project: Arc<ProjectDefinition>,
    events: impl IntoIterator<Item = &'a ExecEvent>,
) -> ExecutionSnapshot {
    let mut fold = Fold::new(color, project);
    for ev in events {
        fold.apply(ev);
    }
    fold.into_snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use weft_core::frames::Frame;
    use weft_core::pulse::PulseStatus;

    fn color() -> Color {
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
        json!({ "id": format!("{source}.{sh}->{target}.{th}"), "source": source, "sourceHandle": sh, "target": target, "targetHandle": th })
    }

    fn project(nodes: Vec<Value>, edges: Vec<Value>) -> Arc<ProjectDefinition> {
        Arc::new(
            serde_json::from_value(json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "nodes": nodes,
                "edges": edges,
                "groups": [],
                "createdAt": "1970-01-01T00:00:00Z",
                "updatedAt": "1970-01-01T00:00:00Z",
            }))
            .expect("test project"),
        )
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

    fn started(node: &str, frames: LoopFrames, at: u64) -> ExecEvent {
        ExecEvent::NodeStarted { color: color(), node_id: node.into(), frames, at_unix: at }
    }

    fn completed(node: &str, frames: LoopFrames, at: u64) -> ExecEvent {
        ExecEvent::NodeCompleted { color: color(), node_id: node.into(), frames, at_unix: at }
    }

    fn emitted(emission: Uuid, node: &str, frames: LoopFrames, port: &str, value: Value) -> ExecEvent {
        ExecEvent::PortEmitted {
            color: color(),
            emission_id: emission,
            node_id: node.into(),
            frames,
            port: port.into(),
            value: Arc::new(value),
            provided: false,
            at_unix: 0,
        }
    }

    fn started_execution() -> ExecEvent {
        ExecEvent::ExecutionStarted {
            color: color(),
            project_id: uuid::Uuid::nil(),
            entry_node: "src".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("h".into()),
            program: None, source_version: None, node_test: false,
            subgraph: None,
            seed: None,
            at_unix: 0,
        }
    }

    fn kicked(node: &str) -> ExecEvent {
        ExecEvent::NodeKicked {
            color: color(),
            node_id: node.into(), frames: vec![],
            firing: true,
            payload: None,
            port_snapshot: None,
            at_unix: 0,
        }
    }

    fn pending<'a>(snap: &'a ExecutionSnapshot, node: &str) -> Vec<&'a weft_core::pulse::Pulse> {
        snap.pulses.get(node).map(|b| b.iter().filter(|p| p.status.is_pending()).collect()).unwrap_or_default()
    }

    /// A replay crosses a call the way the live engine did: the site's
    /// In pushes its frame, the shared body's rows carry it, and the
    /// body's Out hands the result back to that site alone.
    #[test]
    fn a_call_replays_under_its_site_frame_and_answers_its_own_site() {
        use weft_core::project::boundary_types as bt;
        let mut project = project(
            vec![
                node("src", "T", &[], &[("a", "Number"), ("b", "Number")], &[], Value::Null, Value::Null),
                node("a__in", bt::CALL_IN, &[("x", "Number", true)], &[("x", "Number")], &[], json!({"groupId": "a", "role": "In"}), Value::Null),
                node("a__out", bt::CALL_OUT, &[("y", "Number", true)], &[("y", "Number")], &[], json!({"groupId": "a", "role": "Out"}), Value::Null),
                node("b__in", bt::CALL_IN, &[("x", "Number", true)], &[("x", "Number")], &[], json!({"groupId": "b", "role": "In"}), Value::Null),
                node("b__out", bt::CALL_OUT, &[("y", "Number", true)], &[("y", "Number")], &[], json!({"groupId": "b", "role": "Out"}), Value::Null),
                node("B__in", bt::INCLUDE_IN, &[("x", "Number", true)], &[("x", "Number")], &[], json!({"groupId": "B", "role": "In"}), Value::Null),
                node("B.n", "T", &[("in", "Number", true)], &[("out", "Number")], &["B"], Value::Null, Value::Null),
                node("B__out", bt::INCLUDE_OUT, &[("y", "Number", true)], &[("y", "Number")], &[], json!({"groupId": "B", "role": "Out"}), Value::Null),
                node("sa", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
                node("sb", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![
                edge("src", "a", "a__in", "x"), edge("src", "b", "b__in", "x"),
                edge("a__in", "x", "B__in", "x"), edge("b__in", "x", "B__in", "x"),
                edge("B__in", "x", "B.n", "in"), edge("B.n", "out", "B__out", "y"),
                edge("B__out", "y", "a__out", "y"), edge("B__out", "y", "b__out", "y"),
                edge("a__out", "y", "sa", "in"), edge("b__out", "y", "sb", "in"),
            ],
        );
        Arc::get_mut(&mut project).unwrap().groups = serde_json::from_value(json!([
            { "id": "a", "kind": "call", "body": "B", "nodeIds": [] },
            { "id": "b", "kind": "call", "body": "B", "nodeIds": [] },
            { "id": "B", "kind": "body", "nodeIds": ["B.n"] }
        ])).unwrap();
        let call = |site: &str| vec![Frame::Call { site: site.into() }];
        let emission = Uuid::new_v4();
        let events = vec![
            started_execution(),
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
        let mut fold = Fold::new(color(), project);
        for ev in &events {
            fold.apply(ev);
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        // The body's In fired once per site, under that site's frame.
        let mut body_in: Vec<LoopFrames> = snap.executions["B__in"].iter().map(|r| r.frames.clone()).collect();
        body_in.sort();
        assert_eq!(body_in, vec![call("a"), call("b")]);
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
        assert_eq!(snap.executions["a__out"].len(), 1);
        assert!(!snap.executions.contains_key("b__out"));
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
            started_execution(),
            ExecEvent::NodeKicked {
                color: color(),
                node_id: "wired".into(), frames: vec![],
                firing: true,
                payload: Some(json!({"scheduledTime":"event"})),
                port_snapshot: Some(json!({"cron":cron})),
                at_unix: 0,
            },
            started("wired", vec![], 1),
        ];
        let mut fold = Fold::new(color(), project);
        for ev in &events {
            fold.apply(ev);
        }
        assert!(fold.snapshot().corruptions.is_empty(), "{:?}", fold.snapshot().corruptions);
        let view = fold.firing_view("wired", &vec![]).expect("the trigger fired");
        assert_eq!(view.input, json!({ "cron": cron }), "the wired port carries what reached it");
        assert!(view.provided_ports.is_empty(), "baked settings are not authored emissions");
    }

    /// An emitted output reaches an ordinary consumer while its source
    /// has no firing of its own.
    #[test]
    fn supplied_output_reaches_its_consumers_view() {
        let project = project(
            vec![
                node("sched", "T", &[], &[("value", "String")], &[], Value::Null, Value::Null),
                node("wired", "T", &[("cron", "String", true)], &[("scheduledTime", "String")], &[], Value::Null, Value::Null),
            ],
            vec![edge("sched", "value", "wired", "cron")],
        );
        let cron = json!("0 0 * * * *");
        let mut fold = Fold::new(color(), project);
        // The supplied source emits before the consumer starts.
        for ev in [
            started_execution(),
            ExecEvent::PortEmitted {
                color: color(),
                emission_id: Uuid::new_v4(),
                node_id: "sched".into(),
                frames: vec![],
                port: "value".into(),
                value: Arc::new(cron.clone()),
                provided: true,
                at_unix: 0,
            },
            started("wired", vec![], 1),
        ] {
            fold.apply(&ev);
        }
        let view = fold.firing_view("wired", &vec![]).expect("the consumer ran");
        assert_eq!(view.input, json!({ "cron": cron }));
        assert_eq!(view.provided_ports, vec!["cron"]);
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
            started_execution(),
            kicked("src"),
            started("src", vec![], 0),
            emitted(emission, "src", vec![], "out", big.clone()),
            completed("src", vec![], 1),
            started("a", vec![], 2),
        ];
        let mut fold = Fold::new(color(), project);
        for ev in &events {
            fold.apply(ev);
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let a = &snap.pulses["a"][0];
        let b = &snap.pulses["b"][0];
        let c = &snap.pulses["c"][0];
        assert_eq!(a.status, PulseStatus::Absorbed);
        assert_eq!(b.status, PulseStatus::Pending);
        assert_eq!(c.status, PulseStatus::Pending);
        assert!(Arc::ptr_eq(&a.value, &b.value) && Arc::ptr_eq(&b.value, &c.value), "one allocation");
        assert_eq!(*a.value, big);
        assert_eq!(snap.executions["a"][0].pulses_absorbed, vec![a.id]);
        assert_eq!(a.id, weft_core::exec::emission::pulse_id(emission, "out", "a", "in", false));
        let view = fold.firing_view("a", &vec![]).expect("a fired");
        assert_eq!(view.input, json!({ "in": big }));
        assert_eq!(fold.output_of("src", &vec![]), Some(json!({ "out": big })));
        assert_eq!(fold.final_outputs(), json!({ "src": { "out": big } }));
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
            ExecEvent::NodeSkipped {
                color: color(),
                node_id: "b".into(),
                frames: vec![],
                reason: SkipReason::RequiredInputClosed { port: "in".into(), failure: None },
                at_unix: 2,
            },
        ];
        let snap = fold_to_snapshot(color(), project, &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        for n in ["a", "b", "c"] {
            assert_eq!(snap.pulses[n].len(), 1, "{n} got a closure");
            assert!(snap.pulses[n][0].closed);
        }
        assert_eq!(snap.pulses["b"][0].status, PulseStatus::Absorbed);
        assert_eq!(snap.executions["b"][0].status, NodeExecutionStatus::Skipped);
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
        let mut fold = Fold::new(color(), project);
        for e in &events {
            fold.apply(e);
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(snap.pulses["a"].len(), 1);
        assert_eq!(snap.pulses["b"].len(), 1);
        assert_eq!(snap.pulses["a"][0].id, weft_core::exec::emission::pulse_id(emission, "out", "a", "in", false));
        assert_eq!(*snap.pulses["a"][0].value, json!(1));
        assert_eq!(snap.pulses["a"][0].status, PulseStatus::Pending);
        assert_eq!(fold.output_of("src", &vec![]), Some(json!({ "out": 1 })));
    }

    /// The awaited sequence of a firing reads in call order whatever
    /// order its rows landed in (a register and a run output are
    /// written by independent dispatcher paths).
    #[test]
    fn an_awaited_sequence_reads_in_call_order() {
        let register = |call_index: u32| ExecEvent::SuspensionRegistered {
            color: color(),
            node_id: "a".into(),
            frames: vec![],
            token: format!("t{call_index}"),
            spec: make_spec(),
            call_index,
            at_unix: 0,
        };
        let events = vec![
            register(2),
            ExecEvent::RunOutput { color: color(), node_id: "a".into(), frames: vec![], call_index: 0, name: "decide".into(), value: json!("left"), at_unix: 0 },
            register(1),
        ];
        let snap = fold_to_snapshot(color(), fan_out_project(), &events);
        let seq = &snap.awaited_sequences[&FiringLocation::new("a", vec![])];
        assert_eq!(seq.iter().map(|e| e.call_index).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert!(matches!(&seq[0].kind, AwaitedEntryKind::Run { name, .. } if name == "decide"));
        assert!(matches!(&seq[1].kind, AwaitedEntryKind::Await { token, .. } if token == "t1"));
    }

    /// `gen.rows` streams into loop `lp` over its `rows` port; the body
    /// `step` reads `item` and writes `res`.
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
                node("step", "T", &[("item", "Number", true)], &[("res", "Number")], &["lp"], Value::Null, Value::Null),
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
                edge("lp__in", "rows", "step", "item"),
                edge("step", "res", "lp__out", "res"),
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
        let item1 = weft_core::exec::emission::pulse_id(e1, "rows", "lp__in", "rows", false);
        let events = vec![
            started("gen", vec![], 0),
            emitted(e1, "gen", vec![], "rows", json!(7)),
            started("lp__in", vec![], 1),
            loop_row_instantiated(),
            emitted(e2, "gen", vec![], "rows", json!(8)),
            ExecEvent::LoopIterationLaunched {
                color: color(),
                group_id: "lp".into(),
                parent_frames: vec![],
                index: 0,
                stream_pulse: Some(item1.to_string()),
                at_unix: 1,
            },
            ExecEvent::LoopStreamEnded {
                color: color(),
                group_id: "lp".into(),
                parent_frames: vec![],
                end: weft_core::generator::StreamEnd::Failed { error: "upstream broke".into() },
                at_unix: 2,
            },
        ];
        let snap = fold_to_snapshot(color(), stream_loop_project(), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let step = pending(&snap, "step");
        assert_eq!(step.len(), 1, "the item (the implicit index has no wire here): {step:?}");
        let item = step.iter().find(|p| p.target_port == "item").expect("the item reached the body");
        assert_eq!(*item.value, json!(7));
        assert_eq!(item.frames, vec![frame(0)]);
        let loop_in: Vec<Uuid> = snap.pulses["lp__in"].iter().map(|p| p.id).collect();
        assert!(!loop_in.contains(&item1), "the launched item left the LoopIn's bucket");
        assert_eq!(loop_in.len(), 1, "the second item still waits there");
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.launched, vec![0]);
        let ended = snap.loop_runtime.stream_instances_with_recorded_end();
        assert_eq!(ended.len(), 1);
        assert!(matches!(&ended[0].2, weft_core::generator::StreamEnd::Failed { error } if error == "upstream broke"));
    }

    /// The crash window between an emission and the consumer's start:
    /// the pulse refolds Pending, so the consumer is ready again
    /// (at-least-once).
    #[test]
    fn a_crash_between_emission_and_start_refolds_the_consumer_ready() {
        let project = fan_out_project();
        let events = vec![
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(1)),
            completed("src", vec![], 1),
        ];
        let snap = fold_to_snapshot(color(), project.clone(), &events);
        let edge_idx = EdgeIndex::build(&project);
        let ready = weft_core::exec::find_ready_nodes(&project, &snap.pulses, &edge_idx, None);
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
        let snap = fold_to_snapshot(color(), project, &events);
        let sites: Vec<CorruptionSite> = snap.corruptions.iter().map(|c| c.site).collect();
        assert_eq!(
            sites,
            vec![CorruptionSite::PortEmitted, CorruptionSite::NodeLifecycle, CorruptionSite::PortEmitted],
            "{:?}",
            snap.corruptions
        );
        assert!(!snap.pulses.contains_key("a"), "an emission before its start put nothing on the wires");
    }

    /// `src.out` feeds group `g` (`g__in.x` -> `inner.in` -> `g__out.y`
    /// -> `sink.in`), with the group's gate on `src.flow`; `h` is a
    /// group nested in `g` around `inner`.
    fn nested_group_project() -> Arc<ProjectDefinition> {
        let gate = ("_should_flow", "Boolean", false);
        project(
            vec![
                node("src", "T", &[], &[("out", "Number"), ("flow", "Boolean")], &[], Value::Null, Value::Null),
                node("g__in", "Passthrough", &[("x", "Number", true), gate], &[("x", "Number")], &[], json!({ "groupId": "g", "role": "In" }), Value::Null),
                node("h__in", "Passthrough", &[("x", "Number", true)], &[("x", "Number")], &["g"], json!({ "groupId": "h", "role": "In" }), Value::Null),
                node("inner", "T", &[("in", "Number", true)], &[("out", "Number")], &["g", "h"], Value::Null, Value::Null),
                node("h__out", "Passthrough", &[("y", "Number", true)], &[("y", "Number")], &["g"], json!({ "groupId": "h", "role": "Out" }), Value::Null),
                node("g__out", "Passthrough", &[("y", "Number", true)], &[("y", "Number")], &[], json!({ "groupId": "g", "role": "Out" }), Value::Null),
                node("sink", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
            ],
            vec![
                edge("src", "out", "g__in", "x"),
                edge("src", "flow", "g__in", "_should_flow"),
                edge("g__in", "x", "h__in", "x"),
                edge("h__in", "x", "inner", "in"),
                edge("inner", "out", "h__out", "y"),
                edge("h__out", "y", "g__out", "y"),
                edge("g__out", "y", "sink", "in"),
            ],
        )
    }

    /// Two nested boundaries fire from the fold with no row of their
    /// own: the inner node's pulse is the emitted allocation, and both
    /// boundary records read completed.
    #[test]
    fn nested_groups_fold_from_the_emission_alone() {
        let project = nested_group_project();
        let e1 = Uuid::new_v4();
        let events = vec![
            started_execution(),
            kicked("src"),
            started("src", vec![], 0),
            emitted(e1, "src", vec![], "out", json!(9)),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(true)),
            completed("src", vec![], 1),
        ];
        let mut fold = Fold::new(color(), project);
        let mut fired = Vec::new();
        for ev in &events {
            let effects = fold.apply(ev);
            fired.extend(effects.boundaries.into_iter().map(|b| b.node_id));
        }
        let snap = fold.snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(fired, vec!["g__in", "h__in"]);
        assert_eq!(snap.executions["g__in"][0].status, NodeExecutionStatus::Completed);
        assert_eq!(snap.executions["h__in"][0].status, NodeExecutionStatus::Completed);
        let inner = pending(snap, "inner");
        assert_eq!(inner.len(), 1);
        assert_eq!(*inner[0].value, json!(9));
        assert!(Arc::ptr_eq(&inner[0].value, &snap.pulses["g__in"].iter().find(|p| p.target_port == "x").unwrap().value));
        assert!(fold.firing_view("g__in", &vec![]).is_some());
        assert_eq!(fold.output_of("g__in", &vec![]), Some(json!({ "x": 9 })));
    }

    /// A gated-off scope folds skipped: its boundary record reads
    /// skipped, every member is kicked into a scope skip, and the
    /// scope's consumer sees a closure.
    #[test]
    fn a_gated_off_scope_folds_skipped() {
        let project = nested_group_project();
        let events = vec![
            started_execution(),
            kicked("src"),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(9)),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(false)),
            completed("src", vec![], 1),
        ];
        let snap = fold_to_snapshot(color(), project, &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(snap.executions["g__in"][0].status, NodeExecutionStatus::Skipped);
        assert!(pending(&snap, "sink")[0].closed);
        for member in ["h__in", "inner", "h__out"] {
            let kick = snap.kicked.get(&FiringLocation::new(member, vec![])).unwrap_or_else(|| panic!("{member} kicked"));
            assert_eq!(kick.scope_skipped.as_deref(), Some("g"));
        }
        // The nested In boundary, kicked into its skip, fired too and
        // closes nothing more (its scope's surface is already closed).
        assert_eq!(snap.executions["h__in"][0].status, NodeExecutionStatus::Skipped);
        assert!(pending(&snap, "inner").is_empty());
    }

    /// The history says what the wire said: a node skipped because its
    /// input closed on a failure closes its own outputs WITH that
    /// failure, and the frozen example (`output_wires`) records it so.
    #[test]
    fn a_skip_inherited_from_a_failure_is_remembered_with_it() {
        let mut fold = Fold::new(color(), fan_out_project()).with_output_history();
        for event in [
            started_execution(), kicked("src"), started("src", vec![], 0),
            ExecEvent::NodeFailed { color: color(), node_id: "src".into(), frames: vec![], error: "the database is down".into(), at_unix: 1 },
            started("a", vec![], 2),
            ExecEvent::NodeSkipped {
                color: color(), node_id: "a".into(), frames: vec![],
                reason: SkipReason::RequiredInputClosed { port: "in".into(), failure: Some("the database is down".into()) },
                at_unix: 2,
            },
        ] { assert!(!fold.apply(&event).rejected()); }
        let wires = fold.output_wires().unwrap();
        let a_out = wires.iter().find(|wire| wire.node == "a" && wire.port == "out").expect("a.out remembered");
        assert!(a_out.closed);
        assert_eq!(a_out.error.as_deref(), Some("the database is down"), "the skip passes the failure on, in the history too");
    }

    /// A scope gated off by a gate that closed on a failure closes
    /// outward with it, and the history remembers the Out's ports so.
    #[test]
    fn a_scope_gated_off_by_a_failure_is_remembered_with_it() {
        let mut fold = Fold::new(color(), nested_group_project()).with_output_history();
        for event in [
            started_execution(), kicked("src"), started("src", vec![], 0),
            ExecEvent::NodeFailed { color: color(), node_id: "src".into(), frames: vec![], error: "the database is down".into(), at_unix: 1 },
        ] { assert!(!fold.apply(&event).rejected()); }
        let snap = fold.snapshot();
        assert_eq!(snap.executions["g__in"][0].status, NodeExecutionStatus::Skipped);
        assert_eq!(pending(snap, "sink")[0].close_error.as_deref(), Some("the database is down"), "the wire carries it");
        let wires = fold.output_wires().unwrap();
        let out = wires.iter().find(|wire| wire.node == "g__out" && wire.port == "y").expect("g__out.y remembered");
        assert!(out.closed);
        assert_eq!(out.error.as_deref(), Some("the database is down"), "and so does the history");
    }

    /// An In boundary that refuses a value (a String on a Number port)
    /// fails before anything is forwarded: every one of its own ports
    /// and every port of its Out close with that error on the wire, and
    /// the history remembers them so.
    #[test]
    fn a_boundary_that_refused_a_value_is_remembered_failed_throughout() {
        let mut fold = Fold::new(color(), nested_group_project()).with_output_history();
        for event in [
            started_execution(), kicked("src"), started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!("nine")),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(true)),
            completed("src", vec![], 1),
        ] { assert!(!fold.apply(&event).rejected()); }
        let snap = fold.snapshot();
        assert_eq!(snap.executions["g__in"][0].status, NodeExecutionStatus::Failed);
        let refusal = snap.executions["g__in"][0].error.clone().expect("the refusal is the record's error");
        assert_eq!(pending(snap, "sink")[0].close_error.as_deref(), Some(refusal.as_str()), "the wire carries it");
        let wires = fold.output_wires().unwrap();
        for (node, port) in [("g__in", "x"), ("g__out", "y")] {
            let wire = wires.iter().find(|wire| wire.node == node && wire.port == port)
                .unwrap_or_else(|| panic!("{node}.{port} remembered"));
            assert!(wire.closed);
            assert_eq!(wire.error.as_deref(), Some(refusal.as_str()), "{node}.{port} remembers the refusal");
        }
    }

    /// The nested scope of a gated-off scope was taken down by the
    /// enclosing pass: its Out's ports get no closure of their own on
    /// the wire, and none in the history either (a plain one there
    /// would launder the enclosing failure on replay).
    #[test]
    fn a_scope_inside_a_gated_off_scope_remembers_no_closures_of_its_own() {
        let mut fold = Fold::new(color(), nested_group_project()).with_output_history();
        for event in [
            started_execution(), kicked("src"), started("src", vec![], 0),
            ExecEvent::NodeFailed { color: color(), node_id: "src".into(), frames: vec![], error: "the database is down".into(), at_unix: 1 },
        ] { assert!(!fold.apply(&event).rejected()); }
        let snap = fold.snapshot();
        assert_eq!(snap.executions["h__in"][0].status, NodeExecutionStatus::Skipped);
        let wires = fold.output_wires().unwrap();
        assert!(!wires.iter().any(|wire| wire.node == "h__in" || wire.node == "h__out"), "the nested scope closes from the enclosing pass, not its own: {wires:?}");
    }

    /// A seeded run that reuses a gated-off scope's In (and the nested
    /// In under it) but re-runs the members: nothing on the wire ever
    /// told those members (the scope emits nothing inward), so the
    /// reuse tells them again with the scope's kick, and they settle as
    /// scope-skipped exactly as in the run they came from.
    #[test]
    fn a_reused_refused_scope_kicks_the_members_it_does_not_reuse() {
        let project = nested_group_project();
        let mut source = Fold::new(color(), project.clone()).with_output_history();
        for event in [
            started_execution(), kicked("src"), started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(9)),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(false)),
            completed("src", vec![], 1),
        ] { assert!(!source.apply(&event).rejected()); }
        assert_eq!(source.snapshot().executions["h__in"][0].status, NodeExecutionStatus::Skipped);
        let reused = BTreeSet::from([Located::top("src"), Located::top("g__in"), Located::top("h__in")]);
        let eligible = weft_core::seeding::inheritable_nodes(&project, source.snapshot());
        assert!(reused.iter().all(|node| eligible.contains(node)), "{eligible:?}");
        let mut child = Fold::new(Uuid::new_v4(), project);
        child.apply(&started_execution());
        child.inherit(&source, &reused).unwrap();
        let snap = child.snapshot();
        for member in ["inner", "h__out"] {
            let kick = snap.kicked.get(&FiringLocation::new(member, vec![])).unwrap_or_else(|| panic!("{member} told again: {:?}", snap.kicked.keys().collect::<Vec<_>>()));
            assert_eq!(kick.scope_skipped.as_deref(), Some("g"), "{member}");
        }
        assert!(!snap.kicked.contains_key(&FiringLocation::new("h__in", vec![])), "a reused member keeps its record and gets no second kick");
    }

    #[test]
    fn closed_group_boundaries_with_the_same_port_keep_both_outputs() {
        let mut definition = nested_group_project().as_ref().clone();
        let out = definition.nodes.iter_mut().find(|node| node.id == "g__out").unwrap();
        out.inputs[0].name = "x".into();
        out.outputs[0].name = "x".into();
        for edge in &mut definition.edges {
            if edge.target == "g__out" { edge.target_handle = Some("x".into()); }
            if edge.source == "g__out" { edge.source_handle = Some("x".into()); }
        }
        let mut fold = Fold::new(color(), Arc::new(definition)).with_output_history();
        for event in [
            started_execution(), kicked("src"), started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(9)),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(false)),
            completed("src", vec![], 1),
        ] { assert!(!fold.apply(&event).rejected()); }
        let outputs = fold.output_wires().unwrap();
        for boundary in ["g__in", "g__out"] {
            assert_eq!(outputs.iter().filter(|wire| wire.node == boundary && wire.port == "x" && wire.closed).count(), 1, "{boundary} closure survives");
        }
    }

    /// `items` feeds loop `lp` over `items` with carry `acc`; the body
    /// `step` reads `item` and `acc` and writes `acc` and `res`.
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
                node("step", "T", &[("item", "Number", true), ("acc", "Number", true)], &[("acc", "Number"), ("res", "Number")], &["lp"], Value::Null, Value::Null),
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
                edge("lp__in", "items", "step", "item"),
                edge("lp__in", "acc", "step", "acc"),
                edge("step", "acc", "lp__out", "acc"),
                edge("step", "res", "lp__out", "res"),
                edge("lp__out", "res", "sink", "res"),
                edge("lp__out", "acc", "sink", "acc"),
            ],
        )
    }

    fn loop_row_instantiated() -> ExecEvent {
        ExecEvent::LoopInstantiated { color: color(), group_id: "lp".into(), parent_frames: vec![], at_unix: 0 }
    }

    fn loop_row_launched(index: u32) -> ExecEvent {
        ExecEvent::LoopIterationLaunched {
            color: color(),
            group_id: "lp".into(),
            parent_frames: vec![],
            index,
            stream_pulse: None,
            at_unix: 0,
        }
    }

    fn loop_row_out_fired(index: u32) -> ExecEvent {
        ExecEvent::LoopOutFired { color: color(), group_id: "lp".into(), parent_frames: vec![], index, at_unix: 0 }
    }

    fn lp_key() -> LoopInstanceKey {
        LoopInstanceKey { group_id: "lp".into(), parent_frames: vec![], color: color() }
    }

    /// The rows a sequential loop writes up to the first LoopOut.
    fn loop_prefix() -> Vec<ExecEvent> {
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
        let e1 = Uuid::new_v4();
        let items = weft_core::exec::emission::pulse_id(e1, "items", "lp__in", "items", false);
        let events = vec![
            started("feed", vec![], 0),
            emitted(e1, "feed", vec![], "items", json!([10, 20, 30])),
            emitted(Uuid::new_v4(), "feed", vec![], "seed", json!(1)),
            completed("feed", vec![], 0),
            started("lp__in", vec![], 1),
            loop_row_instantiated(),
            ExecEvent::LoopIterationLaunched {
                color: color(),
                group_id: "lp".into(),
                parent_frames: vec![],
                index: 0,
                stream_pulse: Some(items.to_string()),
                at_unix: 1,
            },
        ];
        let snap = fold_to_snapshot(color(), loop_project(false), &events);
        assert_eq!(snap.corruptions.len(), 1, "{:?}", snap.corruptions);
        assert!(snap.corruptions[0].reason.contains("not a stream"), "{}", snap.corruptions[0].reason);
        assert!(snap.pulses["lp__in"].iter().any(|p| p.id == items), "the named pulse stays in the LoopIn's bucket");
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert!(inst.launched.is_empty(), "no iteration was recorded: {:?}", inst.launched);
        assert!(pending(&snap, "step").is_empty(), "no body pulse was put on a wire");
    }

    /// A loop resumed between instantiation and its first LoopOut
    /// rebuilds the instance from the LoopIn's pulses: the outer input,
    /// the carry seed, and iteration 0's body pulses (the slice, the
    /// carry, the index).
    #[test]
    fn a_loop_resumed_before_its_first_loop_out_rebuilds_its_seeds() {
        let snap = fold_to_snapshot(color(), loop_project(false), &loop_prefix());
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.iter_cap, Some(3));
        assert_eq!(inst.launched, vec![0]);
        assert_eq!(*inst.carry_values["acc"], json!(1));
        assert_eq!(*inst.outer_input["items"], json!([10, 20, 30]));
        assert!(!inst.outer_input.contains_key("_should_flow"));
        let step = pending(&snap, "step");
        let by_port = |port: &str| step.iter().find(|p| p.target_port == port).map(|p| (*p.value).clone());
        assert_eq!(by_port("item"), Some(json!(10)));
        assert_eq!(by_port("acc"), Some(json!(1)));
        assert!(step.iter().all(|p| p.frames == vec![frame(0)]));
    }

    /// LoopOut's writes come off its own firing's pulses; the next
    /// launch carries the updated carry; termination puts the
    /// assembled list and the final carry on the outward wires.
    #[test]
    fn loop_out_writes_and_termination_fold_from_the_body_pulses() {
        let mut events = loop_prefix();
        let body = |i: u32, acc: i64, res: i64| {
            vec![
                started("step", vec![frame(i)], 2),
                emitted(Uuid::new_v4(), "step", vec![frame(i)], "acc", json!(acc)),
                emitted(Uuid::new_v4(), "step", vec![frame(i)], "res", json!(res)),
                completed("step", vec![frame(i)], 2),
                started("lp__out", vec![frame(i)], 3),
                loop_row_out_fired(i),
                completed("lp__out", vec![frame(i)], 3),
            ]
        };
        events.extend(body(0, 11, 100));
        events.push(loop_row_launched(1));
        let snap = fold_to_snapshot(color(), loop_project(false), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.out_fired, vec![0]);
        assert_eq!(*inst.carry_values["acc"], json!(11));
        let step1: Vec<_> = pending(&snap, "step").into_iter().filter(|p| p.frames == vec![frame(1)]).collect();
        assert_eq!(*step1.iter().find(|p| p.target_port == "acc").unwrap().value, json!(11), "iteration 1 carries the write");
        assert_eq!(*step1.iter().find(|p| p.target_port == "item").unwrap().value, json!(20));

        events.extend(body(1, 22, 200));
        events.push(loop_row_launched(2));
        events.extend(body(2, 33, 300));
        events.push(ExecEvent::LoopTerminated {
            color: color(),
            group_id: "lp".into(),
            parent_frames: vec![],
            reason: LoopTerminationReason::OverExhausted,
            at_unix: 9,
        });
        let snap = fold_to_snapshot(color(), loop_project(false), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.terminated, Some(LoopTerminationReason::OverExhausted));
        let sink = pending(&snap, "sink");
        let by_port = |port: &str| sink.iter().find(|p| p.target_port == port).map(|p| (*p.value).clone());
        assert_eq!(by_port("res"), Some(json!([100, 200, 300])));
        assert_eq!(by_port("acc"), Some(json!(33)));
        let mut definition = loop_project(false).as_ref().clone();
        definition.groups.push(serde_json::from_value(json!({"id":"lp","kind":"loop","loopConfig":{},"nodeIds":["step"]})).unwrap());
        let eligible = weft_core::seeding::inheritable_nodes(&definition, &snap);
        assert!(["lp__in", "step", "lp__out"].iter().all(|node| eligible.contains(&Located::top(*node))));
        let mut missing_iteration = snap.clone();
        missing_iteration.executions.get_mut("step").unwrap().retain(|record| record.frames != vec![frame(1)]);
        let eligible = weft_core::seeding::inheritable_nodes(&definition, &missing_iteration);
        assert!(["lp__in", "step", "lp__out"].iter().all(|node| !eligible.contains(&Located::top(*node))), "one missing iteration invalidates the whole loop");
        // Replaying the terminal row puts nothing on the wires twice.
        events.push(events.last().unwrap().clone());
        let again = fold_to_snapshot(color(), loop_project(false), &events);
        assert_eq!(pending(&again, "sink").len(), 2);
    }

    #[test]
    fn a_zero_iteration_loop_reuses_its_empty_result_without_inventing_body_firings() {
        let mut definition = loop_project(false).as_ref().clone();
        definition.groups.push(serde_json::from_value(json!({"id":"lp","kind":"loop","loopConfig":{},"nodeIds":["step"]})).unwrap());
        let project = Arc::new(definition);
        let mut source = Fold::new(color(), project.clone()).with_output_history();
        for row in [
            started_execution(), started("feed", vec![], 0),
            emitted(Uuid::new_v4(), "feed", vec![], "items", json!([])),
            emitted(Uuid::new_v4(), "feed", vec![], "seed", json!(1)), completed("feed", vec![], 0),
            started("lp__in", vec![], 1), loop_row_instantiated(), completed("lp__in", vec![], 1),
            ExecEvent::LoopTerminated { color: color(), group_id: "lp".into(), parent_frames: vec![], reason: LoopTerminationReason::OverExhausted, at_unix: 2 },
        ] { assert!(!source.apply(&row).rejected()); }
        let members = BTreeSet::from([Located::top("lp__in"), Located::top("step"), Located::top("lp__out")]);
        let eligible = weft_core::seeding::inheritable_nodes(&project, source.snapshot());
        assert!(members.iter().all(|node| eligible.contains(node)));
        let mut selection = weft_core::project::selection::RunSelection::carve(&project,
            &weft_core::project::selection::SelectionBounds { from: vec!["sink".into()], ..Default::default() }).unwrap();
        selection.suppliers.insert(Located::top("lp__out"));
        let mut birth = started_execution();
        if let ExecEvent::ExecutionStarted { subgraph, .. } = &mut birth { *subgraph = Some(selection); }
        let mut child = Fold::new(Uuid::new_v4(), project);
        child.apply(&birth);
        child.inherit(&source, &members).unwrap();
        assert!(!child.snapshot().executions.contains_key("step"));
        assert!(!child.snapshot().executions.contains_key("lp__out"));
        let result = pending(child.snapshot(), "sink").into_iter().find(|pulse| pulse.target_port == "res").unwrap();
        assert_eq!(result.value.as_ref(), &json!([]));
        assert!(child.snapshot().loop_runtime.iter().all(|(_, instance)| instance.launched.is_empty()));
    }

    /// A failed loop closes its outward surface from the terminal row;
    /// the boundary's own failure row then adds nothing.
    #[test]
    fn a_failed_loop_closes_its_outward_ports_once() {
        let mut events = loop_prefix();
        events.push(ExecEvent::LoopTerminated {
            color: color(),
            group_id: "lp".into(),
            parent_frames: vec![],
            reason: LoopTerminationReason::Failed,
            at_unix: 9,
        });
        events.push(ExecEvent::NodeFailed {
            color: color(),
            node_id: "lp__in".into(),
            frames: vec![],
            error: "boom".into(),
            at_unix: 9,
        });
        let snap = fold_to_snapshot(color(), loop_project(false), &events);
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
            ExecEvent::NodeFailed {
                color: color(),
                node_id: "lp__in".into(),
                frames: vec![],
                error: "config".into(),
                at_unix: 1,
            },
        ];
        let snap = fold_to_snapshot(color(), loop_project(false), &events);
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
            started("step", vec![frame(2)], 2),
            emitted(Uuid::new_v4(), "step", vec![frame(2)], "res", json!(300)),
            completed("step", vec![frame(2)], 2),
            started("lp__out", vec![frame(2)], 3),
            loop_row_out_fired(2),
            completed("lp__out", vec![frame(2)], 3),
        ]);
        let snap = fold_to_snapshot(color(), loop_project(true), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let inst = snap.loop_runtime.get(&lp_key()).expect("instance");
        assert_eq!(inst.launched, vec![0, 1, 2]);
        assert_eq!(inst.out_fired, vec![2]);
        let lanes: HashSet<u32> = pending(&snap, "step").iter().map(|p| p.frames[0].loop_index().expect("a loop frame")).collect();
        assert_eq!(lanes, [0, 1].into_iter().collect(), "lanes 0 and 1 still wait; lane 2 absorbed");
        // A closed write records as Closed, a null slot in the list.
        assert_eq!(inst.gather_lists["res"][&2], weft_core::exec::loop_runtime::LoopWrite::Value(Arc::new(json!(300))));
    }

    #[test]
    fn kicked_payload_survives_dispatch_for_resume() {
        let payload = json!({"body": "hello"});
        let events = vec![
            started_execution(),
            ExecEvent::NodeKicked {
                color: color(),
                node_id: "src".into(), frames: vec![],
                firing: true,
                payload: Some(payload.clone()),
                port_snapshot: None,
                at_unix: 0,
            },
            started("src", vec![], 0),
        ];
        let snap = fold_to_snapshot(color(), fan_out_project(), &events);
        let kick = snap.kicked.get(&FiringLocation::new("src", Vec::new())).expect("kick survives fold");
        assert!(kick.dispatched, "NodeStarted at root frames consumed the kick");
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
    /// the resume clears the suspension and hands the value to the
    /// bridge.
    #[test]
    fn lifecycle_one_record_per_frames() {
        let token = "tok-1".to_string();
        let events = vec![
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(42)),
            completed("src", vec![], 0),
            started("a", vec![], 0),
            ExecEvent::NodeSuspended { color: color(), node_id: "a".into(), frames: vec![], token: token.clone(), at_unix: 0 },
            ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "a".into(),
                frames: vec![],
                token: token.clone(),
                spec: make_spec(),
                call_index: 0,
                at_unix: 0,
            },
            ExecEvent::SuspensionResolved { color: color(), token: token.clone(), value: json!("approved"), at_unix: 0 },
            ExecEvent::NodeResumed { color: color(), node_id: "a".into(), frames: vec![], token: Some(token.clone()), at_unix: 0 },
            completed("a", vec![], 0),
        ];
        let mut fold = Fold::new(color(), fan_out_project());
        let mut resumed = None;
        for ev in &events {
            let effects = fold.apply(ev);
            if let Some(v) = effects.resumed_value {
                resumed = Some(v);
            }
        }
        let snap = fold.into_snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let execs = &snap.executions["a"];
        assert_eq!(execs.len(), 1, "one record per (node, frames)");
        assert_eq!(execs[0].status, NodeExecutionStatus::Completed);
        assert!(snap.suspensions.is_empty(), "suspensions cleared after resume+complete");
        assert!(snap.pending_deliveries.is_empty());
        assert_eq!(resumed, Some(json!("approved")));
        let seq = &snap.awaited_sequences[&FiringLocation::new("a", vec![])];
        assert!(matches!(&seq[0].kind, AwaitedEntryKind::Await { resolved: Some(v), .. } if v == &json!("approved")));
    }

    /// A resume absorbs the pulses that arrived while the node waited,
    /// so a later refold never re-fires a completed node over them.
    #[test]
    fn node_resumed_absorbs_pulses() {
        let events = vec![
            started("a", vec![], 0),
            ExecEvent::NodeSuspended { color: color(), node_id: "a".into(), frames: vec![], token: "t".into(), at_unix: 0 },
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(1)),
            ExecEvent::NodeResumed { color: color(), node_id: "a".into(), frames: vec![], token: None, at_unix: 0 },
        ];
        let snap = fold_to_snapshot(color(), fan_out_project(), &events);
        let a = &snap.pulses["a"][0];
        assert_eq!(a.status, PulseStatus::Absorbed);
        assert_eq!(snap.executions["a"][0].pulses_absorbed, vec![a.id]);
        assert_eq!(snap.executions["a"][0].status, NodeExecutionStatus::Running);
    }

    /// A second firing at a terminal location opens a second record;
    /// its sweep closes a second time with its own pulse ids.
    #[test]
    fn node_started_after_terminal_opens_new_record_and_sweeps_apart() {
        let events = vec![
            started("src", vec![], 0),
            completed("src", vec![], 1),
            started("a", vec![], 2),
            completed("a", vec![], 3),
            started("a", vec![], 4),
            completed("a", vec![], 5),
        ];
        let snap = fold_to_snapshot(color(), fan_out_project(), &events);
        assert_eq!(snap.executions["a"].len(), 2);
        assert!(snap.executions["a"].iter().all(|e| e.status == NodeExecutionStatus::Completed));
    }

    /// A stream take removes the pulse from the table (no tombstone),
    /// and the stream's end is durable on the loop instance.
    #[test]
    fn output_review_keeps_every_stream_item_and_closure_without_a_consumer() {
        let project = project(vec![node("gen", "T", &[], &[("rows", "Generator[Number]")], &[], Value::Null, Value::Null)], vec![]);
        let mut fold = Fold::new(color(), project).with_output_history();
        for event in [
            started_execution(), started("gen", vec![], 1),
            emitted(Uuid::new_v4(), "gen", vec![], "rows", json!(10)),
            emitted(Uuid::new_v4(), "gen", vec![], "rows", json!(20)),
            ExecEvent::PortClosed { color: color(), emission_id: Uuid::new_v4(), node_id: "gen".into(), frames: vec![], port: "rows".into(), provided: false, at_unix: 2 },
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
        let item1 = weft_core::exec::emission::pulse_id(e1, "rows", "take", "rows", false);
        let events = vec![
            started("gen", vec![], 0),
            emitted(e1, "gen", vec![], "rows", json!(1)),
            started("take", vec![], 0),
            emitted(e2, "gen", vec![], "rows", json!(2)),
            ExecEvent::PulsesConsumed {
                color: color(),
                node_id: "take".into(),
                frames: vec![],
                pulse_ids: vec![item1.to_string()],
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), project, &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let bucket = &snap.pulses["take"];
        assert_eq!(bucket.len(), 1, "the taken item is gone, the second item stays");
        assert_eq!(*bucket[0].value, json!(2));
        assert_eq!(bucket[0].status, PulseStatus::Pending, "a running consumer's items are not absorbed by its start");
        assert!(snap.executions["take"][0].pulses_absorbed.is_empty());
    }

    #[test]
    fn suspension_resolved_before_registered_still_resolves() {
        let events = vec![
            ExecEvent::SuspensionResolved { color: color(), token: "t".into(), value: json!(5), at_unix: 0 },
            ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "a".into(),
                frames: vec![],
                token: "t".into(),
                spec: make_spec(),
                call_index: 0,
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), fan_out_project(), &events);
        let seq = &snap.awaited_sequences[&FiringLocation::new("a", vec![])];
        assert!(matches!(&seq[0].kind, AwaitedEntryKind::Await { resolved: Some(v), .. } if v == &json!(5)));
    }


    /// The birth selection removes outgoing wires at the cut, so an
    /// excluded boundary receives no pulses and opens no record.
    #[test]
    fn a_run_subgraph_keeps_the_fold_off_the_outside() {
        let project = nested_group_project();
        let mut birth = started_execution();
        if let ExecEvent::ExecutionStarted { subgraph, .. } = &mut birth {
            *subgraph = Some(weft_core::project::selection::RunSelection::carve(&project,
                &weft_core::project::selection::SelectionBounds {
                    target: vec!["src".into()], ..Default::default()
                }).unwrap());
        }
        let events = vec![
            birth,
            kicked("src"),
            started("src", vec![], 0),
            emitted(Uuid::new_v4(), "src", vec![], "out", json!(9)),
            emitted(Uuid::new_v4(), "src", vec![], "flow", json!(true)),
            completed("src", vec![], 1),
        ];
        let mut fold = Fold::new(color(), project);
        let mut outcomes = Vec::new();
        for e in &events {
            outcomes.extend(fold.apply(e).boundaries.into_iter().map(|b| (b.node_id, matches!(b.outcome, BoundaryOutcome::OutOfScope))));
        }
        let snap = fold.into_snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert!(outcomes.is_empty(), "the cut dispatches no excluded boundary");
        assert!(!snap.executions.contains_key("g__in"), "no record outside the run");
        assert!(!snap.pulses.contains_key("g__in"), "no pulse crosses the cut");
        assert!(pending(&snap, "h__in").is_empty(), "nothing forwarded");
    }

    /// Every rejection site names its row kind, and a rejected row
    /// applies nothing.
    #[test]
    fn every_corruption_site_is_reported_by_its_row() {
        let project = loop_project(false);
        let lp = |kind: &str| -> ExecEvent {
            match kind {
                "launched" => loop_row_launched(0),
                "out" => loop_row_out_fired(0),
                "ended" => ExecEvent::LoopStreamEnded {
                    color: color(),
                    group_id: "lp".into(),
                    parent_frames: vec![],
                    end: weft_core::generator::StreamEnd::Finished,
                    at_unix: 0,
                },
                _ => ExecEvent::LoopTerminated {
                    color: color(),
                    group_id: "lp".into(),
                    parent_frames: vec![],
                    reason: LoopTerminationReason::OverExhausted,
                    at_unix: 0,
                },
            }
        };
        let events = vec![
            // A resume, and a suspension, of a firing that never opened.
            ExecEvent::NodeResumed { color: color(), node_id: "step".into(), frames: vec![], token: None, at_unix: 0 },
            ExecEvent::NodeSuspended { color: color(), node_id: "step".into(), frames: vec![], token: "t".into(), at_unix: 0 },
            started("feed", vec![], 0),
            // A second start over a firing still open.
            started("feed", vec![], 0),
            // A close on an undeclared port.
            ExecEvent::PortClosed { color: color(), emission_id: Uuid::new_v4(), node_id: "feed".into(), frames: vec![], port: "nope".into(), provided: false, at_unix: 0 },
            // A take naming a pulse the table does not hold.
            ExecEvent::PulsesConsumed { color: color(), node_id: "feed".into(), frames: vec![], pulse_ids: vec![Uuid::nil().to_string()], at_unix: 0 },
            // Loop rows with no LoopIn firing / no instance.
            loop_row_instantiated(),
            lp("launched"),
            lp("out"),
            lp("ended"),
            lp("terminated"),
        ];
        let snap = fold_to_snapshot(color(), project, &events);
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
        assert_eq!(snap.executions["feed"].len(), 1, "the duplicate start opened nothing");
        assert!(snap.corruptions[0].reason.contains("node_resumed"), "{:?}", snap.corruptions[0]);
        assert!(snap.corruptions[1].reason.contains("node_suspended"), "{:?}", snap.corruptions[1]);
        assert!(snap.corruptions[2].reason.contains("already open"), "{:?}", snap.corruptions[2]);
        assert!(snap.loop_runtime.get(&lp_key()).is_none());
    }

    /// A group boundary never journals a row, so a row naming one is
    /// corruption, on every row kind, and opens nothing: the pass is
    /// the only thing that fires a boundary on either side.
    #[test]
    fn a_row_naming_a_group_boundary_is_corruption() {
        let events = vec![
            started_execution(),
            started("g__in", vec![], 0),
            emitted(Uuid::new_v4(), "g__in", vec![], "x", json!(1)),
            completed("g__in", vec![], 1),
        ];
        let snap = fold_to_snapshot(color(), nested_group_project(), &events);
        let sites: Vec<CorruptionSite> = snap.corruptions.iter().map(|c| c.site).collect();
        assert_eq!(sites, vec![CorruptionSite::NodeLifecycle, CorruptionSite::PortEmitted, CorruptionSite::NodeLifecycle], "{:?}", snap.corruptions);
        assert!(!snap.executions.contains_key("g__in"), "no record opened for the boundary");
        assert!(!snap.pulses.contains_key("h__in"), "nothing forwarded");
    }

    /// Two firings at one location sweep under different emission ids,
    /// so once the consumer took the first closure, the second firing's
    /// closure is a second pulse and not a replay of the first.
    #[test]
    fn a_second_firing_sweeps_a_second_closure() {
        let skipped = |node: &str, at: u64| ExecEvent::NodeSkipped {
            color: color(),
            node_id: node.into(),
            frames: vec![],
            reason: SkipReason::RequiredInputClosed { port: "in".into(), failure: None },
            at_unix: at,
        };
        let events = vec![
            started("src", vec![], 0),
            completed("src", vec![], 1),
            started("a", vec![], 2),
            skipped("a", 2),
            started("src", vec![], 3),
            completed("src", vec![], 4),
        ];
        let snap = fold_to_snapshot(color(), fan_out_project(), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        assert_eq!(snap.executions["src"].iter().map(|e| e.ordinal).collect::<Vec<_>>(), vec![0, 1]);
        let closures: Vec<&weft_core::pulse::Pulse> = snap.pulses["a"].iter().filter(|p| p.closed).collect();
        assert_eq!(closures.len(), 2, "one closure per firing");
        assert_ne!(closures[0].id, closures[1].id);
        assert_eq!(closures[0].status, PulseStatus::Absorbed);
        assert_eq!(closures[1].status, PulseStatus::Pending, "the second firing closes `a` again");
    }

    #[test]
    fn inherited_input_keeps_its_original_backup_and_only_supplies_the_child_frontier() {
        let project = project(
            vec![
                node("a", "T", &[("in", "Number", true)], &[("out", "Number")], &[], Value::Null, Value::Null),
                node("b", "T", &[("in", "Number", true)], &[], &[], Value::Null, Value::Null),
            ], vec![edge("a", "out", "b", "in")],
        );
        let mut original_selection = weft_core::project::selection::RunSelection::carve(&project,
            &weft_core::project::selection::SelectionBounds { target: vec!["a".into()], ..Default::default() }).unwrap();
        original_selection.input.insert(Located::top("a"), [("in".into(), json!(7))].into_iter().collect());
        let mut birth = started_execution();
        if let ExecEvent::ExecutionStarted { subgraph, .. } = &mut birth { *subgraph = Some(original_selection); }
        let mut original = Fold::new(color(), project.clone()).with_output_history();
        let kick = ExecEvent::NodeKicked { color: color(), node_id: "a".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 };
        for row in [birth, kick, started("a", vec![], 1), emitted(Uuid::new_v4(), "a", vec![], "out", json!(9)), completed("a", vec![], 2)] {
            assert!(!original.apply(&row).rejected());
        }
        let mut selection = weft_core::project::selection::RunSelection::carve(&project,
            &weft_core::project::selection::SelectionBounds { from: vec!["b".into()], ..Default::default() }).unwrap();
        selection.suppliers.insert(Located::top("a"));
        selection.input.insert(Located::top("b"), [("in".into(), json!(55))].into_iter().collect());
        let mut birth = started_execution();
        if let ExecEvent::ExecutionStarted { subgraph, .. } = &mut birth { *subgraph = Some(selection); }
        let mut child = Fold::new(Uuid::new_v4(), project);
        child.apply(&birth);
        child.inherit(&original, &BTreeSet::from([Located::top("a")])).unwrap();
        child.settle(3);
        let history = child.firing_view("a", &vec![]).unwrap();
        assert_eq!(history.input["in"], json!(7));
        assert_eq!(child.snapshot().executions["a"][0].received.backup_ports, vec!["in"]);
        assert_eq!(child.snapshot().executions["a"][0].inherited_from, Some(original.color()));
        assert!(!child.snapshot().pulses.contains_key("a"));
        assert!(child.snapshot().kicked.is_empty());
        assert_eq!(*pending(child.snapshot(), "b")[0].value, json!(9));
        child.apply(&started("b", vec![], 4));
        assert_eq!(child.firing_view("b", &vec![]).unwrap().input["in"], json!(9));
    }

    /// A LoopOut row is a vote only once its writes landed: the same
    /// row again (a firing the runtime already holds) records nothing
    /// and carries no vote for the screen.
    #[test]
    fn a_loop_out_row_votes_once_its_writes_landed() {
        let mut events = loop_prefix();
        events.extend(vec![
            started("step", vec![frame(0)], 2),
            emitted(Uuid::new_v4(), "step", vec![frame(0)], "acc", json!(11)),
            emitted(Uuid::new_v4(), "step", vec![frame(0)], "res", json!(100)),
            completed("step", vec![frame(0)], 2),
            started("lp__out", vec![frame(0)], 3),
        ]);
        let mut fold = Fold::new(color(), loop_project(false));
        for e in &events {
            fold.apply(e);
        }
        let first = fold.apply(&loop_row_out_fired(0));
        assert_eq!(first.loop_out_done, Some(None), "the body cast no vote, and that is the row's effect");
        let again = fold.apply(&loop_row_out_fired(0));
        assert_eq!(again.loop_out_done, None, "a firing already held records nothing");
        assert!(fold.snapshot().corruptions.is_empty(), "{:?}", fold.snapshot().corruptions);
        assert_eq!(fold.snapshot().loop_runtime.get(&lp_key()).unwrap().out_fired, vec![0]);
    }

    /// The dispatcher's cancel row ends the firing and sweeps what it
    /// never mentioned, like any other terminal.
    #[test]
    fn a_cancel_row_sweeps_the_unmentioned_ports() {
        let events = vec![
            started("src", vec![], 0),
            ExecEvent::NodeCancelled { color: color(), node_id: "src".into(), frames: vec![], reason: "stopped".into(), at_unix: 1 },
        ];
        let snap = fold_to_snapshot(color(), fan_out_project(), &events);
        assert_eq!(snap.executions["src"][0].status, NodeExecutionStatus::Cancelled);
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
        let mut fold = Fold::new(color(), project);
        for e in &events {
            fold.apply(e);
        }
        assert!(fold.snapshot().corruptions.is_empty(), "{:?}", fold.snapshot().corruptions);
        assert_eq!(fold.output_of("src", &vec![]), Some(json!({ "out": 1, "other": 2 })), "two emissions merge into one output");
        assert_eq!(fold.output_of("a", &vec![]), None, "a firing that emitted nothing has no output");
        let view = fold.firing_view("a", &vec![]).expect("a fired");
        assert_eq!(view.input, json!({ "in": 1, "also": 2 }));
        assert!(view.closed_ports.is_empty());
        assert_eq!(fold.final_outputs(), json!({ "src": { "out": 1, "other": 2 } }));
    }
}
