//! Journal rows to the events the editor paints.
//!
//! The journal holds facts; the editor's wire shape
//! (`DispatcherEvent`) carries what the screen shows: a firing's input
//! bag, its output, the ports that arrived closed, a group opening or
//! not running. None of that is on the rows any more (see
//! `weft_journal::events`), so the projection keeps ONE fold per open
//! run, applies each row as it lands, and reads the derived
//! facts back off the fold. The live bridge and the replay endpoint
//! both come through here, so live and replay cannot drift.

use std::sync::Arc;

use weft_core::frames::Located;
use weft_core::primitive::LoopInstanceKey;
use weft_core::project::loop_in_id;
use weft_core::ProjectDefinition;
use std::collections::BTreeMap;

use weft_core::record_ref::{Position, Shown};
use weft_journal::{JournalRow, RunEvent, Fold, FoldEffects, SeedChain};

/// An event as the projection paints it: every value it names held with
/// where the record keeps it (`weft_core::record_ref::Shown`), sent by
/// reference or with the values written in.
pub type Painted = weft_core::live_event::DispatcherEvent<Shown>;
type DispatcherEvent = Painted;

/// What a reader that folds a run's journal found when it went for
/// the run's program.
pub enum ProgramLookup {
    Found(Arc<ProjectDefinition>),
    /// The run has no program at all: a node self-test (its birth
    /// row pins no definition hash), or a run that never started. A
    /// node row on such a run is a writer bug.
    NoProgram,
    /// The journal cannot be read back: the birth row no longer
    /// decodes, the owner row is gone, or the stored definition no
    /// longer parses. `weft clean` removes the run.
    Unreadable(String),
    /// The run is readable and the code it ran is not recorded any
    /// more, so nothing a fold would derive can be shown. Its own state
    /// because it is the one a person meets: the rows are all there and
    /// every value is missing, which looks exactly like a broken viewer
    /// until somebody says otherwise.
    ProgramGone(String),
}

impl ProgramLookup {
    /// The program to fold with, when there is one.
    pub fn program(&self) -> Option<Arc<ProjectDefinition>> {
        match self {
            Self::Found(p) => Some(p.clone()),
            Self::NoProgram | Self::Unreadable(_) | Self::ProgramGone(_) => None,
        }
    }

    /// The program, or why `run_id` has none, in words for a person.
    pub fn or_why(&self, run_id: weft_core::RunId) -> Result<Arc<ProjectDefinition>, String> {
        match self {
            Self::Found(p) => Ok(p.clone()),
            Self::NoProgram => Err(format!("run {run_id} ran no program")),
            Self::Unreadable(reason) | Self::ProgramGone(reason) => Err(reason.clone()),
        }
    }

    /// Why this run cannot be painted whole, and where the reason
    /// belongs on screen. `None` when it can (or when it has no
    /// program to miss). A run's projection paints this with the first
    /// row it projects ([`RunProjector`]); a run that paints empty for no
    /// stated reason is the bug that sends people hunting through the
    /// viewer.
    pub fn unpaintable(&self) -> Option<(weft_core::primitive::CorruptionSite, &str)> {
        use weft_core::primitive::CorruptionSite;
        match self {
            Self::Unreadable(reason) => Some((CorruptionSite::UndecodableRow, reason)),
            Self::ProgramGone(reason) => Some((CorruptionSite::MissingProgram, reason)),
            Self::Found(_) | Self::NoProgram => None,
        }
    }
}

/// One run's live projection: its fold plus the project id the
/// events are attributed to. The fold SHOWS the run
/// (`weft_journal::Fold::showing`): a firing's values are held while its
/// events are painted and dropped once it ended, so a projection holds
/// what is open, never the whole run.
pub struct RunProjector {
    /// `None` for a run painted without a fold: only what needs no
    /// program is painted, and its node rows are skipped. Quietly for a
    /// run whose program is gone or unreadable (the projection says why,
    /// [`Self::untold`]); with a warning for a run that never had one,
    /// where a node row is a writer bug.
    fold: Option<Fold>,
    /// Whether the run has a reason it cannot be painted whole, said or
    /// not yet: a node row with no fold is then skipped quietly.
    unpaintable: bool,
    /// Why the run cannot be painted whole, not said yet: painted with the
    /// first row projected, at that row's place, so every reader of the
    /// run paints it at the same place and a follower holding that row
    /// already hears it once.
    untold: Option<(weft_core::primitive::CorruptionSite, String)>,
    run_id: weft_core::RunId,
    project_id: uuid::Uuid,
    /// What a seeded run inherits, imported and painted when its birth row
    /// is projected, then [`Inherited::Nothing`].
    inheritance: Inherited,
}

/// What a run's projection does with its inheritance at its birth row.
/// Whatever it is, the run's own fold keeps going and its own rows paint:
/// what its stand-ins send is on its birth (`RunStarted.given`), so it
/// needs nothing of its ancestors to fold its own rows.
pub enum Inherited {
    /// Nothing to inherit: a run with no seed, or no program.
    Nothing,
    /// Read ([`read_inheritance`]): imported into the fold, and its
    /// ancestors' rows painted right after the birth row, each marked with
    /// the run it came from.
    Read(Inheritance),
    /// The seed chain could not be read (its seed was cleaned): nothing is
    /// imported, and the birth row paints why, once.
    ChainUnreadable(String),
    /// The chain was read and the ancestors could not be reconstructed:
    /// the birth row paints why, once.
    Failed(String),
}

/// What a seeded run inherits, read before its own record is painted
/// ([`read_inheritance`]): its ancestors reconstructed, each keeping what
/// the runs after it take from it, and what the run shows of their rows,
/// painted. Its projection imports from the first and paints the second at
/// its birth.
pub struct Inheritance {
    sources: Arc<BTreeMap<weft_core::RunId, Fold>>,
    events: Vec<DispatcherEvent>,
}

impl RunProjector {
    pub fn new(run_id: weft_core::RunId, program: ProgramLookup, project_id: uuid::Uuid) -> Self {
        let untold = program.unpaintable().map(|(site, reason)| (site, reason.to_string()));
        Self {
            fold: program.program().map(|p| Fold::new(run_id, p).showing()),
            unpaintable: untold.is_some(),
            untold,
            run_id,
            project_id,
            inheritance: Inherited::Nothing,
        }
    }

    /// What this run inherits ([`Inherited`]). Painted when the birth row
    /// is projected: every inherited row lands on the screen before the
    /// run's own, as it sits in the fold, so a seeded run reads whole
    /// instead of as three nodes with values and twelve empty ones.
    /// The project the run's events are attributed to.
    pub fn project_id(&self) -> uuid::Uuid {
        self.project_id
    }

    pub fn with_inheritance(mut self, inheritance: Inherited) -> Self {
        self.inheritance = inheritance;
        self
    }

    /// Apply one row and answer the events it paints. One row can
    /// project to MANY events: a row that decides a scope paints the
    /// decision too, and a value carrying a bus marker paints both ends
    /// of every wire it rode as participants.
    /// A row the fold rejected paints its corruption and nothing else:
    /// whatever the fold holds at its location is an earlier firing's,
    /// and a made-up event would contradict the corruption. The same
    /// event reaches the live screen and the replay, at the row.
    pub fn project(&mut self, row: &JournalRow) -> Vec<DispatcherEvent> {
        let ev = &row.event;
        let (run_id, project_id) = (self.run_id, self.project_id);
        let mut out: Vec<DispatcherEvent> =
            self.untold.take().map(|(site, reason)| DispatcherEvent::JournalCorruption { run_id, project_id, site, reason }).into_iter().collect();
        out.extend(self.project_row(ev, row.position()));
        if let RunEvent::RunStarted { seed: Some(seed), at_unix, .. } = ev {
            let inheritance = std::mem::replace(&mut self.inheritance, Inherited::Nothing);
            if let Some(fold) = &mut self.fold {
                let failed = match inheritance {
                    Inherited::Nothing => None,
                    Inherited::ChainUnreadable(reason) => {
                        out.push(DispatcherEvent::JournalCorruption {
                            run_id: self.run_id, project_id: self.project_id, site: weft_core::primitive::CorruptionSite::UndecodableRow, reason,
                        });
                        None
                    }
                    Inherited::Failed(reason) => Some(reason),
                    Inherited::Read(inheritance) => match weft_journal::seed::import_origins(fold, seed, &inheritance.sources, *at_unix) {
                        Ok(effects) => {
                            out.extend(inheritance.events);
                            out.extend(effect_events(&effects, self.run_id, self.project_id, *at_unix));
                            None
                        }
                        Err(error) => Some(format!("{error:#}")),
                    },
                };
                if let Some(reason) = failed {
                    out.push(DispatcherEvent::JournalCorruption {
                        run_id: self.run_id, project_id: self.project_id,
                        site: weft_core::primitive::CorruptionSite::NodeLifecycle, reason: format!("cannot reconstruct seed history: {reason}"),
                    });
                }
            }
        }
        out
    }

    /// One of this run's own rows, at `at` in its record, through its
    /// fold and onto the screen.
    fn project_row(&mut self, ev: &RunEvent, at: Position) -> Vec<DispatcherEvent> {
        let inherited_from = None;
        let project_id = self.project_id;
        let run_id = self.run_id;
        let at_unix = ev.at_unix();
        let effects = match self.fold.as_mut() {
            Some(fold) => fold.apply_at(ev, Some(at)),
            None => {
                if needs_program(ev) {
                    if !self.unpaintable {
                        tracing::warn!(
                            target: "weft_dispatcher::projection",
                            %run_id, kind = ev.kind_str(),
                            "a node row on a run with no program; nothing to paint it from"
                        );
                    }
                    return Vec::new();
                }
                FoldEffects::default()
            }
        };
        let mut out = if effects.rejected() {
            effects
                .rejections
                .iter()
                .map(|c| DispatcherEvent::JournalCorruption {
                    run_id,
                    project_id,
                    site: c.site,
                    reason: c.reason.clone(),
                })
                .collect()
        } else { match ev {
            RunEvent::RunStarted { entry_node, seed, phase, instance, .. } => {
                vec![DispatcherEvent::RunStarted {
                    run_id, instance: instance.clone(), at_unix,
                    entry_node: entry_node.clone(),
                    phase: *phase,
                    subgraph: self.fold.as_ref().map(|fold| weft_core::project::places::running_places(fold.project()).into_iter().collect()),
                    seed: seed.clone(),
                    project_id,
                }]
            }
            RunEvent::NodeStarted { node_id, frames, .. } => {
                let view = self.view(node_id, frames);
                vec![DispatcherEvent::NodeStarted {
                    run_id, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    input: view.input,
                    closed_ports: view.closed_ports,
                    provided_ports: view.provided_ports,
                    inherited_ports: view.inherited_ports,
                    inherited_from,
                    project_id,
                }]
            }
            RunEvent::NodeCompleted { node_id, frames, .. } => {
                let output = self.fold.as_ref().and_then(|f| f.output_of(node_id, frames));
                vec![DispatcherEvent::NodeCompleted {
                    run_id, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    output,
                    inherited_from,
                    project_id,
                }]
            }
            RunEvent::NodeFailed { node_id, frames, error, .. } => {
                vec![DispatcherEvent::NodeFailed {
                    run_id, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    error: error.clone(),
                    inherited_from,
                    project_id,
                }]
            }
            RunEvent::NodeSkipped { node_id, frames, reason, .. } => {
                let view = self.view(node_id, frames);
                vec![DispatcherEvent::NodeSkipped {
                    run_id, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    closed_ports: view.closed_ports,
                    reason: Some(reason.clone()),
                    inherited_from,
                    project_id,
                }]
            }
            RunEvent::NodeSuspended { node_id, frames, token, .. } => {
                vec![DispatcherEvent::NodeSuspended {
                    run_id, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    token: token.clone(),
                    inherited_from,
                    project_id,
                }]
            }
            RunEvent::NodeResumed { node_id, frames, token, .. } => {
                vec![DispatcherEvent::NodeResumed {
                    run_id, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    token: token.clone(),
                    inherited_from,
                    project_id,
                }]
            }
            // A wait ends on the row that answers, skips or gives it up,
            // or on its registration when that ending was on record first;
            // the fold says which row ended it, and a row that came after
            // the first ending paints nothing.
            RunEvent::SuspensionRegistered { .. }
            | RunEvent::SuspensionResolved { .. }
            | RunEvent::SuspensionSkipped { .. }
            | RunEvent::SuspensionGaveUp { .. } => effects.wait_ended.iter().map(|ended| DispatcherEvent::WaitEnded {
                run_id, at_unix,
                node: ended.at.node_id.clone(),
                frames: ended.at.frames.clone(),
                token: ended.token.clone(),
                ended: ended.end.clone().map(|answer| Shown::of(ended.answer_kept.as_ref(), &Arc::new(answer))),
                inherited_from,
                project_id,
            }).collect(),
            RunEvent::NodeCancelled { node_id, frames, reason, .. } => {
                vec![DispatcherEvent::NodeCancelled {
                    run_id, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    reason: reason.clone(),
                    inherited_from,
                    project_id,
                }]
            }
            RunEvent::RunCompleted { .. } => {
                let outputs = self.fold.as_ref().map(|f| f.final_outputs());
                vec![DispatcherEvent::RunCompleted { run_id, at_unix, outputs, project_id }]
            }
            RunEvent::RunFailed { error, .. } => {
                // No truncation: the live view publishes it on this
                // process only (no NOTIFY hop); the full error is what
                // the operator wants for debugging. Truncation belongs at
                // NOTIFY producer sites (api/project.rs), not here.
                vec![DispatcherEvent::RunFailed {
                    run_id, at_unix,
                    error: error.clone(),
                    project_id,
                }]
            }
            RunEvent::RunCancelled { reason, cause, .. } => {
                vec![DispatcherEvent::RunCancelled {
                    run_id, at_unix,
                    reason: reason.clone(),
                    cause: cause.clone(),
                    project_id,
                }]
            }
            RunEvent::RunTagged { tags, .. } => {
                vec![DispatcherEvent::RunTagged {
                    run_id, at_unix,
                    tags: tags.clone(),
                    project_id,
                }]
            }
            RunEvent::CostReported {
                node_id, frames, cost_id, service, amount_usd, origin, ..
            } => {
                vec![DispatcherEvent::CostReported {
                    run_id, at_unix,
                    inherited_from: None,
                    project_id,
                    node_id: node_id.clone(),
                    frames: frames.clone(),
                    cost_id: cost_id.clone(),
                    service: service.clone(),
                    amount_usd: *amount_usd,
                    origin: origin.clone(),
                }]
            }
            RunEvent::FileEdited { node_id, frames, edit, .. } => {
                vec![DispatcherEvent::FileEdited {
                    run_id, at_unix,
                    inherited_from: None,
                    project_id,
                    node_id: node_id.clone(),
                    frames: frames.clone(),
                    edit: edit.clone(),
                }]
            }
            // Bus events: surfaced so the inspector renders a live IRC-style
            // log per bus. The webview groups by `bus_id` and renders ONE
            // panel per bus on every node listed as a `BusParticipant`.
            RunEvent::BusJoined { bus_id, offset, name, .. } => {
                vec![DispatcherEvent::BusJoined {
                    run_id,
                    project_id,
                    bus_id: bus_id.clone(),
                    offset: *offset,
                    name: name.clone(),
                    at_unix,
                }]
            }
            RunEvent::BusLeft { bus_id, offset, name, .. } => {
                vec![DispatcherEvent::BusLeft {
                    run_id,
                    project_id,
                    bus_id: bus_id.clone(),
                    offset: *offset,
                    name: name.clone(),
                    at_unix,
                }]
            }
            // A window's messages are the window itself: its own row, which
            // this event is the one showing of, so they travel inline and
            // still once. The same holds for a caller's window below.
            RunEvent::BusWindow { bus_id, first_offset, last_offset, messages, totals, .. } => {
                vec![DispatcherEvent::BusWindow {
                    run_id,
                    project_id,
                    bus_id: bus_id.clone(),
                    first_offset: *first_offset,
                    last_offset: *last_offset,
                    messages: messages.clone(),
                    totals: totals.clone(),
                    at_unix,
                }]
            }
            RunEvent::BusClosed { bus_id, offset, end, .. } => {
                vec![DispatcherEvent::BusClosed {
                    run_id,
                    project_id,
                    bus_id: bus_id.clone(),
                    offset: *offset,
                    end: end.clone(),
                    at_unix,
                }]
            }
            // Bus participation is derived from the pulses an emission
            // put on the wires: a pulse whose value carries a bus marker
            // means BOTH the producer node AND the consumer node touched
            // that bus. The webview unions these into a per-bus
            // participant set; each participant gets the inspector's IRC
            // panel for that bus. Every row's emissions are sniffed below,
            // the ones the scopes it decided sent included.
            RunEvent::LoopInstantiated { group_id, parent_frames, .. } => {
                let key = LoopInstanceKey {
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    run_id,
                };
                // The cap and the mode are read off the instance the
                // fold just built (a rejected row painted nothing
                // above; a made-up "no cap, sequential" is exactly what
                // a real unbounded loop looks like).
                let inst = self
                    .fold
                    .as_ref()
                    .and_then(|f| f.snapshot().loop_runtime.get(&key))
                    .expect("an applied LoopInstantiated row built its instance");
                vec![DispatcherEvent::LoopInstantiated {
                    run_id, at_unix, project_id,
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    iter_cap: inst.iter_cap,
                    parallel: inst.config.parallel,
                }]
            }
            RunEvent::LoopIterationLaunched { group_id, parent_frames, index, .. } => {
                vec![DispatcherEvent::LoopIterationLaunched {
                    run_id, at_unix, project_id,
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    index: *index,
                }]
            }
            RunEvent::LoopOutFired { group_id, parent_frames, index, .. } => {
                // The vote is read off the fold's LoopOut firing. No
                // vote at all means the runtime already held this
                // firing (a replayed row, or one after the loop ended):
                // nothing is painted, because a made-up "no vote" is
                // exactly what a real no-vote looks like.
                match effects.loop_out_done {
                    Some(done_vote) => vec![DispatcherEvent::LoopOutFired {
                        run_id, at_unix, project_id,
                        group_id: group_id.clone(),
                        parent_frames: parent_frames.clone(),
                        index: *index,
                        done_vote,
                    }],
                    None => Vec::new(),
                }
            }
            RunEvent::LoopTerminated { group_id, parent_frames, reason, .. } => {
                vec![DispatcherEvent::LoopTerminated {
                    run_id, at_unix, project_id,
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    reason: *reason,
                }]
            }
            // Caller events: surfaced 1:1 so the inspector replays the live
            // caller exchange (connected / inbound / outbound / errored /
            // disconnected) the same way it replays a bus. Payloads carry
            // the same tagged `WirePayload` shape as a bus window's
            // messages.
            RunEvent::CallerConnected { offset, protocol, .. } => {
                vec![DispatcherEvent::CallerConnected {
                    run_id, project_id, offset: *offset,
                    protocol: protocol.clone(), at_unix,
                }]
            }
            RunEvent::CallerWindow { first_offset, last_offset, messages, totals, dropped, .. } => {
                vec![DispatcherEvent::CallerWindow {
                    run_id, project_id,
                    first_offset: *first_offset,
                    last_offset: *last_offset,
                    messages: messages.clone(),
                    totals: totals.clone(),
                    dropped: dropped.clone(),
                    at_unix,
                }]
            }
            RunEvent::CallerErrored { offset, message, .. } => {
                vec![DispatcherEvent::CallerErrored {
                    run_id, project_id, offset: *offset,
                    message: message.clone(), at_unix,
                }]
            }
            RunEvent::CallerDisconnected { offset, reason, dropped, .. } => {
                vec![DispatcherEvent::CallerDisconnected {
                    run_id, project_id, offset: *offset,
                    reason: reason.clone(), dropped: dropped.clone(), at_unix,
                }]
            }
            // LogLine / RunOutput / NodeKicked / PulsesConsumed /
            // LoopStreamEnded / ChargeOpened (its cost is surfaced when
            // a `CostReported` books it), and a port's value or closure
            // (its bus participants are sniffed below):
            // not surfaced through DispatcherEvent. SSE consumers don't
            // need them for live UI; they read the journal directly when
            // they want full detail. Any row can still decide a scope,
            // painted below.
            _ => Vec::new(),
        } };
        // A scope's decision never has a row of its own: what the fold
        // decided after this row is painted as the decision the editor
        // reads the group's state from, and what anything sent is sniffed
        // for bus participants.
        if !effects.rejected() {
            out.extend(effect_events(&effects, run_id, project_id, at_unix));
        }
        out
    }

    fn view(&self, node_id: &str, frames: &weft_core::frames::LoopFrames) -> weft_journal::FiringView {
        self.fold
            .as_ref()
            .and_then(|f| f.firing_view(node_id, frames))
            .unwrap_or(weft_journal::FiringView {
                input: Default::default(),
                closed_ports: Vec::new(),
                provided_ports: Vec::new(),
                inherited_ports: Default::default(),
            })
    }
}

/// What a row did besides its own facts, painted: every copy of a scope
/// it decided, and the bus participants of every pulse it put on a wire.
fn effect_events(effects: &FoldEffects, run_id: weft_core::RunId, project_id: uuid::Uuid, at_unix: u64) -> Vec<DispatcherEvent> {
    let mut out: Vec<DispatcherEvent> = effects.scopes.iter().map(|(copy, state)| DispatcherEvent::ScopeDecided {
        run_id,
        project_id,
        scope: copy.scope.clone(),
        frames: copy.frames.clone(),
        state: state.clone(),
        inherited_from: None,
        at_unix,
    }).collect();
    out.extend(sniff_emissions(run_id, project_id, effects));
    out
}

/// Read what a seeded run inherits, its seed `seed` and its chain `chain`
/// in hand: the ancestors reconstructed (`SeedChain::materialize`), then
/// each origin's rows painted under its own program, including their
/// suspension and resume history. Every record is read a page at a time
/// through `page` (one page of a run's record after a row,
/// `crate::store::decoded_page`). These rows never enter the child's fold.
///
/// `child_program` is the program the child runs: a scope it holds is the
/// child's own fold's to decide and paint, so no ancestor's decision of it
/// is painted.
pub async fn read_inheritance<P, Fut>(
    page: &P,
    chain: &SeedChain,
    seed: &weft_journal::events::Seed,
    child_program: Arc<ProjectDefinition>,
    child: weft_core::RunId,
    project_id: uuid::Uuid,
) -> anyhow::Result<Inheritance>
where
    P: Fn(weft_core::RunId, Option<i32>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Vec<JournalRow>>>,
{
    let sources = Arc::new(chain.materialize(page).await?);
    let seed = Arc::new(seed.clone());
    let mut events = Vec::new();
    for ancestor in chain.ancestors.iter().filter(|ancestor| seed.origins.values().any(|origin| *origin == ancestor.run_id)) {
        let mut painting = AncestorPainting {
            ancestor: ancestor.run_id,
            project: ancestor.project.clone(),
            source: RunProjector::new(ancestor.run_id, ProgramLookup::Found(ancestor.project.clone()), project_id),
            index: None,
        };
        let mut after = None;
        loop {
            let rows = page(ancestor.run_id, after).await?;
            let Some(last) = rows.last().map(|row| row.seq) else { break };
            let (sources, seed, child_program) = (sources.clone(), seed.clone(), child_program.clone());
            (painting, events) = weft_journal::on_blocking_pool(move || -> anyhow::Result<_> {
                for row in &rows {
                    painting.paint(row, &sources, &seed, &child_program, child, project_id, &mut events)?;
                }
                Ok((painting, events))
            })
            .await?;
            after = Some(last);
        }
    }
    Ok(Inheritance { sources, events })
}

/// One ancestor's rows being painted for a run seeded from it
/// ([`read_inheritance`]): its projection, under its own program.
struct AncestorPainting {
    ancestor: weft_core::RunId,
    project: Arc<ProjectDefinition>,
    source: RunProjector,
    /// Its program's index, built the first time a scope decision needs it.
    index: Option<weft_core::project::ProgramIndex>,
}

impl AncestorPainting {
    /// Paint `row`, the ancestor's next, onto `out`: what of it sits at a
    /// place the child reuses from this ancestor, as the child's.
    #[allow(clippy::too_many_arguments)]
    fn paint(
        &mut self,
        row: &JournalRow,
        sources: &BTreeMap<weft_core::RunId, Fold>,
        seed: &weft_journal::events::Seed,
        child_program: &ProjectDefinition,
        child: weft_core::RunId,
        project_id: uuid::Uuid,
        out: &mut Vec<DispatcherEvent>,
    ) -> anyhow::Result<()> {
        let ancestor = self.ancestor;
        let mut events = self.source.project_row(&row.event, row.position());
        if let RunEvent::RunStarted { seed: Some(parent), at_unix, .. } = &row.event {
            let fold = self.source.fold.as_mut().expect("original program supplied");
            let effects = weft_journal::seed::import_origins(fold, parent, sources, *at_unix)?;
            events.extend(effect_events(&effects, ancestor, project_id, *at_unix));
        }
        for mut event in events {
            match &mut event {
                DispatcherEvent::NodeStarted { run_id, node, frames, inherited_from, .. }
                | DispatcherEvent::NodeSuspended { run_id, node, frames, inherited_from, .. }
                | DispatcherEvent::NodeResumed { run_id, node, frames, inherited_from, .. }
                | DispatcherEvent::WaitEnded { run_id, node, frames, inherited_from, .. }
                | DispatcherEvent::NodeCompleted { run_id, node, frames, inherited_from, .. }
                | DispatcherEvent::NodeSkipped { run_id, node, frames, inherited_from, .. } => {
                    if seed.origins.get(&Located::at(node.as_str(), frames)) != Some(&ancestor) { continue; }
                    *run_id = child;
                    *inherited_from = Some(ancestor);
                    out.push(event);
                }
                DispatcherEvent::JournalCorruption { reason, .. } => anyhow::bail!(
                    "original run {} cannot be projected: {reason}", ancestor,
                ),
                DispatcherEvent::LoopInstantiated { run_id, group_id, parent_frames, .. }
                | DispatcherEvent::LoopIterationLaunched { run_id, group_id, parent_frames, .. }
                | DispatcherEvent::LoopOutFired { run_id, group_id, parent_frames, .. }
                | DispatcherEvent::LoopTerminated { run_id, group_id, parent_frames, .. } => {
                    if seed.origins.get(&Located::at(loop_in_id(group_id), parent_frames)) != Some(&ancestor) { continue; }
                    *run_id = child;
                    out.push(event);
                }
                // A copy whose every node this run reuses from the
                // ancestor, and that this run's program does not
                // hold (its cut kept nothing inside it), was decided
                // there: shown as it was.
                DispatcherEvent::ScopeDecided { run_id, scope, frames, inherited_from, .. } => {
                    if child_program.groups.iter().any(|group| group.id == *scope) { continue; }
                    let project = &self.project;
                    let index = self.index.get_or_insert_with(|| weft_core::project::ProgramIndex::build(project));
                    let copy = weft_core::project::scopes::ScopeCopy::new(scope.clone(), frames.clone());
                    let inside = index.scopes().inside(&copy);
                    let mut nodes = inside.nodes.iter().map(|(at, frames)| Located::at(&project.nodes[*at].id, frames)).peekable();
                    if nodes.peek().is_none() || !nodes.all(|place| seed.origins.get(&place) == Some(&ancestor)) { continue; }
                    *run_id = child;
                    *inherited_from = Some(ancestor);
                    out.push(event);
                }
                DispatcherEvent::CostReported { run_id, node_id, frames, inherited_from, .. }
                | DispatcherEvent::FileEdited { run_id, node_id, frames, inherited_from, .. } => {
                    if seed.origins.get(&Located::at(node_id.as_str(), frames)) != Some(&ancestor) { continue; }
                    *run_id = child;
                    *inherited_from = Some(ancestor);
                    out.push(event);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Whether a row is about a firing the fold has to know: a row that
/// can only be painted with the program in hand.
fn needs_program(ev: &RunEvent) -> bool {
    !matches!(
        ev,
        RunEvent::RunStarted { .. }
            | RunEvent::RunCompleted { .. }
            | RunEvent::RunFailed { .. }
            | RunEvent::RunCancelled { .. }
            | RunEvent::RunTagged { .. }
            | RunEvent::CostReported { .. }
            | RunEvent::ChargeOpened { .. }
            | RunEvent::FileEdited { .. }
            | RunEvent::LogLine { .. }
            | RunEvent::BusJoined { .. }
            | RunEvent::BusLeft { .. }
            | RunEvent::BusWindow { .. }
            | RunEvent::BusClosed { .. }
            | RunEvent::CallerConnected { .. }
            | RunEvent::CallerWindow { .. }
            | RunEvent::CallerErrored { .. }
            | RunEvent::CallerDisconnected { .. }
    )
}

/// Bus participants from every pulse a row put on the wires.
fn sniff_emissions(run_id: weft_core::RunId, project_id: uuid::Uuid, effects: &FoldEffects) -> Vec<DispatcherEvent> {
    let mut out = Vec::new();
    for e in &effects.emissions {
        out.extend(sniff_bus_participants(run_id, project_id, &e.source_node, &e.pulse.target_node, &e.pulse.value, e.pulse.closed));
    }
    out
}

/// Derive `BusParticipant` events from one emitted pulse if its value
/// carries a bus marker. The rule is "any pulse, however it got on the
/// wire, derives participants".
fn sniff_bus_participants(
    run_id: uuid::Uuid,
    project_id: uuid::Uuid,
    source_node: &str,
    target_node: &str,
    value: &serde_json::Value,
    closed: bool,
) -> Vec<DispatcherEvent> {
    // Closure pulses are structural markers (`value: Null`) and can never
    // carry a bus marker. Bail before the sniff so a future change that
    // puts non-null payloads on closures can't synthesise spurious edges.
    if closed {
        return Vec::new();
    }
    let Some(bus_id) = weft_core::weft_type::WeftType::bus_marker_id(value) else {
        return Vec::new();
    };
    let bus_id = bus_id.to_string();
    // A `None` mode means the marker carries an id but no recognised mode
    // (malformed: every `BusHandle::marker()` always sets both). Log loud
    // and skip rather than defaulting to journaled, which would mislabel
    // the inspector and hide the corruption.
    let Some(mode) = weft_core::weft_type::WeftType::bus_marker_mode(value) else {
        tracing::warn!(
            target: "weft_dispatcher::projection",
            bus_id, %run_id, marker = %value,
            "skip BusParticipant: marker has id but no recognised mode"
        );
        return Vec::new();
    };
    let ephemeral = mode == weft_core::stream_journal::JournalMode::Ephemeral;
    let mut out = vec![DispatcherEvent::BusParticipant {
        run_id,
        project_id,
        bus_id: bus_id.clone(),
        node_id: source_node.to_string(),
        ephemeral,
    }];
    if target_node != source_node {
        out.push(DispatcherEvent::BusParticipant {
            run_id,
            project_id,
            bus_id,
            node_id: target_node.to_string(),
            ephemeral,
        });
    }
    out
}

/// What a seeded run inherits, read off its birth row and the seeds'
/// journals: the chain the projector paints before the run's own rows.
/// Empty for a run with no seed or no program. A seed that cannot be
/// read (cleaned, or its chain loops) is a reason the run paints
/// without its inheritance; the caller publishes it as the run's
/// corruption, naming `weft clean`, and the run's own rows still paint.
pub async fn run_inheritance(
    state: &crate::state::DispatcherState,
    birth: Option<&RunEvent>,
    program: &ProgramLookup,
) -> Result<weft_journal::SeedChain, String> {
    if program.program().is_none() {
        return Ok(weft_journal::SeedChain::default());
    }
    weft_journal::seed_chain(birth, &record_page(state), |project_id, hash| async move {
        state.program(project_id, &hash).await?
            .ok_or_else(|| anyhow::anyhow!("seed definition {hash} is missing from project {project_id}"))
    })
        .await
        .map_err(|e| format!("this run inherits from a seed that cannot be read: {e:#}"))
}

/// How a reader of a seed's ancestors reads a run's record: one page after
/// a row, decoded (`crate::store::decoded_page`).
pub fn record_page<'a>(state: &'a crate::state::DispatcherState) -> impl Fn(weft_core::RunId, Option<i32>) -> futures::future::BoxFuture<'a, anyhow::Result<Vec<JournalRow>>> + 'a {
    move |run_id, after| Box::pin(crate::store::decoded_page(&*state.runs, run_id, after))
}

/// The first event of run `run_id`'s record, its birth: read alone, off
/// the record's first page. `None` for a record with no rows. The outer
/// error is the database; the inner one says the row no longer decodes.
pub async fn run_birth(state: &crate::state::DispatcherState, run_id: weft_core::RunId) -> anyhow::Result<Result<Option<JournalRow>, String>> {
    let Some(first) = state.runs.record_first_row(run_id).await? else { return Ok(Ok(None)) };
    let record = weft_journal::record::RawRecord { rows: vec![first] };
    let rows = weft_journal::on_blocking_pool(move || record.decode(run_id)).await;
    Ok(rows.map(|rows| rows.into_iter().next()))
}

/// Read the selected firings' original logs together with this run's own tail.
pub async fn run_logs(
    state: &crate::state::DispatcherState,
    run_id: weft_core::RunId,
    limit: u32,
) -> anyhow::Result<Vec<crate::store::LogEntry>> {
    let mut logs = crate::store::LogTail::new(limit);
    state.runs.logs_for(run_id, limit).await?.into_iter().for_each(|line| logs.push(line));
    let inherited = async {
        let Some(birth) = run_birth(state, run_id).await?.map_err(anyhow::Error::msg)? else { return Ok(()) };
        let RunEvent::RunStarted { seed: Some(seed), .. } = &birth.event else { return Ok(()) };
        let program = run_program(state, run_id).await?;
        anyhow::ensure!(program.program().is_some(), "inherited logs need the run's original program");
        let chain = run_inheritance(state, Some(&birth.event), &program).await.map_err(anyhow::Error::msg)?;
        inherited_logs(&record_page(state), &chain, seed, &mut logs).await
    }.await;
    if let Err(error) = inherited {
        logs.push(crate::store::LogEntry::corrupt_row(0, format!("cannot read inherited log history: {error:#}")));
    }
    Ok(logs.into_entries())
}

/// Feed `lines` the log lines of the firings a run seeded by `seed` reuses
/// from each of its ancestors (`chain`), each record read a page at a time
/// through `page`. Only the origins are read.
async fn inherited_logs<P, Fut>(page: &P, chain: &SeedChain, seed: &weft_journal::Seed, lines: &mut crate::store::LogTail) -> anyhow::Result<()>
where
    P: Fn(weft_core::RunId, Option<i32>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Vec<JournalRow>>>,
{
    for ancestor in chain.ancestors.iter().filter(|ancestor| seed.origins.values().any(|origin| *origin == ancestor.run_id)) {
        let mut after = None;
        loop {
            let rows = page(ancestor.run_id, after).await?;
            let Some(last) = rows.last().map(|row| row.seq) else { break };
            rows.iter().filter_map(|row| {
                let mut line = crate::store::LogEntry::from_event(&row.event)?;
                if seed.origins.get(&Located::at(line.node.as_deref()?, &line.frames)) != Some(&ancestor.run_id) { return None; }
                line.inherited_from = Some(ancestor.run_id);
                Some(line)
            }).for_each(|line| lines.push(line));
            after = Some(last);
        }
    }
    Ok(())
}

/// Strict reconstruction for seeding and complete output review: the run's
/// fold with every output it handed out (`Fold::with_output_history`), and
/// its ancestors', by run. Its record is read a page at a time.
pub async fn reconstruct_run(
    state: &crate::state::DispatcherState,
    run_id: weft_core::RunId,
) -> anyhow::Result<std::collections::BTreeMap<weft_core::RunId, weft_journal::Fold>> {
    let found = run_program(state, run_id).await?;
    let program = found.program()
        .ok_or_else(|| anyhow::anyhow!("run {run_id} has no readable original program"))?;
    let birth = run_birth(state, run_id).await?.map_err(anyhow::Error::msg)?;
    let chain = run_inheritance(state, birth.as_ref().map(|birth| &birth.event), &found).await.map_err(anyhow::Error::msg)?;
    let sources = Arc::new(chain.materialize(&record_page(state)).await?);
    let live = weft_journal::LiveFold::from(weft_journal::Fold::new(run_id, program).with_output_history(), sources.clone());
    let live = crate::store::fold_record(&*state.runs, run_id, None, live, fold_page(run_id)).await?;
    let fold = live.into_fold()?;
    let mut folds = Arc::try_unwrap(sources).map_err(|_| anyhow::anyhow!("run {run_id}'s fold still holds its ancestors"))?;
    folds.insert(run_id, fold);
    Ok(folds)
}

/// The step folding one page of run `run_id`'s record into its fold
/// (`crate::store::fold_record`): a row that does not decode fails the
/// read, a fold over part of a record rebuilding a run that never was.
pub fn fold_page(run_id: weft_core::RunId) -> impl Fn(&mut weft_journal::LiveFold, weft_journal::record::RawRecord) -> anyhow::Result<()> + Clone + Send + 'static {
    move |live, page| {
        let rows = page.decode(run_id).map_err(anyhow::Error::msg)?;
        live.apply(&rows)
    }
}

/// Where run `run_id` stands, and the program it runs (its cut, for a run
/// of a cut): its record folded on top of what it inherits, without the
/// output history [`reconstruct_run`] keeps for a seed. What a reader of
/// its nodes' states needs.
pub async fn run_snapshot(
    state: &crate::state::DispatcherState,
    run_id: weft_core::RunId,
) -> anyhow::Result<(Arc<ProjectDefinition>, weft_core::primitive::RunSnapshot)> {
    let found = run_program(state, run_id).await?;
    let program = found.program()
        .ok_or_else(|| anyhow::anyhow!("run {run_id} has no readable original program"))?;
    let birth = run_birth(state, run_id).await?.map_err(anyhow::Error::msg)?;
    let chain = run_inheritance(state, birth.as_ref().map(|birth| &birth.event), &found).await.map_err(anyhow::Error::msg)?;
    let live = weft_journal::LiveFold::new(weft_journal::Fold::new(run_id, program.clone()), &chain, &record_page(state)).await?;
    let live = crate::store::fold_record(&*state.runs, run_id, None, live, fold_page(run_id)).await?;
    Ok((program, live.into_snapshot()))
}

/// The program a run runs, for the readers that fold its
/// journal to SHOW or to END it: the definition recorded under the
/// hash its run row pinned. `Err` only for the database
/// itself (a retry can succeed); everything permanent is a
/// `ProgramLookup` variant, so no reader wedges on one run. A RESUME
/// never comes through here: a worker that cannot find its program
/// fails loudly.
pub async fn run_program(
    state: &crate::state::DispatcherState,
    run_id: weft_core::RunId,
) -> anyhow::Result<ProgramLookup> {
    let Some(owner) = state.runs.run_owner(run_id).await? else {
        return Ok(ProgramLookup::NoProgram);
    };
    program_of_owner(state, run_id, &owner).await
}

/// [`run_program`] for a caller that read the run's row already
/// (`owner`, `RunStore::run_owner`).
pub async fn program_of_owner(
    state: &crate::state::DispatcherState,
    run_id: weft_core::RunId,
    owner: &crate::store::RunOwner,
) -> anyhow::Result<ProgramLookup> {
    program_at(state, run_id, owner, owner.definition_hash.as_ref()).await
}

/// The registered program a run's program is (the one it was cut from,
/// or its own: `RunOwner::registered_hash`), for the readers that name
/// its places the way a person does: every place the run ran is one of
/// its places under the same id, and so is every place a cut left out,
/// so a name is read against the whole program the run was started on,
/// whatever its folder holds now. The permanent misses are
/// [`ProgramLookup`] variants, as for [`run_program`].
pub async fn registered_program(
    state: &crate::state::DispatcherState,
    run_id: weft_core::RunId,
    owner: &crate::store::RunOwner,
) -> anyhow::Result<ProgramLookup> {
    program_at(state, run_id, owner, owner.registered_hash()).await
}

/// The program recorded under `hash` for the run `owner` describes.
async fn program_at(
    state: &crate::state::DispatcherState,
    run_id: weft_core::RunId,
    owner: &crate::store::RunOwner,
    hash: Option<&String>,
) -> anyhow::Result<ProgramLookup> {
    let Some(hash) = hash.cloned() else {
        return Ok(ProgramLookup::NoProgram);
    };
    let read = match state.program(owner.project_id, &hash).await {
        Err(e) => match e.downcast::<weft_core::stored::DecodeError>() {
            Ok(why) => {
                return Ok(ProgramLookup::Unreadable(format!(
                    "the definition recorded for run {run_id} no longer reads ({why}). `weft clean {run_id}` \
                     removes the run."
                )))
            }
            Err(store) => return Err(store),
        },
        Ok(read) => read,
    };
    let Some(program) = read else {
        // A definition is kept for as long as any run points at it, so
        // reaching here means it was pruned along with the last run
        // that used it, or this journal predates that rule (until
        // then the history died with the project row and every past
        // run of a removed project went quiet). Either way the rows
        // are there and their values are not derivable, which is a
        // thing to SAY rather than to log where nobody looks.
        tracing::warn!(
            target: "weft_dispatcher::projection",
            %run_id, project_id = %owner.project_id, %hash,
            "no recorded definition for this run's hash; the run is shown without the \
             values its journal would derive"
        );
        return Ok(ProgramLookup::ProgramGone(format!(
            "the code this run ran (project {}, version {hash}) is no longer recorded, so its \
             inputs and outputs cannot be worked out from the journal. What each node did is \
             still here; the values are not. If you still have those files, `weft build` in the \
             project folder registers that program again, under the very hash this run names, and \
             it reads as it did. If you do not, `weft clean {run_id}` removes the run.",
            owner.project_id
        )));
    };
    Ok(ProgramLookup::Found(program))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use weft_journal::RunEvent;

    const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x100);

    /// `event` read at row `seq` of its record.
    fn row(seq: usize, event: &RunEvent) -> JournalRow {
        JournalRow { seq: seq as i32, index: 0, event: event.clone() }
    }

    /// `events` as a record whose every event is a row of its own.
    fn rows_of(events: Vec<RunEvent>) -> Vec<JournalRow> {
        events.iter().enumerate().map(|(seq, event)| row(seq, event)).collect()
    }

    /// A painted event as a reader that asked for the values reads it.
    fn json(event: DispatcherEvent) -> Value {
        let written = event
            .with_values(&mut |at| Err(weft_core::record_ref::Unresolved::Broken(format!("not at hand: {at:?}"))))
            .expect("a projection holds every value it paints");
        serde_json::to_value(written).expect("event json")
    }

    fn run_id() -> weft_core::RunId {
        uuid::Uuid::nil()
    }

    /// `src.out` feeds group `g` (through its input `x` to `inner.in`,
    /// and `inner.out` through its output `y` to `sink.in`), the group's
    /// gate on `src.flow`.
    fn program() -> Arc<ProjectDefinition> {
        let node = |id: &str, inputs: Vec<Value>, outputs: Vec<&str>, scope: Vec<&str>| {
            json!({
                "id": id, "nodeType": "T", "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 },
                "inputs": inputs,
                "outputs": outputs.iter().map(|o| json!({ "name": o, "portType": "Number", "required": true })).collect::<Vec<_>>(),
                "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false, "images": []
            })
        };
        let inp = |name: &str| json!({ "name": name, "portType": "Number", "required": true });
        let port = |name: &str| json!({ "name": name, "portType": "Number", "required": false });
        Arc::new(
            serde_json::from_value(json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "nodes": [
                    node("src", vec![], vec!["out", "flow"], vec![]),
                    node("inner", vec![inp("in")], vec!["out"], vec!["g"]),
                    node("sink", vec![inp("in")], vec![], vec![]),
                ],
                "edges": [
                    { "id": "e0", "source": "src", "sourceHandle": "out", "target": "inner", "targetHandle": "in",
                      "via": [{ "group": "g", "port": "x", "side": "in" }] },
                    { "id": "e1", "source": "src", "sourceHandle": "flow", "target": "g", "targetHandle": "_should_flow" },
                    { "id": "e2", "source": "inner", "sourceHandle": "out", "target": "sink", "targetHandle": "in",
                      "via": [{ "group": "g", "port": "y", "side": "out" }] }
                ],
                "groups": [{ "id": "g", "kind": "group", "nodeIds": ["inner"], "inPorts": [port("x")], "outPorts": [port("y")] }],
                "createdAt": "1970-01-01T00:00:00Z",
                "updatedAt": "1970-01-01T00:00:00Z",
            }))
            .expect("program"),
        )
    }

    /// Only a found program folds; a removed or unreadable one folds
    /// like no program at all (the cancel path relies on this: the
    /// terminal lands, the per-node cancels cannot be derived).
    #[test]
    fn only_a_found_program_is_a_program() {
        use weft_core::primitive::CorruptionSite;
        assert!(ProgramLookup::Found(program()).program().is_some());
        assert!(ProgramLookup::NoProgram.program().is_none());
        assert!(ProgramLookup::Unreadable("bad hash".into()).program().is_none());
        assert!(ProgramLookup::ProgramGone("no longer recorded".into()).program().is_none());
        // And each unpaintable state reports its OWN site, which is what
        // the graph reads to tell "this row is corrupt" from "the code
        // this whole run ran is gone".
        assert!(ProgramLookup::Found(program()).unpaintable().is_none());
        assert!(ProgramLookup::NoProgram.unpaintable().is_none());
        assert_eq!(
            ProgramLookup::Unreadable("bad hash".into()).unpaintable().map(|(site, _)| site),
            Some(CorruptionSite::UndecodableRow)
        );
        assert_eq!(
            ProgramLookup::ProgramGone("gone".into()).unpaintable().map(|(site, _)| site),
            Some(CorruptionSite::MissingProgram)
        );
    }

    /// The row painted nothing but its corruption(s).
    fn only_corruptions(painted: &[DispatcherEvent]) -> bool {
        !painted.is_empty()
            && painted.iter().all(|e| matches!(e, DispatcherEvent::JournalCorruption { .. }))
    }

    fn started(node: &str, at: u64) -> RunEvent {
        started_in(run_id(), node, at)
    }

    fn started_in(run: weft_core::RunId, node: &str, at: u64) -> RunEvent {
        RunEvent::NodeStarted { run_id: run, node_id: node.into(), frames: vec![], at_unix: at }
    }

    fn completed(node: &str, at: u64) -> RunEvent {
        completed_in(run_id(), node, at)
    }

    fn completed_in(run: weft_core::RunId, node: &str, at: u64) -> RunEvent {
        RunEvent::NodeCompleted { run_id: run, node_id: node.into(), frames: vec![], at_unix: at }
    }

    fn emitted(node: &str, port: &str, value: Value) -> RunEvent {
        emitted_in(run_id(), node, port, value)
    }

    fn emitted_in(run: weft_core::RunId, node: &str, port: &str, value: Value) -> RunEvent {
        RunEvent::PortEmitted {
            run_id: run,
            emission_id: uuid::Uuid::new_v4(),
            node_id: node.into(),
            frames: vec![],
            port: port.into(),
            value: Arc::new(value),
            at_unix: 1,
        }
    }

    /// The birth of `run`, inheriting what `seed` names, given `given`.
    fn born(run: weft_core::RunId, seed: Option<weft_journal::events::Seed>, given: weft_core::project::Given) -> RunEvent {
        RunEvent::RunStarted {
            given: Box::new(given),
            run_id: run,
            project_id: PROJECT,
            entry_node: "src".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("h".into()),
            binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
            cut_from: None,
            seed,
            instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            settings: Default::default(),
        }
    }

    /// [`program`] with each of `stand_ins` (a node, the values it sends
    /// by port, the run they come from) standing in for itself, as a seeded
    /// run's cut has it, and what a run of it is given.
    fn standing_in(stand_ins: &[(&str, Value, weft_core::RunId)]) -> (Arc<ProjectDefinition>, weft_core::project::Given) {
        let mut program = program().as_ref().clone();
        let mut given = weft_core::project::Given::default();
        for (node, ports, run) in stand_ins {
            let ports = ports.as_object().expect("ports by name").iter()
                .map(|(port, value)| (port.clone(), weft_core::project::GivenPort::Value { value: value.clone() }))
                .collect();
            given.outputs.insert(
                Located::top(*node),
                weft_core::project::StandIn { ports, origin: weft_core::project::GivenOrigin::Inherited { run: *run } },
            );
        }
        program.given = given.places();
        (Arc::new(program), given)
    }

    /// A program with nothing in it: the cut of a seeded run that reuses
    /// everything it reaches, so runs nothing.
    fn nothing() -> Arc<ProjectDefinition> {
        Arc::new(serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000", "nodes": [], "edges": [], "groups": [],
            "createdAt": "1970-01-01T00:00:00Z", "updatedAt": "1970-01-01T00:00:00Z",
        })).expect("program"))
    }

    /// The rows of a run through the group.
    fn run_rows(flow: bool) -> Vec<RunEvent> {
        let mut rows = vec![
            born(run_id(), None, Default::default()),
            RunEvent::NodeKicked { run_id: run_id(), node_id: "src".into(), frames: vec![], firing: true, payload: None, port_snapshot: None, at_unix: 0 },
            started("src", 1),
            emitted("src", "out", json!(9)),
            emitted("src", "flow", json!(flow)),
            completed("src", 2),
        ];
        if flow {
            rows.extend(vec![
                started("inner", 3),
                emitted("inner", "out", json!(10)),
                completed("inner", 4),
                started("sink", 5),
                completed("sink", 6),
                RunEvent::RunCompleted { run_id: run_id(), at_unix: 7 },
            ]);
        }
        rows
    }

    /// A run's ancestor: its run, its program, its record.
    type Recorded = (weft_core::RunId, Arc<ProjectDefinition>, Vec<RunEvent>);

    /// The records of `ancestors`, read a page of three rows at a time.
    fn pages(ancestors: &[Recorded]) -> impl Fn(weft_core::RunId, Option<i32>) -> std::future::Ready<anyhow::Result<Vec<JournalRow>>> {
        let records: std::collections::HashMap<weft_core::RunId, Vec<JournalRow>> =
            ancestors.iter().map(|(run, _, events)| (*run, rows_of(events.clone()))).collect();
        move |run, after| std::future::ready(Ok(records.get(&run).into_iter().flatten()
            .filter(|row| after.is_none_or(|after| row.seq > after)).take(3).cloned().collect()))
    }

    /// The seed chain of a run seeded by `seed` from `ancestors`, oldest
    /// first.
    fn chain_of(ancestors: &[Recorded], seed: &weft_journal::events::Seed) -> SeedChain {
        SeedChain::new(ancestors.iter().map(|(run_id, project, events)| weft_journal::seed::Ancestor {
            run_id: *run_id, project: project.clone(), birth: row(0, &events[0]),
        }).collect(), seed)
    }

    /// What run `child`, over `program` and seeded by `seed` from
    /// `ancestors`, inherits ([`read_inheritance`]).
    async fn inherits(ancestors: &[Recorded], seed: &weft_journal::events::Seed, program: Arc<ProjectDefinition>, child: weft_core::RunId) -> Inheritance {
        read_inheritance(&pages(ancestors), &chain_of(ancestors, seed), seed, program, child, PROJECT).await.unwrap()
    }

    fn project_all(rows: &[RunEvent]) -> Vec<Value> {
        let mut projector = RunProjector::new(run_id(), ProgramLookup::Found(program()), PROJECT);
        rows.iter()
            .enumerate()
            .flat_map(|(seq, r)| projector.project(&row(seq, r)))
            .map(json)
            .collect()
    }

    #[tokio::test]
    async fn inherited_firing_keeps_answers_in_order_under_its_original_program() {
        let mut rows = run_rows(true);
        for node in ["src", "sink"] {
            rows.push(RunEvent::LogLine {
                run_id: run_id(), node_id: node.into(), frames: vec![], level: "info".into(),
                message: format!("original {node}"), at_unix: 2, at_unix_ms: Some(2000), seq: Some(1),
            });
            rows.push(RunEvent::CostReported {
                run_id: run_id(), node_id: node.into(), frames: vec![], cost_id: node.into(),
                service: "provider".into(), model: None, amount_usd: Some(0.25), billed: false,
                origin: weft_core::CredentialOwner::Author, metadata: Value::Null, charge: None, at_unix: 2,
            });
        }
        rows.splice(3..3, [
            RunEvent::SuspensionRegistered {
                run_id: run_id(), node_id: "src".into(), frames: vec![], token: "answer".into(),
                spec: wait_spec(), call_index: 0, at_unix: 1,
            },
            RunEvent::NodeSuspended {
                run_id: run_id(), node_id: "src".into(), frames: vec![], token: "answer".into(), at_unix: 1,
            },
            RunEvent::SuspensionResolved { run_id: run_id(), token: "answer".into(), value: json!("approved"), at_unix: 1 },
            RunEvent::NodeResumed {
                run_id: run_id(), node_id: "src".into(), frames: vec![], token: Some("answer".into()), at_unix: 1,
            },
        ]);
        let ancestors = [(run_id(), program(), rows)];
        let child = uuid::Uuid::new_v4();
        let seed = weft_journal::events::Seed {
            parent: run_id(), origins: [(Located::top("src"), run_id())].into(), answers: Default::default(),
        };
        let events: Vec<Value> = inherits(&ancestors, &seed, nothing(), child).await.events.into_iter().map(json).collect();
        let mut tail = crate::store::LogTail::new(100);
        inherited_logs(&pages(&ancestors), &chain_of(&ancestors, &seed), &seed, &mut tail).await.unwrap();
        let logs = tail.into_entries();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "original src");
        assert_eq!(logs[0].inherited_from, Some(run_id()));
        assert_eq!(kinds(&events), vec![
            ("node_started".into(), "src".into()),
            ("node_suspended".into(), "src".into()),
            ("wait_ended".into(), "src".into()),
            ("node_resumed".into(), "src".into()),
            ("node_completed".into(), "src".into()),
            ("cost_reported".into(), "".into()),
        ]);
        assert_eq!(events[2]["ended"], json!({ "type": "answered", "value": "approved" }));
        assert_eq!(events[4]["output"]["out"], 9);
        assert_eq!(events[5]["nodeId"], "src");
        assert_eq!(events[5]["amountUsd"], 0.25);
        for event in events {
            assert_eq!(event["runId"], child.to_string());
            assert_eq!(event["inheritedFrom"], run_id().to_string());
        }
    }

    fn wait_spec() -> weft_core::primitive::SignalSpec {
        weft_core::signal::to_spec(weft_core::signal::Form {
            form_type: "human_query".into(), schema: weft_core::signal::FormSchema { fields: Vec::new() },
            title: None, description: None, consumer_kind: None,
        })
    }

    /// A wait the run held in its worker never suspends or resumes the
    /// firing, so its ending is painted from the row that ended it: here
    /// the give-up, and the answer that came after it paints nothing.
    #[test]
    fn a_held_wait_given_up_paints_its_ending_once() {
        let mut rows: Vec<RunEvent> = run_rows(false).into_iter().take(3).collect();
        rows.extend([
            RunEvent::SuspensionRegistered {
                run_id: run_id(), node_id: "src".into(), frames: vec![], token: "t".into(),
                spec: wait_spec(), call_index: 0, at_unix: 2,
            },
            RunEvent::SuspensionGaveUp { run_id: run_id(), token: "t".into(), error: "node 'src' gave up its wait: quiet".into(), at_unix: 3 },
            RunEvent::SuspensionResolved { run_id: run_id(), token: "t".into(), value: json!("late"), at_unix: 4 },
        ]);
        let ended: Vec<Value> = project_all(&rows).into_iter().filter(|e| e["kind"] == "wait_ended").collect();
        assert_eq!(ended.len(), 1, "{ended:?}");
        assert_eq!(ended[0]["node"], "src");
        assert_eq!(ended[0]["token"], "t");
        assert_eq!(ended[0]["ended"], json!({ "type": "gave_up", "error": "node 'src' gave up its wait: quiet" }));
    }

    /// A copy of a scope whose every node the child reuses was decided in
    /// the run they come from, and is shown inherited from it; a copy the
    /// child runs a node of again is the child's to decide.
    #[tokio::test]
    async fn a_scope_the_child_reuses_whole_is_shown_inherited() {
        let ancestors = [(run_id(), program(), run_rows(true))];
        let child = uuid::Uuid::new_v4();
        let painted = |origins: &[&str]| {
            let seed = weft_journal::events::Seed {
                parent: run_id(),
                origins: origins.iter().map(|node| (Located::top(*node), run_id())).collect(),
                answers: Default::default(),
            };
            let ancestors = &ancestors;
            async move {
                inherits(ancestors, &seed, nothing(), child).await.events.into_iter().map(json)
                    .filter(|event| event["kind"] == "scope_decided").collect::<Vec<Value>>()
            }
        };
        let whole = painted(&["src", "inner"]).await;
        assert_eq!(whole.len(), 1, "{whole:?}");
        assert_eq!(whole[0]["scope"], "g");
        assert_eq!(whole[0]["state"], "open");
        assert_eq!(whole[0]["runId"], child.to_string());
        assert_eq!(whole[0]["inheritedFrom"], run_id().to_string());
        assert!(painted(&["src"]).await.is_empty(), "the child runs `inner` again, so it decides `g` itself");
    }

    /// The scope decisions a seeded run paints, as `(scope, runId,
    /// inheritedFrom)`, through its projection from its birth on.
    async fn scopes_painted(child: weft_core::RunId, program: Arc<ProjectDefinition>, ancestors: &[Recorded], rows: &[RunEvent]) -> Vec<(String, String, Value)> {
        let RunEvent::RunStarted { seed: Some(seed), .. } = &rows[0] else { panic!("a seeded run") };
        let inheritance = inherits(ancestors, seed, program.clone(), child).await;
        let mut projector = RunProjector::new(child, ProgramLookup::Found(program), PROJECT).with_inheritance(Inherited::Read(inheritance));
        rows.iter()
            .enumerate()
            .flat_map(|(seq, event)| projector.project(&row(seq, event)))
            .map(json)
            .inspect(|event| assert_ne!(event["kind"], "journal_corruption", "{event}"))
            .filter(|event| event["kind"] == "scope_decided")
            .map(|event| (event["scope"].as_str().unwrap().to_string(), event["runId"].as_str().unwrap().to_string(), event["inheritedFrom"].clone()))
            .collect()
    }

    /// A seeded run that reuses every node of a scope copy, and whose own
    /// cut still keeps the scope (`sink` runs again, fed by `inner`
    /// standing in), decides the copy itself from what its stand-ins send:
    /// it is painted once, as the child's own, never a second time as the
    /// ancestor's.
    #[tokio::test]
    async fn a_seeded_run_paints_each_scope_copy_once() {
        let parent = run_id();
        let ancestors = [(parent, program(), run_rows(true))];
        let child = uuid::Uuid::from_u128(0xc1);
        let (program, given) = standing_in(&[("src", json!({ "out": 9, "flow": true }), parent), ("inner", json!({ "out": 10 }), parent)]);
        let seed = weft_journal::events::Seed {
            parent,
            origins: [(Located::top("src"), parent), (Located::top("inner"), parent)].into(),
            answers: Default::default(),
        };
        let rows = vec![
            born(child, Some(seed), given),
            started_in(child, "sink", 1),
            completed_in(child, "sink", 2),
            RunEvent::RunCompleted { run_id: child, at_unix: 3 },
        ];
        assert_eq!(scopes_painted(child, program, &ancestors, &rows).await, [("g".to_string(), child.to_string(), Value::Null)]);
    }

    /// A seeded run whose inheritance could not be had still folds and
    /// paints its own rows, its stand-ins read off its birth: a chain that
    /// could not be read, an inheritance that could not be reconstructed,
    /// or one that could not be imported, each says why once, at the birth
    /// row.
    #[test]
    fn a_seeded_run_whose_inheritance_failed_still_paints_its_own_rows() {
        let parent = run_id();
        let child = uuid::Uuid::from_u128(0xc1);
        let (program, given) = standing_in(&[("src", json!({ "out": 9, "flow": true }), parent), ("inner", json!({ "out": 10 }), parent)]);
        let seed = weft_journal::events::Seed {
            parent,
            origins: [(Located::top("src"), parent), (Located::top("inner"), parent)].into(),
            answers: Default::default(),
        };
        let rows = [
            born(child, Some(seed), given),
            started_in(child, "sink", 1),
            completed_in(child, "sink", 2),
            RunEvent::RunCompleted { run_id: child, at_unix: 3 },
        ];
        let unimportable = || Inheritance { sources: Arc::new(BTreeMap::new()), events: Vec::new() };
        for (inherited, said) in [
            (Inherited::ChainUnreadable("seed cleaned".into()), "seed cleaned"),
            (Inherited::Failed("seed run gone".into()), "cannot reconstruct seed history: seed run gone"),
            (Inherited::Read(unimportable()), "outside its ancestor chain"),
        ] {
            let mut projector = RunProjector::new(child, ProgramLookup::Found(program.clone()), PROJECT).with_inheritance(inherited);
            let painted: Vec<(usize, Value)> = rows.iter().enumerate()
                .flat_map(|(seq, event)| projector.project(&row(seq, event)).into_iter().map(move |event| (seq, json(event))))
                .collect();
            let corruptions: Vec<&(usize, Value)> = painted.iter().filter(|(_, event)| event["kind"] == "journal_corruption").collect();
            assert_eq!(corruptions.len(), 1, "{corruptions:?}");
            assert_eq!(corruptions[0].0, 0, "said at the birth row");
            assert!(corruptions[0].1["reason"].as_str().unwrap().contains(said), "{corruptions:?}");
            let sink_started = painted.iter().find(|(_, event)| event["kind"] == "node_started" && event["node"] == "sink").expect("its own row paints");
            assert_eq!(sink_started.1["input"]["in"], 10, "fed by its stand-in, from its birth");
            assert!(painted.iter().any(|(_, event)| event["kind"] == "node_completed" && event["node"] == "sink"), "{painted:?}");
        }
    }

    /// Two hops: a middle run seeded from the first decides `g` itself
    /// (it runs `inner` again), and a run seeded from the middle one that
    /// reuses everything (its cut keeps no scope) shows `g` as the middle
    /// run decided it, inherited from the middle run, and the first run's
    /// own decision of it not at all.
    #[tokio::test]
    async fn a_scope_decided_in_a_seeded_middle_run_is_inherited_from_it() {
        let first = run_id();
        let middle = uuid::Uuid::from_u128(0xb1);
        let last = uuid::Uuid::from_u128(0xc1);
        let (middle_program, middle_given) = standing_in(&[("src", json!({ "out": 9, "flow": true }), first)]);
        let middle_rows = vec![
            born(middle, Some(weft_journal::events::Seed {
                parent: first, origins: [(Located::top("src"), first)].into(), answers: Default::default(),
            }), middle_given),
            started_in(middle, "inner", 3),
            emitted_in(middle, "inner", "out", json!(10)),
            completed_in(middle, "inner", 4),
            started_in(middle, "sink", 5),
            completed_in(middle, "sink", 6),
            RunEvent::RunCompleted { run_id: middle, at_unix: 7 },
        ];
        let ancestors = [(first, program(), run_rows(true)), (middle, middle_program, middle_rows)];
        let seed = weft_journal::events::Seed {
            parent: middle,
            origins: [(Located::top("src"), first), (Located::top("inner"), middle), (Located::top("sink"), middle)].into(),
            answers: Default::default(),
        };
        let rows = vec![born(last, Some(seed), Default::default()), RunEvent::RunCompleted { run_id: last, at_unix: 8 }];
        assert_eq!(
            scopes_painted(last, nothing(), &ancestors, &rows).await,
            [("g".to_string(), last.to_string(), json!(middle.to_string()))],
        );
    }

    fn kinds(events: &[Value]) -> Vec<(String, String)> {
        events
            .iter()
            .map(|e| {
                (
                    e["kind"].as_str().unwrap_or("").to_string(),
                    e.get("node").or_else(|| e.get("scope")).and_then(Value::as_str).unwrap_or("").to_string(),
                )
            })
            .collect()
    }

    /// A firing's start carries the input it received, its completion the
    /// output it emitted, the run's completion the outputs, and the group
    /// is painted once, opened, right after the row that decided it (the
    /// gate's value).
    #[test]
    fn a_run_through_a_group_paints_its_scope_once() {
        let events = project_all(&run_rows(true));
        let k = kinds(&events);
        let expect: Vec<(&str, &str)> = vec![
            ("run_started", ""),
            ("node_started", "src"),
            ("scope_decided", "g"),
            ("node_completed", "src"),
            ("node_started", "inner"),
            ("node_completed", "inner"),
            ("node_started", "sink"),
            ("node_completed", "sink"),
            ("run_completed", ""),
        ];
        let expect: Vec<(String, String)> = expect.into_iter().map(|(a, b)| (a.into(), b.into())).collect();
        assert_eq!(k, expect, "{events:#?}");
        let by = |kind: &str, node: &str| {
            events.iter().find(|e| e["kind"] == kind && e["node"] == node).unwrap_or_else(|| panic!("{kind} {node}"))
        };
        let scope = events.iter().find(|e| e["kind"] == "scope_decided").unwrap();
        assert_eq!(scope["state"], "open");
        assert_eq!(scope["frames"], json!([]));
        assert_eq!(by("node_completed", "src")["output"], json!({ "out": 9, "flow": true }));
        assert_eq!(by("node_started", "inner")["input"], json!({ "in": 9 }));
        assert_eq!(by("node_started", "sink")["input"], json!({ "in": 10 }));
        assert_eq!(by("node_started", "sink")["closedPorts"], json!([]));
        let done = events.iter().find(|e| e["kind"] == "run_completed").unwrap();
        assert_eq!(done["outputs"], json!({ "src": { "out": 9, "flow": true }, "inner": { "out": 10 } }));
    }

    /// A gated-off group paints its scope skipped with the reason, and the
    /// consumer past it started with the closed port listed.
    #[test]
    fn a_gated_off_group_paints_a_skip_with_its_reason() {
        let mut rows = run_rows(false);
        rows.push(started("sink", 3));
        rows.push(RunEvent::NodeSkipped {
            run_id: run_id(),
            node_id: "sink".into(),
            frames: vec![],
            reason: weft_core::run::skip::SkipReason::RequiredInputClosed { port: "in".into(), failure: None },
            at_unix: 3,
        });
        let events = project_all(&rows);
        let scope = events.iter().find(|e| e["kind"] == "scope_decided" && e["scope"] == "g").expect("scope skip painted");
        assert_eq!(scope["state"], "skipped");
        assert_eq!(scope["reason"], json!({ "kind": "did_not_flow" }));
        let sink = events.iter().find(|e| e["kind"] == "node_started" && e["node"] == "sink").expect("sink started");
        assert_eq!(sink["closedPorts"], json!(["in"]));
        let skipped = events.iter().find(|e| e["kind"] == "node_skipped" && e["node"] == "sink").expect("sink skipped");
        assert_eq!(skipped["closedPorts"], json!(["in"]));
    }

    /// A projection opened mid-flight and caught up on the earlier rows
    /// paints the rest exactly as one that saw the run from the start:
    /// what a dispatcher that boots during a run relies on.
    #[test]
    fn a_projection_caught_up_mid_flight_paints_the_same_tail() {
        let rows = run_rows(true);
        let from_start = project_all(&rows);
        let split = 6;
        let mut late = RunProjector::new(run_id(), ProgramLookup::Found(program()), PROJECT);
        for (seq, r) in rows[..split].iter().enumerate() {
            late.project(&row(seq, r));
        }
        let tail: Vec<Value> = rows[split..]
            .iter()
            .enumerate()
            .flat_map(|(seq, r)| late.project(&row(split + seq, r)))
            .map(json)
            .collect();
        // Everything the early rows painted comes first in the full
        // projection; the tail is identical.
        let head_len = from_start.len() - tail.len();
        assert_eq!(from_start[head_len..].to_vec(), tail);
    }

    /// A run with no program (a node self-test) paints its birth, its
    /// terminal and its cost trail, and refuses to paint a node row.
    /// A loop row the fold rejects paints nothing: a made-up cap and
    /// mode would look like a real loop.
    #[test]
    fn a_rejected_loop_row_paints_only_its_corruption() {
        let mut projector = RunProjector::new(run_id(), ProgramLookup::Found(program()), PROJECT);
        let instantiated = RunEvent::LoopInstantiated { run_id: run_id(), group_id: "nope".into(), parent_frames: vec![], at_unix: 1 };
        assert!(only_corruptions(&projector.project(&row(0, &instantiated))));
        let fired = RunEvent::LoopOutFired { run_id: run_id(), group_id: "nope".into(), parent_frames: vec![], index: 0, at_unix: 1 };
        assert!(only_corruptions(&projector.project(&row(1, &fired))));
    }

    #[test]
    fn a_run_with_no_program_paints_only_what_needs_none() {
        let mut projector = RunProjector::new(run_id(), ProgramLookup::NoProgram, PROJECT);
        let rows = [
            RunEvent::RunStarted { given: Default::default(),
                run_id: run_id(),
                project_id: PROJECT,
                entry_node: "probe".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: None,
                binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::NodeTest,
                cut_from: None,
                seed: None,
                instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            started("probe", 1),
            RunEvent::RunCompleted { run_id: run_id(), at_unix: 2 },
        ];
        let events: Vec<Value> = rows
            .iter()
            .enumerate()
            .flat_map(|(seq, r)| projector.project(&row(seq, r)))
            .map(json)
            .collect();
        let k: Vec<String> = events.iter().map(|e| e["kind"].as_str().unwrap().to_string()).collect();
        assert_eq!(k, vec!["run_started", "run_completed"]);
        assert_eq!(events[1]["outputs"], Value::Null);
    }

    /// A node row the fold rejected (a second start over an open
    /// firing, a node the program does not have) paints nothing: the
    /// only thing at its location is an earlier firing, and painting
    /// that under the new row's stamp would contradict the corruption
    /// the same row surfaced.
    #[test]
    fn a_rejected_node_row_paints_only_its_corruption() {
        let mut projector = RunProjector::new(run_id(), ProgramLookup::Found(program()), PROJECT);
        let mut rows = run_rows(true);
        rows.truncate(3);
        for (seq, r) in rows.iter().enumerate() {
            projector.project(&row(seq, r));
        }
        assert!(only_corruptions(&projector.project(&row(10, &started("src", 9)))), "a duplicate start paints its corruption");
        assert!(only_corruptions(&projector.project(&row(11, &started("ghost", 9)))), "an unknown node paints its corruption");
        assert!(only_corruptions(&projector.project(&row(12, &completed("ghost", 9)))));
        // The open firing is still painted by its own rows.
        assert!(!projector.project(&row(13, &completed("src", 10))).is_empty());
    }
}
