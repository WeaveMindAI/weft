//! Journal rows to the events the editor paints.
//!
//! The journal holds facts; the editor's wire shape
//! (`DispatcherEvent`) carries what the screen shows: a firing's input
//! bag, its output, the ports that arrived closed, a group boundary
//! running or skipping. None of that is on the rows any more (see
//! `weft_journal::events`), so the projection keeps ONE fold per open
//! execution, applies each row as it lands, and reads the derived
//! facts back off the fold. The live bridge and the replay endpoint
//! both come through here, so live and replay cannot drift.

use std::sync::Arc;

use weft_core::exec::boundary::{BoundaryDispatch, BoundaryOutcome};
use weft_core::frames::Located;
use weft_core::exec::NodeExecutionStatus;
use weft_core::primitive::LoopInstanceKey;
use weft_core::project::boundary_in_id;
use weft_core::ProjectDefinition;
use std::collections::BTreeMap;

use weft_journal::{ExecEvent, Fold, FoldEffects, SeedChain};

use crate::events::DispatcherEvent;

/// What a reader that folds a color's journal found when it went for
/// the run's program.
pub enum ProgramLookup {
    Found(Arc<ProjectDefinition>),
    /// The color has no program at all: a node self-test (its birth
    /// row pins no definition hash), or a color that never started. A
    /// node row on such a color is a writer bug.
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

    /// The same state, with the reason emptied.
    ///
    /// For a reader that has already published the reason and must not
    /// publish it twice. It lives here because the alternative, a
    /// `match` beside each such reader, was written twice with a `_`
    /// arm that quietly relabels any state it does not name: a third
    /// unpaintable state would have been reported as an undecodable row
    /// in both places, which is the distinction these states exist to
    /// keep.
    pub fn without_reason(&self) -> Self {
        match self {
            Self::Found(p) => Self::Found(p.clone()),
            Self::NoProgram => Self::NoProgram,
            Self::Unreadable(_) => Self::Unreadable(String::new()),
            Self::ProgramGone(_) => Self::ProgramGone(String::new()),
        }
    }

    /// Why this run cannot be painted whole, and where the reason
    /// belongs on screen. `None` when it can (or when it has no
    /// program to miss). Every reader publishes this; a run that paints
    /// empty for no stated reason is the bug that sends people hunting
    /// through the viewer.
    pub fn unpaintable(&self) -> Option<(weft_core::primitive::CorruptionSite, &str)> {
        use weft_core::primitive::CorruptionSite;
        match self {
            Self::Unreadable(reason) => Some((CorruptionSite::UndecodableRow, reason)),
            Self::ProgramGone(reason) => Some((CorruptionSite::MissingProgram, reason)),
            Self::Found(_) | Self::NoProgram => None,
        }
    }
}

/// One execution's live projection: its fold plus the project id the
/// events are attributed to.
pub struct ExecutionProjector {
    /// `None` for a run painted without a fold: only what needs no
    /// program is painted, and its node rows are skipped. Quietly for a
    /// run whose program is gone or unreadable (the corruption was
    /// published when this projection was opened); with a warning for
    /// a run that never had one, where a node row is a writer bug.
    fold: Option<Fold>,
    /// Whether the reason this run cannot be painted whole has already
    /// been said. Consulted only when there is no fold: a run whose
    /// program is gone or unreadable had its reason published when the
    /// projection was opened, so a node row here is skipped quietly,
    /// while a run that never had a program gets a warning, because a
    /// node row on one of those is a writer bug.
    reason_already_published: bool,
    color: weft_core::Color,
    project_id: String,
    /// What a seeded run inherits, handed in by the opener (which
    /// read the seed chain off the journal) and painted right after
    /// the birth row, marked with the run each row came from.
    inheritance: SeedChain,
}

impl ExecutionProjector {
    pub fn new(color: weft_core::Color, program: ProgramLookup, project_id: String) -> Self {
        let reason_already_published = program.unpaintable().is_some();
        Self {
            fold: program.program().map(|p| Fold::new(color, p)),
            reason_already_published,
            color,
            project_id,
            inheritance: SeedChain::default(),
        }
    }

    /// The seed chain this run inherits from (`weft_journal::seed_chain`
    /// over its birth row). Painted when the birth row is projected:
    /// every inherited row lands on the screen before the run's own,
    /// as it sits in the fold, so a seeded run reads whole instead of
    /// as three nodes with values and twelve empty ones.
    pub fn with_inheritance(mut self, chain: SeedChain) -> Self {
        self.inheritance = chain;
        self
    }

    /// Whether this projection folds at all. One that does not costs
    /// nothing to keep, and rebuilding it would republish its
    /// corruption, so the bridge keeps it until the run's terminal.
    pub fn folds(&self) -> bool {
        self.fold.is_some()
    }

    /// Whether `ev` paints the same with or without the run's program:
    /// the birth, the terminals, the money and log trail, the bus and
    /// caller exchanges. A reader with no projection open for the
    /// color paints such a row without opening one.
    pub fn paints_without_program(ev: &ExecEvent) -> bool {
        !needs_program(ev)
    }

    /// Apply one row and answer the events it paints. One row can
    /// project to MANY events: a row that fires a group boundary paints
    /// the boundary's start and end too, and a value carrying a bus
    /// marker paints both ends of every wire it rode as participants.
    /// A row the fold rejected paints its corruption and nothing else:
    /// whatever the fold holds at its location is an earlier firing's,
    /// and a made-up event would contradict the corruption. The same
    /// event reaches the live screen and the replay, at the row.
    pub fn project(&mut self, ev: &ExecEvent) -> Vec<DispatcherEvent> {
        let mut out = self.project_row(ev);
        if let ExecEvent::ExecutionStarted { seed: Some(seed), at_unix, .. } = ev {
            let chain = std::mem::take(&mut self.inheritance);
            if let Some(fold) = &mut self.fold {
                // Every ancestor is reconstructed once, here, and both
                // consumers read from that one reconstruction.
                let inherited = chain.materialize().and_then(|sources| {
                    let effects = weft_journal::seed::import_origins(fold, seed, &sources, *at_unix)?;
                    let boundaries = paint_inherited_boundaries(fold, &effects, self.color, &self.project_id, *at_unix);
                    let mut events = inherited_events(&chain, &sources, seed, self.color, &self.project_id)?;
                    events.extend(boundaries);
                    Ok(events)
                });
                match inherited {
                    Ok(events) => {
                        out.extend(events);
                    }
                    Err(error) => {
                        self.fold = None;
                        self.reason_already_published = true;
                        out.push(DispatcherEvent::JournalCorruption {
                            color: self.color, project_id: self.project_id.clone(),
                            site: weft_core::primitive::CorruptionSite::NodeLifecycle, reason: format!("cannot reconstruct seed history: {error:#}"),
                        });
                    }
                }
            }
        }
        out
    }

    /// One of this execution's own rows through its fold and onto the screen.
    fn project_row(&mut self, ev: &ExecEvent) -> Vec<DispatcherEvent> {
        let inherited_from = None;
        let project_id = self.project_id.clone();
        let color = self.color;
        let at_unix = ev.at_unix();
        let effects = match self.fold.as_mut() {
            Some(fold) => fold.apply(ev),
            None => {
                if needs_program(ev) {
                    if !self.reason_already_published {
                        tracing::warn!(
                            target: "weft_dispatcher::projection",
                            %color, kind = ev.kind_str(),
                            "a node row on an execution with no program; nothing to paint it from"
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
                    color,
                    project_id: project_id.clone(),
                    site: c.site,
                    reason: c.reason.clone(),
                })
                .collect()
        } else { match ev {
            ExecEvent::ExecutionStarted { entry_node, subgraph, seed, .. } => {
                vec![DispatcherEvent::ExecutionStarted {
                    color, at_unix,
                    entry_node: entry_node.clone(),
                    subgraph: subgraph.as_ref().map(|s| s.nodes.iter().cloned().collect()),
                    seed: seed.clone(),
                    project_id,
                }]
            }
            ExecEvent::NodeStarted { node_id, frames, .. } => {
                let view = self.view(node_id, frames);
                vec![DispatcherEvent::NodeStarted {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    input: view.input,
                    closed_ports: view.closed_ports,
                    provided_ports: view.provided_ports,
                    backup_ports: view.backup_ports,
                    inherited_ports: view.inherited_ports,
                    inherited_from,
                    project_id,
                }]
            }
            ExecEvent::NodeCompleted { node_id, frames, .. } => {
                let output = self
                    .fold
                    .as_ref()
                    .and_then(|f| f.output_of(node_id, frames))
                    .unwrap_or(serde_json::Value::Null);
                vec![DispatcherEvent::NodeCompleted {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    output,
                    inherited_from,
                    project_id,
                }]
            }
            ExecEvent::NodeFailed { node_id, frames, error, .. } => {
                vec![DispatcherEvent::NodeFailed {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    error: error.clone(),
                    inherited_from,
                    project_id,
                }]
            }
            ExecEvent::NodeSkipped { node_id, frames, reason, .. } => {
                let view = self.view(node_id, frames);
                vec![DispatcherEvent::NodeSkipped {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    closed_ports: view.closed_ports,
                    reason: Some(reason.clone()),
                    inherited_from,
                    project_id,
                }]
            }
            ExecEvent::PortTypeMismatch { node_id, frames, port, expected, actual, .. } => {
                let mut out = vec![DispatcherEvent::PortTypeMismatch {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    port: port.clone(),
                    expected: expected.clone(),
                    actual: actual.clone(),
                    project_id: project_id.clone(),
                }];
                out.extend(sniff_emissions(color, &project_id, &effects));
                out
            }
            ExecEvent::NodeSuspended { node_id, frames, token, .. } => {
                vec![DispatcherEvent::NodeSuspended {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    token: token.clone(),
                    inherited_from,
                    project_id,
                }]
            }
            ExecEvent::NodeResumed { node_id, frames, token, .. } => {
                vec![DispatcherEvent::NodeResumed {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    token: token.clone(),
                    value: effects.resumed_value.clone(),
                    inherited_from,
                    project_id,
                }]
            }
            ExecEvent::NodeCancelled { node_id, frames, reason, .. } => {
                vec![DispatcherEvent::NodeCancelled {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    reason: reason.clone(),
                    inherited_from,
                    project_id,
                }]
            }
            ExecEvent::ExecutionCompleted { .. } => {
                let outputs = self
                    .fold
                    .as_ref()
                    .map(|f| f.final_outputs())
                    .unwrap_or(serde_json::Value::Null);
                vec![DispatcherEvent::ExecutionCompleted { color, at_unix, outputs, project_id }]
            }
            ExecEvent::ExecutionFailed { error, .. } => {
                // No truncation: journal_bridge fans out via
                // `publish_local` (no NOTIFY hop); the full error is
                // what the operator wants for debugging. Truncation
                // belongs at NOTIFY producer sites (api/project.rs,
                // infra_event_bridge.rs), not here.
                vec![DispatcherEvent::ExecutionFailed {
                    color, at_unix,
                    error: error.clone(),
                    project_id,
                }]
            }
            ExecEvent::ExecutionCancelled { reason, cause, .. } => {
                vec![DispatcherEvent::ExecutionCancelled {
                    color, at_unix,
                    reason: reason.clone(),
                    cause: cause.clone(),
                    project_id,
                }]
            }
            ExecEvent::ExecutionTagged { tags, .. } => {
                vec![DispatcherEvent::ExecutionTagged {
                    color, at_unix,
                    tags: tags.clone(),
                    project_id,
                }]
            }
            ExecEvent::CostReported {
                node_id, frames, cost_id, service, amount_usd, origin, ..
            } => {
                vec![DispatcherEvent::CostReported {
                    color, at_unix,
                    inherited_from: None,
                    project_id,
                    node_id: node_id.clone(),
                    frames: frames.clone(),
                    cost_id: cost_id.clone(),
                    service: service.clone(),
                    amount_usd: *amount_usd,
                    origin: *origin,
                }]
            }
            // Bus events: surfaced so the inspector renders a live IRC-style
            // log per bus. The webview groups by `bus_id` and renders ONE
            // panel per bus on every node listed as a `BusParticipant`.
            ExecEvent::BusJoined { bus_id, offset, name, .. } => {
                vec![DispatcherEvent::BusJoined {
                    color,
                    project_id,
                    bus_id: bus_id.clone(),
                    offset: *offset,
                    name: name.clone(),
                    at_unix,
                }]
            }
            ExecEvent::BusLeft { bus_id, offset, name, .. } => {
                vec![DispatcherEvent::BusLeft {
                    color,
                    project_id,
                    bus_id: bus_id.clone(),
                    offset: *offset,
                    name: name.clone(),
                    at_unix,
                }]
            }
            ExecEvent::BusWindow { bus_id, first_offset, last_offset, messages, totals, .. } => {
                vec![DispatcherEvent::BusWindow {
                    color,
                    project_id,
                    bus_id: bus_id.clone(),
                    first_offset: *first_offset,
                    last_offset: *last_offset,
                    messages: messages.clone(),
                    totals: totals.clone(),
                    at_unix,
                }]
            }
            ExecEvent::BusClosed { bus_id, offset, .. } => {
                vec![DispatcherEvent::BusClosed {
                    color,
                    project_id,
                    bus_id: bus_id.clone(),
                    offset: *offset,
                    at_unix,
                }]
            }
            // Bus participation is derived from the pulses an emission
            // put on the wires: a pulse whose value carries a bus marker
            // means BOTH the producer node AND the consumer node touched
            // that bus. The webview unions these into a per-bus
            // participant set; each participant gets the inspector's IRC
            // panel for that bus. A pulse routed through a group boundary
            // stamps the boundary node as a participant too, on purpose:
            // a Group's inspector SHOULD show the bus conversation flowing
            // through it.
            ExecEvent::PortEmitted { .. } | ExecEvent::PortClosed { .. } => {
                sniff_emissions(color, &project_id, &effects)
            }
            ExecEvent::LoopInstantiated { group_id, parent_frames, .. } => {
                let key = LoopInstanceKey {
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    color,
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
                    color, at_unix, project_id,
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    iter_cap: inst.iter_cap,
                    parallel: inst.config.parallel,
                }]
            }
            ExecEvent::LoopIterationLaunched { group_id, parent_frames, index, .. } => {
                let mut out = vec![DispatcherEvent::LoopIterationLaunched {
                    color, at_unix, project_id: project_id.clone(),
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    index: *index,
                }];
                out.extend(sniff_emissions(color, &project_id, &effects));
                out
            }
            ExecEvent::LoopOutFired { group_id, parent_frames, index, .. } => {
                // The vote is read off the fold's LoopOut firing. No
                // vote at all means the runtime already held this
                // firing (a replayed row, or one after the loop ended):
                // nothing is painted, because a made-up "no vote" is
                // exactly what a real no-vote looks like.
                match effects.loop_out_done {
                    Some(done_vote) => vec![DispatcherEvent::LoopOutFired {
                        color, at_unix, project_id,
                        group_id: group_id.clone(),
                        parent_frames: parent_frames.clone(),
                        index: *index,
                        done_vote,
                    }],
                    None => Vec::new(),
                }
            }
            ExecEvent::LoopTerminated { group_id, parent_frames, reason, .. } => {
                let mut out = vec![DispatcherEvent::LoopTerminated {
                    color, at_unix, project_id: project_id.clone(),
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    reason: *reason,
                }];
                out.extend(sniff_emissions(color, &project_id, &effects));
                out
            }
            // Caller events: surfaced 1:1 so the inspector replays the live
            // caller exchange (connected / inbound / outbound / errored /
            // disconnected) the same way it replays a bus. Payloads carry
            // the same tagged `WirePayload` shape as a bus window's
            // messages.
            ExecEvent::CallerConnected { offset, protocol, .. } => {
                vec![DispatcherEvent::CallerConnected {
                    color, project_id, offset: *offset,
                    protocol: protocol.clone(), at_unix,
                }]
            }
            ExecEvent::CallerInbound { offset, payload, payload_byte_size, .. } => {
                vec![DispatcherEvent::CallerInbound {
                    color, project_id, offset: *offset,
                    payload: payload.clone(),
                    payload_byte_size: *payload_byte_size,
                    at_unix,
                }]
            }
            ExecEvent::CallerOutbound { offset, payload, payload_byte_size, terminal, .. } => {
                vec![DispatcherEvent::CallerOutbound {
                    color, project_id, offset: *offset,
                    payload: payload.clone(),
                    payload_byte_size: *payload_byte_size,
                    terminal: *terminal,
                    at_unix,
                }]
            }
            ExecEvent::CallerErrored { offset, message, .. } => {
                vec![DispatcherEvent::CallerErrored {
                    color, project_id, offset: *offset,
                    message: message.clone(), at_unix,
                }]
            }
            ExecEvent::CallerDisconnected { offset, reason, .. } => {
                vec![DispatcherEvent::CallerDisconnected {
                    color, project_id, offset: *offset,
                    reason: reason.clone(), at_unix,
                }]
            }
            // SuspensionRegistered / SuspensionResolved / LogLine /
            // RunOutput / NodeKicked / PulsesConsumed / LoopStreamEnded:
            // not surfaced through DispatcherEvent. SSE consumers don't
            // need them for live UI; they read the journal directly when
            // they want full detail. A kick can still fire a group
            // boundary, painted below.
            _ => Vec::new(),
        } };
        // A group boundary never has a row of its own: what the fold
        // fired from this row is painted as the start and end the
        // editor reads the group's state from. Its forwarded values
        // sniff for bus participants like any other emission.
        for boundary in &effects.boundaries {
            if let Some(view) = self.fold.as_ref().and_then(|fold| fold.boundary_view(boundary)) {
                out.extend(boundary_events(color, &self.project_id, at_unix, boundary, &view, inherited_from));
            }
        }
        out
    }

    fn view(&self, node_id: &str, frames: &weft_core::frames::LoopFrames) -> weft_journal::FiringView {
        self.fold
            .as_ref()
            .and_then(|f| f.firing_view(node_id, frames))
            .unwrap_or(weft_journal::FiringView {
                input: serde_json::Value::Object(Default::default()),
                closed_ports: Vec::new(),
                provided_ports: Vec::new(),
                backup_ports: Vec::new(),
                inherited_ports: Default::default(),
            })
    }
}

fn paint_inherited_boundaries(fold: &Fold, effects: &FoldEffects, color: weft_core::Color, project_id: &str, at_unix: u64) -> Vec<DispatcherEvent> {
    let mut events = Vec::new();
    for boundary in &effects.boundaries {
        if let Some(view) = fold.boundary_view(boundary) {
            events.extend(boundary_events(color, project_id, at_unix, boundary, &view, None));
        }
    }
    events
}

/// Paint selected original firings under their own program, including their
/// suspension and resume history. These rows never enter the child's fold.
///
/// `sources` is the chain's reconstruction (`SeedChain::materialize`),
/// which already refused any ancestor whose own seed names a run that
/// came after it, so every ancestor's seed resolves inside it.
fn inherited_events(
    chain: &SeedChain,
    sources: &BTreeMap<weft_core::Color, Fold>,
    seed: &weft_journal::events::Seed,
    child: weft_core::Color,
    project_id: &str,
) -> anyhow::Result<Vec<DispatcherEvent>> {
    let mut out = Vec::new();
    for ancestor in &chain.ancestors {
        if seed.origins.values().any(|origin| *origin == ancestor.color) {
            let mut source = ExecutionProjector::new(
                ancestor.color, ProgramLookup::Found(ancestor.project.clone()), project_id.to_owned(),
            );
            for row in &ancestor.rows {
                let mut events = source.project_row(row);
                if let ExecEvent::ExecutionStarted { seed: Some(parent), at_unix, .. } = row {
                    let fold = source.fold.as_mut().expect("original program supplied");
                    let effects = weft_journal::seed::import_origins(fold, parent, sources, *at_unix)?;
                    events.extend(paint_inherited_boundaries(fold, &effects, ancestor.color, project_id, *at_unix));
                }
                for mut event in events {
                    match &mut event {
                        DispatcherEvent::NodeStarted { color, node, frames, inherited_from, .. }
                        | DispatcherEvent::NodeSuspended { color, node, frames, inherited_from, .. }
                        | DispatcherEvent::NodeResumed { color, node, frames, inherited_from, .. }
                        | DispatcherEvent::NodeCompleted { color, node, frames, inherited_from, .. }
                        | DispatcherEvent::NodeSkipped { color, node, frames, inherited_from, .. } => {
                            if seed.origins.get(&Located::at(node.as_str(), frames)) != Some(&ancestor.color) { continue; }
                            *color = child;
                            *inherited_from = Some(ancestor.color);
                            out.push(event);
                        }
                        DispatcherEvent::JournalCorruption { reason, .. } => anyhow::bail!(
                            "original run {} cannot be projected: {reason}", ancestor.color,
                        ),
                        DispatcherEvent::LoopInstantiated { color, group_id, parent_frames, .. }
                        | DispatcherEvent::LoopIterationLaunched { color, group_id, parent_frames, .. }
                        | DispatcherEvent::LoopOutFired { color, group_id, parent_frames, .. }
                        | DispatcherEvent::LoopTerminated { color, group_id, parent_frames, .. } => {
                            if seed.origins.get(&Located::at(boundary_in_id(group_id), parent_frames)) != Some(&ancestor.color) { continue; }
                            *color = child;
                            out.push(event);
                        }
                        DispatcherEvent::CostReported { color, node_id, frames, inherited_from, .. } => {
                            if seed.origins.get(&Located::at(node_id.as_str(), frames)) != Some(&ancestor.color) { continue; }
                            *color = child;
                            *inherited_from = Some(ancestor.color);
                            out.push(event);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Whether a row is about a firing the fold has to know: a row that
/// can only be painted with the program in hand.
fn needs_program(ev: &ExecEvent) -> bool {
    !matches!(
        ev,
        ExecEvent::ExecutionStarted { .. }
            | ExecEvent::ExecutionCompleted { .. }
            | ExecEvent::ExecutionFailed { .. }
            | ExecEvent::ExecutionCancelled { .. }
            | ExecEvent::ExecutionTagged { .. }
            | ExecEvent::CostReported { .. }
            | ExecEvent::LogLine { .. }
            | ExecEvent::BusJoined { .. }
            | ExecEvent::BusLeft { .. }
            | ExecEvent::BusWindow { .. }
            | ExecEvent::BusClosed { .. }
            | ExecEvent::CallerConnected { .. }
            | ExecEvent::CallerInbound { .. }
            | ExecEvent::CallerOutbound { .. }
            | ExecEvent::CallerErrored { .. }
            | ExecEvent::CallerDisconnected { .. }
    )
}

/// The start and end of a boundary firing, as the editor paints a
/// node: it reads a group's state off its `__in` boundary's events.
fn boundary_events(
    color: weft_core::Color,
    project_id: &str,
    at_unix: u64,
    boundary: &BoundaryDispatch,
    view: &weft_journal::FiringView,
    inherited_from: Option<weft_core::Color>,
) -> Vec<DispatcherEvent> {
    let BoundaryOutcome::Fired { status, input, closed_ports, output, skip_reason, error, .. } =
        &boundary.outcome
    else {
        return Vec::new();
    };
    let node = boundary.node_id.clone();
    let frames = boundary.frames.clone();
    let mut out = vec![DispatcherEvent::NodeStarted {
        color,
        at_unix,
        node: node.clone(),
        frames: frames.clone(),
        input: serde_json::Value::Object(weft_core::exec::ready::owned_bag(input)),
        closed_ports: closed_ports.clone(),
        provided_ports: view.provided_ports.clone(),
        backup_ports: view.backup_ports.clone(),
        inherited_ports: view.inherited_ports.clone(),
        inherited_from,
        project_id: project_id.to_string(),
    }];
    out.push(match status {
        NodeExecutionStatus::Skipped => DispatcherEvent::NodeSkipped {
            color,
            at_unix,
            node,
            frames,
            closed_ports: closed_ports.clone(),
            reason: skip_reason.clone(),
            inherited_from,
            project_id: project_id.to_string(),
        },
        NodeExecutionStatus::Failed => DispatcherEvent::NodeFailed {
            color,
            at_unix,
            node,
            frames,
            error: error.clone().expect("a failed boundary carries its error"),
            inherited_from,
            project_id: project_id.to_string(),
        },
        _ => DispatcherEvent::NodeCompleted {
            color,
            at_unix,
            node,
            frames,
            output: output
                .as_ref()
                .filter(|bag| !bag.is_empty())
                .map(|bag| serde_json::Value::Object(weft_core::exec::ready::owned_bag(bag)))
                .unwrap_or(serde_json::Value::Null),
            inherited_from,
            project_id: project_id.to_string(),
        },
    });
    for e in &boundary.emissions {
        out.extend(sniff_bus_participants(color, project_id, &e.source_node, &e.pulse.target_node, &e.pulse.value, e.pulse.closed));
    }
    out
}

/// Bus participants from every pulse a row put on the wires.
fn sniff_emissions(color: weft_core::Color, project_id: &str, effects: &FoldEffects) -> Vec<DispatcherEvent> {
    let mut out = Vec::new();
    for e in &effects.emissions {
        out.extend(sniff_bus_participants(color, project_id, &e.source_node, &e.pulse.target_node, &e.pulse.value, e.pulse.closed));
    }
    out
}

/// Derive `BusParticipant` events from one emitted pulse if its value
/// carries a bus marker. The rule is "any pulse, however it got on the
/// wire, derives participants".
fn sniff_bus_participants(
    color: uuid::Uuid,
    project_id: &str,
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
            bus_id, %color, marker = %value,
            "skip BusParticipant: marker has id but no recognised mode"
        );
        return Vec::new();
    };
    let ephemeral = mode == weft_core::bus::BusMode::Ephemeral;
    let mut out = vec![DispatcherEvent::BusParticipant {
        color,
        project_id: project_id.to_string(),
        bus_id: bus_id.clone(),
        node_id: source_node.to_string(),
        ephemeral,
    }];
    if target_node != source_node {
        out.push(DispatcherEvent::BusParticipant {
            color,
            project_id: project_id.to_string(),
            bus_id,
            node_id: target_node.to_string(),
            ephemeral,
        });
    }
    out
}

/// The program an execution runs, for the readers that fold its
/// journal to SHOW or to END it: the definition recorded under the
/// hash its `ExecutionStarted` pinned. `Err` only for the database
/// itself (a retry can succeed); everything permanent is a
/// `ProgramLookup` variant, so no reader wedges on one color. A RESUME
/// never comes through here: a worker that cannot find its program
/// fails loudly (`route_entry::definition_for`).
/// What a seeded run inherits, read off its birth row and the seeds'
/// journals: the chain the projector paints before the run's own rows.
/// Empty for a run with no seed or no program. A seed that cannot be
/// read (cleaned, or its chain loops) is a reason the run paints
/// without its inheritance; the caller publishes it as the run's
/// corruption, naming `weft clean`, and the run's own rows still paint.
pub async fn execution_inheritance(
    state: &crate::state::DispatcherState,
    rows: &[ExecEvent],
    program: &ProgramLookup,
) -> Result<weft_journal::SeedChain, String> {
    if program.program().is_none() {
        return Ok(weft_journal::SeedChain::default());
    }
    weft_journal::seed_chain(rows, |seed| state.journal.events_log(seed), |project_id, hash| async move {
        let json = state.projects.definition_for_hash(project_id.parse()?, &hash).await?
            .ok_or_else(|| anyhow::anyhow!("seed definition {hash} is missing from project {project_id}"))?;
        Ok(std::sync::Arc::new(serde_json::from_str(&json)?))
    })
        .await
        .map_err(|e| format!("this run inherits from a seed that cannot be read: {e:#}"))
}

/// Read the selected firings' original logs together with this run's own tail.
pub async fn execution_logs(
    state: &crate::state::DispatcherState,
    color: weft_core::Color,
    limit: u32,
) -> anyhow::Result<Vec<crate::journal::LogEntry>> {
    let mut logs = state.journal.logs_for(color, limit).await?;
    let inherited = async {
        let rows = state.journal.events_log(color).await?;
        let Some(ExecEvent::ExecutionStarted { seed: Some(seed), .. }) = rows.first() else { return Ok(Vec::new()); };
        let program = execution_program(state, color).await?;
        anyhow::ensure!(program.program().is_some(), "inherited logs need the run's original program");
        let chain = execution_inheritance(state, &rows, &program).await.map_err(anyhow::Error::msg)?;
        Ok::<_, anyhow::Error>(inherited_logs(&chain, seed))
    }.await;
    match inherited {
        Ok(inherited) => logs.extend(inherited),
        Err(error) => logs.push(crate::journal::LogEntry::corrupt_row(0, format!("cannot read inherited log history: {error:#}"))),
    }
    Ok(crate::journal::LogEntry::tail(logs, limit))
}

fn inherited_logs(chain: &SeedChain, seed: &weft_journal::Seed) -> Vec<crate::journal::LogEntry> {
    chain.ancestors.iter().flat_map(|ancestor| ancestor.rows.iter().filter_map(|event| {
        let mut line = crate::journal::LogEntry::from_event(event)?;
        if seed.origins.get(&Located::at(line.node.as_deref()?, &line.frames)) != Some(&ancestor.color) { return None; }
        line.inherited_from = Some(ancestor.color);
        Some(line)
    })).collect()
}

/// Strict reconstruction for seeding and complete output review.
pub async fn reconstruct_execution(
    state: &crate::state::DispatcherState,
    color: weft_core::Color,
) -> anyhow::Result<std::collections::BTreeMap<weft_core::Color, weft_journal::Fold>> {
    let found = execution_program(state, color).await?;
    let program = found.program()
        .ok_or_else(|| anyhow::anyhow!("run {color} has no readable original program"))?;
    let rows = state.journal.events_log(color).await?;
    let mut chain = execution_inheritance(state, &rows, &found).await.map_err(anyhow::Error::msg)?;
    chain.ancestors.push(weft_journal::seed::Ancestor { color, project: program, rows });
    chain.materialize()
}

pub async fn execution_program(
    state: &crate::state::DispatcherState,
    color: weft_core::Color,
) -> anyhow::Result<ProgramLookup> {
    use crate::journal::ColorLookup;
    let hash = match state.journal.execution_definition_hash(color).await? {
        ColorLookup::Found(hash) => hash,
        ColorLookup::NotFound => return Ok(ProgramLookup::NoProgram),
        ColorLookup::Corrupt => {
            return Ok(ProgramLookup::Unreadable(format!(
                "the birth row of color {color} no longer decodes; its program cannot be found. \
                 `weft clean {color}` removes it."
            )))
        }
    };
    let Some(owner) = state.journal.execution_owner(color).await? else {
        return Ok(ProgramLookup::Unreadable(format!(
            "color {color} has a definition hash but no owner row. `weft clean {color}` removes it."
        )));
    };
    let project_uuid: uuid::Uuid = match owner.project_id.parse() {
        Ok(id) => id,
        Err(e) => {
            return Ok(ProgramLookup::Unreadable(format!(
                "color {color} names a non-uuid project '{}': {e}. `weft clean {color}` removes it.",
                owner.project_id
            )))
        }
    };
    let Some(json) = state.projects.definition_for_hash(project_uuid, &hash).await? else {
        // A definition is kept for as long as any run points at it, so
        // reaching here means it was pruned along with the last run
        // that used it, or this journal predates that rule (until
        // then the history died with the project row and every past
        // run of a removed project went quiet). Either way the rows
        // are there and their values are not derivable, which is a
        // thing to SAY rather than to log where nobody looks.
        tracing::warn!(
            target: "weft_dispatcher::projection",
            %color, project_id = %owner.project_id, %hash,
            "no recorded definition for this execution's hash; the run is shown without the \
             values its journal would derive"
        );
        return Ok(ProgramLookup::ProgramGone(format!(
            "the code this run ran (project {}, version {hash}) is no longer recorded, so its \
             inputs and outputs cannot be worked out from the journal. What each node did is \
             still here; the values are not. If you still have those files, `weft build` in the \
             project folder registers that program again, under the very hash this run names, and \
             it reads as it did. If you do not, `weft clean {color}` removes the run.",
            owner.project_id
        )));
    };
    match serde_json::from_str(&json) {
        Ok(program) => Ok(ProgramLookup::Found(Arc::new(program))),
        Err(e) => Ok(ProgramLookup::Unreadable(format!(
            "the definition recorded for color {color} (project {}, hash {hash}) no longer reads: \
             {e}. `weft clean {color}` removes the run.",
            owner.project_id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use weft_journal::ExecEvent;

    fn color() -> weft_core::Color {
        uuid::Uuid::nil()
    }

    /// `src.out` feeds group `g` (`g__in.x` -> `inner.in` -> `g__out.y`
    /// -> `sink.in`), the group's gate on `src.flow`.
    fn program() -> Arc<ProjectDefinition> {
        let node = |id: &str, ty: &str, inputs: Vec<Value>, outputs: Vec<&str>, scope: Vec<&str>, boundary: Value| {
            json!({
                "id": id, "nodeType": ty, "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 },
                "inputs": inputs,
                "outputs": outputs.iter().map(|o| json!({ "name": o, "portType": "Number", "required": true })).collect::<Vec<_>>(),
                "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false, "images": []
            })
        };
        let inp = |name: &str| json!({ "name": name, "portType": "Number", "required": true });
        let gate = json!({ "name": "_should_flow", "portType": "Boolean", "required": false });
        Arc::new(
            serde_json::from_value(json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "nodes": [
                    node("src", "T", vec![], vec!["out", "flow"], vec![], Value::Null),
                    node("g__in", "Passthrough", vec![inp("x"), gate], vec!["x"], vec![], json!({ "groupId": "g", "role": "In" })),
                    node("inner", "T", vec![inp("in")], vec!["out"], vec!["g"], Value::Null),
                    node("g__out", "Passthrough", vec![inp("y")], vec!["y"], vec![], json!({ "groupId": "g", "role": "Out" })),
                    node("sink", "T", vec![inp("in")], vec![], vec![], Value::Null),
                ],
                "edges": [
                    { "id": "e0", "source": "src", "sourceHandle": "out", "target": "g__in", "targetHandle": "x" },
                    { "id": "e1", "source": "src", "sourceHandle": "flow", "target": "g__in", "targetHandle": "_should_flow" },
                    { "id": "e2", "source": "g__in", "sourceHandle": "x", "target": "inner", "targetHandle": "in" },
                    { "id": "e3", "source": "inner", "sourceHandle": "out", "target": "g__out", "targetHandle": "y" },
                    { "id": "e4", "source": "g__out", "sourceHandle": "y", "target": "sink", "targetHandle": "in" }
                ],
                "groups": [],
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

    fn started(node: &str, at: u64) -> ExecEvent {
        ExecEvent::NodeStarted { color: color(), node_id: node.into(), frames: vec![], at_unix: at }
    }

    fn completed(node: &str, at: u64) -> ExecEvent {
        ExecEvent::NodeCompleted { color: color(), node_id: node.into(), frames: vec![], at_unix: at }
    }

    fn emitted(node: &str, port: &str, value: Value) -> ExecEvent {
        ExecEvent::PortEmitted {
            color: color(),
            emission_id: uuid::Uuid::new_v4(),
            node_id: node.into(),
            frames: vec![],
            port: port.into(),
            value: Arc::new(value),
            provided: false,
            at_unix: 1,
        }
    }

    /// The rows of a run through the group.
    fn run_rows(flow: bool) -> Vec<ExecEvent> {
        let mut rows = vec![
            ExecEvent::ExecutionStarted {
                color: color(),
                project_id: "p".into(),
                entry_node: "src".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some("h".into()),
                program: None, source_version: None, node_test: false,
                subgraph: None,
                seed: None,
                at_unix: 0,
            },
            ExecEvent::NodeKicked { color: color(), node_id: "src".into(), frames: vec![], firing: true, payload: None, port_snapshot: None, at_unix: 0 },
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
                ExecEvent::ExecutionCompleted { color: color(), at_unix: 7 },
            ]);
        }
        rows
    }

    fn project_all(rows: &[ExecEvent]) -> Vec<Value> {
        let mut projector = ExecutionProjector::new(color(), ProgramLookup::Found(program()), "p".into());
        rows.iter()
            .flat_map(|r| projector.project(r))
            .map(|e| serde_json::to_value(e).expect("event json"))
            .collect()
    }

    #[test]
    fn inherited_firing_keeps_answers_in_order_under_its_original_program() {
        let mut rows = run_rows(true);
        for node in ["src", "sink"] {
            rows.push(ExecEvent::LogLine {
                color: color(), node_id: node.into(), frames: vec![], level: "info".into(),
                message: format!("original {node}"), at_unix: 2, at_unix_ms: Some(2000), seq: Some(1),
            });
            rows.push(ExecEvent::CostReported {
                color: color(), node_id: node.into(), frames: vec![], cost_id: node.into(),
                service: "provider".into(), model: None, amount_usd: Some(0.25), billed: false,
                origin: weft_core::CredentialOwner::TheirOwn, metadata: Value::Null, at_unix: 2,
            });
        }
        rows.splice(3..3, [
            ExecEvent::NodeSuspended {
                color: color(), node_id: "src".into(), frames: vec![], token: "answer".into(), at_unix: 1,
            },
            ExecEvent::SuspensionResolved { color: color(), token: "answer".into(), value: json!("approved"), at_unix: 1 },
            ExecEvent::NodeResumed {
                color: color(), node_id: "src".into(), frames: vec![], token: Some("answer".into()), at_unix: 1,
            },
        ]);
        let chain = SeedChain { ancestors: vec![weft_journal::seed::Ancestor {
            color: color(), project: program(), rows,
        }] };
        let child = uuid::Uuid::new_v4();
        let seed = weft_journal::events::Seed {
            parent: color(), origins: [(Located::top("src"), color())].into(),
        };
        let events: Vec<Value> = inherited_events(&chain, &chain.materialize().unwrap(), &seed, child, "p").unwrap()
            .into_iter().map(|event| serde_json::to_value(event).unwrap()).collect();
        let logs = inherited_logs(&chain, &seed);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "original src");
        assert_eq!(logs[0].inherited_from, Some(color()));
        assert_eq!(kinds(&events), vec![
            ("node_started".into(), "src".into()),
            ("node_suspended".into(), "src".into()),
            ("node_resumed".into(), "src".into()),
            ("node_completed".into(), "src".into()),
            ("cost_reported".into(), "".into()),
        ]);
        assert_eq!(events[2]["value"], "approved");
        assert_eq!(events[3]["output"]["out"], 9);
        assert_eq!(events[4]["node_id"], "src");
        assert_eq!(events[4]["amount_usd"], 0.25);
        for event in events {
            assert_eq!(event["color"], child.to_string());
            assert_eq!(event["inherited_from"], color().to_string());
        }
    }

    #[test]
    fn a_boundary_created_by_seed_inputs_is_visible_in_the_next_seed() {
        let middle = uuid::Uuid::new_v4();
        let child = uuid::Uuid::new_v4();
        let mut birth = run_rows(true).remove(0);
        if let ExecEvent::ExecutionStarted { color: c, seed, .. } = &mut birth {
            *c = middle;
            *seed = Some(weft_journal::events::Seed {
                parent: color(), origins: [(Located::top("src"), color())].into(),
            });
        }
        let chain = SeedChain { ancestors: vec![
            weft_journal::seed::Ancestor { color: color(), project: program(), rows: run_rows(true) },
            weft_journal::seed::Ancestor { color: middle, project: program(), rows: vec![birth] },
        ] };
        let seed = weft_journal::events::Seed { parent: middle, origins: [(Located::top("g__in"), middle)].into() };
        let events: Vec<Value> = inherited_events(&chain, &chain.materialize().unwrap(), &seed, child, "p").unwrap()
            .into_iter().map(|event| serde_json::to_value(event).unwrap()).collect();
        assert_eq!(kinds(&events), vec![("node_started".into(), "g__in".into()), ("node_completed".into(), "g__in".into())]);
        assert_eq!(events[1]["output"], json!({"x": 9}));
        assert!(events.iter().all(|event| event["inherited_from"] == middle.to_string()));
    }

    fn kinds(events: &[Value]) -> Vec<(String, String)> {
        events
            .iter()
            .map(|e| {
                (
                    e["kind"].as_str().unwrap_or("").to_string(),
                    e.get("node").and_then(Value::as_str).unwrap_or("").to_string(),
                )
            })
            .collect()
    }

    /// The editor's wire shape is unchanged: a firing's start carries
    /// the input it received, its completion the output it emitted, the
    /// run's completion the outputs, and the boundaries that never
    /// journal a row are painted as started and completed nodes right
    /// after the row that fired them (the emission that made them
    /// ready, as the live engine fires them).
    #[test]
    fn a_run_through_a_group_paints_the_same_events_as_before() {
        let events = project_all(&run_rows(true));
        let k = kinds(&events);
        let expect: Vec<(&str, &str)> = vec![
            ("execution_started", ""),
            ("node_started", "src"),
            ("node_started", "g__in"),
            ("node_completed", "g__in"),
            ("node_completed", "src"),
            ("node_started", "inner"),
            ("node_started", "g__out"),
            ("node_completed", "g__out"),
            ("node_completed", "inner"),
            ("node_started", "sink"),
            ("node_completed", "sink"),
            ("execution_completed", ""),
        ];
        let expect: Vec<(String, String)> = expect.into_iter().map(|(a, b)| (a.into(), b.into())).collect();
        assert_eq!(k, expect, "{events:#?}");
        let by = |kind: &str, node: &str| {
            events.iter().find(|e| e["kind"] == kind && e["node"] == node).unwrap_or_else(|| panic!("{kind} {node}"))
        };
        assert_eq!(by("node_completed", "src")["output"], json!({ "out": 9, "flow": true }));
        assert_eq!(by("node_started", "g__in")["input"], json!({ "x": 9, "_should_flow": true }));
        assert_eq!(by("node_completed", "g__in")["output"], json!({ "x": 9 }));
        assert_eq!(by("node_started", "inner")["input"], json!({ "in": 9 }));
        assert_eq!(by("node_started", "sink")["input"], json!({ "in": 10 }));
        assert_eq!(by("node_started", "sink")["closed_ports"], json!([]));
        let done = events.iter().find(|e| e["kind"] == "execution_completed").unwrap();
        assert_eq!(done["outputs"], json!({ "src": { "out": 9, "flow": true }, "inner": { "out": 10 } }), "boundaries are not a person's nodes");
    }

    /// A gated-off group paints its boundary skipped with the reason,
    /// and the consumer past it started with the closed port listed.
    #[test]
    fn a_gated_off_group_paints_a_skip_with_its_reason() {
        let mut rows = run_rows(false);
        rows.push(started("sink", 3));
        rows.push(ExecEvent::NodeSkipped {
            color: color(),
            node_id: "sink".into(),
            frames: vec![],
            reason: weft_core::exec::skip::SkipReason::RequiredInputClosed { port: "in".into() },
            at_unix: 3,
        });
        let events = project_all(&rows);
        let boundary = events.iter().find(|e| e["kind"] == "node_skipped" && e["node"] == "g__in").expect("boundary skip painted");
        assert_eq!(boundary["reason"], json!({ "kind": "did_not_flow" }));
        let sink = events.iter().find(|e| e["kind"] == "node_started" && e["node"] == "sink").expect("sink started");
        assert_eq!(sink["closed_ports"], json!(["in"]));
        let skipped = events.iter().find(|e| e["kind"] == "node_skipped" && e["node"] == "sink").expect("sink skipped");
        assert_eq!(skipped["closed_ports"], json!(["in"]));
    }

    /// A projection opened mid-flight and caught up on the earlier rows
    /// paints the rest exactly as one that saw the run from the start:
    /// what a dispatcher pod that boots during a run relies on.
    #[test]
    fn a_projection_caught_up_mid_flight_paints_the_same_tail() {
        let rows = run_rows(true);
        let from_start = project_all(&rows);
        let split = 6;
        let mut late = ExecutionProjector::new(color(), ProgramLookup::Found(program()), "p".into());
        for r in &rows[..split] {
            late.project(r);
        }
        let tail: Vec<Value> = rows[split..]
            .iter()
            .flat_map(|r| late.project(r))
            .map(|e| serde_json::to_value(e).expect("event json"))
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
        let mut projector = ExecutionProjector::new(color(), ProgramLookup::Found(program()), "p".into());
        let row = ExecEvent::LoopInstantiated { color: color(), group_id: "nope".into(), parent_frames: vec![], at_unix: 1 };
        assert!(only_corruptions(&projector.project(&row)));
        let row = ExecEvent::LoopOutFired { color: color(), group_id: "nope".into(), parent_frames: vec![], index: 0, at_unix: 1 };
        assert!(only_corruptions(&projector.project(&row)));
    }

    #[test]
    fn a_run_with_no_program_paints_only_what_needs_none() {
        let mut projector = ExecutionProjector::new(color(), ProgramLookup::NoProgram, "p".into());
        let rows = [
            ExecEvent::ExecutionStarted {
                color: color(),
                project_id: "p".into(),
                entry_node: "probe".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: None,
                program: None, source_version: None, node_test: true,
                subgraph: None,
                seed: None,
                at_unix: 0,
            },
            started("probe", 1),
            ExecEvent::ExecutionCompleted { color: color(), at_unix: 2 },
        ];
        let events: Vec<Value> = rows
            .iter()
            .flat_map(|r| projector.project(r))
            .map(|e| serde_json::to_value(e).expect("event json"))
            .collect();
        let k: Vec<String> = events.iter().map(|e| e["kind"].as_str().unwrap().to_string()).collect();
        assert_eq!(k, vec!["execution_started", "execution_completed"]);
        assert_eq!(events[1]["outputs"], Value::Null);
    }

    /// A node row the fold rejected (a second start over an open
    /// firing, a node the program does not have) paints nothing: the
    /// only thing at its location is an earlier firing, and painting
    /// that under the new row's stamp would contradict the corruption
    /// the same row surfaced.
    #[test]
    fn a_rejected_node_row_paints_only_its_corruption() {
        let mut projector = ExecutionProjector::new(color(), ProgramLookup::Found(program()), "p".into());
        let mut rows = run_rows(true);
        rows.truncate(3);
        for r in &rows {
            projector.project(r);
        }
        assert!(only_corruptions(&projector.project(&started("src", 9))), "a duplicate start paints its corruption");
        assert!(only_corruptions(&projector.project(&started("ghost", 9))), "an unknown node paints its corruption");
        assert!(only_corruptions(&projector.project(&completed("ghost", 9))));
        // The open firing is still painted by its own rows.
        assert!(!projector.project(&completed("src", 10)).is_empty());
    }
}
