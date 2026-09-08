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
use weft_core::exec::NodeExecutionStatus;
use weft_core::primitive::LoopInstanceKey;
use weft_core::ProjectDefinition;
use weft_journal::{ExecEvent, Fold, FoldEffects};

use crate::events::DispatcherEvent;

/// What a reader that folds a color's journal found when it went for
/// the run's program.
pub enum ProgramLookup {
    Found(Arc<ProjectDefinition>),
    /// The color has no program at all: a node self-test (its birth
    /// row pins no definition hash), or a color that never started. A
    /// node row on such a color is a writer bug.
    NoProgram,
    /// The birth row pins a hash, but the definition history it points
    /// into is gone: the project was removed (the history is deleted
    /// with the project row, and a project registered again under the
    /// same id starts a fresh one), while the execution's rows outlive
    /// it on purpose. The run reads without its program.
    Deleted,
    /// The program cannot be found and no retry will change that: the
    /// birth row no longer decodes, the owner row is gone, or the
    /// stored definition no longer reads. `weft clean` removes the run.
    Unreadable(String),
}

impl ProgramLookup {
    /// The program to fold with, when there is one.
    pub fn program(&self) -> Option<Arc<ProjectDefinition>> {
        match self {
            Self::Found(p) => Some(p.clone()),
            Self::NoProgram | Self::Deleted | Self::Unreadable(_) => None,
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
    node_rows_expected: bool,
    color: weft_core::Color,
    project_id: String,
}

impl ExecutionProjector {
    pub fn new(color: weft_core::Color, program: ProgramLookup, project_id: String) -> Self {
        let node_rows_expected = matches!(program, ProgramLookup::Deleted | ProgramLookup::Unreadable(_));
        Self { fold: program.program().map(|p| Fold::new(color, p)), node_rows_expected, color, project_id }
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
        let project_id = self.project_id.clone();
        let color = self.color;
        let at_unix = ev.at_unix();
        let effects = match self.fold.as_mut() {
            Some(fold) => fold.apply(ev),
            None => {
                if needs_program(ev) {
                    if !self.node_rows_expected {
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
            ExecEvent::ExecutionStarted { entry_node, .. } => {
                vec![DispatcherEvent::ExecutionStarted {
                    color, at_unix,
                    entry_node: entry_node.clone(),
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
                    project_id,
                }]
            }
            ExecEvent::NodeFailed { node_id, frames, error, .. } => {
                vec![DispatcherEvent::NodeFailed {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    error: error.clone(),
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
                    project_id,
                }]
            }
            ExecEvent::NodeCancelled { node_id, frames, reason, .. } => {
                vec![DispatcherEvent::NodeCancelled {
                    color, at_unix,
                    node: node_id.clone(),
                    frames: frames.clone(),
                    reason: reason.clone(),
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
            out.extend(boundary_events(color, &self.project_id, at_unix, boundary));
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
            })
    }
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
            project_id: project_id.to_string(),
        },
        NodeExecutionStatus::Failed => DispatcherEvent::NodeFailed {
            color,
            at_unix,
            node,
            frames,
            error: error.clone().expect("a failed boundary carries its error"),
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
        // The history is deleted with its project (`project_definition`
        // cascades on the project row) and a project registered again
        // under the same id after that starts a fresh history, while
        // the execution's rows outlive both on purpose. So a missing
        // hash is never told apart from a removed project by whether a
        // project row exists now: the run reads without its program.
        tracing::warn!(
            target: "weft_dispatcher::projection",
            %color, project_id = %owner.project_id, %hash,
            "no recorded definition for this execution's hash (the project was removed); the run \
             is shown without the values its journal would derive"
        );
        return Ok(ProgramLookup::Deleted);
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
        assert!(ProgramLookup::Found(program()).program().is_some());
        assert!(ProgramLookup::NoProgram.program().is_none());
        assert!(ProgramLookup::Deleted.program().is_none());
        assert!(ProgramLookup::Unreadable("bad hash".into()).program().is_none());
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
                node_test: false,
                subgraph: None,
                at_unix: 0,
            },
            ExecEvent::NodeKicked { color: color(), node_id: "src".into(), firing: true, payload: None, port_snapshot: None, at_unix: 0 },
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
        assert_eq!(done["outputs"], json!({ "src": { "out": 9, "flow": true }, "inner": { "out": 10 }, "g__in": { "x": 9 }, "g__out": { "y": 10 } }));
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
                node_test: true,
                subgraph: None,
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
