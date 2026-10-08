//! What the dispatcher tells a client happened: one [`DispatcherEvent`]
//! per thing, with its delivery identity ([`IdentifiedEvent`]). The live
//! stream (`/events/project/{id}`) and a run's replay
//! (`/executions/{id}/replay`) answer the same rows, and every Rust
//! reader (the CLI's `weft events`, `weft logs`, `weft follow`) decodes
//! them through these types.

use serde::{Deserialize, Serialize};

use crate::frames::LoopFrames;
use crate::ExecutionId;

/// An event and its delivery identity. A run's recorded events take theirs
/// from their place in its record (the row's `seq`, the event's index in
/// it), so replay and live delivery identify the same event without
/// comparing timestamps or payload contents.
// SYNC: IdentifiedEvent.event_id <-> extension-vscode/src/execFollower.ts DispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentifiedEvent<T> {
    pub event_id: String,
    #[serde(flatten)]
    pub event: T,
}

impl<T> IdentifiedEvent<T> {
    /// The event at `index` of its run's record row `seq`.
    pub fn recorded(seq: i32, index: u32, event: T) -> Self {
        Self { event_id: format!("journal:{seq}:{index}"), event }
    }

    pub fn transient(event: T) -> Self {
        Self { event_id: format!("live:{}", uuid::Uuid::new_v4()), event }
    }

    pub fn project<U>(self, project: impl FnOnce(T) -> Vec<U>) -> Vec<IdentifiedEvent<U>> {
        project(self.event).into_iter().enumerate().map(|(index, event)| IdentifiedEvent {
            event_id: format!("{}:{index}", self.event_id), event,
        }).collect()
    }
}

pub type LiveEvent = IdentifiedEvent<DispatcherEvent>;


/// An event the dispatcher publishes about some piece of runtime
/// state changing. Tagged enum so SSE serialization matches the
/// spec in the design doc.
// Every event projected from a journal row carries that row's `at_unix`
// (the journal's own stamp), so a replay renders when each thing happened
// rather than when it was read.
// SYNC: DispatcherEvent <-> extension-vscode/src/execFollower.ts DispatcherEvent, weavemind/website/src/lib/graph/dispatcher-host.ts translateDispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DispatcherEvent {
    /// `subgraph` is the node set the run is held to (`None` for the
    /// whole graph): the graph paints every other node as not in this
    /// run. `seed` names the run this one inherits from and what it
    /// re-ran, for the run's banner. `phase` says what the execution
    /// is for: a run of the graph (`fire`), or the setup an `infra
    /// start` or an activation runs, which the editor shows as that verb
    /// working rather than as a run to stop.
    ExecutionStarted {
        execution_id: ExecutionId,
        entry_node: String,
        phase: crate::context::Phase,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subgraph: Option<Vec<crate::frames::Located>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seed: Option<crate::run_spec::Seed>,
        project_id: uuid::Uuid,
        /// Who the run is for, when it is one instance's run. Carried for
        /// any client following the run; no client reads it today.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        instance: Option<crate::instance::InstanceId>,
        at_unix: u64,
    },
    ExecutionCompleted { execution_id: ExecutionId, project_id: uuid::Uuid, outputs: serde_json::Value, at_unix: u64 },
    ExecutionFailed { execution_id: ExecutionId, project_id: uuid::Uuid, error: String, at_unix: u64 },
    /// `cause` is the structured who-or-what behind the cancel (`reason`
    /// is its text). `None` only for a journal row written before the
    /// cause existed; skipped on the wire when absent so the TS peers'
    /// optional (`cause?`) types match reality instead of decoding null.
    ExecutionCancelled {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cause: Option<crate::exec::CancelCause>,
        at_unix: u64,
    },
    /// The run tagged itself (`ctx.tag_execution`); the inspector shows
    /// the tags on the run. `tags` is this call's list, not the run's
    /// cumulative set.
    ExecutionTagged { execution_id: ExecutionId, project_id: uuid::Uuid, tags: Vec<String>, at_unix: u64 },
    /// The run was erased (`weft clean`, a prune, the editor's delete):
    /// its journal, its storage and the wake signals it was parked on
    /// are gone. Rides NOTIFY rather than the journal, since the
    /// journal is what just went; every client drops the run from its
    /// lists and re-reads the project's verbs, because a run parked on
    /// a question counted as preserved state until now.
    ExecutionDeleted { execution_id: ExecutionId, project_id: uuid::Uuid },
    /// Every node event carries `inherited_from` when the firing was
    /// not this run's own but taken from the run it was seeded from
    /// (`weft run --seed`): the row is the seed's, painted here so the
    /// graph shows the reused value and marks it inherited. Absent on
    /// the wire for the run's own firings. `provided_ports` on a start
    /// names input ports receiving supplied values; absent when none.
    NodeStarted {
        execution_id: ExecutionId,
        node: String,
        frames: LoopFrames,
        input: serde_json::Value,
        closed_ports: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        provided_ports: Vec<String>,
        // SYNC: input origins <-> extension-vscode/src/execFollower.ts DispatcherEvent, packages/weft-graph/src/protocol.ts NodeExecEvent, packages/weft-graph/src/webview/lib/types/index.ts NodeExecution
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        backup_ports: Vec<String>,
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        inherited_ports: std::collections::BTreeMap<String, ExecutionId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<ExecutionId>,
        project_id: uuid::Uuid,
        at_unix: u64,
    },
    NodeSuspended { execution_id: ExecutionId, node: String, frames: LoopFrames, token: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<ExecutionId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeResumed { execution_id: ExecutionId, node: String, frames: LoopFrames, token: Option<String>, value: Option<serde_json::Value>, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<ExecutionId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeCancelled { execution_id: ExecutionId, node: String, frames: LoopFrames, reason: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<ExecutionId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeCompleted { execution_id: ExecutionId, node: String, frames: LoopFrames, output: serde_json::Value, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<ExecutionId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeFailed { execution_id: ExecutionId, node: String, frames: LoopFrames, error: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<ExecutionId>, project_id: uuid::Uuid, at_unix: u64 },
    /// `reason` says WHY: the author's `_should_flow` said no, or an
    /// input the node needed never arrived. A decision and a consequence
    /// look identical on the graph without it.
    /// `None` only for a journal row written before the field existed
    /// (the UI renders "reason not recorded"); every live writer sends
    /// `Some`.
    NodeSkipped { execution_id: ExecutionId, node: String, frames: LoopFrames, closed_ports: Vec<String>, reason: Option<crate::exec::skip::SkipReason>, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<ExecutionId>, project_id: uuid::Uuid, at_unix: u64 },
    /// A loop instance was created at `parent_frames`. The inspector
    /// uses this to render a "Loop opened" marker at the loop's box.
    // SYNC: LoopInstantiated <-> extension-vscode/src/execFollower.ts loop_instantiated, packages/weft-graph/src/protocol.ts LoopInspectorEvent 'instantiated'
    LoopInstantiated {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        /// Effective iteration CAP; `None` for an uncapped loop (a
        /// done-driven or stream-driven loop with no `max_iters`),
        /// whose iteration count is unknowable up front.
        iter_cap: Option<u32>,
        parallel: bool,
        at_unix: u64,
    },
    /// An iteration of the loop launched. Inspector renders an
    /// iteration marker at body_frames.
    LoopIterationLaunched {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        at_unix: u64,
    },
    /// LoopOut fired for iteration `index`. The per-port gather /
    /// carry writes ride on the journal but are NOT mirrored to the
    /// inspector stream: the renderer reads the loop's outward emit
    /// (a normal pulse) and the per-iteration body activity, not the
    /// LoopOut firing's raw write map.
    LoopOutFired {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        done_vote: Option<bool>,
        at_unix: u64,
    },
    /// The loop terminated outward and emitted its outer outputs.
    LoopTerminated {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        reason: crate::primitive::LoopTerminationReason,
        at_unix: u64,
    },
    /// One metered call's cost record landed on the journal, attributed to
    /// the exact firing (`node_id` + `frames`). `amount_usd` `None` = the
    /// meter could not resolve the figure (an honest unknown).
    CostReported {
        execution_id: ExecutionId,
        // SYNC: CostReported <-> extension-vscode/src/execFollower.ts DispatcherEvent cost_reported, packages/weft-graph/src/protocol.ts execCost
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<ExecutionId>,
        project_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        /// Stable per-record identity; the webview dedups on it (the same
        /// journal row can arrive via both the replay and the live stream).
        cost_id: String,
        service: String,
        amount_usd: Option<f64>,
        /// Whose credential the call spent (`their-own` or `ours`), so a
        /// client can say whose account a figure landed on.
        origin: crate::CredentialOwner,
        at_unix: u64,
    },
    /// A firing changed a stored file's content in place (the journal's
    /// `FileEdited`): the inspector lists it on that firing's "Files
    /// edited" card. `inherited_from` as on [`Self::CostReported`].
    FileEdited {
        execution_id: ExecutionId,
        // SYNC: FileEdited <-> extension-vscode/src/execFollower.ts DispatcherEvent file_edited, packages/weft-graph/src/protocol.ts execFileEdit
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<ExecutionId>,
        project_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        edit: crate::storage::FileEdit,
        at_unix: u64,
    },
    TriggerUrlChanged { project_id: uuid::Uuid, node_id: String, url: String },
    ProjectRegistered { project_id: uuid::Uuid, name: String },
    ProjectActivated { project_id: uuid::Uuid },
    ProjectDeactivated { project_id: uuid::Uuid },
    /// A project lifecycle axis flipped: entering/leaving a
    /// transitional state (activating, deactivating, building,
    /// cancelling_build) or landing at rest. Carries both axes so a
    /// client can render the new state without a round-trip; clients
    /// that prefer one code path just refetch `/status` on receipt.
    /// This is what makes backend-owned transitional state observable
    /// in near-real-time (the backend-owns-state rule has no teeth
    /// without it).
    ProjectTransitionChanged { project_id: uuid::Uuid, status: String, transition: String },
    /// Infra node transitioned between status values. Catch-all for
    /// supervisor-driven state changes the extension renders as a
    /// per-node badge.
    InfraStatusChanged { project_id: uuid::Uuid, node_id: String, status: String },
    /// Supervisor declared an infra node flaky; the extension shows
    /// the orange banner with `reason`.
    InfraFlaky { project_id: uuid::Uuid, node_id: String, reason: String },
    /// Inverse of InfraFlaky.
    InfraRecovered { project_id: uuid::Uuid, node_id: String },
    /// Supervisor finished terminating an infra node; the
    /// `infra_node` row has been deleted.
    InfraTerminated { project_id: uuid::Uuid, node_id: String },
    /// Supervisor couldn't parse the project's
    /// `health_protocols_json`. The user's config is broken; the
    /// supervisor fell back to defaults. Surfaced as a banner in
    /// the action bar so the user sees their config didn't take.
    InfraConfigError { project_id: uuid::Uuid, error: String },
    /// A bus participant came online. `bus_id` is the channel's uuid
    /// (same one embedded in the bus marker), so the inspector groups
    /// multiple buses cleanly. `offset` is the bus-local position used
    /// to tiebreak same-second entries. `at_unix` is the journal's
    /// stamp so replay renders honest timestamps, not "now".
    BusJoined {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },
    /// A bus participant dropped. Pairs with `BusJoined` for the same
    /// `(bus_id, name)`.
    BusLeft {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },
    /// One journal-aggregation window of a bus's messages (one row per
    /// bus per window; default 1s). A journaled bus's `messages` carry
    /// every message in the window (senders, kinds, payloads); an
    /// ephemeral bus's `messages` are empty and `totals` (count + bytes
    /// per sender/kind) are the whole story. The inspector unpacks
    /// `messages` into its per-message log and renders a summary line
    /// for a window that carries only totals.
    // SYNC: BusWindow <-> crates/weft-journal/src/events.rs BusWindow, packages/weft-graph/src/protocol.ts BusInspectorEvent 'window', extension-vscode/src/execFollower.ts DispatcherEvent 'bus_window'
    BusWindow {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        bus_id: String,
        first_offset: u64,
        last_offset: u64,
        messages: Vec<crate::bus::WindowedBusMessage>,
        totals: Vec<crate::bus::BusWindowTotal>,
        at_unix: u64,
    },
    /// The bus was closed. Inspector renders an explicit
    /// `* the bus closed here` marker; replay cursors stop here.
    BusClosed {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        at_unix: u64,
    },
    /// A live caller attached to this execution. First event in the
    /// caller stream; the inspector opens a "caller" panel on the run.
    CallerConnected {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        offset: u64,
        protocol: String,
        at_unix: u64,
    },
    /// What the conversation said during one window, both directions in
    /// one row, the same shape and clock as a bus window. A message
    /// whose content was not kept still appears carrying its size.
    // SYNC: CallerWindow <-> crates/weft-journal/src/events.rs CallerWindow, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'window', extension-vscode/src/execFollower.ts DispatcherEvent 'caller_window'
    CallerWindow {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        first_offset: u64,
        last_offset: u64,
        messages: Vec<crate::stream_journal::WindowedCallerMessage>,
        totals: Vec<crate::stream_journal::CallerWindowTotal>,
        at_unix: u64,
    },
    /// A node error surfaced to the caller.
    CallerErrored {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        offset: u64,
        message: String,
        at_unix: u64,
    },
    /// The caller is gone (response complete OR disconnected). Last
    /// event in the caller stream; replay cursors stop here.
    CallerDisconnected {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        offset: u64,
        reason: String,
        at_unix: u64,
    },
    /// Graph-level participation: a node was wired to a bus. Derived
    /// from the pulses the fold puts on the wires whose value carries a
    /// bus marker on a `Bus` port: both source and target nodes are
    /// participants.
    /// `ephemeral` is sniffed from the marker JSON itself (which
    /// encodes the bus's mode) so the inspector can render a mode
    /// badge in the panel header without a separate journal event.
    BusParticipant {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        bus_id: String,
        node_id: String,
        ephemeral: bool,
    },
    /// A journal row could not be applied during fold (corruption).
    /// Surfaced one-shot at replay time per affected row so the
    /// inspector can render a muted "N journal rows corrupted"
    /// line. Not alarming by design: corrupt rows are a real but
    /// rare event the user only investigates if they look.
    JournalCorruption {
        execution_id: ExecutionId,
        project_id: uuid::Uuid,
        site: crate::primitive::CorruptionSite,
        reason: String,
    },
}

impl DispatcherEvent {
    pub fn project_id(&self) -> uuid::Uuid {
        match self {
            Self::ExecutionStarted { project_id, .. }
            | Self::ExecutionCompleted { project_id, .. }
            | Self::ExecutionFailed { project_id, .. }
            | Self::ExecutionCancelled { project_id, .. }
            | Self::ExecutionTagged { project_id, .. }
            | Self::ExecutionDeleted { project_id, .. }
            | Self::NodeStarted { project_id, .. }
            | Self::NodeSuspended { project_id, .. }
            | Self::NodeResumed { project_id, .. }
            | Self::NodeCancelled { project_id, .. }
            | Self::NodeCompleted { project_id, .. }
            | Self::NodeFailed { project_id, .. }
            | Self::NodeSkipped { project_id, .. }
            | Self::LoopInstantiated { project_id, .. }
            | Self::LoopIterationLaunched { project_id, .. }
            | Self::LoopOutFired { project_id, .. }
            | Self::LoopTerminated { project_id, .. }
            | Self::CostReported { project_id, .. }
            | Self::FileEdited { project_id, .. }
            | Self::TriggerUrlChanged { project_id, .. }
            | Self::ProjectRegistered { project_id, .. }
            | Self::ProjectActivated { project_id }
            | Self::ProjectDeactivated { project_id }
            | Self::ProjectTransitionChanged { project_id, .. }
            | Self::InfraStatusChanged { project_id, .. }
            | Self::InfraFlaky { project_id, .. }
            | Self::InfraRecovered { project_id, .. }
            | Self::InfraTerminated { project_id, .. }
            | Self::InfraConfigError { project_id, .. }
            | Self::BusJoined { project_id, .. }
            | Self::BusLeft { project_id, .. }
            | Self::BusWindow { project_id, .. }
            | Self::BusClosed { project_id, .. }
            | Self::BusParticipant { project_id, .. }
            | Self::CallerConnected { project_id, .. }
            | Self::CallerWindow { project_id, .. }
            | Self::CallerErrored { project_id, .. }
            | Self::CallerDisconnected { project_id, .. }
            | Self::JournalCorruption { project_id, .. } => *project_id,
        }
    }

    /// The loop and call frames of the one firing this event is about
    /// (a node's lifecycle, a cost or a file edit at that firing); `None`
    /// for an event about the run, the project, a loop as a whole, or
    /// anything else that is not one firing.
    pub fn firing_frames(&self) -> Option<&LoopFrames> {
        match self {
            Self::NodeStarted { frames, .. }
            | Self::NodeSuspended { frames, .. }
            | Self::NodeResumed { frames, .. }
            | Self::NodeCancelled { frames, .. }
            | Self::NodeCompleted { frames, .. }
            | Self::NodeFailed { frames, .. }
            | Self::NodeSkipped { frames, .. }
            | Self::CostReported { frames, .. }
            | Self::FileEdited { frames, .. } => Some(frames),
            Self::ExecutionStarted { .. }
            | Self::ExecutionCompleted { .. }
            | Self::ExecutionFailed { .. }
            | Self::ExecutionCancelled { .. }
            | Self::ExecutionTagged { .. }
            | Self::ExecutionDeleted { .. }
            | Self::LoopInstantiated { .. }
            | Self::LoopIterationLaunched { .. }
            | Self::LoopOutFired { .. }
            | Self::LoopTerminated { .. }
            | Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::ProjectDeactivated { .. }
            | Self::ProjectTransitionChanged { .. }
            | Self::InfraStatusChanged { .. }
            | Self::InfraFlaky { .. }
            | Self::InfraRecovered { .. }
            | Self::InfraTerminated { .. }
            | Self::InfraConfigError { .. }
            | Self::BusJoined { .. }
            | Self::BusLeft { .. }
            | Self::BusWindow { .. }
            | Self::BusClosed { .. }
            | Self::BusParticipant { .. }
            | Self::CallerConnected { .. }
            | Self::CallerWindow { .. }
            | Self::CallerErrored { .. }
            | Self::CallerDisconnected { .. }
            | Self::JournalCorruption { .. } => None,
        }
    }

    /// Whether this event ends its execution (a cancel is an end too).
    // SYNC: DispatcherEvent::is_execution_terminal <-> crates/weft-journal/src/events.rs ExecEvent::is_execution_terminal, crates/weft-journal/src/events.rs EXECUTION_TERMINAL_KINDS_SQL
    pub fn is_execution_terminal(&self) -> bool {
        matches!(
            self,
            Self::ExecutionCompleted { .. } | Self::ExecutionFailed { .. } | Self::ExecutionCancelled { .. }
        )
    }

    pub fn execution_id(&self) -> Option<ExecutionId> {
        match self {
            Self::ExecutionStarted { execution_id, .. }
            | Self::ExecutionCompleted { execution_id, .. }
            | Self::ExecutionFailed { execution_id, .. }
            | Self::ExecutionCancelled { execution_id, .. }
            | Self::ExecutionTagged { execution_id, .. }
            | Self::ExecutionDeleted { execution_id, .. }
            | Self::NodeStarted { execution_id, .. }
            | Self::NodeSuspended { execution_id, .. }
            | Self::NodeResumed { execution_id, .. }
            | Self::NodeCancelled { execution_id, .. }
            | Self::NodeCompleted { execution_id, .. }
            | Self::NodeFailed { execution_id, .. }
            | Self::NodeSkipped { execution_id, .. }
            | Self::LoopInstantiated { execution_id, .. }
            | Self::LoopIterationLaunched { execution_id, .. }
            | Self::LoopOutFired { execution_id, .. }
            | Self::LoopTerminated { execution_id, .. }
            | Self::CostReported { execution_id, .. }
            | Self::FileEdited { execution_id, .. }
            | Self::BusJoined { execution_id, .. }
            | Self::BusLeft { execution_id, .. }
            | Self::BusWindow { execution_id, .. }
            | Self::BusClosed { execution_id, .. }
            | Self::BusParticipant { execution_id, .. }
            | Self::CallerConnected { execution_id, .. }
            | Self::CallerWindow { execution_id, .. }
            | Self::CallerErrored { execution_id, .. }
            | Self::CallerDisconnected { execution_id, .. }
            | Self::JournalCorruption { execution_id, .. } => Some(*execution_id),
            Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::ProjectDeactivated { .. }
            | Self::ProjectTransitionChanged { .. }
            | Self::InfraStatusChanged { .. }
            | Self::InfraFlaky { .. }
            | Self::InfraRecovered { .. }
            | Self::InfraTerminated { .. }
            | Self::InfraConfigError { .. } => None,
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn identities_survive_projection_and_wire_round_trips() {
        let event = DispatcherEvent::ExecutionCompleted {
            execution_id: uuid::Uuid::nil(), project_id: uuid::Uuid::from_u128(0x100),
            outputs: serde_json::json!({}), at_unix: 1,
        };
        let record = IdentifiedEvent::recorded(42, 3, event);
        let projected = record.clone().project(|event| vec![event.clone(), event]);
        assert_ne!(projected[0].event_id, projected[1].event_id);
        let replay = record.project(|event| vec![event.clone(), event]);
        assert_eq!(serde_json::to_value(&projected).unwrap(), serde_json::to_value(replay).unwrap());
        let json = serde_json::to_value(&projected[0]).unwrap();
        assert_eq!(json["event_id"], "journal:42:3:0");
        assert_eq!(json["kind"], "execution_completed");
        let decoded: LiveEvent = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.event_id, projected[0].event_id);
        let distinct = IdentifiedEvent::recorded(42, 4, decoded.event).project(|event| vec![event]);
        assert_ne!(distinct[0].event_id, projected[0].event_id);
    }
}
