//! Event-sourced execution state.
//!
//! The journal records one event per state change reported by the
//! worker (plus a few dispatcher-side events like NodeKicked at
//! fresh-run time). Folding the event log over the program rebuilds a
//! complete `ExecutionSnapshot` (see `crate::fold`): pulses,
//! executions, loops, active suspensions. Replay is the source of
//! truth.
//!
//! THE RULE every row is judged by: the journal records facts the
//! engine learned from OUTSIDE (a trigger payload, a node body's
//! emission, a person's answer, a log line, a cost, a stream take, a
//! cancellation). Everything the engine computed from those facts plus
//! the program is recomputed on read: which wires a value fanned out
//! on, which ports a firing closed, what a group boundary forwarded,
//! what a loop iteration received. A value therefore lives in the
//! journal exactly once, on the `PortEmitted` row of the emission that
//! produced it. A small derived fact may ride a row for the screen
//! (`NodeSkipped.reason`), but the resume never reads it.
//!
//! There is no compatibility decoding: a journal row written to an
//! older shape does not decode, the read that hits it fails naming
//! the color, and `weft clean` removes the execution.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::frames::LoopFrames;
use weft_core::primitive::{LoopTerminationReason, SignalSpec};
use weft_core::Color;

/// One event in the execution log. Append-only; events are never
/// edited or deleted by the dispatcher. User-initiated cleanup
/// (`weft clean`) is the only path that removes them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecEvent {
    ExecutionStarted {
        color: Color,
        project_id: String,
        entry_node: String,
        phase: weft_core::context::Phase,
        /// `running_definition_hash` snapshotted at execution-start
        /// time. Resumes of this color use THIS hash (not the
        /// project row's current hash) to fetch the project
        /// definition from the broker, so a webhook-triggered
        /// resume after a config edit runs the suspended execution
        /// against the SAME shape it was suspended on. Without
        /// this, a resume folds the OLD journal state but executes
        /// against the NEW topology / config, which is undefined
        /// behavior. `None` for a run that executes no project
        /// definition (a node self-test): a resume against such a
        /// color fails loudly as NotFound instead of resuming
        /// against a sentinel hash.
        definition_hash: Option<String>,
        /// True for a node self-test's execution identity: the color
        /// is real (cost attribution, broker scoping, a terminal
        /// event) but its lifecycle is owned by the test task, so
        /// project-lifecycle sweeps (cancel, wipe, drain counting)
        /// must not touch it.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        node_test: bool,
        /// The node set this execution is allowed to dispatch, or
        /// `None` for the whole graph. A trigger fire journals its
        /// computed program here (the fired trigger's downstream plus
        /// what that needs), so a pulse into another program's node is
        /// absorbed and a resume rebuilds the same boundary. A targeted
        /// manual run also records its selected nodes and dependencies.
        /// Untargeted manual runs and setup phases carry `None` (setup
        /// phases compute their scope engine-side). (Named
        /// `subgraph`, not `scope`:
        /// `NodeDefinition.scope` is a node's group-nesting path, a
        /// different thing entirely.)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subgraph: Option<Vec<String>>,
        at_unix: u64,
    },

    NodeKicked {
        color: Color,
        node_id: String,
        /// This kick is the FIRING trigger of the execution. Explicit,
        /// never inferred from `payload` presence: a fire with an empty
        /// body journals `"payload": null`, indistinguishable from an
        /// absent payload after the JSON round trip.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        firing: bool,
        payload: Option<Value>,
        /// The firing trigger's setup-time port snapshot, seeded onto
        /// its ports at dispatch. `None` for every other kicked root.
        /// On the journal so a resume refolds the same input.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port_snapshot: Option<Value>,
        at_unix: u64,
    },

    /// A node was absorbed into a dispatch: the pending pulses at its
    /// location marked Absorbed, a Running `NodeExecution` opened. What
    /// the firing received is not on the row: it is the pulses the
    /// fold absorbs at this point (every pending pulse at the node and
    /// frames, minus the items on its generator ports, which its live
    /// feed takes one by one).
    NodeStarted {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        at_unix: u64,
    },

    /// The firing's body returned. What it handed out is its
    /// `PortEmitted` rows; every declared output port it never
    /// mentioned closes at this point (the fold sweeps them, deriving
    /// the closures from the program), and a generator output's
    /// closure is the stream's clean end.
    NodeCompleted {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        at_unix: u64,
    },

    /// The firing failed with `error`. Ports already emitted keep their
    /// values; every other output closes, a generator output's closure
    /// carrying the error as a FAILED stream end.
    NodeFailed {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        error: String,
        at_unix: u64,
    },

    /// The firing did not run its body: every output closes. `reason`
    /// says WHY: the author's `_should_flow` said no, or an input it
    /// needed never arrived. A decision and a consequence look the same
    /// on the graph without it. The fold never needs it to rebuild the
    /// execution (the closures are the same either way); it is kept for
    /// the screen. A `ScopeSkipped` skip closes nothing: the scope's In
    /// boundary already closed the scope's outward surface.
    NodeSkipped {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        reason: weft_core::exec::skip::SkipReason,
        at_unix: u64,
    },

    NodeSuspended {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        token: String,
        at_unix: u64,
    },

    NodeResumed {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        /// Resume cause:
        /// - `Some(token)`: the firing was Suspended and a
        ///   `SuspensionResolved` for `token` arrived (its value is on
        ///   that row). The fold clears the `suspensions` and
        ///   `pending_deliveries` entries for `token`.
        /// - `None`: crashed-Running recovery (the firing was
        ///   Running when the worker crashed; a fresh worker is
        ///   re-driving it). No suspension token to clear.
        /// Either way the fold absorbs every pulse pending at the
        /// location, as it does for a `NodeStarted`: a resume can
        /// absorb fresh pulses that arrived while the firing was
        /// waiting, and the un-absorb path on a later crashed-Running
        /// recovery needs every absorbed pulse, not just the original
        /// dispatch's. Always written (`null` for a crash recovery): a
        /// missing field is a truncated row and fails to decode.
        #[serde(deserialize_with = "present")]
        token: Option<String>,
        at_unix: u64,
    },

    NodeCancelled {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        reason: String,
        at_unix: u64,
    },

    /// A node body emitted `value` on `port`. THE only row in the
    /// journal that carries a wire value, and it carries it once: the
    /// fold puts the pulses on every outgoing wire of the port itself
    /// (the program has the edges, and a wire's key path is read off
    /// the value at delivery), with ids derived from `emission_id`, so
    /// every later row that names a pulse (a stream take, a loop's item
    /// launch) resolves the same live and on replay. One row per port
    /// per `pulse_downstream` call; a stream item is one row.
    PortEmitted {
        color: Color,
        emission_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        port: String,
        /// Shared with the pulses the live engine put on the wires, so
        /// writing the row copies nothing (serde reads through the
        /// `Arc`).
        value: std::sync::Arc<Value>,
        at_unix: u64,
    },

    /// A node body closed `port` mid-firing (`ctx.close_port`): every
    /// wire off the port carries a closure from here on, the same
    /// shape the termination sweep would produce; on a generator
    /// output this is the stream's early end. A fact the body decided,
    /// so it has its own row; the termination sweep's closures do not.
    PortClosed {
        color: Color,
        emission_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        port: String,
        at_unix: u64,
    },

    /// A node tried to emit a value on `port` whose inferred type is not
    /// compatible with the port's declared type. The engine refused the
    /// value and closed the port instead (downstream sees null); this
    /// row IS that closure. NON-terminal: the node keeps running and its
    /// other ports emit normally. Folds into the matching
    /// `NodeExecution.port_warnings`.
    PortTypeMismatch {
        color: Color,
        emission_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        port: String,
        expected: String,
        actual: String,
        at_unix: u64,
    },

    /// Pulses a live in-process sink consumed OUTSIDE a dispatch: a
    /// running consumer's pull took a generator item, or the engine
    /// dropped items whose consumer already finished. Dispatch-time
    /// absorption rides `NodeStarted`/`NodeResumed`; this event is the
    /// same durability for the take path, so a refold never sees a
    /// taken item as Pending beside a terminal consumer record (which
    /// would re-dispatch the consumer, a double run).
    ///
    /// TODO: every stream item costs one `PortEmitted` + one of these
    /// (a journal write each); a multi-million-item stream needs
    /// windowed writes (the bus already batches its appends this way)
    /// before that volume is real.
    PulsesConsumed {
        color: Color,
        /// The consuming node (the pulses' target).
        node_id: String,
        frames: LoopFrames,
        pulse_ids: Vec<String>,
        at_unix: u64,
    },

    /// A `Loop` instance was created at `parent_frames` when `LoopIn`
    /// first fired for the loop. Everything about the instance (its
    /// config, its iteration cap, its outer input, its carry seeds) is
    /// a function of the LoopIn's own firing, which the fold rebuilds
    /// from the LoopIn's absorbed pulses plus the program.
    LoopInstantiated {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        at_unix: u64,
    },

    /// The engine launched body work for iteration `index` of the loop
    /// at `parent_frames`. For parallel loops, all N are launched
    /// upfront; for sequential, one per fire of `LoopOut`. The body's
    /// pulses (the `over` slice, the broadcast inputs, the carries as
    /// they stand, the implicit `index`) and the body's roots are what
    /// the instance and the program say they are at this point; the
    /// fold puts them on the wires from this row.
    LoopIterationLaunched {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        /// When this iteration's `over` item came from a STREAM
        /// (`over` on a `Generator[T]` port): the item pulse this
        /// launch consumed, absorbed by the fold in the same atomic
        /// row as the launch marker (the item's value comes off that
        /// pulse). `None` for list-driven and done-driven loops. Always
        /// written (`null` for the non-stream sources): a missing field
        /// must fail deserialization, never silently fold as "not a
        /// stream launch" (which would leave the item pulse un-absorbed
        /// and re-deliver it).
        #[serde(deserialize_with = "present")]
        stream_pulse: Option<String>,
        at_unix: u64,
    },

    /// `LoopOut` fired for iteration `index`. What the body wrote to
    /// each gather and carry port, and its `done` vote, are the pulses
    /// the LoopOut firing absorbed; the fold reads them off that
    /// record.
    LoopOutFired {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        at_unix: u64,
    },

    /// The stream driving a stream-`over` loop ENDED. Durable on
    /// purpose: the end's close pulse is consumed the moment it
    /// routes, so without this row a crash between "stream ended" and
    /// "loop terminated" (an iteration still in flight) would resume a
    /// loop that waits forever for a close that can never arrive
    /// again. Folds onto the instance's stream source.
    LoopStreamEnded {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        end: weft_core::primitive::StreamEnd,
        at_unix: u64,
    },

    /// The loop ended: all launched iterations fired their `LoopOut`
    /// AND a termination condition was satisfied, or it was cancelled
    /// or failed. On a clean end the loop's outward pulses (assembled
    /// gather lists + final carries) go on the wires from this row; on
    /// a failed or cancelled end its outward ports close instead.
    LoopTerminated {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        reason: LoopTerminationReason,
        at_unix: u64,
    },

    SuspensionRegistered {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        token: String,
        spec: SignalSpec,
        call_index: u32,
        at_unix: u64,
    },

    SuspensionResolved {
        color: Color,
        token: String,
        value: Value,
        at_unix: u64,
    },

    RunOutput {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        call_index: u32,
        name: String,
        value: Value,
        at_unix: u64,
    },

    /// One metered call's cost, as a provider meter measured it. The only
    /// producer is a meter (run by the runtime around the call); nodes have
    /// no way to state a cost. `amount_usd: None` = the meter could not
    /// resolve the figure (recorded AS unknown, never as $0). `billed` =
    /// the figure moved credits (a platform-billed call) as opposed to a
    /// measurement on a key the user holds; a data-model distinction for
    /// debugging and the ledger, not a UI one.
    CostReported {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        /// The record's stable identity (from the durable task that carried
        /// it). Consumers that see the same journal row more than once (a
        /// replay stream overlapping a live one) dedup on it.
        cost_id: String,
        service: String,
        model: Option<String>,
        amount_usd: Option<f64>,
        billed: bool,
        /// Whose key the call spent: the user's own, or the platform's
        /// (app.weavemind.ai). Part of the money trail (a figure without "whose key" is
        /// half an answer).
        origin: weft_core::CredentialOwner,
        metadata: Value,
        at_unix: u64,
    },

    LogLine {
        color: Color,
        /// The node that wrote it, and the iteration it was in: a log
        /// line is about one firing, and a graph where ten nodes log
        /// is unreadable without it. An empty id is a run-level line.
        node_id: String,
        frames: LoopFrames,
        level: String,
        message: String,
        /// The worker's clock at the write, in milliseconds (`at_unix`
        /// is its seconds), and the line's place among its firing's
        /// side effects. `None` when the writer had neither (a line
        /// that did not come off a worker's clock reads at the end of
        /// its second); always written, so a row lacking the field is
        /// a truncated row and fails to decode.
        #[serde(deserialize_with = "present")]
        at_unix_ms: Option<u64>,
        #[serde(deserialize_with = "present")]
        seq: Option<u64>,
        at_unix: u64,
    },

    /// A node tagged its own execution (`ctx.tag_execution`). The
    /// record of the act, for the inspector; the SELECTABLE copy a
    /// sibling's `ctx.stop_tagged` reads lives beside the execution row
    /// (`execution_tag`), written in the same transaction as this event.
    /// Not folded: tags are not execution state a resume rebuilds.
    ExecutionTagged {
        color: Color,
        tags: Vec<String>,
        at_unix: u64,
    },

    /// The execution ran to its end. The run's outputs are what its
    /// nodes emitted (the `PortEmitted` rows); the row is the terminal
    /// marker and nothing else.
    ExecutionCompleted {
        color: Color,
        at_unix: u64,
    },

    ExecutionFailed {
        color: Color,
        error: String,
        at_unix: u64,
    },

    ExecutionCancelled {
        color: Color,
        /// The cause in words, what a person reads (`cause.to_string()`
        /// when `cause` is set).
        reason: String,
        /// The structured cause: who or what stopped the run. Every
        /// live writer sets it; `None` is a cancel with only its words.
        /// Always written (`null` counts): a row lacking the field is a
        /// truncated row and fails to decode.
        #[serde(deserialize_with = "present")]
        cause: Option<weft_core::exec::CancelCause>,
        at_unix: u64,
    },

    BusJoined {
        color: Color,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },

    BusLeft {
        color: Color,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },

    /// One journal-aggregation window of a bus's messages: the pump
    /// ships ONE row per bus per window (default 1s) instead of a row
    /// per message. A journaled bus's `messages` carry every message
    /// (boundaries, senders, payloads all kept); an ephemeral bus's
    /// `messages` are empty and the `totals` rollup is the whole
    /// journaled story. A quiet bus degenerates to one message per
    /// window, so slow traffic reads exactly as before.
    BusWindow {
        color: Color,
        bus_id: String,
        first_offset: u64,
        last_offset: u64,
        messages: Vec<weft_core::bus::WindowedBusMessage>,
        totals: Vec<weft_core::bus::BusWindowTotal>,
        at_unix: u64,
    },

    BusClosed {
        color: Color,
        bus_id: String,
        offset: u64,
        at_unix: u64,
    },

    // ----- Live caller connection (mirrors the Bus* family) ----------
    //
    // A live `live_connection` run holds ONE caller for the execution, so
    // there is no `bus_id`: the color IS the connection's identity. The
    // exchange is recorded as a replayable per-color event stream the
    // graph view replays exactly like a bus. `offset` is the monotonic
    // per-execution position in the caller stream. Message payloads use
    // the same wire vocabulary as the bus's window rows
    // (`weft_core::bus::WirePayload`: tagged json-or-base64-bytes), so
    // every journaled exchange speaks one payload shape.

    /// The caller attached. The first event in any caller stream.
    CallerConnected {
        color: Color,
        offset: u64,
        /// `"http"` | `"websocket"` (the `Protocol` wire tag).
        protocol: String,
        at_unix: u64,
    },

    /// A message arrived FROM the caller (HTTP request body, WS inbound).
    // SYNC: CallerInbound <-> crates/weft-dispatcher/src/events.rs CallerInbound, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'inbound', extension-vscode/src/execFollower.ts DispatcherEvent 'caller_inbound'
    CallerInbound {
        color: Color,
        offset: u64,
        payload: weft_core::bus::WirePayload,
        payload_byte_size: u64,
        at_unix: u64,
    },

    /// A message went TO the caller (HTTP write/respond chunk, WS send).
    /// `terminal` marks the final outbound (HTTP respond/close, WS close)
    /// so the inspector renders "* the response completed here".
    // SYNC: CallerOutbound <-> crates/weft-dispatcher/src/events.rs CallerOutbound, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'outbound', extension-vscode/src/execFollower.ts DispatcherEvent 'caller_outbound'
    CallerOutbound {
        color: Color,
        offset: u64,
        payload: weft_core::bus::WirePayload,
        payload_byte_size: u64,
        terminal: bool,
        at_unix: u64,
    },

    /// A node error surfaced to the caller (the `error_mode` path:
    /// status/body before streaming, in-band error chunk after, WS close
    /// frame). Recorded so the exchange replay shows where it broke.
    CallerErrored {
        color: Color,
        offset: u64,
        message: String,
        at_unix: u64,
    },

    /// The caller is gone (response complete OR disconnected, the same
    /// event from the run's view). Last event in the caller stream.
    CallerDisconnected {
        color: Color,
        offset: u64,
        reason: String,
        at_unix: u64,
    },
}

/// An optional field that must be PRESENT on the row (`null` counts):
/// serde would otherwise read a missing `Option` as `None`, turning a
/// truncated row into a row that quietly says the other thing.
fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

impl ExecEvent {
    /// Whether this event ends the execution: completed, failed, or
    /// cancelled. The ONE definition of the terminal set in Rust; the
    /// SQL that filters on it lives in
    /// `weft-dispatcher/src/api/execution.rs` (`terminal_outcome`) and
    /// carries a marker back here.
    // SYNC: ExecEvent::is_execution_terminal <-> crates/weft-dispatcher/src/api/execution.rs terminal_outcome (SQL kind list), crates/weft-cli/src/commands/follow.rs is_terminal (SSE kind list), crates/weft-journal/src/tags.rs live_tagged_executions (SQL kind list)
    pub fn is_execution_terminal(&self) -> bool {
        matches!(
            self,
            Self::ExecutionCompleted { .. }
                | Self::ExecutionFailed { .. }
                | Self::ExecutionCancelled { .. }
        )
    }

    pub fn color(&self) -> Color {
        match self {
            Self::ExecutionStarted { color, .. }
            | Self::NodeKicked { color, .. }
            | Self::NodeStarted { color, .. }
            | Self::NodeCompleted { color, .. }
            | Self::NodeFailed { color, .. }
            | Self::NodeSkipped { color, .. }
            | Self::NodeSuspended { color, .. }
            | Self::NodeResumed { color, .. }
            | Self::NodeCancelled { color, .. }
            | Self::PortEmitted { color, .. }
            | Self::PortClosed { color, .. }
            | Self::PortTypeMismatch { color, .. }
            | Self::PulsesConsumed { color, .. }
            | Self::LoopInstantiated { color, .. }
            | Self::LoopIterationLaunched { color, .. }
            | Self::LoopOutFired { color, .. }
            | Self::LoopStreamEnded { color, .. }
            | Self::LoopTerminated { color, .. }
            | Self::SuspensionRegistered { color, .. }
            | Self::SuspensionResolved { color, .. }
            | Self::RunOutput { color, .. }
            | Self::CostReported { color, .. }
            | Self::LogLine { color, .. }
            | Self::ExecutionTagged { color, .. }
            | Self::ExecutionCompleted { color, .. }
            | Self::ExecutionFailed { color, .. }
            | Self::ExecutionCancelled { color, .. }
            | Self::BusJoined { color, .. }
            | Self::BusLeft { color, .. }
            | Self::BusWindow { color, .. }
            | Self::BusClosed { color, .. }
            | Self::CallerConnected { color, .. }
            | Self::CallerInbound { color, .. }
            | Self::CallerOutbound { color, .. }
            | Self::CallerErrored { color, .. }
            | Self::CallerDisconnected { color, .. } => *color,
        }
    }

    /// The journal's stamp on the row.
    pub fn at_unix(&self) -> u64 {
        match self {
            Self::ExecutionStarted { at_unix, .. }
            | Self::NodeKicked { at_unix, .. }
            | Self::NodeStarted { at_unix, .. }
            | Self::NodeCompleted { at_unix, .. }
            | Self::NodeFailed { at_unix, .. }
            | Self::NodeSkipped { at_unix, .. }
            | Self::NodeSuspended { at_unix, .. }
            | Self::NodeResumed { at_unix, .. }
            | Self::NodeCancelled { at_unix, .. }
            | Self::PortEmitted { at_unix, .. }
            | Self::PortClosed { at_unix, .. }
            | Self::PortTypeMismatch { at_unix, .. }
            | Self::PulsesConsumed { at_unix, .. }
            | Self::LoopInstantiated { at_unix, .. }
            | Self::LoopIterationLaunched { at_unix, .. }
            | Self::LoopOutFired { at_unix, .. }
            | Self::LoopStreamEnded { at_unix, .. }
            | Self::LoopTerminated { at_unix, .. }
            | Self::SuspensionRegistered { at_unix, .. }
            | Self::SuspensionResolved { at_unix, .. }
            | Self::RunOutput { at_unix, .. }
            | Self::CostReported { at_unix, .. }
            | Self::LogLine { at_unix, .. }
            | Self::ExecutionTagged { at_unix, .. }
            | Self::ExecutionCompleted { at_unix, .. }
            | Self::ExecutionFailed { at_unix, .. }
            | Self::ExecutionCancelled { at_unix, .. }
            | Self::BusJoined { at_unix, .. }
            | Self::BusLeft { at_unix, .. }
            | Self::BusWindow { at_unix, .. }
            | Self::BusClosed { at_unix, .. }
            | Self::CallerConnected { at_unix, .. }
            | Self::CallerInbound { at_unix, .. }
            | Self::CallerOutbound { at_unix, .. }
            | Self::CallerErrored { at_unix, .. }
            | Self::CallerDisconnected { at_unix, .. } => *at_unix,
        }
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::ExecutionStarted { .. } => "execution_started",
            Self::NodeKicked { .. } => "node_kicked",
            Self::NodeStarted { .. } => "node_started",
            Self::NodeCompleted { .. } => "node_completed",
            Self::NodeFailed { .. } => "node_failed",
            Self::NodeSkipped { .. } => "node_skipped",
            Self::NodeSuspended { .. } => "node_suspended",
            Self::NodeResumed { .. } => "node_resumed",
            Self::NodeCancelled { .. } => "node_cancelled",
            Self::PortEmitted { .. } => "port_emitted",
            Self::PortClosed { .. } => "port_closed",
            Self::PortTypeMismatch { .. } => "port_type_mismatch",
            Self::PulsesConsumed { .. } => "pulses_consumed",
            Self::LoopInstantiated { .. } => "loop_instantiated",
            Self::LoopIterationLaunched { .. } => "loop_iteration_launched",
            Self::LoopOutFired { .. } => "loop_out_fired",
            Self::LoopStreamEnded { .. } => "loop_stream_ended",
            Self::LoopTerminated { .. } => "loop_terminated",
            Self::SuspensionRegistered { .. } => "suspension_registered",
            Self::SuspensionResolved { .. } => "suspension_resolved",
            Self::RunOutput { .. } => "run_output",
            Self::CostReported { .. } => "cost_reported",
            Self::LogLine { .. } => "log_line",
            Self::ExecutionTagged { .. } => "execution_tagged",
            Self::ExecutionCompleted { .. } => "execution_completed",
            Self::ExecutionFailed { .. } => "execution_failed",
            Self::ExecutionCancelled { .. } => "execution_cancelled",
            Self::BusJoined { .. } => "bus_joined",
            Self::BusLeft { .. } => "bus_left",
            Self::BusWindow { .. } => "bus_window",
            Self::BusClosed { .. } => "bus_closed",
            Self::CallerConnected { .. } => "caller_connected",
            Self::CallerInbound { .. } => "caller_inbound",
            Self::CallerOutbound { .. } => "caller_outbound",
            Self::CallerErrored { .. } => "caller_errored",
            Self::CallerDisconnected { .. } => "caller_disconnected",
        }
    }
}


#[cfg(test)]
mod wire_tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn color() -> Color {
        Uuid::nil()
    }

    fn round_trip(ev: ExecEvent) -> Value {
        let s = serde_json::to_string(&ev).expect("serialize");
        let back: ExecEvent = serde_json::from_str(&s).expect("deserialize");
        let again = serde_json::to_string(&back).expect("re-serialize");
        assert_eq!(s, again, "round trip is stable");
        serde_json::from_str(&s).expect("json")
    }

    /// Every reshaped row round-trips, and its kind tag is what the
    /// SQL readers filter on.
    #[test]
    fn reshaped_rows_round_trip() {
        let frames = vec![weft_core::frames::LoopIteration { index: 2 }];
        let emission = Uuid::new_v4();
        let rows = vec![
            ExecEvent::NodeStarted { color: color(), node_id: "n".into(), frames: frames.clone(), at_unix: 1 },
            ExecEvent::NodeCompleted { color: color(), node_id: "n".into(), frames: frames.clone(), at_unix: 1 },
            ExecEvent::NodeFailed { color: color(), node_id: "n".into(), frames: frames.clone(), error: "e".into(), at_unix: 1 },
            ExecEvent::NodeSkipped {
                color: color(),
                node_id: "n".into(),
                frames: frames.clone(),
                reason: weft_core::exec::skip::SkipReason::ScopeSkipped { scope: "g".into() },
                at_unix: 1,
            },
            ExecEvent::NodeResumed { color: color(), node_id: "n".into(), frames: frames.clone(), token: None, at_unix: 1 },
            ExecEvent::NodeCancelled { color: color(), node_id: "n".into(), frames: frames.clone(), reason: "r".into(), at_unix: 1 },
            ExecEvent::PortEmitted {
                color: color(),
                emission_id: emission,
                node_id: "n".into(),
                frames: frames.clone(),
                port: "out".into(),
                value: std::sync::Arc::new(json!({ "nested": { "deep": [1, 2, { "k": null }] } })),
                at_unix: 1,
            },
            ExecEvent::PortClosed { color: color(), emission_id: emission, node_id: "n".into(), frames: frames.clone(), port: "out".into(), at_unix: 1 },
            ExecEvent::PortTypeMismatch {
                color: color(),
                emission_id: emission,
                node_id: "n".into(),
                frames: frames.clone(),
                port: "out".into(),
                expected: "String".into(),
                actual: "Number".into(),
                at_unix: 1,
            },
            ExecEvent::LoopInstantiated { color: color(), group_id: "lp".into(), parent_frames: frames.clone(), at_unix: 1 },
            ExecEvent::LoopIterationLaunched { color: color(), group_id: "lp".into(), parent_frames: frames.clone(), index: 3, stream_pulse: Some(Uuid::nil().to_string()), at_unix: 1 },
            ExecEvent::LoopOutFired { color: color(), group_id: "lp".into(), parent_frames: frames.clone(), index: 3, at_unix: 1 },
            ExecEvent::LoopTerminated { color: color(), group_id: "lp".into(), parent_frames: frames.clone(), reason: LoopTerminationReason::DoneVoted, at_unix: 1 },
            ExecEvent::LoopStreamEnded { color: color(), group_id: "lp".into(), parent_frames: frames.clone(), end: weft_core::generator::StreamEnd::Failed { error: "e".into() }, at_unix: 1 },
            ExecEvent::PulsesConsumed { color: color(), node_id: "n".into(), frames: frames.clone(), pulse_ids: vec![Uuid::nil().to_string()], at_unix: 1 },
            ExecEvent::ExecutionCompleted { color: color(), at_unix: 1 },
        ];
        for row in rows {
            let kind = row.kind_str();
            let json = round_trip(row);
            assert_eq!(json["kind"], kind);
        }
    }

    /// A `null` stream pulse is written, so a row that lacks the field
    /// fails to decode instead of reading as a list launch.
    #[test]
    fn a_launch_row_always_carries_its_stream_pulse_field() {
        let row = ExecEvent::LoopIterationLaunched { color: color(), group_id: "lp".into(), parent_frames: vec![], index: 0, stream_pulse: None, at_unix: 1 };
        let json = round_trip(row);
        assert!(json.get("stream_pulse").is_some_and(Value::is_null));
        let mut without = json.clone();
        without.as_object_mut().unwrap().remove("stream_pulse");
        assert!(serde_json::from_value::<ExecEvent>(without).is_err());
    }

    /// A resume row always carries its token field (`null` for a
    /// crash re-run), so a row that lacks it is a truncated row and
    /// fails to decode instead of reading as a crash re-run.
    #[test]
    fn a_resume_row_always_carries_its_token_field() {
        let row = ExecEvent::NodeResumed { color: color(), node_id: "n".into(), frames: vec![], token: None, at_unix: 1 };
        let json = round_trip(row);
        assert!(json.get("token").is_some_and(Value::is_null));
        let mut without = json.clone();
        without.as_object_mut().unwrap().remove("token");
        assert!(serde_json::from_value::<ExecEvent>(without).is_err());
        let with = ExecEvent::NodeResumed { color: color(), node_id: "n".into(), frames: vec![], token: Some("t".into()), at_unix: 1 };
        assert_eq!(round_trip(with)["token"], json!("t"));
    }

    /// A row written to the old shape (a value copy on a lifecycle row,
    /// a row kind that no longer exists) does not decode: there is no
    /// compatibility path, the read fails naming the color.
    #[test]
    fn old_shapes_are_refused() {
        let old_started = json!({
            "kind": "node_started", "color": color(), "node_id": "n", "frames": [],
            "input": { "in": 1 }, "closed_ports": [], "pulses_absorbed": [], "at_unix": 1
        });
        assert!(serde_json::from_value::<ExecEvent>(old_started).is_err(), "a copied input is refused");
        let old_pulse = json!({
            "kind": "pulse_emitted", "color": color(), "pulse_id": "x", "source_node": "a", "source_port": "o",
            "target_node": "b", "target_port": "i", "frames": [], "value": 1, "closed": false, "close_error": null, "at_unix": 1
        });
        assert!(serde_json::from_value::<ExecEvent>(old_pulse).is_err(), "the per-wire row is gone");
        let old_scope = json!({ "kind": "scope_launched", "color": color(), "group_id": "g", "frames": [], "roots": [], "at_unix": 1 });
        assert!(serde_json::from_value::<ExecEvent>(old_scope).is_err(), "boundaries are never journaled");
        let err = crate::decode_event(color(), &old_started_text()).unwrap_err();
        assert!(err.contains("weft clean"), "{err}");
    }

    fn old_started_text() -> String {
        json!({ "kind": "node_started", "color": color(), "node_id": "n", "frames": [], "input": {}, "at_unix": 1 }).to_string()
    }

    /// Every row round-trips, and carries the kind tag the SQL readers
    /// filter on. The list is checked against the enum: a variant with
    /// no row here fails the count below.
    #[test]
    fn every_row_round_trips() {
        use weft_core::bus::WirePayload;
        let spec = weft_core::signal::to_spec(weft_core::signal::Form {
            form_type: "human_query".into(),
            schema: weft_core::signal::FormSchema { fields: Vec::new() },
            title: None,
            description: None,
            consumer_kind: None,
        });
        let rows = vec![
            ExecEvent::ExecutionStarted {
                color: color(),
                project_id: "p".into(),
                entry_node: "trigger".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some("h".into()),
                node_test: false,
                subgraph: Some(vec!["out".into(), "src".into()]),
                at_unix: 7,
            },
            ExecEvent::ExecutionStarted {
                color: color(),
                project_id: "p".into(),
                entry_node: "node-test:MyNode::my_test".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: None,
                node_test: true,
                subgraph: None,
                at_unix: 7,
            },
            ExecEvent::NodeKicked { color: color(), node_id: "sock".into(), firing: true, payload: Some(json!({"body": "late"})), port_snapshot: Some(json!({"url": "u"})), at_unix: 0 },
            ExecEvent::NodeStarted { color: color(), node_id: "n".into(), frames: vec![weft_core::frames::LoopIteration { index: 2 }], at_unix: 1 },
            ExecEvent::NodeCompleted { color: color(), node_id: "n".into(), frames: vec![], at_unix: 1 },
            ExecEvent::NodeFailed { color: color(), node_id: "n".into(), frames: vec![], error: "boom".into(), at_unix: 1 },
            ExecEvent::NodeSkipped { color: color(), node_id: "n".into(), frames: vec![], reason: weft_core::exec::skip::SkipReason::RequiredInputClosed { port: "in".into() }, at_unix: 1 },
            ExecEvent::NodeSuspended { color: color(), node_id: "n".into(), frames: vec![], token: "t".into(), at_unix: 1 },
            ExecEvent::NodeResumed { color: color(), node_id: "n".into(), frames: vec![], token: Some("t".into()), at_unix: 1 },
            ExecEvent::NodeResumed { color: color(), node_id: "n".into(), frames: vec![], token: None, at_unix: 1 },
            ExecEvent::NodeCancelled { color: color(), node_id: "n".into(), frames: vec![], reason: "stopped".into(), at_unix: 1 },
            ExecEvent::PortEmitted { color: color(), emission_id: Uuid::nil(), node_id: "n".into(), frames: vec![], port: "out".into(), value: std::sync::Arc::new(json!({"k": [1, null]})), at_unix: 1 },
            ExecEvent::PortClosed { color: color(), emission_id: Uuid::nil(), node_id: "n".into(), frames: vec![], port: "out".into(), at_unix: 1 },
            ExecEvent::PortTypeMismatch { color: color(), emission_id: Uuid::nil(), node_id: "n".into(), frames: vec![], port: "out".into(), expected: "String".into(), actual: "Number".into(), at_unix: 1 },
            ExecEvent::PulsesConsumed { color: color(), node_id: "n".into(), frames: vec![], pulse_ids: vec![Uuid::nil().to_string()], at_unix: 1 },
            ExecEvent::LoopInstantiated { color: color(), group_id: "lp".into(), parent_frames: vec![], at_unix: 1 },
            ExecEvent::LoopIterationLaunched { color: color(), group_id: "lp".into(), parent_frames: vec![], index: 3, stream_pulse: Some(Uuid::nil().to_string()), at_unix: 1 },
            ExecEvent::LoopIterationLaunched { color: color(), group_id: "lp".into(), parent_frames: vec![], index: 3, stream_pulse: None, at_unix: 1 },
            ExecEvent::LoopOutFired { color: color(), group_id: "lp".into(), parent_frames: vec![weft_core::frames::LoopIteration { index: 0 }], index: 3, at_unix: 1 },
            ExecEvent::LoopStreamEnded { color: color(), group_id: "lp".into(), parent_frames: vec![], end: weft_core::primitive::StreamEnd::Failed { error: "upstream".into() }, at_unix: 1 },
            ExecEvent::LoopTerminated { color: color(), group_id: "lp".into(), parent_frames: vec![], reason: LoopTerminationReason::DoneVoted, at_unix: 1 },
            ExecEvent::ExecutionCompleted { color: color(), at_unix: 1 },
            ExecEvent::SuspensionRegistered { color: color(), node_id: "n".into(), frames: vec![], token: "t".into(), spec, call_index: 2, at_unix: 1 },
            ExecEvent::SuspensionResolved { color: color(), token: "t".into(), value: json!("v"), at_unix: 1 },
            ExecEvent::RunOutput { color: color(), node_id: "n".into(), frames: vec![], call_index: 1, name: "decide".into(), value: json!("go-left"), at_unix: 1 },
            ExecEvent::CostReported {
                color: color(),
                node_id: "n".into(),
                frames: vec![],
                cost_id: "c".into(),
                service: "llm".into(),
                model: Some("m".into()),
                amount_usd: Some(0.5),
                billed: true,
                origin: weft_core::CredentialOwner::TheirOwn,
                metadata: json!({}),
                at_unix: 1,
            },
            ExecEvent::LogLine { color: color(), node_id: "n".into(), frames: vec![], level: "info".into(), message: "hi".into(), at_unix_ms: None, seq: None, at_unix: 1 },
            ExecEvent::ExecutionTagged { color: color(), tags: vec!["user_1".into()], at_unix: 3 },
            ExecEvent::ExecutionFailed { color: color(), error: "boom".into(), at_unix: 1 },
            ExecEvent::ExecutionCancelled { color: color(), reason: "stopped".into(), cause: Some(weft_core::exec::CancelCause::User), at_unix: 1 },
            ExecEvent::ExecutionCancelled { color: color(), reason: "stopped".into(), cause: None, at_unix: 1 },
            ExecEvent::BusJoined { color: color(), bus_id: "b".into(), offset: 0, name: "a".into(), at_unix: 1 },
            ExecEvent::BusLeft { color: color(), bus_id: "b".into(), offset: 1, name: "a".into(), at_unix: 1 },
            ExecEvent::BusWindow {
                color: color(),
                bus_id: "b".into(),
                first_offset: 0,
                last_offset: 1,
                messages: vec![],
                totals: Default::default(),
                at_unix: 1,
            },
            ExecEvent::BusClosed { color: color(), bus_id: "b".into(), offset: 2, at_unix: 1 },
            ExecEvent::CallerConnected { color: color(), offset: 0, protocol: "websocket".into(), at_unix: 7 },
            ExecEvent::CallerInbound { color: color(), offset: 1, payload: WirePayload::Json(json!({"q": "hi"})), payload_byte_size: 10, at_unix: 8 },
            ExecEvent::CallerInbound { color: color(), offset: 2, payload: WirePayload::Json(Value::Null), payload_byte_size: 4, at_unix: 9 },
            ExecEvent::CallerOutbound { color: color(), offset: 3, payload: WirePayload::Json(json!("chunk")), payload_byte_size: 5, terminal: true, at_unix: 10 },
            ExecEvent::CallerErrored { color: color(), offset: 4, message: "node blew up".into(), at_unix: 11 },
            ExecEvent::CallerDisconnected { color: color(), offset: 5, reason: "response complete".into(), at_unix: 12 },
        ];
        let mut kinds: Vec<&'static str> = rows.iter().map(|r| r.kind_str()).collect();
        kinds.sort_unstable();
        kinds.dedup();
        assert_eq!(kinds.len(), 36, "a variant has no row above: {kinds:?}");
        for row in rows {
            let kind = row.kind_str();
            let json = round_trip(row);
            assert_eq!(json["kind"], kind);
        }
    }

    /// The fields that are legitimately `None` are still always
    /// written: a cancel's structured cause and a log line's clock and
    /// sequence. A row lacking one is a truncated row, and a row
    /// written before the field existed is an old row; neither decodes.
    #[test]
    fn present_optional_fields_are_required_on_the_row() {
        let cancelled = round_trip(ExecEvent::ExecutionCancelled { color: color(), reason: "r".into(), cause: None, at_unix: 1 });
        assert!(cancelled.get("cause").is_some_and(Value::is_null));
        let mut without = cancelled.clone();
        without.as_object_mut().unwrap().remove("cause");
        assert!(serde_json::from_value::<ExecEvent>(without).is_err(), "an old cancel row without its cause is refused");
        let line = round_trip(ExecEvent::LogLine { color: color(), node_id: "n".into(), frames: vec![], level: "info".into(), message: "m".into(), at_unix_ms: None, seq: None, at_unix: 1 });
        for field in ["node_id", "frames", "at_unix_ms", "seq"] {
            let mut without = line.clone();
            without.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<ExecEvent>(without).is_err(), "a log row without '{field}' is refused");
        }
        let mut old_skip = round_trip(ExecEvent::NodeSkipped {
            color: color(),
            node_id: "n".into(),
            frames: vec![],
            reason: weft_core::exec::skip::SkipReason::DidNotFlow,
            at_unix: 1,
        });
        old_skip.as_object_mut().unwrap().remove("reason");
        assert!(serde_json::from_value::<ExecEvent>(old_skip).is_err(), "a skip without its reason is refused");
    }
}

