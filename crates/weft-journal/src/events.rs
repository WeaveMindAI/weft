//! Event-sourced execution state.
//!
//! The journal records one event per state change reported by the
//! worker (plus a few dispatcher-side events like NodeKicked at
//! fresh-run time). Folding the event log reconstructs a complete
//! `ExecutionSnapshot`: pulses, executions, active suspensions. This
//! replaces periodic snapshots. Replay is the source of truth; an
//! explicit snapshot blob is just a materialized view of the fold.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::frames::LoopFrames;
use weft_core::primitive::{
    ExecutionSnapshot, KickedNode, LoopInstanceKey, LoopInstanceSnapshot,
    LoopTerminationReason, SignalSpec, SuspensionInfo,
};
use weft_core::Color;

/// One event in the execution log. Append-only; events are never
/// edited or deleted by the dispatcher. User-initiated cleanup
/// (`weft clean`) is the only path that removes them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
        /// behavior.
        definition_hash: String,
        at_unix: u64,
    },

    NodeKicked {
        color: Color,
        node_id: String,
        payload: Option<Value>,
        at_unix: u64,
    },

    /// A node was absorbed into a dispatch (ready group picked up,
    /// pulses marked Absorbed, NodeExecution::Running created).
    NodeStarted {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        input: Value,
        closed_ports: Vec<String>,
        pulses_absorbed: Vec<String>,
        at_unix: u64,
    },

    NodeCompleted {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        output: Value,
        /// Emissions this terminal firing is responsible for delivering
        /// ATOMICALLY with the marker (see `NodeFailed.closure_emissions`).
        /// Usually just the unmentioned-port CLOSURES; for a synchronous
        /// Passthrough it ALSO carries the forwarded VALUES (push_pulse
        /// materializes each by its `closed` flag, so values and closures
        /// both fold correctly). The field name is kept for cross-event
        /// symmetry; "closures" is the common case, not the only one.
        closure_emissions: Vec<LaunchedEmission>,
        at_unix: u64,
    },

    NodeFailed {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        error: String,
        /// Closure pulses on this firing's unmentioned output ports,
        /// carried in THIS row so the terminal marker and the closures it
        /// implies are one atomic journal write. A crash between two
        /// separate writes would lose the closures: downstream consumers
        /// then neither fire nor skip and the execution refolds Stuck.
        /// Same discipline as `LoopTerminated.outward_emissions`.
        closure_emissions: Vec<LaunchedEmission>,
        at_unix: u64,
    },

    NodeSkipped {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        closed_ports: Vec<String>,
        /// See `NodeFailed.closure_emissions`: the skip's unmentioned-port
        /// closures ride here atomically with the terminal marker.
        closure_emissions: Vec<LaunchedEmission>,
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
        /// - `Some((token, value))`: the firing was Suspended and a
        ///   `SuspensionResolved` arrived. The fold clears the
        ///   `suspensions` and `pending_deliveries` entries for
        ///   `token`.
        /// - `None`: crashed-Running recovery (the firing was
        ///   Running when the worker crashed; a fresh worker is
        ///   re-driving it). No suspension token to clear; the audit
        ///   event exists so `pulses_absorbed` lands in the journal.
        token: Option<String>,
        value: Option<Value>,
        /// Pulse IDs (hex-encoded UUIDs, same format as NodeStarted)
        /// this resume dispatch absorbed. A resume can absorb fresh
        /// pulses that arrived while the firing was Waiting OR a
        /// crashed-Running recovery can absorb pulses the dispatcher
        /// queued while the worker was down; the un-absorb path on
        /// a later crashed-Running recovery needs every absorbed-
        /// pulse ID, not just the ones from the original NodeStarted,
        /// or the resume-time pulses stay stuck in Absorbed forever.
        pulses_absorbed: Vec<String>,
        at_unix: u64,
    },

    NodeCancelled {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        reason: String,
        /// See `NodeFailed.closure_emissions`: a cancelled firing's
        /// outward closures ride here atomically with the terminal marker.
        closure_emissions: Vec<LaunchedEmission>,
        at_unix: u64,
    },

    /// A downstream pulse the engine produced during postprocess.
    PulseEmitted {
        color: Color,
        pulse_id: String,
        source_node: String,
        source_port: String,
        target_node: String,
        target_port: String,
        frames: LoopFrames,
        value: Value,
        closed: bool,
        at_unix: u64,
    },

    /// A `Loop` instance was created at `parent_frames` when `LoopIn`
    /// first fired for the loop. Carries the resolved iteration count
    /// (after zip-trim and `max_iters` cap) and the config snapshot.
    LoopInstantiated {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        iter_count: u32,
        parallel: bool,
        max_iters: Option<u32>,
        /// Iter-input port names. Persisted because the rehydrate
        /// path needs to slice `outer_input` by port to launch later
        /// iterations. NOT optional: a journal row whose writer
        /// omitted this field is corrupt, not legacy.
        over: Vec<String>,
        /// Carry-port names. Persisted so the rehydrate path knows
        /// which `LoopOut` inputs are carry-writes vs gather-writes
        /// without re-reading the project definition.
        carry: Vec<String>,
        /// Zip mode at instantiation time.
        trim_on_mismatch: bool,
        /// Outer input bag captured from the LoopIn firing. Persists
        /// so a resumed worker can launch later iterations without
        /// reading from the (already-absorbed) LoopIn pulse bucket.
        outer_input: HashMap<String, Value>,
        /// Initial carry values seeded at instantiation time (from the
        /// outer-input bag if the user wired a value to the carry
        /// port, otherwise from `weft_type_zero`). Persists so a
        /// resume between instantiation and the first LoopOutFired
        /// rebuilds `carry_values` with the seed instead of an empty
        /// map (which would emit nothing on the body's carry input
        /// for the very first iteration).
        initial_carry: HashMap<String, Value>,
        at_unix: u64,
    },

    /// The engine launched body work for iteration `index` of the loop
    /// at `parent_frames`. For parallel loops, all N are launched
    /// upfront; for sequential, one per fire of `LoopOut`.
    ///
    /// Carries the iteration's body pulses inline (instead of
    /// separate `PulseEmitted` rows) so the launch marker and the
    /// pulses land in ONE journal row. Two rows would open a crash
    /// window with no correct ordering: pulses-then-marker replays
    /// the body twice on resume (marker missing -> `launched` lacks
    /// the index -> LaunchNext re-ships the pulses), and
    /// marker-then-pulses hangs (index in `launched` but no body
    /// pulses to run). NOT optional: a row missing this field is
    /// corrupt, not legacy.
    LoopIterationLaunched {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        body_emissions: Vec<LaunchedEmission>,
        at_unix: u64,
    },

    /// `LoopOut` fired for iteration `index`. `gather_writes` holds the
    /// values the body wrote to each gather port (`Closed` on closure
    /// → null at index). `carry_writes` holds the carry-port updates
    /// (`Closed` → keep previous). `done_vote` is the body's
    /// `self.done` value (None on closure → treated as false).
    /// `Option<Value>` is NOT usable here because default serde
    /// collapses `Some(Value::Null)` and `None` to the same JSON, so
    /// a body that legitimately writes JSON null on a gather/carry
    /// port becomes indistinguishable from "closed the port" after a
    /// journal round-trip.
    LoopOutFired {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        gather_writes: HashMap<String, weft_core::primitive::LoopWrite>,
        carry_writes: HashMap<String, weft_core::primitive::LoopWrite>,
        done_vote: Option<bool>,
        at_unix: u64,
    },

    /// The loop emitted outwardly: all launched iterations fired their
    /// `LoopOut` AND a termination condition was satisfied.
    LoopTerminated {
        color: Color,
        group_id: String,
        parent_frames: LoopFrames,
        reason: LoopTerminationReason,
        /// The loop's outward pulses (assembled gather lists + final
        /// carries on success, or port closures on a failed/abnormal
        /// end), carried in THIS row so the marker and the pulses are
        /// one atomic journal write. Without this, a crash between
        /// shipping the outward pulses and journaling LoopTerminated
        /// leaves the pulses pending with `terminated: None` on refold:
        /// the LoopOut re-fires and emits the loop's outputs a SECOND
        /// time downstream. Same fix as `LoopIterationLaunched`'s
        /// `body_emissions`.
        outward_emissions: Vec<LaunchedEmission>,
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
    /// the figure moved credits (a deployment-billed call) as opposed to a
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
        /// Whose key the call spent: the user's own, or one the deployment
        /// holds. Part of the money trail (a figure without "whose key" is
        /// half an answer).
        origin: weft_core::AccessOrigin,
        metadata: Value,
        at_unix: u64,
    },

    LogLine {
        color: Color,
        level: String,
        message: String,
        at_unix: u64,
    },

    /// A node tried to emit a value on `port` whose inferred type is not
    /// compatible with the port's declared type. The engine refused the
    /// value and closed the port instead (downstream sees null). This is
    /// NON-terminal: the node keeps running and its other ports emit
    /// normally; this row is the visible record that one port's value was
    /// dropped. Folds into the matching `NodeExecution.port_warnings`.
    PortTypeMismatch {
        color: Color,
        node_id: String,
        frames: LoopFrames,
        port: String,
        expected: String,
        actual: String,
        at_unix: u64,
    },

    ExecutionCompleted {
        color: Color,
        outputs: Value,
        at_unix: u64,
    },

    ExecutionFailed {
        color: Color,
        error: String,
        at_unix: u64,
    },

    ExecutionCancelled {
        color: Color,
        reason: String,
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

    BusMessage {
        color: Color,
        bus_id: String,
        offset: u64,
        from: String,
        msg_kind: String,
        /// Tagged `Journaled { value }` vs `Ephemeral`. The earlier
        /// shape used `Option<Value>` which silently conflated
        /// `Some(Value::Null)` (a journaled bus where the body
        /// legitimately sent `null`) with `None` (ephemeral; payload
        /// not journaled). See `weft_core::primitive::JournaledPayload`.
        payload: weft_core::primitive::JournaledPayload,
        payload_byte_size: u64,
        #[serde(with = "weft_core::hex_array8")]
        payload_sha256_prefix: [u8; 8],
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
    // per-execution position in the caller stream. Message payloads reuse
    // `JournaledPayload` (journaled = full value, ephemeral = metadata-only +
    // sliding window) so a high-volume stream does not bloat the journal,
    // identical to the bus's journaled-vs-ephemeral tradeoff.

    /// The caller attached. The first event in any caller stream.
    CallerConnected {
        color: Color,
        offset: u64,
        /// `"http"` | `"websocket"` (the `Protocol` wire tag).
        protocol: String,
        at_unix: u64,
    },

    /// A message arrived FROM the caller (HTTP request body, WS inbound).
    CallerInbound {
        color: Color,
        offset: u64,
        payload: weft_core::primitive::JournaledPayload,
        payload_byte_size: u64,
        #[serde(with = "weft_core::hex_array8")]
        payload_sha256_prefix: [u8; 8],
        at_unix: u64,
    },

    /// A message went TO the caller (HTTP write/respond chunk, WS send).
    /// `terminal` marks the final outbound (HTTP respond/close, WS close)
    /// so the inspector renders "* the response completed here".
    CallerOutbound {
        color: Color,
        offset: u64,
        payload: weft_core::primitive::JournaledPayload,
        payload_byte_size: u64,
        #[serde(with = "weft_core::hex_array8")]
        payload_sha256_prefix: [u8; 8],
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

/// One body pulse carried inline in `LoopIterationLaunched`. Same
/// fields as a `PulseEmitted` row minus `color`/`at_unix` (the
/// carrying event's apply). The fold replays each entry through the
/// same `push_pulse` helper as the `PulseEmitted` arm, so the
/// reconstructed pulse table is byte-identical to what individual
/// `PulseEmitted` rows would have produced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchedEmission {
    pub pulse_id: String,
    pub source_node: String,
    pub source_port: String,
    pub target_node: String,
    pub target_port: String,
    pub frames: LoopFrames,
    pub value: Value,
    pub closed: bool,
}

impl From<weft_core::exec::PulseEmission> for LaunchedEmission {
    fn from(e: weft_core::exec::PulseEmission) -> Self {
        let p = e.pulse;
        Self {
            pulse_id: p.id.to_string(),
            source_node: e.source_node,
            source_port: e.source_port,
            target_node: p.target_node,
            target_port: p.target_port,
            frames: p.frames,
            value: p.value,
            closed: p.closed,
        }
    }
}

impl ExecEvent {
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
            | Self::PulseEmitted { color, .. }
            | Self::LoopInstantiated { color, .. }
            | Self::LoopIterationLaunched { color, .. }
            | Self::LoopOutFired { color, .. }
            | Self::LoopTerminated { color, .. }
            | Self::SuspensionRegistered { color, .. }
            | Self::SuspensionResolved { color, .. }
            | Self::RunOutput { color, .. }
            | Self::CostReported { color, .. }
            | Self::LogLine { color, .. }
            | Self::PortTypeMismatch { color, .. }
            | Self::ExecutionCompleted { color, .. }
            | Self::ExecutionFailed { color, .. }
            | Self::ExecutionCancelled { color, .. }
            | Self::BusJoined { color, .. }
            | Self::BusLeft { color, .. }
            | Self::BusMessage { color, .. }
            | Self::BusClosed { color, .. }
            | Self::CallerConnected { color, .. }
            | Self::CallerInbound { color, .. }
            | Self::CallerOutbound { color, .. }
            | Self::CallerErrored { color, .. }
            | Self::CallerDisconnected { color, .. } => *color,
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
            Self::PulseEmitted { .. } => "pulse_emitted",
            Self::LoopInstantiated { .. } => "loop_instantiated",
            Self::LoopIterationLaunched { .. } => "loop_iteration_launched",
            Self::LoopOutFired { .. } => "loop_out_fired",
            Self::LoopTerminated { .. } => "loop_terminated",
            Self::SuspensionRegistered { .. } => "suspension_registered",
            Self::SuspensionResolved { .. } => "suspension_resolved",
            Self::RunOutput { .. } => "run_output",
            Self::CostReported { .. } => "cost_reported",
            Self::LogLine { .. } => "log_line",
            Self::PortTypeMismatch { .. } => "port_type_mismatch",
            Self::ExecutionCompleted { .. } => "execution_completed",
            Self::ExecutionFailed { .. } => "execution_failed",
            Self::ExecutionCancelled { .. } => "execution_cancelled",
            Self::BusJoined { .. } => "bus_joined",
            Self::BusLeft { .. } => "bus_left",
            Self::BusMessage { .. } => "bus_message",
            Self::BusClosed { .. } => "bus_closed",
            Self::CallerConnected { .. } => "caller_connected",
            Self::CallerInbound { .. } => "caller_inbound",
            Self::CallerOutbound { .. } => "caller_outbound",
            Self::CallerErrored { .. } => "caller_errored",
            Self::CallerDisconnected { .. } => "caller_disconnected",
        }
    }
}

// ----- Fold: events -> ExecutionSnapshot -----------------------------

pub fn fold_to_snapshot(color: Color, events: &[ExecEvent]) -> ExecutionSnapshot {
    use weft_core::exec::{NodeExecution, NodeExecutionStatus, NodeExecutionTable};
    use weft_core::primitive::{CorruptionSite, JournalCorruption};
    use weft_core::pulse::{Pulse, PulseStatus, PulseTable};

    let mut pulses: PulseTable = Default::default();
    let mut executions: NodeExecutionTable = Default::default();
    let mut suspensions: HashMap<String, SuspensionInfo> = HashMap::new();
    let mut pending_deliveries: HashMap<String, Value> = HashMap::new();
    let mut kicked: HashMap<String, KickedNode> = HashMap::new();
    let mut loop_instances: HashMap<LoopInstanceKey, LoopInstanceSnapshot> = HashMap::new();
    let mut awaited_sequences: HashMap<
        (String, weft_core::frames::LoopFrames),
        Vec<weft_core::primitive::AwaitedEntry>,
    > = HashMap::new();
    let mut corruptions: Vec<JournalCorruption> = Vec::new();

    fn report_corruption(
        corruptions: &mut Vec<JournalCorruption>,
        color: Color,
        site: CorruptionSite,
        reason: String,
    ) {
        tracing::error!(
            target: "weft_journal::fold",
            %color, ?site, reason = %reason,
            "skip row during fold (journal corruption)"
        );
        corruptions.push(JournalCorruption { site, reason });
    }

    #[allow(clippy::too_many_arguments)]
    fn push_pulse(
        pulses: &mut PulseTable,
        corruptions: &mut Vec<JournalCorruption>,
        site: CorruptionSite,
        pulse_id: &str,
        color: Color,
        frames: LoopFrames,
        target_node: &str,
        target_port: &str,
        value: Value,
        closed: bool,
    ) {
        let id = match pulse_id.parse::<uuid::Uuid>() {
            Ok(id) => id,
            Err(e) => {
                report_corruption(
                    corruptions,
                    color,
                    site,
                    format!("pulse_id={pulse_id:?} unparseable: {e}"),
                );
                return;
            }
        };
        let pulse = match Pulse::from_journal_emit(
            id,
            color,
            frames,
            target_node.to_string(),
            target_port.to_string(),
            value,
            closed,
        ) {
            Ok(p) => p,
            Err(reason) => {
                report_corruption(
                    corruptions,
                    color,
                    site,
                    format!("pulse_id={pulse_id} invariant violated: {reason}"),
                );
                return;
            }
        };
        // Idempotent replay guard, same stance as the `launched` /
        // `out_fired` / `kicked` folds: pulse ids are minted once per
        // emission, so a second row with the same id is a replayed
        // duplicate, never a second pulse. First row wins.
        let bucket = pulses.entry(target_node.to_string()).or_default();
        if bucket.iter().any(|existing| existing.id == id) {
            return;
        }
        bucket.push(pulse);
    }

    /// Replay the closure pulses a terminal event (NodeCompleted /
    /// NodeFailed / NodeSkipped / NodeCancelled) carries, via the same
    /// `push_pulse` helper as PulseEmitted, so the terminal marker and
    /// its closures fold as one atomic unit.
    fn replay_terminal_closures(
        pulses: &mut PulseTable,
        corruptions: &mut Vec<JournalCorruption>,
        site: CorruptionSite,
        color: Color,
        emissions: &[LaunchedEmission],
    ) {
        for e in emissions {
            push_pulse(
                pulses,
                corruptions,
                site,
                &e.pulse_id,
                color,
                e.frames.clone(),
                &e.target_node,
                &e.target_port,
                e.value.clone(),
                e.closed,
            );
        }
    }

    fn parse_absorbed_ids(
        ids: &[String],
        corruptions: &mut Vec<JournalCorruption>,
        site: CorruptionSite,
        color: Color,
    ) -> Vec<uuid::Uuid> {
        ids.iter()
            .filter_map(|s| match s.parse::<uuid::Uuid>() {
                Ok(id) => Some(id),
                Err(e) => {
                    report_corruption(
                        corruptions,
                        color,
                        site,
                        format!("absorbed_pulse_id={s:?} unparseable: {e}"),
                    );
                    None
                }
            })
            .collect()
    }

    for ev in events {
        match ev {
            ExecEvent::ExecutionStarted { .. } => {}
            ExecEvent::NodeKicked { node_id, payload, .. } => {
                // First kick wins; further kicks on the same node id are a
                // true no-op (the documented contract). The first kick's
                // payload and `dispatched` flag are authoritative; a later
                // kick carrying a different payload is a writer-level bug
                // the fold must not paper over by silently merging.
                kicked.entry(node_id.clone()).or_insert_with(|| KickedNode {
                    payload: payload.clone(),
                    dispatched: false,
                });
            }
            ExecEvent::PulseEmitted {
                color: c,
                pulse_id,
                target_node,
                target_port,
                frames,
                value,
                closed,
                ..
            } => {
                push_pulse(
                    &mut pulses,
                    &mut corruptions,
                    CorruptionSite::PulseEmitted,
                    pulse_id,
                    *c,
                    frames.clone(),
                    target_node,
                    target_port,
                    value.clone(),
                    *closed,
                );
            }
            ExecEvent::NodeStarted { node_id, frames, input, pulses_absorbed, at_unix, color: c, closed_ports: _ } => {
                let absorbed_uuids = parse_absorbed_ids(pulses_absorbed, &mut corruptions, CorruptionSite::NodeStarted, *c);
                if !absorbed_uuids.is_empty() {
                    if let Some(bucket) = pulses.get_mut(node_id) {
                        for p in bucket.iter_mut() {
                            if absorbed_uuids.contains(&p.id) && p.status == PulseStatus::Pending {
                                p.status = PulseStatus::Absorbed;
                            }
                        }
                    }
                }
                if frames.is_empty() {
                    if let Some(k) = kicked.get_mut(node_id) {
                        k.dispatched = true;
                    }
                }
                // Open a new record unless a NON-TERMINAL record already
                // sits at this (color, frames). Live dispatch opens a new
                // record per firing whenever no non-terminal one exists
                // (it ships NodeResumed, never a second NodeStarted, to
                // continue a non-terminal record). So a NodeStarted whose
                // latest same-key record is TERMINAL is a legitimate second
                // firing (e.g. a streaming/bus node fired its consumer
                // twice); collapsing it onto the terminal record would
                // silently drop the second firing's work on refold. A
                // NodeStarted whose latest same-key record is non-terminal
                // can only be a corruption-dedup replay; skip it.
                let has_non_terminal = executions
                    .get(node_id)
                    .map(|v| {
                        v.iter()
                            .rev()
                            .find(|e| e.color == *c && &e.frames == frames)
                            .map(|e| !e.status.is_terminal())
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if !has_non_terminal {
                    let record = NodeExecution {
                        id: uuid::Uuid::new_v4(),
                        node_id: node_id.clone(),
                        status: NodeExecutionStatus::Running,
                        pulses_absorbed: absorbed_uuids,
                        dispatch_pulse: uuid::Uuid::new_v4(),
                        error: None,
                        callback_id: None,
                        started_at: *at_unix,
                        completed_at: None,
                        input: Some(input.clone()),
                        output: None,
                        cost_usd: 0.0,
                        logs: Vec::new(),
                        port_warnings: Vec::new(),
                        color: *c,
                        frames: frames.clone(),
                    };
                    executions.entry(node_id.clone()).or_default().push(record);
                }
            }
            ExecEvent::NodeSuspended { node_id, frames, token, color: c, .. } => {
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.frames == frames)
                    {
                        e.status = NodeExecutionStatus::WaitingForInput;
                        e.callback_id = Some(token.clone());
                    }
                }
            }
            ExecEvent::NodeResumed { node_id, frames, token, pulses_absorbed, color: c, .. } => {
                let absorbed_uuids = parse_absorbed_ids(
                    pulses_absorbed,
                    &mut corruptions,
                    CorruptionSite::NodeResumed,
                    *c,
                );
                // Flip these pulses to Absorbed in the pulse table, exactly
                // as the NodeStarted arm does. The live resume dispatch
                // absorbs them in RAM and journals them here; if the fold
                // leaves them Pending, a refold on a fresh worker has the
                // node's record terminal (Completed) yet a Pending pulse at
                // its (node, frames) location, and `find_ready_nodes` (which
                // keys purely on Pending pulses, ignoring the record) re-fires
                // the node: double execution on every respawn. The un-absorb
                // path (`redispatch_locations`) also assumes resume-time
                // pulses were folded Absorbed before it flips them back.
                if !absorbed_uuids.is_empty() {
                    if let Some(bucket) = pulses.get_mut(node_id) {
                        for p in bucket.iter_mut() {
                            if absorbed_uuids.contains(&p.id) && p.status == PulseStatus::Pending {
                                p.status = PulseStatus::Absorbed;
                            }
                        }
                    }
                }
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.frames == frames)
                    {
                        e.status = NodeExecutionStatus::Running;
                        e.callback_id = None;
                        // Extend with newly-absorbed pulse IDs so the
                        // crashed-Running un-absorb path (lookup by
                        // ID set in apply_snapshot) restores every
                        // pulse this resume dispatch consumed, not
                        // just the originals from NodeStarted.
                        for id in absorbed_uuids {
                            if !e.pulses_absorbed.contains(&id) {
                                e.pulses_absorbed.push(id);
                            }
                        }
                    }
                }
                // Only clear suspension state when this resume was
                // suspension-driven (token present). Crashed-Running
                // recovery has no token to remove.
                if let Some(t) = token {
                    suspensions.remove(t);
                    pending_deliveries.remove(t);
                }
            }
            ExecEvent::NodeCancelled { node_id, frames, reason, closure_emissions, at_unix, color: c } => {
                replay_terminal_closures(&mut pulses, &mut corruptions, CorruptionSite::NodeCancelled, *c, closure_emissions);
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.frames == frames)
                    {
                        e.status = NodeExecutionStatus::Cancelled;
                        e.completed_at = Some(*at_unix);
                        e.error = Some(reason.clone());
                        e.callback_id = None;
                    }
                }
            }
            ExecEvent::NodeCompleted { node_id, frames, output, closure_emissions, at_unix, color: c } => {
                replay_terminal_closures(&mut pulses, &mut corruptions, CorruptionSite::NodeCompleted, *c, closure_emissions);
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.frames == frames)
                    {
                        e.status = NodeExecutionStatus::Completed;
                        e.completed_at = Some(*at_unix);
                        e.output = Some(output.clone());
                        e.callback_id = None;
                    }
                }
            }
            ExecEvent::NodeFailed { node_id, frames, error, closure_emissions, at_unix, color: c } => {
                replay_terminal_closures(&mut pulses, &mut corruptions, CorruptionSite::NodeFailed, *c, closure_emissions);
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.frames == frames)
                    {
                        e.status = NodeExecutionStatus::Failed;
                        e.completed_at = Some(*at_unix);
                        e.error = Some(error.clone());
                    }
                }
            }
            ExecEvent::NodeSkipped { node_id, frames, at_unix, color: c, closed_ports: _, closure_emissions } => {
                replay_terminal_closures(&mut pulses, &mut corruptions, CorruptionSite::NodeSkipped, *c, closure_emissions);
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.frames == frames)
                    {
                        e.status = NodeExecutionStatus::Skipped;
                        e.completed_at = Some(*at_unix);
                    }
                }
            }
            ExecEvent::LoopInstantiated {
                color: c, group_id, parent_frames, iter_count, parallel, max_iters,
                over, carry, trim_on_mismatch, outer_input, initial_carry, ..
            } => {
                let key = LoopInstanceKey {
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    color: *c,
                };
                loop_instances
                    .entry(key)
                    .or_insert_with(|| LoopInstanceSnapshot {
                        iter_count: *iter_count,
                        parallel: *parallel,
                        max_iters: *max_iters,
                        over: over.clone(),
                        carry: carry.clone(),
                        trim_on_mismatch: *trim_on_mismatch,
                        launched: Vec::new(),
                        out_fired: Vec::new(),
                        gather_lists: HashMap::new(),
                        // Seed the carry slot at instantiation time so a
                        // resume between LoopInstantiated and the first
                        // LoopOutFired keeps the initial carry visible
                        // to the body's first iteration.
                        carry_values: initial_carry.clone(),
                        outer_input: outer_input.clone(),
                        terminated: None,
                    });
            }
            ExecEvent::LoopIterationLaunched {
                color: c, group_id, parent_frames, index, body_emissions, ..
            } => {
                // Replay the carried body pulses exactly as the
                // `PulseEmitted` arm does (same `push_pulse` helper):
                // marker and pulses are one atomic row, so a fold
                // either sees both (index in `launched` AND the body
                // pulses pending) or neither.
                for e in body_emissions {
                    push_pulse(
                        &mut pulses,
                        &mut corruptions,
                        CorruptionSite::LoopIterationLaunched,
                        &e.pulse_id,
                        *c,
                        e.frames.clone(),
                        &e.target_node,
                        &e.target_port,
                        e.value.clone(),
                        e.closed,
                    );
                }
                let key = LoopInstanceKey {
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    color: *c,
                };
                match loop_instances.get_mut(&key) {
                    Some(inst) => {
                        if !inst.launched.contains(index) {
                            inst.launched.push(*index);
                        }
                    }
                    None => report_corruption(
                        &mut corruptions,
                        *c,
                        CorruptionSite::LoopIterationLaunched,
                        format!(
                            "LoopIterationLaunched at group_id={group_id} parent_frames={parent_frames:?} index={index} with no prior LoopInstantiated"
                        ),
                    ),
                }
            }
            ExecEvent::LoopOutFired {
                color: c, group_id, parent_frames, index, gather_writes, carry_writes, ..
            } => {
                let key = LoopInstanceKey {
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    color: *c,
                };
                match loop_instances.get_mut(&key) {
                    Some(inst) => {
                        if !inst.out_fired.contains(index) {
                            inst.out_fired.push(*index);
                        }
                        for (port, slot) in gather_writes {
                            inst.gather_lists
                                .entry(port.clone())
                                .or_default()
                                .insert(*index, slot.clone());
                        }
                        // Carry: only Value updates; Closed means
                        // "keep previous" per the LoopWrite contract.
                        for (port, slot) in carry_writes {
                            if let weft_core::primitive::LoopWrite::Value(v) = slot {
                                inst.carry_values.insert(port.clone(), v.clone());
                            }
                        }
                    }
                    None => report_corruption(
                        &mut corruptions,
                        *c,
                        CorruptionSite::LoopOutFired,
                        format!(
                            "LoopOutFired at group_id={group_id} parent_frames={parent_frames:?} index={index} with no prior LoopInstantiated"
                        ),
                    ),
                }
            }
            ExecEvent::LoopTerminated {
                color: c, group_id, parent_frames, reason, outward_emissions, ..
            } => {
                // Replay the carried outward pulses (same atomic
                // marker+pulses discipline as LoopIterationLaunched):
                // a fold sees both `terminated` set AND the outward
                // pulses, or neither, so a crash-resume never re-emits
                // the loop's outputs.
                for e in outward_emissions {
                    push_pulse(
                        &mut pulses,
                        &mut corruptions,
                        CorruptionSite::LoopTerminated,
                        &e.pulse_id,
                        *c,
                        e.frames.clone(),
                        &e.target_node,
                        &e.target_port,
                        e.value.clone(),
                        e.closed,
                    );
                }
                let key = LoopInstanceKey {
                    group_id: group_id.clone(),
                    parent_frames: parent_frames.clone(),
                    color: *c,
                };
                match loop_instances.get_mut(&key) {
                    Some(inst) => inst.terminated = Some(*reason),
                    None => report_corruption(
                        &mut corruptions,
                        *c,
                        CorruptionSite::LoopTerminated,
                        format!(
                            "LoopTerminated at group_id={group_id} parent_frames={parent_frames:?} reason={reason:?} with no prior LoopInstantiated"
                        ),
                    ),
                }
            }
            ExecEvent::SuspensionRegistered {
                node_id, frames, token, spec, call_index, at_unix, ..
            } => {
                suspensions.insert(
                    token.clone(),
                    SuspensionInfo {
                        node_id: node_id.clone(),
                        frames: frames.clone(),
                        spec: spec.clone(),
                        created_at_unix: *at_unix,
                        call_index: *call_index,
                    },
                );
                let key = (node_id.clone(), frames.clone());
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
                let resolved = pending_deliveries.get(token).cloned();
                awaited_sequences
                    .entry(key)
                    .or_default()
                    .push(weft_core::primitive::AwaitedEntry {
                        call_index: *call_index,
                        kind: weft_core::primitive::AwaitedEntryKind::Await {
                            token: token.clone(),
                            resolved,
                        },
                    });
            }
            ExecEvent::RunOutput {
                node_id, frames, call_index, name, value, ..
            } => {
                let key = (node_id.clone(), frames.clone());
                awaited_sequences
                    .entry(key)
                    .or_default()
                    .push(weft_core::primitive::AwaitedEntry {
                        call_index: *call_index,
                        kind: weft_core::primitive::AwaitedEntryKind::Run {
                            name: name.clone(),
                            value: value.clone(),
                        },
                    });
            }
            ExecEvent::SuspensionResolved { token, value, .. } => {
                pending_deliveries.insert(token.clone(), value.clone());
                for entries in awaited_sequences.values_mut() {
                    for entry in entries.iter_mut() {
                        if let weft_core::primitive::AwaitedEntryKind::Await {
                            token: t, resolved,
                        } = &mut entry.kind
                        {
                            if t == token {
                                *resolved = Some(value.clone());
                            }
                        }
                    }
                }
            }
            ExecEvent::PortTypeMismatch { node_id, frames, port, expected, actual, color: c, .. } => {
                if let Some(execs) = executions.get_mut(node_id) {
                    if let Some(e) = execs
                        .iter_mut()
                        .rev()
                        .find(|e| e.color == *c && &e.frames == frames)
                    {
                        e.port_warnings.push(weft_core::exec::PortWarning {
                            port: port.clone(),
                            expected: expected.clone(),
                            actual: actual.clone(),
                        });
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
            ExecEvent::CostReported { color: c, node_id, frames, amount_usd, .. } => {
                if let Some(amount) = amount_usd {
                    if let Some(execs) = executions.get_mut(node_id) {
                        if let Some(e) = execs
                            .iter_mut()
                            .rev()
                            .find(|e| e.color == *c && &e.frames == frames)
                        {
                            e.cost_usd += amount;
                        }
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
            | ExecEvent::ExecutionCompleted { .. }
            | ExecEvent::ExecutionFailed { .. }
            | ExecEvent::ExecutionCancelled { .. }
            | ExecEvent::BusJoined { .. }
            | ExecEvent::BusLeft { .. }
            | ExecEvent::BusMessage { .. }
            | ExecEvent::BusClosed { .. }
            | ExecEvent::CallerConnected { .. }
            | ExecEvent::CallerInbound { .. }
            | ExecEvent::CallerOutbound { .. }
            | ExecEvent::CallerErrored { .. }
            | ExecEvent::CallerDisconnected { .. } => {}
        }
    }

    for entries in awaited_sequences.values_mut() {
        entries.sort_by_key(|e| e.call_index);
    }
    ExecutionSnapshot {
        color,
        pulses,
        executions,
        suspensions,
        kicked,
        pending_deliveries,
        awaited_sequences,
        loop_instances,
        corruptions,
    }
}

#[cfg(test)]
mod fold_pulse_tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;
    use weft_core::frames::{LoopFrames, LoopIteration};
    use weft_core::pulse::PulseStatus;

    fn color() -> Color {
        Uuid::nil()
    }

    fn pulse_id() -> String {
        Uuid::new_v4().to_string()
    }

    fn frame(index: u32) -> LoopIteration {
        LoopIteration { index }
    }

    fn frames(fs: &[LoopIteration]) -> LoopFrames {
        fs.to_vec()
    }

    #[test]
    fn kicked_payload_survives_dispatch_for_resume() {
        let payload = json!({"body": "hello"});
        let events = vec![
            ExecEvent::ExecutionStarted {
                color: color(),
                project_id: "p".into(),
                entry_node: "trigger".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: "test-hash".into(),
                at_unix: 0,
            },
            ExecEvent::NodeKicked {
                color: color(),
                node_id: "trigger".into(),
                payload: Some(payload.clone()),
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "trigger".into(),
                frames: Vec::new(),
                input: json!({}),
                pulses_absorbed: vec![],
                closed_ports: vec![],
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let kick = snap.kicked.get("trigger").expect("trigger kick survives fold");
        assert!(kick.dispatched, "NodeStarted at root frames consumed the kick");
        assert_eq!(
            kick.payload.as_ref(),
            Some(&payload),
            "wake payload preserved so resume can replay it into ctx.wake_payload()"
        );
    }

    #[test]
    fn single_emit_then_absorb() {
        let pid = pulse_id();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "a".into(),
                source_port: "out".into(),
                target_node: "b".into(),
                target_port: "in".into(),
                frames: frames(&[]),
                value: json!(1),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "b".into(),
                frames: frames(&[]),
                input: json!({"in": 1}),
                pulses_absorbed: vec![pid.clone()],
                closed_ports: vec![],
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let bucket = snap.pulses.get("b").expect("bucket b");
        assert_eq!(bucket.len(), 1);
        assert_eq!(bucket[0].id.to_string(), pid);
        assert_eq!(bucket[0].status, PulseStatus::Absorbed);
    }

    #[test]
    fn lifecycle_one_record_per_frames() {
        use weft_core::exec::NodeExecutionStatus;

        let pid = pulse_id();
        let token = "tok-1".to_string();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "review".into(),
                target_port: "in".into(),
                frames: frames(&[]),
                value: json!(42),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                input: json!({"in": 42}),
                pulses_absorbed: vec![pid],
                closed_ports: vec![],
                at_unix: 0,
            },
            ExecEvent::NodeSuspended {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                token: token.clone(),
                at_unix: 0,
            },
            ExecEvent::NodeResumed {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                token: Some(token.clone()),
                value: Some(json!("approved")),
                pulses_absorbed: vec![],
                at_unix: 0,
            },
            ExecEvent::NodeCompleted {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                output: json!({"decision_approved": true}),
                closure_emissions: vec![],
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let execs = snap.executions.get("review").expect("review execs");
        assert_eq!(execs.len(), 1, "one record per (node, frames)");
        assert_eq!(execs[0].status, NodeExecutionStatus::Completed);
        assert!(execs[0].output.is_some());
        assert!(snap.suspensions.is_empty(), "suspensions cleared after resume+complete");
        assert!(snap.pending_deliveries.is_empty());
    }

    #[test]
    fn partial_resume_across_loop_iterations() {
        use weft_core::exec::NodeExecutionStatus;
        use weft_core::signal::{to_spec, Form, FormSchema};

        let mut events = Vec::new();
        let mut tokens = Vec::new();
        for i in 0..5 {
            let pid = pulse_id();
            let body_frames = frames(&[frame(i as u32)]);
            events.push(ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "loop_in".into(),
                source_port: "doubled".into(),
                target_node: "review".into(),
                target_port: "total".into(),
                frames: body_frames.clone(),
                value: json!(i * 8),
                closed: false,
                at_unix: 0,
            });
            events.push(ExecEvent::NodeStarted {
                color: color(),
                node_id: "review".into(),
                frames: body_frames.clone(),
                input: json!({"total": i * 8}),
                pulses_absorbed: vec![pid],
                closed_ports: vec![],
                at_unix: 0,
            });
            let tok = format!("tok-{i}");
            tokens.push(tok.clone());
            events.push(ExecEvent::NodeSuspended {
                color: color(),
                node_id: "review".into(),
                frames: body_frames.clone(),
                token: tok.clone(),
                at_unix: 0,
            });
            let spec = to_spec(Form {
                form_type: "human_query".into(),
                schema: FormSchema {
                    title: String::new(),
                    description: None,
                    fields: Vec::new(),
                },
                title: None,
                description: None,
                consumer_kind: None,
            });
            events.push(ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "review".into(),
                frames: body_frames,
                token: tok,
                spec,
                call_index: 0,
                at_unix: 0,
            });
        }
        events.push(ExecEvent::SuspensionResolved {
            color: color(),
            token: tokens[2].clone(),
            value: json!("approved"),
            at_unix: 0,
        });
        events.push(ExecEvent::NodeResumed {
            color: color(),
            node_id: "review".into(),
            frames: frames(&[frame(2)]),
            token: Some(tokens[2].clone()),
            value: Some(json!("approved")),
            pulses_absorbed: vec![],
            at_unix: 0,
        });
        events.push(ExecEvent::NodeCompleted {
            color: color(),
            node_id: "review".into(),
            frames: frames(&[frame(2)]),
            output: json!({"decision": "approved"}),
            closure_emissions: vec![],
            at_unix: 0,
        });

        let snap = fold_to_snapshot(color(), &events);
        let execs = snap.executions.get("review").expect("review execs");
        assert_eq!(execs.len(), 5, "exactly 5 records, one per iteration");

        let by_index: std::collections::HashMap<u32, &weft_core::exec::NodeExecution> = execs
            .iter()
            .map(|e| (e.frames[0].index, e))
            .collect();
        for i in 0u32..5 {
            let e = by_index.get(&i).expect("iteration present");
            if i == 2 {
                assert_eq!(e.status, NodeExecutionStatus::Completed);
                assert!(e.callback_id.is_none(), "resolved iteration has no callback");
            } else {
                assert_eq!(
                    e.status,
                    NodeExecutionStatus::WaitingForInput,
                    "iteration {i} still parked"
                );
                assert!(e.callback_id.is_some());
            }
        }
        assert_eq!(snap.suspensions.len(), 4);
        assert!(snap.pending_deliveries.is_empty());
    }

    /// Parallel-loop human-in-the-loop: a `parallel: true` loop launches
    /// all N lanes upfront, each lane's body node suspends on its OWN
    /// token at its OWN frame, the signals resolve OUT OF ORDER, and each
    /// lane resumes independently keyed by exact `(node_id, frames)`. The
    /// sibling of `partial_resume_across_loop_iterations` for the parallel
    /// drive mode: this is the path a 5-lane parallel form-wait takes, and
    /// the one the worker-kill/respawn refold has to reconstruct exactly.
    /// Pins that resolving lane 3 then lane 0 leaves lanes 1, 2, 4 parked
    /// at their precise frames, and that the loop instance saw all 5 lanes
    /// launched (so termination can later fire once all resolve).
    #[test]
    fn parallel_loop_lanes_resume_independently_out_of_order() {
        use weft_core::exec::NodeExecutionStatus;
        use weft_core::signal::{to_spec, Form, FormSchema};

        let mut events = vec![ExecEvent::LoopInstantiated {
            color: color(),
            group_id: "lp".into(),
            parent_frames: frames(&[]),
            iter_count: 5,
            parallel: true,
            max_iters: None,
            over: vec!["items".into()],
            carry: vec![],
            trim_on_mismatch: true,
            outer_input: HashMap::from([("items".into(), json!([0, 1, 2, 3, 4]))]),
            initial_carry: HashMap::new(),
            at_unix: 0,
        }];

        let mut tokens = Vec::new();
        for i in 0u32..5 {
            let pid = pulse_id();
            let body_frames = frames(&[frame(i)]);
            // Each lane is launched atomically with the pulse that wakes
            // its body node (the parallel-loop launch path).
            events.push(ExecEvent::LoopIterationLaunched {
                color: color(),
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                index: i,
                body_emissions: vec![LaunchedEmission {
                    pulse_id: pid.clone(),
                    source_node: "lp".into(),
                    source_port: "items".into(),
                    target_node: "review".into(),
                    target_port: "total".into(),
                    frames: body_frames.clone(),
                    value: json!(i),
                    closed: false,
                }],
                at_unix: 0,
            });
            events.push(ExecEvent::NodeStarted {
                color: color(),
                node_id: "review".into(),
                frames: body_frames.clone(),
                input: json!({ "total": i }),
                pulses_absorbed: vec![pid],
                closed_ports: vec![],
                at_unix: 0,
            });
            let tok = format!("tok-{i}");
            tokens.push(tok.clone());
            events.push(ExecEvent::NodeSuspended {
                color: color(),
                node_id: "review".into(),
                frames: body_frames.clone(),
                token: tok.clone(),
                at_unix: 0,
            });
            let spec = to_spec(Form {
                form_type: "human_query".into(),
                schema: FormSchema {
                    title: String::new(),
                    description: None,
                    fields: Vec::new(),
                },
                title: None,
                description: None,
                consumer_kind: None,
            });
            events.push(ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "review".into(),
                frames: body_frames,
                token: tok,
                spec,
                call_index: 0,
                at_unix: 0,
            });
        }

        // Snapshot the journal length BEFORE any resolution: this prefix
        // is exactly what a worker that crashed while all 5 lanes were
        // parked would refold from.
        let parked_prefix_len = events.len();

        // Signals arrive out of order: lane 3 first, then lane 0. Lanes
        // 1, 2, 4 never resolve and must stay parked at their frames.
        for lane in [3u32, 0u32] {
            events.push(ExecEvent::SuspensionResolved {
                color: color(),
                token: tokens[lane as usize].clone(),
                value: json!("approved"),
                at_unix: 0,
            });
            events.push(ExecEvent::NodeResumed {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[frame(lane)]),
                token: Some(tokens[lane as usize].clone()),
                value: Some(json!("approved")),
                pulses_absorbed: vec![],
                at_unix: 0,
            });
            events.push(ExecEvent::NodeCompleted {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[frame(lane)]),
                output: json!({ "decision": "approved" }),
                closure_emissions: vec![],
                at_unix: 0,
            });
        }

        let assert_shape = |snap: &ExecutionSnapshot| {
            let execs = snap.executions.get("review").expect("review execs");
            assert_eq!(execs.len(), 5, "exactly 5 records, one per lane");
            let by_index: std::collections::HashMap<u32, &weft_core::exec::NodeExecution> =
                execs.iter().map(|e| (e.frames[0].index, e)).collect();
            for i in 0u32..5 {
                let e = by_index.get(&i).expect("lane present");
                if i == 0 || i == 3 {
                    assert_eq!(
                        e.status,
                        NodeExecutionStatus::Completed,
                        "resolved lane {i} completed"
                    );
                    assert!(e.callback_id.is_none(), "resolved lane {i} has no callback");
                } else {
                    assert_eq!(
                        e.status,
                        NodeExecutionStatus::WaitingForInput,
                        "unresolved lane {i} still parked at its own frame"
                    );
                    assert!(e.callback_id.is_some(), "parked lane {i} keeps its callback");
                }
            }
            // 3 lanes (1, 2, 4) remain suspended; 2 resolved.
            assert_eq!(snap.suspensions.len(), 3, "three lanes still awaiting input");
            let key = LoopInstanceKey {
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                color: color(),
            };
            let inst = snap.loop_instances.get(&key).expect("loop instance");
            assert_eq!(inst.launched.len(), 5, "all 5 lanes launched");
            assert!(
                inst.terminated.is_none(),
                "loop cannot terminate while lanes still wait"
            );
            assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        };

        let snap = fold_to_snapshot(color(), &events);
        assert_shape(&snap);

        // Worker-respawn path, meaningfully: fold only the PREFIX (before
        // any resolution), exactly what a fresh worker rebuilds when the
        // crash happened with all 5 lanes parked. All 5 must reconstruct
        // as WaitingForInput at their own frames, and the loop must not
        // be terminated, so the late resolutions can then land on the
        // right lanes.
        let prefix_snap = fold_to_snapshot(color(), &events[..parked_prefix_len]);
        let prefix_execs = prefix_snap.executions.get("review").expect("review execs");
        assert_eq!(prefix_execs.len(), 5, "all 5 lanes reconstructed");
        for e in prefix_execs {
            assert_eq!(
                e.status,
                NodeExecutionStatus::WaitingForInput,
                "lane {} parked in the pre-resolution prefix",
                e.frames[0].index
            );
            assert!(e.callback_id.is_some(), "parked lane keeps its callback");
        }
        assert_eq!(prefix_snap.suspensions.len(), 5, "all 5 lanes awaiting input");
        let key = LoopInstanceKey {
            group_id: "lp".into(),
            parent_frames: frames(&[]),
            color: color(),
        };
        assert!(
            prefix_snap.loop_instances.get(&key).unwrap().terminated.is_none(),
            "loop not terminated while every lane is parked"
        );
    }

    /// Crash-replay atomicity: `LoopIterationLaunched` carries its
    /// body pulses in the same row, so a fold sees the index in
    /// `launched` AND the body pulses together (or neither). Pins
    /// that the carried pulses materialize exactly once and that
    /// re-folding the same journal is idempotent (the duplicate-row
    /// guard in `push_pulse` and the `launched.contains` check).
    #[test]
    fn loop_iteration_launched_carries_body_pulses_atomically() {
        let pid = pulse_id();
        let events = vec![
            ExecEvent::LoopInstantiated {
                color: color(),
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                iter_count: 3,
                parallel: false,
                max_iters: None,
                over: vec!["items".into()],
                carry: vec![],
                trim_on_mismatch: true,
                outer_input: HashMap::from([("items".into(), json!([1, 2, 3]))]),
                initial_carry: HashMap::new(),
                at_unix: 0,
            },
            ExecEvent::LoopOutFired {
                color: color(),
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                index: 0,
                gather_writes: HashMap::new(),
                carry_writes: HashMap::new(),
                done_vote: None,
                at_unix: 0,
            },
            ExecEvent::LoopIterationLaunched {
                color: color(),
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                index: 1,
                body_emissions: vec![LaunchedEmission {
                    pulse_id: pid.clone(),
                    source_node: "lp".into(),
                    source_port: "items".into(),
                    target_node: "body".into(),
                    target_port: "item".into(),
                    frames: frames(&[frame(1)]),
                    value: json!(2),
                    closed: false,
                }],
                at_unix: 0,
            },
        ];
        let assert_shape = |snap: &ExecutionSnapshot| {
            let bucket = snap.pulses.get("body").expect("body bucket");
            assert_eq!(bucket.len(), 1, "carried body pulse materializes exactly once");
            assert_eq!(bucket[0].id.to_string(), pid);
            assert_eq!(bucket[0].status, PulseStatus::Pending);
            assert_eq!(bucket[0].frames, frames(&[frame(1)]));
            let key = LoopInstanceKey {
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                color: color(),
            };
            let inst = snap.loop_instances.get(&key).expect("loop instance");
            assert_eq!(inst.launched, vec![1], "marker landed with the pulses");
            assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        };
        let snap = fold_to_snapshot(color(), &events);
        assert_shape(&snap);
        // Re-fold over a journal containing the row twice (the worst
        // a crash-replayed writer could produce): still one pulse,
        // still one launched entry.
        let mut doubled = events.clone();
        doubled.push(events[2].clone());
        let snap2 = fold_to_snapshot(color(), &doubled);
        assert_shape(&snap2);
    }

    #[test]
    fn loop_terminated_carries_outward_pulses_atomically() {
        // Mirror of the launch test for the TERMINATION crash window:
        // the loop's outward pulses ride inside the LoopTerminated row,
        // so a fold sees both `terminated` set AND the outward pulses
        // (or neither). A crash that left the outward pulses without the
        // terminal row would otherwise re-fire LoopOut and double-emit.
        let pid = pulse_id();
        let events = vec![
            ExecEvent::LoopInstantiated {
                color: color(),
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                iter_count: 1,
                parallel: false,
                max_iters: None,
                over: vec!["items".into()],
                carry: vec![],
                trim_on_mismatch: true,
                outer_input: HashMap::from([("items".into(), json!([1]))]),
                initial_carry: HashMap::new(),
                at_unix: 0,
            },
            ExecEvent::LoopTerminated {
                color: color(),
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                reason: weft_core::primitive::LoopTerminationReason::OverExhausted,
                outward_emissions: vec![LaunchedEmission {
                    pulse_id: pid.clone(),
                    source_node: "lp__out".into(),
                    source_port: "results".into(),
                    target_node: "sink".into(),
                    target_port: "in".into(),
                    frames: frames(&[]),
                    value: json!([10]),
                    closed: false,
                }],
                at_unix: 0,
            },
        ];
        let assert_shape = |snap: &ExecutionSnapshot| {
            let bucket = snap.pulses.get("sink").expect("sink bucket");
            assert_eq!(bucket.len(), 1, "outward pulse materializes exactly once");
            assert_eq!(bucket[0].id.to_string(), pid);
            assert_eq!(bucket[0].status, PulseStatus::Pending);
            let key = LoopInstanceKey {
                group_id: "lp".into(),
                parent_frames: frames(&[]),
                color: color(),
            };
            let inst = snap.loop_instances.get(&key).expect("loop instance");
            assert!(inst.terminated.is_some(), "terminated landed with the pulses");
            assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        };
        let snap = fold_to_snapshot(color(), &events);
        assert_shape(&snap);
        // Double the terminal row (worst a crash-replay could produce):
        // still one outward pulse, still terminated.
        let mut doubled = events.clone();
        doubled.push(events[1].clone());
        let snap2 = fold_to_snapshot(color(), &doubled);
        assert_shape(&snap2);
    }

    #[test]
    fn node_failed_carries_closure_pulses_atomically() {
        use weft_core::exec::NodeExecutionStatus;
        // A failed firing's unmentioned-port closures ride inside the
        // NodeFailed row, so a fold sees the terminal AND the closures
        // (or neither). Without this, a crash between two writes loses
        // the closures and downstream refolds Stuck.
        let pid = pulse_id();
        let events = vec![
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "n".into(),
                frames: frames(&[]),
                input: json!({}),
                pulses_absorbed: vec![],
                closed_ports: vec![],
                at_unix: 0,
            },
            ExecEvent::NodeFailed {
                color: color(),
                node_id: "n".into(),
                frames: frames(&[]),
                error: "boom".into(),
                closure_emissions: vec![LaunchedEmission {
                    pulse_id: pid.clone(),
                    source_node: "n".into(),
                    source_port: "out".into(),
                    target_node: "downstream".into(),
                    target_port: "in".into(),
                    frames: frames(&[]),
                    value: json!(null),
                    closed: true,
                }],
                at_unix: 0,
            },
        ];
        let assert_shape = |snap: &ExecutionSnapshot| {
            let bucket = snap.pulses.get("downstream").expect("downstream bucket");
            assert_eq!(bucket.len(), 1, "closure pulse materializes exactly once");
            assert_eq!(bucket[0].id.to_string(), pid);
            assert!(bucket[0].closed, "closure pulse is closed");
            let rec = &snap.executions.get("n").expect("n record")[0];
            assert_eq!(rec.status, NodeExecutionStatus::Failed);
            assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        };
        assert_shape(&fold_to_snapshot(color(), &events));
        // Double the terminal row: still one closure, still Failed.
        let mut doubled = events.clone();
        doubled.push(events[1].clone());
        assert_shape(&fold_to_snapshot(color(), &doubled));
    }

    fn make_spec() -> SignalSpec {
        use weft_core::signal::{to_spec, Form, FormSchema};
        to_spec(Form {
            form_type: "human_query".into(),
            schema: FormSchema {
                title: String::new(),
                description: None,
                fields: Vec::new(),
            },
            title: None,
            description: None,
            consumer_kind: None,
        })
    }

    /// Mid-suspension snapshot: a (node, frames) is parked. The
    /// fold should leave the record in WaitingForInput, with the
    /// suspension info preserved in `suspensions`. No completed_at.
    #[test]
    fn lifecycle_mid_suspension_state() {
        use weft_core::exec::NodeExecutionStatus;

        let pid = pulse_id();
        let token = "tok-park".to_string();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "review".into(),
                target_port: "in".into(),
                frames: frames(&[]),
                value: json!(7),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                input: json!({"in": 7}),
                pulses_absorbed: vec![pid],
                closed_ports: vec![],
                at_unix: 0,
            },
            ExecEvent::NodeSuspended {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                token: token.clone(),
                at_unix: 0,
            },
            ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                token: token.clone(),
                spec: make_spec(),
                call_index: 0,
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let execs = snap.executions.get("review").expect("review execs");
        assert_eq!(execs.len(), 1);
        assert_eq!(execs[0].status, NodeExecutionStatus::WaitingForInput);
        assert_eq!(execs[0].callback_id.as_deref(), Some(token.as_str()));
        assert!(execs[0].completed_at.is_none());
        assert_eq!(snap.suspensions.len(), 1);
    }

    /// A node firing journals a SEQUENCE of awaits/runs:
    ///   await_signal #0 -> (resolved with "first")
    ///   ctx.run #1 ("decide") -> journaled "go-left"
    ///   await_signal #2 -> (still pending, the live tail)
    /// should produce a per-(node, frames) sequence with 3 entries
    /// in call_index order, two flagged as resolved/run-output and
    /// the tail as pending await.
    #[test]
    fn multi_await_replay_sequence() {
        use weft_core::primitive::AwaitedEntryKind;

        let token0 = "tok-0".to_string();
        let token2 = "tok-2".to_string();
        let f = frames(&[]);
        let events = vec![
            ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "review".into(),
                frames: f.clone(),
                token: token0.clone(),
                spec: make_spec(),
                call_index: 0,
                at_unix: 0,
            },
            ExecEvent::SuspensionResolved {
                color: color(),
                token: token0.clone(),
                value: json!("first"),
                at_unix: 0,
            },
            ExecEvent::RunOutput {
                color: color(),
                node_id: "review".into(),
                frames: f.clone(),
                call_index: 1,
                name: "decide".into(),
                value: json!("go-left"),
                at_unix: 0,
            },
            ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "review".into(),
                frames: f.clone(),
                token: token2.clone(),
                spec: make_spec(),
                call_index: 2,
                at_unix: 0,
            },
        ];

        let snap = fold_to_snapshot(color(), &events);
        let key = ("review".to_string(), f.clone());
        let seq = snap
            .awaited_sequences
            .get(&key)
            .expect("sequence for (review, frames)");
        assert_eq!(seq.len(), 3, "three observable points journaled");

        // Entry 0: await resolved with "first".
        assert_eq!(seq[0].call_index, 0);
        match &seq[0].kind {
            AwaitedEntryKind::Await { token, resolved } => {
                assert_eq!(token, &token0);
                assert_eq!(resolved.as_ref().expect("resolved"), &json!("first"));
            }
            other => panic!("expected Await at 0, got {other:?}"),
        }
        // Entry 1: run output journaled.
        assert_eq!(seq[1].call_index, 1);
        match &seq[1].kind {
            AwaitedEntryKind::Run { name, value } => {
                assert_eq!(name, "decide");
                assert_eq!(value, &json!("go-left"));
            }
            other => panic!("expected Run at 1, got {other:?}"),
        }
        // Entry 2: await still pending (resolved=None, the tail).
        assert_eq!(seq[2].call_index, 2);
        match &seq[2].kind {
            AwaitedEntryKind::Await { token, resolved } => {
                assert_eq!(token, &token2);
                assert!(resolved.is_none(), "tail entry not yet resolved");
            }
            other => panic!("expected Await at 2, got {other:?}"),
        }

        // suspensions map still tracks both tokens (only NodeResumed
        // clears one); pending_deliveries holds token0's value.
        assert_eq!(snap.suspensions.len(), 2);
        assert!(snap.suspensions.contains_key(&token0));
        assert!(snap.suspensions.contains_key(&token2));
        assert_eq!(snap.pending_deliveries.get(&token0), Some(&json!("first")));
    }

    /// Pulses absorbed by a resume dispatch must fold to Absorbed.
    /// The bug left resume-time pulses Pending after a refold, so a
    /// fresh worker's `find_ready_nodes` (which keys purely on
    /// Pending pulses) re-fired an already-completed node on every
    /// respawn: double execution.
    #[test]
    fn node_resumed_absorbs_pulses() {
        let p1 = pulse_id();
        let p2 = pulse_id();
        let token = "tok-resume".to_string();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: p1.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "review".into(),
                target_port: "in".into(),
                frames: frames(&[]),
                value: json!(1),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                input: json!({"in": 1}),
                pulses_absorbed: vec![p1],
                closed_ports: vec![],
                at_unix: 0,
            },
            ExecEvent::NodeSuspended {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                token: token.clone(),
                at_unix: 0,
            },
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: p2.clone(),
                source_node: "other".into(),
                source_port: "out".into(),
                target_node: "review".into(),
                target_port: "extra".into(),
                frames: frames(&[]),
                value: json!(2),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeResumed {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                token: Some(token),
                value: Some(json!("approved")),
                pulses_absorbed: vec![p2],
                at_unix: 0,
            },
            ExecEvent::NodeCompleted {
                color: color(),
                node_id: "review".into(),
                frames: frames(&[]),
                output: json!({"done": true}),
                closure_emissions: vec![],
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let bucket = snap.pulses.get("review").expect("review pulse bucket");
        assert_eq!(bucket.len(), 2);
        for p in bucket {
            assert_eq!(
                p.status,
                PulseStatus::Absorbed,
                "pulse on port '{}' must fold Absorbed, not stay Pending",
                p.target_port
            );
        }
    }

    /// A second NodeStarted at the same (node, frames) AFTER a
    /// terminal record is a legitimate second firing and must open a
    /// NEW record. The bug collapsed it onto the terminal record,
    /// silently dropping the second firing's work on refold.
    #[test]
    fn node_started_after_terminal_opens_new_record() {
        use weft_core::exec::NodeExecutionStatus;

        let p1 = pulse_id();
        let p2 = pulse_id();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: p1.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "consumer".into(),
                target_port: "in".into(),
                frames: frames(&[]),
                value: json!(1),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "consumer".into(),
                frames: frames(&[]),
                input: json!({"in": 1}),
                pulses_absorbed: vec![p1],
                closed_ports: vec![],
                at_unix: 0,
            },
            ExecEvent::NodeCompleted {
                color: color(),
                node_id: "consumer".into(),
                frames: frames(&[]),
                output: json!({"n": 1}),
                closure_emissions: vec![],
                at_unix: 0,
            },
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: p2.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "consumer".into(),
                target_port: "in".into(),
                frames: frames(&[]),
                value: json!(2),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "consumer".into(),
                frames: frames(&[]),
                input: json!({"in": 2}),
                pulses_absorbed: vec![p2],
                closed_ports: vec![],
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let execs = snap.executions.get("consumer").expect("consumer execs");
        assert_eq!(execs.len(), 2, "second firing opens a second record");
        assert_eq!(execs[0].status, NodeExecutionStatus::Completed);
        assert_eq!(execs[1].status, NodeExecutionStatus::Running);
    }

    /// First kick wins; a second NodeKicked on the same node is a
    /// true no-op. The bug filled the entry's payload from the
    /// second kick, papering over a writer-level double-kick.
    #[test]
    fn node_kicked_second_kick_is_noop() {
        let events = vec![
            ExecEvent::NodeKicked {
                color: color(),
                node_id: "trigger".into(),
                payload: None,
                at_unix: 0,
            },
            ExecEvent::NodeKicked {
                color: color(),
                node_id: "trigger".into(),
                payload: Some(json!({"body": "late"})),
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let kick = snap.kicked.get("trigger").expect("trigger kick");
        assert!(kick.payload.is_none(), "first kick's payload (None) is authoritative");
    }

    /// SuspensionResolved and SuspensionRegistered are written by
    /// independent dispatcher paths with no ordering between them.
    /// When the resolution lands FIRST, the awaited entry must still
    /// be born resolved (the bug left it `resolved: None`: the await
    /// never resumed, permanent hang with the fire consumed).
    #[test]
    fn suspension_resolved_before_registered_still_resolves() {
        use weft_core::primitive::AwaitedEntryKind;

        let token = "tok-early".to_string();
        let f = frames(&[]);
        let events = vec![
            ExecEvent::SuspensionResolved {
                color: color(),
                token: token.clone(),
                value: json!("early-value"),
                at_unix: 0,
            },
            ExecEvent::SuspensionRegistered {
                color: color(),
                node_id: "review".into(),
                frames: f.clone(),
                token: token.clone(),
                spec: make_spec(),
                call_index: 0,
                at_unix: 0,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        let key = ("review".to_string(), f);
        let seq = snap.awaited_sequences.get(&key).expect("awaited sequence");
        assert_eq!(seq.len(), 1);
        match &seq[0].kind {
            AwaitedEntryKind::Await { token: t, resolved } => {
                assert_eq!(t, &token);
                assert_eq!(
                    resolved.as_ref().expect("entry born resolved despite out-of-order journal"),
                    &json!("early-value")
                );
            }
            other => panic!("expected Await, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod caller_event_wire_tests {
    use super::*;
    use uuid::Uuid;
    use weft_core::primitive::JournaledPayload;

    fn color() -> Color {
        Uuid::nil()
    }

    /// Every caller event round-trips through JSON unchanged. This is the
    /// cross-process wire contract (worker writes the journal, dispatcher
    /// folds and projects it); a renamed field would silently break
    /// inspector replay.
    fn round_trip(ev: ExecEvent) {
        let json = serde_json::to_string(&ev).expect("serialize");
        let back: ExecEvent = serde_json::from_str(&json).expect("deserialize");
        // Compare via re-serialization (ExecEvent isn't PartialEq).
        assert_eq!(
            serde_json::to_value(&ev).unwrap(),
            serde_json::to_value(&back).unwrap(),
            "round-trip changed the shape: {json}"
        );
    }

    #[test]
    fn connected_round_trips() {
        round_trip(ExecEvent::CallerConnected {
            color: color(),
            offset: 0,
            protocol: "websocket".into(),
            at_unix: 7,
        });
    }

    #[test]
    fn inbound_journaled_and_ephemeral_round_trip() {
        round_trip(ExecEvent::CallerInbound {
            color: color(),
            offset: 1,
            payload: JournaledPayload::Journaled {
                value: serde_json::json!({"q": "hi"}),
            },
            payload_byte_size: 10,
            payload_sha256_prefix: [1, 2, 3, 4, 5, 6, 7, 8],
            at_unix: 8,
        });
        // Ephemeral mode: metadata only, no value; must not collapse with
        // a journaled null.
        round_trip(ExecEvent::CallerInbound {
            color: color(),
            offset: 2,
            payload: JournaledPayload::Ephemeral,
            payload_byte_size: 999,
            payload_sha256_prefix: [9; 8],
            at_unix: 9,
        });
    }

    #[test]
    fn outbound_terminal_flag_round_trips() {
        round_trip(ExecEvent::CallerOutbound {
            color: color(),
            offset: 3,
            payload: JournaledPayload::Journaled {
                value: serde_json::json!("chunk"),
            },
            payload_byte_size: 5,
            payload_sha256_prefix: [0; 8],
            terminal: true,
            at_unix: 10,
        });
    }

    #[test]
    fn errored_and_disconnected_round_trip() {
        round_trip(ExecEvent::CallerErrored {
            color: color(),
            offset: 4,
            message: "node blew up".into(),
            at_unix: 11,
        });
        round_trip(ExecEvent::CallerDisconnected {
            color: color(),
            offset: 5,
            reason: "response complete".into(),
            at_unix: 12,
        });
    }

    #[test]
    fn kind_str_is_stable_for_caller_events() {
        // The kind string is the durable DB discriminant; a drift would
        // orphan existing rows.
        assert_eq!(
            ExecEvent::CallerConnected {
                color: color(), offset: 0, protocol: "http".into(), at_unix: 0,
            }
            .kind_str(),
            "caller_connected"
        );
        assert_eq!(
            ExecEvent::CallerDisconnected {
                color: color(), offset: 0, reason: String::new(), at_unix: 0,
            }
            .kind_str(),
            "caller_disconnected"
        );
    }

    #[test]
    fn caller_events_are_observability_only_in_fold() {
        // Folding a stream of caller events must not panic and must not
        // synthesize node/pulse state (they are non-durable, replay-only).
        let events = vec![
            ExecEvent::CallerConnected {
                color: color(), offset: 0, protocol: "http".into(), at_unix: 0,
            },
            ExecEvent::CallerInbound {
                color: color(), offset: 1,
                payload: JournaledPayload::Ephemeral,
                payload_byte_size: 1, payload_sha256_prefix: [0; 8], at_unix: 1,
            },
            ExecEvent::CallerDisconnected {
                color: color(), offset: 2, reason: "done".into(), at_unix: 2,
            },
        ];
        let snap = fold_to_snapshot(color(), &events);
        assert!(snap.executions.is_empty(), "caller events create no node state");
        assert!(snap.corruptions.is_empty(), "caller events fold cleanly");
    }
}
