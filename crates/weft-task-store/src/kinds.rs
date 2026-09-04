//! Wire contract for the task queue: the kind enum and the typed
//! payload struct for every kind. Both producers (dispatcher,
//! engine `context`) and consumers (the dispatcher and engine
//! pickers) refer to these definitions, so a typo can't drift the
//! two sides apart silently.
//!
//! Each `TaskKind` variant maps to one `*Payload` struct with the
//! exact JSON shape the executor expects. The string returned by
//! `TaskKind::as_str()` is the canonical wire tag persisted to the
//! `task.kind` column.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// Dispatcher: route an incoming entry-fire payload onto a
    /// fresh execution. Producer = the public-fire HTTP handler.
    RouteEntry,
    /// Dispatcher: register a wake signal with the listener and
    /// return its mint info to the worker that asked.
    RegisterSignal,
    /// Dispatcher: spawn a worker Pod for the project's pool.
    SpawnPod,
    /// Dispatcher: fire a held-event signal that the listener
    /// observed (Timer fired, SSE event arrived, future browser
    /// session resolved). Producer = listener (via broker).
    /// Replaces the `/signal/internal-resume` HTTP push: keeping
    /// listener → dispatcher coordination on the task table means
    /// the listener never opens an HTTP connection to the
    /// dispatcher and the trust seam stays at the broker.
    FireSignal,
    /// Worker: run a fresh execution.
    Execute,
    /// Worker: resume a suspended execution after a fire.
    Resume,
    /// Worker: cancel a running execution by color. Addressed to
    /// one pod via `target_pod_name`.
    CancelExecution,
    /// Dispatcher: journal a `CostReported` event for one metered
    /// call (a provider meter's figure). Routed
    /// through the task table (not direct journal write) so a
    /// worker pod dying mid-call still has the cost record
    /// committed: the atomic INSERT into `task` is the
    /// durable handoff, and the dispatcher's executor catches up
    /// later regardless of pod state.
    RecordCost,
    /// Dispatcher: journal a `LogLine` event on behalf of a worker.
    /// Same durability rationale as `RecordCost`.
    RecordLog,
    /// Dispatcher: persist a signal kind's evolving durable state (a
    /// delta-poll cursor) onto its signal row. Producer = listener
    /// (via broker), same trust seam as `FireSignal`: the listener
    /// never opens an HTTP connection to the dispatcher.
    UpdateSignalKindState,
    /// Dispatcher: stop every live execution of a project carrying a
    /// tag, on behalf of one of its executions (`ctx.stop_tagged`).
    /// Producer = the broker's `/v1/execution/stop_tagged` handler,
    /// which resolves the ordering anchor at enqueue time; consumer =
    /// the dispatcher's executor, which cancels each match through the
    /// one cancel path. Rides the task table so the stop survives the
    /// asking worker dying right after it asked.
    StopTagged,
}

// This enum holds only the kinds the dispatcher itself ships. A runtime that
// adds its own task kinds registers + enqueues them by string via the
// string-keyed task dispatch, without widening this enum.

impl TaskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RouteEntry => "route_entry",
            Self::RegisterSignal => "register_signal",
            Self::SpawnPod => "spawn_pod",
            Self::FireSignal => "fire_signal",
            Self::Execute => "execute",
            Self::Resume => "resume",
            Self::CancelExecution => "cancel_execution",
            Self::RecordCost => "record_cost",
            Self::RecordLog => "record_log",
            Self::UpdateSignalKindState => "update_signal_kind_state",
            Self::StopTagged => "stop_tagged",
        }
    }
}

impl From<TaskKind> for String {
    fn from(k: TaskKind) -> String {
        k.as_str().to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPayload {
    pub project_id: String,
    pub color: String,
    /// `running_definition_hash` snapshotted at enqueue time (same
    /// value the journal's `ExecutionStarted` carries). The worker
    /// passes it as `expected_hash` to the broker's
    /// `project_fetch_definition`, which looks the shape up in the
    /// APPEND-ONLY `project_definition` history keyed by
    /// `(project_id, hash)`: the execution always runs on the shape
    /// the user clicked Run against, even when a later edit advances
    /// the project row's hash before the worker claims the task. A
    /// missing history row is a hard 404 (the worker fails the
    /// execution loudly); there is no race semantics on this path,
    /// the hash IS the lookup key.
    pub definition_hash: String,
    /// Present only for executions STARTED by a live-caller handshake (the
    /// dispatcher's `/connect` endpoint). Opaque to this generic store:
    /// carries the trigger's full signal spec JSON (kind tag + config body),
    /// from which the worker recovers BOTH the wire protocol (the tag:
    /// `api_endpoint` -> HTTP, `live_socket` -> WS) and the connection knobs
    /// (the body) to build the `CallerConnection` runtime config and expect
    /// a caller to attach for this color. `None` for every ordinary
    /// pull-queue / resume execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_connection: Option<serde_json::Value>,
}

/// Payload for `TaskKind::FireSignal`. Producer = listener; consumer =
/// dispatcher's executor, which calls `dispatch_listener_outcome`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FireSignalPayload {
    pub token: String,
    pub payload: serde_json::Value,
}

/// Payload for `TaskKind::UpdateSignalKindState`. Producer =
/// listener; consumer = the dispatcher's executor, which writes the
/// signal row. Two fences make the write safe against reordering:
/// `placement_generation` (a drained pod's write is rejected once the
/// signal re-placed) and `seq` (a strictly increasing per-holder
/// counter, persisted as the row's own `kind_state_seq` column; an
/// older update arriving late can never regress a newer cursor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSignalKindStatePayload {
    pub token: String,
    pub kind_state: serde_json::Value,
    pub seq: i64,
    pub placement_generation: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelExecutionPayload {
    pub project_id: String,
    pub color: String,
    /// Why the run is being cancelled. The owning worker flips the
    /// color's flag WITH this cause, so the terminal event it writes
    /// (when it beats the dispatcher's own write to the journal) names
    /// the same cause the dispatcher would have.
    pub cause: weft_core::exec::CancelCause,
}

/// Payload for `TaskKind::StopTagged`: "stop every live execution of
/// `project_id` carrying `tag`, asked by execution `by`". The broker
/// builds it when the worker calls `ctx.stop_tagged`, resolving
/// `before_seq` THEN (synchronously with the node's call), so a stop
/// that executes late can never reach a sibling that tagged itself
/// after the ask.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopTaggedPayload {
    pub project_id: String,
    pub tag: String,
    /// The execution that asked. Excluded from the targets under
    /// `StopSelf::Keep`, named as the canceller on every stopped run.
    pub by: String,
    /// Only executions whose tag row has a sequence below this are
    /// stopped. `None` means every live execution carrying the tag
    /// (the `Include` shape: no ordering, the whole batch goes).
    pub before_seq: Option<i64>,
    pub stop_self: weft_core::StopSelf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnPodPayload {
    pub project_id: String,
    pub tenant: String,
    pub namespace: String,
    pub owner_dispatcher: String,
}

/// Payload for `TaskKind::RecordCost`: one metered call's cost, produced by
/// a provider meter, journaled as a `CostReported` event attributed to the
/// exact firing (`node_id` + `frames`). `amount_usd: None` = the meter could
/// not resolve the figure (recorded as unknown, never $0). `billed` = the
/// figure moved credits, vs a measurement on a key the user holds. The
/// dispatcher's executor validates a present amount is `>= 0` and writes the
/// journal event; the broker also rejects bad amounts on enqueue so a
/// malicious worker can't submit and immediately die before validation runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordCostPayload {
    pub color: String,
    pub node_id: String,
    pub frames: weft_core::LoopFrames,
    pub service: String,
    pub model: Option<String>,
    pub amount_usd: Option<f64>,
    pub billed: bool,
    /// Whose key the call spent (the access the metered call rode).
    pub origin: weft_core::CredentialOwner,
    pub metadata: serde_json::Value,
}

/// Payload for `TaskKind::RecordLog`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordLogPayload {
    pub color: String,
    /// The node that wrote the line, and the iteration it was in.
    /// `default` so a task enqueued by an older worker still decodes.
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub frames: weft_core::LoopFrames,
    pub level: String,
    pub message: String,
    /// When the node wrote it, on the worker's clock, in milliseconds:
    /// the dispatcher journals the line later, whenever it drains the
    /// task, and that moment says nothing about the run. `default` so
    /// a task enqueued by an older worker still decodes; it then reads
    /// at the drain, which is all that worker ever recorded.
    #[serde(default)]
    pub at_unix_ms: Option<u64>,
    /// Its place among the firing's side effects: the same counter
    /// the dedup key carries, so two lines one firing wrote in the
    /// same millisecond still read back in order.
    #[serde(default)]
    pub seq: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two steering payloads are the broker->dispatcher (`stop_tagged`)
    /// and dispatcher->worker (`cancel_execution`) contracts for a tag
    /// stop: both anchor shapes and both self choices round-trip, and the
    /// cancel carries its structured cause.
    #[test]
    fn steering_payloads_round_trip() {
        for (before_seq, stop_self) in
            [(Some(17), weft_core::StopSelf::Keep), (None, weft_core::StopSelf::Include)]
        {
            let payload = StopTaggedPayload {
                project_id: "p1".into(),
                tag: "user_7".into(),
                by: "c1".into(),
                before_seq,
                stop_self,
            };
            let json = serde_json::to_value(&payload).unwrap();
            let back: StopTaggedPayload = serde_json::from_value(json.clone()).unwrap();
            assert_eq!(back.before_seq, before_seq, "{json}");
            assert_eq!(back.stop_self, stop_self, "{json}");
            assert_eq!(back.tag, "user_7");
        }
        let cancel = CancelExecutionPayload {
            project_id: "p1".into(),
            color: "c2".into(),
            cause: weft_core::exec::CancelCause::Execution { by: uuid::Uuid::nil(), tag: "user_7".into() },
        };
        let json = serde_json::to_value(&cancel).unwrap();
        let back: CancelExecutionPayload = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(back.cause, cancel.cause, "{json}");
        assert_eq!(TaskKind::StopTagged.as_str(), "stop_tagged");
    }

    /// `RecordCostPayload` is the whole worker->dispatcher cost contract, so
    /// its wire shape is pinned here: both the resolved and the
    /// honest-unknown (`amount_usd: null`) arms, the always-false worker
    /// `billed`, and the key origin.
    #[test]
    fn record_cost_payload_round_trip_both_amount_arms() {
        for amount_usd in [Some(0.000031), None] {
            let payload = RecordCostPayload {
                color: "c1".into(),
                node_id: "ask".into(),
                frames: vec![weft_core::LoopIteration { index: 2 }],
                service: "openrouter".into(),
                model: Some("m".into()),
                amount_usd,
                billed: false,
                origin: weft_core::CredentialOwner::TheirOwn,
                metadata: serde_json::json!({ "tokensPrompt": 12 }),
            };
            let v = serde_json::to_value(&payload).unwrap();
            // The full literal object pins the FIELD NAMES on the wire (a
            // symmetric struct-field rename would round-trip green while
            // breaking every peer that reads the raw JSON).
            assert_eq!(
                v,
                serde_json::json!({
                    "color": "c1", "node_id": "ask", "frames": [{"index": 2}],
                    "service": "openrouter", "model": "m", "amount_usd": amount_usd,
                    "billed": false, "origin": "their-own",
                    "metadata": {"tokensPrompt": 12}
                })
            );
            let back: RecordCostPayload = serde_json::from_value(v).unwrap();
            assert_eq!(back.amount_usd, amount_usd);
            assert!(!back.billed);
            assert_eq!(back.origin, weft_core::CredentialOwner::TheirOwn);
            assert_eq!(back.frames, vec![weft_core::LoopIteration { index: 2 }]);
        }
    }
}
