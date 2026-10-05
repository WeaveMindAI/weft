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
    /// Worker: cancel a running execution by execution. Never claimed: the
    /// worker driving the execution hears it announced on its line and asks
    /// for it (`tasks::cancels_asked`).
    CancelExecution,
    /// Dispatcher: journal a `CostReported` event for one metered
    /// call (a provider meter's figure). Routed
    /// through the task table (not direct journal write) so a
    /// worker dying mid-call still has the cost record
    /// committed: the atomic INSERT into `task` is the
    /// durable handoff, and the dispatcher's executor catches up
    /// later regardless of process state.
    RecordCost,
    /// Dispatcher: journal a `LogLine` event on behalf of a worker.
    /// Same durability rationale as `RecordCost`.
    RecordLog,
    /// Dispatcher: stop every live execution of a project carrying a
    /// tag, on behalf of one of its executions (`ctx.stop_tagged`).
    /// Producer = the broker's `/v1/execution/stop_tagged` handler,
    /// which resolves the ordering anchor at enqueue time; consumer =
    /// the dispatcher's executor, which cancels each match through the
    /// one cancel path. Rides the task table so the stop survives the
    /// asking worker dying right after it asked.
    StopTagged,
    /// Dispatcher: one call a program makes on its own project
    /// (`weft_core::program::ProgramCall`: an infra copy, triggers, a
    /// instance's connections, costs, a clean, instance tokens). Producer =
    /// worker (via broker, which pins the project and the asker to the
    /// run); the worker waits on the task's result.
    ProgramCall,
}

// This enum holds only the kinds the dispatcher itself ships. A runtime that
// adds its own task kinds registers + enqueues them by string via the
// string-keyed task dispatch, without widening this enum.

impl TaskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RouteEntry => "route_entry",
            Self::RegisterSignal => "register_signal",
            Self::FireSignal => "fire_signal",
            Self::Execute => "execute",
            Self::Resume => "resume",
            Self::CancelExecution => "cancel_execution",
            Self::RecordCost => "record_cost",
            Self::RecordLog => "record_log",
            Self::StopTagged => "stop_tagged",
            Self::ProgramCall => "program_call",
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
    pub project_id: uuid::Uuid,
    pub execution_id: String,
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
    /// dispatcher's `/connect` endpoint): the trigger's signal spec, from
    /// which the worker recovers the wire protocol (the tag: `route` ->
    /// HTTP, `socket` -> WS) and the connection knobs, plus what the
    /// caller sent to open the exchange, which the worker puts on the
    /// connection so nodes read it. `None` for every ordinary pull-queue
    /// / resume execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_connection: Option<LiveConnectionStart>,
    /// Present only for an UNRECORDED run (`weft_core::exec::RunKind`):
    /// its birth rows (the `ExecutionStarted`, then its kicks), which are
    /// never written to the journal. The worker seeds the run's
    /// in-memory journal with them. Journal rows as JSON, because this
    /// crate sits below the journal's types. Such a task is never run
    /// twice: a re-run would repeat whatever the first one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unrecorded_birth: Option<Vec<serde_json::Value>>,
    /// How long the execution may run, fixed where it started (its
    /// signal's registration, or `weft run --long`): a short one is served
    /// as one request to a worker, a long one as a job of its own. Read
    /// by the delivery (`tasks::take_deliveries`), so it is on every
    /// execute and resume.
    // SYNC: run_class <-> crates/weft-task-store/src/tasks.rs (take_deliveries reads payload ->> 'run_class')
    pub run_class: weft_core::run_class::RunClass,
}

/// What a live-caller execution starts with: the trigger's full signal
/// spec (kind tag + config body) and the caller's opening request, as the
/// handshake read it. ONE record, built at the birth, read once on the
/// worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveConnectionStart {
    pub spec: weft_core::primitive::SignalSpec,
    pub request: weft_core::caller::LiveRequest,
    /// When a caller is coming: the unix second their routing token
    /// expires. The run is born at the handshake and waits for them; only
    /// the worker their connection reaches claims it (it is never
    /// delivered), and a caller who never came is erased with it once this
    /// passes (`tasks::callers_never_arrived`). `None` for a fired run,
    /// which has nobody coming.
    // SYNC: arrive_by <-> crates/weft-task-store/src/tasks.rs (AWAITS_CALLER)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrive_by: Option<i64>,
    /// Set when the run was FIRED rather than reached by a caller
    /// (`weft run --fire` on a Route). `None` for every real caller,
    /// which is every run that arrives through the gateway.
    ///
    /// The point is the loop. A route's program is unrunnable offline
    /// without this, because the trigger and every Reply behind it ask
    /// for the caller and a fired run has nobody there, so trying a
    /// route meant an install, an activation and an image build before
    /// you could see one value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fired: Option<FiredExchange>,
}

/// The stand-in caller's side of a fired run: there is no socket
/// coming, so the trigger serves the request the author typed and the
/// program's answer goes to the journal instead of a wire.
///
/// It carries nothing. Its PRESENCE is the whole message: this run was
/// fired, so serve a stand-in rather than failing for want of a caller.
/// A bool would say the same thing and read worse at the call sites,
/// where `fired: Some(..)` is the fact being tested.
///
/// It used to carry the body, lifted out of the fire payload so the
/// stand-in could serve it back through the ordinary request call. That
/// meant the language held a field name (`body`) to make the trick
/// work, and a caller trigger's own vocabulary had leaked one level
/// down. The body now stays in the payload, where the author put it,
/// and the node reads it off its own wake when the request carries
/// none.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiredExchange {}

/// Payload for `TaskKind::FireSignal`. Producer = listener; consumer =
/// dispatcher's executor, which calls `dispatch_listener_outcome`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FireSignalPayload {
    pub token: String,
    pub payload: serde_json::Value,
    /// The holder a held connection fired under. The broker takes the
    /// fire only while the signal is still held under that name, so a
    /// copy that lost the row delivers nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelExecutionPayload {
    pub project_id: uuid::Uuid,
    pub execution_id: String,
    /// Why the run is being cancelled. The owning worker flips the
    /// execution's flag WITH this cause, so the terminal event it writes
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
    pub project_id: uuid::Uuid,
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
    pub execution_id: String,
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
    pub execution_id: String,
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
                project_id: uuid::Uuid::nil(),
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
            project_id: uuid::Uuid::nil(),
            execution_id: "c2".into(),
            cause: weft_core::exec::CancelCause::Execution { by: uuid::Uuid::nil(), tag: "user_7".into() },
        };
        let json = serde_json::to_value(&cancel).unwrap();
        let back: CancelExecutionPayload = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(back.cause, cancel.cause, "{json}");
        assert_eq!(TaskKind::StopTagged.as_str(), "stop_tagged");
        assert_eq!(TaskKind::ProgramCall.as_str(), "program_call");
    }

    /// `RecordCostPayload` is the whole worker->dispatcher cost contract, so
    /// its wire shape is pinned here: both the resolved and the
    /// honest-unknown (`amount_usd: null`) arms, the always-false worker
    /// `billed`, and the key origin.
    #[test]
    fn record_cost_payload_round_trip_both_amount_arms() {
        for amount_usd in [Some(0.000031), None] {
            let payload = RecordCostPayload {
                execution_id: "c1".into(),
                node_id: "ask".into(),
                frames: vec![weft_core::frames::Frame::Loop { index: 2 }],
                service: "openrouter".into(),
                model: Some("m".into()),
                amount_usd,
                billed: false,
                origin: weft_core::CredentialOwner::Author,
                metadata: serde_json::json!({ "tokensPrompt": 12 }),
            };
            let v = serde_json::to_value(&payload).unwrap();
            // The full literal object pins the FIELD NAMES on the wire (a
            // symmetric struct-field rename would round-trip green while
            // breaking every peer that reads the raw JSON).
            assert_eq!(
                v,
                serde_json::json!({
                    "execution_id": "c1", "node_id": "ask", "frames": [{"index": 2}],
                    "service": "openrouter", "model": "m", "amount_usd": amount_usd,
                    "billed": false, "origin": "author",
                    "metadata": {"tokensPrompt": 12}
                })
            );
            let back: RecordCostPayload = serde_json::from_value(v).unwrap();
            assert_eq!(back.amount_usd, amount_usd);
            assert!(!back.billed);
            assert_eq!(back.origin, weft_core::CredentialOwner::Author);
            assert_eq!(back.frames, vec![weft_core::frames::Frame::Loop { index: 2 }]);
        }
    }

    /// The live start record rides the execute payload whole: the spec
    /// and the caller's opening request both survive the wire, and an
    /// ordinary execution omits the field.
    #[test]
    fn live_connection_start_round_trips_on_the_execute_payload() {
        let mut request = weft_core::caller::LiveRequest {
            method: "POST".into(),
            path: "chat/room7".into(),
            ..Default::default()
        };
        request.params.insert("room".into(), "room7".into());
        request.caller = Some(serde_json::json!({ "key": 1 }));
        let payload = ExecutionPayload {
            project_id: uuid::Uuid::nil(),
            execution_id: "c1".into(),
            definition_hash: "h".into(),
            live_connection: Some(LiveConnectionStart {
                spec: weft_core::primitive::SignalSpec::of_kind(
                    "route",
                    serde_json::json!({ "path": "chat/{room}" }),
                ),
                request: request.clone(),
                arrive_by: Some(1_000),
                fired: None,
            }),
            unrecorded_birth: None,
            run_class: weft_core::run_class::RunClass::Short,
        };
        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["live_connection"]["spec"]["kind"], "route");
        assert!(
            json["live_connection"].get("fired").is_none(),
            "a real caller carries no stand-in body, and an absent one costs no bytes on the wire"
        );
        assert_eq!(json["live_connection"]["request"]["params"]["room"], "room7");
        assert_eq!(json["live_connection"]["arrive_by"], 1_000, "the claim and the reaper read it by this name");
        let back: ExecutionPayload = serde_json::from_value(json).unwrap();
        assert_eq!(back.live_connection.unwrap().request, request);

        let plain = ExecutionPayload {
            project_id: uuid::Uuid::nil(),
            execution_id: "c1".into(),
            definition_hash: "h".into(),
            live_connection: None,
            unrecorded_birth: None,
            run_class: weft_core::run_class::RunClass::Long,
        };
        let json = serde_json::to_value(&plain).unwrap();
        assert_eq!(json["run_class"], "long", "the delivery reads it off the payload by this name");
        assert!(json.get("live_connection").is_none(), "an ordinary execution omits it");
        assert!(json.get("unrecorded_birth").is_none(), "a recorded run carries no birth rows");

        let unrecorded = ExecutionPayload {
            unrecorded_birth: Some(vec![serde_json::json!({ "kind": "execution_started" })]),
            ..plain
        };
        let back: ExecutionPayload = serde_json::from_value(serde_json::to_value(&unrecorded).unwrap()).unwrap();
        assert_eq!(back.unrecorded_birth.unwrap().len(), 1);
    }

    /// A fired run still reads as fired after the trip to the worker.
    ///
    /// This is the shape that broke, twice over. It was once an
    /// `Option<Value>` holding the body, and a GET's body is null, so
    /// `Some(Null)` went out on the wire as `null` and came back
    /// `None`: the dispatcher said "here is a stand-in caller" and the
    /// worker heard "no caller", waited for a socket nobody was going
    /// to open, and the Route node failed telling the author to trigger
    /// it through a Route. It now carries nothing at all, so the only
    /// thing that can survive the trip is the one thing that matters.
    #[test]
    fn a_fired_run_survives_the_wire() {
        let payload = ExecutionPayload {
            project_id: uuid::Uuid::nil(),
            execution_id: "c1".into(),
            definition_hash: "h".into(),
            live_connection: Some(LiveConnectionStart {
                spec: weft_core::primitive::SignalSpec::of_kind("route", serde_json::json!({})),
                request: weft_core::caller::LiveRequest {
                    method: "GET".into(),
                    path: "cards".into(),
                    ..Default::default()
                },
                arrive_by: None,
                fired: Some(FiredExchange {}),
            }),
            unrecorded_birth: None,
            run_class: weft_core::run_class::RunClass::Short,
        };
        let json = serde_json::to_value(&payload).unwrap();
        let back: ExecutionPayload = serde_json::from_value(json).unwrap();
        let start = back.live_connection.expect("the live connection survives");
        assert!(start.fired.is_some(), "a fired run must still read as fired after the trip");
    }
}

