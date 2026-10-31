//! Wire contract for the task queue: the kind enum and the typed
//! payload struct for every kind. Both producers (a worker through the
//! broker, a listener, the dispatcher) and the dispatcher's executors refer
//! to these definitions, so a typo can't drift the two sides apart
//! silently.
//!
//! Each `TaskKind` variant maps to one `*Payload` struct with the
//! exact JSON shape the executor expects. The string returned by
//! `TaskKind::as_str()` is the canonical wire tag persisted to the
//! `task.kind` column.

use serde::{Deserialize, Serialize};

weft_core::wire_enum! {
    /// What a task asks for, stored as its tag on `task.kind`: the tag is
    /// written once, on the variant, and the wire, the column and the
    /// executor registry all read it from there.
    pub enum TaskKind {
        /// Register a wake signal with the listener and return its mint info
        /// to the worker that asked.
        RegisterSignal = "register_signal",
        /// Withdraw a wait a run gave up (`WithdrawSignalPayload`): its signal
        /// goes, and the listener lets go of it, so nobody is still asked to
        /// answer it. Producer = worker (via broker).
        WithdrawSignal = "withdraw_signal",
        /// Send the last call of a wait whose run was cancelled
        /// (`weft_core::signal::listener_protocol::CancelCallRequest`, the
        /// payload as it is), through the listener. Producer = the
        /// dispatcher's cancel, in the transaction that takes the wait
        /// down, so a cancel never loses its call to a crash; no caller of
        /// the broker may enqueue it. Deduplicated on [`cancel_call_key`].
        CancelCall = "cancel_call",
        /// An answer to a waiting run that a listener picked up (a timer's
        /// tick, an event on a connection it holds), already processed by its
        /// kind. Producer = listener (via broker), which never opens an HTTP
        /// connection to the dispatcher, so the trust seam stays at the
        /// broker.
        FireSignal = "fire_signal",
        /// Stop every live run of a project carrying a tag, on behalf
        /// of one of its runs (`ctx.stop_tagged`). Producer = the
        /// broker's `/v1/run/stop_tagged` handler, which resolves the
        /// ordering anchor at enqueue time; consumer = the dispatcher's
        /// executor, which cancels each match through the one cancel path.
        /// Rides the task table so the stop survives the asking worker dying
        /// right after it asked.
        StopTagged = "stop_tagged",
        /// One call a program makes on its own project
        /// (`weft_core::program::ProgramCall`: an infra copy, triggers, a
        /// instance's connections, costs, a clean, instance tokens). Producer =
        /// worker (via broker, which pins the project and the asker to the
        /// run); the worker waits on the task's result.
        ProgramCall = "program_call",
        /// Run one node self-test on the project's test server and complete
        /// with its report (`RunNodeTestPayload`). Producer = the dispatcher's
        /// own node-test API; no caller of the broker may enqueue it.
        RunNodeTest = "run_node_test",
        /// Drive one resync's take-down and bring-back (`ResyncPayload`).
        /// Producer = the dispatcher's own resync request, in the transaction
        /// that writes the plan down; no caller of the broker may enqueue it.
        Resync = "resync",
        /// Wait for the runs a person's deactivate left going to end
        /// (`TakeDownWaitPayload`), apart from the request, which a
        /// person follows in short looks however long it takes. Producer
        /// = the dispatcher's own deactivate; no caller of the broker may
        /// enqueue it.
        TakeDownWait = "take_down_wait",
        /// Wait for what `weft rm` cancelled to be over
        /// (`RemovalWaitPayload`): the last calls of its cancelled runs
        /// gone out, and, before its history is erased, every run ended.
        /// Apart from the request, which a person follows in short looks.
        /// Producer = the dispatcher's own quiesce; no caller of the broker
        /// may enqueue it.
        RemovalWait = "removal_wait",
        /// Lift the install's maintenance (`weft maintenance off`): start
        /// the infra copies `weft maintenance on` stopped, then bring back
        /// the triggers it took down, then end the maintenance once nothing
        /// is left. Install-wide (no project) and with no payload (`{}`):
        /// what it brings back is what the maintenance tables hold, so a
        /// task run again carries on from what is left. Deduplicated on
        /// [`MAINTENANCE_LIFT_KEY`]. Producer = the dispatcher's own
        /// maintenance API; no caller of the broker may enqueue it.
        MaintenanceLift = "maintenance_lift",
        /// Turn on a project's triggers (`weft activate`, weft bringing back
        /// triggers that went down with their infra or the health check, a
        /// change re-arming the triggers that read it), apart from whoever
        /// asked: an activation waits on its setup run, which may wait on a
        /// person, and the process asking may go away meanwhile. Carried by
        /// its task, whose claim another dispatcher rescues, it goes on
        /// from where its rows say it is. Its payload is the dispatcher's
        /// own (`weft_dispatcher::task_kinds::activate`): the dispatcher is
        /// its only producer, and no caller of the broker may enqueue it.
        Activate = "activate",
    }
}

/// The dedup key of the [`TaskKind::MaintenanceLift`]: one lift at a time,
/// so a second `weft maintenance off` while one goes follows that one.
pub const MAINTENANCE_LIFT_KEY: &str = "maintenance_lift";

/// The dedup key of the [`TaskKind::CancelCall`] of the wait `token`: one
/// per wait, so a cancel that runs twice queues its call once.
pub fn cancel_call_key(token: &str) -> String {
    format!("cancel_call:{token}")
}

/// Payload for `TaskKind::FireSignal`: an answer to a waiting run a
/// listener picked up, already processed by its kind (`/process` ran in
/// the listener), so the dispatcher hands it to its run without asking
/// the listener again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FireSignalPayload {
    pub token: String,
    /// The run the answer resolves.
    pub run_id: weft_core::RunId,
    /// The answer, as the kind made it of the event.
    pub value: serde_json::Value,
    /// The holder a held connection picked the answer up under. The
    /// broker takes it only while the signal is still held under that
    /// name, so a copy that lost the row delivers nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_by: Option<String>,
}

/// Payload for `TaskKind::WithdrawSignal`: the wait `token` of run
/// `run_id`, given up. The broker takes it only when the wait is
/// that run's.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawSignalPayload {
    pub run_id: weft_core::RunId,
    pub token: String,
}

/// Payload for `TaskKind::RegisterSignal`: a node's signal to arm, from
/// the worker running it (a suspension, or a trigger's setup) or from
/// activation arming a captured trigger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSignalPayload {
    pub run_id: weft_core::RunId,
    pub node_id: String,
    pub frames: weft_core::frames::LoopFrames,
    pub spec: weft_core::primitive::SignalSpec,
    pub is_resume: bool,
    /// 0-based ordinal of the `await_signal` call within this
    /// (run, node_id, frames). Set by the worker, which stamps it on
    /// the `SuspensionRegistered` it records, so replay can rebuild the
    /// per-(node, frames) sequence in order. Must not vary across replays
    /// of the same body, so the dedup key and the resume token include it.
    /// Required: a missing field would silently default to 0 and collide
    /// every await on the same frame stack.
    pub call_index: u32,
    /// The trigger's delivered port values at registration time (an
    /// entry registration only; `None` on resumes). Stored on the
    /// signal row and replayed onto the trigger's ports at every fire.
    #[serde(default)]
    pub port_snapshot: Option<serde_json::Value>,
    /// When the registration was asked for (ms since the epoch): the
    /// worker stamps the moment the node called in, activation stamps
    /// its own start. Handed to the listener so a relative wait counts
    /// from here, whatever the trip to the listener cost.
    pub asked_at_unix_ms: i64,
}

/// Payload for `TaskKind::RunNodeTest`: which test of which node, run on
/// which image, for which project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunNodeTestPayload {
    pub project_id: uuid::Uuid,
    pub tenant: String,
    /// The per-package node-test image to run (content-addressed).
    /// Minted by the enqueuer, run verbatim.
    pub image_ref: String,
    /// Node type under test.
    pub node: String,
    /// Declared test name.
    pub test: String,
    /// Live tier: the connection (grant) id resolving the test's
    /// declared service. Absent for basic/fake runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_connection: Option<String>,
    /// Live-test fixture variables (`WEFT_NODE_TEST_*` only; a test reads
    /// them through `LiveRig::fixture`). Values a test cannot
    /// self-provision, like a chat id the tester's bot may message.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub fixtures: std::collections::BTreeMap<String, String>,
}

/// Payload for `TaskKind::Resync`: the drive of the project's resync plan
/// the task carries. Every write of the plan is fenced on it, so a task of
/// an earlier drive writes nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResyncPayload {
    pub driver: uuid::Uuid,
}

/// Payload for `TaskKind::TakeDownWait`: the activations a deactivate
/// took down, by owner, and their entry tokens as they stood before it (it
/// may have deleted their signals): what the wait counts at the workers'
/// door.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TakeDownWaitPayload {
    pub triggers: Vec<weft_core::activation::OwnerTriggers>,
    pub tokens: Vec<String>,
}

/// Payload for `TaskKind::RemovalWait`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemovalWaitPayload {
    /// Wait for every run of the project to end too, not only for their
    /// last calls: what erasing its history needs.
    pub every_run: bool,
}

/// Payload for `TaskKind::StopTagged`: "stop every live run of
/// `project_id` carrying `tag`, asked by run `by`". The broker
/// builds it when the worker calls `ctx.stop_tagged`, resolving
/// `before_seq` THEN (synchronously with the node's call), so a stop
/// that executes late can never reach a sibling that tagged itself
/// after the ask.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopTaggedPayload {
    pub project_id: uuid::Uuid,
    pub tag: String,
    /// The run that asked. Excluded from the targets under
    /// `StopSelf::Keep`, named as the canceller on every stopped run.
    pub by: weft_core::RunId,
    /// Only runs whose tag row has a sequence below this are
    /// stopped. `None` means every live run carrying the tag
    /// (the `Include` shape: no ordering, the whole batch goes).
    pub before_seq: Option<i64>,
    pub stop_self: weft_core::StopSelf,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stop-by-tag payload is the broker->dispatcher contract for a
    /// tag stop: both anchor shapes and both self choices round-trip.
    #[test]
    fn a_stop_by_tag_round_trips() {
        for (before_seq, stop_self) in
            [(Some(17), weft_core::StopSelf::Keep), (None, weft_core::StopSelf::Include)]
        {
            let payload = StopTaggedPayload {
                project_id: uuid::Uuid::nil(),
                tag: "user_7".into(),
                by: uuid::Uuid::from_u128(1),
                before_seq,
                stop_self,
            };
            let json = serde_json::to_value(&payload).unwrap();
            let back: StopTaggedPayload = serde_json::from_value(json.clone()).unwrap();
            assert_eq!(back.before_seq, before_seq, "{json}");
            assert_eq!(back.stop_self, stop_self, "{json}");
            assert_eq!(back.tag, "user_7");
        }
    }

    weft_core::wire_enum_roundtrip_tests!(TaskKind);

    /// The two kinds that were plain strings before they joined the enum
    /// keep the strings rows already carry.
    #[test]
    fn the_kinds_that_were_strings_keep_their_stored_tags() {
        assert_eq!(TaskKind::RunNodeTest.as_str(), "run_node_test");
        assert_eq!(TaskKind::Resync.as_str(), "resync");
    }

    /// A resync task's payload is the shape rows already queued carry.
    #[test]
    fn a_resync_payload_reads_the_stored_shape() {
        let driver = uuid::Uuid::from_u128(3);
        let stored = serde_json::json!({ "driver": driver });
        assert_eq!(serde_json::from_value::<ResyncPayload>(stored.clone()).unwrap(), ResyncPayload { driver });
        assert_eq!(serde_json::to_value(ResyncPayload { driver }).unwrap(), stored);
    }

    /// An answer a listener picked up reaches the dispatcher with its run
    /// and its value as the kind made them.
    #[test]
    fn an_answer_round_trips() {
        let payload = FireSignalPayload {
            token: "t".into(),
            run_id: uuid::Uuid::from_u128(2),
            value: serde_json::json!({ "approved": true }),
            held_by: None,
        };
        let json = serde_json::to_value(&payload).unwrap();
        assert!(json.get("held_by").is_none());
        let back: FireSignalPayload = serde_json::from_value(json).unwrap();
        assert_eq!((back.run_id, back.value), (payload.run_id, payload.value));
    }
}
