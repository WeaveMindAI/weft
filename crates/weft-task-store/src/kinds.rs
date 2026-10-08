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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// Register a wake signal with the listener and return its mint info
    /// to the worker that asked.
    RegisterSignal,
    /// Withdraw a wait a run gave up (`WithdrawSignalPayload`): its signal
    /// goes, and the listener lets go of it, so nobody is still asked to
    /// answer it. Producer = worker (via broker).
    WithdrawSignal,
    /// An answer to a waiting run that a listener picked up (a timer's
    /// tick, an event on a connection it holds), already processed by its
    /// kind. Producer = listener (via broker), which never opens an HTTP
    /// connection to the dispatcher, so the trust seam stays at the
    /// broker.
    FireSignal,
    /// Stop every live execution of a project carrying a tag, on behalf
    /// of one of its executions (`ctx.stop_tagged`). Producer = the
    /// broker's `/v1/execution/stop_tagged` handler, which resolves the
    /// ordering anchor at enqueue time; consumer = the dispatcher's
    /// executor, which cancels each match through the one cancel path.
    /// Rides the task table so the stop survives the asking worker dying
    /// right after it asked.
    StopTagged,
    /// One call a program makes on its own project
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
            Self::RegisterSignal => "register_signal",
            Self::WithdrawSignal => "withdraw_signal",
            Self::FireSignal => "fire_signal",
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

/// Payload for `TaskKind::FireSignal`: an answer to a waiting run a
/// listener picked up, already processed by its kind (`/process` ran in
/// the listener), so the dispatcher hands it to its run without asking
/// the listener again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FireSignalPayload {
    pub token: String,
    /// The run the answer resolves.
    pub execution_id: weft_core::ExecutionId,
    /// The answer, as the kind made it of the event.
    pub value: serde_json::Value,
    /// The holder a held connection picked the answer up under. The
    /// broker takes it only while the signal is still held under that
    /// name, so a copy that lost the row delivers nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_by: Option<String>,
}

/// Payload for `TaskKind::WithdrawSignal`: the wait `token` of run
/// `execution_id`, given up. The broker takes it only when the wait is
/// that run's.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawSignalPayload {
    pub execution_id: weft_core::ExecutionId,
    pub token: String,
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
    pub by: weft_core::ExecutionId,
    /// Only executions whose tag row has a sequence below this are
    /// stopped. `None` means every live execution carrying the tag
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
        assert_eq!(TaskKind::StopTagged.as_str(), "stop_tagged");
        assert_eq!(TaskKind::WithdrawSignal.as_str(), "withdraw_signal");
        assert_eq!(TaskKind::ProgramCall.as_str(), "program_call");
    }

    /// An answer a listener picked up reaches the dispatcher with its run
    /// and its value as the kind made them.
    #[test]
    fn an_answer_round_trips() {
        let payload = FireSignalPayload {
            token: "t".into(),
            execution_id: uuid::Uuid::from_u128(2),
            value: serde_json::json!({ "approved": true }),
            held_by: None,
        };
        let json = serde_json::to_value(&payload).unwrap();
        assert!(json.get("held_by").is_none());
        let back: FireSignalPayload = serde_json::from_value(json).unwrap();
        assert_eq!((back.execution_id, back.value), (payload.execution_id, payload.value));
    }
}
