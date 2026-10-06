//! Append-only journal of execution events. Shared by the
//! dispatcher (folds, reads, and writes what it decides itself), the
//! engine (writes lifecycle and folds on resume, both through the
//! broker), and the broker (writes on the engine's behalf). The
//! listener never touches it: a fire it holds reaches the journal
//! through a `fire_signal` task the dispatcher runs.
//!
//! The engine cannot depend on the dispatcher, but both need the
//! same `ExecEvent` schema and the same INSERT, so the type +
//! write live here.

pub mod events;
pub mod fold;
pub mod seed;
pub mod tags;
pub mod traits;
pub mod unrecorded;
pub mod write;

pub use events::{ExecEvent, Seed, EXECUTION_TERMINAL_KINDS_SQL, RUN_PARKED_SQL};

/// The channel a write of journal rows is announced on, once per execution
/// it wrote to, with the execution as the payload: from the
/// `exec_event_notify_on_insert` trigger in the dispatcher's journal schema
/// group, through the announcement outbox (`weft_task_store::announce`),
/// sent once the write commits and its writer pokes the flusher.
// SYNC: EXEC_EVENT_CHANNEL <-> 'weft_exec_event' in exec_event_notify (crates/weft-dispatcher/src/journal/postgres.rs)
pub const EXEC_EVENT_CHANNEL: &str = "weft_exec_event";
pub use fold::{fold_to_snapshot, FiringView, Fold, FoldEffects};
pub use seed::{fold_seeded, seed_chain, LiveFold, SeedChain};

/// Decode one journal row, or the loud message every reader shares:
/// the execution, the reason, and the recovery (`weft clean`). THE single
/// wording for an undecodable row, whoever reads it (the dispatcher's
/// strict and lossy reads, the engine's resume fold).
pub fn decode_event(execution_id: weft_core::ExecutionId, payload: &str) -> Result<ExecEvent, String> {
    serde_json::from_str::<ExecEvent>(payload).map_err(|e| {
        format!(
            "exec_event row for execution {execution_id} did not decode ({e}); the journal \
             cannot be folded. `weft clean {execution_id}` removes this execution's rows."
        )
    })
}
pub use traits::{decode_rows, JournalClient, JournalRow, NoopJournal, PostgresJournalClient, RawJournalRow};
pub use unrecorded::UnrecordedJournal;
pub use write::{
    lock_execution_ids, record_event_in, record_events, RecordError,
};
