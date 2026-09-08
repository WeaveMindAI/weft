//! Append-only journal of execution events. Shared by the
//! dispatcher (folds + reads), the engine (writes lifecycle), and
//! the listener (writes on fire).
//!
//! The engine cannot depend on the dispatcher, but both need the
//! same `ExecEvent` schema and the same INSERT, so the type +
//! write live here.

pub mod events;
pub mod fold;
pub mod tags;
pub mod traits;
pub mod write;

pub use events::ExecEvent;
pub use fold::{fold_to_snapshot, FiringView, Fold, FoldEffects};

/// Decode one journal row, or the loud message every reader shares:
/// the color, the reason, and the recovery (`weft clean`). THE single
/// wording for an undecodable row, whoever reads it (the dispatcher's
/// strict and lossy reads, the engine's resume fold).
pub fn decode_event(color: weft_core::Color, payload: &str) -> Result<ExecEvent, String> {
    serde_json::from_str::<ExecEvent>(payload).map_err(|e| {
        format!(
            "exec_event row for color {color} did not decode ({e}); the journal \
             cannot be folded. `weft clean {color}` removes this color's rows."
        )
    })
}
pub use traits::{JournalClient, NoopJournal, PostgresJournalClient};
pub use write::{
    record_event, record_event_dedup, record_event_from_pod, record_event_in, RecordError,
};
