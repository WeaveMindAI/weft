//! Append-only journal of execution events. Shared by the
//! dispatcher (folds + reads), the engine (writes lifecycle), and
//! the listener (writes on fire).
//!
//! The engine cannot depend on the dispatcher, but both need the
//! same `ExecEvent` schema and the same INSERT, so the type +
//! write live here.

pub mod events;
pub mod fold;
pub mod seed;
pub mod tags;
pub mod traits;
pub mod write;

pub use events::{ExecEvent, Seed};

/// The channel every journal row notifies on when it commits, with its
/// color as the payload, from the `exec_event_notify_on_insert` trigger
/// in the dispatcher's journal schema group.
pub const EXEC_EVENT_CHANNEL: &str = "weft_exec_event";
pub use fold::{fold_to_snapshot, FiringView, Fold, FoldEffects};
pub use seed::{fold_seeded, seed_chain, LiveFold, SeedChain};

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
pub use traits::{JournalClient, JournalRow, NoopJournal, PostgresJournalClient, RawJournalRow};
pub use write::{
    lock_colors, record_event, record_event_dedup, record_event_from_pod, record_event_in, RecordError,
};
