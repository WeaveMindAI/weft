//! Append-only journal of execution events. Shared by the
//! dispatcher (folds + reads), the engine (writes lifecycle), and
//! the listener (writes on fire).
//!
//! The engine cannot depend on the dispatcher, but both need the
//! same `ExecEvent` schema and the same INSERT, so the type +
//! write live here.

pub mod events;
pub mod traits;
pub mod write;

pub use events::{fold_to_snapshot, ExecEvent};
pub use traits::{JournalClient, PostgresJournalClient};
pub use write::{
    record_event, record_event_dedup, record_event_from_pod, record_event_in, RecordError,
};
