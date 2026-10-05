//! A run's journal rows as the table stores them, and the one read of
//! them. The journal itself is `weft_journal`'s; this crate reads its rows
//! too, inside a worker's claim (`crate::tasks::claim_execution`), so the
//! shape and the read live here, under both.

use serde::{Deserialize, Serialize};

/// One journal row as stored: its place in the table and its payload,
/// undecoded (`weft_journal::decode_event` decodes it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawJournalRow {
    pub id: i64,
    pub payload: String,
}

/// The query for an execution's journal rows after a row id, in the order
/// they were written: `execution` and `after` are SQL expressions (a bind
/// parameter, a column). Every read of a run's rows is this one, so none
/// drifts from the others.
// SYNC: exec_event's (execution_id, id, payload_json) <-> crates/weft-dispatcher/src/journal/postgres.rs (the journal's table), crates/weft-task-store/tests/support/mod.rs (its stand-in)
pub fn rows_after_sql(execution: &str, after: &str) -> String {
    format!("SELECT id, payload_json FROM exec_event WHERE execution_id = {execution} AND id > {after} ORDER BY id ASC")
}
