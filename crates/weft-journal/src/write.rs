//! Direct journal write API. Used by the broker (`PostgresJournalClient`,
//! writing on behalf of workers, whose engine never holds a DB
//! connection) and by the dispatcher's `PostgresJournal::record_event`,
//! so both sides go through one canonical INSERT.
//!
//! Schema invariant: the `exec_event` table layout matches the one
//! created by `weft-dispatcher::journal::postgres::GROUP`. Both
//! crates write the same row shape; only one side owns the
//! migration (the dispatcher, on startup), and the broker piggybacks
//! on it.
//!
//! Ordering invariant: a transaction that writes `exec_event` rows for
//! an execution takes that execution's lock ([`lock_execution_ids`]) BEFORE its first
//! write of any kind. Postgres hands a transaction its id (`xid`) at its
//! first write, and the dispatcher's journal bridge reads `exec_event`
//! in `(writer_xid, id)` order. With the lock first, two writers of one
//! execution get their xids in lock order, which is also their id order, so
//! the bridge applies an execution's rows in the order they were written. A
//! transaction that wrote something else before locking already holds
//! a lower xid, and its later row would be applied ahead of an earlier
//! one (a terminal before the event it closes reopens the run).
//! A transaction whose FIRST write is a `record_event_*` call is covered
//! by the lock that call takes; every other one calls [`lock_execution_ids`]
//! first. The list lives in `weft_dispatcher::settled`'s module doc.

use sqlx::postgres::PgPool;
use thiserror::Error;

use crate::events::ExecEvent;

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("serialize event: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("postgres: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("system clock before unix epoch")]
    BadClock,
}

/// Insert one event into `exec_event`. The table's own trigger
/// announces it on [`crate::EXEC_EVENT_CHANNEL`] when the write
/// commits, which is what wakes the dispatcher's event bridge and a
/// worker waiting on its run's journal.
///
/// `replica` is the writing worker's replica id, stamped on the row.
/// The fence is the broker's: it takes a worker's write only while that
/// replica owns the execution's claim, so a worker that lost its claim
/// cannot pollute the journal. Listener-side and dispatcher-side writes
/// pass `None`.
pub async fn record_event(pool: &PgPool, event: &ExecEvent) -> Result<(), RecordError> {
    record_event_inner(pool, event, None, None).await
}

/// Variant the worker's writes take (through the broker): stamps its
/// replica id on the row.
pub async fn record_event_from_replica(
    pool: &PgPool,
    event: &ExecEvent,
    replica: Option<&str>,
) -> Result<(), RecordError> {
    record_event_inner(pool, event, replica, None).await
}

/// Idempotent variant: caller provides a stable dedup key. A retry
/// of the same write collapses on the partial UNIQUE index. Used
/// by dispatcher tasks (e.g. route_entry) that may re-execute after
/// a crash and must not double-fire ExecutionStarted / NodeKicked.
pub async fn record_event_dedup(
    pool: &PgPool,
    event: &ExecEvent,
    dedup_key: &str,
) -> Result<(), RecordError> {
    record_event_inner(pool, event, None, Some(dedup_key)).await
}

/// Executor-generic variant so a caller can place the INSERT inside
/// its own transaction (the dispatcher pairs `ExecutionStarted` with
/// its `execution` seed atomically). Same row shape as every
/// other path.
pub async fn record_event_in<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
    event: &ExecEvent,
    replica: Option<&str>,
    dedup_key: Option<&str>,
) -> Result<(), RecordError> {
    record_event_inner(executor, event, replica, dedup_key).await
}

/// The advisory lock key of one execution's journal: the ONE definition,
/// shared by [`lock_execution_ids`] and the lock every `record_event_*` takes.
fn execution_id_lock_key(execution_id: weft_core::ExecutionId) -> String {
    format!("exec_event:{execution_id}")
}

/// Take the journal lock of every execution in `execution_ids`, held until the
/// transaction ends. Call it before the transaction's first write when
/// that write is not the `exec_event` insert itself (the module doc
/// says why). Executions are locked in sorted order, so two transactions
/// locking overlapping sets never deadlock on each other.
pub async fn lock_execution_ids(
    tx: &mut sqlx::PgConnection,
    execution_ids: &[weft_core::ExecutionId],
) -> Result<(), RecordError> {
    let mut sorted = execution_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    for execution_id in sorted {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(execution_id_lock_key(execution_id))
            .execute(&mut *tx)
            .await?;
    }
    Ok(())
}

async fn record_event_inner<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
    event: &ExecEvent,
    replica: Option<&str>,
    dedup_key: Option<&str>,
) -> Result<(), RecordError> {
    let payload = serde_json::to_string(event)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| RecordError::BadClock)?
        .as_secs() as i64;
    // The per-execution lock is taken BEFORE the row's id is drawn, and
    // held until the writing transaction ends, so the rows of one execution
    // are numbered and committed in the same order: once a reader sees
    // row N of an execution, every earlier row of that execution is visible too
    // (or was rolled back and never will be). That is what lets a reader
    // resume an execution's log from the last id it applied without ever
    // skipping a row (`JournalClient::rows_after`). Writers of different
    // executions never wait on each other. A transaction that writes anything
    // else first must call [`lock_execution_ids`] before that write (see the
    // module doc); taking the same lock again here is then free.
    sqlx::query(
        "WITH locked AS MATERIALIZED ( \
             SELECT pg_advisory_xact_lock(hashtextextended($7, 0)) \
         ) \
         INSERT INTO exec_event (execution_id, kind, payload_json, created_at, replica, dedup_key) \
         SELECT $1, $2, $3, $4, $5, $6 FROM locked \
         ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING",
    )
    .bind(event.execution_id().to_string())
    .bind(event.kind_str())
    .bind(&payload)
    .bind(now)
    .bind(replica)
    .bind(dedup_key)
    .bind(execution_id_lock_key(event.execution_id()))
    .execute(executor)
    .await?;
    Ok(())
}
