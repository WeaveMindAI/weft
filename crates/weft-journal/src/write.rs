//! Direct journal write API. Used by the broker (`PostgresJournalClient`,
//! writing on behalf of workers, whose engine never holds a DB
//! connection) and by the dispatcher's `PostgresJournal`, so both sides
//! go through one canonical INSERT.
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
//! A transaction whose FIRST write is a write from this module is covered
//! by the lock that write takes; every other one calls [`lock_execution_ids`]
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
    #[error("one journal write mixed rows of execution {first} and {other}")]
    MixedExecutions { first: weft_core::ExecutionId, other: weft_core::ExecutionId },
}

fn unix_now() -> Result<i64, RecordError> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| RecordError::BadClock)?
        .as_secs() as i64)
}

/// Insert `events`, all of execution `events[0]`'s, in order, in ONE
/// statement: one round trip whatever their number. The table's own
/// trigger announces the rows on [`crate::EXEC_EVENT_CHANNEL`] when the
/// write commits, which is what wakes the dispatcher's event bridge and a
/// worker waiting on its run's journal.
///
/// `replica` is the writing worker's replica id, stamped on every row;
/// listener-side and dispatcher-side writes pass `None`. `owner`, when
/// given, fences the write in the same statement: the rows go in only
/// while that replica owns the execution's claim
/// (`execution.owner_replica`), which is how the broker keeps a worker
/// that lost its claim out of the journal. Returns how many rows went in,
/// so a fenced write that wrote nothing tells the caller the replica no
/// longer owns the run (or never did).
pub async fn record_events(
    pool: &PgPool,
    events: &[ExecEvent],
    replica: Option<&str>,
    owner: Option<&str>,
) -> Result<u64, RecordError> {
    insert(pool, events, replica, owner, None).await
}

/// One event, inside the caller's own transaction (the dispatcher pairs
/// `ExecutionStarted` with its `execution` seed atomically). `dedup_key`
/// makes it idempotent: a retry of the same write collapses on the
/// partial UNIQUE index, so a dispatcher task that re-executes after a
/// crash never double-fires `ExecutionStarted` / `NodeKicked`.
pub async fn record_event_in<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
    event: &ExecEvent,
    replica: Option<&str>,
    dedup_key: Option<&str>,
) -> Result<(), RecordError> {
    insert(executor, std::slice::from_ref(event), replica, None, dedup_key).await?;
    Ok(())
}

/// THE insert every journal write makes. The per-execution lock is taken
/// BEFORE any row's id is drawn, and held until the writing transaction
/// ends, so the rows of one execution are numbered and committed in the
/// same order: once a reader sees row N of an execution, every earlier row
/// of that execution is visible too (or was rolled back and never will
/// be). That is what lets a reader resume an execution's log from the last
/// id it applied without ever skipping a row (`JournalClient::rows_after`).
/// Writers of different executions never wait on each other. A
/// transaction that writes anything else first must call
/// [`lock_execution_ids`] before that write (see the module doc); taking
/// the same lock again here is then free. The rows go in the order given
/// (`ORDER BY` the position), so their ids follow it. `dedup_key` applies
/// to a single-row write only.
async fn insert<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
    events: &[ExecEvent],
    replica: Option<&str>,
    owner: Option<&str>,
    dedup_key: Option<&str>,
) -> Result<u64, RecordError> {
    let Some(first) = events.first() else { return Ok(0) };
    let execution_id = first.execution_id();
    if let Some(stray) = events.iter().find(|e| e.execution_id() != execution_id) {
        return Err(RecordError::MixedExecutions { first: execution_id, other: stray.execution_id() });
    }
    debug_assert!(dedup_key.is_none() || events.len() == 1, "a dedup key names one row");
    let kinds: Vec<&str> = events.iter().map(|e| e.kind_str()).collect();
    let payloads: Vec<String> = events.iter().map(serde_json::to_string).collect::<Result<_, _>>()?;
    let now = unix_now()?;
    let written = sqlx::query(
        "WITH locked AS MATERIALIZED ( \
             SELECT pg_advisory_xact_lock(hashtextextended($6, 0)) \
         ) \
         INSERT INTO exec_event (execution_id, kind, payload_json, created_at, replica, dedup_key) \
         SELECT $1, e.kind, e.payload, $4, $5, $8 \
         FROM locked, unnest($2::text[], $3::text[]) WITH ORDINALITY AS e(kind, payload, n) \
         WHERE $7::text IS NULL \
            OR EXISTS (SELECT 1 FROM execution x WHERE x.execution_id = $1 AND x.owner_replica = $7) \
         ORDER BY e.n \
         ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING",
    )
    .bind(execution_id.to_string())
    .bind(&kinds)
    .bind(&payloads)
    .bind(now)
    .bind(replica)
    .bind(execution_id_lock_key(execution_id))
    .bind(owner)
    .bind(dedup_key)
    .execute(executor)
    .await?
    .rows_affected();
    Ok(written)
}

/// The advisory lock key of one execution's journal: the ONE definition,
/// shared by [`lock_execution_ids`] and the lock every write here takes.
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
