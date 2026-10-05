//! Worker-facing journal surface. Two implementations:
//!   - `PostgresJournalClient` (in this crate): direct DB. Used by
//!     the broker, after its scope check.
//!   - `BrokerJournalClient` (in `weft-broker-client`): HTTP through
//!     the broker. Used by workers and listeners.
//!
//! The trait carries only the operations user-namespace processes need.
//! It deliberately omits any signal/admin surface (the dispatcher's
//! `Journal` trait in `weft-dispatcher/src/journal/mod.rs` is a
//! superset for dispatcher-internal use).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use weft_task_store::pg_signal::PgSignalWatch;

use crate::events::ExecEvent;

pub use weft_task_store::journal_rows::RawJournalRow;

/// `rows` of `execution_id`, decoded, in order. One row that no longer
/// decodes fails them all (see [`JournalClient::rows_after`]).
pub fn decode_rows(execution_id: weft_core::ExecutionId, rows: Vec<RawJournalRow>) -> anyhow::Result<Vec<JournalRow>> {
    rows.into_iter()
        .map(|row| {
            let event = crate::decode_event(execution_id, &row.payload).map_err(anyhow::Error::msg)?;
            Ok(JournalRow { id: row.id, event })
        })
        .collect()
}

/// One journal row, decoded.
#[derive(Debug, Clone)]
pub struct JournalRow {
    pub id: i64,
    pub event: ExecEvent,
}

/// Read + write surface used by the worker (engine) and the listener
/// for journal operations. `replica` is the worker's replica id,
/// stamped on every write; the broker takes a write only from the
/// replica that owns the execution's claim. Listener-side callers pass
/// `None`.
#[async_trait]
pub trait JournalClient: Send + Sync {
    /// Insert one event. Errors propagate; the engine's wrapper
    /// converts these into structured warnings.
    async fn record_event(
        &self,
        event: &ExecEvent,
        replica: Option<&str>,
    ) -> anyhow::Result<()>;

    /// Insert `events`, all of one execution, in order. A journal that
    /// reaches the database over the network writes them in one go
    /// (one round trip instead of one per event); the default writes
    /// them one by one.
    async fn record_events(&self, events: &[ExecEvent], replica: Option<&str>) -> anyhow::Result<()> {
        for event in events {
            self.record_event(event, replica).await?;
        }
        Ok(())
    }

    /// How a write of `events` goes out: the parts, in order, each sent in
    /// one request, so a writer that sends a write again can send only the
    /// part that failed. One part by default.
    fn parts<'e>(&self, events: &'e [ExecEvent]) -> anyhow::Result<Vec<&'e [ExecEvent]>> {
        Ok(vec![events])
    }

    /// Whether a failed write of one part never reached the journal (the
    /// connection itself could not be made), so sending it again cannot
    /// record its rows twice. Never, by default: a write that may have
    /// landed is not sent again.
    fn never_reached(&self, _error: &anyhow::Error) -> bool {
        false
    }

    /// The rows of `execution_id` after `after_id`, in order, as RAW payload
    /// strings, holding up to `wait` for at least one to exist (a zero
    /// `wait` answers at once; empty when none came). An execution's rows
    /// are numbered and committed in one order (see `write`), so a
    /// reader that resumes from the last id it applied never passes a
    /// row. Raw for a FERRY (the broker's handler): a hop that decoded
    /// into its own `ExecEvent` and re-encoded would silently strip any
    /// field its build predates, so a row that only passes through must
    /// pass through byte-faithful.
    async fn raw_rows_after(
        &self,
        execution_id: weft_core::ExecutionId,
        after_id: i64,
        wait: Duration,
    ) -> anyhow::Result<Vec<RawJournalRow>>;

    /// [`Self::raw_rows_after`], decoded. A row that no longer decodes
    /// fails the read outright: this read feeds the engine's fold, and a
    /// fold over a partial event list rebuilds a state that never
    /// existed (skips un-happen, closures never cascade), so the
    /// execution fails loudly and `weft clean` removes it, instead of
    /// resuming wrong.
    async fn rows_after(
        &self,
        execution_id: weft_core::ExecutionId,
        after_id: i64,
        wait: Duration,
    ) -> anyhow::Result<Vec<JournalRow>> {
        decode_rows(execution_id, self.raw_rows_after(execution_id, after_id, wait).await?)
    }

    /// Every event of one execution, in order, as it stands now. For a
    /// log that no longer changes (a seed ancestor, which is terminal).
    async fn events_for_execution_id(&self, execution_id: weft_core::ExecutionId) -> anyhow::Result<Vec<ExecEvent>> {
        Ok(self.rows_after(execution_id, 0, Duration::ZERO).await?.into_iter().map(|row| row.event).collect())
    }

    /// True iff a terminal event already exists for `execution_id`. Used
    /// by the worker before writing its own terminal so the
    /// dispatcher's cancel path doesn't bridge double.
    async fn has_terminal_event(&self, execution_id: weft_core::ExecutionId) -> anyhow::Result<bool>;

    /// An unrecorded run that failed: write its whole record (every
    /// event, in order, terminal included) and make it an ordinary
    /// recorded run, in one transaction, so it lists and inspects like
    /// any other. Refused unless the execution is still unrecorded. Only a
    /// journal that reaches the database takes it; every other one
    /// refuses loudly.
    async fn record_retroactively(&self, events: &[ExecEvent], replica: Option<&str>) -> anyhow::Result<()> {
        let _ = (events, replica);
        anyhow::bail!("this journal cannot record an unrecorded run afterwards")
    }

    /// An unrecorded run that ended without failing: drop its execution row
    /// when nothing of it reached the journal (the costs it reported
    /// keep it, since they are addressed by execution), and release its run
    /// files. Only a journal that reaches the database takes it.
    async fn forget_unrecorded(&self, execution_id: weft_core::ExecutionId, replica: Option<&str>) -> anyhow::Result<()> {
        let _ = (execution_id, replica);
        anyhow::bail!("this journal cannot forget an unrecorded run")
    }
}

/// The no-write implementation: every write vanishes, every read
/// answers empty. For runtimes that drive a node body OUTSIDE an
/// execution (a node self-test run has no journal to fold and must
/// not fabricate execution rows), and for tests exercising code that
/// only incidentally holds a journal client.
pub struct NoopJournal;

#[async_trait]
impl JournalClient for NoopJournal {
    async fn record_event(
        &self,
        _event: &ExecEvent,
        _replica: Option<&str>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Nothing is ever written, so nothing ever comes: the hold runs out.
    async fn raw_rows_after(
        &self,
        _execution_id: weft_core::ExecutionId,
        _after_id: i64,
        wait: Duration,
    ) -> anyhow::Result<Vec<RawJournalRow>> {
        tokio::time::sleep(wait).await;
        Ok(Vec::new())
    }

    async fn has_terminal_event(&self, _execution_id: weft_core::ExecutionId) -> anyhow::Result<bool> {
        Ok(false)
    }
}

/// Direct-DB implementation, used by the broker (after its scope check).
/// Holds on the process's signal watch, which must listen on
/// [`crate::EXEC_EVENT_CHANNEL`].
pub struct PostgresJournalClient {
    pool: sqlx::postgres::PgPool,
    signals: Arc<PgSignalWatch>,
}

impl PostgresJournalClient {
    pub fn new(pool: sqlx::postgres::PgPool, signals: Arc<PgSignalWatch>) -> anyhow::Result<Self> {
        signals.require(crate::EXEC_EVENT_CHANNEL)?;
        Ok(Self { pool, signals })
    }
}

#[async_trait]
impl JournalClient for PostgresJournalClient {
    async fn record_event(
        &self,
        event: &ExecEvent,
        replica: Option<&str>,
    ) -> anyhow::Result<()> {
        self.record_events(std::slice::from_ref(event), replica).await
    }

    async fn record_events(&self, events: &[ExecEvent], replica: Option<&str>) -> anyhow::Result<()> {
        crate::write::record_events(&self.pool, events, replica, None)
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn record_retroactively(&self, events: &[ExecEvent], replica: Option<&str>) -> anyhow::Result<()> {
        crate::unrecorded::record_retroactively(&self.pool, events, replica).await
    }

    async fn forget_unrecorded(&self, execution_id: weft_core::ExecutionId, _replica: Option<&str>) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::unrecorded::forget_in(&mut tx, execution_id).await?;
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        Ok(())
    }

    async fn raw_rows_after(
        &self,
        execution_id: weft_core::ExecutionId,
        after_id: i64,
        wait: Duration,
    ) -> anyhow::Result<Vec<RawJournalRow>> {
        let deadline = tokio::time::Instant::now() + wait;
        let execution_id = execution_id.to_string();
        // Subscribed before the first read, so a row committed between
        // an empty read and the wait still ends the wait.
        let mut heard = self.signals.subscribe();
        loop {
            let rows: Vec<(i64, String)> = sqlx::query_as(&weft_task_store::journal_rows::rows_after_sql("$1", "$2"))
            .bind(&execution_id)
            .bind(after_id)
            .fetch_all(&self.pool)
            .await?;
            if !rows.is_empty()
                || !heard.woken_before(deadline, |c, p| c == crate::EXEC_EVENT_CHANNEL && p == execution_id).await?
            {
                return Ok(rows.into_iter().map(|(id, payload)| RawJournalRow { id, payload }).collect());
            }
        }
    }

    async fn has_terminal_event(&self, execution_id: weft_core::ExecutionId) -> anyhow::Result<bool> {
        let row: Option<(String,)> = sqlx::query_as(concat!(
            "SELECT kind FROM exec_event \
             WHERE execution_id = $1 \
               AND kind IN ",
            crate::execution_terminal_kinds_sql!(),
            " LIMIT 1",
        ))
        .bind(execution_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }
}
