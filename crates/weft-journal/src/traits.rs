//! Worker-facing journal surface. Two implementations:
//!   - `PostgresJournalClient` (in this crate): direct DB. Used by
//!     the broker, after its scope check.
//!   - `BrokerJournalClient` (in `weft-broker-client`): HTTP through
//!     the broker. Used by workers and listeners.
//!
//! The trait carries only the operations user-namespace pods need.
//! It deliberately omits any signal/admin surface (the dispatcher's
//! `Journal` trait in `weft-dispatcher/src/journal/mod.rs` is a
//! superset for dispatcher-internal use).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use weft_task_store::pg_signal::PgSignalWatch;

use crate::events::ExecEvent;

/// One journal row as stored: its place in the table and its payload,
/// undecoded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawJournalRow {
    pub id: i64,
    pub payload: String,
}

/// One journal row, decoded.
#[derive(Debug, Clone)]
pub struct JournalRow {
    pub id: i64,
    pub event: ExecEvent,
}

/// Read + write surface used by the worker (engine) and the listener
/// for journal operations. `pod_name` is the worker's k8s Pod name,
/// stamped on every write so the fencing trigger can reject events
/// from a Pod whose `worker_pod` row is no longer alive. Listener-side
/// callers pass `None`.
#[async_trait]
pub trait JournalClient: Send + Sync {
    /// Insert one event. Errors propagate; the engine's wrapper
    /// converts these into structured warnings.
    async fn record_event(
        &self,
        event: &ExecEvent,
        pod_name: Option<&str>,
    ) -> anyhow::Result<()>;

    /// The rows of `color` after `after_id`, in order, as RAW payload
    /// strings, holding up to `wait` for at least one to exist (a zero
    /// `wait` answers at once; empty when none came). A color's rows
    /// are numbered and committed in one order (see `write`), so a
    /// reader that resumes from the last id it applied never passes a
    /// row. Raw for a FERRY (the broker's handler): a hop that decoded
    /// into its own `ExecEvent` and re-encoded would silently strip any
    /// field its build predates, so a row that only passes through must
    /// pass through byte-faithful.
    async fn raw_rows_after(
        &self,
        color: weft_core::Color,
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
        color: weft_core::Color,
        after_id: i64,
        wait: Duration,
    ) -> anyhow::Result<Vec<JournalRow>> {
        self.raw_rows_after(color, after_id, wait)
            .await?
            .into_iter()
            .map(|row| {
                let event = crate::decode_event(color, &row.payload).map_err(anyhow::Error::msg)?;
                Ok(JournalRow { id: row.id, event })
            })
            .collect()
    }

    /// Every event of one execution, in order, as it stands now. For a
    /// log that no longer changes (a seed ancestor, which is terminal).
    async fn events_for_color(&self, color: weft_core::Color) -> anyhow::Result<Vec<ExecEvent>> {
        Ok(self.rows_after(color, 0, Duration::ZERO).await?.into_iter().map(|row| row.event).collect())
    }

    /// True iff a terminal event already exists for `color`. Used
    /// by the worker before writing its own terminal so the
    /// dispatcher's cancel path doesn't bridge double.
    async fn has_terminal_event(&self, color: weft_core::Color) -> anyhow::Result<bool>;
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
        _pod_name: Option<&str>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Nothing is ever written, so nothing ever comes: the hold runs out.
    async fn raw_rows_after(
        &self,
        _color: weft_core::Color,
        _after_id: i64,
        wait: Duration,
    ) -> anyhow::Result<Vec<RawJournalRow>> {
        tokio::time::sleep(wait).await;
        Ok(Vec::new())
    }

    async fn has_terminal_event(&self, _color: weft_core::Color) -> anyhow::Result<bool> {
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
        pod_name: Option<&str>,
    ) -> anyhow::Result<()> {
        crate::write::record_event_from_pod(&self.pool, event, pod_name)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn raw_rows_after(
        &self,
        color: weft_core::Color,
        after_id: i64,
        wait: Duration,
    ) -> anyhow::Result<Vec<RawJournalRow>> {
        let deadline = tokio::time::Instant::now() + wait;
        let color = color.to_string();
        // Subscribed before the first read, so a row committed between
        // an empty read and the wait still ends the wait.
        let mut heard = self.signals.subscribe();
        loop {
            let rows: Vec<(i64, String)> = sqlx::query_as(
                "SELECT id, payload_json FROM exec_event WHERE color = $1 AND id > $2 ORDER BY id ASC",
            )
            .bind(&color)
            .bind(after_id)
            .fetch_all(&self.pool)
            .await?;
            if !rows.is_empty()
                || !heard.woken_before(deadline, |c, p| c == crate::EXEC_EVENT_CHANNEL && p == color).await?
            {
                return Ok(rows.into_iter().map(|(id, payload)| RawJournalRow { id, payload }).collect());
            }
        }
    }

    async fn has_terminal_event(&self, color: weft_core::Color) -> anyhow::Result<bool> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT kind FROM exec_event \
             WHERE color = $1 \
               AND kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
             LIMIT 1",
        )
        .bind(color.to_string())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }
}
