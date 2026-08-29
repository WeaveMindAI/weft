//! Worker-facing journal surface. Two implementations:
//!   - `PostgresJournalClient` (in this crate): direct DB. Used by
//!     the dispatcher.
//!   - `BrokerJournalClient` (in `weft-broker-client`): HTTP through
//!     the broker. Used by workers and listeners.
//!
//! The trait carries only the operations user-namespace pods need.
//! It deliberately omits any signal/admin surface (the dispatcher's
//! `Journal` trait in `weft-dispatcher/src/journal/mod.rs` is a
//! superset for dispatcher-internal use).

use async_trait::async_trait;

use crate::events::ExecEvent;

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

    /// All events for a single execution, ordered. Used for the
    /// boot fold and re-fold-after-stall.
    async fn events_for_color(&self, color: weft_core::Color) -> anyhow::Result<Vec<ExecEvent>>;

    /// The same rows as RAW payload strings, undecoded. For a FERRY (the
    /// broker's journal-fetch handler): a hop that decoded into its own
    /// `ExecEvent` and re-encoded would silently strip any field its
    /// build predates, so a row that only passes through must pass
    /// through byte-faithful. Consumers of the rows decode them
    /// themselves, loudly.
    async fn raw_events_for_color(
        &self,
        color: weft_core::Color,
    ) -> anyhow::Result<Vec<String>>;

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

    async fn events_for_color(&self, _color: weft_core::Color) -> anyhow::Result<Vec<ExecEvent>> {
        Ok(Vec::new())
    }

    async fn raw_events_for_color(
        &self,
        _color: weft_core::Color,
    ) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn has_terminal_event(&self, _color: weft_core::Color) -> anyhow::Result<bool> {
        Ok(false)
    }
}

/// Direct-DB implementation. Used by the dispatcher and by the
/// broker (the broker calls into this after its scope check).
pub struct PostgresJournalClient {
    pool: sqlx::postgres::PgPool,
}

impl PostgresJournalClient {
    pub fn new(pool: sqlx::postgres::PgPool) -> Self {
        Self { pool }
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

    async fn events_for_color(&self, color: weft_core::Color) -> anyhow::Result<Vec<ExecEvent>> {
        let payloads = self.raw_events_for_color(color).await?;
        let mut out = Vec::with_capacity(payloads.len());
        for payload in payloads {
            // This read feeds the engine's resume fold, which rebuilds
            // the execution's state. A fold over a partial event list
            // rebuilds a state that never existed (skips un-happen,
            // closures never cascade), so a row that no longer decodes
            // fails the read outright: the execution fails loudly and
            // `weft clean` removes it, instead of resuming wrong.
            out.push(crate::decode_event(color, &payload).map_err(anyhow::Error::msg)?);
        }
        Ok(out)
    }

    async fn raw_events_for_color(
        &self,
        color: weft_core::Color,
    ) -> anyhow::Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event WHERE color = $1 ORDER BY id ASC",
        )
        .bind(color.to_string())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(p,)| p).collect())
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
