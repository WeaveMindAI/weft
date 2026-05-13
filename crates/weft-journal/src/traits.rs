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

    /// True iff a terminal event already exists for `color`. Used
    /// by the worker before writing its own terminal so the
    /// dispatcher's cancel path doesn't bridge double.
    async fn has_terminal_event(&self, color: weft_core::Color) -> anyhow::Result<bool>;
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
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT payload_json FROM exec_event WHERE color = $1 ORDER BY id ASC",
        )
        .bind(color.to_string())
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let payload: String = row.try_get("payload_json")?;
            // One malformed row must NOT brick the engine's boot
            // fold: that would lock every execution sharing this
            // color out of resuming forever. Log loud and skip.
            // Matches the dispatcher's `events_log` behavior.
            match serde_json::from_str::<ExecEvent>(&payload) {
                Ok(ev) => out.push(ev),
                Err(e) => tracing::error!(
                    target: "weft_journal::traits",
                    %color, error = %e,
                    "skip malformed event payload during fold"
                ),
            }
        }
        Ok(out)
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
