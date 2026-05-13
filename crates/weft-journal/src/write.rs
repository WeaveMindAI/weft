//! Direct journal write API. Used by the engine and the listener
//! once they hold their own DB connections; also used by the
//! dispatcher's `PostgresJournal::record_event` so both sides go
//! through one canonical INSERT.
//!
//! Schema invariant: the `exec_event` table layout matches the one
//! created by `weft-dispatcher::journal::postgres::migrate`. Both
//! crates write the same row shape; only one side owns the
//! migration (the dispatcher, on startup), and the engine + listener
//! piggyback on it.

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

/// Insert one event into `exec_event`. Pure data write; no NOTIFY.
/// Consumers (workers, dispatcher event bridge) read by polling.
///
/// `pod_name` is the writing worker's k8s Pod name. A fencing trigger
/// on `exec_event` rejects writes whose Pod row is not in
/// `{spawning, alive}`, so a stale Pod that survived a respawn or
/// drain transition can't pollute the journal. Listener-side and
/// dispatcher-side writes pass `None`.
pub async fn record_event(pool: &PgPool, event: &ExecEvent) -> Result<(), RecordError> {
    record_event_inner(pool, event, None, None).await
}

/// Variant the worker uses: pass its k8s pod_name so the fencing
/// trigger can validate the Pod is still alive.
pub async fn record_event_from_pod(
    pool: &PgPool,
    event: &ExecEvent,
    pod_name: Option<&str>,
) -> Result<(), RecordError> {
    record_event_inner(pool, event, pod_name, None).await
}

/// Idempotent variant: caller provides a stable dedup key. A retry
/// of the same write collapses on the partial UNIQUE index. Used
/// by dispatcher tasks (e.g. route_entry) that may re-execute after
/// a crash and must not double-fire ExecutionStarted / PulseSeeded.
pub async fn record_event_dedup(
    pool: &PgPool,
    event: &ExecEvent,
    dedup_key: &str,
) -> Result<(), RecordError> {
    record_event_inner(pool, event, None, Some(dedup_key)).await
}

async fn record_event_inner(
    pool: &PgPool,
    event: &ExecEvent,
    pod_name: Option<&str>,
    dedup_key: Option<&str>,
) -> Result<(), RecordError> {
    let payload = serde_json::to_string(event)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| RecordError::BadClock)?
        .as_secs() as i64;
    sqlx::query(
        "INSERT INTO exec_event (color, kind, payload_json, created_at, pod_name, dedup_key) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING",
    )
    .bind(event.color().to_string())
    .bind(event.kind_str())
    .bind(&payload)
    .bind(now)
    .bind(pod_name)
    .bind(dedup_key)
    .execute(pool)
    .await?;
    Ok(())
}
