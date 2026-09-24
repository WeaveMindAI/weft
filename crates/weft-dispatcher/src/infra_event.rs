//! `infra_event` table. The infra-supervisor writes; the dispatcher
//! polls (via `infra_event_bridge`) and fans events out over SSE.
//!
//! Distinct from the `exec_event` journal: those are durable
//! execution-graph events. `infra_event` is for control-plane
//! state changes about the infra itself (a node went flaky, the
//! supervisor finished a terminate, etc).

use anyhow::Result;
use serde_json::Value;
use sqlx::postgres::PgPool;

// One source of truth for the `(kind, payload)` wire contract:
// `weft-broker-client::protocol::{InfraEventKind, InfraEvent}`.
// Both the supervisor (writer) and the dispatcher (reader) import
// from there; a rename or schema drift becomes a compile error at
// the construction site.
pub use weft_broker_client::protocol::{InfraEvent, InfraEventKind};

#[derive(Debug, Clone)]
pub struct InfraEventRow {
    pub id: i64,
    pub tenant_id: String,
    pub project_id: uuid::Uuid,
    /// None for project-wide events (e.g. all infra terminated).
    pub node_id: Option<String>,
    /// Typed payload. Constructed by the supervisor; deserialized
    /// here on read. A row whose `kind` column doesn't parse, or
    /// whose payload doesn't deserialize for its kind, is a writer
    /// bug; the bridge fails loud rather than skip.
    pub event: InfraEvent,
    pub at_unix: i64,
}

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "infra_event",
    tables: &["infra_event"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS infra_event (
            id          BIGSERIAL PRIMARY KEY,
            tenant_id   TEXT NOT NULL,
            project_id  UUID NOT NULL,
            node_id     TEXT,
            kind        TEXT NOT NULL,
            payload     JSONB NOT NULL,
            at_unix     BIGINT NOT NULL,
            -- The transaction that wrote the row, the order the
            -- bridge's cursor reads in (`crate::settled`).
            writer_xid  XID8 NOT NULL DEFAULT pg_current_xact_id()
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_event_chrono ON infra_event(id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_event_settled ON infra_event(writer_xid, id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_event_project ON infra_event(project_id)"#,
        // Wake the dispatcher's bridge on every event, whoever wrote it
        // (the broker for a supervisor, or this crate's own `insert`),
        // when the write commits.
        // SYNC: 'weft_infra_event' <-> crate::infra_event_bridge::INFRA_EVENT_CHANNEL
        r#"CREATE OR REPLACE FUNCTION infra_event_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_event', NEW.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS infra_event_notify_on_insert ON infra_event"#,
        r#"CREATE TRIGGER infra_event_notify_on_insert
            AFTER INSERT ON infra_event
            FOR EACH ROW
            EXECUTE FUNCTION infra_event_notify()"#,
    ],
    seed: &[],
};

pub async fn insert(
    pool: &PgPool,
    tenant_id: &str,
    project_id: uuid::Uuid,
    node_id: Option<&str>,
    event: InfraEvent,
) -> Result<i64> {
    let (kind, payload) = event.into_record();
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO infra_event \
         (tenant_id, project_id, node_id, kind, payload, at_unix) \
         VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT) \
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(node_id)
    .bind(kind.as_str())
    .bind(payload)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// The columns a settled read of `infra_event` takes next to `id`, the
/// ones [`parse_rows`] reads (`crate::settled::SettledReader::read`).
pub const READ_COLUMNS: &str = "tenant_id, project_id, node_id, kind, payload, at_unix";

pub fn parse_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<InfraEventRow>> {
    use sqlx::Row;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let id: i64 = r.try_get("id")?;
        let tenant_id: String = r.try_get("tenant_id")?;
        let project_id: uuid::Uuid = r.try_get("project_id")?;
        let node_id: Option<String> = r.try_get("node_id")?;
        let kind_str: String = r.try_get("kind")?;
        let payload: Value = r.try_get("payload")?;
        let at_unix: i64 = r.try_get("at_unix")?;
        // Fail loud on unknown kind OR unparseable payload: a newer
        // supervisor emits a shape this dispatcher doesn't
        // understand. Advancing the cursor past unparseable rows
        // would silently lose the event forever. The bridge bails
        // the drain; the cursor stays put; retry on next tick.
        let kind = InfraEventKind::parse(&kind_str).ok_or_else(|| {
            anyhow::anyhow!(
                "infra_event row id={id} has unknown kind '{kind_str}'; \
                 refusing to advance cursor. Upgrade the dispatcher."
            )
        })?;
        let event = InfraEvent::from_kind_and_payload(kind, &payload).map_err(|e| {
            anyhow::anyhow!(
                "infra_event row id={id} kind='{kind_str}' has malformed payload: {e}. \
                 Refusing to advance cursor."
            )
        })?;
        out.push(InfraEventRow {
            id,
            tenant_id,
            project_id,
            node_id,
            event,
            at_unix,
        });
    }
    Ok(out)
}

/// Drop every row for a project. Called on `weft rm`.
pub async fn remove_project<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    project_id: uuid::Uuid,
) -> Result<u64> {
    let res = sqlx::query("DELETE FROM infra_event WHERE project_id = $1")
        .bind(project_id)
        .execute(executor)
        .await?;
    Ok(res.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_wire_strings_are_stable() {
        // The SSE bridge keys on these strings. Pin them so a rename
        // doesn't silently break the wire.
        assert_eq!(InfraEventKind::Flaky.as_str(), "flaky");
        assert_eq!(InfraEventKind::Recovered.as_str(), "recovered");
        assert_eq!(InfraEventKind::Failed.as_str(), "failed");
        assert_eq!(InfraEventKind::Stopped.as_str(), "stopped");
        assert_eq!(InfraEventKind::Terminated.as_str(), "terminated");
        assert_eq!(InfraEventKind::Started.as_str(), "started");
        assert_eq!(InfraEventKind::Notify.as_str(), "notify");
    }
}
