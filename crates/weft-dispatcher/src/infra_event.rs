//! `infra_event` table. The infra-supervisor writes; the dispatcher
//! reads each row as it is announced (`infra_event_bridge`) and fans it
//! out over SSE.
//!
//! Distinct from a run's record: that holds durable run-graph
//! events. `infra_event` is for control-plane
//! state changes about the infra itself (a node went flaky, the
//! supervisor finished a terminate, etc).

use anyhow::Result;
use serde_json::Value;
use weft_task_store::db::{self, Client as _, JsonText, Pool};

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
            -- Whose copy of the node the event is about: NULL for the
            -- shared copy, or for a project-wide event.
            instance_id   TEXT,
            kind        TEXT NOT NULL,
            -- As its JSON text (`db::JsonText`): a failure's reason quotes
            -- what the infra said.
            payload     TEXT NOT NULL,
            at_unix     BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_event_chrono ON infra_event(id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_event_project ON infra_event(project_id)"#,
        // Announce every event, whoever wrote it (the broker for a
        // supervisor, or this crate's own `insert`), when the write
        // commits, naming its project and its id.
        // SYNC: 'weft_infra_event' <-> crate::infra_event_bridge::INFRA_EVENT_CHANNEL
        r#"CREATE OR REPLACE FUNCTION infra_event_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_event', NEW.project_id::text || ' ' || NEW.id::text);
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
    data_steps: &[],
};

pub async fn insert(
    pool: &Pool,
    tenant_id: &str,
    project_id: uuid::Uuid,
    node_id: Option<&str>,
    event: InfraEvent,
) -> Result<i64> {
    let (kind, payload) = event.into_record();
    Ok(pool
        .scalar(
            "INSERT INTO infra_event \
             (tenant_id, project_id, node_id, kind, payload, at_unix) \
             VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT) \
             RETURNING id",
            &[&tenant_id, &project_id, &node_id, &kind.as_str(), &JsonText(&payload)],
        )
        .await?)
}

/// The row `id`, `None` when it is gone. A row whose kind or payload this
/// dispatcher cannot read is an error naming it: a newer supervisor wrote
/// a shape this dispatcher does not know.
pub async fn read(pool: &Pool, id: i64) -> Result<Option<InfraEventRow>> {
    /// tenant, project, node, kind, payload, when.
    type EventRow = (String, uuid::Uuid, Option<String>, String, JsonText<Value>, i64);
    let row: Option<EventRow> = pool
        .opt("SELECT tenant_id, project_id, node_id, kind, payload, at_unix FROM infra_event WHERE id = $1", &[&id])
        .await?;
    let Some((tenant_id, project_id, node_id, kind_str, JsonText(payload), at_unix)) = row else { return Ok(None) };
    let kind = InfraEventKind::parse(&kind_str)
        .ok_or_else(|| anyhow::anyhow!("infra_event row id={id} has unknown kind '{kind_str}'. Upgrade the dispatcher."))?;
    let event = InfraEvent::from_kind_and_payload(kind, &payload)
        .map_err(|e| anyhow::anyhow!("infra_event row id={id} kind='{kind_str}' has malformed payload: {e}."))?;
    Ok(Some(InfraEventRow { id, tenant_id, project_id, node_id, event, at_unix }))
}

/// Drop every row for a project. Called on `weft rm`.
pub async fn remove_project(db: &impl db::Client, project_id: uuid::Uuid) -> Result<u64> {
    Ok(db.execute("DELETE FROM infra_event WHERE project_id = $1", &[&project_id]).await?)
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
    }
}
