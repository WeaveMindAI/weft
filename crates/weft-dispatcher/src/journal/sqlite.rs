//! Sqlite-backed journal. Single file under the dispatcher's
//! data_dir. Execution state lives entirely in the `exec_event`
//! table; other tables are lookup indexes (tokens) that the event
//! log doesn't replace.

use std::path::Path;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use weft_core::Color;

use crate::journal::events::ExecEvent;
use crate::journal::{
    EntryKind, EntryTarget, ExecutionSummary, ExtToken, Journal, LogEntry, NodeExecEvent,
    NodeExecKind, OpenSuspension, WakeTarget,
};

pub struct SqliteJournal {
    pool: SqlitePool,
}

impl SqliteJournal {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let opts = SqliteConnectOptions::new().filename(path).create_if_missing(true);
        let pool = SqlitePoolOptions::new().max_connections(8).connect_with(opts).await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS exec_event (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                color TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_exec_event_color
                ON exec_event(color, id);

            CREATE TABLE IF NOT EXISTS suspension (
                token TEXT PRIMARY KEY,
                color TEXT NOT NULL,
                node TEXT NOT NULL,
                metadata TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS entry_token (
                token TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                entry_kind TEXT NOT NULL,
                path TEXT,
                auth TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_entry_token_project
                ON entry_token(project_id);

            CREATE TABLE IF NOT EXISTS ext_token (
                token TEXT PRIMARY KEY,
                name TEXT,
                metadata TEXT,
                created_at INTEGER NOT NULL
            );
            "#,
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl Journal for SqliteJournal {
    // ---------- Event log ----------

    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        let payload = serde_json::to_string(event)?;
        sqlx::query(
            "INSERT INTO exec_event (color, kind, payload_json, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(event.color().to_string())
        .bind(event.kind_str())
        .bind(payload)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn events_log(&self, color: Color) -> anyhow::Result<Vec<ExecEvent>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event WHERE color = ? ORDER BY id ASC",
        )
        .bind(color.to_string())
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (payload,) in rows {
            match serde_json::from_str::<ExecEvent>(&payload) {
                Ok(ev) => out.push(ev),
                Err(e) => {
                    tracing::warn!(
                        target: "weft_dispatcher::journal",
                        color = %color, error = %e,
                        "skip malformed event payload",
                    );
                }
            }
        }
        Ok(out)
    }

    // ---------- Suspension lookup ----------

    async fn record_suspension_with_token(
        &self,
        token: &str,
        color: Color,
        node: &str,
        metadata: Value,
    ) -> anyhow::Result<()> {
        let metadata_str = serde_json::to_string(&metadata)?;
        sqlx::query(
            "INSERT INTO suspension (token, color, node, metadata, created_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(token)
        .bind(color.to_string())
        .bind(node)
        .bind(metadata_str)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn resolve_wake(&self, token: &str) -> anyhow::Result<Option<WakeTarget>> {
        let row: Option<(String, String, String)> =
            sqlx::query_as("SELECT color, node, metadata FROM suspension WHERE token = ?")
                .bind(token)
                .fetch_optional(&self.pool)
                .await?;
        match row {
            None => Ok(None),
            Some((color_str, node, metadata_str)) => {
                let color: Color = color_str.parse()?;
                let metadata: Value = serde_json::from_str(&metadata_str)?;
                Ok(Some(WakeTarget { color, node, metadata }))
            }
        }
    }

    async fn consume_suspension(&self, token: &str) -> anyhow::Result<bool> {
        let result = sqlx::query("DELETE FROM suspension WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn list_open_suspensions(&self) -> anyhow::Result<Vec<OpenSuspension>> {
        let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
            "SELECT token, color, node, metadata, created_at FROM suspension \
             ORDER BY created_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (token, color_str, node, metadata_str, created_at) in rows {
            let Ok(color) = color_str.parse::<Color>() else { continue };
            let metadata: Value = serde_json::from_str(&metadata_str).unwrap_or(Value::Null);
            out.push(OpenSuspension {
                token,
                color,
                node,
                metadata,
                created_at: created_at as u64,
            });
        }
        Ok(out)
    }

    // ---------- Entry tokens ----------

    async fn mint_entry_token(
        &self,
        project_id: &str,
        node_id: &str,
        kind: EntryKind,
        path: Option<&str>,
        auth: Option<Value>,
    ) -> anyhow::Result<String> {
        let token = uuid::Uuid::new_v4().to_string();
        let auth_str = match auth {
            Some(v) => Some(serde_json::to_string(&v)?),
            None => None,
        };
        sqlx::query(
            "INSERT INTO entry_token (token, project_id, node_id, entry_kind, path, auth, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&token)
        .bind(project_id)
        .bind(node_id)
        .bind(kind.as_str())
        .bind(path)
        .bind(auth_str)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(token)
    }

    async fn resolve_entry_token(&self, token: &str) -> anyhow::Result<Option<EntryTarget>> {
        let row: Option<(String, String, String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT project_id, node_id, entry_kind, path, auth FROM entry_token WHERE token = ?",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((project_id, node_id, kind_str, path, auth_str)) => {
                let kind = EntryKind::parse(&kind_str).unwrap_or(EntryKind::Manual);
                let auth = match auth_str {
                    Some(s) => Some(serde_json::from_str(&s)?),
                    None => None,
                };
                Ok(Some(EntryTarget { project_id, node_id, kind, path, auth }))
            }
        }
    }

    async fn drop_entry_tokens(&self, project_id: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM entry_token WHERE project_id = ?")
            .bind(project_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ---------- Extension tokens ----------

    async fn mint_ext_token(
        &self,
        name: Option<&str>,
        metadata: Option<Value>,
    ) -> anyhow::Result<String> {
        let token = format!("wm_ext_{}", uuid::Uuid::new_v4().simple());
        let metadata_str = match metadata {
            Some(v) => Some(serde_json::to_string(&v)?),
            None => None,
        };
        sqlx::query(
            "INSERT INTO ext_token (token, name, metadata, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(&token)
        .bind(name)
        .bind(metadata_str)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(token)
    }

    async fn ext_token_exists(&self, token: &str) -> anyhow::Result<bool> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT token FROM ext_token WHERE token = ?")
                .bind(token)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.is_some())
    }

    async fn list_ext_tokens(&self) -> anyhow::Result<Vec<ExtToken>> {
        let rows: Vec<(String, Option<String>, i64)> = sqlx::query_as(
            "SELECT token, name, created_at FROM ext_token ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(token, name, created_at)| ExtToken {
                token,
                name,
                created_at: created_at as u64,
            })
            .collect())
    }

    async fn revoke_ext_token(&self, token: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM ext_token WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ---------- Derived views over the event log ----------

    async fn execution_project(&self, color: Color) -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event \
             WHERE color = ? AND kind = 'execution_started' \
             ORDER BY id ASC LIMIT 1",
        )
        .bind(color.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let Some((payload,)) = row else { return Ok(None) };
        let ev: ExecEvent = serde_json::from_str(&payload)?;
        Ok(match ev {
            ExecEvent::ExecutionStarted { project_id, .. } => Some(project_id),
            _ => None,
        })
    }

    async fn logs_for(&self, color: Color, limit: u32) -> anyhow::Result<Vec<LogEntry>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event \
             WHERE color = ? AND kind = 'log_line' \
             ORDER BY id ASC LIMIT ?",
        )
        .bind(color.to_string())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (payload,) in rows {
            if let Ok(ExecEvent::LogLine { level, message, at_unix, .. }) =
                serde_json::from_str::<ExecEvent>(&payload)
            {
                out.push(LogEntry { at_unix, level, message });
            }
        }
        Ok(out)
    }

    async fn events_for(&self, color: Color) -> anyhow::Result<Vec<NodeExecEvent>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event \
             WHERE color = ? AND kind IN ('node_started', 'node_completed', 'node_failed', 'node_skipped') \
             ORDER BY id ASC",
        )
        .bind(color.to_string())
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (payload,) in rows {
            let Ok(ev) = serde_json::from_str::<ExecEvent>(&payload) else { continue };
            if let Some(converted) = exec_event_to_node_exec(&ev) {
                out.push(converted);
            }
        }
        Ok(out)
    }

    async fn list_executions(&self, limit: u32) -> anyhow::Result<Vec<ExecutionSummary>> {
        // Collect every ExecutionStarted to get the base rows, then
        // walk forward per color to compute status + completed_at.
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event \
             WHERE kind = 'execution_started' \
             ORDER BY id DESC LIMIT ?",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for (payload,) in rows {
            let Ok(ev) = serde_json::from_str::<ExecEvent>(&payload) else { continue };
            let ExecEvent::ExecutionStarted { color, project_id, entry_node, at_unix } = ev else {
                continue;
            };
            let (status, completed_at) = terminal_for_color(&self.pool, color).await?;
            out.push(ExecutionSummary {
                color,
                project_id,
                entry_node,
                status,
                started_at: at_unix,
                completed_at,
            });
        }
        Ok(out)
    }

    // ---------- Administrative ----------

    async fn delete_execution(&self, color: Color) -> anyhow::Result<()> {
        let s = color.to_string();
        sqlx::query("DELETE FROM exec_event WHERE color = ?")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM suspension WHERE color = ?")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn cancel(&self, color: Color) -> anyhow::Result<()> {
        // Drop any live suspensions for this color and append a
        // cancellation failure event. Workers poll nothing yet (a
        // full cancel protocol lands in Phase B); for now the event
        // is the record.
        sqlx::query("DELETE FROM suspension WHERE color = ?")
            .bind(color.to_string())
            .execute(&self.pool)
            .await?;
        self.record_event(&ExecEvent::ExecutionFailed {
            color,
            error: "cancelled".into(),
            at_unix: now_unix(),
        })
        .await
    }
}

// ----- Helpers --------------------------------------------------------

/// Compute `(status, completed_at)` for a color by walking its
/// event log for the most recent terminal or parking event.
async fn terminal_for_color(
    pool: &SqlitePool,
    color: Color,
) -> anyhow::Result<(String, Option<u64>)> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT payload_json FROM exec_event \
         WHERE color = ? AND kind IN ('execution_completed', 'execution_failed', 'stalled') \
         ORDER BY id DESC LIMIT 1",
    )
    .bind(color.to_string())
    .fetch_all(pool)
    .await?;
    if let Some((payload,)) = rows.into_iter().next() {
        if let Ok(ev) = serde_json::from_str::<ExecEvent>(&payload) {
            match ev {
                ExecEvent::ExecutionCompleted { at_unix, .. } => {
                    return Ok(("completed".into(), Some(at_unix)));
                }
                ExecEvent::ExecutionFailed { at_unix, .. } => {
                    return Ok(("failed".into(), Some(at_unix)));
                }
                ExecEvent::Stalled { at_unix, .. } => {
                    return Ok(("stalled".into(), Some(at_unix)));
                }
                _ => {}
            }
        }
    }
    Ok(("running".into(), None))
}

fn exec_event_to_node_exec(ev: &ExecEvent) -> Option<NodeExecEvent> {
    let (color, node_id, lane, kind, input, output, error, at_unix) = match ev {
        ExecEvent::NodeStarted { color, node_id, lane, input, at_unix, .. } => (
            *color,
            node_id.clone(),
            serde_json::to_string(lane).unwrap_or_default(),
            NodeExecKind::Started,
            Some(input.clone()),
            None,
            None,
            *at_unix,
        ),
        ExecEvent::NodeCompleted { color, node_id, lane, output, at_unix } => (
            *color,
            node_id.clone(),
            serde_json::to_string(lane).unwrap_or_default(),
            NodeExecKind::Completed,
            None,
            Some(output.clone()),
            None,
            *at_unix,
        ),
        ExecEvent::NodeFailed { color, node_id, lane, error, at_unix } => (
            *color,
            node_id.clone(),
            serde_json::to_string(lane).unwrap_or_default(),
            NodeExecKind::Failed,
            None,
            None,
            Some(error.clone()),
            *at_unix,
        ),
        ExecEvent::NodeSkipped { color, node_id, lane, at_unix } => (
            *color,
            node_id.clone(),
            serde_json::to_string(lane).unwrap_or_default(),
            NodeExecKind::Skipped,
            None,
            None,
            None,
            *at_unix,
        ),
        _ => return None,
    };
    Some(NodeExecEvent {
        color,
        node_id,
        lane,
        kind,
        input,
        output,
        error,
        at_unix,
    })
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
