//! Sqlite-backed journal. Single file under the dispatcher's
//! data_dir. Schema is minimal and grows as needed.

use std::path::Path;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use weft_core::{Color, CostReport};

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
            CREATE TABLE IF NOT EXISTS execution (
                color TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                entry_node TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                completed_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS suspension (
                token TEXT PRIMARY KEY,
                color TEXT NOT NULL,
                node TEXT NOT NULL,
                metadata TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (color) REFERENCES execution(color)
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

            CREATE TABLE IF NOT EXISTS node_exec_event (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                color TEXT NOT NULL,
                node_id TEXT NOT NULL,
                lane TEXT NOT NULL,
                kind TEXT NOT NULL,
                input_json TEXT,
                output_json TEXT,
                error TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_node_exec_event_color
                ON node_exec_event(color);

            CREATE TABLE IF NOT EXISTS log_entry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                color TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_log_entry_color
                ON log_entry(color);

            CREATE TABLE IF NOT EXISTS cost_event (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                color TEXT NOT NULL,
                service TEXT NOT NULL,
                model TEXT,
                amount_usd REAL NOT NULL,
                metadata TEXT,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (color) REFERENCES execution(color)
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
    async fn record_start(
        &self,
        color: Color,
        project_id: &str,
        entry_node: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO execution (color, project_id, entry_node, status, started_at) \
             VALUES (?, ?, ?, 'running', ?)",
        )
        .bind(color.to_string())
        .bind(project_id)
        .bind(entry_node)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_suspension(
        &self,
        color: Color,
        node: &str,
        metadata: Value,
    ) -> anyhow::Result<String> {
        let token = uuid::Uuid::new_v4().to_string();
        let metadata_str = serde_json::to_string(&metadata)?;
        sqlx::query(
            "INSERT INTO suspension (token, color, node, metadata, created_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&token)
        .bind(color.to_string())
        .bind(node)
        .bind(metadata_str)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(token)
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

    async fn record_cost(&self, color: Color, report: CostReport) -> anyhow::Result<()> {
        let metadata_str = serde_json::to_string(&report.metadata)?;
        sqlx::query(
            "INSERT INTO cost_event (color, service, model, amount_usd, metadata, created_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(color.to_string())
        .bind(&report.service)
        .bind(&report.model)
        .bind(report.amount_usd)
        .bind(metadata_str)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn cancel(&self, color: Color) -> anyhow::Result<()> {
        let color_str = color.to_string();
        sqlx::query("DELETE FROM suspension WHERE color = ?")
            .bind(&color_str)
            .execute(&self.pool)
            .await?;
        sqlx::query("UPDATE execution SET status = 'cancelled', completed_at = ? WHERE color = ?")
            .bind(now_unix() as i64)
            .bind(&color_str)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn execution_project(&self, color: Color) -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT project_id FROM execution WHERE color = ?")
                .bind(color.to_string())
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(p,)| p))
    }

    async fn append_log(
        &self,
        color: Color,
        level: &str,
        message: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO log_entry (color, level, message, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(color.to_string())
        .bind(level)
        .bind(message)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn logs_for(&self, color: Color, limit: u32) -> anyhow::Result<Vec<LogEntry>> {
        let rows: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT created_at, level, message FROM log_entry \
             WHERE color = ? ORDER BY id ASC LIMIT ?",
        )
        .bind(color.to_string())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(at, level, message)| LogEntry {
                at_unix: at as u64,
                level,
                message,
            })
            .collect())
    }

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

    async fn record_node_event(&self, event: &NodeExecEvent) -> anyhow::Result<()> {
        let input_str = event
            .input
            .as_ref()
            .map(|v| serde_json::to_string(v))
            .transpose()?;
        let output_str = event
            .output
            .as_ref()
            .map(|v| serde_json::to_string(v))
            .transpose()?;
        sqlx::query(
            "INSERT INTO node_exec_event (color, node_id, lane, kind, input_json, output_json, error, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.color.to_string())
        .bind(&event.node_id)
        .bind(&event.lane)
        .bind(event.kind.as_str())
        .bind(input_str)
        .bind(output_str)
        .bind(event.error.as_deref())
        .bind(event.at_unix as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn events_for(&self, color: Color) -> anyhow::Result<Vec<NodeExecEvent>> {
        let rows: Vec<(String, String, String, Option<String>, Option<String>, Option<String>, i64)> =
            sqlx::query_as(
                "SELECT node_id, lane, kind, input_json, output_json, error, created_at \
                 FROM node_exec_event WHERE color = ? ORDER BY id ASC",
            )
            .bind(color.to_string())
            .fetch_all(&self.pool)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (node_id, lane, kind, input, output, error, at) in rows {
            let Some(kind) = NodeExecKind::parse(&kind) else { continue };
            let input_v = input
                .map(|s| serde_json::from_str(&s).unwrap_or(Value::Null));
            let output_v = output
                .map(|s| serde_json::from_str(&s).unwrap_or(Value::Null));
            out.push(NodeExecEvent {
                color,
                node_id,
                lane,
                kind,
                input: input_v,
                output: output_v,
                error,
                at_unix: at as u64,
            });
        }
        Ok(out)
    }

    async fn delete_execution(&self, color: Color) -> anyhow::Result<()> {
        let s = color.to_string();
        sqlx::query("DELETE FROM node_exec_event WHERE color = ?")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM log_entry WHERE color = ?")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM cost_event WHERE color = ?")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM suspension WHERE color = ?")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM execution WHERE color = ?")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_executions(&self, limit: u32) -> anyhow::Result<Vec<ExecutionSummary>> {
        let rows: Vec<(String, String, String, String, i64, Option<i64>)> = sqlx::query_as(
            "SELECT color, project_id, entry_node, status, started_at, completed_at \
             FROM execution ORDER BY started_at DESC LIMIT ?",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (color_str, project_id, entry_node, status, started_at, completed_at) in rows {
            let Ok(color) = color_str.parse::<Color>() else { continue };
            out.push(ExecutionSummary {
                color,
                project_id,
                entry_node,
                status,
                started_at: started_at as u64,
                completed_at: completed_at.map(|v| v as u64),
            });
        }
        Ok(out)
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
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
