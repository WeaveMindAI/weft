//! Sqlite-backed journal. Single file under the dispatcher's
//! data_dir. Schema is minimal and grows as needed.

use std::path::Path;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use weft_core::{Color, CostReport};

use crate::journal::{EntryKind, EntryTarget, Journal, WakeTarget};

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
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
