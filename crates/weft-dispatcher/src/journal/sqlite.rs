//! Sqlite-backed journal. Single-file database under the dispatcher's
//! data_dir. Schema is minimal; we add columns as the dispatcher
//! grows.

use std::path::Path;

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use weft_core::{Color, CostReport};

use crate::journal::{Journal, WakeTarget};

pub struct SqliteJournal {
    pool: SqlitePool,
}

impl SqliteJournal {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
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
        let now = now_unix();
        sqlx::query(
            "INSERT OR REPLACE INTO execution (color, project_id, entry_node, status, started_at) VALUES (?, ?, ?, 'running', ?)",
        )
        .bind(color.to_string())
        .bind(project_id)
        .bind(entry_node)
        .bind(now as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_suspension(
        &self,
        color: Color,
        node: &str,
        metadata: serde_json::Value,
    ) -> anyhow::Result<()> {
        let token = uuid::Uuid::new_v4().to_string();
        let metadata_str = serde_json::to_string(&metadata)?;
        sqlx::query(
            "INSERT INTO suspension (token, color, node, metadata, created_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&token)
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
                let metadata: serde_json::Value = serde_json::from_str(&metadata_str)?;
                Ok(Some(WakeTarget { color, node, metadata }))
            }
        }
    }

    async fn record_cost(&self, color: Color, report: CostReport) -> anyhow::Result<()> {
        let metadata_str = serde_json::to_string(&report.metadata)?;
        sqlx::query(
            "INSERT INTO cost_event (color, service, model, amount_usd, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?)",
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
