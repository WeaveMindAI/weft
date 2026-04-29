//! Postgres-backed journal. Multiple dispatcher Pods share one
//! Postgres database; the event log + token lookup tables are the
//! durable state. Postgres is the source of truth, every dispatcher
//! Pod is a stateless reader/writer.
//!
//! Schema migrations run idempotently on `connect`. They use
//! `CREATE TABLE IF NOT EXISTS` so adding a Pod or restarting one
//! is safe.

use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions};

use weft_core::Color;

use crate::journal::events::ExecEvent;
use crate::journal::{
    EntryKind, EntryTarget, ExecutionSummary, ExtToken, Journal, LogEntry, NodeExecEvent,
    NodeExecKind, OpenSuspension, SignalRegistration, WakeTarget,
};

pub struct PostgresJournal {
    pool: PgPool,
}

impl PostgresJournal {
    /// Connect to Postgres at `database_url` (`postgres://user:pass@host:port/db`).
    /// Creates the schema on first connect; idempotent. Retries
    /// the initial connection for up to 60s so a Pod that boots
    /// before Postgres is ready doesn't crash-loop.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let pool = loop {
            match PgPoolOptions::new()
                .max_connections(16)
                .acquire_timeout(std::time::Duration::from_secs(5))
                .connect(database_url)
                .await
            {
                Ok(p) => break p,
                Err(e) if std::time::Instant::now() < deadline => {
                    tracing::warn!(
                        target: "weft_dispatcher::journal",
                        error = %e,
                        "postgres not ready yet; retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Err(e) => return Err(e.into()),
            }
        };
        migrate(&pool).await?;
        Ok(Self { pool })
    }

    /// Expose the inner pool for sibling modules (lease manager,
    /// EventBus pub/sub) so they can share connections.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS exec_event (
            id BIGSERIAL PRIMARY KEY,
            color TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_exec_event_color ON exec_event(color, id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_exec_event_kind ON exec_event(kind, id DESC)"#,
        r#"CREATE TABLE IF NOT EXISTS suspension (
            token TEXT PRIMARY KEY,
            color TEXT NOT NULL,
            node TEXT NOT NULL,
            metadata TEXT NOT NULL,
            created_at BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_suspension_color ON suspension(color)"#,
        r#"CREATE TABLE IF NOT EXISTS entry_token (
            token TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            entry_kind TEXT NOT NULL,
            path TEXT,
            auth TEXT,
            created_at BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_entry_token_project ON entry_token(project_id)"#,
        r#"CREATE TABLE IF NOT EXISTS ext_token (
            token TEXT PRIMARY KEY,
            name TEXT,
            metadata TEXT,
            created_at BIGINT NOT NULL
        )"#,
        r#"CREATE TABLE IF NOT EXISTS signal (
            token TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            color TEXT,
            node_id TEXT NOT NULL,
            is_resume BOOLEAN NOT NULL,
            user_url TEXT,
            kind TEXT NOT NULL,
            spec_json TEXT NOT NULL,
            created_at BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_tenant ON signal(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_project ON signal(project_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_color ON signal(color)"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

#[async_trait]
impl Journal for PostgresJournal {
    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        let payload = serde_json::to_string(event)?;
        sqlx::query(
            "INSERT INTO exec_event (color, kind, payload_json, created_at) \
             VALUES ($1, $2, $3, $4)",
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
            "SELECT payload_json FROM exec_event WHERE color = $1 ORDER BY id ASC",
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
             VALUES ($1, $2, $3, $4, $5)",
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
        let row: Option<(String, String, String)> = sqlx::query_as(
            "SELECT color, node, metadata FROM suspension WHERE token = $1",
        )
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
        let result = sqlx::query("DELETE FROM suspension WHERE token = $1")
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
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
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
            "SELECT project_id, node_id, entry_kind, path, auth FROM entry_token WHERE token = $1",
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
        sqlx::query("DELETE FROM entry_token WHERE project_id = $1")
            .bind(project_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn mint_ext_token(
        &self,
        token: &str,
        name: Option<&str>,
        metadata: Option<Value>,
    ) -> anyhow::Result<()> {
        let metadata_str = match metadata {
            Some(v) => Some(serde_json::to_string(&v)?),
            None => None,
        };
        sqlx::query(
            "INSERT INTO ext_token (token, name, metadata, created_at) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(token)
        .bind(name)
        .bind(metadata_str)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn ext_token_exists(&self, token: &str) -> anyhow::Result<bool> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT token FROM ext_token WHERE token = $1",
        )
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

    async fn revoke_ext_token(&self, identifier: &str) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "DELETE FROM ext_token WHERE token = $1 OR name = $1",
        )
        .bind(identifier)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn execution_project(&self, color: Color) -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event \
             WHERE color = $1 AND kind = 'execution_started' \
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
             WHERE color = $1 AND kind = 'log_line' \
             ORDER BY id ASC LIMIT $2",
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
             WHERE color = $1 AND kind IN ('node_started', 'node_completed', 'node_failed', 'node_skipped') \
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
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event \
             WHERE kind = 'execution_started' \
             ORDER BY id DESC LIMIT $1",
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

    async fn delete_execution(&self, color: Color) -> anyhow::Result<()> {
        let s = color.to_string();
        sqlx::query("DELETE FROM exec_event WHERE color = $1")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM suspension WHERE color = $1")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn cancel(&self, color: Color) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM suspension WHERE color = $1")
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

    async fn signal_insert(&self, sig: &SignalRegistration) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO signal (token, tenant_id, project_id, color, node_id, is_resume, user_url, kind, spec_json, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(&sig.token)
        .bind(&sig.tenant_id)
        .bind(&sig.project_id)
        .bind(sig.color.map(|c| c.to_string()))
        .bind(&sig.node_id)
        .bind(sig.is_resume)
        .bind(sig.user_url.as_deref())
        .bind(&sig.kind)
        .bind(&sig.spec_json)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<(
            String,
            String,
            String,
            Option<String>,
            String,
            bool,
            Option<String>,
            String,
            String,
        )> = sqlx::query_as(
            "SELECT token, tenant_id, project_id, color, node_id, is_resume, user_url, kind, spec_json \
             FROM signal WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_signal))
    }

    async fn signal_remove(&self, token: &str) -> anyhow::Result<bool> {
        let res = sqlx::query("DELETE FROM signal WHERE token = $1")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn signal_list_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<(
            String,
            String,
            String,
            Option<String>,
            String,
            bool,
            Option<String>,
            String,
            String,
        )> = sqlx::query_as(
            "SELECT token, tenant_id, project_id, color, node_id, is_resume, user_url, kind, spec_json \
             FROM signal WHERE project_id = $1",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_signal).collect())
    }

    async fn signal_list_for_tenant(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<(
            String,
            String,
            String,
            Option<String>,
            String,
            bool,
            Option<String>,
            String,
            String,
        )> = sqlx::query_as(
            "SELECT token, tenant_id, project_id, color, node_id, is_resume, user_url, kind, spec_json \
             FROM signal WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_signal).collect())
    }

    async fn signal_count_for_tenant(&self, tenant_id: &str) -> anyhow::Result<usize> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM signal WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0 as usize)
    }

    async fn signal_remove_for_color(
        &self,
        color: Color,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<(
            String,
            String,
            String,
            Option<String>,
            String,
            bool,
            Option<String>,
            String,
            String,
        )> = sqlx::query_as(
            "DELETE FROM signal WHERE color = $1 \
             RETURNING token, tenant_id, project_id, color, node_id, is_resume, user_url, kind, spec_json",
        )
        .bind(color.to_string())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_signal).collect())
    }

    async fn signal_remove_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<(
            String,
            String,
            String,
            Option<String>,
            String,
            bool,
            Option<String>,
            String,
            String,
        )> = sqlx::query_as(
            "DELETE FROM signal WHERE project_id = $1 \
             RETURNING token, tenant_id, project_id, color, node_id, is_resume, user_url, kind, spec_json",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_signal).collect())
    }
}

fn row_to_signal(
    row: (
        String,
        String,
        String,
        Option<String>,
        String,
        bool,
        Option<String>,
        String,
        String,
    ),
) -> SignalRegistration {
    let (token, tenant_id, project_id, color_str, node_id, is_resume, user_url, kind, spec_json) =
        row;
    let color = color_str.and_then(|s| s.parse::<Color>().ok());
    SignalRegistration {
        token,
        tenant_id,
        project_id,
        color,
        node_id,
        is_resume,
        user_url,
        kind,
        spec_json,
    }
}

async fn terminal_for_color(
    pool: &PgPool,
    color: Color,
) -> anyhow::Result<(String, Option<u64>)> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT payload_json FROM exec_event \
         WHERE color = $1 AND kind IN ('execution_completed', 'execution_failed', 'stalled') \
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
    match ev {
        ExecEvent::NodeStarted { color, node_id, lane, input, at_unix, .. } => Some(NodeExecEvent {
            color: *color,
            node_id: node_id.clone(),
            lane: serde_json::to_string(lane).unwrap_or_default(),
            kind: NodeExecKind::Started,
            input: Some(input.clone()),
            output: None,
            error: None,
            token: None,
            value: None,
            reason: None,
            at_unix: *at_unix,
        }),
        ExecEvent::NodeSuspended { color, node_id, lane, token, at_unix } => Some(NodeExecEvent {
            color: *color,
            node_id: node_id.clone(),
            lane: serde_json::to_string(lane).unwrap_or_default(),
            kind: NodeExecKind::Suspended,
            input: None,
            output: None,
            error: None,
            token: Some(token.clone()),
            value: None,
            reason: None,
            at_unix: *at_unix,
        }),
        ExecEvent::NodeResumed { color, node_id, lane, token, value, at_unix } => Some(NodeExecEvent {
            color: *color,
            node_id: node_id.clone(),
            lane: serde_json::to_string(lane).unwrap_or_default(),
            kind: NodeExecKind::Resumed,
            input: None,
            output: None,
            error: None,
            token: Some(token.clone()),
            value: Some(value.clone()),
            reason: None,
            at_unix: *at_unix,
        }),
        ExecEvent::NodeRetried { color, node_id, lane, reason, at_unix } => Some(NodeExecEvent {
            color: *color,
            node_id: node_id.clone(),
            lane: serde_json::to_string(lane).unwrap_or_default(),
            kind: NodeExecKind::Retried,
            input: None,
            output: None,
            error: None,
            token: None,
            value: None,
            reason: Some(reason.clone()),
            at_unix: *at_unix,
        }),
        ExecEvent::NodeCancelled { color, node_id, lane, reason, at_unix } => Some(NodeExecEvent {
            color: *color,
            node_id: node_id.clone(),
            lane: serde_json::to_string(lane).unwrap_or_default(),
            kind: NodeExecKind::Cancelled,
            input: None,
            output: None,
            error: Some(reason.clone()),
            token: None,
            value: None,
            reason: Some(reason.clone()),
            at_unix: *at_unix,
        }),
        ExecEvent::NodeCompleted { color, node_id, lane, output, at_unix } => Some(NodeExecEvent {
            color: *color,
            node_id: node_id.clone(),
            lane: serde_json::to_string(lane).unwrap_or_default(),
            kind: NodeExecKind::Completed,
            input: None,
            output: Some(output.clone()),
            error: None,
            token: None,
            value: None,
            reason: None,
            at_unix: *at_unix,
        }),
        ExecEvent::NodeFailed { color, node_id, lane, error, at_unix } => Some(NodeExecEvent {
            color: *color,
            node_id: node_id.clone(),
            lane: serde_json::to_string(lane).unwrap_or_default(),
            kind: NodeExecKind::Failed,
            input: None,
            output: None,
            error: Some(error.clone()),
            token: None,
            value: None,
            reason: None,
            at_unix: *at_unix,
        }),
        ExecEvent::NodeSkipped { color, node_id, lane, at_unix } => Some(NodeExecEvent {
            color: *color,
            node_id: node_id.clone(),
            lane: serde_json::to_string(lane).unwrap_or_default(),
            kind: NodeExecKind::Skipped,
            input: None,
            output: None,
            error: None,
            token: None,
            value: None,
            reason: None,
            at_unix: *at_unix,
        }),
        _ => None,
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
