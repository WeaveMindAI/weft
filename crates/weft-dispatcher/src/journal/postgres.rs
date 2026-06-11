//! Postgres-backed journal. Multiple dispatcher Pods share one
//! Postgres database; the event log + token lookup tables are the
//! durable state. Postgres is the source of truth, every dispatcher
//! Pod is a stateless reader/writer.
//!
//! Schema migrations run idempotently on `connect`. They use
//! `CREATE TABLE IF NOT EXISTS` so adding a Pod or restarting one
//! is safe.

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};

use weft_core::Color;

use weft_journal::ExecEvent;
use crate::journal::{
    ApiToken, ColorLookup, ExecutionSummary, Journal, LogEntry, SignalRegistration,
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

    /// The color's first `ExecutionStarted` event, decoded. The ONE
    /// fetch behind `execution_project` and
    /// `execution_definition_hash`. A row whose JSON no longer
    /// decodes is a PERMANENT poison: returning `Err` would make
    /// pollers (the journal bridge's per-row processing) retry the
    /// same row forever, stalling the cursor fleet-wide. Log loud
    /// and report `Corrupt` (non-retryable, distinct from
    /// `NotFound`) so callers can word the failure honestly.
    async fn execution_started(&self, color: Color) -> anyhow::Result<ColorLookup<ExecEvent>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event \
             WHERE color = $1 AND kind = 'execution_started' \
             ORDER BY id ASC LIMIT 1",
        )
        .bind(color.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let Some((payload,)) = row else { return Ok(ColorLookup::NotFound) };
        match serde_json::from_str::<ExecEvent>(&payload) {
            Ok(ev) => Ok(ColorLookup::Found(ev)),
            Err(e) => {
                tracing::error!(
                    target: "weft_dispatcher::journal",
                    %color,
                    error = %e,
                    "ExecutionStarted row failed to decode \
                     (permanent corruption, retrying cannot fix it)"
                );
                Ok(ColorLookup::Corrupt)
            }
        }
    }

    /// Write one event, pairing an `ExecutionStarted` with its
    /// `execution_color` seed in ONE transaction. The seed
    /// denormalizes (color, project_id, tenant_id) so the broker's
    /// scope check and the terminal sweeps
    /// (`list_non_terminal_colors_for_project`) can see the color
    /// without folding the journal; a crash between the event insert
    /// and a separate seed would create a color those sweeps can
    /// NEVER see (untouchable junk), so the two commit together. A
    /// missing project row fails the whole write loudly instead of
    /// silently journaling an unsweepable execution.
    async fn record_with_seed(
        &self,
        event: &ExecEvent,
        dedup_key: Option<&str>,
    ) -> anyhow::Result<()> {
        let ExecEvent::ExecutionStarted { color, project_id, at_unix, phase, .. } = event else {
            return weft_journal::record_event_in(&self.pool, event, None, dedup_key)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"));
        };
        let mut tx = self.pool.begin().await?;
        weft_journal::record_event_in(&mut *tx, event, None, dedup_key)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows = sqlx::query(
            "INSERT INTO execution_color (color, project_id, tenant_id, started_at_unix, phase) \
             SELECT $1, $2, p.tenant_id, $3, $4 FROM project p WHERE p.id = $2::uuid \
             ON CONFLICT (color) DO NOTHING",
        )
        .bind(color.to_string())
        .bind(project_id)
        .bind(*at_unix as i64)
        .bind(phase.as_str())
        .execute(&mut *tx)
        .await?;
        if rows.rows_affected() == 0 {
            // Zero rows = conflict (already seeded: a dedup'd retry)
            // OR missing project. Only the latter is an error.
            let (already_seeded,): (bool,) = sqlx::query_as(
                "SELECT EXISTS(SELECT 1 FROM execution_color WHERE color = $1)",
            )
            .bind(color.to_string())
            .fetch_one(&mut *tx)
            .await?;
            if !already_seeded {
                anyhow::bail!(
                    "refuse to journal ExecutionStarted for color {color}: project \
                     {project_id} has no row, so the execution_color seed (which the \
                     broker scope check and the terminal sweeps depend on) cannot be \
                     written; register the project first"
                );
            }
        }
        tx.commit().await?;
        Ok(())
    }
}

async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    let stmts = [
        // exec_event: append-only journal. `dedup_key` is the
        // idempotency knob writers that may retry (dispatcher tasks
        // that crash mid-execution) populate; the partial UNIQUE
        // means unkeyed events (most worker-side events) are
        // unrestricted, keyed events collapse on conflict.
        r#"CREATE TABLE IF NOT EXISTS exec_event (
            id BIGSERIAL PRIMARY KEY,
            color TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            pod_name TEXT,
            dedup_key TEXT
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_exec_event_color ON exec_event(color, id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_exec_event_kind ON exec_event(kind, id DESC)"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_exec_event_dedup
           ON exec_event(dedup_key) WHERE dedup_key IS NOT NULL"#,
        // api_token: token-scoped enumeration credential. Allow
        // sets are TEXT[] arrays so parameterized binding gives no
        // SQL-injection surface and the filter SQL is a single `&&`
        // (overlap) clause per scope dimension. Empty array on any
        // column = wildcard (matches everything).
        r#"CREATE TABLE IF NOT EXISTS api_token (
            token TEXT PRIMARY KEY,
            name TEXT,
            allowed_kinds TEXT[] NOT NULL DEFAULT '{}',
            allowed_projects UUID[] NOT NULL DEFAULT '{}',
            allowed_tags TEXT[] NOT NULL DEFAULT '{}',
            metadata TEXT,
            created_at BIGINT NOT NULL
        )"#,
        // signal: one row per registered wake target (entry trigger
        // or resume token). Routing fields denormalize what the
        // public router needs at fire time so it doesn't parse
        // spec_json per-request.
        r#"CREATE TABLE IF NOT EXISTS signal (
            token TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            color TEXT,
            node_id TEXT NOT NULL,
            is_resume BOOLEAN NOT NULL,
            spec_json TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            -- Opaque per-kind state persisted at register time and
            -- read back at rehydrate time. Empty for most kinds.
            -- Timer uses it to persist absolute next_fire_at_unix_ms
            -- for After-style schedules so a listener restart
            -- doesn't reset the clock. Future stateful kinds (e.g.
            -- SSE last-event-id, socket reconnect token) use the
            -- same column.
            kind_state JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- FIFO queue of fires that landed while the project was
            -- not Active (Activating / park / hibernate-in-grace /
            -- Deactivating). Each element is { "payload": <json>,
            -- "received_at_unix": <int> }. Entry signals append on
            -- every fire; resume signals append iff the queue is
            -- empty (first submission wins; subsequent ones for the
            -- same suspension are dropped). Drained on reactivate by
            -- replaying every element through dispatch_listener_outcome,
            -- then clearing the array.
            parked_fires JSONB NOT NULL DEFAULT '[]'::jsonb,
            -- Claim guard for the drain loop: set when a dispatcher
            -- pod claims this row's queue for replay, cleared on
            -- either success (alongside parked_fires=[]) or failure
            -- (release). A sweeper releases stale claims older than
            -- the claim-stale threshold so a dispatcher crash
            -- mid-step doesn't leave the row uncloseable.
            drain_claimed_at_unix BIGINT,
            -- Per-claim owner nonce. Set when a drain claims the row;
            -- every pop + the release is fenced on it. If a stale-claim
            -- sweep hands the row to a sibling pod mid-drain, the
            -- original drainer's fenced pop matches 0 rows and it aborts
            -- instead of popping an element the new owner already
            -- dispatched (which would silently drop an undispatched fire).
            drain_claimed_by TEXT,
            consumer_kind TEXT,
            tags TEXT[] NOT NULL DEFAULT '{}',
            consumer_payload TEXT,
            surface_kind TEXT NOT NULL DEFAULT 'task_callback',
            mount_path TEXT,
            auth_kind TEXT NOT NULL DEFAULT 'none',
            auth_config JSONB
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_tenant ON signal(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_project ON signal(project_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_color ON signal(color)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_consumer_kind ON signal(consumer_kind)"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_mount_path
             ON signal(mount_path) WHERE mount_path IS NOT NULL"#,
        // Entry rows are keyed by (project_id, node_id): that pair
        // is what TriggerSetup re-targets on every reactivate. The
        // partial unique index lets `signal_insert` upsert entry
        // rows in place. Resume rows (is_resume=TRUE) skip the
        // constraint because each suspension mints its own row.
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_entry_node
             ON signal(project_id, node_id) WHERE is_resume = FALSE"#,
        // execution_color binds an execution color to its project +
        // tenant, denormalized for the broker's scope-check fast path.
        // The dispatcher INSERTs this row alongside ExecutionStarted
        // (which is the only event that introduces a fresh color).
        // Workers / listeners never write here; they only need it to
        // exist so the broker can answer "does color C belong to
        // tenant T" without re-folding the journal.
        r#"CREATE TABLE IF NOT EXISTS execution_color (
            color TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            started_at_unix BIGINT NOT NULL,
            phase TEXT NOT NULL,
            -- Worker pod that owns this color's writes. Set at
            -- ExecutionStarted time and never changes. The broker
            -- rejects any journal_record whose caller.pod_name
            -- doesn't match, so a compromised worker can only
            -- journal under its own bound pod, not cross-write
            -- sibling executions in the same tenant. NULL means
            -- "no pod assigned yet" (dispatcher-orchestrated
            -- writes); broker writes are always pod-bound.
            owner_pod_name TEXT
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_color_tenant ON execution_color(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_color_project ON execution_color(project_id)"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

#[async_trait]
impl Journal for PostgresJournal {
    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        // Single canonical row shape lives in weft-journal so the
        // dispatcher, engine, and listener all INSERT identical rows;
        // `record_with_seed` adds the execution_color seed in the
        // same transaction for ExecutionStarted.
        self.record_with_seed(event, None).await
    }

    async fn record_event_dedup(
        &self,
        event: &ExecEvent,
        dedup_key: &str,
    ) -> anyhow::Result<()> {
        self.record_with_seed(event, Some(dedup_key)).await
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

    async fn consume_suspension(&self, token: &str) -> anyhow::Result<bool> {
        // Drop the signal row entirely; resume tokens are
        // single-use. Entry triggers (is_resume=false) are NOT
        // touched here; deactivate handles those.
        let result = sqlx::query(
            "DELETE FROM signal WHERE token = $1 AND is_resume = TRUE",
        )
        .bind(token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn mint_api_token(&self, tok: &ApiToken) -> anyhow::Result<()> {
        let metadata_str = match &tok.metadata {
            Some(v) => Some(serde_json::to_string(v)?),
            None => None,
        };
        sqlx::query(
            "INSERT INTO api_token \
             (token, name, allowed_kinds, allowed_projects, allowed_tags, metadata, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&tok.token)
        .bind(&tok.name)
        .bind(&tok.allowed_kinds)
        .bind(&tok.allowed_projects)
        .bind(&tok.allowed_tags)
        .bind(metadata_str)
        .bind(crate::lease::now_unix())
        .execute(&self.pool)
        .await?;
        Ok(())
    }


    async fn get_api_token(&self, token: &str) -> anyhow::Result<Option<ApiToken>> {
        let row: Option<(
            String,
            Option<String>,
            Vec<String>,
            Vec<uuid::Uuid>,
            Vec<String>,
            Option<String>,
            i64,
        )> = sqlx::query_as(
            "SELECT token, name, allowed_kinds, allowed_projects, allowed_tags, \
                    metadata, created_at \
             FROM api_token WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(token, name, kinds, projects, tags, metadata, created_at)| ApiToken {
                token,
                name,
                allowed_kinds: kinds,
                allowed_projects: projects,
                allowed_tags: tags,
                metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
                created_at: created_at as u64,
            },
        ))
    }

    async fn list_api_tokens(&self) -> anyhow::Result<Vec<ApiToken>> {
        let rows: Vec<(
            String,
            Option<String>,
            Vec<String>,
            Vec<uuid::Uuid>,
            Vec<String>,
            Option<String>,
            i64,
        )> = sqlx::query_as(
            "SELECT token, name, allowed_kinds, allowed_projects, allowed_tags, \
                    metadata, created_at \
             FROM api_token ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(token, name, kinds, projects, tags, metadata, created_at)| ApiToken {
                token,
                name,
                allowed_kinds: kinds,
                allowed_projects: projects,
                allowed_tags: tags,
                metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
                created_at: created_at as u64,
            })
            .collect())
    }

    async fn revoke_api_token(&self, identifier: &str) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "DELETE FROM api_token WHERE token = $1 OR name = $1",
        )
        .bind(identifier)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn execution_project(&self, color: Color) -> anyhow::Result<ColorLookup<String>> {
        Ok(match self.execution_started(color).await? {
            ColorLookup::Found(ExecEvent::ExecutionStarted { project_id, .. }) => {
                ColorLookup::Found(project_id)
            }
            ColorLookup::Found(_) => ColorLookup::NotFound,
            ColorLookup::NotFound => ColorLookup::NotFound,
            ColorLookup::Corrupt => ColorLookup::Corrupt,
        })
    }

    async fn execution_definition_hash(
        &self,
        color: Color,
    ) -> anyhow::Result<ColorLookup<String>> {
        Ok(match self.execution_started(color).await? {
            ColorLookup::Found(ExecEvent::ExecutionStarted { definition_hash, .. }) => {
                ColorLookup::Found(definition_hash)
            }
            ColorLookup::Found(_) => ColorLookup::NotFound,
            ColorLookup::NotFound => ColorLookup::NotFound,
            ColorLookup::Corrupt => ColorLookup::Corrupt,
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

    async fn list_executions(&self, limit: u32) -> anyhow::Result<Vec<ExecutionSummary>> {
        // Single query: every execution_started row joined laterally
        // against its latest terminal event. Replaces the prior N+1
        // (one query per started row to look up the terminal); the
        // dashboard pulls hundreds of rows so the round-trip count
        // matters.
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT s.payload_json, t.payload_json \
             FROM exec_event s \
             LEFT JOIN LATERAL ( \
                 SELECT payload_json FROM exec_event \
                 WHERE color = s.color \
                   AND kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
                 ORDER BY id DESC LIMIT 1 \
             ) t ON TRUE \
             WHERE s.kind = 'execution_started' \
             ORDER BY s.id DESC LIMIT $1",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for (started_payload, terminal_payload) in rows {
            let Ok(started) = serde_json::from_str::<ExecEvent>(&started_payload) else {
                continue;
            };
            let ExecEvent::ExecutionStarted {
                color, project_id, entry_node, at_unix, ..
            } = started else {
                continue;
            };
            // The LATERAL subquery's WHERE filter only selects
            // execution_{completed,failed,cancelled} rows, so any
            // other variant landing here means the journal row was
            // corrupted post-write. Surface that loudly: returning a
            // "running" placeholder would let the dashboard show a
            // terminal execution as live.
            let (status, completed_at) = match terminal_payload {
                None => ("running".into(), None),
                Some(p) => match serde_json::from_str::<ExecEvent>(&p)? {
                    ExecEvent::ExecutionCompleted { at_unix, .. } => {
                        ("completed".into(), Some(at_unix))
                    }
                    ExecEvent::ExecutionFailed { at_unix, .. } => {
                        ("failed".into(), Some(at_unix))
                    }
                    ExecEvent::ExecutionCancelled { at_unix, .. } => {
                        ("cancelled".into(), Some(at_unix))
                    }
                    other => anyhow::bail!(
                        "list_executions: terminal lookup returned non-terminal \
                         event for color {color}: {other:?}"
                    ),
                },
            };
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

    async fn list_non_terminal_colors_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<Color>> {
        // A color is "non-terminal" iff it belongs to this project
        // AND has no terminal event in the journal. `execution_color`
        // is the denormalized (color, project_id) index seeded at
        // ExecutionStarted time, so we get the project filter as an
        // indexed equality lookup rather than parsing JSON. NOT
        // EXISTS lets Postgres short-circuit per row.
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT ec.color FROM execution_color ec \
             WHERE ec.project_id = $1 \
               AND NOT EXISTS ( \
                   SELECT 1 FROM exec_event t \
                   WHERE t.color = ec.color \
                     AND t.kind IN ('execution_completed', \
                                    'execution_failed', \
                                    'execution_cancelled') \
               )",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (c,) in rows {
            let color: Color = c
                .parse()
                .map_err(|e| anyhow::anyhow!("bad color in execution_color: {e}"))?;
            out.push(color);
        }
        Ok(out)
    }

    async fn delete_execution(&self, color: Color) -> anyhow::Result<()> {
        let s = color.to_string();
        sqlx::query("DELETE FROM exec_event WHERE color = $1")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        // Resume tokens for this color: signal rows with is_resume=true.
        sqlx::query(
            "DELETE FROM signal WHERE color = $1 AND is_resume = TRUE",
        )
        .bind(&s)
        .execute(&self.pool)
        .await?;
        // execution_color is the denormalized (color, project_id,
        // tenant_id) index seeded at ExecutionStarted time. Without
        // this delete, the row outlives the journal it indexes and
        // `list_non_terminal_colors_for_project` keeps returning the
        // cleaned color forever as "non-terminal" (the NOT EXISTS
        // terminal-event check passes vacuously when all events are
        // gone). Wipe / cancel_running then re-sweep a ghost.
        sqlx::query("DELETE FROM execution_color WHERE color = $1")
            .bind(&s)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn signal_insert(&self, sig: &SignalRegistration) -> anyhow::Result<()> {
        let payload_str = match &sig.consumer_payload {
            Some(v) => Some(serde_json::to_string(v)?),
            None => None,
        };
        // Entry rows (is_resume=false) reuse the existing token
        // across reactivates: the register_signal task looks up
        // the existing token for (project_id, node_id, is_resume=
        // FALSE) before calling INSERT, so the conflict path just
        // refreshes spec_json/mount_path/auth_*/consumer_payload
        // on the same row. Parked_payload from before the
        // reactivate drains cleanly because the token didn't
        // change. Resume rows (is_resume=TRUE) always insert
        // fresh: their token is per-suspension.
        sqlx::query(
            "INSERT INTO signal \
             (token, tenant_id, project_id, color, node_id, is_resume, \
              spec_json, created_at, consumer_kind, tags, consumer_payload, \
              surface_kind, mount_path, auth_kind, auth_config, kind_state) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) \
             ON CONFLICT (token) DO UPDATE SET \
                 spec_json = EXCLUDED.spec_json, \
                 consumer_kind = EXCLUDED.consumer_kind, \
                 tags = EXCLUDED.tags, \
                 consumer_payload = EXCLUDED.consumer_payload, \
                 surface_kind = EXCLUDED.surface_kind, \
                 mount_path = EXCLUDED.mount_path, \
                 auth_kind = EXCLUDED.auth_kind, \
                 auth_config = EXCLUDED.auth_config, \
                 kind_state = EXCLUDED.kind_state",
        )
        .bind(&sig.token)
        .bind(&sig.tenant_id)
        .bind(&sig.project_id)
        .bind(sig.color.map(|c| c.to_string()))
        .bind(&sig.node_id)
        .bind(sig.is_resume)
        .bind(&sig.spec_json)
        .bind(crate::lease::now_unix())
        .bind(sig.consumer_kind.as_deref())
        .bind(&sig.tags)
        .bind(payload_str)
        .bind(&sig.surface_kind)
        .bind(sig.mount_path.as_deref())
        .bind(&sig.auth_kind)
        .bind(sig.auth_config.as_ref())
        .bind(&sig.kind_state)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<SignalRow> = sqlx::query_as(SIGNAL_SELECT_WHERE_TOKEN)
            .bind(token)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(row_to_signal))
    }

    async fn signal_remove_many(
        &self,
        tokens: &[String],
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_TOKENS_RETURNING)
            .bind(tokens)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(row_to_signal).collect())
    }

    async fn signal_list_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_SELECT_WHERE_PROJECT)
            .bind(project_id)
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
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_COLOR_RETURNING)
            .bind(color.to_string())
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(row_to_signal).collect())
    }

    async fn signal_remove_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_PROJECT_RETURNING)
            .bind(project_id)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(row_to_signal).collect())
    }
}

const SIGNAL_SELECT_WHERE_TOKEN: &str =
    "SELECT token, tenant_id, project_id, color, node_id, is_resume, \
     spec_json, consumer_kind, tags, consumer_payload, \
     surface_kind, mount_path, auth_kind, auth_config, kind_state \
     FROM signal WHERE token = $1";

const SIGNAL_SELECT_WHERE_PROJECT: &str =
    "SELECT token, tenant_id, project_id, color, node_id, is_resume, \
     spec_json, consumer_kind, tags, consumer_payload, \
     surface_kind, mount_path, auth_kind, auth_config, kind_state \
     FROM signal WHERE project_id = $1";

const SIGNAL_DELETE_BY_COLOR_RETURNING: &str =
    "DELETE FROM signal WHERE color = $1 RETURNING token, tenant_id, project_id, color, \
     node_id, is_resume, spec_json, consumer_kind, tags, \
     consumer_payload, surface_kind, mount_path, \
     auth_kind, auth_config, kind_state";

const SIGNAL_DELETE_BY_PROJECT_RETURNING: &str =
    "DELETE FROM signal WHERE project_id = $1 RETURNING token, tenant_id, project_id, color, \
     node_id, is_resume, spec_json, consumer_kind, tags, \
     consumer_payload, surface_kind, mount_path, \
     auth_kind, auth_config, kind_state";

const SIGNAL_DELETE_BY_TOKENS_RETURNING: &str =
    "DELETE FROM signal WHERE token = ANY($1) RETURNING token, tenant_id, project_id, color, \
     node_id, is_resume, spec_json, consumer_kind, tags, \
     consumer_payload, surface_kind, mount_path, \
     auth_kind, auth_config, kind_state";

/// Row shape for signal SELECTs. `FromRow` (not a tuple) because
/// the row exceeds sqlx's 16-tuple cap.
#[derive(sqlx::FromRow)]
struct SignalRow {
    token: String,
    tenant_id: String,
    project_id: String,
    color: Option<String>,
    node_id: String,
    is_resume: bool,
    spec_json: String,
    consumer_kind: Option<String>,
    tags: Vec<String>,
    consumer_payload: Option<String>,
    surface_kind: String,
    mount_path: Option<String>,
    auth_kind: String,
    auth_config: Option<serde_json::Value>,
    kind_state: serde_json::Value,
}

fn row_to_signal(row: SignalRow) -> SignalRegistration {
    let color = row.color.and_then(|s| s.parse::<Color>().ok());
    let consumer_payload = row
        .consumer_payload
        .and_then(|s| serde_json::from_str(&s).ok());
    SignalRegistration {
        token: row.token,
        tenant_id: row.tenant_id,
        project_id: row.project_id,
        color,
        node_id: row.node_id,
        is_resume: row.is_resume,
        spec_json: row.spec_json,
        consumer_kind: row.consumer_kind,
        tags: row.tags,
        consumer_payload,
        surface_kind: row.surface_kind,
        mount_path: row.mount_path,
        auth_kind: row.auth_kind,
        auth_config: row.auth_config,
        kind_state: row.kind_state,
    }
}

// `crate::lease::now_unix` is the canonical wall-clock reader.
