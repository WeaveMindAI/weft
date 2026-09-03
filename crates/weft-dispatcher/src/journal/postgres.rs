//! Postgres-backed journal. Multiple dispatcher Pods share one
//! Postgres database; the event log + token lookup tables are the
//! durable state. Postgres is the source of truth, every dispatcher
//! Pod is a stateless reader/writer.
//!
//! Applies no schema of its own: the boot's `app::apply_core_schema`
//! runs every group (this crate's [`GROUP`] included) in one pass.

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};

use weft_core::Color;

use weft_journal::{decode_event, ExecEvent};
use crate::journal::{
    SignalToken, ColorLookup, ExecutionOwner, ExecutionPage, ExecutionQuery, ExecutionSummary,
    Journal, LogEntry, SignalRegistration,
};

pub struct PostgresJournal {
    pool: PgPool,
}

/// Turn a `(started_payload, terminal_payload)` pair (an `execution_started`
/// event JSON + its latest terminal event JSON, if any) into an
/// `ExecutionSummary`. Shared by `list_executions` and `execution_summary` so
/// the started-decode + terminal-status mapping lives in exactly one place.
/// `Err` (naming the color and `weft clean`) for any unusable row: one
/// that does not decode, or one whose kind column and payload disagree.
/// The caller decides whether that fails the read or renders as a
/// broken row.
fn summary_from_payloads(
    color: Color,
    started_payload: &str,
    terminal_payload: Option<String>,
) -> anyhow::Result<ExecutionSummary> {
    let started = decode_event(color, started_payload).map_err(anyhow::Error::msg)?;
    let ExecEvent::ExecutionStarted {
        color, project_id, entry_node, at_unix, ..
    } = started
    else {
        // The row was selected by kind = 'execution_started', so a
        // decodable non-started payload means the kind column and the
        // payload disagree: corrupted post-write, same as undecodable.
        anyhow::bail!(
            "exec_event row for color {color} is kind execution_started but decodes \
             to a different event; `weft clean {color}` removes this color's rows"
        );
    };
    // The terminal lookup only selects execution_{completed,failed,cancelled}
    // rows, so any other variant here means the journal row was corrupted
    // post-write. Surface that loudly: a "running" placeholder would show a
    // terminal execution as live.
    let (status, completed_at) = match terminal_payload {
        None => ("running".to_string(), None),
        Some(p) => match decode_event(color, &p).map_err(anyhow::Error::msg)? {
            ExecEvent::ExecutionCompleted { at_unix, .. } => ("completed".to_string(), Some(at_unix)),
            ExecEvent::ExecutionFailed { at_unix, .. } => ("failed".to_string(), Some(at_unix)),
            ExecEvent::ExecutionCancelled { at_unix, .. } => ("cancelled".to_string(), Some(at_unix)),
            other => anyhow::bail!(
                "execution summary: terminal lookup returned non-terminal event \
                 for color {color}: {other:?}"
            ),
        },
    };
    Ok(ExecutionSummary {
        color,
        project_id,
        entry_node,
        status,
        started_at: at_unix,
        completed_at,
    })
}

/// Every payload for `color`, in write order, undecoded. Takes the
/// executor so both the pool reads and the in-transaction cancel read
/// share one query.
async fn payload_rows<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
    color: Color,
) -> anyhow::Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT payload_json FROM exec_event WHERE color = $1 ORDER BY id ASC",
    )
    .bind(color.to_string())
    .fetch_all(executor)
    .await?;
    Ok(rows.into_iter().map(|(p,)| p).collect())
}

/// Strictly decode every row: one undecodable row fails the whole read
/// (a fold over a partial event list rebuilds a state that never
/// existed).
fn decode_all(color: Color, rows: Vec<String>) -> anyhow::Result<Vec<ExecEvent>> {
    rows.into_iter()
        .map(|payload| decode_event(color, &payload).map_err(anyhow::Error::msg))
        .collect()
}

impl PostgresJournal {
    /// A connected pool for `database_url`
    /// (`postgres://user:pass@host:port/db`). Retries the initial
    /// connection for up to 60s so a Pod that boots before Postgres is
    /// ready doesn't crash-loop. Applies NO schema: the boot applies
    /// every group in one pass ([`crate::app::apply_core_schema`]), so
    /// every pending migration across every group runs in one global id
    /// order rather than this crate's group jumping the queue.
    pub async fn connect_pool(database_url: &str) -> anyhow::Result<PgPool> {
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
        Ok(pool)
    }

    /// Wrap an EXISTING pool. Pure: the schema (this crate's [`GROUP`]
    /// included) is applied once, before construction, by
    /// [`crate::app::apply_core_schema`]; a second application here
    /// would run this group's pending migrations ahead of every other
    /// group's, breaking the one-global-id-order guarantee.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Expose the inner pool for sibling modules (lease manager,
    /// EventBus pub/sub) so they can share connections.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The color's first `ExecutionStarted` event, decoded. The ONE
    /// fetch behind `execution_definition_hash` (`execution_project`
    /// and `execution_tenant` read the `execution_color` mirror
    /// instead, so they survive a corrupt payload). A row whose JSON no longer
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
        if !matches!(event, ExecEvent::ExecutionStarted { .. }) {
            return weft_journal::record_event_in(&self.pool, event, None, dedup_key)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"));
        }
        let mut tx = self.pool.begin().await?;
        Self::write_started_in(&mut tx, event, dedup_key).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Write an `ExecutionStarted` event AND its `execution_color` seed on the
    /// caller's transaction (the two must commit together; see
    /// `record_with_seed`'s doc). A missing project row fails the whole write
    /// loudly instead of silently journaling an unsweepable execution.
    async fn write_started_in(
        tx: &mut sqlx::PgConnection,
        event: &ExecEvent,
        dedup_key: Option<&str>,
    ) -> anyhow::Result<()> {
        let ExecEvent::ExecutionStarted { color, project_id, at_unix, phase, node_test, .. } =
            event
        else {
            anyhow::bail!("write_started_in requires an ExecutionStarted event");
        };
        weft_journal::record_event_in(&mut *tx, event, None, dedup_key)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let rows = sqlx::query(
            "INSERT INTO execution_color (color, project_id, tenant_id, started_at_unix, phase, kind) \
             SELECT $1, $2, p.tenant_id, $3, $4, $5 FROM project p WHERE p.id = $2::uuid \
             ON CONFLICT (color) DO NOTHING",
        )
        .bind(color.to_string())
        .bind(project_id)
        .bind(*at_unix as i64)
        .bind(phase.as_str())
        .bind(if *node_test { "node_test" } else { "execution" })
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
        Ok(())
    }

    /// The one-transaction execution BIRTH: `ExecutionStarted` + seed + the
    /// entry kicks. Shared by `start_execution` and `start_live_execution`.
    async fn write_birth_in(
        tx: &mut sqlx::PgConnection,
        start: &ExecEvent,
        kicks: &[ExecEvent],
    ) -> anyhow::Result<()> {
        Self::write_started_in(tx, start, None).await?;
        for kick in kicks {
            weft_journal::record_event_in(&mut *tx, kick, None, None)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        Ok(())
    }
}

/// The journal's schema. First in `app::ALL_GROUPS` (the `worker_pod`
/// group's fencing trigger attaches to `exec_event`); applied by the
/// boot's one `apply_core_schema` pass, never here.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "exec_event",
    tables: &["exec_event", "signal_token", "signal", "execution_color"],
    ddl: &[
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
        // signal_token: token-scoped enumeration credential. Allow
        // sets are TEXT[] arrays so parameterized binding gives no
        // SQL-injection surface and the filter SQL is a single `&&`
        // (overlap) clause per scope dimension. Empty array on any
        // column = wildcard (matches everything).
        r#"CREATE TABLE IF NOT EXISTS signal_token (
            id UUID PRIMARY KEY,
            -- sha256 hex of the full token value. The raw value is NEVER
            -- stored (show-once): lookups hash the presented credential.
            token_hash TEXT NOT NULL UNIQUE,
            -- Display prefix ("wft-<word>-…") so lists can tell tokens apart.
            recognizer TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            name TEXT,
            allowed_projects UUID[] NOT NULL DEFAULT '{}',
            allowed_tags TEXT[] NOT NULL DEFAULT '{}',
            created_at BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_token_tenant ON signal_token(tenant_id)"#,
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
            -- The connection this signal acts as (`spec.access.id`),
            -- denormalized at register time. NULL for kinds without
            -- one. Inbound provider pushes route account-to-signal
            -- on this column, so the match is one indexed filter
            -- instead of a spec-parsing scan.
            access_id TEXT,
            created_at BIGINT NOT NULL,
            -- Opaque per-kind state persisted at register time and
            -- read back at rehydrate time. Empty for most kinds.
            -- Timer uses it to persist absolute next_fire_at_unix_ms
            -- for After-style schedules so a listener restart
            -- doesn't reset the clock. Future stateful kinds (e.g.
            -- SSE last-event-id, socket reconnect token) use the
            -- same column.
            kind_state JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- The kind_state write fence: a strictly-increasing
            -- per-signal version stamped by the holder's durable
            -- cursor writes. An update whose seq is not above the
            -- stored one is dropped (the newer state stands), and a
            -- register that carries prior state forward loses to any
            -- newer in-flight write the same way. A REAL column, so
            -- the version can never be lost by handling the state
            -- blob (the blob stays purely the kind's own state).
            kind_state_seq BIGINT NOT NULL DEFAULT 0,
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
            -- The trigger's delivered port values at registration time
            -- (entry signals only). Replayed onto the trigger's ports at
            -- every fire: a trigger's inputs are whatever they were at
            -- trigger setup.
            port_snapshot JSONB,
            consumer_payload TEXT,
            surface_kind TEXT NOT NULL DEFAULT 'task_callback',
            mount_path TEXT,
            auth_kind TEXT NOT NULL DEFAULT 'none',
            auth_config JSONB,
            -- Placement: which pooled listener pod currently holds this
            -- signal's live in-RAM registry entry (its Timer/SSE loop).
            -- NULL when no listener holds it yet (freshly registered
            -- before placement, or the holding pod died and it awaits
            -- re-placement). The fire path resolves token -> this pod's
            -- admin URL; boot/rehydrate lists `WHERE listener_pod = me`
            -- to rebuild a restarted pod's registry. A pooled listener
            -- holds many tenants' signals, so placement is per-signal,
            -- not per-tenant.
            listener_pod TEXT,
            -- Monotonic placement generation, bumped on EVERY (re)placement
            -- (set_placement). The holding pod is told its generation at
            -- register time and stamps it on every held-event fire it
            -- enqueues. A move registers the signal on the new pod under
            -- gen+1 BEFORE unregistering the old pod, so during the brief
            -- both-armed overlap the old pod still fires under the OLD gen.
            -- The broker rejects any FireSignal whose generation is below
            -- the row's current generation: the stale (old-pod) fire from
            -- the overlap is dropped, the new pod's fire passes. This is
            -- the fence that prevents a self-firing kind (Timer/SSE) from
            -- double-firing across a scale-down move.
            placement_generation BIGINT NOT NULL DEFAULT 0
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_tenant ON signal(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_listener_pod
             ON signal(listener_pod) WHERE listener_pod IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_project ON signal(project_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_color ON signal(color)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_consumer_kind ON signal(consumer_kind)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_access_id ON signal(access_id)
           WHERE access_id IS NOT NULL"#,
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
            -- Worker pod that owns this color's writes. NULL until the
            -- first worker claims a color-bearing task (the broker
            -- stamps it in task_claim_one); thereafter it is the pod of
            -- the LATEST claimer. The broker rejects any journal_record
            -- whose caller.pod_name doesn't match, so a compromised
            -- worker can only journal under its own bound pod, not
            -- cross-write sibling executions in the same tenant.
            --
            -- "Latest claimer wins" is how a resume hands ownership to a
            -- new pod when the original is gone: the resume task is
            -- pinned to the original owner if it is still alive (so only
            -- it reclaims and ownership stays stable), and spawns + pins
            -- to a fresh pod only when the owner is dead (so the handoff
            -- is the ONLY time ownership moves). Without that pinning a
            -- fresh worker could steal a live owner's color mid-flight
            -- now that a project can run more than one worker; see
            -- `task_kinds::execute::enqueue_resume`.
            -- NULL also covers dispatcher-orchestrated writes (no pod).
            owner_pod_name TEXT,
            -- What this color IS: 'execution' (a project run; the
            -- project-lifecycle sweeps, cancel, wipe, and drain
            -- counting operate on these) or 'node_test' (a node
            -- self-test's identity: real cost attribution and broker
            -- scoping, but its lifecycle is owned by its task, so the
            -- project sweeps must never cancel it or wait on it).
            kind TEXT NOT NULL DEFAULT 'execution'
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_color_tenant ON execution_color(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_color_project ON execution_color(project_id)"#,
    ],
    seed: &[],
};

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

    async fn events_log_lossy(
        &self,
        color: Color,
    ) -> anyhow::Result<(Vec<ExecEvent>, Vec<String>)> {
        let rows = payload_rows(&self.pool, color).await?;
        let mut out = Vec::with_capacity(rows.len());
        let mut bad = Vec::new();
        for payload in rows {
            match decode_event(color, &payload) {
                Ok(ev) => out.push(ev),
                Err(reason) => bad.push(reason),
            }
        }
        Ok((out, bad))
    }

    async fn start_execution(
        &self,
        start: &ExecEvent,
        kicks: &[ExecEvent],
        task: weft_task_store::tasks::NewTask,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        // Enqueue FIRST and only write the birth on a FRESH insert, exactly
        // like `start_live_execution` gates on `Admitted`: "one birth per
        // color" holds by construction even if a caller ever replays a color
        // (the dedup'd task collapses, and the birth is not double-written;
        // ExecutionStarted itself carries no dedup key).
        let outcome = weft_task_store::tasks::enqueue_dedup_in(&mut tx, task).await?;
        if matches!(outcome, weft_task_store::tasks::DedupOutcome::Inserted(_)) {
            Self::write_birth_in(&mut tx, start, kicks).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn start_live_execution(
        &self,
        start: &ExecEvent,
        kicks: &[ExecEvent],
        task: weft_task_store::tasks::NewTask,
        saturation: f64,
    ) -> anyhow::Result<weft_task_store::tasks::LiveAdmitOutcome> {
        use weft_task_store::tasks::LiveAdmitOutcome;
        let mut tx = self.pool.begin().await?;
        let outcome =
            weft_task_store::tasks::admit_live_execution_in(&mut tx, &task, saturation).await?;
        // Only a FRESH admission births the execution: `Saturated` wrote
        // nothing (the caller retries), and `AlreadyAdmitted`'s original
        // admission already committed the birth.
        if matches!(outcome, LiveAdmitOutcome::Admitted(_)) {
            Self::write_birth_in(&mut tx, start, kicks).await?;
        }
        tx.commit().await?;
        Ok(outcome)
    }

    async fn cancel_never_claimed_execution(
        &self,
        color: Color,
        reason: &str,
    ) -> anyhow::Result<weft_task_store::tasks::SetupFailureOutcome> {
        use weft_task_store::tasks::SetupFailureOutcome;
        let mut tx = self.pool.begin().await?;
        let outcome =
            weft_task_store::tasks::delete_pending_live_execution_in(&mut tx, &color.to_string())
                .await?;
        if outcome == SetupFailureOutcome::WorkerOwnsIt {
            tx.commit().await?;
            return Ok(outcome);
        }
        // No worker will ever run this color: journal the cancel terminals in
        // the SAME transaction as the task delete, so "task deleted" and
        // "cancel journaled" can never disagree. Per-node cancels land BEFORE
        // ExecutionCancelled (same ordering rule as the dispatcher's cancel
        // catch-up writer: a terminal-first partial write would make a retry
        // skip the per-node rows forever). A payload that fails to decode
        // fails the whole read, matching `events_log`.
        let events = decode_all(color, payload_rows(&mut *tx, color).await?)?;
        let has_terminal = events.iter().any(ExecEvent::is_execution_terminal);
        if !has_terminal {
            // The write list comes from the ONE shared definition of a
            // dispatcher-side cancel (`cancel_terminal_events`), so this
            // transactional writer and the retrying trait-based writer
            // (`journal_cancel_terminals`) can never drift.
            let now = crate::lease::now_unix() as u64;
            for (event, dedup) in
                crate::api::execution::cancel_terminal_events(color, &events, reason, now)
            {
                weft_journal::record_event_in(&mut *tx, &event, None, Some(&dedup))
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }
        }
        tx.commit().await?;
        Ok(SetupFailureOutcome::NoWorkerWillRun)
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

    async fn mint_signal_token(&self, tok: &SignalToken) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO signal_token \
             (id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(tok.id)
        .bind(&tok.token_hash)
        .bind(&tok.recognizer)
        .bind(&tok.tenant_id)
        .bind(&tok.name)
        .bind(&tok.allowed_projects)
        .bind(&tok.allowed_tags)
        // Store the caller-stamped mint time verbatim (the handler set it from
        // the canonical clock), so postgres and the mock agree.
        .bind(tok.created_at as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }


    async fn get_signal_token(&self, token_hash: &str) -> anyhow::Result<Option<SignalToken>> {
        let row: Option<SignalTokenRow> = sqlx::query_as(
            "SELECT id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
                    created_at \
             FROM signal_token WHERE token_hash = $1",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_signal_token).transpose()
    }

    async fn list_signal_tokens(&self, tenant: &str) -> anyhow::Result<Vec<SignalToken>> {
        let rows: Vec<SignalTokenRow> = sqlx::query_as(
            "SELECT id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
                    created_at \
             FROM signal_token WHERE tenant_id = $1 ORDER BY created_at DESC",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_signal_token).collect()
    }

    async fn revoke_signal_token(&self, id: uuid::Uuid, tenant: &str) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "DELETE FROM signal_token WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id)
        .bind(tenant)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn execution_owner(&self, color: Color) -> anyhow::Result<Option<ExecutionOwner>> {
        // One read of the `execution_color` row, whose project and
        // tenant were stamped together in the SAME transaction as
        // `ExecutionStarted` (see `write_birth_in`). Never re-derived:
        // not from the started payload (undecodable exactly when
        // `weft clean` is the way out), and not from the project store
        // (deletable, and an execution deliberately outlives its
        // project).
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT project_id, tenant_id FROM execution_color WHERE color = $1")
                .bind(color.to_string())
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(project_id, tenant)| ExecutionOwner { project_id, tenant }))
    }

    async fn execution_definition_hash(
        &self,
        color: Color,
    ) -> anyhow::Result<ColorLookup<String>> {
        Ok(match self.execution_started(color).await? {
            // A definition-less start (a node self-test) answers
            // NotFound: nothing may resume against it, and the
            // caller's existing unknown-color bail is the loud path.
            ColorLookup::Found(ExecEvent::ExecutionStarted {
                definition_hash: Some(hash),
                ..
            }) => ColorLookup::Found(hash),
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

    async fn list_executions(
        &self,
        tenant: &str,
        query: &ExecutionQuery,
    ) -> anyhow::Result<ExecutionPage> {
        // One SQL statement: every `execution_started` row for this tenant (the
        // EXISTS-style JOIN against `execution_color` keeps the tenant wall in
        // SQL), narrowed by the optional project + start-time filters, joined
        // laterally against its latest terminal event, newest first, with
        // limit/offset paging. A parallel COUNT over the same filters gives the
        // total so a consumer can render page controls. Filters + paging live in
        // SQL so a tenant with a huge history never truncates blindly.
        //
        // Bind order is fixed ($1 tenant, $2 project filter, $3 after, $4 before)
        // and every optional filter is a `($n IS NULL OR ...)` clause so one
        // prepared statement serves every filter combination.
        // The `execution_color` row (seeded at start) carries the real, indexed
        // columns the filters key on: `tenant_id` (the wall), `project_id`, and
        // `started_at_unix`. `exec_event` only has `color`/`kind`/`payload_json`,
        // so we filter on `ec` and fetch the started `payload_json` from the
        // matching `execution_started` event.
        let project = query.project_id.as_deref();
        let after = query.started_after.map(|v| v as i64);
        let before = query.started_before.map(|v| v as i64);
        // PROJECT EXECUTIONS only: this list is the user's record of
        // their project running. A node-test color is a real identity
        // (its cost trail is addressed by color from the test report),
        // but it has no definition, no graph, and no resume, so it
        // never belongs in this listing.
        let where_clause = "ec.tenant_id = $1 \
             AND ec.kind = 'execution' \
             AND ($2::text IS NULL OR ec.project_id = $2) \
             AND ($3::bigint IS NULL OR ec.started_at_unix >= $3) \
             AND ($4::bigint IS NULL OR ec.started_at_unix < $4)";

        // The count carries the SAME started-event predicate as the row
        // query's inner lateral join: a seeded `execution_color` row
        // with no `execution_started` event can never be listed, so it
        // must not be counted either, else `total` promises rows the
        // pages cannot produce.
        let total: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM execution_color ec WHERE {where_clause} \
             AND EXISTS ( \
                 SELECT 1 FROM exec_event \
                 WHERE color = ec.color AND kind = 'execution_started' \
             )"
        ))
        .bind(tenant)
        .bind(project)
        .bind(after)
        .bind(before)
        .fetch_one(&self.pool)
        .await?;

        let rows: Vec<(String, String, i64, String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT ec.color, ec.project_id, ec.started_at_unix, \
                    s.payload_json, t.payload_json \
             FROM execution_color ec \
             JOIN LATERAL ( \
                 SELECT payload_json FROM exec_event \
                 WHERE color = ec.color AND kind = 'execution_started' \
                 ORDER BY id ASC LIMIT 1 \
             ) s ON TRUE \
             LEFT JOIN LATERAL ( \
                 SELECT payload_json FROM exec_event \
                 WHERE color = ec.color \
                   AND kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
                 ORDER BY id DESC LIMIT 1 \
             ) t ON TRUE \
             WHERE {where_clause} \
             ORDER BY ec.started_at_unix DESC, ec.color DESC LIMIT $5 OFFSET $6"
        ))
        .bind(tenant)
        .bind(project)
        .bind(after)
        .bind(before)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut executions = Vec::with_capacity(rows.len());
        for (color_text, project_id, started_at, started_payload, terminal_payload) in rows {
            let color: Color = color_text.parse().map_err(|e| {
                anyhow::anyhow!("execution_color row holds a non-uuid color '{color_text}': {e}")
            })?;
            // One corrupt row must not take the whole list down (the
            // list is also the door to `weft clean`, the recovery for
            // exactly this state), and it must not vanish either: the
            // count includes it, so the page renders it as a broken
            // row (inspectable via replay, deletable).
            match summary_from_payloads(color, &started_payload, terminal_payload) {
                Ok(summary) => executions.push(summary),
                Err(e) => {
                    tracing::error!(
                        target: "weft_dispatcher::journal",
                        %color, error = %e,
                        "execution row does not decode; listed as corrupt"
                    );
                    executions.push(ExecutionSummary {
                        color,
                        project_id,
                        entry_node: String::new(),
                        status: "corrupt".to_string(),
                        started_at: started_at as u64,
                        completed_at: None,
                    });
                }
            }
        }
        Ok(ExecutionPage { executions, total: total.0.max(0) as u64 })
    }

    async fn execution_summary(
        &self,
        color: Color,
    ) -> anyhow::Result<Option<ExecutionSummary>> {
        // Direct point-lookup by color: the started row plus its latest terminal
        // event, no windowed list scan. Returns None when the color has no
        // `execution_started` row.
        let row: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT s.payload_json, t.payload_json \
             FROM exec_event s \
             LEFT JOIN LATERAL ( \
                 SELECT payload_json FROM exec_event \
                 WHERE color = s.color \
                   AND kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
                 ORDER BY id DESC LIMIT 1 \
             ) t ON TRUE \
             WHERE s.kind = 'execution_started' AND s.color = $1 \
             ORDER BY s.id ASC LIMIT 1",
        )
        .bind(color.to_string())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            None => Ok(None),
            Some((started_payload, terminal_payload)) => {
                summary_from_payloads(color, &started_payload, terminal_payload).map(Some)
            }
        }
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
        // EXISTS lets Postgres short-circuit per row. PROJECT
        // EXECUTIONS only (`kind = 'execution'`): this is the one
        // read behind the cancel/wipe sweeps and the drain count, and
        // a node-test color's lifecycle is owned by its task, never by
        // the project's.
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT ec.color FROM execution_color ec \
             WHERE ec.project_id = $1 \
               AND ec.kind = 'execution' \
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

    async fn list_terminal_colors_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<std::collections::HashSet<Color>> {
        // The complement of the non-terminal query: colors with a
        // terminal event. Distinct because a color has one terminal
        // event but the join could otherwise repeat it.
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT ec.color FROM execution_color ec \
             WHERE ec.project_id = $1 \
               AND EXISTS ( \
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
        let mut out = std::collections::HashSet::with_capacity(rows.len());
        for (c,) in rows {
            let color: Color = c
                .parse()
                .map_err(|e| anyhow::anyhow!("bad color in execution_color: {e}"))?;
            out.insert(color);
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

    async fn signal_insert(
        &self,
        sig: &SignalRegistration,
        placement: &crate::journal::SignalPlacement,
    ) -> anyhow::Result<()> {
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
        // `listener_pod` + `placement_generation` are written WITH the
        // row (not a later separate UPDATE) so a committed `signal` row
        // never has a NULL holder while a pod already holds it in RAM,
        // and so a register that FAILS before this insert leaves the row
        // untouched (the generation is computed by `next_generation`, a
        // pure read, and committed ONLY here). On reactivate (ON CONFLICT)
        // this is the first and only write of the new generation
        // (`prior + 1`) for this placement, applied together with the new
        // holder.
        //
        // The write runs under the per-pod advisory lock
        // (`listener::pod_lock_key`) and is guarded on the pod's
        // `listener_pod` registry row still existing: the idle reaper
        // deletes that row under the SAME lock with a none-placed
        // re-check, so exactly one of {this stamp, that reap} wins and a
        // signal can never be committed pointing at a reaped pod (which
        // nothing would ever fire). A zero-row result means the pod was
        // reaped between placement's pick and this write; fail loud, the
        // register task's retry re-places onto a live pod.
        //
        // kind_state on conflict is seq-FENCED: the register carries
        // the prior state forward at the seq it read, and a cursor
        // write that landed in between carries a higher seq, so the
        // reactivate loses to it instead of rewinding a live cursor.
        // Equal seq = the register's (possibly recomputed) state wins,
        // which is the deliberate overwrite (a timer's fresh schedule).
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(crate::listener::pod_lock_key(&placement.listener_pod))
            .execute(&mut *tx)
            .await?;
        let res = sqlx::query(
            "INSERT INTO signal \
             (token, tenant_id, project_id, color, node_id, is_resume, \
              spec_json, access_id, created_at, consumer_kind, tags, port_snapshot, \
              consumer_payload, \
              surface_kind, mount_path, auth_kind, auth_config, kind_state, \
              kind_state_seq, listener_pod, placement_generation) \
             SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, \
                    $17, $18, $19, $20, $21 \
             WHERE EXISTS (SELECT 1 FROM listener_pod WHERE pod_name = $20) \
             ON CONFLICT (token) DO UPDATE SET \
                 spec_json = EXCLUDED.spec_json, \
                 access_id = EXCLUDED.access_id, \
                 consumer_kind = EXCLUDED.consumer_kind, \
                 tags = EXCLUDED.tags, \
                 port_snapshot = EXCLUDED.port_snapshot, \
                 consumer_payload = EXCLUDED.consumer_payload, \
                 surface_kind = EXCLUDED.surface_kind, \
                 mount_path = EXCLUDED.mount_path, \
                 auth_kind = EXCLUDED.auth_kind, \
                 auth_config = EXCLUDED.auth_config, \
                 kind_state = CASE \
                     WHEN signal.kind_state_seq <= EXCLUDED.kind_state_seq \
                         THEN EXCLUDED.kind_state \
                     ELSE signal.kind_state END, \
                 kind_state_seq = GREATEST(signal.kind_state_seq, EXCLUDED.kind_state_seq), \
                 listener_pod = EXCLUDED.listener_pod, \
                 placement_generation = EXCLUDED.placement_generation",
        )
        .bind(&sig.token)
        .bind(&sig.tenant_id)
        .bind(&sig.project_id)
        .bind(sig.color.map(|c| c.to_string()))
        .bind(&sig.node_id)
        .bind(sig.is_resume)
        .bind(&sig.spec_json)
        .bind(sig.access_id.as_deref())
        .bind(crate::lease::now_unix())
        .bind(sig.consumer_kind.as_deref())
        .bind(&sig.tags)
        .bind(sig.port_snapshot.as_ref())
        .bind(payload_str)
        .bind(&sig.surface_kind)
        .bind(sig.mount_path.as_deref())
        .bind(&sig.auth_kind)
        .bind(sig.auth_config.as_ref())
        .bind(&sig.kind_state)
        .bind(sig.kind_state_seq)
        .bind(&placement.listener_pod)
        .bind(placement.generation)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        if res.rows_affected() == 0 {
            anyhow::bail!(
                "signal '{}' could not be placed: listener pod '{}' was reaped between \
                 placement and this write; the register retry places onto a live pod",
                sig.token,
                placement.listener_pod
            );
        }
        Ok(())
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<SignalRow> = sqlx::query_as(SIGNAL_SELECT_WHERE_TOKEN)
            .bind(token)
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_to_signal).transpose()
    }

    async fn signal_update_kind_state(
        &self,
        token: &str,
        kind_state: &serde_json::Value,
        seq: i64,
        placement_generation: i64,
    ) -> anyhow::Result<bool> {
        // Both fences live in the WHERE so the write is atomic (no
        // read-then-write window); the seq is a real column, so the
        // state blob stays purely the kind's own state.
        let res = sqlx::query(
            "UPDATE signal
             SET kind_state = $2::jsonb, kind_state_seq = $3
             WHERE token = $1
               AND placement_generation <= $4
               AND kind_state_seq < $3",
        )
        .bind(token)
        .bind(kind_state)
        .bind(seq)
        .bind(placement_generation)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
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
        rows.into_iter().map(row_to_signal).collect()
    }

    async fn signal_list_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_SELECT_WHERE_PROJECT)
            .bind(project_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_signal).collect()
    }

    async fn signal_remove_for_color(
        &self,
        color: Color,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_COLOR_RETURNING)
            .bind(color.to_string())
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_signal).collect()
    }

    async fn signal_remove_for_project(
        &self,
        project_id: &str,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_PROJECT_RETURNING)
            .bind(project_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_signal).collect()
    }
}

const SIGNAL_SELECT_WHERE_TOKEN: &str =
    "SELECT token, tenant_id, project_id, color, node_id, is_resume, \
     spec_json, access_id, consumer_kind, tags, port_snapshot, consumer_payload, \
     surface_kind, mount_path, auth_kind, auth_config, kind_state, kind_state_seq \
     FROM signal WHERE token = $1";

const SIGNAL_SELECT_WHERE_PROJECT: &str =
    "SELECT token, tenant_id, project_id, color, node_id, is_resume, \
     spec_json, access_id, consumer_kind, tags, port_snapshot, consumer_payload, \
     surface_kind, mount_path, auth_kind, auth_config, kind_state, kind_state_seq \
     FROM signal WHERE project_id = $1";

const SIGNAL_DELETE_BY_COLOR_RETURNING: &str =
    "DELETE FROM signal WHERE color = $1 RETURNING token, tenant_id, project_id, color, \
     node_id, is_resume, spec_json, access_id, consumer_kind, tags, port_snapshot, \
     consumer_payload, surface_kind, mount_path, \
     auth_kind, auth_config, kind_state, kind_state_seq";

const SIGNAL_DELETE_BY_PROJECT_RETURNING: &str =
    "DELETE FROM signal WHERE project_id = $1 RETURNING token, tenant_id, project_id, color, \
     node_id, is_resume, spec_json, access_id, consumer_kind, tags, port_snapshot, \
     consumer_payload, surface_kind, mount_path, \
     auth_kind, auth_config, kind_state, kind_state_seq";

const SIGNAL_DELETE_BY_TOKENS_RETURNING: &str =
    "DELETE FROM signal WHERE token = ANY($1) RETURNING token, tenant_id, project_id, color, \
     node_id, is_resume, spec_json, access_id, consumer_kind, tags, port_snapshot, \
     consumer_payload, surface_kind, mount_path, \
     auth_kind, auth_config, kind_state, kind_state_seq";

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
    access_id: Option<String>,
    consumer_kind: Option<String>,
    tags: Vec<String>,
    port_snapshot: Option<serde_json::Value>,
    consumer_payload: Option<String>,
    surface_kind: String,
    mount_path: Option<String>,
    auth_kind: String,
    auth_config: Option<serde_json::Value>,
    kind_state: serde_json::Value,
    kind_state_seq: i64,
}

fn row_to_signal(row: SignalRow) -> anyhow::Result<SignalRegistration> {
    // Distinguish a NULL column (a legitimately absent value) from a NON-NULL value
    // that fails to decode (corrupt state). A resume signal's `color` is matched by
    // the fire/resume path to route the signal to its suspended execution: silently
    // collapsing a corrupt color to `None` would make that execution unresumable
    // with no error, so a present-but-unparseable color fails LOUD here.
    let color = match row.color {
        None => None,
        Some(s) => Some(
            s.parse::<Color>()
                .map_err(|e| anyhow::anyhow!("corrupt signal.color '{s}' for token {}: {e}", row.token))?,
        ),
    };
    let consumer_payload = match row.consumer_payload {
        None => None,
        Some(s) => Some(serde_json::from_str(&s).map_err(|e| {
            anyhow::anyhow!("corrupt signal.consumer_payload for token {}: {e}", row.token)
        })?),
    };
    Ok(SignalRegistration {
        token: row.token,
        tenant_id: row.tenant_id,
        project_id: row.project_id,
        color,
        node_id: row.node_id,
        is_resume: row.is_resume,
        spec_json: row.spec_json,
        access_id: row.access_id,
        consumer_kind: row.consumer_kind,
        tags: row.tags,
        port_snapshot: row.port_snapshot,
        consumer_payload,
        surface_kind: row.surface_kind,
        mount_path: row.mount_path,
        auth_kind: row.auth_kind,
        auth_config: row.auth_config,
        kind_state: row.kind_state,
        kind_state_seq: row.kind_state_seq,
    })
}

/// The `signal_token` SELECT row shape (id, token_hash, recognizer, tenant_id,
/// name, allowed_projects, allowed_tags, created_at). One tuple type so both
/// readers decode it through the single fallible `row_to_signal_token`.
type SignalTokenRow = (
    uuid::Uuid,
    String,
    String,
    String,
    Option<String>,
    Vec<uuid::Uuid>,
    Vec<String>,
    i64,
);

fn row_to_signal_token(row: SignalTokenRow) -> anyhow::Result<SignalToken> {
    let (id, token_hash, recognizer, tenant_id, name, projects, tags, created_at) = row;
    Ok(SignalToken {
        id,
        token_hash,
        recognizer,
        tenant_id,
        name,
        allowed_projects: projects,
        allowed_tags: tags,
        created_at: created_at as u64,
    })
}

// `crate::lease::now_unix` is the canonical wall-clock reader.
