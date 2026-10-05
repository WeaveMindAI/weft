//! Postgres-backed journal. Multiple dispatchers share one
//! Postgres database; the event log + token lookup tables are the
//! durable state. Postgres is the source of truth, every dispatcher
//! process is a stateless reader/writer.
//!
//! Applies no schema of its own: the boot's `app::apply_core_schema`
//! runs every group (this crate's [`GROUP`] included) in one pass.

use anyhow::Context;
use async_trait::async_trait;
use sqlx::postgres::PgPool;

use weft_core::ExecutionId;

use weft_journal::{decode_event, ExecEvent};
// The tag column every summary read selects, in claim order. The SQL
// itself lives with the table's every other read and write, in
// `weft_journal::tags`; it is spliced into the two summary queries,
// which both have the execution in scope as `execution ec`.
use weft_journal::tags::TAGS_LATERAL;
use weft_journal::EXECUTION_TERMINAL_KINDS_SQL as TERMINAL;
use weft_journal::RUN_PARKED_SQL as PARKED;
use crate::journal::{
    CancelWrite, SignalToken, ExecutionIdLookup, ExecutionOwner, ExecutionQuery, Journal, LogEntry, SignalRegistration,
};
use weft_core::program::{ExecutionPage, ExecutionSummary, RunStatus, SummaryStatus};

pub struct PostgresJournal {
    pool: PgPool,
}

/// Retention reads the durable reference without interpreting execution
/// state (an older row's selection format must neither keep nor free
/// its code). A malformed reference refuses cleanup, since deletion must
/// be safe: only an explicit `null` (a run with no program, such as a
/// node self-test) means "nothing to keep"; a missing key is a row this
/// reader does not understand.
fn retained_definition(payload: &str) -> anyhow::Result<Option<String>> {
    let row: serde_json::Value = serde_json::from_str(payload)?;
    let fields = row.as_object().context("birth row is not an object")?;
    // The SQL already selects birth rows by the `kind` column; the payload
    // saying the same is the check that column and payload agree.
    anyhow::ensure!(fields.get("kind").and_then(serde_json::Value::as_str) == Some("execution_started"), "row is not an execution birth");
    match fields.get("definition_hash") {
        Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(hash)) => Ok(Some(hash.clone())),
        Some(_) => anyhow::bail!("birth row's definition_hash is not a string"),
        None => anyhow::bail!("birth row carries no definition_hash field"),
    }
}

#[cfg(test)]
mod retention_tests {
    use super::retained_definition;

    #[test]
    fn retention_reads_only_the_program_reference_and_refuses_a_broken_one() {
        for selection in [r#"["mid"]"#, r#"{"nodes":["mid"]}"#] {
            let row = format!(r#"{{"kind":"execution_started","entry_node":"mid","subgraph":{selection},"definition_hash":"kept"}}"#);
            assert_eq!(retained_definition(&row).unwrap().as_deref(), Some("kept"));
        }
        assert_eq!(retained_definition(r#"{"kind":"execution_started","definition_hash":null}"#).unwrap(), None);
        for row in ["broken", "[]", r#"{"kind":"execution_started","definition_hash":42}"#, r#"{"kind":"node_started"}"#,
            r#"{"kind":"execution_started","entry_node":"mid"}"#] {
            assert!(retained_definition(row).is_err(), "{row}");
        }
    }
}

/// The `execution.phase` column as a phase. The column is NOT
/// NULL and only ever written from `Phase::as_str`, so an unreadable
/// value is corruption of the same row whose payload already failed to
/// decode; it is logged and answered as a fire so the row still
/// appears (in the listing, on the point-get, and to the running set
/// the cancel and wipe sweeps read, which must never be the thing a
/// bad row breaks).
pub(crate) fn phase_from_column(text: &str) -> weft_core::context::Phase {
    weft_core::context::Phase::from_tag(text).unwrap_or_else(|| {
        tracing::warn!(
            target: "weft_dispatcher::journal",
            phase = %text,
            "execution row holds an unknown phase; answered as a fire"
        );
        weft_core::context::Phase::Fire
    })
}

/// The row a run whose journal no longer decodes is listed as: the
/// same corrupt row from the listing and from the point-get, so the
/// run that `weft executions` shows as corrupt can still be opened,
/// inspected through its replay, and deleted. Everything on it comes
/// from the `execution` columns the query matched on, never
/// from a guess: a corrupt row that said `fire` while `--phase
/// trigger_setup` selected it would contradict the filter that found
/// it.
fn corrupt_summary(
    execution_id: ExecutionId,
    project_id: uuid::Uuid,
    phase_text: &str,
    started_at: i64,
    error: &anyhow::Error,
) -> ExecutionSummary {
    // `warn`, not `error`: the point-get is polled by the editor for
    // an open run, so a corrupt one would otherwise fill the log with
    // the same line; the row's `corrupt` status is what the reader
    // acts on, and the replay carries the error itself.
    tracing::warn!(
        target: "weft_dispatcher::journal",
        %execution_id, error = %error,
        "execution row does not decode; answered as corrupt"
    );
    ExecutionSummary {
        execution_id,
        project_id,
        entry_node: String::new(),
        status: SummaryStatus::Corrupt,
        phase: phase_from_column(phase_text),
        started_at: started_at as u64,
        completed_at: None,
        tags: Vec::new(),
        cancel_cause: None,
        error: None,
        skipped_nodes: 0,
        instance: None,
    }
}

/// Turn a `(started_payload, terminal_payload)` pair (an `execution_started`
/// event JSON + its latest terminal event JSON, if any) into an
/// `ExecutionSummary`. Shared by `list_executions` and `execution_summary` so
/// the started-decode + terminal-status mapping lives in exactly one place.
/// `Err` (naming the execution and `weft clean`) for any unusable row: one
/// that does not decode, or one whose kind column and payload disagree;
/// both callers answer that with `corrupt_summary`.
fn summary_from_payloads(
    execution_id: ExecutionId,
    started_payload: &str,
    terminal_payload: Option<String>,
    tags: Vec<String>,
    skipped_nodes: i64,
) -> anyhow::Result<ExecutionSummary> {
    let started = decode_event(execution_id, started_payload).map_err(anyhow::Error::msg)?;
    let ExecEvent::ExecutionStarted {
        execution_id, project_id, entry_node, phase, at_unix, instance, ..
    } = started
    else {
        // The row was selected by kind = 'execution_started', so a
        // decodable non-started payload means the kind column and the
        // payload disagree: corrupted post-write, same as undecodable.
        anyhow::bail!(
            "exec_event row for execution {execution_id} is kind execution_started but decodes \
             to a different event; `weft clean {execution_id}` removes this execution's rows"
        );
    };
    // The terminal lookup only selects execution_{completed,failed,cancelled}
    // rows, so any other variant here means the journal row was corrupted
    // post-write. Surface that loudly: a "running" placeholder would show a
    // terminal execution as live.
    let (status, completed_at, cancel_cause, error) = match terminal_payload {
        None => (RunStatus::Running, None, None, None),
        Some(p) => match decode_event(execution_id, &p).map_err(anyhow::Error::msg)? {
            ExecEvent::ExecutionCompleted { at_unix, .. } => (RunStatus::Completed, Some(at_unix), None, None),
            ExecEvent::ExecutionFailed { at_unix, error, .. } => (RunStatus::Failed, Some(at_unix), None, Some(error)),
            ExecEvent::ExecutionCancelled { at_unix, cause, .. } => (RunStatus::Cancelled, Some(at_unix), cause, None),
            other => anyhow::bail!(
                "execution summary: terminal lookup returned non-terminal event \
                 for execution {execution_id}: {other:?}"
            ),
        },
    };
    Ok(ExecutionSummary {
        execution_id,
        project_id,
        entry_node,
        status: status.into(),
        phase,
        started_at: at_unix,
        completed_at,
        tags,
        cancel_cause,
        error,
        skipped_nodes: skipped_nodes.max(0) as u64,
        instance,
    })
}

/// The lateral that counts an execution's skipped node firings, for the
/// panel's "completed, 3 skipped" words. Same shape as `TAGS_LATERAL`.
const SKIPPED_LATERAL: &str = "(SELECT COUNT(*) FROM exec_event WHERE execution_id = ec.execution_id AND kind = 'node_skipped')";

/// Write the dispatcher-side cancel terminals for `execution_id` on the
/// caller's transaction: `NodeCancelled` per non-terminal node, then
/// `ExecutionCancelled`, from the ONE shared definition of that list
/// (`cancel_terminal_events`), so every transactional cancel writer
/// emits identical rows. Skips entirely (`None`) when the journal
/// already holds a terminal, so a worker's own terminal is never
/// contradicted. Per-node cancels land BEFORE the terminal: a
/// terminal-first partial write would make a retry skip the per-node
/// rows forever. A payload that fails to decode fails the whole read,
/// matching `events_log`. Returns the per-node count written.
async fn cancel_terminals_in(
    tx: &mut sqlx::PgConnection,
    execution_id: ExecutionId,
    program: Option<&weft_core::ProjectDefinition>,
    cause: &weft_core::exec::CancelCause,
) -> anyhow::Result<Option<usize>> {
    let events = decode_all(execution_id, payload_rows(&mut *tx, execution_id).await?)?;
    if events.iter().any(ExecEvent::is_execution_terminal) {
        return Ok(None);
    }
    let now = crate::lease::now_unix() as u64;
    let writes = crate::api::execution::cancel_terminal_events(execution_id, &events, program, cause, now)?;
    let node_cancellations = writes.len() - 1;
    for (event, dedup) in writes {
        weft_journal::record_event_in(&mut *tx, &event, None, Some(&dedup))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    Ok(Some(node_cancellations))
}

/// Every payload for `execution_id`, in write order, undecoded. Takes the
/// executor so both the pool reads and the in-transaction cancel read
/// share one query.
async fn payload_rows<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
    execution_id: ExecutionId,
) -> anyhow::Result<Vec<weft_journal::RawJournalRow>> {
    let rows: Vec<(i64, String)> = sqlx::query_as(&weft_task_store::journal_rows::rows_after_sql("$1", "0"))
        .bind(execution_id.to_string())
        .fetch_all(executor)
        .await?;
    Ok(rows.into_iter().map(|(id, payload)| weft_journal::RawJournalRow { id, payload }).collect())
}

/// Strictly decode every row: one undecodable row fails the whole read
/// (a fold over a partial event list rebuilds a state that never
/// existed).
fn decode_all(execution_id: ExecutionId, rows: Vec<weft_journal::RawJournalRow>) -> anyhow::Result<Vec<ExecEvent>> {
    Ok(weft_journal::decode_rows(execution_id, rows)?.into_iter().map(|row| row.event).collect())
}

impl PostgresJournal {
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

    /// The execution's first `ExecutionStarted` event, decoded. The ONE
    /// fetch behind `execution_definition_hash` (`execution_project`
    /// and `execution_tenant` read the `execution` mirror
    /// instead, so they survive a corrupt payload). A row whose JSON no longer
    /// decodes is a PERMANENT poison: returning `Err` would make
    /// pollers (the journal bridge's per-row processing) retry the
    /// same row forever, stalling the cursor fleet-wide. Log loud
    /// and report `Corrupt` (non-retryable, distinct from
    /// `NotFound`) so callers can word the failure honestly.
    async fn execution_started(&self, execution_id: ExecutionId) -> anyhow::Result<ExecutionIdLookup<ExecEvent>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT payload_json FROM exec_event \
             WHERE execution_id = $1 AND kind = 'execution_started' \
             ORDER BY id ASC LIMIT 1",
        )
        .bind(execution_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let Some((payload,)) = row else { return Ok(ExecutionIdLookup::NotFound) };
        match serde_json::from_str::<ExecEvent>(&payload) {
            Ok(ev) => Ok(ExecutionIdLookup::Found(ev)),
            Err(e) => {
                tracing::error!(
                    target: "weft_dispatcher::journal",
                    %execution_id,
                    error = %e,
                    "ExecutionStarted row failed to decode \
                     (permanent corruption, retrying cannot fix it)"
                );
                Ok(ExecutionIdLookup::Corrupt)
            }
        }
    }

    /// Write one event, pairing an `ExecutionStarted` with its
    /// `execution` seed in ONE transaction. The seed
    /// denormalizes (execution, project_id, tenant_id) so the broker's
    /// scope check and the terminal sweeps
    /// (`list_non_terminal_execution_ids_for_project`) can see the execution
    /// without folding the journal; a crash between the event insert
    /// and a separate seed would create an execution those sweeps can
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
        let started = StartedRow::of(event, dedup_key, crate::lease::now_unix())?;
        sqlx::query("SELECT weft_execution_started($1)")
            .bind(serde_json::to_value(&started)?)
            .execute(&self.pool)
            .await
            .map_err(birth_refusal)?;
        Ok(())
    }

    /// A run's birth (`weft_start_execution`), behind `admission` when it
    /// has one: one round trip whatever the birth writes.
    async fn birth(
        &self,
        start: &ExecEvent,
        kicks: &[ExecEvent],
        task: weft_task_store::tasks::NewTask,
        trigger_setup: Option<TriggerSetupRow>,
        admission: Option<&crate::entry_limits::Admission>,
    ) -> anyhow::Result<Result<(), crate::entry_limits::Refused>> {
        let now = crate::lease::now_unix();
        let started = StartedRow::of(start, None, now)?;
        if let Some(stray) = kicks.iter().find(|kick| kick.execution_id() != start.execution_id()) {
            anyhow::bail!("a birth of execution {} kicks a node of execution {}", start.execution_id(), stray.execution_id());
        }
        let kicks = kicks
            .iter()
            .map(|kick| Ok(EventRow { kind: kick.kind_str(), payload: serde_json::to_string(kick)? }))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let call = BirthCall {
            started,
            kicks,
            task: weft_task_store::tasks::DedupRow::of(&task, uuid::Uuid::new_v4(), now)?,
            trigger_setup,
            admission,
        };
        let answer: serde_json::Value = sqlx::query_scalar("SELECT weft_start_execution($1)")
            .bind(serde_json::to_value(&call)?)
            .fetch_one(&self.pool)
            .await
            .map_err(birth_refusal)?;
        match answer.get("outcome").and_then(serde_json::Value::as_str) {
            Some("started" | "already_started") => Ok(Ok(())),
            Some("refused") => {
                let refused: crate::entry_limits::Answer =
                    serde_json::from_value(answer["refused"].clone()).context("read the birth's refusal")?;
                Ok(Err(refused.refused()?))
            }
            _ => anyhow::bail!("the database answered a birth with '{answer}', which weft does not read"),
        }
    }
}

/// A failure of the birth functions. One they raise themselves (`RAISE
/// EXCEPTION`, SQLSTATE P0001) is passed on in its own words, which name
/// what is missing and what to do; any other keeps the database's whole
/// error and where in the functions it happened, since one call now does
/// what several statements did.
fn birth_refusal(e: sqlx::Error) -> anyhow::Error {
    let Some(db) = e.as_database_error() else { return anyhow::Error::from(e) };
    if db.code().as_deref() == Some("P0001") {
        return anyhow::anyhow!("{}", db.message());
    }
    let at = db.try_downcast_ref::<sqlx::postgres::PgDatabaseError>().and_then(|pg| pg.r#where()).map(str::to_string);
    let e = anyhow::Error::from(e);
    match at {
        Some(at) => e.context(format!("a run's birth failed in the database, at: {at}")),
        None => e.context("a run's birth failed in the database"),
    }
}

/// One journal row as the birth functions take it.
#[derive(serde::Serialize)]
struct EventRow {
    kind: &'static str,
    payload: String,
}

/// An `ExecutionStarted` and its seed, as `weft_execution_started` takes
/// them.
// SYNC: StartedRow's fields <-> weft_execution_started, weft_start_execution (GROUP below)
#[derive(serde::Serialize)]
struct StartedRow {
    execution_id: String,
    project_id: uuid::Uuid,
    /// Whether the run keeps a journal; an unrecorded run is born with its
    /// seed alone, its birth riding the execute task.
    journaled: bool,
    kind: &'static str,
    payload: String,
    at_unix: i64,
    phase: &'static str,
    run_kind: &'static str,
    instance_id: Option<String>,
    fired_by: Option<String>,
    source_version: Option<String>,
    dedup_key: Option<String>,
    created_at: i64,
}

impl StartedRow {
    fn of(event: &ExecEvent, dedup_key: Option<&str>, created_at: i64) -> anyhow::Result<Self> {
        let ExecEvent::ExecutionStarted { execution_id, project_id, at_unix, phase, run_kind, source_version, instance, fired_trigger, .. } =
            event
        else {
            anyhow::bail!("an execution's birth row must be its ExecutionStarted");
        };
        Ok(Self {
            execution_id: execution_id.to_string(),
            project_id: *project_id,
            journaled: run_kind.journaled(),
            kind: event.kind_str(),
            payload: serde_json::to_string(event)?,
            at_unix: *at_unix as i64,
            phase: phase.as_str(),
            run_kind: run_kind.as_str(),
            instance_id: instance.as_ref().map(|m| m.as_str().to_string()),
            fired_by: fired_trigger.clone(),
            source_version: source_version.clone(),
            dedup_key: dedup_key.map(str::to_string),
            created_at,
        })
    }
}

/// A trigger setup's birth: recorded in flight, and, when an activation
/// asked for it (the activation is the setup's own execution), born only
/// while that activation still owns its rows.
#[derive(serde::Serialize)]
struct TriggerSetupRow {
    for_activation: bool,
}

/// Everything `weft_start_execution` takes.
// SYNC: BirthCall's fields <-> weft_start_execution (GROUP below)
#[derive(serde::Serialize)]
struct BirthCall<'a> {
    started: StartedRow,
    kicks: Vec<EventRow>,
    task: weft_task_store::tasks::DedupRow,
    trigger_setup: Option<TriggerSetupRow>,
    admission: Option<&'a crate::entry_limits::Admission>,
}

/// The journal's schema. First in `app::ALL_GROUPS` (other groups'
/// triggers attach to its tables); applied by the
/// boot's one `apply_core_schema` pass, never here.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "exec_event",
    tables: &["exec_event", "signal_token", "signal", "execution", "execution_tag", "trigger_setup", "trigger_bake"],
    ddl: &[
        // One row per trigger setup in flight. Several may run at once for
        // one project (each activation claims its own triggers, one instance's
        // at a time), so the setup's own execution is the key.
        r#"CREATE TABLE IF NOT EXISTS trigger_setup (
            project_id UUID NOT NULL,
            execution_id TEXT PRIMARY KEY
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_trigger_setup_project ON trigger_setup(project_id)"#,
        // One bake per (project, owner, program identity): an instance's
        // triggers capture that instance's own values (their copy's
        // address), so they are baked apart from the shared ones. The key
        // is the identity's digest (`ProgramIdentity::digest`): the
        // identity itself lists one hash per compiled node type and does
        // not fit a btree entry once a worker carries the whole catalog.
        // The full identity travels inside `bake_json`. A setup of some
        // triggers merges its captures into the bake it shares a key with.
        r#"CREATE TABLE IF NOT EXISTS trigger_bake (
            project_id UUID NOT NULL,
            instance_id TEXT,
            program_hash TEXT NOT NULL,
            bake_json TEXT NOT NULL
        )"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_trigger_bake_key
             ON trigger_bake(project_id, instance_id, program_hash) NULLS NOT DISTINCT"#,
        // exec_event: append-only journal. `dedup_key` is the
        // idempotency knob writers that may retry (dispatcher tasks
        // that crash mid-execution) populate; the partial UNIQUE
        // means unkeyed events (most worker-side events) are
        // unrestricted, keyed events collapse on conflict.
        // SYNC: exec_event's (execution_id, id, payload_json) <-> crates/weft-task-store/src/journal_rows.rs (rows_after_sql), crates/weft-task-store/tests/support/mod.rs (its stand-in)
        r#"CREATE TABLE IF NOT EXISTS exec_event (
            id BIGSERIAL PRIMARY KEY,
            execution_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            -- The worker replica that wrote the row; NULL for the
            -- dispatcher's and listener's own writes.
            replica TEXT,
            dedup_key TEXT,
            -- The transaction that wrote the row, the order the
            -- dispatcher's cursor reads in (`crate::settled`).
            writer_xid XID8 NOT NULL DEFAULT pg_current_xact_id()
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_exec_event_execution_id ON exec_event(execution_id, id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_exec_event_settled ON exec_event(writer_xid, id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_exec_event_kind ON exec_event(kind, id DESC)"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_exec_event_dedup
           ON exec_event(dedup_key) WHERE dedup_key IS NOT NULL"#,
        // Announce every row, with its execution, when it commits: the
        // dispatcher's event bridge wakes on any, and a worker waiting
        // on its run's journal (through the broker) on its own execution's.
        // Many rows of one execution in one transaction collapse to one
        // notification, since Postgres drops a duplicate payload within
        // a transaction.
        // SYNC: 'weft_exec_event' <-> weft_journal::EXEC_EVENT_CHANNEL
        r#"CREATE OR REPLACE FUNCTION exec_event_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_exec_event', NEW.execution_id);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS exec_event_notify_on_insert ON exec_event"#,
        r#"CREATE TRIGGER exec_event_notify_on_insert
            AFTER INSERT ON exec_event
            FOR EACH ROW
            EXECUTE FUNCTION exec_event_notify()"#,
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
            -- The display dimension, which does NOT follow the
            -- empty-means-wildcard rule above: a display can be a
            -- credential, so a token says nothing about displays and
            -- reads none. `all_displays` is the wildcard, set by
            -- `weft token mint --displays`.
            allowed_displays TEXT[] NOT NULL DEFAULT '{}',
            all_displays BOOLEAN NOT NULL DEFAULT FALSE,
            created_at BIGINT NOT NULL,
            -- An instance token: the one instance it acts inside, in its one
            -- project (`allowed_projects` holds exactly that one). NULL for a
            -- token scoped to no instance.
            instance_id TEXT,
            -- When it stops working (unix seconds); NULL never. An
            -- instance token always has one: it lives in a browser.
            expires_at BIGINT,
            -- What the token may do: `caller` (the scoped outside
            -- credential above, an instance's included) or `operator` (the
            -- tenant's admin key, which carries no scope and acts as no
            -- instance). Checked at every door.
            -- SYNC: kind values <-> weft_core::signal_token::TokenKind::as_str
            kind TEXT NOT NULL DEFAULT 'caller' CHECK (kind IN ('caller', 'operator')),
            CONSTRAINT signal_token_instance_has_one_project
                CHECK (instance_id IS NULL OR cardinality(allowed_projects) = 1),
            CONSTRAINT signal_token_instance_expires
                CHECK (instance_id IS NULL OR expires_at IS NOT NULL),
            CONSTRAINT signal_token_operator_is_nobody
                CHECK (kind = 'caller' OR instance_id IS NULL)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_token_tenant ON signal_token(tenant_id)"#,
        // signal: one row per registered wake target (entry trigger
        // or resume token). Routing fields denormalize what the
        // public router needs at fire time so it doesn't parse
        // spec_json per-request.
        r#"CREATE TABLE IF NOT EXISTS signal (
            token TEXT PRIMARY KEY,
            program_json JSONB,
            setup_execution_id UUID,
            source_version TEXT,
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            execution_id TEXT,
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
            -- replica claims this row's queue for replay, cleared on
            -- either success (alongside parked_fires=[]) or failure
            -- (release). A sweeper releases stale claims older than
            -- the claim-stale threshold so a dispatcher crash
            -- mid-step doesn't leave the row uncloseable.
            drain_claimed_at_unix BIGINT,
            -- Per-claim owner nonce. Set when a drain claims the row;
            -- every pop + the release is fenced on it. If a stale-claim
            -- sweep hands the row to a sibling replica mid-drain, the
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
            -- A public entry's route pattern under its tenant
            -- (`/<tenant>/chat/{room}`); the dispatcher matches a call
            -- against every pattern of the tenant in Rust, so this is
            -- never an equality lookup key.
            mount_path TEXT,
            -- The HTTP methods the public entry serves, uppercase; empty
            -- = any. Two entries of one tenant may not overlap in both
            -- pattern and method (checked at register time).
            mount_methods TEXT[] NOT NULL DEFAULT '{}',
            -- Whose signal: NULL for the program's shared ones, else the
            -- instance whose copy of a per-instance trigger this is, or whose
            -- run is waiting on it.
            instance_id TEXT,
            -- The trigger whose activation gates this signal (with
            -- `instance_id`, the `trigger_activation` row the fire gate
            -- reads): an entry signal's own trigger, or the trigger that
            -- fired the run a wait belongs to. NULL for a wait of a run
            -- started by hand, which no activation governs.
            activation_trigger TEXT,
            auth_kind TEXT NOT NULL DEFAULT 'none',
            auth_config JSONB,
            -- Whether the signal keeps a connection to the outside open
            -- between fires (its kind's decision for it, from the
            -- listener's `/prepare`): a holder holds it, and the number
            -- of holders is counted from these rows.
            holds BOOLEAN NOT NULL DEFAULT FALSE,
            -- The holder holding its connection now (its replica id) and
            -- until when (unix seconds): a lease it renews while it lives
            -- and another holder takes once it lapses. NULL while nobody
            -- holds it. A registration that rewrites the row clears both,
            -- so whoever held the old one lets go and the new one comes up
            -- fresh.
            held_by TEXT,
            held_until BIGINT,
            -- What its holder says the connection is doing
            -- (`{"status", "transport"}`), for the node's display, which
            -- another process renders. NULL while nobody holds it.
            serving JSONB
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_tenant ON signal(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_project ON signal(project_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_execution_id ON signal(execution_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_consumer_kind ON signal(consumer_kind)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_access_id ON signal(access_id)
           WHERE access_id IS NOT NULL"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_mount_path
             ON signal(mount_path, mount_methods) WHERE mount_path IS NOT NULL"#,
        // What a holder claims from: the held signals, by when their
        // lease ends.
        r#"CREATE INDEX IF NOT EXISTS idx_signal_held ON signal(held_until) WHERE holds"#,
        // Wake the holder sizing when a held signal comes or goes, so the
        // holders start as the first one is registered and stop with the
        // last.
        // SYNC: 'weft_held_signals' <-> crate::holders::HELD_SIGNALS_CHANNEL
        r#"CREATE OR REPLACE FUNCTION signal_held_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_held_signals', '');
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS signal_held_on_insert ON signal"#,
        r#"CREATE TRIGGER signal_held_on_insert
            AFTER INSERT ON signal
            FOR EACH ROW
            WHEN (NEW.holds)
            EXECUTE FUNCTION signal_held_notify()"#,
        r#"DROP TRIGGER IF EXISTS signal_held_on_delete ON signal"#,
        r#"CREATE TRIGGER signal_held_on_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            WHEN (OLD.holds)
            EXECUTE FUNCTION signal_held_notify()"#,
        r#"DROP TRIGGER IF EXISTS signal_held_on_change ON signal"#,
        r#"CREATE TRIGGER signal_held_on_change
            AFTER UPDATE OF holds ON signal
            FOR EACH ROW
            WHEN (NEW.holds IS DISTINCT FROM OLD.holds)
            EXECUTE FUNCTION signal_held_notify()"#,
        // Tell every dispatcher a tenant's routes changed, so its copy of
        // them (`crate::held::Held::routes`) is read again: a public entry
        // coming, going, or changing anything the handshake reads. The
        // project group's trigger uses the same function (a project row
        // gone reads as an inactive route).
        // SYNC: 'weft_routes' <-> crate::held::ROUTES_CHANNEL
        r#"CREATE OR REPLACE FUNCTION routes_notify_tenant() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_routes', OLD.tenant_id);
                ELSE
                    PERFORM pg_notify('weft_routes', NEW.tenant_id);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS signal_routes_on_insert ON signal"#,
        r#"CREATE TRIGGER signal_routes_on_insert
            AFTER INSERT ON signal
            FOR EACH ROW
            WHEN (NEW.surface_kind = 'public_entry')
            EXECUTE FUNCTION routes_notify_tenant()"#,
        r#"DROP TRIGGER IF EXISTS signal_routes_on_delete ON signal"#,
        r#"CREATE TRIGGER signal_routes_on_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            WHEN (OLD.surface_kind = 'public_entry')
            EXECUTE FUNCTION routes_notify_tenant()"#,
        r#"DROP TRIGGER IF EXISTS signal_routes_on_change ON signal"#,
        r#"CREATE TRIGGER signal_routes_on_change
            AFTER UPDATE OF surface_kind, mount_path, mount_methods, project_id, node_id, spec_json,
                auth_kind, auth_config, port_snapshot, program_json, source_version, instance_id,
                activation_trigger ON signal
            FOR EACH ROW
            WHEN (NEW.surface_kind = 'public_entry' OR OLD.surface_kind = 'public_entry')
            EXECUTE FUNCTION routes_notify_tenant()"#,
        // Entry rows are keyed by (project_id, node_id), `node_id`
        // being the trigger's place spelled the way a person writes
        // it (`one.door`), so a file called from two places holds two
        // rows: that pair is what TriggerSetup re-targets on every
        // reactivate. The partial unique index lets `signal_insert`
        // upsert entry rows in place. Resume rows (is_resume=TRUE)
        // skip the constraint because each suspension mints its own
        // row.
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_entry_node
             ON signal(project_id, node_id, instance_id) NULLS NOT DISTINCT WHERE is_resume = FALSE"#,
        // The fire gate's join, and what taking an activation down selects.
        r#"CREATE INDEX IF NOT EXISTS idx_signal_activation
             ON signal(project_id, activation_trigger, instance_id) WHERE activation_trigger IS NOT NULL"#,
        // Wake the parked-fires sweep when a fire is parked, so a
        // project that is already Active again replays it at once
        // instead of on the sweep's next look. Only a growing queue
        // speaks: the drain's own pops shrink it.
        // SYNC: 'weft_parked_fire' <-> crate::reaper::PARKED_FIRE_CHANNEL
        r#"CREATE OR REPLACE FUNCTION signal_parked_fire_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_parked_fire', NEW.project_id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS signal_parked_fire_notify_on_grow ON signal"#,
        r#"CREATE TRIGGER signal_parked_fire_notify_on_grow
            AFTER UPDATE OF parked_fires ON signal
            FOR EACH ROW
            WHEN (jsonb_array_length(NEW.parked_fires) > jsonb_array_length(OLD.parked_fires))
            EXECUTE FUNCTION signal_parked_fire_notify()"#,
        // execution binds an execution to its project +
        // tenant, denormalized for the broker's scope-check fast path.
        // The dispatcher INSERTs this row alongside ExecutionStarted
        // (which is the only event that introduces a fresh execution).
        // Workers / listeners never write here; they only need it to
        // exist so the broker can answer "does execution C belong to
        // tenant T" without re-folding the journal.
        r#"CREATE TABLE IF NOT EXISTS execution (
            execution_id TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            started_at_unix BIGINT NOT NULL,
            phase TEXT NOT NULL,
            -- Worker replica that owns this execution's writes. NULL until the
            -- first worker claims an execution-bearing task (the broker
            -- stamps it in task_claim_execution); thereafter it is the replica of
            -- the LATEST claimer. The broker rejects any journal_record
            -- whose caller.replica doesn't match, so a compromised
            -- worker can only journal under its own bound replica, not
            -- cross-write sibling executions in the same tenant.
            --
            -- "Latest claimer wins" is how a resume hands ownership to a
            -- new replica when the original is gone: the resume task is
            -- pinned to the original owner if it is still alive (so only
            -- it reclaims and ownership stays stable), and spawns + pins
            -- to a fresh replica only when the owner is dead (so the handoff
            -- is the ONLY time ownership moves). Without that pinning a
            -- fresh worker could steal a live owner's execution mid-flight
            -- now that a project can run more than one worker; see
            -- `task_kinds::execute::enqueue_resume`.
            -- NULL also covers dispatcher-orchestrated writes (no replica).
            owner_replica TEXT,
            -- What this execution IS (`weft_core::exec::RunKind`):
            -- 'execution' (a project run; the project-lifecycle sweeps,
            -- cancel, wipe, drain counting and the listings operate on
            -- these), 'node_test' (a node self-test's identity: real
            -- cost attribution and broker scoping, but its lifecycle is
            -- owned by its task, so the project sweeps must never cancel
            -- it or wait on it), or 'unrecorded' (a run whose journal
            -- lives in its worker's memory: never listed, dropped when
            -- it ends unless its costs keep it, and turned into an
            -- 'execution' with its whole record written if it fails).
            kind TEXT NOT NULL DEFAULT 'execution',
            -- Which instance the run is for: the one its `ExecutionStarted`
            -- names, copied here in the same transaction so every
            -- instance filter (clean, costs, an instance token's reads) is
            -- a column read. NULL for a run of the shared program.
            instance_id TEXT,
            -- The trigger whose firing started the run (its
            -- `ExecutionStarted.fired_trigger`), NULL for a run started
            -- by hand and every setup run. With `instance_id` it names the
            -- activation the run belongs to: a wait the run registers is
            -- gated by that activation, and taking it down reaches it.
            fired_by TEXT,
            -- When an unrecorded run ended, for the one whose row its
            -- costs keep after it is over (`weft_journal::unrecorded`):
            -- NULL while it runs, so the live rule stops counting it the
            -- moment it ends, before its execute task is closed. NULL
            -- for every other kind, whose ending is its journal row.
            ended_at_unix BIGINT
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_tenant ON execution(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_project ON execution(project_id)"#,
        // An instance's runs of one project: what `ctx.runs().instance(id)`,
        // `weft clean --instance` and an instance token's reads select.
        r#"CREATE INDEX IF NOT EXISTS idx_execution_instance
             ON execution(project_id, instance_id) WHERE instance_id IS NOT NULL"#,
        // The execution LISTING reads exactly this shape: one tenant's
        // project runs, newest first, a page at a time, with the
        // optional project / time / phase filters applied on top. The
        // index carries the wall, the sort and the tie-break, so a page
        // is a walk of `limit` rows instead of a sort of the tenant's
        // whole history; `kind` is in the predicate rather than the key
        // because every listing query is about project runs (a node
        // test's execution is never listed).
        r#"CREATE INDEX IF NOT EXISTS idx_execution_listing
             ON execution(tenant_id, started_at_unix DESC, execution_id DESC)
             WHERE kind = 'execution'"#,
        // execution_tag: the selectable copy of `ctx.tag_execution`, one
        // row per (execution, tag), written by the broker on the worker's
        // behalf in the same transaction as the `ExecutionTagged`
        // event. `ctx.stop_tagged` selects on it: "every live execution
        // of this project carrying tag T" is one indexed read joined to
        // `execution` instead of a fold over every open journal.
        // `seq` is the order tags were written, gap-tolerant and never
        // tied, which is what the last-one-wins rule compares (unix
        // seconds would tie two runs of the same user inside one
        // second). The (execution, tag) uniqueness is what makes a body
        // replayed after a durable wait land on the same row instead of moving
        // the run's place in the order. Rows go with the execution's
        // journal on `weft clean`.
        r#"CREATE TABLE IF NOT EXISTS execution_tag (
            seq BIGSERIAL PRIMARY KEY,
            execution_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            tagged_at_unix BIGINT NOT NULL,
            UNIQUE (execution_id, tag)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_tag_tag ON execution_tag(tag)"#,
        // An execution's journal lock, held until the transaction ends: the
        // ONE spelling of its key, taken by every journal write here and by
        // `weft_journal::lock_execution_ids`.
        r#"CREATE OR REPLACE FUNCTION weft_lock_execution(p_execution_id TEXT) RETURNS VOID AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('exec_event:' || p_execution_id, 0));
            END;
            $$ LANGUAGE plpgsql"#,
        // THE journal insert (`weft_journal::write`): the execution's lock,
        // then the rows in the order given, fenced on `p_owner` when one is
        // named. Returns how many rows went in. The lock comes first so
        // rows of one execution are numbered and committed in the same
        // order; taking it again in a transaction that already holds it is
        // free.
        r#"CREATE OR REPLACE FUNCTION weft_journal_append(
                p_execution_id TEXT, p_kinds TEXT[], p_payloads TEXT[], p_created_at BIGINT,
                p_replica TEXT, p_owner TEXT, p_dedup_key TEXT
            ) RETURNS BIGINT AS $$
            DECLARE
                written BIGINT;
            BEGIN
                PERFORM weft_lock_execution(p_execution_id);
                INSERT INTO exec_event (execution_id, kind, payload_json, created_at, replica, dedup_key)
                    SELECT p_execution_id, e.kind, e.payload, p_created_at, p_replica, p_dedup_key
                    FROM unnest(p_kinds, p_payloads) WITH ORDINALITY AS e(kind, payload, n)
                    WHERE p_owner IS NULL
                       OR EXISTS (SELECT 1 FROM execution x
                                  WHERE x.execution_id = p_execution_id AND x.owner_replica = p_owner)
                    ORDER BY e.n
                    ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING;
                GET DIAGNOSTICS written = ROW_COUNT;
                RETURN written;
            END;
            $$ LANGUAGE plpgsql"#,
        // An execution's `ExecutionStarted` and its `execution` seed,
        // which commit together: the seed is what the broker's scope check
        // and the terminal sweeps see an execution by, so one without the
        // other would be an execution nothing can ever sweep. A missing
        // project row refuses the whole write. The source version the run
        // was prepared from is held (FOR KEY SHARE) so it cannot be removed
        // under the run. An unrecorded run is born with its seed alone.
        // SYNC: p's fields <-> crate::journal::postgres::StartedRow
        r#"CREATE OR REPLACE FUNCTION weft_execution_started(p JSONB) RETURNS VOID AS $$
            DECLARE
                v_execution_id TEXT := p->>'execution_id';
                v_project UUID := (p->>'project_id')::uuid;
                seeded BIGINT;
            BEGIN
                PERFORM weft_lock_execution(v_execution_id);
                IF p->>'source_version' IS NOT NULL THEN
                    PERFORM 1 FROM project_version
                        WHERE project_id = v_project AND id = p->>'source_version' FOR KEY SHARE;
                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'source version % was removed during preparation; run the command again',
                            p->>'source_version';
                    END IF;
                END IF;
                IF (p->>'journaled')::boolean THEN
                    PERFORM weft_journal_append(v_execution_id, ARRAY[p->>'kind'], ARRAY[p->>'payload'],
                        (p->>'created_at')::bigint, NULL, NULL, p->>'dedup_key');
                END IF;
                INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase, kind, instance_id, fired_by)
                    SELECT v_execution_id, v_project, pr.tenant_id, (p->>'at_unix')::bigint, p->>'phase',
                           p->>'run_kind', p->>'instance_id', p->>'fired_by'
                    FROM project pr WHERE pr.id = v_project
                    ON CONFLICT (execution_id) DO NOTHING;
                GET DIAGNOSTICS seeded = ROW_COUNT;
                IF seeded = 0 AND NOT EXISTS (SELECT 1 FROM execution WHERE execution_id = v_execution_id) THEN
                    RAISE EXCEPTION 'refuse to journal ExecutionStarted for execution %: project % has no row, so the execution seed (which the broker scope check and the terminal sweeps depend on) cannot be written; register the project first',
                        v_execution_id, v_project;
                END IF;
            END;
            $$ LANGUAGE plpgsql"#,
        // A run's whole birth, in one call: one execution's admission (its
        // entry's limits, when it has any), its first task, its
        // `ExecutionStarted` and seed, and the kicks that start it.
        // Answers `{"outcome": "started" | "already_started" | "refused"}`,
        // a refusal carrying which limit (`weft_admit`'s answer). Nothing
        // is written for a run already born (its birth row, or its seed for
        // an unrecorded run) or whose task is already live: a retried birth
        // collapses onto the first, which keeps the slot it took. A refused
        // run writes only its counts. The execution's lock is taken before
        // any write, which is the journal's ordering rule.
        // SYNC: p's fields <-> crate::journal::postgres::BirthCall
        r#"CREATE OR REPLACE FUNCTION weft_start_execution(p JSONB) RETURNS JSONB AS $$
            DECLARE
                v_started JSONB := p->'started';
                v_task JSONB := p->'task';
                v_execution_id TEXT := v_started->>'execution_id';
                v_project UUID := (v_started->>'project_id')::uuid;
                v_journaled BOOLEAN := (v_started->>'journaled')::boolean;
                v_refused JSONB;
                v_inserted BOOLEAN;
            BEGIN
                PERFORM weft_lock_execution(v_execution_id);
                IF (v_journaled AND EXISTS (SELECT 1 FROM exec_event
                        WHERE execution_id = v_execution_id AND kind = 'execution_started'))
                   OR (NOT v_journaled AND EXISTS (SELECT 1 FROM execution WHERE execution_id = v_execution_id)) THEN
                    RETURN jsonb_build_object('outcome', 'already_started');
                END IF;
                IF jsonb_typeof(p->'admission') = 'object' THEN
                    v_refused := weft_admit(p->'admission');
                    IF v_refused IS NOT NULL THEN
                        RETURN jsonb_build_object('outcome', 'refused', 'refused', v_refused);
                    END IF;
                END IF;
                SELECT d.inserted INTO v_inserted FROM weft_enqueue_dedup(
                    (v_task->>'id')::uuid, v_task->>'kind', v_task->>'target', (v_task->>'project_id')::uuid,
                    v_task->>'dedup_key', v_task->>'execution_id', v_task->>'tenant_id', v_task->>'target_replica',
                    v_task->>'binary_hash', v_task->'payload', (v_task->>'created_at')::bigint) d;
                IF NOT v_inserted THEN
                    RETURN jsonb_build_object('outcome', 'already_started');
                END IF;
                PERFORM weft_execution_started(v_started);
                -- A trigger setup is born only while the activation that asked
                -- for it still owns its rows (a cancel between the claim and
                -- here wins), and is recorded as in flight.
                IF jsonb_typeof(p->'trigger_setup') = 'object' THEN
                    IF (p->'trigger_setup'->>'for_activation')::boolean THEN
                        PERFORM 1 FROM trigger_activation
                            WHERE project_id = v_project
                              AND activating_execution_id = v_execution_id::uuid
                              AND status = 'activating'
                            FOR UPDATE;
                        IF NOT FOUND THEN
                            RAISE EXCEPTION 'activation % ended before trigger setup could start', v_execution_id;
                        END IF;
                    END IF;
                    INSERT INTO trigger_setup (project_id, execution_id) VALUES (v_project, v_execution_id);
                END IF;
                IF v_journaled AND jsonb_array_length(p->'kicks') > 0 THEN
                    PERFORM weft_journal_append(v_execution_id,
                        ARRAY(SELECT k->>'kind' FROM jsonb_array_elements(p->'kicks') WITH ORDINALITY AS e(k, n) ORDER BY n),
                        ARRAY(SELECT k->>'payload' FROM jsonb_array_elements(p->'kicks') WITH ORDINALITY AS e(k, n) ORDER BY n),
                        (v_started->>'created_at')::bigint, NULL, NULL, NULL);
                END IF;
                RETURN jsonb_build_object('outcome', 'started');
            END;
            $$ LANGUAGE plpgsql"#,
    ],
    seed: &[],
};

#[async_trait]
impl Journal for PostgresJournal {
    async fn is_trigger_setup_pending(&self, execution_id: ExecutionId) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM trigger_setup WHERE execution_id = $1)")
            .bind(execution_id.to_string()).fetch_one(&self.pool).await?)
    }

    async fn finish_trigger_setup(&self, execution_id: ExecutionId, bake: Option<&super::TriggerBake>) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        let owner: Option<(uuid::Uuid,)> = sqlx::query_as(
            "DELETE FROM trigger_setup WHERE execution_id = $1 RETURNING project_id",
        ).bind(execution_id.to_string()).fetch_optional(&mut *tx).await?;
        if let (Some((project_id,)), Some(bake)) = (owner, bake) {
            anyhow::ensure!(bake.project_id == project_id && bake.execution_id == execution_id, "bake does not belong to its setup");
            let instance = bake.instance.as_ref().map(|m| m.as_str());
            let digest = bake.program.digest();
            // A setup of some triggers refreshes those and keeps what an
            // earlier setup captured for the others, under the row lock.
            let prior: Option<(String,)> = sqlx::query_as(
                "SELECT bake_json FROM trigger_bake \
                 WHERE project_id = $1 AND instance_id IS NOT DISTINCT FROM $2 AND program_hash = $3 FOR UPDATE",
            ).bind(project_id).bind(instance).bind(&digest).fetch_optional(&mut *tx).await?;
            let merged = match prior {
                Some((json,)) => serde_json::from_str::<super::TriggerBake>(&json)?.refreshed_by(bake),
                None => bake.clone(),
            };
            sqlx::query("INSERT INTO trigger_bake (project_id, instance_id, program_hash, bake_json) VALUES ($1, $2, $3, $4) \
                ON CONFLICT (project_id, instance_id, program_hash) DO UPDATE SET bake_json = EXCLUDED.bake_json")
                .bind(project_id).bind(instance).bind(&digest)
                .bind(serde_json::to_string(&merged)?).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn trigger_bakes(&self, project_id: uuid::Uuid, instance: Option<&weft_core::instance::InstanceId>) -> anyhow::Result<Vec<super::TriggerBake>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT bake_json FROM trigger_bake WHERE project_id = $1 AND instance_id IS NOT DISTINCT FROM $2",
        )
            .bind(project_id).bind(instance.map(|m| m.as_str())).fetch_all(&self.pool).await?;
        rows.into_iter().map(|(value,)| serde_json::from_str(&value).map_err(Into::into)).collect()
    }

    async fn record_event(&self, event: &ExecEvent) -> anyhow::Result<()> {
        // Single canonical row shape lives in weft-journal so the
        // dispatcher, engine, and listener all INSERT identical rows;
        // `record_with_seed` adds the execution seed in the
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
        execution_id: ExecutionId,
    ) -> anyhow::Result<(Vec<crate::events::IdentifiedEvent<ExecEvent>>, Vec<String>)> {
        let rows = payload_rows(&self.pool, execution_id).await?;
        let mut out = Vec::with_capacity(rows.len());
        let mut bad = Vec::new();
        for row in rows {
            match decode_event(execution_id, &row.payload) {
                Ok(ev) => out.push(crate::events::IdentifiedEvent::recorded(row.id, ev)),
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
        for_activation: bool,
    ) -> anyhow::Result<()> {
        let trigger_setup = match start {
            ExecEvent::ExecutionStarted { phase: weft_core::context::Phase::TriggerSetup, .. } => Some(TriggerSetupRow { for_activation }),
            _ => None,
        };
        let born = self.birth(start, kicks, task, trigger_setup, None).await?;
        born.map_err(|refused| anyhow::anyhow!("a birth with no limits to check was refused: {refused:?}"))
    }

    async fn admit_and_start_execution(
        &self,
        admission: &crate::entry_limits::Admission,
        start: &ExecEvent,
        kicks: &[ExecEvent],
        task: weft_task_store::tasks::NewTask,
    ) -> anyhow::Result<Result<(), crate::entry_limits::Refused>> {
        anyhow::ensure!(
            !matches!(start, ExecEvent::ExecutionStarted { phase: weft_core::context::Phase::TriggerSetup, .. }),
            "a trigger setup is not admitted at an entry's limits"
        );
        self.birth(start, kicks, task, None, Some(admission)).await
    }

    async fn cancel_execution(
        &self,
        execution_id: ExecutionId,
        program: Option<&weft_core::ProjectDefinition>,
        cause: &weft_core::exec::CancelCause,
    ) -> anyhow::Result<CancelWrite> {
        let mut tx = self.pool.begin().await?;
        // 0. Execution lock before the first write (the signal delete): the
        //    ordering invariant on `weft_journal::write`.
        weft_journal::lock_execution_ids(&mut tx, &[execution_id]).await?;
        // 1. The wake signals go first in the write order for the same
        //    reason they went first when these were separate statements:
        //    a fire that resolves its signal row after this commits finds
        //    nothing, and one that resolved it before enqueues a resume
        //    the worker refuses on the terminal below.
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_EXECUTION_ID_RETURNING)
            .bind(execution_id.to_string())
            .fetch_all(&mut *tx)
            .await
            .context("cancel_execution: strip the execution's signals: read a signal row")?;
        let removed: Vec<SignalRegistration> =
            rows.into_iter().map(row_to_signal).collect::<anyhow::Result<_>>()?;
        // 2 + 3. Only a started execution has a journal to close and a process to
        //    flag; its project and tenant were stamped on this row in the
        //    birth transaction, so no other lookup is needed.
        let owner: Option<(uuid::Uuid, String, String)> =
            sqlx::query_as("SELECT project_id, tenant_id, kind FROM execution WHERE execution_id = $1")
                .bind(execution_id.to_string())
                .fetch_optional(&mut *tx)
                .await?;
        let mut write = CancelWrite { removed, ..CancelWrite::default() };
        if let Some((project_id, tenant_id, kind)) = owner {
            let kind = weft_core::exec::RunKind::parse(&kind)
                .map_err(|e| anyhow::anyhow!("execution.kind of {execution_id}: {e}"))?;
            // An unrecorded run has no journal to close: the process driving it
            // cancels it in memory and forgets it. With no process left to do
            // that, the run died with its process, so it is forgotten here.
            if kind.journaled() {
                write.node_cancellations = cancel_terminals_in(&mut tx, execution_id, program, cause).await?;
            }
            write.task_enqueued = crate::task_kinds::execute::enqueue_cancel_in(
                &mut tx,
                project_id,
                execution_id,
                tenant_id.as_str(),
                cause,
            )
            .await?;
            if !kind.journaled() && !write.task_enqueued {
                weft_journal::unrecorded::forget_in(&mut tx, execution_id).await?;
                crate::storage::enqueue_sweep_in(&mut tx, tenant_id.as_str(), &execution_id.to_string()).await?;
            }
        }
        tx.commit().await?;
        Ok(write)
    }

    async fn consume_suspension(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        // Drop the signal row entirely; resume tokens are
        // single-use. Entry triggers (is_resume=false) are NOT
        // touched here; deactivate handles those.
        let row: Option<SignalRow> = sqlx::query_as(SIGNAL_DELETE_RESUME_BY_TOKEN_RETURNING)
            .bind(token)
            .fetch_optional(&self.pool)
            .await
            .context("consume_suspension: read a signal row")?;
        row.map(row_to_signal).transpose()
    }

    async fn mint_signal_token(&self, tok: &SignalToken) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO signal_token \
             (id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
              allowed_displays, all_displays, created_at, instance_id, expires_at, kind) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(tok.id)
        .bind(&tok.token_hash)
        .bind(&tok.recognizer)
        .bind(&tok.tenant_id)
        .bind(&tok.name)
        .bind(&tok.allowed_projects)
        .bind(&tok.allowed_tags)
        .bind(&tok.allowed_displays)
        .bind(tok.all_displays)
        // Store the caller-stamped mint time verbatim (the handler set it from
        // the canonical clock), so postgres and the fake agree.
        .bind(tok.created_at as i64)
        .bind(tok.instance.as_ref().map(|m| m.as_str()))
        .bind(tok.expires_at.map(|at| at as i64))
        .bind(tok.kind.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }


    async fn get_signal_token(&self, token_hash: &str) -> anyhow::Result<Option<SignalToken>> {
        let row: Option<SignalTokenRow> = sqlx::query_as(
            "SELECT id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
                    allowed_displays, all_displays, created_at, instance_id, expires_at, kind \
             FROM signal_token WHERE token_hash = $1",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_signal_token).transpose()
    }

    async fn seed_operator_token(&self, tok: &SignalToken) -> anyhow::Result<bool> {
        anyhow::ensure!(tok.kind == weft_core::signal_token::TokenKind::Operator, "only an operator key is seeded");
        let res = sqlx::query(
            "INSERT INTO signal_token \
             (id, token_hash, recognizer, tenant_id, name, created_at, kind) \
             SELECT $1, $2, $3, $4, $5, $6, 'operator' \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM signal_token WHERE tenant_id = $4 AND kind = 'operator') \
             ON CONFLICT (token_hash) DO NOTHING",
        )
        .bind(tok.id)
        .bind(&tok.token_hash)
        .bind(&tok.recognizer)
        .bind(&tok.tenant_id)
        .bind(&tok.name)
        .bind(tok.created_at as i64)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn list_signal_tokens(&self, tenant: &str) -> anyhow::Result<Vec<SignalToken>> {
        let rows: Vec<SignalTokenRow> = sqlx::query_as(
            "SELECT id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
                    allowed_displays, all_displays, created_at, instance_id, expires_at, kind \
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

    async fn execution_owner(&self, execution_id: ExecutionId) -> anyhow::Result<Option<ExecutionOwner>> {
        // One read of the `execution` row, whose project and
        // tenant were stamped together in the SAME transaction as
        // `ExecutionStarted` (see `write_birth_in`). Never re-derived:
        // not from the started payload (undecodable exactly when
        // `weft clean` is the way out), and not from the project store
        // (deletable, and an execution deliberately outlives its
        // project).
        let row: Option<(uuid::Uuid, String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT project_id, tenant_id, instance_id, fired_by FROM execution WHERE execution_id = $1",
        )
        .bind(execution_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(project_id, tenant, instance, fired_by)| {
            Ok(ExecutionOwner {
                project_id,
                tenant,
                instance: instance.map(weft_core::instance::InstanceId::new).transpose().map_err(|e| {
                    anyhow::anyhow!("corrupt execution.instance_id for {execution_id}: {e}")
                })?,
                fired_by,
            })
        })
        .transpose()
    }

    async fn execution_definition_hash(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<ExecutionIdLookup<String>> {
        Ok(match self.execution_started(execution_id).await? {
            // A definition-less start (a node self-test) answers
            // NotFound: nothing may resume against it, and the
            // caller's existing unknown-execution bail is the loud path.
            ExecutionIdLookup::Found(ExecEvent::ExecutionStarted {
                definition_hash: Some(hash),
                ..
            }) => ExecutionIdLookup::Found(hash),
            ExecutionIdLookup::Found(_) => ExecutionIdLookup::NotFound,
            ExecutionIdLookup::NotFound => ExecutionIdLookup::NotFound,
            ExecutionIdLookup::Corrupt => ExecutionIdLookup::Corrupt,
        })
    }

    async fn definition_hashes_in_use(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<String>> {
        // Read off the birth rows themselves rather than any copy of
        // them: this answer decides what gets DELETED. Only the program
        // reference matters here; an older execution's selection format
        // must not prevent retention of its code or cleanup of unused code.
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT ec.execution_id, ev.payload_json FROM execution ec \
             JOIN exec_event ev ON ev.execution_id = ec.execution_id \
             WHERE ec.project_id = $1 AND ev.kind = 'execution_started'",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        let mut out: Vec<String> = Vec::new();
        for (execution_id, payload) in rows {
            if let Some(hash) = retained_definition(&payload)
                .with_context(|| format!("cannot read the program reference on the birth row of execution {execution_id}; retaining its code"))?
            { out.push(hash); }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn logs_for(&self, execution_id: ExecutionId, limit: u32) -> anyhow::Result<Vec<LogEntry>> {
        // Only the log-worthy kinds leave the database (`LogEntry::KINDS`
        // is the projection's own list): every one of the execution's, in
        // journal order, and the tail is cut by `LogEntry::tail` in
        // WRITTEN order, the same code the fake runs. The cut cannot be
        // the SQL's: a node's line lands in the journal whenever a process
        // drains its task, so the row order is not the run's, and the
        // written time lives inside the payload, which only a decode
        // can read honestly. A display read, like `events_log_lossy`:
        // a payload that fails to decode becomes an error line naming
        // the row and `weft clean`, never a line quietly missing (read
        // as "the node never said that") and never a read that hides
        // every surviving line behind a 500.
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT payload_json, created_at FROM exec_event \
             WHERE execution_id = $1 AND kind = ANY($2) \
             ORDER BY id ASC",
        )
        .bind(execution_id.to_string())
        .bind(LogEntry::KINDS)
        .fetch_all(&self.pool)
        .await?;
        let mut entries: Vec<LogEntry> = Vec::with_capacity(rows.len());
        for (payload, created_at) in &rows {
            match decode_event(execution_id, payload) {
                Ok(event) => entries.extend(LogEntry::from_event(&event)),
                Err(e) => entries.push(LogEntry::corrupt_row(*created_at as u64, e)),
            }
        }
        Ok(LogEntry::tail(entries, limit))
    }

    async fn list_executions(
        &self,
        tenant: &str,
        query: &ExecutionQuery,
    ) -> anyhow::Result<ExecutionPage> {
        // One SQL statement: every `execution_started` row for this tenant (the
        // EXISTS-style JOIN against `execution` keeps the tenant wall in
        // SQL), narrowed by the optional project + start-time filters, joined
        // laterally against its latest terminal event, newest first, with
        // limit/offset paging. A parallel COUNT over the same filters gives the
        // total so a consumer can render page controls. Filters + paging live in
        // SQL so a tenant with a huge history never truncates blindly.
        //
        // Bind order is fixed ($1 tenant, $2 project filter, $3 after, $4 before,
        // $5 phase, $6 entry node, $7 status, $8 instance, $9 tag) and every optional filter is a
        // `($n IS NULL OR ...)` clause so one prepared statement serves every
        // filter combination.
        // The `execution` row (seeded at start) carries the real, indexed
        // columns the filters key on: `tenant_id` (the wall), `project_id`, and
        // `started_at_unix`. `exec_event` only has `execution_id`/`kind`/`payload_json`,
        // so we filter on `ec` and fetch the started `payload_json` from the
        // matching `execution_started` event.
        let project = query.project_id;
        let after = query.started_after.map(|v| v as i64);
        let before = query.started_before.map(|v| v as i64);
        let phase = query.phase.map(|p| p.as_str());
        // The entry node is not a column: it lives in the
        // `execution_started` payload, so it is matched inside the same
        // lookup of that event both queries already do rather than in a
        // second pass. The tenant, project and time predicates narrow
        // the scan first, so this reads a payload only for rows that
        // already matched everything else.
        let entry_node = query.entry_node.as_deref();
        // Status is not a column either: it IS which terminal event the
        // run ended on, "running" is the absence of one, and
        // "waiting_for_input" is that absence with a resume signal
        // registered for the run (what the listing's overlay reads).
        // Written as one clause over `ec` alone so the count and the page
        // agree without the page's terminal join. A row the clause matches
        // whose birth no longer decodes still lists, as `corrupt` (SQL
        // cannot see the decode); `weft clean` deletes a listed run only
        // when `RunStatus::reaches` its decoded status, so a filtered
        // clean never deletes it.
        // SYNC: list_executions (status clause) <-> crates/weft-core/src/program.rs RunStatus
        let status = query.status.map(|s| s.as_str());
        // PROJECT EXECUTIONS only: this list is the user's record of
        // their project running. A node-test execution is a real identity
        // (its cost trail is addressed by execution from the test report),
        // but it has no definition, no graph, and no resume, so it
        // never belongs in this listing.
        // The `waiting_for_input` arm is a run with no ending that is
        // parked (`RUN_PARKED_SQL`, the shared parked rule).
        let where_clause = format!("ec.tenant_id = $1 \
             AND ec.kind = 'execution' \
             AND ($2::uuid IS NULL OR ec.project_id = $2) \
             AND ($3::bigint IS NULL OR ec.started_at_unix >= $3) \
             AND ($4::bigint IS NULL OR ec.started_at_unix < $4) \
             AND ($5::text IS NULL OR ec.phase = $5) \
             AND ($7::text IS NULL OR CASE WHEN $7 IN ('running', 'waiting_for_input') THEN NOT EXISTS ( \
                     SELECT 1 FROM exec_event WHERE execution_id = ec.execution_id \
                       AND kind IN {TERMINAL} \
                 ) AND ($7 = 'running' OR {PARKED}) ELSE EXISTS ( \
                     SELECT 1 FROM exec_event WHERE execution_id = ec.execution_id \
                       AND kind = 'execution_' || $7 \
                 ) END) \
             AND ($8::text IS NULL OR ec.instance_id = $8) \
             AND ($9::text IS NULL OR EXISTS ( \
                     SELECT 1 FROM execution_tag et WHERE et.execution_id = ec.execution_id AND et.tag = $9 \
                 ))");

        // The count carries the SAME started-event predicate as the row
        // query's inner lateral join: a seeded `execution` row
        // with no `execution_started` event can never be listed, so it
        // must not be counted either, else `total` promises rows the
        // pages cannot produce.
        let total: (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM execution ec WHERE {where_clause} \
             AND EXISTS ( \
                 SELECT 1 FROM exec_event \
                 WHERE execution_id = ec.execution_id AND kind = 'execution_started' \
                   AND ($6::text IS NULL OR payload_json::jsonb->>'entry_node' = $6) \
             )"
        ))
        .bind(tenant)
        .bind(project)
        .bind(after)
        .bind(before)
        .bind(phase)
        .bind(entry_node)
        .bind(status)
        .bind(query.instance.as_ref().map(|m| m.as_str()))
        .bind(query.tag.as_deref())
        .fetch_one(&self.pool)
        .await?;

        let rows: Vec<(String, uuid::Uuid, String, i64, String, Option<String>, Vec<String>, i64)> = sqlx::query_as(&format!(
            "SELECT ec.execution_id, ec.project_id, ec.phase, ec.started_at_unix, \
                    s.payload_json, t.payload_json, {TAGS_LATERAL}, {SKIPPED_LATERAL} \
             FROM execution ec \
             JOIN LATERAL ( \
                 SELECT payload_json FROM exec_event \
                 WHERE execution_id = ec.execution_id AND kind = 'execution_started' \
                   AND ($6::text IS NULL OR payload_json::jsonb->>'entry_node' = $6) \
                 ORDER BY id ASC LIMIT 1 \
             ) s ON TRUE \
             LEFT JOIN LATERAL ( \
                 SELECT payload_json FROM exec_event \
                 WHERE execution_id = ec.execution_id \
                   AND kind IN {TERMINAL} \
                 ORDER BY id DESC LIMIT 1 \
             ) t ON TRUE \
             WHERE {where_clause} \
               AND ($12::bigint IS NULL OR (ec.started_at_unix, ec.execution_id) < ($12, $13::text)) \
             ORDER BY ec.started_at_unix DESC, ec.execution_id DESC LIMIT $10 OFFSET $11"
        ))
        .bind(tenant)
        .bind(project)
        .bind(after)
        .bind(before)
        .bind(phase)
        .bind(entry_node)
        .bind(status)
        .bind(query.instance.as_ref().map(|m| m.as_str()))
        .bind(query.tag.as_deref())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .bind(query.below.map(|(started, _)| started as i64))
        .bind(query.below.map(|(_, execution_id)| execution_id.to_string()))
        .fetch_all(&self.pool)
        .await?;

        let mut executions = Vec::with_capacity(rows.len());
        for (execution_id_text, project_id, phase_text, started_at, started_payload, terminal_payload, tags, skipped) in rows {
            let execution_id: ExecutionId = execution_id_text.parse().map_err(|e| {
                anyhow::anyhow!("execution row holds a non-uuid execution '{execution_id_text}': {e}")
            })?;
            // One corrupt row must not take the whole list down (the
            // list is also the door to `weft clean`, the recovery for
            // exactly this state), and it must not vanish either: the
            // count includes it, so the page renders it as a broken
            // row (inspectable via replay, deletable).
            executions.push(
                summary_from_payloads(execution_id, &started_payload, terminal_payload, tags, skipped)
                    .unwrap_or_else(|e| corrupt_summary(execution_id, project_id, &phase_text, started_at, &e)),
            );
        }
        Ok(ExecutionPage { executions, total: total.0.max(0) as u64 })
    }

    async fn execution_ids_with_prefix(&self, tenant: &str, prefix: &str) -> anyhow::Result<Vec<ExecutionId>> {
        // `LIKE` on the text form with the prefix escaped: a prefix is
        // hex and dashes, but the escape keeps a stray `%` or `_` from
        // widening the match.
        let pattern = format!(
            "{}%",
            prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
        );
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT execution_id FROM execution \
             WHERE tenant_id = $1 AND kind = 'execution' AND execution_id LIKE $2 \
             ORDER BY started_at_unix DESC LIMIT 2",
        )
        .bind(tenant)
        .bind(pattern)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|(c,)| c.parse::<ExecutionId>().map_err(Into::into)).collect()
    }

    async fn execution_summary(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<Option<ExecutionSummary>> {
        // Direct point-lookup by execution: the started row plus its latest terminal
        // event, no windowed list scan. Returns None when the execution has no
        // `execution_started` row.
        let row: Option<(uuid::Uuid, String, i64, String, Option<String>, Vec<String>, i64)> = sqlx::query_as(&format!(
            "SELECT ec.project_id, ec.phase, ec.started_at_unix, s.payload_json, t.payload_json, {TAGS_LATERAL}, {SKIPPED_LATERAL} \
             FROM exec_event s \
             JOIN execution ec ON ec.execution_id = s.execution_id \
             LEFT JOIN LATERAL ( \
                 SELECT payload_json FROM exec_event \
                 WHERE execution_id = s.execution_id \
                   AND kind IN {TERMINAL} \
                 ORDER BY id DESC LIMIT 1 \
             ) t ON TRUE \
             WHERE s.kind = 'execution_started' AND s.execution_id = $1 \
             ORDER BY s.id ASC LIMIT 1"
        ))
        .bind(execution_id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            None => Ok(None),
            Some((project_id, phase_text, started_at, started_payload, terminal_payload, tags, skipped)) => {
                Ok(Some(
                    summary_from_payloads(execution_id, &started_payload, terminal_payload, tags, skipped)
                        .unwrap_or_else(|e| corrupt_summary(execution_id, project_id, &phase_text, started_at, &e)),
                ))
            }
        }
    }

    async fn execution_ids_for_project(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<ExecutionId>> {
        // `execution` is the mirror every execution gets when it is
        // born, written in the same transaction as the birth row, so it
        // answers "did this project ever start this execution" without
        // touching a payload.
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT execution_id FROM execution WHERE project_id = $1")
                .bind(project_id)
                .fetch_all(&self.pool)
                .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (text,) in rows {
            match text.parse::<ExecutionId>() {
                Ok(execution_id) => out.push(execution_id),
                // Said out loud rather than skipped in silence: the
                // caller asked by project, so it has no execution to ask
                // with instead, and a row nothing can name is what makes
                // a version-tree row undeletable.
                Err(e) => tracing::error!(
                    target: "weft_dispatcher::journal",
                    %project_id, execution_id = %text, error = %e,
                    "an execution row's execution is not a uuid; it is left out of every \
                     answer that asks which executions this project has"
                ),
            }
        }
        Ok(out)
    }

    async fn execution_summaries_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<std::collections::HashMap<ExecutionId, ExecutionSummary>> {
        // The same shape as `execution_summary`, once for the project
        // instead of once per execution.
        // `DISTINCT ON (s.execution_id) ... ORDER BY s.execution_id, s.id ASC` pins the
        // FIRST `execution_started` row of each execution, which is what the
        // per-execution read and the paged listing both pin with their own
        // `ORDER BY id ASC LIMIT 1`. Without it an execution with two birth rows
        // answered twice and whichever came back last won, so this read and
        // `execution_summary` could describe the same run differently.
        let rows: Vec<(String, uuid::Uuid, String, i64, String, Option<String>, Vec<String>, i64)> =
            sqlx::query_as(&format!(
                "SELECT DISTINCT ON (s.execution_id) \
                        s.execution_id, ec.project_id, ec.phase, ec.started_at_unix, s.payload_json, t.payload_json, {TAGS_LATERAL}, {SKIPPED_LATERAL} \
                 FROM exec_event s \
                 JOIN execution ec ON ec.execution_id = s.execution_id \
                 LEFT JOIN LATERAL ( \
                     SELECT payload_json FROM exec_event \
                     WHERE execution_id = s.execution_id \
                       AND kind IN {TERMINAL} \
                     ORDER BY id DESC LIMIT 1 \
                 ) t ON TRUE \
                 WHERE s.kind = 'execution_started' AND ec.project_id = $1 \
                 ORDER BY s.execution_id, s.id ASC"
            ))
            .bind(project_id)
            .fetch_all(&self.pool)
            .await?;
        let mut out = std::collections::HashMap::with_capacity(rows.len());
        for (execution_id_text, project, phase_text, started_at, started_payload, terminal_payload, tags, skipped) in rows {
            let Ok(execution_id) = execution_id_text.parse::<ExecutionId>() else {
                // An execution column that is not a uuid is a corrupt row, and
                // the caller asked by project so it has no execution to look up
                // instead. Said out loud: the run reads as `unknown` in the
                // tree either way, and silence would leave nobody a way to
                // find out why.
                tracing::error!(
                    target: "weft_dispatcher::journal",
                    %project_id, execution_id = %execution_id_text,
                    "an execution_started row carries an execution that is not a uuid; that run cannot \
                     be summarised"
                );
                continue;
            };
            let summary = summary_from_payloads(execution_id, &started_payload, terminal_payload, tags, skipped)
                .unwrap_or_else(|e| corrupt_summary(execution_id, project, &phase_text, started_at, &e));
            out.insert(execution_id, summary);
        }
        Ok(out)
    }

    async fn list_non_terminal_execution_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<(ExecutionId, weft_core::context::Phase)>> {
        // An execution is "non-terminal" iff it belongs to this project
        // AND is a live project run. `execution` is the
        // denormalized (execution, project_id) index seeded at birth, so
        // the project filter is an indexed equality lookup. This is the
        // one read behind the cancel/wipe sweeps and the drain count; a
        // node-test execution's lifecycle is owned by its task, never by
        // the project's, so the rule never matches one.
        // Liveness is the one rule every sweep shares
        // (`weft_journal::unrecorded::LIVE_RUN_SQL`): an unrecorded run
        // counts while a live process can still be driving it.
        let query = format!(
            "SELECT ec.execution_id, ec.phase FROM execution ec \
             WHERE ec.project_id = $1 AND {} \
             ORDER BY ec.started_at_unix ASC, ec.execution_id ASC",
            weft_journal::unrecorded::LIVE_RUN_SQL
        );
        let rows: Vec<(String, String)> = sqlx::query_as(&query)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (c, phase) in rows {
            let execution_id: ExecutionId = c
                .parse()
                .map_err(|e| anyhow::anyhow!("bad execution in execution: {e}"))?;
            out.push((execution_id, phase_from_column(&phase)));
        }
        Ok(out)
    }

    async fn live_tagged_executions(
        &self,
        project_id: uuid::Uuid,
        tag: &str,
    ) -> anyhow::Result<Vec<weft_journal::tags::TaggedExecution>> {
        Ok(weft_journal::tags::live_tagged_executions(&self.pool, project_id, tag).await?)
    }

    async fn list_terminal_execution_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<std::collections::HashSet<ExecutionId>> {
        // The complement of the non-terminal query: executions with a
        // terminal event. Distinct because an execution has one terminal
        // event but the join could otherwise repeat it.
        let rows: Vec<(String,)> = sqlx::query_as(concat!(
            "SELECT DISTINCT ec.execution_id FROM execution ec \
             WHERE ec.project_id = $1 \
               AND EXISTS ( \
                   SELECT 1 FROM exec_event t \
                   WHERE t.execution_id = ec.execution_id \
                     AND t.kind IN ",
            weft_journal::execution_terminal_kinds_sql!(),
            ")",
        ))
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        let mut out = std::collections::HashSet::with_capacity(rows.len());
        for (c,) in rows {
            let execution_id: ExecutionId = c
                .parse()
                .map_err(|e| anyhow::anyhow!("bad execution in execution: {e}"))?;
            out.insert(execution_id);
        }
        Ok(out)
    }

    async fn delete_execution(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut tx = self.pool.begin().await?;
        let removed = erase_execution_ids(&mut tx, &[execution_id]).await?;
        tx.commit().await?;
        Ok(removed)
    }

    async fn erase_unclaimed_live_run(
        &self,
        execution_id: ExecutionId,
        which: weft_task_store::tasks::UnclaimedLiveRun,
    ) -> anyhow::Result<bool> {
        use weft_task_store::tasks::UnclaimedLiveRun;
        let mut tx = self.pool.begin().await?;
        weft_journal::lock_execution_ids(&mut tx, &[execution_id]).await?;
        let condition = match which {
            UnclaimedLiveRun::PastDeadline { .. } => weft_task_store::tasks::never_arrived_sql("$2"),
            UnclaimedLiveRun::NeverPassedOn => weft_task_store::tasks::unclaimed_live_sql(),
        };
        let erase = format!("DELETE FROM task WHERE execution_id = $1 AND {condition}");
        let mut erase = sqlx::query(&erase).bind(execution_id.to_string());
        if let UnclaimedLiveRun::PastDeadline { now } = which {
            erase = erase.bind(now);
        }
        let unclaimed = erase.execute(&mut *tx).await?.rows_affected();
        if unclaimed == 0 {
            return Ok(false);
        }
        // A run nothing ever drove parked on nothing, so no listener holds
        // a signal of it.
        let removed = erase_execution_ids(&mut tx, &[execution_id]).await?;
        anyhow::ensure!(
            removed.is_empty(),
            "live run {execution_id} was never claimed yet held {} resume signal(s); left in place",
            removed.len()
        );
        sqlx::query("DELETE FROM entry_slot WHERE execution_id = $1")
            .bind(execution_id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn delete_project_executions(&self, project_id: uuid::Uuid) -> anyhow::Result<u64> {
        let mut tx = self.pool.begin().await?;
        // The project's executions come from the index rather than from the
        // journal: it is the one table that knows which project an execution
        // belongs to without parsing an event body, and it is the table
        // the erase is about to empty for them.
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT execution_id FROM execution WHERE project_id = $1")
                .bind(project_id)
                .fetch_all(&mut *tx)
                .await?;
        let execution_ids: Vec<ExecutionId> = rows
            .into_iter()
            .map(|(c,)| c.parse().map_err(|e| anyhow::anyhow!("bad execution in execution: {e}")))
            .collect::<anyhow::Result<_>>()?;
        // The signals these runs were parked on come back too, but
        // there is nothing to tell a process: `weft rm` deactivates before
        // it erases, and that already stripped and unregistered every
        // signal of the project.
        erase_execution_ids(&mut tx, &execution_ids).await?;
        tx.commit().await?;
        Ok(execution_ids.len() as u64)
    }

    async fn projects_with_orphan_executions(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT ec.project_id FROM execution ec \
             WHERE NOT EXISTS (SELECT 1 FROM project p WHERE p.id = ec.project_id)",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn signal_insert(&self, sig: &SignalRegistration) -> anyhow::Result<super::SignalWrite> {
        // Entry rows (is_resume=false) reuse the existing token
        // across reactivates: the register_signal task looks up
        // the existing token for (project_id, node_id, is_resume=
        // FALSE) before calling INSERT, so the conflict path just
        // refreshes spec_json/mount_path/mount_methods/auth_*/consumer_payload
        // on the same row. Parked_payload from before the
        // reactivate drains cleanly because the token didn't
        // change. Resume rows (is_resume=TRUE) always insert
        // fresh: their token is per-suspension.
        // A compare-and-set on kind_state_seq: the registration read the
        // row at `sig.kind_state_seq` and computed its state from that,
        // so it lands only while the row is still there, and moves the
        // row one past so a wake's claim taken at the old version fails.
        // A claim that landed first moved the row: nothing is written and
        // the caller recomputes from the new state (`StateMoved`).
        let mut tx = self.pool.begin().await?;
        if let Some(setup) = sig.setup_execution_id {
            // Arm only while THIS activation still owns the trigger's row:
            // a cancelled or superseded activation must not arm over the
            // one that replaced it.
            let owned: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM trigger_activation \
                 WHERE project_id = $1 AND trigger = $2 AND instance_id IS NOT DISTINCT FROM $3 \
                   AND status = 'activating' AND activating_execution_id = $4 FOR UPDATE)",
            )
            .bind(sig.project_id)
            .bind(&sig.node_id)
            .bind(sig.instance.as_ref().map(|m| m.as_str()))
            .bind(setup)
            .fetch_one(&mut *tx)
            .await?;
            anyhow::ensure!(!sig.is_resume && owned,
                "activation ended before trigger '{}' could be armed", sig.node_id);
        }
        if let Some(version) = &sig.source_version {
            crate::versions::retain_source_version(&mut tx, sig.project_id, version).await?;
        }
        let query = sqlx::query(&SIGNAL_INSERT)
            .bind(&sig.token)
            .bind(&sig.tenant_id)
            .bind(sig.project_id)
            .bind(sig.execution_id.map(|c| c.to_string()))
            .bind(&sig.node_id)
            .bind(sig.is_resume)
            .bind(crate::lease::now_unix())
            .bind(sig.instance.as_ref().map(|m| m.as_str()))
            .bind(sig.activation_trigger.as_deref());
        let written = bind_signal_refreshed(query, sig)?
            .bind(sig.kind_state_seq)
            .execute(&mut *tx)
            .await?;
        if written.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(super::SignalWrite::StateMoved);
        }
        tx.commit().await?;
        Ok(super::SignalWrite::Written)
    }

    async fn signal_restore(&self, sig: &SignalRegistration) -> anyhow::Result<super::SignalWrite> {
        // The same columns `signal_insert`'s refresh writes, under the
        // same compare-and-set on kind_state_seq, and the same hold on
        // the source version it names: a version pruned since the row
        // was replaced is refused rather than pointed at.
        let mut tx = self.pool.begin().await?;
        if let Some(version) = &sig.source_version {
            crate::versions::retain_source_version(&mut tx, sig.project_id, version)
                .await
                .with_context(|| format!("signal_restore: put signal {} back", sig.token))?;
        }
        let query = sqlx::query(&SIGNAL_RESTORE).bind(&sig.token);
        let written = bind_signal_refreshed(query, sig)?
            .bind(sig.kind_state_seq)
            .execute(&mut *tx)
            .await
            .context("signal_restore: put a signal row back")?;
        if written.rows_affected() == 1 {
            tx.commit().await?;
            return Ok(super::SignalWrite::Written);
        }
        let present: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM signal WHERE token = $1)")
            .bind(&sig.token)
            .fetch_one(&mut *tx)
            .await?;
        tx.rollback().await?;
        anyhow::ensure!(present, "signal {} has no row to restore", sig.token);
        Ok(super::SignalWrite::StateMoved)
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<SignalRow> = sqlx::query_as(SIGNAL_SELECT_WHERE_TOKEN)
            .bind(token)
            .fetch_optional(&self.pool)
            .await
            .context("signal_get: read a signal row")?;
        row.map(row_to_signal).transpose()
    }

    async fn signal_entry_at(
        &self,
        project_id: uuid::Uuid,
        node: &str,
        instance: Option<&weft_core::instance::InstanceId>,
    ) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<SignalRow> = sqlx::query_as(SIGNAL_SELECT_ENTRY_AT_PLACE)
            .bind(project_id)
            .bind(node)
            .bind(instance.map(|m| m.as_str()))
            .fetch_optional(&self.pool)
            .await
            .context("signal_entry_at: read a signal row")?;
        row.map(row_to_signal).transpose()
    }

    async fn signal_remove_many(
        &self,
        tokens: &[String],
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        remove_signals(&self.pool, tokens).await
    }

    async fn signal_list_for_execution_id(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_SELECT_WHERE_EXECUTION_ID_RESUME)
            .bind(execution_id.to_string())
            .fetch_all(&self.pool)
            .await
            .context("signal_list_for_execution_id: read a signal row")?;
        rows.into_iter().map(row_to_signal).collect()
    }

    async fn signal_list_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_SELECT_WHERE_PROJECT)
            .bind(project_id)
            .fetch_all(&self.pool)
            .await
            .context("signal_list_for_project: read a signal row")?;
        rows.into_iter().map(row_to_signal).collect()
    }

    async fn signal_remove_for_execution_id(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_EXECUTION_ID_RETURNING)
            .bind(execution_id.to_string())
            .fetch_all(&self.pool)
            .await
            .context("signal_remove_for_execution_id: read a signal row")?;
        rows.into_iter().map(row_to_signal).collect()
    }

    async fn signal_remove_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        remove_project_signals(&self.pool, project_id).await
    }
}

/// Erase every trace of these executions, inside the caller's
/// transaction.
///
/// ONE list of where an execution lives, used by the per-execution erase
/// (`weft clean`) and the per-project one (`weft rm`), because two
/// lists would drift and the drift would be invisible: a table the
/// project erase forgot leaves rows nothing can ever reach again, on a
/// path nobody runs twice.
///
/// It has to be one transaction. Half-applied, a crash in the window
/// leaves tag and index rows whose journal is empty, which is exactly
/// the row set the live tag read selects for, and a later stop writes
/// fresh cancel rows into a deleted journal, resurrecting a ghost run.
/// Every row of these executions, in one transaction. Answers the resume
/// signals it removed, because a listener process holds each of those in
/// RAM and only the caller can tell it to let go.
async fn erase_execution_ids(
    tx: &mut sqlx::PgConnection,
    execution_ids: &[ExecutionId],
) -> anyhow::Result<Vec<SignalRegistration>> {
    if execution_ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<String> = execution_ids.iter().map(|c| c.to_string()).collect();
    sqlx::query("DELETE FROM trigger_setup WHERE execution_id = ANY($1)")
        .bind(&ids).execute(&mut *tx).await?;
    sqlx::query("DELETE FROM exec_event WHERE execution_id = ANY($1)")
        .bind(&ids).execute(&mut *tx).await?;
    for execution_id in execution_ids {
        weft_journal::tags::delete_for_execution_id(&mut *tx, *execution_id).await?;
    }
    // Resume tokens for these executions: signal rows with is_resume=true.
    // Returned, not just dropped: the listener still serves each one,
    // and a plain DELETE here is exactly what left a deleted run's
    // question answerable there.
    let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_RESUME_BY_EXECUTION_IDS_RETURNING)
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await
        .context("erase executions: read a resume signal row")?;
    let removed = rows.into_iter().map(row_to_signal).collect::<anyhow::Result<Vec<_>>>()?;
    // execution is the denormalized (execution, project_id,
    // tenant_id) index seeded at ExecutionStarted time. Without this
    // delete the row outlives the journal it indexes, and
    // `list_non_terminal_execution_ids_for_project` keeps returning the erased
    // execution forever as non-terminal (its NOT EXISTS terminal check
    // passes vacuously once every event is gone), so wipe and
    // cancel_running re-sweep a ghost.
    sqlx::query("DELETE FROM execution WHERE execution_id = ANY($1)")
        .bind(&ids).execute(&mut *tx).await?;
    // The run's row in the version tree belongs to the version store,
    // not here. For one execution `clean_execution` drops it alongside this;
    // for a whole project the project's removal already took the tree.
    Ok(removed)
}

pub(crate) async fn remove_project_signals<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    project_id: uuid::Uuid,
) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_PROJECT_RETURNING)
        .bind(project_id).fetch_all(executor).await
        .context("remove project signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// Every signal of the project but the waits of `except` (the run that
/// asked for a whole-project take-down keeps its own), deleted and handed
/// back for the listener cleanup.
pub async fn remove_project_signals_except<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    project_id: uuid::Uuid,
    except: Option<ExecutionId>,
) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_PROJECT_EXCEPT_RETURNING)
        .bind(project_id)
        .bind(except.map(|c| c.to_string()))
        .fetch_all(executor)
        .await
        .context("remove project signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// Delete the signals of projects that no longer exist, handing them
/// back for the listener unregister.
pub async fn remove_signals_of_removed_projects(pool: &sqlx::PgPool) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_OF_REMOVED_PROJECTS_RETURNING)
        .fetch_all(pool)
        .await
        .context("remove the signals of removed projects: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// The keys as two parallel arrays for an `unnest` join, `''` standing
/// for the shared owner (an instance id is never empty).
fn activation_key_arrays(keys: &[weft_core::activation::ActivationKey]) -> (Vec<String>, Vec<String>) {
    keys.iter()
        .map(|k| (k.trigger.clone(), k.instance().map(|m| m.as_str().to_string()).unwrap_or_default()))
        .unzip()
}

/// Every signal the activations `keys` govern: their entry signals and
/// the waits of the runs their triggers fired.
pub(crate) async fn activation_signals<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<Vec<SignalRegistration>> {
    let (triggers, instances) = activation_key_arrays(keys);
    let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_SELECT_BY_ACTIVATIONS)
        .bind(project_id)
        .bind(&triggers)
        .bind(&instances)
        .fetch_all(executor)
        .await
        .context("activation signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// What the activations `keys` kept while their triggers were off: the
/// fires parked on their signals and the runs of theirs waiting on a
/// person, which a reactivate's choice decides about
/// (`crate::api::project::apply_reactivate_choice`).
pub(crate) async fn activation_preservation(
    pool: &sqlx::PgPool,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<weft_core::projects::PreservationCounts> {
    let (triggers, instances) = activation_key_arrays(keys);
    let (parked, suspended): (i64, i64) = sqlx::query_as(PRESERVED_BY_ACTIVATIONS)
        .bind(project_id)
        .bind(&triggers)
        .bind(&instances)
        .fetch_one(pool)
        .await
        .context("count what the activations kept")?;
    Ok(weft_core::projects::PreservationCounts { parked: parked as usize, suspended: suspended as usize })
}

/// Delete every signal the activations `keys` govern, handed back for
/// the listener cleanup.
pub(crate) async fn remove_activation_signals<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<Vec<SignalRegistration>> {
    let (triggers, instances) = activation_key_arrays(keys);
    let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_ACTIVATIONS_RETURNING)
        .bind(project_id)
        .bind(&triggers)
        .bind(&instances)
        .fetch_all(executor)
        .await
        .context("remove activation signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

pub(crate) async fn remove_signals<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    tokens: &[String],
) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_TOKENS_RETURNING)
        .bind(tokens).fetch_all(executor).await
        .context("remove signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// The columns every signal read hands back, in `SignalRow` order,
/// each behind the table prefix the query names the row by (`""` or
/// `"s."`). One list so every SELECT and DELETE ... RETURNING that
/// decodes into `SignalRow` agrees with it: a column added to the row
/// and missing from one query fails at run time, not compile time.
macro_rules! signal_columns {
    ($p:literal) => {
        concat!(
            $p, "token, ", $p, "tenant_id, ", $p, "project_id, ", $p, "execution_id, ",
            $p, "node_id, ", $p, "is_resume, ", $p, "spec_json, ", $p, "access_id, ",
            $p, "consumer_kind, ", $p, "tags, ", $p, "port_snapshot, ",
            $p, "consumer_payload, ", $p, "surface_kind, ", $p, "mount_path, ",
            $p, "auth_kind, ", $p, "auth_config, ", $p, "kind_state, ",
            $p, "kind_state_seq, ", $p, "program_json, ", $p, "setup_execution_id, ", $p, "source_version, ",
            $p, "mount_methods, ", $p, "instance_id, ", $p, "activation_trigger, ", $p, "holds"
        )
    };
}
pub(crate) use signal_columns;

const SIGNAL_SELECT_WHERE_TOKEN: &str =
    concat!("SELECT ", signal_columns!(""), " FROM signal WHERE token = $1");

/// In registration order, which is the order a run's waits are listed in.
const SIGNAL_SELECT_WHERE_EXECUTION_ID_RESUME: &str = concat!(
    "SELECT ",
    signal_columns!(""),
    " FROM signal WHERE execution_id = $1 AND is_resume ORDER BY created_at"
);

const SIGNAL_SELECT_WHERE_PROJECT: &str =
    concat!("SELECT ", signal_columns!(""), " FROM signal WHERE project_id = $1");

/// The one entry at a place: unique by `idx_signal_entry_node`.
const SIGNAL_SELECT_ENTRY_AT_PLACE: &str = concat!(
    "SELECT ",
    signal_columns!(""),
    " FROM signal WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3 \
     AND is_resume = FALSE"
);

/// The columns a registration writes over a row it replaces (and a
/// restore writes back), in the order [`bind_signal_refreshed`] binds
/// them. `kind_state_seq` is not among them: each statement moves it one
/// past the version it compares against. The row's identity (token,
/// tenant, project, instance, execution, node, is_resume, trigger) and its
/// parked fires are never among them.
// SYNC: SIGNAL_REFRESHED_COLUMNS <-> bind_signal_refreshed (same order)
//       <-> journal/fake.rs `copy_refreshed`
const SIGNAL_REFRESHED_COLUMNS: [&str; 16] = [
    "spec_json",
    "program_json",
    "setup_execution_id",
    "source_version",
    "access_id",
    "consumer_kind",
    "tags",
    "port_snapshot",
    "consumer_payload",
    "surface_kind",
    "mount_path",
    "mount_methods",
    "auth_kind",
    "auth_config",
    "kind_state",
    "holds",
];

/// Bind a registration's [`SIGNAL_REFRESHED_COLUMNS`] values, in order.
fn bind_signal_refreshed<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    sig: &'q SignalRegistration,
) -> anyhow::Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
    Ok(query
        .bind(&sig.spec_json)
        .bind(sig.program.as_ref().map(serde_json::to_value).transpose()?)
        .bind(sig.setup_execution_id)
        .bind(&sig.source_version)
        .bind(sig.access_id.as_deref())
        .bind(sig.consumer_kind.as_deref())
        .bind(&sig.tags)
        .bind(sig.port_snapshot.as_ref())
        .bind(sig.consumer_payload.as_ref().map(serde_json::to_string).transpose()?)
        .bind(&sig.surface_kind)
        .bind(sig.mount_path.as_deref())
        .bind(&sig.mount_methods)
        .bind(&sig.auth_kind)
        .bind(sig.auth_config.as_ref())
        .bind(&sig.kind_state)
        .bind(sig.holds))
}

/// `signal_insert`: the nine identity columns ($1..$9), the refreshed
/// ones, then the version the registration read (the row lands one past
/// it). On a token conflict only the refreshed columns change, and only
/// while the row is still at that version; the row's holder lets go
/// ([`LET_GO`]), so the rewritten signal comes up fresh.
static SIGNAL_INSERT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    const IDENTITY: [&str; 9] = [
        "token", "tenant_id", "project_id", "execution_id", "node_id", "is_resume", "created_at", "instance_id",
        "activation_trigger",
    ];
    let refreshed = SIGNAL_REFRESHED_COLUMNS;
    let seq = IDENTITY.len() + refreshed.len() + 1;
    let columns = IDENTITY.iter().chain(refreshed.iter()).copied().collect::<Vec<_>>().join(", ");
    let values = (1..seq).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ");
    let set = refreshed.iter().map(|c| format!("{c} = EXCLUDED.{c}")).collect::<Vec<_>>().join(", ");
    format!(
        "INSERT INTO signal ({columns}, kind_state_seq) VALUES ({values}, ${seq} + 1) \
         ON CONFLICT (token) DO UPDATE SET {set}, kind_state_seq = EXCLUDED.kind_state_seq, {LET_GO} \
         WHERE signal.kind_state_seq = EXCLUDED.kind_state_seq - 1"
    )
});

/// What a rewrite of a signal row does to its holder: its lease goes, so
/// the holder running the old row stops it at its next look, and the row
/// is claimed and brought up again as it now reads.
const LET_GO: &str = "held_by = NULL, held_until = NULL, serving = NULL";

/// `signal_restore`: the token ($1), the refreshed columns, then the
/// version the row must still be at (it moves one past).
static SIGNAL_RESTORE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let refreshed = SIGNAL_REFRESHED_COLUMNS;
    let seq = refreshed.len() + 2;
    let set = refreshed.iter().enumerate().map(|(i, c)| format!("{c} = ${}", i + 2)).collect::<Vec<_>>().join(", ");
    format!("UPDATE signal SET {set}, kind_state_seq = ${seq} + 1, {LET_GO} WHERE token = $1 AND kind_state_seq = ${seq}")
});

const SIGNAL_DELETE_BY_EXECUTION_ID_RETURNING: &str =
    concat!("DELETE FROM signal WHERE execution_id = $1 RETURNING ", signal_columns!(""));

const SIGNAL_DELETE_BY_PROJECT_RETURNING: &str =
    concat!("DELETE FROM signal WHERE project_id = $1 RETURNING ", signal_columns!(""));

const SIGNAL_DELETE_BY_PROJECT_EXCEPT_RETURNING: &str = concat!(
    // `$2` NULL keeps nothing back: `execution_id IS DISTINCT FROM NULL` would
    // spare every entry signal (their execution is NULL too).
    "DELETE FROM signal WHERE project_id = $1 AND ($2::text IS NULL OR execution_id IS DISTINCT FROM $2) RETURNING ",
    signal_columns!("")
);

const SIGNAL_DELETE_OF_REMOVED_PROJECTS_RETURNING: &str = concat!(
    "DELETE FROM signal s WHERE NOT EXISTS (SELECT 1 FROM project p WHERE p.id = s.project_id) RETURNING ",
    signal_columns!("")
);

/// The signals a set of activations governs: project `$1`, the
/// activations given as the parallel arrays `$2` (triggers) and `$3`
/// (instances, `''` for the shared one), as [`activation_key_arrays`]
/// builds them.
macro_rules! governed_by_activations {
    () => {
        "project_id = $1 \
         AND (activation_trigger, COALESCE(instance_id, '')) IN (SELECT * FROM unnest($2::text[], $3::text[]))"
    };
}

const SIGNAL_SELECT_BY_ACTIVATIONS: &str =
    concat!("SELECT ", signal_columns!(""), " FROM signal WHERE ", governed_by_activations!());

const SIGNAL_DELETE_BY_ACTIVATIONS_RETURNING: &str =
    concat!("DELETE FROM signal WHERE ", governed_by_activations!(), " RETURNING ", signal_columns!(""));

const PRESERVED_BY_ACTIVATIONS: &str = concat!(
    "SELECT COALESCE(SUM(jsonb_array_length(parked_fires)), 0)::bigint, \
            COUNT(*) FILTER (WHERE is_resume = TRUE AND jsonb_array_length(parked_fires) = 0) \
     FROM signal WHERE ",
    governed_by_activations!()
);

const SIGNAL_DELETE_BY_TOKENS_RETURNING: &str =
    concat!("DELETE FROM signal WHERE token = ANY($1) RETURNING ", signal_columns!(""));

const SIGNAL_DELETE_RESUME_BY_EXECUTION_IDS_RETURNING: &str = concat!(
    "DELETE FROM signal WHERE execution_id = ANY($1) AND is_resume = TRUE RETURNING ",
    signal_columns!("")
);

const SIGNAL_DELETE_RESUME_BY_TOKEN_RETURNING: &str = concat!(
    "DELETE FROM signal WHERE token = $1 AND is_resume = TRUE RETURNING ",
    signal_columns!("")
);

/// Row shape for signal SELECTs. `FromRow` (not a tuple) because
/// the row exceeds sqlx's 16-tuple cap.
#[derive(sqlx::FromRow)]
pub(crate) struct SignalRow {
    pub(crate) setup_execution_id: Option<uuid::Uuid>,
    pub(crate) source_version: Option<String>,
    pub(crate) program_json: Option<serde_json::Value>,
    pub(crate) token: String,
    pub(crate) tenant_id: String,
    pub(crate) project_id: uuid::Uuid,
    pub(crate) execution_id: Option<String>,
    pub(crate) node_id: String,
    pub(crate) is_resume: bool,
    pub(crate) spec_json: String,
    pub(crate) access_id: Option<String>,
    pub(crate) consumer_kind: Option<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) port_snapshot: Option<serde_json::Value>,
    pub(crate) consumer_payload: Option<String>,
    pub(crate) surface_kind: String,
    pub(crate) mount_path: Option<String>,
    pub(crate) mount_methods: Vec<String>,
    pub(crate) instance_id: Option<String>,
    pub(crate) activation_trigger: Option<String>,
    pub(crate) auth_kind: String,
    pub(crate) auth_config: Option<serde_json::Value>,
    pub(crate) kind_state: serde_json::Value,
    pub(crate) kind_state_seq: i64,
    pub(crate) holds: bool,
}

pub(crate) fn row_to_signal(row: SignalRow) -> anyhow::Result<SignalRegistration> {
    // Distinguish a NULL column (a legitimately absent value) from a NON-NULL value
    // that fails to decode (corrupt state). A resume signal's `execution_id` is matched by
    // the fire/resume path to route the signal to its suspended execution: silently
    // collapsing a corrupt execution to `None` would make that execution unresumable
    // with no error, so a present-but-unparseable execution fails LOUD here.
    let execution_id = match row.execution_id {
        None => None,
        Some(s) => Some(
            s.parse::<ExecutionId>()
                .map_err(|e| anyhow::anyhow!("corrupt signal.execution '{s}' for token {}: {e}", row.token))?,
        ),
    };
    // The payload is the CACHE of what a consumer surface renders,
    // rebuilt from the spec on every re-register and read by nothing
    // else. One unreadable cache must not make the row unreadable: this
    // decoder is on the path that cancels and deletes signals, so
    // failing here would leave a row nobody can list and nobody can
    // remove. It is loud in the log and the row keeps its identity.
    let consumer_payload = match row.consumer_payload {
        None => None,
        Some(s) => match serde_json::from_str(&s) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::error!(
                    target: "weft_dispatcher::journal",
                    token = %row.token,
                    error = %e,
                    "corrupt signal.consumer_payload: this signal is missing from every \
                     consumer's list. A trigger's row rebuilds its card the next time the \
                     project is activated; a row waiting mid-execution never does, so that \
                     one stays invisible until the execution is cancelled (weft stop, or \
                     clear-all on the token)."
                );
                None
            }
        },
    };
    Ok(SignalRegistration {
        setup_execution_id: row.setup_execution_id,
        instance: row
            .instance_id
            .map(weft_core::instance::InstanceId::new)
            .transpose()
            .map_err(|e| anyhow::anyhow!("corrupt signal.instance_id for token {}: {e}", row.token))?,
        activation_trigger: row.activation_trigger,
        source_version: row.source_version,
        program: row.program_json.map(serde_json::from_value).transpose()?,
        token: row.token,
        tenant_id: row.tenant_id,
        project_id: row.project_id,
        execution_id,
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
        mount_methods: row.mount_methods,
        auth_kind: row.auth_kind,
        auth_config: row.auth_config,
        kind_state: row.kind_state,
        kind_state_seq: row.kind_state_seq,
        holds: row.holds,
    })
}

/// Revoke instance tokens of `instance` in `project_id`: the one `id`, or
/// every one of them. What a program's `ctx.tokens().instance(..).revoke()`
/// does; answers how many went.
// SYNC: signal_token instance rows <-> crates/weft-broker/src/program_tokens.rs (the program's mint)
pub async fn revoke_instance_tokens(
    pool: &sqlx::PgPool,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &weft_core::instance::InstanceId,
    id: Option<uuid::Uuid>,
) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM signal_token \
         WHERE tenant_id = $1 AND instance_id = $2 AND allowed_projects = ARRAY[$3]::uuid[] \
           AND ($4::uuid IS NULL OR id = $4)",
    )
    .bind(tenant)
    .bind(instance.as_str())
    .bind(project_id)
    .bind(id)
    .execute(pool)
    .await?
    .rows_affected())
}

/// How many instance tokens of `project_id` still work (not expired at
/// `now_unix`), per instance, one entry per instance with at least one.
pub async fn instance_token_counts(
    pool: &sqlx::PgPool,
    tenant: &str,
    project_id: uuid::Uuid,
    now_unix: i64,
) -> anyhow::Result<Vec<(weft_core::instance::InstanceId, u32)>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT instance_id, count(*)::bigint FROM signal_token \
         WHERE tenant_id = $1 AND instance_id IS NOT NULL AND allowed_projects = ARRAY[$2]::uuid[] \
           AND expires_at > $3 \
         GROUP BY instance_id ORDER BY instance_id",
    )
    .bind(tenant)
    .bind(project_id)
    .bind(now_unix)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(instance, n)| {
            let instance = weft_core::instance::InstanceId::new(instance).map_err(|e| anyhow::anyhow!("signal_token.instance_id: {e}"))?;
            Ok((instance, u32::try_from(n)?))
        })
        .collect()
}

/// Revoke every instance token of `project_id`: an instance token acts in
/// its one project, so it outlives a removed project as nothing anybody
/// could use.
pub(crate) async fn revoke_project_instance_tokens<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    project_id: uuid::Uuid,
) -> anyhow::Result<u64> {
    Ok(sqlx::query("DELETE FROM signal_token WHERE instance_id IS NOT NULL AND allowed_projects = ARRAY[$1]::uuid[]")
        .bind(project_id)
        .execute(executor)
        .await?
        .rows_affected())
}

/// The `signal_token` SELECT row shape (id, token_hash, recognizer, tenant_id,
/// name, allowed_projects, allowed_tags, allowed_displays, all_displays,
/// created_at, instance_id, expires_at, kind). One tuple type so both readers decode it through the single
/// fallible `row_to_signal_token`.
type SignalTokenRow = (
    uuid::Uuid,
    String,
    String,
    String,
    Option<String>,
    Vec<uuid::Uuid>,
    Vec<String>,
    Vec<String>,
    bool,
    i64,
    Option<String>,
    Option<i64>,
    String,
);

fn row_to_signal_token(row: SignalTokenRow) -> anyhow::Result<SignalToken> {
    let (
        id,
        token_hash,
        recognizer,
        tenant_id,
        name,
        projects,
        tags,
        displays,
        all_displays,
        created_at,
        instance,
        expires_at,
        kind,
    ) = row;
    let kind = weft_core::signal_token::TokenKind::parse(&kind).ok_or_else(|| anyhow::anyhow!(
        "signal_token row {id} has unknown kind '{kind}'; must be one of: {}",
        weft_core::signal_token::TokenKind::accepted()
    ))?;
    Ok(SignalToken {
        id,
        kind,
        token_hash,
        recognizer,
        tenant_id,
        name,
        allowed_projects: projects,
        allowed_tags: tags,
        allowed_displays: displays,
        all_displays,
        created_at: created_at as u64,
        instance: instance
            .map(weft_core::instance::InstanceId::new)
            .transpose()
            .map_err(|e| anyhow::anyhow!("signal_token {id}: corrupt instance_id: {e}"))?,
        expires_at: expires_at.map(|at| at as u64),
    })
}

// `crate::lease::now_unix` is the canonical wall-clock reader.
