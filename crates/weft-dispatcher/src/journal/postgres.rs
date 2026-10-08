//! Postgres-backed journal. Multiple dispatchers share one
//! Postgres database; the run rows, their records and the signal tables
//! are the durable state. Postgres is the source of truth, every
//! dispatcher process is a stateless reader/writer.
//!
//! Applies no schema of its own: the boot's `app::apply_core_schema`
//! runs every group (this crate's [`GROUP`] included) in one pass.

use anyhow::Context;
use async_trait::async_trait;
use sqlx::postgres::PgPool;

use weft_core::ExecutionId;

use weft_journal::record::{Appended, Queued, Then};
use weft_journal::ExecEvent;
use crate::journal::{CancelWrite, ExecutionOwner, ExecutionQuery, Journal, LogEntry, SignalRegistration, SignalToken};
use weft_core::program::{ExecutionPage, ExecutionSummary, RunStatus};

pub struct PostgresJournal {
    pool: PgPool,
}

/// The `run.phase` column as a phase. The column is NOT NULL and only
/// ever written from `Phase::as_str`, so an unreadable value is a broken
/// row; it is logged and answered as a fire so the row still appears (in
/// the listing, on the point-get, and to the running set the cancel and
/// wipe sweeps read, which must never be the thing a bad row breaks).
pub(crate) fn phase_from_column(text: &str) -> weft_core::context::Phase {
    weft_core::context::Phase::from_tag(text).unwrap_or_else(|| {
        tracing::warn!(
            target: "weft_dispatcher::journal",
            phase = %text,
            "run row holds an unknown phase; answered as a fire"
        );
        weft_core::context::Phase::Fire
    })
}

/// The columns a summary is read from, over `run r`, in [`SummaryRow`]
/// order: the run's own columns, its tags, and whether it is parked on a
/// wait.
// SYNC: SUMMARY_COLUMNS <-> SummaryRow
const SUMMARY_COLUMNS: &str = concat!(
    "r.execution_id, r.project_id, r.entry_node, r.phase, r.state, r.started_at, r.ended_at, r.outcome, \
     r.error, r.cancel_cause, r.skipped, r.instance_id, ",
    "(SELECT COALESCE(array_agg(tag ORDER BY seq), '{}') FROM execution_tag WHERE execution_id = r.execution_id)"
);

/// One run's summary columns ([`SUMMARY_COLUMNS`]).
#[derive(sqlx::FromRow)]
struct SummaryRow {
    execution_id: ExecutionId,
    project_id: uuid::Uuid,
    entry_node: Option<String>,
    phase: String,
    state: String,
    started_at: i64,
    ended_at: Option<i64>,
    outcome: Option<String>,
    error: Option<String>,
    cancel_cause: Option<sqlx::types::Json<weft_core::exec::CancelCause>>,
    skipped: i32,
    instance_id: Option<String>,
    #[sqlx(rename = "coalesce")]
    tags: Vec<String>,
}

impl SummaryRow {
    /// The summary the row says: where the run stands is its state (a
    /// parked run waits for input), and how it ended its outcome.
    fn summary(self) -> anyhow::Result<ExecutionSummary> {
        let status = match (self.state.as_str(), self.outcome.as_deref()) {
            ("ended", Some(outcome)) => match weft_journal::record::Outcome::parse(outcome) {
                Some(weft_journal::record::Outcome::Completed) => RunStatus::Completed,
                Some(weft_journal::record::Outcome::Failed) => RunStatus::Failed,
                Some(weft_journal::record::Outcome::Cancelled) => RunStatus::Cancelled,
                None => anyhow::bail!("run {} holds an unknown outcome '{outcome}'", self.execution_id),
            },
            ("parked", _) => RunStatus::WaitingForInput,
            ("running" | "queued", _) => RunStatus::Running,
            (state, outcome) => anyhow::bail!("run {} holds state '{state}' with outcome {outcome:?}", self.execution_id),
        };
        Ok(ExecutionSummary {
            execution_id: self.execution_id,
            project_id: self.project_id,
            entry_node: self.entry_node.unwrap_or_default(),
            status,
            phase: phase_from_column(&self.phase),
            started_at: self.started_at as u64,
            completed_at: self.ended_at.map(|at| at as u64),
            tags: self.tags,
            cancel_cause: self.cancel_cause.map(|cause| cause.0),
            error: self.error,
            skipped_nodes: self.skipped.max(0) as u64,
            instance: self
                .instance_id
                .map(weft_core::instance::InstanceId::new)
                .transpose()
                .map_err(|e| anyhow::anyhow!("run {} holds a broken instance: {e}", self.execution_id))?,
        })
    }
}

impl PostgresJournal {
    /// Wrap an EXISTING pool. Pure: the schema (this crate's [`GROUP`]
    /// included) is applied once, before construction, by
    /// [`crate::app::apply_core_schema`].
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Expose the inner pool for sibling modules (lease manager,
    /// EventBus pub/sub) so they can share connections.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Answer the wait `token` with `value`, on the caller's transaction:
/// the wait's signal goes (a wait is answered once), and the answer
/// reaches its run. A run nobody drives (parked, queued) gets it in its
/// record, `SuspensionResolved` at `last_seq + 1`, and is queued for a
/// worker to carry on; a run a worker drives is written by that worker
/// alone, so the answer is handed to it
/// (`weft_task_store::parked_fires::hand_answer_in`). The run's row is
/// locked before the signal is touched, the order every writer of a run
/// takes them in. The caller pokes the outbox once it commits.
pub(crate) async fn answer_in(
    tx: &mut sqlx::PgConnection,
    token: &str,
    value: &serde_json::Value,
    from: AnswerFrom,
) -> anyhow::Result<crate::journal::Answered> {
    use crate::journal::Answered;
    let run: Option<Option<ExecutionId>> = sqlx::query_scalar("SELECT execution_id FROM signal WHERE token = $1 AND is_resume")
        .bind(token)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(run) = run else { return Ok(Answered::Gone) };
    let execution_id = run.with_context(|| format!("the wait {token} names no run"))?;
    let locked = weft_journal::record::lock_in(&mut *tx, execution_id).await?;
    if from == AnswerFrom::Sender {
        // An answer queued while the run's trigger was not live is the
        // wait's one answer (its sender was told so): the queue's drain
        // hands it over, and this one finds the wait answered. The signal's
        // row is locked first, and `park` holds that lock while it queues
        // one, so one queued meanwhile is seen here.
        let signal: Option<i32> = sqlx::query_scalar("SELECT 1 FROM signal WHERE token = $1 AND is_resume FOR UPDATE")
            .bind(token)
            .fetch_optional(&mut *tx)
            .await?;
        let queued: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM parked_fire WHERE token = $1 AND is_resume AND execution_id IS NULL)",
        )
        .bind(token)
        .fetch_one(&mut *tx)
        .await?;
        if signal.is_none() || queued {
            return Ok(Answered::Gone);
        }
    }
    let Some(row) = sqlx::query_as::<_, SignalRow>(SIGNAL_DELETE_RESUME_BY_TOKEN_RETURNING)
        .bind(token)
        .fetch_optional(&mut *tx)
        .await
        .context("answer a wait: read its signal row")?
    else {
        return Ok(Answered::Gone);
    };
    let consumed = row_to_signal(row)?;
    let Some(locked) = locked.filter(|locked| locked.state != "ended") else { return Ok(Answered::RunEnded { consumed }) };
    if locked.owner.is_some() {
        weft_task_store::parked_fires::hand_answer_in(&mut *tx, token, execution_id, value).await?;
        return Ok(Answered::Reached { consumed });
    }
    let resolved = ExecEvent::SuspensionResolved {
        execution_id,
        token: token.to_string(),
        value: value.clone(),
        at_unix: crate::lease::now_unix() as u64,
    };
    match weft_journal::record::append_locked_in(&mut *tx, execution_id, &locked, std::slice::from_ref(&resolved), weft_journal::record::DISPATCHER, Then::Queued).await? {
        Appended::At(_) => Ok(Answered::Reached { consumed }),
        other => anyhow::bail!("run {execution_id} was locked with no owner, yet its answer was not written: {other:?}"),
    }
}

/// Who answers a wait, for [`answer_in`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnswerFrom {
    /// Its sender, now: refused once an answer is queued for the wait.
    Sender,
    /// The queue's drain, handing over the answer queued for it.
    Queue,
}

/// End `execution_id`, a run nobody drives, cancelled for `cause`, on the
/// caller's transaction with its row locked as `locked`: `NodeCancelled`
/// per open node, then `ExecutionCancelled`, from the ONE shared
/// definition of that list (`cancel_terminal_events`). Answers the
/// per-node count written.
async fn cancel_unowned_in(
    tx: &mut sqlx::PgConnection,
    execution_id: ExecutionId,
    locked: &weft_journal::record::Locked,
    program: Option<&weft_core::ProjectDefinition>,
    cause: &weft_core::exec::CancelCause,
) -> anyhow::Result<usize> {
    let events = weft_journal::record::read_record(&mut *tx, execution_id, None).await?.events(execution_id).map_err(anyhow::Error::msg)?;
    let now = crate::lease::now_unix() as u64;
    let writes = crate::api::execution::cancel_terminal_events(execution_id, &events, program, cause, now)?;
    let node_cancellations = writes.len() - 1;
    match weft_journal::record::append_locked_in(&mut *tx, execution_id, locked, &writes, weft_journal::record::DISPATCHER, Then::Stays).await? {
        Appended::At(_) => Ok(node_cancellations),
        other => anyhow::bail!("run {execution_id} was locked with no owner, yet its cancel was not written: {other:?}"),
    }
}

/// The journal's schema. First in `app::ALL_GROUPS` (other groups'
/// triggers attach to its tables); applied by the
/// boot's one `apply_core_schema` pass, never here.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    // Named for the table it first held; it is the run record's group.
    name: "exec_event",
    tables: &["run", "run_log", "run_selection", "signal_token", "signal", "execution_tag", "trigger_setup", "trigger_bake"],
    ddl: &[
        // One row per trigger setup in flight. Several may run at once for
        // one project (each activation claims its own triggers, one instance's
        // at a time), so the setup's own execution is the key.
        r#"CREATE TABLE IF NOT EXISTS trigger_setup (
            project_id UUID NOT NULL,
            execution_id UUID PRIMARY KEY
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
        // run: a run's whole life on one row. Born by the worker that bears
        // it (its first batch, `weft_record_batch`) or queued by the
        // dispatcher (`weft run`, setup runs), claimed by a worker
        // (`state = 'running'`, `owner`, `epoch` raised), let go of
        // (`parked` on a wait, `queued` for a hand-back), and ended. Only
        // the owner writes a running run's record, in its order; anybody
        // else writes a run with no owner, under its row lock, at
        // `last_seq + 1`. Everything the listing, `weft status` and
        // `ctx.runs()` say about a run is a column here.
        // SYNC: run's columns <-> weft_journal::record (the batch and the queued birth), weft_journal::read::RunRow
        r#"CREATE TABLE IF NOT EXISTS run (
            execution_id UUID PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- fire | trigger_setup | infra_setup | ... (`Phase::as_str`)
            phase TEXT NOT NULL,
            -- execution | node_test (`RunKind`)
            kind TEXT NOT NULL,
            -- fast | durable (`Keeping`)
            keeping TEXT NOT NULL,
            -- false: a run that keeps no record, written down only because
            -- it failed, reported a cost or stored a file.
            recorded BOOLEAN NOT NULL,
            entry_node TEXT,
            instance_id TEXT,
            fired_by TEXT,
            source_version TEXT,
            -- NULL for a run of no program (a node test).
            definition_hash TEXT,
            binary_hash TEXT,
            -- The digest of its selection (`run_selection`), NULL for a run
            -- of the whole program.
            selection TEXT,
            -- The run it was seeded from (`weft run --seed`), the places
            -- it ran again rather than inherit, what it was asked to run
            -- (`weft_core::run_spec::RunSpec`, as resolved) and the saved
            -- example it came from: what the version tree shows of a run
            -- started by hand. NULL and empty for every other run.
            seed_of UUID,
            stale TEXT[] NOT NULL DEFAULT '{}',
            spec JSONB,
            example TEXT,
            -- running | parked | queued | ended
            state TEXT NOT NULL,
            -- The worker replica driving it, while it runs.
            owner TEXT,
            -- The owner's fencing token: raised by every claim and by the
            -- lost-run sweep, so a batch from an owner that lost the run is
            -- refused.
            epoch INTEGER NOT NULL DEFAULT 1,
            -- The `seq` of its last record row.
            last_seq INTEGER NOT NULL,
            -- How many times it was handed to a worker, this time included.
            attempts INTEGER NOT NULL DEFAULT 1,
            -- Until when (unix seconds) a queued run counts as handed to a
            -- worker that has not claimed it yet.
            delivered_until BIGINT,
            -- A cancel waiting for its owner to write the run's ending.
            cancel_requested JSONB,
            -- It holds resume signals the dispatcher has to take down now
            -- that it ended (set by its ending, cleared once handled).
            holds_signals BOOLEAN NOT NULL DEFAULT FALSE,
            -- Somebody waits on its ending (a trigger setup, baked from
            -- it); cleared once the dispatcher handled it.
            watch_end BOOLEAN NOT NULL DEFAULT FALSE,
            started_at BIGINT NOT NULL,
            ended_at BIGINT,
            -- completed | failed | cancelled
            outcome TEXT,
            error TEXT,
            cancel_cause JSONB,
            -- Node firings it skipped.
            skipped INTEGER NOT NULL DEFAULT 0,
            -- It stored files of its own, which its ending's sweep reclaims.
            wrote_files BOOLEAN NOT NULL DEFAULT FALSE,
            -- What its metered calls cost, in micro-dollars.
            cost_micro_usd BIGINT NOT NULL DEFAULT 0,
            -- How long it is kept once it ended, in seconds (NULL: for
            -- ever), and the moment that runs out, set when it ends.
            keep_for BIGINT,
            keep_until BIGINT
        ) WITH (fillfactor = 85)"#,
        // The listing: a project's runs, newest first.
        r#"CREATE INDEX IF NOT EXISTS run_listing ON run (project_id, started_at DESC)"#,
        // A project's runs that have not ended (queued, running, parked):
        // what a take-down reaches, what a drain counts, what `weft status`
        // shows going. Never holds a run born and ended in one batch.
        r#"CREATE INDEX IF NOT EXISTS run_live ON run (project_id) WHERE state <> 'ended'"#,
        // What is being worked on, by whom (the one in-flight predicate,
        // `weft_task_store::in_flight`).
        r#"CREATE INDEX IF NOT EXISTS run_in_flight ON run (owner) WHERE state = 'running'"#,
        // What delivery hands to workers.
        r#"CREATE INDEX IF NOT EXISTS run_queued ON run (started_at) WHERE state = 'queued'"#,
        // What retention deletes. Keyed on a column delivery never
        // changes, so a delivery lease update stays in place.
        r#"CREATE INDEX IF NOT EXISTS run_expiry ON run (keep_until) WHERE state = 'ended'"#,
        // Ended runs whose ending the dispatcher has yet to handle.
        r#"CREATE INDEX IF NOT EXISTS run_end_unhandled ON run (project_id)
             WHERE state = 'ended' AND (watch_end OR holds_signals)"#,
        // run_log: a run's record, one row per run per write, in the run's
        // order: the write's events, compressed (`weft_journal::stored`).
        r#"CREATE TABLE IF NOT EXISTS run_log (
            execution_id UUID NOT NULL,
            seq INTEGER NOT NULL,
            events BYTEA NOT NULL,
            -- The replica that wrote it.
            writer TEXT NOT NULL,
            written_at BIGINT NOT NULL,
            PRIMARY KEY (execution_id, seq)
        )"#,
        // Already compressed: Postgres compressing it again gains nothing.
        r#"ALTER TABLE run_log ALTER COLUMN events SET STORAGE EXTERNAL"#,
        // Each run selection once, by digest
        // (`weft_core::project::selection::RecordedSelection`).
        r#"CREATE TABLE IF NOT EXISTS run_selection (
            digest TEXT PRIMARY KEY,
            selection JSONB NOT NULL
        )"#,
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
            execution_id UUID,
            node_id TEXT NOT NULL,
            -- A run's wait (it names its run) or an entry (it names none):
            -- never one without the other.
            is_resume BOOLEAN NOT NULL CHECK (is_resume = (execution_id IS NOT NULL)),
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
        // Tell a project's workers one of its triggers changed, so their
        // copy of them (`weft_engine::door`) is read again: an entry coming,
        // going, changing anything a door admits by, or its holder changing.
        // SYNC: 'weft_triggers' <-> weft_broker_client::line::TRIGGERS_CHANNEL
        r#"CREATE OR REPLACE FUNCTION triggers_notify_project() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_triggers', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_triggers', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS signal_triggers_on_insert ON signal"#,
        r#"CREATE TRIGGER signal_triggers_on_insert
            AFTER INSERT ON signal
            FOR EACH ROW
            WHEN (NOT NEW.is_resume)
            EXECUTE FUNCTION triggers_notify_project()"#,
        r#"DROP TRIGGER IF EXISTS signal_triggers_on_delete ON signal"#,
        r#"CREATE TRIGGER signal_triggers_on_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            WHEN (NOT OLD.is_resume)
            EXECUTE FUNCTION triggers_notify_project()"#,
        r#"DROP TRIGGER IF EXISTS signal_triggers_on_change ON signal"#,
        r#"CREATE TRIGGER signal_triggers_on_change
            AFTER UPDATE OF surface_kind, mount_path, mount_methods, project_id, node_id, spec_json,
                auth_kind, auth_config, port_snapshot, program_json, source_version, instance_id,
                activation_trigger, held_by ON signal
            FOR EACH ROW
            WHEN (NOT NEW.is_resume)
            EXECUTE FUNCTION triggers_notify_project()"#,
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
        // execution_tag: the selectable copy of `ctx.tag_execution`, one
        // row per (execution, tag), written by the broker on the worker's
        // behalf once the run's own `ExecutionTagged` is on record.
        // `ctx.stop_tagged` selects on it: "every live execution of this
        // project carrying tag T" is one indexed read joined to `run`
        // instead of a fold over every open record.
        // `seq` is the order tags were written, gap-tolerant and never
        // tied, which is what the last-one-wins rule compares (unix
        // seconds would tie two runs of the same user inside one
        // second). The (execution, tag) uniqueness is what makes a body
        // replayed after a durable wait land on the same row instead of moving
        // the run's place in the order. Rows go with the run.
        r#"CREATE TABLE IF NOT EXISTS execution_tag (
            seq BIGSERIAL PRIMARY KEY,
            execution_id UUID NOT NULL,
            tag TEXT NOT NULL,
            tagged_at_unix BIGINT NOT NULL,
            UNIQUE (execution_id, tag)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_tag_tag ON execution_tag(tag)"#,
        // THE write of a worker's records (`weft_journal::record::record_batch`):
        // one call, one transaction, for every run of one batch of one of
        // the worker's writer lanes. Synced to disk only when somebody
        // waits on it (`p_durable`, a durable run at a commit point); a
        // batch of fast runs commits without waiting for the disk.
        //
        // Per run (the `p_ids` arrays, aligned; `p_born` and `p_ended` are
        // JSON arrays aligned the same way, `null` where it does not apply):
        //   - a run that starts here (`first_seq = 0`) is inserted, already
        //     ended when it ends in the same batch;
        //   - a run that goes on is applied only when it follows on from its
        //     record exactly, under this owner and epoch, while running;
        //   - a batch sent again (its answer was lost) finds its own last row
        //     there and is "already_applied", changing nothing;
        //   - a run another worker bore (a retried fire) is
        //     "born_elsewhere", and anything else is "refused".
        // Rows are locked in id order, the order every writer of run rows
        // takes them in. Only the runs applied now bring their rows, their
        // answers taken (`p_resolved_*`, by run ordinal), their version
        // counts (on this lane's own row), their search queue entry and
        // their storage sweep. Answers each run's fate, its project, and
        // whether it ended now with somebody to tell (`watch_end`,
        // `holds_signals`), in the order given.
        // SYNC: weft_record_batch's parameters and answer <-> weft_journal::record::record_batch
        r#"CREATE OR REPLACE FUNCTION weft_record_batch(
                p_writer TEXT, p_tenant TEXT, p_lane TEXT, p_now BIGINT, p_durable BOOLEAN,
                p_ids UUID[], p_epochs INTEGER[], p_first INTEGER[], p_last INTEGER[],
                p_costs BIGINT[], p_skipped INTEGER[], p_wrote BOOLEAN[], p_born JSONB, p_ended JSONB,
                p_row_run INTEGER[], p_row_seq INTEGER[], p_row_events BYTEA[],
                p_resolved_run INTEGER[], p_resolved_token TEXT[],
                p_selections JSONB
            ) RETURNS TABLE (o_execution_id UUID, o_fate TEXT, o_project_id UUID, o_tell_end BOOLEAN) AS $$
            DECLARE
                v_fates TEXT[];
                v_inserted BIGINT;
            BEGIN
                PERFORM set_config('synchronous_commit', CASE WHEN p_durable THEN 'on' ELSE 'off' END, true);
                INSERT INTO run_selection (digest, selection)
                    SELECT s->>'digest', s->'selection' FROM jsonb_array_elements(p_selections) AS s
                    ON CONFLICT (digest) DO NOTHING;
                PERFORM 1 FROM run r WHERE r.execution_id = ANY(p_ids) ORDER BY r.execution_id FOR UPDATE;
                SELECT array_agg(CASE
                        WHEN v.first_seq = 0 AND r.execution_id IS NULL THEN 'insert'
                        WHEN v.first_seq > 0 AND r.last_seq = v.first_seq - 1 AND r.owner = p_writer
                             AND r.epoch = v.epoch AND r.state = 'running' THEN 'continue'
                        WHEN EXISTS (SELECT 1 FROM run_log l
                                     WHERE l.execution_id = v.id AND l.seq = v.last_seq AND l.writer = p_writer) THEN 'already_applied'
                        WHEN v.first_seq = 0 THEN 'born_elsewhere'
                        ELSE 'refused' END ORDER BY v.n)
                    INTO v_fates
                    FROM unnest(p_ids, p_epochs, p_first, p_last) WITH ORDINALITY AS v(id, epoch, first_seq, last_seq, n)
                    LEFT JOIN run r ON r.execution_id = v.id;
                WITH born AS (
                    INSERT INTO run (execution_id, project_id, tenant_id, phase, kind, keeping, recorded, entry_node,
                                     instance_id, fired_by, source_version, definition_hash, binary_hash, selection,
                                     seed_of, state, owner, epoch, last_seq, started_at, ended_at, outcome, error,
                                     cancel_cause, skipped, wrote_files, cost_micro_usd, keep_for, keep_until)
                    SELECT v.id, (x.b->>'project_id')::uuid, p_tenant, x.b->>'phase', x.b->>'kind', x.b->>'keeping',
                           (x.b->>'recorded')::boolean, x.b->>'entry_node', x.b->>'instance_id', x.b->>'fired_by',
                           x.b->>'source_version', x.b->>'definition_hash', x.b->>'binary_hash', x.b->>'selection',
                           (x.b->>'seed_of')::uuid,
                           CASE WHEN x.e IS NULL THEN 'running' ELSE 'ended' END,
                           CASE WHEN x.e IS NULL THEN p_writer END,
                           v.epoch, v.last_seq, (x.b->>'started_at')::bigint, (x.e->>'at')::bigint, x.e->>'outcome',
                           x.e->>'error', x.e->'cancel_cause', v.skipped, v.wrote, v.costs, (x.b->>'keep_for')::bigint,
                           (x.e->>'at')::bigint + (x.b->>'keep_for')::bigint
                    FROM unnest(p_ids, p_epochs, p_last, p_costs, p_skipped, p_wrote, v_fates)
                            WITH ORDINALITY AS v(id, epoch, last_seq, costs, skipped, wrote, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b,
                                                   NULLIF(p_ended->(v.n::integer - 1), 'null'::jsonb) AS e) x
                    WHERE v.fate = 'insert'
                    ORDER BY v.id
                    ON CONFLICT (execution_id) DO NOTHING
                    RETURNING 1
                ) SELECT count(*) INTO v_inserted FROM born;
                -- A run another worker inserted between the read above and
                -- this insert (both bore one fire's run) is theirs.
                IF v_inserted < (SELECT count(*) FROM unnest(v_fates) AS f(fate) WHERE f.fate = 'insert') THEN
                    SELECT array_agg(CASE WHEN v.fate = 'insert' AND r.xmin <> xid(pg_current_xact_id())
                                          THEN 'born_elsewhere' ELSE v.fate END ORDER BY v.n)
                        INTO v_fates
                        FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.execution_id = v.id;
                END IF;
                UPDATE run r SET
                        last_seq = v.last_seq,
                        cost_micro_usd = r.cost_micro_usd + v.costs,
                        skipped = r.skipped + v.skipped,
                        wrote_files = r.wrote_files OR v.wrote,
                        state = CASE WHEN x.e IS NULL THEN r.state ELSE 'ended' END,
                        owner = CASE WHEN x.e IS NULL THEN r.owner END,
                        ended_at = (x.e->>'at')::bigint,
                        outcome = x.e->>'outcome',
                        error = x.e->>'error',
                        cancel_cause = x.e->'cancel_cause',
                        keep_until = (x.e->>'at')::bigint + r.keep_for,
                        holds_signals = x.e IS NOT NULL AND EXISTS (SELECT 1 FROM signal s WHERE s.execution_id = r.execution_id)
                    FROM unnest(p_ids, p_last, p_costs, p_skipped, p_wrote, v_fates)
                            WITH ORDINALITY AS v(id, last_seq, costs, skipped, wrote, fate, n)
                        CROSS JOIN LATERAL (SELECT NULLIF(p_ended->(v.n::integer - 1), 'null'::jsonb) AS e) x
                    WHERE v.fate = 'continue' AND r.execution_id = v.id;
                INSERT INTO run_log (execution_id, seq, events, writer, written_at)
                    SELECT p_ids[w.run], w.seq, w.events, p_writer, p_now
                    FROM unnest(p_row_run, p_row_seq, p_row_events) AS w(run, seq, events)
                    WHERE v_fates[w.run] IN ('insert', 'continue')
                    ORDER BY 1, 2
                    ON CONFLICT (execution_id, seq) DO NOTHING;
                DELETE FROM parked_fire pf
                    USING unnest(p_resolved_run, p_resolved_token) AS t(run, token)
                    WHERE v_fates[t.run] IN ('insert', 'continue') AND pf.token = t.token AND pf.is_resume;
                -- An answer handed to a run that ended without taking it
                -- has nobody left to take it.
                DELETE FROM parked_fire pf
                    USING unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND pf.execution_id = v.id;
                INSERT INTO version_runs (project_id, source_version, lane, runs, last_run)
                    SELECT (x.b->>'project_id')::uuid, x.b->>'source_version', p_lane, count(*),
                           (array_agg(v.id ORDER BY v.id DESC))[1]
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b) x
                    WHERE v.fate = 'insert' AND x.b->>'source_version' IS NOT NULL
                      AND x.b->>'kind' = 'execution' AND (x.b->>'recorded')::boolean
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                    ON CONFLICT (project_id, source_version, lane) DO UPDATE
                        SET runs = version_runs.runs + EXCLUDED.runs,
                            last_run = GREATEST(version_runs.last_run, EXCLUDED.last_run);
                INSERT INTO run_search_queue (execution_id)
                    SELECT r.execution_id
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.execution_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND r.recorded
                    ON CONFLICT (execution_id) DO NOTHING;
                INSERT INTO storage_sweep (execution_id, tenant_id, enqueued_at_unix)
                    SELECT r.execution_id, r.tenant_id, p_now
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.execution_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended->(v.n::integer - 1) <> 'null'::jsonb
                      AND r.wrote_files
                    ON CONFLICT (execution_id) DO NOTHING;
                RETURN QUERY
                    SELECT v.id,
                           CASE WHEN v.fate IN ('insert', 'continue') THEN 'accepted' ELSE v.fate END,
                           r.project_id,
                           COALESCE(v.fate IN ('insert', 'continue') AND r.state = 'ended'
                                    AND (r.watch_end OR r.holds_signals), FALSE)
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.execution_id = v.id
                    ORDER BY v.n;
            END;
            $$ LANGUAGE plpgsql"#,
    ],
    seed: &[],
};

#[async_trait]
impl Journal for PostgresJournal {
    async fn is_trigger_setup_pending(&self, execution_id: ExecutionId) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM trigger_setup WHERE execution_id = $1)")
            .bind(execution_id).fetch_one(&self.pool).await?)
    }

    async fn finish_trigger_setup(&self, execution_id: ExecutionId, bake: Option<&super::TriggerBake>) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        let owner: Option<(uuid::Uuid,)> = sqlx::query_as(
            "DELETE FROM trigger_setup WHERE execution_id = $1 RETURNING project_id",
        ).bind(execution_id).fetch_optional(&mut *tx).await?;
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

    async fn append(&self, execution_id: ExecutionId, events: &[ExecEvent], then: Then) -> anyhow::Result<Appended> {
        let mut tx = self.pool.begin().await?;
        let appended = weft_journal::record::append_unowned_in(&mut tx, execution_id, events, weft_journal::record::DISPATCHER, then).await?;
        tx.commit().await?;
        if matches!(appended, Appended::At(_)) {
            // The write announced its row (and its ending, its storage
            // sweep) through the outbox.
            weft_task_store::announce::committed(&self.pool);
        }
        Ok(appended)
    }

    async fn events_log_lossy(
        &self,
        execution_id: ExecutionId,
    ) -> anyhow::Result<(Vec<crate::events::IdentifiedEvent<ExecEvent>>, Vec<String>)> {
        let mut conn = self.pool.acquire().await?;
        let record = weft_journal::record::read_record(&mut conn, execution_id, None).await?;
        let (rows, bad) = record.decode_lossy(execution_id);
        Ok((rows.into_iter().map(|row| crate::events::IdentifiedEvent::recorded(row.seq, row.index, row.event)).collect(), bad))
    }

    async fn queue_run(&self, queued: Queued<'_>, for_activation: bool) -> anyhow::Result<bool> {
        let birth = queued.events.first().context("a run is queued with its birth")?;
        let execution_id = birth.execution_id();
        let ExecEvent::ExecutionStarted { project_id, phase, .. } = birth else {
            anyhow::bail!("run {execution_id} is queued with a row that is not its birth");
        };
        let mut tx = self.pool.begin().await?;
        let project_known: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM project WHERE id = $1)")
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;
        anyhow::ensure!(project_known, "run {execution_id} cannot be queued: project {project_id} has no row; register the project first");
        if !weft_journal::record::insert_queued_in(&mut tx, &queued).await? {
            return Ok(false);
        }
        // A trigger setup is queued only while the activation that asked
        // for it still owns its rows (a cancel between the claim and here
        // wins), and is recorded as in flight.
        if *phase == weft_core::context::Phase::TriggerSetup {
            if for_activation {
                let owned: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM trigger_activation \
                     WHERE project_id = $1 AND activating_execution_id = $2 AND status = 'activating' FOR UPDATE)",
                )
                .bind(project_id)
                .bind(execution_id)
                .fetch_one(&mut *tx)
                .await?;
                anyhow::ensure!(owned, "activation {execution_id} ended before its trigger setup could start");
            }
            sqlx::query("INSERT INTO trigger_setup (project_id, execution_id) VALUES ($1, $2)")
                .bind(project_id)
                .bind(execution_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn cancel_execution(
        &self,
        execution_id: ExecutionId,
        program: Option<&weft_core::ProjectDefinition>,
        cause: &weft_core::exec::CancelCause,
    ) -> anyhow::Result<CancelWrite> {
        let mut tx = self.pool.begin().await?;
        // The run's row first: the one lock every writer of a run takes,
        // in the same order.
        let locked = weft_journal::record::lock_in(&mut tx, execution_id).await?;
        // The wake signals go: a fire that resolves its signal row after
        // this commits finds nothing to wake.
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_BY_EXECUTION_ID_RETURNING)
            .bind(execution_id)
            .fetch_all(&mut *tx)
            .await
            .context("cancel_execution: strip the run's signals: read a signal row")?;
        let removed: Vec<SignalRegistration> = rows.into_iter().map(row_to_signal).collect::<anyhow::Result<_>>()?;
        let mut write = CancelWrite { removed, ..CancelWrite::default() };
        match locked {
            Some(locked) if locked.state == "ended" => {}
            Some(locked) if locked.owner.is_some() => {
                sqlx::query("UPDATE run SET cancel_requested = $2 WHERE execution_id = $1")
                    .bind(execution_id)
                    .bind(sqlx::types::Json(cause))
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(weft_task_store::runs::CANCEL_CHANNEL)
                    .bind(weft_task_store::runs::cancel_payload(locked.project_id, execution_id))
                    .execute(&mut *tx)
                    .await?;
                write.requested = true;
            }
            Some(locked) => {
                write.node_cancellations = Some(cancel_unowned_in(&mut tx, execution_id, &locked, program, cause).await?);
            }
            None => {}
        }
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        Ok(write)
    }

    async fn let_go_of_lost(
        &self,
        execution_id: ExecutionId,
        lapsed_before: i64,
        program: Option<&weft_core::ProjectDefinition>,
    ) -> anyhow::Result<crate::journal::Lost> {
        use crate::journal::Lost;
        let mut tx = self.pool.begin().await?;
        let Some(mut locked) = weft_journal::record::lock_in(&mut tx, execution_id).await? else { return Ok(Lost::NotLost) };
        let Some(owner) = locked.owner.clone().filter(|_| locked.state == "running") else { return Ok(Lost::NotLost) };
        let alive: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM worker_lease WHERE replica = $1 AND leased_until_unix >= $2)")
            .bind(&owner)
            .bind(lapsed_before)
            .fetch_one(&mut *tx)
            .await?;
        if alive {
            return Ok(Lost::NotLost);
        }
        let durable = locked.keeping == weft_core::run_settings::Keeping::Durable.as_str();
        sqlx::query("UPDATE run SET owner = NULL, epoch = epoch + 1, state = CASE WHEN $2 THEN 'queued' ELSE state END WHERE execution_id = $1")
            .bind(execution_id)
            .bind(durable)
            .execute(&mut *tx)
            .await?;
        locked.owner = None;
        let lost = if durable {
            locked.state = "queued".into();
            // The answers handed to it while it ran go on its record now.
            if !weft_journal::record::resolve_handed_in(&mut tx, execution_id, &locked, weft_journal::record::DISPATCHER).await? {
                weft_journal::record::notify_queued_in(&mut tx, locked.project_id).await?;
            }
            Lost::Requeued
        } else {
            cancel_unowned_in(&mut tx, execution_id, &locked, program, &weft_core::exec::CancelCause::fast_run_lost()).await?;
            Lost::Ended
        };
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        Ok(lost)
    }

    async fn answer(&self, token: &str, value: &serde_json::Value) -> anyhow::Result<crate::journal::Answered> {
        let mut tx = self.pool.begin().await?;
        let answered = answer_in(&mut tx, token, value, AnswerFrom::Sender).await?;
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        Ok(answered)
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
        type OwnerRow = (uuid::Uuid, String, Option<String>, Option<String>, String, Option<String>, Option<String>, Option<String>);
        let row: Option<OwnerRow> = sqlx::query_as(
            "SELECT project_id, tenant_id, instance_id, fired_by, phase, definition_hash, binary_hash, source_version \
             FROM run WHERE execution_id = $1",
        )
                .bind(execution_id)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(project_id, tenant, instance, fired_by, phase, definition_hash, binary_hash, source_version)| {
            Ok(ExecutionOwner {
                project_id,
                tenant,
                instance: instance
                    .map(weft_core::instance::InstanceId::new)
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("run {execution_id} holds a broken instance: {e}"))?,
                fired_by,
                phase: weft_core::context::Phase::from_tag(&phase)
                    .ok_or_else(|| anyhow::anyhow!("run {execution_id} holds an unknown phase '{phase}'"))?,
                definition_hash,
                binary_hash,
                source_version,
            })
        })
        .transpose()
    }

    async fn definition_hashes_in_use(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT definition_hash FROM run WHERE project_id = $1 AND definition_hash IS NOT NULL ORDER BY 1",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn logs_for(&self, execution_id: ExecutionId, limit: u32) -> anyhow::Result<Vec<LogEntry>> {
        // The cut is made in WRITTEN order by `LogEntry::tail`, the same
        // code the fake runs: lines of parallel firings reach the record
        // in whatever order they ran, so the record's order is not the
        // order they were written in.
        let mut conn = self.pool.acquire().await?;
        let record = weft_journal::record::read_record(&mut conn, execution_id, None).await?;
        let selection = record.selection.as_ref().map(|stored| {
            weft_core::project::selection::RecordedSelection::read(stored.digest.clone(), stored.selection.clone())
        });
        let written_at: Vec<(i32, i64)> = sqlx::query_as("SELECT seq, written_at FROM run_log WHERE execution_id = $1")
            .bind(execution_id)
            .fetch_all(&mut *conn)
            .await?;
        let mut entries: Vec<LogEntry> = Vec::new();
        for row in &record.rows {
            match weft_journal::stored::decode(execution_id, selection.as_ref(), &row.events) {
                Ok(events) => entries.extend(events.iter().flat_map(LogEntry::from_event)),
                Err(e) => {
                    let at = written_at.iter().find(|(seq, _)| *seq == row.seq).map_or(0, |(_, at)| *at);
                    entries.push(LogEntry::corrupt_row(at as u64, e));
                }
            }
        }
        Ok(LogEntry::tail(entries, limit))
    }

    async fn list_executions(&self, tenant: &str, query: &ExecutionQuery) -> anyhow::Result<ExecutionPage> {
        // Bind order is fixed ($1 tenant, $2 project, $3 after, $4 before,
        // $5 phase, $6 entry node, $7 status, $8 instance, $9 tag, $10
        // node, $11 search) and every optional filter is a `($n IS NULL OR
        // ...)` clause, so one prepared statement serves every filter
        // combination. PROJECT RUNS only (`kind = 'execution'`): a node
        // test's run has no definition, no graph, and no resume, so it
        // never belongs in this listing. What a run's record holds (the
        // nodes it started, its words) is read from its search entry,
        // built behind the writes once it ended (`crate::search_index`).
        // SYNC: list_executions (status clause) <-> crates/weft-core/src/program.rs RunStatus
        let where_clause = "r.tenant_id = $1 \
             AND r.kind = 'execution' \
             AND ($2::uuid IS NULL OR r.project_id = $2) \
             AND ($3::bigint IS NULL OR r.started_at >= $3) \
             AND ($4::bigint IS NULL OR r.started_at < $4) \
             AND ($5::text IS NULL OR r.phase = $5) \
             AND ($6::text IS NULL OR r.entry_node = $6) \
             AND ($7::text IS NULL OR CASE $7 \
                     WHEN 'running' THEN r.state <> 'ended' \
                     WHEN 'waiting_for_input' THEN r.state = 'parked' \
                     ELSE r.state = 'ended' AND r.outcome = $7 END) \
             AND ($8::text IS NULL OR r.instance_id = $8) \
             AND ($9::text IS NULL OR EXISTS ( \
                     SELECT 1 FROM execution_tag et WHERE et.execution_id = r.execution_id AND et.tag = $9)) \
             AND ($10::text IS NULL OR EXISTS ( \
                     SELECT 1 FROM run_search rs WHERE rs.execution_id = r.execution_id AND $10 = ANY(rs.nodes))) \
             AND ($11::text IS NULL OR EXISTS ( \
                     SELECT 1 FROM run_search rs WHERE rs.execution_id = r.execution_id \
                       AND rs.words @@ websearch_to_tsquery('simple', $11)))";
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM run r WHERE {where_clause}"))
            .bind(tenant)
            .bind(query.project_id)
            .bind(query.started_after.map(|v| v as i64))
            .bind(query.started_before.map(|v| v as i64))
            .bind(query.phase.map(|p| p.as_str()))
            .bind(query.entry_node.as_deref())
            .bind(query.status.map(|s| s.as_str()))
            .bind(query.instance.as_ref().map(|m| m.as_str()))
            .bind(query.tag.as_deref())
            .bind(query.node.as_deref())
            .bind(query.search.as_deref())
            .fetch_one(&self.pool)
            .await?;
        let rows: Vec<SummaryRow> = sqlx::query_as(&format!(
            "SELECT {SUMMARY_COLUMNS} FROM run r \
             WHERE {where_clause} \
               AND ($14::bigint IS NULL OR (r.started_at, r.execution_id) < ($14, $15::uuid)) \
             ORDER BY r.started_at DESC, r.execution_id DESC LIMIT $12 OFFSET $13"
        ))
        .bind(tenant)
        .bind(query.project_id)
        .bind(query.started_after.map(|v| v as i64))
        .bind(query.started_before.map(|v| v as i64))
        .bind(query.phase.map(|p| p.as_str()))
        .bind(query.entry_node.as_deref())
        .bind(query.status.map(|s| s.as_str()))
        .bind(query.instance.as_ref().map(|m| m.as_str()))
        .bind(query.tag.as_deref())
        .bind(query.node.as_deref())
        .bind(query.search.as_deref())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .bind(query.below.map(|(started, _)| started as i64))
        .bind(query.below.map(|(_, execution_id)| execution_id))
        .fetch_all(&self.pool)
        .await?;
        let executions = rows.into_iter().map(SummaryRow::summary).collect::<anyhow::Result<_>>()?;
        Ok(ExecutionPage { executions, total: total.max(0) as u64 })
    }

    async fn execution_ids_with_prefix(&self, tenant: &str, prefix: &str) -> anyhow::Result<Vec<ExecutionId>> {
        // `LIKE` on the text form with the prefix escaped: a prefix is hex
        // and dashes, but the escape keeps a stray `%` or `_` from widening
        // the match.
        let pattern = format!("{}%", prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"));
        Ok(sqlx::query_scalar(
            "SELECT execution_id FROM run WHERE tenant_id = $1 AND kind = 'execution' AND execution_id::text LIKE $2 \
             ORDER BY started_at DESC LIMIT 2",
        )
        .bind(tenant)
        .bind(pattern)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn execution_summary(&self, execution_id: ExecutionId) -> anyhow::Result<Option<ExecutionSummary>> {
        let row: Option<SummaryRow> = sqlx::query_as(&format!("SELECT {SUMMARY_COLUMNS} FROM run r WHERE r.execution_id = $1"))
            .bind(execution_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(SummaryRow::summary).transpose()
    }

    async fn execution_ids_for_project(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<ExecutionId>> {
        Ok(sqlx::query_scalar("SELECT execution_id FROM run WHERE project_id = $1").bind(project_id).fetch_all(&self.pool).await?)
    }

    async fn execution_summaries(
        &self,
        execution_ids: &[ExecutionId],
    ) -> anyhow::Result<std::collections::HashMap<ExecutionId, ExecutionSummary>> {
        let rows: Vec<SummaryRow> = sqlx::query_as(&format!("SELECT {SUMMARY_COLUMNS} FROM run r WHERE r.execution_id = ANY($1)"))
            .bind(execution_ids)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(|row| row.summary().map(|summary| (summary.execution_id, summary))).collect()
    }

    async fn going_execution_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<(ExecutionId, weft_core::context::Phase)>> {
        // Oldest first, ties broken on the id: the editor follows the last
        // one as the latest run.
        let rows: Vec<(ExecutionId, String)> = sqlx::query_as(
            "SELECT execution_id, phase FROM run WHERE project_id = $1 AND kind = 'execution' AND state IN ('queued', 'running') \
             ORDER BY started_at ASC, execution_id ASC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(execution_id, phase)| (execution_id, phase_from_column(&phase))).collect())
    }

    async fn live_tagged_executions(
        &self,
        project_id: uuid::Uuid,
        tag: &str,
    ) -> anyhow::Result<Vec<weft_journal::tags::TaggedExecution>> {
        Ok(weft_journal::tags::live_tagged_executions(&self.pool, project_id, tag).await?)
    }

    async fn settled_execution_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<std::collections::HashSet<ExecutionId>> {
        let rows: Vec<ExecutionId> = sqlx::query_scalar("SELECT execution_id FROM run WHERE project_id = $1 AND state IN ('ended', 'parked')")
            .bind(project_id)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().collect())
    }

    async fn delete_execution(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut tx = self.pool.begin().await?;
        let removed = erase_execution_ids(&mut tx, &[execution_id]).await?;
        tx.commit().await?;
        Ok(removed)
    }

    async fn delete_project_executions(&self, project_id: uuid::Uuid) -> anyhow::Result<u64> {
        let mut tx = self.pool.begin().await?;
        let execution_ids: Vec<ExecutionId> = sqlx::query_scalar("SELECT execution_id FROM run WHERE project_id = $1")
            .bind(project_id)
            .fetch_all(&mut *tx)
            .await?;
        // The signals these runs were parked on come back too, but
        // there is nothing to tell a process: `weft rm` deactivates before
        // it erases, and that already stripped and unregistered every
        // signal of the project.
        erase_execution_ids(&mut tx, &execution_ids).await?;
        tx.commit().await?;
        Ok(execution_ids.len() as u64)
    }

    async fn erase_expired(&self, now: i64, limit: i64) -> anyhow::Result<(usize, Vec<SignalRegistration>)> {
        let mut tx = self.pool.begin().await?;
        let execution_ids: Vec<ExecutionId> = sqlx::query_scalar(
            "SELECT execution_id FROM run WHERE state = 'ended' AND keep_until < $1 \
                 AND NOT (watch_end OR holds_signals) \
             ORDER BY keep_until LIMIT $2 FOR UPDATE SKIP LOCKED",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        let removed = erase_execution_ids(&mut tx, &execution_ids).await?;
        tx.commit().await?;
        Ok((execution_ids.len(), removed))
    }

    async fn projects_with_orphan_executions(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT r.project_id FROM run r WHERE NOT EXISTS (SELECT 1 FROM project p WHERE p.id = r.project_id)",
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
        // A wait is its run's: the run's row first (the lock every writer
        // of a run takes first, and written before the run registers
        // anything), and none for a run already ended, whose ending found
        // no signal to take down.
        if let Some(run) = sig.execution_id.filter(|_| sig.is_resume) {
            match weft_journal::record::lock_in(&mut tx, run).await? {
                None => anyhow::bail!("run {run} has no record, so its wait '{}' cannot be registered", sig.node_id),
                Some(locked) if locked.state == "ended" => {
                    anyhow::bail!("run {run} ended before its wait '{}' could be registered", sig.node_id)
                }
                Some(_) => {}
            }
        }
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
            .bind(sig.execution_id)
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

    async fn signal_withdraw(&self, execution_id: ExecutionId, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<SignalRow> = sqlx::query_as(SIGNAL_DELETE_WAIT_OF_RUN_RETURNING)
            .bind(token)
            .bind(execution_id)
            .fetch_optional(&self.pool)
            .await
            .context("signal_withdraw: read the signal row")?;
        row.map(row_to_signal).transpose()
    }

    async fn signal_list_for_execution_id(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_SELECT_WHERE_EXECUTION_ID_RESUME)
            .bind(execution_id)
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
            .bind(execution_id)
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

/// Erase every trace of these runs, inside the caller's transaction.
///
/// ONE list of where a run lives, used by the per-run erase (`weft clean`)
/// and the per-project one (`weft rm`), because two lists would drift and
/// the drift would be invisible: a table the project erase forgot leaves
/// rows nothing can ever reach again. One transaction, so a crash leaves
/// either the whole run or none of it. Answers the resume signals it
/// removed, because a listener process holds each of those in RAM and only
/// the caller can tell it to let go.
async fn erase_execution_ids(
    tx: &mut sqlx::PgConnection,
    execution_ids: &[ExecutionId],
) -> anyhow::Result<Vec<SignalRegistration>> {
    if execution_ids.is_empty() {
        return Ok(Vec::new());
    }
    // The runs' rows first, in id order: the lock every writer of a run
    // takes before anything else of it (an answer reaching one of its
    // waits holds the row, then its signal).
    sqlx::query("SELECT 1 FROM run WHERE execution_id = ANY($1) ORDER BY execution_id FOR UPDATE")
        .bind(execution_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM trigger_setup WHERE execution_id = ANY($1)").bind(execution_ids).execute(&mut *tx).await?;
    weft_journal::tags::delete_for_execution_ids(&mut *tx, execution_ids).await?;
    let rows: Vec<SignalRow> = sqlx::query_as(SIGNAL_DELETE_RESUME_BY_EXECUTION_IDS_RETURNING)
        .bind(execution_ids)
        .fetch_all(&mut *tx)
        .await
        .context("erase runs: read a resume signal row")?;
    let removed = rows.into_iter().map(row_to_signal).collect::<anyhow::Result<Vec<_>>>()?;
    sqlx::query("DELETE FROM parked_fire WHERE execution_id = ANY($1)").bind(execution_ids).execute(&mut *tx).await?;
    for table in ["run_search", "run_search_queue", "run_log", "run"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE execution_id = ANY($1)")).bind(execution_ids).execute(&mut *tx).await?;
    }
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
        .bind(except)
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
    "DELETE FROM signal WHERE project_id = $1 AND ($2::uuid IS NULL OR execution_id IS DISTINCT FROM $2) RETURNING ",
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
    "SELECT COALESCE(SUM(waiting.n), 0)::bigint, COUNT(*) FILTER (WHERE is_resume AND waiting.n = 0) \
     FROM signal s CROSS JOIN LATERAL (SELECT COUNT(*) AS n FROM parked_fire pf WHERE pf.token = s.token) waiting \
     WHERE ",
    governed_by_activations!()
);

const SIGNAL_DELETE_WAIT_OF_RUN_RETURNING: &str =
    concat!("DELETE FROM signal WHERE token = $1 AND is_resume AND execution_id = $2 RETURNING ", signal_columns!(""));

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
    pub(crate) execution_id: Option<ExecutionId>,
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
    let execution_id = row.execution_id;
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
