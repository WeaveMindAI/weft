//! The four stores over Postgres. Multiple dispatchers share one
//! Postgres database; the run rows, their records and the signal tables
//! are the durable state. Postgres is the source of truth, every
//! dispatcher process is a stateless reader/writer.
//!
//! Applies no schema of its own: the boot runs every group (this
//! module's [`GROUP`] included) in one pass.

use anyhow::Context;
use async_trait::async_trait;
use weft_task_store::db::{self, Client, FromRow, Pool};

use weft_core::RunId;

use weft_journal::record::{Appended, Queued, Then};
use weft_journal::RunEvent;
use crate::store::{
    Answered, BakeStore, CancelWrite, LogEntry, Lost, RunListQuery, RunOwner, RunStore, SignalRegistration, SignalStore,
    SignalToken, TokenStore,
};
use weft_core::program::{RunPage, RunSummary, RunStatus};

/// [`RunStore`], [`SignalStore`], [`TokenStore`] and [`BakeStore`] over
/// one pool: their tables share transactions (a cancel strips the run's
/// signals, an answer writes its run), so one value implements all four.
pub struct PostgresStore {
    pool: Pool,
}

/// The `run.phase` column as a phase. The column is NOT NULL and only
/// ever written from `Phase::as_str`, so an unreadable value is a broken
/// row; it is logged and answered as a fire so the row still appears (in
/// the listing, on the point-get, and to the running set the cancel and
/// wipe sweeps read, which must never be the thing a bad row breaks).
pub(crate) fn phase_from_column(text: &str) -> weft_core::context::Phase {
    weft_core::context::Phase::from_tag(text).unwrap_or_else(|| {
        tracing::warn!(
            target: "weft_dispatcher::store",
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
    "r.run_id, r.project_id, r.entry_node, r.phase, r.state, r.started_at, r.ended_at, r.outcome, \
     r.error, r.cancel_cause, r.skipped, r.instance_id, ",
    "(SELECT COALESCE(array_agg(tag ORDER BY seq), '{}') FROM run_tag WHERE run_id = r.run_id)"
);

/// One run's summary columns ([`SUMMARY_COLUMNS`]).
struct SummaryRow {
    run_id: RunId,
    project_id: uuid::Uuid,
    entry_node: Option<String>,
    phase: String,
    state: String,
    started_at: i64,
    ended_at: Option<i64>,
    outcome: Option<String>,
    error: Option<String>,
    cancel_cause: Option<db::JsonText<weft_core::run::CancelCause>>,
    skipped: i32,
    instance_id: Option<String>,
    tags: Vec<String>,
}

impl FromRow for SummaryRow {
    fn from_row(row: db::Row) -> db::Result<Self> {
        Ok(Self {
            run_id: row.try_get("run_id")?,
            project_id: row.try_get("project_id")?,
            entry_node: row.try_get("entry_node")?,
            phase: row.try_get("phase")?,
            state: row.try_get("state")?,
            started_at: row.try_get("started_at")?,
            ended_at: row.try_get("ended_at")?,
            outcome: row.try_get("outcome")?,
            error: row.try_get("error")?,
            cancel_cause: row.try_get("cancel_cause")?,
            skipped: row.try_get("skipped")?,
            instance_id: row.try_get("instance_id")?,
            tags: row.try_get("coalesce")?,
        })
    }
}

impl SummaryRow {
    /// The summary the row says: where the run stands is its state (a
    /// parked run waits for input), and how it ended its outcome.
    fn summary(self) -> anyhow::Result<RunSummary> {
        let status = match (self.state.as_str(), self.outcome.as_deref()) {
            ("ended", Some(outcome)) => match weft_journal::record::Outcome::parse(outcome) {
                Some(weft_journal::record::Outcome::Completed) => RunStatus::Completed,
                Some(weft_journal::record::Outcome::Failed) => RunStatus::Failed,
                Some(weft_journal::record::Outcome::Cancelled) => RunStatus::Cancelled,
                None => anyhow::bail!("run {} holds an unknown outcome '{outcome}'", self.run_id),
            },
            ("parked", _) => RunStatus::WaitingForInput,
            ("running" | "queued", _) => RunStatus::Running,
            (state, outcome) => anyhow::bail!("run {} holds state '{state}' with outcome {outcome:?}", self.run_id),
        };
        Ok(RunSummary {
            run_id: self.run_id,
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
                .map_err(|e| anyhow::anyhow!("run {} holds a broken instance: {e}", self.run_id))?,
        })
    }
}

impl PostgresStore {
    /// Wrap an EXISTING pool. Pure: the schema (this crate's [`GROUP`]
    /// included) is applied once, before construction, by the boot's one
    /// schema pass.
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    /// Expose the inner pool for sibling modules (lease manager,
    /// EventBus pub/sub) so they can share connections.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

/// Answer the wait `token` with `answer`, on the caller's transaction:
/// the wait's signal goes (a wait is answered once), and the answer
/// reaches its run. A run nobody drives (parked, queued) gets it in its
/// record, `SuspensionResolved` at `last_seq + 1`, and is queued for a
/// worker to carry on; a run a worker drives is written by that worker
/// alone, so the answer is handed to it
/// (`weft_task_store::parked_fires::hand_answer_in`, which also makes the
/// news its worker hears it by). The run's row is locked before the signal
/// is touched, the order every writer of a run takes them in. The caller
/// pokes the outbox once it commits.
pub(crate) async fn answer_in(
    tx: &impl Client,
    token: &str,
    answer: &weft_core::primitive::WaitAnswer,
    from: AnswerFrom,
) -> anyhow::Result<Answered> {
    let run: Option<Option<RunId>> = tx.scalar_opt("SELECT run_id FROM signal WHERE token = $1 AND is_resume", &[&token]).await?;
    let Some(run) = run else { return Ok(Answered::Gone) };
    let run_id = run.with_context(|| format!("the wait {token} names no run"))?;
    let locked = weft_journal::record::lock_in(tx, run_id).await?;
    if from == AnswerFrom::Sender {
        // An answer queued while the run's trigger was not live is the
        // wait's one answer (its sender was told so): the queue's drain
        // hands it over, and this one finds the wait answered. The signal's
        // row is locked first, and `park` holds that lock while it queues
        // one, so one queued meanwhile is seen here.
        let signal: Option<i32> = tx.scalar_opt("SELECT 1 FROM signal WHERE token = $1 AND is_resume FOR UPDATE", &[&token]).await?;
        let queued: bool = tx
            .scalar("SELECT EXISTS (SELECT 1 FROM parked_fire WHERE token = $1 AND is_resume AND run_id IS NULL)", &[&token])
            .await?;
        if signal.is_none() || queued {
            return Ok(Answered::Gone);
        }
    }
    let Some(row) = tx
        .opt::<SignalRow>(SIGNAL_DELETE_RESUME_BY_TOKEN_RETURNING, &[&token])
        .await
        .context("answer a wait: read its signal row")?
    else {
        return Ok(Answered::Gone);
    };
    let consumed = row_to_signal(row)?;
    let Some(locked) = locked.filter(|locked| locked.state != "ended") else { return Ok(Answered::RunEnded { consumed }) };
    if locked.owner.is_some() {
        weft_task_store::parked_fires::hand_answer_in(tx, token, run_id, answer).await?;
        return Ok(Answered::Reached { consumed });
    }
    let resolved = RunEvent::wait_answered(run_id, token.to_string(), answer.clone(), crate::lease::now_unix() as u64);
    match weft_journal::record::append_locked_in(tx, run_id, &locked, std::slice::from_ref(&resolved), weft_journal::record::DISPATCHER, Then::Queued).await? {
        Appended::At(_) => Ok(Answered::Reached { consumed }),
        other => anyhow::bail!("run {run_id} was locked with no owner, yet its answer was not written: {other:?}"),
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

/// End `run_id`, a run nobody drives, cancelled for `cause`, on the
/// caller's transaction with its row locked as `locked`: `NodeCancelled`
/// per open node, its open charges booked unknown, then
/// `RunCancelled`, from the ONE shared
/// definition of that list (`cancel_terminal_events`). Answers the
/// per-node count written.
async fn cancel_unowned_in(
    tx: &impl Client,
    run_id: RunId,
    locked: &weft_journal::record::Locked,
    program: Option<&weft_core::ProjectDefinition>,
    cause: &weft_core::run::CancelCause,
) -> anyhow::Result<usize> {
    let record = weft_journal::record::read_record(tx, run_id, None).await?;
    let now = crate::lease::now_unix() as u64;
    let (program, cause) = (program.cloned(), cause.clone());
    let writes = weft_journal::on_blocking_pool(move || {
        let events = record.events(run_id).map_err(anyhow::Error::msg)?;
        crate::api::run::cancel_terminal_events(run_id, &events, program.as_ref(), &cause, now)
    })
    .await?;
    let node_cancellations = writes.iter().filter(|write| matches!(write, RunEvent::NodeCancelled { .. })).count();
    match weft_journal::record::append_locked_in(tx, run_id, locked, &writes, weft_journal::record::DISPATCHER, Then::Stays).await? {
        Appended::At(_) => Ok(node_cancellations),
        other => anyhow::bail!("run {run_id} was locked with no owner, yet its cancel was not written: {other:?}"),
    }
}

/// The schema of the stores' tables. Early in `app::DISPATCHER_GROUPS` (other
/// groups' triggers attach to its tables), after the access store, whose
/// `access_grant` one of its triggers attaches to; applied by the boot's
/// one schema pass, never here.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "run",
    tables: &["run", "run_log", "signal_token", "signal", "run_tag", "trigger_setup", "trigger_bake"],
    ddl: &[
        // One row per trigger setup in flight. Several may run at once for
        // one project (each activation claims its own triggers, one instance's
        // at a time), so the setup's own run is the key.
        r#"CREATE TABLE IF NOT EXISTS trigger_setup (
            project_id UUID NOT NULL,
            run_id UUID PRIMARY KEY
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
            bake_json TEXT NOT NULL,
            -- The version `bake_json` was written at (`TriggerBake::STORED`).
            bake_json_version INTEGER NOT NULL,
            -- The source version it baked, which a version's removal
            -- deletes its bakes by: Postgres copies it out of the bake, so
            -- it can never disagree with it.
            source_version TEXT GENERATED ALWAYS AS (bake_json::jsonb ->> 'sourceVersion') STORED
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
            run_id UUID PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            -- fire | trigger_setup | infra_setup | ... (`Phase::as_str`)
            phase TEXT NOT NULL,
            -- project | node_test (`RunKind`)
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
            -- NULL for a run of no program (a node test), and then so is
            -- `binary_hash`: the two name one program identity, and
            -- delivery reads one while a claim reads the other.
            definition_hash TEXT,
            -- For a run of a cut, the registered program it was cut from:
            -- the program its triggers arm and its results are held against.
            cut_from TEXT,
            binary_hash TEXT,
            -- The run it was seeded from (`weft run --seed`), the places
            -- it ran again rather than inherit, what it was asked to run
            -- (`weft_core::run_spec::RunSpec`, as resolved) and the saved
            -- example it came from: what the version tree shows of a run
            -- started by hand. NULL and empty for every other run.
            seed_of UUID,
            stale TEXT[] NOT NULL DEFAULT '{}',
            -- As its JSON text (`db::JsonText`), like `cancel_requested` and
            -- `cancel_cause`: each holds words from the run's nodes or from
            -- outside, and comes back exactly.
            spec TEXT,
            -- The version `spec` was written at (`RunSpec::STORED`).
            spec_version INTEGER NOT NULL,
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
            -- Until when (unix seconds) the run counts as delivered to a
            -- worker: a queued run handed out and not claimed yet (or
            -- waiting to be tried again), and a running run claimed or born
            -- by a worker whose lease may not be written yet.
            delivered_until BIGINT,
            -- A queued run's `delivered_until` is a wait before it is
            -- tried again (its workers refused it, were still starting, or
            -- could not be reached), not a hand-off out right now: what a
            -- change of the project's worker levers cuts short.
            waits BOOLEAN NOT NULL DEFAULT FALSE,
            -- How many times its workers refused it since it was last
            -- claimed (`weft_dispatcher::delivery`); the last refusal ends
            -- it, and the project's worker levers changing starts the count
            -- again.
            refusals SMALLINT NOT NULL DEFAULT 0,
            -- How many of its hand-offs got no answer since it was last
            -- claimed; the last stops its tries, its `delivered_until` then
            -- saying when, until something that can fix it starts them
            -- again (`weft_dispatcher::delivery::UNREACHED_TO_STOP`).
            unreached SMALLINT NOT NULL DEFAULT 0,
            -- When its workers first answered it busy since it was last
            -- claimed (unix seconds); NULL while they have not. Its pause
            -- before the next try grows with it, and past the install's
            -- alert delay it is written down for `weft status`
            -- (`weft_dispatcher::delivery::busy`).
            busy_since BIGINT,
            -- Its workers answered it busy, refused it or could not be
            -- reached with it since it was last claimed. Unlike the three
            -- counts above, its tries starting again (a worker of the
            -- project coming alive, a build registering) leave it set: it
            -- answers "did this run that leaves the queue carry a failure
            -- of its delivery", which ends what `weft status` shows of the
            -- project's workers (`weft_task_store::unanswered::delivery_waits_ended`).
            -- A claim clears it, as do its ending and a change of the
            -- project's worker levers, so only a queued run carries it.
            waited_on_workers BOOLEAN NOT NULL DEFAULT FALSE,
            -- A cancel waiting for its owner to write the run's ending.
            cancel_requested TEXT,
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
            cancel_cause TEXT,
            -- Node firings it skipped.
            skipped INTEGER NOT NULL DEFAULT 0,
            -- It stored files of its own, which its ending's sweep reclaims.
            wrote_files BOOLEAN NOT NULL DEFAULT FALSE,
            -- What its metered calls cost, in micro-dollars.
            cost_micro_usd BIGINT NOT NULL DEFAULT 0,
            -- How long it is kept once it ended, in seconds (NULL: for
            -- ever), and the moment that runs out, set when it ends.
            keep_for BIGINT,
            keep_until BIGINT,
            CONSTRAINT run_program_identity_whole
                CHECK ((definition_hash IS NULL) = (binary_hash IS NULL))
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
        // A worker's lease coming alive (made, or renewed after it had
        // lapsed: once per stretch of its work) while its project has queued
        // runs whose tries stopped because its workers could not be reached
        // wakes the delivery, which takes those runs again
        // (`weft_dispatcher::delivery`). Any other lease coming alive says
        // nothing here, so no replica's delivery looks for nothing. On
        // `worker_lease`, whose group comes before this one.
        // SYNC: 'weft_workers_back' <-> weft_dispatcher::delivery::WORKERS_BACK_CHANNEL
        // SYNC: 3 <-> weft_dispatcher::delivery::UNREACHED_TO_STOP
        r#"CREATE OR REPLACE FUNCTION worker_lease_workers_back_notify() RETURNS trigger AS $$
            BEGIN
                IF EXISTS (SELECT 1 FROM run r WHERE r.project_id = NEW.project_id AND r.state = 'queued' AND r.unreached >= 3) THEN
                    PERFORM pg_notify('weft_workers_back', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS worker_lease_workers_back_on_insert ON worker_lease"#,
        r#"CREATE TRIGGER worker_lease_workers_back_on_insert
            AFTER INSERT ON worker_lease
            FOR EACH ROW
            EXECUTE FUNCTION worker_lease_workers_back_notify()"#,
        // SYNC: the renewal's WHEN <-> weft_task_store::worker_door (worker_lease_alive_on_renewal)
        r#"DROP TRIGGER IF EXISTS worker_lease_workers_back_on_renewal ON worker_lease"#,
        r#"CREATE TRIGGER worker_lease_workers_back_on_renewal
            AFTER UPDATE OF leased_until_unix ON worker_lease
            FOR EACH ROW
            WHEN (OLD.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT)
            EXECUTE FUNCTION worker_lease_workers_back_notify()"#,
        // What retention deletes. Keyed on a column delivery never
        // changes, so a delivery lease update stays in place.
        r#"CREATE INDEX IF NOT EXISTS run_expiry ON run (keep_until) WHERE state = 'ended'"#,
        // Ended runs whose ending the dispatcher has yet to handle.
        r#"CREATE INDEX IF NOT EXISTS run_end_unhandled ON run (project_id)
             WHERE state = 'ended' AND (watch_end OR holds_signals)"#,
        // run_log: a run's record, one row per run per write, in the run's
        // order: the write's events, compressed (`weft_journal::stored`).
        r#"CREATE TABLE IF NOT EXISTS run_log (
            run_id UUID NOT NULL,
            seq INTEGER NOT NULL,
            events BYTEA NOT NULL,
            -- The version its events were written at
            -- (`weft_journal::stored::STORED_EVENTS`).
            events_version INTEGER NOT NULL,
            -- The replica that wrote it.
            writer TEXT NOT NULL,
            written_at BIGINT NOT NULL,
            PRIMARY KEY (run_id, seq)
        )"#,
        // Already compressed: Postgres compressing it again gains nothing.
        r#"ALTER TABLE run_log ALTER COLUMN events SET STORAGE EXTERNAL"#,
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
            -- The version `program_json` was written at
            -- (`ProgramIdentity::STORED`).
            program_json_version INTEGER NOT NULL,
            -- The program's two hashes, which the doors route by: Postgres
            -- copies them out of `program_json`, which stays whole because
            -- it is the identity the signal was registered under.
            definition_hash TEXT GENERATED ALWAYS AS (program_json ->> 'definition_hash') STORED,
            binary_hash TEXT GENERATED ALWAYS AS (program_json ->> 'binary_hash') STORED,
            setup_run_id UUID,
            source_version TEXT,
            tenant_id TEXT NOT NULL,
            project_id UUID NOT NULL,
            run_id UUID,
            node_id TEXT NOT NULL,
            -- A run's wait (it names its run) or an entry (it names none):
            -- never one without the other.
            is_resume BOOLEAN NOT NULL CHECK (is_resume = (run_id IS NOT NULL)),
            spec_json TEXT NOT NULL,
            -- The version `spec_json` was written at (`SignalSpec::STORED`).
            spec_json_version INTEGER NOT NULL,
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
            -- same column. As its JSON text (`db::JsonText`), like
            -- `port_snapshot` and `auth_config`: a kind keeps what an
            -- outside service answered in it, and reads it back exactly.
            kind_state TEXT NOT NULL DEFAULT '{}',
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
            port_snapshot TEXT,
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
            auth_config TEXT,
            -- Whether the signal keeps a connection to the outside open
            -- between fires (its kind's decision for it, from the
            -- listener's `/prepare`): a holder holds it, and the number
            -- of holders is counted from these rows.
            holds BOOLEAN NOT NULL DEFAULT FALSE,
            -- The holder holding its connection now (its replica id): a
            -- claim with no expiry, freed when the holder lets go or when
            -- its dead man's check goes off. NULL while nobody holds it. A
            -- registration that rewrites the row clears it, so whoever
            -- held the old one lets go and the new one comes up fresh.
            held_by TEXT,
            -- Read by nothing: the expiry the previous release's holders
            -- put on their claim, which they still write while an upgrade
            -- rolls out. The release after this one drops it.
            held_until BIGINT,
            -- Which registration this row is: every registration upsert
            -- and restore moves it to a number never used before, so a
            -- wake set for an older one is told apart, and a give-up names
            -- the one it gave up on.
            generation BIGSERIAL NOT NULL,
            -- Why the listener or its holder gave up on it for good,
            -- shown on its node; NULL while it is served. Cleared by a
            -- registration again (`LET_GO`) or by `set_holds`.
            held_failed TEXT,
            -- Why the listener did not let go of it once its trigger
            -- stopped taking work (its door refuses, or its window ended), so it is
            -- asked again (`crate::listener::note_let_go`); NULL once it
            -- let go. Cleared by a registration again (`LET_GO`).
            let_go_failed TEXT,
            -- What its holder says the connection is doing
            -- (`{"status", "transport"}`), for the node's display, which
            -- another process renders, as its JSON text (`db::JsonText`):
            -- its status quotes why a connection went down. NULL while
            -- nobody holds it.
            serving TEXT,
            -- Its connection (`access_id`) changed or went since this
            -- registration began (`signal_access_stale`): a listener copy
            -- may still keep the old credentials for it, so it is
            -- registered again (`crate::holders::reregister_stale_access`).
            -- Only on a signal no holder holds: a holder hears the change
            -- itself. Cleared by a registration again (`LET_GO`).
            access_stale BOOLEAN NOT NULL DEFAULT FALSE
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_tenant ON signal(tenant_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_project ON signal(project_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_run_id ON signal(run_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_consumer_kind ON signal(consumer_kind)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_access_id ON signal(access_id)
           WHERE access_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS idx_signal_access_stale ON signal(token) WHERE access_stale"#,
        // A connection was made again (a reconnect or a rotate bumps its
        // `reconnects`) or went: every signal acting as it that no holder
        // holds is marked, in the same transaction, for its registration
        // again, and the holders are told
        // (`crate::holders::ACCESS_REMADE_CHANNEL`). A token refresh, a
        // widened scope or a renamed label changes nothing a registration
        // read, so it marks and tells nothing. `access_grant` is the access
        // store's (`weft_access_store::GROUP`, applied before this group).
        // SYNC: 'weft_access_remade' <-> crate::holders::ACCESS_REMADE_CHANNEL
        //
        // A reconnect also brings back a held signal that stopped because
        // its connection was refused (`held_failed`, written by the
        // listener's give-up): holders claim it again. A connection that
        // went brings nothing back.
        r#"CREATE OR REPLACE FUNCTION signal_access_stale() RETURNS trigger AS $$
            BEGIN
                UPDATE signal SET access_stale = TRUE
                 WHERE access_id = OLD.id::text AND NOT holds AND NOT access_stale;
                IF TG_OP = 'UPDATE' THEN
                    UPDATE signal SET held_failed = NULL
                     WHERE access_id = OLD.id::text AND holds AND held_failed IS NOT NULL;
                END IF;
                PERFORM pg_notify('weft_access_remade', OLD.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS access_grant_stales_signals ON access_grant"#,
        r#"CREATE TRIGGER access_grant_stales_signals
            AFTER UPDATE OF reconnects ON access_grant
            FOR EACH ROW
            WHEN (NEW.reconnects IS DISTINCT FROM OLD.reconnects)
            EXECUTE FUNCTION signal_access_stale()"#,
        r#"DROP TRIGGER IF EXISTS access_grant_gone_stales_signals ON access_grant"#,
        r#"CREATE TRIGGER access_grant_gone_stales_signals
            AFTER DELETE ON access_grant
            FOR EACH ROW
            EXECUTE FUNCTION signal_access_stale()"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_mount_path
             ON signal(mount_path, mount_methods) WHERE mount_path IS NOT NULL"#,
        // What a holder claims from and lets go of: the held signals, by
        // their holder.
        r#"CREATE INDEX IF NOT EXISTS idx_signal_held ON signal(held_by) WHERE holds"#,
        // Wake the holder sizing, which tells the holders, whenever the
        // held set changes: a held signal comes or goes (so the holders
        // start as the first one is registered and stop with the last),
        // gives up, or loses its holder (a let-go, a rewrite, a dead
        // holder's check), so another holder takes it.
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
            AFTER UPDATE OF holds, held_failed ON signal
            FOR EACH ROW
            WHEN (NEW.holds IS DISTINCT FROM OLD.holds OR NEW.held_failed IS DISTINCT FROM OLD.held_failed)
            EXECUTE FUNCTION signal_held_notify()"#,
        r#"DROP TRIGGER IF EXISTS signal_held_on_release ON signal"#,
        r#"CREATE TRIGGER signal_held_on_release
            AFTER UPDATE OF held_by ON signal
            FOR EACH ROW
            WHEN (NEW.holds AND NEW.held_by IS NULL AND OLD.held_by IS NOT NULL)
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
        // run_tag: the selectable copy of `ctx.tag_run`, one
        // row per (run, tag), written by the broker on the worker's
        // behalf once the run's own `RunTagged` is on record.
        // `ctx.stop_tagged` selects on it: "every live run of this
        // project carrying tag T" is one indexed read joined to `run`
        // instead of a fold over every open record.
        // `seq` is the order tags were written, gap-tolerant and never
        // tied, which is what the last-one-wins rule compares (unix
        // seconds would tie two runs of the same user inside one
        // second). The (run, tag) uniqueness is what makes a body
        // replayed after a durable wait land on the same row instead of moving
        // the run's place in the order. Rows go with the run.
        r#"CREATE TABLE IF NOT EXISTS run_tag (
            seq BIGSERIAL PRIMARY KEY,
            run_id UUID NOT NULL,
            tag TEXT NOT NULL,
            tagged_at_unix BIGINT NOT NULL,
            UNIQUE (run_id, tag)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_run_tag_tag ON run_tag(tag)"#,
        // THE write of a worker's records (`weft_journal::record::record_batch`):
        // one call, one transaction, for every run of one batch of one of
        // the worker's writer lanes. Synced to disk only when somebody
        // waits on it (`p_durable`, a durable run at a commit point); a
        // batch of fast runs commits without waiting for the disk.
        //
        // The cut programs the batch's births run (`p_programs`) go in with
        // the runs that went in naming them, in the same transaction, each
        // once, marked with what they were cut from (`p_born`'s
        // `cut_from`), so no run is ever on record without its program and
        // no program is kept that no run on record runs.
        // The batch's rows and programs carry the version the worker wrote
        // them at (`p_events_version`, each program's
        // `project_json_version`), both required: a broker of the release
        // before versions is refused by the writer guard before it gets
        // here, and a current broker fills what an older worker left out.
        // Per run (the `p_ids` arrays, aligned; `p_born` is a JSON array
        // aligned the same way, `null` where it does not apply, and holds
        // only words weft makes itself; the ending is four arrays, all
        // NULL for a run that does not end here, so its error and cause,
        // which carry text from the run's nodes, are values of their own
        // and never JSON the database parses):
        //   - a run that starts here (`first_seq = 0`) is inserted, already
        //     ended when it ends in the same batch;
        //   - a run that goes on is applied only when it follows on from its
        //     record exactly, under this owner and epoch, while running;
        //   - a batch sent again (its answer was lost) finds its own last row
        //     there and is "already_applied", changing nothing;
        //   - a run another worker bore (a retried fire) is
        //     "born_elsewhere", one born for a project being removed
        //     (`project.removing`, `weft rm` takes its runs down) is
        //     "project_removing", and one born for a project that is gone
        //     is "refused", like anything else.
        // Rows are locked in id order, the order every writer of run rows
        // takes them in. Only the runs applied now bring their rows, their
        // answers taken (`p_resolved_*`, by run ordinal), their version
        // counts (on this lane's own row), their search queue entry and
        // their storage sweep. A run born running counts as its writer's
        // until `p_delivered_until`, before the writer's lease is written.
        // Answers each run's fate, its project, whether it ended now with
        // somebody to tell (`watch_end`, `holds_signals`), for one that
        // ended now until when it is kept, and whether it was born running
        // under a writer with no live lease (`o_unwatched`: nothing has
        // woken the look at lapsed holds for that worker yet), in the order
        // given. The writer's lease is read only when a run was inserted.
        // SYNC: weft_record_batch's parameters and answer <-> weft_journal::record::record_batch
        concat!(r#"CREATE OR REPLACE FUNCTION weft_record_batch(
                p_writer TEXT, p_tenant TEXT, p_lane TEXT, p_now BIGINT, p_delivered_until BIGINT, p_durable BOOLEAN,
                p_ids UUID[], p_epochs INTEGER[], p_first INTEGER[], p_last INTEGER[],
                p_costs BIGINT[], p_skipped INTEGER[], p_wrote BOOLEAN[], p_born JSONB,
                p_ended_at BIGINT[], p_outcomes TEXT[], p_errors TEXT[], p_cancel_causes TEXT[],
                p_row_run INTEGER[], p_row_seq INTEGER[], p_row_events BYTEA[],
                p_resolved_run INTEGER[], p_resolved_token TEXT[],
                p_programs JSONB, p_events_version INTEGER
            ) RETURNS TABLE (o_run_id UUID, o_fate TEXT, o_project_id UUID, o_tell_end BOOLEAN, o_keep_until BIGINT,
                             o_unwatched BOOLEAN) AS $$
            DECLARE
                v_fates TEXT[];
                v_inserted BIGINT;
                v_writer_alive BOOLEAN := TRUE;
            BEGIN
                PERFORM set_config('synchronous_commit', CASE WHEN p_durable THEN 'on' ELSE 'off' END, true);
                PERFORM 1 FROM run r WHERE r.run_id = ANY(p_ids) ORDER BY r.run_id FOR UPDATE;
                SELECT array_agg(CASE
                        WHEN v.first_seq = 0 AND r.run_id IS NULL THEN 'insert'
                        WHEN v.first_seq > 0 AND r.last_seq = v.first_seq - 1 AND r.owner = p_writer
                             AND r.epoch = v.epoch AND r.state = 'running' THEN 'continue'
                        WHEN EXISTS (SELECT 1 FROM run_log l
                                     WHERE l.run_id = v.id AND l.seq = v.last_seq AND l.writer = p_writer) THEN 'already_applied'
                        WHEN v.first_seq = 0 THEN 'born_elsewhere'
                        ELSE 'refused' END ORDER BY v.n)
                    INTO v_fates
                    FROM unnest(p_ids, p_epochs, p_first, p_last) WITH ORDINALITY AS v(id, epoch, first_seq, last_seq, n)
                    LEFT JOIN run r ON r.run_id = v.id;
                WITH born AS (
                    INSERT INTO run (run_id, project_id, tenant_id, phase, kind, keeping, recorded, entry_node,
                                     instance_id, fired_by, source_version, definition_hash, cut_from, binary_hash,
                                     seed_of, state, owner, epoch, last_seq, started_at, ended_at, outcome, error,
                                     cancel_cause, skipped, wrote_files, cost_micro_usd, keep_for, keep_until,
                                     delivered_until, spec_version)
                    SELECT v.id, (x.b->>'project_id')::uuid, p_tenant, x.b->>'phase', x.b->>'kind', x.b->>'keeping',
                           (x.b->>'recorded')::boolean, x.b->>'entry_node', x.b->>'instance_id', x.b->>'fired_by',
                           x.b->>'source_version', x.b->>'definition_hash', x.b->>'cut_from', x.b->>'binary_hash',
                           (x.b->>'seed_of')::uuid,
                           CASE WHEN v.ended_at IS NULL THEN 'running' ELSE 'ended' END,
                           CASE WHEN v.ended_at IS NULL THEN p_writer END,
                           v.epoch, v.last_seq, (x.b->>'started_at')::bigint, v.ended_at, v.outcome,
                           v.error, v.cancel_cause, v.skipped, v.wrote, v.costs, (x.b->>'keep_for')::bigint,
                           v.ended_at + (x.b->>'keep_for')::bigint,
                           CASE WHEN v.ended_at IS NULL THEN p_delivered_until END, (x.b->>'spec_version')::integer
                    FROM unnest(p_ids, p_epochs, p_last, p_costs, p_skipped, p_wrote, v_fates,
                                p_ended_at, p_outcomes, p_errors, p_cancel_causes)
                            WITH ORDINALITY AS v(id, epoch, last_seq, costs, skipped, wrote, fate,
                                                 ended_at, outcome, error, cancel_cause, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b) x
                    WHERE v.fate = 'insert'
                      AND EXISTS (SELECT 1 FROM project p WHERE p.id = (x.b->>'project_id')::uuid AND NOT p.removing)
                    ORDER BY v.id
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING 1
                ) SELECT count(*) INTO v_inserted FROM born;
                -- A run another worker inserted between the read above and
                -- this insert (both bore one fire's run) is theirs; one
                -- whose project is gone or being removed went nowhere,
                -- and the worker is told which.
                IF v_inserted < (SELECT count(*) FROM unnest(v_fates) AS f(fate) WHERE f.fate = 'insert') THEN
                    SELECT array_agg(CASE WHEN v.fate = 'insert' AND r.run_id IS NULL AND p.removing THEN 'project_removing'
                                          WHEN v.fate = 'insert' AND r.run_id IS NULL THEN 'refused'
                                          WHEN v.fate = 'insert' AND r.xmin <> xid(pg_current_xact_id())
                                          THEN 'born_elsewhere' ELSE v.fate END ORDER BY v.n)
                        INTO v_fates
                        FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.run_id = v.id
                        LEFT JOIN project p ON p.id = (p_born->(v.n::integer - 1)->>'project_id')::uuid;
                END IF;
                -- A cut is kept only for a run born now that runs it,
                -- marked with the registered program it was cut from: a
                -- refused birth keeps nothing, and nothing but a cut a
                -- birth names goes in.
                INSERT INTO project_definition (project_id, definition_hash, project_json, project_json_version,
                                                recorded_at_unix, cut_from)
                    SELECT r.project_id, r.definition_hash, c->>'project_json',
                           (c->>'project_json_version')::integer, p_now, r.cut_from
                    FROM jsonb_array_elements(p_programs) AS c
                        JOIN unnest(p_ids, v_fates) AS v(id, fate) ON v.fate = 'insert'
                        JOIN run r ON r.run_id = v.id
                    WHERE r.project_id = (c->>'project_id')::uuid AND r.definition_hash = c->>'definition_hash'
                      AND r.cut_from IS NOT NULL
                    ON CONFLICT (project_id, definition_hash) DO NOTHING;
                UPDATE run r SET
                        last_seq = v.last_seq,
                        cost_micro_usd = r.cost_micro_usd + v.costs,
                        skipped = r.skipped + v.skipped,
                        wrote_files = r.wrote_files OR v.wrote,
                        state = CASE WHEN v.ended_at IS NULL THEN r.state ELSE 'ended' END,
                        owner = CASE WHEN v.ended_at IS NULL THEN r.owner END,
                        ended_at = v.ended_at,
                        outcome = v.outcome,
                        error = v.error,
                        cancel_cause = v.cancel_cause,
                        keep_until = v.ended_at + r.keep_for,
                        holds_signals = v.ended_at IS NOT NULL AND EXISTS (SELECT 1 FROM signal s WHERE s.run_id = r.run_id)
                    FROM unnest(p_ids, p_last, p_costs, p_skipped, p_wrote, v_fates,
                                p_ended_at, p_outcomes, p_errors, p_cancel_causes)
                            WITH ORDINALITY AS v(id, last_seq, costs, skipped, wrote, fate,
                                                 ended_at, outcome, error, cancel_cause, n)
                    WHERE v.fate = 'continue' AND r.run_id = v.id;
                INSERT INTO run_log (run_id, seq, events, events_version, writer, written_at)
                    SELECT p_ids[w.run], w.seq, w.events, p_events_version, p_writer, p_now
                    FROM unnest(p_row_run, p_row_seq, p_row_events) AS w(run, seq, events)
                    WHERE v_fates[w.run] IN ('insert', 'continue')
                    ORDER BY 1, 2
                    ON CONFLICT (run_id, seq) DO NOTHING;
                DELETE FROM parked_fire pf
                    USING unnest(p_resolved_run, p_resolved_token) AS t(run, token)
                    WHERE v_fates[t.run] IN ('insert', 'continue') AND pf.token = t.token AND pf.is_resume;
                -- An answer handed to a run that ended without taking it
                -- has nobody left to take it.
                DELETE FROM parked_fire pf
                    USING unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                    WHERE v.fate IN ('insert', 'continue') AND p_ended_at[v.n::integer] IS NOT NULL
                      AND pf.run_id = v.id;
                INSERT INTO version_runs (project_id, source_version, lane, runs, last_run)
                    SELECT (x.b->>'project_id')::uuid, x.b->>'source_version', p_lane, count(*),
                           (array_agg(v.id ORDER BY v.id DESC))[1]
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        CROSS JOIN LATERAL (SELECT p_born->(v.n::integer - 1) AS b) x
                    WHERE v.fate = 'insert' AND x.b->>'source_version' IS NOT NULL
                      AND x.b->>'kind' = 'project' AND (x.b->>'recorded')::boolean
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                    ON CONFLICT (project_id, source_version, lane) DO UPDATE
                        SET runs = version_runs.runs + EXCLUDED.runs,
                            last_run = GREATEST(version_runs.last_run, EXCLUDED.last_run);
                INSERT INTO run_search_queue (run_id)
                    SELECT r.run_id
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.run_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended_at[v.n::integer] IS NOT NULL
                      AND r.recorded
                    ON CONFLICT (run_id) DO NOTHING;
                INSERT INTO storage_sweep (run_id, tenant_id, enqueued_at_unix)
                    SELECT r.run_id, r.tenant_id, p_now
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        JOIN run r ON r.run_id = v.id
                    WHERE v.fate IN ('insert', 'continue') AND p_ended_at[v.n::integer] IS NOT NULL
                      AND r.wrote_files
                    ON CONFLICT (run_id) DO NOTHING;
                IF 'insert' = ANY(v_fates) THEN
                    v_writer_alive := EXISTS (SELECT 1 FROM worker_lease l
                                              WHERE l.replica = p_writer AND "#, weft_task_store::worker_alive!("l"), r#");
                END IF;
                RETURN QUERY
                    SELECT v.id,
                           CASE WHEN v.fate IN ('insert', 'continue') THEN 'accepted' ELSE v.fate END,
                           r.project_id,
                           COALESCE(v.fate IN ('insert', 'continue') AND r.state = 'ended'
                                    AND (r.watch_end OR r.holds_signals), FALSE),
                           CASE WHEN v.fate IN ('insert', 'continue') AND p_ended_at[v.n::integer] IS NOT NULL
                                THEN r.keep_until END,
                           COALESCE(v.fate = 'insert' AND r.state = 'running' AND NOT v_writer_alive, FALSE)
                    FROM unnest(p_ids, v_fates) WITH ORDINALITY AS v(id, fate, n)
                        LEFT JOIN run r ON r.run_id = v.id
                    ORDER BY v.n;
            END;
            $$ LANGUAGE plpgsql"#),
    ],
    seed: &[],
    data_steps: &[],
};

#[async_trait]
impl BakeStore for PostgresStore {
    async fn is_trigger_setup_pending(&self, run_id: RunId) -> anyhow::Result<bool> {
        Ok(self.pool.scalar("SELECT EXISTS (SELECT 1 FROM trigger_setup WHERE run_id = $1)", &[&run_id]).await?)
    }

    async fn finish_trigger_setup(&self, run_id: RunId, bake: Option<&super::TriggerBake>) -> anyhow::Result<Option<super::UnreadablePriorBake>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let mut lost = None;
        let owner: Option<uuid::Uuid> =
            tx.scalar_opt("DELETE FROM trigger_setup WHERE run_id = $1 RETURNING project_id", &[&run_id]).await?;
        if let (Some(project_id), Some(bake)) = (owner, bake) {
            anyhow::ensure!(bake.project_id == project_id && bake.run_id == run_id, "bake does not belong to its setup");
            let instance = bake.instance.as_ref().map(|m| m.as_str());
            let digest = bake.program.digest();
            // A setup of some triggers refreshes those and keeps what an
            // earlier setup captured for the others, under the row lock. A
            // prior bake that no longer reads is replaced by this one: it
            // keeps nothing a refresh could carry, and refusing would leave
            // the setup unable ever to land. What it held for the others is
            // gone, which the answer says.
            let prior: Option<(String, weft_core::stored::Version)> = tx
                .opt(
                    "SELECT bake_json, bake_json_version FROM trigger_bake \
                     WHERE project_id = $1 AND instance_id IS NOT DISTINCT FROM $2 AND program_hash = $3 FOR UPDATE",
                    &[&project_id, &instance, &digest],
                )
                .await?;
            let merged = match prior {
                Some((json, version)) => match super::TriggerBake::STORED.decode(json.as_str(), version) {
                    Ok(prior) => prior.refreshed_by(bake),
                    Err(e) => {
                        let e = e.at(super::TriggerBake::row(project_id, instance, &digest));
                        lost = Some(super::UnreadablePriorBake { error: e.to_string() });
                        bake.clone()
                    }
                },
                None => bake.clone(),
            };
            tx.execute(
                "INSERT INTO trigger_bake (project_id, instance_id, program_hash, bake_json, bake_json_version) VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (project_id, instance_id, program_hash) DO UPDATE SET bake_json = EXCLUDED.bake_json, bake_json_version = EXCLUDED.bake_json_version",
                &[&project_id, &instance, &digest, &serde_json::to_string(&merged)?, &super::TriggerBake::STORED.version()],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(lost)
    }

    async fn trigger_bakes(&self, project_id: uuid::Uuid, instance: Option<&weft_core::instance::InstanceId>) -> anyhow::Result<Vec<super::TriggerBake>> {
        let instance = instance.map(|m| m.as_str());
        let rows: Vec<(String, weft_core::stored::Version, String)> = self
            .pool
            .all(
                "SELECT bake_json, bake_json_version, program_hash FROM trigger_bake WHERE project_id = $1 AND instance_id IS NOT DISTINCT FROM $2",
                &[&project_id, &instance],
            )
            .await?;
        rows.into_iter()
            .map(|(value, version, program)| {
                Ok(super::TriggerBake::STORED
                    .decode(value.as_str(), version)
                    .map_err(|e| e.at(super::TriggerBake::row(project_id, instance, &program)))?)
            })
            .collect()
    }
}

#[async_trait]
impl RunStore for PostgresStore {
    async fn append(&self, run_id: RunId, events: &[RunEvent], then: Then) -> anyhow::Result<Appended> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let appended = weft_journal::record::append_unowned_in(&tx, run_id, events, weft_journal::record::DISPATCHER, then).await?;
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
        run_id: RunId,
    ) -> anyhow::Result<(Vec<crate::events::IdentifiedEvent<RunEvent>>, Vec<String>)> {
        let conn = self.pool.get().await?;
        let record = weft_journal::record::read_record(&conn, run_id, None).await?;
        let (rows, bad) = weft_journal::on_blocking_pool(move || record.decode_lossy(run_id)).await;
        Ok((rows.into_iter().map(|row| crate::events::IdentifiedEvent::recorded(row.seq, row.index, row.event)).collect(), bad))
    }

    async fn queue_run(&self, queued: Queued<'_>, for_activation: bool) -> anyhow::Result<bool> {
        let birth = queued.events.first().context("a run is queued with its birth")?;
        let run_id = birth.run_id();
        let RunEvent::RunStarted { project_id, phase, .. } = birth else {
            anyhow::bail!("run {run_id} is queued with a row that is not its birth");
        };
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // Its project's row is checked by the insert itself.
        if !weft_journal::record::insert_queued_in(&tx, &queued).await? {
            return Ok(false);
        }
        // A trigger setup is queued only while the activation that asked
        // for it still owns its rows (a cancel between the claim and here
        // wins), and is recorded as in flight.
        if *phase == weft_core::context::Phase::TriggerSetup {
            if for_activation {
                let owned: bool = tx
                    .scalar(
                        "SELECT EXISTS (SELECT 1 FROM trigger_activation \
                         WHERE project_id = $1 AND activating_run_id = $2 AND status = 'activating' FOR UPDATE)",
                        &[project_id, &run_id],
                    )
                    .await?;
                anyhow::ensure!(owned, "activation {run_id} ended before its trigger setup could start");
            }
            tx.execute("INSERT INTO trigger_setup (project_id, run_id) VALUES ($1, $2)", &[project_id, &run_id]).await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn cancel_run(
        &self,
        run_id: RunId,
        program: Option<&weft_core::ProjectDefinition>,
        cause: &weft_core::run::CancelCause,
    ) -> anyhow::Result<CancelWrite> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // The run's row first: the one lock every writer of a run takes,
        // in the same order.
        let locked = weft_journal::record::lock_in(&tx, run_id).await?;
        // The wake signals go: a fire that resolves its signal row after
        // this commits finds nothing to wake. The run is cancelled, so each
        // wait's last call is queued with them.
        let (removed, cancel_calls) = remove_run_waits_in(&tx, run_id, true).await.context("cancel_run")?;
        let mut write = CancelWrite { removed, cancel_calls, ..CancelWrite::default() };
        match locked {
            Some(locked) if locked.state == "ended" => {}
            Some(locked) if locked.owner.is_some() => {
                tx.execute("UPDATE run SET cancel_requested = $2 WHERE run_id = $1", &[&run_id, &db::JsonText(cause)]).await?;
                // Announced, and the install's news for the copy holding
                // the line of the run's worker (`weft_task_store::announce`).
                // SYNC: 'cancel' <-> weft_task_store::announce::kind_named
                tx.execute(
                    "SELECT pg_notify($1, $2), weft_tell($3, $4, 'cancel')",
                    &[
                        &weft_task_store::runs::CANCEL_CHANNEL,
                        &weft_task_store::runs::run_payload(locked.project_id, run_id),
                        &locked.project_id,
                        &run_id,
                    ],
                )
                .await?;
                write.requested = true;
            }
            Some(locked) => {
                write.node_cancellations = Some(cancel_unowned_in(&tx, run_id, &locked, program, cause).await?);
            }
            None => {}
        }
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        if write.requested {
            weft_task_store::announce::news_written(&self.pool);
        }
        Ok(write)
    }

    async fn let_go_of_lost(
        &self,
        run_id: RunId,
        lapsed_before: i64,
        program: Option<&weft_core::ProjectDefinition>,
    ) -> anyhow::Result<Lost> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let Some(mut locked) = weft_journal::record::lock_in(&tx, run_id).await? else { return Ok(Lost::NotLost) };
        if locked.owner.is_none() || locked.state != "running" {
            return Ok(Lost::NotLost);
        }
        // Its owner still holds it (`weft_task_store::run_held_until_sql!`).
        let held: bool = tx
            .scalar(
                concat!("SELECT ", weft_task_store::run_held_until_sql!("r"), " >= $2 FROM run r WHERE r.run_id = $1"),
                &[&run_id, &lapsed_before],
            )
            .await?;
        if held {
            return Ok(Lost::NotLost);
        }
        // Asked to stop while its worker drove it: nobody is left to write
        // its ending, so it ends here, for what asked, whatever it keeps.
        // Queued again, it would only start a worker to read the cancel
        // off its claim (and one for a project being removed, whose
        // workers were just killed).
        if let Some(cause) = locked.cancel_requested.take() {
            tx.execute("UPDATE run SET owner = NULL, epoch = epoch + 1, delivered_until = NULL WHERE run_id = $1", &[&run_id]).await?;
            locked.owner = None;
            cancel_unowned_in(&tx, run_id, &locked, program, &cause.0).await?;
            tx.commit().await?;
            weft_task_store::announce::committed(&self.pool);
            return Ok(Lost::Ended);
        }
        let durable = locked.keeping == weft_core::run_settings::Keeping::Durable.as_str();
        tx.execute(
            "UPDATE run SET owner = NULL, epoch = epoch + 1, delivered_until = NULL, \
                 state = CASE WHEN $2::boolean THEN 'queued' ELSE state END WHERE run_id = $1",
            &[&run_id, &durable],
        )
        .await?;
        locked.owner = None;
        let lost = if durable {
            locked.state = "queued".into();
            // The answers handed to it while it ran go on its record now.
            if !weft_journal::record::resolve_handed_in(&tx, run_id, &locked, weft_journal::record::DISPATCHER).await? {
                weft_journal::record::notify_queued_in(&tx, locked.project_id).await?;
            }
            Lost::Requeued
        } else {
            cancel_unowned_in(&tx, run_id, &locked, program, &weft_core::run::CancelCause::fast_run_lost()).await?;
            Lost::Ended
        };
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        Ok(lost)
    }

    async fn run_owner(&self, run_id: RunId) -> anyhow::Result<Option<RunOwner>> {
        type OwnerRow = (uuid::Uuid, String, Option<String>, Option<String>, String, Option<String>, Option<String>, Option<String>, Option<String>);
        let row: Option<OwnerRow> = self
            .pool
            .opt(
                "SELECT project_id, tenant_id, instance_id, fired_by, phase, definition_hash, cut_from, binary_hash, source_version \
                 FROM run WHERE run_id = $1",
                &[&run_id],
            )
            .await?;
        row.map(|(project_id, tenant, instance, fired_by, phase, definition_hash, cut_from, binary_hash, source_version)| {
            Ok(RunOwner {
                project_id,
                tenant,
                instance: instance
                    .map(weft_core::instance::InstanceId::new)
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("run {run_id} holds a broken instance: {e}"))?,
                fired_by,
                phase: weft_core::context::Phase::from_tag(&phase)
                    .ok_or_else(|| anyhow::anyhow!("run {run_id} holds an unknown phase '{phase}'"))?,
                definition_hash,
                cut_from,
                binary_hash,
                source_version,
            })
        })
        .transpose()
    }

    async fn logs_for(&self, run_id: RunId, limit: u32) -> anyhow::Result<Vec<LogEntry>> {
        // The cut is made in WRITTEN order by `LogEntry::tail`, the same
        // code the fake runs: lines of parallel firings reach the record
        // in whatever order they ran, so the record's order is not the
        // order they were written in.
        let conn = self.pool.get().await?;
        let record = weft_journal::record::read_record(&conn, run_id, None).await?;
        let written_at: Vec<(i32, i64)> = conn.all("SELECT seq, written_at FROM run_log WHERE run_id = $1", &[&run_id]).await?;
        Ok(weft_journal::on_blocking_pool(move || {
            let mut entries: Vec<LogEntry> = Vec::new();
            for row in &record.rows {
                match weft_journal::stored::decode(run_id, &row.events, row.events_version) {
                    Ok(events) => entries.extend(events.iter().flat_map(LogEntry::from_event)),
                    Err(e) => {
                        let at = written_at.iter().find(|(seq, _)| *seq == row.seq).map_or(0, |(_, at)| *at);
                        entries.push(LogEntry::corrupt_row(at as u64, e));
                    }
                }
            }
            LogEntry::tail(entries, limit)
        })
        .await)
    }

    async fn list_runs(&self, tenant: &str, query: &RunListQuery) -> anyhow::Result<RunPage> {
        // Bind order is fixed ($1 tenant, $2 project, $3 after, $4 before,
        // $5 phase, $6 entry node, $7 status, $8 instance, $9 tag, $10
        // node, $11 search) and every optional filter is a `($n IS NULL OR
        // ...)` clause, so one prepared statement serves every filter
        // combination. PROJECT RUNS only (`kind = 'project'`): a node
        // test's run has no definition, no graph, and no resume, so it
        // never belongs in this listing. What a run's record holds (the
        // nodes it started, its words) is read from its search entry, built
        // once it ended, here, by the first listing that reads one
        // (`crate::search_index`).
        if query.node.is_some() || query.search.is_some() {
            crate::search_index::index_queued(&self.pool).await?;
        }
        // SYNC: list_runs (status clause) <-> crates/weft-core/src/program.rs RunStatus
        let where_clause = "r.tenant_id = $1 \
             AND r.kind = 'project' \
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
                     SELECT 1 FROM run_tag et WHERE et.run_id = r.run_id AND et.tag = $9)) \
             AND ($10::text IS NULL OR EXISTS ( \
                     SELECT 1 FROM run_search rs WHERE rs.run_id = r.run_id AND $10 = ANY(rs.nodes))) \
             AND ($11::text IS NULL OR EXISTS ( \
                     SELECT 1 FROM run_search rs WHERE rs.run_id = r.run_id \
                       AND rs.words @@ websearch_to_tsquery('simple', $11)))";
        let started_after = query.started_after.map(|v| v as i64);
        let started_before = query.started_before.map(|v| v as i64);
        let phase = query.phase.map(|p| p.as_str());
        let status = query.status.map(|s| s.as_str());
        let instance = query.instance.as_ref().map(|m| m.as_str());
        let filters: [db::Param; 11] = [
            &tenant,
            &query.project_id,
            &started_after,
            &started_before,
            &phase,
            &query.entry_node,
            &status,
            &instance,
            &query.tag,
            &query.node,
            &query.search,
        ];
        let total: i64 = self.pool.scalar(&format!("SELECT COUNT(*) FROM run r WHERE {where_clause}"), &filters).await?;
        let limit = query.limit as i64;
        let offset = query.offset as i64;
        let below_started = query.below.map(|(started, _)| started as i64);
        let below_run = query.below.map(|(_, run_id)| run_id);
        let mut params = filters.to_vec();
        params.extend([&limit as db::Param, &offset, &below_started, &below_run]);
        let rows: Vec<SummaryRow> = self
            .pool
            .all(
                &format!(
                    "SELECT {SUMMARY_COLUMNS} FROM run r \
                     WHERE {where_clause} \
                       AND ($14::bigint IS NULL OR (r.started_at, r.run_id) < ($14, $15::uuid)) \
                     ORDER BY r.started_at DESC, r.run_id DESC LIMIT $12 OFFSET $13"
                ),
                &params,
            )
            .await?;
        let runs = rows.into_iter().map(SummaryRow::summary).collect::<anyhow::Result<_>>()?;
        Ok(RunPage { runs, total: total.max(0) as u64 })
    }

    async fn run_ids_with_prefix(&self, tenant: &str, prefix: &str) -> anyhow::Result<Vec<RunId>> {
        // `LIKE` on the text form with the prefix escaped: a prefix is hex
        // and dashes, but the escape keeps a stray `%` or `_` from widening
        // the match.
        let pattern = format!("{}%", prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"));
        Ok(self
            .pool
            .scalars(
                "SELECT run_id FROM run WHERE tenant_id = $1 AND kind = 'project' AND run_id::text LIKE $2 \
                 ORDER BY started_at DESC LIMIT 2",
                &[&tenant, &pattern],
            )
            .await?)
    }

    async fn run_summary(&self, run_id: RunId) -> anyhow::Result<Option<RunSummary>> {
        let row: Option<SummaryRow> =
            self.pool.opt(&format!("SELECT {SUMMARY_COLUMNS} FROM run r WHERE r.run_id = $1"), &[&run_id]).await?;
        row.map(SummaryRow::summary).transpose()
    }

    async fn run_summaries(
        &self,
        run_ids: &[RunId],
    ) -> anyhow::Result<std::collections::HashMap<RunId, RunSummary>> {
        let rows: Vec<SummaryRow> =
            self.pool.all(&format!("SELECT {SUMMARY_COLUMNS} FROM run r WHERE r.run_id = ANY($1)"), &[&run_ids]).await?;
        rows.into_iter().map(|row| row.summary().map(|summary| (summary.run_id, summary))).collect()
    }

    async fn going_run_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<(RunId, weft_core::context::Phase)>> {
        // Oldest first, ties broken on the id: the editor follows the last
        // one as the latest run.
        let rows: Vec<(RunId, String)> = self
            .pool
            .all(
                "SELECT run_id, phase FROM run WHERE project_id = $1 AND kind = 'project' AND state IN ('queued', 'running') \
                 ORDER BY started_at ASC, run_id ASC",
                &[&project_id],
            )
            .await?;
        Ok(rows.into_iter().map(|(run_id, phase)| (run_id, phase_from_column(&phase))).collect())
    }

    async fn live_tagged_runs(
        &self,
        project_id: uuid::Uuid,
        tag: &str,
    ) -> anyhow::Result<Vec<weft_journal::tags::TaggedRun>> {
        Ok(weft_journal::tags::live_tagged_runs(&self.pool, project_id, tag).await?)
    }

    async fn settled_run_ids_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<std::collections::HashSet<RunId>> {
        let rows: Vec<RunId> = self
            .pool
            .scalars("SELECT run_id FROM run WHERE project_id = $1 AND state IN ('ended', 'parked')", &[&project_id])
            .await?;
        Ok(rows.into_iter().collect())
    }

    async fn delete_run(&self, run_id: RunId) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let removed = erase_run_ids(&tx, &[run_id]).await?;
        tx.commit().await?;
        Ok(removed)
    }

    async fn erase_expired(&self, now: i64, limit: i64) -> anyhow::Result<(usize, Vec<SignalRegistration>)> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // SYNC: the erase predicate <-> next_expiry
        let run_ids: Vec<RunId> = tx
            .scalars(
                "SELECT run_id FROM run WHERE state = 'ended' AND keep_until < $1 \
                     AND NOT (watch_end OR holds_signals) \
                 ORDER BY keep_until LIMIT $2 FOR UPDATE SKIP LOCKED",
                &[&now, &limit],
            )
            .await?;
        let removed = erase_run_ids(&tx, &run_ids).await?;
        tx.commit().await?;
        Ok((run_ids.len(), removed))
    }

    async fn next_expiry(&self) -> anyhow::Result<Option<i64>> {
        // SYNC: the erase predicate <-> erase_expired
        Ok(self
            .pool
            .scalar("SELECT MIN(keep_until) FROM run WHERE state = 'ended' AND NOT (watch_end OR holds_signals)", &[])
            .await?)
    }
}

#[async_trait]
impl SignalStore for PostgresStore {
    async fn answer(&self, token: &str, answer: &weft_core::primitive::WaitAnswer) -> anyhow::Result<Answered> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let answered = answer_in(&tx, token, answer, AnswerFrom::Sender).await?;
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        // An answer that reached its run may have been handed to the
        // worker driving it, which left news for that worker.
        if matches!(answered, Answered::Reached { .. }) {
            weft_task_store::announce::news_written(&self.pool);
        }
        Ok(answered)
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
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // A wait is its run's: the run's row first (the lock every writer
        // of a run takes first, and written before the run registers
        // anything), and none for a run already ended, whose ending found
        // no signal to take down.
        if let Some(run) = sig.run_id.filter(|_| sig.is_resume) {
            match weft_journal::record::lock_in(&tx, run).await? {
                None => anyhow::bail!("run {run} has no record, so its wait '{}' cannot be registered", sig.node_id),
                Some(locked) if locked.state == "ended" => {
                    anyhow::bail!("run {run} ended before its wait '{}' could be registered", sig.node_id)
                }
                Some(_) => {}
            }
        }
        if let Some(setup) = sig.setup_run_id {
            // Arm only while THIS activation still owns the trigger's row:
            // a cancelled or superseded activation must not arm over the
            // one that replaced it.
            let instance = sig.instance.as_ref().map(|m| m.as_str());
            let owned: bool = tx
                .scalar(
                    "SELECT EXISTS (SELECT 1 FROM trigger_activation \
                     WHERE project_id = $1 AND trigger = $2 AND instance_id IS NOT DISTINCT FROM $3 \
                       AND status = 'activating' AND activating_run_id = $4 FOR UPDATE)",
                    &[&sig.project_id, &sig.node_id, &instance, &setup],
                )
                .await?;
            anyhow::ensure!(!sig.is_resume && owned,
                "activation ended before trigger '{}' could be armed", sig.node_id);
        }
        if let Some(version) = &sig.source_version {
            crate::versions::retain_source_version(&tx, sig.project_id, version).await?;
        }
        let created_at = crate::lease::now_unix();
        let instance = sig.instance.as_ref().map(|m| m.as_str());
        let refreshed = SignalRefreshed::of(sig)?;
        let mut params: Vec<db::Param> =
            vec![&sig.token, &sig.tenant_id, &sig.project_id, &sig.run_id, &sig.node_id, &sig.is_resume, &created_at, &instance, &sig.activation_trigger];
        params.extend(refreshed.params());
        params.push(&sig.kind_state_seq);
        let Some(generation) = tx.scalar_opt::<i64>(SIGNAL_INSERT.as_str(), &params).await? else {
            tx.rollback().await?;
            return Ok(super::SignalWrite::StateMoved);
        };
        tx.commit().await?;
        Ok(super::SignalWrite::Written { generation })
    }

    async fn signal_restore(&self, sig: &SignalRegistration) -> anyhow::Result<super::SignalWrite> {
        // The same columns `signal_insert`'s refresh writes, under the
        // same compare-and-set on kind_state_seq, and the same hold on
        // the source version it names: a version pruned since the row
        // was replaced is refused rather than pointed at.
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        if let Some(version) = &sig.source_version {
            crate::versions::retain_source_version(&tx, sig.project_id, version)
                .await
                .with_context(|| format!("signal_restore: put signal {} back", sig.token))?;
        }
        let refreshed = SignalRefreshed::of(sig)?;
        let mut params: Vec<db::Param> = vec![&sig.token];
        params.extend(refreshed.params());
        params.push(&sig.kind_state_seq);
        let written = tx.scalar_opt::<i64>(SIGNAL_RESTORE.as_str(), &params).await.context("signal_restore: put a signal row back")?;
        if let Some(generation) = written {
            tx.commit().await?;
            return Ok(super::SignalWrite::Written { generation });
        }
        let present: bool = tx.scalar("SELECT EXISTS (SELECT 1 FROM signal WHERE token = $1)", &[&sig.token]).await?;
        tx.rollback().await?;
        anyhow::ensure!(present, "signal {} has no row to restore", sig.token);
        Ok(super::SignalWrite::StateMoved)
    }

    async fn signal_get(&self, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<SignalRow> =
            self.pool.opt(SIGNAL_SELECT_WHERE_TOKEN, &[&token]).await.context("signal_get: read a signal row")?;
        row.map(row_to_signal).transpose()
    }

    async fn signal_entry_at(
        &self,
        project_id: uuid::Uuid,
        node: &str,
        instance: Option<&weft_core::instance::InstanceId>,
    ) -> anyhow::Result<Option<SignalRegistration>> {
        let instance = instance.map(|m| m.as_str());
        let row: Option<SignalRow> = self
            .pool
            .opt(SIGNAL_SELECT_ENTRY_AT_PLACE, &[&project_id, &node, &instance])
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

    async fn signal_remove_registration(&self, token: &str, generation: i64) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<SignalRow> = self
            .pool
            .opt(SIGNAL_DELETE_REGISTRATION_RETURNING, &[&token, &generation])
            .await
            .context("signal_remove_registration: read the signal row")?;
        row.map(row_to_signal).transpose()
    }

    async fn signal_withdraw(&self, run_id: RunId, token: &str) -> anyhow::Result<Option<SignalRegistration>> {
        let row: Option<SignalRow> = self
            .pool
            .opt(SIGNAL_DELETE_WAIT_OF_RUN_RETURNING, &[&token, &run_id])
            .await
            .context("signal_withdraw: read the signal row")?;
        row.map(row_to_signal).transpose()
    }

    async fn signal_list_for_run_id(&self, run_id: RunId) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = self
            .pool
            .all(SIGNAL_SELECT_WHERE_RUN_ID_RESUME, &[&run_id])
            .await
            .context("signal_list_for_run_id: read a signal row")?;
        rows.into_iter().map(row_to_signal).collect()
    }

    async fn signal_list_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let rows: Vec<SignalRow> = self
            .pool
            .all(SIGNAL_SELECT_WHERE_PROJECT, &[&project_id])
            .await
            .context("signal_list_for_project: read a signal row")?;
        rows.into_iter().map(row_to_signal).collect()
    }

    async fn signal_remove_for_run_id(
        &self,
        run_id: RunId,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // Whether the run was cancelled, which a wait registered after the
        // cancel (or a lost fast run, ended cancelled with its waits left
        // for here) needs to send its last call. Read, never locked: the
        // caller (`crate::run_ends`) holds the ended run's row in a
        // transaction of its own while this runs, and an ended run's
        // outcome no longer moves.
        // The outcome alone decides: a cancel asked of a run that finished
        // before its worker saw it left the run completed or failed, and
        // its waits send nothing.
        let cancelled: bool = tx
            .scalar_opt("SELECT COALESCE(outcome = 'cancelled', FALSE) FROM run WHERE run_id = $1", &[&run_id])
            .await?
            .unwrap_or(false);
        let (removed, _) = remove_run_waits_in(&tx, run_id, cancelled).await.context("signal_remove_for_run_id")?;
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        Ok(removed)
    }

    async fn signal_remove_for_project(
        &self,
        project_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<SignalRegistration>> {
        remove_project_signals(&self.pool, project_id).await
    }
}

#[async_trait]
impl TokenStore for PostgresStore {
    async fn mint_signal_token(&self, tok: &SignalToken) -> anyhow::Result<()> {
        // Store the caller-stamped mint time verbatim (the handler set it from
        // the canonical clock), so postgres and the fake agree.
        let created_at = tok.created_at as i64;
        let instance = tok.instance.as_ref().map(|m| m.as_str());
        let expires_at = tok.expires_at.map(|at| at as i64);
        self.pool
            .execute(
                "INSERT INTO signal_token \
                 (id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
                  allowed_displays, all_displays, created_at, instance_id, expires_at, kind) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                &[
                    &tok.id,
                    &tok.token_hash,
                    &tok.recognizer,
                    &tok.tenant_id,
                    &tok.name,
                    &tok.allowed_projects,
                    &tok.allowed_tags,
                    &tok.allowed_displays,
                    &tok.all_displays,
                    &created_at,
                    &instance,
                    &expires_at,
                    &tok.kind.as_str(),
                ],
            )
            .await?;
        Ok(())
    }

    async fn get_signal_token(&self, token_hash: &str) -> anyhow::Result<Option<SignalToken>> {
        let row: Option<SignalTokenRow> = self
            .pool
            .opt(
                "SELECT id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
                        allowed_displays, all_displays, created_at, instance_id, expires_at, kind \
                 FROM signal_token WHERE token_hash = $1",
                &[&token_hash],
            )
            .await?;
        row.map(row_to_signal_token).transpose()
    }

    async fn list_signal_tokens(&self, tenant: &str) -> anyhow::Result<Vec<SignalToken>> {
        let rows: Vec<SignalTokenRow> = self
            .pool
            .all(
                "SELECT id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
                        allowed_displays, all_displays, created_at, instance_id, expires_at, kind \
                 FROM signal_token WHERE tenant_id = $1 ORDER BY created_at DESC",
                &[&tenant],
            )
            .await?;
        rows.into_iter().map(row_to_signal_token).collect()
    }

    async fn revoke_signal_token(&self, id: uuid::Uuid, tenant: &str) -> anyhow::Result<bool> {
        let deleted = self.pool.execute("DELETE FROM signal_token WHERE id = $1 AND tenant_id = $2", &[&id, &tenant]).await?;
        Ok(deleted > 0)
    }
}

/// Take every wait of `run_id` down in the caller's transaction, and when
/// the run was `cancelled`, queue the last call of each wait that carries
/// one (`weft_task_store::TaskKind::CancelCall`) in the same commit, so the
/// outside service hears of the cancel even if this process dies right
/// after. THE removal of a run's waits: the cancel and the ending of a run
/// that held waits both come through here, so a wait is told of its run's
/// cancel however it went (registered after the cancel, or ended with its
/// run lost). Answers the waits removed and how many calls were queued.
async fn remove_run_waits_in(
    tx: &impl Client,
    run_id: RunId,
    cancelled: bool,
) -> anyhow::Result<(Vec<SignalRegistration>, usize)> {
    let rows: Vec<SignalRow> =
        tx.all(SIGNAL_DELETE_BY_RUN_ID_RETURNING, &[&run_id]).await.context("strip the run's signals: read a signal row")?;
    let removed: Vec<SignalRegistration> = rows.into_iter().map(row_to_signal).collect::<anyhow::Result<_>>()?;
    let cancel_calls = match cancelled {
        true => queue_cancel_calls_in(tx, &removed).await?,
        false => 0,
    };
    Ok((removed, cancel_calls))
}

/// Queue, in the caller's transaction, the last call of each of `waits`
/// that carries one (`weft_task_store::TaskKind::CancelCall`), for waits
/// just taken down because their run was cancelled. Answers how many it
/// queued (a call already queued for the same wait is not queued again).
async fn queue_cancel_calls_in(tx: &impl Client, waits: &[SignalRegistration]) -> anyhow::Result<usize> {
    let mut queued = 0;
    for wait in waits {
        let call = match wait.cancel_call() {
            Ok(Some(call)) => call,
            Ok(None) => continue,
            // Registration validated the spec, so this row was written
            // past that check; the waits still go.
            Err(e) => {
                tracing::error!(
                    target: "weft_dispatcher::cancel",
                    run_id = ?wait.run_id, token = %wait.token, error = %format!("{e:#}"),
                    "a cancelled wait's spec cannot be read, so whether it carries a last call is unknown; none is sent"
                );
                continue;
            }
        };
        let task = weft_task_store::tasks::NewTask {
            kind: weft_task_store::TaskKind::CancelCall,
            project_id: Some(wait.project_id),
            dedup_key: Some(weft_task_store::kinds::cancel_call_key(&wait.token)),
            run_id: wait.run_id,
            tenant_id: wait.tenant_id.clone(),
            payload: serde_json::to_value(&call).context("a last call's payload")?,
        };
        let outcome = weft_task_store::tasks::enqueue_dedup_in(tx, task).await.context("queue a last call")?;
        if matches!(outcome, weft_task_store::tasks::DedupOutcome::Inserted(_)) {
            queued += 1;
        }
    }
    Ok(queued)
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
async fn erase_run_ids(
    tx: &impl Client,
    run_ids: &[RunId],
) -> anyhow::Result<Vec<SignalRegistration>> {
    if run_ids.is_empty() {
        return Ok(Vec::new());
    }
    // The runs' rows first, in id order: the lock every writer of a run
    // takes before anything else of it (an answer reaching one of its
    // waits holds the row, then its signal). The same read names the
    // projects of the queued runs that carried a failure of their delivery
    // (`run.waited_on_workers`, which only a queued run carries).
    let failed: Vec<Option<uuid::Uuid>> = tx
        .scalars(
            "SELECT CASE WHEN waited_on_workers THEN project_id END \
             FROM run WHERE run_id = ANY($1) ORDER BY run_id FOR UPDATE",
            &[&run_ids],
        )
        .await?;
    tx.execute("DELETE FROM trigger_setup WHERE run_id = ANY($1)", &[&run_ids]).await?;
    weft_journal::tags::delete_for_run_ids(tx, run_ids).await?;
    let rows: Vec<SignalRow> = tx
        .all(SIGNAL_DELETE_RESUME_BY_RUN_IDS_RETURNING, &[&run_ids])
        .await
        .context("erase runs: read a resume signal row")?;
    let removed = rows.into_iter().map(row_to_signal).collect::<anyhow::Result<Vec<_>>>()?;
    tx.execute("DELETE FROM parked_fire WHERE run_id = ANY($1)", &[&run_ids]).await?;
    // The queue before the entries: a search building a run's entry holds
    // its queue row, so the erase waits for it there and then removes the
    // entry it wrote.
    for table in ["run_search_queue", "run_search", "run_log", "run"] {
        tx.execute(&format!("DELETE FROM {table} WHERE run_id = ANY($1)"), &[&run_ids]).await?;
    }
    // A queued run its workers had failed (busy, refused, not reached)
    // went without reaching one: what `weft status` shows of them goes
    // with it, as a claim's does, unless another queued run still waits
    // on them.
    let failed: std::collections::BTreeSet<uuid::Uuid> = failed.into_iter().flatten().collect();
    for project in failed {
        weft_task_store::unanswered::delivery_waits_ended(tx, project).await?;
    }
    Ok(removed)
}

/// Erase every run of `project_id`, with everything of each
/// ([`erase_run_ids`]), on the caller's transaction: `weft rm`'s,
/// which removed the project row first, so its history goes rather than
/// being orphaned (once the row is gone nothing could reach it). Answers
/// how many runs went, and the resume signals that went with them.
pub(crate) async fn erase_project_runs(tx: &impl Client, project_id: uuid::Uuid) -> anyhow::Result<(usize, Vec<SignalRegistration>)> {
    let run_ids: Vec<RunId> = tx.scalars("SELECT run_id FROM run WHERE project_id = $1", &[&project_id]).await?;
    // Removing the project ends the runs it had not ended yet (a run whose
    // cancel its worker had not written when the worker was retired):
    // their waits go out like a cancelled run's, with their last calls.
    // A run that ended any other way sends none.
    let cut_short: std::collections::HashSet<RunId> = tx
        .scalars::<RunId>(
            "SELECT run_id FROM run WHERE project_id = $1 AND (state <> 'ended' OR outcome = 'cancelled')",
            &[&project_id],
        )
        .await?
        .into_iter()
        .collect();
    let removed = erase_run_ids(tx, &run_ids).await?;
    let cut_short_waits: Vec<SignalRegistration> =
        removed.iter().filter(|wait| wait.run_id.is_some_and(|run| cut_short.contains(&run))).cloned().collect();
    queue_cancel_calls_in(tx, &cut_short_waits).await?;
    Ok((run_ids.len(), removed))
}

pub(crate) async fn remove_project_signals(db: &impl Client, project_id: uuid::Uuid) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> = db
        .all(SIGNAL_DELETE_BY_PROJECT_RETURNING, &[&project_id])
        .await
        .context("remove project signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// Every signal of the project but the waits of `except` (the run that
/// asked for a whole-project take-down keeps its own), deleted and handed
/// back for the listener cleanup.
pub async fn remove_project_signals_except(
    db: &impl Client,
    project_id: uuid::Uuid,
    except: Option<RunId>,
) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> = db
        .all(SIGNAL_DELETE_BY_PROJECT_EXCEPT_RETURNING, &[&project_id, &except])
        .await
        .context("remove project signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// Every signal the activations `keys` govern: their entry signals and
/// the waits of the runs their triggers fired.
pub(crate) async fn activation_signals(
    db: &impl Client,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<Vec<SignalRegistration>> {
    let (triggers, instances) = crate::activation_store::key_arrays(keys);
    let rows: Vec<SignalRow> = db
        .all(SIGNAL_SELECT_BY_ACTIVATIONS, &[&project_id, &triggers, &instances])
        .await
        .context("activation signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// The entry tokens of `keys`' triggers (their resume signals left out):
/// what their workers count at the door for them (`crate::drain`).
pub(crate) async fn entry_tokens(
    db: &impl Client,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<Vec<String>> {
    Ok(activation_signals(db, project_id, keys).await?.into_iter().filter(|signal| !signal.is_resume).map(|signal| signal.token).collect())
}

/// The work held on the activations `keys`: the arrivals queued on their
/// entry signals (events, calls) and the runs their triggers fired that
/// are paused on a wait (an answer queued for one goes with it). What a
/// take-down keeps or drops, and what an activation releases.
pub async fn held_on(
    db: &impl Client,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<weft_core::projects::HeldWork> {
    let (triggers, instances) = crate::activation_store::key_arrays(keys);
    let (arrivals, paused): (i64, i64) = db
        .one(HELD_ON_ACTIVATIONS, &[&project_id, &triggers, &instances])
        .await
        .context("count the work held on the activations")?;
    Ok(weft_core::projects::HeldWork { arrivals: arrivals.max(0) as u64, paused: paused.max(0) as u64 })
}

/// The work held on every activation of `project_id` that holds any, by
/// activation: [`held_on`] for all of them at once, for the project's
/// status.
pub async fn held_by_activation(
    pool: &Pool,
    project_id: uuid::Uuid,
) -> anyhow::Result<std::collections::BTreeMap<weft_core::activation::ActivationKey, weft_core::projects::HeldWork>> {
    let arrivals: Vec<(String, Option<String>, i64)> = pool
        .all(
            "SELECT s.activation_trigger, s.instance_id, COUNT(*)::bigint FROM parked_fire p JOIN signal s ON s.token = p.token \
             WHERE s.project_id = $1 AND s.activation_trigger IS NOT NULL AND NOT s.is_resume GROUP BY s.activation_trigger, s.instance_id",
            &[&project_id],
        )
        .await
        .context("count the arrivals held on the project's activations")?;
    let paused: Vec<(String, Option<String>, i64)> = pool
        .all(
            "SELECT fired_by, instance_id, COUNT(*)::bigint FROM run \
             WHERE project_id = $1 AND state = 'parked' AND fired_by IS NOT NULL GROUP BY fired_by, instance_id",
            &[&project_id],
        )
        .await
        .context("count the paused runs of the project's activations")?;
    let key = |trigger: String, instance: Option<String>| -> anyhow::Result<weft_core::activation::ActivationKey> {
        let instance = instance.map(weft_core::instance::InstanceId::new).transpose().map_err(anyhow::Error::msg)?;
        Ok(weft_core::activation::ActivationKey::new(trigger, weft_core::instance::Owner::from_instance(instance)))
    };
    let mut held: std::collections::BTreeMap<weft_core::activation::ActivationKey, weft_core::projects::HeldWork> = Default::default();
    for (trigger, instance, n) in arrivals {
        held.entry(key(trigger, instance)?).or_default().arrivals += n.max(0) as u64;
    }
    for (trigger, instance, n) in paused {
        held.entry(key(trigger, instance)?).or_default().paused += n.max(0) as u64;
    }
    Ok(held)
}

/// Move the runs `runs` that are still paused onto `program` of the build
/// `binary_hash` (`source_version` its source): each resumes on that
/// build's workers, running that program from its own record, which a
/// resync keeping what its triggers held does. A run that resumed or
/// ended meanwhile, or is on that build already (a resync's task run
/// again), is left as it is. Answers how many moved.
pub async fn move_paused_runs(
    pool: &Pool,
    runs: &[weft_core::RunId],
    program: &weft_journal::record::RunsProgram,
    binary_hash: &str,
    source_version: &str,
) -> anyhow::Result<u64> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let moved = tx
        .execute(
            "UPDATE run SET binary_hash = $2, definition_hash = $3, cut_from = $4, source_version = $5 \
             WHERE run_id = ANY($1) AND state = 'parked' \
               AND (binary_hash IS DISTINCT FROM $2 OR definition_hash IS DISTINCT FROM $3)",
            &[&runs, &binary_hash, &program.definition_hash(), &program.cut_from(), &source_version],
        )
        .await
        .context("move the paused runs onto the new build")?;
    // The cut is kept only once a run names it, so a move that moved
    // nothing keeps no program nobody runs.
    if moved > 0 {
        weft_journal::record::insert_cut_in(&tx, program).await.context("keep the cut the moved runs run")?;
    }
    tx.commit().await?;
    Ok(moved)
}

/// Delete the arrivals queued on the activations `keys`' entry signals: a
/// take-down dropping what is held while its door goes on taking. The
/// signals stay, and so do the answers queued for the waits of the runs
/// their triggers fired (a paused one's go as it is cancelled).
pub async fn drop_held_arrivals(
    db: &impl Client,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<u64> {
    let (triggers, instances) = crate::activation_store::key_arrays(keys);
    db.execute(HELD_ARRIVALS_DELETE, &[&project_id, &triggers, &instances])
        .await
        .context("drop the arrivals held on the activations")
}

/// Give each of `places`' rows its fresh kind state, each only while the
/// row is still at the version its state was computed from, which the
/// write moves one past (so a wake's claim taken at the old version
/// fails). Answers whether every one landed; when one did not (a wake
/// claimed in between, or the row went), the caller rolls back and
/// computes them again.
pub(crate) async fn write_fresh_places(db: &impl Client, places: &[crate::store::FreshPlace]) -> anyhow::Result<bool> {
    if places.is_empty() {
        return Ok(true);
    }
    let tokens: Vec<&str> = places.iter().map(|p| p.token.as_str()).collect();
    let seqs: Vec<i64> = places.iter().map(|p| p.kind_state_seq).collect();
    let states: Vec<db::JsonText<&serde_json::Value>> = places.iter().map(|p| db::JsonText(&p.kind_state)).collect();
    let written = db
        .execute(
            "UPDATE signal s SET kind_state = p.kind_state, kind_state_seq = s.kind_state_seq + 1 \
             FROM unnest($1::text[], $2::bigint[], $3::text[]) AS p(token, seq, kind_state) \
             WHERE s.token = p.token AND s.kind_state_seq = p.seq",
            &[&tokens, &seqs, &states],
        )
        .await
        .context("give the signals a fresh place")?;
    Ok(written == places.len() as u64)
}

/// Delete the entry signals of the activations `keys` (their arrivals
/// go with them), handed back for the listener cleanup. The waits of the
/// runs their triggers fired stay with those runs.
pub(crate) async fn remove_entry_signals(
    db: &impl Client,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<Vec<SignalRegistration>> {
    let (triggers, instances) = crate::activation_store::key_arrays(keys);
    let rows: Vec<SignalRow> = db
        .all(SIGNAL_DELETE_ENTRIES_BY_ACTIVATIONS_RETURNING, &[&project_id, &triggers, &instances])
        .await
        .context("remove the activations' entry signals: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

pub(crate) async fn remove_signals(db: &impl Client, tokens: &[String]) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> =
        db.all(SIGNAL_DELETE_BY_TOKENS_RETURNING, &[&tokens]).await.context("remove signals: read a signal row")?;
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
            $p, "token, ", $p, "tenant_id, ", $p, "project_id, ", $p, "run_id, ",
            $p, "node_id, ", $p, "is_resume, ", $p, "spec_json, ", $p, "access_id, ",
            $p, "consumer_kind, ", $p, "tags, ", $p, "port_snapshot, ",
            $p, "consumer_payload, ", $p, "surface_kind, ", $p, "mount_path, ",
            $p, "auth_kind, ", $p, "auth_config, ", $p, "kind_state, ",
            $p, "kind_state_seq, ", $p, "program_json, ", $p, "program_json_version, ", $p, "spec_json_version, ",
            $p, "setup_run_id, ", $p, "source_version, ",
            $p, "mount_methods, ", $p, "instance_id, ", $p, "activation_trigger, ", $p, "holds, ",
            $p, "generation"
        )
    };
}
pub(crate) use signal_columns;

const SIGNAL_SELECT_WHERE_TOKEN: &str =
    concat!("SELECT ", signal_columns!(""), " FROM signal WHERE token = $1");

/// In registration order, which is the order a run's waits are listed in.
const SIGNAL_SELECT_WHERE_RUN_ID_RESUME: &str = concat!(
    "SELECT ",
    signal_columns!(""),
    " FROM signal WHERE run_id = $1 AND is_resume ORDER BY created_at"
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
/// restore writes back), in the order [`SignalRefreshed::params`] hands
/// them. `kind_state_seq` is not among them: each statement moves it one
/// past the version it compares against. The row's identity (token,
/// tenant, project, instance, run, node, is_resume, trigger) and its
/// parked fires are never among them.
// SYNC: SIGNAL_REFRESHED_COLUMNS <-> SignalRefreshed::params (same order)
//       <-> store/fake.rs `copy_refreshed`
const SIGNAL_REFRESHED_COLUMNS: [&str; 18] = [
    "spec_json",
    "spec_json_version",
    "program_json",
    "program_json_version",
    "setup_run_id",
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

/// A registration's [`SIGNAL_REFRESHED_COLUMNS`] values, with the ones
/// written in another form than the registration holds them made.
struct SignalRefreshed<'s> {
    sig: &'s SignalRegistration,
    spec_json_version: weft_core::stored::Version,
    program_json: Option<serde_json::Value>,
    program_json_version: weft_core::stored::Version,
    consumer_payload: Option<String>,
    port_snapshot: Option<db::JsonText<&'s serde_json::Value>>,
    auth_config: Option<db::JsonText<&'s serde_json::Value>>,
    kind_state: db::JsonText<&'s serde_json::Value>,
}

impl<'s> SignalRefreshed<'s> {
    fn of(sig: &'s SignalRegistration) -> anyhow::Result<Self> {
        Ok(Self {
            sig,
            spec_json_version: weft_core::primitive::SignalSpec::STORED.version(),
            program_json: sig.program.as_ref().map(serde_json::to_value).transpose()?,
            program_json_version: weft_core::project::hash::ProgramIdentity::STORED.version(),
            consumer_payload: sig.consumer_payload.as_ref().map(serde_json::to_string).transpose()?,
            port_snapshot: sig.port_snapshot.as_ref().map(db::JsonText),
            auth_config: sig.auth_config.as_ref().map(db::JsonText),
            kind_state: db::JsonText(&sig.kind_state),
        })
    }

    /// The values, in [`SIGNAL_REFRESHED_COLUMNS`] order.
    fn params(&self) -> [db::Param<'_>; 18] {
        let sig = self.sig;
        [
            &sig.spec_json,
            &self.spec_json_version,
            &self.program_json,
            &self.program_json_version,
            &sig.setup_run_id,
            &sig.source_version,
            &sig.access_id,
            &sig.consumer_kind,
            &sig.tags,
            &self.port_snapshot,
            &self.consumer_payload,
            &sig.surface_kind,
            &sig.mount_path,
            &sig.mount_methods,
            &sig.auth_kind,
            &self.auth_config,
            &self.kind_state,
            &sig.holds,
        ]
    }
}

/// `signal_insert`: the nine identity columns ($1..$9), the refreshed
/// ones, then the version the registration read (the row lands one past
/// it). On a token conflict only the refreshed columns change, and only
/// while the row is still at that version; the row's holder lets go
/// ([`LET_GO`]), so the rewritten signal comes up fresh. Answers the
/// registration (`generation`) the write made, nothing when it did not land.
static SIGNAL_INSERT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    const IDENTITY: [&str; 9] = [
        "token", "tenant_id", "project_id", "run_id", "node_id", "is_resume", "created_at", "instance_id",
        "activation_trigger",
    ];
    let refreshed = SIGNAL_REFRESHED_COLUMNS;
    let seq = IDENTITY.len() + refreshed.len() + 1;
    let columns = IDENTITY.iter().chain(refreshed.iter()).copied().collect::<Vec<_>>().join(", ");
    let values = (1..seq).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ");
    let set = refreshed.iter().map(|c| format!("{c} = EXCLUDED.{c}")).collect::<Vec<_>>().join(", ");
    format!(
        "INSERT INTO signal ({columns}, kind_state_seq) VALUES ({values}, ${seq}::bigint + 1) \
         ON CONFLICT (token) DO UPDATE SET {set}, kind_state_seq = EXCLUDED.kind_state_seq, {LET_GO} \
         WHERE signal.kind_state_seq = EXCLUDED.kind_state_seq - 1 \
         RETURNING generation"
    )
});

/// What a rewrite of a signal row does to its holder and its standing: its
/// claim goes, so the holder running the old row stops it at its next look
/// (the release is news to the holders) and the row is claimed and brought
/// up again as it now reads; a give-up, and a let-go the listener refused,
/// are forgotten, so the new row is tried again; and the row becomes a new
/// registration, which takes its connection's credentials afresh.
const LET_GO: &str = "held_by = NULL, serving = NULL, held_failed = NULL, let_go_failed = NULL, access_stale = FALSE, \
                      generation = nextval('signal_generation_seq')";

/// `signal_restore`: the token ($1), the refreshed columns, then the
/// version the row must still be at (it moves one past). Answers the
/// registration it made, like `SIGNAL_INSERT`.
static SIGNAL_RESTORE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let refreshed = SIGNAL_REFRESHED_COLUMNS;
    let seq = refreshed.len() + 2;
    let set = refreshed.iter().enumerate().map(|(i, c)| format!("{c} = ${}", i + 2)).collect::<Vec<_>>().join(", ");
    format!("UPDATE signal SET {set}, kind_state_seq = ${seq}::bigint + 1, {LET_GO} WHERE token = $1 AND kind_state_seq = ${seq} RETURNING generation")
});

const SIGNAL_DELETE_BY_RUN_ID_RETURNING: &str =
    concat!("DELETE FROM signal WHERE run_id = $1 RETURNING ", signal_columns!(""));

const SIGNAL_DELETE_BY_PROJECT_RETURNING: &str =
    concat!("DELETE FROM signal WHERE project_id = $1 RETURNING ", signal_columns!(""));

const SIGNAL_DELETE_BY_PROJECT_EXCEPT_RETURNING: &str = concat!(
    // `$2` NULL keeps nothing back: `run_id IS DISTINCT FROM NULL` would
    // spare every entry signal (their run is NULL too).
    "DELETE FROM signal WHERE project_id = $1 AND ($2::uuid IS NULL OR run_id IS DISTINCT FROM $2) RETURNING ",
    signal_columns!("")
);

/// The signals a set of activations governs: project `$1`, the
/// activations given as the parallel arrays `$2` (triggers) and `$3`
/// (instances, `''` for the shared one), as [`crate::activation_store::key_arrays`]
/// builds them.
macro_rules! governed_by_activations {
    () => {
        "project_id = $1 \
         AND (activation_trigger, COALESCE(instance_id, '')) IN (SELECT * FROM unnest($2::text[], $3::text[]))"
    };
}

const SIGNAL_SELECT_BY_ACTIVATIONS: &str =
    concat!("SELECT ", signal_columns!(""), " FROM signal WHERE ", governed_by_activations!());

/// Every signal the listener kept after it was told to let go of it
/// (`signal.let_go_failed`, `crate::listener::note_let_go`).
pub(crate) async fn signals_kept_after_let_go(pool: &Pool) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> = pool
        .all(concat!("SELECT ", signal_columns!(""), " FROM signal WHERE let_go_failed IS NOT NULL"), &[])
        .await
        .context("signals kept after a let-go: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// Every signal whose connection changed or went since its registration
/// began (`signal.access_stale`).
pub async fn signals_with_stale_access(pool: &Pool) -> anyhow::Result<Vec<SignalRegistration>> {
    let rows: Vec<SignalRow> = pool
        .all(concat!("SELECT ", signal_columns!(""), " FROM signal WHERE access_stale ORDER BY token"), &[])
        .await
        .context("signals whose connection changed: read a signal row")?;
    rows.into_iter().map(row_to_signal).collect()
}

/// Make `stale` (as read, still marked) a new registration, keeping the
/// mark until [`access_taken_afresh`] says the listener brought it up.
/// Its state moves on (`kind_state_seq`), so a write from the registration
/// it replaces (a wake still under way) is refused, and a give-up of it
/// (`held_failed`) is forgotten: the connection made again is what it was
/// waiting for. Answers the new row, or `None` when it moved since it was read (registered
/// again, gone, or no longer marked), which is then not this caller's.
pub async fn reregister_for_access(pool: &Pool, stale: &SignalRegistration) -> anyhow::Result<Option<SignalRegistration>> {
    let row: Option<SignalRow> = pool
        .opt(
            concat!(
                "UPDATE signal SET generation = nextval('signal_generation_seq'), kind_state_seq = kind_state_seq + 1, held_failed = NULL \
                 WHERE token = $1 AND generation = $2 AND access_stale RETURNING ",
                signal_columns!("")
            ),
            &[&stale.token, &stale.generation],
        )
        .await
        .context("register again a signal whose connection changed")?;
    row.map(row_to_signal).transpose()
}

/// The listener brought registration `generation` of `token` up, or
/// answered why it could not (its own retries own it then): its mark goes.
pub async fn access_taken_afresh(pool: &Pool, token: &str, generation: i64) -> anyhow::Result<()> {
    pool.execute("UPDATE signal SET access_stale = FALSE WHERE token = $1 AND generation = $2", &[&token, &generation])
        .await
        .context("clear the mark of a signal registered again for its connection")?;
    Ok(())
}

const SIGNAL_DELETE_ENTRIES_BY_ACTIVATIONS_RETURNING: &str =
    concat!("DELETE FROM signal WHERE NOT is_resume AND ", governed_by_activations!(), " RETURNING ", signal_columns!(""));

const HELD_ARRIVALS_DELETE: &str =
    concat!("DELETE FROM parked_fire WHERE token IN (SELECT token FROM signal WHERE NOT is_resume AND ", governed_by_activations!(), ")");

const HELD_ON_ACTIVATIONS: &str = concat!(
    "SELECT (SELECT COUNT(*) FROM parked_fire pf WHERE pf.token IN (SELECT token FROM signal WHERE NOT is_resume AND ",
    governed_by_activations!(),
    "))::bigint, \
     (SELECT COUNT(*) FROM run r WHERE r.project_id = $1 AND r.state = 'parked' \
        AND (r.fired_by, COALESCE(r.instance_id, '')) IN (SELECT * FROM unnest($2::text[], $3::text[])))::bigint"
);

const SIGNAL_DELETE_WAIT_OF_RUN_RETURNING: &str =
    concat!("DELETE FROM signal WHERE token = $1 AND is_resume AND run_id = $2 RETURNING ", signal_columns!(""));

/// One registration of a row, only while the row still is it.
const SIGNAL_DELETE_REGISTRATION_RETURNING: &str =
    concat!("DELETE FROM signal WHERE token = $1 AND generation = $2 RETURNING ", signal_columns!(""));

const SIGNAL_DELETE_BY_TOKENS_RETURNING: &str =
    concat!("DELETE FROM signal WHERE token = ANY($1) RETURNING ", signal_columns!(""));

const SIGNAL_DELETE_RESUME_BY_RUN_IDS_RETURNING: &str = concat!(
    "DELETE FROM signal WHERE run_id = ANY($1) AND is_resume = TRUE RETURNING ",
    signal_columns!("")
);

const SIGNAL_DELETE_RESUME_BY_TOKEN_RETURNING: &str = concat!(
    "DELETE FROM signal WHERE token = $1 AND is_resume = TRUE RETURNING ",
    signal_columns!("")
);

/// Row shape for signal SELECTs ([`signal_columns!`]): a struct, since
/// the row is wider than a tuple reads.
pub(crate) struct SignalRow {
    pub(crate) setup_run_id: Option<uuid::Uuid>,
    pub(crate) source_version: Option<String>,
    pub(crate) program_json: Option<serde_json::Value>,
    pub(crate) program_json_version: weft_core::stored::Version,
    pub(crate) token: String,
    pub(crate) tenant_id: String,
    pub(crate) project_id: uuid::Uuid,
    pub(crate) run_id: Option<RunId>,
    pub(crate) node_id: String,
    pub(crate) is_resume: bool,
    pub(crate) spec_json: String,
    pub(crate) spec_json_version: weft_core::stored::Version,
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
    pub(crate) generation: i64,
}

impl FromRow for SignalRow {
    fn from_row(row: db::Row) -> db::Result<Self> {
        Ok(Self {
            setup_run_id: row.try_get("setup_run_id")?,
            source_version: row.try_get("source_version")?,
            program_json: row.try_get("program_json")?,
            program_json_version: row.try_get("program_json_version")?,
            token: row.try_get("token")?,
            tenant_id: row.try_get("tenant_id")?,
            project_id: row.try_get("project_id")?,
            run_id: row.try_get("run_id")?,
            node_id: row.try_get("node_id")?,
            is_resume: row.try_get("is_resume")?,
            spec_json: row.try_get("spec_json")?,
            spec_json_version: row.try_get("spec_json_version")?,
            access_id: row.try_get("access_id")?,
            consumer_kind: row.try_get("consumer_kind")?,
            tags: row.try_get("tags")?,
            port_snapshot: row.try_get::<_, Option<db::JsonText<serde_json::Value>>>("port_snapshot")?.map(|json| json.0),
            consumer_payload: row.try_get("consumer_payload")?,
            surface_kind: row.try_get("surface_kind")?,
            mount_path: row.try_get("mount_path")?,
            mount_methods: row.try_get("mount_methods")?,
            instance_id: row.try_get("instance_id")?,
            activation_trigger: row.try_get("activation_trigger")?,
            auth_kind: row.try_get("auth_kind")?,
            auth_config: row.try_get::<_, Option<db::JsonText<serde_json::Value>>>("auth_config")?.map(|json| json.0),
            kind_state: row.try_get::<_, db::JsonText<serde_json::Value>>("kind_state")?.0,
            kind_state_seq: row.try_get("kind_state_seq")?,
            holds: row.try_get("holds")?,
            generation: row.try_get("generation")?,
        })
    }
}

pub(crate) fn row_to_signal(row: SignalRow) -> anyhow::Result<SignalRegistration> {
    let run_id = row.run_id;
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
                    target: "weft_dispatcher::store",
                    token = %row.token,
                    error = %e,
                    "corrupt signal.consumer_payload: this signal is missing from every \
                     consumer's list. A trigger's row rebuilds its card the next time the \
                     project is activated; a row waiting mid-run never does, so that \
                     one stays invisible until the run is cancelled (weft stop, or \
                     clear-all on the token)."
                );
                None
            }
        },
    };
    let program = row
        .program_json
        .map(|json| weft_core::project::hash::ProgramIdentity::STORED.decode(json, row.program_json_version))
        .transpose()
        .map_err(|e| e.at(format_args!("signal {}'s program", row.token)))?;
    let spec_json = weft_core::primitive::SignalSpec::STORED
        .in_todays_shape(row.spec_json, row.spec_json_version)
        .map_err(|e| e.at(format_args!("signal {}", row.token)))?;
    Ok(SignalRegistration {
        setup_run_id: row.setup_run_id,
        instance: row
            .instance_id
            .map(weft_core::instance::InstanceId::new)
            .transpose()
            .map_err(|e| anyhow::anyhow!("corrupt signal.instance_id for token {}: {e}", row.token))?,
        activation_trigger: row.activation_trigger,
        source_version: row.source_version,
        program,
        token: row.token,
        tenant_id: row.tenant_id,
        project_id: row.project_id,
        run_id,
        node_id: row.node_id,
        is_resume: row.is_resume,
        spec_json,
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
        generation: row.generation,
    })
}

/// Revoke every instance token of `project_id`: an instance token acts in
/// its one project, so it outlives a removed project as nothing anybody
/// could use.
pub(crate) async fn revoke_project_instance_tokens(db: &impl Client, project_id: uuid::Uuid) -> anyhow::Result<u64> {
    Ok(db
        .execute(
            "DELETE FROM signal_token WHERE instance_id IS NOT NULL AND allowed_projects = ARRAY[$1::uuid]",
            &[&project_id],
        )
        .await?)
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
