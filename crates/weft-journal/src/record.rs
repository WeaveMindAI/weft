//! A run's record in Postgres: its row (`run`, the run's whole life) and
//! its log (`run_log`, its events), and every write and read of them.
//!
//! Four writers, each its own function, and no other:
//! - the worker driving a run, through its writer lanes' batches
//!   ([`record_batch`], one statement, `weft_record_batch`);
//! - the dispatcher starting a run a worker will claim ([`insert_queued_in`]);
//! - anybody writing a run nobody drives (a cancel of a parked run, an
//!   answer to a waiting run, the lost-run sweep), under the run row's
//!   lock at `last_seq + 1` ([`append_unowned_in`]);
//! - a worker claiming a queued run ([`claim`]).
//!
//! The run's row is derived from its events where it can be (its costs,
//! its skips, its ending: [`Written`]), by whoever writes them, in the
//! same statement, so it never disagrees with its log.

use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use weft_core::exec::CancelCause;
use weft_core::project::selection::{RecordedSelection, RunSelection};
use weft_core::run_settings::KeepFor;
use weft_core::ExecutionId;

use crate::events::ExecEvent;
use crate::traits::JournalRow;

/// Who the record says wrote a row the dispatcher wrote (`run_log.writer`).
pub const DISPATCHER: &str = "dispatcher";

/// Who the record says wrote the ending of a run its worker gave up
/// ([`give_up_in`]): the broker, in the worker's stead, under a name of
/// its own, so a late batch of the worker's for that same place in the
/// record is refused rather than taken for one already applied.
pub const GAVE_UP: &str = "gave-up";

/// One row of a run's record as stored: its place in the run's order and
/// its events, compressed (`crate::stored`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLogRow {
    pub seq: i32,
    #[serde(with = "base64_bytes")]
    pub events: Vec<u8>,
}

/// A run's selection as the record keeps it (`run_selection`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSelection {
    pub digest: String,
    pub selection: RunSelection,
}

impl StoredSelection {
    pub fn of(recorded: &RecordedSelection) -> Self {
        Self { digest: recorded.digest().to_string(), selection: (**recorded.selection()).clone() }
    }
}

/// A run's record as read, raw: the selection its birth names (read with
/// it, so its birth decodes) and its rows from some `seq` on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawRecord {
    pub selection: Option<StoredSelection>,
    pub rows: Vec<RunLogRow>,
}

impl RawRecord {
    /// The record's events, each with its place. One row that does not
    /// decode fails the whole read: a fold over part of a run rebuilds a
    /// run that never was.
    pub fn decode(&self, execution_id: ExecutionId) -> Result<Vec<JournalRow>, String> {
        let (decoded, bad) = self.decode_lossy(execution_id);
        match bad.into_iter().next() {
            Some(reason) => Err(reason),
            None => Ok(decoded),
        }
    }

    /// The record's events for DISPLAY: every row that decodes, and the
    /// reason of each row that does not, naming it, so a reader shows
    /// what exists and says what it cannot.
    pub fn decode_lossy(&self, execution_id: ExecutionId) -> (Vec<JournalRow>, Vec<String>) {
        let selection = self.selection.as_ref().map(|stored| RecordedSelection::read(stored.digest.clone(), stored.selection.clone()));
        let mut decoded = Vec::new();
        let mut bad = Vec::new();
        for row in &self.rows {
            match crate::stored::decode(execution_id, selection.as_ref(), &row.events) {
                Ok(events) => decoded
                    .extend(events.into_iter().enumerate().map(|(index, event)| JournalRow { seq: row.seq, index: index as u32, event })),
                Err(reason) => bad.push(reason),
            }
        }
        (decoded, bad)
    }

    /// The record's events, in order (see [`Self::decode`]).
    pub fn events(&self, execution_id: ExecutionId) -> Result<Vec<ExecEvent>, String> {
        Ok(self.decode(execution_id)?.into_iter().map(|row| row.event).collect())
    }
}

/// How a run ended, as its row says (`run.outcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Failed,
    Cancelled,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// A run's ending, as its row keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ending {
    pub at: i64,
    pub outcome: Outcome,
    // Left out when absent: the batch reads each as a key of this JSON
    // (`weft_record_batch`), and a JSON `null` would land on the row as a
    // value instead of no value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_cause: Option<CancelCause>,
}

impl Ending {
    /// The ending `event` records, if it is one.
    pub fn of(event: &ExecEvent) -> Option<Self> {
        let at = event.at_unix() as i64;
        match event {
            ExecEvent::ExecutionCompleted { .. } => Some(Self { at, outcome: Outcome::Completed, error: None, cancel_cause: None }),
            ExecEvent::ExecutionFailed { error, .. } => Some(Self { at, outcome: Outcome::Failed, error: Some(error.clone()), cancel_cause: None }),
            ExecEvent::ExecutionCancelled { cause, .. } => {
                Some(Self { at, outcome: Outcome::Cancelled, error: None, cancel_cause: cause.clone() })
            }
            _ => None,
        }
    }
}

/// What a write of a run's events says about the run besides its rows:
/// what its metered calls cost, how many firings it skipped, its ending,
/// and the answers it took (so the waiting answers go, `parked_fire`).
/// Read off the events by whoever writes them, so a run's row and its log
/// never disagree.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Written {
    pub cost_micro_usd: i64,
    pub skipped: i32,
    pub ended: Option<Ending>,
    pub resolved: Vec<String>,
}

impl Written {
    /// What `events`, all of one run, say.
    pub fn of(events: &[ExecEvent]) -> Self {
        let mut written = Self::default();
        written.add(events);
        written
    }

    /// Add what `events` say to what this already holds. A run has one
    /// ending: a second one in its record would be a writer's bug, and the
    /// first stands.
    pub fn add(&mut self, events: &[ExecEvent]) {
        for event in events {
            match event {
                ExecEvent::CostReported { amount_usd: Some(usd), .. } => self.cost_micro_usd += micro_usd(*usd),
                ExecEvent::NodeSkipped { .. } => self.skipped += 1,
                ExecEvent::SuspensionResolved { token, .. } | ExecEvent::SuspensionSkipped { token, .. } => {
                    self.resolved.push(token.clone())
                }
                _ => {}
            }
            if self.ended.is_none() {
                self.ended = Ending::of(event);
            }
        }
    }
}

/// `usd` in micro-dollars, the unit a run's cost total is kept in.
pub fn micro_usd(usd: f64) -> i64 {
    (usd * 1_000_000.0).round() as i64
}

/// The columns a run's row is born with, read off its birth (its
/// `ExecutionStarted`), with how long it is kept once it ended, resolved
/// against its project's default.
// SYNC: Born's fields <-> weft_record_batch's `p_born` (crates/weft-dispatcher/src/journal/postgres.rs)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Born {
    pub project_id: uuid::Uuid,
    pub phase: String,
    pub kind: String,
    pub keeping: String,
    pub recorded: bool,
    pub entry_node: Option<String>,
    pub instance_id: Option<String>,
    pub fired_by: Option<String>,
    pub source_version: Option<String>,
    pub definition_hash: Option<String>,
    pub binary_hash: Option<String>,
    pub selection: Option<String>,
    pub seed_of: Option<ExecutionId>,
    pub started_at: i64,
    pub keep_for: Option<i64>,
}

impl Born {
    /// The row `birth` makes, kept for `keep_for` once it ended; `None`
    /// when `birth` is not a birth.
    pub fn of(birth: &ExecEvent, keep_for: KeepFor) -> Option<Self> {
        let ExecEvent::ExecutionStarted {
            project_id, entry_node, phase, definition_hash, binary_hash, source_version, run_kind, selection, seed, instance,
            fired_trigger, settings, at_unix, ..
        } = birth
        else {
            return None;
        };
        Some(Self {
            project_id: *project_id,
            phase: phase.as_str().to_string(),
            kind: run_kind.as_str().to_string(),
            keeping: settings.keeping().as_str().to_string(),
            recorded: settings.recorded(),
            entry_node: (!entry_node.is_empty()).then(|| entry_node.clone()),
            instance_id: instance.as_ref().map(|instance| instance.as_str().to_string()),
            fired_by: fired_trigger.clone(),
            source_version: source_version.clone(),
            definition_hash: definition_hash.clone(),
            binary_hash: binary_hash.clone(),
            selection: selection.as_ref().map(|selection| selection.digest().to_string()),
            seed_of: seed.as_ref().map(|seed| seed.parent),
            started_at: *at_unix as i64,
            keep_for: keep_for.seconds(),
        })
    }
}

/// What became of one run of a batch ([`record_batch`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fate {
    /// Its rows went in.
    Accepted,
    /// The same rows were already there, written by this writer: a batch
    /// sent again after its answer was lost. Nothing changed.
    AlreadyApplied,
    /// A run that started in this batch, which another worker bore first
    /// (two workers took one fire): it is theirs, and nothing of this one
    /// went in.
    BornElsewhere,
    /// Its rows do not follow on from its record under this writer (it
    /// lost the run to another worker, or the run ended or was let go):
    /// nothing went in.
    Refused,
}

impl Fate {
    fn parse(text: &str) -> anyhow::Result<Self> {
        Ok(match text {
            "accepted" => Self::Accepted,
            "already_applied" => Self::AlreadyApplied,
            "born_elsewhere" => Self::BornElsewhere,
            "refused" => Self::Refused,
            other => anyhow::bail!("the record answered a run with '{other}', which weft does not read"),
        })
    }
}

/// One run's answer from [`record_batch`]: its fate, its project (`None`
/// for a run the record does not hold), and whether it ended now with
/// somebody to tell (it is watched, or holds signals to take down).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded {
    pub execution_id: ExecutionId,
    pub fate: Fate,
    pub project_id: Option<uuid::Uuid>,
    pub tell_end: bool,
}

/// Write one batch of a worker's writer lane (`crate::frame`): every run
/// of it in one statement and one transaction, synced to disk when the
/// batch says somebody waits on it. `writer` is the worker's replica, the
/// owner the batch's runs are fenced on; `lane` keys its version counts.
/// Answers each run's fate, in the batch's order. Sending a batch again is
/// always safe: what already went in answers `AlreadyApplied`.
pub async fn record_batch(
    conn: &mut PgConnection,
    writer: &str,
    tenant: &str,
    lane: &str,
    batch: &crate::frame::Batch<'_>,
) -> anyhow::Result<Vec<Recorded>> {
    let head = &batch.head;
    let mut ids = Vec::with_capacity(head.runs.len());
    let mut epochs = Vec::with_capacity(head.runs.len());
    let mut first = Vec::with_capacity(head.runs.len());
    let mut last = Vec::with_capacity(head.runs.len());
    let mut costs = Vec::with_capacity(head.runs.len());
    let mut skipped = Vec::with_capacity(head.runs.len());
    let mut wrote = Vec::with_capacity(head.runs.len());
    let mut born = Vec::with_capacity(head.runs.len());
    let mut ended = Vec::with_capacity(head.runs.len());
    let mut row_run = Vec::new();
    let mut row_seq = Vec::new();
    let mut resolved_run = Vec::new();
    let mut resolved_token = Vec::new();
    for (at, run) in head.runs.iter().enumerate() {
        let ordinal = i32::try_from(at + 1)?;
        ids.push(run.execution_id);
        epochs.push(run.epoch);
        first.push(run.first_seq);
        last.push(run.first_seq + i32::try_from(run.row_sizes.len())? - 1);
        costs.push(run.written.cost_micro_usd);
        skipped.push(run.written.skipped);
        wrote.push(run.wrote_files);
        born.push(&run.born);
        ended.push(&run.written.ended);
        for seq in run.first_seq..run.first_seq + i32::try_from(run.row_sizes.len())? {
            row_run.push(ordinal);
            row_seq.push(seq);
        }
        for token in &run.written.resolved {
            resolved_run.push(ordinal);
            resolved_token.push(token.as_str());
        }
    }
    let rows: Vec<&[u8]> = batch.rows.clone();
    let answered: Vec<(ExecutionId, String, Option<uuid::Uuid>, bool)> = sqlx::query_as(
        "SELECT o_execution_id, o_fate, o_project_id, o_tell_end FROM weft_record_batch(\
         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)",
    )
    .bind(writer)
    .bind(tenant)
    .bind(lane)
    .bind(unix_now())
    .bind(head.durable)
    .bind(&ids)
    .bind(&epochs)
    .bind(&first)
    .bind(&last)
    .bind(&costs)
    .bind(&skipped)
    .bind(&wrote)
    .bind(sqlx::types::Json(&born))
    .bind(sqlx::types::Json(&ended))
    .bind(&row_run)
    .bind(&row_seq)
    .bind(&rows)
    .bind(&resolved_run)
    .bind(&resolved_token)
    .bind(sqlx::types::Json(&head.selections))
    .fetch_all(&mut *conn)
    .await?;
    anyhow::ensure!(
        answered.len() == ids.len() && answered.iter().zip(&ids).all(|(answer, id)| answer.0 == *id),
        "the record answered a batch of {} runs with {} answers out of its order",
        ids.len(),
        answered.len()
    );
    answered
        .into_iter()
        .map(|(execution_id, fate, project_id, tell_end)| Ok(Recorded { execution_id, fate: Fate::parse(&fate)?, project_id, tell_end }))
        .collect()
}

/// A run the dispatcher starts for a worker to claim: `weft run`, a setup
/// run, a program's call that starts one. Everything its row is born with
/// besides what its birth says.
#[derive(Debug, Clone)]
pub struct Queued<'a> {
    /// Its birth, its kicks, and what it is born holding (`events[0]` is
    /// its `ExecutionStarted`).
    pub events: &'a [ExecEvent],
    pub tenant: &'a str,
    pub keep_for: KeepFor,
    /// Somebody waits on its ending: a trigger setup, whose bake is made
    /// from it (`weft_dispatcher::run_ends`).
    pub watch_end: bool,
    /// What the version tree shows of a run started by hand.
    pub stale: &'a [String],
    pub spec: Option<&'a serde_json::Value>,
    pub example: Option<&'a str>,
}

/// Start `queued` on the caller's transaction: its row (`queued`, no owner,
/// never claimed), its record's first row and its selection, and the
/// delivery woken. `false` when the run is already on record (a retried
/// start, by its id): nothing is written then. A queued run is started by
/// hand or sets triggers up, never fired by one, so it adds nothing to its
/// version's trigger runs (those are counted as workers bear them).
pub async fn insert_queued_in(conn: &mut PgConnection, queued: &Queued<'_>) -> anyhow::Result<bool> {
    let birth = queued.events.first().ok_or_else(|| anyhow::anyhow!("a run is started with at least its birth"))?;
    let execution_id = birth.execution_id();
    let born = Born::of(birth, queued.keep_for).ok_or_else(|| anyhow::anyhow!("run {execution_id} is started with a row that is not its birth"))?;
    if let Some(stray) = queued.events.iter().find(|event| event.execution_id() != execution_id) {
        anyhow::bail!("the start of run {execution_id} carries a row of run {}", stray.execution_id());
    }
    if let ExecEvent::ExecutionStarted { selection: Some(selection), .. } = birth {
        insert_selection_in(conn, selection).await?;
    }
    let written = Written::of(queued.events);
    // A queued run has not run: an ending in its start would leave a row
    // that waits for a worker over a record that says it ended.
    anyhow::ensure!(written.ended.is_none(), "run {execution_id} is started with its ending");
    let inserted = sqlx::query(
        "INSERT INTO run (execution_id, project_id, tenant_id, phase, kind, keeping, recorded, entry_node, instance_id, \
             fired_by, source_version, definition_hash, binary_hash, selection, seed_of, stale, spec, example, state, \
             epoch, last_seq, attempts, watch_end, started_at, skipped, cost_micro_usd, keep_for) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, 'queued', \
             0, 0, 0, $19, $20, $21, $22, $23) \
         ON CONFLICT (execution_id) DO NOTHING",
    )
    .bind(execution_id)
    .bind(born.project_id)
    .bind(queued.tenant)
    .bind(&born.phase)
    .bind(&born.kind)
    .bind(&born.keeping)
    .bind(born.recorded)
    .bind(&born.entry_node)
    .bind(&born.instance_id)
    .bind(&born.fired_by)
    .bind(&born.source_version)
    .bind(&born.definition_hash)
    .bind(&born.binary_hash)
    .bind(&born.selection)
    .bind(born.seed_of)
    .bind(queued.stale)
    .bind(queued.spec)
    .bind(queued.example)
    .bind(queued.watch_end)
    .bind(born.started_at)
    .bind(written.skipped)
    .bind(written.cost_micro_usd)
    .bind(born.keep_for)
    .execute(&mut *conn)
    .await?;
    if inserted.rows_affected() == 0 {
        return Ok(false);
    }
    sqlx::query("INSERT INTO run_log (execution_id, seq, events, writer, written_at) VALUES ($1, 0, $2, $3, $4)")
        .bind(execution_id)
        .bind(crate::stored::encode(queued.events))
        .bind(DISPATCHER)
        .bind(unix_now())
        .execute(&mut *conn)
        .await?;
    notify_queued_in(conn, born.project_id).await?;
    Ok(true)
}

/// Announce, in the caller's transaction, that a run of `project_id` was
/// queued for a worker (`weft_task_store::runs::RUN_QUEUED_CHANNEL`).
pub async fn notify_queued_in(conn: &mut PgConnection, project_id: uuid::Uuid) -> anyhow::Result<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(weft_task_store::runs::RUN_QUEUED_CHANNEL)
        .bind(project_id.to_string())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Keep `selection` in the record, once.
pub async fn insert_selection_in(conn: &mut PgConnection, selection: &RecordedSelection) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO run_selection (digest, selection) VALUES ($1, $2) ON CONFLICT (digest) DO NOTHING")
        .bind(selection.digest())
        .bind(sqlx::types::Json(&**selection.selection()))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Where a run nobody drives goes once [`append_unowned_in`] wrote to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Then {
    /// It stays where it is (parked or queued), or ends if the events end it.
    Stays,
    /// It is queued for a worker to carry it on (an answer reached it, it
    /// was handed back, its worker was lost).
    Queued,
}

/// What [`append_unowned_in`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Appended {
    /// Written at this `seq`.
    At(i32),
    /// A worker drives the run: only it writes its record. Nothing written.
    Driven { owner: String },
    /// The run already ended. Nothing written.
    Ended,
    /// No such run.
    Missing,
}

/// The row of `execution_id`, locked for the caller's transaction, as a
/// writer that is not its owner reads it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Locked {
    pub project_id: uuid::Uuid,
    pub tenant_id: String,
    pub state: String,
    /// `fast` or `durable` (`weft_core::run_settings::Keeping`).
    pub keeping: String,
    pub owner: Option<String>,
    pub last_seq: i32,
    pub epoch: i32,
    pub keep_for: Option<i64>,
    pub definition_hash: Option<String>,
}

/// Lock `execution_id`'s row on the caller's transaction (`FOR UPDATE`),
/// the one serialization point of a run every writer that is not its
/// owner goes through.
pub async fn lock_in(conn: &mut PgConnection, execution_id: ExecutionId) -> anyhow::Result<Option<Locked>> {
    Ok(sqlx::query_as(
        "SELECT project_id, tenant_id, state, keeping, owner, last_seq, epoch, keep_for, definition_hash \
         FROM run WHERE execution_id = $1 FOR UPDATE",
    )
    .bind(execution_id)
    .fetch_optional(&mut *conn)
    .await?)
}

/// Write `events` into the record of `execution_id`, which nobody drives,
/// at `last_seq + 1`, on the caller's transaction, and move the run as
/// `then` says (an ending in `events` ends it whatever `then` says). Its
/// costs, skips and ending go on its row in the same statement. `writer`
/// is who the record says wrote it. Refused when a worker drives the run
/// (its owner writes its record) or it already ended. The write is
/// announced through the outbox: the caller pokes it once its transaction
/// commits (`weft_task_store::announce::committed`).
pub async fn append_unowned_in(
    conn: &mut PgConnection,
    execution_id: ExecutionId,
    events: &[ExecEvent],
    writer: &str,
    then: Then,
) -> anyhow::Result<Appended> {
    let Some(locked) = lock_in(conn, execution_id).await? else { return Ok(Appended::Missing) };
    append_locked_in(conn, execution_id, &locked, events, writer, then).await
}

/// [`append_unowned_in`] on a row the caller already locked ([`lock_in`]).
pub async fn append_locked_in(
    conn: &mut PgConnection,
    execution_id: ExecutionId,
    locked: &Locked,
    events: &[ExecEvent],
    writer: &str,
    then: Then,
) -> anyhow::Result<Appended> {
    if let Some(owner) = &locked.owner {
        return Ok(Appended::Driven { owner: owner.clone() });
    }
    if locked.state == "ended" {
        return Ok(Appended::Ended);
    }
    if let Some(stray) = events.iter().find(|event| event.execution_id() != execution_id) {
        anyhow::bail!("a write to run {execution_id} carries a row of run {}", stray.execution_id());
    }
    let seq = locked.last_seq + 1;
    let now = unix_now();
    if !events.is_empty() {
        sqlx::query("INSERT INTO run_log (execution_id, seq, events, writer, written_at) VALUES ($1, $2, $3, $4, $5)")
            .bind(execution_id)
            .bind(seq)
            .bind(crate::stored::encode(events))
            .bind(writer)
            .bind(now)
            .execute(&mut *conn)
            .await?;
    }
    let written = Written::of(events);
    let ended = written.ended.as_ref();
    let state = match (ended, then) {
        (Some(_), _) => "ended",
        (None, Then::Queued) => "queued",
        (None, Then::Stays) => locked.state.as_str(),
    };
    // Announced through the outbox (`weft_announce`), which its writer
    // pokes once it commits: its new row for the live view, and its ending
    // when somebody waits on it.
    let tell_end: bool = sqlx::query_scalar(
        "UPDATE run SET last_seq = $2, state = $3, cost_micro_usd = cost_micro_usd + $4, skipped = skipped + $5, \
             ended_at = $6, outcome = $7, error = $8, cancel_cause = $9, keep_until = $6 + keep_for, \
             delivered_until = NULL, \
             holds_signals = ($6 IS NOT NULL AND EXISTS (SELECT 1 FROM signal s WHERE s.execution_id = $1)) \
         WHERE execution_id = $1 \
         RETURNING $6 IS NOT NULL AND (watch_end OR holds_signals)",
    )
    .bind(execution_id)
    .bind(if events.is_empty() { locked.last_seq } else { seq })
    .bind(state)
    .bind(written.cost_micro_usd)
    .bind(written.skipped)
    .bind(ended.map(|ending| ending.at))
    .bind(ended.map(|ending| ending.outcome.as_str()))
    .bind(ended.and_then(|ending| ending.error.as_deref()))
    .bind(ended.and_then(|ending| ending.cancel_cause.as_ref()).map(sqlx::types::Json))
    .fetch_one(&mut *conn)
    .await?;
    let mut channels = Vec::new();
    let mut payloads = Vec::new();
    if !events.is_empty() {
        channels.push(crate::RUN_LOG_CHANNEL);
        payloads.extend(crate::run_log_payloads(&locked.project_id, [&execution_id]));
    }
    if tell_end {
        channels.push(crate::RUN_ENDED_CHANNEL);
        payloads.extend(crate::run_ended_payloads([&execution_id]));
    }
    if !channels.is_empty() {
        sqlx::query("SELECT weft_announce(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)")
            .bind(&channels)
            .bind(&payloads)
            .execute(&mut *conn)
            .await?;
    }
    if state == "queued" {
        notify_queued_in(conn, locked.project_id).await?;
    }
    if !written.resolved.is_empty() {
        sqlx::query("DELETE FROM parked_fire WHERE token = ANY($1) AND is_resume")
            .bind(&written.resolved)
            .execute(&mut *conn)
            .await?;
    }
    if ended.is_some() {
        // An answer handed to the run that it never took has nobody left
        // to take it.
        sqlx::query("DELETE FROM parked_fire WHERE execution_id = $1").bind(execution_id).execute(&mut *conn).await?;
        sqlx::query(
            "INSERT INTO run_search_queue (execution_id) SELECT $1 FROM run WHERE execution_id = $1 AND recorded \
             ON CONFLICT (execution_id) DO NOTHING",
        )
        .bind(execution_id)
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "INSERT INTO storage_sweep (execution_id, tenant_id, enqueued_at_unix) \
             SELECT execution_id, tenant_id, $2 FROM run WHERE execution_id = $1 AND wrote_files \
             ON CONFLICT (execution_id) DO NOTHING",
        )
        .bind(execution_id)
        .bind(now)
        .execute(&mut *conn)
        .await?;
    }
    Ok(Appended::At(seq))
}

/// Write the answers handed to `execution_id` while a worker drove it
/// (`weft_task_store::parked_fires::hand_answer_in`) into its record, as
/// `SuspensionResolved` (or `SuspensionSkipped` for a skip) at
/// `last_seq + 1`, and queue it for a worker to
/// carry on, on the caller's transaction with its row locked as `locked`
/// and no owner left (its worker just let go of it, or went away). Answers
/// whether any were waiting; with none, nothing is written.
pub async fn resolve_handed_in(conn: &mut PgConnection, execution_id: ExecutionId, locked: &Locked, writer: &str) -> anyhow::Result<bool> {
    let handed = weft_task_store::parked_fires::answers_for(&mut *conn, execution_id).await?;
    if handed.is_empty() {
        return Ok(false);
    }
    let at_unix = unix_now() as u64;
    let resolved: Vec<ExecEvent> = handed
        .into_iter()
        .map(|(token, answer)| ExecEvent::wait_answered(execution_id, token, answer, at_unix))
        .collect();
    match append_locked_in(conn, execution_id, locked, &resolved, writer, Then::Queued).await? {
        Appended::At(_) => Ok(true),
        other => anyhow::bail!("run {execution_id} was locked with no owner, yet the answers handed to it were not written: {other:?}"),
    }
}

/// What [`give_up_in`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GaveUp {
    /// The run is ended, failed, its ending written at this `seq`.
    Ended(i32),
    /// `owner` does not drive the run (any more): its new owner, or
    /// whoever ended it, has it. Nothing written.
    NotOwned,
}

/// End `execution_id`, a run `owner` drives but whose record it can no
/// longer write (a write of it failed): its owner is cleared and its
/// ending, failed with `why`, is written after the last row that landed,
/// on the caller's transaction. Refused when `owner` does not drive it.
pub async fn give_up_in(conn: &mut PgConnection, execution_id: ExecutionId, owner: &str, why: &str) -> anyhow::Result<GaveUp> {
    let Some(mut locked) = lock_in(conn, execution_id).await? else { return Ok(GaveUp::NotOwned) };
    if locked.state != "running" || locked.owner.as_deref() != Some(owner) {
        return Ok(GaveUp::NotOwned);
    }
    sqlx::query("UPDATE run SET owner = NULL WHERE execution_id = $1").bind(execution_id).execute(&mut *conn).await?;
    locked.owner = None;
    let ending = ExecEvent::ExecutionFailed { execution_id, error: why.to_string(), at_unix: unix_now() as u64 };
    match append_locked_in(conn, execution_id, &locked, std::slice::from_ref(&ending), GAVE_UP, Then::Stays).await? {
        Appended::At(seq) => Ok(GaveUp::Ended(seq)),
        other => anyhow::bail!("run {execution_id} was locked running with no owner, yet its ending was not written: {other:?}"),
    }
}

/// A queued run as the worker that claimed it starts from: how it is
/// fenced (its epoch, which its batches carry), how many times it was
/// handed to a worker, its program, a cancel already waiting for it, and
/// its whole record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claimed {
    pub epoch: i32,
    pub attempts: i32,
    pub definition_hash: String,
    pub cancel_requested: Option<CancelCause>,
    pub record: RawRecord,
}

/// What a claim reads off the run row: its new epoch, its attempts, its
/// program, and a cancel already asked for.
type ClaimedRow = (i32, i32, Option<String>, Option<sqlx::types::Json<CancelCause>>);

/// Claim queued run `execution_id` of `project_id` for `owner`: it is
/// running, owned by `owner` under a raised epoch, its delivery done.
/// `None` when it is not queued (another worker claimed it, it ended, it
/// is not this project's). A claim `owner` holds already is given again
/// under a new epoch: the answer of one that committed may have been lost
/// on its way, and the worker never drives a run twice (it asks only for
/// a run it is not driving), so nothing was written under the old epoch.
pub async fn claim(conn: &mut PgConnection, execution_id: ExecutionId, project_id: uuid::Uuid, owner: &str) -> anyhow::Result<Option<Claimed>> {
    let claimed: Option<ClaimedRow> = sqlx::query_as(
        "UPDATE run SET state = 'running', owner = $3, epoch = epoch + 1, attempts = attempts + 1, delivered_until = NULL \
         WHERE execution_id = $1 AND project_id = $2 AND (state = 'queued' OR (state = 'running' AND owner = $3)) \
         RETURNING epoch, attempts, definition_hash, cancel_requested",
    )
    .bind(execution_id)
    .bind(project_id)
    .bind(owner)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((epoch, attempts, definition_hash, cancel_requested)) = claimed else { return Ok(None) };
    let definition_hash = definition_hash.ok_or_else(|| anyhow::anyhow!("run {execution_id} runs no program, so no worker can claim it"))?;
    let record = read_record(conn, execution_id, None).await?;
    Ok(Some(Claimed { epoch, attempts, definition_hash, cancel_requested: cancel_requested.map(|cause| cause.0), record }))
}

/// The record of `execution_id`: its rows after `after` (every row when
/// `None`, with the selection its birth names).
pub async fn read_record(conn: &mut PgConnection, execution_id: ExecutionId, after: Option<i32>) -> anyhow::Result<RawRecord> {
    let selection = match after {
        Some(_) => None,
        None => sqlx::query_as::<_, (String, sqlx::types::Json<RunSelection>)>(
            "SELECT s.digest, s.selection FROM run r JOIN run_selection s ON s.digest = r.selection WHERE r.execution_id = $1",
        )
        .bind(execution_id)
        .fetch_optional(&mut *conn)
        .await?
        .map(|(digest, selection)| StoredSelection { digest, selection: selection.0 }),
    };
    let rows: Vec<(i32, Vec<u8>)> =
        sqlx::query_as("SELECT seq, events FROM run_log WHERE execution_id = $1 AND seq > $2 ORDER BY seq")
            .bind(execution_id)
            .bind(after.unwrap_or(-1))
            .fetch_all(&mut *conn)
            .await?;
    Ok(RawRecord { selection, rows: rows.into_iter().map(|(seq, events)| RunLogRow { seq, events }).collect() })
}

fn unix_now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |since| since.as_secs() as i64)
}

/// A row's events on the wire, which is JSON: base64.
mod base64_bytes {
    use base64::Engine as _;

    pub fn serialize<S: serde::Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD.decode(text).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> ExecutionId {
        ExecutionId::from_u128(3)
    }

    /// An ending with no error and no cause carries neither key: the batch
    /// reads each key as the row's value, and a JSON `null` would land as
    /// a value no reader decodes.
    #[test]
    fn an_ending_without_a_cause_carries_no_cause() {
        let completed = Ending::of(&ExecEvent::ExecutionCompleted { execution_id: run(), at_unix: 5 }).unwrap();
        let wire = serde_json::to_value(&completed).unwrap();
        assert_eq!(wire, serde_json::json!({ "at": 5, "outcome": "completed" }));
        assert_eq!(serde_json::from_value::<Ending>(wire).unwrap(), completed);
        let cancelled = Ending::of(&ExecEvent::ExecutionCancelled { execution_id: run(), reason: "stopped".into(), cause: Some(CancelCause::User), at_unix: 6 }).unwrap();
        assert!(serde_json::to_value(&cancelled).unwrap().get("cancel_cause").is_some());
    }

    /// A row that does not decode fails the strict read whole, and the
    /// display read keeps every other row and names the bad one.
    #[test]
    fn a_bad_row_fails_the_strict_read_and_is_named_by_the_display_read() {
        let execution_id = run();
        let completed = ExecEvent::ExecutionCompleted { execution_id, at_unix: 2 };
        let record = RawRecord {
            selection: None,
            rows: vec![
                RunLogRow { seq: 1, events: b"not zstd".to_vec() },
                RunLogRow { seq: 2, events: crate::stored::encode(std::slice::from_ref(&completed)) },
            ],
        };
        assert!(record.decode(execution_id).unwrap_err().contains(&execution_id.to_string()));
        let (rows, bad) = record.decode_lossy(execution_id);
        assert_eq!(rows.iter().map(|row| (row.seq, row.index)).collect::<Vec<_>>(), [(2, 0)]);
        assert!(matches!(rows[0].event, ExecEvent::ExecutionCompleted { at_unix: 2, .. }));
        assert_eq!(bad.len(), 1);
    }

    /// A write's costs, skips, answers and ending are read off its events;
    /// a cost nobody could put a figure on adds nothing, and the first
    /// ending stands.
    #[test]
    fn a_write_says_what_its_events_say() {
        let execution_id = run();
        let cost = |amount_usd| ExecEvent::CostReported {
            execution_id,
            node_id: "llm".into(),
            frames: Vec::new(),
            cost_id: "c".into(),
            service: "s".into(),
            model: None,
            amount_usd,
            billed: false,
            origin: weft_core::CredentialOwner::Platform,
            metadata: serde_json::Value::Null,
            at_unix: 1,
        };
        let events = vec![
            cost(Some(0.25)),
            cost(None),
            ExecEvent::NodeSkipped {
                execution_id,
                node_id: "x".into(),
                frames: Vec::new(),
                reason: weft_core::exec::skip::SkipReason::DidNotFlow,
                at_unix: 1,
            },
            ExecEvent::SuspensionResolved { execution_id, token: "t".into(), value: serde_json::Value::Null, at_unix: 2 },
            ExecEvent::ExecutionFailed { execution_id, error: "boom".into(), at_unix: 3 },
            ExecEvent::ExecutionCompleted { execution_id, at_unix: 4 },
        ];
        let written = Written::of(&events);
        assert_eq!(written.cost_micro_usd, 250_000);
        assert_eq!(written.skipped, 1);
        assert_eq!(written.resolved, vec!["t".to_string()]);
        assert_eq!(written.ended, Some(Ending { at: 3, outcome: Outcome::Failed, error: Some("boom".into()), cancel_cause: None }));
    }

    /// A record read raw decodes to its events with their places, its
    /// birth's selection resolved from what was read with it.
    #[test]
    fn a_raw_record_decodes_with_its_places() {
        let execution_id = run();
        let selection = RecordedSelection::new(RunSelection::default());
        let birth = ExecEvent::ExecutionStarted {
            execution_id,
            project_id: uuid::Uuid::nil(),
            entry_node: "door".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("d".into()),
            binary_hash: None,
            source_version: None,
            run_kind: weft_core::exec::RunKind::Execution,
            selection: Some(selection.clone()),
            seed: None,
            instance: None,
            instance_values: Default::default(),
            picks: Default::default(),
            stand_in: None, fired_trigger: None,
            settings: Default::default(),
            at_unix: 0,
        };
        let done = ExecEvent::ExecutionCompleted { execution_id, at_unix: 1 };
        let record = RawRecord {
            selection: Some(StoredSelection::of(&selection)),
            rows: vec![
                RunLogRow { seq: 0, events: crate::stored::encode(std::slice::from_ref(&birth)) },
                RunLogRow { seq: 1, events: crate::stored::encode(std::slice::from_ref(&done)) },
            ],
        };
        let rows = record.decode(execution_id).unwrap();
        assert_eq!(rows.iter().map(|row| (row.seq, row.index)).collect::<Vec<_>>(), vec![(0, 0), (1, 0)]);
        assert!(matches!(&rows[0].event, ExecEvent::ExecutionStarted { selection: Some(read), .. } if *read == selection));
        let wire = serde_json::to_string(&record).unwrap();
        assert_eq!(serde_json::from_str::<RawRecord>(&wire).unwrap(), record, "a record crosses the wire whole");
        let without = RawRecord { selection: None, ..record };
        assert!(without.decode(execution_id).unwrap_err().contains(selection.digest()), "a birth needs its selection");
    }
}
