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
//! The run's row is derived from its events where it can be (its skips,
//! its ending: [`Written`]), and so are its cost rows (`crate::costs`), by
//! whoever writes them, in the same statement or transaction, so neither
//! ever disagrees with its log.

use serde::{Deserialize, Serialize};
use weft_core::run::CancelCause;
use weft_core::project::ProjectDefinition;
use weft_core::projects::PROJECT_BEING_REMOVED;
use weft_core::run_settings::KeepFor;
use weft_core::RunId;
use weft_task_store::db::{self, Client, FromRow, Json, JsonText, Row};

use crate::costs::{Cost, CostColumns};
use crate::events::RunEvent;
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
    /// The version its events were written at (`run_log.events_version`,
    /// `crate::stored::STORED_EVENTS`).
    pub events_version: weft_core::stored::Version,
}

impl RunLogRow {
    /// `events`, all of one run, as this weft writes them at `seq`.
    pub fn of(seq: i32, events: &[RunEvent]) -> Self {
        RunLogRow { seq, events: crate::stored::encode(events), events_version: crate::stored::STORED_EVENTS.version() }
    }
}

/// A program a run runs, as the record keeps it (`project_definition`,
/// under its hash). A run started with bounds runs a cut of its project
/// (`weft_core::project::cut`): the cut is a program of its own, kept the
/// first time a run names it, so the run reads it back by its hash like
/// any other (on every resume, for every reader of its record).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredProgram {
    pub project_id: uuid::Uuid,
    pub definition_hash: String,
    pub project_json: String,
    /// The version its JSON was written at
    /// (`ProjectDefinition::STORED`).
    pub project_json_version: weft_core::stored::Version,
}

/// Its program goes into JSONB (`weft_record_batch`'s `p_programs`) as one
/// JSON string, so a NUL in a node's value is the escaped text `\\u0000`
/// there, never a NUL; the rest is hashes and ids.
impl weft_task_store::db::JsonbSafe for StoredProgram {}

impl StoredProgram {
    /// `program`, of `project_id`, under the hash it is read back by.
    pub fn of(project_id: uuid::Uuid, program: &ProjectDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            project_id,
            definition_hash: weft_core::project::hash::compute_definition_hash(program)?,
            // A string, not the program's JSON itself: a NUL in a node's
            // value must stay escaped text inside the JSONB it goes into.
            project_json: serde_json::to_string(program)?,
            project_json_version: ProjectDefinition::STORED.version(),
        })
    }

    /// Refuse a program that is not what it says: written at another
    /// version than today's (only a worker an older weft built sends one,
    /// and it is never accommodated: `weft resync` rebuilds it), no
    /// program of its project, or not hashing to the hash it is kept under
    /// (a run reading it back by that hash would run something else).
    pub fn check(&self) -> anyhow::Result<()> {
        let today = ProjectDefinition::STORED.version();
        anyhow::ensure!(
            self.project_json_version == today,
            "program {} is written at version {}, and this weft reads version {today}: the worker that sent it was built by another weft; `weft resync` rebuilds it",
            self.definition_hash,
            self.project_json_version
        );
        let program: ProjectDefinition = serde_json::from_str(&self.project_json)
            .map_err(|e| anyhow::anyhow!("program {}: {e}", self.definition_hash))?;
        anyhow::ensure!(program.id == self.project_id, "program {} is of project {}, not {}", self.definition_hash, program.id, self.project_id);
        let hash = weft_core::project::hash::compute_definition_hash(&program)?;
        anyhow::ensure!(hash == self.definition_hash, "program {} hashes to {hash}", self.definition_hash);
        Ok(())
    }
}

/// The program a run runs, as its birth names it and its start keeps it:
/// the program (a cut of the registered one, or the registered one
/// itself when the cut keeps everything) and, for a cut, the registered
/// program it was cut from. Built once ([`Self::of`]); the birth
/// (`crate::birth::Birth`) names the run's program from it, and the start
/// that keeps the cut ([`Queued`], a worker's batch) keeps it from it, so
/// the two cannot name different programs.
#[derive(Debug, Clone)]
pub struct RunsProgram {
    stored: std::sync::Arc<StoredProgram>,
    cut_from: Option<String>,
}

impl RunsProgram {
    /// `cut`, of `project_id`, cut from the registered program whose hash
    /// is `registered_hash`. A cut that hashes to the registered program
    /// is that program: it names no `cut_from` and keeps nothing.
    pub fn of(registered_hash: &str, project_id: uuid::Uuid, cut: &ProjectDefinition) -> anyhow::Result<Self> {
        let stored = StoredProgram::of(project_id, cut)?;
        let cut_from = (stored.definition_hash != registered_hash).then(|| registered_hash.to_string());
        Ok(Self { stored: std::sync::Arc::new(stored), cut_from })
    }

    /// The hash the run's birth names its program by.
    pub fn definition_hash(&self) -> &str {
        &self.stored.definition_hash
    }

    /// The registered program a cut was cut from
    /// (`RunEvent::RunStarted::cut_from`); `None` for the registered
    /// program itself.
    pub fn cut_from(&self) -> Option<&str> {
        self.cut_from.as_deref()
    }

    /// What the run's start keeps with it: the cut, when it is one. The
    /// registered program is kept already, by the build that registered it.
    pub fn kept(&self) -> Option<&std::sync::Arc<StoredProgram>> {
        self.cut_from.is_some().then_some(&self.stored)
    }

    /// How many bytes the program's JSON, held written out here, weighs.
    pub fn json_bytes(&self) -> usize {
        self.stored.project_json.len()
    }
}

/// A run's record as read, raw: its rows from some `seq` on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawRecord {
    pub rows: Vec<RunLogRow>,
}

impl RawRecord {
    /// The record's events, each with its place. One row that does not
    /// decode fails the whole read: a fold over part of a run rebuilds a
    /// run that never was.
    pub fn decode(&self, run_id: RunId) -> Result<Vec<JournalRow>, String> {
        let (decoded, bad) = self.decode_lossy(run_id);
        match bad.into_iter().next() {
            Some((_, reason)) => Err(reason),
            None => Ok(decoded),
        }
    }

    /// The record's events for DISPLAY: every row that decodes, and the
    /// place (`seq`) and reason of each row that does not, so a reader
    /// shows what exists and says what it cannot, where it sits.
    pub fn decode_lossy(&self, run_id: RunId) -> (Vec<JournalRow>, Vec<(i32, String)>) {
        let mut decoded = Vec::new();
        let mut bad = Vec::new();
        for row in &self.rows {
            match crate::stored::decode(run_id, &row.events, row.events_version) {
                Ok(events) => decoded
                    .extend(events.into_iter().enumerate().map(|(index, event)| JournalRow { seq: row.seq, index: index as u32, event })),
                Err(reason) => bad.push((row.seq, reason)),
            }
        }
        (decoded, bad)
    }

    /// The record's events, in order (see [`Self::decode`]).
    pub fn events(&self, run_id: RunId) -> Result<Vec<RunEvent>, String> {
        Ok(self.decode(run_id)?.into_iter().map(|row| row.event).collect())
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_cause: Option<CancelCause>,
}

impl Ending {
    /// The ending `event` records, if it is one.
    pub fn of(event: &RunEvent) -> Option<Self> {
        let at = event.at_unix() as i64;
        match event {
            RunEvent::RunCompleted { .. } => Some(Self { at, outcome: Outcome::Completed, error: None, cancel_cause: None }),
            RunEvent::RunFailed { error, .. } => Some(Self { at, outcome: Outcome::Failed, error: Some(error.clone()), cancel_cause: None }),
            RunEvent::RunCancelled { cause, .. } => {
                Some(Self { at, outcome: Outcome::Cancelled, error: None, cancel_cause: cause.clone() })
            }
            _ => None,
        }
    }
}

/// What a write of a run's events says about the run besides its rows:
/// the costs its metered calls reported (each a row of `run_cost`,
/// `crate::costs`), how many firings it skipped, its ending, and the
/// answers it took (so the waiting answers go, `parked_fire`). Read off
/// the events by whoever writes them, so a run's row and its log never
/// disagree.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Written {
    pub costs: Vec<Cost>,
    pub skipped: i32,
    pub ended: Option<Ending>,
    pub resolved: Vec<String>,
}

impl Written {
    /// What `events`, one write of one run, say. A run has one ending: a
    /// second one in its record would be a writer's bug, and the first
    /// stands.
    pub fn of(events: &[RunEvent]) -> Self {
        let mut written = Self { costs: Cost::all_of(events), ..Self::default() };
        for event in events {
            match event {
                RunEvent::NodeSkipped { .. } => written.skipped += 1,
                RunEvent::SuspensionResolved { token, .. } | RunEvent::SuspensionSkipped { token, .. } => {
                    written.resolved.push(token.clone())
                }
                _ => {}
            }
            if written.ended.is_none() {
                written.ended = Ending::of(event);
            }
        }
        written
    }
}

/// The columns a run's row is born with, read off its birth (its
/// `RunStarted`), with how long it is kept once it ended, resolved
/// against its project's default.
// SYNC: Born's fields <-> weft_record_batch's `p_born` (crates/weft-dispatcher/src/store/postgres.rs)
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
    /// The registered program a run of a cut was cut from
    /// (`RunEvent::RunStarted::cut_from`).
    pub cut_from: Option<String>,
    pub binary_hash: Option<String>,
    pub seed_of: Option<RunId>,
    pub started_at: i64,
    pub keep_for: Option<i64>,
    /// The version a spec on this row is written at (`run.spec_version`,
    /// `RunSpec::STORED`): a run born in a batch holds no spec, and the
    /// column, NOT NULL with no default, is still bound by every writer.
    pub spec_version: weft_core::stored::Version,
}

/// Ids, names weft checked and hashes, nothing a node or a person handed
/// over as a value: it goes into JSONB (`p_born`).
impl weft_task_store::db::JsonbSafe for Born {}

impl Born {
    /// The row `birth` makes, kept for `keep_for` once it ended; `None`
    /// when `birth` is not a birth.
    pub fn of(birth: &RunEvent, keep_for: KeepFor) -> Option<Self> {
        let RunEvent::RunStarted {
            project_id, entry_node, phase, definition_hash, cut_from, binary_hash, source_version, run_kind, seed, instance,
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
            cut_from: cut_from.clone(),
            binary_hash: binary_hash.clone(),
            seed_of: seed.as_ref().map(|seed| seed.parent),
            started_at: *at_unix as i64,
            keep_for: keep_for.seconds(),
            spec_version: weft_core::run_spec::RunSpec::STORED.version(),
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
    /// A run that started in this batch for a project `weft rm` is
    /// removing ([`PROJECT_BEING_REMOVED`]): nothing of it went in.
    ProjectRemoving,
    /// Its rows do not follow on from its record under this writer (it
    /// lost the run to another worker, or the run ended or was let go), or
    /// it started for a project that has no row: nothing went in.
    Refused,
}

impl Fate {
    fn parse(text: &str) -> anyhow::Result<Self> {
        Ok(match text {
            "accepted" => Self::Accepted,
            "already_applied" => Self::AlreadyApplied,
            "born_elsewhere" => Self::BornElsewhere,
            "project_removing" => Self::ProjectRemoving,
            "refused" => Self::Refused,
            other => anyhow::bail!("the record answered a run with '{other}', which weft does not read"),
        })
    }
}

/// One run's answer from [`record_batch`]: its fate, its project (`None`
/// for a run the record does not hold), whether it ended now with
/// somebody to tell (it is watched, or holds signals to take down), and,
/// when it ended now, until when it is kept (`None` for ever, or when it
/// did not end now).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded {
    pub run_id: RunId,
    pub fate: Fate,
    pub project_id: Option<uuid::Uuid>,
    pub tell_end: bool,
    pub keep_until: Option<i64>,
    /// Born running now under a writer with no live lease: nothing has
    /// woken the dispatcher's look at lapsed holds for that worker yet, so
    /// whoever wrote it rings it, or the run would be stranded if the
    /// worker died before its first tick.
    pub unwatched: bool,
}

/// One run's answer as `weft_record_batch` gives it: its id, its fate,
/// its project, whether it ended with somebody to tell, its erase time,
/// whether it was born unwatched.
type RecordedRow = (RunId, String, Option<uuid::Uuid>, bool, Option<i64>, bool);

/// Write one batch of a worker's writer lane (`crate::frame`): every run
/// of it in one statement and one transaction, synced to disk when the
/// batch says somebody waits on it. `writer` is the worker's replica, the
/// owner the batch's runs are fenced on; `lane` keys its version counts.
/// `seal` seals each secret a run made (`crate::frame::KeptSecret`), which
/// is kept, sealed, only for a run that went in: a secret's value never
/// reaches the database any other way. Answers each run's fate, in the
/// batch's order. Sending a batch again is always safe: what already went
/// in answers `AlreadyApplied`, a secret included.
pub async fn record_batch(
    db: &impl Client,
    writer: &str,
    tenant: &str,
    lane: &str,
    batch: &crate::frame::Batch<'_>,
    seal: &(dyn Fn(&serde_json::Value) -> anyhow::Result<String> + Sync),
) -> anyhow::Result<Vec<Recorded>> {
    let head = &batch.head;
    let mut ids = Vec::with_capacity(head.runs.len());
    let mut epochs = Vec::with_capacity(head.runs.len());
    let mut first = Vec::with_capacity(head.runs.len());
    let mut last = Vec::with_capacity(head.runs.len());
    let mut costs = CostColumns::default();
    let mut skipped = Vec::with_capacity(head.runs.len());
    let mut born = Vec::with_capacity(head.runs.len());
    let mut ended_at = Vec::with_capacity(head.runs.len());
    let mut outcomes = Vec::with_capacity(head.runs.len());
    let mut errors = Vec::with_capacity(head.runs.len());
    let mut cancel_causes = Vec::with_capacity(head.runs.len());
    let mut row_run = Vec::new();
    let mut row_seq = Vec::new();
    let mut resolved_run = Vec::new();
    let mut resolved_token = Vec::new();
    let mut secret_run = Vec::new();
    let mut secret_id = Vec::new();
    let mut secret_sealed = Vec::new();
    for (at, run) in head.runs.iter().enumerate() {
        let ordinal = i32::try_from(at + 1)?;
        ids.push(run.run_id);
        epochs.push(run.epoch);
        first.push(run.first_seq);
        last.push(run.first_seq + i32::try_from(run.row_sizes.len())? - 1);
        for cost in &run.written.costs {
            costs.push(ordinal, run.first_seq, cost);
        }
        skipped.push(run.written.skipped);
        born.push(&run.born);
        let ended = run.written.ended.as_ref();
        ended_at.push(ended.map(|ending| ending.at));
        outcomes.push(ended.map(|ending| ending.outcome.as_str()));
        errors.push(ended.and_then(|ending| ending.error.as_deref()));
        cancel_causes.push(ended.and_then(|ending| ending.cancel_cause.as_ref()).map(JsonText));
        for seq in run.first_seq..run.first_seq + i32::try_from(run.row_sizes.len())? {
            row_run.push(ordinal);
            row_seq.push(seq);
        }
        for token in &run.written.resolved {
            resolved_run.push(ordinal);
            resolved_token.push(token.as_str());
        }
        for secret in &run.secrets {
            secret_run.push(ordinal);
            secret_id.push(secret.secret_id.as_uuid());
            secret_sealed.push(seal(&secret.value)?);
        }
    }
    let rows: Vec<&[u8]> = batch.rows.clone();
    let now = unix_now();
    let delivered = delivered_until(now);
    let (born, programs) = (Json(&born), Json(&head.programs));
    let mut params: Vec<db::Param<'_>> = vec![
        &writer,
        &tenant,
        &lane,
        &now,
        &delivered,
        &head.durable,
        &ids,
        &epochs,
        &first,
        &last,
        &skipped,
        &born,
        &ended_at,
        &outcomes,
        &errors,
        &cancel_causes,
        &row_run,
        &row_seq,
        &rows,
        &resolved_run,
        &resolved_token,
    ];
    params.extend(costs.params());
    params.extend([&secret_run as db::Param<'_>, &secret_id, &secret_sealed]);
    params.extend([&programs as db::Param<'_>, &head.events_version]);
    let answered: Vec<RecordedRow> = db
        .all(
            "SELECT o_run_id, o_fate, o_project_id, o_tell_end, o_keep_until, o_unwatched FROM weft_record_batch(\
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, \
             $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39)",
            &params,
        )
        .await?;
    anyhow::ensure!(
        answered.len() == ids.len() && answered.iter().zip(&ids).all(|(answer, id)| answer.0 == *id),
        "the record answered a batch of {} runs with {} answers out of its order",
        ids.len(),
        answered.len()
    );
    answered
        .into_iter()
        .map(|(run_id, fate, project_id, tell_end, keep_until, unwatched)| {
            Ok(Recorded { run_id, fate: Fate::parse(&fate)?, project_id, tell_end, keep_until, unwatched })
        })
        .collect()
}

/// A run the dispatcher starts for a worker to claim: `weft run`, a setup
/// run, a program's call that starts one. Everything its row is born with
/// besides what its birth says.
#[derive(Debug, Clone)]
pub struct Queued<'a> {
    /// Its birth, its kicks, and what it is born holding (`events[0]` is
    /// its `RunStarted`).
    pub events: &'a [RunEvent],
    pub tenant: &'a str,
    pub keep_for: KeepFor,
    /// Somebody waits on its ending: a trigger setup, whose bake is made
    /// from it (`weft_dispatcher::run_ends`).
    pub watch_end: bool,
    /// What the version tree shows of a run started by hand.
    pub stale: &'a [String],
    /// What it was asked to run, written as `RunSpec::STORED` writes it.
    pub spec: Option<&'a weft_core::run_spec::RunSpec>,
    pub example: Option<&'a str>,
    /// The program its birth was born naming (`crate::birth::Birth`): its
    /// cut is kept with the run, once. `None` for a birth built by hand
    /// that names a registered program or none.
    pub program: Option<&'a RunsProgram>,
}

/// Start `queued` on the caller's transaction: its row (`queued`, no owner,
/// never claimed), its record's first row and the cut it runs, and the
/// delivery woken. `false` when the run is already on record (a retried
/// start, by its id): nothing is written then. Errs, writing nothing, when
/// its project has no row or is being removed. A queued run is started by
/// hand or sets triggers up, never fired by one, so it adds nothing to its
/// version's trigger runs (those are counted as workers bear them).
pub async fn insert_queued_in(db: &impl Client, queued: &Queued<'_>) -> anyhow::Result<bool> {
    let birth = queued.events.first().ok_or_else(|| anyhow::anyhow!("a run is started with at least its birth"))?;
    let run_id = birth.run_id();
    let born = Born::of(birth, queued.keep_for).ok_or_else(|| anyhow::anyhow!("run {run_id} is started with a row that is not its birth"))?;
    if let Some(stray) = queued.events.iter().find(|event| event.run_id() != run_id) {
        anyhow::bail!("the start of run {run_id} carries a row of run {}", stray.run_id());
    }
    // The birth names the program it was born with: a cut it names is the
    // one kept with it, and nothing else is.
    let (named, cut_from) = match queued.program {
        Some(program) => (Some(program.definition_hash()), program.cut_from()),
        None => (born.definition_hash.as_deref(), None),
    };
    anyhow::ensure!(
        born.definition_hash.as_deref() == named && born.cut_from.as_deref() == cut_from,
        "run {run_id} is born naming program {:?} cut from {:?}, but is started with program {named:?} cut from {cut_from:?}",
        born.definition_hash,
        born.cut_from,
    );
    if let Some(cut) = queued.program.and_then(RunsProgram::kept) {
        anyhow::ensure!(cut.project_id == born.project_id, "run {run_id} of project {} is started with a cut of project {}", born.project_id, cut.project_id);
    }
    let written = Written::of(queued.events);
    // A queued run has not run: an ending in its start would leave a row
    // that waits for a worker over a record that says it ended.
    anyhow::ensure!(written.ended.is_none(), "run {run_id} is started with its ending");
    // Only for a project still there and not being removed: `weft rm`
    // marks the project first and takes its runs down after, so a run
    // started once the mark is in would outlive the take-down.
    // Each value is cast: a `$n` in the SELECT list of an INSERT has no
    // type of its own to be prepared with.
    let spec = queued.spec.map(JsonText);
    let inserted = db
        .execute(
            "INSERT INTO run (run_id, project_id, tenant_id, phase, kind, keeping, recorded, entry_node, instance_id, \
                 fired_by, source_version, definition_hash, binary_hash, seed_of, stale, spec, example, state, \
                 epoch, last_seq, attempts, watch_end, started_at, skipped, keep_for, cut_from, spec_version) \
             SELECT $1::uuid, $2::uuid, $3::text, $4::text, $5::text, $6::text, $7::boolean, $8::text, $9::text, $10::text, \
                 $11::text, $12::text, $13::text, $14::uuid, $15::text[], $16::text, $17::text, 'queued', \
                 0, 0, 0, $18::boolean, $19::bigint, $20::integer, $21::bigint, $22::text, $23::integer \
             WHERE EXISTS (SELECT 1 FROM project WHERE id = $2 AND NOT removing) \
             ON CONFLICT (run_id) DO NOTHING",
            &[
                &run_id,
                &born.project_id,
                &queued.tenant,
                &born.phase,
                &born.kind,
                &born.keeping,
                &born.recorded,
                &born.entry_node,
                &born.instance_id,
                &born.fired_by,
                &born.source_version,
                &born.definition_hash,
                &born.binary_hash,
                &born.seed_of,
                &queued.stale,
                &spec,
                &queued.example,
                &queued.watch_end,
                &born.started_at,
                &written.skipped,
                &born.keep_for,
                &born.cut_from,
                &weft_core::run_spec::RunSpec::STORED.version(),
            ],
        )
        .await?;
    if inserted == 0 {
        let project = born.project_id;
        let removing: Option<bool> = db.scalar_opt("SELECT removing FROM project WHERE id = $1", &[&project]).await?;
        return match removing {
            Some(false) => Ok(false),
            Some(true) => anyhow::bail!("run {run_id} is not started for project {project}: {PROJECT_BEING_REMOVED}"),
            None => anyhow::bail!("run {run_id} is started for project {project}, which has no row: it was removed, or was never registered"),
        };
    }
    // Only now that the run is on record: a refused start keeps nothing.
    if let Some(program) = queued.program {
        insert_cut_in(db, program).await?;
    }
    db.execute(
        "INSERT INTO run_log (run_id, seq, events, events_version, writer, written_at) VALUES ($1, 0, $2, $3, $4, $5)",
        &[
            &run_id,
            &crate::stored::encode(queued.events),
            &crate::stored::STORED_EVENTS.version(),
            &DISPATCHER,
            &unix_now(),
        ],
    )
    .await?;
    crate::costs::insert_in(db, run_id, 0, &written.costs).await?;
    notify_queued_in(db, born.project_id).await?;
    Ok(true)
}

/// Announce, in the caller's transaction, that a run of `project_id` was
/// queued for a worker (`weft_task_store::runs::RUN_QUEUED_CHANNEL`).
pub async fn notify_queued_in(db: &impl Client, project_id: uuid::Uuid) -> anyhow::Result<()> {
    db.execute("SELECT pg_notify($1, $2)", &[&weft_task_store::runs::RUN_QUEUED_CHANNEL, &project_id.to_string()])
        .await?;
    Ok(())
}

/// Keep the cut `program` runs in the record, once, marked with the
/// registered program it was cut from (`project_definition.cut_from`, so
/// it never reads as a registered version). Nothing for a run of the
/// registered program itself. In the caller's transaction: a run's start
/// keeps it, and so does a move of paused runs onto a new build
/// (`weft_dispatcher`'s `move_paused_runs`).
pub async fn insert_cut_in(db: &impl Client, program: &RunsProgram) -> anyhow::Result<()> {
    let Some(cut) = program.kept() else { return Ok(()) };
    db.execute(
        "INSERT INTO project_definition (project_id, definition_hash, project_json, project_json_version, recorded_at_unix, cut_from, trigger_cuts_walked) \
         VALUES ($1, $2, $3, $4, $5, $6, FALSE) ON CONFLICT (project_id, definition_hash) DO NOTHING",
        &[&cut.project_id, &cut.definition_hash, &cut.project_json, &cut.project_json_version, &unix_now(), &program.cut_from()],
    )
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

/// The row of `run_id`, locked for the caller's transaction, as a
/// writer that is not its owner reads it.
#[derive(Debug, Clone)]
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
    /// Its workers failed its delivery since it was last claimed, which
    /// only a queued run carries (`run.waited_on_workers`): read when it
    /// leaves the queue, never for whether it still waits on them now.
    pub waited_on_workers: bool,
    /// Why it was asked to stop while a worker drove it
    /// (`run.cancel_requested`): the worker that drives it ends it for
    /// this, and whoever lets go of it must not leave it waiting with the
    /// ask unread.
    pub cancel_requested: Option<JsonText<CancelCause>>,
}

impl FromRow for Locked {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            project_id: row.try_get("project_id")?,
            tenant_id: row.try_get("tenant_id")?,
            state: row.try_get("state")?,
            keeping: row.try_get("keeping")?,
            owner: row.try_get("owner")?,
            last_seq: row.try_get("last_seq")?,
            epoch: row.try_get("epoch")?,
            keep_for: row.try_get("keep_for")?,
            definition_hash: row.try_get("definition_hash")?,
            waited_on_workers: row.try_get("waited_on_workers")?,
            cancel_requested: row.try_get("cancel_requested")?,
        })
    }
}

/// Lock `run_id`'s row on the caller's transaction (`FOR UPDATE`),
/// the one serialization point of a run every writer that is not its
/// owner goes through.
pub async fn lock_in(db: &impl Client, run_id: RunId) -> anyhow::Result<Option<Locked>> {
    Ok(db
        .opt(
            "SELECT project_id, tenant_id, state, keeping, owner, last_seq, epoch, keep_for, definition_hash, waited_on_workers, \
             cancel_requested FROM run WHERE run_id = $1 FOR UPDATE",
            &[&run_id],
        )
        .await?)
}

/// Write `events` into the record of `run_id`, which nobody drives,
/// at `last_seq + 1`, on the caller's transaction, and move the run as
/// `then` says (an ending in `events` ends it whatever `then` says). Its
/// skips and ending go on its row, and its costs into `run_cost`, in the
/// same transaction. `writer`
/// is who the record says wrote it. Refused when a worker drives the run
/// (its owner writes its record) or it already ended. The write is
/// announced through the outbox: the caller pokes it once its transaction
/// commits (`weft_task_store::announce::committed`).
pub async fn append_unowned_in(
    db: &impl Client,
    run_id: RunId,
    events: &[RunEvent],
    writer: &str,
    then: Then,
) -> anyhow::Result<Appended> {
    let Some(locked) = lock_in(db, run_id).await? else { return Ok(Appended::Missing) };
    append_locked_in(db, run_id, &locked, events, writer, then).await
}

/// [`append_unowned_in`] on a row the caller already locked ([`lock_in`]).
pub async fn append_locked_in(
    db: &impl Client,
    run_id: RunId,
    locked: &Locked,
    events: &[RunEvent],
    writer: &str,
    then: Then,
) -> anyhow::Result<Appended> {
    if let Some(owner) = &locked.owner {
        return Ok(Appended::Driven { owner: owner.clone() });
    }
    if locked.state == "ended" {
        return Ok(Appended::Ended);
    }
    if let Some(stray) = events.iter().find(|event| event.run_id() != run_id) {
        anyhow::bail!("a write to run {run_id} carries a row of run {}", stray.run_id());
    }
    let seq = locked.last_seq + 1;
    let now = unix_now();
    if !events.is_empty() {
        db.execute(
            "INSERT INTO run_log (run_id, seq, events, events_version, writer, written_at) VALUES ($1, $2, $3, $4, $5, $6)",
            &[&run_id, &seq, &crate::stored::encode(events), &crate::stored::STORED_EVENTS.version(), &writer, &now],
        )
        .await?;
    }
    let written = Written::of(events);
    crate::costs::insert_in(db, run_id, seq, &written.costs).await?;
    let ended = written.ended.as_ref();
    let state = match (ended, then) {
        (Some(_), _) => "ended",
        (None, Then::Queued) => "queued",
        (None, Then::Stays) => locked.state.as_str(),
    };
    // Announced through the outbox (`weft_announce`), which its writer
    // pokes once it commits: its new row for the live view, its ending
    // when somebody waits on it, and, when it ends to be erased, to
    // retention.
    let (tell_end, erased_later): (bool, bool) = db
        .one(
            "UPDATE run SET last_seq = $2, state = $3, skipped = skipped + $4, \
                 ended_at = $5, outcome = $6, error = $7, cancel_cause = $8, keep_until = $5 + keep_for, \
                 delivered_until = NULL, busy_since = CASE WHEN $5::bigint IS NULL THEN busy_since END, \
                 waited_on_workers = waited_on_workers AND $5::bigint IS NULL, \
                 holds_signals = ($5::bigint IS NOT NULL AND EXISTS (SELECT 1 FROM signal s WHERE s.run_id = $1)) \
             WHERE run_id = $1 \
             RETURNING $5::bigint IS NOT NULL AND (watch_end OR holds_signals), $5::bigint IS NOT NULL AND keep_for IS NOT NULL",
            &[
                &run_id,
                &if events.is_empty() { locked.last_seq } else { seq },
                &state,
                &written.skipped,
                &ended.map(|ending| ending.at),
                &ended.map(|ending| ending.outcome.as_str()),
                &ended.and_then(|ending| ending.error.as_deref()),
                &ended.and_then(|ending| ending.cancel_cause.as_ref()).map(JsonText),
            ],
        )
        .await?;
    let mut channels = Vec::new();
    let mut payloads = Vec::new();
    if !events.is_empty() {
        channels.push(crate::RUN_LOG_CHANNEL);
        payloads.extend(crate::run_log_payloads(&locked.project_id, [&run_id]));
    }
    if tell_end {
        channels.push(crate::RUN_ENDED_CHANNEL);
        payloads.extend(crate::run_ended_payloads([&run_id]));
    }
    if erased_later {
        channels.push(crate::RETENTION_CHANNEL);
        payloads.push(String::new());
    }
    if !channels.is_empty() {
        db.execute("SELECT weft_announce(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)", &[&channels, &payloads])
            .await?;
    }
    if state == "queued" {
        notify_queued_in(db, locked.project_id).await?;
    }
    if !written.resolved.is_empty() {
        db.execute("DELETE FROM parked_fire WHERE token = ANY($1) AND is_resume", &[&written.resolved]).await?;
    }
    if ended.is_some() && locked.waited_on_workers {
        // A queued run its workers had failed (busy, refused, not reached)
        // ended without reaching one: what `weft status` shows of them
        // goes with it, as a claim's does, unless another queued run still
        // waits on them.
        weft_task_store::unanswered::delivery_waits_ended(db, locked.project_id).await?;
    }
    if ended.is_some() {
        // An answer handed to the run that it never took has nobody left
        // to take it.
        db.execute("DELETE FROM parked_fire WHERE run_id = $1", &[&run_id]).await?;
        db.execute(
            "INSERT INTO run_search_queue (run_id) SELECT run_id FROM run WHERE run_id = $1 AND recorded \
             ON CONFLICT (run_id) DO NOTHING",
            &[&run_id],
        )
        .await?;
    }
    Ok(Appended::At(seq))
}

/// Write the answers handed to `run_id` while a worker drove it
/// (`weft_task_store::parked_fires::hand_answer_in`) into its record, as
/// `SuspensionResolved` (or `SuspensionSkipped` for a skip) at
/// `last_seq + 1`, and queue it for a worker to
/// carry on, on the caller's transaction with its row locked as `locked`
/// and no owner left (its worker just let go of it, or went away). Answers
/// whether any were waiting; with none, nothing is written.
pub async fn resolve_handed_in(db: &impl Client, run_id: RunId, locked: &Locked, writer: &str) -> anyhow::Result<bool> {
    let handed = weft_task_store::parked_fires::answers_for(db, run_id).await?;
    if handed.is_empty() {
        return Ok(false);
    }
    let at_unix = unix_now() as u64;
    let resolved: Vec<RunEvent> = handed
        .into_iter()
        .map(|(token, answer)| RunEvent::wait_answered(run_id, token, answer, at_unix))
        .collect();
    match append_locked_in(db, run_id, locked, &resolved, writer, Then::Queued).await? {
        Appended::At(_) => Ok(true),
        other => anyhow::bail!("run {run_id} was locked with no owner, yet the answers handed to it were not written: {other:?}"),
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

/// End `run_id`, a run `owner` drives but whose record it can no
/// longer write (a write of it failed): its owner is cleared and, after
/// the last row that landed, every charge its record leaves open is booked
/// unknown (`crate::fold::unpriced_charges_closed`: the worker's own
/// booking of them could not be written) and its ending, failed with
/// `why`, is written, on the caller's transaction. Refused when `owner`
/// does not drive it.
pub async fn give_up_in(db: &impl Client, run_id: RunId, owner: &str, why: &str) -> anyhow::Result<GaveUp> {
    let Some(mut locked) = lock_in(db, run_id).await? else { return Ok(GaveUp::NotOwned) };
    if locked.state != "running" || locked.owner.as_deref() != Some(owner) {
        return Ok(GaveUp::NotOwned);
    }
    // Its worker gives it up itself: no memory note of a worker that went
    // away is left on it (`run.lost_memory_full_at`).
    db.execute(
        "UPDATE run SET owner = NULL, lost_memory_full_at = NULL, lost_memory_used = NULL, lost_memory_limit = NULL WHERE run_id = $1",
        &[&run_id],
    )
    .await?;
    locked.owner = None;
    let at_unix = unix_now() as u64;
    // A record that does not read still ends: the run's ending is what
    // matters most, and the charges it may leave open are said lost.
    let read = fold_pages(|after| read_page(db, run_id, after), None, Ok(crate::fold::ChargeBook::default()), move |book: &mut Result<crate::fold::ChargeBook, String>, page| {
        if let Ok(open) = book {
            match page.decode(run_id) {
                Ok(rows) => rows.iter().for_each(|row| open.apply(&row.event)),
                Err(e) => *book = Err(e),
            }
        }
        Ok(())
    })
    .await?;
    let mut writes = match read {
        Ok(book) => crate::fold::unpriced_charges_closed(
            run_id,
            book,
            "its worker gave the run up, so it ended before the job was read back",
            at_unix,
        ),
        Err(e) => {
            tracing::error!(
                target: "weft_journal::record", %run_id, error = %e,
                "a run given up has a record that does not read; any charge it left open stays off its cost trail"
            );
            Vec::new()
        }
    };
    writes.push(RunEvent::RunFailed { run_id, error: why.to_string(), at_unix });
    match append_locked_in(db, run_id, &locked, &writes, GAVE_UP, Then::Stays).await? {
        Appended::At(seq) => Ok(GaveUp::Ended(seq)),
        other => anyhow::bail!("run {run_id} was locked running with no owner, yet its ending was not written: {other:?}"),
    }
}

/// A queued run as the worker that claimed it starts from: how it is
/// fenced (its epoch, which its batches carry), how many times it was
/// handed to a worker, its program, a cancel already waiting for it, and
/// the memory note of the worker it was lost by, when that worker went
/// away with its memory nearly full (`run.lost_memory_full_at`,
/// `weft_task_store::worker_door::MemoryNote`). Its record the worker
/// reads after, page by page ([`read_page`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claimed {
    pub epoch: i32,
    pub attempts: i32,
    pub definition_hash: String,
    pub cancel_requested: Option<CancelCause>,
    pub lost_full: Option<weft_task_store::worker_door::MemoryNote>,
}

/// What a claim reads off the run row: its new epoch, its attempts, its
/// program, a cancel already asked for, and the memory note of the worker
/// it was lost by (its three columns). The first three are there exactly
/// when the run was claimed, which a run of no program never is.
type ClaimedRow = (Option<i32>, Option<i32>, Option<String>, Option<JsonText<CancelCause>>, Option<i64>, Option<i64>, Option<i64>);

/// Until when a run claimed or born by a worker at `now` counts as
/// delivered to that worker though its lease is not written yet
/// (`run.delivered_until`): one lease length at this install's pace. The
/// worker's first tick lands well within it, and the lost-run sweep leaves
/// the run alone until then (`weft_task_store::run_held_until_sql!`).
pub fn delivered_until(now: i64) -> i64 {
    now + weft_core::time_scale::scaled_secs(weft_task_store::worker_door::WORKER_LEASE_SECS)
}

/// Claim queued run `run_id` of `project_id` for `owner`: it is
/// running, owned by `owner` under a raised epoch, and it counts as
/// `owner`'s until [`delivered_until`] whatever its lease says; the tries
/// its delivery counted (refusals, hand-offs with no answer, a stretch of
/// busy answers) start again from none, and so does the mark that its
/// workers failed it (`run.waited_on_workers`), so only a queued run ever
/// carries them. A run carrying that mark reaching a worker ends what
/// `weft status` shows of the project's workers, once no other queued run
/// of the project waits on them
/// (`weft_task_store::unanswered::delivery_waits_ended`): the mark, not
/// the counts, since its tries starting again (a worker coming alive, a
/// build registering) reset the counts while the failure they counted
/// is still what `weft status` shows. `None` when it is not queued
/// (another worker claimed it, it ended, it is not this project's); an
/// error, claiming nothing, when it runs no program (a node test's run,
/// which delivery ends before it reaches a worker). A claim `owner` holds already is given again
/// under a new epoch: the answer of one that committed may have been lost
/// on its way, and the worker never drives a run twice (it asks only for
/// a run it is not driving), so nothing was written under the old epoch.
/// The claim leaves the run's memory note (`run.lost_memory_full_at`) as
/// it is: a claim given again reads the same note, and whatever lets the
/// run go next writes it anew.
pub async fn claim(db: &impl Client, run_id: RunId, project_id: uuid::Uuid, owner: &str) -> anyhow::Result<Option<Claimed>> {
    // The row is locked first and its mark read off the version the lock
    // waited for (a `busy` that committed meanwhile included), then
    // cleared by the update, and when it was set, what `weft status` shows
    // of the project's workers ended, all in one statement: they commit
    // together, so a claim never stands with that left showing. A run of
    // no program is found and left as it is, so the claim refuses it
    // having written nothing: a claim that stood would leave the run with
    // an owner that never drives it.
    let sql = format!(
        "WITH was AS (SELECT waited_on_workers, definition_hash IS NOT NULL AS runs_a_program FROM run \
                      WHERE run_id = $1 AND project_id = $2 AND (state = 'queued' OR (state = 'running' AND owner = $3)) \
                      FOR UPDATE), \
         claimed AS (UPDATE run SET state = 'running', owner = $3, epoch = epoch + 1, attempts = attempts + 1, delivered_until = $4, \
                         refusals = 0, unreached = 0, busy_since = NULL, waited_on_workers = FALSE \
                      FROM was WHERE run.run_id = $1 AND was.runs_a_program \
                      RETURNING run.epoch, run.attempts, run.definition_hash, run.cancel_requested, run.lost_memory_full_at, \
                                run.lost_memory_used, run.lost_memory_limit, was.waited_on_workers), \
         ended AS ({ended} AND EXISTS (SELECT 1 FROM claimed WHERE claimed.waited_on_workers)) \
         SELECT claimed.epoch, claimed.attempts, claimed.definition_hash, claimed.cancel_requested, \
                claimed.lost_memory_full_at, claimed.lost_memory_used, claimed.lost_memory_limit \
         FROM was LEFT JOIN claimed ON TRUE",
        ended = weft_task_store::unanswered::delivery_waits_ended_sql("$2", Some("$1")),
    );
    let claimed: Option<ClaimedRow> = db.opt(&sql, &[&run_id, &project_id, &owner, &delivered_until(unix_now())]).await?;
    let Some(claimed) = claimed else { return Ok(None) };
    let (Some(epoch), Some(attempts), Some(definition_hash), cancel_requested, full_at, used, limit) = claimed else {
        anyhow::bail!("run {run_id} runs no program, so no worker can claim it");
    };
    let lost_full = weft_task_store::worker_door::memory_note_of(full_at, used, limit)?;
    Ok(Some(Claimed { epoch, attempts, definition_hash, cancel_requested: cancel_requested.map(|cause| cause.0), lost_full }))
}

/// The most rows one page of a record holds ([`read_page`]): enough that
/// a long run of small rows reads in few trips, few enough that a page
/// of them decoded stays small next to what a reader keeps.
pub const PAGE_ROWS: i64 = 512;

/// The most stored bytes one page of a record holds ([`read_page`]). One
/// row is one write, which can weigh as much as a whole batch of a
/// worker's writes (`weft_broker_client::protocol::JOURNAL_RECORD_BODY_LIMIT`),
/// so a page is cut by weight as well as by count; a single row heavier
/// than this is a page of its own.
pub const PAGE_BYTES: i64 = 8 * 1024 * 1024;

/// One page of the record of `run_id`: its rows after `after` (from its
/// first when `None`), in order, as many as fit in [`PAGE_ROWS`] and
/// [`PAGE_BYTES`], always one at least while any is left. Empty once
/// none is. Every reader of a whole record reads it page by page
/// ([`fold_pages`]), so none holds more than a page of it raw.
pub async fn read_page(db: &impl Client, run_id: RunId, after: Option<i32>) -> anyhow::Result<RawRecord> {
    read_rows_after(db, run_id, after, PAGE_ROWS).await
}

/// Which runs of `past` have a row after the one named beside them (any
/// row when `None`), in one query however many runs it names.
pub async fn written_past(db: &impl Client, past: &[(RunId, Option<i32>)]) -> anyhow::Result<Vec<RunId>> {
    let (runs, after): (Vec<RunId>, Vec<i32>) = past.iter().map(|(run, after)| (*run, after.unwrap_or(-1))).unzip();
    db.scalars(
        "SELECT w.run_id FROM unnest($1::uuid[], $2::int[]) AS w(run_id, after) \
         WHERE EXISTS (SELECT 1 FROM run_log l WHERE l.run_id = w.run_id AND l.seq > w.after)",
        &[&runs, &after],
    )
    .await
    .map_err(Into::into)
}

/// The first row of `run_id`'s record, which holds its birth first; `None`
/// for a run with no record.
pub async fn read_first_row(db: &impl Client, run_id: RunId) -> anyhow::Result<Option<RunLogRow>> {
    Ok(read_rows_after(db, run_id, None, 1).await?.rows.into_iter().next())
}

/// At most `rows` rows of `run_id`'s record after `after`, cut at
/// [`PAGE_BYTES`] ([`read_page`]).
async fn read_rows_after(db: &impl Client, run_id: RunId, after: Option<i32>, rows: i64) -> anyhow::Result<RawRecord> {
    // The weight is the row's stored size (`pg_column_size`, read
    // without fetching the value): the events are compressed already, so
    // it is what the row costs to carry.
    let rows: Vec<(i32, Vec<u8>, weft_core::stored::Version)> = db
        .all(
            "SELECT seq, events, events_version FROM ( \
                 SELECT seq, events, events_version, \
                        sum(pg_column_size(events)) OVER (ORDER BY seq) - pg_column_size(events) AS before \
                 FROM (SELECT seq, events, events_version FROM run_log \
                       WHERE run_id = $1 AND seq > $2 ORDER BY seq LIMIT $3) next \
             ) page WHERE before < $4 ORDER BY seq",
            &[&run_id, &after.unwrap_or(-1), &rows, &PAGE_BYTES],
        )
        .await?;
    Ok(RawRecord { rows: rows.into_iter().map(|(seq, events, events_version)| RunLogRow { seq, events, events_version }).collect() })
}

/// The rows of `run_id`'s record at `seqs`, raw, in order: those that
/// exist (a run erased has none). What a reader resolving the values an
/// event names reads (`weft_core::record_ref::ValueRef`): the rows that
/// keep them, and no other.
pub async fn read_rows(db: &impl Client, run_id: RunId, seqs: &[i32]) -> anyhow::Result<RawRecord> {
    let rows: Vec<(i32, Vec<u8>, weft_core::stored::Version)> = db
        .all("SELECT seq, events, events_version FROM run_log WHERE run_id = $1 AND seq = ANY($2) ORDER BY seq", &[&run_id, &seqs])
        .await?;
    Ok(RawRecord { rows: rows.into_iter().map(|(seq, events, events_version)| RunLogRow { seq, events, events_version }).collect() })
}

/// One page of a run's record, raw ([`RawRecord`]) or decoded (its
/// rows): what [`fold_pages`] walks a record by.
pub trait RecordPage: Send + 'static {
    /// The page's last row; `None` for the empty page past the record's end.
    fn last_seq(&self) -> Option<i32>;
}

impl RecordPage for RawRecord {
    fn last_seq(&self) -> Option<i32> {
        self.rows.last().map(|row| row.seq)
    }
}

impl RecordPage for Vec<JournalRow> {
    fn last_seq(&self) -> Option<i32> {
        self.last().map(|row| row.seq)
    }
}

/// Read a run's record after `after` page by page (`read` answers the
/// page after a row, as [`read_page`] does, raw or decoded), handing each
/// page to `step` with `state` on the blocking pool, and answer the state
/// once the record is read: whatever the record's length, a reader holds
/// one page of it plus what it keeps. A page `step` refuses ends the read
/// with its error. Every reader of a record page by page walks it here,
/// so how pages follow one another (the next one after the last row of
/// this one, an empty one past the end) lives once.
pub async fn fold_pages<S, Page, Read, Fut, Step>(mut read: Read, after: Option<i32>, mut state: S, step: Step) -> anyhow::Result<S>
where
    S: Send + 'static,
    Page: RecordPage,
    Read: FnMut(Option<i32>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Page>>,
    Step: Fn(&mut S, Page) -> anyhow::Result<()> + Clone + Send + 'static,
{
    let mut after = after;
    loop {
        let page = read(after).await?;
        let Some(last) = page.last_seq() else { return Ok(state) };
        let step = step.clone();
        state = crate::on_blocking_pool(move || step(&mut state, page).map(|()| state)).await?;
        after = Some(last);
    }
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

    fn run() -> RunId {
        RunId::from_u128(3)
    }

    /// A program a worker sends is kept only at today's version, of its
    /// own project, under its own hash.
    #[test]
    fn a_program_is_kept_only_at_todays_version_under_its_own_hash() {
        let project = uuid::Uuid::from_u128(9);
        let empty = serde_json::json!({ "id": project, "nodes": [], "edges": [] }).to_string();
        let fresh = StoredProgram::of(project, &serde_json::from_str(&empty).unwrap()).unwrap();
        fresh.check().expect("today's program under its own hash");
        let older = StoredProgram { project_json_version: ProjectDefinition::STORED.version() - 1, ..fresh.clone() };
        assert!(older.check().unwrap_err().to_string().contains("weft resync"));
        let mislabelled = StoredProgram { definition_hash: "the-hash-it-was-made-under".into(), ..fresh.clone() };
        assert!(mislabelled.check().unwrap_err().to_string().contains("hashes to"));
        let elsewhere = StoredProgram { project_id: uuid::Uuid::from_u128(10), ..fresh };
        assert!(elsewhere.check().unwrap_err().to_string().contains("is of project"));
    }

    /// An ending with no error and no cause carries neither key: the batch
    /// reads each key as the row's value, and a JSON `null` would land as
    /// a value no reader decodes.
    #[test]
    fn an_ending_without_a_cause_carries_no_cause() {
        let completed = Ending::of(&RunEvent::RunCompleted { run_id: run(), at_unix: 5 }).unwrap();
        let wire = serde_json::to_value(&completed).unwrap();
        assert_eq!(wire, serde_json::json!({ "at": 5, "outcome": "completed" }));
        assert_eq!(serde_json::from_value::<Ending>(wire).unwrap(), completed);
        let cancelled = Ending::of(&RunEvent::RunCancelled { run_id: run(), reason: "stopped".into(), cause: Some(CancelCause::User), at_unix: 6 }).unwrap();
        assert!(serde_json::to_value(&cancelled).unwrap().get("cancel_cause").is_some());
    }

    /// A row that does not decode fails the strict read whole, and the
    /// display read keeps every other row and names the bad one.
    #[test]
    fn a_bad_row_fails_the_strict_read_and_is_named_by_the_display_read() {
        let run_id = run();
        let completed = RunEvent::RunCompleted { run_id, at_unix: 2 };
        let record = RawRecord {
            rows: vec![
                RunLogRow { seq: 1, events: b"not zstd".to_vec(), events_version: crate::stored::STORED_EVENTS.version() },
                RunLogRow::of(2, std::slice::from_ref(&completed)),
            ],
        };
        assert!(record.decode(run_id).unwrap_err().contains(&run_id.to_string()));
        let (rows, bad) = record.decode_lossy(run_id);
        assert_eq!(rows.iter().map(|row| (row.seq, row.index)).collect::<Vec<_>>(), [(2, 0)]);
        assert!(matches!(rows[0].event, RunEvent::RunCompleted { at_unix: 2, .. }));
        assert_eq!(bad.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(), [1], "the bad row is named by its place");
    }

    /// A write's costs, skips, answers and ending are read off its events;
    /// a cost nobody could put a figure on is kept as unknown, and the
    /// first ending stands.
    #[test]
    fn a_write_says_what_its_events_say() {
        let run_id = run();
        let cost = |amount_usd| RunEvent::CostReported {
            run_id,
            node_id: "llm".into(),
            frames: Vec::new(),
            cost_id: "c".into(),
            service: "s".into(),
            model: None,
            amount_usd,
            billed: false,
            origin: weft_core::CredentialOwner::Platform,
            metadata: serde_json::Value::Null,
            charge: None,
            at_unix: 1,
        };
        let events = vec![
            cost(Some(0.25)),
            cost(None),
            RunEvent::NodeSkipped {
                run_id,
                node_id: "x".into(),
                frames: Vec::new(),
                reason: weft_core::run::skip::SkipReason::DidNotFlow,
                at_unix: 1,
            },
            RunEvent::SuspensionResolved { run_id, token: "t".into(), value: serde_json::Value::Null, at_unix: 2 },
            RunEvent::RunFailed { run_id, error: "boom".into(), at_unix: 3 },
            RunEvent::RunCompleted { run_id, at_unix: 4 },
        ];
        let written = Written::of(&events);
        assert_eq!(written.costs.iter().map(|cost| (cost.index, cost.amount_usd)).collect::<Vec<_>>(), [(0, Some(0.25)), (1, None)]);
        assert_eq!(written.skipped, 1);
        assert_eq!(written.resolved, vec!["t".to_string()]);
        assert_eq!(written.ended, Some(Ending { at: 3, outcome: Outcome::Failed, error: Some("boom".into()), cancel_cause: None }));
    }

    /// A record read raw decodes to its events with their places, and
    /// crosses the wire whole.
    #[test]
    fn a_raw_record_decodes_with_its_places() {
        let run_id = run();
        let birth = RunEvent::RunStarted {
            run_id,
            project_id: uuid::Uuid::nil(),
            entry_node: "door".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("d".into()),
            binary_hash: None, cut_from: None, given: Default::default(),
            source_version: None,
            run_kind: weft_core::run::RunKind::Project,
            seed: None,
            instance: None,
            instance_values: Default::default(),
            picks: Default::default(),
            stand_in: None, fired_trigger: None,
            settings: Default::default(),
            at_unix: 0,
        };
        let done = RunEvent::RunCompleted { run_id, at_unix: 1 };
        let record = RawRecord {
            rows: vec![
                RunLogRow::of(0, std::slice::from_ref(&birth)),
                RunLogRow::of(1, std::slice::from_ref(&done)),
            ],
        };
        let rows = record.decode(run_id).unwrap();
        assert_eq!(rows.iter().map(|row| (row.seq, row.index)).collect::<Vec<_>>(), vec![(0, 0), (1, 0)]);
        assert!(matches!(&rows[0].event, RunEvent::RunStarted { entry_node, .. } if entry_node == "door"));
        let wire = serde_json::to_string(&record).unwrap();
        assert_eq!(serde_json::from_str::<RawRecord>(&wire).unwrap(), record, "a record crosses the wire whole");
    }
}
