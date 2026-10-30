//! The `unanswered_call` table: weft's own work that keeps failing.
//!
//! When weft cannot do part of its own work (wake a role, hand a run to a
//! project's workers, carry on one of a role's loops, act on a project's
//! infra), the work behind it waits: an infra start nobody picks up, a run
//! nobody claims. The process that tried is not the one a person asks, so
//! each failure is written here, and `weft status` shows the ones still
//! current, with the error, for whoever is waiting to see why.
//!
//! A failure is written when it starts or when weft stops retrying it (a
//! role's loop: once its short retries ran out, and again on each failed
//! pass a write rings after that, so it shows the latest error), and
//! stays current until the thing it holds up is resolved:
//!
//! - a role's ring, until a ring goes through ([`answered`]);
//! - a role's loop that stopped trying, until its next drain goes through;
//! - a project's infra the supervisor gave up on, until the supervisor's
//!   next pass over that project goes through;
//! - a project's infra commands the supervisor gave up running, until
//!   the ending of a later command of the project is written;
//! - a project's work whose holder went away and that the dispatcher's
//!   look at lapsed holds could not repair, until a look repairs it
//!   ([`still_failing`]);
//! - a project's signals the listener kept serving once their trigger's
//!   door stopped taking work, until it lets go of them ([`still_failing`]);
//! - what the host still holds of removed projects that the supervisor
//!   could not delete, until a listing of the host that goes through
//!   holds none of those projects any more;
//! - stored files the bucket would not remove, until none is left;
//! - the install's news, which refused a batch past its few tries, until
//!   a look at the news waiting finds no refused news left
//!   (`crate::announce`);
//! - a project's workers, until a run of the project born after the
//!   failure reached a worker while none of its queued runs waits on them
//!   (read off the runs themselves, so a run handed over costs no write
//!   here; a queued run waits on them while it carries a failure of its
//!   delivery, [`delivery_failed_sql`]: answered busy, refused, or not
//!   reached), until a run its workers failed since its last claim
//!   (`run.waited_on_workers`, which its tries starting again leave set)
//!   leaves the queue (claimed, ended or erased) and leaves none waiting
//!   ([`delivery_waits_ended`]), or
//!   until the project's worker levers change, which tries its queued
//!   runs afresh.
//!
//! On an install that alerts ([`alert_after`]), the write that finds a
//! failure stuck also marks it alerted, and its writer logs it once the
//! write commits ([`Alerts`]), under
//! [`weft_platform_traits::ALERT_TARGET`], which the platform turns into
//! an alert (on Google Cloud, a log line its alert policy matches): a
//! failure weft gave up retrying ([`gave_up`]) at once, one it still
//! retries ([`failed`]) on the first write past the install's delay since
//! its stretch began. A failure that clears and comes back is a new
//! stretch, and alerts again.

use anyhow::Result;
use crate::db::{Client, Json, Pool};
use crate::drain::DB_NOW_MS;

pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "unanswered_call",
    tables: &["unanswered_call", "unanswered_alert"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS unanswered_call (
            -- What does not answer (`Callee::key`).
            callee TEXT PRIMARY KEY,
            -- What the failing work does, in a person's words, when the
            -- key alone does not say (a role's loop).
            label TEXT,
            -- The error, as the caller logged it.
            error TEXT NOT NULL,
            -- The first failure of this stretch and the last written, in
            -- unix milliseconds on the database's clock.
            since_ms BIGINT NOT NULL,
            last_ms BIGINT NOT NULL,
            -- When this stretch was logged as an alert (unix ms on the
            -- database's clock); NULL until it is.
            alerted_ms BIGINT
        )"#,
        r#"CREATE TABLE IF NOT EXISTS unanswered_alert (
            -- At most one row: the install alerts on its stuck failures
            -- (`alert_after`); no row, it does not.
            one BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
            -- How long a failure weft still retries is current before it
            -- counts as stuck, in milliseconds.
            after_ms BIGINT NOT NULL
        )"#,
    ],
    seed: &[],
    data_steps: &[],
};

/// What weft could not reach, or could not carry on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Callee<'a> {
    /// A role it rings awake, by `CoreRole::as_str`.
    Role(&'a str),
    /// A project's workers, a run being handed to them.
    Workers(uuid::Uuid),
    /// One of a role's loops (`crate::drain::DrainLoop`), whose pass failed.
    Loop { role: &'a str, name: &'a str },
    /// A project's infra, which the supervisor gave up acting on.
    Infra(uuid::Uuid),
    /// A project's infra commands, which the supervisor gave up running
    /// (given up by the broker's claim, cleared by the ending of a later
    /// command, [`commands_key_sql`]).
    Commands(uuid::Uuid),
    /// A project's work whose holder went away, which the dispatcher's
    /// look at lapsed holds could not repair.
    Repairs(uuid::Uuid),
    /// A project's signals the listener kept serving once their trigger's
    /// door stopped taking work.
    LetGo(uuid::Uuid),
    /// What the host still holds of removed projects, which the
    /// supervisor could not delete (written by the broker's ownership
    /// sync, [`gone_copies_key_sql`]).
    GoneCopies,
    /// Stored files the bucket would not remove.
    Reaps,
    /// The install's news (`weft_platform_traits::News`), which refused
    /// to carry a cancel or an answer to the run it is for.
    News,
}

/// A failure a look over every project writes down per project, rewriting
/// the whole set each time ([`still_failing`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerProject {
    /// [`Callee::Repairs`].
    Repairs,
    /// [`Callee::LetGo`].
    LetGo,
}

impl PerProject {
    // SYNC: the 'repairs:' and 'let-go:' prefixes <-> Callee::key, Failed::of_key
    fn prefix(self) -> &'static str {
        match self {
            PerProject::Repairs => "repairs:",
            PerProject::LetGo => "let-go:",
        }
    }
}

// SYNC: the 'workers:' prefix <-> Callee::key, Failed::of_key
const WORKERS_PREFIX: &str = "workers:";

impl Callee<'_> {
    pub(crate) fn key(self) -> String {
        match self {
            Callee::Role(role) => format!("role:{role}"),
            Callee::Workers(project) => format!("{WORKERS_PREFIX}{project}"),
            Callee::Loop { role, name } => format!("loop:{role}:{name}"),
            Callee::Infra(project) => format!("infra:{project}"),
            Callee::Commands(project) => format!("commands:{project}"),
            Callee::Repairs(project) => format!("{}{project}", PerProject::Repairs.prefix()),
            Callee::LetGo(project) => format!("{}{project}", PerProject::LetGo.prefix()),
            Callee::GoneCopies => "gone-copies:".to_string(),
            Callee::Reaps => "reaps:".to_string(),
            Callee::News => "news:".to_string(),
        }
    }

    /// The one project this holds up, `None` for what holds up every
    /// project. Its key ends with `:<project>` exactly when it holds one
    /// up ([`project_of_key`]).
    pub fn project(self) -> Option<uuid::Uuid> {
        match self {
            Callee::Workers(project)
            | Callee::Infra(project)
            | Callee::Commands(project)
            | Callee::Repairs(project)
            | Callee::LetGo(project) => Some(project),
            Callee::Role(_) | Callee::Loop { .. } | Callee::GoneCopies | Callee::Reaps | Callee::News => None,
        }
    }
}

/// The one project the failure written under `key` holds up, `None` for
/// what holds up every project: the project a key ends with
/// (`:<project>`, hyphenated, lower case), as [`project_part_sql`] reads it.
// SYNC: project_of_key <-> project_part_sql
fn project_of_key(key: &str) -> Option<uuid::Uuid> {
    let (_, tail) = key.rsplit_once(':')?;
    let spelled = tail.len() == 36 && tail.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || b == b'-');
    spelled.then(|| tail.parse().ok()).flatten()
}

/// SQL: the project the `unanswered_call.callee` expression `callee_expr`
/// ends with, as text (`:<project>`), NULL for a key that holds up every
/// project ([`project_of_key`]).
// SYNC: project_part_sql <-> project_of_key
fn project_part_sql(callee_expr: &str) -> String {
    format!("substring({callee_expr} from ':([0-9a-f-]{{36}})$')")
}

/// SQL: the `unanswered_call.callee` key of [`Callee::GoneCopies`].
// SYNC: the 'gone-copies:' key <-> Callee::key
pub fn gone_copies_key_sql() -> &'static str {
    "'gone-copies:'"
}

/// SQL: the `unanswered_call.callee` key of [`Callee::Infra`] for the
/// project uuid expression `project_expr`.
// SYNC: the 'infra:' prefix <-> Callee::key
pub fn infra_key_sql(project_expr: &str) -> String {
    format!("('infra:' || ({project_expr})::text)")
}

/// SQL: the `unanswered_call.callee` key of [`Callee::Commands`] for the
/// project uuid expression `project_expr`.
// SYNC: the 'commands:' prefix <-> Callee::key
pub fn commands_key_sql(project_expr: &str) -> String {
    format!("('commands:' || ({project_expr})::text)")
}

/// SQL: the `unanswered_call.callee` key of [`Callee::Loop`] for the role
/// and loop name expressions `role_expr` and `name_expr`.
// SYNC: the 'loop:' prefix <-> Callee::key
pub fn loop_key_sql(role_expr: &str, name_expr: &str) -> String {
    format!("('loop:' || ({role_expr}) || ':' || ({name_expr}))")
}

/// What a failure written down is about, as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failed {
    Role(String),
    Workers,
    Loop { role: String, name: String },
    Infra,
    Commands,
    Repairs,
    LetGo,
    GoneCopies,
    Reaps,
    News,
}

impl Failed {
    fn of_key(key: &str) -> Option<Self> {
        let (kind, rest) = key.split_once(':')?;
        Some(match kind {
            "role" => Failed::Role(rest.to_string()),
            "workers" => Failed::Workers,
            "loop" => {
                let (role, name) = rest.split_once(':')?;
                Failed::Loop { role: role.to_string(), name: name.to_string() }
            }
            "infra" => Failed::Infra,
            "commands" => Failed::Commands,
            "repairs" => Failed::Repairs,
            "let-go" => Failed::LetGo,
            "gone-copies" => Failed::GoneCopies,
            "reaps" => Failed::Reaps,
            "news" => Failed::News,
            _ => return None,
        })
    }

    /// What is failing, in a person's words, with `label` the row's own
    /// (`unanswered_call.label`): what `weft status` and the alert line
    /// say first.
    pub fn what(&self, label: Option<&str>) -> String {
        match self {
            Failed::Role(role) => match role.as_str() {
                "supervisor" => "weft's supervisor, which starts and stops infra, cannot be reached".to_string(),
                "dispatcher" => "weft's dispatcher, which hands runs to workers, cannot be reached".to_string(),
                role => format!("weft's {role} cannot be reached"),
            },
            Failed::Workers => "this project's workers, which runs are handed to, cannot be reached or cannot take them".to_string(),
            Failed::Loop { role, name } => format!("weft's {role} could not {}", label.unwrap_or(name)),
            Failed::Infra => "weft's supervisor could not get through this project's infra".to_string(),
            Failed::Commands => "weft's supervisor gave up infra commands of this project it could never finish".to_string(),
            Failed::Repairs => "weft could not repair work of this project whose holder went away".to_string(),
            Failed::LetGo => {
                "the listener still serves triggers of this project whose door stopped taking work, and what they pick up is refused".to_string()
            }
            Failed::GoneCopies => {
                "weft's supervisor could not delete what the infra host still holds of removed projects (their disks included)".to_string()
            }
            Failed::Reaps => "weft's broker could not remove stored files from the bucket".to_string(),
            Failed::News => {
                "weft could not tell the install's news (a cancel or an answer reaching a run a worker drives); those runs hear of it late"
                    .to_string()
            }
        }
    }

    /// What weft does about it next and what a person can do, in a
    /// person's words: what `weft status` and the alert line say last.
    pub fn then(&self) -> &'static str {
        match self {
            Failed::Role(_) => "weft rings it again on the next change that needs it",
            Failed::Workers => {
                "a queued run refused 3 times ends, and one its workers could not be reached for 3 times waits until a worker of the \
                 project comes up or a build of it registers; one they keep answering busy (which spends no try) is tried again \
                 after a pause that grows to a minute, for as long as they do; changing the project's worker settings (more memory, for a worker \
                 that stays busy) tries every queued run afresh"
            }
            Failed::Loop { .. } => {
                "weft tries it again a few times by itself, then stops; after that, the next change that concerns it tries it again, \
                 and so does the next start of that weft service"
            }
            Failed::Infra => "the error says whether it tries again by itself; `weft infra look` makes it look again",
            Failed::Commands => "issue the command again; this clears once the ending of a later infra command of the project is written",
            Failed::Repairs => {
                "it tries again twice within half a minute, then whenever it next looks at lapsed work; the error says what fixes it by hand"
            }
            Failed::LetGo => "weft tells the listener to let go of them again at the next change of a trigger",
            Failed::GoneCopies => {
                "the error says whether it tries again by itself; after that, every pass of the supervisor tries again, and \
                 `weft infra look` in any project starts one"
            }
            Failed::Reaps => "they are tried again at the expiry loop's next pass, a wipe of their files, or the tenant's next upload",
            Failed::News => "it is told again when news is next written, or when a weft process starts",
        }
    }
}

/// A failure still current, as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failing {
    pub what: Failed,
    pub label: Option<String>,
    pub error: String,
    pub since_ms: i64,
    pub last_ms: i64,
    /// The database's clock when this was read, which the times above are
    /// on: how long ago reads right whatever clock the reader has.
    pub as_of_ms: i64,
}

/// `callee` failed with `error`, and weft still retries it on its own;
/// `label` says what it does when its key does not (a role's loop). How
/// a row already there is kept and when the write alerts: [`write_sql`].
/// A NUL byte in `error` is dropped: Postgres text holds none, and
/// refusing the whole write over it would leave the failure unrecorded
/// every time. On an install that alerts, the first write past its delay
/// since the stretch began raises the alert: the answer holds it, for the
/// caller to log once the write commits ([`Alerts`]).
pub async fn failed(db: &impl Client, callee: Callee<'_>, label: Option<&str>, error: &str) -> Result<Alerts> {
    write(db, callee, label, error, false, std::time::Duration::ZERO).await
}

/// [`failed`], for a failure that had already lasted `lasted` when it is
/// first written: a stretch this write starts begins that long before it,
/// so the install's delay counts from when the failure began.
pub async fn failed_for(
    db: &impl Client,
    callee: Callee<'_>,
    label: Option<&str>,
    error: &str,
    lasted: std::time::Duration,
) -> Result<Alerts> {
    write(db, callee, label, error, false, lasted).await
}

/// [`failed`], for a failure weft stopped retrying on its own: only an
/// event that can fix it tries it again. On an install that alerts, the
/// first such write of a stretch raises the alert.
pub async fn gave_up(db: &impl Client, callee: Callee<'_>, label: Option<&str>, error: &str) -> Result<Alerts> {
    write(db, callee, label, error, true, std::time::Duration::ZERO).await
}

async fn write(
    db: &impl Client,
    callee: Callee<'_>,
    label: Option<&str>,
    error: &str,
    stopped: bool,
    lasted: std::time::Duration,
) -> Result<Alerts> {
    let alerted: Option<Json<Vec<Alerted>>> = db
        .scalar(
            &format!(
                "WITH written AS ({write}) SELECT {alerted}",
                write = write_sql("SELECT $1::text AS callee, $2::text AS label, $3::text AS error, $5::bigint AS lasted_ms", "$4::boolean"),
                alerted = alerted_sql("written"),
            ),
            &[&callee.key(), &label, &error, &stopped, &i64::try_from(lasted.as_millis()).unwrap_or(i64::MAX)],
        )
        .await?;
    Ok(Alerts::of(alerted))
}

/// SQL: the statement that writes down one failure per row of the query
/// `rows`, whose columns are `callee` (`Callee::key`), `label`, `error`
/// and `lasted_ms` (how long the failure
/// had lasted when first written, [`failed_for`]), with `stopped` the SQL
/// boolean of whether weft stopped retrying them ([`gave_up`]). The one
/// definition of how a failure is written, alert mark included: the
/// helpers above run it, and a statement that gives work up writes its
/// failures in its own round trip by running it as a data-modifying `WITH`
/// query, whose rows [`alerted_sql`] and [`Alerts::of`] turn into the alert.
///
/// A row already there keeps its label when the new one has none, and
/// keeps the start of its stretch, except a project's workers', whose
/// stretch ended once a run born after its last failure reached a worker
/// while no queued run still waits on them. On an install that alerts
/// (`unanswered_alert`), the write that finds the stretch stuck marks it
/// alerted: at once for a failure given up, past the install's delay since
/// the stretch began for one still retried.
pub fn write_sql(rows: &str, stopped: &str) -> String {
    let stretch_over = format!(
        "CASE WHEN starts_with(unanswered_call.callee, '{WORKERS_PREFIX}') THEN {} ELSE FALSE END",
        workers_through_sql(&format!("substr(unanswered_call.callee, {})::uuid", WORKERS_PREFIX.len() + 1), "unanswered_call.last_ms"),
    );
    // Whether the stretch that began at `since` is stuck at `now`.
    let stuck = |since: &str, now: &str| format!("EXISTS (SELECT 1 FROM unanswered_alert a WHERE {stopped} OR {now} - {since} >= a.after_ms)");
    // The row's last write, strictly later than the one before it: two
    // writes in the same millisecond, or one that waited on the row's lock
    // past a later one, never share or move back its time. The alert mark
    // takes this same time, so "this write raised the alert" is
    // `alerted_ms = last_ms`, true for exactly one write.
    let last = "GREATEST(EXCLUDED.last_ms, unanswered_call.last_ms + 1)";
    format!(
        "INSERT INTO unanswered_call (callee, label, error, since_ms, last_ms, alerted_ms) \
         SELECT r.callee, r.label, r.error, n.ms - r.lasted_ms, n.ms, CASE WHEN {stuck_new} THEN n.ms END \
         FROM ({rows}) r, (SELECT {DB_NOW_MS} AS ms) n \
         ON CONFLICT (callee) DO UPDATE SET label = COALESCE(EXCLUDED.label, unanswered_call.label), error = EXCLUDED.error, last_ms = {last}, \
             since_ms = CASE WHEN {stretch_over} THEN EXCLUDED.since_ms ELSE unanswered_call.since_ms END, \
             alerted_ms = CASE WHEN {stretch_over} THEN CASE WHEN EXCLUDED.alerted_ms IS NOT NULL THEN {last} END \
                 ELSE COALESCE(unanswered_call.alerted_ms, CASE WHEN {stuck_kept} THEN {last} END) END \
         RETURNING callee, label, error, since_ms, COALESCE(alerted_ms = last_ms, FALSE) AS alerted, ({stopped}) AS stopped",
        stuck_new = stuck("(n.ms - r.lasted_ms)", "n.ms"),
        stuck_kept = stuck("unanswered_call.since_ms", last),
    )
}

/// SQL: the failures the [`write_sql`] `WITH` query named `written` marked
/// alerted, as one JSON array ([`Alerted`]), NULL when it marked none.
pub fn alerted_sql(written: &str) -> String {
    format!(
        "(SELECT json_agg(json_build_object('callee', w.callee, 'label', w.label, 'error', w.error, \
             'since_ms', w.since_ms, 'stopped', w.stopped) ORDER BY w.callee) \
         FROM {written} w WHERE w.alerted)"
    )
}

/// A failure a write marked alerted, as [`alerted_sql`] returns it.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Alerted {
    callee: String,
    label: Option<String>,
    error: String,
    since_ms: i64,
    stopped: bool,
}

/// The failures a write marked alerted ([`alerted_sql`]), to log once the
/// write is durable ([`Alerts::log`]): a write rolled back never alerted,
/// and its failure alerts on the next write that commits. A caller writing
/// on the pool logs at once; one writing in a transaction logs after its
/// commit.
#[derive(Debug, Default)]
#[must_use = "log the alerts once the write that raised them commits"]
pub struct Alerts(Vec<Alerted>);

impl Alerts {
    /// The alerts of [`alerted_sql`]'s answer.
    pub fn of(alerted: Option<Json<Vec<Alerted>>>) -> Self {
        Self(alerted.map(|json| json.0).unwrap_or_default())
    }

    /// How many failures the write raised an alert for.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The write raised no alert.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// These alerts and `more`, for a transaction that wrote several.
    pub fn and(mut self, more: Alerts) -> Self {
        self.0.extend(more.0);
        self
    }

    /// Log every alert under [`weft_platform_traits::ALERT_TARGET`], in the
    /// words `weft status` says it with.
    pub fn log(self) {
        for alerted in &self.0 {
            alerted.log();
        }
    }
}

impl Alerted {
    fn log(&self) {
        let stopped = match self.stopped {
            true => "weft stopped retrying it",
            false => "weft keeps retrying it",
        };
        let Some(failed) = Failed::of_key(&self.callee) else {
            // A key no `Callee` writes: said as the key itself, so the
            // alert still goes out.
            tracing::error!(
                target: weft_platform_traits::ALERT_TARGET,
                callee = %self.callee,
                since_ms = self.since_ms,
                "weft's own work is stuck ({stopped}): {}: {}; `weft status` cannot read this key back",
                self.callee,
                self.error,
            );
            return;
        };
        let (what, then) = (failed.what(self.label.as_deref()), failed.then());
        tracing::error!(
            target: weft_platform_traits::ALERT_TARGET,
            callee = %self.callee,
            project = project_of_key(&self.callee).map(tracing::field::display),
            since_ms = self.since_ms,
            "weft's own work is stuck ({stopped}): {what}: {}; {then}; `weft status` shows it",
            self.error,
        );
    }
}

/// SQL: whether the queued run row named `run` (a table name or an alias)
/// still waits on its project's workers: they answered it busy
/// (`busy_since`), refused it (`refusals`) or could not be reached with it
/// (`unreached`), each counted only while it is queued
/// (`weft_dispatcher::delivery`). A claim clears all three, and so does a
/// change of the project's worker levers; its tries starting again (a
/// worker of the project coming alive, a build registering) clears
/// `unreached` and `busy_since`. The one definition of "this queued run
/// still waits on its workers". Whether a run leaving the queue carried
/// such a failure is a different question, which its tries starting
/// again must not forget: `run.waited_on_workers` answers it.
// SYNC: the delivery failure columns <-> crates/weft-dispatcher/src/delivery.rs (busy, refused, unreached, take, retry_stopped; the first three also set run.waited_on_workers)
pub fn delivery_failed_sql(run: &str) -> String {
    format!("({run}.busy_since IS NOT NULL OR {run}.unreached > 0 OR {run}.refusals > 0)")
}

/// SQL: whether a queued run of `project_expr` still waits on its
/// workers, carrying a failure of its delivery ([`delivery_failed_sql`]),
/// the run named by `leaving_expr` (its `run_id`) not counted.
fn workers_awaited_sql(project_expr: &str, leaving_expr: Option<&str>) -> String {
    let leaving = leaving_expr.map(|run| format!(" AND waiting.run_id <> {run}")).unwrap_or_default();
    format!(
        "EXISTS (SELECT 1 FROM run waiting WHERE waiting.project_id = {project_expr} AND waiting.state = 'queued'{leaving} AND {})",
        delivery_failed_sql("waiting"),
    )
}

/// SQL: whether the stretch of failures of `project_expr`'s workers last
/// written at `last_ms_expr` (unix ms) is over: a run of the project born
/// after it reached a worker, and no queued run of the project waits on
/// its workers ([`workers_awaited_sql`]).
fn workers_through_sql(project_expr: &str, last_ms_expr: &str) -> String {
    format!(
        "(EXISTS (SELECT 1 FROM run r WHERE r.project_id = {project_expr} \
             AND r.started_at * 1000 > {last_ms_expr} AND r.state <> 'queued') \
         AND NOT {awaited})",
        awaited = workers_awaited_sql(project_expr, None),
    )
}

/// The columns of the journal's `run` table (`weft_dispatcher`'s group)
/// a project's workers' failures are judged by ([`delivery_failed_sql`],
/// [`workers_through_sql`]), for a test whose schema has no journal: every
/// write of a failure reads them.
// SYNC: these columns <-> crates/weft-dispatcher/src/store/postgres.rs (run)
// Built in every build, hidden: the database tests of the crates above
// this one call it too, and a test-only switch here would rebuild them all.
#[doc(hidden)]
pub async fn judged_run_columns(pool: &Pool) -> Result<()> {
    pool.batch_execute(
        "CREATE TABLE run (run_id UUID PRIMARY KEY, project_id UUID NOT NULL, state TEXT NOT NULL, started_at BIGINT NOT NULL, \
         busy_since BIGINT, refusals SMALLINT NOT NULL DEFAULT 0, unreached SMALLINT NOT NULL DEFAULT 0)",
    )
    .await?;
    Ok(())
}

/// A queued run of `project` its workers failed since its last claim
/// (`run.waited_on_workers`) left the queue: a worker claimed it, it
/// ended, or it was erased. What is written down of the project's workers
/// is no longer shown, unless another queued run of the project still
/// waits on them ([`delivery_failed_sql`]). Called by whoever took the
/// run out of the queue, which read the mark off the row it locked.
pub async fn delivery_waits_ended(db: &impl Client, project: uuid::Uuid) -> Result<()> {
    db.execute(&delivery_waits_ended_sql("$1::uuid", None), &[&project]).await?;
    Ok(())
}

/// SQL: the statement of [`delivery_waits_ended`] for project
/// `project_expr` (a UUID). A writer that takes a run out of the queue
/// runs it in that same statement, as a data-modifying `WITH` query, so
/// both commit together; that statement reads the queue as it was before
/// it, where the run leaving still waits, so `leaving_expr` names it (its
/// `run_id`) and it is not counted.
pub fn delivery_waits_ended_sql(project_expr: &str, leaving_expr: Option<&str>) -> String {
    format!(
        "DELETE FROM unanswered_call WHERE callee = '{WORKERS_PREFIX}' || ({project_expr})::text AND NOT {}",
        workers_awaited_sql(project_expr, leaving_expr),
    )
}

/// Make the install alert on its stuck failures, with `after` the delay a
/// failure weft still retries is current before it counts as stuck, or
/// stop it alerting (`None`). The process that applies the install's
/// config writes it at its start.
pub async fn alert_after(pool: &Pool, after: Option<std::time::Duration>) -> Result<()> {
    match after {
        Some(after) => {
            pool.execute(
                "INSERT INTO unanswered_alert (one, after_ms) VALUES (TRUE, $1) \
                 ON CONFLICT (one) DO UPDATE SET after_ms = EXCLUDED.after_ms WHERE unanswered_alert.after_ms <> EXCLUDED.after_ms",
                &[&i64::try_from(after.as_millis()).unwrap_or(i64::MAX)],
            )
            .await?;
        }
        None => {
            pool.execute("DELETE FROM unanswered_alert", &[]).await?;
        }
    }
    Ok(())
}

/// What `callee` held up is resolved: it is no longer shown.
pub async fn answered(db: &impl Client, callee: Callee<'_>) -> Result<()> {
    db.execute("DELETE FROM unanswered_call WHERE callee = $1", &[&callee.key()]).await?;
    Ok(())
}

/// `project` is removed: nothing of it waits any more, so nothing written
/// down of its own work is shown again: every key that holds it up
/// ([`Callee::project`]).
pub async fn forget_project(db: &impl Client, project: uuid::Uuid) -> Result<()> {
    db.execute(&format!("DELETE FROM unanswered_call WHERE {} = $1::uuid::text", project_part_sql("callee")), &[&project]).await?;
    Ok(())
}

/// A look over every project found `kind` failing for `failing` only:
/// what was written down of that kind for the others is no longer shown.
pub async fn still_failing(pool: &Pool, kind: PerProject, failing: &[uuid::Uuid]) -> Result<()> {
    let kept: Vec<String> = failing.iter().map(|project| format!("{}{project}", kind.prefix())).collect();
    pool.execute("DELETE FROM unanswered_call WHERE starts_with(callee, $1) AND NOT (callee = ANY($2))", &[&kind.prefix(), &kept])
        .await?;
    Ok(())
}

/// Every role whose ring is written down as not answered, however long
/// ago: a process that gave up ringing it (it was stopped) left the row,
/// and the work the ring was for still waits, so a process that starts
/// rings these again.
pub async fn unanswered_roles(pool: &Pool) -> Result<Vec<String>> {
    let keys: Vec<String> = pool.scalars("SELECT callee FROM unanswered_call WHERE callee LIKE 'role:%' ORDER BY callee", &[]).await?;
    Ok(keys.into_iter().filter_map(|key| key.strip_prefix("role:").map(str::to_string)).collect())
}

/// Every failure still current that `project`'s work may wait on: every
/// one that holds up every project (a role not answering, a loop failing,
/// what the host holds of removed projects that could not be deleted, the
/// stored files the bucket would not remove, the install's news refusing),
/// and every one that holds up this project ([`Callee::project`]): its
/// workers while their stretch is not over, its infra, infra commands,
/// repairs and signals kept past their door's window.
pub async fn failing_for(pool: &Pool, project: uuid::Uuid) -> Result<Vec<Failing>> {
    let rows: Vec<(String, Option<String>, String, i64, i64, i64)> = pool
        .all(
            &format!(
                "SELECT u.callee, u.label, u.error, u.since_ms, u.last_ms, {DB_NOW_MS} FROM unanswered_call u \
                 WHERE {part} IS NULL \
                    OR ({part} = $2::uuid::text AND (u.callee <> $1 OR NOT {workers_through})) \
                 ORDER BY u.callee",
                part = project_part_sql("u.callee"),
                workers_through = workers_through_sql("$2", "u.last_ms"),
            ),
            &[&Callee::Workers(project).key(), &project],
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(callee, label, error, since_ms, last_ms, as_of_ms)| {
            Some(Failing { what: Failed::of_key(&callee)?, label, error, since_ms, last_ms, as_of_ms })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_key_reads_back_as_what_it_was_written_for() {
        let project = uuid::Uuid::from_u128(7);
        assert_eq!(Failed::of_key(&Callee::Role("supervisor").key()), Some(Failed::Role("supervisor".into())));
        assert_eq!(Failed::of_key(&Callee::Workers(project).key()), Some(Failed::Workers));
        assert_eq!(Failed::of_key(&Callee::Infra(project).key()), Some(Failed::Infra));
        assert_eq!(Failed::of_key(&Callee::Repairs(project).key()), Some(Failed::Repairs));
        assert_eq!(Failed::of_key(&Callee::Commands(project).key()), Some(Failed::Commands));
        assert_eq!(Failed::of_key(&Callee::LetGo(project).key()), Some(Failed::LetGo));
        assert_eq!(Failed::of_key(&Callee::GoneCopies.key()), Some(Failed::GoneCopies));
        assert_eq!(Failed::of_key(gone_copies_key_sql().trim_matches('\'')), Some(Failed::GoneCopies));
        assert_eq!(Failed::of_key(&Callee::Reaps.key()), Some(Failed::Reaps));
        assert_eq!(Failed::of_key(&Callee::News.key()), Some(Failed::News));
        assert_eq!(
            Failed::of_key(&Callee::Loop { role: "dispatcher", name: "delivery" }.key()),
            Some(Failed::Loop { role: "dispatcher".into(), name: "delivery".into() })
        );
        assert_eq!(Failed::of_key("nonsense"), None);
    }

    /// Every key gives back exactly the project its callee holds up, which
    /// is what the alert line names, what `weft status` shows a project,
    /// and what a project's removal forgets.
    #[test]
    fn every_key_gives_back_the_project_it_holds_up() {
        let project = uuid::Uuid::from_u128(0xabcdef);
        let every = [
            Callee::Role("supervisor"),
            Callee::Workers(project),
            Callee::Loop { role: "dispatcher", name: "delivery" },
            Callee::Infra(project),
            Callee::Commands(project),
            Callee::Repairs(project),
            Callee::LetGo(project),
            Callee::GoneCopies,
            Callee::Reaps,
            Callee::News,
        ];
        // A variant added to `Callee` fails to build here: list it above
        // as this match is extended.
        for callee in every {
            match callee {
                Callee::Role(_)
                | Callee::Workers(_)
                | Callee::Loop { .. }
                | Callee::Infra(_)
                | Callee::Commands(_)
                | Callee::Repairs(_)
                | Callee::LetGo(_)
                | Callee::GoneCopies
                | Callee::Reaps
                | Callee::News => {}
            }
            assert_eq!(project_of_key(&callee.key()), callee.project(), "{callee:?}");
        }
        assert_eq!(project_of_key(&format!("workers:{}", project.simple())), None, "only the hyphenated spelling a key is written in");
    }

    /// A loop's words name what it does when its row says, and its own
    /// name otherwise.
    #[test]
    fn a_loop_is_said_by_its_label_when_it_has_one() {
        let failed = Failed::Loop { role: "dispatcher".into(), name: "delivery".into() };
        assert_eq!(failed.what(Some("hand queued runs to workers")), "weft's dispatcher could not hand queued runs to workers");
        assert_eq!(failed.what(None), "weft's dispatcher could not delivery");
    }
}
