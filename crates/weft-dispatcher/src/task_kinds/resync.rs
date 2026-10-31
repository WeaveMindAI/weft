//! `resync` task: one resync's take-down and bring-back, carried out
//! apart from the request that asked for it.
//!
//! `weft resync` takes a project's triggers that are on and behind the
//! registered build down with the person's answers, lets the runs going
//! on the old version finish (or cancels them, `--running cancel`), and
//! only then brings them back on the new one, so nothing on the old
//! version is cut off when the new one takes over (locally, takes the
//! project's port from the old worker). Between
//! the two halves those triggers are off, so the work must not stop
//! halfway whatever happens to the request (its caller goes away, a
//! process restarts) or to one step (a database blip, a worker that could
//! not be reached). The request checks everything it can refuse for
//! first, then writes the resync down on the project row
//! (`project.resync`, a [`ResyncPlan`]: for each owner, the triggers that
//! were on), enters the project's `resyncing` transition and enqueues this
//! task in one transaction, and follows the task to its end
//! (`crate::api::project::resync`).
//!
//! The task takes every owner's triggers down
//! (`crate::api::project::resync_take_down_owner`), waits for the old
//! version ([`let_the_old_version_finish`]), then brings them back owner
//! by owner (`crate::api::project::resync_bring_back_owner`). It writes
//! down which triggers it takes down, and the builds they fired, before it
//! takes them down, brings back exactly those, writes down that the old
//! version is done (`switching`), and writes each owner it brought back
//! into the plan, so a task run again
//! (its process died, the claim lapsed and another claimed it) carries on
//! where the last stopped, and a trigger that was off, or that went down
//! some other way meanwhile, is never turned on by it; an owner's
//! reactivation it had claimed and not landed is carried on from its rows
//! (`crate::api::project::activate_with`). While a reactivation's setup
//! run waits (on a person, for days), the task's claim rests on that run
//! (`crate::api::project::run_trigger_setup`) and is handed back once it
//! ends (`crate::lapses`).
//!
//! Every drive of a plan has its own id ([`ResyncPlan::driver`]), carried
//! by its task, and every write of the plan is fenced on it: a task of an
//! earlier drive, still finishing a step after the plan was given up or
//! taken up again, writes nothing and turns nothing on.
//!
//! While the project holds the transition, no build registers
//! (`crate::build::waiting`) and no other activation of its triggers
//! starts (`crate::activation_store`).
//!
//! A step that fails is tried again a couple of times, a little later each
//! time; then the resync stops, with why written into its plan for `weft
//! status`, and the plan keeps holding the transition. A step that failed
//! because the install's database was busy or out of reach spends none of
//! those tries: it waits for the database a while longer
//! ([`DATABASE_WAITS`]), then stops waiting on it ([`ResyncWait::Database`]),
//! and the stop is taken up again once the database answers. A step refused
//! because the build cannot run the triggers it brings back (a part they
//! need does not compile) stops it at once: every try, and every resume,
//! runs that same build ([`Stopped::build_cannot_run`]). A stop that waits
//! on something that comes back by itself (the infra its triggers read, an
//! activation still landing) is taken up again when it does
//! ([`retry_stopped`]); any stop is taken up again by `weft resync
//! --resume`; `weft cancel-activate` gives it up, leaving the triggers it
//! had not brought back down, and says which.

use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::activation::{ActivationKey, ActivationScope, OwnerTriggers};
use weft_core::instance::Owner;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::api::project::Unrunnable;
use crate::state::DispatcherState;

/// How long a resync waits before it tries a step that failed again, after
/// each failure but the last, which stops it.
const RETRY_WAITS: [u64; 2] = [5, 20];

/// How long a resync waits before it tries again a step that failed
/// because the install's database was busy or out of reach, after each
/// such failure but the last, which stops it waiting on the database
/// ([`ResyncWait::Database`]). Kept apart from [`RETRY_WAITS`]: the step
/// did not fail, the database did not take it, and a busy database is
/// waited out, which takes longer than a blip.
const DATABASE_WAITS: [u64; 4] = [5, 15, 30, 60];

/// How long a caller that waits on the database with no stop to fall back
/// on (a request setting triggers up again after a pick changed,
/// `crate::install_picks`) waits before trying again for the `waited`th
/// time: the resync's own waits ([`DATABASE_WAITS`]), the last repeated for
/// as long as the database does not take it.
pub(crate) fn database_wait(waited: usize) -> std::time::Duration {
    let secs = DATABASE_WAITS.get(waited).or(DATABASE_WAITS.last()).copied().expect("the database waits are not empty");
    weft_core::time_scale::scaled(std::time::Duration::from_secs(secs))
}

/// How long after a resync stopped waiting on the database it is taken up
/// again ([`retry_stopped`]): long enough for the load that refused it to
/// pass, so taking it up again does not land on the same crowd.
const DATABASE_LOOK_AGAIN: std::time::Duration = std::time::Duration::from_secs(30);

/// The steps a stored [`ResyncPlan`] takes from the shape an older weft
/// wrote; `weft_core::stored` says how they run.
pub const RESYNC_UPGRADES: &[&[weft_core::stored::Op]] = &[
    // Version 2: the drive moved to its own column (`project.resync_driver`,
    // filled by `crate::project_store::RESYNC_FILLS`).
    &[weft_core::stored::Op::DropKey { at: "", key: "driver" }],
];

impl ResyncPlan {
    /// A resync as the project row keeps it (`project.resync`).
    pub const STORED: weft_core::stored::Kind<ResyncPlan> = weft_core::stored::Kind::new("resync plan", RESYNC_UPGRADES);

    /// The plan project `project's row holds: `resync`, written at
    /// `version`, and its drive from `resync_driver`.
    pub fn stored(
        project: uuid::Uuid,
        json: impl weft_core::stored::Raw,
        version: weft_core::stored::Version,
        driver: Option<uuid::Uuid>,
    ) -> anyhow::Result<ResyncPlan> {
        let mut plan = Self::STORED.decode(json, version).map_err(|e| e.at(format_args!("project {project}'s resync")))?;
        plan.driver = driver
            .ok_or_else(|| anyhow::anyhow!("project {project} holds a resync with no drive (project.resync_driver is NULL)"))?;
        Ok(plan)
    }

    /// The columns of a project row that hold a plan, as a write sets
    /// them: `$2` its JSON, `$3` the version it is written at, `$4` its
    /// drive. Every write of a plan sets the three together, through
    /// [`Self::row_write`].
    pub const ROW: &'static str = "resync = $2, resync_version = $3, resync_driver = $4";

    /// Write this plan to project `project`'s row, for how many rows it
    /// touched: `sql`, which sets [`Self::ROW`] and names the project as
    /// `$1`, with the plan's JSON, its version and its drive given, and
    /// `more` as `$5` onward.
    pub async fn row_write(
        &self,
        db: &impl weft_task_store::db::Client,
        project: uuid::Uuid,
        sql: &str,
        more: &[weft_task_store::db::Param<'_>],
    ) -> weft_task_store::db::Result<u64> {
        let json = weft_task_store::db::JsonText(self);
        let version = Self::STORED.version();
        let row: [weft_task_store::db::Param<'_>; 4] = [&project, &json, &version, &self.driver];
        let params: Vec<_> = row.into_iter().chain(more.iter().copied()).collect();
        db.execute(sql, &params).await
    }
}

/// A resync under way, as the project row keeps it (`project.resync`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResyncPlan {
    /// The drive under way: minted when the resync begins and each time a
    /// stopped one is taken up again, carried by the task driving it, and
    /// every write of the plan names it. The row keeps it in a column of
    /// its own (`project.resync_driver`), which the store reads it from.
    #[serde(skip)]
    pub driver: uuid::Uuid,
    /// Whose triggers, one owner each, in the order they are resynced.
    pub owners: Vec<ResyncOwner>,
    /// How they go down for the swap.
    pub take_down: weft_core::take_down::TakeDown,
    /// The build it brings live, as the person named it.
    pub build: weft_core::builds::BuildHashes,
    /// The owners brought back so far, the first of `owners` in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resynced: Vec<Owner>,
    /// The addresses their reactivations answer on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<weft_core::activation::ActivationUrl>,
    /// What its take-downs found held on the triggers, kept or dropped.
    #[serde(default)]
    pub held: weft_core::projects::HeldWork,
    /// The builds the triggers it takes down fired, other than the new
    /// one: the old version, whose runs going it waits for (or cancels)
    /// before the new one takes over. Written with the triggers, before
    /// they come down.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub old_builds: Vec<String>,
    /// The entry tokens of the triggers it takes down, read before they
    /// come down: what the wait for the runs they fired counts at the
    /// workers' door (`crate::drain::Reaching::Fired`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub old_tokens: Vec<String>,
    /// Every owner is down and the old version has nothing left running:
    /// what is left is bringing them back.
    #[serde(default)]
    pub switching: bool,
    /// Runs going on the old version when every owner was down (fired by
    /// the triggers it took down, or on `old_builds`): waited for, or
    /// cancelled.
    #[serde(default)]
    pub runs: u64,
    /// The paused runs it moved onto the new build.
    #[serde(default)]
    pub moved_runs: u64,
    /// The triggers it was asked about that were off, which it never
    /// turns on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub off: Vec<OwnerTriggers>,
    /// Why it stopped, once its tries ran out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<Stopped>,
    /// A person gave it up (`crate::project_store::ProjectStoreOps::give_up_resync`),
    /// or weft did, its plan no longer reading
    /// (`crate::project_store::ProjectStoreOps::give_up_unreadable_resync`):
    /// no drive of it goes on, and this says when and what it left down.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub given_up: Option<GivenUp>,
}

/// A resync a person gave up: when, and the triggers it had taken down
/// and not brought back at that moment ([`left_down`]). A trigger leaves
/// `left_down` once someone activates or takes it down
/// (`crate::activation_store`'s writes, in their transaction), and `weft
/// status` shows the rest that the program still has until the next
/// resync begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GivenUp {
    pub at_unix: i64,
    pub left_down: Vec<OwnerTriggers>,
    /// Set when weft gave it up because its plan no longer read: what
    /// it left down is then not known, and `left_down` is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unreadable: Option<UnreadablePlan>,
}

/// A resync's plan as the project row held it when it no longer read,
/// kept on the record that gave it up: why it did not read, and the
/// document itself, which is the only account left of the triggers it
/// had taken down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnreadablePlan {
    pub error: String,
    pub document: Value,
    pub version: weft_core::stored::Version,
}

impl ResyncPlan {
    /// The record that takes the place of a plan that no longer reads
    /// (`unreadable`): given up at `at_unix`, under a drive of its own,
    /// with nothing of the plan's (none of it read).
    pub fn given_up_unreadable(unreadable: UnreadablePlan, at_unix: i64) -> Self {
        ResyncPlan {
            driver: uuid::Uuid::now_v7(),
            owners: Vec::new(),
            take_down: weft_core::take_down::TakeDown::DEFAULT,
            build: Default::default(),
            resynced: Vec::new(),
            urls: Vec::new(),
            held: Default::default(),
            old_builds: Vec::new(),
            old_tokens: Vec::new(),
            switching: false,
            runs: 0,
            moved_runs: 0,
            off: Vec::new(),
            stopped: None,
            given_up: Some(GivenUp { at_unix, left_down: Vec::new(), unreadable: Some(unreadable) }),
        }
    }
}

impl GivenUp {
    /// Drop `keys` from what it left down, owners with none left out:
    /// someone decided about them since. Answers whether any was there.
    pub fn forget(&mut self, keys: &[ActivationKey]) -> bool {
        let before: usize = self.left_down.iter().map(|owner| owner.triggers.len()).sum();
        for owner in &mut self.left_down {
            owner.triggers.retain(|trigger| !keys.iter().any(|key| key.trigger == *trigger && key.owner == owner.owner));
        }
        self.left_down.retain(|owner| !owner.triggers.is_empty());
        self.left_down.iter().map(|owner| owner.triggers.len()).sum::<usize>() < before
    }

    /// Of the triggers it left down, the ones `project` still has for
    /// their owner: the ones `weft activate` can turn on.
    pub fn still_down(&self, project: &weft_core::project::ProjectDefinition) -> Vec<OwnerTriggers> {
        self.left_down
            .iter()
            .filter_map(|owner| {
                let kept: Vec<String> = owner
                    .triggers
                    .iter()
                    .filter(|trigger| ActivationScope::of_owner(vec![(*trigger).clone()], &owner.owner).resolve(project, &[]).is_ok())
                    .cloned()
                    .collect();
                (!kept.is_empty()).then(|| OwnerTriggers { owner: owner.owner.clone(), triggers: kept })
            })
            .collect()
    }
}

/// One owner's triggers a resync takes down and brings back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResyncOwner {
    pub owner: Owner,
    /// Its triggers that were on and behind when the resync began: the
    /// only ones it takes down.
    pub on: Vec<String>,
    /// The ones of `on` it took down, written down before it takes them
    /// down: the ones it brings back.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub down: Vec<String>,
}

impl ResyncOwner {
    /// The activations of `triggers` for this owner.
    pub fn keys(&self, triggers: &[String]) -> Vec<ActivationKey> {
        triggers.iter().map(|trigger| ActivationKey::new(trigger.clone(), self.owner.clone())).collect()
    }

    /// The scope naming `triggers` of this owner.
    pub fn scope(&self, triggers: &[String]) -> ActivationScope {
        ActivationScope::of_owner(triggers.to_vec(), &self.owner)
    }
}

impl ResyncPlan {
    /// The owners not brought back yet.
    pub fn remaining(&self) -> &[ResyncOwner] {
        self.owners.get(self.resynced.len()..).unwrap_or_default()
    }

    /// What its task answers once every owner is back.
    fn answer(self) -> weft_core::deactivation::ResyncResponse {
        let keys: Vec<ActivationKey> = self.owners.iter().flat_map(|owner| owner.keys(&owner.down)).collect();
        let report = crate::take_down::Taken { keys, held: self.held, runs: self.runs, commands: Vec::new(), tokens: Vec::new() }.report(self.take_down);
        weft_core::deactivation::ResyncResponse {
            report,
            urls: self.urls,
            resynced: self.resynced,
            moved_runs: self.moved_runs,
            off: self.off,
            brought_back: None,
        }
    }
}

/// Why a resync stopped with triggers still down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stopped {
    /// The HTTP status its last failure answered with.
    pub status: u16,
    pub error: String,
    /// When it stopped (unix seconds).
    pub at_unix: i64,
    /// What it waits on that comes back by itself, when its failure was
    /// one: it is taken up again then ([`retry_stopped`]). `None`: only a
    /// person takes it up again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waits_for: Option<ResyncWait>,
    /// It stopped because its build cannot run the triggers it brings
    /// back (a part they need does not compile). Taking it up again keeps
    /// that build, so `weft resync --resume` is refused, and the person is
    /// told what does bring them back ([`what_next`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub build_cannot_run: bool,
}

/// What a person is told comes next for a resync that stopped (`stopped`,
/// or none recorded: its task ended without writing why): what takes it
/// up again by itself, and what they can do. One sentence, lower case,
/// for `weft status` and for the refusal the person following it reads.
pub fn what_next(stopped: Option<&Stopped>) -> String {
    let Some(stopped) = stopped else { return TAKE_UP_OR_GIVE_UP.to_string() };
    if stopped.build_cannot_run {
        return BUILD_CANNOT_RUN_NEXT.to_string();
    }
    let by_itself = match stopped.waits_for {
        Some(ResyncWait::Infra) => "it goes on by itself once the infra those triggers read runs; ",
        Some(ResyncWait::Activation) => "it goes on by itself once that activation has landed; ",
        Some(ResyncWait::Database) => "it goes on by itself shortly, once the install's database answers again; ",
        None => "",
    };
    format!("{by_itself}{TAKE_UP_OR_GIVE_UP}")
}

const TAKE_UP_OR_GIVE_UP: &str =
    "`weft resync --resume` tries it again where it stopped; `weft cancel-activate` gives it up and leaves those triggers down";

/// What comes next for a resync whose build cannot run its triggers: a
/// resume keeps that build, and no build registers while the resync holds
/// the project, so it is given up and the fixed source turned on.
pub const BUILD_CANNOT_RUN_NEXT: &str =
    "`weft resync --resume` would take it up again on the same build and fail the same way. Fix the source as the error says, \
     then run `weft cancel-activate`, which gives the resync up and names the triggers it leaves down, and `weft activate`, \
     which builds the fixed source and turns them back on";

/// What a stopped resync waits on that comes back by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResyncWait {
    /// The infra the triggers it brings back read is not running.
    Infra,
    /// An activation of its triggers is still landing.
    Activation,
    /// The install's database was too busy to take a step, or out of
    /// reach (`weft_task_store::db::busy`, `weft_task_store::db::unreachable`).
    Database,
}

/// The resync a project holds, and whether a task drives it now.
#[derive(Debug, Clone, PartialEq)]
pub struct ResyncState {
    pub plan: ResyncPlan,
    pub driven: bool,
    /// Where the task driving it last said it is (nothing while none
    /// drives it, or before it says).
    pub progress: weft_core::deactivation::ResyncProgress,
}

impl ResyncState {
    /// Why it stands still with nothing driving it: its stop, or, when it
    /// recorded none (its task ended without writing it), that.
    pub fn standing_still(&self) -> Option<String> {
        if self.driven {
            return None;
        }
        Some(match &self.plan.stopped {
            Some(stopped) => stopped.error.clone(),
            None => "its task ended without recording why; the dispatcher's log has it".to_string(),
        })
    }

    /// How `weft status` shows it: why it stands still and what comes
    /// next, or where its task last said it is.
    pub fn standing(&self) -> weft_core::projects::ResyncStanding {
        weft_core::projects::ResyncStanding {
            stopped: self
                .standing_still()
                .map(|error| weft_core::projects::ResyncStop { error, then: what_next(self.plan.stopped.as_ref()) }),
            progress: self.progress.clone(),
        }
    }
}

/// The task that drives `project_id`'s resync under `driver`: one at a
/// time per drive.
pub fn task(tenant: &str, project_id: uuid::Uuid, driver: uuid::Uuid) -> weft_task_store::tasks::NewTask {
    weft_task_store::tasks::NewTask {
        kind: weft_task_store::TaskKind::Resync,
        project_id: Some(project_id),
        dedup_key: Some(driver.to_string()),
        run_id: None,
        tenant_id: tenant.to_string(),
        payload: serde_json::to_value(weft_task_store::ResyncPayload { driver }).expect("a resync payload serializes"),
    }
}


pub struct ResyncExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for ResyncExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let id = task.project_id.ok_or_else(|| anyhow::anyhow!("a resync task names no project"))?;
        let weft_task_store::ResyncPayload { driver } = serde_json::from_value(task.payload.clone())?;
        let mut tries = Tries::default();
        let progress = TaskProgress::new(state.pg_pool.clone(), task.id);
        loop {
            match advance(state, id, driver, &progress).await {
                Ok(done) => return Ok(serde_json::to_value(done)?),
                Err(Halt::Given) => {
                    anyhow::bail!(
                        "the resync was stopped with `weft cancel-activate`; the triggers it had not brought back stay down"
                    )
                }
                Err(Halt::Removing) => {
                    anyhow::bail!("the resync stood down: {}", weft_core::projects::PROJECT_BEING_REMOVED)
                }
                Err(Halt::Failed(failure)) => {
                    if let Some(wait) = tries.wait_before_trying_again(&failure) {
                        tracing::warn!(target: "weft_dispatcher::resync", project_id = %id, error = %failure.error, "a resync step failed; trying it again");
                        tokio::time::sleep(weft_core::time_scale::scaled(std::time::Duration::from_secs(wait))).await;
                        continue;
                    }
                    // Once it stops, this drive is over: the row says
                    // `stopped` from the first try at the stop on, so a
                    // resume may start another drive at any moment, and
                    // this one must never drive again beside it.
                    stop_until_taken(id, || stop(state, id, driver, &failure)).await?;
                    anyhow::bail!("{}", failure.error)
                }
            }
        }
    }
}

/// Run `stop` until it lands. Busy means wait: a stop nobody could read
/// would leave the resync standing with no why, and one whose look was
/// not booked would never be taken up, so a stop the database did not take
/// waits on the database and is tried again (writing the plan again is
/// harmless). Only the stop is tried again, never the drive. Any other
/// failure of the stop is the answer.
async fn stop_until_taken<F, Fut>(id: uuid::Uuid, mut stop: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    loop {
        match stop().await {
            Err(e) if crate::api::database_did_not_take(&e) => {
                tracing::warn!(
                    target: "weft_dispatcher::resync",
                    project_id = %id, error = %format!("{e:#}"),
                    "the database did not take the resync's stop; waiting on it and trying again"
                );
                tokio::time::sleep(weft_core::time_scale::scaled(DATABASE_LOOK_AGAIN)).await;
            }
            stopped => return stopped,
        }
    }
}

/// The waits a drive of a resync has left before it stops: one count for
/// steps that failed, one for steps the database did not take.
#[derive(Debug, Default)]
struct Tries {
    failed: usize,
    database: usize,
}

impl Tries {
    /// How many seconds to wait before trying `failure`'s step again;
    /// `None` when it is not tried again: the waits of its kind ran out
    /// ([`RETRY_WAITS`], or [`DATABASE_WAITS`] for a step the database did
    /// not take, which spends none of the others), or its build cannot run
    /// the triggers, which fails the same way on every try.
    fn wait_before_trying_again(&mut self, failure: &Failure) -> Option<u64> {
        if failure.build_cannot_run {
            return None;
        }
        let (waits, spent): (&[u64], &mut usize) = match failure.waits_for {
            Some(ResyncWait::Database) => (&DATABASE_WAITS, &mut self.database),
            _ => (&RETRY_WAITS, &mut self.failed),
        };
        let wait = waits.get(*spent).copied()?;
        *spent += 1;
        Some(wait)
    }
}

/// Why a resync did not finish ([`advance`], one owner's
/// `crate::api::project::resync_take_down_owner`, `resync_bring_back_owner`).
pub(crate) enum Halt {
    /// A person gave it up, or took it up again under another drive: this
    /// drive's plan is gone.
    Given,
    /// `weft rm` marked the project: what the resync had not brought back
    /// is the removal's to take down.
    Removing,
    /// A step failed.
    Failed(Failure),
}

/// A resync step that failed: the status it answered, why, and what it
/// waits on when that comes back by itself.
pub(crate) struct Failure {
    pub status: StatusCode,
    pub error: String,
    pub waits_for: Option<ResyncWait>,
    /// See [`Stopped::build_cannot_run`]: never tried again.
    pub build_cannot_run: bool,
}

impl Halt {
    /// A step that failed waiting on `wait`.
    pub(crate) fn waiting(wait: ResyncWait, (status, error): (StatusCode, String)) -> Self {
        Halt::Failed(Failure { status, error, waits_for: Some(wait), build_cannot_run: false })
    }

    /// A step that failed doing `what` with `e`: one that waits on the
    /// database when the database was busy or out of reach
    /// (`crate::api::database_did_not_take`), an ordinary failure otherwise.
    pub(crate) fn failed(what: &str, e: anyhow::Error) -> Self {
        if crate::api::database_did_not_take(&e) {
            let error = format!("{what}: the install's database was busy or out of reach ({e:#})");
            return Halt::on_the_database(error);
        }
        Halt::from((StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}")))
    }

    /// A step the database did not take, saying `error`: it waits on the
    /// database. Its status is a 500, never a 503: what a resync stopped
    /// with is the answer to whoever follows it, the same on every look,
    /// and a 503 says the same request goes through later
    /// (`weft_core::install::asks_again`), which would keep a follower
    /// asking a stop that never changes.
    pub(crate) fn on_the_database(error: String) -> Self {
        Halt::waiting(ResyncWait::Database, (StatusCode::INTERNAL_SERVER_ERROR, error))
    }
}

/// A step refused because the resync's build cannot run the triggers it
/// brings back stops it for good: no try of the same build goes through.
/// A check that could not be made (a failed read) fails as any step does,
/// and is tried again.
impl From<Unrunnable> for Halt {
    fn from(unrunnable: Unrunnable) -> Self {
        let build_cannot_run = matches!(unrunnable, Unrunnable::Cannot(_));
        match <(StatusCode, String)>::from(unrunnable).into() {
            Halt::Failed(failure) => Halt::Failed(Failure { build_cannot_run, ..failure }),
            other => other,
        }
    }
}

/// A step that answered a 503 was one the database did not take, nothing
/// of it ran (`weft_core::install::asks_again`): it waits on the database
/// ([`Halt::on_the_database`]). Any other answer is an ordinary failure.
impl From<(StatusCode, String)> for Halt {
    fn from((status, error): (StatusCode, String)) -> Self {
        if status == StatusCode::SERVICE_UNAVAILABLE {
            return Halt::on_the_database(error);
        }
        Halt::Failed(Failure { status, error, waits_for: None, build_cannot_run: false })
    }
}

/// [`Halt::failed`] doing `what`, for a `map_err`.
pub(crate) fn failing(what: &'static str) -> impl Fn(anyhow::Error) -> Halt {
    move |e| Halt::failed(what, e)
}

/// Resync every owner not brought back yet under `driver`, writing each
/// into the plan as it comes back, then end the resync, which lands the
/// transition at rest. A drive that ended already (its task is run again
/// after its end) answers as it ended.
async fn advance(
    state: &DispatcherState,
    id: uuid::Uuid,
    driver: uuid::Uuid,
    progress: &TaskProgress,
) -> Result<weft_core::deactivation::ResyncResponse, Halt> {
    let (mut plan, under_way) = state.projects.drive(id, driver).await.map_err(failing("read the resync"))?.ok_or(Halt::Given)?;
    if !under_way {
        return Ok(plan.answer());
    }
    let several = plan.owners.len() > 1;
    let in_context = |plan: &ResyncPlan, at: usize, halt: Halt| match halt {
        Halt::Failed(failure) if several => {
            let done = if plan.resynced.is_empty() {
                "none resynced before it".to_string()
            } else {
                let whose: Vec<String> = plan.resynced.iter().map(weft_core::deactivation::whose_triggers).collect();
                format!("already resynced: {}", whose.join(", "))
            };
            let error = format!("resyncing {}: {} ({done})", weft_core::deactivation::whose_triggers(&plan.owners[at].owner), failure.error);
            Halt::Failed(Failure { error, ..failure })
        }
        halt => halt,
    };
    if !plan.switching {
        for at in plan.resynced.len()..plan.owners.len() {
            if let Err(halt) = crate::api::project::resync_take_down_owner(state, id, &mut plan, at).await {
                return Err(in_context(&plan, at, halt));
            }
        }
        let_the_old_version_finish(state, id, &mut plan, progress).await?;
        plan.switching = true;
        if !state.projects.write_resync(id, &plan).await.map_err(failing("write down that the old version is done"))? {
            return Err(Halt::Given);
        }
    }
    while plan.resynced.len() < plan.owners.len() {
        let at = plan.resynced.len();
        let back = match crate::api::project::resync_bring_back_owner(state, id, &plan, at).await {
            Ok(back) => back,
            Err(halt) => return Err(in_context(&plan, at, halt)),
        };
        plan.urls.extend(back);
        plan.resynced.push(plan.owners[at].owner.clone());
        if !state.projects.write_resync(id, &plan).await.map_err(failing("write down the owners resynced"))? {
            return Err(Halt::Given);
        }
    }
    if !state.projects.end_resync(id, driver).await.map_err(failing("end the resync"))? {
        return Err(Halt::Given);
    }
    crate::transition::publish_transition_changed(state, id).await;
    // Triggers that went down with their infra while the resync held the
    // project could not come back then (it refused every other
    // activation), and their infra may be up already.
    if let Err(e) = crate::api::infra::bring_back_what_went_down_with_infra(state, id).await {
        tracing::error!(
            target: "weft_dispatcher::resync",
            project_id = %id,
            error = %format!("{e:#}"),
            "the resync ended, but bringing back the triggers that went down with their infra meanwhile failed; \
             the next change of a copy tries again, or `weft activate` switches them on"
        );
    }
    // Triggers the health check took down may have been held back from
    // coming on by the routes' one binary (`TriggerCopy::held_back`), which
    // this resync just made one again: the supervisor looks once more, so
    // their health rules bring them back now rather than after their
    // backoff, or after a person asks.
    if let Err(e) = look_again_for_health_downs(state, id).await {
        tracing::error!(
            target: "weft_dispatcher::resync",
            project_id = %id,
            error = %format!("{e:#}"),
            "the resync ended, but asking the supervisor to look again at the triggers the health check took down failed; \
             `weft infra look` asks again"
        );
    }
    Ok(plan.answer())
}

/// Ask the supervisor to look at project `id` again when a trigger the
/// health check took down is still down.
async fn look_again_for_health_downs(state: &DispatcherState, id: uuid::Uuid) -> anyhow::Result<()> {
    let health_down = state.activations.list(id).await?.iter().any(|a| {
        a.lifecycle.status == crate::activation_store::ProjectStatus::Inactive
            && a.lifecycle.went_down == Some(weft_broker_client::activation::WentDown::Health)
    });
    if health_down {
        crate::infra_lifecycle_command::person_asks_for_look(&state.pg_pool, id).await?;
    }
    Ok(())
}

/// The runs going on the old version, once every owner is down so nothing
/// new is born on it: the ones the triggers it took down fired (their cut
/// moved, maybe their code too), and every one on the builds those
/// triggers fired (`plan.old_builds`: the workers the new version takes
/// over from, locally the one holding the project's port). Cancelled
/// under `running: cancel`, then waited for, however long that takes, so
/// the new version takes over only once the old one has nothing left
/// running (`crate::drain`). A run paused on a wait is not going; one that
/// resumes meanwhile is waited for too. While it waits it says on its
/// task which runs it waits for and since when ([`TaskProgress`]), and
/// that it waits for none once they ended. `weft stop <run>` ends one now;
/// `weft cancel-activate` gives the resync up, which ends this wait at
/// once ([`given_up`]).
async fn let_the_old_version_finish(state: &DispatcherState, id: uuid::Uuid, plan: &mut ResyncPlan, progress: &TaskProgress) -> Result<(), Halt> {
    let keys: Vec<ActivationKey> = plan.owners.iter().flat_map(|owner| owner.keys(&owner.down)).collect();
    let target = crate::take_down::TakeDownTarget::Activations(keys.clone());
    let live = crate::take_down::live_runs(state, id).await.map_err(failing("the runs going"))?;
    let mut going: Vec<weft_core::RunId> =
        crate::take_down::affected_runs(&target, &live, None).into_iter().filter(|run| !run.suspended).map(|run| run.run_id).collect();
    if !plan.old_builds.is_empty() {
        going.extend(crate::drain::going_on_builds(state, id, &plan.old_builds).await.map_err(failing("the runs on the old version"))?);
    }
    going.sort();
    going.dedup();
    plan.runs = plan.runs.max(going.len() as u64);
    if !state.projects.write_resync(id, plan).await.map_err(failing("write down the runs on the old version"))? {
        return Err(Halt::Given);
    }
    if plan.take_down.running == weft_core::take_down::Running::Cancel {
        let user = weft_core::run::CancelCause::User;
        let cancelled: Vec<(weft_core::RunId, &weft_core::run::CancelCause)> = going.iter().map(|run| (*run, &user)).collect();
        crate::api::run::cancel_run_ids(state, &cancelled).await.map_err(failing("cancel the runs on the old version"))?;
    }
    let fired = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Fired { keys: &keys, tokens: &plan.old_tokens } };
    let builds = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Builds(&plan.old_builds) };
    let finished = async {
        crate::drain::drain(state, &fired, None, Some(progress)).await?;
        if !plan.old_builds.is_empty() {
            crate::drain::drain(state, &builds, None, Some(progress)).await?;
        }
        progress.waits_for(Vec::new()).await
    };
    tokio::select! {
        finished = finished => finished.map_err(failing("wait for the runs on the old version")),
        given = given_up(state, id, plan.driver) => match given {
            Ok(()) => Err(Halt::Given),
            Err(e) => Err(failing("follow the resync while it waits")(e)),
        },
    }
}

/// Where a resync says on its task what it waits for
/// ([`weft_core::deactivation::ResyncProgress`]), which `weft resync`
/// shows while it follows and `weft status` shows beside the resync. It
/// writes only when what it waits for changed since it last said it.
pub(crate) struct TaskProgress {
    pool: weft_task_store::db::Pool,
    task: uuid::Uuid,
    said: tokio::sync::Mutex<Said>,
}

impl TaskProgress {
    fn new(pool: weft_task_store::db::Pool, task: uuid::Uuid) -> Self {
        Self { pool, task, said: tokio::sync::Mutex::new(Said::default()) }
    }

    /// It waits for `runs` now (none: it waits for no run any more).
    async fn waits_for(&self, runs: Vec<weft_core::deactivation::RunGoing>) -> Result<()> {
        let mut said = self.said.lock().await;
        let Some(progress) = said.news(runs) else { return Ok(()) };
        weft_task_store::tasks::set_progress(&self.pool, self.task, &serde_json::to_value(&progress)?).await?;
        said.last = Some(progress.waits_for);
        Ok(())
    }
}

#[async_trait]
impl crate::drain::SayWaiting for TaskProgress {
    async fn going(&self, runs: Vec<weft_core::deactivation::RunGoing>) -> Result<()> {
        self.waits_for(runs).await
    }
}

/// What a resync last said on its task it waits for.
#[derive(Debug, Default)]
struct Said {
    /// `None` until it said anything.
    last: Option<Vec<weft_core::deactivation::RunGoing>>,
}

impl Said {
    /// What to say when it waits for `runs`: nothing when that is what it
    /// said last. The first time it is always said: a task run again (its
    /// process died) finds on its row what the run before said.
    fn news(&self, runs: Vec<weft_core::deactivation::RunGoing>) -> Option<weft_core::deactivation::ResyncProgress> {
        (self.last.as_ref() != Some(&runs)).then_some(weft_core::deactivation::ResyncProgress { waits_for: runs })
    }
}

/// Where a resync given up is announced (`weft cancel-activate`,
/// `crate::project_store::ProjectStoreOps::give_up_resync`), the project's
/// id as payload: what ends the wait of the drive it gave up, on whichever
/// dispatcher it runs.
pub const RESYNC_GIVEN_UP_CHANNEL: &str = "weft_resync_given_up";

/// Return once `driver`'s drive of `id`'s resync is over (given up, or
/// taken up again under another drive): looked at now, and again each
/// time a resync of the project is given up.
async fn given_up(state: &DispatcherState, id: uuid::Uuid, driver: uuid::Uuid) -> Result<()> {
    let mut heard = state.signals.subscribe();
    let project = id.to_string();
    loop {
        if !matches!(state.projects.drive(id, driver).await?, Some((_, true))) {
            return Ok(());
        }
        heard.woken(|channel, payload| channel == RESYNC_GIVEN_UP_CHANNEL && payload == project).await?;
    }
}

/// Write down why the resync stopped, for `weft status` and the request
/// following it. A drive given up or taken up again meanwhile has nothing
/// to write to.
async fn stop(state: &DispatcherState, id: uuid::Uuid, driver: uuid::Uuid, failure: &Failure) -> Result<()> {
    tracing::error!(target: "weft_dispatcher::resync", project_id = %id, error = %failure.error, "a resync stopped with triggers down");
    let Some((plan, true)) = state.projects.drive(id, driver).await? else { return Ok(()) };
    let stopped = Stopped {
        status: failure.status.as_u16(),
        error: failure.error.clone(),
        at_unix: crate::lease::now_unix(),
        waits_for: failure.waits_for,
        build_cannot_run: failure.build_cannot_run,
    };
    state.projects.write_resync(id, &ResyncPlan { stopped: Some(stopped), ..plan }).await?;
    crate::transition::publish_transition_changed(state, id).await;
    // Nothing announces a database that answers again: the loop that takes
    // stops up again ([`retry_stopped`]) is booked to look then. A machine's
    // loop looks on its own safety tick. The stop is not done until the look
    // is booked: a booking the database did not take fails the stop, and the
    // caller waits on the database and stops again (writing the plan again
    // is harmless), since a stop with no look booked would sit on a cloud
    // install until a person resumed it.
    if failure.waits_for == Some(ResyncWait::Database) {
        if let Some(booking) = &state.booking {
            let look = weft_core::time_scale::scaled(DATABASE_LOOK_AGAIN);
            booking
                .book_sooner_after(crate::reaper::RESYNC_RETURNS, look)
                .await
                .context("book the look that takes up a resync stopped waiting on the database")?;
        }
    }
    Ok(())
}

/// Take up again every stopped resync whose stop waits on something that
/// is back now: the infra its triggers read runs, or no activation of
/// them is still landing. Called where those changes are heard (a copy's
/// status changing, an activation changing status:
/// `crate::api::infra::bring_back_triggers_whose_infra_returned`), so a
/// resync waiting on them goes on without a person. A stop that waits on
/// the database is taken up again once [`DATABASE_LOOK_AGAIN`] has passed
/// since it stopped, the read of it having gone through: answers when the
/// next of those is due, for the caller's loop to look again then, and
/// `None` while none waits (nothing is booked). A stop that waits on
/// nothing that comes back by itself is left for `weft resync --resume`.
///
/// A resync whose plan no longer reads can never be driven, resumed or
/// given up by a person (each of those reads it), so it is given up here,
/// for that project alone, and `weft status` says so
/// (`crate::project_store::ProjectStoreOps::give_up_unreadable_resync`).
/// A project whose retry fails (the store, on the way) is named in the
/// error once every other project was looked at.
pub(crate) async fn retry_stopped(state: &DispatcherState) -> Result<Option<std::time::Duration>> {
    let mut failed = Vec::new();
    let mut next: Option<std::time::Duration> = None;
    let now = crate::lease::now_unix();
    for (id, plan) in state.projects.stopped_resyncs().await? {
        let retried = match plan {
            Ok(plan) => match database_look_in(plan.stopped.as_ref(), now) {
                Some(later) => {
                    next = next.into_iter().chain([later]).min();
                    Ok(())
                }
                None => retry_if_back(state, id, &plan).await,
            },
            Err(unreadable) => give_up_unreadable(state, id, &unreadable).await,
        };
        if let Err(e) = retried {
            failed.push(format!("project {id}: {e:#}"));
        }
    }
    anyhow::ensure!(failed.is_empty(), "taking stopped resyncs up again failed: {}", failed.join("; "));
    Ok(next)
}

/// How long until a resync `stopped` waiting on the database is taken up
/// again, at `now_unix`: `None` when it is due now, or when its stop waits
/// on something else.
fn database_look_in(stopped: Option<&Stopped>, now_unix: i64) -> Option<std::time::Duration> {
    let stopped = stopped.filter(|stopped| stopped.waits_for == Some(ResyncWait::Database))?;
    let due = stopped.at_unix.saturating_add(weft_core::time_scale::scaled(DATABASE_LOOK_AGAIN).as_secs() as i64);
    (due > now_unix).then(|| std::time::Duration::from_secs((due - now_unix) as u64))
}

/// Give up project `id`'s resync, whose plan did not read (`unreadable`).
async fn give_up_unreadable(state: &DispatcherState, id: uuid::Uuid, unreadable: &anyhow::Error) -> Result<()> {
    if state.projects.give_up_unreadable_resync(id).await? {
        tracing::error!(
            target: "weft_dispatcher::resync",
            project_id = %id, error = %format!("{unreadable:#}"),
            "a resync's plan no longer reads, so it was given up; the triggers it had taken down may still be down"
        );
        crate::transition::publish_transition_changed(state, id).await;
    } else {
        tracing::warn!(
            target: "weft_dispatcher::resync",
            project_id = %id, error = %format!("{unreadable:#}"),
            "a resync's plan did not read, and read again under its row's lock there was nothing to give up: it ended, or it reads now"
        );
    }
    Ok(())
}

async fn retry_if_back(state: &DispatcherState, id: uuid::Uuid, plan: &ResyncPlan) -> Result<()> {
    let back = match plan.stopped.as_ref().and_then(|stopped| stopped.waits_for) {
        None => false,
        // The read of its plan went through: the database answers.
        Some(ResyncWait::Database) => true,
        Some(ResyncWait::Activation) => {
            let keys: Vec<ActivationKey> = plan.remaining().iter().flat_map(|owner| owner.keys(&owner.on)).collect();
            !state
                .activations
                .list(id)
                .await?
                .iter()
                .any(|a| keys.contains(&a.key) && a.lifecycle.status == crate::activation_store::ProjectStatus::Activating)
        }
        Some(ResyncWait::Infra) => {
            let Some(project) = state.projects.project(id).await? else { return Ok(()) };
            let mut up = true;
            for owner in plan.remaining() {
                up &= crate::api::project::require_trigger_infra(state, id, &project, &owner.keys(&owner.on)).await.is_ok();
            }
            up
        }
    };
    if !back {
        return Ok(());
    }
    let tenant = state.projects.tenant_for(id).await?.ok_or_else(|| anyhow::anyhow!("no project {id}"))?;
    if state.projects.resume_resync(id, &tenant).await?.is_some() {
        tracing::info!(target: "weft_dispatcher::resync", project_id = %id, "what a stopped resync waited on is back; taking it up again");
        crate::transition::publish_transition_changed(state, id).await;
    }
    Ok(())
}

/// The triggers `plan` took down that are still not on, by owner, as
/// `rows` stand: what a person who gives it up is told stays down. Read
/// in the give-up's own transaction, the rows are the truth: its take-down
/// and its turn-on both go through only while its drive holds the
/// project, so a trigger of `down` it never took down is still on, and one
/// it took down is on only once brought back.
pub fn left_down(plan: &ResyncPlan, rows: &[crate::activation_store::Activation]) -> Vec<OwnerTriggers> {
    plan.owners
        .iter()
        .filter_map(|owner| {
            let down: Vec<String> = owner
                .down
                .iter()
                .filter(|trigger| {
                    let key = ActivationKey::new((*trigger).clone(), owner.owner.clone());
                    !rows.iter().any(|a| a.key == key && a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
                })
                .cloned()
                .collect();
            (!down.is_empty()).then(|| OwnerTriggers { owner: owner.owner.clone(), triggers: down })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_task_store::db;

    /// A call that got no connection, for `why`.
    fn no_connection(why: db::NoConnection) -> anyhow::Error {
        db::Error::NoConnection { why, refused: None }.into()
    }

    fn plan() -> ResyncPlan {
        let ada = Owner::Instance(weft_core::instance::InstanceId::new("ada").unwrap());
        ResyncPlan {
            driver: uuid::Uuid::nil(),
            owners: vec![
                ResyncOwner { owner: Owner::Shared, on: vec!["feed".into(), "door".into()], down: vec!["feed".into(), "door".into()] },
                ResyncOwner { owner: ada, on: vec!["feed".into()], down: Vec::new() },
            ],
            take_down: weft_core::take_down::TakeDown::DEFAULT,
            build: Default::default(),
            resynced: Vec::new(),
            urls: Vec::new(),
            held: Default::default(),
            old_builds: Vec::new(),
            old_tokens: Vec::new(),
            switching: false,
            runs: 0,
            moved_runs: 0,
            off: Vec::new(),
            stopped: None,
            given_up: None,
        }
    }

    /// The owners left are the ones after those brought back.
    #[test]
    fn the_owners_left_follow_those_brought_back() {
        let mut plan = plan();
        assert_eq!(plan.remaining().len(), 2);
        plan.resynced.push(Owner::Shared);
        assert_eq!(plan.remaining().iter().map(|o| o.on.clone()).collect::<Vec<_>>(), [vec!["feed".to_string()]]);
        plan.resynced.push(plan.owners[1].owner.clone());
        assert!(plan.remaining().is_empty());
    }

    /// A stop that waits on nothing reads as it did; one that waits on
    /// infra says so on the wire.
    #[test]
    fn a_stop_says_what_it_waits_on() {
        let stopped = Stopped { status: 428, error: "infra down".into(), at_unix: 1, waits_for: Some(ResyncWait::Infra), build_cannot_run: false };
        let wire = serde_json::to_value(&stopped).unwrap();
        assert_eq!(wire["waits_for"], "infra");
        let none = serde_json::json!({ "status": 409, "error": "no", "at_unix": 1 });
        assert_eq!(serde_json::from_value::<Stopped>(none).unwrap().waits_for, None);
    }

    /// What a given-up resync leaves down is what it took down and is not
    /// on now: never a trigger it did not take down, never one back on.
    #[test]
    fn what_stays_down_is_what_it_took_down_and_is_not_back() {
        use crate::activation_store::{Activation, ActivationLifecycle};
        let plan = plan();
        let row = |trigger: &str, lifecycle: ActivationLifecycle| Activation {
            key: ActivationKey::new(trigger, Owner::Shared),
            lifecycle,
            program: None,
            source_version: None,
            driver_task: None,
            activating: None,
            cut: None,
        };
        let rows = [row("feed", ActivationLifecycle::active()), row("door", ActivationLifecycle::holding())];
        assert_eq!(left_down(&plan, &rows), [OwnerTriggers { owner: Owner::Shared, triggers: vec!["door".into()] }]);
        let rows = [row("feed", ActivationLifecycle::active()), row("door", ActivationLifecycle::active())];
        assert!(left_down(&plan, &rows).is_empty(), "ada's feed was never taken down");
    }

    /// What `weft status` shows of a given-up resync is what it left down
    /// that nobody decided about since and that the program still has for
    /// its owner: a trigger someone activated or took down drops out, and
    /// so does one the program lost.
    #[test]
    fn a_given_up_resync_shows_only_what_is_still_left_down() {
        let ada = Owner::Instance(weft_core::instance::InstanceId::new("ada").unwrap());
        let mut given_up = GivenUp {
            unreadable: None,
            at_unix: 1,
            left_down: vec![
                OwnerTriggers { owner: Owner::Shared, triggers: vec!["feed".into(), "door".into(), "gone".into()] },
                OwnerTriggers { owner: ada.clone(), triggers: vec!["feed".into()] },
            ],
        };
        let nodes = ["feed", "door"].map(|id| {
            serde_json::json!({
                "id": id, "nodeType": "T", "label": null, "config": {}, "position": { "x": 0, "y": 0 },
                "features": { "isTrigger": true },
            })
        });
        let project: weft_core::project::ProjectDefinition =
            serde_json::from_value(serde_json::json!({ "id": uuid::Uuid::nil(), "nodes": nodes, "edges": [] })).unwrap();
        let shared = |triggers: &[&str]| OwnerTriggers { owner: Owner::Shared, triggers: triggers.iter().map(|t| t.to_string()).collect() };
        assert_eq!(given_up.still_down(&project), [shared(&["feed", "door"])], "a trigger the program lost, or no longer has per instance, is not shown");

        assert!(!given_up.forget(&[ActivationKey::new("feed", Owner::Instance(weft_core::instance::InstanceId::new("bob").unwrap()))]));
        assert!(given_up.forget(&[ActivationKey::new("door", Owner::Shared)]), "someone decided about the door");
        assert_eq!(given_up.still_down(&project), [shared(&["feed"])]);
        assert!(given_up.forget(&[ActivationKey::new("feed", Owner::Shared), ActivationKey::new("feed", ada)]));
        assert_eq!(given_up.left_down, [shared(&["gone"])], "an owner with nothing left down goes");
        assert!(given_up.still_down(&project).is_empty());
    }

    /// A step that failed is tried again until its waits run out; one its
    /// build cannot run is never tried again, and what the person is told
    /// next offers no `--resume` (which keeps that build) but the commands
    /// that do bring the triggers back. A stored stop of an older weft,
    /// which says nothing about it, reads as one a resume may retry.
    #[test]
    fn a_build_that_cannot_run_is_not_tried_again_and_offers_what_works() {
        let failure = |build_cannot_run| Failure { status: StatusCode::UNPROCESSABLE_ENTITY, error: "no".into(), waits_for: None, build_cannot_run };
        let mut tries = Tries::default();
        assert_eq!(tries.wait_before_trying_again(&failure(false)), Some(RETRY_WAITS[0]));
        assert_eq!(tries.wait_before_trying_again(&failure(true)), None, "never tried again");
        assert_eq!(tries.wait_before_trying_again(&failure(false)), Some(RETRY_WAITS[1]), "it spent no try");
        assert_eq!(tries.wait_before_trying_again(&failure(false)), None, "the tries ran out");

        let stopped = |build_cannot_run, waits_for| Stopped { status: 422, error: "no".into(), at_unix: 1, waits_for, build_cannot_run };
        let next = what_next(Some(&stopped(true, None)));
        assert!(!next.contains("tries it again") && next.contains("weft cancel-activate") && next.contains("weft activate"), "{next}");
        let next = what_next(Some(&stopped(false, Some(ResyncWait::Infra))));
        assert!(next.starts_with("it goes on by itself") && next.contains("`weft resync --resume` tries it again"), "{next}");
        assert!(what_next(None).contains("`weft resync --resume` tries it again"));

        let older = serde_json::json!({ "status": 422, "error": "no", "at_unix": 1 });
        assert!(!serde_json::from_value::<Stopped>(older).unwrap().build_cannot_run);
        assert!(serde_json::to_value(stopped(false, None)).unwrap().get("build_cannot_run").is_none(), "written only when set");
    }

    /// A store failure while checking that the build can run the triggers
    /// (a read of what the infra saved, answered 500) says nothing about
    /// the build: the step is tried again like any failed one, and the
    /// stop it may end in offers `--resume`. Only a build that cannot run
    /// them stops the resync for good.
    #[test]
    fn a_check_that_could_not_be_made_is_tried_again() {
        let failed = |halt: Halt| match halt {
            Halt::Failed(failure) => failure,
            _ => panic!("a check that refused is a failed step"),
        };
        let unchecked = failed(Unrunnable::Unchecked((StatusCode::INTERNAL_SERVER_ERROR, "infra_node: connection reset".into())).into());
        assert!(!unchecked.build_cannot_run, "a store failure is not the build's");
        assert_eq!((unchecked.status, unchecked.error.as_str()), (StatusCode::INTERNAL_SERVER_ERROR, "infra_node: connection reset"));
        assert_eq!(Tries::default().wait_before_trying_again(&unchecked), Some(RETRY_WAITS[0]), "tried again");
        // A read the database did not take answered 503: it waits on the
        // database, and what it would stop with is no 503.
        let busy = failed(Unrunnable::Unchecked((StatusCode::SERVICE_UNAVAILABLE, "the install could not read the running build right now".into())).into());
        assert_eq!((busy.waits_for, busy.status, busy.build_cannot_run), (Some(ResyncWait::Database), StatusCode::INTERNAL_SERVER_ERROR, false));

        let cannot = failed(Unrunnable::Cannot("node `auth` did not compile".into()).into());
        assert!(cannot.build_cannot_run);
        assert_eq!(cannot.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(Tries::default().wait_before_trying_again(&cannot), None, "never tried again");
    }

    /// A step the database did not take (its pool had no connection for
    /// it, the database was full or out of reach) waits on the database:
    /// it spends none of the tries a step that failed has, and has waits of
    /// its own, after which it stops waiting on the database. Any other
    /// failure of a read is an ordinary failed step.
    #[test]
    fn a_step_the_database_did_not_take_waits_on_the_database() {
        let failed = |halt: Halt| match halt {
            Halt::Failed(failure) => failure,
            _ => panic!("a failed read is a failed step"),
        };
        let busy = failed(Halt::failed("read the resync", no_connection(db::NoConnection::Busy).context("project 1")));
        assert_eq!(busy.waits_for, Some(ResyncWait::Database));
        assert_eq!(busy.status, StatusCode::INTERNAL_SERVER_ERROR, "a stop is never a 503: following it asks the same again");
        assert!(busy.error.starts_with("read the resync: the install's database was busy or out of reach"), "{}", busy.error);
        let gone = failed(failing("end the resync")(no_connection(db::NoConnection::Closed)));
        assert_eq!(gone.waits_for, Some(ResyncWait::Database), "a database out of reach is waited on too");
        let broken = failed(Halt::failed("read the resync", anyhow::Error::from(db::Error::NoRow)));
        assert_eq!((broken.waits_for, broken.status), (None, StatusCode::INTERNAL_SERVER_ERROR));

        let mut tries = Tries::default();
        for wait in DATABASE_WAITS {
            assert_eq!(tries.wait_before_trying_again(&busy), Some(wait));
        }
        assert_eq!(tries.wait_before_trying_again(&broken), Some(RETRY_WAITS[0]), "the database's waits spent no try of a failed step");
        assert_eq!(tries.wait_before_trying_again(&busy), None, "its waits ran out: it stops waiting on the database");

        let stopped = Stopped { status: 500, error: "busy".into(), at_unix: 1, waits_for: Some(ResyncWait::Database), build_cannot_run: false };
        assert_eq!(serde_json::to_value(&stopped).unwrap()["waits_for"], "database");
        let next = what_next(Some(&stopped));
        assert!(next.starts_with("it goes on by itself shortly, once the install's database answers again; "), "{next}");
        assert!(next.contains("`weft resync --resume` tries it again"), "{next}");
    }

    /// A caller that waits on the database with no stop to fall back on
    /// waits as a resync does, and keeps waiting the longest of those waits
    /// for as long as the database does not take it.
    #[test]
    fn a_wait_on_the_database_with_no_stop_keeps_its_longest_wait() {
        let secs = |s: u64| weft_core::time_scale::scaled(std::time::Duration::from_secs(s));
        for (waited, wait) in DATABASE_WAITS.iter().enumerate() {
            assert_eq!(database_wait(waited), secs(*wait));
        }
        let longest = secs(*DATABASE_WAITS.last().unwrap());
        assert_eq!(database_wait(DATABASE_WAITS.len()), longest);
        assert_eq!(database_wait(usize::MAX), longest);
    }

    /// A stop that waits on the database is taken up again once its look
    /// is due, and until then says how long is left; any other stop is not
    /// the database's to time.
    #[test]
    fn a_stop_on_the_database_is_looked_at_again_when_due() {
        let look = weft_core::time_scale::scaled(DATABASE_LOOK_AGAIN).as_secs() as i64;
        let stopped = |waits_for| Stopped { status: 500, error: "busy".into(), at_unix: 1000, waits_for, build_cannot_run: false };
        let database = stopped(Some(ResyncWait::Database));
        if look > 0 {
            assert_eq!(database_look_in(Some(&database), 1000), Some(std::time::Duration::from_secs(look as u64)));
            assert_eq!(database_look_in(Some(&database), 999 + look), Some(std::time::Duration::from_secs(1)));
        }
        assert_eq!(database_look_in(Some(&database), 1000 + look), None, "due: taken up again now");
        assert_eq!(database_look_in(Some(&stopped(Some(ResyncWait::Infra))), 1000), None);
        assert_eq!(database_look_in(Some(&stopped(None)), 1000), None);
        assert_eq!(database_look_in(None, 1000), None);
    }

    /// A resync says what it waits for the first time, and then only when
    /// that changed: a count that finds the same runs writes nothing.
    #[test]
    fn a_resync_says_what_it_waits_for_only_when_it_changed() {
        let run = |n: u128| weft_core::deactivation::RunGoing { run_id: uuid::Uuid::from_u128(n), since_unix: n as i64 };
        let mut said = Said::default();
        let first = said.news(vec![run(1), run(2)]).expect("the first time is said");
        assert_eq!(first.waits_for, [run(1), run(2)]);
        said.last = Some(first.waits_for);
        assert_eq!(said.news(vec![run(1), run(2)]), None, "the same runs: nothing to write");
        assert_eq!(said.news(vec![run(2)]).map(|news| news.waits_for), Some(vec![run(2)]), "one ended");
        said.last = Some(vec![run(2)]);
        assert_eq!(said.news(Vec::new()).map(|news| news.waits_for), Some(Vec::new()), "none left is said");
        assert_eq!(Said::default().news(Vec::new()).map(|news| news.waits_for), Some(Vec::new()), "said once even when none ran");
    }

    /// A stop the database did not take (its booking refused for a full
    /// pool) is tried again on its own until it lands, and any other
    /// failure of it is the answer at once: only the stop is tried again,
    /// never the drive.
    #[tokio::test(start_paused = true)]
    async fn a_stop_the_database_did_not_take_is_tried_again_alone() {
        let tries = std::cell::Cell::new(0);
        let landed = stop_until_taken(uuid::Uuid::nil(), || {
            tries.set(tries.get() + 1);
            let attempt = tries.get();
            async move {
                if attempt < 3 {
                    return Err(no_connection(db::NoConnection::Busy).context("book the look"));
                }
                Ok(())
            }
        })
        .await;
        assert!(landed.is_ok(), "{landed:?}");
        assert_eq!(tries.get(), 3, "tried until it landed, and not once more");

        let tries = std::cell::Cell::new(0);
        let broken = stop_until_taken(uuid::Uuid::nil(), || {
            tries.set(tries.get() + 1);
            async { Err(anyhow::Error::from(db::Error::NoRow)) }
        })
        .await;
        assert!(broken.is_err());
        assert_eq!(tries.get(), 1, "a stop that failed otherwise is not tried again");
    }

    /// The task names the drive it carries, and two drives are two tasks.
    #[test]
    fn a_task_carries_its_drive() {
        let (a, b) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let task_a = task("t", uuid::Uuid::nil(), a);
        assert_ne!(task_a.dedup_key, task("t", uuid::Uuid::nil(), b).dedup_key);
        assert_eq!(task_a.payload["driver"], a.to_string());
    }
}
