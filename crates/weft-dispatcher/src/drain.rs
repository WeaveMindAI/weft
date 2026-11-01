//! The wait a command that takes something down makes for the runs going
//! (`--running wait`), however long that takes, with nothing cancelling
//! them for it: an infra command keeps its copies up until the runs
//! reaching them end ([`Reaching::Infra`]), a resync lets its new version
//! take over once the runs on the old one end ([`Reaching::Builds`]), a
//! deactivate waits for the runs its triggers fired ([`Reaching::Fired`]).
//! The person ends a wait by stopping those runs (`weft stop <run>`), by
//! calling the command off (`weft infra cancel`, `weft cancel-activate`),
//! or, for a deactivate, by no longer waiting (Ctrl+C: the triggers are
//! down already). What is waited on is taken down first
//! (`crate::take_down`), so nothing new is born of it meanwhile; a run
//! started by hand that it reaches is waited for too.
//!
//! "Work going" is two things added up. What the project's live workers
//! say they drive right now, per trigger, at their door tick
//! (`worker_lease.in_flight`, from the door's own count of the runs it
//! admitted), for the triggers whose runs it waits on (on the builds it
//! waits on, for a resync's): a run a
//! worker bore at its door is counted from the moment the door takes it,
//! before anything of it is on record. And the runs on record whose own cut
//! contains them that are queued for a worker or being driven by one
//! (`weft_task_store::in_flight_sql!`, `crate::infra_reach`). A run is
//! often counted twice, once by its worker and once by its row, which is
//! harmless: the count is only ever compared to zero. A run paused on a
//! wait is not going: what becomes of it is the command's `held` answer.

use std::time::Duration;

use tokio::time::Instant;

use weft_core::RunId;
use weft_task_store::db::{self, Client as _};
use weft_task_store::drain::WakeOn;

use crate::infra_reach::InfraReach;
use crate::state::DispatcherState;

/// What a drain waits on: the work of one project that reaches some infra
/// copies, or all of it.
#[derive(Debug, Clone, Copy)]
pub struct DrainScope<'a> {
    pub project_id: uuid::Uuid,
    pub reaching: Reaching<'a>,
}

/// The work a drain waits on. `tokens` are the entry tokens of the
/// triggers whose runs it waits on, read before their take-down (one that
/// refuses with nothing kept deletes their signal rows): what their
/// workers count at the door, a run admitted there and not on record yet
/// (or never, on an unrecorded route) included.
#[derive(Debug, Clone, Copy)]
pub enum Reaching<'a> {
    /// What reaches these infra copies (an infra command's wait), the
    /// triggers reaching them that were taken down with it counted by
    /// `tokens`.
    Infra { reach: &'a InfraReach, tokens: &'a [String] },
    /// The runs these activations' triggers fired (a deactivate's wait).
    Fired { keys: &'a [weft_core::activation::ActivationKey], tokens: &'a [String] },
    /// Every run on these builds (binary hashes): a resync's wait before
    /// its new version takes over.
    Builds(&'a [String]),
    /// Every run of the project (`weft rm --journal`, waiting for what it
    /// cancelled to end before the history goes).
    EveryRun,
}

/// How often a drain still waiting says so in the log.
const BREADCRUMB_EVERY: Duration = Duration::from_secs(30);

/// What makes a drain count again: a run ended (every run it waits on,
/// and every run in its scope waiting to resume, is marked watched,
/// [`mark_watched`]), or an activation changing status.
pub(crate) static WAKE_ON: &[WakeOn] =
    &[WakeOn::any(weft_journal::RUN_ENDED_CHANNEL), WakeOn::any(crate::holders::HELD_SIGNALS_CHANNEL)];

/// Wait until nothing `scope` reaches is going, for as long as it takes:
/// logged every half minute with the runs it waits for, with no query of
/// its own. It counts again when a run it waits on ends ([`WAKE_ON`]) and
/// when the lease of a worker driving what it waits on could lapse
/// ([`workers_look`]); never on a timer of its own. A drain run under a
/// command's `claim` renews it at each count to last past the next one,
/// and counts again within a claim lease, so the claim never runs further
/// ahead than one lease; it fails once the claim is no longer this
/// process's. With `say`, each count also tells it the runs on record it
/// waits for, and since when ([`going_since`]: one read more, made only
/// for a wait somebody follows).
pub async fn drain(
    state: &DispatcherState,
    scope: &DrainScope<'_>,
    claim: Option<&crate::lifecycle_claimer::CommandClaim<'_>>,
    say: Option<&dyn SayWaiting>,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let mut breadcrumb = started + BREADCRUMB_EVERY;
    // Subscribed before the first count, so an ending between the count
    // and the wait is heard.
    let mut heard = state.signals.subscribe();
    loop {
        let reached = Reached::now(state, scope).await?;
        // Watched before counted: a run that ends after the mark announces
        // its ending, which the subscription above hears; one that ended
        // before it is no longer going when counted. Marked after the
        // count, a run that ended in between was counted, never announced,
        // and waited on for good.
        mark_watched(&state.pg_pool, scope.project_id, &reached.all_runs()).await?;
        let left = going(&state.pg_pool, scope.project_id, &reached.door, &reached.all_runs()).await?;
        if left == 0 {
            return Ok(());
        }
        if let Some(say) = say {
            say.going(going_since(&state.pg_pool, &reached.all_runs()).await?).await?;
        }
        let now = Instant::now();
        let look = workers_look(&state.pg_pool, scope.project_id, &reached.door, &reached.all_runs())
            .await?
            .map(|at| now + Duration::from_secs((at + 1 - crate::lease::now_unix()).max(1).unsigned_abs()));
        let mut count_at = look;
        if let Some(claim) = claim {
            // The claim is kept one claim lease at most: a dispatcher that
            // dies mid-drain leaves a claim another takes over within a
            // lease, whatever the drain was waiting for.
            use weft_broker_client::lifecycle_command::{CLAIM_LEASE_TTL, CLAIM_RENEW_INTERVAL};
            let renew_by = now + CLAIM_LEASE_TTL - CLAIM_RENEW_INTERVAL;
            let next = count_at.map_or(renew_by, |count_at| count_at.min(renew_by));
            count_at = Some(next);
            let until = crate::lease::now_unix()
                + i64::try_from(next.saturating_duration_since(now).as_secs() + CLAIM_RENEW_INTERVAL.as_secs()).unwrap_or(i64::MAX / 2);
            anyhow::ensure!(
                claim.keep_until(&state.pg_pool, until).await?,
                "the claim on command {} was taken over by another replica, which answers for it now",
                claim.id
            );
        }
        loop {
            let wake = count_at.map_or(breadcrumb, |count_at| count_at.min(breadcrumb));
            if heard.woken_before(wake, |channel, payload| WAKE_ON.iter().any(|w| w.hears(channel, payload))).await? {
                break;
            }
            if count_at.is_some_and(|count_at| Instant::now() >= count_at) {
                break;
            }
            tracing::info!(
                target: "weft_dispatcher::drain",
                project_id = %scope.project_id, reaching = ?scope.reaching, left,
                runs = ?reached.going_runs(),
                waited_secs = Instant::now().duration_since(started).as_secs(),
                "{}", scope.reaching.waiting_words()
            );
            breadcrumb += BREADCRUMB_EVERY;
        }
    }
}

/// Who hears what a drain waits for at each count ([`drain`]'s `say`): a
/// resync saying it on its task for the person following it.
#[async_trait::async_trait]
pub trait SayWaiting: Send + Sync {
    /// The runs on record it waits for now, oldest first.
    async fn going(&self, runs: Vec<weft_core::deactivation::RunGoing>) -> anyhow::Result<()>;
}

/// The runs among `runs` that are going (queued or driven), oldest first,
/// each with when it started.
pub(crate) async fn going_since(pool: &db::Pool, runs: &[RunId]) -> anyhow::Result<Vec<weft_core::deactivation::RunGoing>> {
    let rows: Vec<(RunId, i64)> = pool
        .all(
            &format!("SELECT r.run_id, r.started_at FROM run r WHERE r.run_id = ANY($1) AND {GOING} ORDER BY r.started_at, r.run_id"),
            &[&runs],
        )
        .await?;
    Ok(rows.into_iter().map(|(run_id, since_unix)| weft_core::deactivation::RunGoing { run_id, since_unix }).collect())
}

/// Cancel every run `reach` reaches, but the one that asked (`except`),
/// for `cause`: the ones going, and the paused ones too when `paused` (a
/// command that drops what is held). Answers how many going runs it
/// cancelled.
pub(crate) async fn cancel_reached(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    reach: &InfraReach,
    except: Option<RunId>,
    paused: bool,
    cause: &weft_core::run::CancelCause,
) -> anyhow::Result<u64> {
    let reached = crate::infra_reach::runs_reached(state, project_id, reach, except).await?;
    let targets: Vec<(RunId, &weft_core::run::CancelCause)> =
        reached.iter().filter(|run| paused || !run.paused).map(|run| (run.run_id, cause)).collect();
    crate::api::run::cancel_run_ids(state, &targets).await?;
    Ok(reached.iter().filter(|run| !run.paused).count() as u64)
}

/// The runs of `project_id` on `builds` (binary hashes) going right now,
/// queued or driven: what a resync waits for, or cancels, before its new
/// version takes over.
pub(crate) async fn going_on_builds(state: &DispatcherState, project_id: uuid::Uuid, builds: &[String]) -> anyhow::Result<Vec<RunId>> {
    Ok(state
        .pg_pool
        .scalars(
            &format!("SELECT r.run_id FROM run r WHERE r.project_id = $1 AND r.binary_hash = ANY($2) AND {GOING}"),
            &[&project_id, &builds],
        )
        .await?)
}

/// How many runs going right now `reach` reaches, but the one that asked,
/// as a report counts them: the ones on record, queued or going.
pub(crate) async fn going_runs(state: &DispatcherState, project_id: uuid::Uuid, reach: &InfraReach, except: Option<RunId>) -> anyhow::Result<u64> {
    let reached = crate::infra_reach::runs_reached(state, project_id, reach, except).await?;
    Ok(reached.iter().filter(|run| !run.paused).count() as u64)
}

impl Reaching<'_> {
    /// What a drain still waiting says in the log, every half minute.
    fn waiting_words(&self) -> &'static str {
        match self {
            Reaching::Infra { .. } => "infra waits for the runs reaching it to end; `weft stop <run>` ends one now, `weft infra cancel` calls the command off",
            Reaching::Fired { .. } => "a deactivate waits for the runs its triggers fired to end; `weft stop <run>` ends one now",
            Reaching::Builds(_) => {
                "a resync waits for the runs on the old version to end before the new one takes over; `weft stop <run>` ends one now, \
                 `weft cancel-activate` gives the resync up"
            }
            Reaching::EveryRun => "a removal waits for the project's runs to end",
        }
    }
}

/// What the workers count at their door that a drain waits on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtTheDoor {
    /// Everything (the whole project).
    Every,
    /// What they count for these entry tokens.
    Tokens(Vec<String>),
    /// Everything the workers of these builds (binary hashes) count.
    Builds(Vec<String>),
}

impl AtTheDoor {
    /// The `(tokens, builds)` the counting SQL filters on, `NULL` for any.
    fn filters(&self) -> (Option<&[String]>, Option<&[String]>) {
        match self {
            AtTheDoor::Every => (None, None),
            AtTheDoor::Tokens(tokens) => (Some(tokens), None),
            AtTheDoor::Builds(builds) => (None, Some(builds)),
        }
    }
}

/// What `scope` reaches right now: what its workers count at their door
/// for it, and the runs on record it waits for, paused or not.
struct Reached {
    door: AtTheDoor,
    runs: Vec<crate::infra_reach::RunReached>,
}

impl Reached {
    async fn now(state: &DispatcherState, scope: &DrainScope<'_>) -> anyhow::Result<Self> {
        let (reach, taken_down) = match scope.reaching {
            Reaching::Infra { reach, tokens } => (reach, tokens),
            Reaching::EveryRun => return Self::every_run(state, scope.project_id).await,
            Reaching::Fired { keys, tokens } => return Self::fired(state, scope.project_id, keys, tokens).await,
            Reaching::Builds(builds) => return Self::on_builds(state, scope.project_id, builds).await,
        };
        let project = state
            .projects
            .project(scope.project_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("project {} is gone", scope.project_id))?;
        let activations = state.activations.list(scope.project_id).await?;
        let keys: Vec<weft_core::activation::ActivationKey> =
            crate::infra_reach::activations_reached(reach, &activations, &project).into_iter().map(|a| a.key.clone()).collect();
        let mut tokens = crate::store::postgres::entry_tokens(&state.pg_pool, scope.project_id, &keys).await?;
        tokens.extend(taken_down.iter().cloned());
        tokens.sort();
        tokens.dedup();
        let runs = crate::infra_reach::runs_reached(state, scope.project_id, reach, None).await?;
        Ok(Self { door: AtTheDoor::Tokens(tokens), runs })
    }

    /// The runs `keys`' triggers fired, and what the workers count for
    /// them at the door (`tokens`).
    async fn fired(state: &DispatcherState, project_id: uuid::Uuid, keys: &[weft_core::activation::ActivationKey], tokens: &[String]) -> anyhow::Result<Self> {
        let target = crate::take_down::TakeDownTarget::Activations(keys.to_vec());
        let live = crate::take_down::live_runs(state, project_id).await?;
        let runs = crate::take_down::affected_runs(&target, &live, None)
            .into_iter()
            .map(|run| crate::infra_reach::RunReached { run_id: run.run_id, paused: run.suspended })
            .collect();
        Ok(Self { door: AtTheDoor::Tokens(tokens.to_vec()), runs })
    }

    /// Every run of the project on `builds` that has not ended, and
    /// everything those builds' workers count.
    async fn on_builds(state: &DispatcherState, project_id: uuid::Uuid, builds: &[String]) -> anyhow::Result<Self> {
        let rows: Vec<(RunId, bool)> = state
            .pg_pool
            .all(
                "SELECT run_id, state = 'parked' FROM run WHERE project_id = $1 AND state <> 'ended' AND binary_hash = ANY($2)",
                &[&project_id, &builds],
            )
            .await?;
        let runs = rows.into_iter().map(|(run_id, paused)| crate::infra_reach::RunReached { run_id, paused }).collect();
        Ok(Self { door: AtTheDoor::Builds(builds.to_vec()), runs })
    }

    /// Everything the project's workers count and every run of it that
    /// has not ended: its signals may be gone already (a removal deletes
    /// them first), and what its workers still drive is counted all the
    /// same.
    async fn every_run(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Self> {
        let runs = crate::take_down::live_runs(state, project_id)
            .await?
            .into_iter()
            .map(|run| crate::infra_reach::RunReached { run_id: run.run_id, paused: run.suspended })
            .collect();
        Ok(Self { door: AtTheDoor::Every, runs })
    }

    /// The runs it waits for, for the breadcrumb.
    fn going_runs(&self) -> Vec<RunId> {
        self.runs.iter().filter(|run| !run.paused).map(|run| run.run_id).collect()
    }

    fn all_runs(&self) -> Vec<RunId> {
        self.runs.iter().map(|run| run.run_id).collect()
    }
}

/// How much of what a drain reaches is going right now (see the module
/// doc): what the live workers of `project_id` count at their door for
/// it (`door`), plus the runs among `runs` on record that are queued or
/// driven.
pub async fn going(pool: &db::Pool, project_id: uuid::Uuid, door: &AtTheDoor, runs: &[RunId]) -> anyhow::Result<i64> {
    const ALIVE: &str = weft_task_store::worker_alive!("l");
    let (tokens, builds) = door.filters();
    let counted: i64 = pool
        .scalar(
            &format!(
                "SELECT COALESCE(SUM(f.n::bigint), 0)::bigint FROM worker_lease l \
                 CROSS JOIN LATERAL jsonb_each_text(l.in_flight) AS f(token, n) \
                 WHERE l.project_id = $1 AND {ALIVE} AND ($2::text[] IS NULL OR f.token = ANY($2)) \
                   AND ($3::text[] IS NULL OR l.binary_hash = ANY($3))"
            ),
            &[&project_id, &tokens, &builds],
        )
        .await?;
    let on_record: i64 =
        pool.scalar(&format!("SELECT COUNT(*) FROM run r WHERE r.run_id = ANY($1) AND {GOING}"), &[&runs]).await?;
    Ok(counted + on_record)
}

/// A run on record that is queued for a worker or being driven by one.
const GOING: &str = concat!("(r.state = 'queued' OR ", weft_task_store::in_flight_sql!("r"), ")");

/// Mark watched every run among `runs` that is going, or
/// paused and so able to go again (`run.watch_end`): its ending then
/// announces itself (`weft_journal::RUN_ENDED_CHANNEL`), so the drain
/// counts again when it ends rather than on a timer, a paused run that
/// resumed meanwhile included. Marked when a drain looks, never on a run's
/// own path.
pub async fn mark_watched(pool: &db::Pool, project_id: uuid::Uuid, runs: &[RunId]) -> anyhow::Result<()> {
    pool.execute(
        &format!(
            "UPDATE run r SET watch_end = TRUE \
             WHERE r.project_id = $1 AND r.run_id = ANY($2) AND NOT r.watch_end AND (r.state = 'parked' OR {GOING})"
        ),
        &[&project_id, &runs],
    )
    .await?;
    Ok(())
}

/// When what a drain reaches (`door` and `runs`, as [`going`] reads them)
/// could next stop counting as going without
/// anything announcing it, in unix seconds: the earliest lease of a live
/// worker counting some of it at its door (a busy worker renews it every
/// second, one whose runs only wait vouched for itself until then, and a
/// lapsed one counts for nothing), and the earliest moment a run of it on
/// record stops being held by its owner (`weft_task_store::run_held_until_sql!`:
/// its owner's lease, or the grace of a run just handed to a worker whose
/// first tick has not landed). The drain counts again then.
pub async fn workers_look(pool: &db::Pool, project_id: uuid::Uuid, door: &AtTheDoor, runs: &[RunId]) -> anyhow::Result<Option<i64>> {
    const ALIVE: &str = weft_task_store::worker_alive!("l");
    const HELD: &str = weft_task_store::run_held_until_sql!("r");
    let (tokens, builds) = door.filters();
    Ok(pool
        .scalar(
            &format!(
                "SELECT MIN(at) FROM ( \
                     SELECT MIN(l.leased_until_unix) AS at FROM worker_lease l \
                     WHERE l.project_id = $1 AND {ALIVE} AND ($4::text[] IS NULL OR l.binary_hash = ANY($4)) \
                       AND EXISTS (SELECT 1 FROM jsonb_each_text(l.in_flight) AS f(token, n) \
                                   WHERE f.n::bigint > 0 AND ($2::text[] IS NULL OR f.token = ANY($2))) \
                     UNION ALL \
                     SELECT MIN({HELD}) FROM run r \
                     WHERE r.run_id = ANY($3) AND r.state = 'running' AND {HELD} >= EXTRACT(EPOCH FROM NOW())::BIGINT \
                 ) looks"
            ),
            &[&project_id, &tokens, &runs, &builds],
        )
        .await?)
}
