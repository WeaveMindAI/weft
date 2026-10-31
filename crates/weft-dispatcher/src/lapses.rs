//! Noticing that whoever held a piece of work went away.
//!
//! Most work announces itself: a task written, a run queued, a command
//! issued. What nothing announces is a holder dying: a worker process whose
//! lease stops being renewed under the runs it drives, a task that ended
//! with an activation it drove still activating, a build that stops
//! beating, a task whose claim rested on a run that ended with nobody left
//! to take the claim back.
//! Only time tells, so this loop looks when the earliest of those could
//! have lapsed ([`earliest_lapse`]) and repairs what did:
//!
//! - a run whose worker went away is queued again, or ends
//!   (`crate::worker_door::sweep_lost_runs`);
//! - a build transition nothing holds lands at rest
//!   (`crate::transition::settle`);
//! - an activation whose task ended without landing it is ended, keeping
//!   what its triggers kept
//!   (`crate::api::project::end_unfinished_activation`). One whose task's
//!   claimant died is not this loop's: the claim is rescued by the picker,
//!   and the task carries the activation on;
//! - a task claim that rested on a run, long past that run's ending, goes
//!   back to be claimed again (a resync's among them, whose task then
//!   brings back what it took down: `crate::task_kinds::resync`).
//!
//! Each repair and the look it books read the same condition, so a hold is
//! looked at exactly when its repair would act on it. A repair that fails
//! is written down for `weft status` and tried again a few times, a little
//! later each time ([`REPAIR_RETRIES`]); then it waits for the next look a
//! real event books. The next look is booked whatever failed. The same
//! look prunes the rows the workers' door tick leaves behind.
//!
//! It is woken when a worker's lease comes alive
//! (`weft_task_store::worker_door::LEASE_ALIVE_CHANNEL`, once per stretch
//! of a worker's work), and by the broker when a worker bore a run before
//! its lease was ever written: a run born at that worker's door is covered
//! from then on, though no dispatcher loop saw it begin. Every look books the
//! next at the earliest lapse there is; so while workers work it looks
//! about once a lease length, a quiet worker that vouched for itself moves
//! it to the vouch's end, and once nothing holds anything it books nothing.
//! What starts a holder that no lease announces (an activation's claim, a
//! build's driver, a run handed to a worker that has not ticked yet) books
//! a look itself ([`book_after`]). The claims the picker, the delivery and the
//! lifecycle claimer make are theirs to rescue: each looks again when its
//! own earliest claim lapses.

use std::time::Duration;

use anyhow::Context;
use weft_task_store::db::{Client as _, Pool};
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};
use weft_task_store::unanswered::{self, Callee};

use crate::state::DispatcherState;

/// The loop's name, which the loops that book it name.
pub const LAPSES: &str = "lapses";

/// When a repair that keeps failing is tried again, in seconds after its
/// first failure (the start of its stretch, `Callee::Repairs`): a couple
/// of short retries, then none until a real event books a look.
pub const REPAIR_RETRIES: [i64; 2] = [5, 25];

/// A repair of one project's work that failed, and why: written down for
/// that project's `weft status` (`Callee::Repairs`), never for another
/// tenant's. The loop books its next look whatever failed, so its own
/// pass never fails for them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairFailed {
    pub project_id: uuid::Uuid,
    pub error: String,
}

/// A worker's lease coming alive wakes the loop.
pub(crate) static WAKE_ON: &[WakeOn] = &[WakeOn::any(weft_task_store::worker_door::LEASE_ALIVE_CHANNEL)];

/// How often a machine's process looks anyway, in real time
/// (`DrainLoop::new` runs it at this install's pace).
const SAFETY: Duration = Duration::from_secs(15);

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new(LAPSES, "notice work whose holder went away", WAKE_ON, SAFETY, move || {
        let state = state.clone();
        async move { look(&state).await }
    })
    .booked_by(&["run_ends", "delivery"])
}

/// One look: every repair, what failed written down, then the next look
/// at the earliest lapse, or sooner for a repair that failed and is still
/// in its retries.
async fn look(state: &DispatcherState) -> anyhow::Result<DrainStep> {
    let now = crate::lease::now_unix();
    let mut failed = crate::worker_door::sweep_lost_runs(state, now).await?;
    failed.extend(end_orphaned_activations(state).await?);
    crate::transition::settle(state, None).await?;
    let handed_back = hand_back_resting_claims(&state.pg_pool, now).await?;
    if handed_back > 0 {
        tracing::warn!(target: "weft_dispatcher::lapses", handed_back, "tasks whose claimant went away while they waited on a run are claimable again");
        weft_task_store::announce::committed(&state.pg_pool);
    }
    weft_task_store::worker_door::prune(&state.pg_pool, now).await?;
    let retry = write_down(&state.pg_pool, failed).await?;
    let lapse = earliest_lapse(&state.pg_pool, now).await?.map(|at| at + 1 - now);
    Ok(match [lapse, retry].into_iter().flatten().min() {
        Some(after) => DrainStep::RetryIn(Duration::from_secs(after.max(1).unsigned_abs())),
        None => DrainStep::Done,
    })
}

/// Write down what this look could not repair, by project, and clear what
/// it repaired of every other project. Answers in how many seconds the
/// failed repairs are next tried again ([`REPAIR_RETRIES`], counted from
/// the start of each project's stretch of failures), `None` when none is
/// still in its retries.
async fn write_down(pool: &Pool, failed: Vec<RepairFailed>) -> anyhow::Result<Option<i64>> {
    let mut by_project: std::collections::BTreeMap<uuid::Uuid, Vec<String>> = std::collections::BTreeMap::new();
    for repair in failed {
        by_project.entry(repair.project_id).or_default().push(repair.error);
    }
    let failing: Vec<uuid::Uuid> = by_project.keys().copied().collect();
    unanswered::still_failing(pool, unanswered::PerProject::Repairs, &failing).await.context("clear the repairs that went through")?;
    let mut retry: Option<i64> = None;
    for (project, errors) in by_project {
        // A stretch not written down yet begins with this failure.
        let failing_for = unanswered::failing_for(pool, project)
            .await
            .context("read how long a repair has been failing")?
            .into_iter()
            .find(|failing| failing.what == unanswered::Failed::Repairs)
            .map_or(0, |stretch| (stretch.as_of_ms - stretch.since_ms) / 1000);
        let error = errors.join("; ");
        let written = match next_retry(failing_for) {
            Some(after) => {
                retry = Some(retry.map_or(after, |sooner| sooner.min(after)));
                unanswered::failed(pool, Callee::Repairs(project), None, &error).await
            }
            None => unanswered::gave_up(pool, Callee::Repairs(project), None, &error).await,
        };
        written.context("write down a repair that failed")?.log();
    }
    Ok(retry)
}

/// In how many seconds a repair failing for `failing_for` seconds is tried
/// again ([`REPAIR_RETRIES`]), `None` once its retries are spent.
fn next_retry(failing_for: i64) -> Option<i64> {
    REPAIR_RETRIES
        .iter()
        .map(|&at| weft_core::time_scale::scaled_secs(at))
        .find(|&at| at > failing_for)
        .map(|at| at - failing_for)
}

/// When the earliest hold this loop watches could lapse (unix seconds),
/// read through the very condition its repair acts on: a running run's
/// owner holding it (`crate::worker_door::lost_run!`), a driver beating
/// alone for a transition (`crate::project_store::held_by_driver_alone`),
/// a task claim resting on a run that ended ([`resting_claim_lapses_at`]).
/// An activation's task claimed and renewed is looked at when its claim
/// would lapse: by then its claimant renewed it, or the task ended (and
/// an activation it left activating is ended:
/// `crate::activation_store::orphaned`), or its claimant died and the
/// picker rescues the claim. One resting, or waiting to be claimed, is
/// looked at when that changes. `None` when nothing is held. Holds already
/// past are what the look before repaired, so only those ahead count, and
/// a look always books ahead.
pub async fn earliest_lapse(pool: &Pool, now: i64) -> anyhow::Result<Option<i64>> {
    let margin = crate::worker_door::lost_margin_secs();
    let stale = crate::transition::heartbeat_stale_secs();
    let rest = weft_task_store::tasks::claim_duration_secs();
    pool.scalar::<Option<i64>>(
        &format!(
            "SELECT MIN(at) FROM ( \
                 SELECT MIN({held}) + $2 AS at FROM run r WHERE r.state = 'running' AND NOT ({lost}) \
                 UNION ALL SELECT MIN(driver.claimed_until_unix) FROM trigger_activation a \
                     JOIN task driver ON driver.id = a.driver_task \
                     WHERE a.status = 'activating' AND driver.status = 'claimed' \
                 UNION ALL SELECT MIN(p.transition_heartbeat_unix) + $3 FROM project p WHERE {transition} \
                 UNION ALL SELECT MIN({claim}) FROM {RESTING_CLAIMS} \
             ) lapses WHERE at >= $1",
            held = weft_task_store::run_held_until_sql!("r"),
            lost = crate::worker_door::lost_run!("$1 - $2"),
            transition = crate::project_store::held_by_driver_alone("p"),
            claim = resting_claim_lapses_at("$4"),
        ),
        &[&now, &margin, &stale, &rest],
    )
    .await
    .context("read the earliest lapse")
}

/// End every activation whose task ended without landing it
/// (`crate::activation_store::orphaned`,
/// `crate::api::project::end_unfinished_activation`); answers the ones
/// that could not be, each with why.
async fn end_orphaned_activations(state: &DispatcherState) -> anyhow::Result<Vec<RepairFailed>> {
    let mut failed = Vec::new();
    for stuck in state.activations.list_orphaned().await? {
        tracing::warn!(
            target: "weft_dispatcher::lapses",
            project_id = %stuck.project_id,
            activation = %stuck.run_id,
            "activation orphaned (the task driving it ended without landing it); ending it"
        );
        if let Err((code, msg)) = crate::api::project::end_unfinished_activation(
            state,
            stuck.project_id,
            stuck.run_id,
            // The task driving the activation ended without landing it,
            // and this look is repairing the row; nothing superseded the
            // run and no person stopped it.
            &weft_core::run::CancelCause::Runtime {
                detail: "the task turning these triggers on ended without finishing; weft ended the activation".into(),
            },
        )
        .await
        {
            tracing::warn!(
                target: "weft_dispatcher::lapses",
                project_id = %stuck.project_id,
                code = %code,
                error = %msg,
                "ending an orphaned activation failed"
            );
            failed.push(RepairFailed {
                project_id: stuck.project_id,
                error: format!("an activation whose task ended could not be ended ({msg}); `weft cancel-activate` ends it"),
            });
            continue;
        }
        crate::transition::publish_transition_changed(state, stuck.project_id).await;
    }
    Ok(failed)
}

/// When a task claim resting on a run lapses, as a SQL expression over the
/// task `t` and its run left-joined as `r`: `rest` (a claim's length)
/// after the run ended, at once (0) when the run is gone, never (NULL)
/// while it goes. Past it its claimant, which takes the claim back as its
/// run ends, died.
fn resting_claim_lapses_at(rest: &str) -> String {
    format!("CASE WHEN r.run_id IS NULL THEN 0 WHEN r.state = 'ended' THEN r.ended_at + {rest} END")
}

/// The task claims resting on a run, `t` with its run left-joined as `r`.
const RESTING_CLAIMS: &str =
    "task t LEFT JOIN run r ON r.run_id = t.claim_rests_on WHERE t.status = 'claimed' AND t.claim_rests_on IS NOT NULL";

/// Put back to be claimed every task whose resting claim lapsed
/// ([`resting_claim_lapses_at`]). Answers how many. Announced as a task
/// ready by the table's own trigger, through the outbox its caller pokes.
async fn hand_back_resting_claims(pool: &Pool, now: i64) -> anyhow::Result<u64> {
    pool.execute(
        &format!(
            "UPDATE task SET status = 'pending', claimed_by = NULL, claimed_until_unix = NULL, claim_rests_on = NULL \
             WHERE id IN (SELECT t.id FROM {RESTING_CLAIMS} AND {} < $1)",
            resting_claim_lapses_at("$2")
        ),
        &[&now, &weft_task_store::tasks::claim_duration_secs()],
    )
    .await
    .context("hand back the claims whose claimant went away")
}

/// Book a look `after` seconds from now, unless one is booked by then:
/// for whoever starts a hold no lease announces. Only when the dispatcher
/// scales to zero; a machine's loop looks on its own. A booking that
/// cannot be written fails: a loop's pass fails with it (its tick is
/// tried again), and a request logs it ([`book_after_or_log`]).
pub async fn book_after(state: &DispatcherState, after: i64) -> anyhow::Result<()> {
    let Some(booking) = &state.booking else { return Ok(()) };
    booking
        .book_sooner_after(LAPSES, Duration::from_secs(after.max(0).unsigned_abs() + 1))
        .await
        .map_err(|e| e.context("book the look at lapsed holds"))
}

/// [`book_after`] for a caller outside any loop's pass (a request, a
/// driver's beat), which has nothing to fail: a booking that cannot be
/// written is logged, and the next write that wakes the loop, or the next
/// look booked, covers it.
pub async fn book_after_or_log(state: &DispatcherState, after: i64) {
    if let Err(e) = book_after(state, after).await {
        tracing::error!(target: "weft_dispatcher::lapses", error = %format!("{e:#}"), "could not book the look at lapsed holds");
    }
}

#[cfg(test)]
mod tests {
    use super::{next_retry, REPAIR_RETRIES};

    /// A repair that keeps failing is tried again at each of its retries,
    /// counted from its first failure, and then not on a clock.
    #[test]
    fn a_failing_repair_is_retried_a_few_times_then_waits_for_an_event() {
        let at = |n: usize| weft_core::time_scale::scaled_secs(REPAIR_RETRIES[n]);
        assert_eq!(next_retry(0), Some(at(0)), "first failure: the first retry");
        assert_eq!(next_retry(at(0)), Some(at(1) - at(0)), "the first retry failed: the second");
        assert_eq!(next_retry(at(1)), None, "the last retry failed: no more on a clock");
        assert_eq!(next_retry(at(1) + 3600), None);
    }
}
