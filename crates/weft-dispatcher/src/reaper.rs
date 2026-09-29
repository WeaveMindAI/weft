//! Background reapers that sweep stale rows. Every copy of the
//! dispatcher runs all of these, and each sweep runs under its own
//! install-wide advisory lock (`lease::REAPER_DOMAIN`, keyed by the
//! reaper's name), so one copy sweeps at a time and a sibling that finds
//! the lock held skips that turn: N copies cost one sweep, not N. The
//! writes stay idempotent underneath (delete-by-key is a no-op the second
//! time, task reclaim is conditional), so a sweep that overlaps a request
//! doing the same work is harmless.
//!
//! Two kinds of reaper, both `DrainLoop`s. The ones that react to a write
//! sleep until that write is announced, with a slow safety tick for what
//! no write announces. The ones that notice SILENCE (a lease that lapsed,
//! a transition whose driver died) cannot be woken by anything, so they
//! run on their safety tick alone.

use std::time::Duration;

use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// The channel a signal row notifies on when a fire is parked on it,
/// with its project id as the payload, from the
/// `signal_parked_fire_notify_on_grow` trigger in `journal::postgres::GROUP`.
pub const PARKED_FIRE_CHANNEL: &str = "weft_parked_fire";

/// The channel a queued terminate sweep notifies on, with its execution as
/// the payload, from the `storage_sweep_notify_on_insert` trigger in
/// `storage::GROUP`.
pub const STORAGE_SWEEP_CHANNEL: &str = "weft_storage_sweep";

/// The longest the parked-fire sweep sleeps between looks, whatever the
/// queues say: it also releases the drain claims a dead copy left, which
/// nothing announces. 30 seconds in real time, at this install's pace
/// (`weft_core::time_scale`).
fn parked_fire_longest_sleep() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(30))
}

const NOTHING: &[WakeOn] = &[];
const ON_PARKED_FIRE: &[WakeOn] = &[WakeOn::any(PARKED_FIRE_CHANNEL)];
const ON_STORAGE_SWEEP: &[WakeOn] = &[WakeOn::any(STORAGE_SWEEP_CHANNEL)];

/// Safety tick of the reapers that are woken by a write: 60 seconds in
/// real time, at this install's pace (`weft_core::time_scale`).
fn woken_reaper_safety() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(60))
}

/// Every reaper, as the loops the dispatcher runs.
pub fn drain_loops(state: &DispatcherState) -> Vec<DrainLoop> {
    vec![
        // Silence detectors: nothing announces a lease that lapsed.
        timed(state, Duration::from_secs(30), "removed_projects", |s| async move { sweep_removed_projects(&s).await }),
        timed(state, Duration::from_secs(3600), "tasks", sweep_tasks),
        timed(state, Duration::from_secs(30), "stuck_transitions", sweep_stuck_transitions),
        timed(state, Duration::from_secs(3600), "retired_rows", sweep_retired_rows),
        timed(state, Duration::from_secs(30), "orphaned_live_executions", sweep_orphaned_live_executions),
        timed(state, Duration::from_secs(60), "stale_cancels", |s| async move {
            let dropped = weft_task_store::tasks::drop_stale_cancels(&s.pg_pool).await?;
            if dropped > 0 {
                tracing::info!(target: "weft_dispatcher::reaper", dropped, "dropped cancels whose execution nothing drives any more");
            }
            Ok(())
        }),
        timed(state, Duration::from_secs(300), "ghost_infra_leases", |s| async move {
            crate::infra_owner::release_ghost_leases(&s.pg_pool).await
        }),
        // The public edge's counters: minutes that no longer count, and
        // slots of runs that never started.
        timed(state, Duration::from_secs(60), "entry_rate", |s| async move {
            crate::entry_limits::sweep(&s.pg_pool, crate::lease::now_unix()).await
        }),
        // Re-parked fires (a route that failed) retry with a backoff stamp
        // on the element; this is what drives the retry once the stamp is
        // due. A newly parked fire wakes it at once; otherwise it sleeps
        // until the earliest head is due.
        woken(state, ON_PARKED_FIRE, "parked_fires", |s| async move {
            crate::api::project::drain_due_parked_fires(&s).await?;
            let now = crate::lease::now_unix();
            let next = crate::api::project::next_parked_fire_due(&s.pg_pool).await?;
            Ok(DrainStep::RetryIn(parked_fire_sleep(now, next)))
        }),
        // Storage plane: the durable terminate sweep (un-kept exec files of
        // a terminated execution). The queue deletes an execution's row only after
        // the broker confirms the sweep; a transient broker failure leaves
        // it for the safety tick. The kept-file expiry sweep is the
        // broker's own loop (it owns the bucket + metadata).
        woken(state, ON_STORAGE_SWEEP, "storage_sweep", |s| async move {
            crate::storage::process_sweep_queue(s).await.map(|()| DrainStep::Done)
        }),
    ]
}

/// A sweep that runs on its interval alone. `interval` is given in real
/// time and runs at this install's pace (`weft_core::time_scale`), like
/// the leases these sweeps judge.
fn timed<F, Fut>(state: &DispatcherState, interval: Duration, name: &'static str, sweep: F) -> DrainLoop
where
    F: Fn(DispatcherState) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let state = state.clone();
    DrainLoop::new(name, NOTHING, weft_core::time_scale::scaled(interval), move || {
        let state = state.clone();
        let sweep = sweep.clone();
        async move {
            sweep_alone(&state, name, || sweep(state.clone())).await?;
            Ok(DrainStep::Done)
        }
    })
}

/// A sweep that runs when one of `wake_on` is announced, and on a slow
/// safety tick. The body's step says whether to look again early.
fn woken<F, Fut>(state: &DispatcherState, wake_on: &'static [WakeOn], name: &'static str, sweep: F) -> DrainLoop
where
    F: Fn(DispatcherState) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = anyhow::Result<DrainStep>> + Send + 'static,
{
    let state = state.clone();
    DrainLoop::new(name, wake_on, woken_reaper_safety(), move || {
        let state = state.clone();
        let sweep = sweep.clone();
        async move {
            // A sibling holding the lock is sweeping right now; what it
            // misses of this wake, its own next look or this one's safety
            // tick covers.
            Ok(sweep_alone(&state, name, || sweep(state.clone())).await?.unwrap_or(DrainStep::Done))
        }
    })
}

/// How long the parked-fire sweep sleeps: until the earliest queued head
/// is due, at least a second (a head due now that did not drain was
/// re-stamped, or is claimed by a live drain) and at most
/// [`parked_fire_longest_sleep`].
fn parked_fire_sleep(now_unix: i64, next_due_unix: Option<i64>) -> Duration {
    let longest = parked_fire_longest_sleep();
    match next_due_unix {
        None => longest,
        Some(due) => Duration::from_secs((due - now_unix).max(1) as u64).min(longest),
    }
}

/// Drop what a removed project left behind that no surviving run needs.
///
/// The removal itself already tries this, and `weft clean` tries again
/// when it deletes a run; both are best-effort, because neither may fail
/// over rows nobody reads. Once the project row is gone no request can
/// reach those rows again (`weft rm` refuses a project it cannot find,
/// and with no runs left there is nothing to clean), so without this loop
/// one transient failure would leave them for good, unlisted and
/// undeletable. Hourly: nothing here is urgent, and a project whose runs
/// all survive is a no-op.
async fn sweep_retired_rows(state: DispatcherState) -> anyhow::Result<()> {
    let mut orphans = state.projects.projects_with_orphan_definitions().await?;
    orphans.extend(state.versions.projects_with_orphan_versions().await?);
    // Executions whose project is gone: the erase at removal is
    // best-effort, so a transient failure there lands here.
    orphans.extend(state.journal.projects_with_orphan_executions().await?);
    orphans.sort();
    orphans.dedup();
    let mut failed = 0usize;
    for project in orphans {
        match state.journal.delete_project_executions(project).await {
            Ok(0) => {}
            Ok(erased) => tracing::info!(
                target: "weft_dispatcher::reaper",
                project_id = %project, executions = erased,
                "erased the executions of a project that is gone"
            ),
            Err(e) => {
                failed += 1;
                tracing::warn!(
                    target: "weft_dispatcher::reaper",
                    project_id = %project, error = %e,
                    "could not erase this removed project's executions; the next sweep tries again"
                );
            }
        }
        // Per project, because recovering from a failure on ONE is the
        // whole reason this loop exists. Propagating the first error
        // abandoned every project after it in id order, every hour, for
        // good.
        match crate::api::project::retire_what_no_run_needs(&state, project).await {
            Ok(0) => {}
            Ok(dropped) => tracing::info!(
                target: "weft_dispatcher::reaper",
                project_id = %project, rows = dropped,
                "retired rows of a removed project that no surviving run needs"
            ),
            Err(e) => {
                failed += 1;
                tracing::warn!(
                    target: "weft_dispatcher::reaper",
                    project_id = %project, error = %e,
                    "could not retire this removed project's rows; the next sweep tries again"
                );
            }
        }
    }
    if failed > 0 {
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            projects = failed,
            "some removed projects' rows could not be retired this cycle"
        );
    }
    Ok(())
}

/// Clear what removed projects left behind: the work queued for their
/// workers, and the signals the listener still holds for them (a
/// registration no project can fire or take down). None of it can do
/// anything once the project row is gone. `weft rm` runs this as soon as
/// the row is gone; the loop catches work queued in the moment of the
/// removal.
pub(crate) async fn sweep_removed_projects(state: &DispatcherState) -> anyhow::Result<()> {
    let dropped = drop_work_of_removed_projects(&state.pg_pool).await?;
    if dropped > 0 {
        tracing::info!(
            target: "weft_dispatcher::reaper",
            dropped,
            "dropped work queued for removed projects"
        );
    }
    let signals = crate::journal::postgres::remove_signals_of_removed_projects(&state.pg_pool).await?;
    if !signals.is_empty() {
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            removed = signals.len(),
            "removed the signals of projects that no longer exist"
        );
        state.listener.unregister_many(&signals).await;
    }
    Ok(())
}

/// Delete the pending worker tasks of projects that no longer exist;
/// returns how many.
pub async fn drop_work_of_removed_projects(pool: &sqlx::PgPool) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM task t \
         WHERE t.status = 'pending' AND t.target = 'worker' AND t.project_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM project p WHERE p.id = t.project_id)",
    )
    .execute(pool)
    .await?
    .rows_affected())
}

/// Run one sweep while holding the reaper's install-wide lock, or skip
/// it (`None`) while a sibling replica holds it.
async fn sweep_alone<T, F, Fut>(state: &DispatcherState, name: &str, sweep: F) -> anyhow::Result<Option<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    crate::lease::with_advisory_lock(
        &state.pg_pool,
        crate::lease::advisory_key(crate::lease::REAPER_DOMAIN, name),
        sweep,
    )
    .await
}

/// Stuck-transition reaper: the crash-recovery half of the project
/// transitional-state model. A transition driven in-process by one
/// dispatcher copy (an activation window, a build) carries
/// a heartbeat the driver bumps; when the driver dies, the heartbeat
/// goes stale and this sweep repairs the row per-transition,
/// status-guarded (safe under N copies; a live driver's row is never
/// touched because its heartbeat is fresh):
///
///   - an activation stuck `activating` -> the same wipe the activate
///     rollback / cancel-activate performs (end the claim + cancel the
///     leaked TriggerSetup execution + drop half-registered signals).
///   - stuck `building` / `cancelling_build` -> clear the marker; the
///     build died with its dispatcher (or keeps running harmlessly to
///     a content-addressed tag); the next verb rebuilds or cache-hits.
///   - `deactivating` -> re-drive the drain-watcher CAS: a
///     deactivation whose terminal events were missed (dispatcher
///     restart between the last execution finishing and the CAS)
///     lands at Inactive here. No heartbeat needed: the check itself
///     is idempotent and cheap.
async fn sweep_stuck_transitions(state: DispatcherState) -> anyhow::Result<()> {
    let stale_before = crate::lease::now_unix() - crate::transition::heartbeat_stale_secs();
    for stuck in state.projects.list_stuck_transitions(stale_before).await? {
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            project_id = %stuck.id,
            transition = stuck.transition.as_str(),
            "build transition orphaned (driver heartbeat stale); clearing marker"
        );
        if state.projects.finish_building(stuck.id).await? {
            crate::transition::publish_transition_changed(&state, stuck.id).await;
        }
    }
    for stuck in state.activations.list_stuck(stale_before).await? {
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            project_id = %stuck.project_id,
            activation = %stuck.execution_id,
            "activation orphaned (driver heartbeat stale); wiping activating state"
        );
        if let Err((code, msg)) = crate::api::project::wipe_activating_state(
            &state,
            stuck.project_id,
            stuck.execution_id,
            // The activation's driver died mid-transition and this sweep
            // is repairing the row; nothing superseded the run and no
            // person stopped it.
            &weft_core::exec::CancelCause::Runtime {
                detail: "the activation's driver died mid-transition; the reaper wiped its state".into(),
            },
        )
        .await
        {
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                project_id = %stuck.project_id,
                code = %code,
                error = %msg,
                "wipe of orphaned activation failed; retrying next sweep"
            );
            continue;
        }
        crate::transition::publish_transition_changed(&state, stuck.project_id).await;
    }
    // Deactivating landings. Two steps, both idempotent, both the
    // SAME building blocks every other drain path uses:
    //   1. If the user's drain cap expired ("wait at most N, then
    //      proceed"), cancel what the activation still waits on via the
    //      ONE cancel helper (`cancel_running_for`).
    //   2. Re-drive the ONE landing CAS (`try_finish_drain`); it flips
    //      Deactivating -> Inactive iff nothing it waits on runs. This
    //      also covers deactivations whose terminal events were missed
    //      across a restart.
    let now = crate::lease::now_unix();
    for (project_id, key) in state.activations.list_deactivating().await? {
        let deadline = state
            .activations
            .list(project_id)
            .await?
            .into_iter()
            .find(|a| a.key == key)
            .and_then(|a| a.lifecycle.drain_deadline_unix);
        if deadline.is_some_and(|deadline| now >= deadline) {
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                %project_id,
                trigger = %key,
                "deactivation drain cap expired; cancelling remaining executions"
            );
            if let Err((code, msg)) = crate::api::project::cancel_running_for(
                &state,
                project_id,
                std::slice::from_ref(&key),
                weft_core::exec::CancelCause::User,
            )
            .await
            {
                tracing::warn!(
                    target: "weft_dispatcher::reaper",
                    %project_id,
                    code = %code,
                    error = %msg,
                    "drain-cap cancel failed; retrying next sweep"
                );
                continue;
            }
        }
        crate::journal_bridge::try_finish_drain(&state, project_id, &key, None).await?;
    }
    Ok(())
}

/// Live executions whose worker went away (`tasks::orphaned_live_executions`):
/// the caller was on THAT worker's connection, so the run cannot resume
/// anywhere else, and its execution is terminally cancelled. The task row
/// is the durable retry handle: anything not fully recovered this tick is
/// re-found next tick.
async fn sweep_orphaned_live_executions(state: DispatcherState) -> anyhow::Result<()> {
    let orphans = weft_task_store::tasks::orphaned_live_executions(&state.pg_pool).await?;
    for orphan in orphans {
        let Ok(execution_id) = orphan.execution_id.parse::<weft_core::ExecutionId>() else {
            // Corrupt execution: leave the task as evidence, surface loud.
            tracing::error!(
                target: "weft_dispatcher::reaper",
                execution_id = %orphan.execution_id, task = %orphan.task_id,
                "orphaned live execution has an unparseable execution; leaving its task for inspection"
            );
            continue;
        };
        // Record the cancel through THE cancel, THEN delete the task. It
        // skips the terminal if one already exists for the execution (the
        // worker wrote its ending, then died before its task flipped),
        // writes `NodeCancelled` per still-running node, and queues no
        // task. On failure the task stays, so the next tick retries (the
        // write is idempotent).
        if let Err(e) = crate::api::execution::cancel_execution_id(
            &state,
            execution_id,
            &weft_core::exec::CancelCause::Runtime {
                detail: "the worker running this live execution went away before it completed; the \
                         caller's connection was on that worker and is gone, so the run cannot resume \
                         elsewhere"
                    .into(),
            },
        )
        .await
        {
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                execution_id = %execution_id, error = %e,
                "failed to record cancel terminal for orphan; task kept, will retry next tick"
            );
            continue;
        }
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            execution_id = %execution_id,
            "live execution orphaned by a worker that went away; recorded ExecutionCancelled (caller is gone)"
        );
        if let Err(e) = weft_task_store::tasks::delete_task(&state.pg_pool, orphan.task_id).await {
            // The cancel is durably recorded, so a leftover task only means
            // a harmless retry next tick (re-record is a no-op).
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                execution_id = %execution_id, error = %e,
                "failed to delete cancelled orphan task; harmless, next tick retries"
            );
        }
    }
    Ok(())
}

/// Tasks-table retention sweep. Once an hour, delete terminal
/// rows older than the retention window so the table stays small.
async fn sweep_tasks(state: DispatcherState) -> anyhow::Result<()> {
    let n = weft_task_store::tasks::sweep_terminal(&state.pg_pool).await?;
    if n > 0 {
        tracing::info!(
            target: "weft_dispatcher::reaper",
            swept = n,
            "tasks sweeper retired terminal rows"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_parked_fire_sweep_sleeps_until_the_next_head_is_due_within_bounds() {
        assert_eq!(parked_fire_sleep(100, Some(107)), Duration::from_secs(7));
        assert_eq!(parked_fire_sleep(100, Some(100)), Duration::from_secs(1), "due now: look again shortly");
        assert_eq!(parked_fire_sleep(100, Some(50)), Duration::from_secs(1));
        assert_eq!(parked_fire_sleep(100, Some(10_000)), parked_fire_longest_sleep());
        assert_eq!(parked_fire_sleep(100, None), parked_fire_longest_sleep(), "nothing queued");
    }
}
