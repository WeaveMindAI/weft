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
//! run on their safety tick alone, and only while something is in motion
//! ([`in_motion`]): a lease nobody holds cannot lapse, so a quiet install
//! looks at nothing and a dispatcher that scales to zero stays there.

use std::time::Duration;

use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// The channel a queued terminate sweep announces on (no payload: the
/// reaper reads the whole queue), from the `storage_sweep_notify_on_insert`
/// trigger in `storage::GROUP`.
pub const STORAGE_SWEEP_CHANNEL: &str = "weft_storage_sweep";

const NOTHING: &[WakeOn] = &[];
pub(crate) static ON_PARKED_FIRE: &[WakeOn] = &[WakeOn::any(weft_task_store::parked_fires::PARKED_FIRE_CHANNEL)];
pub(crate) static ON_STORAGE_SWEEP: &[WakeOn] = &[WakeOn::any(STORAGE_SWEEP_CHANNEL)];
/// A trigger's activation changing status, whether it takes work, or until
/// when: a hibernation starting has a grace window to end.
pub(crate) static ON_HIBERNATION: &[WakeOn] = &[WakeOn::any(crate::holders::HELD_SIGNALS_CHANNEL)];
/// An infra copy coming, going or changing status: one running again
/// brings back the triggers that went down with it.
pub(crate) static ON_INFRA_STATUS: &[WakeOn] = &[WakeOn::any(crate::held::INFRA_STATUS_CHANNEL)];

/// Safety tick of the reapers that are woken by a write: 60 seconds in
/// real time, at this install's pace (`weft_core::time_scale`).
fn woken_reaper_safety() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(60))
}

/// Whether anything in the install is in motion: a task of the
/// dispatcher's claimed, a run queued for a worker or being driven by one,
/// an activation or a build under way, a lifecycle command not finished.
/// While nothing is, no lease can lapse and no driver can die mid-way, so
/// the loops that watch for that have nothing to watch. A run parked on a
/// person keeps nothing here, so it does not keep the install awake.
pub async fn in_motion(pool: &sqlx::PgPool) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM task WHERE status = 'claimed') \
             OR EXISTS (SELECT 1 FROM run WHERE state = 'running') \
             OR EXISTS (SELECT 1 FROM run WHERE state = 'queued') \
             OR EXISTS (SELECT 1 FROM trigger_activation WHERE status IN ('activating', 'deactivating')) \
             OR EXISTS (SELECT 1 FROM project WHERE transition <> 'none') \
             OR EXISTS (SELECT 1 FROM infra_lifecycle_command WHERE completed_at_unix IS NULL)",
    )
    .fetch_one(pool)
    .await?)
}

/// `inner`, looked at again at its safety interval after it ran dry for as
/// long as anything is in motion ([`in_motion`]), and left to sleep until
/// a write wakes it once nothing is.
pub fn while_in_motion(state: &DispatcherState, inner: DrainLoop) -> DrainLoop {
    let state = state.clone();
    let (drain, safety) = (inner.drain.clone(), inner.safety);
    DrainLoop::new(inner.name, inner.wake_on, safety, move || {
        let (state, drain) = (state.clone(), drain.clone());
        async move {
            match drain().await? {
                DrainStep::Done if in_motion(&state.pg_pool).await? => Ok(DrainStep::RetryIn(safety)),
                step => Ok(step),
            }
        }
    })
}

/// Every reaper, as the loops the dispatcher runs.
pub fn drain_loops(state: &DispatcherState) -> Vec<DrainLoop> {
    let mut loops = vec![
        // Silence detectors: nothing announces a lease that lapsed.
        timed(state, Duration::from_secs(30), "removed_projects", |s| async move { sweep_removed_projects(&s).await }),
        timed(state, Duration::from_secs(3600), "tasks", sweep_tasks),
        while_in_motion(state, timed(state, Duration::from_secs(30), "stuck_transitions", sweep_stuck_transitions)),
        timed(state, Duration::from_secs(3600), "retired_rows", sweep_retired_rows),
        timed(state, Duration::from_secs(300), "ghost_infra_leases", |s| async move {
            crate::infra_owner::release_ghost_leases(&s.pg_pool).await
        }),
        // The public edge's counters: minutes that no longer count, and
        // slots of runs that never started.
        while_in_motion(
            state,
            timed(state, Duration::from_secs(60), "entry_rate", |s| async move {
                let now = crate::lease::now_unix();
                crate::entry_limits::sweep(&s.pg_pool, now).await?;
                crate::worker_door::sweep(&s.pg_pool, now).await
            }),
        ),
        // A run whose worker went away: nothing announces a lease that
        // lapsed.
        while_in_motion(
            state,
            timed(state, Duration::from_secs(15), "lost_runs", |s| async move {
                crate::worker_door::sweep_lost_runs(&s, crate::lease::now_unix()).await
            }),
        ),
        // Queued events whose trigger takes them (a hand-over that failed
        // retries after its backoff stamp): a newly parked event wakes it
        // at once; otherwise it sleeps until the earliest head is due.
        woken(state, ON_PARKED_FIRE, "parked_fires", |s| async move { crate::parked_drain::drain_due(&s).await }),
        // Storage plane: the durable terminate sweep (un-kept exec files of
        // a terminated execution). The queue deletes an execution's row only after
        // the broker confirms the sweep; a transient broker failure leaves
        // it and asks for another look soon. The kept-file expiry sweep is
        // the broker's own loop (it owns the bucket + metadata).
        woken(state, ON_STORAGE_SWEEP, "storage_sweep", crate::storage::process_sweep_queue),
        // A hibernation's grace window ending: its triggers stop listening
        // and taking work. Sleeps until the next one ends.
        woken(state, ON_HIBERNATION, "hibernations", end_hibernations),
        // An infra copy running again: the triggers its stop took down
        // with it come back.
        woken(state, ON_INFRA_STATUS, "infra_returns", crate::api::infra::bring_back_triggers_whose_infra_returned),
    ];
    // A machine's fronts are its own containers: one that went down is put
    // back, on a free port when another program took its own (and the
    // project's address follows). A cloud's front is the platform's to keep.
    if state.project_ports.is_some() {
        loops.push(timed(state, Duration::from_secs(30), "fronts", |s| async move { crate::front::serve_all(&s, crate::front::Say::Changes).await }));
    }
    loops
}

/// End every hibernation whose grace window has passed: the listener lets
/// go of its signals (`crate::listener::let_go_of_stopped`), and then its
/// activation stops taking work (`crate::activation_store::end_grace_windows`).
/// In that order, so one whose letting go failed (the listener down, a
/// signal it cannot tear down yet) stays for the next pass, which lets go
/// again, and holds back no other. Then sleep until the next window ends,
/// or until an activation changes.
async fn end_hibernations(state: DispatcherState) -> anyhow::Result<DrainStep> {
    let mut by_project: std::collections::BTreeMap<uuid::Uuid, Vec<weft_core::activation::ActivationKey>> = Default::default();
    for (project_id, key) in crate::activation_store::grace_windows_over(&state.pg_pool).await? {
        by_project.entry(project_id).or_default().push(key);
    }
    let mut kept_any = false;
    for (project_id, keys) in by_project {
        let signals = crate::journal::postgres::activation_signals(&state.pg_pool, project_id, &keys).await?;
        let kept = crate::listener::let_go_of_stopped(&state.pg_pool, &state.listener, &signals).await?;
        let let_go: Vec<weft_core::activation::ActivationKey> = keys
            .into_iter()
            .filter(|key| {
                !signals.iter().any(|s| {
                    kept.contains(&s.token)
                        && s.activation_trigger.as_deref() == Some(key.trigger.as_str())
                        && s.instance.as_ref() == key.instance()
                })
            })
            .collect();
        kept_any |= !kept.is_empty();
        let ended = crate::activation_store::end_grace_windows(&state.pg_pool, project_id, &let_go).await?;
        if ended > 0 {
            tracing::info!(
                target: "weft_dispatcher::reaper",
                %project_id, triggers = ended,
                "a hibernation's grace window ended: its triggers stopped listening and take no more work"
            );
            // Nothing of the project may take work any more: its front goes.
            crate::front::let_go_if_idle(&state, project_id).await?;
        }
    }
    if kept_any {
        return Ok(DrainStep::RetryIn(HIBERNATION_LET_GO_RETRY));
    }
    let now = crate::lease::now_unix();
    Ok(match crate::activation_store::next_grace_end(&state.pg_pool).await? {
        None => DrainStep::Done,
        Some(deadline) => DrainStep::RetryIn(Duration::from_secs((deadline + 1 - now).max(1) as u64)),
    })
}

/// How soon a hibernation whose signals the listener did not let go of is
/// tried again.
const HIBERNATION_LET_GO_RETRY: Duration = Duration::from_secs(30);

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
pub(crate) fn woken<F, Fut>(state: &DispatcherState, wake_on: &'static [WakeOn], name: &'static str, sweep: F) -> DrainLoop
where
    F: Fn(DispatcherState) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = anyhow::Result<DrainStep>> + Send + 'static,
{
    let state = state.clone();
    DrainLoop::new(name, wake_on, woken_reaper_safety(), move || {
        let state = state.clone();
        let sweep = sweep.clone();
        async move {
            // A sibling holding the lock is sweeping right now, but it may
            // have read before the write this wake is for, so look again
            // shortly (`LOCK_HELD_RETRY`).
            Ok(sweep_alone(&state, name, || sweep(state.clone()))
                .await?
                .unwrap_or(DrainStep::RetryIn(weft_task_store::drain::LOCK_HELD_RETRY)))
        }
    })
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

/// Clear what removed projects left behind: the dispatcher's work queued
/// for them, and the signals the listener still holds for them (a
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

/// Delete the pending tasks of projects that no longer exist; returns how
/// many.
pub async fn drop_work_of_removed_projects(pool: &sqlx::PgPool) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM task t \
         WHERE t.status = 'pending' AND t.project_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM project p WHERE p.id = t.project_id)",
    )
    .execute(pool)
    .await?
    .rows_affected())
}

/// Run one sweep while holding the reaper's install-wide lock, or skip
/// it (`None`) while a sibling replica holds it. The lock is held on the
/// lock pool: every sweep starts at once when the process boots, and with
/// the locks on the work pool, more sweeps than it has connections each
/// held one and waited for another, until every request of the process
/// timed out.
async fn sweep_alone<T, F, Fut>(state: &DispatcherState, name: &str, sweep: F) -> anyhow::Result<Option<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    crate::lease::with_advisory_lock(
        &state.lock_pool,
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
