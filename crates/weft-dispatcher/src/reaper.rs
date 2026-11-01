//! Background reapers that sweep stale rows. Every copy of the
//! dispatcher runs all of these, and each sweep runs under its own
//! install-wide lease (`weft_task_store::locks::with_lease`, named
//! after the reaper), so one copy sweeps at a time and a sibling that finds
//! the lease held skips that turn: N copies cost one sweep, not N. The
//! writes stay idempotent underneath (delete-by-key is a no-op the second
//! time, task reclaim is conditional), so a sweep that overlaps a request
//! doing the same work is harmless.
//!
//! Each reaper is a `DrainLoop` that sleeps until a write it cares about
//! is announced, and on a machine also looks on a slow safety tick. A
//! machine's fronts are the one sweep on a timer alone: nothing announces
//! a container that went down, and only a machine has them. What lapses
//! with nobody announcing it is `crate::lapses`'s.

use std::time::Duration;

use async_trait::async_trait;

use weft_task_store::db::Client as _;
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};
use weft_task_store::locks::Leased;

use crate::state::DispatcherState;

/// The channel a run queued for its files' deletion announces on (no payload: the
/// reaper reads the whole queue), from the `storage_sweep_notify_on_insert`
/// trigger in `storage::GROUP`.
pub const STORAGE_SWEEP_CHANNEL: &str = "weft_storage_sweep";

const NOTHING: &[WakeOn] = &[];
pub(crate) static ON_PARKED_FIRE: &[WakeOn] = &[WakeOn::any(weft_task_store::parked_fires::PARKED_FIRE_CHANNEL)];
pub(crate) static ON_STORAGE_SWEEP: &[WakeOn] = &[WakeOn::any(STORAGE_SWEEP_CHANNEL)];
/// A trigger's activation changing status, whether it takes work, or until
/// when: a trigger taken down with `doorForMinutes` has a window to end.
pub(crate) static ON_DOOR_WINDOW: &[WakeOn] = &[WakeOn::any(crate::holders::HELD_SIGNALS_CHANNEL)];
/// An infra copy coming, going or changing status, or an infra command
/// ending: a copy running again (or a take-down called off) brings back the
/// triggers that went down with it, and the events that waited on it. An
/// activation changing status or a held signal going: a
/// trigger refused because another claim held it comes back once that
/// claim ends. Either one: a resync stopped waiting on it goes on.
pub(crate) static ON_INFRA_RETURNS: &[WakeOn] = &[
    WakeOn::any(crate::held::INFRA_STATUS_CHANNEL),
    WakeOn::any(crate::holders::HELD_SIGNALS_CHANNEL),
    // A take-down command ending (a cancel, a failure) with its copies
    // still up brings back what went down with them.
    WakeOn::any(weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL),
];

/// Safety tick of the reapers that are woken by a write: 60 seconds in
/// real time (`DrainLoop::new` runs it at this install's pace).
const WOKEN_REAPER_SAFETY: Duration = Duration::from_secs(60);

/// Every reaper, as the loops the dispatcher runs.
pub fn drain_loops(state: &DispatcherState) -> Vec<DrainLoop> {
    let mut loops = vec![
        // Queued events whose trigger takes them, when their clock comes
        // due: a newly parked event wakes it at once; otherwise it sleeps
        // until the earliest head is due, which a head stamped elsewhere
        // books (`crate::parked_drain::book`).
        woken(state, ON_PARKED_FIRE, crate::parked_drain::PARKED_FIRES, "hand queued trigger events to their runs", |s| async move {
            crate::parked_drain::drain_due(&s).await
        }),
        // Storage plane: the files of the runs whose record went. The
        // queue deletes a run's row only after the broker confirms the
        // files are gone; a broker that keeps failing fails the pass, and
        // the next run erased tries every row again. The deadline sweep is
        // the broker's own loop (it owns the bucket + metadata).
        woken(state, ON_STORAGE_SWEEP, "storage_sweep", "delete the files of erased runs", crate::storage::process_sweep_queue),
        // A door's window ending: its triggers stop listening and taking
        // work. Sleeps until the next one ends.
        woken(state, ON_DOOR_WINDOW, DOOR_WINDOWS, DOOR_WINDOWS_LABEL, close_door_windows),
        // An infra copy running again: the triggers its stop took down
        // with it come back, and the events that waited on it go.
        woken(state, ON_INFRA_RETURNS, INFRA_RETURNS, "bring back triggers whose infra returned", crate::api::infra::bring_back_triggers_whose_infra_returned),
        // The same wakes take up again a resync stopped on one of those
        // changes. A loop of its own, so the look booked for a resync
        // stopped on the database stands whatever else fails on these
        // wakes: a failed pass keeps no look of its own.
        woken(state, ON_INFRA_RETURNS, RESYNC_RETURNS, "take up again the resyncs that stopped waiting on infra, an activation or the database", |s| async move {
            Ok(crate::task_kinds::resync::retry_stopped(&s).await?.map_or(DrainStep::Done, DrainStep::RetryIn))
        }),
    ];
    // A machine's fronts are its own containers: one that went down is put
    // back, on a free port when another program took its own (and the
    // project's address follows). A cloud's front is the platform's to keep.
    if state.project_ports.is_some() {
        loops.push(timed(state, Duration::from_secs(30), "fronts", "put the projects' fronts back", |s| async move { crate::front::serve_all(&s, crate::front::Say::Changes).await }));
    }
    loops
}

/// The loop that brings back what waited on infra or on an activation, by
/// name.
pub(crate) const INFRA_RETURNS: &str = "infra_returns";

/// The loop that takes up again a resync stopped on infra, an activation
/// or the database (`crate::task_kinds::resync::retry_stopped`), by name:
/// a resync that stops waiting on the database books its look.
pub(crate) const RESYNC_RETURNS: &str = "resync_returns";

/// The door-windows loop, by name.
pub(crate) const DOOR_WINDOWS: &str = "door_windows";

/// Close every door whose window has passed ([`close_door_windows_over`],
/// the listener letting go of its signals through
/// `crate::listener::let_go_of_stopped`); a project left with nothing
/// taking work lets go of its front. Then sleep until the next window
/// ends, or until an activation changes.
async fn close_door_windows(state: DispatcherState) -> anyhow::Result<DrainStep> {
    for project_id in close_door_windows_over(&state.pg_pool, &ListenerOf(&state)).await? {
        // Nothing of the project may take work any more: its front goes.
        crate::front::let_go_if_idle(&state, project_id).await?;
    }
    let now = crate::lease::now_unix();
    Ok(match crate::activation_store::next_door_window_end(&state.pg_pool).await? {
        None => DrainStep::Done,
        Some(deadline) => DrainStep::RetryIn(Duration::from_secs((deadline + 1 - now).max(1) as u64)),
    })
}

/// What closing a door window asks of the listener. [`ListenerOf`] in
/// production; the database tests answer it themselves.
#[async_trait]
pub trait DoorWindowListener: Send + Sync {
    /// Let go of `signals`, answering the ones kept, each with why
    /// (`crate::listener::let_go_of_stopped`).
    async fn let_go(&self, signals: &[crate::store::SignalRegistration]) -> anyhow::Result<Vec<(String, String)>>;
    /// The fresh places of the entry signals `signals` whose kind keeps one
    /// (`crate::listener::ListenerClient::fresh_places`).
    async fn fresh_places(&self, signals: &[crate::store::SignalRegistration]) -> anyhow::Result<Vec<crate::store::FreshPlace>>;
}

/// The install's own listener.
pub struct ListenerOf<'a>(pub &'a DispatcherState);

#[async_trait]
impl DoorWindowListener for ListenerOf<'_> {
    async fn let_go(&self, signals: &[crate::store::SignalRegistration]) -> anyhow::Result<Vec<(String, String)>> {
        crate::listener::let_go_of_stopped(&self.0.pg_pool, &self.0.listener, signals).await
    }

    async fn fresh_places(&self, signals: &[crate::store::SignalRegistration]) -> anyhow::Result<Vec<crate::store::FreshPlace>> {
        self.0.listener.fresh_places(signals).await
    }
}

/// How many times a door window's close is asked again when a wake moved
/// one of its rows in between; running out means its wakes never stop
/// claiming.
const CLOSE_ROUNDS: usize = 5;

/// Close every door whose window has passed: the listener lets go of its
/// signals and answers the ones it kept (with why), then its activation
/// stops taking work and its entry signals lose their place in what they
/// read, in one write (`crate::activation_store::close_door_windows`),
/// whatever the listener answered to the let-go: a fire that still
/// arrives is refused by the activation, and a holder hears it stop taking
/// work (`crate::holders::HELD_SIGNALS_CHANNEL`). So nothing that arrived
/// once the window ended is caught up when the trigger listens again. A
/// project whose fresh places cannot be had (the listener cannot be
/// reached) keeps its window open by the flag, refused by the clock all
/// the same, and fails this pass, naming it; the others close.
/// A signal the listener kept is marked on its own row
/// (`crate::listener::note_let_go`) and the listener is told again at each
/// pass until it lets go (or its trigger takes work again); while any is
/// marked, its project's `weft status` names it (`Callee::LetGo`). Answers the
/// projects whose windows ended.
pub async fn close_door_windows_over(pool: &weft_task_store::db::Pool, listener: &dyn DoorWindowListener) -> anyhow::Result<Vec<uuid::Uuid>> {
    use crate::activation_store::WindowsClosed;
    let again = crate::store::postgres::signals_kept_after_let_go(pool).await?;
    if !again.is_empty() {
        let kept = listener.let_go(&again).await?;
        crate::listener::note_let_go(pool, &again, &kept).await?;
    }
    let mut by_project: std::collections::BTreeMap<uuid::Uuid, Vec<weft_core::activation::ActivationKey>> = Default::default();
    for (project_id, key) in crate::activation_store::door_windows_over(pool).await? {
        by_project.entry(project_id).or_default().push(key);
    }
    let mut ended_in = Vec::new();
    let mut not_closed = Vec::new();
    for (project_id, keys) in by_project {
        let mut signals = crate::store::postgres::activation_signals(pool, project_id, &keys).await?;
        let kept = listener.let_go(&signals).await?;
        crate::listener::note_let_go(pool, &signals, &kept).await?;
        let mut round = 0;
        let ended = loop {
            round += 1;
            let entries: Vec<crate::store::SignalRegistration> = signals.iter().filter(|signal| !signal.is_resume).cloned().collect();
            let places = match listener.fresh_places(&entries).await {
                Ok(places) => places,
                Err(e) => break Err(format!("project {project_id}: the places its doors take: {e:#}")),
            };
            match crate::activation_store::close_door_windows(pool, project_id, &keys, &places).await? {
                WindowsClosed::Closed(ended) => break Ok(ended),
                WindowsClosed::StateMoved if round < CLOSE_ROUNDS => {
                    signals = crate::store::postgres::activation_signals(pool, project_id, &keys).await?;
                }
                WindowsClosed::StateMoved => {
                    break Err(format!(
                        "project {project_id}: its signals' state moved under each of {CLOSE_ROUNDS} closes in a row: their wakes keep claiming them"
                    ))
                }
            }
        };
        let ended = match ended {
            Ok(ended) => ended,
            Err(why) => {
                not_closed.push(why);
                continue;
            }
        };
        if ended > 0 {
            tracing::info!(
                target: "weft_dispatcher::reaper",
                %project_id, triggers = ended,
                "a door's window ended: its triggers stopped listening and take no more work"
            );
            ended_in.push(project_id);
        }
    }
    let kept: Vec<(uuid::Uuid, String, String)> = pool
        .all("SELECT project_id, node_id, let_go_failed FROM signal WHERE let_go_failed IS NOT NULL ORDER BY project_id, node_id", &[])
        .await?;
    let mut by_project: std::collections::BTreeMap<uuid::Uuid, Vec<String>> = Default::default();
    for (project, node, why) in kept {
        by_project.entry(project).or_default().push(format!("node {node}: {why}"));
    }
    let failing: Vec<uuid::Uuid> = by_project.keys().copied().collect();
    weft_task_store::unanswered::still_failing(pool, weft_task_store::unanswered::PerProject::LetGo, &failing).await?;
    for (project, named) in by_project {
        tracing::error!(
            target: "weft_dispatcher::reaper",
            %project, signals = ?named,
            "the listener kept serving signals whose door closed; their activations take no work, so what they pick up \
             is refused"
        );
        weft_task_store::unanswered::failed(pool, weft_task_store::unanswered::Callee::LetGo(project), None, &named.join("; ")).await?.log();
    }
    anyhow::ensure!(not_closed.is_empty(), "door windows that ended could not close: {}", not_closed.join("; "));
    Ok(ended_in)
}

/// What the door-windows loop does, in a person's words.
const DOOR_WINDOWS_LABEL: &str = "close the doors whose window passed";

/// A sweep that runs on its interval alone (a machine's only). `interval`
/// is given in real time and runs at this install's pace
/// (`DrainLoop::new` scales it).
fn timed<F, Fut>(state: &DispatcherState, interval: Duration, name: &'static str, label: &'static str, sweep: F) -> DrainLoop
where
    F: Fn(DispatcherState) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let state = state.clone();
    DrainLoop::new(name, label, NOTHING, interval, move || {
        let state = state.clone();
        let sweep = sweep.clone();
        async move {
            // Held by a sibling: it is sweeping now, and this tick is skipped.
            sweep_alone(&state, name, || sweep(state.clone())).await?;
            Ok(DrainStep::Done)
        }
    })
}

/// A sweep that runs when one of `wake_on` is announced, and on a slow
/// safety tick. The body's step says whether to look again early.
pub(crate) fn woken<F, Fut>(state: &DispatcherState, wake_on: &'static [WakeOn], name: &'static str, label: &'static str, sweep: F) -> DrainLoop
where
    F: Fn(DispatcherState) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = anyhow::Result<DrainStep>> + Send + 'static,
{
    let state = state.clone();
    DrainLoop::new(name, label, wake_on, WOKEN_REAPER_SAFETY, move || {
        let state = state.clone();
        let sweep = sweep.clone();
        async move {
            // A sibling holding the lease is sweeping right now, but it may
            // have read before the write this wake is for, so this looks
            // again once the sibling's lease would lapse. Not sooner: a
            // sweep may run for minutes, and a look every moment would
            // keep a cloud install's dispatcher woken for all of it. A
            // sibling still up heard the same write, and its loop sweeps
            // again once this sweep ends (`drain::run` keeps what it heard
            // while draining); the look at the lapse is for a sibling that
            // died holding it.
            Ok(match sweep_alone(&state, name, || sweep(state.clone())).await? {
                Leased::Ran(step) => step,
                Leased::HeldFor(lapses_in) => DrainStep::RetryIn(lapses_in),
            })
        }
    })
}

/// Run one sweep as the only run of it install-wide, or skip it while a
/// sibling replica runs it (`Leased::HeldFor`, how long until its lease
/// would lapse). A lease row, never a held connection:
/// a sweep may run for as long as a trigger's setup takes and take locks
/// of its own on the lock pool, so a lock held for the sweep would hold a
/// lock connection for nothing the whole time, and with every sweep
/// starting at once as the process boots, a lock pool as small as the
/// database's share allows would leave none for the locks the sweeps
/// themselves wait on.
async fn sweep_alone<T, F, Fut>(state: &DispatcherState, name: &str, sweep: F) -> anyhow::Result<Leased<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    weft_task_store::locks::with_lease(&state.pg_pool, &format!("sweep/{name}"), sweep).await
}
