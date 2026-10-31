//! Handing queued runs to the project's workers.
//!
//! A run the dispatcher starts (`weft run`, a setup run) and every run that
//! comes back (an answer reached it while parked, its worker handed it
//! back, its worker went away while it was durable) waits as a `queued`
//! run row. This loop takes queued runs ([`take`], which also marks each
//! delivered for a while so no sibling hands it out twice) and calls a
//! worker for each: `POST /_weft/run/<run_id>` on the project's
//! workers at the run's image. The worker claims the run (its row goes
//! `running`, owned by that worker, its epoch raised) and drives it from
//! its record. A worker that never claims leaves the delivery to run out,
//! and the run is handed out again.
//!
//! The call is held open until the worker answers (the run ended or waits
//! on something outside it), on a task of its own so one run never holds
//! up the next delivery. On a platform that stops a worker nobody is
//! calling (Cloud Run), that held call is what keeps the run's worker up.
//! The database is listened to only around this side's own reads and
//! writes, never for the length of the call: a run that waits a day for a
//! person leaves the database free to sleep meanwhile.

use std::time::Duration;

use anyhow::Context;
use weft_core::net::EmptyBody;
use weft_core::RunId;
use weft_task_store::db::{self, Client, FromRow, Pool, Row};
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// How many runs one pass takes. More waiting means the loop comes
/// straight back for them.
const BATCH: i64 = 32;

/// A run being queued wakes the loop (runs whose tries stopped come back
/// that way when a build of their project registers, [`retry_stopped`]),
/// and so does a project's worker levers changing: the write that changes
/// them makes the project's waiting runs takeable again, their tries
/// forgotten (`crate::project_store::ProjectStoreOps::set_worker_overrides`).
/// A worker's lease coming alive (once per stretch of its work) wakes it
/// too, only when its project has runs whose tries stopped
/// ([`WORKERS_BACK_CHANNEL`]): those are taken again ([`take`]).
pub(crate) static WAKE_ON: &[WakeOn] = &[
    WakeOn::any(weft_task_store::runs::RUN_QUEUED_CHANNEL),
    WakeOn::any(crate::held::WORKER_SETTINGS_CHANNEL),
    WakeOn::any(WORKERS_BACK_CHANNEL),
];

/// The channel that says a project's workers may take its runs again, with
/// the project as the payload: a worker's lease coming alive while the
/// project has queued runs whose tries stopped ([`UNREACHED_TO_STOP`]). A
/// trigger on `worker_lease` in the run group's schema
/// (`crate::store::postgres::GROUP`) sends it.
// SYNC: WORKERS_BACK_CHANNEL <-> 'weft_workers_back' in worker_lease_workers_back_notify (crate::store::postgres::GROUP)
pub const WORKERS_BACK_CHANNEL: &str = "weft_workers_back";

/// How long a run handed to a worker stays out of the queue for it to
/// claim: 30 seconds at this install's pace. One unclaimed by then is
/// handed out again.
fn delivery_lease_secs() -> i64 {
    weft_core::time_scale::scaled_secs(30)
}

/// How long a run whose worker is still starting waits before it is
/// handed out again, joining the same bring-up: twice as long as a cloud
/// holds a caller while a worker comes up, at this install's pace.
pub fn starting_wait_secs() -> i64 {
    weft_core::time_scale::scaled_secs(40)
}

/// How long a run waits before it is handed out again after each try its
/// workers did not take (a refusal, a hand-off that got no answer) but the
/// last: the last refusal ends it ([`REFUSALS_TO_END`]), the last
/// hand-off with no answer stops its tries ([`UNREACHED_TO_STOP`]).
pub const RETRY_WAITS: [i64; 2] = [5, 20];

/// How many refusals end a run.
pub const REFUSALS_TO_END: i16 = RETRY_WAITS.len() as i16 + 1;

/// How many hand-offs that got no answer stop a run's tries: it stays
/// queued, untaken, until something that can fix it happens (a worker of
/// its project comes alive, a build of the project registers, its worker
/// levers change), each of which starts its tries again.
// SYNC: UNREACHED_TO_STOP <-> the 3 in worker_lease_workers_back_notify (crate::store::postgres::GROUP)
pub const UNREACHED_TO_STOP: i16 = RETRY_WAITS.len() as i16 + 1;

/// One queued run taken for delivery.
#[derive(Debug)]
pub struct Delivery {
    pub run_id: RunId,
    pub project_id: uuid::Uuid,
    pub tenant_id: String,
    /// The image it runs on; `None` for a run of no program, which no
    /// worker can claim.
    pub binary_hash: Option<String>,
}

impl FromRow for Delivery {
    fn from_row(row: Row) -> db::Result<Self> {
        Ok(Self {
            run_id: row.try_get("run_id")?,
            project_id: row.try_get("project_id")?,
            tenant_id: row.try_get("tenant_id")?,
            binary_hash: row.try_get("binary_hash")?,
        })
    }
}

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("delivery", "hand queued runs to workers", WAKE_ON, weft_task_store::drain::SAFETY_POLL_INTERVAL, move || {
        let state = state.clone();
        async move {
            let now = crate::lease::now_unix();
            let taken = take(&state.pg_pool, now, BATCH).await?;
            let full = taken.len() as i64 == BATCH;
            for delivery in taken {
                let state = state.clone();
                tokio::spawn(async move { deliver(&state, delivery).await });
            }
            if full {
                return Ok(DrainStep::More);
            }
            // A run handed out and never claimed, or waiting to be tried
            // again, is handed out once its wait runs out, which nothing
            // announces.
            Ok(match next_hand_out(&state.pg_pool).await? {
                Some(at) => DrainStep::RetryIn(Duration::from_secs((at + 1 - now).max(1).unsigned_abs())),
                None => DrainStep::Done,
            })
        }
    })
}

/// Take up to `limit` queued runs to hand out, oldest first, and mark each
/// out with its delivery until a lease from `now` runs out: a run whose
/// wait passed, and a run whose tries stopped ([`UNREACHED_TO_STOP`]) once
/// a worker of its project came alive in the second they stopped or later
/// (`worker_lease.alive_since`, whole seconds like the stop: a worker
/// alive in that same second may have come up after the stop, so it
/// counts), whose tries start again, their busy stretch with them, while
/// `run.waited_on_workers` (its workers failed it) stays until it is
/// claimed. A delivery that never comes back (its process died) leaves its mark to
/// run out, and the run is taken again then.
pub async fn take(pool: &Pool, now: i64, limit: i64) -> anyhow::Result<Vec<Delivery>> {
    Ok(pool
        .all(
            "UPDATE run SET delivered_until = $1::bigint + $2::bigint, waits = FALSE, \
                 unreached = CASE WHEN unreached >= $4 THEN 0 ELSE unreached END, \
                 busy_since = CASE WHEN unreached >= $4 THEN NULL ELSE busy_since END \
             WHERE run_id IN ( \
                 SELECT r.run_id FROM run r \
                 WHERE r.state = 'queued' \
                   AND ((r.unreached < $4 AND (r.delivered_until IS NULL OR r.delivered_until < $1)) \
                        OR (r.unreached >= $4 AND EXISTS (SELECT 1 FROM worker_lease l \
                                                          WHERE l.project_id = r.project_id AND l.alive_since >= r.delivered_until))) \
                 ORDER BY r.started_at LIMIT $3 FOR UPDATE SKIP LOCKED) \
             RETURNING run_id, project_id, tenant_id, binary_hash",
            &[&now, &delivery_lease_secs(), &limit, &UNREACHED_TO_STOP],
        )
        .await?)
}

/// When the earliest queued run still tried is due to be handed out again
/// (unix seconds): one out with a delivery whose lease runs out, or one
/// waiting to be tried again. A run whose tries stopped is due on no clock.
pub async fn next_hand_out(pool: &Pool) -> anyhow::Result<Option<i64>> {
    Ok(pool
        .scalar::<Option<i64>>("SELECT MIN(delivered_until) FROM run WHERE state = 'queued' AND unreached < $1", &[&UNREACHED_TO_STOP])
        .await?)
}

/// How long a run a busy worker turned away first waits before it is
/// handed out again: 5 seconds at this install's pace, about as long as
/// the worker's own `Retry-After`.
pub fn busy_wait_secs() -> i64 {
    weft_core::time_scale::scaled_secs(5)
}

/// The longest a run a busy worker turned away waits before it is handed
/// out again: a minute at this install's pace.
pub fn busy_wait_cap_secs() -> i64 {
    weft_core::time_scale::scaled_secs(60)
}

/// Give back a queued run whose worker is still starting, to be handed
/// out again once [`starting_wait_secs`] passed, joining the same
/// bring-up, counting no try. Not announced: the loop looks again when
/// the earliest such wait runs out.
pub async fn wait_for_worker(pool: &Pool, run_id: RunId) -> anyhow::Result<()> {
    pool.execute(
        "UPDATE run SET delivered_until = $2, waits = TRUE WHERE run_id = $1 AND state = 'queued'",
        &[&run_id, &(crate::lease::now_unix() + starting_wait_secs())],
    )
    .await?;
    Ok(())
}

/// Give back a queued run its worker answered busy, its busy stretch
/// starting now unless one is going (`run.busy_since`, which a claim
/// clears). It is handed out again after a pause as long as the stretch
/// so far, at least [`busy_wait_secs`] and at most
/// [`busy_wait_cap_secs`], and for as long as the workers answer busy: it
/// is real work waiting for room. Answers how long the stretch has lasted,
/// in seconds, `None` when it is no longer queued.
pub async fn busy(pool: &Pool, run_id: RunId) -> anyhow::Result<Option<i64>> {
    Ok(pool
        .scalar_opt(
            "UPDATE run SET busy_since = COALESCE(busy_since, $2::bigint), waits = TRUE, waited_on_workers = TRUE, \
                 delivered_until = $2::bigint + LEAST(GREATEST($2::bigint - COALESCE(busy_since, $2::bigint), $3::bigint), $4::bigint) \
             WHERE run_id = $1 AND state = 'queued' RETURNING $2::bigint - busy_since",
            &[&run_id, &crate::lease::now_unix(), &busy_wait_secs(), &busy_wait_cap_secs()],
        )
        .await?)
}

/// What `weft status` shows for a project whose workers answered a run
/// busy for `secs` seconds.
fn busy_for_words(secs: i64) -> String {
    let lasted = match secs / 60 {
        0 => format!("{secs} seconds"),
        1 => "1 minute".to_string(),
        minutes => format!("{minutes} minutes"),
    };
    format!(
        "this project's worker has answered busy for {lasted} (memory full); raise its memory (`weft workers set --memory <size>`)"
    )
}

/// Count one more refusal of a queued run by its workers: it is given back
/// to be tried again after the wait that refusal earns ([`RETRY_WAITS`]).
/// Answers how many refusals it has now, `None` when it is no longer
/// queued; the [`REFUSALS_TO_END`]th ends it ([`refused_cause`]).
pub async fn refused(pool: &Pool, run_id: RunId) -> anyhow::Result<Option<i16>> {
    Ok(pool
        .scalar_opt(
            "UPDATE run SET refusals = refusals + 1, waits = TRUE, waited_on_workers = TRUE, \
                 delivered_until = $2::bigint + (($3::bigint[])[LEAST(refusals + 1, array_length($3::bigint[], 1))]) \
             WHERE run_id = $1 AND state = 'queued' RETURNING refusals",
            &[&run_id, &crate::lease::now_unix(), &RETRY_WAITS.map(weft_core::time_scale::scaled_secs).to_vec()],
        )
        .await?)
}

/// Count one more hand-off of a queued run that got no answer from its
/// workers or the platform: it is given back to be tried again after the
/// wait that try earns ([`RETRY_WAITS`]), and the [`UNREACHED_TO_STOP`]th
/// stops its tries, its `delivered_until` then saying when on the
/// database's clock (the clock a worker's `alive_since` is on), until
/// something that can fix it starts them again ([`take`],
/// [`retry_stopped`], `crate::project_store::ProjectStoreOps::set_worker_overrides`).
/// Answers how many it has now, `None` when it is no longer queued (its
/// worker claimed it before the call broke).
pub async fn unreached(pool: &Pool, run_id: RunId) -> anyhow::Result<Option<i16>> {
    Ok(pool
        .scalar_opt(
            "UPDATE run SET unreached = unreached + 1, waits = TRUE, waited_on_workers = TRUE, \
                 delivered_until = CASE WHEN unreached + 1 >= $3::smallint THEN EXTRACT(EPOCH FROM NOW())::BIGINT \
                     ELSE $2::bigint + (($4::bigint[])[unreached + 1]) END \
             WHERE run_id = $1 AND state = 'queued' RETURNING unreached",
            &[&run_id, &crate::lease::now_unix(), &UNREACHED_TO_STOP, &RETRY_WAITS.map(weft_core::time_scale::scaled_secs).to_vec()],
        )
        .await?)
}

/// Start again the tries of `project_id`'s queued runs that stopped
/// because its workers could not be reached ([`UNREACHED_TO_STOP`]), in
/// the caller's transaction, announcing them
/// queued so the delivery takes them: for a build of the project
/// registering, which can fix that. `run.waited_on_workers` (their
/// workers failed them) stays until each is claimed.
pub async fn retry_stopped(conn: &impl Client, project_id: uuid::Uuid) -> anyhow::Result<()> {
    let retried = conn
        .execute(
            "UPDATE run SET unreached = 0, busy_since = NULL, waits = FALSE, delivered_until = NULL \
             WHERE project_id = $1 AND state = 'queued' AND unreached >= $2",
            &[&project_id, &UNREACHED_TO_STOP],
        )
        .await?;
    if retried > 0 {
        weft_journal::record::notify_queued_in(conn, project_id).await?;
    }
    Ok(())
}

/// What a run refused [`REFUSALS_TO_END`] times is ended for, naming who
/// gave the last refusal (`by`) and what it said (`error`). It ends the
/// way every run the dispatcher ends itself does
/// ([`crate::api::run::cancel_run_id`]).
pub fn refused_cause(by: RefusedBy, error: &str) -> weft_core::run::CancelCause {
    let who = match by {
        RefusedBy::Worker => "its worker refused it",
        RefusedBy::Platform => "the platform refused weft's calls for it",
        RefusedBy::Settings => "its project's worker settings kept it from any worker",
    };
    weft_core::run::CancelCause::Runtime { detail: format!("{who} {REFUSALS_TO_END} times: {error}") }
}

/// What a run whose build's worker cannot start (`why`) is ended for: at
/// once, never handed out again, its record kept like any ended run's, so
/// once the program runs again it can be picked up from what it did.
pub fn wont_start_cause(run_id: RunId, why: &str) -> weft_core::run::CancelCause {
    weft_core::run::CancelCause::Runtime {
        detail: format!(
            "{why}. Nothing will run on that build again: fix the program and build again (`weft resync` puts \
             triggers that are on onto the new build), then `weft run --seed={run_id}` picks this run up from \
             what it did"
        ),
    }
}

/// End a queued run no worker can ever claim (a run of no program): until
/// its ending lands it shows going and its waiters hang. It ends the way
/// every run the dispatcher ends itself does, a runtime cancel naming why
/// ([`crate::api::run::cancel_run_id`]).
async fn end_undeliverable(state: &DispatcherState, run_id: RunId, reason: &str) -> anyhow::Result<()> {
    tracing::warn!(target: "weft_dispatcher::delivery", %run_id, %reason, "a queued run no worker can claim: ending it");
    let cause = weft_core::run::CancelCause::Runtime { detail: format!("could not be handed to a worker: {reason}") };
    crate::api::run::cancel_run_id(state, run_id, &cause).await
}

/// The project a worker target was asked for does not exist (it was
/// removed): no worker of it can ever take a run.
#[derive(Debug)]
pub struct NotRegistered(pub uuid::Uuid);

impl std::fmt::Display for NotRegistered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "project {} is not registered", self.0)
    }
}

impl std::error::Error for NotRegistered {}

/// The worker target a run runs on: the project's workers at the image it
/// was born on, with the project's own levers over
/// the install's. [`NotRegistered`] when the project is gone; a
/// [`Refusal`] naming the lever when the settings together are a shape no
/// platform could run (the install's changed under a project's own), a
/// definite no that changing them answers. Read from this process's held
/// copy of the levers, which a change reaches a moment after it commits:
/// right for a call to the workers, whose next one follows the change.
pub async fn worker_target(
    state: &DispatcherState,
    tenant: &str,
    project: uuid::Uuid,
    binary_hash: &str,
) -> anyhow::Result<weft_platform_traits::WorkerTarget> {
    // A project registered a moment ago may not have been heard yet: its
    // absence is read from the rows themselves.
    let overrides = match state.held.worker_overrides.held(&project).filter(|held| held.is_some()) {
        Some(held) => held,
        None => stored_overrides(state, project).await?,
    };
    target_with(state, tenant, project, binary_hash, &overrides)
}

/// [`worker_target`] with the levers as the project has them stored now,
/// read from the rows rather than the held copy: for what decides which
/// settings the project's workers run (its front, `Runner::prepare`,
/// `Runner::settle`), where a copy from before a change would bring the
/// old settings back.
pub async fn current_worker_target(
    state: &DispatcherState,
    tenant: &str,
    project: uuid::Uuid,
    binary_hash: &str,
) -> anyhow::Result<weft_platform_traits::WorkerTarget> {
    let overrides = stored_overrides(state, project).await?;
    target_with(state, tenant, project, binary_hash, &overrides)
}

/// `project`'s levers as its rows hold them now (kept in the held copy for
/// the calls after), `None` when the project is gone.
async fn stored_overrides(
    state: &DispatcherState,
    project: uuid::Uuid,
) -> anyhow::Result<std::sync::Arc<Option<weft_platform_traits::WorkerOverrides>>> {
    state.held.worker_overrides.load_fresh(project, || state.projects.worker_overrides(project)).await
}

/// The target of `project`'s workers at the image of `binary_hash` with
/// `overrides` (its levers, `None` when the project is gone).
fn target_with(
    state: &DispatcherState,
    tenant: &str,
    project: uuid::Uuid,
    binary_hash: &str,
    overrides: &Option<weft_platform_traits::WorkerOverrides>,
) -> anyhow::Result<weft_platform_traits::WorkerTarget> {
    let overrides = overrides.as_ref().ok_or(NotRegistered(project))?;
    Ok(weft_platform_traits::WorkerTarget {
        tenant: tenant.to_string(),
        project,
        image: state.builder.images.image_ref(&weft_compiler::build::worker_image_tag(binary_hash)),
        binary_hash: Some(binary_hash.to_string()),
        settings: worker_settings(&state.worker_defaults, overrides, project)?,
    })
}

/// The settings `project`'s workers run with: its own levers over the
/// install's, or a [`Refusal`] naming the lever when together they are a
/// shape no platform could run.
fn worker_settings(
    install: &weft_platform_traits::WorkerSettings,
    overrides: &weft_platform_traits::WorkerOverrides,
    project: uuid::Uuid,
) -> Result<weft_platform_traits::WorkerSettings, Refusal> {
    let settings = install.with(overrides);
    settings.validate().map_err(|e| Refusal { by: RefusedBy::Settings, why: format!("project {project}'s worker settings cannot run: {e}") })?;
    Ok(settings)
}

async fn deliver(state: &DispatcherState, delivery: Delivery) {
    let run_id = delivery.run_id;
    // What this side reads and writes around the call is work: the
    // database is listened to for it, and only for it.
    let busy = state.signals.busy().await;
    let Some(binary_hash) = &delivery.binary_hash else {
        if let Err(e) = end_undeliverable(state, run_id, "it runs no program").await {
            tracing::error!(target: "weft_dispatcher::delivery", %run_id, error = %format!("{e:#}"), "could not end an undeliverable run; its delivery runs out and it is tried again");
        }
        return;
    };
    let target = worker_target(state, &delivery.tenant_id, delivery.project_id, binary_hash).await;
    drop(busy);
    let outcome = match target {
        Err(e) if e.downcast_ref::<NotRegistered>().is_some() => {
            if let Err(e) = end_undeliverable(state, run_id, "its project was removed").await {
                tracing::error!(target: "weft_dispatcher::delivery", %run_id, error = %format!("{e:#}"), "could not end a run whose project was removed; its delivery runs out and it is tried again");
            }
            return;
        }
        Err(e) => Err(e),
        Ok(target) => run_on_worker(state, &target, run_id).await,
    };
    match outcome {
        Ok(weft_core::door_fire::RunAnswer::Failed { error }) => tracing::warn!(
            target: "weft_dispatcher::delivery",
            %run_id,
            %error,
            "the worker's drive of the run failed"
        ),
        Ok(answer) => tracing::debug!(target: "weft_dispatcher::delivery", %run_id, ?answer, "worker answered"),
        Err(e) => {
            let _busy = state.signals.busy().await;
            // The worker may have claimed the run before the call broke:
            // whether it went away with it is for the `lapses` loop to
            // notice, once the grace the claim gave it runs out.
            crate::lapses::book_after_or_log(state, crate::worker_door::lost_margin_secs() * 2).await;
            if let Err(e) = hand_back(state, &delivery, &e).await {
                tracing::warn!(target: "weft_dispatcher::delivery", %run_id, error = %format!("{e:#}"), "could not give the delivery back; it is made again once its lease runs out");
            }
        }
    }
}

/// Give back a delivery no worker took (`e` says why), to be handed out
/// again. A worker still starting is the platform's bring-up going on
/// without this delivery: the run waits, and joins the same bring-up when
/// it is handed out again. The platform logs that bring-up once; per
/// delivery it is only worth a debug line. A worker that answered it is
/// too busy now is up: the run waits a growing pause and counts no try
/// ([`busy`]), and once the workers answered it busy for the install's
/// alert delay (`DispatcherState::alert_after_secs`) that is written down
/// for `weft status` on each try. A refusal (the worker, the platform or
/// the project's worker settings said no) waits a little longer each
/// time, and the last one ends the run naming who refused it. Anything
/// else (no answer, this side's own read failing) waits the same way and
/// counts no refusal; the last such try stops the run's tries until
/// something that can fix it happens ([`UNREACHED_TO_STOP`]). Both are
/// written down for `weft status`, the last try as given up, and only for
/// a run still queued: one its worker claimed before the call broke is
/// that worker's. Whatever is written stays current while any queued run
/// of the project still waits on its workers
/// (`weft_task_store::unanswered::delivery_failed_sql`), and goes once a
/// run they failed since its last claim (`run.waited_on_workers`, which
/// its tries starting again leave set) leaves the queue and leaves none
/// waiting (`weft_task_store::unanswered::delivery_waits_ended`), or once
/// a later run of the project reaches a worker with none waiting.
async fn hand_back(state: &DispatcherState, delivery: &Delivery, e: &anyhow::Error) -> anyhow::Result<()> {
    let run_id = delivery.run_id;
    let error = format!("{e:#}");
    let workers = weft_task_store::unanswered::Callee::Workers(delivery.project_id);
    match GiveBack::of(e) {
        GiveBack::WorkerStarting => {
            tracing::debug!(
                target: "weft_dispatcher::delivery",
                %run_id,
                project = %delivery.project_id,
                reason = %error,
                "the run waits for its worker to start"
            );
            wait_for_worker(&state.pg_pool, run_id).await
        }
        GiveBack::WorkerBusy => {
            let Some(busy_for) = busy(&state.pg_pool, run_id).await? else { return Ok(()) };
            if busy_for < state.alert_after_secs {
                tracing::debug!(
                    target: "weft_dispatcher::delivery",
                    %run_id,
                    project = %delivery.project_id,
                    reason = %error,
                    busy_for_secs = busy_for,
                    "the run waits for its worker to have room"
                );
                return Ok(());
            }
            tracing::warn!(
                target: "weft_dispatcher::delivery",
                %run_id,
                project = %delivery.project_id,
                %error,
                busy_for_secs = busy_for,
                "the project's workers keep answering the run busy"
            );
            // A record this starts begins with the run's busy stretch, so
            // the install's alert delay counts from the first busy answer.
            // A stretch a sibling replica's clock puts in the future began now.
            let lasted = std::time::Duration::from_secs(busy_for.max(0).unsigned_abs());
            weft_task_store::unanswered::failed_for(&state.pg_pool, workers, None, &busy_for_words(busy_for), lasted).await?.log();
            Ok(())
        }
        GiveBack::Refused(by) => {
            let Some(refusals) = refused(&state.pg_pool, run_id).await? else { return Ok(()) };
            tracing::warn!(
                target: "weft_dispatcher::delivery",
                %run_id,
                project = %delivery.project_id,
                %error,
                refusals,
                refused_by = ?by,
                "a hand-off of the run was refused"
            );
            if refusals < REFUSALS_TO_END {
                weft_task_store::unanswered::failed(&state.pg_pool, workers, None, &error).await?.log();
                return Ok(());
            }
            weft_task_store::unanswered::gave_up(&state.pg_pool, workers, None, &error).await?.log();
            crate::api::run::cancel_run_id(state, run_id, &refused_cause(by, &error)).await
        }
        GiveBack::WontStart => {
            // Every run queued on that build ends with it: each would bring
            // up the same worker and meet the same answer.
            let queued: Vec<RunId> = state
                .pg_pool
                .scalars(
                    "SELECT run_id FROM run WHERE project_id = $1 AND binary_hash IS NOT DISTINCT FROM $2 AND state = 'queued'",
                    &[&delivery.project_id, &delivery.binary_hash.as_deref()],
                )
                .await?;
            tracing::warn!(
                target: "weft_dispatcher::delivery",
                %run_id,
                project = %delivery.project_id,
                %error,
                queued = queued.len(),
                "the worker of the run's build cannot start: ending the runs queued on it"
            );
            let causes: Vec<(RunId, weft_core::run::CancelCause)> =
                queued.into_iter().chain(std::iter::once(run_id)).map(|queued| (queued, wont_start_cause(queued, &error))).collect();
            let targets: Vec<(RunId, &weft_core::run::CancelCause)> = causes.iter().map(|(queued, cause)| (*queued, cause)).collect();
            crate::api::run::cancel_run_ids(state, &targets).await.map(drop)
        }
        GiveBack::NotDelivered => {
            let Some(tries) = unreached(&state.pg_pool, run_id).await? else { return Ok(()) };
            tracing::warn!(
                target: "weft_dispatcher::delivery",
                %run_id,
                project = %delivery.project_id,
                %error,
                tries,
                stopped = tries >= UNREACHED_TO_STOP,
                "could not reach the project's workers with the run"
            );
            let alerts = match tries < UNREACHED_TO_STOP {
                true => weft_task_store::unanswered::failed(&state.pg_pool, workers, None, &error).await?,
                false => weft_task_store::unanswered::gave_up(&state.pg_pool, workers, None, &error).await?,
            };
            alerts.log();
            Ok(())
        }
    }
}

/// Why a delivery goes back to the queue: the platform still bringing the
/// worker up (the normal first minutes of a new build, no fault), the
/// worker up and too busy to take it now ([`WorkerBusy`]), a definite no
/// ([`Refusal`], or the platform refusing to bring the worker up:
/// `weft_platform_traits::runner::PlatformRefused`), or no answer at all.
/// Or why it never goes back: the worker of the run's build cannot start
/// at all (`weft_platform_traits::WorkerWontStart`), which no retry
/// changes, so the run ends at once ([`wont_start_cause`]).
#[derive(Debug, PartialEq, Eq)]
enum GiveBack {
    WorkerStarting,
    WorkerBusy,
    Refused(RefusedBy),
    WontStart,
    NotDelivered,
}

impl GiveBack {
    fn of(e: &anyhow::Error) -> Self {
        if weft_platform_traits::WorkerStarting::is(e) {
            GiveBack::WorkerStarting
        } else if weft_platform_traits::WorkerWontStart::is(e) {
            GiveBack::WontStart
        } else if e.downcast_ref::<WorkerBusy>().is_some() {
            GiveBack::WorkerBusy
        } else if let Some(refusal) = e.downcast_ref::<Refusal>() {
            GiveBack::Refused(refusal.by)
        } else if weft_platform_traits::runner::PlatformRefused::is(e) {
            GiveBack::Refused(RefusedBy::Platform)
        } else {
            GiveBack::NotDelivered
        }
    }
}

/// Who answered a hand-off with a definite no.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusedBy {
    /// The worker took the call and answered an error.
    Worker,
    /// The platform refused weft's call, to bring a worker up or to reach
    /// one (permission denied, a quota, a request it calls invalid).
    Platform,
    /// Weft itself: the project's worker settings are a shape no platform
    /// could run ([`worker_target`]).
    Settings,
}

/// A hand-off answered with a definite no, and who said it: what counts
/// as a refusal of the run ([`refused`]).
#[derive(Debug)]
struct Refusal {
    by: RefusedBy,
    why: String,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.why)
    }
}

impl std::error::Error for Refusal {}

/// The worker answered that it cannot take the run now (its memory nearly
/// full, or it is stopping): it is up, and will have room once its runs
/// leave it or the platform adds a copy.
#[derive(Debug)]
struct WorkerBusy {
    why: String,
}

impl std::fmt::Display for WorkerBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.why)
    }
}

impl std::error::Error for WorkerBusy {}

/// What an answer to a hand-off that is not a success says about the run.
#[derive(Debug, PartialEq, Eq)]
enum NotTaken {
    /// The worker's own busy `503` (`weft_engine::busy`, a worker stopping).
    WorkerBusy,
    /// The worker answered any other error: a no from it.
    WorkerRefused,
    /// The platform answered a client error in the worker's place (its
    /// `403`: weft's account may not invoke it): a no from it.
    PlatformRefused,
    /// The platform answered in the worker's place that it could not
    /// serve the call now (`429` with no copy free, `502`/`503`/`504`
    /// while scaling, `404` for a service gone, `408`): no answer from
    /// the workers.
    NoAnswer,
}

impl NotTaken {
    fn of(call: weft_platform_traits::WorkerCall) -> Self {
        use weft_platform_traits::WorkerCall;
        match call {
            WorkerCall::Answered { status: 503, from_worker: true } => NotTaken::WorkerBusy,
            WorkerCall::Answered { from_worker: true, .. } => NotTaken::WorkerRefused,
            WorkerCall::Answered { status, from_worker: false } if (400..500).contains(&status) && !matches!(status, 404 | 408 | 429) => {
                NotTaken::PlatformRefused
            }
            WorkerCall::Answered { .. } | WorkerCall::NoAnswer { .. } => NotTaken::NoAnswer,
        }
    }
}

/// Call the project's workers for one run and hold the call until the
/// worker answers.
async fn run_on_worker(
    state: &DispatcherState,
    target: &weft_platform_traits::WorkerTarget,
    run_id: RunId,
) -> anyhow::Result<weft_core::door_fire::RunAnswer> {
    // The platform refusing to give an address (permission denied, a
    // quota, an invalid request) is a no, kept typed through the context;
    // any other failure to give one (its API timing out or failing) is no
    // answer. Below, an answer that is not a success is read by who gave
    // it and what it says ([`NotTaken`]).
    let endpoint = state
        .runner
        .endpoint(target, weft_platform_traits::Patience::Brief)
        .await
        .context("the platform gave no address for the project's workers")?;
    // No limit on the call: it is held until the run ends, however long
    // the person's program runs.
    let resp = state
        .http
        .post(format!("{}/_weft/run/{run_id}", endpoint.base_url.trim_end_matches('/')))
        .header(weft_platform_traits::WORKER_AUTH_HEADER, endpoint.auth_value())
        .empty_body()
        .send()
        .await
        .map_err(|e| {
            state.runner.call_ended(target, weft_platform_traits::WorkerCall::no_answer(&e));
            anyhow::anyhow!("call the worker at {}: {e}", endpoint.base_url)
        })?;
    let status = resp.status();
    let call = weft_platform_traits::WorkerCall::answered(status, resp.headers());
    state.runner.call_ended(target, call);
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_else(|e| format!("(its body could not be read: {e})"));
        let at = &endpoint.base_url;
        return Err(match NotTaken::of(call) {
            NotTaken::WorkerBusy => WorkerBusy { why: format!("the worker at {at} is busy ({status}): {body}") }.into(),
            NotTaken::WorkerRefused => Refusal { by: RefusedBy::Worker, why: format!("the worker answered {status}: {body}") }.into(),
            NotTaken::PlatformRefused if call.platform_refused() => Refusal {
                by: RefusedBy::Platform,
                why: format!("the platform refused weft's call to the worker at {at} ({status}): weft's account may not invoke it"),
            }
            .into(),
            NotTaken::PlatformRefused => Refusal {
                by: RefusedBy::Platform,
                why: format!("the platform refused weft's call to the worker at {at} ({status}): {body}"),
            }
            .into(),
            NotTaken::NoAnswer => anyhow::anyhow!("the platform could not serve weft's call to the worker at {at} now ({status}): {body}"),
        });
    }
    let answer: weft_core::door_fire::RunAnswer = resp.json().await.map_err(|e| anyhow::anyhow!("read the worker's answer: {e}"))?;
    // Held until here: a platform that stops idle workers itself counts
    // this hold, so the worker was never stopped under the call.
    drop(endpoint);
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::{wont_start_cause, GiveBack, NotTaken, RefusedBy};
    use weft_platform_traits::WorkerCall;

    /// A worker still starting, a worker too busy, a no from the worker or
    /// the platform, a build whose worker cannot start, and a hand-off that
    /// got no answer are told apart: only the no counts as a refusal,
    /// naming who said it.
    #[test]
    fn a_refusal_is_told_apart_from_a_worker_starting_and_from_no_answer() {
        let starting = anyhow::Error::new(weft_platform_traits::WorkerStarting { what: "w".into() }).context("endpoint");
        assert_eq!(GiveBack::of(&starting), GiveBack::WorkerStarting, "found through a context");
        let busy = anyhow::Error::new(super::WorkerBusy { why: "the worker at http://w is busy".into() }).context("deliver");
        assert_eq!(GiveBack::of(&busy), GiveBack::WorkerBusy, "found through a context");
        let refused = anyhow::Error::new(super::Refusal { by: RefusedBy::Worker, why: "the worker answered 500".into() }).context("deliver");
        assert_eq!(GiveBack::of(&refused), GiveBack::Refused(RefusedBy::Worker), "found through a context");
        let platform = anyhow::Error::new(weft_platform_traits::runner::PlatformRefused { why: "Google answered 403".into() })
            .context("the platform gave no address for the project's workers");
        assert_eq!(GiveBack::of(&platform), GiveBack::Refused(RefusedBy::Platform), "the platform refusing to bring a worker up is its no");
        let wont = anyhow::Error::new(weft_platform_traits::WorkerWontStart { why: "it exited".into() }).context("endpoint");
        assert_eq!(GiveBack::of(&wont), GiveBack::WontStart, "a build whose worker cannot start, found through a context");
        assert_eq!(GiveBack::of(&anyhow::anyhow!("call the worker at http://w: timed out")), GiveBack::NotDelivered);
        assert_eq!(GiveBack::of(&anyhow::anyhow!("pool timed out")), GiveBack::NotDelivered, "this side's own read failing");
    }

    /// A run whose build's worker cannot start ends saying why, what fixes
    /// it, and how to pick the run back up.
    #[test]
    fn a_run_whose_worker_cannot_start_says_how_to_pick_it_up() {
        let run_id = uuid::Uuid::from_u128(9);
        let weft_core::run::CancelCause::Runtime { detail } = wont_start_cause(run_id, "the worker of this build cannot start: it exited") else {
            panic!("a runtime cause");
        };
        assert!(detail.starts_with("the worker of this build cannot start: it exited."), "{detail}");
        assert!(detail.contains("weft resync") && detail.contains(&format!("weft run --seed={run_id}")), "{detail}");
    }

    /// A busy answer is never a refusal: the worker's own busy `503` waits
    /// for room, and the platform's answers while it scales or has no copy
    /// free are no answer. Only a definite no refuses, from whoever gave it.
    #[test]
    fn a_busy_answer_is_never_a_refusal() {
        let worker = |status| NotTaken::of(WorkerCall::Answered { status, from_worker: true });
        let platform = |status| NotTaken::of(WorkerCall::Answered { status, from_worker: false });
        assert_eq!(worker(503), NotTaken::WorkerBusy, "weft_engine::busy, or a worker stopping");
        assert_eq!(worker(500), NotTaken::WorkerRefused);
        assert_eq!(worker(429), NotTaken::WorkerRefused, "the program's own answer on its run");
        for status in [429, 502, 503, 504, 404, 408, 500] {
            assert_eq!(platform(status), NotTaken::NoAnswer, "the platform's {status}");
        }
        assert_eq!(platform(403), NotTaken::PlatformRefused, "weft's account may not invoke it");
        assert_eq!(platform(400), NotTaken::PlatformRefused);
    }

    /// What `weft status` shows for a busy worker says for how long, in
    /// the unit a person reads, and what to do about it.
    #[test]
    fn a_busy_worker_is_shown_with_how_long_and_what_to_raise() {
        assert!(super::busy_for_words(30).contains("answered busy for 30 seconds (memory full); raise its memory"));
        assert!(super::busy_for_words(90).contains("for 1 minute (memory full)"));
        assert!(super::busy_for_words(7 * 60 + 5).contains("for 7 minutes (memory full)"));
        assert!(super::busy_for_words(300).contains("`weft workers set --memory <size>`"));
    }

    /// The cause a refused run ends with names who refused it.
    #[test]
    fn a_refused_run_names_who_refused_it() {
        let detail = |by| match super::refused_cause(by, "no") {
            weft_core::run::CancelCause::Runtime { detail } => detail,
            other => panic!("{other:?}"),
        };
        assert_eq!(detail(RefusedBy::Worker), "its worker refused it 3 times: no");
        assert_eq!(detail(RefusedBy::Platform), "the platform refused weft's calls for it 3 times: no");
        assert!(detail(RefusedBy::Settings).starts_with("its project's worker settings"));
    }

    /// Worker settings no platform could run are a no that names the
    /// lever, never a hand-off that got no answer.
    #[test]
    fn settings_no_platform_could_run_are_a_refusal() {
        let install: weft_platform_traits::WorkerSettings = serde_json::from_value(serde_json::json!({})).unwrap();
        let project = uuid::Uuid::nil();
        let fine = weft_platform_traits::WorkerOverrides::default();
        assert!(super::worker_settings(&install, &fine, project).is_ok());
        let none: weft_platform_traits::WorkerOverrides = serde_json::from_value(serde_json::json!({ "max_instances": 0 })).unwrap();
        let refused = anyhow::Error::new(super::worker_settings(&install, &none, project).unwrap_err());
        assert_eq!(GiveBack::of(&refused), GiveBack::Refused(RefusedBy::Settings));
        assert!(refused.to_string().contains("workers.max_instances"), "{refused}");
    }
}
