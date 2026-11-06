//! Handing a trigger's queued events over once it can take them.
//!
//! An event waits in its trigger's queue (`parked_fire`,
//! `weft_task_store::parked_fires`) while the trigger is parked, while it
//! could not become a run yet (no room, what it reads was not ready, its
//! worker was out of reach), or, for an answer, while the run's trigger is
//! parked. A drain hands each token's queue over head by head, first in
//! first out: it takes the head under `FOR UPDATE SKIP LOCKED` in its own
//! transaction, hands it over (an entry's event to the worker's door, an
//! answer to its run), and removes it, or stamps it with what it waits on
//! next (`weft_task_store::parked_fires::Turn`) and stops: one trigger's
//! events never overtake each other. Two drains never hand one event over,
//! and a drain that dies mid-way leaves its head to the next.
//!
//! A head waits on a clock (room to run, a caller's limit, a failed
//! hand-over tried again shortly) or on something no clock brings
//! (`parked_fire.waits_for`). What drains:
//!
//! - the sweep, for heads whose clock came due ([`drain_due`]), booked
//!   at the earliest of them;
//! - an activation, once its triggers are Active again, whatever each
//!   head waits on ([`drain_activations`]);
//! - a change of an instance's values, for the events waiting on them
//!   (`crate::instance_values`), and infra coming back, for those waiting
//!   on it ([`drain_waiting_on`]);
//! - a new version registered or the project's worker settings changing,
//!   for a queue a failing hand-over stopped ([`retry_stopped`]).
//!
//! A head stamped outside the sweep books the sweep's look at its time
//! ([`book`]): nothing announces a stamp.

use std::time::Duration;

use axum::http::StatusCode;
use weft_core::door_fire::{UnclockedWait, WaitsOn};
use weft_task_store::db::{self, Client as _};
use weft_task_store::drain::DrainStep;
use weft_task_store::parked_fires::{take_head, take_head_now, Head, Turn};

use crate::api::signal::Landed;
use crate::state::DispatcherState;

/// The sweep's loop, by name (`crate::reaper`).
pub(crate) const PARKED_FIRES: &str = "parked_fires";

/// Which heads a drain takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Due {
    /// Only one whose clock came due, and that waits on nothing else.
    WhenDue,
    /// Whatever it waits on: something that happened hands it over again.
    Now,
}

/// SQL true for a row `alias` of `parked_fire` that waits on `on`
/// (`parked_fire.waits_for`).
pub(crate) fn waits_on_sql(alias: &str, on: UnclockedWait) -> String {
    // SYNC: the spelling <-> weft_task_store::parked_fires::WaitsFor (`<kind>: <reason>`)
    format!("{alias}.waits_for LIKE '{}: %'", on.as_str())
}

/// Drain the queues of the activations `keys`, which just came back
/// Active: every token of theirs with an event waiting, whatever its head
/// waits on (the activation is what may have fixed it). An event parked
/// after this looked is the sweep's, which its parking wakes.
pub(crate) async fn drain_activations(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<()> {
    let tokens: Vec<String> = crate::store::postgres::activation_signals(&state.pg_pool, project_id, keys)
        .await?
        .into_iter()
        .map(|signal| signal.token)
        .collect();
    let queued: Vec<String> = state
        .pg_pool
        .scalars("SELECT DISTINCT token FROM parked_fire WHERE token = ANY($1) AND run_id IS NULL", &[&tokens])
        .await?;
    drain_tokens(state, &queued).await
}

/// Drain, whatever their clock, the queues of an Active trigger holding an
/// event that waits on `on`: of `project`, or of every project (`None`).
/// For what just happened that it waits on.
pub(crate) async fn drain_waiting_on(state: &DispatcherState, on: UnclockedWait, project: Option<uuid::Uuid>) -> anyhow::Result<()> {
    let tokens = tokens_waiting_on(&state.pg_pool, on, project).await?;
    drain_tokens(state, &tokens).await
}

/// The signals of an Active trigger (of `project`, or of every project)
/// whose queue holds an event waiting on `on`: what [`drain_waiting_on`]
/// drains.
pub async fn tokens_waiting_on(pool: &db::Pool, on: UnclockedWait, project: Option<uuid::Uuid>) -> anyhow::Result<Vec<String>> {
    Ok(pool
        .scalars(
            &format!(
                "SELECT s.token FROM signal s {} \
                 WHERE ($1::uuid IS NULL OR s.project_id = $1) AND COALESCE(a.status, 'active') = 'active' \
                   AND EXISTS (SELECT 1 FROM parked_fire p WHERE p.token = s.token AND {})",
                weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
                waits_on_sql("p", on),
            ),
            &[&project],
        )
        .await?)
}

/// Hand over again the queues of `project` that a failing hand-over
/// stopped, now that something that may have fixed it happened (a new
/// version registered, its worker settings changed). What happened stands
/// whatever this does: a failure is logged, and the queue stays stopped,
/// shown by `weft status`.
pub(crate) async fn retry_stopped(state: &DispatcherState, project: uuid::Uuid) {
    if let Err(e) = drain_waiting_on(state, UnclockedWait::Failure, Some(project)).await {
        tracing::error!(
            target: "weft_dispatcher::parked_drain",
            project_id = %project, error = %format!("{e:#}"),
            "the trigger queues a failing hand-over stopped could not be handed over again; `weft activate` tries again"
        );
    }
}

/// Drain `tokens`' queues whatever their heads wait on, then book the
/// sweep at the earliest head one of them left waiting on a clock. Every
/// token is tried; the ones that failed are named together after.
pub(crate) async fn drain_tokens(state: &DispatcherState, tokens: &[String]) -> anyhow::Result<()> {
    let mut next: Option<i64> = None;
    let mut failed = Vec::new();
    for token in tokens {
        match drain_token(state, token, Due::Now).await {
            Ok(due) => next = next.into_iter().chain(due).min(),
            Err(e) => failed.push(format!("{token}: {e:#}")),
        }
    }
    if let Err(e) = book(state, next).await {
        failed.push(format!("the sweep's next look: {e:#}"));
    }
    anyhow::ensure!(failed.is_empty(), "these trigger queues could not be drained: {}", failed.join("; "));
    Ok(())
}

/// Book the sweep's look at `at` (unix seconds) unless one is booked by
/// then: for a head stamped outside the sweep. Only when the dispatcher
/// scales to zero; a machine's sweep looks on its own. A booking that
/// cannot be written fails, for the caller to fail with.
pub(crate) async fn book(state: &DispatcherState, at: Option<i64>) -> anyhow::Result<()> {
    let (Some(booking), Some(at)) = (&state.booking, at) else { return Ok(()) };
    booking.book_sooner(PARKED_FIRES, at.saturating_mul(1000)).await.map_err(|e| e.context("book the look at queued trigger events"))
}

/// The sweep: every queue of an Active trigger whose head came due gets
/// drained, then it looks again when the next head comes due, however far
/// away. A queue that could not be drained (the database) fails the pass,
/// after the others went.
pub(crate) async fn drain_due(state: &DispatcherState) -> anyhow::Result<DrainStep> {
    let now = crate::lease::now_unix();
    let mut failed = Vec::new();
    for token in due_tokens(&state.pg_pool, now).await? {
        if let Err(e) = drain_token(state, &token, Due::WhenDue).await {
            failed.push(format!("{token}: {e:#}"));
        }
    }
    anyhow::ensure!(failed.is_empty(), "these trigger queues could not be drained: {}", failed.join("; "));
    Ok(match next_due(&state.pg_pool).await? {
        // Nothing waits on a clock: the next park, or what the rest wait
        // on, wakes it.
        None => DrainStep::Done,
        // A head due now that did not drain is held by a drain going on.
        Some(due) => DrainStep::RetryIn(Duration::from_secs((due - crate::lease::now_unix()).max(1).unsigned_abs())),
    })
}

/// The heads the sweep would drain: of a trigger that is Active (or that
/// no activation governs), due at `now`, waiting on no event.
pub async fn due_tokens(pool: &db::Pool, now: i64) -> anyhow::Result<Vec<String>> {
    Ok(pool
        .scalars(
            &format!(
                "SELECT p.token FROM parked_fire p JOIN signal s ON s.token = p.token {} \
                 WHERE COALESCE(a.status, 'active') = 'active' \
                   AND p.seq = (SELECT min(q.seq) FROM parked_fire q WHERE q.token = p.token) \
                   AND p.waits_for IS NULL AND p.not_before <= $1",
                weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
            ),
            &[&now],
        )
        .await?)
}

/// When the earliest head [`due_tokens`] would drain comes due; `None`
/// when no head waits on a clock.
pub async fn next_due(pool: &db::Pool) -> anyhow::Result<Option<i64>> {
    Ok(pool
        .scalar(
            &format!(
                "SELECT MIN(p.not_before) FROM parked_fire p JOIN signal s ON s.token = p.token {} \
                 WHERE COALESCE(a.status, 'active') = 'active' \
                   AND p.seq = (SELECT min(q.seq) FROM parked_fire q WHERE q.token = p.token) \
                   AND p.waits_for IS NULL",
                weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
            ),
            &[],
        )
        .await?)
}

/// Drain `token`'s queue head by head (see the module doc), until it is
/// empty, its head is not due (as `due` says) or held by another drain, or
/// its head waits. Answers when the head it left waiting on a clock comes
/// due (`None`: it left none). `Err` only when the queue could not be read
/// or written; a head that could not be handed over is stamped with its
/// next turn.
pub(crate) async fn drain_token(state: &DispatcherState, token: &str, due: Due) -> anyhow::Result<Option<i64>> {
    loop {
        let mut conn = state.pg_pool.get().await?;
        let tx = conn.transaction().await?;
        let head = match due {
            Due::WhenDue => take_head(&tx, token, crate::lease::now_unix()).await?,
            Due::Now => take_head_now(&tx, token).await?,
        };
        let Some(head) = head else { return Ok(None) };
        let routing = match crate::api::signal::lookup_signal_routing(state, token).await {
            Ok(routing) => routing,
            // The signal went with its queue (`parked_fire_drop_with_signal`),
            // between the take and here.
            Err((StatusCode::NOT_FOUND, _)) => return Ok(None),
            Err((code, msg)) => anyhow::bail!("signal {token} {code}: {msg}"),
        };
        if head.is_resume {
            if !hand_answer(state, tx, &head, &routing).await? {
                return Ok(None);
            }
            continue;
        }
        let fire = weft_core::door_fire::DoorFire {
            token: token.to_string(),
            fire_id: head.waiting.fire_id,
            payload: head.waiting.payload.clone(),
            caller: head.waiting.caller.clone(),
            held_by: None,
            attempts: head.waiting.attempts,
            released: true,
        };
        let landed = crate::api::signal::fire_entry(state, &routing, fire).await;
        let now = crate::lease::now_unix();
        let waiting = &head.waiting;
        let turn = match landed {
            Ok(Landed::Done) => {
                weft_task_store::parked_fires::remove_in(&tx, &head).await?;
                tx.commit().await?;
                continue;
            }
            // It waits its turn again, and the rest of the queue behind it.
            Ok(Landed::Waits { reason, on }) => {
                tracing::info!(target: "weft_dispatcher::parked_drain", %token, fire_id = %waiting.fire_id, on = on.as_str(), %reason, "the head of the queue still waits");
                Turn::next(waiting.attempts, waiting.failures, on, &reason, now)
            }
            // A caller past their own limit: tried again once the worker
            // says the limit has room.
            Ok(Landed::Refused { reason, retry_after_secs }) => {
                tracing::info!(target: "weft_dispatcher::parked_drain", %token, fire_id = %waiting.fire_id, %reason, "the head of the queue waits for its caller's limit");
                let not_before = now.saturating_add(i64::try_from(retry_after_secs).unwrap_or(i64::MAX));
                Turn { attempts: waiting.attempts, not_before, waits_for: None, failures: 0 }
            }
            Err(e) => Turn::next(waiting.attempts, waiting.failures, WaitsOn::Failure, &format!("{e:#}"), now),
        };
        weft_task_store::parked_fires::restamp_in(&tx, &head, &turn).await?;
        tx.commit().await?;
        if let Some(stopped) = turn.waits_for.as_ref().filter(|waits| waits.on == UnclockedWait::Failure) {
            tracing::error!(
                target: "weft_dispatcher::parked_drain",
                %token, fire_id = %waiting.fire_id, failures = turn.failures, error = %stopped.reason,
                "a queued event failed to become a run again and again; its trigger's queue stops here until the trigger \
                 is activated again, a new version registers or the project's worker settings change (`weft status` shows it)"
            );
        }
        return Ok(turn.waits_for.is_none().then_some(turn.not_before));
    }
}

/// Hand the queued answer `head` to its run, on the drain's transaction
/// `tx`, when its run's trigger is back. Whether the drain goes on to the
/// next head.
async fn hand_answer(
    state: &DispatcherState,
    tx: db::Transaction<'_>,
    head: &Head,
    routing: &crate::api::signal::FireGateInfo,
) -> anyhow::Result<bool> {
    // The wait's signal gone, and whether the answer reached its run (and
    // may have left news for the worker driving it).
    let (consumed, reached) = match routing.standing().arrival(crate::lease::now_unix()) {
        // Still parked: it waits on, and nothing behind it can go first.
        weft_core::arrival::Arrival::Wait => return Ok(false),
        weft_core::arrival::Arrival::Refused => {
            tracing::info!(target: "weft_dispatcher::parked_drain", token = %head.token, "a queued answer dropped: its trigger takes no work any more");
            weft_task_store::parked_fires::remove_in(&tx, head).await?;
            (None, false)
        }
        weft_core::arrival::Arrival::Live => match crate::store::postgres::answer_in(&tx, &head.token, &head.waiting.answer(), crate::store::postgres::AnswerFrom::Queue).await? {
            // The wait's signal went, and its queue with it.
            crate::store::Answered::Reached { consumed } => (Some(consumed), true),
            crate::store::Answered::RunEnded { consumed } => (Some(consumed), false),
            crate::store::Answered::Gone => {
                weft_task_store::parked_fires::remove_in(&tx, head).await?;
                (None, false)
            }
        },
    };
    tx.commit().await?;
    weft_task_store::announce::committed(&state.pg_pool);
    if reached {
        weft_task_store::announce::news_written(&state.pg_pool);
    }
    if let Some(consumed) = consumed {
        state.listener.unregister_many(&consumed).await;
    }
    Ok(true)
}
