//! Handing a trigger's queued events over once it can take them.
//!
//! An event waits in its trigger's queue (`parked_fire`,
//! `weft_task_store::parked_fires`) while the trigger is parked, while it
//! could not become a run yet (what it reads was not ready, its worker was
//! full or out of reach), or, for an answer, while the run's trigger is
//! parked. A drain hands each token's queue over head by head, first in
//! first out: it takes the head under `FOR UPDATE SKIP LOCKED` in its own
//! transaction, hands it over (an entry's event to the worker's door, an
//! answer to its run), and removes it, or stamps it with its next try and
//! stops: one trigger's events never overtake each other. Two drains never
//! hand one event over, and a drain that dies mid-way leaves its head to
//! the next.
//!
//! Three things drain: an activation, once its triggers are Active again
//! ([`drain_activations`]); the reaper's sweep, for heads that came due
//! ([`drain_due`]); and a change of an instance's values, for the events
//! waiting on them ([`drain_token`] with [`Due::Now`]).

use std::time::Duration;

use axum::http::StatusCode;
use weft_task_store::drain::DrainStep;
use weft_task_store::parked_fires::{take_head, take_head_now, Head};

use crate::api::signal::Landed;
use crate::state::DispatcherState;

/// Which heads a drain takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Due {
    /// Only one that is due: its backoff ran out, and it does not wait on
    /// its instance.
    WhenDue,
    /// Whatever its backoff: a person or an instance's change routes it
    /// again.
    Now,
}

/// The longest the sweep sleeps between looks, whatever the queues say:
/// 30 seconds, at this install's pace (`weft_core::time_scale`).
fn longest_sleep() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(30))
}

/// Drain the queues of the activations `keys`, which just came back
/// Active: every token of theirs with an event waiting. An event parked
/// after this looked is the sweep's, which its parking wakes.
pub(crate) async fn drain_activations(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<()> {
    let tokens: Vec<String> = crate::journal::postgres::activation_signals(&state.pg_pool, project_id, keys)
        .await?
        .into_iter()
        .map(|signal| signal.token)
        .collect();
    let queued: Vec<String> = sqlx::query_scalar("SELECT DISTINCT token FROM parked_fire WHERE token = ANY($1) AND execution_id IS NULL")
        .bind(&tokens)
        .fetch_all(&state.pg_pool)
        .await?;
    for token in queued {
        drain_token(state, &token, Due::WhenDue).await?;
    }
    Ok(())
}

/// The reaper's sweep: every queue of an Active trigger whose head is due
/// gets drained, then the sweep sleeps until the next head comes due. A
/// token whose hand-over failed is logged and left stamped with its next
/// try; the others go on.
pub(crate) async fn drain_due(state: &DispatcherState) -> anyhow::Result<DrainStep> {
    let now = crate::lease::now_unix();
    for token in due_tokens(&state.pg_pool, now).await? {
        if let Err(e) = drain_token(state, &token, Due::WhenDue).await {
            tracing::warn!(
                target: "weft_dispatcher::parked_drain",
                %token, error = %format!("{e:#}"),
                "a queued event could not be handed over; it is tried again when due"
            );
        }
    }
    Ok(match next_due(&state.pg_pool).await? {
        // Nothing waits: the next park wakes it.
        None => DrainStep::Done,
        Some(due) => DrainStep::RetryIn(sleep_until(crate::lease::now_unix(), due)),
    })
}

/// How long the sweep sleeps while something waits: until the earliest
/// head is due, at least a second (a head due now that did not drain was
/// re-stamped, or is held by a drain going on) and at most
/// [`longest_sleep`].
fn sleep_until(now_unix: i64, next_due_unix: i64) -> Duration {
    Duration::from_secs((next_due_unix - now_unix).max(1) as u64).min(longest_sleep())
}

/// The heads the sweep would drain: of a trigger that is Active (or that
/// no activation governs), due at `now`, waiting on no instance.
pub async fn due_tokens(pool: &sqlx::PgPool, now: i64) -> anyhow::Result<Vec<String>> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT p.token FROM parked_fire p JOIN signal s ON s.token = p.token {} \
         WHERE COALESCE(a.status, 'active') = 'active' \
           AND p.seq = (SELECT min(q.seq) FROM parked_fire q WHERE q.token = p.token) \
           AND p.instance_gap IS NULL AND p.not_before <= $1",
        weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    ))
    .bind(now)
    .fetch_all(pool)
    .await?)
}

/// When the earliest head [`due_tokens`] would drain comes due; `None`
/// when nothing waits.
pub async fn next_due(pool: &sqlx::PgPool) -> anyhow::Result<Option<i64>> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT MIN(p.not_before) FROM parked_fire p JOIN signal s ON s.token = p.token {} \
         WHERE COALESCE(a.status, 'active') = 'active' \
           AND p.seq = (SELECT min(q.seq) FROM parked_fire q WHERE q.token = p.token) \
           AND p.instance_gap IS NULL",
        weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    ))
    .fetch_one(pool)
    .await?)
}

/// Drain `token`'s queue head by head (see the module doc), until it is
/// empty, its head is not due (as `due` says) or held by another drain, or
/// its head waits. `Err` when a head could not be handed over: it stays
/// the head, stamped with its next try.
pub(crate) async fn drain_token(state: &DispatcherState, token: &str, due: Due) -> anyhow::Result<()> {
    loop {
        let mut tx = state.pg_pool.begin().await?;
        let head = match due {
            Due::WhenDue => take_head(&mut tx, token, crate::lease::now_unix()).await?,
            Due::Now => take_head_now(&mut tx, token).await?,
        };
        let Some(head) = head else { return Ok(()) };
        let routing = match crate::api::signal::lookup_signal_routing(state, token).await {
            Ok(routing) => routing,
            // The signal went with its queue (`parked_fire_drop_with_signal`),
            // between the take and here.
            Err((StatusCode::NOT_FOUND, _)) => return Ok(()),
            Err((code, msg)) => anyhow::bail!("signal {token} {code}: {msg}"),
        };
        if head.is_resume {
            if !hand_answer(state, tx, &head, &routing).await? {
                return Ok(());
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
        };
        let landed = crate::api::signal::fire_entry(state, &routing, fire).await;
        let now = crate::lease::now_unix();
        let attempts = head.waiting.attempts;
        match landed {
            Ok(Landed::Done) => {
                weft_task_store::parked_fires::remove_in(&mut tx, &head).await?;
                tx.commit().await?;
            }
            // It waits its turn again, which is no failure of the event's:
            // after its backoff, on its instance's next change, or, for a
            // caller past their own limit, once the worker says the limit
            // has room. The rest of the queue waits behind it.
            Ok(Landed::Waits { reason, instance_gap }) => {
                let (attempts, gap) = if instance_gap { (attempts, Some(reason.as_str())) } else { (attempts + 1, None) };
                let not_before = now + weft_task_store::parked_fires::park_backoff_secs(attempts);
                weft_task_store::parked_fires::restamp_in(&mut tx, &head, attempts, not_before, gap).await?;
                tx.commit().await?;
                tracing::info!(target: "weft_dispatcher::parked_drain", %token, fire_id = %head.waiting.fire_id, attempts, %reason, "the head of the queue still waits");
                return Ok(());
            }
            Ok(Landed::Refused { reason, retry_after_secs }) => {
                weft_task_store::parked_fires::restamp_in(&mut tx, &head, attempts, now + retry_after_secs as i64, None).await?;
                tx.commit().await?;
                tracing::info!(target: "weft_dispatcher::parked_drain", %token, fire_id = %head.waiting.fire_id, %reason, "the head of the queue waits for its caller's limit");
                return Ok(());
            }
            Err(e) => {
                let attempts = attempts + 1;
                weft_task_store::parked_fires::restamp_in(&mut tx, &head, attempts, now + weft_task_store::parked_fires::park_backoff_secs(attempts), None)
                    .await?;
                tx.commit().await?;
                return Err(e.context(format!("hand the queued event {} over", head.waiting.fire_id)));
            }
        }
    }
}

/// Hand the queued answer `head` to its run, on the drain's transaction
/// `tx`, when its run's trigger is back. Whether the drain goes on to the
/// next head.
async fn hand_answer(
    state: &DispatcherState,
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    head: &Head,
    routing: &crate::api::signal::FireGateInfo,
) -> anyhow::Result<bool> {
    let consumed = match routing.standing().arrival(crate::lease::now_unix()) {
        // Still parked: it waits on, and nothing behind it can go first.
        weft_core::arrival::Arrival::Wait => return Ok(false),
        weft_core::arrival::Arrival::Refused => {
            tracing::info!(target: "weft_dispatcher::parked_drain", token = %head.token, "a queued answer dropped: its trigger takes no work any more");
            weft_task_store::parked_fires::remove_in(&mut tx, head).await?;
            None
        }
        weft_core::arrival::Arrival::Live => match crate::journal::postgres::answer_in(&mut tx, &head.token, &head.waiting.answer(), crate::journal::postgres::AnswerFrom::Queue).await? {
            // The wait's signal went, and its queue with it.
            crate::journal::Answered::Reached { consumed } | crate::journal::Answered::RunEnded { consumed } => Some(consumed),
            crate::journal::Answered::Gone => {
                weft_task_store::parked_fires::remove_in(&mut tx, head).await?;
                None
            }
        },
    };
    tx.commit().await?;
    weft_task_store::announce::committed(&state.pg_pool);
    if let Some(consumed) = consumed {
        state.listener.unregister_many(&[consumed]).await;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sweep_sleeps_until_the_next_head_is_due_within_bounds() {
        assert_eq!(sleep_until(100, 107), Duration::from_secs(7));
        assert_eq!(sleep_until(100, 100), Duration::from_secs(1), "due now: look again shortly");
        assert_eq!(sleep_until(100, 50), Duration::from_secs(1));
        assert_eq!(sleep_until(100, 10_000), longest_sleep());
    }
}
