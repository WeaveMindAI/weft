//! Handing queued runs to the project's workers.
//!
//! A run the dispatcher starts (`weft run`, a setup run) and every run that
//! comes back (an answer reached it while parked, its worker handed it
//! back, its worker went away while it was durable) waits as a `queued`
//! run row. This loop takes queued runs ([`take`], which also marks each
//! delivered for a while so no sibling hands it out twice) and calls a
//! worker for each: `POST /_weft/run/<execution_id>` on the project's
//! workers at the run's image. The worker claims the run (its row goes
//! `running`, owned by that worker, its epoch raised) and drives it from
//! its record. A worker that never claims leaves the delivery to run out,
//! and the run is handed out again.
//!
//! The call is held open until the worker answers (the run ended or waits
//! on something outside it), on a task of its own so one run never holds
//! up the next delivery. On a platform that stops a worker nobody is
//! calling (Cloud Run), that held call is what keeps the run's worker up.

use std::time::Duration;

use weft_core::ExecutionId;
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// How many runs one pass takes. More waiting means the loop comes
/// straight back for them.
const BATCH: i64 = 32;

/// A run being queued wakes the loop.
pub(crate) static WAKE_ON: &[WakeOn] = &[WakeOn::any(weft_task_store::runs::RUN_QUEUED_CHANNEL)];

/// How long a run handed to a worker stays out of the queue for it to
/// claim: 30 seconds at this install's pace. One unclaimed by then is
/// handed out again.
fn delivery_lease_secs() -> i64 {
    weft_core::time_scale::scaled_secs(30)
}

/// The safety look, for a delivery that ran out (its worker never
/// claimed), which no write announces: 30 seconds at this install's pace.
fn safety() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(30))
}

/// One queued run taken for delivery.
#[derive(Debug, sqlx::FromRow)]
struct Delivery {
    execution_id: ExecutionId,
    project_id: uuid::Uuid,
    tenant_id: String,
    /// The image it runs on; `None` for a run of no program, which no
    /// worker can claim.
    binary_hash: Option<String>,
}

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("delivery", WAKE_ON, safety(), move || {
        let state = state.clone();
        async move {
            let taken = take(&state.pg_pool, crate::lease::now_unix(), BATCH).await?;
            let full = taken.len() as i64 == BATCH;
            for delivery in taken {
                let state = state.clone();
                tokio::spawn(async move { deliver(&state, delivery).await });
            }
            Ok(if full { DrainStep::More } else { DrainStep::Done })
        }
    })
}

/// Take up to `limit` queued runs nobody is delivering, oldest first, and
/// mark each delivered until a lease from `now` runs out.
async fn take(pool: &sqlx::PgPool, now: i64, limit: i64) -> anyhow::Result<Vec<Delivery>> {
    Ok(sqlx::query_as(
        "UPDATE run SET delivered_until = $1 + $2 WHERE execution_id IN ( \
             SELECT execution_id FROM run \
             WHERE state = 'queued' AND (delivered_until IS NULL OR delivered_until < $1) \
             ORDER BY started_at LIMIT $3 FOR UPDATE SKIP LOCKED) \
         RETURNING execution_id, project_id, tenant_id, binary_hash",
    )
    .bind(now)
    .bind(delivery_lease_secs())
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

/// Give a delivery back, so the next pass makes it again instead of
/// waiting out its lease. Not announced: a worker still starting would
/// otherwise be called again in a tight loop; the next pass is the next
/// run queued, or the safety look.
async fn give_back(pool: &sqlx::PgPool, execution_id: ExecutionId) -> anyhow::Result<()> {
    sqlx::query("UPDATE run SET delivered_until = NULL WHERE execution_id = $1 AND state = 'queued'")
        .bind(execution_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// End a queued run no worker can ever claim (a run of no program): until
/// its ending lands it shows going and its waiters hang. It ends the way
/// every run the dispatcher ends itself does, a runtime cancel naming why
/// ([`crate::api::execution::cancel_execution_id`]).
async fn end_undeliverable(state: &DispatcherState, execution_id: ExecutionId, reason: &str) -> anyhow::Result<()> {
    tracing::warn!(target: "weft_dispatcher::delivery", %execution_id, %reason, "a queued run no worker can claim: ending it");
    let cause = weft_core::exec::CancelCause::Runtime { detail: format!("could not be handed to a worker: {reason}") };
    crate::api::execution::cancel_execution_id(state, execution_id, &cause).await
}

/// The worker target a run runs on: the project's workers at the image it
/// was born on, with the project's own levers over
/// the install's.
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
        None => state.held.worker_overrides.load_fresh(project, || state.projects.worker_overrides(project)).await?,
    };
    let overrides = overrides.as_ref().as_ref().ok_or_else(|| anyhow::anyhow!("project {project} is not registered"))?;
    Ok(weft_platform_traits::WorkerTarget {
        tenant: tenant.to_string(),
        project,
        image: state.builder.images.image_ref(&weft_compiler::build::worker_image_tag(binary_hash)),
        binary_hash: Some(binary_hash.to_string()),
        settings: state.worker_defaults.with(overrides),
    })
}

async fn deliver(state: &DispatcherState, delivery: Delivery) {
    let execution_id = delivery.execution_id;
    let Some(binary_hash) = &delivery.binary_hash else {
        if let Err(e) = end_undeliverable(state, execution_id, "it runs no program").await {
            tracing::error!(target: "weft_dispatcher::delivery", %execution_id, error = %format!("{e:#}"), "could not end an undeliverable run; its delivery runs out and it is tried again");
        }
        return;
    };
    let outcome = match worker_target(state, &delivery.tenant_id, delivery.project_id, binary_hash).await {
        Err(e) => Err(e),
        Ok(target) => run_on_worker(state, &target, execution_id).await,
    };
    match outcome {
        Ok(weft_core::door_fire::RunAnswer::Failed { error }) => tracing::warn!(
            target: "weft_dispatcher::delivery",
            %execution_id,
            %error,
            "the worker's drive of the run failed"
        ),
        Ok(answer) => tracing::debug!(target: "weft_dispatcher::delivery", %execution_id, ?answer, "worker answered"),
        Err(e) => {
            // Nothing claimed it: give the delivery back so the next pass
            // makes it again instead of waiting out the lease. A worker
            // still starting is the platform's bring-up going on without
            // this delivery: the run stays queued, and each later pass
            // joins the same bring-up until it lands. The platform logs
            // that bring-up once; per delivery it is only worth a debug
            // line.
            match GiveBack::of(&e) {
                GiveBack::WorkerStarting => tracing::debug!(
                    target: "weft_dispatcher::delivery",
                    %execution_id,
                    project = %delivery.project_id,
                    reason = %format!("{e:#}"),
                    "the run waits for its worker to start; it is handed out again on the next pass"
                ),
                GiveBack::CouldNotHand => tracing::warn!(
                    target: "weft_dispatcher::delivery",
                    %execution_id,
                    project = %delivery.project_id,
                    error = %format!("{e:#}"),
                    "could not hand the run to a worker; it is handed out again on the next pass"
                ),
            }
            if let Err(e) = give_back(&state.pg_pool, execution_id).await {
                tracing::warn!(target: "weft_dispatcher::delivery", %execution_id, error = %e, "could not give the delivery back; it is made again once its lease runs out");
            }
        }
    }
}

/// Why a delivery goes back to the queue. Both are given back the same
/// way; they differ in what they say, since a worker starting is the
/// normal first minutes of a new build and no fault.
#[derive(Debug, PartialEq, Eq)]
enum GiveBack {
    WorkerStarting,
    CouldNotHand,
}

impl GiveBack {
    fn of(e: &anyhow::Error) -> Self {
        if weft_platform_traits::WorkerStarting::is(e) {
            GiveBack::WorkerStarting
        } else {
            GiveBack::CouldNotHand
        }
    }
}

/// Call the project's workers for one run and hold the call until the
/// worker answers.
async fn run_on_worker(
    state: &DispatcherState,
    target: &weft_platform_traits::WorkerTarget,
    execution_id: ExecutionId,
) -> anyhow::Result<weft_core::door_fire::RunAnswer> {
    let endpoint = state.runner.endpoint(target, weft_platform_traits::Patience::Brief).await?;
    let resp = state
        .http
        .post(format!("{}/_weft/run/{execution_id}", endpoint.base_url.trim_end_matches('/')))
        .header(weft_platform_traits::WORKER_AUTH_HEADER, endpoint.auth_value())
        .send()
        .await
        .map_err(|e| {
            state.runner.call_ended(target, weft_platform_traits::WorkerCall::no_answer(&e));
            anyhow::anyhow!("call the worker at {}: {e}", endpoint.base_url)
        })?;
    let status = resp.status();
    let call = weft_platform_traits::WorkerCall::answered(status, resp.headers());
    state.runner.call_ended(target, call);
    if call.platform_refused() {
        anyhow::bail!("the platform refused weft's call to the worker at {} ({status}): weft's account may not invoke it", endpoint.base_url);
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("the worker answered {status}: {body}");
    }
    let answer: weft_core::door_fire::RunAnswer = resp.json().await.map_err(|e| anyhow::anyhow!("read the worker's answer: {e}"))?;
    // Held until here: a platform that stops idle workers itself counts
    // this hold, so the worker was never stopped under the call.
    drop(endpoint);
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::GiveBack;

    #[test]
    fn a_worker_still_starting_is_told_apart_from_a_failed_hand_off() {
        let starting = anyhow::Error::new(weft_platform_traits::WorkerStarting { what: "w".into() }).context("endpoint");
        assert_eq!(GiveBack::of(&starting), GiveBack::WorkerStarting, "found through a context");
        assert_eq!(GiveBack::of(&anyhow::anyhow!("the worker answered 500")), GiveBack::CouldNotHand);
    }
}
