//! Handing executions to the project's workers.
//!
//! An execution waits in the task table (its execute or resume task,
//! pending, stamped with the worker image it runs on and its run class).
//! This loop takes each one ([`weft_task_store::tasks::take_deliveries`],
//! which also marks it delivered so no sibling hands it out twice) and
//! calls a worker for it: `POST /_weft/run/<execution_id>` on the project's
//! workers at that image for a short run, a job of its own for a long
//! one. The worker claims the task and drives it; the task row stays the
//! truth throughout, so a worker that dies mid-run leaves a claim that
//! lapses, and the next sweep delivers the execution again.
//!
//! A short run's call is held open until the worker answers (the
//! execution ended or waits on something outside it), on a task of its
//! own so one long run never holds up the next delivery.

use std::time::Duration;

use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};
use weft_task_store::tasks::{Delivery, TASK_READY_CHANNEL};

use crate::state::DispatcherState;

/// How many executions one pass takes. More waiting means the loop comes
/// straight back for them.
const BATCH: i64 = 32;

/// A worker task that became claimable wakes the loop; so does a task
/// ending, since a resume waits for its execution's drive to end
/// (`tasks::EXECUTION_ID_PICK`), and nothing else announces that it may go.
pub(crate) static WAKE_ON: &[WakeOn] = &[
    WakeOn { channel: TASK_READY_CHANNEL, concerns: |payload| payload.starts_with("worker:") },
    WakeOn::any(weft_task_store::terminal::TERMINAL_CHANNEL),
];

/// The safety look, for a claim that lapsed (its worker died), which no
/// write announces: 30 seconds at this install's pace.
fn safety() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(30))
}

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("delivery", WAKE_ON, safety(), move || {
        let state = state.clone();
        async move {
            let taken = weft_task_store::tasks::take_deliveries(&state.pg_pool, BATCH).await?;
            let full = taken.len() as i64 == BATCH;
            // Every row in the batch is already stamped delivered, so the
            // good ones go out first, and each undeliverable is ended on
            // its own: one that fails to end is logged and left to its
            // lease, which hands it back to a later pass, instead of
            // stranding the rest of the batch until theirs lapse.
            for delivery in taken.deliveries {
                let state = state.clone();
                tokio::spawn(async move { deliver(&state, delivery).await });
            }
            for undeliverable in taken.undeliverable {
                let task_id = undeliverable.task_id;
                let reason = undeliverable.reason.clone();
                if let Err(e) = end_undeliverable(&state, undeliverable).await {
                    tracing::error!(target: "weft_dispatcher::delivery", %task_id, %reason, error = %format!("{e:#}"), "could not end an undeliverable worker task; its lease retries it");
                }
            }
            Ok(if full { DrainStep::More } else { DrainStep::Done })
        }
    })
}

/// End an execution whose task can never be delivered (no image, no run
/// class): no worker will ever drive it, so nobody else would write its
/// terminal, and until one lands it shows running, its waiters hang and
/// its entry's at-once slot stays taken. It ends the way every run the
/// dispatcher ends itself does, a runtime cancel naming the reason
/// ([`crate::api::execution::cancel_execution_id`]), and only then is the
/// task failed: a crash between the two leaves the task taken, and the
/// next sweep ends it again, which the cancel's dedup keys make a no-op.
async fn end_undeliverable(
    state: &DispatcherState,
    undeliverable: weft_task_store::tasks::Undeliverable,
) -> anyhow::Result<()> {
    let weft_task_store::tasks::Undeliverable { task_id, execution_id, reason } = undeliverable;
    tracing::warn!(target: "weft_dispatcher::delivery", %task_id, ?execution_id, %reason, "undeliverable worker task: ending its execution");
    if let Some(execution_id) = execution_id {
        let cause = weft_core::exec::CancelCause::Runtime { detail: format!("could not be handed to a worker: {reason}") };
        crate::api::execution::cancel_execution_id(state, execution_id, &cause).await?;
    }
    weft_task_store::tasks::fail_undeliverable(&state.pg_pool, task_id, &reason).await?;
    Ok(())
}

/// The worker target an execution runs on: the project's workers at the
/// image its task was stamped with, with the project's own levers over
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
        settings: state.worker_defaults.with(overrides),
    })
}

async fn deliver(state: &DispatcherState, delivery: Delivery) {
    let execution_id = &delivery.execution_id;
    let outcome = match worker_target(state, &delivery.tenant_id, delivery.project_id, &delivery.binary_hash).await {
        Err(e) => Err(e),
        Ok(target) => match delivery.run_class {
            weft_core::run_class::RunClass::Short => run_short(state, &target, execution_id).await,
            weft_core::run_class::RunClass::Long => match execution_id.parse::<uuid::Uuid>() {
                Ok(c) => state.runner.start_long(&target, c, weft_platform_traits::Patience::Brief).await.map(|()| None),
                Err(e) => Err(anyhow::anyhow!("execution id '{execution_id}' is not a uuid: {e}")),
            },
        },
    };
    match outcome {
        Ok(None) => {}
        Ok(Some(answer::RunAnswer::Failed { error })) => tracing::warn!(
            target: "weft_dispatcher::delivery",
            %execution_id,
            %error,
            "the worker's drive of the execution failed; its task was failed with this"
        ),
        Ok(Some(answer)) => tracing::debug!(target: "weft_dispatcher::delivery", %execution_id, ?answer, "worker answered"),
        Err(e) => {
            // Nothing claimed it: give the delivery back so the next sweep
            // makes it again instead of waiting out the lease. A worker
            // still starting is the platform's bring-up going on without
            // this delivery (held only briefly, so its lease never lapses
            // into a second copy): the execution stays queued, and each
            // later sweep joins the same bring-up until it lands. The
            // platform logs that bring-up once; per delivery it is only
            // worth a debug line.
            match GiveBack::of(&e) {
                GiveBack::WorkerStarting => tracing::debug!(
                    target: "weft_dispatcher::delivery",
                    %execution_id,
                    project = %delivery.project_id,
                    reason = %format!("{e:#}"),
                    "the execution waits for its worker to start; it is handed out again on the next sweep"
                ),
                GiveBack::CouldNotHand => tracing::warn!(
                    target: "weft_dispatcher::delivery",
                    %execution_id,
                    project = %delivery.project_id,
                    error = %format!("{e:#}"),
                    "could not hand the execution to a worker; it is handed out again on the next sweep"
                ),
            }
            if let Err(e) = weft_task_store::tasks::release_delivery(&state.pg_pool, delivery.task_id).await {
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

/// Call the project's workers for one short run and hold the call until
/// the worker answers. `Ok(Some(answer))` once it answered.
async fn run_short(
    state: &DispatcherState,
    target: &weft_platform_traits::WorkerTarget,
    execution_id: &str,
) -> anyhow::Result<Option<answer::RunAnswer>> {
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
    let answer: answer::RunAnswer = resp.json().await.map_err(|e| anyhow::anyhow!("read the worker's answer: {e}"))?;
    // Held until here: a platform that stops idle workers itself counts
    // this hold, so the worker was never stopped under the call.
    drop(endpoint);
    Ok(Some(answer))
}

/// The worker's answer, as the dispatcher reads it. The dispatcher does
/// not link the engine, so the shape is restated here.
// SYNC: RunAnswer <-> crates/weft-engine/src/worker.rs RunAnswer
mod answer {
    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(tag = "ended", rename_all = "snake_case")]
    pub enum RunAnswer {
        Completed,
        Failed { error: String },
        LeaseLost,
        NothingToRun,
    }
}

#[cfg(test)]
mod tests {
    use super::answer::RunAnswer;
    use super::GiveBack;

    #[test]
    fn a_worker_still_starting_is_told_apart_from_a_failed_hand_off() {
        let starting = anyhow::Error::new(weft_platform_traits::WorkerStarting { what: "w".into() }).context("endpoint");
        assert_eq!(GiveBack::of(&starting), GiveBack::WorkerStarting, "found through a context");
        assert_eq!(GiveBack::of(&anyhow::anyhow!("the worker answered 500")), GiveBack::CouldNotHand);
    }

    #[test]
    fn the_workers_answer_reads_back() {
        assert_eq!(serde_json::from_value::<RunAnswer>(serde_json::json!({ "ended": "completed" })).unwrap(), RunAnswer::Completed);
        assert_eq!(serde_json::from_value::<RunAnswer>(serde_json::json!({ "ended": "nothing_to_run" })).unwrap(), RunAnswer::NothingToRun);
        assert!(matches!(
            serde_json::from_value::<RunAnswer>(serde_json::json!({ "ended": "failed", "error": "x" })).unwrap(),
            RunAnswer::Failed { .. }
        ));
    }
}
