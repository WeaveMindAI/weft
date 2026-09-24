//! Waking whoever waits on a task the moment it finishes.
//!
//! Every write that makes a task terminal (`complete`, `fail`,
//! `fail_pending`) sends `pg_notify(TERMINAL_CHANNEL, <task id>)` in the
//! same statement, so the notification goes out exactly when the row
//! commits. The process's [`PgSignalWatch`] hears it and the waiter reads
//! the row, so a wait ends when the task does, instead of on the next
//! tick of a poll.

use std::time::Duration;

use anyhow::Result;
use sqlx::postgres::PgPool;
use uuid::Uuid;

use crate::pg_signal::PgSignalWatch;

/// The channel a terminal task write notifies on, with the task's id as
/// the payload.
pub const TERMINAL_CHANNEL: &str = "weft_task_terminal";

/// Wait for `task_id` to finish, or for `timeout` to pass, and hand back
/// its outcome either way (a timed-out outcome is not terminal).
pub async fn wait_for_terminal(
    pool: &PgPool,
    signals: &PgSignalWatch,
    task_id: Uuid,
    timeout: Duration,
) -> Result<crate::tasks::TaskOutcome> {
    wait_until_terminal(signals, task_id, timeout, || crate::tasks::peek(pool, task_id))
        .await?
        .ok_or_else(|| anyhow::anyhow!("task {task_id} disappeared"))
}

/// [`wait_for_terminal`] for a task of `project_id` only: every read is
/// scoped to the project, so `None` when no such task belongs to it (or
/// it went away while waiting).
pub async fn wait_for_terminal_in_project(
    pool: &PgPool,
    signals: &PgSignalWatch,
    task_id: Uuid,
    project_id: Uuid,
    timeout: Duration,
) -> Result<Option<crate::tasks::TaskOutcome>> {
    wait_until_terminal(signals, task_id, timeout, || {
        crate::tasks::peek_for_project(pool, task_id, project_id)
    })
    .await
}

/// The wait both share: read the task with `peek`, and until it is
/// terminal, the timeout passes, or the read finds nothing, wait for its
/// terminal notification and read again.
async fn wait_until_terminal<F, Fut>(
    signals: &PgSignalWatch,
    task_id: Uuid,
    timeout: Duration,
    peek: F,
) -> Result<Option<crate::tasks::TaskOutcome>>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<Option<crate::tasks::TaskOutcome>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    // Subscribed before the first read, so a finish between the read and
    // the wait is still heard.
    let mut signals = signals.subscribe();
    let id = task_id.to_string();
    loop {
        let Some(outcome) = peek().await? else {
            return Ok(None);
        };
        if outcome.status.is_terminal() {
            return Ok(Some(outcome));
        }
        if !signals.woken_before(deadline, |c, p| c == TERMINAL_CHANNEL && p == id).await? {
            return Ok(Some(outcome));
        }
    }
}
