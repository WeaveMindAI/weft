//! `withdraw_signal` task: a run gave up a wait it could not pause on
//! (its worker held it, and the hold ran out), so the wait's signal goes
//! and the listener lets go of it: a form is no longer offered, a timer
//! no longer ticks for it. Only a wait of the run that sent the task is
//! deleted (the delete itself matches the run), whatever token it names.
//!
//! Idempotent: a wait answered, or withdrawn, already has no signal, and
//! a second pass removes nothing.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;
use weft_task_store::WithdrawSignalPayload;

use crate::state::DispatcherState;

pub struct WithdrawSignalExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for WithdrawSignalExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: WithdrawSignalPayload = serde_json::from_value(task.payload.clone())?;
        let removed: Vec<_> = state.journal.signal_withdraw(payload.execution_id, &payload.token).await?.into_iter().collect();
        state.listener.unregister_many(&removed).await;
        tracing::info!(
            target: "weft_dispatcher::withdraw_signal",
            execution_id = %payload.execution_id,
            token = %payload.token,
            removed = removed.len(),
            "withdrew a wait its run gave up"
        );
        Ok(serde_json::json!({ "removed": removed.len() }))
    }
}
