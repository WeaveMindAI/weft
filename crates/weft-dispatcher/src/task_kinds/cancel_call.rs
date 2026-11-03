//! `cancel_call` task: a call weft owes for a run nobody drives any more
//! (`weft_core::signal::cancel_call`): a run was cancelled while it waited
//! on an outside job whose wait carried the service's own cancel address,
//! or a run ended with no worker while a node of it owed a call
//! (`ctx.after_node`). The cancel queued this in the transaction that
//! ended the wait or the run; here the listener sends it, once, as its
//! connection.
//!
//! A call the service refuses, or that cannot reach it, fails the task
//! with why, logged as an error: the job may still be running and
//! charging, or what the node made may still be there, and nothing
//! retries a call to an outside service on its own.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_core::signal::listener_protocol::CancelCallRequest;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

pub struct CancelCallExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for CancelCallExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let call: CancelCallRequest = serde_json::from_value(task.payload.clone())?;
        let on_cancel = &call.call;
        if let Err(e) = state.listener.cancel_call(&call).await {
            tracing::error!(
                target: "weft_dispatcher::cancel_call",
                run_id = ?task.run_id, token = %call.token, method = on_cancel.method.as_http(), url = %on_cancel.url,
                error = %format!("{e:#}"),
                "a last call to an outside service failed; the job it was about may still be running and charging, or what the node made may still be there"
            );
            return Err(e);
        }
        tracing::info!(
            target: "weft_dispatcher::cancel_call",
            run_id = ?task.run_id, token = %call.token, method = on_cancel.method.as_http(), url = %on_cancel.url,
            "sent a last call to an outside service"
        );
        Ok(serde_json::json!({ "sent": true }))
    }
}
