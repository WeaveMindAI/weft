//! `record_log` task: durable handoff for a worker's `ctx.log`.
//! Same shape as `record_cost`: worker enqueues atomically, dies if
//! it wants, a dispatcher pod writes the journal event later.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;
use weft_task_store::RecordLogPayload;

use crate::state::DispatcherState;

pub struct RecordLogExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for RecordLogExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RecordLogPayload = serde_json::from_value(task.payload.clone())?;
        let color: weft_core::Color = payload
            .color
            .parse()
            .map_err(|e| anyhow::anyhow!("bad color in record_log payload: {e}"))?;
        let at_unix = crate::lease::now_unix() as u64;
        let dedup_key = format!("record_log:{}", task.id);
        state
            .journal
            .record_event_dedup(
                &weft_journal::ExecEvent::LogLine {
                    color,
                    level: payload.level,
                    message: payload.message,
                    at_unix,
                },
                &dedup_key,
            )
            .await?;
        Ok(serde_json::json!({}))
    }
}
