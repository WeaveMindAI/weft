//! `record_cost` task: durable handoff for a worker's
//! `ctx.report_cost`. The worker enqueues into the broker's task
//! table (one atomic SQL INSERT), then can die freely. A dispatcher
//! pod claims this task on its own timeline and writes the
//! `CostReported` journal event. Survives worker pod deletion or
//! crash mid-flight.
//!
//! Negative-amount rejection runs at the broker on enqueue, so a
//! compromised worker can't sneak a negative cost into the task
//! table.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;
use weft_task_store::RecordCostPayload;

use crate::state::DispatcherState;

pub struct RecordCostExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for RecordCostExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RecordCostPayload = serde_json::from_value(task.payload.clone())?;
        let color: weft_core::Color = payload
            .color
            .parse()
            .map_err(|e| anyhow::anyhow!("bad color in record_cost payload: {e}"))?;
        let at_unix = crate::lease::now_unix() as u64;
        // Dedup at the journal layer too: a task-executor retry of
        // the same task re-runs `execute` (lease loss + reclaim).
        // Without a journal-side dedup_key, retries would double-
        // count cost. Task id is stable across retries of the same
        // task row.
        let dedup_key = format!("record_cost:{}", task.id);
        state
            .journal
            .record_event_dedup(
                &weft_journal::ExecEvent::CostReported {
                    color,
                    service: payload.service,
                    model: payload.model,
                    amount_usd: payload.amount_usd,
                    metadata: payload.metadata,
                    at_unix,
                },
                &dedup_key,
            )
            .await?;
        Ok(serde_json::json!({}))
    }
}
