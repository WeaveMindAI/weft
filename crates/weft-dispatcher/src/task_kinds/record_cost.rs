//! `record_cost` task: durable handoff for a metered call's cost record
//! (a provider meter's figure, enqueued into the task table in one atomic
//! SQL INSERT), after which the producer can die freely. A dispatcher
//! pod claims this task on its own timeline and writes the
//! `CostReported` journal event. Survives worker pod deletion or
//! crash mid-flight.
//!
//! Payload validation (amount null-or-non-negative, worker records never
//! billed) runs at the broker on enqueue, so a compromised worker can't
//! sneak a bad row into the task table.

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
                    node_id: payload.node_id,
                    frames: payload.frames,
                    // The task id is the record's stable identity: retries
                    // of the same task keep it, distinct records never
                    // share it.
                    cost_id: task.id.to_string(),
                    service: payload.service,
                    model: payload.model,
                    amount_usd: payload.amount_usd,
                    billed: payload.billed,
                    origin: payload.origin,
                    metadata: payload.metadata,
                    at_unix,
                },
                &dedup_key,
            )
            .await?;
        Ok(serde_json::json!({}))
    }
}
