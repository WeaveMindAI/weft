//! `stop_tagged` task: one execution stopping its siblings by tag
//! (`ctx.stop_tagged`). The broker enqueued it when the worker asked,
//! with the ordering anchor (`before_seq`) resolved THEN; here a
//! dispatcher pod reads the live executions carrying the tag, applies
//! the pure stop rule, and cancels each match through the ONE cancel
//! path (`cancel_color`), so a tag stop reaches a parked run exactly the
//! way `weft stop` does: its wake signals are stripped first, its
//! owning worker (if any) gets the flag with the cause, and the journal
//! reaches `ExecutionCancelled` naming who asked and which tag matched.
//!
//! Idempotent under retry: a second pass finds the first pass's targets
//! already terminal (the live read excludes them) and stops nothing new.
//! Anything that tagged itself after the ask sits above `before_seq` and
//! is never reached, however late this runs. Every selected target is
//! attempted before the task reports: one failing cancel must not strand
//! the runs after it, and it must not vanish either, so any failure fails
//! the task with every failed color and its error in the task row.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_core::exec::CancelCause;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;
use weft_task_store::StopTaggedPayload;

use crate::state::DispatcherState;

pub struct StopTaggedExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for StopTaggedExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: StopTaggedPayload = serde_json::from_value(task.payload.clone())?;
        let by: weft_core::Color = payload
            .by
            .parse()
            .map_err(|e| anyhow::anyhow!("bad `by` color in stop_tagged payload: {e}"))?;
        let stopped = stop_tagged(
            state, &payload.project_id, &payload.tag, by, payload.before_seq, payload.stop_self,
        )
        .await?;
        Ok(serde_json::json!({ "stopped": stopped.len() }))
    }
}

/// Stop every live execution of `project_id` the rule selects, and
/// return the colors stopped. The SELECTION is
/// `weft_journal::tags::select_stop_targets`, the CANCEL is
/// `cancel_colors`, nothing here is a third path.
pub async fn stop_tagged(
    state: &DispatcherState,
    project_id: &str,
    tag: &str,
    by: weft_core::Color,
    before_seq: Option<i64>,
    stop_self: weft_core::StopSelf,
) -> Result<Vec<weft_core::Color>> {
    let live = state.journal.live_tagged_executions(project_id, tag).await?;
    let targets = weft_journal::tags::select_stop_targets(&live, by, before_seq, stop_self);
    tracing::info!(
        target: "weft_dispatcher::stop_tagged",
        project = %project_id,
        tag,
        %by,
        ?before_seq,
        ?stop_self,
        live = live.len(),
        stopping = targets.len(),
        "stop_tagged"
    );
    let cause = CancelCause::Execution { by, tag: tag.to_string() };
    let targets: Vec<(weft_core::Color, &CancelCause)> = targets.iter().map(|c| (*c, &cause)).collect();
    crate::api::execution::cancel_colors(state, &targets).await
}
