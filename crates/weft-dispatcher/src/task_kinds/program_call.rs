//! `program_call` task: one call a program makes on a project
//! (`weft_core::program::ProgramCall`). The broker enqueued it for the
//! asking run, under that run's project and owner; a dispatcher carries it
//! out here as that run, through the same actions the CLI and the editor
//! reach (`crate::actions::program_call`), and the worker reads the answer
//! off the task.
//!
//! A call that takes something down never reaches the run that asked: the
//! take-down engines take the asker as the one run to leave alone. With
//! `StopSelf::Include`, and only when the asker is among what the call
//! reaches, the asker is cancelled once the take-down is queued, and the
//! answer says so (`stops_asker`), so the worker waits for its own cancel
//! instead of running on.
//!
//! A replayed run never issues a call twice: the worker journals each
//! answer (`ctx.run`) and a replay reads it back. A call that did run but
//! whose answer was lost is issued again, so every call is safe to repeat:
//! starting a running copy, taking down what is already down, activating
//! what is active, forgetting what is gone all answer without doing
//! anything. A run a call starts is named after the call
//! (`weft_core::program::program_call_id`, of the task's dedup key), so
//! the call issued again answers the run it started instead of starting
//! another.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_core::program::ProgramCallPayload;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

pub struct ProgramCallExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for ProgramCallExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: ProgramCallPayload = serde_json::from_value(task.payload.clone())?;
        let asker_project = task.project_id.ok_or_else(|| anyhow::anyhow!("program_call task names no project"))?;
        // The worker keys every call to its step (`weft_engine` context's
        // program_call), and the broker refuses one with no key.
        let step = task.dedup_key.as_deref().ok_or_else(|| anyhow::anyhow!("program_call task names no step"))?;
        let actor = crate::actions::Actor::Program {
            tenant: crate::tenant::TenantId(task.tenant_id.clone()),
            project: asker_project,
            run: payload.by,
            call: weft_core::program::program_call_id(step),
        };
        let target = payload.project.unwrap_or(asker_project);
        let outcome = crate::actions::program_call(state, &actor, target, &payload.call, payload.stop_self)
            .await
            .map_err(|(_, message)| anyhow::anyhow!("{message}"))?;
        Ok(serde_json::to_value(outcome)?)
    }
}
