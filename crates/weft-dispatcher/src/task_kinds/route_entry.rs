//! `route_entry` task: a dispatcher Pod loads the project,
//! computes trigger seeds, journals ExecutionStarted + PulseSeeded
//! events, and enqueues an execute task. Used by the listener
//! when an entry-trigger fire arrives.
//!
//! Idempotency: dedup on signal token collapses concurrent
//! enqueues; the executor itself derives `color` from the task id
//! via UUIDv5 so a partial-success retry re-emits identical
//! ExecutionStarted / PulseSeeded events instead of forking the
//! execution.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

/// Namespace UUID used to derive deterministic execution colors
/// from task ids. Generated once via `Uuid::new_v4` and frozen.
const COLOR_NAMESPACE: Uuid = Uuid::from_u128(0x9c4a_e6a4_0b3f_4e8e_a0f1_1d3d_9b2c_5a47);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntryPayload {
    /// Token of the signal that fired. Used to look up
    /// `(project_id, node_id)` in the `signal` table at execute
    /// time. The listener doesn't pass project_id directly because
    /// the dispatcher's project store has the up-to-date copy.
    pub token: String,
    /// Payload the trigger fire carried.
    pub payload: Value,
    /// Tenant id, propagated to the spawned execute task for
    /// listener-side resolution of tenant-scoped resources.
    pub tenant_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntryResult {
    pub color: String,
}

pub struct RouteEntryExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for RouteEntryExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RouteEntryPayload = serde_json::from_value(task.payload.clone())?;
        let signal = state
            .journal
            .signal_get(&payload.token)
            .await?
            .ok_or_else(|| anyhow::anyhow!("signal {} not found", payload.token))?;
        let project_uuid: Uuid = signal.project_id.parse()?;
        let project_def = state
            .projects
            .project(project_uuid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("project {} not registered", signal.project_id))?;

        let seeds = crate::api::project::compute_trigger_seeds(
            &project_def,
            &signal.node_id,
            &payload.payload,
        );
        if seeds.is_empty() {
            anyhow::bail!(
                "trigger '{}' has no output downstream; nothing to run",
                signal.node_id
            );
        }

        // Derive color from the task id so a retry of the same
        // task replays the same ExecutionStarted / PulseSeeded
        // events. Each event carries a dedup key so the journal's
        // partial UNIQUE collapses duplicates atomically.
        let color = Uuid::new_v5(&COLOR_NAMESPACE, task.id.as_bytes());
        let now = crate::lease::now_unix() as u64;
        let task_id = task.id;
        state
            .journal
            .record_event_dedup(
                &weft_journal::ExecEvent::ExecutionStarted {
                    color,
                    project_id: signal.project_id.clone(),
                    entry_node: signal.node_id.clone(),
                    phase: weft_core::context::Phase::Fire,
                    at_unix: now,
                },
                &format!("route_entry:{task_id}:start"),
            )
            .await?;
        for seed in &seeds {
            state
                .journal
                .record_event_dedup(
                    &weft_journal::ExecEvent::PulseSeeded {
                        color,
                        pulse_id: seed.pulse_id.clone(),
                        node_id: seed.node_id.clone(),
                        port: "__seed__".to_string(),
                        lane: Vec::new(),
                        value: seed.value.clone(),
                        at_unix: now,
                    },
                    &format!("route_entry:{task_id}:seed:{}", seed.pulse_id),
                )
                .await?;
        }

        // Enqueue an `execute` task targeted at the worker pool. The
        // cold-start trigger spawns a Pod for this project if none is
        // alive; the worker's claim loop folds the journal and runs.
        crate::task_kinds::execute::enqueue_execute(
            &state.pg_pool,
            &signal.project_id,
            color,
            Some(&payload.tenant_id),
        )
        .await?;

        // Entry triggers are persistent: registered once at
        // TriggerSetup, fire many times until deactivate. The signal
        // row stays. Single-use resume signals are deleted in the
        // resume path, not here.

        // SSE for ExecutionStarted is emitted by the journal bridge
        // on its next poll. We don't publish inline because a retry
        // of this task would double-emit; the bridge keys off the
        // event log itself, which the dedup key keeps single-write.

        Ok(serde_json::to_value(RouteEntryResult {
            color: color.to_string(),
        })?)
    }
}
