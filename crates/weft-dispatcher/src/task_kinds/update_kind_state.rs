//! `update_signal_kind_state` task: persist a signal kind's evolving
//! durable state (a delta-poll cursor) onto its signal row. The
//! listener enqueues it through the broker exactly like `fire_signal`
//! (tenant pods never speak HTTP to the dispatcher); the journal's
//! fenced write rejects a drained pod's stale update (placement
//! generation) and an out-of-order older update (the row's
//! `kind_state_seq`), both of which are normal races, not errors.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;
use weft_task_store::UpdateSignalKindStatePayload;

use crate::state::DispatcherState;

pub struct UpdateSignalKindStateExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for UpdateSignalKindStateExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: UpdateSignalKindStatePayload = serde_json::from_value(task.payload.clone())?;
        let written = state
            .journal
            .signal_update_kind_state(
                &payload.token,
                &payload.kind_state,
                payload.seq,
                payload.placement_generation,
            )
            .await?;
        if !written {
            // Three causes share this outcome, all normal races: an
            // out-of-order or stale-pod update (the newer state
            // stands), an unregistered signal (nothing to update), or
            // a save from a just-spawned loop racing the row insert
            // of its own fresh registration (the loop's next save
            // carries the full state and lands). Warn, not debug: a
            // dropped save is harmless in the moment but shortens
            // what a restart can recover, so it should be findable.
            tracing::warn!(
                target: "weft_dispatcher::update_kind_state",
                token = %payload.token, seq = payload.seq,
                generation = payload.placement_generation,
                "kind-state update not applied (out-of-order seq, stale pod, or no \
                 signal row yet); the stored state stands"
            );
        }
        Ok(serde_json::json!({ "written": written }))
    }
}
