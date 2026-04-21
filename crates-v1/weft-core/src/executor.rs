//! Restate auxiliary services.
//!
//! Only durable state services that the axum executor calls via HTTP:
//! - TaskRegistry: tracks pending form-based tasks (feedback, triggers, actions)
//!
//! All project execution logic lives in `executor_axum` (in-memory) and
//! `executor_core` (shared pure functions).

use restate_sdk::prelude::*;

use crate::executor_core::{
    PendingTask, PendingTasksList, TaskType,
};

// =============================================================================
// TASK REGISTRY
// =============================================================================

#[restate_sdk::object]
pub trait TaskRegistry {
    async fn register_task(task: Json<PendingTask>) -> Result<(), HandlerError>;
    async fn complete_task(executionId: String) -> Result<(), HandlerError>;
    /// Force-remove a task regardless of type (used when triggers shut down).
    async fn remove_task(executionId: String) -> Result<(), HandlerError>;

    #[shared]
    async fn list_tasks() -> Result<Json<PendingTasksList>, HandlerError>;
}

pub struct TaskRegistryImpl;

impl TaskRegistry for TaskRegistryImpl {
    async fn register_task(
        &self,
        ctx: ObjectContext<'_>,
        Json(task): Json<PendingTask>,
    ) -> Result<(), HandlerError> {
        let tasks_json: String = ctx.get("tasks").await?.unwrap_or_else(|| "[]".to_string());
        let mut tasks: Vec<PendingTask> = serde_json::from_str(&tasks_json)
            .map_err(|e| TerminalError::new(format!("BUG: corrupt tasks JSON: {}", e)))?;
        tasks.push(task);
        let new_json = serde_json::to_string(&tasks)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("tasks", new_json);
        Ok(())
    }

    async fn complete_task(
        &self,
        ctx: ObjectContext<'_>,
        execution_id: String,
    ) -> Result<(), HandlerError> {
        let tasks_json: String = ctx.get("tasks").await?.unwrap_or_else(|| "[]".to_string());
        let mut tasks: Vec<PendingTask> = serde_json::from_str(&tasks_json)
            .map_err(|e| TerminalError::new(format!("BUG: corrupt tasks JSON: {}", e)))?;
        // Trigger-type tasks are persistent: they stay registered after submission.
        // Only non-trigger tasks are removed on completion.
        tasks.retain(|t| t.executionId != execution_id || t.taskType == TaskType::Trigger);
        let new_json = serde_json::to_string(&tasks)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("tasks", new_json);
        Ok(())
    }

    async fn remove_task(
        &self,
        ctx: ObjectContext<'_>,
        execution_id: String,
    ) -> Result<(), HandlerError> {
        let tasks_json: String = ctx.get("tasks").await?.unwrap_or_else(|| "[]".to_string());
        let mut tasks: Vec<PendingTask> = serde_json::from_str(&tasks_json)
            .map_err(|e| TerminalError::new(format!("BUG: corrupt tasks JSON: {}", e)))?;
        tasks.retain(|t| t.executionId != execution_id);
        let new_json = serde_json::to_string(&tasks)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("tasks", new_json);
        Ok(())
    }

    async fn list_tasks(
        &self,
        ctx: SharedObjectContext<'_>,
    ) -> Result<Json<PendingTasksList>, HandlerError> {
        let tasks_json: String = ctx.get("tasks").await?.unwrap_or_else(|| "[]".to_string());
        let tasks: Vec<PendingTask> = serde_json::from_str(&tasks_json)
            .map_err(|e| TerminalError::new(format!("BUG: corrupt tasks JSON: {}", e)))?;
        Ok(Json(PendingTasksList { tasks }))
    }
}

