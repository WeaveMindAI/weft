//! Producer helpers for `execute`, `resume`, and `cancel_execution`
//! tasks. These all target the per-project worker pool, NOT the
//! dispatcher: the worker's run_pod claim loop in weft-engine
//! consumes them. The handlers (`ExecuteKind`, `CancelExecutionKind`)
//! live in weft-engine.

use anyhow::Result;

use weft_task_store::tasks::{enqueue_dedup, NewTask, TaskTarget};
use weft_task_store::{CancelExecutionPayload, ExecutionPayload, TaskKind};

/// Enqueue an `execute` task scoped to (project_id, color). Dedup on
/// color so racing `/run` calls converge.
///
/// `definition_hash` is the project row's `running_definition_hash`
/// at enqueue time; the worker uses it as the broker's
/// `expected_hash` so the execution runs on the project shape the
/// user clicked Run against, even when a later edit changes the
/// hash before the worker claims.
pub async fn enqueue_execute(
    pool: &sqlx::PgPool,
    project_id: &str,
    color: weft_core::Color,
    definition_hash: &str,
    tenant_id: Option<&str>,
) -> Result<()> {
    enqueue_execution(
        pool,
        TaskKind::Execute,
        project_id,
        color,
        definition_hash,
        tenant_id,
    )
    .await
}

/// Enqueue a `resume` task for `color`. Dedup key is `{color}:resume`
/// so multiple fires arriving while a worker is already running
/// coalesce: the in-flight worker is expected to observe the fresh
/// SuspensionResolved rows during its pre-Stalled re-fetch loop
/// (see `run_one_execution`). Once that worker completes, a fire
/// arriving afterwards spawns a fresh resume task because the prior
/// dedup row has transitioned to `complete`.
///
/// At most one worker ever runs per color at a time. Without that
/// invariant, multiple workers race the same journal stream and
/// emit duplicate NodeResumed/NodeStarted/NodeCompleted events.
pub async fn enqueue_resume(
    pool: &sqlx::PgPool,
    project_id: &str,
    color: weft_core::Color,
    definition_hash: &str,
    tenant_id: Option<&str>,
) -> Result<()> {
    enqueue_execution(
        pool,
        TaskKind::Resume,
        project_id,
        color,
        definition_hash,
        tenant_id,
    )
    .await
}

async fn enqueue_execution(
    pool: &sqlx::PgPool,
    kind: TaskKind,
    project_id: &str,
    color: weft_core::Color,
    definition_hash: &str,
    tenant_id: Option<&str>,
) -> Result<()> {
    let color_str = color.to_string();
    let payload = ExecutionPayload {
        project_id: project_id.to_string(),
        color: color_str.clone(),
        definition_hash: definition_hash.to_string(),
        live_connection: None,
    };
    let dedup = format!("{color_str}:{}", kind.as_str());
    enqueue_dedup(
        pool,
        NewTask {
            kind,
            target: TaskTarget::Worker,
            project_id: Some(project_id.to_string()),
            dedup_key: Some(dedup),
            color: Some(color_str),
            tenant_id: tenant_id.map(str::to_string),
            target_pod_name: None,
            payload: serde_json::to_value(&payload)?,
        },
    )
    .await?;
    Ok(())
}

/// Enqueue a fresh execution STARTED by a live-caller handshake, pinned to
/// a specific worker pod (per-pod addressing) and carrying the trigger's
/// full signal spec (tag + config) so the worker recovers the protocol and
/// the connection knobs and expects a caller to attach for this color. The
/// `target_pod_name` claim filter guarantees ONLY the chosen pod runs it, so
/// the caller (routed to that same pod by the gateway) and the execution
/// land on the same process.
/// ATOMICALLY admit a live execution: pick the least-loaded under-cap worker
/// pod for the project and insert the pinned execute task on it, in one
/// transaction (admission IS the task insert; the task row is the capacity
/// slot, so there is no separate counter to drift). Returns the chosen pod,
/// or `None` if every pod is at `cap` (caller spawns another and retries).
pub async fn admit_live_execution(
    pool: &sqlx::PgPool,
    project_id: &str,
    color: weft_core::Color,
    definition_hash: &str,
    tenant_id: Option<&str>,
    live_spec: serde_json::Value,
    cap: i32,
) -> Result<Option<weft_task_store::tasks::AdmittedPod>> {
    let color_str = color.to_string();
    let payload = ExecutionPayload {
        project_id: project_id.to_string(),
        color: color_str.clone(),
        definition_hash: definition_hash.to_string(),
        live_connection: Some(live_spec),
    };
    let payload_json = serde_json::to_value(&payload)?;
    let admitted = weft_task_store::tasks::admit_live_execution(
        pool,
        project_id,
        &color_str,
        tenant_id,
        &payload_json,
        cap,
    )
    .await?;
    Ok(admitted)
}

/// Enqueue a `cancel_execution` task addressed to the Pod that owns
/// the project pool right now. The task's `target_pod_name` field
/// is set to the owning Pod (looked up via
/// `worker_pod::alive_pod_for_project`); the claim filter on
/// `task.target_pod_name` ensures only that Pod can claim the row,
/// so a sibling Pod in a multi-Pod pool can't accidentally consume
/// the cancel.
///
/// Returns `Ok(false)` if no live Pod exists for the project (the
/// execution must already be terminal; nothing to cancel).
pub async fn enqueue_cancel(
    pool: &sqlx::PgPool,
    project_id: &str,
    color: weft_core::Color,
    tenant_id: Option<&str>,
) -> Result<bool> {
    let Some(pod_name) =
        weft_task_store::worker_pod::alive_pod_for_project(pool, project_id).await?
    else {
        return Ok(false);
    };
    let color_str = color.to_string();
    let payload = CancelExecutionPayload {
        project_id: project_id.to_string(),
        color: color_str.clone(),
    };
    let dedup = format!("{color_str}:cancel");
    enqueue_dedup(
        pool,
        NewTask {
            kind: TaskKind::CancelExecution,
            target: TaskTarget::Worker,
            project_id: Some(project_id.to_string()),
            dedup_key: Some(dedup),
            color: Some(color_str),
            tenant_id: tenant_id.map(str::to_string),
            target_pod_name: Some(pod_name),
            payload: serde_json::to_value(&payload)?,
        },
    )
    .await?;
    Ok(true)
}
