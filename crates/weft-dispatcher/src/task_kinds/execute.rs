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
    // New executions are unpinned: a fresh color has no owner yet, and
    // the atomic task claim guarantees exactly one worker picks it up
    // (that worker becomes the owner). Pinning is only needed for resume
    // (see `enqueue_resume`), where a live owner may already exist.
    enqueue_execution(
        pool,
        TaskKind::Execute,
        project_id,
        color,
        definition_hash,
        tenant_id,
        None,
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
/// At most one worker ever runs per color at a time. With ONE worker
/// per project that held for free (only one pod could claim the
/// unpinned task). Now that a project can run MULTIPLE workers, an
/// unpinned resume could be claimed by a FRESH worker while the
/// original owner is still driving the color (e.g. held warm by a live
/// bus, resolving the suspension in place): the fresh worker would
/// claim, the broker would stamp it as the new owner (latest-claim-
/// wins), and the original's journal writes would then be rejected.
///
/// So we PIN the resume to the color's current owner when that owner is
/// still alive: the `target_pod_name` claim filter then guarantees only
/// the owner reclaims it, keeping "one active pod per color" true. When
/// the owner is gone (crashed / idle-exited / never assigned), the
/// resume stays unpinned: cold_start spawns a fresh pod, it claims, and
/// it legitimately takes over ownership. That handoff is the only time
/// ownership moves.
pub async fn enqueue_resume(
    pool: &sqlx::PgPool,
    project_id: &str,
    color: weft_core::Color,
    definition_hash: &str,
    tenant_id: Option<&str>,
) -> Result<()> {
    // Pin to the color's owner IFF it is still alive; a dead/absent owner
    // leaves the resume unpinned so a fresh pod takes over.
    let alive_owner = alive_color_owner(pool, color).await?;
    enqueue_execution(
        pool,
        TaskKind::Resume,
        project_id,
        color,
        definition_hash,
        tenant_id,
        alive_owner,
    )
    .await
}

/// The pod that currently OWNS a color, but only if that pod is still
/// alive (`spawning`/`alive`). `None` when the color has no owner or the
/// owner is gone. The single source of truth for "which pod is driving
/// this color right now," used to route any task that must reach the
/// driver: a resume (pin so the owner reclaims, keeping one-active-pod-
/// per-color) and a cancel (the cancel flag lives in the owner's
/// in-RAM registry, so the cancel must land on the owner or it no-ops).
/// Routing either to any other pod would also restamp ownership via the
/// claim trigger and fence the real owner, which is exactly why both go
/// through this one owner lookup rather than picking an arbitrary alive
/// pod for the project.
async fn alive_color_owner(
    pool: &sqlx::PgPool,
    color: weft_core::Color,
) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        r#"SELECT ec.owner_pod_name
           FROM execution_color ec
           JOIN worker_pod wp ON wp.pod_name = ec.owner_pod_name
           WHERE ec.color = $1
             AND ec.owner_pod_name IS NOT NULL
             AND wp.status IN ('spawning', 'alive')"#,
    )
    .bind(color.to_string())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(p,)| p))
}

async fn enqueue_execution(
    pool: &sqlx::PgPool,
    kind: TaskKind,
    project_id: &str,
    color: weft_core::Color,
    definition_hash: &str,
    tenant_id: Option<&str>,
    // The pod to pin the task to, or None to let any alive worker for
    // the project claim it. New executions pass None (a fresh color has
    // no owner; whichever worker claims first becomes owner, and the
    // atomic claim guarantees exactly one). Resume passes the alive
    // owner so a sibling worker can't steal a live color.
    target_pod_name: Option<String>,
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
            target_pod_name,
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
/// ATOMICALLY admit a live execution: pick the least-PRESSURED admittable
/// worker for the project (alive, not draining, memory below `saturation`) and
/// insert the pinned execute task on it, in one transaction (admission IS the
/// task insert; the task row is the durable pin). Returns the chosen pod, or
/// `None` if every worker is saturated / draining (caller spawns another and
/// retries). Capacity is memory-bounded, not a connection count.
pub async fn admit_live_execution(
    pool: &sqlx::PgPool,
    project_id: &str,
    color: weft_core::Color,
    definition_hash: &str,
    tenant_id: Option<&str>,
    live_spec: serde_json::Value,
    saturation: f64,
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
        saturation,
    )
    .await?;
    Ok(admitted)
}

/// Enqueue a `cancel_execution` task addressed to the Pod that OWNS
/// this color right now (the pod driving the execution), looked up via
/// `alive_color_owner`. The cancel flag lives in that pod's in-RAM
/// `cancel_registry`, so the cancel must reach the owner to have any
/// effect; routing it to the oldest alive pod (the previous behavior)
/// landed it on a sibling in a multi-pod pool, where it silently
/// no-opped AND, via the claim trigger, restamped color ownership to
/// the sibling and fenced the real owner mid-run. The
/// `task.target_pod_name` claim filter ensures only the owner claims it.
///
/// Returns `Ok(false)` if the color has no live owner (the execution is
/// already terminal or its worker is gone; nothing to cancel).
pub async fn enqueue_cancel(
    pool: &sqlx::PgPool,
    project_id: &str,
    color: weft_core::Color,
    tenant_id: Option<&str>,
) -> Result<bool> {
    let Some(pod_name) = alive_color_owner(pool, color).await? else {
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
