//! Producer helpers for `execute`, `resume`, and `cancel_execution`
//! tasks. The first two are the work a worker is called for (the
//! dispatcher's `delivery` hands each to the project's workers); a cancel
//! is heard by the worker driving its execution, announced on its line,
//! which asks for it. The handlers live in `weft_engine::worker`.

use anyhow::Result;

use weft_task_store::tasks::{enqueue_or_rearm, NewTask, TaskTarget};
use weft_task_store::{CancelExecutionPayload, ExecutionPayload, TaskKind};

/// Enqueue a `resume` task for `execution_id`. Dedup key is `{execution_id}:resume` so
/// multiple fires arriving while a worker is already running coalesce:
/// the in-flight worker observes the fresh `SuspensionResolved` rows
/// during its pre-Stalled re-fetch loop (see `run_one_execution`). Once
/// that worker completes, a fire arriving afterwards gets a fresh resume
/// task because the prior dedup row has transitioned to `complete`.
///
/// At most one worker drives an execution at a time: a resume is neither
/// delivered nor claimable while another task of its execution is being
/// driven (`tasks::EXECUTION_ID_PICK`), so it waits for that drive to end
/// instead of folding the journal a second time beside it.
///
/// A resume already claimed is asked to run once more rather than
/// collapsed onto (`enqueue_or_rearm`): its worker may have read the
/// journal for the last time before this wake was written, and would
/// otherwise finish without driving it.
///
/// The resume runs on the image and under the run class the execution
/// was born with, both read off its `ExecutionStarted`.
pub async fn enqueue_resume(
    pool: &sqlx::PgPool,
    project_id: uuid::Uuid,
    execution_id: weft_core::ExecutionId,
    definition_hash: &str,
    tenant_id: &str,
) -> Result<()> {
    let payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM exec_event WHERE execution_id = $1 AND kind = 'execution_started' ORDER BY id LIMIT 1",
    )
    .bind(execution_id.to_string())
    .fetch_one(pool)
    .await?;
    let birth: weft_journal::ExecEvent = serde_json::from_str(&payload)?;
    let weft_journal::ExecEvent::ExecutionStarted { program: Some(program), project_id: recorded_project, run_class, .. } = birth
    else {
        anyhow::bail!("execution {execution_id} has no recorded production code identity");
    };
    anyhow::ensure!(
        recorded_project == project_id && program.definition_hash == definition_hash,
        "execution {execution_id} does not match the requested project and graph"
    );
    let task = execution_task_spec(ExecutionTask {
        kind: TaskKind::Resume,
        project_id,
        execution_id,
        definition_hash,
        binary_hash: &program.binary_hash,
        tenant_id,
        run_class,
        live_connection: None,
        unrecorded_birth: None,
    })?;
    enqueue_or_rearm(pool, task).await?;
    Ok(())
}

/// What an execution-family task (`execute` / `resume`) is built from.
pub struct ExecutionTask<'a> {
    pub kind: TaskKind,
    pub project_id: uuid::Uuid,
    pub execution_id: weft_core::ExecutionId,
    pub definition_hash: &'a str,
    /// The worker image the task runs on.
    pub binary_hash: &'a str,
    pub tenant_id: &'a str,
    pub run_class: weft_core::run_class::RunClass,
    /// `Some(start)` for a live-caller execution (the worker expects a
    /// caller to attach). One whose caller is on the way
    /// (`LiveConnectionStart::arrive_by`) is never delivered: the worker
    /// the caller reaches claims it, and the claim pins it there.
    pub live_connection: Option<weft_task_store::kinds::LiveConnectionStart>,
    /// An unrecorded run's birth rows (`ExecutionPayload::unrecorded_birth`),
    /// `None` for every recorded run.
    pub unrecorded_birth: Option<&'a [weft_journal::ExecEvent]>,
}

/// Build the `NewTask` for an execution-family task, UNQUEUED: the caller
/// decides how it is inserted (a plain dedup'd enqueue, or committed
/// atomically with the execution's journal birth via
/// `Journal::start_execution`).
pub fn execution_task_spec(task: ExecutionTask<'_>) -> Result<NewTask> {
    let execution_id_str = task.execution_id.to_string();
    let payload = ExecutionPayload {
        project_id: task.project_id,
        execution_id: execution_id_str.clone(),
        definition_hash: task.definition_hash.to_string(),
        live_connection: task.live_connection,
        unrecorded_birth: task
            .unrecorded_birth
            .map(|rows| rows.iter().map(serde_json::to_value).collect::<Result<Vec<_>, _>>())
            .transpose()?,
        run_class: task.run_class,
    };
    let dedup = format!("{execution_id_str}:{}", task.kind.as_str());
    Ok(NewTask {
        kind: task.kind.into(),
        target: TaskTarget::Worker,
        project_id: Some(task.project_id),
        dedup_key: Some(dedup),
        execution_id: Some(execution_id_str),
        tenant_id: task.tenant_id.to_string(),
        target_replica: None,
        binary_hash: Some(task.binary_hash.to_string()),
        payload: serde_json::to_value(&payload)?,
    })
}

/// Enqueue a `cancel_execution` task for `execution_id` when a worker is driving
/// it right now (its execute or resume task holds a claim that is being
/// renewed). The cancel flag lives in that worker's memory; the worker
/// hears the task announced on its line, asks for it, and fires the flag.
///
/// `cause` rides in the payload so the worker flips the execution's flag WITH
/// it: when the worker's terminal write beats the dispatcher's, the
/// journal still names the same cause.
///
/// Returns `Ok(false)` if nothing drives the execution (it is waiting, is
/// terminal, or its worker is gone): the caller's own terminal write is
/// then the whole cancel.
///
/// Runs on the caller's connection so the journal's cancel write can
/// commit it in the same transaction as the terminal rows and the
/// wake-signal strip (`Journal::cancel_execution`): a cancel either lands
/// whole or not at all.
pub async fn enqueue_cancel_in(
    conn: &mut sqlx::PgConnection,
    project_id: uuid::Uuid,
    execution_id: weft_core::ExecutionId,
    tenant_id: &str,
    cause: &weft_core::exec::CancelCause,
) -> Result<bool> {
    let execution_id_str = execution_id.to_string();
    let driven: bool = sqlx::query_scalar(
        r#"SELECT EXISTS (
               SELECT 1 FROM task
               WHERE execution_id = $1 AND kind IN ('execute', 'resume')
                 AND status = 'claimed'
                 AND claimed_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT)"#,
    )
    .bind(&execution_id_str)
    .fetch_one(&mut *conn)
    .await?;
    if !driven {
        return Ok(false);
    }
    let payload = CancelExecutionPayload { project_id, execution_id: execution_id_str.clone(), cause: cause.clone() };
    weft_task_store::tasks::enqueue_dedup_in(
        conn,
        NewTask {
            kind: TaskKind::CancelExecution.into(),
            target: TaskTarget::Worker,
            project_id: Some(project_id),
            dedup_key: Some(format!("{execution_id_str}:cancel")),
            execution_id: Some(execution_id_str),
            tenant_id: tenant_id.to_string(),
            target_replica: None,
            binary_hash: None,
            payload: serde_json::to_value(&payload)?,
        },
    )
    .await?;
    Ok(true)
}
