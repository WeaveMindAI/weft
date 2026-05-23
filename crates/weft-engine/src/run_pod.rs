//! Pod-level entry point for a worker. Boots the `worker_pod` row,
//! builds a `WorkerTaskRegistry` over `execute` / `resume` /
//! `cancel_execution`, and runs the shared worker picker until the
//! Pod is told to shut down.
//!
//! Lifecycle:
//!   1. `register_alive` writes the worker_pod row (via the broker).
//!   2. The heartbeat task keeps it fresh; if the row goes away
//!      (drained, reaped) we set `shutdown=true`.
//!   3. The picker claims worker tasks for our project_id. `execute`
//!      / `resume` are spawned in the background (per-task heartbeat
//!      keeps the claim alive while they run). `cancel_execution`
//!      runs inline against the pod-local cancel registry.
//!   4. On shutdown: cancel every in-flight execution, await their
//!      tokio tasks, mark the row done.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;

use weft_core::cancellation::CancellationFlag;
use weft_core::{Color, NodeCatalog, ProjectDefinition};
use weft_task_store::executor::{run_worker_picker, WorkerTaskKind, WorkerTaskRegistry};
use weft_task_store::tasks::Task;
use weft_task_store::{
    CancelExecutionPayload, ExecutionPayload, TaskKind, WorkerPodClient,
};

use crate::context::EngineClients;
use crate::loop_driver::run_one_execution;

/// How long the worker picker sits idle (no claimable work) before
/// attempting its guarded self-exit. The grace this gives a burst
/// of executions: a new exec arriving within this window reuses the
/// warm pod instead of paying a cold respawn.
const WORKER_IDLE_EXIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Pod-scoped registry: per-color cancellation flag for an in-flight
/// execution. cancel_execution looks up by color and fires the flag.
type CancelRegistry = Arc<Mutex<HashMap<Color, Arc<CancellationFlag>>>>;

/// `IdleExit` impl backed by the broker's guarded CAS. The picker
/// calls `try_idle_exit` after the idle window; the broker flips
/// `alive -> done` only if no pending/claimed work exists for the
/// project, so a concurrent exec keeps the pod alive.
struct WorkerIdleExit {
    worker_pods: Arc<dyn WorkerPodClient>,
    pod_name: String,
}

#[async_trait::async_trait]
impl weft_task_store::executor::IdleExit for WorkerIdleExit {
    async fn try_idle_exit(&self) -> anyhow::Result<bool> {
        self.worker_pods.mark_done_if_idle(&self.pod_name).await
    }
}

/// Per-Pod runtime context for worker task kinds. The registry's
/// `WorkerTaskKind::handle` impls receive `&WorkerCtx`.
#[derive(Clone)]
struct WorkerCtx {
    project: ProjectDefinition,
    catalog: Arc<dyn NodeCatalog>,
    clients: EngineClients,
    pod_name: String,
    tenant_id: String,
    /// The k8s namespace this worker pod runs in (the project
    /// namespace: `wm-project-{tenant}-{project}`). Threaded into
    /// `InfraProvisionContext` so `Node::provision` bodies see the
    /// runtime namespace they're being applied into.
    namespace: String,
    cancel_registry: CancelRegistry,
}

pub async fn run_pod(
    project: ProjectDefinition,
    catalog: Arc<dyn NodeCatalog>,
    clients: EngineClients,
    worker_pods: Arc<dyn WorkerPodClient>,
    pod_name: String,
    project_id: String,
    tenant_id: String,
    namespace: String,
) -> Result<()> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let cancel_registry: CancelRegistry = Arc::new(Mutex::new(HashMap::new()));

    worker_pods
        .register_alive(&pod_name, &project_id)
        .await?;
    spawn_heartbeat(
        worker_pods.clone(),
        pod_name.clone(),
        shutdown.clone(),
    );

    let picker_tasks = clients.tasks.clone();
    let ctx = WorkerCtx {
        project,
        catalog,
        clients,
        pod_name: pod_name.clone(),
        tenant_id,
        namespace,
        cancel_registry: cancel_registry.clone(),
    };

    let registry = WorkerTaskRegistry::builder()
        .register(TaskKind::Execute, Arc::new(ExecuteKind))
        .register(TaskKind::Resume, Arc::new(ExecuteKind))
        .register(TaskKind::CancelExecution, Arc::new(CancelExecutionKind))
        .build();

    // Idle self-exit: after `WORKER_IDLE_EXIT` of no claimable
    // work, the picker attempts the guarded `alive -> done` CAS via
    // the broker. The CAS (not the timer) is the correctness gate.
    let idle_exit: Arc<dyn weft_task_store::executor::IdleExit> = Arc::new(WorkerIdleExit {
        worker_pods: worker_pods.clone(),
        pod_name: pod_name.clone(),
    });
    run_worker_picker(
        picker_tasks,
        ctx,
        registry,
        pod_name.clone(),
        project_id,
        shutdown.clone(),
        idle_exit,
        WORKER_IDLE_EXIT,
    )
    .await;

    // Pod-wide shutdown: cancel every in-flight execution. The
    // loop_driver checks the flag at every iteration top, finishes
    // its in-flight node tokio tasks, and exits. The picker has
    // already returned (run_worker_picker observes shutdown above).
    let flags: Vec<_> = {
        let g = cancel_registry.lock().await;
        g.values().cloned().collect()
    };
    for f in flags {
        f.cancel();
    }
    shutdown.store(true, Ordering::Relaxed);
    let _ = worker_pods.mark_done(&pod_name).await;
    Ok(())
}

/// Background heartbeat. Sets `shutdown` to true if the worker_pod
/// row stops being alive (mark_done / mark_dead, row deleted), or if
/// the broker has been unreachable long enough that the row's lease
/// would have lapsed anyway. Bounding consecutive errors prevents an
/// orphaned pod from running forever after the broker disappears.
fn spawn_heartbeat(
    worker_pods: Arc<dyn WorkerPodClient>,
    pod_name: String,
    shutdown: Arc<AtomicBool>,
) {
    let interval = Duration::from_secs(weft_task_store::HEARTBEAT_INTERVAL_SECS);
    // After this many consecutive errors, the row's stale-recovery
    // window has elapsed and the dispatcher will (or already has)
    // reaped this pod's row. The pod must self-terminate.
    let max_consecutive_errors = (weft_task_store::HEARTBEAT_STALE_SECS as u64
        / weft_task_store::HEARTBEAT_INTERVAL_SECS) as u32
        + 1;
    tokio::spawn(async move {
        let mut consecutive_errors: u32 = 0;
        loop {
            tokio::time::sleep(interval).await;
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            match worker_pods.heartbeat(&pod_name).await {
                Ok(true) => {
                    consecutive_errors = 0;
                }
                Ok(false) => {
                    tracing::warn!(
                        target: "weft_engine::run_pod",
                        %pod_name,
                        "worker_pod row no longer alive; signalling shutdown"
                    );
                    shutdown.store(true, Ordering::Relaxed);
                    break;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors >= max_consecutive_errors {
                        tracing::error!(
                            target: "weft_engine::run_pod",
                            %pod_name, error = %e, consecutive_errors,
                            "heartbeat unreachable past stale-recovery window; signalling shutdown"
                        );
                        shutdown.store(true, Ordering::Relaxed);
                        break;
                    }
                    tracing::warn!(
                        target: "weft_engine::run_pod",
                        error = %e, consecutive_errors,
                        "heartbeat error; will retry"
                    );
                }
            }
        }
    });
}

/// Shared executor for `execute` and `resume`: both fold the
/// journal and run the loop driver. The dispatcher distinguishes
/// the two so SSE can label the event, but the worker treats them
/// identically (the journal carries the lifecycle truth).
struct ExecuteKind;

#[async_trait]
impl WorkerTaskKind<WorkerCtx> for ExecuteKind {
    fn spawn_in_background(&self) -> bool {
        true
    }

    async fn handle(&self, ctx: &WorkerCtx, task: &Task) -> Result<()> {
        let payload: ExecutionPayload = serde_json::from_value(task.payload.clone())?;
        let color: Color = payload
            .color
            .parse()
            .map_err(|e| anyhow::anyhow!("bad color: {e}"))?;

        let flag = CancellationFlag::new_arc();
        ctx.cancel_registry
            .lock()
            .await
            .insert(color, flag.clone());

        let outcome = run_one_execution(
            ctx.project.clone(),
            ctx.catalog.clone(),
            color,
            ctx.clients.clone(),
            ctx.pod_name.clone(),
            ctx.tenant_id.clone(),
            ctx.namespace.clone(),
            flag,
        )
        .await;

        ctx.cancel_registry.lock().await.remove(&color);

        outcome.map(|_| ())
    }
}

/// `cancel_execution` is addressed to one Pod via the task's
/// `target_pod_name` claim filter (set by the dispatcher to the
/// alive Pod for the color's project). A sibling Pod in the same
/// pool can't claim the row, so we just look up the per-color flag
/// and fire it.
struct CancelExecutionKind;

#[async_trait]
impl WorkerTaskKind<WorkerCtx> for CancelExecutionKind {
    async fn handle(&self, ctx: &WorkerCtx, task: &Task) -> Result<()> {
        let payload: CancelExecutionPayload = serde_json::from_value(task.payload.clone())?;
        let color: Color = payload
            .color
            .parse()
            .map_err(|e| anyhow::anyhow!("bad color: {e}"))?;
        let flag = ctx.cancel_registry.lock().await.get(&color).cloned();
        match flag {
            Some(f) => {
                tracing::info!(
                    target: "weft_engine::run_pod",
                    color = %color,
                    "firing per-color cancel flag"
                );
                f.cancel();
            }
            None => {
                // Race: execution finished naturally between the
                // dispatcher reading the worker_pod row and us
                // claiming the cancel. UI sees natural terminal via
                // SSE; cancel is a no-op.
                tracing::debug!(
                    target: "weft_engine::run_pod",
                    color = %color,
                    "cancel for unknown color (already terminal); no-op"
                );
            }
        }
        Ok(())
    }
}
