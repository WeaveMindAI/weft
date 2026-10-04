//! Running claimed tasks under their lease.
//!
//! The dispatcher claims its own tasks in a picker loop
//! ([`dispatcher_picker_loop`]) and runs each through a registry keyed by
//! kind. A worker never picks: it is called for one execution, claims
//! that execution's task, and runs it through [`run_claimed_worker_task`].
//! Both share one lease guard: the claim is renewed while the work runs,
//! and the task ends `complete`, `failed`, or back to `pending` when the
//! lease could not be renewed.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::FutureExt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::tasks::{claim_duration_secs, claim_heartbeat_interval, ClaimFilter, Task};

use crate::traits::TaskStoreClient;

#[async_trait]
pub trait TaskExecutor<Ctx: Send + Sync>: Send + Sync {
    async fn execute(&self, ctx: &Ctx, task: &Task) -> Result<Value>;
}

pub struct TaskRegistry<Ctx: Send + Sync> {
    inner: HashMap<String, Arc<dyn TaskExecutor<Ctx>>>,
    _phantom: PhantomData<Ctx>,
}

impl<Ctx: Send + Sync> Clone for TaskRegistry<Ctx> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<Ctx: Send + Sync> Default for TaskRegistry<Ctx> {
    fn default() -> Self {
        Self {
            inner: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

impl<Ctx: Send + Sync> TaskRegistry<Ctx> {
    pub fn builder() -> TaskRegistryBuilder<Ctx> {
        TaskRegistryBuilder { map: HashMap::new() }
    }

    pub fn get(&self, kind: &str) -> Option<Arc<dyn TaskExecutor<Ctx>>> {
        self.inner.get(kind).cloned()
    }
}

pub struct TaskRegistryBuilder<Ctx: Send + Sync> {
    map: HashMap<String, Arc<dyn TaskExecutor<Ctx>>>,
}

impl<Ctx: Send + Sync> TaskRegistryBuilder<Ctx> {
    pub fn register(
        mut self,
        kind: crate::kinds::TaskKind,
        exec: Arc<dyn TaskExecutor<Ctx>>,
    ) -> Self {
        self.map.insert(kind.as_str().to_string(), exec);
        self
    }

    /// Register an executor by its raw kind STRING. For task kinds that live
    /// OUTSIDE the built-in `TaskKind` enum, dispatch is string-keyed on
    /// `Task.kind`, so an added executor slots in
    /// without widening the built-in enum.
    pub fn register_str(mut self, kind: impl Into<String>, exec: Arc<dyn TaskExecutor<Ctx>>) -> Self {
        self.map.insert(kind.into(), exec);
        self
    }

    pub fn build(self) -> TaskRegistry<Ctx> {
        TaskRegistry {
            inner: self.map,
            _phantom: PhantomData,
        }
    }
}

/// Most dispatcher tasks one process runs at once. Tasks like
/// `register_signal` (HTTP to the listener) or a build can take a while;
/// running them one after another would head-of-line-block the claims. 8
/// is enough that one slow op doesn't park everything else, low enough
/// that we don't open arbitrarily many DB connections at once.
pub const DISPATCHER_PICKER_CONCURRENCY: usize = 8;

/// A dispatcher task that became claimable.
pub static DISPATCHER_READY: &[crate::drain::WakeOn] = &[crate::drain::WakeOn {
    channel: crate::tasks::TASK_READY_CHANNEL,
    concerns: |payload| payload == "dispatcher",
}];

/// The dispatcher's picker as a drain loop: each pass claims one task and
/// runs it on a task of its own (under the concurrency cap), until none
/// is claimable. Woken by a task becoming claimable; the safety look also
/// rescues a task whose claim lapsed (its claimant died), which nothing
/// announces.
pub fn dispatcher_picker_loop<Ctx>(
    store: Arc<dyn TaskStoreClient>,
    ctx: Ctx,
    registry: TaskRegistry<Ctx>,
    replica: String,
) -> crate::drain::DrainLoop
where
    Ctx: Send + Sync + Clone + 'static,
{
    let slots = Arc::new(tokio::sync::Semaphore::new(DISPATCHER_PICKER_CONCURRENCY));
    crate::drain::DrainLoop::new("dispatcher_picker", DISPATCHER_READY, crate::drain::SAFETY_POLL_INTERVAL, move || {
        let (store, ctx, registry, replica, slots) = (store.clone(), ctx.clone(), registry.clone(), replica.clone(), slots.clone());
        async move {
            // At capacity: wait for a running task to finish before claiming
            // another.
            let slot = slots.acquire_owned().await.expect("the picker's semaphore is never closed");
            match store.claim_one(&replica, ClaimFilter::Dispatcher, Duration::ZERO).await? {
                Some(task) => {
                    tokio::spawn(async move {
                        let _slot = slot;
                        run_dispatcher_task(store, ctx, registry, replica, task).await;
                    });
                    Ok(crate::drain::DrainStep::More)
                }
                None => Ok(crate::drain::DrainStep::Done),
            }
        }
    })
}

/// Run one claimed dispatcher task through its executor, under its lease.
async fn run_dispatcher_task<Ctx>(
    store: Arc<dyn TaskStoreClient>,
    ctx: Ctx,
    registry: TaskRegistry<Ctx>,
    replica: String,
    task: Task,
) where
    Ctx: Send + Sync + Clone + 'static,
{
    let Some(executor) = registry.get(&task.kind) else {
        let err = format!("no executor for task kind '{}'", task.kind);
        tracing::error!(
            target: "weft_task_store::executor",
            id = %task.id, kind = %task.kind, error = %err,
            "rejecting unknown task kind"
        );
        if let Err(e) = store.fail(task.id, &replica, err).await {
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task.id, error = %e,
                "fail write failed for unknown-kind reject; row sits claimed until lease expiry"
            );
        }
        return;
    };
    let task_id = task.id;
    let kind = task.kind.clone();
    let lease = LeaseSignal::new();
    let heartbeat = spawn_claim_heartbeat(
        store.clone(),
        task_id,
        replica.clone(),
        lease.clone(),
    );
    let outcome = run_with_lease_guard(
        executor.execute(&ctx, &task),
        lease,
        task_id,
        &kind,
    )
    .await;
    drop(heartbeat);
    finalize_task(store.as_ref(), task_id, &replica, &kind, outcome).await;
}

/// Why the heartbeat task told the executor to stop. Typed so the
/// finalizer acts on the CAUSE, not on a synthesized error string:
/// the two cases need opposite reactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseLoss {
    /// The heartbeat came back "row no longer claimed by us": a
    /// sibling process already re-claimed it (our lease lapsed and was
    /// taken). The thief owns the task now; we touch nothing.
    Stolen,
    /// The heartbeat could not REACH the store past the lease window.
    /// The work did not fail, WE lost the ability to prove liveness,
    /// so the task is surrendered: requeued to `pending` for any process
    /// (including us) to claim again.
    Unrenewable,
}

/// Cancellation token + the reason it fired. The heartbeat task sets
/// the reason before cancelling, so the guard always reads a cause.
#[derive(Clone)]
struct LeaseSignal {
    reason: Arc<std::sync::OnceLock<LeaseLoss>>,
    token: CancellationToken,
}

impl LeaseSignal {
    fn new() -> Self {
        Self {
            reason: Arc::new(std::sync::OnceLock::new()),
            token: CancellationToken::new(),
        }
    }

    fn lost(&self, why: LeaseLoss) {
        let _ = self.reason.set(why);
        self.token.cancel();
    }

    async fn cancelled(&self) {
        self.token.cancelled().await
    }

    fn reason(&self) -> LeaseLoss {
        *self
            .reason
            .get()
            .expect("lease token cancelled without a recorded reason")
    }
}

/// The guard's verdict on one executor run.
enum ExecOutcome {
    /// The executor future ran to completion: a value, an error, or a
    /// panic caught by `catch_unwind`.
    Finished(std::thread::Result<Result<Value>>),
    /// The heartbeat signalled lease loss first; the executor future
    /// was dropped mid-flight.
    LeaseLost(LeaseLoss),
}

/// Run an executor future under the lease guard. If the heartbeat
/// task signals lease loss before the executor finishes, the future
/// is dropped and the typed cause is handed to `finalize_task`, which
/// requeues (surrender) or stands down (stolen). A later claim of the
/// row redoes the work; every task kind is re-runnable by contract
/// (see the idempotency note in `tasks`).
async fn run_with_lease_guard<F>(
    fut: F,
    lease: LeaseSignal,
    task_id: uuid::Uuid,
    kind: &str,
) -> ExecOutcome
where
    F: std::future::Future<Output = Result<Value>>,
{
    tokio::select! {
        out = AssertUnwindSafe(fut).catch_unwind() => ExecOutcome::Finished(out),
        _ = lease.cancelled() => {
            let why = lease.reason();
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind, cause = ?why,
                "lease lost mid-execution; abandoning the executor future"
            );
            ExecOutcome::LeaseLost(why)
        }
    }
}

/// Persist the verdict for one task. Used by both pickers.
///
///   - `Finished(Ok(Ok(value)))`: → `tasks::complete`.
///   - `Finished(Ok(Err(e)))`: → `tasks::fail` with the error message.
///   - `Finished(Err(panic))`: → `tasks::fail` with a "panic: ..."
///     prefix so clients can flag it visibly. Without this
///     layering, a panicking executor would ride the spawned task's
///     JoinError up and get discarded by `try_join_next`, and the row
///     would sit `claimed` until the lease expired.
///   - `LeaseLost(Unrenewable)`: → `tasks::requeue` (guarded on our
///     claim), putting the row back to `pending` for the next claim.
///     A transient store outage must never terminalize work that did
///     not fail.
///   - `LeaseLost(Stolen)`: → nothing. The re-claimer owns the row;
///     any write from us would race its run.
async fn finalize_task(
    store: &dyn TaskStoreClient,
    task_id: uuid::Uuid,
    replica: &str,
    kind: &str,
    outcome: ExecOutcome,
) -> TaskEnd {
    let outcome = match outcome {
        ExecOutcome::Finished(finished) => finished,
        ExecOutcome::LeaseLost(LeaseLoss::Stolen) => {
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind,
                "lease stolen by another claimant; it owns the task, standing down"
            );
            return TaskEnd::LeaseLost;
        }
        ExecOutcome::LeaseLost(LeaseLoss::Unrenewable) => {
            match store.requeue(task_id, replica).await {
                Ok(true) => tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind,
                    "surrendered task requeued; the next claim re-runs it"
                ),
                Ok(false) => tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind,
                    "surrender found the row no longer ours (already re-claimed); \
                     the claimer owns it"
                ),
                Err(e) => tracing::error!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e,
                    "surrender requeue failed; the row sits claimed until its lease \
                     expires, then claim_one rescues it"
                ),
            }
            return TaskEnd::LeaseLost;
        }
    };
    match outcome {
        Ok(Ok(result)) => {
            if let Err(e) = store.complete(task_id, replica, result).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e,
                    "complete write failed; row may have been re-claimed"
                );
            }
            TaskEnd::Completed
        }
        Ok(Err(e)) => {
            let msg = format!("{e:#}");
            if let Err(e2) = store.fail(task_id, replica, msg.clone()).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e2,
                    "fail write failed"
                );
            }
            TaskEnd::Failed(msg)
        }
        Err(panic) => {
            let panic_msg = panic_message(&panic);
            tracing::error!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind, panic = %panic_msg,
                "task panicked; writing tasks::fail"
            );
            let msg = format!("panic: {panic_msg}");
            if let Err(e) = store.fail(task_id, replica, msg.clone()).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e,
                    "fail write after panic also failed"
                );
            }
            TaskEnd::Failed(msg)
        }
    }
}

/// How a claimed task ended, as its claimant saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskEnd {
    Completed,
    /// The work failed (or panicked); the task was failed with this.
    Failed(String),
    /// The claim was lost mid-work: taken by another claimant, or given
    /// back because it could not be renewed. Whoever claims it next runs
    /// it again.
    LeaseLost,
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Renew the claim every [`claim_heartbeat_interval`] until the
/// owning future finishes (which aborts this handle).
///
/// Two exit conditions fire the lease signal, each with its cause:
///   - heartbeat returns `Ok(false)`: the row is no longer claimed by
///     us (sibling process took the lease): `LeaseLoss::Stolen`, the
///     finalizer stands down.
///   - heartbeat errors past `claim_duration_secs() / interval` ticks:
///     the lease has lapsed at this point regardless of what the DB
///     says; a sibling process can re-claim, so we must stop or risk
///     parallel execution of the same row. `LeaseLoss::Unrenewable`,
///     the finalizer requeues the row.
/// A stalled executor is NOT an exit: the heartbeat keeps renewing,
/// the executor keeps running, no leak.
fn spawn_claim_heartbeat(
    store: Arc<dyn TaskStoreClient>,
    task_id: uuid::Uuid,
    replica: String,
    lease: LeaseSignal,
) -> Heartbeat {
    let interval = claim_heartbeat_interval();
    let max_consecutive_errors =
        (claim_duration_secs() as f64 / interval.as_secs_f64()) as u32 + 1;
    Heartbeat(tokio::spawn(async move {
        let mut consecutive_errors: u32 = 0;
        loop {
            tokio::time::sleep(interval).await;
            match store.heartbeat(task_id, &replica).await {
                Ok(true) => {
                    consecutive_errors = 0;
                }
                Ok(false) => {
                    tracing::warn!(
                        target: "weft_task_store::executor",
                        id = %task_id,
                        "heartbeat: lease no longer ours; signalling executor"
                    );
                    lease.lost(LeaseLoss::Stolen);
                    break;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors >= max_consecutive_errors {
                        tracing::error!(
                            target: "weft_task_store::executor",
                            id = %task_id, error = %e, consecutive_errors,
                            "heartbeat unreachable past lease window; signalling executor"
                        );
                        lease.lost(LeaseLoss::Unrenewable);
                        break;
                    }
                    tracing::warn!(
                        target: "weft_task_store::executor",
                        id = %task_id, error = %e, consecutive_errors,
                        "heartbeat error; will retry"
                    );
                }
            }
        }
    }))
}

/// A claim's heartbeat, stopped when it is dropped: the work it keeps
/// claimed may be dropped mid-way (its caller went away, its task was
/// aborted), and a heartbeat left running would hold the claim forever
/// with nothing doing the work.
struct Heartbeat(tokio::task::JoinHandle<()>);

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Run one task a worker claimed for the execution it was called for,
/// under the same lease guard the dispatcher's tasks run under: the claim
/// is renewed while `work` runs, and the task ends `complete`, `failed`,
/// or back to `pending` when the claim could not be renewed. Answers how
/// it ended, for the worker to tell whoever called it.
pub async fn run_claimed_worker_task<F>(
    store: Arc<dyn TaskStoreClient>,
    replica: &str,
    task: &Task,
    work: F,
) -> TaskEnd
where
    F: std::future::Future<Output = Result<()>>,
{
    let lease = LeaseSignal::new();
    let heartbeat = spawn_claim_heartbeat(store.clone(), task.id, replica.to_string(), lease.clone());
    let kind = task.kind.clone();
    let result = serde_json::json!({ "kind": kind });
    let outcome = run_with_lease_guard(async { work.await.map(|()| result) }, lease, task.id, &kind).await;
    drop(heartbeat);
    finalize_task(store.as_ref(), task.id, replica, &kind, outcome).await
}
