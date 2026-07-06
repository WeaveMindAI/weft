//! Trait + registry shape for both pickers (dispatcher-side and
//! worker-side). Each side's main.rs builds a registry, then spawns
//! the matching picker loop.
//!
//! The registry is generic over the context type the executor takes
//! (`DispatcherState` for dispatcher-side, `WorkerContext` for
//! worker-side) so both ends share one piece of plumbing.

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

use crate::tasks::{ClaimFilter, Task, CLAIM_DURATION_SECS, CLAIM_HEARTBEAT_INTERVAL_SECS};
use crate::traits::TaskStoreClient;

#[async_trait]
pub trait TaskExecutor<Ctx: Send + Sync>: Send + Sync {
    async fn execute(&self, ctx: &Ctx, task: &Task) -> Result<Value>;
}

/// Worker idle self-exit. After the picker has been idle (no task
/// claimed) for the idle window, it calls `try_idle_exit`. The impl
/// attempts the guarded `alive -> done` CAS (no pending/claimed work
/// for the project); returning `true` means the pod won the flip and
/// the picker should stop. The CAS, not the picker's idle timer, is
/// the correctness boundary: a task in flight (claimed) or queued
/// (pending) fails the CAS, so the picker keeps running.
#[async_trait]
pub trait IdleExit: Send + Sync {
    async fn try_idle_exit(&self) -> Result<bool>;
}

#[async_trait]
pub trait WorkerTaskKind<Ctx: Send + Sync>: Send + Sync {
    /// Synchronous handler: runs to completion before the picker
    /// claims the next task. Return `Ok(())` for `complete`,
    /// `Err(_)` for `fail`. Use this for fire-and-forget kinds
    /// like `cancel_execution` that take milliseconds.
    async fn handle(&self, ctx: &Ctx, task: &Task) -> Result<()>;

    /// If true, the picker spawns this kind on a tokio task instead
    /// of awaiting it inline. Used for `execute` / `resume` kinds
    /// whose body runs for the whole execution lifetime; the picker
    /// must keep claiming to multiplex many concurrent executions.
    fn spawn_in_background(&self) -> bool {
        false
    }
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
    /// OUTSIDE the built-in `TaskKind` enum (e.g. an in-cluster image build):
    /// dispatch is string-keyed on `Task.kind`, so an added executor slots in
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

pub struct WorkerTaskRegistry<Ctx: Send + Sync> {
    inner: HashMap<String, Arc<dyn WorkerTaskKind<Ctx>>>,
}

impl<Ctx: Send + Sync> Clone for WorkerTaskRegistry<Ctx> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Ctx: Send + Sync> Default for WorkerTaskRegistry<Ctx> {
    fn default() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

impl<Ctx: Send + Sync> WorkerTaskRegistry<Ctx> {
    pub fn builder() -> WorkerTaskRegistryBuilder<Ctx> {
        WorkerTaskRegistryBuilder { map: HashMap::new() }
    }

    pub fn get(&self, kind: &str) -> Option<Arc<dyn WorkerTaskKind<Ctx>>> {
        self.inner.get(kind).cloned()
    }
}

pub struct WorkerTaskRegistryBuilder<Ctx: Send + Sync> {
    map: HashMap<String, Arc<dyn WorkerTaskKind<Ctx>>>,
}

impl<Ctx: Send + Sync> WorkerTaskRegistryBuilder<Ctx> {
    pub fn register(
        mut self,
        kind: crate::kinds::TaskKind,
        kind_impl: Arc<dyn WorkerTaskKind<Ctx>>,
    ) -> Self {
        self.map.insert(kind.as_str().to_string(), kind_impl);
        self
    }

    /// Register a worker kind by its raw kind STRING, the same add-a-string-kind
    /// seam the dispatcher-side builder exposes: dispatch on both sides is
    /// string-keyed on `Task.kind`, so an added worker-target kind slots in
    /// without widening the built-in enum.
    pub fn register_str(
        mut self,
        kind: impl Into<String>,
        kind_impl: Arc<dyn WorkerTaskKind<Ctx>>,
    ) -> Self {
        self.map.insert(kind.into(), kind_impl);
        self
    }

    pub fn build(self) -> WorkerTaskRegistry<Ctx> {
        WorkerTaskRegistry { inner: self.map }
    }
}

/// Maximum concurrent dispatcher tasks per Pod. Tasks like
/// `register_signal` (HTTP to listener) and `spawn_pod`
/// (image pull + kubectl apply + boot wait) can take seconds;
/// running them sequentially would head-of-line-block the picker. 8
/// is enough that one slow op doesn't park everything else, low
/// enough that we don't open arbitrarily many DB connections at once.
pub const DISPATCHER_PICKER_CONCURRENCY: usize = 8;

/// Dispatcher picker: claims `target=dispatcher` tasks and runs each
/// on a tokio task, capped at `DISPATCHER_PICKER_CONCURRENCY`.
/// Per-claim heartbeat renews the lease while the executor runs.
pub async fn run_dispatcher_picker<Ctx>(
    store: Arc<dyn TaskStoreClient>,
    ctx: Ctx,
    registry: TaskRegistry<Ctx>,
    pod_id: String,
) where
    Ctx: Send + Sync + Clone + 'static,
{
    let poll_interval = Duration::from_millis(50);
    let mut in_flight: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        // Reap finished tasks first so the in_flight count reflects
        // reality before we decide whether to claim.
        while in_flight.try_join_next().is_some() {}

        if in_flight.len() >= DISPATCHER_PICKER_CONCURRENCY {
            // At capacity: wait for one to finish before polling.
            let _ = in_flight.join_next().await;
            continue;
        }

        match store.claim_one(&pod_id, ClaimFilter::Dispatcher).await {
            Ok(Some(task)) => {
                spawn_dispatcher_task(
                    &mut in_flight,
                    store.clone(),
                    ctx.clone(),
                    registry.clone(),
                    pod_id.clone(),
                    task,
                );
            }
            Ok(None) => tokio::time::sleep(poll_interval).await,
            Err(e) => {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    error = %e,
                    "dispatcher picker error; backing off"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn spawn_dispatcher_task<Ctx>(
    in_flight: &mut tokio::task::JoinSet<()>,
    store: Arc<dyn TaskStoreClient>,
    ctx: Ctx,
    registry: TaskRegistry<Ctx>,
    pod_id: String,
    task: Task,
) where
    Ctx: Send + Sync + Clone + 'static,
{
    in_flight.spawn(async move {
        let Some(executor) = registry.get(&task.kind) else {
            let err = format!("no executor for task kind '{}'", task.kind);
            tracing::error!(
                target: "weft_task_store::executor",
                id = %task.id, kind = %task.kind, error = %err,
                "rejecting unknown task kind"
            );
            if let Err(e) = store.fail(task.id, &pod_id, err).await {
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
        let lease_lost = CancellationToken::new();
        let heartbeat = spawn_claim_heartbeat(
            store.clone(),
            task_id,
            pod_id.clone(),
            lease_lost.clone(),
        );
        let outcome = run_with_lease_guard(
            executor.execute(&ctx, &task),
            lease_lost.clone(),
            task_id,
            &kind,
        )
        .await;
        heartbeat.abort();
        finalize_task(store.as_ref(), task_id, &pod_id, &kind, outcome).await;
    });
}

/// Run an executor future. If the heartbeat task signals lease loss
/// before the executor finishes, abandon the executor and synthesize
/// a fail outcome. The sibling pod that re-claims will redo the work
/// idempotently (every dispatcher task kind is idempotent on retry,
/// e.g. RegisterSignal / RouteEntry / FireSignal).
async fn run_with_lease_guard<F>(
    fut: F,
    lease_lost: CancellationToken,
    task_id: uuid::Uuid,
    kind: &str,
) -> std::thread::Result<Result<Value>>
where
    F: std::future::Future<Output = Result<Value>>,
{
    tokio::select! {
        out = AssertUnwindSafe(fut).catch_unwind() => out,
        _ = lease_lost.cancelled() => {
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind,
                "lease lost mid-execution; surrendering task"
            );
            Ok(Err(anyhow::anyhow!("lease lost; sibling pod will retry")))
        }
    }
}

/// Persist a `(complete | fail)` decision for one task. Used by both
/// pickers. The `outcome` packs three layers:
///
///   - `Ok(Ok(value))`: executor returned a value. → `tasks::complete`.
///   - `Ok(Err(e))`: executor returned an error. → `tasks::fail` with
///     the error message.
///   - `Err(panic)`: catch_unwind tripped. → `tasks::fail` with a
///     "panic: ..." prefix so the dashboard can flag it visibly.
///
/// Without this layering, a panicking executor would ride the
/// JoinSet's JoinError up and get discarded by `try_join_next`, and
/// the row would sit `claimed` until the lease expired.
async fn finalize_task(
    store: &dyn TaskStoreClient,
    task_id: uuid::Uuid,
    pod_id: &str,
    kind: &str,
    outcome: std::thread::Result<Result<Value>>,
) {
    match outcome {
        Ok(Ok(result)) => {
            if let Err(e) = store.complete(task_id, pod_id, result).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e,
                    "complete write failed; row may have been re-claimed"
                );
            }
        }
        Ok(Err(e)) => {
            let msg = format!("{e:#}");
            if let Err(e2) = store.fail(task_id, pod_id, msg).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e2,
                    "fail write failed"
                );
            }
        }
        Err(panic) => {
            let panic_msg = panic_message(&panic);
            tracing::error!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind, panic = %panic_msg,
                "task panicked; writing tasks::fail"
            );
            let msg = format!("panic: {panic_msg}");
            if let Err(e) = store.fail(task_id, pod_id, msg).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e,
                    "fail write after panic also failed"
                );
            }
        }
    }
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

/// Worker picker: claims `target=worker` tasks scoped to one
/// `project_id` and dispatches each through the registry. Kinds
/// that opt in to `spawn_in_background` get tokio-spawned (with their
/// own heartbeat); the picker keeps claiming. Synchronous kinds
/// (cancel) run inline.
pub async fn run_worker_picker<Ctx>(
    store: Arc<dyn TaskStoreClient>,
    ctx: Ctx,
    registry: WorkerTaskRegistry<Ctx>,
    pod_name: String,
    project_id: String,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    idle_exit: Arc<dyn IdleExit>,
    idle_window: Duration,
) where
    Ctx: Send + Sync + Clone + 'static,
{
    use std::sync::atomic::Ordering;
    let poll_interval = Duration::from_millis(50);
    // When the picker first went idle (no task claimed). Reset to
    // None on every successful claim. When idle longer than
    // `idle_window`, attempt the guarded idle-exit CAS. Uses
    // `tokio::time::Instant` (not `std`) so the idle window is
    // virtualized under `tokio::time::pause()` in tests; in prod
    // it's the same monotonic clock.
    let mut idle_since: Option<tokio::time::Instant> = None;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        match try_worker_one(&store, &ctx, &registry, &pod_name, &project_id).await {
            Ok(true) => {
                idle_since = None;
                continue;
            }
            Ok(false) => {
                let idle_for = match idle_since {
                    Some(t) => t.elapsed(),
                    None => {
                        idle_since = Some(tokio::time::Instant::now());
                        Duration::ZERO
                    }
                };
                if idle_for >= idle_window {
                    // Attempt the guarded exit. The CAS fails if any
                    // pending/claimed work exists (incl. a background
                    // exec holding a claimed task), so this is safe
                    // even though the picker only sees the claim queue.
                    match idle_exit.try_idle_exit().await {
                        Ok(true) => {
                            shutdown.store(true, Ordering::Relaxed);
                            break;
                        }
                        Ok(false) => {
                            // Lost the race (work arrived / in flight).
                            // Reset the timer and keep claiming.
                            idle_since = None;
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "weft_task_store::executor",
                                error = %e,
                                "idle-exit CAS failed; will retry"
                            );
                            idle_since = None;
                        }
                    }
                } else {
                    tokio::time::sleep(poll_interval).await;
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    error = %e,
                    "worker picker error; backing off"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn try_worker_one<Ctx>(
    store: &Arc<dyn TaskStoreClient>,
    ctx: &Ctx,
    registry: &WorkerTaskRegistry<Ctx>,
    pod_name: &str,
    project_id: &str,
) -> Result<bool>
where
    Ctx: Send + Sync + Clone + 'static,
{
    let Some(task) = store
        .claim_one(
            pod_name,
            ClaimFilter::Worker {
                project_id: project_id.to_string(),
            },
        )
        .await?
    else {
        return Ok(false);
    };

    let Some(handler) = registry.get(&task.kind) else {
        let err = format!("no worker handler for task kind '{}'", task.kind);
        tracing::error!(
            target: "weft_task_store::executor",
            id = %task.id, kind = %task.kind, error = %err,
            "rejecting unknown worker task kind"
        );
        if let Err(e) = store.fail(task.id, pod_name, err).await {
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task.id, error = %e,
                "fail write failed for unknown-kind reject; row sits claimed until lease expiry"
            );
        }
        return Ok(true);
    };

    let task_id = task.id;
    let kind = task.kind.clone();

    // The worker's WorkerTaskKind returns Result<()>; adapt to the
    // Result<Value> shape that finalize_task expects so both pickers
    // share the same complete/fail/panic logic.
    let kind_for_value = kind.clone();
    let to_value = move |r: Result<()>| r.map(|()| serde_json::json!({"kind": kind_for_value}));

    if handler.spawn_in_background() {
        let store_inner = store.clone();
        let pod_inner = pod_name.to_string();
        let ctx_inner = ctx.clone();
        let handler_inner = handler.clone();
        let kind_inner = kind.clone();
        tokio::spawn(async move {
            let lease_lost = CancellationToken::new();
            let heartbeat = spawn_claim_heartbeat(
                store_inner.clone(),
                task_id,
                pod_inner.clone(),
                lease_lost.clone(),
            );
            let outcome = run_worker_with_lease_guard(
                handler_inner.handle(&ctx_inner, &task),
                lease_lost.clone(),
                task_id,
                &kind_inner,
                to_value.clone(),
            )
            .await;
            heartbeat.abort();
            finalize_task(store_inner.as_ref(), task_id, &pod_inner, &kind_inner, outcome).await;
        });
        return Ok(true);
    }

    let lease_lost = CancellationToken::new();
    let heartbeat = spawn_claim_heartbeat(
        store.clone(),
        task_id,
        pod_name.to_string(),
        lease_lost.clone(),
    );
    let outcome = run_worker_with_lease_guard(
        handler.handle(ctx, &task),
        lease_lost.clone(),
        task_id,
        &kind,
        to_value.clone(),
    )
    .await;
    heartbeat.abort();
    finalize_task(store.as_ref(), task_id, pod_name, &kind, outcome).await;
    Ok(true)
}

async fn run_worker_with_lease_guard<F, M>(
    fut: F,
    lease_lost: CancellationToken,
    task_id: uuid::Uuid,
    kind: &str,
    to_value: M,
) -> std::thread::Result<Result<Value>>
where
    F: std::future::Future<Output = Result<()>>,
    M: Fn(Result<()>) -> Result<Value>,
{
    tokio::select! {
        out = AssertUnwindSafe(fut).catch_unwind() => out.map(&to_value),
        _ = lease_lost.cancelled() => {
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind,
                "lease lost mid-execution; surrendering task"
            );
            Ok(to_value(Err(anyhow::anyhow!("lease lost; sibling pod will retry"))))
        }
    }
}

/// Renew the claim every `CLAIM_HEARTBEAT_INTERVAL_SECS` until the
/// owning future finishes (which aborts this handle).
///
/// Three exit conditions trip `lease_lost`:
///   - heartbeat returns `Ok(false)`: the row is no longer claimed by
///     us (sibling pod beat the lease): surrender now.
///   - heartbeat errors past `CLAIM_DURATION_SECS / interval` ticks:
///     the lease has lapsed at this point regardless of what the DB
///     says; a sibling pod can re-claim, so we must stop or risk
///     parallel execution of the same row.
///   - the future never trips its own end (executor stalled): the
///     heartbeat keeps renewing, the executor keeps running, no
///     leak.
fn spawn_claim_heartbeat(
    store: Arc<dyn TaskStoreClient>,
    task_id: uuid::Uuid,
    pod_id: String,
    lease_lost: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let max_consecutive_errors =
        (CLAIM_DURATION_SECS as u64 / CLAIM_HEARTBEAT_INTERVAL_SECS) as u32 + 1;
    tokio::spawn(async move {
        let mut consecutive_errors: u32 = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(CLAIM_HEARTBEAT_INTERVAL_SECS)).await;
            match store.heartbeat(task_id, &pod_id).await {
                Ok(true) => {
                    consecutive_errors = 0;
                }
                Ok(false) => {
                    tracing::warn!(
                        target: "weft_task_store::executor",
                        id = %task_id,
                        "heartbeat: lease no longer ours; signalling executor"
                    );
                    lease_lost.cancel();
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
                        lease_lost.cancel();
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
    })
}

#[cfg(test)]
mod idle_exit_tests {
    use super::*;
    use crate::tasks::ClaimFilter;
    use std::sync::atomic::{AtomicU32, Ordering as AtOrd};
    use uuid::Uuid;

    /// TaskStoreClient that never yields a task (the worker is idle).
    /// Only the picker's claim path is exercised; the enqueue/wait
    /// methods are never reached by `run_worker_picker`.
    struct IdleStore;
    #[async_trait]
    impl TaskStoreClient for IdleStore {
        async fn enqueue_dedup(&self, _spec: crate::tasks::NewTask) -> Result<crate::tasks::DedupOutcome> {
            unreachable!("picker never enqueues")
        }
        async fn wait_for_terminal(
            &self,
            _id: Uuid,
            _timeout: Duration,
            _poll: Duration,
        ) -> Result<crate::tasks::TaskOutcome> {
            unreachable!("picker never waits for terminal")
        }
        async fn claim_one(&self, _pod: &str, _f: ClaimFilter) -> Result<Option<Task>> {
            Ok(None)
        }
        async fn heartbeat(&self, _id: Uuid, _pod: &str) -> Result<bool> {
            Ok(true)
        }
        async fn complete(&self, _id: Uuid, _pod: &str, _r: Value) -> Result<()> {
            Ok(())
        }
        async fn fail(&self, _id: Uuid, _pod: &str, _e: String) -> Result<()> {
            Ok(())
        }
    }

    /// Records how many times the picker attempted the idle-exit
    /// CAS, and returns a configurable result.
    struct CountingIdleExit {
        attempts: Arc<AtomicU32>,
        win: bool,
    }
    #[async_trait]
    impl IdleExit for CountingIdleExit {
        async fn try_idle_exit(&self) -> Result<bool> {
            self.attempts.fetch_add(1, AtOrd::Relaxed);
            Ok(self.win)
        }
    }

    fn idle_picker(
        idle_exit: Arc<dyn IdleExit>,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
        window: Duration,
    ) -> impl std::future::Future<Output = ()> {
        run_worker_picker(
            Arc::new(IdleStore),
            (),
            WorkerTaskRegistry::<()>::builder().build(),
            "wp-1".into(),
            "p1".into(),
            shutdown,
            idle_exit,
            window,
        )
    }

    /// The picker must NOT attempt the idle-exit CAS before the idle
    /// window elapses, and MUST attempt + stop once it does (when the
    /// CAS wins).
    #[tokio::test(start_paused = true)]
    async fn attempts_idle_exit_after_window_and_stops_on_win() {
        let attempts = Arc::new(AtomicU32::new(0));
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let idle_exit = Arc::new(CountingIdleExit {
            attempts: attempts.clone(),
            win: true,
        });
        // Picker runs to completion: it idles, crosses the 30s
        // window, the CAS wins, it sets shutdown and returns.
        idle_picker(idle_exit, shutdown.clone(), Duration::from_secs(30)).await;
        assert_eq!(attempts.load(AtOrd::Relaxed), 1, "exactly one winning CAS");
        assert!(shutdown.load(AtOrd::Relaxed), "picker set shutdown on win");
    }

    /// When the CAS loses (work arrived/in-flight), the picker keeps
    /// running: it does NOT stop, and retries on the next window.
    #[tokio::test(start_paused = true)]
    async fn keeps_running_when_cas_loses() {
        let attempts = Arc::new(AtomicU32::new(0));
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let idle_exit = Arc::new(CountingIdleExit {
            attempts: attempts.clone(),
            win: false,
        });
        let sd = shutdown.clone();
        let handle = tokio::spawn(idle_picker(idle_exit, sd, Duration::from_secs(30)));
        // Virtual sleep past two idle windows; auto-advance drives
        // the picker's poll loop. The picker keeps attempting the
        // CAS (losing each time) and never stops.
        tokio::time::sleep(Duration::from_secs(90)).await;
        assert!(!shutdown.load(AtOrd::Relaxed), "lost CAS must not stop the picker");
        assert!(attempts.load(AtOrd::Relaxed) >= 1, "attempted at least once");
        // Externally stop so the spawned picker task ends.
        shutdown.store(true, AtOrd::Relaxed);
        let _ = handle.await;
    }
}
