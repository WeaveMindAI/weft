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
//!   4. On shutdown: cancel every in-flight execution through the same
//!      call a user's cancel makes, wait for those executions to write
//!      their endings, settle the money, mark the row done.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use tokio::sync::Mutex;

use weft_core::cancellation::CancellationFlag;
use weft_core::caller::{InboundMessage, OutboundChunk};
use weft_core::{Color, NodeCatalog, ProjectDefinition};
use weft_task_store::executor::{run_worker_picker, WorkerTaskKind, WorkerTaskRegistry};
use weft_task_store::tasks::Task;
use weft_task_store::{
    CancelExecutionPayload, ExecutionPayload, TaskKind, WorkerPodClient,
};

use crate::context::EngineClients;
use crate::execution_driver::{run_one_execution, ExecutionOutcome};

/// How long the worker picker sits idle (no claimable work) before
/// attempting its guarded self-exit. The grace this gives a burst
/// of executions: a new exec arriving within this window reuses the
/// warm pod instead of paying a cold respawn.
const WORKER_IDLE_EXIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Pod-scoped registry: per-color cancellation flag for an in-flight
/// execution. cancel_execution looks up by color and fires the flag.
type CancelRegistry = Arc<Mutex<HashMap<Color, Arc<CancellationFlag>>>>;

/// Everything one execution registered on the pod, released when the
/// execution ends however it ends.
///
/// These used to be plain statements after the run returned, which an
/// unwind skips, and an unwind is a DESIGNED path here: the bus
/// shutdown panics when the pump has not drained inside its deadline,
/// after the `catch_unwind` around the drive. The pod then kept a
/// cancel flag and a live config for a dead color for the rest of its
/// life, so a later cancel of that execution reported success while
/// firing a flag nobody reads, and the connection server still handed
/// out a config and accepted a socket for a run that no longer exists.
/// A guard cannot be skipped.
struct ExecutionResidue {
    color: Color,
    cancel_registry: CancelRegistry,
    /// THIS execution's flag, so the deferred removal can tell it from
    /// a later claim's. The registry is keyed by color, and a resume of
    /// the same color can be waiting on the color gate and register its
    /// own flag the instant this one's gate is released. Removing by
    /// key alone deleted the NEW execution's flag, so its cancel found
    /// nothing and reported a no-op, and pod shutdown never cancelled
    /// it: the exact failure this guard exists to prevent, moved into
    /// the gap between the guard and the task.
    flag: Arc<CancellationFlag>,
    caller_registry: crate::caller_conn::CallerRegistry,
    live_configs: LiveConfigMap,
    open_charges: Arc<crate::metering::OpenCharges>,
}

impl Drop for ExecutionResidue {
    fn drop(&mut self) {
        let color = self.color;
        // Money first: a charge belongs to this execution, so a job it
        // submitted and never read back is written down as spend with
        // no figure, here, rather than waiting for the pod to die.
        //
        // Nothing in this destructor may panic: a panic in a Drop that
        // is itself running during an unwind aborts the process, and
        // the unwind path is a designed one here (the bus shutdown
        // panics on a pump that will not drain). So every lock is
        // taken defensively and a poisoned one is reported, never
        // unwrapped.
        self.open_charges.flush_color(color, "the execution ended before the job was read back");
        match self.live_configs.lock() {
            Ok(mut configs) => {
                configs.remove(&color);
            }
            Err(_) => tracing::error!(
                target: "weft_engine::run_pod",
                %color,
                "the live-config map is poisoned, so this execution's config was not dropped; \
                 the connection server may still hand out a config for it until the pod exits"
            ),
        }
        self.caller_registry.detach(color);
        // The cancel registry is an async lock, so its removal is a
        // task; `Handle::try_current` because a destructor can run
        // while the runtime is shutting down, where `tokio::spawn`
        // panics. Removing only if the flag is still OURS.
        let registry = self.cancel_registry.clone();
        let mine = self.flag.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let mut reg = registry.lock().await;
                    if reg.get(&color).is_some_and(|f| Arc::ptr_eq(f, &mine)) {
                        reg.remove(&color);
                    }
                });
            }
            Err(_) => tracing::error!(
                target: "weft_engine::run_pod",
                %color,
                "no runtime to drop this execution's cancel flag on (the pod is tearing down); \
                 the entry goes with the process"
            ),
        }
    }
}

/// `IdleExit` impl backed by the broker's guarded CAS. The picker
/// calls `try_idle_exit` after the idle window; the broker flips
/// `alive -> done` only if no pending/claimed work exists for the
/// project, so a concurrent exec keeps the pod alive. A cost record
/// still being resolved (a metered call's figure being written down)
/// also keeps the pod alive: money bookkeeping never dies with an
/// idle exit.
struct WorkerIdleExit {
    worker_pods: Arc<dyn WorkerPodClient>,
    pod_name: String,
    pending_costs: Arc<crate::metering::PendingCostRecords>,
    /// A charge whose amount a later response will state keeps the pod
    /// alive too: the job is still running, and the read that prices it
    /// has not happened yet.
    open_charges: Arc<crate::metering::OpenCharges>,
}

#[async_trait::async_trait]
impl weft_task_store::executor::IdleExit for WorkerIdleExit {
    async fn try_idle_exit(&self) -> anyhow::Result<bool> {
        if self.pending_costs.count() > 0 || self.open_charges.count() > 0 {
            return Ok(false);
        }
        self.worker_pods.mark_done_if_idle(&self.pod_name).await
    }
}

/// Per-Pod runtime context for worker task kinds. The registry's
/// `WorkerTaskKind::handle` impls receive `&WorkerCtx`.
///
/// Note: the `ProjectDefinition` is NOT held here. Each execution
/// claim fetches its own definition from the broker keyed by the
/// task payload's `definition_hash`. The pod caches by hash in
/// `project_cache` so two executions of the same shape pay a single
/// round trip.
#[derive(Clone)]
struct WorkerCtx {
    project_id: String,
    catalog: Arc<dyn NodeCatalog>,
    clients: EngineClients,
    pod_name: String,
    tenant_id: String,
    /// The k8s namespace this worker pod runs in. For an infra project
    /// this is the project's own per-project namespace; for a no-infra
    /// project it is the shared worker namespace. Threaded into
    /// `InfraProvisionContext` so `Node::provision_infra` bodies see the
    /// runtime namespace they're being applied into. (Only infra
    /// projects provision, and those always run in their own namespace,
    /// so a provision body never sees the shared namespace.)
    namespace: String,
    cancel_registry: CancelRegistry,
    /// One drive per color at a time on this pod (see [`ColorGate`]).
    driving: ColorGate,
    /// Cache of fetched definitions keyed by `definition_hash`.
    /// Workers fetch each hash they encounter once; consecutive
    /// claims on the same hash reuse the cached `ProjectDefinition`.
    /// `Arc<ProjectDefinition>` so handing the value to
    /// `run_one_execution` is a refcount bump, not a clone of the
    /// graph. BOUNDED (see `BoundedProjectCache`): every project edit
    /// mints a new hash, so an unbounded map on a long-lived pod for an
    /// actively-edited project would accumulate one full graph per edit
    /// forever. The cache only needs to dedupe consecutive claims of the
    /// same shape, so a small capacity (latest shape plus a few in-flight
    /// resume shapes pinning older hashes) is enough.
    project_cache: ProjectCache,
    /// Per-pod live caller registry: the connection server attaches an
    /// accepted socket here keyed by color; the execute path awaits it.
    caller_registry: crate::caller_conn::CallerRegistry,
    /// Per-color live-connection runtime config, populated by the execute
    /// path from the task payload before the caller attaches, read by the
    /// connection server's resolver to build the connection.
    live_configs: LiveConfigMap,
}

type ProjectCache = Arc<Mutex<BoundedProjectCache>>;

/// Serializes the drives of one color on this pod. A resume is pinned
/// to the color's owner, and it can land while the owner is still
/// driving the color: a person answers a form between the node's
/// `SuspensionRegistered` and the drive's `NodeSuspended` (the node
/// body has registered its wait but has not returned yet), so the
/// resume task is claimed by this same pod while the first drive is
/// mid-flight. Two drives of one color fold the journal twice and run
/// the parked node twice (the second one as a crashed-Running re-run,
/// since the first has not written `NodeSuspended` yet): every side
/// effect below it happens twice. Held for the whole drive, the gate
/// makes the resume wait for the live drive, which resumes the answer
/// in place; the queued drive then folds a journal that already holds
/// the terminal and settles as `AlreadySettled`.
#[derive(Clone, Default)]
struct ColorGate {
    gates: Arc<std::sync::Mutex<HashMap<Color, Arc<Mutex<()>>>>>,
}

/// The gate held for one color; drop it when the drive is over.
struct HeldColor {
    color: Color,
    gates: ColorGate,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl ColorGate {
    /// The gate map, whether or not a previous holder panicked while
    /// holding it.
    ///
    /// The map holds nothing but `Color -> Arc<Mutex<()>>`: an insert or
    /// a remove either happened or did not, so a panic mid-way leaves no
    /// half-built state for a later caller to trip over, and poisoning
    /// carries no information worth acting on. Taking the poison as fatal
    /// is what would hurt: `hold` runs on every drive, so one poisoned
    /// map would panic every later drive on the pod, and the release side
    /// runs inside a destructor during the bus-shutdown unwind, where a
    /// panic aborts the process. Both sides go through here so they
    /// cannot disagree about that.
    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<Color, Arc<Mutex<()>>>> {
        self.gates.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Wait for any drive of `color` on this pod to end, then hold it.
    async fn hold(&self, color: Color) -> HeldColor {
        let gate = self.map().entry(color).or_default().clone();
        let guard = gate.lock_owned().await;
        HeldColor { color, gates: self.clone(), guard: Some(guard) }
    }
}

impl Drop for HeldColor {
    fn drop(&mut self) {
        // Release first, then forget the gate if nobody is waiting on
        // it (the map's own reference is the only one left).
        self.guard.take();
        // NEVER panics: this runs during the bus-shutdown unwind (it is
        // declared beside `ExecutionResidue`, whose own destructor says
        // the same thing), and a panic in a destructor that is itself
        // unwinding aborts the process. `ColorGate::map` is what keeps
        // that promise.
        let mut gates = self.gates.map();
        if gates.get(&self.color).is_some_and(|g| Arc::strong_count(g) == 1) {
            gates.remove(&self.color);
        }
    }
}

/// Per-color live-connection runtime config + heartbeat interval, set by
/// the execute path and read by the connection server's resolver. Uses a
/// std (sync) mutex: the resolver trait is sync and the critical section
/// is a map lookup, never held across an await.
/// What a live execution registers for the connection server before its
/// caller attaches: the runtime config, the heartbeat interval, and the
/// caller's opening request from the start record.
struct LiveStart {
    runtime: weft_core::caller::CallerRuntimeConfig,
    heartbeat_secs: u64,
    request: Arc<weft_core::caller::LiveRequest>,
}

type LiveConfigMap = Arc<std::sync::Mutex<HashMap<Color, Arc<LiveStart>>>>;

/// Insertion-ordered cache bounded to `CAP` entries. On overflow it
/// evicts the oldest-inserted hash. Not a true LRU (no per-get reorder):
/// the access pattern is "claim a hash, reuse it for the burst of
/// executions on that shape, move to the next shape," so insertion order
/// already tracks recency closely enough, and the dumb shape keeps the
/// hot `get` path a plain map lookup with no bookkeeping.
struct BoundedProjectCache {
    map: HashMap<String, Arc<ProjectDefinition>>,
    order: std::collections::VecDeque<String>,
}

impl BoundedProjectCache {
    const CAP: usize = 8;

    fn new() -> Self {
        Self {
            map: HashMap::new(),
            order: std::collections::VecDeque::new(),
        }
    }

    fn get(&self, hash: &str) -> Option<Arc<ProjectDefinition>> {
        self.map.get(hash).cloned()
    }

    fn insert(&mut self, hash: String, def: Arc<ProjectDefinition>) {
        if self.map.insert(hash.clone(), def).is_none() {
            self.order.push_back(hash);
            while self.order.len() > Self::CAP {
                if let Some(evicted) = self.order.pop_front() {
                    self.map.remove(&evicted);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_pod(
    catalog: Arc<dyn NodeCatalog>,
    clients: EngineClients,
    worker_pods: Arc<dyn WorkerPodClient>,
    pod_name: String,
    project_id: String,
    tenant_id: String,
    namespace: String,
    // Live caller connection server: the TCP port the worker accepts
    // gateway-forwarded connections on, and the HMAC secret it verifies
    // dispatcher-signed routing tokens with. The generated `main` reads
    // both from env (`WEFT_CONNECTION_PORT`, `WEFT_CALLER_TOKEN_SECRET`).
    // `None` means no secret was provisioned (local dev without the
    // gateway): the connection server is NOT started, so this worker simply
    // has no live-caller capability. An EMPTY-but-`Some` secret is never
    // constructed; an empty HMAC key validates forgeable tokens (fail-open),
    // so the boundary collapses "empty" to `None` once and never feeds an
    // empty key into the validator.
    connection_port: u16,
    token_secret: Option<Vec<u8>>,
) -> Result<()> {
    weft_core::net::install_crypto_provider();
    let shutdown = Arc::new(AtomicBool::new(false));
    let cancel_registry: CancelRegistry = Arc::new(Mutex::new(HashMap::new()));

    worker_pods
        .register_alive(&pod_name, &project_id)
        .await?;
    spawn_heartbeat(
        worker_pods.clone(),
        pod_name.clone(),
        shutdown.clone(),
        weft_platform_traits::CgroupMemPressure::new(),
    );

    // Per-pod live caller registry + the per-color config the connection
    // server resolves when a caller attaches. The execute path populates
    // `live_configs` from the task payload BEFORE the caller arrives;
    // the server reads it to build the connection.
    let caller_registry = crate::caller_conn::CallerRegistry::new();
    let live_configs: LiveConfigMap = Arc::new(std::sync::Mutex::new(HashMap::new()));
    match token_secret {
        Some(secret) => spawn_connection_server(
            caller_registry.clone(),
            live_configs.clone(),
            cancel_registry.clone(),
            clients.clock.clone(),
            clients.journal.clone(),
            clients.tasks.clone(),
            pod_name.clone(),
            tenant_id.clone(),
            secret,
            connection_port,
        ),
        None => tracing::info!(
            target: "weft_engine::caller_conn",
            "no caller-token secret provisioned; live caller connection server NOT started \
             (this worker serves pull-queue work only)"
        ),
    }
    let picker_tasks = clients.tasks.clone();
    let ctx = WorkerCtx {
        project_id: project_id.clone(),
        catalog,
        clients,
        pod_name: pod_name.clone(),
        tenant_id,
        namespace,
        cancel_registry: cancel_registry.clone(),
        driving: ColorGate::default(),
        project_cache: Arc::new(Mutex::new(BoundedProjectCache::new())),
        caller_registry,
        live_configs,
    };

    let registry = WorkerTaskRegistry::builder()
        .register(TaskKind::Execute, Arc::new(ExecuteKind))
        .register(TaskKind::Resume, Arc::new(ExecuteKind))
        .register(TaskKind::CancelExecution, Arc::new(CancelExecutionKind))
        .build();

    // Idle self-exit: after `WORKER_IDLE_EXIT` of no claimable
    // work, the picker attempts the guarded `alive -> done` CAS via
    // the broker. The CAS (not the timer) is the correctness gate.
    let pending_costs = ctx.clients.pending_costs.clone();
    let open_charges = ctx.clients.open_charges.clone();
    let idle_exit: Arc<dyn weft_task_store::executor::IdleExit> = Arc::new(WorkerIdleExit {
        worker_pods: worker_pods.clone(),
        pod_name: pod_name.clone(),
        pending_costs: pending_costs.clone(),
        open_charges: open_charges.clone(),
    });
    // Every execution and resume this pod drives runs detached from the
    // picker; this is the count of them, and the gate the shutdown below
    // waits on.
    let background = weft_core::in_flight::InFlight::new("worker execution");
    run_worker_picker(
        picker_tasks,
        ctx,
        registry,
        pod_name.clone(),
        project_id,
        shutdown.clone(),
        idle_exit,
        WORKER_IDLE_EXIT,
        background.clone(),
    )
    .await;

    // Pod-wide shutdown: cancel every in-flight execution, through the
    // SAME call the dispatcher's cancel task makes, so a shutdown and a
    // user's cancel stop an execution the one way.
    let colors: Vec<Color> = {
        let g = cancel_registry.lock().await;
        g.keys().copied().collect()
    };
    for color in colors {
        cancel_color(
            &cancel_registry,
            color,
            weft_core::exec::CancelCause::Runtime {
                detail: "the worker pod running this execution was shutting down".into(),
            },
        )
        .await;
    }
    shutdown.store(true, Ordering::Relaxed);
    // Then WAIT for them. A cancel asks an execution to stop; it does
    // not stop it. The driver checks the flag at the top of each
    // iteration, lets its in-flight node tasks finish, folds the journal
    // and writes the terminal row. Exiting before that leaves executions
    // with no ending recorded and their task rows claimed until a lease
    // lapses, which is the state this pod is shutting down BECAUSE of.
    //
    // No deadline: what is being waited on is the tail of work that has
    // already been told to stop, and every node task is itself bounded
    // by the cancellation it was just handed.
    background.wait_zero().await;
    // Money bookkeeping outlives the executions: wait for every in-flight
    // cost resolution to land its record before the row is marked done.
    // Each resolve is internally bounded (request timeout + fixed ledger
    // budget), so this wait always ends.
    //
    // A charge still open once the executions have ended is a call that
    // spent and whose amount no response ever stated (a job submitted and
    // never read back). The pod is going away, so nothing can arrive to
    // price it: book it as the unknown it is, before the wait, so the row
    // still lands.
    open_charges.flush("the worker pod shut down before the job was read back");
    pending_costs.wait_zero().await;
    let _ = worker_pods.mark_done(&pod_name).await;
    Ok(())
}

/// Stop one execution running on this pod: the single place a cancel is
/// applied, whether it came from a person (`weft stop`, the graph's
/// Cancel), from a deactivate, or from the pod shutting itself down.
///
/// Firing the flag is the whole of it. The driver owns what happens
/// next: it notices at the top of its next iteration, lets the node
/// tasks it already started finish, closes the charges they opened, and
/// writes the execution's terminal row.
///
/// An unknown color is not an error. The execution finished on its own
/// between the dispatcher reading this pod's row and the cancel landing,
/// and the run already has its natural ending.
async fn cancel_color(
    registry: &CancelRegistry,
    color: Color,
    cause: weft_core::exec::CancelCause,
) {
    let flag = registry.lock().await.get(&color).cloned();
    match flag {
        Some(f) => {
            tracing::info!(
                target: "weft_engine::run_pod",
                color = %color,
                cause = %cause,
                "firing per-color cancel flag"
            );
            f.cancel_because(cause);
        }
        None => tracing::debug!(
            target: "weft_engine::run_pod",
            color = %color,
            "cancel for unknown color (already terminal); no-op"
        ),
    }
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
    mem_pressure: Arc<dyn weft_platform_traits::MemPressure>,
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
            // Read the pod's own cgroup memory pressure each tick and
            // report it with the heartbeat, so the dispatcher places and
            // scales workers by real memory load (0.0 locally, where
            // there is no cgroup limit, so one worker until squeezed).
            let pressure = mem_pressure.fraction();
            match worker_pods.heartbeat(&pod_name, pressure).await {
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

// ----- Live caller connection wiring ---------------------------------

/// Journal sink that projects caller events to `ExecEvent::Caller*` rows
/// via the broker. Each event is recorded on a spawned task (the
/// connection hot path stays sync + non-blocking). Live connections are
/// non-durable, so a best-effort spawn matches the design: the exchange
/// is observable/replayable, not a resume-critical durability story.
struct BrokerCallerJournal {
    /// Rows go out through ONE writer task, in the order they were
    /// handed over. Two spawned writes would race, and the pair that
    /// races is exactly the pair whose order carries meaning: the window
    /// holding a conversation's last messages, and the row saying the
    /// caller hung up. A reader would see the goodbye before the words.
    rows: tokio::sync::mpsc::UnboundedSender<weft_journal::ExecEvent>,
    /// Why this conversation's journal stopped working, once it has.
    /// The next thing the program tries to send the caller fails with
    /// it, the same way a bus whose journal failed refuses the next
    /// send: a run that finishes looking clean while its exchange is
    /// missing from the record is the silent loss both exist to stop.
    degraded: Arc<std::sync::Mutex<Option<String>>>,
    /// What the journal keeps of this conversation: the same policy
    /// type a bus carries, so the two cannot drift on where content
    /// gets trimmed or on what "ephemeral" means.
    policy: weft_core::stream_journal::JournalPolicy,
    pending: std::sync::Mutex<PendingCallerWindow>,
}

/// Messages said since the last row went out.
#[derive(Default)]
struct PendingCallerWindow {
    /// One connection is one execution, so the color is the same for
    /// every message; kept from the first one rather than passed to the
    /// flush.
    color: Option<Color>,
    messages: Vec<weft_core::stream_journal::WindowedCallerMessage>,
    /// What the messages above already weigh, so a window closes on
    /// size as well as on time (see `JOURNAL_ROW_BYTES`).
    kept_bytes: usize,
    /// The exchange is over: the ticker can stop.
    closed: bool,
}

impl BrokerCallerJournal {
    /// Build the sink and start its window clock. The clock stops when
    /// the exchange ends or when the connection drops the sink,
    /// whichever comes first, so a conversation never leaves a task
    /// behind.
    fn start(
        journal: Arc<dyn weft_journal::JournalClient>,
        pod_name: String,
        policy: weft_core::stream_journal::JournalPolicy,
    ) -> Arc<Self> {
        let (rows, mut incoming) = tokio::sync::mpsc::unbounded_channel();
        let degraded: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        // The writer. One task, one row at a time, awaited: the queue IS
        // the ordering. It ends when the sink drops and the channel
        // closes, so a conversation never leaves a task behind.
        let writer_degraded = degraded.clone();
        tokio::spawn(async move {
            while let Some(event) = incoming.recv().await {
                if let Err(e) = journal.record_event(&event, Some(&pod_name)).await {
                    tracing::error!(
                        target: "weft_engine::caller_conn",
                        error = %e,
                        "caller journal write failed; the exchange is no longer being recorded"
                    );
                    let mut slot = writer_degraded.lock().expect("caller journal degraded");
                    // Keep the FIRST reason: it is the one that explains
                    // the gap, and the ones after it are its echoes.
                    slot.get_or_insert_with(|| format!("journal write failed: {e}"));
                }
            }
        });
        let sink = Arc::new(Self {
            rows,
            degraded,
            policy,
            pending: std::sync::Mutex::new(PendingCallerWindow::default()),
        });
        let weak = Arc::downgrade(&sink);
        let window = policy.window;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(window);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(sink) = weak.upgrade() else { return };
                if sink.closed() {
                    return;
                }
                sink.flush();
            }
        });
        sink
    }

    fn closed(&self) -> bool {
        self.pending.lock().expect("caller journal buffer").closed
    }

    /// Hold one message for the open window. What the journal keeps of
    /// it is decided here rather than at flush time, so an oversized
    /// payload is cut once and the buffer never holds more than the
    /// journal will.
    fn hold(
        &self,
        color: Color,
        offset: u64,
        direction: weft_core::stream_journal::CallerDirection,
        payload: &weft_core::bus::WirePayload,
        terminal: bool,
    ) {
        let kept = weft_core::stream_journal::record(payload, &self.policy);
        let weighs = kept.kept_bytes();
        // A window closes on whichever comes first, its clock or its
        // size: a chatty socket can otherwise put more in one second
        // than one journal row may carry, and the row is then refused
        // for ever. Flushed BEFORE this message joins, so it opens the
        // next window rather than overflowing this one.
        let full = {
            let pending = self.pending.lock().expect("caller journal buffer");
            self.policy.row_is_full(pending.kept_bytes, weighs)
        };
        if full {
            self.flush();
        }
        let mut pending = self.pending.lock().expect("caller journal buffer");
        pending.color.get_or_insert(color);
        pending.kept_bytes += weighs;
        pending.messages.push(weft_core::stream_journal::WindowedCallerMessage {
            offset,
            direction,
            payload: kept.payload,
            payload_byte_size: kept.byte_size,
            trimmed: kept.trimmed,
            terminal,
            at_unix: crate::now_unix(),
        });
    }

    /// Write the open window, if it holds anything.
    fn flush(&self) {
        let (color, messages) = {
            let mut pending = self.pending.lock().expect("caller journal buffer");
            if pending.messages.is_empty() {
                return;
            }
            pending.kept_bytes = 0;
            (pending.color, std::mem::take(&mut pending.messages))
        };
        let Some(color) = color else { return };
        let Some(window) = weft_core::stream_journal::aggregate_caller_window(messages) else {
            return;
        };
        self.emit(weft_journal::ExecEvent::CallerWindow {
            color,
            first_offset: window.first_offset,
            last_offset: window.last_offset,
            messages: window.messages,
            totals: window.totals,
            at_unix: window.last_at_unix,
        });
    }

    /// A lifecycle row (connected, errored, disconnected) goes out on
    /// its own, so the open window is written first: the row stream
    /// must never say a thing happened before something it followed.
    /// Both land on the one queue, in this order, and the writer keeps
    /// them in it.
    fn emit_after_flush(&self, event: weft_journal::ExecEvent) {
        self.flush();
        self.emit(event);
    }

    /// Hand one row to the writer. The send only fails once the writer
    /// is gone, which happens when this sink is being dropped, so there
    /// is nothing left to tell.
    fn emit(&self, event: weft_journal::ExecEvent) {
        let _ = self.rows.send(event);
    }
}

/// Project an `InboundMessage` into the journal's payload vocabulary.
/// Bytes stay bytes: what the journal keeps of them is one rule for
/// every channel and it lives in `stream_journal`, not here, so a
/// conversation and a bus cannot answer it differently.
fn inbound_payload(msg: &InboundMessage) -> weft_core::bus::WirePayload {
    match msg {
        InboundMessage::Json(v) => weft_core::bus::WirePayload::Json(v.clone()),
        InboundMessage::Text(s) => {
            weft_core::bus::WirePayload::Json(serde_json::Value::String(s.clone()))
        }
        InboundMessage::Bytes(b) => {
            weft_core::bus::WirePayload::Bytes(bytes::Bytes::from(b.clone()))
        }
    }
}

fn outbound_payload(chunk: &OutboundChunk) -> weft_core::bus::WirePayload {
    match chunk {
        OutboundChunk::Json(v) => weft_core::bus::WirePayload::Json(v.clone()),
        OutboundChunk::Text(s) => {
            weft_core::bus::WirePayload::Json(serde_json::Value::String(s.clone()))
        }
        OutboundChunk::Bytes(b) => {
            weft_core::bus::WirePayload::Bytes(bytes::Bytes::from(b.clone()))
        }
    }
}

impl crate::caller_conn::CallerJournalSink for BrokerCallerJournal {
    fn degraded(&self) -> Option<String> {
        self.degraded.lock().expect("caller journal degraded").clone()
    }
    fn connected(&self, color: Color, offset: u64, protocol: weft_core::signal::Protocol) {
        self.emit(weft_journal::ExecEvent::CallerConnected {
            color,
            offset,
            protocol: protocol.as_wire_str().to_string(),
            at_unix: crate::now_unix(),
        });
    }
    fn inbound(&self, color: Color, offset: u64, msg: &weft_core::caller::InboundMessage) {
        self.hold(
            color,
            offset,
            weft_core::stream_journal::CallerDirection::Inbound,
            &inbound_payload(msg),
            false,
        );
    }
    fn outbound(
        &self,
        color: Color,
        offset: u64,
        chunk: &weft_core::caller::OutboundChunk,
        terminal: bool,
    ) {
        self.hold(
            color,
            offset,
            weft_core::stream_journal::CallerDirection::Outbound,
            &outbound_payload(chunk),
            terminal,
        );
    }
    fn errored(&self, color: Color, offset: u64, message: &str) {
        self.emit_after_flush(weft_journal::ExecEvent::CallerErrored {
            color,
            offset,
            message: message.to_string(),
            at_unix: crate::now_unix(),
        });
    }
    fn disconnected(&self, color: Color, offset: u64, reason: &str) {
        // The exchange is over: write what is held, say so, and let the
        // window clock stop. Nothing said after this can be lost,
        // because nothing is said after this.
        self.emit_after_flush(weft_journal::ExecEvent::CallerDisconnected {
            color,
            offset,
            reason: reason.to_string(),
            at_unix: crate::now_unix(),
        });
        self.pending.lock().expect("caller journal buffer").closed = true;
    }
}

/// Resolver over the worker's per-color live-config map. The connection
/// server calls this when a caller attaches to learn the protocol/caps so
/// it can build the connection; an unknown color (caller raced ahead of,
/// or long after, the execute task) returns `None` and the server 404s.
struct LiveConfigResolver {
    live_configs: LiveConfigMap,
    journal: Arc<dyn weft_journal::JournalClient>,
    pod_name: String,
}

impl crate::caller_conn::ConnConfigResolver for LiveConfigResolver {
    fn resolve(&self, color: Color) -> Option<crate::caller_conn::ResolvedLiveStart> {
        let start = self
            .live_configs
            .lock()
            .expect("live_configs poisoned")
            .get(&color)
            .cloned()?;
        let sink: Arc<dyn crate::caller_conn::CallerJournalSink> = BrokerCallerJournal::start(
            self.journal.clone(),
            self.pod_name.clone(),
            start.runtime.journal,
        );
        Some(crate::caller_conn::ResolvedLiveStart {
            config: start.runtime.clone(),
            heartbeat_secs: start.heartbeat_secs,
            request: start.request.clone(),
            journal: sink,
        })
    }
}

/// Canceller over the pod-local per-color cancel registry. The connection
/// server fires it when a caller drops in a caller-tied (cancel) run.
struct RegistryCanceller {
    cancel_registry: CancelRegistry,
}

impl crate::caller_conn::ExecutionCanceller for RegistryCanceller {
    fn cancel(&self, color: Color) {
        // Block-in-place is wrong here (sync trait method on an async
        // mutex); use try_lock in a short spin via the blocking handle.
        // The cancel registry is a tokio Mutex; grab it with a dedicated
        // runtime-blocking section. In practice it's never contended.
        let reg = self.cancel_registry.clone();
        tokio::spawn(async move {
            if let Some(flag) = reg.lock().await.get(&color).cloned() {
                flag.cancel_because(weft_core::exec::CancelCause::CallerGone);
            }
        });
    }
}

/// Start the live caller connection server on `port`. Plain HTTP/WS
/// inside the cluster (TLS terminates at the gateway); the signed token
/// authenticates every connection.
#[allow(clippy::too_many_arguments)]
fn spawn_connection_server(
    caller_registry: crate::caller_conn::CallerRegistry,
    live_configs: LiveConfigMap,
    cancel_registry: CancelRegistry,
    clock: Arc<dyn weft_platform_traits::Clock>,
    journal: Arc<dyn weft_journal::JournalClient>,
    tasks: Arc<dyn weft_task_store::TaskStoreClient>,
    pod_name: String,
    tenant_id: String,
    token_secret: Vec<u8>,
    port: u16,
) {
    let state = crate::caller_conn::ConnServerState {
        registry: caller_registry,
        token_secret: Arc::new(token_secret),
        pod_name: pod_name.clone(),
        resolver: Arc::new(LiveConfigResolver {
            live_configs,
            journal,
            pod_name,
        }),
        clock,
        canceller: Arc::new(RegistryCanceller { cancel_registry }),
        tasks,
        tenant_id,
    };
    tokio::spawn(async move {
        if let Err(e) = crate::caller_conn::serve(state, port).await {
            tracing::error!(
                target: "weft_engine::caller_conn",
                error = %e,
                "connection server exited"
            );
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

        // Per-execution definition fetch with pod-local hash cache.
        // First claim of a given (project_id, definition_hash) pays
        // one broker round trip; consecutive claims on the same hash
        // hand back the cached `Arc<ProjectDefinition>` via the
        // cache's `get`. A 404 from the broker (no history row for
        // this hash) is a hard error: the dispatcher should never
        // enqueue a task for a hash whose history row doesn't exist
        // (the set_running_definition_hash precondition refuses that),
        // so a miss here is a real upstream bug.
        let project = fetch_or_cached_project(ctx, &payload.definition_hash).await?;
        let _driving = ctx.driving.hold(color).await;

        let flag = CancellationFlag::new_arc();
        ctx.cancel_registry
            .lock()
            .await
            .insert(color, flag.clone());
        // From here on every exit path, including a panic, releases what
        // this execution registered on the pod.
        let _residue = ExecutionResidue {
            color,
            flag: flag.clone(),
            cancel_registry: ctx.cancel_registry.clone(),
            caller_registry: ctx.caller_registry.clone(),
            live_configs: ctx.live_configs.clone(),
            open_charges: ctx.clients.open_charges.clone(),
        };

        // Live-connection executions carry their trigger's `live_connection`
        // start record. Register the runtime config + the caller's request
        // so the connection server can build the connection when the
        // caller's socket attaches, then wait (bounded by the connect
        // timeout) for the attach so `ctx.caller()` resolves. A no-show
        // leaves `caller = None`; the run proceeds and any node that needs
        // the caller fails loud via the handle's `ensure_connected()`. A
        // malformed record is a dispatcher/worker mismatch: the execution
        // fails rather than running as if nobody were on the line.
        let caller = match &payload.live_connection {
            Some(start) => attach_live_caller(ctx, color, start).await?,
            None => None,
        };
        // Keep the connection so we can end the exchange with the caller
        // after the execution returns (the run takes its own reference).
        // Only a REAL caller needs this: a fired run's exchange is over
        // the moment the program answers, with no socket left to tell.
        let caller_after_run = caller.as_ref().and_then(RunCaller::live);
        let caller = caller.map(|c| c.as_connection());

        let outcome = run_one_execution(
            project,
            ctx.catalog.clone(),
            color,
            ctx.clients.clone(),
            ctx.pod_name.clone(),
            ctx.tenant_id.clone(),
            ctx.namespace.clone(),
            flag,
            caller,
        )
        .await;

        // A caller is attached and the run is over: the exchange ends
        // now, never when the worker exits. A run that did not complete
        // tells the caller why (per the error mode) instead of leaving a
        // silently dropped socket: a driver error, a failed node, a
        // cancel, a stuck graph, and a color that was already settled
        // before this task claimed it all end the same way for the
        // caller, no answer is coming. A run that completed (or stalled
        // into a background job, which resumes without a caller) ends
        // the exchange the way the program left it: a finished stream, a
        // closed socket, or a loud "never answered" on a silent route.
        // Safe after the run returned: the outbound queue drops a push
        // once the caller was answered or closed, so a run that already
        // spoke its last word is not double-messaged.
        if let Some(conn) = &caller_after_run {
            match &outcome {
                Err(e) => conn.surface_error(&format!("execution failed: {e}")).await,
                Ok(ExecutionOutcome::Failed { error }) => {
                    conn.surface_error(&format!("execution failed: {error}")).await
                }
                Ok(ExecutionOutcome::Cancelled { cause }) => {
                    conn.surface_error(&format!("execution cancelled: {cause}")).await
                }
                Ok(ExecutionOutcome::Stuck { report }) => conn.surface_error(&report.to_string()).await,
                Ok(ExecutionOutcome::AlreadySettled) => {
                    conn.surface_error("execution already ended before this worker claimed it").await
                }
                Ok(ExecutionOutcome::Completed) | Ok(ExecutionOutcome::Stalled) => conn.run_ended().await,
            }
        }

        // The cancel flag, the live config and any attached connection
        // are released by `ExecutionResidue` when this returns or
        // unwinds; nothing to do here.
        // No explicit slot release: a live execution's capacity slot IS its
        // execute task row, and the executor flips that task terminal
        // (complete/failed) when this handler returns. Once the task leaves
        // pending/claimed it no longer counts toward the pod's live load, so
        // the slot frees automatically (admission == task existence).

        outcome.map(|_| ())
    }
}

/// Register a live-connection execution's runtime config + opening request
/// and wait for the caller's socket to attach. Returns the attached
/// connection, or `None` if the caller never arrives within the connect
/// timeout. A start record the worker cannot read (a kind that is not a
/// live caller, a config that does not parse) is an error: the dispatcher
/// wrote it, so it is a version mismatch, not a run without a caller.
/// The caller for this run, whoever it is: the real one waiting on a
/// socket, or the stand-in a FIRED run serves its own body to.
///
/// Both are `CallerConnection`s, so the trigger and every node behind
/// it run the same code either way. This is the one place that knows
/// the difference, and it knows it from one field on the start record.
enum RunCaller {
    Live(Arc<crate::caller_conn::LiveCallerConnection>),
    Fired(Arc<crate::fired_caller::FiredCaller>),
}

impl RunCaller {
    fn as_connection(&self) -> Arc<dyn weft_core::caller::CallerConnection> {
        match self {
            Self::Live(c) => c.clone(),
            Self::Fired(c) => c.clone(),
        }
    }

    /// The real connection, for the end-of-run tidy-up that only a
    /// socket needs. A fired run's exchange ends when the program
    /// answers and there is nothing to close afterwards.
    fn live(&self) -> Option<Arc<crate::caller_conn::LiveCallerConnection>> {
        match self {
            Self::Live(c) => Some(c.clone()),
            Self::Fired(_) => None,
        }
    }
}

async fn attach_live_caller(
    ctx: &WorkerCtx,
    color: Color,
    start: &weft_task_store::kinds::LiveConnectionStart,
) -> Result<Option<RunCaller>> {
    // The record carries the full signal spec: the protocol is the kind
    // (tag), the connection knobs are the config body.
    let protocol = weft_core::signal::protocol_for_tag(&start.spec.kind).with_context(|| {
        format!(
            "execute task for {color} tagged a non-live-caller kind '{}' as live",
            start.spec.kind
        )
    })?;
    let cfg: weft_core::signal::LiveConnectionConfig =
        serde_json::from_value(start.spec.config.clone())
            .with_context(|| format!("live-caller config on the execute task for {color}"))?;
    let runtime = weft_core::caller::CallerRuntimeConfig::from_config(&cfg, protocol);
    let connect_timeout = std::time::Duration::from_secs(runtime.connect_timeout_secs);
    ctx.live_configs.lock().expect("live_configs poisoned").insert(
        color,
        Arc::new(LiveStart {
            runtime,
            heartbeat_secs: cfg.heartbeat_interval_secs,
            request: Arc::new(start.request.clone()),
        }),
    );
    // A FIRED run has no socket coming, so waiting for one would burn
    // the whole connect timeout and then run with nobody there. Serve
    // the body the author typed instead, and record the exchange the
    // same way a real one is recorded.
    if start.fired.is_some() {
        if protocol != weft_core::signal::Protocol::Http {
            anyhow::bail!(
                "a Socket cannot be fired: its shape is a conversation over time, and there is \
                 nothing honest to invent for the caller's next message. Point a real client at \
                 it (`weft activate` prints the URL)"
            );
        }
        let journal: Arc<dyn crate::caller_conn::CallerJournalSink> = BrokerCallerJournal::start(
            ctx.clients.journal.clone(),
            ctx.pod_name.clone(),
            cfg.journal_policy(),
        );
        // The stand-in serves the REQUEST and records the answer, and
        // that is all it can honestly do: a fired run's body, when the
        // author typed one, is a field of the trigger's own wake
        // payload, and the node reads it there. Serving it through here
        // would mean impersonating a caller who never sent it, and the
        // field name to do that would have to live in the language.
        return Ok(Some(RunCaller::Fired(crate::fired_caller::FiredCaller::open(
            color,
            weft_core::caller::CallerRuntimeConfig::from_config(&cfg, protocol),
            start.request.clone(),
            journal,
        ))));
    }
    // Wait for the connection server to attach the socket for this color.
    Ok(ctx
        .caller_registry
        .wait_for_attach(color, connect_timeout)
        .await
        .map(RunCaller::Live))
}

/// Pod-local definition fetch: try the cache first; on miss, call
/// the broker; on success, populate the cache so the next execution
/// of the same shape skips the round trip.
///
/// The broker reads from the append-only `project_definition`
/// history table keyed by `(project_id, definition_hash)`, so a
/// resume task whose `definition_hash` was snapshotted on
/// `ExecutionStarted` (potentially under a now-old shape) still
/// gets the EXACT shape it was started on, even after the user has
/// edited and re-registered the project.
///
/// `Ok(None)` would mean the broker doesn't know about this hash
/// at all (no row was ever registered under it); that's a bug in
/// either the dispatcher's task production or the register flow,
/// so we surface it as a hard error.
async fn fetch_or_cached_project(
    ctx: &WorkerCtx,
    definition_hash: &str,
) -> Result<Arc<ProjectDefinition>> {
    {
        let cache = ctx.project_cache.lock().await;
        if let Some(p) = cache.get(definition_hash) {
            return Ok(p.clone());
        }
    }
    match ctx
        .clients
        .project
        .fetch_definition(&ctx.project_id, definition_hash)
        .await?
    {
        Some(def) => {
            let arc = Arc::new(def);
            ctx.project_cache
                .lock()
                .await
                .insert(definition_hash.to_string(), arc.clone());
            Ok(arc)
        }
        None => Err(anyhow::anyhow!(
            "no row in project_definition for project {} hash {}; the \
             dispatcher produced a task for a hash that was never recorded \
             (upstream bug in the task-producer)",
            ctx.project_id,
            definition_hash,
        )),
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
        cancel_color(&ctx.cancel_registry, color, payload.cause).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two drives of one color take turns; another color is not held up.
    #[tokio::test]
    async fn a_color_is_driven_once_at_a_time_on_a_pod() {
        let gate = ColorGate::default();
        let (a, b) = (Color::new_v4(), Color::new_v4());
        let first = gate.hold(a).await;
        let other = gate.hold(b).await;
        drop(other);
        let queued = tokio::spawn({
            let gate = gate.clone();
            async move {
                gate.hold(a).await;
            }
        });
        tokio::task::yield_now().await;
        assert!(!queued.is_finished(), "the second drive waits for the first");
        drop(first);
        queued.await.unwrap();
        assert!(gate.gates.lock().unwrap().is_empty(), "a released color is forgotten");
    }
}
