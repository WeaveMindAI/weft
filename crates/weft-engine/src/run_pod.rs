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
type LiveConfigMap =
    Arc<std::sync::Mutex<HashMap<Color, (weft_core::caller::CallerRuntimeConfig, u64)>>>;

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
            pod_name.clone(),
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
    // execution_driver checks the flag at every iteration top,
    // finishes its in-flight node tokio tasks, and exits. The picker
    // has already returned (run_worker_picker observes shutdown above).
    let flags: Vec<_> = {
        let g = cancel_registry.lock().await;
        g.values().cloned().collect()
    };
    for f in flags {
        f.cancel_because(weft_core::exec::CancelCause::Runtime {
            detail: "the worker pod running this execution was shutting down".into(),
        });
    }
    shutdown.store(true, Ordering::Relaxed);
    // Money bookkeeping outlives the executions: wait for every in-flight
    // cost resolution to land its record before the row is marked done.
    // Each resolve is internally bounded (request timeout + fixed ledger
    // budget), so this wait always ends.
    //
    // A charge still open here is a call that spent and whose amount no
    // response ever stated (a job submitted and never read back). The pod
    // is going away, so nothing can arrive to price it: book it as the
    // unknown it is, before the wait, so the row still lands.
    open_charges.flush("the worker pod shut down before the job was read back");
    pending_costs.wait_zero().await;
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
    journal: Arc<dyn weft_journal::JournalClient>,
    pod_name: String,
}

impl BrokerCallerJournal {
    fn emit(&self, event: weft_journal::ExecEvent) {
        let journal = self.journal.clone();
        let pod = self.pod_name.clone();
        tokio::spawn(async move {
            if let Err(e) = journal.record_event(&event, Some(&pod)).await {
                tracing::warn!(
                    target: "weft_engine::caller_conn",
                    error = %e,
                    "failed to journal caller event"
                );
            }
        });
    }
}

/// Project an `OutboundChunk`/`InboundMessage` into the journal payload
/// plus its byte size, using the same tagged `WirePayload` vocabulary
/// (and the same size derivation) as the bus's window rows, so the two
/// journaled-event paths can never drift.
fn caller_payload(value: serde_json::Value) -> (weft_core::bus::WirePayload, u64) {
    let payload = weft_core::bus::WirePayload::Json(value);
    let size = payload.byte_size();
    (payload, size)
}

fn inbound_to_value(msg: &InboundMessage) -> serde_json::Value {
    match msg {
        InboundMessage::Json(v) => v.clone(),
        InboundMessage::Text(s) => serde_json::Value::String(s.clone()),
        InboundMessage::Bytes(b) => serde_json::json!({ "bytes": b.len() }),
    }
}

fn outbound_to_value(chunk: &OutboundChunk) -> serde_json::Value {
    match chunk {
        OutboundChunk::Json(v) => v.clone(),
        OutboundChunk::Text(s) => serde_json::Value::String(s.clone()),
        OutboundChunk::Bytes(b) => serde_json::json!({ "bytes": b.len() }),
    }
}

impl crate::caller_conn::CallerJournalSink for BrokerCallerJournal {
    fn connected(&self, color: Color, offset: u64, protocol: weft_core::signal::Protocol) {
        self.emit(weft_journal::ExecEvent::CallerConnected {
            color,
            offset,
            protocol: protocol.as_wire_str().to_string(),
            at_unix: crate::now_unix(),
        });
    }
    fn inbound(&self, color: Color, offset: u64, msg: &weft_core::caller::InboundMessage) {
        let (payload, size) = caller_payload(inbound_to_value(msg));
        self.emit(weft_journal::ExecEvent::CallerInbound {
            color,
            offset,
            payload,
            payload_byte_size: size,
            at_unix: crate::now_unix(),
        });
    }
    fn outbound(
        &self,
        color: Color,
        offset: u64,
        chunk: &weft_core::caller::OutboundChunk,
        terminal: bool,
    ) {
        let (payload, size) = caller_payload(outbound_to_value(chunk));
        self.emit(weft_journal::ExecEvent::CallerOutbound {
            color,
            offset,
            payload,
            payload_byte_size: size,
            terminal,
            at_unix: crate::now_unix(),
        });
    }
    fn errored(&self, color: Color, offset: u64, message: &str) {
        self.emit(weft_journal::ExecEvent::CallerErrored {
            color,
            offset,
            message: message.to_string(),
            at_unix: crate::now_unix(),
        });
    }
    fn disconnected(&self, color: Color, offset: u64, reason: &str) {
        self.emit(weft_journal::ExecEvent::CallerDisconnected {
            color,
            offset,
            reason: reason.to_string(),
            at_unix: crate::now_unix(),
        });
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
    fn resolve(
        &self,
        color: Color,
    ) -> Option<(
        weft_core::caller::CallerRuntimeConfig,
        u64,
        Arc<dyn crate::caller_conn::CallerJournalSink>,
    )> {
        let (cfg, heartbeat) = self
            .live_configs
            .lock()
            .expect("live_configs poisoned")
            .get(&color)
            .cloned()?;
        let sink: Arc<dyn crate::caller_conn::CallerJournalSink> = Arc::new(BrokerCallerJournal {
            journal: self.journal.clone(),
            pod_name: self.pod_name.clone(),
        });
        Some((cfg, heartbeat, sink))
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
    pod_name: String,
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
        // config. Register the runtime config so the connection server can
        // build the connection when the caller's socket attaches, then wait
        // (bounded by the connect timeout) for the attach so `ctx.caller()`
        // resolves. A no-show leaves `caller = None`; the run proceeds and
        // any node that needs the caller fails loud via the handle's
        // `ensure_connected()`.
        let caller = match &payload.live_connection {
            Some(spec_json) => attach_live_caller(ctx, color, spec_json).await,
            None => None,
        };
        // Keep a clone so we can surface a run failure to the caller after
        // the execution returns (the connection itself moves into the run).
        let caller_for_error = caller.clone();

        let outcome = run_one_execution(
            project,
            ctx.catalog.clone(),
            color,
            ctx.clients.clone(),
            ctx.pod_name.clone(),
            ctx.tenant_id.clone(),
            ctx.namespace.clone(),
            flag,
            caller.map(|c| c as Arc<dyn weft_core::caller::CallerConnection>),
        )
        .await;

        // A caller is attached and the run did not complete: tell it why
        // (per the error mode) instead of leaving a silently dropped
        // socket. A driver error, a failed node, a cancel, a stuck graph,
        // and a color that was already settled before this task claimed
        // it all end the same way for the caller: no answer is coming.
        // Safe after the run returned: the outbound queue drops a push
        // once the caller was answered or closed, so a run that already
        // spoke its last word is not double-messaged.
        if let Some(conn) = &caller_for_error {
            let why = match &outcome {
                Err(e) => Some(format!("execution failed: {e}")),
                Ok(ExecutionOutcome::Failed { error }) => Some(format!("execution failed: {error}")),
                Ok(ExecutionOutcome::Cancelled { cause }) => Some(format!("execution cancelled: {cause}")),
                Ok(ExecutionOutcome::Stuck { report }) => Some(report.to_string()),
                Ok(ExecutionOutcome::AlreadySettled) => Some(
                    "execution already ended before this worker claimed it".to_string(),
                ),
                Ok(ExecutionOutcome::Completed) | Ok(ExecutionOutcome::Stalled) => None,
            };
            if let Some(why) = why {
                conn.surface_error(&why).await;
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

/// Register a live-connection execution's runtime config and wait for the
/// caller's socket to attach. Returns the attached connection, or `None`
/// if the trigger config is malformed (logged loud) or the caller never
/// arrives within the connect timeout.
async fn attach_live_caller(
    ctx: &WorkerCtx,
    color: Color,
    spec_json: &serde_json::Value,
) -> Option<Arc<crate::caller_conn::LiveCallerConnection>> {
    // The task carries the full signal spec: the protocol is the kind (tag),
    // the connection knobs are the config body.
    let spec: weft_core::primitive::SignalSpec = match serde_json::from_value(spec_json.clone()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                target: "weft_engine::caller_conn",
                color = %color, error = %e,
                "live-caller spec on the execute task is malformed; running without a caller"
            );
            return None;
        }
    };
    let Some(protocol) = weft_core::signal::protocol_for_tag(&spec.kind) else {
        tracing::error!(
            target: "weft_engine::caller_conn",
            color = %color, kind = %spec.kind,
            "execute task tagged a non-live-caller kind as live; running without a caller"
        );
        return None;
    };
    let cfg: weft_core::signal::LiveConnectionConfig = match serde_json::from_value(spec.config.clone()) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                target: "weft_engine::caller_conn",
                color = %color, error = %e,
                "live-caller config on the execute task is malformed; running without a caller"
            );
            return None;
        }
    };
    let runtime = weft_core::caller::CallerRuntimeConfig::from_config(&cfg, protocol);
    let connect_timeout = std::time::Duration::from_secs(runtime.connect_timeout_secs);
    ctx.live_configs
        .lock()
        .expect("live_configs poisoned")
        .insert(color, (runtime, cfg.heartbeat_interval_secs));
    // Wait for the connection server to attach the socket for this color.
    ctx.caller_registry.wait_for_attach(color, connect_timeout).await
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
        let flag = ctx.cancel_registry.lock().await.get(&color).cloned();
        match flag {
            Some(f) => {
                tracing::info!(
                    target: "weft_engine::run_pod",
                    color = %color,
                    cause = %payload.cause,
                    "firing per-color cancel flag"
                );
                f.cancel_because(payload.cause);
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
