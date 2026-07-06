//! Engine-side `ContextHandle`. Lifecycle events go straight to
//! the journal via the broker. Control-plane round-trips
//! (`await_signal`, `register_signal`) go through the dispatcher's
//! task queue (also via the broker): the worker enqueues a task row
//! and waits for completion. Resume values are seeded into the
//! per-(node, frames) await sequence by the loop driver at boot from
//! the journal fold; the body's `await_signal` calls pop entries in
//! call_index order.
//!
//! Infra-provision (the engine-side counterpart to user code's
//! `Node::provision` returning an `InfraSpec`) is driven by the loop
//! driver, NOT by methods on `RunnerHandle`. The loop driver calls
//! `node.provision`, compiles + hashes the returned spec locally,
//! reads prior applied state via the broker, makes a local
//! skip/fresh/replace decision, and (when not Skip) enqueues an
//! `Apply` lifecycle command via `apply_via_supervisor`. The tenant's
//! supervisor pod handles the kubectl work. Once apply completes,
//! the loop driver runs `node.execute` with `Phase::InfraSetup`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use serde_json::Value;
use tokio::sync::mpsc;

use weft_core::bus::{
    BusEntry, BusEntryKind, BusHandle, BusInner, BusLiveness, BusOptions, BusParticipant,
    BusRegistry,
};
use weft_core::cancellation::CancellationFlag;
use weft_core::context::{ContextHandle, LogLevel};
use weft_core::error::{WeftError, WeftResult};
use weft_core::node::NodeOutput;
use weft_core::primitive::{CostReport, SignalSpec};
use weft_core::weft_type::WeftType;
use weft_core::Color;

use crate::now_unix;
use weft_infra::InfraReader;
use weft_journal::{ExecEvent, JournalClient};

use weft_task_store::tasks as task_store;
use weft_task_store::{TaskKind, TaskStoreClient};

/// Serialize a frame stack into the canonical string used in task dedup keys.
/// One definition so the side-effect-task and register-signal-task
/// dedup keys can't drift, and so a serialization failure is surfaced
/// (a swallowed `unwrap_or_default()` would collapse distinct firings to
/// the same empty key and silently drop a task). Frame-stack
/// serialization shouldn't fail in practice, which is exactly why a
/// failure must be loud rather than masked.
fn frames_dedup_key(frames: &weft_core::frames::LoopFrames) -> Result<String, serde_json::Error> {
    serde_json::to_string(frames)
}

/// Bundle of broker-backed clients the engine threads everywhere.
/// Each handle clones cheaply (every field is `Arc<dyn _>`).
#[derive(Clone)]
pub struct EngineClients {
    pub journal: Arc<dyn JournalClient>,
    pub tasks: Arc<dyn TaskStoreClient>,
    pub infra: Arc<dyn InfraReader>,
    /// Broker client for infra applied-state reads + apply enqueue.
    /// Used by the loop driver during `Phase::InfraSetup` to make the
    /// skip/fresh/replace decision locally, then ship the spec to the
    /// supervisor via the `infra_lifecycle_command` table.
    pub infra_state: Arc<dyn InfraStateClient>,
    /// Broker client for fetching the project's `ProjectDefinition`
    /// at execution claim time. The worker no longer carries the
    /// definition baked into the binary; it asks the broker, keyed
    /// by `(project_id, definition_hash)` (hash from the task
    /// payload). Same call per execution; the worker pod caches
    /// across executions by hash so consecutive claims of the same
    /// shape pay only one round trip per shape.
    pub project: Arc<dyn ProjectClient>,
    /// Clock the engine uses for every time-related decision
    /// (deadlines, polling intervals). Production passes the
    /// real clock; layer-3 tests pass `FakeClock` so deadlines
    /// can be exercised without burning real wall-clock seconds.
    pub clock: Arc<dyn weft_platform_traits::Clock>,
    /// Worker-side storage data path (`ctx.storage(...)`): the
    /// lazily-ensured box endpoint + ensure-then-retry policy.
    /// Production: `crate::storage::WorkerStorage`; tests inject a
    /// fake.
    pub storage: Arc<dyn crate::storage::WorkerStorageOps>,
}

/// Trait surface over `BrokerProjectClient` so tests can inject a
/// hand-rolled fake. Production has one impl: the broker-backed HTTP
/// client. The trait owns the hash-gated fetch contract so the
/// engine doesn't need to know about HTTP statuses.
#[async_trait]
pub trait ProjectClient: Send + Sync {
    /// Fetch the project's `ProjectDefinition` keyed by
    /// `expected_hash`. Returns `Some(def)` on hit (the
    /// `(project_id, hash)` row exists in the broker's
    /// `project_definition` history), `None` on miss (no row for
    /// that hash, a real "not found"). Every other failure
    /// (transport, parse) is an `Err`. There is no "raced" case for
    /// this endpoint: the history table is append-only, so a hash
    /// either has a row or it doesn't.
    async fn fetch_definition(
        &self,
        project_id: &str,
        expected_hash: &str,
    ) -> anyhow::Result<Option<weft_core::ProjectDefinition>>;
}

#[async_trait]
impl ProjectClient for weft_broker_client::BrokerProjectClient {
    async fn fetch_definition(
        &self,
        project_id: &str,
        expected_hash: &str,
    ) -> anyhow::Result<Option<weft_core::ProjectDefinition>> {
        // Inherent method on the concrete type, called via
        // <BrokerProjectClient>::fetch_definition to disambiguate
        // from the trait method we're implementing.
        let resp = <weft_broker_client::BrokerProjectClient>::fetch_definition(
            self,
            project_id,
            expected_hash,
        )
        .await?;
        let Some(r) = resp else { return Ok(None); };
        let def: weft_core::ProjectDefinition = serde_json::from_str(&r.project_json)
            .map_err(|e| anyhow::anyhow!("parse project_json: {e}"))?;
        Ok(Some(def))
    }
}

/// Trait surface over `BrokerInfraStateClient` so tests can inject a
/// no-op (or recording) implementation. Production has one impl: the
/// broker-backed HTTP client.
///
/// The trait has two operations: `enqueue_apply` (ship a fresh spec
/// to the supervisor) and `wait_apply` (poll the resulting command
/// row to terminal). The supervisor owns every other concern
/// end-to-end: read prior `infra_node`, compile + hash, decide
/// skip / fresh / replace, run kubectl, update the row. The worker
/// just hands off the spec and waits.
#[async_trait]
pub trait InfraStateClient: Send + Sync {
    async fn enqueue_apply(
        &self,
        project_id: &str,
        node_id: &str,
        spec_json: serde_json::Value,
    ) -> anyhow::Result<i64>;

    async fn wait_apply(
        &self,
        project_id: &str,
        command_id: i64,
    ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse>;
}

#[async_trait]
impl InfraStateClient for weft_broker_client::client::BrokerInfraStateClient {
    async fn enqueue_apply(
        &self,
        project_id: &str,
        node_id: &str,
        spec_json: serde_json::Value,
    ) -> anyhow::Result<i64> {
        self.enqueue_apply(project_id, node_id, spec_json).await
    }
    async fn wait_apply(
        &self,
        project_id: &str,
        command_id: i64,
    ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
        self.wait_apply(project_id, command_id).await
    }
}

/// Per-execution bus state: the registry that resolves markers to live
/// channels, plus the list of buses the engine knows about. The dead-
/// end policy is one line: if the loop is stuck (drained, no waiters,
/// tasks still alive) AND any bus is live, close every bus. Each
/// closed bus wakes its parked cursors and waits with Closed / None;
/// the node tasks unwind, the loop drains them and terminates.
///
/// Vocabulary: this code uses "wait" everywhere, not "park". The word
/// "park" already names `ctx.await_signal` (journal-replay workflow
/// suspension, worker swap). A bus cursor's `next().await` is plain
/// in-process tokio await; the worker stays alive; no swap. The
/// engine's stuck-detector still needs the wake-up so it can know
/// "every in-flight task is blocked on a bus right now" and close.
pub struct BusCoordinator {
    /// Single source of truth for every bus minted this execution.
    /// Holds the strong `Arc<BusInner>` per bus (so the pump's Weak
    /// always upgrades and the bus stays pinned long enough to be
    /// drained); doubles as the marker-lookup table. Released by
    /// `shutdown()` AFTER the final drain has been acked.
    registry: BusRegistry,
    /// Wake the loop's idle-`select!` when a node's bus state changes
    /// (enters/leaves a wait, observes/parks, appends; participant
    /// register/drop arrive as appends). The loop doesn't care WHO
    /// changed; it just needs to re-check stuck.
    bus_wait_notify: tokio::sync::Notify,
    /// Per-NODE-EXECUTION bus liveness, keyed by `(node_id, frames)`.
    /// Each entry holds the node's currently-live bus waits (keyed by
    /// `WaitId`, because one task can `select!`/`join!` over several at
    /// once). A node execution in a parallel loop has one entry per lane
    /// (distinct frames), so lanes never conflate. Read by
    /// `deadlock_provable`: the buses are stuck only when every in-flight
    /// node task is parked here with EVERY one of its waits at its true
    /// `notified.await` AND caught up on that wait's bus's CURRENT append
    /// generation. A node that is computing (no map entry, or an entry
    /// with any wait not parked) keeps the bus alive: it might still
    /// send. A node woken by a send but still unpolled has not re-observed
    /// since the append, so that wait reads as behind; a wait mid-
    /// evaluation reads as not-parked. Either way the close is suppressed
    /// under it, by construction rather than by a scheduler-fairness bet.
    nodes: std::sync::Mutex<std::collections::HashMap<BusParticipant, NodeBusState>>,
    /// Mint for `WaitId`s. Plain monotone counter; ids are never reused
    /// within an execution, so a stale `exit_wait` can never address a
    /// later wait.
    next_wait_id: std::sync::atomic::AtomicU64,
    /// Per-execution journal-pump wake. Buses signal this after every
    /// append (via `Weak<Notify>` they hold). The pump task awaits it
    /// and drains every live bus's unjournaled tail.
    journal_pump_notify: Arc<tokio::sync::Notify>,
    /// Fired by the pump after every drain pass (whether it
    /// successfully journaled entries or not). `shutdown` awaits this
    /// in a loop checking "every bus is drained" so the wait doesn't
    /// poll.
    drain_complete_notify: Arc<tokio::sync::Notify>,
    /// Set to true by `shutdown` AFTER the final drain pass has been
    /// acked. The pump checks this every iteration; once true, the
    /// pump runs one more drain (to capture anything that landed
    /// between the shutdown check and the flag being set, which can't
    /// happen by construction but the extra pass is cheap and honest)
    /// and exits. Replaces a `Weak<BusCoordinator>::upgrade()`-vs-
    /// `drop(coordinator)` race that could leave the pump parked
    /// forever on a multi-thread runtime.
    pump_should_exit: std::sync::atomic::AtomicBool,
}

impl BusCoordinator {
    /// Construct the per-execution coordinator. The pump task is spun
    /// up by the loop driver, not here, because the loop owns the
    /// journal client.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            registry: BusRegistry::new(),
            bus_wait_notify: tokio::sync::Notify::new(),
            nodes: std::sync::Mutex::new(std::collections::HashMap::new()),
            next_wait_id: std::sync::atomic::AtomicU64::new(0),
            journal_pump_notify: Arc::new(tokio::sync::Notify::new()),
            drain_complete_notify: Arc::new(tokio::sync::Notify::new()),
            pump_should_exit: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Handle on the per-execution drain-complete notify. Cloned by
    /// the pump to fire after every drain pass.
    pub fn drain_complete_notify(&self) -> Arc<tokio::sync::Notify> {
        self.drain_complete_notify.clone()
    }

    /// How many node executions are currently fully parked (every one of
    /// their concurrent bus waits at its true park point). Logging /
    /// diagnostics only: the stuck-check reads the count inside
    /// `deadlock_provable`'s locked snapshot, never through this.
    pub fn parked_nodes_count(&self) -> usize {
        self.lock_nodes()
            .values()
            .filter(|s| Self::node_fully_parked(s))
            .count()
    }

    /// A node counts as parked for the deadlock check only when it holds
    /// at least one wait AND EVERY one of its concurrent waits is at its
    /// true park point. A task `select!`ing over two cursors with one
    /// branch still mid-evaluation (or not yet parked) is still working,
    /// so it must not count. (`waits` is never empty for a live entry:
    /// the entry is removed when its last wait exits.)
    fn node_fully_parked(state: &NodeBusState) -> bool {
        !state.waits.is_empty() && state.waits.values().all(|w| w.parked)
    }

    /// Test accessor: how many node executions currently have a liveness
    /// entry (>= 1 live wait). Lets tests assert the map shape directly
    /// (e.g. two registrations from one node collapse to one entry).
    #[cfg(test)]
    pub fn nodes_len(&self) -> usize {
        self.lock_nodes().len()
    }

    /// Lock the liveness map. Taken only on the wait / wake / join /
    /// leave / stuck-check paths, never on the message send path (an
    /// append only bumps its bus's append-generation atomic and wakes
    /// the loop).
    fn lock_nodes(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<BusParticipant, NodeBusState>> {
        self.nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// True when EVERY one of the `in_flight` live node tasks is a node
    /// execution PARKED at its true `notified.await` point (not mid-
    /// evaluation) AND has observed its bus's CURRENT append generation
    /// at its last condition evaluation. `in_flight` is the driver's
    /// live-task count; the close fires only when the count of parked-
    /// and-caught-up nodes equals it, i.e. no live task is off computing
    /// (a computing task, or one reading a bus it never registered on,
    /// might still send and unblock a peer). A node holding several bus
    /// registrations is ONE task and ONE map entry, so it counts once:
    /// this is the whole point of keying liveness on the node execution
    /// rather than on each transient wait.
    ///
    /// - A node behind its bus's generation has an append it has not
    ///   evaluated yet (typically a woken-but-unpolled receiver sitting
    ///   in another worker thread's queue), so it is alive.
    /// - A NOT-parked node is mid-evaluation: its `observed` fired, the
    ///   evaluation has not returned, and it may be about to RESOLVE
    ///   (find a message), not park. It is excluded from the parked
    ///   count, so the count cannot reach `in_flight` under it. Closing
    ///   under a succeeding evaluation would make its follow-up send hit
    ///   `SendError::Closed` and kill a live conversation. `parked`
    ///   fires only after every pre-park re-check failed, so `parked &&
    ///   caught-up` means a provably fruitless evaluation followed by a
    ///   real park.
    ///
    /// Per-bus generations are read UNDER each bus's log lock
    /// (`append_gen_settled`): `push_entry` pushes the entry BEFORE
    /// bumping the generation (both under the log lock), so an unlocked
    /// read could see generation G while an entry past G is already in
    /// the log, a torn read that could enable a close with an unconsumed
    /// message in flight. The parked nodes' buses are snapshotted out of
    /// the liveness map FIRST and the map lock released BEFORE any log
    /// lock is taken, so this scan nests no locks. (No path takes a log
    /// lock then the map lock either: `push_entry` runs under the log
    /// lock but `on_append` only fires a `Notify`; a `WaitGuard` drop
    /// runs `exit_wait` with no log lock held, since the cursor's inner
    /// `log` guard drops before the outer guard. So the lock graph is
    /// acyclic in both directions.)
    ///
    /// Soundness: an append landing between the map snapshot and a bus's
    /// generation read makes that bus's settled generation exceed its
    /// node's observed value, so the scan returns false. An append
    /// cannot land AFTER a successful scan either: every sender is a
    /// node task, a successful scan proved every in-flight task is a
    /// parked node, and a parked node only wakes on an append, so there
    /// is no task left that could send. A `true` here is therefore a
    /// stable fact, not a racy snapshot.
    pub fn deadlock_provable(&self, in_flight: usize) -> bool {
        // Phase 1: snapshot every FULLY-PARKED node's waits (each wait's
        // observed generation + bus) under ONE map lock. A node that is
        // not fully parked (computing, or any wait mid-evaluation / not
        // yet parked) is absent from the snapshot, so
        // `parked nodes == in_flight` can only hold when no live task is
        // off computing. Counting parked NODES (not waits) keeps one node
        // = one in-flight task even when that node holds several
        // concurrent waits. Done under one lock so the count and the
        // per-node state come from the same instant.
        let mut parked_node_count = 0usize;
        let snapshot: Vec<(u64, std::sync::Weak<BusInner>)> = {
            let nodes = self.lock_nodes();
            let mut v = Vec::new();
            for state in nodes.values() {
                if Self::node_fully_parked(state) {
                    parked_node_count += 1;
                    for w in state.waits.values() {
                        v.push((w.observed, w.bus.clone()));
                    }
                }
            }
            // Every in-flight task must be one of these fully-parked
            // nodes. A computing task (no parked entry) makes the count
            // fall short, so a busy peer keeps the bus alive.
            if parked_node_count != in_flight {
                return false;
            }
            v
        };
        // Phase 2: map lock released; read each wait's bus's settled
        // generation under that bus's log lock. EVERY wait of every
        // parked node must be caught up: a node parked on two buses is
        // only deadlocked if neither bus has an unconsumed append.
        snapshot.into_iter().all(|(observed, bus)| {
            match bus.upgrade() {
                Some(bus) => observed >= bus.append_gen_settled(),
                // The wait's bus dropped between the snapshot and here
                // (it left the wait concurrently and the execution let
                // the bus go). State is in motion, so suppress the
                // close; the exit's `exit_wait` woke the loop for a
                // clean re-evaluation.
                None => false,
            }
        })
    }

    /// Mint a fresh bus with the provided options and register it,
    /// attributed to the minting node execution `node` (whose identity
    /// keys its bus liveness). The registry pins the `Arc<BusInner>` (so
    /// the pump's `Weak` always upgrades while the execution is live),
    /// and the bus's engine hooks (liveness + journal-pump notify) are
    /// `Weak` back to this coordinator. `Weak` on the engine side lets
    /// the coordinator drop naturally at execution end; once gone, the
    /// bus's hooks no-op. Errors on an invalid `window`.
    pub fn new_bus(
        self: &Arc<Self>,
        opts: BusOptions,
        node: BusParticipant,
    ) -> Result<BusHandle, &'static str> {
        let weak = Arc::downgrade(self) as std::sync::Weak<dyn BusLiveness>;
        let pump_notify_weak = Arc::downgrade(&self.journal_pump_notify);
        let bus = BusHandle::create_with_engine(opts, weak, pump_notify_weak, Some(node))?;
        self.registry.insert(&bus);
        Ok(bus)
    }

    /// Wait-wake-up future the loop awaits in its idle-`select!`.
    pub fn wait_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.bus_wait_notify.notified()
    }

    /// Handle on the per-execution journal-pump wake-up. The pump task
    /// awaits this notify; buses fire it after every append (via
    /// their `Weak<Notify>`).
    pub fn journal_pump_notify(&self) -> Arc<tokio::sync::Notify> {
        self.journal_pump_notify.clone()
    }

    /// Whether the pump should exit on its next iteration. Set by
    /// `shutdown()` after every entry has been drained; the pump
    /// reads this every iteration and exits cleanly once true.
    pub fn pump_should_exit(&self) -> bool {
        self.pump_should_exit.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Snapshot every currently-live bus's `Weak<BusInner>` so the
    /// pump can drain unjournaled tails without holding the registry
    /// lock for the duration of the journal write.
    pub fn live_bus_inners(&self) -> Vec<std::sync::Weak<BusInner>> {
        self.registry.live_bus_weaks()
    }

    /// Look up a Bus marker JSON value in this execution's registry,
    /// attributed to the resolving node execution `node` (whose identity
    /// keys its own bus liveness).
    pub fn lookup_bus(
        &self,
        marker: &serde_json::Value,
        node: BusParticipant,
    ) -> Result<BusHandle, weft_core::bus::BusLookupError> {
        self.registry.lookup(marker, Some(node))
    }

    /// Whether any bus is live AND not yet closed. The loop reads this
    /// when it decides it is stuck: if true, the stuck must be because
    /// of a bus, so close them.
    pub fn has_live_buses(&self) -> bool {
        self.registry.live_bus_arcs().iter().any(|b| !b.is_closed())
    }

    /// Close every live bus (appends a `Closed` log entry to each).
    /// Does NOT release the registry's `Arc<BusInner>` refs: the pump
    /// still needs to journal the `Closed` entries.
    pub fn close_all(&self) {
        for b in self.registry.live_bus_arcs() {
            b.close();
        }
        self.journal_pump_notify.notify_waiters();
    }

    /// True when the pump has journaled every entry on every live
    /// bus. A `journal_degraded` bus is treated as drained: the pump
    /// can't drain it by definition, the node author already saw the
    /// failure as `SendError::JournalDegraded`, and waiting for it
    /// would just chew through the shutdown deadline before the same
    /// loud panic fires.
    fn fully_drained(&self) -> bool {
        self.registry.live_bus_arcs().iter().all(|b| {
            b.is_journal_degraded() || b.journaled_through() >= b.log_len()
        })
    }

    /// Panic with a diagnostic listing every bus still draining.
    /// Single source of truth for the deadline-miss message so the
    /// "immediate zero-remaining" branch and the "timeout fired"
    /// branch report the same thing.
    fn panic_shutdown_deadline_miss(&self, deadline: std::time::Duration) -> ! {
        let stuck = self
            .registry
            .live_bus_arcs()
            .iter()
            .filter(|b| !b.is_journal_degraded() && b.journaled_through() < b.log_len())
            .map(|b| {
                format!(
                    "bus {} ({} entries unjournaled)",
                    b.id(),
                    b.log_len() - b.journaled_through()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        panic!(
            "bus pump did not drain within {}ms; journal writes are stuck or too slow. \
             Buses still draining: {}",
            deadline.as_millis(),
            if stuck.is_empty() { "<none>".to_string() } else { stuck },
        );
    }

    /// Run the full bus shutdown sequence:
    ///   1. Append a `Closed` entry to every live bus AND wake the
    ///      pump.
    ///   2. Wait (notify-driven, no polling) for the pump to journal
    ///      every entry on every bus, bounded by `deadline`.
    ///   3. Release the `Arc<BusInner>` refs the coordinator pins, so
    ///      buses whose only other Arc is gone free immediately.
    ///   4. Set the pump's exit sentinel AND wake it. The pump's next
    ///      iteration reads the flag and returns. This is the
    ///      deterministic exit path; the prior shape (rely on
    ///      `Weak::upgrade` failing after the caller drops the
    ///      coordinator) had a race where the pump could win the
    ///      upgrade, finish an empty drain, and re-park on a notify
    ///      no one will fire again.
    ///
    /// Panics on deadline-miss: a wedged journal client means replay
    /// is permanently incomplete, and the worker should crash so the
    /// dispatcher surfaces the failure rather than swallowing it.
    pub async fn shutdown(&self, deadline: std::time::Duration) {
        self.close_all();
        let drain = self.drain_complete_notify.clone();
        let start = std::time::Instant::now();
        loop {
            // Arm the wake BEFORE the predicate check so a drain pass
            // that fires between the check and the await is not lost.
            let notified = drain.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.fully_drained() {
                break;
            }
            let remaining = deadline.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                self.panic_shutdown_deadline_miss(deadline);
            }
            match tokio::time::timeout(remaining, notified.as_mut()).await {
                Ok(_) => continue,
                Err(_) => {
                    if self.fully_drained() {
                        break;
                    }
                    self.panic_shutdown_deadline_miss(deadline);
                }
            }
        }
        self.registry.clear();
        // Tell the pump to exit on its next iteration, THEN wake it.
        // Setting the flag BEFORE the wake means the pump's loop body
        // reads `true` on the iteration triggered by this notify, even
        // if the pump had already passed its previous flag-check and
        // was parked on the notify. There is no upgrade race because
        // `pump_should_exit` is a plain AtomicBool we own.
        self.pump_should_exit
            .store(true, std::sync::atomic::Ordering::Release);
        self.journal_pump_notify.notify_waiters();
    }
}

impl Drop for BusCoordinator {
    /// Backstop for the case where the coordinator is dropped without
    /// `shutdown()` being called first (a panic unwind, a test that
    /// forgets to shut down). Two duties:
    ///
    /// 1. Close every live bus. Cursors parked on `next().await`
    ///    return `Ok(None)` on a closed bus, so the node tasks
    ///    holding them unblock and drop instead of staying parked
    ///    until their owning JoinSet is also dropped.
    /// 2. Set the pump exit flag and notify so the journal pump
    ///    wakes from `notified.await` and exits.
    ///
    /// In the normal path (`shutdown().await; drop(coord);`), the
    /// buses are already closed and the flag is already true, so the
    /// extra `close()` calls are idempotent no-ops.
    fn drop(&mut self) {
        for b in self.registry.live_bus_arcs() {
            b.close();
        }
        self.pump_should_exit
            .store(true, std::sync::atomic::Ordering::Release);
        self.journal_pump_notify.notify_waiters();
    }
}

/// One node execution's bus state in the coordinator's liveness map: the
/// set of bus waits this node is currently inside, keyed by `WaitId`. A
/// node is one async task but CAN hold several concurrent waits (a body
/// that `select!`s or `join!`s over two cursors), so `waits` is a map,
/// not a single slot. An entry exists exactly while the node has >= 1
/// live wait; it is removed when its last wait exits. The node counts as
/// "parked" for the deadlock check only when EVERY wait in `waits` is
/// parked-and-caught-up (a task with any branch still live or mid-
/// evaluation is still working). Plain fields: every read and write
/// happens under the liveness mutex.
struct NodeBusState {
    waits: std::collections::HashMap<weft_core::bus::WaitId, WaitState>,
}

/// One bus wait. `bus` is the bus it is parked on (so the stuck-check can
/// read that bus's append generation UNDER its log lock, see
/// `deadlock_provable`); `observed` is the highest generation seen when
/// this wait last (re-)evaluated its condition (`observed` hook); `parked`
/// is whether it is at its true park point (`parked` hook; cleared by
/// every `observed` because an evaluation in progress may resolve instead
/// of parking). Lives from `enter_wait` to `exit_wait` (RAII via
/// `WaitGuard` in weft-core).
struct WaitState {
    bus: std::sync::Weak<BusInner>,
    observed: u64,
    parked: bool,
}

impl BusLiveness for BusCoordinator {
    fn enter_wait(
        &self,
        node: &BusParticipant,
        bus: &std::sync::Arc<BusInner>,
    ) -> weft_core::bus::WaitId {
        let id = self
            .next_wait_id
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        // `observed: 0` registers the wait as conservatively BEHIND and
        // `parked: false` as conservatively MID-EVALUATION (both suppress
        // close) until its first `observed` / `parked`, which the wait
        // loops fire before their first park by construction.
        self.lock_nodes()
            .entry(node.clone())
            .or_insert_with(|| NodeBusState { waits: std::collections::HashMap::new() })
            .waits
            .insert(
                id,
                WaitState {
                    bus: std::sync::Arc::downgrade(bus),
                    observed: 0,
                    parked: false,
                },
            );
        // Use `notify_one` (NOT `notify_waiters`) so a wait-start that
        // fires when the loop is NOT currently parked on `wait_notified`
        // stores a permit. The next `wait_notified().enable()` consumes
        // it and the loop wakes immediately. With `notify_waiters` the
        // notification was lost in that window, and if no further
        // wait-start arrived (because all peers are now blocked), the
        // loop slept until the harness 10s deadline. Test:
        // `hole4_mutual_deadlock_when_both_wait_for_names_that_never_come`
        // flaked ~1 in 10 under parallel load before this change.
        self.bus_wait_notify.notify_one();
        id
    }
    fn exit_wait(&self, node: &BusParticipant, id: weft_core::bus::WaitId) {
        // The `WaitGuard` lifecycle is symmetric (started on construct,
        // ended on drop), so a missing wait here is a real pairing bug.
        // Crash loud in BOTH dev and release: a silently-cleared wait
        // would flip the stuck-check (an unparked node suppresses close)
        // wrongly and the engine could hang or close early.
        //
        // CRITICAL: if this node has OTHER waits still live, the task is
        // provably RUNNING right now (it is executing this guard drop, on
        // its way to resolve a `select!`/`join!` branch and run that
        // branch's code). Its sibling waits' `parked` flags are now stale
        // (set before the task woke), and a stuck-check that read them
        // would see the node as fully parked and could close buses out
        // from under the resolving branch's follow-up send. So clear
        // every surviving wait's `parked` flag (the node is not parked
        // until every wait re-parks) AND wake each sibling's bus so its
        // wait loop re-runs `observed` -> re-check -> `parked` and
        // restores a truthful flag. Without the wake, a `join!` sibling
        // that stays genuinely parked on `notified.await` would never
        // re-run and the cleared flag would suppress a real deadlock
        // forever (a hang). The buses are collected under the lock and
        // woken AFTER releasing it, so no foreign code (even a future
        // `wake_waiters` that grows a locked re-check) ever runs under
        // the liveness map lock.
        let mut wake: Vec<std::sync::Arc<BusInner>> = Vec::new();
        {
            let mut nodes = self.lock_nodes();
            let entry = nodes
                .get_mut(node)
                .expect("exit_wait for unknown node: WaitGuard pairing broken");
            let removed = entry.waits.remove(&id);
            assert!(
                removed.is_some(),
                "exit_wait for unknown wait id {id}: WaitGuard pairing broken"
            );
            if entry.waits.is_empty() {
                nodes.remove(node);
            } else {
                for w in entry.waits.values_mut() {
                    w.parked = false;
                    // A sibling present here has a live wait, and the wait
                    // loops hold an `Arc<BusInner>` across the whole wait,
                    // so this upgrade CANNOT fail. Crash loud if it does:
                    // a cleared-but-unwoken sibling would suppress a real
                    // deadlock forever (the hang this clear+wake prevents).
                    wake.push(w.bus.upgrade().expect(
                        "bus dropped while a sibling wait is live: the wait \
                         loops hold an Arc<BusInner> across the whole wait",
                    ));
                }
            }
        }
        for bus in wake {
            bus.wake_waiters();
        }
        self.bus_wait_notify.notify_one();
    }
    fn on_append(&self) {
        // A bus appended (send/register/close/drop). Wake the idle
        // `select!` so the loop re-evaluates promptly (a stored permit if
        // the loop is not currently parked, same discipline as
        // `enter_wait`). The append itself is visible to the stuck-check
        // through each bus's `append_gen_settled` (a parked wait that
        // has not observed the new generation reads as behind / alive),
        // so no separate generation counter is needed on the coordinator.
        self.bus_wait_notify.notify_one();
    }
    fn observed(&self, node: &BusParticipant, id: weft_core::bus::WaitId) {
        let mut nodes = self.lock_nodes();
        let wait = nodes
            .get_mut(node)
            .and_then(|e| e.waits.get_mut(&id))
            .expect("observed for unknown wait: WaitGuard pairing broken");
        // Read the generation while waiting: the value is the
        // ground-truth "everything appended up to here will be seen by
        // the condition evaluation the caller runs next" (see the
        // `BusLiveness::observed` contract in weft-core). The unlocked
        // read (`append_gen_now`) is sound HERE because a lagging value
        // only makes the wait read as behind, which is conservative; the
        // close decision itself re-reads under the log lock
        // (`deadlock_provable`).
        wait.observed = wait
            .bus
            .upgrade()
            .expect(
                "bus dropped while a node is waiting on it: the wait \
                 loops hold an Arc<BusInner> across the whole wait",
            )
            .append_gen_now();
        // An evaluation is now in progress; it may RESOLVE rather than
        // park. Mark not-parked so the stuck-check cannot close the
        // buses out from under a succeeding evaluation (the resolved
        // node's follow-up send would hit SendError::Closed).
        wait.parked = false;
        drop(nodes);
        // A wait catching up can flip `deadlock_provable` to true; wake
        // the loop so a deadlock that just became provable closes
        // without waiting for an unrelated event.
        self.bus_wait_notify.notify_one();
    }
    fn parked(&self, node: &BusParticipant, id: weft_core::bus::WaitId) {
        let mut nodes = self.lock_nodes();
        let wait = nodes
            .get_mut(node)
            .and_then(|e| e.waits.get_mut(&id))
            .expect("parked for unknown wait: WaitGuard pairing broken");
        wait.parked = true;
        drop(nodes);
        // The last wait parking can flip `deadlock_provable` to true (it
        // is the final event before a deadlock is provable); wake the
        // loop so the close fires without waiting for an unrelated event.
        self.bus_wait_notify.notify_one();
    }
}

/// Bus-journal pump. One task per execution, spawned by the loop
/// driver before the first node dispatches. Awaits the per-execution
/// notify; on every wake, walks every live bus, drains its
/// unjournaled tail, ships the entries to the journal, and ack-bumps
/// the per-bus `journaled_through` cursor.
///
/// Failure handling: a journal write error sets the bus's
/// `journal_degraded` flag (the NEXT `send` returns
/// `SendError::JournalDegraded(reason)`). The execution keeps running;
/// the in-RAM bus log is unaffected; the inspector's replay tail is
/// truncated for the lost range.
///
/// Lifecycle: `BusCoordinator::shutdown()` sets the
/// `pump_should_exit` flag AFTER closing every bus and waiting for
/// the pump to drain the close entries, then wakes the pump. The
/// pump's next iteration reads the flag and returns. The flag is a
/// plain AtomicBool the coordinator owns: no race between the pump's
/// "did the coordinator drop?" check and the caller's drop, which
/// the prior `Weak::upgrade()` shape had.
pub async fn run_bus_journal_task(
    coordinator: std::sync::Weak<BusCoordinator>,
    color: Color,
    journal: Arc<dyn JournalClient>,
    pod_name: String,
) {
    // Hold owned Arcs on both per-execution notifies so we keep
    // operating even if `coordinator.upgrade()` starts returning None
    // for a tick (a torn-down execution that lost its Arc but still
    // has buses we need to drain).
    let (pump_wake, drain_done) = match coordinator.upgrade() {
        Some(c) => (c.journal_pump_notify(), c.drain_complete_notify()),
        None => return,
    };
    let mut known_buses: Vec<std::sync::Weak<BusInner>> = Vec::new();
    loop {
        let notified = pump_wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        // Exit check happens INSIDE the loop body after a wake. The
        // shutdown sequence sets `pump_should_exit` BEFORE waking us,
        // so the flag we read here on a shutdown-triggered wake is
        // already true. We still run one final drain pass first so
        // any entries that landed concurrent with the shutdown flag
        // being set are journaled.
        let should_exit = match coordinator.upgrade() {
            Some(coord) => {
                known_buses = coord.live_bus_inners();
                coord.pump_should_exit()
            }
            // Coordinator dropped before shutdown could set the flag.
            // Treat as "exit": there's nothing live to drain and no
            // one will fire pump_wake again.
            None => true,
        };
        drain_buses(&known_buses, color, journal.as_ref(), &pod_name).await;
        drain_done.notify_waiters();
        if should_exit {
            return;
        }
        notified.await;
    }
}

/// One drain pass across every bus the pump knows about. Per bus:
/// snapshot the unjournaled tail, write events, acknowledge the new
/// `journaled_through`. On write failure: mark the bus as degraded
/// and skip ack so the next pass retries the same tail. Takes Weak
/// refs so the pump can run a final drain after the coordinator has
/// dropped (the buses' Arcs may still be live via close-tokens or
/// participant handles).
async fn drain_buses(
    buses: &[std::sync::Weak<BusInner>],
    color: Color,
    journal: &dyn JournalClient,
    pod_name: &str,
) {
    for weak in buses {
        let Some(inner) = weak.upgrade() else { continue };
        let bus_id_str = inner.id().to_string();
        let tail = inner.drain_journal_tail();
        if tail.is_empty() {
            continue;
        }
        for entry in tail {
            let ev = bus_entry_to_event(color, &bus_id_str, &entry);
            if let Err(e) = journal.record_event(&ev, Some(pod_name)).await {
                tracing::error!(
                    target: "weft_engine::bus",
                    bus = %bus_id_str,
                    offset = entry.offset,
                    error = %e,
                    "bus journal pump failed; marking bus degraded"
                );
                inner.mark_journal_degraded(format!("journal write failed: {e}"));
                break;
            }
            // Ack each entry as it lands so a mid-batch failure
            // retries ONLY the unwritten suffix: acking nothing until
            // the whole batch succeeds would make the next pass
            // re-write the already-journaled prefix (`record_event`
            // has no dedup on this path), duplicating bus messages in
            // the replay.
            inner.acknowledge_journaled_through(entry.offset + 1);
        }
    }
}

/// Project one in-RAM `BusEntry` to its `ExecEvent` shape.
fn bus_entry_to_event(color: Color, bus_id: &str, entry: &BusEntry) -> ExecEvent {
    let at_unix = entry.at_unix;
    let offset = entry.offset;
    match &entry.kind {
        BusEntryKind::Joined { name } => ExecEvent::BusJoined {
            color,
            bus_id: bus_id.to_string(),
            offset,
            name: name.clone(),
            at_unix,
        },
        BusEntryKind::Left { name } => ExecEvent::BusLeft {
            color,
            bus_id: bus_id.to_string(),
            offset,
            name: name.clone(),
            at_unix,
        },
        BusEntryKind::Message {
            from,
            msg_kind,
            payload,
            payload_byte_size,
            payload_sha256_prefix,
        } => ExecEvent::BusMessage {
            color,
            bus_id: bus_id.to_string(),
            offset,
            from: from.clone(),
            msg_kind: msg_kind.clone(),
            // In-RAM `payload: Option<Value>` is `None` only for
            // ephemeral buses (per `BusEntryKind::Message`'s contract);
            // tag the journal event so `Some(Value::Null)` (a journaled
            // bus where the body sent JSON null) stays distinguishable
            // from `None` (no journaled payload at all) across the
            // serde round-trip.
            payload: match payload {
                Some(v) => weft_core::primitive::JournaledPayload::Journaled { value: v.clone() },
                None => weft_core::primitive::JournaledPayload::Ephemeral,
            },
            payload_byte_size: *payload_byte_size,
            payload_sha256_prefix: *payload_sha256_prefix,
            at_unix,
        },
        BusEntryKind::Closed => ExecEvent::BusClosed {
            color,
            bus_id: bus_id.to_string(),
            offset,
            at_unix,
        },
    }
}

/// An emission a node made via `ctx.pulse_downstream` or
/// `ctx.close_port`, sent from the node task to the loop driver. The
/// loop turns it into downstream pulses at the firing's frame stack. The
/// node task keeps running after sending. Bus values are plain JSON
/// markers inside the value variant; the live channels live in the
/// per-execution `BusRegistry` on `BusCoordinator` and never ride on
/// `EmitMsg`.
pub struct EmitMsg {
    pub node_id: String,
    pub frames: weft_core::frames::LoopFrames,
    pub kind: EmitKind,
}

/// The SINGLE message a node task sends to the loop driver, over ONE
/// ordered channel. A node emits zero or more `Emission`s while it runs
/// (each `pulse_downstream` / `close_port`), then sends exactly one
/// `Terminal` when `execute` returns. Putting both on one FIFO channel
/// is load-bearing: it guarantees the driver observes a node's emissions
/// BEFORE its terminal, so the close-unmentioned-ports sweep at the
/// terminal always sees the complete set of emitted ports. Two separate
/// channels left a window where the terminal could be read first and a
/// just-emitted port wrongly closed (skipping its consumer, then a
/// re-dispatch), an emit-then-immediately-return race.
pub enum TaskMsg {
    Emission(EmitMsg),
    Terminal {
        node_id: String,
        color: Color,
        frames: weft_core::frames::LoopFrames,
        outcome: NodeTaskOutcome,
    },
}

/// How a node's `execute` ended. The driver turns each into the
/// firing's terminal lifecycle event.
pub enum NodeTaskOutcome {
    /// `execute` returned `Ok(())`.
    Completed,
    Failed(String),
    /// The node called `await_signal` and is now waiting on a fired
    /// wake signal (carries the suspension token).
    Waiting(String),
}

/// What the emission carries. A node either ships values on N output
/// ports (`Values`) or closes a single port (`Close`). One enum so the
/// loop driver has one channel to drain and the per-firing
/// one-mention-per-port rule applies uniformly across both shapes.
pub enum EmitKind {
    /// A `pulse_downstream` call: emit values on every port in `output`.
    Values(NodeOutput),
    /// A `close_port` call: emit a CLOSURE on `port`. The downstream
    /// subgraph attached to that port at this frame stack learns nothing's
    /// coming, exactly the same shape as the termination-time sweep
    /// for an unmentioned port.
    Close(String),
}

/// Round-trip timeout for control-plane tasks. Generous because
/// some involve listener spawn + Pod readiness wait.
pub(crate) const TASK_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const TASK_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Journal write stamped with this Pod's name for fencing. Logs the
/// error instead of propagating (most call sites are teardown paths
/// that cannot fail), BUT the drive's journal client is wrapped in
/// `PoisonOnWriteFailure`, so the failure latches a flag the drive
/// loop checks every iteration: the worker exits instead of driving
/// on top of a journal that no longer matches its live state.
/// Does `declared` accept a value whose inferred type is `infer(value)`?
/// The runtime output-type gate: a node may only emit on a port a value
/// compatible with the port's declared type. An unresolved declared type
/// (TypeVar / MustOverride / unresolved) accepts anything, since
/// `is_compatible` short-circuits to true when either side is unresolved.
fn type_accepts(declared: &WeftType, value: &Value) -> bool {
    let inferred = WeftType::infer(value);
    WeftType::is_compatible(&inferred, declared)
}

pub async fn record_from_pod(journal: &dyn JournalClient, event: ExecEvent, pod_name: &str) {
    if let Err(e) = journal.record_event(&event, Some(pod_name)).await {
        tracing::error!(
            target: "weft_engine::journal",
            error = %e,
            "journal write failed; drive is now poisoned and the worker will exit"
        );
    }
}

/// Journal-client decorator that latches the first `record_event`
/// failure into a shared flag.
///
/// Lifecycle writes ship one by one (`record_from_pod`), so a FAILED
/// write means the journal is now a strict prefix of what the live
/// worker believes happened. Continuing to drive on that divergence
/// makes every later refold (stall refetch, crash resume) rebuild a
/// different world: a body whose `NodeStarted` was lost but whose
/// `PulseEmitted` rows landed re-runs and double-spends. The drive
/// loop checks the flag every iteration and exits the worker, so a
/// respawned worker refolds from the journal's consistent prefix
/// (re-running the lost suffix, which is the same at-least-once
/// semantics as a crash).
///
/// The bus pump deliberately keeps the UNwrapped client: bus rows are
/// the inspector's replay trail, and their failures already degrade
/// per-bus without killing the worker.
pub struct PoisonOnWriteFailure {
    inner: Arc<dyn JournalClient>,
    poisoned: Arc<std::sync::atomic::AtomicBool>,
}

impl PoisonOnWriteFailure {
    /// Wrap `inner`; returns the wrapped client and the shared flag.
    pub fn wrap(
        inner: Arc<dyn JournalClient>,
    ) -> (Arc<dyn JournalClient>, Arc<std::sync::atomic::AtomicBool>) {
        let poisoned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        (
            Arc::new(Self { inner, poisoned: poisoned.clone() }),
            poisoned,
        )
    }
}

#[async_trait::async_trait]
impl JournalClient for PoisonOnWriteFailure {
    async fn record_event(
        &self,
        event: &ExecEvent,
        pod_name: Option<&str>,
    ) -> anyhow::Result<()> {
        let r = self.inner.record_event(event, pod_name).await;
        if r.is_err() {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
        }
        r
    }

    async fn events_for_color(
        &self,
        color: Color,
    ) -> anyhow::Result<Vec<ExecEvent>> {
        self.inner.events_for_color(color).await
    }

    async fn has_terminal_event(&self, color: Color) -> anyhow::Result<bool> {
        self.inner.has_terminal_event(color).await
    }
}


pub struct RunnerHandle {
    execution_id: String,
    project_id: String,
    color: Color,
    node_id: String,
    node_frames: weft_core::frames::LoopFrames,
    /// Broker-backed clients: journal writes, task enqueue, and
    /// infra reads (infra endpoint lookup) all flow through here.
    clients: EngineClients,
    /// k8s Pod name stamped on every journal write so the fencing
    /// trigger can reject writes from a Pod that has been drained or
    /// reaped.
    pod_name: String,
    /// Tenant id stamped on every task this handle enqueues. The
    /// dispatcher's listener reaper queries the task table by tenant
    /// to tell "in-flight register" from "genuinely idle" before
    /// killing the per-tenant listener pod.
    tenant_id: String,
    cancellation: Arc<CancellationFlag>,
    /// Pre-loaded sequence of past `await_signal` calls for this
    /// (node, frames), seeded by the loop driver from the journal
    /// fold on every dispatch. Each `await_signal` call inside the
    /// node body pops the next entry:
    ///   - resolved=Some(value): return it instantly (replay path).
    ///   - resolved=None (the still-pending tail): suspend with
    ///     this token (we're being re-dispatched but our fire
    ///     hasn't arrived for THIS call yet).
    ///   - exhausted: this is a fresh await; enqueue register_signal
    ///     with the next call_index.
    /// The `Mutex` is just for interior mutability across the
    /// `&self` of the trait method; calls are serialized by the
    /// node body (single Future polling).
    awaited_sequence: Mutex<std::collections::VecDeque<weft_core::primitive::AwaitedEntry>>,
    /// 0-based ordinal of the NEXT `await_signal` call within this
    /// (node, frames) execution. Increments on every call. Combined
    /// with the per-(node, frames) sequence above, it determines
    /// whether the call replays or registers fresh.
    next_call_index: AtomicU32,
    /// 0-based ordinal of the NEXT side-effect call (`report_cost`,
    /// `log`) within this (node, frames). Separate counter from
    /// `next_call_index` so side effects don't shift the replay
    /// alignment of `await_signal` / `run_step`. Used to form
    /// stable dedup keys on the broker task table: under replay,
    /// the body re-runs and emits the same side-effect calls in
    /// the same order, so the dedup keys collapse to the same row.
    next_side_effect_index: AtomicU32,
    /// Number of `register_signal` (entry trigger) calls this node
    /// has made on this invocation. Entry triggers are a one-shot
    /// thing per node per TriggerSetup; we fail loudly on a second
    /// call rather than silently colliding on the dedup key.
    entry_register_count: AtomicU32,
    /// Channel to the loop driver for `pulse_downstream` emissions. The
    /// node task sends `TaskMsg::Emission`s here (the same channel its
    /// terminal `TaskMsg::Terminal` rides, so the loop sees emissions
    /// before the terminal); the loop applies them to the pulse table
    /// while the task keeps running. `None` only in unit tests that
    /// never emit.
    emit_tx: Option<mpsc::UnboundedSender<TaskMsg>>,
    /// Output ports this firing has already emitted on. A second
    /// `pulse_downstream` mentioning a port that's already in here is
    /// a node-author bug (each port can be emitted AT MOST ONCE per
    /// firing) and errors loud. Multiple `pulse_downstream` calls on
    /// DISJOINT ports are fine: that's the "release early then
    /// finalize" pattern (bus marker out, then `done` at the end).
    mentioned_outputs: Mutex<HashSet<String>>,
    /// Per-execution bus coordinator. Owns the `BusRegistry` (the source
    /// of truth for "which buses are live this execution") and the park
    /// counters the loop driver uses for dead-end detection.
    bus_coordinator: Arc<BusCoordinator>,
    /// Output ports this node declares in its metadata, name -> declared
    /// type. Used by `pulse_downstream` / `close_port` to reject emits on
    /// undeclared ports loudly at the API boundary (before the bad-shape
    /// value reaches the loop driver and silently routes through the
    /// (post)process layer's no-such-port fallthrough), AND to validate
    /// the TYPE of each emitted value against the port's declared type
    /// (an incompatible value is refused, the port closed, a
    /// `PortTypeMismatch` recorded).
    declared_outputs: HashMap<String, WeftType>,
    /// Wake event payload for this firing. `Some` only when this
    /// dispatch is the consumption of a `NodeKicked` for a firing
    /// trigger; `None` for every other dispatch (regular pulse-driven
    /// firings, manual-run roots without a payload, setup-phase runs).
    wake_payload: Option<Value>,
    /// Live caller connection for this EXECUTION. A cheap `Arc` clone of
    /// the one connection the loop driver holds for the color, threaded
    /// into every firing's handle so any node can reach the caller via
    /// `ctx.caller()`. `None` for durable runs and for any worker that
    /// did not receive a `live_connection` request. Per-execution, not
    /// per-firing: all firings of one color share the one caller.
    caller_connection: Option<Arc<dyn weft_core::caller::CallerConnection>>,
}

impl RunnerHandle {
    /// This firing's bus-liveness identity: the node id plus its loop
    /// frame stack. A loop running the same body N times in parallel has
    /// N distinct participants (one per frame), so their bus liveness
    /// never conflates.
    fn bus_participant(&self) -> BusParticipant {
        (self.node_id.clone(), self.node_frames.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: String,
        project_id: String,
        color: Color,
        node_id: String,
        node_frames: weft_core::frames::LoopFrames,
        clients: EngineClients,
        pod_name: String,
        tenant_id: String,
        cancellation: Arc<CancellationFlag>,
        bus_coordinator: Arc<BusCoordinator>,
        declared_outputs: HashMap<String, WeftType>,
    ) -> Self {
        Self {
            execution_id,
            project_id,
            color,
            node_id,
            node_frames,
            clients,
            pod_name,
            tenant_id,
            cancellation,
            awaited_sequence: Mutex::new(std::collections::VecDeque::new()),
            next_call_index: AtomicU32::new(0),
            next_side_effect_index: AtomicU32::new(0),
            entry_register_count: AtomicU32::new(0),
            emit_tx: None,
            mentioned_outputs: Mutex::new(HashSet::new()),
            bus_coordinator,
            declared_outputs,
            wake_payload: None,
            caller_connection: None,
        }
    }

    /// Wire the emission channel. Called by the loop driver before
    /// dispatching the node so `pulse_downstream` can reach the loop.
    pub fn with_emit_channel(
        mut self,
        emit_tx: mpsc::UnboundedSender<TaskMsg>,
    ) -> Self {
        self.emit_tx = Some(emit_tx);
        self
    }

    /// Wire the wake payload for this dispatch. Called by the loop
    /// driver only when consuming a `NodeKicked` for a firing trigger;
    /// every other dispatch leaves `wake_payload` as None.
    pub fn with_wake_payload(mut self, payload: Value) -> Self {
        self.wake_payload = Some(payload);
        self
    }

    /// Wire the execution's live caller connection. Called by the loop
    /// driver for every firing of a color that has a caller attached, so
    /// `ctx.caller()` resolves on any node. Left `None` otherwise.
    pub fn with_caller_connection(
        mut self,
        conn: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    ) -> Self {
        self.caller_connection = conn;
        self
    }

    fn next_side_effect_index(&self) -> u32 {
        self.next_side_effect_index.fetch_add(1, Ordering::SeqCst)
    }

    /// Enqueue a side-effect task (cost report, log line, etc.) on
    /// the broker. The broker's INSERT into `task` is the durable
    /// commit; once `Ok` returns, the dispatcher will journal the
    /// event regardless of whether this worker pod survives.
    ///
    /// `dedup_prefix` is the human-readable label used in the dedup
    /// key (`"report_cost"`, `"log"`, ...); the rest of the key
    /// keys to (color, node, frames, side-effect-index) so distinct
    /// calls in one node body produce distinct tasks while a retry
    /// of the same call (e.g. supervisor reconnect, body replay)
    /// collapses to one. Replay is honest because the body re-runs
    /// in the same order and emits the same side-effect sequence.
    async fn enqueue_side_effect_task<P: serde::Serialize>(
        &self,
        dedup_prefix: &str,
        kind: weft_task_store::TaskKind,
        payload: P,
    ) -> WeftResult<()> {
        let payload_json = serde_json::to_value(&payload).map_err(|e| {
            WeftError::Config(format!("{dedup_prefix} payload: {e}"))
        })?;
        let frames_key = frames_dedup_key(&self.node_frames)
            .map_err(|e| WeftError::Config(format!("{dedup_prefix} frames key: {e}")))?;
        let dedup_key = format!(
            "{dedup_prefix}:{color}:{node}:{frames_key}:{idx}",
            color = self.color,
            node = self.node_id,
            idx = self.next_side_effect_index(),
        );
        self.clients
            .tasks
            .enqueue_dedup(weft_task_store::NewTask {
                kind: kind.into(),
                target: weft_task_store::TaskTarget::Dispatcher,
                project_id: Some(self.project_id.clone()),
                dedup_key: Some(dedup_key),
                color: Some(self.color.to_string()),
                tenant_id: Some(self.tenant_id.clone()),
                target_pod_name: None,
                binary_hash: None,
                payload: payload_json,
            })
            .await
            .map_err(|e| WeftError::Config(format!("{dedup_prefix} enqueue: {e}")))?;
        Ok(())
    }

    /// Seed the per-(node, frames) await-call sequence the loop
    /// driver pulled from the journal fold. Replaces
    /// `with_expected_token` from the single-await world: now we
    /// have an ordered sequence, not just one token.
    pub fn with_awaited_sequence(
        mut self,
        sequence: Vec<weft_core::primitive::AwaitedEntry>,
    ) -> Self {
        self.awaited_sequence = Mutex::new(sequence.into());
        self
    }

    /// Reject any port name the node didn't declare in its metadata.
    /// Runs BEFORE the one-emission-per-port gate so a typo'd port is
    /// surfaced as the actual bug (undeclared) rather than as a misleading
    /// "already emitted" error after a second misspelled emit. Without
    /// this, the loop driver's postprocess layer would route through its
    /// undeclared-port fallthrough and silently drop the emit, leaving
    /// a downstream Gather hanging.
    fn check_declared_outputs(&self, ports: &[String]) -> WeftResult<()> {
        for port_name in ports {
            if !self.declared_outputs.contains_key(port_name) {
                return Err(WeftError::NodeExecution(format!(
                    "node '{}' tried to emit on undeclared output port '{}'. \
                     Declare it in metadata.json's outputs list, or correct \
                     the port name in the node body.",
                    self.node_id, port_name
                )));
            }
        }
        Ok(())
    }

    /// Claim a set of output ports for this firing under the
    /// one-emission-per-port rule. Errors loud the first time any port
    /// would be claimed twice (whether by `pulse_downstream` re-emit or
    /// a `close_port` after an emit, in either order). The check is
    /// transactional: if any port in `ports` collides, NONE is recorded,
    /// so the caller sees a clean "this attempt failed" instead of a
    /// partial mention that would poison later legitimate emissions.
    fn mention_or_err(&self, ports: &[String]) -> WeftResult<()> {
        let mut already = self
            .mentioned_outputs
            .lock()
            .expect("mentioned_outputs poisoned");
        for port_name in ports {
            if already.contains(port_name) {
                return Err(WeftError::NodeExecution(format!(
                    "node '{}' touched port '{}' twice in one firing. \
                     Each output port can be emitted or closed AT MOST ONCE per \
                     firing; release ports incrementally (e.g. a bus marker early, \
                     a `done` flag at the end) but never re-emit or re-close a port.",
                    self.node_id, port_name
                )));
            }
        }
        for port_name in ports {
            already.insert(port_name.clone());
        }
        Ok(())
    }

    /// Ship an `EmitMsg` to the loop driver. Errors loud if the handle
    /// has no emit channel (runtime wiring bug) or the loop receiver is
    /// closed (loop dropped while the node task was still running).
    fn send_emission(&self, kind: EmitKind) -> WeftResult<()> {
        let Some(tx) = self.emit_tx.as_ref() else {
            return Err(WeftError::Config(
                "emission called on a handle with no emit channel \
                 (this is a runtime wiring bug)"
                    .into(),
            ));
        };
        tx.send(TaskMsg::Emission(EmitMsg {
            node_id: self.node_id.clone(),
            frames: self.node_frames.clone(),
            kind,
        }))
        .map_err(|_| {
            WeftError::Runtime(anyhow::anyhow!(
                "emission: loop driver receiver closed"
            ))
        })?;
        Ok(())
    }

    /// Journal a non-terminal output-type mismatch for `port`: the node
    /// tried to emit `value` on a port declared `declared`, the types are
    /// incompatible, so the engine closed the port instead. Folds into the
    /// node execution's `port_warnings` and surfaces as a UI warning.
    /// Does not change the node's status.
    async fn record_port_type_mismatch(&self, port: &str, declared: &WeftType, value: &Value) {
        record_from_pod(
            self.clients.journal.as_ref(),
            ExecEvent::PortTypeMismatch {
                color: self.color,
                node_id: self.node_id.clone(),
                frames: self.node_frames.clone(),
                port: port.to_string(),
                expected: declared.to_string(),
                actual: WeftType::infer(value).to_string(),
                at_unix: now_unix(),
            },
            &self.pod_name,
        )
        .await;
    }
}

/// Ship the lifecycle event for a fresh dispatch of (node, frames):
/// `NodeResumed` if this dispatch is resuming a prior firing (either
/// suspension-resolved with a token/value, or crashed-Running recovery
/// with both None), otherwise `NodeStarted`. The audit's load-bearing
/// job in both cases is journaling `pulses_absorbed` so a later
/// crashed-Running un-absorb sees every pulse this dispatch consumed
/// (the fold rebuilds the record's `pulses_absorbed` from journaled
/// events; without this event, resume-time absorbs leak).
///
/// Invalid combo (is_resume=false AND resume_token_value=Some) panics
/// in debug: a caller flipping one but not the other would silently
/// write a NodeStarted while discarding a resume value.
#[allow(clippy::too_many_arguments)]
pub async fn ship_node_lifecycle(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    input: &serde_json::Value,
    closed_ports: &[String],
    pulses_absorbed: &[uuid::Uuid],
    resume_token_value: Option<&(String, serde_json::Value)>,
    is_resume: bool,
) {
    debug_assert!(
        is_resume || resume_token_value.is_none(),
        "ship_node_lifecycle: resume_token_value passed with is_resume=false"
    );
    let event = if is_resume {
        let (token, value) = match resume_token_value {
            Some((t, v)) => (Some(t.clone()), Some(v.clone())),
            None => (None, None),
        };
        ExecEvent::NodeResumed {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            token,
            value,
            pulses_absorbed: pulses_absorbed.iter().map(|u| u.to_string()).collect(),
            at_unix: now_unix(),
        }
    } else {
        ExecEvent::NodeStarted {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            input: input.clone(),
            closed_ports: closed_ports.to_vec(),
            pulses_absorbed: pulses_absorbed.iter().map(|u| u.to_string()).collect(),
            at_unix: now_unix(),
        }
    };
    record_from_pod(journal, event, pod_name).await;
}

pub async fn ship_node_suspended(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    token: &str,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeSuspended {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            token: token.to_string(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

/// Ship a terminal NodeCompleted carrying its unmentioned-port closures
/// atomically (see `NodeFailed.closure_emissions` in weft-journal): the
/// marker and the closures fold as one unit, so a crash between them
/// can't lose the closures and strand downstream consumers.
pub async fn ship_node_completed(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    output: &serde_json::Value,
    closures: Vec<weft_core::exec::PulseEmission>,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeCompleted {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            output: output.clone(),
            closure_emissions: closures.into_iter().map(Into::into).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_failed(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    error: &str,
    closures: Vec<weft_core::exec::PulseEmission>,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeFailed {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            error: error.to_string(),
            closure_emissions: closures.into_iter().map(Into::into).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_skipped(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    closed_ports: &[String],
    closures: Vec<weft_core::exec::PulseEmission>,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeSkipped {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            closed_ports: closed_ports.to_vec(),
            closure_emissions: closures.into_iter().map(Into::into).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

/// Ship every pulse the engine just emitted by writing one journal
/// event per emission. Order is preserved because journal rows have
/// monotonic ids; the fold replays them in insertion order.
pub async fn ship_pulse_emissions(
    journal: &dyn JournalClient,
    pod_name: &str,
    emissions: Vec<weft_core::exec::PulseEmission>,
) {
    for e in emissions {
        let p = e.pulse;
        record_from_pod(
            journal,
            ExecEvent::PulseEmitted {
                color: p.color,
                pulse_id: p.id.to_string(),
                source_node: e.source_node,
                source_port: e.source_port,
                target_node: p.target_node,
                target_port: p.target_port,
                frames: p.frames,
                value: p.value,
                closed: p.closed,
                at_unix: now_unix(),
            },
            pod_name,
        )
        .await;
    }
}

#[async_trait]
impl ContextHandle for RunnerHandle {
    /// Wait-and-resume primitive. Generalized to N awaits per body:
    /// each call has a 0-based call_index keyed on (node, frames).
    /// On every dispatch, the runtime pre-loads the per-(node, frames)
    /// awaited sequence from the journal fold:
    ///
    /// 1. **Replay path** (entry has `resolved=Some`): the call's
    ///    fire arrived in a prior cycle; return the stored value
    ///    instantly. Body keeps running.
    /// 2. **Suspend path** (entry has `resolved=None`): the call's
    ///    suspension is the still-pending tail of the sequence;
    ///    propagate `WeftError::Suspended` to release the worker.
    /// 3. **Fresh path** (sequence exhausted): no past entry for
    ///    this call_index; this is the first time this await runs.
    ///    Enqueue `register_signal` with is_resume=true and the
    ///    current call_index, then propagate `Suspended`. The next
    ///    re-dispatch after the fire will see this entry resolved.
    ///
    /// The author writes `let x = ctx.await_signal(...).await?;` N
    /// times in a row; each call is replay-instant on resume.
    async fn await_signal(&self, spec: SignalSpec) -> WeftResult<Value> {
        // Reconcile the two execution worlds. The wait policy (hold vs
        // suspend + hold time) resolves from the run's SuspendPolicy
        // through the general `wait` machinery; a live caller only supplies
        // that policy. We do NOT fail here on a non-suspendable run that
        // wants to suspend: other branches may still be running and talking
        // to the caller, so the kill/disconnect fires ONCE later, at the
        // true suspension point (the loop driver, when every branch is
        // parked), never when one branch reaches a wait. The driver also
        // owns the "hold the worker warm" decision (it treats an attached
        // caller like a live bus). So there is nothing to special-case in
        // this per-await path; the suspend path below runs unchanged.

        // A durable suspension replays the whole node body on resume.
        // Emitting (`pulse_downstream`) before a durable await is
        // unsound: the replay would re-run the body and re-emit the
        // already-sent pulses (the journal doesn't memoize emissions
        // the way it memoizes awaits/run), duplicating downstream
        // work. A node that holds a live bus must NOT durably suspend
        // anyway: it stays warm and uses bus.recv() instead.
        if !self
            .mentioned_outputs
            .lock()
            .expect("mentioned_outputs poisoned")
            .is_empty()
        {
            return Err(WeftError::NodeExecution(format!(
                "node '{}' called await_signal after pulse_downstream; a node that emits then \
                 durably suspends would re-emit on replay. Emit after all awaits, or (for a \
                 co-alive node) stay warm with bus.recv() instead of await_signal.",
                self.node_id
            )));
        }
        let call_index = self.next_call_index.fetch_add(1, Ordering::SeqCst);

        // Pop the next pre-loaded entry (if any). Replay vs suspend
        // depends on whether its fire already arrived.
        let next_entry = {
            let mut seq = self
                .awaited_sequence
                .lock()
                .expect("awaited_sequence mutex poisoned");
            seq.pop_front()
        };
        if let Some(entry) = next_entry {
            // Sanity-check call_index alignment. If the journal's
            // sequence disagrees with our counter, something
            // replayed out of order; the body is non-deterministic
            // (or the journal is corrupt). Fail the node loudly
            // rather than masking it as a suspension.
            if entry.call_index != call_index {
                return Err(WeftError::NodeExecution(format!(
                    "await_signal call_index mismatch (counter={call_index}, journal={}). \
                     This means the node body's call order changed between replays. \
                     Wrap any non-deterministic logic between awaits in `ctx.run`.",
                    entry.call_index
                )));
            }
            match entry.kind {
                weft_core::primitive::AwaitedEntryKind::Await {
                    token,
                    resolved,
                } => match resolved {
                    Some(value) => return Ok(value),
                    None => {
                        // Pending tail: fire hasn't arrived. Suspend
                        // with the existing token so the dispatcher
                        // doesn't re-register. If a bus is holding the
                        // worker alive, the driver's in-loop resume poll
                        // picks up this token's SuspensionResolved row
                        // when it lands and re-dispatches in process; if
                        // not, the worker exits and respawns on the fire.
                        return Err(WeftError::Suspended { token });
                    }
                },
                weft_core::primitive::AwaitedEntryKind::Run { name, .. } => {
                    return Err(WeftError::NodeExecution(format!(
                        "await_signal at call_index={call_index} but journal has Run('{name}'). \
                         This means the node body called `ctx.run` here on a previous run \
                         and `ctx.await_signal` now; non-deterministic bodies are not safe \
                         to replay. Use `ctx.run` to wrap non-deterministic work."
                    )));
                }
            }
        }

        // Sequence exhausted: this is a fresh await.
        // Enqueue a register_signal task carrying our call_index so
        // the dispatcher journals SuspensionRegistered with the right
        // ordinal. Body propagates Suspended afterwards.
        let reply = enqueue_register_signal_task(
            self.clients.tasks.as_ref(),
            self.color,
            &self.node_id,
            &self.node_frames,
            &spec,
            true,
            &self.tenant_id,
            call_index,
        )
        .await
        .map_err(|e| WeftError::Suspension(format!("request token: {e}")))?;
        tracing::info!(
            target: "weft_engine::suspend",
            node = %self.node_id,
            color = %self.color,
            call_index = call_index,
            token = %reply.token,
            "await_signal: registered; returning Suspended",
        );
        Err(WeftError::Suspended { token: reply.token })
    }

    /// Replay-side of `ctx.run`. Pops the next entry in the
    /// (node, frames) sequence; if it's a Run with our call_index,
    /// return its journaled value. If it's an Await at our index,
    /// the body's call sequence drifted from the journal: error
    /// loudly. If the sequence is exhausted, return None to signal
    /// "fresh path" so the wrapper invokes the closure.
    async fn run_step(&self, name: &str) -> WeftResult<(u32, Option<Value>)> {
        let call_index = self.next_call_index.fetch_add(1, Ordering::SeqCst);
        let next_entry = {
            let mut seq = self
                .awaited_sequence
                .lock()
                .expect("awaited_sequence mutex poisoned");
            seq.pop_front()
        };
        match next_entry {
            Some(entry) => {
                if entry.call_index != call_index {
                    return Err(WeftError::NodeExecution(format!(
                        "ctx.run('{name}') call_index mismatch (counter={call_index}, journal={}). \
                         This means the node body's call order changed between replays. \
                         Wrap any non-deterministic logic in `ctx.run`.",
                        entry.call_index
                    )));
                }
                match entry.kind {
                    weft_core::primitive::AwaitedEntryKind::Run { value, .. } => {
                        Ok((call_index, Some(value)))
                    }
                    weft_core::primitive::AwaitedEntryKind::Await { .. } => {
                        Err(WeftError::NodeExecution(format!(
                            "ctx.run('{name}') at call_index={call_index} but journal has Await. \
                             This means the node body called `ctx.await_signal` here on a previous \
                             run and `ctx.run` now; non-deterministic bodies are not safe to \
                             replay."
                        )))
                    }
                }
            }
            None => Ok((call_index, None)),
        }
    }

    async fn run_record(&self, name: &str, call_index: u32, value: &Value) -> WeftResult<()> {
        // call_index is the value run_step returned, passed in so
        // run_step and run_record agree on the index explicitly
        // rather than via a shared counter both sides read.
        record_from_pod(
            self.clients.journal.as_ref(),
            ExecEvent::RunOutput {
                color: self.color,
                node_id: self.node_id.clone(),
                frames: self.node_frames.clone(),
                call_index,
                name: name.to_string(),
                value: value.clone(),
                at_unix: now_unix(),
            },
            &self.pod_name,
        )
        .await;
        Ok(())
    }

    async fn storage_put(
        &self,
        scope: &weft_core::storage::StorageScope,
        data: weft_core::storage::ByteStream,
        mime_type: &str,
        filename: &str,
        keep: Option<weft_core::storage::KeepTtl>,
        declared_size: Option<u64>,
    ) -> WeftResult<Value> {
        self.clients
            .storage
            .put(self.color, scope, mime_type, filename, keep, declared_size, data)
            .await
    }

    async fn storage_put_from_url(
        &self,
        scope: &weft_core::storage::StorageScope,
        url: &str,
        filename: Option<&str>,
        keep: Option<weft_core::storage::KeepTtl>,
    ) -> WeftResult<Value> {
        // Reuse the process-wide pooled client (a fresh Client::new()
        // per fetch rebuilds the connection pool; see http_client).
        let resp = http_client()
            .get(url)
            .send()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("fetch {url}: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(WeftError::NodeExecution(format!(
                "fetch {url} returned {status}: {}",
                weft_core::truncate_user_string(&body, 512)
            )));
        }
        let mime = weft_core::storage::normalize_content_type(
            resp.headers().get("content-type").and_then(|v| v.to_str().ok()),
        );
        let name = filename
            .filter(|f| !f.is_empty())
            .map(String::from)
            .unwrap_or_else(|| weft_core::storage::filename_from_url(url));
        // A sized response body (Content-Length) is declared up front so the
        // whole quota charge happens before the first byte moves.
        let declared_size = resp.content_length();
        // Stream the body straight through (never buffer the whole
        // file), so a multi-gigabyte download stays bounded-memory.
        let stream: weft_core::storage::ByteStream = Box::pin(
            resp.bytes_stream()
                .map_err(|e| std::io::Error::other(format!("fetch stream: {e}"))),
        );
        self.clients.storage.put(self.color, scope, &mime, &name, keep, declared_size, stream).await
    }

    async fn storage_get(
        &self,
        key: &str,
        range: Option<weft_core::storage::ByteRange>,
    ) -> WeftResult<(weft_core::storage::StoredFileMeta, weft_core::storage::ByteStream)> {
        self.clients.storage.get(self.color, key, range).await
    }

    async fn storage_delete(&self, key: &str) -> WeftResult<()> {
        self.clients.storage.delete(self.color, key).await
    }

    async fn storage_list(
        &self,
        scope: &weft_core::storage::StorageScope,
    ) -> WeftResult<Vec<weft_core::storage::StoredFileMeta>> {
        self.clients.storage.list(self.color, scope).await
    }

    async fn storage_keep(
        &self,
        key: &str,
        ttl: weft_core::storage::KeepTtl,
    ) -> WeftResult<()> {
        self.clients.storage.keep(self.color, key, ttl).await
    }

    async fn storage_presign(&self, key: &str, ttl_secs: Option<u64>) -> WeftResult<String> {
        self.clients.storage.presign(self.color, key, ttl_secs).await
    }

    async fn endpoint_url(&self, name: &str) -> WeftResult<String> {
        let endpoint = self
            .clients
            .infra
            .endpoint_url(&self.project_id, &self.node_id, name)
            .await
            .map_err(|e| WeftError::Config(format!("infra_node lookup: {e}")))?;
        endpoint.ok_or_else(|| {
            WeftError::Config(format!(
                "endpoint '{}' for node '{}' is not available; either the infra isn't running \
                 or the endpoint name is not declared. Check `weft infra status` and the node's \
                 InfraSpec.endpoints list.",
                name, self.node_id
            ))
        })
    }

    async fn endpoint_call(
        &self,
        base: &str,
        method: weft_core::EndpointMethod,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> WeftResult<serde_json::Value> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let client = http_client();
        let req = match method {
            weft_core::EndpointMethod::Get => client.get(&url),
            weft_core::EndpointMethod::Post => {
                let mut r = client.post(&url);
                if let Some(b) = &body {
                    r = r.json(b);
                }
                r
            }
        };
        let resp = req.send().await.map_err(|e| {
            WeftError::Runtime(anyhow::anyhow!("endpoint_call {url}: {e}"))
        })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(WeftError::Runtime(anyhow::anyhow!(
                "endpoint_call {url} returned {status}: {body}"
            )));
        }
        resp.json::<serde_json::Value>().await.map_err(|e| {
            WeftError::Runtime(anyhow::anyhow!(
                "endpoint_call {url} response not JSON: {e}"
            ))
        })
    }

    /// Entry-trigger registration. Synchronous: enqueues the
    /// register_signal task and waits for the dispatcher to ack.
    /// Worker keeps executing; the signal stays armed and
    /// persistent until the project is deactivated. Each external
    /// fire spawns a fresh execution (the dispatcher's relay path
    /// enqueues route_entry instead of resuming this firing).
    /// Distinct from await_signal: no suspend-then-resume cycle.
    async fn register_signal(&self, spec: SignalSpec) -> WeftResult<()> {
        // Entry triggers are one-shot per node per TriggerSetup.
        // The dedup key for this enqueue is `(color, node, frames,
        // is_resume=false, call_index=0)`; a second call from the
        // same node body would collide and silently drop the
        // second trigger. Catch that loudly here so the offending
        // node fails instead of a trigger going missing.
        let prev = self.entry_register_count.fetch_add(1, Ordering::SeqCst);
        if prev > 0 {
            return Err(WeftError::Config(format!(
                "node '{}' called ctx.register_signal more than once; \
                 entry triggers are one-per-node-per-TriggerSetup",
                self.node_id
            )));
        }
        let reply = enqueue_register_signal_task(
            self.clients.tasks.as_ref(),
            self.color,
            &self.node_id,
            &self.node_frames,
            &spec,
            false,
            &self.tenant_id,
            0,
        )
        .await
        .map_err(|e| WeftError::Suspension(format!("register_signal: {e}")))?;
        tracing::info!(
            target: "weft_engine::register",
            node = %self.node_id,
            color = %self.color,
            token = %reply.token,
            "register_signal: dispatcher ack"
        );
        Ok(())
    }

    async fn report_cost(&self, report: CostReport) -> WeftResult<()> {
        let amount = report.amount_usd;
        if !(amount.is_finite() && amount >= 0.0) {
            return Err(WeftError::Config(format!(
                "report_cost amount_usd must be finite and non-negative; got {amount}"
            )));
        }
        self.enqueue_side_effect_task(
            "report_cost",
            weft_task_store::TaskKind::RecordCost,
            weft_task_store::RecordCostPayload {
                color: self.color.to_string(),
                service: report.service,
                model: report.model,
                amount_usd: amount,
                metadata: report.metadata,
            },
        )
        .await
    }

    async fn log(&self, level: LogLevel, message: String) -> WeftResult<()> {
        let level_str = match level {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        };
        tracing::info!(
            target: "weft_engine::node",
            exec = %self.execution_id,
            level = level_str,
            "{message}"
        );
        self.enqueue_side_effect_task(
            "log",
            weft_task_store::TaskKind::RecordLog,
            weft_task_store::RecordLogPayload {
                color: self.color.to_string(),
                level: level_str.to_string(),
                message,
            },
        )
        .await
    }

    fn cancellation(&self) -> Arc<CancellationFlag> {
        self.cancellation.clone()
    }

    fn declared_output_ports(&self) -> &HashMap<String, WeftType> {
        &self.declared_outputs
    }

    /// Fire downstream. Each output port the node mentions in
    /// `output` becomes a pulse on its outgoing edges; mentioned-but-
    /// already-emitted ports error loud (one-emission-per-port rule).
    /// Calling `pulse_downstream` multiple times with DISJOINT ports
    /// is fine (release early then finalize); calling it twice with
    /// OVERLAPPING ports is a node-author bug.
    async fn pulse_downstream(&self, output: NodeOutput) -> WeftResult<()> {
        let ports: Vec<String> = output.outputs.keys().cloned().collect();
        self.check_declared_outputs(&ports)?;
        // Every port this call touches is claimed up front (the
        // one-emission-per-port rule). A type-mismatched port is still
        // "touched": it gets closed instead of emitted, so it must be
        // claimed too, or a later legitimate emit on it would slip past.
        self.mention_or_err(&ports)?;

        // Runtime output-type check: each value must be compatible with
        // its port's DECLARED type (which already reflects any narrowing
        // the author applied in the node header). An incompatible value is
        // refused: we record a non-terminal PortTypeMismatch and CLOSE the
        // port (downstream sees null) instead of letting the wrong-typed
        // value flow. Compatible ports emit together in one Values batch.
        let mut kept = NodeOutput::empty();
        let mut closed: Vec<String> = Vec::new();
        for (port, value) in output.outputs {
            match self.declared_outputs.get(&port) {
                Some(declared) if !type_accepts(declared, &value) => {
                    self.record_port_type_mismatch(&port, declared, &value).await;
                    closed.push(port);
                }
                _ => {
                    kept.outputs.insert(port, value);
                }
            }
        }

        for port in closed {
            self.send_emission(EmitKind::Close(port))?;
        }
        if !kept.outputs.is_empty() {
            self.send_emission(EmitKind::Values(kept))?;
        }
        Ok(())
    }

    /// Close a single output port mid-firing. Goes through the same
    /// one-emission-per-port gate as `pulse_downstream`, then ships an
    /// `EmitKind::Close` so the loop driver emits a closure pulse on
    /// every outgoing edge of that port at this firing's frame stack, the
    /// same shape the termination-time sweep would produce.
    async fn close_port(&self, port: &str) -> WeftResult<()> {
        let port = port.to_string();
        self.check_declared_outputs(std::slice::from_ref(&port))?;
        self.mention_or_err(std::slice::from_ref(&port))?;
        self.send_emission(EmitKind::Close(port))
    }

    fn create_bus(&self, opts: BusOptions) -> WeftResult<(BusHandle, Value)> {
        let handle = self
            .bus_coordinator
            .new_bus(opts, self.bus_participant())
            .map_err(|e| WeftError::Input(format!("ctx.create_bus on node '{}': {e}", self.node_id)))?;
        let marker = handle.marker();
        Ok((handle, marker))
    }

    fn bus(&self, marker: &Value) -> WeftResult<BusHandle> {
        self.bus_coordinator
            .lookup_bus(marker, self.bus_participant())
            .map_err(|e| {
                WeftError::Input(format!(
                    "ctx.bus on node '{}': {e}",
                    self.node_id
                ))
            })
    }

    fn wake_payload(&self) -> Option<&Value> {
        self.wake_payload.as_ref()
    }

    fn caller_connection(&self) -> Option<Arc<dyn weft_core::caller::CallerConnection>> {
        self.caller_connection.clone()
    }
}

/// Reply shape from a `register_signal` task. Mirrors
/// `weft-dispatcher::task_kinds::register_signal::RegisterSignalResult`
/// but lives here because the engine can't depend on the
/// dispatcher.
#[derive(Debug, serde::Deserialize)]
struct RegisterSignalReply {
    token: String,
}

async fn enqueue_register_signal_task(
    tasks: &dyn TaskStoreClient,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    spec: &SignalSpec,
    is_resume: bool,
    tenant_id: &str,
    call_index: u32,
) -> anyhow::Result<RegisterSignalReply> {
    // Task-level dedup so retries (network blip, supervisor
    // reconnect) converge on the same token. `is_resume` is in the
    // key because the same (color, node, frames, call_index) tuple can
    // be reused across a resume + an entry-trigger registration on
    // the same node body (e.g. a node that awaits its own webhook).
    // Separate from the journal-level dedup the dispatcher's
    // executor uses for SuspensionRegistered: that one omits
    // `is_resume` because only resume registrations journal a
    // SuspensionRegistered event.
    let frames_key = frames_dedup_key(frames)?;
    let dedup_key = format!(
        "{}/{}/{}/{}/{}",
        color, node_id, frames_key, is_resume, call_index,
    );
    let payload = serde_json::json!({
        "color": color.to_string(),
        "node_id": node_id,
        "frames": frames,
        "spec": spec,
        "is_resume": is_resume,
        "call_index": call_index,
    });
    let id = tasks
        .enqueue_dedup(task_store::NewTask {
            kind: TaskKind::RegisterSignal.into(),
            target: task_store::TaskTarget::Dispatcher,
            project_id: None,
            dedup_key: Some(dedup_key),
            color: Some(color.to_string()),
            tenant_id: Some(tenant_id.to_string()),
            target_pod_name: None,
            binary_hash: None,
            payload,
        })
        .await?
        .id()
        // Only the broker-backed FireSignal path can fence (placement
        // generation); this register-signal enqueue never does, so it
        // always yields a task id.
        .expect("register-signal enqueue is never fenced");
    let outcome = tasks
        .wait_for_terminal(id, TASK_WAIT_TIMEOUT, TASK_POLL_INTERVAL)
        .await?;
    match outcome.status {
        task_store::TaskStatus::Complete => {
            let result = outcome
                .result
                .ok_or_else(|| anyhow::anyhow!("register_signal returned no result"))?;
            Ok(serde_json::from_value(result)?)
        }
        task_store::TaskStatus::Failed => {
            anyhow::bail!("{}", outcome.error.unwrap_or_else(|| "register_signal failed".into()))
        }
        other => anyhow::bail!("register_signal status: {other:?}"),
    }
}

/// Process-wide `reqwest::Client` for the engine's outbound HTTP
/// (endpoint calls + storage put_from_url fetches). One per worker
/// process so the connection pool stays warm across calls inside a
/// loop body (the anti-pattern is `Client::new()` per request: every
/// call rebuilds the pool).
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

#[cfg(test)]
mod type_check_tests {
    use super::type_accepts;
    use weft_core::storage::StoredFile;
    use weft_core::weft_type::{FileKind, WeftType};

    fn image_value() -> serde_json::Value {
        StoredFile {
            key: "exec/c/img".into(),
            mime_type: "image/png".into(),
            size_bytes: 10,
            filename: "x.png".into(),
        }
        .to_value()
    }

    fn video_value() -> serde_json::Value {
        StoredFile {
            key: "exec/c/vid".into(),
            mime_type: "video/mp4".into(),
            size_bytes: 10,
            filename: "x.mp4".into(),
        }
        .to_value()
    }

    #[test]
    fn declared_file_accepts_any_stored_file() {
        let file = WeftType::file();
        assert!(type_accepts(&file, &image_value()));
        assert!(type_accepts(&file, &video_value()));
    }

    #[test]
    fn narrowed_image_accepts_image_rejects_video() {
        // The File port narrowed to Image: an image flows, a video does not.
        let image = WeftType::primitive(weft_core::weft_type::WeftPrimitive::Image);
        assert!(type_accepts(&image, &image_value()));
        assert!(!type_accepts(&image, &video_value()), "a video on an Image port is refused");
    }

    #[test]
    fn primitive_mismatch_is_rejected() {
        let number = WeftType::primitive(weft_core::weft_type::WeftPrimitive::Number);
        assert!(type_accepts(&number, &serde_json::json!(42)));
        assert!(!type_accepts(&number, &serde_json::json!("not a number")));
    }

    #[test]
    fn unresolved_declared_accepts_anything() {
        // A TypeVar / MustOverride port has no concrete contract yet, so the
        // gate must not reject (is_compatible short-circuits on unresolved).
        assert!(type_accepts(&WeftType::MustOverride, &video_value()));
        assert!(type_accepts(&WeftType::type_var("T"), &serde_json::json!("anything")));
    }

    // Touch FileKind so the import is used even if the helpers change.
    #[test]
    fn image_value_infers_as_image_marker() {
        assert_eq!(FileKind::from_mime("image/png"), FileKind::Image);
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use weft_core::context::ContextHandle;
    use weft_core::primitive::{AwaitedEntry, AwaitedEntryKind};

    fn handle_with_sequence(seq: Vec<AwaitedEntry>) -> RunnerHandle {
        // These tests only exercise run_step which never touches the
        // store. The no-op clients below let the trait objects exist
        // without any IO.
        let clients = EngineClients {
            journal: Arc::new(NoopJournal),
            tasks: Arc::new(NoopTaskStore),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
        };
        RunnerHandle::new(
            "exec-1".into(),
            "00000000-0000-0000-0000-000000000000".into(),
            uuid::Uuid::nil(),
            "node-x".into(),
            weft_core::frames::LoopFrames::default(),
            clients,
            "pod-1".into(),
            "tenant-1".into(),
            std::sync::Arc::new(CancellationFlag::new()),
            BusCoordinator::new(),
            HashMap::new(),
        )
        .with_awaited_sequence(seq)
    }

    // Layer 3: the ContextHandle storage methods over the fake
    // worker-storage (scope-built keys, stored-file value round trip,
    // wall enforcement, keep + sweep semantics).
    #[tokio::test]
    async fn storage_methods_round_trip_and_enforce_the_wall() {
        use weft_core::storage::{KeepTtl, StorageScope, StoredFile};
        let handle = handle_with_sequence(vec![]);

        let file = handle
            .storage_put(
                &StorageScope::Execution,
                weft_core::storage::bytes_stream(bytes::Bytes::from_static(b"payload")),
                "audio/ogg",
                "clip.ogg",
                None,
                Some(7),
            )
            .await
            .expect("put");
        let stored = StoredFile::from_value(&file).expect("self-describing value");
        // Keys are tenant-anchored now (`<tenant>/<scope>/...`); the fake
        // worker storage is seeded as tenant `t1`.
        assert!(stored.key.starts_with("t1/exec/c1/"), "{}", stored.key);
        assert_eq!(stored.size_bytes, 7);

        let (meta, stream) = handle.storage_get(&stored.key, None).await.expect("get");
        assert_eq!(meta.mime_type, "audio/ogg");
        let bytes = weft_core::storage::collect_stream(stream).await.unwrap();
        assert_eq!(&bytes[..], b"payload");

        // The wall: another color's exec key (under the same tenant) is denied.
        let err = match handle.storage_get("t1/exec/OTHER/f0", None).await {
            Err(e) => e,
            Ok(_) => panic!("cross-color get must be denied"),
        };
        assert!(err.to_string().contains("denied"), "{err}");

        // Keep marks the file to survive the broker's terminate sweep (the
        // sweep itself is broker-side, covered by the broker's db tests;
        // the worker has no sweep verb).
        handle.storage_keep(&stored.key, KeepTtl::Default).await.expect("keep");
        assert!(handle.storage_get(&stored.key, None).await.is_ok(), "kept file still readable");

        // Presign mints a (bucket) URL for an owned file.
        let url = handle.storage_presign(&stored.key, Some(60)).await.expect("presign");
        assert!(url.starts_with("http") && url.contains(&stored.key), "{url}");
    }

    struct NoopJournal;
    #[async_trait]
    impl JournalClient for NoopJournal {
        async fn record_event(
            &self,
            _event: &ExecEvent,
            _pod_name: Option<&str>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn events_for_color(
            &self,
            _color: Color,
        ) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(Vec::new())
        }
        async fn has_terminal_event(&self, _color: Color) -> anyhow::Result<bool> {
            Ok(false)
        }
    }
    struct NoopTaskStore;
    #[async_trait]
    impl TaskStoreClient for NoopTaskStore {
        async fn enqueue_dedup(
            &self,
            _spec: weft_task_store::tasks::NewTask,
        ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            unreachable!("replay tests do not enqueue")
        }
        async fn wait_for_terminal(
            &self,
            _task_id: uuid::Uuid,
            _timeout: std::time::Duration,
            _poll_interval: std::time::Duration,
        ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!("replay tests do not wait")
        }
        async fn claim_one(
            &self,
            _pod_id: &str,
            _filter: weft_task_store::tasks::ClaimFilter,
        ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
            Ok(None)
        }
        async fn heartbeat(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
        ) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _result: Value,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _error: String,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }
    struct NoopInfra;
    #[async_trait]
    impl InfraReader for NoopInfra {
        async fn endpoint_url(
            &self,
            _project_id: &str,
            _node_id: &str,
            _endpoint_name: &str,
        ) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
    }

    struct NoopInfraState;
    #[async_trait]
    impl InfraStateClient for NoopInfraState {
        async fn enqueue_apply(
            &self,
            _project_id: &str,
            _node_id: &str,
            _spec_json: serde_json::Value,
        ) -> anyhow::Result<i64> {
            Ok(0)
        }
        async fn wait_apply(
            &self,
            _project_id: &str,
            _command_id: i64,
        ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
            Ok(weft_broker_client::protocol::InfraWaitApplyResponse {
                completed: true,
                outcome: Some(weft_broker_client::protocol::LifecycleOutcome::Succeeded),
                outcome_message: None,
            })
        }
    }

    struct NoopProject;
    #[async_trait]
    impl crate::context::ProjectClient for NoopProject {
        async fn fetch_definition(
            &self,
            _project_id: &str,
            _expected_hash: &str,
        ) -> anyhow::Result<Option<weft_core::ProjectDefinition>> {
            // Replay tests don't take this path (they exercise
            // RunnerHandle directly, never spawning a worker pod);
            // if a test somehow calls this, fail loud.
            anyhow::bail!("NoopProject::fetch_definition not implemented")
        }
    }

    /// Replay path: a Run entry at the next call_index returns
    /// its journaled value without invoking the closure.
    #[tokio::test]
    async fn run_step_replay_returns_journaled_value() {
        let seq = vec![AwaitedEntry {
            call_index: 0,
            kind: AwaitedEntryKind::Run {
                name: "decide".into(),
                value: serde_json::json!("go-left"),
            },
        }];
        let handle = handle_with_sequence(seq);
        let (idx, got) = handle
            .run_step("decide")
            .await
            .expect("run_step ok");
        assert_eq!(idx, 0);
        assert_eq!(got, Some(serde_json::json!("go-left")));
    }

    /// Fresh path: sequence is empty, run_step returns None so
    /// the wrapper invokes the closure.
    #[tokio::test]
    async fn run_step_fresh_returns_none() {
        let handle = handle_with_sequence(Vec::new());
        let (idx, got) = handle.run_step("decide").await.expect("run_step ok");
        assert_eq!(idx, 0);
        assert!(got.is_none(), "no journaled output yet");
    }

    /// Mismatched kind at the same call_index: body called
    /// `ctx.run` but the journal has an `Await` at this index.
    /// Surfaces as Suspension error so the body fails loudly
    /// instead of silently desyncing.
    #[tokio::test]
    async fn run_step_mismatch_kind_errors() {
        let seq = vec![AwaitedEntry {
            call_index: 0,
            kind: AwaitedEntryKind::Await {
                token: "tok-0".into(),
                resolved: Some(serde_json::json!("from-fire")),
            },
        }];
        let handle = handle_with_sequence(seq);
        let err = handle
            .run_step("decide")
            .await
            .expect_err("should fail on kind mismatch");
        let msg = format!("{err}");
        assert!(
            matches!(err, WeftError::NodeExecution(_)),
            "expected NodeExecution variant, got: {err:?}"
        );
        assert!(
            msg.contains("non-deterministic bodies are not safe to replay"),
            "unexpected: {msg}"
        );
    }

    /// Multi-call replay: two Run entries in sequence pop in order.
    #[tokio::test]
    async fn run_step_replay_sequential() {
        let seq = vec![
            AwaitedEntry {
                call_index: 0,
                kind: AwaitedEntryKind::Run {
                    name: "first".into(),
                    value: serde_json::json!("v0"),
                },
            },
            AwaitedEntry {
                call_index: 1,
                kind: AwaitedEntryKind::Run {
                    name: "second".into(),
                    value: serde_json::json!("v1"),
                },
            },
        ];
        let handle = handle_with_sequence(seq);
        assert_eq!(
            handle.run_step("first").await.expect("first"),
            (0, Some(serde_json::json!("v0")))
        );
        assert_eq!(
            handle.run_step("second").await.expect("second"),
            (1, Some(serde_json::json!("v1")))
        );
        // Third call: sequence exhausted, returns None.
        let (idx, val) = handle.run_step("third").await.expect("third");
        assert_eq!(idx, 2);
        assert!(val.is_none());
    }

    /// Mixed sequence: Await then Run. Verifies the counter and
    /// pop work across kinds.
    #[tokio::test]
    async fn await_then_run_replay_in_order() {
        let seq = vec![
            AwaitedEntry {
                call_index: 0,
                kind: AwaitedEntryKind::Await {
                    token: "tok-0".into(),
                    resolved: Some(serde_json::json!("answer")),
                },
            },
            AwaitedEntry {
                call_index: 1,
                kind: AwaitedEntryKind::Run {
                    name: "process".into(),
                    value: serde_json::json!({"shape": "pre-baked"}),
                },
            },
        ];
        let handle = handle_with_sequence(seq);
        use weft_core::signal::{to_spec, Form, FormSchema};
        let spec = to_spec(Form {
            form_type: "human-query".into(),
            schema: FormSchema {
                title: String::new(),
                description: None,
                fields: Vec::new(),
            },
            title: None,
            description: None,
            consumer_kind: None,
        });
        let answer = handle
            .await_signal(spec)
            .await
            .expect("await replay ok");
        assert_eq!(answer, serde_json::json!("answer"));
        let processed = handle
            .run_step("process")
            .await
            .expect("run replay ok");
        assert_eq!(
            processed,
            (1, Some(serde_json::json!({"shape": "pre-baked"})))
        );
    }

    // ----- Live caller surface on the ContextHandle ------------------

    use weft_core::caller::{CallerRuntimeConfig, FakeCallerConnection};
    use weft_core::signal::{Backpressure, DataType, ErrorMode, Protocol};
    use weft_core::wait::SuspendPolicy;

    fn caller_cfg(protocol: Protocol, can_suspend: bool) -> CallerRuntimeConfig {
        CallerRuntimeConfig {
            protocol,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: ErrorMode::Surface,
            connect_timeout_secs: 5,
            max_inbound_bytes: 1024,
            max_session_secs: 0,
            suspend: SuspendPolicy { can_suspend, default_hold_secs: 300 },
            inbound_window: weft_core::caller::DEFAULT_INBOUND_WINDOW,
        }
    }

    #[test]
    fn queries_report_protocol_and_none_without_caller() {
        // No caller wired: both queries false, caller() None.
        let bare = handle_with_sequence(vec![]);
        assert!(!bare.caller_connection().is_some());

        // HTTP caller wired: protocol is Http.
        let http = handle_with_sequence(vec![]).with_caller_connection(Some(
            FakeCallerConnection::connected(caller_cfg(Protocol::Http, false)),
        ));
        assert!(http.caller_connection().is_some());
        let proto = http.caller_connection().unwrap().config().protocol;
        assert_eq!(proto, Protocol::Http);

        let ws = handle_with_sequence(vec![]).with_caller_connection(Some(
            FakeCallerConnection::connected(caller_cfg(Protocol::Websocket, true)),
        ));
        assert_eq!(ws.caller_connection().unwrap().config().protocol, Protocol::Websocket);
    }

    #[tokio::test]
    async fn await_signal_does_not_fail_at_the_call_in_a_tied_run() {
        // A durable wait in a caller-tied run does NOT fail at the await
        // call: other branches may still be running and talking to the
        // caller, so the reconciliation (hold-then-kill) is deferred to the
        // true suspension point in the loop driver. At the call, the await
        // suspends as normal (returns Suspended), it does NOT raise a
        // policy error. Use the pending-tail replay path (a pre-loaded
        // unresolved await) so the suspend is reached without the broker.
        let seq = vec![AwaitedEntry {
            call_index: 0,
            kind: AwaitedEntryKind::Await { token: "tok-pending".into(), resolved: None },
        }];
        let handle = handle_with_sequence(seq).with_caller_connection(Some(
            FakeCallerConnection::connected(caller_cfg(Protocol::Websocket, false)),
        ));
        let spec = weft_core::signal::to_spec(weft_core::signal::Timer {
            spec: weft_core::signal::TimerSpec::After { duration_ms: 1000 },
        });
        let err = handle.await_signal(spec).await.expect_err("a pending await suspends");
        assert!(
            matches!(err, WeftError::Suspended { ref token } if token == "tok-pending"),
            "tied-run await must suspend at the call, not fail; got: {err:?}"
        );
    }
}

/// After `node.provision()` returns, the loop driver calls this to
/// ship the spec to the supervisor and wait for it to settle.
///
/// The worker doesn't compile, doesn't hash, doesn't decide
/// skip/fresh/replace. The supervisor owns all of those: it reads
/// the prior `infra_node` row, compiles the new spec with the real
/// image-tag map + instance id (fresh-mint or reused), hashes,
/// makes the decision, and executes. The worker just polls the
/// command row for terminal state.
///
/// This is a single round-trip from the engine's perspective:
/// "supervisor, please apply this spec; tell me when you're done."
/// Skip detection happens supervisor-side and is invisible to the
/// caller (success is success either way).
pub async fn apply_via_supervisor(
    infra_state: &dyn InfraStateClient,
    clock: &dyn weft_platform_traits::Clock,
    project_id: &str,
    node_id: &str,
    spec: &weft_core::infra::InfraSpec,
) -> anyhow::Result<()> {
    let spec_json = serde_json::to_value(spec)?;
    let cmd_id = infra_state
        .enqueue_apply(project_id, node_id, spec_json)
        .await?;
    let deadline = clock.now() + TASK_WAIT_TIMEOUT;
    loop {
        let resp = infra_state.wait_apply(project_id, cmd_id).await?;
        if resp.completed {
            use weft_broker_client::protocol::LifecycleOutcome;
            match resp.outcome {
                Some(LifecycleOutcome::Succeeded) => return Ok(()),
                Some(LifecycleOutcome::Cancelled) => {
                    // The command was abandoned (e.g. the node was
                    // removed by `remove_node` mid-flight). Not a
                    // failure: surface as "no longer applicable"
                    // and let the engine treat it as completed.
                    let reason = resp.outcome_message.as_deref().unwrap_or("cancelled");
                    tracing::info!(
                        target: "weft_engine::context",
                        project_id,
                        node_id,
                        reason,
                        "supervisor apply cancelled; no longer applicable"
                    );
                    return Ok(());
                }
                Some(LifecycleOutcome::Failed) => {
                    let err = resp
                        .outcome_message
                        .unwrap_or_else(|| "supervisor reported no error detail".into());
                    anyhow::bail!("supervisor apply failed: {err}");
                }
                None => {
                    // completed=true with outcome=None means schema
                    // drift the broker should have caught; fail loud.
                    anyhow::bail!(
                        "supervisor apply completed but returned no outcome"
                    );
                }
            }
        }
        if clock.now() >= deadline {
            anyhow::bail!(
                "supervisor did not complete apply command {cmd_id} within {}s",
                TASK_WAIT_TIMEOUT.as_secs()
            );
        }
        clock.sleep(TASK_POLL_INTERVAL).await;
    }
}

// =====================================================================
//                       Layer-3 tests: bus journal pump
// =====================================================================
//
// These exercise the per-execution `BusCoordinator` + `run_bus_journal_task`
// against a faked `JournalClient`. They prove the four original failure
// modes the redesign was for cannot recur:
//   1. `live_buses` does not leak (Weak refs drop after close).
//   2. The journal pump never silently drops; failure surfaces on the
//      next `send` as `SendError::JournalDegraded`.
//   3. There is no Lagged-style silent swallow: ephemeral consumers get
//      `CursorError::FellBehind`; journaled consumers just lag.
//   4. Register-then-close cannot orphan a `Joined`: the log lock
//      serializes both with the `closed` flag.

#[cfg(test)]
mod bus_pump_tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;
    use weft_core::bus::{BusOptions, SendError};
    use weft_journal::ExecEvent;

    /// Capturing journal client. Stores every `record_event` payload
    /// so tests can assert on the bus events the pump shipped.
    /// Optionally throws on every Nth call to exercise the degraded
    /// path; `fail_next` set to `Some(N)` fails the Nth following
    /// call exactly once, then resets.
    #[derive(Default)]
    struct CaptureJournal {
        events: StdMutex<Vec<ExecEvent>>,
        fail_count: StdMutex<usize>,
    }
    #[async_trait]
    impl weft_journal::JournalClient for CaptureJournal {
        async fn record_event(&self, event: &ExecEvent, _pod: Option<&str>) -> anyhow::Result<()> {
            {
                let mut fc = self.fail_count.lock().unwrap();
                if *fc > 0 {
                    *fc -= 1;
                    return Err(anyhow::anyhow!("simulated journal failure"));
                }
            }
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn events_for_color(&self, _color: Color) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(Vec::new())
        }
        async fn has_terminal_event(&self, _color: Color) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    fn spawn_pump(
        coordinator: &Arc<BusCoordinator>,
        journal: Arc<CaptureJournal>,
        color: Color,
    ) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(coordinator);
        let journal_dyn: Arc<dyn weft_journal::JournalClient> = journal;
        tokio::spawn(run_bus_journal_task(weak, color, journal_dyn, "pod".into()))
    }

    /// Test helper: thin wrapper around `coord.new_bus` so tests have
    /// the same call signature as production. Unwraps the validation
    /// result; tests pass valid options. These tests exercise the
    /// journal pump / drain, not the stuck-detector, so a single
    /// synthetic node identity at root frames is sufficient.
    fn new_bus(coord: &Arc<BusCoordinator>, opts: BusOptions) -> BusHandle {
        coord
            .new_bus(opts, ("test-node".to_string(), Vec::new()))
            .expect("test BusOptions cannot fail")
    }

    /// Test helper: mint a bus attributed to a specific node execution
    /// at root frames, so liveness tests can drive distinct participants.
    fn new_bus_for(coord: &Arc<BusCoordinator>, node_id: &str) -> BusHandle {
        coord
            .new_bus(BusOptions::default(), (node_id.to_string(), Vec::new()))
            .expect("test BusOptions cannot fail")
    }

    fn participant(node_id: &str) -> weft_core::bus::BusParticipant {
        (node_id.to_string(), Vec::new())
    }

    /// Test helper: run the production shutdown sequence, then drop
    /// the coordinator and join the pump task. Bounded by 2s so a
    /// regression fails fast instead of hanging the suite.
    async fn shutdown_and_join(coord: Arc<BusCoordinator>, pump: tokio::task::JoinHandle<()>) {
        coord.shutdown(std::time::Duration::from_secs(2)).await;
        drop(coord);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), pump).await;
    }

    /// Wait until `predicate` returns `true`, polling at 5ms intervals
    /// with a 2s bound so a regression fails fast instead of hanging.
    async fn wait_until<F: FnMut() -> bool>(mut predicate: F) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !predicate() {
            if std::time::Instant::now() > deadline {
                panic!("wait_until timed out");
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// The pump journals every BusJoined / BusMessage / BusClosed in
    /// offset order. The bus's mode is carried in the marker JSON
    /// itself, so there is no separate open event on the wire.
    #[tokio::test]
    async fn pump_journals_full_lifecycle_in_offset_order() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new();
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);

        let mut bus = new_bus(&coord, BusOptions::default());
        bus.register("alice").unwrap();
        bus.send("hi", serde_json::json!("world")).unwrap();
        bus.close();
        // Drop the producer handle so the only Arc<BusInner> is the
        // one the coordinator pins. This matches the production
        // shutdown shape: every node task is gone before close_all.
        drop(bus);

        // 3 events: Joined + Message + Closed.
        let j = journal.clone();
        wait_until(|| j.events.lock().unwrap().len() >= 3).await;

        shutdown_and_join(coord, pump).await;

        let events = journal.events.lock().unwrap().clone();
        let mut joined = 0;
        let mut messages = 0;
        let mut closed = 0;
        let mut last_offset: i64 = -1;
        for ev in &events {
            match ev {
                ExecEvent::BusJoined { offset, .. } => {
                    joined += 1;
                    assert!(*offset as i64 > last_offset);
                    last_offset = *offset as i64;
                }
                ExecEvent::BusMessage { offset, .. } => {
                    messages += 1;
                    assert!(*offset as i64 > last_offset);
                    last_offset = *offset as i64;
                }
                ExecEvent::BusClosed { offset, .. } => {
                    closed += 1;
                    assert!(*offset as i64 > last_offset);
                    last_offset = *offset as i64;
                }
                _ => {}
            }
        }
        assert!(joined >= 1, "at least one Joined journaled");
        assert!(messages >= 1, "at least one Message journaled");
        assert_eq!(closed, 1, "Closed emitted at shutdown");
    }

    /// On a journal write failure, the affected bus is marked
    /// `journal_degraded`; the next `send` returns
    /// `SendError::JournalDegraded`. After `clear_journal_degraded`
    /// (or a successful pump batch), subsequent sends return `Ok`.
    #[tokio::test]
    async fn pump_failure_surfaces_journal_degraded_on_next_send() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new();
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);

        let mut bus = new_bus(&coord, BusOptions::default());
        bus.register("alice").unwrap();
        // Inject a failure on the next journal write the pump tries
        // (which will be the Joined event from the register above).
        *journal.fail_count.lock().unwrap() = 1;
        // Send something so the pump wakes and processes the tail.
        // The send itself succeeds (it's an in-RAM append); the pump
        // then fails to journal the entries and marks the bus.
        let _ = bus.send("warm", serde_json::json!("up"));
        let live_buses = coord.live_bus_inners();
        let bus_inner = live_buses[0].upgrade().unwrap();
        wait_until(|| bus_inner.is_journal_degraded()).await;
        // The next send sees the flag and errors loud.
        let err = bus.send("late", serde_json::json!("x"));
        assert!(matches!(err, Err(SendError::JournalDegraded(_))), "got {err:?}");

        // Clear the flag explicitly and confirm sends resume.
        bus.clear_journal_degraded();
        assert!(bus.send("ok", serde_json::json!("y")).is_ok());

        // Shutdown cleanly.
        bus.close();
        drop(bus);
        shutdown_and_join(coord, pump).await;
    }

    /// Ephemeral bus: the journaled BusMessage has `payload: None`
    /// AND non-zero size + hash prefix. The inspector renders the
    /// metadata; payload bytes never leave the producer's RAM.
    #[tokio::test]
    async fn ephemeral_bus_journal_carries_only_metadata_stub() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new();
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);

        let mut bus = new_bus(
            &coord,
            BusOptions {
                ephemeral: true,
                window: Some(4),
            },
        );
        bus.register("camera").unwrap();
        bus.send("frame", serde_json::json!({"px": "AAAA"})).unwrap();
        bus.close();
        drop(bus);

        let j = journal.clone();
        // 3 events: Joined + Message + Closed.
        wait_until(|| j.events.lock().unwrap().len() >= 3).await;

        shutdown_and_join(coord, pump).await;

        let events = journal.events.lock().unwrap().clone();
        let message = events
            .iter()
            .find_map(|e| match e {
                ExecEvent::BusMessage {
                    payload,
                    payload_byte_size,
                    payload_sha256_prefix,
                    ..
                } => Some((payload.clone(), *payload_byte_size, *payload_sha256_prefix)),
                _ => None,
            })
            .expect("BusMessage journaled");
        assert!(
            matches!(message.0, weft_core::primitive::JournaledPayload::Ephemeral),
            "ephemeral payload must be tagged Ephemeral in journal"
        );
        assert!(message.1 > 0, "byte size must be populated");
        assert_ne!(message.2, [0u8; 8], "hash prefix must be populated");
    }

    /// Once every participant handle drops AND `coord.shutdown()`
    /// releases the coordinator's `Arc<BusInner>` refs, the bus's
    /// `Weak<BusInner>` refs in the registry fail to upgrade. This is
    /// the no-leak property: a fully-closed bus is freed.
    #[tokio::test]
    async fn weak_only_registry_collects_bus_when_handles_drop() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new();
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);

        let weak_after_drop = {
            let bus = new_bus(&coord, BusOptions::default());
            let weak = std::sync::Arc::downgrade(&bus.inner_arc());
            bus.close();
            drop(bus);
            weak
        };
        // Snapshot the weak ref BEFORE shutdown so we can verify
        // post-shutdown collection. shutdown() drains then releases
        // the coordinator's Arc; no other Arc remains.
        coord.shutdown(std::time::Duration::from_secs(2)).await;
        drop(coord);
        assert!(
            weak_after_drop.upgrade().is_none(),
            "weak ref should fail to upgrade after shutdown + handle drop"
        );

        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), pump).await;
    }

    /// Backstop: if the coordinator is dropped WITHOUT calling
    /// `shutdown()` (e.g. a panic unwind in the loop driver), the
    /// pump must still exit cleanly. The mechanism: `Drop for
    /// BusCoordinator` sets `pump_should_exit` AND fires
    /// `journal_pump_notify`. The pump's parked `notified.await`
    /// returns; on the next iteration `coordinator.upgrade()` is
    /// None and the None-branch sets `should_exit = true`; the pump
    /// returns. Without the Drop firing the notify, the pump would
    /// stay parked forever even though the upgrade would correctly
    /// return None.
    #[tokio::test]
    async fn pump_exits_when_coordinator_dropped_without_shutdown() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new();
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);
        // Briefly let the pump start its first iteration so it has
        // observed the live coordinator at least once.
        tokio::task::yield_now().await;
        // Drop the coordinator directly. No shutdown call. The
        // `Drop for BusCoordinator` impl fires the pump notify and
        // sets the exit flag; the pump's notified.await wakes, the
        // next iteration's upgrade returns None, the pump exits.
        drop(coord);
        // Bound the wait so a regression (orphaned pump) fails fast
        // instead of hanging the suite.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), pump).await;
        assert!(
            outcome.is_ok(),
            "pump must exit within 2s when coordinator is dropped without shutdown"
        );
    }

    // ───────────────────────────────────────────────────────────────
    // Node-liveness deadlock detection. These drive the `BusLiveness`
    // hooks directly (the same calls the wait loops make via WaitGuard)
    // so the stuck-check's decision is tested deterministically, with no
    // scheduler races. `deadlock_provable(in_flight)` is the predicate
    // the driver gates `close_all()` on.
    // ───────────────────────────────────────────────────────────────

    /// A node parked on a bus, caught up on its generation, with one
    /// in-flight task, is a provable deadlock. The baseline the harder
    /// cases below must NOT trip.
    #[test]
    fn single_parked_caught_up_node_is_deadlock() {
        let coord = BusCoordinator::new();
        let bus = new_bus_for(&coord, "a").inner_arc();
        let a = participant("a");
        let w = coord.enter_wait(&a, &bus);
        coord.observed(&a, w); // sees generation 0 (no appends)
        coord.parked(&a, w);
        assert!(
            coord.deadlock_provable(1),
            "one in-flight task, parked and caught up: deadlock"
        );
    }

    /// THE CORE FIX, at the map level. A single node holds TWO concurrent
    /// bus waits (a body that `select!`s over two cursors). It is one
    /// async task = ONE in-flight task = ONE map entry, even with two
    /// waits. `nodes_len()` proves the collapse directly; the node counts
    /// as parked only when BOTH waits are parked (a select! with one
    /// branch live is still working), and contributes exactly one toward
    /// `in_flight`. The old per-waiter count saw two waiters and needed
    /// `>= in_flight`, which a second wait could inflate.
    #[test]
    fn one_node_two_concurrent_waits_count_once() {
        let coord = BusCoordinator::new();
        let bus_x = new_bus_for(&coord, "a").inner_arc();
        let bus_y = new_bus_for(&coord, "a").inner_arc();
        let a = participant("a");
        // Same node enters TWO waits (e.g. select! over two cursors).
        let wx = coord.enter_wait(&a, &bus_x);
        let wy = coord.enter_wait(&a, &bus_y);
        assert_eq!(
            coord.nodes_len(),
            1,
            "two concurrent waits from one node = one map entry"
        );
        // Only one branch parked: the task is still working (the other
        // branch may resolve), so NOT counted as parked.
        coord.observed(&a, wx);
        coord.parked(&a, wx);
        assert_eq!(
            coord.parked_nodes_count(),
            0,
            "one wait parked, the other mid-evaluation: node still working"
        );
        assert!(!coord.deadlock_provable(1));
        // Both branches parked and caught up: now the node is parked, and
        // counts as exactly ONE in-flight task.
        coord.observed(&a, wy);
        coord.parked(&a, wy);
        assert_eq!(coord.parked_nodes_count(), 1, "both waits parked: node parked");
        assert!(
            coord.deadlock_provable(1),
            "one in-flight task fully parked despite holding two waits"
        );
    }

    /// A node parked on bus X while another node keeps bus Y live: the
    /// busy node is an in-flight task that is not parked, so the parked
    /// count falls short of `in_flight` and the close is suppressed. The
    /// multi-node "one bus stuck, another still working" case. A computing
    /// task simply has no map entry (liveness is created on `enter_wait`,
    /// not on registration), so `in_flight` alone accounts for it.
    #[test]
    fn computing_task_keeps_bus_alive() {
        let coord = BusCoordinator::new();
        let bus = new_bus_for(&coord, "a").inner_arc();
        let a = participant("a");
        let b = participant("b");
        // 'a' parked; 'b' is an in-flight task off computing (no entry).
        let wa = coord.enter_wait(&a, &bus);
        coord.observed(&a, wa);
        coord.parked(&a, wa);
        assert_eq!(coord.nodes_len(), 1, "only the parked node has an entry");
        assert!(
            !coord.deadlock_provable(2),
            "the computing task ('b') is an in-flight task not parked: no close"
        );
        // Once 'b' also parks caught-up, the deadlock becomes provable.
        let wb = coord.enter_wait(&b, &bus);
        coord.observed(&b, wb);
        coord.parked(&b, wb);
        assert!(
            coord.deadlock_provable(2),
            "both nodes parked and caught up: deadlock"
        );
    }

    /// A node woken by a send but still unpolled reads as BEHIND its
    /// bus's generation, so it is excluded from the parked-caught-up set
    /// and the close is suppressed. Model it: the node parked at
    /// generation 0, then an append bumped the bus to generation 1
    /// WITHOUT the node re-observing. `deadlock_provable` reads the
    /// bus's settled generation (1) > observed (0) -> not caught up.
    #[test]
    fn parked_node_behind_generation_is_not_deadlock() {
        let coord = BusCoordinator::new();
        // The sender is a DIFFERENT node 'b' (not the waiter), so the
        // test models a real two-party exchange, not a node sending to
        // itself.
        let mut producer = new_bus_for(&coord, "b");
        let bus = producer.inner_arc();
        let a = participant("a");
        let w = coord.enter_wait(&a, &bus);
        coord.observed(&a, w); // generation 0
        coord.parked(&a, w);
        // A message lands (generation bumps) but 'a' has not re-observed.
        producer.register("producer").unwrap();
        producer.send("m", serde_json::json!(1)).unwrap();
        assert!(
            !coord.deadlock_provable(1),
            "parked node behind the bus generation has unconsumed input: alive"
        );
        // After re-observing the new generation and re-parking, it is a
        // deadlock again (nothing further will arrive).
        coord.observed(&a, w);
        coord.parked(&a, w);
        assert!(
            coord.deadlock_provable(1),
            "re-observed and re-parked at the current generation: deadlock"
        );
    }

    /// A node parked on TWO buses is deadlocked only if NEITHER bus has an
    /// unconsumed append: `deadlock_provable` checks EVERY wait's bus
    /// generation, not just one. Pins the multi-wait phase-2 scan: park
    /// caught-up on bus X, then append to bus Y; the node is alive
    /// because its Y-wait is behind.
    #[test]
    fn node_parked_on_two_buses_alive_if_either_has_unconsumed() {
        let coord = BusCoordinator::new();
        let bus_x = new_bus_for(&coord, "a").inner_arc();
        let mut producer_y = new_bus_for(&coord, "b");
        let bus_y = producer_y.inner_arc();
        let a = participant("a");
        let wx = coord.enter_wait(&a, &bus_x);
        let wy = coord.enter_wait(&a, &bus_y);
        // Both waits parked, both caught up at generation 0.
        coord.observed(&a, wx);
        coord.parked(&a, wx);
        coord.observed(&a, wy);
        coord.parked(&a, wy);
        assert!(coord.deadlock_provable(1), "both waits caught up: deadlock");
        // An append on bus Y (not yet observed by the Y-wait) revives the
        // node even though its X-wait is still caught up.
        producer_y.register("producer").unwrap();
        producer_y.send("m", serde_json::json!(1)).unwrap();
        assert!(
            !coord.deadlock_provable(1),
            "the Y-wait is behind its bus: the node has unconsumed input, alive"
        );
    }

    /// `exit_wait` for the last wait removes the node entry entirely (no
    /// ghost participant). After a node leaves its wait, it stops being
    /// counted; a phantom in_flight finds no parked node, so no false
    /// close. Mirrors the real handle-drop / wait-cancel path.
    #[test]
    fn last_wait_exit_removes_node_entry() {
        let coord = BusCoordinator::new();
        let bus = new_bus_for(&coord, "a").inner_arc();
        let a = participant("a");
        let w = coord.enter_wait(&a, &bus);
        coord.observed(&a, w);
        coord.parked(&a, w);
        assert_eq!(coord.parked_nodes_count(), 1);
        coord.exit_wait(&a, w);
        assert_eq!(coord.nodes_len(), 0, "node entry gone once its last wait exits");
        assert!(!coord.deadlock_provable(1), "no parked nodes: not provable");
    }

    /// Dropping a REGISTERED handle after the bus is closed must not
    /// panic or leak. The Drop path takes the registration and bails on
    /// the closed bus; with the inert participant ref-count removed,
    /// there is no `leave` to mis-pair. The node's liveness entry (if
    /// any) is governed solely by its waits. Pins that a registered
    /// handle outliving close is harmless.
    #[test]
    fn registered_handle_drop_after_close_is_clean() {
        let coord = BusCoordinator::new();
        let mut bus = new_bus_for(&coord, "a");
        bus.register("a").unwrap();
        coord.close_all();
        // No liveness entry was ever created (the node never entered a
        // wait), and dropping the registered handle on a closed bus must
        // not panic.
        assert_eq!(coord.nodes_len(), 0);
        drop(bus);
        assert_eq!(coord.nodes_len(), 0, "no ghost entry from a post-close drop");
    }

    /// Parallel-loop lanes: the SAME node body running at two different
    /// loop frames is two DISTINCT participants, keyed by `(node_id,
    /// frames)`. One lane deadlocked while the other is still computing
    /// must NOT close the buses. A node-id-only key would conflate the
    /// lanes into one entry and the second-half count (`== 2`) would
    /// fail, so this catches a frames-ignoring regression.
    #[test]
    fn parallel_loop_lanes_are_independent_participants() {
        use weft_core::frames::LoopIteration;
        let coord = BusCoordinator::new();
        // Both lanes share a node id "worker" but differ in frame index.
        let lane0 = ("worker".to_string(), vec![LoopIteration { index: 0 }]);
        let lane1 = ("worker".to_string(), vec![LoopIteration { index: 1 }]);
        let bus0 = coord
            .new_bus(BusOptions::default(), lane0.clone())
            .unwrap()
            .inner_arc();
        let bus1 = coord
            .new_bus(BusOptions::default(), lane1.clone())
            .unwrap()
            .inner_arc();
        // Lane 0 parks (deadlocked). Lane 1 is an in-flight task still
        // computing (no wait yet).
        let w0 = coord.enter_wait(&lane0, &bus0);
        coord.observed(&lane0, w0);
        coord.parked(&lane0, w0);
        assert_eq!(
            coord.nodes_len(),
            1,
            "lanes are distinct entries: only lane 0 has parked"
        );
        assert!(
            !coord.deadlock_provable(2),
            "lane 1 still computing keeps the buses alive"
        );
        // Lane 1 parks too: now both lanes are stuck. If frames were
        // ignored, lane1's enter_wait would land on lane0's entry and
        // parked_nodes_count would read 1, failing this assert.
        let w1 = coord.enter_wait(&lane1, &bus1);
        coord.observed(&lane1, w1);
        coord.parked(&lane1, w1);
        assert_eq!(coord.parked_nodes_count(), 2, "two distinct parked lanes");
        assert!(
            coord.deadlock_provable(2),
            "both lanes parked and caught up on their own buses: deadlock"
        );
    }

    /// A not-yet-parked wait (mid-evaluation: `observed` fired but not
    /// `parked`) excludes its node, even if caught up, because the
    /// evaluation may resolve. Pins the parked flag's role.
    #[test]
    fn mid_evaluation_node_suppresses_close() {
        let coord = BusCoordinator::new();
        let bus = new_bus_for(&coord, "a").inner_arc();
        let a = participant("a");
        let w = coord.enter_wait(&a, &bus);
        coord.observed(&a, w); // caught up, but NOT parked
        assert!(
            !coord.deadlock_provable(1),
            "a mid-evaluation wait may resolve; its node is excluded from the parked count"
        );
        coord.parked(&a, w);
        assert!(coord.deadlock_provable(1), "now parked: deadlock");
    }

    /// REGRESSION (round-2 critical). When a node holds two concurrent
    /// waits both parked, and ONE resolves (its guard drops -> exit_wait),
    /// the node's task is provably running, so the surviving sibling's
    /// `parked` flag must be CLEARED. Otherwise the node would read as
    /// fully-parked in the window between the resolve and the task being
    /// polled, and `deadlock_provable` could close buses under the
    /// resolving branch's follow-up send. Pins that exit_wait clears
    /// siblings: after one wait exits, the node is NOT a deadlock until
    /// the surviving wait re-parks.
    #[test]
    fn exit_wait_clears_surviving_sibling_parked_flag() {
        let coord = BusCoordinator::new();
        let bus_x = new_bus_for(&coord, "a").inner_arc();
        let bus_y = new_bus_for(&coord, "a").inner_arc();
        let a = participant("a");
        let wx = coord.enter_wait(&a, &bus_x);
        let wy = coord.enter_wait(&a, &bus_y);
        // Both waits parked and caught up: the node is fully parked.
        coord.observed(&a, wx);
        coord.parked(&a, wx);
        coord.observed(&a, wy);
        coord.parked(&a, wy);
        assert!(coord.deadlock_provable(1), "both waits parked: deadlock");
        // The wx branch resolves (its guard drops). The task is now
        // running toward that branch's code; wy's parked flag is stale.
        coord.exit_wait(&a, wx);
        assert!(
            !coord.deadlock_provable(1),
            "sibling wy's parked flag cleared: node is running, not parked, no false close"
        );
        // The task re-parks wy (re-evaluated, still nothing): deadlock
        // again, correctly.
        coord.observed(&a, wy);
        coord.parked(&a, wy);
        assert!(
            coord.deadlock_provable(1),
            "wy re-parked caught-up: genuine deadlock re-proven"
        );
    }

    /// The pairing asserts are load-bearing crash-loud contracts: an
    /// `exit_wait` for a wait id that was never entered is a WaitGuard
    /// pairing bug and must panic, not silently no-op (a silent miss
    /// would corrupt the parked accounting).
    #[test]
    #[should_panic(expected = "exit_wait for unknown node")]
    fn exit_wait_unknown_node_panics() {
        let coord = BusCoordinator::new();
        coord.exit_wait(&participant("ghost"), 1);
    }

    #[test]
    #[should_panic(expected = "observed for unknown wait")]
    fn observed_unknown_wait_panics() {
        let coord = BusCoordinator::new();
        let bus = new_bus_for(&coord, "a").inner_arc();
        let a = participant("a");
        let w = coord.enter_wait(&a, &bus);
        // A different (never-minted) id must not address this node's wait.
        coord.observed(&a, w + 999);
    }
}
