//! The engine's shared in-process wait tracker: every construct that
//! parks a node task while the worker stays alive (a bus cursor or
//! `wait_for`, a generator pull, a producer waiting for an emission's
//! delivery) registers its waits here, so the drive loop's stuck-check
//! has ONE picture of "which in-flight tasks are parked, and on what".
//! A second private tracker would let the detector see half the waits
//! and either hang the execution or close it under live work.
//!
//! Vocabulary: this code uses "wait" everywhere, not "park". The word
//! "park" already names `ctx.await_signal` (journal-replay workflow
//! suspension, worker swap). These waits are plain in-process tokio
//! awaits; the worker stays alive; no swap.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use tokio::sync::Notify;
use uuid::Uuid;

use weft_core::error::{WeftError, WeftResult};
use weft_core::liveness::{wait_on, FiringLocation, WaitId, WaitLiveness, WaitSource};

/// One node execution's wait state in the tracker's liveness map: the
/// set of waits this node is currently inside, keyed by `WaitId`. A
/// node is one async task but CAN hold several concurrent waits (a body
/// that `select!`s or `join!`s over two cursors), so `waits` is a map,
/// not a single slot. An entry exists exactly while the node has >= 1
/// live wait; it is removed when its last wait exits. The node counts
/// as "parked" for the deadlock check only when EVERY wait in `waits`
/// is parked-and-caught-up (a task with any branch still live or mid-
/// evaluation is still working). Plain fields: every read and write
/// happens under the liveness mutex.
struct NodeWaitState {
    waits: HashMap<WaitId, WaitState>,
}

/// One wait. `source` is what it is parked on (so the stuck-check can
/// read that source's settled generation, see `deadlock_provable`);
/// `observed` is the highest generation seen when this wait last
/// (re-)evaluated its condition (`observed` hook); `parked` is whether
/// it is at its true park point (`parked` hook; cleared by every
/// `observed` because an evaluation in progress may resolve instead of
/// parking). Lives from `enter_wait` to `exit_wait` (RAII via
/// `WaitGuard` in weft-core).
struct WaitState {
    source: Weak<dyn WaitSource>,
    observed: u64,
    parked: bool,
}

/// Per-execution wait tracker, owned by the execution's drive and
/// shared with every construct that parks node tasks in-process (the
/// bus coordinator, the stream runtime's feeds, the delivery gates).
pub struct WaitTracker {
    /// Wake the loop's idle-`select!` when a node's wait state changes
    /// (enters/leaves a wait, observes/parks, or a source event fires).
    /// The loop doesn't care WHO changed; it just needs to re-check
    /// stuck.
    wait_notify: Notify,
    /// Per-NODE-EXECUTION wait liveness, keyed by `(node_id, frames)`.
    /// Each entry holds the node's currently-live waits (keyed by
    /// `WaitId`, because one task can `select!`/`join!` over several at
    /// once). A node execution in a parallel loop has one entry per
    /// lane (distinct frames), so lanes never conflate. Read by
    /// `deadlock_provable`: the execution is stuck only when every
    /// in-flight node task is parked here with EVERY one of its waits
    /// at its true `notified.await` AND caught up on that wait's
    /// source's CURRENT generation. A node that is computing (no map
    /// entry, or an entry with any wait not parked) keeps everything
    /// alive: it might still send. A node woken by a source event but
    /// still unpolled has not re-observed since the event, so that wait
    /// reads as behind; a wait mid-evaluation reads as not-parked.
    /// Either way the close is suppressed under it, by construction
    /// rather than by a scheduler-fairness bet.
    nodes: Mutex<HashMap<FiringLocation, NodeWaitState>>,
    /// Mint for `WaitId`s. Plain monotone counter; ids are never reused
    /// within an execution, so a stale `exit_wait` can never address a
    /// later wait.
    next_wait_id: AtomicU64,
}

impl WaitTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            wait_notify: Notify::new(),
            nodes: Mutex::new(HashMap::new()),
            next_wait_id: AtomicU64::new(0),
        })
    }

    /// Wait-wake-up future the loop awaits in its idle-`select!`.
    pub fn wait_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.wait_notify.notified()
    }

    /// How many node executions are currently fully parked (every one
    /// of their concurrent waits at its true park point). Logging /
    /// diagnostics only: the stuck-check reads the map inside
    /// `deadlock_provable`'s locked snapshot, never through this.
    pub fn parked_nodes_count(&self) -> usize {
        self.lock_nodes()
            .values()
            .filter(|s| Self::node_fully_parked(s))
            .count()
    }

    /// A node counts as parked for the deadlock check only when it
    /// holds at least one wait AND EVERY one of its concurrent waits is
    /// at its true park point. A task `select!`ing over two cursors
    /// with one branch still mid-evaluation (or not yet parked) is
    /// still working, so it must not count. (`waits` is never empty for
    /// a live entry: the entry is removed when its last wait exits.)
    fn node_fully_parked(state: &NodeWaitState) -> bool {
        !state.waits.is_empty() && state.waits.values().all(|w| w.parked)
    }

    /// Test accessor: how many node executions currently have a
    /// liveness entry (>= 1 live wait). Lets tests assert the map shape
    /// directly (e.g. two registrations from one node collapse to one
    /// entry).
    #[cfg(test)]
    pub fn nodes_len(&self) -> usize {
        self.lock_nodes().len()
    }

    /// Lock the liveness map. Taken only on the wait / wake / leave /
    /// stuck-check paths, never on a source's message send path (a
    /// source event only bumps that source's generation and wakes the
    /// loop).
    fn lock_nodes(&self) -> MutexGuard<'_, HashMap<FiringLocation, NodeWaitState>> {
        self.nodes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// True when EVERY one of the `in_flight` live node tasks (the
    /// driver's identity set of spawned firings) is a node execution
    /// PARKED at its true `notified.await` point (not mid-evaluation)
    /// AND has observed its source's CURRENT generation at its last
    /// condition evaluation. Identity matching (not a count): a wait
    /// entry that does NOT correspond to an in-flight firing (a task a
    /// body detached and leaked past its firing's end, holding a
    /// smuggled handle) is out of contract and deliberately IGNORED
    /// here, so it can neither block stuck-detection forever nor prove
    /// it; the resolution that follows a `true` poisons every source,
    /// which fails such a zombie loudly too. A node holding several
    /// waits is ONE task and ONE map entry, so it counts once: this is
    /// the whole point of keying liveness on the node execution rather
    /// than on each transient wait.
    ///
    /// - A node behind its source's generation has an event it has not
    ///   evaluated yet (typically a woken-but-unpolled waiter sitting
    ///   in another worker thread's queue), so it is alive.
    /// - A NOT-parked node is mid-evaluation: its `observed` fired, the
    ///   evaluation has not returned, and it may be about to RESOLVE,
    ///   not park. Closing under a succeeding evaluation would tear
    ///   down live work. `parked` fires only after every pre-park
    ///   re-check failed, so `parked && caught-up` means a provably
    ///   fruitless evaluation followed by a real park.
    ///
    /// Per-source generations are read via `settled_gen` (each source
    /// reads under its own state lock; a source pushes its change
    /// BEFORE bumping the generation, both under that lock, so an
    /// unlocked read could see generation G while a change past G has
    /// already landed, a torn read that could enable a close with an
    /// unconsumed event in flight). The parked nodes' sources are
    /// snapshotted out of the liveness map FIRST and the map lock
    /// released BEFORE any source lock is taken, so this scan nests no
    /// locks in either direction.
    ///
    /// Soundness: an event landing between the map snapshot and a
    /// source's generation read makes that source's settled generation
    /// exceed its wait's observed value, so the scan returns false.
    /// A `true` is a proof about the TASKS only: every in-flight task
    /// is parked and caught up, so no task will produce a new source
    /// event. It says nothing about messages a task enqueued on the
    /// driver's task channel BEFORE parking (a take, an emission);
    /// those are invisible to the sources' generations, and applying
    /// one can resolve a wait this scan just called dead. The DRIVER
    /// therefore pairs this proof with its own "task channel is empty"
    /// check, in that order, before acting on it (see the stuck-check
    /// in `execution_driver`). Neither half alone is a proof.
    pub fn deadlock_provable(&self, in_flight: &HashSet<FiringLocation>) -> bool {
        if in_flight.is_empty() {
            return false;
        }
        // Phase 1: under ONE map lock, require every in-flight firing
        // to be fully parked, snapshotting its waits' observed
        // generations + sources.
        let snapshot: Vec<(u64, Weak<dyn WaitSource>)> = {
            let nodes = self.lock_nodes();
            let mut v = Vec::new();
            for loc in in_flight {
                let Some(state) = nodes.get(loc) else {
                    // A computing task (no wait entry) keeps everything
                    // alive: it might still send.
                    return false;
                };
                if !Self::node_fully_parked(state) {
                    return false;
                }
                for w in state.waits.values() {
                    v.push((w.observed, w.source.clone()));
                }
            }
            v
        };
        // Phase 2: map lock released; read each wait's source's settled
        // generation under that source's own lock. EVERY wait of every
        // parked node must be caught up: a node parked on two sources
        // is only deadlocked if neither has an unconsumed event.
        snapshot.into_iter().all(|(observed, source)| {
            match source.upgrade() {
                Some(source) => observed >= source.settled_gen(),
                // The wait's source dropped between the snapshot and
                // here (it left the wait concurrently). State is in
                // motion, so suppress the close; the exit's `exit_wait`
                // woke the loop for a clean re-evaluation.
                None => false,
            }
        })
    }
}

impl WaitLiveness for WaitTracker {
    fn enter_wait(&self, node: &FiringLocation, source: &Arc<dyn WaitSource>) -> WaitId {
        let id = self.next_wait_id.fetch_add(1, Ordering::AcqRel) + 1;
        // `observed: 0` registers the wait as conservatively BEHIND and
        // `parked: false` as conservatively MID-EVALUATION (both
        // suppress close) until its first `observed` / `parked`, which
        // the wait loops fire before their first park by construction.
        self.lock_nodes()
            .entry(node.clone())
            .or_insert_with(|| NodeWaitState { waits: HashMap::new() })
            .waits
            .insert(
                id,
                WaitState {
                    source: Arc::downgrade(source),
                    observed: 0,
                    parked: false,
                },
            );
        // Use `notify_one` (NOT `notify_waiters`) so a wait-start that
        // fires when the loop is NOT currently parked on
        // `wait_notified` stores a permit. The next
        // `wait_notified().enable()` consumes it and the loop wakes
        // immediately. With `notify_waiters` the notification was lost
        // in that window, and if no further wait-start arrived (because
        // all peers are now blocked), the loop slept until the harness
        // 10s deadline. Test:
        // `hole4_mutual_deadlock_when_both_wait_for_names_that_never_come`
        // flaked ~1 in 10 under parallel load before this change.
        self.wait_notify.notify_one();
        id
    }
    fn exit_wait(&self, node: &FiringLocation, id: WaitId) {
        // The `WaitGuard` lifecycle is symmetric (started on construct,
        // ended on drop), so a missing wait here is a real pairing bug.
        // Crash loud in BOTH dev and release: a silently-cleared wait
        // would flip the stuck-check (an unparked node suppresses
        // close) wrongly and the engine could hang or close early.
        //
        // CRITICAL: if this node has OTHER waits still live, the task
        // is provably RUNNING right now (it is executing this guard
        // drop, on its way to resolve a `select!`/`join!` branch and
        // run that branch's code). Its sibling waits' `parked` flags
        // are now stale (set before the task woke), and a stuck-check
        // that read them would see the node as fully parked and could
        // close sources out from under the resolving branch's follow-up
        // send. So clear every surviving wait's `parked` flag (the node
        // is not parked until every wait re-parks) AND wake each
        // sibling's source so its wait loop re-runs `observed` ->
        // re-check -> `parked` and restores a truthful flag. Without
        // the wake, a `join!` sibling that stays genuinely parked on
        // `notified.await` would never re-run and the cleared flag
        // would suppress a real deadlock forever (a hang). The sources
        // are collected under the lock and woken AFTER releasing it, so
        // no foreign code ever runs under the liveness map lock.
        let mut wake: Vec<Arc<dyn WaitSource>> = Vec::new();
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
                    // A sibling present here has a live wait, and the
                    // wait loops hold an `Arc` on their source across
                    // the whole wait, so this upgrade CANNOT fail.
                    // Crash loud if it does: a cleared-but-unwoken
                    // sibling would suppress a real deadlock forever
                    // (the hang this clear+wake prevents).
                    wake.push(w.source.upgrade().expect(
                        "wait source dropped while a sibling wait is live: the wait \
                         loops hold an Arc on their source across the whole wait",
                    ));
                }
            }
        }
        for source in wake {
            source.wake_waiters();
        }
        self.wait_notify.notify_one();
    }
    fn on_source_event(&self) {
        // A source changed (append / item / close / delivery). Wake the
        // idle `select!` so the loop re-evaluates promptly (a stored
        // permit if the loop is not currently parked, same discipline
        // as `enter_wait`). The event itself is visible to the
        // stuck-check through each source's `settled_gen` (a parked
        // wait that has not observed the new generation reads as behind
        // / alive), so no separate generation counter is needed here.
        self.wait_notify.notify_one();
    }
    fn observed(&self, node: &FiringLocation, id: WaitId) {
        let mut nodes = self.lock_nodes();
        let wait = nodes
            .get_mut(node)
            .and_then(|e| e.waits.get_mut(&id))
            .expect("observed for unknown wait: WaitGuard pairing broken");
        // Read the generation while waiting: the value is the
        // ground-truth "everything that landed up to here will be seen
        // by the condition evaluation the caller runs next" (see the
        // `WaitLiveness::observed` contract in weft-core). The unlocked
        // read (`gen_now`) is sound HERE because a lagging value only
        // makes the wait read as behind, which is conservative; the
        // close decision itself re-reads under the source's lock
        // (`deadlock_provable`).
        wait.observed = wait
            .source
            .upgrade()
            .expect(
                "wait source dropped while a node is waiting on it: the wait \
                 loops hold an Arc on their source across the whole wait",
            )
            .gen_now();
        // An evaluation is now in progress; it may RESOLVE rather than
        // park. Mark not-parked so the stuck-check cannot close sources
        // out from under a succeeding evaluation.
        wait.parked = false;
        drop(nodes);
        // A wait catching up can flip `deadlock_provable` to true; wake
        // the loop so a deadlock that just became provable resolves
        // without waiting for an unrelated event.
        self.wait_notify.notify_one();
    }
    fn parked(&self, node: &FiringLocation, id: WaitId) {
        let mut nodes = self.lock_nodes();
        let wait = nodes
            .get_mut(node)
            .and_then(|e| e.waits.get_mut(&id))
            .expect("parked for unknown wait: WaitGuard pairing broken");
        wait.parked = true;
        drop(nodes);
        // The last wait parking can flip `deadlock_provable` to true
        // (it is the final event before a deadlock is provable); wake
        // the loop so the resolution fires without waiting for an
        // unrelated event.
        self.wait_notify.notify_one();
    }
}

// ----- Emission-delivery gate ------------------------------------------

/// One acknowledged emission's wait state: the producer of a
/// `yield_downstream` call parks on this until every pulse
/// the emission created has been ABSORBED (a consumer dispatch took it,
/// or a pull took the item), or until the delivery provably cannot
/// happen (consumer skipped / finished without taking / deadlock /
/// cancel), which resolves it as a loud error instead of a forever
/// wait.
///
/// The acknowledgement condition IS pulse absorption: the engine
/// already has that exact first-class event, attached to the individual
/// value, so no second notion of "dispatched" exists beside it. The
/// gate resolves ONCE: a later un-absorb (a crashed-Running resume
/// flipping pulses back to Pending) must never re-arm it, because the
/// producer already resumed and moved on.
pub struct DeliveryGate {
    state: Mutex<GateState>,
    notify: Notify,
    /// Fast mirror of `GateState::gen` for the lock-free observe path.
    gen_mirror: AtomicU64,
}

struct GateState {
    /// `Some` once resolved (delivered, or failed with why). Resolves
    /// exactly once; later events are ignored.
    outcome: Option<Result<(), String>>,
    /// Pulse ids still awaiting absorption. Meaningful only once
    /// `armed`; the driver arms the gate when it applies the emission
    /// and learns which pulses it created.
    remaining: HashSet<Uuid>,
    /// EVERY pulse id this gate was armed with, resolved or not, so
    /// the driver can evict all of a resolved gate's map entries at
    /// once (see `StreamRuntime::on_pulses_absorbed`) instead of
    /// leaking entries that point at an already-resolved gate.
    all_ids: Vec<Uuid>,
    armed: bool,
    /// Monotone generation of state changes, for [`WaitSource`].
    gen: u64,
}

impl DeliveryGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState {
                outcome: None,
                remaining: HashSet::new(),
                all_ids: Vec::new(),
                armed: false,
                gen: 0,
            }),
            notify: Notify::new(),
            gen_mirror: AtomicU64::new(0),
        })
    }

    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Resolve under an already-held lock; first resolution wins.
    ///
    /// Unlike the bus and the generator feed, this source fires no
    /// `WaitLiveness::on_source_event` here, on purpose: a gate only
    /// resolves from driver-side code (absorbs and fails happen inside
    /// the drive loop), so the driver is awake by construction and a
    /// self-ping would be noise. The generation bump still makes a
    /// parked producer read as behind until it re-evaluates.
    fn resolve_locked(&self, st: &mut GateState, outcome: Result<(), String>) {
        if st.outcome.is_some() {
            return;
        }
        st.outcome = Some(outcome);
        st.gen += 1;
        self.gen_mirror.store(st.gen, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// The driver applied the emission: these are the pulses it
    /// created. An emission that created none (unwired ports, or every
    /// pulse deduped against an identical pending value) resolves
    /// delivered immediately. Arming happens BEFORE any absorb can
    /// reach the gate (the driver arms and registers the gate in one
    /// step, and absorbs only find gates through that registration);
    /// `pulse_absorbed` enforces the ordering loudly.
    pub fn arm(&self, pulse_ids: impl IntoIterator<Item = Uuid>) {
        let mut st = self.lock();
        // Record the tracked set UNCONDITIONALLY, even on a gate that
        // was failed before arming: `tracked_ids` drives the map
        // eviction (`StreamRuntime::on_pulses_absorbed` drops every
        // entry of a resolved gate at once), and an empty set on a
        // fail-before-arm gate would leave its other entries pointing
        // at a settled gate forever.
        st.remaining = pulse_ids.into_iter().collect();
        st.all_ids = st.remaining.iter().copied().collect();
        st.armed = true;
        if st.outcome.is_some() {
            // Already resolved (failed before arming): the outcome
            // stands; nothing is awaited any more.
            st.remaining.clear();
            return;
        }
        if st.remaining.is_empty() {
            self.resolve_locked(&mut st, Ok(()));
        }
    }

    /// Every pulse id this gate tracks (armed set, resolved or not).
    /// Empty before arming.
    pub fn tracked_ids(&self) -> Vec<Uuid> {
        self.lock().all_ids.clone()
    }

    /// One tracked pulse was absorbed. Resolves delivered when the last
    /// one lands. Returns true when the gate is now resolved (so the
    /// driver drops its bookkeeping for every tracked id).
    pub fn pulse_absorbed(&self, id: Uuid) -> bool {
        let mut st = self.lock();
        if st.outcome.is_some() {
            return true;
        }
        // An absorb can only reach a gate through the driver's gate
        // map, which is populated in the same step that arms it; an
        // unarmed absorb would silently orphan the id (it would be
        // re-inserted by `arm` and never absorbed again: a hang), so
        // it is an engine pairing bug and crashes loud.
        assert!(
            st.armed,
            "pulse_absorbed on an unarmed DeliveryGate: the driver must arm the gate \
             in the same step that registers it"
        );
        st.remaining.remove(&id);
        if st.remaining.is_empty() {
            self.resolve_locked(&mut st, Ok(()));
        }
        st.outcome.is_some()
    }

    /// The delivery can never happen (consumer skipped, consumer
    /// finished without taking the item, deadlock, bad-shape emission,
    /// cancel): resolve as the given error. A no-op after resolution.
    pub fn fail(&self, reason: String) {
        let mut st = self.lock();
        self.resolve_locked(&mut st, Err(reason));
    }

    fn outcome(&self) -> Option<Result<(), String>> {
        self.lock().outcome.clone()
    }

    /// Park the producer's task until the gate resolves, registered
    /// with the shared wait tracker so the drive loop's stuck-check
    /// sees the wait (and can fail it on a proven deadlock instead of
    /// hanging). Runs the same shared wait protocol
    /// (`liveness::wait_on`) as the bus and generator wait loops.
    pub async fn wait_delivered(
        self: &Arc<Self>,
        liveness: &Weak<dyn WaitLiveness>,
        node: FiringLocation,
    ) -> WeftResult<()> {
        let source = self.clone() as Arc<dyn WaitSource>;
        let outcome = wait_on(liveness, &Some(node), &source, || self.outcome()).await;
        outcome.map_err(|e| {
            WeftError::NodeExecution(format!("the emission was not delivered: {e}"))
        })
    }
}

impl WaitSource for DeliveryGate {
    fn gen_now(&self) -> u64 {
        self.gen_mirror.load(Ordering::Acquire)
    }
    fn settled_gen(&self) -> u64 {
        self.lock().gen
    }
    fn wake_waiters(&self) {
        self.notify.notify_waiters();
    }
    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Dumb fake wait source: a hand-bumped generation + a Notify.
    struct FakeSource {
        gen: AtomicU64,
        notify: Notify,
    }

    impl FakeSource {
        fn new() -> Arc<Self> {
            Arc::new(Self { gen: AtomicU64::new(0), notify: Notify::new() })
        }
        fn bump(&self) {
            self.gen.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl WaitSource for FakeSource {
        fn gen_now(&self) -> u64 {
            self.gen.load(Ordering::Acquire)
        }
        fn settled_gen(&self) -> u64 {
            self.gen.load(Ordering::Acquire)
        }
        fn wake_waiters(&self) {
            self.notify.notify_waiters();
        }
        fn notified(&self) -> tokio::sync::futures::Notified<'_> {
            self.notify.notified()
        }
    }

    fn loc(name: &str) -> FiringLocation {
        FiringLocation::new(name, Vec::new())
    }

    fn in_flight(locs: &[&str]) -> HashSet<FiringLocation> {
        locs.iter().map(|n| loc(n)).collect()
    }

    #[test]
    fn deadlock_needs_every_in_flight_firing_parked_and_caught_up() {
        let tracker = WaitTracker::new();
        let src = FakeSource::new();
        let source = src.clone() as Arc<dyn WaitSource>;
        let a = loc("a");
        let id = tracker.enter_wait(&a, &source);

        // Registered but neither observed nor parked: alive.
        assert!(!tracker.deadlock_provable(&in_flight(&["a"])));
        tracker.observed(&a, id);
        assert!(!tracker.deadlock_provable(&in_flight(&["a"])), "mid-evaluation is alive");
        tracker.parked(&a, id);
        assert!(tracker.deadlock_provable(&in_flight(&["a"])));

        // A second in-flight firing with NO wait entry is computing:
        // the identity check must refuse, whatever the counts say.
        assert!(!tracker.deadlock_provable(&in_flight(&["a", "b"])));

        tracker.exit_wait(&a, id);
        assert!(!tracker.deadlock_provable(&in_flight(&["a"])));
    }

    #[test]
    fn a_zombie_wait_outside_in_flight_cannot_block_detection() {
        // A task a body detached keeps a wait entry alive after its
        // firing ended. Identity matching ignores it: the remaining
        // in-flight firing still proves its own deadlock.
        let tracker = WaitTracker::new();
        let src = FakeSource::new();
        let source = src.clone() as Arc<dyn WaitSource>;

        let zombie = loc("zombie");
        let zid = tracker.enter_wait(&zombie, &source);
        tracker.observed(&zombie, zid);
        // The zombie never parks (it is mid-evaluation forever, the
        // worst case for a count-based check).

        let a = loc("a");
        let id = tracker.enter_wait(&a, &source);
        tracker.observed(&a, id);
        tracker.parked(&a, id);
        assert!(
            tracker.deadlock_provable(&in_flight(&["a"])),
            "a stray entry outside the in-flight set must not poison the check"
        );
        tracker.exit_wait(&a, id);
        tracker.exit_wait(&zombie, zid);
    }

    #[test]
    fn an_unconsumed_source_event_suppresses_the_close() {
        let tracker = WaitTracker::new();
        let src = FakeSource::new();
        let source = src.clone() as Arc<dyn WaitSource>;
        let a = loc("a");
        let id = tracker.enter_wait(&a, &source);
        tracker.observed(&a, id);
        tracker.parked(&a, id);
        // An event lands AFTER the last observation: the wait is
        // behind, so the close is suppressed until it re-observes.
        src.bump();
        assert!(!tracker.deadlock_provable(&in_flight(&["a"])));
        tracker.observed(&a, id);
        tracker.parked(&a, id);
        assert!(tracker.deadlock_provable(&in_flight(&["a"])));
        tracker.exit_wait(&a, id);
    }

    #[test]
    fn a_node_with_two_waits_parks_only_when_both_park() {
        let tracker = WaitTracker::new();
        let src = FakeSource::new();
        let source = src.clone() as Arc<dyn WaitSource>;
        let a = loc("a");
        let w1 = tracker.enter_wait(&a, &source);
        let w2 = tracker.enter_wait(&a, &source);
        assert_eq!(tracker.nodes_len(), 1, "two waits collapse under one node entry");
        tracker.observed(&a, w1);
        tracker.parked(&a, w1);
        tracker.observed(&a, w2);
        assert!(!tracker.deadlock_provable(&in_flight(&["a"])), "one wait mid-evaluation");
        tracker.parked(&a, w2);
        assert!(tracker.deadlock_provable(&in_flight(&["a"])));
        // Exiting one wait clears the sibling's parked flag: the task
        // is provably running (it is executing the exit).
        tracker.exit_wait(&a, w2);
        assert!(!tracker.deadlock_provable(&in_flight(&["a"])));
        tracker.exit_wait(&a, w1);
    }

    #[test]
    fn empty_in_flight_is_never_a_deadlock() {
        let tracker = WaitTracker::new();
        assert!(!tracker.deadlock_provable(&HashSet::new()));
    }

    #[test]
    #[should_panic(expected = "pairing broken")]
    fn exit_without_enter_crashes_loud() {
        let tracker = WaitTracker::new();
        tracker.exit_wait(&loc("a"), 42);
    }

    #[test]
    fn gate_resolves_when_every_armed_pulse_absorbs() {
        let gate = DeliveryGate::new();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        gate.arm([a, b]);
        assert!(!gate.pulse_absorbed(a));
        assert!(gate.pulse_absorbed(b));
        assert_eq!(gate.outcome(), Some(Ok(())));
        assert_eq!(gate.tracked_ids().len(), 2);
    }

    #[test]
    fn gate_armed_empty_resolves_immediately() {
        let gate = DeliveryGate::new();
        gate.arm([]);
        assert_eq!(gate.outcome(), Some(Ok(())));
    }

    #[test]
    fn gate_fail_wins_once_and_later_events_are_ignored() {
        let gate = DeliveryGate::new();
        let a = Uuid::new_v4();
        gate.arm([a]);
        gate.fail("consumer skipped".into());
        assert_eq!(gate.outcome(), Some(Err("consumer skipped".into())));
        // Later absorb / fail are no-ops on the resolved outcome.
        assert!(gate.pulse_absorbed(a));
        gate.fail("later".into());
        assert_eq!(gate.outcome(), Some(Err("consumer skipped".into())));
    }

    #[test]
    fn gate_fail_before_arm_stands() {
        // A bad-shape emission fails at apply time, before any pulse
        // exists; the arm that never happens must not overwrite it.
        let gate = DeliveryGate::new();
        gate.fail("bad shape".into());
        gate.arm([Uuid::new_v4()]);
        assert_eq!(gate.outcome(), Some(Err("bad shape".into())));
    }

    #[test]
    #[should_panic(expected = "unarmed DeliveryGate")]
    fn gate_absorb_before_arm_crashes_loud() {
        let gate = DeliveryGate::new();
        gate.pulse_absorbed(Uuid::new_v4());
    }

    weft_core::stress_test!(
        name: gate_wait_delivered_wakes_on_resolution,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let gate = DeliveryGate::new();
            let a = Uuid::new_v4();
            gate.arm([a]);
            let waiter = {
                let gate = gate.clone();
                tokio::spawn(async move {
                    gate.wait_delivered(&weft_core::liveness::no_liveness(),
                        FiringLocation::new("p", Vec::new())).await
                })
            };
            // Resolution lands before, during, or after the park under
            // the stress runs; every ordering must wake the waiter.
            assert!(gate.pulse_absorbed(a));
            waiter.await.unwrap().unwrap();
        }
    );
}
