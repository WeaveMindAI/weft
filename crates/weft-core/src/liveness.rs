//! In-process wait liveness: how a node task parked on a live channel
//! stays visible to the engine's stuck-detector.
//!
//! Several constructs park a node's task in-process while the worker
//! stays alive: a bus cursor's `next().await`, a bus `wait_for`, a
//! generator consumer's pull, a producer waiting for an emission to be
//! taken. The engine must be able to tell "every in-flight task is
//! parked with nothing left to consume" (a real deadlock it should
//! resolve loudly) from "one task waits while another still computes"
//! (still working). This module is the shared vocabulary for that:
//!
//! - [`FiringLocation`] identifies the node EXECUTION doing the waiting.
//! - [`WaitSource`] is the thing being waited on (a bus, a stream, an
//!   emission's delivery), reduced to the one fact the stuck-check
//!   needs: a monotone generation of its state changes.
//! - [`WaitLiveness`] is the engine-side tracker every wait reports to.
//! - [`wait_on`] is THE wait loop; it holds a private RAII guard so
//!   entry and exit can never unbalance, even across cancellation.
//!
//! ## "wait" vs "park" vocabulary
//!
//! The word "park" in this codebase already names `ctx.await_signal`
//! (journal-replay workflow suspension, worker swap). The waits here
//! are plain in-process tokio awaits; the worker stays alive. The
//! engine only needs the hooks so its stuck-check can prove a deadlock
//! instead of hanging forever.

use std::sync::{Arc, Weak};

/// Identity of one node EXECUTION: the node id plus its loop-frame
/// stack. A loop running the same node body N times in parallel is N
/// distinct executions, one per frame, each with its own wait liveness.
/// This is the key the engine's stuck-check uses to tell "this lane is
/// waiting forever" from "this lane is still computing".
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FiringLocation {
    pub node_id: String,
    pub frames: crate::frames::LoopFrames,
}

impl FiringLocation {
    pub fn new(node_id: impl Into<String>, frames: crate::frames::LoopFrames) -> Self {
        Self { node_id: node_id.into(), frames }
    }
}

/// A single wait's identity within the engine's liveness map. One node
/// execution can hold SEVERAL concurrent waits at once (a body that
/// `tokio::select!`s or `join!`s over two cursors, or two `wait_for`s),
/// each its own guard; the id keeps their parked/observed state
/// separate under the one node entry. Minted by `enter_wait`;
/// meaningless outside an engine (a source built with [`no_liveness`]
/// never mints one, its guard short-circuits to a no-op).
pub type WaitId = u64;

/// The thing a wait is parked ON, reduced to what the stuck-check
/// needs. Every implementor keeps a monotone GENERATION of its state
/// changes (a bus append, a stream item arriving or closing, a
/// delivery resolving), bumped under the same lock that lands the
/// change, so "this wait observed generation G" provably means "its
/// condition evaluation saw every change up to G".
pub trait WaitSource: Send + Sync {
    /// Fast, possibly-lagging generation read (no lock required). Safe
    /// on the observe path only: a lagging value makes the wait read
    /// as behind, which conservatively suppresses the close.
    fn gen_now(&self) -> u64;
    /// Exact generation read under the source's own state lock. The
    /// close decision reads this, so a change landing concurrently is
    /// never missed.
    fn settled_gen(&self) -> u64;
    /// Wake every waiter parked on this source so its wait loop re-runs
    /// observe -> evaluate -> park and restores a truthful parked flag.
    fn wake_waiters(&self);
    /// Arm one waiter slot on this source's notifier. [`wait_on`] arms
    /// it BEFORE evaluating the wait condition, so a state change that
    /// lands mid-evaluation always wakes the parked await (no lost
    /// wakeup by construction).
    fn notified(&self) -> tokio::sync::futures::Notified<'_>;
}

/// THE wait protocol every in-process wait runs, defined once so a
/// missing re-check or an untruthful parked flag cannot creep into one
/// copy. `evaluate` checks the wait condition under the source's own
/// lock and returns `Some` when the wait resolves.
///
/// Shape per iteration: arm the notifier FIRST (a change landing after
/// the arm always wakes the await), then record observed, evaluate,
/// and only park when the evaluation did not resolve. The unguarded
/// first evaluation is a fast path: a condition that already holds
/// resolves without ever registering a wait with the engine.
///
/// `node` is the waiting FIRING's identity. Every wait that belongs to
/// an in-flight node execution MUST carry it. Today every
/// engine-attached source does; `None` is only ever paired with
/// [`no_liveness`] (standalone rigs and tests). A live `liveness` with
/// `node: None` would make the wait invisible to the stuck-check, so
/// never construct that pairing for a firing's wait.
pub async fn wait_on<T>(
    liveness: &Weak<dyn WaitLiveness>,
    node: &Option<FiringLocation>,
    source: &Arc<dyn WaitSource>,
    mut evaluate: impl FnMut() -> Option<T>,
) -> T {
    if let Some(v) = evaluate() {
        return v;
    }
    let guard = WaitGuard::new(liveness, node, source);
    loop {
        let notified = source.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        guard.record_observed();
        if let Some(v) = evaluate() {
            return v;
        }
        guard.record_parked();
        notified.await;
    }
}

/// Node-liveness hooks the engine wires up so its stuck-check can tell
/// "every waiting node is parked forever" (real deadlock) from "one
/// node waits while another still computes" (still working).
///
/// Liveness is attached to the NODE EXECUTION (`(node_id, frames)`),
/// not to each wait: the unit that is "computing" or "waiting" is the
/// async task, and a node holding several waits is still one task. A
/// task CAN be inside several waits at once (select!/join! over two
/// cursors), so each wait is tracked under its own [`WaitId`]; the node
/// counts as parked for the deadlock check only when EVERY one of its
/// concurrent waits is parked-and-caught-up (a select! task with any
/// branch still live or mid-evaluation is still working). Paired via
/// RAII inside [`wait_on`]: every `enter_wait` is followed by exactly
/// one `exit_wait` for the same id, even if the awaiting future is
/// cancelled mid-await. One firing's waits all live on its one task; a
/// detached task sharing a `FiringLocation` (a smuggled handle) is
/// outside this contract, and the parked flag's truthfulness is only
/// guaranteed within it.
pub trait WaitLiveness: Send + Sync {
    /// A node entered a wait on `source`. Mints and returns a `WaitId`
    /// the other per-wait hooks key on. The engine stores a `Weak` on
    /// the source (so the stuck-check can read its generation) and
    /// marks this wait actively waiting but NOT yet parked.
    fn enter_wait(&self, node: &FiringLocation, source: &Arc<dyn WaitSource>) -> WaitId;
    /// The wait `id` under `node` ended (resolved or cancelled). Removes
    /// its slot; the node entry is dropped once it holds no more waits.
    fn exit_wait(&self, node: &FiringLocation, id: WaitId);
    /// Called on every state change of a source that could satisfy a
    /// wait (a bus append, a stream item, a delivery). The engine's
    /// stuck-check uses this as the ground-truth "something happened"
    /// signal: a change that lands while a node is parked must suppress
    /// a stuck declaration, because the parked node has new input to
    /// consume. Relying on scheduler fairness (a single `yield_now`) to
    /// observe the woken node instead is a race that closes live
    /// conversations.
    fn on_source_event(&self);
    /// The wait `id` under `node` is about to (re-)evaluate its wait
    /// condition. The engine records its source's CURRENT generation as
    /// this wait's observed generation, and marks this wait NOT parked.
    /// Contract on the caller (the wait loops): after calling this, a
    /// full condition evaluation runs BEFORE the next park, so
    /// "observed >= G" provably means "this wait's evaluation saw every
    /// change up to generation G" (the generation bumps under the same
    /// lock as the change, after the change lands, so any evaluation
    /// that starts after the bump sees it). The stuck-check closes only
    /// when EVERY parked node has every wait caught up on its source's
    /// current generation: a node woken by a change but still unpolled
    /// in another worker thread's queue is behind by construction, so a
    /// live conversation can never be torn down under it.
    ///
    /// Marking NOT parked matters too: an evaluation in progress may be
    /// about to SUCCEED, and a stuck-close under a succeeding
    /// evaluation would tear down a live conversation.
    fn observed(&self, node: &FiringLocation, id: WaitId);
    /// The wait `id` under `node` is at its TRUE park point: every
    /// pre-park re-check has run, the condition did not resolve, and
    /// the very next thing the wait does is `notified.await`. The
    /// stuck-check requires every wait of every in-flight node to be
    /// parked (and caught up) before acting: a wait between `observed`
    /// and its park is mid-evaluation and may resolve, so it suppresses
    /// the close. The flag flips back to false at the next `observed`
    /// (every wake re-evaluates before any re-park, by construction of
    /// the wait loops).
    fn parked(&self, node: &FiringLocation, id: WaitId);
}

/// Concrete zero-state type used to spell `Weak<dyn WaitLiveness>` when
/// no engine is attached: `Weak::<NoLiveness>::new()` constructs an
/// empty pointer that coerces into the trait-object Weak. `dyn` traits
/// can't be passed to `Weak::new` directly because `dyn Trait` is
/// unsized. Never instantiated: the empty `Weak` never upgrades, so no
/// method here can run; the bodies exist only to satisfy the trait for
/// the unsize coercion.
struct NoLiveness;
impl WaitLiveness for NoLiveness {
    fn enter_wait(&self, _node: &FiringLocation, _source: &Arc<dyn WaitSource>) -> WaitId {
        unreachable!("NoLiveness is never constructed")
    }
    fn exit_wait(&self, _node: &FiringLocation, _id: WaitId) {
        unreachable!("NoLiveness is never constructed")
    }
    fn on_source_event(&self) {
        unreachable!("NoLiveness is never constructed")
    }
    fn observed(&self, _node: &FiringLocation, _id: WaitId) {
        unreachable!("NoLiveness is never constructed")
    }
    fn parked(&self, _node: &FiringLocation, _id: WaitId) {
        unreachable!("NoLiveness is never constructed")
    }
}

/// An empty `Weak<dyn WaitLiveness>` for sources built with no engine
/// attached (tests, standalone rigs). Upgrading it always fails, so
/// every hook is a no-op.
pub fn no_liveness() -> Weak<dyn WaitLiveness> {
    Weak::<NoLiveness>::new() as Weak<dyn WaitLiveness>
}

/// RAII guard around a single wait, held only by [`wait_on`] (private
/// on purpose: publishing it would let a caller rebuild the
/// arm/observe/park loop by hand, which is exactly the drift `wait_on`
/// exists to prevent). Constructor calls `enter_wait`; Drop calls
/// `exit_wait`. Drop fires whether the awaiting future returns
/// normally OR is cancelled mid-await (e.g. the loop aborts a stuck
/// task), so the engine's liveness map stays consistent. Carries the
/// node-execution identity so every hook keys on the right firing. A
/// guard with no node identity (a source not minted by an
/// engine-driven node) is a no-op.
struct WaitGuard {
    liveness: Option<(Arc<dyn WaitLiveness>, FiringLocation, WaitId)>,
}

impl WaitGuard {
    fn new(
        liveness_weak: &Weak<dyn WaitLiveness>,
        node: &Option<FiringLocation>,
        source: &Arc<dyn WaitSource>,
    ) -> Self {
        let liveness = match (liveness_weak.upgrade(), node) {
            (Some(w), Some(node)) => {
                let id = w.enter_wait(node, source);
                Some((w, node.clone(), id))
            }
            _ => None,
        };
        Self { liveness }
    }

    /// Record "I am about to evaluate my wait condition" with the
    /// engine (see [`WaitLiveness::observed`] for the contract: a full
    /// condition evaluation MUST follow this call before the next park).
    /// The wait loops call this immediately before every condition
    /// evaluation that can lead to a park.
    fn record_observed(&self) {
        if let Some((w, node, id)) = &self.liveness {
            w.observed(node, *id);
        }
    }

    /// Record "I am at my true park point" with the engine (see
    /// [`WaitLiveness::parked`]). The wait loops call this immediately
    /// before `notified.await`, AFTER every lost-wakeup re-check, so a
    /// set parked flag provably means "this wait's last evaluation did
    /// not resolve and it is now awaiting".
    fn record_parked(&self) {
        if let Some((w, node, id)) = &self.liveness {
            w.parked(node, *id);
        }
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        if let Some((w, node, id)) = &self.liveness {
            w.exit_wait(node, *id);
        }
    }
}
