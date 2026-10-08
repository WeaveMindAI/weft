//! The waits a run keeps in its worker because it cannot pause.
//!
//! A run pauses on a wait (`ctx.await_signal`) by letting go of its
//! worker: its record holds the wait, and the answer has a worker pick it
//! up again. A run that cannot pause right now ([`Unsuspendable`]) keeps
//! the wait inside the node's own call instead. The call registers the
//! wait as any wait does, then waits here for one of three endings, which
//! the drive hands it:
//! - the answer, which the drive takes from the broker (the call records
//!   it, as it recorded the wait and records a wait given up: a wait's
//!   whole story is written by the call that waits);
//! - giving up, once nothing moved in the run for its `holdSecs`
//!   (`weft_core::run_settings::RunSettings::hold_secs`): the call fails,
//!   and the node may handle that like any outcome of its step;
//! - pausing after all, once the run can (the last bus between its nodes
//!   closed): the call suspends the way it would have from the start.
//!
//! A held wait is one more in-process wait of the run, so it reports to
//! the run's [`crate::wait_tracker::WaitTracker`] like a bus read does:
//! the drive sees when every step left is waiting, which is when the hold
//! clock runs.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use serde_json::Value;
use tokio::sync::Notify;

use weft_core::liveness::{wait_on, FiringLocation, WaitLiveness, WaitSource};

/// Why a run cannot be suspended right now: what makes a wait hold in its
/// worker, and what keeps a run that was asked to leave its worker on it
/// until the process goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unsuspendable {
    /// It keeps no record to be picked back up from.
    Unrecorded,
    /// Its caller (or the stand-in a fired run serves) is still on the
    /// line, and its route does not outlive its caller.
    TiedCaller,
    /// A bus is open, and a bus lives in this worker's memory alone.
    LiveBus,
}

impl Unsuspendable {
    pub(crate) fn why(self) -> &'static str {
        match self {
            Self::Unrecorded => "it keeps no record to be picked back up from (its trigger has `recorded: false`)",
            Self::TiedCaller => "it is tied to its caller, which cannot follow it (its route does not have `outlivesCaller`)",
            Self::LiveBus => "a bus between its nodes is open, and a bus lives in its worker's memory alone",
        }
    }
}

/// Whether the run can be suspended right now: the one check behind a wait
/// that holds, a held wait that pauses after all, and a run asked to leave
/// its worker that drains or stays.
pub(crate) fn unsuspendable(
    caller: Option<&Arc<dyn weft_core::caller::CallerConnection>>,
    bus_coordinator: &crate::context::BusCoordinator,
    settings: weft_core::run_settings::RunSettings,
) -> Option<Unsuspendable> {
    if caller.is_some_and(|conn| !conn.config().outlives_caller && conn.is_connected()) {
        Some(Unsuspendable::TiedCaller)
    } else if !settings.recorded() {
        Some(Unsuspendable::Unrecorded)
    } else if bus_coordinator.has_live_buses() {
        Some(Unsuspendable::LiveBus)
    } else {
        None
    }
}

/// Why a held wait was given up, for its node to put its name in front of.
pub(crate) fn gave_up_because(why: Unsuspendable, hold_secs: u32) -> String {
    match hold_secs {
        0 => format!("the run cannot pause ({}), and its `holdSecs` is 0, so it does not wait", why.why()),
        secs => format!(
            "the run cannot pause ({}), so it held the wait in its worker, and nothing moved in the run for \
             {secs} seconds (its `holdSecs`)",
            why.why()
        ),
    }
}

/// How a held wait ended.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HeldEnd {
    Answered(Value),
    /// Given up, and why.
    GaveUp(String),
    /// The run can pause now: the wait suspends.
    Pause,
}

/// The waits a run holds in its worker, by token. One per run, owned by
/// its [`crate::wait_tracker::WaitTracker`].
pub(crate) struct HeldWaits {
    state: Mutex<HeldState>,
    notify: Notify,
    /// Fast mirror of `HeldState::gen` for the lock-free observe path.
    gen_mirror: AtomicU64,
}

#[derive(Default)]
struct HeldState {
    /// Every wait held, `None` until it ends.
    waits: HashMap<String, Option<HeldEnd>>,
    /// Answers the drive took for waits nobody holds right now: one whose
    /// node registered it and holds it next, or one whose node suspended
    /// on it and is about to read as suspended (`Self::early_for` hands
    /// those to the drive's resume). Kept by token until one of those
    /// takes it.
    early: HashMap<String, Value>,
    /// Waits given up: an answer that comes for one after is dropped.
    given_up: HashSet<String>,
    /// Monotone generation of endings, for [`WaitSource`].
    gen: u64,
}

impl HeldWaits {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { state: Mutex::new(HeldState::default()), notify: Notify::new(), gen_mirror: AtomicU64::new(0) })
    }

    fn lock(&self) -> MutexGuard<'_, HeldState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether a wait is held with no ending yet.
    pub(crate) fn pending(&self) -> bool {
        self.lock().waits.values().any(Option::is_none)
    }

    /// Hold the wait `token` for the node at `node` until it ends. A wait
    /// dropped mid-hold (its run cancelled) leaves nothing behind.
    pub(crate) async fn hold(self: &Arc<Self>, liveness: &Weak<dyn WaitLiveness>, node: FiringLocation, token: &str) -> HeldEnd {
        {
            let mut state = self.lock();
            if let Some(value) = state.early.remove(token) {
                return HeldEnd::Answered(value);
            }
            state.waits.entry(token.to_string()).or_insert(None);
        }
        let forget = Forget { held: self, token };
        let source = self.clone() as Arc<dyn WaitSource>;
        let end = wait_on(liveness, &Some(node), &source, || {
            let mut state = self.lock();
            match state.waits.get(token) {
                Some(Some(_)) => state.waits.remove(token).flatten(),
                _ => None,
            }
        })
        .await;
        drop(forget);
        end
    }

    /// The answer to the wait `token`, taken by the drive: to the call
    /// holding it, else kept for whoever takes it next (see `early`). One
    /// for a wait given up is dropped.
    pub(crate) fn answer(&self, token: &str, value: Value) {
        let mut state = self.lock();
        if state.given_up.contains(token) {
            return;
        }
        match state.waits.get_mut(token) {
            Some(slot @ None) => {
                *slot = Some(HeldEnd::Answered(value));
                self.ended(&mut state);
            }
            // Answered already: the first answer stands.
            Some(Some(HeldEnd::Answered(_))) => {}
            // Pausing (its call has not seen it yet) or not held: kept for
            // the step, which suspends on it or holds it next.
            Some(Some(_)) | None => {
                state.early.insert(token.to_string(), value);
            }
        }
    }

    /// The answers kept for waits that turned out to be suspended steps
    /// (their tokens in `suspended`), for the drive to resume them with.
    pub(crate) fn early_for(&self, suspended: &HashSet<&str>) -> Vec<(String, Value)> {
        let mut state = self.lock();
        let tokens: Vec<String> = state.early.keys().filter(|token| suspended.contains(token.as_str())).cloned().collect();
        tokens.into_iter().filter_map(|token| state.early.remove(&token).map(|value| (token, value))).collect()
    }

    /// The wait `token` was given up without being held (a hold of no
    /// time): an answer that comes for it after is dropped.
    pub(crate) fn gave_up(&self, token: &str) {
        let mut state = self.lock();
        state.early.remove(token);
        state.given_up.insert(token.to_string());
    }

    /// Give up every wait still held, for `why`. Answers how many.
    pub(crate) fn give_up(&self, why: &str) -> usize {
        let mut state = self.lock();
        let tokens: Vec<String> = state.waits.iter().filter(|(_, end)| end.is_none()).map(|(token, _)| token.clone()).collect();
        for token in &tokens {
            state.waits.insert(token.clone(), Some(HeldEnd::GaveUp(why.to_string())));
            state.given_up.insert(token.clone());
        }
        if !tokens.is_empty() {
            self.ended(&mut state);
        }
        tokens.len()
    }

    /// The run can pause now: every wait still held suspends.
    pub(crate) fn pause(&self) {
        let mut state = self.lock();
        let mut paused = false;
        for end in state.waits.values_mut().filter(|end| end.is_none()) {
            *end = Some(HeldEnd::Pause);
            paused = true;
        }
        if paused {
            self.ended(&mut state);
        }
    }

    /// Endings landed: bump the generation and wake the holders. No
    /// `WaitLiveness::on_source_event`: endings only come from the drive,
    /// which is awake by construction.
    fn ended(&self, state: &mut HeldState) {
        state.gen += 1;
        self.gen_mirror.store(state.gen, Ordering::Release);
        self.notify.notify_waiters();
    }
}

/// Drops a held wait that never ended (its node's call was dropped).
struct Forget<'a> {
    held: &'a HeldWaits,
    token: &'a str,
}

impl Drop for Forget<'_> {
    fn drop(&mut self) {
        let mut state = self.held.lock();
        if state.waits.get(self.token).is_some_and(Option::is_none) {
            state.waits.remove(self.token);
        }
    }
}

impl WaitSource for HeldWaits {
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

    fn here() -> FiringLocation {
        FiringLocation::new("form", Vec::new())
    }

    fn liveness() -> (Arc<crate::wait_tracker::WaitTracker>, Weak<dyn WaitLiveness>) {
        let tracker = crate::wait_tracker::WaitTracker::new();
        let weak = Arc::downgrade(&tracker) as Weak<dyn WaitLiveness>;
        (tracker, weak)
    }

    /// An answer ends the wait it is for, also when it came first.
    #[tokio::test]
    async fn an_answer_ends_its_wait_even_when_it_came_first() {
        let (_tracker, weak) = liveness();
        let held = HeldWaits::new();
        held.answer("early", serde_json::json!(1));
        assert_eq!(held.hold(&weak, here(), "early").await, HeldEnd::Answered(serde_json::json!(1)));
        let waiting = tokio::spawn({
            let held = held.clone();
            async move { held.hold(&weak, here(), "late").await }
        });
        while !held.pending() {
            tokio::task::yield_now().await;
        }
        held.answer("late", serde_json::json!(2));
        assert_eq!(waiting.await.unwrap(), HeldEnd::Answered(serde_json::json!(2)));
        assert!(!held.pending());
    }

    /// Giving up ends every wait still held, and an answer that comes
    /// after is dropped.
    #[tokio::test]
    async fn a_wait_given_up_stays_given_up() {
        let (_tracker, weak) = liveness();
        let held = HeldWaits::new();
        let waiting = tokio::spawn({
            let held = held.clone();
            async move { held.hold(&weak, here(), "t").await }
        });
        while !held.pending() {
            tokio::task::yield_now().await;
        }
        assert_eq!(held.give_up("nothing moved"), 1);
        held.answer("t", serde_json::json!("late"));
        assert_eq!(waiting.await.unwrap(), HeldEnd::GaveUp("nothing moved".into()));
        {
            let state = held.lock();
            assert!(state.waits.is_empty() && state.early.is_empty(), "the late answer is not kept");
        }
        assert_eq!(held.give_up("again"), 0);
    }

    /// An answer kept for a wait nobody held goes to the step that
    /// suspended on it, and one for a wait given up without a hold is
    /// dropped.
    #[test]
    fn an_early_answer_waits_for_its_step() {
        let held = HeldWaits::new();
        held.answer("suspends", serde_json::json!(1));
        held.answer("zero", serde_json::json!(2));
        held.gave_up("zero");
        assert!(held.early_for(&HashSet::from(["other"])).is_empty());
        assert_eq!(held.early_for(&HashSet::from(["suspends", "zero"])), vec![("suspends".to_string(), serde_json::json!(1))]);
        held.answer("zero", serde_json::json!(3));
        assert!(held.lock().early.is_empty(), "a wait given up takes no answer");
    }

    /// Once the run can pause, every held wait suspends, and an answer
    /// that comes before its call saw the pause is kept for the step.
    #[tokio::test]
    async fn a_run_that_can_pause_again_pauses_its_held_waits() {
        let (_tracker, weak) = liveness();
        let held = HeldWaits::new();
        let waiting = tokio::spawn({
            let held = held.clone();
            async move { held.hold(&weak, here(), "t").await }
        });
        while !held.pending() {
            tokio::task::yield_now().await;
        }
        held.pause();
        held.answer("t", serde_json::json!("yes"));
        assert_eq!(waiting.await.unwrap(), HeldEnd::Pause);
        assert_eq!(held.early_for(&HashSet::from(["t"])), vec![("t".to_string(), serde_json::json!("yes"))]);
    }

    /// A held wait is parked to the run's tracker: a run whose only step
    /// is holding is provably waiting on the outside.
    #[tokio::test]
    async fn a_held_wait_counts_as_parked() {
        let (tracker, weak) = liveness();
        let held = HeldWaits::new();
        let waiting = tokio::spawn({
            let held = held.clone();
            async move { held.hold(&weak, here(), "t").await }
        });
        let in_flight: HashSet<FiringLocation> = [here()].into_iter().collect();
        while !tracker.deadlock_provable(&in_flight) {
            tokio::task::yield_now().await;
        }
        waiting.abort();
        let _ = waiting.await;
        assert!(!held.pending(), "a dropped hold leaves nothing behind");
    }
}
