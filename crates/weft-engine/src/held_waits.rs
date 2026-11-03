//! The waits a run keeps in its worker: every `state.wait`
//! (`weft_core::context::Kept`) of a run being driven.
//!
//! A wait never pauses its run on its own. Its call registers the wait as
//! any wait does, then waits here for one of four endings, which the
//! drive hands it:
//! - the answer, which the drive takes from the broker (the call records
//!   it, as it recorded the wait and records a wait given up: a wait's
//!   whole story is written by the call that waits), and the body goes on
//!   where it is;
//! - pausing: the run pauses once every node it runs waits and it stayed
//!   that quiet for its `pauseAfterSecs`
//!   (`weft_core::run_settings::RunSettings::pause_after_secs`), or at
//!   once for a hand-back, and every waiting call saves its state and
//!   suspends together; the firing goes on record as suspended on its
//!   wait, so a later answer resumes it;
//! - giving up, in a run that cannot pause ([`Unsuspendable`]), once
//!   nothing moved in the run for its `holdSecs`
//!   (`weft_core::run_settings::RunSettings::hold_secs`): the call fails,
//!   and the node may handle that like any outcome of its firing. Such a
//!   run pauses its held waits once it can (its caller left, on a route
//!   that outlives its caller), by the same rule;
//! - its firing failing: the engine failed it while its body still ran
//!   (an emission it refused, a stream into it gone corrupt,
//!   [`HeldWaits::fail_firing`]), so the call ends with that failure, and
//!   a wait the firing reaches after ends the same way at once.
//!
//! A held wait is one more in-process wait of the run, so it reports to
//! the run's [`crate::wait_tracker::WaitTracker`] like a bus read does:
//! the drive sees when every running node waits, which is when the run is
//! quiet.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use weft_core::primitive::WaitAnswer;
use tokio::sync::Notify;

use weft_core::liveness::{wait_or_pause, FiringLocation, WaitLiveness, WaitSource};

/// Why a run cannot be suspended right now: what makes a held wait give
/// up after its `holdSecs` rather than pause, and what keeps a run that was
/// asked to leave its worker on it until the process goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unsuspendable {
    /// It keeps no record to be picked back up from.
    Unrecorded,
    /// Its caller (or the stand-in a fired run serves) is still on the
    /// line: a run never pauses under a connected caller, whatever its
    /// route's `outlivesCaller` says (that only decides what the caller
    /// leaving does).
    TiedCaller,
    /// It runs no program a worker could pick it back up with: a node
    /// test's run, whose handles say so (`RunnerHandle::without_program`).
    NoProgram,
    /// The firing waiting holds its run (a `ctx.hold()` value of it
    /// exists): its waits do not pause until it lets go.
    Holding,
}

impl Unsuspendable {
    pub(crate) fn why(self) -> &'static str {
        match self {
            Self::NoProgram => "it is a node test, which has no program to be picked back up with",
            Self::Unrecorded => "it keeps no record to be picked back up from (its trigger has `recorded: false`)",
            Self::TiedCaller => "its caller is still on the line, and a caller cannot follow a run to another worker",
            Self::Holding => "the node holds its run (`ctx.hold()`)",
        }
    }
}

/// Whether a run driven over its program can be suspended right now: the
/// one check behind a held wait that gives up, a run whose every node
/// waits that pauses, and a run asked to leave its
/// worker that drains or stays. A run with no program never can either,
/// which the handles of a node test's run say themselves
/// ([`Unsuspendable::NoProgram`]); a drive always has one.
pub(crate) fn unsuspendable(
    caller: Option<&Arc<dyn weft_core::caller::CallerConnection>>,
    settings: weft_core::run_settings::RunSettings,
) -> Option<Unsuspendable> {
    if caller.is_some_and(|conn| conn.holds_the_run()) {
        Some(Unsuspendable::TiedCaller)
    } else if !settings.recorded() {
        Some(Unsuspendable::Unrecorded)
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
    /// Its answer: a value, or a person skipping it.
    Answered(WaitAnswer),
    /// Given up, and why.
    GaveUp(String),
    /// The run pauses: the wait suspends.
    Pause,
    /// The engine failed the firing holding it while its body still ran
    /// (an emission it refused, a stream into it gone corrupt), with this
    /// failure: the wait ends with it.
    FiringFailed(String),
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
    /// node registered it and holds it next, or one whose call is pausing
    /// (that one stays unrecorded, so the broker hands it to the run's
    /// next worker). Kept by token until a hold takes it.
    early: HashMap<String, WaitAnswer>,
    /// Waits given up: an answer that comes for one after is dropped.
    given_up: HashSet<String>,
    /// The firing holding each wait.
    holders: HashMap<String, FiringLocation>,
    /// The waits held that must not pause: their firing holds its run
    /// ([`Unsuspendable::Holding`]). The run pausing leaves them open.
    unpausable: HashSet<String>,
    /// The firings the engine failed while their bodies still ran, with
    /// their failure ([`HeldWaits::fail_firing`]): a wait one of them
    /// holds or reaches after ends at once with it.
    failed: HashMap<FiringLocation, String>,
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
    /// dropped mid-hold (its run cancelled) leaves nothing behind. The
    /// hold is inside the node's `state.wait`, so the run pausing ends it
    /// like any pausable wait: it pauses.
    /// `pausable`: whether the run pausing may end it (`false` while its
    /// firing holds its run: it waits until answered or given up).
    pub(crate) async fn hold(self: &Arc<Self>, liveness: &Weak<dyn WaitLiveness>, node: FiringLocation, token: &str, pausable: bool) -> HeldEnd {
        {
            let mut state = self.lock();
            if let Some(failure) = state.failed.get(&node) {
                let failure = failure.clone();
                state.early.remove(token);
                state.given_up.insert(token.to_string());
                return HeldEnd::FiringFailed(failure);
            }
            if let Some(value) = state.early.remove(token) {
                return HeldEnd::Answered(value);
            }
            state.waits.entry(token.to_string()).or_insert(None);
            state.holders.insert(token.to_string(), node.clone());
            if !pausable {
                state.unpausable.insert(token.to_string());
            }
        }
        let forget = Forget { held: self, token };
        let source = self.clone() as Arc<dyn WaitSource>;
        let end = wait_or_pause(liveness, &Some(node), &source, pausable, || {
            let mut state = self.lock();
            match state.waits.get(token) {
                Some(Some(_)) => {
                    state.holders.remove(token);
                    state.unpausable.remove(token);
                    state.waits.remove(token).flatten()
                }
                _ => None,
            }
        })
        .await
        .unwrap_or(HeldEnd::Pause);
        drop(forget);
        end
    }

    /// The answer to the wait `token`, taken by the drive: to the call
    /// holding it, else kept for whoever takes it next (see `early`). One
    /// for a wait given up is dropped.
    pub(crate) fn answer(&self, token: &str, value: WaitAnswer) {
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
            // the node, which suspends on it or holds it next.
            Some(Some(_)) | None => {
                state.early.insert(token.to_string(), value);
            }
        }
    }

    /// The wait `token` was given up without being held (a hold of no
    /// time): an answer that comes for it after is dropped.
    pub(crate) fn gave_up(&self, token: &str) {
        let mut state = self.lock();
        state.early.remove(token);
        state.given_up.insert(token.to_string());
    }

    /// Give up every wait still held, for `why`; `only_unpausable`: only
    /// those whose firing holds its run. Answers how many.
    pub(crate) fn give_up(&self, why: &str, only_unpausable: bool) -> usize {
        let mut state = self.lock();
        let tokens: Vec<String> = state
            .waits
            .iter()
            .filter(|(token, end)| end.is_none() && (!only_unpausable || state.unpausable.contains(*token)))
            .map(|(token, _)| token.clone())
            .collect();
        for token in &tokens {
            state.waits.insert(token.clone(), Some(HeldEnd::GaveUp(why.to_string())));
            state.given_up.insert(token.clone());
        }
        if !tokens.is_empty() {
            self.ended(&mut state);
        }
        tokens.len()
    }

    /// The engine failed the firing at `at` while its body still runs,
    /// with `failure`: every wait it holds ends with it now, an answer
    /// that comes for one after is dropped, and a wait it reaches later
    /// ends at once the same way ([`Self::failure_of`]).
    pub(crate) fn fail_firing(&self, at: &FiringLocation, failure: &str) {
        let mut state = self.lock();
        state.failed.insert(at.clone(), failure.to_string());
        let tokens: Vec<String> = state
            .holders
            .iter()
            .filter(|(token, holder)| *holder == at && state.waits.get(*token).is_some_and(Option::is_none))
            .map(|(token, _)| token.clone())
            .collect();
        for token in &tokens {
            state.waits.insert(token.clone(), Some(HeldEnd::FiringFailed(failure.to_string())));
            state.given_up.insert(token.clone());
        }
        if !tokens.is_empty() {
            self.ended(&mut state);
        }
    }

    /// The failure the engine failed the firing at `at` with while its
    /// body ran, if it did ([`Self::fail_firing`]).
    pub(crate) fn failure_of(&self, at: &FiringLocation) -> Option<String> {
        self.lock().failed.get(at).cloned()
    }

    /// The run pauses: every wait still held suspends, but one whose
    /// firing holds its run.
    pub(crate) fn pause(&self) {
        let mut state = self.lock();
        let mut paused = false;
        let HeldState { waits, unpausable, .. } = &mut *state;
        for (_, end) in waits.iter_mut().filter(|(token, end)| end.is_none() && !unpausable.contains(*token)) {
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
            state.holders.remove(self.token);
            state.unpausable.remove(self.token);
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

    fn given(value: serde_json::Value) -> WaitAnswer {
        WaitAnswer::Given { value }
    }

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
        held.answer("early", given(serde_json::json!(1)));
        assert_eq!(held.hold(&weak, here(), "early", true).await, HeldEnd::Answered(given(serde_json::json!(1))));
        let waiting = tokio::spawn({
            let held = held.clone();
            async move { held.hold(&weak, here(), "late", true).await }
        });
        while !held.pending() {
            tokio::task::yield_now().await;
        }
        held.answer("late", given(serde_json::json!(2)));
        assert_eq!(waiting.await.unwrap(), HeldEnd::Answered(given(serde_json::json!(2))));
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
            async move { held.hold(&weak, here(), "t", true).await }
        });
        while !held.pending() {
            tokio::task::yield_now().await;
        }
        assert_eq!(held.give_up("nothing moved", false), 1);
        held.answer("t", given(serde_json::json!("late")));
        assert_eq!(waiting.await.unwrap(), HeldEnd::GaveUp("nothing moved".into()));
        {
            let state = held.lock();
            assert!(state.waits.is_empty() && state.early.is_empty(), "the late answer is not kept");
        }
        assert_eq!(held.give_up("again", false), 0);
    }

    /// An answer for a wait given up without a hold is dropped.
    #[test]
    fn a_wait_given_up_takes_no_answer() {
        let held = HeldWaits::new();
        held.answer("zero", given(serde_json::json!(2)));
        held.gave_up("zero");
        held.answer("zero", given(serde_json::json!(3)));
        assert!(held.lock().early.is_empty(), "a wait given up takes no answer");
    }

    /// The run pausing suspends every held wait, and an answer that comes
    /// before its call saw the pause is kept, never handed to it.
    #[tokio::test]
    async fn a_run_pausing_pauses_its_held_waits() {
        let (_tracker, weak) = liveness();
        let held = HeldWaits::new();
        let waiting = tokio::spawn({
            let held = held.clone();
            async move { held.hold(&weak, here(), "t", true).await }
        });
        while !held.pending() {
            tokio::task::yield_now().await;
        }
        held.pause();
        held.answer("t", given(serde_json::json!("yes")));
        assert_eq!(waiting.await.unwrap(), HeldEnd::Pause);
        assert_eq!(held.lock().early.get("t"), Some(&given(serde_json::json!("yes"))));
    }

    /// A firing the engine fails ends the waits it holds with its failure,
    /// drops an answer that comes for one after, and ends a wait it holds
    /// later at once; another firing's wait is left alone.
    #[tokio::test]
    async fn a_failed_firing_ends_its_held_waits_now_and_later() {
        let (_tracker, weak) = liveness();
        let held = HeldWaits::new();
        let other = FiringLocation::new("other", Vec::new());
        let waiting = tokio::spawn({
            let (held, weak) = (held.clone(), weak.clone());
            async move { held.hold(&weak, here(), "t", true).await }
        });
        let beside = tokio::spawn({
            let (held, weak, other) = (held.clone(), weak.clone(), other.clone());
            async move { held.hold(&weak, other, "u", true).await }
        });
        while held.lock().holders.len() < 2 {
            tokio::task::yield_now().await;
        }
        held.fail_firing(&here(), "boom");
        assert_eq!(waiting.await.unwrap(), HeldEnd::FiringFailed("boom".into()));
        held.answer("t", given(serde_json::json!("late")));
        assert!(held.lock().early.is_empty(), "an answer for a failed firing's wait is dropped");
        assert_eq!(held.hold(&weak, here(), "v", true).await, HeldEnd::FiringFailed("boom".into()), "a later wait ends at once");
        assert_eq!(held.failure_of(&here()).as_deref(), Some("boom"));
        held.answer("u", given(serde_json::json!("yes")));
        assert_eq!(beside.await.unwrap(), HeldEnd::Answered(given(serde_json::json!("yes"))), "another firing's wait is left alone");
    }

    /// A held wait is parked to the run's tracker: a run whose only running node
    /// is holding is provably waiting on the outside.
    #[tokio::test]
    async fn a_held_wait_counts_as_parked() {
        let (tracker, weak) = liveness();
        let held = HeldWaits::new();
        let waiting = tokio::spawn({
            let held = held.clone();
            async move { held.hold(&weak, here(), "t", true).await }
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
