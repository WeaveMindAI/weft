//! The runs a worker drives that only wait on something outside them (a
//! person's answer, a cancel), and until when each one waits at most.
//!
//! A worker ticks the broker every second while it has work under way
//! (`crate::door::ticks`), and each tick writes its lease: a database that
//! sleeps once nothing queries it would never sleep under a run that waits
//! a day for a person. So while every run a worker drives only waits
//! outside, its last tick vouches for it until the earliest moment one of
//! those waits gives up, and a lease's length past it, and then it stops
//! ticking: its lease holds until then, which keeps its runs its own and
//! keeps it counted among its program's copies. A run that moves again (its
//! wait answered, or given up at that earliest moment), or a new run
//! coming, wakes the tick, which vouches again for the next earliest.
//!
//! If the worker dies during the wait, its runs are judged lost once the
//! vouched lease passed: about when the first of their waits would have
//! given up, and the others died with the worker.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use weft_core::RunId;

/// How a run that only waits outside waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Quiet {
    /// When its quiet ends at most (unix seconds): its hold gives up, or,
    /// for a node waiting on its caller, its own cap or the caller's
    /// session cap ends ([`RunHold::quiet_on_caller`]).
    pub(crate) until: i64,
    /// Whether it could pause right now (nodes waiting on each other
    /// beside a suspended one), so a stopping worker hands it back, rather
    /// than holding its waits in the worker
    /// (`crate::held_waits::Unsuspendable`).
    pub(crate) can_pause: bool,
}

/// Every run of this worker that only waits outside, by how it waits, and
/// how the door hears of a change.
pub(crate) struct Holds {
    quiet: Mutex<HashMap<RunId, Quiet>>,
    /// Told whenever a run goes quiet or moves again, so the door's tick
    /// looks again at whether, and for how long, it may sleep
    /// (`crate::door::Door::wake`).
    changed: Box<dyn Fn() + Send + Sync>,
}

impl Holds {
    pub(crate) fn new(changed: Box<dyn Fn() + Send + Sync>) -> Arc<Self> {
        Arc::new(Self { quiet: Mutex::default(), changed })
    }

    /// How many runs only wait, and the earliest moment one of those waits
    /// gives up.
    pub(crate) fn quiet(&self) -> (usize, Option<i64>) {
        let quiet = self.quiet.lock().expect("run holds");
        (quiet.len(), quiet.values().map(|quiet| quiet.until).min())
    }

    /// The runs that only wait, by how each waits, held still: no run goes
    /// quiet or moves again until the guard goes.
    pub(crate) fn held_still(&self) -> std::sync::MutexGuard<'_, HashMap<RunId, Quiet>> {
        self.quiet.lock().expect("run holds")
    }

    /// Run `run_id`'s drive says how it waits ([`RunHold`]); its own
    /// cap on this worker ends at `cap_ends` (unix seconds), never without
    /// one. Nothing is kept for it until it first goes quiet.
    pub(crate) fn of(self: &Arc<Self>, run_id: RunId, cap_ends: Option<i64>) -> RunHold {
        RunHold { holds: self.clone(), run_id, cap_ends, quiet: AtomicBool::new(false) }
    }

    fn set(&self, run_id: RunId, how: Option<Quiet>) {
        let changed = {
            let mut quiet = self.quiet.lock().expect("run holds");
            match how {
                Some(how) => quiet.insert(run_id, how) != Some(how),
                None => quiet.remove(&run_id).is_some(),
            }
        };
        if changed {
            (self.changed)();
        }
    }
}

/// One run's place among the [`Holds`], as its drive says it; it moves
/// again (or ends) when this is dropped. A run that never goes quiet never
/// touches the [`Holds`].
pub(crate) struct RunHold {
    holds: Arc<Holds>,
    run_id: RunId,
    /// When the run's own cap on this worker ends (unix seconds).
    cap_ends: Option<i64>,
    /// Whether the run is among the quiet ones.
    quiet: AtomicBool,
}

impl RunHold {
    /// The run only waits, and one of its nodes waits on its caller's next
    /// message, which no hold gives up: it is quiet until the first of its
    /// own cap and its caller's session cap (`session_ends`), and with
    /// neither it is not quiet at all, so its worker keeps ticking.
    pub(crate) fn quiet_on_caller(&self, session_ends: Option<i64>, can_pause: bool) {
        let until = self.cap_ends.into_iter().chain(session_ends).min();
        self.quiet(until.map(|until| Quiet { until, can_pause }));
    }

    /// What is left of the run's own cap on this worker at `now_unix`,
    /// never without one: a run still on its worker then is ended, so one
    /// that could pause has paused by then.
    pub(crate) fn cap_left(&self, now_unix: u64) -> Option<std::time::Duration> {
        self.cap_ends.map(|ends| std::time::Duration::from_secs(u64::try_from(ends).unwrap_or(0).saturating_sub(now_unix)))
    }

    /// The run only waits outside, as `how` says, or it moves again
    /// (`None`). Saying the same again changes nothing.
    pub(crate) fn quiet(&self, how: Option<Quiet>) {
        if how.is_none() && !self.quiet.load(Ordering::Acquire) {
            return;
        }
        self.quiet.store(how.is_some(), Ordering::Release);
        self.holds.set(self.run_id, how);
    }
}

impl Drop for RunHold {
    fn drop(&mut self) {
        self.quiet(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A run counts as quiet from the moment its drive says so until it
    /// moves or its drive ends; the earliest wait is what the door vouches
    /// for; only a change tells the door.
    #[test]
    fn a_run_is_quiet_until_it_moves_or_ends() {
        let changes = Arc::new(AtomicUsize::new(0));
        let holds = Holds::new(Box::new({
            let changes = changes.clone();
            move || {
                changes.fetch_add(1, Ordering::SeqCst);
            }
        }));
        let until = |until| Some(Quiet { until, can_pause: false });
        let (a, b) = (holds.of(uuid::Uuid::from_u128(1), None), holds.of(uuid::Uuid::from_u128(2), None));
        assert_eq!(holds.quiet(), (0, None), "nothing kept before a run goes quiet");
        a.quiet(until(100));
        b.quiet(until(300));
        a.quiet(until(100));
        assert_eq!(holds.quiet(), (2, Some(100)));
        assert_eq!(changes.load(Ordering::SeqCst), 2, "saying the same again tells nobody");
        a.quiet(None);
        assert_eq!(holds.quiet(), (1, Some(300)), "the first wait gave up: the next earliest");
        a.quiet(until(100));
        b.quiet(None);
        assert_eq!(holds.quiet(), (1, Some(100)));
        a.quiet(Some(Quiet { until: 100, can_pause: true }));
        assert_eq!(changes.load(Ordering::SeqCst), 6, "a run that can pause now is a change");
        drop(a);
        assert_eq!(holds.quiet(), (0, None));
        drop(b);
        assert_eq!(changes.load(Ordering::SeqCst), 7, "a run that moved and then ended told the door once");
    }
}
