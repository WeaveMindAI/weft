//! A copy of a role stopping.
//!
//! A process is told to stop by a signal (SIGTERM, Ctrl+C): on a cloud,
//! Cloud Run sends it when it scales a copy in, and when the install
//! workflow deletes the revision an update replaced, and kills the copy 10
//! seconds later (`weft_platform_gcp::STOP_GRACE`); on a machine, when the
//! install's runtime is stopped. The runtime tells the roles it hosts
//! through one [`Stopping`] (`weft_runtime::server::stop_on`), and each
//! answers: the picker claims nothing more and gives back what it still
//! runs once [`GRACE`] has passed, the role loops stop, the dispatcher
//! ends its event streams and the broker its lines, each after what is in
//! flight on it is answered (a held call at once, as its wait running out
//! would: [`Deadline::or_stop`]), and the process exits once that work ended or
//! was given back ([`Stopping::wound_down`]). Whoever was following or
//! holding a line opens it again on a copy that runs.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// How long work in flight (a task being run, a call on a broker line)
/// gets to finish once its copy is stopping, before it is given back to be
/// run again elsewhere and the lines are closed: short work finishes,
/// anything that waits (a setup run waiting on a person) is handed on, the
/// way a lapsed claim is.
pub const GRACE: Duration = Duration::from_secs(5);

/// How long past [`GRACE`] the work given back then gets to be written
/// back (the write that hands a task on). Both together stay inside the
/// 10 seconds Cloud Run leaves a copy between SIGTERM and killing it,
/// with room for the process's last words (`weft_runtime`'s exit).
pub const GIVING_BACK: Duration = Duration::from_secs(3);

/// Whether this process's copy of its role is stopping, and the work it
/// still has in flight. Cloned freely; every clone is the same one.
#[derive(Clone, Default)]
pub struct Stopping {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    token: CancellationToken,
    /// When the stop began: [`GRACE`] and [`GIVING_BACK`] count from it,
    /// whenever their waits start.
    began_at: OnceLock<Instant>,
    in_flight: AtomicUsize,
    settled: tokio::sync::Notify,
}

impl Stopping {
    /// One that has not begun: until the process is told to stop, its copy
    /// runs.
    pub fn new() -> Self {
        Self::default()
    }

    /// This copy is stopping: everything holding it winds down. Only the
    /// first call counts.
    pub fn begin(&self) {
        self.inner.began_at.get_or_init(Instant::now);
        self.inner.token.cancel();
    }

    /// Whether this copy is stopping.
    pub fn has_begun(&self) -> bool {
        self.inner.token.is_cancelled()
    }

    /// Once this copy is stopping.
    pub async fn begun(&self) {
        self.inner.token.cancelled().await
    }

    /// Run `work` until this copy is stopping, then stop it and never end:
    /// a task the process supervises ends the process when it returns
    /// (`weft_dispatcher::app::spawn_supervised`), which would cut short
    /// the winding down a stopping copy does before it exits. Work that
    /// returns on its own still returns.
    pub async fn while_running(&self, work: impl std::future::Future<Output = ()>) {
        tokio::select! {
            () = work => {}
            () = self.begun() => std::future::pending().await,
        }
    }

    /// When the stop began plus `after`. Only once it began.
    fn since_begun(&self, after: Duration) -> Instant {
        *self.inner.began_at.get().expect("a stop that began says when") + after
    }

    /// Once this copy is stopping and [`GRACE`] has passed since: when
    /// work still in flight is given back.
    pub async fn grace_over(&self) {
        self.begun().await;
        tokio::time::sleep_until(self.since_begun(GRACE)).await;
    }

    /// Count one piece of work in flight for as long as the answer lives:
    /// [`Self::settled`] waits for every one.
    pub fn in_flight(&self) -> InFlight {
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        InFlight { inner: self.inner.clone() }
    }

    /// Once no work counted by [`Self::in_flight`] is left.
    pub async fn settled(&self) {
        loop {
            let notified = self.inner.settled.notified();
            if self.inner.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Once this copy is stopping and its work in flight ended or was
    /// given back, or [`GRACE`] and [`GIVING_BACK`] have passed since the
    /// stop began, whichever comes first: whether every piece did. One
    /// left is a claim that lapses, which a running copy takes then.
    pub async fn wound_down(&self) -> bool {
        self.begun().await;
        tokio::time::timeout_at(self.since_begun(GRACE + GIVING_BACK), self.settled()).await.is_ok()
    }
}

/// When a wait ends: at its instant, or, for a wait held for a caller
/// ([`Self::or_stop`]), as soon as its copy is stopping, whichever comes
/// first. A held wait ended by the stop answers what its instant would
/// have ("nothing yet"), so its caller asks again, on a copy that runs,
/// instead of holding this one up until its lines are cut.
#[derive(Clone)]
pub struct Deadline {
    at: Instant,
    stopping: Option<Stopping>,
}

impl Deadline {
    /// Ends at `at` and only then.
    pub fn at(at: Instant) -> Self {
        Self { at, stopping: None }
    }

    /// Ends sooner, once `stopping` has begun (at once if it has).
    pub fn or_stop(self, stopping: &Stopping) -> Self {
        Self { stopping: Some(stopping.clone()), ..self }
    }

    /// Once this deadline has come.
    pub async fn passed(&self) {
        match &self.stopping {
            None => tokio::time::sleep_until(self.at).await,
            Some(stopping) => tokio::select! {
                () = tokio::time::sleep_until(self.at) => {}
                () = stopping.begun() => {}
            },
        }
    }
}

impl From<Instant> for Deadline {
    fn from(at: Instant) -> Self {
        Self::at(at)
    }
}

/// One piece of work in flight ([`Stopping::in_flight`]), counted until
/// dropped.
pub struct InFlight {
    inner: Arc<Inner>,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if self.inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.settled.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Settled waits for every piece of work counted, and no longer.
    weft_core::stress_test! {
        name: settled_waits_for_the_work_in_flight,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let stopping = Stopping::new();
            stopping.settled().await;
            let (one, two) = (stopping.in_flight(), stopping.in_flight());
            let waiting = tokio::spawn({
                let stopping = stopping.clone();
                async move { stopping.settled().await }
            });
            drop(one);
            tokio::task::yield_now().await;
            assert!(!waiting.is_finished(), "one piece is still in flight");
            drop(two);
            tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("settled once nothing is in flight").unwrap();
        }
    }

    // Begun once, seen by every clone.
    weft_core::stress_test! {
        name: every_clone_sees_the_copy_stopping,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let stopping = Stopping::new();
            let held = stopping.clone();
            assert!(!held.has_begun());
            let waiting = tokio::spawn({
                let held = held.clone();
                async move { held.begun().await }
            });
            stopping.begin();
            assert!(held.has_begun());
            tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("begun").unwrap();
        }
    }

    /// The grace and the winding down count from when the stop began,
    /// however late their waits start, and a copy whose work never ends
    /// is let go once both have passed.
    #[tokio::test(start_paused = true)]
    async fn the_waits_count_from_the_stop() {
        let stopping = Stopping::new();
        let stuck = stopping.in_flight();
        stopping.begin();
        tokio::time::sleep(GRACE - Duration::from_secs(1)).await;
        let started = Instant::now();
        stopping.grace_over().await;
        assert_eq!(started.elapsed(), Duration::from_secs(1), "the rest of the grace, not a whole one");
        assert!(!stopping.wound_down().await, "work still in flight past the giving back");
        drop(stuck);
        assert!(stopping.wound_down().await, "nothing in flight");
    }
}
