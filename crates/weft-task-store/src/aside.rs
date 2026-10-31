//! A copy of a role stepping aside for the copy that replaced it.
//!
//! On a cloud, an update or a rollback starts copies of a new revision of
//! each role beside the old ones, and the platform routes every new
//! request to the new ones. An old copy holding something open (a
//! follower's event stream, a worker's line) would otherwise live on for
//! as long as that stays open, claiming work from the shared queue and
//! holding its database connections beside the new copies' own: an old
//! and a new weft side by side. Which copy is current is the platform's to
//! say, and the runtime asks it (`weft_runtime::superseded`); this is how
//! the runtime tells the roles it hosts, and how they answer: the picker claims nothing more and gives
//! back what it still runs once [`GRACE`] has passed, the role loops stop,
//! the dispatcher ends its event streams and the broker its lines, each
//! after what is in flight on it is answered, and the process exits.
//!
//! A process nobody replaced (a local install, a copy whose platform says
//! nothing of revisions) holds one that is never set.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// How long work in flight (a task being run) gets to finish once its
/// copy steps aside, before it is given back to be run again elsewhere:
/// short work finishes, anything that waits (a setup run waiting on a
/// person) is handed on, the way a lapsed claim is.
pub const GRACE: Duration = Duration::from_secs(10);

/// Whether this process's copy of its role was replaced, and the work it
/// still has in flight. Cloned freely; every clone is the same one.
#[derive(Clone, Default)]
pub struct StepAside {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    token: CancellationToken,
    in_flight: AtomicUsize,
    settled: tokio::sync::Notify,
}

impl StepAside {
    /// One that is not set: until the runtime says this copy was replaced,
    /// it is the current one.
    pub fn new() -> Self {
        Self::default()
    }

    /// This copy was replaced: everything holding it steps aside.
    pub fn step_aside(&self) {
        self.inner.token.cancel();
    }

    /// Whether this copy was replaced.
    pub fn is_set(&self) -> bool {
        self.inner.token.is_cancelled()
    }

    /// Once this copy was replaced.
    pub async fn stepped(&self) {
        self.inner.token.cancelled().await
    }

    /// Run `work` while this copy is current, and once it was replaced,
    /// stop it and never end: a task the process supervises ends the
    /// process when it returns (`weft_dispatcher::app::spawn_supervised`),
    /// which would cut short the settling a replaced copy does before it
    /// exits. Work that returns on its own still returns.
    pub async fn while_current(&self, work: impl std::future::Future<Output = ()>) {
        tokio::select! {
            () = work => {}
            () = self.stepped() => std::future::pending().await,
        }
    }

    /// Once this copy was replaced and [`GRACE`] has passed since: when
    /// work still in flight is given back.
    pub async fn grace_over(&self) {
        self.stepped().await;
        tokio::time::sleep(GRACE).await;
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
}

/// One piece of work in flight ([`StepAside::in_flight`]), counted until
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
            let aside = StepAside::new();
            aside.settled().await;
            let (one, two) = (aside.in_flight(), aside.in_flight());
            let waiting = tokio::spawn({
                let aside = aside.clone();
                async move { aside.settled().await }
            });
            drop(one);
            tokio::task::yield_now().await;
            assert!(!waiting.is_finished(), "one piece is still in flight");
            drop(two);
            tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("settled once nothing is in flight").unwrap();
        }
    }

    // Set once, seen by every clone.
    weft_core::stress_test! {
        name: every_clone_sees_the_copy_step_aside,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let aside = StepAside::new();
            let held = aside.clone();
            assert!(!held.is_set());
            let waiting = tokio::spawn({
                let held = held.clone();
                async move { held.stepped().await }
            });
            aside.step_aside();
            assert!(held.is_set());
            tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("stepped").unwrap();
        }
    }
}
