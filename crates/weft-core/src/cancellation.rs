//! Cancellation primitive for executions and the nodes they run.
//!
//! `Arc<Notify>` alone has a fire-and-forget semantic: a `notify_waiters()`
//! call only wakes futures that are *currently* awaiting `notified()`.
//! A future created after the notify call sees nothing. That's wrong for
//! cancellation: once cancelled, every code path that observes the flag
//! later must see it.
//!
//! This wraps an `AtomicBool` (the persistent flag) plus a `Notify` (the
//! wakeup mechanism for blocked waits). `cancel_because()` sets the bool
//! AND notifies, recording why. `is_cancelled()` reads the bool
//! synchronously.
//! `cancelled()` returns a future that resolves immediately if the flag
//! is already set, or on the next notify otherwise.
//!
//! Use `is_cancelled()` at iteration boundaries (loop drivers, apply
//! pipelines) and `cancelled()` inside `tokio::select!` arms to race
//! it against work futures.
//!
//! The flag also remembers WHY, when the canceller says: an execution's
//! flag is flipped by the worker acting on a `cancel_execution` task, by
//! the pod shutting down, or by a live caller dropping, and the terminal
//! event the driver writes afterwards has to name that cause. The first
//! cause to land is the one kept: a run cancelled twice for two reasons
//! was cancelled for the first.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::exec::CancelCause;

#[derive(Debug, Default)]
pub struct CancellationFlag {
    cancelled: AtomicBool,
    notify: Notify,
    /// Why the flag was flipped. `None` only before any cancel: the one
    /// door, [`Self::cancel_because`], stores the cause before it trips
    /// the flag, so a tripped flag always carries one.
    cause: Mutex<Option<CancelCause>>,
}

impl CancellationFlag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Mark cancelled. Idempotent. Wakes every task currently
    /// awaiting `cancelled()`; every future call resolves
    /// immediately. Private: the only door is [`Self::cancel_because`],
    /// so a tripped flag always carries its cause and no terminal ever
    /// has to guess (a node body holds this flag through
    /// `ctx.cancellation()` and would otherwise be able to trip it
    /// causeless).
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Mark cancelled, recording why. The one door: every cancel of an
    /// execution's flag goes through here, so the driver's terminal
    /// write can say what happened instead of guessing. The first
    /// cause recorded wins; a later one is ignored (the run was already
    /// cancelled for the first reason).
    pub fn cancel_because(&self, cause: CancelCause) {
        {
            let mut slot = self.cause.lock().expect("cancellation cause poisoned");
            if slot.is_none() {
                *slot = Some(cause);
            }
        }
        self.cancel();
    }

    /// The cause recorded by [`Self::cancel_because`], if any.
    pub fn cause(&self) -> Option<CancelCause> {
        self.cause.lock().expect("cancellation cause poisoned").clone()
    }

    /// Cheap synchronous check. Use at iteration boundaries.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Future that resolves the moment the flag is set. Resolves
    /// immediately if already set. Use in `tokio::select!` arms to
    /// race long-running work against cancellation.
    ///
    /// Race-safe via `tokio::pin!` + `Notified::enable()`: the
    /// `notify.notified()` future does NOT register as a waiter until
    /// it is first polled, and `notify_waiters` stores no permit, so a
    /// `cancel()` that fires between `notified()` and the await would be
    /// lost without registration. `enable()` registers synchronously
    /// before the re-check; any `cancel()` that lands after registration
    /// wakes the future, any `cancel()` that landed before is caught by
    /// the post-enable flag read.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let waiter = self.notify.notified();
        tokio::pin!(waiter);
        waiter.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        waiter.await;
    }

    /// [`Self::cancelled`] resolving to the error a node body returns
    /// to unwind as cancelled (not failed): the select-arm door, so
    /// node code never names the error type.
    /// `err = cancel.cancelled_err() => return Err(err)`.
    pub async fn cancelled_err(&self) -> crate::error::WeftError {
        self.cancelled().await;
        crate::error::WeftError::Cancelled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression for the `Notify::notified()` arm-then-check race under a
    // multi-threaded tokio runtime: spawn many `cancelled()` waiters and
    // `cancel()`s in pairs, where each `cancel()` happens AFTER the
    // waiter is spawned but BEFORE the waiter's underlying `Notified`
    // future is first polled. With the broken pattern (future created
    // then awaited without `enable()`, `notify_waiters` storing no
    // permit), the wait deadlocks. With `pin!` + `enable()` the wait
    // completes promptly. Stress-looped to surface any future regression
    // on the first failing CI run rather than waiting for the flake.
    crate::stress_test!(
        name: cancelled_does_not_miss_notify_under_multi_thread,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            for _ in 0..200 {
                let flag = Arc::new(CancellationFlag::new());
                let f2 = flag.clone();
                let wait = tokio::spawn(async move { f2.cancelled().await });
                let cancel = tokio::spawn(async move { flag.cancel_because(CancelCause::User) });
                let result = tokio::time::timeout(std::time::Duration::from_millis(500), wait)
                    .await
                    .expect("cancelled() must not hang under multi-threaded runtime");
                result.expect("join ok");
                let _ = cancel.await;
            }
        }
    );

    /// `cancelled()` resolves immediately if the flag is already set.
    #[tokio::test]
    async fn cancelled_returns_immediately_when_already_set() {
        let flag = CancellationFlag::new();
        flag.cancel_because(CancelCause::User);
        flag.cancelled().await;
    }

    /// The first cause recorded is the one the terminal write reads; a
    /// second cancel for a different reason changes nothing, and an
    /// untripped flag carries none.
    #[test]
    fn first_cause_wins() {
        let flag = CancellationFlag::new();
        assert_eq!(flag.cause(), None);
        flag.cancel_because(CancelCause::CallerGone);
        flag.cancel_because(CancelCause::User);
        assert!(flag.is_cancelled());
        assert_eq!(flag.cause(), Some(CancelCause::CallerGone));
    }
}
