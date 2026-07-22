//! Cancellation primitive for executions and the nodes they run.
//!
//! `Arc<Notify>` alone has a fire-and-forget semantic: a `notify_waiters()`
//! call only wakes futures that are *currently* awaiting `notified()`.
//! A future created after the notify call sees nothing. That's wrong for
//! cancellation: once cancelled, every code path that observes the flag
//! later must see it.
//!
//! This wraps an `AtomicBool` (the persistent flag) plus a `Notify` (the
//! wakeup mechanism for blocked waits). `cancel()` sets the bool AND
//! notifies. `is_cancelled()` reads the bool synchronously.
//! `cancelled()` returns a future that resolves immediately if the flag
//! is already set, or on the next notify otherwise.
//!
//! Use `is_cancelled()` at iteration boundaries (loop drivers, apply
//! pipelines) and `cancelled()` inside `tokio::select!` arms to race
//! it against work futures.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct CancellationFlag {
    cancelled: AtomicBool,
    notify: Notify,
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
    /// immediately.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
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
                let cancel = tokio::spawn(async move { flag.cancel() });
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
        flag.cancel();
        flag.cancelled().await;
    }
}
