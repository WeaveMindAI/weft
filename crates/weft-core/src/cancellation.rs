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
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        // Race-free wait: register the notify slot before
        // re-checking the flag. `notify.notified()` returns a
        // future that holds a permit slot the moment it's awaited,
        // so a `cancel()` between the check and the await still
        // wakes us.
        let waiter = self.notify.notified();
        if self.is_cancelled() {
            return;
        }
        waiter.await;
    }
}
