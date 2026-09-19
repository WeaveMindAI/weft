//! "How much work is still running, and tell me when there is none."
//!
//! A worker pod dies in the middle of things unless something holds the
//! door: a cost record still being written, an execution still folding
//! its journal. Both want the same thing, a count that goes up when the
//! work starts, down when it lands, and a way to park until it reaches
//! zero. That count is here once rather than per subsystem, because the
//! interesting part is not the counter but the arm-before-check in
//! [`InFlight::wait_zero`], and a second copy of that is a second chance
//! to get it wrong.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A live count of one kind of in-flight work, with a gate that opens
/// when it empties. `label` names the work in the one error this can
/// report, so a bookkeeping bug says which counter it came from.
pub struct InFlight {
    count: AtomicUsize,
    zero: tokio::sync::Notify,
    label: &'static str,
}

impl InFlight {
    pub fn new(label: &'static str) -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            zero: tokio::sync::Notify::new(),
            label,
        })
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Take a token, released by hand with [`Self::end`]. Reach for
    /// [`Self::token`] instead wherever the work is one scope, since a
    /// guard cannot be forgotten on an early return.
    pub fn begin(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Take a token that releases itself when the returned guard drops,
    /// so an early return or a panic cannot leave the gate shut for
    /// ever.
    pub fn token(self: &Arc<Self>) -> InFlightToken {
        self.begin();
        InFlightToken { owner: self.clone() }
    }

    /// Release one token. A release with none held is a bookkeeping bug
    /// in a caller (one `end` too many): it is refused and logged rather
    /// than let the count wrap to the maximum, which would keep every
    /// [`Self::wait_zero`] waiting for ever.
    pub fn end(&self) {
        let before = self
            .count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| count.checked_sub(1));
        match before {
            Ok(1) => self.zero.notify_waiters(),
            Ok(_) => {}
            Err(_) => tracing::error!(
                target: "weft_core::in_flight",
                label = %self.label,
                "an in-flight token was released with none held; the release is ignored"
            ),
        }
    }

    /// Resolve once nothing of this kind is in flight.
    ///
    /// There is no deadline here on purpose: what is being waited on is
    /// the caller's own work finishing, and the callers each say in
    /// their own docs why theirs ends.
    pub async fn wait_zero(&self) {
        loop {
            // Arm BEFORE checking, so an `end` between the check and the
            // await cannot be missed: a `Notified` future is bound at
            // CREATION (tokio guarantees it completes for any
            // `notify_waiters` that fires after this line, even if the
            // future is first polled later), so the check-then-park
            // window is covered.
            let notified = self.zero.notified();
            if self.count() == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// One unit of in-flight work. Releases its token when dropped.
pub struct InFlightToken {
    owner: Arc<InFlight>,
}

impl Drop for InFlightToken {
    fn drop(&mut self) {
        self.owner.end();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_empty_counter_opens_at_once() {
        let f = InFlight::new("test");
        f.wait_zero().await;
        assert_eq!(f.count(), 0);
    }

    #[tokio::test]
    async fn the_gate_opens_when_the_last_token_drops() {
        let f = InFlight::new("test");
        let a = f.token();
        let b = f.token();
        assert_eq!(f.count(), 2);
        let waiter = {
            let f = f.clone();
            tokio::spawn(async move { f.wait_zero().await })
        };
        drop(a);
        assert_eq!(f.count(), 1);
        drop(b);
        waiter.await.expect("the wait ends once the count reaches zero");
        assert_eq!(f.count(), 0);
    }

    /// A token released twice must not wrap the count below zero, which
    /// would shut the gate for ever.
    #[tokio::test]
    async fn releasing_one_too_many_is_refused() {
        let f = InFlight::new("test");
        drop(f.token());
        f.end();
        assert_eq!(f.count(), 0);
        f.wait_zero().await;
    }
}
