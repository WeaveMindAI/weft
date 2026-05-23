//! Time + sleep abstraction.
//!
//! Production uses `SystemClock` which calls `std::time::Instant::now`
//! and `tokio::time::sleep`. Tests use `FakeClock`, which keeps an
//! internal counter; `sleep` advances the counter and returns
//! immediately. This makes "wait 30s then check flaky transition" a
//! deterministic test that runs in microseconds.
//!
//! The trait is intentionally minimal. Add methods here only when a
//! second subsystem proves it needs them. Right now: `now` and
//! `sleep`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

#[async_trait]
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;
    async fn sleep(&self, d: Duration);
}

/// Production clock. `now` is real wall clock; `sleep` yields to the
/// tokio runtime.
#[derive(Default, Clone)]
pub struct SystemClock;

impl SystemClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}

// ---------- fake (test-helpers) ----------

#[cfg(any(test, feature = "test-helpers"))]
mod fake {
    use super::*;
    use parking_lot::Mutex;
    use std::time::Duration;

    /// In-memory clock for tests. Starts at `Instant::now()` (the
    /// epoch is irrelevant; what matters is the relative offsets).
    /// `sleep` advances the internal counter and returns
    /// immediately; `advance` does the same without an await point.
    /// Read the current time with `now`.
    pub struct FakeClock {
        anchor: Instant,
        elapsed: Mutex<Duration>,
    }

    impl FakeClock {
        pub fn new() -> Arc<Self> {
            Arc::new(Self {
                anchor: Instant::now(),
                elapsed: Mutex::new(Duration::ZERO),
            })
        }

        /// Push the clock forward by `d`. Synchronous; safe to call
        /// from anywhere, including outside an async context.
        pub fn advance(&self, d: Duration) {
            *self.elapsed.lock() += d;
        }

        /// Total elapsed time since construction.
        pub fn elapsed(&self) -> Duration {
            *self.elapsed.lock()
        }
    }

    impl Default for FakeClock {
        fn default() -> Self {
            Self {
                anchor: Instant::now(),
                elapsed: Mutex::new(Duration::ZERO),
            }
        }
    }

    #[async_trait]
    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            self.anchor + *self.elapsed.lock()
        }

        async fn sleep(&self, d: Duration) {
            self.advance(d);
            // Yield so a `loop { do_work(); clock.sleep(d).await; }`
            // pattern in subsystem code gives the test driver a
            // scheduling point to mutate fake state between
            // iterations. Without this, the awaited future
            // resolves synchronously and the loop busy-spins
            // through every iteration before yielding.
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn advance_moves_now_forward() {
            let c = FakeClock::new();
            let t0 = c.now();
            c.advance(Duration::from_secs(10));
            assert_eq!(c.now() - t0, Duration::from_secs(10));
        }

        #[tokio::test]
        async fn sleep_advances_clock() {
            let c = FakeClock::new();
            let t0 = c.now();
            c.sleep(Duration::from_secs(5)).await;
            assert_eq!(c.now() - t0, Duration::from_secs(5));
        }

        #[test]
        fn elapsed_tracks_total() {
            let c = FakeClock::new();
            c.advance(Duration::from_secs(3));
            c.advance(Duration::from_secs(2));
            assert_eq!(c.elapsed(), Duration::from_secs(5));
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::FakeClock;
