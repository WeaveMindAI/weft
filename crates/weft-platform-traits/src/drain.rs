//! THE drain loop: wait for a running-execution count to reach zero
//! before a disruptive lifecycle op proceeds, with a per-request cap.
//!
//! One implementation for every `RunningPolicy::Wait` drain site (the
//! supervisor's stop/terminate drain, the dispatcher's
//! worker-replacement drain), so the semantics can never fork:
//! poll the count, breadcrumb every 30s so the wait is legible, and
//! at the cap report `TimedOut` (the CALLER proceeds anyway with a
//! loud warning; the cap is the user's "wait this long as a courtesy,
//! then do it" choice, not an error). The trigger-side deactivate wait
//! is deliberately NOT this: it is the unbounded, cancellable
//! `Deactivating` state driven by the drain-watcher, not a polling
//! loop inside a verb.
//!
//! `count` is a caller closure (broker HTTP for the supervisor, a
//! direct query for the dispatcher) and may also carry the caller's
//! cancel check (return `Err` to abort the drain, e.g. the user's
//! infra-cancel landing mid-wait). Time flows through `Clock` so
//! layer-3 tests drive the cap deterministically.

use std::time::Duration;

use crate::clock::Clock;

/// How often the drain re-polls the running count.
pub const DRAIN_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// How often a still-draining wait emits a breadcrumb.
const DRAIN_BREADCRUMB_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The running set emptied within the cap.
    Drained,
    /// The cap elapsed with executions still running. The caller
    /// proceeds with the lifecycle op (warning loudly); the leftover
    /// executions bear the op's consequences (e.g. they die with the
    /// worker kill).
    TimedOut { still_running: i64 },
}

/// Wait until `count` reports zero, up to `cap`. `context` names the
/// waiting operation in the breadcrumb logs ("worker replacement",
/// "infra stop"). Returns immediately when the first poll is already
/// zero. An `Err` from `count` aborts the drain and propagates (the
/// caller's cancel check rides inside `count`).
pub async fn drain_until_zero<F, Fut, E>(
    clock: &dyn Clock,
    cap: Duration,
    context: &str,
    mut count: F,
) -> Result<DrainOutcome, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<i64, E>>,
{
    let started = clock.now();
    let mut last_breadcrumb = started;
    loop {
        let running = count().await?;
        if running <= 0 {
            return Ok(DrainOutcome::Drained);
        }
        let now = clock.now();
        if now.duration_since(started) >= cap {
            return Ok(DrainOutcome::TimedOut { still_running: running });
        }
        if now.duration_since(last_breadcrumb) >= DRAIN_BREADCRUMB_INTERVAL {
            last_breadcrumb = now;
            tracing::info!(
                target: "weft_platform::drain",
                context,
                still_running = running,
                elapsed_secs = now.duration_since(started).as_secs(),
                cap_secs = cap.as_secs(),
                "still draining running executions before proceeding"
            );
        }
        clock.sleep(DRAIN_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use std::sync::atomic::{AtomicI64, Ordering};

    #[tokio::test]
    async fn drains_when_count_reaches_zero() {
        let clock = FakeClock::new();
        let remaining = AtomicI64::new(3);
        let outcome = drain_until_zero::<_, _, std::convert::Infallible>(
            clock.as_ref(),
            Duration::from_secs(600),
            "test",
            || {
                let v = remaining.fetch_sub(1, Ordering::SeqCst);
                async move { Ok(v.max(0)) }
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome, DrainOutcome::Drained);
    }

    #[tokio::test]
    async fn times_out_at_the_cap_with_the_leftover_count() {
        let clock = FakeClock::new();
        let outcome = drain_until_zero::<_, _, std::convert::Infallible>(
            clock.as_ref(),
            Duration::from_secs(10),
            "test",
            || async { Ok(5) },
        )
        .await
        .unwrap();
        assert_eq!(outcome, DrainOutcome::TimedOut { still_running: 5 });
    }

    #[tokio::test]
    async fn a_count_error_aborts_and_propagates() {
        let clock = FakeClock::new();
        let err = drain_until_zero::<_, _, &'static str>(
            clock.as_ref(),
            Duration::from_secs(10),
            "test",
            || async { Err("cancelled") },
        )
        .await
        .unwrap_err();
        assert_eq!(err, "cancelled");
    }
}
