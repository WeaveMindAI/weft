//! The wake-and-drain loop every signal-driven dispatcher loop runs.
//!
//! The loops that use it (the lifecycle claimer, the two bridges, the
//! cold-start sweep, the reapers that react to a row) all have the same
//! shape: sleep until a write they care about is announced on the
//! process's [`PgSignalWatch`] (see [`weft_task_store::pg_signal`]),
//! then run their drain body until it reports the queue empty. A safety
//! tick drains anyway every so often, for the notification that was
//! lost, and for the rows whose readiness no write announces (a lease
//! that lapsed, a delay that ran out).
//!
//! The subsystem provides its drain body, which channels wake it, and
//! its safety interval; the coalescing and the timing live here.

use std::time::Duration;

use anyhow::Result;
use tokio::time::Instant;

use weft_task_store::pg_signal::Subscription;

/// The usual safety net for missed notifications. Notifications are
/// best-effort by Postgres design (a reconnecting listener can lose
/// some, though it says so with a recheck), so this only catches what
/// slipped past; 30s of delay on a lost one is acceptable, and a tighter
/// tick would hammer the DB for nothing.
pub const SAFETY_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// What a subsystem's drain body returns after one iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStep {
    /// More work likely remains; the runner re-invokes the body
    /// without waiting. Use when an iteration hit a row-limit or
    /// otherwise expects siblings behind it.
    More,
    /// Queue is empty; the runner sleeps until the next wake.
    Done,
    /// Work exists that cannot be taken yet, and will become takeable
    /// without anything announcing it (a row whose writer has not
    /// committed, a delay that has not run out). The runner looks again
    /// after this long, or on the next wake if that comes first.
    RetryIn(Duration),
}

/// Which notifications wake a loop: those on `channel` whose payload
/// `concerns` says are its business.
#[derive(Clone, Copy)]
pub struct WakeOn {
    pub channel: &'static str,
    pub concerns: fn(&str) -> bool,
}

impl WakeOn {
    /// Every notification on `channel`.
    pub const fn any(channel: &'static str) -> Self {
        Self { channel, concerns: |_| true }
    }
}

/// Drive the drain loop forever: drain once at start (rows that landed
/// before the loop subscribed), then after every wake, every `RetryIn`,
/// and every `safety` interval. Returns only when the process's signal
/// watch stops, since nothing could wake the loop again; the caller's
/// supervisor crashes the pod on that.
///
/// `signals` must be subscribed before the call, and `target` is the
/// tracing target, so subsystems log under their own module name.
pub async fn run<F, Fut>(
    mut signals: Subscription,
    wake_on: &[WakeOn],
    safety: Duration,
    target: &'static str,
    mut drain: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<DrainStep>>,
{
    let mut next_look = Instant::now();
    loop {
        if Instant::now() < next_look {
            let woken = signals
                .woken_before(next_look, |channel, payload| {
                    wake_on.iter().any(|w| w.channel == channel && (w.concerns)(payload))
                })
                .await;
            if let Err(e) = woken {
                tracing::error!(target: "weft_dispatcher::pg_wake", subsystem = target, error = %e, "cannot be woken any more");
                return;
            }
        }
        // Everything heard up to here is covered by the drain that
        // follows, so a burst of notifications costs one drain, not one
        // per notification.
        if let Err(e) = signals.clear() {
            tracing::error!(target: "weft_dispatcher::pg_wake", subsystem = target, error = %e, "cannot be woken any more");
            return;
        }
        next_look = Instant::now() + safety;
        // Drain until the body reports it has nothing left. This is what
        // makes a burst of more rows than one batch finish in one wake.
        loop {
            match drain().await {
                Ok(DrainStep::More) => continue,
                Ok(DrainStep::Done) => break,
                Ok(DrainStep::RetryIn(after)) => {
                    next_look = next_look.min(Instant::now() + after);
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        target: "weft_dispatcher::pg_wake",
                        subsystem = target,
                        error = %e,
                        "drain failed; will retry on next wake"
                    );
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use weft_task_store::pg_signal::Heard;
    use tokio::sync::broadcast;

    const CHANNEL: &str = "chan";
    const WAKE: &[WakeOn] = &[WakeOn { channel: CHANNEL, concerns: |p| p == "mine" }];

    /// A drain body that records when it ran and answers from a script
    /// (then `Done` once the script is spent).
    fn scripted(
        runs: Arc<Mutex<Vec<Duration>>>,
        script: Vec<DrainStep>,
    ) -> impl FnMut() -> std::future::Ready<Result<DrainStep>> {
        let started = Instant::now();
        let script = Arc::new(Mutex::new(std::collections::VecDeque::from(script)));
        move || {
            runs.lock().unwrap().push(started.elapsed());
            std::future::ready(Ok(script.lock().unwrap().pop_front().unwrap_or(DrainStep::Done)))
        }
    }

    fn signal(payload: &str) -> Heard {
        Heard::Signal { channel: CHANNEL, payload: payload.into() }
    }

    #[tokio::test(start_paused = true)]
    async fn it_drains_at_start_and_until_the_body_is_done() {
        let (_tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let body = scripted(runs.clone(), vec![DrainStep::More, DrainStep::More]);
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", body));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 3, "More, More, Done");
    }

    #[tokio::test(start_paused = true)]
    async fn a_wake_that_concerns_it_drains_and_one_that_does_not_is_ignored() {
        let (tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(signal("theirs")).unwrap();
        tx.send(Heard::Signal { channel: "other", payload: "mine".into() }).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 1, "only the start drain");
        tx.send(signal("mine")).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_recheck_drains() {
        let (tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(Heard::Recheck).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_of_wakes_costs_one_drain() {
        let (tx, rx) = broadcast::channel(64);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        for _ in 0..20 {
            tx.send(signal("mine")).unwrap();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn with_no_signal_the_safety_tick_drains() {
        let (_tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(61)).await;
        let runs = runs.lock().unwrap().clone();
        assert_eq!(runs, vec![Duration::ZERO, Duration::from_secs(30), Duration::from_secs(60)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_looks_again_soon_and_only_while_asked() {
        let (_tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let body = scripted(runs.clone(), vec![DrainStep::RetryIn(Duration::from_millis(100)), DrainStep::Done]);
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", body));
        tokio::time::sleep(Duration::from_secs(1)).await;
        let runs = runs.lock().unwrap().clone();
        assert_eq!(runs, vec![Duration::ZERO, Duration::from_millis(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_watch_ends_the_loop() {
        let (tx, rx) = broadcast::channel::<Heard>(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let handle = tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(tx);
        handle.await.unwrap();
    }
}
