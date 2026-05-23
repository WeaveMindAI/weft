//! Shared wake-and-drain pattern for Postgres-LISTEN-driven loops.
//!
//! Two subsystems use this today (`lifecycle_claimer`,
//! `infra_event_bridge`). Both have the same shape:
//!
//! 1. A `PgListener` task subscribes to a channel and bumps a
//!    `Notify` every time a `pg_notify(...)` arrives. Reconnects on
//!    structural errors with backoff.
//! 2. A safety-poll task bumps the same `Notify` every
//!    `SAFETY_POLL_INTERVAL` to catch missed wakes (listener
//!    reconnect mid-flight, dropped NOTIFY).
//! 3. A drain loop calls `notified().await`, then runs the
//!    subsystem's drain body until it reports "queue empty".
//!
//! This module factors out (1) and (2) plus the shape of (3). The
//! subsystem provides only its drain body and channel name; the
//! coalescing / reconnect / safety-poll plumbing lives here.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::Notify;

/// Safety net for missed NOTIFYs. NOTIFY is best-effort by Postgres
/// design (transient listener reconnect can lose messages); this
/// catches drift. Long: 30s of SSE delay is acceptable; tighter
/// polling would hammer the DB for nothing under the steady-state
/// "all wakes arrived" case.
pub const SAFETY_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Backoff for the LISTEN reconnect loop after a structural failure.
const LISTENER_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

/// What a subsystem's drain body returns after one iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStep {
    /// More work likely remains; the runner re-invokes the body
    /// without waiting for the next `Notify`. Use when an iteration
    /// hit a row-limit or otherwise expects siblings behind it.
    More,
    /// Queue is empty; the runner parks on `notified()` until the
    /// next wake.
    Done,
}

/// Spawn the listener + safety-poll tasks AND drive the drain loop
/// forever. Returns when the caller-spawned task it's hosted on is
/// aborted (i.e. never under normal operation).
///
/// `target` is the tracing target; subsystems log under their own
/// module name so logs read naturally.
pub async fn run<F, Fut>(
    pool: PgPool,
    channel: &'static str,
    target: &'static str,
    mut drain: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<DrainStep>>,
{
    let wake = Arc::new(Notify::new());

    let listener_pool = pool.clone();
    let listener_wake = wake.clone();
    tokio::spawn(async move {
        run_listener_forever(&listener_pool, channel, target, listener_wake).await;
    });

    let poll_wake = wake.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SAFETY_POLL_INTERVAL).await;
            poll_wake.notify_one();
        }
    });

    // Kick once at startup so rows that landed before the listener
    // attached land on the first drain. Pre-spawn-pre-listener
    // inserts also land here (the listener's first `listen()` call
    // emits a kick after attaching).
    wake.notify_one();

    loop {
        wake.notified().await;
        // Drain until the body reports `Done`. This is what makes a
        // burst of N>limit rows finish in one wake instead of one
        // batch per safety-poll interval.
        loop {
            match drain().await {
                Ok(DrainStep::More) => continue,
                Ok(DrainStep::Done) => break,
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

async fn run_listener_forever(
    pool: &PgPool,
    channel: &'static str,
    target: &'static str,
    wake: Arc<Notify>,
) {
    loop {
        // `listener_session` returns `Result<Infallible>`: the Ok
        // arm can't be constructed. The allow keeps the irrefutable-
        // pattern warning quiet while documenting intent.
        #[allow(irrefutable_let_patterns)]
        let Err(e) = listener_session(pool, channel, &wake).await
        else {
            unreachable!("listener_session returns Result<Infallible>")
        };
        {
            tracing::warn!(
                target: "weft_dispatcher::pg_wake",
                subsystem = target,
                channel,
                error = %e,
                "listener session failed; reconnecting after backoff"
            );
            tokio::time::sleep(LISTENER_RECONNECT_BACKOFF).await;
        }
    }
}

/// Run one listener session: connect, listen, recv-loop. Returns
/// `Err` on any error (connect failure, recv error). Never returns
/// `Ok`: the recv-loop is infinite. Encoded as `Infallible` so the
/// caller's "match Ok arm" can't drift.
async fn listener_session(
    pool: &PgPool,
    channel: &'static str,
    wake: &Arc<Notify>,
) -> Result<std::convert::Infallible> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(channel).await?;
    // After every (re)connect, drain whatever the safety poll might
    // have missed during the gap.
    wake.notify_one();
    loop {
        listener
            .recv()
            .await
            .map_err(|e| anyhow::anyhow!("PgListener recv error: {e}"))?;
        wake.notify_one();
    }
}
