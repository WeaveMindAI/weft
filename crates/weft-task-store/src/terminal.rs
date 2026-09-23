//! Waking whoever waits on a task the moment it finishes.
//!
//! Every write that makes a task terminal (`complete`, `fail`,
//! `fail_pending`) sends `pg_notify(TERMINAL_CHANNEL, <task id>)` in the
//! same statement, so the notification goes out exactly when the row
//! commits. One [`TerminalWatch`] per process holds a single `LISTEN`
//! connection and fans each id out to the waiters, who then read the row.
//! A wait therefore ends when the task does, instead of on the next tick
//! of a poll.
//!
//! A notification sent while the listening connection is down is lost,
//! so a reconnect tells every waiter to read its row again: nothing that
//! finished during the gap is missed. The row is the truth; a
//! notification only says when to look.
//!
//! The listening connection is its own, outside the caller's pool: it is
//! held for the life of the process, and taking it from the pool would
//! leave one slot fewer for everything else.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use tokio::sync::broadcast;
use uuid::Uuid;

/// The channel a terminal task write notifies on, with the task's id as
/// the payload.
pub const TERMINAL_CHANNEL: &str = "weft_task_terminal";

/// What a waiter hears: one task finished, or every waiter should look
/// at its row again because notifications may have been lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalSignal {
    Finished(Uuid),
    Recheck,
}

/// How many signals a slow waiter may fall behind by before it is told
/// to recheck instead (`broadcast`'s lag). Rechecking is always correct,
/// so the bound is about memory, not correctness.
const FANOUT_CAPACITY: usize = 1024;

/// Wait between attempts to listen again after the connection failed in
/// a way the listener does not recover from by itself.
const RETRY_DELAY: Duration = Duration::from_secs(1);

pub struct TerminalWatch {
    /// A receiver kept only to hand out new ones. The pump holds the one
    /// sender, so if the pump ever stops, every waiter hears `Closed`
    /// and fails loudly instead of sleeping to its timeout.
    template: Mutex<broadcast::Receiver<TerminalSignal>>,
    /// The listening task. It lives exactly as long as the watch: when
    /// the store that owns the watch goes, so does its connection (a
    /// test's database cannot be dropped while it is held).
    pump: tokio::task::JoinHandle<()>,
}

impl Drop for TerminalWatch {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

impl TerminalWatch {
    /// Start listening on `pool`'s database. Fails when the first
    /// connection cannot be made; after that, the listener reconnects on
    /// its own and every waiter is told to recheck.
    pub async fn start(pool: &PgPool) -> Result<Arc<Self>> {
        let own = PgPoolOptions::new()
            .max_connections(1)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with((*pool.connect_options()).clone())
            .await?;
        let listener = listen(&own).await?;
        let (tx, template) = broadcast::channel(FANOUT_CAPACITY);
        let pump = tokio::spawn(pump(listener, own, tx));
        Ok(Arc::new(Self { template: Mutex::new(template), pump }))
    }

    /// Subscribe BEFORE reading the row: a task that finishes between
    /// the read and the subscription would otherwise go unheard.
    pub fn subscribe(&self) -> broadcast::Receiver<TerminalSignal> {
        self.template.lock().expect("the template receiver is never poisoned").resubscribe()
    }
}

async fn listen(own: &PgPool) -> Result<PgListener> {
    let mut listener = PgListener::connect_with(own).await?;
    listener.listen(TERMINAL_CHANNEL).await?;
    Ok(listener)
}

async fn pump(mut listener: PgListener, own: PgPool, tx: broadcast::Sender<TerminalSignal>) {
    loop {
        let e = hear(&mut listener, &tx).await;
        // The listener only recovers by itself from a few kinds of
        // dropped socket, and keeps a connection that failed any other
        // way (a reset, a protocol error), which would fail the same way
        // on every call. So a fresh one, then everyone looks at their
        // row, since nothing was heard meanwhile. The old listener goes
        // first: it holds the one connection its pool allows, and a new
        // one could never get it while it lives.
        tracing::warn!(target: "weft_task_store::terminal", error = %e, "terminal listener failed; listening again");
        drop(listener);
        listener = loop {
            tokio::time::sleep(RETRY_DELAY).await;
            match listen(&own).await {
                Ok(fresh) => break fresh,
                Err(e) => tracing::warn!(target: "weft_task_store::terminal", error = %e, "terminal listener cannot listen yet"),
            }
        };
        let _ = tx.send(TerminalSignal::Recheck);
    }
}

/// Fan out what `listener` hears until it fails, and hand back why.
async fn hear(listener: &mut PgListener, tx: &broadcast::Sender<TerminalSignal>) -> sqlx::Error {
    loop {
        // No receiver is fine on every send: nobody is waiting right now.
        match listener.try_recv().await {
            Ok(Some(notification)) => match notification.payload().parse::<Uuid>() {
                Ok(id) => { let _ = tx.send(TerminalSignal::Finished(id)); }
                Err(e) => tracing::error!(
                    target: "weft_task_store::terminal",
                    payload = notification.payload(), error = %e,
                    "a terminal notification carried something that is not a task id"
                ),
            },
            // The connection dropped and the listener has already made a
            // fresh one and listened again; what finished in between was
            // not heard.
            Ok(None) => { let _ = tx.send(TerminalSignal::Recheck); }
            Err(e) => return e,
        }
    }
}

/// Wait for `task_id` to finish, or for `timeout` to pass, and hand back
/// its outcome either way (a timed-out outcome is not terminal).
pub async fn wait_for_terminal(
    pool: &PgPool,
    watch: &TerminalWatch,
    task_id: Uuid,
    timeout: Duration,
) -> Result<crate::tasks::TaskOutcome> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut signals = watch.subscribe();
    loop {
        let outcome = crate::tasks::peek(pool, task_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("task {task_id} disappeared"))?;
        if outcome.status.is_terminal() {
            return Ok(outcome);
        }
        // Wait for a reason to look again: this task's notification, a
        // recheck, or falling behind (which is a recheck too).
        loop {
            match tokio::time::timeout_at(deadline, signals.recv()).await {
                Err(_) => return Ok(outcome),
                Ok(Ok(TerminalSignal::Finished(id))) if id != task_id => continue,
                Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => break,
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    anyhow::bail!("the terminal-notification listener stopped")
                }
            }
        }
    }
}
