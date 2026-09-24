//! Waking whoever waits on a row the moment it changes.
//!
//! Writes that someone may be waiting on send `pg_notify(channel,
//! payload)` in the same transaction (most of them from a trigger in the
//! table's schema group), so the notification goes out exactly when the
//! row commits. One [`PgSignalWatch`] per process holds a single `LISTEN`
//! connection for every channel the process cares about and fans each
//! notification out to the waiters, who then read their row. A wait
//! therefore ends when the row changes, instead of on the next tick of a
//! poll.
//!
//! A notification sent while the listening connection is down is lost,
//! so a reconnect tells every waiter to read its row again: nothing that
//! changed during the gap is missed. The row is the truth; a
//! notification only says when to look. Every waiter follows the same
//! recipe: subscribe, read the row, wait for a signal that concerns it
//! (or a recheck, or its deadline), read the row again.
//!
//! The listening connection is its own, outside the caller's pool: it is
//! held for the life of the process, and taking it from the pool would
//! leave one slot fewer for everything else.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use tokio::sync::broadcast;

/// The longest any request is held open waiting on a signal. A client
/// that wants to wait longer asks again; a hold this short never meets
/// an idle timeout between a pod and the broker, and a lost signal
/// costs a waiter at most this long.
pub const MAX_HOLD: Duration = Duration::from_secs(25);

/// What a waiter hears: one notification, or "every waiter should look
/// at its row again" because notifications may have been lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Heard {
    Signal { channel: &'static str, payload: Arc<str> },
    Recheck,
}

/// How many signals a slow waiter may fall behind by before it is told
/// to recheck instead (`broadcast`'s lag). Rechecking is always correct,
/// so the bound is about memory, not correctness.
const FANOUT_CAPACITY: usize = 1024;

/// Wait between attempts to listen again after the connection failed in
/// a way the listener does not recover from by itself.
const RETRY_DELAY: Duration = Duration::from_secs(1);

pub struct PgSignalWatch {
    /// The channels this watch listens on, fixed at start.
    channels: &'static [&'static str],
    /// A receiver kept only to hand out new ones. The pump holds the one
    /// sender, so if the pump ever stops, every waiter hears `Closed`
    /// and fails loudly instead of sleeping to its deadline.
    template: Mutex<broadcast::Receiver<Heard>>,
    /// The listening task. It lives exactly as long as the watch: when
    /// the process part that owns the watch goes, so does its connection
    /// (a test's database cannot be dropped while it is held).
    pump: tokio::task::JoinHandle<()>,
}

impl Drop for PgSignalWatch {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

impl PgSignalWatch {
    /// Start listening on `pool`'s database, on every one of `channels`.
    /// Fails when the first connection cannot be made; after that, the
    /// listener reconnects on its own and every waiter is told to
    /// recheck.
    pub async fn start(pool: &PgPool, channels: &'static [&'static str]) -> Result<Arc<Self>> {
        let own = PgPoolOptions::new()
            .max_connections(1)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with((*pool.connect_options()).clone())
            .await?;
        let listener = listen(&own, channels).await?;
        let (tx, template) = broadcast::channel(FANOUT_CAPACITY);
        let pump = tokio::spawn(pump(listener, own, channels, tx));
        Ok(Arc::new(Self { channels, template: Mutex::new(template), pump }))
    }

    /// Subscribe BEFORE reading the row: a change that lands between the
    /// read and the subscription would otherwise go unheard.
    pub fn subscribe(&self) -> Subscription {
        Subscription {
            rx: self.template.lock().expect("the template receiver is never poisoned").resubscribe(),
        }
    }

    /// Refuse a waiter whose channel this watch does not listen on: it
    /// would only ever wake at its deadline, which reads as "works, but
    /// slowly" instead of as the wiring mistake it is.
    pub fn require(&self, channel: &str) -> Result<()> {
        anyhow::ensure!(
            self.channels.contains(&channel),
            "this process's signal watch does not listen on '{channel}' (it listens on {:?}); \
             add the channel where the watch is started",
            self.channels
        );
        Ok(())
    }
}

/// One waiter's view of the watch.
pub struct Subscription {
    rx: broadcast::Receiver<Heard>,
}

/// Any source of what a watch hears; a test drives a waiter by sending
/// into the other end.
impl From<broadcast::Receiver<Heard>> for Subscription {
    fn from(rx: broadcast::Receiver<Heard>) -> Self {
        Self { rx }
    }
}

impl Subscription {
    /// The next thing heard. Falling behind is a recheck; the watch
    /// stopping is an error, loudly, since nothing would ever wake the
    /// waiter again.
    pub async fn next(&mut self) -> Result<Heard> {
        match self.rx.recv().await {
            Ok(heard) => Ok(heard),
            Err(broadcast::error::RecvError::Lagged(_)) => Ok(Heard::Recheck),
            Err(broadcast::error::RecvError::Closed) => {
                anyhow::bail!("the Postgres signal listener stopped")
            }
        }
    }

    /// Forget everything heard so far, for a waiter about to look at
    /// every row anyway: what it already heard is covered by that look,
    /// and only what arrives after it is a reason to look again.
    pub fn clear(&mut self) -> Result<()> {
        loop {
            match self.rx.try_recv() {
                Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
                Err(broadcast::error::TryRecvError::Closed) => {
                    anyhow::bail!("the Postgres signal listener stopped")
                }
            }
        }
    }

    /// Wait for a reason to read the row again: a signal that `concerns`
    /// (given its channel and payload) says is ours, or a recheck.
    /// `false` when `deadline` passed first.
    pub async fn woken_before(
        &mut self,
        deadline: tokio::time::Instant,
        concerns: impl Fn(&str, &str) -> bool,
    ) -> Result<bool> {
        loop {
            match tokio::time::timeout_at(deadline, self.next()).await {
                Err(_) => return Ok(false),
                Ok(heard) => {
                    if wakes(&heard?, &concerns) {
                        return Ok(true);
                    }
                }
            }
        }
    }
}

/// Whether `heard` is a reason for a waiter to look again.
fn wakes(heard: &Heard, concerns: &impl Fn(&str, &str) -> bool) -> bool {
    match heard {
        Heard::Recheck => true,
        Heard::Signal { channel, payload } => concerns(channel, payload),
    }
}

async fn listen(own: &PgPool, channels: &[&'static str]) -> Result<PgListener> {
    let mut listener = PgListener::connect_with(own).await?;
    listener.listen_all(channels.iter().copied()).await?;
    Ok(listener)
}

async fn pump(
    mut listener: PgListener,
    own: PgPool,
    channels: &'static [&'static str],
    tx: broadcast::Sender<Heard>,
) {
    loop {
        let e = hear(&mut listener, channels, &tx).await;
        // The listener only recovers by itself from a few kinds of
        // dropped socket, and keeps a connection that failed any other
        // way (a reset, a protocol error), which would fail the same way
        // on every call. So a fresh one, then everyone looks at their
        // row, since nothing was heard meanwhile. The old listener goes
        // first: it holds the one connection its pool allows, and a new
        // one could never get it while it lives.
        tracing::warn!(target: "weft_task_store::pg_signal", error = %e, "signal listener failed; listening again");
        drop(listener);
        listener = loop {
            tokio::time::sleep(RETRY_DELAY).await;
            match listen(&own, channels).await {
                Ok(fresh) => break fresh,
                Err(e) => tracing::warn!(target: "weft_task_store::pg_signal", error = %e, "signal listener cannot listen yet"),
            }
        };
        let _ = tx.send(Heard::Recheck);
    }
}

/// Fan out what `listener` hears until it fails, and hand back why.
async fn hear(
    listener: &mut PgListener,
    channels: &'static [&'static str],
    tx: &broadcast::Sender<Heard>,
) -> sqlx::Error {
    loop {
        // No receiver is fine on every send: nobody is waiting right now.
        match listener.try_recv().await {
            Ok(Some(notification)) => {
                match channels.iter().find(|c| **c == notification.channel()) {
                    Some(channel) => {
                        let _ = tx.send(Heard::Signal { channel, payload: notification.payload().into() });
                    }
                    None => tracing::error!(
                        target: "weft_task_store::pg_signal",
                        channel = notification.channel(),
                        "heard a channel this watch never listened on"
                    ),
                }
            }
            // The connection dropped and the listener has already made a
            // fresh one and listened again; what changed in between was
            // not heard.
            Ok(None) => { let _ = tx.send(Heard::Recheck); }
            Err(e) => return e,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(channel: &'static str, payload: &str) -> Heard {
        Heard::Signal { channel, payload: payload.into() }
    }

    #[test]
    fn a_recheck_wakes_every_waiter() {
        assert!(wakes(&Heard::Recheck, &|_: &str, _: &str| false));
    }

    #[test]
    fn a_signal_wakes_only_the_waiter_it_concerns() {
        let concerns = |c: &str, p: &str| c == "a" && p == "x";
        assert!(wakes(&signal("a", "x"), &concerns));
        assert!(!wakes(&signal("a", "y"), &concerns));
        assert!(!wakes(&signal("b", "x"), &concerns));
    }

    #[tokio::test]
    async fn clearing_forgets_what_was_heard_and_keeps_listening() {
        let (tx, rx) = broadcast::channel(8);
        let mut subscription = Subscription::from(rx);
        tx.send(signal("a", "x")).unwrap();
        tx.send(Heard::Recheck).unwrap();
        subscription.clear().unwrap();
        tx.send(signal("a", "y")).unwrap();
        assert_eq!(subscription.next().await.unwrap(), signal("a", "y"));
        drop(tx);
        assert!(subscription.clear().is_err(), "a stopped watch is loud");
    }
}
