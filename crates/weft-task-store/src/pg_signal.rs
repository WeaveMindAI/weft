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
//! so the watch says when it loses the connection ([`Heard::Lost`]) and a
//! reconnect tells every waiter to read its row again: nothing that
//! changed during the gap is missed. The row is the truth; a
//! notification only says when to look. Every waiter follows the same
//! recipe: subscribe, read the row, wait for a signal that concerns it
//! (or a recheck, or its deadline), read the row again.
//!
//! The listening connection is its own, outside the caller's pool: it is
//! held for the life of the process, and taking it from the pool would
//! leave one slot fewer for everything else. It may need another address
//! than the pool: a `LISTEN` belongs to one database session, which a
//! pooler that hands out a connection per transaction (a serverless
//! Postgres's pooled address) cannot keep. Every time it starts listening
//! it proves it hears back a notification of its own, so a session that
//! cannot listen is refused at once, naming the fix, rather than leaving
//! every waiter to sleep to its deadline.
//!
//! A process that scales to zero listens only while it has work
//! ([`Listening::WhileBusy`]). A serverless database (Neon) goes to sleep
//! once no query has run for a few minutes and closes every connection as
//! it does, the listening one included; listening again at once would wake
//! it straight back up, so a watch that loses its connection while nothing
//! in its process is busy ([`PgSignalWatch::busy`]) stays down until
//! something is, and one that cannot listen again stops trying once
//! nothing is busy any more.
//!
//! What a quiet process misses is another process's writes. The process
//! that wrote was busy making the write, so it heard it, and rings the
//! roles whose loops the write concerns (`weft-runtime`'s role waker);
//! that is how a loop never misses one. What this process keeps in memory
//! (a request's wait, a copy of rows, a worker's line) is told the watch
//! listens again ([`Heard::Resumed`]) and looks again for itself.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use anyhow::Result;
use tokio::sync::broadcast;

use crate::db::{Client as _, Session};
use crate::stopping::Deadline;

/// What a waiter hears: one notification, or "every waiter should look
/// at its row again" because notifications may have been lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Heard {
    Signal { channel: &'static str, payload: Arc<str> },
    Recheck,
    /// Listening again after a connection lost while nothing in the
    /// process was busy ([`Listening::WhileBusy`]): only another process's
    /// writes may have gone unheard meanwhile, so a
    /// waiter or a copy that keeps anything from before looks again as on
    /// a [`Heard::Recheck`]. A role's loops do not: what concerns them was
    /// rung by the process that wrote it (`crate::drain`).
    Resumed,
    /// The listening connection is gone: nothing is heard until a
    /// [`Heard::Recheck`] says it listens again. A waiter that reads its
    /// row anyway has nothing to do; a copy of rows kept in memory
    /// (`crate::held_copy`) stops trusting itself until then.
    Lost,
}

/// How many signals a slow waiter may fall behind by before it is told
/// to recheck instead (`broadcast`'s lag). Rechecking is always correct,
/// so the bound is about memory, not correctness.
const FANOUT_CAPACITY: usize = 1024;

/// Wait between attempts to listen again after the connection failed in
/// a way the listener does not recover from by itself.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// How long after its last busy moment a process still counts as busy
/// when its connection drops: a write that committed just before its
/// request ended may not have been heard yet.
const QUIET_AFTER: Duration = Duration::from_secs(5);

/// How long [`PgSignalWatch::busy`] waits for a quiet watch to listen
/// again before letting the work go ahead without it (the database is
/// down, and that work will say so itself).
const RESUME_WAIT: Duration = Duration::from_secs(10);

/// When a watch listens: for the life of its process, or only while the
/// process has work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Listening {
    /// A lost connection is made again at once: a machine's process, up
    /// for good.
    Always,
    /// A connection lost while nothing in the process is busy is left down
    /// until something is: a process that scales to zero, beside a
    /// database that sleeps (see the module docs).
    WhileBusy,
}

/// What the process is doing, as the watch needs to know it.
struct Demand {
    listening: Listening,
    /// Every [`Busy`] alive.
    held: AtomicUsize,
    /// When the last one ended.
    last_work: Mutex<Instant>,
    /// Told when something needs the watch while it does not listen.
    wanted: Notify,
    /// Told every time the watch listens again.
    listening_again: Notify,
    /// Set when work went ahead while the watch could not listen
    /// ([`PgSignalWatch::busy`] gave up waiting): its writes went unheard.
    unheard_work: AtomicBool,
}

impl Demand {
    /// Whether a connection lost now is left down.
    fn quiet_now(&self) -> bool {
        self.listening == Listening::WhileBusy
            && self.held.load(Ordering::Acquire) == 0
            && self.last_work.lock().expect("demand clock").elapsed() >= QUIET_AFTER
    }
}

/// Something in flight in a process whose watch listens only while it
/// has some ([`PgSignalWatch::busy`]); the watch may go quiet once every
/// one is dropped.
pub struct Busy {
    demand: Arc<Demand>,
}

impl Drop for Busy {
    fn drop(&mut self) {
        *self.demand.last_work.lock().expect("demand clock") = Instant::now();
        self.demand.held.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The watch a request holds busy while it is answered ([`keep_listening`]),
/// in the request's extensions. A handler that goes on to wait only on what
/// reaches the process another way (a worker's answer relayed to its
/// caller, news of a run heard from the install's news) lets go of it, so a
/// wait of hours leaves the database free to sleep; it holds
/// [`PgSignalWatch::busy`] itself around whatever it reads or writes.
#[cfg(feature = "serve")]
#[derive(Clone)]
pub struct RequestListening(Arc<Mutex<Option<Busy>>>);

#[cfg(feature = "serve")]
impl RequestListening {
    /// The request no longer needs the watch.
    pub fn let_go(&self) {
        self.0.lock().expect("a request's listening").take();
    }
}

/// Keep `watch` listening while a request is answered, to the end of its
/// answer's body: a stream of events is listened for until it closes. A
/// process that scales to zero lets its watch go quiet only between
/// requests ([`Listening::WhileBusy`]), and every request it answers may
/// write, so a quiet watch listens again before the request goes on. A
/// handler that let go ([`RequestListening`]) holds nothing.
#[cfg(feature = "serve")]
pub async fn keep_listening(
    axum::extract::State(watch): axum::extract::State<Arc<PgSignalWatch>>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let listening = RequestListening(Arc::new(Mutex::new(Some(watch.busy().await))));
    request.extensions_mut().insert(listening.clone());
    let answer = next.run(request).await;
    let Some(busy) = listening.0.lock().expect("a request's listening").take() else { return answer };
    answer.map(|body| axum::body::Body::new(Held { body, _busy: busy }))
}

/// An answer's body, holding its process busy until it ends.
#[cfg(feature = "serve")]
struct Held {
    body: axum::body::Body,
    _busy: Busy,
}

#[cfg(feature = "serve")]
impl http_body::Body for Held {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

/// The channel a watch proves it can hear on, every time it listens.
const PROBE_CHANNEL: &str = "weft_listen_probe";

/// The name the listening session gives itself, so it can be told apart
/// in `pg_stat_activity` from the connections that do work.
pub const WATCH_APPLICATION_NAME: &str = "weft_signal_watch";

/// How long a watch waits to hear its own probe back. A session that can
/// listen hears it in milliseconds.
const PROBE_WAIT: Duration = Duration::from_secs(5);

pub struct PgSignalWatch {
    /// The channels this watch listens on, fixed at start.
    channels: &'static [&'static str],
    /// A receiver kept only to hand out new ones. The pump holds the one
    /// sender, so if the pump ever stops, every waiter hears `Closed`
    /// and fails loudly instead of sleeping to its deadline.
    template: Mutex<broadcast::Receiver<Heard>>,
    /// Whether the watch listens right now: cleared before
    /// [`Heard::Lost`] goes out, set again before the [`Heard::Recheck`]
    /// that follows a reconnect. A subscriber that fell behind and missed
    /// either reads the state here.
    listening: Arc<AtomicBool>,
    demand: Arc<Demand>,
    /// What [`AlsoTold`] sends through: weak, so the pump stays the one
    /// sender that keeps waiters hearing.
    told: broadcast::WeakSender<Heard>,
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
    /// Start listening at `connect` (a session of its own, see the module
    /// docs), on every one of `channels`. Fails when the first connection
    /// cannot be made or cannot listen; after that, the listener
    /// reconnects on its own (as `listening` says) and every waiter is
    /// told to recheck.
    pub async fn start(connect: &crate::db::Config, channels: &'static [&'static str], listening: Listening) -> Result<Arc<Self>> {
        let mut own = connect.clone();
        own.application_name(WATCH_APPLICATION_NAME);
        let listener = listen(&own, channels).await?;
        let (tx, template) = broadcast::channel(FANOUT_CAPACITY);
        let told = tx.downgrade();
        let listening_now = Arc::new(AtomicBool::new(true));
        let demand = Arc::new(Demand {
            listening,
            held: AtomicUsize::new(0),
            last_work: Mutex::new(Instant::now()),
            wanted: Notify::new(),
            listening_again: Notify::new(),
            unheard_work: AtomicBool::new(false),
        });
        let pump = tokio::spawn(pump(listener, own, channels, tx, listening_now.clone(), demand.clone()));
        Ok(Arc::new(Self { channels, template: Mutex::new(template), listening: listening_now, demand, told, pump }))
    }

    /// Hold this while doing work that writes, or that waits on what this
    /// watch hears (a request, an open line, a command being carried out,
    /// a loop's drain): a watch that is down listens again first (waiting
    /// [`RESUME_WAIT`] at most), and does not go quiet while it is held. A
    /// no-op for a watch that listens [`Listening::Always`].
    pub async fn busy(&self) -> Busy {
        let demand = self.demand.clone();
        demand.held.fetch_add(1, Ordering::AcqRel);
        // The guard first, so a caller dropped while it waits gives it back.
        let held = Busy { demand: demand.clone() };
        if demand.listening == Listening::WhileBusy && !self.listening.load(Ordering::Acquire) {
            demand.wanted.notify_one();
            let resumed = tokio::time::timeout(RESUME_WAIT, async {
                loop {
                    let again = demand.listening_again.notified();
                    tokio::pin!(again);
                    again.as_mut().enable();
                    if self.listening.load(Ordering::Acquire) {
                        return;
                    }
                    again.await;
                }
            })
            .await;
            if resumed.is_err() {
                demand.unheard_work.store(true, Ordering::Release);
                tracing::warn!(
                    target: "weft_task_store::pg_signal",
                    "the signal listener is not listening again after {}s; going ahead without it",
                    RESUME_WAIT.as_secs()
                );
            }
        }
        held
    }

    /// Whether anything holds the watch right now ([`Self::busy`]).
    pub fn holds(&self) -> bool {
        self.demand.held.load(Ordering::Acquire) > 0
    }

    /// A way to tell every waiter what this process heard of the database's
    /// writes another way ([`AlsoTold`]).
    pub fn also_told(&self) -> AlsoTold {
        AlsoTold { channels: self.channels, told: self.told.clone() }
    }

    /// Subscribe BEFORE reading the row: a change that lands between the
    /// read and the subscription would otherwise go unheard.
    pub fn subscribe(&self) -> Subscription {
        Subscription {
            rx: self.template.lock().expect("the template receiver is never poisoned").resubscribe(),
            listening: self.listening.clone(),
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

/// What a process heard of the database's writes without its connection
/// (the install's news, which carries a few announcements while the
/// database sleeps and the watch is quiet), told to every waiter as the
/// watch would have told it. A waiter cannot tell the two apart, and hearing
/// one twice only costs it a look.
pub struct AlsoTold {
    channels: &'static [&'static str],
    told: broadcast::WeakSender<Heard>,
}

impl AlsoTold {
    /// Tell to waiters through `told` on `channels` (a test drives waiters
    /// with it, as [`Subscription::from`] does).
    pub fn over(told: &broadcast::Sender<Heard>, channels: &'static [&'static str]) -> Self {
        Self { channels, told: told.downgrade() }
    }

    /// `payload` was announced on `channel`. Refused on a channel the
    /// watch does not listen on (no waiter would be waiting on it); `false`
    /// once the watch stopped.
    pub fn signal(&self, channel: &str, payload: &str) -> Result<bool> {
        let Some(channel) = self.channels.iter().find(|c| **c == channel) else {
            anyhow::bail!("this process's signal watch does not listen on '{channel}' (it listens on {:?})", self.channels);
        };
        Ok(self.tell(Heard::Signal { channel, payload: payload.into() }))
    }

    fn tell(&self, heard: Heard) -> bool {
        let Some(told) = self.told.upgrade() else { return false };
        // No waiter right now is fine: nobody waits on it.
        let _ = told.send(heard);
        true
    }
}

/// One waiter's view of the watch.
pub struct Subscription {
    rx: broadcast::Receiver<Heard>,
    listening: Arc<AtomicBool>,
}

/// Any source of what a watch hears; a test drives a waiter by sending
/// into the other end. It counts as listening.
impl From<broadcast::Receiver<Heard>> for Subscription {
    fn from(rx: broadcast::Receiver<Heard>) -> Self {
        Self::with_listening(rx, Arc::new(AtomicBool::new(true)))
    }
}

impl Subscription {
    /// A subscription whose watch's listening state is `listening` (a
    /// test sets it as a watch's pump would).
    pub fn with_listening(rx: broadcast::Receiver<Heard>, listening: Arc<AtomicBool>) -> Self {
        Self { rx, listening }
    }

    /// Whether the watch listens right now (see [`PgSignalWatch`]'s
    /// `listening`).
    pub fn listening(&self) -> bool {
        self.listening.load(Ordering::Acquire)
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

    /// Wait, however long it takes, for a reason to read the row again: a
    /// signal that `concerns` (given its channel and payload) says is
    /// ours, or a recheck.
    pub async fn woken(&mut self, concerns: impl Fn(&str, &str) -> bool) -> Result<()> {
        loop {
            if wakes(&self.next().await?, &concerns) {
                return Ok(());
            }
        }
    }

    /// Wait for a reason to read the row again: a signal that `concerns`
    /// (given its channel and payload) says is ours, or a recheck.
    /// `false` when `deadline` passed first (an instant, or a held wait's
    /// [`Deadline`], which also passes once its copy is stopping).
    pub async fn woken_before(
        &mut self,
        deadline: impl Into<Deadline>,
        concerns: impl Fn(&str, &str) -> bool,
    ) -> Result<bool> {
        let deadline = deadline.into();
        let passed = deadline.passed();
        tokio::pin!(passed);
        loop {
            // What was heard first, as a timeout would: a wake already
            // there when the deadline comes still wakes.
            tokio::select! {
                biased;
                heard = self.next() => {
                    if wakes(&heard?, &concerns) {
                        return Ok(true);
                    }
                }
                () = &mut passed => return Ok(false),
            }
        }
    }
}

/// Whether `heard` is a reason for a waiter to look again.
fn wakes(heard: &Heard, concerns: &impl Fn(&str, &str) -> bool) -> bool {
    match heard {
        Heard::Recheck | Heard::Resumed => true,
        // The recheck that follows a reconnect is what wakes the waiter.
        Heard::Lost => false,
        Heard::Signal { channel, payload } => concerns(channel, payload),
    }
}

/// A session of its own at `own`, listening on every one of `channels`.
/// It is never made again behind weft's back: when it drops, the pump
/// decides whether to listen again, and a process with nothing busy must
/// not wake a sleeping database.
async fn listen(own: &crate::db::Config, channels: &[&'static str]) -> Result<Session> {
    let mut listener = Session::connect(own).await?;
    listener.listen(channels.iter().copied().chain([PROBE_CHANNEL])).await?;
    prove_it_hears(&mut listener).await?;
    Ok(listener)
}

/// Send a notification on this very session and wait to hear it back. A
/// pooler that runs each statement on whichever server connection is free
/// leaves the `LISTEN` on one and the notification on another, and the
/// session never hears anything.
async fn prove_it_hears(listener: &mut Session) -> Result<()> {
    let payload = uuid::Uuid::new_v4().to_string();
    listener.execute("SELECT pg_notify($1, $2)", &[&PROBE_CHANNEL, &payload]).await?;
    let heard = tokio::time::timeout(PROBE_WAIT, async {
        loop {
            match listener.notified().await? {
                Some(n) if n.channel() == PROBE_CHANNEL && n.payload() == payload => return anyhow::Ok(()),
                Some(_) => {}
                None => anyhow::bail!("the listening connection dropped while proving it can hear"),
            }
        }
    })
    .await;
    match heard {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "the database session cannot LISTEN: a notification it sent itself never came back within {}s. \
             Its address goes through a pooler that hands out a connection per transaction; set \
             WEFT_DATABASE_LISTEN_URL to a direct (session) address of the same database",
            PROBE_WAIT.as_secs()
        ),
    }
}

async fn pump(
    mut listener: Session,
    own: crate::db::Config,
    channels: &'static [&'static str],
    tx: broadcast::Sender<Heard>,
    listening: Arc<AtomicBool>,
    demand: Arc<Demand>,
) {
    loop {
        let e = hear(&mut listener, channels, &tx).await;
        // A fresh session, then everyone looks at their row, since nothing
        // was heard meanwhile. The old one is closed first, so the process
        // never holds two.
        drop(listener);
        listening.store(false, Ordering::Release);
        let _ = tx.send(Heard::Lost);
        // Lost while busy, or with work that went ahead before it listened
        // again, this process's own writes went unheard (nothing rang for
        // them), so every loop looks again once it listens, even if that
        // work ended before then.
        let dropped_quiet = demand.quiet_now();
        if dropped_quiet {
            // The database closing an idle process's connection as it goes
            // to sleep: listen again once there is work, not before, or the
            // listening would wake it straight back up.
            tracing::info!(target: "weft_task_store::pg_signal", error = %e, "the signal listener's connection closed while nothing here is busy; listening again when work arrives");
            wait_for_work(&demand).await;
        } else {
            tracing::warn!(target: "weft_task_store::pg_signal", error = %e, "signal listener failed; listening again");
        }
        // Work waits on a watch coming back from quiet: no pause before
        // its first try.
        let mut pause = !dropped_quiet;
        listener = loop {
            if pause {
                tokio::time::sleep(RETRY_DELAY).await;
                // The work that wanted the watch ended while it could not
                // listen: stop trying until there is work again, so a
                // database that cannot be reached, or went to sleep, is
                // not tried every second for nothing.
                if demand.quiet_now() {
                    tracing::info!(target: "weft_task_store::pg_signal", "nothing here is busy any more; listening again when work arrives");
                    wait_for_work(&demand).await;
                }
            }
            pause = true;
            match listen(&own, channels).await {
                Ok(fresh) => break fresh,
                Err(e) => tracing::warn!(target: "weft_task_store::pg_signal", error = %e, "signal listener cannot listen yet"),
            }
        };
        listening.store(true, Ordering::Release);
        demand.listening_again.notify_waiters();
        let own_writes_unheard = !dropped_quiet | demand.unheard_work.swap(false, Ordering::AcqRel);
        let _ = tx.send(if own_writes_unheard { Heard::Recheck } else { Heard::Resumed });
    }
}

/// Until something in the process holds the watch ([`PgSignalWatch::busy`]).
async fn wait_for_work(demand: &Demand) {
    loop {
        let wanted = demand.wanted.notified();
        tokio::pin!(wanted);
        wanted.as_mut().enable();
        if demand.held.load(Ordering::Acquire) > 0 {
            return;
        }
        wanted.await;
    }
}

/// Fan out what `listener` hears until it fails, and hand back why.
async fn hear(
    listener: &mut Session,
    channels: &'static [&'static str],
    tx: &broadcast::Sender<Heard>,
) -> anyhow::Error {
    loop {
        // No receiver is fine on every send: nobody is waiting right now.
        match listener.notified().await {
            Ok(Some(notification)) if notification.channel() == PROBE_CHANNEL => {}
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
            // The connection closed, or broke (a database that went
            // silent is given up on within about a minute, `crate::db::config`):
            // the pump says the connection is lost, listens again the way
            // it first did, and tells everyone to look again.
            Ok(None) => return anyhow::anyhow!("the listening connection closed"),
            Err(e) => return e.into(),
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
        assert!(wakes(&Heard::Resumed, &|_: &str, _: &str| false));
    }

    #[test]
    fn a_signal_wakes_only_the_waiter_it_concerns() {
        let concerns = |c: &str, p: &str| c == "a" && p == "x";
        assert!(wakes(&signal("a", "x"), &concerns));
        assert!(!wakes(&signal("a", "y"), &concerns));
        assert!(!wakes(&signal("b", "x"), &concerns));
    }

    /// What a process heard another way reaches every waiter as the watch
    /// would have told it; a channel nobody listens on is refused, and a
    /// watch that stopped hears nothing more.
    #[tokio::test]
    async fn what_is_told_another_way_reaches_every_waiter() {
        let (tx, rx) = broadcast::channel(8);
        let mut subscription = Subscription::from(rx);
        let told = AlsoTold::over(&tx, &["a"]);
        assert!(told.signal("a", "x").unwrap());
        assert!(told.signal("b", "x").is_err(), "a channel this watch does not listen on");
        assert_eq!(subscription.next().await.unwrap(), signal("a", "x"));
        drop(tx);
        assert!(!told.signal("a", "y").unwrap(), "the watch stopped");
        assert!(subscription.next().await.is_err(), "and its waiters hear so: telling it another way keeps nothing alive");
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

    /// A held wait ends the moment its copy starts stopping, long before
    /// its deadline, answering as the deadline would; a wait on a copy
    /// that runs waits its whole deadline, and one whose copy was already
    /// stopping ends at once.
    #[tokio::test(start_paused = true)]
    async fn a_held_wait_ends_as_its_copy_starts_stopping() {
        let (tx, rx) = broadcast::channel(8);
        let stopping = crate::stopping::Stopping::new();
        let held = Deadline::at(Instant::now() + Duration::from_secs(60)).or_stop(&stopping);
        let mut subscription = Subscription::from(rx);
        let started = Instant::now();
        let waiting = tokio::spawn(async move { subscription.woken_before(held, |_: &str, _: &str| false).await });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!waiting.is_finished(), "a copy that runs keeps holding");
        stopping.begin();
        assert!(!waiting.await.unwrap().unwrap(), "ended as its deadline would");
        assert_eq!(started.elapsed(), Duration::from_secs(1), "at the stop, not the deadline");

        let mut subscription = Subscription::from(tx.subscribe());
        let started = Instant::now();
        let held = Deadline::at(Instant::now() + Duration::from_secs(60)).or_stop(&stopping);
        assert!(!subscription.woken_before(held, |_: &str, _: &str| false).await.unwrap());
        assert_eq!(started.elapsed(), Duration::ZERO, "already stopping");

        let started = Instant::now();
        let unheld = Deadline::at(Instant::now() + Duration::from_secs(60));
        assert!(!subscription.woken_before(unheld, |_: &str, _: &str| false).await.unwrap());
        assert_eq!(started.elapsed(), Duration::from_secs(60), "a wait not held for a caller ignores the stop");
    }
}
