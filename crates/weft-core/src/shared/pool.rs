//! A pool of live things a worker keeps for its runs (`RunContext::pool`):
//! connections to a database, ready interpreter processes. Built on
//! [`super`]: the pool itself is one shared thing, filed under the key it
//! is asked by, and what it holds is a list of idle items and a count of
//! the ones out.
//!
//! ```text
//! idle list  [conn, conn, conn]          (newest last)
//! take one   -> newest idle, still fresh and passing the check? else make one
//! cap        -> at most `max` out at once, the rest wait (a cancel ends the wait)
//! give back  -> when the lease drops: reset it, then idle again
//! close      -> idle longer than `idle_close`? let go
//! ```
//!
//! The node keeps only its own steps: how to make one, how to check one is
//! still good, how to clear what a run left on it. Everything else is
//! here.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
// tokio's clock, the one the close timer sleeps on, so the two agree
// (and a paused test clock moves both).
use tokio::time::Instant;

use super::{SharedHandle, SharedKey};
use crate::cancellation::CancellationFlag;
use crate::context::RunContext;
use crate::error::{WeftError, WeftResult};

/// What a lease runs on its item before it goes back to the pool: the item
/// back when it is fit to hand out again, an error (logged) when it is
/// let go instead.
type Reset<T> = Arc<dyn Fn(T) -> std::pin::Pin<Box<dyn Future<Output = WeftResult<T>> + Send>> + Send + Sync>;

/// One worker's pool of `T` (see the module doc): the shared thing a
/// [`PoolAsk`] files under its key.
pub struct Pool<T> {
    /// Oldest first, each with when it went idle.
    idle: Mutex<Vec<(T, Instant)>>,
    /// One place per item out, when the pool has a cap.
    room: Option<Arc<tokio::sync::Semaphore>>,
    max: Option<usize>,
    idle_close: Option<Duration>,
}

impl<T: Send + 'static> Pool<T> {
    fn new(max: Option<usize>, idle_close: Option<Duration>) -> Self {
        Self { idle: Mutex::new(Vec::new()), room: max.map(|n| Arc::new(tokio::sync::Semaphore::new(n))), max, idle_close }
    }

    /// The newest idle item that is still fresh at `now` and passes
    /// `check`. Every one idle too long, and every one the check finds
    /// gone, is let go on the way.
    fn take_idle(&self, now: Instant, check: &mut dyn FnMut(&mut T) -> bool) -> Option<T> {
        let mut idle = self.idle.lock().expect("pool poisoned");
        if let Some(close) = self.idle_close {
            idle.retain(|(_, since)| still_fresh(*since, now, close));
        }
        while let Some((mut item, _)) = idle.pop() {
            if check(&mut item) {
                return Some(item);
            }
        }
        None
    }

    /// Put `item` back, idle from `now`, and let it go once it has sat
    /// idle for `idle_close` if nobody took it. The close is a timer, so a
    /// worker without CPU between requests runs it late; the next take
    /// lets the item go then, before anything uses it.
    fn give_back(self: Arc<Self>, item: T, now: Instant) {
        self.idle.lock().expect("pool poisoned").push((item, now));
        let Some(close) = self.idle_close else { return };
        let pool = Arc::downgrade(&self);
        tokio::spawn(async move {
            tokio::time::sleep(close).await;
            if let Some(pool) = pool.upgrade() {
                let now = Instant::now();
                pool.idle.lock().expect("pool poisoned").retain(|(_, since)| still_fresh(*since, now, close));
            }
        });
    }

    /// How many items sit idle.
    #[cfg(test)]
    fn idle_len(&self) -> usize {
        self.idle.lock().expect("pool poisoned").len()
    }
}

/// Whether an item idle since `since` may still be handed out at `now`,
/// under an idle close of `close`.
fn still_fresh(since: Instant, now: Instant, close: Duration) -> bool {
    now.saturating_duration_since(since) < close
}

/// One run's ask for an item of a pool (`RunContext::pool`): set how the
/// pool behaves, then [`Self::take`].
pub struct PoolAsk<'c, K, T, M> {
    pub(crate) ctx: &'c RunContext,
    pub(crate) key: K,
    pub(crate) make: M,
    pub(crate) settings: Settings<'c, T>,
}

/// A pool's liveness check of an idle item ([`PoolAsk::check`]).
type Check<'c, T> = Box<dyn FnMut(&mut T) -> bool + Send + 'c>;

/// How a pool behaves, as one ask says it ([`PoolAsk`]).
pub(crate) struct Settings<'c, T> {
    max: Option<usize>,
    idle_close: Option<Duration>,
    check: Option<Check<'c, T>>,
    reset: Option<Reset<T>>,
}

impl<T> Default for Settings<'_, T> {
    fn default() -> Self {
        Self { max: None, idle_close: None, check: None, reset: None }
    }
}

impl<'c, K, T, M> PoolAsk<'c, K, T, M> {
    /// At most `n` items out at once on this worker; a take past that
    /// waits for one to come back (and ends with the cancelled error if
    /// the run is cancelled meanwhile). Every ask of one pool says the
    /// same cap: one that says another, or none, is refused. With none,
    /// nobody waits.
    pub fn max(mut self, n: usize) -> Self {
        self.settings.max = Some(n);
        self
    }

    /// Let an item go once it sat idle this long; one idle longer is never
    /// handed out either (the other side may have closed it meanwhile).
    /// Every ask of one pool says the same. With none, an idle item is
    /// kept for as long as the pool is (`shared_idle_seconds`).
    pub fn idle_close(mut self, after: Duration) -> Self {
        self.settings.idle_close = Some(after);
        self
    }

    /// Whether an idle item is still good to hand out (a connection still
    /// open, a process still running). One that is not is let go and the
    /// next is tried, then a new one is made.
    pub fn check(mut self, alive: impl FnMut(&mut T) -> bool + Send + 'c) -> Self {
        self.settings.check = Some(Box::new(alive));
        self
    }

    /// What clears an item before it goes back to the pool (a session's
    /// settings and open transaction): it hands the item back when it is
    /// fit for the next run, or an error, and then the item is let go and
    /// the error logged. The lease keeps its place under the cap while
    /// this runs.
    pub fn reset<F, Fut>(mut self, reset: F) -> Self
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = WeftResult<T>> + Send + 'static,
    {
        self.settings.reset = Some(Arc::new(move |item| Box::pin(reset(item))));
        self
    }

    /// An item of the pool, for this run: an idle one that is still fresh
    /// and passes the check, else one `make` builds from what the key
    /// resolves to (the opened connection for an access, nothing for a
    /// name). Past the cap it waits for one to come back, and that wait
    /// ends with the cancelled error the moment the run is cancelled. A
    /// failed make takes no place.
    pub async fn take<Fut>(self) -> WeftResult<Lease<T>>
    where
        K: SharedKey + Send + 'c,
        T: Send + 'static,
        M: FnOnce(K::From) -> Fut,
        Fut: Future<Output = WeftResult<T>>,
    {
        let PoolAsk { ctx, key, make, settings } = self;
        let super::Resolved { slot, fingerprint, from } = key.resolve(ctx).await?;
        settings.take(&ctx.handle.shared(), &slot, fingerprint, ctx.cancellation(), || make(from)).await
    }
}

impl<T: Send + 'static> Settings<'_, T> {
    /// [`PoolAsk::take`], once the key named the pool: `slot` and
    /// `fingerprint` in `shared`, for the run `cancel` belongs to.
    async fn take<Fut>(
        self,
        shared: &super::Shared,
        slot: &str,
        fingerprint: u64,
        cancel: Arc<CancellationFlag>,
        make: impl FnOnce() -> Fut,
    ) -> WeftResult<Lease<T>>
    where
        Fut: Future<Output = WeftResult<T>>,
    {
        let Settings { max, idle_close, check, reset } = self;
        if max == Some(0) {
            return Err(WeftError::Config("a pool capped at 0 items could never hand one out; give a max of 1 or more".into()));
        }
        let pool: SharedHandle<Pool<T>> = shared.get_or_build(slot, fingerprint, None, || async { Ok(Pool::new(max, idle_close)) }).await?;
        if pool.max != max || pool.idle_close != idle_close {
            return Err(WeftError::Config(format!(
                "the pool '{slot}' is asked for with max {} and idle close {}, and it is held with max {} and idle close {}: \
                 every ask of one pool says the same",
                said(max),
                said(idle_close.map(|d| format!("{d:?}"))),
                said(pool.max),
                said(pool.idle_close.map(|d| format!("{d:?}"))),
            )));
        }
        let place = match &pool.room {
            Some(room) => Some(cancel.race(room.clone().acquire_owned()).await?.expect("a pool's room is never closed")),
            None => None,
        };
        let mut check = check.unwrap_or_else(|| Box::new(|_: &mut T| true));
        let item = match pool.take_idle(Instant::now(), &mut *check) {
            Some(item) => item,
            None => make().await?,
        };
        Ok(Lease { item: Some(item), pool: Some(pool), place, reset, cancel })
    }
}

/// A setting as a refusal says it.
fn said(setting: Option<impl std::fmt::Display>) -> String {
    setting.map_or("none".to_string(), |s| s.to_string())
}

/// An item of a pool, for one run's use ([`PoolAsk::take`]): it derefs to
/// the item, and goes back to the pool when dropped (through the pool's
/// reset, when it has one). Not when the run was cancelled with it in
/// hand: whatever it was doing may still be under way on it (a statement
/// running on a connection), so it is let go, which stops it. Nor after
/// [`Self::discard`].
pub struct Lease<T: Send + 'static> {
    item: Option<T>,
    pool: Option<SharedHandle<Pool<T>>>,
    /// Held until the item is back in the pool or let go, so its place
    /// under the cap stays taken while it is reset.
    place: Option<tokio::sync::OwnedSemaphorePermit>,
    reset: Option<Reset<T>>,
    cancel: Arc<CancellationFlag>,
}

impl<T: Send + 'static> Lease<T> {
    /// Let the item go instead of giving it back: it is no good for the
    /// next run (a process that stopped answering, a connection whose
    /// state this run cannot vouch for).
    pub fn discard(mut self) {
        self.item = None;
    }
}

impl<T: Send + 'static> std::ops::Deref for Lease<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.item.as_ref().expect("a lease holds its item until it is dropped")
    }
}

impl<T: Send + 'static> std::ops::DerefMut for Lease<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.item.as_mut().expect("a lease holds its item until it is dropped")
    }
}

impl<T: Send + 'static> Drop for Lease<T> {
    fn drop(&mut self) {
        let (Some(item), Some(pool)) = (self.item.take(), self.pool.take()) else { return };
        if self.cancel.is_cancelled() {
            return;
        }
        let place = self.place.take();
        match self.reset.take() {
            None => pool.arc().give_back(item, Instant::now()),
            Some(reset) => {
                tokio::spawn(async move {
                    match reset(item).await {
                        Ok(item) => pool.arc().give_back(item, Instant::now()),
                        Err(e) => tracing::warn!(target: "weft_core::pool", error = %e, "a pooled item is let go, its reset failed"),
                    }
                    drop(place);
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::Shared;
    use super::*;
    use crate::run::CancelCause;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A fake item: a number, and whether it is still alive.
    #[derive(Debug)]
    struct Conn {
        id: usize,
        alive: bool,
    }

    /// A worker's map, a run's flag, and a count of items made: what a
    /// pool touches.
    struct Rig {
        shared: Arc<Shared>,
        cancel: Arc<CancellationFlag>,
        made: Arc<AtomicUsize>,
    }

    impl Rig {
        fn new(idle: Duration) -> Self {
            Self { shared: Shared::new(idle), cancel: CancellationFlag::new_arc(), made: Arc::new(AtomicUsize::new(0)) }
        }

        /// Take from the pool "db" as `settings` says, making a fresh
        /// numbered item when none is idle.
        async fn take(&self, settings: Settings<'static, Conn>) -> WeftResult<Lease<Conn>> {
            let made = self.made.clone();
            settings
                .take(&self.shared, "named:db", 0, self.cancel.clone(), move || async move {
                    Ok(Conn { id: made.fetch_add(1, Ordering::SeqCst), alive: true })
                })
                .await
        }

        async fn pool(&self) -> SharedHandle<Pool<Conn>> {
            self.shared.get_or_build::<Pool<Conn>, _, _>("named:db", 0, None, || async { unreachable!("the pool exists") }).await.unwrap()
        }
    }

    fn capped(n: usize) -> Settings<'static, Conn> {
        Settings { max: Some(n), ..Settings::default() }
    }

    #[tokio::test]
    async fn an_item_given_back_is_reused_and_one_out_is_never_shared() {
        let rig = Rig::new(Duration::from_secs(60));
        let a = rig.take(Settings::default()).await.unwrap();
        let b = rig.take(Settings::default()).await.unwrap();
        assert_ne!(a.id, b.id, "two out at once are two items");
        let a_id = a.id;
        drop(a);
        let again = rig.take(Settings::default()).await.unwrap();
        assert_eq!(again.id, a_id, "the one given back is handed out again");
        assert_eq!(rig.made.load(Ordering::SeqCst), 2);
    }

    crate::stress_test!(
        name: past_its_cap_a_take_waits_for_one_to_come_back,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            let rig = Arc::new(Rig::new(Duration::from_secs(60)));
            let first = rig.take(capped(1)).await.unwrap();
            let waiting = tokio::time::timeout(Duration::from_millis(50), rig.take(capped(1))).await;
            assert!(waiting.is_err(), "one place, taken");
            let first_id = first.id;
            let next = tokio::spawn({
                let rig = rig.clone();
                async move { rig.take(capped(1)).await.map(|lease| lease.id) }
            });
            drop(first);
            let got = tokio::time::timeout(Duration::from_secs(5), next).await.expect("a place came free").unwrap().unwrap();
            assert_eq!(got, first_id, "the one given back, not a new one");
            assert_eq!(rig.made.load(Ordering::SeqCst), 1);
        }
    );

    #[tokio::test]
    async fn an_idle_item_failing_its_check_is_let_go_and_a_new_one_made() {
        let rig = Rig::new(Duration::from_secs(60));
        let mut lease = rig.take(Settings::default()).await.unwrap();
        lease.alive = false;
        drop(lease);
        let checked = Settings { check: Some(Box::new(|c: &mut Conn| c.alive)), ..Settings::default() };
        let fresh = rig.take(checked).await.unwrap();
        assert_eq!(fresh.id, 1, "the dead one was let go");
        drop(fresh);
        assert_eq!(rig.pool().await.idle_len(), 1, "only the live one is idle");
    }

    #[tokio::test(start_paused = true)]
    async fn an_item_idle_past_its_close_is_let_go() {
        let rig = Rig::new(Duration::from_secs(600));
        let close = Duration::from_secs(5);
        let closing = || Settings { idle_close: Some(close), ..Settings::default() };
        drop(rig.take(closing()).await.unwrap());
        let pool = rig.pool().await;
        assert_eq!(pool.idle_len(), 1);
        // The close timer, on the paused clock.
        tokio::time::sleep(close + Duration::from_millis(10)).await;
        assert_eq!(pool.idle_len(), 0, "let go once idle past its close");
        assert_eq!(rig.take(closing()).await.unwrap().id, 1, "a new one made");
    }

    /// An item past its close that the timer has not reached yet (a
    /// worker without CPU between requests) is let go at the next take,
    /// never handed out. Pure decision, so it is pinned on its own.
    #[test]
    fn an_item_idle_past_its_close_is_stale_whatever_the_timer_did() {
        let since = Instant::now();
        let close = Duration::from_secs(5);
        assert!(still_fresh(since, since + Duration::from_secs(4), close));
        assert!(!still_fresh(since, since + close, close));
    }

    #[tokio::test]
    async fn a_reset_runs_before_an_item_is_idle_again_and_a_failed_one_lets_it_go() {
        let rig = Rig::new(Duration::from_secs(60));
        let resets = Arc::new(AtomicUsize::new(0));
        let resetting = || {
            let resets = resets.clone();
            let reset: Reset<Conn> = Arc::new(move |c: Conn| {
                resets.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { if c.alive { Ok(c) } else { Err(WeftError::NodeFailed("left dirty".into())) } })
            });
            Settings { max: Some(1), reset: Some(reset), ..Settings::default() }
        };
        drop(rig.take(resetting()).await.unwrap());
        let mut lease = tokio::time::timeout(Duration::from_secs(5), rig.take(resetting()))
            .await
            .expect("the place comes back once reset")
            .unwrap();
        assert_eq!((lease.id, resets.load(Ordering::SeqCst)), (0, 1), "reset, then reused");
        lease.alive = false;
        drop(lease);
        let next = tokio::time::timeout(Duration::from_secs(5), rig.take(resetting()))
            .await
            .expect("a failed reset gives its place back too")
            .unwrap();
        assert_eq!(next.id, 1, "the one whose reset failed was let go");
    }

    #[tokio::test]
    async fn a_lease_dropped_under_a_cancel_or_discarded_never_goes_back() {
        let rig = Rig::new(Duration::from_secs(60));
        rig.take(Settings::default()).await.unwrap().discard();
        let lease = rig.take(Settings::default()).await.unwrap();
        assert_eq!(lease.id, 1, "the discarded one was let go");
        rig.cancel.cancel_because(CancelCause::User);
        drop(lease);
        assert_eq!(rig.pool().await.idle_len(), 0, "dropped under a cancel: let go");
    }

    crate::stress_test!(
        name: waiting_for_a_place_ends_on_cancel,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            let rig = Arc::new(Rig::new(Duration::from_secs(60)));
            let _held = rig.take(capped(1)).await.unwrap();
            let waiting = tokio::spawn({
                let rig = rig.clone();
                async move { rig.take(capped(1)).await.map(|_| ()) }
            });
            rig.cancel.cancel_because(CancelCause::User);
            let ended = tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("the wait ended").unwrap();
            assert!(matches!(ended, Err(WeftError::Cancelled)), "{ended:?}");
        }
    );

    #[tokio::test]
    async fn a_cap_of_none_or_another_than_held_is_refused() {
        let rig = Rig::new(Duration::from_secs(60));
        assert!(matches!(rig.take(capped(0)).await, Err(WeftError::Config(_))));
        let _held = rig.take(capped(2)).await.unwrap();
        let other = rig.take(capped(3)).await;
        assert!(matches!(&other, Err(WeftError::Config(why)) if why.contains("says the same")), "{:?}", other.err());
        assert!(rig.take(Settings::default()).await.is_err(), "none is another cap too");
    }
}
