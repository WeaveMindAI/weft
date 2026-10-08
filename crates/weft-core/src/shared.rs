//! Live things a worker holds for its runs to share
//! (`ExecutionContext::shared`): an open pool of database connections, a
//! set of ready interpreter processes. Each is built the first time a run
//! asks for it, handed to every run after that, let go once nothing used
//! it for the project's `shared_idle_seconds` worker lever, and gone with
//! the worker. A worker serves one project, so nothing here is ever
//! another project's, and a project that scales to zero holds nothing.
//!
//! A run asks by a key ([`SharedKey`]), and the KIND of key decides
//! everything else: what the thing is filed under, what makes it stale,
//! and what its build is handed. A connection ([`Access`]) is filed under
//! the connection and stale once its values change (a new password,
//! after its infra pushed one), and its build gets the opened
//! connection. A name (`&str`) is filed under the name, never stale, and
//! its build gets nothing. A new kind of key is one more impl of
//! [`SharedKey`]; no caller changes.
//!
//! A thing may also say how many runs may use it at once on one worker
//! ([`SharedAsk::with_limit`]): a pool of database connections that
//! must stay under the database's own limit, say. A run past the limit
//! waits for one to finish with it; with no limit, nobody waits. The
//! limit is the thing's own: a thing that replaces it (its connection's
//! values changed) has its own places while runs still hold the old one,
//! and an [`SharedHandle::arc`] kept past its handle holds no place.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::access::{Access, OpenedConnection};
use crate::context::ExecutionContext;
use crate::error::WeftResult;

/// What a run asks for a shared thing by (see the module doc).
pub trait SharedKey {
    /// What the build is handed.
    type From: Send;

    /// Where the thing is filed, what it is built from, and what the build
    /// gets, read through the run's own `ctx`.
    fn resolve(self, ctx: &ExecutionContext) -> impl Future<Output = WeftResult<Resolved<Self::From>>> + Send;
}

/// A key, resolved for one ask.
pub struct Resolved<F> {
    /// What the thing is filed under, unique across kinds.
    pub slot: String,
    /// What the thing was built from beyond its slot: a different one
    /// replaces it.
    pub fingerprint: u64,
    pub from: F,
}

/// A connection: built from its opened values, rebuilt once they change.
impl SharedKey for &Access {
    type From = OpenedConnection;

    async fn resolve(self, ctx: &ExecutionContext) -> WeftResult<Resolved<OpenedConnection>> {
        let opened = ctx.open(self).await?;
        Ok(Resolved {
            slot: format!("connection:{}", self.access_id()),
            fingerprint: fingerprint_of(opened.values()),
            from: opened,
        })
    }
}

/// A name the node chooses: built once, never stale.
impl SharedKey for &str {
    type From = ();

    async fn resolve(self, _ctx: &ExecutionContext) -> WeftResult<Resolved<()>> {
        Ok(Resolved { slot: named_slot(self), fingerprint: 0, from: () })
    }
}

/// Where a thing asked for by name is filed, whoever asks: a node through
/// `ctx.shared(name, ..)`, or code under a node's calls through
/// [`Shared::named`].
fn named_slot(name: &str) -> String {
    format!("named:{name}")
}

/// What a worker holds for its runs (see the module doc).
pub struct Shared {
    things: Mutex<HashMap<(TypeId, String), Arc<Held>>>,
    /// How long a thing nothing used is held.
    idle: Duration,
}

struct Held {
    fingerprint: u64,
    value: tokio::sync::OnceCell<Arc<dyn Any + Send + Sync>>,
    /// When a run last asked for it or let go of it. A thing a run holds
    /// is in use whatever this says ([`Shared::sweep`]).
    last_used: Mutex<Instant>,
    /// How many runs may use it at once, when it says, and the room that
    /// holds them to it.
    limit: Option<usize>,
    room: Option<Arc<tokio::sync::Semaphore>>,
}

/// A shared thing, for one run's use: what [`SharedAsk`] answers. Dropping
/// it gives back its place, when the thing has a limit, and starts its idle
/// window again.
pub struct SharedHandle<T> {
    value: Arc<T>,
    _in_use: InUse,
}

/// A run's hold on a [`Held`]: while one lives, the sweep keeps the thing
/// (so a run using a pool longer than the idle window never sees a second
/// one built beside it, past its limit), and its drop marks the thing
/// used.
struct InUse {
    _place: Option<tokio::sync::OwnedSemaphorePermit>,
    held: Arc<Held>,
}

impl Drop for InUse {
    fn drop(&mut self) {
        self.held.touch();
    }
}

impl<T> SharedHandle<T> {
    /// The thing itself, for keeping past this handle (a task that gives
    /// a connection back to its pool after the run let go of it).
    pub fn arc(&self) -> Arc<T> {
        self.value.clone()
    }
}

impl<T> std::ops::Deref for SharedHandle<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

/// One run's ask for a shared thing (`ExecutionContext::shared`): awaited
/// as it is, or with a limit first.
pub struct SharedAsk<'c, K, F> {
    pub(crate) ctx: &'c ExecutionContext,
    pub(crate) key: K,
    pub(crate) build: F,
    pub(crate) limit: Option<usize>,
}

impl<K, F> SharedAsk<'_, K, F> {
    /// At most `runs` runs on this worker use the thing at once; the next
    /// one waits until one of them drops its handle. Every ask for the
    /// thing says the same limit (the same code asks the same way): one
    /// that says another, or a limit of none, is refused.
    pub fn with_limit(mut self, runs: usize) -> Self {
        self.limit = Some(runs);
        self
    }
}

impl<'c, K, T, F, Fut> std::future::IntoFuture for SharedAsk<'c, K, F>
where
    K: SharedKey + Send + 'c,
    T: Send + Sync + 'static,
    F: FnOnce(K::From) -> Fut + Send + 'c,
    Fut: Future<Output = WeftResult<T>> + Send + 'c,
{
    type Output = WeftResult<SharedHandle<T>>;
    type IntoFuture = std::pin::Pin<Box<dyn Future<Output = Self::Output> + Send + 'c>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let SharedAsk { ctx, key, build, limit } = self;
            let Resolved { slot, fingerprint, from } = key.resolve(ctx).await?;
            ctx.handle.shared().get_or_build(&slot, fingerprint, limit, || build(from)).await
        })
    }
}

impl Shared {
    pub fn new(idle: Duration) -> Arc<Self> {
        Arc::new(Self { things: Mutex::new(HashMap::new()), idle })
    }

    /// The `T` filed under `slot` and `fingerprint`, built by `build` when
    /// there is none: the first time, after it went idle, or when it was
    /// built from another fingerprint (which it replaces). Built once
    /// however many runs ask at the same moment. A build that fails holds
    /// nothing, so the next ask builds again.
    pub async fn get_or_build<T, F, Fut>(&self, slot: &str, fingerprint: u64, limit: Option<usize>, build: F) -> WeftResult<SharedHandle<T>>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = WeftResult<T>>,
    {
        if limit == Some(0) {
            return Err(crate::error::WeftError::Config(format!(
                "the shared '{slot}' is limited to 0 runs at once, which no run could ever use; give a limit of 1 or more"
            )));
        }
        let held = {
            let mut things = self.things.lock().expect("shared poisoned");
            let entry = things.entry((TypeId::of::<T>(), slot.to_string())).or_insert_with(|| Arc::new(Held::new(fingerprint, limit)));
            if entry.fingerprint != fingerprint {
                *entry = Arc::new(Held::new(fingerprint, limit));
            }
            if entry.limit != limit {
                return Err(crate::error::WeftError::Config(format!(
                    "the shared '{slot}' is asked for with a limit of {} runs at once, and it is held with {}: every \
                     ask for one shared thing says the same limit",
                    limit.map_or("none".to_string(), |n| n.to_string()),
                    entry.limit.map_or("none".to_string(), |n| n.to_string()),
                )));
            }
            entry.clone()
        };
        held.touch();
        let value = held
            .value
            .get_or_try_init(|| async { build().await.map(|built| Arc::new(built) as Arc<dyn Any + Send + Sync>) })
            .await?
            .clone();
        let place = match &held.room {
            Some(room) => Some(room.clone().acquire_owned().await.expect("a shared thing's room is never closed")),
            None => None,
        };
        Ok(SharedHandle { value: value.downcast::<T>().expect("a thing is filed under its own type"), _in_use: InUse { _place: place, held } })
    }

    /// The `T` filed under `name`, as `ctx.shared(name, build)` answers it,
    /// for code that holds the worker's map but runs under no `ctx` (a
    /// provider meter, below a node's HTTP calls).
    pub async fn named<T, F, Fut>(&self, name: &str, build: F) -> WeftResult<SharedHandle<T>>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = WeftResult<T>>,
    {
        self.get_or_build(&named_slot(name), 0, None, build).await
    }

    /// Let go of every thing no run holds and none asked for or let go of
    /// within the idle window.
    pub fn sweep(&self, now: Instant) {
        let idle = self.idle;
        self.things.lock().expect("shared poisoned").retain(|_, held| {
            // The map's own reference, plus one per handle a run holds.
            Arc::strong_count(held) > 1 || now.saturating_duration_since(*held.last_used.lock().expect("shared poisoned")) < idle
        });
    }

    /// How many things are held.
    pub fn len(&self) -> usize {
        self.things.lock().expect("shared poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sweep on an interval for as long as the process runs.
    pub fn sweep_every(self: &Arc<Self>, every: Duration) {
        let shared = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                let Some(shared) = shared.upgrade() else { return };
                shared.sweep(Instant::now());
            }
        });
    }
}

impl Held {
    fn touch(&self) {
        *self.last_used.lock().expect("shared poisoned") = Instant::now();
    }

    fn new(fingerprint: u64, limit: Option<usize>) -> Self {
        Self {
            fingerprint,
            value: tokio::sync::OnceCell::new(),
            last_used: Mutex::new(Instant::now()),
            limit,
            room: limit.map(|runs| Arc::new(tokio::sync::Semaphore::new(runs))),
        }
    }
}

/// The same values give the same fingerprint, in this process.
fn fingerprint_of(values: &std::collections::BTreeMap<String, String>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    values.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn a_thing_is_built_once_for_every_run_that_asks_and_again_on_a_new_fingerprint() {
        let shared = Shared::new(Duration::from_secs(60));
        let builds = Arc::new(AtomicUsize::new(0));
        let build = |n: usize| {
            let builds = builds.clone();
            move || async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, crate::error::WeftError>(n)
            }
        };
        let (a, b) = tokio::join!(shared.get_or_build::<usize, _, _>("db", 1, None, build(1)), shared.get_or_build::<usize, _, _>("db", 1, None, build(2)));
        assert_eq!((*a.unwrap(), *b.unwrap()), (1, 1), "one build for both");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(*shared.get_or_build::<usize, _, _>("db", 2, None, build(3)).await.unwrap(), 3, "another fingerprint builds afresh");
        assert_eq!(shared.len(), 1, "and lets the old one go");
        // Another type in the same slot is another thing.
        assert_eq!(*shared.get_or_build::<String, _, _>("db", 2, None, || async { Ok("s".to_string()) }).await.unwrap(), "s");
        assert_eq!(shared.len(), 2);
    }

    #[tokio::test]
    async fn a_failed_build_holds_nothing_and_an_idle_thing_goes() {
        let shared = Shared::new(Duration::from_secs(60));
        let failed = shared
            .get_or_build::<usize, _, _>("db", 1, None, || async { Err(crate::error::WeftError::NodeExecution("no".into())) })
            .await;
        assert!(failed.is_err());
        assert_eq!(*shared.get_or_build::<usize, _, _>("db", 1, None, || async { Ok(7) }).await.unwrap(), 7, "the next ask builds");
        shared.sweep(Instant::now() + Duration::from_secs(30));
        assert_eq!(shared.len(), 1, "inside the window");
        shared.sweep(Instant::now() + Duration::from_secs(61));
        assert!(shared.is_empty(), "past it");
    }

    #[tokio::test]
    async fn a_thing_a_run_holds_is_never_swept_and_its_window_starts_when_let_go() {
        let shared = Shared::new(Duration::from_secs(60));
        let builds = Arc::new(AtomicUsize::new(0));
        let ask = || {
            let builds = builds.clone();
            shared.get_or_build::<usize, _, _>("pool", 0, Some(1), move || async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            })
        };
        let held = ask().await.unwrap();
        shared.sweep(Instant::now() + Duration::from_secs(600));
        assert_eq!(shared.len(), 1, "held past the window, still kept");
        drop(held);
        shared.sweep(Instant::now() + Duration::from_secs(30));
        assert_eq!(shared.len(), 1, "the window starts at the release");
        let _again = ask().await.unwrap();
        assert_eq!(builds.load(Ordering::SeqCst), 1, "never a second one beside the first");
        drop(_again);
        shared.sweep(Instant::now() + Duration::from_secs(61));
        assert!(shared.is_empty());
    }

    #[tokio::test]
    async fn past_its_limit_a_run_waits_for_one_to_finish_with_it() {
        let shared = Shared::new(Duration::from_secs(60));
        let ask = || shared.get_or_build::<usize, _, _>("pool", 0, Some(2), || async { Ok(1) });
        let (a, b) = (ask().await.unwrap(), ask().await.unwrap());
        let third = tokio::time::timeout(Duration::from_millis(50), ask()).await;
        assert!(third.is_err(), "two places, both taken");
        drop(a);
        let c = tokio::time::timeout(Duration::from_secs(5), ask()).await.expect("a place came free").unwrap();
        assert_eq!((*b, *c), (1, 1));
        let free = shared.get_or_build::<String, _, _>("other", 0, None, || async { Ok("x".to_string()) });
        assert_eq!(*free.await.unwrap(), "x", "no limit, no wait");
    }

    /// A limit no run could use, and a limit other than the one the thing
    /// is held with, are refused rather than hanging or being ignored.
    #[tokio::test]
    async fn a_limit_of_none_or_another_than_held_is_refused() {
        let shared = Shared::new(Duration::from_secs(60));
        assert!(shared.get_or_build::<usize, _, _>("pool", 0, Some(0), || async { Ok(1) }).await.is_err());
        let _held = shared.get_or_build::<usize, _, _>("pool", 0, Some(2), || async { Ok(1) }).await.unwrap();
        let other = shared.get_or_build::<usize, _, _>("pool", 0, Some(5), || async { Ok(1) }).await;
        assert!(matches!(other, Err(crate::error::WeftError::Config(why)) if why.contains("same limit")));
        assert!(shared.get_or_build::<usize, _, _>("pool", 0, None, || async { Ok(1) }).await.is_err(), "none is another limit too");
    }

    #[test]
    fn the_same_values_give_the_same_fingerprint_and_a_changed_one_another() {
        let mut values = std::collections::BTreeMap::from([("password".to_string(), "a".to_string())]);
        let before = fingerprint_of(&values);
        assert_eq!(before, fingerprint_of(&values.clone()));
        values.insert("password".into(), "b".into());
        assert_ne!(before, fingerprint_of(&values));
    }
}
