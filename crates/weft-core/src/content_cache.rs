//! A bounded in-memory cache, dropping the least recently used to keep the
//! memory flat. Mostly for values derived from content that never changes
//! under its name: a program recorded under its definition hash, what a
//! definition declares under its digest. That content never changes, so
//! every process keeps its own and nothing invalidates it; content deleted
//! from the store (a removed project's unused versions) may still be
//! answered by a process that read it before, which is harmless: nothing
//! still in use names it. A copy of rows that do change
//! (`weft_task_store::held_copy`) keeps them here too, and drops an entry
//! (`forget`) the moment its rows change.
//!
//! A cache is bounded by how many entries it holds ([`ContentCache::new`])
//! when its values are about the same size, and by how many bytes they
//! weigh ([`ContentCache::weighed`]) when they are not: a program is a few
//! kilobytes or tens of megabytes, so a count of them bounds nothing.
//! A value read from JSON is weighed by that JSON ([`json_bytes`]). A value
//! heavier than the whole bound is still kept, alone, so a process that
//! keeps reading one program bigger than its bound reads it once.
//!
//! A value read from somewhere slow is read through
//! [`ContentCache::get_or_load`], which reads each key once however many
//! callers ask for it at the same moment: a burst of runs on a cold worker
//! reads its program once, not once per run.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

/// See the module doc. Keyed by what names the value: a program by its
/// project and definition hash, what a definition declares by its digest,
/// a held copy's rows by whose they are.
pub struct ContentCache<K, V> {
    entries: Mutex<Entries<K, V>>,
    /// What one value weighs, in bytes, and how many bytes all of them may
    /// weigh together; `None` for a cache bounded by its entries alone.
    weight: Option<Weight<V>>,
}

struct Entries<K, V> {
    held: lru::LruCache<K, Held<V>>,
    /// What every held value weighs together.
    bytes: usize,
    /// The loads in flight ([`ContentCache::get_or_load`]), each watched
    /// by the callers waiting on it until it lands.
    loading: HashMap<K, watch::Receiver<Landed<V>>>,
}

/// What a load in flight landed on: nothing yet, its value, or its error.
type Landed<V> = Option<Result<Arc<V>, LoadFailed>>;

/// The error a load ([`ContentCache::get_or_load`]) failed with, shared by
/// every caller that waited on it: the same error, so a caller can still
/// ask what it is (`downcast_ref`) whichever caller ran the load.
#[derive(Debug, Clone)]
pub struct LoadFailed(Arc<anyhow::Error>);

impl LoadFailed {
    /// The error the load failed with, if it is a `T` (or a `T` gave its
    /// context), as `anyhow::Error::downcast_ref` reads it.
    pub fn downcast_ref<T: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

impl std::fmt::Display for LoadFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

/// Its causes are in its words (`{:#}`), so it names no source of its own.
impl std::error::Error for LoadFailed {}

struct Held<V> {
    value: Arc<V>,
    bytes: usize,
}

struct Weight<V> {
    of: fn(&V) -> usize,
    limit: usize,
}

impl<K: Hash + Eq, V> ContentCache<K, V> {
    /// A cache holding at most `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        let capacity = std::num::NonZeroUsize::new(capacity).expect("a content cache holds at least one entry");
        Self { entries: Mutex::new(Entries::of(lru::LruCache::new(capacity))), weight: None }
    }

    /// A cache whose values weigh at most `limit` bytes together, each
    /// weighed once by `weigh` as it is put, with one exception: a value
    /// heavier than `limit` on its own is kept alone (everything else goes
    /// to make room for it), until the next put drops it as the least
    /// recently used. What it holds weighs at most the larger of `limit`
    /// and the heaviest single value put.
    pub fn weighed(limit: usize, weigh: fn(&V) -> usize) -> Self {
        Self { entries: Mutex::new(Entries::of(lru::LruCache::unbounded())), weight: Some(Weight { of: weigh, limit }) }
    }

    /// The value held under `key`, if this process has it.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.entries.lock().expect("content cache").held.get(key).map(|held| held.value.clone())
    }

    /// Keep `value` as what is held under `key`, dropping the least
    /// recently used until the cache is within its bound again, or until
    /// `value` is all it holds (see [`Self::weighed`]).
    pub fn put(&self, key: K, value: Arc<V>) {
        let bytes = self.weight.as_ref().map_or(0, |weight| (weight.of)(&value));
        let mut entries = self.entries.lock().expect("content cache");
        if let Some(replaced) = entries.held.put(key, Held { value, bytes }) {
            entries.bytes -= replaced.bytes;
        }
        entries.bytes += bytes;
        let Some(weight) = &self.weight else { return };
        // The value just put is the most recently used, so everything else
        // goes before it, and it stays even when it is all that is left.
        while entries.bytes > weight.limit && entries.held.len() > 1 {
            let (_, gone) = entries.held.pop_lru().expect("a cache holding two values has a least recently used");
            entries.bytes -= gone.bytes;
        }
    }

    /// Drop what is held under `key`: for a copy of rows that change, never
    /// for content under its name.
    pub fn forget(&self, key: &K) {
        let mut entries = self.entries.lock().expect("content cache");
        if let Some(gone) = entries.held.pop(key) {
            entries.bytes -= gone.bytes;
        }
    }

    /// Drop everything held.
    pub fn forget_all(&self) {
        let mut entries = self.entries.lock().expect("content cache");
        entries.held.clear();
        entries.bytes = 0;
    }
}

/// The length of `value`'s JSON, counted as it is written out, nothing
/// kept: what it weighs in a [`ContentCache::weighed`], and what a journal
/// row's trim measures (`crate::stream_journal`). A parsed program holds
/// about three times its JSON (`weft-dispatcher`'s `tests/resync_memory.rs`
/// measures it). Run once per value put, never on a hit.
pub fn json_bytes<T: serde::Serialize>(value: &T) -> usize {
    struct Counted(usize);
    impl std::io::Write for Counted {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counted = Counted(0);
    serde_json::to_writer(&mut counted, value).expect("a value read from its JSON writes back as JSON");
    counted.0
}

impl<K, V> Entries<K, V> {
    fn of(held: lru::LruCache<K, Held<V>>) -> Self {
        Self { held, bytes: 0, loading: HashMap::new() }
    }
}

impl<K: Hash + Eq + Clone, V> ContentCache<K, V> {
    /// The value held under `key`, else what `load` reads, kept under
    /// `key`. One load runs per key at a time: a caller asking for a key
    /// another caller is loading waits for that load and gets what it
    /// read, so a burst of callers reads the value once.
    ///
    /// A load that fails keeps nothing, and every caller that waited on it
    /// gets its error ([`LoadFailed`], the same one the caller that ran it
    /// gets): the burst that met a failure fails together, and the next
    /// caller loads again. Waiters share the error rather than
    /// each trying again in turn, because a value that cannot be read (a
    /// content the store no longer holds) would otherwise be read and
    /// refused once per waiter, one after another. A load whose caller
    /// went away before it landed (its future dropped, or a panic) hands
    /// the load to one of its waiters, which runs its own.
    ///
    /// For content under its name: a load in flight when its key is
    /// forgotten ([`Self::forget`]) still keeps what it read.
    pub async fn get_or_load<F, Fut>(&self, key: K, load: F) -> Result<Arc<V>, LoadFailed>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<V>>,
    {
        let mut load = Some(load);
        loop {
            let mut landing = match self.ask(&key) {
                Asked::Held(value) => return Ok(value),
                Asked::Lead(lands) => {
                    let load = load.take().expect("a caller loads once: it returns what its load landed on");
                    return Loading { cache: self, key, lands }.land(load).await;
                }
                Asked::Wait(landing) => landing,
            };
            let landed = match landing.wait_for(Option::is_some).await {
                Ok(landed) => landed.clone(),
                // Its caller went away before it landed: load again.
                Err(_) => continue,
            };
            return landed.expect("waited until it landed");
        }
    }

    /// What a caller of [`Self::get_or_load`] asking for `key` does now,
    /// decided under the lock: take the value held, wait on the load in
    /// flight, or lead a load of its own (registered as in flight here).
    fn ask(&self, key: &K) -> Asked<V> {
        let mut entries = self.entries.lock().expect("content cache");
        if let Some(held) = entries.held.get(key) {
            return Asked::Held(held.value.clone());
        }
        if let Some(landing) = entries.loading.get(key) {
            return Asked::Wait(landing.clone());
        }
        let (lands, landing) = watch::channel(None);
        entries.loading.insert(key.clone(), landing);
        Asked::Lead(lands)
    }

    /// Drop every entry `whose` picks, by its key or by what it holds: for a
    /// copy of rows whose change names a group (a project's, a tenant's).
    pub fn forget_where(&self, whose: impl Fn(&K, &V) -> bool) {
        let mut entries = self.entries.lock().expect("content cache");
        let picked: Vec<K> = entries.held.iter().filter(|(key, held)| whose(key, &held.value)).map(|(key, _)| key.clone()).collect();
        for key in &picked {
            if let Some(gone) = entries.held.pop(key) {
                entries.bytes -= gone.bytes;
            }
        }
    }
}

/// What [`ContentCache::ask`] answers.
enum Asked<V> {
    Held(Arc<V>),
    Wait(watch::Receiver<Landed<V>>),
    Lead(watch::Sender<Landed<V>>),
}

/// A load in flight ([`ContentCache::get_or_load`]): it leaves the
/// cache's loads in flight however its caller ends (landed, failed,
/// dropped, panicked), and only then closes, so a waiter that sees it
/// closed before it landed finds no load in flight and runs its own.
struct Loading<'a, K: Hash + Eq, V> {
    cache: &'a ContentCache<K, V>,
    key: K,
    lands: watch::Sender<Landed<V>>,
}

impl<K: Hash + Eq + Clone, V> Loading<'_, K, V> {
    /// Run `load`, keep what it read, and hand what it landed on to every
    /// caller waiting on it.
    async fn land<F, Fut>(self, load: F) -> Result<Arc<V>, LoadFailed>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<V>>,
    {
        match load().await {
            Ok(value) => {
                let value = Arc::new(value);
                // Kept before the load leaves the loads in flight: a caller
                // between the two finds it held, never a key nobody loads.
                self.cache.put(self.key.clone(), value.clone());
                self.lands.send_replace(Some(Ok(value.clone())));
                Ok(value)
            }
            Err(e) => {
                let failed = LoadFailed(Arc::new(e));
                self.lands.send_replace(Some(Err(failed.clone())));
                Err(failed)
            }
        }
    }
}

impl<K: Hash + Eq, V> Drop for Loading<'_, K, V> {
    fn drop(&mut self) {
        // A poisoned lock means a panic while it was held, which every
        // other use of the cache reports already; panicking again here,
        // during that unwind, would abort the process.
        if let Ok(mut entries) = self.cache.entries.lock() {
            entries.loading.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A value comes back under its project and name only, and the least
    /// recently used goes first once the cache is full.
    #[test]
    fn values_come_back_by_name_and_the_oldest_goes_first() {
        let cache = ContentCache::new(2);
        let (a, b) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        cache.put((a, "h1".to_string()), Arc::new(1));
        cache.put((b, "h1".to_string()), Arc::new(2));
        assert_eq!(cache.get(&(a, "h1".to_string())).as_deref(), Some(&1));
        assert!(cache.get(&(a, "h2".to_string())).is_none());
        cache.put((a, "h3".to_string()), Arc::new(3));
        assert!(cache.get(&(b, "h1".to_string())).is_none(), "the least recently used went");
        assert_eq!(cache.get(&(a, "h1".to_string())).as_deref(), Some(&1));
    }

    /// A weighed cache holds as many values as fit in its bytes, however
    /// many that is: the least recently used go until the rest fit, a value
    /// put again under its key counts once, and one heavier than the whole
    /// bound is kept alone until the next put.
    #[test]
    fn a_weighed_cache_holds_what_fits_in_its_bytes() {
        let cache: ContentCache<&str, String> = ContentCache::weighed(10, String::len);
        cache.put("a", Arc::new("aaaa".into()));
        cache.put("b", Arc::new("bbbb".into()));
        assert!(cache.get(&"a").is_some(), "8 bytes fit in 10, and `a` is now the most recently used");
        cache.put("c", Arc::new("cccc".into()));
        assert!(cache.get(&"b").is_none(), "12 bytes do not fit: the least recently used went");
        assert!(cache.get(&"a").is_some() && cache.get(&"c").is_some());
        cache.put("c", Arc::new("cc".into()));
        cache.put("d", Arc::new("dddd".into()));
        assert!(cache.get(&"a").is_some() && cache.get(&"c").is_some() && cache.get(&"d").is_some(), "4 + 2 + 4: `c` weighs what it holds now");
        cache.put("a", Arc::new("a".repeat(11)));
        assert_eq!(cache.get(&"a").as_deref().map(String::len), Some(11), "heavier than the bound: kept");
        assert!(cache.get(&"c").is_none() && cache.get(&"d").is_none(), "and alone");
        cache.put("e", Arc::new("eeee".into()));
        assert!(cache.get(&"a").is_none(), "the next put drops it as the least recently used");
        cache.put("c", Arc::new("cccc".into()));
        assert!(cache.get(&"c").is_some() && cache.get(&"e").is_some(), "the dropped `a` weighs nothing any more");
        cache.forget(&"e");
        cache.put("f", Arc::new("ffff".into()));
        cache.put("d", Arc::new("dd".into()));
        assert!(cache.get(&"c").is_some() && cache.get(&"f").is_some() && cache.get(&"d").is_some(), "what was forgotten weighs nothing any more");
    }

    // Callers asking for one key at the same moment read it once between
    // them, and every one gets the value that load read.
    crate::stress_test!(
        name: callers_at_once_load_a_key_once,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let cache: Arc<ContentCache<&str, String>> = Arc::new(ContentCache::weighed(1024, String::len));
            let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let together = Arc::new(tokio::sync::Barrier::new(8));
            let callers: Vec<_> = (0..8)
                .map(|_| {
                    let (cache, loads, together) = (cache.clone(), loads.clone(), together.clone());
                    tokio::spawn(async move {
                        together.wait().await;
                        cache
                            .get_or_load("k", || async {
                                loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                tokio::task::yield_now().await;
                                Ok("read".to_string())
                            })
                            .await
                            .unwrap()
                    })
                })
                .collect();
            let mut got = Vec::new();
            for caller in callers {
                got.push(caller.await.unwrap());
            }
            assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 1, "one load for every caller");
            assert!(got.iter().all(|value| Arc::ptr_eq(value, &got[0])), "every caller got what that load read");
        }
    );

    // A load that fails keeps nothing: every caller that met it gets its
    // error, and the next caller loads again.
    crate::stress_test!(
        name: a_failed_load_fails_its_callers_and_keeps_nothing,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let cache: Arc<ContentCache<&str, String>> = Arc::new(ContentCache::new(4));
            let together = Arc::new(tokio::sync::Barrier::new(8));
            let callers: Vec<_> = (0..8)
                .map(|_| {
                    let (cache, together) = (cache.clone(), together.clone());
                    tokio::spawn(async move {
                        together.wait().await;
                        cache
                            .get_or_load("k", || async {
                                tokio::task::yield_now().await;
                                Err(anyhow::anyhow!("the store no longer holds it"))
                            })
                            .await
                    })
                })
                .collect();
            for caller in callers {
                let e = caller.await.unwrap().unwrap_err();
                assert!(e.to_string().contains("the store no longer holds it"), "{e:#}");
            }
            assert!(cache.get(&"k").is_none());
            let value = cache.get_or_load("k", || async { Ok("read".to_string()) }).await.unwrap();
            assert_eq!(*value, "read");
        }
    );

    /// A caller that goes away before its load lands hands the key to the
    /// caller waiting on it, which loads it itself.
    #[tokio::test]
    async fn a_load_whose_caller_went_away_is_loaded_by_its_waiter() {
        let cache: Arc<ContentCache<&str, String>> = Arc::new(ContentCache::new(4));
        let leader = tokio::spawn({
            let cache = cache.clone();
            async move { cache.get_or_load("k", std::future::pending).await }
        });
        // One thread: each yield runs the spawned tasks to where they wait.
        tokio::task::yield_now().await;
        let waiter = tokio::spawn({
            let cache = cache.clone();
            async move { cache.get_or_load("k", || async { Ok("read".to_string()) }).await }
        });
        tokio::task::yield_now().await;
        leader.abort();
        assert_eq!(*waiter.await.unwrap().unwrap(), "read");
        assert!(cache.entries.lock().unwrap().loading.is_empty(), "no load left in flight");
    }
}
