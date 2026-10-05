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

use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// See the module doc. Keyed by what names the value: a program by its
/// project and definition hash, what a definition declares by its digest,
/// a held copy's rows by whose they are.
pub struct ContentCache<K, V> {
    entries: Mutex<lru::LruCache<K, Arc<V>>>,
}

impl<K: Hash + Eq, V> ContentCache<K, V> {
    /// A cache holding at most `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        let capacity = std::num::NonZeroUsize::new(capacity).expect("a content cache holds at least one entry");
        Self { entries: Mutex::new(lru::LruCache::new(capacity)) }
    }

    /// The value held under `key`, if this process has it.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.entries.lock().expect("content cache").get(key).cloned()
    }

    /// Keep `value` as what is held under `key`.
    pub fn put(&self, key: K, value: Arc<V>) {
        self.entries.lock().expect("content cache").put(key, value);
    }

    /// Drop what is held under `key`: for a copy of rows that change, never
    /// for content under its name.
    pub fn forget(&self, key: &K) {
        self.entries.lock().expect("content cache").pop(key);
    }

    /// Drop everything held.
    pub fn forget_all(&self) {
        self.entries.lock().expect("content cache").clear();
    }
}

impl<K: Hash + Eq + Clone, V> ContentCache<K, V> {
    /// Drop every entry `whose` picks, by its key or by what it holds: for a
    /// copy of rows whose change names a group (a project's, a tenant's).
    pub fn forget_where(&self, whose: impl Fn(&K, &V) -> bool) {
        let mut entries = self.entries.lock().expect("content cache");
        let picked: Vec<K> = entries.iter().filter(|(key, value)| whose(key, value)).map(|(key, _)| key.clone()).collect();
        for key in &picked {
            entries.pop(key);
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
}
