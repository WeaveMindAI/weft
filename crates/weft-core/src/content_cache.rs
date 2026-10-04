//! A bounded in-memory cache of values derived from content that never
//! changes under its name: a program recorded under its definition hash,
//! what a definition declares under its digest. Content under a name never
//! changes, so every process keeps its own and nothing invalidates it; the
//! bound keeps the memory flat, dropping the least recently used. Content
//! deleted from the store (a removed project's unused versions) may still
//! be answered by a process that read it before, which is harmless: nothing
//! still in use names it.

use std::sync::{Arc, Mutex};

/// See the module doc. Keyed by the project and the content's own name
/// (its hash or digest).
pub struct ContentCache<V> {
    entries: Mutex<lru::LruCache<(uuid::Uuid, String), Arc<V>>>,
}

impl<V> ContentCache<V> {
    /// A cache holding at most `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        let capacity = std::num::NonZeroUsize::new(capacity).expect("a content cache holds at least one entry");
        Self { entries: Mutex::new(lru::LruCache::new(capacity)) }
    }

    /// The value `project` holds under `name`, if this process has it.
    pub fn get(&self, project: uuid::Uuid, name: &str) -> Option<Arc<V>> {
        self.entries.lock().expect("content cache").get(&(project, name.to_string())).cloned()
    }

    /// Keep `value` as what `project` holds under `name`.
    pub fn put(&self, project: uuid::Uuid, name: String, value: Arc<V>) {
        self.entries.lock().expect("content cache").put((project, name), value);
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
        cache.put(a, "h1".into(), Arc::new(1));
        cache.put(b, "h1".into(), Arc::new(2));
        assert_eq!(cache.get(a, "h1").as_deref(), Some(&1));
        assert!(cache.get(a, "h2").is_none());
        cache.put(a, "h3".into(), Arc::new(3));
        assert!(cache.get(b, "h1").is_none(), "the least recently used went");
        assert_eq!(cache.get(a, "h1").as_deref(), Some(&1));
    }
}
