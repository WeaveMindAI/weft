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

use std::hash::Hash;
use std::sync::{Arc, Mutex};

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
}

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
        Self { entries: Mutex::new(Entries { held: lru::LruCache::new(capacity), bytes: 0 }), weight: None }
    }

    /// A cache whose values weigh at most `limit` bytes together, each
    /// weighed once by `weigh` as it is put. A value heavier than `limit`
    /// on its own is never kept.
    pub fn weighed(limit: usize, weigh: fn(&V) -> usize) -> Self {
        Self { entries: Mutex::new(Entries { held: lru::LruCache::unbounded(), bytes: 0 }), weight: Some(Weight { of: weigh, limit }) }
    }

    /// The value held under `key`, if this process has it.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.entries.lock().expect("content cache").held.get(key).map(|held| held.value.clone())
    }

    /// Keep `value` as what is held under `key`, dropping the least
    /// recently used until the cache is within its bound again.
    pub fn put(&self, key: K, value: Arc<V>) {
        let bytes = self.weight.as_ref().map_or(0, |weight| (weight.of)(&value));
        let mut entries = self.entries.lock().expect("content cache");
        let Some(weight) = &self.weight else {
            entries.held.put(key, Held { value, bytes });
            return;
        };
        if bytes > weight.limit {
            if let Some(gone) = entries.held.pop(&key) {
                entries.bytes -= gone.bytes;
            }
            return;
        }
        if let Some(replaced) = entries.held.put(key, Held { value, bytes }) {
            entries.bytes -= replaced.bytes;
        }
        entries.bytes += bytes;
        while entries.bytes > weight.limit {
            let (_, gone) = entries.held.pop_lru().expect("a cache over its weight holds something");
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

impl<K: Hash + Eq + Clone, V> ContentCache<K, V> {
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
    /// bound is not kept (nor is what its key held before).
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
        assert!(cache.get(&"a").is_none(), "heavier than the bound: neither it nor what its key held is kept");
        cache.put("e", Arc::new("eeee".into()));
        assert!(cache.get(&"c").is_some() && cache.get(&"d").is_some() && cache.get(&"e").is_some(), "the dropped `a` weighs nothing any more");
        cache.forget(&"e");
        cache.put("f", Arc::new("ffff".into()));
        assert!(cache.get(&"c").is_some() && cache.get(&"d").is_some(), "what was forgotten weighs nothing any more");
    }
}
