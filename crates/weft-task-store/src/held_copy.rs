//! A copy, in this process's memory, of rows that rarely change and are
//! read on every request: a tenant's routes, the install's domains, a
//! project's worker settings.
//!
//! Reading them from the database on every request costs a round trip
//! each, and with the database in another building that is most of what a
//! request spends. So each process keeps the last read, and drops an entry
//! the moment the database says its rows changed: every write to those
//! rows sends `pg_notify(channel, key)` from a trigger in the table's
//! schema group, in the writing transaction, and the process's one
//! [`PgSignalWatch`] hears it. A sibling replica that made the change
//! tells this one through the same notification, so every replica follows
//! the rows.
//!
//! The rows stay the truth. While the listening connection is down the
//! copy keeps nothing and every read goes to the database: a change made
//! then would not be heard. Listening again drops every entry and keeps
//! again; a watch that stopped for good turns the copy off for good.

use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use weft_core::content_cache::ContentCache;

use crate::pg_signal::{Heard, PgSignalWatch, Subscription};

/// Which entries a [`Changed::Matching`] drops, by key or by what each holds.
pub type EntryFilter<K, V> = Box<dyn Fn(&K, &V) -> bool + Send>;

/// Which entries a notification on the copy's channel drops.
pub enum Changed<K, V> {
    /// The rows of this one key.
    Key(K),
    /// Every entry this picks, by its key or by what it holds: the payload
    /// names a group (a project's, a tenant's) rather than one key.
    Matching(EntryFilter<K, V>),
    /// Anything: the payload names nothing this copy can key by.
    Everything,
}

/// See the module doc.
pub struct HeldCopy<K, V> {
    entries: ContentCache<K, V>,
    /// Bumped by every drop, under this lock, so a read that started
    /// before a change never stores what it read once the change is
    /// heard: what it read may be the rows from before the change.
    generation: Mutex<u64>,
    /// `false` while nothing would tell the copy about a change (the
    /// listening connection is down, or the watch stopped for good), so it
    /// keeps nothing.
    following: AtomicBool,
    /// The task that follows the channel; it ends with the copy.
    follower: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Whether a read is worth keeping: rows nobody will ask for again (a
    /// tenant name a scanner made up) are answered and not kept.
    keep: fn(&V) -> bool,
}

impl<K, V> Drop for HeldCopy<K, V> {
    fn drop(&mut self) {
        if let Some(follower) = self.follower.lock().expect("held copy follower").take() {
            follower.abort();
        }
    }
}

impl<K, V> HeldCopy<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// A copy of at most `capacity` keys, following `channel` on `signals`
    /// from now on: `key_of` reads which key a notification's payload
    /// names, and `keep` says whether a read is worth keeping. Refused when `signals` does not listen on `channel`, which
    /// would leave the copy serving rows nothing follows.
    pub fn follow(
        signals: &PgSignalWatch,
        channel: &'static str,
        capacity: usize,
        key_of: fn(&str) -> Changed<K, V>,
        keep: fn(&V) -> bool,
    ) -> anyhow::Result<Arc<Self>> {
        signals.require(channel)?;
        Ok(Self::following(signals.subscribe(), channel, capacity, key_of, keep))
    }

    /// [`Self::follow`] on any subscription (a test sends into its other
    /// end).
    pub fn following(
        subscription: Subscription,
        channel: &'static str,
        capacity: usize,
        key_of: fn(&str) -> Changed<K, V>,
        keep: fn(&V) -> bool,
    ) -> Arc<Self> {
        let copy = Arc::new(Self {
            entries: ContentCache::new(capacity),
            generation: Mutex::new(0),
            // A copy made while the watch is down keeps nothing until it
            // listens again.
            following: AtomicBool::new(subscription.listening()),
            follower: Mutex::new(None),
            keep,
        });
        let follower = tokio::spawn(follow(Arc::downgrade(&copy), subscription, channel, key_of));
        *copy.follower.lock().expect("held copy follower") = Some(follower);
        copy
    }

    /// A copy that follows nothing and so keeps nothing: every read goes
    /// to the rows. For a process (a test, a tool) with nothing to tell it
    /// about changes.
    pub fn unfollowed(capacity: usize, keep: fn(&V) -> bool) -> Arc<Self> {
        let (_, rx) = tokio::sync::broadcast::channel(1);
        let subscription = Subscription::with_listening(rx, Arc::new(AtomicBool::new(false)));
        Self::following(subscription, "", capacity, |_| Changed::Everything, keep)
    }

    /// The rows under `key` as this process holds them, if it does.
    pub fn held(&self, key: &K) -> Option<Arc<V>> {
        if !self.following.load(Ordering::Acquire) {
            return None;
        }
        self.entries.get(key)
    }

    /// The rows under `key`: this process's copy when it has one, else
    /// what `load` reads, kept unless a change was heard while it read.
    pub async fn get_or_load<F, Fut, E>(&self, key: K, load: F) -> Result<Arc<V>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>>,
    {
        match self.held(&key) {
            Some(held) => Ok(held),
            None => self.load_fresh(key, load).await,
        }
    }

    /// The rows under `key` as `load` reads them now, kept like
    /// [`Self::get_or_load`]'s. For an answer that refuses something on
    /// what the copy says (no such route, a copy not running): a change
    /// that has committed and not been heard yet is a few milliseconds
    /// old, and the refusal is confirmed against the rows themselves.
    pub async fn load_fresh<F, Fut, E>(&self, key: K, load: F) -> Result<Arc<V>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>>,
    {
        let following = self.following.load(Ordering::Acquire);
        let before = *self.generation.lock().expect("held copy generation");
        let read = Arc::new(load().await?);
        if following && (self.keep)(&read) {
            let generation = self.generation.lock().expect("held copy generation");
            if *generation == before && self.following.load(Ordering::Acquire) {
                self.entries.put(key, read.clone());
            }
        }
        Ok(read)
    }

    fn drop_changed(&self, changed: Changed<K, V>) {
        let mut generation = self.generation.lock().expect("held copy generation");
        *generation += 1;
        match changed {
            Changed::Key(key) => self.entries.forget(&key),
            Changed::Matching(whose) => self.entries.forget_where(whose),
            Changed::Everything => self.entries.forget_all(),
        }
    }

    /// Keep nothing until [`Self::follow_again`]: changes are not heard.
    fn stop_following(&self) {
        let mut generation = self.generation.lock().expect("held copy generation");
        self.following.store(false, Ordering::Release);
        *generation += 1;
        self.entries.forget_all();
    }

    /// Changes are heard again: start from nothing and keep again.
    fn follow_again(&self) {
        let mut generation = self.generation.lock().expect("held copy generation");
        *generation += 1;
        self.entries.forget_all();
        self.following.store(true, Ordering::Release);
    }
}

async fn follow<K, V>(copy: Weak<HeldCopy<K, V>>, mut subscription: Subscription, channel: &'static str, key_of: fn(&str) -> Changed<K, V>)
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    loop {
        let heard = subscription.next().await;
        let Some(copy) = copy.upgrade() else { return };
        match heard {
            Ok(Heard::Signal { channel: heard_on, payload }) if heard_on == channel => copy.drop_changed(key_of(&payload)),
            Ok(Heard::Signal { .. }) => {}
            // A recheck can also mean this copy fell behind and missed what
            // was said, a lost connection included: whether the watch
            // listens now is what decides.
            Ok(Heard::Recheck) if subscription.listening() => copy.follow_again(),
            Ok(Heard::Recheck | Heard::Lost) => copy.stop_following(),
            Err(e) => {
                tracing::error!(
                    target: "weft_task_store::held_copy",
                    channel, error = %format!("{e:#}"),
                    "the copy of rows on this channel stopped following them; every read goes to the database from now on"
                );
                copy.stop_following();
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    const CHANNEL: &str = "rows";

    fn by_payload(payload: &str) -> Changed<String, u32> {
        match payload {
            "" => Changed::Everything,
            // `group:<prefix>`: every key starting with it, or holding 0.
            group if group.starts_with("group:") => {
                let prefix = group["group:".len()..].to_string();
                Changed::Matching(Box::new(move |key: &String, value: &u32| key.starts_with(&prefix) || *value == 0))
            }
            key => Changed::Key(key.to_string()),
        }
    }

    fn copy() -> (broadcast::Sender<Heard>, Arc<HeldCopy<String, u32>>) {
        let (tx, rx) = broadcast::channel(8);
        (tx, HeldCopy::following(Subscription::from(rx), CHANNEL, 16, by_payload, |_| true))
    }

    async fn read(copy: &HeldCopy<String, u32>, key: &str, value: u32) -> u32 {
        *copy.get_or_load(key.to_string(), || async move { Ok::<_, ()>(value) }).await.unwrap()
    }

    fn generation(copy: &HeldCopy<String, u32>) -> u64 {
        *copy.generation.lock().unwrap()
    }

    /// Send `heard` and wait until the follower acted on it.
    async fn hear(tx: &broadcast::Sender<Heard>, copy: &HeldCopy<String, u32>, heard: Heard) {
        let before = generation(copy);
        tx.send(heard).unwrap();
        while generation(copy) == before {
            tokio::task::yield_now().await;
        }
    }

    weft_core::stress_test!(
        name: a_held_key_is_answered_from_memory_until_its_rows_change,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, copy) = copy();
            assert_eq!(read(&copy, "a", 1).await, 1);
            assert_eq!(read(&copy, "a", 2).await, 1, "held");
            assert_eq!(read(&copy, "b", 7).await, 7);
            hear(&tx, &copy, Heard::Signal { channel: CHANNEL, payload: "a".into() }).await;
            assert_eq!(read(&copy, "a", 2).await, 2, "its rows changed, so it is read again");
            assert_eq!(read(&copy, "b", 8).await, 7, "another key's change leaves this one held");
            hear(&tx, &copy, Heard::Recheck).await;
            assert_eq!(read(&copy, "b", 9).await, 9, "a recheck drops everything");
        }
    );

    weft_core::stress_test!(
        name: another_channel_changes_nothing,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, copy) = copy();
            read(&copy, "a", 1).await;
            tx.send(Heard::Signal { channel: "other", payload: "a".into() }).unwrap();
            // Then one it does act on, so the first was surely heard.
            hear(&tx, &copy, Heard::Signal { channel: CHANNEL, payload: "b".into() }).await;
            assert_eq!(read(&copy, "a", 2).await, 1);
        }
    );

    // A read that started before a change is answered but not kept: it
    // may have read the rows from before the change.
    weft_core::stress_test!(
        name: a_read_overtaken_by_a_change_is_not_kept,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, copy) = copy();
            let (started_tx, started) = tokio::sync::oneshot::channel::<()>();
            let (go, wait) = tokio::sync::oneshot::channel::<()>();
            let reading = {
                let copy = copy.clone();
                tokio::spawn(async move {
                    *copy
                        .get_or_load("a".to_string(), || async move {
                            started_tx.send(()).unwrap();
                            wait.await.unwrap();
                            Ok::<_, ()>(1)
                        })
                        .await
                        .unwrap()
                })
            };
            started.await.unwrap();
            hear(&tx, &copy, Heard::Signal { channel: CHANNEL, payload: "a".into() }).await;
            go.send(()).unwrap();
            assert_eq!(reading.await.unwrap(), 1, "the read is still answered");
            assert_eq!(read(&copy, "a", 2).await, 2, "but it was not kept");
        }
    );

    // While the listening connection is down nothing says what changed,
    // so the copy keeps nothing; listening again keeps again, from
    // nothing.
    weft_core::stress_test!(
        name: a_lost_connection_keeps_nothing_until_it_listens_again,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, copy) = copy();
            read(&copy, "a", 1).await;
            hear(&tx, &copy, Heard::Lost).await;
            assert_eq!(read(&copy, "a", 2).await, 2, "nothing held");
            assert_eq!(read(&copy, "a", 3).await, 3, "and nothing kept");
            hear(&tx, &copy, Heard::Recheck).await;
            assert_eq!(read(&copy, "a", 4).await, 4);
            assert_eq!(read(&copy, "a", 5).await, 4, "kept again");
        }
    );

    // A copy that fell behind and missed the connection being lost reads
    // the watch's state on the recheck that tells it it fell behind.
    weft_core::stress_test!(
        name: a_copy_that_missed_the_loss_reads_whether_the_watch_listens,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, rx) = broadcast::channel(8);
            let listening = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let copy: Arc<HeldCopy<String, u32>> =
                HeldCopy::following(Subscription::with_listening(rx, listening.clone()), CHANNEL, 16, by_payload, |_| true);
            read(&copy, "a", 1).await;
            listening.store(false, Ordering::Release);
            hear(&tx, &copy, Heard::Recheck).await;
            assert_eq!(read(&copy, "a", 2).await, 2);
            assert_eq!(read(&copy, "a", 3).await, 3, "nothing kept while the watch is down");
            listening.store(true, Ordering::Release);
            hear(&tx, &copy, Heard::Recheck).await;
            read(&copy, "a", 4).await;
            assert_eq!(read(&copy, "a", 5).await, 4, "kept again");
        }
    );

    // A read nobody will ask for again is answered and not kept.
    weft_core::stress_test!(
        name: a_read_not_worth_keeping_is_answered_and_not_kept,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (_tx, rx) = broadcast::channel(8);
            let copy: Arc<HeldCopy<String, u32>> = HeldCopy::following(Subscription::from(rx), CHANNEL, 16, by_payload, |v| *v != 0);
            assert_eq!(read(&copy, "a", 0).await, 0);
            assert_eq!(read(&copy, "a", 1).await, 1, "the empty answer was not kept");
            assert_eq!(read(&copy, "a", 2).await, 1);
        }
    );

    // A watch that stopped can no longer say what changed, so the copy
    // keeps nothing from then on.
    weft_core::stress_test!(
        name: a_stopped_watch_turns_the_copy_off,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, copy) = copy();
            read(&copy, "a", 1).await;
            drop(tx);
            while copy.following.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            assert_eq!(read(&copy, "a", 2).await, 2);
            assert_eq!(read(&copy, "a", 3).await, 3, "nothing is kept any more");
        }
    );

    weft_core::stress_test!(
        name: a_change_naming_a_group_drops_that_group_alone,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, copy) = copy();
            read(&copy, "ann/a", 1).await;
            read(&copy, "ann/b", 2).await;
            read(&copy, "bob/a", 3).await;
            read(&copy, "eve/a", 0).await;
            hear(&tx, &copy, Heard::Signal { channel: CHANNEL, payload: "group:ann/".into() }).await;
            assert_eq!(read(&copy, "ann/a", 10).await, 10, "named by its key");
            assert_eq!(read(&copy, "ann/b", 20).await, 20);
            assert_eq!(read(&copy, "eve/a", 30).await, 30, "named by what it holds");
            assert_eq!(read(&copy, "bob/a", 40).await, 3, "another group stays held");
        }
    );
}
