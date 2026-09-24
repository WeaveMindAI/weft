//! What the nodes on an open graph are showing, pushed to the editor as
//! it changes instead of asked for on a timer.
//!
//! A node's display (an infra node's container on its `/live`, or the
//! listener kind holding a trigger's signal) has no event of its own:
//! somebody has to go and look. So this pod looks, every
//! [`LOOK_EVERY`], but only at the nodes a connected editor is showing,
//! only while one is connected, and once per node however many editors
//! show it. A look that finds what the last one found sends nothing.
//!
//! RAM on this pod is the right home for it: a feed exists only for the
//! editors connected to this pod, and dies with their connections. A
//! sibling pod serving another editor runs its own. So a press that
//! changes a display is announced on [`LOOK_NOW_CHANNEL`], and every pod
//! (the one that took the press included) looks again at once if it
//! runs that node's feed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Notify};
use weft_task_store::pg_signal::{Heard, PgSignalWatch, Subscription};

/// The channel a press announces "look at this display now" on, to every
/// pod. The payload is the [`DisplayKey`] as JSON.
pub const LOOK_NOW_CHANNEL: &str = "weft_display_look_now";

/// How often a watched node's display is looked at.
pub const LOOK_EVERY: Duration = Duration::from_secs(3);

/// Where a node's display is served from.
// SYNC: DisplaySource <-> extension-vscode/src/graphView.ts (DisplayRoute.source)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisplaySource {
    /// An infra node's own container.
    Infra,
    /// The listener kind holding a trigger's signal.
    Signal,
}

/// One node's display: a project, where it is served from, and the node
/// as a person spells its place (`one.door`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DisplayKey {
    pub project: uuid::Uuid,
    pub source: DisplaySource,
    pub node: String,
}

/// What a node is showing, as the editor draws it.
// SYNC: NodeFeed <-> packages/weft-graph/src/protocol.ts (NodeFeedState)
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum NodeFeed {
    Ok { items: Vec<weft_core::live::LiveItem> },
    /// Nothing serves it yet: the infra is not provisioned, or the
    /// signal is not registered. A resting state, distinct from an empty
    /// list and from a failure.
    Absent,
    Error { error: String },
}

/// One node's display on the wire: which node, and what it shows.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NodeDisplay {
    pub source: DisplaySource,
    pub node: String,
    #[serde(flatten)]
    pub feed: NodeFeed,
}

/// Looks at one node's display.
#[async_trait]
pub trait DisplayReader: Send + Sync + 'static {
    async fn read(&self, key: &DisplayKey) -> NodeFeed;
}

/// The feeds this pod is running, one per node some connection watches.
#[derive(Default)]
pub struct DisplayFeeds {
    feeds: Mutex<HashMap<DisplayKey, Weak<Feed>>>,
}

/// A running feed. Held by every connection watching the node; the last
/// one to let go stops it.
pub struct Feed {
    key: DisplayKey,
    shown: watch::Receiver<Option<NodeFeed>>,
    look_now: Arc<Notify>,
    looker: tokio::task::JoinHandle<()>,
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.looker.abort();
    }
}

impl Feed {
    pub fn key(&self) -> &DisplayKey {
        &self.key
    }

    /// What the node shows, starting with the latest look (`None` until
    /// the first look lands).
    pub fn shown(&self) -> watch::Receiver<Option<NodeFeed>> {
        self.shown.clone()
    }
}

impl DisplayFeeds {
    /// The feeds of a pod that hears every pod's presses on `signals`
    /// (which must listen on [`LOOK_NOW_CHANNEL`]).
    pub fn with_look_now(signals: &PgSignalWatch) -> anyhow::Result<Arc<Self>> {
        signals.require(LOOK_NOW_CHANNEL)?;
        let feeds = Arc::new(Self::default());
        crate::app::spawn_supervised("display_look_now", relay_look_now(signals.subscribe(), feeds.clone()));
        Ok(feeds)
    }

    /// Tell every pod, this one included, that `key`'s display just
    /// changed. A failed announce leaves each feed to its next tick, at
    /// most [`LOOK_EVERY`] later, so it is logged rather than failing the
    /// press that already happened.
    pub async fn announce_look_now(pool: &sqlx::PgPool, key: &DisplayKey) {
        let payload = serde_json::to_string(key).expect("a display key serializes");
        if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
            .bind(LOOK_NOW_CHANNEL)
            .bind(&payload)
            .execute(pool)
            .await
        {
            tracing::error!(target: "weft_dispatcher::display_feeds", error = %e, "pg_notify look-now failed");
        }
    }

    /// The feed for `key`: the one already running, or a new one looking
    /// through `reader` (at once, then every [`LOOK_EVERY`]).
    pub fn watch(&self, key: DisplayKey, reader: Arc<dyn DisplayReader>) -> Arc<Feed> {
        let mut feeds = self.feeds.lock().expect("the feed map is never poisoned");
        if let Some(feed) = feeds.get(&key).and_then(Weak::upgrade) {
            return feed;
        }
        let (tx, shown) = watch::channel(None);
        let look_now = Arc::new(Notify::new());
        let looker = tokio::spawn(look(key.clone(), reader, tx, look_now.clone()));
        let feed = Arc::new(Feed { key: key.clone(), shown, look_now, looker });
        feeds.retain(|_, feed| feed.strong_count() > 0);
        feeds.insert(key, Arc::downgrade(&feed));
        feed
    }

    /// Look at `key` now instead of at its next tick, when something just
    /// changed it (a button pressed on the node). A node nobody here
    /// watches is not looked at.
    pub fn look_now(&self, key: &DisplayKey) {
        let feeds = self.feeds.lock().expect("the feed map is never poisoned");
        if let Some(feed) = feeds.get(key).and_then(Weak::upgrade) {
            feed.look_now.notify_one();
        }
    }
}

/// Look again at every display a pod announced a press on. A recheck
/// (notifications possibly lost) looks at nothing: each feed's own tick
/// catches up within [`LOOK_EVERY`]. Returns only when the signal watch
/// stops, which crashes the pod through its supervisor.
async fn relay_look_now(mut heard: Subscription, feeds: Arc<DisplayFeeds>) {
    loop {
        match heard.next().await {
            Ok(Heard::Signal { channel, payload }) if channel == LOOK_NOW_CHANNEL => {
                match serde_json::from_str::<DisplayKey>(&payload) {
                    Ok(key) => feeds.look_now(&key),
                    Err(e) => tracing::warn!(
                        target: "weft_dispatcher::display_feeds",
                        error = %e,
                        "could not decode a look-now payload"
                    ),
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(target: "weft_dispatcher::display_feeds", error = %e, "cross-pod look-now stopped");
                return;
            }
        }
    }
}

async fn look(
    key: DisplayKey,
    reader: Arc<dyn DisplayReader>,
    shown: watch::Sender<Option<NodeFeed>>,
    look_now: Arc<Notify>,
) {
    loop {
        let feed = reader.read(&key).await;
        shown.send_if_modified(|current| worth_sending(current, feed));
        tokio::select! {
            _ = tokio::time::sleep(LOOK_EVERY) => {}
            _ = look_now.notified() => {}
        }
    }
}

/// Take a fresh look into `current`, and say whether it changed what the
/// node shows: a look that finds what the last one found sends nothing.
fn worth_sending(current: &mut Option<NodeFeed>, fresh: NodeFeed) -> bool {
    if current.as_ref() == Some(&fresh) {
        return false;
    }
    *current = Some(fresh);
    true
}

/// The production reader: the same reads the editor's `/live` doors
/// answer from.
pub struct StateReader(pub crate::state::DispatcherState);

#[async_trait]
impl DisplayReader for StateReader {
    async fn read(&self, key: &DisplayKey) -> NodeFeed {
        let read = match key.source {
            DisplaySource::Infra => crate::api::infra::read_live(&self.0, key.project, &key.node).await,
            DisplaySource::Signal => crate::api::signal::read_signal_live(&self.0, key.project, &key.node).await,
        };
        match read {
            Ok(feed) => NodeFeed::Ok { items: feed.items },
            Err((axum::http::StatusCode::NOT_FOUND, _)) => NodeFeed::Absent,
            Err((_, error)) => NodeFeed::Error { error },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_look_that_finds_what_the_last_one_found_sends_nothing() {
        let mut current = None;
        assert!(worth_sending(&mut current, NodeFeed::Absent), "the first look");
        assert!(!worth_sending(&mut current, NodeFeed::Absent));
        assert!(worth_sending(&mut current, NodeFeed::Ok { items: vec![] }));
        assert!(!worth_sending(&mut current, NodeFeed::Ok { items: vec![] }));
        assert!(worth_sending(&mut current, NodeFeed::Error { error: "down".into() }));
    }

    #[test]
    fn a_display_reaches_the_wire_in_the_shape_the_editor_reads() {
        let absent = NodeDisplay { source: DisplaySource::Signal, node: "one.door".into(), feed: NodeFeed::Absent };
        assert_eq!(
            serde_json::to_value(&absent).unwrap(),
            serde_json::json!({ "source": "signal", "node": "one.door", "state": "absent" })
        );
        let ok = NodeDisplay { source: DisplaySource::Infra, node: "db".into(), feed: NodeFeed::Ok { items: vec![] } };
        assert_eq!(
            serde_json::to_value(&ok).unwrap(),
            serde_json::json!({ "source": "infra", "node": "db", "state": "ok", "items": [] })
        );
        let error = NodeDisplay { source: DisplaySource::Infra, node: "db".into(), feed: NodeFeed::Error { error: "x".into() } };
        assert_eq!(serde_json::to_value(&error).unwrap()["error"], "x");
    }

    /// Counts its looks and always finds the same thing.
    #[derive(Default)]
    struct CountingReader {
        looks: AtomicUsize,
    }

    #[async_trait]
    impl DisplayReader for CountingReader {
        async fn read(&self, _key: &DisplayKey) -> NodeFeed {
            self.looks.fetch_add(1, Ordering::SeqCst);
            NodeFeed::Absent
        }
    }

    fn key(node: &str) -> DisplayKey {
        DisplayKey { project: uuid::Uuid::nil(), source: DisplaySource::Infra, node: node.into() }
    }

    #[tokio::test(start_paused = true)]
    async fn watchers_of_one_node_share_one_feed_and_the_last_to_leave_stops_it() {
        let feeds = DisplayFeeds::default();
        let reader = Arc::new(CountingReader::default());
        let a = feeds.watch(key("db"), reader.clone());
        let b = feeds.watch(key("db"), reader.clone());
        assert!(Arc::ptr_eq(&a, &b));
        tokio::time::sleep(LOOK_EVERY * 2 + Duration::from_millis(1)).await;
        assert_eq!(reader.looks.load(Ordering::SeqCst), 3, "at once, then once per tick, for both");

        drop(a);
        tokio::time::sleep(LOOK_EVERY).await;
        assert_eq!(reader.looks.load(Ordering::SeqCst), 4, "one watcher left keeps it looking");

        drop(b);
        tokio::time::sleep(LOOK_EVERY * 3).await;
        assert_eq!(reader.looks.load(Ordering::SeqCst), 4, "nobody watches: nobody looks");

        // A watcher arriving later starts a fresh feed.
        let _c = feeds.watch(key("db"), reader.clone());
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(reader.looks.load(Ordering::SeqCst), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn a_press_looks_again_at_once() {
        let feeds = DisplayFeeds::default();
        let reader = Arc::new(CountingReader::default());
        let _feed = feeds.watch(key("db"), reader.clone());
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(reader.looks.load(Ordering::SeqCst), 1);
        feeds.look_now(&key("db"));
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(reader.looks.load(Ordering::SeqCst), 2);
        feeds.look_now(&key("unwatched"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_press_announced_by_a_sibling_pod_looks_again_here() {
        let feeds = Arc::new(DisplayFeeds::default());
        let reader = Arc::new(CountingReader::default());
        let _feed = feeds.watch(key("db"), reader.clone());
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(reader.looks.load(Ordering::SeqCst), 1);

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        let relay = tokio::spawn(relay_look_now(rx.into(), feeds.clone()));
        let payload = serde_json::to_string(&key("db")).unwrap();
        tx.send(Heard::Signal { channel: LOOK_NOW_CHANNEL, payload: payload.into() }).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(reader.looks.load(Ordering::SeqCst), 2);
        relay.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_watcher_sees_only_changes() {
        struct Flip(AtomicUsize);
        #[async_trait]
        impl DisplayReader for Flip {
            async fn read(&self, _key: &DisplayKey) -> NodeFeed {
                // Absent, Absent, then Ok for good.
                if self.0.fetch_add(1, Ordering::SeqCst) < 2 { NodeFeed::Absent } else { NodeFeed::Ok { items: vec![] } }
            }
        }
        let feeds = DisplayFeeds::default();
        let feed = feeds.watch(key("db"), Arc::new(Flip(AtomicUsize::new(0))));
        let mut shown = feed.shown();
        shown.changed().await.unwrap();
        assert_eq!(*shown.borrow_and_update(), Some(NodeFeed::Absent));
        shown.changed().await.unwrap();
        assert_eq!(*shown.borrow_and_update(), Some(NodeFeed::Ok { items: vec![] }));
        tokio::time::sleep(LOOK_EVERY * 5).await;
        assert!(!shown.has_changed().unwrap(), "the same look again is not news");
    }
}
