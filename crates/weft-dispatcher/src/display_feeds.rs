//! What the nodes on an open graph are showing, pushed to the editor as
//! it changes instead of asked for on a timer.
//!
//! A node's display (an infra node's container on its `/live`, or the
//! listener kind holding a trigger's signal) has no event of its own:
//! somebody has to go and look. So this process looks, every
//! [`LOOK_EVERY`], but only at the nodes a connected editor is showing,
//! only while one is connected, and once per node however many editors
//! show it. A look that finds what the last one found sends nothing.
//!
//! Where to look (a container's address, the token the listener holds a
//! trigger by) is read from the database once per feed and kept: a look
//! asks only the container or the listener. What could move it is
//! announced (an infra copy's status or address on
//! `crate::held::INFRA_STATUS_CHANNEL`, a trigger's signal or activation on
//! `crate::holders::HELD_SIGNALS_CHANNEL` and `crate::held::ROUTES_CHANNEL`),
//! and each drops what the feeds it concerns kept, as does a watch that
//! may have missed something (`Heard::Recheck`, `Heard::Resumed`).
//!
//! RAM on this process is the right home for it: a feed exists only for the
//! editors connected to this process, and dies with their connections. A
//! sibling process serving another editor runs its own. So a press that
//! changes a display is announced on [`LOOK_NOW_CHANNEL`], and every process
//! (the one that took the press included) looks again at once if it
//! runs that node's feed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Notify};
use weft_task_store::pg_signal::{Heard, PgSignalWatch, Subscription};

/// The channel a press announces "look at this display now" on, to every
/// process. The payload is the [`DisplayKey`] as JSON.
pub const LOOK_NOW_CHANNEL: &str = "weft_display_look_now";

/// How often a watched node's display is looked at.
pub const LOOK_EVERY: Duration = Duration::from_secs(3);

/// Where a node's display is served from.
// SYNC: DisplaySource <-> extension-vscode/src/displayRoutes.ts (DisplayRoute.source)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisplaySource {
    /// An infra node's own container.
    Infra,
    /// The listener kind holding a trigger's signal.
    Signal,
    /// The listener kind holding a node's wait in one run
    /// ([`run_waits`]): a node waiting on an outside job shows what the
    /// job last answered.
    Wait,
}

/// One node's display: a project, where it is served from, and the node
/// as a person spells its place (`one.door`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DisplayKey {
    pub project: uuid::Uuid,
    pub source: DisplaySource,
    pub node: String,
    /// Whose copy of a per-instance infra node: absent for the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<weft_core::instance::InstanceId>,
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

/// Where a node's display is asked for, as [`DisplayReader::find`] read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisplayAt {
    /// Nothing serves it yet (no copy running, no signal registered).
    Nowhere,
    /// An infra node's endpoint named by its `live_endpoint`, at the
    /// address it answers at now (a container's port, a process's, or a
    /// path inside another node's endpoint).
    InfraEndpoint { endpoint_url: String },
    /// The listener, holding the trigger's signal.
    Listener(crate::api::signal::SignalLiveAt),
}

/// Looks at one node's display.
#[async_trait]
pub trait DisplayReader: Send + Sync + 'static {
    /// Where `key`'s display is asked for: reads the database. `Err`
    /// when that could not be told now (shown, and asked again at the
    /// next look).
    async fn find(&self, key: &DisplayKey) -> Result<DisplayAt, String>;
    /// What it shows at `at`: asks only the container or the listener.
    async fn read(&self, key: &DisplayKey, at: &DisplayAt) -> NodeFeed;
}

/// The feeds this process is running, one per node some connection watches.
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
    /// Set when where it looks may have moved: the next look finds it
    /// again.
    moved: Arc<AtomicBool>,
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
    /// The feeds of a process that hears every process's presses, and
    /// what moves a display, on `signals` (which must listen on
    /// [`HEARD_ON`]).
    pub fn with_look_now(signals: &PgSignalWatch) -> anyhow::Result<Arc<Self>> {
        for channel in HEARD_ON {
            signals.require(channel)?;
        }
        let feeds = Arc::new(Self::default());
        crate::app::spawn_supervised("display_look_now", relay_look_now(signals.subscribe(), feeds.clone()));
        Ok(feeds)
    }

    /// Tell every process, this one included, that `key`'s display just
    /// changed. A failed announce leaves each feed to its next tick, at
    /// most [`LOOK_EVERY`] later, so it is logged rather than failing the
    /// press that already happened.
    pub async fn announce_look_now(pool: &weft_task_store::db::Pool, key: &DisplayKey) {
        use weft_task_store::db::Client as _;
        let payload = serde_json::to_string(key).expect("a display key serializes");
        if let Err(e) = pool.execute("SELECT pg_notify($1, $2)", &[&LOOK_NOW_CHANNEL, &payload]).await {
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
        let moved = Arc::new(AtomicBool::new(false));
        let looker = tokio::spawn(look(key.clone(), reader, tx, look_now.clone(), moved.clone()));
        let feed = Arc::new(Feed { key: key.clone(), shown, look_now, moved, looker });
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

    /// Where the feeds `moved` names look may have moved: each finds it
    /// again, and looks now.
    pub fn moved(&self, moved: impl Fn(&DisplayKey) -> bool) {
        let feeds = self.feeds.lock().expect("the feed map is never poisoned");
        for feed in feeds.values().filter_map(Weak::upgrade).filter(|feed| moved(&feed.key)) {
            feed.moved.store(true, Ordering::SeqCst);
            feed.look_now.notify_one();
        }
    }
}

/// What the feeds hear: presses, and what moves where a display is.
const HEARD_ON: [&str; 4] =
    [LOOK_NOW_CHANNEL, crate::held::INFRA_STATUS_CHANNEL, crate::holders::HELD_SIGNALS_CHANNEL, crate::held::ROUTES_CHANNEL];

/// Look again at every display a process announced a press on, and find
/// again where the displays something moved are: an infra copy of a
/// project (its id the payload), any trigger's signal or activation, or
/// anything at all when the watch may have missed it. Returns only when
/// the signal watch stops, which crashes the process through its
/// supervisor.
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
            Ok(Heard::Signal { channel, payload }) if channel == crate::held::INFRA_STATUS_CHANNEL => {
                let project = payload.parse::<uuid::Uuid>().ok();
                feeds.moved(|key| key.source == DisplaySource::Infra && project.is_none_or(|project| key.project == project));
            }
            Ok(Heard::Signal { channel, .. }) if channel == crate::holders::HELD_SIGNALS_CHANNEL || channel == crate::held::ROUTES_CHANNEL => {
                feeds.moved(|key| key.source == DisplaySource::Signal);
            }
            Ok(Heard::Signal { .. }) => {}
            Ok(Heard::Recheck | Heard::Resumed | Heard::Lost) => feeds.moved(|_| true),
            Err(e) => {
                tracing::error!(target: "weft_dispatcher::display_feeds", error = %e, "cross-instance look-now stopped");
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
    moved: Arc<AtomicBool>,
) {
    let mut found: Option<DisplayAt> = None;
    loop {
        if moved.swap(false, Ordering::SeqCst) {
            found = None;
        }
        let feed = match &found {
            Some(at) => reader.read(&key, at).await,
            None => match reader.find(&key).await {
                Ok(at) => {
                    let feed = reader.read(&key, &at).await;
                    found = Some(at);
                    feed
                }
                Err(error) => NodeFeed::Error { error },
            },
        };
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

/// One wait of a run, where [`run_waits`] asks what it shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitAt {
    /// The node waiting, spelled as its place (`one.render`).
    pub node: String,
    /// Where the listener holds it, and the address a caller reaches it
    /// at, when it has one (`crate::api::signal::SignalLiveAt`).
    pub at: crate::api::signal::SignalLiveAt,
}

/// What [`run_waits`] reads: one run's waits, and what each shows.
#[async_trait]
pub trait WaitReader: Send + Sync + 'static {
    /// The waits `run` has now, in registration order: reads the database.
    async fn waits(&self, run: weft_core::RunId) -> Result<Vec<WaitAt>, String>;
    /// What the wait at `at` shows: asks the listener.
    async fn read(&self, at: &crate::api::signal::SignalLiveAt) -> NodeFeed;
}

/// What the waits of `run` show, one message per node whenever it changes,
/// and `Absent` once a node's wait is over: looked at at once, then every
/// [`LOOK_EVERY`], for as long as the caller holds the stream (the editor
/// holds it while it follows the run). A node waiting several times at
/// once (inside a loop) shows each of its waits, each line named with
/// which one it is. A failed read of the run's waits shows on every node
/// shown so far.
///
/// One loop per stream rather than a shared [`Feed`]: a feed is one node's
/// display, under a key known up front, while a run's waits are a set
/// that grows and shrinks as its nodes wait, and the loop ends with the
/// stream that holds it.
pub fn run_waits(reader: Arc<dyn WaitReader>, run: weft_core::RunId) -> impl futures::Stream<Item = NodeDisplay> {
    struct Looking {
        reader: Arc<dyn WaitReader>,
        shown: std::collections::BTreeMap<String, NodeFeed>,
        queued: std::collections::VecDeque<NodeDisplay>,
        first: bool,
    }
    let start = Looking { reader, shown: Default::default(), queued: Default::default(), first: true };
    futures::stream::unfold(start, move |mut looking| async move {
        loop {
            if let Some(next) = looking.queued.pop_front() {
                return Some((next, looking));
            }
            if !std::mem::take(&mut looking.first) {
                tokio::time::sleep(LOOK_EVERY).await;
            }
            let now = look_at_waits(looking.reader.as_ref(), run, &looking.shown).await;
            for (node, feed) in now {
                let changed = match &feed {
                    NodeFeed::Absent => looking.shown.remove(&node).is_some(),
                    _ => looking.shown.insert(node.clone(), feed.clone()).as_ref() != Some(&feed),
                };
                if changed {
                    looking.queued.push_back(NodeDisplay { source: DisplaySource::Wait, node, feed });
                }
            }
        }
    })
}

/// One look at the waits of `run`: what each node with a wait shows now,
/// and `Absent` for each node in `shown` whose wait is over.
async fn look_at_waits(
    reader: &dyn WaitReader,
    run: weft_core::RunId,
    shown: &std::collections::BTreeMap<String, NodeFeed>,
) -> Vec<(String, NodeFeed)> {
    let waits = match reader.waits(run).await {
        Ok(waits) => waits,
        Err(error) => return shown.keys().map(|node| (node.clone(), NodeFeed::Error { error: error.clone() })).collect(),
    };
    let mut by_node: std::collections::BTreeMap<String, Vec<crate::api::signal::SignalLiveAt>> = std::collections::BTreeMap::new();
    for wait in waits {
        by_node.entry(wait.node).or_default().push(wait.at);
    }
    let mut now = Vec::new();
    for (node, waits) in &by_node {
        let mut feeds = Vec::new();
        for at in waits {
            feeds.push(reader.read(at).await);
        }
        now.push((node.clone(), several(feeds)));
    }
    now.extend(shown.keys().filter(|node| !by_node.contains_key(*node)).map(|node| (node.clone(), NodeFeed::Absent)));
    now
}

/// One node's display from what each of its waits shows: the one itself,
/// or, for several, every line of each named `(k of n)`. A wait gone
/// between the read of the waits and its own read shows nothing; one that
/// failed fails the whole.
fn several(feeds: Vec<NodeFeed>) -> NodeFeed {
    let shown: Vec<NodeFeed> = feeds.into_iter().filter(|feed| *feed != NodeFeed::Absent).collect();
    if let Some(error) = shown.iter().find(|feed| matches!(feed, NodeFeed::Error { .. })) {
        return error.clone();
    }
    match shown.len() {
        0 => NodeFeed::Absent,
        1 => shown.into_iter().next().expect("one"),
        n => NodeFeed::Ok {
            items: shown
                .into_iter()
                .enumerate()
                .flat_map(|(k, feed)| match feed {
                    NodeFeed::Ok { items } => items,
                    NodeFeed::Absent | NodeFeed::Error { .. } => Vec::new(),
                }
                .into_iter()
                .map(move |mut item| {
                    item.label = format!("{} ({} of {n})", item.label, k + 1);
                    item
                }))
                .collect(),
        },
    }
}

/// The production readers: the same reads the editor's `/live` doors
/// answer from.
pub struct StateReader(pub crate::state::DispatcherState);

#[async_trait]
impl DisplayReader for StateReader {
    async fn find(&self, key: &DisplayKey) -> Result<DisplayAt, String> {
        let (state, instance) = (&self.0, key.instance.as_ref());
        let found = match key.source {
            DisplaySource::Infra => crate::api::infra::live_endpoint_url(state, key.project, &key.node, instance)
                .await
                .map(|endpoint_url| DisplayAt::InfraEndpoint { endpoint_url }),
            DisplaySource::Signal => crate::api::signal::signal_live_at(state, key.project, &key.node, instance).await.map(DisplayAt::Listener),
            // A wait belongs to one run, and its run's stream reads it
            // (`run_waits`); a node's own feed never names one.
            DisplaySource::Wait => return Err(format!("the wait at '{}' is shown with its run, never as a feed of its own", key.node)),
        };
        match found {
            Ok(at) => Ok(at),
            Err((axum::http::StatusCode::NOT_FOUND, _)) => Ok(DisplayAt::Nowhere),
            Err((_, error)) => Err(error),
        }
    }

    async fn read(&self, key: &DisplayKey, at: &DisplayAt) -> NodeFeed {
        let read = match at {
            DisplayAt::Nowhere => return NodeFeed::Absent,
            DisplayAt::InfraEndpoint { endpoint_url } => crate::api::infra::fetch_live(&self.0, endpoint_url).await,
            DisplayAt::Listener(at) => crate::api::signal::fetch_signal_live(&self.0, &key.node, at).await,
        };
        match read {
            Ok(feed) => NodeFeed::Ok { items: feed.items },
            Err((axum::http::StatusCode::NOT_FOUND, _)) => NodeFeed::Absent,
            Err((_, error)) => NodeFeed::Error { error },
        }
    }
}

#[async_trait]
impl WaitReader for StateReader {
    async fn waits(&self, run: weft_core::RunId) -> Result<Vec<WaitAt>, String> {
        let signals = self.0.registrations.signal_list_for_run_id(run).await.map_err(|e| format!("the run's waits: {e:#}"))?;
        // The configured base, as for a trigger's display: the address is
        // handed on to whoever answers the wait. Read only once a wait has
        // an outside address.
        let base = self.0.external_base();
        let mut waits = Vec::new();
        for s in signals.into_iter().filter(|s| s.is_resume && s.run_id == Some(run)) {
            let address = match s.public_path() {
                Some(path) => Some(base.url(&path).await.map_err(|e| format!("the run's waits: {e:#}"))?),
                None => None,
            };
            waits.push(WaitAt { node: s.node_id, at: crate::api::signal::SignalLiveAt { token: s.token, address } });
        }
        Ok(waits)
    }

    async fn read(&self, at: &crate::api::signal::SignalLiveAt) -> NodeFeed {
        match self.0.listener.live(&at.token, at.address.as_deref()).await {
            Ok(Some(answer)) => NodeFeed::Ok { items: answer.live.items },
            Ok(None) => NodeFeed::Absent,
            Err(e) => NodeFeed::Error { error: format!("the wait's display: {e:#}") },
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

    /// Counts its finds (each a database read) and its looks, and always
    /// finds the same thing.
    #[derive(Default)]
    struct CountingReader {
        finds: AtomicUsize,
        looks: AtomicUsize,
    }

    #[async_trait]
    impl DisplayReader for CountingReader {
        async fn find(&self, _key: &DisplayKey) -> Result<DisplayAt, String> {
            self.finds.fetch_add(1, Ordering::SeqCst);
            Ok(DisplayAt::InfraEndpoint { endpoint_url: "http://db".into() })
        }

        async fn read(&self, _key: &DisplayKey, _at: &DisplayAt) -> NodeFeed {
            self.looks.fetch_add(1, Ordering::SeqCst);
            NodeFeed::Absent
        }
    }

    /// A feed reads where to look once, then only looks there; what moves
    /// its node's display (its infra changing, a recheck) makes it find it
    /// again, at once, and what moves another project's leaves it be.
    #[tokio::test(start_paused = true)]
    async fn a_feed_finds_its_display_once_until_something_moves_it() {
        let feeds = Arc::new(DisplayFeeds::default());
        let reader = Arc::new(CountingReader::default());
        let _feed = feeds.watch(key("db"), reader.clone());
        tokio::time::sleep(LOOK_EVERY * 3 + Duration::from_millis(1)).await;
        assert_eq!((reader.finds.load(Ordering::SeqCst), reader.looks.load(Ordering::SeqCst)), (1, 4), "no read after the first");

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        let relay = tokio::spawn(relay_look_now(rx.into(), feeds.clone()));
        let other = uuid::Uuid::from_u128(9).to_string();
        tx.send(Heard::Signal { channel: crate::held::INFRA_STATUS_CHANNEL, payload: other.into() }).unwrap();
        tx.send(Heard::Signal { channel: crate::holders::HELD_SIGNALS_CHANNEL, payload: "".into() }).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(reader.finds.load(Ordering::SeqCst), 1, "another project's infra, or a trigger, moves no infra display here");

        tx.send(Heard::Signal { channel: crate::held::INFRA_STATUS_CHANNEL, payload: uuid::Uuid::nil().to_string().into() }).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!((reader.finds.load(Ordering::SeqCst), reader.looks.load(Ordering::SeqCst)), (2, 5), "found again, and looked at at once");

        tx.send(Heard::Resumed).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(reader.finds.load(Ordering::SeqCst), 3, "a watch that may have missed something finds it again");
        relay.abort();
    }

    fn key(node: &str) -> DisplayKey {
        DisplayKey { project: uuid::Uuid::nil(), source: DisplaySource::Infra, node: node.into(), instance: None }
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
    async fn a_press_announced_by_a_sibling_instance_looks_again_here() {
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
            async fn find(&self, _key: &DisplayKey) -> Result<DisplayAt, String> {
                Ok(DisplayAt::Nowhere)
            }

            async fn read(&self, _key: &DisplayKey, _at: &DisplayAt) -> NodeFeed {
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

    /// One run's waits, as a test sets them, and what each token shows.
    struct Waits {
        waits: std::sync::Mutex<Result<Vec<WaitAt>, String>>,
        shows: std::sync::Mutex<HashMap<String, NodeFeed>>,
    }

    impl Default for Waits {
        fn default() -> Self {
            Self { waits: std::sync::Mutex::new(Ok(Vec::new())), shows: Default::default() }
        }
    }

    impl Waits {
        fn set(&self, waits: &[(&str, &str)]) {
            let at = |token: &str| crate::api::signal::SignalLiveAt { token: token.into(), address: None };
            *self.waits.lock().unwrap() = Ok(waits.iter().map(|(n, t)| WaitAt { node: n.to_string(), at: at(t) }).collect());
        }
        fn show(&self, token: &str, text: &str) {
            let items = vec![weft_core::live::LiveItem::text("Last answer", text)];
            self.shows.lock().unwrap().insert(token.into(), NodeFeed::Ok { items });
        }
    }

    #[async_trait]
    impl WaitReader for Waits {
        async fn waits(&self, _run: weft_core::RunId) -> Result<Vec<WaitAt>, String> {
            self.waits.lock().unwrap().clone()
        }
        async fn read(&self, at: &crate::api::signal::SignalLiveAt) -> NodeFeed {
            self.shows.lock().unwrap().get(&at.token).cloned().unwrap_or(NodeFeed::Absent)
        }
    }

    fn answer(display: &NodeDisplay) -> Vec<(String, String)> {
        match &display.feed {
            NodeFeed::Ok { items } => items.iter().map(|i| (i.label.clone(), i.data.as_str().unwrap().to_string())).collect(),
            other => panic!("not shown: {other:?}"),
        }
    }

    /// A run's waits show at once, then only when what they show changes;
    /// a node whose wait is over says so once, and a node waiting twice at
    /// once shows both waits, each line named with which one it is.
    #[tokio::test(start_paused = true)]
    async fn a_runs_waits_show_what_changes_and_go_when_they_end() {
        use futures::StreamExt;
        let reader = Arc::new(Waits::default());
        reader.set(&[("render", "t1")]);
        reader.show("t1", "IN_QUEUE");
        let mut shown = Box::pin(run_waits(reader.clone(), weft_core::RunId::from_u128(1)));

        let first = shown.next().await.unwrap();
        assert_eq!((first.source, first.node.as_str()), (DisplaySource::Wait, "render"));
        assert_eq!(answer(&first), vec![("Last answer".to_string(), "IN_QUEUE".to_string())]);

        reader.show("t1", "IN_PROGRESS");
        let next = tokio::time::timeout(LOOK_EVERY * 2, shown.next()).await.expect("a change is sent at the next look").unwrap();
        assert_eq!(answer(&next), vec![("Last answer".to_string(), "IN_PROGRESS".to_string())]);
        assert!(tokio::time::timeout(LOOK_EVERY * 3, shown.next()).await.is_err(), "the same look again is not news");

        reader.set(&[("each", "a"), ("each", "b")]);
        reader.show("a", "one");
        reader.show("b", "two");
        let mut seen = [
            tokio::time::timeout(LOOK_EVERY * 2, shown.next()).await.unwrap().unwrap(),
            tokio::time::timeout(LOOK_EVERY * 2, shown.next()).await.unwrap().unwrap(),
        ];
        seen.sort_by(|x, y| x.node.cmp(&y.node));
        assert_eq!(answer(&seen[0]), vec![("Last answer (1 of 2)".to_string(), "one".to_string()), ("Last answer (2 of 2)".to_string(), "two".to_string())]);
        assert_eq!((seen[1].node.as_str(), &seen[1].feed), ("render", &NodeFeed::Absent), "its wait is over");

        *reader.waits.lock().unwrap() = Err("the database is down".into());
        let failed = tokio::time::timeout(LOOK_EVERY * 2, shown.next()).await.unwrap().unwrap();
        assert_eq!((failed.node.as_str(), &failed.feed), ("each", &NodeFeed::Error { error: "the database is down".into() }));
    }
}
