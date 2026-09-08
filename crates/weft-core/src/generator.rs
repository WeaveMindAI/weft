//! Generator streams: the buffered-stream state and the consumer half
//! of a `Generator[T]` port.
//!
//! A `Generator[T]` port is an ordinary typed port that accepts being
//! emitted into multiple times. The producer's `pulse_downstream` on it
//! is a yield; every item travels as a normal pulse on the edge, typed
//! against the element type `T`. The consumer is dispatched by the
//! FIRST item exactly like any port dispatches; later items are routed
//! by the engine into the firing's live [`GeneratorFeed`], and the
//! node's body pulls them through the [`Generator`] handle it reads
//! from `ctx.inputs`:
//!
//! ```ignore
//! let rows = ctx.inputs.get::<Generator<Row>>("rows")?;
//! while let Some(row) = rows.next().await? { /* ... */ }
//! ```
//!
//! Stream end is the closure pulse the engine emits when the producer's
//! firing terminates (or the producer calls `ctx.close_port`): a pull
//! that takes it sees `Ok(None)` on a clean end, or the producer's
//! error on a failed one ([`StreamEnd::Failed`]). A failed end always
//! surfaces through `?`, never as a value a body could mistake for an
//! empty stream.
//!
//! Every waiting pull registers with the engine's shared wait tracker
//! (`crate::liveness`), the same one bus waits use, so a consumer
//! parked on a pull participates in deadlock detection instead of
//! hanging the execution.

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::error::{WeftError, WeftResult};
use crate::liveness::{wait_on, FiringLocation, WaitLiveness, WaitSource};
use crate::weft_type::GENERATOR_MARKER_KEY;

/// Default for how many un-taken items one generator edge may buffer
/// before a further emission fails loudly. Only reachable by a
/// producer that runs far ahead of its consumer WITHOUT waiting for
/// delivery: the error tells it to either emit with `yield_downstream`
/// (each yield waits for its pull, so the buffer never grows), let
/// the consumer catch up, or declare a higher cap for its stream
/// (`ctx.set_max_buffered_items`). An unbounded default would be a
/// silent memory leak dressed up as convenience.
pub const DEFAULT_MAX_BUFFERED_ITEMS: usize = 4096;

/// Why a stream ended (`Finished` reads as `Ok(None)` on a pull,
/// `Failed` as the producer's error through `?`). Defined in
/// `crate::primitive` because the journal's loop snapshots carry it on
/// the wire; re-exported here as the streaming vocabulary's home.
pub use crate::primitive::StreamEnd;

/// Outcome of a non-waiting take ([`Generator::try_next`]).
#[derive(Debug, Clone, PartialEq)]
pub enum TryNext<T> {
    /// An item was buffered and is now taken.
    Item(T),
    /// Nothing buffered right now; the stream is still open.
    Empty,
    /// The stream finished cleanly and holds nothing more.
    Finished,
}

/// One live stream's delivered-but-not-yet-taken items plus its end
/// marker. THE one implementation of "what did this stream deliver
/// that nobody took, and has it ended": [`GeneratorFeed`] wraps it for
/// consuming node bodies, and the engine's stream-driven loops queue
/// through it while an iteration is busy. Items are `(pulse id,
/// value)`: each stays backed by its pulse in the engine's pulse table
/// until the take absorbs it.
#[derive(Clone)]
pub struct StreamBuffer {
    queue: VecDeque<(Uuid, Arc<Value>)>,
    end: Option<StreamEnd>,
}

impl StreamBuffer {
    pub fn new() -> Self {
        Self { queue: VecDeque::new(), end: None }
    }

    /// Buffer one item. An item arriving after the end is an ordering
    /// bug in whoever feeds this buffer (a producer cannot emit past
    /// its close); it is refused loudly and NOT recorded, so a re-pass
    /// repeats the same loud error instead of silently succeeding.
    pub fn push(&mut self, pulse_id: Uuid, value: Arc<Value>) -> Result<(), String> {
        if self.end.is_some() {
            return Err("an item arrived after the stream's end".into());
        }
        self.queue.push_back((pulse_id, value));
        Ok(())
    }

    /// Deliver the stream's end. Idempotent: the first end wins (a
    /// producer's explicit `close_port` followed by the termination
    /// sweep's closure is one end, not two). Returns whether this call
    /// applied it.
    pub fn close(&mut self, end: StreamEnd) -> bool {
        if self.end.is_some() {
            return false;
        }
        self.end = Some(end);
        true
    }

    /// Buffer an item PAST a recorded end. Only for re-delivery after
    /// a journal refold: an item that originally arrived before the
    /// end refolds as Pending while the end itself is durable, so the
    /// re-route legitimately lands after the seeded end. Every
    /// end-honoring decision reads the buffer first (an end with items
    /// still queued never terminates anything), so a reinstated item
    /// is always taken before the end is. Live arrival order stays
    /// guarded by [`Self::push`].
    pub fn reinstate(&mut self, pulse_id: Uuid, value: Arc<Value>) {
        self.queue.push_back((pulse_id, value));
    }

    /// Take the oldest buffered item, if any.
    pub fn pop(&mut self) -> Option<(Uuid, Arc<Value>)> {
        self.queue.pop_front()
    }

    /// Empty the buffer, returning the discarded items' pulse ids.
    pub fn drain(&mut self) -> Vec<Uuid> {
        self.queue.drain(..).map(|(id, _)| id).collect()
    }

    /// Overwrite the end marker regardless of what it was. Only for
    /// teardown paths that must poison an already-ended stream (items
    /// were discarded, so a clean `Finished` would lie).
    pub fn force_end(&mut self, end: StreamEnd) {
        self.end = Some(end);
    }

    /// The end marker, once set. Items queued before the end are still
    /// takeable.
    pub fn end(&self) -> Option<&StreamEnd> {
        self.end.as_ref()
    }

    pub fn buffered_len(&self) -> usize {
        self.queue.len()
    }
}

impl Default for StreamBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// One consumer feed's state under its lock: the shared buffer plus
/// the wait generation.
struct FeedState {
    buf: StreamBuffer,
    /// Monotone generation of state changes (item pushed, end set),
    /// bumped under this lock; see [`WaitSource`].
    gen: u64,
}

/// The live receiving end of one generator edge, owned by the engine
/// for the lifetime of the consuming firing. The engine pushes items
/// and the end into it; the consumer's [`Generator`] handle pulls.
pub struct GeneratorFeed {
    state: Mutex<FeedState>,
    notify: Notify,
    /// Fast mirror of `FeedState::gen` for the lock-free observe path.
    gen_mirror: AtomicU64,
    /// The engine's wait tracker; pulls park through it so the
    /// stuck-check sees them. Empty outside an engine (test rigs).
    liveness: Weak<dyn WaitLiveness>,
    /// The consuming firing's identity, keying its wait liveness.
    node: Option<FiringLocation>,
    /// The consumer's input port name, for error messages.
    port: String,
    /// Fired by a take with the taken item's pulse id, so the engine
    /// absorbs the pulse (which is also what resolves a producer's
    /// delivery wait). A no-op closure outside an engine.
    on_taken: Box<dyn Fn(Uuid) + Send + Sync>,
}

impl GeneratorFeed {
    pub fn new(
        port: impl Into<String>,
        liveness: Weak<dyn WaitLiveness>,
        node: Option<FiringLocation>,
        on_taken: Box<dyn Fn(Uuid) + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(FeedState { buf: StreamBuffer::new(), gen: 0 }),
            notify: Notify::new(),
            gen_mirror: AtomicU64::new(0),
            liveness,
            node,
            port: port.into(),
            on_taken,
        })
    }

    fn lock(&self) -> MutexGuard<'_, FeedState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Bump the generation under the state lock and mirror it, wake
    /// every parked pull so it re-evaluates, and hand the engine's
    /// tracker its ground-truth "something happened" signal (the
    /// [`WaitLiveness::on_source_event`] contract: a change landing
    /// while a node is parked must suppress a stuck declaration).
    fn bump_and_wake(&self, st: &mut FeedState) {
        st.gen += 1;
        self.gen_mirror.store(st.gen, Ordering::Release);
        self.notify.notify_waiters();
        if let Some(liveness) = self.liveness.upgrade() {
            liveness.on_source_event();
        }
    }

    /// Ingest one item pulse. An item arriving after the end is an
    /// engine ordering bug (a producer cannot emit past its close); it
    /// is refused loudly, and a re-pass repeats the same error.
    pub fn push(&self, pulse_id: Uuid, value: Arc<Value>) -> WeftResult<()> {
        let mut st = self.lock();
        st.buf.push(pulse_id, value).map_err(|e| {
            WeftError::NodeExecution(format!(
                "stream '{}': {e}; engine ordering bug",
                self.port
            ))
        })?;
        self.bump_and_wake(&mut st);
        Ok(())
    }

    /// Deliver the stream's end. Idempotent: the first end wins.
    /// Returns whether this call applied it.
    pub fn close(&self, end: StreamEnd) -> bool {
        let mut st = self.lock();
        let applied = st.buf.close(end);
        if applied {
            self.bump_and_wake(&mut st);
        }
        applied
    }

    /// The consumer is gone and will never pull again: abandon the
    /// feed. Drops every still-buffered item, returning their pulse
    /// ids so the engine absorbs them, and settles the end a straggler
    /// pull (a handle smuggled into a task that outlives the firing)
    /// will read. A fully-drained end keeps its own story (a clean
    /// `Finished` stays clean, a `Failed` keeps the producer's real
    /// error); anything else is force-ended as FAILED so a straggler
    /// never reads a clean end over a truncated stream.
    pub fn abandon(&self) -> Vec<Uuid> {
        let mut st = self.lock();
        let ids = st.buf.drain();
        let fully_drained_end = ids.is_empty() && st.buf.end().is_some();
        if !fully_drained_end {
            let mut error =
                "the consuming firing ended before draining this stream".to_string();
            if !ids.is_empty() {
                error.push_str(&format!(" with {} items still un-taken", ids.len()));
            }
            if let Some(StreamEnd::Failed { error: cause }) = st.buf.end() {
                error.push_str(&format!(
                    " (the stream had already failed upstream: {cause})"
                ));
            }
            st.buf.force_end(StreamEnd::Failed { error });
        }
        self.bump_and_wake(&mut st);
        ids
    }

    /// The end marker, once the producer's side ended. Items queued
    /// before the end are still takeable.
    pub fn end(&self) -> Option<StreamEnd> {
        self.lock().buf.end().cloned()
    }
}

impl WaitSource for GeneratorFeed {
    fn gen_now(&self) -> u64 {
        self.gen_mirror.load(Ordering::Acquire)
    }
    fn settled_gen(&self) -> u64 {
        self.lock().gen
    }
    fn wake_waiters(&self) {
        self.notify.notify_waiters();
    }
    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}

/// One buffered item as a pull sees it: the pulse that carried it and
/// its shared value.
type PulledItem = Option<(Uuid, Arc<Value>)>;

/// One evaluation of the pull condition, under the feed lock. `Some` =
/// resolved (an item, the clean end, or the failure); `None` = nothing
/// yet, keep waiting.
fn evaluate_pull(feed: &GeneratorFeed) -> Option<WeftResult<PulledItem>> {
    let mut st = feed.lock();
    if let Some((id, v)) = st.buf.pop() {
        return Some(Ok(Some((id, v))));
    }
    match st.buf.end() {
        Some(StreamEnd::Finished) => Some(Ok(None)),
        Some(StreamEnd::Failed { error }) => Some(Err(WeftError::NodeExecution(format!(
            "stream '{}' failed upstream: {error}",
            feed.port
        )))),
        None => None,
    }
}

/// The typed consumer handle for a `Generator[T]` input port. Read it
/// from the input bag like any other input
/// (`ctx.inputs.get::<Generator<Row>>("rows")?`); the bag's value is a
/// live handle the engine installed for this firing, so the read only
/// works inside the consuming node's body. Reading the port twice
/// hands back two handles over the SAME stream (one shared cursor).
pub struct Generator<T> {
    feed: Arc<GeneratorFeed>,
    _t: PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for Generator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Generator")
            .field("port", &self.feed.port)
            .field("end", &self.feed.end())
            .finish()
    }
}

impl<T: DeserializeOwned> Generator<T> {
    fn convert(&self, id: Uuid, value: Arc<Value>) -> WeftResult<T> {
        // The take is reported BEFORE conversion: the item left the
        // buffer either way, and the producer's delivery wait is about
        // the taking, not about whether the consumer can parse it.
        (self.feed.on_taken)(id);
        // The buffered item is the pulse's shared value; the consumer's
        // typed copy is deserialized straight off it, so the only
        // owned copy is the one the body receives.
        serde::Deserialize::deserialize(&*value).map_err(|e| {
            WeftError::Input(format!(
                "stream '{}': item does not deserialize into {}: {e}",
                self.feed.port,
                std::any::type_name::<T>(),
            ))
        })
    }

    /// Take the next item, waiting until one arrives or the stream
    /// ends. `Ok(Some(item))` per item, `Ok(None)` once on a clean end,
    /// the producer's error if the stream failed. The natural body is
    /// `while let Some(item) = gen.next().await? { ... }`.
    pub async fn next(&self) -> WeftResult<Option<T>> {
        // Park through the shared wait protocol (`liveness::wait_on`),
        // same discipline as a bus wait, so the stuck-check can prove
        // a deadlock instead of guessing.
        let source = self.feed.clone() as Arc<dyn WaitSource>;
        let resolved = wait_on(&self.feed.liveness, &self.feed.node, &source, || {
            evaluate_pull(&self.feed)
        })
        .await;
        match resolved? {
            Some((id, v)) => Ok(Some(self.convert(id, v)?)),
            None => Ok(None),
        }
    }

    /// Take the next item WITHOUT waiting: an already-buffered item, or
    /// `Empty` when nothing is buffered and the stream is still open,
    /// or `Finished` after a clean end. A failed stream errors, exactly
    /// as `next` would.
    pub fn try_next(&self) -> WeftResult<TryNext<T>> {
        match evaluate_pull(&self.feed) {
            Some(res) => match res? {
                Some((id, v)) => Ok(TryNext::Item(self.convert(id, v)?)),
                None => Ok(TryNext::Finished),
            },
            None => Ok(TryNext::Empty),
        }
    }

    /// Consume the whole stream into a list. Waits for the end; a
    /// failed stream errors WITHOUT handing back the partial list (a
    /// partial list that looks complete is the silent-truncation bug
    /// the failed end exists to prevent).
    pub async fn drain(&self) -> WeftResult<Vec<T>> {
        let mut out = Vec::new();
        while let Some(item) = self.next().await? {
            out.push(item);
        }
        Ok(out)
    }

    /// The stream's end marker, if the producer's side already ended:
    /// `Finished` or `Failed` with the error. `None` while the stream
    /// is still open. Items buffered before the end are still takeable
    /// even when this is `Some`.
    pub fn end(&self) -> Option<StreamEnd> {
        self.feed.end()
    }
}

impl<'de, T> serde::Deserialize<'de> for Generator<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = Value::deserialize(deserializer)?;
        let id = generator_marker_id(&value).ok_or_else(|| {
            D::Error::custom(
                "not a generator handle; a Generator<T> input reads the live handle the \
                 engine installs for the consuming firing",
            )
        })?;
        let feed = lookup_feed(id).ok_or_else(|| {
            D::Error::custom(
                "this generator handle is not live; a Generator<T> input can only be read \
                 inside its consuming firing",
            )
        })?;
        Ok(Self { feed, _t: PhantomData })
    }
}

// ----- Handle marker + live-feed registry ------------------------------
//
// The input bag stores plain JSON, and `ctx.inputs.get` goes through
// serde, so the live feed cannot ride in the bag itself. The bag holds
// a marker carrying a fresh uuid; the engine registers the feed under
// that uuid for exactly the consuming firing's lifetime and removes it
// at the firing's terminal. Process-wide (not per-execution) because a
// `Deserialize` impl has no execution context to reach; the uuid keys
// are globally unique so executions can never collide. Entries are
// `Weak`: the feed's real owner is the engine's stream runtime, so a
// registration that outlives its feed (a missed unregister on some
// teardown path) self-heals into "not live" instead of pinning the
// whole buffered stream in the process forever.

fn registry() -> MutexGuard<'static, HashMap<Uuid, Weak<GeneratorFeed>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<Uuid, Weak<GeneratorFeed>>>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Build the bag marker for a registered feed.
pub fn generator_marker(id: Uuid) -> Value {
    serde_json::json!({ GENERATOR_MARKER_KEY: { "id": id.to_string() } })
}

/// The feed id behind a generator marker value, if it is one.
pub fn generator_marker_id(value: &Value) -> Option<Uuid> {
    value
        .as_object()?
        .get(GENERATOR_MARKER_KEY)?
        .as_object()?
        .get("id")?
        .as_str()?
        .parse()
        .ok()
}

/// Register a feed and mint the id to put in its marker. Takes a
/// BORROW on purpose: the registry keeps only a `Weak`, so the caller
/// must go on owning the `Arc` for as long as the marker should
/// resolve (an owning signature here once invited a caller to hand
/// over its only strong ref and dead-register the feed). The
/// registrant (the engine, or a test rig) OWNS the entry and must
/// `unregister_feed` when the consuming firing ends, on every outcome,
/// so the process-wide map never accumulates dead entries.
pub fn register_feed(feed: &Arc<GeneratorFeed>) -> Uuid {
    let id = Uuid::new_v4();
    registry().insert(id, Arc::downgrade(feed));
    id
}

/// Remove a feed registration. Idempotent.
pub fn unregister_feed(id: Uuid) {
    registry().remove(&id);
}

fn lookup_feed(id: Uuid) -> Option<Arc<GeneratorFeed>> {
    let mut reg = registry();
    match reg.get(&id).and_then(Weak::upgrade) {
        Some(feed) => Some(feed),
        None => {
            // A dead entry (its feed was dropped without unregister):
            // self-heal the map while answering "not live".
            reg.remove(&id);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn feed() -> Arc<GeneratorFeed> {
        GeneratorFeed::new("rows", crate::liveness::no_liveness(), None, Box::new(|_| {}))
    }

    /// A typed handle over `f`, built through the marker + registry
    /// path exactly like production (there is no other way to mint
    /// one). The registration self-heals via the Weak registry when a
    /// test does not unregister.
    fn handle(f: &Arc<GeneratorFeed>) -> Generator<i64> {
        let id = register_feed(f);
        serde_json::from_value(generator_marker(id)).expect("live handle")
    }

    #[tokio::test]
    async fn items_then_clean_end_read_in_order() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.push(Uuid::new_v4(), Arc::new(json!(2))).unwrap();
        f.close(StreamEnd::Finished);
        let g = handle(&f);
        assert_eq!(g.next().await.unwrap(), Some(1));
        assert_eq!(g.next().await.unwrap(), Some(2));
        assert_eq!(g.next().await.unwrap(), None);
        // The end is sticky: further pulls keep answering finished.
        assert_eq!(g.next().await.unwrap(), None);
    }

    crate::stress_test!(
        name: a_waiting_pull_wakes_on_push,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let f = GeneratorFeed::new(
                "rows",
                crate::liveness::no_liveness(),
                None,
                Box::new(|_| {}),
            );
            let id = register_feed(&f);
            let g: Generator<i64> =
                serde_json::from_value(generator_marker(id)).expect("live handle");
            let pull = tokio::spawn(async move { g.next().await });
            // Under the multi-thread stress runs the push lands before,
            // during, and after the pull's park; every ordering must
            // deliver the item.
            f.push(Uuid::new_v4(), Arc::new(json!(7))).unwrap();
            assert_eq!(pull.await.unwrap().unwrap(), Some(7));
            unregister_feed(id);
        }
    );

    #[tokio::test]
    async fn failed_end_errors_and_drain_returns_no_partial_list() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.close(StreamEnd::Failed { error: "boom".into() });
        let g = handle(&f);
        // The buffered item still comes through; the failure lands
        // where the stream actually broke.
        let err = g.drain().await.unwrap_err().to_string();
        assert!(err.contains("boom"), "{err}");
    }

    #[tokio::test]
    async fn try_next_distinguishes_empty_from_finished() {
        let f = feed();
        let g = handle(&f);
        assert_eq!(g.try_next().unwrap(), TryNext::Empty);
        f.push(Uuid::new_v4(), Arc::new(json!(5))).unwrap();
        assert_eq!(g.try_next().unwrap(), TryNext::Item(5));
        f.close(StreamEnd::Finished);
        assert_eq!(g.try_next().unwrap(), TryNext::Finished);
    }

    #[test]
    fn push_after_end_is_refused_every_time() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.close(StreamEnd::Finished);
        assert!(f.push(Uuid::new_v4(), Arc::new(json!(2))).is_err(), "no item lands after the end");
        // The refusal must repeat: a refused item is not recorded, so
        // a routing re-pass hits the same loud error instead of
        // silently succeeding.
        assert!(f.push(Uuid::new_v4(), Arc::new(json!(2))).is_err());
    }

    #[test]
    fn close_is_idempotent_first_end_wins() {
        let f = feed();
        assert!(f.close(StreamEnd::Finished));
        assert!(!f.close(StreamEnd::Failed { error: "late".into() }));
        assert_eq!(f.end(), Some(StreamEnd::Finished));
    }

    #[tokio::test]
    async fn discard_poisons_stragglers_instead_of_reading_a_clean_end() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.close(StreamEnd::Finished);
        let g = handle(&f);
        let ids = f.abandon();
        assert_eq!(ids.len(), 1);
        // A pull after the discard must NOT see the clean end the
        // producer wrote: an item was thrown away, so a clean end
        // would be a silent truncation.
        let err = g.next().await.unwrap_err().to_string();
        assert!(err.contains("un-taken"), "{err}");
    }

    #[tokio::test]
    async fn discard_after_a_fully_drained_clean_end_stays_clean() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.close(StreamEnd::Finished);
        let g = handle(&f);
        assert_eq!(g.next().await.unwrap(), Some(1));
        assert!(f.abandon().is_empty());
        assert_eq!(g.next().await.unwrap(), None, "nothing was lost; the clean end stands");
    }

    #[tokio::test]
    async fn abandon_keeps_a_drained_failed_end_over_the_generic_teardown_message() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.close(StreamEnd::Failed { error: "producer boom".into() });
        let g = handle(&f);
        assert_eq!(g.next().await.unwrap(), Some(1));
        assert!(f.abandon().is_empty());
        // The producer's real failure is the cause a straggler must
        // read; abandoning the feed must not overwrite it.
        let err = g.next().await.unwrap_err().to_string();
        assert!(err.contains("producer boom"), "{err}");
    }

    #[tokio::test]
    async fn abandon_with_items_lost_reports_both_the_loss_and_the_upstream_failure() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.close(StreamEnd::Failed { error: "producer boom".into() });
        let g = handle(&f);
        let ids = f.abandon();
        assert_eq!(ids.len(), 1);
        let err = g.next().await.unwrap_err().to_string();
        assert!(err.contains("un-taken"), "{err}");
        assert!(err.contains("producer boom"), "{err}");
    }

    #[test]
    fn wrong_typed_item_errors_loudly_naming_the_port() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!("not a number"))).unwrap();
        let g = handle(&f);
        let err = g.try_next().unwrap_err().to_string();
        assert!(err.contains("rows"), "{err}");
    }

    #[test]
    fn marker_round_trips_through_the_registry() {
        let f = feed();
        let id = register_feed(&f);
        let marker = generator_marker(id);
        assert_eq!(generator_marker_id(&marker), Some(id));
        let g: Generator<i64> = serde_json::from_value(marker.clone()).unwrap();
        assert_eq!(g.try_next().unwrap(), TryNext::Empty);
        unregister_feed(id);
        let err = serde_json::from_value::<Generator<i64>>(marker).unwrap_err();
        assert!(err.to_string().contains("not live"), "{err}");
    }

    #[test]
    fn a_dropped_feed_self_heals_out_of_the_registry() {
        let f = feed();
        let id = register_feed(&f);
        drop(f);
        let err = serde_json::from_value::<Generator<i64>>(generator_marker(id)).unwrap_err();
        assert!(err.to_string().contains("not live"), "{err}");
    }
}
