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
//! let mut state = ctx.keep(()).await?;
//! let rows = ctx.inputs.get::<Generator<Row>>("rows")?;
//! while let Some(row) = rows.next(&mut state).await? { /* ... */ }
//! ```
//!
//! A pull is a place the run can pause, so it takes the firing's state
//! (`crate::context::PausePoint`). The feed counts what the firing took;
//! the count is saved with the state, and a resumed firing's feed starts
//! from it and is refilled with what was yielded and not taken.
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
//! hanging the run.

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::context::{PausePoint, Point};
use crate::error::{WeftError, WeftResult};
use crate::liveness::{wait_or_pause, FiringLocation, WaitLiveness, WaitSource};
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
    /// Items the firing took with [`Generator::drain`] and has not been
    /// handed yet: drain holds them here until the stream ends, so a save
    /// while it waits carries them (`crate::run::saved::Positions::drained`)
    /// and the feed of a body run again from it starts with them.
    drained: VecDeque<Arc<Value>>,
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
    /// How many items the consuming firing took off this stream, its
    /// takes before a pause included: what its saved state carries
    /// (`crate::run::saved::Positions::taken`).
    taken: AtomicU64,
    /// The producer's end, once it reached this feed (or the end a body
    /// run again starts with): what the firing's saved state carries
    /// (`crate::run::saved::Positions::ended`). Never an end the engine
    /// forced (a deadlock, a pause, an abandon).
    end_taken: Mutex<Option<StreamEnd>>,
    /// The feed started ended ([`Self::new`]'s `ended`): items routed to
    /// it are the ones its firing had not taken, which arrive after that
    /// end and are still taken first.
    end_seeded: bool,
    /// The firing is pausing ([`Self::pause`]): it takes no end anymore.
    paused: std::sync::atomic::AtomicBool,
    /// The pulse ids taken that the record does not hold yet
    /// ([`Self::takes_to_record`]).
    unrecorded: Mutex<Vec<Uuid>>,
}

impl GeneratorFeed {
    /// A feed for input `port` whose firing took `taken` items before
    /// (0 for a firing's first run), of which a `drain` held `drained`
    /// without handing them yet, and, when the stream's end had reached
    /// it then, starting `ended`.
    pub fn new(
        port: impl Into<String>,
        liveness: Weak<dyn WaitLiveness>,
        node: Option<FiringLocation>,
        on_taken: Box<dyn Fn(Uuid) + Send + Sync>,
        taken: u64,
        drained: Vec<Value>,
        ended: Option<StreamEnd>,
    ) -> Arc<Self> {
        let mut buf = StreamBuffer::new();
        if let Some(end) = &ended {
            buf.close(end.clone());
        }
        let drained = drained.into_iter().map(Arc::new).collect();
        Arc::new(Self {
            state: Mutex::new(FeedState { buf, gen: 0, drained }),
            notify: Notify::new(),
            gen_mirror: AtomicU64::new(0),
            liveness,
            node,
            port: port.into(),
            on_taken,
            taken: AtomicU64::new(taken),
            end_seeded: ended.is_some(),
            paused: std::sync::atomic::AtomicBool::new(false),
            end_taken: Mutex::new(ended),
            unrecorded: Mutex::new(Vec::new()),
        })
    }

    /// The input port this feed serves.
    pub fn port(&self) -> &str {
        &self.port
    }

    /// What a save of the firing carries of `feeds` (see [`StreamsSaved`]),
    /// each stream's read in one go under its lock: an item is either
    /// still in its buffer, or counted, held by a drain, and among the
    /// takes this save records, never part of each; and a stream's end is
    /// there with its close's take, or neither is.
    pub fn saved_of(feeds: &[Arc<Self>]) -> StreamsSaved {
        let mut saved = StreamsSaved::default();
        for feed in feeds {
            let st = feed.lock();
            let port = feed.port().to_string();
            saved.taken.insert(port.clone(), feed.taken.load(Ordering::Acquire));
            if !st.drained.is_empty() {
                saved.drained.insert(port.clone(), st.drained.iter().map(|value| Value::clone(value)).collect());
            }
            if let Some(end) = feed.end_taken.lock().unwrap_or_else(PoisonError::into_inner).clone() {
                saved.ended.insert(port, end);
            }
            saved.takes.append(&mut feed.unrecorded.lock().unwrap_or_else(PoisonError::into_inner));
        }
        saved
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
        if self.end_seeded {
            st.buf.reinstate(pulse_id, value);
            self.bump_and_wake(&mut st);
            return Ok(());
        }
        st.buf.push(pulse_id, value).map_err(|e| {
            WeftError::NodeFailed(format!(
                "stream '{}': {e}; engine ordering bug",
                self.port
            ))
        })?;
        self.bump_and_wake(&mut st);
        Ok(())
    }

    /// Deliver the producer's end, carried by its close `pulse_id`, which
    /// the firing takes there and then (the engine, which routes it,
    /// takes it off its wire): it goes on record with the firing's next
    /// save (or its end), and that save carries the end
    /// ([`Self::saved_of`]). Idempotent: the first end wins, and a feed
    /// that started ended takes the close it was routed again. Refused
    /// (`false`) once the firing is pausing ([`Self::pause`]): its pause
    /// already saved where it stands, so the close stays on its wire for
    /// the feed its resumed body gets. Under the state lock throughout, so
    /// a save sees the end and its close together, or neither.
    pub fn close_taken(&self, pulse_id: Uuid, end: StreamEnd) -> bool {
        let mut st = self.lock();
        if self.paused.load(Ordering::Acquire) {
            return false;
        }
        if st.buf.close(end.clone()) {
            *self.end_taken.lock().unwrap_or_else(PoisonError::into_inner) = Some(end);
            self.bump_and_wake(&mut st);
        }
        self.unrecorded.lock().unwrap_or_else(PoisonError::into_inner).push(pulse_id);
        true
    }

    /// The firing is pausing: from here on the feed takes no end
    /// ([`Self::close_taken`]). Under the state lock, so an end taken
    /// before it is in the pause's save.
    pub fn pause(&self) {
        let _st = self.lock();
        self.paused.store(true, Ordering::Release);
    }

    /// Whether the firing is pausing ([`Self::pause`]).
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// End the stream for a reason of the engine's own (a deadlock), not
    /// the producer's. Idempotent: the first end wins. Returns whether
    /// this call applied it.
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

    /// The consuming firing paused: hand back every item still buffered
    /// (its pulse ids, to go back on their wires for the feed its resumed
    /// body gets), and end this feed failed for any straggler pull. What
    /// it took is the firing's, saved with its state.
    pub fn set_aside(&self) -> Vec<Uuid> {
        let mut st = self.lock();
        let ids = st.buf.drain();
        st.buf.force_end(StreamEnd::Failed { error: "the consuming firing paused".into() });
        self.bump_and_wake(&mut st);
        ids
    }

    /// The pulse ids this firing took and its record does not hold as
    /// taken yet, emptied: the takes go on record in batches, with each
    /// save of the firing's state and at its end, so a firing run again
    /// after a crash from its last save is handed again what it took
    /// since.
    pub fn takes_to_record(&self) -> Vec<Uuid> {
        std::mem::take(&mut *self.unrecorded.lock().unwrap_or_else(PoisonError::into_inner))
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

/// What a save of a firing carries of its streams
/// ([`GeneratorFeed::saved_of`]): how many items it took off each, the
/// ones a drain holds (streams where one holds any), the ends it took,
/// and the pulse ids of its takes the record does not hold yet, which go
/// on record in the same write.
#[derive(Debug, Default)]
pub struct StreamsSaved {
    pub taken: std::collections::BTreeMap<String, u64>,
    pub drained: std::collections::BTreeMap<String, Vec<Value>>,
    pub ended: std::collections::BTreeMap<String, StreamEnd>,
    pub takes: Vec<Uuid>,
}

/// What a pull resolved with.
enum Pulled {
    /// A buffered item, taken by this pull: the pulse that carried it
    /// and its shared value.
    Fresh(Uuid, Arc<Value>),
    /// The clean end.
    End,
}

/// One evaluation of the pull condition, under the feed lock. `Some` =
/// resolved (an item, the clean end, or the failure); `None` = nothing
/// yet, keep waiting. `by_drain`: the pull is a [`Generator::drain`]'s,
/// the only one what a drain holds belongs to.
fn evaluate_pull(feed: &GeneratorFeed, by_drain: bool) -> Option<WeftResult<Pulled>> {
    let mut st = feed.lock();
    // Its pause saved what it took (`GeneratorFeed::pause`, under this
    // lock): an item taken after it would be in no save, and lost to the
    // resumed body.
    if feed.is_paused() {
        return Some(Err(WeftError::NodeFailed(format!(
            "stream '{}' was pulled after its firing paused; a body that paused hands the pause back with `?` \
             and carries on from its state when it resumes",
            feed.port
        ))));
    }
    // Items a drain holds are its own until it hands them all: another
    // pull beside it (a second handle on the port), or a body that paused
    // in a drain and came back pulling item by item, would split the list.
    if !by_drain && !st.drained.is_empty() {
        return Some(Err(WeftError::NodeFailed(format!(
            "stream '{}' was pulled item by item while a drain of it holds what it took (beside it, or after this \
             node paused in that drain): read a stream either with `drain` or item by item, and come back to the \
             same call after a pause",
            feed.port
        ))));
    }
    if let Some((id, v)) = st.buf.pop() {
        // Counted, on its way to the record, and held by a drain, under the
        // lock the item left the buffer under: a save
        // (`GeneratorFeed::saved_of`) sees it in the buffer, or taken,
        // recorded with that save and held, never part of each.
        feed.taken.fetch_add(1, Ordering::AcqRel);
        feed.unrecorded.lock().unwrap_or_else(PoisonError::into_inner).push(id);
        if by_drain {
            st.drained.push_back(v.clone());
        }
        return Some(Ok(Pulled::Fresh(id, v)));
    }
    match st.buf.end() {
        Some(StreamEnd::Finished) => Some(Ok(Pulled::End)),
        Some(StreamEnd::Failed { error }) => Some(Err(WeftError::NodeFailed(format!(
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
    /// Take the item pulse `id` carried off its wire, which resolves its
    /// producer's delivery wait (counted
    /// and on its way to the record already, as it left the buffer:
    /// `evaluate_pull`).
    fn take(&self, id: Uuid) {
        (self.feed.on_taken)(id);
    }

    /// The item a pull resolved with, taken (when it is fresh) and read;
    /// `None` at the clean end.
    fn taken_item(&self, pulled: Pulled) -> WeftResult<Option<T>> {
        let value = match pulled {
            Pulled::Fresh(id, value) => {
                // The take is reported BEFORE conversion: the item left
                // the buffer either way, and the producer's delivery wait
                // is about the taking, not about whether the consumer can
                // parse it.
                self.take(id);
                value
            }
            Pulled::End => return Ok(None),
        };
        self.read(&value).map(Some)
    }

    fn read(&self, value: &Value) -> WeftResult<T> {
        // The buffered item is the pulse's shared value; the consumer's
        // typed copy is deserialized straight off it, so the only
        // owned copy is the one the body receives.
        serde::Deserialize::deserialize(value).map_err(|e| {
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
    /// `while let Some(item) = gen.next(&mut state).await? { ... }`.
    /// When the run pauses while this waits, the firing's state is saved
    /// (nothing taken) and the pull returns the suspension the body hands
    /// back with `?`.
    #[track_caller]
    pub fn next<'a, P: PausePoint>(&'a self, at: &'a mut P) -> impl std::future::Future<Output = WeftResult<Option<T>>> + 'a {
        self.next_at(at, Point::direct("pulled a stream", std::panic::Location::caller()))
    }

    async fn next_at<P: PausePoint>(&self, at: &mut P, point: Point) -> WeftResult<Option<T>> {
        at.before(&point.on(Point::stream_key(&self.feed.port)), self.feed.node.is_some())?;
        // Park through the shared wait protocol (`liveness::wait_or_pause`),
        // same discipline as a bus wait, so the stuck-check can prove
        // a deadlock instead of guessing, and the run can pause here.
        let source = self.feed.clone() as Arc<dyn WaitSource>;
        let resolved = match wait_or_pause(&self.feed.liveness, &self.feed.node, &source, at.pausable(), || evaluate_pull(&self.feed, false)).await {
            Ok(resolved) => resolved,
            Err(crate::liveness::Paused) => return Err(at.paused().await),
        };
        self.taken_item(resolved?)
    }

    /// Take the next item WITHOUT waiting: an already-buffered item, or
    /// `Empty` when nothing is buffered and the stream is still open,
    /// or `Finished` after a clean end. A failed stream errors, exactly
    /// as `next` would. It never waits, so the run cannot pause in it,
    /// but it takes the state all the same: a direct read, held to the
    /// rules of `crate::context::Places` like the others.
    #[track_caller]
    pub fn try_next(&self, at: &impl PausePoint) -> WeftResult<TryNext<T>> {
        at.before(&Point::direct_never_waiting("pulled a stream", std::panic::Location::caller()).on(Point::stream_key(&self.feed.port)), self.feed.node.is_some())?;
        match evaluate_pull(&self.feed, false) {
            Some(res) => match self.taken_item(res?)? {
                Some(item) => Ok(TryNext::Item(item)),
                None => Ok(TryNext::Finished),
            },
            None => Ok(TryNext::Empty),
        }
    }

    /// Consume the whole stream into a list. Waits for the end; a
    /// failed stream errors WITHOUT handing back the partial list (a
    /// partial list that looks complete is the silent-truncation bug
    /// the failed end exists to prevent).
    ///
    /// It takes each item as it arrives (a producer yielding in lock
    /// step waits on each take) and holds it on the feed until the end
    /// ([`FeedState::drained`]), so the run can pause while it waits:
    /// the save carries what it holds, and the drain of the resumed body
    /// starts with it.
    #[track_caller]
    pub fn drain<'a, P: PausePoint>(&'a self, at: &'a mut P) -> impl std::future::Future<Output = WeftResult<Vec<T>>> + 'a {
        self.drain_at(at, Point::direct("drained a stream", std::panic::Location::caller()))
    }

    async fn drain_at<P: PausePoint>(&self, at: &mut P, point: Point) -> WeftResult<Vec<T>> {
        at.before(&point.on(Point::stream_key(&self.feed.port)), self.feed.node.is_some())?;
        let source = self.feed.clone() as Arc<dyn WaitSource>;
        loop {
            let pulled = match wait_or_pause(&self.feed.liveness, &self.feed.node, &source, at.pausable(), || evaluate_pull(&self.feed, true)).await {
                Ok(pulled) => pulled?,
                Err(crate::liveness::Paused) => return Err(at.paused().await),
            };
            match pulled {
                // Held already, as it left the buffer (`evaluate_pull`).
                Pulled::Fresh(id, _) => self.take(id),
                Pulled::End => {
                    let drained = std::mem::take(&mut self.feed.lock().drained);
                    return drained.iter().map(|value| self.read(value)).collect();
                }
            }
        }
    }

    /// The stream's end marker, if the producer's side already ended:
    /// `Finished` or `Failed` with the error. `None` while the stream
    /// is still open. Items buffered before the end are still takeable
    /// even when this is `Some`.
    pub fn end(&self) -> Option<StreamEnd> {
        self.feed.end()
    }
}

impl<T> crate::context::kept::sealed::Sealed for Generator<T> {}

/// A stream read through the state (`Kept::read`): the next item.
impl<T: Serialize + DeserializeOwned + Clone + Send> crate::context::StateRead for Generator<T> {
    type Item = T;

    fn read_through<'a, P: PausePoint>(
        &'a mut self,
        at: &'a mut P,
        point: Point,
    ) -> impl std::future::Future<Output = WeftResult<Option<T>>> + Send + 'a {
        self.next_at(at, point)
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
// at the firing's terminal. Process-wide (not per-run) because a
// `Deserialize` impl has no run context to reach; the uuid keys
// are globally unique so runs can never collide. Entries are
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
    use crate::context::HarnessRead;

    fn feed() -> Arc<GeneratorFeed> {
        GeneratorFeed::new("rows", crate::liveness::no_liveness(), None, Box::new(|_| {}), 0, Vec::new(), None)
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
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), Some(1));
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), Some(2));
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), None);
        // The end is sticky: further pulls keep answering finished.
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), None);
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
                0,
                Vec::new(),
                None,
            );
            let id = register_feed(&f);
            let g: Generator<i64> =
                serde_json::from_value(generator_marker(id)).expect("live handle");
            let pull = tokio::spawn(async move { g.next(&mut HarnessRead).await });
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
        let err = g.drain(&mut HarnessRead).await.unwrap_err().to_string();
        assert!(err.contains("boom"), "{err}");
    }

    #[tokio::test]
    async fn try_next_distinguishes_empty_from_finished() {
        let f = feed();
        let g = handle(&f);
        assert_eq!(g.try_next(&HarnessRead).unwrap(), TryNext::Empty);
        f.push(Uuid::new_v4(), Arc::new(json!(5))).unwrap();
        assert_eq!(g.try_next(&HarnessRead).unwrap(), TryNext::Item(5));
        f.close(StreamEnd::Finished);
        assert_eq!(g.try_next(&HarnessRead).unwrap(), TryNext::Finished);
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
        let err = g.next(&mut HarnessRead).await.unwrap_err().to_string();
        assert!(err.contains("un-taken"), "{err}");
    }

    #[tokio::test]
    async fn discard_after_a_fully_drained_clean_end_stays_clean() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.close(StreamEnd::Finished);
        let g = handle(&f);
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), Some(1));
        assert!(f.abandon().is_empty());
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), None, "nothing was lost; the clean end stands");
    }

    #[tokio::test]
    async fn abandon_keeps_a_drained_failed_end_over_the_generic_teardown_message() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.close(StreamEnd::Failed { error: "producer boom".into() });
        let g = handle(&f);
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), Some(1));
        assert!(f.abandon().is_empty());
        // The producer's real failure is the cause a straggler must
        // read; abandoning the feed must not overwrite it.
        let err = g.next(&mut HarnessRead).await.unwrap_err().to_string();
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
        let err = g.next(&mut HarnessRead).await.unwrap_err().to_string();
        assert!(err.contains("un-taken"), "{err}");
        assert!(err.contains("producer boom"), "{err}");
    }

    #[test]
    fn wrong_typed_item_errors_loudly_naming_the_port() {
        let f = feed();
        f.push(Uuid::new_v4(), Arc::new(json!("not a number"))).unwrap();
        let g = handle(&f);
        let err = g.try_next(&HarnessRead).unwrap_err().to_string();
        assert!(err.contains("rows"), "{err}");
    }

    #[test]
    fn marker_round_trips_through_the_registry() {
        let f = feed();
        let id = register_feed(&f);
        let marker = generator_marker(id);
        assert_eq!(generator_marker_id(&marker), Some(id));
        let g: Generator<i64> = serde_json::from_value(marker.clone()).unwrap();
        assert_eq!(g.try_next(&HarnessRead).unwrap(), TryNext::Empty);
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

    /// The producer's end taken by a firing goes on record with its next
    /// save, and a feed of a body run again from that save starts ended:
    /// the items it had not taken still come first.
    #[tokio::test]
    async fn an_end_taken_is_saved_and_a_feed_run_again_starts_ended() {
        let f = GeneratorFeed::new("rows", crate::liveness::no_liveness(), None, Box::new(|_| {}), 0, Vec::new(), None);
        let close = Uuid::new_v4();
        assert!(f.close_taken(close, StreamEnd::Finished));
        assert_eq!(f.end_taken.lock().unwrap().clone(), Some(StreamEnd::Finished));
        assert_eq!(f.takes_to_record(), vec![close], "the close goes on record like a take");

        let pausing = GeneratorFeed::new("rows", crate::liveness::no_liveness(), None, Box::new(|_| {}), 0, Vec::new(), None);
        pausing.pause();
        assert!(!pausing.close_taken(Uuid::new_v4(), StreamEnd::Finished), "a pausing feed leaves the close on its wire");
        assert_eq!((pausing.end_taken.lock().unwrap().clone(), pausing.takes_to_record()), (None, Vec::new()));

        let again = GeneratorFeed::new("rows", crate::liveness::no_liveness(), None, Box::new(|_| {}), 2, Vec::new(), Some(StreamEnd::Finished));
        again.push(Uuid::new_v4(), Arc::new(json!(7))).expect("an item it had not taken lands after the end it started with");
        let id = register_feed(&again);
        let g: Generator<i64> = serde_json::from_value(generator_marker(id)).expect("live handle");
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), Some(7));
        assert_eq!(g.next(&mut HarnessRead).await.unwrap(), None, "then the end");
        assert_eq!(again.end_taken.lock().unwrap().clone(), Some(StreamEnd::Finished), "its next save still carries the end");
        unregister_feed(id);
    }

    /// A drain holds what it took on the feed, where a save reads it; a
    /// feed started with what a drain held hands it to the drain first,
    /// without taking it again, and refuses a pull item by item.
    #[tokio::test]
    async fn a_drain_holds_what_it_took_where_a_save_reads_it() {
        let f = GeneratorFeed::new("rows", crate::liveness::no_liveness(), None, Box::new(|_| {}), 0, Vec::new(), None);
        let id = register_feed(&f);
        let g: Generator<i64> = serde_json::from_value(generator_marker(id)).expect("live handle");
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.push(Uuid::new_v4(), Arc::new(json!(2))).unwrap();
        let drain = tokio::spawn(async move { g.drain(&mut HarnessRead).await });
        while f.taken.load(Ordering::Relaxed) < 2 {
            tokio::task::yield_now().await;
        }
        let saved = GeneratorFeed::saved_of(std::slice::from_ref(&f));
        assert_eq!((saved.taken["rows"], saved.drained["rows"].clone(), saved.takes.len()), (2, vec![json!(1), json!(2)], 2), "held until the end, on its way to the record");
        f.close(StreamEnd::Finished);
        assert_eq!(drain.await.unwrap().unwrap(), vec![1, 2]);
        assert!(GeneratorFeed::saved_of(std::slice::from_ref(&f)).drained.is_empty(), "handed over at the end");
        unregister_feed(id);

        let again = GeneratorFeed::new("rows", crate::liveness::no_liveness(), None, Box::new(|_| {}), 2, vec![json!(1), json!(2)], None);
        again.push(Uuid::new_v4(), Arc::new(json!(3))).unwrap();
        again.close(StreamEnd::Finished);
        let id = register_feed(&again);
        let g: Generator<i64> = serde_json::from_value(generator_marker(id)).expect("live handle");
        let refused = g.next(&mut HarnessRead).await.unwrap_err().to_string();
        assert!(refused.contains("while a drain of it holds what it took"), "{refused}");
        assert_eq!(g.drain(&mut HarnessRead).await.unwrap(), vec![1, 2, 3]);
        assert_eq!(again.taken.load(Ordering::Relaxed), 3, "what it held is not taken again");
        unregister_feed(id);
    }

    /// A pull after the firing paused (a body that swallowed its pause)
    /// fails instead of taking an item no save holds.
    #[tokio::test]
    async fn a_pull_after_the_firing_paused_is_refused() {
        let f = GeneratorFeed::new("rows", crate::liveness::no_liveness(), None, Box::new(|_| {}), 0, Vec::new(), None);
        f.push(Uuid::new_v4(), Arc::new(json!(1))).unwrap();
        f.pause();
        let id = register_feed(&f);
        let g: Generator<i64> = serde_json::from_value(generator_marker(id)).expect("live handle");
        let refused = g.next(&mut HarnessRead).await.unwrap_err().to_string();
        assert!(refused.contains("pulled after its firing paused"), "{refused}");
        assert_eq!(f.taken.load(Ordering::Relaxed), 0);
        assert_eq!(f.set_aside().len(), 1, "the item goes back on its wire");
        unregister_feed(id);
    }
}
