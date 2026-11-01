//! Turning new work away while the worker's memory is nearly full.
//!
//! What one run holds depends on the program: a run that loads a large
//! file, or a node that keeps a big value in memory, can take far more
//! than another. Past its memory the platform kills the whole worker, and
//! every run on it dies with it. So a worker refuses to start more work
//! while its memory is past [`FULL_ABOVE`] of its budget, answering `503`
//! with a `Retry-After`: an outside caller is told to try again in a
//! moment, and a run weft hands it is handed again on the next delivery.
//! Runs already going are never touched. Everything else that fills up
//! behind a busy worker makes new work wait instead (the writer lanes'
//! byte budget, `crate::journal_writer`): waiting does not free memory a
//! run already holds, which is why this one guard refuses.
//!
//! The budget is settled once, when the worker starts ([`Budget`]): its
//! container's own limit (its cgroup's `memory.max`) when it runs in a
//! container that sets one. Without one, the worker has no budget and the
//! guard never refuses: a worker running as a plain process on a person's
//! machine shares that machine with everything else on it, and the other
//! apps filling the memory is no reason for it to turn its runs away. The
//! cgroup counts
//! memory the process holds whether or not it is in use, which is honest
//! because the worker's allocator hands freed memory back to the system
//! (the generated worker binary runs on jemalloc). The reading is taken by
//! a sampler every [`SAMPLE_EVERY`], never on a request's path.
//!
//! A worker past its memory is killed with no word of its own, so while
//! it is past the line the guard holds a note
//! (`weft_broker_client::protocol::MemoryNote`), kept on the worker's
//! lease row: a note on a worker that went away says it went away full.
//! Every door tick that writes the row carries the guard's note, so the
//! row holds it however late it was written (`crate::door`), and each
//! crossing also tells the broker at once ([`MemoryTold`]), so a worker
//! killed right after it crossed still left it. The worker that claims a
//! run it was driving reads the note with its claim, and fails the firing
//! it left running saying the worker ran out of memory rather than only
//! that it went away (`crate::run_driver`).

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// How full the memory may be before new work is turned away.
const FULL_ABOVE: f64 = 0.90;

/// What a refused caller is told to wait before asking again.
const RETRY_AFTER_SECS: u64 = 1;

/// How often the sampler reads the memory.
const SAMPLE_EVERY: std::time::Duration = std::time::Duration::from_millis(100);

/// A container's cgroup files, v2 then v1: its usage and its limit.
const CGROUPS: [(&str, &str); 2] = [
    ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max"),
    ("/sys/fs/cgroup/memory/memory.usage_in_bytes", "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
];

/// How much memory is in use and how much there is, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Memory {
    pub used: u64,
    pub limit: u64,
}

impl Memory {
    /// Whether new work is turned away at this reading.
    pub fn full(&self) -> bool {
        self.limit > 0 && (self.used as f64) > (self.limit as f64) * FULL_ABOVE
    }
}

/// A worker's memory budget: its container's cgroup, read from these
/// files (usage, limit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    current: &'static str,
    max: &'static str,
}

impl Budget {
    /// This worker's budget: its container's limited cgroup, or `None`
    /// when it runs in none.
    pub fn of_cgroup() -> Option<Self> {
        limited_cgroup()
    }

    /// How many bytes the budget allows; `None` when its cgroup cannot be
    /// read.
    pub fn limit(&self) -> Option<u64> {
        self.read().map(|memory| memory.limit)
    }

    /// The memory now, against this budget; `None` when it cannot be read.
    pub fn read(&self) -> Option<Memory> {
        let file = |path: &str| std::fs::read_to_string(path).ok();
        from_cgroup(file(self.current), file(self.max))
    }
}

/// The container's cgroup (v2, then v1), when it sets a limit (`max`, or
/// cgroup v1's huge "no limit" number, sets none).
fn limited_cgroup() -> Option<Budget> {
    let file = |path: &str| std::fs::read_to_string(path).ok();
    for (current, max) in CGROUPS {
        if !std::path::Path::new(max).exists() {
            continue;
        }
        let (used, limit) = (file(current), file(max));
        if used.is_none() || limit.is_none() {
            tracing::warn!(
                target: "weft_engine::busy",
                "this container's memory cgroup cannot be read, so a worker nearly out of memory does not turn new work away"
            );
            return None;
        }
        return from_cgroup(used, limit).map(|_| Budget { current, max });
    }
    None
}

/// A cgroup's reading: its usage and its limit, when it sets one.
fn from_cgroup(current: Option<String>, max: Option<String>) -> Option<Memory> {
    let used = current?.trim().parse::<u64>().ok()?;
    let limit = max?.trim().parse::<u64>().ok()?;
    // cgroup v1 says "no limit" with a number near the largest it holds.
    (limit < 1 << 60).then_some(Memory { used, limit })
}

/// The memory crossing the line, one way or the other
/// ([`MemoryGuard::sample`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Crossing {
    /// Into nearly full, at this reading.
    Full(Memory),
    /// Back to having room.
    Room,
}

/// Whether the worker's memory is nearly full, as its sampler last read
/// it ([`MemoryGuard::sample`]), and its note while it is.
#[derive(Default)]
pub struct MemoryGuard {
    full: std::sync::atomic::AtomicBool,
    /// Its note while full, `None` while it has room: what every write of
    /// its state to the broker carries.
    note: std::sync::Mutex<Option<weft_broker_client::protocol::MemoryNote>>,
    /// The last reading failed: said once per stretch of failed readings.
    unread: std::sync::atomic::AtomicBool,
    /// Held across each write of the note to the broker (a door tick, a
    /// crossing told), each reading the note once it holds it: the last
    /// write to land always carries the latest note.
    writing: tokio::sync::Mutex<()>,
}

impl MemoryGuard {
    /// Sample this worker's [`Budget`] every [`SAMPLE_EVERY`] for as long
    /// as the guard is held; with no budget, never (it is never full).
    /// `crossed` is handed each crossing of the line, either way.
    pub fn start(self: &Arc<Self>, budget: Option<Budget>, crossed: impl Fn(Crossing) + Send + 'static) {
        let Some(budget) = budget else { return };
        let held = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut every = tokio::time::interval(SAMPLE_EVERY);
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                every.tick().await;
                let Some(guard) = held.upgrade() else { return };
                if let Some(crossing) = guard.sample(budget.read(), crate::now_unix()) {
                    crossed(crossing);
                }
            }
        });
    }

    /// The turn to write the note to the broker, to hold until the write
    /// landed or failed, and the note as it is once the turn came.
    pub(crate) async fn note_to_write(&self) -> (tokio::sync::MutexGuard<'_, ()>, Option<weft_broker_client::protocol::MemoryNote>) {
        let turn = self.writing.lock().await;
        let note = *self.note.lock().expect("the memory note");
        (turn, note)
    }

    /// Take a reading at `at_unix`, saying the change once each way rather
    /// than per refused request, and keeping the note it makes. Answers
    /// the crossing, when it made one. A reading that fails crosses
    /// nothing: the guard keeps what it last read, both for the work it
    /// turns away and for the note, since a failed reading says nothing
    /// about how full the memory is.
    pub(crate) fn sample(&self, reading: Option<Memory>, at_unix: u64) -> Option<Crossing> {
        let Some(reading) = reading else {
            if !self.unread.swap(true, std::sync::atomic::Ordering::Relaxed) {
                tracing::error!(
                    target: "weft_engine::busy",
                    full = self.is_full(),
                    "the worker's memory cgroup cannot be read; the guard keeps what it last read until it can"
                );
            }
            return None;
        };
        if self.unread.swap(false, std::sync::atomic::Ordering::Relaxed) {
            tracing::info!(target: "weft_engine::busy", "the worker's memory cgroup reads again");
        }
        let full = Some(reading).filter(Memory::full);
        let was = self.full.swap(full.is_some(), std::sync::atomic::Ordering::Relaxed);
        if was != full.is_some() {
            *self.note.lock().expect("the memory note") = full.map(|memory| weft_broker_client::protocol::MemoryNote {
                used_bytes: memory.used,
                limit_bytes: memory.limit,
                at_unix,
            });
        }
        match (was, full) {
            (false, Some(memory)) => {
                tracing::warn!(
                    target: "weft_engine::busy",
                    used = memory.used, limit = memory.limit,
                    "the worker's memory is nearly full; new work is turned away until runs end"
                );
                Some(Crossing::Full(memory))
            }
            (true, None) => {
                tracing::info!(target: "weft_engine::busy", "the worker's memory has room again; new work is taken again");
                Some(Crossing::Room)
            }
            _ => None,
        }
    }

    fn is_full(&self) -> bool {
        self.full.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Tells the broker of each crossing at once, on the io runtime, apart
/// from the threads node bodies run on: the door's next tick would carry
/// it too, but a worker killed right after it crossed may not live to
/// tick. Each write takes its turn ([`MemoryGuard::note_to_write`]), so a
/// crossing told late never undoes a newer one.
pub(crate) struct MemoryTold {
    broker: Arc<dyn crate::door::DoorBroker>,
    io: tokio::runtime::Handle,
    guard: std::sync::Weak<MemoryGuard>,
}

impl MemoryTold {
    pub(crate) fn new(broker: Arc<dyn crate::door::DoorBroker>, io: tokio::runtime::Handle, guard: &Arc<MemoryGuard>) -> Self {
        Self { broker, io, guard: Arc::downgrade(guard) }
    }

    /// Tell the broker the guard's note as it is once its turn comes: the
    /// note while full, none once the memory has room.
    pub(crate) fn tell(&self) -> tokio::task::JoinHandle<()> {
        let (broker, guard) = (self.broker.clone(), self.guard.clone());
        self.io.spawn(async move {
            let Some(guard) = guard.upgrade() else { return };
            let (_turn, full) = guard.note_to_write().await;
            if let Err(e) = broker.memory(full).await {
                let left = if full.is_some() { "not kept" } else { "not taken off" };
                tracing::warn!(
                    target: "weft_engine::busy",
                    "the worker's memory note was {left} at once; the door's next tick carries it: {e:#}"
                );
            }
        })
    }
}

/// Middleware: refuse a request that would start work while the worker's
/// memory is nearly full (see the module doc). Every request a worker
/// serves but its health check starts work: a run weft hands it, or a
/// caller, which starts its run. The health check always answers.
pub async fn refuse_when_busy(State(guard): State<Arc<MemoryGuard>>, request: Request, next: Next) -> Response {
    if request.uri().path() != "/_weft/healthz" && guard.is_full() {
        return busy();
    }
    next.run(request).await
}

/// What a request turned away is answered: here for memory, at the door
/// for a worker taking all the runs it may (`crate::door::permits`).
pub(crate) fn busy() -> Response {
    let mut answer = (StatusCode::SERVICE_UNAVAILABLE, "This worker is busy right now; try again in a moment.").into_response();
    answer.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(RETRY_AFTER_SECS));
    answer
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt as _;

    /// A worker whose memory is nearly full turns new work away, and its
    /// health check still answers; once it has room, it takes work again.
    #[tokio::test]
    async fn work_is_turned_away_while_the_memory_is_nearly_full() {
        let guard = Arc::new(MemoryGuard::default());
        assert_eq!(guard.sample_now(Some(Memory { used: 950, limit: 1000 })), Some(Crossing::Full(Memory { used: 950, limit: 1000 })), "crossing into full");
        assert_eq!(guard.sample_now(Some(Memory { used: 960, limit: 1000 })), None, "still full: no second crossing");
        let app = axum::Router::new()
            .route("/_weft/fire", axum::routing::post(|| async { "started" }))
            .route("/_weft/healthz", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(guard.clone(), refuse_when_busy));
        let call = |method: &str, path: &str| {
            axum::http::Request::builder().method(method).uri(path).body(axum::body::Body::empty()).unwrap()
        };
        let refused = app.clone().oneshot(call("POST", "/_weft/fire")).await.unwrap();
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(refused.headers()[header::RETRY_AFTER], RETRY_AFTER_SECS.to_string().as_str());
        let health = app.clone().oneshot(call("GET", "/_weft/healthz")).await.unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(guard.sample_now(Some(Memory { used: 100, limit: 1000 })), Some(Crossing::Room), "back to room");
        assert_eq!(guard.sample_now(Some(Memory { used: 200, limit: 1000 })), None, "still room: no second crossing");
        assert_eq!(app.oneshot(call("POST", "/_weft/fire")).await.unwrap().status(), StatusCode::OK);
        assert!(matches!(guard.sample_now(Some(Memory { used: 950, limit: 1000 })), Some(Crossing::Full(_))), "full again: a crossing of its own");
        assert_eq!(guard.sample_now(None), None, "a reading that fails crosses nothing");
        assert!(guard.is_full(), "and the guard keeps turning work away");
        assert_eq!(guard.sample_now(Some(Memory { used: 100, limit: 1000 })), Some(Crossing::Room), "a reading again: the crossing it makes");
        assert_eq!(guard.sample_now(None), None);
        assert!(!guard.is_full(), "a failed reading keeps room too");
    }

    impl MemoryGuard {
        fn sample_now(&self, reading: Option<Memory>) -> Option<Crossing> {
            self.sample(reading, 12)
        }
    }

    /// The guard keeps its note while full, from the reading it crossed
    /// at, and none once it has room; a crossing told goes out as the note
    /// is when its turn comes, so one told late tells the newer note.
    #[tokio::test]
    async fn a_crossing_tells_the_note_as_it_is_when_its_turn_comes() {
        let broker = crate::door::fake::FakeDoorBroker::new(Vec::new());
        let guard = Arc::new(MemoryGuard::default());
        let told = MemoryTold::new(broker.clone(), tokio::runtime::Handle::current(), &guard);
        let note = weft_broker_client::protocol::MemoryNote { used_bytes: 950, limit_bytes: 1000, at_unix: 12 };
        guard.sample_now(Some(Memory { used: 950, limit: 1000 }));
        assert_eq!(guard.sample(Some(Memory { used: 990, limit: 1000 }), 13), None);
        assert_eq!(guard.note_to_write().await.1, Some(note), "the reading it crossed at");
        told.tell().await.unwrap();
        guard.sample_now(Some(Memory { used: 100, limit: 1000 }));
        told.tell().await.unwrap();
        assert_eq!(*broker.memory_states.lock().unwrap(), [Some(note), None]);

        // Full again, told, and room again before that went out: both
        // tell the room.
        guard.sample_now(Some(Memory { used: 950, limit: 1000 }));
        let late = told.tell();
        guard.sample_now(Some(Memory { used: 100, limit: 1000 }));
        let last = told.tell();
        late.await.unwrap();
        last.await.unwrap();
        assert_eq!(broker.memory_states.lock().unwrap()[2..], [None, None]);
    }

    #[test]
    fn a_cgroup_with_a_limit_is_read_and_one_without_is_not() {
        assert_eq!(from_cgroup(Some("100\n".into()), Some("1000\n".into())), Some(Memory { used: 100, limit: 1000 }));
        assert_eq!(from_cgroup(Some("100\n".into()), Some("max\n".into())), None);
        assert_eq!(from_cgroup(Some("100".into()), Some("9223372036854771712".into())), None, "cgroup v1's no limit");
        assert_eq!(from_cgroup(None, Some("1000".into())), None);
    }

    #[test]
    fn work_is_turned_away_only_past_the_line() {
        assert!(!Memory { used: 900, limit: 1000 }.full());
        assert!(Memory { used: 901, limit: 1000 }.full());
        assert!(!Memory { used: 5, limit: 0 }.full(), "no limit read refuses nothing");
    }
}
