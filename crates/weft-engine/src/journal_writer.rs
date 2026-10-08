//! The worker's record writer: a few writer lanes per process, shared by
//! every run on it.
//!
//! Every run hands its events to its one handle ([`DriveJournal`]) and goes
//! on at once. A run is pinned to one lane by its id, so its events are
//! written in the order it handed them. Each lane sends what its runs
//! queued as one batch (one call on the line, one statement in the
//! database, `weft_journal::frame`), answers whoever waits on rows that
//! were in it, and starts again. Whatever arrives while a batch is on its
//! way goes in the next one, so a hundred runs ending at once cost one
//! write, and the lanes write side by side.
//!
//! ```text
//!   run A ─┐                 ┌─ lane 0 ── one batch in flight ──┐
//!   run B ─┼─ hash(run) % K ─┼─ lane 1 ── one batch in flight ──┼─► broker ─► record
//!   run C ─┘                 └─ lane 2 ── one batch in flight ──┘
//! ```
//!
//! - **Gathering.** A lane nobody waits on gathers for up to
//!   [`WriterSettings::gather`] or [`BATCH_RUNS`] runs before it sends: a
//!   record transaction costs the database far more than one run in it,
//!   and a fast run's record trailing a moment longer is waited on by
//!   nobody. Somebody waiting (a durable run at a commit point, a run
//!   pausing, a broker call about a run) has it send at once.
//! - **Caps.** A batch carries at most [`BATCH_BYTES`] of events or
//!   [`BATCH_RUNS`] runs, the runs somebody waits on first.
//! - **A full queue makes runs wait.** What waits to be written is held to
//!   [`WriterSettings::queue_bytes`]; past it, a run handing events waits
//!   for room, so the worker answers as fast as the record takes rows.
//! - **Sending again is safe.** The record takes a batch sent twice once
//!   (`weft_journal::record::Fate::AlreadyApplied`), so a batch with no
//!   answer is sent again until answered, within [`RESEND_FOR`].
//! - **A failed write stops only its runs** (see [`DriveJournal`],
//!   "Poison"); the others go on.
//! - **An unrecorded run** keeps no history: it writes a note of itself
//!   only when it fails or something names it, and nothing when it ends
//!   well (`weft_journal::unrecorded`).
//!
//! The lanes run on the process's own small runtime (see
//! [`WorkerJournal::start`]), so their wake-ups never land on the threads
//! serving calls.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use weft_core::run_settings::{KeepFor, Keeping, RunSettings};
use weft_core::ExecutionId;
use weft_journal::frame::{BatchAnswer, BatchHead, RunHead};
use weft_journal::record::{Born, Fate, StoredSelection, Written};
use weft_journal::unrecorded::UnrecordedNote;
use weft_journal::{BatchError, ExecEvent, JournalClient, RecordClient};

/// How the lanes batch. Read from the worker's environment
/// ([`WriterSettings::from_env`]).
#[derive(Debug, Clone, Copy)]
pub struct WriterSettings {
    /// How many writer lanes the process has.
    pub lanes: usize,
    /// How long a lane nobody waits on gathers before it sends.
    pub gather: Duration,
    /// How many bytes of events may wait to be written (queued, or on their
    /// way) before a run handing more waits for room. Weighed with
    /// [`ExecEvent::held_bytes`].
    pub queue_bytes: usize,
}

impl Default for WriterSettings {
    /// Four lanes; a 2 ms gather; 64 MiB waiting, what Restate gives the
    /// same buffer (its `self_proposal_queue_memory_limit`).
    fn default() -> Self {
        Self {
            lanes: 4,
            gather: Duration::from_millis(2),
            queue_bytes: 64 << 20,
        }
    }
}

impl WriterSettings {
    /// `WEFT_JOURNAL_LANES`, `WEFT_JOURNAL_GATHER_MS` and
    /// `WEFT_JOURNAL_QUEUE_BYTES`, each the default when unset.
    pub fn from_env() -> anyhow::Result<Self> {
        fn read(name: &str) -> anyhow::Result<Option<u64>> {
            match std::env::var(name) {
                Ok(raw) => Ok(Some(raw.trim().parse().map_err(|e| anyhow::anyhow!("{name} is not a whole number: {e}"))?)),
                Err(_) => Ok(None),
            }
        }
        let mut settings = Self::default();
        if let Some(lanes) = read("WEFT_JOURNAL_LANES")? {
            anyhow::ensure!((1..=u64::from(u16::MAX)).contains(&lanes), "WEFT_JOURNAL_LANES must be between 1 and {}", u16::MAX);
            settings.lanes = lanes as usize;
        }
        if let Some(ms) = read("WEFT_JOURNAL_GATHER_MS")? {
            settings.gather = Duration::from_millis(ms);
        }
        if let Some(bytes) = read("WEFT_JOURNAL_QUEUE_BYTES")? {
            anyhow::ensure!(bytes > 0, "WEFT_JOURNAL_QUEUE_BYTES must be at least 1");
            settings.queue_bytes = bytes as usize;
        }
        Ok(settings)
    }
}

/// The most events one batch carries, in bytes: no frame holds up the line
/// (the door tick that renews the worker's lease travels on the same
/// socket) and no durable waiter rides a huge synced batch. A single event
/// that weighs more goes alone.
pub const BATCH_BYTES: usize = 1 << 20;

/// The most runs one batch carries.
pub const BATCH_RUNS: usize = 1000;

/// How long a batch with no answer is sent again for: at first and at most
/// between tries, and in all. The broker is the install's own service, and
/// one out of reach this long is down, so the runs of the batch stop on it
/// like on any failed write.
const RESEND_FIRST: Duration = Duration::from_millis(100);
const RESEND_LONGEST: Duration = Duration::from_secs(5);
const RESEND_FOR: Duration = Duration::from_secs(60);

/// The process's writer lanes (see the module doc). Their tasks end once
/// this is dropped and what was queued is written.
pub struct WorkerJournal {
    lanes: Vec<Arc<Lane>>,
    record: Arc<dyn RecordClient>,
    budget: Arc<Budget>,
}

/// What a run's handle is made from: which run, how it is kept, and where
/// its record stands (a run born here starts at epoch 1 and `seq` 0; one
/// claimed goes on from its claim).
#[derive(Debug, Clone)]
pub struct RunSpec {
    pub execution_id: ExecutionId,
    pub settings: RunSettings,
    /// How long it is kept once it ended, resolved against its project's
    /// default: what its row is born with.
    pub keep_for: KeepFor,
    pub epoch: i32,
    pub next_seq: i32,
    /// The credentials its caller sent, cleaned from every event it writes.
    pub redaction: weft_core::caller::Redaction,
}

impl WorkerJournal {
    /// The lanes over `record`, their tasks spawned on `io`: the process's
    /// own small runtime, apart from the threads serving calls.
    pub fn start(record: Arc<dyn RecordClient>, settings: WriterSettings, io: &tokio::runtime::Handle) -> Arc<Self> {
        let budget = Arc::new(Budget { held: AtomicUsize::new(0), max: settings.queue_bytes, freed: tokio::sync::Notify::new() });
        let lanes: Vec<Arc<Lane>> = (0..settings.lanes)
            .map(|index| {
                Arc::new(Lane {
                    index: u16::try_from(index).expect("WEFT_JOURNAL_LANES is checked under 65536"),
                    queue: Mutex::new(LaneQueue::default()),
                    runs: Mutex::new(HashMap::new()),
                    wake: tokio::sync::Notify::new(),
                    urgent: tokio::sync::Notify::new(),
                    idle: tokio::sync::Notify::new(),
                })
            })
            .collect();
        for lane in &lanes {
            io.spawn(lane_loop(lane.clone(), record.clone(), budget.clone(), settings.gather));
        }
        Arc::new(Self { lanes, record, budget })
    }

    /// The one handle a run writes its record through, every part of it
    /// (its drive, its caller's exchange, its buses).
    pub fn run(self: &Arc<Self>, spec: RunSpec) -> Arc<DriveJournal> {
        let lane = lane_of(spec.execution_id, self.lanes.len());
        let holding = match (spec.settings.recorded(), spec.next_seq) {
            (true, _) => Holding::Recorded,
            (false, 0) => Holding::Unrecorded(UnrecordedNote::default()),
            // Picked up again from its record: its row exists, and must end.
            (false, _) => Holding::Unrecorded(UnrecordedNote::after_written()),
        };
        let run = Arc::new(RunLog {
            execution_id: spec.execution_id,
            lane,
            keeping: spec.settings.keeping(),
            keep_for: spec.keep_for,
            redaction: spec.redaction,
            state: Mutex::new(RunState {
                epoch: spec.epoch,
                next_seq: spec.next_seq,
                pending: VecDeque::new(),
                pending_bytes: 0,
                handed: 0,
                holding,
                replica: None,
                wrote_files: false,
                wanted: 0,
                queued: false,
            }),
            poison: Poison::new(),
            acked: tokio::sync::watch::Sender::new(0),
            born_elsewhere: AtomicBool::new(false),
        });
        self.lanes[lane].runs.lock().expect("record writer").insert(spec.execution_id, run.clone());
        Arc::new(DriveJournal { writer: self.clone(), run })
    }

    /// Before a call that names `execution_id` reaches the broker: the
    /// run's record so far is written, so the broker finds the run (an
    /// unrecorded run leaves a note of itself, `weft_journal::unrecorded`).
    /// Nothing to do for a run this process does not write.
    pub async fn record_first(&self, execution_id: ExecutionId) -> anyhow::Result<()> {
        match self.live_run(execution_id) {
            Some(run) => self.note(&run).await,
            None => Ok(()),
        }
    }

    /// A run of this process stores files of its own, which its ending's
    /// sweep reclaims: said before the bytes move, since a store that fails
    /// part way may still leave a part to reclaim.
    pub fn stored_files(&self, execution_id: ExecutionId) {
        if let Some(run) = self.live_run(execution_id) {
            run.state.lock().expect("record writer").wrote_files = true;
        }
    }

    /// Answers once every event handed so far, of every run, was written
    /// or failed: what a stopping worker waits on before it goes.
    pub async fn written(&self) {
        for lane in &self.lanes {
            loop {
                let idle = lane.idle.notified();
                tokio::pin!(idle);
                idle.as_mut().enable();
                {
                    let queue = lane.queue.lock().expect("record writer");
                    if queue.runs.is_empty() && !queue.in_flight {
                        break;
                    }
                }
                idle.await;
            }
        }
    }

    /// End `execution_id`, a run this worker drives whose record it can no
    /// longer write (a write of it failed, see [`DriveJournal`], "Poison"):
    /// the broker writes its ending, failed with `why`, after the last row
    /// that landed, and the run is no longer this worker's. Asked until the
    /// broker answers: until then the run reads as running.
    pub async fn give_up(&self, execution_id: ExecutionId, why: &str) {
        give_up(self.record.as_ref(), execution_id, why).await;
    }

    /// Where the lanes send, and a run's record is read from.
    pub fn record(&self) -> &Arc<dyn RecordClient> {
        &self.record
    }

    fn live_run(&self, execution_id: ExecutionId) -> Option<Arc<RunLog>> {
        self.lanes[lane_of(execution_id, self.lanes.len())].runs.lock().expect("record writer").get(&execution_id).cloned()
    }

    /// Answer once every event `run` handed so far is on record. Refused
    /// once the run is poisoned.
    async fn flush(&self, run: &RunLog) -> anyhow::Result<()> {
        if let Some(e) = run.poisoned_error() {
            return Err(e);
        }
        let target = {
            let mut state = run.state.lock().expect("record writer");
            if *run.acked.borrow() >= state.handed {
                return Ok(());
            }
            state.wanted = state.wanted.max(state.handed);
            state.handed
        };
        self.lanes[run.lane].urgent.notify_one();
        let mut acked = run.acked.subscribe();
        tokio::select! {
            reached = acked.wait_for(|acked| *acked >= target) => {
                reached.map(|_| ()).map_err(|_| anyhow::anyhow!("the record writer ended before answering"))
            }
            () = run.poison.wait() => Err(run.poisoned_error().expect("woken by its poison")),
        }
    }

    /// Before a broker call that names `run`: what it handed so far goes on
    /// record. An unrecorded run writes its birth, which leaves a note of
    /// itself (its row, marked as not recorded). An unrecorded run whose
    /// ending was handed already, unwritten, gets no row now: no ending
    /// would ever follow it.
    async fn note(&self, run: &Arc<RunLog>) -> anyhow::Result<()> {
        {
            let lane = &self.lanes[run.lane];
            let mut queue = lane.queue.lock().expect("record writer");
            let mut state = run.state.lock().expect("record writer");
            if let Holding::Unrecorded(note) = &mut state.holding {
                anyhow::ensure!(note.written() || !run.poison.has_ending(), "the run ended before this call named it");
                let events: Vec<ExecEvent> = note.take_birth().into_iter().collect();
                let bytes = weight(&events);
                self.budget.force(bytes);
                push(lane, &mut queue, &mut state, run, events, bytes);
            }
        }
        self.flush(run).await
    }

    /// Put `events`, admitted, where `run`'s holding says (see `Holding`),
    /// `taken` bytes of room already taken for them: what goes on the lane
    /// takes the room it needs, and what does not gives it back. A push
    /// never waits while holding the lane's queue.
    fn put(&self, run: &Arc<RunLog>, events: Vec<ExecEvent>, taken: usize) {
        let lane = &self.lanes[run.lane];
        let mut queue = lane.queue.lock().expect("record writer");
        let mut state = run.state.lock().expect("record writer");
        if events.iter().any(ExecEvent::is_execution_terminal) {
            run.poison.ending_handed();
        }
        let events = self.route(&mut state, events);
        let bytes = weight(&events);
        if bytes > taken {
            self.budget.force(bytes - taken);
        } else {
            self.budget.release(taken - bytes);
        }
        push(lane, &mut queue, &mut state, run, events, bytes);
    }

    /// What of `events` goes on the lane now, as `run`'s holding says: all
    /// of them for a recorded run, what its note keeps for an unrecorded
    /// one (`weft_journal::unrecorded`).
    fn route(&self, state: &mut RunState, events: Vec<ExecEvent>) -> Vec<ExecEvent> {
        match &mut state.holding {
            Holding::Recorded => events,
            Holding::Unrecorded(note) => note.route(events),
        }
    }
}

impl Drop for WorkerJournal {
    fn drop(&mut self) {
        for lane in &self.lanes {
            lane.queue.lock().expect("record writer").closed = true;
            lane.wake.notify_one();
        }
    }
}

/// Which lane `execution_id` writes on: by the random tail of its id, never
/// the time bytes of a UUIDv7, which every run born in one millisecond
/// shares.
fn lane_of(execution_id: ExecutionId, lanes: usize) -> usize {
    let tail = u64::from_be_bytes(execution_id.as_bytes()[8..16].try_into().expect("eight bytes"));
    (tail % lanes as u64) as usize
}

fn weight(events: &[ExecEvent]) -> usize {
    events.iter().map(ExecEvent::held_bytes).sum()
}

/// Queue `events` (weighing `bytes`, their room taken) of `run` on `lane`.
fn push(lane: &Lane, queue: &mut LaneQueue, state: &mut RunState, run: &Arc<RunLog>, events: Vec<ExecEvent>, bytes: usize) {
    if events.is_empty() {
        return;
    }
    state.handed += events.len() as u64;
    state.pending_bytes += bytes;
    state.pending.extend(events);
    if !state.queued {
        state.queued = true;
        queue.runs.push_back(run.clone());
    }
    lane.wake.notify_one();
    if queue.runs.len() >= BATCH_RUNS {
        lane.urgent.notify_one();
    }
}

/// What waits to be written, across the lanes: a run handing events waits
/// while it is over [`WriterSettings::queue_bytes`].
struct Budget {
    held: AtomicUsize,
    max: usize,
    freed: tokio::sync::Notify,
}

impl Budget {
    /// Take room for `bytes`, waiting while there is none. A write bigger
    /// than the whole budget takes it once nothing else waits.
    async fn take(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        loop {
            let freed = self.freed.notified();
            tokio::pin!(freed);
            freed.as_mut().enable();
            let held = self.held.load(Ordering::Acquire);
            if held == 0 || held + bytes <= self.max {
                if self.held.compare_exchange(held, held + bytes, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    return;
                }
                continue;
            }
            freed.await;
        }
    }

    /// Take room for `bytes` without waiting, for code that cannot wait (a
    /// connection's hot path, an unrecorded run's note): the next
    /// run handing events waits for it.
    fn force(&self, bytes: usize) {
        if bytes > 0 {
            self.held.fetch_add(bytes, Ordering::AcqRel);
        }
    }

    fn release(&self, bytes: usize) {
        if bytes > 0 {
            self.held.fetch_sub(bytes, Ordering::AcqRel);
            self.freed.notify_waiters();
        }
    }
}

/// One writer lane: the runs pinned to it, those with events to write in
/// the order they came, and at most one batch of them on its way.
struct Lane {
    index: u16,
    queue: Mutex<LaneQueue>,
    /// Every run of this process pinned to this lane, by id, while it has a
    /// handle ([`WorkerJournal::record_first`]).
    runs: Mutex<HashMap<ExecutionId, Arc<RunLog>>>,
    /// Something was queued, or the writer closed.
    wake: tokio::sync::Notify,
    /// Somebody started waiting, or the queue holds a full batch: a
    /// gathering pause ends at once.
    urgent: tokio::sync::Notify,
    /// The lane went idle: nothing queued, nothing on its way.
    idle: tokio::sync::Notify,
}

#[derive(Default)]
struct LaneQueue {
    runs: VecDeque<Arc<RunLog>>,
    in_flight: bool,
    closed: bool,
}

/// One run's side of the writer. Locks are taken in the order: its lane's
/// queue, its state, its poison.
struct RunLog {
    execution_id: ExecutionId,
    lane: usize,
    keeping: Keeping,
    keep_for: KeepFor,
    redaction: weft_core::caller::Redaction,
    state: Mutex<RunState>,
    poison: Poison,
    /// How many of the events handed are on record.
    acked: tokio::sync::watch::Sender<u64>,
    /// Another worker bore this run first (two workers took one fire).
    born_elsewhere: AtomicBool,
}

struct RunState {
    epoch: i32,
    next_seq: i32,
    pending: VecDeque<ExecEvent>,
    pending_bytes: usize,
    /// How many events were queued, ever: what a flush waits to see on
    /// record.
    handed: u64,
    holding: Holding,
    /// The replica the run writes under, fixed by its first write.
    replica: Option<Option<String>>,
    wrote_files: bool,
    /// The most events a flush waits to see on record; the lane sends at
    /// once while fewer are ([`RunLog::waited_on`]). A flush already
    /// answered counts no more, so the run's later events gather as usual.
    wanted: u64,
    /// In its lane's queue.
    queued: bool,
}

/// Where a run's events go.
enum Holding {
    /// To its record: a recorded run.
    Recorded,
    /// To its note: an unrecorded run (`weft_journal::unrecorded`).
    Unrecorded(UnrecordedNote),
}

impl RunLog {
    /// Whether a flush waits for events of this run not on record yet.
    fn waited_on(&self) -> bool {
        self.state.lock().expect("record writer").wanted > *self.acked.borrow()
    }

    fn poisoned_error(&self) -> Option<anyhow::Error> {
        self.poison.get().map(|why| anyhow::anyhow!("an earlier record write of this run failed: {why}"))
    }

    /// Stop this run's writes for `why` (the first reason is kept). A run
    /// that already let go of its handle has nobody left to hear that, so
    /// it is said here, naming the run. `true` when the run let go with its
    /// ending handed, which will now never be written, so the writer has
    /// the broker end it ([`give_up`]) when the run is still this worker's.
    fn fail(&self, why: String) -> bool {
        let Stopped::Unheard { ending_handed } = self.poison.set(why.clone()) else { return false };
        tracing::error!(
            target: "weft_engine::journal",
            execution_id = %self.execution_id, error = %why,
            "a record write of a run that had already let go of it failed; the rest of its record is not written"
        );
        ending_handed
    }
}

/// End a run whose record its worker can no longer write (see
/// [`WorkerJournal::give_up`]): asked until the broker answers.
async fn give_up(record: &dyn RecordClient, execution_id: ExecutionId, why: &str) {
    let mut wait = RESEND_FIRST;
    loop {
        match record.give_up(execution_id, format!("the run's record could not be written: {why}")).await {
            Ok(()) => return,
            Err(BatchError::Refused(refused)) => {
                tracing::warn!(
                    target: "weft_engine::journal",
                    %execution_id, %refused,
                    "a run whose record could not be written is no longer this worker's to end"
                );
                return;
            }
            Err(BatchError::Unanswered(e)) => {
                tracing::warn!(
                    target: "weft_engine::journal",
                    %execution_id, error = %format!("{e:#}"), retry_in_ms = wait.as_millis() as u64,
                    "could not end a run whose record could not be written; asking again"
                );
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(RESEND_LONGEST);
            }
        }
    }
}

/// One run's share of a batch, as taken off its lane.
struct Taken {
    run: Arc<RunLog>,
    events: Vec<ExecEvent>,
    first_seq: i32,
    epoch: i32,
    wrote_files: bool,
    bytes: usize,
}

/// A lane's task: wait for runs, gather when nobody waits, send one batch,
/// answer, and again; until the writer is dropped and nothing is left.
async fn lane_loop(lane: Arc<Lane>, record: Arc<dyn RecordClient>, budget: Arc<Budget>, gather: Duration) {
    loop {
        loop {
            let woken = lane.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            {
                let queue = lane.queue.lock().expect("record writer");
                if !queue.runs.is_empty() {
                    break;
                }
                if queue.closed {
                    return;
                }
            }
            woken.await;
        }
        let urgent = || {
            let queue = lane.queue.lock().expect("record writer");
            queue.runs.len() >= BATCH_RUNS || queue.runs.iter().any(|run| run.waited_on())
        };
        if !gather.is_zero() {
            // A wake-up left over from a flush the last batch already
            // answered says nothing: the lane looks again before sending.
            let gathered = tokio::time::Instant::now() + gather;
            while !urgent() {
                tokio::select! {
                    () = tokio::time::sleep_until(gathered) => break,
                    () = lane.urgent.notified() => {}
                }
            }
        }
        let taken = take_batch(&lane, &budget);
        if !taken.is_empty() {
            let (body, head_runs) = frame(lane.index, &taken);
            let answer = send(record.as_ref(), body).await;
            let unwritten = answered(&taken, head_runs, answer);
            budget.release(taken.iter().map(|taken| taken.bytes).sum());
            for run in unwritten {
                let record = record.clone();
                let why = run.poison.get().expect("a stopped run keeps its reason");
                tokio::spawn(async move { give_up(record.as_ref(), run.execution_id, &why).await });
            }
        }
        let mut queue = lane.queue.lock().expect("record writer");
        queue.in_flight = false;
        if queue.runs.is_empty() {
            lane.idle.notify_waiters();
        }
    }
}

/// Take the lane's next batch: the runs somebody waits on first, then the
/// others in the order they came, each run's events in order, under the
/// caps. A run whose events do not all fit stays queued for the next one.
/// A stopped run's events are dropped here: its record is behind, and
/// rows after the gap would read as if there were none.
fn take_batch(lane: &Lane, budget: &Budget) -> Vec<Taken> {
    let mut queue = lane.queue.lock().expect("record writer");
    queue.in_flight = true;
    let (waited, rest): (Vec<Arc<RunLog>>, Vec<Arc<RunLog>>) =
        queue.runs.drain(..).partition(|run| run.waited_on());
    let mut taken: Vec<Taken> = Vec::new();
    let mut bytes = 0usize;
    for run in waited.into_iter().chain(rest) {
        let mut state = run.state.lock().expect("record writer");
        if run.poison.get().is_some() {
            budget.release(std::mem::take(&mut state.pending_bytes));
            state.pending.clear();
            state.queued = false;
            continue;
        }
        if taken.len() >= BATCH_RUNS || (!taken.is_empty() && bytes >= BATCH_BYTES) {
            drop(state);
            queue.runs.push_back(run);
            continue;
        }
        let mut events = Vec::new();
        let mut run_bytes = 0usize;
        while let Some(event) = state.pending.front() {
            let weight = event.held_bytes();
            if !(taken.is_empty() && events.is_empty()) && bytes + run_bytes + weight > BATCH_BYTES {
                break;
            }
            run_bytes += weight;
            events.push(state.pending.pop_front().expect("just looked"));
        }
        if events.is_empty() {
            drop(state);
            queue.runs.push_back(run);
            continue;
        }
        state.pending_bytes -= run_bytes;
        bytes += run_bytes;
        let first_seq = state.next_seq;
        state.next_seq += 1;
        let (epoch, wrote_files) = (state.epoch, state.wrote_files);
        state.queued = !state.pending.is_empty();
        let more = state.queued;
        drop(state);
        if more {
            queue.runs.push_back(run.clone());
        }
        taken.push(Taken { run, events, first_seq, epoch, wrote_files, bytes: run_bytes });
    }
    taken
}

/// `taken` as one call's body, off every lock: each run's events
/// serialized and compressed once, here, and what its row learns from
/// them.
fn frame(lane: u16, taken: &[Taken]) -> (Vec<u8>, usize) {
    let mut head = BatchHead { durable: false, lane, runs: Vec::with_capacity(taken.len()), selections: Vec::new() };
    let mut rows = Vec::with_capacity(taken.len());
    for taken in taken {
        let born = (taken.first_seq == 0).then(|| Born::of(&taken.events[0], taken.run.keep_for)).flatten();
        if let Some(ExecEvent::ExecutionStarted { selection: Some(selection), .. }) = born.as_ref().map(|_| &taken.events[0]) {
            if !head.selections.iter().any(|stored| stored.digest == selection.digest()) {
                head.selections.push(StoredSelection::of(selection));
            }
        }
        let row = weft_journal::stored::encode(&taken.events);
        head.durable |= taken.run.keeping == Keeping::Durable;
        head.runs.push(RunHead {
            execution_id: taken.run.execution_id,
            epoch: taken.epoch,
            first_seq: taken.first_seq,
            row_sizes: vec![u32::try_from(row.len()).expect("a row is under 4 GiB")],
            born,
            written: Written::of(&taken.events),
            wrote_files: taken.wrote_files,
        });
        rows.push(row);
    }
    let runs = head.runs.len();
    (weft_journal::frame::encode(&head, rows.iter().map(Vec::as_slice)), runs)
}

/// Send `body` until it is answered, within [`RESEND_FOR`]. A batch sent
/// again lands once (see the module doc).
async fn send(record: &dyn RecordClient, body: Vec<u8>) -> Result<BatchAnswer, BatchError> {
    let started = tokio::time::Instant::now();
    let mut wait = RESEND_FIRST;
    loop {
        match record.record_batch(body.clone()).await {
            Err(BatchError::Unanswered(e)) if started.elapsed() < RESEND_FOR => {
                tracing::warn!(
                    target: "weft_engine::journal",
                    error = %format!("{e:#}"), retry_in_ms = wait.as_millis() as u64,
                    "a batch of records got no answer; sending it again"
                );
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(RESEND_LONGEST);
            }
            answer => return answer,
        }
    }
}

/// Hand each run of the batch its fate: its events are on record, or it
/// stops. Answers the runs whose ending will now never be written by them,
/// which the broker is to end ([`RunLog::fail`]).
fn answered(taken: &[Taken], runs: usize, answer: Result<BatchAnswer, BatchError>) -> Vec<Arc<RunLog>> {
    let why = match answer {
        Ok(answer) if answer.fates.len() == runs => {
            for (taken, fate) in taken.iter().zip(answer.fates) {
                let run = &taken.run;
                match fate {
                    Fate::Accepted | Fate::AlreadyApplied => run.acked.send_modify(|acked| *acked += taken.events.len() as u64),
                    // Neither is this worker's to end: the run is another's.
                    Fate::BornElsewhere => {
                        run.born_elsewhere.store(true, Ordering::Release);
                        run.fail(format!("another worker bore run {} first; it is that worker's", run.execution_id));
                    }
                    Fate::Refused => {
                        run.fail(format!(
                            "this worker no longer drives run {} (another worker took it, or it ended); nothing more of it is recorded",
                            run.execution_id
                        ));
                    }
                }
            }
            return Vec::new();
        }
        Ok(answer) => format!("the record answered a batch of {runs} runs with {} fates", answer.fates.len()),
        Err(e) => e.to_string(),
    };
    tracing::error!(target: "weft_engine::journal", error = %why, runs, "a batch of records failed; the runs it held stop");
    taken.iter().filter(|taken| taken.run.fail(why.clone())).map(|taken| taken.run.clone()).collect()
}


/// Why a run's record can no longer be trusted, once it can't: the first
/// reason is kept, and whoever waits for it hears it at once. Whether the
/// run let go of its handle is kept beside it, under one lock, so a
/// failure is heard by the run or said by the writer, exactly once.
struct Poison {
    state: Mutex<PoisonState>,
    heard: tokio::sync::watch::Sender<bool>,
}

#[derive(Default)]
struct PoisonState {
    why: Option<String>,
    /// The run let go of its handle ([`DriveJournal::leave`]).
    let_go: bool,
    /// The run's ending was handed: its record ends with it.
    ending_handed: bool,
}

/// What [`Poison::set`] found.
enum Stopped {
    /// A reason was kept already.
    Already,
    /// The first reason, and the run still holds its handle: it hears it.
    Heard,
    /// The first reason, and the run let go: nobody hears it.
    Unheard { ending_handed: bool },
}

impl Poison {
    fn new() -> Self {
        Self { state: Mutex::new(PoisonState::default()), heard: tokio::sync::watch::Sender::new(false) }
    }

    fn set(&self, why: String) -> Stopped {
        let stopped = {
            let mut state = self.state.lock().expect("record writer");
            if state.why.is_some() {
                return Stopped::Already;
            }
            state.why = Some(why);
            if state.let_go { Stopped::Unheard { ending_handed: state.ending_handed } } else { Stopped::Heard }
        };
        self.heard.send_replace(true);
        stopped
    }

    fn get(&self) -> Option<String> {
        self.state.lock().expect("record writer").why.clone()
    }

    fn ending_handed(&self) {
        self.state.lock().expect("record writer").ending_handed = true;
    }

    fn has_ending(&self) -> bool {
        self.state.lock().expect("record writer").ending_handed
    }

    /// The run lets go of its handle: a reason kept from now on is said by
    /// the writer. Answers the reason already kept, if one is.
    fn let_go(&self) -> Option<String> {
        let mut state = self.state.lock().expect("record writer");
        state.let_go = true;
        state.why.clone()
    }

    async fn wait(&self) {
        let _ = self.heard.subscribe().wait_for(|poisoned| *poisoned).await;
    }
}

/// How a drive lets go of its run's handle ([`DriveJournal::leave`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leaving {
    /// The run ended: a durable run waits for its record, which is what it
    /// guarantees; a fast run lets go at once, its record trailing behind
    /// it. How it is kept is the handle's own.
    Ended,
    /// The run pauses or is handed back: whoever picks it up rebuilds it
    /// from its record, so every event is written first (an unrecorded run
    /// never carries on: it cannot be let go).
    CarriesOn,
}

/// The one handle a run writes its record through: its drive's events, its
/// nodes' own (through their ctx), its caller's exchange and its buses',
/// all on its lane in the order handed, and nothing on the run's path
/// waits for it.
///
/// **Waiting for it.** [`Self::flush`] answers once everything handed so
/// far is on record. Only the commit points ask: a durable run before a
/// step that reaches outside starts and before an answer leaves, any run
/// before it pauses ([`Self::leave`]), and any run before a broker call
/// that names it ([`WorkerJournal::record_first`]). A fast run that ends
/// does not wait: it hands its ending over and lets go, and its record
/// follows in order.
///
/// **Poison.** A failed write means the record is now behind what the
/// worker believes happened, and driving on rebuilds a different world on
/// every later fold. So the first failed write of a run stops all its
/// writes, every later [`Self::flush`] and write fails naming it, the
/// drive stops at its next turn ([`Self::is_poisoned`]), and a drive idling
/// on a long node hears it at once ([`Self::poisoned`]). The run's ending
/// is then the broker's to write ([`WorkerJournal::give_up`]): the drive
/// asks for it, or the writer does once the run let go.
pub struct DriveJournal {
    writer: Arc<WorkerJournal>,
    run: Arc<RunLog>,
}

impl DriveJournal {
    pub fn execution_id(&self) -> ExecutionId {
        self.run.execution_id
    }

    /// How the run is kept: a durable run waits for its record at every
    /// commit point.
    pub fn keeping(&self) -> Keeping {
        self.run.keeping
    }

    /// A write failed since the drive began.
    pub fn is_poisoned(&self) -> bool {
        self.run.poison.get().is_some()
    }

    /// Answers once a write of this run failed: what a drive waiting on
    /// something else (a long node, news) also waits on, so it stops at
    /// once.
    pub async fn poisoned(&self) {
        self.run.poison.wait().await
    }

    /// Another worker bore this run first (a fire handed to two workers):
    /// the run is that worker's, and this one stops without ending it.
    pub fn born_elsewhere(&self) -> bool {
        self.run.born_elsewhere.load(Ordering::Acquire)
    }

    /// Answer once every event handed so far is on record. Refused once the
    /// run is poisoned: a run whose record is already behind starts nothing
    /// more.
    pub async fn flush(&self) -> anyhow::Result<()> {
        self.writer.flush(&self.run).await
    }

    /// Let go of this handle as the run leaves its drive, everything it
    /// writes handed over: wait for it to be on record when `leaving` asks
    /// for that, or let go at once (a fast run that ended), its record
    /// written behind it and a write of it that fails later said by the
    /// writer. Refused when a write of the run already failed.
    pub async fn leave(&self, leaving: Leaving) -> anyhow::Result<()> {
        let written = match leaving {
            Leaving::Ended => match self.run.keeping {
                Keeping::Fast => Ok(()),
                Keeping::Durable => self.writer.flush(&self.run).await,
            },
            Leaving::CarriesOn => self.writer.note(&self.run).await,
        };
        match self.run.poison.let_go() {
            Some(why) => Err(anyhow::anyhow!("an earlier record write of this run failed: {why}")),
            None => written,
        }
    }

    /// Hand one event over at once, from code that cannot wait (a
    /// connection's hot path).
    pub fn hand(&self, event: &ExecEvent, replica: Option<&str>) -> anyhow::Result<()> {
        let events = self.admit(std::slice::from_ref(event), replica)?;
        self.writer.put(&self.run, events, 0);
        Ok(())
    }

    /// Hand `events` over, waiting for room while too much waits to be
    /// written. Room is taken before the events are put, so the run's
    /// events keep the order they were put in.
    async fn enqueue(&self, events: &[ExecEvent], replica: Option<&str>) -> anyhow::Result<()> {
        let events = self.admit(events, replica)?;
        let recorded = matches!(self.run.state.lock().expect("record writer").holding, Holding::Recorded);
        let taken = if recorded { weight(&events) } else { 0 };
        self.writer.budget.take(taken).await;
        self.writer.put(&self.run, events, taken);
        Ok(())
    }

    /// Refuse a write once the run is poisoned or ended, under a replica
    /// other than the one its first write fixed, or of another run; clean
    /// what is let through of the caller's credentials.
    fn admit(&self, events: &[ExecEvent], replica: Option<&str>) -> anyhow::Result<Vec<ExecEvent>> {
        if let Some(e) = self.run.poisoned_error() {
            return Err(e);
        }
        if self.run.poison.has_ending() {
            anyhow::bail!("run {} already handed its ending; nothing after it is recorded", self.run.execution_id);
        }
        let fail = |why: String| -> anyhow::Result<Vec<ExecEvent>> {
            self.run.fail(why.clone());
            anyhow::bail!(why)
        };
        {
            let mut state = self.run.state.lock().expect("record writer");
            match &state.replica {
                Some(first) if first.as_deref() != replica => {
                    drop(state);
                    return fail("one run wrote under two replicas; its record can no longer be trusted".into());
                }
                Some(_) => {}
                None => state.replica = Some(replica.map(str::to_string)),
            }
        }
        if let Some(stray) = events.iter().find(|event| event.execution_id() != self.run.execution_id) {
            return fail(format!(
                "the record of run {} was handed an event of run {}; a run records itself only",
                self.run.execution_id,
                stray.execution_id()
            ));
        }
        match events.iter().map(|event| weft_journal::redacted(event, &self.run.redaction)).collect() {
            Ok(events) => Ok(events),
            Err(e) => fail(format!("an event of run {} could not be cleaned of its caller's credentials: {e}", self.run.execution_id)),
        }
    }
}

impl Drop for DriveJournal {
    fn drop(&mut self) {
        let mut runs = self.writer.lanes[self.run.lane].runs.lock().expect("record writer");
        if runs.get(&self.run.execution_id).is_some_and(|run| Arc::ptr_eq(run, &self.run)) {
            runs.remove(&self.run.execution_id);
        }
    }
}

#[async_trait::async_trait]
impl JournalClient for DriveJournal {
    async fn record_event(&self, event: &ExecEvent, replica: Option<&str>) -> anyhow::Result<()> {
        self.enqueue(std::slice::from_ref(event), replica).await
    }

    async fn record_events(&self, events: &[ExecEvent], replica: Option<&str>) -> anyhow::Result<()> {
        self.enqueue(events, replica).await
    }

    async fn events_for_execution_id(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<ExecEvent>> {
        let record = self.writer.record.record_of(execution_id).await?;
        record.events(execution_id).map_err(anyhow::Error::msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_record::FakeRecord;

    fn started(execution_id: ExecutionId, node: &str) -> ExecEvent {
        ExecEvent::NodeStarted { execution_id, node_id: node.into(), frames: Vec::new(), at_unix: 0 }
    }

    fn completed(execution_id: ExecutionId) -> ExecEvent {
        ExecEvent::ExecutionCompleted { execution_id, at_unix: 0 }
    }

    fn failed(execution_id: ExecutionId) -> ExecEvent {
        ExecEvent::ExecutionFailed { execution_id, error: "boom".into(), at_unix: 0 }
    }

    fn writer(record: &Arc<FakeRecord>, settings: WriterSettings) -> Arc<WorkerJournal> {
        WorkerJournal::start(record.clone(), settings, &tokio::runtime::Handle::current())
    }

    fn spec(execution_id: ExecutionId, settings: RunSettings) -> RunSpec {
        RunSpec { execution_id, settings, keep_for: KeepFor::WEFT_DEFAULT, epoch: 1, next_seq: 0, redaction: Default::default() }
    }

    fn fast(writer: &Arc<WorkerJournal>) -> (ExecutionId, Arc<DriveJournal>) {
        let id = weft_core::new_execution_id();
        (id, writer.run(spec(id, RunSettings::default())))
    }

    fn durable(writer: &Arc<WorkerJournal>) -> (ExecutionId, Arc<DriveJournal>) {
        let id = weft_core::new_execution_id();
        (id, writer.run(spec(id, RunSettings::new(Keeping::Durable, true).unwrap())))
    }

    fn unrecorded(writer: &Arc<WorkerJournal>) -> (ExecutionId, Arc<DriveJournal>) {
        let id = weft_core::new_execution_id();
        (id, writer.run(spec(id, RunSettings::new(Keeping::Fast, false).unwrap())))
    }

    /// Writes answer at once and land in the order handed; a flush answers
    /// once they all have.
    #[tokio::test]
    async fn events_go_out_in_the_background_in_order() {
        let record = Arc::new(FakeRecord::default());
        let (run, drive) = fast(&writer(&record, WriterSettings::default()));
        for node in ["a", "b", "c"] {
            drive.record_event(&started(run, node), Some("w")).await.unwrap();
        }
        drive.flush().await.unwrap();
        assert_eq!(record.log(run), ["a", "b", "c"]);
    }

    /// Runs queued together on one lane go out as ONE batch, one row each,
    /// each run's events in order.
    #[tokio::test(start_paused = true)]
    async fn the_runs_of_a_lane_share_one_batch() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings { lanes: 1, gather: Duration::from_millis(50), ..Default::default() });
        let (one, first) = fast(&writer);
        let (other, second) = fast(&writer);
        first.record_event(&started(one, "a1"), Some("w")).await.unwrap();
        second.record_event(&started(other, "b1"), Some("w")).await.unwrap();
        first.record_event(&started(one, "a2"), Some("w")).await.unwrap();
        writer.written().await;
        let heads = record.heads();
        assert_eq!(heads.len(), 1, "one batch for both runs");
        assert_eq!(heads[0].runs.len(), 2);
        assert!(heads[0].runs.iter().all(|run| run.row_sizes.len() == 1 && run.first_seq == 0));
        assert_eq!(record.log(one), ["a1", "a2"]);
        assert_eq!(record.log(other), ["b1"]);
    }

    weft_core::stress_test!(
        name: each_runs_order_holds_across_lanes,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            let record = Arc::new(FakeRecord::default());
            let writer = writer(&record, WriterSettings { gather: Duration::from_micros(200), ..Default::default() });
            let runs: Vec<(ExecutionId, Arc<DriveJournal>)> = (0..40).map(|_| fast(&writer)).collect();
            let mut tasks = Vec::new();
            for (run, drive) in runs.clone() {
                tasks.push(tokio::spawn(async move {
                    for step in 0..25 {
                        drive.record_event(&started(run, &step.to_string()), Some("w")).await.unwrap();
                        if step % 7 == 0 {
                            drive.flush().await.unwrap();
                        }
                    }
                }));
            }
            for task in tasks {
                task.await.unwrap();
            }
            writer.written().await;
            let want: Vec<String> = (0..25).map(|step| step.to_string()).collect();
            for (run, _) in &runs {
                assert_eq!(record.log(*run), want);
            }
        }
    );

    /// Somebody waiting does not sit out the gathering pause.
    #[tokio::test(start_paused = true)]
    async fn a_waiter_skips_the_gathering_pause() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings { gather: Duration::from_secs(3600), ..Default::default() });
        let (run, drive) = fast(&writer);
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), drive.flush()).await.expect("answered without the pause").unwrap();
        assert_eq!(record.log(run), ["a"]);
    }

    /// A flush already answered makes nothing urgent: what the run hands
    /// after it gathers like any other events, so a durable run's next
    /// write carries everything up to the next one somebody waits for.
    #[tokio::test(start_paused = true)]
    async fn an_answered_flush_lets_later_events_gather() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings { gather: Duration::from_secs(3600), ..Default::default() });
        let (run, drive) = fast(&writer);
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        drive.flush().await.unwrap();
        drive.record_event(&started(run, "b"), Some("w")).await.unwrap();
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(record.heads().len(), 1, "b waits for the next flush or the gathering pause");
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        drive.flush().await.unwrap();
        assert_eq!(record.log(run), ["a", "b", "execution_completed"]);
        assert_eq!(record.heads().len(), 2);
    }

    /// A batch carries at most `BATCH_RUNS` runs, the run somebody waits on
    /// among the first.
    #[tokio::test(start_paused = true)]
    async fn a_batch_is_capped_and_waiters_go_first() {
        let record = Arc::new(FakeRecord::default());
        record.hold();
        let writer = writer(&record, WriterSettings { lanes: 1, gather: Duration::from_secs(3600), ..Default::default() });
        let runs: Vec<(ExecutionId, Arc<DriveJournal>)> = (0..BATCH_RUNS + 500).map(|_| fast(&writer)).collect();
        for (run, drive) in &runs {
            drive.record_event(&started(*run, "a"), Some("w")).await.unwrap();
        }
        // The first batch is on its way, held: the rest wait behind it.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let (last, waiting) = runs.last().unwrap().clone();
        let flushed = tokio::spawn(async move { waiting.flush().await });
        record.open();
        flushed.await.unwrap().unwrap();
        writer.written().await;
        let heads = record.heads();
        assert_eq!(heads[0].runs.len(), BATCH_RUNS);
        assert_eq!(heads[1].runs.len(), 500);
        assert_eq!(heads[1].runs[0].execution_id, last, "the run somebody waits on goes first");
    }

    /// A run the record refuses stops, and only it: its later writes and
    /// flushes fail naming why, and other runs go on.
    #[tokio::test]
    async fn a_refused_run_stops_alone() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings::default());
        let (run, refused) = fast(&writer);
        record.refuse(run);
        refused.record_event(&started(run, "a"), Some("w")).await.unwrap();
        let error = refused.flush().await.unwrap_err();
        assert!(format!("{error:#}").contains("no longer drives"), "{error:#}");
        assert!(refused.record_event(&started(run, "b"), Some("w")).await.is_err(), "a poisoned run takes nothing more");

        let (other, healthy) = fast(&writer);
        healthy.record_event(&started(other, "c"), Some("w")).await.unwrap();
        healthy.flush().await.expect("another run goes on");
        assert_eq!(record.log(other), ["c"]);
        assert!(record.gave_up().is_empty(), "a refused run is another's to end");
    }

    /// A batch whose answer was lost is sent again and lands once.
    #[tokio::test(start_paused = true)]
    async fn a_batch_sent_again_lands_once() {
        let record = Arc::new(FakeRecord::default());
        record.lose_answers(1);
        let (run, drive) = fast(&writer(&record, WriterSettings::default()));
        drive.record_events(&[started(run, "a"), started(run, "b")], Some("w")).await.unwrap();
        drive.flush().await.expect("answered on the second send");
        drive.record_event(&started(run, "c"), Some("w")).await.unwrap();
        drive.flush().await.unwrap();
        assert_eq!(record.log(run), ["a", "b", "c"], "each event once, in order");
        assert_eq!(record.heads().len(), 3, "the first batch twice, then the next");
    }

    /// A broker out of reach for longer than the resend window stops the
    /// run, like any failed write.
    #[tokio::test(start_paused = true)]
    async fn a_broker_out_of_reach_too_long_stops_the_run() {
        let record = Arc::new(FakeRecord::default());
        record.unanswered(usize::MAX);
        let (run, drive) = fast(&writer(&record, WriterSettings::default()));
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        assert!(drive.flush().await.is_err());
        assert!(drive.is_poisoned());
    }

    /// A drive waiting on something else hears its record fail at once.
    #[tokio::test]
    async fn a_waiting_drive_hears_a_failed_write() {
        let record = Arc::new(FakeRecord::default());
        record.refuse_batches(1);
        let (run, drive) = fast(&writer(&record, WriterSettings::default()));
        let waiting = {
            let drive = drive.clone();
            tokio::spawn(async move { drive.poisoned().await })
        };
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("heard the failure").unwrap();
        assert!(drive.is_poisoned());
    }

    /// A write under another replica, or of another run, poisons the run.
    #[tokio::test]
    async fn a_run_writes_itself_under_one_replica() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings::default());
        let (run, drive) = fast(&writer);
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        assert!(drive.record_event(&started(run, "b"), Some("other")).await.is_err());
        assert!(drive.is_poisoned());

        let (_, other) = fast(&writer);
        assert!(other.record_event(&started(weft_core::new_execution_id(), "x"), Some("w")).await.is_err());
        assert!(other.is_poisoned());
    }

    /// Nothing is written after a run's ending.
    #[tokio::test]
    async fn nothing_follows_an_ending() {
        let record = Arc::new(FakeRecord::default());
        let (run, drive) = fast(&writer(&record, WriterSettings::default()));
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        assert!(drive.record_event(&started(run, "late"), Some("w")).await.is_err());
        assert!(!drive.is_poisoned(), "the run's record is whole");
        drive.flush().await.unwrap();
        assert_eq!(record.log(run), ["execution_completed"]);
    }

    weft_core::stress_test!(
        name: a_full_queue_makes_runs_wait_until_a_batch_is_answered,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            let record = Arc::new(FakeRecord::default());
            record.hold();
            let one = started(weft_core::new_execution_id(), "a").held_bytes();
            let writer = writer(&record, WriterSettings { queue_bytes: one, ..Default::default() });
            let (run, drive) = fast(&writer);
            drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
            let next = {
                let drive = drive.clone();
                tokio::spawn(async move { drive.record_event(&started(run, "b"), Some("w")).await })
            };
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!next.is_finished(), "no room until the first batch is answered");
            record.open();
            tokio::time::timeout(Duration::from_secs(5), next).await.expect("room once answered").unwrap().unwrap();
            drive.flush().await.unwrap();
            assert_eq!(record.log(run), ["a", "b"]);
        }
    );

    /// A fast run that ended lets go at once, however far behind its record
    /// is, and its record lands afterwards in order, its ending last.
    #[tokio::test]
    async fn a_fast_run_lets_go_without_waiting_for_its_record() {
        let record = Arc::new(FakeRecord::default());
        record.hold();
        let writer = writer(&record, WriterSettings::default());
        let (run, drive) = fast(&writer);
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), drive.leave(Leaving::Ended)).await.expect("let go").unwrap();
        assert!(record.log(run).is_empty(), "nothing written yet");
        record.open();
        writer.written().await;
        assert_eq!(record.log(run), ["a", "execution_completed"]);
    }

    /// A durable run that ended waits for its record, ending included, and
    /// its batch is synced.
    #[tokio::test(start_paused = true)]
    async fn a_durable_run_waits_for_its_record_to_end() {
        let record = Arc::new(FakeRecord::default());
        record.hold();
        let (run, drive) = durable(&writer(&record, WriterSettings::default()));
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        let leaving = tokio::spawn(async move { drive.leave(Leaving::Ended).await });
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert!(!leaving.is_finished(), "still waiting for its record");
        record.open();
        leaving.await.unwrap().unwrap();
        assert_eq!(record.log(run), ["a", "execution_completed"]);
        assert!(record.heads().iter().all(|head| head.durable));
    }

    /// A run that carries on elsewhere waits for its record, even a fast
    /// one: whoever picks it up rebuilds it from that record.
    #[tokio::test(start_paused = true)]
    async fn a_run_that_carries_on_waits_for_its_record() {
        let record = Arc::new(FakeRecord::default());
        record.hold();
        let (run, drive) = fast(&writer(&record, WriterSettings::default()));
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        let leaving = tokio::spawn(async move { drive.leave(Leaving::CarriesOn).await });
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert!(!leaving.is_finished(), "still waiting for its record");
        record.open();
        leaving.await.unwrap().unwrap();
        assert_eq!(record.log(run), ["a"]);
    }

    /// A fast run that let go and whose record then failed is ended by the
    /// broker, naming why: its own ending never lands.
    #[tokio::test]
    async fn a_run_whose_record_fails_after_it_let_go_is_given_up() {
        let record = Arc::new(FakeRecord::default());
        record.refuse_batches(1);
        let writer = writer(&record, WriterSettings::default());
        let (run, drive) = fast(&writer);
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        drive.leave(Leaving::Ended).await.unwrap();
        writer.written().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while record.gave_up().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("given up");
        let gave_up = record.gave_up();
        assert_eq!(gave_up.len(), 1);
        assert_eq!(gave_up[0].0, run);
        assert!(gave_up[0].1.contains("could not be written") && gave_up[0].1.contains("refused the batch"), "{}", gave_up[0].1);
    }

    /// A run another worker bore first stops, says so, and is not ended by
    /// this one.
    #[tokio::test]
    async fn a_run_born_elsewhere_stops_without_ending_it() {
        let record = Arc::new(FakeRecord::default());
        let (run, drive) = fast(&writer(&record, WriterSettings::default()));
        record.born_elsewhere(run);
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        assert!(drive.flush().await.is_err());
        assert!(drive.born_elsewhere());
        assert!(record.gave_up().is_empty());
    }

    /// An unrecorded run's birth.
    fn born(execution_id: ExecutionId) -> ExecEvent {
        let mut birth: ExecEvent = serde_json::from_value(serde_json::json!({
            "kind": "execution_started", "project_id": execution_id, "entry_node": "door", "phase": "fire",
            "settings": { "recorded": false }, "at_unix": 0
        }))
        .expect("a birth");
        if let ExecEvent::ExecutionStarted { execution_id: id, .. } = &mut birth {
            *id = execution_id;
        }
        birth
    }

    /// An unrecorded run that ends well writes nothing.
    #[tokio::test]
    async fn an_unrecorded_run_that_ends_well_writes_nothing() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings::default());
        let (run, drive) = unrecorded(&writer);
        drive.record_event(&born(run), Some("w")).await.unwrap();
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        drive.leave(Leaving::Ended).await.unwrap();
        writer.written().await;
        assert!(record.heads().is_empty());
    }

    /// An unrecorded run that fails writes its birth and its failure, and
    /// none of its history.
    #[tokio::test]
    async fn an_unrecorded_run_that_fails_writes_its_failure() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings::default());
        let (run, drive) = unrecorded(&writer);
        drive.record_event(&born(run), Some("w")).await.unwrap();
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        drive.record_event(&failed(run), Some("w")).await.unwrap();
        drive.leave(Leaving::Ended).await.unwrap();
        writer.written().await;
        assert_eq!(record.log(run), ["execution_started", "execution_failed"]);
        assert_eq!(record.heads().len(), 1);
    }

    /// A broker call naming an unrecorded run writes its birth first, and
    /// its ending then follows, since its row must end; nothing in between.
    #[tokio::test]
    async fn an_unrecorded_run_named_to_the_broker_leaves_a_note_and_ends_it() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings::default());
        let (run, drive) = unrecorded(&writer);
        drive.record_event(&born(run), Some("w")).await.unwrap();
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        writer.record_first(run).await.unwrap();
        assert_eq!(record.log(run), ["execution_started"], "the note");
        drive.record_event(&started(run, "b"), Some("w")).await.unwrap();
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        writer.record_first(run).await.expect("its row exists, and its ending follows");
        drive.leave(Leaving::Ended).await.unwrap();
        writer.written().await;
        assert_eq!(record.log(run), ["execution_started", "execution_completed"]);
        writer.record_first(weft_core::new_execution_id()).await.expect("a run this process does not write");
    }

    /// A broker call naming an unrecorded run that ended with no row is
    /// refused: its ending would never follow the row it would leave.
    #[tokio::test]
    async fn an_unrecorded_run_that_ended_unwritten_is_not_named() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings::default());
        let (run, drive) = unrecorded(&writer);
        drive.record_event(&born(run), Some("w")).await.unwrap();
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        let refused = writer.record_first(run).await.expect_err("ended with no row");
        assert!(refused.to_string().contains("ended before this call named it"), "{refused:#}");
        drive.leave(Leaving::Ended).await.unwrap();
        writer.written().await;
        assert!(record.heads().is_empty());
    }

    /// A run that stored files says so on its rows from then on.
    #[tokio::test]
    async fn a_run_that_stored_files_says_so() {
        let record = Arc::new(FakeRecord::default());
        let writer = writer(&record, WriterSettings::default());
        let (run, drive) = fast(&writer);
        drive.record_event(&started(run, "a"), Some("w")).await.unwrap();
        drive.flush().await.unwrap();
        writer.stored_files(run);
        drive.record_event(&completed(run), Some("w")).await.unwrap();
        drive.flush().await.unwrap();
        let wrote: Vec<bool> = record.heads().iter().map(|head| head.runs[0].wrote_files).collect();
        assert_eq!(wrote, [false, true]);
    }
}
