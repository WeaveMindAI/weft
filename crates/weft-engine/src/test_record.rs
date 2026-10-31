//! A record in memory, for the engine's tests: the broker's record as a
//! worker's writer lanes see it (`weft_journal::RecordClient`), with
//! levers to refuse a run, lose answers or hold every batch.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use weft_core::RunId;
use weft_journal::frame::{BatchAnswer, BatchHead, RunHead};
use weft_journal::record::{Fate, RawRecord, RunLogRow};
use weft_journal::{BatchError, RunEvent, RecordClient};

/// A batch as a lane sent it: each run's head and its events.
pub(crate) fn decode_batch(body: &[u8]) -> (BatchHead, Vec<(RunHead, Vec<RunEvent>)>) {
    let batch = weft_journal::frame::decode(body).expect("a lane frames what it sends");
    let runs = batch
        .head
        .runs
        .iter()
        .zip(&batch.rows)
        .map(|(run, row)| {
            let events = weft_journal::stored::decode(run.run_id, row, batch.head.events_version).expect("a lane writes rows that read back");
            (run.clone(), events)
        })
        .collect();
    (batch.head, runs)
}

/// How many rows a page of an in-memory record holds: a couple, so every
/// reader of a record in the engine's tests crosses page edges.
const PAGE: usize = 2;

/// `events`, all of one run, as a record whose every event is a row of its
/// own, read after row `after` a page at a time, as the broker's record
/// answers (`weft_journal::record::read_page`).
pub(crate) fn page_of(events: &[RunEvent], after: Option<i32>) -> RawRecord {
    let rows = events
        .iter()
        .enumerate()
        .map(|(seq, event)| (seq as i32, event))
        .filter(|(seq, _)| after.is_none_or(|after| *seq > after))
        .take(PAGE)
        .map(|(seq, event)| RunLogRow::of(seq, std::slice::from_ref(event)))
        .collect();
    RawRecord { rows }
}

/// The same page, decoded, as a `weft_journal::JournalClient` answers it.
pub(crate) fn rows_of(run_id: RunId, events: &[RunEvent], after: Option<i32>) -> Vec<weft_journal::JournalRow> {
    page_of(events, after).decode(run_id).expect("an in-memory record reads back")
}

/// What the record keeps of one run.
#[derive(Debug, Default)]
struct Run {
    last_seq: i32,
    events: Vec<RunEvent>,
}

#[derive(Default)]
struct State {
    runs: HashMap<RunId, Run>,
    /// Every secret an accepted run's batch carried, by id.
    secrets: HashMap<weft_core::secret::SecretId, serde_json::Value>,
    heads: Vec<BatchHead>,
    gave_up: Vec<(RunId, String)>,
    refused: HashSet<RunId>,
    born_elsewhere: HashSet<RunId>,
    unanswered: usize,
    lost_answers: usize,
    refused_batches: usize,
}

/// The record in memory (see the module doc). Open unless [`Self::hold`]
/// closed it.
pub(crate) struct FakeRecord {
    state: Mutex<State>,
    open: tokio::sync::watch::Sender<bool>,
}

impl Default for FakeRecord {
    fn default() -> Self {
        Self { state: Mutex::new(State::default()), open: tokio::sync::watch::Sender::new(true) }
    }
}

impl FakeRecord {
    /// Hold every batch until [`Self::open`].
    pub(crate) fn hold(&self) {
        self.open.send_replace(false);
    }

    pub(crate) fn open(&self) {
        self.open.send_replace(true);
    }

    /// Answer `Refused` for `run_id`'s rows from now on.
    pub(crate) fn refuse(&self, run_id: RunId) {
        self.state.lock().unwrap().refused.insert(run_id);
    }

    /// Answer `BornElsewhere` for the birth of `run_id`.
    pub(crate) fn born_elsewhere(&self, run_id: RunId) {
        self.state.lock().unwrap().born_elsewhere.insert(run_id);
    }

    /// The next `calls` batches reach nothing and get no answer.
    pub(crate) fn unanswered(&self, calls: usize) {
        self.state.lock().unwrap().unanswered = calls;
    }

    /// The next `calls` batches land and their answer is lost.
    pub(crate) fn lose_answers(&self, calls: usize) {
        self.state.lock().unwrap().lost_answers = calls;
    }

    /// The next `calls` batches are refused whole.
    pub(crate) fn refuse_batches(&self, calls: usize) {
        self.state.lock().unwrap().refused_batches = calls;
    }

    /// Every event on record of `run_id`, in order.
    pub(crate) fn events(&self, run_id: RunId) -> Vec<RunEvent> {
        self.state.lock().unwrap().runs.get(&run_id).map(|run| run.events.clone()).unwrap_or_default()
    }

    /// The node of every `NodeStarted` on record of `run_id`, and the
    /// kind of every other event, in order.
    pub(crate) fn log(&self, run_id: RunId) -> Vec<String> {
        self.events(run_id)
            .iter()
            .map(|event| match event {
                RunEvent::NodeStarted { node_id, .. } => node_id.clone(),
                other => other.kind_str().to_string(),
            })
            .collect()
    }

    /// The heads of the batches that landed, in order.
    pub(crate) fn heads(&self) -> Vec<BatchHead> {
        self.state.lock().unwrap().heads.clone()
    }

    /// The runs ended through [`RecordClient::give_up`], with why.
    pub(crate) fn gave_up(&self) -> Vec<(RunId, String)> {
        self.state.lock().unwrap().gave_up.clone()
    }
}

#[async_trait::async_trait]
impl RecordClient for FakeRecord {
    async fn record_batch(&self, batch: Vec<u8>) -> Result<BatchAnswer, BatchError> {
        self.open.subscribe().wait_for(|open| *open).await.expect("the record outlives its writes");
        let mut state = self.state.lock().unwrap();
        if state.unanswered > 0 {
            state.unanswered -= 1;
            return Err(BatchError::Unanswered(anyhow::anyhow!("the broker is out of reach")));
        }
        if state.refused_batches > 0 {
            state.refused_batches -= 1;
            return Err(BatchError::Refused("the broker refused the batch".into()));
        }
        let (head, runs) = decode_batch(&batch);
        let mut fates = Vec::new();
        for (run, events) in runs {
            let id = run.run_id;
            let fate = if state.refused.contains(&id) {
                Fate::Refused
            } else if run.first_seq == 0 && state.born_elsewhere.contains(&id) {
                Fate::BornElsewhere
            } else {
                match state.runs.get(&id) {
                    Some(known) if run.first_seq <= known.last_seq => Fate::AlreadyApplied,
                    Some(known) if run.first_seq != known.last_seq + 1 => Fate::Refused,
                    None if run.first_seq != 0 => Fate::Refused,
                    _ => Fate::Accepted,
                }
            };
            if fate == Fate::Accepted {
                let known = state.runs.entry(id).or_default();
                known.last_seq = run.first_seq;
                known.events.extend(events);
                state.secrets.extend(run.secrets.into_iter().map(|secret| (secret.secret_id, secret.value)));
            }
            fates.push(fate);
        }
        state.heads.push(head);
        if state.lost_answers > 0 {
            state.lost_answers -= 1;
            return Err(BatchError::Unanswered(anyhow::anyhow!("the answer was lost")));
        }
        Ok(BatchAnswer { fates })
    }

    async fn record_of(&self, run_id: RunId, after: Option<i32>) -> anyhow::Result<RawRecord> {
        Ok(page_of(&self.events(run_id), after))
    }

    async fn secret(&self, secret_id: weft_core::secret::SecretId) -> anyhow::Result<Option<serde_json::Value>> {
        Ok(self.state.lock().unwrap().secrets.get(&secret_id).cloned())
    }

    async fn give_up(&self, run_id: RunId, why: String) -> Result<(), BatchError> {
        self.state.lock().unwrap().gave_up.push((run_id, why));
        Ok(())
    }
}
