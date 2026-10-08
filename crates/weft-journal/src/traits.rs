//! The journal as a run's code sees it: where its events go, and how a
//! finished run's record is read back. On a worker the events go to the
//! run's writer lane (`weft_engine::journal_writer`), and a record is read
//! through the broker; the dispatcher has its own, wider surface
//! (`weft_dispatcher::journal::Journal`).

use async_trait::async_trait;

use crate::events::ExecEvent;

/// One event of a run's record, decoded: the row it was written in (one
/// write is one row, `crate::record::RunLogRow`) and its place inside it.
#[derive(Debug, Clone)]
pub struct JournalRow {
    pub seq: i32,
    pub index: u32,
    pub event: ExecEvent,
}

/// An event's place in its run's record, in the order the events were
/// written: what a reader that names single events counts by (the
/// editor's live feed). A read of a run's record resumes after a row,
/// never inside one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    pub seq: i32,
    pub index: u32,
}

impl JournalRow {
    pub fn position(&self) -> Position {
        Position { seq: self.seq, index: self.index }
    }
}

/// Write and read surface a run's code holds. `replica` is the writing
/// worker's replica id; a drive writes under one replica only.
#[async_trait]
pub trait JournalClient: Send + Sync {
    /// Hand one event to the run's record.
    async fn record_event(&self, event: &ExecEvent, replica: Option<&str>) -> anyhow::Result<()>;

    /// Hand `events`, all of one run, in order.
    async fn record_events(&self, events: &[ExecEvent], replica: Option<&str>) -> anyhow::Result<()> {
        for event in events {
            self.record_event(event, replica).await?;
        }
        Ok(())
    }

    /// Every event of a run on record, in order: a seed ancestor's (which
    /// ended), or the run's own once what it handed is on record. A row that
    /// does not decode fails the read.
    async fn events_for_execution_id(&self, execution_id: weft_core::ExecutionId) -> anyhow::Result<Vec<ExecEvent>>;
}

/// Where a worker's writer lanes send their batches, and where a run's
/// record is read from: the broker, over the worker's line. A trait so the
/// lanes are tested against a fake.
#[async_trait]
pub trait RecordClient: Send + Sync {
    /// Send one batch (`crate::frame`), answered run by run in its order.
    /// Sending a batch again is always safe: what already went in answers
    /// `AlreadyApplied` (`crate::record::Fate`).
    async fn record_batch(&self, batch: Vec<u8>) -> Result<crate::frame::BatchAnswer, BatchError>;

    /// The whole record of a run of this worker's project, raw.
    async fn record_of(&self, execution_id: weft_core::ExecutionId) -> anyhow::Result<crate::record::RawRecord>;

    /// End `execution_id`, a run this worker drives but whose record it can
    /// no longer write: the record writes its ending, failed with `why`,
    /// after the last row that landed, and the run is no longer this
    /// worker's (`crate::record::give_up_in`). `Refused` when this worker
    /// does not drive it.
    async fn give_up(&self, execution_id: weft_core::ExecutionId, why: String) -> Result<(), BatchError>;
}

/// Why a batch got no answer.
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    /// The record refused the batch whole (it cannot read it, or this
    /// process may not write): sending it again would hear the same.
    #[error("the record refused the batch: {0}")]
    Refused(String),
    /// No answer came: the batch may have landed or not, and sending it
    /// again is safe.
    #[error("the batch got no answer: {0:#}")]
    Unanswered(anyhow::Error),
}

/// The no-write implementation: every write vanishes, every read answers
/// empty. For runtimes that drive a node body OUTSIDE a run (a node
/// self-test has no record to fold), and for tests exercising code that
/// only incidentally holds a journal client.
pub struct NoopJournal;

#[async_trait]
impl JournalClient for NoopJournal {
    async fn record_event(&self, _event: &ExecEvent, _replica: Option<&str>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn events_for_execution_id(&self, _execution_id: weft_core::ExecutionId) -> anyhow::Result<Vec<ExecEvent>> {
        Ok(Vec::new())
    }
}
