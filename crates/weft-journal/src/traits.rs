//! The journal as a run's code sees it: where its events go, and how a
//! finished run's record is read back. On a worker the events go to the
//! run's writer lane (`weft_engine::journal_writer`), and a record is read
//! through the broker; the dispatcher has its own, wider surface
//! (`weft_dispatcher::store::RunStore`).

use async_trait::async_trait;

use crate::events::RunEvent;

/// One event of a run's record, decoded: the row it was written in (one
/// write is one row, `crate::record::RunLogRow`) and its place inside it.
#[derive(Debug, Clone)]
pub struct JournalRow {
    pub seq: i32,
    pub index: u32,
    pub event: RunEvent,
}

pub use weft_core::record_ref::Position;

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
    async fn record_event(&self, event: &RunEvent, replica: Option<&str>) -> anyhow::Result<()>;

    /// Hand `events`, all of one run, in order.
    async fn record_events(&self, events: &[RunEvent], replica: Option<&str>) -> anyhow::Result<()> {
        for event in events {
            self.record_event(event, replica).await?;
        }
        Ok(())
    }

    /// One page of a run's record after row `after` (from its first when
    /// `None`), each event with its place, empty once none is left: a seed
    /// ancestor's (which ended), or the run's own once what it handed is on
    /// record. A row that does not decode fails the read.
    async fn record_page(&self, run_id: weft_core::RunId, after: Option<i32>) -> anyhow::Result<Vec<JournalRow>>;
}

/// Every event of a run's record after row `after`, read a page at a time
/// through `page` (one page after a row, as [`JournalClient::record_page`]
/// answers): the few that landed since a fold last looked.
pub async fn rows_after<F, Fut>(page: F, after: Option<i32>) -> anyhow::Result<Vec<JournalRow>>
where
    F: FnMut(Option<i32>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Vec<JournalRow>>>,
{
    crate::record::fold_pages(page, after, Vec::new(), |rows: &mut Vec<JournalRow>, page: Vec<JournalRow>| {
        rows.extend(page);
        Ok(())
    })
    .await
}

/// One page of run `run_id`'s record after `after` through `client`,
/// decoded on the blocking pool: what a worker reads a record by.
pub async fn decoded_page(client: &dyn RecordClient, run_id: weft_core::RunId, after: Option<i32>) -> anyhow::Result<Vec<JournalRow>> {
    let page = client.record_of(run_id, after).await?;
    crate::on_blocking_pool(move || page.decode(run_id)).await.map_err(anyhow::Error::msg)
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

    /// One page of the record of a run of this worker's project, raw:
    /// its rows after `after` (from its first when `None`), empty once
    /// none is left (`crate::record::read_page`).
    async fn record_of(&self, run_id: weft_core::RunId, after: Option<i32>) -> anyhow::Result<crate::record::RawRecord>;

    /// End `run_id`, a run this worker drives but whose record it can
    /// no longer write: the record writes its ending, failed with `why`,
    /// after the last row that landed, and the run is no longer this
    /// worker's (`crate::record::give_up_in`). `Refused` when this worker
    /// does not drive it.
    async fn give_up(&self, run_id: weft_core::RunId, why: String) -> Result<(), BatchError>;
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

