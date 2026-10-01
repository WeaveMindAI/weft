//! Unrecorded runs (`weft_core::exec::RunKind::Unrecorded`): a run whose
//! journal lives in the worker's memory instead of the database.
//!
//! A website polling a status route every few seconds would otherwise
//! leave a recorded run per poll. The run still has a real execution (its
//! `execution` row is written at birth: broker scope, costs, owner
//! fencing and cancel all need it), but its rows are held here, where the
//! engine reads them back exactly as it reads the real journal. Only cost
//! rows go through to the database as they happen, because money is
//! never dropped.
//!
//! How it ends decides what is left ([`UnrecordedJournal::settle`]):
//! a failed run writes its whole record afterwards and becomes an
//! ordinary recorded run, so it lists and inspects like any other (its
//! cost rows written so far are taken out and written again in their
//! place in the record, so every one lands after the birth and the step
//! it belongs to); a run that completed or was cancelled leaves nothing
//! but its costs.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use weft_core::exec::RunKind;
use weft_core::ExecutionId;

use crate::events::ExecEvent;
use crate::traits::{JournalClient, RawJournalRow};

/// THE rule for "this run is live", as a SQL predicate over an
/// `execution` row aliased `ec`: every project sweep (stop, wipe,
/// stop by tag, the drain and running counts) reads it, so a run is live
/// to all of them or to none.
///
/// A recorded run is live until its journal holds an ending. An
/// unrecorded run never writes one when it succeeds, so it is live while
/// its execute task is still waiting or held by a claim that is being
/// renewed: that is exactly while a worker can still be driving it (a
/// claim that lapsed is a worker that went away). Forgetting it ends that
/// at once: the row goes, or, when its costs keep the row, `ended_at_unix`
/// is stamped, both before its task is closed.
pub const LIVE_RUN_SQL: &str = concat!("( \
    (ec.kind = 'execution' AND NOT EXISTS ( \
        SELECT 1 FROM exec_event term WHERE term.execution_id = ec.execution_id \
          AND term.kind IN ", crate::execution_terminal_kinds_sql!(), ")) \
    OR (ec.kind = 'unrecorded' AND ec.ended_at_unix IS NULL AND EXISTS ( \
        SELECT 1 FROM task run_task \
        WHERE run_task.execution_id = ec.execution_id AND run_task.kind = 'execute' \
          AND (run_task.status = 'pending' \
               OR (run_task.status = 'claimed' \
                   AND run_task.claimed_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT)))) \
)");

/// The channel an unrecorded run's ending is announced on, at the commit
/// of the transaction that forgot it, with an [`UnrecordedEnded`] as the
/// payload. A recorded run's ending wakes the dispatcher through its
/// terminal journal row; an unrecorded run writes none, so this is its
/// equivalent, and the dispatcher re-checks the drain of the activation
/// the run belonged to.
pub const UNRECORDED_ENDED_CHANNEL: &str = "weft_unrecorded_ended";

/// What the dispatcher needs to finish an ended unrecorded run's
/// bookkeeping: the entry slot it held, and the drain it may have been
/// holding up. Its row can be gone by the time this is heard, so the
/// ending carries it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UnrecordedEnded {
    pub execution_id: ExecutionId,
    pub project_id: uuid::Uuid,
    pub fired_by: Option<String>,
    pub instance: Option<String>,
}

/// What an unrecorded run left behind once it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settled {
    /// It failed: its whole record is now in the journal, as a recorded run.
    Recorded,
    /// It completed or was cancelled: nothing of it is kept but its costs.
    Forgotten,
}

/// The journal of one unrecorded run, held in memory, seeded with the
/// birth rows its execute task carried. Reads and writes of its own execution
/// stay here; cost rows also go to `real` as they are written.
pub struct UnrecordedJournal {
    execution_id: ExecutionId,
    real: Arc<dyn JournalClient>,
    held: Mutex<Held>,
    written: tokio::sync::Notify,
}

/// The run's rows, and whether they were taken to be settled: after
/// that, a row written would be lost, so it is refused.
struct Held {
    events: Vec<ExecEvent>,
    settled: bool,
}

impl UnrecordedJournal {
    /// Seed the run's memory with its birth: the `ExecutionStarted` (of
    /// the unrecorded kind) first, then its kicks, all of `execution_id`.
    pub fn seeded(execution_id: ExecutionId, birth: Vec<ExecEvent>, real: Arc<dyn JournalClient>) -> anyhow::Result<Arc<Self>> {
        match birth.first() {
            Some(ExecEvent::ExecutionStarted { run_kind: RunKind::Unrecorded, .. }) => {}
            _ => anyhow::bail!(
                "the execute task for unrecorded run {execution_id} does not start with its unrecorded birth"
            ),
        }
        if let Some(stray) = birth.iter().find(|event| event.execution_id() != execution_id) {
            anyhow::bail!("the birth of unrecorded run {execution_id} carries a row of {}", stray.execution_id());
        }
        Ok(Arc::new(Self {
            execution_id,
            real,
            held: Mutex::new(Held { events: birth, settled: false }),
            written: tokio::sync::Notify::new(),
        }))
    }

    /// Every row the run holds, in order.
    pub fn events(&self) -> Vec<ExecEvent> {
        self.held.lock().expect("unrecorded journal").events.clone()
    }

    fn rows_after_now(&self, after_id: i64) -> anyhow::Result<Vec<RawJournalRow>> {
        self.held
            .lock()
            .expect("unrecorded journal")
            .events
            .iter()
            .enumerate()
            .map(|(i, event)| (i as i64 + 1, event))
            .filter(|(id, _)| *id > after_id)
            .map(|(id, event)| Ok(RawJournalRow { id, payload: serde_json::to_string(event)? }))
            .collect()
    }

    /// End the run's record, once the run is over and everything it
    /// writes is written (its caller's sink closed and drained). From
    /// here on a write is refused. A run that failed, or stopped with no
    /// ending at all, is written to the real journal in full (with a
    /// failure ending in the second case) as a recorded run. Anything
    /// else is forgotten.
    pub async fn settle(&self, replica: Option<&str>) -> anyhow::Result<Settled> {
        let events = {
            let mut held = self.held.lock().expect("unrecorded journal");
            anyhow::ensure!(!held.settled, "unrecorded run {} was already settled", self.execution_id);
            held.settled = true;
            if !held.events.iter().any(ExecEvent::is_execution_terminal) {
                let at_unix = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock before unix epoch")
                    .as_secs();
                held.events.push(ExecEvent::ExecutionFailed {
                    execution_id: self.execution_id,
                    error: "the run stopped without an ending; an unrecorded run cannot wait, so it \
                            cannot be parked to finish later"
                        .into(),
                    at_unix,
                });
            }
            held.events.clone()
        };
        match events.iter().rev().find(|event| event.is_execution_terminal()) {
            Some(ExecEvent::ExecutionCompleted { .. } | ExecEvent::ExecutionCancelled { .. }) => {
                self.real.forget_unrecorded(self.execution_id, replica).await?;
                Ok(Settled::Forgotten)
            }
            _ => {
                self.real.record_retroactively(&as_recorded(events), replica).await?;
                Ok(Settled::Recorded)
            }
        }
    }
}

/// An unrecorded run's rows as the record of a recorded run, cost rows in
/// their place: the birth now says so, and [`record_retroactively`] swaps
/// the cost rows already in the journal for these, never writing one
/// twice.
pub fn as_recorded(events: Vec<ExecEvent>) -> Vec<ExecEvent> {
    events
        .into_iter()
        .map(|mut event| {
            if let ExecEvent::ExecutionStarted { run_kind, .. } = &mut event {
                *run_kind = RunKind::Execution;
            }
            event
        })
        .collect()
}

#[async_trait]
impl JournalClient for UnrecordedJournal {
    async fn record_event(&self, event: &ExecEvent, replica: Option<&str>) -> anyhow::Result<()> {
        anyhow::ensure!(
            event.execution_id() == self.execution_id,
            "unrecorded run {} tried to write a row of run {}",
            self.execution_id,
            event.execution_id()
        );
        // Money is never held in memory only: a cost reaches the
        // journal as it happens, whatever becomes of the run.
        let refused = || {
            anyhow::anyhow!(
                "unrecorded run {} was already settled, so its {} row would be lost; everything a run writes \
                 is written before it is settled",
                self.execution_id,
                event.kind_str()
            )
        };
        anyhow::ensure!(!self.held.lock().expect("unrecorded journal").settled, refused());
        if matches!(event, ExecEvent::CostReported { .. }) {
            self.real.record_event(event, replica).await?;
        }
        let mut held = self.held.lock().expect("unrecorded journal");
        if held.settled {
            return Err(refused());
        }
        held.events.push(event.clone());
        drop(held);
        self.written.notify_waiters();
        Ok(())
    }

    async fn raw_rows_after(&self, execution_id: ExecutionId, after_id: i64, wait: Duration) -> anyhow::Result<Vec<RawJournalRow>> {
        // Another run's history (a seed's ancestors) is in the real
        // journal; only this run's own rows live here.
        if execution_id != self.execution_id {
            return self.real.raw_rows_after(execution_id, after_id, wait).await;
        }
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let written = self.written.notified();
            tokio::pin!(written);
            written.as_mut().enable();
            let rows = self.rows_after_now(after_id)?;
            if !rows.is_empty() || tokio::time::timeout_at(deadline, written).await.is_err() {
                return Ok(rows);
            }
        }
    }

    async fn has_terminal_event(&self, execution_id: ExecutionId) -> anyhow::Result<bool> {
        if execution_id != self.execution_id {
            return self.real.has_terminal_event(execution_id).await;
        }
        Ok(self.held.lock().expect("unrecorded journal").events.iter().any(ExecEvent::is_execution_terminal))
    }
}

/// Write a failed unrecorded run's record and make it a recorded run, in
/// one transaction, announcing its ending ([`UNRECORDED_ENDED_CHANNEL`])
/// at the commit. Refused when the execution is not an unrecorded run (a
/// second settle, or an execution that never was one).
///
/// The only rows of the run already in the journal are its costs, written
/// as they happened, so their ids are lower than the birth's would be:
/// a reader folding the record would meet a cost before the step it
/// belongs to and drop it. So they are taken out and the record is
/// written in order, each cost of `events` in its place carrying its
/// row's writer and dedup key (matched by `cost_id`); a cost that reached
/// the journal some other way (a provider meter's `record_cost` task,
/// which the run never saw) goes after the record, where a cost landing
/// late always goes. All under the run's execution lock, so no cost can land
/// between the take-out and the rewrite.
pub async fn record_retroactively(
    pool: &sqlx::PgPool,
    events: &[ExecEvent],
    replica: Option<&str>,
) -> anyhow::Result<()> {
    let Some(first) = events.first() else {
        anyhow::bail!("an unrecorded run's record has at least its birth");
    };
    let execution_id = first.execution_id();
    anyhow::ensure!(
        events.iter().all(|event| event.execution_id() == execution_id),
        "the record of unrecorded run {execution_id} carries rows of another run"
    );
    let mut tx = pool.begin().await?;
    crate::write::lock_execution_ids(&mut tx, &[execution_id]).await?;
    let row: Option<(uuid::Uuid, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT project_id, fired_by, instance_id FROM execution WHERE execution_id = $1 AND kind = $2 FOR UPDATE",
    )
    .bind(execution_id.to_string())
    .bind(RunKind::Unrecorded.as_str())
    .fetch_optional(&mut *tx)
    .await?;
    let Some((project_id, fired_by, instance)) = row else {
        anyhow::bail!("run {execution_id} is not an unrecorded run, so its record cannot be written afterwards");
    };
    let rows: Vec<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT payload_json, replica, dedup_key FROM exec_event WHERE execution_id = $1 ORDER BY id")
            .bind(execution_id.to_string())
            .fetch_all(&mut *tx)
            .await?;
    sqlx::query("DELETE FROM exec_event WHERE execution_id = $1").bind(execution_id.to_string()).execute(&mut *tx).await?;
    let mut held: Vec<Option<HeldCost>> = rows
        .into_iter()
        .map(|(payload, replica, dedup)| {
            let event = crate::decode_event(execution_id, &payload).map_err(|e| anyhow::anyhow!("{e}"))?;
            let ExecEvent::CostReported { cost_id, .. } = &event else {
                anyhow::bail!(
                    "unrecorded run {execution_id} already has a {} row in the journal; only its costs may be there before its record",
                    event.kind_str()
                );
            };
            Ok(Some(HeldCost { cost_id: cost_id.clone(), event, replica, dedup }))
        })
        .collect::<anyhow::Result<_>>()?;
    for event in events {
        let row = match event {
            ExecEvent::CostReported { cost_id: id, .. } => {
                held.iter_mut().find(|row| row.as_ref().is_some_and(|held| &held.cost_id == id)).and_then(Option::take)
            }
            _ => None,
        };
        let (writer, dedup) = match &row {
            Some(held) => (held.replica.as_deref(), held.dedup.as_deref()),
            None => (replica, None),
        };
        crate::write::record_event_in(&mut *tx, event, writer, dedup).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    for HeldCost { event, replica, dedup, .. } in held.into_iter().flatten() {
        crate::write::record_event_in(&mut *tx, &event, replica.as_deref(), dedup.as_deref())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    sqlx::query("UPDATE execution SET kind = $2 WHERE execution_id = $1")
        .bind(execution_id.to_string())
        .bind(RunKind::Execution.as_str())
        .execute(&mut *tx)
        .await?;
    announce_ended(&mut tx, UnrecordedEnded { execution_id, project_id, fired_by, instance }).await?;
    tx.commit().await?;
    Ok(())
}

/// A cost row of an unrecorded run found in the journal before its
/// record: the row's writer and dedup key are kept when it is rewritten.
struct HeldCost {
    cost_id: String,
    event: ExecEvent,
    replica: Option<String>,
    dedup: Option<String>,
}

/// Announce an unrecorded run's ending on [`UNRECORDED_ENDED_CHANNEL`],
/// delivered when the caller's transaction commits.
async fn announce_ended(tx: &mut sqlx::PgConnection, ended: UnrecordedEnded) -> anyhow::Result<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(UNRECORDED_ENDED_CHANNEL)
        .bind(serde_json::to_string(&ended)?)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

/// End an unrecorded run on the caller's transaction: drop its execution row
/// when no row of it is in the journal, or stamp it ended when its costs
/// keep it (they are addressed by execution; it never lists, only recorded
/// runs do), and announce the ending on [`UNRECORDED_ENDED_CHANNEL`],
/// delivered when the transaction commits. True when the row went.
pub async fn forget_in(tx: &mut sqlx::PgConnection, execution_id: ExecutionId) -> anyhow::Result<bool> {
    crate::write::lock_execution_ids(&mut *tx, &[execution_id]).await?;
    let owner: Option<(uuid::Uuid, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT project_id, fired_by, instance_id FROM execution WHERE execution_id = $1 AND kind = $2",
    )
    .bind(execution_id.to_string())
    .bind(RunKind::Unrecorded.as_str())
    .fetch_optional(&mut *tx)
    .await?;
    let Some((project_id, fired_by, instance)) = owner else {
        // Already forgotten (a cancel with no process, then the worker's own
        // settle, or the reverse): nothing ends twice.
        return Ok(false);
    };
    // A task still waiting would start the run after its row is gone.
    sqlx::query("DELETE FROM task WHERE execution_id = $1 AND kind = 'execute' AND status = 'pending'")
        .bind(execution_id.to_string())
        .execute(&mut *tx)
        .await?;
    let gone = sqlx::query(
        "DELETE FROM execution WHERE execution_id = $1 AND kind = $2 \
         AND NOT EXISTS (SELECT 1 FROM exec_event WHERE execution_id = $1)",
    )
    .bind(execution_id.to_string())
    .bind(RunKind::Unrecorded.as_str())
    .execute(&mut *tx)
    .await?;
    let gone = gone.rows_affected() > 0;
    if !gone {
        sqlx::query("UPDATE execution SET ended_at_unix = $2 WHERE execution_id = $1 AND ended_at_unix IS NULL")
            .bind(execution_id.to_string())
            .bind(chrono::Utc::now().timestamp())
            .execute(&mut *tx)
            .await?;
    }
    announce_ended(tx, UnrecordedEnded { execution_id, project_id, fired_by, instance }).await?;
    Ok(gone)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real journal as the settle sees it: what went through as it
    /// happened, what was recorded afterwards, and whether it was forgotten.
    #[derive(Default)]
    struct Real {
        written: Mutex<Vec<ExecEvent>>,
        recorded: Mutex<Option<Vec<ExecEvent>>>,
        forgotten: Mutex<bool>,
    }
    #[async_trait]
    impl JournalClient for Real {
        async fn record_event(&self, event: &ExecEvent, _: Option<&str>) -> anyhow::Result<()> {
            self.written.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn raw_rows_after(&self, _: ExecutionId, _: i64, _: Duration) -> anyhow::Result<Vec<RawJournalRow>> {
            Ok(Vec::new())
        }
        async fn has_terminal_event(&self, _: ExecutionId) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn record_retroactively(&self, events: &[ExecEvent], _: Option<&str>) -> anyhow::Result<()> {
            *self.recorded.lock().unwrap() = Some(events.to_vec());
            Ok(())
        }
        async fn forget_unrecorded(&self, _: ExecutionId, _: Option<&str>) -> anyhow::Result<()> {
            *self.forgotten.lock().unwrap() = true;
            Ok(())
        }
    }

    fn birth(execution_id: ExecutionId) -> Vec<ExecEvent> {
        vec![
            ExecEvent::ExecutionStarted {
                execution_id,
                project_id: uuid::Uuid::nil(),
                entry_node: "route".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some("h".into()),
                program: None,
                source_version: None,
                run_kind: RunKind::Unrecorded,
                subgraph: None,
                seed: None,
                instance: None,
                instance_values: Default::default(), picks: Default::default(),
                fired_trigger: Some("route".into()),
                run_class: weft_core::run_class::RunClass::Short,
                at_unix: 1,
            },
            ExecEvent::NodeKicked { execution_id, node_id: "route".into(), frames: vec![], firing: true, payload: None, port_snapshot: None, at_unix: 1 },
        ]
    }

    fn cost(execution_id: ExecutionId) -> ExecEvent {
        ExecEvent::CostReported {
            execution_id,
            node_id: "llm".into(),
            frames: vec![],
            cost_id: "c".into(),
            service: "llm".into(),
            model: None,
            amount_usd: Some(0.1),
            billed: true,
            origin: weft_core::CredentialOwner::Author,
            metadata: serde_json::json!({}),
            at_unix: 2,
        }
    }

    #[tokio::test]
    async fn reads_serve_the_birth_and_what_the_run_wrote() {
        let execution_id = ExecutionId::new_v4();
        let real = Arc::new(Real::default());
        let journal = UnrecordedJournal::seeded(execution_id, birth(execution_id), real.clone()).unwrap();
        let rows = journal.rows_after(execution_id, 0, Duration::ZERO).await.unwrap();
        assert_eq!(rows.len(), 2);
        journal.record_event(&ExecEvent::NodeCompleted { execution_id, node_id: "route".into(), frames: vec![], at_unix: 2 }, None).await.unwrap();
        let after = journal.rows_after(execution_id, 2, Duration::ZERO).await.unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].id, 3);
        assert!(!journal.has_terminal_event(execution_id).await.unwrap());
        assert!(real.written.lock().unwrap().is_empty(), "nothing but costs goes through while it runs");
    }

    #[tokio::test]
    async fn a_held_read_wakes_on_the_next_write() {
        let execution_id = ExecutionId::new_v4();
        let journal = UnrecordedJournal::seeded(execution_id, birth(execution_id), Arc::new(Real::default())).unwrap();
        let reader = {
            let journal = journal.clone();
            tokio::spawn(async move { journal.rows_after(execution_id, 2, Duration::from_secs(30)).await })
        };
        tokio::task::yield_now().await;
        journal.record_event(&ExecEvent::ExecutionCompleted { execution_id, at_unix: 3 }, None).await.unwrap();
        assert_eq!(reader.await.unwrap().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_cost_goes_through_as_it_happens() {
        let execution_id = ExecutionId::new_v4();
        let real = Arc::new(Real::default());
        let journal = UnrecordedJournal::seeded(execution_id, birth(execution_id), real.clone()).unwrap();
        journal.record_event(&cost(execution_id), None).await.unwrap();
        assert_eq!(real.written.lock().unwrap().len(), 1);
        assert_eq!(journal.events().len(), 3, "and the run still reads it");
    }

    #[tokio::test]
    async fn a_completed_run_is_forgotten() {
        let execution_id = ExecutionId::new_v4();
        let real = Arc::new(Real::default());
        let journal = UnrecordedJournal::seeded(execution_id, birth(execution_id), real.clone()).unwrap();
        journal.record_event(&ExecEvent::ExecutionCompleted { execution_id, at_unix: 3 }, None).await.unwrap();
        assert_eq!(journal.settle(None).await.unwrap(), Settled::Forgotten);
        assert!(*real.forgotten.lock().unwrap());
        assert!(real.recorded.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_failed_run_is_recorded_whole_as_a_recorded_run() {
        let execution_id = ExecutionId::new_v4();
        let real = Arc::new(Real::default());
        let journal = UnrecordedJournal::seeded(execution_id, birth(execution_id), real.clone()).unwrap();
        journal.record_event(&cost(execution_id), None).await.unwrap();
        journal.record_event(&ExecEvent::ExecutionFailed { execution_id, error: "boom".into(), at_unix: 3 }, None).await.unwrap();
        assert_eq!(journal.settle(None).await.unwrap(), Settled::Recorded);
        let recorded = real.recorded.lock().unwrap().clone().unwrap();
        let kinds: Vec<&str> = recorded.iter().map(ExecEvent::kind_str).collect();
        assert_eq!(kinds, ["execution_started", "node_kicked", "cost_reported", "execution_failed"], "costs sit in their place");
        assert!(matches!(recorded[0], ExecEvent::ExecutionStarted { run_kind: RunKind::Execution, .. }));
        assert!(!*real.forgotten.lock().unwrap());
    }

    #[tokio::test]
    async fn a_run_with_no_ending_is_recorded_as_failed() {
        let execution_id = ExecutionId::new_v4();
        let real = Arc::new(Real::default());
        let journal = UnrecordedJournal::seeded(execution_id, birth(execution_id), real.clone()).unwrap();
        assert_eq!(journal.settle(None).await.unwrap(), Settled::Recorded);
        let recorded = real.recorded.lock().unwrap().clone().unwrap();
        assert!(matches!(recorded.last(), Some(ExecEvent::ExecutionFailed { .. })));
    }

    #[tokio::test]
    async fn a_write_after_the_settle_is_refused() {
        let execution_id = ExecutionId::new_v4();
        let journal = UnrecordedJournal::seeded(execution_id, birth(execution_id), Arc::new(Real::default())).unwrap();
        journal.record_event(&ExecEvent::ExecutionFailed { execution_id, error: "boom".into(), at_unix: 3 }, None).await.unwrap();
        journal.settle(None).await.unwrap();
        let late = ExecEvent::NodeCompleted { execution_id, node_id: "route".into(), frames: vec![], at_unix: 4 };
        let refused = journal.record_event(&late, None).await.unwrap_err().to_string();
        assert!(refused.contains("already settled"), "{refused}");
        assert!(journal.settle(None).await.is_err(), "a run is settled once");
    }

    #[test]
    fn a_birth_that_is_not_unrecorded_is_refused() {
        let execution_id = ExecutionId::new_v4();
        let mut rows = birth(execution_id);
        if let ExecEvent::ExecutionStarted { run_kind, .. } = &mut rows[0] {
            *run_kind = RunKind::Execution;
        }
        assert!(UnrecordedJournal::seeded(execution_id, rows, Arc::new(Real::default())).is_err());
        assert!(UnrecordedJournal::seeded(execution_id, birth(ExecutionId::new_v4()), Arc::new(Real::default())).is_err());
    }
}
