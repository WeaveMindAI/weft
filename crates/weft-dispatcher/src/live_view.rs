//! A run's live view: its new record rows, painted for whoever on this
//! process follows its project (`crate::events::EventBus`).
//!
//! Every write of a run's record is announced on
//! `weft_journal::RUN_LOG_CHANNEL`, one project and the runs of it that got
//! rows. When nobody here follows that project, the announcement is
//! dropped after reading its first id. When somebody does, each of its runs
//! is read past the last row this process painted, folded into the run's
//! projection (`crate::projection::ExecutionProjector`), and the events it
//! paints are published to this process's subscribers. A run first seen
//! here (it started before anybody watched, or this process just started
//! watching it) is read and painted from its birth: the screen dedups by
//! event id (`journal:{seq}:{index}`), so painting a row twice is harmless,
//! and missing one is not.
//!
//! A relay of this process's own subscribers, living while the process
//! does: every dispatcher process reads for its own watchers, so nothing is
//! sent across processes. A lost announcement leaves the screen behind
//! until the run's next row, or its replay; the record itself is whole.
//!
//! Why a projection and not the record's events as they are: the screen's
//! wire shape is `DispatcherEvent`, and the rows no longer carry what the
//! screen shows (a firing's input, its output, a group boundary running).
//! The record is the durable shape; the events are derived by folding the
//! rows over the program, the same way the replay does.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use weft_core::ExecutionId;
use weft_journal::ExecEvent;
use weft_task_store::pg_signal::Heard;

use crate::projection::{execution_program, ExecutionProjector, ProgramLookup};
use crate::state::DispatcherState;

/// A projection that has painted no row for this long is dropped: a run
/// parked on a form for days would otherwise hold its whole fold in this
/// process's RAM. Its next row rebuilds it from the record.
const IDLE_TTL: Duration = Duration::from_secs(10 * 60);

/// One run this process paints.
struct LiveRun {
    project_id: uuid::Uuid,
    projector: ExecutionProjector,
    /// The last record row painted.
    painted: i32,
    last_row_at: Instant,
}

/// Every run this process paints right now.
#[derive(Default)]
struct Views {
    runs: HashMap<ExecutionId, LiveRun>,
}

/// Paint the runs of the projects followed on this process, for the
/// process's whole life.
pub async fn run(state: DispatcherState) {
    let mut heard = state.signals.subscribe();
    let mut views = Views::default();
    loop {
        let step = match heard.next().await {
            Ok(Heard::Signal { channel, payload }) if channel == weft_journal::RUN_LOG_CHANNEL => {
                match weft_journal::run_log_of_payload(&payload) {
                    Some((project_id, runs)) => views.logged(&state, project_id, &runs).await,
                    None => {
                        tracing::warn!(target: "weft_dispatcher::live_view", %payload, "an announcement of new record rows names no project");
                        Ok(())
                    }
                }
            }
            // Announcements may have been lost: every run painted here is
            // read again past its last painted row.
            Ok(Heard::Recheck | Heard::Resumed) => views.recheck(&state).await,
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!(target: "weft_dispatcher::live_view", error = %e, "the live view stopped hearing new record rows");
                return;
            }
        };
        if let Err(e) = step {
            // The screen falls behind until the run's next row; the record
            // is whole and the replay reads it.
            tracing::warn!(target: "weft_dispatcher::live_view", error = %format!("{e:#}"), "could not paint new record rows");
        }
        views.forget_idle(Instant::now());
    }
}

impl Views {
    /// `runs` of `project_id` got rows.
    async fn logged(&mut self, state: &DispatcherState, project_id: uuid::Uuid, runs: &[ExecutionId]) -> anyhow::Result<()> {
        if !state.events.watched(project_id).await {
            self.runs.retain(|_, live| live.project_id != project_id);
            return Ok(());
        }
        for execution_id in runs {
            self.paint(state, *execution_id, project_id).await?;
        }
        Ok(())
    }

    /// Read every run painted here past its last painted row.
    async fn recheck(&mut self, state: &DispatcherState) -> anyhow::Result<()> {
        let open: Vec<(ExecutionId, uuid::Uuid)> = self.runs.iter().map(|(id, live)| (*id, live.project_id)).collect();
        for (execution_id, project_id) in open {
            if state.events.watched(project_id).await {
                self.paint(state, execution_id, project_id).await?;
            } else {
                self.runs.remove(&execution_id);
            }
        }
        Ok(())
    }

    /// Paint the rows of `execution_id` past the last one painted, opening
    /// its projection from its birth when it has none here.
    async fn paint(&mut self, state: &DispatcherState, execution_id: ExecutionId, project_id: uuid::Uuid) -> anyhow::Result<()> {
        let after = self.runs.get(&execution_id).map(|live| live.painted);
        let record = {
            let mut conn = state.pg_pool.acquire().await?;
            weft_journal::record::read_record(&mut conn, execution_id, after).await?
        };
        let Some(last) = record.rows.last().map(|row| row.seq) else { return Ok(()) };
        let rows = match record.decode(execution_id) {
            Ok(rows) => rows,
            Err(reason) => {
                // The run paints only what needs no program from here on;
                // its corruption is said once, as its projection opens.
                publish_unreadable(
                    state,
                    execution_id,
                    project_id,
                    weft_core::primitive::CorruptionSite::UndecodableRow,
                    format!("a row of this execution no longer decodes: {reason}"),
                )
                .await;
                self.runs.insert(
                    execution_id,
                    LiveRun {
                        project_id,
                        projector: ExecutionProjector::new(execution_id, ProgramLookup::Unreadable(String::new()), project_id),
                        painted: last,
                        last_row_at: Instant::now(),
                    },
                );
                return Ok(());
            }
        };
        let mut live = match self.runs.remove(&execution_id) {
            Some(live) => live,
            None => LiveRun { project_id, projector: open_projection(state, execution_id, project_id, &rows).await?, painted: -1, last_row_at: Instant::now() },
        };
        let mut ended = false;
        for row in &rows {
            ended |= row.event.is_execution_terminal();
            for event in crate::events::IdentifiedEvent::recorded(row.seq, row.index, ()).project(|()| live.projector.project(&row.event)) {
                state.events.publish_local(event).await;
            }
        }
        live.painted = last;
        live.last_row_at = Instant::now();
        // An ended run gets no more rows.
        if !ended {
            self.runs.insert(execution_id, live);
        }
        Ok(())
    }

    /// Drop the projections that painted nothing for a while. One that
    /// does not fold at all holds no RAM worth freeing, and reopening it
    /// would republish its corruption, so it stays until its ending.
    fn forget_idle(&mut self, now: Instant) {
        self.runs.retain(|_, live| !live.projector.folds() || now.duration_since(live.last_row_at) < IDLE_TTL);
    }
}

/// Open the projection of `execution_id`, whose record starts with `rows`.
/// Only the database itself is an error. Anything permanent about the run
/// (its program cannot be found, its seed cannot be read) degrades its
/// projection to the program-free painting and publishes why.
async fn open_projection(
    state: &DispatcherState,
    execution_id: ExecutionId,
    project_id: uuid::Uuid,
    rows: &[weft_journal::JournalRow],
) -> anyhow::Result<ExecutionProjector> {
    // Whatever stops this run from painting whole, say it once, here. The
    // projector then carries a reason-less copy of the same state, so it
    // does not say it again on every later row.
    let found = execution_program(state, execution_id).await?;
    let program = match found.unpaintable() {
        Some((site, reason)) => {
            publish_unreadable(state, execution_id, project_id, site, reason.to_string()).await;
            found.without_reason()
        }
        None => found,
    };
    let birth: Vec<ExecEvent> = rows.iter().take(1).map(|row| row.event.clone()).collect();
    let inheritance = match crate::projection::execution_inheritance(state, &birth, &program).await {
        Ok(chain) => chain,
        Err(reason) => {
            // A cleaned seed leaves the run's own rows paintable and its
            // inherited nodes empty on the screen.
            publish_unreadable(state, execution_id, project_id, weft_core::primitive::CorruptionSite::UndecodableRow, reason).await;
            weft_journal::SeedChain::default()
        }
    };
    Ok(ExecutionProjector::new(execution_id, program, project_id).with_inheritance(inheritance))
}

/// A run whose record cannot be read back: logged, and published as the
/// same corruption event the replay publishes, so the screen shows the run
/// as unreadable and names `weft clean`.
pub(crate) async fn publish_unreadable(
    state: &DispatcherState,
    execution_id: ExecutionId,
    project_id: uuid::Uuid,
    site: weft_core::primitive::CorruptionSite,
    reason: String,
) {
    tracing::error!(
        target: "weft_dispatcher::live_view",
        %execution_id, %reason,
        "this execution's record cannot be read back; its live view paints only what needs no \
         program. `weft clean` removes the execution."
    );
    let corruption = crate::events::IdentifiedEvent::transient(crate::events::DispatcherEvent::JournalCorruption {
        execution_id,
        project_id,
        site,
        reason,
    });
    state.events.publish_local(corruption).await;
}
