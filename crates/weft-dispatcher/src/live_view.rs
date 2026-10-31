//! A run's live view: its new record rows, painted for whoever on this
//! process follows its project (`crate::events::EventBus`).
//!
//! Every write of a run's record is announced on
//! `weft_journal::RUN_LOG_CHANNEL`, one project and the runs of it that got
//! rows. When nobody here follows that project, the announcement is
//! dropped after reading its first id. When somebody does, each of its runs
//! is read past the last row this process painted, a page at a time
//! (`crate::painting`), folded into the run's projection, and the events
//! each page paints are published to this process's subscribers as one
//! item. A run first seen here (it started before anybody watched, or this
//! process just started watching it) is read and painted from its birth:
//! a follower of one run skips what it holds already by each event's place
//! in the record (`journal:{seq}:{index}:{k}`), so painting a row twice is
//! harmless, and missing one is not.
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

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use weft_core::live_event::ShownEvent;
use weft_core::RunId;
use weft_task_store::pg_signal::Heard;

use crate::events::EventBus;
use crate::painting::{Painting, Sink};
use crate::projection::RunProjector;
use crate::state::DispatcherState;

/// A projection that has painted no row for this long is dropped: a run
/// parked on a form for days would otherwise hold its fold in this
/// process's RAM. Its next row rebuilds it from the record.
const IDLE_TTL: Duration = Duration::from_secs(10 * 60);

/// One run this process paints.
struct LiveRun {
    project_id: uuid::Uuid,
    projector: RunProjector,
    /// The last record row painted.
    painted: Option<i32>,
    last_row_at: Instant,
}

/// A run whose projection went idle and was dropped: what its rebuild needs
/// so its watchers hear nothing twice. The rows through `painted` were
/// published, with every corruption they paint, so a rebuild folds those
/// rows quietly and says nothing about them again (a screen counts every
/// corruption it hears).
struct IdleRun {
    project_id: uuid::Uuid,
    painted: Option<i32>,
}

/// Every run this process paints right now, and the ones it painted whose
/// projection went idle, for as long as their project is followed here.
#[derive(Default)]
struct Views {
    runs: HashMap<RunId, LiveRun>,
    idle: HashMap<RunId, IdleRun>,
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
    async fn logged(&mut self, state: &DispatcherState, project_id: uuid::Uuid, runs: &[RunId]) -> anyhow::Result<()> {
        if !state.events.watched(project_id).await {
            self.forget_project(project_id);
            return Ok(());
        }
        let woken = self.woken(&*state.runs, runs).await?;
        for run_id in runs {
            if !self.idle.contains_key(run_id) || woken.contains(run_id) {
                self.paint(state, *run_id, project_id).await?;
            }
        }
        Ok(())
    }

    /// Read every run painted here past its last painted row: the live
    /// ones each, and the idle ones only once one read over them all
    /// found rows past what was painted.
    async fn recheck(&mut self, state: &DispatcherState) -> anyhow::Result<()> {
        let projects: HashSet<uuid::Uuid> =
            self.runs.values().map(|live| live.project_id).chain(self.idle.values().map(|idle| idle.project_id)).collect();
        for project_id in projects {
            if !state.events.watched(project_id).await {
                self.forget_project(project_id);
            }
        }
        let idle: Vec<RunId> = self.idle.keys().copied().collect();
        let woken = self.woken(&*state.runs, &idle).await?;
        let open: Vec<(RunId, uuid::Uuid)> = self
            .runs
            .iter()
            .map(|(id, live)| (*id, live.project_id))
            .chain(self.idle.iter().filter(|(id, _)| woken.contains(id)).map(|(id, idle)| (*id, idle.project_id)))
            .collect();
        for (run_id, project_id) in open {
            self.paint(state, run_id, project_id).await?;
        }
        Ok(())
    }

    /// Which of `runs` that are idle here got rows past what was painted,
    /// in one read however many are idle: an idle run is rebuilt only
    /// then.
    async fn woken(&self, store: &dyn crate::store::RunStore, runs: &[RunId]) -> anyhow::Result<HashSet<RunId>> {
        let past: Vec<(RunId, Option<i32>)> = runs.iter().filter_map(|id| self.idle.get(id).map(|idle| (*id, idle.painted))).collect();
        if past.is_empty() {
            return Ok(HashSet::new());
        }
        Ok(store.written_past(&past).await?.into_iter().collect())
    }

    /// Nobody here follows `project_id` any more: nothing of its runs is
    /// kept. A watcher that comes later hears each run from its birth.
    fn forget_project(&mut self, project_id: uuid::Uuid) {
        self.runs.retain(|_, live| live.project_id != project_id);
        self.idle.retain(|_, idle| idle.project_id != project_id);
    }

    /// Paint the rows of `run_id` past the last one painted, opening
    /// its projection from its birth when it has none here, and rebuilding
    /// it when it went idle (asked only once rows landed past what was
    /// painted, [`Self::woken`]). A read that fails (the database, for
    /// now) leaves the run as idle as of what was published, so the next
    /// try says nothing twice.
    async fn paint(&mut self, state: &DispatcherState, run_id: RunId, project_id: uuid::Uuid) -> anyhow::Result<()> {
        let (painting, last_row_at) = match (self.runs.remove(&run_id), self.idle.remove(&run_id)) {
            (Some(live), _) => (Painting { painted: live.painted, ..Painting::new(live.projector) }, live.last_row_at),
            (None, Some(idle)) => {
                // Its rows through `painted` were published, with whatever
                // they paint of why the run cannot be painted whole.
                let projector = match crate::painting::open(state, run_id, project_id).await {
                    Ok(projector) => projector,
                    Err(e) => {
                        self.idle.insert(run_id, idle);
                        return Err(e);
                    }
                };
                (Painting { shown_through: idle.painted, ..Painting::new(projector) }, Instant::now())
            }
            (None, None) => (Painting::new(crate::painting::open(state, run_id, project_id).await?), Instant::now()),
        };
        let before = painting.painted;
        let painted = crate::painting::paint(&*state.runs, run_id, painting, Published { events: state.events.clone(), project_id }).await;
        let (painting, _) = match painted {
            Ok(painted) => painted,
            Err(cut) => {
                self.idle.insert(run_id, IdleRun { project_id, painted: cut.delivered });
                return Err(cut.into());
            }
        };
        // A run that ended stays like any other until it goes idle: its
        // record can still get rows (a meter settling a call it measured),
        // and painting them goes on from here instead of from its birth.
        self.runs.insert(run_id, LiveRun {
            project_id,
            projector: painting.projector,
            painted: painting.painted,
            last_row_at: if painting.painted != before { Instant::now() } else { last_row_at },
        });
        Ok(())
    }

    /// Drop the projections that painted nothing for a while, every run
    /// alike, keeping what a rebuild needs to say nothing twice
    /// ([`IdleRun`]).
    fn forget_idle(&mut self, now: Instant) {
        let idle: Vec<RunId> = self.runs.iter().filter(|(_, live)| now.duration_since(live.last_row_at) >= IDLE_TTL).map(|(id, _)| *id).collect();
        for run_id in idle {
            if let Some(live) = self.runs.remove(&run_id) {
                self.idle.insert(run_id, IdleRun { project_id: live.project_id, painted: live.painted });
            }
        }
    }
}

/// Where the live view's paints go: this process's subscribers of the
/// run's project, one item per page painted.
struct Published {
    events: EventBus,
    project_id: uuid::Uuid,
}

#[async_trait::async_trait]
impl Sink for Published {
    type Page = Vec<ShownEvent>;

    fn page(&mut self, events: Vec<ShownEvent>) -> anyhow::Result<Vec<ShownEvent>> {
        Ok(events)
    }

    async fn deliver(&mut self, events: Vec<ShownEvent>) -> anyhow::Result<()> {
        self.events.publish_batch(self.project_id, events).await;
        Ok(())
    }
}

/// Something about a run that no row of its record paints (the bake its
/// trigger setup landed on): logged, and published as the same corruption
/// event its record's rows paint, so the screen shows it on the run.
/// `reason` names what did not read and what to do about it.
pub(crate) async fn publish_unreadable(
    state: &DispatcherState,
    run_id: RunId,
    project_id: uuid::Uuid,
    site: weft_core::primitive::CorruptionSite,
    reason: String,
) {
    tracing::error!(
        target: "weft_dispatcher::live_view",
        %run_id, %reason,
        "something this run's view reads cannot be read back"
    );
    let corruption = crate::events::IdentifiedEvent::transient(weft_core::live_event::DispatcherEvent::JournalCorruption {
        run_id,
        project_id,
        site,
        reason,
    });
    state.events.publish_local(corruption).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_journal::RunEvent;

    const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x100);

    /// Run `n`'s record: its birth and `tags` tags, one row each.
    async fn run_with_rows(store: &crate::store::FakeStore, n: u128, tags: u64) -> RunId {
        let run_id = uuid::Uuid::from_u128(n);
        let birth = RunEvent::RunStarted {
            given: Default::default(), run_id, project_id: PROJECT, entry_node: "entry".into(),
            phase: weft_core::context::Phase::Fire, definition_hash: None, binary_hash: None, source_version: None,
            run_kind: weft_core::run::RunKind::Project, cut_from: None, seed: None, instance: None, stand_in: None,
            fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0, settings: Default::default(),
        };
        store.record_event(&birth).await.unwrap();
        for tag in 1..=tags {
            store.record_event(&RunEvent::RunTagged { run_id, tags: vec![tag.to_string()], at_unix: tag }).await.unwrap();
        }
        run_id
    }

    /// Of the runs asked about, only the idle ones whose record got a row
    /// past what was painted wake: a live run is not asked about, and an
    /// idle one painted through its last row stays idle.
    #[tokio::test]
    async fn only_idle_runs_with_rows_past_what_was_painted_wake() {
        let store = crate::store::FakeStore::new();
        store.set_project_tenant(PROJECT, "t");
        let (caught_up, behind, never_painted, live) =
            (run_with_rows(&store, 1, 2).await, run_with_rows(&store, 2, 2).await, run_with_rows(&store, 3, 0).await, run_with_rows(&store, 4, 2).await);
        let mut views = Views::default();
        views.idle.insert(caught_up, IdleRun { project_id: PROJECT, painted: Some(2) });
        views.idle.insert(behind, IdleRun { project_id: PROJECT, painted: Some(1) });
        views.idle.insert(never_painted, IdleRun { project_id: PROJECT, painted: None });
        let woken = views.woken(&store, &[caught_up, behind, never_painted, live]).await.unwrap();
        assert_eq!(woken, HashSet::from([behind, never_painted]));
        assert!(views.woken(&store, &[live]).await.unwrap().is_empty(), "a run that is not idle here is never asked about");
    }
}
