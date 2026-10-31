//! A run's record painted: its rows read a page at a time, each folded
//! into the run's projection (`crate::projection::RunProjector`) and the
//! events it paints handed on as they come. The one reader behind every
//! way a run's history is shown: the replay, a run's events, a follower
//! catching up, the live view opening a run. None of them holds more of a
//! run than one page of its record and what its projection keeps open; a
//! seeded run's ancestors are read the same way before it
//! (`crate::projection::read_inheritance`).

use weft_core::live_event::{IdentifiedEvent, ShownEvent};
use weft_core::primitive::CorruptionSite;
use weft_core::RunId;

use crate::projection::{read_inheritance, record_page, run_birth, run_inheritance, run_program, Inherited, RunProjector};
use crate::state::DispatcherState;
use crate::store::RunStore;

/// Open the projection of run `run_id` of project `project_id`, from its
/// birth. Only the database itself is an error. Anything permanent about
/// the run (its program cannot be found, its seed chain cannot be read)
/// degrades its projection, which says why with the first row it paints,
/// at that row's place, so a reader that holds that row already hears it
/// no more than any other event of the row.
pub async fn open(state: &DispatcherState, run_id: RunId, project_id: uuid::Uuid) -> anyhow::Result<RunProjector> {
    let program = run_program(state, run_id).await?;
    // What the run inherits is named by its birth, its first row. A row
    // that does not decode is said by the read that meets it.
    let birth = run_birth(state, run_id).await?.unwrap_or(None);
    let seed = birth.as_ref().and_then(|birth| weft_journal::seed::seed_of(&birth.event));
    let inherited = match (seed, program.program()) {
        (Some(seed), Some(child_program)) => match run_inheritance(state, birth.as_ref().map(|birth| &birth.event), &program).await {
            // What its ancestors hand down, read before its own record.
            // Reading them can fail on a database that answered a moment
            // ago as well as on a broken ancestor: either is said where
            // the run's birth paints.
            Ok(chain) => match read_inheritance(&record_page(state), &chain, seed, child_program, run_id, project_id).await {
                Ok(inheritance) => Inherited::Read(inheritance),
                Err(e) => Inherited::Failed(format!("{e:#}")),
            },
            // A cleaned seed leaves the run's own rows paintable and its
            // inherited nodes empty on the screen; the birth says why.
            Err(reason) => Inherited::ChainUnreadable(reason),
        },
        _ => Inherited::Nothing,
    };
    Ok(RunProjector::new(run_id, program, project_id).with_inheritance(inherited))
}

/// Where the events a painting paints go, a page at a time, in two steps:
/// [`Sink::page`] makes what a page's events become (encoded, spelled) on
/// the blocking pool beside the fold, and [`Sink::deliver`] hands it on
/// from the runtime, so a reader slow to take it holds a task, never a
/// blocking thread. An error from either ends the read with it.
#[async_trait::async_trait]
pub trait Sink: Send + 'static {
    type Page: Send + 'static;

    fn page(&mut self, events: Vec<ShownEvent>) -> anyhow::Result<Self::Page>;

    async fn deliver(&mut self, page: Self::Page) -> anyhow::Result<()>;
}

/// Where a painting of a run's record stands as it reads it: its
/// projection, and the last row it painted.
pub struct Painting {
    pub projector: RunProjector,
    /// The last record row painted: the next read starts after it.
    pub painted: Option<i32>,
    /// The rows the reader showed already, from an earlier projection of
    /// the same run: folded to rebuild the projection, and nothing of them
    /// handed on again.
    pub shown_through: Option<i32>,
}

impl Painting {
    /// A painting of a run's record from its birth.
    pub fn new(projector: RunProjector) -> Self {
        Self { projector, painted: None, shown_through: None }
    }

    /// Fold `page`, the run's next, and answer the events it paints, in
    /// the record's order: a row that no longer decodes paints why, at its
    /// own place, the one event it has. Rows at or before
    /// [`Self::shown_through`] are folded and paint nothing.
    fn page(&mut self, run_id: RunId, mut page: weft_journal::record::RawRecord) -> Vec<ShownEvent> {
        let last = page.rows.last().map(|row| row.seq);
        let shown = page.rows.partition_point(|row| self.shown_through.is_some_and(|through| row.seq <= through));
        let unshown = weft_journal::record::RawRecord { rows: page.rows.split_off(shown) };
        let (quiet, _) = page.decode_lossy(run_id);
        for row in &quiet {
            self.projector.project(row);
        }
        let (rows, bad) = unshown.decode_lossy(run_id);
        let project_id = self.projector.project_id();
        let undecodable = |(seq, reason): (i32, String)| {
            tracing::error!(target: "weft_dispatcher::painting", %run_id, seq, %reason, "a row of this run's record no longer decodes");
            let corruption = weft_core::live_event::DispatcherEvent::JournalCorruption { run_id, project_id, site: CorruptionSite::UndecodableRow, reason };
            IdentifiedEvent::projected(weft_core::record_ref::Position { seq, index: 0 }, [corruption])
        };
        let mut bad = bad.into_iter().peekable();
        let mut events = Vec::new();
        for row in &rows {
            while let Some(before) = bad.next_if(|(seq, _)| *seq < row.seq) {
                events.extend(undecodable(before));
            }
            events.extend(IdentifiedEvent::projected(row.position(), self.projector.project(row)));
        }
        events.extend(bad.flat_map(undecodable));
        self.painted = last.or(self.painted);
        events
    }
}

/// Paint run `run_id`'s record past `painting`'s last painted row (from
/// its birth when none) into `sink`, a page at a time: each page read,
/// folded and made into what `sink` hands on, on the blocking pool, then
/// handed on before the next page is read. Answers both, the record read.
pub async fn paint<S: Sink>(runs: &dyn RunStore, run_id: RunId, mut painting: Painting, mut sink: S) -> Result<(Painting, S), Interrupted> {
    // The rows shown already count as handed on: a rebuild cut short
    // before it passes them carries on after them all the same.
    let mut delivered = painting.painted.max(painting.shown_through);
    loop {
        let cut = |error: anyhow::Error| Interrupted { error, delivered };
        let page = match runs.record_page(run_id, painting.painted).await {
            Ok(page) => page,
            Err(e) => return Err(cut(e)),
        };
        let Some(last) = page.rows.last().map(|row| row.seq) else { return Ok((painting, sink)) };
        let paged = weft_journal::on_blocking_pool(move || -> anyhow::Result<_> {
            let events = painting.page(run_id, page);
            let out = sink.page(events)?;
            Ok((painting, sink, out))
        })
        .await;
        let out;
        (painting, sink, out) = match paged {
            Ok(paged) => paged,
            Err(e) => return Err(cut(e)),
        };
        if let Err(e) = sink.deliver(out).await {
            return Err(cut(e));
        }
        delivered = delivered.max(Some(last));
    }
}

/// A painting cut short ([`paint`]): why, and the last record row whose
/// events reached the sink, which a reader that carries on after it starts
/// from so it hands nothing on twice.
#[derive(Debug, thiserror::Error)]
#[error("{error:#}")]
pub struct Interrupted {
    pub error: anyhow::Error,
    pub delivered: Option<i32>,
}

/// The event about where run `run_id` stands now that a reader of its
/// history says after its rows: `RunParked` when it is parked (parking
/// writes no record row, and a reader that takes the history before the
/// live events, `weft wait --waiting-for-input`, learns here that it
/// waits).
pub async fn parked_now(state: &DispatcherState, run_id: RunId, project_id: uuid::Uuid) -> anyhow::Result<Option<ShownEvent>> {
    use weft_core::live_event::DispatcherEvent;
    let summary = state.runs.run_summary(run_id).await?;
    Ok(summary
        .is_some_and(|summary| summary.status == weft_core::program::RunStatus::WaitingForInput)
        .then(|| IdentifiedEvent::transient(DispatcherEvent::RunParked { run_id, project_id })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::ProgramLookup;
    use weft_journal::RunEvent;

    const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x100);

    /// The events a sink was handed, page by page, by their row.
    #[derive(Default)]
    struct Pages(Vec<Vec<i32>>);

    #[async_trait::async_trait]
    impl Sink for Pages {
        type Page = Vec<i32>;

        fn page(&mut self, events: Vec<ShownEvent>) -> anyhow::Result<Vec<i32>> {
            Ok(events.iter().map(|event| event.position().expect("a recorded event").at.seq).collect())
        }

        async fn deliver(&mut self, page: Vec<i32>) -> anyhow::Result<()> {
            self.0.push(page);
            Ok(())
        }
    }

    /// A run's record of a birth and four tags, one row each.
    async fn tagged_run() -> (crate::store::FakeStore, RunId) {
        let store = crate::store::FakeStore::new();
        store.set_project_tenant(PROJECT, "t");
        let run_id = uuid::Uuid::from_u128(7);
        let birth = RunEvent::RunStarted {
            given: Default::default(), run_id, project_id: PROJECT, entry_node: "entry".into(),
            phase: weft_core::context::Phase::Fire, definition_hash: None, binary_hash: None, source_version: None,
            run_kind: weft_core::run::RunKind::Project, cut_from: None, seed: None, instance: None, stand_in: None,
            fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0, settings: Default::default(),
        };
        store.record_event(&birth).await.unwrap();
        for tag in 1..5 {
            store.record_event(&RunEvent::RunTagged { run_id, tags: vec![tag.to_string()], at_unix: tag }).await.unwrap();
        }
        (store, run_id)
    }

    /// A painting goes on past the last row it painted, a page at a time,
    /// each page handed on before the next is read; a painting rebuilt for a
    /// reader that showed some rows already folds them and hands on only
    /// what follows.
    #[tokio::test]
    async fn a_painting_hands_on_only_the_rows_past_what_it_painted_or_showed() {
        let (store, run_id) = tagged_run().await;
        let projector = || RunProjector::new(run_id, ProgramLookup::NoProgram, PROJECT);
        let (painting, sink) = paint(&store, run_id, Painting::new(projector()), Pages::default()).await.unwrap();
        assert_eq!(sink.0, [vec![0, 1], vec![2, 3], vec![4]], "every row once, a page at a time");
        assert_eq!(painting.painted, Some(4));
        store.record_event(&RunEvent::RunTagged { run_id, tags: vec!["5".into()], at_unix: 5 }).await.unwrap();
        let (painting, sink) = paint(&store, run_id, painting, Pages::default()).await.unwrap();
        assert_eq!(sink.0, [vec![5]], "only the row that landed since");
        assert_eq!(painting.painted, Some(5));
        let rebuilt = Painting { shown_through: Some(2), ..Painting::new(projector()) };
        let (painting, sink) = paint(&store, run_id, rebuilt, Pages::default()).await.unwrap();
        assert_eq!(sink.0.concat(), [3, 4, 5], "the rows shown already are folded, not handed on");
        assert_eq!(painting.painted, Some(5));
    }

    /// A sink that takes `takes` pages, then refuses the next.
    struct Refuses {
        takes: usize,
    }

    #[async_trait::async_trait]
    impl Sink for Refuses {
        type Page = ();

        fn page(&mut self, _: Vec<ShownEvent>) -> anyhow::Result<()> {
            Ok(())
        }

        async fn deliver(&mut self, (): ()) -> anyhow::Result<()> {
            anyhow::ensure!(self.takes > 0, "the reader went away");
            self.takes -= 1;
            Ok(())
        }
    }

    /// A painting cut short names the last row whose events reached the
    /// sink, and a rebuilt one counts the rows shown already as reached, so
    /// the next try hands on nothing twice.
    #[tokio::test]
    async fn a_cut_painting_names_the_last_row_it_handed_on() {
        let (store, run_id) = tagged_run().await;
        let projector = || RunProjector::new(run_id, ProgramLookup::NoProgram, PROJECT);
        let cut = paint(&store, run_id, Painting::new(projector()), Refuses { takes: 1 }).await.err().expect("cut");
        assert_eq!(cut.delivered, Some(1), "the first page reached the sink");
        let rebuilt = Painting { shown_through: Some(3), ..Painting::new(projector()) };
        let cut = paint(&store, run_id, rebuilt, Refuses { takes: 0 }).await.err().expect("cut");
        assert_eq!(cut.delivered, Some(3), "cut before it passed the rows shown already");
        let rebuilt = Painting { shown_through: Some(1), ..Painting::new(projector()) };
        let cut = paint(&store, run_id, rebuilt, Refuses { takes: 2 }).await.err().expect("cut");
        assert_eq!(cut.delivered, Some(3), "the page past the rows shown reached the sink");
    }

    /// Every corruption sits at a place in the run's record, like every
    /// other event: a row that no longer decodes paints why at its own
    /// place, in order, and a program that is gone is said with the first
    /// row painted. A painting rebuilt for a reader that showed those rows
    /// says neither again, so a follower that heard them from one reader
    /// skips them from any other by their place.
    #[test]
    fn a_corruption_is_painted_at_its_rows_place_and_once() {
        use weft_journal::record::{RawRecord, RunLogRow};
        let run_id = uuid::Uuid::from_u128(7);
        let tagged = |seq: i32| RunLogRow::of(seq, &[RunEvent::RunTagged { run_id, tags: vec![seq.to_string()], at_unix: seq as u64 }]);
        let bad = RunLogRow { seq: 1, events: b"not zstd".to_vec(), events_version: weft_journal::stored::STORED_EVENTS.version() };
        let record = || RawRecord { rows: vec![tagged(0), bad.clone(), tagged(2)] };
        let projector = || RunProjector::new(run_id, ProgramLookup::ProgramGone("its program is gone".into()), PROJECT);
        let painted = |painting: &mut Painting| -> Vec<(String, String)> {
            painting
                .page(run_id, record())
                .iter()
                .map(|event| {
                    let kind = match event.event {
                        weft_core::live_event::DispatcherEvent::JournalCorruption { .. } => "journal_corruption",
                        weft_core::live_event::DispatcherEvent::RunTagged { .. } => "run_tagged",
                        _ => "other",
                    };
                    (event.position().expect("every event of a row has its place").to_string(), kind.to_string())
                })
                .collect()
        };
        let place = |at: &str, kind: &str| (at.to_string(), kind.to_string());
        assert_eq!(
            painted(&mut Painting::new(projector())),
            [
                place("0:0:0", "journal_corruption"),
                place("0:0:1", "run_tagged"),
                place("1:0:0", "journal_corruption"),
                place("2:0:0", "run_tagged"),
            ],
            "the program's reason with the first row, the bad row's at its place, in the record's order"
        );
        let rebuilt = &mut Painting { shown_through: Some(1), ..Painting::new(projector()) };
        assert_eq!(painted(rebuilt), [place("2:0:0", "run_tagged")], "what was shown is said again by nobody");
    }
}
