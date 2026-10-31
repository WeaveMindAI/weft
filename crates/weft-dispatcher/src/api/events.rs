//! Server-Sent Events endpoints. Clients (CLI `weft follow`, `weft run`,
//! `weft wait`, the VS Code extension) follow a project's events, or one
//! run's. The dispatcher's EventBus (in-memory queue of batches per
//! project) is the source of what happens live; a run's record is the
//! source of what happened before.

use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::{Stream, StreamExt};
use tokio::sync::{broadcast, mpsc};

use weft_core::live_event::{EventPosition, ShownEvent};

use crate::actions::run_admin::{encoded, Values};
use crate::authenticator::{authorize_project, CallerTenant};
use crate::events::Follower;
use crate::state::DispatcherState;

/// What a follower asks of a stream: `with_values=true` has the values
/// written in (`run_admin::Values`); `after=<seq>:<index>:<k>`, on a run's
/// stream, starts past the event at that place in the run's record.
#[derive(Debug, Default, serde::Deserialize)]
pub struct FollowQuery {
    pub with_values: Option<bool>,
    pub after: Option<String>,
}

/// `GET /events/project/{id}`: every event of a project's runs and
/// lifecycle, as it happens. It covers many runs, so it has no single
/// place to start from or catch up to: a follower that falls behind is
/// told so and the stream ends, and the follower looks at the project's
/// state again before following anew (the editor's `ReconnectingStream`,
/// `weft follow`).
pub async fn project_stream(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(project_id): Path<uuid::Uuid>,
    Query(query): Query<FollowQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, broadcast::error::RecvError>>>, StatusCode> {
    authorize_project(&state, &caller.0, project_id)
        .await
        .map_err(|(s, _)| s)?;
    let rx = state.events.subscribe_project(project_id).await;
    let stream = live_events(rx, Values::asked(query.with_values)).take_until(stepping_aside(&state));
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// What the nodes of one graph are showing, pushed as it changes: the
/// editor names the nodes it draws a display for (`infra=` for infra
/// nodes, `signals=` for triggers, each a comma-separated list of places
/// as a person spells them), and gets each one's current display at
/// once, then every change. With `run=<id>`, the stream also carries what
/// that run's waiting nodes show (`DisplaySource::Wait`). Nothing is looked
/// at once the stream closes.
pub async fn display_stream(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(project): Path<uuid::Uuid>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, (StatusCode, String)> {
    use crate::display_feeds::{DisplayKey, DisplaySource, StateReader};
    authorize_project(&state, &caller.0, project).await?;
    let reader = std::sync::Arc::new(StateReader(state.clone()));
    let mut feeds = Vec::new();
    for (param, source) in [("infra", DisplaySource::Infra), ("signals", DisplaySource::Signal)] {
        for node in query.get(param).into_iter().flat_map(|list| list.split(',')).filter(|n| !n.is_empty()) {
            // The editor shows the program's shared copies; an instance's
            // own copy is seen through that instance's token.
            let key = DisplayKey { project, source, node: node.to_string(), instance: None };
            feeds.push(state.displays.watch(key, reader.clone()));
        }
    }
    // The run the editor follows, whose waiting nodes show what their
    // waits read: only a run of this project.
    let run = match query.get("run").filter(|run| !run.is_empty()) {
        Some(run) => {
            let run: weft_core::RunId = run.parse().map_err(|_| (StatusCode::BAD_REQUEST, format!("run={run} is not a run id")))?;
            let owner = state.runs.run_owner(run).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the run's row: {e:#}")))?;
            if owner.is_none_or(|owner| owner.project_id != project) {
                return Err((StatusCode::NOT_FOUND, format!("there is no run {run} in this project")));
            }
            Some(run)
        }
        None => None,
    };
    // A stream of nothing would end at once, and a client that
    // reconnects on an ended stream would ask again in a loop.
    if feeds.is_empty() && run.is_none() {
        return Err((StatusCode::BAD_REQUEST, "name at least one node (infra= or signals=) or a run (run=)".to_string()));
    }
    let mut streams: Vec<DisplayEvents> = feeds.into_iter().map(|feed| Box::pin(display_changes(feed)) as _).collect();
    if let Some(run) = run {
        let waits = crate::display_feeds::run_waits(reader, run).map(|display| Ok(display_event(&display)));
        streams.push(Box::pin(waits));
    }
    let stream = futures::stream::select_all(streams).take_until(stepping_aside(&state));
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// One source of display events on a display stream: a node's feed, or a
/// run's waits.
type DisplayEvents = std::pin::Pin<Box<dyn Stream<Item = Result<Event, std::convert::Infallible>> + Send>>;

/// One node's display as the SSE event the editor reads.
fn display_event(display: &crate::display_feeds::NodeDisplay) -> Event {
    Event::default().data(serde_json::to_string(display).expect("a display serializes"))
}

/// Every display one feed shows, as SSE events, for as long as the
/// stream holds the feed.
fn display_changes(
    feed: std::sync::Arc<crate::display_feeds::Feed>,
) -> impl Stream<Item = Result<Event, std::convert::Infallible>> {
    let shown = feed.shown();
    futures::stream::unfold((feed, shown, true), |(feed, mut shown, first)| async move {
        loop {
            // The first turn sends what is there already; after that,
            // only what changes.
            if !first || shown.borrow().is_none() {
                shown.changed().await.ok()?;
            }
            let current = shown.borrow_and_update().clone();
            if let Some(display) = current {
                let message = crate::display_feeds::NodeDisplay {
                    source: feed.key().source,
                    node: feed.key().node.clone(),
                    feed: display,
                };
                return Some((Ok(display_event(&message)), (feed, shown, false)));
            }
        }
    })
}

/// `GET /events/run/{id}`: one run's events, from its start (or past
/// `after`, the place in its record of the last event the follower holds),
/// then live, each once, in order, for as long as the follower stays (a
/// run's record can still get rows after its ending: a meter settling a
/// call it measured). The history is read off the run's record, then
/// [`CAUGHT_UP`] is sent, then the live events, skipping what the record
/// already gave. A follower that falls behind the live events goes back to
/// the record past the last event it was sent and on to live again, so it
/// never misses one and the stream never ends for it. A follower that
/// loses the connection opens it again with `after`.
pub async fn run_stream(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id): Path<String>,
    Query(query): Query<FollowQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, (StatusCode, String)> {
    let target_run_id: uuid::Uuid = run_id.parse().map_err(|_| (StatusCode::BAD_REQUEST, format!("'{run_id}' is not a run id")))?;
    let after = query
        .after
        .as_deref()
        .map(str::parse::<EventPosition>)
        .transpose()
        .map_err(|why| (StatusCode::BAD_REQUEST, format!("after: {why}")))?;
    // Resolve + tenant-gate in the ONE place that owns "who owns this
    // run": an unknown or cross-tenant run is 404 either way, and the
    // project id rides back for the stream's attribution.
    let project_id = crate::authenticator::authorize_run(&*state.runs, &caller.0, target_run_id).await?.project_id;
    // Subscribed BEFORE the record is read, so nothing that lands meanwhile
    // falls between the two.
    let live = state.events.subscribe_batches(project_id).await;
    let (tx, rx) = mpsc::channel(FOLLOWER_AHEAD);
    let aside = stepping_aside(&state);
    let history = RecordHistory { state, run_id: target_run_id, project_id, painting: None };
    let left = tx.clone();
    let follow = RunFollow::new(history, target_run_id, Values::asked(query.with_values), after, tx);
    tokio::spawn(async move {
        // A follower leaving ends its stream: only a failure on the way
        // is said.
        if let Err(e) = follow.run(live).await {
            if !left.is_closed() {
                tracing::warn!(target: "weft_dispatcher::api", run_id = %target_run_id, error = %format!("{e:#}"), "a run's follower stopped");
            }
        }
    });
    let stream = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|event| (Ok(event), rx)) }).take_until(aside);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// Once this copy of the dispatcher steps aside (`DispatcherState::aside`):
/// where every event stream ends, after the event it is sending. Its
/// follower opens it again, on a current copy: a run's stream from the
/// last event it holds (`after`), a project's stream after looking at the
/// project's state again, a display stream from the displays as they are.
fn stepping_aside(state: &DispatcherState) -> impl std::future::Future<Output = ()> + Send + 'static {
    let aside = state.aside.clone();
    async move { aside.stepped().await }
}

/// How many events wait for a slow follower of a run before its read of
/// the record pauses for it.
const FOLLOWER_AHEAD: usize = 64;

/// The name of the SSE event a run's stream sends once what happened
/// before is sent and what follows is live: a follower that shows a run
/// as still going waits for it, so a run whose history holds its end is
/// never shown going.
// SYNC: CAUGHT_UP <-> extension-vscode/src/runFollower.ts CAUGHT_UP, crates/weft-cli/src/commands/follow.rs CAUGHT_UP
pub const CAUGHT_UP: &str = "caught_up";

/// Where a run's follower catches up from: what happened past the last
/// event it holds.
#[async_trait::async_trait]
trait History: Send {
    /// Send to `to` the run's events past what it was sent (what about the
    /// run cannot be read among them, each at its row's place), then the
    /// event about where the run stands. Answers `to` with where it
    /// stands.
    async fn catch_up(&mut self, to: Catching) -> anyhow::Result<Catching>;
}

/// The run's record, painted into the follower's own projection
/// (`crate::painting`): opened at its first catch-up and kept, so each
/// catch-up after a lag reads only the rows past the last one it painted.
/// The projection shows the run (a firing's values held while it is open),
/// so what one follower keeps does not grow with the run.
struct RecordHistory {
    state: DispatcherState,
    run_id: uuid::Uuid,
    project_id: uuid::Uuid,
    painting: Option<crate::painting::Painting>,
}

#[async_trait::async_trait]
impl History for RecordHistory {
    async fn catch_up(&mut self, to: Catching) -> anyhow::Result<Catching> {
        let painting = match self.painting.take() {
            Some(painting) => painting,
            None => crate::painting::Painting::new(crate::painting::open(&self.state, self.run_id, self.project_id).await?),
        };
        let (painting, mut to) = crate::painting::paint(&*self.state.runs, self.run_id, painting, to).await?;
        self.painting = Some(painting);
        if let Some(parked) = crate::painting::parked_now(&self.state, self.run_id, self.project_id).await? {
            to.send(parked).await?;
        }
        Ok(to)
    }
}

/// One follower of one run ([`run_stream`]).
struct RunFollow<H> {
    history: H,
    run_id: uuid::Uuid,
    to: Catching,
}

impl<H: History> RunFollow<H> {
    /// A follower of run `run_id` holding the events up to `sent`, its
    /// events going to `tx` with their values as `values` says.
    fn new(history: H, run_id: uuid::Uuid, values: Values, sent: Option<EventPosition>, tx: mpsc::Sender<Event>) -> Self {
        Self { history, run_id, to: Catching { tx, values, sent, parked: false } }
    }

    /// Follow until the follower leaves (the answer is then the error of a
    /// send nobody receives).
    async fn run(self, mut live: broadcast::Receiver<crate::events::Batch>) -> anyhow::Result<()> {
        let Self { mut history, run_id, mut to } = self;
        loop {
            to = history.catch_up(to).await?;
            to.tx.send(Event::default().event(CAUGHT_UP).data(CAUGHT_UP)).await.map_err(|_| anyhow::anyhow!("the follower left"))?;
            follow(&mut to, run_id, &mut live).await?;
        }
    }
}

/// Send `to` the live events of run `run_id` it was not sent yet, until it
/// lags behind them.
async fn follow(to: &mut Catching, run_id: uuid::Uuid, live: &mut broadcast::Receiver<crate::events::Batch>) -> anyhow::Result<()> {
    loop {
        // A follower that left frees its place on the project's queue
        // now, never at the project's next event, which may not come.
        let heard = tokio::select! {
            heard = live.recv() => heard,
            () = to.tx.closed() => anyhow::bail!("the follower left"),
        };
        let batch = match heard {
            Ok(batch) => batch,
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                tracing::debug!(target: "weft_dispatcher::api", %run_id, missed, "a run's follower lagged; catching up from the record");
                return Ok(());
            }
            Err(broadcast::error::RecvError::Closed) => anyhow::bail!("the project's events stopped"),
        };
        for event in batch.iter().filter(|event| event.event.run_id() == Some(run_id)) {
            to.send(event.clone()).await?;
        }
    }
}

/// A run's follower: where its events go, and what it was sent, so each
/// event reaches it once whether it comes off the record or the live
/// queue.
struct Catching {
    tx: mpsc::Sender<Event>,
    values: Values,
    /// The place of the last recorded event sent (or the follower already
    /// held): nothing at or before it goes out again.
    sent: Option<EventPosition>,
    /// Whether the follower heard the run is parked and no recorded event
    /// went out since: the same parking reaches it off its catch-up and
    /// off the live queue, and it hears it once. A parking after the run
    /// moved again follows recorded events, so it is heard too.
    parked: bool,
}

impl Catching {
    /// Whether `event` goes out to the follower, noted as sent when it
    /// does.
    fn admit(&mut self, event: &ShownEvent) -> bool {
        match event.position() {
            Some(at) => {
                if self.sent.is_some_and(|sent| at <= sent) {
                    return false;
                }
                self.sent = Some(at);
                self.parked = false;
                true
            }
            None if matches!(event.event, weft_core::live_event::DispatcherEvent::RunParked { .. }) => !std::mem::replace(&mut self.parked, true),
            None => true,
        }
    }

    /// Send one event, unless the follower was sent it.
    async fn send(&mut self, event: ShownEvent) -> anyhow::Result<()> {
        if !self.admit(&event) {
            return Ok(());
        }
        self.tx.send(to_sse(event, self.values)?).await.map_err(|_| anyhow::anyhow!("the follower left"))
    }
}

#[async_trait::async_trait]
impl crate::painting::Sink for Catching {
    type Page = Vec<Event>;

    fn page(&mut self, events: Vec<ShownEvent>) -> anyhow::Result<Vec<Event>> {
        let mut page = Vec::new();
        for event in events {
            if self.admit(&event) {
                page.push(to_sse(event, self.values)?);
            }
        }
        Ok(page)
    }

    async fn deliver(&mut self, page: Vec<Event>) -> anyhow::Result<()> {
        for event in page {
            self.tx.send(event).await.map_err(|_| anyhow::anyhow!("the follower left"))?;
        }
        Ok(())
    }
}

fn live_events(
    rx: Follower,
    values: Values,
) -> impl Stream<Item = Result<Event, broadcast::error::RecvError>> {
    futures::stream::unfold(Some(rx), move |rx| async move {
        let mut rx = rx?;
        match rx.recv().await {
            Ok(event) => match to_sse(event, values) {
                Ok(event) => Some((Ok(event), Some(rx))),
                Err(e) => {
                    tracing::error!(target: "weft_dispatcher::api", error = %format!("{e:#}"), "an event could not be written; ending the stream");
                    None
                }
            },
            Err(broadcast::error::RecvError::Closed) => None,
            Err(err) => {
                // Dropped updates invalidate a project's live view. End
                // this subscription with an error so clients show the loss
                // and look at the project again, instead of continuing
                // with an incomplete history.
                tracing::warn!(%err, "ending a project's event stream after lost updates");
                Some((Err(err), None))
            }
        }
    })
}

fn to_sse(event: ShownEvent, values: Values) -> anyhow::Result<Event> {
    let payload = String::from_utf8(encoded(event, values)?).expect("JSON is UTF-8");
    Ok(Event::default().data(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use crate::events::{DispatcherEvent, IdentifiedEvent};

    const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x100);

    fn completed(run_id: uuid::Uuid) -> ShownEvent {
        IdentifiedEvent::transient(DispatcherEvent::RunCompleted { run_id, project_id: PROJECT, outputs: None, at_unix: 1 })
    }

    /// Event `seq` of run `run_id`'s record: a tag, or its ending.
    fn recorded(run_id: uuid::Uuid, seq: i32, ends: bool) -> ShownEvent {
        let event = if ends {
            DispatcherEvent::RunCompleted { run_id, project_id: PROJECT, outputs: None, at_unix: 1 }
        } else {
            DispatcherEvent::RunTagged { run_id, project_id: PROJECT, tags: vec![seq.to_string()], at_unix: 1 }
        };
        IdentifiedEvent { event_id: crate::events::EventId::Recorded(at(seq)), event }
    }

    fn at(seq: i32) -> EventPosition {
        EventPosition { at: weft_core::record_ref::Position { seq, index: 0 }, k: 0 }
    }

    /// A run's record as a follower catches up from it: the events it holds
    /// so far, painted.
    struct Painted(std::sync::Arc<std::sync::Mutex<Vec<ShownEvent>>>);

    #[async_trait::async_trait]
    impl History for Painted {
        async fn catch_up(&mut self, to: Catching) -> anyhow::Result<Catching> {
            let events = self.0.lock().unwrap().clone();
            painted_into(to, events).await
        }
    }

    /// `events` handed to `to` the way a painting hands a page.
    async fn painted_into(mut to: Catching, events: Vec<ShownEvent>) -> anyhow::Result<Catching> {
        use crate::painting::Sink;
        let page = to.page(events)?;
        to.deliver(page).await?;
        Ok(to)
    }

    /// Follow run `run_id` over `record` and `live`, from past `after`,
    /// until `want` recorded events came (or, wanting none, until it
    /// caught up), then leave; answers the places of the recorded events
    /// it was sent, in order, and how many times it heard it caught up.
    async fn followed(run_id: uuid::Uuid, record: Painted, live: broadcast::Receiver<crate::events::Batch>, after: Option<EventPosition>, want: usize) -> (Vec<i32>, usize) {
        let (tx, mut rx) = mpsc::channel(4);
        let follow = RunFollow::new(record, run_id, Values::ByReference, after, tx);
        let task = tokio::spawn(follow.run(live));
        let (mut sent, mut caught_up) = (Vec::new(), 0);
        while let Some(event) = rx.recv().await {
            let wire = format!("{event:?}");
            if wire.contains(CAUGHT_UP) {
                caught_up += 1;
            } else {
                sent.extend(wire.split("journal:").nth(1).and_then(|rest| rest.split(':').next()).and_then(|seq| seq.parse::<i32>().ok()));
            }
            if sent.len() == want && (want > 0 || caught_up > 0) {
                break;
            }
        }
        drop(rx);
        assert!(task.await.unwrap().is_err(), "a follower runs until it leaves");
        (sent, caught_up)
    }

    /// A display stream sends what the node shows as soon as it is
    /// known, then only what changes, named by the node the editor asked
    /// for.
    #[tokio::test(start_paused = true)]
    async fn a_display_stream_sends_the_current_display_then_changes() {
        use crate::display_feeds::{DisplayAt, DisplayFeeds, DisplayKey, DisplayReader, DisplaySource, NodeFeed, LOOK_EVERY};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Flip(AtomicUsize);
        #[async_trait::async_trait]
        impl DisplayReader for Flip {
            async fn find(&self, _key: &DisplayKey) -> Result<DisplayAt, String> {
                Ok(DisplayAt::Nowhere)
            }

            async fn read(&self, _key: &DisplayKey, _at: &DisplayAt) -> NodeFeed {
                if self.0.fetch_add(1, Ordering::SeqCst) < 3 { NodeFeed::Absent } else { NodeFeed::Ok { items: vec![] } }
            }
        }
        let feeds = DisplayFeeds::default();
        let key = DisplayKey { project: uuid::Uuid::nil(), source: DisplaySource::Signal, node: "one.door".into(), instance: None };
        let feed = feeds.watch(key, std::sync::Arc::new(Flip(AtomicUsize::new(0))));
        let stream = display_changes(feed);
        futures::pin_mut!(stream);
        let started = tokio::time::Instant::now();
        let first = stream.next().await.unwrap().unwrap();
        assert!(format!("{first:?}").contains(r#"\"state\":\"absent\""#), "{first:?}");
        let second = stream.next().await.unwrap().unwrap();
        assert!(format!("{second:?}").contains(r#"\"state\":\"ok\""#), "{second:?}");
        assert!(format!("{second:?}").contains(r#"\"node\":\"one.door\""#), "{second:?}");
        assert_eq!(started.elapsed(), LOOK_EVERY * 3, "the unchanged looks in between sent nothing");
    }

    weft_core::stress_test!(
        name: a_project_follower_that_lags_is_told_and_the_stream_ends,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, rx) = broadcast::channel(1);
            let run_id = uuid::Uuid::new_v4();
            tx.send(std::sync::Arc::from([completed(run_id)])).unwrap();
            tx.send(std::sync::Arc::from([completed(run_id)])).unwrap();
            let stream = live_events(Follower::of(rx), Values::ByReference);
            futures::pin_mut!(stream);
            assert!(matches!(stream.next().await, Some(Err(broadcast::error::RecvError::Lagged(1)))));
            assert!(stream.next().await.is_none());
        }
    );

    weft_core::stress_test!(
        name: a_burst_of_one_run_takes_one_place_and_reaches_its_follower_whole,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let run_id = uuid::Uuid::new_v4();
            let (tx, rx) = broadcast::channel(1);
            // More events than a project's queue holds items, in one paint.
            let burst: Vec<ShownEvent> = (0..1000).map(|seq| recorded(run_id, seq, seq == 999)).collect();
            tx.send(std::sync::Arc::from(burst)).unwrap();
            let (sent, _) = followed(run_id, Painted(Default::default()), rx, None, 1000).await;
            assert_eq!(sent, (0..1000).collect::<Vec<_>>());
        }
    );

    /// A run's record that grows while a follower reads it: what lands
    /// during the follower's first read is put on the project's queue, more
    /// of it than the queue holds, so the follower falls behind.
    struct Growing {
        record: std::sync::Arc<std::sync::Mutex<Vec<ShownEvent>>>,
        landing: std::sync::Mutex<Option<(broadcast::Sender<crate::events::Batch>, Vec<ShownEvent>)>>,
    }

    #[async_trait::async_trait]
    impl History for Growing {
        async fn catch_up(&mut self, to: Catching) -> anyhow::Result<Catching> {
            let read = self.record.lock().unwrap().clone();
            if let Some((queue, landing)) = self.landing.lock().unwrap().take() {
                for event in landing {
                    self.record.lock().unwrap().push(event.clone());
                    queue.send(std::sync::Arc::from([event])).unwrap();
                }
            }
            painted_into(to, read).await
        }
    }

    weft_core::stress_test!(
        name: a_run_follower_that_lags_catches_up_from_the_record_each_event_once,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let run_id = uuid::Uuid::new_v4();
            let (tx, rx) = broadcast::channel(1);
            let record = std::sync::Arc::new(std::sync::Mutex::new((0..3).map(|seq| recorded(run_id, seq, false)).collect::<Vec<_>>()));
            let landing = (3..9).map(|seq| recorded(run_id, seq, seq == 8)).collect();
            let history = Growing { record, landing: std::sync::Mutex::new(Some((tx, landing))) };
            let (out, mut events) = mpsc::channel(4);
            let task = tokio::spawn(RunFollow::new(history, run_id, Values::ByReference, None, out).run(rx));
            let (mut sent, mut caught_up) = (Vec::new(), 0);
            // The second catch-up says it caught up after what it sent.
            while sent.len() < 9 || caught_up < 2 {
                let wire = format!("{:?}", events.recv().await.expect("the follower follows"));
                if wire.contains(CAUGHT_UP) {
                    caught_up += 1;
                } else {
                    sent.extend(wire.split("journal:").nth(1).and_then(|rest| rest.split(':').next()).and_then(|seq| seq.parse::<i32>().ok()));
                }
            }
            drop(events);
            assert!(task.await.unwrap().is_err(), "a follower runs until it leaves");
            assert_eq!(sent, (0..9).collect::<Vec<_>>(), "each event once, in order, across the catch-up");
            assert_eq!(caught_up, 2, "caught up once from the start, once after it fell behind");
        }
    );

    weft_core::stress_test!(
        name: a_run_follower_from_a_place_gets_what_follows_it_then_live_without_repeats,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let run_id = uuid::Uuid::new_v4();
            let other = uuid::Uuid::new_v4();
            let record = std::sync::Arc::new(std::sync::Mutex::new((0..10).map(|seq| recorded(run_id, seq, false)).collect::<Vec<_>>()));
            let (tx, rx) = broadcast::channel(8);
            // Live overlaps what the record gave, and carries another run.
            tx.send(std::sync::Arc::from([recorded(run_id, 8, false), recorded(other, 11, false), recorded(run_id, 9, false), recorded(run_id, 10, false)])).unwrap();
            tx.send(std::sync::Arc::from([recorded(run_id, 11, true)])).unwrap();
            let (sent, caught_up) = followed(run_id, Painted(record), rx, Some(at(4)), 7).await;
            assert_eq!(sent, (5..12).collect::<Vec<_>>());
            assert_eq!(caught_up, 1, "the history, then live");
        }
    );

    /// A parked run's record as a follower catches up from it: its events,
    /// then that it is parked, as `RecordHistory` says it.
    struct Parked(Vec<ShownEvent>);

    #[async_trait::async_trait]
    impl History for Parked {
        async fn catch_up(&mut self, to: Catching) -> anyhow::Result<Catching> {
            let mut to = painted_into(to, self.0.clone()).await?;
            to.send(parked(self.0[0].event.run_id().unwrap())).await?;
            Ok(to)
        }
    }

    fn parked(run_id: uuid::Uuid) -> ShownEvent {
        IdentifiedEvent::transient(DispatcherEvent::RunParked { run_id, project_id: PROJECT })
    }

    weft_core::stress_test!(
        name: a_parked_run_reaches_its_follower_parked_once_per_parking,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let run_id = uuid::Uuid::new_v4();
            let (tx, rx) = broadcast::channel(8);
            // The same parking off the live queue, then the run moves on
            // and parks again.
            tx.send(std::sync::Arc::from([parked(run_id)])).unwrap();
            tx.send(std::sync::Arc::from([recorded(run_id, 2, false), parked(run_id)])).unwrap();
            let (out, mut events) = mpsc::channel(4);
            let history = Parked((0..2).map(|seq| recorded(run_id, seq, false)).collect());
            let task = tokio::spawn(RunFollow::new(history, run_id, Values::ByReference, None, out).run(rx));
            let mut heard = Vec::new();
            while heard.len() < 6 {
                let wire = format!("{:?}", events.recv().await.expect("the follower follows"));
                heard.push(if wire.contains("run_parked") { "parked".to_string() } else if wire.contains(CAUGHT_UP) { "caught up".to_string() } else { "event".to_string() });
            }
            drop(events);
            assert!(task.await.unwrap().is_err(), "a follower runs until it leaves");
            assert_eq!(heard, ["event", "event", "parked", "caught up", "event", "parked"]);
        }
    );

    weft_core::stress_test!(
        name: a_follower_of_a_run_that_ended_gets_its_history_then_what_lands_after,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let run_id = uuid::Uuid::new_v4();
            let record = std::sync::Arc::new(std::sync::Mutex::new((0..4).map(|seq| recorded(run_id, seq, seq == 3)).collect::<Vec<_>>()));
            let (tx, rx) = broadcast::channel(4);
            // A meter settling after the run's ending: its row lands later.
            tx.send(std::sync::Arc::from([recorded(run_id, 4, false)])).unwrap();
            assert_eq!(followed(run_id, Painted(record.clone()), rx, None, 5).await, (vec![0, 1, 2, 3, 4], 1));
            let (_tx, rx) = broadcast::channel(1);
            assert_eq!(followed(run_id, Painted(record), rx, Some(at(3)), 0).await, (Vec::new(), 1), "it held everything already");
        }
    );
}
