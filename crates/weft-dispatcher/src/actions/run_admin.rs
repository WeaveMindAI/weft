//! Every action on runs that already exist: stopping one (`weft stop`),
//! waking a wait (`weft wake`), reading one (its detail, logs, events,
//! replay, outputs, a node's state), finding one by an id prefix, listing,
//! counting and cleaning them, and deleting one (`weft clean <run>`).
//!
//! A run is held to its OWNER, never to its project: a run outlives its
//! project (its record is the record of what ran, and `weft rm` takes only
//! the project), so every action on one run authorizes by the tenant
//! stamped on the run at birth ([`authorize`]). That is also exactly the
//! program's rule (any project of the same owner): a project cannot
//! change owner, so a run of the actor's owner is a run of one of the
//! actor's owner's projects, whether or not that project still exists.
//! The actions that read a project's runs (list, count, clean, costs,
//! latest) are held to the project instead.

use std::collections::HashMap;

use axum::http::StatusCode;
use weft_core::program::runs::{EventsRequest, LogsRequest, OutputsRequest, RunList, RunStopped, WakeRequest};
use weft_core::program::{
    CleanOutcome, CleanRequest, CostFilter, CostRecord, DeletedRun, NodeState, ParkedWait, ResolvedRun, RunDetail, RunEvents, RunFilter, RunLogs,
    RunPage, RunStatus, RunSummary, StillGoing, MAX_RUNS_PAGE,
};
use weft_core::RunId;

use super::{failed, Actor, Refused};
use crate::api::run::{cancel_run_id, clean_run, list_query};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;
use crate::store::{RunListQuery, RunOwner};

/// This area's actions ([`super::names`]).
pub const NAMES: &[(&str, super::Reach)] = &[
    ("weft.runs.clean", super::Reach::Everyone),
    ("weft.runs.list", super::Reach::Everyone),
    ("weft.runs.count", super::Reach::Everyone),
    ("weft.runs.node_state", super::Reach::Everyone),
    ("weft.costs.list", super::Reach::Everyone),
    ("weft.runs.stop", super::Reach::Everyone),
    ("weft.runs.wake", super::Reach::Everyone),
    ("weft.runs.logs", super::Reach::Everyone),
    ("weft.runs.events", super::Reach::Everyone),
    ("weft.runs.outputs", super::Reach::Everyone),
    ("weft.runs.detail", super::Reach::Everyone),
    ("weft.runs.resolve", super::Reach::Everyone),
    ("weft.runs.delete", super::Reach::Everyone),
];

/// Hold `actor` to the run `run_id`: the tenant stamped on it must be the
/// actor's (see the module's note). A run of another owner answers as a
/// run that does not exist, so nobody can probe which ids exist elsewhere.
async fn authorize(state: &DispatcherState, actor: &Actor, run_id: RunId) -> Result<RunOwner, Refused> {
    crate::authenticator::authorize_run(&*state.runs, actor.tenant(), run_id).await
}

// ----- finding a run ------------------------------------------------------

/// How many ambiguous matches an error names before it stops. Enough to
/// recognise the run you meant, few enough that the instruction after
/// them is still on screen.
const AMBIGUOUS_PREFIX_SHOWN: usize = 5;

/// The one run of the actor's owner whose id starts with `prefix` (a full
/// uuid resolves to itself). 404 when nothing matches, 409 when the prefix
/// is short enough to match several, naming them.
pub async fn resolve(state: &DispatcherState, actor: &Actor, prefix: &str) -> Result<ResolvedRun, Refused> {
    let prefix = prefix.to_ascii_lowercase();
    if prefix.len() < 4 || !prefix.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err((StatusCode::BAD_REQUEST, format!("'{prefix}' is not the start of a run id: give at least four hex characters")));
    }
    let matches = state.runs.run_ids_with_prefix(actor.tenant().as_str(), &prefix).await.map_err(failed("run_ids_with_prefix"))?;
    match matches.as_slice() {
        [] => Err((StatusCode::NOT_FOUND, format!("no run starts with '{prefix}'"))),
        [one] => Ok(ResolvedRun { run_id: *one }),
        // A count and a few short ids, never the whole list: an empty or
        // one-character prefix matches everything the project ever ran,
        // and printing a hundred full uuids buries the one sentence that
        // says what to do about it.
        several => Err((
            StatusCode::CONFLICT,
            format!(
                "'{prefix}' starts {} runs ({}{}); give more characters",
                several.len(),
                several.iter().take(AMBIGUOUS_PREFIX_SHOWN).map(|c| c.to_string()[..8].to_string()).collect::<Vec<_>>().join(", "),
                if several.len() > AMBIGUOUS_PREFIX_SHOWN { ", ..." } else { "" },
            ),
        )),
    }
}

/// The most recent run of `project`; 404 when it has none (`weft logs`
/// with no run named).
pub async fn latest(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<RunSummary, Refused> {
    let page = list(state, actor, Some(project), &RunList { limit: 1, ..RunList::default() }).await?;
    page.runs.into_iter().next().ok_or_else(|| (StatusCode::NOT_FOUND, format!("project {project} has no runs")))
}

// ----- stopping and waking ------------------------------------------------

/// Stop a run, as `weft stop` does. A run that already ended has nothing
/// to stop, and the answer says so with its status (the editor's Stop
/// once sat on "Cancelling..." for a run that had finished an hour
/// before), so a stop carried again answers the same instead of failing.
pub async fn stop(state: &DispatcherState, actor: &Actor, run_id: RunId) -> Result<RunStopped, Refused> {
    authorize(state, actor, run_id).await?;
    if let Some(summary) = state.runs.run_summary(run_id).await.map_err(failed("run summary"))? {
        if let Some(status) = already_ended(summary.status) {
            return Ok(RunStopped { already_ended: Some(status) });
        }
    }
    cancel_run_id(state, run_id, &weft_core::run::CancelCause::User).await.map_err(|e| {
        tracing::error!(target: "weft_dispatcher::cancel", run_id = %run_id, error = %e, "cancel_run_id failed");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;
    Ok(RunStopped { already_ended: None })
}

/// `status` when the run is over with, `None` while it can still be
/// stopped.
fn already_ended(status: RunStatus) -> Option<RunStatus> {
    match status {
        RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled => Some(status),
        RunStatus::Running | RunStatus::WaitingForInput => None,
    }
}

/// Resolve a wait now, instead of waiting for whatever it waits for (`weft
/// wake`). Answers how the fire was taken.
///
/// This side decides whether the wake may happen: the actor's owner, and
/// that the node really has a wait parked on this run. The node is read
/// against the program the run was started on, so a node moved since is
/// named the way it was in that run. A node inside a loop can wait in
/// several iterations at once, and then `iteration` (`3`, or `3.0` inside
/// an inner loop) says which; without it the wake is refused, listing
/// them. What the wait then wakes WITH is the signal kind's own shape, so
/// the listener holding it is asked, and it answers with nothing for every
/// kind that has no truthful stand-in (a form is waiting for an answer;
/// there is no inventing one). Those are refused naming the kind, and the
/// fire goes through the same lifecycle gate every other fire passes.
///
/// Nothing here knows which kinds can be woken. That is why: a tier
/// that minted one kind's payload would have to be edited for the next.
pub async fn wake(state: &DispatcherState, actor: &Actor, run_id: RunId, node: &str, request: &WakeRequest) -> Result<StatusCode, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    let program = crate::projection::registered_program(state, run_id, &owner).await.map_err(failed("read the run's program"))?.or_why(run_id);
    // Bounded, like every other caller string this surface echoes: the
    // node name is a raw path segment, so an enormous one would come
    // straight back in the body.
    let node = weft_core::truncate_user_string(node, 256);
    let (_, weft_core::project::places::Named::Node(place)) = named_in_run(program.as_deref(), run_id, &node)? else {
        return Err((StatusCode::BAD_REQUEST, format!("'{node}' is a group, and only a node waits; name the node inside it that waits")));
    };
    let program = program.map_err(|why| (StatusCode::CONFLICT, why))?;
    // A wait is registered under the place spelled the way the program
    // writes it, which is what the name was just read into.
    let spelled = weft_core::project::address_of(&*program, &place.id, &place.path);
    let iteration = request
        .iteration
        .as_deref()
        .map(weft_core::live_event::spelled::parse_iteration)
        .transpose()
        .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let loops = weft_core::live_event::spelled::check_iteration(
        &*program,
        &weft_core::project::places::Named::Node(place.clone()),
        iteration.as_deref().unwrap_or_default(),
    )
    .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let waits: Vec<ParkedWait> =
        parked_waits(state, run_id).await.map_err(failed("signals"))?.into_iter().filter(|w| w.node == spelled).collect();
    // Which iteration each wait sits in is read off the run's record, and
    // only when something asks which: one wait, with no iteration named,
    // is the one.
    let iterations = if waits.len() > 1 || iteration.is_some() {
        let (_, snapshot) = crate::projection::run_snapshot(state, run_id).await.map_err(failed("the run's record"))?;
        Some(crate::node_state::wait_iterations(&snapshot, &place))
    } else {
        None
    };
    let wait = pick_wait(&waits, iterations.as_ref(), iteration.as_deref(), loops, &spelled, run_id)?;
    // WHETHER a wake may happen is this side's question, and it has been
    // answered above: the project is live, the node really is waiting,
    // the actor may touch it. WHAT it wakes with is the signal kind's,
    // so the listener is asked. A kind that cannot be woken by hand
    // answers with nothing, and the refusal below names the kind without
    // this tier ever knowing one.
    let payload = state.listener.wake_by_hand(&wait.token).await.map_err(failed("ask what it wakes with"))?;
    let Some(payload) = payload else {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "'{}' is waiting on a {} signal, which expects a value; answer it (its form, in the browser extension or through its signal URL) instead of waking it",
                wait.node, wait.kind
            ),
        ));
    };
    crate::api::signal::fire_registered_signal(state, &wait.token, payload).await
}

/// The one of `waits` (the waits of `run_id` at the place `spelled`, a
/// place inside `loops` loops) a wake means: the only one, or the only one
/// in the loop iterations `wanted`, read off `iterations` (each wait's
/// iterations by token, from the run's record,
/// [`crate::node_state::wait_iterations`]; `None` when the record was not
/// read, because nothing asked which). Anything but exactly one is
/// refused: none names what there is, several list each by its
/// iteration, so the next wake can name one.
fn pick_wait(
    waits: &[ParkedWait],
    iterations: Option<&HashMap<String, Vec<u32>>>,
    wanted: Option<&[u32]>,
    loops: usize,
    spelled: &str,
    run_id: RunId,
) -> Result<ParkedWait, Refused> {
    if waits.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("'{spelled}' is not waiting on anything in {run_id}; `weft events {run_id} --node {spelled}` shows what it did"),
        ));
    }
    let of = |w: &ParkedWait| iterations.and_then(|iterations| iterations.get(&w.token));
    let chosen: Vec<&ParkedWait> =
        waits.iter().filter(|w| wanted.is_none_or(|wanted| of(w).is_some_and(|at| at.starts_with(wanted)))).collect();
    let listed = |waits: &[&ParkedWait]| {
        waits
            .iter()
            .map(|w| of(w).map_or_else(|| format!("wait {} (not on the run's record yet)", w.token), |at| iteration_text(at)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match chosen.as_slice() {
        [one] => Ok((*one).clone()),
        [] => Err((
            StatusCode::NOT_FOUND,
            format!(
                "'{spelled}' is not waiting in iteration {} in {run_id}; it waits in {}",
                iteration_text(wanted.unwrap_or_default()),
                listed(&waits.iter().collect::<Vec<_>>())
            ),
        )),
        several if loops == 0 => Err((
            StatusCode::CONFLICT,
            format!("'{spelled}' holds {} waits in {run_id}, outside every loop, and nothing tells them apart", several.len()),
        )),
        several => Err((
            StatusCode::CONFLICT,
            format!("'{spelled}' waits {} times in {run_id}, in iterations {}; say which with `--iteration`", several.len(), listed(several)),
        )),
    }
}

/// Loop iterations as `--iteration` takes them (`3.0`).
fn iteration_text(iteration: &[u32]) -> String {
    iteration.iter().map(u32::to_string).collect::<Vec<_>>().join(".")
}

// ----- reading one run ----------------------------------------------------

/// One run as the listing shows it, and what it is parked on.
pub async fn detail(state: &DispatcherState, actor: &Actor, run_id: RunId) -> Result<RunDetail, Refused> {
    authorize(state, actor, run_id).await?;
    // Direct point-lookup by run (authorization above already proved the
    // actor owns it), so a run older than any list window still resolves
    // instead of 404ing.
    let summary = state
        .runs
        .run_summary(run_id)
        .await
        .map_err(failed("read the run's summary"))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    // The waits ride with the status they produced: a run reads as
    // `waiting_for_input` because these rows exist, and they exist
    // before the journal's `NodeSuspended` lands (the node registers
    // its wait, then returns), so a client that learns the run is
    // parked learns from the same read what it is parked on.
    let waiting = if summary.status == RunStatus::WaitingForInput {
        parked_waits(state, run_id).await.map_err(failed("read the run's waits"))?
    } else {
        Vec::new()
    };
    Ok(RunDetail { summary, waiting })
}

/// The waits of `run_id`, from the resume-signal rows that make it
/// `waiting_for_input`, in registration order.
async fn parked_waits(state: &DispatcherState, run_id: RunId) -> anyhow::Result<Vec<ParkedWait>> {
    let signals = state.registrations.signal_list_for_run_id(run_id).await?;
    waits_of(&signals, run_id)
}

fn waits_of(signals: &[crate::store::SignalRegistration], run_id: RunId) -> anyhow::Result<Vec<ParkedWait>> {
    signals
        .iter()
        .filter(|s| s.is_resume && s.run_id == Some(run_id))
        .map(|s| {
            let spec: weft_core::primitive::SignalSpec = serde_json::from_str(&s.spec_json)?;
            Ok(ParkedWait { node: s.node_id.clone(), token: s.token.clone(), kind: spec.kind })
        })
        .collect()
}

/// How many log lines one read returns, and the ceiling on asking for
/// more. `RunStore::logs_for` answers with the TAIL, so the default holds the
/// end of a long run, which is where a run goes wrong. A `limit`
/// outside `1..=MAX` is refused, not quietly moved: a caller who
/// asked for more than the ceiling would otherwise read a cut log as
/// the whole one.
const DEFAULT_LOG_LINES: u32 = 1_000;
const MAX_LOG_LINES: u32 = 20_000;

/// The tail of a run's log, each line's node spelled the way the program
/// the run was started on is written, whatever its project holds now.
pub async fn logs(state: &DispatcherState, actor: &Actor, run_id: RunId, request: &LogsRequest) -> Result<RunLogs, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    let limit = request.limit.unwrap_or(DEFAULT_LOG_LINES);
    if !(1..=MAX_LOG_LINES).contains(&limit) {
        return Err((StatusCode::BAD_REQUEST, format!("limit is {limit}; one read holds between 1 and {MAX_LOG_LINES} lines")));
    }
    // The journal's error names the run and `weft clean` when a row
    // no longer decodes; the reader gets it, not a bare 500.
    let entries = crate::projection::run_logs(state, run_id, limit).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let lookup = crate::projection::registered_program(state, run_id, &owner).await.map_err(failed("read the run's program"))?;
    let program = lookup.program();
    // Indexed once: every line is spelled through it.
    let graph = program.as_deref().map(weft_core::project::graph::ProjectGraph::new);
    let spell = |node: String, frames: &weft_core::LoopFrames| {
        let Some(program) = &graph else { return node };
        let path: Vec<String> = weft_core::frames::call_path(frames).into_iter().map(str::to_string).collect();
        weft_core::project::placed_address(program, &node, &path).unwrap_or_else(|| {
            tracing::error!(target: "weft_dispatcher::api", %run_id, %node, "a log line names a node the run's program does not have; it is shown by its id");
            node
        })
    };
    let lines = entries
        .into_iter()
        .map(|e| weft_core::program::RunLogLine {
            inherited_from: e.inherited_from,
            at_unix: e.at_unix,
            level: e.level,
            node: e.node.map(|node| spell(node, &e.frames)),
            frames: e.frames,
            message: e.message,
        })
        .collect();
    let unspelled = lookup.unpaintable().map(|(_, why)| why.to_string());
    Ok(RunLogs { limit, lines, unspelled })
}

/// Complete ordered output evidence, including streams and inherited
/// results; `request.expect` names the places a later diff puts first.
pub async fn outputs(
    state: &DispatcherState,
    actor: &Actor,
    run_id: RunId,
    request: &OutputsRequest,
) -> Result<weft_core::run_spec::Expected, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    let sources = crate::projection::reconstruct_run(state, run_id).await.map_err(failed("read output history"))?;
    let fold = &sources[&run_id];
    // The run's own program is its cut, and a seeded run's cut leaves out
    // what it reuses (a run that reuses everything it reaches runs an
    // empty one), while its record holds the reused results too. So every
    // place is spelled through the registered program the cut was taken
    // from: ids are that program's own, and only it still has the loop a
    // reused `LoopIn` or `LoopOut` belongs to.
    let program = crate::projection::registered_program(state, run_id, &owner)
        .await
        .map_err(failed("read the run's program"))?
        .or_why(run_id)
        .map_err(|why| (StatusCode::CONFLICT, why))?;
    // Indexed once: every wire and place below is spelled through it.
    let project = &weft_core::project::graph::ProjectGraph::new(&program);
    // A loop's `LoopIn` is the compiler's: what it hands the body is the
    // wire of the node that fed the loop, so its wire says nothing a
    // person's wire does not, and its name (the loop's) would collide
    // with theirs. Its `LoopOut` is different: the list it gathers over
    // the iterations exists nowhere else, so it stays, as the loop's own
    // output (`doubler.results`).
    let boundary = |id: &str| {
        weft_core::project::graph::GraphView::node(project, id)
            .is_some_and(|n| n.group_boundary.is_some() && n.node_type != weft_core::project::boundary_types::LOOP_OUT)
    };
    let wires = fold
        .output_wires()
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .iter()
        .filter(|wire| !boundary(&wire.node))
        .map(|wire| weft_core::run_spec::ExpectedWire::spell(project, wire))
        .collect();
    // Every place the run covers, spelled the way the wires are: what its
    // cut runs and what its seed reused (a node in an included file once
    // per site that reaches it, a loop once).
    let nodes = weft_core::project::places::every_place(fold.project())
        .into_iter()
        .chain(fold.snapshot().inherited_origins.keys().cloned())
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<String>>();
    let mut focus = std::collections::BTreeSet::new();
    for spelled in weft_core::comma_list::comma_list(request.expect.iter()) {
        let spelling = match named_in_run(Ok::<_, &String>(project), run_id, &spelled)?.1 {
            weft_core::project::places::Named::Node(place) => weft_core::project::address_of(project, &place.id, &place.path),
            weft_core::project::places::Named::Group(place) if weft_core::project::graph::GraphView::is_loop(project, &place.id) => {
                weft_core::project::group_address(project, &place.id, &place.path)
            }
            weft_core::project::places::Named::Group(_) => {
                return Err((StatusCode::BAD_REQUEST, format!("'{spelled}' is a group, which has no outputs of its own; name a node, or a loop")));
            }
        };
        if !nodes.contains(&spelling) {
            return Err((StatusCode::BAD_REQUEST, format!("'{spelled}' did not run in run {run_id}, so it has no outputs there")));
        }
        focus.insert(spelling);
    }
    Ok(weft_core::run_spec::Expected { wires, nodes, focus: focus.into_iter().collect() })
}

/// Where one node of a run stands, the node spelled the way `--from` takes
/// it (`one.render`).
pub async fn node_state(state: &DispatcherState, actor: &Actor, run_id: RunId, node: &str) -> Result<NodeState, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    crate::node_state::read(state, owner.project_id, run_id, node).await
}

/// A JSON answer written as it is read: its body's chunks, in order. A
/// chunk that is an error cuts the body short, which its reader meets as
/// an answer that did not finish (the status went out with the first
/// chunk).
pub type Streamed = tokio::sync::mpsc::Receiver<Result<bytes::Bytes, std::io::Error>>;

/// How many chunks of a streamed answer wait for a slow reader before the
/// read of the record pauses for it.
const STREAM_AHEAD: usize = 16;

/// How the events of a run's history name the values they show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Values {
    /// By where the record keeps each (`weft_core::record_ref::ValueRef`):
    /// the editor, which fetches a value when it shows it.
    ByReference,
    /// Written in: a reader that wants the values (`weft freeze`, the CLI
    /// following a run).
    Written,
}

impl Values {
    /// `with_values=true` on a URL asks for the values written in.
    pub fn asked(with_values: Option<bool>) -> Self {
        if with_values == Some(true) { Self::Written } else { Self::ByReference }
    }
}

/// `event` as its reader asked for it ([`Values`]).
pub fn encoded(event: weft_core::live_event::ShownEvent, values: Values) -> anyhow::Result<Vec<u8>> {
    let crate::events::IdentifiedEvent { event_id, event } = event;
    Ok(match values {
        Values::ByReference => serde_json::to_vec(&crate::events::IdentifiedEvent { event_id, event: event.by_reference() })?,
        Values::Written => serde_json::to_vec(&crate::events::IdentifiedEvent { event_id, event: written(event)? })?,
    })
}

/// `event` with every value it names written in, each read off the
/// projection that painted it, which holds them all.
fn written(event: weft_core::live_event::DispatcherEvent<weft_core::record_ref::Shown>) -> anyhow::Result<DispatcherEvent<serde_json::Value>> {
    event
        .with_values(&mut |at| Err(weft_core::record_ref::Unresolved::Broken(format!("the projection holds no value at {at:?}"))))
        .map_err(anyhow::Error::from)
}

/// A JSON array written to a [`Streamed`] answer an element at a time:
/// a page of elements as a run's record is painted, then what comes
/// after.
struct JsonArray {
    tx: tokio::sync::mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
    values: Values,
    started: bool,
}

impl JsonArray {
    fn chunk(&mut self, event: weft_core::live_event::ShownEvent) -> anyhow::Result<bytes::Bytes> {
        let mut chunk = Vec::new();
        chunk.push(if self.started { b',' } else { b'[' });
        self.started = true;
        chunk.extend(encoded(event, self.values)?);
        Ok(chunk.into())
    }

    /// The array's end: `]`, or `[]` for one that got no element.
    fn end(&self) -> bytes::Bytes {
        bytes::Bytes::from_static(if self.started { b"]" } else { b"[]" })
    }
}

impl JsonArray {
    /// Send `page`, a run of elements, unless it is empty.
    async fn send(&self, page: Vec<u8>) -> anyhow::Result<()> {
        if page.is_empty() {
            return Ok(());
        }
        self.tx.send(Ok(page.into())).await.map_err(|_| anyhow::anyhow!("the reader left"))
    }
}

#[async_trait::async_trait]
impl crate::painting::Sink for JsonArray {
    type Page = Vec<u8>;

    fn page(&mut self, events: Vec<weft_core::live_event::ShownEvent>) -> anyhow::Result<Vec<u8>> {
        let mut page = Vec::new();
        for event in events {
            page.extend(self.chunk(event)?);
        }
        Ok(page)
    }

    async fn deliver(&mut self, page: Vec<u8>) -> anyhow::Result<()> {
        self.send(page).await
    }
}

/// Write `answer` to a [`Streamed`] answer from a task of its own, cutting
/// the body short on its error.
fn streamed<F>(answer: impl FnOnce(tokio::sync::mpsc::Sender<Result<bytes::Bytes, std::io::Error>>) -> F) -> Streamed
where
    F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(STREAM_AHEAD);
    let written = answer(tx.clone());
    tokio::spawn(async move {
        if let Err(e) = written.await {
            tracing::warn!(target: "weft_dispatcher::api", error = %format!("{e:#}"), "a streamed answer was cut short");
            let _ = tx.send(Err(std::io::Error::other(format!("{e:#}")))).await;
        }
    });
    rx
}

/// A run's journaled history through the same projection as live events,
/// as a JSON array written as the record is read. Terminals come from the
/// journal, never a synthesized second completion; a run parked now ends
/// its replay with `RunParked`, the one event about where it stands rather
/// than what it did. Its values named as `values` says.
pub async fn replay(state: &DispatcherState, actor: &Actor, run_id: RunId, values: Values) -> Result<Streamed, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    let project_id = owner.project_id;
    // Only the database itself fails the read; a program that cannot be
    // found paints the run as unreadable at its first row, the way the
    // live view does for the same record, naming `weft clean`.
    let projector = crate::painting::open(state, run_id, project_id).await.map_err(failed("replay: look up the run's program"))?;
    let state = state.clone();
    Ok(streamed(move |tx| async move {
        let array = JsonArray { tx: tx.clone(), values, started: false };
        let (_, mut array) = crate::painting::paint(&*state.runs, run_id, crate::painting::Painting::new(projector), array).await?;
        if let Some(event) = crate::painting::parked_now(&state, run_id, project_id).await? {
            let chunk = array.chunk(event)?;
            tx.send(Ok(chunk)).await.map_err(|_| anyhow::anyhow!("the reader left"))?;
        }
        tx.send(Ok(array.end())).await.map_err(|_| anyhow::anyhow!("the reader left"))?;
        Ok(())
    }))
}

/// A run's events the way a person reads them (`weft events`): the
/// replay's rows with their values written in, kept by `request` and
/// spelled, both against the program the run was started on, never
/// whatever program its project holds now. So a node renamed or moved
/// since is found by the name it had in that run, and its rows print under
/// that name. A name that program does not have is refused naming the
/// run. When the program cannot be read, the rows name their nodes by id
/// and say why, and only a node filter, which cannot be read without it,
/// is refused. Written as the record is read: `{"events":[...],"total":N}`,
/// with `"unspelled"` last when the rows name nodes by id.
pub async fn events(state: &DispatcherState, actor: &Actor, run_id: RunId, request: EventsRequest) -> Result<Streamed, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    let lookup = crate::projection::registered_program(state, run_id, &owner).await.map_err(failed("read the run's program"))?;
    let writer = EventsWriter::new(run_id, &lookup, request)?;
    let projector = crate::painting::open(state, run_id, owner.project_id).await.map_err(failed("events: look up the run's program"))?;
    let state = state.clone();
    let project_id = owner.project_id;
    Ok(streamed(move |tx| async move {
        tx.send(Ok(bytes::Bytes::from_static(b"{\"events\":"))).await.map_err(|_| anyhow::anyhow!("the reader left"))?;
        let sink = EventsSink { writer, array: JsonArray { tx: tx.clone(), values: Values::Written, started: false } };
        let (_, EventsSink { mut writer, mut array }) = crate::painting::paint(&*state.runs, run_id, crate::painting::Painting::new(projector), sink).await?;
        let parked = crate::painting::parked_now(&state, run_id, project_id).await?;
        for row in writer.rows(parked.into_iter().collect())? {
            let chunk = array.chunk_row(&row)?;
            tx.send(Ok(chunk)).await.map_err(|_| anyhow::anyhow!("the reader left"))?;
        }
        let mut tail = array.end().to_vec();
        tail.extend(format!(",\"total\":{}", writer.total).into_bytes());
        if let Some(why) = &writer.unspelled {
            tail.extend(b",\"unspelled\":");
            tail.extend(serde_json::to_vec(why)?);
        }
        tail.push(b'}');
        tx.send(Ok(tail.into())).await.map_err(|_| anyhow::anyhow!("the reader left"))?;
        Ok(())
    }))
}

/// [`events`] for a program's call (`weft.runs.events`): the same rows,
/// collected into one answer. A program's call is answered whole, and the
/// node that asked holds the answer whole, so collecting it here holds no
/// more than the asker does.
async fn events_answered(state: &DispatcherState, actor: &Actor, run_id: RunId, request: EventsRequest) -> Result<RunEvents, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    let lookup = crate::projection::registered_program(state, run_id, &owner).await.map_err(failed("read the run's program"))?;
    let writer = EventsWriter::new(run_id, &lookup, request)?;
    let projector = crate::painting::open(state, run_id, owner.project_id).await.map_err(failed("events: look up the run's program"))?;
    let collect = Collected { writer, rows: Vec::new() };
    let (_, Collected { mut writer, mut rows }) = crate::painting::paint(&*state.runs, run_id, crate::painting::Painting::new(projector), collect)
        .await
        .map_err(failed("events: read the run's record"))?;
    let parked = crate::painting::parked_now(state, run_id, owner.project_id).await.map_err(failed("events: read the run's status"))?;
    rows.extend(writer.rows(parked.into_iter().collect()).map_err(failed("events: an event"))?);
    Ok(RunEvents { total: writer.total, events: rows, unspelled: writer.unspelled })
}

impl JsonArray {
    /// [`Self::chunk`] for a row already written out.
    fn chunk_row(&mut self, row: &serde_json::Value) -> anyhow::Result<bytes::Bytes> {
        let mut chunk = Vec::new();
        chunk.push(if self.started { b',' } else { b'[' });
        self.started = true;
        chunk.extend(serde_json::to_vec(row)?);
        Ok(chunk.into())
    }
}

/// [`events`]' rows going out as they are painted.
struct EventsSink {
    writer: EventsWriter,
    array: JsonArray,
}

#[async_trait::async_trait]
impl crate::painting::Sink for EventsSink {
    type Page = Vec<u8>;

    fn page(&mut self, events: Vec<weft_core::live_event::ShownEvent>) -> anyhow::Result<Vec<u8>> {
        let mut page = Vec::new();
        for row in self.writer.rows(events)? {
            page.extend(self.array.chunk_row(&row)?);
        }
        Ok(page)
    }

    async fn deliver(&mut self, page: Vec<u8>) -> anyhow::Result<()> {
        self.array.send(page).await
    }
}

/// [`events_answered`]' rows, collected.
struct Collected {
    writer: EventsWriter,
    rows: Vec<serde_json::Value>,
}

#[async_trait::async_trait]
impl crate::painting::Sink for Collected {
    type Page = ();

    fn page(&mut self, events: Vec<weft_core::live_event::ShownEvent>) -> anyhow::Result<()> {
        self.rows.extend(self.writer.rows(events)?);
        Ok(())
    }

    async fn deliver(&mut self, (): ()) -> anyhow::Result<()> {
        Ok(())
    }
}

/// What [`events`] keeps and how it spells it, a batch of rows at a time:
/// pure, so how a name is read against the run's own program is pinned
/// without a database.
struct EventsWriter {
    filter: weft_core::live_event::spelled::EventsFilter,
    /// The run's program, every row spelled through it; `None` when it
    /// cannot be read.
    program: Option<std::sync::Arc<weft_core::ProjectDefinition>>,
    unspelled: Option<String>,
    /// How many events the run has, before the filters.
    total: usize,
}

impl EventsWriter {
    fn new(run_id: RunId, lookup: &crate::projection::ProgramLookup, request: EventsRequest) -> Result<Self, Refused> {
        use weft_core::live_event::spelled::{parse_iteration, EventsFilter};
        let program = lookup.or_why(run_id);
        let iteration =
            request.iteration.as_deref().map(parse_iteration).transpose().map_err(|why| (StatusCode::BAD_REQUEST, why))?.unwrap_or_default();
        let filter = match &request.node {
            Some(spelled) => {
                let graph = program.as_deref().map(weft_core::project::graph::ProjectGraph::new);
                let (graph, named) = named_in_run(graph.as_ref().map_err(|why| *why), run_id, spelled)?;
                EventsFilter::at(graph, named, request.kind, iteration)
            }
            None => EventsFilter::anywhere(request.kind, iteration),
        }
        .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
        let unspelled = lookup.unpaintable().map(|(_, why)| why.to_string());
        Ok(Self { filter, program: program.ok(), unspelled, total: 0 })
    }

    /// The rows `events` are, values written in and spelled, that the
    /// filter keeps. The program is indexed once per batch: every row is
    /// spelled through it, so a lookup per row is constant time, not a
    /// scan of the program.
    fn rows(&mut self, events: Vec<weft_core::live_event::ShownEvent>) -> anyhow::Result<Vec<serde_json::Value>> {
        let graph = self.program.as_deref().map(weft_core::project::graph::ProjectGraph::new);
        let mut rows = Vec::new();
        for crate::events::IdentifiedEvent { event_id, event } in events {
            self.total += 1;
            let event = written(event)?;
            let wire = serde_json::to_value(crate::events::IdentifiedEvent { event_id, event: &event })?;
            if !self.filter.keeps(&event, &wire) {
                continue;
            }
            match &graph {
                Some(graph) => rows.extend(weft_core::live_event::spelled::spell_row(wire, graph)),
                None => rows.push(wire),
            }
        }
        Ok(rows)
    }
}

/// How many references one values request may name: an editor asks for
/// the values one screen shows.
const MAX_VALUE_REFS: usize = 4096;

/// How deep one reference may nest parts and lists: a loop's gathered list
/// of parts of rows is three deep, and loops inside loops add one each.
const MAX_VALUE_REF_DEPTH: usize = 32;

/// Refuse a values request naming more references than
/// [`MAX_VALUE_REFS`], or one nesting deeper than [`MAX_VALUE_REF_DEPTH`]:
/// checked before anything walks the references or reads the database.
fn within_value_limits(refs: &[weft_core::record_ref::ValueRef]) -> Result<(), Refused> {
    if refs.len() > MAX_VALUE_REFS {
        return Err((StatusCode::BAD_REQUEST, format!("a values request names {} references; at most {MAX_VALUE_REFS} are read at once", refs.len())));
    }
    if let Some(at) = refs.iter().position(|at| at.deeper_than(MAX_VALUE_REF_DEPTH)) {
        return Err((StatusCode::BAD_REQUEST, format!("reference {at} nests deeper than {MAX_VALUE_REF_DEPTH} parts and lists")));
    }
    Ok(())
}

/// The values some events of run `run_id` name (`POST /runs/{id}/values`),
/// each read off the record row that keeps it, or said gone when the
/// run's record no longer exists. A reference may read another run's record
/// (a seeded run's inherited value is kept in the run it came from), of
/// the same project only. Only the rows the references name are read.
pub async fn values(state: &DispatcherState, actor: &Actor, run_id: RunId, refs: Vec<weft_core::record_ref::ValueRef>) -> Result<weft_core::program::ValuesAnswer, Refused> {
    use std::collections::{BTreeMap, BTreeSet};
    use weft_core::record_ref::ValueRef;
    within_value_limits(&refs)?;
    let owner = authorize(state, actor, run_id).await?;
    // Every row each run is read at, and whether its birth is.
    let mut rows: BTreeMap<RunId, BTreeSet<i32>> = BTreeMap::new();
    let mut births: BTreeSet<RunId> = BTreeSet::new();
    fn bases(at: &ValueRef, rows: &mut BTreeMap<RunId, BTreeSet<i32>>, births: &mut BTreeSet<RunId>) {
        match at {
            ValueRef::Row { run, seq, .. } => {
                rows.entry(*run).or_default().insert(*seq);
            }
            ValueRef::Given { run, .. } => {
                births.insert(*run);
            }
            ValueRef::Part { of, .. } => bases(of, rows, births),
            ValueRef::List { items } => items.iter().for_each(|item| bases(item, rows, births)),
            ValueRef::Written { .. } => {}
        }
    }
    refs.iter().for_each(|at| bases(at, &mut rows, &mut births));
    let runs: BTreeSet<RunId> = rows.keys().copied().chain(births.iter().copied()).collect();
    // The runs whose record exists: a row missing from one of them is a
    // reference the record disagrees with, a run erased is gone.
    let mut existing: BTreeSet<RunId> = [run_id].into();
    for other in runs.iter().filter(|other| **other != run_id) {
        // One of another project is never read.
        match state.runs.run_owner(*other).await.map_err(failed("values: read a run's row"))? {
            Some(found) if found.project_id != owner.project_id => {
                return Err((StatusCode::NOT_FOUND, format!("a value names run {other}, which is not of the project run {run_id} is")));
            }
            Some(_) => {
                existing.insert(*other);
            }
            None => {}
        }
    }
    // What each row kept, by its place, and what each birth was given.
    let mut kept: std::collections::HashMap<(RunId, i32, u32), weft_journal::RunEvent> = std::collections::HashMap::new();
    for (run, seqs) in rows {
        let seqs: Vec<i32> = seqs.into_iter().collect();
        let record = state.runs.record_rows(run, &seqs).await.map_err(failed("values: read the record"))?;
        let decoded = weft_journal::on_blocking_pool(move || record.decode(run)).await.map_err(|why| (StatusCode::INTERNAL_SERVER_ERROR, why))?;
        kept.extend(decoded.into_iter().map(|row| ((run, row.seq, row.index), row.event)));
    }
    let mut born: std::collections::HashMap<RunId, weft_journal::RunEvent> = std::collections::HashMap::new();
    for run in births {
        let birth = crate::projection::run_birth(state, run).await.map_err(failed("values: read a run's birth"))?.map_err(|why| (StatusCode::INTERNAL_SERVER_ERROR, why))?;
        if let Some(birth) = birth {
            born.insert(run, birth.event);
        }
    }
    Ok(weft_core::program::ValuesAnswer { values: resolved(&refs, &existing, &kept, &born) })
}

/// What each of `refs` reads, given the runs whose record exists
/// (`existing`), the events the record rows they name keep (`kept`, by run
/// and place) and the births of the runs they name a given value of
/// (`born`). A run whose record is not there is gone; a row or a birth
/// missing from a record that is there is a reference the record disagrees
/// with. Pure, so how a reference reads is pinned without a database.
fn resolved(
    refs: &[weft_core::record_ref::ValueRef],
    existing: &std::collections::BTreeSet<RunId>,
    kept: &std::collections::HashMap<(RunId, i32, u32), weft_journal::RunEvent>,
    born: &std::collections::HashMap<RunId, weft_journal::RunEvent>,
) -> Vec<weft_core::program::KeptValue> {
    use weft_core::program::KeptValue;
    use weft_core::record_ref::{Unresolved, ValueRef};
    // `None` only for a run whose record is gone; a run that exists and
    // misses what the reference names is broken.
    let missing = |run: &RunId, what: String| -> Result<Option<std::sync::Arc<serde_json::Value>>, Unresolved> {
        if existing.contains(run) { Err(Unresolved::Broken(what)) } else { Ok(None) }
    };
    let mut read = |at: &ValueRef| -> Result<Option<std::sync::Arc<serde_json::Value>>, Unresolved> {
        match at {
            ValueRef::Row { run, seq, index } => match kept.get(&(*run, *seq, *index)) {
                None => missing(run, format!("run {run}'s record has no event at {seq}:{index}")),
                Some(event) => Ok(Some(event.value_kept().ok_or_else(|| Unresolved::Broken(format!("the event at {seq}:{index} of run {run} keeps no value")))?)),
            },
            ValueRef::Given { run, place, port, side } => match born.get(run) {
                None => missing(run, format!("run {run}'s record has no birth")),
                Some(birth) => Ok(Some(birth.given_value(place, port, *side).ok_or_else(|| Unresolved::Broken(format!("run {run} was given nothing at {place}'s '{port}'")))?)),
            },
            other => Err(Unresolved::Broken(format!("{other:?} is read through what it is made of"))),
        }
    };
    refs.iter()
        .map(|at| match at.resolve(&mut read) {
            Ok(value) => KeptValue::Value { value: std::sync::Arc::unwrap_or_clone(value) },
            Err(gone @ Unresolved::Gone(_)) => KeptValue::Gone { gone: format!("{gone}: its keep time ran out, or it was cleaned") },
            // A part the value does not have (a reference the editor took
            // off another, a key the wire found absent), or a record and a
            // reference that disagree: said for this value alone.
            Err(Unresolved::Broken(broken)) => KeptValue::Broken { broken },
        })
        .collect()
}

/// What `spelled` names in the program `run_id` was started on
/// (`weft_core::project::places::named`): a 400 naming the run for a name
/// that program does not have (the name a person has now may not be the
/// one it had then), a 409 when the program cannot be read. Answers with
/// the program it was read in.
fn named_in_run<'g, G: weft_core::project::graph::GraphView>(
    program: Result<&'g G, &String>,
    run_id: RunId,
    spelled: &str,
) -> Result<(&'g G, weft_core::project::places::Named), Refused> {
    let program = program.map_err(|why| {
        (StatusCode::CONFLICT, format!("'{spelled}' cannot be read: names are read against the program run {run_id} ran, and {why}"))
    })?;
    weft_core::project::places::named(program, spelled)
        .map(|named| (program, named))
        .map_err(|why| (StatusCode::BAD_REQUEST, format!("in the program run {run_id} ran: {why}")))
}

// ----- a project's runs ----------------------------------------------------

/// THE runs listing (`weft runs`, the editor's Runs view, `ctx.runs()
/// .list(..)`): the newest runs `request` reaches, in `project` or (an
/// operator only, `project` absent) every project of the actor's owner,
/// with how many it reaches in all. A run parked on a wait reads
/// `waiting_for_input`. A page outside 1 to [`MAX_RUNS_PAGE`] is refused,
/// never quietly cut, so a caller cannot read a short page as the whole.
pub async fn list(state: &DispatcherState, actor: &Actor, project: Option<uuid::Uuid>, request: &RunList) -> Result<RunPage, Refused> {
    if let Some(project) = project {
        actor.may_act_on(state, project).await?;
    }
    if !(1..=MAX_RUNS_PAGE).contains(&request.limit) {
        return Err((StatusCode::BAD_REQUEST, format!("a runs listing takes 1 to {MAX_RUNS_PAGE} runs, not {}", request.limit)));
    }
    let query = list_query(project, request, crate::lease::now_unix() as u64);
    state.runs.list_runs(actor.tenant().as_str(), &query).await.map_err(failed("list runs"))
}

/// How many runs `filter` reaches in `project`.
pub async fn count(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, filter: &RunFilter) -> Result<u64, Refused> {
    actor.may_act_on(state, project).await?;
    let query = crate::api::run::run_query(Some(project), filter, 0, crate::lease::now_unix() as u64);
    Ok(state.runs.list_runs(actor.tenant().as_str(), &query).await.map_err(failed("count runs"))?.total)
}

/// What a clean did, and whether it stops the asking run (a program's
/// clean whose filters reach it, with `StopSelf::Include`).
pub struct Cleaned {
    pub outcome: CleanOutcome,
    pub stops_asker: bool,
}

/// Delete every run the request reaches, in its project (or every project
/// of the actor's owner). A run still going is never deleted from under
/// itself: `cancel` stops it (its rows go with the next clean), `leave`
/// leaves it running. The asking run is never touched; with
/// `StopSelf::Include`, and when the filters reach it, it is stopped once
/// the rest is done. Each deleted project's bare versions are swept as its
/// runs go, the way `weft clean <run_id>` sweeps them.
pub async fn clean(state: &DispatcherState, actor: &Actor, request: &CleanRequest, stop_self: weft_core::StopSelf) -> Result<Cleaned, Refused> {
    if let Some(project) = request.project {
        actor.may_act_on(state, project).await?;
    }
    let asker_matches = asker_matches_filter(state, request.project, &request.filter, actor).await?;
    let outcome = clean_runs(state, actor, request.project, &request.filter, request.running).await?;
    let stops_asker = stop_self == weft_core::StopSelf::Include && asker_matches;
    super::stop_asker(state, actor, stops_asker).await?;
    Ok(Cleaned { outcome, stops_asker })
}

async fn clean_runs(
    state: &DispatcherState,
    actor: &Actor,
    project: Option<uuid::Uuid>,
    filter: &RunFilter,
    running: StillGoing,
) -> Result<CleanOutcome, Refused> {
    let cancel_going = running == StillGoing::Cancel;
    let query = crate::api::run::run_query(project, filter, MAX_RUNS_PAGE, crate::lease::now_unix() as u64);
    let mut outcome = CleanOutcome::default();
    // The pages walk back in time from a bound fixed now (a run that
    // starts while this cleans is never reached), each page starting
    // strictly after the last run the one before it listed. Whatever the
    // walk deletes, cancels or leaves behind itself, the next page is
    // exactly the runs it has not reached yet.
    let mut below = None;
    let mut swept_projects: std::collections::BTreeSet<uuid::Uuid> = Default::default();
    loop {
        let page = state.runs.list_runs(actor.tenant().as_str(), &RunListQuery { below, ..query.clone() }).await.map_err(failed("list runs"))?;
        let Some(last) = page.runs.last().map(|run| (run.started_at, run.run_id)) else {
            break;
        };
        below = Some(last);
        for run in page.runs {
            if Some(run.run_id) == actor.run() {
                continue;
            }
            if run.status == RunStatus::Running {
                if cancel_going {
                    cancel_run_id(state, run.run_id, &weft_core::run::CancelCause::User).await.map_err(failed("cancel"))?;
                    outcome.cancelled += 1;
                } else {
                    outcome.left_running += 1;
                }
                continue;
            }
            if filter.status.is_some_and(|status| !status.reaches(run.status)) {
                continue;
            }
            let project_id = clean_run(state, actor.tenant(), run.run_id)
                .await
                .map_err(|e| (e.status(), format!("delete run {}: {}", run.run_id, e.message())))?;
            swept_projects.insert(project_id);
            outcome.deleted += 1;
        }
    }
    for project_id in swept_projects {
        if let Err((status, message)) = super::versions::sweep_bare_versions(state, project_id).await {
            tracing::warn!(
                target: "weft_dispatcher::versions",
                %project_id, %status, %message,
                "the runs are deleted; the versions they may have left bare could not be swept"
            );
        }
    }
    Ok(outcome)
}

/// Whether the asking run is one a clean of `project` by `filter` reaches
/// (a clean that includes the asker stops it rather than deleting it from
/// under itself).
async fn asker_matches_filter(state: &DispatcherState, project: Option<uuid::Uuid>, filter: &RunFilter, actor: &Actor) -> Result<bool, Refused> {
    let Some(asker) = actor.run() else { return Ok(false) };
    let Some(run) = state.runs.run_summary(asker).await.map_err(failed("the asking run"))? else {
        return Ok(false);
    };
    let now = crate::lease::now_unix() as u64;
    let tagged = match &filter.tag {
        Some(tag) => run.tags.iter().any(|t| t == tag),
        None => true,
    };
    Ok(project.is_none_or(|project| run.project_id == project)
        && filter.instance.as_ref().is_none_or(|m| run.instance.as_ref() == Some(m))
        && filter.status.is_none_or(|s| s.reaches(RunStatus::Running))
        && filter.node.as_deref().is_none_or(|n| n == run.entry_node)
        && filter.older_than_secs.is_none_or(|secs| run.started_at <= now.saturating_sub(secs))
        && tagged)
}

/// Remove one run (`weft clean <run>`), and sweep the version its removal
/// left bare.
///
/// The sweep is HERE and not in whoever asked. Deleting the last run of
/// a version is what makes that version bare, and a bare version shows
/// in `weft tree` with nothing under it and a status of `unknown`, for
/// ever, with no verb that reaches it. The CLI used to do the sweep
/// itself, which left the editor's own delete not doing it at all, and
/// the CLI could only sweep the project it was standing in, which for
/// `weft clean <run_id>` (a run can be cleaned from anywhere) was
/// frequently the wrong one.
///
/// A run still going is refused, the way a clean never deletes one from
/// under itself: its worker would go on writing the record being erased.
/// A run asking to delete itself is refused for the same reason.
pub async fn delete(state: &DispatcherState, actor: &Actor, run_id: RunId) -> Result<DeletedRun, crate::api::storage::ApiError> {
    authorize(state, actor, run_id).await?;
    if actor.run() == Some(run_id) {
        return Err((StatusCode::CONFLICT, format!("run {run_id} is the run asking; a run cannot delete its own record while it runs")).into());
    }
    let summary = state.runs.run_summary(run_id).await.map_err(failed("run summary"))?;
    if summary.is_some_and(|summary| summary.status == RunStatus::Running) {
        return Err((StatusCode::CONFLICT, format!("run {run_id} is still going; stop it first (`weft stop {run_id}`), then delete it")).into());
    }
    let project_id = clean_run(state, actor.tenant(), run_id).await?;
    // The run IS deleted, which is what was asked for, so a sweep that
    // cannot run is said out loud and does not fail the delete: the
    // reaper and the next `weft clean` both reach the same rows.
    let swept = super::versions::sweep_bare_versions(state, project_id).await.unwrap_or_else(|(status, message)| {
        tracing::warn!(
            target: "weft_dispatcher::versions",
            %run_id, project_id = %project_id, %status, %message,
            "the run is deleted; the version it may have left bare could not be swept"
        );
        Vec::new()
    });
    Ok(DeletedRun { project: project_id, swept })
}

/// Cost records of the project's runs, filtered: the rows of `run_cost`
/// (one per cost a run's record reports) the filter reaches, in the order
/// the runs started and each run's record's, every filter in the query
/// (`weft_journal::costs::list`). A cost's instance is its run's, born
/// with the run and never changed.
pub async fn costs(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, filter: &CostFilter) -> Result<Vec<CostRecord>, Refused> {
    actor.may_act_on(state, project_id).await?;
    let project = state.projects.project(project_id).await.map_err(failed("project"))?;
    weft_journal::costs::list(&state.pg_pool, project_id, project.as_ref(), filter).await.map_err(failed("costs"))
}

// ----- a program's calls on runs ------------------------------------------

/// Carry out one call of `actor` on runs other than a start
/// (`super::runs::program_call` carries that); `project` is the call's
/// project, which only the calls on a project's runs read.
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    call: &weft_core::program::runs::RunCall,
    stop_self: weft_core::StopSelf,
) -> Result<weft_core::program::ProgramCallOutcome, Refused> {
    use weft_core::program::runs::RunCall;
    match call {
        // `actions::runs` carries a start and hands every other call here.
        RunCall::Start { .. } => Err(super::internal("a run call", "a start reached the calls on existing runs")),
        RunCall::Clean { filter, running } => {
            let request = CleanRequest { project: Some(project), filter: filter.clone(), running: *running };
            let cleaned = clean(state, actor, &request, stop_self).await?;
            Ok(weft_core::program::ProgramCallOutcome { value: serde_json::to_value(cleaned.outcome).map_err(failed("answer"))?, stops_asker: cleaned.stops_asker })
        }
        RunCall::List(request) => super::answered(&list(state, actor, Some(project), request).await?),
        RunCall::Count { filter } => super::answered(&weft_core::program::RunsCounted { total: count(state, actor, project, filter).await? }),
        RunCall::Costs { filter } => super::answered(&costs(state, actor, project, filter).await?),
        RunCall::NodeState { run, node } => super::answered(&node_state(state, actor, *run, node).await?),
        RunCall::Stop { run } => {
            let stopped = stop(state, actor, *run).await?;
            // Naming its own run is how a program stops itself: the cancel
            // is issued, and its worker waits for exactly that.
            let stops_asker = stopped.already_ended.is_none() && actor.run() == Some(*run);
            Ok(weft_core::program::ProgramCallOutcome { value: serde_json::to_value(stopped).map_err(failed("answer"))?, stops_asker })
        }
        RunCall::Wake { run, node, request } => {
            wake(state, actor, *run, node, request).await?;
            super::answered(&serde_json::Value::Null)
        }
        RunCall::Logs { run, request } => super::answered(&logs(state, actor, *run, request).await?),
        RunCall::Events { run, request } => super::answered(&events_answered(state, actor, *run, request.clone()).await?),
        RunCall::Outputs { run, request } => super::answered(&outputs(state, actor, *run, request).await?),
        RunCall::Detail { run } => super::answered(&detail(state, actor, *run).await?),
        RunCall::Resolve { prefix } => super::answered(&resolve(state, actor, prefix).await?),
        RunCall::Delete { run } => {
            let deleted = delete(state, actor, *run).await.map_err(|e| (e.status(), e.message().to_string()))?;
            super::answered(&deleted)
        }
    }
}

#[cfg(test)]
mod waits_tests {
    use super::*;

    fn signal(token: &str, run_id: Option<RunId>, node: &str, is_resume: bool, kind: &str) -> crate::store::SignalRegistration {
        crate::store::SignalRegistration {
            instance: None,
            activation_trigger: None,
            source_version: None,
            setup_run_id: None,
            program: None,
            token: token.into(),
            tenant_id: "t".into(),
            project_id: uuid::Uuid::from_u128(0x100),
            run_id,
            node_id: node.into(),
            is_resume,
            spec_json: serde_json::json!({ "kind": kind }).to_string(),
            access_id: None,
            consumer_kind: None,
            tags: vec![],
            port_snapshot: None,
            consumer_payload: None,
            surface_kind: "public_entry".into(),
            mount_path: None,
            mount_methods: Vec::new(),
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
            kind_state_seq: 0,
            holds: false,
            generation: 0,
        }
    }

    /// A run's waits are its own resume signals, in order; entry
    /// signals and another run's waits are not.
    #[test]
    fn a_runs_waits_are_its_resume_signals() {
        let (mine, other) = (RunId::new_v4(), RunId::new_v4());
        let signals = vec![
            signal("e", None, "tick", false, "timer"),
            signal("a", Some(mine), "hold", true, "timer"),
            signal("b", Some(other), "review", true, "form"),
            signal("c", Some(mine), "review", true, "form"),
        ];
        let waits = waits_of(&signals, mine).unwrap();
        assert_eq!(
            waits,
            vec![
                ParkedWait { node: "hold".into(), token: "a".into(), kind: "timer".into() },
                ParkedWait { node: "review".into(), token: "c".into(), kind: "form".into() },
            ]
        );
    }

    /// A wait inside an included file was registered under its place,
    /// spelled (`sweep.key`), and goes out exactly as the row holds it:
    /// the same file under another site is another wait with another
    /// spelling, and nothing here has to tell them apart.
    #[test]
    fn a_wait_inside_an_included_file_goes_out_as_its_place() {
        let mine = RunId::new_v4();
        let signals = vec![signal("a", Some(mine), "sweep.key", true, "timer"), signal("b", Some(mine), "again.key", true, "timer")];
        let waits = waits_of(&signals, mine).unwrap();
        let out: Vec<&str> = waits.iter().map(|w| w.node.as_str()).collect();
        assert_eq!(out, vec!["sweep.key", "again.key"]);
        let wire = serde_json::to_value(&waits[0]).unwrap();
        assert_eq!(wire["node"], "sweep.key");
    }

    /// A wake picks one of several waits at one node by the iteration the
    /// run's record holds each in: the only one without asking, the one
    /// named (or the only one inside the outer iteration named), and
    /// several unnamed refused listing their iterations.
    #[test]
    fn a_wake_picks_its_wait_by_the_iteration_on_the_record() {
        let mine = RunId::new_v4();
        let waits = waits_of(&[signal("t0", Some(mine), "lp.ask", true, "timer"), signal("t1", Some(mine), "lp.ask", true, "timer")], mine).unwrap();
        let iterations: HashMap<String, Vec<u32>> = [("t0".to_string(), vec![0, 0]), ("t1".to_string(), vec![1, 2])].into();
        let pick = |waits: &[ParkedWait], wanted: Option<&[u32]>| pick_wait(waits, Some(&iterations), wanted, 2, "lp.ask", mine);
        assert_eq!(pick(&waits, Some(&[1, 2])).unwrap().token, "t1");
        assert_eq!(pick(&waits, Some(&[1])).unwrap().token, "t1");
        let (status, why) = pick(&waits, Some(&[3])).unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(why.contains("it waits in 0.0, 1.2"), "{why}");
        let (status, why) = pick(&waits, None).unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(why.contains("in iterations 0.0, 1.2; say which with `--iteration`"), "{why}");
        assert_eq!(pick_wait(&waits[..1], None, None, 2, "lp.ask", mine).unwrap().token, "t0", "one wait needs no record");
        assert_eq!(pick(&[], None).unwrap_err().0, StatusCode::NOT_FOUND);
        // A wait the record does not hold yet is listed by its token, and
        // no iteration picks it.
        let (_, why) = pick_wait(&waits, Some(&HashMap::new()), None, 2, "lp.ask", mine).unwrap_err();
        assert!(why.contains("wait t0 (not on the run's record yet)"), "{why}");
    }

    /// `--iteration` is checked against the place before any wait is
    /// read, by the same check `weft events --iteration` passes: refused
    /// at a node in no loop, and with more numbers than loops around it.
    #[test]
    fn a_wake_refuses_an_iteration_the_place_cannot_have() {
        use weft_core::live_event::spelled::check_iteration;
        use weft_core::project::places::named;
        let node = |id: &str, scope: &[&str]| {
            serde_json::json!({
                "id": id, "nodeType": "T", "label": null, "config": {},
                "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
                "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false
            })
        };
        let p: weft_core::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("plain", &[]), node("lp.ask", &["lp"])],
            "edges": [],
            "groups": [{"id": "lp", "kind": "loop", "loopConfig": {}, "nodeIds": ["lp.ask"]}]
        }))
        .unwrap();
        let check = |spelled: &str, iteration: &[u32]| check_iteration(&p, &named(&p, spelled).unwrap(), iteration);
        assert!(check("plain", &[0]).unwrap_err().contains("is in no loop"));
        assert!(check("lp.ask", &[1, 0]).unwrap_err().contains("at most 1 number,"));
        assert_eq!(check("lp.ask", &[1]), Ok(1));
        assert_eq!(check("plain", &[]), Ok(0));
    }

    /// A stop answers a run that is over with by its status, and stops
    /// one still going or parked.
    #[test]
    fn a_stop_reads_an_ended_run_as_already_ended() {
        for status in [RunStatus::Completed, RunStatus::Failed, RunStatus::Cancelled] {
            assert_eq!(already_ended(status), Some(status));
        }
        assert_eq!(already_ended(RunStatus::Running), None);
        assert_eq!(already_ended(RunStatus::WaitingForInput), None);
    }
}

#[cfg(test)]
mod wake_tier_boundary {
    /// This module's own source, read at compile time. The test module
    /// is cut off first, since the words to look for would otherwise be
    /// found in the looking.
    fn code() -> &'static str {
        include_str!("run_admin.rs").split("#[cfg(test)]").next().expect("a split always yields a first piece")
    }

    /// Waking a parked wait must name no signal kind and mint no kind's
    /// payload. Which kinds can be woken, and with what, is the
    /// listener's answer (`/wake_by_hand`), because it is the tier that
    /// owns the kinds.
    ///
    /// A source check, because putting it back is SILENT: a second
    /// wakeable kind would simply be refused with "that expects a
    /// value", which reads like a deliberate rule rather than a tier
    /// that was never told.
    #[test]
    fn waking_names_no_kind_and_mints_no_payload() {
        for needle in ["Timer", "\"timer\"", "scheduledTime", "actualTime"] {
            assert!(
                !code().contains(needle),
                "the wake action must not know a signal kind: found `{needle}`. \
                 What a wait wakes with is the kind's own shape and comes from \
                 the listener; minting one here means the next wakeable kind is \
                 quietly unwakeable."
            );
        }
    }
}

#[cfg(test)]
mod events_tests {
    use super::*;
    use crate::projection::ProgramLookup;
    use std::sync::Arc;
    use weft_core::ProjectDefinition;

    const RUN: RunId = RunId::from_u128(9);

    /// The file `setup` included at the site `setup`, its node `insights`
    /// inside the group `stages`, or (`moved`) one group deeper, inside
    /// `stages.brains`: the same program before and after a person moved
    /// the node.
    fn program(moved: bool) -> Arc<ProjectDefinition> {
        let (insights, groups) = if moved {
            (
                "@src:setup.stages.brains.insights",
                serde_json::json!([
                    {"id": "@src:setup", "kind": "body", "nodeIds": []},
                    {"id": "@src:setup.stages", "kind": "group", "nodeIds": [], "parentGroupId": "@src:setup"},
                    {"id": "@src:setup.stages.brains", "kind": "group", "nodeIds": ["@src:setup.stages.brains.insights"], "parentGroupId": "@src:setup.stages"},
                    {"id": "setup", "kind": "call", "body": "@src:setup", "nodeIds": []}
                ]),
            )
        } else {
            (
                "@src:setup.stages.insights",
                serde_json::json!([
                    {"id": "@src:setup", "kind": "body", "nodeIds": []},
                    {"id": "@src:setup.stages", "kind": "group", "nodeIds": ["@src:setup.stages.insights"], "parentGroupId": "@src:setup"},
                    {"id": "setup", "kind": "call", "body": "@src:setup", "nodeIds": []}
                ]),
            )
        };
        let scope: Vec<&str> =
            if moved { vec!["@src:setup", "@src:setup.stages", "@src:setup.stages.brains"] } else { vec!["@src:setup", "@src:setup.stages"] };
        Arc::new(
            serde_json::from_value(serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "nodes": [{
                    "id": insights, "nodeType": "T", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
                    "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false, "images": []
                }],
                "edges": [],
                "groups": groups,
            }))
            .unwrap(),
        )
    }

    /// What the run recorded, under the program it ran (`moved` false): its
    /// start, and `insights` completing under the site `setup`.
    fn rows() -> Vec<weft_core::live_event::ShownEvent> {
        let project_id = uuid::Uuid::nil();
        [
            DispatcherEvent::<serde_json::Value>::RunCompleted { run_id: RUN, project_id, outputs: None, at_unix: 1 },
            serde_json::from_value(serde_json::json!({
                "kind": "node_completed", "runId": RUN, "projectId": project_id, "node": "@src:setup.stages.insights",
                "frames": [{"site": "setup"}], "output": {"text": "hi"}, "atUnix": 1
            }))
            .unwrap(),
        ]
        .into_iter()
        .map(|event| event.map_values(&mut |value| weft_core::record_ref::Shown::of(None, &std::sync::Arc::new(value))))
        .map(crate::events::IdentifiedEvent::transient)
        .collect()
    }

    /// The answer [`events`] writes for `rows`, collected.
    fn events_answer(run_id: RunId, lookup: &ProgramLookup, rows: Vec<weft_core::live_event::ShownEvent>, request: EventsRequest) -> Result<RunEvents, Refused> {
        let mut writer = EventsWriter::new(run_id, lookup, request)?;
        let events = writer.rows(rows).map_err(failed("events"))?;
        Ok(RunEvents { total: writer.total, events, unspelled: writer.unspelled })
    }

    fn by_node(node: &str) -> EventsRequest {
        EventsRequest { node: Some(node.into()), kind: None, iteration: None }
    }

    /// A node moved since a run is found on that run by the name it had
    /// there, and its rows read under that name: the name is read against
    /// the program the run ran, never the one the project holds now.
    #[test]
    fn a_node_moved_since_is_found_by_its_old_name_on_the_old_run() {
        let ran = ProgramLookup::Found(program(false));
        let answer = events_answer(RUN, &ran, rows(), by_node("setup.stages.insights")).unwrap();
        assert_eq!(answer.total, 2);
        assert_eq!(answer.events.len(), 1, "{:?}", answer.events);
        assert_eq!(answer.events[0]["node"], "setup.stages.insights");
        // The name it has now is no name of that run's program, and the
        // refusal says whose program it was read against.
        let (status, why) = events_answer(RUN, &ran, rows(), by_node("setup.stages.brains.insights")).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(why.contains(&format!("the program run {RUN} ran")) && why.contains("unknown node"), "{why}");
        // Read against the program as it is now, the old name is what
        // misses: the reading this action never does.
        let now = ProgramLookup::Found(program(true));
        assert!(events_answer(RUN, &now, rows(), by_node("setup.stages.insights")).is_err());
    }

    /// A program that cannot be read leaves the rows by id, saying why, and
    /// refuses only a node filter, which cannot be read without it.
    #[test]
    fn an_unreadable_program_shows_ids_and_refuses_a_node_filter() {
        let gone = ProgramLookup::ProgramGone("the code this run ran is no longer recorded".into());
        let all = events_answer(RUN, &gone, rows(), EventsRequest::default()).unwrap();
        assert_eq!(all.events.len(), 2);
        assert_eq!(all.events[1]["node"], "@src:setup.stages.insights");
        assert!(all.unspelled.as_deref().is_some_and(|why| why.contains("no longer recorded")));
        let (status, _) = events_answer(RUN, &gone, rows(), by_node("setup.stages.insights")).unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
    }

    /// An iteration counts from the loops around the node named, so one
    /// asked for with no node, or at a node in no loop, is refused rather
    /// than matching every loop at once.
    #[test]
    fn an_iteration_needs_a_node_in_a_loop() {
        let ran = ProgramLookup::Found(program(false));
        let anywhere = EventsRequest { iteration: Some("3".into()), ..EventsRequest::default() };
        let (status, why) = events_answer(RUN, &ran, rows(), anywhere).unwrap_err();
        assert!(status == StatusCode::BAD_REQUEST && why.contains("needs --node"), "{why}");
        let outside = EventsRequest { iteration: Some("3".into()), ..by_node("setup.stages.insights") };
        let (status, why) = events_answer(RUN, &ran, rows(), outside).unwrap_err();
        assert!(status == StatusCode::BAD_REQUEST && why.contains("is in no loop"), "{why}");
    }
}

#[cfg(test)]
mod streamed_tests {
    use super::*;
    use std::sync::Arc;
    use weft_core::live_event::{IdentifiedEvent, ShownEvent};
    use weft_core::record_ref::{Position, Shown, ValueRef};

    const RUN: RunId = RunId::from_u128(0x77);

    fn started(seq: i32, value: serde_json::Value) -> ShownEvent {
        let kept = Arc::new(ValueRef::Row { run: RUN, seq: seq - 1, index: 0 });
        let event = DispatcherEvent::NodeStarted {
            run_id: RUN, node: "n".into(), frames: vec![],
            input: [("in".to_string(), Shown::of(Some(&kept), &Arc::new(value)))].into(),
            closed_ports: vec![], provided_ports: vec![], inherited_ports: Default::default(), inherited_from: None,
            project_id: uuid::Uuid::nil(), at_unix: 1,
        };
        IdentifiedEvent::projected(Position { seq, index: 0 }, [event]).next().unwrap()
    }

    /// An array written a chunk at a time is, byte for byte, the array
    /// written whole, by reference and with the values written in alike.
    #[test]
    fn an_array_written_in_chunks_is_the_array_written_whole() {
        let events = vec![started(1, serde_json::json!("a")), started(2, serde_json::json!({ "big": [1, 2, 3] })), started(3, serde_json::Value::Null)];
        for values in [Values::ByReference, Values::Written] {
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            let mut array = JsonArray { tx, values, started: false };
            let mut body = Vec::new();
            for event in events.clone() {
                body.extend(array.chunk(event).unwrap());
            }
            body.extend(array.end());
            let whole = match values {
                Values::ByReference => serde_json::to_vec(&events.iter().cloned().map(|e| IdentifiedEvent { event_id: e.event_id, event: e.event.by_reference() }).collect::<Vec<_>>()).unwrap(),
                Values::Written => serde_json::to_vec(&events.iter().cloned().map(|e| IdentifiedEvent { event_id: e.event_id, event: written(e.event).unwrap() }).collect::<Vec<_>>()).unwrap(),
            };
            assert_eq!(String::from_utf8(body).unwrap(), String::from_utf8(whole).unwrap(), "{values:?}");
        }
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let empty = JsonArray { tx, values: Values::ByReference, started: false };
        assert_eq!(&empty.end()[..], b"[]");
    }

    /// The values a run's events name read back off the rows that keep
    /// them, parts read off them, lists made of them; a row the record no
    /// longer holds, or a part a value does not have, said for that value
    /// alone.
    #[test]
    fn references_read_back_off_the_rows_that_keep_them() {
        use weft_core::program::KeptValue;
        let emitted = weft_journal::RunEvent::PortEmitted {
            run_id: RUN, emission_id: uuid::Uuid::nil(), node_id: "n".into(), frames: vec![], port: "out".into(),
            value: Arc::new(serde_json::json!({ "text": "hi", "items": [1, 2] })), at_unix: 1,
        };
        let kept = [((RUN, 3, 0), emitted)].into();
        let born = Default::default();
        let row = ValueRef::Row { run: RUN, seq: 3, index: 0 };
        let refs = vec![
            row.clone(),
            row.clone().part(vec![weft_core::record_ref::Step::Key("items".into()), weft_core::record_ref::Step::Item(1)]),
            ValueRef::List { items: vec![row.clone().part(vec![weft_core::record_ref::Step::Key("text".into())]), ValueRef::Written { value: Arc::new(serde_json::json!(0)) }] },
            ValueRef::Row { run: RUN, seq: 9, index: 0 },
            row.part(vec![weft_core::record_ref::Step::Key("nope".into())]),
        ];
        let existing = [RUN].into();
        let answer = resolved(&refs, &existing, &kept, &born);
        assert_eq!(answer[0], KeptValue::Value { value: serde_json::json!({ "text": "hi", "items": [1, 2] }) });
        assert_eq!(answer[1], KeptValue::Value { value: serde_json::json!(2) });
        assert_eq!(answer[2], KeptValue::Value { value: serde_json::json!(["hi", 0]) });
        assert!(matches!(&answer[3], KeptValue::Broken { broken } if broken.contains("no event at 9:0")), "a row missing from a record that exists: {:?}", answer[3]);
        assert!(matches!(&answer[4], KeptValue::Broken { broken } if broken.contains("no key 'nope'")), "{:?}", answer[4]);
        assert_eq!(serde_json::to_value(&answer[4]).unwrap()["broken"].as_str().map(|_| ()), Some(()), "a broken value says why under `broken`");
    }

    /// A row keeping a secret reads back as the secret's marker, alone or
    /// inside a value: no reader of the record ever holds its value.
    #[test]
    fn a_secret_reads_back_as_its_marker() {
        use weft_core::program::KeptValue;
        let marker = weft_core::secret::Secret::new(weft_core::secret::SecretId::mint()).to_value();
        let emitted = weft_journal::RunEvent::PortEmitted {
            run_id: RUN, emission_id: uuid::Uuid::nil(), node_id: "n".into(), frames: vec![], port: "out".into(),
            value: Arc::new(serde_json::json!({ "token": marker })), at_unix: 1,
        };
        let kept = [((RUN, 3, 0), emitted)].into();
        let row = ValueRef::Row { run: RUN, seq: 3, index: 0 };
        let refs = vec![row.clone(), row.part(vec![weft_core::record_ref::Step::Key("token".into())])];
        let answer = resolved(&refs, &[RUN].into(), &kept, &Default::default());
        assert_eq!(answer[0], KeptValue::Value { value: serde_json::json!({ "token": marker }) });
        assert_eq!(answer[1], KeptValue::Value { value: marker });
    }

    /// Only a run whose record is gone reads as gone, a row of it or its
    /// birth alike; the same references into a run that exists are broken.
    #[test]
    fn only_an_erased_run_reads_as_gone() {
        use weft_core::program::KeptValue;
        let erased = uuid::Uuid::from_u128(0xe);
        let refs = vec![
            ValueRef::Row { run: erased, seq: 1, index: 0 },
            ValueRef::Given { run: erased, place: weft_core::frames::Located::top("src"), port: "out".into(), side: weft_core::record_ref::GivenSide::Output },
            ValueRef::Given { run: RUN, place: weft_core::frames::Located::top("src"), port: "out".into(), side: weft_core::record_ref::GivenSide::Output },
        ];
        let answer = resolved(&refs, &[RUN].into(), &Default::default(), &Default::default());
        for gone in &answer[..2] {
            assert!(matches!(gone, KeptValue::Gone { gone } if gone.contains("no longer holds") && gone.contains("cleaned")), "{gone:?}");
        }
        assert!(matches!(&answer[2], KeptValue::Broken { broken } if broken.contains("has no birth")), "{:?}", answer[2]);
        assert_eq!(serde_json::to_value(&answer[0]).unwrap()["gone"].as_str().map(|_| ()), Some(()), "a gone value says why under `gone`");
    }

    /// A request naming more references than the cap, or one nesting
    /// deeper than the cap, is refused naming the limit; one at the caps
    /// is read.
    #[test]
    fn a_values_request_past_its_limits_is_refused() {
        let row = ValueRef::Row { run: RUN, seq: 1, index: 0 };
        assert!(within_value_limits(&vec![row.clone(); MAX_VALUE_REFS]).is_ok());
        let (status, why) = within_value_limits(&vec![row.clone(); MAX_VALUE_REFS + 1]).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(why.contains(&MAX_VALUE_REFS.to_string()), "{why}");
        let nested = |depth: usize| (1..depth).fold(row.clone(), |inner, _| ValueRef::List { items: vec![inner] });
        assert!(within_value_limits(&[nested(MAX_VALUE_REF_DEPTH)]).is_ok());
        let (status, why) = within_value_limits(&[row.clone(), nested(MAX_VALUE_REF_DEPTH + 1)]).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(why.contains("reference 1") && why.contains(&MAX_VALUE_REF_DEPTH.to_string()), "{why}");
    }
}
