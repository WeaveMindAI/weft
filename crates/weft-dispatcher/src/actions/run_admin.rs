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
use weft_core::instance::InstanceId;
use weft_core::program::runs::{EventsRequest, LogsRequest, OutputsRequest, RunList, RunStopped, WakeRequest};
use weft_core::program::{
    CleanOutcome, CleanRequest, CostFilter, CostRecord, DeletedRun, NodeState, ParkedWait, ResolvedRun, RunDetail, RunEvents, RunFilter, RunLogs,
    RunPage, RunStatus, RunSummary, StillGoing, MAX_RUNS_PAGE,
};
use weft_core::RunId;
use weft_task_store::db::Client as _;

use super::{failed, Actor, Refused};
use crate::api::run::{cancel_run_id, clean_run, list_query};
use crate::events::{DispatcherEvent, LiveEvent};
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

/// A run's journaled history through the same projection as live events.
/// Terminals come from the journal, never a synthesized second completion;
/// a run parked now ends its replay with `RunParked`, the one event about
/// where it stands rather than what it did.
pub async fn replay(state: &DispatcherState, actor: &Actor, run_id: RunId) -> Result<Vec<LiveEvent>, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    replay_events(state, run_id, owner.project_id).await
}

/// The rows [`replay`] answers, for a run the actor was held to.
async fn replay_events(state: &DispatcherState, run_id: RunId, project_id: uuid::Uuid) -> Result<Vec<LiveEvent>, Refused> {
    // The full record, folded over the run's program through the SAME
    // projection the live view runs (`crate::live_view`), so replay and
    // live cannot drift; bus and caller events ride along.
    // Lossy read: the inspector renders what exists, and every row
    // that no longer decodes lands below as its own JournalCorruption
    // entry, naming `weft clean`, instead of taking the response down.
    let (raw_events, unreadable_rows) = state.runs.events_log_lossy(run_id).await.map_err(failed("replay: read the journal"))?;
    // Only the database itself fails the read; a program that cannot
    // be found paints the run as unreadable, the way the live bridge
    // does for the same journal, and names `weft clean` below.
    // Everything the reader must be TOLD about this run, with the site
    // that places it on screen.
    let mut corruptions: Vec<(weft_core::primitive::CorruptionSite, String)> =
        unreadable_rows.into_iter().map(|reason| (weft_core::primitive::CorruptionSite::UndecodableRow, reason)).collect();
    let found = crate::projection::run_program(state, run_id).await.map_err(failed("replay: look up the run's program"))?;
    let program = match found.unpaintable() {
        // The reason travels with the run, at its own site: a row
        // that no longer decodes is one missing value, a missing
        // program is every value at once and belongs beside the run
        // rather than inside a node.
        Some((site, reason)) => {
            corruptions.push((site, reason.to_string()));
            found.without_reason()
        }
        None => found,
    };
    // What the run inherits is named by its birth, its first row.
    let birth: Vec<weft_journal::RunEvent> = raw_events.iter().take(1).map(|r| r.event.clone()).collect();
    let inheritance = match crate::projection::run_inheritance(state, &birth, &program).await {
        Ok(chain) => chain,
        Err(reason) => {
            corruptions.push((weft_core::primitive::CorruptionSite::UndecodableRow, reason));
            weft_journal::SeedChain::default()
        }
    };
    let mut projector = crate::projection::RunProjector::new(run_id, program, project_id).with_inheritance(inheritance);
    let mut out: Vec<LiveEvent> = weft_journal::on_blocking_pool(move || {
        let mut out = Vec::new();
        for record in raw_events {
            let projected = projector.project(&record.event);
            out.extend(crate::events::IdentifiedEvent { event_id: record.event_id, event: () }.project(|()| projected));
        }
        out
    })
    .await;
    // A run parked now says so after its history: parking writes no
    // record row, and a reader that takes the history before the live
    // events (`weft wait --waiting-for-input`) learns here that it waits.
    let summary = state.runs.run_summary(run_id).await.map_err(failed("replay: read the run's status"))?;
    if summary.is_some_and(|summary| summary.status == RunStatus::WaitingForInput) {
        out.push(crate::events::IdentifiedEvent::transient(DispatcherEvent::RunParked { run_id, project_id }));
    }
    // A row the fold could not apply is painted as its corruption, at
    // the row, by the projector; the rows that never decoded follow.
    for (site, reason) in corruptions {
        out.push(crate::events::IdentifiedEvent::transient(DispatcherEvent::JournalCorruption { run_id, project_id, site, reason }));
    }
    Ok(out)
}

/// A run's events the way a person reads them (`weft events`): the
/// replay's rows, kept by `request` and spelled, both against the program
/// the run was started on, never whatever program its project holds now.
/// So a node renamed or moved since is found by the name it had in that
/// run, and its rows print under that name. A name that program does not
/// have is refused naming the run. When the program cannot be read, the
/// rows name their nodes by id and say why, and only a node filter, which
/// cannot be read without it, is refused.
pub async fn events(state: &DispatcherState, actor: &Actor, run_id: RunId, request: EventsRequest) -> Result<RunEvents, Refused> {
    let owner = authorize(state, actor, run_id).await?;
    let lookup = crate::projection::registered_program(state, run_id, &owner).await.map_err(failed("read the run's program"))?;
    let rows = replay_events(state, run_id, owner.project_id).await?;
    events_answer(run_id, &lookup, &rows, request)
}

/// [`events`] once the run's program and rows are read: pure, so how a
/// name is read against the run's own program is pinned without a
/// database.
fn events_answer(run_id: RunId, lookup: &crate::projection::ProgramLookup, rows: &[LiveEvent], request: EventsRequest) -> Result<RunEvents, Refused> {
    use weft_core::live_event::spelled::{parse_iteration, spell_row, EventsFilter};
    let program = lookup.or_why(run_id);
    // Indexed once for the request: every row is spelled through it, so a
    // lookup per row is constant time, not a scan of the program.
    let graph = program.as_deref().map(weft_core::project::graph::ProjectGraph::new);
    let iteration =
        request.iteration.as_deref().map(parse_iteration).transpose().map_err(|why| (StatusCode::BAD_REQUEST, why))?.unwrap_or_default();
    let filter = match &request.node {
        Some(spelled) => {
            let (graph, named) = named_in_run(graph.as_ref().map_err(|why| *why), run_id, spelled)?;
            EventsFilter::at(graph, named, request.kind, iteration)
        }
        None => EventsFilter::anywhere(request.kind, iteration),
    }
    .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let mut events = Vec::new();
    for row in rows {
        let wire = serde_json::to_value(row).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("an event: {e}")))?;
        if !filter.keeps(&row.event, &wire) {
            continue;
        }
        match &graph {
            Ok(graph) => events.extend(spell_row(wire, graph)),
            Err(_) => events.push(wire),
        }
    }
    let unspelled = lookup.unpaintable().map(|(_, why)| why.to_string());
    Ok(RunEvents { total: rows.len(), events, unspelled })
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

/// Cost records of the project's runs, filtered: the `CostReported`
/// events of the records of the runs the filter can reach (its run, its
/// instance, and the runs still going or ended since its `since`). The
/// run's instance is its run row's, born with the run and never changed.
pub async fn costs(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, filter: &CostFilter) -> Result<Vec<CostRecord>, Refused> {
    actor.may_act_on(state, project_id).await?;
    let runs: Vec<(RunId, Option<String>)> = state
        .pg_pool
        .all(
            "SELECT run_id, instance_id FROM run \
             WHERE project_id = $1 \
               AND ($2::text IS NULL OR instance_id = $2) \
               AND ($3::uuid IS NULL OR run_id = $3) \
               AND ($4::bigint IS NULL OR ended_at IS NULL OR ended_at >= $4) \
             ORDER BY started_at, run_id",
            &[&project_id, &filter.instance.as_ref().map(|m| m.as_str()), &filter.run, &filter.since_unix.map(|s| s as i64)],
        )
        .await
        .map_err(failed("costs"))?;
    let project = state.projects.project(project_id).await.map_err(failed("project"))?;
    let mut out = Vec::new();
    let mut reported = Vec::new();
    for (run_id, instance) in runs {
        let record = {
            let conn = state.pg_pool.get().await.map_err(failed("costs"))?;
            weft_journal::record::read_record(&conn, run_id, None).await.map_err(failed("cost record"))?
        };
        // The run row keeps only the run's total, so the figures are
        // read off the record, decoded off the call threads, keeping
        // only its costs.
        let costs = weft_journal::on_blocking_pool(move || {
            record.events(run_id).map(|events| {
                events.into_iter().filter(|event| matches!(event, weft_journal::RunEvent::CostReported { .. })).collect::<Vec<_>>()
            })
        })
        .await
        .map_err(failed("cost record"))?;
        reported.push(costs.into_iter().map(move |event| (run_id, event, instance.clone())));
    }
    for (run_id, event, instance) in reported.into_iter().flatten() {
        let weft_journal::RunEvent::CostReported { node_id, frames, service, model, amount_usd, origin, at_unix, .. } = event else {
            continue;
        };
        if filter.since_unix.is_some_and(|since| at_unix < since) {
            continue;
        }
        let node = match &project {
            Some(project) => {
                let call_path: Vec<String> = weft_core::frames::call_path(&frames).into_iter().map(str::to_string).collect();
                weft_core::project::address_of(project, &node_id, &call_path)
            }
            None => node_id,
        };
        if filter.node.as_deref().is_some_and(|n| n != node) || filter.service.as_deref().is_some_and(|s| s != service) {
            continue;
        }
        if filter.paid_by.as_ref().is_some_and(|paid_by| !paid_by.covers(&origin)) {
            continue;
        }
        out.push(CostRecord {
            run: run_id,
            instance: instance.map(InstanceId::new).transpose().map_err(failed("cost row instance"))?,
            node,
            service,
            model,
            amount_usd,
            paid_by: origin,
            at_unix,
        });
    }
    Ok(out)
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
        RunCall::Events { run, request } => super::answered(&events(state, actor, *run, request.clone()).await?),
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
    fn rows() -> Vec<LiveEvent> {
        let project_id = uuid::Uuid::nil();
        [
            DispatcherEvent::RunCompleted { run_id: RUN, project_id, outputs: serde_json::Value::Null, at_unix: 1 },
            serde_json::from_value(serde_json::json!({
                "kind": "node_completed", "runId": RUN, "projectId": project_id, "node": "@src:setup.stages.insights",
                "frames": [{"site": "setup"}], "output": {"text": "hi"}, "atUnix": 1
            }))
            .unwrap(),
        ]
        .into_iter()
        .map(crate::events::IdentifiedEvent::transient)
        .collect()
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
        let answer = events_answer(RUN, &ran, &rows(), by_node("setup.stages.insights")).unwrap();
        assert_eq!(answer.total, 2);
        assert_eq!(answer.events.len(), 1, "{:?}", answer.events);
        assert_eq!(answer.events[0]["node"], "setup.stages.insights");
        // The name it has now is no name of that run's program, and the
        // refusal says whose program it was read against.
        let (status, why) = events_answer(RUN, &ran, &rows(), by_node("setup.stages.brains.insights")).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(why.contains(&format!("the program run {RUN} ran")) && why.contains("unknown node"), "{why}");
        // Read against the program as it is now, the old name is what
        // misses: the reading this action never does.
        let now = ProgramLookup::Found(program(true));
        assert!(events_answer(RUN, &now, &rows(), by_node("setup.stages.insights")).is_err());
    }

    /// A program that cannot be read leaves the rows by id, saying why, and
    /// refuses only a node filter, which cannot be read without it.
    #[test]
    fn an_unreadable_program_shows_ids_and_refuses_a_node_filter() {
        let gone = ProgramLookup::ProgramGone("the code this run ran is no longer recorded".into());
        let all = events_answer(RUN, &gone, &rows(), EventsRequest::default()).unwrap();
        assert_eq!(all.events.len(), 2);
        assert_eq!(all.events[1]["node"], "@src:setup.stages.insights");
        assert!(all.unspelled.as_deref().is_some_and(|why| why.contains("no longer recorded")));
        let (status, _) = events_answer(RUN, &gone, &rows(), by_node("setup.stages.insights")).unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
    }

    /// An iteration counts from the loops around the node named, so one
    /// asked for with no node, or at a node in no loop, is refused rather
    /// than matching every loop at once.
    #[test]
    fn an_iteration_needs_a_node_in_a_loop() {
        let ran = ProgramLookup::Found(program(false));
        let anywhere = EventsRequest { iteration: Some("3".into()), ..EventsRequest::default() };
        let (status, why) = events_answer(RUN, &ran, &rows(), anywhere).unwrap_err();
        assert!(status == StatusCode::BAD_REQUEST && why.contains("needs --node"), "{why}");
        let outside = EventsRequest { iteration: Some("3".into()), ..by_node("setup.stages.insights") };
        let (status, why) = events_answer(RUN, &ran, &rows(), outside).unwrap_err();
        assert!(status == StatusCode::BAD_REQUEST && why.contains("is in no loop"), "{why}");
    }
}
