//! The run routes the CLI and the editor reach: each parses, holds the
//! caller as an [`Actor::Operator`] and calls its action
//! (`crate::actions::run_admin`), which holds every gate, so a program's
//! `ctx` gets the same answers and refusals. Below them, the run
//! mechanisms every tier shares: THE cancel ([`cancel_run_id`]), THE
//! removal of one run ([`clean_run`]) and THE reading of a listing as a
//! store query ([`list_query`]).

use std::sync::Arc;

use weft_core::ProjectDefinition;

use axum::{extract::{Path, Query, State}, http::StatusCode, Json};
use serde::Deserialize;

use weft_core::program::runs::{EventsRequest, LogsRequest, OutputsRequest, PageMark, RunList, WakeRequest};
use weft_core::program::{DeletedRun, ResolvedRun, RunDetail, RunPage};
use weft_core::run::CancelCause;
use weft_core::RunId;
use weft_task_store::db::Client as _;

use crate::actions::{run_admin, Actor};
use crate::authenticator::{authorize_run, CallerTenant};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;
use crate::store::RunListQuery;

type Refused = (StatusCode, String);

/// A run id from its path segment.
fn run_id(raw: &str) -> Result<RunId, Refused> {
    raw.parse().map_err(|_| (StatusCode::BAD_REQUEST, format!("'{raw}' is not a run id (a uuid)")))
}

/// `GET /runs/resolve/{prefix}`.
pub async fn resolve_run_id(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(prefix): Path<String>,
) -> Result<Json<ResolvedRun>, Refused> {
    run_admin::resolve(&state, &Actor::operator(caller.0), &prefix).await.map(Json)
}

/// `POST /runs/{id}/cancel` (`weft stop`): a run that already ended is a
/// 409 naming its status.
pub async fn cancel(State(state): State<DispatcherState>, caller: CallerTenant, Path(raw): Path<String>) -> Result<StatusCode, Refused> {
    let run_id = run_id(&raw)?;
    let stopped = run_admin::stop(&state, &Actor::operator(caller.0), run_id).await?;
    match stopped.already_ended {
        Some(status) => Err((StatusCode::CONFLICT, format!("run {run_id} already ended ({})", status.as_str()))),
        None => Ok(StatusCode::NO_CONTENT),
    }
}

/// `GET /runs/{id}`.
pub async fn get(State(state): State<DispatcherState>, caller: CallerTenant, Path(raw): Path<String>) -> Result<Json<RunDetail>, Refused> {
    run_admin::detail(&state, &Actor::operator(caller.0), run_id(&raw)?).await.map(Json)
}

/// `GET /runs/{id}/logs`.
pub async fn list_logs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(raw): Path<String>,
    Query(request): Query<LogsRequest>,
) -> Result<Json<weft_core::program::RunLogs>, Refused> {
    run_admin::logs(&state, &Actor::operator(caller.0), run_id(&raw)?, &request).await.map(Json)
}

/// `GET /runs/{id}/outputs`: complete ordered output evidence, including
/// streams and inherited results.
pub async fn outputs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(raw): Path<String>,
    Query(request): Query<OutputsRequest>,
) -> Result<Json<weft_core::run_spec::Expected>, Refused> {
    run_admin::outputs(&state, &Actor::operator(caller.0), run_id(&raw)?, &request).await.map(Json)
}

/// `GET /runs/{id}/nodes/{node}/state`.
pub async fn node_state(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((raw, node)): Path<(String, String)>,
) -> Result<Json<weft_core::program::NodeState>, Refused> {
    run_admin::node_state(&state, &Actor::operator(caller.0), run_id(&raw)?, &node).await.map(Json)
}

/// `GET /runs/{id}/replay`.
pub async fn replay(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(raw): Path<String>,
) -> Result<Json<Vec<crate::events::LiveEvent>>, Refused> {
    run_admin::replay(&state, &Actor::operator(caller.0), run_id(&raw)?).await.map(Json)
}

/// `GET /runs/{id}/events` (`weft events`).
pub async fn events(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(raw): Path<String>,
    Query(request): Query<EventsRequest>,
) -> Result<Json<weft_core::program::RunEvents>, Refused> {
    run_admin::events(&state, &Actor::operator(caller.0), run_id(&raw)?, request).await.map(Json)
}

/// `POST /runs/{id}/wake/{node}` (`weft wake`): answers how the fire was
/// taken.
pub async fn wake(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((raw, node)): Path<(String, String)>,
    Query(request): Query<WakeRequest>,
) -> Result<StatusCode, Refused> {
    run_admin::wake(&state, &Actor::operator(caller.0), run_id(&raw)?, &node, &request).await
}

/// `GET /runs`: the URL spelling of a [`RunList`] (query parameters are
/// snake_case), plus the project. Every parameter is optional; `limit`
/// is a page when absent.
// SYNC: ListRunsParams <-> weft_core::program::runs::RunList
#[derive(Debug, Deserialize)]
pub struct ListRunsParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    /// Only this project's runs; every project of the caller's when absent.
    pub project_id: Option<uuid::Uuid>,
    pub started_after: Option<u64>,
    pub started_before: Option<u64>,
    pub phase: Option<weft_core::context::Phase>,
    /// The node that started the run (`RunFilter::node`).
    pub entry_node: Option<String>,
    /// Any other word is a 400 before anything is read.
    pub status: Option<weft_core::program::RunStatus>,
    pub instance: Option<weft_core::instance::InstanceId>,
    pub tag: Option<String>,
    /// A node that fired in the run (`RunList::through`).
    pub node: Option<String>,
    pub search: Option<String>,
    /// The page after the run that started at `before_started` with id
    /// `before_run` (`RunList::after`): both or neither.
    pub before_started: Option<u64>,
    pub before_run: Option<RunId>,
}

const DEFAULT_PAGE: u32 = 50;

impl ListRunsParams {
    fn into_list(self) -> Result<(Option<uuid::Uuid>, RunList), Refused> {
        let after = match (self.before_started, self.before_run) {
            (Some(started_at), Some(run)) => Some(PageMark { started_at, run }),
            (None, None) => None,
            _ => return Err((StatusCode::BAD_REQUEST, "`before_started` and `before_run` go together: the last run of the page before".into())),
        };
        let filter = weft_core::program::RunFilter {
            instance: self.instance,
            status: self.status,
            node: self.entry_node,
            tag: self.tag,
            older_than_secs: None,
        };
        let list = RunList {
            filter,
            limit: self.limit.unwrap_or(DEFAULT_PAGE),
            offset: self.offset.unwrap_or(0),
            phase: self.phase,
            started_after: self.started_after,
            started_before: self.started_before,
            through: self.node,
            search: self.search,
            after,
        };
        Ok((self.project_id, list))
    }
}

/// `GET /runs` (`weft runs`, the editor's Runs view).
pub async fn list_runs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Query(params): Query<ListRunsParams>,
) -> Result<Json<RunPage>, Refused> {
    let (project, list) = params.into_list()?;
    run_admin::list(&state, &Actor::operator(caller.0), project, &list).await.map(Json)
}

/// `GET /projects/{id}/runs/latest`: the project's newest run, or 404
/// (`weft logs` with no run named).
pub async fn latest_for_project(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<weft_core::program::RunSummary>, Refused> {
    run_admin::latest(&state, &Actor::operator(caller.0), id).await.map(Json)
}

/// `POST /runs/clean` (`weft clean --project .. --status ..`).
pub async fn clean(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(body): Json<weft_core::program::CleanRequest>,
) -> Result<Json<weft_core::program::CleanOutcome>, Refused> {
    let cleaned = run_admin::clean(&state, &Actor::operator(caller.0), &body, weft_core::StopSelf::Keep).await?;
    Ok(Json(cleaned.outcome))
}

/// `DELETE /runs/{id}` (`weft clean <run>`): the run's project, and the
/// versions it left bare.
pub async fn delete_run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(raw): Path<String>,
) -> Result<Json<DeletedRun>, crate::api::storage::ApiError> {
    run_admin::delete(&state, &Actor::operator(caller.0), run_id(&raw)?).await.map(Json)
}

// ----- the run mechanisms --------------------------------------------------

/// A `500` for a mechanism whose caller answers a bare status, with why
/// it failed in the log.
fn failed(what: &'static str) -> impl FnOnce(anyhow::Error) -> StatusCode {
    move |e| {
        tracing::error!(target: "weft_dispatcher::api", error = %format!("{e:#}"), "could not {what}");
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// Cancel every run in `targets`, each with its own cause, attempting
/// ALL of them before reporting. One failing run must not strand the
/// ones after it (they would stay live with their wakes registered), and
/// a failure must not disappear either: if any cancel failed, the
/// result is an error naming every failed run and why, so a task
/// built on this is recorded failed with the real errors, never
/// completed with a count nobody reads. Returns the runs cancelled.
pub async fn cancel_run_ids(
    state: &DispatcherState,
    targets: &[(RunId, &CancelCause)],
) -> anyhow::Result<Vec<RunId>> {
    let mut cancelled = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for (run_id, cause) in targets {
        match cancel_run_id(state, *run_id, cause).await {
            Ok(()) => cancelled.push(*run_id),
            Err(e) => failures.push(format!("{run_id}: {e:#}")),
        }
    }
    if !failures.is_empty() {
        // Name both sides: whoever repairs this by hand needs to know
        // which runs are terminal now and which are still live.
        let succeeded: Vec<String> = cancelled.iter().map(|c| c.to_string()).collect();
        anyhow::bail!(
            "{} of {} cancel(s) failed: {}; cancelled: [{}]",
            failures.len(),
            failures.len() + cancelled.len(),
            failures.join("; "),
            succeeded.join(", ")
        );
    }
    Ok(cancelled)
}

/// Cancel a single run, for `cause`. THE cancel: every caller
/// (`weft stop`, the sweeps, a sibling run's `stop_tagged`, a drain at its
/// deadline) goes through here and says why.
///
/// The durable part is ONE transaction (`RunStore::cancel_run`),
/// under the run's row lock: its wake signals go, and then
///
///   - a run a worker drives gets its `cancel_requested` set and the
///     cancel announced (`weft_task_store::runs::CANCEL_CHANNEL`): the
///     broker pushes it down the worker's line, the worker fires the
///     run's flag and writes its ending with this cause, and a worker
///     that claims the run later reads the cancel with its claim;
///   - a run nobody drives (parked, queued) is ended here, its per-node
///     cancels, its open charges booked unknown and its
///     `RunCancelled` written at `last_seq + 1`
///     ([`cancel_terminal_events`]).
///
/// Either all of it lands or none does, so a database failure mid-cancel
/// leaves the run exactly as it was and the next attempt succeeds. After
/// the commit the listener forgets the stripped signals (the rows are
/// already gone, so a late fire finds nothing), and the live view paints
/// the ending for whoever watches.
pub async fn cancel_run_id(
    state: &DispatcherState,
    run_id: RunId,
    cause: &CancelCause,
) -> anyhow::Result<()> {
    tracing::info!(
        target: "weft_dispatcher::cancel",
        run_id = %run_id,
        %cause,
        "cancel_run_id start"
    );
    // The per-node cancels come off the fold, which needs the run's
    // program; a run with none (never started, a node self-test)
    // has no nodes to cancel, and a program that cannot be found is no
    // reason to leave the run running: the terminal lands anyway.
    let program = program_for_cancel(state, run_id).await?;
    let write = state.runs.cancel_run(run_id, program.as_deref(), cause).await?;
    state.listener.unregister_many(&write.removed).await;
    tracing::info!(
        target: "weft_dispatcher::cancel",
        run_id = %run_id,
        signals_removed = write.removed.len(),
        asked_its_worker = write.requested,
        node_cancellations = ?write.node_cancellations,
        "cancel committed"
    );
    Ok(())
}

/// THE definition of a dispatcher-side cancel of a run nobody drives: the
/// ordered events that end it, `NodeCancelled` per open node, an unknown
/// `CostReported` per charge its record leaves open
/// (`weft_journal::unpriced_charges_closed`), then `RunCancelled`
/// (always the last). Pure, so every cancel writer
/// writes IDENTICAL rows. They are written in one row under the run's row
/// lock, so the ending never lands without its per-node cancels.
pub fn cancel_terminal_events(
    run_id: RunId,
    events: &[weft_journal::RunEvent],
    program: Option<&ProjectDefinition>,
    cause: &CancelCause,
    now: u64,
) -> anyhow::Result<Vec<weft_journal::RunEvent>> {
    use weft_journal::RunEvent;
    let reason = cause.to_string();
    let mut writes = Vec::new();
    match program {
        Some(program) => {
            // The runs map says which nodes are still non-terminal.
            // A row the fold could not apply was logged at `error!`
            // level and is the inspector's `/replay` to show; the
            // records that did fold are the ones to flip.
            let snapshot = weft_journal::fold_to_snapshot(
                run_id,
                Arc::new(program.clone()),
                events,
            );
            for e in snapshot.firings.open() {
                writes.push(RunEvent::NodeCancelled {
                    run_id,
                    node_id: e.node_id.clone(),
                    frames: e.frames.clone(),
                    reason: reason.clone(),
                    at_unix: now,
                });
            }
        }
        // No program: a run that never ran a node body (a node
        // self-test) has nothing per node to flip. A run whose
        // program is gone (its project was removed) or cannot be read
        // still ends, on the terminal alone: the stop must land, and
        // the run's status is read off the terminal. Its node records
        // stay as they were (the run view paints them from a
        // fold this run no longer has), which is what a run without a
        // program looks like everywhere else.
        None => {
            if events.iter().any(|e| matches!(e, RunEvent::NodeStarted { .. })) {
                tracing::warn!(
                    target: "weft_dispatcher::cancel",
                    %run_id,
                    "cancelling without the run's program: the terminal is written, the \
                     per-node cancels cannot be derived"
                );
            }
        }
    }
    // A charge its record leaves open spent money no drive is left to
    // price: booked unknown before the ending.
    let why = format!("the run was cancelled before the job was read back: {reason}");
    writes.extend(weft_journal::unpriced_charges_closed(run_id, events, &why, now));
    writes.push(RunEvent::RunCancelled { run_id, reason, cause: Some(cause.clone()), at_unix: now });
    Ok(writes)
}

/// The program a cancel folds with to derive its per-node cancels, or
/// `None` when the run has none to fold with: no program at all, a
/// removed project, or a program that cannot be read (logged; the
/// stop still lands on the terminal). `Err` only for the database.
pub async fn program_for_cancel(
    state: &crate::state::DispatcherState,
    run_id: RunId,
) -> anyhow::Result<Option<Arc<ProjectDefinition>>> {
    let lookup = crate::projection::run_program(state, run_id).await?;
    // Both unpaintable states are worth saying out loud here, and
    // `unpaintable` is the one place that knows which they are: an
    // unreadable row and a retired program both end with the terminal
    // written and no per-node cancels, and a person reading the log
    // needs to know which of the two they are looking at.
    if let Some((site, reason)) = lookup.unpaintable() {
        tracing::warn!(
            target: "weft_dispatcher::cancel",
            %run_id, %reason, ?site,
            "cancelling a run whose program cannot be folded: the terminal is written, the \
             per-node cancels cannot be derived"
        );
    }
    Ok(lookup.program())
}

/// The terminal outcome recorded for a run, if any: its run row's
/// `outcome`, stamped by the write that records its ending. `None` means the run is
/// still in flight. Used both for cancel-dedup and as the source of
/// truth when the in-RAM event bus drops events (broadcast `Lagged`).
pub(crate) async fn terminal_outcome(
    pool: &weft_task_store::db::Pool,
    run_id: RunId,
) -> anyhow::Result<Option<TerminalOutcome>> {
    let outcome: Option<Option<String>> = pool.scalar_opt("SELECT outcome FROM run WHERE run_id = $1", &[&run_id]).await?;
    Ok(outcome.flatten().map(|outcome| match outcome.as_str() {
        "completed" => TerminalOutcome::Completed,
        "cancelled" => TerminalOutcome::Cancelled,
        _ => TerminalOutcome::Failed,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalOutcome {
    Completed,
    Failed,
    Cancelled,
}

/// THE reading of a runs listing as a run store query, shared by every
/// door that takes one (list, count, clean), so they reach the same runs.
/// Its upper start bound is fixed at `now`: a run that starts after the
/// question is never part of the answer, `started_before` lowers it, and
/// `older_than_secs` keeps runs started at least that long ago.
pub(crate) fn list_query(project: Option<uuid::Uuid>, list: &RunList, now: u64) -> RunListQuery {
    let filter = &list.filter;
    let bound = now.saturating_sub(filter.older_than_secs.unwrap_or(0)) + 1;
    RunListQuery {
        limit: list.limit,
        offset: list.offset,
        project_id: project,
        started_after: list.started_after,
        started_before: Some(list.started_before.map_or(bound, |before| before.min(bound))),
        phase: list.phase,
        entry_node: filter.node.clone(),
        status: filter.status,
        instance: filter.instance.clone(),
        tag: filter.tag.clone(),
        node: list.through.clone().filter(|n| !n.trim().is_empty()),
        search: list.search.clone().filter(|s| !s.trim().is_empty()),
        below: list.after.map(|mark| (mark.started_at, mark.run)),
    }
}

/// [`list_query`] for a [`weft_core::program::RunFilter`] alone, a page of
/// `limit` runs (a count and a clean).
pub(crate) fn run_query(project: Option<uuid::Uuid>, filter: &weft_core::program::RunFilter, limit: u32, now: u64) -> RunListQuery {
    list_query(project, &RunList { filter: filter.clone(), limit, ..RunList::default() }, now)
}

/// THE removal of one run (`weft clean <run_id>`, and each run a
/// prune drops): its storage folder, then its journal, its tags, its
/// resume tokens (on the process that served them too), and its row in
/// the version tree, together; then the word to every client.
pub(crate) async fn clean_run(
    state: &DispatcherState,
    caller: &crate::tenant::TenantId,
    run_id: RunId,
) -> Result<uuid::Uuid, crate::api::storage::ApiError> {
    // The gate already read the owning row; keep it rather than asking
    // again. Its tenant is the one the storage prefix was WRITTEN
    // under, so the wipe below addresses the same bytes the run
    // created even for a project that has since been removed (asking
    // the project store for the tenant would fail exactly there).
    let owner = authorize_run(&*state.runs, caller, run_id).await?;
    // Wipe the run's storage folder (kept survivors included:
    // `weft clean <run_id>` IS the explicit removal verb for them)
    // BEFORE the journal rows go, while the run's row still exists.
    // A spent run's storage address dies with its journal history;
    // every failure below aborts so a retry can still wipe, never
    // orphaning the prefix. The store's own answer goes back as it is:
    // what it left and why, and whether waiting is enough.
    crate::storage::wipe_prefix(state, &format!("{}/run/{run_id}/", owner.tenant))
        .await
        .map_err(|e| {
            tracing::error!(
                target: "weft_dispatcher::storage",
                %run_id, error = %e,
                "could not wipe run storage; aborting clean so a retry can"
            );
            crate::api::storage::storage_err(e)
        })?;
    // Head stops pointing at the run first: a failure after it leaves
    // the run whole and reachable, and the same command retries.
    state.working_copies.forget_head_run(run_id).await.map_err(|e| {
        tracing::error!(
            target: "weft_dispatcher::versions",
            %run_id, error = %e,
            "could not move head off this run; nothing was deleted, retry"
        );
        (StatusCode::INTERNAL_SERVER_ERROR, "could not move head off this run; nothing was deleted, retry".to_string())
    })?;
    let removed = state
        .runs
        .delete_run(run_id)
        .await
        .map_err(|e| (failed("delete the run")(e), "could not delete the run's journal; retry".to_string()))?;
    // The questions the run was parked on went with its rows, and the
    // listener holding each one still serves it until told: a
    // client with a signal token could list and answer a form
    // belonging to a run that no longer exists. Same call as a cancel.
    state.listener.unregister_many(&removed).await;
    // Every window learns the run is gone the way it learns a cancel.
    // NOTIFY, not the journal: the journal is what was just erased.
    state
        .events
        .publish(DispatcherEvent::RunDeleted { run_id, project_id: owner.project_id })
        .await;
    Ok(owner.project_id)
}

#[cfg(test)]
mod cancel_tests {
    use super::*;
    use weft_journal::RunEvent;

    fn program() -> ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [{
                "id": "wait", "nodeType": "Wait", "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": []
        }))
        .expect("program")
    }

    fn open_firing(run_id: RunId) -> Vec<RunEvent> {
        vec![
            RunEvent::NodeKicked { run_id, node_id: "wait".into(), frames: vec![], firing: true, payload: None, port_snapshot: None, at_unix: 0 },
            RunEvent::NodeStarted { run_id, node_id: "wait".into(), frames: vec![], at_unix: 1 },
        ]
    }

    /// With the program, every open firing gets its `NodeCancelled`
    /// before the terminal; without one (a removed or unreadable
    /// program, a node self-test) the terminal still lands, alone.
    #[test]
    fn cancel_writes_per_node_rows_with_the_program_and_the_terminal_without() {
        let run_id = RunId::new_v4();
        let program = program();
        let with = cancel_terminal_events(run_id, &open_firing(run_id), Some(&program), &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = with.iter().map(RunEvent::kind_str).collect();
        assert_eq!(kinds, vec!["node_cancelled", "run_cancelled"]);
        assert!(matches!(&with[0], RunEvent::NodeCancelled { node_id, .. } if node_id == "wait"));
        let without = cancel_terminal_events(run_id, &open_firing(run_id), None, &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = without.iter().map(RunEvent::kind_str).collect();
        assert_eq!(kinds, vec!["run_cancelled"]);
        assert!(matches!(&without[0], RunEvent::RunCancelled { cause: Some(CancelCause::User), .. }));
    }

    /// A charge the record leaves open is booked unknown, closing it,
    /// after the per-node cancels and before the terminal.
    #[test]
    fn cancel_books_each_open_charge_unknown_before_the_terminal() {
        let run_id = RunId::new_v4();
        let mut record = open_firing(run_id);
        record.push(RunEvent::ChargeOpened {
            run_id,
            node_id: "wait".into(),
            frames: vec![],
            service: "jobs".into(),
            charge: "job-1".into(),
            scratch: serde_json::json!({}),
            origin: weft_core::CredentialOwner::Platform,
            at_unix: 2,
        });
        let writes = cancel_terminal_events(run_id, &record, Some(&program()), &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = writes.iter().map(RunEvent::kind_str).collect();
        assert_eq!(kinds, vec!["node_cancelled", "cost_reported", "run_cancelled"]);
        assert!(matches!(&writes[1], RunEvent::CostReported { charge: Some(charge), amount_usd: None, .. } if charge == "job-1"));
    }
}
