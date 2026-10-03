//! Execution state read endpoints. Writers journal events directly
//! to Postgres from the worker. What's left here is: cancel
//! (control), delete (cleanup), and the reader endpoints the CLI
//! and VS Code extension hit over HTTP: logs, replay,
//! list_executions, get.

use std::sync::Arc;

use weft_core::ProjectDefinition;

use axum::{extract::{Path, Query, State}, http::StatusCode, Json};
use serde::Deserialize;

use weft_core::exec::CancelCause;
use weft_core::program::SummaryStatus;
use weft_core::ExecutionId;

use crate::authenticator::{authorize_execution, authorize_project, CallerTenant};
use crate::journal::ExecutionQuery;
use weft_core::program::{DeletedExecution, ExecutionDetail, ExecutionPage, ParkedWait, ResolvedExecution};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

/// How many ambiguous matches an error names before it stops. Enough to
/// recognise the run you meant, few enough that the instruction after
/// them is still on screen.
const AMBIGUOUS_PREFIX_SHOWN: usize = 5;

/// The one execution of the caller's whose execution starts with `prefix`
/// (a full uuid resolves to itself). 404 when nothing matches, 409
/// when the prefix is short enough to match several, naming them.
pub async fn resolve_execution_id(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(prefix): Path<String>,
) -> Result<Json<ResolvedExecution>, (StatusCode, String)> {
    let prefix = prefix.to_ascii_lowercase();
    if prefix.len() < 4 || !prefix.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("'{prefix}' is not the start of an execution id: give at least four hex characters"),
        ));
    }
    let matches = state
        .journal
        .execution_ids_with_prefix(caller.0.as_str(), &prefix)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("execution_ids_with_prefix: {e}")))?;
    match matches.as_slice() {
        [] => Err((StatusCode::NOT_FOUND, format!("no execution starts with '{prefix}'"))),
        [one] => Ok(Json(ResolvedExecution { execution_id: *one })),
        // A count and a few short ids, never the whole list: an empty or
        // one-character prefix matches everything the project ever ran,
        // and printing a hundred full uuids buries the one sentence that
        // says what to do about it.
        several => Err((
            StatusCode::CONFLICT,
            format!(
                "'{prefix}' starts {} executions ({}{}); give more characters",
                several.len(),
                several
                    .iter()
                    .take(AMBIGUOUS_PREFIX_SHOWN)
                    .map(|c| c.to_string()[..8].to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                if several.len() > AMBIGUOUS_PREFIX_SHOWN { ", ..." } else { "" },
            ),
        )),
    }
}

pub async fn cancel(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(execution_id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let execution_id: ExecutionId = execution_id_str
        .parse()
        .map_err(|e: uuid::Error| (StatusCode::BAD_REQUEST, e.to_string()))?;
    authorize_execution(&*state.journal, &caller.0, execution_id).await?;
    // A run that already ended has nothing to cancel: said so, with
    // its status, instead of a silent no-op the caller would wait on
    // forever (the editor's Stop once sat on "Cancelling..." for a run
    // that had finished an hour before).
    if let Some(summary) = state
        .journal
        .execution_summary(execution_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("execution summary: {e}")))?
    {
        if let Some(refusal) = already_ended(summary.status) {
            return Err((StatusCode::CONFLICT, format!("execution {execution_id} already ended ({refusal})")));
        }
    }
    cancel_execution_id(&state, execution_id, &CancelCause::User).await.map_err(|e| {
        tracing::error!(target: "weft_dispatcher::cancel", execution_id = %execution_id, error = %e, "cancel_execution_id failed");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// The status word to answer a cancel of a run that is over with, or
/// `None` while the run can still be cancelled (running, or a corrupt
/// row whose terminal nobody can read: cancelling it is the safe side).
fn already_ended(status: SummaryStatus) -> Option<&'static str> {
    use weft_core::program::RunStatus;
    match status {
        SummaryStatus::Run(RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled) => Some(status.as_str()),
        SummaryStatus::Run(RunStatus::Running | RunStatus::WaitingForInput) | SummaryStatus::Corrupt => None,
    }
}

/// Cancel every execution in `targets`, each with its own cause, attempting
/// ALL of them before reporting. One failing execution must not strand the
/// ones after it (they would stay live with their wakes registered), and
/// a failure must not disappear either: if any cancel failed, the
/// result is an error naming every failed execution and why, so a task
/// built on this is recorded failed with the real errors, never
/// completed with a count nobody reads. Returns the executions cancelled.
pub async fn cancel_execution_ids(
    state: &DispatcherState,
    targets: &[(ExecutionId, &CancelCause)],
) -> anyhow::Result<Vec<ExecutionId>> {
    let mut cancelled = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for (execution_id, cause) in targets {
        match cancel_execution_id(state, *execution_id, cause).await {
            Ok(()) => cancelled.push(*execution_id),
            Err(e) => failures.push(format!("{execution_id}: {e:#}")),
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

/// Cancel a single execution, for `cause`. THE cancel: every caller
/// (`weft stop`, the sweeps, a sibling run's `stop_tagged`) goes
/// through here and says why, and the cause lands on every terminal
/// row this writes AND on the `cancel_execution` task, so the owning
/// worker's own write (if it gets there first) names the same cause.
///
/// The durable part is ONE transaction (`Journal::cancel_execution`):
/// strip the wake signals, journal the terminals, queue the cancel
/// task for the alive owner process. Either all of it lands or none does,
/// so a database failure mid-cancel leaves the run exactly as it was
/// and the next attempt succeeds; nothing can strip a run's wakes and
/// then fail to end it. Two paths then converge on the one observable
/// outcome (the journal reads `ExecutionCancelled`):
///
///   - When a worker is alive and driving this execution, the task
///     fires the per-execution `CancellationFlag` (~50ms), the loop driver
///     exits, and the worker's own terminal write finds the rows
///     already there and skips (idempotent).
///   - With no worker driving it (a suspended run, no process at all), the
///     rows written here ARE the terminal.
///
/// After the commit the listener forgets the stripped signals in RAM
/// (the durable row is already gone, so a late fire finds nothing),
/// and the journal bridge publishes the new rows onto the project's
/// SSE bus so the frontend exits "Cancelling...".
pub async fn cancel_execution_id(
    state: &DispatcherState,
    execution_id: ExecutionId,
    cause: &CancelCause,
) -> anyhow::Result<()> {
    tracing::info!(
        target: "weft_dispatcher::cancel",
        execution_id = %execution_id,
        %cause,
        "cancel_execution_id start"
    );
    // The per-node cancels come off the fold, which needs the run's
    // program; an execution with none (never started, a node self-test)
    // has no nodes to cancel, and a program that cannot be found is no
    // reason to leave the run running: the terminal lands anyway.
    let program = program_for_cancel(state, execution_id).await?;
    let write = state.journal.cancel_execution(execution_id, program.as_deref(), cause).await?;
    state.listener.unregister_many(&write.removed).await;
    tracing::info!(
        target: "weft_dispatcher::cancel",
        execution_id = %execution_id,
        signals_removed = write.removed.len(),
        task_enqueued = write.task_enqueued,
        node_cancellations = ?write.node_cancellations,
        "cancel committed"
    );
    Ok(())
}

/// THE definition of a dispatcher-side cancel write: the ordered
/// `(event, dedup_key)` list that flips an execution terminal. Pure, so
/// every transactional cancel writer emits IDENTICAL rows and can
/// never drift on the ordering rule, the dedup-key format, or the
/// closure-emission policy.
///
/// Per-node cancellations come BEFORE `ExecutionCancelled` (always the last
/// entry). Otherwise a partial run that journaled the terminal event first
/// would set has-terminal=true, and a retry would skip the per-node writes
/// forever, leaving node UI states stuck on "running". Each per-node write is
/// dedup-keyed on (execution, node, frame-stack) so a partial failure + retry
/// (e.g. the orphan sweep's retry-next-tick loop) collapses instead of
/// stacking a duplicate NodeCancelled row (which would also republish a
/// duplicate UI event); the terminal's key makes the row-level write safe even
/// if two cancels for the same execution race past their has-terminal checks.
pub fn cancel_terminal_events(
    execution_id: ExecutionId,
    events: &[weft_journal::ExecEvent],
    program: Option<&ProjectDefinition>,
    cause: &CancelCause,
    now: u64,
) -> anyhow::Result<Vec<(weft_journal::ExecEvent, String)>> {
    use weft_journal::ExecEvent;
    let reason = cause.to_string();
    let mut writes = Vec::new();
    match program {
        Some(program) => {
            // The executions map says which nodes are still non-terminal.
            // A row the fold could not apply was logged at `error!`
            // level and is the inspector's `/replay` to show; the
            // records that did fold are the ones to flip.
            let snapshot = weft_journal::fold_to_snapshot(
                execution_id,
                Arc::new(program.clone()),
                events,
            );
            for (node_id, execs) in &snapshot.executions {
                for e in execs {
                    if e.status.is_terminal() {
                        continue;
                    }
                    let frames_key: String =
                        weft_core::frames::frames_text(&e.frames);
                    writes.push((
                        ExecEvent::NodeCancelled {
                            execution_id,
                            node_id: node_id.clone(),
                            frames: e.frames.clone(),
                            reason: reason.clone(),
                            at_unix: now,
                        },
                        format!("cancel:{execution_id}:{node_id}:{frames_key}"),
                    ));
                }
            }
        }
        // No program: an execution that never ran a node body (a node
        // self-test) has nothing per node to flip. An execution whose
        // program is gone (its project was removed) or cannot be read
        // still ends, on the terminal alone: the stop must land, and
        // the run's status is read off the terminal. Its node records
        // stay as they were (the execution view paints them from a
        // fold this run no longer has), which is what a run without a
        // program looks like everywhere else.
        None => {
            if events.iter().any(|e| matches!(e, ExecEvent::NodeStarted { .. })) {
                tracing::warn!(
                    target: "weft_dispatcher::cancel",
                    %execution_id,
                    "cancelling without the run's program: the terminal is written, the \
                     per-node cancels cannot be derived"
                );
            }
        }
    }
    writes.push((
        ExecEvent::ExecutionCancelled { execution_id, reason, cause: Some(cause.clone()), at_unix: now },
        format!("execution_cancelled:{execution_id}"),
    ));
    Ok(writes)
}

/// The program a cancel folds with to derive its per-node cancels, or
/// `None` when the execution has none to fold with: no program at all, a
/// removed project, or a program that cannot be read (logged; the
/// stop still lands on the terminal). `Err` only for the database.
pub async fn program_for_cancel(
    state: &crate::state::DispatcherState,
    execution_id: ExecutionId,
) -> anyhow::Result<Option<Arc<ProjectDefinition>>> {
    let lookup = crate::projection::execution_program(state, execution_id).await?;
    // Both unpaintable states are worth saying out loud here, and
    // `unpaintable` is the one place that knows which they are: an
    // unreadable row and a retired program both end with the terminal
    // written and no per-node cancels, and a person reading the log
    // needs to know which of the two they are looking at.
    if let Some((site, reason)) = lookup.unpaintable() {
        tracing::warn!(
            target: "weft_dispatcher::cancel",
            %execution_id, %reason, ?site,
            "cancelling a run whose program cannot be folded: the terminal is written, the \
             per-node cancels cannot be derived"
        );
    }
    Ok(lookup.program())
}

/// Is anything actually WORKING on this execution?
///
/// An execution advances because a task carries it: a `pending` one a
/// worker will claim, or a `claimed` one a worker holds. With neither,
/// the run is recorded as going and nothing is going to move it, which
/// is a different state from "still in flight" and has to be told apart
/// from it. A caller that waits on the first forever is waiting on a
/// run that is already over.
///
/// Deliberately a question about WORK, not about time: nothing here
/// ages a run out, because a legitimate setup may take as long as the
/// nodes inside it take. Only the absence of any task says nobody is
/// coming.
pub(crate) async fn execution_is_being_worked_on(
    pool: &sqlx::PgPool,
    execution_id: ExecutionId,
) -> anyhow::Result<bool> {
    // `task.execution_id` is TEXT, so the execution goes in as its string form;
    // binding the uuid itself matches nothing and would read as "no
    // task", which here means "declare every run dead".
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT count(*) FROM task \
         WHERE execution_id = $1 AND status IN ('pending', 'claimed')",
    )
    .bind(execution_id.to_string())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(n,)| n > 0).unwrap_or(false))
}

/// The terminal outcome recorded for an execution, if any. The journal is
/// the authoritative source: `Completed`/`Failed`/`Cancelled` are the
/// three terminal `exec_event` kinds. `None` means the execution is
/// still in flight. Used both for cancel-dedup and as the source of
/// truth when the in-RAM event bus drops events (broadcast `Lagged`).
pub(crate) async fn terminal_outcome(
    pool: &sqlx::PgPool,
    execution_id: ExecutionId,
) -> anyhow::Result<Option<TerminalOutcome>> {
    let row: Option<(String,)> = sqlx::query_as(concat!(
        "SELECT kind FROM exec_event \
         WHERE execution_id = $1 \
           AND kind IN ",
        weft_journal::execution_terminal_kinds_sql!(),
        " LIMIT 1",
    ))
    .bind(execution_id.to_string())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(kind,)| match kind.as_str() {
        "execution_completed" => TerminalOutcome::Completed,
        "execution_cancelled" => TerminalOutcome::Cancelled,
        _ => TerminalOutcome::Failed,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalOutcome {
    Completed,
    Failed,
    Cancelled,
}

/// Overlay the honest `waiting_for_input` status onto a batch of
/// execution summaries. The journal fold only knows started/terminal, so
/// a suspended execution (waiting on a human / an external resume,
/// holding NO worker) folds to `running`, which misreads as active work.
/// The dispatcher's resume-signal rows are the source of truth for
/// suspension (the same set `running_count` and the drain wait exclude),
/// so every status read that serves clients routes through this overlay
/// and the three surfaces (list, point-get, latest) can never disagree.
/// The value reuses the per-node vocabulary (`waiting_for_input`, the
/// word that replaced the ghost `suspended` variant) so clients style
/// one concept, and it is NON-terminal: pollers keep waiting through it,
/// exactly as they did when it read `running`.
async fn overlay_suspended(
    state: &DispatcherState,
    summaries: &mut [weft_core::program::ExecutionSummary],
) -> Result<(), StatusCode> {
    use std::collections::HashMap;
    let mut sets: HashMap<uuid::Uuid, std::collections::HashSet<ExecutionId>> = HashMap::new();
    for s in summaries.iter_mut() {
        if s.status != SummaryStatus::Run(weft_core::program::RunStatus::Running) {
            continue;
        }
        let set = match sets.entry(s.project_id) {
            std::collections::hash_map::Entry::Occupied(known) => known.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => slot.insert(
                crate::api::project::suspended_execution_id_set(state, s.project_id)
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            ),
        };
        s.status = s.status.parked(set.contains(&s.execution_id));
    }
    Ok(())
}

pub async fn get(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(execution_id_str): Path<String>,
) -> Result<Json<ExecutionDetail>, StatusCode> {
    let execution_id: ExecutionId = execution_id_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    authorize_execution(&*state.journal, &caller.0, execution_id)
        .await
        .map_err(|(s, _)| s)?;
    // Direct point-lookup by execution (authorization above already proved the
    // caller owns it), so an execution older than any list window still
    // resolves instead of 404ing.
    let summary = state
        .journal
        .execution_summary(execution_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let mut batch = [summary];
    overlay_suspended(&state, &mut batch).await?;
    let [summary] = batch;
    // The waits ride with the status they produced: a run reads as
    // `waiting_for_input` because these rows exist, and they exist
    // before the journal's `NodeSuspended` lands (the node registers
    // its wait, then returns), so a client that learns the run is
    // parked learns from the same read what it is parked on.
    let waiting = if summary.status == SummaryStatus::Run(weft_core::program::RunStatus::WaitingForInput) {
        parked_waits(&state, execution_id).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        Vec::new()
    };
    Ok(Json(ExecutionDetail { summary, waiting }))
}

/// The waits of `execution_id`, from the resume-signal rows that make it
/// `waiting_for_input`, in registration order.
async fn parked_waits(state: &DispatcherState, execution_id: ExecutionId) -> anyhow::Result<Vec<ParkedWait>> {
    let signals = state.journal.signal_list_for_execution_id(execution_id).await?;
    waits_of(&signals, execution_id)
}

fn waits_of(signals: &[crate::journal::SignalRegistration], execution_id: ExecutionId) -> anyhow::Result<Vec<ParkedWait>> {
    signals
        .iter()
        .filter(|s| s.is_resume && s.execution_id == Some(execution_id))
        .map(|s| {
            let spec: weft_core::primitive::SignalSpec = serde_json::from_str(&s.spec_json)?;
            Ok(ParkedWait { node: s.node_id.clone(), token: s.token.clone(), kind: spec.kind })
        })
        .collect()
}

/// How many log lines one read returns, and the ceiling on asking for
/// more. The journal answers with the TAIL, so the default holds the
/// end of a long run, which is where a run goes wrong. A `limit`
/// outside `1..=MAX` is refused, not quietly moved: a caller who
/// asked for more than the ceiling would otherwise read a cut log as
/// the whole one.
const DEFAULT_LOG_LINES: u32 = 1_000;
const MAX_LOG_LINES: u32 = 20_000;

#[derive(Debug, Deserialize)]
pub struct ListLogsParams {
    pub limit: Option<u32>,
}


pub async fn list_logs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(execution_id_str): Path<String>,
    Query(params): Query<ListLogsParams>,
) -> Result<Json<weft_core::program::ExecutionLogs>, (StatusCode, String)> {
    let execution_id: ExecutionId = execution_id_str
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("'{execution_id_str}' is not an execution id (a uuid)")))?;
    authorize_execution(&*state.journal, &caller.0, execution_id).await?;
    let limit = params.limit.unwrap_or(DEFAULT_LOG_LINES);
    if !(1..=MAX_LOG_LINES).contains(&limit) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("limit is {limit}; one read holds between 1 and {MAX_LOG_LINES} lines"),
        ));
    }
    // The journal's error names the execution and `weft clean` when a row
    // no longer decodes; the reader gets it, not a bare 500.
    let entries = crate::projection::execution_logs(&state, execution_id, limit)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let lines = entries
        .into_iter()
        .map(|e| weft_core::program::ExecutionLogLine {
            inherited_from: e.inherited_from,
            at_unix: e.at_unix,
            level: e.level,
            node: e.node,
            frames: e.frames,
            message: e.message,
        })
        .collect();
    Ok(Json(weft_core::program::ExecutionLogs { limit, lines }))
}

/// Complete ordered output evidence, including streams and inherited results.
pub async fn outputs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(execution_id_str): Path<String>,
) -> Result<Json<weft_core::run_spec::Expected>, (StatusCode, String)> {
    let execution_id: ExecutionId = execution_id_str.parse().map_err(|_| (StatusCode::BAD_REQUEST, "bad execution".into()))?;
    authorize_execution(&*state.journal, &caller.0, execution_id).await?;
    let sources = crate::projection::reconstruct_execution(&state, execution_id).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("read output history: {error:#}")))?;
    let project = sources[&execution_id].project();
    // A forwarding boundary is the compiler's: what entered a group is
    // the wire of the node that fed it, what left is the wire of the
    // node that filled it, so its wire says nothing a person's wire
    // does not, and its name (the group's) would collide with theirs.
    // A loop's Out is different: the list it gathers over the
    // iterations exists nowhere else, so it stays, as the loop's own
    // output (`doubler.results`).
    let boundary = |id: &str| project.nodes.iter().any(|n| n.id == id && n.group_boundary.is_some()
        && n.node_type != weft_core::project::boundary_types::LOOP_OUT);
    let wires = sources[&execution_id].output_wires()
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .iter().filter(|wire| !boundary(&wire.node))
        .map(|wire| weft_core::run_spec::ExpectedWire::spell(project, wire)).collect();
    // Every place of the run, spelled the way the wires are: a node in
    // an included file once per site that reaches it, a group once.
    let nodes = weft_core::project::selection::every_place(project).iter()
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    Ok(Json(weft_core::run_spec::Expected { wires, nodes, focus: Vec::new() }))
}

/// Replay journaled history through the same projection as live events.
/// Terminals come from the journal, never a synthesized second completion.
pub async fn replay(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(execution_id_str): Path<String>,
) -> Result<Json<Vec<crate::events::LiveEvent>>, StatusCode> {
    let execution_id: ExecutionId = execution_id_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    // Resolve + tenant-gate in the ONE place that owns "who owns this
    // execution": a lookup failure is 500, an unknown or cross-tenant
    // execution is 404, and the resolved project rides back for the
    // replay's event attribution.
    let project_id =
        authorize_execution(&*state.journal, &caller.0, execution_id).await.map_err(|(s, _)| s)?.project_id;
    // The full ExecEvent log, folded over the run's program through
    // the SAME projection the live `journal_bridge` runs, so replay and
    // live cannot drift; bus and caller events ride along.
    // Lossy read: the inspector renders what exists, and every row
    // that no longer decodes lands below as its own JournalCorruption
    // entry, naming `weft clean`, instead of taking the response down.
    let (raw_events, unreadable_rows) =
        state.journal.events_log_lossy(execution_id).await.map_err(|e| {
            tracing::error!(
                target: "weft_dispatcher::api",
                %execution_id, error = %e,
                "replay: reading the journal failed"
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    // Only the database itself fails the read; a program that cannot
    // be found paints the run as unreadable, the way the live bridge
    // does for the same journal, and names `weft clean` below.
    // Everything the reader must be TOLD about this run, with the site
    // that places it on screen.
    let mut corruptions: Vec<(weft_core::primitive::CorruptionSite, String)> = unreadable_rows
        .into_iter()
        .map(|reason| (weft_core::primitive::CorruptionSite::UndecodableRow, reason))
        .collect();
    let found = crate::projection::execution_program(&state, execution_id).await.map_err(|e| {
        tracing::error!(
            target: "weft_dispatcher::api",
            %execution_id, error = %e,
            "replay: looking up the run's program failed"
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let program = {
        match found.unpaintable() {
            // The reason travels with the run, at its own site: a row
            // that no longer decodes is one missing value, a missing
            // program is every value at once and belongs beside the run
            // rather than inside a node.
            Some((site, reason)) => {
                corruptions.push((site, reason.to_string()));
                found.without_reason()
            }
            None => found,
        }
    };
    let rows: Vec<weft_journal::ExecEvent> = raw_events.iter().map(|r| r.event.clone()).collect();
    let inheritance = match crate::projection::execution_inheritance(&state, &rows, &program).await {
        Ok(chain) => chain,
        Err(reason) => {
            corruptions.push((weft_core::primitive::CorruptionSite::UndecodableRow, reason));
            weft_journal::SeedChain::default()
        }
    };
    let mut projector = crate::projection::ExecutionProjector::new(execution_id, program, project_id)
        .with_inheritance(inheritance);
    let mut out: Vec<crate::events::LiveEvent> = Vec::new();
    for record in raw_events {
        let projected = projector.project(&record.event);
        out.extend(
            crate::events::IdentifiedEvent { event_id: record.event_id, event: () }
                .project(|()| projected),
        );
    }
    // A row the fold could not apply is painted as its corruption, at
    // the row, by the projector; the rows that never decoded follow.
    for (site, reason) in corruptions {
        out.push(crate::events::IdentifiedEvent::transient(DispatcherEvent::JournalCorruption {
            execution_id,
            project_id,
            site,
            reason,
        }));
    }
    Ok(Json(out))
}

/// Query params for the executions list: page size + offset, and optional
/// project + start-time-range filters. All optional; `limit` defaults to a page
/// and is capped so a single request can never pull an unbounded slice.
#[derive(Debug, Deserialize)]
pub struct ListExecutionsParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub project_id: Option<uuid::Uuid>,
    /// Inclusive lower bound on start time (unix seconds).
    pub started_after: Option<u64>,
    /// Exclusive upper bound on start time (unix seconds).
    pub started_before: Option<u64>,
    /// Only runs of this phase (`fire`, `trigger_setup`, `infra_setup`).
    pub phase: Option<weft_core::context::Phase>,
    /// Only runs started by this entry node.
    pub entry_node: Option<String>,
    /// Only runs standing this way ([`weft_core::program::RunStatus`]);
    /// any other word is a 400 before anything is read.
    pub status: Option<weft_core::program::RunStatus>,
    /// Only runs for this instance.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// Only runs carrying this tag.
    pub tag: Option<String>,
}

const DEFAULT_PAGE: u32 = 50;
const MAX_PAGE: u32 = weft_core::program::MAX_RUNS_PAGE;

pub async fn list_executions(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Query(params): Query<ListExecutionsParams>,
) -> Result<Json<ExecutionPage>, StatusCode> {
    let query = ExecutionQuery {
        limit: params.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE),
        offset: params.offset.unwrap_or(0),
        project_id: params.project_id,
        started_after: params.started_after,
        started_before: params.started_before,
        phase: params.phase,
        entry_node: params.entry_node,
        status: params.status,
        instance: params.instance,
        tag: params.tag,
        below: None,
    };
    let mut page = state
        .journal
        .list_executions(caller.0.as_str(), &query)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    overlay_suspended(&state, &mut page.executions).await?;
    Ok(Json(page))
}

/// Return the most recent execution for a project, or 404 if
/// the project has none. Used by `weft logs` (no-arg form) to
/// find the execution to dump logs for.
pub async fn latest_for_project(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<weft_core::program::ExecutionSummary>, StatusCode> {
    authorize_project(&state, &caller.0, id)
        .await
        .map_err(|(s, _)| s)?;
    // Ask SQL for exactly the newest execution of this project (project-filtered,
    // limit 1), rather than scanning a fixed window and filtering in memory.
    let query = ExecutionQuery {
        limit: 1,
        offset: 0,
        project_id: Some(id),
        started_after: None,
        started_before: None,
        phase: None,
        entry_node: None,
        status: None,
        instance: None,
        tag: None,
        below: None,
    };
    let mut page = state
        .journal
        .list_executions(caller.0.as_str(), &query)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    overlay_suspended(&state, &mut page.executions).await?;
    page.executions.into_iter().next().map(Json).ok_or(StatusCode::NOT_FOUND)
}

/// `POST /executions/{execution_id}/wake/{node}`: resolve a wait now, instead
/// of waiting for whatever it waits for.
///
/// This side decides whether the wake may happen: the caller's tenancy,
/// and that the node really has a wait parked on this execution. What the
/// wait then wakes WITH is the signal kind's own shape, so the listener
/// holding it is asked, and it answers with nothing for every kind that
/// has no truthful stand-in (a form is waiting for an answer; there is
/// no inventing one). Those are refused naming the kind, and the fire
/// goes through the same lifecycle gate every other fire passes.
///
/// Nothing here knows which kinds can be woken. That is why: a tier
/// that minted one kind's payload would have to be edited for the next.
pub async fn wake(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((execution_id_str, node)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let execution_id: ExecutionId = execution_id_str.parse().map_err(|_| (StatusCode::BAD_REQUEST, "bad execution".to_string()))?;
    // Authorization only: the execution's own waits are read below, and the
    // wall this crosses is the caller's tenancy, not the project id.
    authorize_execution(&*state.journal, &caller.0, execution_id).await?;
    let waits = parked_waits(&state, execution_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signals: {e}")))?;
    // The node is named the way a person writes it (`one.review`), which
    // is the key its wait is registered under: one spelling, on the wire
    // and in the row, so nothing here translates.
    let Some(wait) = waits.into_iter().find(|w| w.node == node) else {
        // Bounded, like every other caller string this surface echoes: the
        // node name is a raw path segment, so an enormous one would come
        // straight back in the body.
        let node = weft_core::truncate_user_string(&node, 256);
        return Err((
            StatusCode::NOT_FOUND,
            format!("'{node}' is not waiting on anything in {execution_id}; `weft events {execution_id} --node {node}` shows what it did"),
        ));
    };
    // WHETHER a wake may happen is this side's question, and it has been
    // answered above: the project is live, the node really is waiting,
    // the caller may touch it. WHAT it wakes with is the signal kind's,
    // so the listener is asked. A kind that cannot be woken by hand
    // answers with nothing, and the refusal below names the kind without
    // this tier ever knowing one.
    let payload = state
        .listener
        .wake_by_hand(&wait.token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("ask what it wakes with: {e:#}")))?;
    let Some(payload) = payload else {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "'{}' is waiting on a {} signal, which expects a value; answer it (its form, in the browser extension or through its signal URL) instead of waking it",
                wait.node,
                wait.kind
            ),
        ));
    };
    crate::api::signal::fire_registered_signal(&state, &wait.token, payload).await
}

pub async fn clean(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    axum::Json(body): axum::Json<weft_core::program::CleanRequest>,
) -> Result<axum::Json<weft_core::program::CleanOutcome>, (StatusCode, String)> {
    if let Some(project) = body.project {
        authorize_project(&state, &caller.0, project).await?;
    }
    clean_runs(&state, &caller.0, body.project, &body.filter, body.running, None)
        .await
        .map(axum::Json)
}

/// Delete every run `filter` reaches, in `project` (or every project of
/// `tenant`). A run still going is never deleted from under itself:
/// `cancel` stops it (its rows go with the next clean), `wait` leaves it
/// running. `asked_by` is never touched: a program cleaning runs is not
/// among them. Each deleted project's bare versions are swept as its runs
/// go, the way `weft clean <execution_id>` sweeps them.
pub(crate) async fn clean_runs(
    state: &DispatcherState,
    tenant: &crate::tenant::TenantId,
    project: Option<uuid::Uuid>,
    filter: &weft_core::program::RunFilter,
    running: weft_core::running_policy::RunningPolicy,
    asked_by: Option<ExecutionId>,
) -> Result<weft_core::program::CleanOutcome, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let query = run_query(project, filter, MAX_PAGE, crate::lease::now_unix() as u64);
    let mut outcome = weft_core::program::CleanOutcome::default();
    // The pages walk back in time from a bound fixed now (a run that
    // starts while this cleans is never reached), each page starting
    // strictly after the last run the one before it listed. Whatever the
    // walk deletes, cancels or leaves behind itself, the next page is
    // exactly the runs it has not reached yet.
    let mut below = None;
    let mut swept_projects: std::collections::BTreeSet<uuid::Uuid> = Default::default();
    loop {
        let page = state
            .journal
            .list_executions(
                tenant.as_str(),
                &ExecutionQuery { below, ..query.clone() },
            )
            .await
            .map_err(|e| internal("list runs", e))?;
        let Some(last) = page.executions.last().map(|run| (run.started_at, run.execution_id)) else {
            break;
        };
        below = Some(last);
        for run in page.executions {
            if Some(run.execution_id) == asked_by {
                continue;
            }
            if run.status == SummaryStatus::Run(weft_core::program::RunStatus::Running) {
                match running {
                    weft_core::running_policy::RunningPolicy::Cancel => {
                        cancel_execution_id(state, run.execution_id, &weft_core::exec::CancelCause::User)
                            .await
                            .map_err(|e| internal("cancel", e))?;
                        outcome.cancelled += 1;
                    }
                    weft_core::running_policy::RunningPolicy::Wait => outcome.left_running += 1,
                }
                continue;
            }
            // The listing's status clause is SQL and cannot see a row
            // that no longer decodes, so it lists one as `corrupt`; a
            // filtered clean deletes only the runs the filter reaches,
            // never those. An unfiltered clean still removes them.
            if filter.status.is_some_and(|status| !status.reaches(run.status)) {
                continue;
            }
            let project_id = clean_execution(state, tenant, run.execution_id)
                .await
                .map_err(|status| (status, format!("delete run {}", run.execution_id)))?;
            swept_projects.insert(project_id);
            outcome.deleted += 1;
        }
    }
    for project_id in swept_projects {
        if let Err((status, message)) = crate::api::versions::sweep_bare_versions(state, project_id).await {
            tracing::warn!(
                target: "weft_dispatcher::versions",
                %project_id, %status, %message,
                "the runs are deleted; the versions they may have left bare could not be swept"
            );
        }
    }
    Ok(outcome)
}

/// THE reading of a [`weft_core::program::RunFilter`] as a journal query,
/// shared by every door that takes one (clean, list, count), so they
/// reach the same runs. Its upper start bound is fixed at `now`: a run
/// that starts after the question is never part of the answer, and
/// `older_than_secs` keeps runs started at least that long ago.
pub(crate) fn run_query(
    project: Option<uuid::Uuid>,
    filter: &weft_core::program::RunFilter,
    limit: u32,
    now: u64,
) -> ExecutionQuery {
    ExecutionQuery {
        limit,
        offset: 0,
        project_id: project,
        started_after: None,
        started_before: Some(now.saturating_sub(filter.older_than_secs.unwrap_or(0)) + 1),
        phase: None,
        entry_node: filter.node.clone(),
        status: filter.status,
        instance: filter.instance.clone(),
        tag: filter.tag.clone(),
        below: None,
    }
}

/// The newest `limit` runs `filter` reaches in `project`, with how many it
/// reaches in all; a run parked on a wait reads `waiting_for_input`, as
/// in every other listing.
pub(crate) async fn list_runs(
    state: &DispatcherState,
    tenant: &crate::tenant::TenantId,
    project: uuid::Uuid,
    filter: &weft_core::program::RunFilter,
    limit: u32,
) -> Result<ExecutionPage, (StatusCode, String)> {
    if !(1..=MAX_PAGE).contains(&limit) {
        return Err((StatusCode::BAD_REQUEST, format!("a runs listing takes 1 to {MAX_PAGE} runs, not {limit}")));
    }
    let query = run_query(Some(project), filter, limit, crate::lease::now_unix() as u64);
    let mut page = state
        .journal
        .list_executions(tenant.as_str(), &query)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list runs: {e:#}")))?;
    overlay_suspended(state, &mut page.executions)
        .await
        .map_err(|status| (status, "mark the runs parked on a wait".to_string()))?;
    Ok(page)
}

/// How many runs `filter` reaches in `project`.
pub(crate) async fn count_runs(
    state: &DispatcherState,
    tenant: &crate::tenant::TenantId,
    project: uuid::Uuid,
    filter: &weft_core::program::RunFilter,
) -> Result<u64, (StatusCode, String)> {
    let query = run_query(Some(project), filter, 0, crate::lease::now_unix() as u64);
    let page = state
        .journal
        .list_executions(tenant.as_str(), &query)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("count runs: {e:#}")))?;
    Ok(page.total)
}

/// Remove one run, and sweep the version its removal left bare.
///
/// The sweep is HERE and not in whoever asked. Deleting the last run of
/// a version is what makes that version bare, and a bare version shows
/// in `weft tree` with nothing under it and a status of `unknown`, for
/// ever, with no verb that reaches it. The CLI used to do the sweep
/// itself, which left the editor's own delete not doing it at all, and
/// the CLI could only sweep the project it was standing in, which for
/// `weft clean <execution_id>` (an execution can be cleaned from anywhere) was
/// frequently the wrong one.
///
/// Answers the project and what was swept, so a caller can say so.
pub async fn delete_execution(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(execution_id_str): Path<String>,
) -> Result<axum::Json<DeletedExecution>, StatusCode> {
    let execution_id: ExecutionId = execution_id_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let project_id = clean_execution(&state, &caller.0, execution_id).await?;
    // The run IS deleted, which is what was asked for, so a sweep that
    // cannot run is said out loud and does not fail the delete: the
    // reaper and the next `weft clean` both reach the same rows.
    let swept = crate::api::versions::sweep_bare_versions(&state, project_id).await.unwrap_or_else(|(status, message)| {
        tracing::warn!(
            target: "weft_dispatcher::versions",
            %execution_id, project_id = %project_id, %status, %message,
            "the run is deleted; the version it may have left bare could not be swept"
        );
        Vec::new()
    });
    Ok(axum::Json(DeletedExecution { project: project_id, swept }))
}

/// THE removal of one execution (`weft clean <execution_id>`, and each run a
/// prune drops): its storage folder, then its journal, its tags, its
/// resume tokens (on the process that served them too), and its row in
/// the version tree, together; then the word to every client.
pub(crate) async fn clean_execution(
    state: &DispatcherState,
    caller: &crate::tenant::TenantId,
    execution_id: ExecutionId,
) -> Result<uuid::Uuid, StatusCode> {
    // The gate already read the owning row; keep it rather than asking
    // again. Its tenant is the one the storage prefix was WRITTEN
    // under, so the wipe below addresses the same bytes the run
    // created even for a project that has since been removed (asking
    // the project store for the tenant would fail exactly there).
    let owner = authorize_execution(&*state.journal, caller, execution_id).await.map_err(|(s, _)| s)?;
    // Wipe the execution's storage folder (kept survivors included:
    // `weft clean <execution_id>` IS the explicit removal verb for them)
    // BEFORE the journal rows go, while the execution's row still exists.
    // A spent execution's storage address dies with its journal history;
    // every failure below aborts so a retry can still wipe, never
    // orphaning the prefix.
    crate::storage::wipe_prefix(state, &format!("{}/exec/{execution_id}/", owner.tenant))
        .await
        .map_err(|e| {
            tracing::error!(
                target: "weft_dispatcher::storage",
                %execution_id, error = %e,
                "could not wipe execution storage; aborting clean so a retry can"
            );
            StatusCode::SERVICE_UNAVAILABLE
        })?;
    // The version tree's row for this run goes first, dropped by the
    // store that owns that table.
    //
    // Before the journal, because the journal row is what makes this
    // call REACHABLE: `authorize_execution` reads `execution`,
    // which `delete_execution` removes. Deleting the journal first and
    // failing here left a tree row with no journal, and then `weft
    // clean` answered 404 for ever (no owner row to authorize against)
    // while every later prune refused the subtree because a run with no
    // terminal row reads as still in flight. This way round, a failure
    // leaves everything reachable and the same command retries: the
    // tree delete is idempotent.
    state.versions.delete_run(execution_id).await.map_err(|e| {
        tracing::error!(
            target: "weft_dispatcher::versions",
            %execution_id, error = %e,
            "could not drop this run from the version tree; nothing was deleted, retry"
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let removed = state
        .journal
        .delete_execution(execution_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // The questions the run was parked on went with its rows, and the
    // listener holding each one still serves it until told: a
    // client with a signal token could list and answer a form
    // belonging to a run that no longer exists. Same call as a cancel.
    state.listener.unregister_many(&removed).await;
    // Every window learns the run is gone the way it learns a cancel.
    // NOTIFY, not the journal: the journal is what was just erased.
    state
        .events
        .publish(DispatcherEvent::ExecutionDeleted { execution_id, project_id: owner.project_id })
        .await;
    // This may have been the last run keeping a removed project's code
    // and tree rows on file. A failure here leaves rows nobody reads and
    // nothing else, and the reaper's `retired_rows` sweep comes back for
    // them, so it is logged rather than failing a clean that has already
    // done its work.
    if let Err(e) = crate::api::project::retire_what_no_run_needs(state, owner.project_id).await {
        tracing::warn!(
            target: "weft_dispatcher::projection",
            %execution_id, project_id = %owner.project_id, error = %e,
            "could not retire what this run was the last to need; the reaper will retry"
        );
    }
    Ok(owner.project_id)
}

#[cfg(test)]
mod waits_tests {
    use super::*;

    fn signal(token: &str, execution_id: Option<ExecutionId>, node: &str, is_resume: bool, kind: &str) -> crate::journal::SignalRegistration {
        crate::journal::SignalRegistration {
            instance: None,
            activation_trigger: None,
            source_version: None,
            setup_execution_id: None,
            program: None,
            token: token.into(),
            tenant_id: "t".into(),
            project_id: uuid::Uuid::from_u128(0x100),
            execution_id,
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
        }
    }

    /// A run's waits are its own resume signals, in order; entry
    /// signals and another run's waits are not.
    #[test]
    fn a_runs_waits_are_its_resume_signals() {
        let (mine, other) = (ExecutionId::new_v4(), ExecutionId::new_v4());
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
        let mine = ExecutionId::new_v4();
        let signals = vec![
            signal("a", Some(mine), "sweep.key", true, "timer"),
            signal("b", Some(mine), "again.key", true, "timer"),
        ];
        let waits = waits_of(&signals, mine).unwrap();
        let out: Vec<&str> = waits.iter().map(|w| w.node.as_str()).collect();
        assert_eq!(out, vec!["sweep.key", "again.key"]);
        let wire = serde_json::to_value(&waits[0]).unwrap();
        assert_eq!(wire["node"], "sweep.key");
    }
}

#[cfg(test)]
mod cancel_tests {
    use super::*;
    use weft_journal::ExecEvent;

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

    fn open_firing(execution_id: ExecutionId) -> Vec<ExecEvent> {
        vec![
            ExecEvent::NodeKicked { execution_id, node_id: "wait".into(), frames: vec![], firing: true, payload: None, port_snapshot: None, at_unix: 0 },
            ExecEvent::NodeStarted { execution_id, node_id: "wait".into(), frames: vec![], at_unix: 1 },
        ]
    }

    /// With the program, every open firing gets its `NodeCancelled`
    /// before the terminal; without one (a removed or unreadable
    /// program, a node self-test) the terminal still lands, alone.
    #[test]
    fn cancel_writes_per_node_rows_with_the_program_and_the_terminal_without() {
        let execution_id = ExecutionId::new_v4();
        let program = program();
        let with = cancel_terminal_events(execution_id, &open_firing(execution_id), Some(&program), &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = with.iter().map(|(e, _)| e.kind_str()).collect();
        assert_eq!(kinds, vec!["node_cancelled", "execution_cancelled"]);
        assert_eq!(with[0].1, format!("cancel:{execution_id}:wait:"));
        let without = cancel_terminal_events(execution_id, &open_firing(execution_id), None, &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = without.iter().map(|(e, _)| e.kind_str()).collect();
        assert_eq!(kinds, vec!["execution_cancelled"]);
        assert!(matches!(&without[0].0, ExecEvent::ExecutionCancelled { cause: Some(CancelCause::User), .. }));
    }
}

#[cfg(test)]
mod wake_tier_boundary {
    /// This module's own source, read at compile time. The test module
    /// is cut off first, since the words to look for would otherwise be
    /// found in the looking.
    fn code() -> &'static str {
        include_str!("execution.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("a split always yields a first piece")
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
                "the wake handler must not know a signal kind: found `{needle}`. \
                 What a wait wakes with is the kind's own shape and comes from \
                 the listener; minting one here means the next wakeable kind is \
                 quietly unwakeable."
            );
        }
    }
}
