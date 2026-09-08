//! Execution state read endpoints. Writers journal events directly
//! to Postgres from the worker. What's left here is: cancel
//! (control), delete (cleanup), and the reader endpoints the CLI
//! and VS Code extension hit over HTTP: logs, replay,
//! list_executions, get.

use std::sync::Arc;

use weft_core::ProjectDefinition;

use axum::{extract::{Path, Query, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::exec::CancelCause;
use weft_core::Color;

use crate::authenticator::{authorize_execution, authorize_project, CallerTenant};
use crate::journal::{ExecutionPage, ExecutionQuery};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

/// The one execution of the caller's whose color starts with `prefix`
/// (a full uuid resolves to itself). 404 when nothing matches, 409
/// when the prefix is short enough to match several, naming them.
pub async fn resolve_color(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(prefix): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let prefix = prefix.to_ascii_lowercase();
    if prefix.len() < 4 || !prefix.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("'{prefix}' is not the start of a color: give at least four hex characters"),
        ));
    }
    let matches = state
        .journal
        .colors_with_prefix(caller.0.as_str(), &prefix)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("colors_with_prefix: {e}")))?;
    match matches.as_slice() {
        [] => Err((StatusCode::NOT_FOUND, format!("no execution starts with '{prefix}'"))),
        [one] => Ok(Json(serde_json::json!({ "color": one.to_string() }))),
        several => Err((
            StatusCode::CONFLICT,
            format!(
                "'{prefix}' starts more than one execution ({}, ...); give more characters",
                several.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ")
            ),
        )),
    }
}

pub async fn cancel(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(color_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let color: Color = color_str
        .parse()
        .map_err(|e: uuid::Error| (StatusCode::BAD_REQUEST, e.to_string()))?;
    authorize_execution(&*state.journal, &caller.0, color).await?;
    cancel_color(&state, color, &CancelCause::User).await.map_err(|e| {
        tracing::error!(target: "weft_dispatcher::cancel", color = %color, error = %e, "cancel_color failed");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// Cancel every color in `targets`, each with its own cause, attempting
/// ALL of them before reporting. One failing color must not strand the
/// ones after it (they would stay live with their wakes registered), and
/// a failure must not disappear either: if any cancel failed, the
/// result is an error naming every failed color and why, so a task
/// built on this is recorded failed with the real errors, never
/// completed with a count nobody reads. Returns the colors cancelled.
pub async fn cancel_colors(
    state: &DispatcherState,
    targets: &[(Color, &CancelCause)],
) -> anyhow::Result<Vec<Color>> {
    let mut cancelled = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for (color, cause) in targets {
        match cancel_color(state, *color, cause).await {
            Ok(()) => cancelled.push(*color),
            Err(e) => failures.push(format!("{color}: {e:#}")),
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
/// task for the alive owner pod. Either all of it lands or none does,
/// so a database failure mid-cancel leaves the run exactly as it was
/// and the next attempt succeeds; nothing can strip a run's wakes and
/// then fail to end it. Two paths then converge on the one observable
/// outcome (the journal reads `ExecutionCancelled`):
///
///   - When a worker Pod is alive and driving this color, the task
///     fires the per-color `CancellationFlag` (~50ms), the loop driver
///     exits, and the worker's own terminal write finds the rows
///     already there and skips (idempotent).
///   - With no worker driving it (a suspended run, no pod at all), the
///     rows written here ARE the terminal.
///
/// After the commit the listener forgets the stripped signals in RAM
/// (the durable row is already gone, so a late fire finds nothing),
/// and the journal bridge publishes the new rows onto the project's
/// SSE bus so the frontend exits "Cancelling...".
pub async fn cancel_color(
    state: &DispatcherState,
    color: Color,
    cause: &CancelCause,
) -> anyhow::Result<()> {
    tracing::info!(
        target: "weft_dispatcher::cancel",
        color = %color,
        %cause,
        "cancel_color start"
    );
    // The per-node cancels come off the fold, which needs the run's
    // program; a color with none (never started, a node self-test)
    // has no nodes to cancel, and a program that cannot be found is no
    // reason to leave the run running: the terminal lands anyway.
    let program = program_for_cancel(state, color).await?;
    let write = state.journal.cancel_execution(color, program.as_deref(), cause).await?;
    state
        .listeners
        .unregister_many(&state.pg_pool, &write.removed)
        .await;
    tracing::info!(
        target: "weft_dispatcher::cancel",
        color = %color,
        signals_removed = write.removed.len(),
        task_enqueued = write.task_enqueued,
        node_cancellations = ?write.node_cancellations,
        "cancel committed"
    );
    Ok(())
}

/// THE definition of a dispatcher-side cancel write: the ordered
/// `(event, dedup_key)` list that flips a color terminal. Pure, so the
/// transactional cancel writers (`Journal::cancel_execution` and
/// `Journal::cancel_never_claimed_execution`) emit IDENTICAL rows and
/// can never drift on the ordering rule, the dedup-key format, or the
/// closure-emission policy.
///
/// Per-node cancellations come BEFORE `ExecutionCancelled` (always the last
/// entry). Otherwise a partial run that journaled the terminal event first
/// would set has-terminal=true, and a retry would skip the per-node writes
/// forever, leaving node UI states stuck on "running". Each per-node write is
/// dedup-keyed on (color, node, frame-stack) so a partial failure + retry
/// (e.g. the orphan sweep's retry-next-tick loop) collapses instead of
/// stacking a duplicate NodeCancelled row (which would also republish a
/// duplicate UI event); the terminal's key makes the row-level write safe even
/// if two cancels for the same color race past their has-terminal checks.
pub fn cancel_terminal_events(
    color: Color,
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
                color,
                Arc::new(program.clone()),
                events,
            );
            for (node_id, execs) in &snapshot.executions {
                for e in execs {
                    if e.status.is_terminal() {
                        continue;
                    }
                    let frames_key: String =
                        e.frames.iter().map(|f| f.index.to_string()).collect::<Vec<_>>().join(".");
                    writes.push((
                        ExecEvent::NodeCancelled {
                            color,
                            node_id: node_id.clone(),
                            frames: e.frames.clone(),
                            reason: reason.clone(),
                            at_unix: now,
                        },
                        format!("cancel:{color}:{node_id}:{frames_key}"),
                    ));
                }
            }
        }
        // No program: a color that never ran a node body (a node
        // self-test) has nothing per node to flip. A color whose
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
                    %color,
                    "cancelling without the run's program: the terminal is written, the \
                     per-node cancels cannot be derived"
                );
            }
        }
    }
    writes.push((
        ExecEvent::ExecutionCancelled { color, reason, cause: Some(cause.clone()), at_unix: now },
        format!("execution_cancelled:{color}"),
    ));
    Ok(writes)
}

/// The program a cancel folds with to derive its per-node cancels, or
/// `None` when the color has none to fold with: no program at all, a
/// removed project, or a program that cannot be read (logged; the
/// stop still lands on the terminal). `Err` only for the database.
pub async fn program_for_cancel(
    state: &crate::state::DispatcherState,
    color: Color,
) -> anyhow::Result<Option<Arc<ProjectDefinition>>> {
    let lookup = crate::projection::execution_program(state, color).await?;
    if let crate::projection::ProgramLookup::Unreadable(reason) = &lookup {
        tracing::warn!(
            target: "weft_dispatcher::cancel",
            %color, %reason,
            "cancelling a run whose program cannot be read: the terminal is written, the \
             per-node cancels cannot be derived"
        );
    }
    Ok(lookup.program())
}

/// The terminal outcome recorded for a color, if any. The journal is
/// the authoritative source: `Completed`/`Failed`/`Cancelled` are the
/// three terminal `exec_event` kinds. `None` means the execution is
/// still in flight. Used both for cancel-dedup and as the source of
/// truth when the in-RAM event bus drops events (broadcast `Lagged`).
pub(crate) async fn terminal_outcome(
    pool: &sqlx::PgPool,
    color: Color,
) -> anyhow::Result<Option<TerminalOutcome>> {
    // SYNC: terminal_outcome (SQL kind list) <-> crates/weft-journal/src/events.rs ExecEvent::is_execution_terminal, crates/weft-cli/src/commands/follow.rs is_terminal (SSE kind list)
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT kind FROM exec_event \
         WHERE color = $1 \
           AND kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
         LIMIT 1",
    )
    .bind(color.to_string())
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

pub(crate) async fn has_terminal_event(pool: &sqlx::PgPool, color: Color) -> anyhow::Result<bool> {
    Ok(terminal_outcome(pool, color).await?.is_some())
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
    summaries: &mut [crate::journal::ExecutionSummary],
) -> Result<(), StatusCode> {
    use std::collections::HashMap;
    let mut sets: HashMap<String, std::collections::HashSet<Color>> = HashMap::new();
    for s in summaries.iter_mut() {
        if s.status != "running" {
            continue;
        }
        if !sets.contains_key(&s.project_id) {
            let set = crate::api::project::suspended_color_set(state, &s.project_id)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            sets.insert(s.project_id.clone(), set);
        }
        if sets[&s.project_id].contains(&s.color) {
            s.status = "waiting_for_input".to_string();
        }
    }
    Ok(())
}

pub async fn get(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(color_str): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    authorize_execution(&*state.journal, &caller.0, color)
        .await
        .map_err(|(s, _)| s)?;
    // Direct point-lookup by color (authorization above already proved the
    // caller owns it), so an execution older than any list window still
    // resolves instead of 404ing.
    let summary = state
        .journal
        .execution_summary(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let mut batch = [summary];
    overlay_suspended(&state, &mut batch).await?;
    let [summary] = batch;
    Ok(Json(serde_json::json!({
        "color": summary.color.to_string(),
        "project_id": summary.project_id,
        "entry_node": summary.entry_node,
        "status": summary.status,
        "phase": summary.phase,
        "started_at": summary.started_at,
        "completed_at": summary.completed_at,
        "tags": summary.tags,
    })))
}

#[derive(Debug, Serialize)]
pub struct LogLineOut {
    pub at_unix: u64,
    pub level: String,
    /// The firing this line is about: the node, and the loop
    /// iteration it was in. Absent on the wire for a run-level line
    /// (the run itself failing or being cancelled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frames: weft_core::LoopFrames,
    pub message: String,
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

/// A run's log: the tail, and the limit that cut it, so a reader who
/// sent none still knows how long a full page is.
#[derive(Debug, Serialize)]
pub struct LogsOut {
    pub limit: u32,
    pub lines: Vec<LogLineOut>,
}

pub async fn list_logs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(color_str): Path<String>,
    Query(params): Query<ListLogsParams>,
) -> Result<Json<LogsOut>, (StatusCode, String)> {
    let color: Color = color_str
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("'{color_str}' is not a color (a uuid)")))?;
    authorize_execution(&*state.journal, &caller.0, color).await?;
    let limit = params.limit.unwrap_or(DEFAULT_LOG_LINES);
    if !(1..=MAX_LOG_LINES).contains(&limit) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("limit is {limit}; one read holds between 1 and {MAX_LOG_LINES} lines"),
        ));
    }
    // The journal's error names the color and `weft clean` when a row
    // no longer decodes; the reader gets it, not a bare 500.
    let entries = state
        .journal
        .logs_for(color, limit)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let lines = entries
        .into_iter()
        .map(|e| LogLineOut {
            at_unix: e.at_unix,
            level: e.level,
            node: e.node,
            frames: e.frames,
            message: e.message,
        })
        .collect();
    Ok(Json(LogsOut { limit, lines }))
}

/// Replay a past execution: returns every journaled event the SSE
/// stream would have emitted live, shaped as `DispatcherEvent` so the
/// webview's live-SSE handler can process them with the same code
/// path. Bus events (joined/left/message) ride along too, so the
/// inspector's IRC log renders on replay exactly as it did live.
///
/// Terminal events (`ExecutionCompleted` / `ExecutionFailed` /
/// `ExecutionCancelled`) are NOT synthesized from the execution
/// summary. They are already in the journal log when the execution
/// settled (the summary's status is itself derived from the presence
/// of that journal row), and `ExecutionProjector` projects them
/// faithfully (carrying the real outputs / error / reason). Synthesizing
/// a duplicate from the summary would emit a lossy second terminal
/// (empty payloads) that overrides the real one on the receiving
/// side. A still-running execution has no terminal in the log; the
/// live SSE delivers it when it lands.
pub async fn replay(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(color_str): Path<String>,
) -> Result<Json<Vec<crate::events::LiveEvent>>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    // Resolve + tenant-gate in the ONE place that owns "who owns this
    // execution": a lookup failure is 500, an unknown or cross-tenant
    // color is 404, and the resolved project rides back for the
    // replay's event attribution.
    let project_id =
        authorize_execution(&*state.journal, &caller.0, color).await.map_err(|(s, _)| s)?.project_id;
    // The full ExecEvent log, folded over the run's program through
    // the SAME projection the live `journal_bridge` runs, so replay and
    // live cannot drift; bus and caller events ride along.
    // Lossy read: the inspector renders what exists, and every row
    // that no longer decodes lands below as its own JournalCorruption
    // entry, naming `weft clean`, instead of taking the response down.
    let (raw_events, mut undecodable) =
        state.journal.events_log_lossy(color).await.map_err(|e| {
            tracing::error!(
                target: "weft_dispatcher::api",
                %color, error = %e,
                "replay: reading the journal failed"
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    // Only the database itself fails the read; a program that cannot
    // be found paints the run as unreadable, the way the live bridge
    // does for the same journal, and names `weft clean` below.
    let program = match crate::projection::execution_program(&state, color).await.map_err(|e| {
        tracing::error!(
            target: "weft_dispatcher::api",
            %color, error = %e,
            "replay: looking up the run's program failed"
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })? {
        crate::projection::ProgramLookup::Unreadable(reason) => {
            undecodable.push(reason);
            crate::projection::ProgramLookup::Unreadable(String::new())
        }
        found => found,
    };
    let mut projector = crate::projection::ExecutionProjector::new(color, program, project_id.clone());
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
    for reason in undecodable {
        out.push(crate::events::IdentifiedEvent::transient(DispatcherEvent::JournalCorruption {
            color,
            project_id: project_id.clone(),
            site: weft_core::primitive::CorruptionSite::UndecodableRow,
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
    pub project_id: Option<String>,
    /// Inclusive lower bound on start time (unix seconds).
    pub started_after: Option<u64>,
    /// Exclusive upper bound on start time (unix seconds).
    pub started_before: Option<u64>,
    /// Only runs of this phase (`fire`, `trigger_setup`, `infra_setup`).
    pub phase: Option<weft_core::context::Phase>,
}

const DEFAULT_PAGE: u32 = 50;
const MAX_PAGE: u32 = 200;

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
/// find the color to dump logs for.
pub async fn latest_for_project(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
) -> Result<Json<crate::journal::ExecutionSummary>, StatusCode> {
    let id = id_str.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    authorize_project(&state, &caller.0, id)
        .await
        .map_err(|(s, _)| s)?;
    // Ask SQL for exactly the newest execution of this project (project-filtered,
    // limit 1), rather than scanning a fixed window and filtering in memory.
    let query = ExecutionQuery {
        limit: 1,
        offset: 0,
        project_id: Some(id_str),
        started_after: None,
        started_before: None,
        phase: None,
    };
    let mut page = state
        .journal
        .list_executions(caller.0.as_str(), &query)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    overlay_suspended(&state, &mut page.executions).await?;
    page.executions.into_iter().next().map(Json).ok_or(StatusCode::NOT_FOUND)
}

pub async fn delete_execution(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(color_str): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    // The gate already read the owning row; keep it rather than asking
    // again. Its tenant is the one the storage prefix was WRITTEN
    // under, so the wipe below addresses the same bytes the run
    // created even for a project that has since been removed (asking
    // the project store for the tenant would fail exactly there).
    let owner = authorize_execution(&*state.journal, &caller.0, color).await.map_err(|(s, _)| s)?;
    // Wipe the execution's storage folder (kept survivors included:
    // `weft clean <color>` IS the explicit removal verb for them)
    // BEFORE the journal rows go, while the color's row still exists.
    // A spent color's storage address dies with its journal history;
    // every failure below aborts so a retry can still wipe, never
    // orphaning the prefix.
    crate::storage::wipe_prefix(&state, &format!("{}/exec/{color}/", owner.tenant))
        .await
        .map_err(|e| {
            tracing::error!(
                target: "weft_dispatcher::storage",
                %color, error = %e,
                "could not wipe execution storage; aborting clean so a retry can"
            );
            StatusCode::SERVICE_UNAVAILABLE
        })?;
    state
        .journal
        .delete_execution(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
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

    fn open_firing(color: Color) -> Vec<ExecEvent> {
        vec![
            ExecEvent::NodeKicked { color, node_id: "wait".into(), firing: true, payload: None, port_snapshot: None, at_unix: 0 },
            ExecEvent::NodeStarted { color, node_id: "wait".into(), frames: vec![], at_unix: 1 },
        ]
    }

    /// With the program, every open firing gets its `NodeCancelled`
    /// before the terminal; without one (a removed or unreadable
    /// program, a node self-test) the terminal still lands, alone.
    #[test]
    fn cancel_writes_per_node_rows_with_the_program_and_the_terminal_without() {
        let color = Color::new_v4();
        let program = program();
        let with = cancel_terminal_events(color, &open_firing(color), Some(&program), &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = with.iter().map(|(e, _)| e.kind_str()).collect();
        assert_eq!(kinds, vec!["node_cancelled", "execution_cancelled"]);
        assert_eq!(with[0].1, format!("cancel:{color}:wait:"));
        let without = cancel_terminal_events(color, &open_firing(color), None, &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = without.iter().map(|(e, _)| e.kind_str()).collect();
        assert_eq!(kinds, vec!["execution_cancelled"]);
        assert!(matches!(&without[0].0, ExecEvent::ExecutionCancelled { cause: Some(CancelCause::User), .. }));
    }
}
