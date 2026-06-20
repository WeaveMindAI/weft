//! Execution state read endpoints. Writers journal events directly
//! to Postgres from the worker. What's left here is: cancel
//! (control), delete (cleanup), and the reader endpoints the CLI
//! and VS Code extension hit over HTTP: logs, replay,
//! list_executions, get.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use weft_core::Color;

use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

pub async fn cancel(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let color: Color = color_str
        .parse()
        .map_err(|e: uuid::Error| (StatusCode::BAD_REQUEST, e.to_string()))?;
    cancel_color(&state, color).await.map_err(|e| {
        tracing::error!(target: "weft_dispatcher::cancel", color = %color, error = %e, "cancel_color failed");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// Cancel a single execution.
///
/// Two paths converge to a single observable outcome (the journal
/// reaches `ExecutionCancelled`):
///
///   - When a worker Pod is alive for this project, it might be
///     actively running this color's loop driver. We enqueue a
///     `cancel_execution` task; the worker fires the per-color
///     `CancellationFlag`, the loop driver exits, the worker would
///     try to journal terminals (idempotent: skips if already done).
///   - In every case, the dispatcher writes the terminal events
///     itself (NodeCancelled per non-terminal node + ExecutionCancelled).
///     This handles the suspended-execution case (no Pod alive, the
///     worker exited cleanly when it stalled) and races where the
///     worker is alive but not running this color.
///
/// Order matters:
///   1. Strip wake signals so webhooks / timers / forms can no
///      longer revive the execution.
///   2. Enqueue the cancel task IF a live worker exists. We don't
///      want orphan tasks accumulating for projects with no worker
///      (the queue would leak).
///   3. Journal NodeCancelled for non-terminal nodes (the worker's
///      same code path is idempotent on `has_terminal_event` so it
///      won't double-write).
///   4. Journal ExecutionCancelled.
///   5. The journal bridge polls these new rows and publishes them
///      onto the project's SSE bus so the frontend exits the
///      "Cancelling..." pending state.
pub async fn cancel_color(state: &DispatcherState, color: Color) -> anyhow::Result<()> {
    tracing::info!(
        target: "weft_dispatcher::cancel",
        color = %color,
        "cancel_color start"
    );

    // 1. Strip wake-signal registrations. Must be first: if we
    //    journaled terminals first, a webhook could fire in the
    //    gap and resume a dead execution. A DB failure here MUST
    //    abort the cancel: continuing past it leaves the wake
    //    signals registered, so the next webhook revives a
    //    "cancelled" execution.
    let removed = state
        .journal
        .signal_remove_for_color(color)
        .await?;
    tracing::info!(
        target: "weft_dispatcher::cancel",
        color = %color,
        signals_removed = removed.len(),
        "wake signals stripped"
    );
    state
        .listeners
        .unregister_many(&state.pg_pool, &removed)
        .await;

    let project_id = match state.journal.execution_project(color).await? {
        crate::journal::ColorLookup::Found(p) => p,
        crate::journal::ColorLookup::NotFound => {
            tracing::warn!(
                target: "weft_dispatcher::cancel",
                color = %color,
                "no project_id for color; nothing to do"
            );
            return Ok(());
        }
        crate::journal::ColorLookup::Corrupt => {
            tracing::warn!(
                target: "weft_dispatcher::cancel",
                color = %color,
                "journal row for color is corrupt; cannot resolve project; nothing to do"
            );
            return Ok(());
        }
    };

    // 2. Always enqueue cancel_execution. If a worker is alive AND
    //    is currently running this color, the task fires the
    //    per-color CancellationFlag fast (~50ms), the loop driver
    //    exits, and the worker stops emitting node events. If no
    //    worker is running this color (suspended, or no worker at
    //    all), the task is a harmless no-op when claimed (or
    //    eventually reaped). The dispatcher's terminal-journal
    //    write below still runs in every case, so the frontend's
    //    "Cancelling..." state always exits.
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let enqueued = crate::task_kinds::execute::enqueue_cancel(
        &state.pg_pool,
        &project_id,
        color,
        Some(tenant.as_str()),
    )
    .await?;
    if enqueued {
        tracing::info!(
            target: "weft_dispatcher::cancel",
            color = %color,
            project = %project_id,
            "cancel task enqueued"
        );
    } else {
        tracing::debug!(
            target: "weft_dispatcher::cancel",
            color = %color,
            project = %project_id,
            "no live worker pod; cancel is a no-op (execution already terminal)"
        );
    }

    // 3 + 4. Journal terminal events directly. Done in every path
    // (live worker or not). The worker's own terminal-write path
    // is idempotent and skips if these rows already exist.
    journal_cancel_terminals(state, color, "Cancelled by user").await?;

    Ok(())
}

/// Write NodeCancelled per non-terminal node + ExecutionCancelled
/// directly from the dispatcher. Used when
/// the worker isn't going to do it (suspended execution, no live
/// worker, race window). Idempotent: skips entirely if the journal
/// already shows a terminal for this color, so it never stacks a second
/// terminal on a color a worker may have already finished (the canonical
/// dispatcher-side terminal writer; other call sites must route through it
/// rather than recording `ExecutionCancelled` directly).
pub(crate) async fn journal_cancel_terminals(
    state: &DispatcherState,
    color: Color,
    reason: &str,
) -> anyhow::Result<()> {
    use weft_journal::ExecEvent;

    if has_terminal_event(&state.pg_pool, color).await? {
        tracing::info!(
            target: "weft_dispatcher::cancel",
            color = %color,
            "terminal already journaled; skipping dispatcher-side write"
        );
        return Ok(());
    }

    let now = crate::lease::now_unix() as u64;

    // Per-node cancellation MUST land before ExecutionCancelled.
    // Otherwise a partial run that journaled the terminal event
    // first would set has_terminal_event=true, and a retry would
    // skip the per-node writes forever, leaving node UI states
    // stuck on "running".
    let events = state.journal.events_log(color).await?;
    let snapshot = weft_journal::fold_to_snapshot(color, &events);
    // `snapshot.corruptions` is intentionally not consumed here. The
    // cancel handler only needs the executions map to know which
    // nodes are still non-terminal. The inspector's `/replay` path
    // is the user-visible surface for corruptions; `report_corruption`
    // already logged each row at `error!` level for ops.
    let mut wrote_node_count = 0usize;
    for (node_id, execs) in &snapshot.executions {
        for e in execs {
            if e.status.is_terminal() {
                continue;
            }
            let event = ExecEvent::NodeCancelled {
                color,
                node_id: node_id.clone(),
                frames: e.frames.clone(),
                reason: reason.to_string(),
                // Dispatcher-side catch-up cancel only flips records
                // terminal; the closure cascade is the worker/cleanup's
                // job, so no per-node closures ride here.
                closure_emissions: Vec::new(),
                at_unix: now,
            };
            // Dedup-key each per-node write so a partial failure + retry (e.g.
            // the orphan sweep's retry-next-tick loop, which re-runs this whole
            // function) collapses instead of stacking a duplicate NodeCancelled
            // row (which would also republish a duplicate UI event). The key is
            // stable per (color, node, frame-stack).
            let frames_key: String =
                e.frames.iter().map(|f| f.index.to_string()).collect::<Vec<_>>().join(".");
            let dedup = format!("cancel:{color}:{node_id}:{frames_key}");
            state.journal.record_event_dedup(&event, &dedup).await?;
            wrote_node_count += 1;
        }
    }

    let terminal = ExecEvent::ExecutionCancelled {
        color,
        reason: reason.to_string(),
        at_unix: now,
    };
    // Dedup the terminal too (idempotent re-run): a re-call also short-circuits
    // at `has_terminal_event` above, but the key makes the row-level write safe
    // even if two cancels for the same color race past that check.
    state
        .journal
        .record_event_dedup(&terminal, &format!("execution_cancelled:{color}"))
        .await?;
    tracing::info!(
        target: "weft_dispatcher::cancel",
        color = %color,
        node_cancellations = wrote_node_count,
        "journaled ExecutionCancelled"
    );
    Ok(())
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

pub async fn get(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let summary = state
        .journal
        .list_executions(1024)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .find(|s| s.color == color)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({
        "color": summary.color.to_string(),
        "project_id": summary.project_id,
        "entry_node": summary.entry_node,
        "status": summary.status,
        "started_at": summary.started_at,
        "completed_at": summary.completed_at,
    })))
}

#[derive(Debug, Serialize)]
pub struct LogLineOut {
    pub at_unix: u64,
    pub level: String,
    pub message: String,
}

pub async fn list_logs(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<Json<Vec<LogLineOut>>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let entries = state
        .journal
        .logs_for(color, 1_000)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        entries
            .into_iter()
            .map(|e| LogLineOut {
                at_unix: e.at_unix,
                level: e.level,
                message: e.message,
            })
            .collect(),
    ))
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
/// of that journal row), and `to_dispatcher_events` projects them
/// faithfully (carrying the real outputs / error / reason). Synthesizing
/// a duplicate from the summary would emit a lossy second terminal
/// (empty payloads) that overrides the real one on the receiving
/// side. A still-running execution has no terminal in the log; the
/// live SSE delivers it when it lands.
pub async fn replay(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<Json<Vec<DispatcherEvent>>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    // `execution_project` errors propagate as 500. An unknown color
    // is also fatal here: replaying events for a color we can't
    // attribute to a project would emit them on the empty-string
    // project bucket, which no SSE subscriber listens to. Surface
    // NotFound as 404; a corrupt journal row (already logged loud at
    // the decode site) is a server-side defect, so 500.
    let project_id = match state
        .journal
        .execution_project(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        crate::journal::ColorLookup::Found(p) => p,
        crate::journal::ColorLookup::NotFound => return Err(StatusCode::NOT_FOUND),
        crate::journal::ColorLookup::Corrupt => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };
    // Use the full ExecEvent log so bus events ride along with node
    // lifecycle events. The same `to_dispatcher_events` mapper the
    // live `journal_bridge` uses runs over the log; replay and live
    // share the projection so they can't drift.
    let raw_events = state
        .journal
        .events_log(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // Fold once for corruption detection. The fold is otherwise
    // unused here (the replay sends raw ExecEvent projections, not
    // snapshot state), but it's cheap relative to the network round-
    // trip and gives the inspector a one-shot list of any rows that
    // could not be applied. The same fold runs in the engine resume
    // path and the cancel handler; this is the inspector's window.
    let snapshot = weft_journal::fold_to_snapshot(color, &raw_events);
    let mut out: Vec<DispatcherEvent> = raw_events
        .into_iter()
        .flat_map(|e| {
            crate::journal_bridge::to_dispatcher_events(&e, project_id.clone())
        })
        .collect();
    for c in snapshot.corruptions {
        out.push(DispatcherEvent::JournalCorruption {
            color,
            project_id: project_id.clone(),
            site: c.site,
            reason: c.reason,
        });
    }
    Ok(Json(out))
}

pub async fn list_executions(
    State(state): State<DispatcherState>,
) -> Result<Json<Vec<crate::journal::ExecutionSummary>>, StatusCode> {
    let summaries = state
        .journal
        .list_executions(200)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(summaries))
}

/// Return the most recent execution for a project, or 404 if
/// the project has none. Used by `weft logs` (no-arg form) to
/// find the color to dump logs for.
pub async fn latest_for_project(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<crate::journal::ExecutionSummary>, StatusCode> {
    let summaries = state
        .journal
        .list_executions(500)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    summaries
        .into_iter()
        .find(|s| s.project_id == id_str)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn delete_execution(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    // Wipe the execution's storage folder (kept survivors included:
    // `weft clean <color>` IS the explicit removal verb for them)
    // BEFORE the journal rows go, while the color->project mapping
    // still exists. A spent color's storage address dies with its
    // journal history.
    if let Ok(Some(project_id)) = state.journal.execution_project(color).await.map(|p| p.found())
    {
        let tenant = state.tenant_router.tenant_for_project(&project_id);
        let has_box = crate::storage_box::box_exists(&state.pg_pool, tenant.as_str())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if has_box {
            let box_url = crate::storage_box::box_url(&state, &tenant);
            state
                .storage_admin
                .wipe_prefix(&box_url, &format!("exec/{color}/"))
                .await
                .map_err(|e| {
                    tracing::error!(
                        target: "weft_dispatcher::storage",
                        %color, error = %e,
                        "could not wipe execution storage; aborting clean so a retry can"
                    );
                    StatusCode::SERVICE_UNAVAILABLE
                })?;
        }
    }
    state
        .journal
        .delete_execution(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}
