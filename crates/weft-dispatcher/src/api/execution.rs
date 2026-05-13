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
        .unregister_many_if_alive(&state.pg_pool, &removed)
        .await;

    let project_id = match state.journal.execution_project(color).await? {
        Some(p) => p,
        None => {
            tracing::warn!(
                target: "weft_dispatcher::cancel",
                color = %color,
                "no project_id for color; nothing to do"
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
    journal_cancel_terminals(state, color).await?;

    Ok(())
}

/// Write NodeCancelled per non-terminal node + ExecutionCancelled
/// directly from the dispatcher. Used when
/// the worker isn't going to do it (suspended execution, no live
/// worker, race window). Idempotent: skips entirely if the journal
/// already shows a terminal for this color.
async fn journal_cancel_terminals(state: &DispatcherState, color: Color) -> anyhow::Result<()> {
    use weft_journal::ExecEvent;

    if has_terminal_event(&state.pg_pool, color).await? {
        tracing::info!(
            target: "weft_dispatcher::cancel",
            color = %color,
            "terminal already journaled; skipping dispatcher-side write"
        );
        return Ok(());
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Per-node cancellation MUST land before ExecutionCancelled.
    // Otherwise a partial run that journaled the terminal event
    // first would set has_terminal_event=true, and a retry would
    // skip the per-node writes forever, leaving node UI states
    // stuck on "running".
    let events = state.journal.events_log(color).await?;
    let snapshot = weft_journal::fold_to_snapshot(color, &events);
    let mut wrote_node_count = 0usize;
    for (node_id, execs) in &snapshot.executions {
        for e in execs {
            if e.status.is_terminal() {
                continue;
            }
            let event = ExecEvent::NodeCancelled {
                color,
                node_id: node_id.clone(),
                lane: e.lane.clone(),
                reason: "Cancelled by user".to_string(),
                at_unix: now,
            };
            state.journal.record_event(&event).await?;
            wrote_node_count += 1;
        }
    }

    let terminal = ExecEvent::ExecutionCancelled {
        color,
        reason: "Cancelled by user".to_string(),
        at_unix: now,
    };
    state.journal.record_event(&terminal).await?;
    tracing::info!(
        target: "weft_dispatcher::cancel",
        color = %color,
        node_cancellations = wrote_node_count,
        "journaled ExecutionCancelled"
    );
    Ok(())
}

async fn has_terminal_event(pool: &sqlx::PgPool, color: Color) -> anyhow::Result<bool> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT kind FROM exec_event \
         WHERE color = $1 \
           AND kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
         LIMIT 1",
    )
    .bind(color.to_string())
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
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

/// Replay a past execution: returns journaled node events shaped
/// as `DispatcherEvent` so the webview's live-SSE handler can
/// process them with the same code path.
///
/// We also surface the terminal execution_completed /
/// execution_failed event at the end when the summary row tells
/// us the run settled. Without that, the extension's ActionBar
/// can't flip its `isRunning` flag off and the Stop Execution
/// button stays visible.
pub async fn replay(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<Json<Vec<DispatcherEvent>>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let project_id = state
        .journal
        .execution_project(color)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let raw_events = state
        .journal
        .events_for(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut out: Vec<DispatcherEvent> = raw_events
        .into_iter()
        .map(|e| node_event_to_dispatcher(e, project_id.clone()))
        .collect();

    // Infer the terminal state from the execution summary so the
    // UI sees ExecutionCompleted / ExecutionFailed and flips out
    // of "running" mode. A still-running exec (no terminal yet)
    // returns only node events; the live SSE will deliver the
    // terminal event when it happens.
    if let Some(summary) = state
        .journal
        .list_executions(500)
        .await
        .ok()
        .and_then(|list| list.into_iter().find(|s| s.color == color))
    {
        match summary.status.to_ascii_lowercase().as_str() {
            "completed" => out.push(DispatcherEvent::ExecutionCompleted {
                color,
                project_id: project_id.clone(),
                outputs: serde_json::Value::Null,
            }),
            "failed" => out.push(DispatcherEvent::ExecutionFailed {
                color,
                project_id: project_id.clone(),
                // Per-node errors are already in the stream; the
                // summary doesn't carry one so we leave it empty.
                error: String::new(),
            }),
            "cancelled" => out.push(DispatcherEvent::ExecutionCancelled {
                color,
                project_id: project_id.clone(),
                reason: "Cancelled by user".to_string(),
            }),
            _ => {}
        }
    }
    Ok(Json(out))
}

/// Translate a journaled per-node event into the SSE-shaped
/// `DispatcherEvent` the extension's apply handler expects. Same
/// field names live wire uses (`node`, `lane`, `project_id`).
fn node_event_to_dispatcher(
    e: crate::journal::NodeExecEvent,
    project_id: String,
) -> DispatcherEvent {
    use crate::journal::NodeExecKind;
    match e.kind {
        NodeExecKind::Started => DispatcherEvent::NodeStarted {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            input: e.input.unwrap_or(serde_json::Value::Null),
            project_id,
        },
        NodeExecKind::Suspended => DispatcherEvent::NodeSuspended {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            token: e.token.unwrap_or_default(),
            project_id,
        },
        NodeExecKind::Resumed => DispatcherEvent::NodeResumed {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            token: e.token.unwrap_or_default(),
            value: e.value.unwrap_or(serde_json::Value::Null),
            project_id,
        },
        NodeExecKind::Cancelled => DispatcherEvent::NodeCancelled {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            reason: e.reason.unwrap_or_default(),
            project_id,
        },
        NodeExecKind::Completed => DispatcherEvent::NodeCompleted {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            output: e.output.unwrap_or(serde_json::Value::Null),
            project_id,
        },
        NodeExecKind::Failed => DispatcherEvent::NodeFailed {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            error: e.error.unwrap_or_default(),
            project_id,
        },
        NodeExecKind::Skipped => DispatcherEvent::NodeSkipped {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            project_id,
        },
    }
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
    state
        .journal
        .delete_execution(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}
