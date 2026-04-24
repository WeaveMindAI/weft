//! Execution state read endpoints. Writers (cost, log, suspension,
//! node events, status) live on the worker-to-dispatcher WebSocket
//! in `api::ws`. What's left here is: cancel (control), delete
//! (cleanup), and the reader endpoints the CLI, VS Code extension,
//! and dashboard hit over HTTP: logs, replay, list_executions, get.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use weft_core::Color;

use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

pub async fn cancel(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let project_id = state.journal.execution_project(color).await.ok().flatten();
    state.journal.cancel(color).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(project_id) = project_id {
        state
            .events
            .publish(DispatcherEvent::ExecutionFailed {
                color,
                project_id,
                error: "cancelled".into(),
            })
            .await;
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get(State(_state): State<DispatcherState>, Path(_color): Path<String>) -> Json<Value> {
    // Phase B: execution status + cost aggregation.
    Json(serde_json::json!({ "status": "unknown" }))
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
