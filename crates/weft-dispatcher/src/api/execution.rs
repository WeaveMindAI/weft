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

/// Replay a past execution: returns all node events in order so
/// the webview can animate the execution from its beginning.
pub async fn replay(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<Json<Vec<crate::journal::NodeExecEvent>>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let events = state
        .journal
        .events_for(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(events))
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
