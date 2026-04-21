//! Execution lifecycle handlers. Workers POST to these endpoints
//! while they run; the CLI, VS Code extension, and dashboard GET /
//! subscribe to learn about state changes.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::{Color, CostReport};

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

pub async fn record_cost(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
    Json(report): Json<CostReport>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let report_for_event = report.clone();
    state
        .journal
        .record_cost(color, report)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Ok(Some(project_id)) = state.journal.execution_project(color).await {
        state
            .events
            .publish(DispatcherEvent::CostReported {
                color,
                project_id,
                service: report_for_event.service,
                amount_usd: report_for_event.amount_usd,
            })
            .await;
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub struct SuspensionRequest {
    pub node_id: String,
    pub project_id: String,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Serialize)]
pub struct SuspensionResponse {
    /// Opaque token the worker surfaces to humans. For a form
    /// suspension, the public URL is `{dispatcher}/f/{token}`.
    pub token: String,
    /// Full form URL ready to serve to humans.
    pub form_url: String,
}

pub async fn record_suspension(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
    Json(req): Json<SuspensionRequest>,
) -> Result<Json<SuspensionResponse>, (StatusCode, String)> {
    let color: Color = color_str
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad color".into()))?;

    // Stamp project_id into metadata so the form handler can find
    // the binary to spawn when the submission lands.
    let mut metadata = req.metadata;
    if let Some(obj) = metadata.as_object_mut() {
        obj.insert("project_id".into(), Value::String(req.project_id.clone()));
    } else {
        metadata = serde_json::json!({
            "project_id": req.project_id,
            "original": metadata,
        });
    }

    let metadata_for_event = metadata.clone();
    let token = state
        .journal
        .record_suspension(color, &req.node_id, metadata)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;

    state
        .events
        .publish(DispatcherEvent::ExecutionSuspended {
            color,
            node: req.node_id.clone(),
            token: token.clone(),
            metadata: metadata_for_event,
            project_id: req.project_id.clone(),
        })
        .await;

    let form_url = format!("http://localhost:{}/f/{}", state.config.http_port, token);
    Ok(Json(SuspensionResponse { token, form_url }))
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StatusReport {
    Started { entry_node: String },
    Completed { outputs: Value },
    Failed { error: String },
}

pub async fn report_status(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
    Json(report): Json<StatusReport>,
) -> Result<StatusCode, (StatusCode, String)> {
    let color: Color = color_str
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad color".into()))?;
    let project_id = state
        .journal
        .execution_project(color)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown color".into()))?;

    let event = match report {
        StatusReport::Started { entry_node } => DispatcherEvent::ExecutionStarted {
            color,
            entry_node,
            project_id,
        },
        StatusReport::Completed { outputs } => DispatcherEvent::ExecutionCompleted {
            color,
            project_id,
            outputs,
        },
        StatusReport::Failed { error } => DispatcherEvent::ExecutionFailed {
            color,
            project_id,
            error,
        },
    };
    state.events.publish(event).await;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub struct LogLineIn {
    #[serde(default = "default_level")]
    pub level: String,
    pub message: String,
}

fn default_level() -> String {
    "info".into()
}

pub async fn append_log(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
    Json(line): Json<LogLineIn>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    state
        .journal
        .append_log(color, &line.level, &line.message)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
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
