//! Execution lifecycle handlers. Cancel, query status, accept cost
//! reports and suspension records from running workers.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::{Color, CostReport};

use crate::state::DispatcherState;

pub async fn cancel(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    state.journal.cancel(color).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get(State(_state): State<DispatcherState>, Path(_color): Path<String>) -> Json<Value> {
    // Phase A2+: execution status + recent cost aggregation.
    Json(serde_json::json!({ "status": "unknown" }))
}

pub async fn record_cost(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
    Json(report): Json<CostReport>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    state
        .journal
        .record_cost(color, report)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
    /// Opaque token the worker can surface to humans. For a form
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
    metadata
        .as_object_mut()
        .map(|m| m.insert("project_id".into(), Value::String(req.project_id.clone())));
    if metadata.as_object().is_none() {
        // Coerce non-object metadata into an object so we can
        // attach project_id without losing the original.
        metadata = serde_json::json!({ "project_id": req.project_id, "original": metadata });
    }

    let token = state
        .journal
        .record_suspension(color, &req.node_id, metadata)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;

    let form_url = format!("http://localhost:{}/f/{}", state.config.http_port, token);
    Ok(Json(SuspensionResponse { token, form_url }))
}
