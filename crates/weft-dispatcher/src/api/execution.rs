//! Execution lifecycle handlers. Cancel, query status, accept cost
//! reports from running workers.

use axum::{extract::{Path, State}, http::StatusCode, Json};
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
    // Phase A2+: actual execution status + recent cost aggregation.
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
