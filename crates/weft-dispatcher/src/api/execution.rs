use axum::{extract::{Path, State}, Json};
use serde_json::Value;

use crate::state::DispatcherState;

pub async fn cancel(State(_state): State<DispatcherState>, Path(_color): Path<String>) -> Json<Value> {
    Json(serde_json::json!({ "status": "not_implemented" }))
}

pub async fn get(State(_state): State<DispatcherState>, Path(_color): Path<String>) -> Json<Value> {
    Json(serde_json::json!({ "status": "not_implemented" }))
}
