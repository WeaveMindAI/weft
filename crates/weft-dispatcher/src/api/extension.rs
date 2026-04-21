//! Browser extension API. Ported from v1 dashboard proxy; extension
//! now talks directly to dispatcher. Opaque tokens stay. Same shape
//! as v1's `/ext/*` surface.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde_json::Value;

use crate::state::DispatcherState;

pub async fn list_tasks(State(_state): State<DispatcherState>, Path(_token): Path<String>) -> Json<Value> {
    Json(serde_json::json!({ "tasks": [] }))
}

pub async fn complete_task(
    State(_state): State<DispatcherState>,
    Path((_token, _execution_id)): Path<(String, String)>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_IMPLEMENTED, Json(serde_json::json!({ "status": "not_implemented" })))
}

pub async fn submit_trigger(
    State(_state): State<DispatcherState>,
    Path((_token, _trigger_task_id)): Path<(String, String)>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_IMPLEMENTED, Json(serde_json::json!({ "status": "not_implemented" })))
}

pub async fn dismiss_action(
    State(_state): State<DispatcherState>,
    Path((_token, _action_id)): Path<(String, String)>,
) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_IMPLEMENTED, Json(serde_json::json!({ "status": "not_implemented" })))
}

pub async fn health(State(_state): State<DispatcherState>, Path(_token): Path<String>) -> Json<Value> {
    Json(serde_json::json!({ "ok": true }))
}
