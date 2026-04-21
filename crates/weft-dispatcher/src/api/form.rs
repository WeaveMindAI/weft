//! Form submission entry. Matches `/f/{token}` routes minted when a
//! node calls `ctx.await_form`. On submit, resolves the suspension
//! and wakes the execution at the suspended node with the form value.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde_json::Value;

use crate::state::DispatcherState;

pub async fn submit(
    State(_state): State<DispatcherState>,
    Path(_token): Path<String>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_IMPLEMENTED, Json(serde_json::json!({ "status": "not_implemented" })))
}
