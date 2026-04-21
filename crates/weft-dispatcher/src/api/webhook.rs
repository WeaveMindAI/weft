//! Webhook entry. Matches `/w/{token}/{*path}` routes minted at
//! project activation. Looks up the token in the wake index, mints a
//! new color, schedules the entry node.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde_json::Value;

use crate::state::DispatcherState;

pub async fn handle(
    State(_state): State<DispatcherState>,
    Path((_token, _path)): Path<(String, String)>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_IMPLEMENTED, Json(serde_json::json!({ "status": "not_implemented" })))
}
