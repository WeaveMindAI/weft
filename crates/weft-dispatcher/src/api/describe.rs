//! Catalog introspection. Tooling (Tangle, VS Code extension, the ops
//! dashboard's node picker) fetches the per-project or global node
//! catalog from these endpoints. Delegates to weft-compiler's
//! describe module.

use axum::{extract::{Path, State}, Json};
use serde_json::Value;

use crate::state::DispatcherState;

pub async fn nodes(State(_state): State<DispatcherState>) -> Json<Value> {
    Json(serde_json::json!({ "nodes": [], "warnings": [] }))
}

pub async fn project_catalog(
    State(_state): State<DispatcherState>,
    Path(_id): Path<String>,
) -> Json<Value> {
    Json(serde_json::json!({ "nodes": [], "warnings": [] }))
}
