//! The ops dashboard UI. Static SvelteKit/React build output is
//! bundled into the dispatcher binary (via rust-embed in phase A2) and
//! served from `/dashboard/*`.
//!
//! This is an ops dashboard only (watch executions, see URLs, manage
//! projects). It does NOT do code editing; that lives in the VS Code
//! extension.

use axum::{extract::{Path, State}, http::StatusCode, response::IntoResponse};

use crate::state::DispatcherState;

pub async fn serve(State(_state): State<DispatcherState>, Path(_path): Path<String>) -> impl IntoResponse {
    (StatusCode::NOT_IMPLEMENTED, "dashboard ui not yet bundled")
}
