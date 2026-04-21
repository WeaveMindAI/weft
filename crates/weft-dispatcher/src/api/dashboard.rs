//! The ops dashboard UI. A single-page HTML shell served from the
//! dispatcher; fetches live state via the existing JSON endpoints
//! and renders it in the browser. Deliberately minimal: ops only, no
//! code editing. Phase B replaces the inlined HTML with a proper
//! svelte/react build embedded via rust-embed.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::IntoResponse,
};

use crate::state::DispatcherState;

const INDEX_HTML: &str = include_str!("dashboard.html");

pub async fn serve(State(_state): State<DispatcherState>, Path(_path): Path<String>) -> impl IntoResponse {
    index()
}

pub async fn serve_root(State(_state): State<DispatcherState>) -> impl IntoResponse {
    index()
}

fn index() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        INDEX_HTML,
    )
}
