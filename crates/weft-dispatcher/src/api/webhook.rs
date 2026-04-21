//! Webhook entry. `POST /w/{token}` or `POST /w/{token}/{*path}`:
//! look up the token in the journal, mint a new color, spawn a
//! worker to run the entry node with the body as initial input.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use crate::backend::WakeContext;
use crate::state::DispatcherState;

#[derive(Debug, Serialize)]
pub struct WebhookResponse {
    pub color: String,
}

pub async fn handle(
    State(state): State<DispatcherState>,
    Path((token, _path)): Path<(String, String)>,
    body: Option<Json<Value>>,
) -> Result<(StatusCode, Json<WebhookResponse>), (StatusCode, String)> {
    let entry = state
        .journal
        .resolve_entry_token(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown token".into()))?;

    let summary = state
        .projects
        .get(
            entry
                .project_id
                .parse::<uuid::Uuid>()
                .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad project id".into()))?,
        )
        .await
        .ok_or((StatusCode::GONE, "project no longer registered".into()))?;

    let payload = body.map(|Json(v)| v).unwrap_or(Value::Null);
    let color = uuid::Uuid::new_v4();
    let wake = WakeContext {
        project_id: entry.project_id.clone(),
        color,
        resume_node: entry.node_id.clone(),
        resume_value: payload,
    };

    state
        .workers
        .spawn_worker(&summary.binary_path, wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;

    Ok((StatusCode::ACCEPTED, Json(WebhookResponse { color: color.to_string() })))
}
