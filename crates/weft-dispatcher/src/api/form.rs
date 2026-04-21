//! Form submission entry. `POST /f/{token}`: look up the suspension
//! token, resume the execution at the suspended node with the form
//! value.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use crate::backend::WakeContext;
use crate::state::DispatcherState;

#[derive(Debug, Serialize)]
pub struct FormSubmitResponse {
    pub color: String,
}

pub async fn submit(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<FormSubmitResponse>, (StatusCode, String)> {
    let target = state
        .journal
        .resolve_wake(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown token".into()))?;

    // Resolve which project owns this color. The journal's
    // suspension row doesn't carry project_id directly; we look up
    // via the execution table. For phase A2 we approximate by
    // assuming the project id lives in the suspension metadata
    // under "project_id". This keeps the schema compatible while
    // we wire the full resume loop.
    let project_id_str = target
        .metadata
        .get("project_id")
        .and_then(|v| v.as_str())
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "suspension missing project_id".into()))?;
    let project_id = project_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad project id".into()))?;
    let summary = state
        .projects
        .get(project_id)
        .await
        .ok_or((StatusCode::GONE, "project no longer registered".into()))?;

    // Consume the token before respawning: otherwise the same
    // submission could fire twice if the client retries.
    state
        .journal
        .consume_suspension(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("consume: {e}")))?;

    let wake = WakeContext {
        project_id: project_id_str.to_string(),
        color: target.color,
        resume_node: target.node.clone(),
        resume_value: body,
        kind: crate::backend::WakeKind::Resume,
    };

    state
        .workers
        .spawn_worker(&summary.binary_path, wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;

    Ok(Json(FormSubmitResponse { color: target.color.to_string() }))
}
