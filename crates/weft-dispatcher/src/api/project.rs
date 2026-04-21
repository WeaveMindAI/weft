//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh execution.
//! `weft run` on the CLI calls these.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::ProjectDefinition;

use crate::backend::WakeContext;
use crate::project_store::ProjectStatus;
use crate::state::DispatcherState;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    pub status: String,
}

pub async fn list(State(state): State<DispatcherState>) -> Json<Vec<ProjectSummary>> {
    let items = state.projects.list().await;
    Json(items.into_iter().map(|p| ProjectSummary {
        id: p.id.to_string(),
        name: p.name,
        status: p.status.as_str().to_string(),
    }).collect())
}

/// Register a project with the dispatcher. Body is a compiled
/// ProjectDefinition. The dispatcher writes it to disk and tracks it
/// in its project store.
pub async fn register(
    State(state): State<DispatcherState>,
    Json(project): Json<ProjectDefinition>,
) -> Result<Json<ProjectSummary>, (StatusCode, String)> {
    let summary = state
        .projects
        .register(project)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("register: {e}")))?;
    Ok(Json(ProjectSummary {
        id: summary.id.to_string(),
        name: summary.name,
        status: summary.status.as_str().to_string(),
    }))
}

pub async fn get(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
) -> Result<Json<ProjectSummary>, StatusCode> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    let summary = state.projects.get(id).await.ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(ProjectSummary {
        id: summary.id.to_string(),
        name: summary.name,
        status: summary.status.as_str().to_string(),
    }))
}

pub async fn remove(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    if state.projects.remove(id).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[derive(Debug, Deserialize)]
pub struct RunRequest {
    /// Optional override: which node to start from. Defaults to the
    /// first entry-primitive-bearing node in the project. If none,
    /// falls back to the first top-level node.
    #[serde(default)]
    pub entry_node: Option<String>,
    /// Initial payload for the entry node's first pulse.
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Serialize)]
pub struct RunResponse {
    pub color: String,
}

/// Start a fresh execution for a registered project.
pub async fn run(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
    Json(body): Json<RunRequest>,
) -> Result<Json<RunResponse>, (StatusCode, String)> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let summary = state
        .projects
        .get(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;

    // Resolve the entry node. Simple strategy for phase A2: explicit
    // override -> first node in project. Full entry-primitive-based
    // resolution lands with the webhook routing.
    let entry_nodes = state.projects.entry_nodes(id).await;
    let resume_node = match &body.entry_node {
        Some(n) => n.clone(),
        None => entry_nodes
            .first()
            .map(|n| n.id.clone())
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "project has no nodes".into()))?,
    };

    let color = uuid::Uuid::new_v4();
    let wake = WakeContext {
        project_id: id.to_string(),
        color,
        resume_node,
        resume_value: body.payload,
    };

    state
        .workers
        .spawn_worker(&summary.binary_path, wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;

    Ok(Json(RunResponse { color: color.to_string() }))
}

pub async fn activate(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    if state.projects.set_status(id, ProjectStatus::Active).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn deactivate(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    if state.projects.set_status(id, ProjectStatus::Inactive).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
