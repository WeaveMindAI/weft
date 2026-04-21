//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh execution.
//! `weft run` on the CLI calls these.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::primitive::EntryPrimitive;
use weft_core::ProjectDefinition;

use crate::backend::WakeContext;
use crate::journal::EntryKind;
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

#[derive(Debug, Serialize)]
pub struct ActivateResponse {
    pub urls: Vec<ActivationUrl>,
}

#[derive(Debug, Serialize)]
pub struct ActivationUrl {
    pub node_id: String,
    pub kind: String,
    pub url: String,
}

/// Activate a project. For each node that declares an entry
/// primitive, mint an entry token and return the user-facing URL.
/// Webhook tokens under `/w/{token}/{path}`, cron tokens are
/// registered for scheduled firing (future), manual ones just get
/// surfaced for completeness.
pub async fn activate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let project = state
        .projects
        .project(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;

    // Drop stale tokens before minting fresh ones. This also wipes
    // the URLs from a previous activation so they stop resolving.
    let project_id = id.to_string();
    state
        .journal
        .drop_entry_tokens(&project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drop tokens: {e}")))?;

    let base = format!("http://localhost:{}", state.config.http_port);
    let mut urls = Vec::new();

    for node in &project.nodes {
        for entry in &node.entry {
            let (kind, path, auth, url) = match entry {
                EntryPrimitive::Webhook { path, auth } => {
                    let auth_json = serde_json::to_value(auth).ok();
                    let token = state
                        .journal
                        .mint_entry_token(
                            &project_id,
                            &node.id,
                            EntryKind::Webhook,
                            Some(path.as_str()),
                            auth_json.clone(),
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mint: {e}")))?;
                    let url = if path.is_empty() {
                        format!("{base}/w/{token}")
                    } else {
                        format!("{base}/w/{token}/{path}")
                    };
                    ("webhook", Some(path.clone()), auth_json, url)
                }
                EntryPrimitive::Cron { schedule } => {
                    let token = state
                        .journal
                        .mint_entry_token(
                            &project_id,
                            &node.id,
                            EntryKind::Cron,
                            Some(schedule.as_str()),
                            None,
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mint: {e}")))?;
                    let url = format!("cron:{schedule} (token {token})");
                    ("cron", Some(schedule.clone()), None, url)
                }
                EntryPrimitive::Manual => {
                    let token = state
                        .journal
                        .mint_entry_token(&project_id, &node.id, EntryKind::Manual, None, None)
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mint: {e}")))?;
                    let url = format!("manual (token {token})");
                    ("manual", None, None, url)
                }
                EntryPrimitive::Event { .. } => {
                    // Infra-backed event. Handled by the infra
                    // subscription path, not an entry token.
                    continue;
                }
            };

            let _ = (path, auth);
            urls.push(ActivationUrl {
                node_id: node.id.clone(),
                kind: kind.to_string(),
                url,
            });
        }
    }

    state.projects.set_status(id, ProjectStatus::Active).await;
    Ok(Json(ActivateResponse { urls }))
}

pub async fn deactivate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let project_id = id.to_string();
    state
        .journal
        .drop_entry_tokens(&project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drop tokens: {e}")))?;
    if !state.projects.set_status(id, ProjectStatus::Inactive).await {
        return Err((StatusCode::NOT_FOUND, "project not found".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}
