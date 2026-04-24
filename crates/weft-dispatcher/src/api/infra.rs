//! Infra lifecycle endpoints. Three verbs, matching v1:
//!
//!   - `POST /projects/{id}/infra/start` — bring infra up. If it's
//!     never been provisioned, apply manifests. If it's stopped,
//!     scale the Deployment back to 1. If it's already running,
//!     return 409 so the user doesn't accidentally double-apply
//!     and end up with two Deployments.
//!   - `POST /projects/{id}/infra/stop` — scale running Deployments
//!     to 0. Keeps PVC / Service / Ingress so `start` can resume
//!     the same instance with its auth state.
//!   - `POST /projects/{id}/infra/terminate` — delete every k8s
//!     resource the sidecar owns. PVC goes too: next `start` is
//!     fresh (e.g. WhatsApp re-pairing required). Idempotent.
//!
//! Plus:
//!   - `GET /projects/{id}/infra/status` — list each infra node with
//!     its current lifecycle status + endpoint URL.
//!   - `GET /projects/{id}/infra/nodes/{node_id}/live` — unchanged,
//!     proxies the sidecar's `/live` JSON for the extension poller.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use serde_json::Value;

use crate::backend::InfraSpec;
use crate::infra::InfraStatus;
use crate::state::DispatcherState;

// ----- start --------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct InfraStatusEntry {
    pub node_id: String,
    pub status: InfraStatus,
    pub endpoint_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InfraResponse {
    pub nodes: Vec<InfraStatusEntry>,
}

pub async fn start(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<InfraResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let project = state
        .projects
        .project(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;

    let mut nodes = Vec::new();
    for node in &project.nodes {
        if !node.requires_infra {
            continue;
        }
        let entry_now = state.infra_registry.get(&project_id, &node.id);
        match entry_now {
            Some(e) if e.status == InfraStatus::Running => {
                return Err((
                    StatusCode::CONFLICT,
                    format!(
                        "infra for '{}' is already running. Stop or terminate it first.",
                        node.id
                    ),
                ));
            }
            Some(e) => {
                // Stopped → scale back up and wait until the
                // Deployment's Pod passes readiness, so a
                // follow-up `activate` won't race the sidecar.
                state
                    .infra
                    .scale_up(&e.handle)
                    .await
                    .map_err(|err| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("scale_up {} failed: {err}", node.id),
                        )
                    })?;
                state.infra.wait_ready(&e.handle).await.map_err(|err| {
                    (
                        StatusCode::GATEWAY_TIMEOUT,
                        format!("sidecar '{}' never became ready: {err}", node.id),
                    )
                })?;
                state
                    .infra_registry
                    .set_status(&project_id, &node.id, InfraStatus::Running);
                nodes.push(InfraStatusEntry {
                    node_id: node.id.clone(),
                    status: InfraStatus::Running,
                    endpoint_url: e.handle.endpoint_url.clone(),
                });
            }
            None => {
                // First bring-up: apply manifests.
                let sidecar = node.sidecar.clone().ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!(
                            "node '{}' requires_infra but has no sidecar spec in metadata",
                            node.id
                        ),
                    )
                })?;
                let spec = InfraSpec {
                    project_id: project_id.clone(),
                    infra_node_id: node.id.clone(),
                    sidecar,
                    config: node.config.clone(),
                };
                let handle = state.infra.provision(spec).await.map_err(|err| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("provision {} failed: {err}", node.id),
                    )
                })?;
                // Don't return until the sidecar's /health is
                // answering. Otherwise a back-to-back `activate`
                // call races the k8s schedule + container startup
                // and the trigger-setup sub-exec fails with
                // "connection refused" on /outputs.
                state.infra.wait_ready(&handle).await.map_err(|err| {
                    (
                        StatusCode::GATEWAY_TIMEOUT,
                        format!("sidecar '{}' never became ready: {err}", node.id),
                    )
                })?;
                nodes.push(InfraStatusEntry {
                    node_id: node.id.clone(),
                    status: InfraStatus::Running,
                    endpoint_url: handle.endpoint_url.clone(),
                });
                state
                    .infra_registry
                    .insert_running(project_id.clone(), node.id.clone(), handle);
            }
        }
    }
    Ok(Json(InfraResponse { nodes }))
}

// ----- stop ---------------------------------------------------------

pub async fn stop(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<InfraResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let entries = state.infra_registry.list_for_project(&project_id);
    if entries.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            "no provisioned infra for this project; nothing to stop".into(),
        ));
    }
    let mut any_running = false;
    let mut out = Vec::new();
    for (node_id, entry) in entries {
        if entry.status == InfraStatus::Stopped {
            out.push(InfraStatusEntry {
                node_id,
                status: InfraStatus::Stopped,
                endpoint_url: entry.handle.endpoint_url.clone(),
            });
            continue;
        }
        any_running = true;
        state.infra.scale_to_zero(&entry.handle).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("scale_to_zero {node_id} failed: {e}"),
            )
        })?;
        state
            .infra_registry
            .set_status(&project_id, &node_id, InfraStatus::Stopped);
        out.push(InfraStatusEntry {
            node_id,
            status: InfraStatus::Stopped,
            endpoint_url: entry.handle.endpoint_url,
        });
    }
    if !any_running {
        return Err((
            StatusCode::CONFLICT,
            "infra is already stopped. Start it first or terminate it.".into(),
        ));
    }
    // Stopping infra leaves any previously-activated triggers
    // pointing at a dead sidecar endpoint, so force-deactivate
    // first. No-op if the project wasn't active.
    let _ = crate::api::project::deactivate_project(&state, id).await;
    Ok(Json(InfraResponse { nodes: out }))
}

// ----- terminate ----------------------------------------------------

pub async fn terminate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    // Terminate implies deactivate: sidecar endpoints disappear,
    // so any active listener subscribed to them is about to fail.
    // Force-deactivate first so we tear the listener down cleanly
    // rather than leave it spewing errors.
    let _ = crate::api::project::deactivate_project(&state, id).await;
    // Terminate is idempotent and nondestructive of the record: we
    // clear the registry entirely (so start can re-provision fresh).
    for (_node_id, entry) in state.infra_registry.remove_project(&project_id) {
        let _ = state.infra.delete(entry.handle).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

// ----- status -------------------------------------------------------

pub async fn status(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<InfraResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let nodes = state
        .infra_registry
        .list_for_project(&project_id)
        .into_iter()
        .map(|(node_id, entry)| InfraStatusEntry {
            node_id,
            status: entry.status,
            endpoint_url: entry.handle.endpoint_url,
        })
        .collect();
    Ok(Json(InfraResponse { nodes }))
}

// ----- live proxy (unchanged) ---------------------------------------

pub async fn live(
    State(state): State<DispatcherState>,
    Path((id_str, node_id)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let handle = state
        .infra_registry
        .handle_if_running(&project_id, &node_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("no running infra for node '{node_id}' (start it first)"),
            )
        })?;
    let endpoint = handle.endpoint_url.ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "infra handle has no endpoint URL yet".into(),
        )
    })?;

    let base = endpoint.trim_end_matches('/').trim_end_matches("/action");
    let live_url = format!("{base}/live");
    let resp = reqwest::Client::new()
        .get(&live_url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("GET {live_url}: {e}")))?;
    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("sidecar returned {}", resp.status()),
        ));
    }
    let body: Value = resp.json().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("invalid JSON from sidecar: {e}"),
        )
    })?;
    Ok(Json(body))
}

fn parse_id(s: &str) -> Result<uuid::Uuid, (StatusCode, String)> {
    s.parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))
}
