//! Infra lifecycle endpoints. Three verbs, matching v1:
//!
//!   - `POST /projects/{id}/infra/start` — bring infra up. Spawns
//!     an `InfraSetup`-phase worker; the infra node's `execute()`
//!     calls `ctx.provision_sidecar(spec)`, which round-trips to
//!     the dispatcher's `InfraBackend` and returns the endpoint.
//!     The WS handler also scales Stopped sidecars back to
//!     Running before running the sub-exec so the node sees a
//!     live sidecar via `provision_sidecar`'s idempotent short-
//!     circuit path.
//!   - `POST /projects/{id}/infra/stop` — scale running
//!     Deployments to 0. Keeps PVC / Service / Ingress so
//!     `start` can resume the same instance with its auth state.
//!   - `POST /projects/{id}/infra/terminate` — delete every k8s
//!     resource the sidecar owns. PVC goes too: next `start` is
//!     fresh (e.g. WhatsApp re-pairing required). Idempotent.
//!
//! Plus:
//!   - `GET /projects/{id}/infra/status` — list each infra node
//!     with its current lifecycle status + endpoint URL.
//!   - `GET /projects/{id}/infra/nodes/{node_id}/live` —
//!     unchanged, proxies the sidecar's `/live` JSON for the
//!     extension poller.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use serde_json::Value;

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

    // Pre-flight: per-node status check. Any node already Running
    // is an error; Stopped nodes need to be scaled back up so the
    // InfraSetup worker's `provision_sidecar` call short-circuits
    // to the existing handle instead of trying to re-apply.
    let mut to_run: Vec<String> = Vec::new();
    for node in &project.nodes {
        if !node.requires_infra {
            continue;
        }
        match state.infra_registry.get(&project_id, &node.id) {
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
                state.infra.scale_up(&e.handle).await.map_err(|err| {
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
            }
            None => {
                to_run.push(node.id.clone());
            }
        }
    }

    // Nothing to newly provision? We already scaled up any Stopped
    // instances; just return the current status.
    if !to_run.is_empty() {
        crate::api::project::run_infra_setup(&state, id, to_run).await?;
    }

    // Snapshot and return.
    let mut nodes = Vec::new();
    for node in &project.nodes {
        if !node.requires_infra {
            continue;
        }
        if let Some(e) = state.infra_registry.get(&project_id, &node.id) {
            nodes.push(InfraStatusEntry {
                node_id: node.id.clone(),
                status: e.status,
                endpoint_url: e.handle.endpoint_url.clone(),
            });
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
        state.infra.scale_to_zero(&entry.handle).await.map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("scale_to_zero {node_id} failed: {err}"),
            )
        })?;
        state
            .infra_registry
            .set_status(&project_id, &node_id, InfraStatus::Stopped);
        out.push(InfraStatusEntry {
            node_id,
            status: InfraStatus::Stopped,
            endpoint_url: entry.handle.endpoint_url.clone(),
        });
    }
    if any_running {
        let _ = crate::api::project::deactivate_project(&state, id).await;
    }
    Ok(Json(InfraResponse { nodes: out }))
}

// ----- terminate ----------------------------------------------------

pub async fn terminate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<InfraResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let entries = state.infra_registry.list_for_project(&project_id);
    if entries.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            "no provisioned infra for this project; nothing to terminate".into(),
        ));
    }
    let _ = crate::api::project::deactivate_project(&state, id).await;
    let mut out = Vec::new();
    for (node_id, entry) in entries {
        state.infra.delete(entry.handle.clone()).await.map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("delete {node_id} failed: {err}"),
            )
        })?;
        state.infra_registry.remove(&project_id, &node_id);
        out.push(InfraStatusEntry {
            node_id,
            status: InfraStatus::Stopped,
            endpoint_url: None,
        });
    }
    Ok(Json(InfraResponse { nodes: out }))
}

// ----- status -------------------------------------------------------

pub async fn status(
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
        if let Some(e) = state.infra_registry.get(&project_id, &node.id) {
            nodes.push(InfraStatusEntry {
                node_id: node.id.clone(),
                status: e.status,
                endpoint_url: e.handle.endpoint_url.clone(),
            });
        }
    }
    Ok(Json(InfraResponse { nodes }))
}

// ----- live ---------------------------------------------------------

pub async fn live(
    State(state): State<DispatcherState>,
    Path((id_str, node_id)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let entry = state
        .infra_registry
        .get(&project_id, &node_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "no such infra node".into()))?;
    let endpoint = entry.handle.endpoint_url.as_ref().ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            "infra has no endpoint URL".to_string(),
        )
    })?;
    let live_url = endpoint
        .trim_end_matches("/action")
        .trim_end_matches('/')
        .to_string()
        + "/live";
    let client = reqwest::Client::new();
    let resp = client
        .get(&live_url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("live fetch: {e}")))?;
    let value: Value = resp
        .json()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("live parse: {e}")))?;
    Ok(Json(value))
}

fn parse_id(raw: &str) -> Result<uuid::Uuid, (StatusCode, String)> {
    raw.parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))
}
