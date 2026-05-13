//! Infra lifecycle endpoints.
//!
//!   - `POST /projects/{id}/infra/start`: bring infra up. Spawns
//!     an `InfraSetup`-phase worker; the infra node's `execute()`
//!     calls `ctx.provision_sidecar(spec)`, which enqueues a
//!     `provision_sidecar` task. Stopped sidecars are scaled back
//!     to Running first so the task's idempotent fast path returns
//!     the existing handle.
//!   - `POST /projects/{id}/infra/stop`: scale running
//!     Deployments to 0. Keeps PVC / Service / Ingress so
//!     `start` can resume the same instance with its auth state.
//!   - `POST /projects/{id}/infra/terminate`: delete every k8s
//!     resource the sidecar owns. PVC goes too. Idempotent.
//!   - `GET /projects/{id}/infra/status`: list each infra node
//!     with its current lifecycle status + endpoint URL.
//!   - `GET /projects/{id}/infra/nodes/{node_id}/live`: proxies
//!     the sidecar's `/live` JSON for the extension poller.

use std::collections::BTreeMap;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::infra::InfraStatus;
use crate::state::DispatcherState;

/// Body for `POST /projects/{id}/infra/start` and
/// `POST /projects/{id}/infra/upgrade`.
#[derive(Debug, Default, Deserialize)]
pub struct StartRequest {
    /// Source hash for this start. Stored on the project row so
    /// the InfraSetup-phase worker (and subsequent fire workers)
    /// pull the right image. Optional in tests; production paths
    /// always set it.
    #[serde(default, rename = "sourceHash", alias = "source_hash")]
    pub source_hash: Option<String>,
    /// Infra hash for this start. Stored on the project row so
    /// drift detection compares like-with-like.
    #[serde(default, rename = "infraHash", alias = "infra_hash")]
    pub infra_hash: Option<String>,
    /// Per-node sidecar source-hash. Keyed by infra node id.
    /// Pre-written into infra_pod.running_image_hash so the infra
    /// backend uses the right image tag at provision time. NOT a
    /// drift signal anymore; drift is the project-level infra_hash.
    #[serde(default, rename = "sidecarHashes", alias = "sidecar_hashes")]
    pub sidecar_hashes: BTreeMap<String, String>,
}

/// Body for `POST /projects/{id}/infra/stop`,
/// `POST /projects/{id}/infra/terminate`. Mirrors the explicit
/// consent the CLI prompts the user for before destructive infra
/// actions: when triggers are active, the dispatcher refuses
/// without `deactivate_triggers: true`.
#[derive(Debug, Default, Deserialize)]
pub struct StopRequest {
    #[serde(default, rename = "deactivateTriggers", alias = "deactivate_triggers")]
    pub deactivate_triggers: bool,
}

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
    body: Option<Json<StartRequest>>,
) -> Result<Json<InfraResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let project = state
        .projects
        .project(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;
    let body = body.map(|Json(b)| b).unwrap_or_default();

    // Refuse start if triggers are currently active. The user must
    // explicitly deactivate or resync first; the dispatcher will
    // never silently drop active triggers on a start (unlike
    // stop/terminate/upgrade, where the user can opt in via
    // deactivate_triggers=true).
    let summary = state.projects.get(id).await;
    let status = summary.as_ref().map(|s| s.status);
    if matches!(status, Some(crate::project_store::ProjectStatus::Active)) {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "project has active triggers; run `weft deactivate` or `weft resync` \
             first, then start infra"
                .into(),
        ));
    }
    if matches!(status, Some(crate::project_store::ProjectStatus::Activating)) {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "activate is in flight; cancel it (`weft cancel-activate`) before \
             starting infra"
                .into(),
        ));
    }

    // Persist the source / infra hashes early so the InfraSetup
    // worker (and drift-detection that runs after) sees fresh state.
    if let Some(hash) = body.source_hash.as_deref() {
        state.projects.set_running_source_hash(id, hash).await;
    }
    if let Some(hash) = body.infra_hash.as_deref() {
        state.projects.set_running_infra_hash(id, hash).await;
    }

    // Pre-write per-node sidecar image hashes so the
    // provision_sidecar task uses them as the docker tag suffix.
    // Done BEFORE the InfraSetup sub-exec runs.
    let namespace = state.namespace_mapper.namespace_for(
        &state.tenant_router.tenant_for_project(&project_id),
    );
    for node in project.nodes.iter().filter(|n| n.requires_infra) {
        if let Some(hash) = body.sidecar_hashes.get(&node.id) {
            crate::infra::set_pending_image_hash(
                &state.pg_pool,
                &project_id,
                &node.id,
                &namespace,
                hash,
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("set_pending_image_hash {}: {e}", node.id),
                )
            })?;
        }
    }

    // Track every node we touch in this start call so we can roll
    // back atomically if anything later in the sequence fails.
    // Nodes with prior_status == None were freshly provisioned (need
    // full delete on rollback). Nodes with Some(Stopped) were scaled
    // up (need to be scaled back to 0 on rollback).
    let mut touched: Vec<TouchedNode> = Vec::new();

    // Pre-flight: per-node status check. Any node already Running
    // is an error; Stopped nodes get scaled back up so the
    // InfraSetup worker's `provision_sidecar` task short-circuits
    // to the existing handle instead of trying to re-apply.
    //
    // A row with status=stopped + empty instance_id is a
    // "pending hash placeholder" written by set_pending_image_hash;
    // it isn't a real provisioned-and-stopped sidecar. Treat it as
    // a never-provisioned node and queue it for InfraSetup.
    let mut to_run: Vec<String> = Vec::new();
    for node in &project.nodes {
        if !node.requires_infra {
            continue;
        }
        let entry = crate::infra::get(&state.pg_pool, &project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
        match entry {
            Some(e) if e.status == InfraStatus::Running => {
                return Err((
                    StatusCode::CONFLICT,
                    format!(
                        "infra for '{}' is already running. Stop or terminate it first.",
                        node.id
                    ),
                ));
            }
            Some(e) if e.handle.id.is_empty() => {
                // Placeholder row: pending hash was set, no real
                // sidecar was ever provisioned. Treat as unprovisioned.
                to_run.push(node.id.clone());
                touched.push(TouchedNode {
                    node_id: node.id.clone(),
                    prior_status: None,
                });
            }
            Some(e) => {
                if let Err(err) = state.infra.scale_up(&e.handle).await {
                    rollback_touched(&state, &project_id, &touched).await;
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("scale_up {} failed: {err}", node.id),
                    ));
                }
                if let Err(err) = state.infra.wait_ready(&e.handle).await {
                    // We scaled it up, so record it as touched
                    // BEFORE rolling back so it gets reverted.
                    touched.push(TouchedNode {
                        node_id: node.id.clone(),
                        prior_status: Some(InfraStatus::Stopped),
                    });
                    rollback_touched(&state, &project_id, &touched).await;
                    return Err((
                        StatusCode::GATEWAY_TIMEOUT,
                        format!("sidecar '{}' never became ready: {err}", node.id),
                    ));
                }
                if let Err(err) = crate::infra::set_status(
                    &state.pg_pool,
                    &project_id,
                    &node.id,
                    InfraStatus::Running,
                )
                .await
                {
                    touched.push(TouchedNode {
                        node_id: node.id.clone(),
                        prior_status: Some(InfraStatus::Stopped),
                    });
                    rollback_touched(&state, &project_id, &touched).await;
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {err}")));
                }
                touched.push(TouchedNode {
                    node_id: node.id.clone(),
                    prior_status: Some(InfraStatus::Stopped),
                });
            }
            None => {
                to_run.push(node.id.clone());
                touched.push(TouchedNode {
                    node_id: node.id.clone(),
                    prior_status: None,
                });
            }
        }
    }

    // Newly-provisioning nodes go through the InfraSetup sub-exec.
    // If it fails mid-way some provision_sidecar tasks may have
    // already inserted infra_pod rows + applied k8s resources;
    // rollback_touched cleans those up so the user sees an
    // all-or-nothing start.
    if !to_run.is_empty() {
        if let Err(err) = crate::api::project::run_infra_setup(&state, id, to_run).await {
            rollback_touched(&state, &project_id, &touched).await;
            return Err(err);
        }
    }

    // Snapshot and return.
    let mut nodes = Vec::new();
    for node in &project.nodes {
        if !node.requires_infra {
            continue;
        }
        let entry = crate::infra::get(&state.pg_pool, &project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
        if let Some(e) = entry {
            nodes.push(InfraStatusEntry {
                node_id: node.id.clone(),
                status: e.status,
                endpoint_url: e.handle.endpoint_url.clone(),
            });
        }
    }
    Ok(Json(InfraResponse { nodes }))
}

struct TouchedNode {
    node_id: String,
    /// What this node looked like before the current start call.
    /// `None` = freshly provisioned (rollback = delete k8s + row).
    /// `Some(Stopped)` = scaled up from 0 (rollback = scale to 0).
    prior_status: Option<InfraStatus>,
}

/// Best-effort rollback of nodes touched during a failed start.
/// Each node is reverted to its prior_status. Errors are logged but
/// don't abort the sweep: a partially-successful rollback is still
/// strictly better than leaving everything in an inconsistent state.
async fn rollback_touched(
    state: &DispatcherState,
    project_id: &str,
    touched: &[TouchedNode],
) {
    for node in touched {
        match node.prior_status {
            None => {
                // Freshly provisioned. Look up the row (may or may
                // not exist depending on how far provision_sidecar
                // got), delete the k8s resources, drop the row.
                let entry = match crate::infra::get(&state.pg_pool, project_id, &node.node_id).await {
                    Ok(Some(e)) => e,
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!(
                            target: "weft_dispatcher::infra",
                            project_id,
                            node_id = %node.node_id,
                            error = %e,
                            "rollback_touched: failed to read infra_pod row"
                        );
                        continue;
                    }
                };
                // Placeholder row (set_pending_image_hash wrote it
                // but no k8s resources were ever applied). Just
                // drop the row; nothing in the cluster to delete.
                if entry.handle.id.is_empty() {
                    if let Err(err) = crate::infra::remove(&state.pg_pool, project_id, &node.node_id).await {
                        tracing::warn!(
                            target: "weft_dispatcher::infra",
                            project_id,
                            node_id = %node.node_id,
                            error = %err,
                            "rollback_touched: drop placeholder row failed"
                        );
                    }
                    continue;
                }
                if let Err(err) = state.infra.delete(entry.handle.clone()).await {
                    tracing::warn!(
                        target: "weft_dispatcher::infra",
                        project_id,
                        node_id = %node.node_id,
                        error = %err,
                        "rollback_touched: delete k8s resources failed"
                    );
                }
                if let Err(err) = crate::infra::remove(&state.pg_pool, project_id, &node.node_id).await {
                    tracing::warn!(
                        target: "weft_dispatcher::infra",
                        project_id,
                        node_id = %node.node_id,
                        error = %err,
                        "rollback_touched: drop infra_pod row failed"
                    );
                }
            }
            Some(InfraStatus::Stopped) => {
                // Scaled up from 0; scale back to 0 and mark Stopped.
                let entry = match crate::infra::get(&state.pg_pool, project_id, &node.node_id).await {
                    Ok(Some(e)) => e,
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!(
                            target: "weft_dispatcher::infra",
                            project_id,
                            node_id = %node.node_id,
                            error = %e,
                            "rollback_touched: failed to read infra_pod row"
                        );
                        continue;
                    }
                };
                if let Err(err) = state.infra.scale_to_zero(&entry.handle).await {
                    tracing::warn!(
                        target: "weft_dispatcher::infra",
                        project_id,
                        node_id = %node.node_id,
                        error = %err,
                        "rollback_touched: scale_to_zero failed"
                    );
                }
                if let Err(err) = crate::infra::set_status(
                    &state.pg_pool,
                    project_id,
                    &node.node_id,
                    InfraStatus::Stopped,
                )
                .await
                {
                    tracing::warn!(
                        target: "weft_dispatcher::infra",
                        project_id,
                        node_id = %node.node_id,
                        error = %err,
                        "rollback_touched: set_status failed"
                    );
                }
            }
            Some(InfraStatus::Running) => {
                // Shouldn't happen: we refuse start for already-Running
                // nodes before touching anything. Defensive log.
                tracing::warn!(
                    target: "weft_dispatcher::infra",
                    project_id,
                    node_id = %node.node_id,
                    "rollback_touched: unexpected prior_status=Running"
                );
            }
        }
    }
}

pub async fn stop(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    body: Option<Json<StopRequest>>,
) -> Result<Json<InfraResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let entries = crate::infra::list_for_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
    if entries.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            "no provisioned infra for this project; nothing to stop".into(),
        ));
    }

    // Stop will deactivate any active triggers (their sidecars are
    // about to go down). Require explicit consent so a CLI/curl
    // user can never lose triggers without acknowledging it.
    let summary = state.projects.get(id).await;
    let status = summary.as_ref().map(|s| s.status);
    if matches!(status, Some(crate::project_store::ProjectStatus::Activating)) {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "activate is in flight; cancel it (`weft cancel-activate`) before \
             stopping infra"
                .into(),
        ));
    }
    let is_active = matches!(status, Some(crate::project_store::ProjectStatus::Active));
    if is_active && !body.deactivate_triggers {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "project has active triggers; pass {\"deactivate_triggers\": true} \
             in the body to confirm. The CLI prompts the user before sending this."
                .into(),
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
        crate::infra::set_status(
            &state.pg_pool,
            &project_id,
            &node_id,
            InfraStatus::Stopped,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
        out.push(InfraStatusEntry {
            node_id,
            status: InfraStatus::Stopped,
            endpoint_url: entry.handle.endpoint_url.clone(),
        });
    }
    if any_running {
        crate::api::project::deactivate_project(&state, id).await?;
    }
    Ok(Json(InfraResponse { nodes: out }))
}

pub async fn terminate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    body: Option<Json<StopRequest>>,
) -> Result<Json<InfraResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let entries = crate::infra::list_for_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
    if entries.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            "no provisioned infra for this project; nothing to terminate".into(),
        ));
    }

    // Same explicit-consent rule as stop.
    let summary = state.projects.get(id).await;
    let status = summary.as_ref().map(|s| s.status);
    if matches!(status, Some(crate::project_store::ProjectStatus::Activating)) {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "activate is in flight; cancel it (`weft cancel-activate`) before \
             terminating infra"
                .into(),
        ));
    }
    let is_active = matches!(status, Some(crate::project_store::ProjectStatus::Active));
    if is_active && !body.deactivate_triggers {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "project has active triggers; pass {\"deactivate_triggers\": true} \
             in the body to confirm. terminate is destructive (PVCs go too)."
                .into(),
        ));
    }

    crate::api::project::deactivate_project(&state, id).await?;
    let mut out = Vec::new();
    for (node_id, entry) in entries {
        state.infra.delete(entry.handle.clone()).await.map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("delete {node_id} failed: {err}"),
            )
        })?;
        crate::infra::remove(&state.pg_pool, &project_id, &node_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
        out.push(InfraStatusEntry {
            node_id,
            status: InfraStatus::Stopped,
            endpoint_url: None,
        });
    }
    Ok(Json(InfraResponse { nodes: out }))
}

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
        let entry = crate::infra::get(&state.pg_pool, &project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
        if let Some(e) = entry {
            nodes.push(InfraStatusEntry {
                node_id: node.id.clone(),
                status: e.status,
                endpoint_url: e.handle.endpoint_url.clone(),
            });
        }
    }
    Ok(Json(InfraResponse { nodes }))
}

pub async fn live(
    State(state): State<DispatcherState>,
    Path((id_str, node_id)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let entry = crate::infra::get(&state.pg_pool, &project_id, &node_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?
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

/// `POST /projects/{id}/infra/upgrade`. Atomic stop + sidecar
/// image swap + start. Same body shape as start (sourceHash +
/// infraHash + per-node sidecarHashes). Triggers are deactivated as
/// part of the stop step (gated by `deactivate_triggers: true` like
/// stop / terminate); the user re-clicks Activate after upgrade to
/// bring triggers back up.
pub async fn upgrade(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    body: Option<Json<UpgradeRequest>>,
) -> Result<Json<InfraResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let body = body.map(|Json(b)| b).unwrap_or_default();

    // Same explicit-consent rule as stop / terminate.
    let summary = state.projects.get(id).await;
    let status = summary.as_ref().map(|s| s.status);
    if matches!(status, Some(crate::project_store::ProjectStatus::Activating)) {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "activate is in flight; cancel it (`weft cancel-activate`) before \
             upgrading infra"
                .into(),
        ));
    }
    let is_active = matches!(status, Some(crate::project_store::ProjectStatus::Active));
    if is_active && !body.deactivate_triggers {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "project has active triggers; pass {\"deactivate_triggers\": true} \
             in the body to confirm. Upgrade restarts infra Pods."
                .into(),
        ));
    }

    // Step 1: stop (scale infra Pods to zero, deactivate triggers).
    // Reuse the stop handler's body so behavior stays in lockstep.
    let stop_body = StopRequest {
        deactivate_triggers: body.deactivate_triggers,
    };
    let _ = stop(
        State(state.clone()),
        Path(id_str.clone()),
        Some(Json(stop_body)),
    )
    .await?;

    // Step 2: persist new hashes, then run start (which provisions
    // / scales-up + InfraSetup the sub-exec). The pre-write of
    // sidecar_hashes happens inside start() already; we reuse it.
    let start_body = StartRequest {
        source_hash: body.source_hash,
        infra_hash: body.infra_hash,
        sidecar_hashes: body.sidecar_hashes,
    };
    start(State(state), Path(id_str), Some(Json(start_body))).await
}

/// Body for `POST /projects/{id}/infra/upgrade`. Combines start's
/// hash inputs with stop's `deactivate_triggers` consent flag.
#[derive(Debug, Default, Deserialize)]
pub struct UpgradeRequest {
    #[serde(default, rename = "sourceHash", alias = "source_hash")]
    pub source_hash: Option<String>,
    #[serde(default, rename = "infraHash", alias = "infra_hash")]
    pub infra_hash: Option<String>,
    #[serde(default, rename = "sidecarHashes", alias = "sidecar_hashes")]
    pub sidecar_hashes: BTreeMap<String, String>,
    #[serde(default, rename = "deactivateTriggers", alias = "deactivate_triggers")]
    pub deactivate_triggers: bool,
}

fn parse_id(raw: &str) -> Result<uuid::Uuid, (StatusCode, String)> {
    raw.parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))
}
