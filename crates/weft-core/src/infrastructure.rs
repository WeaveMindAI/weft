use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use restate_sdk::prelude::*;

use crate::sidecar::{ActionRequest, ActionResponse};
use crate::k8s_provisioner;
use crate::executor_core::ProjectExecutionRequest;

fn executor_base_url() -> String {
    std::env::var("EXECUTOR_CALLBACK_URL")
        .unwrap_or_else(|_| "http://localhost:9081".to_string())
}

// =============================================================================
// HELPERS
// =============================================================================

/// Compute the infrastructure instance ID for a given project and node.
/// K8s label values are capped at 63 bytes, so we truncate the UUIDs.
/// Format: wf-{12hex}-{12hex} = 28 chars (well under 63, even with -data suffix).
/// Both parts are sanitized to lowercase alphanumeric only (RFC 1123).
pub fn infra_instance_id(project_id: &str, node_id: &str) -> String {
    let wf_short: String = project_id.chars().filter(|c| c.is_ascii_alphanumeric()).take(12).collect();
    let nd_short: String = node_id.chars().filter(|c| c.is_ascii_alphanumeric()).take(12).collect();
    format!("wf-{}-{}", wf_short.to_lowercase(), nd_short.to_lowercase())
}

// =============================================================================
// INFRA CLIENT,used by consumer nodes to call sidecar actions
//
// Constructed from an endpoint URL (passed through edges from the infra node).
// No Restate resolution, no registry lookup. Just HTTP.
// =============================================================================

#[derive(Debug, Clone)]
pub struct InfraClient {
    endpointUrl: String,
    client: reqwest::Client,
}

impl InfraClient {
    pub fn new(endpoint_url: &str) -> Self {
        Self {
            endpointUrl: endpoint_url.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn execute_action(
        &self,
        action: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let req = ActionRequest {
            action: action.to_string(),
            payload,
        };

        let max_retries = 3u32;
        let mut delay_ms = 500u64;
        let mut last_err = String::new();

        for attempt in 0..=max_retries {
            let result = self.client.post(&self.endpointUrl)
                .json(&req)
                .timeout(std::time::Duration::from_secs(30))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    let resp: ActionResponse = response.json().await
                        .map_err(|e| format!("Failed to parse {} response: {}", action, e))?;
                    return Ok(resp.result);
                }
                Ok(response) if response.status().as_u16() == 429 || response.status().as_u16() == 503 => {
                    last_err = format!("{} returned {} (retryable)", action, response.status());
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    return Err(format!("{} failed ({}): {}", action, status, body));
                }
                Err(e) => {
                    last_err = format!("Failed to call {}: {}", action, e);
                }
            }

            if attempt < max_retries {
                tracing::warn!("[InfraClient] {} attempt {}/{} failed: {}. Retrying in {}ms",
                    action, attempt + 1, max_retries, last_err, delay_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(4000);
            }
        }

        Err(last_err)
    }
}

// =============================================================================
// STATUS TYPES
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartInfraRequest {
    pub weftCode: String,
    #[serde(default)]
    pub userId: Option<String>,
    #[serde(default)]
    pub project: Option<crate::project::ProjectDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraSetupCallback {
    pub status: String,
    #[serde(default)]
    pub executionId: Option<String>,
    #[serde(default)]
    pub nodeOutputs: serde_json::Value,
    #[serde(default)]
    pub nodeStatuses: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraStatusResponse {
    pub projectId: String,
    /// Overall status: "running", "stopped", "starting", "failed", "terminated", "none"
    pub status: String,
    pub nodes: Vec<InfraNodeStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executionId: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraNodeStatus {
    pub nodeId: String,
    pub nodeType: String,
    pub instanceId: String,
    pub status: String,
}

/// Stored endpoint URLs for all infrastructure nodes in a project.
/// Maps node ID -> endpointUrl (the sidecar's action endpoint).
/// This is the only platform-managed value; all other outputs come from /outputs at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraEndpointUrls {
    pub urls: BTreeMap<String, String>,
}

// =============================================================================
// INFRASTRUCTURE MANAGER, Restate virtual object keyed by project_id
//
// Thin orchestrator:
//   start_all, extract infra subgraph, dispatch to ProjectExecutor
//   infra_setup_completed, extract endpointUrl from each infra node's output
//   stop_all, scale K8s deployments to 0 directly
//   terminate_all, delete K8s resources directly
//   get_status, return stored state
//   get_infra_endpoint_urls, return stored endpointUrl per infra node
// =============================================================================

#[restate_sdk::object]
pub trait InfrastructureManager {
    async fn start_all(req: Json<StartInfraRequest>) -> Result<Json<InfraStatusResponse>, HandlerError>;
    async fn infra_setup_completed(result: Json<InfraSetupCallback>) -> Result<(), HandlerError>;
    async fn stop_all() -> Result<Json<InfraStatusResponse>, HandlerError>;
    async fn do_stop() -> Result<(), HandlerError>;
    async fn terminate_all() -> Result<Json<InfraStatusResponse>, HandlerError>;
    async fn do_terminate() -> Result<(), HandlerError>;

    #[shared]
    async fn get_status() -> Result<Json<InfraStatusResponse>, HandlerError>;

    #[shared]
    async fn get_infra_endpoint_urls() -> Result<Json<InfraEndpointUrls>, HandlerError>;
}

pub struct InfrastructureManagerImpl;

impl InfrastructureManager for InfrastructureManagerImpl {
    async fn start_all(
        &self,
        ctx: ObjectContext<'_>,
        Json(req): Json<StartInfraRequest>,
    ) -> Result<Json<InfraStatusResponse>, HandlerError> {
        let project_id = ctx.key().to_string();

        let current_status = ctx.get::<String>("infra_status").await?.unwrap_or_default();
        tracing::info!("[START_ALL] project={} current_status='{}'", project_id, current_status);

        let pid = uuid::Uuid::parse_str(&project_id)
            .map_err(|_| TerminalError::new(format!("Invalid project UUID: {}", project_id)))?;
        let project = if let Some(mut pd) = req.project {
            pd.id = pid;
            pd
        } else {
            crate::weft_compiler::compile(&req.weftCode, pid)
                .map_err(|e| TerminalError::new(format!("Failed to compile weftCode: {:?}", e)))?
        };

        // Store weftCode for potential re-use
        ctx.set("weft_code", req.weftCode.clone());

        let sub_project = project.extract_infra_subgraph()
            .map_err(|e| TerminalError::new(e))?;

        let infra_nodes: Vec<_> = sub_project.nodes.iter()
            .filter(|n| n.features.isInfrastructure)
            .collect();

        if infra_nodes.is_empty() {
            return Ok(Json(InfraStatusResponse {
                projectId: project_id,
                status: "none".to_string(),
                nodes: vec![],
                executionId: None,
            }));
        }

        // Enforce infra limits before provisioning
        const MAX_GLOBAL_INFRA_DEPLOYMENTS: u64 = 10;
        const MAX_USER_INFRA_DEPLOYMENTS: u64 = 2;

        let global_count: u64 = ctx.run(|| async move {
            let client = kube::Client::try_default().await
                .map_err(|e| TerminalError::new(format!("K8s client: {}", e)))?;
            let count = k8s_provisioner::count_running_infra_deployments(&client).await
                .map_err(|e| TerminalError::new(e))?;
            Ok(count as u64)
        }).await?;

        let user_id_for_limit = req.userId.clone();
        let user_count: u64 = ctx.run(|| {
            let uid = user_id_for_limit.clone();
            async move {
                if let Some(ref uid) = uid {
                    let client = kube::Client::try_default().await
                        .map_err(|e| TerminalError::new(format!("K8s client: {}", e)))?;
                    let count = k8s_provisioner::count_running_infra_deployments_for_user(&client, uid).await
                        .map_err(|e| TerminalError::new(e))?;
                    Ok(count as u64)
                } else {
                    Ok(0u64)
                }
            }
        }).await?;

        tracing::info!(
            "[START_ALL] infra limits check: global={}/{}, user={}/{}",
            global_count, MAX_GLOBAL_INFRA_DEPLOYMENTS,
            user_count, MAX_USER_INFRA_DEPLOYMENTS
        );

        if global_count >= MAX_GLOBAL_INFRA_DEPLOYMENTS {
            return Ok(Json(InfraStatusResponse {
                projectId: project_id,
                status: "limit_reached".to_string(),
                nodes: vec![],
                executionId: None,
            }));
        }

        if user_count >= MAX_USER_INFRA_DEPLOYMENTS {
            return Ok(Json(InfraStatusResponse {
                projectId: project_id,
                status: "user_limit_reached".to_string(),
                nodes: vec![],
                executionId: None,
            }));
        }

        let mut node_statuses = Vec::new();
        for node in &infra_nodes {
            let instance_id = infra_instance_id(&project_id, &node.id);
            node_statuses.push(InfraNodeStatus {
                nodeId: node.id.clone(),
                nodeType: node.nodeType.to_string(),
                instanceId: instance_id,
                status: "starting".to_string(),
            });
        }

        let mapping_json = serde_json::to_string(&node_statuses)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("infra_nodes", mapping_json);
        ctx.set("infra_status", "starting".to_string());

        // Cancel any previous infra-setup executor
        if let Some(prev_exec_id) = ctx.get::<String>("infra_execution_id").await? {
            tracing::info!("Cancelling previous infra-setup executor: {}", prev_exec_id);
            let executor_url = executor_base_url();
            let url = format!("{}/ProjectExecutor/{}/cancel", executor_url, prev_exec_id);
            let _ = reqwest::Client::new().post(&url)
                .timeout(std::time::Duration::from_secs(30))
                .send().await;
        }

        let run_counter: u64 = ctx.get::<u64>("infra_run_counter").await?.unwrap_or(0) + 1;
        ctx.set("infra_run_counter", run_counter);

        let execution_id = format!("infra-setup-{}-{}", project_id, run_counter);
        ctx.set("infra_execution_id", execution_id.clone());

        let restate_ingress = std::env::var("RESTATE_CALLBACK_URL")
            .unwrap_or_else(|_| {
                let port = std::env::var("RESTATE_PORT").unwrap_or_else(|_| "8080".to_string());
                format!("http://localhost:{}", port)
            });
        let callback_url = format!(
            "{}/InfrastructureManager/{}/infra_setup_completed",
            restate_ingress, project_id
        );

        let namespace = req.userId.as_ref().map(|uid| format!("wm-{}", uid.to_lowercase()));

        if let Some(ref ns) = namespace {
            ctx.set("infra_namespace", ns.clone());
        }

        let start_req = ProjectExecutionRequest {
            project: sub_project,
            input: serde_json::json!({
                "projectId": project_id,
                "namespace": namespace,
            }),
            userId: req.userId.clone(),
            statusCallbackUrl: Some(callback_url),
            isInfraSetup: true,
            isTriggerSetup: false,
            weftCode: None,
            testMode: false,
            triggerId: None,
            nodeType: None,
            mocks: None,
        };
        let executor_url = executor_base_url();
        let url = format!("{}/ProjectExecutor/{}/start", executor_url, execution_id);
        // Server-to-server: orchestrator's auth in cloud mode requires the
        // internal API key. Bare reqwest::Client wouldn't send it.
        let mut req_builder = reqwest::Client::new().post(&url).json(&start_req);
        if let Ok(key) = std::env::var("INTERNAL_API_KEY") {
            if !key.is_empty() {
                req_builder = req_builder.header("x-internal-api-key", key);
            }
        }
        if let Err(e) = req_builder.timeout(std::time::Duration::from_secs(30)).send().await {
            tracing::error!("[InfrastructureManager] Failed to start infra sub-flow: {}", e);
        }

        tracing::info!(
            "Infrastructure sub-flow dispatched for project {} (execution: {})",
            project_id, execution_id
        );

        Ok(Json(InfraStatusResponse {
            projectId: project_id,
            status: "starting".to_string(),
            nodes: node_statuses,
            executionId: Some(execution_id),
        }))
    }

    async fn infra_setup_completed(
        &self,
        ctx: ObjectContext<'_>,
        Json(callback): Json<InfraSetupCallback>,
    ) -> Result<(), HandlerError> {
        let project_id = ctx.key().to_string();

        // Ignore stale callbacks: if infra_execution_id was cleared (by stop/terminate),
        // or if the callback's executionId doesn't match the current one, discard it.
        let current_exec_id = ctx.get::<String>("infra_execution_id").await?;
        match (&current_exec_id, &callback.executionId) {
            (None, _) => {
                tracing::info!(
                    "Ignoring infra_setup_completed for project {} (no active infra execution)",
                    project_id
                );
                return Ok(());
            }
            (Some(current), Some(cb)) if current != cb => {
                tracing::warn!(
                    "Ignoring stale infra_setup_completed for project {} (callback from {}, current is {})",
                    project_id, cb, current
                );
                return Ok(());
            }
            _ => {}
        }

        tracing::info!(
            "Infrastructure setup completed for project {}: status={}",
            project_id, callback.status
        );

        if callback.status != "completed" {
            tracing::error!("Infrastructure setup failed for project {}", project_id);
            ctx.set("infra_status", "failed".to_string());

            let mut node_statuses = load_infra_nodes(&ctx).await?;
            for ns in &mut node_statuses {
                ns.status = "failed".to_string();
            }
            let mapping_json = serde_json::to_string(&node_statuses).unwrap_or_default();
            ctx.set("infra_nodes", mapping_json);
            return Ok(());
        }

        // Extract endpointUrl from each infra node's output.
        // This is the only value we store; everything else comes from /outputs at runtime.
        let all_outputs = callback.nodeOutputs.as_object().cloned().unwrap_or_default();
        let mut endpoint_urls: BTreeMap<String, String> = BTreeMap::new();
        let mut node_statuses = load_infra_nodes(&ctx).await?;

        for ns in &mut node_statuses {
            if let Some(output) = all_outputs.get(&ns.nodeId) {
                if let Some(url) = output.get("endpointUrl").and_then(|v| v.as_str()) {
                    endpoint_urls.insert(ns.nodeId.clone(), url.to_string());
                } else {
                    tracing::warn!("Infra node {} completed but has no endpointUrl in output", ns.nodeId);
                }
            }
            ns.status = "running".to_string();
        }

        let urls_json = serde_json::to_string(&endpoint_urls)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("infra_endpoint_urls", urls_json);

        let mapping_json = serde_json::to_string(&node_statuses)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("infra_nodes", mapping_json);
        ctx.set("infra_status", "running".to_string());

        tracing::info!("Infrastructure is now running for project {}", project_id);
        Ok(())
    }

    async fn stop_all(
        &self,
        ctx: ObjectContext<'_>,
    ) -> Result<Json<InfraStatusResponse>, HandlerError> {
        let project_id = ctx.key().to_string();
        let current_status = ctx.get::<String>("infra_status").await?.unwrap_or_default();
        tracing::info!("[STOP_ALL] project={} current_status='{}'", project_id, current_status);

        // Clear execution ID so any stale callback from the cancelled
        // executor is ignored by infra_setup_completed.
        if let Some(prev_exec_id) = ctx.get::<String>("infra_execution_id").await? {
            ctx.clear("infra_execution_id");
            let executor_url = executor_base_url();
            let url = format!("{}/ProjectExecutor/{}/cancel", executor_url, prev_exec_id);
            let _ = reqwest::Client::new().post(&url)
                .timeout(std::time::Duration::from_secs(30))
                .send().await;
        }

        // Set transitional status and commit immediately. The actual K8s work
        // happens in do_stop, which we self-call. This way get_status returns
        // "stopping" right away instead of blocking until K8s ops finish.
        let mut node_statuses = load_infra_nodes(&ctx).await?;
        for ns in &mut node_statuses {
            ns.status = "stopping".to_string();
        }
        let mapping_json = serde_json::to_string(&node_statuses).unwrap_or_default();
        ctx.set("infra_nodes", mapping_json);
        ctx.set("infra_status", "stopping".to_string());

        // Self-call to do the actual K8s work in a separate handler invocation
        ctx.object_client::<InfrastructureManagerClient>(&project_id)
            .do_stop()
            .send();

        Ok(Json(InfraStatusResponse {
            projectId: project_id,
            status: "stopping".to_string(),
            nodes: node_statuses,
            executionId: None,
        }))
    }

    async fn do_stop(
        &self,
        ctx: ObjectContext<'_>,
    ) -> Result<(), HandlerError> {
        let project_id = ctx.key().to_string();
        let namespace = ctx.get::<String>("infra_namespace").await?.unwrap_or_default();
        let mut node_statuses = load_infra_nodes(&ctx).await?;

        for ns in &mut node_statuses {
            let iid = ns.instanceId.clone();
            let ns_k8s = namespace.clone();
            let scale_result = ctx.run(|| async move {
                let client = kube::Client::try_default().await
                    .map_err(|e| TerminalError::new(format!("K8s client: {}", e)))?;
                k8s_provisioner::scale_instance_deployments_to_zero(&client, &ns_k8s, &iid).await
                    .map_err(|e| TerminalError::new(e))?;
                Ok(())
            }).await;

            match scale_result {
                Ok(_) => {
                    ns.status = "stopped".to_string();
                    tracing::info!("[DO_STOP] instance {} scaled to 0", ns.instanceId);
                }
                Err(e) => {
                    ns.status = "failed".to_string();
                    tracing::error!("[DO_STOP] failed to scale {}: {:?}", ns.instanceId, e);
                }
            }
        }

        let all_stopped = node_statuses.iter().all(|ns| ns.status == "stopped");
        let final_status = if all_stopped { "stopped" } else { "failed" };

        let mapping_json = serde_json::to_string(&node_statuses).unwrap_or_default();
        ctx.set("infra_nodes", mapping_json);
        ctx.set("infra_status", final_status.to_string());

        tracing::info!("[DO_STOP] project={} final_status={}", project_id, final_status);
        Ok(())
    }

    async fn terminate_all(
        &self,
        ctx: ObjectContext<'_>,
    ) -> Result<Json<InfraStatusResponse>, HandlerError> {
        let project_id = ctx.key().to_string();
        let current_status = ctx.get::<String>("infra_status").await?.unwrap_or_default();
        tracing::info!("[TERMINATE_ALL] project={} current_status='{}'", project_id, current_status);

        // Clear execution ID so any stale callback is ignored.
        if let Some(prev_exec_id) = ctx.get::<String>("infra_execution_id").await? {
            ctx.clear("infra_execution_id");
            let executor_url = executor_base_url();
            let url = format!("{}/ProjectExecutor/{}/cancel", executor_url, prev_exec_id);
            let _ = reqwest::Client::new().post(&url)
                .timeout(std::time::Duration::from_secs(30))
                .send().await;
        }

        // Set transitional status and commit immediately.
        let mut node_statuses = load_infra_nodes(&ctx).await?;
        for ns in &mut node_statuses {
            ns.status = "terminating".to_string();
        }
        let mapping_json = serde_json::to_string(&node_statuses).unwrap_or_default();
        ctx.set("infra_nodes", mapping_json);
        ctx.set("infra_status", "terminating".to_string());

        // Self-call to do the actual K8s work
        ctx.object_client::<InfrastructureManagerClient>(&project_id)
            .do_terminate()
            .send();

        Ok(Json(InfraStatusResponse {
            projectId: project_id,
            status: "terminating".to_string(),
            nodes: node_statuses,
            executionId: None,
        }))
    }

    async fn do_terminate(
        &self,
        ctx: ObjectContext<'_>,
    ) -> Result<(), HandlerError> {
        let project_id = ctx.key().to_string();
        let namespace = ctx.get::<String>("infra_namespace").await?.unwrap_or_default();
        let mut node_statuses = load_infra_nodes(&ctx).await?;

        for ns in &mut node_statuses {
            let iid = ns.instanceId.clone();
            let ns_k8s = namespace.clone();
            let delete_result = ctx.run(|| async move {
                let client = kube::Client::try_default().await
                    .map_err(|e| TerminalError::new(format!("K8s client: {}", e)))?;
                k8s_provisioner::delete_instance_resources(&client, &ns_k8s, &iid).await
                    .map_err(|e| TerminalError::new(e))?;
                Ok(())
            }).await;

            match delete_result {
                Ok(_) => {
                    ns.status = "terminated".to_string();
                    tracing::info!("[DO_TERMINATE] instance {} K8s resources deleted", ns.instanceId);
                }
                Err(e) => {
                    ns.status = "failed".to_string();
                    tracing::error!("[DO_TERMINATE] failed to delete {}: {:?}", ns.instanceId, e);
                }
            }
        }

        // Clear stored state
        ctx.clear("infra_nodes");
        ctx.clear("infra_endpoint_urls");
        ctx.clear("infra_execution_id");
        ctx.clear("infra_namespace");
        ctx.clear("project");
        ctx.set("infra_status", "terminated".to_string());

        tracing::info!("[DO_TERMINATE] project={} terminated", project_id);
        Ok(())
    }

    async fn get_status(
        &self,
        ctx: SharedObjectContext<'_>,
    ) -> Result<Json<InfraStatusResponse>, HandlerError> {
        let project_id = ctx.key().to_string();
        let status = ctx.get::<String>("infra_status").await?
            .unwrap_or_else(|| "none".to_string());
        let nodes_json: String = ctx.get("infra_nodes").await?
            .unwrap_or_else(|| "[]".to_string());
        let nodes: Vec<InfraNodeStatus> = serde_json::from_str(&nodes_json)
            .map_err(|e| TerminalError::new(format!("Failed to deserialize infra nodes: {}", e)))?;
        let exec_id = if status == "starting" {
            ctx.get::<String>("infra_execution_id").await?
        } else {
            None
        };

        Ok(Json(InfraStatusResponse {
            projectId: project_id,
            status,
            nodes,
            executionId: exec_id,
        }))
    }

    async fn get_infra_endpoint_urls(
        &self,
        ctx: SharedObjectContext<'_>,
    ) -> Result<Json<InfraEndpointUrls>, HandlerError> {
        let json: String = ctx.get("infra_endpoint_urls").await?
            .unwrap_or_else(|| "{}".to_string());
        let urls: BTreeMap<String, String> = serde_json::from_str(&json)
            .map_err(|e| TerminalError::new(format!("Failed to deserialize infra endpoint URLs: {}", e)))?;
        Ok(Json(InfraEndpointUrls { urls }))
    }
}

async fn load_infra_nodes(ctx: &ObjectContext<'_>) -> Result<Vec<InfraNodeStatus>, HandlerError> {
    let json: String = ctx.get("infra_nodes").await?
        .unwrap_or_else(|| "[]".to_string());
    serde_json::from_str(&json)
        .map_err(|e| TerminalError::new(format!("Failed to deserialize infra nodes: {}", e)).into())
}
