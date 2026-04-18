use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use weft_nodes::{TriggerInfo, TriggerStartConfig};
use weft_core::project::ProjectDefinition;
use weft_core::executor_core::ProjectExecutionRequest;

use crate::state::AppState;
use crate::trigger_store;
use crate::usage_store;
use crate::log_utils;

#[derive(Debug, Deserialize, Serialize)]
struct DashboardTokenClaims {
    pub user_id: String,
    pub email: Option<String>,
    pub iss: String,
    pub aud: String,
    pub iat: u64,
    pub exp: u64,
}

/// Check the request's internal API key. Constant-time, reads from state
/// (no per-request env access).
fn has_valid_internal_api_key(state: &AppState, headers: &HeaderMap) -> bool {
    use subtle::ConstantTimeEq;
    if state.internal_api_key.is_empty() {
        return false;
    }
    headers
        .get("x-internal-api-key")
        .and_then(|h| h.to_str().ok())
        .map(|provided| {
            provided
                .as_bytes()
                .ct_eq(state.internal_api_key.as_bytes())
                .into()
        })
        .unwrap_or(false)
}

fn is_local_mode() -> bool {
    std::env::var("DEPLOYMENT_MODE")
        .unwrap_or_else(|_| "cloud".to_string())
        .to_lowercase()
        == "local"
}

fn admin_email() -> String {
    std::env::var("ADMIN_EMAIL").unwrap_or_else(|_| "admin@weavemind.ai".to_string())
}

fn decode_dashboard_claims(headers: &HeaderMap) -> Result<DashboardTokenClaims, &'static str> {
    let secret = std::env::var("DASHBOARD_EMBED_SECRET").map_err(|_| "missing_auth_secret")?;

    let auth_header = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .ok_or("missing_auth_header")?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or("invalid_auth_header")?;

    let key = DecodingKey::from_secret(secret.as_bytes());
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&["weavemind-website"]);
    validation.set_audience(&["weavemind-dashboard"]);

    decode::<DashboardTokenClaims>(token, &key, &validation)
        .map(|data| data.claims)
        .map_err(|_| "invalid_token")
}

fn require_user_or_admin(
    headers: &HeaderMap,
    user_id: &str,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if is_local_mode() {
        return Ok(());
    }

    let claims = decode_dashboard_claims(headers).map_err(|reason| {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": format!("Unauthorized ({})", reason) })),
        )
    })?;

    let is_admin = claims
        .email
        .as_deref()
        .map(|email| email == admin_email())
        .unwrap_or(false);

    if claims.user_id == user_id || is_admin {
        return Ok(());
    }

    Err((
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({ "error": "Access denied" })),
    ))
}

fn require_admin(headers: &HeaderMap) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if is_local_mode() {
        return Ok(());
    }

    let claims = decode_dashboard_claims(headers).map_err(|reason| {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": format!("Unauthorized ({})", reason) })),
        )
    })?;

    let is_admin = claims
        .email
        .as_deref()
        .map(|email| email == admin_email())
        .unwrap_or(false);

    if is_admin {
        return Ok(());
    }

    Err((
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({ "error": "Admin access required" })),
    ))
}

fn require_internal_or_user_or_admin(
    state: &AppState,
    headers: &HeaderMap,
    user_id: &str,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if has_valid_internal_api_key(state, headers) {
        return Ok(());
    }

    require_user_or_admin(headers, user_id)
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

// =============================================================================
// TRIGGER ENDPOINTS
// =============================================================================

#[derive(Serialize)]
pub struct TriggerListResponse {
    pub triggers: Vec<TriggerInfo>,
}

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct ListTriggersQuery {
    pub userId: Option<String>,
}

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct RegisterTriggerRequest {
    pub triggerId: Option<String>,
    pub projectId: Uuid,
    pub triggerNodeId: Option<String>,
    pub triggerCategory: String,
    pub nodeType: Option<String>,
    pub userId: Option<String>,
    pub config: Option<serde_json::Value>,
    pub credentials: Option<serde_json::Value>,
    pub weftCode: Option<String>,
    pub projectHash: Option<String>,
}

#[derive(Serialize)]
#[allow(non_snake_case)]
pub struct RegisterTriggerResponse {
    pub triggerId: String,
    pub webhookUrl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct TriggerSetupCallback {
    pub executionId: Option<String>,
    pub status: String,
    pub nodeOutputs: Option<serde_json::Value>,
}

/// List triggers, optionally filtered by userId query parameter.
/// 
/// When userId is provided, only triggers belonging to that user are returned.
/// This enables authorization at the API level - callers should pass the
/// authenticated user's ID to ensure users only see their own triggers.
pub async fn list_triggers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListTriggersQuery>,
) -> Json<TriggerListResponse> {
    // Read from database for persistent state across instances
    let pool = &state.db_pool;
    
    // Use user-filtered query if userId provided (for authorization)
    let result = if let Some(ref user_id) = query.userId {
        trigger_store::list_triggers_by_user(pool, user_id).await
    } else {
        trigger_store::list_all_triggers(pool).await
    };
    
    match result {
        Ok(db_triggers) => {
            // Load pending actions to overlay transitional states (infra pattern)
            let pending_actions: std::collections::HashMap<String, String> = {
                let mut map = std::collections::HashMap::new();
                // Collect all unique project IDs from the triggers
                let project_ids: std::collections::HashSet<&str> = db_triggers.iter()
                    .map(|t| t.projectId.as_str())
                    .collect();
                for wf_id in project_ids {
                    if let Ok(actions) = trigger_store::get_trigger_pending_actions_by_project(pool, wf_id).await {
                        for (trigger_id, action, _) in actions {
                            map.insert(trigger_id, action);
                        }
                    }
                }
                map
            };

            let triggers: Vec<TriggerInfo> = db_triggers.into_iter()
                .filter(|t| t.status == "running" || t.status == "pending" || t.status == "setup_pending")
                .map(|t| {
                    // Overlay pending action if present (e.g. "activating", "deactivating")
                    let display_status = if let Some(action) = pending_actions.get(&t.id) {
                        match action.as_str() {
                            "activating" => "Activating".to_string(),
                            "deactivating" => "Deactivating".to_string(),
                            other => other.to_string(),
                        }
                    } else {
                        match t.status.as_str() {
                            "running" => "Running".to_string(),
                            "setup_pending" => "Activating".to_string(),
                            other => other.to_string(),
                        }
                    };
                    TriggerInfo {
                        triggerId: t.id,
                        triggerCategory: t.triggerCategory,
                        projectId: t.projectId,
                        status: display_status,
                        projectHash: t.projectHash,
                    }
                })
                .collect();
            tracing::info!("list_triggers from database: {} triggers (userId filter: {:?})", triggers.len(), query.userId);
            Json(TriggerListResponse { triggers })
        }
        Err(e) => {
            tracing::error!("Failed to list triggers from database: {}", e);
            Json(TriggerListResponse { triggers: vec![] })
        }
    }
}

pub async fn register_trigger(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterTriggerRequest>,
) -> (StatusCode, Json<RegisterTriggerResponse>) {
    let trigger_id = req.triggerId.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
    let trigger_node_id = req.triggerNodeId.clone().unwrap_or_else(|| trigger_id.clone());
    
    // Generate webhook URL for webhook-type triggers
    let webhook_url = match req.triggerCategory.as_str() {
        "Webhook" => Some(format!("/api/v1/webhooks/{}", trigger_id)),
        _ => None,
    };
    
    tracing::info!(
        "Registering trigger {} of type {} for project {}",
        trigger_id, req.triggerCategory, req.projectId
    );
    if let Some(ref config) = req.config {
        tracing::debug!("Trigger config: {}", log_utils::safe_json_log(config));
    }
    
    let node_type = req.nodeType.clone()
        .or_else(|| req.config.as_ref()
            .and_then(|c| c.get("nodeType"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()))
        .unwrap_or_else(|| "Unknown".to_string());
    
    let pool = &state.db_pool;
    let config = req.config.clone().unwrap_or(serde_json::json!({}));
    let user_id = match req.userId.as_deref() {
        Some(id) if !id.is_empty() => id,
        _ => {
            tracing::error!("userId is required for trigger registration");
            return (
                StatusCode::BAD_REQUEST,
                Json(RegisterTriggerResponse {
                    triggerId: String::new(),
                    webhookUrl: None,
                    status: Some("error".to_string()),
                }),
            );
        }
    };
    // Store the project hash sent by the frontend for stale detection.
    // The frontend computes this from its stripped project fingerprint.
    let project_hash_ref = req.projectHash.as_deref();

    if let Err(e) = trigger_store::upsert_trigger(
            pool,
            &trigger_store::UpsertTriggerParams {
                id: &trigger_id,
                project_id: &req.projectId.to_string(),
                trigger_node_id: &trigger_node_id,
                trigger_category: &req.triggerCategory,
                node_type: &node_type,
                user_id: Some(user_id),
                config: &config,
                credentials: req.credentials.as_ref(),
                project_definition: req.weftCode.as_ref().map(|c| serde_json::Value::String(c.clone())).as_ref(),
                project_hash: project_hash_ref,
            },
        ).await {
        tracing::error!("Failed to save trigger {} to database: {}", trigger_id, e);
    } else {
        tracing::info!("Trigger {} saved to database", trigger_id);
    }
    
    // ALL triggers go through the sub-execution flow.
    // Compile weftCode into a ProjectDefinition and extract the trigger setup
    // subgraph, and dispatch it to the executor with isTriggerSetup=true.
    let weft_code = match req.weftCode {
        Some(ref code) if !code.is_empty() => code.clone(),
        _ => {
            tracing::error!("weftCode is required for trigger registration");
            return (
                StatusCode::BAD_REQUEST,
                Json(RegisterTriggerResponse {
                    triggerId: String::new(),
                    webhookUrl: None,
                    status: Some("error".to_string()),
                }),
            );
        }
    };

    let mut wf: ProjectDefinition = match weft_core::weft_compiler::compile(&weft_code, req.projectId) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("Failed to compile weftCode for trigger: {:?}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(RegisterTriggerResponse {
                    triggerId: String::new(),
                    webhookUrl: None,
                    status: Some("error".to_string()),
                }),
            );
        }
    };
    if let Err(errors) = weft_nodes::enrich::enrich_project(&mut wf, state.node_registry) {
        tracing::error!("Project validation failed: {:?}", errors);
        return (
            StatusCode::BAD_REQUEST,
            Json(RegisterTriggerResponse {
                triggerId: String::new(),
                webhookUrl: None,
                status: Some(format!("Project validation failed: {}", errors.join("; "))),
            }),
        );
    }

    let sub_wf = match wf.extract_trigger_setup_subgraph(&trigger_node_id) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to extract trigger setup subgraph: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RegisterTriggerResponse {
                    triggerId: String::new(),
                    webhookUrl: None,
                    status: Some("error".to_string()),
                }),
            );
        }
    };

    // Set transitional "activating" state immediately so status queries reflect it
    if let Err(e) = trigger_store::set_trigger_pending_action(pool, &trigger_id, "activating").await { tracing::warn!("Trigger DB update failed: {}", e); }

    // Cancel any previous setup execution to avoid stale callbacks (infra pattern)
    if let Ok(Some(existing)) = trigger_store::get_trigger(pool, &trigger_id).await {
        if let Some(ref prev_exec_id) = existing.setupExecutionId {
            tracing::info!(
                "Cancelling previous trigger setup execution {} for trigger {}",
                prev_exec_id, trigger_id
            );
            let cancel_url = format!(
                "{}/ProjectExecutor/{}/cancel",
                state.executor_url, prev_exec_id
            );
            let _ = state.http_client.post(&cancel_url)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await;
        }

        // Stop any existing in-memory trigger before re-setup
        if existing.status == "running" {
            let service = state.trigger_service.lock().await;
            let _ = service.stop_trigger(&trigger_id).await;
            let _ = service.unregister_trigger(&trigger_id).await;
        }
    }

    // Increment run counter atomically and build unique execution ID.
    // The counter ensures each activation gets a unique execution ID,
    // so stale callbacks from previous activations can be detected.
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let api_base = std::env::var("API_BASE_URL")
        .or_else(|_| std::env::var("WEBHOOK_BASE_URL"))
        .unwrap_or_else(|_| format!("http://localhost:{}", port));

    let run_counter = match trigger_store::increment_setup_run_counter(pool, &trigger_id).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to increment setup run counter for trigger {}: {}", trigger_id, e);
            1
        }
    };
    let execution_id = format!("trigger-setup-{}-{}", trigger_id, run_counter);

    // Update with the real execution_id (no counter increment, just set the ID)
    if let Err(e) = trigger_store::set_setup_execution_id(pool, &trigger_id, &execution_id).await { tracing::warn!("Trigger DB update failed: {}", e); }

    let callback_url = format!(
        "{}/api/v1/triggers/{}/setup-completed",
        api_base, trigger_id
    );

    let start_req = ProjectExecutionRequest {
        project: sub_wf,
        input: serde_json::json!({
            "projectId": req.projectId.to_string(),
            "triggerNodeId": trigger_node_id,
        }),
        userId: req.userId.clone(),
        statusCallbackUrl: Some(callback_url),
        isInfraSetup: false,
        isTriggerSetup: true,
        weftCode: None,
        testMode: false,
        triggerId: None,
        nodeType: None,
        mocks: None,
    };

    let url = format!(
        "{}/ProjectExecutor/{}/start",
        state.executor_url, execution_id
    );
    match state.http_client.post(&url)
        .json(&start_req)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
    {
        Ok(_) => {
            tracing::info!(
                "Trigger setup sub-execution dispatched for trigger {} (execution: {})",
                trigger_id, execution_id
            );
            if let Err(e) = trigger_store::update_trigger_status(pool, &trigger_id, "setup_pending", None).await { tracing::warn!("Trigger DB update failed: {}", e); }
        }
        Err(e) => {
            tracing::error!("Failed to dispatch trigger setup sub-execution: {}", e);
            if let Err(e) = trigger_store::update_trigger_status(pool, &trigger_id, "failed", None).await { tracing::warn!("Trigger DB update failed: {}", e); }
            if let Err(e) = trigger_store::clear_trigger_pending_action(pool, &trigger_id).await { tracing::warn!("Trigger DB update failed: {}", e); }
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RegisterTriggerResponse {
                    triggerId: String::new(),
                    webhookUrl: None,
                    status: Some("failed".to_string()),
                }),
            );
        }
    }

    (
        StatusCode::ACCEPTED,
        Json(RegisterTriggerResponse {
            triggerId: trigger_id,
            webhookUrl: webhook_url,
            status: Some("activating".to_string()),
        }),
    )
}

/// Callback endpoint for trigger setup sub-execution completion.
/// The executor calls this when the trigger setup sub-graph finishes.
/// We extract the trigger node's output (its resolved inputs), merge them into
/// the trigger config, and call keep_alive (via register_trigger on TriggerService).
pub async fn trigger_setup_completed(
    State(state): State<Arc<AppState>>,
    Path(trigger_id): Path<String>,
    Json(callback): Json<TriggerSetupCallback>,
) -> StatusCode {
    tracing::info!(
        "Trigger setup completed for trigger {}: status={}, executionId={:?}",
        trigger_id, callback.status, callback.executionId
    );

    let pool = &state.db_pool;

    // Load trigger from database to get stored config, credentials, and setup_execution_id
    let trigger = match trigger_store::get_trigger(pool, &trigger_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::error!("Trigger {} not found in database", trigger_id);
            return StatusCode::NOT_FOUND;
        }
        Err(e) => {
            tracing::error!("Failed to load trigger {}: {}", trigger_id, e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    // Stale callback detection (infra pattern):
    // If the trigger's setup_execution_id was cleared (by deactivation) or doesn't
    // match this callback's executionId, this is a stale callback: discard it.
    match (&trigger.setupExecutionId, &callback.executionId) {
        (None, _) => {
            tracing::info!(
                "Ignoring trigger_setup_completed for {} (no active setup execution, trigger may have been deactivated)",
                trigger_id
            );
            return StatusCode::OK;
        }
        (Some(current), Some(cb)) if current != cb => {
            tracing::warn!(
                "Ignoring stale trigger_setup_completed for {} (callback from {}, current is {})",
                trigger_id, cb, current
            );
            return StatusCode::OK;
        }
        _ => {}
    }

    if callback.status != "completed" {
        tracing::error!("Trigger setup failed for trigger {}", trigger_id);
        if let Err(e) = trigger_store::update_trigger_status(pool, &trigger_id, "failed", None).await { tracing::warn!("Trigger DB update failed: {}", e); }
        if let Err(e) = trigger_store::clear_trigger_pending_action(pool, &trigger_id).await { tracing::warn!("Trigger DB update failed: {}", e); }
        return StatusCode::OK;
    }

    // Extract the trigger node's output from the sub-execution results.
    // The nodeOutputs map contains { nodeId: output } for each node in the subgraph.
    let trigger_node_id = &trigger.triggerNodeId;
    let resolved_config = callback.nodeOutputs
        .as_ref()
        .and_then(|outputs| outputs.get(trigger_node_id))
        .cloned()
        .unwrap_or(serde_json::json!({}));

    // Merge the resolved config (from upstream inputs) with the stored config
    let mut merged_config = trigger.config.clone();
    if let (Some(base), Some(resolved)) = (merged_config.as_object_mut(), resolved_config.as_object()) {
        for (k, v) in resolved {
            base.insert(k.clone(), v.clone());
        }
    }

    tracing::info!(
        "Trigger {} resolved config from setup: {}",
        trigger_id, log_utils::safe_json_log(&resolved_config)
    );

    // Inject userId into the config so keep_alive can pass it to the FormRegistrar
    if let Some(obj) = merged_config.as_object_mut() {
        if let Some(ref uid) = trigger.userId {
            obj.insert("userId".to_string(), serde_json::Value::String(uid.clone()));
        }
    }

    // Build TriggerStartConfig with the merged config
    let start_config = TriggerStartConfig {
        id: trigger_id.clone(),
        projectId: trigger.projectId.clone(),
        triggerNodeId: trigger_node_id.clone(),
        config: merged_config.clone(),
        credentials: trigger.credentials.clone(),
    };

    // Update the stored config in DB so trigger restarts use the resolved values
    if let Err(e) = trigger_store::update_trigger_config(pool, &trigger_id, &merged_config).await { tracing::warn!("Trigger DB update failed: {}", e); }

    // Start the trigger
    let service = state.trigger_service.lock().await;

    if let Err(e) = service.register_trigger(start_config, &trigger.triggerCategory).await {
        tracing::error!("Failed to start trigger {} after setup: {}", trigger_id, e);
        if let Err(e) = trigger_store::update_trigger_status(pool, &trigger_id, "failed", None).await { tracing::warn!("Trigger DB update failed: {}", e); }
        if let Err(e) = trigger_store::clear_trigger_pending_action(pool, &trigger_id).await { tracing::warn!("Trigger DB update failed: {}", e); }
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    if let Err(e) = trigger_store::update_trigger_status(pool, &trigger_id, "running", Some(&state.instance_id)).await {
        tracing::error!("CRITICAL: Trigger {} is running but failed to update DB status: {}", trigger_id, e);
    }
    if let Err(e) = trigger_store::clear_trigger_pending_action(pool, &trigger_id).await {
        tracing::error!("Failed to clear pending action for trigger {}: {}", trigger_id, e);
    }
    tracing::info!("Trigger {} started after setup completion", trigger_id);

    StatusCode::OK
}

pub async fn unregister_project_triggers(
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> StatusCode {
    tracing::info!("Unregistering all triggers for project {}", project_id);

    let pool = &state.db_pool;

    // Load triggers before stopping so we can cancel their setup executions
    let triggers_to_stop = match trigger_store::list_triggers_by_project(pool, &project_id).await {
        Ok(all) => all.into_iter()
            .filter(|t| t.status == "running" || t.status == "pending" || t.status == "setup_pending")
            .collect::<Vec<_>>(),
        Err(e) => {
            tracing::error!("Failed to list triggers for project {}: {}", project_id, e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    // Set transitional "deactivating" state for each trigger immediately
    for t in &triggers_to_stop {
        if let Err(e) = trigger_store::set_trigger_pending_action(pool, &t.id, "deactivating").await { tracing::warn!("Trigger DB update failed: {}", e); }
    }

    // Clear setup_execution_id FIRST so that any cancel callback is rejected as stale,
    // then send the cancel request. This avoids a race where the callback sets status='failed'
    // before stop_triggers_by_project runs.
    for t in &triggers_to_stop {
        if let Err(e) = trigger_store::clear_setup_execution_id(pool, &t.id).await { tracing::warn!("Trigger DB update failed: {}", e); }

        if let Some(ref exec_id) = t.setupExecutionId {
            tracing::info!("Cancelling setup execution {} for trigger {}", exec_id, t.id);
            let cancel_url = format!(
                "{}/ProjectExecutor/{}/cancel",
                state.executor_url, exec_id
            );
            let _ = state.http_client.post(&cancel_url)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await;
        }
    }

    // Stop all triggers in the database
    match trigger_store::stop_triggers_by_project(pool, &project_id).await {
        Ok(ids) => {
            tracing::info!("Stopped {} triggers in database for project {}: {:?}", ids.len(), project_id, ids);
        }
        Err(e) => {
            tracing::error!("Failed to stop triggers for project {}: {}", project_id, e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };
    
    // Stop in-memory triggers using the list loaded before any DB mutations,
    // not stopped_ids which can be empty if a callback raced and changed status.
    let service = state.trigger_service.lock().await;
    for t in &triggers_to_stop {
        if let Err(e) = service.stop_trigger(&t.id).await {
            tracing::warn!("Failed to stop trigger {} in memory: {}", t.id, e);
        }
        if let Err(e) = service.unregister_trigger(&t.id).await {
            tracing::warn!("Failed to unregister trigger {} in memory: {}", t.id, e);
        }
    }

    // Clear pending actions now that deactivation is complete
    for t in &triggers_to_stop {
        if let Err(e) = trigger_store::clear_trigger_pending_action(pool, &t.id).await { tracing::warn!("Trigger DB update failed: {}", e); }
    }
    
    StatusCode::OK
}

// =============================================================================
// USAGE TRACKING ENDPOINTS
// =============================================================================

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct UsageQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct UsageEventRequest {
    pub userId: String,
    pub eventType: String,
    pub subtype: Option<String>,
    pub projectId: Option<String>,
    pub executionId: Option<String>,
    pub nodeId: Option<String>,
    pub model: Option<String>,
    pub promptTokens: Option<i32>,
    pub completionTokens: Option<i32>,
    pub costUsd: Option<f64>,
    pub isByok: Option<bool>,
    pub metadata: Option<serde_json::Value>,
}

pub async fn record_usage_event(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<UsageEventRequest>,
) -> impl IntoResponse {
    let is_internal = is_local_mode() || has_valid_internal_api_key(&state, &headers);
    if !is_internal {
        if let Err((status, json)) = require_user_or_admin(&headers, &req.userId) {
            return (status, json).into_response();
        }
    }

    // Only internal services can set isByok. User-facing requests always pay.
    let is_byok = if is_internal { req.isByok.unwrap_or(false) } else { false };

    let is_local = is_local_mode();
    let result = match req.eventType.as_str() {
        "service" | "tangle" => {
            usage_store::record_service_cost(
                &state.db_pool,
                &req.userId,
                &req.eventType,
                req.subtype.as_deref(),
                req.projectId.as_deref(),
                req.executionId.as_deref(),
                req.nodeId.as_deref(),
                req.model.as_deref(),
                req.promptTokens,
                req.completionTokens,
                req.costUsd.unwrap_or(0.0),
                is_byok,
                is_local,
                req.metadata.as_ref(),
            )
            .await
        }
        "infra_daily" => {
            usage_store::record_infra_daily(
                &state.db_pool,
                &req.userId,
                req.costUsd.unwrap_or(0.0),
                req.metadata.as_ref(),
            )
            .await
        }
        "execution" => {
            tracing::warn!(
                "record_usage_event rejecting event_type=execution from {}; use /usage/start-execution",
                req.userId
            );
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "event_type=execution not allowed here; use POST /api/v1/usage/start-execution"
                })),
            )
                .into_response();
        }
        _ => {
            tracing::warn!("Unknown usage event type: {}", req.eventType);
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    match result {
        Ok(_) => StatusCode::CREATED.into_response(),
        Err(e) => {
            tracing::error!("Failed to record usage event: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct StartExecutionRequest {
    pub userId: String,
    pub projectId: String,
    pub executionId: String,
    /// Optional: which trigger fired this run (webhook, schedule, etc.).
    /// Stored on the executions row for the dashboard list.
    #[serde(default)]
    pub triggerId: Option<String>,
    /// Optional: the trigger's node type (Webhook, Cron, ManualTrigger, ...).
    #[serde(default)]
    pub nodeType: Option<String>,
}

/// Single chokepoint for project-execution start. Cloud: atomic
/// gate + charge + executions row. Local: writes the executions row only.
pub async fn start_execution(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<StartExecutionRequest>,
) -> impl IntoResponse {
    let local = is_local_mode();
    if !local && !has_valid_internal_api_key(&state, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Internal auth required" })),
        )
            .into_response();
    }

    // `anonymous` must never reach here in cloud mode. Published/visitor runs
    // are supposed to resolve to the owner's userId before the orchestrator
    // starts. If one slips through, fail loudly rather than run free.
    if !local && req.userId == "anonymous" {
        tracing::error!(
            "start_execution received anonymous userId in cloud mode (execution={}): refusing",
            req.executionId
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "anonymous userId is not billable" })),
        )
            .into_response();
    }

    if Uuid::parse_str(&req.projectId).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "projectId must be a UUID" })),
        )
            .into_response();
    }
    // executionId can be `<uuid>`, `publish-<owner>-<uuid>`, or
    // `trigger-setup-<triggerId>-<counter>`; bound length and charset.
    let exec_ok = !req.executionId.is_empty()
        && req.executionId.len() <= 128
        && req
            .executionId
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !exec_ok {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "executionId must be 1..=128 ASCII alnum/_/- chars" })),
        )
            .into_response();
    }
    // triggerId is opaque (`${projectId}-${nodeId}` from the dashboard).
    if let Some(ref tid) = req.triggerId {
        let valid = !tid.is_empty()
            && tid.len() <= 128
            && tid.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        if !valid {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "triggerId must be 1..=128 ASCII alnum/_/- chars" })),
            )
                .into_response();
        }
    }
    if let Some(ref nt) = req.nodeType {
        let valid = !nt.is_empty()
            && nt.len() <= 64
            && nt.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        if !valid {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "nodeType must be 1..=64 ASCII alnum/_/- chars" })),
            )
                .into_response();
        }
    }

    if local {
        // No fee in local; still register the executions row.
        let result = sqlx::query(
            r#"
            INSERT INTO executions (id, project_id, user_id, trigger_id, node_type, status)
            VALUES ($1, $2::uuid, $3, $4, $5, 'running')
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&req.executionId)
        .bind(&req.projectId)
        .bind(&req.userId)
        .bind(req.triggerId.as_deref())
        .bind(req.nodeType.as_deref())
        .execute(&state.db_pool)
        .await;
        return match result {
            Ok(_) => StatusCode::CREATED.into_response(),
            Err(e) => {
                tracing::error!(
                    "start_execution (local) failed to insert executions row (execution={}): {}",
                    req.executionId, e
                );
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        };
    }

    match usage_store::record_execution(
        &state.db_pool,
        &req.userId,
        &req.projectId,
        &req.executionId,
        req.triggerId.as_deref(),
        req.nodeType.as_deref(),
    )
    .await
    {
        Ok(usage_store::StartExecutionOutcome::Allowed) => StatusCode::CREATED.into_response(),
        Ok(usage_store::StartExecutionOutcome::InsufficientCredits { balance, required }) => {
            (
                StatusCode::PAYMENT_REQUIRED,
                Json(serde_json::json!({
                    "error": "Insufficient credits",
                    "balance": balance,
                    "required": required,
                })),
            )
                .into_response()
        }
        Ok(usage_store::StartExecutionOutcome::ProjectNotOwned) => {
            tracing::warn!(
                "start_execution: user {} does not own project {}",
                req.userId, req.projectId
            );
            (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({ "error": "Project not owned by caller" })),
            )
                .into_response()
        }
        Ok(usage_store::StartExecutionOutcome::ExecutionIdConflictWrongUser) => {
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "Execution ID already in use by another user" })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(
                "start_execution DB error (user={}, execution={}): {}",
                req.userId, req.executionId, e
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn get_usage(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    Query(query): Query<UsageQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(response) = require_user_or_admin(&headers, &user_id) {
        return response;
    }

    let today = chrono::Utc::now().date_naive();

    let from_date = query
        .from
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| today - chrono::Duration::days(30));

    let to_date = query
        .to
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or(today);

    // Aggregate today's data before querying (so the response is fresh)
    let _ = usage_store::aggregate_daily(&state.db_pool, today).await;

    match usage_store::get_daily_usage(&state.db_pool, &user_id, from_date, to_date).await {
        Ok(usage) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "userId": user_id,
                "from": from_date.to_string(),
                "to": to_date.to_string(),
                "daily": usage,
            })),
        ),
        Err(e) => {
            tracing::error!("Failed to get usage for user {}: {}", user_id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Failed to get usage data" })),
            )
        }
    }
}

// =============================================================================
// EXECUTION COST ENDPOINT
// =============================================================================

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct ExecutionCostQuery {
    pub executionId: String,
}

pub async fn get_execution_cost(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ExecutionCostQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Extract user_id from JWT claims for ownership check
    let user_id = if is_local_mode() || has_valid_internal_api_key(&state, &headers) {
        None
    } else {
        match decode_dashboard_claims(&headers) {
            Ok(claims) => Some(claims.user_id),
            Err(_) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({ "error": "Unauthorized" })),
                );
            }
        }
    };

    match usage_store::get_execution_cost(&state.db_pool, &query.executionId, user_id.as_deref()).await {
        Ok(cost) => (
            StatusCode::OK,
            Json(serde_json::json!({ "cost": cost })),
        ),
        Err(e) => {
            tracing::error!("Failed to get execution cost for {}: {}", query.executionId, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Failed to get execution cost" })),
            )
        }
    }
}

// =============================================================================
// CREDITS ENDPOINTS
// =============================================================================

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct AddCreditsRequest {
    pub userId: String,
    pub amount: f64,
    pub reason: Option<String>,
}

pub async fn add_credits(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<AddCreditsRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(response) = require_admin(&headers) {
        return response;
    }

    if req.amount <= 0.0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Amount must be positive" })),
        );
    }

    let reason = req.reason.unwrap_or_else(|| "admin_grant".to_string());

    match add_credits_to_user(&state.db_pool, &req.userId, req.amount, &reason).await {
        Ok(new_balance) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "userId": req.userId,
                "added": req.amount,
                "balance": new_balance,
                "reason": reason,
            })),
        ),
        Err(e) => {
            tracing::error!("Failed to add credits for user {}: {}", req.userId, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Failed to add credits" })),
            )
        }
    }
}

async fn add_credits_to_user(
    pool: &sqlx::PgPool,
    user_id: &str,
    amount: f64,
    reason: &str,
) -> Result<f64, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Upsert balance
    let row = sqlx::query_scalar!(
        r#"INSERT INTO user_credits (user_id, balance_usd, updated_at)
           VALUES ($1, $2, NOW())
           ON CONFLICT (user_id) DO UPDATE SET
               balance_usd = user_credits.balance_usd + $2,
               updated_at = NOW()
           RETURNING balance_usd"#,
        user_id,
        amount,
    )
    .fetch_one(&mut *tx)
    .await?;

    // Audit log
    sqlx::query!(
        r#"INSERT INTO credit_transactions (user_id, amount_usd, reason, balance_after)
           VALUES ($1, $2, $3, $4)"#,
        user_id,
        amount,
        reason,
        row,
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(row)
}

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct GetCreditsQuery {
    pub userId: String,
}

pub async fn get_credits(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<GetCreditsQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(response) = require_internal_or_user_or_admin(&state, &headers, &query.userId) {
        return response;
    }

    match sqlx::query_as::<_, (f64, bool, String)>(
        r#"SELECT uc.balance_usd, uc.has_paid, uc.tier FROM user_credits uc WHERE uc.user_id = $1"#,
    )
    .bind(&query.userId)
    .fetch_optional(&state.db_pool)
    .await
    {
        Ok(Some((balance, has_paid, tier))) => {
            // Look up execution cost and margin for the user's tier
            let tier_info: Option<(f64, f64)> = sqlx::query_as(
                "SELECT execution_base_cost, margin FROM pricing_tiers WHERE tier = $1",
            )
            .bind(&tier)
            .fetch_optional(&state.db_pool)
            .await
            .unwrap_or(None);
            let (exec_cost, margin) = tier_info.unwrap_or((0.01, 1.6));

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "userId": query.userId,
                    "balance": balance,
                    "hasPaid": has_paid,
                    "tier": tier,
                    "executionCost": exec_cost,
                    "margin": margin,
                })),
            )
        }
        Ok(None) => (
            StatusCode::OK,
            Json(serde_json::json!({ "userId": query.userId, "balance": 0.0, "hasPaid": false, "tier": "usage", "executionCost": 0.01, "margin": 1.6 })),
        ),
        Err(e) => {
            tracing::error!("Failed to get credits for user {}: {}", query.userId, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Failed to get credits" })),
            )
        }
    }
}

/// Start infrastructure for a project: compile + enrich weftCode, then forward to Restate.
/// This ensures the Restate handler receives a fully enriched ProjectDefinition with
/// correct features (isInfrastructure, isTrigger, etc.) from the node registry.
pub async fn start_infra(
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let weft_code = body.get("weftCode").and_then(|v| v.as_str()).unwrap_or_default();
    let user_id = body.get("userId").and_then(|v| v.as_str());

    // Require userId in cloud mode
    if !is_local_mode() && (user_id.is_none() || user_id == Some("anonymous")) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": "userId is required to start infrastructure"
        }))).into_response();
    }

    // Feature gate and credit check: skip in local mode
    if !is_local_mode() {
        if let Some(uid) = user_id {
            let has_paid: bool = sqlx::query_scalar(
                "SELECT COALESCE(has_paid, false) FROM user_credits WHERE user_id = $1",
            )
            .bind(uid)
            .fetch_optional(&state.db_pool)
            .await
            .expect("DB error checking has_paid for infra start")
            .unwrap_or(false);

            if !has_paid {
                return (StatusCode::PAYMENT_REQUIRED, Json(serde_json::json!({
                    "error": "Infrastructure requires a payment. Add credits to your account to unlock this feature."
                }))).into_response();
            }
        }
    }

    // Credit check: require $5 reserve per running infra instance (skip in local mode)
    if !is_local_mode() {
        if let Some(uid) = user_id {
            let running_count: i64 = sqlx::query_scalar(
                r#"
                SELECT COUNT(*) FROM infra_pending_action ipa
                JOIN projects p ON p.id::text = ipa.project_id
                WHERE p.user_id = $1 AND ipa.action NOT IN ('stopping', 'terminating')
                "#,
            )
            .bind(uid)
            .fetch_one(&state.db_pool)
            .await
            .unwrap_or(0);

            if let Err(msg) = usage_store::check_infra_start_allowed(&state.db_pool, uid, running_count).await {
                return (StatusCode::PAYMENT_REQUIRED, Json(serde_json::json!({
                    "error": msg
                }))).into_response();
            }
        }
    }

    // Compile + enrich
    let pid = match Uuid::parse_str(&project_id) {
        Ok(p) => p,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                "error": "project_id must be a UUID"
            }))).into_response();
        }
    };
    let project = match weft_core::weft_compiler::compile(weft_code, pid) {
        Ok(mut w) => {
            if let Err(errors) = weft_nodes::enrich::enrich_project(&mut w, state.node_registry) {
                tracing::error!("Project validation failed: {:?}", errors);
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                    "error": format!("Project validation failed: {}", errors.join("; "))
                }))).into_response();
            }
            w
        }
        Err(e) => {
            tracing::error!("Failed to compile weftCode for infra start: {:?}", e);
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                "error": format!("Weft compilation failed: {:?}", e)
            }))).into_response();
        }
    };

    // Set transitional flag in Postgres (immediate, so get_infra_status returns "starting")
    if let Err(e) = sqlx::query(
        "INSERT INTO infra_pending_action (project_id, action) VALUES ($1, 'starting')
         ON CONFLICT (project_id) DO UPDATE SET action = 'starting', created_at = NOW()"
    )
    .bind(&project_id)
    .execute(&state.db_pool)
    .await {
        tracing::error!("[start_infra] Failed to set pending action: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
    }

    // Forward to Restate with the pre-compiled project (fire-and-forget via /send)
    let mut req_body = serde_json::json!({
        "weftCode": weft_code,
        "project": project,
    });
    if let Some(uid) = user_id {
        req_body.as_object_mut().unwrap().insert("userId".to_string(), serde_json::json!(uid));
    }

    let url = format!("{}/InfrastructureManager/{}/start_all/send", state.restate_url, project_id);
    match state.http_client.post(&url)
        .json(&req_body)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() || resp.status() == reqwest::StatusCode::ACCEPTED => {
            tracing::info!("[start_infra] Restate accepted start_all for project {}", project_id);
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!("[start_infra] Restate rejected start_all for {}: {} {}", project_id, status, body);
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Restate rejected start request" }))).into_response();
        }
        Err(e) => {
            tracing::error!("[start_infra] Failed to reach Restate for {}: {}", project_id, e);
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": format!("Cannot reach Restate: {}", e) }))).into_response();
        }
    }

    Json(serde_json::json!({ "status": "starting", "nodes": [], "projectId": project_id })).into_response()
}

/// Kill all stuck InfrastructureManager invocations for a project, unblocking
/// the Restate virtual object key so the user can retry start_all.
pub async fn force_retry_infra(
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    // Validate project_id is a UUID to prevent SQL injection via Restate's /query endpoint
    if !project_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "Invalid project ID" }))).into_response();
    }

    let admin_url = &state.restate_admin_url;
    let client = &state.http_client;

    let query = format!(
        "SELECT id, target, status FROM sys_invocation \
         WHERE target LIKE '%InfrastructureManager/{}%' \
         AND status NOT IN ('completed')",
        project_id
    );

    let query_url = format!("{}/query", admin_url);
    let query_resp = client.post(&query_url)
        .header("accept", "application/json")
        .body(query)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    // Parse response: Restate returns { "rows": [[col0, col1, ...], ...] }
    // Our SELECT order: id (0), target (1), status (2)
    let inv_ids: Vec<String> = match query_resp {
        Ok(resp) if resp.status().is_success() => {
            let body = resp.text().await.unwrap_or_default();
            tracing::debug!("[force_retry_infra] Restate query response: {}", &body[..body.len().min(500)]);
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(data) => data.get("rows")
                    .and_then(|r| r.as_array())
                    .map(|rows| {
                        rows.iter()
                            .filter_map(|row| {
                                row.as_array()
                                    .and_then(|cols| cols.first())
                                    .and_then(|v| v.as_str())
                                    .map(String::from)
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                Err(e) => {
                    tracing::error!("[force_retry_infra] Failed to parse query response: {} body={}", e, &body[..body.len().min(200)]);
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Failed to query Restate" }))).into_response();
                }
            }
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!("[force_retry_infra] Restate query failed ({}): {}", status, body);
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Restate admin query failed" }))).into_response();
        }
        Err(e) => {
            tracing::error!("[force_retry_infra] Failed to reach Restate admin: {}", e);
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Cannot reach Restate admin API" }))).into_response();
        }
    };

    let mut killed = 0usize;
    for inv_id in &inv_ids {
        let kill_url = format!("{}/invocations/{}/kill", admin_url, inv_id);
        let kill_resp = client.patch(&kill_url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await;
        match kill_resp {
            Ok(r) if r.status().is_success() => {
                tracing::info!("[force_retry_infra] Killed invocation {} for project {}", inv_id, project_id);
                killed += 1;
            }
            Ok(r) => {
                tracing::warn!("[force_retry_infra] Kill {} returned {}", inv_id, r.status());
            }
            Err(e) => {
                tracing::warn!("[force_retry_infra] Failed to kill {}: {}", inv_id, e);
            }
        }
    }

    tracing::info!(
        "[force_retry_infra] project={}: found {} stuck invocations, killed {}",
        project_id, inv_ids.len(), killed
    );

    Json(serde_json::json!({
        "found": inv_ids.len(),
        "killed": killed,
    })).into_response()
}

/// Stop infrastructure: write transitional flag to Postgres, then fire Restate /send.
/// The Postgres flag ensures get_infra_status returns "stopping" immediately,
/// even if the Restate exclusive handler hasn't processed the request yet.
pub async fn stop_infra(
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    if !project_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "Invalid project ID" }))).into_response();
    }

    // Set transitional flag in Postgres (immediate, no lock contention)
    if let Err(e) = sqlx::query(
        "INSERT INTO infra_pending_action (project_id, action) VALUES ($1, 'stopping')
         ON CONFLICT (project_id) DO UPDATE SET action = 'stopping', created_at = NOW()"
    )
    .bind(&project_id)
    .execute(&state.db_pool)
    .await {
        tracing::error!("[stop_infra] Failed to set pending action: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
    }

    // Fire Restate /send (durable, fire-and-forget)
    let url = format!("{}/InfrastructureManager/{}/stop_all/send", state.restate_url, project_id);
    let client = &state.http_client;
    match client.post(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() || resp.status() == reqwest::StatusCode::ACCEPTED => {
            tracing::info!("[stop_infra] Restate accepted stop_all for project {}", project_id);
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!("[stop_infra] Restate rejected stop_all for {}: {} {}", project_id, status, body);
            // Clear the pending action since the request failed
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Restate rejected stop request" }))).into_response();
        }
        Err(e) => {
            tracing::error!("[stop_infra] Failed to reach Restate for {}: {}", project_id, e);
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Cannot reach Restate" }))).into_response();
        }
    }

    Json(serde_json::json!({ "status": "stopping" })).into_response()
}

/// Terminate infrastructure: same pattern as stop_infra.
pub async fn terminate_infra(
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    if !project_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "Invalid project ID" }))).into_response();
    }

    if let Err(e) = sqlx::query(
        "INSERT INTO infra_pending_action (project_id, action) VALUES ($1, 'terminating')
         ON CONFLICT (project_id) DO UPDATE SET action = 'terminating', created_at = NOW()"
    )
    .bind(&project_id)
    .execute(&state.db_pool)
    .await {
        tracing::error!("[terminate_infra] Failed to set pending action: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Database error" }))).into_response();
    }

    let url = format!("{}/InfrastructureManager/{}/terminate_all/send", state.restate_url, project_id);
    let client = &state.http_client;
    match client.post(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() || resp.status() == reqwest::StatusCode::ACCEPTED => {
            tracing::info!("[terminate_infra] Restate accepted terminate_all for project {}", project_id);
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!("[terminate_infra] Restate rejected terminate_all for {}: {} {}", project_id, status, body);
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Restate rejected terminate request" }))).into_response();
        }
        Err(e) => {
            tracing::error!("[terminate_infra] Failed to reach Restate for {}: {}", project_id, e);
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Cannot reach Restate" }))).into_response();
        }
    }

    Json(serde_json::json!({ "status": "terminating" })).into_response()
}

/// Get infrastructure status: queries Restate get_status, overlays Postgres
/// transitional flag when the backend hasn't caught up yet.
pub async fn get_infra_status(
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    if !project_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "Invalid project ID" }))).into_response();
    }

    // Query Restate for the actual status
    let url = format!("{}/InfrastructureManager/{}/get_status", state.restate_url, project_id);
    let client = &state.http_client;
    let restate_resp = client.post(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    let (restate_status, restate_body) = match restate_resp {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<serde_json::Value>().await {
                Ok(body) => {
                    let status = body.get("status").and_then(|s| s.as_str()).unwrap_or("none").to_string();
                    (status, body)
                }
                Err(e) => {
                    tracing::error!("[get_infra_status] Failed to parse Restate response: {}", e);
                    return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Invalid Restate response" }))).into_response();
                }
            }
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!("[get_infra_status] Restate error for {}: {} {}", project_id, status, body);
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Restate error" }))).into_response();
        }
        Err(e) => {
            tracing::error!("[get_infra_status] Cannot reach Restate for {}: {}", project_id, e);
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Cannot reach Restate" }))).into_response();
        }
    };

    // Check for pending action in Postgres
    let pending: Option<(String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT action, created_at FROM infra_pending_action WHERE project_id = $1"
    )
    .bind(&project_id)
    .fetch_optional(&state.db_pool)
    .await
    .unwrap_or(None);

    let transitional_states = ["starting", "stopping", "terminating"];

    if let Some((pending_action, created_at)) = pending {
        // If the pending action is stale (> 5 minutes), clean it up
        let age = chrono::Utc::now() - created_at;
        if age > chrono::Duration::minutes(5) {
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            // Fall through to return Restate's status
        } else if transitional_states.contains(&restate_status.as_str()) && restate_status == pending_action {
            // Restate has caught up to the same transitional state we requested, clear the pending action.
            // We check the action matches to handle rapid stop->terminate: if Restate reports
            // "stopping" but the pending action is "terminating", we keep the pending action.
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            // Return Restate's status (it's already correct)
        } else if restate_status == "stopped" || restate_status == "terminated" || restate_status == "failed" || restate_status == "none" {
            // Terminal state reached, clear the pending action
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            // Return Restate's status
        } else if pending_action == "starting" && restate_status == "running" {
            // Start completed: Restate reached "running", clear the pending "starting" action
            let _ = sqlx::query("DELETE FROM infra_pending_action WHERE project_id = $1")
                .bind(&project_id).execute(&state.db_pool).await;
            // Return Restate's "running" status
        } else {
            // Restate still reports non-transitional (e.g. "running") but we have a pending stop/terminate.
            // The backend hasn't processed the stop/terminate yet. Return the pending action as status.
            let mut response = restate_body.clone();
            response["status"] = serde_json::Value::String(pending_action);
            return Json(response).into_response();
        }
    }

    Json(restate_body).into_response()
}

/// Proxy GET /live from a sidecar for a specific infra node.
/// Looks up the sidecar endpoint URL via InfrastructureManager, then forwards the request.
pub async fn get_infra_live_data(
    State(state): State<Arc<AppState>>,
    Path((project_id, node_id)): Path<(String, String)>,
) -> impl IntoResponse {
    // Get endpoint URLs from Restate
    let url = format!("{}/InfrastructureManager/{}/get_infra_endpoint_urls", state.restate_url, project_id);
    let urls_resp = state.http_client.post(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    let endpoint_url = match urls_resp {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<serde_json::Value>().await {
                Ok(body) => {
                    body.get("urls")
                        .and_then(|u| u.get(&node_id))
                        .and_then(|v| v.as_str())
                        .map(String::from)
                }
                Err(_) => None,
            }
        }
        _ => None,
    };

    let endpoint_url = match endpoint_url {
        Some(u) => u,
        None => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "No endpoint URL for this node" }))).into_response();
        }
    };

    // The endpoint URL points to /action. Derive the base URL and call /live.
    let base_url = endpoint_url.trim_end_matches("/action");
    let live_url = format!("{}/live", base_url);

    match state.http_client.get(&live_url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<serde_json::Value>().await {
                Ok(body) => Json(body).into_response(),
                Err(_) => (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "Invalid response from sidecar" }))).into_response(),
            }
        }
        Ok(resp) => {
            let status = resp.status().as_u16();
            (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": format!("Sidecar returned {}", status) }))).into_response()
        }
        Err(e) => {
            (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": format!("Cannot reach sidecar: {}", e) }))).into_response()
        }
    }
}

// =============================================================================
// TEMP FILE STORAGE
// =============================================================================

// =============================================================================
// UNIFIED FILE STORAGE
//
// One endpoint for all file operations: user uploads, temp media, etc.
// Storage: data/files/{file_id} (flat, UUID-keyed, O(1) lookup)
// Metadata: data/files/{file_id}.meta (JSON sidecar)
//
// Flow:
//   1. POST /api/v1/files → creates metadata, returns { file_id, upload_url, url }
//   2. PUT upload_url with bytes → writes to disk
//   3. GET /api/v1/files/{file_id} → serves file
//
// In cloud mode, the cloud-api overrides POST to return a presigned R2 PUT URL
// instead of a local upload_url. The GET endpoint is also overridden.
// =============================================================================

fn files_dir() -> std::path::PathBuf {
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());
    std::path::PathBuf::from(data_dir).join("files")
}

fn api_base_url() -> String {
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    std::env::var("API_URL")
        .or_else(|_| std::env::var("API_BASE_URL"))
        .unwrap_or_else(|_| format!("http://localhost:{}", port))
}

fn sanitize_filename(raw: &str) -> String {
    let cleaned: String = raw
        .rsplit(['/', '\\']).next().unwrap_or("file")
        .replace("..", "")
        .chars().filter(|c| !c.is_control()).collect();
    if cleaned.is_empty() { "file".to_string() } else { cleaned }
}

/// Authenticate a file request. Returns the user_id.
/// In local mode: no auth needed, returns "local".
/// Otherwise: requires internal API key (backend nodes) or dashboard JWT.
fn authenticate_file_request(state: &AppState, headers: &HeaderMap) -> Result<String, (StatusCode, String)> {
    if is_local_mode() {
        return Ok("local".to_string());
    }
    if has_valid_internal_api_key(state, headers) {
        return headers.get("x-user-id")
            .and_then(|h| h.to_str().ok())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .ok_or((StatusCode::BAD_REQUEST, "Missing x-user-id header".to_string()));
    }
    // Dashboard JWT auth
    let claims = decode_dashboard_claims(headers)
        .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;
    Ok(claims.user_id)
}

/// POST /api/v1/files
///
/// Creates a file record and returns an upload URL.
/// The caller then PUTs bytes to the upload_url.
///
/// Request body: { filename, mimeType, sizeBytes, ephemeral?, executionId? }
/// Response: { file_id, upload_url, url, filename, mimeType }
pub async fn create_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let user_id = authenticate_file_request(&state, &headers)?;

    let filename = sanitize_filename(
        body.get("filename").and_then(|v| v.as_str()).unwrap_or("file")
    );
    let mime_type = body.get("mimeType").and_then(|v| v.as_str()).unwrap_or("application/octet-stream");
    let ephemeral = body.get("ephemeral").and_then(|v| v.as_bool()).unwrap_or(false);
    let execution_id = body.get("executionId").and_then(|v| v.as_str()).unwrap_or("");

    let file_id = Uuid::new_v4();
    let base = files_dir();
    if let Err(e) = tokio::fs::create_dir_all(&base).await {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create files dir: {e}")));
    }

    // Write metadata
    let meta = serde_json::json!({
        "filename": filename,
        "mimeType": mime_type,
        "userId": user_id,
        "ephemeral": ephemeral,
        "executionId": execution_id,
        "createdAt": chrono::Utc::now().to_rfc3339(),
    });
    let meta_path = base.join(format!("{}.meta", file_id));
    tokio::fs::write(&meta_path, serde_json::to_string(&meta).unwrap().as_bytes())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to write metadata: {e}")))?;

    let api_base = api_base_url();
    let upload_url = format!("{}/api/v1/files/{}/upload", api_base, file_id);
    let url = format!("{}/api/v1/files/{}", api_base, file_id);

    Ok(Json(serde_json::json!({
        "file_id": file_id.to_string(),
        "upload_url": upload_url,
        "url": url,
        "filename": filename,
        "mimeType": mime_type,
    })))
}

/// PUT /api/v1/files/{file_id}/upload
///
/// Receives file bytes and writes them to disk.
/// In cloud mode this endpoint is unused (bytes go directly to R2 via presigned URL).
pub async fn upload_file_bytes(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(file_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Auth
    let _user_id = authenticate_file_request(&state, &headers)?;

    // Validate file_id is a UUID
    let file_uuid = Uuid::parse_str(&file_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid file_id".to_string()))?;

    if body.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Empty file body".to_string()));
    }

    let base = files_dir();
    let meta_path = base.join(format!("{}.meta", file_uuid));

    // Verify the file record exists (POST /api/v1/files must be called first)
    if !tokio::fs::metadata(&meta_path).await.is_ok() {
        return Err((StatusCode::NOT_FOUND, "File record not found. Call POST /api/v1/files first.".to_string()));
    }

    // Write file bytes
    let file_path = base.join(file_uuid.to_string());
    tokio::fs::write(&file_path, &body)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to write file: {e}")))?;

    tracing::info!("Uploaded file {} ({} bytes)", file_uuid, body.len());
    Ok(StatusCode::OK)
}

/// GET /api/v1/files/{file_id}
///
/// Serves a locally stored file with the correct Content-Type.
pub async fn get_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(file_id): Path<String>,
) -> impl IntoResponse {
    let file_uuid = match Uuid::parse_str(&file_id) {
        Ok(u) => u,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    // In local mode, no auth. Otherwise require internal key or JWT.
    if !is_local_mode() {
        if let Err((status, msg)) = authenticate_file_request(&state, &headers) {
            return (status, msg).into_response();
        }
    }

    let base = files_dir();
    let file_path = base.join(file_uuid.to_string());
    let meta_path = base.join(format!("{}.meta", file_uuid));

    let bytes = match tokio::fs::read(&file_path).await {
        Ok(b) => b,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let mime = tokio::fs::read_to_string(&meta_path).await.ok()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .and_then(|v| v.get("mimeType").and_then(|m| m.as_str()).map(|s| s.to_string()))
        .unwrap_or_else(|| "application/octet-stream".to_string());

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, mime)],
        bytes,
    ).into_response()
}

/// DELETE /api/v1/files/{file_id}
///
/// Deletes a locally stored file and its metadata.
pub async fn delete_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let user_id = authenticate_file_request(&state, &headers)?;
    let file_uuid = Uuid::parse_str(&file_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid file_id".to_string()))?;

    let base = files_dir();
    let file_path = base.join(file_uuid.to_string());
    let meta_path = base.join(format!("{}.meta", file_uuid));

    // Check ownership (in local mode, skip: single user)
    if !is_local_mode() {
        let meta = tokio::fs::read_to_string(&meta_path).await
            .map_err(|_| (StatusCode::NOT_FOUND, "File not found".to_string()))?;
        let meta: serde_json::Value = serde_json::from_str(&meta)
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Corrupt metadata".to_string()))?;
        let owner = meta.get("userId").and_then(|v| v.as_str()).unwrap_or("");
        if owner != user_id {
            return Err((StatusCode::FORBIDDEN, "Access denied".to_string()));
        }
    }

    let _ = tokio::fs::remove_file(&file_path).await;
    let _ = tokio::fs::remove_file(&meta_path).await;

    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/files
///
/// Lists files for the current user.
pub async fn list_files(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let user_id = authenticate_file_request(&state, &headers)?;
    let base = files_dir();

    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(&base).await
        .map_err(|_| (StatusCode::OK, "[]".to_string()))?; // Empty dir = empty list

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".meta") { continue; }

        if let Ok(raw) = tokio::fs::read_to_string(entry.path()).await {
            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&raw) {
                let owner = meta.get("userId").and_then(|v| v.as_str()).unwrap_or("");
                if owner == user_id || is_local_mode() {
                    let file_id = name.trim_end_matches(".meta");
                    files.push(serde_json::json!({
                        "id": file_id,
                        "filename": meta.get("filename").and_then(|v| v.as_str()).unwrap_or(""),
                        "mimeType": meta.get("mimeType").and_then(|v| v.as_str()).unwrap_or(""),
                        "ephemeral": meta.get("ephemeral").and_then(|v| v.as_bool()).unwrap_or(false),
                        "createdAt": meta.get("createdAt").and_then(|v| v.as_str()).unwrap_or(""),
                    }));
                }
            }
        }
    }

    Ok(Json(files))
}
