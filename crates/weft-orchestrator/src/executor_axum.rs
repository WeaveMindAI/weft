//! In-memory axum-based project executor.
//!
//! Replaces the Restate-based ProjectExecutor for the hot path.
//! All execution state lives in a DashMap keyed by execution_id.
//! Node dispatches are fired via tokio::spawn (no journaling).
//! Calls to TaskRegistry / NodeInstanceRegistry
//! go through HTTP to the Restate ingress.
//!
//! Architecture: each execution is split into:
//! - ExecImmutable (Arc): project, edge_idx, initial_input (never changes)
//! - ExecMutable (Mutex): pulses, cancelled
//! This lets us borrow project/edge_idx while mutating pulses.

use std::sync::Arc;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use tokio::sync::Mutex;
use weft_core::{
    ProjectDefinition,
    ProjectExecutionRequest, ProjectExecutionResult,
    NodeCallbackRequest, ProvideInputRequest,
    NodeStatusMap, NodeOutputMap,
    PendingTask, TaskType,
    PulseStatus, PulseTable,
    NodeExecutionStatus, NodeExecution, NodeExecutionTable,
    NodeExecuteRequest, NodeInstance,
};
use weft_core::executor_core::{
    find_ready_nodes, emit_null_downstream, preprocess_input, postprocess_output,
    check_completion,
    build_completion_callback_payload, build_cancel_callback_payload,
    init_pulses,
    build_node_statuses_from_executions, build_node_ordering_from_executions,
    build_node_outputs_from_executions,
};
use weft_core::project::EdgeIndex;

// =============================================================================
// STATE
// =============================================================================

/// Immutable per-execution data (never changes after creation).
struct ExecImmutable {
    project: ProjectDefinition,
    edge_idx: EdgeIndex,
    initial_input: serde_json::Value,
    user_id: Option<String>,
    status_callback_url: Option<String>,
    is_infra_setup: bool,
    is_trigger_setup: bool,
    test_mode: bool,
    /// Mock overrides from test configs. Keys are node/group IDs.
    mocks: std::collections::HashMap<String, serde_json::Value>,
}

/// Mutable per-execution data.
struct ExecMutable {
    pulses: PulseTable,
    /// Records of each node execution (dispatch, completion, logs, cost).
    node_executions: NodeExecutionTable,
    cancelled: bool,
}

/// Full execution handle: immutable data in Arc, mutable data in Mutex.
/// The Arc<ExecImmutable> can be cloned cheaply and borrowed while
/// ExecMutable is locked, avoiding split-borrow issues.
struct Execution {
    imm: Arc<ExecImmutable>,
    mt: Mutex<ExecMutable>,
}

pub struct ExecutorState {
    executions: DashMap<String, Arc<Execution>>,
    restate_url: String,
    api_url: String,
    /// Carries `x-internal-api-key` by default. Internal targets only
    /// (weft-api, Restate). Never user-controlled URLs: the key would leak.
    http_client: reqwest::Client,
    /// No auth headers. Use for status callbacks, node-runner dispatches,
    /// anything whose URL came from a request body or extension registration.
    external_http_client: reqwest::Client,
    callback_base: String,
    node_registry: &'static weft_nodes::NodeTypeRegistry,
}

impl ExecutorState {
    pub fn new(restate_url: String, callback_base: String) -> Self {
        let node_registry: &'static weft_nodes::NodeTypeRegistry =
            Box::leak(Box::new(weft_nodes::NodeTypeRegistry::new()));
        // API_URL is required in cloud (silent fallback would lose every charge).
        let is_local: bool = std::env::var("DEPLOYMENT_MODE")
            .unwrap_or_else(|_| "cloud".to_string())
            .to_lowercase()
            == "local";
        let api_url = match std::env::var("API_URL") {
            Ok(v) if !v.is_empty() => v,
            _ if is_local => "http://localhost:3000".to_string(),
            _ => panic!(
                "API_URL must be set in non-local DEPLOYMENT_MODE. \
                 Set it to the weft-api service URL (e.g. \
                 'http://weavemind-api:3001' in k8s, or set \
                 DEPLOYMENT_MODE=local for local development)."
            ),
        };
        let http_client = {
            let mut headers = reqwest::header::HeaderMap::new();
            if let Ok(key) = std::env::var("INTERNAL_API_KEY") {
                if !key.is_empty() {
                    if let Ok(val) = reqwest::header::HeaderValue::from_str(&key) {
                        headers.insert("x-internal-api-key", val);
                    }
                }
            }
            reqwest::Client::builder()
                .default_headers(headers)
                .connect_timeout(std::time::Duration::from_secs(10))
                .pool_idle_timeout(std::time::Duration::from_secs(30))
                .tcp_keepalive(std::time::Duration::from_secs(15))
                .build()
                .expect("failed to build internal HTTP client")
        };
        let external_http_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .tcp_keepalive(std::time::Duration::from_secs(15))
            .build()
            .expect("failed to build external HTTP client");
        Self {
            executions: DashMap::new(),
            restate_url,
            api_url,
            http_client,
            external_http_client,
            callback_base,
            node_registry,
        }
    }
}

pub type SharedState = Arc<ExecutorState>;

// =============================================================================
// ROUTER
// =============================================================================

pub fn router(state: SharedState) -> Router {
    let cors = tower_http::cors::CorsLayer::permissive();

    // Compile runs before billing, so cap to bound zero-credit DoS via huge weftCode.
    let start_body_limit = axum::extract::DefaultBodyLimit::max(512 * 1024);

    Router::new()
        .route("/ProjectExecutor/{execution_id}/start", post(handle_start))
        .route("/ProjectExecutor/{execution_id}/start/send", post(handle_start))
        .layer(start_body_limit)
        .route("/ProjectExecutor/{execution_id}/execution_callback", post(handle_execution_callback))
        .route("/ProjectExecutor/{execution_id}/cancel", post(handle_cancel))
        .route("/ProjectExecutor/{execution_id}/provide_input", post(handle_provide_input))
        .route("/ProjectExecutor/{execution_id}/get_status", get(handle_get_status).post(handle_get_status))
        .route("/ProjectExecutor/{execution_id}/get_node_statuses", get(handle_get_node_statuses).post(handle_get_node_statuses))
        .route("/ProjectExecutor/{execution_id}/get_all_outputs", get(handle_get_all_outputs).post(handle_get_all_outputs))
        .route("/ProjectExecutor/{execution_id}/get_node_executions", get(handle_get_node_executions).post(handle_get_node_executions))
        .route("/ProjectExecutor/{execution_id}/retry_node_dispatch", post(handle_retry_node_dispatch))
        .layer(cors)
        .with_state(state)
}

// =============================================================================
// HANDLERS
// =============================================================================

enum BillingOutcome {
    Proceed,
    InsufficientCredits(serde_json::Value),
    ProjectNotOwned(serde_json::Value),
    ExecutionIdConflict(serde_json::Value),
    /// Internal auth misconfig: never surface the underlying body to callers.
    InternalAuthFailed(serde_json::Value),
    BadRequest(serde_json::Value),
    /// Unknown weft-api response: never surface the underlying body to callers.
    InternalProtocolError(serde_json::Value),
    ApiUnreachable,
}

/// Call weft-api's atomic gate+ledger endpoint. Local-mode policy lives
/// in weft-api (no-op there). Retries 4x with ~15s total cap on transport
/// errors and 5xx; if still unreachable we fail the start (no free runs
/// during an outage).
async fn authorize_and_charge_start(
    state: &SharedState,
    execution_id: &str,
    project_id: uuid::Uuid,
    user_id: Option<&str>,
    trigger_id: Option<&str>,
    node_type: Option<&str>,
) -> BillingOutcome {
    // None => no attribution: only reachable in local mode (handle_start
    // rejects None in cloud).
    let user_id = match user_id {
        Some(uid) => uid,
        None => return BillingOutcome::Proceed,
    };

    let body = serde_json::json!({
        "userId": user_id,
        "projectId": project_id.to_string(),
        "executionId": execution_id,
        "triggerId": trigger_id,
        "nodeType": node_type,
    });
    let url = format!("{}/api/v1/usage/start-execution", state.api_url);

    // 4 attempts, 3s timeout, 1s backoff => ~15s worst case. 5xx retries; 4xx propagates.
    let backoffs_ms = [0u64, 1000, 1000, 1000];
    let mut last_transport_err: Option<String> = None;
    for (attempt, sleep_ms) in backoffs_ms.iter().enumerate() {
        if *sleep_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(*sleep_ms)).await;
        }
        let send = state
            .http_client
            .post(&url)
            .json(&body)
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await;
        match send {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    return BillingOutcome::Proceed;
                }
                // Helper to read the body as JSON, falling back to an
                // empty object when weft-api returns a non-JSON error.
                let read_payload = |resp: reqwest::Response| async move {
                    resp.json::<serde_json::Value>()
                        .await
                        .unwrap_or_else(|_| serde_json::json!({}))
                };
                match status {
                    reqwest::StatusCode::PAYMENT_REQUIRED => {
                        return BillingOutcome::InsufficientCredits(read_payload(resp).await);
                    }
                    reqwest::StatusCode::FORBIDDEN => {
                        return BillingOutcome::ProjectNotOwned(read_payload(resp).await);
                    }
                    reqwest::StatusCode::CONFLICT => {
                        return BillingOutcome::ExecutionIdConflict(read_payload(resp).await);
                    }
                    reqwest::StatusCode::UNAUTHORIZED => {
                        // Misconfiguration: orchestrator should always carry
                        // a valid internal key. Log loudly.
                        tracing::error!(
                            "[axum] start_execution returned 401 (internal-auth misconfig) for execution {}",
                            execution_id
                        );
                        return BillingOutcome::InternalAuthFailed(read_payload(resp).await);
                    }
                    reqwest::StatusCode::BAD_REQUEST => {
                        return BillingOutcome::BadRequest(read_payload(resp).await);
                    }
                    s if s.is_server_error() => {
                        tracing::warn!(
                            "[axum] start_execution returned {} for execution {} (attempt {}); retrying",
                            s, execution_id, attempt + 1
                        );
                        last_transport_err = Some(format!("HTTP {}", s));
                        continue;
                    }
                    other => {
                        // Unknown status: protocol mismatch with weft-api.
                        // Log internally; do not surface body externally.
                        let body = read_payload(resp).await;
                        tracing::error!(
                            "[axum] start_execution returned unexpected {} for execution {}: {}",
                            other, execution_id, body
                        );
                        return BillingOutcome::InternalProtocolError(body);
                    }
                }
            }
            Err(e) => {
                last_transport_err = Some(e.to_string());
                tracing::warn!(
                    "[axum] start_execution billing transport error for execution {} (attempt {}): {}",
                    execution_id, attempt + 1, e
                );
            }
        }
    }
    tracing::error!(
        "[axum] start_execution billing unreachable for execution {}: {:?}",
        execution_id, last_transport_err
    );
    BillingOutcome::ApiUnreachable
}

/// Resolve the trusted userId for a start request.
///
/// Cloud requires a valid `x-internal-api-key`. With the key present,
/// prefer `x-user-id` (cloud-api injects this after JWT verification);
/// otherwise fall back to `req.userId` (server-to-server callers).
/// `x-user-id` without the key is NEVER trusted: any pod with network
/// reach to the orchestrator could otherwise spoof it.
///
/// Local mode skips auth entirely.
fn resolve_trusted_user_id(
    headers: &axum::http::HeaderMap,
    req_user_id: Option<&str>,
) -> Result<Option<String>, axum::response::Response> {
    let is_local = std::env::var("DEPLOYMENT_MODE")
        .unwrap_or_else(|_| "cloud".to_string())
        .to_lowercase()
        == "local";
    if is_local {
        return Ok(req_user_id.map(|s| s.to_string()));
    }

    // Cloud: require valid internal API key. No fallback.
    let configured_key = match std::env::var("INTERNAL_API_KEY").ok().filter(|k| !k.is_empty()) {
        Some(k) => k,
        None => {
            tracing::error!("[axum] handle_start rejected: INTERNAL_API_KEY not configured in cloud mode");
            return Err((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Server misconfiguration" })),
            )
                .into_response());
        }
    };
    use subtle::ConstantTimeEq;
    let provided_key = headers.get("x-internal-api-key").and_then(|h| h.to_str().ok());
    let key_ok = provided_key
        .map(|p| p.as_bytes().ct_eq(configured_key.as_bytes()).into())
        .unwrap_or(false);
    if !key_ok {
        tracing::warn!("[axum] handle_start rejected: missing or invalid x-internal-api-key");
        return Err((
            axum::http::StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Unauthorized" })),
        )
            .into_response());
    }

    // Internal key valid. Prefer cloud-api's x-user-id (verified JWT identity)
    // over the request body. Server-to-server callers without a JWT identity
    // set req.userId directly; trust it because they hold the internal key.
    let header_uid = headers
        .get("x-user-id")
        .and_then(|h| h.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    Ok(header_uid.or_else(|| req_user_id.map(|s| s.to_string())))
}

async fn handle_start(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(req): Json<ProjectExecutionRequest>,
) -> impl IntoResponse {
    tracing::info!("[axum] start: execution={}, weftCode={}", execution_id, if req.weftCode.is_some() { "present" } else { "NONE" });

    // Validate caller identity before anything else; downstream uses only this.
    let trusted_user_id = match resolve_trusted_user_id(&headers, req.userId.as_deref()) {
        Ok(uid) => uid,
        Err(response) => return response,
    };

    // If weftCode is provided, compile it to get the ProjectDefinition
    let mut project = if let Some(ref weft_code) = req.weftCode {
        tracing::info!("[axum] compiling weftCode ({} bytes)", weft_code.len());
        match weft_core::weft_compiler::compile(weft_code, req.project.id) {
            Ok(mut compiled) => {
                // Preserve the original project metadata (id is set by the compiler).
                compiled.name = req.project.name.clone();
                compiled.description = req.project.description.clone();
                compiled.createdAt = req.project.createdAt;
                compiled.updatedAt = req.project.updatedAt;
                compiled
            }
            Err(errors) => {
                let msg = errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
                tracing::error!("[axum] weft compile error: {}", msg);
                return (StatusCode::BAD_REQUEST, Json(ProjectExecutionResult {
                    executionId: execution_id,
                    status: "failed".to_string(),
                    output: None,
                    error: Some(format!("Weft compilation failed: {}", msg)),
                })).into_response();
            }
        }
    } else {
        tracing::info!("[axum] no weftCode, using pre-built project ({} nodes, {} edges)", req.project.nodes.len(), req.project.edges.len());
        req.project
    };

    // Enrich with registry metadata (features, ports, filter UI-only nodes)
    if let Err(errors) = weft_nodes::enrich::enrich_project(&mut project, state.node_registry) {
        let msg = format!("Project validation failed:\n{}", errors.join("\n"));
        tracing::error!("[axum] {}", msg);
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "executionId": execution_id,
            "status": "failed",
            "error": msg,
        }))).into_response();
    }

    tracing::info!("[axum] final project: {} nodes, {} edges", project.nodes.len(), project.edges.len());
    for e in &project.edges {
        tracing::debug!("[axum] edge: {}.{} -> {}.{}", e.source, e.sourceHandle.as_deref().unwrap_or("?"), e.target, e.targetHandle.as_deref().unwrap_or("?"));
    }

    // Cloud requires userId; without this guard, a server-to-server caller
    // that forgot to set it would short-circuit billing and run free.
    let is_local = std::env::var("DEPLOYMENT_MODE")
        .unwrap_or_else(|_| "cloud".to_string())
        .to_lowercase()
        == "local";
    if !is_local && trusted_user_id.is_none() {
        tracing::error!(
            "[axum] handle_start refused: cloud mode requires userId (execution {})",
            execution_id
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "userId is required" })),
        )
            .into_response();
    }

    // Billing chokepoint. After compile (don't charge for invalid projects),
    // before any execution state is created (rejected payment leaves no orphan).
    match authorize_and_charge_start(
        &state,
        &execution_id,
        project.id,
        trusted_user_id.as_deref(),
        req.triggerId.as_deref(),
        req.nodeType.as_deref(),
    ).await {
        BillingOutcome::Proceed => {}
        BillingOutcome::InsufficientCredits(payload) => {
            return (StatusCode::PAYMENT_REQUIRED, Json(payload)).into_response();
        }
        BillingOutcome::ProjectNotOwned(payload) => {
            return (StatusCode::FORBIDDEN, Json(payload)).into_response();
        }
        BillingOutcome::ExecutionIdConflict(payload) => {
            return (StatusCode::CONFLICT, Json(payload)).into_response();
        }
        BillingOutcome::InternalAuthFailed(payload) => {
            // Misconfig: log internally, return generic 500.
            tracing::error!("[axum] billing endpoint rejected internal auth: {}", payload);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response();
        }
        BillingOutcome::BadRequest(payload) => {
            return (StatusCode::BAD_REQUEST, Json(payload)).into_response();
        }
        BillingOutcome::InternalProtocolError(payload) => {
            tracing::error!("[axum] billing protocol error: {}", payload);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response();
        }
        BillingOutcome::ApiUnreachable => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "Billing service unreachable. Please retry in a moment, and report on Discord if the issue persists.",
                })),
            )
                .into_response();
        }
    }

    let edge_idx = EdgeIndex::build(&project);
    let pulses = init_pulses(&project, &edge_idx);

    let imm = Arc::new(ExecImmutable {
        project,
        edge_idx,
        initial_input: req.input,
        user_id: trusted_user_id,
        status_callback_url: req.statusCallbackUrl,
        is_infra_setup: req.isInfraSetup,
        is_trigger_setup: req.isTriggerSetup,
        test_mode: req.testMode,
        mocks: req.mocks.unwrap_or_default(),
    });

    let mt = ExecMutable {
        pulses,
        node_executions: NodeExecutionTable::new(),
        cancelled: false,
    };

    let exec = Arc::new(Execution { imm: imm.clone(), mt: Mutex::new(mt) });
    state.executions.insert(execution_id.clone(), exec.clone());

    // Collect dispatch work under mutex, then dispatch outside
    let work = {
        let mut mt = exec.mt.lock().await;
        collect_dispatch_work(&imm, &mut mt)
    };
    execute_dispatch_work(&state, &execution_id, &imm, work).await;

    (StatusCode::OK, Json(ProjectExecutionResult {
        executionId: execution_id,
        status: "running".to_string(),
        output: None,
        error: None,
    })).into_response()
}

async fn handle_execution_callback(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
    Json(req): Json<NodeCallbackRequest>,
) -> impl IntoResponse {
    // Async callback path: used by nodes that pause mid-execution (e.g., HumanQuery sends
    // WaitingForInput here). Final completion comes via the synchronous dispatch response.
    match process_execution_callback(&state, &execution_id, req).await {
        Ok((dispatch_work, restate_tasks, imm)) => {
            run_completion_side_effects(&state, &execution_id, dispatch_work, restate_tasks, &imm).await;
            (StatusCode::OK, "ok").into_response()
        }
        Err(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
    }
}

/// Core logic for processing a node completion. Returns dispatch work and restate
/// tasks for the caller to execute. Called both from the HTTP handler and from
/// the dispatch task.
async fn process_execution_callback(
    state: &SharedState,
    execution_id: &str,
    req: NodeCallbackRequest,
) -> Result<(Vec<DispatchWorkItem>, Vec<PendingTask>, Arc<ExecImmutable>), String> {
    let pulse_id = req.pulseId.clone();
    tracing::info!("[axum] execution_callback: execution={} node={} pulse={} status={:?}", execution_id, req.nodeId, pulse_id, req.status);

    let exec = match state.executions.get(execution_id) {
        Some(e) => e.clone(),
        None => {
            tracing::error!("[axum] execution_callback for unknown execution: {}", execution_id);
            return Err("execution not found".to_string());
        }
    };

    let imm = &exec.imm;

    // --- All pulse mutations + dispatch collection happen under mutex ---
    // We also collect any Restate tasks to register outside the lock.
    let mut dispatch_work = Vec::new();
    let mut restate_tasks: Vec<PendingTask> = Vec::new();

    {
        let mut mt = exec.mt.lock().await;

        if mt.cancelled {
            return Ok((Vec::new(), Vec::new(), imm.clone()));
        }

        // Find execution info from NodeExecution (not pulse)
        let exec_info = mt.node_executions.get(&req.nodeId)
            .and_then(|execs| execs.iter().find(|e| e.pulseId == pulse_id))
            .map(|e| (e.color.clone(), e.lane.clone()));

        let (color, lane) = match exec_info {
            Some(info) => info,
            None => {
                tracing::error!("[axum] BUG: NodeExecution for pulse {} not found for node {}", pulse_id, req.nodeId);
                return Err("node execution not found".to_string());
            }
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Handle error
        if req.status == weft_core::NodeExecutionStatus::Failed || req.error.is_some() {
            let error_msg = req.error.unwrap_or_else(|| "Unknown error".to_string());
            tracing::error!("[axum] Node {} pulse {} failed: {}", req.nodeId, pulse_id, error_msg);

            // Update NodeExecution record
            if let Some(execs) = mt.node_executions.get_mut(&req.nodeId) {
                if let Some(exec) = execs.iter_mut().find(|e| e.pulseId == pulse_id) {
                    exec.status = NodeExecutionStatus::Failed;
                    exec.error = Some(error_msg);
                    exec.completedAt = Some(now_ms);
                    exec.costUsd = req.costUsd;
                }
            }

            // Emit null downstream so dependent nodes can proceed
            {
                let ExecMutable { pulses, node_executions, .. } = &mut *mt;
                postprocess_output(&req.nodeId, &serde_json::Value::Null, &color, &lane, &imm.project, pulses, &imm.edge_idx, node_executions);
            }
            dispatch_work = collect_dispatch_work(imm, &mut mt);
        }
        // Handle WaitingForInput
        else if req.status == weft_core::NodeExecutionStatus::WaitingForInput {
            let callback_id = req.waitingMetadata.as_ref()
                .map(|m| m.callbackId.clone())
                .unwrap_or_else(|| format!("{}-{}-{}", execution_id, req.nodeId, pulse_id));
            let runner_instance_id = req.waitingMetadata.as_ref()
                .and_then(|m| m.runnerInstanceId.clone());

            // Update NodeExecution record
            if let Some(execs) = mt.node_executions.get_mut(&req.nodeId) {
                if let Some(exec) = execs.iter_mut().find(|e| e.pulseId == pulse_id) {
                    exec.status = NodeExecutionStatus::WaitingForInput;
                    exec.callbackId = Some(callback_id.clone());
                    exec.runnerInstanceId = runner_instance_id.clone();
                }
            }

            if let Some(ref metadata) = req.waitingMetadata {
                let callback_id = callback_id.clone();
                restate_tasks.push(PendingTask {
                    executionId: callback_id,
                    nodeId: req.nodeId.clone(),
                    title: metadata.title.clone().unwrap_or_else(|| "Waiting for input".to_string()),
                    description: metadata.description.clone(),
                    data: req.output.clone().unwrap_or(serde_json::Value::Null),
                    createdAt: chrono::Utc::now().to_rfc3339(),
                    userId: imm.user_id.clone(),
                    taskType: TaskType::Task,
                    actionUrl: None,
                    formSchema: metadata.formSchema.clone(),
                    metadata: metadata.metadata.clone(),
                });
            }
            // No dispatch work for waiting
        }
        // Normal completion (or any other status)
        else {
            let output_value = req.output.unwrap_or(serde_json::Value::Null);

            // Check for __notify_action__
            if let Some(notify_action) = output_value.get("__notify_action__") {
                let action_id = notify_action.get("actionId")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&format!("{}-{}-action", execution_id, req.nodeId))
                    .to_string();
                restate_tasks.push(PendingTask {
                    executionId: action_id,
                    nodeId: req.nodeId.clone(),
                    title: notify_action.get("title").and_then(|v| v.as_str()).unwrap_or("Action").to_string(),
                    description: notify_action.get("description").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    data: notify_action.get("data").cloned().unwrap_or(serde_json::Value::Null),
                    createdAt: chrono::Utc::now().to_rfc3339(),
                    userId: imm.user_id.clone(),
                    taskType: TaskType::Action,
                    actionUrl: notify_action.get("actionUrl").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    formSchema: None,
                    metadata: serde_json::Value::Object(serde_json::Map::new()),
                });
            }

            // Update NodeExecution record
            if let Some(execs) = mt.node_executions.get_mut(&req.nodeId) {
                if let Some(exec) = execs.iter_mut().find(|e| e.pulseId == pulse_id) {
                    exec.status = NodeExecutionStatus::Completed;
                    exec.completedAt = Some(now_ms);
                    exec.output = Some(output_value.clone());
                    exec.costUsd = req.costUsd;
                }
            }

            // Output postprocessing: emit downstream Pending pulses
            {
                let ExecMutable { pulses, node_executions, .. } = &mut *mt;
                postprocess_output(&req.nodeId, &output_value, &color, &lane, &imm.project, pulses, &imm.edge_idx, node_executions);
            }

            // Collect dispatch work + check completion (under mutex)
            dispatch_work = collect_dispatch_work(imm, &mut mt);
        }

        // Check completion + unreachable node detection
        if !check_and_notify_inmem(&state, &execution_id, imm, &mt).await {
            // Unreachable detection: no active executions but Pending pulses remain
            let any_active_exec = mt.node_executions.values()
                .flat_map(|es| es.iter())
                .any(|e| !e.status.is_terminal());
            let any_pending = mt.pulses.values()
                .flat_map(|ps| ps.iter())
                .any(|p| p.status == PulseStatus::Pending);
            if !any_active_exec && any_pending {
                tracing::warn!("[axum] MARKING UNREACHABLE: no active executions but pending pulses exist");
                for node_pulses in mt.pulses.values_mut() {
                    for p in node_pulses.iter_mut() {
                        if p.status == PulseStatus::Pending {
                            p.status = PulseStatus::Absorbed;
                        }
                    }
                }
                check_and_notify_inmem(&state, &execution_id, imm, &mt).await;
            }
        }
        // mt (MutexGuard) dropped here
    }

    Ok((dispatch_work, restate_tasks, imm.clone()))
}

/// Execute the results of process_execution_callback: dispatch work + register tasks.
/// Uses Box::pin to break the async type recursion cycle:
/// dispatch_node_inmem -> process_execution_callback -> run_completion_side_effects -> execute_dispatch_work -> dispatch_node_inmem
fn run_completion_side_effects<'a>(
    state: &'a SharedState,
    execution_id: &'a str,
    dispatch_work: Vec<DispatchWorkItem>,
    restate_tasks: Vec<PendingTask>,
    imm: &'a Arc<ExecImmutable>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let state2 = state.clone();
        let task_fut = async move {
            for task in restate_tasks {
                register_task_via_restate(&state2, task).await;
            }
        };
        tokio::join!(
            task_fut,
            execute_dispatch_work(state, execution_id, imm, dispatch_work),
        );
    })
}


async fn handle_cancel(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
) -> impl IntoResponse {
    tracing::info!("[axum] cancel: execution={}", execution_id);

    let exec = match state.executions.get(&execution_id) {
        Some(e) => e.clone(),
        None => return (StatusCode::NOT_FOUND, "execution not found").into_response(),
    };

    let imm = &exec.imm;

    // Phase 1: under the mutex, flip state and collect the callback_ids that
    // need to be cleaned up. We do the Restate calls OUTSIDE the lock so the
    // cancel handler doesn't hold it for minutes while doing N sequential
    // HTTP POSTs.
    let callback_ids_to_complete: Vec<String> = {
        let mut mt = exec.mt.lock().await;
        mt.cancelled = true;

        let callback_ids: Vec<String> = mt.node_executions.values()
            .flat_map(|execs| execs.iter())
            .filter(|e| e.status == NodeExecutionStatus::WaitingForInput)
            .filter_map(|e| e.callbackId.clone())
            .collect();

        for node_pulses in mt.pulses.values_mut() {
            for p in node_pulses.iter_mut() {
                if p.status == PulseStatus::Pending {
                    p.status = PulseStatus::Absorbed;
                }
            }
        }

        let cancel_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        for execs in mt.node_executions.values_mut() {
            for exec in execs.iter_mut() {
                if !exec.status.is_terminal() {
                    exec.status = NodeExecutionStatus::Cancelled;
                    exec.completedAt = Some(cancel_ms);
                }
            }
        }

        callback_ids
    };

    // Phase 2: clean up Restate tasks, without holding the lock. Cap
    // concurrency so a cancel on an execution with hundreds of orphan tasks
    // can't stampede Restate (each complete_task invocation allocates journal
    // state, 220+ in parallel ate past the 4Gi limit and crashloop-evicted the
    // pod, taking orchestrator down with it).
    let cleanup_count = callback_ids_to_complete.len();
    if cleanup_count > 0 {
        tracing::info!("[axum] cancel: cleaning up {} pending tasks", cleanup_count);
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(20));
        let mut set = tokio::task::JoinSet::new();
        for cb_id in callback_ids_to_complete {
            let state = state.clone();
            let permit = semaphore.clone();
            set.spawn(async move {
                let _permit = permit.acquire_owned().await.ok();
                complete_task_via_restate(&state, &cb_id).await;
            });
        }
        while set.join_next().await.is_some() {}
    }

    // Phase 3: notify the dashboard (re-lock briefly to build the payload).
    if let Some(ref callback_url) = imm.status_callback_url {
        let payload = {
            let mt = exec.mt.lock().await;
            build_cancel_callback_payload(&execution_id, &mt.node_executions, &mt.pulses)
        };
        if let Err(e) = post_status_callback(state.external_http_client.clone(), callback_url, &payload).await {
            tracing::error!("[axum] Status callback failed for execution={}: {}", execution_id, e);
        }
    }

    (StatusCode::OK, "ok").into_response()
}

async fn handle_provide_input(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
    Json(req): Json<ProvideInputRequest>,
) -> impl IntoResponse {
    tracing::info!("[axum] provide_input: execution={} node={} pulse={}", execution_id, req.nodeId, req.pulseId);

    let exec = match state.executions.get(&execution_id) {
        Some(e) => e.clone(),
        None => return (StatusCode::NOT_FOUND, "execution not found").into_response(),
    };

    // Verify NodeExecution is waiting and read (callback_id, runner_instance_id)
    let (callback_id, runner_instance_id) = {
        let mt = exec.mt.lock().await;
        let exec_rec = mt.node_executions.get(&req.nodeId)
            .and_then(|execs| execs.iter().find(|e| e.pulseId == req.pulseId));
        match exec_rec {
            Some(e) if e.status == NodeExecutionStatus::WaitingForInput => {
                let cb = e.callbackId.clone().unwrap_or_else(|| format!("{}-{}-{}", execution_id, req.nodeId, req.pulseId));
                (cb, e.runnerInstanceId.clone())
            }
            Some(e) => {
                return (StatusCode::BAD_REQUEST, format!("execution not waiting (status: {})", e.status.as_str())).into_response();
            }
            None => {
                return (StatusCode::NOT_FOUND, "node execution not found").into_response();
            }
        }
    };

    // Remove from TaskRegistry
    complete_task_via_restate(&state, &callback_id).await;

    // Skip: send null output to the node so it can handle cancellation
    if req.skip {
        tracing::info!("[axum] provide_input: SKIP requested for node={} pulse={}", req.nodeId, req.pulseId);
        // For skip, we still need to unblock the node. Send null to the input_response
        // endpoint. The node decides how to handle it (typically returns null output).
    }

    // Route the form submission back to the same node-runner pod that registered
    // the in-memory channel. Form-input channels are per-pod, so a random pod
    // would 404. If runner_instance_id is missing the WaitingForInput record
    // predates this fix and there is no way to recover the originating pod;
    // tell the user to refresh.
    let runner_id = match runner_instance_id.as_deref() {
        Some(id) => id,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "This form session is too old to recover (started before the latest deploy). Please re-trigger the workflow to get a fresh form."
                })),
            ).into_response();
        }
    };

    let inst = match find_instance_by_id_via_restate(&state, runner_id).await {
        Some(inst) => inst,
        None => {
            tracing::error!("[axum] provide_input: runner instance '{}' missing from registry (cb={})", runner_id, callback_id);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "The server handling your form has restarted, the form session was lost. Please re-trigger the workflow to retry."
                })),
            ).into_response();
        }
    };

    // Forward the human's response to the node's /input_response endpoint.
    // This resolves the oneshot channel inside the node, allowing execute() to continue.
    // The node will eventually send execution_callback when it finishes.
    let input_payload = if req.skip {
        serde_json::Value::Null
    } else {
        req.input
    };

    // Set NodeExecution back to Running since the node is resuming
    {
        let mut mt = exec.mt.lock().await;
        if let Some(execs) = mt.node_executions.get_mut(&req.nodeId) {
            if let Some(exec_rec) = execs.iter_mut().find(|e| e.pulseId == req.pulseId) {
                exec_rec.status = NodeExecutionStatus::Running;
                exec_rec.callbackId = None;
            }
        }
    }

    // The runner_id segment pins the request through cloud-api / load-balanced
    // Services to the exact pod holding the in-memory form-input channel.
    let url = format!("{}/input_response/{}/{}", inst.endpoint, runner_id, callback_id);
    tracing::info!("[axum] provide_input: POSTing to {}", url);

    // Retry transient failures (node-runner can be momentarily unreachable
    // during rolling deploys or under load spikes). Internal client: node-runner
    // is operator-controlled.
    let delays = [
        std::time::Duration::from_millis(300),
        std::time::Duration::from_secs(1),
    ];
    let mut last_status: Option<reqwest::StatusCode> = None;
    let mut last_err: Option<String> = None;
    for (attempt, delay) in std::iter::once(std::time::Duration::ZERO).chain(delays).enumerate() {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        match state.http_client.post(&url).json(&input_payload).send().await {
            Ok(r) if r.status().is_success() => {
                if attempt > 0 {
                    tracing::info!("[axum] input_response succeeded on retry {} (cb={})", attempt, callback_id);
                }
                return (StatusCode::OK, "ok").into_response();
            }
            Ok(r) => {
                last_status = Some(r.status());
                last_err = None;
                tracing::warn!("[axum] input_response attempt {} returned {} (cb={})", attempt + 1, r.status(), callback_id);
            }
            Err(e) => {
                last_status = None;
                last_err = Some(e.to_string());
                tracing::warn!("[axum] input_response attempt {} failed (cb={}): {}", attempt + 1, callback_id, e);
            }
        }
    }
    tracing::error!("[axum] input_response FAILED after retries (cb={}): status={:?} err={:?}", callback_id, last_status, last_err);
    let user_msg = match last_status {
        Some(s) if s.as_u16() == 404 => "The form session has expired or the server handling it has restarted. Please re-trigger the workflow.".to_string(),
        Some(s) => format!("The server returned an error ({}) while submitting your form. The system may be overloaded, please retry in a moment.", s),
        None => "Could not reach the server handling your form. The system may be overloaded, please retry in a moment.".to_string(),
    };
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({ "error": user_msg })),
    ).into_response()
}

async fn handle_get_status(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
) -> impl IntoResponse {
    let exec = match state.executions.get(&execution_id) {
        Some(e) => e.clone(),
        None => return (StatusCode::NOT_FOUND, Json(serde_json::json!("unknown"))).into_response(),
    };
    let mt = exec.mt.lock().await;
    if mt.cancelled {
        return (StatusCode::OK, Json(serde_json::json!("cancelled"))).into_response();
    }
    let result = check_completion(&mt.pulses, &mt.node_executions);
    let status = match result {
        Some(true) => "failed",
        Some(false) => "completed",
        None => "running",
    };
    (StatusCode::OK, Json(serde_json::json!(status))).into_response()
}

async fn handle_get_node_statuses(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
) -> impl IntoResponse {
    let exec = match state.executions.get(&execution_id) {
        Some(e) => e.clone(),
        None => return (StatusCode::NOT_FOUND, "execution not found").into_response(),
    };
    let imm = &exec.imm;
    let mt = exec.mt.lock().await;
    let statuses = build_node_statuses_from_executions(&mt.node_executions, &mt.pulses);
    let ordering = build_node_ordering_from_executions(&mt.node_executions);
    let active_edges = weft_core::executor_core::compute_active_edges(&mt.pulses, &imm.project);
    (StatusCode::OK, Json(NodeStatusMap { statuses, ordering, activeEdges: active_edges })).into_response()
}

async fn handle_get_all_outputs(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
) -> impl IntoResponse {
    let exec = match state.executions.get(&execution_id) {
        Some(e) => e.clone(),
        None => return (StatusCode::NOT_FOUND, "execution not found").into_response(),
    };
    let mt = exec.mt.lock().await;
    let outputs = build_node_outputs_from_executions(&mt.node_executions);
    (StatusCode::OK, Json(NodeOutputMap { outputs })).into_response()
}

async fn handle_get_node_executions(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
) -> impl IntoResponse {
    let exec = match state.executions.get(&execution_id) {
        Some(e) => e.clone(),
        None => return (StatusCode::NOT_FOUND, "execution not found").into_response(),
    };
    let mt = exec.mt.lock().await;
    (StatusCode::OK, Json(&mt.node_executions)).into_response()
}

async fn handle_retry_node_dispatch(
    State(state): State<SharedState>,
    Path(execution_id): Path<String>,
    body: String,
) -> impl IntoResponse {
    let node_id: String = serde_json::from_str(&body).unwrap_or(body);
    tracing::info!("[axum] retry_node_dispatch: execution={} node={}", execution_id, node_id);

    let exec = match state.executions.get(&execution_id) {
        Some(e) => e.clone(),
        None => return (StatusCode::NOT_FOUND, "execution not found").into_response(),
    };

    let imm = &exec.imm;
    let mt = exec.mt.lock().await;

    // Find running NodeExecutions for this node and re-dispatch them
    let node = imm.project.nodes.iter().find(|n| n.id == node_id);
    if let Some(node) = node {
        let running_execs: Vec<(String, serde_json::Value)> = mt.node_executions.get(&node_id)
            .map(|execs| execs.iter()
                .filter(|e| e.status == NodeExecutionStatus::Running)
                .map(|e| {
                    (e.pulseId.clone(), e.input.clone().unwrap_or(serde_json::Value::Null))
                })
                .collect())
            .unwrap_or_default();

        let project_id = imm.project.id.to_string();
        let node = node.clone();
        let is_infra = imm.is_infra_setup;
        let is_trigger = imm.is_trigger_setup;
        let test_mode = imm.test_mode;
        let user_id = imm.user_id.clone();
        let mocks = imm.mocks.clone();
        drop(mt);
        for (pid, input) in running_execs {
            dispatch_node_inmem(&state, &execution_id, &node, input, &pid, &project_id, is_infra, is_trigger, test_mode, user_id.as_deref(), &mocks).await;
        }
    }

    (StatusCode::OK, "ok").into_response()
}

// =============================================================================
// DISPATCH LOGIC
// =============================================================================

/// Dispatch work item: all data needed to dispatch a node outside the mutex.
struct DispatchWorkItem {
    node: weft_core::NodeDefinition,
    input: serde_json::Value,
    pulse_id: String,
    project_id: String,
}

/// Collect ready nodes and prepare dispatch work.
/// Skip propagation happens synchronously (modifies pulses).
/// Actual dispatches are collected as work items to be executed outside the mutex.
fn collect_dispatch_work(
    imm: &ExecImmutable,
    mt: &mut ExecMutable,
) -> Vec<DispatchWorkItem> {
    let mut work_items = Vec::new();

    loop {
        // Preprocess: Expand input splitting + Gather input collapsing.
        // Runs until stable so all pulses end up at compatible depths.
        while preprocess_input(&imm.project, &mut mt.pulses) {}

        let ready = find_ready_nodes(&imm.project, &mt.pulses, &imm.initial_input, &imm.edge_idx);
        if ready.is_empty() {
            break;
        }

        let mut made_progress = false;

        for (node_id, group) in ready {
            let node = match imm.project.nodes.iter().find(|n| n.id == node_id) {
                Some(n) => n,
                None => continue,
            };

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // Mark input pulses as Absorbed (consumed by this dispatch)
            if let Some(node_pulses) = mt.pulses.get_mut(&node.id) {
                for p in node_pulses.iter_mut() {
                    if group.pulse_ids.contains(&p.id) && p.status == PulseStatus::Pending {
                        p.status = PulseStatus::Absorbed;
                    }
                }
            }


            if let Some(ref error_msg) = group.error {
                // Dispatch-time error (type mismatch, etc.)
                tracing::error!("[axum dispatch] node={} ERROR: {}", node.id, error_msg);
                mt.node_executions.entry(node.id.clone()).or_default().push(NodeExecution {
                    id: uuid::Uuid::new_v4().to_string(),
                    nodeId: node.id.clone(),
                    status: NodeExecutionStatus::Failed,
                    pulseIdsAbsorbed: group.pulse_ids.clone(),
                    pulseId: String::new(),
                    error: Some(error_msg.clone()),
                    callbackId: None,
                    runnerInstanceId: None,
                    startedAt: now_ms,
                    completedAt: Some(now_ms),
                    input: Some(group.input.clone()),
                    output: None,
                    costUsd: 0.0,
                    logs: Vec::new(),
                    color: group.color.clone(),
                    lane: group.lane.clone(),
                });
                emit_null_downstream(&node.id, &group.color, &group.lane, &imm.project, &mut mt.pulses, &imm.edge_idx, &mut mt.node_executions);
                made_progress = true;
            } else if group.should_skip {
                tracing::debug!("[axum dispatch] node={} lane={:?} SKIPPED", node.id, group.lane);
                mt.node_executions.entry(node.id.clone()).or_default().push(NodeExecution {
                    id: uuid::Uuid::new_v4().to_string(),
                    nodeId: node.id.clone(),
                    status: NodeExecutionStatus::Skipped,
                    pulseIdsAbsorbed: group.pulse_ids.clone(),
                    pulseId: String::new(),
                    error: None,
                    callbackId: None,
                    runnerInstanceId: None,
                    startedAt: now_ms,
                    completedAt: Some(now_ms),
                    input: Some(group.input.clone()),
                    output: None,
                    costUsd: 0.0,
                    logs: Vec::new(),
                    color: group.color.clone(),
                    lane: group.lane.clone(),
                });
                // If the skipped node is a group In boundary, the entire
                // group body is skipped as a unit. Mark every inner node as
                // Skipped (so their status shows up in the execution view)
                // and emit null on the Out boundary's outputs, then skip
                // emitting from the In boundary itself (its outputs would
                // just cascade into the already-skipped inner nodes).
                if let Some(gb) = node.groupBoundary.as_ref() {
                    if gb.role == weft_core::project::GroupBoundaryRole::In {
                        let group_id = gb.groupId.clone();
                        for inner in imm.project.nodes.iter() {
                            if inner.scope.contains(&group_id) && inner.id != node.id {
                                mt.node_executions.entry(inner.id.clone()).or_default().push(NodeExecution {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    nodeId: inner.id.clone(),
                                    status: NodeExecutionStatus::Skipped,
                                    pulseIdsAbsorbed: vec![],
                                    pulseId: String::new(),
                                    error: None,
                                    callbackId: None,
                                    runnerInstanceId: None,
                                    startedAt: now_ms,
                                    completedAt: Some(now_ms),
                                    input: None,
                                    output: None,
                                    costUsd: 0.0,
                                    logs: Vec::new(),
                                    color: group.color.clone(),
                                    lane: group.lane.clone(),
                                });
                            }
                        }
                        // Find the Out boundary for this group and emit null
                        // from it so downstream consumers of the group see
                        // the skip. The Out boundary itself is marked Skipped.
                        let out_id_opt = imm.project.nodes.iter().find_map(|n| {
                            match n.groupBoundary.as_ref() {
                                Some(b) if b.groupId == group_id
                                    && b.role == weft_core::project::GroupBoundaryRole::Out => Some(n.id.clone()),
                                _ => None,
                            }
                        });
                        if let Some(out_id) = out_id_opt {
                            mt.node_executions.entry(out_id.clone()).or_default().push(NodeExecution {
                                id: uuid::Uuid::new_v4().to_string(),
                                nodeId: out_id.clone(),
                                status: NodeExecutionStatus::Skipped,
                                pulseIdsAbsorbed: vec![],
                                pulseId: String::new(),
                                error: None,
                                callbackId: None,
                                runnerInstanceId: None,
                                startedAt: now_ms,
                                completedAt: Some(now_ms),
                                input: None,
                                output: None,
                                costUsd: 0.0,
                                logs: Vec::new(),
                                color: group.color.clone(),
                                lane: group.lane.clone(),
                            });
                            emit_null_downstream(&out_id, &group.color, &group.lane, &imm.project, &mut mt.pulses, &imm.edge_idx, &mut mt.node_executions);
                        }
                        // IMPORTANT: do NOT emit null from the In boundary
                        // itself: that would re-trigger the now-skipped inner
                        // nodes via their pending pulse queues.
                    } else {
                        emit_null_downstream(&node.id, &group.color, &group.lane, &imm.project, &mut mt.pulses, &imm.edge_idx, &mut mt.node_executions);
                    }
                } else {
                    emit_null_downstream(&node.id, &group.color, &group.lane, &imm.project, &mut mt.pulses, &imm.edge_idx, &mut mt.node_executions);
                }
                made_progress = true;
            } else if imm.test_mode && node.groupBoundary.as_ref().map_or(false, |gb| {
                gb.role == weft_core::project::GroupBoundaryRole::In && imm.mocks.contains_key(&gb.groupId)
            }) {
                // Group In passthrough for a mocked group: short-circuit the entire group.
                let gb = node.groupBoundary.as_ref().unwrap();
                let group_id = &gb.groupId;
                let out_node = imm.project.nodes.iter().find(|n| {
                    n.groupBoundary.as_ref().map_or(false, |b| {
                        b.groupId == *group_id && b.role == weft_core::project::GroupBoundaryRole::Out
                    })
                });
                let out_id = match out_node {
                    Some(n) => n.id.clone(),
                    None => {
                        tracing::error!("[axum dispatch] BUG: no Out boundary found for group '{}'", group_id);
                        continue;
                    }
                };
                tracing::info!("[axum dispatch] mocking group '{}': In completed, emitting mock on {}", group_id, out_id);

                // Mark In passthrough as completed
                mt.node_executions.entry(node.id.clone()).or_default().push(NodeExecution {
                    id: uuid::Uuid::new_v4().to_string(),
                    nodeId: node.id.clone(),
                    status: NodeExecutionStatus::Completed,
                    pulseIdsAbsorbed: group.pulse_ids.clone(),
                    pulseId: String::new(),
                    error: None,
                    callbackId: None,
                    runnerInstanceId: None,
                    startedAt: now_ms,
                    completedAt: Some(now_ms),
                    input: Some(group.input.clone()),
                    output: Some(group.input.clone()),
                    costUsd: 0.0,
                    logs: Vec::new(),
                    color: group.color.clone(),
                    lane: group.lane.clone(),
                });

                // Find Out boundary and sanitize mock data against its output ports
                let mock_value = &imm.mocks[group_id];
                let out_node = imm.project.nodes.iter().find(|n| n.id == out_id);
                let out_ports = out_node.map(|n| &n.outputs[..]).unwrap_or(&[]);
                let sanitized = sanitize_mock_output(mock_value, out_ports);

                // Mark Out boundary as completed with mock data
                mt.node_executions.entry(out_id.clone()).or_default().push(NodeExecution {
                    id: uuid::Uuid::new_v4().to_string(),
                    nodeId: out_id.clone(),
                    status: NodeExecutionStatus::Completed,
                    pulseIdsAbsorbed: vec![],
                    pulseId: String::new(),
                    error: None,
                    callbackId: None,
                    runnerInstanceId: None,
                    startedAt: now_ms,
                    completedAt: Some(now_ms),
                    input: Some(sanitized.clone()),
                    output: Some(sanitized.clone()),
                    costUsd: 0.0,
                    logs: Vec::new(),
                    color: group.color.clone(),
                    lane: group.lane.clone(),
                });

                // Emit mock data downstream of Out boundary via postprocess_output
                postprocess_output(&out_id, &sanitized, &group.color, &group.lane, &imm.project, &mut mt.pulses, &imm.edge_idx, &mut mt.node_executions);
                made_progress = true;
            } else if imm.test_mode && is_inside_mocked_group(node, &imm.mocks) {
                // Node is inside a mocked group: mark as skipped, no pulse emission.
                tracing::debug!("[axum dispatch] node={} SKIPPED (inside mocked group)", node.id);
                mt.node_executions.entry(node.id.clone()).or_default().push(NodeExecution {
                    id: uuid::Uuid::new_v4().to_string(),
                    nodeId: node.id.clone(),
                    status: NodeExecutionStatus::Skipped,
                    pulseIdsAbsorbed: group.pulse_ids.clone(),
                    pulseId: String::new(),
                    error: None,
                    callbackId: None,
                    runnerInstanceId: None,
                    startedAt: now_ms,
                    completedAt: Some(now_ms),
                    input: Some(group.input.clone()),
                    output: None,
                    costUsd: 0.0,
                    logs: Vec::new(),
                    color: group.color.clone(),
                    lane: group.lane.clone(),
                });
                made_progress = true;
            } else {
                // Normal dispatch: create NodeExecution, generate a pulse_id for callback routing
                let pulse_id = uuid::Uuid::new_v4().to_string();
                let project_id = imm.project.id.to_string();

                mt.node_executions.entry(node.id.clone()).or_default().push(NodeExecution {
                    id: uuid::Uuid::new_v4().to_string(),
                    nodeId: node.id.clone(),
                    status: NodeExecutionStatus::Running,
                    pulseIdsAbsorbed: group.pulse_ids.clone(),
                    pulseId: pulse_id.clone(),
                    error: None,
                    callbackId: None,
                    runnerInstanceId: None,
                    startedAt: now_ms,
                    completedAt: None,
                    input: Some(group.input.clone()),
                    output: None,
                    costUsd: 0.0,
                    logs: Vec::new(),
                    color: group.color.clone(),
                    lane: group.lane.clone(),
                });

                work_items.push(DispatchWorkItem {
                    node: node.clone(),
                    input: group.input,
                    pulse_id,
                    project_id,
                });
                made_progress = true;
            }
        }

        if !made_progress {
            break;
        }
    }

    work_items
}

/// Execute collected dispatch work items. Does NOT hold the per-execution mutex.
/// All dispatch setup (instance lookup, infra endpoints) runs concurrently via tokio::JoinSet.
/// The actual HTTP POST to node services is fire-and-forget (tokio::spawn inside dispatch_node_inmem).
async fn execute_dispatch_work(
    state: &SharedState,
    execution_id: &str,
    imm: &ExecImmutable,
    work_items: Vec<DispatchWorkItem>,
) {
    if work_items.is_empty() {
        return;
    }
    tracing::debug!("[axum] dispatching {} nodes for execution={}", work_items.len(), execution_id);
    let mut set = tokio::task::JoinSet::new();
    for item in work_items {
        let state = state.clone();
        let execution_id = execution_id.to_string();
        let is_infra_setup = imm.is_infra_setup;
        let is_trigger_setup = imm.is_trigger_setup;
        let test_mode = imm.test_mode;
        let user_id = imm.user_id.clone();
        let mocks = imm.mocks.clone();
        set.spawn(async move {
            dispatch_node_inmem(
                &state, &execution_id, &item.node, item.input, &item.pulse_id,
                &item.project_id, is_infra_setup, is_trigger_setup, test_mode, user_id.as_deref(),
                &mocks,
            ).await;
        });
    }
    while let Some(_) = set.join_next().await {}
}

use weft_core::{is_inside_mocked_group, sanitize_mock_output};

/// Dispatch a single node execution. Does NOT hold the per-execution mutex.
/// Instance lookup uses the shared cache on ExecutorState (DashMap, lock-free).
/// The actual HTTP call to the node service is fire-and-forget via tokio::spawn.
async fn dispatch_node_inmem(
    state: &SharedState,
    execution_id: &str,
    node: &weft_core::NodeDefinition,
    input: serde_json::Value,
    pulse_id: &str,
    project_id: &str,
    is_infra_setup: bool,
    is_trigger_setup: bool,
    test_mode: bool,
    user_id: Option<&str>,
    mocks: &std::collections::HashMap<String, serde_json::Value>,
) {
    let node_type_str = node.nodeType.to_string();
    let mut input = input;

    // Infrastructure node endpoint injection
    if node.features.isInfrastructure && !is_infra_setup {
        let urls = get_infra_endpoint_urls_via_restate(state, project_id).await;
        match urls.and_then(|u| u.get(&node.id).cloned()) {
            Some(url) => {
                if let Some(obj) = input.as_object_mut() {
                    obj.insert("_endpointUrl".to_string(), serde_json::json!(url));
                }
            }
            None => {
                tracing::error!("No endpointUrl for infra node {}", node.id);
                fire_node_failed(state, execution_id, &node.id, pulse_id, &format!("No endpointUrl for infra node {}. Start infrastructure first.", node.id)).await;
                return;
            }
        }
    }

    // Mock intercept: direct node mock (group mocking is handled in collect_dispatch_work)
    if test_mode && !node.features.isTrigger && !node.features.isInfrastructure {
        if let Some(mock_value) = mocks.get(&node.id) {
            let sanitized = sanitize_mock_output(mock_value, &node.outputs);
            tracing::info!("[axum] test mode: using mock output for node={} pulse={}", node.id, pulse_id);
            let completed = NodeCallbackRequest {
                executionId: execution_id.to_string(),
                nodeId: node.id.clone(),
                status: weft_core::NodeExecutionStatus::Completed,
                output: Some(sanitized),
                error: None,
                waitingMetadata: None,
                pulseId: pulse_id.to_string(),
                costUsd: 0.0,
            };
            if let Ok((dw, rt, imm)) = process_execution_callback(state, execution_id, completed).await {
                run_completion_side_effects(state, execution_id, dw, rt, &imm).await;
            }
            return;
        }
    }

    // Instance lookup. No cache: with N node-runner pods registered as distinct
    // instances, caching pins all dispatch to the cached one (no load balancing).
    // Restate handles read-only #[shared] lookups cheaply; cost not worth the
    // load-balancing regression.
    let instance = match find_instance_via_restate(state, &node_type_str).await {
        Some(inst) => inst,
        None => {
            let error_msg = format!("No node service available for type '{}'. The node service may not be running.", node_type_str);
            tracing::error!("[axum] {}", error_msg);
            // Fail the execution loudly instead of silently queueing
            let failed = NodeCallbackRequest {
                executionId: execution_id.to_string(),
                nodeId: node.id.clone(),
                status: weft_core::NodeExecutionStatus::Failed,
                output: None,
                error: Some(error_msg),
                waitingMetadata: None,
                pulseId: pulse_id.to_string(),
                costUsd: 0.0,
            };
            if let Ok((dw, rt, imm)) = process_execution_callback(state, execution_id, failed).await {
                run_completion_side_effects(state, execution_id, dw, rt, &imm).await;
            }
            return;
        }
    };

    let callback_url = format!(
        "{}/ProjectExecutor/{}/execution_callback",
        state.callback_base,
        execution_id
    );

    let http_req = NodeExecuteRequest {
        executionId: execution_id.to_string(),
        nodeId: node.id.clone(),
        nodeType: node.nodeType.to_string(),
        config: serde_json::to_value(&node.config).unwrap_or_else(|e| {
            tracing::error!("BUG: failed to serialize node config for {}: {}", node.id, e);
            serde_json::Value::Object(serde_json::Map::new())
        }),
        input: input.clone(),
        callbackUrl: callback_url,
        userId: user_id.map(|s| s.to_string()),
        projectId: Some(project_id.to_string()),
        outputs: node.outputs.clone(),
        features: node.features.clone(),
        isInfraSetup: is_infra_setup,
        isTriggerSetup: is_trigger_setup,
        pulseId: pulse_id.to_string(),
    };

    let endpoint = format!("{}/execute", instance.endpoint);
    let node_id = node.id.clone();
    let pulse_id = pulse_id.to_string();
    // Internal client: node-runner endpoints are operator-controlled
    // (registered via NodeInstanceRegistry, not user input). Future community
    // extension-runner: will need a separate path.
    let client = state.http_client.clone();

    // Dispatch via tokio::spawn: keeps the HTTP connection open until the node
    // finishes executing. If the node-runner crashes, the connection breaks and
    // the retry logic handles it. No more fire-and-forget callbacks.
    let state_clone = state.clone();
    let exec_id_clone = execution_id.to_string();
    tokio::spawn(async move {
        let max_retries = 5u32;
        let mut delay_secs = 1u64;

        for attempt in 0..=max_retries {
            // Bail out if the execution was cancelled. Without this check a
            // retry storm (e.g. HumanQuery holding the connection longer than
            // the forwarder's timeout) can keep hammering cloud-api for
            // minutes after the user clicked stop.
            if let Some(exec) = state_clone.executions.get(&exec_id_clone) {
                if exec.mt.lock().await.cancelled {
                    tracing::info!("[axum dispatch] node={} pulse={} aborting retry loop (execution cancelled)", node_id, pulse_id);
                    return;
                }
            }
            let result = client.post(&endpoint).json(&http_req).send().await;
            match result {
                Ok(response) if response.status().is_success() => {
                    // Node completed: parse the response body as the completion callback
                    let body_text = response.text().await.unwrap_or_default();
                    match serde_json::from_str::<NodeCallbackRequest>(&body_text) {
                        Ok(completed) => {
                            tracing::debug!("[axum dispatch] node={} pulse={} completed via response", node_id, pulse_id);
                            // Feed directly into the completion handler
                            let state_ref = &state_clone;
                            if let Ok((dw, rt, imm)) = process_execution_callback(state_ref, &exec_id_clone, completed).await {
                                run_completion_side_effects(state_ref, &exec_id_clone, dw, rt, &imm).await;
                            }
                        }
                        Err(e) => {
                            tracing::error!("[axum dispatch] node={} pulse={} failed to parse response: {}. Body (first 500 chars): {}", node_id, pulse_id, e, &body_text[..body_text.len().min(500)]);
                            let completed = NodeCallbackRequest::failed(&exec_id_clone, &node_id, &pulse_id, &format!("Failed to parse node response: {}", e));
                            if let Ok((dw, rt, imm)) = process_execution_callback(&state_clone, &exec_id_clone, completed).await {
                                run_completion_side_effects(&state_clone, &exec_id_clone, dw, rt, &imm).await;
                            }
                        }
                    }
                    return;
                }
                Ok(response) if response.status().as_u16() == 429 || response.status().as_u16() == 502 || response.status().as_u16() == 503 => {
                    if attempt < max_retries {
                        tracing::warn!("[axum dispatch] node={} pulse={} HTTP {} (attempt {}/{}), retrying...", node_id, pulse_id, response.status(), attempt + 1, max_retries);
                        tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
                        delay_secs = (delay_secs * 2).min(16);
                    } else {
                        let status = response.status();
                        tracing::error!("[axum dispatch] node={} pulse={} HTTP {} after {} retries, marking failed", node_id, pulse_id, status, max_retries);
                        let completed = NodeCallbackRequest::failed(&exec_id_clone, &node_id, &pulse_id, &format!("HTTP {} after retries", status));
                        if let Ok((dw, rt, imm)) = process_execution_callback(&state_clone, &exec_id_clone, completed).await {
                            run_completion_side_effects(&state_clone, &exec_id_clone, dw, rt, &imm).await;
                        }
                        return;
                    }
                }
                Ok(response) => {
                    let status = response.status();
                    tracing::error!("[axum dispatch] node={} pulse={} HTTP {} (non-retryable)", node_id, pulse_id, status);
                    let completed = NodeCallbackRequest::failed(&exec_id_clone, &node_id, &pulse_id, &format!("HTTP {}", status));
                    if let Ok((dw, rt, imm)) = process_execution_callback(&state_clone, &exec_id_clone, completed).await {
                        run_completion_side_effects(&state_clone, &exec_id_clone, dw, rt, &imm).await;
                    }
                    return;
                }
                Err(e) => {
                    if attempt < max_retries {
                        tracing::warn!("[axum dispatch] node={} pulse={} network error (attempt {}/{}): {}", node_id, pulse_id, attempt + 1, max_retries, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
                        delay_secs = (delay_secs * 2).min(16);
                    } else {
                        tracing::error!("[axum dispatch] node={} pulse={} network error after retries: {}", node_id, pulse_id, e);
                        let completed = NodeCallbackRequest::failed(&exec_id_clone, &node_id, &pulse_id, &format!("network error: {}", e));
                        if let Ok((dw, rt, imm)) = process_execution_callback(&state_clone, &exec_id_clone, completed).await {
                            run_completion_side_effects(&state_clone, &exec_id_clone, dw, rt, &imm).await;
                        }
                        return;
                    }
                }
            }
        }
    });
}

// =============================================================================
// HELPERS
// =============================================================================

async fn check_and_notify_inmem(
    state: &SharedState,
    execution_id: &str,
    imm: &ExecImmutable,
    mt: &ExecMutable,
) -> bool {
    let result = check_completion(&mt.pulses, &mt.node_executions);
    if result.is_none() {
        return false;
    }
    let any_failed = result.unwrap();
    tracing::info!("[axum] Project {} completed (any_failed={})", execution_id, any_failed);

    // If cancel fired while a dispatch response was still in-flight, a
    // stale completion callback can race behind cancel and flip the
    // dashboard's execution status back to completed/failed. Suppress it:
    // cancel already sent the authoritative `cancelled` status.
    if mt.cancelled {
        tracing::info!("[axum] Skipping completion callback for cancelled execution={}", execution_id);
        return true;
    }

    if let Some(ref callback_url) = imm.status_callback_url {
        let payload = build_completion_callback_payload(execution_id, &mt.node_executions, &mt.pulses, any_failed);
        if let Err(e) = post_status_callback(state.external_http_client.clone(), callback_url, &payload).await {
            tracing::error!("[axum] Completion callback failed for execution={}: {}", execution_id, e);
        }
    }

    // Schedule cleanup of completed execution after 60s
    let state_clone = state.clone();
    let exec_id = execution_id.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        state_clone.executions.remove(&exec_id);
        tracing::debug!("[axum] Cleaned up completed execution: {}", exec_id);
    });

    true
}

async fn fire_node_failed(state: &SharedState, execution_id: &str, node_id: &str, pulse_id: &str, error: &str) {
    let fail_req = NodeCallbackRequest::failed(execution_id, node_id, pulse_id, error);
    if let Ok((dw, rt, imm)) = process_execution_callback(state, execution_id, fail_req).await {
        run_completion_side_effects(state, execution_id, dw, rt, &imm).await;
    }
}

// =============================================================================
// RESTATE HTTP CALLS
// =============================================================================

async fn register_task_via_restate(state: &SharedState, task: PendingTask) {
    let url = format!("{}/TaskRegistry/global/register_task", state.restate_url);
    let cb_id = task.executionId.clone();
    // Retry with exponential backoff. Restate can be transiently overloaded
    // (rocksdb stalls, partition flushing) but recovers within seconds.
    // Without retries the user's pending task vanishes and they see no form.
    let delays = [
        std::time::Duration::from_millis(500),
        std::time::Duration::from_secs(2),
        std::time::Duration::from_secs(5),
    ];
    let mut last_err: String = "no attempt made".to_string();
    for (attempt, delay) in std::iter::once(std::time::Duration::ZERO).chain(delays).enumerate() {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let fut = state.http_client.post(&url).json(&task).send();
        match tokio::time::timeout(std::time::Duration::from_secs(10), fut).await {
            Ok(Ok(resp)) if resp.status().is_success() => {
                if attempt > 0 {
                    tracing::info!("[axum] register_task succeeded on retry {} (cb={})", attempt, cb_id);
                }
                return;
            }
            Ok(Ok(resp)) => last_err = format!("Restate returned {}", resp.status()),
            Ok(Err(e)) => last_err = format!("transport error: {}", e),
            Err(_) => last_err = "timeout after 10s".to_string(),
        }
        tracing::warn!("[axum] register_task attempt {} failed (cb={}): {}", attempt + 1, cb_id, last_err);
    }
    // All retries exhausted. The task is lost; the user's form will never appear.
    // Loud error so this surfaces in alerting.
    tracing::error!(
        "[axum] register_task FAILED after retries (cb={}): {}. Form will not appear for the user. \
         This usually means Restate is overloaded; check restate pod health and rocksdb stalls.",
        cb_id, last_err
    );
}

/// POST a status update (cancelled/completed/failed) to the dashboard's
/// `/api/executions/{id}` endpoint. The dashboard enforces JWT auth on every
/// `/api/*` route, but there is no user session on a server-initiated
/// callback, so we authenticate with `x-internal-api-key` instead and let the
/// dashboard's middleware recognize it as a trusted service call.
async fn post_status_callback(
    client: reqwest::Client,
    url: &str,
    payload: &serde_json::Value,
) -> Result<(), reqwest::Error> {
    let mut req = client.post(url).json(payload);
    if let Ok(key) = std::env::var("INTERNAL_API_KEY") {
        if !key.is_empty() {
            req = req.header("x-internal-api-key", key);
        }
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        tracing::error!("[axum] Status callback returned {}: {}", resp.status(), url);
    }
    Ok(())
}

async fn complete_task_via_restate(state: &SharedState, callback_id: &str) {
    let url = format!("{}/TaskRegistry/global/complete_task", state.restate_url);
    let fut = state.http_client.post(&url).json(&callback_id).send();
    match tokio::time::timeout(std::time::Duration::from_secs(10), fut).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::error!("[axum] Failed to complete task via Restate (cb={}): {}", callback_id, e),
        Err(_) => tracing::error!("[axum] Timed out completing task via Restate (cb={})", callback_id),
    }
}

/// Retries an idempotent registry lookup with exponential backoff.
/// Returns None only after all attempts fail (transient or "not found").
/// Distinguishing "transient failure" from "really not found" is hard from
/// this side; if Restate returns success-with-None we treat it as a real
/// "not found" and don't retry, otherwise we back off.
async fn lookup_instance_with_retry(state: &SharedState, url: &str, body: serde_json::Value) -> Option<NodeInstance> {
    let delays = [
        std::time::Duration::from_millis(200),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(3),
    ];
    let mut last_err = String::new();
    for (attempt, delay) in std::iter::once(std::time::Duration::ZERO).chain(delays).enumerate() {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let fut = state.http_client.post(url).json(&body).send();
        match tokio::time::timeout(std::time::Duration::from_secs(5), fut).await {
            Ok(Ok(resp)) if resp.status().is_success() => {
                // Treat successful empty body as a real "not found" (no retry).
                return resp.json::<Option<NodeInstance>>().await.ok().flatten();
            }
            Ok(Ok(resp)) => last_err = format!("Restate returned {}", resp.status()),
            Ok(Err(e)) => last_err = format!("transport: {}", e),
            Err(_) => last_err = "timeout after 5s".to_string(),
        }
        tracing::warn!("[axum] instance lookup attempt {} failed: {}", attempt + 1, last_err);
    }
    tracing::error!("[axum] instance lookup exhausted retries: {}", last_err);
    None
}

async fn find_instance_via_restate(state: &SharedState, node_type: &str) -> Option<NodeInstance> {
    let url = format!("{}/NodeInstanceRegistry/global/find_instance_for_node_type", state.restate_url);
    lookup_instance_with_retry(state, &url, serde_json::Value::String(node_type.to_string())).await
}

async fn find_instance_by_id_via_restate(state: &SharedState, instance_id: &str) -> Option<NodeInstance> {
    let url = format!("{}/NodeInstanceRegistry/global/find_instance_by_id", state.restate_url);
    lookup_instance_with_retry(state, &url, serde_json::Value::String(instance_id.to_string())).await
}


async fn get_infra_endpoint_urls_via_restate(state: &SharedState, project_id: &str) -> Option<std::collections::HashMap<String, String>> {
    let url = format!("{}/InfrastructureManager/{}/get_infra_endpoint_urls", state.restate_url, project_id);
    match state.http_client.post(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            #[derive(serde::Deserialize)]
            struct UrlsResponse {
                urls: std::collections::HashMap<String, String>,
            }
            resp.json::<UrlsResponse>().await.ok().map(|r| r.urls)
        }
        _ => None,
    }
}
