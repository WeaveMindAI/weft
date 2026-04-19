use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::extension_tokens;
use crate::state::AppState;

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct PendingTaskView {
    pub executionId: String,
    pub nodeId: String,
    pub title: String,
    pub description: Option<String>,
    pub data: Option<serde_json::Value>,
    pub createdAt: String,
    pub taskType: Option<String>,
    pub actionUrl: Option<String>,
    pub formSchema: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct TaskListResponse {
    pub tasks: Vec<PendingTaskView>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
pub struct CompleteTaskRequest {
    pub nodeId: String,
    pub input: serde_json::Value,
    /// Full callback ID from the PendingTask (e.g. "{uuid}-{nodeId}" or "{uuid}-{nodeId}-{lane}").
    /// Used to extract the lane index for Human nodes inside ForEach.
    #[serde(default)]
    pub callbackId: Option<String>,
}

/// List pending tasks for the user associated with this token
pub async fn list_tasks(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    let pool = &state.db_pool;

    // Validate token and get user_id
    let user_id = match extension_tokens::validate_token(pool, &token).await {
        Some(uid) => uid,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            ).into_response();
        }
    };

    tracing::debug!("Listing tasks for user {} via extension token", user_id);

    // Fetch tasks from Restate's TaskRegistry
    let restate_url = format!("{}/TaskRegistry/global/list_tasks", state.restate_url);

    let client = reqwest::Client::new();
    match client.get(&restate_url).send().await {
        Ok(response) if response.status().is_success() => {
            match response.json::<serde_json::Value>().await {
                Ok(data) => {
                    // Filter tasks by user_id
                    let all_tasks = data.get("tasks").and_then(|t| t.as_array()).cloned().unwrap_or_default();
                    
                    let filtered_tasks: Vec<PendingTaskView> = all_tasks
                        .into_iter()
                        .filter(|task| {
                            // Only show tasks that belong to this user
                            task.get("userId")
                                .and_then(|u| u.as_str())
                                .map(|task_user_id| task_user_id == user_id)
                                .unwrap_or(false)
                        })
                        .map(|task| PendingTaskView {
                            executionId: task.get("executionId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            nodeId: task.get("nodeId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            title: task.get("title").and_then(|v| v.as_str()).unwrap_or("Task").to_string(),
                            description: task.get("description").and_then(|v| v.as_str()).map(String::from),
                            data: task.get("data").cloned().filter(|v| !v.is_null()),
                            createdAt: task.get("createdAt").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            taskType: task.get("taskType").and_then(|v| v.as_str()).map(String::from),
                            actionUrl: task.get("actionUrl").and_then(|v| v.as_str()).map(String::from),
                            formSchema: task.get("formSchema").cloned(),
                            metadata: task.get("metadata").cloned().unwrap_or(serde_json::Value::Object(serde_json::Map::new())),
                        })
                        .collect();

                    tracing::debug!("Found {} tasks for user {}", filtered_tasks.len(), user_id);
                    Json(TaskListResponse { tasks: filtered_tasks }).into_response()
                }
                Err(e) => {
                    tracing::error!("Failed to parse Restate response: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": "Failed to parse tasks" })),
                    ).into_response()
                }
            }
        }
        Ok(response) => {
            let status = response.status();
            tracing::error!("Restate returned error: {}", status);
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": format!("Upstream error: {}", status) })),
            ).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to reach Restate: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "Failed to reach task service" })),
            ).into_response()
        }
    }
}

/// Complete a task (approve or reject)
pub async fn complete_task(
    State(state): State<Arc<AppState>>,
    Path((token, execution_id)): Path<(String, String)>,
    Json(req): Json<CompleteTaskRequest>,
) -> impl IntoResponse {
    let pool = &state.db_pool;

    // Validate token and get user_id
    let user_id = match extension_tokens::validate_token(pool, &token).await {
        Some(uid) => uid,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            ).into_response();
        }
    };

    tracing::info!(
        "Completing task for execution {} by user {} via extension",
        execution_id, user_id
    );

    // execution_id (path param) is the bare project UUID.
    // callbackId (body) is the full callback ID: "{executionId}-{nodeId}-{pulseId}-{seq}".
    // Format: {uuid (36 chars)}-{nodeId}-{uuid (36 chars)}-{seq number}
    // We need to extract the pulseId (second UUID) and strip the sequence suffix.
    let pulse_id: String = req.callbackId.as_deref().map(|cb| {
        // UUID is 5 dash-separated segments (36 chars with 4 dashes).
        let uuid_end = cb.splitn(6, '-').take(5).map(|s| s.len()).sum::<usize>() + 4;
        if uuid_end >= cb.len() { return String::new(); }
        let remainder = &cb[uuid_end + 1..]; // "{nodeId}-{pulseId}-{seq}"
        // The pulseId is a UUID (36 chars). Strip the trailing "-{seq}" first,
        // then take the last 36 chars as the pulse UUID.
        // Find the last '-': everything after it is the sequence number.
        if let Some(last_dash) = remainder.rfind('-') {
            let without_seq = &remainder[..last_dash]; // "{nodeId}-{pulseId}"
            if without_seq.len() >= 36 {
                without_seq[without_seq.len() - 36..].to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        }
    }).unwrap_or_default();

    // Forward to Axum executor's provide_input
    let executor_url = format!(
        "{}/ProjectExecutor/{}/provide_input",
        state.executor_url, execution_id
    );

    let payload = serde_json::json!({
        "nodeId": req.nodeId,
        "input": req.input,
        "pulseId": pulse_id,
    });

    let client = reqwest::Client::new();
    match client.post(&executor_url).json(&payload).send().await {
        Ok(response) if response.status().is_success() => {
            tracing::info!("Task completed successfully");
            Json(serde_json::json!({ "status": "completed" })).into_response()
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Restate returned error: {} - {}", status, body);
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": format!("Upstream error: {}", status) })),
            ).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to reach Restate: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "Failed to reach project service" })),
            ).into_response()
        }
    }
}

/// Cancel a task (skip downstream execution, remove from TaskRegistry)
pub async fn cancel_task(
    State(state): State<Arc<AppState>>,
    Path((token, execution_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let pool = &state.db_pool;

    let user_id = match extension_tokens::validate_token(pool, &token).await {
        Some(uid) => uid,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            ).into_response();
        }
    };

    tracing::info!(
        "Cancelling task {} by user {} via extension",
        execution_id, user_id
    );

    // execution_id here is the full callback ID: "{projectUuid}-{nodeId}-{pulseId}"
    // Parse out the project UUID (first 36 chars), nodeId, and pulseId (last 36 chars).
    let callback_id = &execution_id;
    let client = reqwest::Client::new();

    // Parse callback ID: "{projectUuid}-{nodeId}-{pulseUuid}-{seq}"
    let parsed = (|| {
        let uuid_end = callback_id.splitn(6, '-').take(5).map(|s| s.len()).sum::<usize>() + 4;
        if uuid_end >= callback_id.len() { return None; }
        let project_id = &callback_id[..uuid_end];
        let remainder = &callback_id[uuid_end + 1..]; // "{nodeId}-{pulseUuid}-{seq}"
        // Strip trailing "-{seq}" (sequence number)
        let last_dash = remainder.rfind('-')?;
        let without_seq = &remainder[..last_dash]; // "{nodeId}-{pulseUuid}"
        if without_seq.len() < 37 { return None; } // at least 36 (uuid) + 1 (dash)
        let pulse_id = &without_seq[without_seq.len() - 36..];
        let node_id = &without_seq[..without_seq.len() - 37]; // strip "-{pulseUuid}"
        Some((project_id.to_string(), node_id.to_string(), pulse_id.to_string()))
    })();

    // If the callback ID can't be parsed (old/malformed task), just remove from TaskRegistry
    let Some((project_id, node_id, pulse_id)) = parsed else {
        tracing::info!("Unparseable callback ID '{}', removing stale task from TaskRegistry", callback_id);
        let restate_url = format!(
            "{}/TaskRegistry/global/complete_task",
            state.restate_url
        );
        let _ = client.post(&restate_url).json(&callback_id).send().await;
        return Json(serde_json::json!({ "status": "removed" })).into_response();
    };

    // Call provide_input with skip=true
    let executor_url = format!(
        "{}/ProjectExecutor/{}/provide_input",
        state.executor_url, project_id
    );

    let payload = serde_json::json!({
        "nodeId": node_id,
        "input": serde_json::Value::Null,
        "pulseId": pulse_id,
        "skip": true,
    });

    match client.post(&executor_url).json(&payload).send().await {
        Ok(response) if response.status().is_success() => {
            tracing::info!("Task cancelled (skipped) successfully");
            Json(serde_json::json!({ "status": "cancelled" })).into_response()
        }
        Ok(response) if response.status() == StatusCode::NOT_FOUND => {
            // Execution no longer exists, just remove from TaskRegistry
            tracing::info!("Execution gone, removing stale task from TaskRegistry");
            let restate_url = format!(
                "{}/TaskRegistry/global/complete_task",
                state.restate_url
            );
            let _ = client.post(&restate_url).json(&callback_id).send().await;
            Json(serde_json::json!({ "status": "removed" })).into_response()
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Cancel failed: {} - {}", status, body);
            // Fallback: remove from TaskRegistry anyway so user isn't stuck
            let restate_url = format!(
                "{}/TaskRegistry/global/complete_task",
                state.restate_url
            );
            let _ = client.post(&restate_url).json(&callback_id).send().await;
            Json(serde_json::json!({ "status": "removed" })).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to reach orchestrator: {}", e);
            // Fallback: remove from TaskRegistry anyway
            let restate_url = format!(
                "{}/TaskRegistry/global/complete_task",
                state.restate_url
            );
            let _ = client.post(&restate_url).json(&callback_id).send().await;
            Json(serde_json::json!({ "status": "removed" })).into_response()
        }
    }
}

/// Dismiss an action (just removes from TaskRegistry, no project interaction)
pub async fn dismiss_action(
    State(state): State<Arc<AppState>>,
    Path((token, action_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let pool = &state.db_pool;

    // Validate token and get user_id
    let user_id = match extension_tokens::validate_token(pool, &token).await {
        Some(uid) => uid,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            ).into_response();
        }
    };

    tracing::info!(
        "Dismissing action {} by user {} via extension",
        action_id, user_id
    );

    // Call TaskRegistry to remove the action
    let restate_url = format!(
        "{}/TaskRegistry/global/complete_task",
        state.restate_url
    );

    let client = reqwest::Client::new();
    match client.post(&restate_url).json(&action_id).send().await {
        Ok(response) if response.status().is_success() => {
            tracing::info!("Action dismissed successfully");
            Json(serde_json::json!({ "status": "dismissed" })).into_response()
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Restate returned error: {} - {}", status, body);
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": format!("Upstream error: {}", status) })),
            ).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to reach Restate: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "Failed to reach task service" })),
            ).into_response()
        }
    }
}

/// Submit a trigger form (fires the trigger with form data)
pub async fn submit_trigger(
    State(state): State<Arc<AppState>>,
    Path((token, trigger_task_id)): Path<(String, String)>,
    Json(req): Json<CompleteTaskRequest>,
) -> impl IntoResponse {
    let pool = &state.db_pool;

    let user_id = match extension_tokens::validate_token(pool, &token).await {
        Some(uid) => uid,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            ).into_response();
        }
    };

    tracing::info!(
        "Trigger form submitted for {} by user {} via extension",
        trigger_task_id, user_id
    );

    // trigger_task_id is "trigger-{triggerId}", extract the triggerId
    let trigger_id = trigger_task_id.strip_prefix("trigger-").unwrap_or(&trigger_task_id);

    // Route submission to the TriggerService's form submission sender
    let senders = state.trigger_service.lock().await.form_submission_senders.clone();
    let senders_read = senders.read().await;

    if let Some(sender) = senders_read.get(trigger_id) {
        let submission = weft_nodes::FormSubmission {
            data: req.input,
        };
        if sender.send(submission).is_ok() {
            Json(serde_json::json!({ "status": "submitted" })).into_response()
        } else {
            (
                StatusCode::GONE,
                Json(serde_json::json!({ "error": "Trigger is no longer running" })),
            ).into_response()
        }
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Trigger not found" })),
        ).into_response()
    }
}

/// Validate extension token and return the owner's user_id.
/// Used by cloud-api to authenticate form assistant requests.
pub async fn validate_token_handler(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    let pool = &state.db_pool;

    match extension_tokens::validate_token(pool, &token).await {
        Some(user_id) => {
            Json(serde_json::json!({ "userId": user_id })).into_response()
        }
        None => {
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            ).into_response()
        }
    }
}

/// Remove every TaskRegistry entry owned by this user whose callback_id
/// starts with `{execution_id}-`. Used by the extension "Clear for this run"
/// button to flush orphan tasks left behind by prior duplication bugs or
/// by executions that were cancelled before the TaskRegistry could be
/// cleaned up. Does NOT try to resume or skip the node; the execution is
/// assumed dead.
pub async fn cleanup_tasks_for_execution(
    State(state): State<Arc<AppState>>,
    Path((token, execution_id)): Path<(String, String)>,
) -> impl IntoResponse {
    // Reject anything that isn't a real UUID so the prefix match doesn't
    // degenerate into an over-broad filter (e.g. execution_id="a" would
    // match every callback_id starting with "a-" within the user's scope).
    let parsed = match uuid::Uuid::parse_str(&execution_id) {
        Ok(u) => u,
        Err(_) => return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "execution_id must be a UUID" })),
        ).into_response(),
    };
    // Normalize to hyphenated lowercase so the prefix match compares against
    // the same canonical form we use elsewhere (Uuid::Display and callback_id
    // construction both produce lowercase).
    cleanup_tasks_inner(state, token, Some(parsed.as_hyphenated().to_string())).await
}

/// Remove every TaskRegistry entry owned by this user. Used by the
/// extension "Clear all" button.
pub async fn cleanup_all_tasks(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    cleanup_tasks_inner(state, token, None).await
}

async fn cleanup_tasks_inner(
    state: Arc<AppState>,
    token: String,
    execution_id_filter: Option<String>,
) -> axum::response::Response {
    let pool = &state.db_pool;

    let user_id = match extension_tokens::validate_token(pool, &token).await {
        Some(uid) => uid,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            ).into_response();
        }
    };

    let client = reqwest::Client::new();
    let list_url = format!("{}/TaskRegistry/global/list_tasks", state.restate_url);
    let complete_url = format!("{}/TaskRegistry/global/complete_task", state.restate_url);

    // Pull the full task list, filter by userId (and optionally by executionId
    // prefix on the callback_id), then fire concurrent complete_task calls.
    // callback_id format is "{executionId}-{nodeId}-{pulseId}-{seq}", so
    // prefix-matching is how we scope to a single execution.
    let tasks_json = match client.get(&list_url).send().await {
        Ok(r) if r.status().is_success() => match r.json::<serde_json::Value>().await {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("cleanup: failed to parse list_tasks response: {}", e);
                return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "upstream parse error" }))).into_response();
            }
        },
        Ok(r) => {
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": format!("upstream {}", r.status()) }))).into_response();
        }
        Err(e) => {
            tracing::error!("cleanup: failed to reach TaskRegistry: {}", e);
            return (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": "failed to reach task service" }))).into_response();
        }
    };

    let all_tasks = tasks_json.get("tasks").and_then(|t| t.as_array()).cloned().unwrap_or_default();

    // The TaskRegistry stores the callback_id in the `executionId` field of
    // each PendingTask (see executor_axum.rs where PendingTask.executionId is
    // set to callback_id for WaitingForInput tasks). That's what complete_task
    // consumes, so we collect those.
    // Build the per-execution prefix once and compare case-insensitively:
    // UUIDs SHOULD be lowercase on both sides (see cleanup_tasks_for_execution),
    // but a historical task with uppercase storage should still match.
    let exec_prefix_lower = execution_id_filter.as_ref().map(|e| format!("{}-", e.to_ascii_lowercase()));
    let callback_ids: Vec<String> = all_tasks.into_iter()
        .filter(|t| {
            t.get("userId").and_then(|u| u.as_str()) == Some(user_id.as_str())
        })
        .filter_map(|t| t.get("executionId").and_then(|v| v.as_str()).map(String::from))
        .filter(|cb| match &exec_prefix_lower {
            Some(prefix) => cb.len() >= prefix.len() && cb[..prefix.len()].eq_ignore_ascii_case(prefix),
            None => true,
        })
        .collect();

    let count = callback_ids.len();
    tracing::info!("cleanup: removing {} tasks for user {} (execution filter: {:?})", count, user_id, execution_id_filter);

    // Bounded concurrency so a user with hundreds of orphan tasks can't OOM
    // Restate by firing every complete_task at once (each invocation costs
    // journal state on Restate's side).
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(20));
    let mut set = tokio::task::JoinSet::new();
    for cb_id in callback_ids {
        let client = client.clone();
        let url = complete_url.clone();
        let permit = semaphore.clone();
        set.spawn(async move {
            let _permit = permit.acquire_owned().await.ok();
            let fut = client.post(&url).json(&cb_id).send();
            match tokio::time::timeout(std::time::Duration::from_secs(10), fut).await {
                Ok(Ok(_)) => true,
                Ok(Err(e)) => { tracing::warn!("cleanup: complete_task failed for {}: {}", cb_id, e); false }
                Err(_) => { tracing::warn!("cleanup: complete_task timed out for {}", cb_id); false }
            }
        });
    }

    let mut succeeded = 0usize;
    while let Some(res) = set.join_next().await {
        if matches!(res, Ok(true)) { succeeded += 1; }
    }

    Json(serde_json::json!({
        "removed": succeeded,
        "attempted": count,
    })).into_response()
}

/// Health check for extension - validates token is valid
pub async fn health_check(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    tracing::debug!("Extension health check for token: {}", token);
    
    let pool = &state.db_pool;

    match extension_tokens::validate_token(pool, &token).await {
        Some(user_id) => {
            tracing::info!("Extension health check OK for token {} (user: {})", token, user_id);
            Json(serde_json::json!({ "status": "ok" })).into_response()
        }
        None => {
            tracing::warn!("Extension health check FAILED - invalid token: {}", token);
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            ).into_response()
        }
    }
}
