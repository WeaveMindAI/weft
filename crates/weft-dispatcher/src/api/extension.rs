//! Browser extension API. Ported from v1's dashboard proxy; the v2
//! extension talks directly to this dispatcher. Token = an opaque
//! extension token (`wm_ext_*`) the user pasted into the browser.
//! Suspension completion routes through the same form-submission
//! pipeline used by /f/{token}.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use crate::backend::WakeContext;
use crate::state::DispatcherState;

#[derive(Debug, Serialize)]
pub struct PendingTaskOut {
    #[serde(rename = "executionId")]
    pub execution_id: String,
    #[serde(rename = "nodeId")]
    pub node_id: String,
    pub title: String,
    pub description: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "taskType")]
    pub task_type: String,
    #[serde(rename = "formSchema", skip_serializing_if = "Option::is_none")]
    pub form_schema: Option<Value>,
    #[serde(rename = "actionUrl", skip_serializing_if = "Option::is_none")]
    pub action_url: Option<String>,
    #[serde(rename = "tokenHint")]
    pub token_hint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

pub async fn list_tasks(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
) -> Result<Json<Vec<PendingTaskOut>>, StatusCode> {
    require_token(&state, &token).await?;

    let open = state
        .journal
        .list_open_suspensions()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let tasks = open
        .into_iter()
        .map(|s| {
            let kind = s
                .metadata
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("form");
            let schema = s.metadata.get("schema").cloned();
            let (task_type, title, form_schema, action_url) = match kind {
                "form" => (
                    "Task",
                    schema_title(&schema).unwrap_or_else(|| format!("Input for {}", s.node)),
                    schema,
                    None,
                ),
                "action" => (
                    "Action",
                    schema_title(&schema).unwrap_or_else(|| format!("Action: {}", s.node)),
                    None,
                    s.metadata
                        .get("action_url")
                        .and_then(|v| v.as_str())
                        .map(|x| x.to_string()),
                ),
                _ => ("Task", format!("Input for {}", s.node), schema, None),
            };
            PendingTaskOut {
                execution_id: s.color.to_string(),
                node_id: s.node,
                title,
                description: s
                    .metadata
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(|x| x.to_string()),
                created_at: format_unix(s.created_at),
                task_type: task_type.to_string(),
                form_schema,
                action_url,
                token_hint: s.token,
                metadata: Some(s.metadata),
            }
        })
        .collect();

    Ok(Json(tasks))
}

pub async fn complete_task(
    State(state): State<DispatcherState>,
    Path((token, execution_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, (StatusCode, String)> {
    require_token(&state, &token)
        .await
        .map_err(|c| (c, "invalid token".into()))?;

    let color = execution_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad execution id".into()))?;

    // Find the open suspension for this execution. The extension
    // passes executionId = color; we look up the live suspension.
    let suspensions = state
        .journal
        .list_open_suspensions()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let suspension = suspensions
        .into_iter()
        .find(|s| s.color == color)
        .ok_or((StatusCode::NOT_FOUND, "no open suspension for execution".into()))?;

    let project_id_str = suspension
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

    state
        .journal
        .consume_suspension(&suspension.token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("consume: {e}")))?;

    // Journal the fire. The worker's next fold will seed this
    // delivery into its link via `pending_deliveries`.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::SuspensionResolved {
            color,
            token: suspension.token.clone(),
            value: body,
            at_unix: now,
        })
        .await;

    // Ensure a worker is alive for this color. Atomic via the slot
    // mutex: only the first concurrent POST that sees `Idle` spawns.
    let must_spawn = state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if matches!(slot, crate::slots::Slot::Idle { .. }) {
                    let mut q = match std::mem::replace(
                        slot,
                        crate::slots::Slot::Idle {
                            queued: std::collections::VecDeque::new(),
                        },
                    ) {
                        crate::slots::Slot::Idle { queued } => queued,
                        _ => unreachable!(),
                    };
                    q.push_front(crate::slots::QueuedWake::Start(
                        weft_core::primitive::WakeMessage::Resume,
                    ));
                    *slot = crate::slots::Slot::Starting { queued: q, worker: None };
                    true
                } else {
                    false
                }
            })
        })
        .await;

    if must_spawn {
        let wake = WakeContext { project_id: project_id_str.to_string(), color };
        let worker = state
            .workers
            .spawn_worker(&summary.binary_path, wake)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::WorkerSpawned { color, at_unix: now })
            .await;
        state
            .slots
            .with_slot(color, move |slot| {
                Box::pin(async move {
                    if let crate::slots::Slot::Starting { worker: w, .. } = slot {
                        *w = Some(worker);
                    }
                })
            })
            .await;
    }

    state
        .events
        .publish(crate::events::DispatcherEvent::ExecutionResumed {
            color,
            node: suspension.node,
            project_id: project_id_str.to_string(),
        })
        .await;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn submit_trigger(
    State(state): State<DispatcherState>,
    Path((token, trigger_task_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, (StatusCode, String)> {
    // Trigger-style tasks are handled identically to form tasks in
    // v2: the extension POSTs the form payload, we resume the
    // suspended execution. The `trigger_task_id` field maps to the
    // same "execution id = color" used by complete_task.
    complete_task(State(state), Path((token, trigger_task_id)), Json(body)).await
}

pub async fn dismiss_action(
    State(state): State<DispatcherState>,
    Path((token, action_id)): Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    require_token(&state, &token).await?;
    let color = action_id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    // For actions, "dismiss" means: remove the pending suspension
    // without resuming the execution (it just stays unresolved, the
    // user made clear they don't care). For v2 we simply consume the
    // suspension's token to drop it from the queue.
    let suspensions = state
        .journal
        .list_open_suspensions()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(s) = suspensions.into_iter().find(|s| s.color == color) {
        let _ = state.journal.consume_suspension(&s.token).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn health(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    require_token(&state, &token).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn cleanup_all(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
) -> Result<StatusCode, StatusCode> {
    require_token(&state, &token).await?;
    // Phase A: no-op; the extension's "cleanup" endpoint in v1 was
    // for clearing stale toast notifications. The dispatcher has no
    // toast queue. Return 204 so the extension is happy.
    Ok(StatusCode::NO_CONTENT)
}

pub async fn cleanup_execution(
    State(state): State<DispatcherState>,
    Path((token, _execution_id)): Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    require_token(&state, &token).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, serde::Deserialize)]
pub struct MintTokenBody {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct MintedToken {
    pub token: String,
    pub name: Option<String>,
}

pub async fn mint_token(
    State(state): State<DispatcherState>,
    Json(body): Json<MintTokenBody>,
) -> Result<Json<MintedToken>, (StatusCode, String)> {
    let token = state
        .journal
        .mint_ext_token(body.name.as_deref(), body.metadata)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    Ok(Json(MintedToken { token, name: body.name }))
}

pub async fn list_tokens(
    State(state): State<DispatcherState>,
) -> Result<Json<Vec<MintedToken>>, (StatusCode, String)> {
    let tokens = state
        .journal
        .list_ext_tokens()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    Ok(Json(
        tokens
            .into_iter()
            .map(|t| MintedToken { token: t.token, name: t.name })
            .collect(),
    ))
}

pub async fn revoke_token(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    state
        .journal
        .revoke_ext_token(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn require_token(state: &DispatcherState, token: &str) -> Result<(), StatusCode> {
    let exists = state
        .journal
        .ext_token_exists(token)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if exists {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn schema_title(schema: &Option<Value>) -> Option<String> {
    schema
        .as_ref()?
        .get("title")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn format_unix(at: u64) -> String {
    chrono::DateTime::from_timestamp(at as i64, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_else(|| at.to_string())
}
