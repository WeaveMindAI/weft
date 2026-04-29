//! Browser extension API. Ported from v1's dashboard proxy; the v2
//! extension talks directly to this dispatcher. Token = an opaque
//! extension token (`wm_tk_*`) the user pasted into the browser.
//! Suspension completion routes through the same form-submission
//! pipeline used by /f/{token}.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use weft_core::primitive::DispatcherToWorker;

use crate::backend::WakeContext;
use crate::state::DispatcherState;

/// What complete_task should do after journaling the resolution.
/// The slot's current state determines the path: deliver on the
/// live WS, spawn a fresh worker, or rely on an in-flight spawn
/// to pick up the pending delivery from its handshake snapshot.
enum CompleteAction {
    DeliverLive(mpsc::Sender<DispatcherToWorker>),
    Spawn,
    None,
}

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
            // The metadata we journal is `{ spec: WakeSignalSpec,
            // node_id, lane }`. Form schema + description live
            // under `spec.kind`. WakeSignalKind::Form serializes
            // to `{ "kind": "form", "form_type": ..., "schema": ...,
            // "description": ... }`. Action variants will follow
            // the same shape under different `kind` discriminants.
            let kind = s
                .metadata
                .pointer("/spec/kind/kind")
                .and_then(|v| v.as_str())
                .unwrap_or("form")
                .to_string();
            let schema = s.metadata.pointer("/spec/kind/schema").cloned();
            let description = s
                .metadata
                .pointer("/spec/kind/description")
                .and_then(|v| v.as_str())
                .map(|x| x.to_string());
            let action_url = s
                .metadata
                .pointer("/spec/kind/action_url")
                .and_then(|v| v.as_str())
                .map(|x| x.to_string());

            let (task_type, title, form_schema, action_url) = match kind.as_str() {
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
                    action_url,
                ),
                _ => ("Task", format!("Input for {}", s.node), schema, None),
            };
            PendingTaskOut {
                execution_id: s.color.to_string(),
                node_id: s.node,
                title,
                description,
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

    // Resolve the project from the journal's execution-started
    // event for this color. Keeps the suspension metadata lean.
    let project_id_str = state
        .journal
        .execution_project(color)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "execution not journaled".into()))?;
    let project_id = project_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad project id".into()))?;
    if state.projects.get(project_id).await.is_none() {
        return Err((StatusCode::GONE, "project no longer registered".into()));
    }

    state
        .journal
        .consume_suspension(&suspension.token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("consume: {e}")))?;

    // Journal the fire. The worker's next fold will seed this
    // delivery into its link via `pending_deliveries`. We clone
    // the body so the same value is available for direct WS
    // delivery to a Live worker (see `CompleteAction::DeliverLive`
    // below) without a journal round-trip.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let resolve_value = body.clone();
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::SuspensionResolved {
            color,
            token: suspension.token.clone(),
            value: body,
            at_unix: now,
        })
        .await;

    // The slot can be in any of these states when a fire arrives:
    //
    //   * `Live`: a worker is parked in `await_signal`. We send
    //     `Deliver` over the WS so its oneshot resolves immediately.
    //     Without this, the worker waits forever even though the
    //     submission has been journaled.
    //
    //   * `StalledGrace`: a worker stalled but hasn't exited yet.
    //     Same Deliver send wakes it via the grace handler, which
    //     promotes the slot back to Live.
    //
    //   * `Idle`: no worker. Queue a Resume start and spawn a fresh
    //     worker; its handshake snapshot will surface the new
    //     `pending_deliveries` entry and seed the delivery into
    //     the link.
    //
    //   * `Starting` / `WaitingReconnect`: a spawn is already in
    //     flight; the new worker's snapshot will pick up the
    //     pending delivery. Nothing extra to do.
    //
    // The atomic block below classifies the slot and either grabs
    // the WS sender (Live / StalledGrace) or flips Idle->Starting.
    let action = {
        state
            .slots
            .with_slot(color, move |slot| {
                Box::pin(async move {
                    use crate::slots::Slot;
                    match slot {
                        Slot::Live { sender, .. } => {
                            CompleteAction::DeliverLive(sender.clone())
                        }
                        Slot::StalledGrace { sender, .. } => {
                            CompleteAction::DeliverLive(sender.clone())
                        }
                        Slot::Idle { .. } => {
                            let mut q = match std::mem::replace(
                                slot,
                                Slot::Idle {
                                    queued: std::collections::VecDeque::new(),
                                },
                            ) {
                                Slot::Idle { queued } => queued,
                                _ => unreachable!(),
                            };
                            q.push_front(crate::slots::QueuedWake::Start(
                                weft_core::primitive::WakeMessage::Resume,
                            ));
                            *slot = Slot::Starting { queued: q, worker: None };
                            CompleteAction::Spawn
                        }
                        _ => CompleteAction::None,
                    }
                })
            })
            .await
    };

    match action {
        CompleteAction::DeliverLive(sender) => {
            let _ = sender
                .send(weft_core::primitive::DispatcherToWorker::Deliver(
                    weft_core::primitive::Delivery {
                        token: suspension.token.clone(),
                        value: resolve_value,
                    },
                ))
                .await;
        }
        CompleteAction::Spawn => {
            let wake = WakeContext::resolve(&state, project_id_str.to_string(), color);
            let worker = state
                .workers
                .spawn_worker(wake)
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
        CompleteAction::None => {
            // Spawn already in flight (Starting / WaitingReconnect).
            // The new worker will see the pending_deliveries entry
            // in its handshake snapshot and seed it itself.
        }
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

/// Hard-cancel the execution this task belongs to. Differs from
/// `dismiss_action` (which only consumes the suspension and leaves
/// the worker waiting forever) and from the form complete path
/// (which delivers a value and resumes). The semantics here are
/// "the user no longer wants this run to finish": we tell the
/// worker to stop via `DispatcherToWorker::Cancel`, drop all open
/// suspensions for this color, and emit `ExecutionFailed` on the
/// project's SSE bus.
pub async fn cancel_task(
    State(state): State<DispatcherState>,
    Path((token, execution_id)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    require_token(&state, &token)
        .await
        .map_err(|c| (c, "invalid token".into()))?;
    let color = execution_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad execution id".into()))?;
    crate::api::execution::cancel_color(&state, color)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e}")))?;
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
    /// Token shape:
    ///   - "friendly" (default): `wm_tk_<adj>-<noun>-<NN>`. Easy
    ///     to read, low entropy. Fine on localhost where CORS
    ///     blocks cross-origin probing.
    ///   - "hard": `wm_tk_<32-hex>`. High entropy, ugly. Use
    ///     when exposing the dispatcher beyond localhost.
    /// Both share the `wm_tk_` prefix so a token always reads
    /// as a Weavemind token at a glance.
    #[serde(default)]
    pub style: Option<String>,
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
    // Pick the token shape. Default = friendly (`wm_tk_<adj>-
    // <noun>-<NN>`); explicit "hard" gives a uuid-backed body
    // for setups exposed beyond localhost.
    let token = match body.style.as_deref() {
        Some("hard") => crate::api::extension_names::hard_token(),
        _ => crate::api::extension_names::friendly_token(),
    };

    // Optional human label, separate from the token itself. If
    // the caller didn't supply one, mirror the token suffix
    // (without the wm_tk_ prefix) so `weft token ls` still
    // shows something readable instead of an empty column.
    let name = body
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| token.strip_prefix("wm_tk_").unwrap_or(&token).to_string());

    state
        .journal
        .mint_ext_token(&token, Some(&name), body.metadata)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    Ok(Json(MintedToken { token, name: Some(name) }))
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
    Path(identifier): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let removed = state
        .journal
        .revoke_ext_token(&identifier)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("no token matching '{identifier}'")))
    }
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
    let s = schema.as_ref()?.get("title")?.as_str()?;
    if s.trim().is_empty() {
        return None;
    }
    Some(s.to_string())
}

fn format_unix(at: u64) -> String {
    chrono::DateTime::from_timestamp(at as i64, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_else(|| at.to_string())
}
