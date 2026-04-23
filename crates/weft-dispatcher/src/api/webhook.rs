//! Webhook entry. `POST /w/{token}` or `POST /w/{token}/{*path}`:
//! resolve the entry token, compute the firing-trigger subgraph,
//! mint a new color, spawn a worker seeded with the computed roots.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use crate::backend::WakeContext;
use crate::state::DispatcherState;

#[derive(Debug, Serialize)]
pub struct WebhookResponse {
    pub color: String,
}

pub async fn handle_root(
    state: State<DispatcherState>,
    Path(token): Path<String>,
    body: Option<Json<Value>>,
) -> Result<(StatusCode, Json<WebhookResponse>), (StatusCode, String)> {
    handle_inner(state, token, body).await
}

pub async fn handle(
    state: State<DispatcherState>,
    Path((token, _path)): Path<(String, String)>,
    body: Option<Json<Value>>,
) -> Result<(StatusCode, Json<WebhookResponse>), (StatusCode, String)> {
    handle_inner(state, token, body).await
}

async fn handle_inner(
    State(state): State<DispatcherState>,
    token: String,
    body: Option<Json<Value>>,
) -> Result<(StatusCode, Json<WebhookResponse>), (StatusCode, String)> {
    let entry = state
        .journal
        .resolve_entry_token(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown token".into()))?;

    let project_uuid = entry
        .project_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad project id".into()))?;
    let summary = state
        .projects
        .get(project_uuid)
        .await
        .ok_or((StatusCode::GONE, "project no longer registered".into()))?;
    let project = state
        .projects
        .project(project_uuid)
        .await
        .ok_or((StatusCode::GONE, "project definition missing".into()))?;

    // Compute seeds for the firing trigger. The payload is wrapped
    // as `{ body: <posted JSON> }` to match what ApiPost's input
    // bag expects on the firing node's __seed__.
    let payload = serde_json::json!({ "body": body.map(|Json(v)| v).unwrap_or(Value::Null) });
    let seeds = crate::api::project::compute_trigger_seeds(&project, &entry.node_id, &payload);
    if seeds.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "trigger '{}' has no output nodes reachable downstream; nothing to run",
                entry.node_id
            ),
        ));
    }

    let color = uuid::Uuid::new_v4();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    state
        .journal
        .record_event(&crate::journal::ExecEvent::ExecutionStarted {
            color,
            project_id: entry.project_id.clone(),
            entry_node: entry.node_id.clone(),
            at_unix: now,
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    for seed in &seeds {
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::PulseSeeded {
                color,
                node_id: seed.node_id.clone(),
                port: "__seed__".to_string(),
                lane: Vec::new(),
                value: seed.value.clone(),
                at_unix: now,
            })
            .await;
    }

    // Queue the Fresh wake with the computed seeds, spawn the worker.
    state
        .slots
        .with_slot(color, {
            let seeds = seeds.clone();
            move |slot| {
                Box::pin(async move {
                    let queued = match slot {
                        crate::slots::Slot::Idle { queued, .. }
                        | crate::slots::Slot::Starting { queued, .. }
                        | crate::slots::Slot::WaitingReconnect { queued, .. } => queued,
                        crate::slots::Slot::Live { .. } => {
                            *slot = crate::slots::Slot::Idle {
                                queued: std::collections::VecDeque::new(),
                            };
                            let crate::slots::Slot::Idle { queued, .. } = slot else {
                                unreachable!()
                            };
                            queued
                        }
                    };
                    queued.push_back(crate::slots::QueuedWake::Start(
                        weft_core::primitive::WakeMessage::Fresh { seeds },
                    ));
                })
            }
        })
        .await;

    let wake = WakeContext { project_id: entry.project_id.clone(), color };
    let worker = state
        .workers
        .spawn_worker(&summary.binary_path, wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::WorkerSpawned {
            color,
            at_unix: now,
        })
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

    state
        .events
        .publish(crate::events::DispatcherEvent::ExecutionStarted {
            color,
            entry_node: entry.node_id.clone(),
            project_id: entry.project_id.clone(),
        })
        .await;

    Ok((StatusCode::ACCEPTED, Json(WebhookResponse { color: color.to_string() })))
}
