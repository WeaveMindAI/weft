//! Form submission entry. `POST /f/{token}`: look up the suspension,
//! journal the fire as `SuspensionResolved`, ensure a worker is
//! alive for the color.
//!
//! The journal is the only source of truth for fires. A worker that
//! spawns for this color folds the event log into an
//! `ExecutionSnapshot` whose `pending_deliveries` map contains every
//! fire not yet consumed by a `NodeCompleted`. The worker seeds
//! those into its link and the waiting nodes pick them up via
//! `await_signal`'s resume path. No in-memory slot queue for
//! deliveries, no WebSocket push, no race.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use crate::backend::WakeContext;
use crate::slots::Slot;
use crate::state::DispatcherState;

#[derive(Debug, Serialize)]
pub struct FormSubmitResponse {
    pub color: String,
}

pub async fn submit(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<FormSubmitResponse>, (StatusCode, String)> {
    // One URL scheme, two kinds of tokens: suspension tokens
    // resume an existing execution; entry tokens (minted at
    // project activation) start a fresh one. Try suspension first
    // (single-use, shorter lifetime) and fall back to entry-token
    // trigger fire.
    let suspension = state
        .journal
        .resolve_wake(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let Some(target) = suspension else {
        return submit_entry_form(state, token, body).await;
    };

    let color = target.color;
    let project_id_str = state
        .journal
        .execution_project(color)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "execution not journaled".into()))?;
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
        .consume_suspension(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("consume: {e}")))?;

    state
        .journal
        .record_event(&crate::journal::ExecEvent::SuspensionResolved {
            color,
            token: token.clone(),
            value: body,
            at_unix: now_unix(),
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;

    ensure_worker(&state, color, &project_id_str, &summary.binary_path).await?;

    state
        .events
        .publish(crate::events::DispatcherEvent::ExecutionResumed {
            color,
            node: target.node.clone(),
            project_id: project_id_str,
        })
        .await;

    Ok(Json(FormSubmitResponse { color: color.to_string() }))
}

/// Make sure a worker is alive for this color. Idempotent: if the
/// slot is already `Starting`/`Live`/`WaitingReconnect`, no-op. If
/// it's `Idle`, atomically transition to `Starting` and spawn.
async fn ensure_worker(
    state: &DispatcherState,
    color: weft_core::Color,
    project_id: &str,
    binary_path: &std::path::Path,
) -> Result<(), (StatusCode, String)> {
    let must_spawn = state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if matches!(slot, Slot::Idle { .. }) {
                    let mut q = match std::mem::replace(
                        slot,
                        Slot::Idle { queued: std::collections::VecDeque::new() },
                    ) {
                        Slot::Idle { queued } => queued,
                        _ => unreachable!(),
                    };
                    q.push_front(crate::slots::QueuedWake::Start(
                        weft_core::primitive::WakeMessage::Resume,
                    ));
                    *slot = Slot::Starting { queued: q, worker: None };
                    true
                } else {
                    false
                }
            })
        })
        .await;

    if !must_spawn {
        return Ok(());
    }

    let wake = WakeContext { project_id: project_id.to_string(), color };
    let worker = state
        .workers
        .spawn_worker(binary_path, wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::WorkerSpawned {
            color,
            at_unix: now_unix(),
        })
        .await;
    state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if let Slot::Starting { worker: w, .. } = slot {
                    *w = Some(worker);
                }
            })
        })
        .await;
    Ok(())
}

/// Entry-token form submission: minted by `/projects/{id}/activate`
/// for nodes with an entry-use Form signal (HumanTrigger). Spawns
/// a fresh execution seeded by the trigger-fire subgraph. Same
/// path webhook.rs uses; differs only in how the payload is named
/// to the firing node.
async fn submit_entry_form(
    state: DispatcherState,
    token: String,
    body: Value,
) -> Result<Json<FormSubmitResponse>, (StatusCode, String)> {
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

    // Form submission is delivered to the firing node as
    // `{ body: <submission> }`, matching the shape trigger nodes
    // already know to unwrap (webhook.rs does the same).
    let payload = serde_json::json!({ "body": body });
    let seeds = crate::api::project::compute_trigger_seeds(
        &project,
        &entry.node_id,
        &payload,
    );
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
    let now = now_unix();
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

    state
        .slots
        .with_slot(color, {
            let seeds = seeds.clone();
            move |slot| {
                Box::pin(async move {
                    let queued = match slot {
                        Slot::Idle { queued, .. }
                        | Slot::Starting { queued, .. }
                        | Slot::WaitingReconnect { queued, .. } => queued,
                        Slot::Live { .. } => {
                            *slot = Slot::Idle {
                                queued: std::collections::VecDeque::new(),
                            };
                            let Slot::Idle { queued, .. } = slot else { unreachable!() };
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
        .record_event(&crate::journal::ExecEvent::WorkerSpawned { color, at_unix: now })
        .await;
    state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if let Slot::Starting { worker: w, .. } = slot {
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

    Ok(Json(FormSubmitResponse { color: color.to_string() }))
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
