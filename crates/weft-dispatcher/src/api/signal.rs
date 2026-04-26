//! `POST /signal-fired`: called by a listener when any wake signal
//! fires. Dispatcher authenticates against the project's relay
//! token, looks up the signal, and either spawns a fresh execution
//! (entry signal) or delivers the payload to a suspended lane
//! (resume signal).

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::state::DispatcherState;

#[derive(Debug, Deserialize)]
pub struct SignalFiredRequest {
    pub project_id: String,
    pub token: String,
    pub payload: Value,
}

#[derive(Debug, Serialize)]
pub struct SignalFiredResponse {
    /// Color of the spawned execution (for entry signals) or the
    /// color the delivery was routed to (for resume signals).
    pub color: String,
}

pub async fn signal_fired(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<SignalFiredRequest>,
) -> Result<Json<SignalFiredResponse>, (StatusCode, String)> {
    // Auth: the bearer token must match this project's listener
    // relay token. Guards against arbitrary callers forging fires.
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    let listener = state
        .listeners
        .get(&req.project_id)
        .ok_or((StatusCode::NOT_FOUND, "no listener for project".into()))?;
    if bearer != listener.relay_token {
        return Err((StatusCode::UNAUTHORIZED, "bad relay token".into()));
    }

    // Token must be tracked.
    let meta = state
        .signal_tracker
        .get(&req.token)
        .ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?;
    if meta.project_id != req.project_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "project id mismatch between request and tracked signal".into(),
        ));
    }

    // Resume vs fresh.
    if meta.is_resume {
        // Suspension resolution: delegate to the same fold/respawn
        // path the old form.rs used.
        let color = route_resume(&state, &req.token, req.payload).await?;
        Ok(Json(SignalFiredResponse {
            color: color.to_string(),
        }))
    } else {
        // Entry fire: spawn a fresh execution seeded by the
        // firing-trigger subgraph.
        let color = route_entry(&state, &meta, req.payload).await?;
        Ok(Json(SignalFiredResponse {
            color: color.to_string(),
        }))
    }
}

async fn route_entry(
    state: &DispatcherState,
    meta: &crate::listener::RegisteredSignalMeta,
    payload: Value,
) -> Result<uuid::Uuid, (StatusCode, String)> {
    let project_uuid: uuid::Uuid = meta
        .project_id
        .parse()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad project id".into()))?;
    let summary = state
        .projects
        .get(project_uuid)
        .await
        .ok_or((StatusCode::GONE, "project not registered".into()))?;
    let project = state
        .projects
        .project(project_uuid)
        .await
        .ok_or((StatusCode::GONE, "project definition missing".into()))?;

    let seeds = crate::api::project::compute_trigger_seeds(&project, &meta.node_id, &payload);
    if seeds.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "trigger '{}' has no output downstream; nothing to run",
                meta.node_id
            ),
        ));
    }

    let color = uuid::Uuid::new_v4();
    let now = unix_now();
    state
        .journal
        .record_event(&crate::journal::ExecEvent::ExecutionStarted {
            color,
            project_id: meta.project_id.clone(),
            entry_node: meta.node_id.clone(),
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
                        weft_core::primitive::WakeMessage::Fresh {
                            seeds,
                            phase: weft_core::context::Phase::Fire,
                        },
                    ));
                })
            }
        })
        .await;

    let _ = &summary;
    let wake = crate::backend::WakeContext {
        project_id: meta.project_id.clone(),
        color,
    };
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
    state
        .events
        .publish(crate::events::DispatcherEvent::ExecutionStarted {
            color,
            entry_node: meta.node_id.clone(),
            project_id: meta.project_id.clone(),
        })
        .await;
    Ok(color)
}

async fn route_resume(
    state: &DispatcherState,
    token: &str,
    payload: Value,
) -> Result<uuid::Uuid, (StatusCode, String)> {
    // Resolve the suspension the v2 way: journal holds the mapping
    // token → (color, node). Same as the old form.rs suspension
    // path, lifted here so both webhook and form tokens route the
    // same way through the listener.
    let target = state
        .journal
        .resolve_wake(token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown suspension".into()))?;
    let color = target.color;
    let project_id_str = state
        .journal
        .execution_project(color)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "execution not journaled".into()))?;
    let project_uuid: uuid::Uuid = project_id_str
        .parse()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad project id".into()))?;
    // Project lookup confirms registration; the binary identity is
    // implicit in the project id (the worker image tag derives from
    // it), so we only need to confirm it exists.
    if state.projects.get(project_uuid).await.is_none() {
        return Err((StatusCode::GONE, "project not registered".into()));
    }

    state
        .journal
        .consume_suspension(token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("consume: {e}")))?;
    state
        .journal
        .record_event(&crate::journal::ExecEvent::SuspensionResolved {
            color,
            token: token.to_string(),
            value: payload,
            at_unix: unix_now(),
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;

    // Drop the tracker entry since the suspension is single-use.
    state.signal_tracker.remove(token);

    // Respawn the worker if the slot is idle.
    ensure_worker(state, color, &project_id_str).await?;

    state
        .events
        .publish(crate::events::DispatcherEvent::ExecutionResumed {
            color,
            node: target.node.clone(),
            project_id: project_id_str,
        })
        .await;

    Ok(color)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Make sure a worker is alive for this color. Idempotent: if the
/// slot is already `Starting`/`Live`/`WaitingReconnect`, no-op. If
/// it's `Idle`, atomically transition to `Starting` and spawn.
async fn ensure_worker(
    state: &DispatcherState,
    color: weft_core::Color,
    project_id: &str,
) -> Result<(), (StatusCode, String)> {
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
                    *slot = crate::slots::Slot::Starting {
                        queued: q,
                        worker: None,
                    };
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

    let wake = crate::backend::WakeContext {
        project_id: project_id.to_string(),
        color,
    };
    let worker = state
        .workers
        .spawn_worker(wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::WorkerSpawned {
            color,
            at_unix: unix_now(),
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
    Ok(())
}
