//! `POST /signal-fired`: called by a listener when any wake signal
//! fires. Dispatcher authenticates against the tenant's relay
//! token, looks up the signal, and either spawns a fresh execution
//! (entry signal) or delivers the payload to a suspended lane
//! (resume signal).
//!
//! `POST /signal-failed`: listener exhausted retries; dispatcher
//! records SuspensionFailed so the affected node fails (only that
//! lane).
//!
//! `POST /listener/empty`: listener's registry hit zero; dispatcher
//! kills the listener pod if its own signal_tracker is also empty
//! for this tenant.

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
    pub tenant_id: String,
    pub token: String,
    pub payload: Value,
}

/// Mirrors `weft_listener::SignalFiredAck`. The two crates carry
/// their own copy so neither has to depend on the other's wire
/// types.
#[derive(Debug, Serialize)]
#[serde(tag = "ack", rename_all = "snake_case")]
pub enum SignalFiredAck {
    Consume { color: String },
    Retry { retry_after_ms: u64, reason: String },
}

#[derive(Debug, Deserialize)]
pub struct SignalFailedRequest {
    pub tenant_id: String,
    pub token: String,
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct ListenerEmptyRequest {
    pub tenant_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ListenerRegisterMeRequest {
    pub tenant_id: String,
}

pub async fn signal_fired(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<SignalFiredRequest>,
) -> Result<Json<SignalFiredAck>, (StatusCode, String)> {
    auth_relay(&state, &req.tenant_id, &headers)?;

    let meta = state
        .journal
        .signal_get(&req.token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?;
    let expected_tenant = state.tenant_router.tenant_for_project(&meta.project_id);
    if expected_tenant.as_str() != req.tenant_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "tenant mismatch between request and tracked signal".into(),
        ));
    }

    let routed = if meta.is_resume {
        route_resume(&state, &req.token, req.payload).await
    } else {
        route_entry(&state, &meta, req.payload).await
    };
    match routed {
        Ok(color) => Ok(Json(SignalFiredAck::Consume {
            color: color.to_string(),
        })),
        Err((status, reason)) if is_transient(status) => Ok(Json(SignalFiredAck::Retry {
            retry_after_ms: 200,
            reason,
        })),
        Err(e) => Err(e),
    }
}

/// Status codes the dispatcher considers transient, so the listener
/// retries instead of escalating to fail-dispatch. Anything else is
/// a hard error returned as-is.
fn is_transient(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
            | StatusCode::CONFLICT
    )
}

pub async fn signal_failed(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<SignalFailedRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    auth_relay(&state, &req.tenant_id, &headers)?;

    // Drop the signal row. Even if it's already gone, we still
    // journal the failure for the color we can resolve via the
    // wake table.
    let _removed = state
        .journal
        .signal_remove(&req.token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let target = state
        .journal
        .resolve_wake(&req.token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;

    if let Some(target) = target {
        state
            .journal
            .consume_suspension(&req.token)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("consume: {e}")))?;
        state
            .journal
            .record_event(&crate::journal::ExecEvent::SuspensionFailed {
                color: target.color,
                node_id: target.node.clone(),
                token: req.token.clone(),
                reason: req.reason.clone(),
                at_unix: unix_now(),
            })
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;

        // Surface a NodeFailed for only the affected node so the
        // SSE timeline matches what the engine will see on resume.
        let project_id = state
            .journal
            .execution_project(target.color)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
            .unwrap_or_default();
        state
            .events
            .publish(crate::events::DispatcherEvent::NodeFailed {
                color: target.color,
                node: target.node.clone(),
                lane: String::new(),
                error: format!("suspension fire failed: {}", req.reason),
                project_id,
            })
            .await;

        // Kick the worker so it picks up the SuspensionFailed and
        // fails just this node's lane.
        let project_id_str = state
            .journal
            .execution_project(target.color)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?
            .unwrap_or_default();
        if !project_id_str.is_empty() {
            ensure_worker(&state, target.color, &project_id_str).await?;
        }
    } else {
        tracing::warn!(
            target: "weft_dispatcher::signal",
            token = %req.token,
            "signal-failed for unknown token; nothing to journal"
        );
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn listener_register_me(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<ListenerRegisterMeRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    auth_relay(&state, &req.tenant_id, &headers)?;

    // Listener pod just booted (or restarted). Re-push every signal
    // we have on file for this tenant so its in-memory Registry
    // matches our durable state.
    let signals = state
        .journal
        .signal_list_for_tenant(&req.tenant_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let listener = state
        .listeners
        .get(&req.tenant_id)
        .ok_or((StatusCode::NOT_FOUND, "no listener for tenant".into()))?;
    for sig in &signals {
        let spec: weft_core::primitive::WakeSignalSpec =
            match serde_json::from_str(&sig.spec_json) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        target: "weft_dispatcher::signal",
                        token = %sig.token, error = %e,
                        "drop malformed spec_json on rehydrate"
                    );
                    continue;
                }
            };
        let _ = crate::listener::register_signal(&listener, &sig.token, &spec, &sig.node_id).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Diagnostic: report what each tenant's listener thinks it's
/// listening to, alongside what the journal thinks should be
/// there. Drift between the two means the cleanup pipeline went
/// wrong somewhere; the operator can compare and decide whether
/// to manually nuke the listener Deployment.
///
/// Response shape:
/// ```json
/// [
///   {
///     "tenant_id": "local",
///     "listener_url": "http://...",
///     "deploy_name": "listener-local",
///     "journal_signal_count": 0,
///     "listener_registry": [{"token": "...", "node_id": "...", ...}]
///   }
/// ]
/// ```
///
/// `listener_registry` is `null` when the listener didn't respond
/// (network error, deployment missing). That's load-bearing
/// drift information itself: a row with non-zero journal count
/// AND a missing listener means signals will never fire.
pub async fn listener_inspect(
    State(state): State<DispatcherState>,
) -> Result<Json<Vec<Value>>, (StatusCode, String)> {
    let mut out = Vec::new();
    for (tenant_id, handle) in state.listeners.list() {
        let journal_count = state
            .journal
            .signal_count_for_tenant(&tenant_id)
            .await
            .unwrap_or(0);
        let listener_registry: Option<Value> = match reqwest::Client::new()
            .get(format!("{}/signals", handle.admin_url.trim_end_matches('/')))
            .bearer_auth(&handle.admin_token)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
            _ => None,
        };
        out.push(serde_json::json!({
            "tenant_id": tenant_id,
            "listener_url": handle.admin_url,
            "journal_signal_count": journal_count,
            "listener_registry": listener_registry,
        }));
    }
    Ok(Json(out))
}

pub async fn listener_empty(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Json(req): Json<ListenerEmptyRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    auth_relay(&state, &req.tenant_id, &headers)?;

    // Race guard: check our own count for this tenant. If something
    // landed between the listener's "I'm empty" notice and now, keep
    // the listener alive and return 409 so the listener stays put.
    let count = state
        .journal
        .signal_count_for_tenant(&req.tenant_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    if count > 0 {
        return Ok(StatusCode::CONFLICT);
    }

    let tenant = crate::tenant::TenantId(req.tenant_id.clone());
    let namespace = state.namespace_mapper.namespace_for(&tenant);
    state
        .listeners
        .kill(
            &tenant,
            &namespace,
            state.listener_backend.as_ref(),
            &state.pg_pool,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("kill listener: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

fn auth_relay(
    state: &DispatcherState,
    tenant_id: &str,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, String)> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    let listener = state
        .listeners
        .get(tenant_id)
        .ok_or((StatusCode::NOT_FOUND, "no listener for tenant".into()))?;
    if bearer != listener.relay_token {
        return Err((StatusCode::UNAUTHORIZED, "bad relay token".into()));
    }
    Ok(())
}

async fn route_entry(
    state: &DispatcherState,
    meta: &crate::journal::SignalRegistration,
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
                pulse_id: seed.pulse_id.clone(),
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
                        // Fresh entry: should not collide with a
                        // live or stalled worker for this brand-new
                        // color. Reset to Idle defensively.
                        crate::slots::Slot::Live { .. }
                        | crate::slots::Slot::StalledGrace { .. } => {
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
    let wake = crate::backend::WakeContext::resolve(state, meta.project_id.clone(), color);
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

    // Drop the signal row since the suspension is single-use.
    let _ = state.journal.signal_remove(token).await;

    // Hot path: ownership-aware deliver. If another Pod owns the
    // slot, forward `/internal/deliver-color` to it; that Pod owns
    // the live WS and can hand the value to the warm worker. If no
    // Pod owns the slot (or the lease expired), respawn locally.
    let lease = crate::lease::lookup_slot(&state.pg_pool, color)
        .await
        .ok()
        .flatten();
    let delivered = if let Some((owner, leased_until)) = lease.clone() {
        if owner == state.pod_id.as_str() {
            try_deliver_to_warm_worker(state, color, token.to_string()).await
        } else if crate::lease::is_lease_live(leased_until) {
            forward_deliver_to_owner(state, color, token, &owner).await
        } else {
            false
        }
    } else {
        false
    };
    if !delivered {
        ensure_worker(state, color, &project_id_str).await?;
    }

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

/// If the slot is in `StalledGrace`, promote it back to Live and
/// forward a Deliver via the warm sender. Returns true on success.
/// On any other slot state, returns false: caller falls back to the
/// normal `ensure_worker` respawn path.
async fn try_deliver_to_warm_worker(
    state: &DispatcherState,
    color: weft_core::Color,
    token: String,
) -> bool {
    use weft_core::primitive::Delivery;
    // Read the most-recent SuspensionResolved value out of the
    // journal so a Deliver(value) can land on the warm worker.
    let Ok(events) = state.journal.events_log(color).await else {
        return false;
    };
    let value = events.iter().rev().find_map(|e| match e {
        crate::journal::ExecEvent::SuspensionResolved { token: t, value, .. } if t == &token => {
            Some(value.clone())
        }
        _ => None,
    });
    let Some(value) = value else {
        return false;
    };

    let sender = state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                let taken = std::mem::replace(slot, crate::slots::Slot::Idle {
                    queued: std::collections::VecDeque::new(),
                });
                match taken {
                    crate::slots::Slot::StalledGrace {
                        sender,
                        worker_instance_id,
                        ..
                    } => {
                        *slot = crate::slots::Slot::Live {
                            sender: sender.clone(),
                            worker_instance_id,
                        };
                        Some(sender)
                    }
                    other => {
                        *slot = other;
                        None
                    }
                }
            })
        })
        .await;
    let Some(sender) = sender else {
        return false;
    };
    sender
        .send(weft_core::primitive::DispatcherToWorker::Deliver(Delivery {
            token,
            value,
        }))
        .await
        .is_ok()
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

    let wake = crate::backend::WakeContext::resolve(state, project_id.to_string(), color);
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

/// Pod-to-Pod forward: ask the slot owner to deliver this token's
/// value. The journal already has `SuspensionResolved`; we just
/// route the in-RAM Deliver to the right Pod's WS sender.
async fn forward_deliver_to_owner(
    state: &DispatcherState,
    color: weft_core::Color,
    token: &str,
    owner_pod_id: &str,
) -> bool {
    let Ok(events) = state.journal.events_log(color).await else {
        return false;
    };
    let value = events.iter().rev().find_map(|e| match e {
        crate::journal::ExecEvent::SuspensionResolved { token: t, value, .. } if t == token => {
            Some(value.clone())
        }
        _ => None,
    });
    let Some(value) = value else {
        return false;
    };
    let req = crate::api::internal::DeliverColorRequest {
        color: color.to_string(),
        token: token.to_string(),
        value,
    };
    crate::routing::forward_to_pod_noreply(state, owner_pod_id, "/internal/deliver-color", &req)
        .await
        .is_ok()
}
