//! Execution state read endpoints. Writers (cost, log, suspension,
//! node events, status) live on the worker-to-dispatcher WebSocket
//! in `api::ws`. What's left here is: cancel (control), delete
//! (cleanup), and the reader endpoints the CLI, VS Code extension,
//! and dashboard hit over HTTP: logs, replay, list_executions, get.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Serialize;
use serde_json::Value;

use weft_core::Color;
use weft_core::primitive::DispatcherToWorker;

use crate::events::DispatcherEvent;
use crate::slots::Slot;

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
use crate::state::DispatcherState;

pub async fn cancel(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    cancel_color(&state, color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Cancel a single execution end-to-end:
///
///  1. Tell the live worker (if any) to stop via
///     `DispatcherToWorker::Cancel`. The engine wires this to the
///     cancellation `Notify` so the loop driver exits with
///     `LoopOutcome::Failed { error: "cancelled" }`.
///  2. Tear down every wake signal still registered on behalf of
///     this color: ask the tenant's listener to unregister each
///     token, drop the matching SignalTracker entries, drop the
///     journal's suspension lookup rows. Without this, webhooks /
///     timers / forms would keep firing into a dead execution.
///  3. Append an `ExecutionFailed { error: "cancelled" }` event
///     to the journal.
///  4. Broadcast `ExecutionFailed` on the project's SSE bus so
///     UIs flip out of "running" immediately.
///  5. Drop the dispatcher's slot so a future wake on this color
///     gets a fresh slot rather than reaching the dead worker.
///
/// Used both by the public `/executions/{color}/cancel` route and
/// internally by `extension::cancel_task` and
/// `project::deactivate_project`.
pub async fn cancel_color(state: &DispatcherState, color: Color) -> anyhow::Result<()> {
    // 1. Send Cancel over the WS AND kill the underlying worker
    //    Pod. The Cancel message is for a worker that's currently
    //    connected and running; kill_worker handles the pod that's
    //    in ImagePullBackOff or otherwise alive but not yet on the
    //    WS. Without the kill, k8s eventually schedules the pod,
    //    the worker connects, and runs the workflow as a zombie
    //    even after the user pressed Stop.
    let lease = crate::lease::lookup_slot(&state.pg_pool, color).await.ok().flatten();
    if let Some((owner, leased_until)) = lease {
        if owner == state.pod_id.as_str() {
            // Pull both the live WS sender and the worker handle
            // out of the slot in one pass; the kill happens after
            // we drop the lock so we don't block other waiters.
            let (live_sender, worker_handle) = state
                .slots
                .with_slot(color, |slot| {
                    Box::pin(async move {
                        match slot {
                            Slot::Live { sender, .. } => (Some(sender.clone()), None),
                            Slot::StalledGrace { sender, worker, .. } => {
                                (Some(sender.clone()), worker.take())
                            }
                            Slot::Starting { worker, .. } => (None, worker.take()),
                            Slot::WaitingReconnect { worker, .. } => (None, worker.take()),
                            Slot::Idle { .. } => (None, None),
                        }
                    })
                })
                .await;
            if let Some(sender) = live_sender {
                let _ = sender.send(DispatcherToWorker::Cancel).await;
            }
            if let Some(handle) = worker_handle {
                let _ = state.workers.kill_worker(handle).await;
            }
        } else if crate::lease::is_lease_live(leased_until) {
            // Forward to the owning Pod over internal HTTP.
            let req = crate::api::internal::CancelColorRequest {
                color: color.to_string(),
            };
            if let Err(e) = crate::routing::forward_to_pod_noreply(
                state,
                &owner,
                "/internal/cancel-color",
                &req,
            )
            .await
            {
                tracing::warn!(
                    target: "weft_dispatcher::cancel",
                    %color, owner = %owner, error = %e,
                    "internal cancel forward failed; continuing with journal-side cleanup"
                );
            }
        }
    }

    // 2. Strip every wake-signal registration tied to this color.
    let removed = state
        .journal
        .signal_remove_for_color(color)
        .await
        .unwrap_or_default();
    for meta in &removed {
        if let Some(handle) = state.listeners.get(&meta.tenant_id) {
            let _ = crate::listener::unregister_signal(&handle, &meta.token).await;
        }
        let _ = state.journal.consume_suspension(&meta.token).await;
    }

    // 3. Per-node cancellation. Fold the current journal, find
    //    every non-terminal record (Running, WaitingForInput),
    //    and journal one NodeCancelled per. Without this, the
    //    graph would leave parked nodes in their last lifecycle
    //    state forever (the modal would show no clear "Cancelled
    //    by user" reason). Frontends apply each NodeCancelled to
    //    flip the matching record visually.
    let project_id = state.journal.execution_project(color).await.ok().flatten();
    let now = unix_now();
    let cancel_reason = "Cancelled by user".to_string();
    let events_for_fold = state.journal.events_log(color).await.unwrap_or_default();
    let snapshot = crate::journal::fold_to_snapshot(color, &events_for_fold);
    let mut to_cancel: Vec<(String, weft_core::lane::Lane)> = Vec::new();
    for (node_id, execs) in &snapshot.executions {
        for e in execs {
            if !e.status.is_terminal() {
                to_cancel.push((node_id.clone(), e.lane.clone()));
            }
        }
    }
    for (node_id, lane) in to_cancel {
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::NodeCancelled {
                color,
                node_id: node_id.clone(),
                lane: lane.clone(),
                reason: cancel_reason.clone(),
                at_unix: now,
            })
            .await;
        if let Some(project_id) = &project_id {
            let lane_str = serde_json::to_string(&lane).unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeCancelled {
                    color,
                    node: node_id,
                    lane: lane_str,
                    reason: cancel_reason.clone(),
                    project_id: project_id.clone(),
                })
                .await;
        }
    }

    // 4. Journal cancellation. Also drops any straggler suspension
    //    rows the loop above missed (e.g. tracker entry was lost
    //    on dispatcher restart but the journal still has it).
    state.journal.cancel(color).await?;

    // Race guard: a SuspensionRequest could land in the WS handler
    // between step 1's Cancel send and the worker observing it.
    // Sweep again so a freshly-registered token doesn't orphan.
    let stragglers = state
        .journal
        .signal_remove_for_color(color)
        .await
        .unwrap_or_default();
    for meta in &stragglers {
        if let Some(handle) = state.listeners.get(&meta.tenant_id) {
            let _ = crate::listener::unregister_signal(&handle, &meta.token).await;
        }
        let _ = state.journal.consume_suspension(&meta.token).await;
    }

    // 4. Broadcast on the project's SSE bus.
    if let Some(project_id) = project_id {
        state
            .events
            .publish(DispatcherEvent::ExecutionFailed {
                color,
                project_id,
                error: "cancelled".into(),
            })
            .await;
    }

    // 5. Drop the slot so a future wake on this color gets a
    //    fresh slot rather than reaching the dead worker.
    state.slots.drop_slot(color).await;
    Ok(())
}

pub async fn get(State(_state): State<DispatcherState>, Path(_color): Path<String>) -> Json<Value> {
    // Phase B: execution status + cost aggregation.
    Json(serde_json::json!({ "status": "unknown" }))
}

#[derive(Debug, Serialize)]
pub struct LogLineOut {
    pub at_unix: u64,
    pub level: String,
    pub message: String,
}

pub async fn list_logs(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<Json<Vec<LogLineOut>>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let entries = state
        .journal
        .logs_for(color, 1_000)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        entries
            .into_iter()
            .map(|e| LogLineOut {
                at_unix: e.at_unix,
                level: e.level,
                message: e.message,
            })
            .collect(),
    ))
}

/// Replay a past execution: returns journaled node events shaped
/// as `DispatcherEvent` so the webview's live-SSE handler can
/// process them with the same code path.
///
/// We also surface the terminal execution_completed /
/// execution_failed event at the end when the summary row tells
/// us the run settled. Without that, the extension's ActionBar
/// can't flip its `isRunning` flag off and the Stop Execution
/// button stays visible.
pub async fn replay(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<Json<Vec<DispatcherEvent>>, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let project_id = state
        .journal
        .execution_project(color)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let raw_events = state
        .journal
        .events_for(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut out: Vec<DispatcherEvent> = raw_events
        .into_iter()
        .map(|e| node_event_to_dispatcher(e, project_id.clone()))
        .collect();

    // Infer the terminal state from the execution summary so the
    // UI sees ExecutionCompleted / ExecutionFailed and flips out
    // of "running" mode. A still-running exec (no terminal yet)
    // returns only node events; the live SSE will deliver the
    // terminal event when it happens.
    if let Some(summary) = state
        .journal
        .list_executions(500)
        .await
        .ok()
        .and_then(|list| list.into_iter().find(|s| s.color == color))
    {
        match summary.status.to_ascii_lowercase().as_str() {
            "completed" => out.push(DispatcherEvent::ExecutionCompleted {
                color,
                project_id: project_id.clone(),
                outputs: serde_json::Value::Null,
            }),
            "failed" => out.push(DispatcherEvent::ExecutionFailed {
                color,
                project_id: project_id.clone(),
                // Per-node errors are already in the stream; the
                // summary doesn't carry one so we leave it empty.
                error: String::new(),
            }),
            _ => {}
        }
    }
    Ok(Json(out))
}

/// Translate a journaled per-node event into the SSE-shaped
/// `DispatcherEvent` the extension's apply handler expects. Same
/// field names live wire uses (`node`, `lane`, `project_id`).
fn node_event_to_dispatcher(
    e: crate::journal::NodeExecEvent,
    project_id: String,
) -> DispatcherEvent {
    use crate::journal::NodeExecKind;
    match e.kind {
        NodeExecKind::Started => DispatcherEvent::NodeStarted {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            input: e.input.unwrap_or(serde_json::Value::Null),
            project_id,
        },
        NodeExecKind::Suspended => DispatcherEvent::NodeSuspended {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            token: e.token.unwrap_or_default(),
            project_id,
        },
        NodeExecKind::Resumed => DispatcherEvent::NodeResumed {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            token: e.token.unwrap_or_default(),
            value: e.value.unwrap_or(serde_json::Value::Null),
            project_id,
        },
        NodeExecKind::Retried => DispatcherEvent::NodeRetried {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            reason: e.reason.unwrap_or_default(),
            project_id,
        },
        NodeExecKind::Cancelled => DispatcherEvent::NodeCancelled {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            reason: e.reason.unwrap_or_default(),
            project_id,
        },
        NodeExecKind::Completed => DispatcherEvent::NodeCompleted {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            output: e.output.unwrap_or(serde_json::Value::Null),
            project_id,
        },
        NodeExecKind::Failed => DispatcherEvent::NodeFailed {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            error: e.error.unwrap_or_default(),
            project_id,
        },
        NodeExecKind::Skipped => DispatcherEvent::NodeSkipped {
            color: e.color,
            node: e.node_id,
            lane: e.lane,
            project_id,
        },
    }
}

pub async fn list_executions(
    State(state): State<DispatcherState>,
) -> Result<Json<Vec<crate::journal::ExecutionSummary>>, StatusCode> {
    let summaries = state
        .journal
        .list_executions(200)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(summaries))
}

/// Return the most recent execution for a project, or 404 if
/// the project has none. Used by `weft logs` (no-arg form) to
/// find the color to dump logs for.
pub async fn latest_for_project(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<crate::journal::ExecutionSummary>, StatusCode> {
    let summaries = state
        .journal
        .list_executions(500)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    summaries
        .into_iter()
        .find(|s| s.project_id == id_str)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn delete_execution(
    State(state): State<DispatcherState>,
    Path(color_str): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let color: Color = color_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    state
        .journal
        .delete_execution(color)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}
