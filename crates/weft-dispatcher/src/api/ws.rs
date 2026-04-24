//! WebSocket endpoint used by workers (spawned project binaries) to
//! duplex-communicate with the dispatcher.
//!
//! Protocol shape lives in `weft_core::primitive::{DispatcherToWorker,
//! WorkerToDispatcher}`. Each direction is a stream of typed JSON
//! messages; the dispatcher's end is this handler, the worker's end
//! lives in `weft-engine::dispatcher_link`.
//!
//! The handler owns one slot per color for the lifetime of the
//! connection. On connect it transitions the slot from `Starting`
//! (or `Idle`, for a direct reconnect) to `Live`, flushes queued
//! wakes via a `Start` message, then loops.

use std::collections::VecDeque;
use std::time::SystemTime;

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Path, State},
    response::Response,
};
use tokio::sync::mpsc;

use weft_core::primitive::{DispatcherToWorker, WakeMessage, WorkerToDispatcher};
use weft_core::Color;

use crate::events::DispatcherEvent;
use crate::slots::{QueuedWake, Slot};
use crate::state::DispatcherState;

pub async fn connect(
    ws: WebSocketUpgrade,
    Path(color): Path<String>,
    State(state): State<DispatcherState>,
) -> Response {
    let color: Color = match color.parse() {
        Ok(c) => c,
        Err(_) => {
            return Response::builder()
                .status(axum::http::StatusCode::BAD_REQUEST)
                .body(axum::body::Body::from("bad color"))
                .unwrap();
        }
    };
    ws.on_upgrade(move |socket| handle_socket(socket, color, state))
}

async fn handle_socket(socket: WebSocket, color: Color, state: DispatcherState) {
    let (mut ws_writer, mut ws_reader) = {
        use futures::stream::StreamExt;
        socket.split()
    };

    // Channel the HTTP handlers push into when they want to send a
    // message to this worker. One writer task owns the socket.
    let (tx, mut rx) = mpsc::channel::<DispatcherToWorker>(64);

    // Writer task: drain the channel, serialize, push to the socket.
    let writer = tokio::spawn(async move {
        use futures::sink::SinkExt;
        while let Some(msg) = rx.recv().await {
            let payload = match serde_json::to_string(&msg) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "weft_dispatcher::ws", "serialize outbound: {e}");
                    continue;
                }
            };
            if let Err(e) = ws_writer.send(Message::Text(payload.into())).await {
                tracing::warn!(target: "weft_dispatcher::ws", "socket write failed: {e}");
                break;
            }
        }
        // Attempt a clean close; ignore errors.
        let _ = ws_writer.close().await;
    });

    // Read the handshake frame. Either `Ready` (first connection
    // for this color) or `Reconnected` (transient socket drop; the
    // worker already has its in-memory state and just wants to
    // resume streaming).
    let first = loop {
        match ws_reader.next().await {
            Some(Ok(Message::Text(t))) => match serde_json::from_str::<WorkerToDispatcher>(&t) {
                Ok(msg) => break msg,
                Err(e) => {
                    tracing::warn!(target: "weft_dispatcher::ws", "bad frame pre-ready: {e}");
                }
            },
            Some(Ok(Message::Binary(_))) | Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) | None => {
                writer.abort();
                return;
            }
            Some(Err(e)) => {
                tracing::warn!(target: "weft_dispatcher::ws", "ws pre-handshake: {e}");
                writer.abort();
                return;
            }
        }
    };

    match first {
        WorkerToDispatcher::Ready => {
            if !handle_ready(&state, color, tx.clone()).await {
                writer.abort();
                return;
            }
        }
        WorkerToDispatcher::Reconnected { worker_instance_id } => {
            if !handle_reconnect(&state, color, &worker_instance_id, tx.clone()).await {
                // Dispatcher rejected the reconnect (already spawned
                // a replacement, or this color's slot is gone).
                writer.abort();
                return;
            }
        }
        other => {
            tracing::warn!(target: "weft_dispatcher::ws", "expected Ready/Reconnected, got {other:?}");
            writer.abort();
            return;
        }
    }

    // Main read loop.
    use futures::stream::StreamExt;
    while let Some(frame) = ws_reader.next().await {
        let text = match frame {
            Ok(Message::Text(t)) => t,
            Ok(Message::Binary(_)) | Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
            Ok(Message::Close(_)) => break,
            Err(e) => {
                tracing::warn!(target: "weft_dispatcher::ws", "read: {e}");
                break;
            }
        };
        let msg: WorkerToDispatcher = match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(target: "weft_dispatcher::ws", "parse: {e}");
                continue;
            }
        };
        if !handle_message(&state, color, msg, &tx).await {
            break;
        }
    }

    // Socket closed. If the slot is already `Idle` / gone, the
    // worker ended cleanly (Completed / Failed / Stalled moved it
    // there). If the slot is still `Live`, the socket dropped
    // without a terminal message: transition to `WaitingReconnect`
    // and start a 30-second grace timer. If the same worker
    // reconnects in time it gets promoted back to `Live`; if not,
    // the timer fires, we mark the old worker as crashed, and
    // spawn a replacement from the event log.
    let instance = state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if let Slot::Live { worker_instance_id, .. } = slot {
                    let id = worker_instance_id.clone();
                    *slot = Slot::WaitingReconnect {
                        since: std::time::Instant::now(),
                        queued: VecDeque::new(),
                        worker: None,
                        worker_instance_id: id.clone(),
                    };
                    Some(id)
                } else {
                    None
                }
            })
        })
        .await;

    if let Some(instance_id) = instance {
        tracing::warn!(
            target: "weft_dispatcher::ws",
            %color, instance = %instance_id,
            "worker socket dropped; awaiting reconnect (30s grace)"
        );
        let state_clone = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            // If still WaitingReconnect with this instance id after
            // the grace period, the worker didn't come back.
            let expired = state_clone
                .slots
                .with_slot(color, move |slot| {
                    let instance_id = instance_id.clone();
                    Box::pin(async move {
                        matches!(
                            slot,
                            Slot::WaitingReconnect { worker_instance_id, .. }
                                if *worker_instance_id == instance_id
                        )
                    })
                })
                .await;
            if expired {
                tracing::warn!(
                    target: "weft_dispatcher::ws",
                    %color, "reconnect grace expired; treating as crash"
                );
                handle_worker_crash(&state_clone, color).await;
            }
        });
    }

    let _ = writer.await;
}

/// Accept a fresh `Ready` handshake. Folds the event log to rebuild
/// the snapshot, transitions the slot to `Live`, ships the `Start`
/// packet. Returns `false` if the handshake couldn't complete; the
/// caller aborts the socket.
async fn handle_ready(
    state: &DispatcherState,
    color: Color,
    tx: mpsc::Sender<DispatcherToWorker>,
) -> bool {
    let events = match state.journal.events_log(color).await {
        Ok(ev) => ev,
        Err(e) => {
            tracing::error!(target: "weft_dispatcher::ws", "events_log: {e}");
            return false;
        }
    };
    let folded_snapshot = if events.is_empty() {
        None
    } else {
        Some(crate::journal::fold_to_snapshot(color, &events))
    };

    let worker_instance_id = uuid::Uuid::new_v4().to_string();
    let start = {
        let tx2 = tx.clone();
        let instance_for_slot = worker_instance_id.clone();
        let instance_for_start = worker_instance_id.clone();
        state
            .slots
            .with_slot(color, move |slot| {
                let tx = tx2.clone();
                let instance_for_slot = instance_for_slot.clone();
                let instance_for_start = instance_for_start.clone();
                Box::pin(async move {
                    let mut queued = match std::mem::replace(
                        slot,
                        Slot::Live {
                            sender: tx.clone(),
                            worker_instance_id: instance_for_slot,
                        },
                    ) {
                        Slot::Idle { queued, .. }
                        | Slot::Starting { queued, .. }
                        | Slot::WaitingReconnect { queued, .. } => queued,
                        Slot::Live { .. } => VecDeque::new(),
                    };
                    let wake = extract_start(&mut queued);
                    DispatcherToWorker::Start {
                        wake,
                        snapshot: folded_snapshot,
                        worker_instance_id: Some(instance_for_start),
                    }
                })
            })
            .await
    };
    tx.send(start).await.is_ok()
}

/// Accept a `Reconnected { worker_instance_id }` handshake. The
/// slot must be in `WaitingReconnect` with a matching instance id.
/// Restores the previous suspensions, transitions to `Live` with
/// the new sender, flushes queued wakes. Returns `false` if we
/// can't accept this reconnect (wrong state, or the dispatcher
/// already gave up and the slot is Idle from a fresh respawn).
async fn handle_reconnect(
    state: &DispatcherState,
    color: Color,
    worker_instance_id: &str,
    tx: mpsc::Sender<DispatcherToWorker>,
) -> bool {
    let expected = worker_instance_id.to_string();
    let deliveries = state
        .slots
        .with_slot(color, {
            let expected = expected.clone();
            let tx = tx.clone();
            move |slot| {
                Box::pin(async move {
                    match std::mem::replace(
                        slot,
                        Slot::Idle { queued: VecDeque::new() },
                    ) {
                        Slot::WaitingReconnect {
                            worker_instance_id: ref prev,
                            ..
                        } if *prev == expected => {
                            *slot = Slot::Live {
                                sender: tx,
                                worker_instance_id: expected.clone(),
                            };
                            true
                        }
                        other => {
                            *slot = other;
                            false
                        }
                    }
                })
            }
        })
        .await;

    if !deliveries {
        tracing::warn!(
            target: "weft_dispatcher::ws",
            %color, instance = %worker_instance_id,
            "rejecting Reconnected: slot not in WaitingReconnect or id mismatch"
        );
        return false;
    }

    // Resume after transient drop: no fresh state. The worker
    // kept its in-memory tables. New fires that arrived during
    // the disconnect are in the journal (SuspensionResolved
    // events); on the next ready-scan those nodes will re-check
    // via the resume path and pull the values.
    let resume = DispatcherToWorker::Start {
        wake: WakeMessage::Resume,
        snapshot: None,
        worker_instance_id: None,
    };
    tx.send(resume).await.is_ok()
}

/// Respawn a worker for `color` after the previous one crashed
/// (socket dropped without a clean terminal message). Folds the
/// event log to rebuild state, checks the crash-loop counter, and
/// either spawns a fresh worker or marks the execution Failed with
/// a best-guess culprit.
async fn handle_worker_crash(state: &DispatcherState, color: Color) {
    // Count recent WorkerSpawned events since the last NodeCompleted.
    // Three strikes in a row without new progress = stop trying.
    let events = match state.journal.events_log(color).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(target: "weft_dispatcher::ws", "events_log: {e}");
            return;
        }
    };
    let mut strikes_since_progress = 0u32;
    for ev in events.iter().rev() {
        match ev {
            crate::journal::ExecEvent::NodeCompleted { .. }
            | crate::journal::ExecEvent::NodeFailed { .. }
            | crate::journal::ExecEvent::NodeSkipped { .. } => break,
            crate::journal::ExecEvent::WorkerSpawned { .. }
            | crate::journal::ExecEvent::WorkerCrashed { .. } => {
                strikes_since_progress += 1;
            }
            _ => {}
        }
    }

    // Record the crash event itself so the loop detector sees it.
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::WorkerCrashed {
            color,
            reason: "socket dropped without terminal message".into(),
            at_unix: now_unix(),
        })
        .await;

    if strikes_since_progress >= 3 {
        let culprits = guess_crash_culprits(&events);
        let reason = if culprits.is_empty() {
            format!(
                "worker crashed {strikes_since_progress} times in a row \
                 without making progress; giving up"
            )
        } else {
            format!(
                "worker crashed {strikes_since_progress} times in a row \
                 without making progress; likely culprit node(s): {culprits:?}"
            )
        };
        tracing::error!(target: "weft_dispatcher::ws", %color, "{reason}");
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::ExecutionFailed {
                color,
                error: reason.clone(),
                at_unix: now_unix(),
            })
            .await;
        let project_id = project_of(state, color).await.unwrap_or_default();
        state
            .events
            .publish(DispatcherEvent::ExecutionFailed {
                color,
                error: reason,
                project_id,
            })
            .await;
        state.slots.drop_slot(color).await;
        return;
    }

    // Queue a Resume start and spawn a replacement. The WS handler
    // will fold the event log into a snapshot when the new worker
    // connects and sends Ready.
    let project_id = match project_of(state, color).await {
        Some(p) => p,
        None => {
            tracing::warn!(target: "weft_dispatcher::ws", %color, "no project id; giving up respawn");
            state.slots.drop_slot(color).await;
            return;
        }
    };
    let project_uuid = match project_id.parse::<uuid::Uuid>() {
        Ok(u) => u,
        Err(_) => {
            state.slots.drop_slot(color).await;
            return;
        }
    };
    let summary = match state.projects.get(project_uuid).await {
        Some(s) => s,
        None => {
            state.slots.drop_slot(color).await;
            return;
        }
    };

    state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                let mut queued = VecDeque::new();
                queued.push_back(crate::slots::QueuedWake::Start(
                    weft_core::primitive::WakeMessage::Resume,
                ));
                *slot = Slot::Idle { queued };
            })
        })
        .await;

    let wake = crate::backend::WakeContext { project_id: project_id.clone(), color };
    let spawn_result = state.workers.spawn_worker(&summary.binary_path, wake).await;
    match spawn_result {
        Ok(worker) => {
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
                        if let Slot::Idle { queued } = std::mem::replace(
                            slot,
                            Slot::Idle { queued: VecDeque::new() },
                        ) {
                            *slot = Slot::Starting { queued, worker: Some(worker) };
                        }
                    })
                })
                .await;
        }
        Err(e) => {
            tracing::error!(
                target: "weft_dispatcher::ws",
                %color,
                error = %e,
                "respawn failed; execution cannot recover"
            );
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::ExecutionFailed {
                    color,
                    error: format!("respawn failed: {e}"),
                    at_unix: now_unix(),
                })
                .await;
            state.slots.drop_slot(color).await;
        }
    }
}

/// Best-effort guess at which nodes caused a crash loop. Looks at
/// the tail of the event log: any NodeStarted events that weren't
/// followed by a NodeCompleted/Failed/Skipped are the likely
/// culprits (they were dispatched, then the worker crashed before
/// reporting a result).
fn guess_crash_culprits(events: &[crate::journal::ExecEvent]) -> Vec<String> {
    use crate::journal::ExecEvent;
    let mut unresolved: std::collections::HashSet<String> = Default::default();
    for ev in events {
        match ev {
            ExecEvent::NodeStarted { node_id, .. } => {
                unresolved.insert(node_id.clone());
            }
            ExecEvent::NodeCompleted { node_id, .. }
            | ExecEvent::NodeFailed { node_id, .. }
            | ExecEvent::NodeSkipped { node_id, .. } => {
                unresolved.remove(node_id);
            }
            _ => {}
        }
    }
    let mut out: Vec<String> = unresolved.into_iter().collect();
    out.sort();
    out
}

fn extract_start(queued: &mut VecDeque<QueuedWake>) -> WakeMessage {
    // The slot's queue only ever holds one `Start` entry (manual
    // run, trigger fire, or resume). Pop it; default to Resume if
    // absent (shouldn't happen in practice).
    match queued.pop_front() {
        Some(QueuedWake::Start(w)) => w,
        None => WakeMessage::Resume,
    }
}

async fn handle_message(
    state: &DispatcherState,
    color: Color,
    msg: WorkerToDispatcher,
    tx: &mpsc::Sender<DispatcherToWorker>,
) -> bool {
    match msg {
        WorkerToDispatcher::Ready => true, // stray re-ready; ignore
        WorkerToDispatcher::Reconnected { .. } => {
            // The handshake path already consumed the first
            // `Reconnected`. Any subsequent one on a live socket is
            // a protocol violation; log and ignore.
            tracing::warn!(target: "weft_dispatcher::ws", %color, "stray Reconnected on live socket; ignored");
            true
        }
        WorkerToDispatcher::SuspensionRequest { request_id, node_id, lane, spec } => {
            let token = uuid::Uuid::new_v4().to_string();

            // Look up this color's project + its active listener,
            // then let the listener own URL-minting and any
            // kind-specific setup (timer tasks, sse loops, etc).
            let project_id = match state.journal.execution_project(color).await {
                Ok(Some(p)) => p,
                _ => {
                    tracing::error!(
                        target: "weft_dispatcher::ws",
                        %color,
                        "no project for color; cannot register suspension"
                    );
                    return false;
                }
            };
            let listener = state.listeners.get(&project_id);
            let user_url = if let Some(listener) = &listener {
                match crate::listener::register_signal(listener, &token, &spec, &node_id).await {
                    Ok(url) => url,
                    Err(e) => {
                        tracing::error!(
                            target: "weft_dispatcher::ws",
                            %color,
                            error = %e,
                            "listener register failed"
                        );
                        return false;
                    }
                }
            } else {
                tracing::error!(
                    target: "weft_dispatcher::ws",
                    %color,
                    %project_id,
                    "project has no active listener; activate first"
                );
                return false;
            };

            // Persist the suspension so the signal-fired endpoint
            // can resolve the token back to its color+node, and
            // append a SuspensionRegistered event to the replayable
            // log.
            let metadata = serde_json::json!({
                "spec": spec,
                "node_id": node_id,
                "lane": lane,
            });
            if let Err(e) = state
                .journal
                .record_suspension_with_token(&token, color, &node_id, metadata)
                .await
            {
                tracing::error!(target: "weft_dispatcher::ws", "record_suspension: {e}");
                return false;
            }
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::SuspensionRegistered {
                    color,
                    node_id: node_id.clone(),
                    lane: lane.clone(),
                    token: token.clone(),
                    spec: spec.clone(),
                    at_unix: now_unix(),
                })
                .await;

            // Track so /signal-fired can route the resume.
            let kind_label = kind_of(&spec.kind);
            state.signal_tracker.insert(
                token.clone(),
                crate::listener::RegisteredSignalMeta {
                    project_id,
                    token: token.clone(),
                    node_id: node_id.clone(),
                    is_resume: true,
                    user_url: user_url.clone(),
                    kind: kind_label.into(),
                },
            );

            let _ = tx
                .send(DispatcherToWorker::SuspensionToken {
                    request_id,
                    token,
                    user_url,
                })
                .await;
            true
        }
        WorkerToDispatcher::RegisterSignalRequest { request_id, node_id, spec } => {
            // TriggerSetup-phase entry registration from a node.
            // Same listener registration as SuspensionRequest but the
            // tracker entry is `is_resume: false` and the signal
            // persists (not single-use).
            let project_id = match state.journal.execution_project(color).await {
                Ok(Some(p)) => p,
                _ => {
                    tracing::error!(
                        target: "weft_dispatcher::ws",
                        %color,
                        "no project for color; cannot register entry signal"
                    );
                    return false;
                }
            };
            let listener = match state.listeners.get(&project_id) {
                Some(l) => l,
                None => {
                    tracing::error!(
                        target: "weft_dispatcher::ws",
                        %color,
                        %project_id,
                        "project has no active listener; activate must have spawned one first"
                    );
                    return false;
                }
            };
            let token = uuid::Uuid::new_v4().to_string();
            let user_url = match crate::listener::register_signal(&listener, &token, &spec, &node_id).await {
                Ok(url) => url,
                Err(e) => {
                    tracing::error!(
                        target: "weft_dispatcher::ws",
                        %color,
                        error = %e,
                        "listener register (entry) failed"
                    );
                    return false;
                }
            };
            let kind_label = kind_of(&spec.kind);
            state.signal_tracker.insert(
                token.clone(),
                crate::listener::RegisteredSignalMeta {
                    project_id,
                    token: token.clone(),
                    node_id: node_id.clone(),
                    is_resume: false,
                    user_url: user_url.clone(),
                    kind: kind_label.into(),
                },
            );
            let _ = tx
                .send(DispatcherToWorker::RegisterSignalAck {
                    request_id,
                    token,
                    user_url,
                })
                .await;
            true
        }
        WorkerToDispatcher::SidecarEndpointRequest { request_id, node_id } => {
            let project_id = match state.journal.execution_project(color).await {
                Ok(Some(p)) => p,
                _ => String::new(),
            };
            let endpoint = if project_id.is_empty() {
                None
            } else {
                // handle_if_running returns None for a Stopped
                // sidecar so the worker fails loudly instead of
                // calling a dead DNS name.
                state
                    .infra_registry
                    .handle_if_running(&project_id, &node_id)
                    .and_then(|h| h.endpoint_url)
            };
            let _ = tx
                .send(DispatcherToWorker::SidecarEndpoint { request_id, endpoint })
                .await;
            true
        }
        WorkerToDispatcher::Stalled => {
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::Stalled {
                    color,
                    at_unix: now_unix(),
                })
                .await;
            // Transition slot back to Idle.
            state
                .slots
                .with_slot(color, move |slot| {
                    Box::pin(async move {
                        *slot = Slot::Idle { queued: VecDeque::new() };
                    })
                })
                .await;
            let _ = tx.send(DispatcherToWorker::StalledAck).await;

            // If the journal still has fires that weren't consumed
            // by this worker (happens when concurrent form POSTs
            // raced the worker's fold), auto-respawn from the event
            // log. The new worker will seed those pending
            // deliveries on Start.
            if has_pending_deliveries(state, color).await {
                spawn_respawn(state, color).await;
            }

            false
        }
        WorkerToDispatcher::Completed { outputs } => {
            state.slots.drop_slot(color).await;
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::ExecutionCompleted {
                    color,
                    outputs: outputs.clone(),
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::ExecutionCompleted {
                    color,
                    outputs,
                    project_id,
                })
                .await;
            false
        }
        WorkerToDispatcher::Failed { error } => {
            state.slots.drop_slot(color).await;
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::ExecutionFailed {
                    color,
                    error: error.clone(),
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::ExecutionFailed {
                    color,
                    error,
                    project_id,
                })
                .await;
            false
        }
        WorkerToDispatcher::NodeEvent {
            node_id,
            lane,
            event,
            input,
            output,
            error,
            pulses_absorbed,
        } => {
            let kind = match crate::journal::NodeExecKind::parse(&event) {
                Some(k) => k,
                None => {
                    tracing::warn!(target: "weft_dispatcher::ws", "unknown node event: {event}");
                    return true;
                }
            };
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();

            // Append to the unified event log. `events_for` (the
            // replay reader) folds these back into NodeExecEvent.
            let exec_event = match kind {
                crate::journal::NodeExecKind::Started => {
                    crate::journal::ExecEvent::NodeStarted {
                        color,
                        node_id: node_id.clone(),
                        lane: lane_struct.clone(),
                        input: input.clone().unwrap_or(serde_json::Value::Null),
                        pulses_absorbed: pulses_absorbed.clone(),
                        at_unix: now_unix(),
                    }
                }
                crate::journal::NodeExecKind::Completed => {
                    crate::journal::ExecEvent::NodeCompleted {
                        color,
                        node_id: node_id.clone(),
                        lane: lane_struct.clone(),
                        output: output.clone().unwrap_or(serde_json::Value::Null),
                        at_unix: now_unix(),
                    }
                }
                crate::journal::NodeExecKind::Failed => crate::journal::ExecEvent::NodeFailed {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct.clone(),
                    error: error.clone().unwrap_or_default(),
                    at_unix: now_unix(),
                },
                crate::journal::NodeExecKind::Skipped => crate::journal::ExecEvent::NodeSkipped {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct.clone(),
                    at_unix: now_unix(),
                },
            };
            let _ = state.journal.record_event(&exec_event).await;

            // Derive PulseEmitted events from the project's edges.
            // Running them here means replay doesn't have to call
            // postprocess_output; the pulse table rebuilds straight
            // from the events.
            if matches!(
                kind,
                crate::journal::NodeExecKind::Completed | crate::journal::NodeExecKind::Skipped
            ) {
                let out_value = match kind {
                    crate::journal::NodeExecKind::Completed => {
                        output.clone().unwrap_or(serde_json::Value::Null)
                    }
                    crate::journal::NodeExecKind::Skipped
                    | crate::journal::NodeExecKind::Failed => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                emit_pulse_events(state, color, &node_id, &lane_struct, &out_value).await;
            }

            let project_id = project_of(state, color).await.unwrap_or_default();
            let dispatcher_event = match kind {
                crate::journal::NodeExecKind::Started => DispatcherEvent::NodeStarted {
                    color,
                    node: node_id,
                    lane,
                    input: input.unwrap_or(serde_json::Value::Null),
                    project_id,
                },
                crate::journal::NodeExecKind::Completed => DispatcherEvent::NodeCompleted {
                    color,
                    node: node_id,
                    lane,
                    output: output.unwrap_or(serde_json::Value::Null),
                    project_id,
                },
                crate::journal::NodeExecKind::Failed => DispatcherEvent::NodeFailed {
                    color,
                    node: node_id,
                    lane,
                    error: error.unwrap_or_default(),
                    project_id,
                },
                crate::journal::NodeExecKind::Skipped => DispatcherEvent::NodeSkipped {
                    color,
                    node: node_id,
                    lane,
                    project_id,
                },
            };
            state.events.publish(dispatcher_event).await;
            true
        }
        WorkerToDispatcher::Log { level, message } => {
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::LogLine {
                    color,
                    level,
                    message,
                    at_unix: now_unix(),
                })
                .await;
            true
        }
        WorkerToDispatcher::Cost(report) => {
            let service = report.service.clone();
            let amount_usd = report.amount_usd;
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::CostReported {
                    color,
                    service: report.service,
                    model: report.model,
                    amount_usd: report.amount_usd,
                    metadata: report.metadata,
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::CostReported {
                    color,
                    project_id,
                    service,
                    amount_usd,
                })
                .await;
            true
        }
    }
}

/// Derive `PulseEmitted` events from the project's edges after a
/// node completes or is skipped. Mirrors `postprocess_output` in
/// `weft-core::exec::postprocess` but records instead of mutating a
/// pulse table. Replay uses these events to rebuild the pulse
/// table, so the engine and the journal agree by construction.
async fn emit_pulse_events(
    state: &DispatcherState,
    color: Color,
    source_node: &str,
    lane: &weft_core::lane::Lane,
    output: &serde_json::Value,
) {
    let project_id = match project_of(state, color).await {
        Some(p) => p,
        None => return,
    };
    let project_uuid = match project_id.parse::<uuid::Uuid>() {
        Ok(u) => u,
        Err(_) => return,
    };
    let project = match state.projects.project(project_uuid).await {
        Some(p) => p,
        None => return,
    };
    let edge_idx = weft_core::project::EdgeIndex::build(&project);
    for edge in edge_idx.get_outgoing(&project, source_node) {
        let source_port = edge.source_handle.as_deref().unwrap_or("default");
        let target_port = edge.target_handle.as_deref().unwrap_or("default");
        let routed = output
            .get(source_port)
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::PulseEmitted {
                color,
                source_node: source_node.to_string(),
                source_port: source_port.to_string(),
                target_node: edge.target.clone(),
                target_port: target_port.to_string(),
                lane: lane.clone(),
                value: routed,
                at_unix: now_unix(),
            })
            .await;
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn project_of(state: &DispatcherState, color: Color) -> Option<String> {
    state.journal.execution_project(color).await.ok().flatten()
}

/// Fold the journal and check whether any fires arrived that the
/// last worker didn't consume (i.e. `pending_deliveries` is
/// non-empty after removing the consumed ones). Used after a stall
/// to decide whether to auto-respawn: if fires are pending, the
/// new worker will pick them up via its Start snapshot.
async fn has_pending_deliveries(state: &DispatcherState, color: Color) -> bool {
    let Ok(events) = state.journal.events_log(color).await else {
        return false;
    };
    let snap = crate::journal::fold_to_snapshot(color, &events);
    !snap.pending_deliveries.is_empty()
}

/// Queue a Resume Start, spawn a fresh worker, transition slot to
/// Starting. Used after a stall when the journal has unconsumed
/// fires: the outgoing worker missed them, the new worker picks
/// them up via the folded Start snapshot.
async fn spawn_respawn(state: &DispatcherState, color: Color) {
    let Some(project_id) = project_of(state, color).await else { return };
    let Ok(project_uuid) = project_id.parse::<uuid::Uuid>() else { return };
    let Some(summary) = state.projects.get(project_uuid).await else { return };

    // Reserve the spawn atomically: only transition Idle → Starting.
    let reserved = state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if matches!(slot, Slot::Idle { .. }) {
                    let mut q = match std::mem::replace(
                        slot,
                        Slot::Idle { queued: VecDeque::new() },
                    ) {
                        Slot::Idle { queued } => queued,
                        _ => unreachable!(),
                    };
                    q.push_front(QueuedWake::Start(WakeMessage::Resume));
                    *slot = Slot::Starting { queued: q, worker: None };
                    true
                } else {
                    false
                }
            })
        })
        .await;
    if !reserved {
        return;
    }

    let wake = crate::backend::WakeContext { project_id: project_id.clone(), color };
    match state.workers.spawn_worker(&summary.binary_path, wake).await {
        Ok(worker) => {
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
        }
        Err(e) => {
            tracing::error!(
                target: "weft_dispatcher::ws",
                %color, error = %e,
                "auto-respawn after stall failed"
            );
        }
    }
}



fn kind_of(kind: &weft_core::primitive::WakeSignalKind) -> &'static str {
    use weft_core::primitive::WakeSignalKind;
    match kind {
        WakeSignalKind::Webhook { .. } => "webhook",
        WakeSignalKind::Timer { .. } => "timer",
        WakeSignalKind::Form { .. } => "form",
        WakeSignalKind::Sse { .. } => "sse",
        WakeSignalKind::Socket { .. } => "socket",
    }
}
