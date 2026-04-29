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
                } else if matches!(slot, Slot::StalledGrace { .. }) {
                    // Worker exited cleanly post-Exit. Drop to Idle.
                    *slot = Slot::Idle { queued: VecDeque::new() };
                    None
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
    // Claim ownership of this color's slot. If another Pod already
    // owns a live lease, abort: the worker will reconnect via the
    // round-robin Service and eventually land on the right Pod.
    match crate::routing::route_for_color(state, color).await {
        Ok(crate::routing::ColorRoute::Local) => {}
        Ok(crate::routing::ColorRoute::Forward { owner_pod_id }) => {
            tracing::info!(
                target: "weft_dispatcher::ws",
                %color, owner = %owner_pod_id,
                "slot owned by another Pod; rejecting Ready so worker retries"
            );
            return false;
        }
        Err(e) => {
            tracing::error!(target: "weft_dispatcher::ws", "claim_slot: {e}");
            return false;
        }
    }

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
                        Slot::Live { .. } | Slot::StalledGrace { .. } => VecDeque::new(),
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
    // Same slot-ownership check as handle_ready. If we don't own
    // the lease, drop the socket and let the worker retry.
    match crate::routing::route_for_color(state, color).await {
        Ok(crate::routing::ColorRoute::Local) => {}
        Ok(crate::routing::ColorRoute::Forward { owner_pod_id }) => {
            tracing::info!(
                target: "weft_dispatcher::ws",
                %color, owner = %owner_pod_id,
                "slot owned by another Pod; rejecting Reconnected"
            );
            return false;
        }
        Err(e) => {
            tracing::error!(target: "weft_dispatcher::ws", "claim_slot on reconnect: {e}");
            return false;
        }
    }

    let expected = worker_instance_id.to_string();
    // Fast path: same Pod, transient drop. Slot is in
    // WaitingReconnect with matching instance_id; preserve the
    // worker's in-memory state by sending Start { Resume,
    // snapshot: None }.
    let same_pod_fast_path = state
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

    if same_pod_fast_path {
        let resume = DispatcherToWorker::Start {
            wake: WakeMessage::Resume,
            snapshot: None,
            worker_instance_id: None,
        };
        return tx.send(resume).await.is_ok();
    }

    // Slow path: cross-Pod adoption (or stale slot). The journal
    // is the source of truth; fold it and ship a Start with the
    // full snapshot. The worker reconciles its in-memory state on
    // the spot. If anything was lost during the outage we can't
    // recover it, but the journal IS the contract.
    tracing::info!(
        target: "weft_dispatcher::ws",
        %color, instance = %worker_instance_id,
        "adopting reconnected worker (cross-Pod or stale slot path)"
    );

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
    state
        .slots
        .with_slot(color, {
            let tx = tx.clone();
            let expected = expected.clone();
            move |slot| {
                Box::pin(async move {
                    *slot = Slot::Live {
                        sender: tx,
                        worker_instance_id: expected,
                    };
                })
            }
        })
        .await;
    let resume = DispatcherToWorker::Start {
        wake: WakeMessage::Resume,
        snapshot: folded_snapshot,
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
    if state.projects.get(project_uuid).await.is_none() {
        state.slots.drop_slot(color).await;
        return;
    }

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

    let wake = crate::backend::WakeContext::resolve(state, project_id.clone(), color);
    let spawn_result = state.workers.spawn_worker(wake).await;
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
            match handle_signal_register(
                state, color, &node_id, &lane, &spec, /*is_resume=*/ true,
            )
            .await
            {
                Ok((token, user_url)) => {
                    let _ = tx
                        .send(DispatcherToWorker::SuspensionToken {
                            request_id,
                            token,
                            user_url,
                            error: None,
                        })
                        .await;
                }
                Err(err) => {
                    tracing::error!(
                        target: "weft_dispatcher::ws",
                        %color, error = %err,
                        "suspension register failed; replying with error"
                    );
                    let _ = tx
                        .send(DispatcherToWorker::SuspensionToken {
                            request_id,
                            token: String::new(),
                            user_url: None,
                            error: Some(err),
                        })
                        .await;
                }
            }
            true
        }
        WorkerToDispatcher::RegisterSignalRequest { request_id, node_id, spec } => {
            match handle_signal_register(
                state, color, &node_id, &Default::default(), &spec, /*is_resume=*/ false,
            )
            .await
            {
                Ok((token, user_url)) => {
                    let _ = tx
                        .send(DispatcherToWorker::RegisterSignalAck {
                            request_id,
                            token,
                            user_url,
                            error: None,
                        })
                        .await;
                }
                Err(err) => {
                    tracing::error!(
                        target: "weft_dispatcher::ws",
                        %color, error = %err,
                        "entry signal register failed; replying with error"
                    );
                    let _ = tx
                        .send(DispatcherToWorker::RegisterSignalAck {
                            request_id,
                            token: String::new(),
                            user_url: None,
                            error: Some(err),
                        })
                        .await;
                }
            }
            true
        }
        WorkerToDispatcher::ProvisionSidecarRequest { request_id, node_id, spec } => {
            let project_id = match state.journal.execution_project(color).await {
                Ok(Some(p)) => p,
                _ => String::new(),
            };
            let reply = if project_id.is_empty() {
                DispatcherToWorker::ProvisionSidecarReply {
                    request_id,
                    instance_id: None,
                    endpoint_url: None,
                    error: Some("no project id for execution".into()),
                }
            } else {
                // If already running, skip provisioning and hand
                // back the existing handle. Mirrors v1's "idempotent
                // provision on restart" behavior.
                if let Some(existing) =
                    state.infra_registry.handle_if_running(&project_id, &node_id)
                {
                    DispatcherToWorker::ProvisionSidecarReply {
                        request_id,
                        instance_id: Some(existing.id.clone()),
                        endpoint_url: existing.endpoint_url.clone(),
                        error: None,
                    }
                } else {
                    let tenant = state.tenant_router.tenant_for_project(&project_id);
                    let namespace = state.namespace_mapper.namespace_for(&tenant);
                    let infra_spec = crate::backend::InfraSpec {
                        project_id: project_id.clone(),
                        infra_node_id: node_id.clone(),
                        sidecar: spec,
                        config: serde_json::Value::Null,
                        tenant: tenant.to_string(),
                        namespace,
                    };
                    match state.infra.provision(infra_spec).await {
                        Ok(handle) => {
                            state.infra_registry.insert_running(
                                project_id.clone(),
                                node_id.clone(),
                                handle.clone(),
                            );
                            DispatcherToWorker::ProvisionSidecarReply {
                                request_id,
                                instance_id: Some(handle.id.clone()),
                                endpoint_url: handle.endpoint_url.clone(),
                                error: None,
                            }
                        }
                        Err(e) => DispatcherToWorker::ProvisionSidecarReply {
                            request_id,
                            instance_id: None,
                            endpoint_url: None,
                            error: Some(format!("{e}")),
                        },
                    }
                }
            };
            let _ = tx.send(reply).await;
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
            let _ = tx.send(DispatcherToWorker::StalledAck).await;

            // If a fire raced the stall (in journal but not consumed
            // by this worker), forward it now via Deliver so the
            // worker re-enters drive() instead of dying. Otherwise
            // start the grace timer.
            if has_pending_deliveries(state, color).await {
                forward_pending_to_worker(state, color, tx).await;
                // Stay Live: the worker just got a Deliver and is
                // about to re-enter the loop.
                return true;
            }

            let grace_secs = state.workers.idle_grace_seconds();
            if grace_secs == 0 {
                // No grace: send Exit immediately. Worker closes WS.
                let _ = tx.send(DispatcherToWorker::Exit).await;
                state
                    .slots
                    .with_slot(color, move |slot| {
                        Box::pin(async move {
                            *slot = Slot::Idle { queued: VecDeque::new() };
                        })
                    })
                    .await;
                return true;
            }

            // Park the slot in StalledGrace. Spawn a watchdog that
            // sends Exit after grace_secs unless the slot already
            // moved (Deliver promoted it back to Live).
            let grace_until = std::time::Instant::now()
                + std::time::Duration::from_secs(grace_secs);
            let live_state = state
                .slots
                .with_slot(color, {
                    let tx = tx.clone();
                    move |slot| {
                        let tx = tx.clone();
                        Box::pin(async move {
                            match std::mem::replace(
                                slot,
                                Slot::Idle { queued: VecDeque::new() },
                            ) {
                                Slot::Live { sender, worker_instance_id } => {
                                    *slot = Slot::StalledGrace {
                                        sender: tx,
                                        worker: None,
                                        worker_instance_id,
                                        grace_until,
                                    };
                                    let _ = sender;
                                    Some(())
                                }
                                other => {
                                    *slot = other;
                                    None
                                }
                            }
                        })
                    }
                })
                .await;
            if live_state.is_none() {
                tracing::warn!(
                    target: "weft_dispatcher::ws",
                    %color, "Stalled while slot was not Live; ignoring"
                );
                return true;
            }

            let state_clone = state.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(grace_secs)).await;
                expire_stall_grace(&state_clone, color).await;
            });

            true
        }
        WorkerToDispatcher::Completed { outputs } => {
            state.slots.drop_slot(color).await;
            cleanup_execution_signals(state, color).await;
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
            cleanup_execution_signals(state, color).await;
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
        WorkerToDispatcher::NodeStarted {
            node_id,
            lane,
            input,
            pulses_absorbed,
        } => {
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::NodeStarted {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct,
                    input: input.clone(),
                    pulses_absorbed,
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeStarted {
                    color,
                    node: node_id,
                    lane,
                    input,
                    project_id,
                })
                .await;
            true
        }
        WorkerToDispatcher::NodeSuspended { node_id, lane, token } => {
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::NodeSuspended {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct,
                    token: token.clone(),
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeSuspended {
                    color,
                    node: node_id,
                    lane,
                    token,
                    project_id,
                })
                .await;
            true
        }
        WorkerToDispatcher::NodeResumed { node_id, lane, token, value } => {
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::NodeResumed {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct,
                    token: token.clone(),
                    value: value.clone(),
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeResumed {
                    color,
                    node: node_id,
                    lane,
                    token,
                    value,
                    project_id,
                })
                .await;
            true
        }
        WorkerToDispatcher::NodeRetried { node_id, lane, reason } => {
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::NodeRetried {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct,
                    reason: reason.clone(),
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeRetried {
                    color,
                    node: node_id,
                    lane,
                    reason,
                    project_id,
                })
                .await;
            true
        }
        WorkerToDispatcher::NodeCompleted { node_id, lane, output } => {
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::NodeCompleted {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct,
                    output: output.clone(),
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeCompleted {
                    color,
                    node: node_id,
                    lane,
                    output,
                    project_id,
                })
                .await;
            true
        }
        WorkerToDispatcher::NodeFailed { node_id, lane, error } => {
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::NodeFailed {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct,
                    error: error.clone(),
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeFailed {
                    color,
                    node: node_id,
                    lane,
                    error,
                    project_id,
                })
                .await;
            true
        }
        WorkerToDispatcher::NodeSkipped { node_id, lane } => {
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::NodeSkipped {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct,
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeSkipped {
                    color,
                    node: node_id,
                    lane,
                    project_id,
                })
                .await;
            true
        }
        WorkerToDispatcher::NodeCancelled { node_id, lane, reason } => {
            let lane_struct: weft_core::lane::Lane =
                serde_json::from_str(&lane).unwrap_or_default();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::NodeCancelled {
                    color,
                    node_id: node_id.clone(),
                    lane: lane_struct,
                    reason: reason.clone(),
                    at_unix: now_unix(),
                })
                .await;
            let project_id = project_of(state, color).await.unwrap_or_default();
            state
                .events
                .publish(DispatcherEvent::NodeCancelled {
                    color,
                    node: node_id,
                    lane,
                    reason,
                    project_id,
                })
                .await;
            true
        }
        WorkerToDispatcher::PulsesEmitted { pulses } => {
            // One journal entry per produced pulse, with the
            // engine-minted `pulse_id`. Replay reconstructs each
            // pulse with the same UUID; downstream `NodeStarted`
            // events that absorbed these pulses match by UUID.
            for ep in pulses {
                let _ = state
                    .journal
                    .record_event(&crate::journal::ExecEvent::PulseEmitted {
                        color,
                        pulse_id: ep.pulse_id,
                        source_node: ep.source_node,
                        source_port: ep.source_port,
                        target_node: ep.target_node,
                        target_port: ep.target_port,
                        lane: ep.lane,
                        value: ep.value,
                        at_unix: now_unix(),
                    })
                    .await;
            }
            true
        }
        WorkerToDispatcher::PulsesExpanded {
            node_id,
            port,
            absorbed_pulse_id,
            color: _,
            base_lane,
            children,
        } => {
            let children = children
                .into_iter()
                .map(|c| crate::journal::ExpandedChildRecord {
                    pulse_id: c.pulse_id,
                    lane_suffix: c.lane_suffix,
                    value: c.value,
                })
                .collect();
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::PulsesExpanded {
                    color,
                    node_id,
                    port,
                    absorbed_pulse_id,
                    base_lane,
                    children,
                    at_unix: now_unix(),
                })
                .await;
            true
        }
        WorkerToDispatcher::PulsesGathered {
            node_id,
            port,
            absorbed_pulse_ids,
            color: _,
            parent_lane,
            pulse_id,
            value,
        } => {
            let _ = state
                .journal
                .record_event(&crate::journal::ExecEvent::PulsesGathered {
                    color,
                    node_id,
                    port,
                    absorbed_pulse_ids,
                    parent_lane,
                    pulse_id,
                    value,
                    at_unix: now_unix(),
                })
                .await;
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


fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn project_of(state: &DispatcherState, color: Color) -> Option<String> {
    state.journal.execution_project(color).await.ok().flatten()
}

/// Tear down every wake-signal registration tied to a terminal
/// execution. Removes the signal rows from the journal, asks the
/// listener to unregister each token (so the per-tenant listener
/// can exit when its registry hits zero), and consumes the
/// matching suspensions. Mirrors the cancel path's cleanup
/// (`api::execution::cancel`); without this, an execution that
/// ends with active suspensions leaves orphan rows in the
/// `signal` table, which then keeps the listener alive forever
/// (the `/listener/empty` race-guard counts journal rows).
async fn cleanup_execution_signals(state: &DispatcherState, color: Color) {
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

/// Drain any pending suspension fires sitting in the journal but
/// not yet consumed, and forward each as a `Deliver` to the worker
/// over `tx`. Used by the Stalled handler when fires raced the
/// stall: instead of killing-and-respawning, we hand the new
/// values to the worker so it can re-enter `drive()`.
async fn forward_pending_to_worker(
    state: &DispatcherState,
    color: Color,
    tx: &mpsc::Sender<DispatcherToWorker>,
) {
    let Ok(events) = state.journal.events_log(color).await else {
        return;
    };
    let snap = crate::journal::fold_to_snapshot(color, &events);
    for (token, value) in snap.pending_deliveries {
        let _ = tx
            .send(DispatcherToWorker::Deliver(
                weft_core::primitive::Delivery { token, value },
            ))
            .await;
    }
}

/// Watchdog body: when the grace timer expires, send `Exit` to the
/// worker if the slot is still `StalledGrace`, then transition to
/// `Idle`. If the slot moved (Deliver promoted it back to Live, or
/// the worker exited on its own), this is a no-op.
async fn expire_stall_grace(state: &DispatcherState, color: Color) {
    let action = state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                let taken = std::mem::replace(slot, Slot::Idle { queued: VecDeque::new() });
                match taken {
                    Slot::StalledGrace { sender, worker_instance_id, .. } => {
                        // Leave slot Idle.
                        Some((sender, worker_instance_id))
                    }
                    other => {
                        *slot = other;
                        None
                    }
                }
            })
        })
        .await;
    if let Some((sender, instance)) = action {
        tracing::info!(
            target: "weft_dispatcher::ws",
            %color, instance = %instance,
            "stall grace expired; sending Exit"
        );
        let _ = sender.send(DispatcherToWorker::Exit).await;
    }
}

/// Shared body for SuspensionRequest and RegisterSignalRequest:
/// resolve tenant + ensure listener (lazy spawn), call /register
/// on the listener, journal the registration (resume only),
/// insert a SignalTracker entry. Returns `(token, user_url)` on
/// success or an error message the caller surfaces to the worker.
async fn handle_signal_register(
    state: &DispatcherState,
    color: Color,
    node_id: &str,
    lane: &weft_core::lane::Lane,
    spec: &weft_core::primitive::WakeSignalSpec,
    is_resume: bool,
) -> Result<(String, Option<String>), String> {
    let project_id = match state.journal.execution_project(color).await {
        Ok(Some(p)) => p,
        Ok(None) => return Err("no project for color".into()),
        Err(e) => return Err(format!("journal: {e}")),
    };
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let namespace = state.namespace_mapper.namespace_for(&tenant);
    let dispatcher_url = state.config.cluster_dispatcher_url();
    let deploy_name = crate::listener::deploy_name_for_tenant(tenant.as_str());
    let listener = state
        .listeners
        .ensure(
            &tenant,
            &namespace,
            &dispatcher_url,
            state.listener_backend.as_ref(),
            &state.pg_pool,
            &deploy_name,
            state.pod_id.as_str(),
        )
        .await
        .map_err(|e| format!("listener spawn failed: {e}"))?;

    let token = uuid::Uuid::new_v4().to_string();
    let user_url = crate::listener::register_signal(&listener, &token, spec, node_id)
        .await
        .map_err(|e| format!("listener register failed: {e}"))?;

    if is_resume {
        let metadata = serde_json::json!({
            "spec": spec,
            "node_id": node_id,
            "lane": lane,
        });
        state
            .journal
            .record_suspension_with_token(&token, color, node_id, metadata)
            .await
            .map_err(|e| format!("record_suspension: {e}"))?;
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::SuspensionRegistered {
                color,
                node_id: node_id.to_string(),
                lane: lane.clone(),
                token: token.clone(),
                spec: spec.clone(),
                at_unix: now_unix(),
            })
            .await;
    }

    let kind_label = kind_of(&spec.kind);
    let spec_json = serde_json::to_string(spec)
        .map_err(|e| format!("serialize spec: {e}"))?;
    state
        .journal
        .signal_insert(&crate::journal::SignalRegistration {
            token: token.clone(),
            tenant_id: tenant.to_string(),
            project_id,
            color: if is_resume { Some(color) } else { None },
            node_id: node_id.to_string(),
            is_resume,
            user_url: user_url.clone(),
            kind: kind_label.to_string(),
            spec_json,
        })
        .await
        .map_err(|e| format!("signal_insert: {e}"))?;

    Ok((token, user_url))
}
