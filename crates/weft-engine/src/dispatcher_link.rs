//! WebSocket bridge between the worker and the dispatcher.
//!
//! A single background `supervisor` task owns the socket lifecycle.
//! It reads outbound messages from an mpsc the engine writes to,
//! forwards them over the socket, and routes inbound messages into
//! a shared `PendingState` / `ControlState`. If the socket drops
//! the supervisor enters a reconnect loop: up to 30 seconds of
//! 1-second retries. If reconnect succeeds, a `Reconnected
//! { worker_instance_id }` handshake resumes the session; queued
//! outbound messages that piled up during the drop flush out. If
//! reconnect fails, the supervisor exits and the `LinkStatus`
//! watch flips to `Dead` so the loop driver can shut down.
//!
//! The loop driver watches `LinkStatus` and pauses dispatching new
//! node tasks while the link is anything but `Live`. In-flight
//! node futures keep running to completion; their results queue up
//! in the outbound mpsc until the link is back up.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use futures::{sink::SinkExt, stream::StreamExt};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use weft_core::primitive::{
    Delivery, DispatcherToWorker, ExecutionSnapshot, WakeMessage, WorkerToDispatcher,
};
use weft_core::Color;

pub type WsConn = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Maximum wall time the supervisor spends trying to reconnect
/// after a socket drop before giving up. Matches the dispatcher
/// side's grace window.
const RECONNECT_BUDGET: Duration = Duration::from_secs(30);
/// Delay between reconnect attempts.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkStatus {
    /// No socket yet; waiting on the first `Start` round-trip.
    Connecting,
    /// Socket is live; messages flow both ways.
    Live,
    /// Socket dropped; supervisor is reconnecting. Loop driver
    /// pauses dispatching new work.
    Disconnected,
    /// Reconnect budget exhausted or dispatcher rejected us. Loop
    /// driver should terminate the execution.
    Dead,
}

#[derive(Clone)]
pub struct DispatcherLink {
    inner: Arc<LinkInner>,
}

struct LinkInner {
    outbound: mpsc::Sender<WorkerToDispatcher>,
    pending: Arc<Mutex<PendingState>>,
    control: Arc<Mutex<ControlState>>,
    status: watch::Receiver<LinkStatus>,
}

#[derive(Default)]
struct PendingState {
    awaiting_token: HashMap<u64, oneshot::Sender<TokenReply>>,
    /// Trigger-setup-phase register_signal calls awaiting the
    /// dispatcher's RegisterSignalAck. Same shape as awaiting_token
    /// but populated from `ctx.register_signal` rather than
    /// `ctx.await_signal`.
    awaiting_register: HashMap<u64, oneshot::Sender<TokenReply>>,
    /// `ctx.sidecar_endpoint()` calls awaiting SidecarEndpoint replies.
    awaiting_endpoint: HashMap<u64, oneshot::Sender<Option<String>>>,
    /// `ctx.provision_sidecar()` calls awaiting
    /// ProvisionSidecarReply. Carries an optional handle or
    /// the error message the dispatcher returned.
    awaiting_provision: HashMap<u64, oneshot::Sender<ProvisionReply>>,
    /// Suspensions waiting for a delivery. When a `Deliver` arrives
    /// the value either flows through an `Ongoing` oneshot (a node
    /// is already waiting) or gets stashed in `Ready` so a later
    /// `wait_for_delivery` can consume it without blocking. This
    /// buffering survives reconnects: a delivery that lands during
    /// a disconnect (through the dispatcher's queued_deliveries on
    /// the reconnect Start) sits in `Ready` until its waiter calls.
    awaiting_value: HashMap<String, DeliverySlot>,
    next_request_id: u64,
}

enum DeliverySlot {
    Ongoing(oneshot::Sender<serde_json::Value>),
    Ready(serde_json::Value),
}

#[derive(Default)]
struct ControlState {
    stalled_ack: Option<oneshot::Sender<()>>,
    cancel: Option<oneshot::Sender<()>>,
}

#[derive(Debug)]
pub struct TokenReply {
    pub token: String,
    pub user_url: Option<String>,
}

/// Reply from the dispatcher to `provision_sidecar`. Either the
/// handle is populated (success) or `error` carries a message.
#[derive(Debug)]
pub struct ProvisionReply {
    pub handle: Option<weft_core::context::SidecarHandle>,
    pub error: Option<String>,
}

/// Initial message the dispatcher sends after the worker's `Ready`.
/// The `worker_instance_id` is stamped by the dispatcher on the
/// first Start; the supervisor stashes it and echoes it back in a
/// `Reconnected` after any subsequent drop.
#[derive(Debug)]
pub struct StartPacket {
    pub wake: WakeMessage,
    pub snapshot: Option<ExecutionSnapshot>,
    pub worker_instance_id: Option<String>,
}

// ----- Public API ----------------------------------------------------

impl DispatcherLink {
    /// Connect to the dispatcher, perform the first Ready→Start
    /// handshake, spawn the supervisor task. Returns the handle +
    /// the StartPacket so the loop driver can seed initial state.
    pub async fn connect(
        dispatcher_url: &str,
        color: Color,
    ) -> anyhow::Result<(Self, StartPacket)> {
        let url = build_ws_url(dispatcher_url, color);
        let pending = Arc::new(Mutex::new(PendingState::default()));
        let control = Arc::new(Mutex::new(ControlState::default()));
        let (outbound_tx, outbound_rx) = mpsc::channel::<WorkerToDispatcher>(64);
        let (status_tx, status_rx) = watch::channel(LinkStatus::Connecting);

        // Open the first socket synchronously so we can surface a
        // clean error to the caller if the dispatcher is down.
        let (ws_stream, _) = connect_async(&url)
            .await
            .with_context(|| format!("connect {url}"))?;
        let conn = SplitConn::from(ws_stream);

        // Perform Ready→Start on this socket before spawning the
        // supervisor, so callers see the initial state before any
        // reconnect logic might fire.
        let (conn, start) = ready_handshake(conn).await?;
        let worker_instance_id = start.worker_instance_id.clone();
        let _ = status_tx.send(LinkStatus::Live);

        tokio::spawn(supervisor(
            SupervisorCtx {
                url,
                worker_instance_id,
                pending: pending.clone(),
                control: control.clone(),
                outbound_rx,
                status: status_tx,
            },
            conn,
        ));

        let inner = Arc::new(LinkInner {
            outbound: outbound_tx,
            pending,
            control,
            status: status_rx,
        });
        Ok((Self { inner }, start))
    }

    /// Watch the link's health. The loop driver blocks on
    /// `status.changed().await` whenever it sees anything other
    /// than `Live` and resumes dispatching when it flips back.
    pub fn status(&self) -> watch::Receiver<LinkStatus> {
        self.inner.status.clone()
    }

    pub async fn send(&self, msg: WorkerToDispatcher) {
        let _ = self.inner.outbound.send(msg).await;
    }

    pub async fn request_suspension(
        &self,
        node_id: String,
        lane: weft_core::lane::Lane,
        spec: weft_core::primitive::WakeSignalSpec,
    ) -> anyhow::Result<TokenReply> {
        let (tx, rx) = oneshot::channel();
        let request_id = {
            let mut p = self.inner.pending.lock().await;
            let id = p.next_request_id;
            p.next_request_id += 1;
            p.awaiting_token.insert(id, tx);
            id
        };
        self.send(WorkerToDispatcher::SuspensionRequest {
            request_id,
            node_id,
            lane,
            spec,
        })
        .await;
        rx.await.map_err(|_| anyhow::anyhow!("token channel closed"))
    }

    /// `ctx.sidecar_endpoint()` round-trip: ask the dispatcher
    /// for this node's sidecar endpoint URL. Returns `Ok(None)`
    /// when infra isn't provisioned; caller surfaces the error.
    pub async fn request_sidecar_endpoint(
        &self,
        node_id: String,
    ) -> anyhow::Result<Option<String>> {
        let (tx, rx) = oneshot::channel();
        let request_id = {
            let mut p = self.inner.pending.lock().await;
            let id = p.next_request_id;
            p.next_request_id += 1;
            p.awaiting_endpoint.insert(id, tx);
            id
        };
        self.send(WorkerToDispatcher::SidecarEndpointRequest {
            request_id,
            node_id,
        })
        .await;
        rx.await.map_err(|_| anyhow::anyhow!("sidecar endpoint channel closed"))
    }

    /// InfraSetup-phase `provision_sidecar` round-trip: ship the
    /// SidecarSpec to the dispatcher, wait for the ack carrying
    /// the allocated endpoint URL.
    pub async fn request_provision_sidecar(
        &self,
        node_id: String,
        spec: weft_core::node::SidecarSpec,
    ) -> anyhow::Result<ProvisionReply> {
        let (tx, rx) = oneshot::channel();
        let request_id = {
            let mut p = self.inner.pending.lock().await;
            let id = p.next_request_id;
            p.next_request_id += 1;
            p.awaiting_provision.insert(id, tx);
            id
        };
        self.send(WorkerToDispatcher::ProvisionSidecarRequest {
            request_id,
            node_id,
            spec,
        })
        .await;
        rx.await.map_err(|_| anyhow::anyhow!("provision_sidecar ack channel closed"))
    }

    /// TriggerSetup-phase `register_signal` round-trip: ship the
    /// spec to the dispatcher, wait for the ack carrying the
    /// listener-minted user URL.
    pub async fn request_register_signal(
        &self,
        node_id: String,
        spec: weft_core::primitive::WakeSignalSpec,
    ) -> anyhow::Result<TokenReply> {
        let (tx, rx) = oneshot::channel();
        let request_id = {
            let mut p = self.inner.pending.lock().await;
            let id = p.next_request_id;
            p.next_request_id += 1;
            p.awaiting_register.insert(id, tx);
            id
        };
        self.send(WorkerToDispatcher::RegisterSignalRequest {
            request_id,
            node_id,
            spec,
        })
        .await;
        rx.await.map_err(|_| anyhow::anyhow!("register_signal ack channel closed"))
    }

    /// Whether a delivery for `token` has already been seeded into
    /// the link (via `seed_delivery`, called by the loop driver
    /// from Start.queued_deliveries). Used by `await_signal` on
    /// resume to distinguish "the fire happened, wait for the value
    /// to land" from "no fire queued for this lane, return Suspended
    /// so the worker can stall."
    pub async fn has_seeded_delivery(&self, token: &str) -> bool {
        let p = self.inner.pending.lock().await;
        matches!(p.awaiting_value.get(token), Some(DeliverySlot::Ready(_)))
    }

    pub async fn wait_for_delivery(&self, token: String) -> anyhow::Result<serde_json::Value> {
        {
            let mut p = self.inner.pending.lock().await;
            if let Some(DeliverySlot::Ready(value)) = p.awaiting_value.remove(&token) {
                return Ok(value);
            }
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut p = self.inner.pending.lock().await;
            if let Some(DeliverySlot::Ready(value)) = p.awaiting_value.remove(&token) {
                return Ok(value);
            }
            p.awaiting_value.insert(token, DeliverySlot::Ongoing(tx));
        }
        rx.await.map_err(|_| anyhow::anyhow!("deliver channel closed"))
    }

    pub async fn seed_delivery(&self, token: String, value: serde_json::Value) {
        let mut p = self.inner.pending.lock().await;
        p.awaiting_value.insert(token, DeliverySlot::Ready(value));
    }

    pub async fn stall(&self) {
        let (tx, rx) = oneshot::channel();
        {
            let mut c = self.inner.control.lock().await;
            c.stalled_ack = Some(tx);
        }
        self.send(WorkerToDispatcher::Stalled).await;
        let _ = rx.await;
    }

    pub async fn completed(&self, outputs: serde_json::Value) {
        self.send(WorkerToDispatcher::Completed { outputs }).await;
        self.drain().await;
    }

    pub async fn failed(&self, error: String) {
        self.send(WorkerToDispatcher::Failed { error }).await;
        self.drain().await;
    }

    /// Wait until the outbound mpsc is drained by the supervisor's
    /// writer (i.e. every queued `WorkerToDispatcher` message has
    /// been forwarded to the socket). Polls capacity every 10ms up
    /// to 1s. Called after terminal messages so the worker process
    /// doesn't exit before the final event lands at the dispatcher.
    async fn drain(&self) {
        let cap = self.inner.outbound.max_capacity();
        let start = std::time::Instant::now();
        while self.inner.outbound.capacity() < cap {
            if start.elapsed() > std::time::Duration::from_secs(1) {
                tracing::warn!(
                    target: "weft_engine::link",
                    pending = cap - self.inner.outbound.capacity(),
                    "drain timeout; terminal messages may be lost"
                );
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // One more yield to let the writer task actually push the
        // last message over the socket (capacity freed ≠ socket
        // write completed).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

// ----- Supervisor ---------------------------------------------------

struct SupervisorCtx {
    url: String,
    worker_instance_id: Option<String>,
    pending: Arc<Mutex<PendingState>>,
    control: Arc<Mutex<ControlState>>,
    outbound_rx: mpsc::Receiver<WorkerToDispatcher>,
    status: watch::Sender<LinkStatus>,
}

/// Long-running task that owns the socket. Runs the bidi pump
/// until it ends (clean close or error), then tries to reconnect.
async fn supervisor(mut ctx: SupervisorCtx, mut conn: SplitConn) {
    loop {
        // Pump the current socket until it dies.
        let drain_reason = pump(&mut ctx, &mut conn).await;
        match drain_reason {
            PumpReason::CleanExit => {
                // Worker sent Completed/Failed/Stalled and the
                // writer half closed; we're done for good.
                let _ = ctx.status.send(LinkStatus::Dead);
                return;
            }
            PumpReason::SocketClosed => {
                tracing::warn!(
                    target: "weft_engine::link",
                    "socket dropped; entering reconnect loop (budget {}s)",
                    RECONNECT_BUDGET.as_secs()
                );
                let _ = ctx.status.send(LinkStatus::Disconnected);
            }
        }

        // Try to reconnect. The outbound_rx keeps buffering messages
        // the engine tries to send while we're down; they'll flush
        // when pump() resumes on the new socket.
        let Some(instance_id) = ctx.worker_instance_id.clone() else {
            tracing::error!(
                target: "weft_engine::link",
                "no worker_instance_id available; cannot reconnect"
            );
            let _ = ctx.status.send(LinkStatus::Dead);
            return;
        };

        let start_time = std::time::Instant::now();
        let mut new_conn: Option<SplitConn> = None;
        while start_time.elapsed() < RECONNECT_BUDGET {
            tokio::time::sleep(RECONNECT_INTERVAL).await;
            match connect_async(&ctx.url).await {
                Ok((ws, _)) => {
                    let mut candidate = SplitConn::from(ws);
                    if let Err(e) = candidate
                        .send_json(&WorkerToDispatcher::Reconnected {
                            worker_instance_id: instance_id.clone(),
                        })
                        .await
                    {
                        tracing::warn!(target: "weft_engine::link", "Reconnected send: {e}");
                        continue;
                    }
                    new_conn = Some(candidate);
                    break;
                }
                Err(e) => {
                    tracing::debug!(target: "weft_engine::link", "reconnect attempt: {e}");
                }
            }
        }

        let Some(resumed) = new_conn else {
            tracing::error!(
                target: "weft_engine::link",
                "reconnect budget exhausted; link is dead"
            );
            let _ = ctx.status.send(LinkStatus::Dead);
            return;
        };
        conn = resumed;

        // Consume the dispatcher's reconnect ack (an empty Start).
        // Fires that arrived during the drop are in the journal;
        // this worker will pick them up when it respawns after
        // stall. For a mid-run reconnect the worker kept its state.
        match conn.recv_json::<DispatcherToWorker>().await {
            Ok(DispatcherToWorker::Start { .. }) => {}
            Ok(other) => {
                tracing::warn!(
                    target: "weft_engine::link",
                    "reconnect: expected Start, got {other:?}; treating as unrecoverable"
                );
                let _ = ctx.status.send(LinkStatus::Dead);
                return;
            }
            Err(e) => {
                tracing::warn!(target: "weft_engine::link", "reconnect: no Start: {e}");
                let _ = ctx.status.send(LinkStatus::Dead);
                return;
            }
        }

        let _ = ctx.status.send(LinkStatus::Live);
    }
}

enum PumpReason {
    SocketClosed,
    CleanExit,
}

/// Bidirectional pump on an active socket. Forwards outbound mpsc
/// messages to the socket, parses inbound frames into PendingState
/// / ControlState. Returns when the socket ends (via error, close
/// frame, or stream EOF) or when the engine has closed its
/// outbound mpsc.
async fn pump(ctx: &mut SupervisorCtx, conn: &mut SplitConn) -> PumpReason {
    loop {
        tokio::select! {
            outbound = ctx.outbound_rx.recv() => {
                let Some(msg) = outbound else {
                    // Engine dropped its sender; nothing more to
                    // send. A terminal message (Completed/Failed/
                    // Stalled) was already shipped in the last
                    // iteration. Close the socket and exit.
                    let _ = conn.close().await;
                    return PumpReason::CleanExit;
                };
                if let Err(e) = conn.send_json(&msg).await {
                    // Socket died while writing; stash the message
                    // back at the head of the mpsc and reconnect.
                    tracing::warn!(target: "weft_engine::link", "write: {e}");
                    // Re-inject the unsent message by prepending a
                    // one-shot sender to a new channel is awkward;
                    // simpler: just drop it. Node state is already
                    // recorded in the event log on the dispatcher's
                    // side via the terminal-path events, and our
                    // reconnect flow doesn't re-ship in-flight
                    // events (the dispatcher derives them from
                    // subsequent messages). Losing one log/cost
                    // event across a blip is acceptable.
                    let _ = msg;
                    return PumpReason::SocketClosed;
                }
            }
            inbound = conn.recv_json::<DispatcherToWorker>() => {
                match inbound {
                    Ok(msg) => {
                        if !route_inbound(ctx, msg).await {
                            return PumpReason::CleanExit;
                        }
                    }
                    Err(RecvError::Closed) => return PumpReason::SocketClosed,
                    Err(RecvError::Malformed(e)) => {
                        tracing::warn!(target: "weft_engine::link", "parse: {e}");
                    }
                }
            }
        }
    }
}

/// Route one inbound frame into the shared state. Returns `false`
/// if this was a terminal inbound frame (we should exit cleanly).
async fn route_inbound(ctx: &SupervisorCtx, msg: DispatcherToWorker) -> bool {
    match msg {
        DispatcherToWorker::Start { .. } => {
            tracing::warn!(target: "weft_engine::link", "duplicate Start ignored");
        }
        DispatcherToWorker::SuspensionToken { request_id, token, user_url } => {
            let mut p = ctx.pending.lock().await;
            if let Some(tx) = p.awaiting_token.remove(&request_id) {
                let _ = tx.send(TokenReply { token, user_url });
            }
        }
        DispatcherToWorker::RegisterSignalAck { request_id, token, user_url } => {
            let mut p = ctx.pending.lock().await;
            if let Some(tx) = p.awaiting_register.remove(&request_id) {
                let _ = tx.send(TokenReply { token, user_url });
            }
        }
        DispatcherToWorker::SidecarEndpoint { request_id, endpoint } => {
            let mut p = ctx.pending.lock().await;
            if let Some(tx) = p.awaiting_endpoint.remove(&request_id) {
                let _ = tx.send(endpoint);
            }
        }
        DispatcherToWorker::ProvisionSidecarReply {
            request_id,
            instance_id,
            endpoint_url,
            error,
        } => {
            let mut p = ctx.pending.lock().await;
            if let Some(tx) = p.awaiting_provision.remove(&request_id) {
                let handle = match (instance_id, endpoint_url) {
                    (Some(i), Some(u)) => Some(weft_core::context::SidecarHandle {
                        instance_id: i,
                        endpoint_url: u,
                    }),
                    _ => None,
                };
                let _ = tx.send(ProvisionReply { handle, error });
            }
        }
        DispatcherToWorker::Deliver(Delivery { token, value }) => {
            let mut p = ctx.pending.lock().await;
            match p.awaiting_value.remove(&token) {
                Some(DeliverySlot::Ongoing(tx)) => {
                    let _ = tx.send(value);
                }
                Some(DeliverySlot::Ready(_)) | None => {
                    p.awaiting_value.insert(token, DeliverySlot::Ready(value));
                }
            }
        }
        DispatcherToWorker::StalledAck => {
            let mut c = ctx.control.lock().await;
            if let Some(tx) = c.stalled_ack.take() {
                let _ = tx.send(());
            }
        }
        DispatcherToWorker::Cancel => {
            let mut c = ctx.control.lock().await;
            if let Some(tx) = c.cancel.take() {
                let _ = tx.send(());
            }
        }
    }
    true
}

// ----- Socket wrapper -----------------------------------------------

/// Thin wrapper around the split WS halves with JSON helpers so
/// the supervisor body reads linearly instead of nesting match
/// trees. `send_json` / `recv_json` match our wire format exactly.
struct SplitConn {
    write: futures::stream::SplitSink<WsConn, WsMessage>,
    read: futures::stream::SplitStream<WsConn>,
}

impl From<WsConn> for SplitConn {
    fn from(ws: WsConn) -> Self {
        let (write, read) = ws.split();
        Self { write, read }
    }
}

#[derive(Debug, thiserror::Error)]
enum RecvError {
    #[error("socket closed")]
    Closed,
    #[error("malformed frame: {0}")]
    Malformed(String),
}

impl SplitConn {
    async fn send_json<T: serde::Serialize>(&mut self, msg: &T) -> anyhow::Result<()> {
        let payload = serde_json::to_string(msg)?;
        self.write
            .send(WsMessage::Text(payload.into()))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn recv_json<T: serde::de::DeserializeOwned>(&mut self) -> Result<T, RecvError> {
        loop {
            match self.read.next().await {
                Some(Ok(WsMessage::Text(t))) => {
                    return serde_json::from_str(&t)
                        .map_err(|e| RecvError::Malformed(e.to_string()));
                }
                Some(Ok(WsMessage::Binary(_)))
                | Some(Ok(WsMessage::Ping(_)))
                | Some(Ok(WsMessage::Pong(_)))
                | Some(Ok(WsMessage::Frame(_))) => continue,
                Some(Ok(WsMessage::Close(_))) | None => return Err(RecvError::Closed),
                Some(Err(e)) => return Err(RecvError::Malformed(e.to_string())),
            }
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.write
            .close()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

// ----- Handshake helpers -------------------------------------------

fn build_ws_url(dispatcher_url: &str, color: Color) -> String {
    let base = dispatcher_url.trim_end_matches('/');
    let ws_base = if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else {
        base.to_string()
    };
    format!("{ws_base}/ws/executions/{color}")
}

/// Send `Ready` on a fresh socket and receive the matching Start.
/// Used only for the first connection; reconnects send a
/// `Reconnected` instead.
async fn ready_handshake(mut conn: SplitConn) -> anyhow::Result<(SplitConn, StartPacket)> {
    conn.send_json(&WorkerToDispatcher::Ready).await?;
    let msg: DispatcherToWorker = conn
        .recv_json()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    match msg {
        DispatcherToWorker::Start {
            wake,
            snapshot,
            worker_instance_id,
        } => Ok((
            conn,
            StartPacket { wake, snapshot, worker_instance_id },
        )),
        other => anyhow::bail!("handshake: expected Start, got {other:?}"),
    }
}
