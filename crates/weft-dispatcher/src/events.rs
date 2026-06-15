//! Pub/sub for project and execution events. Two layers:
//!
//!   - **Per-pod broadcast** (`EventBus`): SSE handlers subscribe;
//!     local publishers push directly. Tokio `broadcast::Sender` keyed
//!     by `project_id`.
//!   - **Cross-pod fanout via Postgres LISTEN/NOTIFY**: a publisher
//!     calls `EventBus::publish`, which (a) pushes locally so this
//!     pod's SSE consumers see it instantly and (b) issues `NOTIFY
//!     weft_dispatcher_events, '<json>'`. A long-lived LISTEN task on
//!     every other pod receives, decodes, and pushes to its own local
//!     broadcast. Use `publish_local` when the caller knows the event
//!     is pod-local (no cross-pod fanout needed).
//!
//! The split is deliberate: ExecEvent flows through `journal_bridge`
//! which polls `exec_event` independently on every pod (so each pod
//! ends up publishing the same events to its local broadcast). The
//! NOTIFY channel only carries the smaller cross-cutting events that
//! don't sit on the journal path: ProjectRegistered, ProjectActivated,
//! ProjectDeactivated, TriggerUrlChanged. These fit inside Postgres
//! NOTIFY's 8000-byte payload cap with room to spare.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::{broadcast, RwLock};

use weft_core::frames::LoopFrames;
use weft_core::Color;

/// LISTEN channel name. Single channel for all cross-pod events;
/// receivers route by `project_id` themselves.
const NOTIFY_CHANNEL: &str = "weft_dispatcher_events";

/// An event the dispatcher publishes about some piece of runtime
/// state changing. Tagged enum so SSE serialization matches the
/// spec in the design doc.
// SYNC: DispatcherEvent <-> extension-vscode/src/execFollower.ts DispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DispatcherEvent {
    ExecutionStarted { color: Color, entry_node: String, project_id: String },
    ExecutionCompleted { color: Color, project_id: String, outputs: serde_json::Value },
    ExecutionFailed { color: Color, project_id: String, error: String },
    ExecutionCancelled { color: Color, project_id: String, reason: String },
    NodeStarted { color: Color, node: String, frames: LoopFrames, input: serde_json::Value, closed_ports: Vec<String>, project_id: String },
    NodeSuspended { color: Color, node: String, frames: LoopFrames, token: String, project_id: String },
    NodeResumed { color: Color, node: String, frames: LoopFrames, token: Option<String>, value: Option<serde_json::Value>, project_id: String },
    NodeCancelled { color: Color, node: String, frames: LoopFrames, reason: String, project_id: String },
    NodeCompleted { color: Color, node: String, frames: LoopFrames, output: serde_json::Value, project_id: String },
    NodeFailed { color: Color, node: String, frames: LoopFrames, error: String, project_id: String },
    NodeSkipped { color: Color, node: String, frames: LoopFrames, closed_ports: Vec<String>, project_id: String },
    /// A node emitted a value whose type is incompatible with the
    /// declared (possibly narrowed) type of `port`. The engine refused
    /// the value and closed the port (downstream sees null); the node did
    /// NOT fail. The extension renders this as a per-port warning.
    PortTypeMismatch { color: Color, node: String, frames: LoopFrames, port: String, expected: String, actual: String, project_id: String },
    /// A loop instance was created at `parent_frames`. The inspector
    /// uses this to render a "Loop opened" marker at the loop's box.
    LoopInstantiated {
        color: Color,
        project_id: String,
        group_id: String,
        parent_frames: LoopFrames,
        iter_count: u32,
        parallel: bool,
    },
    /// An iteration of the loop launched. Inspector renders an
    /// iteration marker at body_frames.
    LoopIterationLaunched {
        color: Color,
        project_id: String,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
    },
    /// LoopOut fired for iteration `index`. The per-port gather /
    /// carry writes ride on the journal but are NOT mirrored to the
    /// inspector stream: the renderer reads the loop's outward emit
    /// (a normal pulse) and the per-iteration body activity, not the
    /// LoopOut firing's raw write map.
    LoopOutFired {
        color: Color,
        project_id: String,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        done_vote: Option<bool>,
    },
    /// The loop terminated outward and emitted its outer outputs.
    LoopTerminated {
        color: Color,
        project_id: String,
        group_id: String,
        parent_frames: LoopFrames,
        reason: weft_core::primitive::LoopTerminationReason,
    },
    CostReported { color: Color, project_id: String, service: String, amount_usd: f64 },
    TriggerUrlChanged { project_id: String, node_id: String, url: String },
    ProjectRegistered { project_id: String, name: String },
    ProjectActivated { project_id: String },
    ProjectDeactivated { project_id: String },
    /// Infra node transitioned between status values. Catch-all for
    /// supervisor-driven state changes the extension renders as a
    /// per-node badge.
    InfraStatusChanged { project_id: String, node_id: String, status: String },
    /// Supervisor declared an infra node flaky; the extension shows
    /// the orange banner with `reason`.
    InfraFlaky { project_id: String, node_id: String, reason: String },
    /// Inverse of InfraFlaky.
    InfraRecovered { project_id: String, node_id: String },
    /// Supervisor finished terminating an infra node; the
    /// `infra_node` row has been deleted.
    InfraTerminated { project_id: String, node_id: String },
    /// Supervisor couldn't parse the project's
    /// `health_protocols_json`. The user's config is broken; the
    /// supervisor fell back to defaults. Surfaced as a banner in
    /// the action bar so the user sees their config didn't take.
    InfraConfigError { project_id: String, error: String },
    /// A bus participant came online. `bus_id` is the channel's uuid
    /// (same one embedded in the bus marker), so the inspector groups
    /// multiple buses cleanly. `offset` is the bus-local position used
    /// to tiebreak same-second entries. `at_unix` is the journal's
    /// stamp so replay renders honest timestamps, not "now".
    BusJoined {
        color: Color,
        project_id: String,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },
    /// A bus participant dropped. Pairs with `BusJoined` for the same
    /// `(bus_id, name)`.
    BusLeft {
        color: Color,
        project_id: String,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },
    /// A `send` landed on a bus. `from` is the registered name of the
    /// `payload` is the tagged `BusPayload` (`Journaled { value }`
    /// for journaled buses, `Ephemeral` for ephemeral). The inspector
    /// renders metadata-only on `Ephemeral` using `payload_byte_size`
    /// and the 8-byte SHA-256 prefix. Mirrors the journal shape so
    /// `Journaled { value: Value::Null }` and `Ephemeral` never
    /// collapse to the same JSON.
    BusMessage {
        color: Color,
        project_id: String,
        bus_id: String,
        offset: u64,
        from: String,
        msg_kind: String,
        payload: weft_core::primitive::BusPayload,
        payload_byte_size: u64,
        #[serde(with = "weft_core::hex_array8")]
        payload_sha256_prefix: [u8; 8],
        at_unix: u64,
    },
    /// The bus was closed. Inspector renders an explicit
    /// `* the bus closed here` marker; replay cursors stop here.
    BusClosed {
        color: Color,
        project_id: String,
        bus_id: String,
        offset: u64,
        at_unix: u64,
    },
    /// Graph-level participation: a node was wired to a bus. Derived
    /// from `PulseEmitted` events whose payload carries a bus marker
    /// on a `Bus` port: both source and target nodes are participants.
    /// `ephemeral` is sniffed from the marker JSON itself (which
    /// encodes the bus's mode) so the inspector can render a mode
    /// badge in the panel header without a separate journal event.
    BusParticipant {
        color: Color,
        project_id: String,
        bus_id: String,
        node_id: String,
        ephemeral: bool,
    },
    /// A journal row could not be applied during fold (corruption).
    /// Surfaced one-shot at replay time per affected row so the
    /// inspector can render a muted "N journal rows corrupted"
    /// line. Not alarming by design: corrupt rows are a real but
    /// rare event the user only investigates if they look.
    JournalCorruption {
        color: Color,
        project_id: String,
        site: weft_core::primitive::CorruptionSite,
        reason: String,
    },
}

impl DispatcherEvent {
    pub fn project_id(&self) -> &str {
        match self {
            Self::ExecutionStarted { project_id, .. }
            | Self::ExecutionCompleted { project_id, .. }
            | Self::ExecutionFailed { project_id, .. }
            | Self::ExecutionCancelled { project_id, .. }
            | Self::NodeStarted { project_id, .. }
            | Self::NodeSuspended { project_id, .. }
            | Self::NodeResumed { project_id, .. }
            | Self::NodeCancelled { project_id, .. }
            | Self::NodeCompleted { project_id, .. }
            | Self::NodeFailed { project_id, .. }
            | Self::NodeSkipped { project_id, .. }
            | Self::PortTypeMismatch { project_id, .. }
            | Self::LoopInstantiated { project_id, .. }
            | Self::LoopIterationLaunched { project_id, .. }
            | Self::LoopOutFired { project_id, .. }
            | Self::LoopTerminated { project_id, .. }
            | Self::CostReported { project_id, .. }
            | Self::TriggerUrlChanged { project_id, .. }
            | Self::ProjectRegistered { project_id, .. }
            | Self::ProjectActivated { project_id }
            | Self::ProjectDeactivated { project_id }
            | Self::InfraStatusChanged { project_id, .. }
            | Self::InfraFlaky { project_id, .. }
            | Self::InfraRecovered { project_id, .. }
            | Self::InfraTerminated { project_id, .. }
            | Self::InfraConfigError { project_id, .. }
            | Self::BusJoined { project_id, .. }
            | Self::BusLeft { project_id, .. }
            | Self::BusMessage { project_id, .. }
            | Self::BusClosed { project_id, .. }
            | Self::BusParticipant { project_id, .. }
            | Self::JournalCorruption { project_id, .. } => project_id,
        }
    }

    pub fn color(&self) -> Option<Color> {
        match self {
            Self::ExecutionStarted { color, .. }
            | Self::ExecutionCompleted { color, .. }
            | Self::ExecutionFailed { color, .. }
            | Self::ExecutionCancelled { color, .. }
            | Self::NodeStarted { color, .. }
            | Self::NodeSuspended { color, .. }
            | Self::NodeResumed { color, .. }
            | Self::NodeCancelled { color, .. }
            | Self::NodeCompleted { color, .. }
            | Self::NodeFailed { color, .. }
            | Self::NodeSkipped { color, .. }
            | Self::PortTypeMismatch { color, .. }
            | Self::LoopInstantiated { color, .. }
            | Self::LoopIterationLaunched { color, .. }
            | Self::LoopOutFired { color, .. }
            | Self::LoopTerminated { color, .. }
            | Self::CostReported { color, .. }
            | Self::BusJoined { color, .. }
            | Self::BusLeft { color, .. }
            | Self::BusMessage { color, .. }
            | Self::BusClosed { color, .. }
            | Self::BusParticipant { color, .. }
            | Self::JournalCorruption { color, .. } => Some(*color),
            Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::ProjectDeactivated { .. }
            | Self::InfraStatusChanged { .. }
            | Self::InfraFlaky { .. }
            | Self::InfraRecovered { .. }
            | Self::InfraTerminated { .. }
            | Self::InfraConfigError { .. } => None,
        }
    }
}

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<RwLock<HashMap<String, broadcast::Sender<DispatcherEvent>>>>,
    /// Postgres pool used by `publish` for NOTIFY. `None` for tests
    /// or single-pod contexts where the cross-pod channel isn't
    /// wired; in that case `publish` skips the NOTIFY step and
    /// behaves like `publish_local` (the absence of cross-pod fanout
    /// is the caller's responsibility to choose by passing None).
    pool: Option<PgPool>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            pool: None,
        }
    }
}

impl EventBus {
    /// In-process-only bus (tests, pre-pool init).
    pub fn local_only() -> Self {
        Self::default()
    }

    /// Bus with cross-pod fanout via Postgres LISTEN/NOTIFY.
    /// Spawns a long-lived LISTEN task that pushes received events
    /// into the local broadcast.
    pub async fn with_notify(pool: PgPool) -> anyhow::Result<Self> {
        let bus = Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            pool: Some(pool.clone()),
        };
        let bus_for_listener = bus.clone();
        tokio::spawn(async move {
            if let Err(e) = run_listener(pool, bus_for_listener).await {
                tracing::error!(
                    target: "weft_dispatcher::events",
                    error = %e,
                    "LISTEN task exited; cross-pod fanout disabled until restart"
                );
            }
        });
        Ok(bus)
    }

    pub async fn subscribe_project(&self, project_id: &str) -> broadcast::Receiver<DispatcherEvent> {
        let mut inner = self.inner.write().await;
        inner
            .entry(project_id.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    /// Push to local subscribers only. Used by `journal_bridge`,
    /// where every pod's bridge polls the journal independently
    /// (the cross-pod fanout for ExecEvent is the journal itself).
    pub async fn publish_local(&self, event: DispatcherEvent) {
        self.publish_local_inner(&event).await;
    }

    /// Push locally AND issue NOTIFY so sibling pods receive it.
    /// Used for the events that don't ride the journal:
    /// ProjectRegistered/Activated/Deactivated, TriggerUrlChanged,
    /// and the ExecutionStarted "fast-path" emitted by run/activate
    /// before the journal_bridge poll picks up the row.
    pub async fn publish(&self, event: DispatcherEvent) {
        self.publish_local_inner(&event).await;
        let Some(pool) = &self.pool else {
            return;
        };
        let payload = match serde_json::to_string(&event) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    target: "weft_dispatcher::events",
                    error = %e,
                    "serialize DispatcherEvent for NOTIFY"
                );
                return;
            }
        };
        // Postgres NOTIFY caps payloads at 8000 bytes (NAMEDATALEN -
        // some). Events sent via `publish()` (vs `publish_local()`)
        // do NOT have a journal poll-based recovery: ProjectRegistered
        // / ProjectActivated / ProjectDeactivated / TriggerUrlChanged /
        // InfraStatusChanged / InfraFlaky / InfraRecovered /
        // InfraTerminated / InfraConfigError / ExecutionStarted-fast-
        // path all ride the NOTIFY-only path. If one of these blows
        // the cap, sibling pods miss the event entirely until the
        // next user action triggers a fresh round-trip; this is a
        // real failure mode worth alerting on, not a recoverable
        // race. Every user-string field on a publish-path event
        // (`reason` on InfraFlaky, `error` on InfraConfigError,
        // `name` on ProjectRegistered, `entry_node` on the
        // ExecutionStarted fast-path, `node_id`/`url` on
        // TriggerUrlChanged) is bounded at construction via
        // `weft_core::truncate_user_string(.., 4096)`, so tripping
        // this branch is an invariant violation (an unbounded field
        // slipped into a publish-path event), not expected input.
        if payload.len() > 7800 {
            tracing::error!(
                target: "weft_dispatcher::events",
                size = payload.len(),
                kind = ?std::mem::discriminant(&event),
                "DispatcherEvent too large for Postgres NOTIFY; sibling pods will miss it"
            );
            return;
        }
        if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
            .bind(NOTIFY_CHANNEL)
            .bind(&payload)
            .execute(pool)
            .await
        {
            tracing::error!(
                target: "weft_dispatcher::events",
                error = %e,
                "pg_notify failed"
            );
        }
    }

    async fn publish_local_inner(&self, event: &DispatcherEvent) {
        let inner = self.inner.read().await;
        if let Some(tx) = inner.get(event.project_id()) {
            // broadcast::Sender::send errors only when there are
            // no live receivers; that's a normal idle state (no
            // SSE clients subscribed), not a failure to discard.
            let _ = tx.send(event.clone());
        }
    }
}

/// Long-lived LISTEN handler. Two failure surfaces:
///
/// - Initial `connect_with` / `listen` failures (Postgres down at
///   boot or transient hiccup). The outer reconnect loop retries
///   with exponential backoff so a Postgres outage at boot doesn't
///   silently disable cross-pod fanout for the lifetime of this pod
///   (the earlier shape used `?` on connect/listen and let the spawn
///   task die on first failure).
/// - Per-message decode errors and `listener.recv()` errors AFTER a
///   successful connect. PgListener handles connection drops
///   internally and re-establishes LISTEN; these get logged and the
///   inner loop continues. Once recv() returns a hard error that
///   PgListener can't recover from, we break out of the inner loop
///   and the outer loop reconnects from scratch.
///
/// The outer `Result` is therefore never returned in normal
/// operation; the function only ends on task cancellation.
async fn run_listener(pool: PgPool, bus: EventBus) -> anyhow::Result<()> {
    let mut backoff_secs: u64 = 1;
    const BACKOFF_CAP_SECS: u64 = 30;
    loop {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::events",
                    error = %e,
                    backoff_secs,
                    "PgListener connect failed; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(BACKOFF_CAP_SECS);
                continue;
            }
        };
        if let Err(e) = listener.listen(NOTIFY_CHANNEL).await {
            tracing::warn!(
                target: "weft_dispatcher::events",
                error = %e,
                backoff_secs,
                "PgListener listen() failed; retrying"
            );
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(BACKOFF_CAP_SECS);
            continue;
        }
        // Connected. Do NOT reset backoff yet: a connection that
        // succeeds at `listen()` but errors on the first `recv()`
        // (Postgres backend in a restart loop, network flap) would
        // otherwise busy-loop reconnect with zero sleep. The
        // connection counts as genuinely usable once it either
        // receives a message OR survives quietly for a while: a
        // healthy-but-quiet cluster (no NOTIFY traffic for hours)
        // must not keep an escalated backoff from an old flap and
        // pay the 30s cap on every later reconnect.
        const HEALTHY_AFTER: std::time::Duration = std::time::Duration::from_secs(60);
        let connected_at = std::time::Instant::now();
        loop {
            match listener.recv().await {
                Ok(notif) => {
                    backoff_secs = 1;
                    let payload = notif.payload();
                    match serde_json::from_str::<DispatcherEvent>(payload) {
                        Ok(event) => bus.publish_local_inner(&event).await,
                        Err(e) => {
                            tracing::warn!(
                                target: "weft_dispatcher::events",
                                error = %e,
                                payload_len = payload.len(),
                                "could not decode NOTIFY payload"
                            );
                        }
                    }
                }
                Err(e) => {
                    if connected_at.elapsed() >= HEALTHY_AFTER {
                        backoff_secs = 1;
                    }
                    tracing::warn!(
                        target: "weft_dispatcher::events",
                        error = %e,
                        backoff_secs,
                        "PgListener recv error; reconnecting after backoff"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(BACKOFF_CAP_SECS);
                    break;
                }
            }
        }
    }
}
