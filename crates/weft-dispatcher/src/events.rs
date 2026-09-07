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

/// An event and its delivery identity. Journal projections derive identities
/// from the stored row plus projection index, so replay and live delivery
/// identify the same event without comparing timestamps or payload contents.
// SYNC: IdentifiedEvent.event_id <-> extension-vscode/src/execFollower.ts DispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentifiedEvent<T> {
    pub event_id: String,
    #[serde(flatten)]
    pub event: T,
}

impl<T> IdentifiedEvent<T> {
    pub fn recorded(id: i64, event: T) -> Self {
        Self { event_id: format!("journal:{id}"), event }
    }

    pub fn transient(event: T) -> Self {
        Self { event_id: format!("live:{}", uuid::Uuid::new_v4()), event }
    }

    pub fn project<U>(self, project: impl FnOnce(T) -> Vec<U>) -> Vec<IdentifiedEvent<U>> {
        project(self.event).into_iter().enumerate().map(|(index, event)| IdentifiedEvent {
            event_id: format!("{}:{index}", self.event_id), event,
        }).collect()
    }
}

pub type LiveEvent = IdentifiedEvent<DispatcherEvent>;

/// LISTEN channel name. Single channel for all cross-pod events;
/// receivers route by `project_id` themselves.
const NOTIFY_CHANNEL: &str = "weft_dispatcher_events";

/// An event the dispatcher publishes about some piece of runtime
/// state changing. Tagged enum so SSE serialization matches the
/// spec in the design doc.
// Every event projected from a journal row carries that row's `at_unix`
// (the journal's own stamp), so a replay renders when each thing happened
// rather than when it was read.
// SYNC: DispatcherEvent <-> extension-vscode/src/execFollower.ts DispatcherEvent, weavemind/website/src/lib/graph/dispatcher-host.ts translateDispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DispatcherEvent {
    ExecutionStarted { color: Color, entry_node: String, project_id: String, at_unix: u64 },
    ExecutionCompleted { color: Color, project_id: String, outputs: serde_json::Value, at_unix: u64 },
    ExecutionFailed { color: Color, project_id: String, error: String, at_unix: u64 },
    /// `cause` is the structured who-or-what behind the cancel (`reason`
    /// is its text). `None` only for a journal row written before the
    /// cause existed; skipped on the wire when absent so the TS peers'
    /// optional (`cause?`) types match reality instead of decoding null.
    ExecutionCancelled {
        color: Color,
        project_id: String,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cause: Option<weft_core::exec::CancelCause>,
        at_unix: u64,
    },
    /// The run tagged itself (`ctx.tag_execution`); the inspector shows
    /// the tags on the run. `tags` is this call's list, not the run's
    /// cumulative set.
    ExecutionTagged { color: Color, project_id: String, tags: Vec<String>, at_unix: u64 },
    NodeStarted { color: Color, node: String, frames: LoopFrames, input: serde_json::Value, closed_ports: Vec<String>, project_id: String, at_unix: u64 },
    NodeSuspended { color: Color, node: String, frames: LoopFrames, token: String, project_id: String, at_unix: u64 },
    NodeResumed { color: Color, node: String, frames: LoopFrames, token: Option<String>, value: Option<serde_json::Value>, project_id: String, at_unix: u64 },
    NodeCancelled { color: Color, node: String, frames: LoopFrames, reason: String, project_id: String, at_unix: u64 },
    NodeCompleted { color: Color, node: String, frames: LoopFrames, output: serde_json::Value, project_id: String, at_unix: u64 },
    NodeFailed { color: Color, node: String, frames: LoopFrames, error: String, project_id: String, at_unix: u64 },
    /// `reason` says WHY: the author's `_should_flow` said no, or an
    /// input the node needed never arrived. A decision and a consequence
    /// look identical on the graph without it.
    /// `None` only for a journal row written before the field existed
    /// (the UI renders "reason not recorded"); every live writer sends
    /// `Some`.
    NodeSkipped { color: Color, node: String, frames: LoopFrames, closed_ports: Vec<String>, reason: Option<weft_core::exec::skip::SkipReason>, project_id: String, at_unix: u64 },
    /// A node emitted a value whose type is incompatible with the
    /// declared (possibly narrowed) type of `port`. The engine refused
    /// the value and closed the port (downstream sees null); the node did
    /// NOT fail. The extension renders this as a per-port warning.
    PortTypeMismatch { color: Color, node: String, frames: LoopFrames, port: String, expected: String, actual: String, project_id: String, at_unix: u64 },
    /// A loop instance was created at `parent_frames`. The inspector
    /// uses this to render a "Loop opened" marker at the loop's box.
    // SYNC: LoopInstantiated <-> extension-vscode/src/execFollower.ts loop_instantiated, packages/weft-graph/src/protocol.ts LoopInspectorEvent 'instantiated'
    LoopInstantiated {
        color: Color,
        project_id: String,
        group_id: String,
        parent_frames: LoopFrames,
        /// Effective iteration CAP; `None` for an uncapped loop (a
        /// done-driven or stream-driven loop with no `max_iters`),
        /// whose iteration count is unknowable up front.
        iter_cap: Option<u32>,
        parallel: bool,
        at_unix: u64,
    },
    /// An iteration of the loop launched. Inspector renders an
    /// iteration marker at body_frames.
    LoopIterationLaunched {
        color: Color,
        project_id: String,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        at_unix: u64,
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
        at_unix: u64,
    },
    /// The loop terminated outward and emitted its outer outputs.
    LoopTerminated {
        color: Color,
        project_id: String,
        group_id: String,
        parent_frames: LoopFrames,
        reason: weft_core::primitive::LoopTerminationReason,
        at_unix: u64,
    },
    /// One metered call's cost record landed on the journal, attributed to
    /// the exact firing (`node_id` + `frames`). `amount_usd` `None` = the
    /// meter could not resolve the figure (an honest unknown).
    CostReported {
        color: Color,
        project_id: String,
        node_id: String,
        frames: LoopFrames,
        /// Stable per-record identity; the webview dedups on it (the same
        /// journal row can arrive via both the replay and the live stream).
        cost_id: String,
        service: String,
        amount_usd: Option<f64>,
        /// Whose credential the call spent (`their-own` or `ours`), so a
        /// client can say whose account a figure landed on.
        origin: weft_core::CredentialOwner,
        at_unix: u64,
    },
    TriggerUrlChanged { project_id: String, node_id: String, url: String },
    ProjectRegistered { project_id: String, name: String },
    ProjectActivated { project_id: String },
    ProjectDeactivated { project_id: String },
    /// A project lifecycle axis flipped: entering/leaving a
    /// transitional state (activating, deactivating, building,
    /// cancelling_build) or landing at rest. Carries both axes so a
    /// client can render the new state without a round-trip; clients
    /// that prefer one code path just refetch `/status` on receipt.
    /// This is what makes backend-owned transitional state observable
    /// in near-real-time (the backend-owns-state rule has no teeth
    /// without it).
    ProjectTransitionChanged { project_id: String, status: String, transition: String },
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
    /// One journal-aggregation window of a bus's messages (one row per
    /// bus per window; default 1s). A journaled bus's `messages` carry
    /// every message in the window (senders, kinds, payloads); an
    /// ephemeral bus's `messages` are empty and `totals` (count + bytes
    /// per sender/kind) are the whole story. The inspector unpacks
    /// `messages` into its per-message log and renders a summary line
    /// for a window that carries only totals.
    BusWindow {
        color: Color,
        project_id: String,
        bus_id: String,
        first_offset: u64,
        last_offset: u64,
        messages: Vec<weft_core::bus::WindowedBusMessage>,
        totals: Vec<weft_core::bus::BusWindowTotal>,
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
    /// A live caller attached to this execution. First event in the
    /// caller stream; the inspector opens a "caller" panel on the run.
    CallerConnected {
        color: Color,
        project_id: String,
        offset: u64,
        protocol: String,
        at_unix: u64,
    },
    /// A message arrived from the caller. `payload` is the tagged
    /// `WirePayload` (json value or base64 bytes), the same wire
    /// vocabulary as a bus window's messages.
    // SYNC: CallerInbound <-> crates/weft-journal/src/events.rs CallerInbound, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'inbound', extension-vscode/src/execFollower.ts DispatcherEvent 'caller_inbound'
    CallerInbound {
        color: Color,
        project_id: String,
        offset: u64,
        payload: weft_core::bus::WirePayload,
        payload_byte_size: u64,
        at_unix: u64,
    },
    /// A message was sent to the caller. `terminal` marks the final
    /// outbound (HTTP respond/close, WS close).
    // SYNC: CallerOutbound <-> crates/weft-journal/src/events.rs CallerOutbound, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'outbound', extension-vscode/src/execFollower.ts DispatcherEvent 'caller_outbound'
    CallerOutbound {
        color: Color,
        project_id: String,
        offset: u64,
        payload: weft_core::bus::WirePayload,
        payload_byte_size: u64,
        terminal: bool,
        at_unix: u64,
    },
    /// A node error surfaced to the caller.
    CallerErrored {
        color: Color,
        project_id: String,
        offset: u64,
        message: String,
        at_unix: u64,
    },
    /// The caller is gone (response complete OR disconnected). Last
    /// event in the caller stream; replay cursors stop here.
    CallerDisconnected {
        color: Color,
        project_id: String,
        offset: u64,
        reason: String,
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
            | Self::ExecutionTagged { project_id, .. }
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
            | Self::ProjectTransitionChanged { project_id, .. }
            | Self::InfraStatusChanged { project_id, .. }
            | Self::InfraFlaky { project_id, .. }
            | Self::InfraRecovered { project_id, .. }
            | Self::InfraTerminated { project_id, .. }
            | Self::InfraConfigError { project_id, .. }
            | Self::BusJoined { project_id, .. }
            | Self::BusLeft { project_id, .. }
            | Self::BusWindow { project_id, .. }
            | Self::BusClosed { project_id, .. }
            | Self::BusParticipant { project_id, .. }
            | Self::CallerConnected { project_id, .. }
            | Self::CallerInbound { project_id, .. }
            | Self::CallerOutbound { project_id, .. }
            | Self::CallerErrored { project_id, .. }
            | Self::CallerDisconnected { project_id, .. }
            | Self::JournalCorruption { project_id, .. } => project_id,
        }
    }

    pub fn color(&self) -> Option<Color> {
        match self {
            Self::ExecutionStarted { color, .. }
            | Self::ExecutionCompleted { color, .. }
            | Self::ExecutionFailed { color, .. }
            | Self::ExecutionCancelled { color, .. }
            | Self::ExecutionTagged { color, .. }
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
            | Self::BusWindow { color, .. }
            | Self::BusClosed { color, .. }
            | Self::BusParticipant { color, .. }
            | Self::CallerConnected { color, .. }
            | Self::CallerInbound { color, .. }
            | Self::CallerOutbound { color, .. }
            | Self::CallerErrored { color, .. }
            | Self::CallerDisconnected { color, .. }
            | Self::JournalCorruption { color, .. } => Some(*color),
            Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::ProjectDeactivated { .. }
            | Self::ProjectTransitionChanged { .. }
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
    inner: Arc<RwLock<HashMap<String, broadcast::Sender<LiveEvent>>>>,
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

    pub async fn subscribe_project(&self, project_id: &str) -> broadcast::Receiver<LiveEvent> {
        let mut inner = self.inner.write().await;
        inner
            .entry(project_id.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    /// Push to local subscribers only. Used by `journal_bridge`,
    /// where every pod's bridge polls the journal independently
    /// (the cross-pod fanout for ExecEvent is the journal itself).
    pub async fn publish_local(&self, event: LiveEvent) {
        self.publish_local_inner(&event).await;
    }

    /// Push locally AND issue NOTIFY so sibling pods receive it.
    /// Used for the events that don't ride the journal:
    /// ProjectRegistered/Activated/Deactivated and TriggerUrlChanged.
    /// Execution events use only the journal bridge, preserving their
    /// identity across history and live delivery.
    pub async fn publish(&self, event: DispatcherEvent) {
        let event = IdentifiedEvent::transient(event);
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
        // InfraTerminated / InfraConfigError all ride the NOTIFY-only path.
        // If one of these blows
        // the cap, sibling pods miss the event entirely until the
        // next user action triggers a fresh round-trip; this is a
        // real failure mode worth alerting on, not a recoverable
        // race. Every user-string field on a publish-path event
        // (`reason` on InfraFlaky, `error` on InfraConfigError,
        // `name` on ProjectRegistered, `node_id`/`url` on
        // TriggerUrlChanged) is bounded at construction via
        // `weft_core::truncate_user_string(.., 4096)`, so tripping
        // this branch is an invariant violation (an unbounded field
        // slipped into a publish-path event), not expected input.
        if payload.len() > 7800 {
            tracing::error!(
                target: "weft_dispatcher::events",
                size = payload.len(),
                kind = ?std::mem::discriminant(&event.event),
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

    async fn publish_local_inner(&self, event: &LiveEvent) {
        let inner = self.inner.read().await;
        if let Some(tx) = inner.get(event.event.project_id()) {
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
                    match serde_json::from_str::<LiveEvent>(payload) {
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

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn identities_survive_projection_and_wire_round_trips() {
        let event = DispatcherEvent::ExecutionCompleted {
            color: uuid::Uuid::nil(), project_id: "p".into(),
            outputs: serde_json::json!({}), at_unix: 1,
        };
        let record = IdentifiedEvent::recorded(42, event);
        let projected = record.clone().project(|event| vec![event.clone(), event]);
        assert_ne!(projected[0].event_id, projected[1].event_id);
        let replay = record.project(|event| vec![event.clone(), event]);
        assert_eq!(serde_json::to_value(&projected).unwrap(), serde_json::to_value(replay).unwrap());
        let json = serde_json::to_value(&projected[0]).unwrap();
        assert_eq!(json["event_id"], "journal:42:0");
        assert_eq!(json["kind"], "execution_completed");
        let decoded: LiveEvent = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.event_id, projected[0].event_id);
        let distinct = IdentifiedEvent::recorded(43, decoded.event).project(|event| vec![event]);
        assert_ne!(distinct[0].event_id, projected[0].event_id);
    }
}
