//! Pub/sub for project and execution events. Two layers:
//!
//!   - **Per-process broadcast** (`EventBus`): SSE handlers subscribe;
//!     local publishers push directly. Tokio `broadcast::Sender` keyed
//!     by `project_id`.
//!   - **Cross-process fanout via Postgres LISTEN/NOTIFY**: a publisher
//!     calls `EventBus::publish`, which (a) pushes locally so this
//!     process's SSE consumers see it instantly and (b) issues `NOTIFY
//!     weft_dispatcher_events, '<json>'`. A long-lived LISTEN task on
//!     every other process receives, decodes, and pushes to its own local
//!     broadcast. Use `publish_local` when the caller knows the event
//!     is process-local (no cross-process fanout needed).
//!
//! The split is deliberate: ExecEvent flows through `journal_bridge`
//! which polls `exec_event` independently on every process (so each process
//! ends up publishing the same events to its local broadcast). The
//! NOTIFY channel only carries the smaller cross-cutting events that
//! don't sit on the journal path: ProjectRegistered, ProjectActivated,
//! ProjectDeactivated, TriggerUrlChanged, ExecutionDeleted. These fit inside Postgres
//! NOTIFY's 8000-byte payload cap with room to spare.

use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::{broadcast, RwLock};

/// The event types are weft-core's, so the CLI reads the same rows.
pub use weft_core::live_event::{DispatcherEvent, IdentifiedEvent, LiveEvent};

/// LISTEN channel name. Single channel for all cross-process events;
/// receivers route by `project_id` themselves.
pub const NOTIFY_CHANNEL: &str = "weft_dispatcher_events";

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<RwLock<HashMap<uuid::Uuid, broadcast::Sender<LiveEvent>>>>,
    /// Postgres pool used by `publish` for NOTIFY. `None` for tests
    /// or single-process contexts where the cross-process channel isn't
    /// wired; in that case `publish` skips the NOTIFY step and
    /// behaves like `publish_local` (the absence of cross-process fanout
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
    /// Bus with cross-process fanout via Postgres NOTIFY: what a sibling process
    /// publishes arrives on `signals` (the process's one `LISTEN`
    /// connection, which must listen on [`NOTIFY_CHANNEL`]) and is
    /// pushed into the local broadcast.
    pub fn with_notify(
        pool: PgPool,
        signals: &weft_task_store::pg_signal::PgSignalWatch,
    ) -> anyhow::Result<Self> {
        signals.require(NOTIFY_CHANNEL)?;
        let bus = Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            pool: Some(pool),
        };
        let heard = signals.subscribe();
        crate::app::spawn_supervised("event_bus_fanout", relay_sibling_events(heard, bus.clone()));
        Ok(bus)
    }

    pub async fn subscribe_project(&self, project_id: uuid::Uuid) -> broadcast::Receiver<LiveEvent> {
        let mut inner = self.inner.write().await;
        inner
            .entry(project_id)
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    /// Push to local subscribers only. Used by `journal_bridge`,
    /// where every process's bridge polls the journal independently
    /// (the cross-process fanout for ExecEvent is the journal itself).
    pub async fn publish_local(&self, event: LiveEvent) {
        self.publish_local_inner(&event).await;
    }

    /// Push locally AND issue NOTIFY so sibling processes receive it.
    /// Used for the events that don't ride the journal:
    /// ProjectRegistered/Activated/Deactivated, TriggerUrlChanged and
    /// ExecutionDeleted (the one execution event with no journal row
    /// to ride, the journal being what was deleted). Every other
    /// execution event uses only the journal bridge, preserving its
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
        // the cap, sibling processes miss the event entirely until the
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
                "DispatcherEvent too large for Postgres NOTIFY; sibling instances will miss it"
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
        if let Some(tx) = inner.get(&event.event.project_id()) {
            // broadcast::Sender::send errors only when there are
            // no live receivers; that's a normal idle state (no
            // SSE clients subscribed), not a failure to discard.
            let _ = tx.send(event.clone());
        }
    }
}

/// Push every event a sibling process published into this process's local
/// broadcast. A lost notification is a missed SSE event that no journal
/// replays (the events published this way have no row to ride), which is
/// why `publish` bounds their size; a recheck therefore has nothing to
/// look at. Returns only when the process's signal watch stops, which crashes
/// the process through its supervisor: cross-process fanout would be gone.
async fn relay_sibling_events(mut heard: weft_task_store::pg_signal::Subscription, bus: EventBus) {
    loop {
        match heard.next().await {
            Ok(weft_task_store::pg_signal::Heard::Signal { channel, payload }) if channel == NOTIFY_CHANNEL => {
                match serde_json::from_str::<LiveEvent>(&payload) {
                    Ok(event) => bus.publish_local_inner(&event).await,
                    Err(e) => tracing::warn!(
                        target: "weft_dispatcher::events",
                        error = %e,
                        payload_len = payload.len(),
                        "could not decode NOTIFY payload"
                    ),
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(target: "weft_dispatcher::events", error = %e, "cross-instance event fanout stopped");
                return;
            }
        }
    }
}
