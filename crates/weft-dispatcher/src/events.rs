//! Pub/sub for project and run events. Two layers:
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
//! The split is deliberate: a run's events and the infra's reach every
//! process from the database itself (`crate::live_view` reads each run's
//! new record rows, `crate::infra_event_bridge` each new `infra_event`
//! row, on every process that has somebody following the project), so
//! each process publishes them to its own broadcast. The NOTIFY channel
//! only carries the smaller cross-cutting events that have no row to read:
//! ProjectRegistered, ProjectActivated, ProjectDeactivated,
//! TriggerUrlChanged, RunDeleted, and RunParked, which the broker
//! announces as it parks a run. These fit inside Postgres NOTIFY's
//! 8000-byte payload cap with room to spare.

use std::collections::HashMap;
use std::sync::Arc;

use weft_task_store::db::Pool;
use tokio::sync::{broadcast, RwLock};

/// The event types are weft-core's, so the CLI reads the same rows.
pub use weft_core::live_event::{DispatcherEvent, EventId, IdentifiedEvent, LiveEvent, ShownEvent};

/// One item of a project's queue of events: what one paint of one run put
/// on the screen (`crate::live_view`, a page of its record's rows), or one
/// announcement. The queue holds a fixed number of items, so a burst of a
/// fast run takes one place in it however many events it holds.
pub type Batch = Arc<[ShownEvent]>;

/// How many items a project's queue holds for its slowest follower. A
/// follower that falls further behind is told it lagged
/// ([`broadcast::error::RecvError::Lagged`]): a run's follower catches up
/// from the record (`crate::api::events::run_stream`), the others look at
/// the database.
const QUEUE: usize = 256;

/// LISTEN channel name. Single channel for all cross-process events;
/// receivers route by `project_id` themselves.


#[derive(Clone)]
pub struct EventBus {
    inner: Arc<RwLock<HashMap<uuid::Uuid, broadcast::Sender<Batch>>>>,
    /// Postgres pool used by `publish` for NOTIFY. `None` for tests
    /// or single-process contexts where the cross-process channel isn't
    /// wired; in that case `publish` skips the NOTIFY step and
    /// behaves like `publish_local` (the absence of cross-process fanout
    /// is the caller's responsibility to choose by passing None).
    pool: Option<Pool>,
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
    /// connection, which must listen on
    /// [`weft_task_store::runs::EVENTS_CHANNEL`]) and is
    /// pushed into the local broadcast.
    pub fn with_notify(
        pool: Pool,
        signals: &weft_task_store::pg_signal::PgSignalWatch,
    ) -> anyhow::Result<Self> {
        signals.require(weft_task_store::runs::EVENTS_CHANNEL)?;
        let bus = Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            pool: Some(pool),
        };
        let heard = signals.subscribe();
        crate::app::spawn_supervised("event_bus_fanout", relay_sibling_events(heard, bus.clone()));
        Ok(bus)
    }

    pub async fn subscribe_project(&self, project_id: uuid::Uuid) -> Follower {
        Follower::of(self.subscribe_batches(project_id).await)
    }

    /// [`Self::subscribe_project`], a batch at a time.
    pub async fn subscribe_batches(&self, project_id: uuid::Uuid) -> broadcast::Receiver<Batch> {
        let mut inner = self.inner.write().await;
        inner
            .entry(project_id)
            .or_insert_with(|| broadcast::channel(QUEUE).0)
            .subscribe()
    }

    /// Whether anybody on this process follows `project_id`'s events right
    /// now: what the live view paints a run for.
    pub async fn watched(&self, project_id: uuid::Uuid) -> bool {
        self.inner.read().await.get(&project_id).is_some_and(|sender| sender.receiver_count() > 0)
    }

    /// Push to local subscribers only. Used by the live view and the infra
    /// event bridge, which every process runs for its own subscribers (the
    /// cross-process fanout for them is the database itself).
    pub async fn publish_local(&self, event: ShownEvent) {
        let project_id = event.event.project_id();
        self.send(project_id, Arc::from([event])).await;
    }

    /// Push one paint's events, all of `project_id`, to local subscribers
    /// as one item (the live view's pages).
    pub async fn publish_batch(&self, project_id: uuid::Uuid, events: Vec<ShownEvent>) {
        if events.is_empty() {
            return;
        }
        self.send(project_id, Arc::from(events)).await;
    }

    /// Push locally AND issue NOTIFY so sibling processes receive it.
    /// Used for the events that don't ride the journal:
    /// ProjectRegistered/Activated/Deactivated, TriggerUrlChanged and
    /// RunDeleted (the one run event with no record row
    /// to ride, the record being what was deleted). Every other
    /// run event comes only through the live view, keeping its
    /// identity across history and live delivery.
    pub async fn publish(&self, event: DispatcherEvent) {
        let event = IdentifiedEvent::transient(event);
        self.send(event.event.project_id(), Arc::from([IdentifiedEvent { event_id: event.event_id, event: event.event.clone().shown() }])).await;
        let Some(pool) = &self.pool else {
            return;
        };
        // Postgres NOTIFY caps payloads at 8000 bytes (NAMEDATALEN -
        // some). Events sent via `publish()` (vs `publish_local()`)
        // do NOT have a journal poll-based recovery: ProjectRegistered
        // / ProjectActivated / ProjectDeactivated / TriggerUrlChanged /
        // InfraStatusChanged / InfraFlaky / InfraRecovered /
        // InfraTerminated all ride the NOTIFY-only path. If one of
        // these blows the cap, sibling processes miss the event
        // entirely until the next user action triggers a fresh
        // round-trip; this is a real failure mode worth alerting on,
        // not a recoverable race. Every user-string field on a
        // publish-path event (`reason` on InfraFlaky, `name` on
        // ProjectRegistered, `node_id`/`url` on TriggerUrlChanged) is bounded at construction via
        // `weft_core::truncate_user_string(.., 4096)`, so a refusal for
        // its size is an invariant violation (an unbounded field slipped
        // into a publish-path event), not expected input.
        if let Err(e) = weft_task_store::runs::announce_event(pool, &event).await {
            tracing::error!(
                target: "weft_dispatcher::events",
                error = %format!("{e:#}"),
                kind = ?std::mem::discriminant(&event.event),
                "a DispatcherEvent could not be announced to sibling instances; they miss it"
            );
        }
    }

    async fn send(&self, project_id: uuid::Uuid, batch: Batch) {
        let inner = self.inner.read().await;
        if let Some(tx) = inner.get(&project_id) {
            // broadcast::Sender::send errors only when there are
            // no live receivers; that's a normal idle state (no
            // SSE clients subscribed), not a failure to discard.
            let _ = tx.send(batch);
        }
    }
}

/// A follower of one project's events, an event at a time out of the
/// batches its queue holds ([`Batch`]).
pub struct Follower {
    rx: broadcast::Receiver<Batch>,
    batch: Option<Batch>,
    next: usize,
}

impl Follower {
    pub fn of(rx: broadcast::Receiver<Batch>) -> Self {
        Self { rx, batch: None, next: 0 }
    }

    /// The next event; `Lagged` when batches were dropped before this
    /// follower read them (how many), `Closed` when the bus is gone.
    pub async fn recv(&mut self) -> Result<ShownEvent, broadcast::error::RecvError> {
        loop {
            if let Some(event) = self.batch.as_ref().and_then(|batch| batch.get(self.next)) {
                self.next += 1;
                return Ok(event.clone());
            }
            self.batch = Some(self.rx.recv().await?);
            self.next = 0;
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
            Ok(weft_task_store::pg_signal::Heard::Signal { channel, payload }) if channel == weft_task_store::runs::EVENTS_CHANNEL => {
                match serde_json::from_str::<LiveEvent>(&payload) {
                    // An announced event carries no value at hand: the
                    // events announced this way name none.
                    Ok(event) => bus.send(event.event.project_id(), Arc::from([IdentifiedEvent { event_id: event.event_id, event: event.event.shown() }])).await,
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
