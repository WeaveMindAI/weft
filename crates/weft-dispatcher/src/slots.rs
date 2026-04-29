//! Per-color execution slot. The dispatcher holds at most one slot
//! per color (execution id). A slot is one of:
//!
//! - `Idle`: no worker alive. The last durable snapshot (if any)
//!   lives in the journal; wakes that arrive queue here and get
//!   delivered with the next `Start` message when a worker is
//!   spawned.
//! - `Starting`: the dispatcher has spawned a worker; waiting for
//!   its WebSocket `Ready`. Wakes continue to queue.
//! - `Live`: the worker is connected. Wakes are pushed through the
//!   WebSocket immediately. A per-token map tracks active
//!   suspensions so deliveries land in the right lane.
//! - `WaitingReconnect`: the WebSocket dropped (worker crash or
//!   network blip). We hold incoming wakes until either the worker
//!   reconnects (transition back to `Live`) or a timeout fires and
//!   we move to a recovery path (load orphan snapshot from disk or
//!   mark the execution failed).
//!
//! All transitions go through `Slots::with_slot_mut`, which wraps
//! a per-color `tokio::Mutex`. One worker per color at a time is the
//! invariant.
//!
//! Phase A holds the slot map in RAM (`DashMap<Color, Mutex<Slot>>`).
//! Phase B moves the ownership lookup into a shared store (Postgres
//! or Redis) so multiple dispatcher instances can route wakes to the
//! right one.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::{mpsc, Mutex};

use weft_core::primitive::{DispatcherToWorker, WakeMessage};
use weft_core::Color;

use crate::backend::WorkerHandle;

/// Handle used by the dispatcher HTTP router to push outbound
/// messages to a live worker. Concrete implementation is an
/// `mpsc::Sender<DispatcherToWorker>` drained by the WebSocket
/// writer task. We keep it behind a type alias so the rest of the
/// codebase doesn't depend on tungstenite types.
pub type WorkerSender = mpsc::Sender<DispatcherToWorker>;

pub enum Slot {
    /// No worker alive for this color. Wakes queue up here.
    /// Snapshots aren't stored inline; they're folded from the
    /// event log at worker-spawn time (see `api::ws::handle_socket`).
    Idle {
        queued: VecDeque<QueuedWake>,
    },
    /// A worker has been spawned (or is about to be); waiting for
    /// its WS `Ready`. Wakes continue to queue. `worker` is `None`
    /// between the spawn-reservation and the actual
    /// `spawn_worker` call; this brief window is still `Starting`
    /// so another form POST won't also decide to spawn.
    Starting {
        queued: VecDeque<QueuedWake>,
        worker: Option<WorkerHandle>,
    },
    /// Worker is connected over WebSocket. The
    /// `worker_instance_id` is stamped at first `Ready` and used
    /// to validate a later `Reconnected` message after a socket
    /// drop.
    Live {
        sender: WorkerSender,
        worker_instance_id: String,
    },
    /// WebSocket dropped unexpectedly. Dispatcher gives the worker
    /// up to `grace` seconds to reconnect. If a reconnect lands,
    /// transition back to Live with the new sender. If the grace
    /// timer fires first, the dispatcher kills the worker, folds
    /// events to snapshot, and spawns a replacement.
    WaitingReconnect {
        since: Instant,
        queued: VecDeque<QueuedWake>,
        worker: Option<WorkerHandle>,
        worker_instance_id: String,
    },
    /// Worker reported `Stalled` (parked on suspensions) and is
    /// alive but idle. We hold the WS sender so a fire arriving
    /// before `grace_until` forwards directly without a respawn.
    /// On expiry, kill the worker and transition to Idle.
    StalledGrace {
        sender: WorkerSender,
        worker: Option<WorkerHandle>,
        worker_instance_id: String,
        grace_until: Instant,
    },
}

/// A wake message queued for the next worker. The only thing the
/// slot buffers is the initial `Start` for a fresh or resumed
/// worker. Wake-signal deliveries are NOT queued here; they live in
/// the journal as `SuspensionResolved` events and get folded into
/// the worker's Start snapshot via `pending_deliveries`.
pub enum QueuedWake {
    Start(WakeMessage),
}

#[derive(Clone)]
pub struct Slots {
    inner: Arc<DashMap<Color, Arc<Mutex<Slot>>>>,
}

impl Slots {
    pub fn new() -> Self {
        Self { inner: Arc::new(DashMap::new()) }
    }

    /// Get or create the slot for `color`, then run the closure
    /// while holding its lock. The closure is async so handlers can
    /// await network I/O with the lock held; keep such awaits short.
    pub async fn with_slot<F, R>(&self, color: Color, f: F) -> R
    where
        F: for<'a> FnOnce(&'a mut Slot) -> futures::future::BoxFuture<'a, R>,
    {
        let slot = self
            .inner
            .entry(color)
            .or_insert_with(|| {
                Arc::new(Mutex::new(Slot::Idle {
                    queued: VecDeque::new(),
                }))
            })
            .clone();
        let mut guard = slot.lock().await;
        f(&mut guard).await
    }

    /// Drop the slot entirely (execution completed or terminally
    /// failed). Releases the mutex and removes the entry from the
    /// map so memory doesn't grow unboundedly.
    pub async fn drop_slot(&self, color: Color) {
        self.inner.remove(&color);
    }
}

impl Default for Slots {
    fn default() -> Self {
        Self::new()
    }
}

