//! Engine-side `ContextHandle`. Lifecycle events go straight to
//! the journal via the broker. Control-plane round-trips
//! (`await_signal`, `register_signal`) go through the dispatcher's
//! task queue (also via the broker): the worker enqueues a task row
//! and waits for completion. Resume values are seeded into the
//! per-(node, frames) await sequence by the loop driver at boot from
//! the journal fold; the body's `await_signal` calls pop entries in
//! call_index order.
//!
//! Infra-provision (the engine-side counterpart to user code's
//! `Node::provision_infra` returning an `InfraSpec`) is driven by the loop
//! driver, NOT by methods on `RunnerHandle`. The loop driver calls
//! `node.provision`, compiles + hashes the returned spec locally,
//! reads prior applied state via the broker, makes a local
//! skip/fresh/replace decision, and (when not Skip) enqueues an
//! `Apply` lifecycle command via `apply_via_supervisor`. The tenant's
//! supervisor pod handles the kubectl work. Once apply completes,
//! the loop driver runs the node's body with `Phase::InfraSetup`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use serde_json::Value;
use tokio::sync::mpsc;

use weft_core::bus::{BusEntry, BusEntryKind, BusHandle, BusInner, BusOptions, BusRegistry};
use weft_core::cancellation::CancellationFlag;
use weft_core::liveness::{FiringLocation, WaitLiveness};
use weft_core::context::{ContextHandle, LogLevel};
use weft_core::error::{WeftError, WeftResult};
use weft_core::node::NodeOutput;
use weft_core::primitive::SignalSpec;
use weft_core::weft_type::WeftType;
use weft_core::Color;

use crate::now_unix;
use crate::wait_tracker::{DeliveryGate, WaitTracker};
use weft_infra::InfraReader;
use weft_journal::{ExecEvent, JournalClient};

use weft_task_store::tasks as task_store;
use weft_task_store::{TaskKind, TaskStoreClient};

/// Serialize a frame stack into the canonical string used in task dedup keys.
/// One definition so the side-effect-task and register-signal-task
/// dedup keys can't drift, and so a serialization failure is surfaced
/// (a swallowed `unwrap_or_default()` would collapse distinct firings to
/// the same empty key and silently drop a task). Frame-stack
/// serialization shouldn't fail in practice, which is exactly why a
/// failure must be loud rather than masked.
fn frames_dedup_key(frames: &weft_core::frames::LoopFrames) -> Result<String, serde_json::Error> {
    serde_json::to_string(frames)
}

/// Bundle of broker-backed clients the engine threads everywhere.
/// Each handle clones cheaply (every field is `Arc<dyn _>`).
#[derive(Clone)]
pub struct EngineClients {
    pub journal: Arc<dyn JournalClient>,
    pub tasks: Arc<dyn TaskStoreClient>,
    pub infra: Arc<dyn InfraReader>,
    /// Broker client for infra applied-state reads + apply enqueue.
    /// Used by the loop driver during `Phase::InfraSetup` to make the
    /// skip/fresh/replace decision locally, then ship the spec to the
    /// supervisor via the `infra_lifecycle_command` table.
    pub infra_state: Arc<dyn InfraStateClient>,
    /// Broker client for fetching the project's `ProjectDefinition`
    /// at execution claim time. The worker no longer carries the
    /// definition baked into the binary; it asks the broker, keyed
    /// by `(project_id, definition_hash)` (hash from the task
    /// payload). Same call per execution; the worker pod caches
    /// across executions by hash so consecutive claims of the same
    /// shape pay only one round trip per shape.
    pub project: Arc<dyn ProjectClient>,
    /// Clock the engine uses for every time-related decision
    /// (deadlines, polling intervals). Production passes the
    /// real clock; layer-3 tests pass `FakeClock` so deadlines
    /// can be exercised without burning real wall-clock seconds.
    pub clock: Arc<dyn weft_platform_traits::Clock>,
    /// Worker-side storage data path (`ctx.storage(...)`): the
    /// lazily-ensured box endpoint + ensure-then-retry policy.
    /// Production: `crate::storage::WorkerStorage`; tests inject a
    /// fake.
    pub storage: Arc<dyn crate::storage::WorkerStorageOps>,
    /// The connection surface (`ctx.open` resolve/release). Production:
    /// the broker-backed client; tests inject a fake.
    pub access_broker: Arc<dyn AccessBroker>,
    /// Cost resolutions still in flight (a metered call's figure being
    /// resolved + recorded after its response ended). The pod's exit paths
    /// refuse to die while this is non-zero, so money is never dropped by
    /// a shutdown racing a resolve.
    pub pending_costs: Arc<crate::metering::PendingCostRecords>,
}

impl EngineClients {
    /// The production bundle: every client speaks to the same broker with the
    /// same pod token, and the clock is the real one. The worker binary is
    /// GENERATED, so it cannot be typechecked with the engine; keeping the
    /// composition here means a field added or renamed on `EngineClients` is a
    /// compile error in this crate rather than a silent break in generated
    /// code.
    pub fn from_broker(broker_url: &str, broker_token_path: &std::path::Path) -> Self {
        let token =
            weft_broker_client::TokenSource::new(broker_token_path.to_path_buf());
        Self {
            journal: weft_broker_client::BrokerJournalClient::new(
                broker_url.to_string(),
                token.clone(),
            ),
            tasks: weft_broker_client::BrokerTaskStoreClient::new(
                broker_url.to_string(),
                token.clone(),
            ),
            infra: weft_broker_client::BrokerInfraClient::new(
                broker_url.to_string(),
                token.clone(),
            ),
            infra_state: weft_broker_client::BrokerInfraStateClient::new(
                broker_url.to_string(),
                token.clone(),
            ),
            project: weft_broker_client::BrokerProjectClient::new(
                broker_url.to_string(),
                token.clone(),
            ),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::WorkerStorage::new(
                broker_url.to_string(),
                broker_token_path.to_path_buf(),
            ),
            access_broker: weft_broker_client::BrokerAccessClient::new(
                broker_url.to_string(),
                token,
            ),
            pending_costs: crate::metering::PendingCostRecords::new(),
        }
    }
}

/// Trait surface over `BrokerProjectClient` so tests can inject a
/// hand-rolled fake. Production has one impl: the broker-backed HTTP
/// client. The trait owns the hash-gated fetch contract so the
/// engine doesn't need to know about HTTP statuses.
#[async_trait]
pub trait ProjectClient: Send + Sync {
    /// Fetch the project's `ProjectDefinition` keyed by
    /// `expected_hash`. Returns `Some(def)` on hit (the
    /// `(project_id, hash)` row exists in the broker's
    /// `project_definition` history), `None` on miss (no row for
    /// that hash, a real "not found"). Every other failure
    /// (transport, parse) is an `Err`. There is no "raced" case for
    /// this endpoint: the history table is append-only, so a hash
    /// either has a row or it doesn't.
    async fn fetch_definition(
        &self,
        project_id: &str,
        expected_hash: &str,
    ) -> anyhow::Result<Option<weft_core::ProjectDefinition>>;
}

#[async_trait]
impl ProjectClient for weft_broker_client::BrokerProjectClient {
    async fn fetch_definition(
        &self,
        project_id: &str,
        expected_hash: &str,
    ) -> anyhow::Result<Option<weft_core::ProjectDefinition>> {
        // Inherent method on the concrete type, called via
        // <BrokerProjectClient>::fetch_definition to disambiguate
        // from the trait method we're implementing.
        let resp = <weft_broker_client::BrokerProjectClient>::fetch_definition(
            self,
            project_id,
            expected_hash,
        )
        .await?;
        let Some(r) = resp else { return Ok(None); };
        let def: weft_core::ProjectDefinition = serde_json::from_str(&r.project_json)
            .map_err(|e| anyhow::anyhow!("parse project_json: {e}"))?;
        Ok(Some(def))
    }
}

/// Trait surface over `BrokerInfraStateClient` so tests can inject a
/// no-op (or recording) implementation. Production has one impl: the
/// broker-backed HTTP client.
///
/// The trait has two operations: `enqueue_apply` (ship a fresh spec
/// to the supervisor) and `wait_apply` (poll the resulting command
/// row to terminal). The supervisor owns every other concern
/// end-to-end: read prior `infra_node`, compile + hash, decide
/// skip / fresh / replace, run kubectl, update the row. The worker
/// just hands off the spec and waits.
#[async_trait]
pub trait InfraStateClient: Send + Sync {
    async fn enqueue_apply(
        &self,
        project_id: &str,
        node_id: &str,
        spec_json: serde_json::Value,
    ) -> anyhow::Result<i64>;

    async fn wait_apply(
        &self,
        project_id: &str,
        command_id: i64,
    ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse>;
}

#[async_trait]
impl InfraStateClient for weft_broker_client::client::BrokerInfraStateClient {
    async fn enqueue_apply(
        &self,
        project_id: &str,
        node_id: &str,
        spec_json: serde_json::Value,
    ) -> anyhow::Result<i64> {
        self.enqueue_apply(project_id, node_id, spec_json).await
    }
    async fn wait_apply(
        &self,
        project_id: &str,
        command_id: i64,
    ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
        self.wait_apply(project_id, command_id).await
    }
}

/// Trait surface over `BrokerAccessClient` so tests can inject a fake.
/// The worker's whole connection desk: resolve a connection reference
/// for one firing (cost records ride the generic task rail) and
/// release it when the node finishes. Production has one impl: the
/// broker-backed HTTP client.
#[async_trait]
pub trait AccessBroker: Send + Sync {
    async fn resolve_connection(
        &self,
        req: &weft_broker_client::protocol::ResolveConnectionRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::ResolveConnectionResponse>;

    async fn release_connection(
        &self,
        req: &weft_broker_client::protocol::ReleaseConnectionRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::ReleaseConnectionResponse>;

    async fn publish_access(
        &self,
        req: &weft_broker_client::protocol::PublishAccessRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::PublishAccessResponse>;

    async fn published_access(
        &self,
        req: &weft_broker_client::protocol::PublishedAccessRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::PublishedAccessResponse>;
}

#[async_trait]
impl AccessBroker for weft_broker_client::client::BrokerAccessClient {
    async fn resolve_connection(
        &self,
        req: &weft_broker_client::protocol::ResolveConnectionRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::ResolveConnectionResponse> {
        self.resolve_connection(req).await
    }

    async fn publish_access(
        &self,
        req: &weft_broker_client::protocol::PublishAccessRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::PublishAccessResponse> {
        self.publish_access(req).await
    }

    async fn published_access(
        &self,
        req: &weft_broker_client::protocol::PublishedAccessRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::PublishedAccessResponse> {
        self.published_access(req).await
    }

    async fn release_connection(
        &self,
        req: &weft_broker_client::protocol::ReleaseConnectionRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::ReleaseConnectionResponse> {
        self.release_connection(req).await
    }
}

/// Hand-rolled fake `AccessBroker`: connections in one map, every
/// resolve/release recorded in one append-only log each.
// Gated to `test` only (not `test-helpers`): the sole consumers are this
// crate's own tests. Widen to `test-helpers` if a downstream crate ever needs
// it, matching the other engine fakes.
#[cfg(test)]
pub struct FakeAccessBroker {
    /// Connections by connection id: the handoff response to answer.
    connections: std::sync::Mutex<
        HashMap<String, weft_broker_client::protocol::ResolveConnectionResponse>,
    >,
    /// Every resolve request received, in order.
    pub resolved: std::sync::Mutex<Vec<weft_broker_client::protocol::ResolveConnectionRequest>>,
    /// The value maps of every release received, in order.
    pub released:
        std::sync::Mutex<Vec<std::collections::BTreeMap<String, String>>>,
    /// Every publish received, in order.
    pub published: std::sync::Mutex<Vec<weft_broker_client::protocol::PublishAccessRequest>>,
}

#[cfg(test)]
impl FakeAccessBroker {
    /// Fake with no connections (resolving fails loudly).
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: std::sync::Mutex::new(HashMap::new()),
            resolved: std::sync::Mutex::new(Vec::new()),
            released: std::sync::Mutex::new(Vec::new()),
            published: std::sync::Mutex::new(Vec::new()),
        })
    }

    pub fn set_connection(
        &self,
        connection_id: &str,
        resp: weft_broker_client::protocol::ResolveConnectionResponse,
    ) {
        self.connections.lock().unwrap().insert(connection_id.to_string(), resp);
    }

    /// A one-bearer-step connection answering `key`, the common shape
    /// tests need. User-owned (`TheirOwn`).
    pub fn set_bearer_connection(&self, connection_id: &str, key: &str) {
        self.set_owned_bearer_connection(
            connection_id,
            key,
            weft_core::CredentialOwner::TheirOwn,
        );
    }

    /// The same bearer shape with an explicit owner, for pinning the
    /// owner-dependent release behavior.
    pub fn set_owned_bearer_connection(
        &self,
        connection_id: &str,
        key: &str,
        owner: weft_core::CredentialOwner,
    ) {
        let mut values = std::collections::BTreeMap::new();
        values.insert("token".to_string(), key.to_string());
        self.set_connection(connection_id, weft_broker_client::protocol::ResolveConnectionResponse {
            values,
            auth: vec![weft_core::access::spec::AuthStep::Header {
                name: "Authorization".into(),
                value: weft_core::access::spec::Template::new("Bearer {token}"),
            }],
            identity: None,
            relay_url: None,
            owner,
        });
    }
}

#[cfg(test)]
#[async_trait]
impl AccessBroker for FakeAccessBroker {
    async fn resolve_connection(
        &self,
        req: &weft_broker_client::protocol::ResolveConnectionRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::ResolveConnectionResponse> {
        self.resolved.lock().unwrap().push(req.clone());
        match self.connections.lock().unwrap().get(&req.connection_id) {
            Some(resp) => Ok(resp.clone()),
            None => anyhow::bail!(
                "connection {} does not exist here; pick one on the access node",
                req.connection_id
            ),
        }
    }

    async fn release_connection(
        &self,
        req: &weft_broker_client::protocol::ReleaseConnectionRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::ReleaseConnectionResponse> {
        self.released.lock().unwrap().push(req.values.clone());
        Ok(weft_broker_client::protocol::ReleaseConnectionResponse {})
    }

    async fn publish_access(
        &self,
        req: &weft_broker_client::protocol::PublishAccessRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::PublishAccessResponse> {
        self.published.lock().unwrap().push(req.clone());
        Ok(weft_broker_client::protocol::PublishAccessResponse {
            connection: published_connection(&req.node_id, &req.service),
        })
    }

    async fn published_access(
        &self,
        req: &weft_broker_client::protocol::PublishedAccessRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::PublishedAccessResponse> {
        let found = self
            .published
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.node_id == req.node_id && p.service == req.service);
        Ok(match found {
            true => weft_broker_client::protocol::PublishedAccessResponse::Published {
                connection: published_connection(&req.node_id, &req.service),
            },
            false => weft_broker_client::protocol::PublishedAccessResponse::NothingPublished,
        })
    }
}

/// The one connection a node publishes, as the fake answers it from
/// both verbs: publishing and reading back hand out the same value,
/// which is the property production guarantees.
#[cfg(test)]
fn published_connection(
    node_id: &str,
    service: &str,
) -> weft_broker_client::protocol::PublishedConnection {
    weft_broker_client::protocol::PublishedConnection {
        connection_id: format!("published-{node_id}-{service}"),
        identity: Some(format!("{service} run by {node_id}")),
    }
}

/// Per-execution bus state: the registry that resolves markers to live
/// channels, plus the list of buses the engine knows about. The dead-
/// end policy is one line: if the loop is stuck (drained, no waiters,
/// tasks still alive) AND any bus is live, close every bus. Each
/// closed bus wakes its parked cursors and waits with Closed / None;
/// the node tasks unwind, the loop drains them and terminates.
///
/// Vocabulary: this code uses "wait" everywhere, not "park". The word
/// "park" already names `ctx.await_signal` (journal-replay workflow
/// suspension, worker swap). A bus cursor's `next().await` is plain
/// in-process tokio await; the worker stays alive; no swap. The
/// engine's stuck-detector still needs the wake-up so it can know
/// "every in-flight task is blocked on a bus right now" and close.
pub struct BusCoordinator {
    /// Single source of truth for every bus minted this execution.
    /// Holds the strong `Arc<BusInner>` per bus (so the pump's Weak
    /// always upgrades and the bus stays pinned long enough to be
    /// drained); doubles as the marker-lookup table. Released by
    /// `shutdown()` AFTER the final drain has been acked.
    registry: BusRegistry,
    /// The execution's shared in-process wait tracker. The bus is one
    /// wait source among several (generator pulls and emission-delivery
    /// waits register here too); the tracker owns the per-node liveness
    /// map and `deadlock_provable`, so the drive loop's stuck-check has
    /// ONE picture of every parked task. See `crate::wait_tracker`.
    waits: Arc<WaitTracker>,
    /// Per-execution journal-pump wake. Buses signal this after every
    /// append (via `Weak<Notify>` they hold). The pump task awaits it
    /// and drains every live bus's unjournaled tail.
    journal_pump_notify: Arc<tokio::sync::Notify>,
    /// Fired by the pump after every drain pass (whether it
    /// successfully journaled entries or not). `shutdown` awaits this
    /// in a loop checking "every bus is drained" so the wait doesn't
    /// poll.
    drain_complete_notify: Arc<tokio::sync::Notify>,
    /// Set to true by `shutdown` AFTER the final drain pass has been
    /// acked. The pump checks this every iteration; once true, the
    /// pump runs one more drain (to capture anything that landed
    /// between the shutdown check and the flag being set, which can't
    /// happen by construction but the extra pass is cheap and honest)
    /// and exits. Replaces a `Weak<BusCoordinator>::upgrade()`-vs-
    /// `drop(coordinator)` race that could leave the pump parked
    /// forever on a multi-thread runtime.
    pump_should_exit: std::sync::atomic::AtomicBool,
}

impl BusCoordinator {
    /// Construct the per-execution coordinator. The pump task is spun
    /// up by the loop driver, not here, because the loop owns the
    /// journal client. `waits` is the execution's shared wait tracker,
    /// owned by the execution (the bus is just one of its clients).
    pub fn new(waits: Arc<WaitTracker>) -> Arc<Self> {
        Arc::new(Self {
            registry: BusRegistry::new(),
            waits,
            journal_pump_notify: Arc::new(tokio::sync::Notify::new()),
            drain_complete_notify: Arc::new(tokio::sync::Notify::new()),
            pump_should_exit: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Handle on the per-execution drain-complete notify. Cloned by
    /// the pump to fire after every drain pass.
    pub fn drain_complete_notify(&self) -> Arc<tokio::sync::Notify> {
        self.drain_complete_notify.clone()
    }

    /// Mint a fresh bus with the provided options and register it,
    /// attributed to the minting node execution `node` (whose identity
    /// keys its wait liveness). The registry pins the `Arc<BusInner>`
    /// (so the pump's `Weak` always upgrades while the execution is
    /// live); the bus's engine hooks (the shared wait tracker + the
    /// journal-pump notify) are `Weak` on purpose so the coordinator
    /// drops naturally at execution end; once gone, the bus's hooks
    /// no-op. Errors on an invalid `window`.
    pub fn new_bus(
        self: &Arc<Self>,
        opts: BusOptions,
        node: FiringLocation,
    ) -> Result<BusHandle, &'static str> {
        let weak = Arc::downgrade(&self.waits) as std::sync::Weak<dyn WaitLiveness>;
        let pump_notify_weak = Arc::downgrade(&self.journal_pump_notify);
        let bus = BusHandle::create_with_engine(opts, weak, pump_notify_weak, Some(node))?;
        self.registry.insert(&bus);
        Ok(bus)
    }

    /// Handle on the per-execution journal-pump wake-up. The pump task
    /// awaits this notify; buses fire it after every append (via
    /// their `Weak<Notify>`).
    pub fn journal_pump_notify(&self) -> Arc<tokio::sync::Notify> {
        self.journal_pump_notify.clone()
    }

    /// Whether the pump should exit on its next iteration. Set by
    /// `shutdown()` after every entry has been drained; the pump
    /// reads this every iteration and exits cleanly once true.
    pub fn pump_should_exit(&self) -> bool {
        self.pump_should_exit.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Snapshot every currently-live bus's `Weak<BusInner>` so the
    /// pump can drain unjournaled tails without holding the registry
    /// lock for the duration of the journal write.
    pub fn live_bus_inners(&self) -> Vec<std::sync::Weak<BusInner>> {
        self.registry.live_bus_weaks()
    }

    /// Look up a Bus marker JSON value in this execution's registry,
    /// attributed to the resolving node execution `node` (whose identity
    /// keys its own bus liveness).
    pub fn lookup_bus(
        &self,
        marker: &serde_json::Value,
        node: FiringLocation,
    ) -> Result<BusHandle, weft_core::bus::BusLookupError> {
        self.registry.lookup(marker, Some(node))
    }

    /// Whether any bus is live AND not yet closed. The loop reads this
    /// when it decides it is stuck: if true, the stuck must be because
    /// of a bus, so close them.
    pub fn has_live_buses(&self) -> bool {
        self.registry.live_bus_arcs().iter().any(|b| !b.is_closed())
    }

    /// Close every live bus (appends a `Closed` log entry to each).
    /// Does NOT release the registry's `Arc<BusInner>` refs: the pump
    /// still needs to journal the `Closed` entries.
    pub fn close_all(&self) {
        for b in self.registry.live_bus_arcs() {
            b.close();
        }
        self.journal_pump_notify.notify_waiters();
    }

    /// True when the pump has journaled every entry on every live
    /// bus. A `journal_degraded` bus is treated as drained: the pump
    /// can't drain it by definition, the node author already saw the
    /// failure as `SendError::JournalDegraded`, and waiting for it
    /// would just chew through the shutdown deadline before the same
    /// loud panic fires.
    fn fully_drained(&self) -> bool {
        self.registry.live_bus_arcs().iter().all(|b| {
            b.is_journal_degraded() || b.journaled_through() >= b.log_len()
        })
    }

    /// Panic with a diagnostic listing every bus still draining.
    /// Single source of truth for the deadline-miss message so the
    /// "immediate zero-remaining" branch and the "timeout fired"
    /// branch report the same thing.
    fn panic_shutdown_deadline_miss(&self, deadline: std::time::Duration) -> ! {
        let stuck = self
            .registry
            .live_bus_arcs()
            .iter()
            .filter(|b| !b.is_journal_degraded() && b.journaled_through() < b.log_len())
            .map(|b| {
                format!(
                    "bus {} ({} entries unjournaled)",
                    b.id(),
                    b.log_len() - b.journaled_through()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        panic!(
            "bus pump did not drain within {}ms; journal writes are stuck or too slow. \
             Buses still draining: {}",
            deadline.as_millis(),
            if stuck.is_empty() { "<none>".to_string() } else { stuck },
        );
    }

    /// Run the full bus shutdown sequence:
    ///   1. Append a `Closed` entry to every live bus AND wake the
    ///      pump.
    ///   2. Wait (notify-driven, no polling) for the pump to journal
    ///      every entry on every bus, bounded by `deadline`.
    ///   3. Release the `Arc<BusInner>` refs the coordinator pins, so
    ///      buses whose only other Arc is gone free immediately.
    ///   4. Set the pump's exit sentinel AND wake it. The pump's next
    ///      iteration reads the flag and returns. This is the
    ///      deterministic exit path; the prior shape (rely on
    ///      `Weak::upgrade` failing after the caller drops the
    ///      coordinator) had a race where the pump could win the
    ///      upgrade, finish an empty drain, and re-park on a notify
    ///      no one will fire again.
    ///
    /// Panics on deadline-miss: a wedged journal client means replay
    /// is permanently incomplete, and the worker should crash so the
    /// dispatcher surfaces the failure rather than swallowing it.
    pub async fn shutdown(&self, deadline: std::time::Duration) {
        self.close_all();
        let drain = self.drain_complete_notify.clone();
        let start = std::time::Instant::now();
        loop {
            // Arm the wake BEFORE the predicate check so a drain pass
            // that fires between the check and the await is not lost.
            let notified = drain.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.fully_drained() {
                break;
            }
            let remaining = deadline.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                self.panic_shutdown_deadline_miss(deadline);
            }
            match tokio::time::timeout(remaining, notified.as_mut()).await {
                Ok(_) => continue,
                Err(_) => {
                    if self.fully_drained() {
                        break;
                    }
                    self.panic_shutdown_deadline_miss(deadline);
                }
            }
        }
        self.registry.clear();
        // Tell the pump to exit on its next iteration, THEN wake it.
        // Setting the flag BEFORE the wake means the pump's loop body
        // reads `true` on the iteration triggered by this notify, even
        // if the pump had already passed its previous flag-check and
        // was parked on the notify. There is no upgrade race because
        // `pump_should_exit` is a plain AtomicBool we own.
        self.pump_should_exit
            .store(true, std::sync::atomic::Ordering::Release);
        self.journal_pump_notify.notify_waiters();
    }
}

impl Drop for BusCoordinator {
    /// Backstop for the case where the coordinator is dropped without
    /// `shutdown()` being called first (a panic unwind, a test that
    /// forgets to shut down). Two duties:
    ///
    /// 1. Close every live bus. Cursors parked on `next().await`
    ///    return `Ok(None)` on a closed bus, so the node tasks
    ///    holding them unblock and drop instead of staying parked
    ///    until their owning JoinSet is also dropped.
    /// 2. Set the pump exit flag and notify so the journal pump
    ///    wakes from `notified.await` and exits.
    ///
    /// In the normal path (`shutdown().await; drop(coord);`), the
    /// buses are already closed and the flag is already true, so the
    /// extra `close()` calls are idempotent no-ops.
    fn drop(&mut self) {
        for b in self.registry.live_bus_arcs() {
            b.close();
        }
        self.pump_should_exit
            .store(true, std::sync::atomic::Ordering::Release);
        self.journal_pump_notify.notify_waiters();
    }
}

// The per-node wait liveness (NodeWaitState, WaitState, enter/exit/
// observed/parked, deadlock_provable) lives in `crate::wait_tracker`:
// the bus shares it with every other in-process wait source
// (generator pulls, emission-delivery waits).

/// Bus-journal pump. One task per execution, spawned by the loop
/// driver before the first node dispatches. Awaits the per-execution
/// notify; on every wake, walks every live bus, drains its
/// unjournaled tail, ships the entries to the journal, and ack-bumps
/// the per-bus `journaled_through` cursor.
///
/// Failure handling: a journal write error sets the bus's
/// `journal_degraded` flag (the NEXT `send` returns
/// `SendError::JournalDegraded(reason)`). The execution keeps running;
/// the in-RAM bus log is unaffected; the inspector's replay tail is
/// truncated for the lost range.
///
/// Lifecycle: `BusCoordinator::shutdown()` sets the
/// `pump_should_exit` flag AFTER closing every bus and waiting for
/// the pump to drain the close entries, then wakes the pump. The
/// pump's next iteration reads the flag and returns. The flag is a
/// plain AtomicBool the coordinator owns: no race between the pump's
/// "did the coordinator drop?" check and the caller's drop, which
/// the prior `Weak::upgrade()` shape had.
pub async fn run_bus_journal_task(
    coordinator: std::sync::Weak<BusCoordinator>,
    color: Color,
    journal: Arc<dyn JournalClient>,
    pod_name: String,
) {
    // Hold owned Arcs on both per-execution notifies so we keep
    // operating even if `coordinator.upgrade()` starts returning None
    // for a tick (a torn-down execution that lost its Arc but still
    // has buses we need to drain).
    let (pump_wake, drain_done) = match coordinator.upgrade() {
        Some(c) => (c.journal_pump_notify(), c.drain_complete_notify()),
        None => return,
    };
    let mut known_buses: Vec<std::sync::Weak<BusInner>> = Vec::new();
    // The per-bus window state (open windows + buffered cursor). Lives
    // on the pump task, not the bus: the bus's own contract stays
    // "entries in order"; the windowing clock is the pump's.
    let mut windows: HashMap<uuid::Uuid, PumpBusState> = HashMap::new();
    loop {
        let notified = pump_wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        // Exit check happens INSIDE the loop body after a wake. The
        // shutdown sequence sets `pump_should_exit` BEFORE waking us,
        // so the flag we read here on a shutdown-triggered wake is
        // already true. We still run one final drain pass first so
        // any entries that landed concurrent with the shutdown flag
        // being set are journaled (and every open window flushed).
        let should_exit = match coordinator.upgrade() {
            Some(coord) => {
                known_buses = coord.live_bus_inners();
                coord.pump_should_exit()
            }
            // Coordinator dropped before shutdown could set the flag.
            // Treat as "exit": there's nothing live to drain and no
            // one will fire pump_wake again.
            None => true,
        };
        drain_buses(
            &known_buses,
            color,
            journal.as_ref(),
            &pod_name,
            &mut windows,
            /* flush_all = */ should_exit,
        )
        .await;
        drain_done.notify_waiters();
        if should_exit {
            return;
        }
        // Sleep until the next open window's flush is due (so a fast
        // stream's window closes on time even with no further appends),
        // or until an append wakes us, whichever first.
        let next_deadline = windows.values().map(|w| w.deadline).min();
        match next_deadline {
            Some(deadline) => {
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            None => notified.await,
        }
    }
}

/// The pump's per-bus windowing state: the messages of the currently
/// open window and the cursor of what has been pulled off the bus
/// (buffered here or written), so a drain never double-ingests.
struct PumpBusState {
    entries: Vec<BusEntry>,
    /// When the open window must flush.
    deadline: tokio::time::Instant,
    /// One past the highest offset already ingested (buffered or
    /// written). `drain_journal_tail` re-serves from the bus's own
    /// `journaled_through` (only bumped at write), so this filter is
    /// what keeps buffered entries from being ingested twice.
    buffered_through: u64,
}

/// One drain pass across every bus the pump knows about. Per bus:
/// ingest the unjournaled tail (messages into the open window,
/// membership entries written immediately, flushing the window first
/// to keep offset order), then flush any window whose deadline passed
/// (or every window, on the final drain). Acknowledge to the bus only
/// what was WRITTEN; on a write failure mark the bus degraded and keep
/// the window buffered so the next pass retries. Takes Weak refs so
/// the pump can run a final drain after the coordinator has dropped.
async fn drain_buses(
    buses: &[std::sync::Weak<BusInner>],
    color: Color,
    journal: &dyn JournalClient,
    pod_name: &str,
    windows: &mut HashMap<uuid::Uuid, PumpBusState>,
    flush_all: bool,
) {
    for weak in buses {
        let Some(inner) = weak.upgrade() else { continue };
        let bus_id = inner.id();
        let bus_id_str = bus_id.to_string();
        let tail = inner.drain_journal_tail();
        let buffered_through =
            windows.get(&bus_id).map(|w| w.buffered_through).unwrap_or(0);

        'entries: for entry in tail {
            if entry.offset < buffered_through {
                continue;
            }
            match &entry.kind {
                BusEntryKind::Message { .. } => {
                    let state = windows.entry(bus_id).or_insert_with(|| PumpBusState {
                        entries: Vec::new(),
                        deadline: tokio::time::Instant::now() + inner.journal_window(),
                        buffered_through: entry.offset,
                    });
                    if state.entries.is_empty() {
                        state.deadline =
                            tokio::time::Instant::now() + inner.journal_window();
                    }
                    state.buffered_through = entry.offset + 1;
                    state.entries.push(entry);
                }
                // Membership entries are journaled individually, in
                // offset order: flush the open window first so the row
                // stream never reorders against the bus log.
                BusEntryKind::Joined { .. }
                | BusEntryKind::Left { .. }
                | BusEntryKind::Closed => {
                    if let Some(state) = windows.get_mut(&bus_id) {
                        if matches!(
                            write_open_window(&inner, color, &bus_id_str, journal, pod_name, state)
                                .await,
                            WindowWrite::Degraded
                        ) {
                            break 'entries;
                        }
                    }
                    let ev = membership_event(color, &bus_id_str, &entry);
                    if let Err(e) = journal.record_event(&ev, Some(pod_name)).await {
                        tracing::error!(
                            target: "weft_engine::bus",
                            bus = %bus_id_str,
                            offset = entry.offset,
                            error = %e,
                            "bus journal pump failed; marking bus degraded"
                        );
                        inner.mark_journal_degraded(format!("journal write failed: {e}"));
                        break 'entries;
                    }
                    inner.acknowledge_journaled_through(entry.offset + 1);
                    if let Some(state) = windows.get_mut(&bus_id) {
                        state.buffered_through =
                            state.buffered_through.max(entry.offset + 1);
                    }
                }
            }
        }

        // Flush the open window when its time is up (or on the final
        // drain, so nothing stays buffered past shutdown/close).
        if let Some(state) = windows.get_mut(&bus_id) {
            if flush_all || state.deadline <= tokio::time::Instant::now() {
                let _ = write_open_window(&inner, color, &bus_id_str, journal, pod_name, state)
                    .await;
            }
        }
        // A bus with nothing buffered needs no window state (and no
        // timer wake). Dropping the state cannot regress the dedup
        // cursor: an empty window means everything buffered was
        // flushed, so the bus's own `journaled_through` covers it.
        if windows.get(&bus_id).is_some_and(|w| w.entries.is_empty()) {
            windows.remove(&bus_id);
        }
    }
    // Drop window state for buses that no longer exist (their entries
    // can never be written; the bus died un-drained and said so).
    windows.retain(|id, _| {
        buses.iter().any(|w| w.upgrade().is_some_and(|b| b.id() == *id))
    });
}

/// Outcome of writing a bus's open window row, named so the call sites
/// read as the contract they enforce.
enum WindowWrite {
    /// Everything buffered for this window is now in the journal (a
    /// row was written, or the window held nothing to write).
    UpToDate,
    /// The journal write failed: the bus is marked degraded and the
    /// window stays buffered so the next pass retries. The caller must
    /// stop ingesting this bus's tail for this pass.
    Degraded,
}

/// Write `state`'s open window as one `BusWindow` row for `inner`.
async fn write_open_window(
    inner: &Arc<BusInner>,
    color: Color,
    bus_id_str: &str,
    journal: &dyn JournalClient,
    pod_name: &str,
    state: &mut PumpBusState,
) -> WindowWrite {
    let Some(aggregate) =
        weft_core::bus::aggregate_window(&state.entries, !inner.ephemeral())
    else {
        return WindowWrite::UpToDate;
    };
    let ev = ExecEvent::BusWindow {
        color,
        bus_id: bus_id_str.to_string(),
        first_offset: aggregate.first_offset,
        last_offset: aggregate.last_offset,
        messages: aggregate.messages,
        totals: aggregate.totals,
        // The row's stamp is the LAST entry's append time: the row
        // describes the bus's own appends, not the moment the pump got
        // around to flushing them.
        at_unix: aggregate.last_at_unix,
    };
    if let Err(e) = journal.record_event(&ev, Some(pod_name)).await {
        tracing::error!(
            target: "weft_engine::bus",
            bus = %bus_id_str,
            first_offset = aggregate.first_offset,
            error = %e,
            "bus window journal write failed; marking bus degraded"
        );
        inner.mark_journal_degraded(format!("journal write failed: {e}"));
        return WindowWrite::Degraded;
    }
    inner.acknowledge_journaled_through(aggregate.last_offset + 1);
    state.entries.clear();
    WindowWrite::UpToDate
}

/// Project one membership `BusEntry` to its `ExecEvent` shape. Message
/// entries never come through here (they ride window rows).
fn membership_event(color: Color, bus_id: &str, entry: &BusEntry) -> ExecEvent {
    let at_unix = entry.at_unix;
    let offset = entry.offset;
    match &entry.kind {
        BusEntryKind::Joined { name } => ExecEvent::BusJoined {
            color,
            bus_id: bus_id.to_string(),
            offset,
            name: name.clone(),
            at_unix,
        },
        BusEntryKind::Left { name } => ExecEvent::BusLeft {
            color,
            bus_id: bus_id.to_string(),
            offset,
            name: name.clone(),
            at_unix,
        },
        BusEntryKind::Closed => ExecEvent::BusClosed {
            color,
            bus_id: bus_id.to_string(),
            offset,
            at_unix,
        },
        BusEntryKind::Message { .. } => {
            unreachable!("Message entries are windowed, never journaled individually")
        }
    }
}

/// An emission a node made via `ctx.pulse_downstream` or
/// `ctx.close_port`, sent from the node task to the loop driver. The
/// loop turns it into downstream pulses at the firing's frame stack. The
/// node task keeps running after sending. Bus values are plain JSON
/// markers inside the value variant; the live channels live in the
/// per-execution `BusRegistry` on `BusCoordinator` and never ride on
/// `EmitMsg`.
pub struct EmitMsg {
    /// The emitting firing's identity.
    pub loc: FiringLocation,
    pub kind: EmitKind,
    /// `Some` when this emission asked to wait for delivery
    /// (`yield_downstream`): the producer's task is parked on
    /// this gate; the loop driver arms it with the pulse ids the
    /// emission created and it resolves at the existing absorb sites.
    /// `None` for the fire-and-forget default.
    pub delivery: Option<Arc<DeliveryGate>>,
    /// The producer's declared per-stream un-taken buffer caps
    /// (`set_max_buffered_items`) as snapshotted when this emission was
    /// constructed (an `Arc` bump, not a map clone, per item), keyed by
    /// output port. For a sequential body a declaration therefore
    /// governs exactly the emissions after it; ports absent here use
    /// [`weft_core::generator::DEFAULT_MAX_BUFFERED_ITEMS`].
    pub stream_caps: Arc<HashMap<String, usize>>,
}

/// The SINGLE message a node task sends to the loop driver, over ONE
/// ordered channel. A node emits zero or more `Emission`s while it runs
/// (each `pulse_downstream` / `close_port`), then sends exactly one
/// `Terminal` when `execute` returns. Putting both on one FIFO channel
/// is load-bearing: it guarantees the driver observes a node's emissions
/// BEFORE its terminal, so the close-unmentioned-ports sweep at the
/// terminal always sees the complete set of emitted ports. Two separate
/// channels left a window where the terminal could be read first and a
/// just-emitted port wrongly closed (skipping its consumer, then a
/// re-dispatch), an emit-then-immediately-return race.
pub enum TaskMsg {
    Emission(EmitMsg),
    /// A running consumer's pull took one generator item. The driver
    /// absorbs the item's pulse (the take's durability rides a
    /// `PulsesConsumed` journal row) and resolves any delivery gate
    /// waiting on it. Rides the same FIFO channel as emissions and the
    /// terminal, so a take is always applied before the consumer's own
    /// terminal.
    StreamItemTaken {
        /// The consuming firing's identity.
        loc: FiringLocation,
        pulse_id: uuid::Uuid,
    },
    Terminal {
        /// The terminating firing's identity.
        loc: FiringLocation,
        color: Color,
        outcome: NodeTaskOutcome,
    },
}

/// How a node's `execute` ended. The driver turns each into the
/// firing's terminal lifecycle event.
pub enum NodeTaskOutcome {
    /// `execute` returned `Ok(())`.
    Completed,
    Failed(String),
    /// The node called `await_signal` and is now waiting on a fired
    /// wake signal (carries the suspension token).
    Waiting(String),
}

/// What the emission carries. A node either ships values on N output
/// ports (`Values`) or closes a single port (`Close`). One enum so the
/// loop driver has one channel to drain and the per-firing
/// one-mention-per-port rule applies uniformly across both shapes.
pub enum EmitKind {
    /// A `pulse_downstream` call: emit values on every port in `output`.
    Values(NodeOutput),
    /// A `close_port` call: emit a CLOSURE on `port`. The downstream
    /// subgraph attached to that port at this frame stack learns nothing's
    /// coming, exactly the same shape as the termination-time sweep
    /// for an unmentioned port.
    Close(String),
}

/// Round-trip timeout for control-plane tasks. Generous because
/// some involve listener spawn + Pod readiness wait.
pub(crate) const TASK_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const TASK_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Journal write stamped with this Pod's name for fencing. Logs the
/// error instead of propagating (most call sites are teardown paths
/// that cannot fail), BUT the drive's journal client is wrapped in
/// `PoisonOnWriteFailure`, so the failure latches a flag the drive
/// loop checks every iteration: the worker exits instead of driving
/// on top of a journal that no longer matches its live state.
/// The runtime output-type gate: a node may only emit on a port a value
/// its declared type accepts (`WeftType::accepts_runtime_value`: a
/// declared named/record shape validates the value against its
/// contract, everything else infers and compares structurally; an
/// unresolved declared type accepts anything).
fn type_accepts(declared: &WeftType, value: &Value) -> bool {
    declared.accepts_runtime_value(value)
}

pub async fn record_from_pod(journal: &dyn JournalClient, event: ExecEvent, pod_name: &str) {
    if let Err(e) = journal.record_event(&event, Some(pod_name)).await {
        tracing::error!(
            target: "weft_engine::journal",
            error = %e,
            "journal write failed; drive is now poisoned and the worker will exit"
        );
    }
}

/// Journal-client decorator that latches the first `record_event`
/// failure into a shared flag.
///
/// Lifecycle writes ship one by one (`record_from_pod`), so a FAILED
/// write means the journal is now a strict prefix of what the live
/// worker believes happened. Continuing to drive on that divergence
/// makes every later refold (stall refetch, crash resume) rebuild a
/// different world: a body whose `NodeStarted` was lost but whose
/// `PulseEmitted` rows landed re-runs and double-spends. The drive
/// loop checks the flag every iteration and exits the worker, so a
/// respawned worker refolds from the journal's consistent prefix
/// (re-running the lost suffix, which is the same at-least-once
/// semantics as a crash).
///
/// The bus pump deliberately keeps the UNwrapped client: bus rows are
/// the inspector's replay trail, and their failures already degrade
/// per-bus without killing the worker.
pub struct PoisonOnWriteFailure {
    inner: Arc<dyn JournalClient>,
    poisoned: Arc<std::sync::atomic::AtomicBool>,
}

impl PoisonOnWriteFailure {
    /// Wrap `inner`; returns the wrapped client and the shared flag.
    pub fn wrap(
        inner: Arc<dyn JournalClient>,
    ) -> (Arc<dyn JournalClient>, Arc<std::sync::atomic::AtomicBool>) {
        let poisoned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        (
            Arc::new(Self { inner, poisoned: poisoned.clone() }),
            poisoned,
        )
    }
}

#[async_trait::async_trait]
impl JournalClient for PoisonOnWriteFailure {
    async fn record_event(
        &self,
        event: &ExecEvent,
        pod_name: Option<&str>,
    ) -> anyhow::Result<()> {
        let r = self.inner.record_event(event, pod_name).await;
        if r.is_err() {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
        }
        r
    }

    async fn events_for_color(
        &self,
        color: Color,
    ) -> anyhow::Result<Vec<ExecEvent>> {
        self.inner.events_for_color(color).await
    }

    async fn raw_events_for_color(&self, color: Color) -> anyhow::Result<Vec<String>> {
        self.inner.raw_events_for_color(color).await
    }

    async fn has_terminal_event(&self, color: Color) -> anyhow::Result<bool> {
        self.inner.has_terminal_event(color).await
    }
}

/// What one firing has done to its output ports, under ONE lock (see
/// the `port_claims` field doc): the ports it mentioned (emitted or
/// closed) and the generator ports it explicitly ENDED (`close_port`,
/// the early end-of-stream verb; a yield after the end is a
/// node-author bug and errors loud).
#[derive(Default)]
struct PortClaims {
    mentioned: HashSet<String>,
    ended_streams: HashSet<String>,
}

pub struct RunnerHandle {
    execution_id: String,
    project_id: String,
    color: Color,
    node_id: String,
    /// The node's catalog type, sent with runtime-key / cost-provision
    /// requests so the runtime's policy + audit trail name the exact
    /// node kind asking.
    node_type: String,
    node_frames: weft_core::frames::LoopFrames,
    /// Broker-backed clients: journal writes, task enqueue, and
    /// infra reads (infra endpoint lookup) all flow through here.
    clients: EngineClients,
    /// The recipe for the service THIS node publishes a connection to,
    /// resolved by the compiler and carried on the node's definition.
    /// `None` for the overwhelming majority of nodes, which publish
    /// nothing.
    published_service: Option<weft_core::AccessSpec>,
    /// k8s Pod name stamped on every journal write so the fencing
    /// trigger can reject writes from a Pod that has been drained or
    /// reaped.
    pod_name: String,
    /// Tenant id stamped on every task this handle enqueues. The
    /// dispatcher's listener reaper queries the task table by tenant
    /// to tell "in-flight register" from "genuinely idle" before
    /// killing the per-tenant listener pod.
    tenant_id: String,
    cancellation: Arc<CancellationFlag>,
    /// Pre-loaded sequence of past `await_signal` calls for this
    /// (node, frames), seeded by the loop driver from the journal
    /// fold on every dispatch. Each `await_signal` call inside the
    /// node body pops the next entry:
    ///   - resolved=Some(value): return it instantly (replay path).
    ///   - resolved=None (the still-pending tail): suspend with
    ///     this token (we're being re-dispatched but our fire
    ///     hasn't arrived for THIS call yet).
    ///   - exhausted: this is a fresh await; enqueue register_signal
    ///     with the next call_index.
    /// The `Mutex` is just for interior mutability across the
    /// `&self` of the trait method; calls are serialized by the
    /// node body (single Future polling).
    awaited_sequence: Mutex<std::collections::VecDeque<weft_core::primitive::AwaitedEntry>>,
    /// 0-based ordinal of the NEXT `await_signal` call within this
    /// (node, frames) execution. Increments on every call. Combined
    /// with the per-(node, frames) sequence above, it determines
    /// whether the call replays or registers fresh.
    next_call_index: AtomicU32,
    /// 0-based ordinal of the NEXT side-effect call (`log`)
    /// within this (node, frames). Separate counter from
    /// `next_call_index` so side effects don't shift the replay
    /// alignment of `await_signal` / `run_step`. Used to form
    /// stable dedup keys on the broker task table: under replay,
    /// the body re-runs and emits the same side-effect calls in
    /// the same order, so the dedup keys collapse to the same row.
    next_side_effect_index: AtomicU32,
    /// Number of `register_signal` (entry trigger) calls this node
    /// has made on this invocation. Entry triggers are a one-shot
    /// thing per node per TriggerSetup; we fail loudly on a second
    /// call rather than silently colliding on the dedup key.
    entry_register_count: AtomicU32,
    /// Channel to the loop driver for `pulse_downstream` emissions. The
    /// node task sends `TaskMsg::Emission`s here (the same channel its
    /// terminal `TaskMsg::Terminal` rides, so the loop sees emissions
    /// before the terminal); the loop applies them to the pulse table
    /// while the task keeps running. `None` only in unit tests that
    /// never emit.
    emit_tx: Option<mpsc::UnboundedSender<TaskMsg>>,
    /// Output ports this firing has already emitted on. A second
    /// `pulse_downstream` mentioning a port that's already in here is
    /// a node-author bug (each port can be emitted AT MOST ONCE per
    /// firing) and errors loud. Multiple `pulse_downstream` calls on
    /// DISJOINT ports are fine: that's the "release early then
    /// finalize" pattern (bus marker out, then `done` at the end).
    /// `Generator[T]` ports are EXEMPT from the once-only claim (each
    /// emission is one item of the stream) but still recorded here, so
    /// the no-emission-before-a-durable-suspend guard covers yields
    /// too. The once-only rule is not weakened by the exemption: it
    /// exists because a second pulse at one `(color, frames)` key has
    /// no identity a waiting consumer could reconcile, and a generator
    /// consumer never reconciles: it fires once and pulls items in
    /// arrival order.
    ///
    /// ONE lock for both sets: they answer the one question "what has
    /// this firing done to port P", and every check reads both, so a
    /// single lock makes the check-and-record atomic AND makes a
    /// lock-order inversion between them unspellable (a cloned ctx used
    /// from two spawned tasks could otherwise deadlock `close_port`
    /// against `pulse_downstream`, invisibly to the stuck-detector).
    port_claims: Mutex<PortClaims>,
    /// Per-stream un-taken buffer caps this body declared
    /// (`set_max_buffered_items`), keyed by output port. Each emission
    /// carries the `Arc` snapshot current when the message was
    /// CONSTRUCTED (a refcount bump, not a map clone, on the per-item
    /// hot path), so for a sequential body a declaration governs
    /// exactly the emissions after it; absent ports use
    /// [`weft_core::generator::DEFAULT_MAX_BUFFERED_ITEMS`]. Declaring
    /// caps concurrently with emitting from another task off a cloned
    /// ctx has no ordering guarantee (the snapshot is
    /// construction-time, not channel-time).
    stream_caps: Mutex<Arc<HashMap<String, usize>>>,
    /// Whether this node declares any `Generator[T]` INPUT. A stream
    /// consumer must not `await_signal`: a durable suspension replays
    /// the body from the top, and the already-pulled stream cannot be
    /// replayed (the producer's items were consumed live).
    has_generator_input: bool,
    /// The execution's shared wait tracker: this firing's delivery
    /// waits register here (its bus waits and generator pulls reach
    /// the same tracker through their own sources).
    waits: Arc<WaitTracker>,
    /// Per-execution bus coordinator. Owns the `BusRegistry`, the
    /// source of truth for which buses are live this execution.
    bus_coordinator: Arc<BusCoordinator>,
    /// Output ports this node declares in its metadata, name -> declared
    /// type. Used by `pulse_downstream` / `close_port` to reject emits on
    /// undeclared ports loudly at the API boundary (before the bad-shape
    /// value reaches the loop driver and silently routes through the
    /// (post)process layer's no-such-port fallthrough), AND to validate
    /// the TYPE of each emitted value against the port's declared type
    /// (an incompatible value is refused, the port closed, a
    /// `PortTypeMismatch` recorded).
    declared_outputs: HashMap<String, WeftType>,
    /// Wake event payload for this firing. `Some` only when this
    /// dispatch is the consumption of a `NodeKicked` for a firing
    /// trigger; `None` for every other dispatch (regular pulse-driven
    /// firings, manual-run roots without a payload, setup-phase runs).
    wake_payload: Option<Value>,
    /// Live caller connection for this EXECUTION. A cheap `Arc` clone of
    /// the one connection the loop driver holds for the color, threaded
    /// into every firing's handle so any node can reach the caller via
    /// `ctx.caller()`. `None` for durable runs and for any worker that
    /// did not receive a `live_connection` request. Per-execution, not
    /// per-firing: all firings of one color share the one caller.
    caller_connection: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    /// The grant id and resolved value map of the RUNTIME-OWNED (`Ours`)
    /// connections this firing opened (`ctx.open`), released by the
    /// loop driver when the node's body finishes (see
    /// [`Self::close_opened_accesses`]); nothing node-facing releases
    /// one. A their-own connection is never tracked: there is nothing
    /// of the runtime's to retire, and the user's own secrets must
    /// not travel back on a release.
    opened_accesses: Mutex<Vec<(String, std::collections::BTreeMap<String, String>)>>,
}

impl RunnerHandle {
    /// This firing's wait-liveness identity: the node id plus its loop
    /// frame stack. A loop running the same body N times in parallel has
    /// N distinct firings (one per frame), so their wait liveness never
    /// conflates.
    fn firing_location(&self) -> FiringLocation {
        FiringLocation::new(self.node_id.clone(), self.node_frames.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: String,
        project_id: String,
        color: Color,
        node_id: String,
        node_type: String,
        node_frames: weft_core::frames::LoopFrames,
        clients: EngineClients,
        published_service: Option<weft_core::AccessSpec>,
        pod_name: String,
        tenant_id: String,
        cancellation: Arc<CancellationFlag>,
        waits: Arc<WaitTracker>,
        bus_coordinator: Arc<BusCoordinator>,
        declared_outputs: HashMap<String, WeftType>,
        has_generator_input: bool,
    ) -> Self {
        Self {
            execution_id,
            project_id,
            color,
            node_id,
            node_type,
            node_frames,
            clients,
            published_service,
            pod_name,
            tenant_id,
            cancellation,
            awaited_sequence: Mutex::new(std::collections::VecDeque::new()),
            next_call_index: AtomicU32::new(0),
            next_side_effect_index: AtomicU32::new(0),
            entry_register_count: AtomicU32::new(0),
            emit_tx: None,
            port_claims: Mutex::new(PortClaims::default()),
            stream_caps: Mutex::new(Arc::new(HashMap::new())),
            has_generator_input,
            waits,
            bus_coordinator,
            declared_outputs,
            wake_payload: None,
            caller_connection: None,
            opened_accesses: Mutex::new(Vec::new()),
        }
    }

    /// The recipe this node publishes against, from its own metadata
    /// (resolved by the compiler). The service is not something the
    /// body says: it is declared once, in `publishes`, and read from
    /// here, so a body and its metadata cannot disagree about it.
    /// Wait for an address to answer, saying where the wait has got
    /// to on THIS node's own output.
    ///
    /// The breadcrumb goes through the journal like any other node
    /// log, so a run that is waiting on a slow workload says so where
    /// the person watching it is already looking.
    async fn wait_until_routable_logging(&self, url: &str, name: &str) -> WeftResult<()> {
        let lines = std::sync::Mutex::new(Vec::<String>::new());
        let outcome = wait_until_routable(
            url,
            name,
            self.clients.clock.as_ref(),
            self.cancellation.as_ref(),
            &|line: String| lines.lock().expect("breadcrumbs").push(line),
        )
        .await;
        // Shipped after the wait rather than inside it: the sink is
        // called from a plain closure, and journaling is async.
        for line in lines.into_inner().expect("breadcrumbs") {
            self.log(LogLevel::Info, line).await?;
        }
        outcome
    }

    fn published_spec(&self) -> WeftResult<weft_core::AccessSpec> {
        self.published_service.clone().ok_or_else(|| {
            WeftError::Config(
                "this node hands out a connection to a service it runs, but its metadata \
                 does not say which: add `\"publishes\": \"<service>\"` to it"
                    .to_string(),
            )
        })
    }

    /// Release every runtime-owned connection this firing opened (the
    /// only kind tracked; see `opened_accesses`). The loop driver
    /// calls this once the node's body has finished (any outcome); a
    /// release that fails is logged loudly rather than failing the
    /// node, because a runtime-supplied credential's own window is the
    /// backstop. Returns one entry per failed release (empty = all
    /// released), so callers that must surface a leak can.
    pub async fn close_opened_accesses(&self) -> Vec<String> {
        let opened: Vec<(String, std::collections::BTreeMap<String, String>)> =
            std::mem::take(&mut *self.opened_accesses.lock().unwrap());
        let mut failures = Vec::new();
        for (grant, values) in opened {
            let req = weft_broker_client::protocol::ReleaseConnectionRequest {
                color: self.color.to_string(),
                values,
            };
            if let Err(e) = self.clients.access_broker.release_connection(&req).await {
                tracing::error!(
                    target: "weft_engine::metering",
                    node = %self.node_id,
                    grant = %grant,
                    "releasing a connection failed (its window remains the backstop): {e:#}"
                );
                failures.push(format!("grant {grant}: {e:#}"));
            }
        }
        failures
    }

    /// Wire the emission channel. Called by the loop driver before
    /// dispatching the node so `pulse_downstream` can reach the loop.
    pub fn with_emit_channel(
        mut self,
        emit_tx: mpsc::UnboundedSender<TaskMsg>,
    ) -> Self {
        self.emit_tx = Some(emit_tx);
        self
    }

    /// Wire the wake payload for this dispatch. Called by the loop
    /// driver only when consuming a `NodeKicked` for a firing trigger;
    /// every other dispatch leaves `wake_payload` as None.
    pub fn with_wake_payload(mut self, payload: Value) -> Self {
        self.wake_payload = Some(payload);
        self
    }

    /// Wire the execution's live caller connection. Called by the loop
    /// driver for every firing of a color that has a caller attached, so
    /// `ctx.caller()` resolves on any node. Left `None` otherwise.
    pub fn with_caller_connection(
        mut self,
        conn: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    ) -> Self {
        self.caller_connection = conn;
        self
    }

    fn next_side_effect_index(&self) -> u32 {
        self.next_side_effect_index.fetch_add(1, Ordering::SeqCst)
    }

    /// Enqueue a durable side-effect task (a log line) on the broker.
    /// The broker's INSERT into `task` is the durable commit; once `Ok`
    /// returns, the dispatcher will journal the event regardless of
    /// whether this worker pod survives.
    ///
    /// `dedup_prefix` is the human-readable label used in the dedup
    /// key (`"log"`); the rest of the key keys to (color, node, frames,
    /// side-effect-index) so distinct calls in one node body produce
    /// distinct tasks while a retry of the same call (e.g. supervisor
    /// reconnect, body replay) collapses to one. Replay is honest
    /// because the body re-runs in the same order and emits the same
    /// side-effect sequence.
    async fn enqueue_side_effect_task<P: serde::Serialize>(
        &self,
        dedup_prefix: &str,
        kind: weft_task_store::TaskKind,
        payload: P,
    ) -> WeftResult<()> {
        let payload_json = serde_json::to_value(&payload).map_err(|e| {
            WeftError::Config(format!("{dedup_prefix} payload: {e}"))
        })?;
        let dedup_key = self.side_effect_dedup_key(dedup_prefix)?;
        self.clients
            .tasks
            .enqueue_dedup(weft_task_store::NewTask {
                kind: kind.into(),
                target: weft_task_store::TaskTarget::Dispatcher,
                project_id: Some(self.project_id.clone()),
                dedup_key: Some(dedup_key),
                color: Some(self.color.to_string()),
                tenant_id: Some(self.tenant_id.clone()),
                target_pod_name: None,
                binary_hash: None,
                payload: payload_json,
            })
            .await
            .map_err(|e| WeftError::Config(format!("{dedup_prefix} enqueue: {e}")))?;
        Ok(())
    }

    /// One idempotency key per side effect: retries of the SAME effect
    /// reuse the key (broker dedups), distinct effects of one firing get
    /// distinct indices.
    ///
    /// The index counts up from 0 and RESETS to 0 when a fresh worker
    /// re-runs this node body after a crash. So a FULL replay from the top
    /// re-emits each side effect at the same index and lands on the same
    /// key (dedup collapses it). This holds only while the body runs the
    /// same sequence of side effects on a replay as on the first run. A
    /// node that PAUSES mid-body (`await_signal` / `ctx.run`) and emits a
    /// DIFFERENT number of log lines before vs after the pause across runs
    /// would shift these indices and mis-key one (double-write or wrongly
    /// suppress). `log` is the only side effect keyed this way, and log
    /// output is observational, so a mis-keyed line is harmless.
    fn side_effect_dedup_key(&self, prefix: &str) -> WeftResult<String> {
        let frames_key = frames_dedup_key(&self.node_frames)
            .map_err(|e| WeftError::Config(format!("{prefix} frames key: {e}")))?;
        Ok(format!(
            "{prefix}:{color}:{node}:{frames_key}:{idx}",
            color = self.color,
            node = self.node_id,
            idx = self.next_side_effect_index(),
        ))
    }

    /// Seed the per-(node, frames) await-call sequence the loop
    /// driver pulled from the journal fold. Replaces
    /// `with_expected_token` from the single-await world: now we
    /// have an ordered sequence, not just one token.
    pub fn with_awaited_sequence(
        mut self,
        sequence: Vec<weft_core::primitive::AwaitedEntry>,
    ) -> Self {
        self.awaited_sequence = Mutex::new(sequence.into());
        self
    }

    /// Reject any port name the node didn't declare in its metadata.
    /// Runs BEFORE the one-emission-per-port gate so a typo'd port is
    /// surfaced as the actual bug (undeclared) rather than as a misleading
    /// "already emitted" error after a second misspelled emit. Without
    /// this, the loop driver's postprocess layer would route through its
    /// undeclared-port fallthrough and silently drop the emit, leaving
    /// a downstream Gather hanging.
    fn check_declared_outputs(&self, ports: &[String]) -> WeftResult<()> {
        for port_name in ports {
            if !self.declared_outputs.contains_key(port_name) {
                return Err(WeftError::NodeExecution(format!(
                    "node '{}' tried to emit on undeclared output port '{}'. \
                     Declare it in metadata.json's outputs list, or correct \
                     the port name in the node body.",
                    self.node_id, port_name
                )));
            }
        }
        Ok(())
    }

    /// Whether this node declares `port` as a `Generator[T]` output
    /// (a user alias whose body is one included).
    fn is_generator_output(&self, port: &str) -> bool {
        self.declared_outputs
            .get(port)
            .is_some_and(|t| t.as_generator().is_some())
    }

    /// Claim a set of output ports for this firing under the
    /// one-emission-per-port rule. Errors loud the first time any port
    /// would be claimed twice (whether by `pulse_downstream` re-emit or
    /// a `close_port` after an emit, in either order). `Generator[T]`
    /// ports are exempt from the once-only claim (every emission is one
    /// item of the stream) but are refused after their explicit close
    /// (a yield past the end). The check is transactional: if any port
    /// in `ports` collides, NONE is recorded, so the caller sees a
    /// clean "this attempt failed" instead of a partial mention that
    /// would poison later legitimate emissions.
    fn mention_or_err(&self, ports: &[String]) -> WeftResult<()> {
        let mut claims = self.lock_port_claims();
        for port_name in ports {
            if claims.ended_streams.contains(port_name) {
                return Err(WeftError::NodeExecution(format!(
                    "node '{}' yielded on stream port '{}' after closing it; a close \
                     ends the stream, nothing can follow it.",
                    self.node_id, port_name
                )));
            }
            if claims.mentioned.contains(port_name) && !self.is_generator_output(port_name) {
                return Err(WeftError::NodeExecution(format!(
                    "node '{}' touched port '{}' twice in one firing. \
                     Each output port can be emitted or closed AT MOST ONCE per \
                     firing; release ports incrementally (e.g. a bus marker early, \
                     a `done` flag at the end) but never re-emit or re-close a port.",
                    self.node_id, port_name
                )));
            }
        }
        for port_name in ports {
            claims.mentioned.insert(port_name.clone());
        }
        Ok(())
    }

    fn lock_port_claims(&self) -> std::sync::MutexGuard<'_, PortClaims> {
        self.port_claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_awaited_sequence(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::VecDeque<weft_core::primitive::AwaitedEntry>>
    {
        self.awaited_sequence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Ship an `EmitMsg` to the loop driver. Errors loud if the handle
    /// has no emit channel (runtime wiring bug) or the loop receiver is
    /// closed (loop dropped while the node task was still running).
    fn send_emission(
        &self,
        kind: EmitKind,
        delivery: Option<Arc<DeliveryGate>>,
    ) -> WeftResult<()> {
        let Some(tx) = self.emit_tx.as_ref() else {
            return Err(WeftError::Config(
                "emission called on a handle with no emit channel \
                 (this is a runtime wiring bug)"
                    .into(),
            ));
        };
        // Snapshot bound to a local so no lock is held across the send.
        let stream_caps = self
            .stream_caps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        tx.send(TaskMsg::Emission(EmitMsg {
            loc: self.firing_location(),
            kind,
            delivery,
            stream_caps,
        }))
        .map_err(|_| {
            WeftError::Runtime(anyhow::anyhow!(
                "emission: loop driver receiver closed"
            ))
        })?;
        Ok(())
    }

    /// Journal a non-terminal output-type mismatch for `port`: the node
    /// tried to emit `value` on a port declared `declared`, the types are
    /// incompatible, so the engine closed the port instead. Folds into the
    /// node execution's `port_warnings` and surfaces as a UI warning.
    /// Does not change the node's status.
    async fn record_port_type_mismatch(&self, port: &str, declared: &WeftType, value: &Value) {
        record_from_pod(
            self.clients.journal.as_ref(),
            ExecEvent::PortTypeMismatch {
                color: self.color,
                node_id: self.node_id.clone(),
                frames: self.node_frames.clone(),
                port: port.to_string(),
                expected: declared.to_string(),
                actual: WeftType::infer(value).to_string(),
                at_unix: now_unix(),
            },
            &self.pod_name,
        )
        .await;
    }
}

/// Ship the lifecycle event for a fresh dispatch of (node, frames):
/// `NodeResumed` if this dispatch is resuming a prior firing (either
/// suspension-resolved with a token/value, or crashed-Running recovery
/// with both None), otherwise `NodeStarted`. The audit's load-bearing
/// job in both cases is journaling `pulses_absorbed` so a later
/// crashed-Running un-absorb sees every pulse this dispatch consumed
/// (the fold rebuilds the record's `pulses_absorbed` from journaled
/// events; without this event, resume-time absorbs leak).
///
/// Invalid combo (is_resume=false AND resume_token_value=Some) panics
/// in debug: a caller flipping one but not the other would silently
/// write a NodeStarted while discarding a resume value.
#[allow(clippy::too_many_arguments)]
pub async fn ship_node_lifecycle(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    input: &serde_json::Value,
    closed_ports: &[String],
    pulses_absorbed: &[uuid::Uuid],
    resume_token_value: Option<&(String, serde_json::Value)>,
    is_resume: bool,
) {
    debug_assert!(
        is_resume || resume_token_value.is_none(),
        "ship_node_lifecycle: resume_token_value passed with is_resume=false"
    );
    let event = if is_resume {
        let (token, value) = match resume_token_value {
            Some((t, v)) => (Some(t.clone()), Some(v.clone())),
            None => (None, None),
        };
        ExecEvent::NodeResumed {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            token,
            value,
            pulses_absorbed: pulses_absorbed.iter().map(|u| u.to_string()).collect(),
            at_unix: now_unix(),
        }
    } else {
        ExecEvent::NodeStarted {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            input: input.clone(),
            closed_ports: closed_ports.to_vec(),
            pulses_absorbed: pulses_absorbed.iter().map(|u| u.to_string()).collect(),
            at_unix: now_unix(),
        }
    };
    record_from_pod(journal, event, pod_name).await;
}

pub async fn ship_node_suspended(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    token: &str,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeSuspended {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            token: token.to_string(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

/// Ship a terminal NodeCompleted carrying its unmentioned-port closures
/// atomically (see `NodeFailed.closure_emissions` in weft-journal): the
/// marker and the closures fold as one unit, so a crash between them
/// can't lose the closures and strand downstream consumers.
pub async fn ship_node_completed(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    output: &serde_json::Value,
    closures: Vec<weft_core::exec::PulseEmission>,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeCompleted {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            output: output.clone(),
            closure_emissions: closures.into_iter().map(Into::into).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_failed(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    error: &str,
    closures: Vec<weft_core::exec::PulseEmission>,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeFailed {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            error: error.to_string(),
            closure_emissions: closures.into_iter().map(Into::into).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

pub async fn ship_node_skipped(
    journal: &dyn JournalClient,
    pod_name: &str,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    closed_ports: &[String],
    reason: &weft_core::exec::skip::SkipReason,
    closures: Vec<weft_core::exec::PulseEmission>,
) {
    record_from_pod(
        journal,
        ExecEvent::NodeSkipped {
            color,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            closed_ports: closed_ports.to_vec(),
            reason: Some(reason.clone()),
            closure_emissions: closures.into_iter().map(Into::into).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
}

/// Ship every pulse the engine just emitted by writing one journal
/// event per emission. Order is preserved because journal rows have
/// monotonic ids; the fold replays them in insertion order.
pub async fn ship_pulse_emissions(
    journal: &dyn JournalClient,
    pod_name: &str,
    emissions: Vec<weft_core::exec::PulseEmission>,
) {
    for e in emissions {
        let p = e.pulse;
        record_from_pod(
            journal,
            ExecEvent::PulseEmitted {
                color: p.color,
                pulse_id: p.id.to_string(),
                source_node: e.source_node,
                source_port: e.source_port,
                target_node: p.target_node,
                target_port: p.target_port,
                frames: p.frames,
                value: p.value,
                closed: p.closed,
                close_error: p.close_error,
                at_unix: now_unix(),
            },
            pod_name,
        )
        .await;
    }
}

#[async_trait]
impl ContextHandle for RunnerHandle {
    /// Wait-and-resume primitive. Generalized to N awaits per body:
    /// each call has a 0-based call_index keyed on (node, frames).
    /// On every dispatch, the runtime pre-loads the per-(node, frames)
    /// awaited sequence from the journal fold:
    ///
    /// 1. **Replay path** (entry has `resolved=Some`): the call's
    ///    fire arrived in a prior cycle; return the stored value
    ///    instantly. Body keeps running.
    /// 2. **Suspend path** (entry has `resolved=None`): the call's
    ///    suspension is the still-pending tail of the sequence;
    ///    propagate `WeftError::Suspended` to release the worker.
    /// 3. **Fresh path** (sequence exhausted): no past entry for
    ///    this call_index; this is the first time this await runs.
    ///    Enqueue `register_signal` with is_resume=true and the
    ///    current call_index, then propagate `Suspended`. The next
    ///    re-dispatch after the fire will see this entry resolved.
    ///
    /// The author writes `let x = ctx.await_signal(...).await?;` N
    /// times in a row; each call is replay-instant on resume.
    async fn await_signal(&self, spec: SignalSpec) -> WeftResult<Value> {
        // Reconcile the two execution worlds. The wait policy (hold vs
        // suspend + hold time) resolves from the run's SuspendPolicy
        // through the general `wait` machinery; a live caller only supplies
        // that policy. We do NOT fail here on a non-suspendable run that
        // wants to suspend: other branches may still be running and talking
        // to the caller, so the kill/disconnect fires ONCE later, at the
        // true suspension point (the loop driver, when every branch is
        // parked), never when one branch reaches a wait. The driver also
        // owns the "hold the worker warm" decision (it treats an attached
        // caller like a live bus). So there is nothing to special-case in
        // this per-await path; the suspend path below runs unchanged.

        // A durable suspension replays the whole node body on resume.
        // Emitting (`pulse_downstream`) before a durable await is
        // unsound: the replay would re-run the body and re-emit the
        // already-sent pulses (the journal doesn't memoize emissions
        // the way it memoizes awaits/run), duplicating downstream
        // work. A node that holds a live bus must NOT durably suspend
        // anyway: it stays warm and uses bus.recv() instead.
        if !self.lock_port_claims().mentioned.is_empty() {
            return Err(WeftError::NodeExecution(
                weft_core::context::emitted_then_await_signal_error(&self.node_id),
            ));
        }
        // A stream CONSUMER cannot durably suspend either: the resume
        // replays the body from the top, and the items its earlier
        // pulls consumed were delivered live and cannot be replayed.
        if self.has_generator_input {
            return Err(WeftError::NodeExecution(
                weft_core::context::stream_consumer_await_signal_error(&self.node_id),
            ));
        }
        let call_index = self.next_call_index.fetch_add(1, Ordering::SeqCst);

        // Pop the next pre-loaded entry (if any). Replay vs suspend
        // depends on whether its fire already arrived.
        let next_entry = self.lock_awaited_sequence().pop_front();
        if let Some(entry) = next_entry {
            // Sanity-check call_index alignment. If the journal's
            // sequence disagrees with our counter, something
            // replayed out of order; the body is non-deterministic
            // (or the journal is corrupt). Fail the node loudly
            // rather than masking it as a suspension.
            if entry.call_index != call_index {
                return Err(WeftError::NodeExecution(format!(
                    "await_signal call_index mismatch (counter={call_index}, journal={}). \
                     This means the node body's call order changed between replays. \
                     Wrap any non-deterministic logic between awaits in `ctx.run`.",
                    entry.call_index
                )));
            }
            match entry.kind {
                weft_core::primitive::AwaitedEntryKind::Await {
                    token,
                    resolved,
                } => match resolved {
                    Some(value) => return Ok(value),
                    None => {
                        // Pending tail: fire hasn't arrived. Suspend
                        // with the existing token so the dispatcher
                        // doesn't re-register. If a bus is holding the
                        // worker alive, the driver's in-loop resume poll
                        // picks up this token's SuspensionResolved row
                        // when it lands and re-dispatches in process; if
                        // not, the worker exits and respawns on the fire.
                        return Err(WeftError::Suspended { token });
                    }
                },
                weft_core::primitive::AwaitedEntryKind::Run { name, .. } => {
                    return Err(WeftError::NodeExecution(format!(
                        "await_signal at call_index={call_index} but journal has Run('{name}'). \
                         This means the node body called `ctx.run` here on a previous run \
                         and `ctx.await_signal` now; non-deterministic bodies are not safe \
                         to replay. Use `ctx.run` to wrap non-deterministic work."
                    )));
                }
            }
        }

        // Sequence exhausted: this is a fresh await.
        // Enqueue a register_signal task carrying our call_index so
        // the dispatcher journals SuspensionRegistered with the right
        // ordinal. Body propagates Suspended afterwards.
        let reply = enqueue_register_signal_task(
            self.clients.tasks.as_ref(),
            self.color,
            &self.node_id,
            &self.node_frames,
            &spec,
            true,
            &self.tenant_id,
            call_index,
            // A resume registration wakes THIS parked firing; ports are
            // live in the body, nothing to replay later.
            None,
        )
        .await
        .map_err(|e| WeftError::Suspension(format!("request token: {e}")))?;
        tracing::info!(
            target: "weft_engine::suspend",
            node = %self.node_id,
            color = %self.color,
            call_index = call_index,
            token = %reply.token,
            "await_signal: registered; returning Suspended",
        );
        Err(WeftError::Suspended { token: reply.token })
    }

    /// Replay-side of `ctx.run`. Pops the next entry in the
    /// (node, frames) sequence; if it's a Run with our call_index,
    /// return its journaled value. If it's an Await at our index,
    /// the body's call sequence drifted from the journal: error
    /// loudly. If the sequence is exhausted, return None to signal
    /// "fresh path" so the wrapper invokes the closure.
    async fn run_step(&self, name: &str) -> WeftResult<(u32, Option<Value>)> {
        let call_index = self.next_call_index.fetch_add(1, Ordering::SeqCst);
        let next_entry = self.lock_awaited_sequence().pop_front();
        match next_entry {
            Some(entry) => {
                if entry.call_index != call_index {
                    return Err(WeftError::NodeExecution(format!(
                        "ctx.run('{name}') call_index mismatch (counter={call_index}, journal={}). \
                         This means the node body's call order changed between replays. \
                         Wrap any non-deterministic logic in `ctx.run`.",
                        entry.call_index
                    )));
                }
                match entry.kind {
                    weft_core::primitive::AwaitedEntryKind::Run { value, .. } => {
                        Ok((call_index, Some(value)))
                    }
                    weft_core::primitive::AwaitedEntryKind::Await { .. } => {
                        Err(WeftError::NodeExecution(format!(
                            "ctx.run('{name}') at call_index={call_index} but journal has Await. \
                             This means the node body called `ctx.await_signal` here on a previous \
                             run and `ctx.run` now; non-deterministic bodies are not safe to \
                             replay."
                        )))
                    }
                }
            }
            None => Ok((call_index, None)),
        }
    }

    async fn run_record(&self, name: &str, call_index: u32, value: &Value) -> WeftResult<()> {
        // call_index is the value run_step returned, passed in so
        // run_step and run_record agree on the index explicitly
        // rather than via a shared counter both sides read.
        record_from_pod(
            self.clients.journal.as_ref(),
            ExecEvent::RunOutput {
                color: self.color,
                node_id: self.node_id.clone(),
                frames: self.node_frames.clone(),
                call_index,
                name: name.to_string(),
                value: value.clone(),
                at_unix: now_unix(),
            },
            &self.pod_name,
        )
        .await;
        Ok(())
    }

    async fn storage_put(
        &self,
        scope: &weft_core::storage::StorageScope,
        data: weft_core::storage::ByteStream,
        mime_type: &str,
        filename: &str,
        keep: Option<weft_core::storage::KeepTtl>,
        declared_size: Option<u64>,
    ) -> WeftResult<Value> {
        self.clients
            .storage
            .put(self.color, scope, mime_type, filename, keep, declared_size, data)
            .await
    }

    async fn storage_put_from_url(
        &self,
        scope: &weft_core::storage::StorageScope,
        url: &str,
        filename: Option<&str>,
        keep: Option<weft_core::storage::KeepTtl>,
    ) -> WeftResult<Value> {
        // Reuse the process-wide pooled client (a fresh Client::new()
        // per fetch rebuilds the connection pool; see http_client).
        let resp = http_client()
            .get(url)
            .send()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("fetch {url}: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(WeftError::NodeExecution(format!(
                "fetch {url} returned {status}: {}",
                weft_core::truncate_user_string(&body, 512)
            )));
        }
        let mime = weft_core::storage::normalize_content_type(
            resp.headers().get("content-type").and_then(|v| v.to_str().ok()),
        );
        let name = filename
            .filter(|f| !f.is_empty())
            .map(String::from)
            .unwrap_or_else(|| weft_core::storage::filename_from_url(url));
        // A sized response body (Content-Length) is declared up front so the
        // whole quota charge happens before the first byte moves.
        let declared_size = resp.content_length();
        // Stream the body straight through (never buffer the whole
        // file), so a multi-gigabyte download stays bounded-memory.
        let stream: weft_core::storage::ByteStream = Box::pin(
            resp.bytes_stream()
                .map_err(|e| std::io::Error::other(format!("fetch stream: {e}"))),
        );
        self.clients.storage.put(self.color, scope, &mime, &name, keep, declared_size, stream).await
    }

    async fn storage_get(
        &self,
        key: &str,
        range: Option<weft_core::storage::ByteRange>,
    ) -> WeftResult<(weft_core::storage::StoredFileMeta, weft_core::storage::ByteStream)> {
        self.clients.storage.get(self.color, key, range).await
    }

    async fn storage_get_url(
        &self,
        url: &str,
        declared_mime: &str,
        declared_filename: &str,
        declared_size: u64,
        range: Option<weft_core::storage::ByteRange>,
    ) -> WeftResult<(weft_core::storage::StoredFileMeta, weft_core::storage::ByteStream)> {
        // Fetch the external URL directly (worker-side, isolated). A byte range
        // is passed through as a Range header so a piecewise read never buffers
        // the whole resource; a server that ignores Range simply returns the
        // full body (the caller's range logic still slices correctly).
        let mut req = http_client().get(url);
        if let Some(r) = range {
            let header = match r.end {
                Some(e) if e < r.start => {
                    return Err(WeftError::NodeExecution(format!(
                        "invalid byte range: end {e} precedes start {}",
                        r.start
                    )));
                }
                Some(e) if e == r.start => {
                    let meta = weft_core::storage::StoredFileMeta {
                        key: String::new(),
                        mime_type: declared_mime.to_string(),
                        size_bytes: declared_size,
                        filename: declared_filename.to_string(),
                        keep: false,
                        expires_at_unix: None,
                        keep_ttl_secs: None,
                        created_at_unix: 0,
                    };
                    return Ok((meta, weft_core::storage::bytes_stream(bytes::Bytes::new())));
                }
                Some(e) => format!("bytes={}-{}", r.start, e - 1),
                None => format!("bytes={}-", r.start),
            };
            req = req.header("range", header);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("fetch {url}: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(WeftError::NodeExecution(format!(
                "fetch {url} returned {status}: {}",
                weft_core::truncate_user_string(&body, 512)
            )));
        }
        // The response's own Content-Type is authoritative for the bytes; fall
        // back to the marker's declared mime when the server omits one (rather
        // than the generic octet-stream `normalize_content_type` would give).
        // Same for the size (Content-Length when present, else the declared).
        let ct = resp.headers().get("content-type").and_then(|v| v.to_str().ok());
        let mime = match ct {
            Some(ct) => weft_core::storage::normalize_content_type(Some(ct)),
            None => declared_mime.to_string(),
        };
        let size = resp.content_length().unwrap_or(declared_size);
        let meta = weft_core::storage::StoredFileMeta {
            key: String::new(),
            mime_type: mime,
            size_bytes: size,
            filename: declared_filename.to_string(),
            keep: false,
            expires_at_unix: None,
            keep_ttl_secs: None,
            created_at_unix: 0,
        };
        let stream: weft_core::storage::ByteStream = Box::pin(
            resp.bytes_stream()
                .map_err(|e| std::io::Error::other(format!("fetch stream: {e}"))),
        );
        Ok((meta, stream))
    }

    async fn storage_delete(&self, key: &str) -> WeftResult<()> {
        self.clients.storage.delete(self.color, key).await
    }

    async fn storage_list(
        &self,
        scope: &weft_core::storage::StorageScope,
    ) -> WeftResult<Vec<weft_core::storage::StoredFileMeta>> {
        self.clients.storage.list(self.color, scope).await
    }

    async fn storage_keep(
        &self,
        key: &str,
        ttl: weft_core::storage::KeepTtl,
    ) -> WeftResult<()> {
        self.clients.storage.keep(self.color, key, ttl).await
    }

    async fn storage_presign(&self, key: &str, ttl_secs: Option<u64>) -> WeftResult<String> {
        self.clients.storage.presign(self.color, key, ttl_secs).await
    }

    async fn storage_public_link(&self, key: &str, ttl_secs: Option<u64>) -> WeftResult<Option<String>> {
        self.clients.storage.public_link(self.color, key, ttl_secs).await
    }

    async fn endpoint_url(&self, name: &str) -> WeftResult<String> {
        let endpoint = self
            .clients
            .infra
            .endpoint_url(&self.project_id, &self.node_id, name)
            .await
            .map_err(|e| WeftError::Config(format!("infra_node lookup: {e}")))?;
        let url = endpoint.ok_or_else(|| {
            WeftError::Config(format!(
                "endpoint '{}' for node '{}' is not available; either the infra isn't running \
                 or the endpoint name is not declared. Check `weft infra status` and the node's \
                 InfraSpec.endpoints list.",
                name, self.node_id
            ))
        })?;
        self.wait_until_routable_logging(&url, &format!("endpoint '{name}'")).await?;
        Ok(url)
    }

    async fn endpoint_call(
        &self,
        base: &str,
        method: weft_core::EndpointMethod,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> WeftResult<serde_json::Value> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let client = http_client();
        let req = match method {
            weft_core::EndpointMethod::Get => client.get(&url),
            weft_core::EndpointMethod::Post => {
                let mut r = client.post(&url);
                if let Some(b) = &body {
                    r = r.json(b);
                }
                r
            }
        };
        let Some(first) = req.try_clone() else {
            return Err(WeftError::Runtime(anyhow::anyhow!(
                "endpoint_call {url}: the request body cannot be retried"
            )));
        };
        let resp = match first.send().await {
            Ok(resp) => resp,
            // A CONNECT failure means the address stopped answering
            // between being resolved and being called, which is the
            // same routing gap `ctx.endpoint` waits out and the same
            // way out: wait for it to come back, then ask once more.
            // A handle is resolved once and used across several
            // calls, so the workload restarting mid-run lands here
            // rather than at resolution.
            //
            // Only a connect failure. A refusal from the service, a
            // timeout, a TLS failure: those are ANSWERS, and they
            // surface immediately.
            Err(e) if e.is_connect() => {
                self.wait_until_routable_logging(base, "this node's own service").await?;
                req.send().await.map_err(|e| {
                    WeftError::Runtime(anyhow::anyhow!("endpoint_call {url}: {e}"))
                })?
            }
            Err(e) => {
                return Err(WeftError::Runtime(anyhow::anyhow!("endpoint_call {url}: {e}")))
            }
        };
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(WeftError::Runtime(anyhow::anyhow!(
                "endpoint_call {url} returned {status}: {body}"
            )));
        }
        resp.json::<serde_json::Value>().await.map_err(|e| {
            WeftError::Runtime(anyhow::anyhow!(
                "endpoint_call {url} response not JSON: {e}"
            ))
        })
    }

    /// Entry-trigger registration. Synchronous: enqueues the
    /// register_signal task and waits for the dispatcher to ack.
    /// Worker keeps executing; the signal stays armed and
    /// persistent until the project is deactivated. Each external
    /// fire spawns a fresh execution (the dispatcher's relay path
    /// enqueues route_entry instead of resuming this firing).
    /// Distinct from await_signal: no suspend-then-resume cycle.
    async fn register_signal(&self, spec: SignalSpec, port_snapshot: Value) -> WeftResult<()> {
        // Entry triggers are one-shot per node per TriggerSetup.
        // The dedup key for this enqueue is `(color, node, frames,
        // is_resume=false, call_index=0)`; a second call from the
        // same node body would collide and silently drop the
        // second trigger. Catch that loudly here so the offending
        // node fails instead of a trigger going missing.
        let prev = self.entry_register_count.fetch_add(1, Ordering::SeqCst);
        if prev > 0 {
            return Err(WeftError::Config(format!(
                "node '{}' called ctx.register_signal more than once; \
                 entry triggers are one-per-node-per-TriggerSetup",
                self.node_id
            )));
        }
        let reply = enqueue_register_signal_task(
            self.clients.tasks.as_ref(),
            self.color,
            &self.node_id,
            &self.node_frames,
            &spec,
            false,
            &self.tenant_id,
            0,
            Some(port_snapshot),
        )
        .await
        .map_err(|e| WeftError::Suspension(format!("register_signal: {e}")))?;
        tracing::info!(
            target: "weft_engine::register",
            node = %self.node_id,
            color = %self.color,
            token = %reply.token,
            "register_signal: dispatcher ack"
        );
        Ok(())
    }

    async fn publish_access(
        &self,
        values: std::collections::BTreeMap<String, String>,
    ) -> WeftResult<weft_core::access::Access> {
        let spec = self.published_spec()?;
        let service = spec.service.clone();
        let req = weft_broker_client::protocol::PublishAccessRequest {
            color: self.color.to_string(),
            node_id: self.node_id.clone(),
            service: service.clone(),
            spec,
            values,
            // The connection list's middle column. The node id reads
            // as what it is: the thing in this project that opened it.
            label: Some(self.node_id.clone()),
        };
        let resp = self.clients.access_broker.publish_access(&req).await.map_err(|e| {
            WeftError::NodeExecution(format!("publish the '{service}' connection: {e:#}"))
        })?;
        Ok(weft_core::access::Access::new(
            resp.connection.connection_id,
            service,
            resp.connection.identity,
        ))
    }

    async fn published_access(&self) -> WeftResult<Option<weft_core::access::Access>> {
        let service = self.published_spec()?.service.clone();
        let req = weft_broker_client::protocol::PublishedAccessRequest {
            color: self.color.to_string(),
            node_id: self.node_id.clone(),
            service: service.clone(),
        };
        let resp = self.clients.access_broker.published_access(&req).await.map_err(|e| {
            WeftError::NodeExecution(format!(
                "look up the '{service}' connection this node published: {e:#}"
            ))
        })?;
        Ok(resp
            .connection()
            .map(|c| weft_core::access::Access::new(c.connection_id, service, c.identity)))
    }

    async fn open_connection(
        &self,
        access: &weft_core::access::Access,
        window: std::time::Duration,
    ) -> WeftResult<weft_core::access::OpenedConnection> {
        let service = access.service().to_string();
        let req = weft_broker_client::protocol::ResolveConnectionRequest {
            color: self.color.to_string(),
            node_id: self.node_id.clone(),
            frames: self.node_frames.clone(),
            node_type: self.node_type.clone(),
            connection_id: access.access_id().to_string(),
            service: service.clone(),
            required_permissions: access.required_permissions().to_vec(),
            required_values: access.required_values().to_vec(),
            expected_duration_secs: window.as_secs(),
        };
        let resp = self.clients.access_broker.resolve_connection(&req).await.map_err(|e| {
            WeftError::NodeExecution(format!("open the '{service}' connection: {e:#}"))
        })?;
        // Remember the lease so the loop driver releases it when this
        // node's body finishes (`close_opened_accesses`). Only an
        // Ours-owned connection is remembered: a release retires a
        // runtime-supplied credential, and a their-own connection's
        // stored values are the user's own and never travel back.
        if resp.owner == weft_core::CredentialOwner::Ours {
            self.opened_accesses
                .lock()
                .unwrap()
                .push((access.access_id().to_string(), resp.values.clone()));
        }

        let steps = weft_core::access::client::resolve_steps(&resp.auth, &resp.values)
            .map_err(|e| {
                WeftError::NodeExecution(format!(
                    "the '{service}' connection's auth steps do not resolve: {e}"
                ))
            })?;
        let sink = Arc::new(crate::metering::CostSink {
            tasks: self.clients.tasks.clone(),
            pending: self.clients.pending_costs.clone(),
            project_id: self.project_id.clone(),
            tenant_id: self.tenant_id.clone(),
            color: self.color,
            node_id: self.node_id.clone(),
            frames: self.node_frames.clone(),
            service: service.clone(),
            origin: resp.owner,
        });
        let client = crate::metering::connection_client(
            &service,
            steps.clone(),
            resp.relay_url.as_deref(),
            sink.clone(),
        )?;
        let dialer = Arc::new(crate::socket::ConnectionSocketDial::new(
            steps,
            resp.relay_url.clone(),
            sink,
        ));
        Ok(weft_core::access::OpenedConnection::assemble(
            service,
            resp.values,
            resp.auth,
            resp.identity,
            resp.owner,
            client,
            dialer,
        ))
    }

    async fn log(&self, level: LogLevel, message: String) -> WeftResult<()> {
        let level_str = match level {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        };
        tracing::info!(
            target: "weft_engine::node",
            exec = %self.execution_id,
            level = level_str,
            "{message}"
        );
        self.enqueue_side_effect_task(
            "log",
            weft_task_store::TaskKind::RecordLog,
            weft_task_store::RecordLogPayload {
                color: self.color.to_string(),
                level: level_str.to_string(),
                message,
            },
        )
        .await
    }

    fn cancellation(&self) -> Arc<CancellationFlag> {
        self.cancellation.clone()
    }

    fn declared_output_ports(&self) -> &HashMap<String, WeftType> {
        &self.declared_outputs
    }

    /// Fire downstream. Each output port the node mentions in
    /// `output` becomes a pulse on its outgoing edges; mentioned-but-
    /// already-emitted ports error loud (one-emission-per-port rule;
    /// `Generator[T]` ports accept repeats, each emission one item).
    /// Calling `pulse_downstream` multiple times with DISJOINT ports
    /// is fine (release early then finalize); calling it twice with
    /// OVERLAPPING non-generator ports is a node-author bug.
    /// `wait_delivered` parks this body until every pulse the call
    /// created has been absorbed (the consumer dispatched / the item
    /// pulled), failing loudly when that can never happen.
    async fn pulse_downstream(&self, output: NodeOutput, wait_delivered: bool) -> WeftResult<()> {
        let ports: Vec<String> = output.outputs.keys().cloned().collect();
        self.check_declared_outputs(&ports)?;

        // Runtime output-type check, classification FIRST with no
        // claims, no journaling, no sends: an error below must leave
        // the call a clean no-op (a partial mention would refuse a
        // later legitimate re-attempt as "touched twice"). Each value
        // must be compatible with its port's DECLARED type (which
        // already reflects any narrowing the author applied in the
        // node header). An incompatible value on a plain port is
        // refused: the port is recorded as a non-terminal
        // PortTypeMismatch and CLOSED (downstream sees null) instead
        // of letting the wrong-typed value flow.
        //
        // A GENERATOR port (asked via `as_generator`, so a nominal
        // alias behaves identically) checks each emission against the
        // ELEMENT type, and a mistyped item FAILS the whole call
        // instead of closing the port: closing would silently end a
        // live stream mid-flight and read downstream as a clean
        // finish, which is exactly the masked-truncation failure the
        // typed stream exists to prevent.
        let mut kept = NodeOutput::new();
        let mut mismatched: Vec<(String, Value)> = Vec::new();
        for (port, value) in output.outputs {
            match self.declared_outputs.get(&port).map(|d| (d, d.as_generator())) {
                Some((_, Some(element))) if !type_accepts(element, &value) => {
                    return Err(WeftError::NodeExecution(format!(
                        "node '{}' yielded a value on stream port '{}' that the element \
                         type '{}' does not accept (got {})",
                        self.node_id,
                        port,
                        element,
                        WeftType::infer(&value),
                    )));
                }
                Some((declared, None)) if !type_accepts(declared, &value) => {
                    mismatched.push((port, value));
                }
                _ => {
                    kept.outputs.insert(port, value);
                }
            }
        }

        // The call is now known to proceed: claim every touched port
        // (the one-emission-per-port rule). A type-mismatched port is
        // still "touched": it gets closed instead of emitted, so it
        // must be claimed too, or a later legitimate emit on it would
        // slip past.
        self.mention_or_err(&ports)?;
        for (port, value) in mismatched {
            let declared = self
                .declared_outputs
                .get(&port)
                .expect("classified above from this same map");
            self.record_port_type_mismatch(&port, declared, &value).await;
            self.send_emission(EmitKind::Close(port), None)?;
        }
        if !wait_delivered {
            if !kept.outputs.is_empty() {
                self.send_emission(EmitKind::Values(kept), None)?;
            }
            return Ok(());
        }
        // Delivery-waiting emission that keeps no ports: NOTHING was
        // emitted (empty output, or every value refused by its port's
        // declared type). The caller's contract is "block until this
        // was taken"; reporting success over a handoff that never
        // happened would be a silent no-op, so fail loud instead.
        if kept.outputs.is_empty() {
            return Err(WeftError::NodeExecution(format!(
                "node '{}' called yield_downstream but nothing was emitted (the output \
                 was empty, or every value was refused by its port's declared type); \
                 there is nothing whose delivery could be awaited",
                self.node_id
            )));
        }
        let gate = DeliveryGate::new();
        self.send_emission(EmitKind::Values(kept), Some(gate.clone()))?;
        let liveness = Arc::downgrade(&self.waits) as std::sync::Weak<dyn WaitLiveness>;
        gate.wait_delivered(&liveness, self.firing_location()).await
    }

    fn set_max_buffered_items(&self, port: &str, items: usize) -> WeftResult<()> {
        if !self.is_generator_output(port) {
            return Err(WeftError::NodeExecution(format!(
                "node '{}' called set_max_buffered_items on '{port}', which is not a \
                 Generator output it declares",
                self.node_id
            )));
        }
        if items == 0 {
            return Err(WeftError::NodeExecution(format!(
                "node '{}': set_max_buffered_items(0) on '{port}'; a cap of 0 could never \
                 accept even the first item",
                self.node_id
            )));
        }
        // Clone-on-write into a fresh Arc: emissions snapshot the Arc
        // (cheap), so the map is never mutated behind a snapshot.
        let mut caps = self
            .stream_caps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut next = (**caps).clone();
        next.insert(port.to_string(), items);
        *caps = Arc::new(next);
        Ok(())
    }

    /// Close a single output port mid-firing. Goes through the same
    /// one-emission-per-port gate as `pulse_downstream`, then ships an
    /// `EmitKind::Close` so the loop driver emits a closure pulse on
    /// every outgoing edge of that port at this firing's frame stack, the
    /// same shape the termination-time sweep would produce. On a
    /// `Generator[T]` port this is the EARLY end-of-stream verb: legal
    /// after any number of yields, once.
    async fn close_port(&self, port: &str) -> WeftResult<()> {
        let port = port.to_string();
        self.check_declared_outputs(std::slice::from_ref(&port))?;
        if self.is_generator_output(&port) {
            // One lock for the end-and-mention record (the same
            // `port_claims` lock every emission check takes), so the
            // claim is atomic and cannot order-invert against
            // `mention_or_err`.
            let mut claims = self.lock_port_claims();
            if !claims.ended_streams.insert(port.clone()) {
                return Err(WeftError::NodeExecution(format!(
                    "node '{}' closed stream port '{}' twice; a stream ends once.",
                    self.node_id, port
                )));
            }
            // Also recorded as a mention so the no-emission-before-a-
            // durable-suspend guard covers the explicit end too.
            claims.mentioned.insert(port.clone());
        } else {
            self.mention_or_err(std::slice::from_ref(&port))?;
        }
        self.send_emission(EmitKind::Close(port), None)
    }

    fn create_bus(&self, opts: BusOptions) -> WeftResult<(BusHandle, Value)> {
        let handle = self
            .bus_coordinator
            .new_bus(opts, self.firing_location())
            .map_err(|e| WeftError::Input(format!("ctx.create_bus on node '{}': {e}", self.node_id)))?;
        let marker = handle.marker();
        Ok((handle, marker))
    }

    fn bus(&self, marker: &Value) -> WeftResult<BusHandle> {
        self.bus_coordinator
            .lookup_bus(marker, self.firing_location())
            .map_err(|e| {
                WeftError::Input(format!(
                    "ctx.bus on node '{}': {e}",
                    self.node_id
                ))
            })
    }

    fn wake_payload(&self) -> Option<&Value> {
        self.wake_payload.as_ref()
    }

    fn caller_connection(&self) -> Option<Arc<dyn weft_core::caller::CallerConnection>> {
        self.caller_connection.clone()
    }
}

/// Reply shape from a `register_signal` task. Mirrors
/// `weft-dispatcher::task_kinds::register_signal::RegisterSignalResult`
/// but lives here because the engine can't depend on the
/// dispatcher.
#[derive(Debug, serde::Deserialize)]
struct RegisterSignalReply {
    token: String,
}

async fn enqueue_register_signal_task(
    tasks: &dyn TaskStoreClient,
    color: Color,
    node_id: &str,
    frames: &weft_core::frames::LoopFrames,
    spec: &SignalSpec,
    is_resume: bool,
    tenant_id: &str,
    call_index: u32,
    port_snapshot: Option<Value>,
) -> anyhow::Result<RegisterSignalReply> {
    // Task-level dedup so retries (network blip, supervisor
    // reconnect) converge on the same token. `is_resume` is in the
    // key because the same (color, node, frames, call_index) tuple can
    // be reused across a resume + an entry-trigger registration on
    // the same node body (e.g. a node that awaits its own webhook).
    // Separate from the journal-level dedup the dispatcher's
    // executor uses for SuspensionRegistered: that one omits
    // `is_resume` because only resume registrations journal a
    // SuspensionRegistered event.
    let frames_key = frames_dedup_key(frames)?;
    let dedup_key = format!(
        "{}/{}/{}/{}/{}",
        color, node_id, frames_key, is_resume, call_index,
    );
    let payload = serde_json::json!({
        "color": color.to_string(),
        "node_id": node_id,
        "frames": frames,
        "spec": spec,
        "is_resume": is_resume,
        "call_index": call_index,
        "port_snapshot": port_snapshot,
    });
    let id = tasks
        .enqueue_dedup(task_store::NewTask {
            kind: TaskKind::RegisterSignal.into(),
            target: task_store::TaskTarget::Dispatcher,
            project_id: None,
            dedup_key: Some(dedup_key),
            color: Some(color.to_string()),
            tenant_id: Some(tenant_id.to_string()),
            target_pod_name: None,
            binary_hash: None,
            payload,
        })
        .await?
        .id()
        // Only the broker-backed FireSignal path can fence (placement
        // generation); this register-signal enqueue never does, so it
        // always yields a task id.
        .expect("register-signal enqueue is never fenced");
    let outcome = tasks
        .wait_for_terminal(id, TASK_WAIT_TIMEOUT, TASK_POLL_INTERVAL)
        .await?;
    match outcome.status {
        task_store::TaskStatus::Complete => {
            let result = outcome
                .result
                .ok_or_else(|| anyhow::anyhow!("register_signal returned no result"))?;
            Ok(serde_json::from_value(result)?)
        }
        task_store::TaskStatus::Failed => {
            anyhow::bail!("{}", outcome.error.unwrap_or_else(|| "register_signal failed".into()))
        }
        other => anyhow::bail!("register_signal status: {other:?}"),
    }
}

/// Gap between connection attempts while waiting for an endpoint's
/// address to start answering.
const ENDPOINT_ROUTABLE_RETRY: std::time::Duration = std::time::Duration::from_millis(250);
/// How often a wait that is still going says so. Long enough not to
/// fill the log, short enough that a stuck endpoint is visible in the
/// node's own output rather than guessed at.
const ENDPOINT_WAITING_BREADCRUMB: std::time::Duration = std::time::Duration::from_secs(15);

/// Hold an endpoint's address back until something is actually
/// answering on it, then hand it over.
///
/// A workload is marked ready BEFORE the cluster writes the routing
/// rules that make its address answer, so anything that dials the
/// instant weft reports the infra running lands in that gap and gets
/// its connection refused. Waiting where a node RESOLVES an address
/// means the guarantee holds for whatever it speaks next: HTTP, a
/// database protocol, a raw socket. A node author never sees this,
/// which is the point.
///
/// Only the reachability of the address is waited on, by opening a TCP
/// connection and dropping it: nothing is sent, so this says nothing
/// about the service being READY, which is what its own readiness
/// probe is for. A UDP endpoint is handed over unchecked, having
/// nothing to connect to; an address that is not a URL with a host
/// and a port is refused.
///
/// NO deadline. An address is handed out as soon as the infra is
/// APPLIED, which is before the container has finished booting, so
/// what this waits on is the user's own workload starting: an image
/// pull, a database initialising its files. Putting a bound on that
/// would fail somebody's node for having a slow first boot. The wait
/// says where it has got to every so often, and ends the moment the
/// execution is cancelled, which is what `weft stop` and Ctrl+C do.
async fn wait_until_routable(
    url: &str,
    name: &str,
    clock: &dyn weft_platform_traits::clock::Clock,
    cancel: &CancellationFlag,
    say: &(dyn Fn(String) + Send + Sync),
) -> WeftResult<()> {
    let Some(address) = weft_core::context::endpoint_socket_address(url)? else {
        return Ok(());
    };
    wait_until_answering(&address, clock, cancel, name, say, |address| async move {
        // A blackholed address (SYNs dropped rather than refused)
        // would otherwise sit in one connect for the kernel's whole
        // retry schedule.
        match tokio::time::timeout(
            ENDPOINT_ROUTABLE_RETRY * 4,
            tokio::net::TcpStream::connect(address),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("the address accepted no connection in time".to_string()),
        }
    })
    .await
    .map_err(|last| {
        // The only way out of this wait is cancellation, and the
        // engine already has one word for that. Reporting it as a
        // runtime failure would make a stopped run look like a broken
        // one.
        tracing::info!(
            target: "weft_engine::endpoint",
            endpoint = %name,
            address = %address,
            last_error = %last,
            "gave up waiting for this node's own service: the execution was cancelled"
        );
        WeftError::Cancelled
    })
}

/// Try `dial` until it succeeds or the execution is cancelled,
/// resting on the clock in between and saying where it has got to
/// every so often.
///
/// The dialling is a parameter so the waiting can be tested for what
/// it promises (it returns the instant the address answers, it keeps
/// going for as long as the workload takes, it ends on cancellation)
/// without opening a socket, which would put the test back at the
/// mercy of whether the host refuses a dead port or silently drops
/// it.
async fn wait_until_answering<F, Fut>(
    address: &str,
    clock: &dyn weft_platform_traits::clock::Clock,
    cancel: &CancellationFlag,
    name: &str,
    say: &(dyn Fn(String) + Send + Sync),
    dial: F,
) -> Result<(), String>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let started = clock.now();
    let mut said = started;
    let mut last;
    loop {
        match dial(address.to_string()).await {
            Ok(()) => return Ok(()),
            Err(e) => last = e,
        }
        if cancel.is_cancelled() {
            return Err(last);
        }
        // Waiting silently for a workload that never comes up is the
        // shape that leaves someone staring at a stalled run with
        // nothing to go on. Said on the NODE's own output, which is
        // where they are already looking, not only in a pod log they
        // would have to know to go and find.
        if clock.now().duration_since(said) >= ENDPOINT_WAITING_BREADCRUMB {
            said = clock.now();
            let waited = clock.now().duration_since(started).as_secs();
            say(format!(
                "waiting for {name} at {address} to start answering ({waited}s so far; last \
                 attempt: {last}). It answers once the workload has finished starting. \
                 `weft stop` ends the run."
            ));
        }
        clock.sleep(ENDPOINT_ROUTABLE_RETRY).await;
    }
}

/// Process-wide `reqwest::Client` for the engine's outbound HTTP
/// (endpoint calls + storage put_from_url fetches). One per worker
/// process so the connection pool stays warm across calls inside a
/// loop body (the anti-pattern is `Client::new()` per request: every
/// call rebuilds the pool).
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

#[cfg(test)]
mod type_check_tests {
    use super::type_accepts;
    use weft_core::storage::StoredFile;
    use weft_core::weft_type::{FileKind, WeftType};

    fn image_value() -> serde_json::Value {
        StoredFile {
            key: "exec/c/img".into(),
            mime_type: "image/png".into(),
            size_bytes: 10,
            filename: "x.png".into(),
        }
        .to_value()
    }

    fn video_value() -> serde_json::Value {
        StoredFile {
            key: "exec/c/vid".into(),
            mime_type: "video/mp4".into(),
            size_bytes: 10,
            filename: "x.mp4".into(),
        }
        .to_value()
    }

    #[test]
    fn declared_file_accepts_any_stored_file() {
        let file = WeftType::file();
        assert!(type_accepts(&file, &image_value()));
        assert!(type_accepts(&file, &video_value()));
    }

    #[test]
    fn narrowed_image_accepts_image_rejects_video() {
        // The File port narrowed to Image: an image flows, a video does not.
        let image = WeftType::primitive(weft_core::weft_type::WeftPrimitive::Image);
        assert!(type_accepts(&image, &image_value()));
        assert!(!type_accepts(&image, &video_value()), "a video on an Image port is refused");
    }

    #[test]
    fn primitive_mismatch_is_rejected() {
        let number = WeftType::primitive(weft_core::weft_type::WeftPrimitive::Number);
        assert!(type_accepts(&number, &serde_json::json!(42)));
        assert!(!type_accepts(&number, &serde_json::json!("not a number")));
    }

    #[test]
    fn unresolved_declared_accepts_anything() {
        // A TypeVar / MustOverride port has no concrete contract yet, so the
        // gate must not reject (is_compatible short-circuits on unresolved).
        assert!(type_accepts(&WeftType::MustOverride, &video_value()));
        assert!(type_accepts(&WeftType::type_var("T"), &serde_json::json!("anything")));
    }

    // Touch FileKind so the import is used even if the helpers change.
    #[test]
    fn image_value_infers_as_image_marker() {
        assert_eq!(FileKind::from_mime("image/png"), FileKind::Image);
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use weft_core::context::ContextHandle;
    use weft_core::primitive::{AwaitedEntry, AwaitedEntryKind};

    fn handle_with_sequence(seq: Vec<AwaitedEntry>) -> RunnerHandle {
        // These tests only exercise run_step which never touches the
        // store. The no-op clients below let the trait objects exist
        // without any IO.
        let clients = EngineClients {
            journal: Arc::new(NoopJournal),
            tasks: Arc::new(NoopTaskStore),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            access_broker: FakeAccessBroker::new(),
            pending_costs: crate::metering::PendingCostRecords::new(),
        };
        RunnerHandle::new(
            "exec-1".into(),
            "00000000-0000-0000-0000-000000000000".into(),
            uuid::Uuid::nil(),
            "node-x".into(),
            "TestNode".into(),
            weft_core::frames::LoopFrames::default(),
            clients,
            None,
            "pod-1".into(),
            "tenant-1".into(),
            std::sync::Arc::new(CancellationFlag::new()),
            crate::wait_tracker::WaitTracker::new(),
            BusCoordinator::new(crate::wait_tracker::WaitTracker::new()),
            HashMap::new(),
            false,
        )
        .with_awaited_sequence(seq)
    }

    /// Same rig as `handle_with_sequence` but with a caller-supplied
    /// `PaidCallClient` fake, for the access/provision/settle tests.
    fn handle_with_access_broker(access_broker: Arc<FakeAccessBroker>) -> RunnerHandle {
        let clients = EngineClients {
            journal: Arc::new(NoopJournal),
            tasks: Arc::new(NoopTaskStore),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            access_broker,
            pending_costs: crate::metering::PendingCostRecords::new(),
        };
        RunnerHandle::new(
            "exec-1".into(),
            "00000000-0000-0000-0000-000000000000".into(),
            uuid::Uuid::nil(),
            "node-x".into(),
            "TestNode".into(),
            weft_core::frames::LoopFrames::default(),
            clients,
            None,
            "pod-1".into(),
            "tenant-1".into(),
            std::sync::Arc::new(CancellationFlag::new()),
            crate::wait_tracker::WaitTracker::new(),
            BusCoordinator::new(crate::wait_tracker::WaitTracker::new()),
            HashMap::new(),
            false,
        )
    }

    fn ctx_over(handle: RunnerHandle) -> weft_core::ExecutionContext {
        ctx_over_arc(Arc::new(handle))
    }

    /// `ctx_over` with the caller keeping a concrete `Arc<RunnerHandle>`
    /// (the same shape the loop driver uses to call
    /// `close_opened_accesses` after the body).
    fn ctx_over_arc(handle: Arc<RunnerHandle>) -> weft_core::ExecutionContext {
        weft_core::ExecutionContext::new(
            "exec-1".into(),
            "00000000-0000-0000-0000-000000000000".into(),
            "node-x".into(),
            "TestNode".into(),
            None,
            uuid::Uuid::nil(),
            weft_core::frames::LoopFrames::default(),
            weft_core::context::ValueBag::inputs(Default::default(), Default::default(), Vec::new()),
            handle,
        )
    }

    /// How long the paid call may run: the connection's window (a
    /// runtime-supplied credential's guaranteed-usable life).
    const CALL_WINDOW: std::time::Duration = std::time::Duration::from_secs(600);

    // Layer 3: the ctx.open surface over the fake access broker: one
    // resolve per open, the lease released by the RUNTIME (never node
    // code, and only for a runtime-owned credential), the window
    // threaded through, and the derived credential.
    #[tokio::test]
    async fn open_resolves_once_and_the_runtime_releases_the_lease() {
        let fake = FakeAccessBroker::new();
        fake.set_owned_bearer_connection("conn-1", "sk-1", weft_core::CredentialOwner::Ours);
        let handle = Arc::new(handle_with_access_broker(fake.clone()));
        let ctx = ctx_over_arc(handle.clone());

        let access = weft_core::Access::new("conn-1", "openrouter", None);
        let conn = ctx.open_within(&access, CALL_WINDOW).await.unwrap();
        assert_eq!(conn.credential().unwrap(), "sk-1", "one bearer step derives the string");
        assert_eq!(conn.owner(), weft_core::CredentialOwner::Ours);

        let resolved = fake.resolved.lock().unwrap().clone();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].connection_id, "conn-1");
        assert_eq!(resolved[0].service, "openrouter");
        assert_eq!(resolved[0].node_type, "TestNode");
        assert_eq!(
            resolved[0].expected_duration_secs,
            CALL_WINDOW.as_secs(),
            "the credential's life is the window the node declared, declared once"
        );

        // Nothing node-facing releases; the loop driver releases every
        // runtime-owned connection once the body finished.
        assert!(fake.released.lock().unwrap().is_empty());
        handle.close_opened_accesses().await;
        let released = fake.released.lock().unwrap().clone();
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].get("token").map(String::as_str), Some("sk-1"));
        // Idempotent: a second sweep has nothing left to release.
        handle.close_opened_accesses().await;
        assert_eq!(fake.released.lock().unwrap().len(), 1);
    }

    /// A their-own connection is never released back: the stored
    /// values are the user's own secrets, and there is nothing of the
    /// runtime's to retire, so no release call carries them.
    #[tokio::test]
    async fn a_their_own_connection_is_never_released_back() {
        let fake = FakeAccessBroker::new();
        fake.set_bearer_connection("conn-1", "sk-user");
        let handle = Arc::new(handle_with_access_broker(fake.clone()));
        let ctx = ctx_over_arc(handle.clone());

        let conn = ctx
            .open_within(&weft_core::Access::new("conn-1", "openrouter", None), CALL_WINDOW)
            .await
            .unwrap();
        assert_eq!(conn.owner(), weft_core::CredentialOwner::TheirOwn);
        handle.close_opened_accesses().await;
        assert!(
            fake.released.lock().unwrap().is_empty(),
            "the user's own secrets never travel back on a release"
        );
    }

    /// The window is optional: a node that says nothing gets the default.
    #[tokio::test]
    async fn the_default_window_is_used_when_a_node_declares_none() {
        let fake = FakeAccessBroker::new();
        fake.set_bearer_connection("conn-1", "sk-1");
        let ctx = ctx_over(handle_with_access_broker(fake.clone()));

        ctx.open(&weft_core::Access::new("conn-1", "openrouter", None)).await.unwrap();
        assert_eq!(
            fake.resolved.lock().unwrap()[0].expected_duration_secs,
            weft_core::context::DEFAULT_PROVIDER_WINDOW.as_secs()
        );
    }

    /// The marker's required permissions ride the resolve, so the
    /// store's drift backstop sees exactly what the consumer declared.
    #[tokio::test]
    async fn required_permissions_ride_the_resolve() {
        let fake = FakeAccessBroker::new();
        fake.set_bearer_connection("conn-1", "sk-1");
        let ctx = ctx_over(handle_with_access_broker(fake.clone()));

        let access = weft_core::Access::new("conn-1", "google", None)
            .with_required_permissions(vec!["drive.readonly".into()]);
        ctx.open(&access).await.unwrap();
        assert_eq!(
            fake.resolved.lock().unwrap()[0].required_permissions,
            vec!["drive.readonly".to_string()]
        );
    }

    /// A connection the store does not know is a loud error carrying
    /// the pick-one hint; ctx.client(None) answers a plain client with
    /// no resolve at all (the works-without-a-connection path).
    #[tokio::test]
    async fn an_unknown_connection_errors_loud_and_none_stays_plain() {
        let fake = FakeAccessBroker::new();
        let ctx = ctx_over(handle_with_access_broker(fake.clone()));

        let dead = weft_core::Access::new("id-gone", "slack", None);
        let err = ctx.open(&dead).await.unwrap_err().to_string();
        assert!(err.contains("pick one"), "{err}");

        ctx.client(None).await.expect("no connection = a plain client");
        assert_eq!(fake.resolved.lock().unwrap().len(), 1, "None never resolves");
    }

    /// ctx.client sugar: open + hand back the signed-in client, one
    /// resolve, one lease.
    #[tokio::test]
    async fn client_sugar_opens_and_leases() {
        let fake = FakeAccessBroker::new();
        // Ours exercises the lease path (only a runtime credential
        // releases), and a runtime credential only opens for a METERED
        // service, so the fixture is a metered one.
        fake.set_owned_bearer_connection("id-1", "sk-or-1", weft_core::CredentialOwner::Ours);
        let handle = Arc::new(handle_with_access_broker(fake.clone()));
        let ctx = ctx_over_arc(handle.clone());

        let access = weft_core::Access::new("id-1", "openrouter", Some("Q @ Acme".into()));
        ctx.client(&access).await.expect("a connected grant resolves to a client");
        let resolved = fake.resolved.lock().unwrap().clone();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].connection_id, "id-1");
        assert_eq!(resolved[0].service, "openrouter");
        handle.close_opened_accesses().await;
        assert_eq!(fake.released.lock().unwrap().len(), 1, "the sugar leases too");
    }

    /// The security invariant that must never be quietly undone: a LIVE
    /// node test resolves its provider credential through the EXACT same
    /// path a worker firing does. A node-test pod is not a worker (it
    /// never claims a task), but on the credential + metering seam it is
    /// deliberately made to look like one to the broker: same
    /// `RunnerHandle::open_connection`, so the same broker resolve
    /// request, so the same tenant/owner gate, proxy, and metering.
    ///
    /// This test opens the same connection two ways, once through a
    /// worker's `RunnerHandle` and once through the live-test rig's
    /// handle, over identical fake brokers, and asserts the resolve
    /// request the broker sees is worker-shaped and IDENTICAL in every
    /// security-carrying field (color, tenant is not on the wire but the
    /// color maps to it broker-side, project, service, connection id,
    /// declared permissions/values). If someone ever gives the test path
    /// a reduced handle, a raw key, or a broker bypass, the two requests
    /// diverge and this fails.
    // Gated with the rig itself: `test_rig` compiles only for the
    // emitted per-package test crate, so this cross-check rides the
    // same feature.
    #[cfg(feature = "node-tests")]
    #[tokio::test]
    async fn a_live_test_resolves_credentials_exactly_like_a_worker() {
        use weft_core::node::{Node, NodeManifest, NodeMetadata};
        use weft_core::ExecutionContext;

        // A node whose whole body is "open the declared connection":
        // the one act whose plumbing we are pinning.
        fn opens_manifest() -> &'static NodeMetadata {
            static M: std::sync::OnceLock<NodeMetadata> = std::sync::OnceLock::new();
            M.get_or_init(|| {
                serde_json::from_value(serde_json::json!({
                    "type": "OpensConnection",
                    "label": "Opens connection",
                    "description": "test-only node",
                    "inputs": [{"name": "account", "type": "Access", "required": false}],
                    "outputs": []
                }))
                .expect("manifest")
            })
        }
        struct OpensConnection;
        impl NodeManifest for OpensConnection {
            fn manifest(&self) -> &'static NodeMetadata {
                opens_manifest()
            }
        }
        // The access BOTH paths open, built the same way, so the
        // resolve-request assertions on scopes/values pin real
        // propagation rather than comparing two empty vecs. A regression
        // that dropped declared scopes on the test path would then fail
        // the test.
        fn scoped_access() -> weft_core::Access {
            weft_core::Access::new("id-1", "openrouter", None)
                .with_required_permissions(vec!["models.read".into()])
                .with_required_values(vec!["org_id".into()])
        }

        #[async_trait::async_trait]
        impl Node for OpensConnection {
            async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
                ctx.client(&scoped_access()).await?;
                Ok(())
            }
        }

        // Same fixture on both sides: a runtime-owned (Ours) metered
        // connection, the shape that exercises the proxy/lease path.
        let make_broker = || {
            let b = FakeAccessBroker::new();
            b.set_owned_bearer_connection("id-1", "sk-or-1", weft_core::CredentialOwner::Ours);
            b
        };

        // (1) The WORKER path: a plain RunnerHandle opens the connection.
        let worker_broker = make_broker();
        let worker_clients = EngineClients {
            journal: Arc::new(NoopJournal),
            tasks: Arc::new(NoopTaskStore),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            access_broker: worker_broker.clone(),
            pending_costs: crate::metering::PendingCostRecords::new(),
        };
        let color = uuid::Uuid::from_u128(0xC0);
        let worker_handle = Arc::new(RunnerHandle::new(
            color.to_string(),
            "project-1".into(),
            color,
            "node-x".into(),
            "OpensConnection".into(),
            weft_core::frames::LoopFrames::default(),
            worker_clients,
            None,
            "worker-pod-1".into(),
            "tenant-1".into(),
            std::sync::Arc::new(CancellationFlag::new()),
            crate::wait_tracker::WaitTracker::new(),
            BusCoordinator::new(crate::wait_tracker::WaitTracker::new()),
            HashMap::new(),
            false,
        ));
        ctx_over_arc(worker_handle).client(&scoped_access()).await.expect("worker opens");

        // (2) The LIVE TEST path: the rig builds its handle and runs the
        // node, going through the production `open_connection` under its
        // capture layer, over the same clients composition.
        let test_broker = make_broker();
        let test_clients = EngineClients {
            journal: Arc::new(NoopJournal),
            tasks: Arc::new(NoopTaskStore),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            access_broker: test_broker.clone(),
            pending_costs: crate::metering::PendingCostRecords::new(),
        };
        let runner = crate::test_rig::LiveTestRunner::new(
            test_clients,
            test_catalog(),
            "test-pod-1".into(),
            "tenant-1".into(),
            "project-1".into(),
            Some(color),
        );
        let rig = runner.rig("id-1", "openrouter");
        rig.run(&OpensConnection, serde_json::json!({})).await.ok().expect("rig opens");

        // The two resolve requests must match on every field that
        // decides which credential the broker hands back. A divergence
        // here means the test path stopped being a worker to the broker.
        let w = worker_broker.resolved.lock().unwrap().clone();
        let t = test_broker.resolved.lock().unwrap().clone();
        assert_eq!(w.len(), 1, "worker resolves once");
        assert_eq!(t.len(), 1, "test resolves once");
        let (w, t) = (&w[0], &t[0]);
        // The colour is the whole scope: the broker resolves the
        // tenant and the project from it, so a matching colour IS a
        // matching project.
        assert_eq!(w.color, t.color, "same execution color -> same broker tenant/owner gate");
        assert_eq!(w.service, t.service, "same service");
        assert_eq!(w.connection_id, t.connection_id, "same grant");
        assert_eq!(
            w.required_permissions, t.required_permissions,
            "same declared scopes on the wire"
        );
        assert_eq!(w.required_values, t.required_values, "same declared values on the wire");
    }

    // Layer 3: the ContextHandle storage methods over the fake
    // worker-storage (scope-built keys, stored-file value round trip,
    // wall enforcement, keep + sweep semantics).
    #[tokio::test]
    async fn storage_methods_round_trip_and_enforce_the_wall() {
        use weft_core::storage::{KeepTtl, StorageScope, StoredFile};
        let handle = handle_with_sequence(vec![]);

        let file = handle
            .storage_put(
                &StorageScope::Execution,
                weft_core::storage::bytes_stream(bytes::Bytes::from_static(b"payload")),
                "audio/ogg",
                "clip.ogg",
                None,
                Some(7),
            )
            .await
            .expect("put");
        let stored = StoredFile::from_value(&file).expect("self-describing value");
        // Keys are tenant-anchored now (`<tenant>/<scope>/...`); the fake
        // worker storage is seeded as tenant `t1`.
        assert!(stored.key.starts_with("t1/exec/c1/"), "{}", stored.key);
        assert_eq!(stored.size_bytes, 7);

        let (meta, stream) = handle.storage_get(&stored.key, None).await.expect("get");
        assert_eq!(meta.mime_type, "audio/ogg");
        let bytes = weft_core::storage::collect_stream(stream).await.unwrap();
        assert_eq!(&bytes[..], b"payload");

        // The wall: another color's exec key (under the same tenant) is denied.
        let err = match handle.storage_get("t1/exec/OTHER/f0", None).await {
            Err(e) => e,
            Ok(_) => panic!("cross-color get must be denied"),
        };
        assert!(err.to_string().contains("denied"), "{err}");

        // Keep marks the file to survive the broker's terminate sweep (the
        // sweep itself is broker-side, covered by the broker's db tests;
        // the worker has no sweep verb).
        handle.storage_keep(&stored.key, KeepTtl::Default).await.expect("keep");
        assert!(handle.storage_get(&stored.key, None).await.is_ok(), "kept file still readable");

        // Presign mints a (bucket) URL for an owned file.
        let url = handle.storage_presign(&stored.key, Some(60)).await.expect("presign");
        assert!(url.starts_with("http") && url.contains(&stored.key), "{url}");
    }

    /// A url-backed file value routes by its handle in every storage
    /// verb: presign hands the URL back as-is (it already IS a URL an
    /// external service can fetch); delete/keep are bucket-only and
    /// error loud naming the URL.
    #[tokio::test]
    async fn url_backed_values_route_by_handle_in_storage_verbs() {
        use weft_core::storage::{KeepTtl, StorageScope};
        let ctx = ctx_over(handle_with_sequence(vec![]));
        let ty = weft_core::WeftType::parse("Image").unwrap();
        let url_value = weft_core::storage::url_file_value("https://x/pic.png", &ty);
        let url_file = weft_core::storage::FileHandle::from_value(&url_value).unwrap();
        let storage = ctx.storage(StorageScope::Execution);

        assert_eq!(storage.presign(&url_file, None).await.unwrap(), "https://x/pic.png");

        let err = storage.keep(&url_file, KeepTtl::Default).await.unwrap_err();
        assert!(err.to_string().contains("external URL"), "{err}");
        let err = storage.delete(&url_file).await.unwrap_err();
        assert!(err.to_string().contains("external URL"), "{err}");
    }

    use weft_journal::NoopJournal;

    struct NoopTaskStore;
    #[async_trait]
    impl TaskStoreClient for NoopTaskStore {
        async fn enqueue_dedup(
            &self,
            _spec: weft_task_store::tasks::NewTask,
        ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            unreachable!("replay tests do not enqueue")
        }
        async fn wait_for_terminal(
            &self,
            _task_id: uuid::Uuid,
            _timeout: std::time::Duration,
            _poll_interval: std::time::Duration,
        ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!("replay tests do not wait")
        }
        async fn claim_one(
            &self,
            _pod_id: &str,
            _filter: weft_task_store::tasks::ClaimFilter,
        ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
            Ok(None)
        }
        async fn heartbeat(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
        ) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn requeue(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
        ) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _result: Value,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(
            &self,
            _task_id: uuid::Uuid,
            _pod_id: &str,
            _error: String,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }
    struct NoopInfra;
    #[async_trait]
    impl InfraReader for NoopInfra {
        async fn endpoint_url(
            &self,
            _project_id: &str,
            _node_id: &str,
            _endpoint_name: &str,
        ) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
    }

    struct NoopInfraState;
    #[async_trait]
    impl InfraStateClient for NoopInfraState {
        async fn enqueue_apply(
            &self,
            _project_id: &str,
            _node_id: &str,
            _spec_json: serde_json::Value,
        ) -> anyhow::Result<i64> {
            Ok(0)
        }
        async fn wait_apply(
            &self,
            _project_id: &str,
            _command_id: i64,
        ) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
            Ok(weft_broker_client::protocol::InfraWaitApplyResponse {
                completed: true,
                outcome: Some(weft_broker_client::protocol::LifecycleOutcome::Succeeded),
                outcome_message: None,
            })
        }
    }

    /// A catalog with no nodes: these tests never publish, and a node
    /// it cannot find declares nothing, which is the honest answer for
    /// an empty catalog.
    ///
    /// Gated like its one caller, which builds a handle only when the
    /// node-test surface is compiled in.
    #[cfg(feature = "node-tests")]
    fn test_catalog() -> &'static dyn weft_core::NodeCatalog {
        struct Empty;
        impl weft_core::NodeCatalog for Empty {
            fn lookup(&self, _node_type: &str) -> Option<&'static dyn weft_core::node::Node> {
                None
            }
            fn all(&self) -> Vec<&'static str> {
                Vec::new()
            }
        }
        &Empty
    }

    struct NoopProject;
    #[async_trait]
    impl crate::context::ProjectClient for NoopProject {
        async fn fetch_definition(
            &self,
            _project_id: &str,
            _expected_hash: &str,
        ) -> anyhow::Result<Option<weft_core::ProjectDefinition>> {
            // Replay tests don't take this path (they exercise
            // RunnerHandle directly, never spawning a worker pod);
            // if a test somehow calls this, fail loud.
            anyhow::bail!("NoopProject::fetch_definition not implemented")
        }
    }

    /// Replay path: a Run entry at the next call_index returns
    /// its journaled value without invoking the closure.
    #[tokio::test]
    async fn run_step_replay_returns_journaled_value() {
        let seq = vec![AwaitedEntry {
            call_index: 0,
            kind: AwaitedEntryKind::Run {
                name: "decide".into(),
                value: serde_json::json!("go-left"),
            },
        }];
        let handle = handle_with_sequence(seq);
        let (idx, got) = handle
            .run_step("decide")
            .await
            .expect("run_step ok");
        assert_eq!(idx, 0);
        assert_eq!(got, Some(serde_json::json!("go-left")));
    }

    /// Fresh path: sequence is empty, run_step returns None so
    /// the wrapper invokes the closure.
    #[tokio::test]
    async fn run_step_fresh_returns_none() {
        let handle = handle_with_sequence(Vec::new());
        let (idx, got) = handle.run_step("decide").await.expect("run_step ok");
        assert_eq!(idx, 0);
        assert!(got.is_none(), "no journaled output yet");
    }

    /// Mismatched kind at the same call_index: body called
    /// `ctx.run` but the journal has an `Await` at this index.
    /// Surfaces as Suspension error so the body fails loudly
    /// instead of silently desyncing.
    #[tokio::test]
    async fn run_step_mismatch_kind_errors() {
        let seq = vec![AwaitedEntry {
            call_index: 0,
            kind: AwaitedEntryKind::Await {
                token: "tok-0".into(),
                resolved: Some(serde_json::json!("from-fire")),
            },
        }];
        let handle = handle_with_sequence(seq);
        let err = handle
            .run_step("decide")
            .await
            .expect_err("should fail on kind mismatch");
        let msg = format!("{err}");
        assert!(
            matches!(err, WeftError::NodeExecution(_)),
            "expected NodeExecution variant, got: {err:?}"
        );
        assert!(
            msg.contains("non-deterministic bodies are not safe to replay"),
            "unexpected: {msg}"
        );
    }

    /// Multi-call replay: two Run entries in sequence pop in order.
    #[tokio::test]
    async fn run_step_replay_sequential() {
        let seq = vec![
            AwaitedEntry {
                call_index: 0,
                kind: AwaitedEntryKind::Run {
                    name: "first".into(),
                    value: serde_json::json!("v0"),
                },
            },
            AwaitedEntry {
                call_index: 1,
                kind: AwaitedEntryKind::Run {
                    name: "second".into(),
                    value: serde_json::json!("v1"),
                },
            },
        ];
        let handle = handle_with_sequence(seq);
        assert_eq!(
            handle.run_step("first").await.expect("first"),
            (0, Some(serde_json::json!("v0")))
        );
        assert_eq!(
            handle.run_step("second").await.expect("second"),
            (1, Some(serde_json::json!("v1")))
        );
        // Third call: sequence exhausted, returns None.
        let (idx, val) = handle.run_step("third").await.expect("third");
        assert_eq!(idx, 2);
        assert!(val.is_none());
    }

    /// Mixed sequence: Await then Run. Verifies the counter and
    /// pop work across kinds.
    #[tokio::test]
    async fn await_then_run_replay_in_order() {
        let seq = vec![
            AwaitedEntry {
                call_index: 0,
                kind: AwaitedEntryKind::Await {
                    token: "tok-0".into(),
                    resolved: Some(serde_json::json!("answer")),
                },
            },
            AwaitedEntry {
                call_index: 1,
                kind: AwaitedEntryKind::Run {
                    name: "process".into(),
                    value: serde_json::json!({"shape": "pre-baked"}),
                },
            },
        ];
        let handle = handle_with_sequence(seq);
        use weft_core::signal::{to_spec, Form, FormSchema};
        let spec = to_spec(Form {
            form_type: "human-query".into(),
            schema: FormSchema { fields: Vec::new() },
            title: None,
            description: None,
            consumer_kind: None,
        });
        let answer = handle
            .await_signal(spec)
            .await
            .expect("await replay ok");
        assert_eq!(answer, serde_json::json!("answer"));
        let processed = handle
            .run_step("process")
            .await
            .expect("run replay ok");
        assert_eq!(
            processed,
            (1, Some(serde_json::json!({"shape": "pre-baked"})))
        );
    }

    // ----- Live caller surface on the ContextHandle ------------------

    use weft_core::caller::{CallerRuntimeConfig, FakeCallerConnection};
    use weft_core::signal::{Backpressure, DataType, ErrorMode, Protocol};
    use weft_core::wait::SuspendPolicy;

    fn caller_cfg(protocol: Protocol, can_suspend: bool) -> CallerRuntimeConfig {
        CallerRuntimeConfig {
            protocol,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: ErrorMode::Surface,
            connect_timeout_secs: 5,
            max_inbound_bytes: 1024,
            max_session_secs: 0,
            suspend: SuspendPolicy { can_suspend, default_hold_secs: 300 },
            inbound_window: weft_core::caller::DEFAULT_INBOUND_WINDOW,
        }
    }

    #[test]
    fn queries_report_protocol_and_none_without_caller() {
        // No caller wired: both queries false, caller() None.
        let bare = handle_with_sequence(vec![]);
        assert!(bare.caller_connection().is_none());

        // HTTP caller wired: protocol is Http.
        let http = handle_with_sequence(vec![]).with_caller_connection(Some(
            FakeCallerConnection::connected(caller_cfg(Protocol::Http, false)),
        ));
        assert!(http.caller_connection().is_some());
        let proto = http.caller_connection().unwrap().config().protocol;
        assert_eq!(proto, Protocol::Http);

        let ws = handle_with_sequence(vec![]).with_caller_connection(Some(
            FakeCallerConnection::connected(caller_cfg(Protocol::Websocket, true)),
        ));
        assert_eq!(ws.caller_connection().unwrap().config().protocol, Protocol::Websocket);
    }

    #[tokio::test]
    async fn await_signal_does_not_fail_at_the_call_in_a_tied_run() {
        // A durable wait in a caller-tied run does NOT fail at the await
        // call: other branches may still be running and talking to the
        // caller, so the reconciliation (hold-then-kill) is deferred to the
        // true suspension point in the loop driver. At the call, the await
        // suspends as normal (returns Suspended), it does NOT raise a
        // policy error. Use the pending-tail replay path (a pre-loaded
        // unresolved await) so the suspend is reached without the broker.
        let seq = vec![AwaitedEntry {
            call_index: 0,
            kind: AwaitedEntryKind::Await { token: "tok-pending".into(), resolved: None },
        }];
        let handle = handle_with_sequence(seq).with_caller_connection(Some(
            FakeCallerConnection::connected(caller_cfg(Protocol::Websocket, false)),
        ));
        let spec = weft_core::signal::to_spec(weft_core::signal::Timer {
            spec: weft_core::signal::TimerSpec::After { duration_ms: 1000 },
        });
        let err = handle.await_signal(spec).await.expect_err("a pending await suspends");
        assert!(
            matches!(err, WeftError::Suspended { ref token } if token == "tok-pending"),
            "tied-run await must suspend at the call, not fail; got: {err:?}"
        );
    }
}

/// After `node.provision_infra()` returns, the loop driver calls this to
/// ship the spec to the supervisor and wait for it to settle.
///
/// The worker doesn't compile, doesn't hash, doesn't decide
/// skip/fresh/replace. The supervisor owns all of those: it reads
/// the prior `infra_node` row, compiles the new spec with the real
/// image-tag map + instance id (fresh-mint or reused), hashes,
/// makes the decision, and executes. The worker just polls the
/// command row for terminal state.
///
/// This is a single round-trip from the engine's perspective:
/// "supervisor, please apply this spec; tell me when you're done."
/// Skip detection happens supervisor-side and is invisible to the
/// caller (success is success either way).
pub async fn apply_via_supervisor(
    infra_state: &dyn InfraStateClient,
    clock: &dyn weft_platform_traits::Clock,
    project_id: &str,
    node_id: &str,
    spec: &weft_core::infra::InfraSpec,
) -> anyhow::Result<()> {
    let spec_json = serde_json::to_value(spec)?;
    let cmd_id = infra_state
        .enqueue_apply(project_id, node_id, spec_json)
        .await?;
    let deadline = clock.now() + TASK_WAIT_TIMEOUT;
    loop {
        let resp = infra_state.wait_apply(project_id, cmd_id).await?;
        if resp.completed {
            use weft_broker_client::protocol::LifecycleOutcome;
            match resp.outcome {
                Some(LifecycleOutcome::Succeeded) => return Ok(()),
                Some(LifecycleOutcome::Cancelled) => {
                    // The command was abandoned (e.g. the node was
                    // removed by `remove_node` mid-flight). Not a
                    // failure: surface as "no longer applicable"
                    // and let the engine treat it as completed.
                    let reason = resp.outcome_message.as_deref().unwrap_or("cancelled");
                    tracing::info!(
                        target: "weft_engine::context",
                        project_id,
                        node_id,
                        reason,
                        "supervisor apply cancelled; no longer applicable"
                    );
                    return Ok(());
                }
                Some(LifecycleOutcome::Failed) => {
                    let err = resp
                        .outcome_message
                        .unwrap_or_else(|| "supervisor reported no error detail".into());
                    anyhow::bail!("supervisor apply failed: {err}");
                }
                None => {
                    // completed=true with outcome=None means schema
                    // drift the broker should have caught; fail loud.
                    anyhow::bail!(
                        "supervisor apply completed but returned no outcome"
                    );
                }
            }
        }
        if clock.now() >= deadline {
            anyhow::bail!(
                "supervisor did not complete apply command {cmd_id} within {}s",
                TASK_WAIT_TIMEOUT.as_secs()
            );
        }
        clock.sleep(TASK_POLL_INTERVAL).await;
    }
}

// =====================================================================
//                       Layer-3 tests: bus journal pump
// =====================================================================
//
// These exercise the per-execution `BusCoordinator` + `run_bus_journal_task`
// against a faked `JournalClient`. They prove the four original failure
// modes the redesign was for cannot recur:
//   1. `live_buses` does not leak (Weak refs drop after close).
//   2. The journal pump never silently drops; failure surfaces on the
//      next `send` as `SendError::JournalDegraded`.
//   3. There is no Lagged-style silent swallow of retained data: an
//      ephemeral consumer's cursor resumes at the oldest retained
//      entry; journaled consumers just lag.
//   4. Register-then-close cannot orphan a `Joined`: the log lock
//      serializes both with the `closed` flag.

#[cfg(test)]
mod bus_pump_tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;
    use weft_core::bus::{BusOptions, SendError};
    use weft_journal::ExecEvent;

    /// Capturing journal client. Stores every `record_event` payload
    /// so tests can assert on the bus events the pump shipped.
    /// Optionally throws on every Nth call to exercise the degraded
    /// path; `fail_next` set to `Some(N)` fails the Nth following
    /// call exactly once, then resets.
    #[derive(Default)]
    struct CaptureJournal {
        events: StdMutex<Vec<ExecEvent>>,
        fail_count: StdMutex<usize>,
    }
    #[async_trait]
    impl weft_journal::JournalClient for CaptureJournal {
        async fn record_event(&self, event: &ExecEvent, _pod: Option<&str>) -> anyhow::Result<()> {
            {
                let mut fc = self.fail_count.lock().unwrap();
                if *fc > 0 {
                    *fc -= 1;
                    return Err(anyhow::anyhow!("simulated journal failure"));
                }
            }
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn events_for_color(&self, _color: Color) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(Vec::new())
        }
        async fn raw_events_for_color(&self, _color: Color) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn has_terminal_event(&self, _color: Color) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    fn spawn_pump(
        coordinator: &Arc<BusCoordinator>,
        journal: Arc<CaptureJournal>,
        color: Color,
    ) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(coordinator);
        let journal_dyn: Arc<dyn weft_journal::JournalClient> = journal;
        tokio::spawn(run_bus_journal_task(weak, color, journal_dyn, "pod".into()))
    }

    /// Test helper: thin wrapper around `coord.new_bus` so tests have
    /// the same call signature as production. Unwraps the validation
    /// result; tests pass valid options. These tests exercise the
    /// journal pump / drain, not the stuck-detector, so a single
    /// synthetic node identity at root frames is sufficient.
    fn new_bus(coord: &Arc<BusCoordinator>, opts: BusOptions) -> BusHandle {
        coord
            .new_bus(opts, FiringLocation::new("test-node", Vec::new()))
            .expect("test BusOptions cannot fail")
    }

    /// Test helper: mint a bus attributed to a specific node execution
    /// at root frames, so liveness tests can drive distinct participants.
    fn new_bus_for(coord: &Arc<BusCoordinator>, node_id: &str) -> BusHandle {
        coord
            .new_bus(BusOptions::default(), FiringLocation::new(node_id, Vec::new()))
            .expect("test BusOptions cannot fail")
    }

    fn firing(node_id: &str) -> FiringLocation {
        FiringLocation::new(node_id, Vec::new())
    }

    /// A bus handle's inner as the generic wait source the tracker
    /// hooks take (the registry pins the inner Arc, so the borrowed
    /// handle's later drop is harmless).
    fn wait_src(b: &BusHandle) -> Arc<dyn weft_core::liveness::WaitSource> {
        b.inner_arc()
    }

    /// Test helper: run the production shutdown sequence, then drop
    /// the coordinator and join the pump task. Bounded by 2s so a
    /// regression fails fast instead of hanging the suite.
    async fn shutdown_and_join(coord: Arc<BusCoordinator>, pump: tokio::task::JoinHandle<()>) {
        coord.shutdown(std::time::Duration::from_secs(2)).await;
        drop(coord);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), pump).await;
    }

    /// Wait until `predicate` returns `true`, polling at 5ms intervals
    /// with a 2s bound so a regression fails fast instead of hanging.
    async fn wait_until<F: FnMut() -> bool>(mut predicate: F) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !predicate() {
            if std::time::Instant::now() > deadline {
                panic!("wait_until timed out");
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Wait until `predicate` is true, re-checking after every pump
    /// drain pass: the coordinator's `drain_complete_notify` fires
    /// once per pass, which is the real "the pump wrote something"
    /// signal (no sleep-polling). The notified future is armed BEFORE
    /// each check so a pass landing between check and park cannot be a
    /// lost wake-up. Bounded by 2s so a regression fails fast.
    async fn wait_for_drained<F: FnMut() -> bool>(coord: &Arc<BusCoordinator>, mut predicate: F) {
        let drained = coord.drain_complete_notify();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let notified = drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if predicate() {
                return;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep_until(deadline) => {
                    panic!("timed out waiting for the pump to drain the expected event");
                }
            }
        }
    }

    /// The pump journals BusJoined / BusWindow / BusClosed in offset
    /// order (membership rows flush the open window first, so the row
    /// stream never reorders against the bus log). The bus's mode is
    /// carried in the marker JSON itself.
    #[tokio::test]
    async fn pump_journals_full_lifecycle_in_offset_order() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new(crate::wait_tracker::WaitTracker::new());
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);

        let mut bus = new_bus(&coord, BusOptions::default());
        bus.register("alice").unwrap();
        bus.send("hi", serde_json::json!("world")).unwrap();
        bus.close();
        // Drop the producer handle so the only Arc<BusInner> is the
        // one the coordinator pins. This matches the production
        // shutdown shape: every node task is gone before close_all.
        drop(bus);

        // 3 events: Joined + Message + Closed.
        let j = journal.clone();
        wait_until(|| j.events.lock().unwrap().len() >= 3).await;

        shutdown_and_join(coord, pump).await;

        let events = journal.events.lock().unwrap().clone();
        let mut joined = 0;
        let mut messages = 0;
        let mut closed = 0;
        let mut last_offset: i64 = -1;
        for ev in &events {
            match ev {
                ExecEvent::BusJoined { offset, .. } => {
                    joined += 1;
                    assert!(*offset as i64 > last_offset);
                    last_offset = *offset as i64;
                }
                ExecEvent::BusWindow { first_offset, last_offset: lo, .. } => {
                    messages += 1;
                    assert!(*first_offset as i64 > last_offset);
                    last_offset = *lo as i64;
                }
                ExecEvent::BusClosed { offset, .. } => {
                    closed += 1;
                    assert!(*offset as i64 > last_offset);
                    last_offset = *offset as i64;
                }
                _ => {}
            }
        }
        assert!(joined >= 1, "at least one Joined journaled");
        assert!(messages >= 1, "at least one window journaled");
        assert_eq!(closed, 1, "Closed emitted at shutdown");
    }

    // A fast stream's window flushes when its TIME is up, without any
    // close or membership entry forcing it: two sends inside one short
    // window land as ONE BusWindow row carrying both messages, written
    // while the bus is still open. Stress-looped: the pump's deadline
    // select, the drain-notify handshake, and the multi-thread
    // scheduler race here by construction.
    weft_core::stress_test!(
        name: a_window_flushes_on_its_own_deadline,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let color = uuid::Uuid::new_v4();
            let coord = BusCoordinator::new(crate::wait_tracker::WaitTracker::new());
            let journal = Arc::new(CaptureJournal::default());
            let pump = spawn_pump(&coord, journal.clone(), color);

            let mut bus = new_bus(
                &coord,
                BusOptions {
                    journal_window: Some(std::time::Duration::from_millis(50)),
                    ..Default::default()
                },
            );
            bus.register("mic").unwrap();
            bus.send("frame", serde_json::json!(1)).unwrap();
            bus.send("frame", serde_json::json!(2)).unwrap();

            // No close: only the window deadline can flush a BusWindow row.
            let j = journal.clone();
            wait_for_drained(&coord, || {
                j.events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|e| matches!(e, ExecEvent::BusWindow { .. }))
            })
            .await;
            let events = journal.events.lock().unwrap().clone();
            let (messages, totals) = events
                .iter()
                .find_map(|e| match e {
                    ExecEvent::BusWindow { messages, totals, .. } => {
                        Some((messages.clone(), totals.clone()))
                    }
                    _ => None,
                })
                .expect("a BusWindow row was journaled on the deadline");
            assert_eq!(messages.len(), 2, "both sends in one row");
            assert_eq!(
                (totals[0].from.as_str(), totals[0].msg_kind.as_str(), totals[0].count),
                ("mic", "frame", 2),
                "the rollup names the sender/kind and counts both sends"
            );
            drop(events);

            bus.close();
            drop(bus);
            shutdown_and_join(coord, pump).await;
        }
    );

    /// On a journal write failure, the affected bus is marked
    /// `journal_degraded`; the next `send` returns
    /// `SendError::JournalDegraded`. The pump's next SUCCESSFUL batch
    /// clears the flag on its own, after which sends return `Ok`.
    #[tokio::test]
    async fn pump_failure_surfaces_journal_degraded_on_next_send() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new(crate::wait_tracker::WaitTracker::new());
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);

        let mut bus = new_bus(&coord, BusOptions::default());
        bus.register("alice").unwrap();
        // Inject a failure on the next journal write the pump tries
        // (which will be the Joined event from the register above).
        *journal.fail_count.lock().unwrap() = 1;
        // Send something so the pump wakes and processes the tail.
        // The send itself succeeds (it's an in-RAM append); the pump
        // then fails to journal the entries and marks the bus.
        let _ = bus.send("warm", serde_json::json!("up"));
        let live_buses = coord.live_bus_inners();
        let bus_inner = live_buses[0].upgrade().unwrap();
        wait_until(|| bus_inner.is_journal_degraded()).await;
        // The next send sees the flag and errors loud.
        let err = bus.send("late", serde_json::json!("x"));
        assert!(matches!(err, Err(SendError::JournalDegraded(_))), "got {err:?}");

        // Recovery is the pump's own retry: a fresh append wakes it
        // (a rejected send appends nothing, so a second participant's
        // register provides the wake), the buffered tail now writes
        // successfully, the ack clears the flag, and sends resume.
        let mut bob = bus.new_handle();
        bob.register("bob").unwrap();
        wait_until(|| !bus_inner.is_journal_degraded()).await;
        assert!(bus.send("ok", serde_json::json!("y")).is_ok());

        // Shutdown cleanly.
        bus.close();
        drop(bob);
        drop(bus);
        shutdown_and_join(coord, pump).await;
    }

    // Ephemeral bus: the journaled window carries NO message payloads
    // (an empty `messages` list) and a totals rollup with a real byte
    // count. Payload bytes never leave the producer's RAM.
    // Stress-looped: the close-forces-flush ordering and the
    // drain-notify handshake race under the multi-thread scheduler.
    weft_core::stress_test!(
        name: ephemeral_bus_journal_carries_only_metadata_stub,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let color = uuid::Uuid::new_v4();
            let coord = BusCoordinator::new(crate::wait_tracker::WaitTracker::new());
            let journal = Arc::new(CaptureJournal::default());
            let pump = spawn_pump(&coord, journal.clone(), color);

            let mut bus = new_bus(
                &coord,
                BusOptions {
                    ephemeral: true,
                    window: Some(4),
                    ..Default::default()
                },
            );
            bus.register("camera").unwrap();
            bus.send("frame", serde_json::json!({"px": "AAAA"})).unwrap();
            bus.close();
            drop(bus);

            // The Closed membership row is the LAST thing the pump ships
            // for this bus (any open window flushes first, in offset
            // order), so its presence proves the window row landed too.
            let j = journal.clone();
            wait_for_drained(&coord, || {
                j.events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|e| matches!(e, ExecEvent::BusClosed { .. }))
            })
            .await;

            shutdown_and_join(coord, pump).await;

            let events = journal.events.lock().unwrap().clone();
            let (messages, totals) = events
                .iter()
                .find_map(|e| match e {
                    ExecEvent::BusWindow { messages, totals, .. } => {
                        Some((messages.clone(), totals.clone()))
                    }
                    _ => None,
                })
                .expect("BusWindow journaled");
            assert!(messages.is_empty(), "ephemeral windows carry no payloads");
            assert_eq!(totals.len(), 1);
            assert_eq!((totals[0].from.as_str(), totals[0].msg_kind.as_str()), ("camera", "frame"));
            assert_eq!(totals[0].count, 1);
            assert!(totals[0].bytes > 0, "byte rollup must be populated");
        }
    );

    /// Once every participant handle drops AND `coord.shutdown()`
    /// releases the coordinator's `Arc<BusInner>` refs, the bus's
    /// `Weak<BusInner>` refs in the registry fail to upgrade. This is
    /// the no-leak property: a fully-closed bus is freed.
    #[tokio::test]
    async fn weak_only_registry_collects_bus_when_handles_drop() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new(crate::wait_tracker::WaitTracker::new());
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);

        let weak_after_drop = {
            let bus = new_bus(&coord, BusOptions::default());
            let weak = std::sync::Arc::downgrade(&bus.inner_arc());
            bus.close();
            drop(bus);
            weak
        };
        // Snapshot the weak ref BEFORE shutdown so we can verify
        // post-shutdown collection. shutdown() drains then releases
        // the coordinator's Arc; no other Arc remains.
        coord.shutdown(std::time::Duration::from_secs(2)).await;
        drop(coord);
        assert!(
            weak_after_drop.upgrade().is_none(),
            "weak ref should fail to upgrade after shutdown + handle drop"
        );

        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), pump).await;
    }

    /// Backstop: if the coordinator is dropped WITHOUT calling
    /// `shutdown()` (e.g. a panic unwind in the loop driver), the
    /// pump must still exit cleanly. The mechanism: `Drop for
    /// BusCoordinator` sets `pump_should_exit` AND fires
    /// `journal_pump_notify`. The pump's parked `notified.await`
    /// returns; on the next iteration `coordinator.upgrade()` is
    /// None and the None-branch sets `should_exit = true`; the pump
    /// returns. Without the Drop firing the notify, the pump would
    /// stay parked forever even though the upgrade would correctly
    /// return None.
    #[tokio::test]
    async fn pump_exits_when_coordinator_dropped_without_shutdown() {
        let color = uuid::Uuid::new_v4();
        let coord = BusCoordinator::new(crate::wait_tracker::WaitTracker::new());
        let journal = Arc::new(CaptureJournal::default());
        let pump = spawn_pump(&coord, journal.clone(), color);
        // Briefly let the pump start its first iteration so it has
        // observed the live coordinator at least once.
        tokio::task::yield_now().await;
        // Drop the coordinator directly. No shutdown call. The
        // `Drop for BusCoordinator` impl fires the pump notify and
        // sets the exit flag; the pump's notified.await wakes, the
        // next iteration's upgrade returns None, the pump exits.
        drop(coord);
        // Bound the wait so a regression (orphaned pump) fails fast
        // instead of hanging the suite.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), pump).await;
        assert!(
            outcome.is_ok(),
            "pump must exit within 2s when coordinator is dropped without shutdown"
        );
    }

    // ───────────────────────────────────────────────────────────────
    // Bus-integrated liveness checks: the tracker's own decision logic
    // is unit-tested in `crate::wait_tracker` against a fake source;
    // the tests here pin the BUS side of the contract (the append
    // generation the bus reports, the registry interplay), driving the
    // same hooks the wait loops fire via WaitGuard.
    // ───────────────────────────────────────────────────────────────

    /// The execution-owned tracker plus a coordinator wired to it,
    /// mirroring the driver's construction order.
    fn coord_with_tracker() -> (Arc<crate::wait_tracker::WaitTracker>, Arc<BusCoordinator>) {
        let waits = crate::wait_tracker::WaitTracker::new();
        (waits.clone(), BusCoordinator::new(waits))
    }

    fn in_flight(locs: &[&FiringLocation]) -> std::collections::HashSet<FiringLocation> {
        locs.iter().map(|l| (*l).clone()).collect()
    }

    /// A node woken by a send but still unpolled reads as BEHIND its
    /// bus's generation, so it is excluded from the parked-caught-up set
    /// and the close is suppressed. Model it: the node parked at
    /// generation 0, then an append bumped the bus to generation 1
    /// WITHOUT the node re-observing. `deadlock_provable` reads the
    /// bus's settled generation (1) > observed (0) -> not caught up.
    #[test]
    fn parked_node_behind_generation_is_not_deadlock() {
        let (waits, coord) = coord_with_tracker();
        // The sender is a DIFFERENT node 'b' (not the waiter), so the
        // test models a real two-party exchange, not a node sending to
        // itself.
        let mut producer = new_bus_for(&coord, "b");
        let bus = wait_src(&producer);
        let a = firing("a");
        let w = waits.enter_wait(&a, &bus);
        waits.observed(&a, w); // generation 0
        waits.parked(&a, w);
        // A message lands (generation bumps) but 'a' has not re-observed.
        producer.register("producer").unwrap();
        producer.send("m", serde_json::json!(1)).unwrap();
        assert!(
            !waits.deadlock_provable(&in_flight(&[&a])),
            "parked node behind the bus generation has unconsumed input: alive"
        );
        // After re-observing the new generation and re-parking, it is a
        // deadlock again (nothing further will arrive).
        waits.observed(&a, w);
        waits.parked(&a, w);
        assert!(
            waits.deadlock_provable(&in_flight(&[&a])),
            "re-observed and re-parked at the current generation: deadlock"
        );
    }

    /// A node parked on TWO buses is deadlocked only if NEITHER bus has an
    /// unconsumed append: `deadlock_provable` checks EVERY wait's bus
    /// generation, not just one. Pins the multi-wait phase-2 scan: park
    /// caught-up on bus X, then append to bus Y; the node is alive
    /// because its Y-wait is behind.
    #[test]
    fn node_parked_on_two_buses_alive_if_either_has_unconsumed() {
        let (waits, coord) = coord_with_tracker();
        let bus_x = wait_src(&new_bus_for(&coord, "a"));
        let mut producer_y = new_bus_for(&coord, "b");
        let bus_y = wait_src(&producer_y);
        let a = firing("a");
        let wx = waits.enter_wait(&a, &bus_x);
        let wy = waits.enter_wait(&a, &bus_y);
        // Both waits parked, both caught up at generation 0.
        waits.observed(&a, wx);
        waits.parked(&a, wx);
        waits.observed(&a, wy);
        waits.parked(&a, wy);
        assert!(waits.deadlock_provable(&in_flight(&[&a])), "both waits caught up: deadlock");
        // An append on bus Y (not yet observed by the Y-wait) revives the
        // node even though its X-wait is still caught up.
        producer_y.register("producer").unwrap();
        producer_y.send("m", serde_json::json!(1)).unwrap();
        assert!(
            !waits.deadlock_provable(&in_flight(&[&a])),
            "the Y-wait is behind its bus: the node has unconsumed input, alive"
        );
    }

    /// Dropping a REGISTERED handle after the bus is closed must not
    /// panic or leak. The Drop path takes the registration and bails on
    /// the closed bus; with the inert participant ref-count removed,
    /// there is no `leave` to mis-pair. The node's liveness entry (if
    /// any) is governed solely by its waits. Pins that a registered
    /// handle outliving close is harmless.
    #[test]
    fn registered_handle_drop_after_close_is_clean() {
        let (waits, coord) = coord_with_tracker();
        let mut bus = new_bus_for(&coord, "a");
        bus.register("a").unwrap();
        coord.close_all();
        // No liveness entry was ever created (the node never entered a
        // wait), and dropping the registered handle on a closed bus must
        // not panic.
        assert_eq!(waits.nodes_len(), 0);
        drop(bus);
        assert_eq!(waits.nodes_len(), 0, "no ghost entry from a post-close drop");
    }

    /// Parallel-loop lanes: the SAME node body running at two different
    /// loop frames is two DISTINCT participants, keyed by `(node_id,
    /// frames)`. One lane deadlocked while the other is still computing
    /// must NOT close the buses. A node-id-only key would conflate the
    /// lanes into one entry and the second-half count (`== 2`) would
    /// fail, so this catches a frames-ignoring regression.
    #[test]
    fn parallel_loop_lanes_are_independent_participants() {
        use weft_core::frames::LoopIteration;
        let (waits, coord) = coord_with_tracker();
        // Both lanes share a node id "worker" but differ in frame index.
        let lane0 = FiringLocation::new("worker", vec![LoopIteration { index: 0 }]);
        let lane1 = FiringLocation::new("worker", vec![LoopIteration { index: 1 }]);
        let bus0 = wait_src(&coord.new_bus(BusOptions::default(), lane0.clone()).unwrap());
        let bus1 = wait_src(&coord.new_bus(BusOptions::default(), lane1.clone()).unwrap());
        // Lane 0 parks (deadlocked). Lane 1 is an in-flight task still
        // computing (no wait yet).
        let w0 = waits.enter_wait(&lane0, &bus0);
        waits.observed(&lane0, w0);
        waits.parked(&lane0, w0);
        assert_eq!(
            waits.nodes_len(),
            1,
            "lanes are distinct entries: only lane 0 has parked"
        );
        assert!(
            !waits.deadlock_provable(&in_flight(&[&lane0, &lane1])),
            "lane 1 still computing keeps the buses alive"
        );
        // Lane 1 parks too: now both lanes are stuck. If frames were
        // ignored, lane1's enter_wait would land on lane0's entry and
        // parked_nodes_count would read 1, failing this assert.
        let w1 = waits.enter_wait(&lane1, &bus1);
        waits.observed(&lane1, w1);
        waits.parked(&lane1, w1);
        assert_eq!(waits.parked_nodes_count(), 2, "two distinct parked lanes");
        assert!(
            waits.deadlock_provable(&in_flight(&[&lane0, &lane1])),
            "both lanes parked and caught up on their own buses: deadlock"
        );
    }
}


/// The endpoint-address wait: an infra address is handed to a node
/// only once something answers on it.
///
/// No sockets here. Whether a dead port is REFUSED or silently
/// dropped is the host's choice, and a test that binds one is really
/// testing the machine it runs on. The dialling is a parameter
/// precisely so these can drive it.
#[cfg(test)]
mod endpoint_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use weft_core::CancellationFlag;
    use weft_platform_traits::clock::FakeClock;

    /// A sink the tests ignore. The one test that cares about
    /// breadcrumbs collects them instead.
    fn say(_: String) {}

    /// The gap this covers is real: an address that is not routable
    /// YET must be waited out, and the wait must end the moment
    /// something answers there.
    #[tokio::test]
    async fn an_endpoint_is_held_back_until_something_answers_on_it() {
        let attempts = AtomicUsize::new(0);
        let clock = FakeClock::new();
        let cancel = CancellationFlag::new();
        super::wait_until_answering("db.svc:5432", clock.as_ref(), &cancel, "sql", &say, |_| async {
            // Refused twice, exactly like a workload still booting,
            // then answering.
            match attempts.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => Err("connection refused".to_string()),
                _ => Ok(()),
            }
        })
        .await
        .expect("the wait ends when the address starts answering");
        assert_eq!(attempts.load(Ordering::SeqCst), 3, "it stopped the moment it got an answer");
    }

    /// A workload that takes a long time to boot is WAITED for, not
    /// failed. There is no bound to hit: the address is handed out as
    /// soon as the infra is applied, so what this waits on is the
    /// user's own container starting, and no number is the right
    /// number to give up at.
    #[tokio::test]
    async fn a_slow_workload_is_waited_for_however_long_it_takes() {
        let attempts = AtomicUsize::new(0);
        let clock = FakeClock::new();
        let cancel = CancellationFlag::new();
        // Far past any deadline this used to have.
        let slow = 10_000;
        super::wait_until_answering("db.svc:5432", clock.as_ref(), &cancel, "sql", &say, |_| async {
            if attempts.fetch_add(1, Ordering::SeqCst) < slow {
                Err("connection refused".to_string())
            } else {
                Ok(())
            }
        })
        .await
        .expect("a slow boot is not a failure");
        assert_eq!(attempts.load(Ordering::SeqCst), slow + 1);
    }

    /// Stopping the execution ends the wait. That is the way out of a
    /// workload that never comes up, and why not having a deadline is
    /// safe rather than a hang.
    #[tokio::test]
    async fn cancelling_the_execution_ends_the_wait() {
        let clock = FakeClock::new();
        let cancel = Arc::new(CancellationFlag::new());
        let armed = cancel.clone();
        let attempts = AtomicUsize::new(0);
        let last = super::wait_until_answering(
            "db.svc:5432",
            clock.as_ref(),
            &cancel,
            "sql",
            &say,
            |_| {
                // Cancelled while the very first attempt is in flight,
                // exactly as `weft stop` would.
                armed.cancel();
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err("connection refused".to_string()) }
            },
        )
        .await
        .expect_err("a cancelled wait gives up");
        assert_eq!(last, "connection refused", "and says what it last saw");
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "without trying again");
    }

    /// A wait that is taking a while SAYS SO, on the node's own
    /// output, naming how long it has waited and the way out. A silent
    /// stall is the thing a user cannot act on.
    #[tokio::test]
    async fn a_long_wait_says_where_it_has_got_to() {
        let clock = FakeClock::new();
        let cancel = CancellationFlag::new();
        let said = std::sync::Mutex::new(Vec::<String>::new());
        let attempts = AtomicUsize::new(0);
        // Long enough to cross the breadcrumb interval several times.
        let slow = 1_000;
        super::wait_until_answering(
            "db.svc:5432",
            clock.as_ref(),
            &cancel,
            "endpoint 'sql'",
            &|line: String| said.lock().expect("said").push(line),
            |_| async {
                if attempts.fetch_add(1, Ordering::SeqCst) < slow {
                    Err("connection refused".to_string())
                } else {
                    Ok(())
                }
            },
        )
        .await
        .expect("a slow boot is not a failure");

        let said = said.into_inner().expect("said");
        assert!(!said.is_empty(), "a long wait must not be silent");
        let first = &said[0];
        assert!(first.contains("endpoint 'sql'"), "it names the endpoint: {first}");
        assert!(first.contains("db.svc:5432"), "and the address: {first}");
        assert!(first.contains("weft stop"), "and the way out: {first}");
    }
}
