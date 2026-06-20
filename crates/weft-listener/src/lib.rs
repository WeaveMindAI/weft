//! Per-tenant listener service. Kind-aware processor for signals,
//! running in the tenant's k8s namespace.
//!
//! Endpoints (network-trusted; only reachable from `weft-system`):
//!   POST /register, /unregister, /process, /render, /display, /action.
//! Held-connection loops per stateful kind (Timer, SSE) enqueue a
//! `FireSignal` task through the broker when their event fires; the
//! dispatcher's task picker then runs the same `dispatch_listener_outcome`
//! a stateless fire would.

pub mod config;
pub mod fire_sink;
pub mod kinds;
pub mod protocol;
pub mod registry;
pub mod router;

pub use config::ListenerConfig;
pub use router::router;

use std::sync::Arc;

use weft_broker_client::TokenSource;
use weft_platform_traits::mem_pressure::{
    is_saturated, CgroupMemPressure, MemPressure, SATURATION_MEM_FRACTION,
};
use weft_task_store::TaskStoreClient;

use crate::fire_sink::FireSignalSink;
use crate::registry::Registry;

#[derive(Clone)]
pub struct ListenerState {
    pub config: Arc<ListenerConfig>,
    pub registry: Arc<Registry>,
    /// Sink wrapping the broker task client; held-event kinds call
    /// this when their event fires.
    pub fire_sink: FireSignalSink,
    /// Per-token plaintext secret cache. Populated when a kind's
    /// register_spec mints a secret (api-key with generate=true);
    /// surfaced via /display while the listener pod is alive. Pod
    /// restart drops the cache; the user has to regenerate.
    pub secret_cache: Arc<dashmap::DashMap<String, String>>,
    /// Broker task client + token source, kept on state so the
    /// `/rehydrate` HTTP handler can re-run the boot-time rebuild
    /// without main.rs having to wire a closure through axum.
    pub tasks: Arc<dyn TaskStoreClient>,
    pub token_source: TokenSource,
    /// Reads this pod's real memory pressure. Saturation is decided from
    /// THIS (not a held-connection count), so the listener and the
    /// supervisor use one consistent load metric and a pod sheds load
    /// based on how close it actually is to its memory limit.
    pub mem_pressure: Arc<dyn MemPressure>,
}

impl ListenerState {
    /// The pod's current load. `saturated` is decided from REAL memory
    /// pressure (usage/limit) at the shared `SATURATION_MEM_FRACTION`
    /// threshold, not a work-item count: a count is a dishonest proxy
    /// (5 live sockets are not 500 idle timers). The dispatcher treats
    /// `saturated` as authoritative and stops placing new signals here
    /// once it is true; `mem_pressure` rides along for the scale-down
    /// planner's headroom math + observability. `signals` /
    /// `held_connections` remain for observability only.
    pub fn load_report(&self) -> crate::protocol::LoadReport {
        let fraction = self.mem_pressure.fraction();
        crate::protocol::LoadReport {
            saturated: is_saturated(fraction, SATURATION_MEM_FRACTION),
            mem_pressure: fraction,
            signals: self.registry.len() as u32,
            held_connections: self.registry.held_connection_count() as u32,
        }
    }
}

impl ListenerState {
    pub async fn new(
        config: ListenerConfig,
        tasks: Arc<dyn TaskStoreClient>,
        token_source: TokenSource,
    ) -> anyhow::Result<Self> {
        let fire_sink = FireSignalSink::new(tasks.clone());
        Ok(Self {
            config: Arc::new(config),
            registry: Arc::new(Registry::new()),
            fire_sink,
            secret_cache: Arc::new(dashmap::DashMap::new()),
            tasks,
            token_source,
            mem_pressure: CgroupMemPressure::new(),
        })
    }
}
