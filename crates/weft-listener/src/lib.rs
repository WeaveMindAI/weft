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
}

impl ListenerState {
    pub async fn new(
        config: ListenerConfig,
        tasks: Arc<dyn TaskStoreClient>,
        token_source: TokenSource,
    ) -> anyhow::Result<Self> {
        let fire_sink = FireSignalSink::new(tasks.clone(), config.tenant_id.clone());
        Ok(Self {
            config: Arc::new(config),
            registry: Arc::new(Registry::new()),
            fire_sink,
            secret_cache: Arc::new(dashmap::DashMap::new()),
            tasks,
            token_source,
        })
    }
}
