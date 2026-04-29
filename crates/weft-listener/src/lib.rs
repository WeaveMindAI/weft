//! Per-project listener service.
//!
//! The listener is a small generic service that owns every wake
//! signal for one active project. It runs as its own process (a
//! pod in the project's k8s namespace in cloud; a local subprocess
//! in dev). Only our code runs here — never user node code — so it
//! is safe to keep alive long-term.
//!
//! Responsibilities:
//!   - Expose user-facing `POST /signal/{token}` and
//!     `GET /signal/{token}` for externally-fired kinds (Webhook,
//!     Form).
//!   - Expose `POST /register` and `POST /unregister` to the
//!     dispatcher for signal lifecycle management.
//!   - Run internal loops per signal kind (timer ticks, SSE
//!     subscriptions, socket connections) and translate each event
//!     into a fire relayed to the dispatcher.
//!   - When any signal fires, POST to the dispatcher's
//!     `/signal-fired` endpoint with `{token, payload}`.
//!
//! The listener never persists anything. If it restarts, the
//! dispatcher re-pushes every active signal via `/register`.

pub mod config;
pub mod kinds;
pub mod protocol;
pub mod registry;
pub mod router;
pub mod relay;

pub use config::ListenerConfig;
pub use protocol::{
    EmptyNotice, FireRelay, RegisterMeNotice, RegisterRequest, RegisterResponse,
    SignalFailedNotice, SignalFiredAck, UnregisterRequest,
};
pub use registry::{Registry, RegisteredSignal};
pub use router::router;

use std::sync::Arc;

/// Listener state shared across HTTP handlers and internal tasks.
#[derive(Clone)]
pub struct ListenerState {
    pub config: Arc<ListenerConfig>,
    pub registry: Arc<Registry>,
    pub relay: Arc<relay::FireRelayer>,
}

impl ListenerState {
    pub fn new(config: ListenerConfig) -> Self {
        let config = Arc::new(config);
        let registry = Arc::new(Registry::new());
        let relay = Arc::new(relay::FireRelayer::new(config.clone(), registry.clone()));
        Self { config, registry, relay }
    }
}
