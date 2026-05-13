//! Configuration passed to a listener instance at startup.
//! Populated from env vars (production) or directly (tests).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// Tenant this listener serves. One listener instance multiplexes
    /// every project belonging to this tenant.
    pub tenant_id: String,
    /// Port the HTTP server binds. The dispatcher calls into
    /// `/register`, `/unregister`, `/process`, `/render`; tenant
    /// pods cannot reach this port (NetworkPolicy denies).
    pub http_port: u16,
    /// Broker base URL. Listener uses it for the rehydrate-time
    /// signal lookup AND to enqueue `FireSignal` tasks when held
    /// events fire (timer expiry, SSE event arrival).
    pub broker_url: String,
}
