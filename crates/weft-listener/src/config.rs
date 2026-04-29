//! Configuration passed to a listener instance at startup.
//!
//! Populated from env vars (in production) or directly (in tests /
//! in-process dev). Read once; never reloaded.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// Tenant this listener serves. One listener instance multiplexes
    /// every project belonging to this tenant. Included on every
    /// fire relay so the dispatcher can route fire authentication.
    pub tenant_id: String,
    /// Port the HTTP server listens on. Public-facing for Webhook
    /// and Form kinds; dispatcher-only for /register.
    pub http_port: u16,
    /// Base URL other services use to reach this listener. The
    /// listener mints user-facing signal URLs with this as prefix.
    pub public_base_url: String,
    /// Dispatcher base URL. Listener POSTs fires here.
    pub dispatcher_url: String,
    /// Shared token authenticating listener→dispatcher calls.
    /// Dispatcher verifies on every inbound fire.
    pub relay_token: String,
    /// Shared token the dispatcher presents on /register and
    /// /unregister. Listener verifies.
    pub admin_token: String,
}
