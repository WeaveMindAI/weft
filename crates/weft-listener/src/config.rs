//! Configuration passed to a listener instance at startup.
//! Populated from env vars (production) or directly (tests).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// This listener pod's name. MUST be the literal Deployment name the
    /// dispatcher minted (injected as a plain `WEFT_POD_NAME` env value,
    /// NOT a downward-API `fieldRef: metadata.name`, which would resolve
    /// to the auto-generated pod name and make rehydrate find zero
    /// signals: see the listener manifest in the dispatcher). A pooled
    /// listener is identified by its pod, not a tenant: placement rows in
    /// the `signal` table point at `listener_pod`, and boot-time
    /// rehydrate rebuilds the registry from `WHERE listener_pod = this
    /// pod`. The listener holds signals from many tenants, so there is no
    /// per-listener tenant; each signal carries its own tenant (see
    /// `RegisteredSignal.tenant_id`).
    pub pod_name: String,
    /// Port the HTTP server binds. The dispatcher calls into
    /// `/register`, `/unregister`, `/process`, `/render`; tenant
    /// pods cannot reach this port (NetworkPolicy denies).
    pub http_port: u16,
    /// Broker base URL. Listener uses it for the rehydrate-time
    /// signal lookup AND to enqueue `FireSignal` tasks when held
    /// events fire (timer expiry, SSE event arrival).
    pub broker_url: String,
}
