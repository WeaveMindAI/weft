//! Wire types shared between the listener and the dispatcher.
//!
//! `RegisterRequest` / `UnregisterRequest`: dispatcher → listener.
//! `FireRelay`: listener → dispatcher.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use weft_core::primitive::WakeSignalSpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// Opaque token the dispatcher minted. Used as the URL segment
    /// for externally-fired kinds and as the routing key on every
    /// fire relay.
    pub token: String,
    /// The resolved signal spec. Carries everything kind-specific.
    pub spec: WakeSignalSpec,
    /// Node id this signal belongs to. Relayed back so the
    /// dispatcher can attribute fires.
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    /// User-facing URL the dispatcher surfaces to its caller. `None`
    /// for internal-only kinds (Timer, Socket, SSE).
    pub user_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterRequest {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FireRelay {
    pub project_id: String,
    pub token: String,
    pub payload: Value,
}
