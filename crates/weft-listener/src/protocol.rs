//! Wire types shared between the listener and the dispatcher.
//!
//! `RegisterRequest` / `UnregisterRequest`: dispatcher → listener.
//! `FireRelay`: listener → dispatcher.
//! `SignalFiredAck`: dispatcher → listener (response to FireRelay).
//! `EmptyNotice`: listener → dispatcher when its registry empties.
//! `SignalFailedNotice`: listener → dispatcher after exhausting retries.

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
    pub tenant_id: String,
    pub token: String,
    pub payload: Value,
}

/// Dispatcher's reply to a FireRelay. `Consume` means the
/// dispatcher routed the fire successfully and the listener may
/// drop its registration. `Retry` means the dispatcher couldn't
/// route right now (worker still spawning, transient error); the
/// listener should back off and try again.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "ack", rename_all = "snake_case")]
pub enum SignalFiredAck {
    Consume {
        /// Color of the spawned execution (entry signal) or the
        /// color the resume routed to.
        color: String,
    },
    Retry {
        retry_after_ms: u64,
        reason: String,
    },
}

/// Listener tells the dispatcher its in-memory registry has hit
/// zero, meaning nothing left to listen to. Dispatcher checks its
/// own count_for_tenant; if also zero, kills the listener pod.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmptyNotice {
    pub tenant_id: String,
}

/// Listener exhausted its retries on a fire and is unregistering
/// the signal. Dispatcher records `SuspensionFailed` so the
/// affected node fails (only that lane).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalFailedNotice {
    pub tenant_id: String,
    pub token: String,
    pub reason: String,
}

/// Listener boots empty (Pod restart, fresh deploy) and asks the
/// dispatcher to re-push every signal it has on file for this
/// tenant. The dispatcher iterates its `signal` table and POSTs
/// `/register` for each row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterMeNotice {
    pub tenant_id: String,
}
