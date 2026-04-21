//! Sidecar protocol types.
//!
//! These are the generic envelope types used by InfraClient and platform
//! sidecars. Every sidecar exposes:
//!   POST /action, accepts ActionRequest, returns ActionResponse
//!   GET  /health, liveness check
//!   GET  /outputs, runtime-computed values exposed as node output ports

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionRequest {
    pub action: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResponse {
    pub result: serde_json::Value,
}
