//! Wire types of the storage box's HTTP surface, shared by the
//! service (this crate's `service`), the worker/dispatcher/CLI
//! clients (this crate's `client`), and the dispatcher's handshake
//! relay. One definition; everything else imports it.

use serde::{Deserialize, Serialize};
use weft_core::storage::{KeepTtl, StoredFileMeta};

pub use crate::store::{Usage, UsageDisk};

/// Headers of the data-path requests. Header names live here so the
/// service and the client cannot drift.
pub const HDR_COLOR: &str = "x-weft-color";
pub const HDR_SCOPE: &str = "x-weft-scope";
pub const HDR_MIME: &str = "x-weft-mime";
pub const HDR_FILENAME: &str = "x-weft-filename";
pub const HDR_KEEP: &str = "x-weft-keep";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse {
    pub files: Vec<StoredFileMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeepRequest {
    pub key: String,
    pub ttl: KeepTtl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignRequest {
    pub key: String,
    /// None = service default (~15 min). Clamped to the configured
    /// maximum; a presign is an expiring artifact by definition.
    pub ttl_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignResponse {
    pub url: String,
}

/// Control-plane mint (the dispatcher's user-download handshake).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintRequest {
    pub key: String,
    pub ttl_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintResponse {
    pub capability: String,
    /// Path (query included) on the box's PUBLIC base URL the
    /// capability unlocks: `/public/get?cap=...`.
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepExecRequest {
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepExecResponse {
    pub swept: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WipePrefixRequest {
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WipePrefixResponse {
    pub wiped: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keep_request_round_trips() {
        let r = KeepRequest { key: "exec/c/1".into(), ttl: KeepTtl::Secs { secs: 60 } };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v, json!({"key": "exec/c/1", "ttl": {"kind": "secs", "secs": 60}}));
        let back: KeepRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.key, r.key);
    }

    #[test]
    fn mint_response_round_trips() {
        let r = MintResponse { capability: "v1.a.b".into(), path: "/public/get?cap=v1.a.b".into() };
        let v = serde_json::to_value(&r).unwrap();
        let back: MintResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back.capability, r.capability);
        assert_eq!(back.path, r.path);
    }
}
