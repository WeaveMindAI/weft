//! Storage-plane HTTP surface on the dispatcher: the `weft files` CLI verbs.
//!
//! The CLI authenticates to the DISPATCHER (which resolves the acting tenant:
//! the `local` tenant by default, or the request's authenticated tenant), and the
//! dispatcher PROXIES each verb to the broker's runtime-file admin surface as the
//! control plane (the broker owns the bucket + metadata).
//! The dispatcher never touches file bytes: a download returns a presigned bucket
//! URL the client streams from directly.
//!
//! Tenant walling: a caller only ever reaches its own tenant's files. The CLI
//! sends a bare scope key (`<scope>/<owner>/<id>`); the dispatcher prefixes the
//! caller's tenant and rejects any key naming a different tenant, so a wipe or
//! download can never cross tenants.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::authenticator::CallerTenant;
use crate::state::DispatcherState;
use crate::tenant::TenantId;

type ApiError = (StatusCode, String);

fn internal(e: impl std::fmt::Display) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
}

/// Map a storage-proxy error to an HTTP status. A broker 404 (the file doesn't
/// exist) surfaces as 404, and any other terminal broker refusal (a 4xx: bad
/// prefix/scope, denied, over quota, conflict) surfaces with ITS OWN status, not
/// the blanket 500 `internal` would give: `check` tags these as `StorageNotFound` /
/// `BrokerRejected` in the error chain. A 4xx the broker chose is a client-actionable
/// error, so the CLI user should see it, not an opaque 500. Everything else (a real
/// dispatcher/transport fault) is a 500.
fn storage_err(e: anyhow::Error) -> ApiError {
    if e.downcast_ref::<crate::storage::StorageNotFound>().is_some() {
        return (StatusCode::NOT_FOUND, "storage object not found".into());
    }
    if let Some(rejected) = e.downcast_ref::<crate::storage::BrokerRejected>() {
        // Re-map the broker's own 4xx onto our axum StatusCode. Fall back to 500 if
        // it isn't a valid/expected client-error code.
        if let Ok(status) = StatusCode::from_u16(rejected.status.as_u16()) {
            if status.is_client_error() {
                return (status, format!("{e}"));
            }
        }
    }
    internal(e)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilesResponse {
    pub files: Vec<weft_core::storage::StoredFileMeta>,
}

/// GET /storage/files: every runtime file in the caller's tenant.
pub async fn list_files(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
) -> Result<Json<FilesResponse>, ApiError> {
    let tenant = caller.0;
    let files = crate::storage::tenant_list(&state, tenant.as_str()).await.map_err(storage_err)?;
    Ok(Json(FilesResponse { files }))
}

/// GET /storage/usage: the caller's footprint (bytes + file count).
pub async fn usage(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = caller.0;
    let u = crate::storage::tenant_usage(&state, tenant.as_str()).await.map_err(storage_err)?;
    Ok(Json(serde_json::json!({
        "storedBytes": u.stored_bytes,
        "fileCount": u.file_count,
    })))
}

#[derive(Debug, Deserialize)]
pub struct DownloadRequest {
    pub key: String,
    /// Presigned-URL lifetime; None = broker default (~15 min).
    pub ttl_secs: Option<u64>,
}

/// POST /storage/files/download: resolve the acting tenant, prefix the key, and
/// ask the broker to presign a single-file download URL (with the file's name +
/// size for the client). Returns `PresignResult` directly (the presign result IS
/// the download-handshake response; no separate same-fields struct).
pub async fn download(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<DownloadRequest>,
) -> Result<Json<weft_core::storage::PresignResult>, ApiError> {
    let tenant = caller.0;
    let key = ensure_tenant_key(&tenant, &req.key)?;
    let p = crate::storage::presign(&state, &key, req.ttl_secs).await.map_err(storage_err)?;
    Ok(Json(p))
}

/// Prefix a CLI-supplied key with the caller's tenant if it isn't already, and
/// reject a key that names a DIFFERENT tenant (a cross-tenant reach). The CLI
/// thinks in `<scope>/<owner>/<id>`; the wire key is
/// `<tenant>/<scope>/<owner>/<id>`.
fn ensure_tenant_key(tenant: &TenantId, key: &str) -> Result<String, ApiError> {
    let prefix = format!("{}/", tenant.as_str());
    if key.starts_with(&prefix) {
        return Ok(key.to_string());
    }
    // A bare 3-segment scope key gets the caller's tenant prepended; anything
    // else (a 4-segment key with a non-matching tenant, or junk) is denied. The
    // tag set comes from the shared `is_scope_tag` so it can't fork from the
    // broker's grammar.
    let segs: Vec<&str> = key.split('/').collect();
    match segs.as_slice() {
        [scope, _, _] if weft_core::storage::key::is_scope_tag(scope) => Ok(format!("{prefix}{key}")),
        _ => Err((StatusCode::FORBIDDEN, "key does not belong to the caller's tenant".into())),
    }
}

#[derive(Serialize)]
pub struct PublicBaseResponse {
    pub public_base_url: String,
}

/// GET /storage/public-base: the object store's browser-facing origin, which the
/// webview adds to its CSP `img-src`/`media-src` so an `<img>`/`<video>` can
/// stream presigned bytes directly from the bucket. `WEFT_OBJECT_STORE_PUBLIC_ENDPOINT`
/// overrides the in-cluster slot endpoint with the browser-reachable one (e.g. a
/// public object-store or ingress host); it falls back to the slot endpoint.
pub async fn public_base() -> Json<PublicBaseResponse> {
    let public_base_url = std::env::var("WEFT_OBJECT_STORE_PUBLIC_ENDPOINT")
        .or_else(|_| std::env::var("WEFT_OBJECT_STORE_ENDPOINT"))
        .unwrap_or_default();
    Json(PublicBaseResponse { public_base_url })
}

#[derive(Debug, Deserialize)]
pub struct RemoveRequest {
    /// Exactly one of `key` (one file) or `prefix` (a whole space, e.g.
    /// `shared/team/` or `exec/<color>/`).
    pub key: Option<String>,
    pub prefix: Option<String>,
}

/// DELETE /storage/files.
pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<RemoveRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = caller.0;
    match (&req.key, &req.prefix) {
        (Some(key), None) => {
            let key = ensure_tenant_key(&tenant, key)?;
            crate::storage::delete_key(&state, &key).await.map_err(storage_err)?;
            Ok(Json(serde_json::json!({ "removed": 1 })))
        }
        (None, Some(prefix)) => {
            let prefix = ensure_tenant_prefix(&tenant, prefix)?;
            // `storage_err` (not `internal`): a broker 4xx here (e.g. the broker's
            // `validate_wipe_prefix` refusing a malformed prefix) must reach the CLI
            // as that 4xx, not collapse into an opaque 500 the user can't act on.
            let wiped = crate::storage::wipe_prefix(&state, &prefix).await.map_err(storage_err)?;
            Ok(Json(serde_json::json!({ "removed": wiped })))
        }
        _ => Err((StatusCode::BAD_REQUEST, "provide exactly one of `key` or `prefix`".into())),
    }
}

/// Prefix a CLI-supplied wipe prefix with the caller's tenant. The CLI sends
/// `<scope>/<owner>/`; the wire prefix is `<tenant>/<scope>/<owner>/`. A prefix
/// already starting with the caller's tenant passes through; one starting with a
/// known scope tag gets prefixed; anything else (a cross-tenant reach) is denied.
fn ensure_tenant_prefix(tenant: &TenantId, prefix: &str) -> Result<String, ApiError> {
    let t = format!("{}/", tenant.as_str());
    if prefix.starts_with(&t) {
        return Ok(prefix.to_string());
    }
    let first = prefix.split('/').next().unwrap_or("");
    if weft_core::storage::key::is_scope_tag(first) {
        Ok(format!("{t}{prefix}"))
    } else {
        Err((StatusCode::FORBIDDEN, "prefix does not belong to the caller's tenant".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> TenantId {
        TenantId("alice".into())
    }

    #[test]
    fn ensure_tenant_key_prefixes_bare_scope_keys() {
        assert_eq!(ensure_tenant_key(&t(), "exec/c1/f").unwrap(), "alice/exec/c1/f");
        assert_eq!(ensure_tenant_key(&t(), "project/p1/f").unwrap(), "alice/project/p1/f");
        assert_eq!(ensure_tenant_key(&t(), "shared/team/f").unwrap(), "alice/shared/team/f");
        assert_eq!(ensure_tenant_key(&t(), "alice/exec/c1/f").unwrap(), "alice/exec/c1/f");
    }

    #[test]
    fn ensure_tenant_key_rejects_cross_tenant_reach() {
        let err = ensure_tenant_key(&t(), "bob/exec/c1/f").unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(ensure_tenant_key(&t(), "garbage").is_err());
    }

    #[test]
    fn storage_err_maps_broker_statuses() {
        // A broker 404 surfaces as 404, a broker 4xx refusal keeps ITS status,
        // and anything else (transport fault) is a 500. This is the branch that
        // decides what the CLI user sees; pin it.
        let nf = anyhow::Error::new(crate::storage::StorageNotFound).context("x");
        assert_eq!(storage_err(nf).0, StatusCode::NOT_FOUND);
        let rejected = anyhow::Error::new(crate::storage::BrokerRejected {
            status: reqwest::StatusCode::FORBIDDEN,
        })
        .context("x");
        assert_eq!(storage_err(rejected).0, StatusCode::FORBIDDEN);
        assert_eq!(storage_err(anyhow::anyhow!("boom")).0, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn ensure_tenant_prefix_prefixes_and_walls() {
        assert_eq!(ensure_tenant_prefix(&t(), "exec/c1/").unwrap(), "alice/exec/c1/");
        assert_eq!(ensure_tenant_prefix(&t(), "alice/shared/team/").unwrap(), "alice/shared/team/");
        assert!(ensure_tenant_prefix(&t(), "bob/exec/c1/").is_err());
    }
}
