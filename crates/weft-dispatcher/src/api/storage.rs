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

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::authenticator::CallerTenant;
use crate::state::DispatcherState;
use crate::tenant::TenantId;

/// A refusal: a status and a message, plus the broker's "still completing"
/// marker ([`weft_core::storage::COMPLETING_HEADER`]) passed through when the
/// broker set it.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
    completing: bool,
}

impl From<(StatusCode, String)> for ApiError {
    fn from((status, message): (StatusCode, String)) -> Self {
        Self { status, message, completing: false }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if self.completing {
            (self.status, [(weft_core::storage::COMPLETING_HEADER, "retry")], self.message).into_response()
        } else {
            (self.status, self.message).into_response()
        }
    }
}

fn internal(e: impl std::fmt::Display) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into()
}

/// Map a storage-proxy error to an HTTP status. A broker 404 (the file doesn't
/// exist) surfaces as 404, and any other terminal broker refusal (a 4xx: bad
/// prefix/scope, denied, over quota, conflict) surfaces with ITS OWN status, not
/// the blanket 500 `internal` would give: `check` tags these as `StorageNotFound` /
/// `BrokerRejected` in the error chain. A 4xx the broker chose is a client-actionable
/// error, so the CLI user should see it, not an opaque 500. Everything else (a real
/// dispatcher/transport fault) is a 500.
pub(crate) fn storage_err(e: anyhow::Error) -> ApiError {
    if e.downcast_ref::<crate::storage::StorageNotFound>().is_some() {
        return (StatusCode::NOT_FOUND, format!("{e:#}")).into();
    }
    if let Some(rejected) = e.downcast_ref::<crate::storage::BrokerRejected>() {
        // Re-map the broker's own 4xx onto our axum StatusCode. Fall back to 500 if
        // it isn't a valid/expected client-error code.
        if let Ok(status) = StatusCode::from_u16(rejected.status.as_u16()) {
            if status.is_client_error() {
                return ApiError { status, message: format!("{e}"), completing: rejected.completing };
            }
        }
    }
    internal(e)
}

/// GET /storage/files/meta/{key}: one active file owned by the caller.
pub async fn file_meta(
    State(state): State<DispatcherState>, caller: CallerTenant, Path(key): Path<String>,
) -> Result<Response, ApiError> {
    let key = ensure_tenant_key(&caller.0, &key)?;
    match crate::storage::file_meta(&state, &key).await {
        Ok(meta) => Ok(Json(meta).into_response()),
        Err(error) if error.downcast_ref::<crate::storage::StorageNotFound>().is_some() => {
            Ok((StatusCode::NOT_FOUND, [("x-weft-not-found", "file")], "file is not stored").into_response())
        }
        Err(error) => Err(storage_err(error)),
    }
}

/// GET /storage/files: every runtime file in the caller's tenant.
pub async fn list_files(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
) -> Result<Json<weft_core::storage::ListFilesResponse>, ApiError> {
    let tenant = caller.0;
    let files = crate::storage::tenant_list(&state, tenant.as_str()).await.map_err(storage_err)?;
    Ok(Json(weft_core::storage::ListFilesResponse { files }))
}

/// GET /storage/usage: the caller's footprint (bytes + file count).
pub async fn usage(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
) -> Result<Json<weft_core::storage::TenantUsage>, ApiError> {
    let tenant = caller.0;
    Ok(Json(crate::storage::tenant_usage(&state, tenant.as_str()).await.map_err(storage_err)?))
}

/// POST /storage/files/download: resolve the acting tenant, prefix the key, and
/// mint a relay download link (with the file's name + size for the client).
/// The answer is a `/public/files/{token}` URL, NOT a presigned bucket URL:
/// the download handshake serves browsers
/// and CLIs on the user's side of any port forward, tunnel, or proxy, and a
/// presigned URL's signature covers the exact host the client must send, so
/// one rewritten hop turns it into a bucket signature error. The token URL
/// comes back on the address THIS caller used, so it rides the same base
/// every other call they make already reaches.
pub async fn download(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    headers: axum::http::HeaderMap,
    Json(req): Json<weft_core::storage::DownloadRequest>,
) -> Result<Json<weft_core::storage::PresignResult>, ApiError> {
    let tenant = caller.0;
    let key = ensure_tenant_key(&tenant, &req.key)?;
    let base = crate::storage::LinkBase::for_request(&headers).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let p = crate::storage::download_link(&state, &base, &key, req.ttl_secs).await.map_err(storage_err)?;
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
    // A bare scope key gets the caller's tenant prepended; anything else (a
    // key with a non-matching tenant, or junk) is denied. The grammar is the
    // shared `is_scope_key`, so it can't fork from the broker's.
    if weft_core::storage::key::is_scope_key(key) {
        Ok(format!("{prefix}{key}"))
    } else {
        Err(ApiError::from((StatusCode::FORBIDDEN, "key does not belong to the caller's tenant".into())))
    }
}

/// GET /public/files/{token}: the PUBLIC RELAY for a minted file link.
/// A pure pass-through to the broker's `/v1/storage/admin/relay`
/// (authenticated with the dispatcher's identity): the broker, the one
/// service holding the bucket's credentials, resolves the token and
/// streams the bytes; this route only carries them out the public door. The
/// token IS the credential (unguessable, expiring); the broker's 404
/// for missing and expired links passes through unchanged, and broker
/// transport failures answer 502 with the detail logged.
pub async fn public_file(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
) -> Result<Response, ApiError> {
    let bad_gateway = |detail: String| {
        tracing::error!(target: "weft_dispatcher::storage", "public file relay: {detail}");
        (StatusCode::BAD_GATEWAY, "file fetch failed".to_string())
    };
    let upstream = state
        .broker
        .request(reqwest::Method::GET, &format!("/v1/storage/admin/relay/{token}"))
        .await
        .map_err(|e| bad_gateway(format!("{e:#}")))?
        .send()
        .await
        .map_err(|e| bad_gateway(e.to_string()))?;
    let status = upstream.status();
    if !status.is_success() {
        // The broker's own answer (404 unknown/expired) keeps its
        // status; only its body text is echoed, never internals.
        let msg = upstream.text().await.unwrap_or_default();
        return Err(ApiError::from((status, msg)));
    }
    let mut builder = Response::builder();
    for header in ["content-type", "content-length", "content-disposition"] {
        if let Some(v) = upstream.headers().get(header) {
            builder = builder.header(header, v);
        }
    }
    builder
        .body(axum::body::Body::from_stream(upstream.bytes_stream()))
        .map_err(internal)
}

// ---------- asset upload (the pre-build sync) ----------
//
// The CLI drives the broker's multipart upload contract through these
// routes: begin mints a PROJECT-scoped key, parts/resume return part URLs
// presigned for the client (bytes go client -> bucket directly), complete
// returns the stored-file marker value. Keys are tenant-walled exactly like
// the download verb.

/// POST /storage/upload/begin: start an upload into the caller's tenant's
/// assets. Returns the minted key (`<tenant>/asset/<sha256>`) + fixed part
/// size, or that the tenant already stores this content.
///
/// The wall here is the TENANT (from auth), the same boundary every other verb
/// on this plane holds, and the quota charged is the caller's own. No project
/// is named: an asset is one file per tenant whichever projects hold it.
pub async fn upload_begin(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<weft_core::storage::AssetUploadBeginRequest>,
) -> Result<Json<weft_core::storage::UploadBeginResponse>, ApiError> {
    let tenant = caller.0;
    let out = crate::storage::upload_begin(&state, tenant.as_str(), req).await.map_err(storage_err)?;
    Ok(Json(out))
}

/// POST /storage/assets/held: which of the named contents the caller's
/// tenant already stores (a publish's diff input). Tenant from auth, so the
/// answer never reaches another tenant's files.
pub async fn assets_held(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<weft_core::storage::AssetsHeldRequest>,
) -> Result<Json<weft_core::storage::AssetsHeldResponse>, ApiError> {
    let held = crate::storage::assets_held(&state, caller.0.as_str(), req).await.map_err(storage_err)?;
    Ok(Json(held))
}

/// Publish the complete asset set of the successfully resolved source.
/// The broker checks every key against the authenticated tenant and project.
pub async fn asset_references(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(mut req): Json<weft_core::storage::AssetReferencesRequest>,
) -> Result<Json<weft_core::storage::AssetsPublished>, ApiError> {
    let project = req.project.parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "project is not a valid id".to_string()))?;
    // The build's own assets, plus every blob a surviving version of the
    // project names: a version's files are the tenant's assets too, and
    // must outlive the builds that stopped referencing them. What no
    // version and no build of ANY of the tenant's projects names expires,
    // which is how a prune reclaims its blobs. A version's blob is KEPT, not
    // required: one that expired or was removed is that version's loss
    // (it cannot be branched back to), never a reason the next build
    // cannot publish. Requiring it left a project unable to build at
    // all, with the way out being the build itself.
    // Read once and computed once: a prune between two reads would make
    // the kept set and the warnings disagree about which versions exist.
    let blob_err = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("version blobs: {e}"));
    let versions = state.versions.versions(project).await.map_err(blob_err)?;
    let source = state.versions.registered_source(project).await.map_err(blob_err)?;
    // Which versions name each blob, so a missing one is reported as
    // the version's loss, by the id `weft tree` shows.
    let mut named_by: std::collections::BTreeMap<String, Vec<String>> = std::collections::BTreeMap::new();
    for version in &versions {
        for key in crate::api::versions::blob_keys(std::iter::once((version.id.as_str(), &version.manifest))).map_err(blob_err)? {
            named_by.entry(ensure_tenant_key(&caller.0, &key)?).or_default().push(version.id[..8].to_string());
        }
    }
    req.kept.extend(named_by.keys().cloned());
    if let Some(source) = &source {
        req.kept.extend(crate::api::versions::blob_keys(std::iter::once(("registered sources", source))).map_err(blob_err)?);
    }
    for key in req.keys.iter_mut().chain(req.kept.iter_mut()) {
        *key = ensure_tenant_key(&caller.0, key)?;
    }
    let outcome = crate::storage::set_asset_references(&state, caller.0.as_str(), req)
        .await.map_err(storage_err)?;
    let warnings: Vec<String> = outcome.missing.iter().map(|key| {
        let versions = named_by.get(key).map(|v| v.join(", ")).unwrap_or_else(|| "the registered sources".into());
        tracing::warn!(%project, key, %versions, "a version names a stored file that no longer exists");
        let (noun, verb, that) = if versions.contains(", ") { ("versions", "name", "those versions") } else { ("version", "names", "that version") };
        format!("{noun} {versions} {verb} a stored file that no longer exists in storage ({}); {that} cannot be branched back to, `weft prune` drops it",
            key.rsplit('/').next().unwrap_or(key))
    }).collect();
    Ok(Json(weft_core::storage::AssetsPublished { warnings }))
}

/// POST /storage/upload/parts: reserve + presign the parts the caller
/// names. Naming them is what makes a reservation idempotent; see
/// `weft_core::storage::UploadPartsRequest`.
pub async fn upload_parts(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(mut req): Json<weft_core::storage::UploadPartsRequest>,
) -> Result<Json<weft_core::storage::UploadPartsResponse>, ApiError> {
    let tenant = caller.0;
    req.key = ensure_tenant_key(&tenant, &req.key)?;
    let out = crate::storage::upload_parts(&state, tenant.as_str(), req).await.map_err(storage_err)?;
    Ok(Json(out))
}

/// POST /storage/upload/part-done: record a landed part's etag.
pub async fn upload_part_done(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(mut req): Json<weft_core::storage::PartDoneRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant = caller.0;
    req.key = ensure_tenant_key(&tenant, &req.key)?;
    crate::storage::upload_part_done(&state, tenant.as_str(), req).await.map_err(storage_err)?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /storage/upload/complete: finalize; returns the stored-file marker
/// value (`{"__weft_image__": {...}}` etc.) the field's config holds.
pub async fn upload_complete(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(mut req): Json<weft_core::storage::UploadCompleteRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = caller.0;
    req.key = ensure_tenant_key(&tenant, &req.key)?;
    let out = crate::storage::upload_complete(&state, tenant.as_str(), req).await.map_err(storage_err)?;
    Ok(Json(out))
}

/// POST /storage/upload/resume: fresh URLs for the parts that never landed.
pub async fn upload_resume(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(mut req): Json<weft_core::storage::UploadResumeRequest>,
) -> Result<Json<weft_core::storage::UploadResumeResponse>, ApiError> {
    let tenant = caller.0;
    req.key = ensure_tenant_key(&tenant, &req.key)?;
    let out = crate::storage::upload_resume(&state, tenant.as_str(), req).await.map_err(storage_err)?;
    Ok(Json(out))
}

/// POST /storage/upload/abort: cancel an in-flight upload.
pub async fn upload_abort(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(mut req): Json<weft_core::storage::UploadAbortRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant = caller.0;
    req.key = ensure_tenant_key(&tenant, &req.key)?;
    crate::storage::upload_abort(&state, tenant.as_str(), req).await.map_err(storage_err)?;
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /storage/files.
pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<weft_core::storage::RemoveFilesRequest>,
) -> Result<Json<weft_core::storage::FilesRemoved>, ApiError> {
    let tenant = caller.0;
    match (&req.key, &req.prefix) {
        (Some(key), None) => {
            let key = ensure_tenant_key(&tenant, key)?;
            crate::storage::delete_key(&state, &key).await.map_err(storage_err)?;
            Ok(Json(weft_core::storage::FilesRemoved { removed: 1 }))
        }
        (None, Some(prefix)) => {
            let prefix = ensure_tenant_prefix(&tenant, prefix)?;
            // `storage_err` (not `internal`): a broker 4xx here (e.g. the broker's
            // `validate_wipe_prefix` refusing a malformed prefix) must reach the CLI
            // as that 4xx, not collapse into an opaque 500 the user can't act on.
            let wiped = crate::storage::wipe_prefix(&state, &prefix).await.map_err(storage_err)?;
            Ok(Json(weft_core::storage::FilesRemoved { removed: wiped }))
        }
        _ => Err(ApiError::from((StatusCode::BAD_REQUEST, "provide exactly one of `key` or `prefix`".into()))),
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
        Err(ApiError::from((StatusCode::FORBIDDEN, "prefix does not belong to the caller's tenant".into())))
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
        let sha = "a".repeat(64);
        assert_eq!(ensure_tenant_key(&t(), &format!("asset/{sha}")).unwrap(), format!("alice/asset/{sha}"));
    }

    #[test]
    fn ensure_tenant_key_rejects_cross_tenant_reach() {
        let err = ensure_tenant_key(&t(), "bob/exec/c1/f").unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        assert!(ensure_tenant_key(&t(), "garbage").is_err());
    }

    #[test]
    fn storage_err_maps_broker_statuses() {
        // A broker 404 surfaces as 404, a broker 4xx refusal keeps ITS status,
        // and anything else (transport fault) is a 500. This is the branch that
        // decides what the CLI user sees; pin it.
        let nf = anyhow::Error::new(crate::storage::StorageNotFound)
            .context("File expired or was deleted. Upload it again and start a new run.");
        let ApiError { status, message, .. } = storage_err(nf);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(message.contains("expired") && message.contains("start a new run"), "{message}");
        let rejected = anyhow::Error::new(crate::storage::BrokerRejected {
            status: reqwest::StatusCode::FORBIDDEN,
            completing: false,
        })
        .context("x");
        assert_eq!(storage_err(rejected).status, StatusCode::FORBIDDEN);
        assert_eq!(storage_err(anyhow::anyhow!("boom")).status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// The broker's "still completing" marker reaches the caller, so a
    /// client can tell "ask again shortly" from a conflict that stays.
    #[test]
    fn storage_err_passes_the_completing_marker_through() {
        let completing = anyhow::Error::new(crate::storage::BrokerRejected {
            status: reqwest::StatusCode::CONFLICT,
            completing: true,
        })
        .context("x");
        let response = storage_err(completing).into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(response.headers().contains_key(weft_core::storage::COMPLETING_HEADER));
        let conflict = anyhow::Error::new(crate::storage::BrokerRejected {
            status: reqwest::StatusCode::CONFLICT,
            completing: false,
        })
        .context("x");
        assert!(!storage_err(conflict).into_response().headers().contains_key(weft_core::storage::COMPLETING_HEADER));
    }

    #[test]
    fn ensure_tenant_prefix_prefixes_and_walls() {
        assert_eq!(ensure_tenant_prefix(&t(), "exec/c1/").unwrap(), "alice/exec/c1/");
        assert_eq!(ensure_tenant_prefix(&t(), "alice/shared/team/").unwrap(), "alice/shared/team/");
        assert!(ensure_tenant_prefix(&t(), "bob/exec/c1/").is_err());
    }
}
