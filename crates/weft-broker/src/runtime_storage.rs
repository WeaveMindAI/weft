//! The broker's runtime-file HTTP surface (`ctx.storage`).
//!
//! Worker data path (bearer = the worker's projected SA token, resolved
//! in-process to `Worker { tenant, project, color }`). BYTES never transit the
//! broker: it mints presigned URLs and the worker moves bytes direct to/from the
//! bucket.
//!   POST   /v1/storage/upload/begin      mint the key, charge a known size, open the upload
//!   POST   /v1/storage/upload/parts      reserve + presign the next part(s), exact size signed
//!   POST   /v1/storage/upload/part-done  record a landed part's etag
//!   POST   /v1/storage/upload/complete   assemble + flip the file live
//!   POST   /v1/storage/upload/resume     re-presign the parts that never landed
//!   POST   /v1/storage/upload/abort      cancel, free the reservation
//!   GET    /v1/storage/download-url/{*key}  metadata + presigned GET URL
//!   GET    /v1/storage/meta/{*key}       metadata only (no access bump)
//!   DELETE /v1/storage/files/{*key}
//!   GET    /v1/storage/list?scope=...
//!   POST   /v1/storage/keep
//!   POST   /v1/storage/presign           presigned GET URL for external APIs
//!
//! Control-plane admin path (bearer = the dispatcher's SA -> ControlPlane;
//! the CLI `weft files` verbs proxy through the dispatcher to here):
//!   POST   /v1/storage/admin/tenant-list    one tenant's files
//!   POST   /v1/storage/admin/tenant-usage   one tenant's (count, bytes)
//!   DELETE /v1/storage/admin/files/{*key}   delete one file
//!   POST   /v1/storage/admin/presign        presign one file
//!   POST   /v1/storage/admin/wipe-prefix    weft rm / weft clean
//!   POST   /v1/storage/admin/sweep-exec     terminate sweep for one color
//!
//! The broker is the single gatekeeper: it verifies the caller, runs the pure
//! `key` wall, enforces quota, records metadata, and is the ONLY thing that signs
//! bucket requests. But it NEVER carries the bytes: every read/write is a presigned
//! URL the caller uses to hit the bucket directly, on a short expiry.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;

use weft_core::storage::key::{self, CallerAuth};
use weft_core::storage::{
    DownloadUrlResponse, ListFilesResponse, PartDoneRequest, PresignResponse, PresignResult,
    StorageScope, StoredFileMeta, SweepExecRequest, SweepExecResponse, TenantScopeRequest,
    TenantUsage, UploadAbortRequest, UploadBeginRequest, UploadBeginResponse,
    UploadCompleteRequest, UploadPartsRequest, UploadPartsResponse, UploadResumeRequest,
    UploadResumeResponse, WipePrefixRequest, WipePrefixResponse,
};
use weft_platform_traits::PresignAudience;

use crate::runtime_store::{RuntimeStore, RuntimeStoreError};
use crate::state::BrokerState;

/// The color claim a worker stamps on every storage call, so the broker scopes
/// the op to that execution. (The file's scope / mime / filename / keep travel
/// in the JSON body of upload/begin, not headers.)
// SYNC: HDR_COLOR <-> crates/weft-engine/src/storage.rs (HDR_COLOR)
pub const HDR_COLOR: &str = "x-weft-color";

type ApiError = (StatusCode, String);

/// The runtime-file routes, merged onto the broker router. Mounted only when
/// the deploy has an object-store slot (else the handlers fail loud with a
/// clear "no storage slot configured" 500, never a silent default).
pub fn router() -> Router<Arc<BrokerState>> {
    Router::new()
        // Upload: multipart, bytes go worker->bucket direct on per-part URLs
        // whose exact size is signed in. The broker never carries the bytes.
        .route("/v1/storage/upload/begin", post(upload_begin))
        .route("/v1/storage/upload/parts", post(upload_parts))
        .route("/v1/storage/upload/part-done", post(upload_part_done))
        .route("/v1/storage/upload/complete", post(upload_complete))
        .route("/v1/storage/upload/resume", post(upload_resume))
        .route("/v1/storage/upload/abort", post(upload_abort))
        // Download: mint a presigned GET URL + return the metadata (bytes go
        // bucket->worker direct). Delete stays a plain broker verb (no bytes).
        .route("/v1/storage/download-url/{*key}", get(download_url))
        .route("/v1/storage/files/{*key}", delete(delete_file))
        .route("/v1/storage/meta/{*key}", get(get_meta))
        .route("/v1/storage/list", get(list_files))
        .route("/v1/storage/keep", post(keep_file))
        .route("/v1/storage/presign", post(presign))
        .route("/v1/storage/admin/tenant-list", post(admin_tenant_list))
        .route("/v1/storage/admin/tenant-usage", post(admin_tenant_usage))
        .route("/v1/storage/admin/files/{*key}", delete(admin_delete_file))
        .route("/v1/storage/admin/presign", post(admin_presign))
        .route("/v1/storage/admin/wipe-prefix", post(admin_wipe_prefix))
        .route("/v1/storage/admin/sweep-exec", post(admin_sweep_exec))
}

// ---------- error mapping ----------

// The generic body returned for an internal (500) storage error. The full detail is
// logged server-side; it is NOT echoed to the caller because the runtime-storage
// data path is reached by UNTRUSTED worker pods (user node code runs there), and the
// `Other`/anyhow chain carries internal detail (SQL text, driver messages, table
// names) that must not be disclosed to attacker-controlled code. The typed 4xx
// variants below are safe and actionable, so they keep their specific messages.
const INTERNAL_STORAGE_ERROR_BODY: &str = "internal storage error";

fn map_err(e: RuntimeStoreError) -> ApiError {
    match e {
        RuntimeStoreError::NotFound(m) => (StatusCode::NOT_FOUND, format!("file not found: {m}")),
        RuntimeStoreError::Denied(m) => (StatusCode::FORBIDDEN, m),
        RuntimeStoreError::Invalid(m) => (StatusCode::BAD_REQUEST, m),
        RuntimeStoreError::QuotaExceeded(m) => (StatusCode::PAYLOAD_TOO_LARGE, m),
        RuntimeStoreError::Other(e) => {
            tracing::error!(target: "weft_broker::runtime_storage", error = format!("{e:#}"), "runtime-store op failed");
            (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_STORAGE_ERROR_BODY.to_string())
        }
    }
}

fn map_anyhow(e: anyhow::Error) -> ApiError {
    tracing::error!(target: "weft_broker::runtime_storage", error = format!("{e:#}"), "runtime-store op failed");
    (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_STORAGE_ERROR_BODY.to_string())
}

/// The runtime store, or a loud 500 when the deploy configured no slot. The
/// runtime-file plane is a hard dependency; a cluster without a bucket fails
/// the request rather than silently dropping bytes.
fn store(state: &BrokerState) -> Result<&Arc<RuntimeStore>, ApiError> {
    state.runtime_store.as_ref().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "no object-store slot configured; runtime storage is unavailable. Set \
         WEFT_OBJECT_STORE_ENDPOINT (+ bucket/creds) on the broker."
            .into(),
    ))
}

// ---------- caller resolution ----------

/// Resolve the worker caller for a data-path request (the `x-weft-color`
/// header carries the optional execution color claim). Rejects a
/// control-plane caller on the data path (it uses the admin surface).
async fn worker_caller(state: &Arc<BrokerState>, headers: &HeaderMap) -> Result<CallerAuth, ApiError> {
    let color = headers.get(HDR_COLOR).and_then(|v| v.to_str().ok());
    let caller = crate::auth::resolve_storage_caller(state, headers, color).await?;
    match &caller {
        CallerAuth::Worker { .. } => Ok(caller),
        CallerAuth::ControlPlane => Err((
            StatusCode::FORBIDDEN,
            "control-plane callers use the admin surface, not the data path".into(),
        )),
    }
}

/// Resolve + require a control-plane caller for an admin request.
async fn control_plane(state: &Arc<BrokerState>, headers: &HeaderMap) -> Result<(), ApiError> {
    match crate::auth::resolve_storage_caller(state, headers, None).await? {
        CallerAuth::ControlPlane => Ok(()),
        CallerAuth::Worker { .. } => {
            Err((StatusCode::FORBIDDEN, "the admin surface is dispatcher-only".into()))
        }
    }
}

// ---------- worker data path ----------

/// Validate mime + filename as SERVEABLE at the upload boundary, so a stored file
/// is ALWAYS serveable later (a control char would otherwise make every later get
/// a 500 on already-stored junk the user cannot fix). This is a HTTP-serving
/// sanity check, NOT a weft-type check (the type system validates values at the
/// port, unrelated to storage).
fn validate_serveable(mime: &str, filename: &str) -> Result<(), ApiError> {
    if mime.is_empty() || mime.parse::<axum::http::HeaderValue>().is_err() {
        return Err((StatusCode::BAD_REQUEST, "mimeType is not a serveable media type".into()));
    }
    if filename.contains('"') || filename.chars().any(|c| c.is_control()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "filename must not contain quotes or control characters".into(),
        ));
    }
    Ok(())
}

/// `POST /v1/storage/upload/begin`: start a multipart upload. The broker mints
/// the key, gates the file count, charges a declared total against the byte
/// quota (an over-cap size is rejected before any byte can land anywhere), and
/// opens the bucket's multipart upload. The metadata (mime/filename/keep) is
/// captured here, once.
async fn upload_begin(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadBeginRequest>,
) -> Result<Json<UploadBeginResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    validate_serveable(&req.mime_type, &req.filename)?;
    let (key, part_size) = store
        .begin_upload(
            &caller,
            &req.scope,
            &req.mime_type,
            &req.filename,
            req.keep,
            state.entitlements.as_ref(),
            req.declared_size,
        )
        .await
        .map_err(map_err)?;
    Ok(Json(UploadBeginResponse { key, part_size }))
}

/// `POST /v1/storage/upload/parts`: reserve + presign the next part(s). Each
/// returned URL is signed with the part's exact size; a stream that would
/// cross the byte quota is rejected here (and the upload aborted).
async fn upload_parts(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadPartsRequest>,
) -> Result<Json<UploadPartsResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let parts = store
        .reserve_parts(&caller, &req.key, &req.sizes, state.entitlements.as_ref())
        .await
        .map_err(map_err)?;
    Ok(Json(UploadPartsResponse { parts }))
}

/// `POST /v1/storage/upload/part-done`: record a landed part's etag (verbatim).
async fn upload_part_done(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<PartDoneRequest>,
) -> Result<Response, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    store.record_part(&caller, &req.key, req.part_number, &req.etag).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /v1/storage/upload/complete`: finalize the upload and return the
/// stored-file value the node re-emits onto edges.
async fn upload_complete(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadCompleteRequest>,
) -> Result<Response, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let meta = store.complete_upload(&caller, &req.key).await.map_err(map_err)?;
    let file = crate::runtime_store::meta_to_stored_file(&meta);
    Ok(Json(file.to_value()).into_response())
}

/// `POST /v1/storage/upload/resume`: fresh URLs for the parts that never landed.
async fn upload_resume(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadResumeRequest>,
) -> Result<Json<UploadResumeResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let (part_size, missing) = store.resume_upload(&caller, &req.key).await.map_err(map_err)?;
    Ok(Json(UploadResumeResponse { part_size, missing }))
}

/// `POST /v1/storage/upload/abort`: cancel an in-flight upload, freeing its
/// quota reservation. Idempotent.
async fn upload_abort(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadAbortRequest>,
) -> Result<Response, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    store.abort_upload(&caller, &req.key).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}


/// `GET /v1/storage/download-url/{key}`: return the file's metadata plus a
/// presigned GET URL so the worker reads bytes DIRECTLY from the bucket. Counts as
/// access (bumps a kept file's expiry), like the old streaming get did.
async fn download_url(
    State(state): State<Arc<BrokerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<DownloadUrlQuery>,
) -> Result<Json<DownloadUrlResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let parsed = wall(&caller, &key)?;
    let (meta, url) = store
        .download_url(&parsed, PresignAudience::Internal, q.ttl_secs)
        .await
        .map_err(map_err)?;
    Ok(Json(DownloadUrlResponse { meta, url }))
}

#[derive(Deserialize)]
struct DownloadUrlQuery {
    #[serde(default)]
    ttl_secs: Option<u64>,
}

async fn get_meta(
    State(state): State<Arc<BrokerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Json<StoredFileMeta>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let parsed = wall(&caller, &key)?;
    Ok(Json(store.meta(&parsed).await.map_err(map_err)?))
}

async fn delete_file(
    State(state): State<Arc<BrokerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let parsed = wall(&caller, &key)?;
    store.delete(&parsed).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct ScopeQuery {
    /// JSON-encoded `StorageScope`.
    scope: String,
}


async fn list_files(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Query(q): Query<ScopeQuery>,
) -> Result<Json<ListFilesResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let scope: StorageScope = serde_json::from_str(&q.scope)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad scope: {e}")))?;
    let prefix = key::prefix_for_list(&caller, &scope).map_err(|e| (StatusCode::FORBIDDEN, e))?;
    let files = store.list(&prefix).await.map_err(map_anyhow)?;
    Ok(Json(ListFilesResponse { files }))
}

async fn keep_file(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::KeepRequest>,
) -> Result<Json<StoredFileMeta>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let parsed = wall(&caller, &req.key)?;
    Ok(Json(store.keep(&parsed, req.ttl).await.map_err(map_err)?))
}

async fn presign(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::PresignRequest>,
) -> Result<Json<PresignResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = store(&state)?;
    let parsed = wall(&caller, &req.key)?;
    Ok(Json(PresignResponse { url: store.presign(&parsed, req.ttl_secs).await.map_err(map_err)? }))
}

/// Parse the key through the wall's grammar and confirm the caller may touch
/// it. Every key-addressed worker verb goes through here (so "a key reaching
/// the store passed the wall" holds by construction).
fn wall(caller: &CallerAuth, key: &str) -> Result<key::ParsedKey, ApiError> {
    let parsed = key::parse_key(key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    key::check_key_access(caller, &parsed).map_err(|e| (StatusCode::FORBIDDEN, e))?;
    Ok(parsed)
}

// ---------- control-plane admin (the dispatcher's CLI proxy) ----------
//
// The wire envelopes live in `weft_core::storage` (single definition, shared
// with the dispatcher's admin client), so the two ends cannot drift.

async fn admin_tenant_list(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<TenantScopeRequest>,
) -> Result<Json<ListFilesResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = store(&state)?;
    let prefix = key::ParsedKey::tenant_prefix(&req.tenant)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let files = store.list(&prefix).await.map_err(map_anyhow)?;
    Ok(Json(ListFilesResponse { files }))
}

async fn admin_tenant_usage(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<TenantScopeRequest>,
) -> Result<Json<TenantUsage>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = store(&state)?;
    let (file_count, stored_bytes) = store.tenant_usage(&req.tenant).await.map_err(map_anyhow)?;
    Ok(Json(TenantUsage { stored_bytes, file_count }))
}

async fn admin_delete_file(
    State(state): State<Arc<BrokerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    control_plane(&state, &headers).await?;
    let store = store(&state)?;
    // The store takes a ParsedKey, so the control-plane's key passes the
    // wall's grammar here too (a key reaching the store is always a real one).
    let parsed = key::parse_key(&key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    store.delete(&parsed).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn admin_presign(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::PresignRequest>,
) -> Result<Json<PresignResult>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = store(&state)?;
    let parsed = key::parse_key(&req.key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    // Read the meta first (name + size) so a missing file is a clean 404 before
    // minting, then presign (which also bumps a kept file's TTL).
    let meta = store.meta(&parsed).await.map_err(map_err)?;
    let url = store.presign(&parsed, req.ttl_secs).await.map_err(map_err)?;
    Ok(Json(PresignResult { url, filename: meta.filename, size_bytes: meta.size_bytes }))
}

async fn admin_wipe_prefix(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<WipePrefixRequest>,
) -> Result<Json<WipePrefixResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = store(&state)?;
    // The wipe prefix must be a scope/tenant boundary (the wall's grammar):
    // never a bare `starts_with` that could reach across tenants or owners.
    key::validate_wipe_prefix(&req.prefix).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let wiped = store.wipe_prefix(&req.prefix).await.map_err(map_anyhow)?;
    Ok(Json(WipePrefixResponse { wiped }))
}

async fn admin_sweep_exec(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<SweepExecRequest>,
) -> Result<Json<SweepExecResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = store(&state)?;
    let (swept, lingering) = store.sweep_exec(&req.tenant, &req.color).await.map_err(map_anyhow)?;
    Ok(Json(SweepExecResponse { swept, lingering }))
}
