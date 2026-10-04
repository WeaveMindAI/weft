//! The broker's runtime-file HTTP surface (`ctx.storage`).
//!
//! Worker data path (bearer = the worker's platform identity token, resolved
//! in-process to `Worker { tenant, project, execution_id }`). BYTES never transit the
//! broker: it mints presigned URLs and the worker moves bytes direct to/from the
//! bucket.
//!   POST   /v1/storage/upload/begin      mint the key, charge a known size, open the upload
//!   POST   /v1/storage/upload/replace    the same, for new bytes overwriting a stored file
//!   POST   /v1/storage/upload/parts      reserve + presign the NAMED part(s), exact size signed
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
//!   POST   /v1/storage/admin/sweep-exec     terminate sweep for one execution
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
    AssetUploadBeginRequest, AssetsHeldRequest, AssetsHeldResponse, DownloadUrlResponse, ListFilesResponse,
    PartDoneRequest,
    PresignResponse, PresignResult, StorageScope, StoredFile, StoredFileMeta, SweepExecRequest,
    SweepExecResponse, Tenanted, TenantScopeRequest, TenantUsage, UploadAbortRequest,
    UploadBeginRequest, UploadBeginResponse, UploadCompleteRequest, UploadPartsRequest,
    UploadPartsResponse, UploadResumeRequest, UploadResumeResponse, WipePrefixRequest,
    WipePrefixResponse,
};
use weft_platform_traits::PresignAudience;

use crate::auth::control_plane;
use crate::runtime_store::{RuntimeStore, RuntimeStoreError};
use crate::state::BrokerState;

/// The execution claim a worker stamps on every storage call, so the broker scopes
/// the op to that execution. (The file's scope / mime / filename / keep travel
/// in the JSON body of upload/begin, not headers.)
// SYNC: HDR_EXECUTION_ID <-> crates/weft-engine/src/storage.rs (HDR_EXECUTION_ID)
pub const HDR_EXECUTION_ID: &str = "x-weft-execution-id";

/// A refusal: a status and a message, plus the "still completing" marker
/// ([`weft_core::storage::COMPLETING_HEADER`]) when the refusal is that
/// one, so every verb that can meet a claimed upload answers it the same way.
#[derive(Debug)]
struct ApiError {
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

/// The runtime-file routes, merged onto the broker router. Mounted only when
/// the deploy has an object-store slot (else the handlers fail loud with a
/// clear "no storage slot configured" 500, never a silent default).
pub fn router() -> Router<Arc<BrokerState>> {
    Router::new()
        // Upload: multipart, bytes go worker->bucket direct on per-part URLs
        // whose exact size is signed in. The broker never carries the bytes.
        .route("/v1/storage/upload/begin", post(upload_begin))
        .route("/v1/storage/upload/replace", post(upload_replace))
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
        .route("/v1/storage/identity", post(find_identity))
        .route("/v1/storage/keep", post(keep_file))
        .route("/v1/storage/presign", post(presign))
        .route("/v1/storage/public-link", post(public_link))
        // Admin upload: the dispatcher drives the SAME multipart contract on
        // behalf of a tenant's editor session (a file-drop config field).
        // Project-scoped only; part URLs are presigned for the browser-facing
        // endpoint (the editor PUTs bytes to the bucket directly).
        .route("/v1/storage/admin/upload/begin", post(admin_upload_begin))
        .route("/v1/storage/admin/upload/parts", post(admin_upload_parts))
        .route("/v1/storage/admin/upload/part-done", post(admin_upload_part_done))
        .route("/v1/storage/admin/upload/complete", post(admin_upload_complete))
        .route("/v1/storage/admin/upload/resume", post(admin_upload_resume))
        .route("/v1/storage/admin/upload/abort", post(admin_upload_abort))
        .route("/v1/storage/admin/tenant-list", post(admin_tenant_list))
        .route("/v1/storage/admin/assets-held", post(admin_assets_held))
        .route("/v1/storage/admin/asset-references", post(admin_asset_references))
        .route("/v1/storage/admin/tenant-usage", post(admin_tenant_usage))
        .route("/v1/storage/admin/files/{*key}", delete(admin_delete_file))
        .route("/v1/storage/admin/meta/{*key}", get(admin_meta))
        .route("/v1/storage/admin/presign", post(admin_presign))
        .route("/v1/storage/admin/download-link", post(admin_download_link))
        .route("/v1/storage/admin/relay/{token}", axum::routing::get(admin_relay))
        .route("/v1/storage/admin/wipe-prefix", post(admin_wipe_prefix))
        .route("/v1/storage/admin/sweep-exec", post(admin_sweep_exec))
}

// ---------- error mapping ----------

// The generic body returned for an internal (500) storage error. The full detail is
// logged by the broker; it is NOT echoed to the caller because the runtime-storage
// data path is reached by UNTRUSTED workers (user node code runs there), and the
// `Other`/anyhow chain carries internal detail (SQL text, driver messages, table
// names) that must not be disclosed to attacker-controlled code. The typed 4xx
// variants below are safe and actionable, so they keep their specific messages.
const INTERNAL_STORAGE_ERROR_BODY: &str = "internal storage error (the broker's log names the cause)";

fn map_err(e: RuntimeStoreError) -> ApiError {
    let completing = matches!(e, RuntimeStoreError::Completing(_));
    let (status, message) = match e {
        RuntimeStoreError::NotFound(key) => (StatusCode::NOT_FOUND, unavailable_file_message(&key)),
        RuntimeStoreError::Denied(m) => (StatusCode::FORBIDDEN, m),
        RuntimeStoreError::Invalid(m) => (StatusCode::BAD_REQUEST, m),
        RuntimeStoreError::QuotaExceeded(m) => (StatusCode::PAYLOAD_TOO_LARGE, m),
        RuntimeStoreError::Conflict(m) | RuntimeStoreError::Completing(m) => (StatusCode::CONFLICT, m),
        RuntimeStoreError::Lost(m) => (StatusCode::GONE, m),
        // SYNC: replace outcome statuses <-> crates/weft-engine/src/storage.rs WorkerStorage::replace
        RuntimeStoreError::Stale(m) => (StatusCode::PRECONDITION_FAILED, m),
        RuntimeStoreError::Other(e) => {
            tracing::error!(target: "weft_broker::runtime_storage", error = format!("{e:#}"), "runtime-store op failed");
            (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_STORAGE_ERROR_BODY.to_string())
        }
    };
    ApiError { status, message, completing }
}

fn unavailable_file_message(key: &str) -> String {
    let is_asset =
        key::parse_key(key).is_ok_and(|p| matches!(p.scope, key::KeyScope::Asset));
    // Expiry and explicit deletion both remove the row. Do not invent which
    // happened once that evidence is gone; explain both and name the recovery.
    // The recovery differs by scope: an asset is uploaded by the build from
    // the project's own source, so rebuilding puts it back and clears its
    // countdown. Keep File is for a file a NODE made, and the store refuses
    // it on an asset, so naming it here would send the reader after a knob
    // this file does not have.
    if is_asset {
        return format!(
            "File '{key}' is no longer available: it may have expired or been deleted. \
             A file the project uploads expires after {} days once nothing in the \
             workflow references it any more, even if an older run is still waiting. \
             Build and deploy the project again to upload it back, then start a new run.",
            crate::runtime_store::DEFAULT_KEEP_TTL_SECS / 86400
        );
    }
    format!(
        "File '{key}' is no longer available: it may have expired or been deleted. \
         Files can expire according to their keep duration, even while a run is \
         waiting. Create the file again and start a new run; to keep a file longer \
         next time, put a Keep File node after the one that made it, or raise its days."
    )
}

fn map_anyhow(e: anyhow::Error) -> ApiError {
    tracing::error!(target: "weft_broker::runtime_storage", error = format!("{e:#}"), "runtime-store op failed");
    (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_STORAGE_ERROR_BODY.to_string()).into()
}

// ---------- caller resolution ----------

/// Resolve the worker caller for a data-path request (the `x-weft-execution_id`
/// header carries the optional execution claim). Rejects a
/// control-plane caller on the data path (it uses the admin surface).
async fn worker_caller(state: &Arc<BrokerState>, headers: &HeaderMap) -> Result<CallerAuth, ApiError> {
    let execution_id = headers.get(HDR_EXECUTION_ID).and_then(|v| v.to_str().ok());
    let caller = crate::auth::resolve_storage_caller(state, headers, execution_id).await?;
    match &caller {
        CallerAuth::Worker { .. } => Ok(caller),
        CallerAuth::ControlPlane | CallerAuth::Tenant { .. } => Err(ApiError::from((
            StatusCode::FORBIDDEN,
            "control-plane callers use the admin surface, not the data path".into(),
        ))),
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
        return Err(ApiError::from((StatusCode::BAD_REQUEST, "mimeType is not a serveable media type".into())));
    }
    if filename.contains('"') || filename.chars().any(|c| c.is_control()) {
        return Err(ApiError::from((
            StatusCode::BAD_REQUEST,
            "filename must not contain quotes or control characters".into(),
        )));
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
    let store = &state.runtime_store;
    // Assets are sync-managed derived state (content-hash ids minted by the
    // pre-build sync through the control-plane surface); node code writing
    // into the asset scope would fork that ownership, so refuse it loudly.
    if matches!(req.scope, StorageScope::Asset) {
        return Err(ApiError::from((
            StatusCode::FORBIDDEN,
            "the asset scope is managed by the pre-build asset sync; node code writes \
             execution/project/shared scopes"
                .into(),
        )));
    }
    validate_serveable(&req.mime_type, &req.filename)?;
    let begun = store
        .begin_upload(
            &caller,
            &crate::runtime_store::UploadSpec {
                scope: &req.scope,
                mime: &req.mime_type,
                filename: &req.filename,
                keep: req.keep,
                declared_size: req.declared_size,
                content_hash: None,
                identity: req.identity.as_deref(),
            },
            state.entitlements.as_ref(),
        )
        .await
        .map_err(map_err)?;
    match begun {
        crate::runtime_store::BeginUpload::Ready { key, part_size } => {
            Ok(Json(UploadBeginResponse { key, part_size, already_stored: false, resume: false }))
        }
        // An identified begin whose scope already holds that identity:
        // the caller's own dedup answer, so it uploads nothing and reads
        // the file it named. There is no part size because there are no
        // parts to send.
        crate::runtime_store::BeginUpload::AlreadyStored { key } => {
            Ok(Json(UploadBeginResponse {
                key,
                part_size: 0,
                already_stored: true,
                resume: false,
            }))
        }
        // Already part way up: the caller carries on with it rather than
        // being refused until the leftovers are cleared.
        crate::runtime_store::BeginUpload::Resume { key, part_size } => {
            Ok(Json(UploadBeginResponse {
                key,
                part_size,
                already_stored: false,
                resume: true,
            }))
        }
    }
}

/// `POST /v1/storage/upload/replace`: begin overwriting a stored file with
/// new bytes. Answers like a begin; the `key` is the replacement's own
/// upload key, which the parts/complete verbs name as for any upload, and
/// complete answers the REPLACED file's value (its key, its new size and
/// version). 409 while another replacement of the file is in flight, 412
/// when `expected_version` is not the file's version any more.
async fn upload_replace(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::UploadReplaceRequest>,
) -> Result<Json<UploadBeginResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let (key, part_size) = state
        .runtime_store
        .begin_replace(&caller, &req.key, req.declared_size, req.expected_version, state.entitlements.as_ref())
        .await
        .map_err(map_err)?;
    Ok(Json(UploadBeginResponse { key, part_size, already_stored: false, resume: false }))
}

/// `POST /v1/storage/upload/parts`: reserve + presign the parts the caller
/// NAMES (a part number is the part's position, so asking twice reserves
/// once; see `weft_core::storage::UploadPartsRequest`). Each
/// returned URL is signed with the part's exact size; a stream that would
/// cross the byte quota is rejected here (and the upload aborted).
async fn upload_parts(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadPartsRequest>,
) -> Result<Json<UploadPartsResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
    let parts = store
        .reserve_parts(
            &caller,
            &req.key,
            &req.parts,
            state.entitlements.as_ref(),
            PresignAudience::Internal,
        )
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
    let store = &state.runtime_store;
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
    let store = &state.runtime_store;
    complete_response(store.complete_upload(&caller, &req.key).await)
}

/// A completion's answer. "Still completing" is a 409 carrying the
/// [`weft_core::storage::COMPLETING_HEADER`] marker, the one answer that
/// means "ask complete again" (SYNC: crates/weft-engine/src/storage.rs
/// complete_step <-> crates/weft-cli/src/commands/assets.rs completing_step).
fn complete_response(result: Result<StoredFileMeta, RuntimeStoreError>) -> Result<Response, ApiError> {
    let meta = result.map_err(map_err)?;
    Ok(Json(StoredFile::from(&meta).to_value()).into_response())
}

/// `POST /v1/storage/upload/resume`: fresh URLs for the parts that never landed.
async fn upload_resume(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadResumeRequest>,
) -> Result<Json<UploadResumeResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
    let (part_size, missing, reserved_bytes) = store
        .resume_upload(&caller, &req.key, state.entitlements.as_ref(), PresignAudience::Internal)
        .await
        .map_err(map_err)?;
    Ok(Json(UploadResumeResponse { part_size, missing, reserved_bytes }))
}

/// `POST /v1/storage/upload/abort`: cancel an in-flight upload, freeing its
/// quota reservation. Idempotent.
async fn upload_abort(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadAbortRequest>,
) -> Result<Response, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
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
    let store = &state.runtime_store;
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
    let store = &state.runtime_store;
    let parsed = wall(&caller, &key)?;
    Ok(Json(store.meta(&parsed).await.map_err(map_err)?))
}

async fn admin_meta(
    State(state): State<Arc<BrokerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Json<StoredFileMeta>, ApiError> {
    control_plane(&state, &headers).await?;
    let parsed = key::parse_key(&key).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    Ok(Json(state.runtime_store.meta(&parsed).await.map_err(map_err)?))
}

async fn delete_file(
    State(state): State<Arc<BrokerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
    let parsed = wall(&caller, &key)?;
    // An asset is shared by every project of the tenant and lives while any
    // version references it (`set_asset_references`); node code deleting one
    // would pull it out from under another project, so refuse it like the
    // upload verbs do.
    if matches!(parsed.scope, key::KeyScope::Asset) {
        return Err(ApiError::from((
            StatusCode::FORBIDDEN,
            "the asset scope is managed by version publishing; an asset goes when no version references it".into(),
        )));
    }
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
    let store = &state.runtime_store;
    let scope: StorageScope = serde_json::from_str(&q.scope)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad scope: {e}")))?;
    let prefix = key::prefix_for_list(&caller, &scope).map_err(|e| (StatusCode::FORBIDDEN, e))?;
    let files = store.list(&prefix).await.map_err(map_anyhow)?;
    Ok(Json(ListFilesResponse { files }))
}

/// `POST /v1/storage/identity`: the file stored under an identity in a
/// scope, or null. Answers from the row alone; no bucket access, no
/// expiry bump (nothing was read).
async fn find_identity(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::IdentityLookupRequest>,
) -> Result<Json<weft_core::storage::IdentityLookupResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
    let file = store.find_identity(&caller, &req.scope, &req.identity).await.map_err(map_err)?;
    Ok(Json(weft_core::storage::IdentityLookupResponse { file }))
}

async fn keep_file(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::KeepRequest>,
) -> Result<Json<StoredFileMeta>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
    let parsed = wall(&caller, &req.key)?;
    Ok(Json(store.keep(&parsed, req.ttl).await.map_err(map_err)?))
}

/// `POST /v1/storage/presign`: the link a node body hands out for a stored
/// file, and the `url` the runtime puts on every file marker before a body
/// runs. The internet-reachable link when the install serves one (so a
/// provider can fetch it too), else a URL signed for the install's own
/// address: the node body can fetch that one, nothing outside can.
async fn presign(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::PresignRequest>,
) -> Result<Json<PresignResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
    let parsed = wall(&caller, &req.key)?;
    let url = match public_link_url(&state, store, &parsed, req.ttl_secs, &weft_core::storage::LinkReach::Internet).await? {
        Some(url) => url,
        None => {
            store
                .download_url(&parsed, PresignAudience::Internal, req.ttl_secs)
                .await
                .map_err(map_err)?
                .1
        }
    };
    Ok(Json(PresignResponse { url }))
}

/// `POST /v1/storage/public-link`: a URL the asker named in `reach` can
/// fetch the file from, or `url: None` when there is none (an internet
/// asker then inlines the bytes instead).
async fn public_link(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::PresignRequest>,
) -> Result<Json<weft_core::storage::PublicLinkResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
    let parsed = wall(&caller, &req.key)?;
    let url = public_link_url(&state, store, &parsed, req.ttl_secs, &req.reach).await?;
    Ok(Json(weft_core::storage::PublicLinkResponse { url }))
}

/// Mint the link for a file, or `None` when the asker has no address
/// to be given. Three configurations, one rule each:
/// - the open internet reaches the store's presigned URLs
///   (`ObjectStoreSettings::public_internet`): the bucket's own presigned
///   URL, zero relay hops;
/// - a base exists for the asker ([`relay_base`]): a relay link under
///   it, resolved by the public `/public/files/{token}` route;
/// - neither: no link exists.
async fn public_link_url(
    state: &BrokerState,
    store: &RuntimeStore,
    parsed: &key::ParsedKey,
    ttl_secs: Option<u64>,
    reach: &weft_core::storage::LinkReach,
) -> Result<Option<String>, ApiError> {
    let base = relay_base(state.internet_base(), &state.public_base_url, reach)?;
    Ok(match link_route(state.object_store_public_internet, base) {
        LinkRoute::DirectPresign => Some(store.presign(parsed, ttl_secs).await.map_err(map_err)?),
        LinkRoute::Relay(base) => {
            let token = store.mint_public_link(parsed, ttl_secs).await.map_err(map_err)?;
            Some(format!("{}/public/files/{token}", base.trim_end_matches('/')))
        }
        LinkRoute::InstallOnly => None,
    })
}

/// Which way a file link is served, decided from two configured facts
/// alone: the operator's declaration that the bucket's public endpoint
/// is a real internet host, and whether an internet-reachable base
/// exists at all.
#[derive(Debug, PartialEq, Eq)]
enum LinkRoute<'a> {
    /// The bucket's own presigned URL, zero relay hops.
    DirectPresign,
    /// A relay link under the internet base.
    Relay(&'a str),
    /// No internet-reachable link exists: a node body gets a link signed
    /// for the install's own address, and an outside consumer gets the
    /// bytes inline.
    InstallOnly,
}

/// The base a relay link is minted under for `reach`. An internet
/// asker gets the internet address. A caller of the install gets the
/// address its own request came in on, when the run carries one: one
/// install answers on several addresses at once, and the caller can
/// reach exactly the one it used. A caller no request stands behind
/// gets the configured address: the internet one when there is one,
/// else the install's own stable base.
///
/// The caller's base is taken as the worker states it. That is safe for
/// the same reason a request-built link is: the link is a capability URL
/// whose token is the credential, so a base pointing elsewhere only
/// sends the token somewhere its holder chose. It must still be a bare
/// http(s) address, or the link built on it would not be a link.
fn relay_base<'a>(
    internet_base: Option<&'a str>,
    install_base: &'a str,
    reach: &'a weft_core::storage::LinkReach,
) -> Result<Option<&'a str>, ApiError> {
    Ok(match reach {
        weft_core::storage::LinkReach::Internet => internet_base,
        weft_core::storage::LinkReach::Caller { base: Some(base) } => Some(caller_base(base)?),
        weft_core::storage::LinkReach::Caller { base: None } => internet_base.or(Some(install_base)),
    })
}

/// A caller's base, refused unless it is an http(s) address with a host
/// and nothing past an optional path (no query, fragment or userinfo).
fn caller_base(base: &str) -> Result<&str, ApiError> {
    let refused = || (StatusCode::BAD_REQUEST, format!("the caller's base '{base}' is not an http(s) address"));
    let parsed = url::Url::parse(base).map_err(|_| refused())?;
    let bare = matches!(parsed.scheme(), "http" | "https")
        && parsed.host_str().is_some()
        && parsed.query().is_none()
        && parsed.fragment().is_none()
        && parsed.username().is_empty()
        && parsed.password().is_none();
    if bare { Ok(base) } else { Err(refused().into()) }
}

fn link_route(bucket_internet: bool, internet_base: Option<&str>) -> LinkRoute<'_> {
    if bucket_internet {
        LinkRoute::DirectPresign
    } else if let Some(base) = internet_base {
        LinkRoute::Relay(base)
    } else {
        LinkRoute::InstallOnly
    }
}

/// Parse the key through the wall's grammar and confirm the caller may touch
/// it. Every key-addressed worker verb goes through here (so "a key reaching
/// the store passed the wall" holds by construction).
fn wall(caller: &CallerAuth, key: &str) -> Result<key::ParsedKey, ApiError> {
    let parsed = key::parse_key(key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    key::check_key_access(caller, &parsed).map_err(|e| (StatusCode::FORBIDDEN, e))?;
    Ok(parsed)
}

// ---------- control-plane admin upload (the dispatcher's editor proxy) ----------
//
// The dispatcher verified the acting tenant; the broker re-runs the key wall
// against that tenant on every key-addressed verb, so a dispatcher bug can
// still never cross tenants. The store code is the exact worker path: the
// admin surface only differs in WHO vouches for the caller and in the part
// URLs' audience (External: the editor's browser PUTs to the bucket directly).

/// Re-derive the acting caller for a key-addressed admin upload verb: the key
/// must be ASSET-scoped (the one admin-uploadable plane) and belong to the
/// vouched tenant. Returns the caller whose walls (`check_key_access`) then
/// hold for the store call.
fn tenant_caller_for_key(tenant: &str, k: &str) -> Result<CallerAuth, ApiError> {
    let parsed = key::parse_key(k).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if parsed.tenant != tenant {
        return Err(ApiError::from((
            StatusCode::FORBIDDEN,
            "denied: key does not belong to the acting tenant".into(),
        )));
    }
    if parsed.scope != key::KeyScope::Asset {
        return Err(ApiError::from((
            StatusCode::FORBIDDEN,
            "admin uploads are asset-scoped; the key names a different scope".into(),
        )));
    }
    Ok(CallerAuth::Tenant { tenant: tenant.to_string() })
}

async fn admin_upload_begin(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Tenanted { tenant, inner: req }): Json<Tenanted<AssetUploadBeginRequest>>,
) -> Result<Json<UploadBeginResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    validate_serveable(&req.mime_type, &req.filename)?;
    let caller = CallerAuth::Tenant { tenant: tenant.clone() };
    let begun = store
        .begin_upload(
            &caller,
            &crate::runtime_store::UploadSpec {
                scope: &StorageScope::Asset,
                mime: &req.mime_type,
                filename: &req.filename,
                keep: None,
                declared_size: req.declared_size,
                content_hash: Some(&req.content_hash),
                identity: None,
            },
            state.entitlements.as_ref(),
        )
        .await
        .map_err(map_err)?;
    Ok(Json(match begun {
        crate::runtime_store::BeginUpload::Ready { key, part_size } => {
            UploadBeginResponse { key, part_size, already_stored: false, resume: false }
        }
        crate::runtime_store::BeginUpload::AlreadyStored { key } => {
            UploadBeginResponse { key, part_size: 0, already_stored: true, resume: false }
        }
        crate::runtime_store::BeginUpload::Resume { key, part_size } => {
            UploadBeginResponse { key, part_size, already_stored: false, resume: true }
        }
    }))
}

async fn admin_asset_references(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<weft_core::storage::AssetReferencesRequest>>,
) -> Result<Json<weft_core::storage::AssetReferencesResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let missing = state.runtime_store
        .set_asset_references(&req.tenant, &req.inner.project, &req.inner.keys, &req.inner.kept)
        .await.map_err(map_err)?;
    Ok(Json(weft_core::storage::AssetReferencesResponse { missing }))
}

/// Which of the named contents the tenant stores whole: what a publish
/// skips uploading.
async fn admin_assets_held(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<AssetsHeldRequest>>,
) -> Result<Json<AssetsHeldResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let keys = state.runtime_store.held_assets(&req.tenant, &req.inner.hashes).await.map_err(map_err)?;
    Ok(Json(AssetsHeldResponse { keys }))
}

async fn admin_upload_parts(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<UploadPartsRequest>>,
) -> Result<Json<UploadPartsResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let caller = tenant_caller_for_key(&req.tenant, &req.inner.key)?;
    let parts = store
        .reserve_parts(
            &caller,
            &req.inner.key,
            &req.inner.parts,
            state.entitlements.as_ref(),
            PresignAudience::External,
        )
        .await
        .map_err(map_err)?;
    Ok(Json(UploadPartsResponse { parts }))
}

async fn admin_upload_part_done(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<PartDoneRequest>>,
) -> Result<Response, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let caller = tenant_caller_for_key(&req.tenant, &req.inner.key)?;
    store
        .record_part(&caller, &req.inner.key, req.inner.part_number, &req.inner.etag)
        .await
        .map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn admin_upload_complete(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<UploadCompleteRequest>>,
) -> Result<Response, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let caller = tenant_caller_for_key(&req.tenant, &req.inner.key)?;
    complete_response(store.complete_upload(&caller, &req.inner.key).await)
}

async fn admin_upload_resume(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<UploadResumeRequest>>,
) -> Result<Json<UploadResumeResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let caller = tenant_caller_for_key(&req.tenant, &req.inner.key)?;
    let (part_size, missing, reserved_bytes) = store
        .resume_upload(&caller, &req.inner.key, state.entitlements.as_ref(), PresignAudience::External)
        .await
        .map_err(map_err)?;
    Ok(Json(UploadResumeResponse { part_size, missing, reserved_bytes }))
}

async fn admin_upload_abort(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<Tenanted<UploadAbortRequest>>,
) -> Result<Response, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let caller = tenant_caller_for_key(&req.tenant, &req.inner.key)?;
    store.abort_upload(&caller, &req.inner.key).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
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
    let store = &state.runtime_store;
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
    let store = &state.runtime_store;
    let (file_count, stored_bytes) = store.tenant_usage(&req.tenant).await.map_err(map_anyhow)?;
    Ok(Json(TenantUsage { stored_bytes, file_count }))
}

async fn admin_delete_file(
    State(state): State<Arc<BrokerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    // The store takes a ParsedKey, so the control-plane's key passes the
    // wall's grammar here too (a key reaching the store is always a real one).
    let parsed = key::parse_key(&key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    store.delete(&parsed).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// GET /v1/storage/admin/relay/{token}: stream a minted public-relay
/// link's bytes. The dispatcher's public `/public/files/{token}` route
/// forwards here because its egress is locked to the control plane;
/// the broker is the one service with bucket reach, so it resolves the
/// token (missing and expired are an identical 404) and streams the
/// presigned internal fetch through.
async fn admin_relay(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    axum::extract::Path(token): axum::extract::Path<String>,
) -> Result<axum::response::Response, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let Some(link) = store
        .resolve_public_link(&token)
        .await
        .map_err(|e| map_err(RuntimeStoreError::Other(e)))?
    else {
        return Err(ApiError::from((StatusCode::NOT_FOUND, "no such file link".into())));
    };
    static RELAY_HTTP: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    let http = RELAY_HTTP.get_or_init(reqwest::Client::new);
    let upstream = http.get(&link.fetch_url).send().await.map_err(|e| {
        tracing::error!(target: "weft_broker::runtime_storage", error = %e, "relay bucket fetch failed");
        (StatusCode::BAD_GATEWAY, "file fetch failed".to_string())
    })?;
    if !upstream.status().is_success() {
        tracing::error!(
            target: "weft_broker::runtime_storage",
            status = %upstream.status(),
            "relay bucket fetch refused"
        );
        return Err(ApiError::from((StatusCode::BAD_GATEWAY, "file fetch failed".to_string())));
    }
    // content-length only when the bucket stated one for THIS response
    // (restating the row's size against a body someone else produced
    // would lie on a torn or replaced object; absent = chunked).
    // mime/filename passed validate_serveable at the upload boundary,
    // so both are header-safe as stored.
    let mut resp = axum::response::Response::builder()
        .header("content-type", link.mime_type)
        .header("content-disposition", format!("inline; filename=\"{}\"", link.filename));
    if let Some(len) = upstream.headers().get("content-length") {
        resp = resp.header("content-length", len);
    }
    resp.body(axum::body::Body::from_stream(upstream.bytes_stream())).map_err(|e| {
        tracing::error!(target: "weft_broker::runtime_storage", error = %e, "relay response build failed");
        ApiError::from((StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_STORAGE_ERROR_BODY.to_string()))
    })
}

async fn admin_presign(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::PresignRequest>,
) -> Result<Json<PresignResult>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let parsed = key::parse_key(&req.key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    // Read the meta first (name + size) so a missing file is a clean 404 before
    // minting, then presign (which also bumps a kept file's TTL).
    let meta = store.meta(&parsed).await.map_err(map_err)?;
    let url = store.presign(&parsed, req.ttl_secs).await.map_err(map_err)?;
    Ok(Json(PresignResult { url, filename: meta.filename, size_bytes: meta.size_bytes }))
}

/// Mint a relay download token for one file (the dispatcher builds the
/// `/public/files/{token}` URL on its own public base). The browser
/// lane of the download handshake: host-rewrite-proof where a presigned
/// bucket URL is not (its signature covers the exact host).
async fn admin_download_link(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<weft_core::storage::PresignRequest>,
) -> Result<Json<weft_core::storage::DownloadLinkResult>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let parsed = key::parse_key(&req.key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    // Meta first (name + size) so a missing file is a clean 404 before
    // minting, mirroring admin_presign.
    let meta = store.meta(&parsed).await.map_err(map_err)?;
    let token = store.mint_public_link(&parsed, req.ttl_secs).await.map_err(map_err)?;
    Ok(Json(weft_core::storage::DownloadLinkResult {
        token,
        filename: meta.filename,
        size_bytes: meta.size_bytes,
    }))
}

async fn admin_wipe_prefix(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<WipePrefixRequest>,
) -> Result<Json<WipePrefixResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
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
    let store = &state.runtime_store;
    let (swept, lingering) = store.sweep_exec(&req.tenant, &req.execution_id).await.map_err(map_anyhow)?;
    Ok(Json(SweepExecResponse { swept, lingering }))
}

#[cfg(test)]
mod link_route_tests {
    use super::{link_route, map_err, LinkRoute, RuntimeStoreError, StatusCode};

    #[test]
    fn unavailable_uploaded_file_explains_expiry_and_how_to_recover() {
        let key = format!("tenant/asset/{}", "a".repeat(64));
        let super::ApiError { status, message, .. } = map_err(RuntimeStoreError::NotFound(key.clone()));
        assert_eq!(status, StatusCode::NOT_FOUND);
        for text in [
            &key,
            "expired or been deleted",
            "30 days",
            "older run is still waiting",
            "Build and deploy the project again",
            "start a new run",
        ] {
            assert!(message.contains(text), "missing {text:?}: {message}");
        }
        // Keep File is refused on an asset, so it is not the recovery for
        // an uploaded file and must not be offered.
        assert!(!message.contains("Keep File"), "{message}");
    }

    /// Every verb that meets a claimed upload (parts, part-done, resume,
    /// complete, delete) answers the same marked 409.
    #[test]
    fn a_completing_refusal_carries_the_marker() {
        use axum::response::IntoResponse;
        let response = map_err(RuntimeStoreError::Completing("busy".into())).into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(response.headers().contains_key(weft_core::storage::COMPLETING_HEADER));
        let conflict = map_err(RuntimeStoreError::Conflict("no".into())).into_response();
        assert!(!conflict.headers().contains_key(weft_core::storage::COMPLETING_HEADER));
    }

    #[test]
    fn unavailable_generated_file_does_not_claim_a_fixed_thirty_day_lifetime() {
        let message = map_err(RuntimeStoreError::NotFound("tenant/exec/execution_id/file".into())).message;
        assert!(message.contains("keep duration"), "{message}");
        assert!(!message.contains("30 days"), "{message}");
    }

    /// A caller's link rides the address its request came in on, even
    /// when the install has an internet address; only a caller no request
    /// stands behind falls back to the configured ones. An internet asker
    /// never gets the loopback base.
    #[test]
    fn a_caller_link_rides_the_callers_own_address() {
        use weft_core::storage::LinkReach;
        let tunnel = Some("https://weft-dev.example.com");
        let local = "http://127.0.0.1:14111";
        let caller = LinkReach::Caller { base: Some(local.to_string()) };
        assert_eq!(super::relay_base(tunnel, "http://install", &caller).unwrap(), Some(local));
        let unbased = LinkReach::Caller { base: None };
        assert_eq!(super::relay_base(tunnel, "http://install", &unbased).unwrap(), tunnel);
        assert_eq!(super::relay_base(None, "http://install", &unbased).unwrap(), Some("http://install"));
        assert_eq!(super::relay_base(None, "http://install", &LinkReach::Internet).unwrap(), None);
        assert_eq!(super::relay_base(tunnel, "http://install", &LinkReach::Internet).unwrap(), tunnel);
    }

    /// A caller's own address is only ever a bare http(s) base: anything
    /// else is refused rather than turned into a link to nowhere.
    #[test]
    fn a_caller_base_is_a_bare_http_address() {
        for good in ["http://127.0.0.1:14111", "https://weft.example.com", "https://site.example/weft"] {
            assert_eq!(super::caller_base(good).unwrap(), good);
        }
        for bad in ["ftp://x.example", "not a url", "https://u:p@x.example", "https://x.example/?q=1", "https://x.example/#f"] {
            assert_eq!(super::caller_base(bad).unwrap_err().status, StatusCode::BAD_REQUEST, "{bad}");
        }
    }

    #[test]
    fn route_follows_the_configured_facts() {
        // Internet-declared bucket wins outright (even with a base up:
        // the direct URL is the no-hop path).
        assert_eq!(link_route(true, None), LinkRoute::DirectPresign);
        assert_eq!(link_route(true, Some("https://x.example")), LinkRoute::DirectPresign);
        // Private bucket + internet base: relay under the base.
        assert_eq!(link_route(false, Some("https://x.example")), LinkRoute::Relay("https://x.example"));
        // Neither: no internet link; a node body gets the install
        // address, an outside consumer the bytes.
        assert_eq!(link_route(false, None), LinkRoute::InstallOnly);
    }
}
