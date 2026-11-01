//! The broker's runtime-file HTTP surface (`ctx.storage`).
//!
//! Worker data path (bearer = the worker's platform identity token, resolved
//! in-process to `Worker { tenant, project, run_id }`). BYTES never transit the
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
//!   POST   /v1/storage/link              a link for node code or for the outside
//!   POST   /v1/storage/copy              a copy into a scope, inside the bucket
//!   POST   /v1/storage/upload-slot       a slot a page uploads one file through
//!   POST   /v1/storage/upload/claim      the file a slot uploaded, for this run
//!
//! Control-plane admin path (bearer = the dispatcher's SA -> ControlPlane;
//! the CLI `weft files` verbs proxy through the dispatcher to here):
//!   POST   /v1/storage/admin/tenant-list    one tenant's files
//!   POST   /v1/storage/admin/tenant-usage   one tenant's (count, bytes)
//!   DELETE /v1/storage/admin/files/{*key}   delete one file
//!   POST   /v1/storage/admin/link           a download link, for a person, weft or the outside
//!   POST   /v1/storage/admin/wipe-prefix    a removed project's files, `weft files rm --prefix`
//!   POST   /v1/storage/admin/delete-run-files  every file of a run whose record went
//!   GET    /v1/storage/admin/install-cli/{tree}  the weft CLI the install stored for a source tree
//!
//! A page's upload through a slot (bearer = the dispatcher, which passes
//! the page's verbs on from its `/files/upload/...` door; the slot in each
//! body is the page's credential, checked here):
//!   POST   /v1/storage/slot/{begin,parts,part-done,complete,resume,abort}
//!   POST   /v1/storage/slot/check     whether `begin` would take the file, reserving nothing
//!
//! The broker is the single gatekeeper: it verifies the caller, runs the pure
//! `key` wall, enforces quota, records metadata, and is the ONLY thing that signs
//! bucket requests. But it NEVER carries the bytes: every read/write is a presigned
//! URL the caller uses to hit the bucket directly, on a short expiry.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use serde::{Deserialize, Deserializer};

use weft_core::storage::key::{self, CallerAuth};
use weft_core::storage::slot::{
    ClaimUploadRequest, ClaimUploadResponse, SlotBeginRequest, Slotted, UploadSlotClaims, UploadSlotRequest, UploadSlotResponse,
};
use weft_core::storage::{
    AdminLinkRequest, AssetUploadBeginRequest, AssetsHeldRequest, AssetsHeldResponse, CopyRequest,
    DownloadUrlResponse, FileLink, LinkAnswer, LinkReach, LinkReader, LinkRequest, ListFilesResponse, PartDoneRequest,
    PresignResult, RunFilesRequest, RunFilesResponse, StorageScope, StoredFile, StoredFileMeta, Tenanted,
    TenantScopeRequest, TenantUsage, UploadAbortRequest, UploadBeginRequest, UploadBeginResponse,
    PartSender, UploadCompleteRequest, UploadPartsRequest, UploadPartsResponse, UploadResumeRequest,
    UploadResumeResponse, WipePrefixRequest, WipePrefixResponse,
};
use weft_platform_traits::PresignAudience;
use weft_task_store::db::Client as _;

use crate::auth::control_plane;
use crate::runtime_store::RuntimeStoreError;
use crate::state::BrokerState;
use weft_core::wire_shape::{Json, Query};

/// The run claim a worker stamps on every storage call, so the broker scopes
/// the op to that run. (The file's scope / mime / filename / keep travel
/// in the JSON body of upload/begin, not headers.)
// SYNC: HDR_RUN_ID <-> crates/weft-engine/src/storage.rs (HDR_RUN_ID)
pub const HDR_RUN_ID: &str = "x-weft-run-id";

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
        .route("/v1/storage/link", post(link))
        .route("/v1/storage/copy", post(copy_file))
        .route("/v1/storage/upload-slot", post(upload_slot))
        .route("/v1/storage/upload/claim", post(claim_upload))
        // A page's upload through a slot, passed on by the dispatcher's
        // `/files/upload/...` door: the four-step upload, held to the slot.
        .route("/v1/storage/slot/check", post(slot_check))
        .route("/v1/storage/slot/begin", post(slot_begin))
        .route("/v1/storage/slot/parts", post(slot_parts))
        .route("/v1/storage/slot/part-done", post(slot_part_done))
        .route("/v1/storage/slot/complete", post(slot_complete))
        .route("/v1/storage/slot/resume", post(slot_resume))
        .route("/v1/storage/slot/abort", post(slot_abort))
        // Admin upload: the dispatcher drives the SAME multipart contract on
        // behalf of a tenant's editor session (a file-drop config field).
        // Project-scoped only; part URLs are presigned for the browser-facing
        // endpoint (the editor PUTs bytes to the bucket directly).
        .route("/v1/storage/admin/upload/begin", post(admin_upload_begin))
        .route("/v1/storage/admin/import/begin", post(admin_import_begin))
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
        .route("/v1/storage/admin/install-cli/{tree}", get(admin_install_cli))
        .route("/v1/storage/admin/link", post(admin_link))
        .route("/v1/storage/admin/wipe-prefix", post(admin_wipe_prefix))
        .route("/v1/storage/admin/delete-run-files", post(admin_delete_run_files))
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
        // The request was understood and its bytes are wrong for it: the
        // same request can never succeed, and the upload is already ended.
        RuntimeStoreError::NotItsContent(m) => (StatusCode::UNPROCESSABLE_ENTITY, m),
        RuntimeStoreError::NoUpload(m) => (StatusCode::NOT_FOUND, m),
        // The bucket refused rows, named in the message and marked; waiting
        // does not clear it, so it is a plain conflict (a wipe left waiting on
        // a live claim answers `Completing` instead, the "ask again" one).
        RuntimeStoreError::RemovalIncomplete(m) => (StatusCode::CONFLICT, m),
        // SYNC: replace outcome statuses <-> crates/weft-engine/src/storage.rs WorkerStorage::replace
        RuntimeStoreError::Stale(m) => (StatusCode::PRECONDITION_FAILED, m),
        // A database that could not be reached is said as one (logged with
        // its cause), and "nothing was done" only when the store failed
        // before its first write, so the caller waits and sends the call
        // again; past a write, asking again is still the fix, without that
        // promise. Anything else is the generic body.
        RuntimeStoreError::BeforeWrites(e) => {
            opaque(crate::handlers::unavailable_or_internal(e.context("runtime-store op failed")))
        }
        RuntimeStoreError::Other(e) => {
            opaque(crate::handlers::unavailable_after_writes_or_internal(e.context("runtime-store op failed")))
        }
    };
    ApiError { status, message, completing }
}

/// A failed database call's answer, with a 500's body replaced by
/// [`INTERNAL_STORAGE_ERROR_BODY`] (the cause is in the log).
fn opaque(answer: (StatusCode, String)) -> (StatusCode, String) {
    match answer {
        (StatusCode::INTERNAL_SERVER_ERROR, _) => (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_STORAGE_ERROR_BODY.to_string()),
        answer => answer,
    }
}

fn unavailable_file_message(key: &str) -> String {
    let scope = key::parse_key(key).map(|p| p.scope);
    // The row is gone with the file, so what removed it is not on record:
    // say what can remove a file of this scope, and how to make one last.
    // An asset is uploaded by the build from the project's own source, so
    // rebuilding puts it back and clears its countdown; KeepFile is for a
    // file a NODE made, and the store refuses it on an asset.
    let days = crate::runtime_store::DEFAULT_KEEP_TTL_SECS / 86400;
    match scope {
        Ok(key::KeyScope::Asset) => format!(
            "File '{key}' is no longer available: it may have expired or been deleted. \
             A file the project uploads expires after {days} days once nothing in the \
             workflow references it any more, even if an older run is still waiting. \
             Build and deploy the project again to upload it back, then start a new run."
        ),
        Ok(key::KeyScope::Run { .. }) => format!(
            "File '{key}' is no longer available: it was deleted with its run (a run's files go \
             when its record does: its keep time ran out, `weft clean`, `weft rm`), at its \
             deadline, or by hand. To make a file last, keep it in the project (a Keep File node \
             with `scope: project`, or a storage node with `scope: project`), or keep its runs \
             longer (`keepRunsFor`, `weft run --keep-for`)."
        ),
        Ok(key::KeyScope::Project { .. } | key::KeyScope::Instance { .. }) => format!(
            "File '{key}' is no longer available: it was deleted with its project (`weft rm`), \
             at its deadline, or by hand. A file with no deadline lives as long as its project \
             (a Keep File node with `ttlDays: 0` takes its deadline away)."
        ),
        Ok(key::KeyScope::Shared { .. }) => format!(
            "File '{key}' is no longer available: it was deleted by hand or at its deadline. \
             A file with no deadline lives until somebody deletes it (a Keep File node with \
             `ttlDays: 0` takes its deadline away)."
        ),
        // Every `NotFound` carries the key of the file it looked for
        // (an upload no longer in flight is `NoUpload`), so a key that does
        // not read is a store bug: said plainly, with no made-up cause.
        Err(_) => format!("File '{key}' is no longer available."),
    }
}

// ---------- caller resolution ----------

/// Resolve the worker caller for a data-path request (the `x-weft-run_id`
/// header carries the optional run claim). Rejects a
/// control-plane caller on the data path (it uses the admin surface).
async fn worker_caller(state: &Arc<BrokerState>, headers: &HeaderMap) -> Result<CallerAuth, ApiError> {
    let run_id = headers.get(HDR_RUN_ID).and_then(|v| v.to_str().ok());
    let caller = crate::auth::resolve_storage_caller(state, headers, run_id).await?;
    match &caller {
        CallerAuth::Worker { .. } => Ok(caller),
        CallerAuth::ControlPlane | CallerAuth::Tenant { .. } | CallerAuth::Copying { .. } => Err(ApiError::from((
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
             run/project/shared scopes"
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
                slot: None,
                imported: None,
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
    callers_own_parts(req.sender)?;
    let store = &state.runtime_store;
    let parts = store
        .reserve_parts(
            &caller,
            &req.key,
            &req.parts,
            state.entitlements.as_ref(),
            &PresignAudience::Install,
        )
        .await
        .map_err(map_err)?;
    Ok(Json(UploadPartsResponse { parts }))
}

/// Refuse part URLs for weft's own process on a route a worker or a page
/// calls: only the control plane uploads as the runtime.
fn callers_own_parts(sender: PartSender) -> Result<(), ApiError> {
    match sender {
        PartSender::Caller => Ok(()),
        PartSender::Runtime => Err(ApiError::from((
            StatusCode::FORBIDDEN,
            "denied: only the control plane uploads parts as weft's own process".into(),
        ))),
    }
}

/// The audience the control plane's part URLs are presigned for: a
/// person's machine it fronts, or weft's own process.
fn admin_parts_audience(sender: PartSender) -> PresignAudience {
    match sender {
        PartSender::Caller => PresignAudience::External,
        PartSender::Runtime => PresignAudience::Install,
    }
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
    callers_own_parts(req.sender)?;
    let store = &state.runtime_store;
    let (part_size, missing, reserved_bytes) = store
        .resume_upload(&caller, &req.key, state.entitlements.as_ref(), &PresignAudience::Install)
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
) -> Result<Json<DownloadUrlResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let parsed = wall(&caller, &key)?;
    let (meta, url, _) = state
        .runtime_store
        .link(&parsed, PresignAudience::Install, None, false)
        .await
        .map_err(map_err)?;
    Ok(Json(DownloadUrlResponse { meta, url }))
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
    /// Sent JSON-encoded (`weft_engine::storage`'s list), read as part of
    /// the query: a scope of another shape is a query of another shape.
    #[serde(deserialize_with = "json_encoded")]
    scope: StorageScope,
}

/// A query value that carries JSON, read as the `T` it encodes.
fn json_encoded<'de, D: Deserializer<'de>, T: serde::de::DeserializeOwned>(d: D) -> Result<T, D::Error> {
    serde_json::from_str(&String::deserialize(d)?).map_err(serde::de::Error::custom)
}


async fn list_files(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Query(q): Query<ScopeQuery>,
) -> Result<Json<ListFilesResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let store = &state.runtime_store;
    let prefix = key::prefix_for_list(&caller, &q.scope).map_err(|e| (StatusCode::FORBIDDEN, e))?;
    let files = store.list(&prefix).await.map_err(map_err)?;
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

/// `POST /v1/storage/link`: a link to one of the caller's files for node
/// code (`internal`, straight to the bucket at the workers' address), for
/// the project's containers (`container`) or for the outside
/// (`external`), as [`link_route`] routes it, whether the open internet
/// opens it, and the file it opens. An outside link reads the
/// project's visitor address in the same statement that reads the file.
async fn link(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<LinkRequest>,
) -> Result<Json<LinkAnswer>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let parsed = wall(&caller, &req.key)?;
    let visitors_of = match req.reach {
        LinkReach::External => Some(caller_project(&caller)?),
        LinkReach::Internal | LinkReach::Container => None,
    };
    let granted = state.runtime_store.grant_link(&parsed, req.ttl_secs, true, visitors_of).await.map_err(map_err)?;
    let (audience, public) = link_route(&state, req.reach, granted.visitor_address.clone()).await?;
    let url = state.runtime_store.sign_link(&granted, audience).await.map_err(map_err)?;
    Ok(Json(LinkAnswer { link: FileLink { url, public }, file: granted.file }))
}

/// The project a worker caller acts for.
fn caller_project(caller: &CallerAuth) -> Result<uuid::Uuid, ApiError> {
    match caller {
        CallerAuth::Worker { project_id, .. } => project_uuid(project_id),
        _ => Err(ApiError::from((StatusCode::FORBIDDEN, "only a project's own callers ask for its links".into()))),
    }
}

fn project_uuid(project: &str) -> Result<uuid::Uuid, ApiError> {
    project.parse().map_err(|_| ApiError::from((StatusCode::BAD_REQUEST, format!("'{project}' is not a project id"))))
}

/// [`route_link`] with this install's store, its containers' address,
/// own address and internet address.
async fn link_route(state: &BrokerState, reach: LinkReach, visitor: Option<String>) -> Result<(PresignAudience, bool), ApiError> {
    route_link(
        reach,
        state.object_store_public_internet,
        state.container_files_base.as_deref(),
        visitor,
        &state.own_base_url,
        || state.internet_base(),
    )
    .await
}

/// The audience of a page's upload parts through a slot of `project`:
/// the way an outside link of the project goes. The visitor address is
/// read only when the store does not take the page's bytes itself.
async fn slot_parts_audience(state: &BrokerState, project: &str) -> Result<PresignAudience, ApiError> {
    let visitor = match state.object_store_public_internet {
        true => None,
        false => visitor_address(state, project_uuid(project)?).await?,
    };
    Ok(link_route(state, LinkReach::External, visitor).await?.0)
}

/// The address `project` declared its visitors reach the install at (a
/// site that passes `/weft/...` on), when it declared one.
async fn visitor_address(state: &BrokerState, project: uuid::Uuid) -> Result<Option<String>, ApiError> {
    state
        .pool
        .scalar_opt("SELECT visitor_address FROM project_frontend WHERE project_id = $1 AND visitor_address IS NOT NULL", &[&project])
        .await
        // A read, ahead of every write of the call that asks it.
        .map_err(|e| opaque(crate::handlers::unavailable_or_internal(anyhow::Error::from(e).context("read the project's visitor address"))).into())
}

/// Where a link goes, and whether the open internet opens it. Node code
/// (`internal`) reaches the store at the workers' address, which the
/// internet opens only on a store it reaches (`store_public`, Cloud
/// Storage). The project's containers (`container`) reach it through the
/// files door at the install's address they reach (`containers`, a
/// local install's internal port as a container reaches it), which only
/// the machine's containers open; with none, they reach the store where
/// the workers do, and the link is the internal one. For the outside
/// (`external`): the store's own link when the internet reaches it; else through the install's forwarder at the
/// project's `visitor` address, else at the install's internet address
/// (`internet`, asked only when no visitor address applies: a quick
/// tunnel's is a call to cloudflared), both public; else at the install's
/// `own` address, which only its machine or its network opens. An
/// internet address that cannot be read refuses the link: one through a
/// tunnel that is down would work for nobody.
async fn route_link<F, Fut>(
    reach: LinkReach,
    store_public: bool,
    containers: Option<&str>,
    visitor: Option<String>,
    own: &str,
    internet: F,
) -> Result<(PresignAudience, bool), ApiError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<String>>>,
{
    match (reach, containers) {
        (LinkReach::Container, Some(base)) => return Ok((PresignAudience::Forwarded { base: base.to_string() }, false)),
        (LinkReach::Internal | LinkReach::Container, _) => return Ok((PresignAudience::Install, store_public)),
        (LinkReach::External, _) => {}
    }
    if store_public {
        return Ok((PresignAudience::External, true));
    }
    if let Some(base) = visitor {
        return Ok((PresignAudience::Forwarded { base }, true));
    }
    let internet = internet().await.map_err(|e| {
        tracing::error!(target: "weft_broker::runtime_storage", error = format!("{e:#}"), "the install's internet address could not be read");
        ApiError::from((
            StatusCode::SERVICE_UNAVAILABLE,
            "the install's tunnel does not say its address right now; `weft daemon start` opens it again".to_string(),
        ))
    })?;
    Ok(match internet {
        Some(base) => (PresignAudience::Forwarded { base }, true),
        None => (PresignAudience::Forwarded { base: own.to_string() }, false),
    })
}

/// `POST /v1/storage/copy`: copy one of the caller's files into a scope
/// it writes, inside the bucket.
async fn copy_file(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<CopyRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let from = wall(&caller, &req.key)?;
    let meta = state
        .runtime_store
        .copy_file(&caller, &from, &req.scope, req.identity.as_deref(), req.keep, state.entitlements.as_ref())
        .await
        .map_err(map_err)?;
    Ok(Json(StoredFile::from(&meta).to_value()))
}

/// `POST /v1/storage/upload-slot`: a slot the caller's page uploads one
/// file through, signed for the caller's project. A slot into the run's
/// own instance names that instance.
async fn upload_slot(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<UploadSlotRequest>,
) -> Result<Json<UploadSlotResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    req.validate().map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let CallerAuth::Worker { tenant, project_id, instance, .. } = &caller else {
        return Err(ApiError::from((StatusCode::FORBIDDEN, "a slot is asked by a project's run".into())));
    };
    let scope = match req.scope {
        StorageScope::Instance { of: None } => {
            let instance = instance.as_deref().ok_or_else(|| {
                ApiError::from((StatusCode::BAD_REQUEST, "this run is for no instance, so a slot into its instance names one".to_string()))
            })?;
            let instance = weft_core::instance::InstanceId::new(instance).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
            StorageScope::instance_of(instance)
        }
        scope => scope,
    };
    let claims = UploadSlotClaims {
        sid: uuid::Uuid::new_v4().to_string(),
        tenant: tenant.clone(),
        project: project_id.clone(),
        scope,
        max_bytes: req.max_bytes,
        accepts: req.accepts,
        exp: now_unix() + (req.minutes * 60) as i64,
        keep: req.keep,
    };
    Ok(Json(UploadSlotResponse { slot: state.slot_key.mint(&claims) }))
}

/// `POST /v1/storage/upload/claim`: the files pages uploaded through
/// slots of the caller's project, received by the caller's run, each into
/// a place of the type its claim names, all of them or none. Every refusal
/// a slot or its upload meets is the run's caller's to fix (they sent the
/// slot), so it is answered in the slot's own words, naming which slot of
/// the call, with a status the engine hands back as their input error: a
/// slot that does not read 400, one whose time is over 410, a file that
/// does not fit 400, nothing uploaded 404, received by another run or made
/// for another project 409. Only the worker's own credentials meet
/// 401/403.
// SYNC: claim refusal statuses <-> crates/weft-engine/src/storage.rs is_slot_refusal
async fn claim_upload(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<ClaimUploadRequest>,
) -> Result<Json<ClaimUploadResponse>, ApiError> {
    let caller = worker_caller(&state, &headers).await?;
    let now = now_unix();
    let read = req
        .claims
        .iter()
        .map(|claim| {
            let named = |why: String| crate::runtime_store::of_slot_message(&claim.slot, &why);
            let claims = state.slot_key.read(&claim.slot, now).map_err(|why| {
                // A slot that reads at any moment is a real one whose time is over.
                let ended = state.slot_key.read(&claim.slot, i64::MIN).is_ok();
                ApiError::from((if ended { StatusCode::GONE } else { StatusCode::BAD_REQUEST }, named(why)))
            })?;
            let fits = received_into(&claim.fits).map_err(|why| (StatusCode::BAD_REQUEST, named(why)))?;
            Ok((claims, fits))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let slots: Vec<_> = req
        .claims
        .iter()
        .zip(&read)
        .map(|(claim, (claims, fits))| crate::runtime_store::ClaimedSlot { slot: &claim.slot, claims, fits })
        .collect();
    let received = state.runtime_store.claim_uploads(&caller, &slots).await.map_err(map_err)?;
    Ok(Json(ClaimUploadResponse { files: received.iter().map(|meta| StoredFile::from(meta).to_value()).collect() }))
}

/// The type of the place a received upload is sent to (`Image`, `Video |
/// Null`, as the engine writes it), which must take a file.
fn received_into(fits: &str) -> Result<weft_core::weft_type::WeftType, String> {
    let ty = weft_core::weft_type::WeftType::try_parse(fits).map_err(|why| format!("'{fits}' is not a type: {why}"))?;
    if ty.admitted_file_kinds().is_empty() {
        return Err(format!("an upload is a file, and a place of type {fits} takes none"));
    }
    Ok(ty)
}

/// Seconds since the unix epoch, as the store's rows count them.
fn now_unix() -> i64 {
    crate::handlers::unix_now_secs() as i64
}

// ---------- a page's upload through a slot ----------
//
// The dispatcher passes a page's verbs on (it is the caller, as the
// control plane); the slot in each body is the page's own credential. Each
// verb acts as the slot's project, on the slot's one upload and nothing
// else of the project's.

/// The slot a page's verb names, read, and the caller its upload is made
/// as: the slot's project (and instance), with no run.
async fn slot_caller(state: &Arc<BrokerState>, headers: &HeaderMap, slot: &str) -> Result<(UploadSlotClaims, CallerAuth), ApiError> {
    control_plane(state, headers).await?;
    let claims = state.slot_key.read(slot, now_unix()).map_err(|why| (StatusCode::FORBIDDEN, why))?;
    let instance = match &claims.scope {
        StorageScope::Instance { of } => of.as_ref().map(|i| i.as_str().to_string()),
        _ => None,
    };
    let caller = CallerAuth::Worker { tenant: claims.tenant.clone(), project_id: claims.project.clone(), run_id: None, instance };
    Ok((claims, caller))
}

/// [`slot_caller`] for a verb on an upload already begun, held to the
/// slot's own upload.
async fn slot_upload(state: &Arc<BrokerState>, headers: &HeaderMap, slot: &str, key: &str) -> Result<(UploadSlotClaims, CallerAuth), ApiError> {
    let (claims, caller) = slot_caller(state, headers, slot).await?;
    if !state.runtime_store.is_slot_upload(key, &claims.sid).await.map_err(map_err)? {
        return Err(ApiError::from((StatusCode::FORBIDDEN, "this is not the slot's upload".into())));
    }
    Ok((claims, caller))
}

/// A page's begin, read and held to its slot: the stated type against
/// the slot's type, the name and type as serveable (the store holds the
/// size to the slot's largest). What `begin` reserves and `check` asks
/// about, so the two refuse alike.
struct SlotBegin {
    claims: UploadSlotClaims,
    caller: CallerAuth,
    mime: String,
    /// Where the upload waits: a run-scoped slot's waits in the project
    /// until the run that receives it takes it into its own space.
    scope: StorageScope,
    filename: String,
    size_bytes: u64,
    /// The sha256 the page states for the file, when it hashed it.
    sha256: Option<String>,
}

impl SlotBegin {
    async fn read(state: &Arc<BrokerState>, headers: &HeaderMap, slot: &str, req: SlotBeginRequest) -> Result<Self, ApiError> {
        let (claims, caller) = slot_caller(state, headers, slot).await?;
        let accepts = weft_core::storage::slot::accepted_type(&claims.accepts).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
        let mime = match req.mime_type.trim() {
            "" => "application/octet-stream".to_string(),
            stated => stated.to_string(),
        };
        let stated = weft_core::weft_type::FileKind::from_mime(&mime);
        let admitted = accepts.admitted_file_kinds();
        if stated != weft_core::weft_type::FileKind::Blob && !admitted.contains(&stated) && !admitted.contains(&weft_core::weft_type::FileKind::Blob) {
            return Err(ApiError::from((StatusCode::BAD_REQUEST, format!("a {mime} file is not what this upload takes ({accepts})"))));
        }
        validate_serveable(&mime, &req.filename)?;
        let scope = match &claims.scope {
            StorageScope::Run => StorageScope::Project,
            scope => scope.clone(),
        };
        Ok(Self { claims, caller, mime, scope, filename: req.filename, size_bytes: req.size_bytes, sha256: req.sha256 })
    }

    fn spec(&self) -> crate::runtime_store::UploadSpec<'_> {
        crate::runtime_store::UploadSpec {
            scope: &self.scope,
            mime: &self.mime,
            filename: &self.filename,
            keep: None,
            declared_size: Some(self.size_bytes),
            content_hash: None,
            identity: None,
            slot: Some(crate::runtime_store::SlotUpload {
                sid: &self.claims.sid,
                ends_unix: self.claims.exp,
                max_bytes: self.claims.max_bytes,
                accepts: &self.claims.accepts,
                sha256: self.sha256.as_deref(),
            }),
            imported: None,
        }
    }
}

/// `POST /v1/storage/slot/check`: 204 when `begin` would take this file,
/// else the very refusal `begin` gives, reserving nothing: what a page
/// asks before it hashes a file it may not send, and so names no sha256.
async fn slot_check(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Slotted { slot, inner: req }): Json<Slotted<SlotBeginRequest>>,
) -> Result<Response, ApiError> {
    let begin = SlotBegin::read(&state, &headers, &slot, req).await?;
    state.runtime_store.check_slot_upload(&begin.caller, &begin.spec(), state.entitlements.as_ref()).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /v1/storage/slot/begin`: begin the slot's one upload (or carry on
/// with it), held to the slot's size and type.
async fn slot_begin(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Slotted { slot, inner: req }): Json<Slotted<SlotBeginRequest>>,
) -> Result<Json<UploadBeginResponse>, ApiError> {
    let begin = SlotBegin::read(&state, &headers, &slot, req).await?;
    let begun = state
        .runtime_store
        .begin_slot_upload(&begin.caller, &begin.spec(), state.entitlements.as_ref())
        .await
        .map_err(map_err)?;
    Ok(Json(match begun {
        crate::runtime_store::BeginUpload::Ready { key, part_size } => UploadBeginResponse { key, part_size, already_stored: false, resume: false },
        crate::runtime_store::BeginUpload::Resume { key, part_size } => UploadBeginResponse { key, part_size, already_stored: false, resume: true },
        crate::runtime_store::BeginUpload::AlreadyStored { key } => {
            UploadBeginResponse { key, part_size: 0, already_stored: true, resume: false }
        }
    }))
}

async fn slot_parts(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Slotted { slot, inner: req }): Json<Slotted<UploadPartsRequest>>,
) -> Result<Json<UploadPartsResponse>, ApiError> {
    let (claims, caller) = slot_upload(&state, &headers, &slot, &req.key).await?;
    callers_own_parts(req.sender)?;
    let audience = slot_parts_audience(&state, &claims.project).await?;
    let parts = state
        .runtime_store
        .reserve_parts(&caller, &req.key, &req.parts, state.entitlements.as_ref(), &audience)
        .await
        .map_err(map_err)?;
    Ok(Json(UploadPartsResponse { parts }))
}

async fn slot_part_done(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Slotted { slot, inner: req }): Json<Slotted<PartDoneRequest>>,
) -> Result<Response, ApiError> {
    let (_, caller) = slot_upload(&state, &headers, &slot, &req.key).await?;
    state.runtime_store.record_part(&caller, &req.key, req.part_number, &req.etag).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /v1/storage/slot/complete`: land the slot's upload (held to the
/// slot's type as it lands), and read it back for its hash once landed.
async fn slot_complete(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Slotted { slot, inner: req }): Json<Slotted<UploadCompleteRequest>>,
) -> Result<Response, ApiError> {
    let (_, caller) = slot_upload(&state, &headers, &slot, &req.key).await?;
    state.runtime_store.complete_upload(&caller, &req.key).await.map_err(map_err)?;
    // Read back after the answer: the page waits for nothing, and a hash
    // that never lands only means this content is uploaded in full the
    // next time too.
    let store = state.runtime_store.clone();
    let key = req.key.clone();
    tokio::spawn(async move {
        if let Err(e) = store.hash_upload(&key).await {
            tracing::warn!(target: "weft_broker::runtime_storage", key = %key, error = format!("{e:#}"), "an upload could not be read back for its hash");
        }
    });
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn slot_resume(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Slotted { slot, inner: req }): Json<Slotted<UploadResumeRequest>>,
) -> Result<Json<UploadResumeResponse>, ApiError> {
    let (claims, caller) = slot_upload(&state, &headers, &slot, &req.key).await?;
    callers_own_parts(req.sender)?;
    let audience = slot_parts_audience(&state, &claims.project).await?;
    let (part_size, missing, reserved_bytes) = state
        .runtime_store
        .resume_upload(&caller, &req.key, state.entitlements.as_ref(), &audience)
        .await
        .map_err(map_err)?;
    Ok(Json(UploadResumeResponse { part_size, missing, reserved_bytes }))
}

async fn slot_abort(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Slotted { slot, inner: req }): Json<Slotted<UploadAbortRequest>>,
) -> Result<Response, ApiError> {
    let (_, caller) = slot_upload(&state, &headers, &slot, &req.key).await?;
    state.runtime_store.abort_upload(&caller, &req.key).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
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
// URLs' audience (External for the person's machine it fronts, whose browser
// or CLI PUTs to the bucket directly; Runtime when weft's own process sends
// the parts, `PartSender::Runtime`).

/// Re-derive the acting caller for a key-addressed admin upload verb: the key
/// must belong to the vouched tenant. An asset key is the tenant's asset
/// plane; any other key gets the copying caller, which the admin surface
/// begins only for a file copied in from another install, and which
/// reaches every key of the tenant: these routes act for the tenant's
/// operator, who may already remove any of its files. Returns the caller
/// whose walls (`check_key_access`) then hold for the store call.
fn tenant_caller_for_key(tenant: &str, k: &str) -> Result<CallerAuth, ApiError> {
    let parsed = key::parse_key(k).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if parsed.tenant != tenant {
        return Err(ApiError::from((
            StatusCode::FORBIDDEN,
            "denied: key does not belong to the acting tenant".into(),
        )));
    }
    Ok(match parsed.scope {
        key::KeyScope::Asset => CallerAuth::Tenant { tenant: tenant.to_string() },
        _ => CallerAuth::Copying { tenant: tenant.to_string() },
    })
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
                slot: None,
                imported: None,
            },
            state.entitlements.as_ref(),
        )
        .await
        .map_err(map_err)?;
    Ok(Json(begun_answer(begun)))
}

/// Begin copying one file in from another install, under the key it had
/// there with the acting tenant in it (`weft migrate files`). The same
/// copy begun again answers the file already here, or carries on with the
/// upload part way up; anything else under the key is a conflict.
// SYNC: admin_import_begin <-> crates/weft-core/src/storage/mod.rs FileImportBeginRequest
async fn admin_import_begin(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(Tenanted { tenant, inner: req }): Json<Tenanted<weft_core::storage::FileImportBeginRequest>>,
) -> Result<Json<UploadBeginResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    validate_serveable(&req.mime_type, &req.filename)?;
    let from = key::parse_key(&req.key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let here = key::ParsedKey { tenant: tenant.clone(), ..from };
    let scope = here.scope.storage_scope().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if scope == StorageScope::Asset {
        return Err(ApiError::from((
            StatusCode::BAD_REQUEST,
            "an asset is copied through the asset upload, which finds it by its content".into(),
        )));
    }
    let begun = state
        .runtime_store
        .begin_upload(
            &CallerAuth::Copying { tenant },
            &crate::runtime_store::UploadSpec {
                scope: &scope,
                mime: &req.mime_type,
                filename: &req.filename,
                keep: req.keep,
                declared_size: Some(req.size_bytes),
                content_hash: None,
                identity: None,
                slot: None,
                imported: Some(crate::runtime_store::ImportedFile { key: &here, version: req.version, created_at_unix: req.created_at_unix }),
            },
            state.entitlements.as_ref(),
        )
        .await
        .map_err(map_err)?;
    Ok(Json(begun_answer(begun)))
}

/// A begin as the admin surface answers it.
fn begun_answer(begun: crate::runtime_store::BeginUpload) -> UploadBeginResponse {
    match begun {
        crate::runtime_store::BeginUpload::Ready { key, part_size } => UploadBeginResponse { key, part_size, already_stored: false, resume: false },
        crate::runtime_store::BeginUpload::AlreadyStored { key } => UploadBeginResponse { key, part_size: 0, already_stored: true, resume: false },
        crate::runtime_store::BeginUpload::Resume { key, part_size } => UploadBeginResponse { key, part_size, already_stored: false, resume: true },
    }
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
    let mut unheld = std::collections::BTreeMap::new();
    for hash in req.inner.hashes.iter().filter(|hash| !keys.contains_key(*hash)) {
        let parsed = key::ParsedKey::asset(&req.tenant, hash).map_err(|e| ApiError::from((StatusCode::BAD_REQUEST, e)))?;
        unheld.insert(hash.clone(), parsed.to_key());
    }
    Ok(Json(AssetsHeldResponse { keys, unheld }))
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
            &admin_parts_audience(req.inner.sender),
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
        .resume_upload(&caller, &req.inner.key, state.entitlements.as_ref(), &admin_parts_audience(req.inner.sender))
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
    let files = store.list(&prefix).await.map_err(map_err)?;
    Ok(Json(ListFilesResponse { files }))
}

async fn admin_tenant_usage(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<TenantScopeRequest>,
) -> Result<Json<TenantUsage>, ApiError> {
    control_plane(&state, &headers).await?;
    let store = &state.runtime_store;
    let (file_count, stored_bytes) = store.tenant_usage(&req.tenant).await.map_err(map_err)?;
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

/// `POST /v1/storage/admin/link`: a download link for one file, with its
/// name, size and version, and whether the open internet opens it: for a
/// person's machine (the CLI, the editor), for weft's own process (a
/// version build reading its files), or for somebody outside reaching the
/// install as a project's callers do. An outside link's file is checked
/// to be one that project's callers reach ([`outside_reader`]), so a
/// dispatcher bug can still never hand out another tenant's file.
/// How long a link to the install's stored CLI works: long enough for a
/// deploy to download it on a slow runner.
const INSTALL_CLI_LINK_SECS: u64 = 3600;

/// `GET /v1/storage/admin/install-cli/{tree}`: a link to the weft CLI the
/// install workflow stored for the source tree `tree`, with its sha256;
/// 404 when it stored none. The install's own file, beside every tenant's
/// and no tenant's, which the workflow writes and replaces, so nothing of
/// it is recorded here.
async fn admin_install_cli(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Path(tree): Path<String>,
) -> Result<Json<weft_core::install::InstallCli>, ApiError> {
    control_plane(&state, &headers).await?;
    if !weft_core::install::is_tree_id(&tree) {
        return Err(ApiError::from((StatusCode::BAD_REQUEST, format!("'{tree}' is not a git tree id (40 or 64 lowercase hex digits)"))));
    }
    // A read and a signature: nothing is written, so a failure is one
    // asking again may clear.
    let failed = |e: anyhow::Error| map_err(RuntimeStoreError::BeforeWrites(e.context("read the stored weft CLI")));
    let (cli, sha256) = weft_core::install::install_cli_keys(&tree);
    let Some(sha256) = state.object_store.get(&sha256).await.map_err(failed)? else {
        return Err(ApiError::from((StatusCode::NOT_FOUND, format!("no weft CLI is stored for the source tree {tree}"))));
    };
    let link = weft_platform_traits::GetLink {
        audience: PresignAudience::External,
        signed_at: std::time::SystemTime::now(),
        ttl_secs: INSTALL_CLI_LINK_SECS,
        content_type: Some("application/octet-stream".into()),
        disposition: None,
    };
    let url = state.object_store.presign_get(&cli, &link).await.map_err(failed)?;
    Ok(Json(weft_core::install::InstallCli { url, sha256: String::from_utf8_lossy(&sha256).trim().to_string() }))
}

async fn admin_link(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<AdminLinkRequest>,
) -> Result<Json<PresignResult>, ApiError> {
    control_plane(&state, &headers).await?;
    let parsed = key::parse_key(&req.key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let (visitors_of, hands_out) = match &req.reader {
        LinkReader::Person => (None, true),
        LinkReader::Runtime => (None, false),
        LinkReader::Outside { project } => (Some(outside_reader(&state, project, &parsed).await?), true),
    };
    let granted = state.runtime_store.grant_link(&parsed, req.ttl_secs, hands_out, visitors_of).await.map_err(map_err)?;
    // A person's and weft's own links are the store's, which the internet
    // opens when it reaches the store.
    let (audience, public) = match &req.reader {
        LinkReader::Person => (PresignAudience::External, state.object_store_public_internet),
        LinkReader::Runtime => (PresignAudience::Install, state.object_store_public_internet),
        LinkReader::Outside { .. } => link_route(&state, LinkReach::External, granted.visitor_address.clone()).await?,
    };
    let url = state.runtime_store.sign_link(&granted, audience).await.map_err(map_err)?;
    let file = granted.file;
    Ok(Json(PresignResult { url, filename: file.filename, size_bytes: file.size_bytes, version: file.version, public }))
}

/// The project an outside link is for, once the file at `parsed` is
/// checked to be one its callers reach: a file of the project's tenant,
/// the one wall a worker of the project meets.
async fn outside_reader(state: &BrokerState, project: &str, parsed: &key::ParsedKey) -> Result<uuid::Uuid, ApiError> {
    let project = project_uuid(project)?;
    let tenant = crate::scope::lookup_project_tenant(&state.scope_cache, &state.pool, project).await?;
    let reader = CallerAuth::Worker { tenant, project_id: project.to_string(), run_id: None, instance: None };
    key::check_key_access(&reader, parsed).map_err(|e| (StatusCode::FORBIDDEN, e))?;
    Ok(project)
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
    let wiped = store.wipe_prefix(&req.prefix).await.map_err(map_err)?;
    Ok(Json(WipePrefixResponse { wiped }))
}

async fn admin_delete_run_files(
    State(state): State<Arc<BrokerState>>,
    headers: HeaderMap,
    Json(req): Json<RunFilesRequest>,
) -> Result<Json<RunFilesResponse>, ApiError> {
    control_plane(&state, &headers).await?;
    let deleted = state.runtime_store.delete_run_files(&req.tenant, &req.run_ids).await.map_err(map_err)?;
    Ok(Json(RunFilesResponse { deleted }))
}

#[cfg(test)]
mod link_route_tests {
    use super::{map_err, route_link, LinkReach, PresignAudience, RuntimeStoreError, StatusCode};
    use weft_platform_traits::object_store::{fake::FakeObjectStore, GetLink, ObjectStore};
    use weft_task_store::db;

    /// What a statement that got no connection fails with.
    fn no_connection() -> db::Error {
        db::Error::NoConnection { why: db::NoConnection::Busy, refused: None }
    }

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

    /// A file that is gone says what can have deleted a file of its scope
    /// (its run, its project, its deadline, a person) and how to make one
    /// last.
    #[test]
    fn a_gone_file_says_what_deletes_its_scope_and_how_to_make_it_last() {
        let id = uuid::Uuid::nil();
        let gone = |key: &str| map_err(RuntimeStoreError::NotFound(key.into())).message;
        let run = gone(&format!("t/run/{id}/f"));
        for text in ["deleted with its run", "its keep time ran out", "at its", "deadline", "by hand", "`scope: project`", "--keep-for"] {
            assert!(run.contains(text), "missing {text:?}: {run}");
        }
        let project = gone(&format!("t/project/{id}/f"));
        for text in ["deleted with its project", "`weft rm`", "deadline", "by hand", "`ttlDays: 0`"] {
            assert!(project.contains(text), "missing {text:?}: {project}");
        }
        let instance = gone(&format!("t/instance/{id}/ada/f"));
        assert!(instance.contains("deleted with its project"), "{instance}");
        let shared = gone("t/shared/team/f");
        assert!(shared.contains("by hand or at its deadline") && !shared.contains("with its"), "{shared}");
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

    /// A removal that left rows answers a 409 carrying what it left and
    /// why, never the generic internal body.
    #[test]
    fn an_incomplete_removal_says_what_it_left() {
        let super::ApiError { status, message, completing } =
            map_err(RuntimeStoreError::RemovalIncomplete("left 1 the storage bucket refused to remove".into()));
        assert_eq!((status, completing), (StatusCode::CONFLICT, false));
        assert!(message.contains("refused to remove"), "{message}");
    }

    #[test]
    fn a_gone_run_file_does_not_claim_a_fixed_thirty_day_lifetime() {
        let message = map_err(RuntimeStoreError::NotFound("tenant/run/run_id/file".into())).message;
        assert!(!message.contains("30 days"), "{message}");
    }

    const OWN: &str = "http://127.0.0.1:14111";

    fn forwarded(base: &str) -> PresignAudience {
        PresignAudience::Forwarded { base: base.into() }
    }

    /// What the internet address read answers, and a note that it was read.
    fn internet(answer: anyhow::Result<Option<String>>) -> (std::sync::Arc<std::sync::atomic::AtomicBool>, impl FnOnce() -> std::future::Ready<anyhow::Result<Option<String>>>) {
        let read = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = read.clone();
        (read, move || {
            seen.store(true, std::sync::atomic::Ordering::SeqCst);
            std::future::ready(answer)
        })
    }

    fn tunnel() -> anyhow::Result<Option<String>> {
        Ok(Some("https://t.example".into()))
    }

    /// Node code reaches the store at the workers' address, which the
    /// internet opens only on a store it reaches.
    #[tokio::test]
    async fn an_internal_link_is_public_only_on_a_public_store() {
        for store_public in [false, true] {
            let (read, ask) = internet(tunnel());
            let route = route_link(LinkReach::Internal, store_public, None, Some("https://shop.example".into()), OWN, ask).await.unwrap();
            assert_eq!(route, (PresignAudience::Install, store_public));
            assert!(!read.load(std::sync::atomic::Ordering::SeqCst));
        }
    }

    /// A local install's internal port as its containers reach it.
    const CONTAINERS: &str = "http://host.docker.internal:14113";

    /// A link for the project's containers on a local install goes
    /// through the files door on the internal port as a container reaches
    /// it, signed for that address (a container's `127.0.0.1` is itself),
    /// and the internet does not open it. On an install whose containers
    /// reach the store where the workers do (a cloud one), it is the
    /// internal link, whatever the store. Neither asks the tunnel.
    #[tokio::test]
    async fn a_container_link_goes_through_the_containers_address_else_it_is_the_internal_one() {
        let (read, ask) = internet(tunnel());
        let local = route_link(LinkReach::Container, false, Some(CONTAINERS), None, OWN, ask).await.unwrap();
        assert_eq!(local, (forwarded(CONTAINERS), false));
        let signed = FakeObjectStore::new()
            .presign_get(
                "runtime/t/k",
                &GetLink {
                    audience: local.0,
                    signed_at: std::time::UNIX_EPOCH,
                    ttl_secs: 900,
                    content_type: None,
                    disposition: None,
                },
            )
            .await
            .unwrap();
        assert!(signed.starts_with("http://host.docker.internal:14113/files/"), "{signed}");
        for store_public in [false, true] {
            let (_, ask) = internet(tunnel());
            let internal = route_link(LinkReach::Internal, store_public, None, None, OWN, ask).await.unwrap();
            let (_, ask) = internet(tunnel());
            assert_eq!(route_link(LinkReach::Container, store_public, None, None, OWN, ask).await.unwrap(), internal);
        }
        assert!(!read.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// A store the internet reaches signs its own link, whatever else the
    /// install has.
    #[tokio::test]
    async fn an_outside_link_on_a_public_store_is_the_stores() {
        let (read, ask) = internet(tunnel());
        let route = route_link(LinkReach::External, true, None, Some("https://shop.example".into()), OWN, ask).await.unwrap();
        assert_eq!(route, (PresignAudience::External, true));
        assert!(!read.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// The project's visitor address wins, and the tunnel is never asked.
    #[tokio::test]
    async fn an_outside_link_goes_through_the_visitor_address_without_asking_the_tunnel() {
        let (read, ask) = internet(Err(anyhow::anyhow!("never asked")));
        let route = route_link(LinkReach::External, false, None, Some("https://shop.example/weft".into()), OWN, ask).await.unwrap();
        assert_eq!(route, (forwarded("https://shop.example/weft"), true));
        assert!(!read.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// With no visitor address: the internet address, public; with none
    /// either, the install's own, which the internet does not open.
    #[tokio::test]
    async fn an_outside_link_without_a_site_goes_through_the_internet_else_the_own_address() {
        let (read, ask) = internet(tunnel());
        assert_eq!(route_link(LinkReach::External, false, None, None, OWN, ask).await.unwrap(), (forwarded("https://t.example"), true));
        assert!(read.load(std::sync::atomic::Ordering::SeqCst));
        let (_, ask) = internet(Ok(None));
        assert_eq!(route_link(LinkReach::External, false, None, None, OWN, ask).await.unwrap(), (forwarded(OWN), false));
    }

    /// A tunnel that cannot say its address refuses the link: one through
    /// a tunnel that is down would open for nobody.
    #[tokio::test]
    async fn an_unreadable_tunnel_refuses_the_outside_link() {
        let (_, ask) = internet(Err(anyhow::anyhow!("cloudflared is down")));
        let refused = route_link(LinkReach::External, false, None, None, OWN, ask).await.unwrap_err();
        assert_eq!(refused.status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// The place a claim sends a file to takes one, nullable or not, as
    /// the engine writes its type.
    #[test]
    fn a_claim_goes_into_a_place_that_takes_a_file() {
        assert!(super::received_into("Video | Null").is_ok());
        assert!(super::received_into("File").is_ok());
        assert!(super::received_into("String").is_err());
        assert!(super::received_into("not a type ((").is_err());
    }

    /// A slot with no upload is said as one, never as a missing file.
    #[test]
    fn a_slot_with_no_upload_says_so() {
        let super::ApiError { status, message, .. } = map_err(RuntimeStoreError::NoUpload("nothing was uploaded".into()));
        assert_eq!((status, message.as_str()), (StatusCode::NOT_FOUND, "nothing was uploaded"));
    }

    /// A store op may have committed its first steps when a later one gets
    /// no connection, so it is told to ask again, never that nothing was
    /// done.
    #[test]
    fn a_store_op_without_a_connection_never_says_nothing_was_done() {
        let super::ApiError { status, message, .. } = map_err(RuntimeStoreError::Other(no_connection().into()));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(message, weft_broker_client::protocol::NOT_DONE);
    }

    /// A store op that could not get a connection before its first write
    /// says nothing was done, so the caller waits and sends it again; any
    /// other failure there keeps the generic body.
    #[test]
    fn a_store_op_without_its_first_connection_says_nothing_was_done() {
        let super::ApiError { status, message, .. } = map_err(RuntimeStoreError::BeforeWrites(no_connection().into()));
        assert_eq!((status, message.as_str()), (StatusCode::SERVICE_UNAVAILABLE, weft_broker_client::protocol::NOT_DONE));
        let super::ApiError { status, message, .. } = map_err(RuntimeStoreError::BeforeWrites(db::Error::NoRow.into()));
        assert_eq!((status, message.as_str()), (StatusCode::INTERNAL_SERVER_ERROR, super::INTERNAL_STORAGE_ERROR_BODY));
    }
}

// These tests need a real Postgres. A module named `db_tests` is how
// `.config/nextest.toml` finds database tests inside a library: the
// default profile leaves them out, the `db` profile runs them.
#[cfg(test)]
mod db_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use weft_task_store::db::{self, Client as _, Conn, Pool};
    use weft_core::storage::key::{self, CallerAuth};
    use weft_core::storage::StorageScope;
    use weft_platform_traits::clock::FakeClock;
    use weft_platform_traits::object_store::fake::FakeObjectStore;
    use weft_platform_traits::object_store::{GetLink, ObjectEntry};
    use weft_platform_traits::{CopyProgress, ObjectStore, PresignAudience};

    use super::{map_err, ApiError, StatusCode};
    use crate::entitlement::LocalEntitlementSource;
    use crate::runtime_store::{BeginUpload, RuntimeStore, RuntimeStoreError, UploadSpec};

    /// The fake bucket, which on its first `delete` takes the one
    /// connection of `pool` and keeps it: whatever the caller asks of the
    /// database after that bucket call finds no connection. Dumb injection.
    struct TakesTheConnectionOnDelete {
        bucket: FakeObjectStore,
        pool: Pool,
        taken: tokio::sync::Mutex<Option<Conn>>,
    }

    #[async_trait::async_trait]
    impl ObjectStore for TakesTheConnectionOnDelete {
        async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
            self.bucket.put(key, bytes).await
        }
        async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
            self.bucket.get(key).await
        }
        async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Option<Bytes>> {
            self.bucket.get_range(key, start, end).await
        }
        async fn exists(&self, key: &str) -> anyhow::Result<bool> {
            self.bucket.exists(key).await
        }
        async fn size(&self, key: &str) -> anyhow::Result<Option<u64>> {
            self.bucket.size(key).await
        }
        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            let mut taken = self.taken.lock().await;
            if taken.is_none() {
                *taken = Some(self.pool.get().await.expect("the pool's one connection"));
            }
            self.bucket.delete(key).await
        }
        async fn list(&self, prefix: &str) -> anyhow::Result<Vec<ObjectEntry>> {
            self.bucket.list(prefix).await
        }
        async fn presign_get(&self, key: &str, link: &GetLink) -> anyhow::Result<String> {
            self.bucket.presign_get(key, link).await
        }
        async fn create_multipart(&self, key: &str, content_type: &str) -> anyhow::Result<String> {
            self.bucket.create_multipart(key, content_type).await
        }
        async fn copy(&self, from: &str, to: &str, progress: &dyn CopyProgress) -> anyhow::Result<u64> {
            self.bucket.copy(from, to, progress).await
        }
        async fn presign_part(
            &self,
            key: &str,
            upload_id: &str,
            part_number: i32,
            part_size: u64,
            audience: &PresignAudience,
            ttl_secs: u64,
        ) -> anyhow::Result<String> {
            self.bucket.presign_part(key, upload_id, part_number, part_size, audience, ttl_secs).await
        }
        async fn upload_part(&self, key: &str, upload_id: &str, part_number: i32, bytes: Bytes) -> anyhow::Result<String> {
            self.bucket.upload_part(key, upload_id, part_number, bytes).await
        }
        async fn complete_multipart(&self, key: &str, upload_id: &str, parts: &[(i32, String)]) -> anyhow::Result<u64> {
            self.bucket.complete_multipart(key, upload_id, parts).await
        }
        async fn abort_multipart(&self, key: &str, upload_id: &str) -> anyhow::Result<()> {
            self.bucket.abort_multipart(key, upload_id).await
        }
        async fn multipart_exists(&self, key: &str, upload_id: &str) -> anyhow::Result<bool> {
            self.bucket.multipart_exists(key, upload_id).await
        }
    }

    /// A delete fences its file's row and commits, removes the object, then
    /// drops the row. When the last step gets no connection, the first two
    /// are done (sending the delete again finds no file), so the answer
    /// asks again without saying nothing was done.
    #[weft_db_macros::test]
    async fn a_delete_that_loses_its_connection_after_the_fence_never_says_nothing_was_done(pool: Pool) {
        weft_task_store::apply_groups(&pool, &[&crate::runtime_store::GROUP, &weft_task_store::unanswered::GROUP])
            .await
            .unwrap();
        weft_task_store::unanswered::judged_run_columns(&pool).await.unwrap();
        // One connection, waited for briefly, so the step after the bucket's
        // delete finds none.
        let one = Pool::open(pool.config().clone(), 1, Duration::from_secs(1), db::Waits::Expected);
        let bucket = Arc::new(TakesTheConnectionOnDelete { bucket: FakeObjectStore::new(), pool: one.clone(), taken: Default::default() });
        let clock = FakeClock::new();
        // The file is stored through the whole pool; only the delete runs
        // on the one connection.
        let stores = RuntimeStore::new(pool.clone(), bucket.clone(), clock.clone());
        let store = RuntimeStore::new(one.clone(), bucket.clone(), clock);
        let caller = CallerAuth::Worker { tenant: "t1".into(), project_id: "p1".into(), run_id: Some("c1".into()), instance: None };
        let spec = UploadSpec {
            scope: &StorageScope::Project,
            mime: "text/plain",
            filename: "empty",
            keep: None,
            declared_size: Some(0),
            content_hash: None,
            identity: None,
            slot: None,
            imported: None,
        };
        let entitlements = LocalEntitlementSource::from_env();
        let BeginUpload::Ready { key, .. } = stores.begin_upload(&caller, &spec, &entitlements).await.unwrap() else {
            panic!("a uuid-id begin is ready");
        };
        stores.complete_upload(&caller, &key).await.unwrap();
        assert!(!bucket.bucket.is_empty(), "the empty file is an object");
        let parsed = key::parse_key(&key).unwrap();

        let failed = store.delete(&parsed).await.unwrap_err();
        let RuntimeStoreError::Other(cause) = &failed else { panic!("a database failure, got {failed:?}") };
        assert!(
            db::database_no_connection(cause).is_some(),
            "the last step got no connection: {cause:#}"
        );
        let ApiError { status, message, .. } = map_err(failed);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(message, weft_broker_client::protocol::NOT_DONE);

        // What committed before the failure: the row is fenced and the
        // object is gone, so the delete sent again finds no file.
        let status: String = pool.scalar("SELECT status FROM runtime_file WHERE key = $1", &[&key]).await.unwrap();
        assert_eq!(status, "reaping");
        assert!(bucket.bucket.is_empty(), "the object was removed");
        drop(bucket.taken.lock().await.take());
        assert!(matches!(store.delete(&parsed).await, Err(RuntimeStoreError::NotFound(_))));
    }

    /// A delete that cannot get its first connection has done nothing: it
    /// says so, the caller sends it again, and that one deletes the file.
    #[weft_db_macros::test]
    async fn a_delete_with_no_first_connection_says_nothing_was_done(pool: Pool) {
        weft_task_store::apply_groups(&pool, &[&crate::runtime_store::GROUP, &weft_task_store::unanswered::GROUP])
            .await
            .unwrap();
        weft_task_store::unanswered::judged_run_columns(&pool).await.unwrap();
        let one = Pool::open(pool.config().clone(), 1, Duration::from_secs(1), db::Waits::Expected);
        let bucket = Arc::new(FakeObjectStore::new());
        let clock = FakeClock::new();
        let stores = RuntimeStore::new(pool.clone(), bucket.clone(), clock.clone());
        let store = RuntimeStore::new(one.clone(), bucket.clone(), clock);
        let caller = CallerAuth::Worker { tenant: "t1".into(), project_id: "p1".into(), run_id: Some("c1".into()), instance: None };
        let spec = UploadSpec {
            scope: &StorageScope::Project,
            mime: "text/plain",
            filename: "empty",
            keep: None,
            declared_size: Some(0),
            content_hash: None,
            identity: None,
            slot: None,
            imported: None,
        };
        let entitlements = LocalEntitlementSource::from_env();
        let BeginUpload::Ready { key, .. } = stores.begin_upload(&caller, &spec, &entitlements).await.unwrap() else {
            panic!("a uuid-id begin is ready");
        };
        stores.complete_upload(&caller, &key).await.unwrap();
        let parsed = key::parse_key(&key).unwrap();

        let taken = one.get().await.unwrap();
        let failed = store.delete(&parsed).await.unwrap_err();
        assert!(matches!(failed, RuntimeStoreError::BeforeWrites(_)), "{failed:?}");
        let ApiError { status, message, .. } = map_err(failed);
        assert_eq!((status, message.as_str()), (StatusCode::SERVICE_UNAVAILABLE, weft_broker_client::protocol::NOT_DONE));
        let status: String = pool.scalar("SELECT status FROM runtime_file WHERE key = $1", &[&key]).await.unwrap();
        assert_eq!(status, "active", "nothing of the delete ran");
        assert!(!bucket.is_empty(), "the object is still there");

        drop(taken);
        store.delete(&parsed).await.unwrap();
        assert!(bucket.is_empty(), "the delete sent again removed the file");
    }
}

#[cfg(test)]
mod scope_query_tests {
    use axum::extract::FromRequestParts;
    use weft_core::wire_shape::AnswersUnreadable;

    use super::{HeaderMap, IntoResponse, Query, Response, ScopeQuery, StatusCode, StorageScope};

    /// Answers what does not read the way the broker does, without the
    /// identity check.
    struct Refuses;

    impl AnswersUnreadable for Refuses {
        type Caller = ();

        async fn caller(&self, _: &HeaderMap) -> Result<(), Response> {
            Ok(())
        }

        fn other_shape(&self, (): (), path: &str, why: &str) -> Response {
            (weft_core::wire_shape::UNREADABLE_SHAPE, weft_core::wire_shape::unreadable_shape(&format!("{path}: {why}"))).into_response()
        }
    }

    async fn scope(query: &str) -> Result<StorageScope, StatusCode> {
        let (mut parts, ()) = axum::http::Request::get(format!("/v1/storage/list?{query}")).body(()).unwrap().into_parts();
        Query::<ScopeQuery>::from_request_parts(&mut parts, &Refuses).await.map(|Query(q)| q.scope).map_err(|answer| answer.status())
    }

    /// The scope travels JSON-encoded in the query; one of another shape
    /// (or none) is a query of another shape, answered as every call the
    /// broker cannot read is.
    #[tokio::test]
    async fn a_scope_of_another_shape_is_answered_as_a_call_the_broker_cannot_read() {
        assert_eq!(scope("scope=%7B%22kind%22%3A%22project%22%7D").await, Ok(StorageScope::Project));
        assert_eq!(scope("scope=%22project%22").await, Err(StatusCode::UPGRADE_REQUIRED));
        assert_eq!(scope("scope=%7B").await, Err(StatusCode::UPGRADE_REQUIRED));
        assert_eq!(scope("other=1").await, Err(StatusCode::UPGRADE_REQUIRED));
    }
}
