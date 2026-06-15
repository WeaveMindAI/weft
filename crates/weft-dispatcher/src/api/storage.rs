//! Storage-plane HTTP surface on the dispatcher.
//!
//! User/CLI plane (`weft files` / `weft storage config`): list,
//! usage, delete, profile, and the DOWNLOAD HANDSHAKE (authorize +
//! ask the box to mint a capability + return the box's public URL;
//! the client then streams DIRECTLY from the box; bytes never touch
//! the dispatcher).
//!
//! Internal plane (`/internal/storage/...`): the box's grow/shrink
//! disk requests, authenticated by relaying the box's bearer to the
//! broker (verdict must be THE SAME TENANT's storage box).

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::{Deserialize, Serialize};
use weft_broker_client::protocol::StorageAuthorizeResponse;
use weft_storage::auth::RawAuth;
use weft_storage::client::StorageClientError;

use crate::state::DispatcherState;
use crate::storage_box;
use crate::tenant::TenantId;

type ApiError = (StatusCode, String);

fn internal(e: impl std::fmt::Display) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
}

fn from_client_err(e: StorageClientError) -> ApiError {
    match e {
        StorageClientError::NotFound(m) => (StatusCode::NOT_FOUND, m),
        StorageClientError::Denied(m) => (StatusCode::FORBIDDEN, m),
        StorageClientError::Unreachable(m) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("storage box unreachable: {m}"),
        ),
        StorageClientError::Other(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
    }
}

/// Resolve the acting tenant for a user/CLI request. OSS routes
/// every project to `local`; cloud's router derives from auth.
/// `project` is optional: `weft files` outside a project context
/// still resolves to the caller's (single) tenant.
fn acting_tenant(state: &DispatcherState, project: Option<&str>) -> TenantId {
    state.tenant_router.tenant_for_project(project.unwrap_or(""))
}

#[derive(Debug, Deserialize)]
pub struct FilesQuery {
    pub project: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilesResponse {
    pub files: Vec<weft_core::storage::StoredFileMeta>,
}

/// GET /storage/files: every file in the tenant's box (the CLI
/// organizes by key prefix). 200 with empty list when no box exists
/// (nothing stored is not an error).
pub async fn list_files(
    State(state): State<DispatcherState>,
    Query(q): Query<FilesQuery>,
) -> Result<Json<FilesResponse>, ApiError> {
    let tenant = acting_tenant(&state, q.project.as_deref());
    if !storage_box::box_exists(&state.pg_pool, tenant.as_str()).await.map_err(internal)? {
        return Ok(Json(FilesResponse { files: vec![] }));
    }
    let url = storage_box::box_url(&state, &tenant);
    let files = state.storage_admin.list_all(&url).await.map_err(from_client_err)?;
    Ok(Json(FilesResponse { files }))
}

/// GET /storage/usage.
pub async fn usage(
    State(state): State<DispatcherState>,
    Query(q): Query<FilesQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = acting_tenant(&state, q.project.as_deref());
    if !storage_box::box_exists(&state.pg_pool, tenant.as_str()).await.map_err(internal)? {
        return Ok(Json(serde_json::json!({
            "provisioned": false,
            "storedBytes": 0,
            "fileCount": 0,
            "disks": [],
        })));
    }
    let url = storage_box::box_url(&state, &tenant);
    let usage = state.storage_admin.usage(&url).await.map_err(from_client_err)?;
    let mut v = serde_json::to_value(usage).map_err(internal)?;
    v["provisioned"] = serde_json::json!(true);
    Ok(Json(v))
}

#[derive(Debug, Deserialize)]
pub struct DownloadRequest {
    pub key: String,
    pub project: Option<String>,
    /// Capability lifetime; None = box default (~15 min).
    pub ttl_secs: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadResponse {
    /// Fully-qualified public URL on the box's ingress path. The
    /// client streams the bytes from HERE; the dispatcher only did
    /// this handshake.
    pub url: String,
}

/// POST /storage/files/download: the brokered handshake. The
/// dispatcher resolves the acting TENANT (OSS: the single `local`
/// tenant; cloud: from the request's auth via `tenant_router`), asks
/// that tenant's box to mint a short-lived single-file capability, and
/// returns the box's public URL. It does NOT stream bytes.
///
/// Walling is TENANT-level here: a caller only ever reaches its own
/// tenant's box. Per-USER / per-project ACL (which user may read which
/// file inside a tenant) is NOT enforced in this OSS handler; it is
/// cloud-only behavior layered on TOP by the closed cloud-api plane
/// that fronts this endpoint (same split as the rest of auth: the
/// generic mechanism lives here, the paid-tenant ACL lives in
/// weavemind). In single-user OSS the tenant IS the trust boundary, so
/// there is nothing more to gate.
pub async fn download(
    State(state): State<DispatcherState>,
    Json(req): Json<DownloadRequest>,
) -> Result<Json<DownloadResponse>, ApiError> {
    let tenant = acting_tenant(&state, req.project.as_deref());
    if !storage_box::box_exists(&state.pg_pool, tenant.as_str()).await.map_err(internal)? {
        return Err((StatusCode::NOT_FOUND, "file expired or deleted (no storage box)".into()));
    }
    let url = storage_box::box_url(&state, &tenant);
    let mint = state
        .storage_admin
        .mint(&url, &req.key, req.ttl_secs)
        .await
        .map_err(from_client_err)?;
    let tenant_label = crate::project_namespace::SafeLabel::new(tenant.as_str(), 63);
    let public_base = storage_box::public_base_url(&state, &tenant_label);
    Ok(Json(DownloadResponse { url: format!("{public_base}{}", mint.path) }))
}

#[derive(Serialize)]
pub struct PublicBaseResponse {
    pub public_base_url: String,
}

/// GET /storage/public-base: the deployment's public base URL
/// (`http://127.0.0.1:<port>` local, `https://files.<host>` cloud).
/// The webview reads this at boot to allow the storage origin in its
/// CSP `img-src`/`media-src`, so an <img>/<video> can stream the
/// bytes directly from the box (the same way the cloud web app will).
/// Tenant-agnostic: the host/origin is shared across tenants (the
/// tenant lives in the URL path), so this returns the bare base.
pub async fn public_base(State(state): State<DispatcherState>) -> Json<PublicBaseResponse> {
    Json(PublicBaseResponse { public_base_url: state.public_base_url.clone() })
}

#[derive(Debug, Deserialize)]
pub struct RemoveRequest {
    pub project: Option<String>,
    /// Exactly one of `key` (one file) or `prefix` (a whole space,
    /// e.g. `shared/team/` or `exec/<color>/`).
    pub key: Option<String>,
    pub prefix: Option<String>,
}

/// DELETE /storage/files.
pub async fn remove(
    State(state): State<DispatcherState>,
    Json(req): Json<RemoveRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = acting_tenant(&state, req.project.as_deref());
    if !storage_box::box_exists(&state.pg_pool, tenant.as_str()).await.map_err(internal)? {
        return Err((StatusCode::NOT_FOUND, "nothing stored (no storage box)".into()));
    }
    let url = storage_box::box_url(&state, &tenant);
    match (&req.key, &req.prefix) {
        (Some(key), None) => {
            state.storage_admin.delete_key(&url, key).await.map_err(from_client_err)?;
            Ok(Json(serde_json::json!({ "removed": 1 })))
        }
        (None, Some(prefix)) => {
            let wiped = state
                .storage_admin
                .wipe_prefix(&url, prefix)
                .await
                .map_err(from_client_err)?;
            Ok(Json(serde_json::json!({ "removed": wiped })))
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            "provide exactly one of `key` or `prefix`".into(),
        )),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileBody {
    pub storage_class: Option<String>,
    pub disk_unit_bytes: i64,
}

/// GET /storage/profile.
pub async fn get_profile(
    State(state): State<DispatcherState>,
    Query(q): Query<FilesQuery>,
) -> Result<Json<ProfileBody>, ApiError> {
    let tenant = acting_tenant(&state, q.project.as_deref());
    let p = storage_box::profile(&state.pg_pool, tenant.as_str())
        .await
        .map_err(internal)?;
    Ok(Json(ProfileBody { storage_class: p.storage_class, disk_unit_bytes: p.disk_unit_bytes }))
}

/// PUT /storage/profile. Applies to disks provisioned from now on.
pub async fn set_profile(
    State(state): State<DispatcherState>,
    Query(q): Query<FilesQuery>,
    Json(body): Json<ProfileBody>,
) -> Result<Json<ProfileBody>, ApiError> {
    let tenant = acting_tenant(&state, q.project.as_deref());
    storage_box::set_profile(
        &state.pg_pool,
        tenant.as_str(),
        body.storage_class.clone(),
        body.disk_unit_bytes,
    )
    .await
    .map_err(|e| (StatusCode::BAD_REQUEST, format!("{e:#}")))?;
    Ok(Json(body))
}

// ---------- internal plane (the box's grow/shrink) ----------

/// Authenticate an internal storage request: the bearer must resolve
/// (via the broker) to THE SAME tenant's storage box, or to the
/// control plane (operator tooling through the dispatcher's own SA).
async fn require_box_of(
    state: &DispatcherState,
    headers: &HeaderMap,
    tenant: &str,
) -> Result<(), ApiError> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
    match state
        .broker_authorize
        .authorize_raw(bearer, None)
        .await
        .map_err(internal)?
    {
        RawAuth::Allowed(StorageAuthorizeResponse::StorageBox { tenant_id })
            if tenant_id == tenant =>
        {
            Ok(())
        }
        RawAuth::Allowed(StorageAuthorizeResponse::ControlPlane) => Ok(()),
        RawAuth::Allowed(_) => Err((
            StatusCode::FORBIDDEN,
            "disk requests are accepted only from the tenant's own storage box".into(),
        )),
        RawAuth::Denied(reason) => Err((StatusCode::FORBIDDEN, reason)),
    }
}

/// POST /internal/storage/{tenant}/disks/add.
pub async fn disk_add(
    State(state): State<DispatcherState>,
    Path(tenant): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_box_of(&state, &headers, &tenant).await?;
    storage_box::grow(&state, &TenantId(tenant)).await.map_err(internal)?;
    Ok(Json(serde_json::json!({})))
}

#[derive(Debug, Deserialize)]
pub struct DiskRemoveRequest {
    pub disk: String,
}

/// POST /internal/storage/{tenant}/disks/remove.
pub async fn disk_remove(
    State(state): State<DispatcherState>,
    Path(tenant): Path<String>,
    headers: HeaderMap,
    Json(req): Json<DiskRemoveRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_box_of(&state, &headers, &tenant).await?;
    storage_box::shrink(&state, &TenantId(tenant), &req.disk)
        .await
        .map_err(internal)?;
    Ok(Json(serde_json::json!({})))
}
