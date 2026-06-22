//! The storage box's HTTP surface.
//!
//! Data path (workers, bearer = projected SA token, verified via the
//! broker):
//!   PUT    /v1/files            streaming put -> stored-file JSON
//!   GET    /v1/files/{*key}     streaming get (optional Range)
//!   GET    /v1/meta/{*key}      metadata only (no access bump)
//!   DELETE /v1/files/{*key}
//!   GET    /v1/list?scope=...
//!   POST   /v1/keep
//!   POST   /v1/presign          temporary signed URL for external APIs
//!
//! Public path (capability-gated; reached via the tenant ingress):
//!   GET /public/get?cap=...     streaming get (optional Range)
//!
//! Admin path (dispatcher only):
//!   POST /admin/mint            capability for a user download
//!   POST /admin/sweep-exec      terminate sweep for one color
//!   POST /admin/wipe-prefix     weft rm / weft clean
//!   GET  /admin/usage
//!   GET  /admin/list-all
//!
//! Bulk bytes only ever flow caller<->box; the dispatcher does the
//! ensure/handshake control plane and never streams file bodies.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use futures::TryStreamExt;
use serde::Deserialize;
use weft_core::storage::{ByteRange, ByteStream, KeepTtl, StorageScope, StoredFile};

use crate::auth::{AuthOutcome, BoxAuthOps};
use crate::capability;
use crate::config::{DEFAULT_CAPABILITY_TTL, MAX_CAPABILITY_TTL};
use crate::key::{self, CallerAuth, KeyScope};
use crate::protocol::*;
use crate::store::{Store, StoreError};

pub struct ServiceState {
    pub store: Arc<Store>,
    pub auth: Arc<dyn BoxAuthOps>,
    /// This box's own tenant (`WEFT_TENANT_ID`). The box is
    /// single-tenant by deployment; `authed_caller` passes this to
    /// `BoxAuthOps::authorize`, which denies any worker resolved to a
    /// different tenant. That tenant wall is what every prefix scope
    /// (including the unconditional `Shared` grant) relies on, now
    /// that a shared-namespace worker can physically reach any
    /// tenant's box.
    pub box_tenant: String,
    /// Public base URL presigned links are built on (the tenant
    /// ingress host routing to this box), e.g.
    /// `https://<tenant-host>/storage`.
    pub public_base_url: String,
}

type ApiError = (StatusCode, String);

fn internal(e: impl std::fmt::Display) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

impl From<StoreError> for ApiErrorWrap {
    fn from(e: StoreError) -> Self {
        ApiErrorWrap(match e {
            StoreError::NotFound(k) => (StatusCode::NOT_FOUND, format!("file not found: {k}")),
            StoreError::Conflict(m) => (StatusCode::CONFLICT, m),
            StoreError::Invalid(m) => (StatusCode::BAD_REQUEST, m),
            StoreError::Corrupt(m) => {
                tracing::error!(target: "weft_storage::service", "{m}");
                (StatusCode::INTERNAL_SERVER_ERROR, m)
            }
            StoreError::Other(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
        })
    }
}

/// Newtype so `?` converts StoreError to the axum error tuple
/// without an orphan-rule fight.
struct ApiErrorWrap(ApiError);

impl IntoResponse for ApiErrorWrap {
    fn into_response(self) -> Response {
        self.0.into_response()
    }
}

pub fn router(state: Arc<ServiceState>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/files", put(put_file))
        .route("/v1/files/{*key}", get(get_file).delete(delete_file))
        .route("/v1/meta/{*key}", get(get_meta))
        .route("/v1/list", get(list_files))
        .route("/v1/keep", post(keep_file))
        .route("/v1/presign", post(presign))
        .route("/public/get", get(public_get))
        .route("/admin/mint", post(admin_mint))
        .route("/admin/files/{*key}", axum::routing::delete(admin_delete_file))
        .route("/admin/sweep-exec", post(admin_sweep_exec))
        .route("/admin/wipe-prefix", post(admin_wipe_prefix))
        .route("/admin/usage", get(admin_usage))
        .route("/admin/list-all", get(admin_list_all))
        .with_state(state)
}

// ---------- auth helpers ----------

async fn authed_caller(
    state: &ServiceState,
    headers: &HeaderMap,
) -> Result<CallerAuth, ApiError> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
    let color = headers.get(HDR_COLOR).and_then(|v| v.to_str().ok());
    match state.auth.authorize(&state.box_tenant, bearer, color).await.map_err(internal)? {
        AuthOutcome::Allowed(caller) => Ok(caller),
        AuthOutcome::Denied(reason) => Err((StatusCode::FORBIDDEN, reason)),
    }
}

async fn authed_worker(
    state: &ServiceState,
    headers: &HeaderMap,
) -> Result<CallerAuth, ApiError> {
    let caller = authed_caller(state, headers).await?;
    match &caller {
        CallerAuth::Worker { .. } => Ok(caller),
        CallerAuth::ControlPlane => Err((
            StatusCode::FORBIDDEN,
            "control-plane callers use the admin surface, not the data path".into(),
        )),
    }
}

async fn authed_control_plane(
    state: &ServiceState,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    match authed_caller(state, headers).await? {
        CallerAuth::ControlPlane => Ok(()),
        CallerAuth::Worker { .. } => Err((
            StatusCode::FORBIDDEN,
            "the admin surface is dispatcher-only".into(),
        )),
    }
}

/// The shared-space name of a `StorageScope`, if it is Shared.
fn shared_name(scope: &StorageScope) -> Option<&str> {
    match scope {
        StorageScope::Shared { name } => Some(name.as_str()),
        _ => None,
    }
}

/// Record the shared-space grant (naming a shared space IS the opt-in;
/// the grant table is the audit/listing surface). No-op for non-shared
/// scopes or non-worker callers. The ONE place grant recording lives;
/// every verb that touches a shared space routes through here.
async fn record_shared_grant(
    state: &ServiceState,
    caller: &CallerAuth,
    shared_name: Option<&str>,
) -> Result<(), ApiError> {
    if let (Some(name), CallerAuth::Worker { project_id, .. }) = (shared_name, caller) {
        state
            .store
            .record_grant(project_id, name)
            .await
            .map_err(|e| ApiErrorWrap::from(e).0)?;
    }
    Ok(())
}

/// Wall check for key-addressed verbs + the shared-name auto-grant.
async fn check_access_and_grant(
    state: &ServiceState,
    caller: &CallerAuth,
    key: &str,
) -> Result<key::ParsedKey, ApiError> {
    let parsed = key::parse_key(key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    key::check_key_access(caller, &parsed.scope).map_err(|e| (StatusCode::FORBIDDEN, e))?;
    let shared = match &parsed.scope {
        KeyScope::Shared { name } => Some(name.as_str()),
        _ => None,
    };
    record_shared_grant(state, caller, shared).await?;
    Ok(parsed)
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ApiError> {
    match headers.get(name) {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(Some)
            .map_err(|_| (StatusCode::BAD_REQUEST, format!("header {name} is not utf8"))),
    }
}

/// Parse an HTTP `Range` header (`bytes=a-b` inclusive, `bytes=a-`,
/// `bytes=-n` suffix) into our exclusive-end `ByteRange`.
fn parse_range(headers: &HeaderMap, size: u64) -> Result<Option<ByteRange>, ApiError> {
    let Some(raw) = header_str(headers, "range")? else {
        return Ok(None);
    };
    let bad = || (StatusCode::RANGE_NOT_SATISFIABLE, format!("unsupported Range '{raw}'"));
    let spec = raw.strip_prefix("bytes=").ok_or_else(bad)?;
    if spec.contains(',') {
        // Multi-range responses (multipart/byteranges) are not part
        // of this protocol; reject rather than serve a wrong shape.
        return Err(bad());
    }
    let (start_s, end_s) = spec.split_once('-').ok_or_else(bad)?;
    let range = match (start_s.is_empty(), end_s.is_empty()) {
        (false, true) => ByteRange { start: start_s.parse().map_err(|_| bad())?, end: None },
        (false, false) => {
            let start: u64 = start_s.parse().map_err(|_| bad())?;
            let end_incl: u64 = end_s.parse().map_err(|_| bad())?;
            ByteRange { start, end: Some(end_incl.saturating_add(1).min(size)) }
        }
        (true, false) => {
            let n: u64 = end_s.parse().map_err(|_| bad())?;
            // `bytes=-0` requests the last zero bytes: unsatisfiable.
            if n == 0 {
                return Err(bad());
            }
            ByteRange { start: size.saturating_sub(n), end: None }
        }
        (true, true) => return Err(bad()),
    };
    // `start >= size` is unsatisfiable (valid offsets are 0..size-1):
    // a request for `bytes=<size>-` must be 416, not a malformed 206
    // with a `bytes <size>-<size-1>/<size>` Content-Range. The suffix
    // form clamps start into range, so this only rejects offset forms.
    if range.start >= size || range.end.map(|e| e < range.start).unwrap_or(false) {
        return Err(bad());
    }
    Ok(Some(range))
}

/// Build a streaming body response. When `range` is `Some`, the
/// response is an honest HTTP 206 Partial Content with `Content-Range`
/// + `Content-Length` for the served slice, so any standard client
/// (a resuming download, a browser `<img>`, an external API fetching a
/// presigned URL) can range-request and resume correctly. `Accept-Ranges`
/// is always advertised so clients know resume is supported.
fn stream_response(
    meta: weft_core::storage::StoredFileMeta,
    stream: ByteStream,
    range: Option<ByteRange>,
) -> Response {
    let mut resp = Response::new(Body::from_stream(stream.map_err(std::io::Error::other)));
    if range.is_some() {
        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
    }
    let headers = resp.headers_mut();
    // mime_type was validated as a serveable HeaderValue at put time.
    headers.insert(
        "content-type",
        meta.mime_type.parse().expect("mime validated at put time"),
    );
    headers.insert("accept-ranges", "bytes".parse().expect("static header value"));
    headers.insert(
        "x-weft-meta",
        serde_json::to_string(&meta)
            .expect("meta serializes")
            .parse()
            .expect("meta json is a valid header value"),
    );
    match range {
        None => {
            headers.insert(
                "content-length",
                meta.size_bytes.to_string().parse().expect("number is a valid header value"),
            );
        }
        Some(r) => {
            let end = r.end.unwrap_or(meta.size_bytes); // exclusive
            let last = end.saturating_sub(1); // inclusive, for Content-Range
            let len = end.saturating_sub(r.start);
            headers.insert(
                "content-range",
                format!("bytes {}-{}/{}", r.start, last, meta.size_bytes)
                    .parse()
                    .expect("content-range is a valid header value"),
            );
            headers.insert(
                "content-length",
                len.to_string().parse().expect("number is a valid header value"),
            );
        }
    }
    resp
}

// ---------- data path ----------

#[derive(Deserialize)]
struct ScopeQuery {
    /// JSON-encoded `StorageScope`.
    scope: String,
}

async fn put_file(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    let caller = authed_worker(&state, &headers).await?;
    let scope: StorageScope = serde_json::from_str(
        header_str(&headers, HDR_SCOPE)?
            .ok_or((StatusCode::BAD_REQUEST, format!("missing {HDR_SCOPE} header")))?,
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad {HDR_SCOPE}: {e}")))?;
    // mime + filename are stored verbatim and later RE-EMITTED into
    // response headers (Content-Type, Content-Disposition, x-weft-meta).
    // Validate them as serveable HERE, at the put boundary, so a stored
    // file is ALWAYS serveable. Without this, a control char in the
    // filename would make every later GET a 500 on an already-stored
    // file (junk the user cannot fix). Reject loudly with 400 instead.
    let mime = header_str(&headers, HDR_MIME)?
        .ok_or((StatusCode::BAD_REQUEST, format!("missing {HDR_MIME} header")))?
        .to_string();
    if mime.is_empty() || mime.parse::<axum::http::HeaderValue>().is_err() {
        return Err((StatusCode::BAD_REQUEST, format!("{HDR_MIME} is not a serveable media type")));
    }
    let filename = header_str(&headers, HDR_FILENAME)?
        .ok_or((StatusCode::BAD_REQUEST, format!("missing {HDR_FILENAME} header")))?
        .to_string();
    // The Content-Disposition filename is quoted; a literal double
    // quote or any control char would break the header, so reject them
    // at put (rather than mangling the name at serve time).
    if filename.contains('"') || filename.chars().any(|c| c.is_control()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{HDR_FILENAME} must not contain quotes or control characters"),
        ));
    }
    let keep: Option<KeepTtl> = match header_str(&headers, HDR_KEEP)? {
        None => None,
        Some(raw) => Some(
            serde_json::from_str(raw)
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad {HDR_KEEP}: {e}")))?,
        ),
    };
    if keep.is_some() && !matches!(scope, StorageScope::Execution) {
        return Err((
            StatusCode::BAD_REQUEST,
            "keep only applies to execution-scoped files; project/shared files are \
             persistent without a flag"
                .into(),
        ));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let key = key::key_for_put(&caller, &scope, &id)
        .map_err(|e| (StatusCode::FORBIDDEN, e))?;
    record_shared_grant(&state, &caller, shared_name(&scope)).await?;
    let stream: ByteStream = Box::pin(
        body.into_data_stream()
            .map_err(|e| std::io::Error::other(format!("request body: {e}"))),
    );
    let meta = state
        .store
        .put(&key, &mime, &filename, keep, stream)
        .await
        .map_err(|e| ApiErrorWrap::from(e).0)?;
    let file = StoredFile {
        key: meta.key.clone(),
        mime_type: meta.mime_type.clone(),
        size_bytes: meta.size_bytes,
        filename: meta.filename.clone(),
    };
    Ok(Json(file.to_value()).into_response())
}

async fn get_file(
    State(state): State<Arc<ServiceState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let caller = authed_worker(&state, &headers).await?;
    let parsed = check_access_and_grant(&state, &caller, &key).await?;
    let meta = state
        .store
        .meta(&parsed)
        .await
        .ok_or((StatusCode::NOT_FOUND, format!("file not found: {key}")))?;
    let range = parse_range(&headers, meta.size_bytes)?;
    let (meta, stream) = state
        .store
        .get(&parsed, range)
        .await
        .map_err(|e| ApiErrorWrap::from(e).0)?;
    Ok(stream_response(meta, stream, range))
}

async fn get_meta(
    State(state): State<Arc<ServiceState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let caller = authed_worker(&state, &headers).await?;
    let parsed = check_access_and_grant(&state, &caller, &key).await?;
    let meta = state
        .store
        .meta(&parsed)
        .await
        .ok_or((StatusCode::NOT_FOUND, format!("file not found: {key}")))?;
    Ok(Json(meta).into_response())
}

async fn delete_file(
    State(state): State<Arc<ServiceState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let caller = authed_worker(&state, &headers).await?;
    let parsed = check_access_and_grant(&state, &caller, &key).await?;
    state.store.delete(&parsed).await.map_err(|e| ApiErrorWrap::from(e).0)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn list_files(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Query(q): Query<ScopeQuery>,
) -> Result<Json<ListResponse>, ApiError> {
    let caller = authed_worker(&state, &headers).await?;
    let scope: StorageScope = serde_json::from_str(&q.scope)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad scope: {e}")))?;
    let prefix =
        key::prefix_for_list(&caller, &scope).map_err(|e| (StatusCode::FORBIDDEN, e))?;
    record_shared_grant(&state, &caller, shared_name(&scope)).await?;
    Ok(Json(ListResponse { files: state.store.list(&prefix).await }))
}

async fn keep_file(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Json(req): Json<KeepRequest>,
) -> Result<Response, ApiError> {
    let caller = authed_worker(&state, &headers).await?;
    let parsed = check_access_and_grant(&state, &caller, &req.key).await?;
    if !matches!(parsed.scope, KeyScope::Exec { .. }) {
        return Err((
            StatusCode::BAD_REQUEST,
            "keep only applies to execution-scoped files; project/shared files are \
             persistent without a flag"
                .into(),
        ));
    }
    let meta = state
        .store
        .keep(&parsed, req.ttl)
        .await
        .map_err(|e| ApiErrorWrap::from(e).0)?;
    Ok(Json(meta).into_response())
}

/// Resolve + clamp a requested capability TTL.
fn capability_ttl(ttl_secs: Option<u64>) -> u64 {
    ttl_secs
        .unwrap_or(DEFAULT_CAPABILITY_TTL.as_secs())
        .min(MAX_CAPABILITY_TTL.as_secs())
        .max(1)
}

async fn mint_for(
    state: &ServiceState,
    key: &key::ParsedKey,
    ttl_secs: Option<u64>,
) -> Result<(String, String), ApiError> {
    // The cap is signed over the canonical key string; `public_get`
    // re-parses it back through the wall's grammar before touching the
    // store, so every cap names a real <scope>/<owner>/<id> on both ends.
    let key_str = key.to_key();
    // Minting for a missing file would hand out a URL that can only
    // 404; fail the mint instead. Minting counts as access (bumps a
    // kept file's TTL).
    state.store.touch_access(key).await.map_err(|e| ApiErrorWrap::from(e).0)?;
    let secret = state.store.capability_secret().await.map_err(|e| ApiErrorWrap::from(e).0)?;
    let exp = state.store.clock().now_unix() + capability_ttl(ttl_secs) as i64;
    let cap = capability::mint(&secret, &key_str, exp);
    let path = format!("/public/get?cap={cap}");
    Ok((cap, path))
}

async fn presign(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Json(req): Json<PresignRequest>,
) -> Result<Json<PresignResponse>, ApiError> {
    let caller = authed_worker(&state, &headers).await?;
    let parsed = check_access_and_grant(&state, &caller, &req.key).await?;
    let (_cap, path) = mint_for(&state, &parsed, req.ttl_secs).await?;
    Ok(Json(PresignResponse {
        url: format!("{}{}", state.public_base_url.trim_end_matches('/'), path),
    }))
}

// ---------- public (capability-gated) ----------

#[derive(Deserialize)]
struct CapQuery {
    cap: String,
}

async fn public_get(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Query(q): Query<CapQuery>,
) -> Result<Response, ApiError> {
    let secret = state.store.capability_secret().await.map_err(|e| ApiErrorWrap::from(e).0)?;
    let claims = capability::validate(&secret, &q.cap, state.store.clock().now_unix())
        .map_err(|e| (StatusCode::FORBIDDEN, e))?;
    // Re-parse the cap's key through the wall's grammar before it reaches
    // the store: the cap is signed, but the store API only accepts a
    // ParsedKey, so a malformed key (a tampered or stale-format cap) is
    // rejected here rather than trusted because it carried a signature.
    let parsed = key::parse_key(&claims.key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let meta = state
        .store
        .meta(&parsed)
        .await
        .ok_or((StatusCode::NOT_FOUND, "file expired or deleted".to_string()))?;
    let range = parse_range(&headers, meta.size_bytes)?;
    let (meta, stream) = state
        .store
        .get(&parsed, range)
        .await
        .map_err(|e| ApiErrorWrap::from(e).0)?;
    let mut resp = stream_response(meta.clone(), stream, range);
    // Name the file for a browser download. `attachment` is correct
    // even for the inline image-preview path: an <img>/<video> tag
    // renders the bytes regardless of disposition (the tag, not the
    // header, decides inline rendering), while a direct browser
    // navigation saves the file. The filename was validated quote-free
    // and control-char-free at put time, so it is always header-safe.
    resp.headers_mut().insert(
        "content-disposition",
        format!("attachment; filename=\"{}\"", meta.filename)
            .parse()
            .expect("filename validated header-safe at put time"),
    );
    Ok(resp)
}

// ---------- admin (dispatcher only) ----------

async fn admin_mint(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Json(req): Json<MintRequest>,
) -> Result<Json<MintResponse>, ApiError> {
    authed_control_plane(&state, &headers).await?;
    // The store API takes a ParsedKey, so parse the control-plane's
    // requested key through the wall's grammar before minting a cap over
    // it (the cap and `public_get` both go through the same grammar).
    let parsed = key::parse_key(&req.key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let (capability, path) = mint_for(&state, &parsed, req.ttl_secs).await?;
    Ok(Json(MintResponse { capability, path }))
}

async fn admin_delete_file(
    State(state): State<Arc<ServiceState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authed_control_plane(&state, &headers).await?;
    // The store API takes a ParsedKey, so the control-plane's key passes
    // the wall's grammar here, even though the caller is trusted: "a key
    // reaching the store is always a real <scope>/<owner>/<id>" holds by
    // construction, not by the accident that `delete` is index-keyed.
    let parsed = key::parse_key(&key).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    state.store.delete(&parsed).await.map_err(|e| ApiErrorWrap::from(e).0)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn admin_sweep_exec(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Json(req): Json<SweepExecRequest>,
) -> Result<Json<SweepExecResponse>, ApiError> {
    authed_control_plane(&state, &headers).await?;
    let swept = state
        .store
        .sweep_exec(&req.color)
        .await
        .map_err(|e| ApiErrorWrap::from(e).0)?;
    Ok(Json(SweepExecResponse { swept }))
}

async fn admin_wipe_prefix(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Json(req): Json<WipePrefixRequest>,
) -> Result<Json<WipePrefixResponse>, ApiError> {
    authed_control_plane(&state, &headers).await?;
    let wiped = state
        .store
        .wipe_prefix(&req.prefix)
        .await
        .map_err(|e| ApiErrorWrap::from(e).0)?;
    Ok(Json(WipePrefixResponse { wiped }))
}

async fn admin_usage(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
) -> Result<Json<Usage>, ApiError> {
    authed_control_plane(&state, &headers).await?;
    Ok(Json(state.store.usage().await.map_err(|e| ApiErrorWrap::from(e).0)?))
}

async fn admin_list_all(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
) -> Result<Json<ListResponse>, ApiError> {
    authed_control_plane(&state, &headers).await?;
    Ok(Json(ListResponse { files: state.store.list_all().await }))
}
