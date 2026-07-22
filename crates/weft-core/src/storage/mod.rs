//! Storage-plane surface types: scopes, keep-TTLs, the stored-file
//! reference value, and the byte-stream aliases shared by the
//! `ContextHandle` storage methods, the engine's client, and the
//! storage service.
//!
//! A stored file is referenced everywhere by a small SELF-DESCRIBING
//! value tagged with its CONCRETE type
//! (`{"__weft_<image|video|audio|blob>__": {key, mimeType, sizeBytes,
//! filename}}`, NO url). Bytes never ride the journal/pulse/task
//! path; they flow worker<->box or client<->box directly. The key is
//! the full tenant-local storage path (`exec/<color>/<id>`,
//! `project/<project_id>/<id>`, `shared/<name>/<id>`), so a key
//! alone names both the file and the scope wall that guards it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{WeftError, WeftResult};

/// The storage key grammar + the identity wall (pure functions): given a
/// broker-verified caller and a key, allowed or denied with no policy. The
/// runtime-storage data path (the broker) and the CLI key-prefixers (the
/// dispatcher) both go through it, so the grammar lives next to the
/// `StorageScope`/`StoredFile` contract it guards, in one dependency-free place.
pub mod key;

/// Boxed byte stream used for streaming put/get. `'static` so it can
/// cross the `ContextHandle` trait object; chunks are `Bytes` so
/// hops are zero-copy.
#[cfg(feature = "runtime")]
pub type ByteStream = futures::stream::BoxStream<'static, std::io::Result<bytes::Bytes>>;

/// Wrap a fully-buffered payload as a one-chunk [`ByteStream`].
#[cfg(feature = "runtime")]
pub fn bytes_stream(bytes: bytes::Bytes) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(bytes) }))
}

/// Extension -> mime guess for a name/URL with no served Content-Type (an
/// asset ref at compile time, a pasted URL). Display + marker-kind selection
/// only: whenever real bytes are fetched, the response's own Content-Type is
/// authoritative. Small common set; unknown -> octet-stream.
// SYNC: mime_from_filename <-> packages/weft-graph/src/webview/lib/utils/file-browser.ts EXT_MIME
pub fn mime_from_filename(name: &str) -> &'static str {
    match name.rsplit('.').next().map(str::to_ascii_lowercase).as_deref() {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("avif") => "image/avif",
        Some("mp4") => "video/mp4",
        Some("mov") => "video/quicktime",
        Some("webm") => "video/webm",
        Some("mkv") => "video/x-matroska",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("ogg") => "audio/ogg",
        Some("flac") => "audio/flac",
        Some("m4a") => "audio/mp4",
        Some("pdf") => "application/pdf",
        Some("csv") => "text/csv",
        Some("txt") => "text/plain",
        Some("json") => "application/json",
        Some("zip") => "application/zip",
        _ => "application/octet-stream",
    }
}

/// The url-form file value for an external media ref (`@file("https://…",
/// Image)` or the editor's paste-URL): a concrete marker whose payload's
/// handle is the URL. The marker KIND comes from the declared type when it
/// names a concrete file primitive (an `Image` ref is `__weft_image__`
/// whatever the extension suggests); a union (`File`/`Media`) falls back to
/// the extension's mime guess. Size is unknown until fetched (0, display
/// only); the worker's fetch reports the real Content-Type at run time.
pub fn url_file_value(url: &str, declared: &crate::weft_type::WeftType) -> Value {
    let filename = filename_from_url(url);
    let mime = mime_from_filename(&filename);
    let kind = declared
        .concrete_file_kind()
        .unwrap_or_else(|| crate::weft_type::FileKind::from_mime(mime));
    crate::weft_type::WeftType::file_marker(
        kind,
        serde_json::json!({
            "url": url,
            "mimeType": mime,
            "sizeBytes": 0,
            "filename": filename,
        }),
    )
}

/// The stored-file value for `file` with the marker KIND picked by a DECLARED
/// type when it names a concrete file primitive (an asset `@file` ref typed
/// `Image` is `__weft_image__` even if its extension guesses a different
/// mime); a union (`File`/`Media`) or non-file declaration falls back to the
/// file's own mime, exactly like [`StoredFile::to_value`].
pub fn typed_file_value(file: &StoredFile, declared: &crate::weft_type::WeftType) -> Value {
    let kind = declared.concrete_file_kind().unwrap_or_else(|| file.kind());
    crate::weft_type::WeftType::file_marker(
        kind,
        serde_json::to_value(file).expect("StoredFile serializes"),
    )
}

/// Is `s` a content hash: exactly 64 lowercase hex chars (sha256). The id
/// grammar of the ASSET scope (`asset/<project>/<sha256>`): the hash IS the
/// file's identity, so "this content is uploaded" is a key-existence check and
/// the pre-build sync's diff is a set compare. Anything else in asset-id
/// position is a rejected request, never a lookup.
pub fn is_content_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Derive a filename from a URL's last path segment, sans query and
/// fragment. Falls back to `download.bin` when the URL has no usable
/// path (ends in `/`, or is bare host). Used by the storage
/// put-from-url capability when the caller gives no explicit name.
pub fn filename_from_url(url: &str) -> String {
    let no_query = url.split(['?', '#']).next().unwrap_or(url);
    let last = no_query.trim_end_matches('/').rsplit('/').next().unwrap_or("");
    if last.is_empty() {
        "download.bin".to_string()
    } else {
        last.to_string()
    }
}

/// Normalize an HTTP `Content-Type` header into a storable mime type:
/// drop any `; charset=...` parameter and trim, falling back to
/// `application/octet-stream` for an absent or empty value. Every node
/// that streams a remote URL into storage funnels its content-type
/// through here so the stored mime is consistent (a divergence here
/// once stored `image/png; charset=binary`, which broke stored-file typing
/// downstream). Pure (takes the already-extracted header string), so
/// nodes keep their HTTP client and this stays in core.
pub fn normalize_content_type(header: Option<&str>) -> String {
    header
        .map(|s| s.split(';').next().unwrap_or(s).trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("application/octet-stream")
        .to_string()
}

/// Collect a [`ByteStream`] into one contiguous buffer. Convenience
/// for callers that want the whole payload in memory; large files
/// should consume the stream incrementally instead.
#[cfg(feature = "runtime")]
pub async fn collect_stream(mut stream: ByteStream) -> std::io::Result<bytes::Bytes> {
    use futures::StreamExt;
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(bytes::Bytes::from(buf))
}

/// Which key-prefix wall a storage handle operates inside. One box
/// per tenant; the scope picks the prefix, the caller's verified
/// identity picks the values inside it (its own color / project).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StorageScope {
    /// `exec/<color>/`: walled to a single run, swept on terminate
    /// unless kept. The default.
    Execution,
    /// `project/<project_id>/`: outlives runs, shared across the
    /// project's executions, wiped by `weft clean` / `weft rm`.
    Project,
    /// `shared/<name>/`: tenant-scoped shared space. Projects that
    /// name the same `name` meet in the same prefix; first use by a
    /// project auto-grants it. Opt-in by naming.
    Shared { name: String },
    /// `asset/<project_id>/`: the project's PUBLISHED ASSETS, the storage
    /// copies of files the source references via media `@file` refs. Derived
    /// state owned by the pre-build asset sync (content-hash ids; created and
    /// deleted only through the control-plane surface). Workers READ this
    /// scope like project scope; the worker data path refuses writes to it.
    Asset,
}

impl Default for StorageScope {
    fn default() -> Self {
        Self::Execution
    }
}

/// Lifetime of a KEPT execution-scoped file. Every access bumps the
/// expiry back to now + TTL, so actively-used survivors never
/// expire. `Default` resolves to the storage service's configured
/// default (30 days); the number deliberately lives in one place
/// (the service's config module), not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KeepTtl {
    /// Service default (30 days, access-bumped).
    Default,
    /// now + this many seconds, access-bumped.
    Secs { secs: u64 },
    /// Never expires; explicit `weft files rm` / `weft clean` only.
    Never,
}

/// Byte range for a partial `get`. `start` inclusive, `end`
/// EXCLUSIVE (Rust-range convention; the HTTP edge converts to/from
/// the inclusive `Range` header form). `end == None` means "to the
/// end of the file".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteRange {
    pub start: u64,
    pub end: Option<u64>,
}

/// Per-file metadata as stored and listed by the storage service.
/// This is the wire shape of `list` / `inspect` responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredFileMeta {
    pub key: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
    pub filename: String,
    /// True iff this exec-scoped file is flagged to survive the
    /// terminate sweep. Always false for project/shared files (they
    /// are persistent without a flag).
    pub keep: bool,
    /// Unix seconds at which a kept file expires (access-bumped).
    /// `None` = no expiry (project/shared files, `KeepTtl::Never`).
    #[serde(rename = "expiresAtUnix")]
    pub expires_at_unix: Option<i64>,
    /// The kept file's TTL in seconds, so an access can recompute
    /// `expires_at = now + ttl`. `None` when there is no expiry.
    #[serde(rename = "keepTtlSecs")]
    pub keep_ttl_secs: Option<u64>,
    #[serde(rename = "createdAtUnix")]
    pub created_at_unix: i64,
}

/// The self-describing stored-file reference: the payload INSIDE a
/// concrete `__weft_<kind>__` marker (see `to_value`). The ONLY thing
/// that flows on edges / into the journal for a stored file. Carries
/// NO url: byte access always goes through an authenticated fetch (or
/// an explicit, expiring presigned URL a node deliberately mints). The
/// concrete kind (Image/Video/Audio/Blob) is derived from `mime_type`.
// SYNC: StoredFile <-> packages/weft-graph/src/protocol.ts FileValueWire
//       (per-kind markers __weft_image__/video/audio/blob, parseFileValue)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredFile {
    pub key: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
    pub filename: String,
}

impl StoredFile {
    /// The concrete kind of this stored file, classified once from its
    /// mime (image/* -> Image, video/* -> Video, audio/* -> Audio, else
    /// -> Blob). This is what picks the value's marker on the wire.
    pub fn kind(&self) -> crate::weft_type::FileKind {
        crate::weft_type::FileKind::from_mime(&self.mime_type)
    }

    /// Wrap into the CONCRETE stored-file value that flows on edges:
    /// `{ "__weft_<kind>__": { key, mimeType, sizeBytes, filename } }`.
    /// The marker IS the type, `detect_file_type` reads it directly
    /// (no mime re-guessing). There is no `__weft_media__` umbrella.
    pub fn to_value(&self) -> Value {
        crate::weft_type::WeftType::file_marker(
            self.kind(),
            serde_json::to_value(self).expect("StoredFile serializes"),
        )
    }

    /// Parse a concrete stored-file value back, regardless of which of
    /// the four markers (`__weft_image__`/video/audio/blob) it carries.
    /// Errors loud when the value is not a stored-file marker or the
    /// marker is not key-backed (a url/data file value has no storage
    /// key to act on).
    pub fn from_value(value: &Value) -> WeftResult<Self> {
        let obj = value.as_object().ok_or_else(|| {
            WeftError::Input(format!(
                "not a stored-file value (not an object): {}",
                crate::truncate_user_string(&value.to_string(), 256)
            ))
        })?;
        let kind = crate::weft_type::FileKind::from_marker_obj(obj).ok_or_else(|| {
            WeftError::Input(format!(
                "not a stored-file value (no __weft_image__/video/audio/blob marker): {}",
                crate::truncate_user_string(&value.to_string(), 256)
            ))
        })?;
        let payload = &obj[kind.marker_key()];
        serde_json::from_value(payload.clone()).map_err(|e| {
            WeftError::Input(format!(
                "stored-file value is not a stored-file reference (key/mimeType/sizeBytes/filename): {e}"
            ))
        })
    }

}

/// How to reach the bytes behind a file value: the marker payload's HANDLE.
/// A file value is a self-describing reference (see [`StoredFile`]); the
/// handle is the one field that says WHERE the bytes live. A bucket-backed
/// file carries a `key` (read via a presigned bucket URL); a file pointing at
/// an external resource carries a `url` (fetched directly). The read path
/// ([`crate::context::StorageHandle::get`]) branches on this so a node's
/// `get_bytes(&file)` is identical whichever handle the value carries: the
/// node holds an ADDRESS and decides what to do with it, and the address may
/// be a bucket key or an external URL.
///
/// (`data`, an inline base64 handle, is a third form the type system accepts
/// but the read path does not yet resolve; it is deliberately not a variant
/// here so an unresolved `data` value fails loud rather than reading wrong.)
// SYNC: FileHandle <-> packages/weft-graph/src/protocol.ts parseFileValue
#[derive(Debug, Clone, PartialEq)]
pub enum FileHandle {
    /// The bytes live in the storage bucket under this tenant-local key.
    Key(String),
    /// The bytes live at this external URL, fetched directly by the worker.
    Url {
        url: String,
        /// The declared mime, so a read reports it without a fetch-ahead
        /// (the actual Content-Type on fetch is authoritative for bytes).
        mime_type: String,
        filename: String,
        size_bytes: u64,
    },
}

impl FileHandle {
    /// Parse a file value into its handle. Accepts a bare key string (a node
    /// held onto a key), a `key`-backed marker, or a `url`-backed marker.
    /// Errors loud on a value that is neither, or a marker whose only handle
    /// is one the read path can't resolve (`data`), so a caller never silently
    /// reads the wrong bytes.
    pub fn from_value(value: &Value) -> WeftResult<Self> {
        if let Some(s) = value.as_str() {
            if s.is_empty() {
                return Err(WeftError::Input("empty storage key".into()));
            }
            return Ok(FileHandle::Key(s.to_string()));
        }
        let obj = value.as_object().ok_or_else(|| {
            WeftError::Input(format!(
                "not a file value (not an object): {}",
                crate::truncate_user_string(&value.to_string(), 256)
            ))
        })?;
        let kind = crate::weft_type::FileKind::from_marker_obj(obj).ok_or_else(|| {
            WeftError::Input(format!(
                "not a file value (no __weft_image__/video/audio/blob marker): {}",
                crate::truncate_user_string(&value.to_string(), 256)
            ))
        })?;
        let payload = obj[kind.marker_key()].as_object().ok_or_else(|| {
            WeftError::Input("file marker payload is not an object".into())
        })?;
        // A key-backed marker is the common case; a url-backed one points
        // outside the bucket. Exactly one handle is expected; prefer `key`
        // when both are somehow present (a bucket copy is authoritative).
        if let Some(key) = payload.get("key").and_then(Value::as_str) {
            if key.is_empty() {
                return Err(WeftError::Input("file marker has an empty key".into()));
            }
            return Ok(FileHandle::Key(key.to_string()));
        }
        if let Some(url) = payload.get("url").and_then(Value::as_str) {
            if url.is_empty() {
                return Err(WeftError::Input("file marker has an empty url".into()));
            }
            return Ok(FileHandle::Url {
                url: url.to_string(),
                mime_type: payload
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .unwrap_or("application/octet-stream")
                    .to_string(),
                filename: payload
                    .get("filename")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| filename_from_url(url)),
                size_bytes: payload.get("sizeBytes").and_then(Value::as_u64).unwrap_or(0),
            });
        }
        Err(WeftError::Input(
            "file value has no readable handle (expected `key` or `url`; a `data` inline \
             handle is not readable through storage)"
                .into(),
        ))
    }
}

/// Deserializing a `FileHandle` parses a FILE VALUE (the `__weft_<kind>__`
/// marker an upstream node emitted, or a bare key string), via
/// [`FileHandle::from_value`]. This is what makes
/// `ctx.get::<FileHandle>("port")` work: files are just another type to the
/// accessors, and the parse fails loud on a value with no readable handle.
impl<'de> serde::Deserialize<'de> for FileHandle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        FileHandle::from_value(&value).map_err(serde::de::Error::custom)
    }
}

// The worker <-> broker runtime-file HTTP envelopes (requests + responses).
// Defined ONCE here (the broker and the engine's worker client both depend on
// `weft-core`), so a field rename is a single edit that both sides pick up.
// No SYNC marker: this IS the single definition.

/// `GET /v1/storage/files` (list): the files under a scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListFilesResponse {
    pub files: Vec<StoredFileMeta>,
}

/// `POST /v1/storage/presign`: a presigned GET URL for one file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignResponse {
    pub url: String,
}

// The upload contract is multipart: `begin` mints the key and (for a known
// total size) charges the byte quota up front; `parts` reserves + presigns
// each part with its EXACT size signed into the URL (the bucket rejects any
// other body length, so a reservation can never be exceeded); `part-done`
// records a landed part's etag; `complete` assembles the object and flips the
// file live; `resume` re-presigns the parts that never landed after an
// interruption; `abort` cancels and frees the reservation.

/// `POST /v1/storage/upload/begin`: start an upload. `declared_size` is the
/// total byte size when the caller knows it up front (charged against the
/// quota immediately); `None` for an unknown-length stream (each part is
/// charged as it is reserved).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadBeginRequest {
    pub scope: StorageScope,
    pub mime_type: String,
    pub filename: String,
    #[serde(default)]
    pub keep: Option<KeepTtl>,
    #[serde(default)]
    pub declared_size: Option<u64>,
}

/// Begin response: the minted key + the fixed part size for this upload.
/// Every part the caller reserves must be exactly `part_size` bytes except
/// the final one (which may be smaller and marks the end of the upload).
/// A content-addressed (asset) begin whose content is already stored ACTIVE
/// answers `already_stored: true` with the existing key: there is nothing to
/// transfer, the caller must not reserve parts, and `part_size` is a
/// meaningless 0.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadBeginResponse {
    pub key: String,
    pub part_size: u64,
    #[serde(default)]
    pub already_stored: bool,
}

/// `POST /v1/storage/upload/parts`: reserve + presign the next parts, in
/// order, sized exactly as the caller will upload them. Part numbers are
/// assigned by the server, consecutively.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPartsRequest {
    pub key: String,
    pub sizes: Vec<u64>,
}

/// One reserved part: its assigned number, its exact byte size, and the
/// presigned URL to PUT exactly that many bytes to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignedPart {
    pub part_number: i32,
    pub size_bytes: u64,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPartsResponse {
    pub parts: Vec<PresignedPart>,
}

/// `POST /v1/storage/upload/part-done`: report a landed part. The etag is the
/// bucket's response header VERBATIM (quotes included); the server records the
/// part's size from its own reservation, never from the caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartDoneRequest {
    pub key: String,
    pub part_number: i32,
    pub etag: String,
}

/// `POST /v1/storage/upload/complete`: finalize the upload (all reserved
/// parts must have been reported done). Responds with the stored-file value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCompleteRequest {
    pub key: String,
}

/// `POST /v1/storage/upload/resume`: after an interruption, learn which
/// reserved parts never landed and get fresh URLs for exactly those.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadResumeRequest {
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadResumeResponse {
    pub part_size: u64,
    pub missing: Vec<PresignedPart>,
}

/// `POST /v1/storage/upload/abort`: cancel an in-flight upload, freeing its
/// quota reservation and discarding any landed parts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadAbortRequest {
    pub key: String,
}

/// `GET /v1/storage/download-url/{key}`: the file metadata + a presigned GET URL
/// the worker reads bytes from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadUrlResponse {
    pub meta: StoredFileMeta,
    pub url: String,
}

/// `POST /v1/storage/keep`: extend an execution-scoped file's lifetime past the
/// run that created it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeepRequest {
    pub key: String,
    pub ttl: KeepTtl,
}

/// `POST /v1/storage/presign`: mint a presigned GET URL for a stored file (for
/// handing to an external API), scoped to the one key with a short TTL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignRequest {
    pub key: String,
    pub ttl_secs: Option<u64>,
}

// The broker's control-plane admin envelopes (the dispatcher's CLI-verb proxy:
// `weft files ls/usage/download/rm`). Same single-definition rule as the worker
// envelopes above: the broker's handlers and the dispatcher's admin client both
// use these, so a field rename is one edit. No SYNC marker needed.

/// `POST /v1/storage/admin/tenant-list` / `tenant-usage`: the tenant the
/// control plane is acting for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantScopeRequest {
    pub tenant: String,
}

/// `POST /v1/storage/admin/tenant-usage` response: one tenant's footprint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantUsage {
    #[serde(rename = "storedBytes")]
    pub stored_bytes: u64,
    #[serde(rename = "fileCount")]
    pub file_count: u64,
}

/// `POST /v1/storage/admin/presign` response, returned verbatim as the
/// dispatcher's `/storage/files/download` response (the presign result IS the
/// download handshake; no separate same-fields struct). Carries the file's
/// friendly name and total size besides the URL: a presigned S3 GET has no
/// `x-weft-meta`, so the CLI gets these from the handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignResult {
    pub url: String,
    pub filename: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
}

/// `POST /v1/storage/admin/wipe-prefix`: wipe a whole scope/tenant prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WipePrefixRequest {
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WipePrefixResponse {
    pub wiped: u64,
}

// The admin upload surface: the dispatcher drives the same multipart upload
// contract as a worker, on behalf of a tenant's editor session (a file-drop
// config field). Editor uploads are always PROJECT-scoped (there is no
// execution at edit time, and project files persist without a keep flag), so
// `begin` names the project explicitly; every later verb is key-addressed and
// carries only the acting tenant (the key itself names project + tenant, and
// the broker re-checks they match).

/// `POST /v1/storage/admin/upload/begin`: start an ASSET upload for
/// `tenant`/`project` (the pre-build sync publishing a source-referenced
/// media file). Same size semantics as `UploadBeginRequest`; `content_hash`
/// is the sha256 that becomes the key id (content-addressed). The admin
/// surface uploads ONLY the asset plane: every other scope is written by
/// workers through the data path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminUploadBeginRequest {
    pub tenant: String,
    pub project: String,
    pub mime_type: String,
    pub filename: String,
    #[serde(default)]
    pub declared_size: Option<u64>,
    pub content_hash: String,
}

/// The key-addressed admin upload verbs (`parts`/`part-done`/`complete`/
/// `resume`/`abort`): the acting tenant wrapping the same worker envelope the
/// data path uses, so the two surfaces cannot drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenanted<T> {
    pub tenant: String,
    #[serde(flatten)]
    pub inner: T,
}

/// `POST /v1/storage/admin/list-prefix`: the files under one scope-boundary
/// prefix (the pre-build sync's asset diff over `<tenant>/asset/<project>/`).
/// The broker validates the prefix with the same scope-boundary grammar as a
/// wipe, so it can only ever range one owner's space.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPrefixRequest {
    pub prefix: String,
}

/// `POST /v1/storage/admin/sweep-exec`: terminate-sweep one color's un-kept
/// exec files (crashed uploads reaped; completed files stamped to expire
/// after the post-run linger).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepExecRequest {
    pub tenant: String,
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepExecResponse {
    /// Rows removed outright (crashed/abandoned uploads under the color).
    pub swept: u64,
    /// Completed un-kept files stamped with the post-run linger expiry
    /// (deleted by the expiry sweep once it passes).
    pub lingering: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filename_from_url_extracts_last_segment() {
        assert_eq!(filename_from_url("https://x.com/a/b/clip.ogg"), "clip.ogg");
        assert_eq!(filename_from_url("https://x.com/a/b/clip.ogg?token=1"), "clip.ogg");
        assert_eq!(filename_from_url("https://x.com/a/b/clip.ogg#frag"), "clip.ogg");
        assert_eq!(filename_from_url("https://x.com/dir/"), "dir");
        assert_eq!(filename_from_url("https://x.com"), "x.com");
    }

    #[test]
    fn normalize_content_type_strips_charset_and_falls_back() {
        assert_eq!(normalize_content_type(Some("image/png")), "image/png");
        assert_eq!(normalize_content_type(Some("image/png; charset=binary")), "image/png");
        assert_eq!(normalize_content_type(Some("text/plain ; charset=utf-8")), "text/plain");
        assert_eq!(normalize_content_type(Some("")), "application/octet-stream");
        assert_eq!(normalize_content_type(None), "application/octet-stream");
    }

    // Layer 2: wire-shape round-trips for every cross-process type.

    #[test]
    fn admin_upload_envelopes_round_trip() {
        // The `Tenanted<T>` wrapper flattens the inner worker envelope, so the
        // wire shape is one flat object: `{tenant, key, sizes}` for parts, etc.
        let parts = Tenanted {
            tenant: "alice".to_string(),
            inner: UploadPartsRequest { key: "alice/project/p1/f".into(), sizes: vec![5, 3] },
        };
        let v = serde_json::to_value(&parts).unwrap();
        assert_eq!(
            v,
            json!({"tenant": "alice", "key": "alice/project/p1/f", "sizes": [5, 3]})
        );
        let back: Tenanted<UploadPartsRequest> = serde_json::from_value(v).unwrap();
        assert_eq!(back.tenant, "alice");
        assert_eq!(back.inner.sizes, vec![5, 3]);

        let begin = AdminUploadBeginRequest {
            tenant: "alice".into(),
            project: "p1".into(),
            mime_type: "image/png".into(),
            filename: "x.png".into(),
            declared_size: Some(8),
            content_hash: "a".repeat(64),
        };
        let v = serde_json::to_value(&begin).unwrap();
        assert_eq!(
            v,
            json!({"tenant": "alice", "project": "p1", "mime_type": "image/png",
                   "filename": "x.png", "declared_size": 8,
                   "content_hash": "a".repeat(64)})
        );
        let back: AdminUploadBeginRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.project, "p1");
        // declared_size is optional on the wire; content_hash is not (the
        // admin surface uploads only the content-addressed asset plane).
        let min: AdminUploadBeginRequest = serde_json::from_value(json!({
            "tenant": "t", "project": "p", "mime_type": "a/b", "filename": "f",
            "content_hash": "b".repeat(64)
        }))
        .unwrap();
        assert_eq!(min.declared_size, None);
    }

    #[test]
    fn stored_file_value_round_trip() {
        let m = StoredFile {
            key: "exec/0188-color/9f3a".into(),
            mime_type: "audio/ogg".into(),
            size_bytes: 4_200_000,
            filename: "clip.ogg".into(),
        };
        let v = m.to_value();
        // Exact wire shape: the CONCRETE marker (audio/ogg -> __weft_audio__),
        // payload with camelCase keys, NO url.
        assert_eq!(
            v,
            json!({"__weft_audio__": {
                "key": "exec/0188-color/9f3a",
                "mimeType": "audio/ogg",
                "sizeBytes": 4_200_000u64,
                "filename": "clip.ogg",
            }})
        );
        assert_eq!(StoredFile::from_value(&v).unwrap(), m);
        // A non image/video/audio mime takes the Blob marker.
        let pdf = StoredFile {
            key: "exec/c/x".into(),
            mime_type: "application/pdf".into(),
            size_bytes: 10,
            filename: "x.pdf".into(),
        };
        assert!(pdf.to_value().get("__weft_blob__").is_some());
        assert_eq!(StoredFile::from_value(&pdf.to_value()).unwrap(), pdf);
    }

    #[test]
    fn stored_file_value_is_typed_by_its_marker() {
        let m = StoredFile {
            key: "exec/c/1".into(),
            mime_type: "audio/ogg".into(),
            size_bytes: 1,
            filename: "a.ogg".into(),
        };
        let t = crate::weft_type::WeftType::infer(&m.to_value());
        assert_eq!(
            t,
            crate::weft_type::WeftType::Primitive(crate::weft_type::WeftPrimitive::Audio)
        );
    }

    /// Deserializing a FileHandle parses a FILE VALUE via `from_value`,
    /// which is what makes `ctx.get::<FileHandle>(..)` work: key-backed
    /// and url-backed markers and bare key strings parse; a data-backed
    /// or non-file value fails loud through serde.
    #[test]
    fn file_handle_deserializes_through_from_value() {
        let key_backed = json!({"__weft_image__": {
            "key": "k1", "mimeType": "image/png", "sizeBytes": 3, "filename": "a.png"
        }});
        assert_eq!(
            serde_json::from_value::<FileHandle>(key_backed).unwrap(),
            FileHandle::Key("k1".into())
        );
        let url_backed = json!({"__weft_image__": {
            "url": "https://x.com/a.png", "mimeType": "image/png", "sizeBytes": 3, "filename": "a.png"
        }});
        assert!(matches!(
            serde_json::from_value::<FileHandle>(url_backed).unwrap(),
            FileHandle::Url { url, .. } if url == "https://x.com/a.png"
        ));
        assert_eq!(
            serde_json::from_value::<FileHandle>(json!("bare-key")).unwrap(),
            FileHandle::Key("bare-key".into())
        );
        let data_backed = json!({"__weft_image__": {
            "data": "aGk=", "mimeType": "image/png", "sizeBytes": 2, "filename": "a.png"
        }});
        let e = serde_json::from_value::<FileHandle>(data_backed).unwrap_err().to_string();
        assert!(e.contains("no readable handle"), "{e}");
        assert!(serde_json::from_value::<FileHandle>(json!(42)).is_err());
    }

    #[test]
    fn from_value_rejects_non_marker_and_url_file() {
        // No stored-file marker at all.
        assert!(StoredFile::from_value(&json!({"key": "x"})).is_err());
        // A concrete marker IS present, but the payload is url-backed (no
        // storage key): a valid file value, not a stored reference, so
        // acting on it (get/delete) must error loud.
        let url_file = json!({"__weft_image__": {"url": "https://x", "mimeType": "image/png"}});
        assert!(StoredFile::from_value(&url_file).is_err());
    }

    #[test]
    fn file_handle_parses_key_and_url_forms() {
        // A bare key string is a Key handle.
        assert_eq!(
            FileHandle::from_value(&json!("project/p/1")).unwrap(),
            FileHandle::Key("project/p/1".into())
        );
        assert!(FileHandle::from_value(&json!("")).is_err());

        // A key-backed marker is a Key handle (the common case).
        let key_file = StoredFile {
            key: "shared/team/2".into(),
            mime_type: "application/pdf".into(),
            size_bytes: 9,
            filename: "d.pdf".into(),
        };
        assert_eq!(
            FileHandle::from_value(&key_file.to_value()).unwrap(),
            FileHandle::Key("shared/team/2".into())
        );

        // A url-backed marker is a Url handle, carrying the declared metadata.
        let url_file = json!({"__weft_image__": {
            "url": "https://ex.com/a.png",
            "mimeType": "image/png",
            "filename": "a.png",
            "sizeBytes": 1234
        }});
        assert_eq!(
            FileHandle::from_value(&url_file).unwrap(),
            FileHandle::Url {
                url: "https://ex.com/a.png".into(),
                mime_type: "image/png".into(),
                filename: "a.png".into(),
                size_bytes: 1234,
            }
        );

        // A url marker with only the handle + mime derives a filename from the
        // URL and defaults size to 0 (both display-only; the fetch is truth).
        let bare = json!({"__weft_blob__": {"url": "https://ex.com/d/x.bin", "mimeType": "application/octet-stream"}});
        assert_eq!(
            FileHandle::from_value(&bare).unwrap(),
            FileHandle::Url {
                url: "https://ex.com/d/x.bin".into(),
                mime_type: "application/octet-stream".into(),
                filename: "x.bin".into(),
                size_bytes: 0,
            }
        );

        // A data-inline marker has no readable handle: fail loud, never read wrong.
        let data_file = json!({"__weft_image__": {"data": "aGVsbG8=", "mimeType": "image/png"}});
        assert!(FileHandle::from_value(&data_file).is_err());
        // Not a file value at all.
        assert!(FileHandle::from_value(&json!({"key": "x"})).is_err());
        // An empty url is refused.
        assert!(FileHandle::from_value(&json!({"__weft_blob__": {"url": ""}})).is_err());
    }

    #[test]
    fn keep_and_presign_request_wire_shapes() {
        // Worker -> broker envelopes: exact wire, both directions.
        let keep = KeepRequest { key: "t/exec/c/1".into(), ttl: KeepTtl::Secs { secs: 60 } };
        let v = serde_json::to_value(&keep).unwrap();
        assert_eq!(v, json!({"key": "t/exec/c/1", "ttl": {"kind": "secs", "secs": 60}}));
        let back: KeepRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.key, keep.key);
        assert_eq!(back.ttl, keep.ttl);

        let presign = PresignRequest { key: "t/project/p/1".into(), ttl_secs: Some(900) };
        let v = serde_json::to_value(&presign).unwrap();
        assert_eq!(v, json!({"key": "t/project/p/1", "ttl_secs": 900}));
        let back: PresignRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.key, presign.key);
        assert_eq!(back.ttl_secs, presign.ttl_secs);
    }

    #[test]
    fn admin_envelope_wire_shapes() {
        // Dispatcher admin-proxy <-> broker envelopes: the camelCase renames are
        // the wire contract the CLI download handshake also reads verbatim.
        let r = PresignResult { url: "https://b/x".into(), filename: "a.pdf".into(), size_bytes: 9 };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v, json!({"url": "https://b/x", "filename": "a.pdf", "sizeBytes": 9}));
        let back: PresignResult = serde_json::from_value(v).unwrap();
        assert_eq!(back.size_bytes, 9);

        let u = TenantUsage { stored_bytes: 42, file_count: 3 };
        let v = serde_json::to_value(&u).unwrap();
        assert_eq!(v, json!({"storedBytes": 42, "fileCount": 3}));
        let back: TenantUsage = serde_json::from_value(v).unwrap();
        assert_eq!((back.stored_bytes, back.file_count), (42, 3));

        assert_eq!(
            serde_json::to_value(TenantScopeRequest { tenant: "t1".into() }).unwrap(),
            json!({"tenant": "t1"})
        );
        assert_eq!(
            serde_json::to_value(WipePrefixRequest { prefix: "t1/exec/c/".into() }).unwrap(),
            json!({"prefix": "t1/exec/c/"})
        );
        assert_eq!(serde_json::to_value(WipePrefixResponse { wiped: 2 }).unwrap(), json!({"wiped": 2}));
        assert_eq!(
            serde_json::to_value(SweepExecRequest { tenant: "t1".into(), color: "c1".into() }).unwrap(),
            json!({"tenant": "t1", "color": "c1"})
        );
        assert_eq!(
            serde_json::to_value(SweepExecResponse { swept: 1, lingering: 2 }).unwrap(),
            json!({"swept": 1, "lingering": 2})
        );
    }

    #[test]
    fn scope_wire_shape() {
        assert_eq!(
            serde_json::to_value(StorageScope::Execution).unwrap(),
            json!({"kind": "execution"})
        );
        assert_eq!(
            serde_json::to_value(StorageScope::Shared { name: "team".into() }).unwrap(),
            json!({"kind": "shared", "name": "team"})
        );
        assert_eq!(
            serde_json::to_value(StorageScope::Asset).unwrap(),
            json!({"kind": "asset"})
        );
        let s: StorageScope = serde_json::from_value(json!({"kind": "project"})).unwrap();
        assert_eq!(s, StorageScope::Project);
    }

    #[test]
    fn keep_ttl_wire_shape() {
        assert_eq!(
            serde_json::to_value(KeepTtl::Secs { secs: 60 }).unwrap(),
            json!({"kind": "secs", "secs": 60})
        );
        assert_eq!(serde_json::to_value(KeepTtl::Never).unwrap(), json!({"kind": "never"}));
        let t: KeepTtl = serde_json::from_value(json!({"kind": "default"})).unwrap();
        assert_eq!(t, KeepTtl::Default);
    }

    #[test]
    fn stored_file_meta_wire_shape() {
        let m = StoredFileMeta {
            key: "exec/c/1".into(),
            mime_type: "video/mp4".into(),
            size_bytes: 5,
            filename: "v.mp4".into(),
            keep: true,
            expires_at_unix: Some(1_700_000_000),
            keep_ttl_secs: Some(86_400),
            created_at_unix: 1_600_000_000,
        };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["mimeType"], "video/mp4");
        assert_eq!(v["expiresAtUnix"], 1_700_000_000);
        let back: StoredFileMeta = serde_json::from_value(v).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn upload_envelopes_wire_shape() {
        // begin: declared_size and keep are optional on the wire.
        let b: UploadBeginRequest = serde_json::from_value(json!({
            "scope": {"kind": "execution"},
            "mime_type": "image/png",
            "filename": "a.png",
        }))
        .unwrap();
        assert_eq!(b.declared_size, None);
        assert!(b.keep.is_none());
        let b2: UploadBeginRequest = serde_json::from_value(json!({
            "scope": {"kind": "project"},
            "mime_type": "image/png",
            "filename": "a.png",
            "keep": {"kind": "never"},
            "declared_size": 42,
        }))
        .unwrap();
        assert_eq!(b2.declared_size, Some(42));

        let r = UploadBeginResponse {
            key: "t/exec/c/1".into(),
            part_size: 8 << 20,
            already_stored: false,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            json!({"key": "t/exec/c/1", "part_size": 8_388_608u64, "already_stored": false})
        );
        // `already_stored` defaults false on the wire, so a begin reply
        // without the field parses as a fresh reservation.
        let back: UploadBeginResponse =
            serde_json::from_value(json!({"key": "t/exec/c/1", "part_size": 1})).unwrap();
        assert!(!back.already_stored);

        let p = PresignedPart { part_number: 2, size_bytes: 5, url: "u".into() };
        let v = serde_json::to_value(UploadPartsResponse { parts: vec![p.clone()] }).unwrap();
        assert_eq!(v, json!({"parts": [{"part_number": 2, "size_bytes": 5, "url": "u"}]}));

        let d: PartDoneRequest = serde_json::from_value(json!({
            "key": "k", "part_number": 1, "etag": "\"abc\"",
        }))
        .unwrap();
        // The etag survives verbatim, quotes included.
        assert_eq!(d.etag, "\"abc\"");

        let res = UploadResumeResponse { part_size: 8, missing: vec![p] };
        let back: UploadResumeResponse =
            serde_json::from_value(serde_json::to_value(&res).unwrap()).unwrap();
        assert_eq!(back.missing.len(), 1);
        assert_eq!(back.part_size, 8);
    }

    #[tokio::test]
    async fn bytes_stream_round_trip() {
        let b = bytes::Bytes::from_static(b"hello");
        let collected = collect_stream(bytes_stream(b.clone())).await.unwrap();
        assert_eq!(collected, b);
    }
}
