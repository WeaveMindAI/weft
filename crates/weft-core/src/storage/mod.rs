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
//! the full tenant-local storage path (`run/<run_id>/<id>`,
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
#[cfg(feature = "runtime")]
pub mod diff;
#[cfg(feature = "runtime")]
pub mod media;
/// The upload slot: a page's visitor uploading a file straight to storage,
/// and the route that receives it.
#[cfg(feature = "runtime")]
pub mod slot;

/// The most one port value may weigh on a wire, as JSON. Bytes belong
/// in storage (a stored-file value is a few hundred bytes whatever the
/// file weighs); a value over this fails the node that emitted it. The
/// broker's journal write bounds one recorded event with headroom over
/// this, so the node's refusal is the one a user ever sees.
pub const MAX_WIRE_VALUE_BYTES: usize = 100 * 1024;

/// The response header marking a 409 that means "this upload is being
/// completed by someone right now": any change to it is refused, and the
/// file is about to land. The broker sets it on every such refusal and the
/// dispatcher passes it through, so a client tells "ask again shortly"
/// apart from a conflict that will not resolve on its own.
// SYNC: COMPLETING_HEADER <-> packages/weft-connect/src/core/upload.ts COMPLETING_HEADER
pub const COMPLETING_HEADER: &str = "x-weft-completing";

/// What `value` weighs on a wire: its compact JSON, in bytes. The one
/// measure [`check_wire_value`] holds to [`MAX_WIRE_VALUE_BYTES`], so a
/// node that bounds its own output with it can never disagree with the
/// refusal. A value that does not serialize weighs `usize::MAX`.
pub fn wire_weight<T: Serialize + ?Sized>(value: &T) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |v| v.len())
}

/// The longest start of `text`, cut between two characters, that
/// weighs at most `budget` on a wire as a JSON string ([`wire_weight`]:
/// its UTF-8 bytes, the two quotes, and every escape JSON writes, so a
/// newline counts two). For a node that keeps a text under the limit
/// itself and says it cut it. A budget below 2, the weight of the empty
/// string, has no answer and is the caller's bug: it panics.
pub fn cut_to_weight(text: &str, budget: usize) -> &str {
    assert!(budget >= 2, "cut_to_weight: a budget of {budget} bytes holds not even the empty string's two quotes");
    if wire_weight(text) <= budget {
        return text;
    }
    // Every place `text` can be cut, the empty start included.
    let ends: Vec<usize> = text.char_indices().map(|(i, _)| i).chain(std::iter::once(text.len())).collect();
    let fits = |i: usize| wire_weight(&text[..ends[i]]) <= budget;
    // The weight only grows with the length, so a binary search finds
    // the last cut that fits: `lo` fits (or is the empty start), `hi`
    // does not.
    let (mut lo, mut hi) = (0, ends.len() - 1);
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        if fits(mid) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    &text[..ends[lo]]
}

/// `text` cut to at most `max_chars` characters, and further, with
/// [`cut_to_weight`], to what one port carries when even that weighs
/// more than a wire takes, with whether anything was cut. The shape of
/// a node's text setting like `maxChars` paired with a `truncated`
/// output.
pub fn cap_text(text: &str, max_chars: usize) -> (&str, bool) {
    let by_chars = match text.char_indices().nth(max_chars) {
        Some((i, _)) => &text[..i],
        None => text,
    };
    let cut = cut_to_weight(by_chars, MAX_WIRE_VALUE_BYTES);
    (cut, cut.len() < text.len())
}

/// A weight said the way every refusal over the limit says it: "117
/// KB, and a wire carries at most 100 KB". A refusal puts it after its
/// own subject ("port 'x' of 'y' carries ...", "the rows weigh ...")
/// and adds its own fix.
pub fn wire_weight_said(bytes: usize) -> String {
    format!("{} KB, and a wire carries at most {} KB", bytes.div_ceil(1024), MAX_WIRE_VALUE_BYTES / 1024)
}

/// When `value` weighs more than a wire carries, its weight said
/// ([`wire_weight_said`]); `None` when it fits.
pub fn over_wire<T: Serialize + ?Sized>(value: &T) -> Option<String> {
    let bytes = wire_weight(value);
    (bytes > MAX_WIRE_VALUE_BYTES).then(|| wire_weight_said(bytes))
}

/// Where a page of a list ends: the items from `offset` (past the end
/// is an empty page at the end) that one wire carries as a list, at
/// most `max` of them. `Err(index)` when the page's first item, at
/// `index`, weighs more than a wire on its own: no page can hold it,
/// so the caller decides, once, whether to refuse it or cut it. `max`
/// is at least 1: an empty page would never move a cursor on, so a
/// page size of 0 is the caller's bug and panics (a node refuses such a
/// setting where it reads it).
pub fn wire_page(items: &[Value], offset: usize, max: usize) -> Result<std::ops::Range<usize>, usize> {
    assert!(max >= 1, "wire_page: a page of at most 0 items never moves on");
    let start = offset.min(items.len());
    // The list's own brackets, then each item and the comma before it.
    let mut total = 2;
    let mut end = start;
    for item in items[start..].iter().take(max) {
        let added = wire_weight(item) + usize::from(end > start);
        if total + added > MAX_WIRE_VALUE_BYTES {
            break;
        }
        total += added;
        end += 1;
    }
    if end == start && start < items.len() {
        return Err(start);
    }
    Ok(start..end)
}

/// Whether `value` may travel a wire from port `port` of node `node`:
/// the refusal names the port, the weight, the limit, and the two ways
/// out, since what fits depends on what the value is.
pub fn check_wire_value(node: &str, port: &str, value: &Value) -> Result<(), String> {
    let Some(weight) = over_wire(value) else {
        return Ok(());
    };
    Err(format!(
        "port '{port}' of '{node}' carries {weight}. Return less: one page of it with where the next \
         one starts, or a part that says it was cut. Or store the data and send the stored file \
         instead, whose value weighs a few hundred bytes whatever the file holds"
    ))
}

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

/// Wrap an HTTP response body as a [`ByteStream`], so a node can pipe
/// an authenticated download straight into `StorageHandle::put_stream`
/// without buffering it (the connection-client twin of
/// `put_from_url`, which only covers unauthenticated fetches).
#[cfg(feature = "runtime")]
pub fn response_stream(resp: reqwest::Response) -> ByteStream {
    use futures::StreamExt;
    Box::pin(resp.bytes_stream().map(|chunk| chunk.map_err(std::io::Error::other)))
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
        // The files a version snapshot publishes: program text, node
        // source, manifests. Text to the store, whatever the extension.
        Some("weft" | "toml" | "rs" | "sql" | "md" | "py" | "sh") => "text/plain",
        _ => "application/octet-stream",
    }
}

/// The url-form file value for an external media ref (`@asset("https://…",
/// Image)`, which the editor's paste-URL writes): a marker of the kind the
/// ref declares (an `Image` ref is `__weft_image__` whatever the extension
/// suggests; the compiler refuses a ref that leaves the kind open) whose
/// payload's handle is the URL. Size is unknown until fetched (0, display
/// only); the worker's fetch reports the real Content-Type at run time.
pub fn url_file_value(url: &str, kind: crate::weft_type::FileKind) -> Value {
    let filename = filename_from_url(url);
    let mime = mime_from_filename(&filename);
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

/// The filename a response names for itself, out of its
/// `Content-Disposition` header: `attachment; filename="voice.ogg"`,
/// quoted or bare, and the RFC 5987 `filename*=UTF-8''voice.ogg`
/// form. `None` when the header is absent or names nothing usable.
///
/// A server that says what its file is called knows better than the
/// URL does (`/media/3EB0C767` is an id, not a name), and a file that
/// keeps its extension is the difference between a document arriving
/// as `voice.ogg` and arriving as an unnamed blob. Any path separator
/// is dropped: the name is a label, never a place to write.
pub fn filename_from_disposition(header: &str) -> Option<String> {
    let mut best: Option<String> = None;
    for part in header.split(';') {
        let part = part.trim();
        // `inline` / `attachment` carry no `=`; they are not the name.
        let Some((key, value)) = part.split_once('=') else { continue };
        let (key, value) = (key.trim().to_ascii_lowercase(), value.trim());
        let value: std::borrow::Cow<str> = match key.as_str() {
            // The extended form carries a charset and a language before
            // the name, and the name percent-encoded; only the name is
            // wanted, as it was written.
            "filename*" => {
                let encoded = value.rsplit('\'').next().unwrap_or(value);
                match percent_encoding::percent_decode_str(encoded).decode_utf8() {
                    Ok(name) => name,
                    Err(_) => continue,
                }
            }
            "filename" if best.is_none() => value.into(),
            _ => continue,
        };
        let value = value.trim().trim_matches('"');
        let name = value.rsplit(['/', '\\']).next().unwrap_or(value).trim();
        if !name.is_empty() && name != "." && name != ".." {
            let owned = name.to_string();
            if key == "filename*" {
                return Some(owned);
            }
            best = Some(owned);
        }
    }
    best
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

/// Where a file is stored, which is how long it lives. A scope never
/// says who may read a file: any run of any project of the tenant reads
/// any file for as long as it exists. The scope picks the prefix, the
/// caller's verified identity picks the values inside it (its own run /
/// project). A deadline (a keep with a time, [`KeepTtl`]) deletes a file
/// in any scope when it arrives; its owner being deleted deletes it too,
/// whichever comes first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StorageScope {
    /// `run/<run_id>/`: deleted with its run, when the run's record is
    /// deleted (its keep time running out, `weft clean`, `weft rm`), never
    /// at the run's end. The default.
    #[default]
    Run,
    /// `project/<project_id>/`: deleted with its project (`weft rm`).
    Project,
    /// `shared/<name>/`: a space the tenant's projects meet in by naming
    /// the same `name`. It belongs to no run or project, so it is deleted
    /// only by hand or at its deadline.
    Shared { name: String },
    /// `asset/<sha256>`: the project's PUBLISHED ASSETS, named by content, the storage
    /// copies of files the source references via `@asset`. Current source
    /// references protect their copies; replaced/removed ones receive the
    /// default access-renewed TTL (content-hash ids). Workers READ this
    /// scope like project scope; the worker data path refuses writes to it.
    Asset,
    /// `instance/<project_id>/<instance>/`: one instance's files in this
    /// project. They are deleted by hand (forgetting the instance, `weft
    /// files rm`), at their deadline, or with the project they live in. `of` names the instance; absent, the run's own
    /// instance (an error in a run for no instance).
    Instance {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        of: Option<crate::instance::InstanceId>,
    },
}


impl StorageScope {
    /// The run's own instance's files (`StorageScope::Instance { of: None }`).
    pub fn instance() -> Self {
        StorageScope::Instance { of: None }
    }

    /// One instance's files in this project, named by id.
    pub fn instance_of(instance: crate::instance::InstanceId) -> Self {
        StorageScope::Instance { of: Some(instance) }
    }

    /// The scope a node's `scope` setting names, as a program writes it:
    /// `run`, `project`, or `instance` (the run's own). The one reading of
    /// those words, for the nodes that offer the setting; `None` for any
    /// other word.
    pub fn named(word: &str) -> Option<Self> {
        match word {
            "run" => Some(StorageScope::Run),
            "project" => Some(StorageScope::Project),
            "instance" => Some(StorageScope::instance()),
            _ => None,
        }
    }
}

/// A file's deadline, in any scope but the asset one: it is deleted when
/// the deadline arrives, or with its owner (its run, its project) if that
/// comes first. Every access bumps the deadline back to now + TTL, so
/// actively-used files never reach it. `Default` resolves to
/// [`DEFAULT_KEEP_TTL_SECS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KeepTtl {
    /// [`DEFAULT_KEEP_TTL_SECS`] (30 days), access-bumped.
    Default,
    /// now + this many seconds, access-bumped.
    Secs { secs: u64 },
    /// No deadline: the file lives as long as its scope says.
    Never,
}

/// What [`KeepTtl::Default`] resolves to: 30 days, renewed by every
/// access. Also the countdown a retired asset starts on.
pub const DEFAULT_KEEP_TTL_SECS: u64 = 30 * 24 * 3600;

impl KeepTtl {
    /// The lifetime in seconds, `None` for [`KeepTtl::Never`].
    pub fn secs(self) -> Option<u64> {
        match self {
            KeepTtl::Never => None,
            KeepTtl::Default => Some(DEFAULT_KEEP_TTL_SECS),
            KeepTtl::Secs { secs } => Some(secs),
        }
    }
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
    /// Unix seconds at which this file's deadline arrives, renewed on
    /// access. `None` = no deadline (it lives as long as its scope says:
    /// a current source asset, a file stored with no lifetime,
    /// `KeepTtl::Never`).
    #[serde(rename = "expiresAtUnix")]
    pub expires_at_unix: Option<i64>,
    /// The file's TTL in seconds, so an access can recompute
    /// `expires_at = now + ttl`. `None` when there is no expiry.
    #[serde(rename = "keepTtlSecs")]
    pub keep_ttl_secs: Option<u64>,
    #[serde(rename = "createdAtUnix")]
    pub created_at_unix: i64,
    /// Which write of the file's content this is: 1 when made, one more
    /// after every replacement (see [`StoredFile::version`]).
    pub version: u64,
}

/// The version a file starts at, and the one a stored-file value written
/// before files carried versions stands for: every file stored then was
/// given version 1 when the version column arrived.
pub const FIRST_FILE_VERSION: u64 = 1;

fn first_file_version() -> u64 {
    FIRST_FILE_VERSION
}

/// The self-describing stored-file reference: the payload INSIDE a
/// concrete `__weft_<kind>__` marker (see `to_value`). The ONLY thing
/// that flows on edges / into the journal for a stored file. Carries
/// NO url: byte access always goes through an authenticated fetch (or
/// an expiring link a node deliberately asks for, `internal_link`,
/// `container_link` or `external_link`). The
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
    /// Which write of the file's content this value names. A key is
    /// never reused and every change of the bytes bumps the version, so
    /// (key, version) is one exact content: what an edit compares
    /// before it writes, and what a re-run that reuses a node's output
    /// checks the file still holds.
    #[serde(default = "first_file_version")]
    pub version: u64,
    /// How long a link handed out for this file lives, in seconds, when
    /// whoever made the value chose (`StorageHandle::link_lifetime`, the
    /// `LinkOptions` node); absent, [`DEFAULT_LINK_SECS`]. It rides the
    /// value, not the stored file: two values of one file may hand out
    /// links of different lives.
    #[serde(
        default,
        rename = "linkSecs",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "link_secs_in_range"
    )]
    pub link_secs: Option<u64>,
}

/// A value's `linkSecs` read off the wire, refused when no link can live
/// that long ([`checked_link_secs`]).
fn link_secs_in_range<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(deserializer)?
        .map(checked_link_secs)
        .transpose()
        .map_err(serde::de::Error::custom)
}

/// How long a link to a stored file lives when its value names no
/// lifetime ([`StoredFile::link_secs`]).
pub const DEFAULT_LINK_SECS: u64 = 15 * 60;

/// The longest a link to a stored file lives: seven days, the longest a
/// storage signature allows.
pub const MAX_LINK_SECS: u64 = 7 * 24 * 3600;

/// `secs` when a link can live that long (one second to
/// [`MAX_LINK_SECS`]), else the refusal naming the range. Checked
/// wherever a lifetime enters a value, so a value never carries one
/// that every link made from it would fail on. Zero reads "under a
/// second": a lifetime handed in finer than whole seconds (a `Duration`
/// of half a second) arrives here as 0.
pub fn checked_link_secs(secs: u64) -> Result<u64, String> {
    match secs {
        0 => Err("a link lives 1 second to 7 days, not under a second".to_string()),
        secs if secs <= MAX_LINK_SECS => Ok(secs),
        secs => Err(format!("a link lives 1 second to 7 days, not {secs} seconds")),
    }
}

impl From<&StoredFileMeta> for StoredFile {
    fn from(meta: &StoredFileMeta) -> Self {
        Self {
            key: meta.key.clone(),
            mime_type: meta.mime_type.clone(),
            size_bytes: meta.size_bytes,
            filename: meta.filename.clone(),
            version: meta.version,
            link_secs: None,
        }
    }
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

/// Every stored file a value names, however deep: a file value on its
/// own, inside a list, or a field of a record. A url-backed file is not
/// in storage and is left out.
pub fn stored_files_within(value: &Value) -> Vec<StoredFile> {
    let mut found = Vec::new();
    let mut stack = vec![value];
    while let Some(value) = stack.pop() {
        match value {
            Value::Array(items) => stack.extend(items.iter().rev()),
            Value::Object(fields) => {
                if crate::weft_type::FileKind::from_marker_obj(fields).is_some() {
                    if let Ok(file) = StoredFile::from_value(value) {
                        found.push(file);
                    }
                } else {
                    stack.extend(fields.values().rev());
                }
            }
            _ => {}
        }
    }
    found
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
    /// `link_secs` is the value's own link lifetime
    /// ([`StoredFile::link_secs`]), which every link handed out for it
    /// lives.
    Key { key: String, link_secs: Option<u64> },
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
    /// The handle of the stored file at `key`, handing out links of the
    /// default lifetime.
    pub fn key(key: impl Into<String>) -> Self {
        FileHandle::Key { key: key.into(), link_secs: None }
    }

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
            return Ok(FileHandle::key(s));
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
            let link_secs = match payload.get("linkSecs") {
                None | Some(Value::Null) => None,
                Some(secs) => {
                    let secs = secs.as_u64().ok_or_else(|| {
                        WeftError::Input(format!("file marker's linkSecs is not a whole number of seconds: {secs}"))
                    })?;
                    Some(checked_link_secs(secs).map_err(WeftError::Input)?)
                }
            };
            return Ok(FileHandle::Key { key: key.to_string(), link_secs });
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

/// `POST /v1/storage/identity`: the file stored under `identity` in
/// `scope`, if there is one. What an identified `put_from_url` asks
/// BEFORE fetching, so a source already stored costs no download at
/// all (the begin's own already-stored answer is the race-safe
/// backstop, but by then the bytes are already on their way).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityLookupRequest {
    pub scope: StorageScope,
    pub identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityLookupResponse {
    pub file: Option<StoredFileMeta>,
}

/// A link to a stored file: where to fetch it, and whether the open
/// internet can (a provider handed a link it cannot open gets the bytes
/// in its request instead). What `StorageHandle::internal_link`,
/// `container_link` and `external_link` answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileLink {
    pub url: String,
    pub public: bool,
}

/// The answer to a [`LinkRequest`]: the link, and the record of the file
/// it opens (what a send beside the link names it by).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkAnswer {
    pub link: FileLink,
    pub file: StoredFileMeta,
}

/// Who opens a link to a stored file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkReach {
    /// Node code inside the install: straight to storage, at the address
    /// a worker reaches it by, never through a tunnel.
    Internal,
    /// A project's own container (an infra unit's): at the address the
    /// install's containers reach it by, never through a tunnel. On a
    /// local install that is the install's internal port as a container
    /// reaches it, through the files door there (a container's own
    /// `127.0.0.1` is itself); on an install whose store every container
    /// reaches at the workers' address (a cloud one), the `Internal` link.
    Container,
    /// Anything outside the install's containers (a browser, an app, an
    /// outside service): the project's visitor address, else weft's
    /// tunnel, else the install's own address (`public: false`); on a
    /// cloud install, the store's own link.
    External,
}

/// `POST /v1/storage/link`: a link to one file for `reach`, living
/// `ttl_secs` (the value's `linkSecs`, else [`DEFAULT_LINK_SECS`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkRequest {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
    pub reach: LinkReach,
}

/// `POST /v1/storage/copy`: copy the stored file at `key` into `scope`
/// inside storage. `identity` and `keep` as for an upload's begin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyRequest {
    pub key: String,
    pub scope: StorageScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep: Option<KeepTtl>,
}

/// Who a link the control plane asks for is for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LinkReader {
    /// A person's machine: the CLI, the editor, a browser beside them.
    Person,
    /// weft's own process (the dispatcher reading a version's file).
    Runtime,
    /// Somebody outside the install reaching it as `project`'s callers
    /// do (a browser extension showing a form's file): the link a node's
    /// `external_link` gives.
    Outside { project: String },
}

/// `POST /v1/storage/admin/link`: a download link for one file, for
/// `reader`, answered as a [`PresignResult`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminLinkRequest {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
    pub reader: LinkReader,
}

/// The `Content-Disposition` every link weft hands out for a stored file
/// answers with: a download, under the file's name. Opening a link in a
/// tab never shows the file under the address it came from, whatever its
/// type; an `<img>`, `<video>` or `<audio>` ignores it and still shows
/// or plays the file. The quoted name is for old clients (ASCII only,
/// quotes and backslashes dropped), the encoded one the exact name.
pub fn download_disposition(filename: &str) -> String {
    format!("attachment; {}", disposition_filename(filename))
}

/// `filename="..."; filename*=UTF-8''...` for `filename`, what a
/// `Content-Disposition` names a file by.
pub fn disposition_filename(filename: &str) -> String {
    let plain: String = filename
        .chars()
        .map(|c| if c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\' { c } else { '_' })
        .collect();
    let encoded: String = filename
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'_' | b'~' => (b as char).to_string(),
            other => format!("%{other:02X}"),
        })
        .collect();
    format!("filename=\"{plain}\"; filename*=UTF-8''{encoded}")
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
#[serde(rename_all = "camelCase")]
pub struct UploadBeginRequest {
    pub scope: StorageScope,
    pub mime_type: String,
    pub filename: String,
    #[serde(default)]
    pub keep: Option<KeepTtl>,
    #[serde(default)]
    pub declared_size: Option<u64>,
    /// What this file is a copy OF, when the caller knows (a WhatsApp
    /// message id, a document id at some provider). A begin naming an
    /// identity that is already stored ACTIVE in the same scope
    /// answers `already_stored` with that file's key instead of a
    /// fresh reservation: the same source, fetched twice, is one
    /// file. `None` for a file that is its own thing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
}

/// `POST /v1/storage/upload/replace`: start overwriting the stored file at
/// `key` with new bytes. Answers an [`UploadBeginResponse`] whose `key` is
/// the REPLACEMENT's own upload key: the parts, resume, abort and complete
/// verbs name that key exactly as for a new file, and complete answers the
/// replaced file's value (its own key, its new size). The file keeps its
/// key, scope, name, type and lifetime; only its content changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadReplaceRequest {
    pub key: String,
    #[serde(default)]
    pub declared_size: Option<u64>,
    /// The version the new content was made from (an edit read it
    /// first); refused with 412 when the file has moved on since. `None`
    /// overwrites whatever is there.
    #[serde(default)]
    pub expected_version: Option<u64>,
}

/// One change to a stored file's content, as the run's journal records
/// it for the inspector: which file, from which version to which, and
/// what changed in words a person reads ([`diff::edit_diff`]). Display
/// only: nothing is ever rebuilt from it.
// SYNC: FileEdit <-> packages/weft-graph/src/protocol.ts FileEditWire
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileEdit {
    pub key: String,
    pub filename: String,
    /// The version the change was made from; `None` for an overwrite
    /// that never read the file.
    #[serde(rename = "fromVersion")]
    pub from_version: Option<u64>,
    #[serde(rename = "toVersion")]
    pub to_version: u64,
    pub diff: String,
}

/// How one attempt at replacing a file's content ended. `Stale` and
/// `Busy` change nothing: an edit re-reads on `Stale`, and every writer
/// waits out `Busy` (another write of the same file in flight).
#[derive(Debug, Clone, PartialEq)]
pub enum ReplaceOutcome {
    /// The file now holds the new content, at the version carried.
    Replaced(StoredFile),
    /// The file is not at the version the new content was made from.
    Stale,
    /// Another replacement of the file is in flight.
    Busy,
}

/// Begin response: the minted key + the fixed part size for this upload.
/// Every part the caller reserves must be exactly `part_size` bytes except
/// the final one (which may be smaller and marks the end of the upload).
/// A content-addressed (asset) begin whose content is already stored ACTIVE
/// answers `already_stored: true` with the existing key: there is nothing to
/// transfer, the caller must not reserve parts, and `part_size` is a
/// meaningless 0.
// SYNC: UploadBeginResponse <-> packages/weft-connect/src/core/upload.ts UploadBeginAnswer
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadBeginResponse {
    pub key: String,
    pub part_size: u64,
    #[serde(default)]
    pub already_stored: bool,
    /// This content is already part way up under `key`: carry on with it
    /// (`/storage/upload/resume`) instead of sending it from the start.
    ///
    /// Content-addressed keys only, which is every asset: the key IS the
    /// hash of the bytes, so an upload already in flight under it holds
    /// the same bytes this caller has. Carrying on is safe even while
    /// another uploader is doing the same, because a part is reserved by
    /// NUMBER (see [`UploadPartsRequest`]) and both writers put identical
    /// bytes in it.
    #[serde(default)]
    pub resume: bool,
}

/// `POST /v1/storage/upload/parts`: reserve + presign parts, each NAMED by
/// its number and sized exactly as the caller will upload it.
///
/// The caller names the number rather than asking for "the next one", and
/// that is what makes a reservation idempotent: part 3 of a key is always
/// the same bytes of the same file, so asking for it twice reserves it
/// once. Two publishes of the SAME content (the key is the content's
/// hash, so they are uploading identical bytes) used to each ask for "the
/// next part", between them reserve more parts than the file has, and
/// both fail on a total that no longer added up, leaving the upload
/// unfinishable until the hourly sweep removed it. Now they both ask for
/// part 3, both write the same bytes to it, and whichever completes
/// first is the answer for both.
// SYNC: UploadPartsRequest <-> packages/weft-connect/src/core/upload.ts UploadPartsBody
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPartsRequest {
    pub key: String,
    pub parts: Vec<PartAsk>,
    /// Who PUTs the parts, which their URLs are presigned for.
    #[serde(default, skip_serializing_if = "PartSender::is_caller")]
    pub sender: PartSender,
}

/// Who sends an upload's part bytes, which the store presigns the part
/// URLs for. Left out on the wire, it is [`Self::Caller`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartSender {
    /// Whoever sends the request, at the address its route reaches the
    /// store from: a worker's own, a person's machine for the control
    /// plane's routes (the CLI, the editor).
    #[default]
    Caller,
    /// weft's own process (the dispatcher uploading for itself): the
    /// control plane's routes only, refused anywhere else.
    Runtime,
}

impl PartSender {
    pub fn is_caller(&self) -> bool {
        *self == Self::Caller
    }
}

/// One part a caller is about to upload: which part of the file it is,
/// and how many bytes it will PUT.
// SYNC: PartAsk <-> packages/weft-connect/src/core/upload.ts PartAsk
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartAsk {
    /// 1-based, and it IS the position: part `n` of an upload whose part
    /// size is `p` carries the file's bytes from `(n - 1) * p`.
    pub part_number: i32,
    pub size_bytes: u64,
}

/// One reserved part: its assigned number, its exact byte size, and the
/// presigned URL to PUT exactly that many bytes to.
// SYNC: PresignedPart <-> packages/weft-connect/src/core/upload.ts PresignedPart
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PresignedPart {
    pub part_number: i32,
    pub size_bytes: u64,
    /// Where in the FILE this part's bytes start.
    ///
    /// Stated by the store, because only the store knows the layout: the
    /// part numbers it has already reserved and how big each one is. An
    /// uploader that worked the offset out for itself had to assume the
    /// parts it was handed are one contiguous run at the end of what has
    /// landed, and a resume can hand it a part from the MIDDLE (any part
    /// whose PUT failed while a later one succeeded). Sending the wrong
    /// bytes under the right part number is not caught anywhere
    /// downstream (complete only checks the sizes add up, and the content
    /// hash is taken over what was read locally), so the store would end
    /// up holding a scrambled object under an honest content hash.
    pub offset_bytes: u64,
    pub url: String,
}

// SYNC: UploadPartsResponse <-> packages/weft-connect/src/core/upload.ts UploadPartsAnswer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPartsResponse {
    pub parts: Vec<PresignedPart>,
}

/// `POST /v1/storage/upload/part-done`: report a landed part. The etag is the
/// bucket's response header VERBATIM (quotes included); the server records the
/// part's size from its own reservation, never from the caller.
// SYNC: PartDoneRequest <-> packages/weft-connect/src/core/upload.ts PartDoneBody
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartDoneRequest {
    pub key: String,
    pub part_number: i32,
    pub etag: String,
}

/// `POST /v1/storage/upload/complete`: finalize the upload (all reserved
/// parts must have been reported done). Responds with the stored-file value.
// SYNC: UploadCompleteRequest <-> packages/weft-connect/src/core/upload.ts UploadKeyBody
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCompleteRequest {
    pub key: String,
}

/// `POST /v1/storage/upload/resume`: after an interruption, learn which
/// reserved parts never landed and get fresh URLs for exactly those.
// SYNC: UploadResumeRequest <-> packages/weft-connect/src/core/upload.ts UploadKeyBody
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadResumeRequest {
    pub key: String,
    /// Who PUTs the missing parts, which their URLs are presigned for.
    #[serde(default, skip_serializing_if = "PartSender::is_caller")]
    pub sender: PartSender,
}

// SYNC: UploadResumeResponse <-> packages/weft-connect/src/core/upload.ts UploadResumeAnswer
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadResumeResponse {
    pub part_size: u64,
    pub missing: Vec<PresignedPart>,
    /// How much of the file the store has already carved into parts,
    /// landed and missing together.
    ///
    /// Where the NEW parts begin. `missing` says what still has to be
    /// sent of what was already carved, and this says where the carving
    /// stopped, which are two different lengths whenever a part in the
    /// middle is the missing one. An uploader that started reserving
    /// again from the end of the last part it sent would reserve bytes
    /// the store had already carved, and the upload could then never be
    /// completed (the reserved sizes stop adding up to the declared
    /// size).
    pub reserved_bytes: u64,
}

/// `POST /v1/storage/upload/abort`: cancel an in-flight upload, freeing its
/// quota reservation and discarding any landed parts.
// SYNC: UploadAbortRequest <-> packages/weft-connect/src/core/upload.ts UploadKeyBody
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

/// `POST /v1/storage/keep`: set a file's deadline (or clear it with
/// [`KeepTtl::Never`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeepRequest {
    pub key: String,
    pub ttl: KeepTtl,
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

/// `POST /v1/storage/admin/link` response, returned verbatim as the
/// dispatcher's `/storage/files/download` response (the link IS the
/// download handshake; no separate same-fields struct). Carries the file's
/// friendly name and total size besides the link, so a client names the
/// file and can resume it (`Range`) without a request of its own first.
// SYNC: PresignResult <-> packages/weft-graph/src/protocol.ts StoredFileDownload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignResult {
    pub url: String,
    pub filename: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
    /// The file's version when the link was made: a download that asks
    /// for a fresh link to carry on holds it to the same content.
    pub version: u64,
    /// Whether the open internet opens the link (see [`FileLink`]).
    pub public: bool,
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
// run at edit time), so
// `begin` names the project explicitly; every later verb is key-addressed and
// carries only the acting tenant (the key itself names project + tenant, and
// the broker re-checks they match).

/// Every admin upload verb: the acting tenant wrapping the envelope the
/// other surface uses, so the two cannot drift. `begin` wraps the
/// dispatcher's own [`AssetUploadBeginRequest`] (`POST
/// /v1/storage/admin/upload/begin`: the admin surface uploads ONLY the
/// asset plane, every other scope is written by workers through the data
/// path); the key-addressed verbs (`parts`/`part-done`/`complete`/
/// `resume`/`abort`) wrap the worker envelope of the data path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenanted<T> {
    pub tenant: String,
    #[serde(flatten)]
    pub inner: T,
}

/// Replace the set of the tenant's assets one project references (its
/// source's files and its versions' files). Only the acting tenant's asset
/// keys are accepted. The dispatcher accepts bare scope keys and adds the
/// authenticated tenant before forwarding to storage. An asset no project of
/// the tenant references any more starts a TTL instead of being deleted, so
/// old runs can still read it; an empty set drops this project's references.
///
/// `keys` are this build's own files and must all be there (the build
/// just uploaded them). `kept` are files an older version of the project
/// still names: kept alive when present, reported back when gone, since
/// a version whose blob expired or was removed must not stop the next
/// build from publishing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetReferencesRequest {
    pub project: String,
    pub keys: Vec<String>,
    #[serde(default)]
    pub kept: Vec<String>,
}

/// The broker's answer to an asset references update: the `kept` keys
/// that no longer exist, so the dispatcher can say which version lost
/// a file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssetReferencesResponse {
    pub missing: Vec<String>,
}

// The dispatcher's own `/storage/*` bodies, which the CLI sends. The
// tenant is never in them: the dispatcher takes it from the caller's
// credential.

/// `POST /storage/upload/begin`: an ASSET upload (a version's file, or a
/// media `@asset` the source references) into the caller's tenant.
/// `content_hash` is the sha256 that becomes the key id, so content the
/// tenant already stores, from whichever project, answers "already stored".
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetUploadBeginRequest {
    pub mime_type: String,
    pub filename: String,
    #[serde(default)]
    pub declared_size: Option<u64>,
    pub content_hash: String,
}

/// `POST /storage/import/begin`: one file copied in from another install
/// (`weft migrate files`), into the caller's tenant. It keeps the key it
/// had there, its tenant segment replaced by the caller's, and its
/// version, birth and lifetime, so a stored-file value naming it still
/// does. The rest of the upload is the ordinary multipart one.
// SYNC: FileImportBeginRequest <-> crates/weft-broker/src/runtime_storage.rs admin_import_begin
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileImportBeginRequest {
    /// The key it had on the install it comes from.
    pub key: String,
    pub version: u64,
    pub mime_type: String,
    pub filename: String,
    pub size_bytes: u64,
    pub created_at_unix: i64,
    /// Its lifetime there; `None` for a file that does not expire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep: Option<KeepTtl>,
}

impl FileImportBeginRequest {
    /// The begin that copies the file `meta` describes.
    pub fn of(meta: &StoredFileMeta) -> Self {
        let keep = meta.keep_ttl_secs.map(|secs| KeepTtl::Secs { secs });
        Self {
            key: meta.key.clone(),
            version: meta.version,
            mime_type: meta.mime_type.clone(),
            filename: meta.filename.clone(),
            size_bytes: meta.size_bytes,
            created_at_unix: meta.created_at_unix,
            keep,
        }
    }
}

/// `POST /storage/assets/held`: which of these contents the caller's tenant
/// already stores, so a publish uploads only the rest. Asked by hash rather
/// than by listing, so the answer costs what the publish names, never what
/// the tenant has accumulated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetsHeldRequest {
    pub hashes: Vec<String>,
}

/// The answer to [`AssetsHeldRequest`]: `content hash -> stored key` for
/// every named content the tenant stores whole (an upload still in flight
/// is not held), and for every other one the key its upload is stored
/// under (`unheld`), which a check that uploads nothing stands in with.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssetsHeldResponse {
    pub keys: std::collections::BTreeMap<String, String>,
    pub unheld: std::collections::BTreeMap<String, String>,
}

/// `POST /storage/files/download`: a download link for one of the
/// caller's files, answered as a [`PresignResult`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub key: String,
    /// Download-link lifetime; `None` is the broker's default (about 15
    /// minutes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

/// `DELETE /storage/files`: exactly one of `key` (one file) or `prefix`
/// (a whole space, e.g. `shared/team/` or `run/<run_id>/`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveFilesRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

/// What a `DELETE /storage/files` removed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesRemoved {
    pub removed: u64,
}

/// `POST /v1/storage/admin/delete-run-files`: delete every file of the
/// runs of one tenant whose records were deleted (their run-scoped files go
/// with them), a page of them at once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunFilesRequest {
    pub tenant: String,
    pub run_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunFilesResponse {
    /// The files deleted.
    pub deleted: u64,
}

/// The media type a byte prefix announces itself as, from its signature
/// (the "magic number"): the common image, video and audio containers,
/// plus PDF and zip. `None` when the head matches nothing known, which is
/// what a plain text file or an exotic container look like here; the
/// caller then falls back on the filename. Pure, on the first bytes only;
/// SVG is text and its root element may sit behind a long prolog, so hand
/// in as much of the head as is cheap (the sync keeps a kilobyte aside).
///
/// This is the one place the language looks at BYTES to name a type. The
/// stored MIME of a synced asset comes from here first (a PNG named
/// `.txt` is still `image/png`), and [`received_file`] holds an
/// `@asset` declaration to it; it never picks a marker.
pub fn sniff_mime(head: &[u8]) -> Option<&'static str> {
    let starts = |sig: &[u8]| head.starts_with(sig);
    let at = |offset: usize, sig: &[u8]| head.len() >= offset + sig.len() && &head[offset..offset + sig.len()] == sig;
    // ISO base media (`....ftyp<brand>`): MP4/MOV/M4A/HEIC share the box,
    // the brand says which.
    if at(4, b"ftyp") {
        let brand = &head[8..head.len().min(12)];
        return Some(if brand.starts_with(b"M4A ") || brand.starts_with(b"M4B ") {
            "audio/mp4"
        } else if brand.starts_with(b"heic") || brand.starts_with(b"heix") || brand.starts_with(b"mif1") {
            "image/heic"
        } else if brand.starts_with(b"avif") {
            "image/avif"
        } else if brand.starts_with(b"qt  ") {
            "video/quicktime"
        } else {
            "video/mp4"
        });
    }
    if starts(b"\x89PNG\r\n\x1a\n") {
        return Some("image/png");
    }
    if starts(b"\xff\xd8\xff") {
        return Some("image/jpeg");
    }
    if starts(b"GIF87a") || starts(b"GIF89a") {
        return Some("image/gif");
    }
    if starts(b"BM") {
        return Some("image/bmp");
    }
    if starts(b"II*\0") || starts(b"MM\0*") {
        return Some("image/tiff");
    }
    if starts(b"\0\0\x01\0") {
        return Some("image/x-icon");
    }
    if starts(b"RIFF") {
        if at(8, b"WEBP") {
            return Some("image/webp");
        }
        if at(8, b"AVI ") {
            return Some("video/x-msvideo");
        }
        if at(8, b"WAVE") {
            return Some("audio/wav");
        }
    }
    // SVG is text: an XML prolog or the root element, anywhere in the head.
    let text_head = match std::str::from_utf8(head) {
        Ok(text) => text,
        // The head may end mid-character; read the longest valid prefix.
        Err(e) => std::str::from_utf8(&head[..e.valid_up_to()]).unwrap_or(""),
    };
    let trimmed = text_head.trim_start_matches(['\u{feff}', ' ', '\t', '\r', '\n']);
    if trimmed.starts_with("<svg") || (trimmed.starts_with("<?xml") && text_head.contains("<svg")) {
        return Some("image/svg+xml");
    }
    if starts(b"\x1aE\xdf\xa3") {
        return Some("video/webm");
    }
    if starts(b"FLV\x01") {
        return Some("video/x-flv");
    }
    if starts(b"\x30\x26\xb2\x75\x8e\x66\xcf\x11") {
        return Some("video/x-ms-asf");
    }
    if starts(b"ID3") || (head.len() >= 2 && head[0] == 0xff && (head[1] & 0xe0) == 0xe0) {
        return Some("audio/mpeg");
    }
    if starts(b"OggS") {
        return Some("audio/ogg");
    }
    if starts(b"fLaC") {
        return Some("audio/flac");
    }
    if starts(b"FORM") && (at(8, b"AIFF") || at(8, b"AIFC")) {
        return Some("audio/aiff");
    }
    if starts(b"%PDF") {
        return Some("application/pdf");
    }
    if starts(b"PK\x03\x04") {
        return Some("application/zip");
    }
    None
}

/// What a file handed over for a place declared `declared` (a caller's
/// upload, an `@asset` in source) is stored as: its media type and the
/// kind of file value it becomes, or why the place cannot take it. An
/// `Image` declaration over video bytes is refused naming both kinds,
/// bytes with no known signature cannot be a declared media kind, and a
/// union is held to its file members (`Image | Null` takes only an
/// image, `Media` any of image, video, audio).
///
/// The error names WHAT is wrong and nothing about where it came from:
/// a caller must never be shown a piece of weft syntax their request has
/// nothing to do with. Each caller frames it.
///
/// The stored type is the one the sender stated (`claimed`: a request's
/// `Content-Type`, a `data:` link's), so a `.csv` stays `text/csv`. The
/// first bytes only fill it in when none was stated (two containers hold
/// either video or audio, WebM and MP4, and the declaration picks which),
/// and catch a stated media kind the bytes plainly are not (`image/png`
/// over an mp3). With nothing stated and nothing recognized, the file is
/// `application/octet-stream`. A place that admits `Blob` but not the kind
/// the bytes are takes them as plain bytes when nothing was stated; a
/// stated type the place does not admit is refused. A declaration that is
/// not a file type takes the file as it is (its caller decides).
pub fn received_file(
    bytes: &[u8],
    claimed: Option<&str>,
    declared: &crate::weft_type::WeftType,
) -> Result<(String, crate::weft_type::FileKind), String> {
    use crate::weft_type::FileKind;
    let admitted = declared.admitted_file_kinds();
    let expected = || admitted.iter().map(|k| k.primitive().to_string()).collect::<Vec<_>>().join(" or ");
    let sniffed = sniff_mime(bytes);
    // Every kind the bytes could be: one, or either of a container's two.
    let byte_kinds: Vec<FileKind> = match sniffed {
        Some(mime) if audio_or_video(mime).is_some() => vec![FileKind::Video, FileKind::Audio],
        Some(mime) => vec![FileKind::from_mime(mime)],
        None => Vec::new(),
    };
    // A media place holds the bytes to its kinds whatever was stated.
    if !admitted.is_empty() && !admitted.contains(&FileKind::Blob) && !byte_kinds.iter().any(|k| admitted.contains(k)) {
        return Err(match byte_kinds.first() {
            Some(found) => format!("the bytes are {}, not the {} that was declared", found.primitive(), expected()),
            None => format!(
                "the bytes carry no {} signature this weft knows (declare it Blob to skip the check)",
                expected()
            ),
        });
    }
    // A stated type that names no media kind (`application/octet-stream`,
    // curl's form type) says nothing about bytes that ARE a media kind a
    // place needs: those are read off the bytes, as when nothing was
    // stated. Any other statement stands.
    let stated = claimed.map(str::trim).filter(|mime| !mime.is_empty()).filter(|mime| {
        FileKind::from_mime(mime) != FileKind::Blob
            || admitted.is_empty()
            || admitted.contains(&FileKind::Blob)
            || !byte_kinds.iter().any(|k| admitted.contains(k))
    });
    let mime = match (stated, sniffed) {
        // The media type itself is case-blind (`Image/PNG` is `image/png`):
        // it is stored lower-cased, its parameters as sent. A container
        // stated as the half the place does not take (`video/webm` over a
        // voice clip for an Audio place) is the half it does.
        (Some(stated), _) => {
            let (essence, params) = stated.split_once(';').map_or((stated, ""), |(e, p)| (e, p));
            let essence = essence.trim().to_ascii_lowercase();
            let essence = match sniffed.and_then(audio_or_video) {
                Some(container) if [container.0, container.1].contains(&essence.as_str()) => {
                    container_half(essence, container, &admitted)
                }
                _ => essence,
            };
            if params.is_empty() { essence } else { format!("{essence};{params}") }
        }
        (None, Some(sniffed)) => {
            let fits = |kind: FileKind| admitted.is_empty() || admitted.contains(&kind);
            match audio_or_video(sniffed) {
                Some((_, audio)) if fits(FileKind::Audio) && !fits(FileKind::Video) => audio.to_string(),
                Some((video, _)) if fits(FileKind::Video) => video.to_string(),
                _ if fits(FileKind::from_mime(sniffed)) => sniffed.to_string(),
                _ => OCTET_STREAM.to_string(),
            }
        }
        (None, None) => OCTET_STREAM.to_string(),
    };
    let kind = FileKind::from_mime(&mime);
    if !admitted.is_empty() && !admitted.contains(&kind) {
        return Err(format!("it was sent as {mime}, which is not the {} that was declared", expected()));
    }
    if kind != FileKind::Blob && !byte_kinds.is_empty() && !byte_kinds.contains(&kind) {
        return Err(format!("it was sent as {mime}, but the bytes are {}", byte_kinds[0].primitive()));
    }
    Ok((mime, kind))
}

const OCTET_STREAM: &str = "application/octet-stream";

/// The media type a stored file of type `mime` is handed over as to a
/// place of type `declared`, or why it does not fit. The file's type is
/// already settled (read off its bytes when it was stored,
/// [`received_file`]), so this holds it to the place's file kinds: a place
/// that takes `Blob` takes any file, a media place its own kinds, and a
/// container that holds either video or audio (WebM, MP4) is handed over
/// as the half the place takes. The media type comes back lower-cased as
/// [`received_file`] stores it, its parameters as they were. A
/// declaration that is not a file type is refused: a file has no place
/// there. The refusal names both types and nothing about where the file
/// came from; each caller frames it.
pub fn fit_mime(mime: &str, declared: &crate::weft_type::WeftType) -> Result<String, String> {
    use crate::weft_type::FileKind;
    let admitted = declared.admitted_file_kinds();
    if admitted.is_empty() {
        return Err(format!("the place is declared {declared}, which takes no file"));
    }
    let (essence, params) = mime.split_once(';').map_or((mime, ""), |(e, p)| (e, p));
    let essence = essence.trim().to_ascii_lowercase();
    let essence = match audio_or_video(&essence) {
        Some(container) => container_half(essence, container, &admitted),
        None => essence,
    };
    let kind = FileKind::from_mime(&essence);
    if !admitted.contains(&FileKind::Blob) && !admitted.contains(&kind) {
        let expected = admitted.iter().map(|k| k.primitive().to_string()).collect::<Vec<_>>().join(" or ");
        return Err(format!("{essence} is {}, not the {expected} that was declared", kind.primitive()));
    }
    Ok(if params.is_empty() { essence } else { format!("{essence};{params}") })
}

/// Does a file already handed over as `mime` ([`fit_mime`]'s answer for the
/// place it first went to) fit a place of type `declared` as it is? A
/// handed-over file is one type, so one that fits only as the other half
/// of its container (an `audio/webm` sent on to a Video place) is refused
/// with the type it would need, as a file that does not fit at all is.
pub fn fits_as_handed_over(mime: &str, declared: &crate::weft_type::WeftType) -> Result<(), String> {
    let fitted = fit_mime(mime, declared)?;
    if fitted != mime {
        return Err(format!("it was handed over as {mime}, and the {declared} here takes it only as {fitted}"));
    }
    Ok(())
}

/// The half of `container` (its video and audio types) a file stated as
/// `essence`, one of the two, is held as by a place admitting `admitted`:
/// the stated half, unless the place takes only the other.
fn container_half(
    essence: String,
    (video, audio): (&'static str, &'static str),
    admitted: &[crate::weft_type::FileKind],
) -> String {
    use crate::weft_type::FileKind;
    let kind = FileKind::from_mime(&essence);
    let other = if kind == FileKind::Audio { (FileKind::Video, video) } else { (FileKind::Audio, audio) };
    if !admitted.is_empty() && !admitted.contains(&kind) && admitted.contains(&other.0) {
        other.1.to_string()
    } else {
        essence
    }
}

/// The (video, audio) media types of a container that holds either,
/// read from its type.
pub fn audio_or_video(mime: &str) -> Option<(&'static str, &'static str)> {
    match mime {
        "video/webm" | "audio/webm" => Some(("video/webm", "audio/webm")),
        "video/mp4" | "audio/mp4" => Some(("video/mp4", "audio/mp4")),
        _ => None,
    }
}

#[cfg(test)]
mod sniff_tests {
    use super::{received_file, sniff_mime};

    fn check_declared_kind(declared: &WeftType, head: &[u8]) -> Result<(), String> {
        received_file(head, None, declared).map(|_| ())
    }
    use crate::weft_type::{FileKind, WeftPrimitive, WeftType};

    #[test]
    fn signatures_name_their_media_type() {
        assert_eq!(sniff_mime(b"\x89PNG\r\n\x1a\n...."), Some("image/png"));
        assert_eq!(sniff_mime(b"\xff\xd8\xff\xe0JFIF"), Some("image/jpeg"));
        assert_eq!(sniff_mime(b"RIFF\0\0\0\0WEBPVP8 "), Some("image/webp"));
        assert_eq!(sniff_mime(b"\0\0\0\x18ftypavif\0\0\x02\0"), Some("image/avif"));
        assert_eq!(sniff_mime(b"%PDF-1.7"), Some("application/pdf"));
        assert_eq!(sniff_mime(b"PK\x03\x04"), Some("application/zip"));
        assert_eq!(sniff_mime(b"just text"), None);
    }

    #[test]
    fn an_svg_behind_a_long_prolog_is_still_an_image() {
        let mut head = b"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n".to_vec();
        head.extend_from_slice(b"<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">\n");
        head.extend_from_slice(b"<!-- Generator: a tool that writes a long comment before the root element -->\n");
        head.extend_from_slice(b"<svg xmlns=\"http://www.w3.org/2000/svg\">");
        assert!(head.len() > 64, "the case is a prolog longer than the old window");
        assert_eq!(sniff_mime(&head), Some("image/svg+xml"));
        // A head cut mid-character still reads.
        let mut cut = b"<svg>\xe2\x82".to_vec();
        cut.truncate(7);
        assert_eq!(sniff_mime(&cut), Some("image/svg+xml"));
    }

    #[test]
    fn signatures_name_their_kind() {
        let sniff_file_kind = |head: &[u8]| sniff_mime(head).map(FileKind::from_mime).filter(|k| *k != FileKind::Blob);
        assert_eq!(sniff_file_kind(b"\x89PNG\r\n\x1a\n...."), Some(FileKind::Image));
        assert_eq!(sniff_file_kind(b"\xff\xd8\xff\xe0JFIF"), Some(FileKind::Image));
        assert_eq!(sniff_file_kind(b"RIFF\0\0\0\0WEBPVP8 "), Some(FileKind::Image));
        assert_eq!(sniff_file_kind(b"<svg xmlns=\"http://www.w3.org/2000/svg\">"), Some(FileKind::Image));
        assert_eq!(sniff_file_kind(b"\0\0\0\x18ftypisom\0\0\x02\0"), Some(FileKind::Video));
        assert_eq!(sniff_file_kind(b"\0\0\0\x18ftypM4A \0\0\x02\0"), Some(FileKind::Audio));
        assert_eq!(sniff_file_kind(b"\0\0\0\x18ftypheic\0\0\x02\0"), Some(FileKind::Image));
        assert_eq!(sniff_file_kind(b"\x1aE\xdf\xa3\x01"), Some(FileKind::Video));
        assert_eq!(sniff_file_kind(b"ID3\x04\0\0"), Some(FileKind::Audio));
        assert_eq!(sniff_file_kind(b"RIFF\0\0\0\0WAVEfmt "), Some(FileKind::Audio));
        assert_eq!(sniff_file_kind(b"OggS\0\x02"), Some(FileKind::Audio));
        assert_eq!(sniff_file_kind(b"hello, world"), None);
        assert_eq!(sniff_file_kind(b""), None);
    }

    #[test]
    fn a_declaration_is_held_to_the_bytes() {
        let image = WeftType::primitive(WeftPrimitive::Image);
        assert!(check_declared_kind(&image, b"\x89PNG\r\n\x1a\n").is_ok());
        let e = check_declared_kind(&image, b"ID3\x04").unwrap_err();
        assert!(e.contains("are Audio, not the Image"), "{e}");
        let e = check_declared_kind(&image, b"just text").unwrap_err();
        assert!(e.contains("no Image signature"), "{e}");
        // Blob and text declarations check nothing.
        assert!(check_declared_kind(&WeftType::primitive(WeftPrimitive::Blob), b"just text").is_ok());
        assert!(check_declared_kind(&WeftType::primitive(WeftPrimitive::String), b"ID3").is_ok());
    }

    /// A union is held to its file members: an optional image still
    /// takes only an image, and `Media` takes any media kind but no
    /// unrecognized bytes. `File` admits Blob, so it checks nothing.
    #[test]
    fn a_union_is_held_to_its_file_members() {
        let optional_image = WeftType::parse("Image | Null").unwrap();
        assert!(check_declared_kind(&optional_image, b"\x89PNG\r\n\x1a\n").is_ok());
        let e = check_declared_kind(&optional_image, b"ID3\x04").unwrap_err();
        assert!(e.contains("are Audio, not the Image"), "{e}");
        let media = WeftType::parse("Media").unwrap();
        assert!(check_declared_kind(&media, b"ID3\x04").is_ok());
        let e = check_declared_kind(&media, b"just text").unwrap_err();
        assert!(e.contains("no Image or Video or Audio signature"), "{e}");
        assert!(check_declared_kind(&WeftType::parse("File").unwrap(), b"just text").is_ok());
    }

    /// The bytes decide the stored type; a sender's claim only picks
    /// audio or video in a container that holds either.
    #[test]
    fn a_received_file_keeps_the_type_it_was_sent_as() {
        use crate::weft_type::FileKind;
        let file = WeftType::parse("File").unwrap();
        let png = b"\x89PNG\r\n\x1a\n";
        assert_eq!(received_file(png, Some("image/x-anything"), &file).unwrap(), ("image/x-anything".into(), FileKind::Image));
        assert_eq!(received_file(b"a,b\n1,2", Some("text/csv"), &file).unwrap(), ("text/csv".into(), FileKind::Blob));
        // Nothing stated: the bytes fill it in, else plain bytes.
        assert_eq!(received_file(png, None, &file).unwrap(), ("image/png".into(), FileKind::Image));
        assert_eq!(received_file(b"a,b\n1,2", None, &file).unwrap(), ("application/octet-stream".into(), FileKind::Blob));
        // A generic statement over a picture for an Image place: the
        // bytes decide; an upper-case statement reads as its kind.
        let image = WeftType::parse("Image").unwrap();
        assert_eq!(received_file(png, Some("application/octet-stream"), &image).unwrap(), ("image/png".into(), FileKind::Image));
        assert_eq!(received_file(png, Some("Image/PNG"), &image).unwrap(), ("image/png".into(), FileKind::Image));
        // A stated media kind the bytes plainly are not is refused.
        let e = received_file(b"ID3\x04 an mp3", Some("image/png"), &file).unwrap_err();
        assert!(e.contains("sent as image/png, but the bytes are Audio"), "{e}");
        // A Blob place takes unrecognized or unstated bytes as plain bytes,
        // and refuses a stated type it does not admit.
        let blob = WeftType::parse("Blob").unwrap();
        assert_eq!(received_file(png, None, &blob).unwrap(), ("application/octet-stream".into(), FileKind::Blob));
        let e = received_file(png, Some("image/png"), &blob).unwrap_err();
        assert!(e.contains("sent as image/png, which is not the Blob"), "{e}");
        // WebM holds either: the statement, then the declaration, picks.
        let webm = b"\x1aE\xdf\xa3\x01\x00\x00\x00";
        let audio = WeftType::parse("Audio").unwrap();
        assert_eq!(received_file(webm, Some("audio/webm"), &file).unwrap(), ("audio/webm".into(), FileKind::Audio));
        assert_eq!(received_file(webm, None, &audio).unwrap(), ("audio/webm".into(), FileKind::Audio));
        assert_eq!(received_file(webm, None, &file).unwrap(), ("video/webm".into(), FileKind::Video));
        // Stated as the container's other half, it is the half the place takes.
        assert_eq!(received_file(webm, Some("video/webm"), &audio).unwrap(), ("audio/webm".into(), FileKind::Audio));
        let e = received_file(webm, Some("audio/webm"), &WeftType::parse("Image").unwrap()).unwrap_err();
        assert!(e.contains("not the Image"), "{e}");
    }

    /// The reason says what is wrong with the bytes and nothing about
    /// weft's own syntax: a caller uploading a file must not be shown
    /// `@asset(...)`, which belongs to source they never wrote.
    #[test]
    fn the_reason_carries_no_source_syntax() {
        let image = WeftType::primitive(WeftPrimitive::Image);
        let e = check_declared_kind(&image, b"ID3\x04").unwrap_err();
        assert!(!e.contains("@asset"), "{e}");
    }
}

#[cfg(test)]
mod disposition_tests {
    use super::{download_disposition, filename_from_disposition};

    /// A link is a download under the file's own name: plain for old
    /// clients, exact in the encoded form, and read back to the name.
    #[test]
    fn a_link_downloads_under_the_files_name() {
        let disposition = download_disposition("été a\"b.pdf");
        assert_eq!(disposition, "attachment; filename=\"_t_ a_b.pdf\"; filename*=UTF-8''%C3%A9t%C3%A9%20a%22b.pdf");
        assert_eq!(filename_from_disposition(&disposition), Some("été a\"b.pdf".to_string()));
    }

    /// The name a server gives its own file, in the three shapes a
    /// server writes it, and never a path.
    #[test]
    fn a_server_names_its_file() {
        assert_eq!(
            filename_from_disposition("inline; filename=\"voice.ogg\""),
            Some("voice.ogg".to_string())
        );
        assert_eq!(
            filename_from_disposition("attachment; filename=report.pdf"),
            Some("report.pdf".to_string())
        );
        assert_eq!(
            filename_from_disposition("attachment; filename=\"fallback.txt\"; filename*=UTF-8\'\'real.txt"),
            Some("real.txt".to_string()),
            "the extended form wins"
        );
        assert_eq!(
            filename_from_disposition("inline; filename=\"_t_ _.pdf\"; filename*=UTF-8''%C3%A9t%C3%A9%20%F0%9F%8E%89.pdf"),
            Some("été 🎉.pdf".to_string()),
            "the extended form is percent-decoded"
        );
        assert_eq!(
            filename_from_disposition("attachment; filename=\"../../etc/passwd\""),
            Some("passwd".to_string()),
            "a name is a label, never a path"
        );
        assert_eq!(filename_from_disposition("inline"), None);
        assert_eq!(filename_from_disposition("attachment; filename=\"\""), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_wire_value_is_weighed_as_json_and_refused_over_the_cap() {
        assert!(check_wire_value("n", "out", &json!({"a": "b"})).is_ok());
        let at_cap = json!("x".repeat(MAX_WIRE_VALUE_BYTES - 2));
        assert!(check_wire_value("n", "out", &at_cap).is_ok(), "the cap itself passes");
        let over = json!("x".repeat(MAX_WIRE_VALUE_BYTES));
        let refusal = check_wire_value("pic", "bytes", &over).unwrap_err();
        assert!(refusal.starts_with("port 'bytes' of 'pic' carries 101 KB, and a wire carries at most 100 KB"), "{refusal}");
        let less = refusal.find("Return less: one page of it").expect("the way out any node or script has");
        let stored = refusal.find("send the stored file").expect("the way out for bytes");
        assert!(less < stored, "returning less comes first: {refusal}");
    }

    #[test]
    fn wire_weight_is_the_measure_the_refusal_uses() {
        // `"x...x"` weighs its length and the two quotes.
        let fits = "x".repeat(MAX_WIRE_VALUE_BYTES - 2);
        assert_eq!(wire_weight(fits.as_str()), MAX_WIRE_VALUE_BYTES);
        assert!(check_wire_value("n", "out", &json!(fits)).is_ok());
        let over = "x".repeat(MAX_WIRE_VALUE_BYTES - 1);
        assert_eq!(wire_weight(&json!(over)), MAX_WIRE_VALUE_BYTES + 1);
        assert!(check_wire_value("n", "out", &json!(over)).is_err());
        // A newline is two bytes of JSON, a slice weighs as a list.
        assert_eq!(wire_weight("a\nb"), 6);
        assert_eq!(wire_weight(&[json!(1), json!(2)][..]), 5);
    }

    #[test]
    fn a_cut_lands_between_characters_and_fits_its_budget() {
        // "é" is two bytes: a budget of 5 holds the quotes and one "é",
        // and never half of the next.
        assert_eq!(cut_to_weight("ééé", 5), "é");
        assert_eq!(cut_to_weight("ééé", 6), "éé");
        assert_eq!(cut_to_weight("abc", 100), "abc", "what fits stays whole");
        assert_eq!(cut_to_weight("abc", 2), "", "only the quotes fit");
        assert_eq!(cut_to_weight("a\nb", 5), "a\n", "an escape counts what it weighs");
        let long = "x".repeat(MAX_WIRE_VALUE_BYTES * 2);
        assert_eq!(wire_weight(cut_to_weight(&long, MAX_WIRE_VALUE_BYTES)), MAX_WIRE_VALUE_BYTES);
    }

    #[test]
    fn a_wire_page_stops_at_its_size_and_at_what_a_wire_carries() {
        let small: Vec<Value> = (0..5).map(|i| json!({ "n": i })).collect();
        assert_eq!(wire_page(&small, 0, 2), Ok(0..2), "at most the page size");
        assert_eq!(wire_page(&small, 4, 2), Ok(4..5), "the last page holds what is left");
        assert_eq!(wire_page(&small, 9, 2), Ok(5..5), "past the end is an empty page");
        // Three items of 40 KB: two fit a wire, whatever the page size.
        let heavy: Vec<Value> = (0..3).map(|_| json!({ "a": "x".repeat(40 * 1024) })).collect();
        assert_eq!(wire_page(&heavy, 0, 500), Ok(0..2));
        assert!(wire_weight(&heavy[0..2]) <= MAX_WIRE_VALUE_BYTES);
        assert_eq!(wire_page(&heavy, 2, 500), Ok(2..3));
        // An item no page can hold is named, wherever the page starts.
        let huge = vec![json!(1), json!({ "a": "x".repeat(MAX_WIRE_VALUE_BYTES) })];
        assert_eq!(wire_page(&huge, 1, 500), Err(1));
        assert_eq!(wire_page(&huge, 0, 500), Ok(0..1), "a page before it stops short of it");
    }

    #[test]
    #[should_panic(expected = "not even the empty string")]
    fn a_budget_below_the_empty_string_is_a_caller_bug() {
        cut_to_weight("abc", 1);
    }

    #[test]
    #[should_panic(expected = "never moves on")]
    fn a_page_of_zero_items_is_a_caller_bug() {
        let _ = wire_page(&[json!(1)], 0, 0);
    }

    #[test]
    fn over_wire_says_the_weight_and_the_limit() {
        assert_eq!(over_wire(&json!({ "a": 1 })), None);
        let over = json!("x".repeat(MAX_WIRE_VALUE_BYTES));
        assert_eq!(over_wire(&over).as_deref(), Some("101 KB, and a wire carries at most 100 KB"));
    }

    #[test]
    fn cap_text_counts_characters_then_the_wire_and_says_whether_it_cut() {
        assert_eq!(cap_text("hello", 10), ("hello", false));
        assert_eq!(cap_text("héllo", 3), ("hél", true), "characters, not bytes");
        // Fifty thousand characters of three bytes each: the character
        // cap keeps them all, and the wire is what cuts.
        let heavy = "日".repeat(50_000);
        let (cut, was_cut) = cap_text(&heavy, 50_000);
        assert!(was_cut);
        assert!(wire_weight(cut) <= MAX_WIRE_VALUE_BYTES && wire_weight(cut) + 3 > MAX_WIRE_VALUE_BYTES);
    }

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

    /// Who sends the parts travels as `sender`, left out for the caller
    /// (so the CLI, the editor and workers send what they always sent)
    /// and read back as the caller when missing.
    #[test]
    fn the_part_sender_round_trips_and_defaults_to_the_caller() {
        let parts = UploadPartsRequest { key: "k".into(), parts: vec![], sender: PartSender::Runtime };
        let v = serde_json::to_value(&parts).unwrap();
        assert_eq!(v, json!({"key": "k", "parts": [], "sender": "runtime"}));
        assert_eq!(serde_json::from_value::<UploadPartsRequest>(v).unwrap().sender, PartSender::Runtime);
        let resume = UploadResumeRequest { key: "k".into(), sender: PartSender::Runtime };
        let v = serde_json::to_value(Tenanted { tenant: "alice".into(), inner: resume }).unwrap();
        assert_eq!(v, json!({"tenant": "alice", "key": "k", "sender": "runtime"}));
        let back: Tenanted<UploadResumeRequest> = serde_json::from_value(v).unwrap();
        assert_eq!(back.inner.sender, PartSender::Runtime);

        let caller = UploadResumeRequest { key: "k".into(), sender: PartSender::Caller };
        assert_eq!(serde_json::to_value(&caller).unwrap(), json!({"key": "k"}));
        assert_eq!(serde_json::from_value::<UploadResumeRequest>(json!({"key": "k"})).unwrap().sender, PartSender::Caller);
        assert_eq!(serde_json::from_value::<UploadPartsRequest>(json!({"key": "k", "parts": []})).unwrap().sender, PartSender::Caller);
        assert!(serde_json::from_value::<UploadPartsRequest>(json!({"key": "k", "parts": [], "sender": "person"})).is_err());
    }

    #[test]
    fn admin_upload_envelopes_round_trip() {
        let references = Tenanted {
            tenant: "alice".into(),
            inner: AssetReferencesRequest { project: "p1".into(), keys: vec![], kept: vec![] },
        };
        let value = serde_json::to_value(&references).unwrap();
        assert_eq!(value, json!({"tenant": "alice", "project": "p1", "keys": [], "kept": []}));
        let back: Tenanted<AssetReferencesRequest> = serde_json::from_value(value).unwrap();
        assert!(back.inner.keys.is_empty(), "empty retires the last source asset");
        assert!(serde_json::from_value::<AssetReferencesRequest>(json!({"project": "p1"})).is_err());
        // The `Tenanted<T>` wrapper flattens the inner worker envelope, so the
        // wire shape is one flat object: `{tenant, key, parts}` for parts, etc.
        let parts = Tenanted {
            tenant: "alice".to_string(),
            inner: UploadPartsRequest {
                key: "alice/project/p1/f".into(),
                parts: vec![
                    PartAsk { part_number: 1, size_bytes: 5 },
                    PartAsk { part_number: 2, size_bytes: 3 },
                ],
                sender: PartSender::Caller,
            },
        };
        let v = serde_json::to_value(&parts).unwrap();
        assert_eq!(
            v,
            json!({
                "tenant": "alice",
                "key": "alice/project/p1/f",
                "parts": [
                    {"partNumber": 1, "sizeBytes": 5},
                    {"partNumber": 2, "sizeBytes": 3},
                ],
            })
        );
        let back: Tenanted<UploadPartsRequest> = serde_json::from_value(v).unwrap();
        assert_eq!(back.tenant, "alice");
        // The part NUMBER travels: it is the part's position in the file,
        // so asking twice reserves once.
        assert_eq!(back.inner.parts[1].part_number, 2);
        assert_eq!(back.inner.parts[1].size_bytes, 3);

        let begin = Tenanted {
            tenant: "alice".into(),
            inner: AssetUploadBeginRequest {
                mime_type: "image/png".into(),
                filename: "x.png".into(),
                declared_size: Some(8),
                content_hash: "a".repeat(64),
            },
        };
        let v = serde_json::to_value(&begin).unwrap();
        assert_eq!(
            v,
            json!({"tenant": "alice", "mimeType": "image/png",
                   "filename": "x.png", "declaredSize": 8,
                   "contentHash": "a".repeat(64)})
        );
        let back: Tenanted<AssetUploadBeginRequest> = serde_json::from_value(v).unwrap();
        assert_eq!(back.inner.content_hash, "a".repeat(64));
        // declared_size is optional on the wire; content_hash is not (the
        // admin surface uploads only the content-addressed asset plane).
        let min: Tenanted<AssetUploadBeginRequest> = serde_json::from_value(json!({
            "tenant": "t", "mimeType": "a/b", "filename": "f",
            "contentHash": "b".repeat(64)
        }))
        .unwrap();
        assert_eq!(min.inner.declared_size, None);
    }

    #[test]
    fn stored_file_value_round_trip() {
        let m = StoredFile {
            key: "run/0188-run_id/9f3a".into(),
            mime_type: "audio/ogg".into(),
            size_bytes: 4_200_000,
            filename: "clip.ogg".into(),
            version: 3,
            link_secs: None,
        };
        let v = m.to_value();
        // Exact wire shape: the CONCRETE marker (audio/ogg -> __weft_audio__),
        // payload with camelCase keys, NO url.
        assert_eq!(
            v,
            json!({"__weft_audio__": {
                "key": "run/0188-run_id/9f3a",
                "mimeType": "audio/ogg",
                "sizeBytes": 4_200_000u64,
                "filename": "clip.ogg",
                "version": 3,
            }})
        );
        assert_eq!(StoredFile::from_value(&v).unwrap(), m);
        // A value written before files carried versions names the
        // version every such file was given.
        let mut older = v.clone();
        older["__weft_audio__"].as_object_mut().unwrap().remove("version");
        assert_eq!(StoredFile::from_value(&older).unwrap().version, FIRST_FILE_VERSION);
        // A non image/video/audio mime takes the Blob marker.
        let pdf = StoredFile {
            key: "run/c/x".into(),
            mime_type: "application/pdf".into(),
            size_bytes: 10,
            filename: "x.pdf".into(),
            version: FIRST_FILE_VERSION,
            link_secs: None,
        };
        assert!(pdf.to_value().get("__weft_blob__").is_some());
        assert_eq!(StoredFile::from_value(&pdf.to_value()).unwrap(), pdf);
    }

    #[test]
    fn stored_file_value_is_typed_by_its_marker() {
        let m = StoredFile {
            key: "run/c/1".into(),
            mime_type: "audio/ogg".into(),
            size_bytes: 1,
            filename: "a.ogg".into(),
            version: FIRST_FILE_VERSION,
            link_secs: None,
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
            FileHandle::key("k1")
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
            FileHandle::key("bare-key")
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

    /// A link lives one second to seven days, and a value carrying a
    /// lifetime outside that is refused when it is read, as a handle and
    /// as a stored file alike.
    #[test]
    fn a_value_carries_only_a_link_lifetime_a_link_can_live() {
        assert_eq!(checked_link_secs(1), Ok(1));
        assert_eq!(checked_link_secs(MAX_LINK_SECS), Ok(MAX_LINK_SECS));
        assert_eq!(checked_link_secs(0).unwrap_err(), "a link lives 1 second to 7 days, not under a second");
        assert!(checked_link_secs(MAX_LINK_SECS + 1).is_err());
        let file = |secs: u64| {
            json!({"__weft_blob__": {"key": "project/p/1", "mimeType": "text/plain", "sizeBytes": 1, "filename": "a.txt", "linkSecs": secs}})
        };
        for secs in [1, MAX_LINK_SECS] {
            assert_eq!(StoredFile::from_value(&file(secs)).unwrap().link_secs, Some(secs));
            assert_eq!(
                FileHandle::from_value(&file(secs)).unwrap(),
                FileHandle::Key { key: "project/p/1".into(), link_secs: Some(secs) }
            );
        }
        for secs in [0, MAX_LINK_SECS + 1, 10 * 365 * 24 * 3600] {
            let refused = StoredFile::from_value(&file(secs)).unwrap_err().to_string();
            assert!(refused.contains("a link lives 1 second to 7 days"), "{refused}");
            let refused = FileHandle::from_value(&file(secs)).unwrap_err();
            assert!(matches!(refused, WeftError::Input(_)), "{refused:?}");
            assert!(refused.to_string().contains("a link lives 1 second to 7 days"), "{refused}");
        }
    }

    /// A stored file fits a place by its type: a place taking `Blob` any
    /// file, a media place its own kinds.
    #[test]
    fn fit_mime_holds_a_file_to_the_places_kinds() {
        let ty = |t: &str| crate::weft_type::WeftType::parse(t).unwrap();
        assert_eq!(fit_mime("image/png", &ty("Image")).unwrap(), "image/png");
        assert_eq!(fit_mime("image/png", &ty("Media")).unwrap(), "image/png");
        assert_eq!(fit_mime("application/pdf", &ty("File")).unwrap(), "application/pdf");
        assert_eq!(fit_mime("text/csv", &ty("File | Null")).unwrap(), "text/csv");
        assert_eq!(fit_mime("video/mp4", &ty("Blob")).unwrap(), "video/mp4");
        assert_eq!(
            fit_mime("video/mp4", &ty("Image")).unwrap_err(),
            "video/mp4 is Video, not the Image that was declared"
        );
        assert!(fit_mime("image/png", &ty("Video")).is_err());
        assert!(fit_mime("audio/mpeg", &ty("Video")).is_err(), "an mp3 holds no video");
        assert!(fit_mime("application/octet-stream", &ty("Media")).is_err());
        assert!(fit_mime("application/octet-stream", &ty("Image | Null")).is_err());
    }

    /// The media type is lower-cased as `received_file` stores it; its
    /// parameters stay as they were.
    #[test]
    fn fit_mime_lower_cases_the_media_type_and_keeps_its_parameters() {
        let ty = |t: &str| crate::weft_type::WeftType::parse(t).unwrap();
        assert_eq!(fit_mime("Image/PNG; q=1", &ty("Image | Null")).unwrap(), "image/png; q=1");
        assert_eq!(fit_mime("Video/MP4", &ty("Audio")).unwrap(), "audio/mp4");
    }

    /// A container that holds either video or audio is handed over as the
    /// half the place takes, and as the half it was stored as when the
    /// place takes that.
    #[test]
    fn fit_mime_hands_a_container_over_as_the_half_the_place_takes() {
        let ty = |t: &str| crate::weft_type::WeftType::parse(t).unwrap();
        assert_eq!(fit_mime("video/webm", &ty("Audio")).unwrap(), "audio/webm");
        assert_eq!(fit_mime("video/mp4", &ty("Audio")).unwrap(), "audio/mp4");
        assert_eq!(fit_mime("audio/webm", &ty("Video")).unwrap(), "video/webm");
        assert_eq!(fit_mime("audio/webm;codecs=opus", &ty("Video | Null")).unwrap(), "video/webm;codecs=opus");
        assert_eq!(fit_mime("audio/webm", &ty("Media")).unwrap(), "audio/webm");
        assert_eq!(fit_mime("video/webm", &ty("Video | Audio")).unwrap(), "video/webm");
        assert_eq!(fit_mime("audio/mp4", &ty("File")).unwrap(), "audio/mp4");
    }

    /// A file already handed over fits again only as the type it went as.
    #[test]
    fn a_handed_over_file_fits_again_only_as_its_own_type() {
        let ty = |t: &str| crate::weft_type::WeftType::parse(t).unwrap();
        assert!(fits_as_handed_over("audio/webm", &ty("Audio")).is_ok());
        assert!(fits_as_handed_over("audio/webm", &ty("Media")).is_ok());
        assert!(fits_as_handed_over("image/png; q=1", &ty("File")).is_ok());
        assert_eq!(
            fits_as_handed_over("audio/webm", &ty("Video")).unwrap_err(),
            "it was handed over as audio/webm, and the Video here takes it only as video/webm"
        );
        assert_eq!(
            fits_as_handed_over("image/png", &ty("Audio")).unwrap_err(),
            "image/png is Image, not the Audio that was declared"
        );
        assert!(fits_as_handed_over("image/png", &ty("String")).is_err());
    }

    /// A declaration that is not a file type takes no file.
    #[test]
    fn fit_mime_refuses_a_place_that_is_not_a_file_type() {
        let ty = |t: &str| crate::weft_type::WeftType::parse(t).unwrap();
        assert_eq!(
            fit_mime("application/pdf", &ty("String")).unwrap_err(),
            "the place is declared String, which takes no file"
        );
        assert!(fit_mime("image/png", &ty("Number | Null")).is_err());
    }

    #[test]
    fn file_handle_parses_key_and_url_forms() {
        // A bare key string is a Key handle.
        assert_eq!(
            FileHandle::from_value(&json!("project/p/1")).unwrap(),
            FileHandle::key("project/p/1")
        );
        assert!(FileHandle::from_value(&json!("")).is_err());

        // A key-backed marker is a Key handle (the common case).
        let key_file = StoredFile {
            key: "shared/team/2".into(),
            mime_type: "application/pdf".into(),
            size_bytes: 9,
            filename: "d.pdf".into(),
            version: FIRST_FILE_VERSION,
            link_secs: None,
        };
        assert_eq!(
            FileHandle::from_value(&key_file.to_value()).unwrap(),
            FileHandle::key("shared/team/2")
        );
        // A value's own link lifetime rides on its handle, and on the wire
        // only when it was set.
        let lasting = StoredFile { link_secs: Some(7200), ..key_file.clone() };
        assert_eq!(lasting.to_value()["__weft_blob__"]["linkSecs"], json!(7200));
        assert!(key_file.to_value()["__weft_blob__"].get("linkSecs").is_none());
        assert_eq!(
            FileHandle::from_value(&lasting.to_value()).unwrap(),
            FileHandle::Key { key: "shared/team/2".into(), link_secs: Some(7200) }
        );
        let mut torn = lasting.to_value();
        torn["__weft_blob__"]["linkSecs"] = json!("soon");
        assert!(FileHandle::from_value(&torn).is_err());

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
    fn keep_and_link_request_wire_shapes() {
        // Worker -> broker envelopes: exact wire, both directions.
        let keep = KeepRequest { key: "t/run/c/1".into(), ttl: KeepTtl::Secs { secs: 60 } };
        let v = serde_json::to_value(&keep).unwrap();
        assert_eq!(v, json!({"key": "t/run/c/1", "ttl": {"kind": "secs", "secs": 60}}));
        let back: KeepRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.key, keep.key);
        assert_eq!(back.ttl, keep.ttl);

        let link = LinkRequest { key: "t/project/p/1".into(), ttl_secs: Some(900), reach: LinkReach::External };
        let v = serde_json::to_value(&link).unwrap();
        assert_eq!(v, json!({"key": "t/project/p/1", "ttlSecs": 900, "reach": "external"}));
        let back: LinkRequest = serde_json::from_value(v).unwrap();
        assert_eq!((back.key, back.ttl_secs, back.reach), (link.key, link.ttl_secs, link.reach));
        // No lifetime named: the value's own default is asked for.
        let bare: LinkRequest = serde_json::from_value(json!({"key": "k", "reach": "internal"})).unwrap();
        assert_eq!((bare.ttl_secs, bare.reach), (None, LinkReach::Internal));
        let container = LinkRequest { key: "k".into(), ttl_secs: None, reach: LinkReach::Container };
        let v = serde_json::to_value(&container).unwrap();
        assert_eq!(v, json!({"key": "k", "reach": "container"}));
        assert_eq!(serde_json::from_value::<LinkRequest>(v).unwrap().reach, LinkReach::Container);
        assert_eq!(
            serde_json::to_value(FileLink { url: "https://x/f".into(), public: false }).unwrap(),
            json!({"url": "https://x/f", "public": false})
        );
        let admin = AdminLinkRequest { key: "t/project/p/1".into(), ttl_secs: None, reader: LinkReader::Person };
        assert_eq!(serde_json::to_value(&admin).unwrap(), json!({"key": "t/project/p/1", "reader": {"kind": "person"}}));
        let outside = LinkReader::Outside { project: "p".into() };
        assert_eq!(serde_json::to_value(&outside).unwrap(), json!({"kind": "outside", "project": "p"}));

        let replace = UploadReplaceRequest { key: "t/project/p/1".into(), declared_size: Some(12), expected_version: Some(4) };
        let v = serde_json::to_value(&replace).unwrap();
        assert_eq!(v, json!({"key": "t/project/p/1", "declaredSize": 12, "expectedVersion": 4}));
        let back: UploadReplaceRequest = serde_json::from_value(v).unwrap();
        assert_eq!(
            (back.key, back.declared_size, back.expected_version),
            (replace.key, replace.declared_size, replace.expected_version)
        );
        // An overwrite names no version.
        let blind: UploadReplaceRequest = serde_json::from_value(json!({"key": "t/project/p/1"})).unwrap();
        assert_eq!(blind.expected_version, None);
    }

    #[test]
    fn admin_envelope_wire_shapes() {
        // Dispatcher admin-proxy <-> broker envelopes: the camelCase renames are
        // the wire contract the CLI download handshake also reads verbatim.
        let r = PresignResult { url: "https://b/x".into(), filename: "a.pdf".into(), size_bytes: 9, version: 1, public: false };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v, json!({"url": "https://b/x", "filename": "a.pdf", "sizeBytes": 9, "version": 1, "public": false}));
        let back: PresignResult = serde_json::from_value(v).unwrap();
        assert_eq!((back.size_bytes, back.version, back.public), (9, 1, false));

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
            serde_json::to_value(WipePrefixRequest { prefix: "t1/run/c/".into() }).unwrap(),
            json!({"prefix": "t1/run/c/"})
        );
        assert_eq!(serde_json::to_value(WipePrefixResponse { wiped: 2 }).unwrap(), json!({"wiped": 2}));
        assert_eq!(
            serde_json::to_value(RunFilesRequest { tenant: "t1".into(), run_ids: vec!["c1".into()] }).unwrap(),
            json!({"tenant": "t1", "run_ids": ["c1"]})
        );
        assert_eq!(serde_json::to_value(RunFilesResponse { deleted: 1 }).unwrap(), json!({"deleted": 1}));
    }

    #[test]
    fn scope_wire_shape() {
        assert_eq!(
            serde_json::to_value(StorageScope::Run).unwrap(),
            json!({"kind": "run"})
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
            key: "run/c/1".into(),
            mime_type: "video/mp4".into(),
            size_bytes: 5,
            filename: "v.mp4".into(),
            expires_at_unix: Some(1_700_000_000),
            keep_ttl_secs: Some(86_400),
            created_at_unix: 1_600_000_000,
            version: 2,
        };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["mimeType"], "video/mp4");
        assert_eq!(v["version"], 2);
        // The value the meta hands downstream carries the same version.
        assert_eq!(StoredFile::from(&m).version, 2);
        assert_eq!(v["expiresAtUnix"], 1_700_000_000);
        let back: StoredFileMeta = serde_json::from_value(v).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn upload_envelopes_wire_shape() {
        // begin: declared_size and keep are optional on the wire.
        let b: UploadBeginRequest = serde_json::from_value(json!({
            "scope": {"kind": "run"},
            "mimeType": "image/png",
            "filename": "a.png",
        }))
        .unwrap();
        assert_eq!(b.declared_size, None);
        assert!(b.keep.is_none());
        let b2: UploadBeginRequest = serde_json::from_value(json!({
            "scope": {"kind": "project"},
            "mimeType": "image/png",
            "filename": "a.png",
            "keep": {"kind": "never"},
            "declaredSize": 42,
        }))
        .unwrap();
        assert_eq!(b2.declared_size, Some(42));

        let r = UploadBeginResponse {
            key: "t/run/c/1".into(),
            part_size: 8 << 20,
            already_stored: false,
            resume: false,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            json!({
                "key": "t/run/c/1",
                "partSize": 8_388_608u64,
                "alreadyStored": false,
                "resume": false,
            })
        );
        // Both flags default false on the wire, so a begin reply without
        // them parses as a fresh reservation.
        let back: UploadBeginResponse =
            serde_json::from_value(json!({"key": "t/run/c/1", "partSize": 1})).unwrap();
        assert!(!back.already_stored);
        assert!(!back.resume);

        let p = PresignedPart { part_number: 2, size_bytes: 5, offset_bytes: 8, url: "u".into() };
        let v = serde_json::to_value(UploadPartsResponse { parts: vec![p.clone()] }).unwrap();
        assert_eq!(
            v,
            json!({"parts": [{"partNumber": 2, "sizeBytes": 5, "offsetBytes": 8, "url": "u"}]})
        );

        let d: PartDoneRequest = serde_json::from_value(json!({
            "key": "k", "partNumber": 1, "etag": "\"abc\"",
        }))
        .unwrap();
        // The etag survives verbatim, quotes included.
        assert_eq!(d.etag, "\"abc\"");

        let res = UploadResumeResponse { part_size: 8, missing: vec![p], reserved_bytes: 13 };
        let back: UploadResumeResponse =
            serde_json::from_value(serde_json::to_value(&res).unwrap()).unwrap();
        assert_eq!(back.part_size, 8);
        // The offset rides with the part, so a resume can hand back a
        // part from the middle of the file and the uploader still writes
        // the right bytes under it.
        assert_eq!(back.missing[0].offset_bytes, 8);
        // And new parts begin past every part already carved, which is
        // not where the missing one ends.
        assert_eq!(back.reserved_bytes, 13);
        // Required, both of them: an answer that leaves the offset out
        // and is read as zero writes the start of the file into a part
        // that stands for somewhere else in it.
        assert!(
            serde_json::from_value::<UploadResumeResponse>(serde_json::json!({
                "partSize": 8,
                "missing": [{"partNumber": 1, "sizeBytes": 8, "url": "u"}],
                "reservedBytes": 8,
            }))
            .is_err(),
            "offset_bytes must be required"
        );
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn bytes_stream_round_trip() {
        let b = bytes::Bytes::from_static(b"hello");
        let collected = collect_stream(bytes_stream(b.clone())).await.unwrap();
        assert_eq!(collected, b);
    }
}
