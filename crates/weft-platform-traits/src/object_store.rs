//! The object-storage seam: a flat, content-keyed blob store both the
//! dispatcher AND the worker (weft-engine) talk to. It lives HERE in
//! platform-traits, not in the dispatcher alongside the other policy
//! seams, on purpose: the runtime `ctx.storage` plane runs INSIDE the
//! worker, and weft-engine does not (and must not) depend on
//! weft-dispatcher. A cross-cutting capability both processes need is exactly
//! what this crate is for (same as `Clock`).
//!
//! The store is deliberately dumb: keyed put / get / head / delete /
//! list / presign over opaque bytes. It is the deploy-time SLOT the
//! install is handed (S3-compatible endpoint + bucket + creds), mirroring
//! how the image registry is a slot: the bundled default points it at a
//! SeaweedFS service, and it can be pointed at any S3-compatible bucket
//! (e.g. GCS) instead. Everything above it (content-defined chunking,
//! the tree/version model, the runtime storage scopes) is built on top in
//! higher crates and is backing-agnostic.
//!
//! There is no "local default that fails loud" here: object storage is a
//! hard dependency of a running install (the source plane AND the runtime
//! plane both need it), so an install without a configured store is a
//! startup error, surfaced where the slot is constructed, not a silent
//! no-op impl that defers the failure to first use.
//!
//! `FakeObjectStore` is an in-memory drop-in for tests (a map plus a
//! recorded call log), gated behind `test-helpers` so it never reaches a
//! release binary.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use weft_core::access::spec::percent_encode;

/// The longest a presigned URL may live: a presign is an expiring artifact,
/// never a durable public link, and seven days is also the most an S3 or a
/// Cloud Storage signature allows. A store refuses a longer one.
pub const MAX_PRESIGN_TTL_SECS: u64 = weft_core::storage::MAX_LINK_SECS;

/// The most an S3 store copies in one request: one piece of a copy. A
/// bigger object is copied piece by piece ([`copy_ranges`]), so its
/// caller hears of progress at least once per piece, however slowly the
/// store copies (one request may copy up to 5 GiB, and reports nothing
/// until it returns). Every piece but the last is this size, well past
/// the 5 MiB a multipart part takes at least; the last may be smaller,
/// which S3 allows.
const COPY_PIECE_BYTES: u64 = 1024 * 1024 * 1024;

/// The inclusive byte ranges, in order, that copy an object of `size`
/// bytes piece by piece.
fn copy_ranges(size: u64) -> Vec<(u64, u64)> {
    (0..size.div_ceil(COPY_PIECE_BYTES))
        .map(|i| (i * COPY_PIECE_BYTES, ((i + 1) * COPY_PIECE_BYTES).min(size) - 1))
        .collect()
}

/// A key as a URL path: each segment percent-encoded (RFC 3986), the `/`
/// between segments kept. S3's copy source and Cloud Storage's object path
/// both name an object this way.
pub fn encode_key(key: &str) -> String {
    key.split('/').map(percent_encode).collect::<Vec<_>>().join("/")
}

/// The `CopySource` an S3 copy of `key` names: `<bucket>/<key>`, the key
/// URL-encoded as the API requires (the SDK sends it as given).
fn copy_source(bucket: &str, key: &str) -> String {
    format!("{bucket}/{}", encode_key(key))
}

/// Where the presigned URL will be used FROM. A signature is bound to the
/// host in the URL, so the store must sign for the host the caller can
/// reach. A store with a public endpoint beside the one weft does its I/O
/// on (`objectStore.publicEndpoint`) signs a person's link for the public
/// one; a store every caller reaches at one address (Cloud Storage) signs
/// the same URL for all of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresignAudience {
    /// A person's own machine (the CLI, the editor, a browser beside the
    /// install): sign for the public endpoint (`objectStore.publicEndpoint`).
    External,
    /// The install itself: weft's own runtime reading for itself (the
    /// dispatcher reading a version's file, the broker hashing an upload)
    /// or a project's worker running node code. Sign for the endpoint the
    /// runtime does its own I/O on, which the workers reach it at too.
    Install,
    /// Somebody who reaches the store through the install's forwarder
    /// ([`FILES_DOOR`]) at `base` (`https://<tunnel>`, a site that passes
    /// `/weft/...` on: `https://shop.example/weft`, or a local install's
    /// internal port as its containers reach it): sign for `base`'s host,
    /// which the forwarder hands the store as `X-Forwarded-Host`, and answer
    /// the link under `base` ([`forwarded_link`]).
    Forwarded { base: String },
}

/// The install's forwarder: the path under which it passes requests on to
/// the object store unchanged (`<base>/files/<object key>?<query>` goes to
/// `<store>/<bucket>/<object key>?<query>`), so a link signed for the
/// install's public address reaches the bytes with every header (`Range`,
/// `If-None-Match`) intact.
// SYNC: FILES_DOOR <-> crates/weft-dispatcher/src/api/mod.rs (FILES_DOOR_PATH), packages/weft-connect/src/server/passthrough.ts PASSED_DOORS
pub const FILES_DOOR: &str = "files";

/// The part of the bucket the forwarder passes on: the runtime files, and
/// nothing of the source plane's chunks and trees.
pub const FORWARDED_PREFIX: &str = "runtime/";

/// A link signed for `base`'s host (`signed`, as the store signs it:
/// `<scheme>://<host>/<bucket>/<object key>?<query>`) as the forwarder
/// serves it: `<base>/files/<object key>?<query>`. The signature covers the
/// path the STORE sees (`/<bucket>/<object key>`), which is what the
/// forwarder sends it, so moving the link under `base`'s own path changes
/// nothing it signs.
pub fn forwarded_link(base: &str, bucket: &str, signed: &str) -> Result<String> {
    let parsed = url::Url::parse(signed).with_context(|| format!("a signed link is not a URL: {signed}"))?;
    let object = parsed
        .path()
        .strip_prefix(&format!("/{bucket}/"))
        .ok_or_else(|| anyhow!("a signed link names no object of bucket '{bucket}': {signed}"))?;
    let query = parsed.query().map(|q| format!("?{q}")).unwrap_or_default();
    Ok(format!("{}/{FILES_DOOR}/{object}{query}", base.trim_end_matches('/')))
}

/// The origin (`<scheme>://<host>[:<port>]`) of an http(s) base, which a
/// link forwarded under it is signed for. Refused when `base` is not an
/// http(s) address with a host and nothing past its path.
pub fn base_origin(base: &str) -> Result<String> {
    let parsed = url::Url::parse(base).with_context(|| format!("'{base}' is not an address"))?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "http" | "https")
            && parsed.host_str().is_some()
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.username().is_empty()
            && parsed.password().is_none(),
        "'{base}' is not an http(s) address with nothing after its path"
    );
    Ok(parsed.origin().ascii_serialization())
}

/// What a download link is for: who opens it, from when it is signed and
/// how long it lives, and how the store answers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetLink {
    pub audience: PresignAudience,
    /// The moment the link is signed at, which may be earlier than now:
    /// signing two links for one file at the same moment gives the same
    /// link, which is what keeps a link stable across answers.
    pub signed_at: std::time::SystemTime,
    /// How long after `signed_at` the link works.
    pub ttl_secs: u64,
    /// The `Content-Type` the store answers with.
    pub content_type: Option<String>,
    /// The `Content-Disposition` the store answers with.
    pub disposition: Option<String>,
}

/// What a copy ([`ObjectStore::copy`]) tells its caller as it goes: a
/// copy of a big object takes a while, and whoever waits on it learns it
/// is still alive each time a piece of it is done.
#[async_trait]
pub trait CopyProgress: Send + Sync {
    /// One more piece of the copy is done. An error stops the copy.
    async fn copied_piece(&self) -> Result<()>;
}

/// A copy nobody follows.
pub struct Unfollowed;

#[async_trait]
impl CopyProgress for Unfollowed {
    async fn copied_piece(&self) -> Result<()> {
        Ok(())
    }
}

/// One entry returned by `list`: the object's full key and its size. The
/// store lists by key prefix; callers that want a hierarchical view derive
/// it from the keys (the store itself is flat, like every S3 bucket).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
}

/// The flat object store. Async, keyed by opaque string, values are opaque
/// bytes. Implementations: `S3ObjectStore` (any S3-compatible bucket, over
/// the AWS SDK), the Google platform's `GcsObjectStore` (Cloud Storage as the
/// process's own account), and `FakeObjectStore` for tests.
///
/// Every method fails LOUD on a backend error (no silent recovery): a put
/// that does not land, or a get that cannot reach the bucket, returns
/// `Err`, never a partial or a default. `exists`/`get` distinguish
/// "definitely not there" (`Ok(false)` / `Ok(None)`) from "could not tell"
/// (`Err`), so callers never mistake a transient outage for a missing key.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Write `bytes` at `key`, overwriting any existing object. Content-
    /// addressed callers only ever write a key whose value is fixed by its
    /// name, so an overwrite is a byte-identical no-op; non-addressed
    /// callers (the runtime plane) rely on last-write-wins.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()>;

    /// Read the whole object at `key`. `Ok(None)` iff the key definitively
    /// does not exist; `Err` iff the backend could not be reached or
    /// answered with an error other than not-found.
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;

    /// Read a byte range `[start, end)` (end exclusive) of the object at
    /// `key`. Same `Ok(None)`-means-absent contract as `get`.
    ///
    /// Contract at the edges (both impls obey it):
    /// - `end <= start` (an empty range) yields `Ok(Some(empty))` if the object
    ///   exists, `Ok(None)` if absent, with no backend read.
    /// - a NON-empty range whose `start` is at or past the object's end is out of
    ///   bounds and yields `Err` (S3 answers 416 Range Not Satisfiable, which is
    ///   NOT not-found, so it surfaces as an error, not `Ok(None)`). Callers pass
    ///   in-bounds ranges derived from the object's known size; an out-of-bounds
    ///   range is a bug, surfaced loudly rather than masked to an empty read.
    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Option<Bytes>>;

    /// Does `key` exist? `Ok(false)` iff definitively absent; `Err` iff the
    /// backend could not be reached. Content-addressed writers use this to
    /// skip re-uploading a chunk that is already present.
    async fn exists(&self, key: &str) -> Result<bool>;

    /// The size in bytes of the object at `key`, or `Ok(None)` if absent.
    async fn size(&self, key: &str) -> Result<Option<u64>>;

    /// Delete `key`. Deleting an absent key is NOT an error (idempotent):
    /// the post-condition "the key is gone" already holds.
    async fn delete(&self, key: &str) -> Result<()>;

    /// List every object whose key starts with `prefix`, in lexical key
    /// order. The store is flat, so this is a flat list of full keys; a
    /// folder view is the caller's projection over them.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectEntry>>;

    /// Mint a presigned DOWNLOAD (GET) URL for `key` as `link` describes it:
    /// signed for its audience's network at its `signed_at`, working for its
    /// `ttl_secs`, answering with its content type and disposition. The URL
    /// grants reads of that one object (whole or by `Range`) and expires on
    /// its own; the caller uses it directly without ever holding a
    /// credential. Signing is deterministic: the same `link` for the same
    /// key is the same URL.
    async fn presign_get(&self, key: &str, link: &GetLink) -> Result<String>;

    /// Start a multipart upload for `key`, whose object the store will
    /// keep as `content_type` (what it answers a plain read with). Returns
    /// the upload id: the handle every subsequent part/complete/abort call
    /// names, and the resume handle a caller persists to pick the upload
    /// back up after an interruption. The upload holds no visible object
    /// until `complete_multipart`.
    async fn create_multipart(&self, key: &str, content_type: &str) -> Result<String>;

    /// Copy the object at `from` to `to` inside the store: no byte leaves
    /// it. Overwrites `to`. Returns the copy's size. A missing `from` is an
    /// error. `progress` hears of each piece the store finishes, the last
    /// one included; its error stops the copy there and is the copy's
    /// error, and an object the copy already made at `to` is the caller's
    /// to remove.
    async fn copy(&self, from: &str, to: &str, progress: &dyn CopyProgress) -> Result<u64>;

    /// Mint a presigned URL for uploading ONE part, with the part's EXACT
    /// byte size signed into it (`Content-Length` is a signed header): the
    /// bucket rejects any body that is not exactly `part_size` bytes. That
    /// signed size is what lets a caller reserve bytes against a quota
    /// BEFORE handing out the URL and trust the bucket to enforce the
    /// reservation. It is the only upload URL this trait mints, so a size
    /// a caller reserved can never be exceeded. `audience` picks the
    /// signing host, same as `presign_get`.
    async fn presign_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part_size: u64,
        audience: &PresignAudience,
        ttl_secs: u64,
    ) -> Result<String>;

    /// Upload ONE part DIRECTLY from this process (no presigned URL): for
    /// when the caller itself moves bytes it already holds (e.g.
    /// concatenating stored objects) instead of handing a URL to an
    /// external uploader. Returns the part's etag, verbatim, exactly like a
    /// presigned PUT's response header.
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: Bytes,
    ) -> Result<String>;

    /// Complete the multipart upload from the caller's OWN `(part_number,
    /// etag)` list, ascending, etags verbatim as returned by the part PUTs
    /// (quotes included; normalizing them breaks completion on some
    /// backends). Returns the final object's authoritative size in bytes.
    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> Result<u64>;

    /// Abort a multipart upload, discarding its landed parts. Idempotent:
    /// aborting an unknown/already-aborted upload is not an error (the
    /// post-condition "the upload is gone" already holds). NOTE: a part PUT
    /// in flight while the abort runs can still land afterwards, so a
    /// caller that must guarantee zero residue re-aborts on its sweep.
    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()>;

    /// Whether the multipart upload is still open: true while it can take
    /// parts and be completed, false once it is gone (completed, aborted,
    /// or never existed). Read through ListParts, which answers 404
    /// `NoSuchUpload` for a gone upload on S3, the GCS XML API and
    /// SeaweedFS alike (SeaweedFS removes the upload's folder when it
    /// completes). It cannot say WHICH way the upload went; a caller that
    /// never aborts an upload it is completing reads "gone" as "completed"
    /// and checks the object.
    async fn multipart_exists(&self, key: &str, upload_id: &str) -> Result<bool>;
}

/// An S3-compatible store's config: where the bucket lives and the key pair
/// that reaches it. A local install points it at its bundled SeaweedFS.
///
/// `endpoint_url` is what makes this S3-compatible-rather-than-AWS: it
/// overrides the SDK's default AWS endpoint so the same client talks to
/// SeaweedFS, or any S3 API. `force_path_style` (bucket in the PATH, not the
/// hostname) is required by SeaweedFS and most non-AWS S3 servers, so it
/// defaults on.
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// The S3-compatible endpoint URL (e.g. the internal SeaweedFS service,
    /// or an AWS regional endpoint).
    pub endpoint_url: String,
    /// The single bucket every object lives in (prefixes namespace the
    /// planes: `chunks/`, `trees/`, `runtime/<tenant>/...`).
    pub bucket: String,
    /// The region to sign requests for. Non-AWS endpoints ignore the value
    /// but the signer still needs one; default `us-east-1`.
    pub region: String,
    /// Static access key id (for SeaweedFS, its configured admin key).
    pub access_key_id: String,
    /// Static secret access key paired with `access_key_id`.
    pub secret_access_key: String,
    /// Bucket-in-path addressing. On for SeaweedFS and most S3-compatible
    /// servers; AWS/GCS tolerate it too.
    pub force_path_style: bool,
    /// The BROWSER/host-reachable endpoint presigned URLs are signed for, when it
    /// differs from `endpoint_url`. The broker reaches the bucket over the
    /// internal `endpoint_url` for its own I/O, but a presigned download URL is
    /// handed to an external caller (a browser, an external API, the e2e on the
    /// host) that cannot resolve an internal DNS name, so it must be signed for
    /// a reachable host. `None` (an `endpoint_url` that is already public) means
    /// presign against `endpoint_url` directly. For local dev / e2e this is the
    /// host-forwarded SeaweedFS address.
    pub public_endpoint_url: Option<String>,
}

/// The environment variables the object store's key pair reaches the
/// runtime by.
// SYNC: the object store's key names <-> scripts/run-e2e.sh (reads both from the install's secrets.env)
pub const OBJECT_STORE_ACCESS_KEY_ENV: &str = "WEFT_OBJECT_STORE_ACCESS_KEY";
pub const OBJECT_STORE_SECRET_KEY_ENV: &str = "WEFT_OBJECT_STORE_SECRET_KEY";

impl ObjectStoreConfig {
    /// The store the install config names, with its credentials read from
    /// [`OBJECT_STORE_ACCESS_KEY_ENV`] and [`OBJECT_STORE_SECRET_KEY_ENV`]
    /// (secrets reach the process through its environment). Missing
    /// credentials are an error naming them: object storage is a hard
    /// dependency, never a silent default.
    pub fn from_settings(settings: &crate::config::S3StoreSettings) -> Result<Self> {
        let secret = |name: &str| -> Result<String> {
            std::env::var(name)
                .ok()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("{name} must be set: the object store's credentials reach the runtime through its environment"))
        };
        Ok(Self {
            endpoint_url: settings.endpoint.clone(),
            bucket: settings.bucket.clone(),
            region: settings.region.clone(),
            access_key_id: secret(OBJECT_STORE_ACCESS_KEY_ENV)?,
            secret_access_key: secret(OBJECT_STORE_SECRET_KEY_ENV)?,
            force_path_style: settings.force_path_style,
            public_endpoint_url: settings.public_endpoint.clone(),
        })
    }
}

/// Build an S3-compatible store from its settings (a Cloud Storage bucket
/// reached as the process's own account is the Google platform's).
pub async fn object_store_for(settings: &crate::config::S3StoreSettings) -> Result<SharedObjectStore> {
    Ok(Arc::new(S3ObjectStore::new(&ObjectStoreConfig::from_settings(settings)?).await?))
}

/// Production `ObjectStore` over the AWS Rust SDK's S3 client. The SDK is used
/// purely as the S3-protocol client (request building + SigV4 signing); the
/// endpoint override points it at whatever S3-compatible server the slot names
/// (SeaweedFS, AWS, a client bucket). No AWS service is implied.
pub struct S3ObjectStore {
    client: aws_sdk_s3::Client,
    /// A second client whose endpoint is the BROWSER/host-reachable public
    /// endpoint, used ONLY for presigning so a handed-out URL is reachable by an
    /// external caller. `None` when the slot configured no separate public
    /// endpoint (the endpoint is already public): presign against the main
    /// `client`.
    presign_client: Option<aws_sdk_s3::Client>,
    bucket: String,
}

impl S3ObjectStore {
    /// Build the client from the slot config. Loads the SDK defaults first (which
    /// install the tokio async-sleep impl + HTTP client + retry runtime, required
    /// for the client to function, building a bare `Config::Builder` omits them
    /// and the client panics on first use), then layers our slot overrides:
    /// static credentials + an endpoint override + path-style addressing, the
    /// standard recipe for talking to a non-AWS S3 server.
    pub async fn new(cfg: &ObjectStoreConfig) -> anyhow::Result<Self> {
        let store = Self::clients(cfg).await;
        store.ensure_bucket().await?;
        Ok(store)
    }

    /// The clients, without touching the store.
    async fn clients(cfg: &ObjectStoreConfig) -> Self {
        let creds = aws_credential_types::Credentials::from_keys(
            &cfg.access_key_id,
            &cfg.secret_access_key,
            None,
        );
        // HTTPS on `ring`, like every other TLS client in weft (see the
        // workspace Cargo.toml's `aws-smithy-http-client`).
        let https = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
            ))
            .build_https();
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .http_client(https)
            .region(aws_sdk_s3::config::Region::new(cfg.region.clone()))
            .credentials_provider(creds)
            // Set the retry posture EXPLICITLY rather than inheriting the SDK's
            // unstated default (adaptive, ~3 attempts): object stores do return
            // transient 503s/throttles, so a small BOUNDED retry is right, but
            // it must be a chosen number the trait's "fails loud, no silent
            // recovery" contract can reason about, not a hidden default that
            // masks a flapping backend behind extra latency.
            .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3))
            .load()
            .await;
        let client_for = |endpoint: &str| {
            let s3_config = aws_sdk_s3::config::Builder::from(&shared)
                .endpoint_url(endpoint)
                .force_path_style(cfg.force_path_style)
                // Only add checksum headers when an operation REQUIRES one,
                // never by default. The SDK's default (WhenSupported, since
                // v1.69) injects a SIGNED `x-amz-checksum-*` header into every
                // put/upload-part; a presigned-URL caller does not replay that
                // header, so every presigned upload would fail signature
                // validation on S3-compatible servers that check it. Explicit
                // WhenRequired restores plain uploads everywhere.
                .request_checksum_calculation(
                    aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
                )
                .response_checksum_validation(
                    aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
                )
                .build();
            aws_sdk_s3::Client::from_conf(s3_config)
        };
        Self {
            client: client_for(&cfg.endpoint_url),
            // Only build a separate presign client when the public endpoint
            // genuinely differs from the I/O endpoint.
            presign_client: cfg
                .public_endpoint_url
                .as_ref()
                .filter(|p| p.as_str() != cfg.endpoint_url)
                .map(|p| client_for(p)),
            bucket: cfg.bucket.clone(),
        }
    }

    /// Ensure the slot's bucket exists and is reachable (idempotent). Creating a
    /// bucket on first PutObject is not something every S3 store does (SeaweedFS
    /// before 4.01 did not), so the store creates it once at startup; an already-existing /
    /// already-owned bucket (the common
    /// case, and what every restart hits) is a no-op.
    ///
    /// Fail LOUD (no warn-and-continue): if `head_bucket` succeeds the bucket is
    /// there and we are done. Otherwise we try `create_bucket`; an
    /// already-exists/owned race is benign. Any OTHER create error is a genuine
    /// boot fault (bucket missing and uncreatable, or creds that can neither head
    /// nor create it), so it propagates and the process fails to start with a
    /// message naming the fix, rather than booting into a store that 500s on the
    /// first real op.
    async fn ensure_bucket(&self) -> anyhow::Result<()> {
        if self.client.head_bucket().bucket(&self.bucket).send().await.is_ok() {
            return Ok(());
        }
        match self.client.create_bucket().bucket(&self.bucket).send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                // A create that conflicts with an existing/owned bucket is a
                // benign race (another process created it between our head and
                // create). Detect it by the HTTP 409 status, not by string-
                // matching the Debug output: the substring form breaks on an
                // SDK version bump or a non-AWS store (SeaweedFS / GCS) that
                // words the conflict differently.
                let is_conflict = e
                    .raw_response()
                    .map(|r| r.status().as_u16() == 409)
                    .unwrap_or(false);
                if is_conflict {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(
                        "object-store bucket '{}' is not reachable and could not be created: {e:?}. \
                         Pre-provision the bucket, or grant the store's credentials head/create \
                         permission on it, then restart.",
                        self.bucket
                    ))
                }
            }
        }
    }

    /// How a link for `audience` is signed: the client whose endpoint that
    /// caller reaches the store at, and, for a link through the forwarder,
    /// the origin it is signed for instead (the client's own endpoint
    /// overridden for that one signature).
    ///   - External (a person's machine): the public-endpoint client when
    ///     one is configured, else the I/O client (a bucket whose endpoint is
    ///     already publicly reachable).
    ///   - Install (weft's runtime, a project's worker): the I/O client.
    ///   - Forwarded: the I/O client, signed for the base's origin.
    fn signer(&self, audience: &PresignAudience) -> Result<(&aws_sdk_s3::Client, Option<String>)> {
        Ok(match audience {
            PresignAudience::External => (self.presign_client.as_ref().unwrap_or(&self.client), None),
            PresignAudience::Install => (&self.client, None),
            PresignAudience::Forwarded { base } => (&self.client, Some(base_origin(base)?)),
        })
    }

    /// A signed URI as `audience` opens it: under the forwarder's door for
    /// a forwarded base, as signed otherwise.
    fn opened_by(&self, audience: &PresignAudience, signed: String) -> Result<String> {
        match audience {
            PresignAudience::Forwarded { base } => forwarded_link(base, &self.bucket, &signed),
            _ => Ok(signed),
        }
    }

    /// The size and the store's `Content-Type` of the object at `key`, in
    /// one head; `None` when no object is there.
    async fn head(&self, key: &str) -> Result<Option<(u64, Option<String>)>> {
        match self.client.head_object().bucket(&self.bucket).key(key).send().await {
            Ok(out) => {
                // A HEAD on an existing object always carries content_length (a 0-byte
                // object reports Some(0)). A MISSING field is an SDK/server anomaly, not
                // a real "size 0": defaulting it to 0 would silently under-report a
                // present file (quota, range reads). Fail loud instead.
                let len = out.content_length().ok_or_else(|| {
                    anyhow::anyhow!("object-store head {key}: response carried no content_length")
                })?;
                // A negative content-length is as anomalous as a missing one;
                // masking it to 0 (`.max(0)`) would under-report a present
                // file, the exact hole the missing-field branch above guards.
                // Fail loud.
                if len < 0 {
                    anyhow::bail!("object-store head {key}: negative content_length {len}");
                }
                Ok(Some((len as u64, out.content_type().map(str::to_string))))
            }
            Err(e) if Self::is_not_found(&e) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("object-store head {key}")),
        }
    }

    /// True iff an S3 error is a definitive "no such key" (404), as opposed
    /// to any other error (which must surface). Used to map get/head into the
    /// `Ok(None)`-means-absent contract without swallowing real failures.
    fn is_not_found<E: std::fmt::Debug>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
        use aws_sdk_s3::error::SdkError;
        match err {
            SdkError::ServiceError(se) => se.raw().status().as_u16() == 404,
            _ => false,
        }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes))
            .send()
            .await
            .with_context(|| format!("object-store put {key}"))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let out = match self.client.get_object().bucket(&self.bucket).key(key).send().await {
            Ok(out) => out,
            Err(e) if Self::is_not_found(&e) => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("object-store get {key}")),
        };
        let data = out.body.collect().await.with_context(|| format!("object-store read body {key}"))?;
        Ok(Some(data.into_bytes()))
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Option<Bytes>> {
        // S3 Range is inclusive on both ends; our contract is [start, end)
        // exclusive, so the header end is `end - 1`. An empty range yields an
        // empty slice without a request.
        if end <= start {
            return Ok(if self.exists(key).await? { Some(Bytes::new()) } else { None });
        }
        let range = format!("bytes={start}-{}", end - 1);
        let out = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range)
            .send()
            .await
        {
            Ok(out) => out,
            Err(e) if Self::is_not_found(&e) => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("object-store get_range {key}")),
        };
        let data =
            out.body.collect().await.with_context(|| format!("object-store read range body {key}"))?;
        Ok(Some(data.into_bytes()))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self.client.head_object().bucket(&self.bucket).key(key).send().await {
            Ok(_) => Ok(true),
            Err(e) if Self::is_not_found(&e) => Ok(false),
            Err(e) => Err(e).with_context(|| format!("object-store head {key}")),
        }
    }

    async fn size(&self, key: &str) -> Result<Option<u64>> {
        Ok(self.head(key).await?.map(|(size, _)| size))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        // S3 DeleteObject is already idempotent (deleting an absent key is a
        // 204), so this satisfies the "delete is not an error when absent"
        // contract directly.
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("object-store delete {key}"))?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectEntry>> {
        let mut out = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req =
                self.client.list_objects_v2().bucket(&self.bucket).prefix(prefix);
            if let Some(token) = &continuation {
                req = req.continuation_token(token);
            }
            let page = req.send().await.with_context(|| format!("object-store list {prefix}"))?;
            for obj in page.contents() {
                if let Some(key) = obj.key() {
                    // Fail loud on a missing/negative size, exactly like `size()`:
                    // coercing it to 0 would silently under-report a present object
                    // to any caller that sums list sizes (quota/accounting). A
                    // present listed object always carries a non-negative size.
                    let raw = obj.size().ok_or_else(|| {
                        anyhow::anyhow!("object-store list {prefix}: entry {key} carried no size")
                    })?;
                    if raw < 0 {
                        anyhow::bail!("object-store list {prefix}: entry {key} negative size {raw}");
                    }
                    out.push(ObjectEntry { key: key.to_string(), size: raw as u64 });
                }
            }
            if page.is_truncated().unwrap_or(false) {
                continuation = page.next_continuation_token().map(|s| s.to_string());
                if continuation.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        // S3 returns lexical order per page; sort to guarantee total order
        // across pages (the contract callers rely on for determinism).
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn presign_get(&self, key: &str, link: &GetLink) -> Result<String> {
        let cfg = aws_sdk_s3::presigning::PresigningConfig::builder()
            .start_time(link.signed_at)
            .expires_in(Duration::from_secs(link.ttl_secs))
            .build()
            .with_context(|| "build presigning config")?;
        let (client, origin) = self.signer(&link.audience)?;
        let op = client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .set_response_content_type(link.content_type.clone())
            .set_response_content_disposition(link.disposition.clone());
        let signed = match origin {
            Some(origin) => op.customize().config_override(aws_sdk_s3::config::Builder::default().endpoint_url(origin)).presigned(cfg).await,
            None => op.presigned(cfg).await,
        }
        .with_context(|| format!("presign GET {key}"))?;
        self.opened_by(&link.audience, signed.uri().to_string())
    }

    async fn create_multipart(&self, key: &str, content_type: &str) -> Result<String> {
        let out = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .send()
            .await
            .with_context(|| format!("object-store create-multipart {key}"))?;
        out.upload_id()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("object-store create-multipart {key}: response carried no upload id"))
    }

    async fn copy(&self, from: &str, to: &str, progress: &dyn CopyProgress) -> Result<u64> {
        let (size, content_type) = self.head(from).await?.ok_or_else(|| anyhow!("object-store copy: no object at {from}"))?;
        let source = copy_source(&self.bucket, from);
        if size <= COPY_PIECE_BYTES {
            self.client
                .copy_object()
                .bucket(&self.bucket)
                .key(to)
                .copy_source(&source)
                .send()
                .await
                .with_context(|| format!("object-store copy {from} to {to}"))?;
            // One request, so one piece.
            progress.copied_piece().await?;
            return Ok(size);
        }
        // Past one piece, the store copies it piece by piece into a
        // multipart upload of the target, still all inside it.
        let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
        let upload_id = self.create_multipart(to, &content_type).await?;
        let copied: Result<u64> = async {
            let mut parts = Vec::new();
            for (index, (start, end)) in copy_ranges(size).into_iter().enumerate() {
                let part_number = i32::try_from(index + 1).map_err(|_| anyhow!("object-store copy {from}: too many parts"))?;
                let out = self
                    .client
                    .upload_part_copy()
                    .bucket(&self.bucket)
                    .key(to)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .copy_source(&source)
                    .copy_source_range(format!("bytes={start}-{end}"))
                    .send()
                    .await
                    .with_context(|| format!("object-store copy {from} to {to}, part {part_number}"))?;
                let etag = out
                    .copy_part_result()
                    .and_then(|r| r.e_tag())
                    .ok_or_else(|| anyhow!("object-store copy {from} to {to}: part {part_number} answered no etag"))?;
                parts.push((part_number, etag.to_string()));
                progress.copied_piece().await?;
            }
            self.complete_multipart(to, &upload_id, &parts).await
        }
        .await;
        match copied {
            Ok(size) => Ok(size),
            Err(e) => {
                // Whichever step failed (a part, or the completion), the
                // half-made target holds nothing anybody reads; a failed
                // abort is said beside the copy's own failure.
                if let Err(abort) = self.abort_multipart(to, &upload_id).await {
                    return Err(e.context(format!("and its upload {upload_id} could not be aborted: {abort:#}")));
                }
                Err(e)
            }
        }
    }

    async fn presign_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part_size: u64,
        audience: &PresignAudience,
        ttl_secs: u64,
    ) -> Result<String> {
        let cfg = aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(ttl_secs))
            .with_context(|| "build presigning config")?;
        let (client, origin) = self.signer(audience)?;
        // Setting content_length EXPLICITLY makes it a SIGNED header (the
        // signer only excludes it when unset), which is the whole point: the
        // bucket rejects a body that is not exactly `part_size` bytes.
        let op = client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .content_length(i64::try_from(part_size).map_err(|_| {
                anyhow!("object-store presign-part {key} #{part_number}: part size {part_size} overflows i64")
            })?);
        let signed = match origin {
            Some(origin) => op.customize().config_override(aws_sdk_s3::config::Builder::default().endpoint_url(origin)).presigned(cfg).await,
            None => op.presigned(cfg).await,
        }
        .with_context(|| format!("object-store presign-part {key} #{part_number}"))?;
        self.opened_by(audience, signed.uri().to_string())
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: Bytes,
    ) -> Result<String> {
        let out = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes))
            .send()
            .await
            .with_context(|| format!("object-store upload-part {key} #{part_number}"))?;
        out.e_tag()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("object-store upload-part {key} #{part_number}: response carried no etag"))
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> Result<u64> {
        let completed_parts: Vec<aws_sdk_s3::types::CompletedPart> = parts
            .iter()
            .map(|(n, etag)| {
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(*n)
                    .e_tag(etag)
                    .build()
            })
            .collect();
        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .with_context(|| format!("object-store complete-multipart {key}"))?;
        // The completion response carries no size; a head on the now-visible
        // object is the authoritative answer.
        self.size(key)
            .await?
            .ok_or_else(|| anyhow!("object-store complete-multipart {key}: object absent after completion"))
    }

    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        match self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            // An unknown upload id (already aborted / completed) is the
            // idempotent success case: the upload is gone either way.
            Err(e) if Self::is_not_found(&e) => Ok(()),
            Err(e) => Err(e).with_context(|| format!("object-store abort-multipart {key}")),
        }
    }

    async fn multipart_exists(&self, key: &str, upload_id: &str) -> Result<bool> {
        match self
            .client
            .list_parts()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .max_parts(1)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if Self::is_not_found(&e) => Ok(false),
            Err(e) => Err(e).with_context(|| format!("object-store list-parts {key}")),
        }
    }
}

/// In-memory `ObjectStore` for tests: a map plus an append-only call log.
/// Dumb by construction (no chunking, no logic), so a test asserts exactly
/// "these keys were written / these calls were made" without a real bucket.
/// Gated behind `test-helpers` so it never links into a release binary.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;
    use anyhow::bail;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    /// One recorded call against the fake, for assertions.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum FakeCall {
        Put { key: String, len: usize },
        Get { key: String },
        GetRange { key: String, start: u64, end: u64 },
        Exists { key: String },
        Size { key: String },
        Delete { key: String },
        List { prefix: String },
        PresignGet { key: String, link: GetLink },
        CreateMultipart { key: String, content_type: String },
        Copy { from: String, to: String },
        PresignPart { key: String, upload_id: String, part_number: i32, part_size: u64 },
        UploadPart { key: String, upload_id: String, part_number: i32, len: usize },
        CompleteMultipart { key: String, upload_id: String, parts: usize },
        AbortMultipart { key: String, upload_id: String },
    }

    /// One in-progress fake multipart upload: the reservations minted by
    /// `presign_part` (the "signed" sizes) and the parts that landed.
    #[derive(Default)]
    struct FakeUpload {
        key: String,
        /// part_number -> the size signed into that part's latest presign.
        reserved: BTreeMap<i32, u64>,
        /// part_number -> (etag, bytes) for landed parts.
        parts: BTreeMap<i32, (String, Bytes)>,
    }

    #[derive(Default)]
    pub struct FakeObjectStore {
        objects: Mutex<BTreeMap<String, Bytes>>,
        uploads: Mutex<BTreeMap<String, FakeUpload>>,
        upload_seq: Mutex<u64>,
        calls: Mutex<Vec<FakeCall>>,
        /// Keys whose next `delete`s fail, with how many still fail, for
        /// exercising callers' reap-retry paths. Dumb injection, no logic.
        fail_deletes: Mutex<BTreeMap<String, u32>>,
        /// Make the next `complete_multipart` fail with the upload left
        /// open (one-shot), like a bucket that was briefly unreachable.
        fail_complete_once: Mutex<bool>,
        /// One-shot pause for the next completion, after the bytes are
        /// swapped in and before it answers: (tell the test it got there,
        /// wait for the test to let it go). Dumb injection, no logic.
        hold_complete: Mutex<Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>,
    }

    impl FakeObjectStore {
        pub fn new() -> Self {
            Self::default()
        }

        /// Make the next `times` calls of `delete(key)` fail (later deletes
        /// succeed).
        pub fn fail_next_deletes(&self, key: &str, times: u32) {
            self.fail_deletes.lock().insert(key.to_string(), times);
        }

        /// Pause the next `complete_multipart` once its object holds the new
        /// bytes. Returns (resolves when it got there, send to let it answer).
        pub fn hold_next_complete(&self) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            *self.hold_complete.lock() = Some((entered_tx, release_rx));
            (entered_rx, release_tx)
        }

        /// Assemble an in-progress upload's parts into its object, the way
        /// the bucket's completion does. The upload is gone afterwards, so
        /// a second completion of it fails like the real backends'
        /// `NoSuchUpload`.
        fn assemble(&self, key: &str, upload_id: &str, parts: &[(i32, String)]) -> Result<u64> {
            let mut uploads = self.uploads.lock();
            let upload = uploads
                .get(upload_id)
                .ok_or_else(|| anyhow!("no such upload {upload_id}"))?;
            if upload.key != key {
                anyhow::bail!("upload {upload_id} is for key {}, not {key}", upload.key);
            }
            // Real S3/SeaweedFS reject a completion with no parts (there is no
            // multipart way to make a zero-byte object) with InvalidPart /
            // MalformedXML. Model that so the empty-object path is never
            // (re)routed through multipart by mistake.
            if parts.is_empty() {
                anyhow::bail!("InvalidPart: multipart completion needs at least one part");
            }
            let mut assembled = Vec::new();
            let mut last = 0;
            for (n, etag) in parts {
                if *n <= last {
                    anyhow::bail!("parts not ascending at #{n}");
                }
                last = *n;
                let (landed_etag, bytes) = upload
                    .parts
                    .get(n)
                    .ok_or_else(|| anyhow!("InvalidPart: part #{n} never landed"))?;
                if landed_etag != etag {
                    anyhow::bail!("InvalidPart: part #{n} etag mismatch");
                }
                // Real S3 also rejects a zero-byte part; the store must never
                // reserve one (an empty object uploads zero parts instead).
                if bytes.is_empty() {
                    anyhow::bail!("InvalidPart: part #{n} is empty (parts must be non-empty)");
                }
                assembled.extend_from_slice(bytes);
            }
            uploads.remove(upload_id);
            let size = assembled.len() as u64;
            self.objects.lock().insert(key.to_string(), Bytes::from(assembled));
            Ok(size)
        }

        /// Make the next `complete_multipart` fail, leaving the upload open.
        pub fn fail_next_complete(&self) {
            *self.fail_complete_once.lock() = true;
        }

        /// Snapshot of every recorded call, in order.
        pub fn calls(&self) -> Vec<FakeCall> {
            self.calls.lock().clone()
        }

        /// Every key currently stored, lexical order.
        pub fn keys(&self) -> Vec<String> {
            self.objects.lock().keys().cloned().collect()
        }

        /// Number of objects currently stored.
        pub fn len(&self) -> usize {
            self.objects.lock().len()
        }

        pub fn is_empty(&self) -> bool {
            self.objects.lock().is_empty()
        }

        /// Upload ids of the multipart uploads still in progress.
        pub fn in_progress_uploads(&self) -> Vec<String> {
            self.uploads.lock().keys().cloned().collect()
        }

        /// The parts landed so far for an in-progress upload, ascending, as
        /// `(part_number, etag, size)`. Test-assertion helper.
        pub fn landed_parts(&self, upload_id: &str) -> Vec<(i32, String, u64)> {
            self.uploads
                .lock()
                .get(upload_id)
                .map(|u| {
                    u.parts
                        .iter()
                        .map(|(n, (etag, bytes))| (*n, etag.clone(), bytes.len() as u64))
                        .collect()
                })
                .unwrap_or_default()
        }

        /// The test's "PUT to a presigned part URL". Enforces exactly what the
        /// real bucket enforces on a signed content-length: the body must be
        /// EXACTLY the size signed into the URL, or the request is rejected
        /// and nothing lands. Returns the part's etag (quotes included, like
        /// the real response header).
        pub fn put_part(&self, url: &str, bytes: Bytes) -> Result<String> {
            let rest = url
                .strip_prefix("fake-multipart://")
                .ok_or_else(|| anyhow!("not a fake part url: {url}"))?;
            let (upload_id, part_number) = rest
                .split_once('#')
                .ok_or_else(|| anyhow!("malformed fake part url: {url}"))?;
            let part_number = part_number.split_once('@').map_or(part_number, |(n, _)| n);
            let part_number: i32 = part_number.parse().context("part number in fake url")?;
            let mut uploads = self.uploads.lock();
            let upload = uploads
                .get_mut(upload_id)
                .ok_or_else(|| anyhow!("no such upload {upload_id} (aborted or completed)"))?;
            let reserved = *upload
                .reserved
                .get(&part_number)
                .ok_or_else(|| anyhow!("part #{part_number} of {upload_id} was never presigned"))?;
            if bytes.len() as u64 != reserved {
                anyhow::bail!(
                    "signature mismatch: part #{part_number} body is {} bytes, signed length is {reserved}",
                    bytes.len()
                );
            }
            let etag = format!("\"fake-etag-{upload_id}-{part_number}-{}\"", bytes.len());
            upload.parts.insert(part_number, (etag.clone(), bytes));
            Ok(etag)
        }
    }

    /// Where a fake link points for `audience`: the base a forwarded
    /// link goes under, a fixed address per audience otherwise, so a test
    /// reads off a link who it was made for.
    fn fake_origin(audience: &PresignAudience) -> String {
        match audience {
            PresignAudience::External => "fake-object-store://external".into(),
            PresignAudience::Install => "fake-object-store://install".into(),
            PresignAudience::Forwarded { base } => format!("{}/{FILES_DOOR}", base.trim_end_matches('/')),
        }
    }

    #[async_trait]
    impl ObjectStore for FakeObjectStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
            self.calls.lock().push(FakeCall::Put { key: key.to_string(), len: bytes.len() });
            self.objects.lock().insert(key.to_string(), bytes);
            Ok(())
        }

        async fn get(&self, key: &str) -> Result<Option<Bytes>> {
            self.calls.lock().push(FakeCall::Get { key: key.to_string() });
            Ok(self.objects.lock().get(key).cloned())
        }

        async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Option<Bytes>> {
            self.calls
                .lock()
                .push(FakeCall::GetRange { key: key.to_string(), start, end });
            let map = self.objects.lock();
            let Some(b) = map.get(key) else { return Ok(None) };
            // Empty range: mirror S3 (empty slice, object exists).
            if end <= start {
                return Ok(Some(Bytes::new()));
            }
            // A non-empty range whose start is at/past the object end is out of
            // bounds. S3 answers 416 -> Err; mirror that here rather than clamping
            // to an empty slice, so a test can't silently rely on the wrong shape.
            if start as usize >= b.len() {
                anyhow::bail!(
                    "object-store get_range {key}: start {start} past object end {}",
                    b.len()
                );
            }
            // End beyond the object is fine (S3 returns bytes up to the end); clamp.
            let s = start as usize;
            let e = (end as usize).min(b.len());
            Ok(Some(b.slice(s..e)))
        }

        async fn exists(&self, key: &str) -> Result<bool> {
            self.calls.lock().push(FakeCall::Exists { key: key.to_string() });
            Ok(self.objects.lock().contains_key(key))
        }

        async fn size(&self, key: &str) -> Result<Option<u64>> {
            self.calls.lock().push(FakeCall::Size { key: key.to_string() });
            Ok(self.objects.lock().get(key).map(|b| b.len() as u64))
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.calls.lock().push(FakeCall::Delete { key: key.to_string() });
            if let Some(left) = self.fail_deletes.lock().get_mut(key).filter(|left| **left > 0) {
                *left -= 1;
                bail!("injected delete failure for {key}");
            }
            self.objects.lock().remove(key);
            Ok(())
        }

        async fn list(&self, prefix: &str) -> Result<Vec<ObjectEntry>> {
            self.calls.lock().push(FakeCall::List { prefix: prefix.to_string() });
            let map = self.objects.lock();
            Ok(map
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| ObjectEntry { key: k.clone(), size: v.len() as u64 })
                .collect())
        }

        async fn presign_get(&self, key: &str, link: &GetLink) -> Result<String> {
            self.calls.lock().push(FakeCall::PresignGet { key: key.to_string(), link: link.clone() });
            // A deterministic fake URL that encodes the grant, so a test can
            // assert what was minted without a real signer.
            let at = link.signed_at.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or_default();
            Ok(format!("{}/{key}?at={at}&ttl={}", fake_origin(&link.audience), link.ttl_secs))
        }

        /// One piece, so `progress` hears of it once.
        async fn copy(&self, from: &str, to: &str, progress: &dyn CopyProgress) -> Result<u64> {
            self.calls.lock().push(FakeCall::Copy { from: from.to_string(), to: to.to_string() });
            let size = {
                let mut objects = self.objects.lock();
                let bytes = objects.get(from).cloned().ok_or_else(|| anyhow!("no object at {from}"))?;
                let size = bytes.len() as u64;
                objects.insert(to.to_string(), bytes);
                size
            };
            progress.copied_piece().await?;
            Ok(size)
        }

        async fn create_multipart(&self, key: &str, content_type: &str) -> Result<String> {
            self.calls
                .lock()
                .push(FakeCall::CreateMultipart { key: key.to_string(), content_type: content_type.to_string() });
            let mut seq = self.upload_seq.lock();
            *seq += 1;
            let id = format!("fake-upload-{}", *seq);
            self.uploads
                .lock()
                .insert(id.clone(), FakeUpload { key: key.to_string(), ..Default::default() });
            Ok(id)
        }

        async fn presign_part(
            &self,
            key: &str,
            upload_id: &str,
            part_number: i32,
            part_size: u64,
            audience: &PresignAudience,
            _ttl_secs: u64,
        ) -> Result<String> {
            self.calls.lock().push(FakeCall::PresignPart {
                key: key.to_string(),
                upload_id: upload_id.to_string(),
                part_number,
                part_size,
            });
            let mut uploads = self.uploads.lock();
            let upload = uploads
                .get_mut(upload_id)
                .ok_or_else(|| anyhow!("no such upload {upload_id}"))?;
            if upload.key != key {
                anyhow::bail!("upload {upload_id} is for key {}, not {key}", upload.key);
            }
            // Record the "signed" length; the latest presign for a part wins,
            // exactly like re-presigning after a URL expiry.
            upload.reserved.insert(part_number, part_size);
            // A forwarded part's link says where it goes, before the part
            // the fake reads back out of it.
            Ok(match audience {
                PresignAudience::Forwarded { base } => format!("fake-multipart://{upload_id}#{part_number}@{base}"),
                _ => format!("fake-multipart://{upload_id}#{part_number}"),
            })
        }

        async fn upload_part(
            &self,
            key: &str,
            upload_id: &str,
            part_number: i32,
            bytes: Bytes,
        ) -> Result<String> {
            self.calls.lock().push(FakeCall::UploadPart {
                key: key.to_string(),
                upload_id: upload_id.to_string(),
                part_number,
                len: bytes.len(),
            });
            let mut uploads = self.uploads.lock();
            let upload = uploads
                .get_mut(upload_id)
                .ok_or_else(|| anyhow!("no such upload {upload_id}"))?;
            if upload.key != key {
                anyhow::bail!("upload {upload_id} is for key {}, not {key}", upload.key);
            }
            // Direct (in-process) part upload: no presign, no signed length; the
            // etag shape matches the presigned PUT helper's so completion code
            // can't tell the two paths apart, exactly like the real bucket.
            let etag = format!("\"fake-etag-{upload_id}-{part_number}-{}\"", bytes.len());
            upload.parts.insert(part_number, (etag.clone(), bytes));
            Ok(etag)
        }

        async fn complete_multipart(
            &self,
            key: &str,
            upload_id: &str,
            parts: &[(i32, String)],
        ) -> Result<u64> {
            self.calls.lock().push(FakeCall::CompleteMultipart {
                key: key.to_string(),
                upload_id: upload_id.to_string(),
                parts: parts.len(),
            });
            if std::mem::take(&mut *self.fail_complete_once.lock()) {
                bail!("injected: the bucket is unreachable");
            }
            let size = self.assemble(key, upload_id, parts)?;
            let hold = self.hold_complete.lock().take();
            if let Some((entered, release)) = hold {
                let _ = entered.send(());
                let _ = release.await;
            }
            Ok(size)
        }

        async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
            self.calls.lock().push(FakeCall::AbortMultipart {
                key: key.to_string(),
                upload_id: upload_id.to_string(),
            });
            // Removing an absent upload is the idempotent success case.
            self.uploads.lock().remove(upload_id);
            Ok(())
        }

        async fn multipart_exists(&self, _key: &str, upload_id: &str) -> Result<bool> {
            Ok(self.uploads.lock().contains_key(upload_id))
        }
    }
}

/// Convenience: an `Arc<dyn ObjectStore>` is what the slot threads through
/// the composition root, so callers name this alias rather than spelling
/// the trait object everywhere.
pub type SharedObjectStore = Arc<dyn ObjectStore>;

#[cfg(test)]
mod tests {
    use super::fake::{FakeCall, FakeObjectStore};
    use super::*;

    /// Each audience is signed for the endpoint that caller reaches the
    /// store at: a browser the public one, the install (its runtime and
    /// its workers) the one the runtime does its I/O on, and a forwarded
    /// link the forwarder's base.
    #[tokio::test]
    async fn each_audience_is_signed_for_its_own_endpoint() {
        let store = S3ObjectStore::clients(&ObjectStoreConfig {
            endpoint_url: "http://127.0.0.1:14115".into(),
            bucket: "weft".into(),
            region: "us-east-1".into(),
            access_key_id: "k".into(),
            secret_access_key: "s".into(),
            force_path_style: true,
            public_endpoint_url: Some("https://files.example.com".into()),
        })
        .await;
        let url = |audience| {
            let store = &store;
            async move { store.presign_get("a/b", &link(audience)).await.unwrap() }
        };
        assert!(url(PresignAudience::External).await.starts_with("https://files.example.com/weft/a/b?"));
        assert!(url(PresignAudience::Install).await.starts_with("http://127.0.0.1:14115/weft/a/b?"));
        assert!(url(PresignAudience::Forwarded { base: "https://shop.example/weft".into() })
            .await
            .starts_with("https://shop.example/weft/files/a/b?"));
        // A local install's containers, at its internal port as they
        // reach it.
        assert!(url(PresignAudience::Forwarded { base: "http://host.docker.internal:14113".into() })
            .await
            .starts_with("http://host.docker.internal:14113/files/a/b?"));
    }

    fn link(audience: PresignAudience) -> GetLink {
        GetLink {
            audience,
            signed_at: std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            ttl_secs: 900,
            content_type: Some("video/mp4".into()),
            disposition: Some("attachment; filename=\"a.mp4\"".into()),
        }
    }

    fn store_at(public: &str) -> ObjectStoreConfig {
        ObjectStoreConfig {
            endpoint_url: "http://127.0.0.1:14115".into(),
            bucket: "weft".into(),
            region: "us-east-1".into(),
            access_key_id: "k".into(),
            secret_access_key: "s".into(),
            force_path_style: true,
            public_endpoint_url: Some(public.into()),
        }
    }

    /// A forwarded link is the link signed for the base's host, moved
    /// under the base's path and the forwarder's door: the store sees the
    /// path it signed, so the signature is the one a link straight to that
    /// host carries.
    #[tokio::test]
    async fn a_forwarded_link_carries_the_signature_of_its_host() {
        let store = S3ObjectStore::clients(&store_at("https://shop.example")).await;
        let direct = store.presign_get("runtime/t/k", &link(PresignAudience::External)).await.unwrap();
        let forwarded = store
            .presign_get("runtime/t/k", &link(PresignAudience::Forwarded { base: "https://shop.example/weft/".into() }))
            .await
            .unwrap();
        let (direct_path, direct_query) = direct.split_once('?').unwrap();
        let (forwarded_path, forwarded_query) = forwarded.split_once('?').unwrap();
        assert_eq!(direct_path, "https://shop.example/weft/runtime/t/k");
        assert_eq!(forwarded_path, "https://shop.example/weft/files/runtime/t/k");
        assert_eq!(direct_query, forwarded_query);
    }

    /// The same link asked twice is the same URL (what keeps a link stable
    /// across answers), and it carries the answer's type and disposition.
    #[tokio::test]
    async fn a_link_is_signed_the_same_for_the_same_moment() {
        let store = S3ObjectStore::clients(&store_at("https://files.example.com")).await;
        let once = store.presign_get("a/b", &link(PresignAudience::External)).await.unwrap();
        assert_eq!(once, store.presign_get("a/b", &link(PresignAudience::External)).await.unwrap());
        assert!(once.contains("response-content-type=video%2Fmp4"), "{once}");
        assert!(once.contains("response-content-disposition=attachment"), "{once}");
        assert!(once.contains("X-Amz-Date=20231114T221320Z"), "{once}");
        let later = GetLink { signed_at: std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_900), ..link(PresignAudience::External) };
        assert_ne!(once, store.presign_get("a/b", &later).await.unwrap());
    }

    #[test]
    fn a_base_is_an_http_address_and_a_link_moves_under_it() {
        assert_eq!(base_origin("https://shop.example/weft").unwrap(), "https://shop.example");
        assert_eq!(base_origin("http://127.0.0.1:14111").unwrap(), "http://127.0.0.1:14111");
        for bad in ["ftp://x.example", "not a url", "https://u:p@x.example", "https://x.example/?q=1"] {
            assert!(base_origin(bad).is_err(), "{bad}");
        }
        assert_eq!(
            forwarded_link("https://t.example", "weft", "https://t.example/weft/runtime/a?X=1").unwrap(),
            "https://t.example/files/runtime/a?X=1"
        );
        assert!(forwarded_link("https://t.example", "weft", "https://t.example/other/runtime/a").is_err());
    }

    #[test]
    fn a_big_copy_is_cut_into_pieces_that_cover_it_once() {
        assert_eq!(copy_ranges(10), vec![(0, 9)]);
        assert_eq!(copy_ranges(COPY_PIECE_BYTES), vec![(0, COPY_PIECE_BYTES - 1)]);
        assert_eq!(
            copy_ranges(2 * COPY_PIECE_BYTES + 5),
            vec![(0, COPY_PIECE_BYTES - 1), (COPY_PIECE_BYTES, 2 * COPY_PIECE_BYTES - 1), (2 * COPY_PIECE_BYTES, 2 * COPY_PIECE_BYTES + 4)]
        );
        assert!(copy_ranges(0).is_empty());
        // Every piece but the last is a whole one: no smaller than the
        // 5 MiB a multipart part takes.
        let pieces = copy_ranges(3 * COPY_PIECE_BYTES + 1);
        assert!(pieces[..pieces.len() - 1].iter().all(|(start, end)| end - start + 1 >= 5 * 1024 * 1024));
    }

    #[test]
    fn a_copy_source_encodes_each_key_segment() {
        assert_eq!(
            copy_source("weft", "runtime/t 1/a+b \u{e9}.txt"),
            "weft/runtime/t%201/a%2Bb%20%C3%A9.txt"
        );
    }

    #[tokio::test]
    async fn put_then_get_round_trips() {
        let store = FakeObjectStore::new();
        store.put("a", Bytes::from_static(b"hello")).await.unwrap();
        assert_eq!(store.get("a").await.unwrap().as_deref(), Some(&b"hello"[..]));
        assert!(store.exists("a").await.unwrap());
        assert_eq!(store.size("a").await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn missing_key_is_none_not_err() {
        let store = FakeObjectStore::new();
        assert_eq!(store.get("nope").await.unwrap(), None);
        assert!(!store.exists("nope").await.unwrap());
        assert_eq!(store.size("nope").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let store = FakeObjectStore::new();
        store.put("a", Bytes::from_static(b"x")).await.unwrap();
        store.delete("a").await.unwrap();
        store.delete("a").await.unwrap();
        assert!(!store.exists("a").await.unwrap());
    }

    #[tokio::test]
    async fn list_filters_by_prefix_in_key_order() {
        let store = FakeObjectStore::new();
        store.put("chunks/b", Bytes::from_static(b"1")).await.unwrap();
        store.put("chunks/a", Bytes::from_static(b"22")).await.unwrap();
        store.put("trees/x", Bytes::from_static(b"333")).await.unwrap();
        let listed = store.list("chunks/").await.unwrap();
        assert_eq!(
            listed,
            vec![
                ObjectEntry { key: "chunks/a".into(), size: 2 },
                ObjectEntry { key: "chunks/b".into(), size: 1 },
            ]
        );
    }

    #[tokio::test]
    async fn get_range_honors_the_bounds_contract() {
        let store = FakeObjectStore::new();
        store.put("a", Bytes::from_static(b"0123456789")).await.unwrap();
        assert_eq!(store.get_range("a", 2, 5).await.unwrap().as_deref(), Some(&b"234"[..]));
        // End past the object clamps to the object length (S3 returns available
        // bytes up to the end).
        assert_eq!(store.get_range("a", 8, 100).await.unwrap().as_deref(), Some(&b"89"[..]));
        // An empty range (end <= start) is an empty slice, object exists.
        assert_eq!(store.get_range("a", 5, 5).await.unwrap().as_deref(), Some(&b""[..]));
        // Start at/past the object end for a NON-empty range is out of bounds: S3
        // answers 416, so the contract is an Err, NOT an empty read (the fake used
        // to clamp to empty, hiding the real error path).
        assert!(store.get_range("a", 50, 60).await.is_err());
        // Absent object is None regardless of range.
        assert_eq!(store.get_range("missing", 0, 4).await.unwrap(), None);
    }

    #[tokio::test]
    async fn presign_get_encodes_audience_time_and_ttl() {
        let store = FakeObjectStore::new();
        let url = store.presign_get("k", &link(PresignAudience::External)).await.unwrap();
        assert_eq!(url, "fake-object-store://external/k?at=1700000000&ttl=900");
    }

    #[tokio::test]
    async fn multipart_round_trips_in_part_order() {
        let store = FakeObjectStore::new();
        let id = store.create_multipart("k", "text/plain").await.unwrap();
        let u1 = store
            .presign_part("k", &id, 1, 3, &PresignAudience::Install, 600)
            .await
            .unwrap();
        let u2 = store
            .presign_part("k", &id, 2, 2, &PresignAudience::Install, 600)
            .await
            .unwrap();
        // Land them out of order; completion order comes from the part list.
        let e2 = store.put_part(&u2, Bytes::from_static(b"de")).unwrap();
        let e1 = store.put_part(&u1, Bytes::from_static(b"abc")).unwrap();
        assert!(store.multipart_exists("k", &id).await.unwrap(), "open until completed");
        let parts = [(1, e1), (2, e2)];
        let size = store.complete_multipart("k", &id, &parts).await.unwrap();
        assert_eq!(size, 5);
        assert_eq!(store.get("k").await.unwrap().as_deref(), Some(&b"abcde"[..]));
        assert!(store.in_progress_uploads().is_empty());
        // Like S3, GCS and SeaweedFS: a completed upload is gone, so it no
        // longer lists and a second completion is refused.
        assert!(!store.multipart_exists("k", &id).await.unwrap());
        assert!(store.complete_multipart("k", &id, &parts).await.is_err());
    }

    #[tokio::test]
    async fn part_with_wrong_size_is_rejected_by_the_signed_length() {
        let store = FakeObjectStore::new();
        let id = store.create_multipart("k", "text/plain").await.unwrap();
        let url = store
            .presign_part("k", &id, 1, 3, &PresignAudience::Install, 600)
            .await
            .unwrap();
        let err = store.put_part(&url, Bytes::from_static(b"toolong")).unwrap_err();
        assert!(err.to_string().contains("signature mismatch"), "{err}");
        // Nothing landed.
        assert!(store.landed_parts(&id).is_empty());
    }

    #[tokio::test]
    async fn complete_with_wrong_or_missing_etag_fails() {
        let store = FakeObjectStore::new();
        let id = store.create_multipart("k", "text/plain").await.unwrap();
        let url = store
            .presign_part("k", &id, 1, 1, &PresignAudience::Install, 600)
            .await
            .unwrap();
        store.put_part(&url, Bytes::from_static(b"x")).unwrap();
        assert!(store
            .complete_multipart("k", &id, &[(1, "\"wrong\"".into())])
            .await
            .is_err());
        assert!(store.complete_multipart("k", &id, &[(2, "\"e\"".into())]).await.is_err());
    }

    #[tokio::test]
    async fn abort_drops_the_upload_and_is_idempotent() {
        let store = FakeObjectStore::new();
        let id = store.create_multipart("k", "text/plain").await.unwrap();
        let url = store
            .presign_part("k", &id, 1, 1, &PresignAudience::Install, 600)
            .await
            .unwrap();
        store.put_part(&url, Bytes::from_static(b"x")).unwrap();
        store.abort_multipart("k", &id).await.unwrap();
        store.abort_multipart("k", &id).await.unwrap();
        assert!(store.in_progress_uploads().is_empty());
        // A PUT racing/after the abort fails: the upload is gone.
        assert!(store.put_part(&url, Bytes::from_static(b"x")).is_err());
        assert!(!store.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn landed_parts_reports_what_arrived() {
        let store = FakeObjectStore::new();
        let id = store.create_multipart("k", "text/plain").await.unwrap();
        for n in [1i32, 3] {
            let url = store
                .presign_part("k", &id, n, 2, &PresignAudience::Install, 600)
                .await
                .unwrap();
            store.put_part(&url, Bytes::from_static(b"xy")).unwrap();
        }
        let landed: Vec<i32> = store.landed_parts(&id).into_iter().map(|(n, _, _)| n).collect();
        assert_eq!(landed, vec![1, 3]);
    }

    #[tokio::test]
    async fn calls_are_recorded_in_order() {
        let store = FakeObjectStore::new();
        store.put("a", Bytes::from_static(b"x")).await.unwrap();
        store.get("a").await.unwrap();
        let calls = store.calls();
        assert_eq!(calls[0], FakeCall::Put { key: "a".into(), len: 1 });
        assert_eq!(calls[1], FakeCall::Get { key: "a".into() });
    }
}
