//! The object-storage seam: a flat, content-keyed blob store both the
//! dispatcher AND the worker (weft-engine) talk to. It lives HERE in
//! platform-traits, not in the dispatcher alongside the other policy
//! seams, on purpose: the runtime `ctx.storage` plane runs INSIDE the
//! worker, and weft-engine does not (and must not) depend on
//! weft-dispatcher. A cross-cutting capability both pods need is exactly
//! what this crate is for (same as `KubeClient` and `Clock`).
//!
//! The store is deliberately dumb: keyed put / get / head / delete /
//! list / presign over opaque bytes. It is the deploy-time SLOT the
//! cluster is handed (S3-compatible endpoint + bucket + creds), mirroring
//! how the image registry is a slot: the bundled default points it at a
//! SeaweedFS service, and it can be pointed at any S3-compatible bucket
//! (e.g. GCS) instead. Everything above it (content-defined chunking,
//! the tree/version model, the runtime storage scopes) is built on top in
//! higher crates and is backing-agnostic.
//!
//! There is no "local default that fails loud" here: object storage is a
//! hard dependency of a running cluster (the source plane AND the runtime
//! plane both need it), so a deployment without a configured store is a
//! deploy error, surfaced where the slot is constructed, not a silent
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

/// Which network the presigned URL will be used FROM. An S3 signature is bound to
/// the host in the URL, so the store must sign for the host the caller can reach.
/// The two audiences differ only when the bucket sits behind a split-horizon setup
/// (a browser reaches it at a public host; an in-cluster worker reaches it at the
/// internal host); the local-dev SeaweedFS port-forward is exactly that case. A
/// bucket whose endpoint is already publicly reachable in-cluster collapses both
/// to the same URL, so this stays a no-op there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresignAudience {
    /// A caller OUTSIDE the cluster (a browser, an external API): sign for the
    /// public endpoint (`WEFT_OBJECT_STORE_PUBLIC_ENDPOINT`).
    External,
    /// A caller INSIDE the cluster (a worker running node code): sign for the
    /// I/O endpoint the broker itself uses (`WEFT_OBJECT_STORE_ENDPOINT`).
    Internal,
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
/// bytes. Implementations are S3-compatible HTTP clients (the production
/// impl is `S3ObjectStore` over the AWS SDK; tests use `FakeObjectStore`).
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

    /// Mint a presigned DOWNLOAD (GET) URL for `key`, valid for `ttl_secs`
    /// seconds, signed for `audience`'s network. The URL grants exactly that
    /// one read on that one object and expires on its own; the caller uses it
    /// directly without ever holding a credential.
    async fn presign_get(
        &self,
        key: &str,
        audience: PresignAudience,
        ttl_secs: u64,
    ) -> Result<String>;

    /// Mint a presigned single-shot UPLOAD (PUT) URL for `key`, with the
    /// body's EXACT byte size signed into it: the bucket rejects any body
    /// that is not exactly `content_length` bytes. There is deliberately NO
    /// unsized upload URL on this trait: every write grant carries a signed
    /// length (this one for single objects, `presign_part` for multipart
    /// parts), so a size a caller reserved can never be exceeded.
    async fn presign_put(
        &self,
        key: &str,
        content_length: u64,
        audience: PresignAudience,
        ttl_secs: u64,
    ) -> Result<String>;

    /// Start a multipart upload for `key`. Returns the upload id: the handle
    /// every subsequent part/complete/abort call names, and the resume handle
    /// a caller persists to pick the upload back up after an interruption.
    /// The upload holds no visible object until `complete_multipart`.
    async fn create_multipart(&self, key: &str) -> Result<String>;

    /// Mint a presigned URL for uploading ONE part, with the part's EXACT
    /// byte size signed into it (`Content-Length` is a signed header): the
    /// bucket rejects any body that is not exactly `part_size` bytes. That
    /// signed size is what lets a caller reserve bytes against a quota
    /// BEFORE handing out the URL and trust the bucket to enforce the
    /// reservation. `audience` picks the signing host, same as `presign`.
    async fn presign_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part_size: u64,
        audience: PresignAudience,
        ttl_secs: u64,
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
}

/// The deploy-time slot config: where the bucket lives and how to reach it.
/// This is the one piece of storage configuration, mirroring how `RegistryConfig`
/// is the image-registry slot. The default fills it from env pointing at the
/// bundled SeaweedFS service; it can point at any S3-compatible endpoint (e.g.
/// GCS) instead.
///
/// `endpoint_url` is what makes this S3-compatible-rather-than-AWS: it
/// overrides the SDK's default AWS endpoint so the same client talks to GCS,
/// SeaweedFS, or any S3 API. `force_path_style` (bucket in the PATH, not the
/// hostname) is required by SeaweedFS and most non-AWS S3 servers, so it
/// defaults on.
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// The S3-compatible endpoint URL (e.g. the in-cluster SeaweedFS service,
    /// or the GCS/AWS regional endpoint).
    pub endpoint_url: String,
    /// The single bucket every object lives in (prefixes namespace the
    /// planes: `chunks/`, `trees/`, `runtime/<tenant>/...`).
    pub bucket: String,
    /// The region to sign requests for. Non-AWS endpoints ignore the value
    /// but the signer still needs one; default `us-east-1`.
    pub region: String,
    /// Static access key id. For GCS this is an HMAC key; for SeaweedFS the
    /// configured admin key.
    pub access_key_id: String,
    /// Static secret access key paired with `access_key_id`.
    pub secret_access_key: String,
    /// Bucket-in-path addressing. On for SeaweedFS and most S3-compatible
    /// servers; AWS/GCS tolerate it too.
    pub force_path_style: bool,
    /// The BROWSER/host-reachable endpoint presigned URLs are signed for, when it
    /// differs from `endpoint_url`. The broker reaches the bucket over the
    /// in-cluster `endpoint_url` for its own I/O, but a presigned download URL is
    /// handed to an external caller (a browser, an external API, the e2e on the
    /// host) that cannot resolve an in-cluster DNS name, so it must be signed for
    /// a reachable host. `None` (an `endpoint_url` that is already public) means
    /// presign against `endpoint_url` directly. For local dev / e2e this is the
    /// host-forwarded SeaweedFS address.
    pub public_endpoint_url: Option<String>,
}

impl ObjectStoreConfig {
    /// Read the slot from env, or `Ok(None)` if unconfigured. A cluster
    /// without a storage slot is a deploy error surfaced at the composition
    /// root (object storage is a hard dependency), NOT a silent default.
    ///
    /// `WEFT_OBJECT_STORE_ENDPOINT` is the presence switch: if unset, no
    /// slot. If set, bucket + creds are required (fail loud if missing).
    pub fn from_env() -> Result<Option<Self>> {
        let Some(endpoint_url) =
            std::env::var("WEFT_OBJECT_STORE_ENDPOINT").ok().filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let req = |name: &str| -> Result<String> {
            std::env::var(name)
                .ok()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("{name} must be set when WEFT_OBJECT_STORE_ENDPOINT is set"))
        };
        let region = std::env::var("WEFT_OBJECT_STORE_REGION")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "us-east-1".to_string());
        // Path-style defaults ON (SeaweedFS + most non-AWS need it); only an
        // explicit "false" turns it off.
        let force_path_style = std::env::var("WEFT_OBJECT_STORE_FORCE_PATH_STYLE")
            .ok()
            .map(|s| s != "false" && s != "0")
            .unwrap_or(true);
        let public_endpoint_url = std::env::var("WEFT_OBJECT_STORE_PUBLIC_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty());
        Ok(Some(Self {
            endpoint_url,
            bucket: req("WEFT_OBJECT_STORE_BUCKET")?,
            region,
            access_key_id: req("WEFT_OBJECT_STORE_ACCESS_KEY")?,
            secret_access_key: req("WEFT_OBJECT_STORE_SECRET_KEY")?,
            force_path_style,
            public_endpoint_url,
        }))
    }
}

/// Build the `ObjectStore` slot from env, the one place every binary that needs
/// the store (the dispatcher and the broker) constructs it, so they read the SAME env and
/// fail loud identically. `Ok(None)` iff no slot is configured (open weft with
/// no object store); `Ok(Some(store))` when the slot is set; `Err` iff the slot
/// is half-configured (endpoint set but bucket/creds missing).
pub async fn object_store_from_env() -> Result<Option<SharedObjectStore>> {
    match ObjectStoreConfig::from_env()? {
        Some(cfg) => Ok(Some(Arc::new(S3ObjectStore::new(&cfg).await?))),
        None => Ok(None),
    }
}

/// Production `ObjectStore` over the AWS Rust SDK's S3 client. The SDK is used
/// purely as the S3-protocol client (request building + SigV4 signing); the
/// endpoint override points it at whatever S3-compatible server the slot names
/// (GCS, SeaweedFS, AWS, a client bucket). No AWS service is implied.
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
        let creds = aws_credential_types::Credentials::from_keys(
            &cfg.access_key_id,
            &cfg.secret_access_key,
            None,
        );
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
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
        let store = Self {
            client: client_for(&cfg.endpoint_url),
            // Only build a separate presign client when the public endpoint
            // genuinely differs from the I/O endpoint.
            presign_client: cfg
                .public_endpoint_url
                .as_ref()
                .filter(|p| p.as_str() != cfg.endpoint_url)
                .map(|p| client_for(p)),
            bucket: cfg.bucket.clone(),
        };
        store.ensure_bucket().await?;
        Ok(store)
    }

    /// Ensure the slot's bucket exists and is reachable (idempotent). SeaweedFS
    /// does not auto-create a bucket on first PutObject, so the store creates it
    /// once at startup; an already-existing / already-owned bucket (the common
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
                // benign race (another pod created it between our head and
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

    /// The client whose endpoint the presigned-URL caller can actually reach:
    ///   - External (browser / external API): the public-endpoint client when
    ///     one is configured, else the I/O client (a bucket whose endpoint is
    ///     already publicly reachable).
    ///   - Internal (in-cluster worker): always the I/O client, the same
    ///     endpoint the broker uses for its own bucket access.
    fn signing_client(&self, audience: PresignAudience) -> &aws_sdk_s3::Client {
        match audience {
            PresignAudience::External => self.presign_client.as_ref().unwrap_or(&self.client),
            PresignAudience::Internal => &self.client,
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
                Ok(Some(len as u64))
            }
            Err(e) if Self::is_not_found(&e) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("object-store head {key}")),
        }
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

    async fn presign_get(
        &self,
        key: &str,
        audience: PresignAudience,
        ttl_secs: u64,
    ) -> Result<String> {
        let cfg = aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(ttl_secs))
            .with_context(|| "build presigning config")?;
        let uri = self
            .signing_client(audience)
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(cfg)
            .await
            .with_context(|| format!("presign GET {key}"))?;
        Ok(uri.uri().to_string())
    }

    async fn presign_put(
        &self,
        key: &str,
        content_length: u64,
        audience: PresignAudience,
        ttl_secs: u64,
    ) -> Result<String> {
        let cfg = aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(ttl_secs))
            .with_context(|| "build presigning config")?;
        // Setting content_length EXPLICITLY makes it a SIGNED header (the
        // signer only excludes it when unset): the bucket rejects any body
        // that is not exactly this many bytes.
        let uri = self
            .signing_client(audience)
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_length(i64::try_from(content_length).map_err(|_| {
                anyhow!("presign PUT {key}: content length {content_length} overflows i64")
            })?)
            .presigned(cfg)
            .await
            .with_context(|| format!("presign PUT {key}"))?;
        Ok(uri.uri().to_string())
    }

    async fn create_multipart(&self, key: &str) -> Result<String> {
        let out = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("object-store create-multipart {key}"))?;
        out.upload_id()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("object-store create-multipart {key}: response carried no upload id"))
    }

    async fn presign_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part_size: u64,
        audience: PresignAudience,
        ttl_secs: u64,
    ) -> Result<String> {
        let cfg = aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(ttl_secs))
            .with_context(|| "build presigning config")?;
        // Setting content_length EXPLICITLY makes it a SIGNED header (the
        // signer only excludes it when unset), which is the whole point: the
        // bucket rejects a body that is not exactly `part_size` bytes.
        let presigned = self
            .signing_client(audience)
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .content_length(i64::try_from(part_size).map_err(|_| {
                anyhow!("object-store presign-part {key} #{part_number}: part size {part_size} overflows i64")
            })?)
            .presigned(cfg)
            .await
            .with_context(|| format!("object-store presign-part {key} #{part_number}"))?;
        Ok(presigned.uri().to_string())
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
        PresignGet { key: String, audience: PresignAudience, ttl_secs: u64 },
        PresignPut { key: String, content_length: u64, audience: PresignAudience, ttl_secs: u64 },
        CreateMultipart { key: String },
        PresignPart { key: String, upload_id: String, part_number: i32, part_size: u64 },
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
        /// Keys whose NEXT `delete` fails (one-shot, then cleared), for
        /// exercising callers' reap-retry paths. Dumb injection, no logic.
        fail_delete_once: Mutex<std::collections::BTreeSet<String>>,
    }

    impl FakeObjectStore {
        pub fn new() -> Self {
            Self::default()
        }

        /// Make the next `delete(key)` fail once (subsequent deletes succeed).
        pub fn fail_next_delete(&self, key: &str) {
            self.fail_delete_once.lock().insert(key.to_string());
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
            if self.fail_delete_once.lock().remove(key) {
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

        async fn presign_get(
            &self,
            key: &str,
            audience: PresignAudience,
            ttl_secs: u64,
        ) -> Result<String> {
            self.calls
                .lock()
                .push(FakeCall::PresignGet { key: key.to_string(), audience, ttl_secs });
            // A deterministic fake URL that encodes the grant, so a test can
            // assert what was minted without a real signer.
            Ok(format!("fake-object-store://GET/{key}?ttl={ttl_secs}"))
        }

        async fn presign_put(
            &self,
            key: &str,
            content_length: u64,
            audience: PresignAudience,
            ttl_secs: u64,
        ) -> Result<String> {
            self.calls.lock().push(FakeCall::PresignPut {
                key: key.to_string(),
                content_length,
                audience,
                ttl_secs,
            });
            Ok(format!("fake-object-store://PUT/{key}?len={content_length}&ttl={ttl_secs}"))
        }

        async fn create_multipart(&self, key: &str) -> Result<String> {
            self.calls.lock().push(FakeCall::CreateMultipart { key: key.to_string() });
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
            _audience: PresignAudience,
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
            Ok(format!("fake-multipart://{upload_id}#{part_number}"))
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

        async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
            self.calls.lock().push(FakeCall::AbortMultipart {
                key: key.to_string(),
                upload_id: upload_id.to_string(),
            });
            // Removing an absent upload is the idempotent success case.
            self.uploads.lock().remove(upload_id);
            Ok(())
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
    async fn presign_encodes_method_length_and_ttl() {
        let store = FakeObjectStore::new();
        let url = store.presign_get("k", PresignAudience::External, 600).await.unwrap();
        assert!(url.contains("GET"));
        assert!(url.contains("ttl=600"));
        assert!(url.contains("/k"));
        let url = store.presign_put("k", 42, PresignAudience::External, 600).await.unwrap();
        assert!(url.contains("PUT"));
        assert!(url.contains("len=42"));
    }

    #[tokio::test]
    async fn multipart_round_trips_in_part_order() {
        let store = FakeObjectStore::new();
        let id = store.create_multipart("k").await.unwrap();
        let u1 = store
            .presign_part("k", &id, 1, 3, PresignAudience::Internal, 600)
            .await
            .unwrap();
        let u2 = store
            .presign_part("k", &id, 2, 2, PresignAudience::Internal, 600)
            .await
            .unwrap();
        // Land them out of order; completion order comes from the part list.
        let e2 = store.put_part(&u2, Bytes::from_static(b"de")).unwrap();
        let e1 = store.put_part(&u1, Bytes::from_static(b"abc")).unwrap();
        let size = store.complete_multipart("k", &id, &[(1, e1), (2, e2)]).await.unwrap();
        assert_eq!(size, 5);
        assert_eq!(store.get("k").await.unwrap().as_deref(), Some(&b"abcde"[..]));
        assert!(store.in_progress_uploads().is_empty());
    }

    #[tokio::test]
    async fn part_with_wrong_size_is_rejected_by_the_signed_length() {
        let store = FakeObjectStore::new();
        let id = store.create_multipart("k").await.unwrap();
        let url = store
            .presign_part("k", &id, 1, 3, PresignAudience::Internal, 600)
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
        let id = store.create_multipart("k").await.unwrap();
        let url = store
            .presign_part("k", &id, 1, 1, PresignAudience::Internal, 600)
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
        let id = store.create_multipart("k").await.unwrap();
        let url = store
            .presign_part("k", &id, 1, 1, PresignAudience::Internal, 600)
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
        let id = store.create_multipart("k").await.unwrap();
        for n in [1i32, 3] {
            let url = store
                .presign_part("k", &id, n, 2, PresignAudience::Internal, 600)
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
