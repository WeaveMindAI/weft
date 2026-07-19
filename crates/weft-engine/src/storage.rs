//! Worker-side storage data path: the worker's client of the broker's
//! runtime-file plane (`ctx.storage`).
//!
//! The worker holds NO bucket credentials. Every CONTROL call is an authenticated
//! HTTP call to the broker (the same broker the worker already uses for the journal
//! / tasks): the broker verifies the worker's identity, runs the key wall, enforces
//! quota, mints keys, and records metadata. But BYTES never flow through the broker.
//!
//! A put is MULTIPART, driven entirely in here (node code just calls `ctx.put`):
//! `upload/begin` mints the key (charging a known total size against the quota up
//! front), then the stream is sliced into parts; each part is reserved via
//! `upload/parts` (which returns a URL signed with the part's EXACT size; a stream
//! is quota-charged per part here), PUT directly to the bucket (with a bounded
//! retry that re-presigns via `upload/resume` on a dropped connection or expired
//! URL), and its etag reported via `upload/part-done`; `upload/complete` assembles
//! the file. Any unrecoverable failure aborts the upload (`upload/abort`, freeing
//! the quota reservation) and surfaces loud. A get is simpler: `download-url`
//! returns the metadata + a presigned GET URL, and the worker reads the bytes
//! DIRECTLY from the bucket. Presigned URLs are signed for the worker's in-cluster
//! endpoint.
//!
//! There is no "ensure the storage exists" handshake and no retry-on-
//! unreachable dance: the broker is a long-lived cluster service, always up
//! like the journal, so an unreachable broker is a real failure surfaced to
//! the node, not a transient to paper over.

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use serde_json::Value;

use weft_core::storage::{
    ByteRange, ByteStream, DownloadUrlResponse, KeepTtl, ListFilesResponse, PresignResponse,
    PresignedPart, StorageScope, StoredFile, StoredFileMeta, UploadBeginResponse,
    UploadPartsResponse, UploadResumeResponse,
};
use weft_core::error::{WeftError, WeftResult};
use weft_core::Color;

/// The color claim header the worker stamps on every storage call.
// SYNC: HDR_COLOR <-> crates/weft-broker/src/runtime_storage.rs (HDR_COLOR)
const HDR_COLOR: &str = "x-weft-color";

/// Color-parameterized storage surface the `ContextHandle` storage methods
/// delegate to. One impl per worker process; Layer-3 tests inject a fake.
#[async_trait]
pub trait WorkerStorageOps: Send + Sync {
    async fn put(
        &self,
        color: Color,
        scope: &StorageScope,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<Value>;
    async fn get(
        &self,
        color: Color,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)>;
    async fn delete(&self, color: Color, key: &str) -> WeftResult<()>;
    async fn list(&self, color: Color, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>>;
    async fn keep(&self, color: Color, key: &str, ttl: KeepTtl) -> WeftResult<()>;
    async fn presign(&self, color: Color, key: &str, ttl_secs: Option<u64>) -> WeftResult<String>;
}

/// Map a broker HTTP failure to a node-facing error. A transport failure
/// (broker unreachable) is a real NodeExecution error: the broker is a core
/// service, so its absence is a failure to surface, not a transient to retry
/// silently around.
fn http_err(context: &str, e: reqwest::Error) -> WeftError {
    WeftError::NodeExecution(format!("storage: {context}: {e}"))
}

async fn status_err(context: &str, resp: reqwest::Response) -> WeftError {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    match status.as_u16() {
        404 => WeftError::NodeExecution(format!("storage file not found: {body}")),
        401 | 403 => WeftError::NodeExecution(format!("storage denied: {body}")),
        413 => WeftError::NodeExecution(format!("storage quota exceeded: {body}")),
        _ => WeftError::NodeExecution(format!("storage: {context}: {status}: {body}")),
    }
}

/// The worker's broker runtime-file client.
pub struct WorkerStorage {
    broker_url: String,
    token_path: std::path::PathBuf,
    http: reqwest::Client,
}

// The request/response envelopes (the upload/* set, ListFilesResponse,
// PresignResponse, DownloadUrlResponse) are the shared worker<->broker
// contract; they live in `weft_core::storage` so this client and the broker
// share one definition.

impl WorkerStorage {
    /// `broker_url` is the in-cluster broker the worker already talks to;
    /// `token_path` is the worker's projected SA token (re-read every call so
    /// kubelet rotation propagates). `_tenant_id` is no longer needed (the
    /// broker resolves the tenant from the token), but kept off the signature.
    pub fn new(broker_url: String, token_path: std::path::PathBuf) -> Arc<Self> {
        Arc::new(Self { broker_url, token_path, http: reqwest::Client::new() })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.broker_url.trim_end_matches('/'), path)
    }

    /// The worker's bearer (its projected SA token), re-read every call.
    async fn bearer(&self) -> WeftResult<String> {
        let bytes = tokio::fs::read(&self.token_path).await.map_err(|e| {
            WeftError::NodeExecution(format!(
                "read SA token at {}: {e}",
                self.token_path.display()
            ))
        })?;
        Ok(String::from_utf8(bytes)
            .map_err(|_| WeftError::NodeExecution("SA token not utf8".into()))?
            .trim()
            .to_string())
    }

    /// A request builder with the worker's bearer + the color claim header.
    async fn authed(
        &self,
        req: reqwest::RequestBuilder,
        color: Color,
    ) -> WeftResult<reqwest::RequestBuilder> {
        Ok(req.bearer_auth(self.bearer().await?).header(HDR_COLOR, color.to_string()))
    }

    /// POST a JSON body to a broker storage endpoint and deserialize the JSON
    /// response. `what` names the op for error context.
    async fn post_json<B: serde::Serialize + ?Sized, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        color: Color,
        body: &B,
        what: &str,
    ) -> WeftResult<T> {
        let resp = self
            .authed(self.http.post(self.url(path)), color)
            .await?
            .json(body)
            .send()
            .await
            .map_err(|e| http_err(what, e))?;
        if !resp.status().is_success() {
            return Err(status_err(what, resp).await);
        }
        resp.json().await.map_err(|e| http_err(what, e))
    }

    /// POST a JSON body to a broker storage endpoint that answers with no
    /// content (part-done / abort).
    async fn post_no_content<B: serde::Serialize + ?Sized>(
        &self,
        path: &str,
        color: Color,
        body: &B,
        what: &str,
    ) -> WeftResult<()> {
        let resp = self
            .authed(self.http.post(self.url(path)), color)
            .await?
            .json(body)
            .send()
            .await
            .map_err(|e| http_err(what, e))?;
        if !resp.status().is_success() {
            return Err(status_err(what, resp).await);
        }
        Ok(())
    }

    /// Slice the stream into parts and drive the multipart flow for `key`:
    /// reserve each part (exact size), PUT it to its signed URL, report its
    /// etag, then complete. Bounded memory: at most one part is buffered.
    async fn drive_upload(
        &self,
        color: Color,
        key: &str,
        part_size: u64,
        mut data: ByteStream,
    ) -> WeftResult<Value> {
        use futures::StreamExt;
        let part_size = part_size as usize;
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = data.next().await {
            let chunk = chunk.map_err(|e| {
                WeftError::NodeExecution(format!("storage: reading upload stream: {e}"))
            })?;
            buf.extend_from_slice(&chunk);
            while buf.len() >= part_size {
                let rest = buf.split_off(part_size);
                let part = bytes::Bytes::from(std::mem::replace(&mut buf, rest));
                self.upload_one_part(color, key, part).await?;
            }
        }
        // The final short part, only if there are leftover bytes. A stream that
        // ended exactly on a part boundary flushes nothing; a stream that
        // produced NO bytes uploads ZERO parts (an empty object is not a
        // multipart part, which S3 cannot represent; `complete` writes it as a
        // plain empty object).
        if !buf.is_empty() {
            self.upload_one_part(color, key, bytes::Bytes::from(buf)).await?;
        }
        let value: Value = self
            .post_json(
                "/v1/storage/upload/complete",
                color,
                &weft_core::storage::UploadCompleteRequest { key: key.to_string() },
                "upload complete",
            )
            .await?;
        StoredFile::from_value(&value)
            .map_err(|e| WeftError::NodeExecution(format!("bad upload-complete response: {e}")))?;
        Ok(value)
    }

    /// Reserve one part (its URL comes back signed to exactly this size),
    /// PUT it, and record its etag.
    async fn upload_one_part(&self, color: Color, key: &str, bytes: bytes::Bytes) -> WeftResult<()> {
        let UploadPartsResponse { parts } = self
            .post_json(
                "/v1/storage/upload/parts",
                color,
                &weft_core::storage::UploadPartsRequest {
                    key: key.to_string(),
                    sizes: vec![bytes.len() as u64],
                },
                "reserve upload part",
            )
            .await?;
        let part = parts.into_iter().next().ok_or_else(|| {
            WeftError::NodeExecution("storage: part reservation returned no part".into())
        })?;
        self.put_part(color, key, part, bytes).await
    }

    /// PUT one reserved part to its signed URL with a bounded retry: a failed
    /// attempt (dropped connection, expired URL) gets a FRESH URL for exactly
    /// this part via `resume` and tries again. On success the etag (verbatim
    /// response header) is reported to the broker. Retries re-send the
    /// worker's own buffered bytes; nothing external is re-consumed.
    async fn put_part(
        &self,
        color: Color,
        key: &str,
        mut part: PresignedPart,
        bytes: bytes::Bytes,
    ) -> WeftResult<()> {
        const ATTEMPTS: u32 = 3;
        let mut last_err = String::new();
        for attempt in 1..=ATTEMPTS {
            if attempt > 1 {
                let UploadResumeResponse { missing, .. } = self
                    .post_json(
                        "/v1/storage/upload/resume",
                        color,
                        &weft_core::storage::UploadResumeRequest { key: key.to_string() },
                        "resume upload",
                    )
                    .await?;
                match missing.into_iter().find(|p| p.part_number == part.part_number) {
                    Some(fresh) => part = fresh,
                    // Not missing anymore: a previous attempt landed AND was
                    // recorded (the done-report raced its own error path).
                    None => return Ok(()),
                }
            }
            match self.try_put_part(&part, &bytes).await {
                Ok(etag) => {
                    self.post_no_content(
                        "/v1/storage/upload/part-done",
                        color,
                        &weft_core::storage::PartDoneRequest {
                            key: key.to_string(),
                            part_number: part.part_number,
                            etag,
                        },
                        "record uploaded part",
                    )
                    .await?;
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        target: "weft_engine::storage",
                        key = %key, part = part.part_number, attempt, error = %e,
                        "part upload attempt failed"
                    );
                    last_err = e;
                }
            }
        }
        Err(WeftError::NodeExecution(format!(
            "storage: upload interrupted: part {} failed after {ATTEMPTS} attempts: {last_err}",
            part.part_number
        )))
    }

    /// One raw PUT of a part's bytes to its presigned URL. Returns the etag
    /// response header verbatim (quotes included). String error so the caller
    /// can fold attempts into one loud message.
    async fn try_put_part(&self, part: &PresignedPart, bytes: &bytes::Bytes) -> Result<String, String> {
        let resp = self
            .http
            .put(&part.url)
            .body(bytes.clone())
            .send()
            .await
            .map_err(|e| format!("PUT part to bucket: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("bucket rejected part: {status}: {body}"));
        }
        resp.headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| "bucket part response carried no etag".to_string())
    }
}

#[async_trait]
impl WorkerStorageOps for WorkerStorage {
    async fn put(
        &self,
        color: Color,
        scope: &StorageScope,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<Value> {
        // Begin: the broker mints the key, charges a declared size against the
        // quota up front, and opens the multipart upload. The broker never
        // sees the bytes.
        // `already_stored` cannot fire here: the worker path never sends a
        // content hash (only the asset scope is content-addressed, and the
        // worker data path may not write it).
        let UploadBeginResponse { key, part_size, already_stored: _ } = self
            .post_json(
                "/v1/storage/upload/begin",
                color,
                &weft_core::storage::UploadBeginRequest {
                    scope: scope.clone(),
                    mime_type: mime_type.to_string(),
                    filename: filename.to_string(),
                    keep,
                    declared_size,
                },
                "upload begin",
            )
            .await?;
        // Drive the parts + completion; on ANY failure past begin, abort the
        // upload so its quota reservation is freed (idempotent: a quota
        // rejection already aborted broker-side), then surface the failure.
        match self.drive_upload(color, &key, part_size, data).await {
            Ok(value) => Ok(value),
            Err(e) => {
                if let Err(abort) = self
                    .post_no_content(
                        "/v1/storage/upload/abort",
                        color,
                        &weft_core::storage::UploadAbortRequest { key: key.clone() },
                        "abort upload",
                    )
                    .await
                {
                    tracing::warn!(
                        target: "weft_engine::storage",
                        key = %key, error = %abort,
                        "failed to abort interrupted upload; the broker sweep will reap it"
                    );
                }
                Err(e)
            }
        }
    }

    async fn get(
        &self,
        color: Color,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)> {
        // 1. Ask the broker for the metadata + a presigned GET URL (bumps a kept
        //    file's expiry, 404s a missing file). No bytes through the broker.
        let resp = self
            .authed(self.http.get(self.url(&format!("/v1/storage/download-url/{key}"))), color)
            .await?
            .send()
            .await
            .map_err(|e| http_err("download-url", e))?;
        if !resp.status().is_success() {
            return Err(status_err("download-url", resp).await);
        }
        let DownloadUrlResponse { meta, url } =
            resp.json().await.map_err(|e| http_err("download-url response", e))?;
        // 2. Fetch the bytes DIRECTLY from the bucket, applying the Range here so a
        //    partial read never buffers the whole object anywhere.
        let mut req = self.http.get(&url);
        if let Some(r) = range {
            // Reject an inverted range loudly (end precedes start): a silent
            // 1-byte reinterpretation would hide a caller bug. An empty range
            // (end == start) is a valid zero-length read, short-circuited to an
            // empty body without hitting the bucket (`bytes=s-s-1` is invalid).
            let header = match r.end {
                Some(e) if e < r.start => {
                    return Err(WeftError::NodeExecution(format!(
                        "invalid byte range: end {e} precedes start {}",
                        r.start
                    )));
                }
                Some(e) if e == r.start => {
                    return Ok((meta, weft_core::storage::bytes_stream(bytes::Bytes::new())));
                }
                Some(e) => format!("bytes={}-{}", r.start, e - 1),
                None => format!("bytes={}-", r.start),
            };
            req = req.header("range", header);
        }
        let bytes_resp = req.send().await.map_err(|e| http_err("download from bucket", e))?;
        if !bytes_resp.status().is_success() {
            return Err(status_err("download from bucket", bytes_resp).await);
        }
        let stream: ByteStream = Box::pin(
            bytes_resp
                .bytes_stream()
                .map_err(|e| std::io::Error::other(format!("download stream: {e}"))),
        );
        Ok((meta, stream))
    }

    async fn delete(&self, color: Color, key: &str) -> WeftResult<()> {
        let resp = self
            .authed(self.http.delete(self.url(&format!("/v1/storage/files/{key}"))), color)
            .await?
            .send()
            .await
            .map_err(|e| http_err("delete", e))?;
        if !resp.status().is_success() {
            return Err(status_err("delete", resp).await);
        }
        Ok(())
    }

    async fn list(&self, color: Color, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
        let resp = self
            .authed(self.http.get(self.url("/v1/storage/list")), color)
            .await?
            .query(&[("scope", serde_json::to_string(scope).expect("scope serializes"))])
            .send()
            .await
            .map_err(|e| http_err("list", e))?;
        if !resp.status().is_success() {
            return Err(status_err("list", resp).await);
        }
        let out: ListFilesResponse = resp.json().await.map_err(|e| http_err("list response", e))?;
        Ok(out.files)
    }

    async fn keep(&self, color: Color, key: &str, ttl: KeepTtl) -> WeftResult<()> {
        let resp = self
            .authed(self.http.post(self.url("/v1/storage/keep")), color)
            .await?
            .json(&weft_core::storage::KeepRequest { key: key.to_string(), ttl })
            .send()
            .await
            .map_err(|e| http_err("keep", e))?;
        if !resp.status().is_success() {
            return Err(status_err("keep", resp).await);
        }
        Ok(())
    }

    async fn presign(&self, color: Color, key: &str, ttl_secs: Option<u64>) -> WeftResult<String> {
        let resp = self
            .authed(self.http.post(self.url("/v1/storage/presign")), color)
            .await?
            .json(&weft_core::storage::PresignRequest { key: key.to_string(), ttl_secs })
            .send()
            .await
            .map_err(|e| http_err("presign", e))?;
        if !resp.status().is_success() {
            return Err(status_err("presign", resp).await);
        }
        let out: PresignResponse = resp.json().await.map_err(|e| http_err("presign response", e))?;
        Ok(out.url)
    }

}

// ---------- fake (tests) ----------

/// In-memory `WorkerStorageOps` for layer-3 tests: a map keyed by the
/// canonical storage key, with the same scope wall the broker enforces. Dumb:
/// state in a map, no business logic beyond the wall + keep flag.
#[cfg(any(test, feature = "test-helpers"))]
pub use fake::FakeWorkerStorage;

#[cfg(any(test, feature = "test-helpers"))]
mod fake {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    use weft_core::storage::key::{self, CallerAuth};

    pub struct FakeWorkerStorage {
        /// The seeded caller identity the wall checks against (tenant t1,
        /// project p1, color c1), mirroring the broker's verdict.
        identity: CallerAuth,
        files: Mutex<BTreeMap<String, (StoredFileMeta, bytes::Bytes)>>,
        /// Monotonic id counter: the broker mints collision-free keys, so the
        /// fake must too. Deriving the id from `files.len()` reused a freed id
        /// after a delete (delete `f2`, next put reuses `f2`), which could
        /// silently overwrite the wrong entry and mask an ordering bug.
        next_id: Mutex<u64>,
    }

    impl FakeWorkerStorage {
        /// A fake bound to (tenant t1, project p1, color c1).
        pub fn new() -> Arc<Self> {
            Arc::new(Self {
                identity: CallerAuth::Worker {
                    tenant: "t1".into(),
                    project_id: "p1".into(),
                    color: Some("c1".into()),
                },
                files: Mutex::new(BTreeMap::new()),
                next_id: Mutex::new(0),
            })
        }

        fn enforce_wall(&self, key: &str) -> WeftResult<key::ParsedKey> {
            let parsed = key::parse_key(key)
                .map_err(|e| WeftError::NodeExecution(format!("storage: {e}")))?;
            key::check_key_access(&self.identity, &parsed)
                .map_err(|e| WeftError::NodeExecution(format!("storage denied: {e}")))?;
            Ok(parsed)
        }
    }

    #[async_trait]
    impl WorkerStorageOps for FakeWorkerStorage {
        async fn put(
            &self,
            _color: Color,
            scope: &StorageScope,
            mime_type: &str,
            filename: &str,
            keep: Option<KeepTtl>,
            _declared_size: Option<u64>,
            data: ByteStream,
        ) -> WeftResult<Value> {
            let id = {
                let mut n = self.next_id.lock();
                let id = *n;
                *n += 1;
                format!("f{id}")
            };
            let key = key::key_for_put(&self.identity, scope, &id)
                .map_err(|e| WeftError::NodeExecution(format!("storage denied: {e}")))?
                .to_key();
            let bytes = weft_core::storage::collect_stream(data)
                .await
                .map_err(|e| WeftError::NodeExecution(format!("storage: {e}")))?;
            let meta = StoredFileMeta {
                key: key.clone(),
                mime_type: mime_type.to_string(),
                size_bytes: bytes.len() as u64,
                filename: filename.to_string(),
                keep: keep.is_some(),
                expires_at_unix: None,
                keep_ttl_secs: None,
                created_at_unix: 0,
            };
            let file = StoredFile {
                key: key.clone(),
                mime_type: meta.mime_type.clone(),
                size_bytes: meta.size_bytes,
                filename: meta.filename.clone(),
            };
            self.files.lock().insert(key, (meta, bytes));
            Ok(file.to_value())
        }

        async fn get(
            &self,
            _color: Color,
            key: &str,
            range: Option<ByteRange>,
        ) -> WeftResult<(StoredFileMeta, ByteStream)> {
            self.enforce_wall(key)?;
            let (meta, bytes) = self
                .files
                .lock()
                .get(key)
                .cloned()
                .ok_or_else(|| WeftError::NodeExecution(format!("storage file not found: {key}")))?;
            let bytes = match range {
                None => bytes,
                Some(r) => {
                    // Mirror the real impl EXACTLY: reject an inverted range
                    // loudly, treat an empty range as a zero-length read, and
                    // clamp the end to the object length. Without this the fake
                    // panicked on `end < start` (slice start > end) where the
                    // real path silently reinterpreted it, so an L3 test would
                    // diverge from production.
                    if let Some(e) = r.end {
                        if e < r.start {
                            return Err(WeftError::NodeExecution(format!(
                                "invalid byte range: end {e} precedes start {}",
                                r.start
                            )));
                        }
                    }
                    let end = r.end.unwrap_or(bytes.len() as u64).min(bytes.len() as u64);
                    let start = r.start.min(end);
                    bytes.slice(start as usize..end as usize)
                }
            };
            Ok((meta, weft_core::storage::bytes_stream(bytes)))
        }

        async fn delete(&self, _color: Color, key: &str) -> WeftResult<()> {
            self.enforce_wall(key)?;
            self.files
                .lock()
                .remove(key)
                .map(|_| ())
                .ok_or_else(|| WeftError::NodeExecution(format!("storage file not found: {key}")))
        }

        async fn list(&self, _color: Color, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
            let prefix = key::prefix_for_list(&self.identity, scope)
                .map_err(|e| WeftError::NodeExecution(format!("storage denied: {e}")))?;
            Ok(self
                .files
                .lock()
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, (m, _))| m.clone())
                .collect())
        }

        async fn keep(&self, _color: Color, key: &str, ttl: KeepTtl) -> WeftResult<()> {
            self.enforce_wall(key)?;
            let mut files = self.files.lock();
            let (meta, _) = files
                .get_mut(key)
                .ok_or_else(|| WeftError::NodeExecution(format!("storage file not found: {key}")))?;
            meta.keep = true;
            meta.keep_ttl_secs = match ttl {
                KeepTtl::Never => None,
                KeepTtl::Default => Some(30 * 24 * 3600),
                KeepTtl::Secs { secs } => Some(secs),
            };
            Ok(())
        }

        async fn presign(&self, _color: Color, key: &str, _ttl_secs: Option<u64>) -> WeftResult<String> {
            self.enforce_wall(key)?;
            if !self.files.lock().contains_key(key) {
                return Err(WeftError::NodeExecution(format!("storage file not found: {key}")));
            }
            Ok(format!("https://fake-bucket/runtime/{key}?sig=fake"))
        }
    }
}
