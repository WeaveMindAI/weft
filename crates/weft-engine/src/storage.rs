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
//! DIRECTLY from the bucket. Presigned URLs are signed for the worker's internal
//! endpoint. A copy is the broker's own (`copy`): the bucket copies the object,
//! so nothing passes through the worker.
//!
//! There is no "ensure the storage exists" handshake and no retry-on-
//! unreachable dance: the broker is a long-lived install service, always up
//! like the journal, so an unreachable broker is a real failure surfaced to
//! the node, not a transient to paper over. A call the broker turns away
//! busy (no database connection for it, nothing of it run) is different:
//! busy means wait, so it is sent again until it goes through, like every
//! other call to the broker (`WorkerStorage::send`).

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use serde_json::Value;

use weft_core::storage::{
    ByteRange, ByteStream, DownloadUrlResponse, KeepTtl, LinkAnswer, LinkReach, ListFilesResponse, PresignedPart,
    StorageScope, StoredFile, StoredFileMeta, UploadBeginResponse, UploadPartsResponse, UploadResumeResponse,
};
use weft_core::error::{WeftError, WeftResult};
use weft_core::weft_type::WeftType;
use weft_core::RunId;

/// The run claim header the worker stamps on every storage call.
// SYNC: HDR_RUN_ID <-> crates/weft-broker/src/runtime_storage.rs (HDR_RUN_ID)
const HDR_RUN_ID: &str = "x-weft-run-id";

/// Run-parameterized storage surface the `ContextHandle` storage methods
/// delegate to. One impl per worker process; Layer-3 tests inject a fake.
#[async_trait]
pub trait WorkerStorageOps: Send + Sync {
    /// `identity` names what the file is a copy of; a put whose
    /// identity is already stored in `scope` answers the existing file
    /// and moves no bytes (see `StorageHandle::identified`).
    async fn put(
        &self,
        run_id: RunId,
        scope: &StorageScope,
        identity: Option<&str>,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<Value>;
    /// One attempt at overwriting the stored file at `key` with `data`
    /// (same key, scope, name, type and lifetime); see
    /// `ContextHandle::storage_replace` for the outcomes.
    async fn replace(
        &self,
        run_id: RunId,
        key: &str,
        expected_version: Option<u64>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<weft_core::storage::ReplaceOutcome>;
    async fn get(
        &self,
        run_id: RunId,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)>;
    async fn delete(&self, run_id: RunId, key: &str) -> WeftResult<()>;
    async fn list(&self, run_id: RunId, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>>;
    /// The file already stored under `identity` in `scope`, if any.
    async fn find(&self, run_id: RunId, scope: &StorageScope, identity: &str) -> WeftResult<Option<StoredFileMeta>>;
    async fn keep(&self, run_id: RunId, key: &str, ttl: KeepTtl) -> WeftResult<()>;
    /// A link to `key` for `reach`, living `ttl_secs` (`None`: the
    /// default), with the file's record.
    async fn link(&self, run_id: RunId, key: &str, ttl_secs: Option<u64>, reach: LinkReach) -> WeftResult<LinkAnswer>;
    /// A copy of `key` into `scope`, made inside the store.
    async fn copy(
        &self,
        run_id: RunId,
        key: &str,
        scope: &StorageScope,
        identity: Option<&str>,
        keep: Option<KeepTtl>,
    ) -> WeftResult<Value>;
    /// A slot a page uploads one file through.
    async fn upload_slot(&self, run_id: RunId, request: &weft_core::storage::slot::UploadSlotRequest) -> WeftResult<String>;
    /// The files uploaded through each claim's slot, received by `run_id`
    /// each for a place of its type, in the claims' order: all of them or
    /// none.
    async fn claim_uploads(&self, run_id: RunId, claims: &[(&str, &WeftType)]) -> WeftResult<Vec<Value>>;
    /// The bytes of the tenant's content `key` (`<tenant>/asset/<sha256>`),
    /// read with no run named: what a program names
    /// (`weft_core::content`), read when the worker reads the program,
    /// before any run of it. `None` when the store answers that it holds
    /// no such content (`is_gone`); an error when it could not be asked
    /// or answered anything else, which a later read may get past.
    async fn content(&self, key: &str) -> WeftResult<Option<Vec<u8>>>;
}

/// Map a failure to reach the broker (or the bucket) to a node-facing
/// error. A transport failure is a real failure of the node: the broker is
/// a core service, so its absence is surfaced, never retried silently.
fn http_err(context: &str, e: reqwest::Error) -> WeftError {
    WeftError::NodeFailed(format!("storage: {context}: {e}"))
}

/// Whether a storage answer says the store holds no such file, for good:
/// never there (404), or lost (410). Any other refusal may pass.
fn is_gone(status: reqwest::StatusCode) -> bool {
    matches!(status, reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::GONE)
}

/// A refusal of a storage call, as the node sees it.
fn refusal(context: &str, status: reqwest::StatusCode, body: String) -> WeftError {
    if status == weft_broker_client::protocol::UNREADABLE_SHAPE {
        return WeftError::NodeFailed(weft_broker_client::WorkerOutdated { path: context.to_string(), said: body }.to_string());
    }
    match status.as_u16() {
        // The broker says in its own words what is missing (a file and
        // what deleted it, or an upload that is not in flight).
        404 if !body.is_empty() => WeftError::NodeFailed(format!("storage: {body}")),
        404 => WeftError::NodeFailed(format!("storage: {context}: not found")),
        401 | 403 => WeftError::NodeFailed(format!("storage denied: {body}")),
        413 => WeftError::NodeFailed(format!("storage quota exceeded: {body}")),
        _ => WeftError::NodeFailed(format!("storage: {context}: {status}: {body}")),
    }
}

/// The broker's answer to one storage call, read whole. Every answer is a
/// small JSON body or none: a file's bytes go to and from the bucket,
/// never through the broker.
struct Reply {
    status: reqwest::StatusCode,
    /// `upload/complete`'s "still landing" mark
    /// ([`weft_core::storage::COMPLETING_HEADER`]).
    completing: bool,
    body: bytes::Bytes,
}

impl Reply {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    fn refusal(&self, context: &str) -> WeftError {
        refusal(context, self.status, self.text())
    }

    /// The answer's JSON, or the refusal it is.
    fn answer<T: serde::de::DeserializeOwned>(&self, context: &str) -> WeftResult<T> {
        if !self.status.is_success() {
            return Err(self.refusal(context));
        }
        serde_json::from_slice(&self.body)
            .map_err(|e| WeftError::NodeFailed(format!("storage: {context}: the broker's answer does not read: {e}")))
    }

    /// Nothing, or the refusal it is.
    fn done(&self, context: &str) -> WeftResult<()> {
        if !self.status.is_success() {
            return Err(self.refusal(context));
        }
        Ok(())
    }
}

/// Whether the broker turned a storage call away busy with nothing of it
/// run, so it is sent again, like every other call to the broker
/// (`weft_broker_client::client::while_busy`). Any other 503 may have half
/// happened and fails the node.
// SYNC: NOT_DONE <-> crates/weft-broker-client/src/client.rs turned_away
fn turned_away(sent: &WeftResult<Reply>) -> bool {
    matches!(sent, Ok(reply) if weft_broker_client::client::answered_busy(reply.status, &reply.body))
}

/// The statuses a claim answers when the slot is no good for this call
/// (malformed, unknown, already received by another run, ended, too big,
/// or not of the type asked). Every other 4xx (the worker's own
/// credentials refused, a timeout, a rate limit) is not the caller's to
/// fix.
fn is_slot_refusal(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 400 | 404 | 409 | 410 | 413 | 422)
}

/// How many times a completion is asked before giving up: with the 1s
/// doubling backoff capped at 30s, about four minutes, near the broker's
/// lease after which its sweep drives the completion itself.
const COMPLETE_ATTEMPTS: u32 = 12;

/// What one answer to `upload/complete` means.
#[derive(Debug, PartialEq, Eq)]
enum CompleteStep {
    /// The file: done.
    Landed,
    /// Ask again: still landing (409 with `x-weft-completing`), or the
    /// broker failed without saying (5xx).
    Retry,
    /// The broker gave its final verdict and ended the upload (410).
    Ended,
    /// Refused before any completion was claimed (an incomplete upload,
    /// any other 4xx): the upload is aborted.
    Refused,
}

// SYNC: the marker <-> crates/weft-broker/src/runtime_storage.rs complete_response
// SYNC: completion backoff <-> crates/weft-cli/src/commands/assets.rs completing_wait
fn complete_step(status: reqwest::StatusCode, completing: bool) -> CompleteStep {
    if status.is_success() {
        CompleteStep::Landed
    } else if (status == reqwest::StatusCode::CONFLICT && completing) || status.is_server_error() {
        CompleteStep::Retry
    } else if status == reqwest::StatusCode::GONE {
        CompleteStep::Ended
    } else {
        CompleteStep::Refused
    }
}

/// The worker's broker runtime-file client.
pub struct WorkerStorage {
    broker_url: String,
    token: weft_broker_client::TokenSource,
    http: reqwest::Client,
}

// The request/response envelopes (the upload/* set, ListFilesResponse,
// PresignResponse, DownloadUrlResponse) are the shared worker<->broker
// contract; they live in `weft_core::storage` so this client and the broker
// share one definition.

impl WorkerStorage {
    /// `broker_url` is the broker the worker already talks to; `token` is
    /// the worker's identity for it (the broker resolves the tenant and
    /// project from it).
    pub fn new(broker_url: String, token: weft_broker_client::TokenSource) -> Arc<Self> {
        Arc::new(Self { broker_url, token, http: weft_core::net::http_client().build().expect("reqwest client") })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.broker_url.trim_end_matches('/'), path)
    }

    /// A request builder with the worker's identity, and the run claim
    /// header when the call acts for a run.
    async fn authed(
        &self,
        req: reqwest::RequestBuilder,
        run_id: Option<RunId>,
    ) -> WeftResult<reqwest::RequestBuilder> {
        let bearer = self
            .token
            .read(self.broker_url.trim_end_matches('/'))
            .await
            .map_err(|e| WeftError::NodeFailed(format!("get an identity token for the broker: {e:#}")))?;
        let mut req = req.bearer_auth(bearer);
        if let Some(run_id) = run_id {
            req = req.header(HDR_RUN_ID, run_id.to_string());
        }
        for (name, value) in self.token.headers() {
            req = req.header(name, value);
        }
        Ok(req)
    }

    /// Send one storage call to the broker (`request` builds it from the
    /// client and the endpoint's URL), and again for as long as the broker
    /// turns it away busy with nothing of it run ([`turned_away`]): no
    /// deadline, since a node's storage call is the user's work, and the
    /// waits are said in the log with every other broker call's. Every
    /// call's body is JSON or nothing, so it is built again as it was.
    async fn send(
        &self,
        run_id: Option<RunId>,
        path: &str,
        what: &str,
        request: impl Fn(&reqwest::Client, String) -> reqwest::RequestBuilder + Sync,
    ) -> WeftResult<Reply> {
        weft_broker_client::client::while_busy(path, turned_away, || async {
            let resp = self
                .authed(request(&self.http, self.url(path)), run_id)
                .await?
                .send()
                .await
                .map_err(|e| http_err(what, e))?;
            let status = resp.status();
            let completing = resp.headers().contains_key(weft_core::storage::COMPLETING_HEADER);
            let body = resp.bytes().await.map_err(|e| {
                WeftError::NodeFailed(format!("storage: {what}: {status}, and its body could not be read: {e}"))
            })?;
            Ok(Reply { status, completing, body })
        })
        .await
    }

    /// GET a broker storage endpoint and deserialize the JSON response.
    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str, run_id: RunId, what: &str) -> WeftResult<T> {
        self.send(Some(run_id), path, what, |http, url| http.get(url)).await?.answer(what)
    }

    /// POST a JSON body to a broker storage endpoint and deserialize the JSON
    /// response. `what` names the op for error context.
    async fn post_json<B: serde::Serialize + Sync + ?Sized, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        run_id: RunId,
        body: &B,
        what: &str,
    ) -> WeftResult<T> {
        self.send(Some(run_id), path, what, |http, url| http.post(url).json(body)).await?.answer(what)
    }

    /// POST a JSON body to a broker storage endpoint that answers with no
    /// content (part-done / abort / keep).
    async fn post_no_content<B: serde::Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        run_id: RunId,
        body: &B,
        what: &str,
    ) -> WeftResult<()> {
        self.send(Some(run_id), path, what, |http, url| http.post(url).json(body)).await?.done(what)
    }

    /// The bytes behind a presigned GET `url`, read DIRECTLY from the
    /// bucket, the Range applied there so a partial read never buffers the
    /// whole object anywhere.
    async fn bucket_read(&self, url: &str, range: Option<ByteRange>) -> WeftResult<ByteStream> {
        self.bucket_get(url, range).await?.map_err(|(status, body)| refusal("download from bucket", status, body))
    }

    /// [`Self::bucket_read`], a refusal by the bucket answered as its
    /// status and body (`Ok(Err(..))`) for the caller to read.
    async fn bucket_get(&self, url: &str, range: Option<ByteRange>) -> WeftResult<Result<ByteStream, (reqwest::StatusCode, String)>> {
        let mut req = self.http.get(url);
        if let Some(r) = range {
            // Reject an inverted range loudly (end precedes start): a silent
            // 1-byte reinterpretation would hide a caller bug. An empty range
            // (end == start) is a valid zero-length read, short-circuited to an
            // empty body without hitting the bucket (`bytes=s-s-1` is invalid).
            let header = match r.end {
                Some(e) if e < r.start => {
                    return Err(WeftError::NodeFailed(format!(
                        "invalid byte range: end {e} precedes start {}",
                        r.start
                    )));
                }
                Some(e) if e == r.start => {
                    return Ok(Ok(weft_core::storage::bytes_stream(bytes::Bytes::new())));
                }
                Some(e) => format!("bytes={}-{}", r.start, e - 1),
                None => format!("bytes={}-", r.start),
            };
            req = req.header("range", header);
        }
        let bytes_resp = req.send().await.map_err(|e| http_err("download from bucket", e))?;
        let status = bytes_resp.status();
        if !status.is_success() {
            let body = bytes_resp.text().await.map_err(|e| {
                WeftError::NodeFailed(format!("storage: download from bucket: {status}, and its body could not be read: {e}"))
            })?;
            return Ok(Err((status, body)));
        }
        let stream: ByteStream = Box::pin(
            bytes_resp
                .bytes_stream()
                .map_err(|e| std::io::Error::other(format!("download stream: {e}"))),
        );
        Ok(Ok(stream))
    }

    /// Slice the stream into parts and upload them for `key`: reserve each
    /// part (exact size), PUT it to its signed URL, report its etag.
    /// Bounded memory: at most one part is buffered.
    async fn upload_parts(
        &self,
        run_id: RunId,
        key: &str,
        part_size: u64,
        mut data: ByteStream,
    ) -> WeftResult<()> {
        use futures::StreamExt;
        let part_size = part_size as usize;
        let mut buf: Vec<u8> = Vec::new();
        // Parts are NAMED by their position, from 1, and this loop is the
        // only writer of this key's stream, so the part it is on is the part
        // it says it is on.
        let mut part_number: i32 = 1;
        while let Some(chunk) = data.next().await {
            let chunk = chunk.map_err(|e| {
                WeftError::NodeFailed(format!("storage: reading upload stream: {e}"))
            })?;
            buf.extend_from_slice(&chunk);
            while buf.len() >= part_size {
                let rest = buf.split_off(part_size);
                let part = bytes::Bytes::from(std::mem::replace(&mut buf, rest));
                self.upload_one_part(run_id, key, part_number, part).await?;
                part_number += 1;
            }
        }
        // The final short part, only if there are leftover bytes. A stream that
        // ended exactly on a part boundary flushes nothing; a stream that
        // produced NO bytes uploads ZERO parts (an empty object is not a
        // multipart part, which S3 cannot represent; `complete` writes it as a
        // plain empty object).
        if !buf.is_empty() {
            self.upload_one_part(run_id, key, part_number, bytes::Bytes::from(buf)).await?;
        }
        Ok(())
    }

    /// Upload the parts of the upload begun under `key`, then complete it.
    ///
    /// A failure while uploading parts aborts the upload, so its quota
    /// reservation is freed (idempotent: a quota rejection already aborted
    /// broker-side). Completion is different: once the broker claims it,
    /// the bytes may be landing and an abort is refused, so completion is
    /// retried until it answers for good ([`Self::complete_until_landed`]).
    async fn drive_or_abort(
        &self,
        run_id: RunId,
        key: &str,
        part_size: u64,
        data: ByteStream,
    ) -> WeftResult<StoredFile> {
        if let Err(e) = self.upload_parts(run_id, key, part_size, data).await {
            self.abort_upload(run_id, key).await;
            return Err(e);
        }
        let value = self.complete_until_landed(run_id, key).await?;
        StoredFile::from_value(&value).map_err(|e| {
            WeftError::NodeFailed(format!("storage: upload '{key}' completed but its answer is not a file: {e}"))
        })
    }

    async fn abort_upload(&self, run_id: RunId, key: &str) {
        if let Err(abort) = self
            .post_no_content(
                "/v1/storage/upload/abort",
                run_id,
                &weft_core::storage::UploadAbortRequest { key: key.to_string() },
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
    }

    /// Ask the broker to complete `key` until it answers for good. Complete
    /// is idempotent, so a lost answer, a broker error, or the
    /// `x-weft-completing` "still landing" answer is asked again with
    /// backoff. A refusal before the broker claimed the completion (an
    /// incomplete upload) aborts it; a final verdict (410, the broker ended
    /// the upload) is reported as it is. An internal wait, so it is
    /// bounded: past it, the failure names the key and says the broker's
    /// sweep finishes or ends it.
    async fn complete_until_landed(&self, run_id: RunId, key: &str) -> WeftResult<Value> {
        let request = weft_core::storage::UploadCompleteRequest { key: key.to_string() };
        let mut delay = std::time::Duration::from_secs(1);
        let mut last = String::new();
        for _ in 0..COMPLETE_ATTEMPTS {
            // A busy broker is waited out inside `send` and spends no
            // attempt: nothing of the call ran.
            let sent = self
                .send(Some(run_id), "/v1/storage/upload/complete", "upload complete", |http, url| http.post(url).json(&request))
                .await;
            match sent {
                Err(e) => last = e.to_string(),
                Ok(reply) => match complete_step(reply.status, reply.completing) {
                    CompleteStep::Landed => return reply.answer("upload complete"),
                    CompleteStep::Retry => last = format!("{}: {}", reply.status, reply.text()),
                    CompleteStep::Ended => {
                        let e = reply.refusal("upload complete");
                        return Err(WeftError::NodeFailed(format!("storage: upload '{key}' was ended by the broker: {e}")));
                    }
                    CompleteStep::Refused => {
                        let e = reply.refusal("upload complete");
                        self.abort_upload(run_id, key).await;
                        return Err(e);
                    }
                },
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(std::time::Duration::from_secs(30));
        }
        Err(WeftError::NodeFailed(format!(
            "storage: upload '{key}' did not finish completing ({last}); the broker's sweep \
             finishes it or ends it, so look for the file again later"
        )))
    }

    /// Reserve one part (its URL comes back signed to exactly this size),
    /// PUT it, and record its etag.
    async fn upload_one_part(
        &self,
        run_id: RunId,
        key: &str,
        part_number: i32,
        bytes: bytes::Bytes,
    ) -> WeftResult<()> {
        let UploadPartsResponse { parts } = self
            .post_json(
                "/v1/storage/upload/parts",
                run_id,
                &weft_core::storage::UploadPartsRequest {
                    key: key.to_string(),
                    parts: vec![weft_core::storage::PartAsk {
                        part_number,
                        size_bytes: bytes.len() as u64,
                    }],
                    sender: weft_core::storage::PartSender::Caller,
                },
                "reserve upload part",
            )
            .await?;
        let part = parts.into_iter().next().ok_or_else(|| {
            WeftError::NodeFailed("storage: part reservation returned no part".into())
        })?;
        self.put_part(run_id, key, part, bytes).await
    }

    /// PUT one reserved part to its signed URL with a bounded retry: a failed
    /// attempt (dropped connection, expired URL) gets a FRESH URL for exactly
    /// this part via `resume` and tries again. On success the etag (verbatim
    /// response header) is reported to the broker. Retries re-send the
    /// worker's own buffered bytes; nothing external is re-consumed.
    async fn put_part(
        &self,
        run_id: RunId,
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
                        run_id,
                        &weft_core::storage::UploadResumeRequest {
                            key: key.to_string(),
                            sender: weft_core::storage::PartSender::Caller,
                        },
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
                        run_id,
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
        Err(WeftError::NodeFailed(format!(
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
            let body = resp.text().await.map_err(|e| format!("bucket rejected part: {status}, and its body could not be read: {e}"))?;
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
        run_id: RunId,
        scope: &StorageScope,
        identity: Option<&str>,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<Value> {
        // Begin: the broker mints the key, charges a declared size against the
        // quota up front, and opens the multipart upload. The broker never
        // sees the bytes.
        // `resume` is not read here: it can only come back for a
        // CONTENT-ADDRESSED key (an asset, where two callers can hold the
        // same bytes), and a node's upload is run-scoped under a
        // freshly minted id, so nothing can already be part way up under it.
        let UploadBeginResponse { key, part_size, already_stored, resume: _ } = self
            .post_json(
                "/v1/storage/upload/begin",
                run_id,
                &weft_core::storage::UploadBeginRequest {
                    scope: scope.clone(),
                    mime_type: mime_type.to_string(),
                    filename: filename.to_string(),
                    keep,
                    declared_size,
                    identity: identity.map(str::to_string),
                },
                "upload begin",
            )
            .await?;
        // An identified put whose identity the scope already holds:
        // the broker answered the file it has, nothing to upload. The
        // stream is dropped unread (a URL fetch never pulls its body).
        if already_stored {
            let meta: StoredFileMeta = self.get_json(&format!("/v1/storage/meta/{key}"), run_id, "meta").await?;
            return Ok(StoredFile::from(&meta).to_value());
        }
        Ok(self.drive_or_abort(run_id, &key, part_size, data).await?.to_value())
    }

    async fn replace(
        &self,
        run_id: RunId,
        key: &str,
        expected_version: Option<u64>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<weft_core::storage::ReplaceOutcome> {
        // The replacement is an ordinary upload under a key of its own,
        // written onto the replaced file's object; complete answers the
        // replaced file's value. The begin is where the broker refuses a
        // file another write holds (409) or that moved on from the
        // version this content was made from (412), before any byte
        // moves.
        let request = weft_core::storage::UploadReplaceRequest { key: key.to_string(), declared_size, expected_version };
        let reply = self
            .send(Some(run_id), "/v1/storage/upload/replace", "replace begin", |http, url| http.post(url).json(&request))
            .await?;
        // SYNC: replace outcome statuses <-> crates/weft-broker/src/runtime_storage.rs map_err
        match reply.status {
            reqwest::StatusCode::CONFLICT => return Ok(weft_core::storage::ReplaceOutcome::Busy),
            reqwest::StatusCode::PRECONDITION_FAILED => return Ok(weft_core::storage::ReplaceOutcome::Stale),
            _ => {}
        }
        let UploadBeginResponse { key: upload_key, part_size, .. } = reply.answer("replace begin")?;
        let file = self.drive_or_abort(run_id, &upload_key, part_size, data).await?;
        Ok(weft_core::storage::ReplaceOutcome::Replaced(file))
    }

    async fn get(
        &self,
        run_id: RunId,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)> {
        // 1. Ask the broker for the metadata + a presigned GET URL (bumps a kept
        //    file's expiry, 404s a missing file). No bytes through the broker.
        let DownloadUrlResponse { meta, url } =
            self.get_json(&format!("/v1/storage/download-url/{key}"), run_id, "download-url").await?;
        // 2. Fetch the bytes DIRECTLY from the bucket, applying the Range here so a
        //    partial read never buffers the whole object anywhere.
        Ok((meta, self.bucket_read(&url, range).await?))
    }

    async fn content(&self, key: &str) -> WeftResult<Option<Vec<u8>>> {
        let what = "read a program's content";
        // The broker answers from the file's row, so its 404 is the store
        // saying the content is not there; the bucket's, that its bytes are
        // not.
        let reply = self.send(None, &format!("/v1/storage/download-url/{key}"), what, |http, url| http.get(url)).await?;
        if is_gone(reply.status) {
            return Ok(None);
        }
        let DownloadUrlResponse { url, .. } = reply.answer(what)?;
        let stream = match self.bucket_get(&url, None).await? {
            Ok(stream) => stream,
            Err((status, _)) if is_gone(status) => return Ok(None),
            Err((status, body)) => return Err(refusal("download from bucket", status, body)),
        };
        let bytes = weft_core::storage::collect_stream(stream)
            .await
            .map_err(|e| WeftError::NodeFailed(format!("storage: {what}: {e}")))?;
        Ok(Some(bytes.to_vec()))
    }

    async fn delete(&self, run_id: RunId, key: &str) -> WeftResult<()> {
        self.send(Some(run_id), &format!("/v1/storage/files/{key}"), "delete", |http, url| http.delete(url)).await?.done("delete")
    }

    async fn find(&self, run_id: RunId, scope: &StorageScope, identity: &str) -> WeftResult<Option<StoredFileMeta>> {
        let out: weft_core::storage::IdentityLookupResponse = self
            .post_json(
                "/v1/storage/identity",
                run_id,
                &weft_core::storage::IdentityLookupRequest {
                    scope: scope.clone(),
                    identity: identity.to_string(),
                },
                "identity lookup",
            )
            .await?;
        Ok(out.file)
    }

    async fn list(&self, run_id: RunId, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
        let scope = [("scope", serde_json::to_string(scope).expect("scope serializes"))];
        let out: ListFilesResponse =
            self.send(Some(run_id), "/v1/storage/list", "list", |http, url| http.get(url).query(&scope)).await?.answer("list")?;
        Ok(out.files)
    }

    async fn keep(&self, run_id: RunId, key: &str, ttl: KeepTtl) -> WeftResult<()> {
        self.post_no_content("/v1/storage/keep", run_id, &weft_core::storage::KeepRequest { key: key.to_string(), ttl }, "keep")
            .await
    }

    async fn link(&self, run_id: RunId, key: &str, ttl_secs: Option<u64>, reach: LinkReach) -> WeftResult<LinkAnswer> {
        self.post_json(
            "/v1/storage/link",
            run_id,
            &weft_core::storage::LinkRequest { key: key.to_string(), ttl_secs, reach },
            "link",
        )
        .await
    }

    async fn copy(
        &self,
        run_id: RunId,
        key: &str,
        scope: &StorageScope,
        identity: Option<&str>,
        keep: Option<KeepTtl>,
    ) -> WeftResult<Value> {
        self.post_json(
            "/v1/storage/copy",
            run_id,
            &weft_core::storage::CopyRequest {
                key: key.to_string(),
                scope: scope.clone(),
                identity: identity.map(str::to_string),
                keep,
            },
            "copy",
        )
        .await
    }

    async fn upload_slot(&self, run_id: RunId, request: &weft_core::storage::slot::UploadSlotRequest) -> WeftResult<String> {
        let out: weft_core::storage::slot::UploadSlotResponse =
            self.post_json("/v1/storage/upload-slot", run_id, request, "upload slot").await?;
        Ok(out.slot)
    }

    /// A refusal of any slot (no good, ended, another project's, already
    /// received, too big, not of the type its place takes, or nothing
    /// finished uploading through it) receives none of them, is the
    /// caller's to fix and comes back as an input error in the store's
    /// words. Anything else, the broker refusing the worker's own
    /// credentials included, is the node's failure.
    async fn claim_uploads(&self, run_id: RunId, claims: &[(&str, &WeftType)]) -> WeftResult<Vec<Value>> {
        use weft_core::storage::slot::{ClaimUploadRequest, ClaimUploadResponse, SlotClaim};
        let request = ClaimUploadRequest {
            claims: claims.iter().map(|(slot, fits)| SlotClaim { slot: slot.to_string(), fits: fits.wire_string() }).collect(),
        };
        let reply = self
            .send(Some(run_id), "/v1/storage/upload/claim", "receive uploads", |http, url| http.post(url).json(&request))
            .await?;
        if is_slot_refusal(reply.status) {
            return Err(WeftError::Input(reply.text()));
        }
        let answer: ClaimUploadResponse = reply.answer("receive uploads")?;
        if answer.files.len() != claims.len() {
            return Err(WeftError::NodeFailed(format!(
                "storage: receive uploads: asked for {} files, the broker answered {}",
                claims.len(),
                answer.files.len()
            )));
        }
        Ok(answer.files)
    }
}

// ---------- fake (tests) ----------

/// In-memory `WorkerStorageOps` for layer-3 tests: a map keyed by the
/// canonical storage key, behind the same tenant wall the broker enforces
/// (`check_key_access`). Dumb: state in a map, no business logic beyond the
/// wall and a file's deadline.
#[cfg(any(test, feature = "test-helpers"))]
pub use fake::FakeWorkerStorage;

#[cfg(any(test, feature = "test-helpers"))]
mod fake {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    use weft_core::storage::key::{self, CallerAuth};
    use weft_core::storage::FileLink;

    pub struct FakeWorkerStorage {
        /// The seeded caller identity the wall checks against (tenant t1,
        /// project p1, run c1), mirroring the broker's verdict.
        identity: CallerAuth,
        files: Mutex<BTreeMap<String, (StoredFileMeta, bytes::Bytes)>>,
        /// `(scope, identity)` of every identified put, to the key it
        /// minted.
        identities: Mutex<BTreeMap<(String, String), String>>,
        /// The contents programs name, by key ([`Self::hold_content`]), and
        /// every key read, in order. While `contents_down`, every read of
        /// one fails as a store that cannot be reached does
        /// ([`Self::contents_down`]).
        contents: Mutex<BTreeMap<String, bytes::Bytes>>,
        content_reads: Mutex<Vec<String>>,
        contents_down: Mutex<bool>,
        /// Monotonic id counter: the broker mints collision-free keys, so the
        /// fake must too. Deriving the id from `files.len()` reused a freed id
        /// after a delete (delete `f2`, next put reuses `f2`), which could
        /// silently overwrite the wrong entry and mask an ordering bug.
        next_id: Mutex<u64>,
    }

    impl FakeWorkerStorage {
        /// A fake bound to (tenant t1, project p1, run c1).
        pub fn new() -> Arc<Self> {
            Arc::new(Self {
                identity: CallerAuth::Worker {
                    tenant: "t1".into(),
                    project_id: "p1".into(),
                    run_id: Some("c1".into()),
                    instance: None,
                },
                files: Mutex::new(BTreeMap::new()),
                identities: Mutex::new(BTreeMap::new()),
                contents: Mutex::new(BTreeMap::new()),
                content_reads: Mutex::new(Vec::new()),
                contents_down: Mutex::new(false),
                next_id: Mutex::new(0),
            })
        }

        /// Store `bytes` as the tenant's content (as a version or a build
        /// stores it), answering its key.
        pub fn hold_content(&self, bytes: &[u8]) -> String {
            let hash = weft_core::project::hash::sha256_hex(bytes);
            let key = format!("t1/asset/{hash}");
            self.contents.lock().insert(key.clone(), bytes::Bytes::copy_from_slice(bytes));
            key
        }

        /// Make every content read fail (`down`) as a store that cannot be
        /// reached does, or answer again.
        pub fn contents_down(&self, down: bool) {
            *self.contents_down.lock() = down;
        }

        /// Every content key read so far, in order.
        pub fn content_reads(&self) -> Vec<String> {
            self.content_reads.lock().clone()
        }

        fn enforce_wall(&self, key: &str) -> WeftResult<key::ParsedKey> {
            let parsed = key::parse_key(key)
                .map_err(|e| WeftError::NodeFailed(format!("storage: {e}")))?;
            key::check_key_access(&self.identity, &parsed)
                .map_err(|e| WeftError::NodeFailed(format!("storage denied: {e}")))?;
            Ok(parsed)
        }
    }

    #[async_trait]
    impl WorkerStorageOps for FakeWorkerStorage {
        async fn put(
            &self,
            _run_id: RunId,
            scope: &StorageScope,
            identity: Option<&str>,
            mime_type: &str,
            filename: &str,
            keep: Option<KeepTtl>,
            _declared_size: Option<u64>,
            data: ByteStream,
        ) -> WeftResult<Value> {
            // An identified put the scope already holds answers the
            // stored file, as the broker does; nothing is written.
            let identity_key = identity.map(|i| (format!("{scope:?}"), i.to_string()));
            if let Some(identity_key) = &identity_key {
                if let Some(existing) = self.identities.lock().get(identity_key) {
                    let files = self.files.lock();
                    let (meta, _) = files.get(existing).expect("an identified key is stored");
                    return Ok(StoredFile::from(meta).to_value());
                }
            }
            let id = {
                let mut n = self.next_id.lock();
                let id = *n;
                *n += 1;
                format!("f{id}")
            };
            let key = key::key_for_put(&self.identity, scope, &id)
                .map_err(|e| WeftError::NodeFailed(format!("storage denied: {e}")))?
                .to_key();
            let bytes = weft_core::storage::collect_stream(data)
                .await
                .map_err(|e| WeftError::NodeFailed(format!("storage: {e}")))?;
            let meta = StoredFileMeta {
                key: key.clone(),
                mime_type: mime_type.to_string(),
                size_bytes: bytes.len() as u64,
                filename: filename.to_string(),
                expires_at_unix: None,
                keep_ttl_secs: keep.and_then(KeepTtl::secs),
                created_at_unix: 0,
                version: weft_core::storage::FIRST_FILE_VERSION,
            };
            let file = StoredFile::from(&meta);
            if let Some(identity_key) = identity_key {
                self.identities.lock().insert(identity_key, key.clone());
            }
            self.files.lock().insert(key, (meta, bytes));
            Ok(file.to_value())
        }

        async fn replace(
            &self,
            _run_id: RunId,
            key: &str,
            expected_version: Option<u64>,
            _declared_size: Option<u64>,
            data: ByteStream,
        ) -> WeftResult<weft_core::storage::ReplaceOutcome> {
            self.enforce_wall(key)?;
            let bytes = weft_core::storage::collect_stream(data)
                .await
                .map_err(|e| WeftError::NodeFailed(format!("storage: {e}")))?;
            let mut files = self.files.lock();
            let (meta, stored) = files
                .get_mut(key)
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            if expected_version.is_some_and(|v| v != meta.version) {
                return Ok(weft_core::storage::ReplaceOutcome::Stale);
            }
            meta.size_bytes = bytes.len() as u64;
            meta.version += 1;
            *stored = bytes;
            Ok(weft_core::storage::ReplaceOutcome::Replaced(StoredFile::from(&*meta)))
        }

        async fn find(&self, _run_id: RunId, scope: &StorageScope, identity: &str) -> WeftResult<Option<StoredFileMeta>> {
            let key = self.identities.lock().get(&(format!("{scope:?}"), identity.to_string())).cloned();
            Ok(key.and_then(|k| self.files.lock().get(&k).map(|(meta, _)| meta.clone())))
        }

        async fn get(
            &self,
            _run_id: RunId,
            key: &str,
            range: Option<ByteRange>,
        ) -> WeftResult<(StoredFileMeta, ByteStream)> {
            self.enforce_wall(key)?;
            let (meta, bytes) = self
                .files
                .lock()
                .get(key)
                .cloned()
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
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
                            return Err(WeftError::NodeFailed(format!(
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

        async fn delete(&self, _run_id: RunId, key: &str) -> WeftResult<()> {
            self.enforce_wall(key)?;
            self.files
                .lock()
                .remove(key)
                .map(|_| ())
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))
        }

        async fn list(&self, _run_id: RunId, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
            let prefix = key::prefix_for_list(&self.identity, scope)
                .map_err(|e| WeftError::NodeFailed(format!("storage denied: {e}")))?;
            Ok(self
                .files
                .lock()
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, (m, _))| m.clone())
                .collect())
        }

        async fn keep(&self, _run_id: RunId, key: &str, ttl: KeepTtl) -> WeftResult<()> {
            self.enforce_wall(key)?;
            let mut files = self.files.lock();
            let (meta, _) = files
                .get_mut(key)
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            meta.keep_ttl_secs = ttl.secs();
            Ok(())
        }

        async fn link(&self, _run_id: RunId, key: &str, ttl_secs: Option<u64>, reach: LinkReach) -> WeftResult<LinkAnswer> {
            self.enforce_wall(key)?;
            let file = self
                .files
                .lock()
                .get(key)
                .map(|(meta, _)| meta.clone())
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            let ttl = ttl_secs.unwrap_or(weft_core::storage::DEFAULT_LINK_SECS);
            // The fake install is reached by nobody outside: its outside
            // link is the install's own address, which the internet does
            // not open.
            let link = match reach {
                LinkReach::Internal => FileLink { url: format!("http://fake-bucket/runtime/{key}?ttl={ttl}"), public: false },
                LinkReach::Container => FileLink { url: format!("http://fake-containers/files/runtime/{key}?ttl={ttl}"), public: false },
                LinkReach::External => FileLink { url: format!("http://fake-install/files/runtime/{key}?ttl={ttl}"), public: false },
            };
            Ok(LinkAnswer { link, file })
        }

        async fn copy(
            &self,
            run_id: RunId,
            key: &str,
            scope: &StorageScope,
            identity: Option<&str>,
            keep: Option<KeepTtl>,
        ) -> WeftResult<Value> {
            self.enforce_wall(key)?;
            let (meta, bytes) = self
                .files
                .lock()
                .get(key)
                .cloned()
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            self.put(run_id, scope, identity, &meta.mime_type, &meta.filename, keep, Some(meta.size_bytes), weft_core::storage::bytes_stream(bytes))
                .await
        }

        async fn upload_slot(&self, _run_id: RunId, request: &weft_core::storage::slot::UploadSlotRequest) -> WeftResult<String> {
            request.validate().map_err(WeftError::Input)?;
            Ok(format!("{}fake-slot", weft_core::storage::slot::SLOT_PREFIX))
        }

        async fn claim_uploads(&self, _run_id: RunId, claims: &[(&str, &WeftType)]) -> WeftResult<Vec<Value>> {
            let slots: Vec<&str> = claims.iter().map(|(slot, _)| *slot).collect();
            Err(WeftError::Input(format!("nothing was uploaded through '{}' (the fake storage takes no page's upload)", slots.join("', '"))))
        }

        async fn content(&self, key: &str) -> WeftResult<Option<Vec<u8>>> {
            self.enforce_wall(key)?;
            self.content_reads.lock().push(key.to_string());
            if *self.contents_down.lock() {
                return Err(WeftError::NodeFailed(format!("storage: read a program's content: the broker could not be reached for '{key}'")));
            }
            Ok(self.contents.lock().get(key).map(|bytes| bytes.to_vec()))
        }
    }
}

#[cfg(test)]
mod complete_tests {
    use super::{complete_step, CompleteStep};
    use reqwest::StatusCode;

    #[test]
    fn only_the_marked_conflict_and_server_errors_are_asked_again() {
        assert_eq!(complete_step(StatusCode::OK, false), CompleteStep::Landed);
        assert_eq!(complete_step(StatusCode::CONFLICT, true), CompleteStep::Retry);
        assert_eq!(complete_step(StatusCode::INTERNAL_SERVER_ERROR, false), CompleteStep::Retry);
        assert_eq!(complete_step(StatusCode::GONE, false), CompleteStep::Ended);
        // An unmarked conflict is some other refusal, never "still landing".
        assert_eq!(complete_step(StatusCode::CONFLICT, false), CompleteStep::Refused);
        assert_eq!(complete_step(StatusCode::BAD_REQUEST, false), CompleteStep::Refused);
    }
}

#[cfg(test)]
mod claim_tests {
    use super::*;
    use axum::http::StatusCode;
    use weft_core::storage::slot::{ClaimUploadRequest, ClaimUploadResponse};

    /// A broker answering `upload/claim`: a claim for an `Audio` place is
    /// refused naming its slot (409), a slot named `short` gets an answer
    /// missing a file, a slot named `locked` is the worker's own
    /// credentials refused (401), and anything else is answered with one
    /// file per claim, keyed by its slot. Also answers how many requests
    /// it took.
    async fn broker() -> (Arc<WorkerStorage>, Arc<std::sync::atomic::AtomicUsize>) {
        let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = asked.clone();
        let claim = move |axum::Json(req): axum::Json<ClaimUploadRequest>| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::ready(answer(req))
        };
        fn answer(req: ClaimUploadRequest) -> axum::response::Response {
            use axum::response::IntoResponse;
            if let Some(refused) = req.claims.iter().find(|c| c.fits == "Audio") {
                return (StatusCode::CONFLICT, format!("the file uploaded through '{}' does not fit here", refused.slot)).into_response();
            }
            if req.claims.iter().any(|c| c.slot == "locked") {
                return (StatusCode::UNAUTHORIZED, "who are you").into_response();
            }
            let files = req.claims.iter().filter(|c| c.slot != "short").map(|c| serde_json::json!({"key": c.slot, "fits": c.fits})).collect();
            axum::Json(ClaimUploadResponse { files }).into_response()
        }
        let app = axum::Router::new().route("/v1/storage/upload/claim", axum::routing::post(claim));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let token = weft_broker_client::TokenSource::worker(Arc::new(weft_platform_traits::identity::FixedToken("t".into())), "r1");
        (WorkerStorage::new(format!("http://{addr}"), token), asked)
    }

    /// Every slot of one claim goes to the broker in ONE request and the
    /// files come back in the claims' order; a refusal of any slot is the
    /// caller's input error in the broker's words, and an answer missing a
    /// file or the worker's credentials refused is the node's failure.
    /// (That a refused claim takes none of the slots is the broker's.)
    #[tokio::test]
    async fn uploads_are_claimed_in_one_request() {
        let (storage, asked) = broker().await;
        let ty = |t: &str| WeftType::parse(t).unwrap();
        let (image, video, audio) = (ty("Image"), ty("Video | Null"), ty("Audio"));
        let files = storage.claim_uploads(RunId::nil(), &[("weft-upload:a", &image), ("weft-upload:b", &video)]).await.unwrap();
        assert_eq!(
            files,
            [serde_json::json!({"key": "weft-upload:a", "fits": "Image"}), serde_json::json!({"key": "weft-upload:b", "fits": "Video | Null"})]
        );
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1, "two slots, one request");

        let refused = storage.claim_uploads(RunId::nil(), &[("weft-upload:a", &image), ("weft-upload:b", &audio)]).await.unwrap_err();
        assert!(matches!(&refused, WeftError::Input(why) if why.contains("'weft-upload:b' does not fit")), "{refused:?}");

        let short = storage.claim_uploads(RunId::nil(), &[("weft-upload:a", &image), ("short", &image)]).await.unwrap_err();
        assert!(matches!(&short, WeftError::NodeFailed(why) if why.contains("asked for 2 files, the broker answered 1")), "{short:?}");

        let locked = storage.claim_uploads(RunId::nil(), &[("locked", &image)]).await.unwrap_err();
        assert!(matches!(locked, WeftError::NodeFailed(_)), "{locked:?}");
    }
}

#[cfg(test)]
mod busy_tests {
    use super::*;
    use axum::http::StatusCode;
    use std::sync::atomic::{AtomicU32, Ordering};
    use weft_broker_client::protocol::NOT_DONE;

    fn reply(status: reqwest::StatusCode, body: &str) -> WeftResult<Reply> {
        Ok(Reply { status, completing: false, body: bytes::Bytes::from(body.to_string()) })
    }

    /// Only the broker's own "nothing was done" is sent again; a 503 that
    /// may have half happened, any other status, or a broker that could
    /// not be reached is the call's answer.
    #[test]
    fn only_a_storage_call_turned_away_busy_is_sent_again() {
        assert!(turned_away(&reply(reqwest::StatusCode::SERVICE_UNAVAILABLE, NOT_DONE)));
        assert!(!turned_away(&reply(reqwest::StatusCode::SERVICE_UNAVAILABLE, "the database connection dropped; ask again")));
        assert!(!turned_away(&reply(reqwest::StatusCode::INTERNAL_SERVER_ERROR, NOT_DONE)));
        assert!(!turned_away(&reply(reqwest::StatusCode::OK, "{}")));
        assert!(!turned_away(&Err(WeftError::NodeFailed("storage: keep: connection refused".into()))));
    }

    /// A broker whose `keep` turns the first three calls away busy, then
    /// keeps; whose `delete` answers a 503 that may have half happened;
    /// and whose `upload/complete` turns the first three away busy, then
    /// answers the file. Each route counts its calls.
    async fn broker(calls: Arc<[AtomicU32; 3]>) -> Arc<WorkerStorage> {
        use axum::response::IntoResponse;
        let busy = || (StatusCode::SERVICE_UNAVAILABLE, NOT_DONE).into_response();
        let (keep, delete, complete) = (calls.clone(), calls.clone(), calls);
        let app = axum::Router::new()
            .route(
                "/v1/storage/keep",
                axum::routing::post(move || async move {
                    if keep[0].fetch_add(1, Ordering::SeqCst) < 3 { busy() } else { StatusCode::NO_CONTENT.into_response() }
                }),
            )
            .route(
                "/v1/storage/files/{*key}",
                axum::routing::delete(move || async move {
                    delete[1].fetch_add(1, Ordering::SeqCst);
                    (StatusCode::SERVICE_UNAVAILABLE, "the database connection dropped, or the database is unavailable; ask again")
                        .into_response()
                }),
            )
            .route(
                "/v1/storage/upload/complete",
                axum::routing::post(move || async move {
                    if complete[2].fetch_add(1, Ordering::SeqCst) < 3 {
                        busy()
                    } else {
                        axum::Json(serde_json::json!({"key": "k"})).into_response()
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let token = weft_broker_client::TokenSource::worker(Arc::new(weft_platform_traits::identity::FixedToken("t".into())), "r1");
        WorkerStorage::new(format!("http://{addr}"), token)
    }

    /// A storage call turned away busy waits and goes through, without
    /// failing the node; one answered a 503 that may have half happened
    /// fails the node at once, sent once. A completion's busy answers
    /// spend none of its attempts: it lands on the fourth call.
    #[tokio::test]
    async fn a_storage_call_turned_away_busy_waits_until_it_goes_through() {
        let calls = Arc::new([AtomicU32::new(0), AtomicU32::new(0), AtomicU32::new(0)]);
        let storage = broker(calls.clone()).await;
        let run = RunId::nil();

        storage.keep(run, "k", KeepTtl::Never).await.unwrap();
        assert_eq!(calls[0].load(Ordering::SeqCst), 4);

        let failed = storage.delete(run, "k").await.unwrap_err();
        assert!(matches!(&failed, WeftError::NodeFailed(why) if why.contains("503")), "{failed:?}");
        assert_eq!(calls[1].load(Ordering::SeqCst), 1, "a 503 that may have half happened is not sent again");

        assert_eq!(storage.complete_until_landed(run, "k").await.unwrap(), serde_json::json!({"key": "k"}));
        assert_eq!(calls[2].load(Ordering::SeqCst), 4);
    }
}

#[cfg(test)]
mod abort_tests {
    use super::*;
    use axum::http::StatusCode;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// What the fake broker and bucket saw of one upload.
    #[derive(Default)]
    struct Seen {
        parts_done: AtomicU32,
        completes: AtomicU32,
        aborted: std::sync::Mutex<Vec<String>>,
    }

    /// A broker that opens every upload under `run/r/f` in parts of four
    /// bytes, signs each part to its own bucket address beside it, and
    /// writes down every part reported landed, every completion and every
    /// abort; the bucket takes any part.
    async fn broker(seen: Arc<Seen>) -> Arc<WorkerStorage> {
        use axum::response::IntoResponse;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (done, completes, aborts) = (seen.clone(), seen.clone(), seen);
        let app = axum::Router::new()
            .route(
                "/v1/storage/upload/begin",
                axum::routing::post(|| async { axum::Json(serde_json::json!({ "key": "run/r/f", "partSize": 4 })) }),
            )
            .route(
                "/v1/storage/upload/parts",
                axum::routing::post(move |axum::Json(asked): axum::Json<weft_core::storage::UploadPartsRequest>| async move {
                    let parts = asked
                        .parts
                        .iter()
                        .map(|part| serde_json::json!({
                            "partNumber": part.part_number,
                            "sizeBytes": part.size_bytes,
                            "offsetBytes": 0,
                            "url": format!("http://{addr}/bucket/{}", part.part_number),
                        }))
                        .collect::<Vec<_>>();
                    axum::Json(serde_json::json!({ "parts": parts }))
                }),
            )
            .route("/bucket/{part}", axum::routing::put(|| async { ([("etag", "\"e\"")], "").into_response() }))
            .route(
                "/v1/storage/upload/part-done",
                axum::routing::post(move || async move {
                    done.parts_done.fetch_add(1, Ordering::SeqCst);
                    StatusCode::NO_CONTENT
                }),
            )
            .route(
                "/v1/storage/upload/complete",
                axum::routing::post(move || async move {
                    completes.completes.fetch_add(1, Ordering::SeqCst);
                    axum::Json(serde_json::json!({ "key": "run/r/f" }))
                }),
            )
            .route(
                "/v1/storage/upload/abort",
                axum::routing::post(move |axum::Json(abort): axum::Json<weft_core::storage::UploadAbortRequest>| async move {
                    aborts.aborted.lock().unwrap().push(abort.key);
                    StatusCode::NO_CONTENT
                }),
            );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let token = weft_broker_client::TokenSource::worker(Arc::new(weft_platform_traits::identity::FixedToken("t".into())), "r1");
        WorkerStorage::new(format!("http://{addr}"), token)
    }

    /// A source that breaks off part way through a file fails the node's
    /// put, and the upload it began is aborted, once and by its key, so
    /// the broker frees what it reserved for it; it is never completed,
    /// so no cut-short file is kept as if it were whole. Here one whole
    /// part had landed when the source broke.
    #[tokio::test]
    async fn an_upload_whose_source_breaks_off_is_aborted_and_never_completed() {
        let seen = Arc::new(Seen::default());
        let storage = broker(seen.clone()).await;
        let breaking: ByteStream = Box::pin(futures::stream::iter(vec![
            Ok(bytes::Bytes::from_static(b"abcd")),
            Ok(bytes::Bytes::from_static(b"ef")),
            Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "the source hung up")),
        ]));
        let failed = storage
            .put(RunId::nil(), &StorageScope::Run, None, "application/octet-stream", "f.bin", None, Some(12), breaking)
            .await
            .unwrap_err();
        assert!(matches!(&failed, WeftError::NodeFailed(why) if why.contains("the source hung up")), "{failed:?}");
        assert_eq!(seen.parts_done.load(Ordering::SeqCst), 1, "the whole part that came first landed");
        assert_eq!(*seen.aborted.lock().unwrap(), vec!["run/r/f".to_string()]);
        assert_eq!(seen.completes.load(Ordering::SeqCst), 0, "a broken upload is never completed");
    }
}
