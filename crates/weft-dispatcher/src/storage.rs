//! The dispatcher's storage control plane: the CLI `weft files` proxy to the
//! broker's runtime-file admin surface, plus the durable terminate-sweep queue.
//!
//! The broker is the single gatekeeper that owns the bucket + the
//! `runtime_file` metadata; the dispatcher carries no stored bytes of its own
//! (it reads a version's files on a link the broker signs, and its forwarder,
//! `crate::files_door`, passes requests on to the store unchanged). It is here
//! only to (1) front the CLI verbs (the CLI authenticates to the dispatcher, which
//! resolves the acting tenant and forwards to the broker as the control plane),
//! and (2) durably delete the files of the runs whose record went: a run's
//! files are deleted with its record, so the erase of every run enqueues a
//! row in the same transaction, and this reaper drains
//! it by asking the broker to delete the run's files.

use anyhow::{Context, Result};
use weft_task_store::db::Client as _;
use weft_task_store::drain::DrainStep;

use weft_core::storage::{
    AdminLinkRequest, AssetUploadBeginRequest, AssetsHeldRequest, AssetsHeldResponse, LinkReader, ListFilesResponse,
    PartDoneRequest, PresignResult,
    RunFilesRequest, RunFilesResponse, StoredFileMeta, Tenanted, TenantScopeRequest, TenantUsage,
    UploadAbortRequest, UploadBeginResponse, UploadCompleteRequest, UploadPartsRequest,
    UploadPartsResponse, UploadResumeRequest, UploadResumeResponse, WipePrefixRequest,
    WipePrefixResponse,
};

use crate::state::DispatcherState;

/// How long one storage call to the broker may take when its work does
/// not grow with what is stored: a row read or written, a link signed,
/// parts reserved, one tenant's files listed. The broker answers these in
/// well under a second, and within its database's own wait for a
/// connection when the database is busy; a broker that took the call and
/// never answered would otherwise hold it, and whatever waits on it, for
/// good. The calls whose work grows with the data (a prefix wiped, a run's
/// files swept, an upload's parts assembled) pass no limit.
pub(crate) const BROKER_STORAGE_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// The slowest a read of a stored file on a signed link may go before it
/// is given up: the store and the install sit in one cloud region (or on
/// one machine), where a read goes many times faster than this.
const STORE_READ_FLOOR_BYTES_PER_SEC: u64 = 1024 * 1024;

/// What a read of a stored file is given on top of its length's time:
/// the store's first byte, which can take seconds on a cold bucket.
const STORE_READ_START: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a read of a stored file `size_bytes` long may take
/// ([`STORE_READ_START`] plus its length at [`STORE_READ_FLOOR_BYTES_PER_SEC`]):
/// bounded, since nothing the person controls is in it, and sized by the
/// file, since a large one takes longer.
fn store_read_timeout(size_bytes: u64) -> std::time::Duration {
    STORE_READ_START + std::time::Duration::from_secs(size_bytes.div_ceil(STORE_READ_FLOOR_BYTES_PER_SEC))
}

// ---------- broker admin client ----------
//
// The wire envelopes live in `weft_core::storage` (single definition, shared
// with the broker's handlers), so the two ends cannot drift.

// The dispatcher's authenticated client of the broker's runtime-file admin
// surface: identity + URL joining live in `crate::role_client` (shared
// with the access-admin forwards and the listener); the typed retry
// classes below are this surface's own.

/// A sentinel in the error chain saying the broker answered 404 for a single-file
/// op. The api layer downcasts to this so a missing file surfaces as 404 to the
/// user instead of a blanket 500 (the broker's status class collapsed by `check`).
#[derive(Debug)]
pub struct StorageNotFound;

impl std::fmt::Display for StorageNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage object not found")
    }
}
impl std::error::Error for StorageNotFound {}

/// A broker 4xx other than 404: the broker understood the request and REFUSED
/// it (bad tenant/run/key shape, denied scope). Terminal for the request as
/// sent; retrying the identical request can never succeed. Carried typed through
/// the anyhow chain so retry loops (the sweep queue) can tell a dead request
/// from a transient broker fault.
#[derive(Debug)]
pub struct BrokerRejected {
    pub status: reqwest::StatusCode,
    /// The broker marked the refusal "this upload is being completed right
    /// now" ([`weft_core::storage::COMPLETING_HEADER`]); passed through so
    /// the caller can ask again instead of giving up.
    pub completing: bool,
}

impl std::fmt::Display for BrokerRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "broker rejected the request ({})", self.status)
    }
}

impl std::error::Error for BrokerRejected {}

/// The broker was out of reach, or said it could not serve right now (a
/// 502, 503 or 504 other than [`StorageBusy`]'s): a fault that passes,
/// which the person asks again on.
#[derive(Debug)]
pub struct StorageUnavailable;

impl std::fmt::Display for StorageUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage is out of reach right now")
    }
}

impl std::error::Error for StorageUnavailable {}

/// The broker turned the call away busy with nothing of it run
/// (`weft_broker_client::client::answered_busy`: its database gave it no
/// connection, or the platform in front of it handed it to no copy):
/// the same call goes through once it can.
#[derive(Debug)]
pub struct StorageBusy;

impl std::fmt::Display for StorageBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage turned the call away busy, and nothing of it ran")
    }
}

impl std::error::Error for StorageBusy {}

/// Whether `e` failed because the broker turned it away busy
/// ([`StorageBusy`]), anywhere in its chain.
pub fn storage_busy(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| cause.is::<StorageBusy>())
}

/// Whether `e` failed because storage could not be reached: the broker
/// answered it was unavailable ([`StorageUnavailable`]), or the request to it
/// never got through (no connection, a timeout).
pub fn storage_unreachable(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause.is::<StorageUnavailable>()
            || cause.downcast_ref::<reqwest::Error>().is_some_and(|r| r.is_connect() || r.is_timeout())
    })
}

async fn check(resp: reqwest::Response, what: &str) -> Result<reqwest::Response> {
    use reqwest::StatusCode;
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    let completing = resp.headers().contains_key(weft_core::storage::COMPLETING_HEADER);
    let body = resp.text().await.unwrap_or_default();
    if status == StatusCode::NOT_FOUND {
        // Preserve the 404 class through the anyhow chain: the api handler downcasts
        // to StorageNotFound and returns 404, not 500, for a missing file.
        return Err(anyhow::Error::new(StorageNotFound)
            .context(format!("broker storage {what} returned {status}: {body}")));
    }
    // Terminal client errors ONLY: the broker understood the request and refuses it
    // permanently (bad shape, denied scope, over quota, method/conflict). These are
    // BrokerRejected so the sweep queue stops retrying them.
    //
    // Deliberately NOT terminal: 401 UNAUTHORIZED. The broker returns 401 when it
    // cannot verify the caller's identity token, which includes a TRANSIENT fault
    // (the platform's signing keys momentarily unreachable, a token minted just
    // across a key rotation), not only a permanent refusal. Treating it as terminal
    // would permanently dead-letter a storage sweep on a passing blip and leak the
    // files. So 401 falls through to the transient bail below and is retried,
    // alongside 5xx.
    let terminal = matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::FORBIDDEN
            | StatusCode::PAYLOAD_TOO_LARGE
            | StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::CONFLICT
            | StatusCode::UNPROCESSABLE_ENTITY
    );
    if terminal {
        // A refusal, not a fault: typed so retry loops can stop retrying it.
        return Err(anyhow::Error::new(BrokerRejected { status, completing })
            .context(format!("broker storage {what} returned {status}: {body}")));
    }
    // Checked before the unavailable statuses below: a busy answer is a
    // 503 too, and only this one says nothing of the call ran.
    if weft_broker_client::client::answered_busy(status, body.as_bytes()) {
        return Err(anyhow::Error::new(StorageBusy).context(format!("broker storage {what} returned {status}: {body}")));
    }
    if matches!(status, StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT) {
        return Err(anyhow::Error::new(StorageUnavailable).context(format!("broker storage {what} returned {status}: {body}")));
    }
    // Everything else (401 auth-resolution fault, 429, 5xx, unexpected): transient,
    // retry.
    anyhow::bail!("broker storage {what} returned {status}: {body}")
}

/// POST one admin verb to the broker and parse its JSON response. Every
/// admin-surface call is this exact shape (identity bearer, JSON in, JSON out,
/// status classified by `check`), so it lives once. `limit` bounds the
/// whole call ([`BROKER_STORAGE_CALL_TIMEOUT`]); `None` for a verb whose
/// work grows with the data.
async fn post_admin<Resp: serde::de::DeserializeOwned>(
    state: &DispatcherState,
    path: &str,
    what: &str,
    limit: Option<std::time::Duration>,
    body: &impl serde::Serialize,
) -> Result<Resp> {
    let resp = admin_call(state, path, what, limit, body).await?;
    check(resp, what).await?.json().await.with_context(|| format!("{what} parse"))
}

/// Same as `post_admin` for verbs whose success response carries no body.
async fn post_admin_unit(
    state: &DispatcherState,
    path: &str,
    what: &str,
    limit: Option<std::time::Duration>,
    body: &impl serde::Serialize,
) -> Result<()> {
    let resp = admin_call(state, path, what, limit, body).await?;
    check(resp, what).await?;
    Ok(())
}

/// Send one admin verb to the broker, within `limit` when there is one.
async fn admin_call(
    state: &DispatcherState,
    path: &str,
    what: &str,
    limit: Option<std::time::Duration>,
    body: &impl serde::Serialize,
) -> Result<reqwest::Response> {
    let mut request = state.broker.request(reqwest::Method::POST, path).await?.json(body);
    if let Some(limit) = limit {
        request = request.timeout(limit);
    }
    request.send().await.with_context(|| format!("broker {what}"))
}

/// Read one active file without changing its lifetime or minting a link.
pub async fn file_meta(state: &DispatcherState, key: &str) -> Result<StoredFileMeta> {
    let response = state
        .broker
        .request(reqwest::Method::GET, &format!("/v1/storage/admin/meta/{key}"))
        .await?
        .timeout(BROKER_STORAGE_CALL_TIMEOUT)
        .send()
        .await
        .context("read stored file metadata")?;
    check(response, "file metadata").await?.json().await.context("parse stored file metadata")
}

/// List one tenant's runtime files (the `weft files ls` surface).
pub async fn tenant_list(state: &DispatcherState, tenant: &str) -> Result<Vec<StoredFileMeta>> {
    let out: ListFilesResponse = post_admin(
        state,
        "/v1/storage/admin/tenant-list",
        "tenant-list",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &TenantScopeRequest { tenant: tenant.to_string() },
    )
    .await?;
    Ok(out.files)
}

/// One tenant's footprint (the `weft files usage` surface).
pub async fn tenant_usage(state: &DispatcherState, tenant: &str) -> Result<TenantUsage> {
    post_admin(
        state,
        "/v1/storage/admin/tenant-usage",
        "tenant-usage",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &TenantScopeRequest { tenant: tenant.to_string() },
    )
    .await
}

/// Publish current source references without changing node-created files' TTLs.
pub async fn set_asset_references(
    state: &DispatcherState,
    tenant: &str,
    references: weft_core::storage::AssetReferencesRequest,
) -> Result<weft_core::storage::AssetReferencesResponse> {
    post_admin(
        state,
        "/v1/storage/admin/asset-references",
        "update asset lifetimes",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &Tenanted { tenant: tenant.into(), inner: references },
    ).await
}

/// Delete one file by its tenant-anchored key (`weft files rm <key>`).
pub async fn delete_key(state: &DispatcherState, key: &str) -> Result<()> {
    let resp = state
        .broker
        .request(reqwest::Method::DELETE, &format!("/v1/storage/admin/files/{key}"))
        .await?
        .timeout(BROKER_STORAGE_CALL_TIMEOUT)
        .send()
        .await
        .context("broker delete-key")?;
    check(resp, "delete-key").await?;
    Ok(())
}

/// A download link for one file, with its name and size, for `reader`:
/// a person's machine (the CLI, the editor), weft's own process, or
/// somebody outside reaching the install as a project's callers do. The
/// link goes straight to the store (or through the install's forwarder
/// for the outside), so a client may ask for any `Range` of it and resume.
pub async fn download_link(
    state: &DispatcherState,
    key: &str,
    ttl_secs: Option<u64>,
    reader: LinkReader,
) -> Result<PresignResult> {
    post_admin(
        state,
        "/v1/storage/admin/link",
        "link",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &AdminLinkRequest { key: key.to_string(), ttl_secs, reader },
    )
    .await
}

/// The whole content of one stored file, read straight from the store on
/// a link the broker signed for this process. What a version build reads
/// its files with.
pub async fn read_file(state: &DispatcherState, key: &str) -> Result<Vec<u8>> {
    let link = download_link(state, key, None, LinkReader::Runtime).await?;
    let resp = state
        .http
        .get(&link.url)
        .timeout(store_read_timeout(link.size_bytes))
        .send()
        .await
        .with_context(|| format!("read stored file {key}"))?;
    anyhow::ensure!(resp.status().is_success(), "the store answered {} reading stored file {key}", resp.status());
    let bytes = resp.bytes().await.with_context(|| format!("read stored file {key}"))?;
    anyhow::ensure!(
        bytes.len() as u64 == link.size_bytes,
        "stored file {key} came back {} bytes long, and the store records {}",
        bytes.len(),
        link.size_bytes
    );
    Ok(bytes.to_vec())
}

/// One stored file streamed into a new file at `dest`, chunk by chunk, on
/// a link the broker signed for this process: what a version build lays
/// its files out with, so no file is ever whole in memory. A body longer
/// or shorter than the store records fails.
pub async fn download_file(state: &DispatcherState, key: &str, dest: &std::path::Path) -> Result<()> {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    let link = download_link(state, key, None, LinkReader::Runtime).await?;
    let resp = state
        .http
        .get(&link.url)
        .timeout(store_read_timeout(link.size_bytes))
        .send()
        .await
        .with_context(|| format!("read stored file {key}"))?;
    anyhow::ensure!(resp.status().is_success(), "the store answered {} reading stored file {key}", resp.status());
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)
        .await
        .with_context(|| format!("create {}", dest.display()))?;
    let mut body = resp.bytes_stream();
    let mut total = 0u64;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.with_context(|| format!("read stored file {key}"))?;
        total += chunk.len() as u64;
        anyhow::ensure!(
            total <= link.size_bytes,
            "stored file {key} came back longer than the {} bytes the store records",
            link.size_bytes
        );
        file.write_all(&chunk).await.with_context(|| format!("write {}", dest.display()))?;
    }
    file.flush().await.with_context(|| format!("write {}", dest.display()))?;
    anyhow::ensure!(
        total == link.size_bytes,
        "stored file {key} came back {total} bytes long, and the store records {}",
        link.size_bytes
    );
    Ok(())
}

/// A version build's view of the storage plane (`crate::build::ProjectStorage`).
/// It owns its handle on the state, so a build can share it with the
/// blocking compile (`crate::build::source::VersionFiles`).
pub struct BrokerStorage(pub DispatcherState);

#[async_trait::async_trait]
impl crate::build::ProjectStorage for BrokerStorage {
    async fn read(&self, key: &str) -> Result<Vec<u8>> {
        read_file(&self.0, key).await
    }

    async fn download(&self, key: &str, dest: &std::path::Path) -> Result<()> {
        download_file(&self.0, key, dest).await
    }

    async fn meta(&self, key: &str) -> Result<StoredFileMeta> {
        file_meta(&self.0, key).await
    }

    async fn store_texts(&self, tenant: &str, texts: &std::collections::BTreeMap<String, Vec<u8>>) -> Result<()> {
        weft_assets::publish_held(texts, &asset_store(&self.0, tenant)).await
    }
}

/// Wipe a whole scope/tenant prefix (`weft files rm <prefix>` / project-delete).
pub async fn wipe_prefix(state: &DispatcherState, prefix: &str) -> Result<u64> {
    let out: WipePrefixResponse = post_admin(
        state,
        "/v1/storage/admin/wipe-prefix",
        "wipe-prefix",
        // Every file under the prefix is deleted: no limit.
        None,
        &WipePrefixRequest { prefix: prefix.to_string() },
    )
    .await?;
    Ok(out.wiped)
}

/// Delete every file of the runs of `tenant` whose records went.
async fn delete_run_files(state: &DispatcherState, tenant: &str, run_ids: &[weft_core::RunId]) -> Result<RunFilesResponse> {
    post_admin(
        state,
        "/v1/storage/admin/delete-run-files",
        "delete-run-files",
        // Every file the runs stored is deleted: no limit.
        None,
        &RunFilesRequest { tenant: tenant.to_string(), run_ids: run_ids.iter().map(ToString::to_string).collect() },
    )
    .await
}

// ---------- asset upload proxy ----------
//
// The pre-build asset sync drives the broker's multipart upload contract
// through here: the dispatcher resolves the acting tenant (its api layer) and
// forwards each verb to the broker's admin upload surface. Bytes never pass
// through: the returned part URLs are presigned for the caller, which PUTs
// straight to the bucket.

/// Begin an ASSET upload (content-addressed: `content_hash` becomes the key
/// id) for `tenant`, the one the api layer resolved (never caller-claimed);
/// returns the minted key + part size.
pub async fn upload_begin(
    state: &DispatcherState,
    tenant: &str,
    req: AssetUploadBeginRequest,
) -> Result<UploadBeginResponse> {
    post_admin(
        state,
        "/v1/storage/admin/upload/begin",
        "upload-begin",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Begin copying one file into `tenant` from another install.
pub async fn import_begin(
    state: &DispatcherState,
    tenant: &str,
    req: weft_core::storage::FileImportBeginRequest,
) -> Result<UploadBeginResponse> {
    post_admin(state, "/v1/storage/admin/import/begin", "import-begin", Some(BROKER_STORAGE_CALL_TIMEOUT), &Tenanted { tenant: tenant.to_string(), inner: req }).await
}

/// Which of the named contents `tenant` already stores: a publish's diff.
pub async fn assets_held(
    state: &DispatcherState,
    tenant: &str,
    req: AssetsHeldRequest,
) -> Result<AssetsHeldResponse> {
    post_admin(
        state,
        "/v1/storage/admin/assets-held",
        "assets-held",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Reserve + presign the next parts (browser-facing URLs).
pub async fn upload_parts(
    state: &DispatcherState,
    tenant: &str,
    req: UploadPartsRequest,
) -> Result<UploadPartsResponse> {
    post_admin(
        state,
        "/v1/storage/admin/upload/parts",
        "upload-parts",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Record a landed part's etag.
pub async fn upload_part_done(
    state: &DispatcherState,
    tenant: &str,
    req: PartDoneRequest,
) -> Result<()> {
    post_admin_unit(
        state,
        "/v1/storage/admin/upload/part-done",
        "upload-part-done",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Finalize the upload; returns the stored-file marker value the config holds.
pub async fn upload_complete(
    state: &DispatcherState,
    tenant: &str,
    req: UploadCompleteRequest,
) -> Result<serde_json::Value> {
    post_admin(
        state,
        "/v1/storage/admin/upload/complete",
        "upload-complete",
        // The store assembles the file from its parts: no limit.
        None,
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Fresh browser-facing URLs for the parts that never landed.
pub async fn upload_resume(
    state: &DispatcherState,
    tenant: &str,
    req: UploadResumeRequest,
) -> Result<UploadResumeResponse> {
    post_admin(
        state,
        "/v1/storage/admin/upload/resume",
        "upload-resume",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Cancel an in-flight editor upload, freeing its reservation.
pub async fn upload_abort(
    state: &DispatcherState,
    tenant: &str,
    req: UploadAbortRequest,
) -> Result<()> {
    post_admin_unit(
        state,
        "/v1/storage/admin/upload/abort",
        "upload-abort",
        Some(BROKER_STORAGE_CALL_TIMEOUT),
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// One tenant's asset uploads as the install itself drives them, over
/// the verbs above: what dispatcher code publishes files through
/// ([`asset_store`]).
pub struct InstallUploads<'a> {
    state: &'a DispatcherState,
    tenant: String,
}

/// The tenant's content-addressed assets for dispatcher code to publish
/// into: `weft_assets::publish_hashed(&files, &source, &asset_store(..))`.
pub fn asset_store<'a>(state: &'a DispatcherState, tenant: &str) -> weft_assets::upload::ContentStore<InstallUploads<'a>> {
    weft_assets::upload::ContentStore::new(InstallUploads { state, tenant: tenant.to_string() })
}

#[async_trait::async_trait]
impl weft_assets::upload::UploadPlane for InstallUploads<'_> {
    async fn held(&self, hashes: &[String]) -> Result<AssetsHeldResponse> {
        assets_held(self.state, &self.tenant, AssetsHeldRequest { hashes: hashes.to_vec() }).await
    }

    async fn begin(&self, request: weft_assets::upload::Begin) -> Result<UploadBeginResponse> {
        match request {
            weft_assets::upload::Begin::Asset(request) => upload_begin(self.state, &self.tenant, request).await,
            weft_assets::upload::Begin::Copy(request) => import_begin(self.state, &self.tenant, request).await,
        }
    }

    async fn resume(&self, key: &str) -> Result<UploadResumeResponse> {
        upload_resume(self.state, &self.tenant, UploadResumeRequest { key: key.to_string(), sender: weft_core::storage::PartSender::Runtime }).await
    }

    async fn parts(&self, key: &str, parts: Vec<weft_core::storage::PartAsk>) -> Result<Vec<weft_core::storage::PresignedPart>> {
        Ok(upload_parts(self.state, &self.tenant, UploadPartsRequest { key: key.to_string(), parts, sender: weft_core::storage::PartSender::Runtime })
            .await?.parts)
    }

    async fn part_done(&self, request: PartDoneRequest) -> Result<()> {
        upload_part_done(self.state, &self.tenant, request).await
    }

    async fn complete(&self, key: &str) -> Result<()> {
        upload_complete(self.state, &self.tenant, UploadCompleteRequest { key: key.to_string() }).await?;
        Ok(())
    }

    async fn abort(&self, key: &str) -> Result<()> {
        upload_abort(self.state, &self.tenant, UploadAbortRequest { key: key.to_string() }).await
    }

    async fn meta_if_found(&self, key: &str) -> Result<Option<StoredFileMeta>> {
        match file_meta(self.state, key).await {
            Ok(meta) => Ok(Some(meta)),
            Err(e) if e.downcast_ref::<StorageNotFound>().is_some() => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn completing(&self, error: &anyhow::Error) -> bool {
        error.chain().any(|cause| cause.downcast_ref::<BrokerRejected>().is_some_and(|rejected| rejected.completing))
    }

    fn interrupted(&self, error: &anyhow::Error) -> bool {
        storage_unreachable(error) || storage_busy(error)
    }
}

// ---------- durable queue of the runs whose files go ----------

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "storage_sweep",
    tables: &["storage_sweep"],
    ddl: &[r#"
        -- Durable queue of the runs whose files go: a row per run whose
        -- record was deleted, whether it stored files or not. Inserted
        -- by the erase of the run's record (the dispatcher's
        -- `erase_run_ids`), deleted by the reaper once the broker
        -- confirmed the run's files are gone.
        CREATE TABLE IF NOT EXISTS storage_sweep (
            run_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            enqueued_at_unix BIGINT NOT NULL
        );
        "#,
        // Wake the sweep reaper when runs are queued, through the
        // announcement outbox like every other wake (`weft_task_store::announce`):
        // sent once the write commits. One wake per statement, not per
        // row: an erase queues every run it erases at once, and the
        // reaper reads the queue a page at a time whatever woke it.
        // SYNC: 'weft_storage_sweep' <-> crate::reaper::STORAGE_SWEEP_CHANNEL
        r#"CREATE OR REPLACE FUNCTION storage_sweep_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM weft_announce('weft_storage_sweep', '');
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS storage_sweep_notify_on_insert ON storage_sweep"#,
        r#"CREATE TRIGGER storage_sweep_notify_on_insert
            AFTER INSERT ON storage_sweep
            FOR EACH STATEMENT
            EXECUTE FUNCTION storage_sweep_notify()"#,
    ],
    seed: &[],
    data_steps: &[],
};

/// How many queued runs one pass of [`process_sweep_queue`] takes: every
/// erased run is queued, most of them with no file, so a pass reads a page
/// and asks the broker once per tenant in it.
const SWEEP_PAGE: i64 = 500;

/// The reaper of [`GROUP`]'s queue: ask the broker to delete the files of
/// the queued runs, a page at a time, one call per tenant in the page. A
/// row is removed only after the broker confirmed; a fault that passes (the
/// broker unreachable, a 5xx, an upload still completing) leaves the rows
/// and fails the pass once every tenant was tried, so the loop tries again
/// shortly a few times and then writes the failure down for `weft status`
/// (`weft_task_store::drain`); the next run erased wakes it, and every row
/// is tried again (a deletion is idempotent). A refusal (a 4xx: the broker
/// understood and rejected the request, or a file the bucket refused to
/// remove, which stays marked in `weft status`) drops the rows with an
/// error log naming the runs: retrying the identical request every tick
/// forever would be a silent infinite loop over rows the user can neither
/// see nor clear (the files left stay deletable via `weft files rm`).
pub async fn process_sweep_queue(state: DispatcherState) -> Result<DrainStep> {
    let rows: Vec<(weft_core::RunId, String)> = state
        .pg_pool
        .all("SELECT run_id, tenant_id FROM storage_sweep ORDER BY enqueued_at_unix LIMIT $1", &[&SWEEP_PAGE])
        .await?;
    let full_page = rows.len() as i64 == SWEEP_PAGE;
    let mut by_tenant: std::collections::BTreeMap<String, Vec<weft_core::RunId>> = std::collections::BTreeMap::new();
    for (run_id, tenant) in rows {
        by_tenant.entry(tenant).or_default().push(run_id);
    }
    let pool = &state.pg_pool;
    let done = |run_ids: Vec<weft_core::RunId>| async move {
        pool.execute("DELETE FROM storage_sweep WHERE run_id = ANY($1)", &[&run_ids]).await
    };
    let mut deferred = Vec::new();
    for (tenant, run_ids) in by_tenant {
        match delete_run_files(&state, &tenant, &run_ids).await {
            Ok(out) => {
                if out.deleted > 0 {
                    tracing::info!(target: "weft_dispatcher::storage", tenant = %tenant, runs = run_ids.len(), deleted = out.deleted, "deleted the files of runs whose records went");
                }
                done(run_ids).await?;
            }
            // Nothing left under the runs: done.
            Err(e) if e.downcast_ref::<StorageNotFound>().is_some() => {
                done(run_ids).await?;
            }
            Err(e) if e.downcast_ref::<BrokerRejected>().is_some_and(|rejected| !rejected.completing) => {
                tracing::error!(
                    target: "weft_dispatcher::storage",
                    tenant = %tenant, runs = ?run_ids, error = format!("{e:#}"),
                    "deleting runs' files was REFUSED by the broker; dropping their queue rows \
                     (any files left stay listable and deletable via `weft files`)"
                );
                done(run_ids).await?;
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::storage",
                    tenant = %tenant, runs = run_ids.len(), error = %e,
                    "deleting runs' files deferred (a fault that passes)"
                );
                deferred.push(format!("{} runs of tenant {tenant}: {e:#}", run_ids.len()));
            }
        }
    }
    anyhow::ensure!(
        deferred.is_empty(),
        "the broker could not delete the files of erased runs: {}",
        deferred.join("; ")
    );
    Ok(if full_page { DrainStep::More } else { DrainStep::Done })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stored file's read is given its start, plus a second for every
    /// started MiB of it.
    #[test]
    fn a_store_read_is_given_its_start_and_its_length() {
        let secs = |size: u64| store_read_timeout(size).as_secs();
        assert_eq!(secs(0), 30);
        assert_eq!(secs(1), 31, "a partial MiB counts whole");
        assert_eq!(secs(1024 * 1024), 31);
        assert_eq!(secs(1024 * 1024 + 1), 32);
        assert_eq!(secs(2 * 1024 * 1024 * 1024), 30 + 2048, "a 2 GiB file gets about 34 minutes");
    }

    /// A broker 503 saying nothing ran (or the platform's 429) is busy,
    /// which the install answers as its own 503; any other 503 is storage
    /// out of reach.
    #[tokio::test]
    async fn a_busy_broker_is_told_from_storage_out_of_reach() {
        let answer = |status: u16, body: &str| {
            reqwest::Response::from(axum::http::Response::builder().status(status).body(body.to_string()).unwrap())
        };
        let not_done = weft_broker_client::protocol::NOT_DONE;
        let busy = check(answer(503, not_done), "wipe").await.unwrap_err();
        assert!(storage_busy(&busy) && !storage_unreachable(&busy), "{busy:#}");
        let crowded = check(answer(429, ""), "wipe").await.unwrap_err();
        assert!(storage_busy(&crowded), "{crowded:#}");
        let down = check(answer(503, "the object store is down"), "wipe").await.unwrap_err();
        assert!(storage_unreachable(&down) && !storage_busy(&down), "{down:#}");
    }
}
