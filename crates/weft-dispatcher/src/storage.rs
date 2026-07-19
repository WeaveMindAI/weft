//! The dispatcher's storage control plane: the CLI `weft files` proxy to the
//! broker's runtime-file admin surface, plus the durable terminate-sweep queue.
//!
//! The broker is the single gatekeeper that owns the bucket + the
//! `runtime_file` metadata; the dispatcher never touches bytes. It is here only
//! to (1) front the CLI verbs (the CLI authenticates to the dispatcher, which
//! resolves the acting tenant and forwards to the broker as the control plane),
//! and (2) durably drive the terminate sweep: a worker can stall-then-die before
//! its eager sweep runs, so the journal bridge enqueues a row per terminated
//! color and this reaper drains it by asking the broker to sweep the color's
//! un-kept exec files.

use anyhow::{Context, Result};
use sqlx::PgPool;

use weft_core::storage::{
    AdminUploadBeginRequest, ListFilesResponse, ListPrefixRequest, PartDoneRequest,
    PresignRequest, PresignResult,
    StoredFileMeta, SweepExecRequest, SweepExecResponse, Tenanted, TenantScopeRequest, TenantUsage,
    UploadAbortRequest, UploadBeginResponse, UploadCompleteRequest, UploadPartsRequest,
    UploadPartsResponse, UploadResumeRequest, UploadResumeResponse, WipePrefixRequest,
    WipePrefixResponse,
};

use crate::state::DispatcherState;

// ---------- broker admin client ----------
//
// The wire envelopes live in `weft_core::storage` (single definition, shared
// with the broker's handlers), so the two ends cannot drift.

/// The dispatcher's authenticated client of the broker's runtime-file admin
/// surface. It signs every request with the dispatcher's own SA token, which
/// the broker resolves to the control-plane identity.
async fn read_token(state: &DispatcherState) -> Result<String> {
    // Re-read every call so kubelet token rotation propagates; async so the
    // read never blocks the runtime (the token is re-projected periodically).
    let bytes = tokio::fs::read(&state.broker_token_path)
        .await
        .with_context(|| format!("read dispatcher SA token at {}", state.broker_token_path.display()))?;
    Ok(String::from_utf8(bytes).context("SA token not utf8")?.trim().to_string())
}

fn admin_url(state: &DispatcherState, path: &str) -> String {
    format!("{}{}", state.broker_url.trim_end_matches('/'), path)
}

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
/// it (bad tenant/color/key shape, denied scope). Terminal for the request as
/// sent; retrying the identical request can never succeed. Carried typed through
/// the anyhow chain so retry loops (the sweep queue) can tell a dead request
/// from a transient broker fault.
#[derive(Debug)]
pub struct BrokerRejected {
    pub status: reqwest::StatusCode,
}

impl std::fmt::Display for BrokerRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "broker rejected the request ({})", self.status)
    }
}

impl std::error::Error for BrokerRejected {}

async fn check(resp: reqwest::Response, what: &str) -> Result<reqwest::Response> {
    use reqwest::StatusCode;
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
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
    // Deliberately NOT terminal: 401 UNAUTHORIZED. The broker returns 401 when the
    // Kubernetes TokenReview *call itself* fails (kube-apiserver unreachable /
    // throttled / a momentarily-stale SA token across rotation), i.e. a TRANSIENT
    // control-plane fault, not a permanent refusal. Treating it as terminal would
    // permanently dead-letter a storage sweep on a passing apiserver blip and leak
    // the files. So 401 falls through to the transient bail below and is retried,
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
        return Err(anyhow::Error::new(BrokerRejected { status })
            .context(format!("broker storage {what} returned {status}: {body}")));
    }
    // Everything else (401 auth-resolution fault, 429, 5xx, unexpected): transient,
    // retry.
    anyhow::bail!("broker storage {what} returned {status}: {body}")
}

/// POST one admin verb to the broker and parse its JSON response. Every
/// admin-surface call is this exact shape (SA bearer, JSON in, JSON out,
/// status classified by `check`), so it lives once.
async fn post_admin<Resp: serde::de::DeserializeOwned>(
    state: &DispatcherState,
    path: &str,
    what: &str,
    body: &impl serde::Serialize,
) -> Result<Resp> {
    let resp = state
        .http
        .post(admin_url(state, path))
        .bearer_auth(read_token(state).await?)
        .json(body)
        .send()
        .await
        .with_context(|| format!("broker {what}"))?;
    check(resp, what).await?.json().await.with_context(|| format!("{what} parse"))
}

/// Same as `post_admin` for verbs whose success response carries no body.
async fn post_admin_unit(
    state: &DispatcherState,
    path: &str,
    what: &str,
    body: &impl serde::Serialize,
) -> Result<()> {
    let resp = state
        .http
        .post(admin_url(state, path))
        .bearer_auth(read_token(state).await?)
        .json(body)
        .send()
        .await
        .with_context(|| format!("broker {what}"))?;
    check(resp, what).await?;
    Ok(())
}

/// List one tenant's runtime files (the `weft files ls` surface).
pub async fn tenant_list(state: &DispatcherState, tenant: &str) -> Result<Vec<StoredFileMeta>> {
    let out: ListFilesResponse = post_admin(
        state,
        "/v1/storage/admin/tenant-list",
        "tenant-list",
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
        &TenantScopeRequest { tenant: tenant.to_string() },
    )
    .await
}

/// Delete one file by its tenant-anchored key (`weft files rm <key>`).
pub async fn delete_key(state: &DispatcherState, key: &str) -> Result<()> {
    let resp = state
        .http
        .delete(admin_url(state, &format!("/v1/storage/admin/files/{key}")))
        .bearer_auth(read_token(state).await?)
        .send()
        .await
        .context("broker delete-key")?;
    check(resp, "delete-key").await?;
    Ok(())
}

/// Presign a download URL for one file (the `weft files download` handshake),
/// with the file's name + size for the CLI.
pub async fn presign(state: &DispatcherState, key: &str, ttl_secs: Option<u64>) -> Result<PresignResult> {
    post_admin(
        state,
        "/v1/storage/admin/presign",
        "presign",
        &PresignRequest { key: key.to_string(), ttl_secs },
    )
    .await
}

/// Wipe a whole scope/tenant prefix (`weft files rm <prefix>` / project-delete).
pub async fn wipe_prefix(state: &DispatcherState, prefix: &str) -> Result<u64> {
    let out: WipePrefixResponse = post_admin(
        state,
        "/v1/storage/admin/wipe-prefix",
        "wipe-prefix",
        &WipePrefixRequest { prefix: prefix.to_string() },
    )
    .await?;
    Ok(out.wiped)
}

/// Terminate-sweep a color's un-kept exec files: the broker reaps crashed
/// uploads now and stamps completed files with the post-run linger expiry.
async fn sweep_exec(state: &DispatcherState, tenant: &str, color: &str) -> Result<SweepExecResponse> {
    post_admin(
        state,
        "/v1/storage/admin/sweep-exec",
        "sweep-exec",
        &SweepExecRequest { tenant: tenant.to_string(), color: color.to_string() },
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
/// id); returns the minted key + part size.
pub async fn upload_begin(
    state: &DispatcherState,
    tenant: &str,
    project: &str,
    req: UploadBeginParams,
) -> Result<UploadBeginResponse> {
    post_admin(
        state,
        "/v1/storage/admin/upload/begin",
        "upload-begin",
        &AdminUploadBeginRequest {
            tenant: tenant.to_string(),
            project: project.to_string(),
            mime_type: req.mime_type,
            filename: req.filename,
            declared_size: req.declared_size,
            content_hash: req.content_hash,
        },
    )
    .await
}

/// The begin parameters the sync supplies (tenant/project are resolved by the
/// api layer, not caller-claimed).
pub struct UploadBeginParams {
    pub mime_type: String,
    pub filename: String,
    pub declared_size: Option<u64>,
    pub content_hash: String,
}

/// The files under one project's asset prefix: the sync's diff input.
pub async fn asset_list(
    state: &DispatcherState,
    tenant: &str,
    project: &str,
) -> Result<Vec<StoredFileMeta>> {
    let prefix = weft_core::storage::key::ParsedKey::asset_prefix(tenant, project)
        .map_err(|e| anyhow::anyhow!("asset prefix: {e}"))?;
    let out: ListFilesResponse = post_admin(
        state,
        "/v1/storage/admin/list-prefix",
        "list-prefix",
        &ListPrefixRequest { prefix },
    )
    .await?;
    Ok(out.files)
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
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

// ---------- durable terminate-sweep queue ----------

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        -- Durable terminate-sweep queue: a row per terminated color whose
        -- un-kept exec files still need sweeping. Inserted by the journal
        -- bridge (the durable observer of terminate), deleted by the sweep
        -- reaper once the broker confirmed the sweep.
        CREATE TABLE IF NOT EXISTS storage_sweep (
            color TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            enqueued_at_unix BIGINT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await
    .context("storage_sweep migrate")?;
    Ok(())
}

/// Enqueue a terminate sweep for `color`. Called by the journal bridge when it
/// observes a terminal exec event; idempotent.
pub async fn enqueue_sweep(pool: &PgPool, tenant: &str, color: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO storage_sweep (color, tenant_id, enqueued_at_unix) \
         VALUES ($1, $2, $3) ON CONFLICT (color) DO NOTHING",
    )
    .bind(color)
    .bind(tenant)
    .bind(crate::lease::now_unix())
    .execute(pool)
    .await?;
    Ok(())
}

/// Sweep-queue reaper: ask the broker to sweep each pending color's un-kept
/// exec files. A row is removed only after the broker confirmed; a TRANSIENT
/// broker error (unreachable, 5xx) leaves the row for the next tick (the sweep
/// is idempotent). A TERMINAL refusal (a 4xx: the broker understood and
/// rejected the request) is loud + dead-lettered: retrying the identical
/// request every tick forever would be a silent infinite loop over a row the
/// user can neither see nor clear, so the row is dropped with an error log
/// naming the color (the files, if any, remain reclaimable via `weft files`).
pub async fn process_sweep_queue(state: DispatcherState) -> Result<()> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT color, tenant_id FROM storage_sweep ORDER BY enqueued_at_unix")
            .fetch_all(&state.pg_pool)
            .await?;
    for (color, tenant) in rows {
        match sweep_exec(&state, &tenant, &color).await {
            Ok(out) => {
                if out.swept > 0 || out.lingering > 0 {
                    tracing::info!(
                        target: "weft_dispatcher::storage",
                        %color, tenant = %tenant, swept = out.swept, lingering = out.lingering,
                        "terminate sweep: reaped crashed uploads, stamped completed \
                         un-kept exec files with the post-run linger expiry"
                    );
                }
                sqlx::query("DELETE FROM storage_sweep WHERE color = $1")
                    .bind(&color)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) if e.downcast_ref::<StorageNotFound>().is_some() => {
                // 404 from the broker means there was nothing to sweep for this
                // color (its files are already gone). That is terminal SUCCESS, not
                // a rejection: drop the row quietly (debug, not error).
                tracing::debug!(
                    target: "weft_dispatcher::storage",
                    %color, tenant = %tenant,
                    "terminate sweep found nothing to remove; clearing the queue row"
                );
                sqlx::query("DELETE FROM storage_sweep WHERE color = $1")
                    .bind(&color)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) if e.downcast_ref::<BrokerRejected>().is_some() => {
                // The broker understood and permanently refuses this request (bad
                // shape / denied / over quota). Retrying it every tick forever would
                // be a silent infinite loop over a row nobody can clear, so drop it
                // with a loud error naming the color (any files stay reclaimable via
                // `weft files`).
                tracing::error!(
                    target: "weft_dispatcher::storage",
                    %color, tenant = %tenant, error = format!("{e:#}"),
                    "terminate sweep REJECTED by the broker; dropping the queue row \
                     (any remaining files for this color stay listable/deletable via \
                     the storage API)"
                );
                sqlx::query("DELETE FROM storage_sweep WHERE color = $1")
                    .bind(&color)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) => {
                // Transient (broker unreachable, apiserver blip surfaced as 401,
                // 5xx): keep the row and retry next tick.
                tracing::warn!(
                    target: "weft_dispatcher::storage",
                    %color, tenant = %tenant, error = %e,
                    "terminate sweep deferred (transient broker/control-plane fault); will retry"
                );
            }
        }
    }
    Ok(())
}
