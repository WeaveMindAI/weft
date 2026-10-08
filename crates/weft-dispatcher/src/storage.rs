//! The dispatcher's storage control plane: the CLI `weft files` proxy to the
//! broker's runtime-file admin surface, plus the durable terminate-sweep queue.
//!
//! The broker is the single gatekeeper that owns the bucket + the
//! `runtime_file` metadata; the dispatcher never touches bytes. It is here only
//! to (1) front the CLI verbs (the CLI authenticates to the dispatcher, which
//! resolves the acting tenant and forwards to the broker as the control plane),
//! and (2) durably drive the terminate sweep: a worker can stall-then-die before
//! its eager sweep runs, so the write that ends a run that stored files of
//! its own enqueues a row, and this reaper drains it by asking the broker to
//! sweep the run's un-kept exec files.

use anyhow::{Context, Result};
use weft_task_store::drain::{DrainStep, SAFETY_POLL_INTERVAL};

use weft_core::storage::{
    AssetUploadBeginRequest, AssetsHeldRequest, AssetsHeldResponse, ListFilesResponse, PartDoneRequest,
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
/// it (bad tenant/execution/key shape, denied scope). Terminal for the request as
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
    // Everything else (401 auth-resolution fault, 429, 5xx, unexpected): transient,
    // retry.
    anyhow::bail!("broker storage {what} returned {status}: {body}")
}

/// POST one admin verb to the broker and parse its JSON response. Every
/// admin-surface call is this exact shape (identity bearer, JSON in, JSON out,
/// status classified by `check`), so it lives once.
async fn post_admin<Resp: serde::de::DeserializeOwned>(
    state: &DispatcherState,
    path: &str,
    what: &str,
    body: &impl serde::Serialize,
) -> Result<Resp> {
    let resp = state
        .broker
        .request(reqwest::Method::POST, path)
        .await?
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
        .broker
        .request(reqwest::Method::POST, path)
        .await?
        .json(body)
        .send()
        .await
        .with_context(|| format!("broker {what}"))?;
    check(resp, what).await?;
    Ok(())
}

/// Read one active file without changing its lifetime or minting a link.
pub async fn file_meta(state: &DispatcherState, key: &str) -> Result<StoredFileMeta> {
    let response = state
        .broker
        .request(reqwest::Method::GET, &format!("/v1/storage/admin/meta/{key}"))
        .await?
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
        &Tenanted { tenant: tenant.into(), inner: references },
    ).await
}

/// Delete one file by its tenant-anchored key (`weft files rm <key>`).
pub async fn delete_key(state: &DispatcherState, key: &str) -> Result<()> {
    let resp = state
        .broker
        .request(reqwest::Method::DELETE, &format!("/v1/storage/admin/files/{key}"))
        .await?
        .send()
        .await
        .context("broker delete-key")?;
    check(resp, "delete-key").await?;
    Ok(())
}

/// Mint a relay download link for one file: the broker's token joined
/// onto the base `base` names, resolved by the public
/// `/public/files/{token}` route. What a browser download rides (a
/// presigned bucket URL breaks the moment a forward/proxy rewrites the
/// host, because its signature covers it; a token URL does not care).
///
/// `base` is the address the client that will FETCH this link reaches
/// this dispatcher at. Callers serving a request pass
/// [`LinkBase::for_request`], built from that request's own host, so
/// the link comes back on whichever of this install's addresses the
/// caller is already using. There is deliberately no default: one
/// install answers on several addresses at once (the operator's
/// loopback, a tunnel's public name, an ingress host), so a link
/// built from a configured constant is wrong for every client that
/// arrived at one of the others, and that failure is invisible until
/// someone clicks the link.
pub async fn download_link(
    state: &DispatcherState,
    base: &LinkBase,
    key: &str,
    ttl_secs: Option<u64>,
) -> Result<PresignResult> {
    let minted: weft_core::storage::DownloadLinkResult = post_admin(
        state,
        "/v1/storage/admin/download-link",
        "download-link",
        &PresignRequest { key: key.to_string(), ttl_secs, reach: weft_core::storage::LinkReach::default() },
    )
    .await?;
    Ok(PresignResult {
        url: format!("{}/public/files/{}", base.as_str(), minted.token),
        filename: minted.filename,
        size_bytes: minted.size_bytes,
    })
}

/// The whole content of one stored file, fetched through the broker (the
/// one service with bucket reach): a link token minted for it, then the
/// broker's own relay of that token. What a version build reads its files
/// with.
pub async fn read_file(state: &DispatcherState, key: &str) -> Result<Vec<u8>> {
    let minted: weft_core::storage::DownloadLinkResult = post_admin(
        state,
        "/v1/storage/admin/download-link",
        "download-link",
        &PresignRequest { key: key.to_string(), ttl_secs: Some(300), reach: weft_core::storage::LinkReach::default() },
    )
    .await?;
    let resp = state
        .broker
        .request(reqwest::Method::GET, &format!("/v1/storage/admin/relay/{}", minted.token))
        .await?
        .send()
        .await
        .context("broker relay")?;
    let bytes = check(resp, "relay").await?.bytes().await.context("read a stored file")?;
    anyhow::ensure!(
        bytes.len() as u64 == minted.size_bytes,
        "stored file {key} came back {} bytes long, and the store records {}",
        bytes.len(),
        minted.size_bytes
    );
    Ok(bytes.to_vec())
}

/// A version build's view of the storage plane (`crate::build::ProjectStorage`).
pub struct BrokerStorage<'a>(pub &'a DispatcherState);

#[async_trait::async_trait]
impl crate::build::ProjectStorage for BrokerStorage<'_> {
    async fn read(&self, key: &str) -> Result<Vec<u8>> {
        read_file(self.0, key).await
    }

    async fn meta(&self, key: &str) -> Result<StoredFileMeta> {
        file_meta(self.0, key).await
    }
}

/// The address a minted link is built on: where the client that will
/// fetch it reaches this dispatcher. Always trailing-slash-free.
///
/// It is a type rather than a bare `&str` so a call site cannot pass
/// "some URL that was lying around" by accident: it comes from the
/// request made by the client that will fetch the link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkBase(String);

impl LinkBase {
    /// The address THIS request came in on: the right base whenever
    /// the client that asked is the client that will fetch.
    pub fn for_request(headers: &axum::http::HeaderMap) -> Result<Self, String> {
        Self::from_request_host(weft_core::net::request_base_url(headers))
    }

    /// The choice `for_request` makes, over plain values.
    fn from_request_host(requested: Option<String>) -> Result<Self, String> {
        let base = requested.ok_or_else(|| "cannot construct a link: the request has no valid Host (or X-Forwarded-Host / X-Forwarded-Prefix) header".to_string())?;
        Ok(Self(base.trim_end_matches('/').to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
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

/// Terminate-sweep an execution's un-kept exec files: the broker reaps crashed
/// uploads now and stamps completed files with the post-run linger expiry.
async fn sweep_exec(state: &DispatcherState, tenant: &str, execution_id: &str) -> Result<SweepExecResponse> {
    post_admin(
        state,
        "/v1/storage/admin/sweep-exec",
        "sweep-exec",
        &SweepExecRequest { tenant: tenant.to_string(), execution_id: execution_id.to_string() },
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
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
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

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "storage_sweep",
    tables: &["storage_sweep"],
    ddl: &[r#"
        -- Durable terminate-sweep queue: a row per ended run that stored
        -- files of its own, whose un-kept exec files still need sweeping.
        -- Inserted by the write that ends the run (`weft_record_batch`,
        -- `weft_journal::record::append_locked_in`), deleted by the sweep
        -- reaper once the broker confirmed the sweep.
        CREATE TABLE IF NOT EXISTS storage_sweep (
            execution_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            enqueued_at_unix BIGINT NOT NULL
        );
        "#,
        // Wake the sweep reaper when an execution is queued, through the
        // announcement outbox like every other wake (`weft_task_store::announce`):
        // sent once the write commits, one wake for every row a flush
        // takes, since the reaper reads the whole queue.
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
            FOR EACH ROW
            EXECUTE FUNCTION storage_sweep_notify()"#,
    ],
    seed: &[],
};

/// Sweep-queue reaper: ask the broker to sweep each pending execution's un-kept
/// exec files. A row is removed only after the broker confirmed; a TRANSIENT
/// broker error (unreachable, 5xx) leaves the row, and the answer asks for
/// another look after `SAFETY_POLL_INTERVAL` rather than waiting for the next
/// write (the sweep is idempotent). A TERMINAL refusal (a 4xx: the broker understood and
/// rejected the request) is loud + dead-lettered: retrying the identical
/// request every tick forever would be a silent infinite loop over a row the
/// user can neither see nor clear, so the row is dropped with an error log
/// naming the execution (the files, if any, remain reclaimable via `weft files`).
pub async fn process_sweep_queue(state: DispatcherState) -> Result<DrainStep> {
    let rows: Vec<(weft_core::ExecutionId, String)> =
        sqlx::query_as("SELECT execution_id, tenant_id FROM storage_sweep ORDER BY enqueued_at_unix")
            .fetch_all(&state.pg_pool)
            .await?;
    let mut deferred = false;
    for (execution_id, tenant) in rows {
        match sweep_exec(&state, &tenant, &execution_id.to_string()).await {
            Ok(out) => {
                if out.swept > 0 || out.lingering > 0 {
                    tracing::info!(
                        target: "weft_dispatcher::storage",
                        %execution_id, tenant = %tenant, swept = out.swept, lingering = out.lingering,
                        "terminate sweep: reaped crashed uploads, stamped completed \
                         un-kept exec files with the post-run linger expiry"
                    );
                }
                sqlx::query("DELETE FROM storage_sweep WHERE execution_id = $1")
                    .bind(execution_id)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) if e.downcast_ref::<StorageNotFound>().is_some() => {
                // 404 from the broker means there was nothing to sweep for this
                // execution (its files are already gone). That is terminal SUCCESS, not
                // a rejection: drop the row quietly (debug, not error).
                tracing::debug!(
                    target: "weft_dispatcher::storage",
                    %execution_id, tenant = %tenant,
                    "terminate sweep found nothing to remove; clearing the queue row"
                );
                sqlx::query("DELETE FROM storage_sweep WHERE execution_id = $1")
                    .bind(execution_id)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) if e.downcast_ref::<BrokerRejected>().is_some() => {
                // The broker understood and permanently refuses this request (bad
                // shape / denied / over quota). Retrying it every tick forever would
                // be a silent infinite loop over a row nobody can clear, so drop it
                // with a loud error naming the execution (any files stay reclaimable via
                // `weft files`).
                tracing::error!(
                    target: "weft_dispatcher::storage",
                    %execution_id, tenant = %tenant, error = format!("{e:#}"),
                    "terminate sweep REJECTED by the broker; dropping the queue row \
                     (any remaining files for this execution stay listable/deletable via \
                     the storage API)"
                );
                sqlx::query("DELETE FROM storage_sweep WHERE execution_id = $1")
                    .bind(execution_id)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) => {
                // Transient (broker unreachable, apiserver blip surfaced as 401,
                // 5xx): keep the row and look again soon.
                tracing::warn!(
                    target: "weft_dispatcher::storage",
                    %execution_id, tenant = %tenant, error = %e,
                    "terminate sweep deferred (transient broker/control-plane fault); will retry"
                );
                deferred = true;
            }
        }
    }
    Ok(if deferred { DrainStep::RetryIn(SAFETY_POLL_INTERVAL) } else { DrainStep::Done })
}

#[cfg(test)]
mod link_base_tests {
    use super::LinkBase;

    /// The caller's own address wins, whichever of the install's
    /// addresses they arrived on, and a trailing slash never doubles
    /// up in the link.
    #[test]
    fn a_link_is_built_on_the_address_the_caller_used() {
        for host in ["http://127.0.0.1:14112", "https://weft-dev-copper-lantern.weavemind.ai"] {
            let base = LinkBase::from_request_host(Some(host.to_string())).unwrap();
            assert_eq!(base.as_str(), host);
        }
        assert_eq!(
            LinkBase::from_request_host(Some("https://a.example.com/".into())).unwrap().as_str(),
            "https://a.example.com"
        );
    }

    #[test]
    fn a_request_with_no_host_cannot_mint_a_link() {
        assert!(LinkBase::from_request_host(None).unwrap_err().contains("Host"));
    }
}
