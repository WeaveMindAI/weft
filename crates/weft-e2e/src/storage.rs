//! Stored-file assertions: list, download, and check files a program wrote.
//!
//! A program writes files through a storage node; the rig reads them back the
//! way the CLI / web app does:
//!   - list:     `GET /storage/files` -> `{ files: [meta...] }`
//!   - download: `POST /storage/files/download { key }` -> `{ url }`,
//!               then GET the bytes from that (box-public) URL.
//!
//! File keys are scoped: `exec/<execution_id>/<id>` (execution scratch, swept on
//! terminate unless kept), `project/<project_id>/<id>`, `shared/<name>/<id>`.

use std::time::Duration;

use anyhow::{bail, Result};
use weft_core::storage::{ListFilesResponse, StoredFileMeta};

use crate::client::{poll_until_describing, Dispatcher};

/// Every stored file of the caller's tenant (the dispatcher takes the
/// tenant from the credential).
pub async fn list(disp: &Dispatcher) -> Result<Vec<StoredFileMeta>> {
    let listing: ListFilesResponse = disp.get_json("/storage/files").await?;
    Ok(listing.files)
}

/// Find files under a SCOPE key prefix (e.g. `exec/<execution_id>/` for one run's
/// scratch). Wire keys are tenant-anchored (`<tenant>/<scope>/<owner>/<id>`),
/// but tests think in the scope portion, so match the key with its leading
/// `<tenant>/` segment stripped. Tenant-agnostic, so it works for any
/// tenant, `local` included.
pub async fn list_prefix(disp: &Dispatcher, prefix: &str) -> Result<Vec<StoredFileMeta>> {
    Ok(list(disp)
        .await?
        .into_iter()
        .filter(|f| {
            f.key
                .split_once('/')
                .map(|(_tenant, scope_key)| scope_key.starts_with(prefix))
                .unwrap_or(false)
        })
        .collect())
}

/// Download a stored file's bytes by key: handshake for a presigned bucket URL,
/// then stream the bytes directly from the storage bucket.
///
/// The bucket (SeaweedFS) is a docker container on the host in the local
/// emulation, published on the loopback. A container still settling can
/// transiently answer `502`/`503`/`504` or refuse the connection. That is a
/// not-ready state, not a download failure, so we poll through those codes +
/// transport errors until the bucket is serving (bounded). Any OTHER non-success
/// (403 denied/expired signature, 404 gone) fails fast.
pub async fn download(disp: &Dispatcher, key: &str) -> Result<Vec<u8>> {
    // Gateway "upstream not ready yet" codes: retry these, fail fast on the rest.
    const NOT_READY: [u16; 3] = [502, 503, 504];
    let deadline = Duration::from_secs(60);
    let interval = Duration::from_millis(500);

    // Mint the presigned URL ONCE, not per attempt. The presign counts as an
    // access (it bumps a KEPT file's TTL); re-minting every poll would bump it
    // up to ~120 times. A not-ready bucket response is independent of the URL,
    // so one mint suffices. Give it a TTL well above the poll deadline so the
    // signed URL cannot expire mid-wait.
    let body = weft_core::storage::DownloadRequest { key: key.to_string(), ttl_secs: Some(deadline.as_secs() + 600) };
    let handshake: weft_core::storage::PresignResult =
        disp.post_json("/storage/files/download", &serde_json::to_value(&body)?).await?;
    let url = handshake.url;

    // Poll the (single) URL through the box's cold-wake window; a timeout
    // names the LAST observation, turning a genuinely-down box from a vague
    // "timed out" into an actionable error.
    //
    // Two flavors of "not ready yet" both retry: (a) an HTTP 502/503/504 (nginx
    // is up but has no healthy box upstream registered), and (b) a TRANSPORT
    // error (connection refused / reset: nginx itself isn't accepting yet,
    // earlier in the cold wake). Both are transient wake states, not download
    // failures, so the poll rides them out. Only a definitive HTTP response
    // (403 denied, 404 gone) fails fast.
    let last = std::sync::Mutex::new(String::new());
    poll_until_describing(
        &format!("the storage box to serve the download for {key}"),
        deadline,
        interval,
        || {
            let url = &url;
            let last = &last;
            async move {
                let observed = match disp.get_abs_raw(url).await {
                    Ok((status, bytes)) => {
                        if status.is_success() {
                            return Ok(Some(bytes));
                        }
                        if !NOT_READY.contains(&status.as_u16()) {
                            bail!(
                                "download GET {url} -> HTTP {status}: {}",
                                String::from_utf8_lossy(&bytes)
                            );
                        }
                        format!("HTTP {status}")
                    }
                    // Transport error: nginx not accepting connections yet. Retry.
                    Err(e) => format!("transport error: {e}"),
                };
                *last.lock().unwrap() = observed;
                Ok(None)
            }
        },
        || {
            format!(
                "last observation was [{}] from {url}; the box never became routable: is it \
                 stuck waking, or genuinely down?",
                last.lock().unwrap()
            )
        },
    )
    .await
}

/// Assert a file exists under `prefix` whose bytes equal `expected`. Returns the
/// matched file's key. The common storage check: "the program wrote this".
pub async fn assert_file_contents(
    disp: &Dispatcher,
    prefix: &str,
    expected: &[u8],
) -> Result<String> {
    let files = list_prefix(disp, prefix).await?;
    if files.is_empty() {
        bail!("no stored files under prefix '{prefix}'");
    }
    for f in &files {
        let bytes = download(disp, &f.key).await?;
        if bytes == expected {
            return Ok(f.key.clone());
        }
    }
    bail!(
        "no file under '{prefix}' matched the expected {} bytes (found {} file(s): {:?})",
        expected.len(),
        files.len(),
        files.iter().map(|f| f.key.as_str()).collect::<Vec<_>>()
    )
}
