//! Stored-file assertions: list, download, and check files a program wrote.
//!
//! A program writes files through a storage node; the rig reads them back the
//! way the CLI / web app does:
//!   - list:     `GET /storage/files?project={id}` -> `{ files: [meta...] }`
//!   - download: `POST /storage/files/download { key, project }` -> `{ url }`,
//!               then GET the bytes from that (box-public) URL.
//!
//! File keys are scoped: `exec/<color>/<id>` (execution scratch, swept on
//! terminate unless kept), `project/<project_id>/<id>`, `shared/<name>/<id>`.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::client::Dispatcher;

/// Metadata for one stored file (the fields the rig asserts on). Thin accessor
/// over the JSON; SYNC by name with StoredFileMeta (camelCase on the wire).
#[derive(Debug, Clone)]
pub struct StoredFile(pub Value);

impl StoredFile {
    pub fn key(&self) -> Option<&str> {
        self.0.get("key").and_then(Value::as_str)
    }
    pub fn filename(&self) -> Option<&str> {
        self.0.get("filename").and_then(Value::as_str)
    }
    pub fn mime_type(&self) -> Option<&str> {
        self.0.get("mimeType").and_then(Value::as_str)
    }
    pub fn size_bytes(&self) -> Option<u64> {
        self.0.get("sizeBytes").and_then(Value::as_u64)
    }
    pub fn keep(&self) -> bool {
        self.0.get("keep").and_then(Value::as_bool).unwrap_or(false)
    }
}

/// List every stored file visible to `project_id`'s tenant.
pub async fn list(disp: &Dispatcher, project_id: &Uuid) -> Result<Vec<StoredFile>> {
    let path = format!("/storage/files?project={project_id}");
    let resp: Value = disp.get_json(&path).await?;
    let files = resp
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(files.into_iter().map(StoredFile).collect())
}

/// Find files under a key prefix (e.g. `exec/<color>/` for one run's scratch).
pub async fn list_prefix(
    disp: &Dispatcher,
    project_id: &Uuid,
    prefix: &str,
) -> Result<Vec<StoredFile>> {
    Ok(list(disp, project_id)
        .await?
        .into_iter()
        .filter(|f| f.key().map(|k| k.starts_with(prefix)).unwrap_or(false))
        .collect())
}

/// Download a stored file's bytes by key: handshake for a capability URL, then
/// stream the bytes from the box's public ingress.
///
/// The box is a SCALE-TO-ZERO service behind nginx. The dispatcher's
/// `ensure_box` waits for the box process, but there is still a short window,
/// after a cold wake, where nginx has not yet registered the box's endpoint and
/// the public route answers `503`/`502`/`504` ("no healthy upstream"). That is a
/// transient not-ready state, not a download failure, so we poll through those
/// gateway codes until the box is actually serving (bounded; a user-controlled
/// operation legitimately running long is not the case here, this is internal
/// box-wake readiness). Any OTHER non-success (403 denied, 404 gone) fails fast.
pub async fn download(disp: &Dispatcher, project_id: &Uuid, key: &str) -> Result<Vec<u8>> {
    // Gateway "upstream not ready yet" codes: retry these, fail fast on the rest.
    const NOT_READY: [u16; 3] = [502, 503, 504];
    let deadline = Duration::from_secs(60);
    let interval = Duration::from_millis(500);

    // Mint the capability URL ONCE, not per attempt. Minting is NOT free: it
    // calls the box's `touch_access`, which on a KEPT file rewrites the file's
    // metadata on disk and pushes its TTL forward; re-minting every poll would
    // do that up to ~120 times. A stale URL is never the cause of a
    // gateway-not-ready code (those are nginx-has-no-upstream, independent of
    // the capability), so one mint suffices. Give it a TTL well above the poll
    // deadline so it cannot expire mid-wait. (Same mint-once-then-reuse
    // discipline as `signal.rs`, which avoids littering the token table.)
    let body = json!({
        "key": key,
        "project": project_id.to_string(),
        "ttl_secs": deadline.as_secs() + 600,
    });
    let resp: Value = disp.post_json("/storage/files/download", &body).await?;
    let url = resp
        .get("url")
        .and_then(Value::as_str)
        .with_context(|| format!("download handshake for {key} missing `url`: {resp}"))?
        .to_string();

    // Poll the (single) URL through the box's cold-wake window. A bespoke loop
    // (not `poll_until`) so a timeout surfaces the LAST observation, turning a
    // genuinely-down box from a vague "timed out" into an actionable error.
    //
    // Two flavors of "not ready yet" both retry: (a) an HTTP 502/503/504 (nginx
    // is up but has no healthy box upstream registered), and (b) a TRANSPORT
    // error (connection refused / reset: nginx itself isn't accepting yet,
    // earlier in the cold wake). Both are transient wake states, not download
    // failures, so the loop rides them out. Only a definitive HTTP response
    // (403 denied, 404 gone) fails fast.
    let start = std::time::Instant::now();
    loop {
        // What this attempt observed, for the timeout diagnostic.
        let last = match disp.get_abs_raw(&url).await {
            Ok((status, bytes)) => {
                if status.is_success() {
                    return Ok(bytes);
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
        if start.elapsed() >= deadline {
            bail!(
                "storage box did not serve the download for {key} within {deadline:?}; \
                 last observation was [{last}] from {url} (box never became routable: \
                 is it stuck waking, or genuinely down?)"
            );
        }
        tokio::time::sleep(interval).await;
    }
}

/// Assert a file exists under `prefix` whose bytes equal `expected`. Returns the
/// matched file's key. The common storage check: "the program wrote this".
pub async fn assert_file_contents(
    disp: &Dispatcher,
    project_id: &Uuid,
    prefix: &str,
    expected: &[u8],
) -> Result<String> {
    let files = list_prefix(disp, project_id, prefix).await?;
    if files.is_empty() {
        bail!("no stored files under prefix '{prefix}' for project {project_id}");
    }
    for f in &files {
        let key = f.key().context("stored file missing key")?;
        let bytes = download(disp, project_id, key).await?;
        if bytes == expected {
            return Ok(key.to_string());
        }
    }
    bail!(
        "no file under '{prefix}' matched the expected {} bytes (found {} file(s): {:?})",
        expected.len(),
        files.len(),
        files.iter().filter_map(StoredFile::key).collect::<Vec<_>>()
    )
}
