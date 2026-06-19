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
pub async fn download(disp: &Dispatcher, project_id: &Uuid, key: &str) -> Result<Vec<u8>> {
    let body = json!({ "key": key, "project": project_id.to_string() });
    let resp: Value = disp.post_json("/storage/files/download", &body).await?;
    let url = resp
        .get("url")
        .and_then(Value::as_str)
        .with_context(|| format!("download handshake for {key} missing `url`: {resp}"))?;
    let (status, bytes) = disp.get_abs_raw(url).await?;
    if !status.is_success() {
        bail!(
            "download GET {url} -> HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(bytes)
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
