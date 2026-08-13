//! Shared Drive plumbing: the API base, the metadata read every
//! file-addressed node opens with, and the create-answer shape the
//! file-producing nodes (copy, upload) share.

use serde_json::{json, Value};

use weft::access::client::{get_json, required_str};
use weft::node::NodeOutput;
use weft::WeftResult;

pub const API: &str = "https://www.googleapis.com/drive/v3";

/// The one-shot upload endpoint (metadata + bytes in one request)
/// lives under its own base.
pub const UPLOAD_API: &str = "https://www.googleapis.com/upload/drive/v3";

/// Read a file's metadata, asking for exactly `fields`. Shared drives
/// included: a watched or wired file id is a file id wherever it
/// lives.
pub async fn file_meta(
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    file_id: &str,
    fields: &str,
    what: &str,
) -> WeftResult<Value> {
    get_json(
        http,
        &format!("{API}/files/{file_id}?fields={fields}&supportsAllDrives=true"),
        what,
    )
    .await
}

/// The `{name, parents}` metadata body of a file-creating call (copy,
/// upload): a blank name/folder input means "let Drive decide", the
/// same skip-blank rule every optional text input follows.
pub fn file_metadata(name: Option<String>, folder: Option<String>) -> Value {
    let mut body = json!({});
    if let Some(n) = name.filter(|n| !n.trim().is_empty()) {
        body["name"] = json!(n);
    }
    if let Some(f) = folder.filter(|f| !f.trim().is_empty()) {
        body["parents"] = json!([f]);
    }
    body
}

/// The output every file-creating node emits (`fileId` + `link`), read
/// loudly off a `?fields=id,webViewLink` answer.
pub fn created_file_output(answer: &Value, what: &str) -> WeftResult<NodeOutput> {
    Ok(NodeOutput::new()
        .set("fileId", required_str(answer, what, "id")?.to_string())
        .set("link", required_str(answer, what, "webViewLink")?.to_string()))
}
