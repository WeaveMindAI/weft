//! Shared Drive plumbing: the API base, the metadata read every
//! file-addressed node opens with, and the create-answer shape the
//! file-producing nodes (copy, upload) share.

use serde_json::{json, Value};

use weft::access::client::{get_json, required_str, url};
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
    let url = url(API, &["files", file_id])?.query("fields", fields).query("supportsAllDrives", true);
    get_json(http, url.as_str(), what).await
}

/// The `{name, parents}` metadata body of a file-creating call (copy,
/// upload): an unset name or folder means "let Drive decide".
pub fn file_metadata(name: Option<String>, folder: Option<String>) -> Value {
    let mut body = json!({});
    if let Some(n) = name {
        body["name"] = json!(n);
    }
    if let Some(f) = folder {
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

// This file is a package-level SHARED helper, not a node, so its unit
// tests stay an ordinary `#[cfg(test)]` block.
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A name and a folder ride the metadata part; an unset one is left
    /// for Drive to decide.
    #[test]
    fn the_metadata_carries_what_was_given() {
        assert_eq!(file_metadata(Some("a.txt".into()), Some("dest".into())), json!({ "name": "a.txt", "parents": ["dest"] }));
        assert_eq!(file_metadata(Some("a.txt".into()), None), json!({ "name": "a.txt" }));
    }
}
