//! GoogleDriveDownload: pull a Drive file into storage. A binary file
//! streams via `alt=media`; a Google-native file (Doc, Sheet, Slides)
//! has no bytes and is EXPORTED: Docs/Slides as PDF, Sheets as CSV,
//! overridable through the export format input.

use async_trait::async_trait;
use serde_json::Value;

use weft::access::client::required_str;
use weft::node::NodeOutput;
use weft::storage::{KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::drive;

/// Default export mime per Google-native kind.
fn default_export(native: &str) -> Option<(&'static str, &'static str)> {
    match native {
        "application/vnd.google-apps.document" => Some(("application/pdf", "pdf")),
        "application/vnd.google-apps.spreadsheet" => Some(("text/csv", "csv")),
        "application/vnd.google-apps.presentation" => Some(("application/pdf", "pdf")),
        _ => None,
    }
}

#[derive(NodeManifest)]
pub struct GoogleDriveDownloadNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleDriveDownloadNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let file_id: String = ctx.inputs.get("fileId")?;
        let export_mime: Option<String> = ctx.inputs.opt("exportFormat")?;
        let keep: bool = ctx.inputs.get("keep")?;

        let http = ctx.client(&account).await?;
        let meta: Value = drive::file_meta(
            &http,
            &file_id,
            "name,mimeType",
            "google drive: read file metadata",
        )
        .await?;
        let name = required_str(&meta, "file metadata", "name")?.to_string();
        let native_mime = required_str(&meta, "file metadata", "mimeType")?.to_string();

        let (url, stored_mime, filename) = if native_mime.starts_with("application/vnd.google-apps.")
        {
            let (mime, ext) = match export_mime.as_deref().filter(|m| !m.trim().is_empty()) {
                Some(m) => (m.to_string(), "bin"),
                None => match default_export(&native_mime) {
                    Some((m, e)) => (m.to_string(), e),
                    None => weft::node_bail!(
                        "'{native_mime}' has no default export; set the export format input"
                    ),
                },
            };
            (
                format!(
                    "{}/files/{file_id}/export?mimeType={}",
                    drive::API,
                    urlencoding::encode(&mime)
                ),
                mime,
                format!("{name}.{ext}"),
            )
        } else {
            (format!("{}/files/{file_id}?alt=media", drive::API), native_mime, name)
        };

        let resp = http.get(&url).send().await.node_err("google drive: download")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            .put_response(
                resp,
                "google drive: download the file",
                Some(&stored_mime),
                &filename,
                keep.then_some(KeepTtl::Default),
            )
            .await?;
        ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
    }
}
