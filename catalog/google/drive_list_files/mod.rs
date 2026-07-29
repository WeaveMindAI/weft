//! GoogleDriveListFiles: one authenticated GET; the store refreshed
//! the token lazily if it was near expiry (rotating any single-use
//! refresh token) before the client was handed over.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GoogleDriveListFilesNode;

#[async_trait]
impl Node for GoogleDriveListFilesNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get_or("query", String::new())?;
        // `pageSize` declares a metadata default, so the bag always
        // holds a value.
        let page_size: f64 = ctx.inputs.get("pageSize")?;

        let drive = ctx.client(&access).await?;
        let mut req = drive
            .get("https://www.googleapis.com/drive/v3/files")
            .query(&[
                ("pageSize", (page_size as u64).to_string()),
                ("fields", "files(id,name,mimeType)".to_string()),
            ]);
        if !query.trim().is_empty() {
            req = req.query(&[("q", query.trim())]);
        }
        let resp = req.send().await.node_err("google drive: list files")?;
        let status = resp.status();
        let answer: Value = resp.json().await.node_err("google drive: read list response")?;
        if !status.is_success() {
            weft::node_bail!(
                "google drive answered {status} listing files: {}",
                answer.pointer("/error/message").and_then(Value::as_str).unwrap_or("no detail")
            );
        }
        let files = answer.get("files").cloned().unwrap_or(Value::Array(Vec::new()));
        ctx.pulse_downstream(NodeOutput::new().set("files", files)).await
    }
}
