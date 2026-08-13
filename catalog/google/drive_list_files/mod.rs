//! GoogleDriveListFiles: one authenticated GET; the store refreshed
//! the token lazily if it was near expiry (rotating any single-use
//! refresh token) before the client was handed over.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GoogleDriveListFilesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleDriveListFilesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get_or("query", String::new())?;
        // `pageSize` declares a metadata default, so the bag always
        // holds a value.
        let page_size: f64 = ctx.inputs.get("pageSize")?;

        let drive = ctx.client(&access).await?;
        // Drive may answer FEWER than pageSize per page even when more
        // match, so honoring the cap takes paging until it fills (or
        // the listing ends), never trusting one page.
        let wanted = page_size as usize;
        let mut base = format!(
            "{}/files?pageSize={}&fields=nextPageToken,files(id,name,mimeType)",
            super::drive::API,
            page_size as u64,
        );
        if !query.trim().is_empty() {
            base.push_str(&format!("&q={}", urlencoding::encode(query.trim())));
        }
        let mut files =
            super::api::paged(&drive, &base, "files", "list the files", |got| {
                got.len() >= wanted
            })
            .await?;
        files.truncate(wanted);
        ctx.pulse_downstream(NodeOutput::new().set("files", Value::Array(files))).await
    }
}
