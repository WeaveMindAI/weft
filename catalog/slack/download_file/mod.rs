//! SlackDownloadFile: pull a Slack-hosted file (an F... id, e.g. from
//! the file-shared trigger) into execution storage and emit the
//! stored-file reference. `files.info` names the private download URL;
//! fetching it needs the connection's bearer, so the authenticated
//! client streams it and storage takes the body chunk by chunk.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackDownloadFileNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackDownloadFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let file_id: String = ctx.inputs.get("fileId")?;
        let client = ctx.client(&access).await?;

        let info = api::get(&client, "files.info", &[("file", file_id.clone())]).await?;
        let url = info
            .pointer("/file/url_private")
            .and_then(Value::as_str)
            .node_err("slack: files.info carries no url_private")?
            .to_string();
        let name = info
            .pointer("/file/name")
            .and_then(Value::as_str)
            .unwrap_or("slack-file")
            .to_string();
        let mime = info
            .pointer("/file/mimetype")
            .and_then(Value::as_str)
            .unwrap_or("application/octet-stream")
            .to_string();

        // url_private only answers with the workspace bearer, so this
        // fetch runs on the connection's client and streams into
        // storage (bounded memory, whatever the file size).
        let resp = client.get(&url).send().await.node_err("slack: fetch file bytes")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            .put_response(
                resp,
                "slack: download the file",
                Some(&mime),
                &name,
                // Swept with the run like every node's file; KeepFile keeps it.
                None,
            )
            .await?;
        ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
    }
}
