//! SlackUploadFile: share a stored file into a channel (optionally a
//! thread) through Slack's external-upload flow, the only upload path
//! Slack still supports: mint an upload URL for the exact byte length
//! (`files.getUploadURLExternal`), POST the bytes there, then finish
//! with `files.completeUploadExternal`, which does the actual share.
//!
//! The upload URL POST is a bare pre-signed endpoint, not a Web API
//! method: no `ok` envelope, plain HTTP status semantics, and it rides
//! the PLAIN client: the URL is pre-signed precisely so no credential
//! is needed, and the workspace token never travels to a host named by
//! a response field.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackUploadFileNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackUploadFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let file: FileHandle = ctx.inputs.get("file")?;
        let channel: String = ctx.inputs.get("channel")?;
        let thread_ts: Option<String> = ctx.inputs.opt("threadTs")?;
        let title: Option<String> = ctx.inputs.opt("title")?;
        let comment: Option<String> = ctx.inputs.opt("comment")?;

        // The upload URL is minted for an exact length, so the bytes are
        // collected first. Slack caps uploads at 1 GB; a workflow file
        // that size is out of scope for a chat share.
        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&file).await?;
        // Two distinct facts: the WIRE filename (owns the extension
        // Slack derives the preview and download name from) is the
        // stored file's real name; `title` is only the display name on
        // the finished share.
        let display_title = title.clone().unwrap_or_else(|| meta.filename.clone());

        let minted = api::get(
            &ctx,
            &access,
            "files.getUploadURLExternal",
            &[
                ("filename", meta.filename.clone()),
                ("length", bytes.len().to_string()),
            ],
        )
        .await?;
        let upload_url = api::required_str(&minted, "files.getUploadURLExternal", "upload_url")?;
        let file_id = api::required_str(&minted, "files.getUploadURLExternal", "file_id")?.to_string();

        let resp = ctx
            .http()
            .post(upload_url)
            .body(bytes)
            .send()
            .await
            .node_err("slack: upload bytes")?;
        if !resp.status().is_success() {
            weft::node_bail!("slack's upload endpoint answered {}", resp.status());
        }

        let mut complete = json!({
            "files": [{ "id": file_id, "title": display_title }],
            "channel_id": channel,
        });
        if let Some(t) = thread_ts {
            complete["thread_ts"] = json!(t);
        }
        if let Some(c) = comment {
            complete["initial_comment"] = json!(c);
        }
        let answer =
            api::call(&ctx, &access, "files.completeUploadExternal", complete).await?;
        // The metadata promises "a link to the shared file": a
        // completed share answering without one is a broken contract,
        // not an empty string downstream posts as a dead link.
        let permalink = answer
            .pointer("/files/0/permalink")
            .and_then(Value::as_str)
            .node_err("slack: completeUploadExternal answered without the file's permalink")?
            .to_string();

        ctx.pulse_downstream(
            NodeOutput::new().set("fileId", file_id).set("permalink", permalink),
        )
        .await
    }
}
