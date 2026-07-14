//! FetchToStorage: download a URL straight into execution storage and
//! emit a stored-file reference. The HTTP response body streams into
//! storage chunk by chunk (never buffered whole), so a multi-gigabyte
//! file is handled in bounded memory. The reference is what flows
//! downstream; the bytes only move again when a node reads or presigns.

use async_trait::async_trait;

use weft_core::node::NodeOutput;
use weft_core::storage::{KeepTtl, StorageScope, StoredFile};
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct FetchToStorageNode;

#[async_trait]
impl Node for FetchToStorageNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url = ctx.input.required_str("url", "url")?;
        let keep: bool = ctx.config.get("keep").unwrap_or(false);
        let filename = ctx.input.get_optional::<String>("filename")?;
        let keep_ttl = keep.then_some(KeepTtl::Default);

        // The whole fetch-stream-into-storage path is a language
        // capability: ctx GETs the URL, derives the mime, streams the
        // body in (bounded memory), and returns the stored-file reference.
        let file = ctx
            .storage(StorageScope::Execution)
            .put_from_url(&url, filename.as_deref(), keep_ttl)
            .await?;

        let stored = StoredFile::from_value(&file)?;
        let out = NodeOutput::with("file", file)
            .set("sizeBytes", serde_json::json!(stored.size_bytes))
            .set("mimeType", serde_json::json!(stored.mime_type));
        ctx.pulse_downstream(out).await
    }
}
