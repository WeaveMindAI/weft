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
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("url")?;
        // `keep` declares a metadata default, so the bag always holds a
        // value; a required read keeps the default in ONE place.
        let keep: bool = ctx.inputs.get("keep")?;
        let filename: Option<String> = ctx.inputs.opt("filename")?;
        let keep_ttl = keep.then_some(KeepTtl::Default);

        // The whole fetch-stream-into-storage path is a language
        // capability: ctx GETs the URL, derives the mime, streams the
        // body in (bounded memory), and returns the stored-file reference.
        let file = ctx
            .storage(StorageScope::Execution)
            .put_from_url(&url, filename.as_deref(), keep_ttl)
            .await?;

        let stored = StoredFile::from_value(&file)?;
        let out = NodeOutput::new()
            .set("file", file)
            .set("sizeBytes", stored.size_bytes)
            .set("mimeType", stored.mime_type);
        ctx.pulse_downstream(out).await
    }
}
