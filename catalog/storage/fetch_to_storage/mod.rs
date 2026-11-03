//! FetchToStorage: download a URL straight into storage and emit a
//! stored-file reference. The HTTP response body streams into storage
//! chunk by chunk (never buffered whole), so a multi-gigabyte file is
//! handled in bounded memory. The reference is what flows downstream;
//! the bytes only move again when a node reads or presigns.
//!
//! With `scope: project` and an `identity`, the fetch is once per
//! project: the storage service answers a second fetch of the same
//! identity with the file it holds, and nothing is downloaded.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::StoredFile;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::lifetime;

#[derive(NodeManifest)]
pub struct FetchToStorageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FetchToStorageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("url")?;
        let filename: Option<String> = ctx.inputs.opt("filename")?;
        let scope = ctx.inputs.get::<lifetime::Scope>("scope")?;
        let identity: Option<String> = ctx.inputs.opt("identity")?;
        // No default on purpose, as on every storage node: absent is the
        // scope's own lifetime (a run file goes with its run, a
        // project file stays).
        let ttl_days: Option<u64> = ctx.inputs.opt("ttlDays")?;
        let keep_ttl = ttl_days.map(lifetime::ttl_of_days);

        // The whole fetch-stream-into-storage path is a language
        // capability: ctx GETs the URL, derives the mime, streams the
        // body in (bounded memory), and returns the stored-file
        // reference. An identity makes it ask the store first.
        let mut storage = ctx.storage(scope.into());
        if let Some(identity) = identity {
            storage = storage.identified(identity);
        }
        let file = storage.put_from_url(&url, None, filename.as_deref(), keep_ttl).await?;

        ctx.pulse_downstream(NodeOutput::stored_file(StoredFile::from_value(&file)?)).await
    }
}
