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
use weft::storage::{KeepTtl, StorageScope, StoredFile};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("url")?;
        // `keep` declares a metadata default, so the bag always holds a
        // value; a required read keeps the default in ONE place.
        let keep: bool = ctx.inputs.get("keep")?;
        let filename: Option<String> = ctx.inputs.opt("filename")?;
        let scope_name: String = ctx.inputs.get("scope")?;
        let identity: Option<String> = ctx.inputs.opt("identity")?.filter(|s: &String| !s.is_empty());
        let scope = match scope_name.as_str() {
            "execution" => StorageScope::Execution,
            "project" => StorageScope::Project,
            other => weft::node_bail!("`scope` is '{other}'; it is `execution` or `project`"),
        };
        // `keep` is a run-scoped file's way of outliving the run; a
        // project file is persistent already, so the pair is a
        // contradiction the author should see rather than a flag
        // quietly dropped.
        if keep && scope == StorageScope::Project {
            weft::node_bail!(
                "`keep` is on with `scope: project`; a project file already outlives the run, \
                 so drop `keep` or use `scope: execution`"
            );
        }
        let keep_ttl = keep.then_some(KeepTtl::Default);

        // The whole fetch-stream-into-storage path is a language
        // capability: ctx GETs the URL, derives the mime, streams the
        // body in (bounded memory), and returns the stored-file
        // reference. An identity makes it ask the store first.
        let mut storage = ctx.storage(scope);
        if let Some(identity) = identity {
            storage = storage.identified(identity);
        }
        let file = storage.put_from_url(&url, filename.as_deref(), keep_ttl).await?;

        let stored = StoredFile::from_value(&file)?;
        let out = NodeOutput::new()
            .set("file", file)
            .set("sizeBytes", stored.size_bytes)
            .set("mimeType", stored.mime_type);
        ctx.pulse_downstream(out).await
    }
}
