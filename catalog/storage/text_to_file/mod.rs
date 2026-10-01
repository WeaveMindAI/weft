//! TextToFile: store a string as a file and emit its reference. The
//! way text too big for a wire (100 KB) moves between nodes, and the way
//! a node that takes a file gets one made of text.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::StoredFile;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifetime;

#[derive(NodeManifest)]
pub struct TextToFileNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for TextToFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let text: String = ctx.inputs.get("text")?;
        // Both declare metadata defaults, so the bag always holds them.
        let filename: String = ctx.inputs.get("filename")?;
        let mime_type: String = ctx.inputs.get("mimeType")?;
        let scope = lifetime::scope_named(&ctx.inputs.get::<String>("scope")?)?;
        // No default on purpose: absent is the scope's own lifetime (an
        // execution file goes with its run, a project file stays).
        let ttl_days: Option<u64> = ctx.inputs.opt("ttl_days")?;
        let file = ctx
            .storage(scope)
            .put(text.into_bytes(), &mime_type, &filename, ttl_days.map(lifetime::ttl_of_days))
            .await?;
        ctx.pulse_downstream(NodeOutput::stored_file(StoredFile::from_value(&file)?)).await
    }
}
