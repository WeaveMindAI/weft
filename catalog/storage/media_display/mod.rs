//! MediaDisplay: a terminal preview sink that shows media inline in
//! the graph. Like Debug, it emits nothing; the graph view reads the
//! input value off the SSE stream and, per the node's
//! `features.display` declaration, renders it in place (an image in
//! an image tag, audio and video in a real player). A key-backed file
//! fetches its bytes through the SAME authenticated handshake a user
//! download uses; a url-backed file is rendered straight from its URL
//! by the viewer's browser. The input port's media union guarantees
//! the value is displayable media; the node body only has to check
//! that it carries a resolvable handle (key or url; an inline
//! data-backed value has neither), failing loud otherwise.

use async_trait::async_trait;

use weft::storage::FileHandle;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct MediaDisplayNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MediaDisplayNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The media union on the port already guarantees this is
        // displayable media; the only thing left to enforce is that it
        // carries a handle the preview can resolve (a storage key or
        // an external URL), which is exactly what parsing into a
        // FileHandle checks.
        let _handle: FileHandle = ctx.inputs.get("media")?;
        Ok(())
    }
}
