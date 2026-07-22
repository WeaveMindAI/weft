//! DownloadLink: a terminal sink that surfaces a download button for a
//! file in the graph. Like Debug/ImageDisplay it emits nothing; the
//! graph view reads the input off the SSE stream and, seeing a file
//! value (`features.showDownloadLink`), renders the filename, size, and
//! an action button. A key-backed file downloads through the SAME
//! authenticated handshake a CLI/user download uses; a url-backed file
//! links straight to its external URL. The body only validates that the
//! value carries a resolvable handle (key or url) so a bad wiring fails
//! loudly.

use async_trait::async_trait;

use weft_core::storage::FileHandle;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct DownloadLinkNode;

#[async_trait]
impl Node for DownloadLinkNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Validate the value carries a resolvable handle (key or url),
        // which is exactly what parsing into a FileHandle checks.
        let _handle: FileHandle = ctx.ports.get("file")?;
        Ok(())
    }
}
