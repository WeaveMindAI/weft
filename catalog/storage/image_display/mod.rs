//! ImageDisplay: a terminal preview sink that shows an image inline in
//! the graph. Like Debug, it emits nothing; the graph view reads the
//! input value off the SSE stream and, seeing an image file value
//! (`features.showImagePreview`), renders it in place. A key-backed
//! image fetches its bytes through the SAME authenticated handshake a
//! user download uses; a url-backed image is rendered straight from its
//! URL by the viewer's browser. The input port is typed `Image`, so the
//! type system guarantees the value is an image; the node body only has
//! to check that it carries a resolvable handle (key or url; an inline
//! data-backed image has neither), failing loud otherwise.

use async_trait::async_trait;

use weft_core::storage::FileHandle;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ImageDisplayNode;

#[async_trait]
impl Node for ImageDisplayNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The `Image` port type already guarantees this is an image; the
        // only thing left to enforce is that it carries a handle the
        // preview can resolve (a storage key or an external URL), which
        // is exactly what parsing into a FileHandle checks.
        let _handle: FileHandle = ctx.ports.get("image")?;
        Ok(())
    }
}
