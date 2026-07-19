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
use weft_core::{ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct ImageDisplayNode;

#[async_trait]
impl Node for ImageDisplayNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let image = ctx.input.raw("image").cloned().ok_or_else(|| {
            WeftError::Input("ImageDisplay: no value on input port 'image'".into())
        })?;
        // The `Image` port type already guarantees this is an image; the
        // only thing left to enforce is that it carries a handle the
        // preview can resolve (a storage key or an external URL).
        FileHandle::from_value(&image).map_err(|e| {
            WeftError::Input(format!(
                "ImageDisplay needs an image with a storage key or URL (e.g. from FetchToStorage): {e}"
            ))
        })?;
        Ok(())
    }
}
