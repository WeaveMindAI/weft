//! ImageDisplay: a terminal preview sink that shows a stored image
//! inline in the graph. Like Debug, it emits nothing; the graph view
//! reads the input value off the SSE stream and, seeing a stored-image
//! reference (`features.showImagePreview`), fetches the bytes through
//! the SAME authenticated handshake a user download uses and renders
//! them in place. The input port is typed `Image`, so the type system
//! guarantees the value is an image; the node body only has to check
//! that it is a STORED reference (a url/data-backed image has no key
//! the preview handshake can resolve), failing loud otherwise.

use async_trait::async_trait;

use weft_core::storage::StoredFile;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftError, WeftResult};

pub struct ImageDisplayNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for ImageDisplayNode {
    fn node_type(&self) -> &'static str {
        "ImageDisplay"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("ImageDisplay metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let image = ctx.input.raw("image").cloned().ok_or_else(|| {
            WeftError::Input("ImageDisplay: no value on input port 'image'".into())
        })?;
        // The `Image` port type already guarantees this is an image; the
        // only thing left to enforce is that it is a STORED reference (the
        // editor preview resolves its key through the download handshake).
        // A url/data-backed image has no key to resolve; fail loud.
        StoredFile::from_value(&image).map_err(|e| {
            WeftError::Input(format!(
                "ImageDisplay needs a stored image reference (e.g. from FetchToStorage): {e}"
            ))
        })?;
        Ok(())
    }
}
