//! DownloadLink: a terminal sink that surfaces a download button for a
//! stored file in the graph. Like Debug/ImageDisplay it emits nothing;
//! the graph view reads the input off the SSE stream and, seeing a
//! stored-file reference (`features.showDownloadLink`), renders the
//! filename, size, and a Download button that runs the SAME
//! authenticated handshake a CLI/user download uses. The body only
//! validates that a stored reference arrived so a bad wiring fails
//! loudly.

use async_trait::async_trait;

use weft_core::storage::StoredFile;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct DownloadLinkNode;

#[async_trait]
impl Node for DownloadLinkNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let file = ctx.input.raw("file").cloned().ok_or_else(|| {
            WeftError::Input("DownloadLink: no value on input port 'file'".into())
        })?;
        StoredFile::from_value(&file).map_err(|e| {
            WeftError::Input(format!(
                "DownloadLink needs a stored file reference (e.g. from FetchToStorage): {e}"
            ))
        })?;
        Ok(())
    }
}
