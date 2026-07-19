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
use weft_core::{ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct DownloadLinkNode;

#[async_trait]
impl Node for DownloadLinkNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let file = ctx.input.raw("file").cloned().ok_or_else(|| {
            WeftError::Input("DownloadLink: no value on input port 'file'".into())
        })?;
        FileHandle::from_value(&file).map_err(|e| {
            WeftError::Input(format!(
                "DownloadLink needs a file with a storage key or URL (e.g. from FetchToStorage): {e}"
            ))
        })?;
        Ok(())
    }
}
