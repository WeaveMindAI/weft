//! FileSize: read a media/File input's bytes and emit their count. A
//! project-local custom node used by the e2e rig to prove the `@asset`
//! config path end to end: an `@asset("path"|"url", File)` source line
//! resolves (via the pre-build asset sync and the compile) into a concrete
//! file value on this node's `file` config, and at run time the node
//! consumes it as a normal media input, reads the bytes behind the value's
//! handle (bucket key or URL), and reports how many there were. The rig
//! asserts the count equals the payload it wrote, closing the loop.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::NodeOutput;
use weft_core::storage::StorageScope;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct FileSizeNode;

#[async_trait]
impl Node for FileSizeNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The file value the resolved `@asset` config produced arrives on the
        // `file` port exactly like a wired media value; `get_bytes` reads the
        // bytes behind its handle (bucket key or URL) via the storage handle.
        let file = ctx.input.raw("file").cloned().ok_or_else(|| {
            weft_core::WeftError::NodeExecution("FileSize: no `file` input present".into())
        })?;
        let (_meta, bytes) = ctx.storage(StorageScope::Project).get_bytes(&file).await?;
        ctx.pulse_downstream(
            NodeOutput::empty().set("size", Value::from(bytes.len() as u64)),
        )
        .await
    }
}
