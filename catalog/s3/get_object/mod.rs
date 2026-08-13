//! S3GetObject: one authenticated GET, streamed into storage. The
//! node addresses the stand-in base `https://storage/...`; the
//! access's BaseUrl step aims it at the stored endpoint and the SigV4
//! step signs the request, all behind the client.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::{KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::s3;

#[derive(NodeManifest)]
pub struct S3GetObjectNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for S3GetObjectNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let bucket: String = ctx.inputs.get("bucket")?;
        let key: String = ctx.inputs.get("key")?;
        let keep: bool = ctx.inputs.get("keep")?;

        let s3 = ctx.client(&access).await?;
        let resp = s3
            .get(s3::object_url(&bucket, &key)?)
            .send()
            .await
            .node_err("s3: get object")?;
        // The one genuinely local bit: an object key has no served
        // filename, so the last path segment stands in.
        let filename = key.rsplit('/').next().unwrap_or(&key).to_string();
        let stored = ctx
            .storage(StorageScope::Execution)
            .put_response(
                resp,
                "s3: read the object",
                None,
                &filename,
                keep.then_some(KeepTtl::Default),
            )
            .await?;
        ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
    }
}
