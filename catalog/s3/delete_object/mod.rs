//! S3DeleteObject: one authenticated DELETE.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::s3;

#[derive(NodeManifest)]
pub struct S3DeleteObjectNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for S3DeleteObjectNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let bucket: String = ctx.inputs.get("bucket")?;
        let key: String = ctx.inputs.get("key")?;

        let s3 = ctx.client(&access).await?;
        let resp = s3
            .delete(s3::object_url(&bucket, &key)?)
            .send()
            .await
            .node_err("s3: delete object")?;
        s3::ok_or_bail(resp, "the delete").await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
