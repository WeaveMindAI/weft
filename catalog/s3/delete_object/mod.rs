//! S3DeleteObject: one authenticated DELETE.

use async_trait::async_trait;

use weft::access::client::checked_send;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

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
        checked_send(s3.delete(s3::object_url(&bucket, &key)?), "delete the object").await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
