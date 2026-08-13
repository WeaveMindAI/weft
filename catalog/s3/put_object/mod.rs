//! S3PutObject: one authenticated PUT. The node addresses the
//! stand-in base `https://storage/...`; the access's BaseUrl step aims
//! it at the stored endpoint and the SigV4 step signs the final
//! request (body hash included), all behind the client.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::s3;

#[derive(NodeManifest)]
pub struct S3PutObjectNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for S3PutObjectNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let bucket: String = ctx.inputs.get("bucket")?;
        let key: String = ctx.inputs.get("key")?;
        let content: String = ctx.inputs.get("content")?;

        let s3 = ctx.client(&access).await?;
        let resp = s3
            .put(s3::object_url(&bucket, &key)?)
            .body(content)
            .send()
            .await
            .node_err("s3: put object")?;
        let resp = s3::ok_or_bail(resp, "the upload").await?;
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
            .node_err("s3: the store answered without an ETag")?;
        ctx.pulse_downstream(NodeOutput::new().set("etag", etag)).await
    }
}
