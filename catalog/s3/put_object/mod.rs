//! S3PutObject: one authenticated PUT. The node addresses the
//! stand-in base `https://storage/...`; the access's BaseUrl step aims
//! it at the stored endpoint and the SigV4 step signs the final
//! request (body hash included), all behind the client.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct S3PutObjectNode;

#[async_trait]
impl Node for S3PutObjectNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let bucket: String = ctx.inputs.get("bucket")?;
        let key: String = ctx.inputs.get("key")?;
        let content: String = ctx.inputs.get("content")?;

        let s3 = ctx.client(&access).await?;
        let resp = s3
            .put(format!("https://storage/{bucket}/{key}"))
            .body(content)
            .send()
            .await
            .node_err("s3: put object")?;
        let status = resp.status();
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            weft::node_bail!(
                "the store answered {status} on the upload: {}",
                weft::truncate_user_string(&body, 300)
            );
        }
        let etag = etag.node_err("s3: the store answered without an ETag")?;
        ctx.pulse_downstream(NodeOutput::new().set("etag", etag)).await
    }
}
