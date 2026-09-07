//! BaileyFetchMedia: pull one message's media out of the bridge by
//! its id. The history fetch carries a media message's id and type
//! but never its bytes (a voice bot's history would otherwise download
//! every clip on every read); this node is the on-demand half, the
//! same `/media/<messageId>` path the live receive uses.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::StoredFile;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyFetchMediaNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyFetchMediaNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let endpoint_url: String = ctx.inputs.get("endpointUrl")?;
        let message_id: String = ctx.inputs.get("messageId")?;
        let stored = super::bridge_api::fetch_media(&ctx, &endpoint_url, &message_id).await?;
        ctx.pulse_downstream(NodeOutput::stored_file(StoredFile::from_value(&stored)?)).await
    }
}
