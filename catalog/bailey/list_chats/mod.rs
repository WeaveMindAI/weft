//! BaileyListChats: POSTs a `getChats` action to the project's
//! WhatsApp bridge and emits the participating group chats. Pure
//! Fire-phase.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyListChatsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyListChatsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let endpoint_url: String = ctx.inputs.get("endpointUrl")?;
        let result =
            super::bridge_api::action(&ctx, &endpoint_url, "getChats", serde_json::json!({}))
                .await?;
        let chats = result["chats"]
            .as_array()
            .node_err(format!("bridge getChats response missing result.chats: {result}"))?
            .clone();
        let count = chats.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("chats", chats).set("count", count)).await
    }
}
