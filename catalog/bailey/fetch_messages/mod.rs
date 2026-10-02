//! BaileyFetchMessages: POSTs a `fetchMessages` action to the
//! project's WhatsApp bridge and emits a chat's recent history (the
//! bridge waits on / requests history sync itself). Pure Fire-phase.

use async_trait::async_trait;

use weft::infra::InfraHandle;
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyFetchMessagesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyFetchMessagesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bridge: InfraHandle = ctx.inputs.get("bridge")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let count: f64 = ctx.inputs.get("count")?;

        let result = ctx.endpoint_of(&bridge)
            .await?
            .action("fetchMessages", serde_json::json!({ "chatId": chat_id, "count": count }))
            .await?;
        let messages = result["messages"]
            .as_array()
            .node_err(format!("bridge fetchMessages response missing result.messages: {result}"))?
            .clone();
        let count = messages.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("messages", messages).set("count", count))
            .await
    }
}
