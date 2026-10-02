//! BaileyMarkRead: POSTs a `readMessages` action to the project's
//! WhatsApp bridge (the read receipt, blue ticks on the sender's
//! side). Pure Fire-phase; no infra lifecycle of its own.

use async_trait::async_trait;

use weft::infra::InfraHandle;
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyMarkReadNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyMarkReadNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bridge: InfraHandle = ctx.inputs.get("bridge")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let message_id: String = ctx.inputs.get("messageId")?;

        ctx.endpoint_of(&bridge)
            .await?
            .action("readMessages", serde_json::json!({ "chatId": chat_id, "messageId": message_id }))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
