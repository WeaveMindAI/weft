//! BaileyDeleteMessage: POSTs a `deleteMessage` action to the
//! project's WhatsApp bridge (a revoke of the account's own message;
//! WhatsApp only lets a sender revoke its own). Pure Fire-phase.

use async_trait::async_trait;

use weft::infra::InfraHandle;
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyDeleteMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyDeleteMessageNode {
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
            .action("deleteMessage", serde_json::json!({ "chatId": chat_id, "messageId": message_id }))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
