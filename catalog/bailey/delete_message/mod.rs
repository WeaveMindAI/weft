//! BaileyDeleteMessage: POSTs a `deleteMessage` action to the
//! project's WhatsApp bridge (a revoke of the account's own message;
//! WhatsApp only lets a sender revoke its own). Pure Fire-phase.

use async_trait::async_trait;

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
        let endpoint_url: String = ctx.inputs.get("endpointUrl")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let message_id: String = ctx.inputs.get("messageId")?;

        super::bridge_api::action(
            &ctx,
            &endpoint_url,
            "deleteMessage",
            serde_json::json!({ "chatId": chat_id, "messageId": message_id }),
        )
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
