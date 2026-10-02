//! BaileyReact: POSTs a `sendReaction` action to the project's
//! WhatsApp bridge. Pure Fire-phase; no infra lifecycle of its own.

use async_trait::async_trait;

use weft::infra::InfraHandle;
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyReactNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyReactNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bridge: InfraHandle = ctx.inputs.get("bridge")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let message_id: String = ctx.inputs.get("messageId")?;
        let emoji: String = ctx.inputs.get("emoji")?;

        ctx.endpoint_of(&bridge)
            .await?
            .action("sendReaction", serde_json::json!({ "chatId": chat_id, "messageId": message_id, "emoji": emoji }))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
