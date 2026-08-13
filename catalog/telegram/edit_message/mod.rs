//! TelegramEditMessage: rewrite a message the bot sent earlier
//! (addressed by chat + message id, both from Send Message).

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct TelegramEditMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for TelegramEditMessageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        // Read as an integer directly: a fractional id is a loud type
        // error, never a silent truncation.
        let message_id: i64 = ctx.inputs.get("messageId")?;
        let text: String = ctx.inputs.get("text")?;

        api::call(
            &ctx,
            &access,
            "editMessageText",
            serde_json::json!({
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
            }),
        )
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
