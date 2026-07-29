//! TelegramSendMessage: one authenticated POST. The client rewrites
//! the URL to `/bot<token>/sendMessage` (the service's PathPrefix
//! step); the body addresses the API generically and never sees the
//! token.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct TelegramSendMessageNode;

#[async_trait]
impl Node for TelegramSendMessageNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let text: String = ctx.inputs.get("text")?;

        let tg = ctx.client(&access).await?;
        let resp = tg
            .post("https://api.telegram.org/sendMessage")
            .json(&serde_json::json!({ "chat_id": chat_id, "text": text }))
            .send()
            .await
            .node_err("telegram: send message")?;
        let answer: Value = resp.json().await.node_err("telegram: read send response")?;
        if answer.get("ok").and_then(Value::as_bool) != Some(true) {
            weft::node_bail!(
                "telegram refused the send: {}",
                answer.get("description").and_then(Value::as_str).unwrap_or("no detail")
            );
        }
        let message_id = answer
            .pointer("/result/message_id")
            .and_then(Value::as_i64)
            .node_err("telegram: sent message carries no message_id")?;
        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}
