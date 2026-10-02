//! TelegramSendMessage: one authenticated POST. The client rewrites
//! the URL to `/bot<token>/sendMessage` (the service's PathPrefix
//! step); the body addresses the API generically and never sees the
//! token.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

/// One link button, as the `buttons` port's type declares it.
#[derive(serde::Deserialize)]
struct LinkButton {
    label: String,
    url: String,
}

#[derive(NodeManifest)]
pub struct TelegramSendMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for TelegramSendMessageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let text: String = ctx.inputs.get("text")?;
        let parse_mode: Option<String> = ctx.inputs.opt("parseMode")?;
        let buttons: Vec<LinkButton> = ctx.inputs.opt("buttons")?.unwrap_or_default();
        // Read as an integer directly: a fractional id is a loud type
        // error, never a silent truncation.
        let reply_to: Option<i64> = ctx.inputs.opt("replyTo")?;

        let mut body = serde_json::json!({ "chat_id": chat_id, "text": text });
        if let Some(m) = parse_mode.filter(|m| !m.trim().is_empty() && m != "plain") {
            body["parse_mode"] = serde_json::json!(m);
        }
        if let Some(r) = reply_to {
            body["reply_parameters"] = serde_json::json!({ "message_id": r });
        }
        // Link buttons under the message, one per row; the port's type
        // holds each to a {label, url}. An empty list means no keyboard.
        if !buttons.is_empty() {
            let rows: Vec<Value> = buttons
                .iter()
                .map(|b| serde_json::json!([{ "text": b.label, "url": b.url }]))
                .collect();
            body["reply_markup"] = serde_json::json!({ "inline_keyboard": rows });
        }

        let answer = api::call(&ctx.client(&access).await?, "sendMessage", |req| req.json(&body)).await?;
        let message_id = api::result_message_id(&answer, "sendMessage")?;
        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}
