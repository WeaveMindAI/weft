//! TelegramSendMessage: one authenticated POST. The client rewrites
//! the URL to `/bot<token>/sendMessage` (the service's PathPrefix
//! step); the body addresses the API generically and never sees the
//! token.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

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
        let buttons: Option<Value> = ctx.inputs.opt("buttons")?;
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
        // Link buttons under the message: a list of {label, url} rows.
        if let Some(Value::Array(entries)) = buttons {
            let rows: Vec<Value> = entries
                .iter()
                .filter_map(|b| {
                    let label = b["label"].as_str()?;
                    let url = b["url"].as_str()?;
                    Some(serde_json::json!([{ "text": label, "url": url }]))
                })
                .collect();
            if rows.len() != entries.len() {
                weft::node_bail!("every button needs a label and a url");
            }
            // An empty list simply means no keyboard.
            if !rows.is_empty() {
                body["reply_markup"] = serde_json::json!({ "inline_keyboard": rows });
            }
        }

        let answer = api::call(&ctx, &access, "sendMessage", body).await?;
        let message_id = api::result_message_id(&answer, "sendMessage")?;
        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}
