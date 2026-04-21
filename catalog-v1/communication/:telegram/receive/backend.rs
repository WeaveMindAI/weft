//! TelegramReceive Trigger Node - polls Telegram Bot API for new messages.
//!
//! Uses getUpdates with long polling and offset tracking.

use async_trait::async_trait;

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerHandle,
    TriggerStartConfig, FieldDef,
};
use crate::{register_node, NodeResult};

pub struct TelegramReceiveNode;

#[async_trait]
impl Node for TelegramReceiveNode {
    fn node_type(&self) -> &'static str {
        "TelegramReceive"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Receive",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
            ],
            outputs: vec![
                PortDef::new("text", "String", false),
                PortDef::new("chatId", "String", false),
                PortDef::new("chatTitle", "String", false),
                PortDef::new("chatType", "String", false),
                PortDef::new("fromUsername", "String", false),
                PortDef::new("fromFirstName", "String", false),
                PortDef::new("fromId", "String", false),
                PortDef::new("messageId", "String", false),
                PortDef::new("date", "String", false),
                PortDef::new("isReply", "Boolean", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Polling),
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("chatId"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        if ctx.isTriggerSetup {
            return NodeResult::completed(ctx.input.clone());
        }
        let payload = ctx.input.get("triggerPayload")
            .cloned()
            .unwrap_or(ctx.input.clone());
        NodeResult::completed(payload)
    }

    async fn keep_alive(&self,
        config: TriggerStartConfig,
        ctx: TriggerContext,
    ) -> Result<TriggerHandle, TriggerError> {
        let bot_token = config.require_str("botToken")
            .map_err(|_| TriggerError::Config("Missing 'botToken' in config. Connect a TelegramConfig node.".to_string()))?;
        let chat_id_filter = config.get_str("chatId");

        ctx.spawn(&config, TriggerCategory::Polling, move |emit, shutdown| async move {
            let client = reqwest::Client::new();
            let base_url = format!("https://api.telegram.org/bot{}", bot_token);
            let mut offset: Option<i64> = None;

            tokio::pin!(shutdown);

            loop {
                let mut url = format!(
                    "{}/getUpdates?timeout=30&allowed_updates=[\"message\"]",
                    base_url
                );
                if let Some(off) = offset {
                    url.push_str(&format!("&offset={}", off));
                }

                tokio::select! {
                    _ = &mut shutdown => break,
                    result = client.get(&url)
                        .timeout(std::time::Duration::from_secs(40))
                        .send() => {

                        let resp = match result {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::warn!("Telegram request failed: {}", e);
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        };

                        if !resp.status().is_success() {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            tracing::warn!("Telegram API error: {} - {}", status, body);
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }

                        let body: serde_json::Value = match resp.json().await {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::warn!("Telegram parse error: {}", e);
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        };

                        if !body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                            let desc = body.get("description").and_then(|v| v.as_str()).unwrap_or("unknown error");
                            tracing::warn!("Telegram API returned ok=false: {}", desc);
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }

                        let updates = body.get("result")
                            .and_then(|r| r.as_array())
                            .cloned()
                            .unwrap_or_default();

                        for update in &updates {
                            if let Some(update_id) = update.get("update_id").and_then(|v| v.as_i64()) {
                                offset = Some(update_id + 1);
                            }

                            let message = match update.get("message") {
                                Some(m) => m,
                                None => continue,
                            };

                            let text = match message.get("text").and_then(|v| v.as_str()) {
                                Some(t) => t.to_string(),
                                None => continue,
                            };

                            let chat = message.get("chat").cloned().unwrap_or_default();
                            let chat_id = chat.get("id").and_then(|v| v.as_i64())
                                .map(|id| id.to_string())
                                .unwrap_or_default();

                            if let Some(ref filter) = chat_id_filter {
                                if chat_id != *filter { continue; }
                            }

                            let chat_title = chat.get("title")
                                .or_else(|| chat.get("first_name"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let chat_type = chat.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();

                            let from = message.get("from").cloned().unwrap_or_default();
                            let from_username = from.get("username").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let from_first_name = from.get("first_name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let from_id = from.get("id").and_then(|v| v.as_i64())
                                .map(|id| id.to_string())
                                .unwrap_or_default();

                            let message_id = message.get("message_id").and_then(|v| v.as_i64())
                                .map(|id| id.to_string())
                                .unwrap_or_default();

                            let date = message.get("date").and_then(|v| v.as_i64())
                                .map(|ts| {
                                    chrono::DateTime::from_timestamp(ts, 0)
                                        .map(|dt| dt.to_rfc3339())
                                        .unwrap_or_default()
                                })
                                .unwrap_or_default();

                            let is_reply = message.get("reply_to_message").is_some();

                            emit.emit(serde_json::json!({
                                "text": text,
                                "chatId": chat_id,
                                "chatTitle": chat_title,
                                "chatType": chat_type,
                                "fromUsername": from_username,
                                "fromFirstName": from_first_name,
                                "fromId": from_id,
                                "messageId": message_id,
                                "date": date,
                                "isReply": is_reply,
                            }))?;
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

register_node!(TelegramReceiveNode);
