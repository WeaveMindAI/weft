//! SlackReceive Trigger Node - connects to Slack via Socket Mode WebSocket
//! and fires events when messages are received.
//!
//! Socket Mode flow:
//! 1. POST apps.connections.open with app-level token to get WSS URL
//! 2. Connect to WebSocket
//! 3. Receive events wrapped in envelopes
//! 4. Acknowledge each event by sending back { envelope_id }
//! 5. Handle disconnects and reconnects

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerHandle,
    TriggerStartConfig, FieldDef,
};
use crate::{register_node, NodeResult};

pub struct SlackReceiveNode;

async fn obtain_wss_url(app_token: &str) -> Result<String, TriggerError> {
    let client = reqwest::Client::new();
    let resp = client
        .post("https://slack.com/api/apps.connections.open")
        .header("Authorization", format!("Bearer {}", app_token))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await
        .map_err(|e| TriggerError::Connection(format!("Failed to call apps.connections.open: {}", e)))?;

    let body: serde_json::Value = resp.json().await
        .map_err(|e| TriggerError::Connection(format!("Failed to parse connections.open response: {}", e)))?;

    if !body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
        let error = body.get("error").and_then(|v| v.as_str()).unwrap_or("unknown");
        return Err(TriggerError::Config(format!("Slack apps.connections.open failed: {}", error)));
    }

    body.get("url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| TriggerError::Connection("No URL in apps.connections.open response".to_string()))
}

#[async_trait]
impl Node for SlackReceiveNode {
    fn node_type(&self) -> &'static str {
        "SlackReceive"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Slack Receive",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
            ],
            outputs: vec![
                PortDef::new("text", "String", false),
                PortDef::new("userId", "String", false),
                PortDef::new("channelId", "String", false),
                PortDef::new("channelType", "String", false),
                PortDef::new("timestamp", "String", false),
                PortDef::new("threadTs", "String", false),
                PortDef::new("teamId", "String", false),
                PortDef::new("isThread", "Boolean", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Socket),
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("channelId"),
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
        let app_token = config.require_str("appToken")
            .map_err(|_| TriggerError::Config("Missing 'appToken' in config. Connect a SlackConfig node with an App-Level Token (xapp-...).".to_string()))?;
        let channel_id_filter = config.get_str("channelId");

        // Establish initial connection before spawning (fail fast on bad credentials)
        let wss_url = obtain_wss_url(&app_token).await?;
        let (ws_stream, _) = connect_async(&wss_url)
            .await
            .map_err(|e| TriggerError::Connection(format!("Failed to connect to Slack WebSocket: {}", e)))?;

        let (mut write, mut read) = ws_stream.split();

        // Wait for hello message
        let first_msg = read.next().await
            .ok_or_else(|| TriggerError::Connection("No hello from Slack".to_string()))?
            .map_err(|e| TriggerError::Connection(e.to_string()))?;

        if let Message::Text(text) = first_msg {
            let hello: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| TriggerError::Connection(format!("Failed to parse Slack hello: {}", e)))?;
            let msg_type = hello.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if msg_type != "hello" {
                return Err(TriggerError::Connection(format!("Expected 'hello' from Slack, got '{}'", msg_type)));
            }
        }

        ctx.spawn(&config, TriggerCategory::Socket, move |emit, shutdown| async move {
            tokio::pin!(shutdown);

            loop {
                tokio::select! {
                    biased;

                    _ = &mut shutdown => {
                        let _ = write.close().await;
                        break;
                    }

                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                let envelope: serde_json::Value = match serde_json::from_str(&text) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        tracing::warn!("Slack parse error: {}", e);
                                        continue;
                                    }
                                };

                                let msg_type = envelope.get("type").and_then(|v| v.as_str()).unwrap_or("");

                                // Handle disconnect: reconnect
                                if msg_type == "disconnect" {
                                    let reason = envelope.get("reason").and_then(|v| v.as_str()).unwrap_or("unknown");
                                    tracing::info!("Slack disconnect: {}", reason);

                                    match obtain_wss_url(&app_token).await {
                                        Ok(new_url) => {
                                            match connect_async(&new_url).await {
                                                Ok((new_ws, _)) => {
                                                    let (new_write, new_read) = new_ws.split();
                                                    write = new_write;
                                                    read = new_read;
                                                    tracing::info!("Slack reconnected");
                                                    continue;
                                                }
                                                Err(e) => {
                                                    tracing::error!("Slack reconnect failed: {}", e);
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Slack failed to get new WSS URL: {}", e);
                                            break;
                                        }
                                    }
                                }

                                // Acknowledge the envelope
                                if let Some(envelope_id) = envelope.get("envelope_id").and_then(|v| v.as_str()) {
                                    let ack = serde_json::json!({ "envelope_id": envelope_id });
                                    if let Err(e) = write.send(Message::Text(ack.to_string())).await {
                                        tracing::warn!("Slack failed to ack: {}", e);
                                    }
                                }

                                if msg_type != "events_api" { continue; }

                                let payload = match envelope.get("payload") {
                                    Some(p) => p,
                                    None => continue,
                                };

                                let event = match payload.get("event") {
                                    Some(e) => e,
                                    None => continue,
                                };

                                let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
                                if event_type != "message" { continue; }

                                // Skip bot messages and message subtypes (edits, deletes, etc.)
                                if event.get("bot_id").is_some() || event.get("subtype").is_some() {
                                    continue;
                                }

                                let channel_id = event.get("channel").and_then(|v| v.as_str()).unwrap_or("").to_string();

                                if let Some(ref filter) = channel_id_filter {
                                    if channel_id != *filter { continue; }
                                }

                                let text_content = event.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let user_id = event.get("user").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let channel_type = event.get("channel_type").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let ts = event.get("ts").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let thread_ts = event.get("thread_ts").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let team_id = payload.get("team_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let is_thread = !thread_ts.is_empty();

                                emit.emit(serde_json::json!({
                                    "text": text_content,
                                    "userId": user_id,
                                    "channelId": channel_id,
                                    "channelType": channel_type,
                                    "timestamp": ts,
                                    "threadTs": thread_ts,
                                    "teamId": team_id,
                                    "isThread": is_thread,
                                }))?;
                            }
                            Some(Ok(Message::Close(frame))) => {
                                if let Some(cf) = frame {
                                    tracing::info!("Slack WebSocket closed: code={}, reason={}", cf.code, cf.reason);
                                }
                                break;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Slack WebSocket error: {}", e);
                                break;
                            }
                            None => break,
                            _ => {}
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

register_node!(SlackReceiveNode);
