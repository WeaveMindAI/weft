//! Discord Gateway trigger node - connects to Discord via WebSocket
//! and fires events when messages are received.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerHandle,
    TriggerStartConfig, FieldDef,
};
use crate::{register_node, NodeResult};

const DISCORD_GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[allow(non_snake_case)]
struct DiscordConfig {
    #[serde(default)]
    pub botToken: String,
    #[serde(default)]
    pub guildId: Option<String>,
    #[serde(default)]
    pub channelIds: Option<Vec<String>>,
    #[serde(default)]
    pub eventTypes: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GatewayPayload {
    op: u8,
    d: Option<serde_json::Value>,
    s: Option<u64>,
    t: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IdentifyPayload {
    token: String,
    intents: u64,
    properties: IdentifyProperties,
}

#[derive(Debug, Serialize, Deserialize)]
struct IdentifyProperties {
    os: String,
    browser: String,
    device: String,
}

pub struct DiscordNode;

#[async_trait]
impl Node for DiscordNode {
    fn node_type(&self) -> &'static str {
        "DiscordReceive"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Discord",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
            ],
            outputs: vec![
                PortDef::new("content", "String", false),
                PortDef::new("authorName", "String", false),
                PortDef::new("authorId", "String", false),
                PortDef::new("channelId", "String", false),
                PortDef::new("guildId", "String", false),
                PortDef::new("messageId", "String", false),
                PortDef::new("timestamp", "String", false),
                PortDef::new("isBot", "Boolean", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Socket),
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("guildId"),
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
        let discord_config: DiscordConfig = config.parse_config()?;

        if discord_config.botToken.is_empty() {
            return Err(TriggerError::Config("Discord bot token is required".to_string()));
        }

        // Establish initial connection before spawning (fail fast on bad credentials)
        let (ws_stream, _) = connect_async(DISCORD_GATEWAY_URL)
            .await
            .map_err(|e| TriggerError::Connection(format!("Failed to connect to Discord: {}", e)))?;

        let (mut write, mut read) = ws_stream.split();

        let first_msg = read.next().await
            .ok_or_else(|| TriggerError::Connection("No hello from Discord".to_string()))?
            .map_err(|e| TriggerError::Connection(e.to_string()))?;

        let hello: GatewayPayload = match first_msg {
            Message::Text(text) => serde_json::from_str(&text)
                .map_err(|e| TriggerError::Config(format!("Failed to parse hello: {}", e)))?,
            _ => return Err(TriggerError::Config("Expected text message".to_string())),
        };

        if hello.op != 10 {
            return Err(TriggerError::Connection("Expected Hello opcode".to_string()));
        }

        let heartbeat_interval = hello.d
            .as_ref()
            .and_then(|d| d.get("heartbeat_interval"))
            .and_then(|v| v.as_u64())
            .unwrap_or(45000);

        // GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT | DIRECT_MESSAGES
        let intents: u64 = 1 | 512 | 32768 | 4096;

        let identify = GatewayPayload {
            op: 2,
            d: Some(serde_json::to_value(IdentifyPayload {
                token: discord_config.botToken.clone(),
                intents,
                properties: IdentifyProperties {
                    os: "linux".to_string(),
                    browser: "weavemind".to_string(),
                    device: "weavemind".to_string(),
                },
            }).unwrap()),
            s: None,
            t: None,
        };

        write.send(Message::Text(serde_json::to_string(&identify).unwrap()))
            .await
            .map_err(|e| TriggerError::Connection(e.to_string()))?;

        ctx.spawn(&config, TriggerCategory::Socket, move |emit, shutdown| async move {
            let sequence = Arc::new(Mutex::new(0u64));
            let (ws_tx, mut ws_rx) = tokio_mpsc::unbounded_channel::<String>();

            // Heartbeat task
            let seq_for_heartbeat = sequence.clone();
            let ws_tx_for_heartbeat = ws_tx.clone();
            let heartbeat_task = tokio::spawn(async move {
                let jitter = rand::random::<u64>() % heartbeat_interval;
                tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;

                let mut interval = tokio::time::interval(
                    std::time::Duration::from_millis(heartbeat_interval)
                );
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    interval.tick().await;
                    let seq = *seq_for_heartbeat.lock().await;
                    let heartbeat = GatewayPayload {
                        op: 1,
                        d: if seq > 0 { Some(serde_json::json!(seq)) } else { None },
                        s: None,
                        t: None,
                    };
                    if ws_tx_for_heartbeat.send(serde_json::to_string(&heartbeat).unwrap()).is_err() {
                        break;
                    }
                }
            });

            tokio::pin!(shutdown);

            loop {
                tokio::select! {
                    biased;

                    _ = &mut shutdown => {
                        heartbeat_task.abort();
                        let _ = write.close().await;
                        break;
                    }

                    Some(msg) = ws_rx.recv() => {
                        if let Err(e) = write.send(Message::Text(msg)).await {
                            tracing::warn!("Discord failed to send: {}", e);
                            heartbeat_task.abort();
                            break;
                        }
                    }

                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                let payload: GatewayPayload = match serde_json::from_str(&text) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        tracing::warn!("Failed to parse Discord message: {}", e);
                                        continue;
                                    }
                                };

                                if let Some(s) = payload.s {
                                    *sequence.lock().await = s;
                                }

                                match payload.op {
                                    0 => {
                                        if let (Some(event_type), Some(data)) = (payload.t, payload.d) {
                                            if event_type == "MESSAGE_CREATE" {
                                                let author = data.get("author").cloned().unwrap_or_default();
                                                let is_bot = author.get("bot")
                                                    .and_then(|v| v.as_bool())
                                                    .unwrap_or(false);

                                                if is_bot { continue; }

                                                emit.emit(serde_json::json!({
                                                    "content": data.get("content").and_then(|v| v.as_str()).unwrap_or(""),
                                                    "authorName": author.get("username").and_then(|v| v.as_str()).unwrap_or(""),
                                                    "authorId": author.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                                                    "channelId": data.get("channel_id").and_then(|v| v.as_str()).unwrap_or(""),
                                                    "guildId": data.get("guild_id").and_then(|v| v.as_str()).unwrap_or(""),
                                                    "messageId": data.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                                                    "timestamp": data.get("timestamp").and_then(|v| v.as_str()).unwrap_or(""),
                                                    "isBot": false,
                                                }))?;
                                            }
                                        }
                                    }
                                    1 => {
                                        // Heartbeat request from Discord
                                        let seq = *sequence.lock().await;
                                        let heartbeat = GatewayPayload {
                                            op: 1,
                                            d: if seq > 0 { Some(serde_json::json!(seq)) } else { None },
                                            s: None,
                                            t: None,
                                        };
                                        if ws_tx.send(serde_json::to_string(&heartbeat).unwrap()).is_err() {
                                            break;
                                        }
                                    }
                                    7 | 9 => {
                                        // Reconnect or invalid session
                                        heartbeat_task.abort();
                                        break;
                                    }
                                    11 => {} // Heartbeat ACK
                                    _ => {}
                                }
                            }
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                heartbeat_task.abort();
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

register_node!(DiscordNode);
