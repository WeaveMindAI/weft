//! WhatsAppReceive Trigger Node, fires when a WhatsApp message arrives.
//!
//! During trigger setup (isTriggerSetup=true), execute() resolves the bridge
//! endpoint URL from upstream WhatsAppBridge infra, validates reachability,
//! and returns the bridgeEndpointUrl in its output.
//!
//! keep_alive() opens an SSE connection to the sidecar's /events endpoint
//! and fires TriggerEvents through the event_sender channel. This avoids
//! the sidecar needing to POST back to the API (which fails from inside k8s).
//!
//! During normal execution, extracts the message data from the trigger payload.

use async_trait::async_trait;

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerEvent, TriggerHandle,
    TriggerStartConfig,
};
use crate::{register_node, NodeResult};

pub struct WhatsAppReceiveNode;

#[async_trait]
impl Node for WhatsAppReceiveNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppReceive"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "WhatsApp Receive",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
            ],
            outputs: vec![
                PortDef::new("content", "String", false),
                PortDef::new("from", "String", false),
                PortDef::new("pushName", "String", false),
                PortDef::new("messageId", "String", false),
                PortDef::new("timestamp", "String", false),
                PortDef::new("isGroup", "Boolean", false),
                PortDef::new("chatId", "String", false),
                PortDef::new("audio", "Audio", false),
                PortDef::new("image", "Image", false),
                PortDef::new("video", "Video", false),
                PortDef::new("document", "Document", false),
                PortDef::new("messageType", "String", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Webhook),
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        tracing::info!(
            "[WhatsAppReceive] execute: isTriggerSetup={}, isInfraSetup={}, executionId={}, input_keys={:?}",
            ctx.isTriggerSetup, ctx.isInfraSetup, ctx.executionId,
            ctx.input.as_object().map(|o| o.keys().collect::<Vec<_>>())
        );
        if ctx.isTriggerSetup {
            // Trigger setup: resolve bridge URL, validate reachability, return config.
            // keep_alive() will open an SSE connection to receive events.
            let bridge_url = match ctx.input.get("endpointUrl").and_then(|v| v.as_str()) {
                Some(url) if !url.is_empty() => url.to_string(),
                _ => return NodeResult::failed("Missing endpointUrl. Connect the endpointUrl output of a WhatsAppBridge node."),
            };

            // Validate bridge is reachable via health check
            let health_url = bridge_url.trim_end_matches('/').replace("/action", "/health");
            let health_url = if health_url == bridge_url {
                format!("{}/health", bridge_url.trim_end_matches('/'))
            } else {
                health_url
            };

            let client = reqwest::Client::new();
            match client.get(&health_url)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    tracing::info!("[WhatsAppReceive] Bridge reachable at {}", bridge_url);
                }
                Ok(r) => {
                    return NodeResult::failed(&format!("Bridge health check failed: {}", r.status()));
                }
                Err(e) => {
                    return NodeResult::failed(&format!("Failed to reach bridge at {}: {}", bridge_url, e));
                }
            }

            // Return the bridge URL as resolved config, keep_alive will use it
            // to open an SSE connection for receiving events.
            return NodeResult::completed(serde_json::json!({
                "bridgeEndpointUrl": bridge_url,
            }));
        }

        // Normal execution: extract the message data from the trigger payload.
        // The SSE path sends the raw event data directly in TriggerEvent.payload,
        // which becomes triggerPayload in the execution input.
        let payload = ctx.input.get("triggerPayload")
            .cloned()
            .unwrap_or(ctx.input.clone());

        // Support both formats:
        // - SSE path: triggerPayload IS the data directly (from keep_alive)
        // - Webhook path (legacy): { body: { event, data } }
        let data = if payload.get("body").and_then(|b| b.get("data")).is_some() {
            payload.get("body").unwrap().get("data").unwrap().clone()
        } else {
            payload
        };

        let content = data.get("content").and_then(|v| v.as_str());
        let message_type = data.get("messageType").and_then(|v| v.as_str()).unwrap_or("text");
        let msg_id = data.get("messageId").and_then(|v| v.as_str()).unwrap_or("");

        let mut output = serde_json::json!({
            "content": content,
            "audio": serde_json::Value::Null,
            "image": serde_json::Value::Null,
            "video": serde_json::Value::Null,
            "document": serde_json::Value::Null,
            "messageType": message_type,
            "from": data.get("from").and_then(|v| v.as_str()).unwrap_or(""),
            "pushName": data.get("pushName").and_then(|v| v.as_str()).unwrap_or(""),
            "messageId": msg_id,
            "timestamp": data.get("timestamp").cloned().unwrap_or(serde_json::Value::Null),
            "isGroup": data.get("isGroup").and_then(|v| v.as_bool()).unwrap_or(false),
            "chatId": data.get("chatId").and_then(|v| v.as_str()).unwrap_or(""),
        });

        // For media messages, download from sidecar and store as temp media.
        // Stickers are treated as images (they're .webp).
        let output_port = match message_type {
            "image" | "sticker" => Some("image"),
            "video" => Some("video"),
            "audio" => Some("audio"),
            "document" => Some("document"),
            _ => None,
        };
        if let Some(port) = output_port {
            if !msg_id.is_empty() {
            // Bridge URL: stored in config by trigger setup, or in input during manual runs
            let bridge_url = ctx.config.get("bridgeEndpointUrl")
                .or_else(|| ctx.input.get("endpointUrl"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if !bridge_url.is_empty() {
                // Build sidecar media URL: replace /action with /media/:id, or append
                let base = bridge_url.trim_end_matches('/');
                let media_url = if base.ends_with("/action") {
                    format!("{}/media/{}", &base[..base.len() - "/action".len()], msg_id)
                } else {
                    format!("{}/media/{}", base, msg_id)
                };

                // Filename is for display only; actual mime type comes from sidecar response headers
                let filename = format!("{}_{}", message_type, msg_id);

                match ctx.store_temp_media(&media_url, "", &filename).await {
                    Ok(media_obj) => {
                        output[port] = media_obj;
                    }
                    Err(e) => {
                        tracing::warn!("[WhatsAppReceive] Failed to store {} media: {}", message_type, e);
                        // Continue without media, content/caption still available
                    }
                }
            } else {
                tracing::warn!("[WhatsAppReceive] No bridge URL available for media download");
            }
            }
        }

        NodeResult::completed(output)
    }

    async fn keep_alive(
        &self,
        config: TriggerStartConfig,
        ctx: TriggerContext,
    ) -> Result<TriggerHandle, TriggerError> {
        let event_sender = ctx.event_sender;
        let trigger_id = config.id.clone();
        let project_id = config.projectId.clone();
        let trigger_node_id = config.triggerNodeId.clone();

        let bridge_url = config.config
            .get("bridgeEndpointUrl")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if bridge_url.is_empty() {
            return Err(TriggerError::Config("Missing bridgeEndpointUrl in trigger config".to_string()));
        }

        // Build SSE URL: replace /action suffix with /events, or append /events
        let base = bridge_url.trim_end_matches('/');
        let events_url = if base.ends_with("/action") {
            format!("{}/events", &base[..base.len() - "/action".len()])
        } else {
            format!("{}/events", base)
        };

        tracing::info!(
            "[WhatsAppReceive] Opening SSE connection to {} for trigger {}",
            events_url, trigger_id
        );

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

        // Spawn the SSE listener task
        let tid = trigger_id.clone();
        let wid = project_id.clone();
        let tnid = trigger_node_id.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut backoff = 1u64;
            #[allow(unused_assignments)]
            loop {
                let resp = match client.get(&events_url)
                    .header("Accept", "text/event-stream")
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => {
                        tracing::info!("[WhatsAppReceive] SSE connected to {}", events_url);
                        backoff = 1;
                        r
                    }
                    Ok(r) => {
                        tracing::warn!("[WhatsAppReceive] SSE returned {}, retrying in {}s", r.status(), backoff);
                        tokio::select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs(backoff)) => {}
                            _ = &mut shutdown_rx => {
                                tracing::info!("[WhatsAppReceive] Shutdown during SSE backoff");
                                return;
                            }
                        }
                        backoff = (backoff * 2).min(60);
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!("[WhatsAppReceive] SSE connect failed: {}, retrying in {}s", e, backoff);
                        tokio::select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs(backoff)) => {}
                            _ = &mut shutdown_rx => {
                                tracing::info!("[WhatsAppReceive] Shutdown during SSE backoff");
                                return;
                            }
                        }
                        backoff = (backoff * 2).min(60);
                        continue;
                    }
                };

                // Read the SSE stream chunk by chunk
                let mut stream = resp.bytes_stream();
                use futures_util::StreamExt;
                let mut buffer = String::new();

                loop {
                    tokio::select! {
                        chunk = stream.next() => {
                            match chunk {
                                Some(Ok(bytes)) => {
                                    buffer.push_str(&String::from_utf8_lossy(&bytes));
                                    // Process complete SSE messages (double newline delimited)
                                    while let Some(pos) = buffer.find("\n\n") {
                                        let msg = buffer[..pos].to_string();
                                        buffer = buffer[pos + 2..].to_string();

                                        // Parse SSE: lines starting with "data: "
                                        for line in msg.lines() {
                                            if let Some(json_str) = line.strip_prefix("data: ") {
                                                match serde_json::from_str::<serde_json::Value>(json_str) {
                                                    Ok(evt) => {
                                                        let event_type = evt.get("event")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("");
                                                        if event_type == "message.received" {
                                                            let data = evt.get("data").cloned()
                                                                .unwrap_or(serde_json::Value::Null);

                                                            // Skip messages with no usable content.
                                                            // Media messages are forwarded even without text, the node
                                                            // downloads the media via the sidecar /media/:messageId endpoint.
                                                            let content = data.get("content").and_then(|v| v.as_str()).unwrap_or("");
                                                            let audio = data.get("audio").and_then(|v| v.as_str()).unwrap_or("");
                                                            let message_type = data.get("messageType").and_then(|v| v.as_str()).unwrap_or("text");
                                                            let is_media = matches!(message_type, "image" | "video" | "document" | "audio" | "sticker");
                                                            if content.is_empty() && audio.is_empty() && !is_media {
                                                                tracing::debug!(
                                                                    "[WhatsAppReceive] Skipping empty message from {}",
                                                                    data.get("from").and_then(|v| v.as_str()).unwrap_or("unknown")
                                                                );
                                                                continue;
                                                            }

                                                            tracing::info!(
                                                                "[WhatsAppReceive] SSE message from {}",
                                                                data.get("from").and_then(|v| v.as_str()).unwrap_or("unknown")
                                                            );
                                                            let trigger_event = TriggerEvent {
                                                                triggerId: tid.clone(),
                                                                projectId: wid.clone(),
                                                                triggerNodeId: tnid.clone(),
                                                                payload: data,
                                                                timestamp: chrono::Utc::now(),
                                                            };
                                                            if event_sender.send(trigger_event).is_err() {
                                                                tracing::error!("[WhatsAppReceive] Event channel closed");
                                                                return;
                                                            }
                                                        }
                                                    }
                                                    Err(_) => {} // Skip non-JSON lines (e.g., comments)
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Err(e)) => {
                                    tracing::warn!("[WhatsAppReceive] SSE stream error: {}, reconnecting", e);
                                    break;
                                }
                                None => {
                                    tracing::info!("[WhatsAppReceive] SSE stream ended, reconnecting");
                                    break;
                                }
                            }
                        }
                        _ = &mut shutdown_rx => {
                            tracing::info!("[WhatsAppReceive] Shutdown signal received, closing SSE");
                            return;
                        }
                    }
                }

                // Stream ended or errored, reconnect with backoff
                backoff = 1;
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                    _ = &mut shutdown_rx => {
                        tracing::info!("[WhatsAppReceive] Shutdown during reconnect");
                        return;
                    }
                }
            }
        });

        Ok(TriggerHandle::new(
            trigger_id,
            project_id,
            TriggerCategory::Webhook,
            shutdown_tx,
        ))
    }
}

register_node!(WhatsAppReceiveNode);
