//! XReceive Trigger Node - polls X (Twitter) API for new posts matching a query.
//!
//! Uses GET /2/tweets/search/recent with since_id tracking to only fire
//! on new posts.

use async_trait::async_trait;

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerHandle,
    TriggerStartConfig, FieldDef,
};
use crate::{register_node, NodeResult};

pub struct XReceiveNode;

#[async_trait]
impl Node for XReceiveNode {
    fn node_type(&self) -> &'static str {
        "XReceive"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "X Receive",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
            ],
            outputs: vec![
                PortDef::new("text", "String", false),
                PortDef::new("authorUsername", "String", false),
                PortDef::new("authorName", "String", false),
                PortDef::new("authorId", "String", false),
                PortDef::new("postId", "String", false),
                PortDef::new("conversationId", "String", false),
                PortDef::new("createdAt", "String", false),
                PortDef::new("isReply", "Boolean", false),
                PortDef::new("isRetweet", "Boolean", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Polling),
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("query"),
                FieldDef::number("pollIntervalSecs"),
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
        let bearer_token = config.require_str("bearerToken")
            .map_err(|_| TriggerError::Config("Missing 'bearerToken' in config. Connect an XConfig node.".to_string()))?;
        let query = config.require_str("query")
            .map_err(|_| TriggerError::Config("Missing 'query' in config. Set a search query (e.g. @mybot, #mytag).".to_string()))?;
        // Minimum 10 seconds to stay within rate limits
        let poll_interval_secs = config.get_u64("pollIntervalSecs").unwrap_or(30).max(10);
        let poll_interval = std::time::Duration::from_secs(poll_interval_secs);

        ctx.spawn(&config, TriggerCategory::Polling, move |emit, shutdown| async move {
            let client = reqwest::Client::new();
            let mut since_id: Option<String> = None;
            let mut first_poll = true;

            tokio::pin!(shutdown);

            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = tokio::time::sleep(poll_interval) => {
                        let mut url = format!(
                            "https://api.x.com/2/tweets/search/recent?query={}&max_results=10&tweet.fields=author_id,conversation_id,created_at,referenced_tweets&expansions=author_id&user.fields=username,name",
                            urlencoding::encode(&query)
                        );
                        if let Some(ref sid) = since_id {
                            url.push_str(&format!("&since_id={}", sid));
                        }

                        let resp = match client.get(&url)
                            .header("Authorization", format!("Bearer {}", bearer_token))
                            .timeout(std::time::Duration::from_secs(15))
                            .send()
                            .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::warn!("X request failed: {}", e);
                                continue;
                            }
                        };

                        if !resp.status().is_success() {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            tracing::warn!("X API error: {} - {}", status, body);
                            continue;
                        }

                        let body: serde_json::Value = match resp.json().await {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::warn!("X parse error: {}", e);
                                continue;
                            }
                        };

                        if let Some(newest) = body.get("meta")
                            .and_then(|m| m.get("newest_id"))
                            .and_then(|v| v.as_str())
                        {
                            since_id = Some(newest.to_string());
                        }

                        if first_poll {
                            first_poll = false;
                            continue;
                        }

                        // Build user lookup map from includes
                        let users: std::collections::HashMap<String, (String, String)> = body.get("includes")
                            .and_then(|inc| inc.get("users"))
                            .and_then(|u| u.as_array())
                            .map(|arr| {
                                arr.iter().filter_map(|u| {
                                    let id = u.get("id").and_then(|v| v.as_str())?.to_string();
                                    let username = u.get("username").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    let name = u.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    Some((id, (username, name)))
                                }).collect()
                            })
                            .unwrap_or_default();

                        let tweets = body.get("data")
                            .and_then(|d| d.as_array())
                            .cloned()
                            .unwrap_or_default();

                        for tweet in tweets.iter().rev() {
                            let post_id = tweet.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let text = tweet.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let author_id = tweet.get("author_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let conversation_id = tweet.get("conversation_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let created_at = tweet.get("created_at").and_then(|v| v.as_str()).unwrap_or("").to_string();

                            let (author_username, author_name) = users.get(&author_id)
                                .cloned()
                                .unwrap_or_default();

                            let referenced = tweet.get("referenced_tweets").and_then(|r| r.as_array());
                            let is_reply = referenced.as_ref()
                                .map(|refs| refs.iter().any(|r| r.get("type").and_then(|t| t.as_str()) == Some("replied_to")))
                                .unwrap_or(false);
                            let is_retweet = referenced.as_ref()
                                .map(|refs| refs.iter().any(|r| r.get("type").and_then(|t| t.as_str()) == Some("retweeted")))
                                .unwrap_or(false);

                            emit.emit(serde_json::json!({
                                "text": text,
                                "authorUsername": author_username,
                                "authorName": author_name,
                                "authorId": author_id,
                                "postId": post_id,
                                "conversationId": conversation_id,
                                "createdAt": created_at,
                                "isReply": is_reply,
                                "isRetweet": is_retweet,
                            }))?;
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

register_node!(XReceiveNode);
