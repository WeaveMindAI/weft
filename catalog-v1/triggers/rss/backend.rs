//! RSS Feed trigger node - polls RSS/Atom feeds and fires events on new items.

use async_trait::async_trait;
use std::collections::HashSet;

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerHandle,
    TriggerStartConfig, FieldDef,
};
use crate::{register_node, NodeResult};

pub struct RssNode;

#[async_trait]
impl Node for RssNode {
    fn node_type(&self) -> &'static str {
        "Rss"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "RSS Feed",
            inputs: vec![],
            outputs: vec![
                PortDef::new("title", "String", false),
                PortDef::new("link", "String", false),
                PortDef::new("summary", "String", false),
                PortDef::new("content", "String", false),
                PortDef::new("published", "String", false),
                PortDef::new("entryId", "String", false),
                PortDef::new("author", "String", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Polling),
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("url"),
                FieldDef::number("pollIntervalSecs").with_default(serde_json::json!(300)),
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
        let feed_url = config.require_str("url")?;
        let poll_interval_secs = config.get_u64("pollIntervalSecs").unwrap_or(300);
        let poll_interval = std::time::Duration::from_secs(poll_interval_secs);

        ctx.spawn(&config, TriggerCategory::Polling, move |emit, shutdown| async move {
            let client = reqwest::Client::builder()
                .user_agent("WeaveMind/1.0 (RSS Feed Reader)")
                .build()
                .unwrap_or_else(|_| reqwest::Client::new());
            let mut seen_ids: HashSet<String> = HashSet::new();
            let mut first_poll = true;

            tokio::pin!(shutdown);

            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = tokio::time::sleep(poll_interval) => {
                        let response = match client.get(&feed_url).send().await {
                            Ok(r) if r.status().is_success() => r,
                            Ok(r) => {
                                tracing::warn!("RSS got HTTP {}", r.status());
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!("RSS fetch error: {}", e);
                                continue;
                            }
                        };

                        let bytes = match response.bytes().await {
                            Ok(b) => b,
                            Err(e) => { tracing::warn!("RSS read error: {}", e); continue; }
                        };

                        let feed = match feed_rs::parser::parse(&bytes[..]) {
                            Ok(f) => f,
                            Err(e) => { tracing::warn!("RSS parse error: {}", e); continue; }
                        };

                        for entry in feed.entries {
                            let entry_id = entry.id.clone();
                            if !seen_ids.insert(entry_id.clone()) { continue; }
                            if first_poll { continue; }

                            emit.emit(serde_json::json!({
                                "title": entry.title.map(|t| t.content).unwrap_or_default(),
                                "link": entry.links.first().map(|l| l.href.clone()).unwrap_or_default(),
                                "summary": entry.summary.map(|s| s.content).unwrap_or_default(),
                                "content": entry.content.map(|c| c.body.unwrap_or_default()).unwrap_or_default(),
                                "published": entry.published.map(|d| d.to_rfc3339()).unwrap_or_default(),
                                "entryId": entry_id,
                                "author": entry.authors.first().map(|a| a.name.clone()).unwrap_or_default(),
                            }))?;
                        }
                        first_poll = false;
                    }
                }
            }
            Ok(())
        })
    }
}

register_node!(RssNode);
