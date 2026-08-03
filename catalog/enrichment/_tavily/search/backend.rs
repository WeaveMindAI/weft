//! TavilySearch Node - Search the internet using Tavily API.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// Tavily pricing: $0.008/credit, pay-as-you-go (basic search = 1 credit, advanced = 2 credits).
/// Raw cost; margin is applied downstream by get_user_margin().
const TAVILY_COST_PER_CREDIT: f64 = 0.008;

#[derive(Default)]
pub struct TavilySearchNode;

#[async_trait]
impl Node for TavilySearchNode {
    fn node_type(&self) -> &'static str {
        "TavilySearch"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Tavily Search",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", false),
                PortDef::new("query", "String", true),
            ],
            outputs: vec![
                PortDef::new("answer", "String", false),
                PortDef::new("titles", "List[String]", false),
                PortDef::new("urls", "List[String]", false),
                PortDef::new("contents", "List[String]", false),
                PortDef::new("scores", "List[Number]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::number("maxResults"),
                FieldDef::select("searchDepth", vec!["basic", "advanced"]),
                FieldDef::select("topic", vec!["general", "news"]),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let query = ctx.input.get("query")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if query.is_empty() {
            return NodeResult::failed("Search query is required");
        }

        let config_input = ctx.input.get("config").and_then(|v| v.as_object());
        let api_key_value = config_input
            .and_then(|c| c.get("apiKey"))
            .and_then(|v| v.as_str());
        let resolved = match ctx.resolve_api_key(api_key_value, "tavily") {
            Some(r) => r,
            None => return NodeResult::failed(
                "No Tavily API key available. Connect a TavilyConfig node or set the platform key."
            ),
        };

        let max_results = ctx.config.get("maxResults")
            .and_then(|v| v.as_u64())
            .unwrap_or(5) as u32;

        let search_depth = ctx.config.get("searchDepth")
            .and_then(|v| v.as_str())
            .unwrap_or("basic");

        let topic = ctx.config.get("topic")
            .and_then(|v| v.as_str())
            .unwrap_or("general");

        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "query": query,
            "search_depth": search_depth,
            "max_results": max_results,
            "topic": topic,
            "include_answer": "basic",
            "include_raw_content": false,
        });

        let response = client
            .post("https://api.tavily.com/search")
            .header("Authorization", format!("Bearer {}", resolved.key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await;

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Tavily search request failed: {}", e);
                return NodeResult::failed(&format!("Tavily request failed: {}", e));
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Tavily search error ({}): {}", status, body);
            return NodeResult::failed(&format!("Tavily search error ({}): {}", status, body));
        }

        let result: serde_json::Value = match response.json().await {
            Ok(v) => v,
            Err(e) => return NodeResult::failed(&format!("Failed to parse Tavily response: {}", e)),
        };

        let answer = result.get("answer")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let raw_results = result.get("results")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let titles: Vec<serde_json::Value> = raw_results.iter()
            .map(|r| serde_json::json!(r.get("title").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let urls: Vec<serde_json::Value> = raw_results.iter()
            .map(|r| serde_json::json!(r.get("url").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let contents: Vec<serde_json::Value> = raw_results.iter()
            .map(|r| serde_json::json!(r.get("content").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let scores: Vec<serde_json::Value> = raw_results.iter()
            .map(|r| serde_json::json!(r.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0)))
            .collect();

        let credits_used = result.get("usage")
            .and_then(|u| u.get("credits"))
            .and_then(|c| c.as_u64())
            .unwrap_or(1);
        let cost_usd = credits_used as f64 * TAVILY_COST_PER_CREDIT;

        ctx.report_usage_cost("tavily-search", "web_search", cost_usd, resolved.is_byok, Some(serde_json::json!({
            "creditsUsed": credits_used,
            "searchDepth": search_depth,
        }))).await;

        NodeResult::completed(serde_json::json!({
            "answer": answer,
            "titles": titles,
            "urls": urls,
            "contents": contents,
            "scores": scores,
        }))
    }
}

register_node!(TavilySearchNode);
