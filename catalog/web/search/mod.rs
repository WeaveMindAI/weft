//! WebSearch: search the web through Exa (semantic + keyword,
//! auto-picked per query) and emit results with their page text, the
//! shape an LLM step consumes directly.

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[cfg(feature = "node-tests")]
mod tests;

#[derive(NodeManifest)]
pub struct WebSearchNode;

#[async_trait]
impl Node for WebSearchNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get("query")?;
        let num_results: f64 = ctx.inputs.get("numResults")?;
        let include_text: bool = ctx.inputs.get("includeText")?;
        let domains: Vec<String> = ctx.inputs.list("includeDomains")?;

        let mut body = json!({
            "query": query,
            "type": "auto",
            "numResults": (num_results as u64).clamp(1, 100),
        });
        if include_text {
            body["contents"] = json!({ "text": true });
        }
        if !domains.is_empty() {
            body["includeDomains"] = json!(domains);
        }

        let http = ctx.client(&access).await?;
        let answer = weft::access::client::post_json(
            &http,
            "https://api.exa.ai/search",
            &body,
            "exa: search",
        )
        .await?;

        let results: Vec<Value> = answer["results"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|r| {
                json!({
                    "title": r["title"],
                    "url": r["url"],
                    "publishedDate": r["publishedDate"],
                    "text": r["text"],
                })
            })
            .collect();
        let count = results.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new().set("results", json!(results)).set("count", count),
        )
        .await
    }
}
