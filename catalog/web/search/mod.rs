//! WebSearch: search the web through Exa (semantic + keyword,
//! auto-picked per query) and emit results with their page text, the
//! shape an LLM node consumes directly.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use weft::storage::MAX_WIRE_VALUE_BYTES;

use super::wire::fit_texts;

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get("query")?;
        let num_results: u64 = ctx.inputs.get("numResults")?;
        let include_text: bool = ctx.inputs.get("includeText")?;
        let max_text_chars: u64 = ctx.inputs.get("maxTextChars")?;
        let domains: Vec<String> = ctx.inputs.list("includeDomains")?;

        let mut body = json!({
            "query": query,
            "type": "auto",
            "numResults": num_results,
        });
        if include_text {
            // Exa cuts each page's text at `maxCharacters`, so the
            // emitted results carry a bounded amount of text whatever
            // the pages weigh: a wire carries at most 100 KB, and ten
            // long articles uncapped would fail the run on a day the
            // news happened to be long. Settings whose texts could not
            // fit even one byte a character are refused before the paid
            // search.
            if num_results * max_text_chars > MAX_WIRE_VALUE_BYTES as u64 {
                weft::input_bail!(
                    "`numResults` {num_results} times `maxTextChars` {max_text_chars} is more text than the {} KB \
                     a wire carries; lower one of them so the two multiplied are at most {}",
                    MAX_WIRE_VALUE_BYTES / 1024,
                    MAX_WIRE_VALUE_BYTES
                );
            }
            body["contents"] = json!({ "text": { "maxCharacters": max_text_chars } });
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

        let mut results: Vec<Value> = answer["results"]
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
        // A page text of a script that takes several bytes a character
        // weighs more than its count of characters: the longest texts
        // are shortened further until the results fit a wire.
        fit_texts(&mut results, "text").map_err(|fixed| {
            weft::node_error(format!(
                "the results weigh {} without any page text; lower `numResults`",
                weft::storage::wire_weight_said(fixed)
            ))
        })?;
        let count = results.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new().set("results", json!(results)).set("count", count),
        )
        .await
    }
}
