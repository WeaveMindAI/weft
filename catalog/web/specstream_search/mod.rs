//! SpecStreamSearch: search the live web through SpecStream and emit
//! results with the passage of each page closest to the query, the
//! shape an LLM node consumes directly.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::storage::cap_text;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::wire::fit_texts;

#[cfg(feature = "node-tests")]
mod tests;

pub const SEARCH_URL: &str = "https://api.specstream.ai/v1/web-search";

/// The most characters SpecStream returns for one source's text.
const MAX_SOURCE_CHARS: usize = 10_000;

#[derive(NodeManifest)]
pub struct SpecStreamSearchNode;

#[async_trait]
impl Node for SpecStreamSearchNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get("query")?;
        let num_results: u64 = ctx.inputs.get("numResults")?;
        let max_text_chars: usize = ctx.inputs.get("maxTextChars")?;
        let site: Option<String> = ctx.inputs.opt("site")?;

        let http = ctx.client(&access).await?;
        let answer = weft::access::client::post_json(
            &http,
            SEARCH_URL,
            &search_body(&query, num_results, site.as_deref(), max_text_chars),
            "specstream: search",
        )
        .await?;
        let mut results = results_from(&answer, max_text_chars);
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

/// The search request. `compact` asks for one passage per page, the
/// one closest to the query, cut server-side at the node's cap so the
/// answer stays small. SpecStream holds `site:` on every result, so the
/// site rides in the query only when it is set.
pub fn search_body(query: &str, num_results: u64, site: Option<&str>, max_text_chars: usize) -> Value {
    let q = match site {
        Some(site) => format!("{query} site:{site}"),
        None => query.to_string(),
    };
    json!({
        "q": q,
        "num": num_results,
        "compact": true,
        "source_max_chars": max_text_chars.clamp(1, MAX_SOURCE_CHARS),
    })
}

/// Each source shaped to the declared fields. The reply sits under
/// `data`; a source's passage is its `snippet`, cut at `max_text_chars`
/// here too, since SpecStream counts characters the same way.
pub fn results_from(answer: &Value, max_text_chars: usize) -> Vec<Value> {
    answer["data"]["sources"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|s| s["url"].is_string())
        .map(|s| {
            let text = s["snippet"].as_str().map(|t| cap_text(t, max_text_chars).0);
            json!({
                "title": s["title"],
                "url": s["url"],
                "publishedDate": s["published"],
                "text": text,
            })
        })
        .collect()
}
