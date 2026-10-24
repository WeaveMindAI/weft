//! KeenableSearch: search the web through Keenable and emit results
//! with their page text, the shape an LLM node consumes directly. The
//! account is optional: unwired, the node searches on Keenable's public
//! route; with a KeenableAccess wired, on the keyed one.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::storage::cap_text;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::keenable::Keenable;
use super::wire::fit_texts;

#[cfg(feature = "node-tests")]
mod tests;

#[derive(NodeManifest)]
pub struct KeenableSearchNode;

#[async_trait]
impl Node for KeenableSearchNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Option<Access> = ctx.inputs.opt("account")?;
        let query: String = ctx.inputs.get("query")?;
        let num_results: f64 = ctx.inputs.get("numResults")?;
        let max_text_chars: f64 = ctx.inputs.get("maxTextChars")?;
        let site: Option<String> = ctx.inputs.opt("site")?;

        let keenable = Keenable::open(&ctx, account.as_ref()).await?;
        let results = search(
            &keenable,
            &query,
            num_results as u64,
            site.as_deref(),
            max_text_chars as usize,
        )
        .await?;
        let count = results.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new().set("results", json!(results)).set("count", count),
        )
        .await
    }
}

/// One search: the request, then the results shaped and fitted to a
/// wire.
pub async fn search(
    keenable: &Keenable,
    query: &str,
    num_results: u64,
    site: Option<&str>,
    max_text_chars: usize,
) -> WeftResult<Vec<Value>> {
    let answer = keenable
        .post("search", &search_body(query, num_results, site), "keenable: search")
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
    Ok(results)
}

/// The search request. `site` rides only when it names a domain, so an
/// emptied field searches the whole web.
pub fn search_body(query: &str, num_results: u64, site: Option<&str>) -> Value {
    let mut body = json!({ "query": query, "max_results": num_results });
    if let Some(site) = site.map(str::trim).filter(|s| !s.is_empty()) {
        body["site"] = json!(site);
    }
    body
}

/// Each result shaped to the declared fields. Keenable leaves a
/// result's `description` empty and puts the page text in `snippet`,
/// which is cut at `max_text_chars` here.
pub fn results_from(answer: &Value, max_text_chars: usize) -> Vec<Value> {
    answer["results"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|r| {
            let text = r["snippet"].as_str().map(|s| cap_text(s, max_text_chars).0);
            json!({
                "title": r["title"],
                "url": r["url"],
                "text": text,
            })
        })
        .collect()
}
