//! KeenableFetchPage: one URL to clean markdown through Keenable, the
//! shape an LLM node consumes directly. The account is optional:
//! unwired, the node fetches on Keenable's public route; with a
//! KeenableAccess wired, on the keyed one. The markdown is cut at
//! `maxChars`, and shorter still if it would weigh more than a wire
//! carries, and `truncated` says when.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::keenable::Keenable;

#[cfg(feature = "node-tests")]
mod tests;

#[derive(NodeManifest)]
pub struct KeenableFetchPageNode;

/// What one fetch hands back: the page's markdown, cut to `maxChars`
/// and to a wire, whether it was cut, and the page's title.
pub struct FetchedPage {
    pub markdown: String,
    pub truncated: bool,
    pub title: String,
}

#[async_trait]
impl Node for KeenableFetchPageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Option<Access> = ctx.inputs.opt("account")?;
        let url: String = ctx.inputs.get("url")?;
        // `maxChars` declares a metadata default, so the bag holds it.
        let max_chars: f64 = ctx.inputs.get("maxChars")?;

        let keenable = Keenable::open(&ctx, account.as_ref()).await?;
        let page = fetch(&keenable, &url, max_chars as usize).await?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("markdown", page.markdown)
                .set("title", page.title)
                .set("truncated", page.truncated),
        )
        .await
    }
}

/// One fetch: the request, then the page read out of the answer.
pub async fn fetch(keenable: &Keenable, url: &str, max_chars: usize) -> WeftResult<FetchedPage> {
    let answer = keenable
        .get("fetch", &[("url", url)], "keenable: fetch the page")
        .await?;
    // The markdown IS the node's product: an answer without it is a
    // provider inconsistency, never an empty page.
    let markdown = answer
        .get("content")
        .and_then(Value::as_str)
        .node_err("keenable: the fetch succeeded but its answer carries no content")?;
    let (markdown, truncated) = weft::storage::cap_text(markdown, max_chars);
    // A title is genuinely optional (many pages have none).
    let title = answer.get("title").and_then(Value::as_str).unwrap_or_default().to_string();
    Ok(FetchedPage { markdown: markdown.to_string(), truncated, title })
}
