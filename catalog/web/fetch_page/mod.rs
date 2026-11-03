//! FetchPage: one URL to clean markdown through Firecrawl (JS
//! rendered, boilerplate stripped), the shape an LLM node consumes
//! directly. The markdown is cut at `maxChars`, and shorter still if
//! it would weigh more than a wire carries, and `truncated` says when.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[cfg(feature = "node-tests")]
mod tests;

#[derive(NodeManifest)]
pub struct FetchPageNode;

#[async_trait]
impl Node for FetchPageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let url: String = ctx.inputs.get("url")?;
        let only_main: bool = ctx.inputs.get("onlyMainContent")?;
        // `maxChars` declares a metadata default, so the bag holds it.
        let max_chars: usize = ctx.inputs.get("maxChars")?;

        let http = ctx.client(&access).await?;
        let answer = super::firecrawl::call(
            &http,
            "scrape",
            &json!({
                "url": url,
                "formats": ["markdown"],
                "onlyMainContent": only_main,
            }),
            "firecrawl: fetch the page",
        )
        .await?;
        // The markdown IS the node's product: a success answer without
        // it is a provider inconsistency, never an empty page.
        let markdown = answer
            .pointer("/data/markdown")
            .and_then(Value::as_str)
            .node_err("firecrawl: the scrape succeeded but its answer carries no markdown")?;
        let (markdown, truncated) = weft::storage::cap_text(markdown, max_chars);
        // A title is genuinely optional (many pages have none).
        let title = answer
            .pointer("/data/metadata/title")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("markdown", markdown.to_string())
                .set("title", title)
                .set("truncated", truncated),
        )
        .await
    }
}
