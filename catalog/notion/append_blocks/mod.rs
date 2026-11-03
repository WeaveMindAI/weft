//! NotionAppendBlocks: append plain text (or raw Notion blocks) to
//! the end of an existing page or block.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{json_call, url};
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::notion::{children_of, API};

#[derive(NodeManifest)]
pub struct NotionAppendBlocksNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for NotionAppendBlocksNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let page: String = ctx.inputs.get("page")?;
        let content: Option<String> = ctx.inputs.opt("content")?;
        let blocks: Vec<Value> = ctx.inputs.list("blocks")?;

        // Never empty: `oneOfRequired` skips the node with neither input
        // given, a blank `content` counts as not given, and an empty
        // `blocks` falls back to `content`, refused when that is blank
        // too (the node's `blocks` rule).
        let children = children_of(content.as_deref(), blocks);
        let count = children.len() as f64;
        let http = ctx.client(&account).await?;
        json_call(
            http.patch(url(API, &["blocks", &page, "children"])?.as_str())
                .json(&json!({ "children": children })),
            "notion: append the blocks",
        )
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("appended", count)).await
    }
}
