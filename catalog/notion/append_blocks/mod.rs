//! NotionAppendBlocks: append plain text (or raw Notion blocks) to
//! the end of an existing page or block.

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::json_call;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let page: String = ctx.inputs.get("page")?;
        let content: Option<String> = ctx.inputs.opt("content")?;
        let blocks = ctx.inputs.raw("blocks").cloned();

        let children = children_of(content.as_deref(), blocks.as_ref())?;
        let count = children.len() as f64;
        let http = ctx.client(&account).await?;
        json_call(
            http.patch(format!("{API}/blocks/{page}/children"))
                .json(&json!({ "children": children })),
            "notion: append the blocks",
        )
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("appended", count)).await
    }
}
