//! NotionCreatePage: create a page under a parent page, with plain
//! text (or raw Notion blocks) as its body.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::notion::{children_of, API};

#[derive(NodeManifest)]
pub struct NotionCreatePageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for NotionCreatePageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let parent: String = ctx.inputs.get("parentPage")?;
        let title: String = ctx.inputs.get("title")?;
        let content: Option<String> = ctx.inputs.opt("content")?;
        let blocks = ctx.inputs.raw("blocks").cloned();

        let body = json!({
            "parent": { "page_id": parent },
            "properties": {
                "title": { "title": [{ "type": "text", "text": { "content": title } }] },
            },
            "children": children_of(content.as_deref(), blocks.as_ref())?,
        });

        let http = ctx.client(&account).await?;
        let page =
            post_json(&http, &format!("{API}/pages"), &body, "notion: create the page").await?;
        let id = page["id"].as_str().node_err("notion: the page answered no id")?;
        ctx.pulse_downstream(
            NodeOutput::new().set("pageId", id).set("url", page["url"].clone()),
        )
        .await
    }
}
