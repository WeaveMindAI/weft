//! NotionCreatePage: create a page under a parent page, with plain
//! text (or raw Notion blocks) as its body.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{post_json, url};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::notion::{children_of, page_output, API};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let parent: String = ctx.inputs.get("parentPage")?;
        let title: String = ctx.inputs.get("title")?;
        let content: Option<String> = ctx.inputs.opt("content")?;
        let blocks: Vec<Value> = ctx.inputs.list("blocks")?;

        let mut body = json!({
            "parent": { "page_id": parent },
            "properties": {
                "title": { "title": [{ "type": "text", "text": { "content": title } }] },
            },
        });
        // A page with only a title is a page: no body, no `children`.
        let children = children_of(content.as_deref(), blocks);
        if !children.is_empty() {
            body["children"] = json!(children);
        }

        let http = ctx.client(&account).await?;
        let page = post_json(&http, url(API, &["pages"])?.as_str(), &body, "notion: create the page").await?;
        ctx.pulse_downstream(page_output(&page, "notion: create the page")?).await
    }
}
