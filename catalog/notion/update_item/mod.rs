//! NotionUpdateItem: change a database row's properties (and
//! optionally archive/restore it).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{json_call, url};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::notion::{page_output, API};

#[derive(NodeManifest)]
pub struct NotionUpdateItemNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for NotionUpdateItemNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let page: String = ctx.inputs.get("pageId")?;
        let properties: Option<Value> = ctx.inputs.opt("properties")?;
        let archived: Option<bool> = ctx.inputs.opt("archived")?;

        let mut body = json!({});
        if let Some(p) = &properties {
            body["properties"] = p.clone();
        }
        if let Some(a) = archived {
            body["archived"] = json!(a);
        }

        let http = ctx.client(&account).await?;
        let page = json_call(
            http.patch(url(API, &["pages", &page])?.as_str()).json(&body),
            "notion: update the item",
        )
        .await?;
        ctx.pulse_downstream(page_output(&page, "notion: update the item")?).await
    }
}
