//! NotionCreateItem: add a row to a database's data source, with the
//! row's properties as Notion property-value JSON.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{post_json, url};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::notion::{data_source_id, page_output, API};

#[derive(NodeManifest)]
pub struct NotionCreateItemNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for NotionCreateItemNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let database: String = ctx.inputs.get("database")?;
        let source: Option<String> = ctx.inputs.opt("source")?;
        let properties: Value = ctx.inputs.get("properties")?;

        let http = ctx.client(&account).await?;
        let source = data_source_id(&http, &database, source.as_deref()).await?;
        let page = post_json(
            &http,
            url(API, &["pages"])?.as_str(),
            &json!({
                "parent": { "type": "data_source_id", "data_source_id": source },
                "properties": properties,
            }),
            "notion: create the item",
        )
        .await?;
        ctx.pulse_downstream(page_output(&page, "notion: create the item")?).await
    }
}
