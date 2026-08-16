//! NotionCreateItem: add a row to a database's data source, with the
//! row's properties as Notion property-value JSON.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::notion::{data_source_id, API};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let database: String = ctx.inputs.get("database")?;
        let source: Option<String> = ctx.inputs.opt("source")?;
        let properties: Value = ctx.inputs.get("properties")?;

        let http = ctx.client(&account).await?;
        let source = data_source_id(&http, &database, source.as_deref()).await?;
        let page = post_json(
            &http,
            &format!("{API}/pages"),
            &json!({
                "parent": { "type": "data_source_id", "data_source_id": source },
                "properties": properties,
            }),
            "notion: create the item",
        )
        .await?;
        let id = page["id"].as_str().node_err("notion: the item answered no id")?;
        ctx.pulse_downstream(
            NodeOutput::new().set("pageId", id).set("url", page["url"].clone()),
        )
        .await
    }
}
