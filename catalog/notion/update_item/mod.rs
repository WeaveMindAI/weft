//! NotionUpdateItem: change a database row's properties (and
//! optionally archive/restore it).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::json_call;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::notion::API;

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
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
        if body.as_object().expect("object").is_empty() {
            weft::node_bail!("nothing to update: set properties or archived");
        }

        let http = ctx.client(&account).await?;
        let page = json_call(
            http.patch(format!("{API}/pages/{page}")).json(&body),
            "notion: update the item",
        )
        .await?;
        let id = page["id"].as_str().node_err("notion: the update answered no id")?;
        ctx.pulse_downstream(
            NodeOutput::new().set("pageId", id).set("url", page["url"].clone()),
        )
        .await
    }
}
