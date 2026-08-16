//! NotionQueryDatabase: read rows out of a database's data source,
//! with Notion's own filter/sort JSON, paging until `limit`.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::notion::{data_source_id, API};

#[derive(NodeManifest)]
pub struct NotionQueryDatabaseNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for NotionQueryDatabaseNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let database: String = ctx.inputs.get("database")?;
        let source: Option<String> = ctx.inputs.opt("source")?;
        let filter = ctx.inputs.raw("filter").cloned();
        let sorts = ctx.inputs.raw("sorts").cloned();
        let limit: f64 = ctx.inputs.get("limit")?;
        let limit = (limit as usize).clamp(1, 1000);

        let http = ctx.client(&account).await?;
        let source = data_source_id(&http, &database, source.as_deref()).await?;

        let mut items: Vec<Value> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut body = json!({ "page_size": (limit - items.len()).min(100) });
            if let Some(f) = &filter {
                body["filter"] = f.clone();
            }
            if let Some(s) = &sorts {
                body["sorts"] = s.clone();
            }
            if let Some(c) = &cursor {
                body["start_cursor"] = json!(c);
            }
            let answer = post_json(
                &http,
                &format!("{API}/data_sources/{source}/query"),
                &body,
                "notion: query the database",
            )
            .await?;
            items.extend(answer["results"].as_array().into_iter().flatten().cloned());
            cursor = answer["next_cursor"].as_str().map(str::to_string);
            let more = answer["has_more"].as_bool() == Some(true);
            if items.len() >= limit || !more || cursor.is_none() {
                break;
            }
        }
        items.truncate(limit);

        let count = items.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("items", json!(items)).set("count", count))
            .await
    }
}
