//! NotionQueryDatabase: read rows out of a database's data source,
//! with Notion's own filter/sort JSON, paging until `limit`.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{cursor_paged, url, CursorPaging};
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let database: String = ctx.inputs.get("database")?;
        let source: Option<String> = ctx.inputs.opt("source")?;
        let filter = ctx.inputs.raw("filter").cloned();
        let sorts = ctx.inputs.raw("sorts").cloned();
        let limit: usize = ctx.inputs.get("limit")?;

        let http = ctx.client(&account).await?;
        let source = data_source_id(&http, &database, source.as_deref()).await?;
        let query = url(API, &["data_sources", &source, "query"])?;

        let mut body = json!({ "page_size": limit.min(100) });
        if let Some(f) = filter {
            body["filter"] = f;
        }
        if let Some(s) = sorts {
            body["sorts"] = s;
        }
        let mut items: Vec<Value> = Vec::new();
        // Notion takes the cursor in the body, so each page's request
        // puts it there itself.
        cursor_paged(
            CursorPaging {
                items: "/results",
                next: "/next_cursor",
                param: None,
                items_may_be_absent: false,
                past_cap_hint: "lower `limit`",
            },
            "notion: query the database",
            |cursor| {
                let mut page = body.clone();
                if let Some(c) = cursor {
                    page["start_cursor"] = json!(c);
                }
                http.post(query.as_str()).json(&page)
            },
            Ok,
            |page| {
                items.extend(page.iter().cloned());
                Ok((items.len() >= limit).then_some(()))
            },
        )
        .await?;
        items.truncate(limit);

        let count = items.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("items", json!(items)).set("count", count))
            .await
    }
}
