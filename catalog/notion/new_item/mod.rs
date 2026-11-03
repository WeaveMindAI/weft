//! NotionNewItem: fires once per row added to a database. Registers a
//! POST delta poll on the data source's query route, newest first,
//! with the listener's seen-set cursor (activation primes silently,
//! history never replays, the position survives restarts).

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::access::client::url;
use weft::signal::{DeltaMode, PollDelta, PollEndpoint, PollMethod, PollPaging};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::notion::{data_source_id, API};

#[derive(NodeManifest)]
pub struct NotionNewItemNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for NotionNewItemNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let database: String = ctx.inputs.get("database")?;
        let source: Option<String> = ctx.inputs.opt("source")?;
        let interval: u64 = ctx.inputs.get("intervalSecs")?;

        let http = ctx.client(&account).await?;
        let source = data_source_id(&http, &database, source.as_deref()).await?;

        ctx.register_signal(PollEndpoint {
            url: url(API, &["data_sources", &source, "query"])?.into(),
            method: PollMethod::Post,
            // Newest first, so the poll meets the new rows first and
            // follows `next_cursor` only until it reaches one it has
            // already seen.
            body: Some(json!({
                "sorts": [{ "timestamp": "created_time", "direction": "descending" }],
                "page_size": 100,
            })),
            interval_secs: interval,
            delta: Some(PollDelta {
                items: "results".into(),
                cursor_field: Some("id".into()),
                mode: DeltaMode::Set,
                cursor_param: None,
                paging: Some(PollPaging::body("next_cursor", "start_cursor")),
            }),
            access: Some(weft::primitive::AccessRef::from(&account)),
            ..Default::default()
        })
        .await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let item = ctx.wake.nested("item")?;
        let id: String = item.get("id")?;
        let page = item.record()?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("page", page.clone())
                .set("pageId", id)
                .set("url", page["url"].clone())
                .set("properties", page["properties"].clone()),
        )
        .await
    }
}
