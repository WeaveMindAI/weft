//! NotionNewItem: fires once per row added to a database. Registers a
//! POST delta poll on the data source's query route, newest first,
//! with the listener's seen-set cursor (activation primes silently,
//! history never replays, the position survives restarts).

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::signal::{DeltaMode, PollDelta, PollEndpoint, PollMethod};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

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

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let database: String = ctx.inputs.get("database")?;
        let source: Option<String> = ctx.inputs.opt("source")?;
        let interval: f64 = ctx.inputs.get("intervalSecs")?;

        let http = ctx.client(&account).await?;
        let source = data_source_id(&http, &database, source.as_deref()).await?;

        ctx.register_signal(PollEndpoint {
            url: format!("{API}/data_sources/{source}/query"),
            method: PollMethod::Post,
            // Newest first, so a new row is always on the first page
            // however big the database grows.
            body: Some(json!({
                "sorts": [{ "timestamp": "created_time", "direction": "descending" }],
                "page_size": 100,
            })),
            interval_secs: interval as u64,
            delta: Some(PollDelta {
                items: "results".into(),
                cursor_field: Some("id".into()),
                mode: DeltaMode::Set,
                cursor_param: None,
            }),
            access: Some(weft::primitive::AccessRef::from(&account)),
            ..Default::default()
        })
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let wake = ctx.wake.record()?;
        let page = wake["item"].clone();
        let id = page["id"].as_str().node_err("the poll wake carries no page id")?.to_string();
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
