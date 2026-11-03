//! AirtableSearchRecords: read records out of a table, optionally
//! narrowed by an Airtable formula and a view, paging until the
//! requested count.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{cursor_paged, url, CursorPaging};
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::airtable::API;

#[derive(NodeManifest)]
pub struct AirtableSearchRecordsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for AirtableSearchRecordsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let base: String = ctx.inputs.get("base")?;
        let table: String = ctx.inputs.get("table")?;
        let formula: Option<String> = ctx.inputs.opt("filterByFormula")?;
        let view: Option<String> = ctx.inputs.opt("view")?;
        let max: usize = ctx.inputs.get("maxRecords")?;

        let url = url(API, &[&base, &table])?
            .query("maxRecords", max)
            .query("pageSize", max.min(100))
            .query_opt("filterByFormula", formula)
            .query_opt("view", view);

        let http = ctx.client(&account).await?;
        let mut records: Vec<Value> = Vec::new();
        cursor_paged(
            CursorPaging {
                items: "/records",
                next: "/offset",
                param: Some("offset"),
                items_may_be_absent: false,
                past_cap_hint: "lower `maxRecords` or narrow `filterByFormula`",
            },
            "airtable: list the records",
            |_| http.get(url.as_str()),
            Ok,
            |page| {
                records.extend(page.iter().cloned());
                Ok((records.len() >= max).then_some(()))
            },
        )
        .await?;
        records.truncate(max);

        let count = records.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new().set("records", json!(records)).set("count", count),
        )
        .await
    }
}
