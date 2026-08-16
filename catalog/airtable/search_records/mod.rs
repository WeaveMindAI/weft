//! AirtableSearchRecords: read records out of a table, optionally
//! narrowed by an Airtable formula and a view, paging until the
//! requested count.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::get_json;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::airtable::{checked_id, API};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let base: String = ctx.inputs.get("base")?;
        let table: String = ctx.inputs.get("table")?;
        let formula: Option<String> = ctx.inputs.opt("filterByFormula")?;
        let view: Option<String> = ctx.inputs.opt("view")?;
        let max: f64 = ctx.inputs.get("maxRecords")?;
        let max = (max as usize).clamp(1, 1000);

        let base = checked_id(&base, "base id")?;
        let table = checked_id(&table, "table")?;
        let mut query = format!("maxRecords={max}&pageSize={}", max.min(100));
        if let Some(f) = formula.as_deref().filter(|f| !f.trim().is_empty()) {
            query.push_str(&format!("&filterByFormula={}", urlencoding::encode(f)));
        }
        if let Some(v) = view.as_deref().filter(|v| !v.trim().is_empty()) {
            query.push_str(&format!("&view={}", urlencoding::encode(v)));
        }
        let url = format!("{API}/{base}/{}?{query}", urlencoding::encode(table));

        let http = ctx.client(&account).await?;
        let mut records: Vec<Value> = Vec::new();
        let mut offset: Option<String> = None;
        loop {
            let page_url = match &offset {
                Some(o) => format!("{url}&offset={}", urlencoding::encode(o)),
                None => url.clone(),
            };
            let answer = get_json(&http, &page_url, "airtable: list the records").await?;
            records.extend(answer["records"].as_array().into_iter().flatten().cloned());
            offset = answer["offset"].as_str().map(str::to_string);
            if offset.is_none() || records.len() >= max {
                break;
            }
        }
        records.truncate(max);

        let count = records.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new().set("records", json!(records)).set("count", count),
        )
        .await
    }
}
