//! AirtableUpdateRecord: change some of a record's fields (a PATCH:
//! unnamed fields keep their values).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{json_call, required_str, url};
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::airtable::API;

#[derive(NodeManifest)]
pub struct AirtableUpdateRecordNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for AirtableUpdateRecordNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let base: String = ctx.inputs.get("base")?;
        let table: String = ctx.inputs.get("table")?;
        let record: String = ctx.inputs.get("recordId")?;
        let fields: Value = ctx.inputs.get("fields")?;
        let typecast: bool = ctx.inputs.get("typecast")?;

        let mut body = json!({ "fields": fields });
        if typecast {
            body["typecast"] = json!(true);
        }

        let http = ctx.client(&account).await?;
        let updated = json_call(
            http.patch(url(API, &[&base, &table, &record])?.as_str())
                .json(&body),
            "airtable: update the record",
        )
        .await?;
        let id = required_str(&updated, "airtable: update the record", "id")?;
        ctx.pulse_downstream(
            NodeOutput::new().set("recordId", id).set("record", updated.clone()),
        )
        .await
    }
}
