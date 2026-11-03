//! AirtableCreateRecord: add a record to a table.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{post_json, required_str, url};
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::airtable::API;

#[derive(NodeManifest)]
pub struct AirtableCreateRecordNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for AirtableCreateRecordNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let base: String = ctx.inputs.get("base")?;
        let table: String = ctx.inputs.get("table")?;
        let fields: Value = ctx.inputs.get("fields")?;
        let typecast: bool = ctx.inputs.get("typecast")?;

        let mut body = json!({ "fields": fields });
        if typecast {
            body["typecast"] = json!(true);
        }

        let http = ctx.client(&account).await?;
        let record = post_json(
            &http,
            url(API, &[&base, &table])?.as_str(),
            &body,
            "airtable: create the record",
        )
        .await?;
        let id = required_str(&record, "airtable: create the record", "id")?;
        ctx.pulse_downstream(NodeOutput::new().set("recordId", id).set("record", record.clone()))
            .await
    }
}
