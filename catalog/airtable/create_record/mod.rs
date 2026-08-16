//! AirtableCreateRecord: add a record to a table.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::airtable::{checked_id, API};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let base: String = ctx.inputs.get("base")?;
        let table: String = ctx.inputs.get("table")?;
        let fields: Value = ctx.inputs.get("fields")?;
        let typecast: bool = ctx.inputs.get("typecast")?;

        let base = checked_id(&base, "base id")?;
        let table = checked_id(&table, "table")?;
        let mut body = json!({ "fields": fields });
        if typecast {
            body["typecast"] = json!(true);
        }

        let http = ctx.client(&account).await?;
        let record = post_json(
            &http,
            &format!("{API}/{base}/{}", urlencoding::encode(table)),
            &body,
            "airtable: create the record",
        )
        .await?;
        let id = record["id"].as_str().node_err("airtable: the record answered no id")?;
        ctx.pulse_downstream(NodeOutput::new().set("recordId", id).set("record", record.clone()))
            .await
    }
}
