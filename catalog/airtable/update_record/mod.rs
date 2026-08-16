//! AirtableUpdateRecord: change some of a record's fields (a PATCH:
//! unnamed fields keep their values).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::json_call;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::airtable::{checked_id, API};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let base: String = ctx.inputs.get("base")?;
        let table: String = ctx.inputs.get("table")?;
        let record: String = ctx.inputs.get("recordId")?;
        let fields: Value = ctx.inputs.get("fields")?;
        let typecast: bool = ctx.inputs.get("typecast")?;

        let base = checked_id(&base, "base id")?;
        let table = checked_id(&table, "table")?;
        let record = checked_id(&record, "record id")?;
        let mut body = json!({ "fields": fields });
        if typecast {
            body["typecast"] = json!(true);
        }

        let http = ctx.client(&account).await?;
        let updated = json_call(
            http.patch(format!("{API}/{base}/{}/{record}", urlencoding::encode(table)))
                .json(&body),
            "airtable: update the record",
        )
        .await?;
        let id = updated["id"].as_str().node_err("airtable: the update answered no id")?;
        ctx.pulse_downstream(
            NodeOutput::new().set("recordId", id).set("record", updated.clone()),
        )
        .await
    }
}
