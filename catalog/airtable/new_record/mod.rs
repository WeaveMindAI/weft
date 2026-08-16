//! AirtableNewRecord: fires once per record added to a table.
//! Registers a delta poll sorted newest-first on a Created-time
//! field, with the listener's seen-set cursor (activation primes
//! silently, history never replays, the position survives restarts).
//!
//! Airtable's list API can only sort by a real column, so the trigger
//! asks which Created-time column to sort on: newest-first is what
//! keeps a new record on the first page however big the table grows.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::signal::{DeltaMode, PollDelta, PollEndpoint};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::airtable::{checked_id, API};

#[derive(NodeManifest)]
pub struct AirtableNewRecordNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for AirtableNewRecordNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let base: String = ctx.inputs.get("base")?;
        let table: String = ctx.inputs.get("table")?;
        let created_field: String = ctx.inputs.get("createdField")?;
        let interval: f64 = ctx.inputs.get("intervalSecs")?;

        let base = checked_id(&base, "base id")?;
        let table = checked_id(&table, "table")?;
        let url = format!(
            "{API}/{base}/{}?pageSize=100&sort%5B0%5D%5Bfield%5D={}&sort%5B0%5D%5Bdirection%5D=desc",
            urlencoding::encode(table),
            urlencoding::encode(&created_field),
        );

        ctx.register_signal(PollEndpoint {
            url,
            interval_secs: interval as u64,
            delta: Some(PollDelta {
                items: "records".into(),
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
        let record = wake["item"].clone();
        let id = record["id"]
            .as_str()
            .node_err("the poll wake carries no record id")?
            .to_string();
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("record", record.clone())
                .set("recordId", id)
                .set("fields", record["fields"].clone()),
        )
        .await
    }
}
