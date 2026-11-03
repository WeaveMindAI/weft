//! AirtableNewRecord: fires once per record added to a table.
//! Registers a delta poll sorted newest-first on a Created-time
//! field, with the listener's seen-set cursor (activation primes
//! silently, history never replays, the position survives restarts).
//!
//! Airtable's list API can only sort by a real column, so the trigger
//! asks which Created-time column to sort on: newest-first is what
//! lets the poll stop paging at the first record it has already seen.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::access::client::url;
use weft::signal::{DeltaMode, PollDelta, PollEndpoint, PollPaging};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::airtable::API;

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

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let base: String = ctx.inputs.get("base")?;
        let table: String = ctx.inputs.get("table")?;
        let created_field: String = ctx.inputs.get("createdField")?;
        let interval: u64 = ctx.inputs.get("intervalSecs")?;

        let url = url(API, &[&base, &table])?
            .query("pageSize", 100)
            .query("sort[0][field]", &created_field)
            .query("sort[0][direction]", "desc");

        ctx.register_signal(PollEndpoint {
            url: url.into(),
            interval_secs: interval,
            delta: Some(PollDelta {
                items: "records".into(),
                cursor_field: Some("id".into()),
                mode: DeltaMode::Set,
                cursor_param: None,
                paging: Some(PollPaging::query("offset", "offset")),
            }),
            access: Some(weft::primitive::AccessRef::from(&account)),
            ..Default::default()
        })
        .await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let item = ctx.wake.nested("item")?;
        let id: String = item.get("id")?;
        let record = item.record()?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("record", record.clone())
                .set("recordId", id)
                .set("fields", record["fields"].clone()),
        )
        .await
    }
}
