//! GoogleSheetsNewRow: fires once per row added to a tab. Registers a
//! delta poll on the tab's values (the listener owns the durable
//! cursor: activation primes it silently, a listener restart never
//! re-fires history, and only rows past the primed length fire).
//!
//! `setup_trigger` resolves the tab's gid to its title once (the
//! values URL only speaks titles) and registers the poll;
//! `run` receives one `{ item, index }` wake per new row and shapes
//! it: the raw cells, the header-keyed object (when the sheet has a
//! header; read fresh per fire, so renamed columns stay honest), and
//! the 1-based sheet row number.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::signal::{PollDelta, PollEndpoint};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::sheets::{read_cells, row_object, row_to_strings, tab_title, values_url};

#[derive(NodeManifest)]
pub struct GoogleSheetsNewRowNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleSheetsNewRowNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let id: String = ctx.inputs.get("spreadsheet")?;
        let gid: String = ctx.inputs.get("tab")?;
        let interval: f64 = ctx.inputs.get("intervalSecs")?;

        let http = ctx.client(&account).await?;
        let title = tab_title(&http, &id, &gid).await?;

        ctx.register_signal(PollEndpoint {
            url: values_url(&id, &title, None),
            interval_secs: interval as u64,
            delta: Some(PollDelta {
                items: "values".into(),
                cursor_field: None,
                mode: Default::default(),
                cursor_param: None,
            }),
            access: Some(weft::primitive::AccessRef::from(&account)),
            filters: Vec::new(),
        })
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let id: String = ctx.inputs.get("spreadsheet")?;
        let gid: String = ctx.inputs.get("tab")?;
        let has_header: bool = ctx.inputs.get("hasHeader")?;

        let wake = ctx.wake.record()?;
        let cells = row_to_strings(&wake["item"]);
        let index = wake["index"]
            .as_u64()
            .node_err("the poll wake carries no index")?;

        let mut out = NodeOutput::new()
            .set("cells", json!(cells.clone()))
            // The values array is 0-based; sheet rows are 1-based.
            .set("rowNumber", (index + 1) as f64);
        if has_header {
            // The header row is read fresh per fire so renames stay
            // honest; a header-keyed sheet whose first row IS the new
            // row (index 0) has no data meaning, skip the object.
            if index > 0 {
                let http = ctx.client(&account).await?;
                let title = tab_title(&http, &id, &gid).await?;
                let headers = read_cells(&http, &id, &title)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or_default();
                out = out.set("row", Value::Object(row_object(cells, Some(&headers))));
            }
        } else {
            out = out.set("row", Value::Object(row_object(cells, None)));
        }
        ctx.pulse_downstream(out).await
    }
}
