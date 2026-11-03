//! GoogleSheetsUpdate: overwrite one row (addressed by its sheet row
//! number, e.g. the number the append node or the new-row trigger
//! emitted) or an explicit A1 range. Exactly one of the two
//! addressings.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::sheets::{header_for_row, row_to_cells, tab_title, values_url, Row};

#[derive(NodeManifest)]
pub struct GoogleSheetsUpdateNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleSheetsUpdateNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let id: String = ctx.inputs.get("spreadsheet")?;
        let gid: Option<String> = ctx.inputs.opt("tab")?;
        let row: Row = ctx.inputs.get("row")?;
        let row_number: Option<u64> = ctx.inputs.opt("rowNumber")?;
        let has_header: bool = ctx.inputs.get("hasHeader")?;

        // The metadata makes the addressing exactly one of the two:
        // `oneOfRequired` skips the node with neither, and a rule refuses
        // both, written or wired. The widget holds `rowNumber` to a
        // sheet's rows.
        let addr = match row_number {
            Some(n) => format!("A{n}"),
            None => ctx.inputs.get("range")?,
        };

        let http = ctx.client(&account).await?;
        let title = tab_title(&http, &id, gid.as_deref()).await?;

        let headers = header_for_row(&http, &id, &title, &row, has_header).await?;
        let cells = row_to_cells(&row, headers.as_deref())?;

        let url = values_url(&id, &title, Some(&addr))?.query("valueInputOption", "USER_ENTERED");
        weft::access::client::json_call(http.put(url.as_str()).json(&json!({ "values": [cells] })), "update the row")
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
