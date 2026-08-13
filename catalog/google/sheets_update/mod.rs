//! GoogleSheetsUpdate: overwrite one row (addressed by its sheet row
//! number, e.g. the number the append node or the new-row trigger
//! emitted) or an explicit A1 range. Exactly one of the two
//! addressings.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::sheets::{header_for_row, row_to_cells, tab_title, values_url};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let id: String = ctx.inputs.get("spreadsheet")?;
        let gid: String = ctx.inputs.get("tab")?;
        let row: Value = ctx.inputs.get("row")?;
        let row_number: Option<f64> = ctx.inputs.opt("rowNumber")?;
        let range: Option<String> = ctx.inputs.opt("range")?;
        let has_header: bool = ctx.inputs.get("hasHeader")?;

        let http = ctx.client(&account).await?;
        let title = tab_title(&http, &id, &gid).await?;

        let headers = header_for_row(&http, &id, &title, &row, has_header).await?;
        let cells = row_to_cells(&row, headers.as_deref())?;

        let addr = match (row_number, range) {
            (Some(n), None) => {
                let n = n as u64;
                if n == 0 {
                    weft::node_bail!("rowNumber is 1-based; 0 addresses nothing");
                }
                // A whole-row write starting at column A.
                format!("A{n}")
            }
            (None, Some(r)) => r,
            (Some(_), Some(_)) => {
                weft::node_bail!("pick ONE addressing: rowNumber or range, not both")
            }
            (None, None) => weft::node_bail!("pick an addressing: rowNumber or range"),
        };

        let url = format!(
            "{}?valueInputOption=USER_ENTERED",
            values_url(&id, &title, Some(&addr))
        );
        weft::access::client::json_call(http.put(&url).json(&json!({ "values": [cells] })), "update the row")
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
