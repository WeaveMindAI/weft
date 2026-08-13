//! GoogleSheetsAppend: append one row to a tab. The universal logging
//! sink: anything happened, add a row. The row arrives as an object
//! keyed by the header's column names (ordered through the header) or
//! as a plain list of cells.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::sheets::{header_for_row, row_to_cells, tab_title, values_url};

#[derive(NodeManifest)]
pub struct GoogleSheetsAppendNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleSheetsAppendNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let id: String = ctx.inputs.get("spreadsheet")?;
        let gid: String = ctx.inputs.get("tab")?;
        let row: Value = ctx.inputs.get("row")?;
        let has_header: bool = ctx.inputs.get("hasHeader")?;

        let http = ctx.client(&account).await?;
        let title = tab_title(&http, &id, &gid).await?;
        let headers = header_for_row(&http, &id, &title, &row, has_header).await?;
        let cells = row_to_cells(&row, headers.as_deref())?;

        let url = format!(
            "{}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS",
            values_url(&id, &title, None)
        );
        let body = weft::access::client::post_json(
            &http,
            &url,
            &json!({ "values": [cells] }),
            "append the row",
        )
        .await?;
        // "'Tab'!A5:C5" -> 5: the appended row's 1-based sheet row. A
        // successful append always names its updated range; a missing
        // or unparseable one is a broken contract, never a silently
        // absent rowNumber that surfaces far downstream as a
        // confusing addressing error.
        let row_number = body
            .pointer("/updates/updatedRange")
            .and_then(Value::as_str)
            .and_then(|r| r.rsplit(['!', ':']).next())
            .and_then(|cell| {
                cell.trim_start_matches(|c: char| c.is_ascii_alphabetic()).parse::<f64>().ok()
            })
            .node_err("google sheets: the append answered without a parseable updatedRange")?;

        ctx.pulse_downstream(
            NodeOutput::new().set("rowNumber", row_number).set("done", true),
        )
        .await
    }
}
