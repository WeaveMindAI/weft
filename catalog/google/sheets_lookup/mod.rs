//! GoogleSheetsLookup: find the rows where a column holds a value.
//! The read-side workhorse: resolve an order id to its row, check
//! whether an email is already logged, fetch a record by key.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::sheets::{read_cells, row_object, tab_title};

#[derive(NodeManifest)]
pub struct GoogleSheetsLookupNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleSheetsLookupNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let id: String = ctx.inputs.get("spreadsheet")?;
        let gid: String = ctx.inputs.get("tab")?;
        let column: String = ctx.inputs.get("column")?;
        let value: String = ctx.inputs.get("value")?;

        let http = ctx.client(&account).await?;
        let title = tab_title(&http, &id, &gid).await?;
        let mut cells = read_cells(&http, &id, &title).await?.into_iter();
        let Some(headers) = cells.next() else {
            weft::node_bail!("the tab is empty; a lookup needs a header row");
        };
        let Some(col_idx) = headers.iter().position(|h| h == &column) else {
            weft::node_bail!(
                "the header has no column named '{column}' (header: {headers:?})"
            );
        };

        let mut rows = Vec::new();
        let mut row_numbers = Vec::new();
        for (i, record) in cells.enumerate() {
            if record.get(col_idx).map(String::as_str) == Some(value.as_str()) {
                rows.push(Value::Object(row_object(record, Some(&headers))));
                // 1-based sheet row: +2 skips past 1-basing and the header.
                row_numbers.push(json!(i + 2));
            }
        }

        let found = !rows.is_empty();
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("rows", json!(rows))
                .set("rowNumbers", json!(row_numbers))
                .set("found", found),
        )
        .await
    }
}
