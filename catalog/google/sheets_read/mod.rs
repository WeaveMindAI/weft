//! GoogleSheetsRead: read a spreadsheet's rows, signed in or not.
//!
//! The worked example of a node whose account input is optional
//! (`ctx.client` accepts the absent connection). Google serves the two
//! worlds at DIFFERENT addresses: the Sheets API answers a bearer token
//! (including one scoped to just the picked file) but never an
//! anonymous call, while the docs.google.com CSV export serves a
//! link-shared sheet anonymously but ignores bearer tokens (it is the
//! browser's cookie endpoint and answers 404 for a private sheet). So
//! the body branches on whether an account is connected, and both
//! branches feed one pure cells-to-rows step.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::reqwest_middleware::ClientWithMiddleware;
use weft::{node_bail, Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::sheets::{read_cells, rows_from_cells, tab_title};

#[derive(NodeManifest)]
pub struct GoogleSheetsReadNode;

#[async_trait]
impl Node for GoogleSheetsReadNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Option<Access> = ctx.inputs.opt("account")?;
        let id: String = ctx.inputs.get("spreadsheet")?;
        // `tab` and `hasHeader` declare metadata defaults, so the bag
        // always holds values. `tab` is the sheet's gid (what the tab
        // dropdown stores and what a pasted link's #gid= carries).
        let tab: String = ctx.inputs.get("tab")?;
        let has_header: bool = ctx.inputs.get("hasHeader")?;

        let http = ctx.client(account.as_ref()).await?;
        let cells = match &account {
            Some(_) => read_via_sheets_api(&http, &id, &tab).await?,
            None => read_via_public_export(&http, &id, &tab).await?,
        };
        ctx.pulse_downstream(NodeOutput::new().set("rows", rows_from_cells(cells, has_header)))
            .await
    }
}

/// Signed in: the Sheets API. Two calls: gid to title, then the
/// values (both shared package plumbing).
async fn read_via_sheets_api(http: &ClientWithMiddleware,
    id: &str,
    gid: &str,
) -> WeftResult<Vec<Vec<String>>> {
    let title = tab_title(http, id, gid).await?;
    read_cells(http, id, &title).await
}

/// Not signed in: the public CSV export, which serves a sheet shared
/// with 'anyone with the link'.
async fn read_via_public_export(http: &ClientWithMiddleware,
    id: &str,
    gid: &str,
) -> WeftResult<Vec<Vec<String>>> {
    let resp = http
        .get(format!(
            "https://docs.google.com/spreadsheets/d/{id}/export?format=csv&gid={gid}"
        ))
        .send()
        .await
        .node_err("google sheets: export")?;
    let status = resp.status();
    let csv = resp.text().await.node_err("google sheets: read export")?;
    if !status.is_success() {
        node_bail!(
            "google sheets answered {status} exporting the sheet; without a connected \
             account only a sheet shared with 'anyone with the link' can be read"
        );
    }
    parse_csv_cells(&csv)
}

/// CSV text -> cell grid. Pure, so parsing is testable offline.
fn parse_csv_cells(csv_text: &str) -> WeftResult<Vec<Vec<String>>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(csv_text.as_bytes());
    let mut cells = Vec::new();
    for record in reader.records() {
        let record = record.node_err("google sheets: csv row")?;
        cells.push(record.iter().map(str::to_string).collect());
    }
    Ok(cells)
}

#[cfg(feature = "node-tests")]
mod tests;
