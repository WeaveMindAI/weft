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
//!
//! A tab can hold far more than the 100 KB a wire carries, so one call
//! emits one page of it: the data rows from `offset` on, at most
//! `maxRows` of them and only as many as a wire carries, with
//! `nextOffset` where the next page starts and `hasMore` whether there
//! is one.

use async_trait::async_trait;
use serde_json::Value;

use weft::access::client::checked_send;
use weft::node::NodeOutput;
use weft::reqwest_middleware::ClientWithMiddleware;
use weft::storage::wire_page;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::api::page_size;
use super::sheets::{heavy_row, read_cells, rows_from_cells, tab_title};

#[derive(NodeManifest)]
pub struct GoogleSheetsReadNode;

#[async_trait]
impl Node for GoogleSheetsReadNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Option<Access> = ctx.inputs.opt("account")?;
        let id: String = ctx.inputs.get("spreadsheet")?;
        // `tab` is the sheet's gid (what the tab dropdown stores and what
        // a pasted link's #gid= carries); unset is the first tab.
        // `hasHeader` declares a metadata default, so the bag holds it.
        let tab: Option<String> = ctx.inputs.opt("tab")?;
        let has_header: bool = ctx.inputs.get("hasHeader")?;
        let offset = ctx.inputs.get::<f64>("offset")? as usize;
        let max_rows = page_size(&ctx.inputs, "maxRows")?;

        let http = ctx.client(account.as_ref()).await?;
        let cells = match &account {
            Some(_) => read_via_sheets_api(&http, &id, tab.as_deref()).await?,
            None => read_via_public_export(&http, &id, tab.as_deref()).await?,
        };
        // A whole tab can hold millions of cells: its rows are built on
        // tokio's blocking pool, off the threads the worker's calls and
        // its other runs share.
        let rows = tokio::task::spawn_blocking(move || rows_from_cells(cells, has_header))
            .await
            .node_err("google sheets: build the tab's rows")?;
        let rows = rows.as_array().map(Vec::as_slice).unwrap_or_default();
        let page = wire_page(rows, offset, max_rows)
            .map_err(|i| heavy_row(&rows[i], i + 1 + usize::from(has_header)))?;
        let next = page.end;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("rows", Value::Array(rows[page].to_vec()))
                .set("nextOffset", next as f64)
                .set("hasMore", next < rows.len()),
        )
        .await
    }
}

/// Signed in: the Sheets API. Two calls: gid to title, then the
/// values (both shared package plumbing).
async fn read_via_sheets_api(http: &ClientWithMiddleware,
    id: &str,
    gid: Option<&str>,
) -> WeftResult<Vec<Vec<String>>> {
    let title = tab_title(http, id, gid).await?;
    read_cells(http, id, &title).await
}

/// Not signed in: the public CSV export, which serves a sheet shared
/// with 'anyone with the link'. With no gid the export serves the
/// first tab.
async fn read_via_public_export(http: &ClientWithMiddleware,
    id: &str,
    gid: Option<&str>,
) -> WeftResult<Vec<Vec<String>>> {
    let mut url =
        format!("https://docs.google.com/spreadsheets/d/{}/export?format=csv", super::api::segment(id));
    if let Some(gid) = gid {
        url.push_str(&format!("&gid={}", urlencoding::encode(gid)));
    }
    let req = http.get(url);
    let csv = checked_send(
        req,
        "export the google sheet (without a connected account, only a sheet shared \
         with 'anyone with the link' can be read)",
    )
    .await?
    .text()
    .await
    .node_err("google sheets: read export")?;
    // On the blocking pool, as the rows are (see `run`).
    tokio::task::spawn_blocking(move || parse_csv_cells(&csv))
        .await
        .node_err("google sheets: parse the export")?
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
