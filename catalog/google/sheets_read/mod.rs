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
use serde_json::{Map, Value};

use weft::node::NodeOutput;
use weft::access::client::get_json;
use weft::reqwest_middleware::ClientWithMiddleware;
use weft::{node_bail, Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GoogleSheetsReadNode;

#[async_trait]
impl Node for GoogleSheetsReadNode {
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

/// Signed in: the Sheets API. Two calls: the sheet list (to turn the
/// gid into the tab's title, the only addressing `values.get` accepts),
/// then the values themselves.
async fn read_via_sheets_api(http: &ClientWithMiddleware,
    id: &str,
    gid: &str,
) -> WeftResult<Vec<Vec<String>>> {
    let meta: Value = get_json(
        http,
        &format!("https://sheets.googleapis.com/v4/spreadsheets/{id}?fields=sheets.properties"),
        "list the sheet's tabs",
    )
    .await?;
    let title = meta["sheets"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|s| &s["properties"])
        .find(|p| p["sheetId"].as_i64().map(|g| g.to_string()).as_deref() == Some(gid))
        .and_then(|p| p["title"].as_str().map(str::to_string));
    let Some(title) = title else {
        node_bail!("the spreadsheet has no tab with gid {gid}; pick the tab again");
    };
    let values: Value = get_json(
        http,
        &format!(
            "https://sheets.googleapis.com/v4/spreadsheets/{id}/values/{}",
            urlencoding::encode(&format!("'{title}'"))
        ),
        "read the tab's values",
    )
    .await?;
    let cells = values["values"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| {
            row.as_array()
                .into_iter()
                .flatten()
                .map(|cell| match cell {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect()
        })
        .collect();
    Ok(cells)
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

/// Cell grid -> rows of objects. With a header, keys are the first
/// row's cells; without one, positional `c0, c1, ...`. Shared by both
/// read paths so they answer identically.
fn rows_from_cells(cells: Vec<Vec<String>>, has_header: bool) -> Value {
    let mut iter = cells.into_iter();
    let headers: Option<Vec<String>> = if has_header { iter.next() } else { None };
    let rows = iter
        .map(|record| {
            let mut row = Map::new();
            for (i, cell) in record.into_iter().enumerate() {
                let key = match &headers {
                    Some(h) => h.get(i).cloned().unwrap_or_else(|| format!("c{i}")),
                    None => format!("c{i}"),
                };
                row.insert(key, Value::String(cell));
            }
            Value::Object(row)
        })
        .collect();
    Value::Array(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(csv: &str, has_header: bool) -> Value {
        rows_from_cells(parse_csv_cells(csv).unwrap(), has_header)
    }

    #[test]
    fn rows_parse_with_and_without_a_header() {
        let csv = "name,age\nada,36\ngrace,45\n";
        let with = rows(csv, true);
        assert_eq!(with[0]["name"], "ada");
        assert_eq!(with[1]["age"], "45");

        let without = rows(csv, false);
        assert_eq!(without[0]["c0"], "name", "no header: the first row is data");
        assert_eq!(without[2]["c1"], "45");
    }

    /// A ragged row (fewer/more cells than the header) still parses;
    /// extra cells get positional keys rather than being dropped.
    #[test]
    fn ragged_rows_keep_their_cells() {
        let csv = "a,b\n1\n2,3,4\n";
        let r = rows(csv, true);
        assert_eq!(r[0]["a"], "1");
        assert!(r[0].get("b").is_none());
        assert_eq!(r[1]["c2"], "4", "an extra cell keeps a positional key");
    }
}
