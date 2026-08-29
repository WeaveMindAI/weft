//! Shared Google Sheets plumbing for every sheets node in this
//! package: gid-to-title resolution (the tab dropdown stores the gid,
//! the values API only speaks tab titles), cell reads, and the
//! cells-to-rows shaping the read paths share.

use serde_json::{Map, Value};

use weft::access::client::get_json;
use weft::reqwest_middleware::ClientWithMiddleware;
use weft::{node_bail, WeftResult};

/// Resolve a tab's gid to its title (the only addressing
/// `values.get` / `values.append` accept). Fails loudly on an
/// unknown gid: the tab was deleted or the wrong sheet is picked.
pub async fn tab_title(http: &ClientWithMiddleware,
    id: &str,
    gid: &str,
) -> WeftResult<String> {
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
    match title {
        Some(t) => Ok(t),
        None => node_bail!("the spreadsheet has no tab with gid {gid}; pick the tab again"),
    }
}

/// The values URL for a whole tab (or an A1 range within it when
/// `range` is set), correctly quoted and encoded. An apostrophe
/// INSIDE the tab title is doubled per the A1 spec (`'Bob''s Data'`);
/// without it, a tab named `Bob's Data` would parse as the tab `Bob`
/// followed by garbage, across every node that addresses cells.
pub fn values_url(id: &str, title: &str, range: Option<&str>) -> String {
    let quoted_title = format!("'{}'", title.replace('\'', "''"));
    let addr = match range {
        Some(r) => format!("{quoted_title}!{r}"),
        None => quoted_title,
    };
    format!(
        "https://sheets.googleapis.com/v4/spreadsheets/{id}/values/{}",
        urlencoding::encode(&addr)
    )
}

/// The header row that orders a KEYED (object) row, `None` for a list
/// row. A keyed row without 'First row is a header', or against an
/// empty tab, fails loudly: inventing a column order would write
/// values into the wrong columns. Shared by append and update so the
/// rule cannot drift between them.
pub async fn header_for_row(http: &ClientWithMiddleware,
    id: &str,
    title: &str,
    row: &Value,
    has_header: bool,
) -> WeftResult<Option<Vec<String>>> {
    if !row.is_object() {
        return Ok(None);
    }
    if !has_header {
        node_bail!(
            "the row is keyed by column name but 'First row is a header' is off; \
             order is undefined. Pass a list of cells, or turn the header on"
        );
    }
    let cells = read_cells(http, id, title).await?;
    match cells.into_iter().next() {
        Some(h) => Ok(Some(h)),
        None => node_bail!(
            "the tab is empty: there is no header row to order the keyed row by"
        ),
    }
}

/// Read a tab's cells as a string grid via the Sheets API.
pub async fn read_cells(http: &ClientWithMiddleware,
    id: &str,
    title: &str,
) -> WeftResult<Vec<Vec<String>>> {
    let values: Value = get_json(http, &values_url(id, title, None), "read the tab's values").await?;
    Ok(values["values"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| row_to_strings(row))
        .collect())
}

/// One values-API row (an array of cells) to strings.
pub fn row_to_strings(row: &Value) -> Vec<String> {
    row.as_array()
        .into_iter()
        .flatten()
        .map(|cell| match cell {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .collect()
}

/// Cell grid -> rows of objects. With a header, keys are the first
/// row's cells; without one, positional `c0, c1, ...`. Shared by every
/// read path so they answer identically.
pub fn rows_from_cells(cells: Vec<Vec<String>>, has_header: bool) -> Value {
    let mut iter = cells.into_iter();
    let headers: Option<Vec<String>> = if has_header { iter.next() } else { None };
    let rows = iter
        .map(|record| Value::Object(row_object(record, headers.as_deref())))
        .collect();
    Value::Array(rows)
}

/// One row of cells to an object, keyed by the header (positional
/// `c<i>` for cells past it, or entirely without one).
pub fn row_object(cells: Vec<String>, headers: Option<&[String]>) -> Map<String, Value> {
    let mut row = Map::new();
    for (i, cell) in cells.into_iter().enumerate() {
        let key = headers
            .and_then(|h| h.get(i).cloned())
            .unwrap_or_else(|| format!("c{i}"));
        row.insert(key, Value::String(cell));
    }
    row
}

/// A row VALUE a node received (an object keyed by header, or a plain
/// list) to the positional cell list the values API wants. An object
/// needs the header to order by; a key naming no header column is a
/// loud error (a silent drop would lose the user's data).
pub fn row_to_cells(row: &Value, headers: Option<&[String]>) -> WeftResult<Vec<Value>> {
    match row {
        Value::Array(cells) => Ok(cells.clone()),
        Value::Object(map) => {
            let Some(headers) = headers else {
                node_bail!(
                    "the row is an object keyed by column name, but the sheet has no header \
                     row to order by; pass a list of cells instead (or add a header)"
                );
            };
            for key in map.keys() {
                if !headers.iter().any(|h| h == key) {
                    node_bail!(
                        "the row names a column '{key}' the sheet's header does not have \
                         (header: {headers:?})"
                    );
                }
            }
            Ok(headers
                .iter()
                .map(|h| map.get(h).cloned().unwrap_or(Value::String(String::new())))
                .collect())
        }
        other => node_bail!(
            "a row is an object keyed by column name or a list of cells, got: {other}"
        ),
    }
}

// This file is a package-level SHARED helper, not a node, so its unit
// tests stay an ordinary `#[cfg(test)]` block (node self-tests in a
// `tests.rs` belong to nodes; see docs/src/nodes/testing.md).
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn object_rows_order_by_the_header() {
        let headers = vec!["name".to_string(), "age".to_string()];
        let cells =
            row_to_cells(&json!({ "age": "36", "name": "ada" }), Some(&headers)).unwrap();
        assert_eq!(cells, vec![json!("ada"), json!("36")]);
        // A column the row doesn't mention stays an empty cell.
        let cells = row_to_cells(&json!({ "name": "grace" }), Some(&headers)).unwrap();
        assert_eq!(cells, vec![json!("grace"), json!("")]);
    }

    #[test]
    fn unknown_columns_and_headerless_objects_are_loud() {
        let headers = vec!["name".to_string()];
        let err = row_to_cells(&json!({ "nom": "ada" }), Some(&headers)).unwrap_err();
        assert!(err.to_string().contains("nom"), "{err}");
        let err = row_to_cells(&json!({ "name": "ada" }), None).unwrap_err();
        assert!(err.to_string().contains("header"), "{err}");
    }

    #[test]
    fn list_rows_pass_through() {
        let cells = row_to_cells(&json!(["a", 2, true]), None).unwrap();
        assert_eq!(cells, vec![json!("a"), json!(2), json!(true)]);
    }

    #[test]
    fn tab_titles_with_apostrophes_are_a1_escaped() {
        let url = values_url("sheet1", "Bob's Data", None);
        let encoded = urlencoding::encode("'Bob''s Data'");
        assert!(url.ends_with(&*encoded), "{url}");
        let plain = values_url("sheet1", "Data", Some("A1:B2"));
        assert!(plain.ends_with(&*urlencoding::encode("'Data'!A1:B2")), "{plain}");
    }
}
