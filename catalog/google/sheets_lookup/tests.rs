//! GoogleSheetsLookup self-tests: matching rows come back keyed by
//! the header, with their 1-based sheet rows.

use serde_json::json;

use weft::{fixture_spec_like, FakeRig, LiveRig, NodeManifest, NodeTest, WeftResult};

use super::super::sheets::values_url;
use super::super::google_sheets_read::GoogleSheetsReadNode;
use super::GoogleSheetsLookupNode;

fn path(url: &str) -> String {
    url.replace("https://sheets.googleapis.com", "")
}

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("finds_matching_rows_with_their_numbers", finds_rows),
        NodeTest::fake("an_unknown_column_fails_loud", unknown_column),
        NodeTest::live("one_real_lookup_of_a_read_back_value", "google", live_lookup)
            .with_fixture(
                // The node's own spreadsheet input, picker widget and all.
                fixture_spec_like(
                    GoogleSheetsLookupNode.manifest(),
                    "spreadsheet",
                    "GOOGLE_SHEET_ID",
                ),
            ),
    ]
}

fn canned_tab(rig: &FakeRig) {
    rig.respond(
        "GET",
        "/v4/spreadsheets/s1?fields=sheets.properties",
        json!({ "sheets": [{ "properties": { "sheetId": 0, "title": "People" } }] }),
    );
    rig.respond(
        "GET",
        &path(&values_url("s1", "People", None)),
        json!({ "values": [
            ["name", "city"],
            ["ada", "london"],
            ["alan", "london"],
            ["grace", "nyc"],
        ]}),
    );
}

async fn finds_rows(rig: FakeRig) -> WeftResult<()> {
    canned_tab(&rig);
    let outcome = rig
        .run(
            &GoogleSheetsLookupNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": "s1",
                "column": "city",
                "value": "london",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["found"], json!(true));
    assert_eq!(outcome.outputs["rowNumbers"], json!([2, 3]), "1-based, past the header");
    assert_eq!(
        outcome.outputs["rows"][0],
        json!({ "name": "ada", "city": "london" })
    );
    Ok(())
}

async fn unknown_column(rig: FakeRig) -> WeftResult<()> {
    canned_tab(&rig);
    let outcome = rig
        .run(
            &GoogleSheetsLookupNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": "s1",
                "column": "nope",
                "value": "x",
            }),
        )
        .await;
    let err = outcome.result.expect_err("unknown column must refuse").to_string();
    assert!(err.contains("no column named 'nope'"), "{err}");
    Ok(())
}

async fn live_lookup(rig: LiveRig) -> WeftResult<()> {
    // Read-only, on the dedicated test spreadsheet (its first tab
    // holds a header row and at least one data row): read a real
    // (column, value) pair off the first data row, then look it up.
    let sheet = rig.fixture("GOOGLE_SHEET_ID")?;
    let read = rig
        .run(
            &GoogleSheetsReadNode,
            json!({ "account": rig.access("google"), "spreadsheet": sheet, "hasHeader": true }),
        )
        .await
        .ok()?;
    let rows = read.output("rows")?.as_array().expect("rows list").clone();
    let first = rows.first().expect("the test sheet holds at least one data row");
    let (column, value) = first
        .as_object()
        .expect("keyed row")
        .iter()
        .find_map(|(k, v)| {
            v.as_str().filter(|s| !s.is_empty()).map(|s| (k.clone(), s.to_string()))
        })
        .expect("the first data row holds at least one non-empty cell");

    let outcome = rig
        .run(
            &GoogleSheetsLookupNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": sheet,
                "column": column,
                "value": value,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("found")?, &json!(true), "the read-back value is found");
    assert!(
        !outcome.output("rowNumbers")?.as_array().expect("row numbers").is_empty(),
        "the match names its sheet row"
    );
    Ok(())
}
