//! GoogleSheetsRead self-tests. `basic` pins the pure cells-to-rows
//! step through the CSV parser; `fake` runs the signed-in branch
//! against canned Sheets-API answers (gid -> title, then values).

use serde_json::{json, Value};

use weft::{fixture_spec_like, FakeRig, LiveRig, NodeManifest, NodeTest, WeftResult};

use super::{parse_csv_cells, GoogleSheetsReadNode};
use super::super::sheets::rows_from_cells;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("rows_parse_with_and_without_a_header", || {
            let csv = "name,age\nada,36\ngrace,45\n";
            let with = rows(csv, true)?;
            assert_eq!(with[0]["name"], "ada");
            assert_eq!(with[1]["age"], "45");

            let without = rows(csv, false)?;
            assert_eq!(without[0]["c0"], "name", "no header: the first row is data");
            assert_eq!(without[2]["c1"], "45");
            Ok(())
        }),
        NodeTest::basic("ragged_rows_keep_their_cells", || {
            // A ragged row (fewer/more cells than the header) still
            // parses; extra cells get positional keys, never dropped.
            let csv = "a,b\n1\n2,3,4\n";
            let r = rows(csv, true)?;
            assert_eq!(r[0]["a"], "1");
            assert!(r[0].get("b").is_none());
            assert_eq!(r[1]["c2"], "4", "an extra cell keeps a positional key");
            Ok(())
        }),
        NodeTest::fake("signed_in_read_emits_header_keyed_rows", signed_in_read),
        NodeTest::live("one_real_signed_in_read", "google", live_read).with_fixture(
            // The node's own spreadsheet input, picker widget and all.
            fixture_spec_like(GoogleSheetsReadNode.manifest(), "spreadsheet", "GOOGLE_SHEET_ID"),
        ),
    ]
}

fn rows(csv: &str, has_header: bool) -> WeftResult<Value> {
    Ok(rows_from_cells(parse_csv_cells(csv)?, has_header))
}

async fn signed_in_read(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v4/spreadsheets/sheet-1?fields=sheets.properties",
        json!({ "sheets": [
            { "properties": { "sheetId": 0, "title": "People" } },
        ]}),
    );
    // The whole-tab values URL quotes the title: '<title>'.
    rig.respond(
        "GET",
        "/v4/spreadsheets/sheet-1/values/%27People%27",
        json!({ "values": [["name", "age"], ["ada", "36"]] }),
    );

    let outcome = rig
        .run(
            &GoogleSheetsReadNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": "sheet-1",
                "tab": "0",
                "hasHeader": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["rows"], json!([{ "name": "ada", "age": "36" }]));
    Ok(())
}

async fn live_read(rig: LiveRig) -> WeftResult<()> {
    // Read-only, on the dedicated test spreadsheet (its first tab
    // holds a header row and at least one data row).
    let sheet = rig.fixture("GOOGLE_SHEET_ID")?;
    let outcome = rig
        .run(
            &GoogleSheetsReadNode,
            json!({ "account": rig.access("google"), "spreadsheet": sheet, "hasHeader": true }),
        )
        .await
        .ok()?;
    let rows = outcome.output("rows")?.as_array().expect("rows list").clone();
    assert!(!rows.is_empty(), "the test sheet holds at least one data row");
    assert!(rows[0].is_object(), "with a header, rows come back keyed");
    Ok(())
}
