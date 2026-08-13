//! GoogleSheetsAppend self-tests: the append call and the emitted
//! 1-based row number.

use serde_json::json;

use weft::{fixture_spec_like, FakeRig, LiveRig, NodeManifest, NodeTest, WeftResult};

use super::super::sheets::values_url;
use super::super::google_sheets_update::GoogleSheetsUpdateNode;
use super::GoogleSheetsAppendNode;

fn path(url: &str) -> String {
    url.replace("https://sheets.googleapis.com", "")
}

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("appends_a_list_row_and_reads_back_its_number", appends),
        NodeTest::live("one_real_append_then_blanked", "google", live_append).with_fixture(
            // The node's own spreadsheet input, picker widget and all.
            fixture_spec_like(
                GoogleSheetsAppendNode.manifest(),
                "spreadsheet",
                "GOOGLE_SHEET_ID",
            ),
        ),
    ]
}

async fn appends(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v4/spreadsheets/s1?fields=sheets.properties",
        json!({ "sheets": [{ "properties": { "sheetId": 0, "title": "Log" } }] }),
    );
    rig.respond(
        "POST",
        &format!(
            "{}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS",
            path(&values_url("s1", "Log", None))
        ),
        json!({ "updates": { "updatedRange": "'Log'!A5:B5" } }),
    );
    let outcome = rig
        .run(
            &GoogleSheetsAppendNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": "s1",
                "row": ["ada", "36"],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["rowNumber"], json!(5.0), "parsed off updatedRange");
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.requests()[1].body.clone().expect("append body");
    assert_eq!(body["values"], json!([["ada", "36"]]));
    Ok(())
}

async fn live_append(rig: LiveRig) -> WeftResult<()> {
    // Append a marker row to the dedicated test spreadsheet, then
    // blank it through the update node: the values API's append lands
    // after the last row holding data, so a blanked trailing row is
    // reused and repeated runs never grow the sheet.
    let sheet = rig.fixture("GOOGLE_SHEET_ID")?;
    let marker = format!("weft-node-tests {}", uuid::Uuid::new_v4());
    let outcome = rig
        .run(
            &GoogleSheetsAppendNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": sheet,
                "row": ["weft-node-tests", marker],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("done")?, &json!(true));
    let row_number = outcome.output("rowNumber")?.as_f64().expect("row number");
    assert!(row_number >= 2.0, "the append landed past the header, got row {row_number}");

    let blanked = rig
        .run(
            &GoogleSheetsUpdateNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": sheet,
                "row": ["", ""],
                "rowNumber": row_number,
            }),
        )
        .await
        .ok()?;
    assert_eq!(blanked.output("done")?, &json!(true), "the appended row is blanked back");
    Ok(())
}
