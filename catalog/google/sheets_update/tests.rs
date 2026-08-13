//! GoogleSheetsUpdate self-tests: the two addressings and their
//! exclusivity.

use serde_json::json;

use weft::{fixture_spec_like, FakeRig, LiveRig, NodeManifest, NodeTest, WeftResult};

use super::super::sheets::values_url;
use super::super::google_sheets_append::GoogleSheetsAppendNode;
use super::super::google_sheets_read::GoogleSheetsReadNode;
use super::GoogleSheetsUpdateNode;

fn path(url: &str) -> String {
    url.replace("https://sheets.googleapis.com", "")
}

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_row_number_writes_the_whole_row", by_row_number),
        NodeTest::fake("both_addressings_at_once_refuse", both_addressings),
        NodeTest::live("one_real_overwrite_read_back", "google", live_update).with_fixture(
            // The node's own spreadsheet input, picker widget and all.
            fixture_spec_like(
                GoogleSheetsUpdateNode.manifest(),
                "spreadsheet",
                "GOOGLE_SHEET_ID",
            ),
        ),
    ]
}

async fn by_row_number(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v4/spreadsheets/s1?fields=sheets.properties",
        json!({ "sheets": [{ "properties": { "sheetId": 0, "title": "Log" } }] }),
    );
    rig.respond(
        "PUT",
        &format!(
            "{}?valueInputOption=USER_ENTERED",
            path(&values_url("s1", "Log", Some("A5")))
        ),
        json!({}),
    );
    let outcome = rig
        .run(
            &GoogleSheetsUpdateNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": "s1",
                "row": ["ada", "37"],
                "rowNumber": 5,
                "hasHeader": false,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.requests()[1].body.clone().expect("update body");
    assert_eq!(body["values"], json!([["ada", "37"]]));
    Ok(())
}

async fn both_addressings(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v4/spreadsheets/s1?fields=sheets.properties",
        json!({ "sheets": [{ "properties": { "sheetId": 0, "title": "Log" } }] }),
    );
    let outcome = rig
        .run(
            &GoogleSheetsUpdateNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": "s1",
                "row": ["x"],
                "rowNumber": 5,
                "range": "B2:C2",
                "hasHeader": false,
            }),
        )
        .await;
    let err = outcome.result.expect_err("two addressings must refuse").to_string();
    assert!(err.contains("ONE addressing"), "{err}");
    Ok(())
}

async fn live_update(rig: LiveRig) -> WeftResult<()> {
    // Append a scratch row to the dedicated test spreadsheet, overwrite
    // it with a marker, read the marker back, then blank the row: the
    // values API's append lands after the last row holding data, so a
    // blanked trailing row is reused and repeated runs never grow the
    // sheet.
    let sheet = rig.fixture("GOOGLE_SHEET_ID")?;
    let appended = rig
        .run(
            &GoogleSheetsAppendNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": sheet,
                "row": ["weft-node-tests", "before"],
            }),
        )
        .await
        .ok()?;
    let row_number = appended.output("rowNumber")?.as_f64().expect("row number");

    let marker = format!("weft-node-tests {}", uuid::Uuid::new_v4());
    let updated = rig
        .run(
            &GoogleSheetsUpdateNode,
            json!({
                "account": rig.access("google"),
                "spreadsheet": sheet,
                "row": ["weft-node-tests", marker],
                "rowNumber": row_number,
            }),
        )
        .await
        .ok()?;
    assert_eq!(updated.output("done")?, &json!(true));

    let read = rig
        .run(
            &GoogleSheetsReadNode,
            json!({ "account": rig.access("google"), "spreadsheet": sheet, "hasHeader": true }),
        )
        .await
        .ok()?;
    let landed = read
        .output("rows")?
        .as_array()
        .expect("rows list")
        .iter()
        .any(|row| {
            row.as_object()
                .into_iter()
                .flatten()
                .any(|(_, v)| v.as_str() == Some(marker.as_str()))
        });
    assert!(landed, "the overwrite reads back");

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
    assert_eq!(blanked.output("done")?, &json!(true), "the scratch row is blanked back");
    Ok(())
}
