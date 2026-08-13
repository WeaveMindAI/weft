//! GoogleSheetsNewRow self-tests: the registered values poll and a
//! fire's row decomposition.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::sheets::values_url;
use super::GoogleSheetsNewRowNode;

fn path(url: &str) -> String {
    url.replace("https://sheets.googleapis.com", "")
}

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_values_poll", setup_registers),
        NodeTest::fake("a_fire_emits_the_header_keyed_row", fire_emits_row),
    ]
}

fn canned_tab(rig: &FakeRig) {
    rig.respond(
        "GET",
        "/v4/spreadsheets/s1?fields=sheets.properties",
        json!({ "sheets": [{ "properties": { "sheetId": 0, "title": "Leads" } }] }),
    );
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    canned_tab(&rig);
    rig.run_setup_trigger(
        &GoogleSheetsNewRowNode,
        json!({ "account": rig.access("google"), "spreadsheet": "s1" }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes").to_string();
    assert!(spec.contains(&values_url("s1", "Leads", None)), "polls the tab's values: {spec}");
    Ok(())
}

async fn fire_emits_row(rig: FakeRig) -> WeftResult<()> {
    canned_tab(&rig);
    rig.respond(
        "GET",
        &path(&values_url("s1", "Leads", None)),
        json!({ "values": [["name", "email"], ["ada", "ada@example.com"]] }),
    );
    rig.wake(json!({ "item": ["ada", "ada@example.com"], "index": 1 }));
    let outcome = rig
        .run(
            &GoogleSheetsNewRowNode,
            json!({ "account": rig.access("google"), "spreadsheet": "s1" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["cells"], json!(["ada", "ada@example.com"]));
    assert_eq!(outcome.outputs["rowNumber"], json!(2.0), "0-based index to 1-based row");
    assert_eq!(
        outcome.outputs["row"],
        json!({ "name": "ada", "email": "ada@example.com" })
    );
    Ok(())
}
