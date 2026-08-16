//! AirtableUpdateRecord self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::AirtableUpdateRecordNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("patches_and_emits_the_record", updates)]
}

async fn updates(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "PATCH",
        "/v0/appBase1/tblTable1/rec9",
        json!({ "id": "rec9", "fields": { "Stage": "Won" } }),
    );
    let outcome = rig
        .run(
            &AirtableUpdateRecordNode,
            json!({
                "account": rig.access("airtable"),
                "base": "appBase1",
                "table": "tblTable1",
                "recordId": "rec9",
                "fields": { "Stage": "Won" },
                "typecast": false,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["recordId"], json!("rec9"));
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert!(body.get("typecast").is_none(), "typecast off sends no key");
    Ok(())
}
