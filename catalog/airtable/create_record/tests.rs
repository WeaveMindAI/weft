//! AirtableCreateRecord self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::AirtableCreateRecordNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("creates_and_emits_the_record", creates)]
}

async fn creates(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v0/appBase1/tblTable1",
        json!({ "id": "recNew", "createdTime": "2026-08-13T00:00:00.000Z",
                "fields": { "Name": "Acme" } }),
    );
    let outcome = rig
        .run(
            &AirtableCreateRecordNode,
            json!({
                "account": rig.access("airtable"),
                "base": "appBase1",
                "table": "tblTable1",
                "fields": { "Name": "Acme" },
                "typecast": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["recordId"], json!("recNew"));
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(body["fields"]["Name"], json!("Acme"));
    assert_eq!(body["typecast"], json!(true));
    Ok(())
}
