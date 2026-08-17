//! AirtableCreateRecord self-tests.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{delete_record, BASE_FIXTURE, BASE_LABEL, TABLE_FIXTURE, TABLE_LABEL};

use super::AirtableCreateRecordNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_and_emits_the_record", creates),
        NodeTest::live("one_real_create_and_delete", "airtable", live_create)
            .with_fixture(fixture_spec(BASE_FIXTURE, BASE_LABEL.0, BASE_LABEL.1))
            .with_fixture(fixture_spec(TABLE_FIXTURE, TABLE_LABEL.0, TABLE_LABEL.1)),
    ]
}

/// One real record created in the fixture table, then deleted so the
/// table stays empty across runs. Airtable bills nothing for this.
async fn live_create(rig: LiveRig) -> WeftResult<()> {
    let base = rig.fixture(BASE_FIXTURE)?;
    let table = rig.fixture(TABLE_FIXTURE)?;
    let outcome = rig
        .run(
            &AirtableCreateRecordNode,
            json!({
                "account": rig.access("airtable"),
                "base": base,
                "table": table,
                "fields": { "Name": "weft live test (safe to delete)" },
                "typecast": false,
            }),
        )
        .await
        .ok()?;
    let record_id = outcome.output("recordId")?.as_str().unwrap_or_default().to_string();
    assert!(record_id.starts_with("rec"), "a real record id came back: {record_id}");
    let conn = rig.connect().await?;
    delete_record(&conn, &base, &table, &record_id).await
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
