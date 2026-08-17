//! AirtableUpdateRecord self-tests.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{
    delete_record, seed_record, unique_name, BASE_FIXTURE, BASE_LABEL, TABLE_FIXTURE, TABLE_LABEL,
};

use super::AirtableUpdateRecordNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("patches_and_emits_the_record", updates),
        NodeTest::live("one_real_update_and_delete", "airtable", live_update)
            .with_fixture(fixture_spec(BASE_FIXTURE, BASE_LABEL.0, BASE_LABEL.1))
            .with_fixture(fixture_spec(TABLE_FIXTURE, TABLE_LABEL.0, TABLE_LABEL.1)),
    ]
}

/// One real update of a freshly seeded record, then the record is
/// deleted so the table stays empty. Airtable bills nothing for this.
async fn live_update(rig: LiveRig) -> WeftResult<()> {
    let base = rig.fixture(BASE_FIXTURE)?;
    let table = rig.fixture(TABLE_FIXTURE)?;
    let conn = rig.connect().await?;
    let record_id = seed_record(&conn, &base, &table, &unique_name("weft update test")).await?;
    let renamed = unique_name("weft updated");
    let outcome = rig
        .run(
            &AirtableUpdateRecordNode,
            json!({
                "account": rig.access("airtable"),
                "base": base,
                "table": table,
                "recordId": record_id,
                "fields": { "Name": renamed },
                "typecast": false,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("recordId")?.as_str(), Some(record_id.as_str()));
    delete_record(&conn, &base, &table, &record_id).await
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
