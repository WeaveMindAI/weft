//! AirtableCreateRecord self-tests.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{delete_record, BASE_FIXTURE, BASE_LABEL, TABLE_FIXTURE, TABLE_LABEL};

use super::AirtableCreateRecordNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_and_emits_the_record", creates),
        NodeTest::fake("a_refused_record_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_record_comes_out_on_error_when_it_is_wired", refused_wired),
        NodeTest::fake("a_bad_table_id_still_fails_the_run_when_error_is_wired", bad_id_wired),
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

/// Airtable refuses the new record: a field name the table lacks.
fn refuse_the_record(rig: &FakeRig) {
    rig.respond_status(
        "POST",
        "/v0/appBase1/tblTable1",
        422,
        json!({ "error": { "type": "UNKNOWN_FIELD_NAME", "message": "Unknown field name: \"Nme\"" } }),
    );
}

fn record_inputs(rig: &FakeRig, table: &str) -> serde_json::Value {
    json!({
        "account": rig.access("airtable"),
        "base": "appBase1",
        "table": table,
        "fields": { "Nme": "Acme" },
        "typecast": false,
    })
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_record(&rig);
    let err = rig
        .run(&AirtableCreateRecordNode, record_inputs(&rig, "tblTable1"))
        .await
        .failure()?;
    assert!(err.contains("Unknown field name"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_record(&rig);
    rig.wire_output("error");
    let outcome = rig
        .run(&AirtableCreateRecordNode, record_inputs(&rig, "tblTable1"))
        .await
        .ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("Unknown field name"), "{error}");
    for port in ["recordId", "record"] {
        assert!(!outcome.outputs.contains_key(port), "a caught failure emits nothing on {port}");
    }
    Ok(())
}

/// A table id with a `/` is a mistake in the program, never a value
/// for `error`.
async fn bad_id_wired(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    let err = rig
        .run(&AirtableCreateRecordNode, record_inputs(&rig, "tbl/../x"))
        .await
        .failure()?;
    assert!(err.starts_with("input error"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}
