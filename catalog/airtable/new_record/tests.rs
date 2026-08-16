//! AirtableNewRecord self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::AirtableNewRecordNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("registers_the_sorted_delta_poll", registers),
        NodeTest::fake("a_fire_emits_the_record", fires),
    ]
}

async fn registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &AirtableNewRecordNode,
        json!({
            "account": rig.access("airtable"),
            "base": "appB",
            "table": "tblT",
            "createdField": "Created",
            "intervalSecs": 30,
        }),
    )
    .await
    .ok()?;
    let signals = rig.registered_signals();
    assert_eq!(signals.len(), 1);
    let spec = serde_json::to_value(&signals[0].0).expect("spec serializes").to_string();
    assert!(spec.contains("desc"), "newest first: {spec}");
    assert!(spec.contains("Created"), "{spec}");
    assert!(spec.contains("\"mode\":\"set\""), "{spec}");
    Ok(())
}

async fn fires(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({
        "item": { "id": "recNew", "createdTime": "2026-08-13T00:00:00.000Z",
                  "fields": { "Name": "Acme" } }
    }));
    let outcome = rig
        .run(
            &AirtableNewRecordNode,
            json!({
                "account": rig.access("airtable"),
                "base": "appB",
                "table": "tblT",
                "createdField": "Created",
                "intervalSecs": 30,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["recordId"], json!("recNew"));
    assert_eq!(outcome.outputs["fields"]["Name"], json!("Acme"));
    Ok(())
}
