//! HumanTrigger self-tests: the registered form and the submission's
//! port mapping (the approve/reject split closes the losing side).

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::HumanTriggerNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_form", setup_registers),
        NodeTest::fake("an_approval_pulses_only_the_approved_port", approval_maps),
    ]
}

fn fields() -> serde_json::Value {
    json!([{ "fieldType": "approve_reject", "key": "decision" }])
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &HumanTriggerNode,
        json!({ "title": "Ship it?", "fields": fields() }),
    )
    .await
    .ok()?;
    let spec = serde_json::to_value(&rig.registered_signals()[0].0)
        .expect("spec serializes")
        .to_string();
    assert!(spec.contains("human-trigger"), "{spec}");
    assert!(spec.contains("Ship it?"), "{spec}");
    assert!(spec.contains("decision"), "the field rides the schema: {spec}");
    Ok(())
}

async fn approval_maps(rig: FakeRig) -> WeftResult<()> {
    // The compiler's hasFormSchema merge derives these ports; the rig
    // plays that role.
    rig.output_type("decision_approved", WeftType::parse("Boolean").expect("Boolean parses"));
    rig.output_type("decision_rejected", WeftType::parse("Boolean").expect("Boolean parses"));
    rig.wake(json!({ "decision": true }));
    let outcome = rig
        .run(&HumanTriggerNode, json!({ "fields": fields() }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["decision_approved"], json!(true));
    assert!(
        !outcome.outputs.contains_key("decision_rejected"),
        "the losing side stays un-mentioned (structural closure, not a false pulse)"
    );
    Ok(())
}
