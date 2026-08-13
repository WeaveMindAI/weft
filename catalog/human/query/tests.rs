//! HumanQuery self-tests: park on the form, resume on the canned
//! submission, map to ports.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::HumanQueryNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("a_submission_resumes_and_maps_to_ports", submission_maps)]
}

async fn submission_maps(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("decision_approved", WeftType::parse("Boolean").expect("Boolean parses"));
    rig.output_type("decision_rejected", WeftType::parse("Boolean").expect("Boolean parses"));
    // The canned form submission that resumes the park.
    rig.signal(json!({ "decision": false }));
    let outcome = rig
        .run(
            &HumanQueryNode,
            json!({
                "title": "Ship it?",
                "fields": [{ "fieldType": "approve_reject", "key": "decision" }],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["decision_rejected"], json!(true));
    assert!(!outcome.outputs.contains_key("decision_approved"));
    Ok(())
}
