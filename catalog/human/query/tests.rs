//! HumanQuery self-tests: park on the form, resume on the canned
//! submission, map to ports.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::HumanQueryNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_submission_resumes_and_maps_to_ports", submission_maps),
        NodeTest::fake("a_fields_label_and_placeholder_reach_the_form", label_and_placeholder),
    ]
}

/// `label` and `placeholder` are on every kind that takes them without
/// the metadata declaring them per kind, so the form a person sees has
/// to carry both.
async fn label_and_placeholder(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("email", WeftType::parse("String").expect("String parses"));
    rig.signal(json!({ "email": "someone@example.com" }));
    rig.run(
        &HumanQueryNode,
        json!({
            "title": "Who shall we reply to?",
            "fields": [{
                "kind": "text_input",
                "key": "email",
                "label": "Where do we reply?",
                "placeholder": "you@example.com"
            }],
        }),
    )
    .await
    .ok()?;

    let parked = rig.awaited_signals();
    let spec = parked.first().expect("the node parked on a form");
    let field = &spec.config["schema"]["fields"][0];
    assert_eq!(field["label"], json!("Where do we reply?"));
    assert_eq!(field["config"]["placeholder"], json!("you@example.com"));
    assert!(
        field["config"].get("label").is_none(),
        "the label is its own column on the wire, never repeated in config"
    );
    Ok(())
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
                "fields": [{ "kind": "approve_reject", "key": "decision" }],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["decision_rejected"], json!(true));
    assert!(!outcome.outputs.contains_key("decision_approved"));
    Ok(())
}
