//! HumanQuery self-tests: park on the form, resume on the canned
//! submission, map to ports.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::HumanQueryNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("an_image_field_parks_the_stored_file_for_the_files_door", image_parked),
        NodeTest::fake("a_submission_resumes_and_maps_to_ports", submission_maps),
        NodeTest::fake("a_fields_label_and_placeholder_reach_the_form", label_and_placeholder),
    ]
}

/// The parked spec keeps the stored file itself (weft's side of the
/// signal): the listener turns it into the file's facts for the
/// consumer, and the files door mints the link when the form is shown.
async fn image_parked(rig: FakeRig) -> WeftResult<()> {
    let image = rig.store_file("cat.png", "image/png", b"PNGDATA".to_vec());
    rig.output_type("ok_approved", WeftType::parse("Boolean").expect("Boolean parses"));
    rig.output_type("ok_rejected", WeftType::parse("Boolean").expect("Boolean parses"));
    rig.signal(json!({ "ok": true }));
    rig.run(
        &HumanQueryNode,
        json!({
            "title": "Look",
            "fields": [
                { "kind": "display_image", "key": "picture" },
                { "kind": "approve_reject", "key": "ok" }
            ],
            "picture": image,
        }),
    )
    .await
    .ok()?;
    let parked = rig.awaited_signals();
    let spec = parked.first().expect("the node parked on a form");
    let field = &spec.config["schema"]["fields"][0];
    assert_eq!(field["key"], json!("picture"));
    assert!(field["value"]["__weft_image__"].get("key").is_some(), "the parked spec holds the stored file: {}", field["value"]);
    Ok(())
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
