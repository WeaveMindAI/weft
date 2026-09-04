//! TagRun self-tests: every wired input is a tag, the list adds to them,
//! and a node with nothing to tag fails instead of tagging nothing.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::TagRunNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("every_wired_input_is_a_tag", wired_inputs_are_tags),
        NodeTest::fake("the_list_and_the_inputs_add_up", list_and_inputs_add_up),
        NodeTest::fake("a_bad_tag_fails_before_tagging", bad_tag_fails),
        NodeTest::fake("no_tags_is_a_failure", no_tags_fails),
    ]
}

async fn wired_inputs_are_tags(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&TagRunNode, json!({ "sender": "user_7", "channel": "support" }))
        .await
        .ok()?;
    let mut tagged = rig.execution_tags();
    assert_eq!(tagged.len(), 1, "one tag_execution call: {tagged:?}");
    let mut tags = tagged.remove(0);
    tags.sort();
    assert_eq!(tags, vec!["support".to_string(), "user_7".to_string()]);
    assert_eq!(outcome.outputs["done"], json!(true));
    let mut emitted: Vec<String> = serde_json::from_value(outcome.outputs["tags"].clone()).unwrap();
    emitted.sort();
    assert_eq!(emitted, vec!["support".to_string(), "user_7".to_string()]);
    Ok(())
}

/// The declared list and the created ports are one set, a tag named
/// twice counted once.
async fn list_and_inputs_add_up(rig: FakeRig) -> WeftResult<()> {
    rig.run(&TagRunNode, json!({ "sender": "user_7", "tags": ["batch_a", "user_7"] }))
        .await
        .ok()?;
    let tagged = rig.execution_tags();
    assert_eq!(tagged, vec![vec!["user_7".to_string(), "batch_a".to_string()]]);
    Ok(())
}

/// The ctx refuses a tag outside `[A-Za-z0-9_-]{1,64}` before anything
/// is written, and the node surfaces that as its failure.
async fn bad_tag_fails(rig: FakeRig) -> WeftResult<()> {
    // `failure()` is the mirror of `ok()`: the message the body refused with.
    let err = rig.run(&TagRunNode, json!({ "sender": "+33 6 12" })).await.failure()?;
    assert!(err.contains("invalid character"), "{err}");
    assert!(rig.execution_tags().is_empty(), "nothing was tagged");
    Ok(())
}

async fn no_tags_fails(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&TagRunNode, json!({})).await.failure()?;
    assert!(err.contains("no tags"), "{err}");
    Ok(())
}
