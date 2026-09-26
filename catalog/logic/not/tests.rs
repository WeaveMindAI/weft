//! Not self-tests: each Boolean flips, and a value of any other type
//! is refused rather than read as truthy.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::NotNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("true_becomes_false", true_flips),
        NodeTest::fake("false_becomes_true", false_flips),
        NodeTest::fake("a_non_boolean_is_refused", non_boolean),
    ]
}

async fn true_flips(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&NotNode, json!({ "value": true })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!(false));
    Ok(())
}

async fn false_flips(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&NotNode, json!({ "value": false })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!(true));
    Ok(())
}

/// No truthiness: an empty string, a zero, a word all fail loudly.
async fn non_boolean(rig: FakeRig) -> WeftResult<()> {
    for v in [json!(""), json!(0), json!("false")] {
        let error = rig.run(&NotNode, json!({ "value": v })).await.failure()?;
        assert!(error.contains("value"), "the refusal names the port ({v}): {error}");
    }
    Ok(())
}
