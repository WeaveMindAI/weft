//! StopTagged self-tests: every wired input is a tag to stop, the self
//! choice follows the toggle, and nothing is stopped in the fake tier
//! (there are no siblings); the rig records the asks.

use serde_json::json;

use weft::{FakeRig, NodeTest, StopSelf, WeftResult};

use super::StopTaggedNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("wired_inputs_are_the_tags_to_stop_and_self_is_kept", keep_by_default),
        NodeTest::fake("the_toggle_includes_this_run", include_self),
        NodeTest::fake("no_tags_is_a_failure", no_tags_fails),
    ]
}

async fn keep_by_default(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&StopTaggedNode, json!({ "sender": "user_7", "tags": ["batch_a"] }))
        .await
        .ok()?;
    assert_eq!(
        rig.stops(),
        vec![("user_7".to_string(), StopSelf::Keep), ("batch_a".to_string(), StopSelf::Keep)]
    );
    assert_eq!(outcome.outputs["done"], json!(true));
    assert!(rig.execution_tags().is_empty(), "stopping does not tag");
    Ok(())
}

async fn include_self(rig: FakeRig) -> WeftResult<()> {
    rig.run(&StopTaggedNode, json!({ "exp": "exp_3", "includeSelf": true })).await.ok()?;
    assert_eq!(rig.stops(), vec![("exp_3".to_string(), StopSelf::Include)]);
    Ok(())
}

async fn no_tags_fails(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&StopTaggedNode, json!({})).await.failure()?;
    assert!(err.contains("no tags"), "{err}");
    assert!(rig.stops().is_empty());
    Ok(())
}
