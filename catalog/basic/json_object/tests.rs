//! JsonObject self-tests: the wired keys are the object, written order
//! survives, a value of any shape goes in whole, and a node nobody
//! wired is refused rather than emitting `{}`.
//!
//! The "an optional key stayed silent" case is not here and cannot be:
//! whether an absent input skips the node or arrives absent is the
//! engine's decision, taken before this body runs, and it is proved in
//! the engine's own skip tests. What this file pins is that whatever
//! DOES arrive lands under its own name, unchanged.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::JsonObjectNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_wired_keys_are_the_object", keys_are_the_object),
        NodeTest::fake("keys_keep_the_order_they_were_written", written_order),
        NodeTest::fake("a_value_of_any_shape_goes_in_whole", nested_value),
        NodeTest::fake("nothing_wired_is_refused", nothing_wired),
    ]
}

async fn keys_are_the_object(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&JsonObjectNode, json!({ "job": "abc", "status": "running" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["object"], json!({ "job": "abc", "status": "running" }));
    Ok(())
}

/// A reply body is read by a person in a network tab, so the keys come
/// out where the author put them rather than sorted.
async fn written_order(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&JsonObjectNode, json!({ "zebra": 1, "apple": 2, "moose": 3 }))
        .await
        .ok()?;
    let object = outcome.outputs["object"].as_object().expect("an object");
    assert_eq!(
        object.keys().collect::<Vec<_>>(),
        vec!["zebra", "apple", "moose"],
        "written order, not sorted"
    );
    Ok(())
}

/// Nesting is this node wired into this node, so a key whose value is
/// already an object (or a list, or a stored file's reference) must go
/// in exactly as it arrived.
async fn nested_value(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &JsonObjectNode,
            json!({ "card": { "title": "hi", "seen": 2 }, "tags": ["a", "b"] }),
        )
        .await
        .ok()?;
    assert_eq!(
        outcome.outputs["object"],
        json!({ "card": { "title": "hi", "seen": 2 }, "tags": ["a", "b"] })
    );
    Ok(())
}

/// An unwired node would hand on `{}` with nothing on screen saying
/// why, and a caller reading that body sees a successful empty answer.
async fn nothing_wired(rig: FakeRig) -> WeftResult<()> {
    let error = rig.run(&JsonObjectNode, json!({})).await.failure()?;
    assert!(error.contains("nothing is wired"), "{error}");
    Ok(())
}
