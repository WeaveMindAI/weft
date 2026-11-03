//! JsonObject self-tests: the wired keys are the object, written order
//! survives, a value of any shape goes in whole, keys that all stayed
//! silent make `{}`. A node with no keys at all is refused by its
//! metadata rule, at compile time, which the rig does not run.
//!
//! Whether an absent input skips the node or arrives absent is the
//! engine's decision, taken before this body runs, and it is proved in
//! the engine's own skip tests. What this file pins is that whatever
//! DOES arrive lands under its own name, unchanged.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::JsonObjectNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_wired_keys_are_the_object", keys_are_the_object),
        NodeTest::fake("keys_keep_the_order_they_were_written", written_order),
        NodeTest::fake("a_value_of_any_shape_goes_in_whole", nested_value),
        NodeTest::fake("keys_that_all_stayed_silent_make_an_empty_object", all_silent),
    ]
}

/// The keys a compiled JsonObject has: one input per key, as the
/// program wrote them.
fn keys(rig: &FakeRig, names: &[&str]) {
    for name in names {
        rig.input_type(name, WeftType::parse("String | Number | JsonDict | List[String]").expect("parses"));
    }
}

async fn keys_are_the_object(rig: FakeRig) -> WeftResult<()> {
    keys(&rig, &["job", "status"]);
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
    keys(&rig, &["zebra", "apple", "moose"]);
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
    keys(&rig, &["card", "tags"]);
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

/// Every key optional and none arriving: the node ran because its gate
/// said so (the compiler demands one on an all-optional node), and `{}`
/// is the true answer. A route called with `{}` gets `{}` back.
async fn all_silent(rig: FakeRig) -> WeftResult<()> {
    keys(&rig, &["title", "note"]);
    let outcome = rig.run(&JsonObjectNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["object"], json!({}));
    Ok(())
}
