//! FirstInOrder self-tests: the first input that spoke is the answer,
//! and an all-quiet firing emits nothing.
//!
//! A case hands its inputs as a JSON object, so the rig cannot express
//! "this port was written first" (object keys arrive sorted). The
//! priority chain is proved where the order actually lives: the
//! compiler orders created ports by source span
//! (`created_ports_follow_source_order` in the compiler's enrich tests)
//! and `ValueBag::in_order` walks the order it was handed
//! (`in_order_follows_the_port_order` in weft-core).

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::FirstInOrderNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_first_input_that_spoke_is_emitted", first_delivered_wins),
        NodeTest::fake("a_single_branch_passes_through", single_branch),
        NodeTest::fake("all_quiet_emits_nothing", all_quiet),
    ]
}

async fn first_delivered_wins(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&FirstInOrderNode, json!({ "a_checked": "reviewed", "b_quick": "automatic" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["value"], json!("reviewed"));
    Ok(())
}

/// A branch that was cut delivers nothing at all, so the surviving one
/// is the only input in the bag.
async fn single_branch(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&FirstInOrderNode, json!({ "quick": "automatic" })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!("automatic"));
    Ok(())
}

/// Nothing emitted is what closes the output: the engine closes every
/// port a firing did not mention, so the node says only what it did.
async fn all_quiet(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&FirstInOrderNode, json!({})).await.ok()?;
    assert!(
        outcome.outputs.is_empty(),
        "no branch spoke, so nothing may be emitted: {:?}",
        outcome.outputs
    );
    Ok(())
}
