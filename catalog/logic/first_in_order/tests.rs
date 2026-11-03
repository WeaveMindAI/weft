//! FirstInOrder self-tests: the first input that spoke is the answer.
//! A join none of whose branches delivered is skipped before it runs
//! (`skip.rs` proves that), and a FirstInOrder joining nothing is
//! refused by the node's rule, at compile time.
//!
//! A case hands its inputs as a JSON object, so the rig cannot express
//! "this port was written first" (object keys arrive sorted). The
//! priority chain is proved where the order actually lives: the
//! compiler orders created ports by source span
//! (`created_ports_follow_source_order` in the compiler's enrich tests)
//! and `ValueBag::in_order` walks the order it was handed
//! (`in_order_follows_the_port_order` in weft-core).

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::FirstInOrderNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_first_input_that_spoke_is_emitted", first_delivered_wins),
        NodeTest::fake("a_single_branch_passes_through", single_branch),
    ]
}

/// The branches a compiled FirstInOrder joins: one input per branch.
fn branches(rig: &FakeRig, names: &[&str]) {
    for name in names {
        rig.input_type(name, WeftType::parse("String").expect("parses"));
    }
}

async fn first_delivered_wins(rig: FakeRig) -> WeftResult<()> {
    branches(&rig, &["a_checked", "b_quick"]);
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
    branches(&rig, &["checked", "quick"]);
    let outcome = rig.run(&FirstInOrderNode, json!({ "quick": "automatic" })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!("automatic"));
    Ok(())
}
