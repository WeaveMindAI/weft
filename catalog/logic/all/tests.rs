//! All self-tests: the yes is unanimous, one `false` is the whole
//! answer, and nothing wired is refused rather than silently true.
//!
//! The "an input closed" case is not here and cannot be: a closure on a
//! required port skips the node, so the engine answers it before the
//! body is ever called. That rule is proved where it lives, in the
//! engine's skip tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::AllNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("every_input_agreeing_is_the_yes", unanimous),
        NodeTest::fake("one_false_closes_the_answer", one_false),
        NodeTest::fake("a_value_that_is_not_false_is_a_yes", not_a_boolean),
        NodeTest::fake("nothing_wired_is_refused", nothing_wired),
    ]
}

async fn unanimous(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&AllNode, json!({ "rude": true, "sure": true })).await.ok()?;
    assert_eq!(outcome.outputs["yes"], json!(true));
    Ok(())
}

/// The no is an empty firing, which closes `yes`. A gate reading it
/// sees a closure, which is the same no it sees from a cut branch.
async fn one_false(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&AllNode, json!({ "rude": true, "sure": false })).await.ok()?;
    assert!(
        outcome.outputs.is_empty(),
        "one input said no, so nothing may be emitted: {:?}",
        outcome.outputs
    );
    Ok(())
}

/// Read exactly the way `_should_flow` reads a value: only `false` is a
/// no. So a row that arrived, a name, a count, all count as yes, and
/// wiring one of them here behaves as it would wired into a gate.
async fn not_a_boolean(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&AllNode, json!({ "row": { "id": 7 }, "sure": true }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["yes"], json!(true));
    Ok(())
}

/// "All of nothing" is true in logic, and a gate permanently open for
/// no visible reason in a graph.
async fn nothing_wired(rig: FakeRig) -> WeftResult<()> {
    let error = rig.run(&AllNode, json!({})).await.failure()?;
    assert!(error.contains("nothing is wired"), "{error}");
    Ok(())
}
