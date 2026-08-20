//! Range self-tests: the half-open walk in both directions (the rig
//! collects the stream's yields into one array per port) and the loud
//! config refusals.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::RangeNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("walks_up_half_open_with_defaults", walks_up),
        NodeTest::fake("negative_step_walks_down", walks_down),
        NodeTest::fake("from_already_past_to_is_empty", already_past),
        NodeTest::fake("zero_step_fails_loud", zero_step),
        NodeTest::fake("max_finite_step_yields_once_and_terminates", max_finite_step),
    ]
}

async fn walks_up(rig: FakeRig) -> WeftResult<()> {
    // `from` and `step` come from their metadata defaults (0 and 1).
    let outcome = rig.run(&RangeNode, json!({ "to": 5 })).await.ok()?;
    assert_eq!(outcome.outputs["values"], json!([0.0, 1.0, 2.0, 3.0, 4.0]));
    Ok(())
}

async fn walks_down(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&RangeNode, json!({ "from": 5, "to": 0, "step": -1 }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["values"], json!([5.0, 4.0, 3.0, 2.0, 1.0]));
    Ok(())
}

async fn already_past(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&RangeNode, json!({ "from": 7, "to": 3 })).await.ok()?;
    assert_eq!(outcome.outputs["values"], json!([]));
    Ok(())
}

async fn zero_step(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&RangeNode, json!({ "to": 3, "step": 0 })).await;
    let err = outcome.result.expect_err("zero step must refuse").to_string();
    assert!(err.contains("step cannot be zero"), "{err}");
    Ok(())
}

async fn max_finite_step(rig: FakeRig) -> WeftResult<()> {
    // JSON cannot spell Infinity or NaN, so the body's non-finite
    // guard is a backstop no test input can reach; the numeric edge a
    // test CAN pin is the largest finite step: one yield, then
    // `cur += step` saturates past `to` and the walk terminates
    // instead of spinning.
    let outcome = rig
        .run(&RangeNode, json!({ "to": 1, "step": f64::MAX }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["values"], json!([0.0]));
    Ok(())
}
