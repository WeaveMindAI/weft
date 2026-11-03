//! Range self-tests: the half-open walk in both directions (the rig
//! collects the stream's yields into one array per port), the loud
//! config refusals, and a resumed walk carrying on where it stood.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{FakeRig, Node, NodeManifest, NodeMetadata, NodeTest, RunContext, WeftResult};

use super::{RangeNode, Walk};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("walks_up_half_open_with_defaults", walks_up),
        NodeTest::fake("negative_step_walks_down", walks_down),
        NodeTest::fake("from_already_past_to_is_empty", already_past),
        NodeTest::fake("a_step_too_small_to_move_fails_loud", stalled_step),
        NodeTest::fake("max_finite_step_yields_once_and_terminates", max_finite_step),
        NodeTest::fake("a_resumed_walk_carries_on_after_what_it_yielded", resumed_walk),
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

/// A zero step is the metadata rule's to refuse (the rig runs no rules);
/// a step that is not zero but too small to change the number would offer
/// the same number for ever, and fails instead.
async fn stalled_step(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&RangeNode, json!({ "from": 1e16, "to": 1e17, "step": 1 })).await;
    let err = outcome.result.expect_err("a step that cannot move must refuse").to_string();
    assert!(err.contains("does not move past"), "{err}");
    Ok(())
}

async fn max_finite_step(rig: FakeRig) -> WeftResult<()> {
    // JSON cannot spell Infinity or NaN, so the numeric edge a test can
    // pin is the largest finite step: one yield, then
    // `cur += step` saturates past `to` and the walk terminates
    // instead of spinning.
    let outcome = rig
        .run(&RangeNode, json!({ "to": 1, "step": f64::MAX }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["values"], json!([0.0]));
    Ok(())
}

/// Range's first run, standing in for one that paused after `yielded`
/// numbers of `[0, 0.1, 0.2, ...)`: it yields them and saves the state
/// Range saves at that point. A rig never pauses a body, so this is how a
/// test gets a firing to pick up. Range's own manifest makes it the same
/// firing to the rig.
struct PausedAfter(u64);

impl NodeManifest for PausedAfter {
    fn manifest(&self) -> &'static NodeMetadata {
        RangeNode.manifest()
    }
}

#[async_trait]
impl Node for PausedAfter {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let mut state = ctx.keep(Walk::default()).await?;
        let mut cur = 0.0;
        for _ in 0..self.0 {
            ctx.yield_downstream(NodeOutput::new().set("values", json!(cur)), &mut state).await?;
            state.yielded += 1;
            cur += 0.1;
        }
        state.save().await
    }
}

/// Picked up after two numbers, the walk offers the third next and goes
/// on to the end, neither of the first two again. The step is not an
/// integer on purpose: the resumed body walks back with the same
/// additions, so it lands on the float the unbroken walk would have
/// (`0.30000000000000004`, not `0.3`).
async fn resumed_walk(rig: FakeRig) -> WeftResult<()> {
    let inputs = json!({ "to": 0.5, "step": 0.1 });
    let whole = rig.fresh().run(&RangeNode, inputs.clone()).await.ok()?;
    rig.run(&PausedAfter(2), inputs.clone()).await.ok()?;
    assert_eq!(rig.saved_state(&RangeNode), Some(json!({ "yielded": 2 })));
    let resumed = rig.run(&RangeNode, inputs).await.ok()?;
    assert_eq!(resumed.outputs["values"], json!([0.2, 0.30000000000000004, 0.4]));
    assert_eq!(whole.outputs["values"].as_array().expect("an array")[2..], resumed.outputs["values"].as_array().expect("an array")[..]);
    Ok(())
}
