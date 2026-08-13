//! SlackAppMessages self-tests: the app-wide subscription's filters
//! and a fire's fan-out.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackAppMessagesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_subscribes_app_wide_with_the_filters", setup_registers),
        NodeTest::fake("a_fire_fans_the_event", fire_fans),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &SlackAppMessagesNode,
        json!({ "account": rig.access("slack"), "keyword": "deploy" }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes").to_string();
    assert!(spec.contains("messages"), "{spec}");
    assert!(spec.contains("deploy"), "the keyword narrows as a filter: {spec}");
    assert!(spec.contains("bot"), "bots are filtered out by default: {spec}");
    Ok(())
}

async fn fire_fans(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({ "text": "deploy now", "channel": "C1", "workspace": "T1", "user": "U1" }));
    let outcome = rig
        .run(&SlackAppMessagesNode, json!({ "account": rig.access("slack") }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["text"], json!("deploy now"));
    assert_eq!(outcome.outputs["workspace"], json!("T1"), "which install fired");
    Ok(())
}
