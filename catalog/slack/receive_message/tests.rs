//! SlackReceiveMessage self-tests: the filter inputs become event
//! predicates, and a fire fans the named event.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackReceiveMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_translates_the_filters_into_predicates", setup_registers),
        NodeTest::fake("a_fire_fans_the_message_fields", fire_fans),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &SlackReceiveMessageNode,
        json!({
            "account": rig.access("slack"),
            "channel": "C1",
            "keyword": "deploy",
            "replies": "top_level",
        }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes").to_string();
    assert!(spec.contains("C1"), "the channel narrows: {spec}");
    assert!(spec.contains("deploy"), "the keyword narrows: {spec}");
    assert!(spec.contains("thread"), "top-level excludes thread replies: {spec}");
    Ok(())
}

async fn fire_fans(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({ "text": "deploy now", "channel": "C1", "user": "U1", "ts": "1.2" }));
    let outcome = rig
        .run(
            &SlackReceiveMessageNode,
            json!({ "account": rig.access("slack"), "channel": "C1" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["text"], json!("deploy now"));
    assert_eq!(outcome.outputs["ts"], json!("1.2"));
    Ok(())
}
