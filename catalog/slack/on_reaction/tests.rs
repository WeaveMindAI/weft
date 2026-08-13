//! SlackOnReaction self-tests: direction/emoji predicates and the
//! fired address.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackOnReactionNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_narrows_by_direction_and_emoji", setup_registers),
        NodeTest::fake("a_fire_fans_the_message_address", fire_fans),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &SlackOnReactionNode,
        json!({
            "account": rig.access("slack"),
            "emoji": ":eyes:",
            "direction": "added",
        }),
    )
    .await
    .ok()?;
    let spec = serde_json::to_value(&rig.registered_signals()[0].0)
        .expect("spec serializes")
        .to_string();
    assert!(spec.contains("reaction_added"), "{spec}");
    assert!(spec.contains("\"eyes\""), "the :colons: are stripped: {spec}");
    Ok(())
}

async fn fire_fans(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({ "channel": "C1", "ts": "1.2", "emoji": "eyes", "user": "U1" }));
    let outcome = rig
        .run(&SlackOnReactionNode, json!({ "account": rig.access("slack"), "direction": "added" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["channel"], json!("C1"));
    assert_eq!(outcome.outputs["ts"], json!("1.2"));
    Ok(())
}
