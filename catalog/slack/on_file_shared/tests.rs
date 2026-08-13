//! SlackOnFileShared self-tests: the files subscription and a fire's
//! fan-out.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackOnFileSharedNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_subscribes_to_file_shares", setup_registers),
        NodeTest::fake("a_fire_names_the_file", fire_fans),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &SlackOnFileSharedNode,
        json!({ "account": rig.access("slack"), "channel": "C1" }),
    )
    .await
    .ok()?;
    let spec = serde_json::to_value(&rig.registered_signals()[0].0)
        .expect("spec serializes")
        .to_string();
    assert!(spec.contains("file_shared"), "{spec}");
    assert!(spec.contains("C1"), "{spec}");
    Ok(())
}

async fn fire_fans(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({ "fileId": "F1", "channel": "C1", "user": "U1" }));
    let outcome = rig
        .run(&SlackOnFileSharedNode, json!({ "account": rig.access("slack") }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["fileId"], json!("F1"));
    Ok(())
}
