//! ApiEndpoint self-tests: the registered live route and the kick
//! pulse.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::ApiEndpointNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_endpoint_signal", setup_registers),
        NodeTest::fake("a_fire_kicks_the_graph", fire_kicks),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(&ApiEndpointNode, json!({ "path": "hook" })).await.ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes").to_string();
    assert!(spec.contains("hook"), "the configured path rides the signal: {spec}");
    Ok(())
}

async fn fire_kicks(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&ApiEndpointNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["started"], json!(true));
    Ok(())
}
