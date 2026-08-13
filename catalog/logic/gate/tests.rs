//! Gate self-tests: pass routes the value, no-pass closes the port.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::GateNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("pass_emits_the_value", pass_emits),
        NodeTest::fake("no_pass_closes_the_port", no_pass_closes),
    ]
}

async fn pass_emits(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&GateNode, json!({ "pass": true, "value": { "x": 1 } }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["value"], json!({ "x": 1 }));
    assert!(outcome.closed_ports.is_empty());
    Ok(())
}

async fn no_pass_closes(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&GateNode, json!({ "pass": false, "value": "dropped" }))
        .await
        .ok()?;
    assert!(outcome.outputs.is_empty(), "no value may leak past a closed gate");
    assert_eq!(outcome.closed_ports, vec!["value".to_string()]);
    Ok(())
}
