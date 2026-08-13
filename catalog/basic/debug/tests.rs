//! Debug self-tests: a sink that logs what flows in.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::DebugNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("logs_the_value_under_the_node_label", logs_value),
        NodeTest::fake("an_absent_input_logs_null", absent_logs_null),
    ]
}

async fn logs_value(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&DebugNode, json!({ "data": { "n": 42 } })).await.ok()?;
    assert!(outcome.outputs.is_empty(), "Debug is a terminal sink");
    let logs = rig.logs();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].1.contains(r#"{"n":42}"#), "logged the payload: {}", logs[0].1);
    Ok(())
}

async fn absent_logs_null(rig: FakeRig) -> WeftResult<()> {
    rig.run(&DebugNode, json!({})).await.ok()?;
    let logs = rig.logs();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].1.ends_with("null"), "absent input logs null: {}", logs[0].1);
    Ok(())
}
