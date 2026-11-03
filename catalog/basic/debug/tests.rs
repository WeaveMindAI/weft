//! Debug self-tests: a sink that logs what flows in.

use serde_json::json;

use weft::bus::{BusOptions, StreamEnd};
use weft::{FakeRig, NodeTest, WeftResult};

use super::DebugNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("logs_the_value_under_the_node_label", logs_value),
        NodeTest::fake("an_absent_input_logs_null", absent_logs_null),
        NodeTest::fake("a_bus_that_broke_off_fails_the_node_after_logging_its_messages", failed_bus),
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

/// A bus whose producer broke off is followed to its end like any bus,
/// each message logged as it lands, and the read past its end fails the
/// node naming why.
async fn failed_bus(rig: FakeRig) -> WeftResult<()> {
    let (mut bus, marker) = rig.seed_bus(BusOptions::default())?;
    bus.register("llm").expect("a fresh bus accepts its first name");
    bus.send("delta", json!("hel")).expect("an open bus accepts a send");
    bus.inner_arc().close(StreamEnd::Failed { error: "the model went away".into() });
    let err = rig.run(&DebugNode, json!({ "data": marker })).await.failure()?;
    assert!(err.contains("the model went away"), "{err}");
    let logs = rig.logs();
    assert!(logs.iter().any(|(_, line)| line.contains(r#""hel""#)), "the message was logged as it landed: {logs:?}");
    Ok(())
}
