//! GetInstanceValues self-tests: the instance's values come back keyed
//! `node.field`, an instance given nothing reads as empty, and a missing
//! instance is refused before anything is asked.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, WeftResult};

use super::GetInstanceValuesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("values_come_back_by_node_and_field", by_node_and_field),
        NodeTest::fake("an_instance_given_nothing_reads_empty", nothing_given),
        NodeTest::fake("no_instance_is_refused", no_instance),
    ]
}

async fn by_node_and_field(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call(
        "weft.values.get",
        json!({
            "one.read": { "spreadsheet": "s-1" },
            "answer": { "model": "deepseek/deepseek-chat", "key": { "id": "g-1", "identity": "Ada" } }
        }),
    );
    let outcome = rig.run(&GetInstanceValuesNode, json!({ "instance": "ada" })).await.ok()?;
    assert_eq!(
        outcome.outputs["values"],
        json!({
            "one.read.spreadsheet": "s-1",
            "answer.model": "deepseek/deepseek-chat",
            "answer.key": { "id": "g-1", "identity": "Ada" }
        })
    );
    match &rig.program_calls()[0].0 {
        ProgramCall::ValuesGet { instance } => assert_eq!(instance.as_str(), "ada"),
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn nothing_given(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.values.get", json!({}));
    let outcome = rig.run(&GetInstanceValuesNode, json!({ "instance": "ada" })).await.ok()?;
    assert_eq!(outcome.outputs["values"], json!({}));
    Ok(())
}

async fn no_instance(rig: FakeRig) -> WeftResult<()> {
    rig.run(&GetInstanceValuesNode, json!({})).await.failure()?;
    assert!(rig.program_calls().is_empty());
    Ok(())
}
