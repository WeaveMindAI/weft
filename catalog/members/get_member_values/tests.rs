//! GetMemberValues self-tests: the member's values come back keyed
//! `node.field`, a member who gave nothing reads as empty, and a
//! missing member is refused before anything is asked.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, WeftResult};

use super::GetMemberValuesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("values_come_back_by_node_and_field", by_node_and_field),
        NodeTest::fake("a_member_who_gave_nothing_reads_empty", nothing_given),
        NodeTest::fake("no_member_is_refused", no_member),
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
    let outcome = rig.run(&GetMemberValuesNode, json!({ "member": "ada" })).await.ok()?;
    assert_eq!(
        outcome.outputs["values"],
        json!({
            "one.read.spreadsheet": "s-1",
            "answer.model": "deepseek/deepseek-chat",
            "answer.key": { "id": "g-1", "identity": "Ada" }
        })
    );
    match &rig.program_calls()[0].0 {
        ProgramCall::ValuesGet { member } => assert_eq!(member.as_str(), "ada"),
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn nothing_given(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.values.get", json!({}));
    let outcome = rig.run(&GetMemberValuesNode, json!({ "member": "ada" })).await.ok()?;
    assert_eq!(outcome.outputs["values"], json!({}));
    Ok(())
}

async fn no_member(rig: FakeRig) -> WeftResult<()> {
    rig.run(&GetMemberValuesNode, json!({})).await.failure()?;
    assert!(rig.program_calls().is_empty());
    Ok(())
}
