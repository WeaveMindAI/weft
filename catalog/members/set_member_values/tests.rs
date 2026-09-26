//! SetMemberValues self-tests: every value and clear reaches the one
//! change, and a malformed field name or an empty change is refused
//! before anything is asked.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, WeftResult};

use super::SetMemberValuesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("one_change_carries_every_value", one_change),
        NodeTest::fake("a_bad_field_or_nothing_is_refused", refused),
    ]
}

async fn one_change(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.values.change", json!({ "rearmed": ["digest"] }));
    let outcome = rig
        .run(
            &SetMemberValuesNode,
            json!({
                "member": "ada",
                "values": { "one.read.spreadsheet": "s-1", "digest.cron": "0 0 7 * * *" },
                "clear": ["read.tab"]
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["rearmed"], json!(["digest"]));
    let calls = rig.program_calls();
    assert_eq!(calls.len(), 1);
    match &calls[0].0 {
        ProgramCall::ValuesChange { member, set, clear } => {
            assert_eq!(member.as_str(), "ada");
            let mut steps: Vec<(&str, &str)> = set.iter().map(|v| (v.step.as_str(), v.field.as_str())).collect();
            steps.sort();
            assert_eq!(steps, vec![("digest", "cron"), ("one.read", "spreadsheet")]);
            assert_eq!(clear.len(), 1);
            assert_eq!((clear[0].step.as_str(), clear[0].field.as_str()), ("read", "tab"));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&SetMemberValuesNode, json!({ "member": "ada", "values": { "nodot": 1 } })).await.failure()?;
    assert!(err.contains("is not `node.field`"), "{err}");
    let err = rig.run(&SetMemberValuesNode, json!({ "member": "ada" })).await.failure()?;
    assert!(err.contains("nothing to change"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
