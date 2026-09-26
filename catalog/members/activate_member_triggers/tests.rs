//! ActivateMemberTriggers self-tests: every per-member trigger of the
//! member by default, only the named ones when `only` is wired.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, WeftResult};

use super::ActivateMemberTriggersNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("every_trigger_of_the_member", every),
        NodeTest::fake("only_the_named_ones", only),
    ]
}

async fn every(rig: FakeRig) -> WeftResult<()> {
    rig.run(&ActivateMemberTriggersNode, json!({ "member": "ada" })).await.ok()?;
    match &rig.program_calls()[0].0 {
        ProgramCall::TriggerActivate { scope } => {
            assert!(scope.triggers.is_empty());
            assert_eq!(scope.member.as_ref().unwrap().as_str(), "ada");
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn only(rig: FakeRig) -> WeftResult<()> {
    rig.run(&ActivateMemberTriggersNode, json!({ "member": "ada", "only": ["receive"] })).await.ok()?;
    match &rig.program_calls()[0].0 {
        ProgramCall::TriggerActivate { scope } => assert_eq!(scope.triggers, vec!["receive".to_string()]),
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}
