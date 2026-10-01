//! ActivateInstanceTriggers self-tests: every per-instance trigger of the
//! instance by default, only the named ones when `only` is wired.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, WeftResult};

use super::ActivateInstanceTriggersNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("every_trigger_of_the_instance", every),
        NodeTest::fake("only_the_named_ones", only),
    ]
}

async fn every(rig: FakeRig) -> WeftResult<()> {
    rig.run(&ActivateInstanceTriggersNode, json!({ "instance": "ada" })).await.ok()?;
    match &rig.program_calls()[0].0 {
        ProgramCall::TriggerActivate { scope } => {
            assert!(scope.triggers.is_empty());
            assert_eq!(scope.instance.as_ref().unwrap().as_str(), "ada");
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn only(rig: FakeRig) -> WeftResult<()> {
    rig.run(&ActivateInstanceTriggersNode, json!({ "instance": "ada", "only": ["receive"] })).await.ok()?;
    match &rig.program_calls()[0].0 {
        ProgramCall::TriggerActivate { scope } => assert_eq!(scope.triggers, vec!["receive".to_string()]),
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}
