//! DeactivateTriggers self-tests: the instance's triggers go down
//! with the node's choices, and the program's own when no instance is
//! given.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{DeactivationMode, FakeRig, NodeTest, StopSelf, WeftResult};

use super::DeactivateTriggersNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_instances_triggers_go_down", down),
        NodeTest::fake("no_instance_is_the_programs_own_triggers", the_programs_own),
    ]
}

async fn down(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &DeactivateTriggersNode,
        json!({ "instance": "ada", "triggers": "wipe", "running": "cancel", "includeSelf": true }),
    )
    .await
    .ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::TriggerDeactivate { scope, spec }, StopSelf::Include) => {
            assert_eq!(scope.instance.as_ref().unwrap().as_str(), "ada");
            assert_eq!(spec.mode, DeactivationMode::Wipe);
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn the_programs_own(rig: FakeRig) -> WeftResult<()> {
    rig.run(&DeactivateTriggersNode, json!({})).await.ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::TriggerDeactivate { scope, spec }, StopSelf::Keep) => {
            assert!(scope.triggers.is_empty(), "every shared trigger");
            assert!(scope.instance.is_none(), "the program's own triggers");
            assert_eq!(spec.mode, DeactivationMode::Park);
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}
