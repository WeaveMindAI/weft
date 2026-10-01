//! DeactivateInstanceTriggers self-tests: the instance's triggers go down
//! with the node's choices.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{DeactivationMode, FakeRig, NodeTest, StopSelf, WeftResult};

use super::DeactivateInstanceTriggersNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("the_instances_triggers_go_down", down)]
}

async fn down(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &DeactivateInstanceTriggersNode,
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
