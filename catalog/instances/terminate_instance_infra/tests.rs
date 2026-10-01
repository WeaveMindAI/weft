//! TerminateInstanceInfra self-tests: the terminate is asked for that
//! instance's copy with the node's choices.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{DeactivationMode, FakeRig, NodeTest, RunningPolicy, StopSelf, WeftResult};

use super::TerminateInstanceInfraNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("terminates_the_instances_copy", terminates)]
}

async fn terminates(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &TerminateInstanceInfraNode,
        json!({ "node": "one.bridge", "instance": "ada", "triggers": "wipe", "running": "cancel" }),
    )
    .await
    .ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::InfraTerminate { node, instance, spec, disks }, StopSelf::Keep) => {
            assert_eq!(node, "one.bridge");
            assert_eq!(*disks, weft::infra::TerminateDisks::KeepListed);
            assert_eq!(instance.as_ref().unwrap().as_str(), "ada");
            assert_eq!(spec.mode, DeactivationMode::Wipe);
            assert_eq!(spec.running_policy, RunningPolicy::Cancel);
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}
