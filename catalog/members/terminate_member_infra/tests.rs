//! TerminateMemberInfra self-tests: the terminate is asked for that
//! member's copy with the node's choices.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{DeactivationMode, FakeRig, NodeTest, RunningPolicy, StopSelf, WeftResult};

use super::TerminateMemberInfraNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("terminates_the_members_copy", terminates)]
}

async fn terminates(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &TerminateMemberInfraNode,
        json!({ "node": "one.bridge", "member": "ada", "triggers": "wipe", "running": "cancel" }),
    )
    .await
    .ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::InfraTerminate { node, member, spec }, StopSelf::Keep) => {
            assert_eq!(node, "one.bridge");
            assert_eq!(member.as_ref().unwrap().as_str(), "ada");
            assert_eq!(spec.mode, DeactivationMode::Wipe);
            assert_eq!(spec.running_policy, RunningPolicy::Cancel);
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}
