//! StopMemberInfra self-tests: the choices become the take-down spec,
//! the self choice follows the toggle, and a wait before a wipe is
//! refused before anything is asked.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{DeactivationMode, FakeRig, NodeTest, RunningPolicy, StopSelf, WeftResult};

use super::StopMemberInfraNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("parks_and_waits_by_default", defaults),
        NodeTest::fake("the_choices_reach_the_call", choices),
        NodeTest::fake("waiting_before_a_wipe_is_refused", wipe_wait),
    ]
}

async fn defaults(rig: FakeRig) -> WeftResult<()> {
    rig.run(&StopMemberInfraNode, json!({ "node": "bridge", "member": "ada" })).await.ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::InfraStop { node, member, spec }, StopSelf::Keep) => {
            assert_eq!(node, "bridge");
            assert_eq!(member.as_ref().unwrap().as_str(), "ada");
            assert_eq!(spec.mode, DeactivationMode::Park);
            assert_eq!(spec.running_policy, RunningPolicy::Wait);
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn choices(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &StopMemberInfraNode,
        json!({ "node": "bridge", "member": "ada", "triggers": "hibernate", "graceMinutes": 30, "running": "cancel", "includeSelf": true }),
    )
    .await
    .ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::InfraStop { spec, .. }, StopSelf::Include) => {
            assert_eq!(spec.mode, DeactivationMode::Hibernate);
            assert_eq!(spec.grace_minutes, 30);
            assert_eq!(spec.running_policy, RunningPolicy::Cancel);
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn wipe_wait(rig: FakeRig) -> WeftResult<()> {
    let err = rig
        .run(&StopMemberInfraNode, json!({ "node": "bridge", "member": "ada", "triggers": "wipe", "running": "wait" }))
        .await
        .failure()?;
    assert!(err.contains("wipe requires"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
