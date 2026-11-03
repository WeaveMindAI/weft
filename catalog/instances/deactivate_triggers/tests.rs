//! DeactivateTriggers self-tests: the instance's triggers go down
//! with the node's choices, the program's own when no instance is given,
//! taking work and keeping what is held by default. A contradiction
//! (`doorForMinutes` beside a refusing door) is the node's rule, which
//! the compiler and the run-time recheck hold, not this rig.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{Door, FakeRig, Held, NodeTest, Running, StopSelf, WeftResult};

use super::DeactivateTriggersNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_instances_triggers_go_down", down),
        NodeTest::fake("no_instance_is_the_programs_own_triggers", the_programs_own),
        NodeTest::fake("keep_is_no_running_answer", no_keep),
    ]
}

async fn down(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &DeactivateTriggersNode,
        json!({ "instance": "ada", "door": "refuse", "held": "drop", "running": "cancel", "includeSelf": true }),
    )
    .await
    .ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::Triggers(weft::program::triggers::TriggerCall::Deactivate { request: weft::deactivation::DeactivateRequest { scope, choice: take_down } }), StopSelf::Include) => {
            assert_eq!(scope.instances, weft::instance::InstanceTarget::named(vec![weft::instance::InstanceId::new("ada").unwrap()]));
            assert_eq!((take_down.door, take_down.held, take_down.running), (Some(Door::Refuse), Some(Held::Drop), Some(Running::Cancel)));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn the_programs_own(rig: FakeRig) -> WeftResult<()> {
    rig.run(&DeactivateTriggersNode, json!({ "doorForMinutes": 30 })).await.ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::Triggers(weft::program::triggers::TriggerCall::Deactivate { request: weft::deactivation::DeactivateRequest { scope, choice: take_down } }), StopSelf::Keep) => {
            assert!(scope.triggers.is_empty(), "every shared trigger");
            assert!(scope.instances.is_shared(), "the program's own triggers");
            assert_eq!((take_down.door, take_down.door_for_minutes), (Some(Door::Take), Some(30)));
            assert_eq!((take_down.held, take_down.running), (Some(Held::Keep), Some(Running::Wait)));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn no_keep(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&DeactivateTriggersNode, json!({ "running": "keep" })).await.failure()?;
    assert!(err.contains("`wait` or `cancel`"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
