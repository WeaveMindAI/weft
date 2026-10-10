//! StopInfra self-tests: the stop is asked for that instance's copy, or
//! the program's own when no instance is given, the choices become the
//! take-down spec,
//! the self choice follows the toggle, and a wait before a wipe is
//! refused before anything is asked.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{DeactivationMode, FakeRig, NodeTest, RunningPolicy, StopSelf, WeftResult};

use super::StopInfraNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("parks_and_waits_by_default", defaults),
        NodeTest::fake("no_instance_stops_the_programs_own_copy", the_programs_own),
        NodeTest::fake("the_choices_reach_the_call", choices),
        NodeTest::fake("waiting_before_a_wipe_is_refused", wipe_wait),
        NodeTest::fake("no_copy_is_told_apart_from_done", no_copy),
        NodeTest::fake("a_copy_already_down_is_done", already_down),
    ]
}

async fn no_copy(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.stop", json!({ "copy": "no_copy" }));
    let out = rig.run(&StopInfraNode, json!({ "node": "bridge", "instance": "adaa" })).await.ok()?;
    assert_eq!(out.output("noCopy")?, &json!(true));
    assert!(out.output("done").is_err(), "nothing was taken down");
    Ok(())
}

async fn already_down(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.stop", json!({ "copy": "already_down" }));
    let out = rig.run(&StopInfraNode, json!({ "node": "bridge", "instance": "ada" })).await.ok()?;
    assert_eq!(out.output("done")?, &json!(true));
    assert!(out.output("noCopy").is_err());
    Ok(())
}

async fn defaults(rig: FakeRig) -> WeftResult<()> {
    rig.run(&StopInfraNode, json!({ "node": "bridge", "instance": "ada" })).await.ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::InfraStop { node, instance, spec }, StopSelf::Keep) => {
            assert_eq!(node, "bridge");
            assert_eq!(instance.as_ref().unwrap().as_str(), "ada");
            assert_eq!(spec.mode, DeactivationMode::Park);
            assert_eq!(spec.running_policy, RunningPolicy::Wait);
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn the_programs_own(rig: FakeRig) -> WeftResult<()> {
    let out = rig.run(&StopInfraNode, json!({ "node": "pg" })).await.ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::InfraStop { node, instance, .. }, StopSelf::Keep) => {
            assert_eq!(node, "pg");
            assert!(instance.is_none(), "the program's own copy");
        }
        other => panic!("unexpected call {other:?}"),
    }
    assert_eq!(out.output("done")?, &json!(true));
    Ok(())
}

async fn choices(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &StopInfraNode,
        json!({ "node": "bridge", "instance": "ada", "triggers": "hibernate", "graceMinutes": 30, "running": "cancel", "includeSelf": true }),
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
        .run(&StopInfraNode, json!({ "node": "bridge", "instance": "ada", "triggers": "wipe", "running": "wait" }))
        .await
        .failure()?;
    assert!(err.contains("wipe requires"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
