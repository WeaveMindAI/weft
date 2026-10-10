//! StartInfra self-tests: the start is asked for that instance's copy
//! of that node, or the program's own copy when `instance` is unwired,
//! and `done` waits for the copy to run, a copy that
//! fails fails the node, a copy somebody stops (or that never came to be)
//! while the node waits fails it instead of being started again, a start
//! refused for a passing reason is asked again, a node told not to wait
//! finishes once the start is accepted without looking at the copy, and a
//! blank or empty instance is refused before anything is asked (a value
//! that never arrived must not reach the program's own copy).

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, StopSelf, WeftResult};

use super::StartInfraNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("starts_the_instances_copy", starts_the_instances_copy),
        NodeTest::fake("no_instance_starts_the_programs_own_copy", starts_the_programs_own_copy),
        NodeTest::fake("an_empty_instance_is_refused", empty_instance),
        NodeTest::fake("waits_until_the_copy_runs", waits_until_the_copy_runs),
        NodeTest::fake("a_copy_that_fails_fails_the_node", a_copy_that_fails),
        NodeTest::fake("a_copy_stopped_while_waiting_fails_the_node", stopped_while_waiting),
        NodeTest::fake("a_start_that_ends_without_a_copy_fails_the_node", no_copy),
        NodeTest::fake("a_start_refused_for_now_is_asked_again", refused_for_now),
        NodeTest::fake("without_waiting_finishes_once_the_start_is_accepted", without_waiting),
        NodeTest::fake("without_waiting_a_start_refused_for_now_is_asked_again", without_waiting_refused_for_now),
        NodeTest::fake("a_blank_instance_is_refused", blank_instance),
    ]
}

async fn starts_the_instances_copy(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", running());
    let outcome = rig.run(&StartInfraNode, json!({ "node": "bridge", "instance": "user-42" })).await.ok()?;
    let calls = rig.program_calls();
    assert_eq!(calls.len(), 2, "the start, then one look that found it running");
    match &calls[0] {
        (ProgramCall::InfraStart { node, instance }, StopSelf::Keep) => {
            assert_eq!(node, "bridge");
            assert_eq!(instance.as_ref().map(|i| i.as_str()), Some("user-42"));
        }
        other => panic!("unexpected call {other:?}"),
    }
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}

async fn starts_the_programs_own_copy(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!({ "node": "bridge", "status": "running" }));
    let outcome = rig.run(&StartInfraNode, json!({ "node": "bridge" })).await.ok()?;
    let calls = rig.program_calls();
    assert_eq!(calls.len(), 2, "the start, then one look that found it running: {calls:?}");
    for (call, _) in &calls {
        match call {
            ProgramCall::InfraStart { node, instance } | ProgramCall::InfraStatus { node, instance } => {
                assert_eq!(node, "bridge");
                assert!(instance.is_none(), "the program's own copy: {call:?}");
            }
            other => panic!("unexpected call {other:?}"),
        }
    }
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}

async fn empty_instance(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&StartInfraNode, json!({ "node": "bridge", "instance": "", "waitUntilRunning": false })).await.failure()?;
    assert!(err.contains("is not a valid instance id"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}

async fn blank_instance(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&StartInfraNode, json!({ "node": "bridge", "instance": "  " })).await.failure()?;
    assert!(err.contains("is not a valid instance id"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}

fn running() -> serde_json::Value {
    json!({ "node": "bridge", "instance": "user-42", "status": "running" })
}

async fn waits_until_the_copy_runs(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!({ "node": "bridge", "instance": "user-42", "status": "provisioning" }));
    rig.answer_program_call("weft.infra.status", running());
    rig.signal(json!({}));
    let outcome = rig.run(&StartInfraNode, json!({ "node": "bridge", "instance": "user-42" })).await.ok()?;
    assert_eq!(rig.awaited_signals().len(), 1, "one wait between the two looks");
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}

async fn a_copy_that_fails(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call(
        "weft.infra.status",
        json!({ "node": "bridge", "instance": "user-42", "status": "failed", "failure": "image pull failed" }),
    );
    let err = rig.run(&StartInfraNode, json!({ "node": "bridge", "instance": "user-42" })).await.failure()?;
    assert!(err.contains("image pull failed"), "{err}");
    Ok(())
}

async fn stopped_while_waiting(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!({ "node": "bridge", "instance": "user-42", "status": "provisioning" }));
    rig.answer_program_call("weft.infra.status", json!({ "node": "bridge", "instance": "user-42", "status": "stopped" }));
    rig.signal(json!({}));
    let err = rig.run(&StartInfraNode, json!({ "node": "bridge", "instance": "user-42" })).await.failure()?;
    assert!(err.contains("took it down"), "{err}");
    let starts = rig.program_calls().iter().filter(|(call, _)| matches!(call, ProgramCall::InfraStart { .. })).count();
    assert_eq!(starts, 1, "a stop while waiting is never undone by a second start");
    Ok(())
}

async fn no_copy(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!(null));
    let err = rig.run(&StartInfraNode, json!({ "node": "bridge", "instance": "user-42" })).await.failure()?;
    assert!(err.contains("did not come up"), "{err}");
    Ok(())
}

async fn refused_for_now(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.start", json!({ "answer": "waiting", "reason": "project is building" }));
    rig.answer_program_call("weft.infra.start", json!({ "answer": "started" }));
    rig.answer_program_call("weft.infra.status", running());
    rig.signal(json!({}));
    let outcome = rig.run(&StartInfraNode, json!({ "node": "bridge", "instance": "user-42" })).await.ok()?;
    let starts = rig.program_calls().iter().filter(|(call, _)| matches!(call, ProgramCall::InfraStart { .. })).count();
    assert_eq!(starts, 2, "asked again after the wait");
    assert_eq!(rig.awaited_signals().len(), 1);
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}

async fn without_waiting(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&StartInfraNode, json!({ "node": "bridge", "instance": "user-42", "waitUntilRunning": false }))
        .await
        .ok()?;
    let calls = rig.program_calls();
    assert_eq!(calls.len(), 1, "the start only, no look at the copy: {calls:?}");
    assert!(matches!(&calls[0], (ProgramCall::InfraStart { .. }, StopSelf::Keep)), "{calls:?}");
    assert!(rig.awaited_signals().is_empty());
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}

async fn without_waiting_refused_for_now(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.start", json!({ "answer": "waiting", "reason": "project is building" }));
    rig.answer_program_call("weft.infra.start", json!({ "answer": "already_starting" }));
    rig.signal(json!({}));
    let outcome = rig
        .run(&StartInfraNode, json!({ "node": "bridge", "instance": "user-42", "waitUntilRunning": false }))
        .await
        .ok()?;
    let calls = rig.program_calls();
    assert_eq!(calls.len(), 2, "asked again after the wait, then done without a look: {calls:?}");
    assert!(calls.iter().all(|(call, _)| matches!(call, ProgramCall::InfraStart { .. })), "{calls:?}");
    assert_eq!(rig.awaited_signals().len(), 1);
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}
