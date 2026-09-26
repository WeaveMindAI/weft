//! StartMemberInfra self-tests: the start is asked for that member's copy
//! of that node and `done` waits for the copy to run, a copy that fails
//! fails the node, a copy somebody stops (or that never came to be) while
//! the node waits fails it instead of being started again, a start refused
//! for a passing reason is asked again, and a blank member is refused
//! before anything is asked.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, StopSelf, WeftResult};

use super::StartMemberInfraNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("starts_the_members_copy", starts_the_members_copy),
        NodeTest::fake("waits_until_the_copy_runs", waits_until_the_copy_runs),
        NodeTest::fake("a_copy_that_fails_fails_the_node", a_copy_that_fails),
        NodeTest::fake("a_copy_stopped_while_waiting_fails_the_node", stopped_while_waiting),
        NodeTest::fake("a_start_that_ends_without_a_copy_fails_the_node", no_copy),
        NodeTest::fake("a_start_refused_for_now_is_asked_again", refused_for_now),
        NodeTest::fake("a_blank_member_is_refused", blank_member),
    ]
}

async fn starts_the_members_copy(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", running());
    let outcome = rig.run(&StartMemberInfraNode, json!({ "node": "bridge", "member": "user-42" })).await.ok()?;
    let calls = rig.program_calls();
    assert_eq!(calls.len(), 2, "the start, then one look that found it running");
    match &calls[0] {
        (ProgramCall::InfraStart { node, member }, StopSelf::Keep) => {
            assert_eq!(node, "bridge");
            assert_eq!(member.as_ref().map(|m| m.as_str()), Some("user-42"));
        }
        other => panic!("unexpected call {other:?}"),
    }
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}

async fn blank_member(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&StartMemberInfraNode, json!({ "node": "bridge", "member": "  " })).await.failure()?;
    assert!(err.contains("is not a valid member id"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}

fn running() -> serde_json::Value {
    json!({ "node": "bridge", "member": "user-42", "status": "running" })
}

async fn waits_until_the_copy_runs(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!({ "node": "bridge", "member": "user-42", "status": "provisioning" }));
    rig.answer_program_call("weft.infra.status", running());
    rig.signal(json!({}));
    let outcome = rig.run(&StartMemberInfraNode, json!({ "node": "bridge", "member": "user-42" })).await.ok()?;
    assert_eq!(rig.awaited_signals().len(), 1, "one wait between the two looks");
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}

async fn a_copy_that_fails(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call(
        "weft.infra.status",
        json!({ "node": "bridge", "member": "user-42", "status": "failed", "failure": "image pull failed" }),
    );
    let err = rig.run(&StartMemberInfraNode, json!({ "node": "bridge", "member": "user-42" })).await.failure()?;
    assert!(err.contains("image pull failed"), "{err}");
    Ok(())
}

async fn stopped_while_waiting(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!({ "node": "bridge", "member": "user-42", "status": "provisioning" }));
    rig.answer_program_call("weft.infra.status", json!({ "node": "bridge", "member": "user-42", "status": "stopped" }));
    rig.signal(json!({}));
    let err = rig.run(&StartMemberInfraNode, json!({ "node": "bridge", "member": "user-42" })).await.failure()?;
    assert!(err.contains("took it down"), "{err}");
    let starts = rig.program_calls().iter().filter(|(call, _)| matches!(call, ProgramCall::InfraStart { .. })).count();
    assert_eq!(starts, 1, "a stop while waiting is never undone by a second start");
    Ok(())
}

async fn no_copy(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!(null));
    let err = rig.run(&StartMemberInfraNode, json!({ "node": "bridge", "member": "user-42" })).await.failure()?;
    assert!(err.contains("did not come up"), "{err}");
    Ok(())
}

async fn refused_for_now(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.start", json!({ "answer": "waiting", "reason": "project is building" }));
    rig.answer_program_call("weft.infra.start", json!({ "answer": "started" }));
    rig.answer_program_call("weft.infra.status", running());
    rig.signal(json!({}));
    let outcome = rig.run(&StartMemberInfraNode, json!({ "node": "bridge", "member": "user-42" })).await.ok()?;
    let starts = rig.program_calls().iter().filter(|(call, _)| matches!(call, ProgramCall::InfraStart { .. })).count();
    assert_eq!(starts, 2, "asked again after the wait");
    assert_eq!(rig.awaited_signals().len(), 1);
    assert_eq!(outcome.outputs["done"], json!(true));
    Ok(())
}
