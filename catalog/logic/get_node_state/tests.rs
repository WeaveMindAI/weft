//! GetNodeState self-tests: every status the run's record answers comes
//! out as it was answered, `live` and `error` included untouched; the one
//! call carries the parsed run id and the node as written; and a `runId`
//! that is no run id is refused before anything is asked.

use serde_json::{json, Value};

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, RunId, StopSelf, WeftResult};

use super::GetNodeStateNode;

const RUN: &str = "00000000-0000-0000-0000-000000000007";

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_pending_node_reads_pending", |rig| passes_on(rig, json!({ "status": "pending" }))),
        NodeTest::fake("a_running_node_reads_running", |rig| passes_on(rig, json!({ "status": "running" }))),
        NodeTest::fake("a_waiting_node_reads_waiting", |rig| passes_on(rig, json!({ "status": "waiting" }))),
        NodeTest::fake("a_waiting_node_hands_on_its_live_answer_raw", |rig| {
            passes_on(rig, json!({ "status": "waiting", "live": "{\"status\":\"IN_PROGRESS\",\"progress\":0.4}" }))
        }),
        NodeTest::fake("a_waiting_node_whose_answer_was_too_large_says_its_size", |rig| {
            passes_on(rig, json!({ "status": "waiting", "liveTooLarge": 70000 }))
        }),
        NodeTest::fake("a_paused_node_reads_paused", |rig| passes_on(rig, json!({ "status": "paused" }))),
        NodeTest::fake("a_completed_node_reads_completed", |rig| passes_on(rig, json!({ "status": "completed" }))),
        NodeTest::fake("a_failed_node_hands_on_its_error_raw", |rig| {
            passes_on(rig, json!({ "status": "failed", "error": "render: the service answered 500" }))
        }),
        NodeTest::fake("a_skipped_node_reads_skipped", |rig| passes_on(rig, json!({ "status": "skipped" }))),
        NodeTest::fake("a_cancelled_node_reads_cancelled", |rig| passes_on(rig, json!({ "status": "cancelled" }))),
        NodeTest::fake("a_run_id_that_is_no_run_id_is_refused", bad_run_id),
    ]
}

/// The node answers `state` exactly as the record answered it, after one
/// call naming the run and the node as written.
async fn passes_on(rig: FakeRig, answer: Value) -> WeftResult<()> {
    rig.answer_program_call("weft.runs.node_state", answer.clone());
    let outcome = rig.run(&GetNodeStateNode, json!({ "runId": RUN, "node": "one.render" })).await.ok()?;
    assert_eq!(outcome.outputs["state"], answer);
    let calls = rig.program_calls();
    assert_eq!(calls.len(), 1);
    let (ProgramCall::Runs(weft::program::runs::RunCall::NodeState { run, node }), StopSelf::Keep) = &calls[0] else {
        panic!("unexpected call {:?}", calls[0]);
    };
    assert_eq!(*run, RUN.parse::<RunId>().unwrap());
    assert_eq!(node, "one.render");
    Ok(())
}

async fn bad_run_id(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&GetNodeStateNode, json!({ "runId": "run-7", "node": "render" })).await.failure()?;
    assert!(err.contains("'runId'"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
