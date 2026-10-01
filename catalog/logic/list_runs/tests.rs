//! ListRuns self-tests: the runs and the total come out as the runtime
//! answered them, the limit and the filters reach the call, and a limit
//! out of range is refused before anything is asked.

use serde_json::json;

use weft::program::{ProgramCall, RunStatus};
use weft::{FakeRig, NodeTest, WeftResult};

use super::ListRunsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("lists_the_newest_runs_with_the_total", lists_runs),
        NodeTest::fake("a_limit_out_of_range_is_refused", bad_limit),
    ]
}

async fn lists_runs(rig: FakeRig) -> WeftResult<()> {
    let run = json!({
        "execution_id": "00000000-0000-0000-0000-000000000001",
        "project_id": "00000000-0000-0000-0000-0000000000aa",
        "entry_node": "inbox",
        "status": "waiting_for_input",
        "phase": "fire",
        "started_at": 100,
        "completed_at": null,
        "tags": ["user_7"],
        "cancel_cause": null,
        "skipped_nodes": 0,
        "instance": "ada"
    });
    rig.answer_program_call("weft.runs.list", json!({ "executions": [run], "total": 9 }));
    let outcome = rig.run(&ListRunsNode, json!({ "limit": 1, "tag": "user_7", "status": "running" })).await.ok()?;
    assert_eq!(outcome.outputs["total"], json!(9));
    assert_eq!(outcome.outputs["runs"][0]["status"], json!("waiting_for_input"));
    assert_eq!(outcome.outputs["runs"][0]["instance"], json!("ada"));
    match &rig.program_calls()[0].0 {
        ProgramCall::RunsList { filter, limit } => {
            assert_eq!(*limit, 1);
            assert_eq!(filter.tag.as_deref(), Some("user_7"));
            assert_eq!(filter.status, Some(RunStatus::Running));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn bad_limit(rig: FakeRig) -> WeftResult<()> {
    for limit in [json!(0), json!(201)] {
        let err = rig.run(&ListRunsNode, json!({ "limit": limit })).await.failure()?;
        assert!(err.contains("1 to 200"), "{err}");
    }
    let err = rig.run(&ListRunsNode, json!({ "limit": 2.5 })).await.failure()?;
    assert!(err.contains("whole number"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
