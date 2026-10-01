//! CountRuns self-tests: every filter input reaches the one call, the tag
//! is read the way `TagRun` writes it, empty inputs narrow nothing, and a
//! negative or fractional age, or an unknown status, is refused before
//! anything is asked.

use serde_json::json;

use weft::program::{ProgramCall, RunFilter, RunStatus};
use weft::{FakeRig, NodeTest, StopSelf, WeftResult};

use super::CountRunsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("every_filter_reaches_the_count", every_filter),
        NodeTest::fake("empty_inputs_count_every_run", empty_inputs),
        NodeTest::fake("a_negative_or_fractional_age_is_refused", bad_age),
        NodeTest::fake("an_unknown_status_is_refused_naming_the_real_ones", unknown_status),
    ]
}

async fn every_filter(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.runs.count", json!({ "total": 4 }));
    let outcome = rig
        .run(
            &CountRunsNode,
            json!({ "instance": "ada", "status": "failed", "node": "inbox", "tag": "user_7", "olderThanSecs": 60 }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(4));
    let calls = rig.program_calls();
    assert_eq!(calls.len(), 1);
    let (ProgramCall::RunsCount { filter }, StopSelf::Keep) = &calls[0] else {
        panic!("unexpected call {:?}", calls[0]);
    };
    assert_eq!(filter.instance.as_ref().map(|i| i.as_str()), Some("ada"));
    assert_eq!(filter.status, Some(RunStatus::Failed));
    assert_eq!(filter.node.as_deref(), Some("inbox"));
    assert_eq!(filter.tag.as_deref(), Some("user_7"));
    assert_eq!(filter.older_than_secs, Some(60));
    Ok(())
}

async fn empty_inputs(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.runs.count", json!({ "total": 0 }));
    let outcome = rig.run(&CountRunsNode, json!({ "instance": "", "tag": "  " })).await.ok()?;
    assert_eq!(outcome.outputs["count"], json!(0));
    match &rig.program_calls()[0].0 {
        ProgramCall::RunsCount { filter } => assert_eq!(filter, &RunFilter::default()),
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn bad_age(rig: FakeRig) -> WeftResult<()> {
    for age in [json!(-1), json!(1.5)] {
        let err = rig.run(&CountRunsNode, json!({ "olderThanSecs": age })).await.failure()?;
        assert!(err.contains("olderThanSecs"), "{err}");
    }
    assert!(rig.program_calls().is_empty());
    Ok(())
}

async fn unknown_status(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&CountRunsNode, json!({ "status": "done" })).await.failure()?;
    assert!(err.contains("'done'") && err.contains("waiting_for_input"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
