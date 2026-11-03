//! InstanceCosts self-tests: the filters reach the query, the total adds
//! the measured amounts only and `unpricedCalls` counts the rest, and a
//! bad `paidBy` or `run` is refused before anything is asked.

use serde_json::json;

use weft::program::{PaidBy, ProgramCall};
use weft::{FakeRig, NodeTest, RunId, WeftResult};

use super::InstanceCostsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("totals_an_instances_costs", totals_an_instances_costs),
        NodeTest::fake("a_bad_paid_by_is_refused", a_bad_paid_by),
        NodeTest::fake("an_instance_with_no_costs_totals_zero", no_costs),
        NodeTest::fake("the_run_filter_reaches_the_query", run_filter),
        NodeTest::fake("a_run_that_is_no_run_id_is_refused", a_bad_run),
    ]
}

async fn totals_an_instances_costs(rig: FakeRig) -> WeftResult<()> {
    let run = "00000000-0000-0000-0000-000000000001";
    rig.answer_program_call(
        "weft.costs.list",
        json!([
            { "run": run, "instance": "ada", "node": "reply", "service": "openrouter", "amountUsd": 0.25, "paidBy": "platform", "atUnix": 10 },
            { "run": run, "instance": "ada", "node": "reply", "service": "openrouter", "amountUsd": null, "paidBy": "platform", "atUnix": 11 }
        ]),
    );
    let outcome = rig
        .run(&InstanceCostsNode, json!({ "instance": "ada", "service": "openrouter", "paidBy": "platform", "since": 5 }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["totalUsd"], json!(0.25));
    assert_eq!(outcome.outputs["unpricedCalls"], json!(1), "the null amount is counted, so the total says it is short");
    assert_eq!(outcome.outputs["records"].as_array().map(Vec::len), Some(2));
    match &rig.program_calls()[0].0 {
        ProgramCall::Runs(weft::program::runs::RunCall::Costs { filter }) => {
            assert_eq!(filter.instance.as_ref().map(|i| i.as_str()), Some("ada"));
            assert_eq!(filter.service.as_deref(), Some("openrouter"));
            assert_eq!(filter.paid_by, Some(PaidBy::Platform));
            assert_eq!(filter.since_unix, Some(5));
            assert_eq!(filter.run, None, "no `run`, every run of the instance");
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn no_costs(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.costs.list", json!([]));
    let outcome = rig.run(&InstanceCostsNode, json!({ "instance": "ada" })).await.ok()?;
    // A plain 0, never -0 (what an f64 `sum` of nothing gives).
    assert_eq!(outcome.outputs["totalUsd"].to_string(), "0.0");
    assert_eq!(outcome.outputs["unpricedCalls"], json!(0));
    Ok(())
}

async fn run_filter(rig: FakeRig) -> WeftResult<()> {
    let run = "00000000-0000-0000-0000-000000000003";
    rig.answer_program_call(
        "weft.costs.list",
        json!([
            { "run": run, "instance": "ada", "node": "reply", "service": "openrouter", "amountUsd": 0.5, "paidBy": "platform", "atUnix": 10 }
        ]),
    );
    let outcome = rig.run(&InstanceCostsNode, json!({ "instance": "ada", "run": run })).await.ok()?;
    assert_eq!(outcome.outputs["totalUsd"], json!(0.5));
    assert_eq!(outcome.outputs["unpricedCalls"], json!(0));
    match &rig.program_calls()[0].0 {
        ProgramCall::Runs(weft::program::runs::RunCall::Costs { filter }) => {
            assert_eq!(filter.run, Some(run.parse::<RunId>().unwrap()));
            assert_eq!(filter.instance.as_ref().map(|i| i.as_str()), Some("ada"));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn a_bad_run(rig: FakeRig) -> WeftResult<()> {
    for run in ["run-7", ""] {
        let err = rig.run(&InstanceCostsNode, json!({ "instance": "ada", "run": run })).await.failure()?;
        assert!(err.contains("input 'run'"), "{err}");
    }
    assert!(rig.program_calls().is_empty());
    Ok(())
}

async fn a_bad_paid_by(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&InstanceCostsNode, json!({ "instance": "ada", "paidBy": "someone" })).await.failure()?;
    assert!(err.contains("paidBy") && err.contains("someone") && err.contains("`instance`"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
