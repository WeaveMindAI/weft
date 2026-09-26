//! MemberCosts self-tests: the filters reach the query, the total adds
//! the measured amounts only, and a bad `paidBy` is refused before
//! anything is asked.

use serde_json::json;

use weft::program::{PaidBy, ProgramCall};
use weft::{FakeRig, NodeTest, WeftResult};

use super::MemberCostsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("totals_a_members_costs", totals_a_members_costs),
        NodeTest::fake("a_bad_paid_by_is_refused", a_bad_paid_by),
        NodeTest::fake("a_member_with_no_costs_totals_zero", no_costs),
    ]
}

async fn totals_a_members_costs(rig: FakeRig) -> WeftResult<()> {
    let run = "00000000-0000-0000-0000-000000000001";
    rig.answer_program_call(
        "weft.costs.list",
        json!([
            { "run": run, "member": "ada", "node": "reply", "service": "openrouter", "amount_usd": 0.25, "paid_by": "platform", "at_unix": 10 },
            { "run": run, "member": "ada", "node": "reply", "service": "openrouter", "amount_usd": null, "paid_by": "platform", "at_unix": 11 }
        ]),
    );
    let outcome = rig
        .run(&MemberCostsNode, json!({ "member": "ada", "service": "openrouter", "paidBy": "platform", "since": 5 }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["totalUsd"], json!(0.25));
    assert_eq!(outcome.outputs["records"].as_array().map(Vec::len), Some(2));
    match &rig.program_calls()[0].0 {
        ProgramCall::CostsList { filter } => {
            assert_eq!(filter.member.as_ref().map(|m| m.as_str()), Some("ada"));
            assert_eq!(filter.service.as_deref(), Some("openrouter"));
            assert_eq!(filter.paid_by, Some(PaidBy::Platform));
            assert_eq!(filter.since_unix, Some(5));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn no_costs(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.costs.list", json!([]));
    let outcome = rig.run(&MemberCostsNode, json!({ "member": "ada" })).await.ok()?;
    // A plain 0, never -0 (what an f64 `sum` of nothing gives).
    assert_eq!(outcome.outputs["totalUsd"].to_string(), "0.0");
    Ok(())
}

async fn a_bad_paid_by(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&MemberCostsNode, json!({ "member": "ada", "paidBy": "someone" })).await.failure()?;
    assert!(err.contains("platform, author or member"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
