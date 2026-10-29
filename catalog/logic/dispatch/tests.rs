//! Dispatch self-tests: the branch whose `by` field equals the value's
//! takes it, `otherwise` takes what none claims, and every shape that
//! would make the choice a guess fails the node naming what to fix.

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::{claimant, DispatchNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("the_branch_with_the_same_field_claims_the_value", claims),
        NodeTest::basic("a_value_no_branch_claims_goes_to_otherwise", unclaimed),
        NodeTest::basic("a_value_without_the_field_fails", value_without_field),
        NodeTest::basic("a_branch_without_the_field_fails_naming_it", branch_without_field),
        NodeTest::basic("two_branches_with_the_same_field_fail_naming_both", same_field_twice),
        NodeTest::basic("by_picks_the_field_compared", other_field),
        NodeTest::fake("only_the_claiming_branch_emits", node_claims),
        NodeTest::fake("an_unclaimed_value_leaves_on_otherwise", node_unclaimed),
        NodeTest::fake("a_branch_that_received_nothing_fails", silent_branch),
    ]
}

fn tool(name: &str) -> Value {
    json!({ "name": name, "description": format!("the {name} tool"), "parameters": {} })
}

fn call(name: &str) -> Value {
    json!({ "id": "call_1", "name": name, "arguments": "{}" })
}

fn claims() -> WeftResult<()> {
    let (search, read) = (tool("search"), tool("read_file"));
    let branches = [("search", &search), ("readFile", &read)];
    assert_eq!(claimant(&call("read_file"), "name", &branches)?, Some("readFile"));
    assert_eq!(claimant(&call("search"), "name", &branches)?, Some("search"));
    Ok(())
}

fn unclaimed() -> WeftResult<()> {
    let search = tool("search");
    assert_eq!(claimant(&call("delete_everything"), "name", &[("search", &search)])?, None);
    assert_eq!(claimant(&call("search"), "name", &[])?, None, "no branches, nothing claims it");
    Ok(())
}

fn value_without_field() -> WeftResult<()> {
    let search = tool("search");
    let error = claimant(&json!({ "id": "call_1" }), "name", &[("search", &search)])
        .expect_err("the value has no name")
        .to_string();
    assert!(error.contains("value to dispatch has no `name` field"), "{error}");
    let error = claimant(&json!("search"), "name", &[("search", &search)])
        .expect_err("a string has no fields")
        .to_string();
    assert!(error.contains("not an object"), "{error}");
    Ok(())
}

fn branch_without_field() -> WeftResult<()> {
    let (search, nameless) = (tool("search"), json!({ "description": "no name" }));
    let error = claimant(&call("search"), "name", &[("search", &search), ("mystery", &nameless)])
        .expect_err("a branch has no name")
        .to_string();
    assert!(error.contains("branch `mystery` has no `name` field"), "{error}");
    Ok(())
}

/// Equal keys fail even when the value matches neither: the program is
/// wrong whatever arrives, so it never depends on which call came in.
fn same_field_twice() -> WeftResult<()> {
    let (first, second) = (tool("search"), tool("search"));
    for incoming in ["search", "other"] {
        let error = claimant(&call(incoming), "name", &[("webSearch", &first), ("docSearch", &second)])
            .expect_err("two branches named alike")
            .to_string();
        assert!(error.contains("`webSearch` and `docSearch`"), "{error}");
    }
    Ok(())
}

/// `by` names the field on both sides, and numbers compare by value
/// (`2` claims `2.0`), the way Switch compares them.
fn other_field() -> WeftResult<()> {
    let (low, high) = (json!({ "level": 1 }), json!({ "level": 2 }));
    let branches = [("low", &low), ("high", &high)];
    assert_eq!(claimant(&json!({ "level": 2.0, "name": "low" }), "level", &branches)?, Some("high"));
    let error = claimant(&call("search"), "level", &branches).expect_err("a call has no level").to_string();
    assert!(error.contains("no `level` field"), "{error}");
    Ok(())
}

/// The branches a compiled Dispatch holds: an input per branch, in
/// written order, and the output of the same name the compiler makes.
fn branches(rig: &FakeRig, names: &[&str]) {
    let record = WeftType::parse("JsonDict").expect("parses");
    for name in names {
        rig.input_type(name, record.clone());
        rig.output_type(name, record.clone());
    }
}

async fn node_claims(rig: FakeRig) -> WeftResult<()> {
    branches(&rig, &["search", "readFile"]);
    let outcome = rig
        .run(
            &DispatchNode,
            json!({ "value": call("read_file"), "search": tool("search"), "readFile": tool("read_file") }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["readFile"], call("read_file"), "the call itself goes on");
    assert_eq!(outcome.outputs.len(), 1, "every other output stays unset, so it closes: {:?}", outcome.outputs);
    assert!(outcome.closed_ports.is_empty(), "nothing is closed by hand: {:?}", outcome.closed_ports);
    Ok(())
}

async fn node_unclaimed(rig: FakeRig) -> WeftResult<()> {
    branches(&rig, &["search"]);
    let outcome = rig
        .run(&DispatchNode, json!({ "value": call("read_file"), "search": tool("search") }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["otherwise"], call("read_file"));
    assert_eq!(outcome.outputs.len(), 1, "no branch emits: {:?}", outcome.outputs);
    Ok(())
}

async fn silent_branch(rig: FakeRig) -> WeftResult<()> {
    branches(&rig, &["search", "readFile"]);
    let error = rig
        .run(&DispatchNode, json!({ "value": call("search"), "search": tool("search") }))
        .await
        .failure()?;
    assert!(error.contains("branch `readFile` received nothing"), "{error}");
    Ok(())
}
