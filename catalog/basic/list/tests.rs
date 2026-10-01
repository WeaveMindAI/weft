//! List self-tests: the items come out in written order whatever their
//! names, a value of any shape goes in whole, a silent optional port is
//! left out without a gap, and a List with no ports is refused.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::ListNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("items_keep_the_order_they_were_written", written_order),
        NodeTest::fake("a_value_of_any_shape_goes_in_whole", any_shape),
        NodeTest::fake("a_silent_optional_port_is_left_out", silent_optional),
        NodeTest::fake("no_ports_is_refused", no_ports),
    ]
}

fn declare(rig: &FakeRig, ports: &[(&str, &str)]) {
    for (port, ty) in ports {
        rig.input_type(port, WeftType::parse(ty).expect("type parses"));
    }
}

async fn written_order(rig: FakeRig) -> WeftResult<()> {
    declare(&rig, &[("zebra", "Number"), ("apple", "Number"), ("moose", "Number")]);
    let outcome =
        rig.run(&ListNode, json!({ "zebra": 1, "apple": 2, "moose": 3 })).await.ok()?;
    assert_eq!(outcome.outputs["list"], json!([1, 2, 3]), "written order, not sorted by name");
    Ok(())
}

async fn any_shape(rig: FakeRig) -> WeftResult<()> {
    declare(&rig, &[("card", "JsonDict"), ("tags", "List[String]")]);
    let outcome = rig
        .run(&ListNode, json!({ "card": { "title": "hi" }, "tags": ["a", "b"] }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["list"], json!([{ "title": "hi" }, ["a", "b"]]));
    Ok(())
}

/// An optional port that received nothing is absent from the bag; the
/// items after it move up instead of leaving a null behind.
async fn silent_optional(rig: FakeRig) -> WeftResult<()> {
    declare(&rig, &[("first", "String"), ("middle", "String"), ("last", "String")]);
    let outcome = rig.run(&ListNode, json!({ "first": "a", "last": "c" })).await.ok()?;
    assert_eq!(outcome.outputs["list"], json!(["a", "c"]));
    Ok(())
}

async fn no_ports(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&ListNode, json!({})).await.failure()?;
    assert!(err.contains("declares no input ports"), "{err}");
    Ok(())
}
