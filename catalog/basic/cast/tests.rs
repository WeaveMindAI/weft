//! Cast self-tests: the declared output type drives the conversion
//! (the rig plays the compiler's MustOverride-resolution role via
//! `rig.output_type`).

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::CastNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("text_parses_into_the_declared_number", text_to_number),
        NodeTest::fake("a_scalar_stringifies_into_declared_text", scalar_to_text),
        NodeTest::fake("an_impossible_value_fails_loud", impossible_value),
    ]
}

async fn text_to_number(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("value", WeftType::parse("Number").expect("Number parses"));
    let outcome = rig.run(&CastNode, json!({ "value": "42.5" })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!(42.5));
    Ok(())
}

async fn scalar_to_text(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("value", WeftType::parse("String").expect("String parses"));
    let outcome = rig.run(&CastNode, json!({ "value": 42.5 })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!("42.5"));
    Ok(())
}

async fn impossible_value(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("value", WeftType::parse("Number").expect("Number parses"));
    let outcome = rig.run(&CastNode, json!({ "value": "not a number" })).await;
    let err = outcome.result.expect_err("unparseable text must refuse").to_string();
    assert!(err.contains("cannot cast"), "{err}");
    Ok(())
}
