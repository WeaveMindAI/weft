//! Text self-tests: the literal flows through unchanged.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::TextNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("emits_the_configured_literal", emits_literal)]
}

async fn emits_literal(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&TextNode, json!({ "value": "hello world" })).await.ok()?;
    assert_eq!(outcome.outputs["value"], json!("hello world"));
    Ok(())
}
