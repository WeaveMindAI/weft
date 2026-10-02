//! LlmTool self-tests: the emitted ToolDefinition object.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::LlmToolNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_declared_schema_rides_verbatim", declared_schema),
        NodeTest::fake("no_schema_means_no_arguments", empty_schema),
    ]
}

async fn declared_schema(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmToolNode,
            json!({
                "name": "get_weather",
                "description": "current weather",
                "parameters": { "type": "object", "properties": { "city": { "type": "string" } } },
            }),
        )
        .await
        .ok()?;
    let tool = &outcome.outputs["tool"];
    assert_eq!(tool["name"], json!("get_weather"));
    assert_eq!(tool["description"], json!("current weather"));
    assert_eq!(tool["parameters"]["properties"]["city"]["type"], json!("string"));
    Ok(())
}

async fn empty_schema(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&LlmToolNode, json!({ "name": "get_time" })).await.ok()?;
    let tool = &outcome.outputs["tool"];
    assert_eq!(tool["parameters"], json!({ "type": "object", "properties": {} }));
    assert!(tool.get("description").is_none());
    Ok(())
}
