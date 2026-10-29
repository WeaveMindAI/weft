//! LlmTool self-tests: the emitted `LlmTool` record, which an LLM node
//! decodes as minillmlib's `ToolDefinition`.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::LlmToolNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_declared_schema_rides_verbatim", declared_schema),
        NodeTest::fake("no_schema_means_no_arguments", empty_schema),
        NodeTest::fake("the_tool_reads_as_the_libs_definition", reads_as_definition),
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

/// What the LLM nodes do with each item of `tools`: decode it as the
/// lib's `ToolDefinition`. A field the record and the struct disagree
/// on fails here instead of on the first paid call.
async fn reads_as_definition(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&LlmToolNode, json!({ "name": "get_weather", "description": "current weather" }))
        .await
        .ok()?;
    let def: minillmlib::ToolDefinition =
        serde_json::from_value(outcome.outputs["tool"].clone()).expect("an LlmTool is a ToolDefinition");
    assert_eq!(def.name, "get_weather");
    assert_eq!(def.description.as_deref(), Some("current weather"));
    assert_eq!(def.parameters, json!({ "type": "object", "properties": {} }));
    Ok(())
}
