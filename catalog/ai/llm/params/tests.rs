//! LlmParams self-tests: the config fields forward as one object.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::LlmParamsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("forwards_every_configured_field", forwards_fields)]
}

async fn forwards_fields(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmParamsNode,
            json!({ "systemPrompt": "be terse", "temperature": 0.2, "maxTokens": 64 }),
        )
        .await
        .ok()?;
    let params = &outcome.outputs["params"];
    assert_eq!(params["systemPrompt"], json!("be terse"));
    assert_eq!(params["temperature"], json!(0.2));
    assert_eq!(params["maxTokens"], json!(64));
    Ok(())
}
