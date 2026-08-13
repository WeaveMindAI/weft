//! AnthropicProvider self-tests: the emitted `LlmProvider` object.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::AnthropicProviderNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("emits_kind_model_and_the_picked_connection", emits_provider),
        NodeTest::fake("refuses_without_a_connection", refuses_without_connection),
    ]
}

async fn emits_provider(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &AnthropicProviderNode,
            json!({ "connection": rig.access("anthropic"), "model": "claude-sonnet-4-5" }),
        )
        .await
        .ok()?;
    let provider = &outcome.outputs["provider"];
    assert_eq!(provider["kind"], json!("anthropic"));
    assert_eq!(provider["model"], json!("claude-sonnet-4-5"));
    assert!(provider["account"].get("__weft_access__").is_some(), "carries the marker");
    Ok(())
}

async fn refuses_without_connection(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&AnthropicProviderNode, json!({ "model": "claude-sonnet-4-5" })).await;
    let err = outcome.result.expect_err("no connection must refuse").to_string();
    assert!(err.contains("no connection picked"), "{err}");
    Ok(())
}
