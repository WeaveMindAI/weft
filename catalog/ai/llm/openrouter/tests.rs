//! OpenRouterProvider self-tests: the emitted `LlmProvider` object,
//! including the routing knobs riding it verbatim.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::OpenRouterProviderNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("routing_knobs_ride_the_provider_object", emits_with_knobs)]
}

async fn emits_with_knobs(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &OpenRouterProviderNode,
            json!({
                "connection": rig.access("openrouter"),
                "model": "anthropic/claude-sonnet-4.5",
                "servingProvider": "anthropic",
                "providerFallbacks": false,
            }),
        )
        .await
        .ok()?;
    let provider = &outcome.outputs["provider"];
    assert_eq!(provider["kind"], json!("openrouter"));
    assert_eq!(provider["model"], json!("anthropic/claude-sonnet-4.5"));
    assert_eq!(provider["servingProvider"], json!("anthropic"));
    assert_eq!(provider["providerFallbacks"], json!(false));
    assert!(provider["account"].get("__weft_access__").is_some());
    Ok(())
}
