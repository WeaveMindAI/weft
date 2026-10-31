//! OpenRouterDecisionModel self-tests: the emitted `DecisionProvider`
//! object, with the routing settings riding it as OpenRouter's routing
//! block.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::OpenRouterDecisionModelNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_model_and_connection_ride_the_provider_object", emits),
        NodeTest::fake("routing_settings_ride_as_openrouters_block", emits_with_knobs),
        NodeTest::fake("one_routing_setting_alone", emits_with_one_knob),
    ]
}

async fn emits(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &OpenRouterDecisionModelNode,
            json!({ "access": rig.access("openrouter"), "model": "typesafe/jev-1.14" }),
        )
        .await
        .ok()?;
    let provider = &outcome.outputs["provider"];
    assert_eq!(provider["kind"], json!("openrouter"));
    assert_eq!(provider["model"], json!("typesafe/jev-1.14"));
    assert!(provider["account"].get("__weft_access__").is_some());
    assert!(provider.get("access").is_none(), "the wired connection rides as `account` only");
    assert!(provider.get("routing").is_none(), "no routing setting, no routing block");
    Ok(())
}

async fn emits_with_knobs(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &OpenRouterDecisionModelNode,
            json!({
                "access": rig.access("openrouter"),
                "model": "perplexity/pplx-decider-flash-v1.2",
                "servingProvider": "perplexity",
                "providerFallbacks": false,
            }),
        )
        .await
        .ok()?;
    let provider = &outcome.outputs["provider"];
    assert_eq!(provider["routing"], json!({ "order": ["perplexity"], "allow_fallbacks": false }));
    Ok(())
}

async fn emits_with_one_knob(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &OpenRouterDecisionModelNode,
            json!({ "access": rig.access("openrouter"), "model": "m", "providerFallbacks": true }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["provider"]["routing"], json!({ "allow_fallbacks": true }), "an unset setting is left out");
    Ok(())
}
