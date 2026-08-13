//! OpenAIProvider self-tests: the emitted `LlmProvider` object.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::OpenAIProviderNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("emits_kind_model_and_the_picked_connection", emits_provider)]
}

async fn emits_provider(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &OpenAIProviderNode,
            json!({ "connection": rig.access("openai"), "model": "gpt-4o-mini" }),
        )
        .await
        .ok()?;
    let provider = &outcome.outputs["provider"];
    assert_eq!(provider["kind"], json!("openai"));
    assert_eq!(provider["model"], json!("gpt-4o-mini"));
    assert!(provider["account"].get("__weft_access__").is_some());
    Ok(())
}
