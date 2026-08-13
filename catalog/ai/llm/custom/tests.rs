//! CustomProvider self-tests: an OpenAI-compatible endpoint may run
//! without a connection.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::CustomProviderNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("an_unauthenticated_endpoint_is_allowed", unauthenticated)]
}

async fn unauthenticated(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &CustomProviderNode,
            json!({ "baseUrl": "http://localhost:11434/v1", "model": "llama3", "name": "Ollama" }),
        )
        .await
        .ok()?;
    let provider = &outcome.outputs["provider"];
    assert_eq!(provider["kind"], json!("custom"));
    assert_eq!(provider["baseUrl"], json!("http://localhost:11434/v1"));
    assert_eq!(provider["name"], json!("Ollama"));
    assert!(provider.get("account").is_none(), "no connection, no marker");
    Ok(())
}
