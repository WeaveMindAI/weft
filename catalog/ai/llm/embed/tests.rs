//! LlmEmbed self-tests: the embeddings call and its refusals.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::LlmEmbedNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("embeds_texts_and_reports_dimensions", embeds),
        NodeTest::fake("a_non_openrouter_provider_is_refused", wrong_kind),
        NodeTest::fake("empty_texts_are_refused", empty_texts),
        NodeTest::fake("out_of_order_embeddings_land_by_their_index", out_of_order),
        NodeTest::live("one_real_embedding", "openrouter", live_embedding),
    ]
}

async fn embeds(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/v1/embeddings",
        json!({ "data": [
            { "index": 0, "embedding": [0.1, 0.2] },
            { "index": 1, "embedding": [0.3, 0.4] },
        ]}),
    );
    let outcome = rig
        .run(
            &LlmEmbedNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "openai/text-embedding-3-small",
                    "account": rig.access("openrouter"),
                },
                "texts": ["a", "b"],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["embeddings"], json!([[0.1, 0.2], [0.3, 0.4]]));
    assert_eq!(outcome.outputs["dimensions"], json!(2.0));
    let body = rig.requests()[0].body.clone().expect("call body");
    assert_eq!(body["input"], json!(["a", "b"]));
    Ok(())
}

/// The answer arrives with its entries REVERSED: each vector must land
/// at the position its `index` names, so the output pairs input-order.
async fn out_of_order(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/v1/embeddings",
        json!({ "data": [
            { "index": 1, "embedding": [0.3, 0.4] },
            { "index": 0, "embedding": [0.1, 0.2] },
        ]}),
    );
    let outcome = rig
        .run(
            &LlmEmbedNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "openai/text-embedding-3-small",
                    "account": rig.access("openrouter"),
                },
                "texts": ["a", "b"],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["embeddings"], json!([[0.1, 0.2], [0.3, 0.4]]));
    Ok(())
}

async fn wrong_kind(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmEmbedNode,
            json!({
                "provider": { "kind": "openai", "model": "x", "account": rig.access("openai") },
                "texts": "a",
            }),
        )
        .await;
    let err = outcome.result.expect_err("openai kind must refuse").to_string();
    assert!(err.contains("OpenRouterProvider"), "{err}");
    Ok(())
}

async fn empty_texts(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmEmbedNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "x",
                    "account": rig.access("openrouter"),
                },
                "texts": [],
            }),
        )
        .await;
    let err = outcome.result.expect_err("empty texts must refuse").to_string();
    assert!(err.contains("empty"), "{err}");
    Ok(())
}

async fn live_embedding(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmEmbedNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "openai/text-embedding-3-small",
                    "account": rig.access("openrouter"),
                },
                "texts": "hello world",
            }),
        )
        .await
        .ok()?;
    let dims = outcome.output("dimensions")?.as_f64().expect("dimension count");
    assert!(dims > 100.0, "a real embedding has real dimensions, got {dims}");
    Ok(())
}
