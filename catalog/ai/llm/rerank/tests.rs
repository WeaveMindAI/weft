//! LlmRerank self-tests: reordering by the answered indices and the
//! loud refusals.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::LlmRerankNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("reorders_documents_best_first", reorders),
        NodeTest::fake("an_index_past_the_documents_fails_loud", index_out_of_range),
        NodeTest::fake("empty_documents_are_refused", empty_documents),
        NodeTest::live("one_real_rerank", "openrouter", live_rerank),
    ]
}

async fn live_rerank(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmRerankNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "cohere/rerank-v3.5",
                    "account": rig.access("openrouter"),
                },
                "query": "what is the capital of France",
                "documents": [
                    "Bananas are rich in potassium.",
                    "Paris is the capital of France.",
                    "Rust has a borrow checker.",
                ],
                "topN": 1,
            }),
        )
        .await
        .ok()?;
    assert_eq!(
        outcome.output("documents")?,
        &json!(["Paris is the capital of France."]),
        "the on-topic document ranks first"
    );
    Ok(())
}

fn provider(rig: &FakeRig) -> serde_json::Value {
    json!({ "kind": "openrouter", "model": "test/rerank", "account": rig.access("openrouter") })
}

async fn reorders(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/v1/rerank",
        json!({ "results": [
            { "index": 2, "relevance_score": 0.9 },
            { "index": 0, "relevance_score": 0.3 },
        ]}),
    );
    let outcome = rig
        .run(
            &LlmRerankNode,
            json!({
                "provider": provider(&rig),
                "query": "best fruit",
                "documents": ["carrot", "brick", "apple"],
                "topN": 2,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["documents"], json!(["apple", "carrot"]));
    assert_eq!(outcome.outputs["scores"][0]["index"], json!(2));
    let body = rig.requests()[0].body.clone().expect("call body");
    assert_eq!(body["top_n"], json!(2));
    Ok(())
}

async fn index_out_of_range(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/v1/rerank",
        json!({ "results": [{ "index": 9, "relevance_score": 0.9 }]}),
    );
    let outcome = rig
        .run(
            &LlmRerankNode,
            json!({ "provider": provider(&rig), "query": "q", "documents": ["only one"] }),
        )
        .await;
    let err = outcome.result.expect_err("stray index must refuse").to_string();
    assert!(err.contains("past the document list"), "{err}");
    Ok(())
}

async fn empty_documents(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmRerankNode,
            json!({ "provider": provider(&rig), "query": "q", "documents": [] }),
        )
        .await;
    let err = outcome.result.expect_err("empty documents must refuse").to_string();
    assert!(err.contains("empty"), "{err}");
    Ok(())
}
