//! WebSearch self-tests. `fake` pins the request the node builds and
//! the result shaping against a canned Exa answer; `live` runs one
//! real single-result search through the production access path (the
//! cheap route: one small search).

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::WebSearchNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("builds_the_search_and_shapes_the_results", canned_search),
        NodeTest::live("one_real_single_result_search", "exa", live_search),
    ]
}

async fn canned_search(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/search",
        json!({ "results": [
            { "title": "Weft", "url": "https://weft.dev", "publishedDate": "2026-01-01",
              "text": "a workflow language", "extra": "dropped" },
        ]}),
    );
    let outcome = rig
        .run(
            &WebSearchNode,
            json!({
                "account": rig.access("exa"),
                "query": "weft workflow language",
                "numResults": 3,
                "includeText": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(1.0));
    assert_eq!(
        outcome.outputs["results"][0],
        json!({ "title": "Weft", "url": "https://weft.dev",
                "publishedDate": "2026-01-01", "text": "a workflow language" }),
        "results are shaped to the declared fields only"
    );

    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({ "query": "weft workflow language", "type": "auto",
                 "numResults": 3, "contents": { "text": true } })
    );
    Ok(())
}

async fn live_search(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &WebSearchNode,
            json!({
                "account": rig.access("exa"),
                "query": "rust programming language",
                "numResults": 1,
                "includeText": false,
            }),
        )
        .await
        .ok()?;
    let count = outcome.output("count")?.as_f64().unwrap_or(0.0);
    assert!(count >= 1.0, "a real search answers at least one result");
    let first = &outcome.output("results")?[0];
    assert!(
        first["url"].as_str().is_some_and(|u| u.starts_with("http")),
        "a result carries a real URL: {first}"
    );
    Ok(())
}
