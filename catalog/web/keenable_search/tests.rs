//! KeenableSearch self-tests. `fake` pins the request the node builds,
//! the route it picks with and without an account, and the result
//! shaping against a canned Keenable answer; `live` runs one real
//! single-result search on each route (keyless and keyed).

use serde_json::json;

use weft::storage::{wire_weight, MAX_WIRE_VALUE_BYTES};
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::{results_from, search_body, KeenableSearchNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("the_text_is_the_snippet_cut_at_the_cap", text_from_snippet),
        NodeTest::basic("an_empty_site_searches_the_whole_web", empty_site),
        NodeTest::fake("without_an_account_it_searches_the_public_route", keyless_search),
        NodeTest::fake("with_an_account_it_searches_the_keyed_route", keyed_search),
        NodeTest::fake("texts_heavier_than_their_characters_are_cut_to_fit", heavy_texts),
        NodeTest::fake("a_refusal_fails_with_keenables_words", refusal),
        NodeTest::live("one_real_keyless_search", "keenable", live_keyless_search),
        NodeTest::live("one_real_keyed_search", "keenable", live_keyed_search),
    ]
}

fn text_from_snippet() -> WeftResult<()> {
    let answer = json!({ "results": [
        { "title": "Weft", "url": "https://weft.dev", "description": "",
          "snippet": "a workflow language", "acquired_at": "2026-10-01T00:00:00Z" },
        { "title": null, "url": "https://ex.com/long", "description": "", "snippet": "x".repeat(50) },
        { "title": "No text", "url": "https://ex.com/none" },
    ]});
    let results = results_from(&answer, 10);
    assert_eq!(
        results[0],
        json!({ "title": "Weft", "url": "https://weft.dev", "text": "a workflow" }),
        "the text comes from `snippet`, cut at the cap, and only the declared fields stay"
    );
    assert_eq!(results[1]["text"], json!("x".repeat(10)));
    assert_eq!(results[1]["title"], json!(null), "a missing title stays null");
    assert_eq!(results[2]["text"], json!(null), "a result without a snippet has no text");
    Ok(())
}

fn empty_site() -> WeftResult<()> {
    assert_eq!(search_body("q", 5, Some("  ")), json!({ "query": "q", "max_results": 5 }));
    assert_eq!(search_body("q", 5, None), json!({ "query": "q", "max_results": 5 }));
    assert_eq!(
        search_body("q", 5, Some(" docs.rs ")),
        json!({ "query": "q", "max_results": 5, "site": "docs.rs" })
    );
    Ok(())
}

async fn keyless_search(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/search/public",
        json!({ "query": "weft workflow language", "results": [
            { "title": "Weft", "url": "https://weft.dev", "description": "",
              "snippet": "a workflow language", "acquired_at": "2026-10-01T00:00:00Z" },
        ]}),
    );
    let outcome = rig
        .run(
            &KeenableSearchNode,
            json!({ "query": "weft workflow language", "numResults": 3, "site": "weft.dev" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(1.0));
    assert_eq!(
        outcome.outputs["results"][0],
        json!({ "title": "Weft", "url": "https://weft.dev", "text": "a workflow language" }),
        "results are shaped to the declared fields only"
    );

    let sent = &rig.requests()[0];
    assert_eq!(sent.path, "/v1/search/public");
    assert_eq!(sent.header("x-keenable-title"), Some("weft"), "the public route needs the app named");
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({ "query": "weft workflow language", "max_results": 3, "site": "weft.dev" }),
    );
    Ok(())
}

async fn keyed_search(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/search", json!({ "results": [] }));
    let outcome = rig
        .run(&KeenableSearchNode, json!({ "account": rig.access("keenable"), "query": "q" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(0.0));
    let sent = &rig.requests()[0];
    assert_eq!(sent.path, "/v1/search", "a wired account searches the keyed route");
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({ "query": "q", "max_results": 10 }),
        "the manifest's default count rides the request"
    );
    Ok(())
}

/// Ten texts of 4000 characters three bytes each weigh about 120 KB:
/// within `maxTextChars`, past the wire, so the node shortens them.
async fn heavy_texts(rig: FakeRig) -> WeftResult<()> {
    let results: Vec<_> = (0..10)
        .map(|i| json!({ "title": format!("r{i}"), "url": format!("https://ex.com/{i}"), "snippet": "日".repeat(4000) }))
        .collect();
    rig.respond("POST", "/v1/search/public", json!({ "results": results }));
    let outcome = rig.run(&KeenableSearchNode, json!({ "query": "q" })).await.ok()?;
    let results = outcome.output("results")?;
    assert!(wire_weight(results) <= MAX_WIRE_VALUE_BYTES, "the results fit a wire");
    assert_eq!(outcome.outputs["count"], json!(10.0), "every result is kept");
    for r in results.as_array().expect("a list") {
        let chars = r["text"].as_str().expect("text").chars().count();
        assert!(chars > 3000 && chars < 4000, "each text is shortened evenly, not dropped: {chars}");
    }
    Ok(())
}

async fn refusal(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status(
        "POST",
        "/v1/search/public",
        429,
        json!({ "error": "Rate limit exceeded" }),
    );
    let err = rig.run(&KeenableSearchNode, json!({ "query": "q" })).await.failure()?;
    assert!(err.contains("429") && err.contains("Rate limit exceeded"), "{err}");
    Ok(())
}

async fn live_keyless_search(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(&KeenableSearchNode, json!({ "query": "rust programming language", "numResults": 1 }))
        .await
        .ok()?;
    assert_live_result(outcome.output("results")?, outcome.output("count")?)
}

async fn live_keyed_search(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &KeenableSearchNode,
            json!({ "account": rig.access("keenable"), "query": "rust programming language", "numResults": 1 }),
        )
        .await
        .ok()?;
    assert_live_result(outcome.output("results")?, outcome.output("count")?)
}

fn assert_live_result(results: &serde_json::Value, count: &serde_json::Value) -> WeftResult<()> {
    assert!(count.as_f64().unwrap_or(0.0) >= 1.0, "a real search answers at least one result");
    let first = &results[0];
    assert!(
        first["url"].as_str().is_some_and(|u| u.starts_with("http")),
        "a result carries a real URL: {first}"
    );
    assert!(
        first["text"].as_str().is_some_and(|t| !t.is_empty()),
        "a result carries page text from its snippet: {first}"
    );
    Ok(())
}
