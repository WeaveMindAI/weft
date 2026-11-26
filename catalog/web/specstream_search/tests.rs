//! SpecStreamSearch self-tests. `fake` pins the request the node builds,
//! the key it signs with and the result shaping against a canned
//! SpecStream answer; `live` runs one real single-result search.

use serde_json::json;

use weft::storage::{wire_weight, MAX_WIRE_VALUE_BYTES};
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::{results_from, search_body, SpecStreamSearchNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("the_text_is_the_snippet_cut_at_the_cap", text_from_snippet),
        NodeTest::basic("a_site_rides_in_the_query_only_when_set", site_rides),
        NodeTest::basic("the_server_cut_stays_within_what_specstream_takes", server_cut_bounds),
        NodeTest::fake("builds_the_search_and_shapes_the_results", canned_search),
        NodeTest::fake("texts_heavier_than_their_characters_are_cut_to_fit", heavy_texts),
        NodeTest::fake("a_refusal_fails_with_specstreams_words", refusal),
        NodeTest::live("one_real_single_result_search", "specstream", live_search),
    ]
}

fn text_from_snippet() -> WeftResult<()> {
    let answer = json!({ "data": { "query": "q", "sources": [
        { "title": "Weft", "url": "https://weft.dev", "published": "2026-10-01",
          "modified": "2026-11-01", "snippet": "a workflow language" },
        { "title": null, "url": "https://ex.com/long", "snippet": "x".repeat(50) },
        { "title": "No text", "url": "https://ex.com/none" },
        { "title": "No url", "snippet": "dropped" },
    ], "took_ms": 812 }});
    let results = results_from(&answer, 10);
    assert_eq!(results.len(), 3, "a source without a url is dropped");
    assert_eq!(
        results[0],
        json!({ "title": "Weft", "url": "https://weft.dev", "publishedDate": "2026-10-01", "text": "a workflow" }),
        "the text comes from `snippet`, cut at the cap, and only the declared fields stay"
    );
    assert_eq!(results[1]["text"], json!("x".repeat(10)));
    assert_eq!(results[1]["title"], json!(null), "a missing title stays null");
    assert_eq!(results[1]["publishedDate"], json!(null), "a missing date stays null");
    assert_eq!(results[2]["text"], json!(null), "a source without a snippet has no text");
    Ok(())
}

fn site_rides() -> WeftResult<()> {
    assert_eq!(
        search_body("q", 5, None, 4000),
        json!({ "q": "q", "num": 5, "compact": true, "source_max_chars": 4000 })
    );
    assert_eq!(search_body("q", 5, Some("docs.rs"), 4000)["q"], json!("q site:docs.rs"));
    Ok(())
}

fn server_cut_bounds() -> WeftResult<()> {
    assert_eq!(search_body("q", 1, None, 50_000)["source_max_chars"], json!(10_000));
    assert_eq!(search_body("q", 1, None, 0)["source_max_chars"], json!(1));
    Ok(())
}

async fn canned_search(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/web-search",
        json!({ "data": { "query": "weft workflow language site:weft.dev", "sources": [
            { "title": "Weft", "url": "https://weft.dev", "published": "2026-10-01",
              "snippet": "a workflow language", "read_at": "2026-11-26" },
        ], "took_ms": 812, "price_usd": 0.005 }}),
    );
    let outcome = rig
        .run(
            &SpecStreamSearchNode,
            json!({
                "account": rig.access("specstream"),
                "query": "weft workflow language",
                "numResults": 3,
                "site": "weft.dev",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(1.0));
    assert_eq!(
        outcome.outputs["results"][0],
        json!({ "title": "Weft", "url": "https://weft.dev", "publishedDate": "2026-10-01",
                "text": "a workflow language" }),
        "results are shaped to the declared fields only"
    );

    let sent = &rig.requests()[0];
    assert_eq!(sent.path, "/v1/web-search");
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({ "q": "weft workflow language site:weft.dev", "num": 3, "compact": true,
                 "source_max_chars": 4000 }),
        "the manifest's text cap rides every request, so the server cuts each passage"
    );
    Ok(())
}

/// Ten texts of 4000 characters three bytes each weigh about 120 KB:
/// within `maxTextChars`, past the wire, so the node shortens them.
async fn heavy_texts(rig: FakeRig) -> WeftResult<()> {
    let sources: Vec<_> = (0..10)
        .map(|i| json!({ "title": format!("r{i}"), "url": format!("https://ex.com/{i}"), "snippet": "日".repeat(4000) }))
        .collect();
    rig.respond("POST", "/v1/web-search", json!({ "data": { "sources": sources } }));
    let outcome = rig
        .run(&SpecStreamSearchNode, json!({ "account": rig.access("specstream"), "query": "q" }))
        .await
        .ok()?;
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
        "/v1/web-search",
        402,
        json!({ "error": { "code": "insufficient_balance", "message": "The account's balance is used up.", "details": {} } }),
    );
    let err = rig
        .run(&SpecStreamSearchNode, json!({ "account": rig.access("specstream"), "query": "q" }))
        .await
        .failure()?;
    assert!(err.contains("402") && err.contains("balance is used up"), "{err}");
    Ok(())
}

async fn live_search(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &SpecStreamSearchNode,
            json!({ "account": rig.access("specstream"), "query": "rust programming language", "numResults": 1 }),
        )
        .await
        .ok()?;
    assert!(outcome.output("count")?.as_f64().unwrap_or(0.0) >= 1.0, "a real search answers at least one result");
    let first = &outcome.output("results")?[0];
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
