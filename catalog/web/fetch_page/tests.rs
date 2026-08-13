//! FetchPage self-tests: the scrape request's exact payload and the
//! markdown/title extraction, plus the loud refusal path.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::FetchPageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("scrapes_a_page_into_markdown", scrapes_page),
        NodeTest::fake("a_refused_scrape_fails_loud", refused_scrape),
        NodeTest::fake("a_success_without_markdown_fails_loud", missing_markdown),
        NodeTest::live("one_real_scrape", "firecrawl", live_scrape),
    ]
}

async fn live_scrape(rig: weft::LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({ "account": rig.access("firecrawl"), "url": "https://example.com" }),
        )
        .await
        .ok()?;
    let markdown = outcome.output("markdown")?.as_str().expect("markdown text");
    assert!(
        markdown.to_lowercase().contains("example"),
        "example.com scraped into markdown, got: {markdown}"
    );
    Ok(())
}

async fn scrapes_page(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v2/scrape",
        json!({
            "success": true,
            "data": {
                "markdown": "# Hello\n\nBody.",
                "metadata": { "title": "Hello Page" }
            }
        }),
    );
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://example.com/post",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["markdown"], json!("# Hello\n\nBody."));
    assert_eq!(outcome.outputs["title"], json!("Hello Page"));

    let sent = rig.requests();
    assert_eq!(
        sent[0].body.as_ref().expect("scrape payload"),
        &json!({
            "url": "https://example.com/post",
            "formats": ["markdown"],
            "onlyMainContent": true,
        }),
        "exactly the scrape shape the meter prices (no credit-multiplying options)"
    );
    Ok(())
}

async fn missing_markdown(rig: FakeRig) -> WeftResult<()> {
    // The markdown is the node's whole product: a "success" answer
    // without it must refuse loudly, never emit an empty page.
    rig.respond(
        "POST",
        "/v2/scrape",
        json!({ "success": true, "data": { "metadata": { "title": "No Body" } } }),
    );
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://example.com",
            }),
        )
        .await;
    let err = outcome.result.expect_err("missing markdown is loud").to_string();
    assert!(err.contains("no markdown"), "{err}");
    Ok(())
}

async fn refused_scrape(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status(
        "POST",
        "/v2/scrape",
        402,
        json!({ "success": false, "error": "insufficient credits" }),
    );
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://example.com",
            }),
        )
        .await;
    let err = outcome.result.expect_err("a refusal is loud").to_string();
    assert!(err.contains("insufficient credits"), "{err}");
    Ok(())
}
