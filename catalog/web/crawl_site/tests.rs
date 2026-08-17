//! CrawlSite self-tests: submit, poll to completion, accumulate pages
//! across `next` links, and the loud failed-crawl path.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::CrawlSiteNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("crawls_and_accumulates_pages_across_links", crawl_accumulates),
        NodeTest::fake("a_failed_crawl_fails_loud", failed_crawl),
        NodeTest::fake("an_unknown_status_fails_loud", unknown_status),
        NodeTest::live("one_real_single_page_crawl", "firecrawl", live_crawl),
    ]
}

/// One real crawl capped at a single page (the cheapest crawl
/// firecrawl sells, one credit), against a small stable site. Nothing
/// lands on the account.
async fn live_crawl(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &CrawlSiteNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://example.com",
                "limit": 1,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("count")?.as_f64(), Some(1.0));
    let pages = outcome.output("pages")?;
    assert!(
        pages[0]["markdown"].as_str().unwrap_or_default().contains("Example Domain"),
        "the crawled page carries the real content: {}",
        pages[0]
    );
    Ok(())
}

async fn crawl_accumulates(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-1" }));
    // First status read: completed with one page and a `next` link.
    rig.respond(
        "GET",
        "/v2/crawl/job-1",
        json!({
            "status": "completed",
            "data": [
                { "markdown": "page one", "metadata": {
                    "sourceURL": "https://ex.com/1", "title": "One" } }
            ],
            "next": "https://api.firecrawl.dev/v2/crawl/job-1?skip=1",
        }),
    );
    // Second page, no further link.
    rig.respond(
        "GET",
        "/v2/crawl/job-1?skip=1",
        json!({
            "status": "completed",
            "data": [
                { "markdown": "page two", "metadata": {
                    "sourceURL": "https://ex.com/2", "title": "Two" } }
            ],
        }),
    );

    let outcome = rig
        .run(
            &CrawlSiteNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://ex.com",
                "limit": 10,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(2.0), "pages accumulate, never replace");
    assert_eq!(
        outcome.outputs["pages"][0]["url"],
        json!("https://ex.com/1"),
    );
    assert_eq!(outcome.outputs["pages"][1]["markdown"], json!("page two"));

    let submit = &rig.requests()[0];
    assert_eq!(
        submit.body.as_ref().expect("crawl payload")["limit"],
        json!(10),
        "the price-bounding page limit always goes out"
    );
    Ok(())
}

async fn failed_crawl(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-2" }));
    rig.respond(
        "GET",
        "/v2/crawl/job-2",
        json!({ "status": "failed", "error": "robots.txt forbids crawling" }),
    );
    let outcome = rig
        .run(
            &CrawlSiteNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://ex.com",
                "limit": 5,
            }),
        )
        .await;
    let err = outcome.result.expect_err("a failed crawl is loud").to_string();
    assert!(err.contains("robots.txt forbids crawling"), "{err}");
    Ok(())
}

async fn unknown_status(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-3" }));
    // A status outside Firecrawl's documented set must refuse loudly,
    // never poll forever on an answer the node does not understand.
    rig.respond("GET", "/v2/crawl/job-3", json!({ "status": "paused" }));
    let outcome = rig
        .run(
            &CrawlSiteNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://ex.com",
                "limit": 5,
            }),
        )
        .await;
    let err = outcome.result.expect_err("an unknown status is loud").to_string();
    assert!(err.contains("unexpected status 'paused'"), "{err}");
    Ok(())
}
