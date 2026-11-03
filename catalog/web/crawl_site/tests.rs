//! CrawlSite self-tests: submit, park until the crawl ends, accumulate
//! pages across `next` links into one stored JSON lines file, the body
//! run again after the wait starting no second crawl, and the loud
//! failed-crawl path.

use serde_json::{json, Value};

use weft::storage::StorageScope;
use weft::{FakeRig, LiveRig, NodeTest, StoredFile, WeftResult};

use super::CrawlSiteNode;

/// The pages a stored JSON lines file holds, one per line.
fn lines(bytes: &[u8]) -> Vec<Value> {
    std::str::from_utf8(bytes)
        .expect("utf-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("one JSON page a line"))
        .collect()
}

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("crawls_and_accumulates_pages_across_links", crawl_accumulates),
        NodeTest::fake("a_crawl_heavier_than_a_wire_is_stored_whole", heavy_crawl),
        NodeTest::fake("a_resumed_body_starts_no_second_crawl", resumed),
        NodeTest::fake("a_next_link_that_brings_no_pages_ends_the_reading", empty_next),
        NodeTest::fake("a_read_failing_mid_crawl_fails_and_keeps_no_half_file", failed_mid_read),
        NodeTest::fake("a_failed_crawl_fails_loud", failed_crawl),
        NodeTest::fake("an_unknown_status_fails_loud", unknown_status),
        NodeTest::fake("a_failed_crawl_comes_out_on_error_when_it_is_wired", failed_crawl_wired),
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
    let pages = lines(&rig.stored_bytes(outcome.output("pages")?).await?);
    assert!(
        pages[0]["markdown"].as_str().unwrap_or_default().contains("Example Domain"),
        "the crawled page carries the real content: {}",
        pages[0]
    );
    Ok(())
}

async fn crawl_accumulates(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-1" }));
    // The wait carries only the status; the pages are read after it,
    // starting with the finished answer: one page and a `next` link.
    rig.signal(json!({ "status": "completed" }));
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
    let file = StoredFile::from_value(outcome.output("pages")?)?;
    assert_eq!(file.mime_type, "application/jsonl");
    assert_eq!(
        lines(&rig.stored_bytes(&file.key)?),
        vec![
            json!({ "url": "https://ex.com/1", "title": "One", "markdown": "page one" }),
            json!({ "url": "https://ex.com/2", "title": "Two", "markdown": "page two" }),
        ]
    );

    let submit = &rig.requests()[0];
    assert_eq!(
        submit.body.as_ref().expect("crawl payload")["limit"],
        json!(10),
        "the price-bounding page limit always goes out"
    );

    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1, "one wait");
    assert_eq!(awaited[0].kind, "poll_endpoint");
    assert_eq!(awaited[0].config["url"], json!("https://api.firecrawl.dev/v2/crawl/job-1"));
    assert!(awaited[0].access.is_some(), "each poll is signed by the firecrawl connection");
    assert_eq!(
        awaited[0].match_predicates,
        vec![weft::signal::Predicate::neq("status", "scraping")]
    );
    assert_eq!(awaited[0].config["carry"], json!(["status", "error"]), "the wait carries the status, never the pages");
    assert_eq!(
        awaited[0].on_cancel,
        Some(weft::signal::CancelCall::delete("https://api.firecrawl.dev/v2/crawl/job-1")),
        "cancelling the run cancels the crawl"
    );
    Ok(())
}

/// The first run pauses in its wait (the crawl still going): the state
/// holds the job. The run again comes back to the wait on that job and
/// never pays for a second crawl.
async fn resumed(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-8" }));
    rig.respond(
        "GET",
        "/v2/crawl/job-8",
        json!({ "status": "completed", "data": [{ "markdown": "one", "metadata": {} }] }),
    );
    let inputs = json!({ "account": rig.access("firecrawl"), "url": "https://ex.com", "limit": 5 });

    rig.signal_paused();
    let paused = rig.run(&CrawlSiteNode, inputs.clone()).await;
    assert!(paused.paused(), "the first run pauses in its wait: {:?}", paused.result);
    assert_eq!(rig.saved_state(&CrawlSiteNode), Some(json!({ "job": "job-8", "ended": null })));

    rig.signal(json!({ "status": "completed" }));
    let outcome = rig.run(&CrawlSiteNode, inputs).await.ok()?;
    assert_eq!(outcome.outputs["count"], json!(1.0));
    let starts = rig.requests().iter().filter(|r| r.method == "POST").count();
    assert_eq!(starts, 1, "one crawl across the two runs");
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 2, "the paused wait, then the same wait answered");
    assert_eq!(awaited[0].config, awaited[1].config, "both on the one job");
    assert_eq!(awaited[1].on_cancel, awaited[0].on_cancel, "the resumed wait still cancels the crawl");
    Ok(())
}

async fn failed_crawl(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-2" }));
    rig.signal(json!({ "status": "failed", "error": "robots.txt forbids crawling" }));
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

async fn failed_crawl_wired(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-4" }));
    rig.signal(json!({ "status": "failed", "error": "robots.txt forbids crawling" }));
    rig.wire_output("_error");
    let outcome = rig
        .run(
            &CrawlSiteNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://ex.com",
                "limit": 5,
            }),
        )
        .await
        .ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("robots.txt forbids crawling"), "{error}");
    for port in ["pages", "count"] {
        assert!(!outcome.outputs.contains_key(port), "a caught failure emits nothing on {port}");
    }
    Ok(())
}

async fn unknown_status(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-3" }));
    // A status outside Firecrawl's documented set must refuse loudly,
    // never wait forever on an answer the node does not understand.
    rig.signal(json!({ "status": "paused" }));
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

/// Three pages of 40 KB weigh more than a wire: the file holds them all,
/// uncut.
async fn heavy_crawl(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-5" }));
    rig.signal(json!({ "status": "completed" }));
    let data: Vec<_> = (0..3).map(|i| json!({ "markdown": "m".repeat(40 * 1024), "metadata": { "sourceURL": format!("https://ex.com/{i}") } })).collect();
    rig.respond("GET", "/v2/crawl/job-5", json!({ "status": "completed", "data": data }));
    let outcome = rig
        .run(&CrawlSiteNode, json!({ "account": rig.access("firecrawl"), "url": "https://ex.com" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(3.0));
    let file = StoredFile::from_value(outcome.output("pages")?)?;
    let pages = lines(&rig.stored_bytes(&file.key)?);
    assert_eq!(pages.len(), 3);
    assert!(pages.iter().all(|p| p["markdown"].as_str().map(str::len) == Some(40 * 1024)), "every page whole");
    Ok(())
}

/// Firecrawl's rule for reading a finished crawl: stop when there is no
/// `next`, or when a page of the answer brings no new pages.
async fn empty_next(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-6" }));
    rig.signal(json!({ "status": "completed" }));
    rig.respond(
        "GET",
        "/v2/crawl/job-6",
        json!({ "status": "completed", "data": [{ "markdown": "one", "metadata": {} }], "next": "https://api.firecrawl.dev/v2/crawl/job-6?page=2" }),
    );
    rig.respond(
        "GET",
        "/v2/crawl/job-6?page=2",
        json!({ "status": "completed", "data": [], "next": "https://api.firecrawl.dev/v2/crawl/job-6?page=3" }),
    );
    let outcome = rig
        .run(&CrawlSiteNode, json!({ "account": rig.access("firecrawl"), "url": "https://ex.com" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(1.0));
    let reads = rig.requests().iter().filter(|r| r.method == "GET").count();
    assert_eq!(reads, 2, "the empty answer ends it, its `next` is never followed");
    Ok(())
}

/// The pages stream into storage as they are read: a page that breaks
/// on the second answer fails the node with its own error, and the
/// first answer's pages, already streamed, leave no file behind.
async fn failed_mid_read(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/crawl", json!({ "success": true, "id": "job-7" }));
    rig.signal(json!({ "status": "completed" }));
    rig.respond(
        "GET",
        "/v2/crawl/job-7",
        json!({ "status": "completed", "data": [{ "markdown": "one", "metadata": {} }], "next": "https://api.firecrawl.dev/v2/crawl/job-7?page=2" }),
    );
    rig.respond("GET", "/v2/crawl/job-7?page=2", json!({ "status": "completed", "data": [{ "metadata": {} }] }));
    let err = rig
        .run(&CrawlSiteNode, json!({ "account": rig.access("firecrawl"), "url": "https://ex.com" }))
        .await
        .failure()?;
    assert!(err.contains("carries no markdown"), "the read's own error, not the upload's: {err}");
    assert!(rig.stored_files(&StorageScope::Run)?.is_empty(), "no half file is kept");
    Ok(())
}
