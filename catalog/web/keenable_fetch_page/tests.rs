//! KeenableFetchPage self-tests. `fake` pins the request the node
//! builds, the route it picks with and without an account, and how it
//! reads and cuts Keenable's answer; `live` fetches one small real page
//! on each route (keyless and keyed).

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::KeenableFetchPageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("without_an_account_it_fetches_the_public_route", keyless_fetch),
        NodeTest::fake("with_an_account_it_fetches_the_keyed_route", keyed_fetch),
        NodeTest::fake("markdown_past_max_chars_is_cut_and_marked", cut_markdown),
        NodeTest::fake("an_answer_without_content_fails_loudly", no_content),
        NodeTest::live("one_real_keyless_fetch", "keenable", live_keyless_fetch),
        NodeTest::live("one_real_keyed_fetch", "keenable", live_keyed_fetch),
    ]
}

async fn keyless_fetch(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v1/fetch/public?url=https%3A%2F%2Fweft.dev%2Fdocs%3Fpage%3D2",
        json!({ "url": "https://weft.dev/docs?page=2", "title": "Docs",
                "content": "# Docs\n\nhello", "description": "" }),
    );
    let outcome = rig
        .run(&KeenableFetchPageNode, json!({ "url": "https://weft.dev/docs?page=2" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["markdown"], json!("# Docs\n\nhello"));
    assert_eq!(outcome.outputs["title"], json!("Docs"));
    assert_eq!(outcome.outputs["truncated"], json!(false));

    let sent = &rig.requests()[0];
    assert_eq!(sent.path, "/v1/fetch/public");
    assert_eq!(sent.header("x-keenable-title"), Some("weft"), "the public route needs the app named");
    Ok(())
}

async fn keyed_fetch(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v1/fetch?url=https%3A%2F%2Fweft.dev",
        json!({ "url": "https://weft.dev", "content": "hello" }),
    );
    let outcome = rig
        .run(
            &KeenableFetchPageNode,
            json!({ "account": rig.access("keenable"), "url": "https://weft.dev" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["markdown"], json!("hello"));
    assert_eq!(outcome.outputs["title"], json!(""), "a page without a title gives an empty one");
    rig.assert_sent("GET", "/v1/fetch");
    Ok(())
}

async fn cut_markdown(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v1/fetch/public?url=https%3A%2F%2Fweft.dev",
        json!({ "url": "https://weft.dev", "title": "Long", "content": "x".repeat(100) }),
    );
    let outcome = rig
        .run(&KeenableFetchPageNode, json!({ "url": "https://weft.dev", "maxChars": 10 }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["markdown"], json!("x".repeat(10)));
    assert_eq!(outcome.outputs["truncated"], json!(true));
    Ok(())
}

async fn no_content(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v1/fetch/public?url=https%3A%2F%2Fweft.dev",
        json!({ "url": "https://weft.dev", "title": "Odd" }),
    );
    let err = rig
        .run(&KeenableFetchPageNode, json!({ "url": "https://weft.dev" }))
        .await
        .failure()?;
    assert!(err.contains("carries no content"), "{err}");
    Ok(())
}

async fn live_keyless_fetch(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(&KeenableFetchPageNode, json!({ "url": "https://example.com" }))
        .await
        .ok()?;
    let markdown = outcome.output("markdown")?.as_str().unwrap_or_default();
    assert!(!markdown.is_empty(), "a real page has markdown");
    Ok(())
}

async fn live_keyed_fetch(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &KeenableFetchPageNode,
            json!({ "account": rig.access("keenable"), "url": "https://example.com" }),
        )
        .await
        .ok()?;
    let markdown = outcome.output("markdown")?.as_str().unwrap_or_default();
    assert!(!markdown.is_empty(), "a real page has markdown");
    Ok(())
}
