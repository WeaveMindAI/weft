//! RssFeed self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::RssFeedNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("registers_the_feed_delta_poll", registers),
        NodeTest::fake("a_fire_emits_the_entry", fires),
    ]
}

async fn registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &RssFeedNode,
        json!({ "feedUrl": "https://blog.example/feed.xml", "intervalSecs": 300 }),
    )
    .await
    .ok()?;
    let signals = rig.registered_signals();
    assert_eq!(signals.len(), 1);
    let spec = serde_json::to_value(&signals[0].0).expect("spec serializes").to_string();
    assert!(spec.contains("\"format\":\"feed\""), "{spec}");
    assert!(spec.contains("blog.example/feed.xml"), "{spec}");
    assert!(spec.contains("\"mode\":\"set\""), "{spec}");
    Ok(())
}

async fn fires(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({
        "item": {
            "id": "tag:1",
            "title": "Post one",
            "link": "https://blog.example/1",
            "summary": "first",
            "published": "2026-08-13T00:00:00+00:00",
        }
    }));
    let outcome = rig
        .run(
            &RssFeedNode,
            json!({ "feedUrl": "https://blog.example/feed.xml", "intervalSecs": 300 }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["title"], json!("Post one"));
    assert_eq!(outcome.outputs["link"], json!("https://blog.example/1"));
    assert_eq!(outcome.outputs["entry"]["id"], json!("tag:1"));
    Ok(())
}
