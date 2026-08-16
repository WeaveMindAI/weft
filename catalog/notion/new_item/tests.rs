//! NotionNewItem self-tests: the registered poll recipe and the wake
//! shaping.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::NotionNewItemNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("registers_the_post_delta_poll", registers),
        NodeTest::fake("a_fire_emits_the_page", fires),
    ]
}

async fn registers(rig: FakeRig) -> WeftResult<()> {
    let db = "f".repeat(32);
    rig.respond(
        "GET",
        &format!("/v1/databases/{db}"),
        json!({ "data_sources": [{ "id": "src-t", "name": "Rows" }] }),
    );
    rig.run_setup_trigger(
        &NotionNewItemNode,
        json!({
            "account": rig.access("notion"),
            "database": db,
            "intervalSecs": 30,
        }),
    )
    .await
    .ok()?;

    let signals = rig.registered_signals();
    assert_eq!(signals.len(), 1);
    let spec = serde_json::to_value(&signals[0].0).expect("spec serializes").to_string();
    assert!(spec.contains("\"method\":\"post\""), "{spec}");
    assert!(spec.contains("data_sources/src-t/query"), "{spec}");
    assert!(spec.contains("descending"), "the poll asks newest first: {spec}");
    assert!(spec.contains("\"mode\":\"set\""), "{spec}");
    assert!(spec.contains("\"cursor_field\":\"id\""), "{spec}");
    Ok(())
}

async fn fires(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({
        "item": {
            "id": "row-7",
            "url": "https://notion.so/row-7",
            "properties": { "Name": { "title": [] } },
        }
    }));
    let outcome = rig
        .run(
            &NotionNewItemNode,
            json!({
                "account": rig.access("notion"),
                "database": "f".repeat(32),
                "intervalSecs": 30,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["pageId"], json!("row-7"));
    assert_eq!(outcome.outputs["url"], json!("https://notion.so/row-7"));
    assert!(outcome.outputs["properties"].is_object());
    Ok(())
}
