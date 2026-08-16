//! NotionUpdateItem self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::NotionUpdateItemNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("patches_the_properties", updates),
        NodeTest::fake("nothing_to_update_refuses", empty),
    ]
}

async fn updates(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "PATCH",
        "/v1/pages/row-1",
        json!({ "id": "row-1", "url": "https://notion.so/row-1" }),
    );
    let outcome = rig
        .run(
            &NotionUpdateItemNode,
            json!({
                "account": rig.access("notion"),
                "pageId": "row-1",
                "properties": { "Done": { "checkbox": true } },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["pageId"], json!("row-1"));
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(body["properties"]["Done"]["checkbox"], json!(true));
    assert!(body.get("archived").is_none());
    Ok(())
}

async fn empty(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &NotionUpdateItemNode,
            json!({ "account": rig.access("notion"), "pageId": "row-1" }),
        )
        .await;
    let err = outcome.result.expect_err("an empty update must refuse").to_string();
    assert!(err.contains("nothing to update"), "{err}");
    Ok(())
}
