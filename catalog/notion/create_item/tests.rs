//! NotionCreateItem self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::NotionCreateItemNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("creates_the_row_under_the_source", creates)]
}

async fn creates(rig: FakeRig) -> WeftResult<()> {
    let db = "e".repeat(32);
    rig.respond(
        "GET",
        &format!("/v1/databases/{db}"),
        json!({ "data_sources": [{ "id": "src-9", "name": "Rows" }] }),
    );
    rig.respond(
        "POST",
        "/v1/pages",
        json!({ "id": "row-1", "url": "https://notion.so/row-1" }),
    );
    let outcome = rig
        .run(
            &NotionCreateItemNode,
            json!({
                "account": rig.access("notion"),
                "database": db,
                "properties": { "Name": { "title": [{ "text": { "content": "Task" } }] } },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["pageId"], json!("row-1"));
    let body = rig.requests()[1].body.clone().expect("json payload");
    assert_eq!(body["parent"]["data_source_id"], json!("src-9"));
    assert_eq!(body["properties"]["Name"]["title"][0]["text"]["content"], json!("Task"));
    Ok(())
}
