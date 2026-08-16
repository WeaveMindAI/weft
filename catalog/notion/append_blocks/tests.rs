//! NotionAppendBlocks self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::NotionAppendBlocksNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("appends_paragraphs", appends)]
}

async fn appends(rig: FakeRig) -> WeftResult<()> {
    let page = "b".repeat(32);
    rig.respond(
        "PATCH",
        &format!("/v1/blocks/{page}/children"),
        json!({ "results": [] }),
    );
    let outcome = rig
        .run(
            &NotionAppendBlocksNode,
            json!({
                "account": rig.access("notion"),
                "page": page,
                "content": "New line",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["appended"], json!(1.0));
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(
        body["children"][0]["paragraph"]["rich_text"][0]["text"]["content"],
        json!("New line")
    );
    Ok(())
}
