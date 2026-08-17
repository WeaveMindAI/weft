//! NotionAppendBlocks self-tests.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{
    archive_page, unique_title, PARENT_PAGE_FIXTURE, PARENT_PAGE_LABEL,
};

use super::NotionAppendBlocksNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("appends_paragraphs", appends),
        NodeTest::live("one_real_append_on_a_minted_page", "notion", live_append).with_fixture(
            fixture_spec(PARENT_PAGE_FIXTURE, PARENT_PAGE_LABEL.0, PARENT_PAGE_LABEL.1),
        ),
    ]
}

/// One real append onto a page this test mints under the fixture
/// parent, then archives. Notion bills nothing for this.
async fn live_append(rig: LiveRig) -> WeftResult<()> {
    let parent = rig.fixture(PARENT_PAGE_FIXTURE)?;
    let minted = rig
        .run(
            &crate::notion_create_page::NotionCreatePageNode,
            json!({
                "account": rig.access("notion"),
                "parentPage": parent,
                "title": unique_title("weft live test append target"),
                "content": "Minted by the append_blocks live test; safe to archive.",
            }),
        )
        .await
        .ok()?;
    let page_id = minted.output("pageId")?.as_str().unwrap_or_default().to_string();
    let outcome = rig
        .run(
            &NotionAppendBlocksNode,
            json!({
                "account": rig.access("notion"),
                "page": page_id,
                "content": "An appended paragraph.",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("appended")?.as_f64(), Some(1.0));
    let conn = rig.connect().await?;
    archive_page(&conn, &page_id).await
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
