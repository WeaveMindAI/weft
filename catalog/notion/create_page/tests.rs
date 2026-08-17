//! NotionCreatePage self-tests.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{
    archive_page, unique_title, PARENT_PAGE_FIXTURE, PARENT_PAGE_LABEL,
};

use super::NotionCreatePageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_the_page_from_plain_text", creates),
        NodeTest::fake("nothing_to_write_refuses", empty),
        NodeTest::live("one_real_page_created_and_archived", "notion", live_create).with_fixture(
            fixture_spec(PARENT_PAGE_FIXTURE, PARENT_PAGE_LABEL.0, PARENT_PAGE_LABEL.1),
        ),
    ]
}

/// One real page created under the fixture parent, then archived so
/// the workspace stays clean. Notion bills nothing for this.
async fn live_create(rig: LiveRig) -> WeftResult<()> {
    let parent = rig.fixture(PARENT_PAGE_FIXTURE)?;
    let outcome = rig
        .run(
            &NotionCreatePageNode,
            json!({
                "account": rig.access("notion"),
                "parentPage": parent,
                "title": unique_title("weft live test page"),
                "content": "Minted by the create_page live test; safe to archive.",
            }),
        )
        .await
        .ok()?;
    let page_id = outcome.output("pageId")?.as_str().unwrap_or_default().to_string();
    assert!(!page_id.is_empty(), "a real page id came back");
    let conn = rig.connect().await?;
    archive_page(&conn, &page_id).await
}

async fn creates(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/pages",
        json!({ "id": "page-1", "url": "https://notion.so/page-1" }),
    );
    let outcome = rig
        .run(
            &NotionCreatePageNode,
            json!({
                "account": rig.access("notion"),
                "parentPage": "a".repeat(32),
                "title": "Weekly notes",
                "content": "First point\nSecond point\n",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["pageId"], json!("page-1"));
    assert_eq!(outcome.outputs["url"], json!("https://notion.so/page-1"));

    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(body["parent"]["page_id"], json!("a".repeat(32)));
    assert_eq!(
        body["properties"]["title"]["title"][0]["text"]["content"],
        json!("Weekly notes")
    );
    let children = body["children"].as_array().expect("children");
    assert_eq!(children.len(), 2, "one paragraph per line");
    assert_eq!(
        children[1]["paragraph"]["rich_text"][0]["text"]["content"],
        json!("Second point")
    );
    Ok(())
}

async fn empty(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &NotionCreatePageNode,
            json!({
                "account": rig.access("notion"),
                "parentPage": "a".repeat(32),
                "title": "Empty",
            }),
        )
        .await;
    let err = outcome.result.expect_err("an empty body must refuse").to_string();
    assert!(err.contains("nothing to write"), "{err}");
    Ok(())
}
