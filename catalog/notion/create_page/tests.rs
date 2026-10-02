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
        NodeTest::fake("a_title_only_page_has_no_body", title_only),
        NodeTest::fake("a_refused_page_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_page_comes_out_on_error_when_it_is_wired", refused_wired),
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

async fn title_only(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/pages",
        json!({ "id": "page-2", "url": "https://notion.so/page-2" }),
    );
    let outcome = rig
        .run(
            &NotionCreatePageNode,
            json!({
                "account": rig.access("notion"),
                "parentPage": "a".repeat(32),
                "title": "Empty",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["pageId"], json!("page-2"));
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(body["properties"]["title"]["title"][0]["text"]["content"], json!("Empty"));
    assert!(body.get("children").is_none(), "a title-only page sends no body: {body}");
    Ok(())
}

/// Notion refuses the new page: the parent is not shared with the
/// connection.
fn refuse_the_page(rig: &FakeRig) {
    rig.respond_status(
        "POST",
        "/v1/pages",
        404,
        json!({ "object": "error", "message": "Could not find page with ID" }),
    );
}

fn page_inputs(rig: &FakeRig) -> serde_json::Value {
    json!({
        "account": rig.access("notion"),
        "parentPage": "a".repeat(32),
        "title": "Weekly notes",
        "content": "First point",
    })
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_page(&rig);
    let err = rig.run(&NotionCreatePageNode, page_inputs(&rig)).await.failure()?;
    assert!(err.contains("Could not find page"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_page(&rig);
    rig.wire_output("error");
    let outcome = rig.run(&NotionCreatePageNode, page_inputs(&rig)).await.ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("Could not find page"), "{error}");
    for port in ["pageId", "url"] {
        assert!(!outcome.outputs.contains_key(port), "a caught failure emits nothing on {port}");
    }
    Ok(())
}
