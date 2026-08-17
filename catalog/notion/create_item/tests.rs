//! NotionCreateItem self-tests.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{archive_page, unique_title, DATABASE_FIXTURE, DATABASE_LABEL};

use super::NotionCreateItemNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_the_row_under_the_source", creates),
        NodeTest::live("one_real_row_created_and_archived", "notion", live_create).with_fixture(
            fixture_spec(DATABASE_FIXTURE, DATABASE_LABEL.0, DATABASE_LABEL.1),
        ),
    ]
}

/// One real row created in the fixture database, then archived so the
/// database stays clean. Notion bills nothing for this.
async fn live_create(rig: LiveRig) -> WeftResult<()> {
    let database = rig.fixture(DATABASE_FIXTURE)?;
    let outcome = rig
        .run(
            &NotionCreateItemNode,
            json!({
                "account": rig.access("notion"),
                "database": database,
                "properties": {
                    "Name": { "title": [{ "text": { "content": unique_title("weft live row") } }] }
                },
            }),
        )
        .await
        .ok()?;
    let page_id = outcome.output("pageId")?.as_str().unwrap_or_default().to_string();
    assert!(!page_id.is_empty(), "a real row id came back");
    let conn = rig.connect().await?;
    archive_page(&conn, &page_id).await
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
