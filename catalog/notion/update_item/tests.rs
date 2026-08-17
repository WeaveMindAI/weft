//! NotionUpdateItem self-tests.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{archive_page, unique_title, DATABASE_FIXTURE, DATABASE_LABEL};

use super::NotionUpdateItemNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("patches_the_properties", updates),
        NodeTest::fake("nothing_to_update_refuses", empty),
        NodeTest::live("one_real_update_and_archive", "notion", live_update).with_fixture(
            fixture_spec(DATABASE_FIXTURE, DATABASE_LABEL.0, DATABASE_LABEL.1),
        ),
    ]
}

/// One real update of a freshly minted row, then the row is archived
/// so the database stays clean. Notion bills nothing for this.
async fn live_update(rig: LiveRig) -> WeftResult<()> {
    let database = rig.fixture(DATABASE_FIXTURE)?;
    let minted = rig
        .run(
            &crate::notion_create_item::NotionCreateItemNode,
            json!({
                "account": rig.access("notion"),
                "database": database,
                "properties": {
                    "Name": { "title": [{ "text": { "content": unique_title("weft update row") } }] }
                },
            }),
        )
        .await
        .ok()?;
    let page_id = minted.output("pageId")?.as_str().unwrap_or_default().to_string();
    let outcome = rig
        .run(
            &NotionUpdateItemNode,
            json!({
                "account": rig.access("notion"),
                "pageId": page_id,
                "properties": {
                    "Name": { "title": [{ "text": { "content": unique_title("weft updated row") } }] }
                },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("pageId")?.as_str(), Some(page_id.as_str()));
    let conn = rig.connect().await?;
    archive_page(&conn, &page_id).await
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
