//! NotionQueryDatabase self-tests: the source resolution + the paged
//! query against canned answers.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{archive_page, unique_title, DATABASE_FIXTURE, DATABASE_LABEL};

use super::NotionQueryDatabaseNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("resolves_the_source_and_pages_the_query", queries),
        NodeTest::fake("several_sources_without_a_pick_refuses", ambiguous),
        NodeTest::live("one_real_filtered_query", "notion", live_query).with_fixture(
            fixture_spec(DATABASE_FIXTURE, DATABASE_LABEL.0, DATABASE_LABEL.1),
        ),
    ]
}

/// One real filtered query: a row minted under a run-unique title is
/// the only match for its own filter, then it is archived so the
/// database stays clean. Notion bills nothing for this.
async fn live_query(rig: LiveRig) -> WeftResult<()> {
    let database = rig.fixture(DATABASE_FIXTURE)?;
    let title = unique_title("weft query row");
    let minted = rig
        .run(
            &crate::notion_create_item::NotionCreateItemNode,
            json!({
                "account": rig.access("notion"),
                "database": database,
                "properties": { "Name": { "title": [{ "text": { "content": title } }] } },
            }),
        )
        .await
        .ok()?;
    let page_id = minted.output("pageId")?.as_str().unwrap_or_default().to_string();
    let outcome = rig
        .run(
            &NotionQueryDatabaseNode,
            json!({
                "account": rig.access("notion"),
                "database": database,
                "filter": { "property": "Name", "title": { "equals": title } },
                "limit": 10,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("count")?.as_f64(), Some(1.0), "exactly the minted row matches");
    assert_eq!(outcome.output("items")?[0]["id"].as_str(), Some(page_id.as_str()));
    let conn = rig.connect().await?;
    archive_page(&conn, &page_id).await
}

async fn queries(rig: FakeRig) -> WeftResult<()> {
    let db = "c".repeat(32);
    rig.respond(
        "GET",
        &format!("/v1/databases/{db}"),
        json!({ "data_sources": [{ "id": "src-1", "name": "Rows" }] }),
    );
    rig.respond(
        "POST",
        "/v1/data_sources/src-1/query",
        json!({
            "results": [{ "id": "p1" }, { "id": "p2" }],
            "has_more": false,
            "next_cursor": null,
        }),
    );
    let outcome = rig
        .run(
            &NotionQueryDatabaseNode,
            json!({
                "account": rig.access("notion"),
                "database": db,
                "filter": { "property": "Done", "checkbox": { "equals": false } },
                "limit": 50,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(2.0));

    let query = rig.requests()[1].body.clone().expect("query body");
    assert_eq!(query["page_size"], json!(50));
    assert_eq!(query["filter"]["property"], json!("Done"));
    Ok(())
}

async fn ambiguous(rig: FakeRig) -> WeftResult<()> {
    let db = "d".repeat(32);
    rig.respond(
        "GET",
        &format!("/v1/databases/{db}"),
        json!({ "data_sources": [
            { "id": "s1", "name": "Rows" },
            { "id": "s2", "name": "Archive" },
        ]}),
    );
    let outcome = rig
        .run(
            &NotionQueryDatabaseNode,
            json!({ "account": rig.access("notion"), "database": db, "limit": 10 }),
        )
        .await;
    let err = outcome.result.expect_err("several sources must refuse").to_string();
    assert!(err.contains("Rows, Archive"), "{err}");
    Ok(())
}
