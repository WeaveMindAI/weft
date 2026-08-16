//! NotionQueryDatabase self-tests: the source resolution + the paged
//! query against canned answers.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::NotionQueryDatabaseNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("resolves_the_source_and_pages_the_query", queries),
        NodeTest::fake("several_sources_without_a_pick_refuses", ambiguous),
    ]
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
