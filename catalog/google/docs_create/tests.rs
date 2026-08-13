//! GoogleDocsCreate self-tests: create, then the optional first
//! append.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleDocsCreateNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_and_writes_the_initial_text", creates_with_text),
        NodeTest::fake("no_text_means_no_append_call", creates_empty),
        NodeTest::live("one_real_doc_then_deleted", "google", live_create),
    ]
}

/// Create one document via the node, then delete it through the
/// test's own connection (a doc is a Drive file) so repeated runs
/// never pile documents onto the account.
async fn live_create(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &GoogleDocsCreateNode,
            json!({
                "account": rig.access("google"),
                "title": "weft-node-tests",
                "text": "Created by the GoogleDocsCreate live self-test.",
            }),
        )
        .await
        .ok()?;
    // The id IS the delete key, so an empty one makes drive_delete fail
    // loudly on its own; a guard between create and delete would only
    // risk leaking the doc it names.
    let id = outcome.output("documentId")?.as_str().expect("document id").to_string();
    let conn = rig.connect().await?;
    crate::testing::drive_delete(&conn, &id).await
}

async fn creates_with_text(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/documents", json!({ "documentId": "doc9" }));
    rig.respond("POST", "/v1/documents/doc9:batchUpdate", json!({}));
    let outcome = rig
        .run(
            &GoogleDocsCreateNode,
            json!({ "account": rig.access("google"), "title": "Report", "text": "body" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["documentId"], json!("doc9"));
    assert_eq!(
        outcome.outputs["link"],
        json!("https://docs.google.com/document/d/doc9/edit")
    );
    assert_eq!(rig.requests().len(), 2, "create + append");
    Ok(())
}

async fn creates_empty(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/documents", json!({ "documentId": "doc9" }));
    rig.run(
        &GoogleDocsCreateNode,
        json!({ "account": rig.access("google"), "title": "Report" }),
    )
    .await
    .ok()?;
    assert_eq!(rig.requests().len(), 1, "no text, no batchUpdate");
    Ok(())
}
