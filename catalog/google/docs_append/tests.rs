//! GoogleDocsAppend self-tests: the end-of-body insert.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleDocsAppendNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("appends_at_the_end_of_the_body", appends),
        NodeTest::live("one_real_append", "google", live_append).with_fixture(fixture_spec(
            "GOOGLE_DOC_ID",
            "Document id",
            "A Google Doc the test appends a line to: the id from its URL.",
        )),
    ]
}

async fn appends(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/documents/doc1:batchUpdate", json!({}));
    let outcome = rig
        .run(
            &GoogleDocsAppendNode,
            json!({ "account": rig.access("google"), "documentId": "doc1", "text": "hi" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.requests()[0].body.clone().expect("batch body");
    assert_eq!(body["requests"][0]["insertText"]["text"], json!("hi"));
    Ok(())
}

async fn live_append(rig: LiveRig) -> WeftResult<()> {
    // Appends one line to the dedicated test document (a doc the
    // connected account can edit, set aside for this test). The line
    // stays: the appended text IS the proof, and the docs API offers
    // no way to take just it back out.
    let doc = rig.fixture("GOOGLE_DOC_ID")?;
    let outcome = rig
        .run(
            &GoogleDocsAppendNode,
            json!({
                "account": rig.access("google"),
                "documentId": doc,
                "text": "weft-node-tests: appended by the GoogleDocsAppend live self-test\n",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("done")?, &json!(true));
    Ok(())
}
