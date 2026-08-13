//! DownloadLink self-tests: the sink validates the handle, nothing
//! more.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::DownloadLinkNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_stored_file_is_accepted_silently", accepts),
        NodeTest::fake("a_value_without_a_handle_refuses", refuses),
    ]
}

async fn accepts(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("report.pdf", "application/pdf", b"%PDF".to_vec());
    let outcome = rig.run(&DownloadLinkNode, json!({ "file": file })).await.ok()?;
    assert!(outcome.outputs.is_empty(), "a terminal sink emits nothing");
    Ok(())
}

async fn refuses(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&DownloadLinkNode, json!({ "file": { "filename": "x" } }))
        .await;
    assert!(outcome.result.is_err(), "no key, no url: nothing the button could resolve");
    Ok(())
}
