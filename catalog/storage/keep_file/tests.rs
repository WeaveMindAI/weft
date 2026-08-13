//! KeepFile self-tests: the keep flag lands and the exact marker
//! passes through.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::KeepFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("keeps_and_passes_the_marker_through", keeps)]
}

async fn keeps(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("keeper.txt", "text/plain", b"data".to_vec());
    let outcome = rig
        .run(&KeepFileNode, json!({ "file": file.clone() }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["file"], file, "the marker passes through untouched");
    Ok(())
}
