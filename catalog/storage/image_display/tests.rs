//! ImageDisplay self-tests: the preview sink validates the handle.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::ImageDisplayNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_stored_image_is_accepted_silently", accepts),
        NodeTest::fake("a_value_without_a_handle_refuses", refuses),
    ]
}

async fn accepts(rig: FakeRig) -> WeftResult<()> {
    let image = rig.store_file("cat.png", "image/png", b"PNG".to_vec());
    let outcome = rig.run(&ImageDisplayNode, json!({ "image": image })).await.ok()?;
    assert!(outcome.outputs.is_empty(), "a terminal sink emits nothing");
    Ok(())
}

async fn refuses(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&ImageDisplayNode, json!({ "image": { "filename": "x" } }))
        .await;
    assert!(outcome.result.is_err(), "no key, no url: nothing the preview could resolve");
    Ok(())
}
