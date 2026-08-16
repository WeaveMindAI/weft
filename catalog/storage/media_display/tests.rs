//! MediaDisplay self-tests: the preview sink validates the handle.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::MediaDisplayNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("stored_media_is_accepted_silently", accepts),
        NodeTest::fake("a_value_without_a_handle_refuses", refuses),
    ]
}

async fn accepts(rig: FakeRig) -> WeftResult<()> {
    for (name, mime) in
        [("cat.png", "image/png"), ("clip.mp3", "audio/mpeg"), ("clip.mp4", "video/mp4")]
    {
        let media = rig.store_file(name, mime, b"bytes".to_vec());
        let outcome = rig.run(&MediaDisplayNode, json!({ "media": media })).await.ok()?;
        assert!(outcome.outputs.is_empty(), "a terminal sink emits nothing");
    }
    Ok(())
}

async fn refuses(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&MediaDisplayNode, json!({ "media": { "filename": "x" } }))
        .await;
    assert!(outcome.result.is_err(), "no key, no url: nothing the preview could resolve");
    Ok(())
}
