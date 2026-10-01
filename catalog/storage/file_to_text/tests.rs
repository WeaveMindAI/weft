//! FileToText self-tests: a text file comes out as its text; a picture,
//! and bytes that are not text, are refused naming the file.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::FileToTextNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("reads_a_text_file", reads),
        NodeTest::fake("refuses_a_picture", refuses_media),
        NodeTest::fake("refuses_bytes_that_are_not_text", refuses_binary),
    ]
}

async fn reads(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("notes.md", "text/markdown", "# héllo\n".as_bytes().to_vec());
    let outcome = rig.run(&FileToTextNode, json!({ "file": file })).await.ok()?;
    assert_eq!(outcome.output("text")?, &json!("# héllo\n"));
    Ok(())
}

async fn refuses_media(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("cat.png", "image/png", b"PNG".to_vec());
    let err = rig.run(&FileToTextNode, json!({ "file": file })).await.failure()?;
    assert!(err.contains("'cat.png' is an image (image/png), not text"), "{err}");
    Ok(())
}

async fn refuses_binary(rig: FakeRig) -> WeftResult<()> {
    let invalid = rig.store_file("blob.bin", "application/octet-stream", vec![b'a', 0xff, 0xfe]);
    let err = rig.run(&FileToTextNode, json!({ "file": invalid })).await.failure()?;
    assert!(err.contains("'blob.bin'") && err.contains("not UTF-8"), "{err}");
    let nul = rig.store_file("data.bin", "application/octet-stream", vec![b'a', 0, b'b']);
    let err = rig.run(&FileToTextNode, json!({ "file": nul })).await.failure()?;
    assert!(err.contains("NUL byte"), "{err}");
    Ok(())
}
