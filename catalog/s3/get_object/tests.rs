//! S3GetObject self-tests: the read streams into storage with the
//! store's content type.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::S3GetObjectNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("reads_the_object_into_storage", reads),
        NodeTest::fake("a_missing_object_fails_loud", missing),
    ]
}

async fn reads(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("GET", "/pics/photos/cat.png", 200, "image/png", b"PNG".to_vec());
    let outcome = rig
        .run(
            &S3GetObjectNode,
            json!({ "account": rig.access("s3"), "bucket": "pics", "key": "photos/cat.png" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("cat.png"), "the key's last segment");
    assert_eq!(outcome.outputs["mimeType"], json!("image/png"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(3));
    Ok(())
}

async fn missing(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "GET",
        "/pics/gone.txt",
        404,
        "application/xml",
        "<Error><Code>NoSuchKey</Code></Error>".as_bytes().to_vec(),
    );
    let outcome = rig
        .run(
            &S3GetObjectNode,
            json!({ "account": rig.access("s3"), "bucket": "pics", "key": "gone.txt" }),
        )
        .await;
    let err = outcome.result.expect_err("a 404 must refuse").to_string();
    assert!(err.contains("NoSuchKey"), "{err}");
    Ok(())
}
