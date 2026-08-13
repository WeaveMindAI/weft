//! S3DeleteObject self-tests: the one signed DELETE.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::S3DeleteObjectNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("deletes_the_object", deletes)]
}

async fn deletes(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("DELETE", "/pics/old.txt", 204, "application/xml", Vec::<u8>::new());
    let outcome = rig
        .run(
            &S3DeleteObjectNode,
            json!({ "account": rig.access("s3"), "bucket": "pics", "key": "old.txt" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    assert_eq!(rig.requests()[0].method, "DELETE");
    Ok(())
}
