//! S3PutObject self-tests: the one signed PUT and its ETag contract.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::s3_delete_object::S3DeleteObjectNode;
use crate::s3_get_object::S3GetObjectNode;
use crate::s3_list_objects::S3ListObjectsNode;

use super::S3PutObjectNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("puts_the_content_and_emits_the_etag", puts),
        NodeTest::fake("a_dot_segment_key_is_refused", dot_segment),
        NodeTest::live("one_real_put_list_get_delete_round_trip", "s3", live_round_trip)
            .with_fixture(fixture_spec(
                "S3_BUCKET",
                "Bucket",
                "The existing bucket the test round-trips its object through (the \
                 package has no create-bucket node).",
            )),
    ]
}

/// Put `weft-node-tests.txt` into the fixture bucket, see it in the
/// listing, read it back, then delete it: the whole round trip is
/// self-cleaning, so repeated runs never accumulate objects. The
/// bucket itself is a fixture (the package has no create-bucket node).
async fn live_round_trip(rig: LiveRig) -> WeftResult<()> {
    let bucket = rig.fixture("S3_BUCKET")?;
    let key = "weft-node-tests.txt";
    let content = "hello from the weft s3 node tests";
    let account = || rig.access("s3");
    let put = rig
        .run(
            &S3PutObjectNode,
            json!({ "account": account(), "bucket": bucket, "key": key, "content": content }),
        )
        .await
        .ok()?;
    assert!(!put.output("etag")?.as_str().expect("etag").is_empty());
    let listed = rig
        .run(
            &S3ListObjectsNode,
            json!({ "account": account(), "bucket": bucket, "prefix": key }),
        )
        .await
        .ok()?;
    assert!(
        listed.output("keys")?.as_array().expect("keys").contains(&json!(key)),
        "the fresh object is in the listing"
    );
    let got = rig
        .run(
            &S3GetObjectNode,
            json!({ "account": account(), "bucket": bucket, "key": key }),
        )
        .await
        .ok()?;
    let size = got.output("sizeBytes")?.as_f64().expect("size");
    assert_eq!(size, content.len() as f64, "the bytes round-tripped whole");
    let deleted = rig
        .run(
            &S3DeleteObjectNode,
            json!({ "account": account(), "bucket": bucket, "key": key }),
        )
        .await
        .ok()?;
    assert_eq!(deleted.output("done")?, &json!(true));
    Ok(())
}

async fn puts(rig: FakeRig) -> WeftResult<()> {
    // The fake answers 200 with no ETag header; the node treats a
    // missing ETag as a broken store contract, which is itself worth
    // pinning, so this test declares the body and asserts the refusal
    // names the ETag.
    rig.respond_raw("PUT", "/pics/a%20b.txt", 200, "application/xml", Vec::<u8>::new());
    let outcome = rig
        .run(
            &S3PutObjectNode,
            json!({
                "account": rig.access("s3"),
                "bucket": "pics",
                "key": "a b.txt",
                "content": "hello",
            }),
        )
        .await;
    let err = outcome.result.expect_err("no ETag is a broken contract").to_string();
    assert!(err.contains("ETag"), "{err}");
    let sent = rig.requests();
    assert_eq!(sent[0].path, "/pics/a%20b.txt", "the key is segment-encoded");
    assert_eq!(sent[0].body_text.as_deref(), Some("hello"));
    Ok(())
}

async fn dot_segment(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &S3PutObjectNode,
            json!({
                "account": rig.access("s3"),
                "bucket": "pics",
                "key": "a/../b.txt",
                "content": "x",
            }),
        )
        .await;
    let err = outcome.result.expect_err("a dot segment must refuse").to_string();
    assert!(err.contains("path segment"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}
