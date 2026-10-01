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
        NodeTest::fake("a_refused_upload_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_upload_comes_out_on_error_when_it_is_wired", refused_wired),
        NodeTest::fake("a_dot_segment_key_still_fails_the_run_when_error_is_wired", dot_segment_wired),
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

/// The store refuses the PUT with a 403.
fn refuse_the_upload(rig: &FakeRig) {
    rig.respond_raw("PUT", "/pics/a.txt", 403, "application/xml", "<Error>AccessDenied</Error>");
}

fn put_inputs(rig: &FakeRig, key: &str) -> serde_json::Value {
    json!({ "account": rig.access("s3"), "bucket": "pics", "key": key, "content": "x" })
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_upload(&rig);
    let err = rig.run(&S3PutObjectNode, put_inputs(&rig, "a.txt")).await.failure()?;
    assert!(err.contains("AccessDenied"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_upload(&rig);
    rig.wire_output("error");
    let outcome = rig.run(&S3PutObjectNode, put_inputs(&rig, "a.txt")).await.ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("AccessDenied"), "{error}");
    assert!(!outcome.outputs.contains_key("etag"), "a caught failure emits no etag");
    Ok(())
}

/// A `..` key is a mistake in the program, never a value for `error`.
async fn dot_segment_wired(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    let err = rig.run(&S3PutObjectNode, put_inputs(&rig, "a/../b.txt")).await.failure()?;
    assert!(err.starts_with("input error"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}
