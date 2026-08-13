//! S3ListObjects self-tests. `basic` pins the listing-XML scan;
//! `fake` runs the whole body against a canned two-page listing and
//! asserts the pagination + the emitted shape.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::{first_tag, tag_bodies, S3ListObjectsNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("listing_scan_pulls_keys_sizes_and_the_token", || {
            let xml = "<ListBucketResult><Contents><Key>a/b &amp; c.txt</Key><Size>12</Size>\
                       <LastModified>2026-08-06T00:00:00Z</LastModified></Contents>\
                       <Contents><Key>d.png</Key><Size>3</Size></Contents>\
                       <NextContinuationToken>tok==</NextContinuationToken></ListBucketResult>";
            let bodies = tag_bodies(xml, "Contents");
            assert_eq!(bodies.len(), 2);
            assert_eq!(first_tag(&bodies[0], "Key").unwrap(), "a/b & c.txt");
            assert_eq!(first_tag(&bodies[1], "Size").unwrap(), "3");
            assert_eq!(first_tag(xml, "NextContinuationToken").unwrap(), "tok==");
            assert!(first_tag(xml, "Missing").is_none());
            Ok(())
        }),
        NodeTest::fake("lists_across_pages_and_emits_the_objects", paginated_listing),
    ]
}

async fn paginated_listing(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "GET",
        "/pics?list-type=2&max-keys=1000",
        200,
        "application/xml",
        "<ListBucketResult>\
           <Contents><Key>a.png</Key><Size>10</Size>\
           <LastModified>2026-08-06T00:00:00Z</LastModified></Contents>\
           <NextContinuationToken>tok</NextContinuationToken>\
         </ListBucketResult>",
    );
    rig.respond_raw(
        "GET",
        "/pics?list-type=2&max-keys=1000&continuation-token=tok",
        200,
        "application/xml",
        "<ListBucketResult>\
           <Contents><Key>b.png</Key><Size>20</Size>\
           <LastModified>2026-08-07T00:00:00Z</LastModified></Contents>\
         </ListBucketResult>",
    );

    let outcome = rig
        .run(
            &S3ListObjectsNode,
            json!({ "account": rig.access("s3"), "bucket": "pics" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(2.0));
    assert_eq!(outcome.outputs["keys"], json!(["a.png", "b.png"]));
    // The full objects port, sizes and stamps included: the fields a
    // downstream filter reads must never silently default.
    assert_eq!(
        outcome.outputs["objects"],
        json!([
            { "key": "a.png", "sizeBytes": 10.0, "lastModified": "2026-08-06T00:00:00Z" },
            { "key": "b.png", "sizeBytes": 20.0, "lastModified": "2026-08-07T00:00:00Z" },
        ])
    );
    assert_eq!(rig.requests().len(), 2, "one request per page");

    // A listing entry missing its Size is a malformed response and
    // fails the node loudly, never a silent zero.
    let broken = FakeRig::new();
    broken.respond_raw(
        "GET",
        "/pics?list-type=2&max-keys=1000",
        200,
        "application/xml",
        "<ListBucketResult><Contents><Key>x</Key></Contents></ListBucketResult>",
    );
    let outcome = broken
        .run(
            &S3ListObjectsNode,
            json!({ "account": broken.access("s3"), "bucket": "pics" }),
        )
        .await;
    let err = outcome.result.expect_err("missing Size is loud").to_string();
    assert!(err.contains("Size"), "{err}");
    Ok(())
}
