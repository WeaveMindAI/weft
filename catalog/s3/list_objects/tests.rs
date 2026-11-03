//! S3ListObjects self-tests. `basic` pins the listing-XML scan;
//! `fake` runs the whole body against canned listings and asserts the
//! page, where the next one starts, and the emitted shape.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::{decode_entities, first_tag, tag_bodies, S3ListObjectsNode};

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
        NodeTest::basic("entities_decode_once_left_to_right", || {
            assert_eq!(decode_entities("&amp;lt;"), "&lt;");
            assert_eq!(decode_entities("a&#13;b&#x0D;c&#X41;"), "a\rb\rcA");
            assert_eq!(decode_entities("&quot;&apos;&gt;"), "\"'>");
            assert_eq!(decode_entities("a & b &bogus; &#xZZ;"), "a & b &bogus; &#xZZ;");
            Ok(())
        }),
        NodeTest::fake("lists_one_page_and_says_where_the_next_starts", paged_listing),
        NodeTest::fake("a_page_stops_early_at_what_a_wire_carries", heavy_page),
    ]
}

/// One `<Contents>` entry.
fn entry(key: &str, size: u64) -> String {
    format!("<Contents><Key>{key}</Key><Size>{size}</Size><LastModified>2026-08-06T00:00:00Z</LastModified></Contents>")
}

async fn paged_listing(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "GET",
        "/pics?list-type=2&max-keys=2",
        200,
        "application/xml",
        format!("<ListBucketResult><IsTruncated>true</IsTruncated>{}{}</ListBucketResult>", entry("a.png", 10), entry("b.png", 20)),
    );
    rig.respond_raw(
        "GET",
        "/pics?list-type=2&max-keys=2&start-after=b.png",
        200,
        "application/xml",
        format!("<ListBucketResult><IsTruncated>false</IsTruncated>{}</ListBucketResult>", entry("c.png", 30)),
    );

    let first = rig
        .run(&S3ListObjectsNode, json!({ "account": rig.access("s3"), "bucket": "pics", "maxKeys": 2 }))
        .await
        .ok()?;
    assert_eq!(first.outputs["count"], json!(2.0));
    assert_eq!(first.outputs["keys"], json!(["a.png", "b.png"]));
    // The full objects port, sizes and stamps included: the fields a
    // downstream filter reads must never silently default.
    assert_eq!(
        first.outputs["objects"],
        json!([
            { "key": "a.png", "sizeBytes": 10.0, "lastModified": "2026-08-06T00:00:00Z" },
            { "key": "b.png", "sizeBytes": 20.0, "lastModified": "2026-08-06T00:00:00Z" },
        ])
    );
    assert_eq!(first.outputs["lastKey"], json!("b.png"));
    assert_eq!(first.outputs["hasMore"], json!(true), "S3 said the listing goes on");

    let next = rig
        .run(
            &S3ListObjectsNode,
            json!({ "account": rig.access("s3"), "bucket": "pics", "maxKeys": 2, "startAfter": "b.png" }),
        )
        .await
        .ok()?;
    assert_eq!(next.outputs["keys"], json!(["c.png"]));
    assert_eq!(next.outputs["lastKey"], json!("c.png"));
    assert_eq!(next.outputs["hasMore"], json!(false));
    assert_eq!(rig.requests().len(), 2, "one request per call");

    // A listing entry missing its Size is a malformed response and
    // fails the node loudly, never a silent zero.
    let broken = FakeRig::new();
    broken.respond_raw(
        "GET",
        "/pics?list-type=2&max-keys=500",
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

/// A thousand keys of 200 characters weigh about 260 KB as objects:
/// the page stops at what a wire carries, and says there is more even
/// though S3 sent the whole listing.
async fn heavy_page(rig: FakeRig) -> WeftResult<()> {
    let keys: Vec<String> = (0..1000).map(|i| format!("{i:04}-{}", "k".repeat(195))).collect();
    let listing: String = keys.iter().map(|k| entry(k, 1)).collect();
    rig.respond_raw(
        "GET",
        "/pics?list-type=2&max-keys=1000",
        200,
        "application/xml",
        format!("<ListBucketResult><IsTruncated>false</IsTruncated>{listing}</ListBucketResult>"),
    );
    let outcome = rig
        .run(&S3ListObjectsNode, json!({ "account": rig.access("s3"), "bucket": "pics", "maxKeys": 1000 }))
        .await
        .ok()?;
    let objects = outcome.output("objects")?;
    let listed = objects.as_array().expect("objects").len();
    assert!(listed > 0 && listed < 1000, "a page shorter than asked: {listed}");
    assert!(weft::storage::wire_weight(objects) <= weft::storage::MAX_WIRE_VALUE_BYTES);
    assert_eq!(outcome.outputs["lastKey"], json!(keys[listed - 1]), "the next page starts right after it");
    assert_eq!(outcome.outputs["hasMore"], json!(true));
    Ok(())
}
