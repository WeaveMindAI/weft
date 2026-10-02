//! HttpRequest self-tests: the body's declared union stays honest, a
//! binary body arrives intact as a file, and a connection's key rides
//! the header it names.

use std::collections::BTreeMap;

use serde_json::json;

use weft::access::client::{resolve_steps, AppliedStep};
use weft::{AccessSpec, FakeRig, NodeTest, WeftError, WeftResult};

use super::HttpRequestNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_json_object_body_stays_a_dict", object_body),
        NodeTest::fake("a_non_object_body_is_the_verbatim_text", text_body),
        NodeTest::fake("a_refusal_status_still_emits", refusal_status),
        NodeTest::fake("method_headers_and_body_ride_the_request", request_shape),
        NodeTest::fake("a_bad_method_fails_the_run_even_with_error_wired", bad_method_wired),
        NodeTest::fake("a_binary_body_is_stored_whole_as_a_file", binary_body),
        NodeTest::fake("the_recipe_sets_the_named_header", connection_header),
        NodeTest::fake("a_header_set_twice_is_refused", header_set_twice),
    ]
}

async fn bad_method_wired(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    let err = rig
        .run(
            &HttpRequestNode,
            json!({ "url": "https://api.example/api/items", "method": "NOT A METHOD" }),
        )
        .await
        .failure()?;
    assert!(err.starts_with("input error") && err.contains("bad method"), "{err}");
    assert!(rig.requests().is_empty(), "a program mistake refuses before anything is sent");
    Ok(())
}

async fn request_shape(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/items", json!({ "id": 7 }));
    let outcome = rig
        .run(
            &HttpRequestNode,
            json!({
                "url": "https://api.example/api/items",
                "method": "POST",
                "headers": { "authorization": "Bearer tok-1", "x-trace": "t-9" },
                "body": { "name": "widget" },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!(200));

    let sent = rig.requests();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].method, "POST");
    assert_eq!(sent[0].body.as_ref().expect("json body")["name"], json!("widget"));
    assert_eq!(sent[0].header("authorization"), Some("Bearer tok-1"));
    assert_eq!(sent[0].header("x-trace"), Some("t-9"));
    assert_eq!(sent[0].header("content-type"), Some("application/json"));
    Ok(())
}

async fn object_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/api/thing", json!({ "a": 1 }));
    let outcome = rig
        .run(
            &HttpRequestNode,
            json!({ "url": "https://api.example/api/thing", "method": "GET" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!(200));
    assert_eq!(outcome.outputs["ok"], json!(true));
    assert_eq!(outcome.outputs["body"], json!({ "a": 1 }));
    Ok(())
}

async fn text_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("GET", "/plain", 200, "text/plain", "just text".as_bytes().to_vec());
    let outcome = rig
        .run(&HttpRequestNode, json!({ "url": "https://api.example/plain", "method": "GET" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["body"], json!("just text"), "non-JSON stays verbatim text");
    Ok(())
}

async fn refusal_status(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status("POST", "/api/thing", 404, json!({ "error": "gone" }));
    let outcome = rig
        .run(
            &HttpRequestNode,
            json!({
                "url": "https://api.example/api/thing",
                "method": "POST",
                "body": { "x": 1 },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!(404));
    assert_eq!(outcome.outputs["ok"], json!(false), "a refusal is data, not a node failure");
    assert_eq!(rig.requests()[0].body, Some(json!({ "x": 1 })));
    Ok(())
}

async fn binary_body(rig: FakeRig) -> WeftResult<()> {
    let png: &[u8] = &[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0xff, 0x00, 0xfe];
    rig.respond_raw("GET", "/img/logo.png", 200, "image/png", png.to_vec());
    let outcome = rig
        .run(&HttpRequestNode, json!({ "url": "https://api.example/img/logo.png", "method": "GET" }))
        .await
        .ok()?;
    assert!(outcome.outputs.get("body").is_none(), "a binary body is not text");
    let file = weft::StoredFile::from_value(&outcome.outputs["file"])?;
    assert_eq!(file.mime_type, "image/png");
    assert_eq!(file.filename, "logo.png");
    assert_eq!(&rig.stored_bytes(&file.key)?[..], png, "every byte survives");
    Ok(())
}

/// The fake rig's client applies no auth steps, so the header itself
/// is proven on the recipe: the step resolves to the header the
/// connection names, carrying its value.
async fn connection_header(_rig: FakeRig) -> WeftResult<()> {
    let metadata: serde_json::Value = serde_json::from_str(include_str!("metadata.json"))
        .map_err(|e| WeftError::Config(e.to_string()))?;
    let spec: AccessSpec = serde_json::from_value(metadata["service"].clone())
        .map_err(|e| WeftError::Config(e.to_string()))?;
    spec.validate().map_err(WeftError::Config)?;
    let values = BTreeMap::from([
        ("header".to_string(), "X-Api-Key".to_string()),
        ("value".to_string(), "key-123".to_string()),
    ]);
    let steps = resolve_steps(&spec.auth, &values).map_err(WeftError::Config)?;
    assert!(
        steps == [AppliedStep::Header { name: "X-Api-Key".into(), value: "key-123".into() }],
        "the connection's header carries its value"
    );
    Ok(())
}

async fn header_set_twice(rig: FakeRig) -> WeftResult<()> {
    rig.connection_value("http_header_key", "header", "Authorization");
    rig.connection_value("http_header_key", "value", "Bearer key-123");
    let err = rig
        .run(
            &HttpRequestNode,
            json!({
                "url": "https://api.example/api/me",
                "method": "GET",
                "connection": rig.access("http_header_key"),
                "headers": { "authorization": "Bearer other" },
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("authorization") && err.contains("connection"), "{err}");
    assert!(!err.contains("key-123"), "the key never shows: {err}");
    assert!(rig.requests().is_empty());
    Ok(())
}
