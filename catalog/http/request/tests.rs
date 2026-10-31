//! HttpRequest self-tests: the body is held to its declared type, a
//! binary body arrives intact as a file, and a connection's key rides
//! the header it names.

use std::collections::BTreeMap;

use serde_json::json;

use weft::access::client::{resolve_steps, AppliedStep};
use weft::{AccessSpec, FakeRig, NodeTest, WeftError, WeftResult, WeftType};

use super::HttpRequestNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_json_object_body_stays_a_dict", object_body),
        NodeTest::fake("a_non_object_body_is_the_verbatim_text", text_body),
        NodeTest::fake("a_json_array_body_is_its_text_unless_the_declared_type_takes_it", array_body),
        NodeTest::fake("a_type_that_takes_text_keeps_a_json_scalar_as_its_text", scalar_body),
        NodeTest::fake("a_body_of_the_wrong_shape_fails_naming_the_field", wrong_shape_body),
        NodeTest::fake("a_stored_file_object_in_a_response_is_refused", file_marker_body),
        NodeTest::fake("a_refusal_status_still_emits", refusal_status),
        NodeTest::fake("method_headers_and_body_ride_the_request", request_shape),
        NodeTest::fake("a_bad_method_fails_the_run_even_with_error_wired", bad_method_wired),
        NodeTest::fake("a_binary_body_is_stored_whole_as_a_file", binary_body),
        NodeTest::fake("a_text_body_too_big_for_a_wire_is_stored_whole_as_a_file", oversize_text_body),
        NodeTest::fake("a_text_body_too_big_for_a_wire_fails_when_only_body_is_wired", oversize_text_body_unwired_file),
        NodeTest::fake("the_recipe_sets_the_named_header", connection_header),
        NodeTest::fake("a_header_set_twice_is_refused", header_set_twice),
        NodeTest::fake("a_secret_header_is_revealed_into_the_request", secret_header),
        NodeTest::fake("a_plain_object_body_goes_out_as_it_is", plain_body),
        NodeTest::fake("a_secret_the_body_declares_is_revealed_into_the_request", secret_body),
        NodeTest::fake("a_secret_the_body_does_not_declare_fails_before_sending", undeclared_secret_body),
    ]
}

/// A plain object body, left as a `JsonDict` the way every program
/// without a secret writes it, goes out exactly as it was.
async fn plain_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/items", json!({ "id": 7 }));
    rig.input_type("body", WeftType::parse("JsonDict").expect("parses"));
    let body = json!({ "name": "widget", "tags": ["a", "b"], "nested": { "n": 1 } });
    rig.run(&HttpRequestNode, json!({ "url": "https://api.example/api/items", "method": "POST", "body": body.clone() }))
        .await
        .ok()?;
    assert_eq!(rig.requests()[0].body, Some(body));
    Ok(())
}

/// A secret where the type of what is wired into `body` declares one goes
/// into the request revealed.
async fn secret_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/login", json!({ "ok": true }));
    rig.input_type("body", WeftType::parse("{ user: String, token: Secret[String] }").expect("parses"));
    let token = rig.secret(json!("tok-9"));
    rig.run(
        &HttpRequestNode,
        json!({ "url": "https://api.example/api/login", "method": "POST", "body": { "user": "ada", "token": token } }),
    )
    .await
    .ok()?;
    assert_eq!(rig.requests()[0].body, Some(json!({ "user": "ada", "token": "tok-9" })));
    Ok(())
}

/// A secret inside a body left as a `JsonDict` fails the node naming the
/// input, where the secret is and what to declare, and nothing is sent:
/// the API would have received the marker.
async fn undeclared_secret_body(rig: FakeRig) -> WeftResult<()> {
    rig.input_type("body", WeftType::parse("JsonDict").expect("parses"));
    let token = rig.secret(json!("tok-9"));
    let err = rig
        .run(
            &HttpRequestNode,
            json!({ "url": "https://api.example/api/login", "method": "POST", "body": { "auth": { "token": token } } }),
        )
        .await
        .failure()?;
    assert!(err.contains("`body` holds a secret at /auth/token"), "{err}");
    assert!(err.contains("the type of what is wired into `body`") && err.contains("{ token: Secret[String] }"), "{err}");
    assert!(rig.requests().is_empty(), "nothing is sent");
    Ok(())
}

/// A header whose value is a secret (a caller's credential passed on)
/// goes into the request revealed.
async fn secret_header(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/api/me", json!({ "id": 7 }));
    let token = rig.secret(json!("Bearer tok-1"));
    rig.run(
        &HttpRequestNode,
        json!({ "url": "https://api.example/api/me", "method": "GET", "headers": { "authorization": token, "x-trace": "t-9" } }),
    )
    .await
    .ok()?;
    let sent = rig.requests();
    assert_eq!(sent[0].header("authorization"), Some("Bearer tok-1"));
    assert_eq!(sent[0].header("x-trace"), Some("t-9"));
    Ok(())
}

async fn bad_method_wired(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("_error");
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

/// The default `JsonDict | String` has no place for a list, so a JSON
/// array arrives as its text; declared `Json` (or a list type), the same
/// response is the list itself.
async fn array_body(rig: FakeRig) -> WeftResult<()> {
    let array = r#"[[1, "a"], [2, "b"]]"#;
    rig.respond_raw("GET", "/rows", 200, "application/json", array.as_bytes().to_vec());
    let request = json!({ "url": "https://api.example/rows", "method": "GET" });
    let outcome = rig.run(&HttpRequestNode, request.clone()).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!(array), "the default type keeps the text");
    rig.output_type("body", WeftType::parse("Json").expect("parses"));
    let outcome = rig.run(&HttpRequestNode, request.clone()).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!([[1, "a"], [2, "b"]]), "Json takes the list");
    rig.output_type("body", WeftType::parse("List[List[Number | String]]").expect("parses"));
    let outcome = rig.run(&HttpRequestNode, request).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!([[1, "a"], [2, "b"]]), "a list type takes it too");
    Ok(())
}

/// A JSON string or number is still text to a type that takes text:
/// `"done"` keeps its quotes under the default and under `Json`, so what
/// the server sent is what arrives. A type that takes no text gets it
/// parsed.
async fn scalar_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("GET", "/state", 200, "application/json", br#""done""#.to_vec());
    rig.respond_raw("GET", "/count", 200, "application/json", b"42".to_vec());
    let at = |path: &str| json!({ "url": format!("https://api.example{path}"), "method": "GET" });
    let outcome = rig.run(&HttpRequestNode, at("/state")).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!(r#""done""#));
    rig.output_type("body", WeftType::parse("Json").expect("parses"));
    let outcome = rig.run(&HttpRequestNode, at("/count")).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!("42"));
    rig.output_type("body", WeftType::parse("Number").expect("parses"));
    let outcome = rig.run(&HttpRequestNode, at("/count")).await.ok()?;
    assert_eq!(outcome.outputs["body"], json!(42));
    Ok(())
}

/// Declared a record, a response of the wrong shape fails the node naming
/// the field, never as "text where a record was wanted".
async fn wrong_shape_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/verdict", json!({ "score": "high" }));
    rig.output_type("body", WeftType::parse("{ score: Number }").expect("parses"));
    let err = rig
        .run(&HttpRequestNode, json!({ "url": "https://api.example/verdict", "method": "GET" }))
        .await
        .failure()?;
    assert!(err.contains(".score: expected Number"), "{err}");
    Ok(())
}

/// A server answering with a stored-file object cannot point the program
/// at a file of this project: the node refuses it.
async fn file_marker_body(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/pic", json!({ "__weft_image__": { "key": "another/run/key", "mimeType": "image/png" } }));
    let err = rig
        .run(&HttpRequestNode, json!({ "url": "https://api.example/pic", "method": "GET" }))
        .await
        .failure()?;
    assert!(err.contains("stored-file object"), "{err}");
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

/// A JSON object of about 120 KB, more than the 100 KB a wire carries.
fn oversize_json() -> Vec<u8> {
    let text = "x".repeat(weft::storage::MAX_WIRE_VALUE_BYTES + 20 * 1024);
    serde_json::to_vec(&json!({ "items": text })).expect("json")
}

async fn oversize_text_body(rig: FakeRig) -> WeftResult<()> {
    let big = oversize_json();
    rig.respond_raw("GET", "/export.json", 200, "application/json; charset=utf-8", big.clone());
    rig.wire_output("body");
    rig.wire_output("file");
    let outcome = rig
        .run(&HttpRequestNode, json!({ "url": "https://api.example/export.json", "method": "GET" }))
        .await
        .ok()?;
    assert!(outcome.outputs.get("body").is_none(), "a body past the wire limit never rides `body`");
    let file = weft::StoredFile::from_value(outcome.output("file")?)?;
    assert_eq!(file.mime_type, "application/json", "stored as the type the response named");
    assert_eq!(&rig.stored_bytes(&file.key)?[..], &big[..], "the whole body, uncut");
    assert_eq!(outcome.outputs["ok"], json!(true));
    Ok(())
}

async fn oversize_text_body_unwired_file(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("GET", "/export.json", 200, "application/json", oversize_json());
    rig.wire_output("body");
    let err = rig
        .run(&HttpRequestNode, json!({ "url": "https://api.example/export.json", "method": "GET" }))
        .await
        .failure()?;
    assert!(err.contains("121 KB") && err.contains("at most 100 KB"), "it says the weight and the limit: {err}");
    assert!(err.contains("Wire `file`"), "it names the fix: {err}");
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
