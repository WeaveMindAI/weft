//! Route self-tests: the registered signal carries the node's fields, and
//! a firing fans the caller's request onto the fixed ports and the body
//! onto the ports the author declared, per the trigger's data type.

use serde_json::json;

use weft::caller::InboundMessage;
use weft::signal::DataType;
use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::RouteNode;
use crate::testing::{http_caller, request};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_route_signal", setup_registers),
        NodeTest::fake("a_json_body_fans_onto_declared_ports", json_body),
        NodeTest::fake("a_text_body_lands_whole_on_the_one_declared_port", text_body),
        NodeTest::fake("a_bytes_body_is_stored_and_its_file_flows", bytes_body),
        NodeTest::fake("the_request_ports_win_over_a_body_key", request_wins),
        NodeTest::fake("a_declared_port_named_like_a_capture_reads_the_capture", capture_port),
        NodeTest::fake("a_picture_in_a_json_body_is_stored_and_its_file_flows", inline_picture),
        NodeTest::fake("bare_base64_is_typed_by_its_bytes_and_held_to_the_port", bare_base64),
        NodeTest::fake("a_json_body_that_is_not_an_object_is_refused", json_body_not_an_object),
        NodeTest::fake("a_request_with_no_body_still_serves_its_ports", json_body_absent),
        NodeTest::fake("a_fired_runs_body_comes_from_its_wake", fired_body_comes_from_the_wake),
        NodeTest::fake("a_fired_text_body_comes_from_its_wake", fired_text_body_comes_from_the_wake),
        NodeTest::fake("a_run_with_no_caller_fails_loud", no_caller),
    ]
}

/// A json route fans the body's KEYS onto ports, so a body with no keys
/// has nothing to fan. It used to leave every port silent, the branches
/// behind them closed, and the caller with no answer and nothing saying
/// why: a frontend posting a list of items got a run that looked fine.
async fn json_body_not_an_object(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(
        DataType::Json,
        request,
        InboundMessage::Json(json!([1, 2, 3])),
    ));
    rig.output_type("user_id", WeftType::parse("String").expect("parses"));
    let why = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.failure()?;
    // The caller has to recognise what they sent, and know the way out.
    assert!(why.contains("a list"), "{why}");
    assert!(why.contains("json object"), "{why}");
    assert!(why.contains("text"), "it names the other body shape: {why}");
    Ok(())
}

/// A FIRED run has no connection, so the body the author typed arrives
/// as this trigger's own wake field and the node reads it there.
///
/// Nothing upstream lifts it out of the payload to serve it back as if
/// a caller had sent it: that would mean the language holding this
/// node's field name. So the node looks at the connection first, finds
/// nothing, and reads its own wake.
async fn fired_body_comes_from_the_wake(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    let mut wake = serde_json::to_value(&request).expect("request serializes");
    wake["body"] = json!({ "user_id": "u-1", "text": "hi" });
    rig.wake(wake);
    // No connection body, exactly as a fired run arrives.
    rig.attach_caller(http_caller(DataType::Json, request, InboundMessage::Json(json!(null))));
    rig.output_type("user_id", WeftType::parse("String").expect("parses"));
    rig.output_type("text", WeftType::parse("String").expect("parses"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["user_id"], json!("u-1"));
    assert_eq!(outcome.outputs["text"], json!("hi"));
    assert_eq!(outcome.outputs["path"], json!("chat/room7"), "and the request still arrives");
    Ok(())
}

/// A text route fired by hand carries its body as a string, and it
/// lands whole on the one declared port, the same as a real text call.
async fn fired_text_body_comes_from_the_wake(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    let mut wake = serde_json::to_value(&request).expect("request serializes");
    wake["body"] = json!("hello there");
    rig.wake(wake);
    rig.attach_caller(http_caller(DataType::Text, request, InboundMessage::Json(json!(null))));
    rig.output_type("note", WeftType::parse("String").expect("parses"));
    let outcome = rig
        .run(&RouteNode, json!({ "path": "chat/{room}", "dataType": "text" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["note"], json!("hello there"));
    Ok(())
}

/// No body at all is not a bad body. It is how every GET arrives, so
/// the request ports still fire and the body ports simply stay silent.
async fn json_body_absent(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Json, request, InboundMessage::Json(json!(null))));
    rig.output_type("user_id", WeftType::parse("String").expect("parses"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert!(outcome.outputs.get("user_id").is_none(), "no body, so nothing to fill it from");
    assert_eq!(outcome.outputs["path"], json!("chat/room7"), "and the request still arrives");
    Ok(())
}

/// A tiny PNG: the signature every sniffer knows, then padding.
fn png_bytes() -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
    bytes.extend_from_slice(&[0u8; 8]);
    bytes
}

async fn capture_port(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(
        DataType::Json,
        request,
        InboundMessage::Json(json!({ "room": "spoofed", "text": "hi" })),
    ));
    rig.output_type("room", WeftType::parse("String").expect("parses"));
    rig.output_type("text", WeftType::parse("String").expect("parses"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["room"], json!("room7"), "the capture, never the body key");
    assert_eq!(outcome.outputs["text"], json!("hi"));
    assert_eq!(outcome.outputs["params"], json!({ "room": "room7" }), "the fixed port still carries every capture");
    Ok(())
}

async fn inline_picture(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    let data_url = weft::storage::media::data_url("image/png", &png_bytes());
    rig.attach_caller(http_caller(
        DataType::Json,
        request,
        InboundMessage::Json(json!({ "photo": data_url, "caption": "a cat" })),
    ));
    rig.output_type("photo", WeftType::parse("Image").expect("parses"));
    rig.output_type("caption", WeftType::parse("String").expect("parses"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["photo"])?;
    assert_eq!(file.mime_type, "image/png");
    assert_eq!(file.size_bytes, png_bytes().len() as u64);
    assert_eq!(file.filename, "photo");
    assert!(!rig.stored_meta(&file.key)?.keep, "run-scoped: the program keeps what it wants");
    assert_eq!(outcome.outputs["caption"], json!("a cat"), "the other keys fan as before");

    // A stored-file value on a file port passes through untouched.
    let again = crate::testing::request();
    rig.wake(serde_json::to_value(&again).expect("request serializes"));
    let stored = rig.store_file("cat.png", "image/png", png_bytes());
    rig.attach_caller(http_caller(DataType::Json, again, InboundMessage::Json(json!({ "photo": stored }))));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["photo"], stored);

    // Anything else on a file port is refused by name.
    let again = crate::testing::request();
    rig.wake(serde_json::to_value(&again).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Json, again, InboundMessage::Json(json!({ "photo": 42 }))));
    let err = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.failure()?;
    assert!(err.contains("port 'photo' is declared Image") && err.contains("a number"), "{err}");
    Ok(())
}

async fn bare_base64(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    let data_url = weft::storage::media::data_url("image/png", &png_bytes());
    let bare = data_url.split_once(";base64,").expect("a data url").1.to_string();
    rig.attach_caller(http_caller(DataType::Json, request, InboundMessage::Json(json!({ "photo": bare }))));
    rig.output_type("photo", WeftType::parse("Image").expect("parses"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["photo"])?;
    assert_eq!(file.mime_type, "image/png", "typed by the bytes' own signature");

    // Bytes that are not what the port declares fail loud.
    let again = crate::testing::request();
    rig.wake(serde_json::to_value(&again).expect("request serializes"));
    let text = weft::storage::media::data_url("text/plain", b"hello").split_once(";base64,").expect("a data url").1.to_string();
    rig.attach_caller(http_caller(DataType::Json, again, InboundMessage::Json(json!({ "photo": text }))));
    let err = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.failure()?;
    assert!(err.contains("port 'photo'") && err.contains("not what the port declares"), "{err}");

    // Not base64 at all: the string is named for what it is.
    let again = crate::testing::request();
    rig.wake(serde_json::to_value(&again).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Json, again, InboundMessage::Json(json!({ "photo": "not base64!!" }))));
    let err = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.failure()?;
    assert!(err.contains("not valid base64"), "{err}");
    Ok(())
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &RouteNode,
        json!({ "path": "chat/{room}", "method": "post", "dataType": "text", "outlivesCaller": true }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes");
    assert_eq!(spec["kind"], "route");
    assert_eq!(spec["config"]["path"], "chat/{room}");
    assert_eq!(spec["config"]["methods"], json!(["POST"]));
    assert_eq!(spec["config"]["data_type"], "text");
    assert_eq!(spec["config"]["can_suspend"], true);
    assert_eq!(spec["config"]["auth"]["kind"], "none");
    Ok(())
}

async fn json_body(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(
        DataType::Json,
        request,
        InboundMessage::Json(json!({ "user_id": "u-1", "text": "hi", "extra": 3 })),
    ));
    rig.output_type("user_id", WeftType::parse("String").expect("parses"));
    rig.output_type("text", WeftType::parse("String").expect("parses"));
    // A declared port named `body` is one more field, never the whole
    // object: a route says what it receives.
    rig.output_type("body", WeftType::parse("JsonDict").expect("parses"));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["user_id"], json!("u-1"));
    assert_eq!(outcome.outputs["text"], json!("hi"));
    assert!(outcome.outputs.get("extra").is_none(), "an undeclared key is skipped");
    assert!(outcome.outputs.get("body").is_none(), "no whole-object port: {:?}", outcome.outputs);
    assert_eq!(outcome.outputs["method"], json!("POST"));
    assert_eq!(outcome.outputs["path"], json!("chat/room7"));
    assert_eq!(outcome.outputs["params"], json!({ "room": "room7" }));
    assert_eq!(outcome.outputs["query"], json!({ "verbose": "1" }));
    assert_eq!(outcome.outputs["headers"], json!({ "content-type": "application/json" }));
    assert_eq!(outcome.outputs["caller"], json!({ "key": 0 }));
    Ok(())
}

async fn text_body(rig: FakeRig) -> WeftResult<()> {
    let mut request = request();
    request.caller = None;
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Text, request, InboundMessage::Text("hello there".into())));
    rig.output_type("message", WeftType::parse("String").expect("parses"));
    let outcome = rig.run(&RouteNode, json!({ "path": "say", "dataType": "text" })).await.ok()?;
    assert_eq!(outcome.outputs["message"], json!("hello there"));
    assert_eq!(outcome.outputs["caller"], serde_json::Value::Null, "an open route has no caller");
    Ok(())
}

async fn bytes_body(rig: FakeRig) -> WeftResult<()> {
    let mut request = request();
    request.headers = vec![("content-type".into(), "image/png".into())];
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(DataType::Bytes, request, InboundMessage::Bytes(vec![1, 2, 3])));
    rig.output_type("upload", WeftType::parse("File").expect("parses"));
    let outcome = rig.run(&RouteNode, json!({ "path": "upload", "dataType": "bytes" })).await.ok()?;
    let file = weft::storage::StoredFile::from_value(&outcome.outputs["upload"])?;
    assert_eq!(file.mime_type, "image/png");
    assert_eq!(file.size_bytes, 3);
    Ok(())
}

async fn request_wins(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(http_caller(
        DataType::Json,
        request,
        InboundMessage::Json(json!({ "path": "spoofed", "caller": { "key": 9 } })),
    ));
    let outcome = rig.run(&RouteNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["path"], json!("chat/room7"), "the request's path, never the body's");
    assert_eq!(outcome.outputs["caller"], json!({ "key": 0 }), "the gate's identity, never the body's");
    Ok(())
}

async fn no_caller(rig: FakeRig) -> WeftResult<()> {
    rig.wake(serde_json::to_value(request()).expect("request serializes"));
    let err = rig.run(&RouteNode, json!({ "path": "x" })).await.failure()?;
    assert!(err.contains("Route"), "{err}");
    Ok(())
}
