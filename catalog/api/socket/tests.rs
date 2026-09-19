//! Socket self-tests: the registered signal, the opening request on the
//! fixed ports, and one yielded item per message until the caller is
//! gone.

use serde_json::json;

use weft::caller::{InboundMessage, LiveRequest};
use weft::signal::DataType;
use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::SocketNode;
use crate::testing::ws_caller;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_socket_signal", setup_registers),
        NodeTest::fake("each_message_is_one_item_of_inbound", messages_yield),
        NodeTest::fake("a_bytes_socket_stores_each_frame", bytes_frames),
        NodeTest::fake("a_picture_in_a_json_message_is_stored", inline_picture),
    ]
}

fn request() -> LiveRequest {
    let mut request = LiveRequest { method: "GET".into(), path: "chat/room7".into(), ..Default::default() };
    request.params.insert("room".into(), "room7".into());
    request
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(&SocketNode, json!({ "path": "chat/{room}" })).await.ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes");
    assert_eq!(spec["kind"], "socket");
    assert_eq!(spec["config"]["path"], "chat/{room}");
    assert!(spec["config"].get("methods").is_none(), "a socket serves no method");
    Ok(())
}

async fn messages_yield(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(ws_caller(
        DataType::Json,
        request,
        vec![InboundMessage::Json(json!({ "text": "one" })), InboundMessage::Json(json!({ "text": "two" }))],
    ));
    rig.output_type("inbound", WeftType::parse("Generator[JsonDict]").expect("parses"));
    let outcome = rig.run(&SocketNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["path"], json!("chat/room7"));
    assert_eq!(outcome.outputs["params"], json!({ "room": "room7" }));
    assert!(outcome.outputs.get("method").is_none(), "a socket has no method port");
    // The rig folds every yield into the port's array, in order; the
    // scripted stream running dry is the caller disconnecting.
    assert_eq!(outcome.outputs["inbound"], json!([{ "text": "one" }, { "text": "two" }]));
    Ok(())
}

async fn inline_picture(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
    png.extend_from_slice(&[0u8; 8]);
    let data_url = weft::storage::media::data_url("image/png", &png);
    rig.attach_caller(ws_caller(DataType::Json, request, vec![InboundMessage::Json(json!(data_url))]));
    rig.output_type("inbound", WeftType::parse("Generator[Image]").expect("parses"));
    let outcome = rig.run(&SocketNode, json!({ "path": "frames" })).await.ok()?;
    let items = outcome.outputs["inbound"].as_array().expect("one item per message");
    assert_eq!(items.len(), 1);
    let file = weft::storage::StoredFile::from_value(&items[0])?;
    assert_eq!(file.mime_type, "image/png");
    assert_eq!(file.filename, "inbound");
    Ok(())
}

async fn bytes_frames(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    rig.attach_caller(ws_caller(DataType::Bytes, request, vec![InboundMessage::Bytes(vec![7, 8])]));
    rig.output_type("inbound", WeftType::parse("Generator[File]").expect("parses"));
    let outcome = rig.run(&SocketNode, json!({ "path": "frames", "dataType": "bytes" })).await.ok()?;
    let items = outcome.outputs["inbound"].as_array().expect("one item per frame");
    assert_eq!(items.len(), 1);
    let file = weft::storage::StoredFile::from_value(&items[0])?;
    assert_eq!(file.size_bytes, 2);
    assert_eq!(file.filename, "message-1");
    Ok(())
}
