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
        NodeTest::fake("inbound_declared_json_takes_any_json_message", any_json_messages),
        NodeTest::fake("a_bytes_socket_stores_each_frame", bytes_frames),
        NodeTest::fake("a_picture_in_a_json_message_is_stored", inline_picture),
        NodeTest::fake("a_message_that_does_not_fit_is_refused_back_and_the_session_goes_on", misfit_refused),
        NodeTest::fake("a_binary_frame_where_no_file_fits_is_refused_before_it_is_stored", bytes_on_text_refused),
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
    assert_eq!(spec["config"]["error_mode"], "surface");
    assert_eq!(spec["config"]["backpressure"], "block");

    rig.run_setup_trigger(
        &SocketNode,
        json!({ "path": "frames", "whenChunkFails": "drop_chunk", "whenCallerIsSlow": "drop_newest" }),
    )
    .await
    .ok()?;
    let spec = serde_json::to_value(&rig.registered_signals().last().expect("a second socket").0).expect("spec serializes");
    assert_eq!(spec["config"]["error_mode"], "drop_chunk");
    assert_eq!(spec["config"]["backpressure"], "drop_newest");
    let err = rig.run_setup_trigger(&SocketNode, json!({ "path": "frames", "whenCallerIsSlow": "skip" })).await.failure()?;
    assert!(err.contains("'skip'") && err.contains("drop_oldest"), "a word outside the set is refused, naming the set: {err}");

    rig.run_setup_trigger(&SocketNode, json!({ "path": "frames", "journalWindowMs": 5000 })).await.ok()?;
    let spec = serde_json::to_value(&rig.registered_signals().last().expect("a slow-window socket").0).expect("spec serializes");
    assert_eq!(spec["config"]["journal_window_ms"], 5000, "the window reaches the socket");
    let err = rig.run_setup_trigger(&SocketNode, json!({ "path": "frames", "journalWindowMs": 20 })).await.failure()?;
    assert!(err.contains("journalWindowMs"), "a window under the floor is refused, naming the setting: {err}");
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
    // Nothing declared: each item is a JsonDict.
    let outcome = rig.run(&SocketNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["path"], json!("chat/room7"));
    assert_eq!(outcome.outputs["params"], json!({ "room": "room7" }));
    assert!(outcome.outputs.get("method").is_none(), "a socket has no method port");
    // The rig folds every yield into the port's array, in order; the
    // scripted stream running dry is the caller disconnecting.
    assert_eq!(outcome.outputs["inbound"], json!([{ "text": "one" }, { "text": "two" }]));
    Ok(())
}

/// A device that answers whatever shape it likes: declared `Json`, every
/// message comes through as it was sent, a list of lists and a bare
/// string as much as an object.
async fn any_json_messages(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    let conn = ws_caller(
        DataType::Json,
        request,
        vec![
            InboundMessage::Json(json!([[1, "a"], [2, "b"]])),
            InboundMessage::Json(json!("ok")),
            InboundMessage::Json(json!(42)),
            InboundMessage::Json(json!({ "x": 1 })),
        ],
    );
    rig.attach_caller(conn.clone());
    rig.output_type("inbound", WeftType::parse("Generator[Json]").expect("parses"));
    let outcome = rig.run(&SocketNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["inbound"], json!([[[1, "a"], [2, "b"]], "ok", 42, { "x": 1 }]));
    assert!(conn.chunks().is_empty(), "nothing was refused: {:?}", conn.chunks());
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
    assert_eq!(file.filename, "message-1");
    Ok(())
}

/// A message that does not fit the declared item type is the caller's
/// mistake: the caller is told what is wrong, the item is not yielded,
/// and the next message is read as usual.
async fn misfit_refused(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    let conn = ws_caller(
        DataType::Json,
        request,
        vec![InboundMessage::Json(json!({ "x": 1 })), InboundMessage::Json(json!({ "x": 2, "y": 3 }))],
    );
    rig.attach_caller(conn.clone());
    rig.output_type("inbound", WeftType::parse("Generator[{ x: Number, y: Number }]").expect("parses"));
    let outcome = rig.run(&SocketNode, json!({ "path": "chat/{room}" })).await.ok()?;
    assert_eq!(outcome.outputs["inbound"], json!([{ "x": 2, "y": 3 }]), "only the message that fits");
    match conn.chunks().first() {
        Some(weft::caller::OutboundChunk::Json(answer)) => {
            let error = answer["error"].as_str().expect("an error field");
            assert!(error.contains("missing required field 'y'"), "{error}");
        }
        other => panic!("the caller hears what was wrong, got {other:?}"),
    }
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

/// Bytes only ever become a stored file, so a frame of bytes on a socket
/// whose items hold no file is refused back without writing anything.
async fn bytes_on_text_refused(rig: FakeRig) -> WeftResult<()> {
    let request = request();
    rig.wake(serde_json::to_value(&request).expect("request serializes"));
    let conn = ws_caller(
        DataType::Text,
        request,
        vec![InboundMessage::Bytes(vec![7, 8]), InboundMessage::Text("hello".into())],
    );
    rig.attach_caller(conn.clone());
    rig.output_type("inbound", WeftType::parse("Generator[String]").expect("parses"));
    let outcome = rig.run(&SocketNode, json!({ "path": "chat/{room}", "dataType": "text" })).await.ok()?;
    assert_eq!(outcome.outputs["inbound"], json!(["hello"]), "only the message that fits");
    match conn.chunks().first() {
        Some(weft::caller::OutboundChunk::Text(error)) => {
            assert!(error.contains("message-1 is bytes, and it takes String"), "{error}");
        }
        other => panic!("the caller hears what was wrong, got {other:?}"),
    }
    assert!(rig.stored_files(&weft::storage::StorageScope::Run)?.is_empty(), "nothing was stored");
    Ok(())
}
