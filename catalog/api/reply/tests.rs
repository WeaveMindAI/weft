//! Reply self-tests: the one message per wire, the head on HTTP, the
//! refusal of a head on a socket, and the body shapes.

use serde_json::json;

use weft::caller::{CallerCall, CloseReason, InboundMessage, LiveRequest, OutboundChunk, ResponseHead};
use weft::signal::DataType;
use weft::{FakeRig, NodeTest, WeftResult};

use super::ReplyNode;
use crate::testing::{http_caller, ws_caller};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("an_http_reply_is_the_final_body_under_its_head", http_reply),
        NodeTest::fake("a_socket_reply_is_one_message_and_stays_open", socket_reply),
        NodeTest::fake("a_status_behind_a_socket_is_refused", socket_status_refused),
        NodeTest::fake("a_text_route_wants_a_string_body", text_shape),
        NodeTest::fake("a_bytes_route_sends_a_stored_file", bytes_shape),
        NodeTest::fake("a_stored_file_in_a_json_answer_goes_out_as_a_link", file_link),
        NodeTest::basic("a_head_carries_status_and_string_headers", head_shape),
    ]
}

async fn http_reply(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    let outcome = rig
        .run(
            &ReplyNode,
            json!({ "body": { "ok": true }, "status": 201, "headers": { "x-run": "r-1" } }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::Terminate {
                head: Some(ResponseHead::new(201).with_header("x-run", "r-1")),
                final_chunk: Some(OutboundChunk::Json(json!({ "ok": true }))),
                close: None,
            },
        ]
    );
    Ok(())
}

async fn socket_reply(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    rig.run(&ReplyNode, json!({ "body": { "echo": "hi" } })).await.ok()?;
    assert_eq!(conn.chunks(), vec![OutboundChunk::Json(json!({ "echo": "hi" }))]);
    assert_eq!(conn.close_reason(), None::<Option<CloseReason>>, "the socket stays open");
    Ok(())
}

async fn socket_status_refused(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    let err = rig.run(&ReplyNode, json!({ "body": "x", "status": 201 })).await.failure()?;
    assert!(err.contains("no meaning on a socket"), "{err}");
    assert!(conn.chunks().is_empty(), "nothing was sent");
    Ok(())
}

async fn text_shape(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Text, LiveRequest::default(), InboundMessage::Text(String::new()));
    rig.attach_caller(conn.clone());
    let err = rig.run(&ReplyNode, json!({ "body": { "not": "text" } })).await.failure()?;
    assert!(err.contains("must be a String"), "{err}");
    rig.run(&ReplyNode, json!({ "body": "plain" })).await.ok()?;
    assert_eq!(conn.chunks(), vec![OutboundChunk::Text("plain".into())]);
    Ok(())
}

async fn bytes_shape(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Bytes, LiveRequest::default(), InboundMessage::Bytes(vec![]));
    rig.attach_caller(conn.clone());
    let file = rig.store_file("out.bin", "application/octet-stream", vec![9, 9]);
    rig.run(&ReplyNode, json!({ "body": file })).await.ok()?;
    assert_eq!(conn.chunks(), vec![OutboundChunk::Bytes(vec![9, 9])]);
    let err = rig.run(&ReplyNode, json!({ "body": "not a file" })).await.failure()?;
    assert!(err.contains("stored file"), "{err}");
    Ok(())
}

async fn file_link(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    let file = rig.store_file("cat.png", "image/png", vec![1, 2, 3]);
    let key = weft::storage::StoredFile::from_value(&file)?.key;
    let body = json!({ "card": { "photo": file, "caption": "a cat" }, "more": [file] });
    rig.run(&ReplyNode, json!({ "body": body })).await.ok()?;
    let link = json!({
        "url": format!("{}/public/files/{key}", weft::node_test::FAKE_CALLER_LINK_BASE),
        "mimeType": "image/png",
        "filename": "cat.png",
        "sizeBytes": 3,
    });
    assert_eq!(
        conn.chunks(),
        vec![OutboundChunk::Json(json!({ "card": { "photo": link, "caption": "a cat" }, "more": [link] }))],
        "every stored file, however nested, is a link; nothing else moves"
    );
    Ok(())
}

fn head_shape() -> WeftResult<()> {
    let head = crate::wire::head_for(201, Some(&json!({ "x-run": "r-1" })))?;
    assert_eq!(head.status, 201);
    assert_eq!(head.headers, vec![("x-run".to_string(), "r-1".to_string())]);
    assert!(crate::wire::head_for(200, None)?.headers.is_empty());
    assert!(crate::wire::head_for(200, Some(&json!(["x"]))).is_err(), "a list is not a header map");
    assert!(crate::wire::head_for(200, Some(&json!({ "x": 1 }))).is_err(), "a number is not a header value");
    Ok(())
}
