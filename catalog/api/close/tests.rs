//! Close self-tests: a socket's close frame carries code and reason; an
//! HTTP close is the reason as the body when there is one, a bodiless
//! answer when there is none, and a plain end after a stream. The other
//! wire's field is refused, never dropped.

use serde_json::json;

use weft::caller::{
    CallerCall, CallerConnection as _, CloseReason, InboundMessage, LiveRequest, OutboundChunk,
    ResponseHead,
};
use weft::signal::DataType;
use weft::{FakeRig, NodeTest, WeftResult};

use super::CloseNode;
use crate::testing::{http_caller, ws_caller};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_socket_close_carries_code_and_reason", socket_close),
        NodeTest::fake("an_unanswered_route_closes_with_a_bodiless_status", http_bodiless),
        NodeTest::fake("after_a_stream_the_close_just_ends_the_response", http_after_stream),
        NodeTest::fake("a_reason_behind_a_route_is_the_body_under_the_status", http_reason_body),
        NodeTest::fake("a_reason_with_no_status_answers_200", http_reason_default_status),
        NodeTest::fake("a_reason_with_a_204_is_refused", http_reason_on_204),
        NodeTest::fake("a_reason_after_a_stream_is_refused", http_reason_after_stream),
        NodeTest::fake("a_close_code_behind_a_route_is_refused", code_on_route),
        NodeTest::fake("a_status_behind_a_socket_is_refused", status_on_socket),
        NodeTest::fake("a_bodiless_ending_carries_its_own_headers", http_headers_bodiless),
        NodeTest::fake("headers_after_a_stream_are_refused", http_headers_after_stream),
    ]
}

/// The 304 case: an ending with no body still has a head, so it can
/// carry the caching header the next request will send back.
async fn http_headers_bodiless(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    rig.run(&CloseNode, json!({ "status": 304, "headers": { "etag": "\"abc123\"" } })).await.ok()?;
    assert_eq!(
        conn.heads(),
        vec![ResponseHead::new(304).with_header("etag", "\"abc123\"")]
    );
    Ok(())
}

async fn http_headers_after_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    conn.send_chunk(None, OutboundChunk::Json(json!("first"))).await?;
    rig.attach_caller(conn.clone());
    let err = rig
        .run(&CloseNode, json!({ "headers": { "etag": "x" } }))
        .await
        .result
        .expect_err("headers after a stream are refused")
        .to_string();
    assert!(err.contains("already sent the response head"), "{err}");
    Ok(())
}

async fn http_reason_body(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    rig.run(&CloseNode, json!({ "status": 404, "reason": "nothing to sweep" })).await.ok()?;
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::Terminate {
                head: Some(ResponseHead::new(404)),
                final_chunk: Some(OutboundChunk::Json(json!("nothing to sweep"))),
                close: None,
            },
        ]
    );
    Ok(())
}

async fn http_reason_default_status(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Text, LiveRequest::default(), InboundMessage::Text(String::new()));
    rig.attach_caller(conn.clone());
    rig.run(&CloseNode, json!({ "reason": "all done" })).await.ok()?;
    assert_eq!(conn.heads(), vec![ResponseHead::new(200)]);
    assert!(matches!(conn.calls().last(), Some(CallerCall::Terminate { final_chunk: Some(OutboundChunk::Text(t)), .. }) if t == "all done"));
    Ok(())
}

async fn http_reason_on_204(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    let err = rig.run(&CloseNode, json!({ "status": 204, "reason": "gone" })).await.result.expect_err("a 204 carries no body").to_string();
    assert!(err.contains("204") && err.contains("reason"), "{err}");
    assert!(conn.heads().is_empty(), "nothing went out");
    Ok(())
}

async fn http_reason_after_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    conn.send_chunk(None, OutboundChunk::Text("part".into())).await.map_err(weft::node_error)?;
    rig.attach_caller(conn.clone());
    let err = rig.run(&CloseNode, json!({ "reason": "late" })).await.result.expect_err("the response already went out").to_string();
    assert!(err.contains("Stream") && err.contains("reason"), "{err}");
    Ok(())
}

async fn code_on_route(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    let err = rig.run(&CloseNode, json!({ "code": 4001 })).await.result.expect_err("a close code means a socket").to_string();
    assert!(err.contains("code") && err.contains("Route"), "{err}");
    Ok(())
}

async fn status_on_socket(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    let err = rig.run(&CloseNode, json!({ "status": 404 })).await.result.expect_err("a status means a route").to_string();
    assert!(err.contains("status") && err.contains("Socket"), "{err}");
    assert_eq!(conn.close_reason(), None, "the socket stays open");
    Ok(())
}

async fn socket_close(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    let outcome = rig.run(&CloseNode, json!({ "code": 4001, "reason": "done" })).await.ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    assert_eq!(conn.close_reason(), Some(Some(CloseReason { code: 4001, reason: "done".into() })));
    Ok(())
}

async fn http_bodiless(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    rig.run(&CloseNode, json!({ "status": 404 })).await.ok()?;
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::Terminate { head: Some(ResponseHead::new(404)), final_chunk: None, close: None },
        ]
    );
    Ok(())
}

async fn http_after_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    // Something already went out (a Stream's chunk): the head is committed.
    conn.send_chunk(None, OutboundChunk::Text("part".into())).await.map_err(weft::node_error)?;
    rig.attach_caller(conn.clone());
    rig.run(&CloseNode, json!({})).await.ok()?;
    assert_eq!(conn.heads(), Vec::<ResponseHead>::new(), "no second head");
    assert_eq!(conn.close_reason(), Some(None));
    Ok(())
}
