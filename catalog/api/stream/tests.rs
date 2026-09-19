//! Stream self-tests: every bus message reaches the caller in order under
//! the format's framing, the head rides the first chunk, the bus closing
//! ends the HTTP response, a socket stays open, and the pure framing.

use serde_json::json;

use weft::bus::BusOptions;
use weft::caller::{CallerCall, InboundMessage, LiveRequest, OutboundChunk, ResponseHead};
use weft::signal::DataType;
use weft::{FakeRig, NodeTest, WeftResult};

use super::StreamNode;
use crate::framing::{ndjson_line, sse_event, Format};
use crate::testing::{http_caller, ws_caller};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("sse_frames_each_message_and_ends_with_the_bus", sse_stream),
        NodeTest::fake("raw_sends_payloads_as_they_are", raw_stream),
        NodeTest::fake("an_empty_bus_still_answers_with_the_head", empty_stream),
        NodeTest::fake("a_socket_gets_one_message_per_bus_message_and_stays_open", socket_stream),
        NodeTest::basic("formats_parse_and_name_their_content_type", formats),
        NodeTest::fake("first_goes_out_ahead_of_the_bus", first_then_bus),
        NodeTest::basic("sse_frames_text_per_line_and_json_whole", sse_framing),
        NodeTest::basic("ndjson_is_one_json_value_per_line", ndjson_framing),
    ]
}

/// A bus carrying `messages` (as `delta` payloads) and already closed:
/// the producer finished before the stream node reads, which the
/// from-start cursor must cover.
fn closed_bus(rig: &FakeRig, messages: &[serde_json::Value]) -> WeftResult<serde_json::Value> {
    let (mut bus, marker) = rig.seed_bus(BusOptions::default())?;
    bus.register("llm").expect("a fresh bus accepts its first name");
    for m in messages {
        bus.send("delta", m.clone()).expect("an open bus accepts a send");
    }
    bus.close();
    Ok(marker)
}

async fn sse_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!("hel"), json!("lo")])?;
    let outcome = rig
        .run(&StreamNode, json!({ "bus": bus, "format": "sse", "headers": { "x-run": "r-1" } }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::SendChunk {
                head: Some(
                    ResponseHead::new(200)
                        .with_header("x-run", "r-1")
                        .with_header("content-type", "text/event-stream")
                        .with_keepalive(": keepalive\n\n")
                ),
                chunk: OutboundChunk::Text("data: hel\n\n".into()),
            },
            CallerCall::SendChunk { head: None, chunk: OutboundChunk::Text("data: lo\n\n".into()) },
            CallerCall::Terminate { head: None, final_chunk: None, close: None },
        ]
    );
    Ok(())
}

/// The header every streaming API has: one value first, then the feed,
/// framed the same way so the caller reads one stream and not two. It
/// used to be inexpressible, and the way round it was writing a node.
async fn first_then_bus(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!("hel")])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "ndjson", "first": { "id": 7 } }))
        .await
        .ok()?;
    let sent: Vec<OutboundChunk> = conn.chunks();
    assert_eq!(
        sent,
        vec![
            OutboundChunk::Text("{\"id\":7}\n".into()),
            OutboundChunk::Text("\"hel\"\n".into()),
        ],
        "the header leads, framed like everything behind it"
    );

    // Without it the response is the bus alone, byte for byte.
    let plain = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(plain.clone());
    let bus = closed_bus(&rig, &[json!("hel")])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "ndjson" })).await.ok()?;
    assert_eq!(plain.chunks(), vec![OutboundChunk::Text("\"hel\"\n".into())]);
    Ok(())
}

async fn raw_stream(rig: FakeRig) -> WeftResult<()> {
    // Raw is every byte the author's, so the head carries no filler and
    // there is nothing to write into a quiet connection. That used to be
    // refused. It is not any more: a caller who vanishes is found by
    // their machine no longer acknowledging what it was sent, which the
    // connection layer bounds on every socket whatever the framing, so
    // raw is watched like the rest and streams payloads exactly as they
    // are.
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!({ "n": 1 }), json!("t")])?;
    rig.run(&StreamNode, json!({ "bus": bus, "status": 202 })).await.ok()?;
    assert_eq!(conn.heads(), vec![ResponseHead::new(202)], "raw names no content type and no filler (every byte is payload); the wire fills the type");
    assert_eq!(
        conn.chunks(),
        vec![OutboundChunk::Json(json!({ "n": 1 })), OutboundChunk::Json(json!("t"))]
    );
    Ok(())
}

async fn empty_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = http_caller(DataType::Json, LiveRequest::default(), InboundMessage::Json(json!(null)));
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "ndjson" })).await.ok()?;
    assert_eq!(
        conn.calls(),
        vec![
            CallerCall::EnsureConnected,
            CallerCall::Terminate {
                head: Some(ResponseHead::new(200).with_header("content-type", "application/x-ndjson").with_keepalive("\n")),
                final_chunk: None,
                close: None,
            },
        ]
    );
    Ok(())
}

async fn socket_stream(rig: FakeRig) -> WeftResult<()> {
    let conn = ws_caller(DataType::Json, LiveRequest::default(), vec![]);
    rig.attach_caller(conn.clone());
    let bus = closed_bus(&rig, &[json!("a"), json!("b")])?;
    rig.run(&StreamNode, json!({ "bus": bus, "format": "sse" })).await.ok()?;
    assert_eq!(
        conn.chunks(),
        vec![OutboundChunk::Json(json!("a")), OutboundChunk::Json(json!("b"))],
        "a socket message is a frame: no sse framing, no head"
    );
    assert!(conn.heads().is_empty());
    assert_eq!(conn.close_reason(), None, "the socket stays open");
    let err = rig.run(&StreamNode, json!({ "bus": bus, "status": 500 })).await.failure()?;
    assert!(err.contains("no meaning on a socket"), "{err}");
    Ok(())
}

fn formats() -> WeftResult<()> {
    assert_eq!(Format::parse("sse").map_err(weft::node_error)?, Format::Sse);
    assert_eq!(Format::parse("ndjson").map_err(weft::node_error)?.content_type(), Some("application/x-ndjson"));
    assert_eq!(Format::parse("raw").map_err(weft::node_error)?.content_type(), None);
    assert!(Format::parse("xml").is_err());
    // The filler a quiet stream writes is one a reader of the format
    // drops: a comment event, an empty line; raw has none.
    assert_eq!(Format::Sse.keepalive(), Some(": keepalive\n\n"));
    assert_eq!(Format::Ndjson.keepalive(), Some("\n"));
    assert_eq!(Format::Raw.keepalive(), None);
    Ok(())
}

fn sse_framing() -> WeftResult<()> {
    assert_eq!(sse_event(&json!("hel")), "data: hel\n\n");
    assert_eq!(sse_event(&json!("two\nlines")), "data: two\ndata: lines\n\n");
    assert_eq!(sse_event(&json!({ "a": 1 })), "data: {\"a\":1}\n\n");
    assert_eq!(sse_event(&json!("")), "data: \n\n", "an empty delta is still an event");
    Ok(())
}

fn ndjson_framing() -> WeftResult<()> {
    assert_eq!(ndjson_line(&json!("hel")), "\"hel\"\n");
    assert_eq!(ndjson_line(&json!({ "a": 1 })), "{\"a\":1}\n");
    Ok(())
}
